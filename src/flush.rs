use log::warn;
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info};

use crate::db::{DbInner, FlushMsg, FlushSender};
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskShutdown;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable, WritableKVTable};
use crate::types::{RowAttributes, ValueDeletable};
use crate::utils::spawn_bg_task;
use crate::{db_state, utils};

#[derive(Debug)]
pub(crate) enum WalFlushThreadMsg {
    Shutdown,
    FlushImmutableWals { force_flush: bool },
}

struct WalFlusher {
    db_inner: Arc<DbInner>,
    flush_waiters: Vec<FlushSender>,
}

impl WalFlusher {
    async fn flush(&mut self) -> Result<(), SlateDBError> {
        self.db_inner.state.write().freeze_wal()?;
        self.db_inner.flush_imm_wals().await?;
        self.db_inner.flush().await?;
        for waiter in self.flush_waiters.drain(..) {
            let res = waiter.send(Ok(()));
            if let Err(Err(e)) = res {
                error!("error sending flush response: {e}");
            }
        }
        Ok(())
    }
}

impl DbInner {
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        Ok(())
    }

    pub(crate) async fn flush_imm_table(
        &self,
        id: &db_state::SsTableId,
        imm_table: Arc<KVTable>,
    ) -> Result<SsTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = imm_table.iter();
        while let Some(entry) = iter.next_entry().await? {
            sst_builder.add(entry)?;
        }

        let encoded_sst = sst_builder.build()?;
        let handle = self.table_store.write_sst(id, encoded_sst).await?;
        Ok(handle)
    }

    async fn flush_imm_wal(&self, imm: Arc<ImmutableWal>) -> Result<SsTableHandle, SlateDBError> {
        let wal_id = db_state::SsTableId::Wal(imm.id());
        self.flush_imm_table(&wal_id, imm.table()).await
    }

    fn flush_imm_wal_to_memtable(&self, mem_table: &mut WritableKVTable, imm_table: Arc<KVTable>) {
        let mut iter = imm_table.iter();
        while let Some(kv) = iter.next_entry_sync() {
            match kv.value {
                ValueDeletable::Value(v) => {
                    mem_table.put(
                        kv.key,
                        v,
                        RowAttributes {
                            ts: kv.create_ts,
                            expire_ts: kv.expire_ts,
                        },
                    );
                }
                ValueDeletable::Merge(_) => {
                    todo!()
                }
                ValueDeletable::Tombstone => {
                    mem_table.delete(
                        kv.key,
                        RowAttributes {
                            ts: kv.create_ts,
                            expire_ts: kv.expire_ts,
                        },
                    );
                }
            }
        }
    }

    async fn flush_imm_wals(&self) -> Result<(), SlateDBError> {
        while let Some(imm) = {
            let rguard = self.state.read();
            rguard.state().imm_wal.back().cloned()
        } {
            self.flush_imm_wal(imm.clone()).await?;
            let mut wguard = self.state.write();
            wguard.pop_imm_wal();
            // flush to the memtable before notifying so that data is available for reads
            self.flush_imm_wal_to_memtable(wguard.memtable(), imm.table());
            self.maybe_freeze_memtable(&mut wguard, imm.id())?;
            imm.table().notify_durable(Ok(()));
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_task(
        self: &Arc<Self>,
        mut rx: UnboundedReceiver<FlushMsg<WalFlushThreadMsg>>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let this = Arc::clone(self);
        async fn core_flush_loop(
            this: &Arc<DbInner>,
            flusher: &mut WalFlusher,
            rx: &mut UnboundedReceiver<FlushMsg<WalFlushThreadMsg>>,
        ) -> Result<(), SlateDBError> {
            let mut ticker = tokio::time::interval(this.options.flush_interval);
            let mut err_reader = this.state.read().error_reader();
            loop {
                select! {
                    err = err_reader.await_value() => {
                        return Err(err);
                    }
                    // Tick to freeze and flush the memtable
                    _ = ticker.tick() => {
                        let result = flusher.flush().await;
                        if let Err(err) = result {
                            error!("error from wal flush: {err}");
                            return Err(err);
                        }
                    }
                    msg = rx.recv() => {
                        let (rsp_sender, msg) = msg.expect("channel unexpectedly closed");
                        match msg {
                            WalFlushThreadMsg::Shutdown => {
                                // Stop the thread.
                                _ = this.flush().await;
                                return Ok(())
                            },
                            WalFlushThreadMsg::FlushImmutableWals{ force_flush } => {
                                if let Some(sender) = rsp_sender {
                                    flusher.flush_waiters.push(sender);
                                }

                                if force_flush {
                                    let result = flusher.flush().await;
                                    if let Err(err) = &result {
                                        error!("error from wal flush: {err}");
                                        return Err(err.clone());
                                    }
                                }
                            },
                        }
                    }
                }
            }
        }

        let fut = async move {
            let mut flusher = WalFlusher {
                db_inner: this.clone(),
                flush_waiters: Vec::new(),
            };

            let result = core_flush_loop(&this, &mut flusher, &mut rx).await;
            let error = result.clone().err().unwrap_or(BackgroundTaskShutdown);
            utils::close_and_drain_receiver(&mut rx, &error).await;
            utils::drain_sender_queue(&mut flusher.flush_waiters, &error);
            info!("wal flush thread exiting with {:?}", result);
            result
        };

        let this = Arc::clone(self);
        Some(spawn_bg_task(
            tokio_handle,
            move |err| {
                warn!("flush task exited with {:?}", err);
                // notify any waiters about the failure
                let mut state = this.state.write();
                state.record_fatal_error(err.clone());
                info!("notifying writeable wal of error");
                state.wal().table().notify_durable(Err(err.clone()));
                for imm in state.snapshot().state.imm_wal.iter() {
                    info!("notifying immutable wal {} of error", imm.id());
                    imm.table().notify_durable(Err(err.clone()));
                }
            },
            fut,
        ))
    }
}
