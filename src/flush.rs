use log::warn;
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info};

use crate::db::{DbInner, FlushMsg};
use crate::db_state;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskShutdown;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable, WritableKVTable};
use crate::types::{RowAttributes, ValueDeletable};
use crate::utils::spawn_bg_task;

#[derive(Debug)]
pub(crate) enum WalFlushThreadMsg {
    Shutdown,
    FlushImmutableWals,
}

impl DbInner {
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        self.state.write().freeze_wal()?;
        self.flush_imm_wals().await?;
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
                        let result = this.flush().await;
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
                            WalFlushThreadMsg::FlushImmutableWals => {
                                let result = this.flush().await;
                                if let Err(err) = &result {
                                    error!("error from wal flush: {err}");
                                    return Err(err.clone());
                                }

                                if let Some(rsp_sender) = rsp_sender {
                                    let res = rsp_sender.send(result);
                                    if let Err(Err(err)) = res {
                                        error!("error sending flush response: {err}");
                                    }
                                }
                            },
                        }
                    }
                }
            }
        }

        let fut = async move {
            let result = core_flush_loop(&this, &mut rx).await;

            let pending_result = result.clone().and_then(|_| Err(BackgroundTaskShutdown));
            while !rx.is_empty() {
                let (rsp_sender, _) = rx.recv().await.expect("channel unexpectedly closed");
                if let Some(rsp_sender) = rsp_sender {
                    let _ = rsp_sender.send(pending_result.clone());
                }
            }

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
