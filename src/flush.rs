use log::warn;
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;
use tracing::{error, info};

use crate::db::DbInner;
use crate::db_state;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskShutdown;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable, WritableKVTable};
use crate::utils::{bg_task_result_into_err, spawn_bg_task};

#[derive(Debug)]
pub(crate) enum WalFlushMsg {
    Shutdown,
    FlushImmutableWals {
        sender: Option<Sender<Result<(), SlateDBError>>>,
    },
}

impl DbInner {
    async fn flush(&self) -> Result<(), SlateDBError> {
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

        self.mono_clock
            .fetch_max_last_durable_tick(imm_table.last_tick());
        Ok(handle)
    }

    async fn flush_imm_wal(
        &self,
        id: u64,
        imm: Arc<ImmutableWal>,
    ) -> Result<SsTableHandle, SlateDBError> {
        let wal_id = db_state::SsTableId::Wal(id);
        self.flush_imm_table(&wal_id, imm.table()).await
    }

    fn flush_imm_wal_to_memtable(&self, mem_table: &mut WritableKVTable, imm_table: Arc<KVTable>) {
        let mut iter = imm_table.iter();
        while let Some(kv) = iter.next_entry_sync() {
            mem_table.put(kv);
        }
    }

    async fn flush_imm_wals(&self) -> Result<(), SlateDBError> {
        while let Some((imm, id)) = {
            let rguard = self.state.read();
            let state = rguard.state();
            state
                .imm_wal
                .back()
                .cloned()
                .map(|imm| (imm, state.core().next_wal_sst_id))
        } {
            self.flush_imm_wal(id, imm.clone()).await?;
            let mut wguard = self.state.write();
            wguard.pop_imm_wal();
            wguard.increment_next_wal_id();
            // flush to the memtable before notifying so that data is available for reads
            self.flush_imm_wal_to_memtable(wguard.memtable(), imm.table());
            self.maybe_freeze_memtable(&mut wguard, id)?;
            imm.table().notify_durable(Ok(()));
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_task(
        self: &Arc<Self>,
        mut rx: UnboundedReceiver<WalFlushMsg>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let this = Arc::clone(self);
        async fn core_flush_loop(
            this: &Arc<DbInner>,
            rx: &mut UnboundedReceiver<WalFlushMsg>,
        ) -> Result<(), SlateDBError> {
            let Some(period) = this.options.flush_interval else {
                // If flush_interval is not set, we do not start the flush task.
                return Ok(())
            };

            let mut ticker = tokio::time::interval(period);
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
                        match msg.expect("channel unexpectedly closed") {
                            WalFlushMsg::Shutdown => {
                                // Stop the thread.
                                _ = this.flush().await;
                                return Ok(())
                            },
                            WalFlushMsg::FlushImmutableWals { sender } => {
                                let result = this.flush().await;
                                if let Err(err) = result {
                                    error!("error from wal flush: {err}");
                                    return Err(err);
                                }

                                if let Some(rsp_sender) = sender {
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
            let error = result.clone().err().unwrap_or(BackgroundTaskShutdown);
            Self::close_and_drain_receiver(&mut rx, &error).await;
            info!("wal flush thread exiting with {:?}", result);
            result
        };

        let this = Arc::clone(self);
        Some(spawn_bg_task(
            tokio_handle,
            move |result| {
                let err = bg_task_result_into_err(result);
                warn!("flush task exited with {:?}", err);
                // notify any waiters about the failure
                let mut state = this.state.write();
                state.record_fatal_error(err.clone());
                info!("notifying writeable wal of error");
                state.wal().table().notify_durable(Err(err.clone()));
                info!("notifying immutable wals of error");
                for imm in state.snapshot().state.imm_wal.iter() {
                    imm.table().notify_durable(Err(err.clone()));
                }
            },
            fut,
        ))
    }

    async fn close_and_drain_receiver(
        rx: &mut UnboundedReceiver<WalFlushMsg>,
        error: &SlateDBError,
    ) {
        rx.close();
        while !rx.is_empty() {
            let msg = rx.recv().await.expect("channel unexpectedly closed");
            if let WalFlushMsg::FlushImmutableWals {
                sender: Some(sender),
            } = msg
            {
                let _ = sender.send(Err(error.clone()));
            }
        }
    }
}
