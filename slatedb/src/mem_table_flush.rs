use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskShutdown;
use crate::manifest::store::FenceableManifest;
use crate::utils::{bg_task_result_into_err, spawn_bg_task, IdGenerator};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;
use tracing::{error, info, warn};

#[derive(Debug)]
pub(crate) enum MemtableFlushMsg {
    FlushImmutableMemtables {
        sender: Option<Sender<Result<(), SlateDBError>>>,
    },
    CreateCheckpoint {
        options: CheckpointOptions,
        sender: Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
    Shutdown,
}

pub(crate) struct MemtableFlusher {
    db_inner: Arc<DbInner>,
    manifest: FenceableManifest,
}

impl MemtableFlusher {
    pub(crate) async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        let mut wguard_state = self.db_inner.state.write();
        wguard_state.merge_remote_manifest(self.manifest.prepare_dirty()?);
        Ok(())
    }

    async fn write_checkpoint(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let mut dirty = {
            let rguard_state = self.db_inner.state.read();
            rguard_state.state().manifest.clone()
        };
        let id = self.db_inner.db_context.thread_rng().gen_uuid();
        let checkpoint = self.manifest.new_checkpoint(id, options)?;
        let manifest_id = checkpoint.manifest_id;
        dirty.core.checkpoints.push(checkpoint);
        self.manifest.update_manifest(dirty).await?;
        Ok(CheckpointCreateResult { id, manifest_id })
    }

    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        let dirty = {
            let rguard_state = self.db_inner.state.read();
            rguard_state.state().manifest.clone()
        };
        self.manifest.update_manifest(dirty).await
    }

    pub(crate) async fn write_checkpoint_safely(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        loop {
            self.load_manifest().await?;
            let result = self.write_checkpoint(options).await;
            if matches!(result, Err(SlateDBError::ManifestVersionExists)) {
                warn!("conflicting manifest version. updating and retrying write again.");
            } else {
                return result;
            }
        }
    }

    pub(crate) async fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            let result = self.write_manifest().await;
            if matches!(result, Err(SlateDBError::ManifestVersionExists)) {
                warn!("conflicting manifest version. updating and retrying write again.");
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    async fn flush_imm_memtables_to_l0(&mut self) -> Result<(), SlateDBError> {
        while let Some(imm_memtable) = {
            let rguard = self.db_inner.state.read();
            if rguard.state().core().l0.len() >= self.db_inner.settings.l0_max_ssts {
                warn!(
                    "too many l0 files {} >= {}. Won't flush imm to l0",
                    rguard.state().core().l0.len(),
                    self.db_inner.settings.l0_max_ssts
                );
                rguard.state().core().log_db_runs();
                None
            } else {
                rguard.state().imm_memtable.back().cloned()
            }
        } {
            let id = SsTableId::Compacted(self.db_inner.db_context.thread_rng().gen_ulid());
            let sst_handle = self
                .db_inner
                .flush_imm_table(&id, imm_memtable.table(), true)
                .await?;
            {
                let mut guard = self.db_inner.state.write();
                guard.move_imm_memtable_to_l0(imm_memtable.clone(), sst_handle)?;
            }
            imm_memtable.notify_flush_to_l0(Ok(()));
            self.write_manifest_safely().await?;
            imm_memtable.table().notify_durable(Ok(()));
        }
        Ok(())
    }
}

impl DbInner {
    async fn flush_and_record(
        self: &Arc<Self>,
        flusher: &mut MemtableFlusher,
    ) -> Result<(), SlateDBError> {
        let result = flusher.flush_imm_memtables_to_l0().await;
        if let Err(err) = &result {
            error!("error from memtable flush: {err}");
        } else {
            self.db_stats.immutable_memtable_flushes.inc();
        }
        result
    }

    pub(crate) fn spawn_memtable_flush_task(
        self: &Arc<Self>,
        manifest: FenceableManifest,
        mut flush_rx: UnboundedReceiver<MemtableFlushMsg>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let this = Arc::clone(self);

        async fn core_flush_loop(
            this: &Arc<DbInner>,
            flusher: &mut MemtableFlusher,
            flush_rx: &mut UnboundedReceiver<MemtableFlushMsg>,
        ) -> Result<(), SlateDBError> {
            let mut manifest_poll_interval =
                tokio::time::interval(this.settings.manifest_poll_interval);
            let mut err_reader = this.state.read().error_reader();

            // Stop the loop when the shut down has been received *and* all
            // remaining `rx` flushes have been drained.
            loop {
                tokio::select! {
                    err = err_reader.await_value() => {
                        return Err(err);
                    }
                    _ = manifest_poll_interval.tick() => {
                        if let Err(err) = flusher.load_manifest().await {
                            error!("error loading manifest: {err}");
                            return Err(err);
                        }
                        this.flush_and_record(flusher).await?
                    }
                    flush_msg = flush_rx.recv() => {
                        let msg = flush_msg.expect("channel unexpectedly closed");
                        match msg {
                            MemtableFlushMsg::Shutdown => {
                                return Ok(());
                            },
                            MemtableFlushMsg::FlushImmutableMemtables { sender} => {
                                this.flush_and_record(flusher).await?;
                                if let Some(rsp_sender) = sender {
                                    let res = rsp_sender.send(Ok(()));
                                    if let Err(Err(err)) = res {
                                        error!("error sending flush response: {err}");
                                    }
                                }
                            },
                            MemtableFlushMsg::CreateCheckpoint { options, sender } => {
                                let write_result = flusher.write_checkpoint_safely(&options).await;
                                if let Err(Err(e)) = sender.send(write_result) {
                                    error!("Failed to send checkpoint error: {e}");
                                }
                            }
                        }
                    },
                }
            }
        }

        let fut = async move {
            let mut flusher = MemtableFlusher {
                db_inner: this.clone(),
                manifest,
            };

            // Stop the loop when the shut down has been received *and* all
            // remaining `rx` flushes and checkpoints have been drained.
            let result = core_flush_loop(&this, &mut flusher, &mut flush_rx).await;

            // respond to any pending msgs
            let pending_error = result.clone().err().unwrap_or(BackgroundTaskShutdown);
            Self::drain_messages(&mut flush_rx, &pending_error).await;

            if let Err(err) = flusher.write_manifest_safely().await {
                error!("error writing manifest on shutdown: {}", err);
            }

            info!("memtable flush thread exiting with {:?}", result);
            result
        };

        let this = Arc::clone(self);
        Some(spawn_bg_task(
            tokio_handle,
            move |result| {
                let err = bg_task_result_into_err(result);
                warn!("memtable flush task exited with {:?}", err);
                // notify any waiters that the task has exited
                let mut state = this.state.write();
                state.record_fatal_error(err.clone());
                info!("notifying in-memory memtable of error");
                state.memtable().table().notify_durable(Err(err.clone()));
                for imm_table in state.state().imm_memtable.iter() {
                    info!(
                        "notifying imm memtable (last_wal_id={}) of error",
                        imm_table.recent_flushed_wal_id()
                    );
                    imm_table.notify_flush_to_l0(Err(err.clone()));
                    imm_table.table().notify_durable(Err(err.clone()));
                }
            },
            fut,
        ))
    }

    async fn drain_messages(rx: &mut UnboundedReceiver<MemtableFlushMsg>, error: &SlateDBError) {
        rx.close();
        while !rx.is_empty() {
            let msg = rx.recv().await.expect("channel unexpectedly closed");
            match msg {
                MemtableFlushMsg::CreateCheckpoint { options: _, sender } => {
                    let _ = sender.send(Err(error.clone()));
                }
                MemtableFlushMsg::FlushImmutableMemtables {
                    sender: Some(sender),
                } => {
                    let _ = sender.send(Err(error.clone()));
                }
                _ => (),
            }
        }
    }
}
