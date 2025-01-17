use crate::checkpoint::{Checkpoint, CheckpointCreateResult};
use crate::db::{CheckpointMsg, DbInner, FlushMsg, FlushSender};
use crate::db_state::{CoreDbState, SsTableId};
use crate::error::SlateDBError;
use crate::error::SlateDBError::{BackgroundTaskShutdown, CheckpointMissing};
use crate::manifest_store::FenceableManifest;
use crate::utils;
use crate::utils::spawn_bg_task;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info, warn};
use ulid::Ulid;

#[derive(Debug)]
pub enum MemtableFlushThreadMsg {
    Shutdown,
    FlushImmutableMemtables { force_flush: bool },
}

pub(crate) struct MemtableFlusher {
    db_inner: Arc<DbInner>,
    manifest: FenceableManifest,
    flush_waiters: Vec<FlushSender>,
}

impl MemtableFlusher {
    pub(crate) async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        let core_state = self.manifest.refresh().await?;
        let mut wguard_state = self.db_inner.state.write();
        wguard_state.merge_db_state(core_state);
        Ok(())
    }

    async fn write_checkpoint(
        &mut self,
        checkpoint_msg: &CheckpointMsg,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let mut core = {
            let rguard_state = self.db_inner.state.read();
            rguard_state.state().core.clone()
        };

        let checkpoint = self.build_checkpoint(&core, checkpoint_msg)?;
        let result = CheckpointCreateResult {
            id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        core.checkpoints.push(checkpoint);
        self.manifest.update_db_state(core).await?;
        Ok(result)
    }

    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        let core = {
            let rguard_state = self.db_inner.state.read();
            rguard_state.state().core.clone()
        };
        self.manifest.update_db_state(core).await
    }

    fn build_checkpoint(
        &self,
        state: &CoreDbState,
        msg: &CheckpointMsg,
    ) -> Result<Checkpoint, SlateDBError> {
        let checkpoint_manifest_id = if let Some(source_id) = &msg.options.source {
            if let Some(checkpoint) = state.find_checkpoint(source_id) {
                checkpoint.manifest_id
            } else {
                return Err(CheckpointMissing(*source_id));
            }
        } else {
            self.manifest.next_manifest_id()
        };

        let expire_time = msg.options.lifetime.map(|l| SystemTime::now() + l);

        Ok(Checkpoint {
            id: msg.id,
            manifest_id: checkpoint_manifest_id,
            expire_time,
            create_time: SystemTime::now(),
        })
    }

    pub(crate) async fn write_checkpoint_safely(
        &mut self,
        checkpoint: &CheckpointMsg,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        loop {
            self.load_manifest().await?;
            let result = self.write_checkpoint(checkpoint).await;
            if matches!(result, Err(SlateDBError::ManifestVersionExists)) {
                error!("conflicting manifest version. retry write");
            } else {
                return result;
            }
        }
    }

    pub(crate) async fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.load_manifest().await?;
            let result = self.write_manifest().await;
            if matches!(result, Err(SlateDBError::ManifestVersionExists)) {
                error!("conflicting manifest version. retry write");
            } else {
                return result;
            }
        }
    }

    async fn flush(&mut self, complete_flush: bool) -> Result<(), SlateDBError> {
        let result = self.flush_imm_memtables_to_l0().await;
        if complete_flush {
            while let Some(sender) = self.flush_waiters.pop() {
                if let Err(Err(e)) = sender.send(result.clone()) {
                    error!("Failed to send flush error: {e}");
                }
            }
        }
        result
    }

    async fn flush_imm_memtables_to_l0(&mut self) -> Result<(), SlateDBError> {
        while let Some(imm_memtable) = {
            let rguard = self.db_inner.state.read();
            if rguard.state().core.l0.len() >= self.db_inner.options.l0_max_ssts {
                warn!(
                    "too many l0 files {} >= {}. Won't flush imm to l0",
                    rguard.state().core.l0.len(),
                    self.db_inner.options.l0_max_ssts
                );
                rguard.state().core.log_db_runs();
                None
            } else {
                rguard.state().imm_memtable.back().cloned()
            }
        } {
            let id = SsTableId::Compacted(Ulid::new());
            let sst_handle = self
                .db_inner
                .flush_imm_table(&id, imm_memtable.table())
                .await?;
            {
                let mut guard = self.db_inner.state.write();
                guard.move_imm_memtable_to_l0(imm_memtable.clone(), sst_handle);
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
        complete_flush: bool,
    ) -> Result<(), SlateDBError> {
        let result = flusher.flush(complete_flush).await;
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
        mut flush_rx: UnboundedReceiver<FlushMsg<MemtableFlushThreadMsg>>,
        mut checkpoint_rx: UnboundedReceiver<CheckpointMsg>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let this = Arc::clone(self);

        async fn core_flush_loop(
            this: &Arc<DbInner>,
            flusher: &mut MemtableFlusher,
            flush_rx: &mut UnboundedReceiver<FlushMsg<MemtableFlushThreadMsg>>,
            checkpoint_rx: &mut UnboundedReceiver<CheckpointMsg>,
        ) -> Result<(), SlateDBError> {
            let mut manifest_poll_interval =
                tokio::time::interval(this.options.manifest_poll_interval);
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
                        this.flush_and_record(flusher, false).await?
                    }
                    flush_msg = flush_rx.recv() => {
                        let (rsp_sender, msg) = flush_msg.expect("channel unexpectedly closed");
                        match msg {
                            MemtableFlushThreadMsg::Shutdown => {
                                return Ok(());
                            },
                            MemtableFlushThreadMsg::FlushImmutableMemtables { force_flush } => {
                                if let Some(sender) = rsp_sender {
                                    flusher.flush_waiters.push(sender);
                                }
                                if force_flush {
                                    this.flush_and_record(flusher, true).await?;
                                }
                            }
                        }
                    },
                    checkpoint_msg = checkpoint_rx.recv() => {
                        let checkpoint_msg = checkpoint_msg.expect("channel unexpectedly closed");
                        let write_result = flusher.write_checkpoint_safely(&checkpoint_msg).await;
                        if let Err(Err(e)) = checkpoint_msg.sender.send(write_result) {
                            error!("Failed to send checkpoint error: {e}");
                        }
                    }
                }
            }
        }

        let fut = async move {
            let mut flusher = MemtableFlusher {
                db_inner: this.clone(),
                manifest,
                flush_waiters: Vec::new(),
            };

            // Stop the loop when the shut down has been received *and* all
            // remaining `rx` flushes and checkpoints have been drained.
            let result =
                core_flush_loop(&this, &mut flusher, &mut flush_rx, &mut checkpoint_rx).await;

            // respond to any pending msgs
            let pending_error = result.clone().err().unwrap_or(BackgroundTaskShutdown);
            utils::close_and_drain_receiver(&mut flush_rx, &pending_error).await;
            utils::drain_sender_queue(&mut flusher.flush_waiters, &pending_error);
            Self::close_and_drain_checkpoint_receiver(&mut checkpoint_rx, &pending_error).await;

            if let Err(err) = flusher.write_manifest_safely().await {
                error!("error writing manifest on shutdown: {}", err);
            }

            info!("memtable flush thread exiting with {:?}", result);
            result
        };

        let this = Arc::clone(self);
        Some(spawn_bg_task(
            tokio_handle,
            move |err| {
                warn!("memtable flush task exited with {:?}", err);
                // notify any waiters that the task has exited
                let mut state = this.state.write();
                state.record_fatal_error(err.clone());
                info!("notifying in-memory memtable of error");
                state.memtable().table().notify_durable(Err(err.clone()));
                for imm_table in state.state().imm_memtable.iter() {
                    info!(
                        "notifying imm memtable (last_wal_id={}) of error",
                        imm_table.last_wal_id()
                    );
                    imm_table.notify_flush_to_l0(Err(err.clone()));
                    imm_table.table().notify_durable(Err(err.clone()));
                }
            },
            fut,
        ))
    }

    async fn close_and_drain_checkpoint_receiver(
        rx: &mut UnboundedReceiver<CheckpointMsg>,
        error: &SlateDBError,
    ) {
        rx.close();
        while !rx.is_empty() {
            let msg = rx.recv().await.expect("channel unexpectedly closed");
            let _ = msg.sender.send(Err(error.clone()));
        }
    }
}
