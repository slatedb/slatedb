use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info, warn};
use ulid::Ulid;

use crate::db::{DbInner, FlushMsg};
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskShutdown;
use crate::manifest_store::FenceableManifest;
use crate::utils::spawn_bg_task;

#[derive(Debug)]
pub enum MemtableFlushThreadMsg {
    Shutdown,
    FlushImmutableMemtables,
}

pub(crate) struct MemtableFlusher {
    db_inner: Arc<DbInner>,
    manifest: FenceableManifest,
}

impl MemtableFlusher {
    pub(crate) async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        let current_manifest = self.manifest.refresh().await?;
        let mut wguard_state = self.db_inner.state.write();
        wguard_state.refresh_db_state(current_manifest);
        Ok(())
    }

    pub(crate) async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        let core = {
            let rguard_state = self.db_inner.state.read();
            rguard_state.state().core.clone()
        };
        self.manifest.update_db_state(core).await
    }

    pub(crate) async fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.load_manifest().await?;
            match self.write_manifest().await {
                Ok(_) => return Ok(()),
                Err(SlateDBError::ManifestVersionExists) => {
                    error!("conflicting manifest version. retry write");
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub(crate) async fn flush_imm_memtables_to_l0(&mut self) -> Result<(), SlateDBError> {
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
    pub(crate) fn spawn_memtable_flush_task(
        self: &Arc<Self>,
        manifest: FenceableManifest,
        mut rx: UnboundedReceiver<FlushMsg<MemtableFlushThreadMsg>>,
        tokio_handle: &Handle,
    ) -> Option<tokio::task::JoinHandle<Result<(), SlateDBError>>> {
        let this = Arc::clone(self);

        async fn core_flush_loop(
            this: &Arc<DbInner>,
            flusher: &mut MemtableFlusher,
            rx: &mut UnboundedReceiver<FlushMsg<MemtableFlushThreadMsg>>
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
                        match flusher.flush_imm_memtables_to_l0().await {
                            Ok(_) => {
                                this.db_stats.immutable_memtable_flushes.inc();
                            }
                            Err(err) => {
                                error!("error from memtable flush: {err}");
                                return Err(err);
                            }
                        }
                    }
                    msg = rx.recv() => {
                        let (rsp_sender, msg) = msg.expect("channel unexpectedly closed");
                        match msg {
                            MemtableFlushThreadMsg::Shutdown => {
                                return Ok(());
                            },
                            MemtableFlushThreadMsg::FlushImmutableMemtables => {
                                let result = flusher.flush_imm_memtables_to_l0().await;
                                if let Some(rsp_sender) = rsp_sender {
                                    let res = rsp_sender.send(result.clone());
                                    if let Err(Err(err)) = res {
                                        error!("error sending flush response: {err}");
                                    }
                                }
                                match result {
                                    Ok(_) => {
                                        this.db_stats.immutable_memtable_flushes.inc();
                                    }
                                    Err(err) => {
                                        error!("error from memtable flush: {err}");
                                        return Err(err);
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }

        let fut = async move {
            let mut flusher = MemtableFlusher {
                db_inner: this.clone(),
                manifest,
            };

            // Stop the loop when the shut down has been received *and* all
            // remaining `rx` flushes have been drained.
            let result = core_flush_loop(&this, &mut flusher, &mut rx).await;

            // respond to any pending msgs
            let pending_result = result.clone().and_then(|_| Err(BackgroundTaskShutdown));
            while !rx.is_empty() {
                let (rsp_sender, _) = rx.recv().await.expect("channel unexpectedly closed");
                if let Some(rsp_sender) = rsp_sender {
                    let _ = rsp_sender.send(pending_result.clone());
                }
            }

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
}
