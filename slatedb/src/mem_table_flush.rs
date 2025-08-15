use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::SsTableId;
use crate::dispatcher::{MessageFactory, MessageHandler};
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::utils::IdGenerator;
use async_trait::async_trait;
use log::{error, info, warn};
use std::cmp;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Sender;
use tracing::instrument;

#[derive(Debug)]
pub(crate) enum MemtableMessage {
    PollManifest,
    FlushImmutableMemtables {
        sender: Option<Sender<Result<(), SlateDBError>>>,
    },
    CreateCheckpoint {
        options: CheckpointOptions,
        sender: Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
}

pub(crate) struct MemtableFlusher {
    db_inner: Arc<DbInner>,
    manifest: FenceableManifest,
}

impl MemtableFlusher {
    pub(crate) fn new(db_inner: Arc<DbInner>, manifest: FenceableManifest) -> Self {
        Self { db_inner, manifest }
    }

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
        let id = self.db_inner.rand.rng().gen_uuid();
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

    #[instrument(level = "trace", skip_all)]
    async fn flush_imm_memtables_to_l0(&mut self) -> Result<(), SlateDBError> {
        while let Some(imm_memtable) = {
            let rguard = self.db_inner.state.read();
            if rguard.state().core().l0.len() >= self.db_inner.settings.l0_max_ssts {
                warn!(
                    "won't flush imm to l0 because too many l0 files [l0_len={}, l0_max_ssts={}]",
                    rguard.state().core().l0.len(),
                    self.db_inner.settings.l0_max_ssts
                );
                rguard.state().core().log_db_runs();
                None
            } else {
                rguard.state().imm_memtable.back().cloned()
            }
        } {
            let id = SsTableId::Compacted(
                self.db_inner
                    .rand
                    .rng()
                    .gen_ulid(self.db_inner.system_clock.as_ref()),
            );
            let sst_handle = self
                .db_inner
                .flush_imm_table(&id, imm_memtable.table(), true)
                .await?;
            {
                let mut guard = self.db_inner.state.write();
                guard.modify(|modifier| {
                    let popped = modifier
                        .state
                        .imm_memtable
                        .pop_back()
                        .expect("expected imm memtable");
                    assert!(Arc::ptr_eq(&popped, &imm_memtable));
                    modifier.state.manifest.core.l0.push_front(sst_handle);
                    modifier.state.manifest.core.replay_after_wal_id =
                        imm_memtable.recent_flushed_wal_id();

                    // ensure the persisted manifest tick never goes backwards in time
                    let memtable_tick = imm_memtable.table().last_tick();
                    modifier.state.manifest.core.last_l0_clock_tick = cmp::max(
                        modifier.state.manifest.core.last_l0_clock_tick,
                        memtable_tick,
                    );
                    if modifier.state.manifest.core.last_l0_clock_tick != memtable_tick {
                        return Err(SlateDBError::InvalidClockTick {
                            last_tick: modifier.state.manifest.core.last_l0_clock_tick,
                            next_tick: memtable_tick,
                        });
                    }

                    // update the persisted manifest last_l0_seq as the latest seq in the imm.
                    if let Some(seq) = imm_memtable.table().last_seq() {
                        modifier.state.manifest.core.last_l0_seq = seq;
                    };

                    Ok(())
                })?;
            }
            imm_memtable.notify_flush_to_l0(Ok(()));
            match self.write_manifest_safely().await {
                Ok(_) => {
                    imm_memtable.table().notify_durable(Ok(()));
                }
                Err(err) => {
                    if matches!(err, SlateDBError::Fenced) {
                        if let Err(delete_err) = self.db_inner.table_store.delete_sst(&id).await {
                            warn!(
                                "failed to delete fenced SST [id={:?}, error={}]",
                                id, delete_err
                            );
                        }
                        // refresh manifest and state so that local state reflects remote
                        self.load_manifest().await?;
                    }
                    imm_memtable.table().notify_durable(Err(err.clone()));
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    async fn flush_and_record(self: &mut MemtableFlusher) -> Result<(), SlateDBError> {
        let result = self.flush_imm_memtables_to_l0().await;
        if let Err(err) = &result {
            error!("error from memtable flush [error={}]", err);
        } else {
            self.db_inner.db_stats.immutable_memtable_flushes.inc();
        }
        result
    }
}

#[async_trait]
impl MessageHandler<MemtableMessage> for MemtableFlusher {
    async fn handle(
        &mut self,
        message: MemtableMessage,
        error: Option<SlateDBError>,
    ) -> Result<(), SlateDBError> {
        match (message, error) {
            (MemtableMessage::PollManifest, _) => {
                if let Err(err) = self.load_manifest().await {
                    error!("error loading manifest [error={}]", err);
                    return Err(err);
                }
                self.flush_and_record().await?
            }
            (MemtableMessage::FlushImmutableMemtables { sender }, None) => {
                self.flush_and_record().await?;
                if let Some(rsp_sender) = sender {
                    let res = rsp_sender.send(Ok(()));
                    if let Err(Err(err)) = res {
                        error!("error sending flush response [error={}]", err);
                    }
                }
            }
            (
                MemtableMessage::FlushImmutableMemtables {
                    sender: Some(sender),
                },
                Some(e),
            ) => {
                let _ = sender.send(Err(e));
            }
            (MemtableMessage::CreateCheckpoint { options, sender }, None) => {
                let write_result = self.write_checkpoint_safely(&options).await;
                if let Err(Err(e)) = sender.send(write_result) {
                    error!("Failed to send checkpoint error [error={}]", e);
                }
            }
            (MemtableMessage::CreateCheckpoint { options: _, sender }, Some(e)) => {
                let _ = sender.send(Err(e));
            }
            // skip flush immutable memtables on error if sender is None
            _ => {}
        }
        Ok(())
    }

    async fn cleanup(&mut self, error: Option<SlateDBError>) -> Result<(), SlateDBError> {
        if let Err(err) = self.write_manifest_safely().await {
            error!("error writing manifest on shutdown [err={}]", err);
        }
        let err = Err(error.unwrap_or(SlateDBError::BackgroundTaskShutdown));
        let mut state = self.db_inner.state.write();
        state.memtable().table().notify_durable(err.clone());
        for imm_table in state.state().imm_memtable.iter() {
            info!(
                "notifying imm memtable of error [last_wal_id={}, error={:?}]",
                imm_table.recent_flushed_wal_id(),
                err,
            );
            imm_table.notify_flush_to_l0(err.clone());
            imm_table.table().notify_durable(err.clone());
        }
        Ok(())
    }

    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<MemtableMessage>>)> {
        vec![(
            self.db_inner.settings.manifest_poll_interval,
            Box::new(|| MemtableMessage::PollManifest),
        )]
    }
}
