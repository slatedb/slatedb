use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::{SsTableHandle, SsTableId};
use crate::dispatcher::{MessageFactory, MessageHandler};
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::transactional_object::TransactionalObject;
use crate::utils::IdGenerator;
use async_trait::async_trait;
use fail_parallel::fail_point;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::cmp;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Sender;
use tracing::instrument;
use crate::manifest::Manifest;
use crate::manifest;
use crate::mem_table::ImmutableMemtable;
use crate::transactional_object::{DirtyObject, TransactionalObject};

pub(crate) const MEMTABLE_FLUSHER_TASK_NAME: &str = "memtable_writer";

#[derive(Debug)]
pub(crate) enum MemtableFlushMsg {
    FlushImmutableMemtables {
        sender: Option<Sender<Result<(), SlateDBError>>>,
    },
    CreateCheckpoint {
        options: CheckpointOptions,
        sender: Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
    PollManifest,
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
        let manifest =  self.manifest.refresh().await?;
        self.db_inner.state.write().modify(
            |m| m.state.manifest = Arc::new(manifest.clone())
        );
        Ok(())
    }

    async fn write_checkpoint(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let id = self.db_inner.rand.rng().gen_uuid();
        let checkpoint = self.manifest.write_checkpoint(id, options).await?;
        let manifest_id = checkpoint.manifest_id;
        Ok(CheckpointCreateResult { id, manifest_id })
    }

    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        let wal_buffer = self.db_inner.wal_buffer.clone();
        let txn_manager = self.db_inner.txn_manager.clone();
        Ok(self.manifest.maybe_apply_update(
            |m| Ok(Some(
                ManifestUpdater::new(m.prepare_dirty().map_err(manifest::store::into_slatedb_error)?)
                    .with_next_wal_id(wal_buffer.current_wal_id())
                    .with_min_active_snapshot_seq(txn_manager.min_active_seq())
                    .finish()
            ))
        ).await?)
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
            let txn_manager = self.db_inner.txn_manager.clone();
            let wal_buffer = self.db_inner.wal_buffer.clone();
            // TODO: what if we get an error where we don't know if the update was
            //       successful or not?
            //       probably the safer thing to do is to maintain an in-memory view of the manifest
            //       and then push that into db_state
            let result = self.manifest.maybe_apply_update(
                |m| Ok(Some(
                    ManifestUpdater::new(m.prepare_dirty().map_err(manifest::store::into_slatedb_error)?)
                        .with_next_wal_id(wal_buffer.current_wal_id())
                        .with_new_l0(imm_memtable.clone(), sst_handle.clone())?
                        // its kind of annoying that this needs to be after the above to be set right
                        .with_min_active_snapshot_seq(txn_manager.min_active_seq())
                        .finish()
                ))
            ).await;
            match result {
                Ok(_) => {
                    // at this point we know the data in the memtable is durably stored
                    // so notify the relevant listeners
                    imm_memtable.notify_flush_to_l0(Ok(()));
                    imm_memtable.table().notify_durable(Ok(()));
                    let manifest = self.manifest.manifest();
                    self.db_inner
                        .oracle
                        .last_remote_persisted_seq
                        .store_if_greater(manifest.core.last_l0_seq);
                    {
                        let mut guard = self.db_inner.state.write();
                        guard.modify(
                            |m| {
                                // todo: do something nicer here
                                let popped = m.state.imm_memtable.pop_back().expect("");
                                assert!(Arc::ptr_eq(&imm_memtable, &popped));
                                m.state.manifest = Arc::new(manifest.clone());
                            }
                        )
                    }
                }
                Err(err) => {
                    if matches!(err, SlateDBError::Fenced) {
                        if let Err(delete_err) = self.db_inner.table_store.delete_sst(&id).await {
                            warn!(
                                "failed to delete fenced SST [id={:?}, error={:?}]",
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

    async fn flush_and_record(&mut self) -> Result<(), SlateDBError> {
        fail_point!(
            Arc::clone(&self.db_inner.fp_registry),
            "flush-memtable-to-l0"
        );
        let result = self.flush_imm_memtables_to_l0().await;
        if let Err(err) = &result {
            error!("error from memtable flush [error={:?}]", err);
        } else {
            self.db_inner.db_stats.immutable_memtable_flushes.inc();
        }
        result
    }
}

#[async_trait]
impl MessageHandler<MemtableFlushMsg> for MemtableFlusher {
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<MemtableFlushMsg>>)> {
        vec![(
            self.db_inner.settings.manifest_poll_interval,
            Box::new(|| MemtableFlushMsg::PollManifest),
        )]
    }

    async fn handle(&mut self, message: MemtableFlushMsg) -> Result<(), SlateDBError> {
        match message {
            MemtableFlushMsg::PollManifest => {
                self.load_manifest().await?;
                self.write_manifest().await?;
                self.flush_and_record().await
            }
            MemtableFlushMsg::FlushImmutableMemtables { sender } => {
                let result = self.flush_and_record().await;
                if let Some(rsp_sender) = sender {
                    let res = rsp_sender.send(result.clone());
                    if let Err(Err(err)) = res {
                        error!("error sending flush response [error={:?}]", err);
                    }
                }
                result
            }
            MemtableFlushMsg::CreateCheckpoint { options, sender } => {
                let write_result = self.write_checkpoint(&options).await;
                if let Err(Err(e)) = sender.send(write_result.clone()) {
                    error!("Failed to send checkpoint error [error={:?}]", e);
                }
                write_result.map(|_| ())
            }
        }
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, MemtableFlushMsg>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let error = result.clone().err().unwrap_or(SlateDBError::Closed);
        // drain remaining messages
        while let Some(message) = messages.next().await {
            match message {
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
        if let Err(err) = self.write_manifest().await {
            error!("error writing manifest on shutdown [err={}]", err);
        }
        info!("memtable flush thread exiting [result={:?}]", result);

        // notify in-memory memtables of error
        let state = self.db_inner.state.read();
        debug!(
            "notifying in-memory memtable of shutdown [result={:?}]",
            result
        );
        state.memtable().table().notify_durable(Err(error.clone()));
        for imm_table in state.state().imm_memtable.iter() {
            debug!(
                "notifying imm memtable of shutdown [last_wal_id={}, error={:?}]",
                imm_table.recent_flushed_wal_id(),
                error,
            );
            imm_table.notify_flush_to_l0(Err(error.clone()));
            imm_table.table().notify_durable(Err(error.clone()));
        }
        Ok(())
    }
}

struct ManifestUpdater {
    manifest: DirtyObject<Manifest>,
}

impl ManifestUpdater {
    fn new(manifest: DirtyObject<Manifest>) -> Self {
        Self { manifest }
    }

    fn with_next_wal_id(mut self, next_wal_id: Option<u64>) -> Self {
        if let Some(next_wal_id) = next_wal_id {
            self.manifest.value.core.next_wal_sst_id = next_wal_id;
        }
        self
    }

    fn with_min_active_snapshot_seq(mut self, min_active_snapshot_seq: Option<u64>) -> Self {
        // update the persisted manifest recent_snapshot_min_seq to inform the compactor
        // can safely reclaim the entries with smaller seq. if there's no active snapshot,
        // we simply use the latest l0 seq.
        self.manifest.value.core.recent_snapshot_min_seq =
            min_active_snapshot_seq.unwrap_or(self.manifest.value.core.last_l0_seq);
        self
    }

    fn with_new_l0(
        mut self,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_handle: SsTableHandle,
    ) -> Result<Self, SlateDBError> {
        let last_seq = imm_memtable
            .table()
            .last_seq()
            .expect("flush of l0 with no entries");
        self.manifest.value.core.l0.push_front(sst_handle);
        self.manifest.value.core.replay_after_wal_id = imm_memtable.recent_flushed_wal_id();

        // ensure the persisted manifest tick never goes backwards in time
        let memtable_tick = imm_memtable.table().last_tick();
        self.manifest.value.core.last_l0_clock_tick = cmp::max(
            self.manifest.value.core.last_l0_clock_tick,
            memtable_tick,
        );
        if self.manifest.value.core.last_l0_clock_tick != memtable_tick {
            return Err(SlateDBError::InvalidClockTick {
                last_tick: self.manifest.value.core.last_l0_clock_tick,
                next_tick: memtable_tick,
            });
        }

        // update the persisted manifest last_l0_seq as the latest seq in the imm.
        assert!(last_seq >= self.manifest.value.core.last_l0_seq);
        self.manifest.value.core.last_l0_seq = last_seq;

        let sequence_tracker = imm_memtable.sequence_tracker();
        self
            .manifest
            .value
            .core
            .sequence_tracker
            .extend_from(sequence_tracker);

        Ok(self)
    }

    fn finish(self) -> DirtyObject<Manifest> {
        self.manifest
    }
}
