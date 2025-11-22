use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::{CoreDbState, DbState, SsTableHandle, SsTableId};
use crate::dispatcher::{MessageFactory, MessageHandler};
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::manifest::Manifest;
use crate::mem_table::ImmutableMemtable;
use crate::transactional_object::view::{LocalView, LocalViewManager};
use crate::transactional_object::{DirtyObject, MonotonicId, TransactionalObject};
use crate::utils::IdGenerator;
use crate::Checkpoint;
use async_trait::async_trait;
use fail_parallel::fail_point;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use std::cmp;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Sender;
use tracing::instrument;

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

// TODO: maybe swap names with manifest view
// TODO: maybe change the name "view"
type ManifestViewManager = LocalViewManager<Manifest, LocalManifestView, FenceableManifest>;

pub(crate) struct MemtableFlusher {
    db_inner: Arc<DbInner>,
    manifest: ManifestViewManager,
}

impl MemtableFlusher {
    pub(crate) fn new(
        db_inner: Arc<DbInner>,
        manifest: FenceableManifest,
    ) -> Result<Self, SlateDBError> {
        let manifest =
            ManifestViewManager::new(LocalManifestView::new(manifest.prepare_dirty()?), manifest);
        Ok(Self { db_inner, manifest })
    }

    pub(crate) async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        self.db_inner
            .state
            .write()
            .update_manifest(Arc::new(self.manifest.view().manifest().clone()));
        Ok(())
    }

    async fn write_checkpoint(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let id = self.db_inner.rand.rng().gen_uuid();
        self.manifest
            .maybe_apply_update_to_view(|mut dirty| {
                let checkpoint = Checkpoint::new_for_manifest(
                    dirty.id(),
                    &dirty.value,
                    self.db_inner.system_clock.as_ref(),
                    id,
                    options,
                )?;
                dirty.value.core.checkpoints.push(checkpoint);
                Ok::<DirtyObject<Manifest>, SlateDBError>(dirty)
            })
            .await?;
        let manifest_id = self
            .manifest
            .view()
            .manifest()
            .core
            .checkpoints
            .iter()
            .find(|c| c.id == id)
            .expect("checkpoint written but could not find id")
            .manifest_id;
        Ok(CheckpointCreateResult { id, manifest_id })
    }

    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        let view = self.manifest.view_mut();
        view.set_next_wal_id(self.db_inner.wal_buffer.current_wal_id());
        view.set_min_active_snapshot_seq(self.db_inner.txn_manager.min_active_seq());
        self.manifest.sync().await?;
        self.db_inner
            .state
            .write()
            .update_manifest(Arc::new(self.manifest.view().manifest().clone()));
        Ok(())
    }

    fn replace_oldest_imm_with_new_l0(
        view: &mut LocalManifestView,
        db_state: &Arc<RwLock<DbState>>,
        imm_memtable: &Arc<ImmutableMemtable>,
        sst_handle: SsTableHandle,
        min_active_seq: Option<u64>,
    ) -> Result<(), SlateDBError> {
        view.push_new_l0(imm_memtable.clone(), sst_handle, min_active_seq)?;
        // Commit the manifest view into db state before actually writing the manifest.
        // The sst will be protected from gc because it won't delete any SSTs newer than
        // the last l0 in the latest durable manifest. The view still represents a
        // correct db. Currently this isn't providing much benefit since we will fail the
        // flush task on any errors. But if we add retry logic this lets us drop the
        // imm memtables from memory sooner
        let manifest = view.manifest().clone();
        db_state
            .write()
            .replace_oldest_imm_with_new_l0(imm_memtable, Arc::new(manifest.clone()));
        Ok(())
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
            let imm_memtable_last_seq = imm_memtable.table().last_seq().expect("empty imm table");
            assert!(imm_memtable_last_seq > self.manifest.view().manifest().core.last_l0_seq);
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

            // update the manifest
            let view = self.manifest.view_mut();
            view.set_next_wal_id(self.db_inner.wal_buffer.current_wal_id());
            Self::replace_oldest_imm_with_new_l0(
                view,
                &self.db_inner.state,
                &imm_memtable,
                sst_handle,
                self.db_inner.txn_manager.min_active_seq(),
            )?;

            let last_l0_seq = view.manifest().core.last_l0_seq;
            let result = self.manifest.sync().await.map_err(SlateDBError::from);
            match result {
                Ok(_) => {
                    // at this point we know the data in the memtable is durably stored
                    // so notify the relevant listeners
                    imm_memtable.notify_flush_to_l0(Ok(()));
                    imm_memtable.table().notify_durable(Ok(()));
                    self.db_inner
                        .oracle
                        .last_remote_persisted_seq
                        .store_if_greater(last_l0_seq);
                }
                Err(err) => {
                    if matches!(err, SlateDBError::Fenced) {
                        if let Err(delete_err) = self.db_inner.table_store.delete_sst(&id).await {
                            warn!(
                                "failed to delete fenced SST [id={:?}, error={:?}]",
                                id, delete_err
                            );
                        }
                        // refresh manifest and state so that local state reflects remote//
                        // ignore the error to preserve the fencing error
                        if let Err(load_err) = self.load_manifest().await {
                            warn!("failed to load manifest after being fenced: {:?}", load_err);
                        }
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

struct LocalManifestView {
    dirty_manifest: ReplaceOnly<DirtyObject<Manifest>>,
}

impl LocalManifestView {
    fn new(manifest: DirtyObject<Manifest>) -> Self {
        Self {
            dirty_manifest: ReplaceOnly::new(manifest),
        }
    }
}

impl LocalManifestView {
    fn manifest(&self) -> &Manifest {
        &self.dirty_manifest.get().value
    }

    fn set_next_wal_id(&mut self, next_wal_id: Option<u64>) {
        let mut dirty_manifest = self.dirty_manifest.write();
        if let Some(next_wal_id) = next_wal_id {
            dirty_manifest.value.core.next_wal_sst_id = next_wal_id;
        }
        dirty_manifest.commit();
    }

    fn set_min_active_snapshot_seq(&mut self, min_active_snapshot_seq: Option<u64>) {
        let mut dirty_manifest = self.dirty_manifest.write();
        // update the persisted manifest recent_snapshot_min_seq to inform the compactor
        // can safely reclaim the entries with smaller seq. if there's no active snapshot,
        // we simply use the latest l0 seq.
        dirty_manifest.value.core.recent_snapshot_min_seq =
            min_active_snapshot_seq.unwrap_or(dirty_manifest.value.core.last_l0_seq);
        dirty_manifest.commit();
    }

    fn push_new_l0(
        &mut self,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_handle: SsTableHandle,
        min_active_snapshot_seq: Option<u64>,
    ) -> Result<(), SlateDBError> {
        let mut dirty_manifest = self.dirty_manifest.write();
        let last_seq = imm_memtable
            .table()
            .last_seq()
            .expect("flush of l0 with no entries");
        dirty_manifest.value.core.l0.push_front(sst_handle);
        dirty_manifest.value.core.replay_after_wal_id = imm_memtable.recent_flushed_wal_id();

        // ensure the persisted manifest tick never goes backwards in time
        let memtable_tick = imm_memtable.table().last_tick();
        dirty_manifest.value.core.last_l0_clock_tick =
            cmp::max(dirty_manifest.value.core.last_l0_clock_tick, memtable_tick);
        if dirty_manifest.value.core.last_l0_clock_tick != memtable_tick {
            return Err(SlateDBError::InvalidClockTick {
                last_tick: dirty_manifest.value.core.last_l0_clock_tick,
                next_tick: memtable_tick,
            });
        }

        // update the persisted manifest last_l0_seq as the latest seq in the imm.
        assert!(last_seq >= dirty_manifest.value.core.last_l0_seq);
        dirty_manifest.value.core.last_l0_seq = last_seq;

        let sequence_tracker = imm_memtable.sequence_tracker();
        dirty_manifest
            .value
            .core
            .sequence_tracker
            .extend_from(sequence_tracker);

        dirty_manifest.commit();

        self.set_min_active_snapshot_seq(min_active_snapshot_seq);

        Ok(())
    }
}

impl LocalView<Manifest> for LocalManifestView {
    fn merge(&mut self, mut remote_manifest: DirtyObject<Manifest, MonotonicId>) {
        // The compactor removes tables from l0_last_compacted, so we
        // only want to keep the tables up to there.
        let dirty_manifest = self.dirty_manifest.get();
        let l0_last_compacted = &remote_manifest.value.core.l0_last_compacted;
        let new_l0 = if let Some(l0_last_compacted) = l0_last_compacted {
            dirty_manifest
                .value
                .core
                .l0
                .iter()
                .cloned()
                .take_while(|sst| sst.id.unwrap_compacted_id() != *l0_last_compacted)
                .collect()
        } else {
            dirty_manifest.value.core.l0.iter().cloned().collect()
        };

        let my_db_state = &dirty_manifest.value.core;
        remote_manifest.value.core = CoreDbState {
            initialized: my_db_state.initialized,
            l0_last_compacted: remote_manifest.value.core.l0_last_compacted,
            l0: new_l0,
            compacted: remote_manifest.value.core.compacted,
            next_wal_sst_id: my_db_state.next_wal_sst_id,
            replay_after_wal_id: my_db_state.replay_after_wal_id,
            last_l0_clock_tick: my_db_state.last_l0_clock_tick,
            last_l0_seq: my_db_state.last_l0_seq,
            recent_snapshot_min_seq: my_db_state.recent_snapshot_min_seq,
            sequence_tracker: remote_manifest.value.core.sequence_tracker,
            checkpoints: remote_manifest.value.core.checkpoints,
            wal_object_store_uri: my_db_state.wal_object_store_uri.clone(),
        };
        // TODO: this is weird
        let mut dirty_manifest = self.dirty_manifest.write();
        *dirty_manifest = remote_manifest;
        dirty_manifest.commit();
    }

    fn value(&self) -> &DirtyObject<Manifest, MonotonicId> {
        self.dirty_manifest.get()
    }
}

/// Wraps another value and only allows it to be written by fully replacing it. This is
/// useful for ensuring that code that mutates the value does so atomically in the sense
/// that for a mutation that requires updating multiple fields either all updates are
/// applied or none are applied.
pub struct ReplaceOnly<T: Clone> {
    inner: T,
}

impl<T: Clone> ReplaceOnly<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self { inner }
    }

    pub(crate) fn get(&self) -> &T {
        &self.inner
    }

    pub(crate) fn replace(&mut self, new: T) -> T {
        std::mem::replace(&mut self.inner, new)
    }

    pub fn write(&mut self) -> ReplaceOnlyGuard<T> {
        let new_value = self.inner.clone();
        ReplaceOnlyGuard {
            cell: self,
            new_value
        }
    }
}

struct ReplaceOnlyGuard<'a, T: Clone> {
    cell: &'a mut ReplaceOnly<T>,
    new_value: T,
}

impl <'a, T: Clone> ReplaceOnlyGuard<'a, T> {
    fn commit(self) {
        self.cell.replace(self.new_value);
    }
}

impl <'a, T: Clone> Deref for ReplaceOnlyGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.new_value
    }
}

impl <'a, T: Clone> DerefMut for ReplaceOnlyGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
       &mut self.new_value
    }
}
