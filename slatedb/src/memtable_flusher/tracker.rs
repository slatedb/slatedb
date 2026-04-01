//! Flush tracker — the event loop that coordinates the parallel L0 flush pipeline.
//!
//! The tracker owns:
//! - immutable-memtable frontier capture
//! - upload dispatch policy
//! - coordination between uploader and manifest_writer
//!
//! It does not own:
//! - flush request semantics (see [`super::MemtableFlusher`])
//! - SST build/upload execution
//! - manifest mutation
//! - manifest durability sequencing

use log::debug;

use super::FlushEpoch;
use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::error::SlateDBError;
use crate::memtable_flusher::manifest_writer::{
    FlushResult, ManifestWriter, ManifestWriterCloseResult, ManifestWriterEvent,
};
use crate::memtable_flusher::uploader::{UploadJob, UploadedMemtable, Uploader};
use crate::memtable_flusher::{FlushCommand, FlushTarget};
use crate::oracle::Oracle;
use crate::utils::safe_async_channel;
use crate::utils::IdGenerator;
use fail_parallel::fail_point;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(super) struct FlushTracker {
    inner: Arc<DbInner>,
    uploader: Uploader,
    manifest_writer: ManifestWriter,
    commands: safe_async_channel::SafeReceiver<FlushCommand>,
    next_epoch: FlushEpoch,
    tracked_imms: VecDeque<TrackedImm>,
}

impl FlushTracker {
    pub(super) fn new(
        inner: Arc<DbInner>,
        uploader: Uploader,
        manifest_writer: ManifestWriter,
        commands: safe_async_channel::SafeReceiver<FlushCommand>,
    ) -> Self {
        Self {
            inner,
            uploader,
            manifest_writer,
            commands,
            next_epoch: FlushEpoch(1),
            tracked_imms: VecDeque::new(),
        }
    }

    pub(super) async fn run(mut self) -> Result<(), SlateDBError> {
        loop {
            match self.run_once().await {
                Ok(true) => continue,
                Ok(false) => return self.shutdown(None).await,
                Err(err) => return self.shutdown(Some(err)).await,
            }
        }
    }

    async fn run_once(&mut self) -> Result<bool, SlateDBError> {
        tokio::select! {
            maybe_command = self.commands.recv() => {
                if let Some(command) = maybe_command {
                    self.handle_command(command).await
                } else {
                    Ok(false)
                }
            }
            maybe_uploaded = self.uploader.events().recv() => {
                if let Some(uploaded) = maybe_uploaded {
                    self.handle_uploaded(uploaded).await?;
                    Ok(true)
                } else {
                    Err(SlateDBError::Closed)
                }
            }
            maybe_event = self.manifest_writer.events().recv() => {
                if let Some(event) = maybe_event {
                    self.handle_manifest_writer_event(event).await?;
                    Ok(true)
                } else {
                    Err(SlateDBError::Closed)
                }
            }
        }
    }

    async fn handle_command(&mut self, command: FlushCommand) -> Result<bool, SlateDBError> {
        match command {
            FlushCommand::Flush { target, sender } => {
                fail_point!(
                    Arc::clone(&self.inner.fp_registry),
                    "flush-memtable-to-l0",
                    |_| { Ok(true) }
                );
                self.register_new_imm_memtables();
                self.handle_flush_request(target, sender)?;
                self.reconcile_and_dispatch().await?;
                Ok(true)
            }
            FlushCommand::CreateCheckpoint {
                target,
                options,
                sender,
            } => {
                self.handle_checkpoint_request(target, options, sender)?;
                self.reconcile_and_dispatch().await?;
                Ok(true)
            }
        }
    }

    fn handle_flush_request(
        &mut self,
        target: FlushTarget,
        sender: Option<oneshot::Sender<Result<FlushResult, SlateDBError>>>,
    ) -> Result<(), SlateDBError> {
        if let Some(sender) = sender {
            let through_epoch = self.resolve_target(target);
            self.manifest_writer.send_flush(through_epoch, sender)?;
        }
        Ok(())
    }

    fn handle_checkpoint_request(
        &mut self,
        target: FlushTarget,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        let through_epoch = self.resolve_target(target);
        self.manifest_writer
            .send_checkpoint(through_epoch, options, sender)
    }

    /// Resolves a flush target to the epoch that must become durable,
    /// or `None` for immediate resolution.
    fn resolve_target(&mut self, target: FlushTarget) -> Option<FlushEpoch> {
        self.register_new_imm_memtables();
        match target {
            FlushTarget::CurrentDurable => None,
            FlushTarget::BestEffort => self
                .tracked_imms
                .iter()
                .filter(|tracked| matches!(tracked.state, TrackedImmState::PendingDispatch))
                .take(self.available_l0_slots())
                .last()
                .map(|tracked| tracked.epoch),
            FlushTarget::All => self.tracked_imms.back().map(|tracked| tracked.epoch),
        }
    }

    async fn handle_uploaded(&mut self, uploaded: UploadedMemtable) -> Result<(), SlateDBError> {
        debug!(
            "l0 upload completed [epoch={:?}, sst_id={:?}]",
            uploaded.epoch, uploaded.sst_handle.id
        );
        self.set_tracked_state(uploaded.epoch, TrackedImmState::WritingManifest);
        self.manifest_writer.notify_uploaded(uploaded).await?;
        Ok(())
    }

    async fn handle_manifest_writer_event(
        &mut self,
        event: ManifestWriterEvent,
    ) -> Result<(), SlateDBError> {
        match event {
            ManifestWriterEvent::Flushed { through_epoch } => {
                self.retire_through(through_epoch);
                self.reconcile_and_dispatch().await
            }
            ManifestWriterEvent::ManifestRefreshed => self.reconcile_and_dispatch().await,
        }
    }

    /// Check for newly frozen immutable memtables and dispatch any that are ready.
    /// New IMMs are typically announced through flush commands (e.g. from the write
    /// path), but we re-check on every event to also pick up L0 slots freed by
    /// compaction via manifest refresh.
    async fn reconcile_and_dispatch(&mut self) -> Result<(), SlateDBError> {
        self.register_new_imm_memtables();
        self.dispatch_ready_memtables().await
    }

    fn register_new_imm_memtables(&mut self) {
        let guard = self.inner.state.read();
        for imm_memtable in guard.state().imm_memtable.iter().rev() {
            // TODO: Arc::as_ptr identity is fragile — it assumes the same Arc is never
            // re-inserted after removal. Assigning an SsTableId at memtable freeze would
            // give us a stable identifier to dedup against.
            let ptr = Arc::as_ptr(imm_memtable) as usize;
            if self.tracked_imms.iter().any(|tracked| tracked.ptr == ptr) {
                continue;
            }
            let sst_id = crate::db_state::SsTableId::Compacted(
                self.inner
                    .rand
                    .rng()
                    .gen_ulid(self.inner.system_clock.as_ref()),
            );
            self.tracked_imms.push_back(TrackedImm {
                epoch: self.next_epoch,
                ptr,
                sst_id,
                imm_memtable: Arc::clone(imm_memtable),
                state: TrackedImmState::PendingDispatch,
            });
            self.next_epoch = FlushEpoch(self.next_epoch.0 + 1);
        }
    }

    fn reserved_l0_slots(&self) -> usize {
        self.tracked_imms
            .iter()
            .filter(|tracked| {
                matches!(
                    tracked.state,
                    TrackedImmState::Uploading | TrackedImmState::WritingManifest
                )
            })
            .count()
    }

    fn available_l0_slots(&self) -> usize {
        let l0_len = self.inner.state.read().state().core().l0.len();
        self.inner
            .settings
            .l0_max_ssts
            .saturating_sub(l0_len + self.reserved_l0_slots())
    }

    async fn dispatch_ready_memtables(&mut self) -> Result<(), SlateDBError> {
        while self.available_l0_slots() > 0 {
            let Some(tracked) = self.prepare_next_upload() else {
                return Ok(());
            };
            let epoch = tracked.epoch;
            let sst_id = tracked.sst_id;
            let imm_memtable = Arc::clone(&tracked.imm_memtable);
            let last_seq = tracked.imm_memtable.table().last_seq().unwrap_or(0);
            debug!(
                "dispatching l0 upload [epoch={:?}, sst_id={:?}]",
                epoch, sst_id
            );

            // WAL SSTs must be durable before the L0 is uploaded (see #1255).
            if self.inner.wal_enabled && self.inner.oracle.last_remote_persisted_seq() < last_seq {
                self.inner.flush_wals().await?;
            }
            self.uploader
                .submit(UploadJob::new(epoch, imm_memtable, sst_id))
                .await?;
        }
        Ok(())
    }

    fn prepare_next_upload(&mut self) -> Option<&TrackedImm> {
        let index = self
            .tracked_imms
            .iter()
            .position(|tracked| matches!(tracked.state, TrackedImmState::PendingDispatch))?;
        self.tracked_imms[index].state = TrackedImmState::Uploading;
        Some(&self.tracked_imms[index])
    }

    fn set_tracked_state(&mut self, epoch: FlushEpoch, state: TrackedImmState) {
        let tracked = self
            .tracked_imms
            .iter_mut()
            .find(|tracked| tracked.epoch == epoch)
            .expect("tracked imm not found for epoch");
        tracked.state = state;
    }

    /// Remove tracked imms through the given epoch.
    fn retire_through(&mut self, through_epoch: FlushEpoch) {
        while self
            .tracked_imms
            .front()
            .is_some_and(|tracked| tracked.epoch <= through_epoch)
        {
            self.tracked_imms.pop_front().expect("checked above");
        }
    }

    async fn shutdown(&mut self, err: Option<SlateDBError>) -> Result<(), SlateDBError> {
        let uploader_err = self.uploader.close(false).await.err();
        let writer_close = self.manifest_writer.close().await;
        self.cleanup_orphaned_uploads(&writer_close).await;
        let err = Self::refined_error([err, writer_close.err(), uploader_err].into_iter());
        self.commands.close(err.clone());
        err
    }

    /// Pick the most informative error from multiple shutdown sources.
    /// Starts with `Ok`. `Closed` refines `Ok`. Any other error refines both.
    fn refined_error(
        errors: impl IntoIterator<Item = Option<SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        let mut refined_err: Option<SlateDBError> = None;
        for err in errors.into_iter().flatten() {
            refined_err = match (refined_err, err) {
                (None, err) => Some(err),
                (Some(SlateDBError::Closed), err) => Some(err),
                (err, _) => err,
            }
        }
        if let Some(err) = refined_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Delete orphaned SSTs that were never durably written to the manifest.
    ///
    /// Tables still in `Uploading` state are always cleaned up. Tables in
    /// `WritingManifest` state are only cleaned up when the manifest writer
    /// was fenced, since other errors may have silently succeeded.
    async fn cleanup_orphaned_uploads(&mut self, writer_close: &ManifestWriterCloseResult) {
        let (cleanup_writing, last_durable_epoch) = match writer_close {
            ManifestWriterCloseResult::Ok { last_durable_epoch } => (false, *last_durable_epoch),
            ManifestWriterCloseResult::Fenced { last_durable_epoch } => (true, *last_durable_epoch),
            ManifestWriterCloseResult::Err(_) => (false, None),
        };
        for tracked in &self.tracked_imms {
            let beyond_durable = last_durable_epoch.is_none_or(|epoch| tracked.epoch > epoch);
            let should_clean = beyond_durable
                && match tracked.state {
                    TrackedImmState::Uploading => true,
                    TrackedImmState::WritingManifest => cleanup_writing,
                    TrackedImmState::PendingDispatch => false,
                };
            if should_clean {
                if let Err(delete_err) = self.inner.table_store.delete_sst(&tracked.sst_id).await {
                    log::warn!(
                        "failed to delete orphaned SST [epoch={:?}, id={:?}, error={:?}]",
                        tracked.epoch,
                        tracked.sst_id,
                        delete_err
                    );
                }
            }
        }
    }
}

struct TrackedImm {
    epoch: FlushEpoch,
    ptr: usize,
    sst_id: crate::db_state::SsTableId,
    imm_memtable: Arc<crate::mem_table::ImmutableMemtable>,
    state: TrackedImmState,
}

enum TrackedImmState {
    PendingDispatch,
    Uploading,
    WritingManifest,
}

#[cfg(test)]
mod tests {
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_state::{
        ManifestCore, SsTableHandle, SsTableId, SsTableInfo, SsTableView, SstType,
    };
    use crate::error::SlateDBError;
    use crate::format::sst::{SsTableFormat, SST_FORMAT_VERSION_LATEST};
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::memtable_flusher::{FlushTarget, MemtableFlusher};
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
    use slatedb_common::metrics::MetricsRecorderHelper;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::time::timeout;

    struct TestHarness {
        inner: Arc<DbInner>,
        manifest: FenceableManifest,
        object_store: Arc<dyn ObjectStore>,
        path: String,
    }

    async fn setup_harness(
        path: &str,
        settings: Settings,
        fp_registry: Arc<FailPointRegistry>,
    ) -> TestHarness {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = path.to_string();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand = Arc::new(DbRand::new(42));
        let db_metrics = MetricsRecorderHelper::noop();
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(path.clone()),
            Arc::clone(&object_store),
        ));
        let stored_manifest = StoredManifest::create_new_db(
            Arc::clone(&manifest_store),
            ManifestCore::new_with_wal_object_store(None),
            Arc::clone(&system_clock),
        )
        .await
        .unwrap();
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            PathResolver::new(Path::from(path.clone())),
            Arc::clone(&fp_registry),
            None,
        ));
        let (write_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(
            DbInner::new(
                settings,
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                stored_manifest.prepare_dirty().unwrap(),
                Arc::new(MemtableFlusher::new()),
                write_tx,
                db_metrics,
                fp_registry,
                None,
            )
            .await
            .unwrap(),
        );
        // These tests freeze immutable memtables directly instead of flowing through the WAL
        // pipeline, so treat all sequences as already remotely durable.
        inner.oracle.advance_durable_seq(u64::MAX);
        let manifest =
            FenceableManifest::init_writer(stored_manifest, Duration::from_secs(300), system_clock)
                .await
                .unwrap();
        TestHarness {
            inner,
            manifest,
            object_store,
            path,
        }
    }

    fn freeze_value_imm(
        harness: &TestHarness,
        key: &[u8],
        value: &[u8],
        seq: u64,
        recent_flushed_wal_id: u64,
    ) {
        let mut guard = harness.inner.state.write();
        guard.memtable().put(RowEntry::new_value(key, value, seq));
        guard.freeze_memtable(recent_flushed_wal_id).unwrap();
    }

    fn freeze_merge_imm(
        harness: &TestHarness,
        key: &[u8],
        value: &[u8],
        seq: u64,
        recent_flushed_wal_id: u64,
    ) {
        let mut guard = harness.inner.state.write();
        guard.memtable().put(RowEntry::new_merge(key, value, seq));
        guard.freeze_memtable(recent_flushed_wal_id).unwrap();
    }

    async fn latest_manifest_checkpoint_count(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> usize {
        let manifest_store = ManifestStore::new(&Path::from(path), object_store);
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        manifest.core.checkpoints.len()
    }

    fn seeded_l0_handle(first_key: &[u8]) -> SsTableHandle {
        SsTableHandle::new(
            SsTableId::Compacted(ulid::Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::copy_from_slice(first_key)),
                last_entry: None,
                index_offset: 0,
                index_len: 0,
                filter_offset: 0,
                filter_len: 0,
                compression_codec: None,
                sst_type: SstType::Compacted,
                stats_offset: 0,
                stats_len: 0,
            },
        )
    }

    async fn set_remote_l0_len(path: &str, object_store: Arc<dyn ObjectStore>, l0_len: usize) {
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store));
        let mut stored_manifest =
            StoredManifest::load(manifest_store, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.l0.clear();
        for idx in 0..l0_len {
            dirty.value.core.l0.push_back(SsTableView::new(
                ulid::Ulid::new(),
                seeded_l0_handle(format!("seed-{idx}").as_bytes()),
            ));
        }
        stored_manifest.update(dirty).await.unwrap();
    }

    fn set_local_l0_len(harness: &TestHarness, l0_len: usize) {
        let mut guard = harness.inner.state.write();
        guard.modify(|modifier| {
            modifier.state.manifest.value.core.l0.clear();
            for idx in 0..l0_len {
                modifier
                    .state
                    .manifest
                    .value
                    .core
                    .l0
                    .push_back(SsTableView::new(
                        ulid::Ulid::new(),
                        seeded_l0_handle(format!("local-seed-{idx}").as_bytes()),
                    ));
            }
        });
    }

    fn start_flusher(harness: TestHarness) -> MemtableFlusher {
        let flusher = MemtableFlusher::new();
        flusher.start(
            Arc::clone(&harness.inner),
            harness.manifest,
            &Handle::current(),
        );
        flusher
    }

    #[tokio::test]
    async fn best_effort_flushes_within_l0_budget() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_best_effort",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 11);
        let flusher = start_flusher(harness);

        let result = timeout(
            Duration::from_secs(5),
            flusher.flush(FlushTarget::BestEffort),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(result.durable_through_wal_id, Some(11));
        assert_eq!(result.durable_through_seq, Some(1));

        flusher.close().await.unwrap();
    }

    #[tokio::test]
    async fn through_wal_id_waits_for_durable_upload() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_through_wal_id",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 11);
        let flusher = start_flusher(harness);

        let result = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.durable_through_wal_id, Some(11));
        assert_eq!(result.durable_through_seq, Some(1));

        flusher.close().await.unwrap();
    }

    #[tokio::test]
    async fn through_current_imm_waits_for_durable_upload() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_through_current_imm",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 0);
        freeze_value_imm(&harness, b"k2", b"v2", 2, 0);
        let flusher = start_flusher(harness);

        let result = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.durable_through_wal_id, Some(0));
        assert_eq!(result.durable_through_seq, Some(2));

        flusher.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_resolve_multiple_flush_waiters_on_one_durable_advance() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_multiple_waiters",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 15);
        let flusher = start_flusher(harness);

        let (first, second) = tokio::join!(
            timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All)),
            timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
        );

        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_eq!(first.durable_through_wal_id, Some(15));
        assert_eq!(first.durable_through_seq, Some(1));
        assert_eq!(second, first);

        flusher.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_current_imm_waits_for_flush_barrier() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_checkpoint_current_imm",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 0);
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let flusher = start_flusher(harness);

        let checkpoint = timeout(
            Duration::from_secs(5),
            flusher.create_checkpoint(FlushTarget::All, CheckpointOptions::default()),
        )
        .await
        .unwrap()
        .unwrap();

        let after = latest_manifest_checkpoint_count(&path, object_store).await;
        assert!(checkpoint.manifest_id > 0);
        assert_eq!(after, before + 1);

        flusher.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_all_waits_for_flush_barrier() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_checkpoint_all",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 21);
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let flusher = start_flusher(harness);

        let checkpoint = timeout(
            Duration::from_secs(5),
            flusher.create_checkpoint(FlushTarget::All, CheckpointOptions::default()),
        )
        .await
        .unwrap()
        .unwrap();

        let after = latest_manifest_checkpoint_count(&path, object_store).await;
        assert!(checkpoint.manifest_id > 0);
        assert_eq!(after, before + 1);

        flusher.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_current_durable_succeeds_when_l0_is_full() {
        let settings = Settings {
            l0_max_ssts: 1,
            manifest_poll_interval: Duration::from_millis(10),
            ..Settings::default()
        };
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_checkpoint_l0_backpressure",
            settings,
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        set_remote_l0_len(&harness.path, Arc::clone(&harness.object_store), 1).await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 61);
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let flusher = start_flusher(harness);

        // A CurrentDurable checkpoint should complete promptly even when L0 is
        // full — it captures whatever is already durable without waiting for
        // the flush pipeline to drain.
        let checkpoint = timeout(
            Duration::from_secs(5),
            flusher.create_checkpoint(FlushTarget::CurrentDurable, CheckpointOptions::default()),
        )
        .await
        .unwrap()
        .unwrap();
        let after = latest_manifest_checkpoint_count(&path, object_store).await;
        assert!(checkpoint.manifest_id > 0);
        assert_eq!(after, before + 1);

        flusher.close().await.unwrap();
    }

    #[tokio::test]
    async fn fatal_upload_failure_propagates_to_flush_waiter() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_build_failure",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        freeze_merge_imm(&harness, b"k1", b"merge", 1, 31);
        let flusher = start_flusher(harness);

        // The flush fails because the pipeline shuts down before the
        // epoch becomes durable.
        let flush_result = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap();
        assert!(flush_result.is_err());

        // The specific upload error surfaces through close().
        let close_result = flusher.close().await;
        assert!(close_result.is_err());
        assert!(!matches!(close_result, Err(SlateDBError::Closed)));
    }

    #[tokio::test]
    async fn should_wait_for_manifest_refresh_before_dispatching_when_l0_is_full() {
        let settings = Settings {
            l0_max_ssts: 1,
            manifest_poll_interval: Duration::from_millis(10),
            ..Settings::default()
        };
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_l0_backpressure",
            settings,
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        set_local_l0_len(&harness, 1);
        set_remote_l0_len(&harness.path, Arc::clone(&harness.object_store), 1).await;
        freeze_value_imm(&harness, b"k1", b"v1", 1, 41);
        let inner = Arc::clone(&harness.inner);
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let flusher = start_flusher(harness);

        {
            let flush = flusher.flush(FlushTarget::All);
            tokio::pin!(flush);
            assert!(timeout(Duration::from_millis(100), &mut flush)
                .await
                .is_err());

            // Clear both local and remote L0 so the flusher can make progress.
            {
                let mut guard = inner.state.write();
                guard.modify(|modifier| modifier.state.manifest.value.core.l0.clear());
            }
            set_remote_l0_len(&path, object_store, 0).await;

            let result = timeout(Duration::from_secs(5), &mut flush)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.durable_through_wal_id, Some(41));
            assert_eq!(result.durable_through_seq, Some(1));
        }

        flusher.close().await.unwrap();
    }
}
