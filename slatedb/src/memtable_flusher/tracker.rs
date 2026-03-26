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

use super::FlushEpoch;
use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::error::SlateDBError;
use crate::memtable_flusher::manifest_writer::{FlushResult, ManifestWriter, ManifestWriterEvent};
use crate::memtable_flusher::uploader::{UploadJob, Uploader, UploaderEvent};
use crate::memtable_flusher::{FlushCommand, FlushTarget};
use crate::oracle::Oracle;
use crate::utils::{IdGenerator, WatchableOnceCell};
use fail_parallel::fail_point;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(super) struct FlushTracker {
    inner: Arc<DbInner>,
    uploader: Uploader,
    manifest_writer: ManifestWriter,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    commands: super::FlushCommandReceiver,
    next_epoch: FlushEpoch,
    tracked_imms: VecDeque<TrackedImm>,
}

impl FlushTracker {
    pub(super) fn new(
        inner: Arc<DbInner>,
        uploader: Uploader,
        manifest_writer: ManifestWriter,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        commands: super::FlushCommandReceiver,
    ) -> Self {
        Self {
            inner,
            uploader,
            manifest_writer,
            closed_result,
            commands,
            next_epoch: FlushEpoch(1),
            tracked_imms: VecDeque::new(),
        }
    }

    pub(super) async fn run(mut self) -> Result<(), SlateDBError> {
        loop {
            let should_continue = match self.run_once().await {
                Ok(should_continue) => should_continue,
                Err(err) => return self.handle_fatal_error(err).await,
            };
            if !should_continue {
                return Ok(());
            }
        }
    }

    async fn run_once(&mut self) -> Result<bool, SlateDBError> {
        tokio::select! {
            maybe_command = self.commands.recv() => {
                let command = maybe_command.unwrap_or(FlushCommand::Shutdown);
                self.handle_command(command).await
            }
            maybe_event = self.uploader.events().recv() => {
                if let Some(event) = maybe_event {
                    self.handle_uploader_event(event).await?;
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
            FlushCommand::Shutdown => {
                let uploader_result = self.uploader.close().await;
                let manifest_writer_result = self.manifest_writer.close().await;
                uploader_result?;
                manifest_writer_result?;
                Ok(false)
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

    async fn handle_uploader_event(&mut self, event: UploaderEvent) -> Result<(), SlateDBError> {
        match event {
            UploaderEvent::Uploaded(uploaded) => {
                self.set_tracked_state(uploaded.epoch, TrackedImmState::WritingManifest);
                self.manifest_writer.notify_uploaded(*uploaded).await?;
                Ok(())
            }
            UploaderEvent::Fatal(err) => Err(err),
        }
    }

    async fn handle_manifest_writer_event(
        &mut self,
        event: ManifestWriterEvent,
    ) -> Result<(), SlateDBError> {
        match event {
            ManifestWriterEvent::Flushed { through_epoch, .. } => {
                self.retire_through(through_epoch);
                self.reconcile_and_dispatch().await
            }
            ManifestWriterEvent::ManifestRefreshed => self.reconcile_and_dispatch().await,
            ManifestWriterEvent::Fatal {
                err,
                last_durable_epoch,
            } => {
                // SSTs that never reached the manifest_writer are always safe to
                // delete. For SSTs that were handed to the manifest_writer, we
                // can only be sure the manifest write didn't land when the
                // error is Fenced (another writer took the fence). For
                // other errors the write may have succeeded silently, so
                // we leave orphaned SSTs for the GC to clean up.
                if matches!(err, SlateDBError::Fenced) {
                    self.cleanup_nondurable_uploads(last_durable_epoch).await;
                }
                Err(err)
            }
        }
    }

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
                wal_id: imm_memtable.recent_flushed_wal_id(),
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
        if let Some(tracked) = self
            .tracked_imms
            .iter_mut()
            .find(|tracked| tracked.epoch == epoch)
        {
            tracked.state = state;
        }
    }

    /// Remove tracked imms through the given epoch and return the highest WAL ID.
    fn retire_through(&mut self, through_epoch: FlushEpoch) -> Option<u64> {
        let mut through_wal_id = None;
        while self
            .tracked_imms
            .front()
            .is_some_and(|tracked| tracked.epoch <= through_epoch)
        {
            let tracked = self.tracked_imms.pop_front().expect("checked above");
            through_wal_id = Some(tracked.wal_id);
        }
        through_wal_id
    }

    async fn cleanup_nondurable_uploads(&mut self, last_durable_epoch: Option<FlushEpoch>) {
        // Wait for the uploader to drain so all in-flight uploads complete
        // before we attempt to delete orphaned SSTs.
        let _ = self.uploader.close().await;

        for tracked in &self.tracked_imms {
            let beyond_durable =
                last_durable_epoch.is_none_or(|durable_epoch| tracked.epoch > durable_epoch);
            if beyond_durable
                && matches!(
                    tracked.state,
                    TrackedImmState::Uploading | TrackedImmState::WritingManifest
                )
            {
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

    async fn handle_fatal_error(&mut self, err: SlateDBError) -> Result<(), SlateDBError> {
        self.closed_result.write(Err(err.clone()));
        let _ = self.uploader.close().await;
        let _ = self.manifest_writer.close().await;
        Err(err)
    }
}

struct TrackedImm {
    epoch: FlushEpoch,
    ptr: usize,
    wal_id: u64,
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
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
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
        let stat_registry = Arc::new(StatRegistry::new());
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
                Arc::clone(&stat_registry),
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

        let err = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap()
            .unwrap_err();
        assert!(!matches!(err, SlateDBError::Closed));

        let close_result = flusher.close().await;
        assert!(close_result.is_err());
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
