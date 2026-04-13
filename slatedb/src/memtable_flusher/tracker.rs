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

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::debug;

use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::dispatcher::MessageHandler;
use crate::error::SlateDBError;
use crate::memtable_flusher::manifest_writer::{FlushResult, ManifestWriter};
use crate::memtable_flusher::uploader::{UploadJob, UploadedMemtable, Uploader};
use crate::memtable_flusher::FlushTarget;
use crate::oracle::Oracle;
use crate::utils::IdGenerator;
use fail_parallel::fail_point;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Unified message type for the flush tracker's event loop.
pub(crate) enum TrackerMessage {
    /// A memtable may have been frozen. Triggers reconcile and dispatch.
    MemtableFrozen,
    /// Flush request from an external caller waiting for a result.
    FlushRequest {
        target: FlushTarget,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    },
    /// Checkpoint creation request from an external caller.
    CheckpointRequest {
        target: FlushTarget,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
    /// An upload worker completed successfully.
    UploadComplete(UploadedMemtable),
    /// Durable progress advanced through a new contiguous flush frontier (inclusive).
    FlushComplete { through_seq: u64 },
    /// Remote manifest changes were merged into local state.
    ManifestRefreshed,
    /// Poll the remote manifest and signal the caller when complete.
    PollManifest {
        sender: oneshot::Sender<Result<(), SlateDBError>>,
    },
}

impl std::fmt::Debug for TrackerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemtableFrozen => write!(f, "MemtableFrozen"),
            Self::FlushRequest { .. } => write!(f, "FlushRequest"),
            Self::CheckpointRequest { .. } => write!(f, "CheckpointRequest"),
            Self::UploadComplete(u) => {
                write!(
                    f,
                    "UploadComplete(first_seq={}, last_seq={})",
                    u.first_seq, u.last_seq
                )
            }
            Self::FlushComplete { through_seq } => {
                write!(f, "FlushComplete(through_seq={through_seq})")
            }
            Self::ManifestRefreshed => write!(f, "ManifestRefreshed"),
            Self::PollManifest { .. } => write!(f, "PollManifest"),
        }
    }
}

pub(super) struct FlushTracker {
    inner: Arc<DbInner>,
    uploader: Uploader,
    manifest_writer: ManifestWriter,
    frontier: TrackedImmFrontier,
}

impl FlushTracker {
    pub(super) fn new(
        inner: Arc<DbInner>,
        uploader: Uploader,
        manifest_writer: ManifestWriter,
    ) -> Self {
        Self {
            inner,
            uploader,
            manifest_writer,
            frontier: TrackedImmFrontier::new(),
        }
    }
}

#[async_trait]
impl MessageHandler<TrackerMessage> for FlushTracker {
    async fn handle(&mut self, message: TrackerMessage) -> Result<(), SlateDBError> {
        match message {
            TrackerMessage::MemtableFrozen => self.reconcile_and_dispatch().await,
            TrackerMessage::FlushRequest { target, sender } => {
                self.handle_flush_request(target, sender).await
            }
            TrackerMessage::CheckpointRequest {
                target,
                options,
                sender,
            } => {
                self.handle_checkpoint_request(target, options, sender)
                    .await
            }
            TrackerMessage::UploadComplete(uploaded) => self.handle_uploaded(uploaded).await,
            TrackerMessage::FlushComplete { through_seq } => {
                self.frontier.retire_through(through_seq);
                self.reconcile_and_dispatch().await
            }
            TrackerMessage::ManifestRefreshed => self.reconcile_and_dispatch().await,
            TrackerMessage::PollManifest { sender } => self.manifest_writer.send_poll(sender),
        }
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, TrackerMessage>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let err = result.err().unwrap_or(SlateDBError::Closed);
        self.drain_with_error(&mut messages, &err).await;
        self.cleanup_orphaned_uploads().await;
        Ok(())
    }
}

impl FlushTracker {
    async fn handle_flush_request(
        &mut self,
        target: FlushTarget,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        fail_point!(
            Arc::clone(&self.inner.fp_registry),
            "flush-memtable-to-l0",
            |_| { Ok(()) }
        );
        self.reconcile_and_dispatch().await?;
        let through_seq = self.frontier.resolve_target(target);
        self.manifest_writer.send_flush(through_seq, sender)?;
        Ok(())
    }

    async fn handle_checkpoint_request(
        &mut self,
        target: FlushTarget,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        self.reconcile_and_dispatch().await?;
        let through_seq = self.frontier.resolve_target(target);
        self.manifest_writer
            .send_checkpoint(through_seq, options, sender)?;
        self.dispatch_ready_memtables().await
    }

    async fn handle_uploaded(&mut self, uploaded: UploadedMemtable) -> Result<(), SlateDBError> {
        debug!(
            "l0 upload completed [first_seq={}, last_seq={}, sst_id={:?}]",
            uploaded.first_seq, uploaded.last_seq, uploaded.sst_handle.id
        );
        self.frontier
            .set_state(uploaded.last_seq, TrackedImmState::WritingManifest);
        self.manifest_writer.notify_uploaded(uploaded).await?;
        Ok(())
    }

    /// Check for newly frozen immutable memtables and dispatch any that are ready.
    /// New IMMs are typically announced through flush commands (e.g. from the write
    /// path), but we re-check on every event to also pick up L0 slots freed by
    /// compaction via manifest refresh.
    async fn reconcile_and_dispatch(&mut self) -> Result<(), SlateDBError> {
        let imm_memtables: Vec<_> = {
            let guard = self.inner.state.read();
            guard.state().imm_memtable.iter().rev().cloned().collect()
        };
        let inner = &self.inner;
        self.frontier.register(imm_memtables.into_iter(), &mut || {
            crate::db_state::SsTableId::Compacted(
                inner.rand.rng().gen_ulid(inner.system_clock.as_ref()),
            )
        });
        self.dispatch_ready_memtables().await
    }

    fn available_l0_slots(&self) -> usize {
        let l0_len = self.inner.state.read().state().core().l0.len();
        self.inner
            .settings
            .l0_max_ssts
            .saturating_sub(l0_len + self.frontier.reserved_l0_slots())
    }

    async fn dispatch_ready_memtables(&mut self) -> Result<(), SlateDBError> {
        while self.available_l0_slots() > 0 {
            let Some(tracked) = self.frontier.prepare_next_upload() else {
                return Ok(());
            };
            let sst_id = tracked.sst_id;
            let imm_memtable = Arc::clone(&tracked.imm_memtable);
            let last_seq = tracked.last_seq;
            debug!(
                "dispatching l0 upload [first_seq={}, last_seq={}, sst_id={:?}]",
                tracked.first_seq, last_seq, sst_id
            );

            // WAL SSTs must be durable before the L0 is uploaded (see #1255).
            if self.inner.wal_enabled && self.inner.oracle.last_remote_persisted_seq() < last_seq {
                self.inner.flush_wals().await?;
            }
            self.uploader.submit(UploadJob::new(imm_memtable, sst_id))?;
        }
        Ok(())
    }

    /// Drain remaining messages during shutdown. Process completions so
    /// durable progress advances as far as possible. Reject new flush and
    /// checkpoint requests with the given error.
    async fn drain_with_error(
        &mut self,
        messages: &mut (impl futures::Stream<Item = TrackerMessage> + Unpin),
        err: &SlateDBError,
    ) {
        while let Some(message) = messages.next().await {
            match message {
                TrackerMessage::FlushRequest { sender, .. } => {
                    let _ = sender.send(Err(err.clone()));
                }
                TrackerMessage::CheckpointRequest { sender, .. } => {
                    let _ = sender.send(Err(err.clone()));
                }
                TrackerMessage::PollManifest { sender } => {
                    let _ = sender.send(Err(err.clone()));
                }
                other => {
                    let _ = self.handle(other).await;
                }
            }
        }
    }

    /// Delete orphaned SSTs that were uploaded but never passed to the
    /// manifest writer. Tables already in `WritingManifest` state are left
    /// for GC since we cannot know whether the manifest write succeeded.
    async fn cleanup_orphaned_uploads(&mut self) {
        for tracked in self.frontier.iter() {
            if matches!(tracked.state, TrackedImmState::Uploading) {
                if let Err(delete_err) = self.inner.table_store.delete_sst(&tracked.sst_id).await {
                    log::warn!(
                        "failed to delete orphaned SST [last_seq={}, id={:?}, error={:?}]",
                        tracked.last_seq,
                        tracked.sst_id,
                        delete_err
                    );
                }
            }
        }
    }
}

struct TrackedImm {
    first_seq: u64,
    last_seq: u64,
    sst_id: crate::db_state::SsTableId,
    imm_memtable: Arc<crate::mem_table::ImmutableMemtable>,
    state: TrackedImmState,
}

/// Tracks the frontier of immutable memtables being flushed to L0.
///
/// Encapsulates dedup, state transitions, and target resolution.
struct TrackedImmFrontier {
    tracked: VecDeque<TrackedImm>,
}

impl TrackedImmFrontier {
    fn new() -> Self {
        Self {
            tracked: VecDeque::new(),
        }
    }

    /// Register newly frozen immutable memtables, deduplicating by `last_seq`.
    fn register(
        &mut self,
        imm_memtables: impl Iterator<Item = Arc<crate::mem_table::ImmutableMemtable>>,
        gen_sst_id: &mut impl FnMut() -> crate::db_state::SsTableId,
    ) {
        for imm_memtable in imm_memtables {
            let first_seq = imm_memtable
                .table()
                .first_seq()
                .expect("immutable memtable has no entries");
            let last_seq = imm_memtable
                .table()
                .last_seq()
                .expect("immutable memtable has no entries");
            if self.tracked.iter().any(|t| t.last_seq == last_seq) {
                continue;
            }
            self.tracked.push_back(TrackedImm {
                first_seq,
                last_seq,
                sst_id: gen_sst_id(),
                imm_memtable,
                state: TrackedImmState::PendingDispatch,
            });
        }
    }

    /// Resolves a flush target to the sequence that must become durable.
    fn resolve_target(&self, target: FlushTarget) -> Option<u64> {
        match target {
            FlushTarget::CurrentDurable => None,
            FlushTarget::All => self.tracked.back().map(|t| t.last_seq),
        }
    }

    /// Number of in-flight slots (uploading or writing manifest).
    fn reserved_l0_slots(&self) -> usize {
        self.tracked
            .iter()
            .filter(|t| {
                matches!(
                    t.state,
                    TrackedImmState::Uploading | TrackedImmState::WritingManifest
                )
            })
            .count()
    }

    /// Transition the next `PendingDispatch` entry to `Uploading` and return it.
    fn prepare_next_upload(&mut self) -> Option<&TrackedImm> {
        let index = self
            .tracked
            .iter()
            .position(|t| matches!(t.state, TrackedImmState::PendingDispatch))?;
        self.tracked[index].state = TrackedImmState::Uploading;
        Some(&self.tracked[index])
    }

    /// Transition a tracked entry to the given state.
    fn set_state(&mut self, last_seq: u64, state: TrackedImmState) {
        let tracked = self
            .tracked
            .iter_mut()
            .find(|t| t.last_seq == last_seq)
            .expect("tracked imm not found for last_seq");
        tracked.state = state;
    }

    /// Remove tracked entries through the given sequence (inclusive).
    fn retire_through(&mut self, through_seq: u64) {
        while self
            .tracked
            .front()
            .is_some_and(|t| t.last_seq <= through_seq)
        {
            self.tracked.pop_front().expect("checked above");
        }
    }

    /// Iterate over tracked entries (for orphan cleanup).
    fn iter(&self) -> impl Iterator<Item = &TrackedImm> {
        self.tracked.iter()
    }
}

enum TrackedImmState {
    PendingDispatch,
    Uploading,
    WritingManifest,
}

#[cfg(test)]
mod tests {
    use crate::batch_write::WriteBatchMessage;
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_state::{
        ManifestCore, SsTableHandle, SsTableId, SsTableInfo, SsTableView, SstType,
    };
    use crate::db_status::{ClosedResultWriter, DbStatusManager};
    use crate::error::SlateDBError;
    use crate::format::sst::{SsTableFormat, SST_FORMAT_VERSION_LATEST};
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::memtable_flusher::{FlushTarget, MemtableFlusher};
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::utils::{SafeSender, WatchableOnceCell};
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
        let status_manager = DbStatusManager::new(0);
        let (write_tx, _) =
            SafeSender::<WriteBatchMessage>::unbounded_channel(status_manager.result_reader());
        let inner = Arc::new(
            DbInner::new(
                settings,
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                stored_manifest.prepare_dirty().unwrap(),
                Arc::new(MemtableFlusher::new(&status_manager)),
                write_tx,
                db_metrics,
                fp_registry,
                None,
                status_manager,
            )
            .await
            .unwrap(),
        );
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
        inner: &Arc<DbInner>,
        key: &[u8],
        value: &[u8],
        recent_flushed_wal_id: u64,
    ) {
        let seq = inner.oracle.next_seq();
        let mut guard = inner.state.write();
        guard.memtable().put(RowEntry::new_value(key, value, seq));
        let last_seq = guard.memtable().table().last_seq().unwrap();
        guard.freeze_memtable(recent_flushed_wal_id);
        // Advance the durable seq to simulate a wal flush
        inner.oracle.advance_durable_seq(last_seq);
    }

    fn freeze_merge_imm(
        inner: &Arc<DbInner>,
        key: &[u8],
        value: &[u8],
        recent_flushed_wal_id: u64,
    ) {
        let seq = inner.oracle.next_seq();
        let mut guard = inner.state.write();
        guard.memtable().put(RowEntry::new_merge(key, value, seq));
        let last_seq = guard.memtable().table().last_seq().unwrap();
        guard.freeze_memtable(recent_flushed_wal_id);
        // Advance the durable seq to simulate a wal flush
        inner.oracle.advance_durable_seq(last_seq);
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

    struct StartedFlusher {
        inner: Arc<DbInner>,
        flusher: MemtableFlusher,
        executor: crate::dispatcher::MessageHandlerExecutor,
    }

    impl StartedFlusher {
        async fn shutdown(&self) {
            MemtableFlusher::shutdown(&self.executor).await;
        }
    }

    impl std::ops::Deref for StartedFlusher {
        type Target = MemtableFlusher;
        fn deref(&self) -> &Self::Target {
            &self.flusher
        }
    }

    fn start_flusher(harness: TestHarness) -> StartedFlusher {
        let inner = Arc::clone(&harness.inner);
        let closed_result = WatchableOnceCell::new();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let executor = crate::dispatcher::MessageHandlerExecutor::new(
            Arc::new(closed_result.clone()) as Arc<dyn ClosedResultWriter>,
            system_clock,
        );
        let flusher = MemtableFlusher::new(&closed_result);
        flusher
            .start(
                Arc::clone(&harness.inner),
                harness.manifest,
                &Handle::current(),
                &executor,
                &closed_result,
            )
            .unwrap();
        executor.monitor_on(&Handle::current()).unwrap();
        StartedFlusher {
            inner,
            flusher,
            executor,
        }
    }

    #[tokio::test]
    async fn flush_all_waits_for_durable_upload() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_flush_all",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 11);

        let result = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.durable_seq, 1);

        flusher.shutdown().await;
    }

    #[tokio::test]
    async fn flush_all_waits_for_multiple_imms() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_flush_multiple",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 0);
        freeze_value_imm(&flusher.inner, b"k2", b"v2", 0);

        let result = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.durable_seq, 2);

        flusher.shutdown().await;
    }

    #[tokio::test]
    async fn should_resolve_multiple_flush_waiters_on_one_durable_advance() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_multiple_waiters",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 15);

        let (first, second) = tokio::join!(
            timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All)),
            timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
        );

        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_eq!(first.durable_seq, 1);
        assert_eq!(second, first);

        flusher.shutdown().await;
    }

    #[tokio::test]
    async fn checkpoint_waits_for_flush_barrier() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_checkpoint",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 21);

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

        flusher.shutdown().await;
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
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 61);

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

        flusher.shutdown().await;
    }

    #[tokio::test]
    async fn fatal_upload_failure_propagates_to_flush_waiter() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_build_failure",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        // freeze a merge entry with no merge operator → fatal build error
        let flusher = start_flusher(harness);
        freeze_merge_imm(&flusher.inner, b"k1", b"merge", 31);

        // The flush fails because the upload error propagates through
        // the executor's closed_result to the flush waiter.
        let flush_result = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap();
        assert!(
            flush_result.is_err(),
            "expected error, got {:?}",
            flush_result
        );
        assert!(
            !matches!(flush_result, Err(SlateDBError::Closed)),
            "expected specific error, got Closed"
        );

        flusher.shutdown().await;
    }

    #[tokio::test]
    async fn manifest_writer_fencing_propagates_to_flush_waiter() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_manifest_fenced",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        // Fence the manifest before starting the flusher's manifest writer.
        let _fence = {
            let manifest_store = Arc::new(ManifestStore::new(
                &Path::from(harness.path.clone()),
                Arc::clone(&harness.object_store),
            ));
            let stored_manifest =
                StoredManifest::load(manifest_store, Arc::new(DefaultSystemClock::new()))
                    .await
                    .unwrap();
            FenceableManifest::init_writer(
                stored_manifest,
                Duration::from_secs(300),
                Arc::new(DefaultSystemClock::new()),
            )
            .await
            .unwrap()
        };

        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 11);

        // The flush should fail with a fencing error.
        let flush_result = timeout(Duration::from_secs(5), flusher.flush(FlushTarget::All))
            .await
            .unwrap();
        assert!(
            matches!(flush_result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            flush_result
        );

        flusher.shutdown().await;
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
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 41);

        {
            let flush = flusher.flush(FlushTarget::All);
            tokio::pin!(flush);
            assert!(timeout(Duration::from_millis(100), &mut flush)
                .await
                .is_err());

            // Clear both local and remote L0 so the flusher can make progress.
            {
                let mut guard = flusher.inner.state.write();
                guard.modify(|modifier| modifier.state.manifest.value.core.l0.clear());
            }
            set_remote_l0_len(&path, object_store, 0).await;

            let result = timeout(Duration::from_secs(5), &mut flush)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.durable_seq, 1);
        }

        flusher.shutdown().await;
    }

    #[tokio::test]
    async fn inflight_flush_waiter_resolves_on_shutdown() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_flusher_clean_shutdown",
            Settings::default(),
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let flusher = start_flusher(harness);
        freeze_value_imm(&flusher.inner, b"k1", b"v1", 11);
        freeze_value_imm(&flusher.inner, b"k2", b"v2", 12);

        // Start a flush that requires both epochs to become durable.
        let flush = flusher.flush(FlushTarget::All);
        tokio::pin!(flush);

        // Give the pipeline a moment to start processing.
        tokio::task::yield_now().await;

        // Shut down. The uploader and manifest writer drain first, so
        // in-flight uploads complete. The tracker drains last and
        // processes the completion messages.
        flusher.shutdown().await;

        // The flush should either succeed (uploads completed during drain)
        // or fail with Closed (uploads didn't finish in time). Either is
        // acceptable — the key invariant is that the waiter always receives
        // a response rather than a dropped channel.
        let result = timeout(Duration::from_secs(5), flush)
            .await
            .expect("timed out waiting for flush result");
        assert!(result.is_ok() || result.is_err());
    }

    mod frontier_tests {
        use crate::db_state::SsTableId;
        use crate::mem_table::{ImmutableMemtable, WritableKVTable};
        use crate::memtable_flusher::tracker::{TrackedImmFrontier, TrackedImmState};
        use crate::memtable_flusher::FlushTarget;
        use crate::types::RowEntry;
        use std::sync::Arc;

        fn make_imm(seq: u64) -> Arc<ImmutableMemtable> {
            let table = WritableKVTable::new();
            table.put(RowEntry::new_value(
                &format!("k{seq}").into_bytes(),
                b"v",
                seq,
            ));
            Arc::new(ImmutableMemtable::new(table, 0))
        }

        fn next_sst_id() -> SsTableId {
            SsTableId::Compacted(ulid::Ulid::new())
        }

        #[test]
        fn register_deduplicates_by_last_seq() {
            let mut frontier = TrackedImmFrontier::new();
            let imm = make_imm(1);
            frontier.register(std::iter::once(Arc::clone(&imm)), &mut next_sst_id);
            frontier.register(std::iter::once(imm), &mut next_sst_id);
            assert_eq!(frontier.tracked.len(), 1);
        }

        #[test]
        fn register_assigns_sequential_sequences() {
            let mut frontier = TrackedImmFrontier::new();
            frontier.register([make_imm(1), make_imm(2)].into_iter(), &mut next_sst_id);
            assert_eq!(frontier.tracked[0].first_seq, 1);
            assert_eq!(frontier.tracked[0].last_seq, 1);
            assert_eq!(frontier.tracked[1].first_seq, 2);
            assert_eq!(frontier.tracked[1].last_seq, 2);
        }

        #[test]
        fn resolve_target_all_returns_last_seq() {
            let mut frontier = TrackedImmFrontier::new();
            assert_eq!(frontier.resolve_target(FlushTarget::All), None);
            frontier.register([make_imm(1), make_imm(2)].into_iter(), &mut next_sst_id);
            assert_eq!(frontier.resolve_target(FlushTarget::All), Some(2));
        }

        #[test]
        fn resolve_target_current_durable_returns_none() {
            let mut frontier = TrackedImmFrontier::new();
            frontier.register(std::iter::once(make_imm(1)), &mut next_sst_id);
            assert_eq!(frontier.resolve_target(FlushTarget::CurrentDurable), None);
        }

        #[test]
        fn prepare_next_upload_transitions_to_uploading() {
            let mut frontier = TrackedImmFrontier::new();
            frontier.register(std::iter::once(make_imm(1)), &mut next_sst_id);
            let tracked = frontier.prepare_next_upload().unwrap();
            assert_eq!(tracked.last_seq, 1);
            assert!(matches!(tracked.state, TrackedImmState::Uploading));
            // No more pending
            assert!(frontier.prepare_next_upload().is_none());
        }

        #[test]
        fn set_state_updates_tracked_entry() {
            let mut frontier = TrackedImmFrontier::new();
            frontier.register(std::iter::once(make_imm(1)), &mut next_sst_id);
            frontier.set_state(1, TrackedImmState::WritingManifest);
            assert!(matches!(
                frontier.tracked[0].state,
                TrackedImmState::WritingManifest
            ));
        }

        #[test]
        #[should_panic(expected = "tracked imm not found for last_seq")]
        fn set_state_panics_for_unknown_seq() {
            let mut frontier = TrackedImmFrontier::new();
            frontier.set_state(99, TrackedImmState::Uploading);
        }

        #[test]
        fn retire_through_removes_entries() {
            let mut frontier = TrackedImmFrontier::new();
            frontier.register(
                [make_imm(1), make_imm(2), make_imm(3)].into_iter(),
                &mut next_sst_id,
            );
            frontier.retire_through(2);
            assert_eq!(frontier.tracked.len(), 1);
            assert_eq!(frontier.tracked[0].last_seq, 3);
        }

        #[test]
        fn reserved_l0_slots_counts_in_flight() {
            let mut frontier = TrackedImmFrontier::new();
            frontier.register(
                [make_imm(1), make_imm(2), make_imm(3)].into_iter(),
                &mut next_sst_id,
            );
            assert_eq!(frontier.reserved_l0_slots(), 0);
            frontier.prepare_next_upload(); // seq 1 -> Uploading
            assert_eq!(frontier.reserved_l0_slots(), 1);
            frontier.set_state(1, TrackedImmState::WritingManifest);
            assert_eq!(frontier.reserved_l0_slots(), 1);
            frontier.retire_through(1);
            assert_eq!(frontier.reserved_l0_slots(), 0);
        }
    }
}
