//! Parallel L0 flush SST uploader.
//!
//! The uploader is intentionally narrow in scope:
//! - build an SST from an immutable memtable
//! - upload the SST to object storage
//! - report successful completion
//!
//! It does not own:
//! - manifest mutation
//! - checkpoint creation
//! - flush waiter bookkeeping
//! - timeout enforcement

use super::tracker::TrackerMessage;
use crate::db::DbInner;
use crate::db_state::{SsTableHandle, SsTableId};
use crate::db_status::ClosedResultWriter;
use crate::dispatcher::{MessageHandler, MessageHandlerExecutor};
use crate::error::SlateDBError;
use crate::format::sst::EncodedSsTable;
use crate::mem_table::ImmutableMemtable;
use crate::utils::{IdGenerator, SafeSender};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::{info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;

const UPLOADER_TASK_NAME: &str = "l0_sst_uploader";

/// One immutable-memtable upload request submitted to the uploader. The
/// worker allocates SST ids for each segment internally — there's no upstream
/// pre-allocation, since the number of output SSTs is only known after the
/// memtable is iterated through the segment extractor.
pub(crate) struct UploadJob {
    /// Immutable memtable to build into one or more SSTs.
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
}

impl std::fmt::Debug for UploadJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadJob").finish()
    }
}

impl UploadJob {
    /// Creates a new upload job.
    pub(crate) fn new(imm_memtable: Arc<ImmutableMemtable>) -> Self {
        Self { imm_memtable }
    }
}

/// One uploaded SST from a memtable flush, tagged with the segment it
/// belongs to (RFC-0024). An empty `prefix` denotes the compatibility-encoded
/// `prefix=""` segment whose state lives in the manifest's top-level tree.
#[derive(Clone)]
pub(crate) struct SegmentHandle {
    pub(crate) prefix: Bytes,
    pub(crate) sst_handle: SsTableHandle,
}

#[derive(Clone)]
/// Result of a successfully uploaded immutable memtable. A flush produces
/// one [`SegmentHandle`] per segment touched. Without an extractor configured
/// every flush yields a single handle with empty prefix.
pub(crate) struct UploadedMemtable {
    /// Same immutable memtable that was uploaded.
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    /// Per-segment uploaded SSTs, sorted ascending by `prefix`. Always
    /// non-empty.
    pub(crate) segments: Vec<SegmentHandle>,
    /// Lowest sequence number present in the immutable memtable.
    pub(crate) first_seq: u64,
    /// Highest sequence number present in the immutable memtable.
    pub(crate) last_seq: u64,
}

impl UploadedMemtable {
    #[cfg(test)]
    pub(crate) fn new(
        imm_memtable: Arc<ImmutableMemtable>,
        sst_handle: SsTableHandle,
        first_seq: u64,
        last_seq: u64,
    ) -> Self {
        assert!(first_seq <= last_seq);
        Self {
            imm_memtable,
            segments: vec![SegmentHandle {
                prefix: Bytes::new(),
                sst_handle,
            }],
            first_seq,
            last_seq,
        }
    }
}

/// Wrapper around the upload job channel. Owns the tx channel and registers
/// workers with the executor on [`start`](Self::start).
#[derive(Clone)]
pub(crate) struct Uploader {
    jobs_tx: SafeSender<UploadJob>,
}

impl Uploader {
    pub(crate) fn start(
        db: Arc<DbInner>,
        closed_result: &dyn ClosedResultWriter,
        tracker_tx: SafeSender<TrackerMessage>,
        executor: &MessageHandlerExecutor,
        tokio_handle: &Handle,
    ) -> Result<Self, SlateDBError> {
        let (jobs_tx, jobs_rx) = SafeSender::unbounded_channel(closed_result.result_reader());
        let uploader = Uploader { jobs_tx };
        let handlers = Self::build_handlers(db, tracker_tx);
        executor.add_handlers(
            UPLOADER_TASK_NAME.to_string(),
            handlers,
            jobs_rx,
            tokio_handle,
        )?;
        Ok(uploader)
    }

    pub(crate) fn build_handlers(
        db: Arc<DbInner>,
        tracker_tx: SafeSender<TrackerMessage>,
    ) -> Vec<Box<dyn MessageHandler<UploadJob>>> {
        let parallelism = db.settings.l0_flush_parallelism;
        let retry_backoff = db.settings.manifest_poll_interval;
        (0..parallelism)
            .map(|_| {
                Box::new(UploadHandler::new(
                    Arc::clone(&db),
                    tracker_tx.clone(),
                    retry_backoff,
                )) as Box<dyn MessageHandler<UploadJob>>
            })
            .collect()
    }

    /// Submits a new upload job.
    pub(crate) fn submit(&self, job: UploadJob) -> Result<(), SlateDBError> {
        self.jobs_tx.send(job)
    }

    pub(crate) async fn shutdown(executor: &MessageHandlerExecutor) {
        if let Err(e) = executor.shutdown_task(UPLOADER_TASK_NAME).await {
            warn!("failed to shutdown l0 sst uploader [error={:?}]", e);
        }
    }
}

/// MessageHandler that builds and uploads one SST per job.
pub(crate) struct UploadHandler {
    db: Arc<DbInner>,
    tracker_tx: SafeSender<TrackerMessage>,
    retry_backoff: Duration,
}

impl UploadHandler {
    pub(crate) fn new(
        db: Arc<DbInner>,
        tracker_tx: SafeSender<TrackerMessage>,
        retry_backoff: Duration,
    ) -> Self {
        Self {
            db,
            tracker_tx,
            retry_backoff,
        }
    }

    async fn upload_with_retry(&self, job: &UploadJob) -> Result<UploadedMemtable, SlateDBError> {
        // Build once, retry only the upload. `write_sst` takes
        // `&EncodedSsTable`, so the encoded SSTs stay alive for retries —
        // no need to rebuild from the memtable on transient upload errors.
        let built = self.db.build_imm_ssts(job.imm_memtable.table()).await?;
        let first_seq = job
            .imm_memtable
            .table()
            .first_seq()
            .expect("flush of l0 with no entries");
        let last_seq = job
            .imm_memtable
            .table()
            .last_seq()
            .expect("flush of l0 with no entries");

        // Upload all segment SSTs concurrently. `try_join_all` short-circuits
        // on the first fatal error and drops the remaining futures; sibling
        // uploads that already landed before the abort are left for the
        // garbage collector to reclaim, since the worker allocates ids
        // internally and they are not visible here for explicit cleanup.
        let segments = futures::future::try_join_all(
            built
                .iter()
                .map(|sst| self.upload_one_segment(&job.imm_memtable, &sst.prefix, &sst.encoded)),
        )
        .await?;

        Ok(UploadedMemtable {
            imm_memtable: Arc::clone(&job.imm_memtable),
            segments,
            first_seq,
            last_seq,
        })
    }

    /// Upload a single segment SST with retry. Each retry reuses the
    /// already-built `encoded` so the upload loop never rebuilds from the
    /// memtable.
    async fn upload_one_segment(
        &self,
        imm_memtable: &Arc<ImmutableMemtable>,
        prefix: &Bytes,
        encoded: &EncodedSsTable,
    ) -> Result<SegmentHandle, SlateDBError> {
        let sst_id =
            SsTableId::Compacted(self.db.rand.rng().gen_ulid(self.db.system_clock.as_ref()));
        let written_bytes = encoded.remaining_len() as u64;
        loop {
            match self
                .db
                .upload_compacted_sst(&sst_id, imm_memtable.table(), encoded, true)
                .await
            {
                Ok(sst_handle) => {
                    self.db.db_stats.l0_flush_bytes.increment(written_bytes);
                    return Ok(SegmentHandle {
                        prefix: prefix.clone(),
                        sst_handle,
                    });
                }
                Err(e) => {
                    // When the WAL is enabled and the database is shutting
                    // down, give up immediately. The data is already durable
                    // in the WAL and will be recovered on the next startup.
                    if self.db.wal_enabled && self.db.check_closed().is_err() {
                        info!(
                            "skipping l0 upload retry during shutdown [sst_id={:?}, error={:?}]",
                            sst_id, e
                        );
                        return Err(e);
                    }
                    self.db.system_clock.sleep(self.retry_backoff).await;
                }
            }
        }
    }
}

#[async_trait]
impl MessageHandler<UploadJob> for UploadHandler {
    async fn handle(&mut self, job: UploadJob) -> Result<(), SlateDBError> {
        let success = self.upload_with_retry(&job).await?;
        self.tracker_tx
            .send(TrackerMessage::UploadComplete(success))?;
        Ok(())
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, UploadJob>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        // On clean shutdown, drain remaining jobs.
        if result.is_ok() {
            while let Some(job) = messages.next().await {
                let success = self.upload_with_retry(&job).await?;
                self.tracker_tx
                    .send(TrackerMessage::UploadComplete(success))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{TrackerMessage, UploadJob, Uploader};
    use crate::config::Settings;
    use crate::db::DbInner;
    use crate::db_state::SsTableView;
    use crate::db_status::{ClosedResultWriter, DbStatusManager};
    use crate::error::SlateDBError;
    use crate::format::sst::SsTableFormat;
    use crate::iter::RowEntryIterator;
    use crate::manifest::ManifestCore;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::TableStore;
    use crate::test_utils::FixedThreeBytePrefixExtractor;
    use crate::types::{RowEntry, ValueDeletable};
    use crate::utils::WatchableOnceCell;
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

    async fn setup_db(path: &str, fp_registry: Arc<FailPointRegistry>) -> Arc<DbInner> {
        setup_db_with_extractor(path, fp_registry, None).await
    }

    async fn setup_db_with_extractor(
        path: &str,
        fp_registry: Arc<FailPointRegistry>,
        segment_extractor: Option<Arc<dyn crate::prefix_extractor::PrefixExtractor>>,
    ) -> Arc<DbInner> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let settings = Settings::default();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand = Arc::new(DbRand::new(42));
        let db_metrics = MetricsRecorderHelper::noop();
        let manifest_store = Arc::new(crate::manifest::store::ManifestStore::new(
            &Path::from(path),
            Arc::clone(&object_store),
        ));
        let stored_manifest = crate::manifest::store::StoredManifest::create_new_db(
            manifest_store,
            ManifestCore::new_with_wal_object_store(None),
            Arc::clone(&system_clock),
        )
        .await
        .unwrap();
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            PathResolver::new(Path::from(path)),
            fp_registry.clone(),
            None,
        ));
        let status_manager = DbStatusManager::new(0);
        let (write_tx, _) =
            crate::utils::SafeSender::unbounded_channel(status_manager.result_reader());
        Arc::new(
            DbInner::new(
                settings,
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                stored_manifest.prepare_dirty().unwrap(),
                Arc::new(crate::memtable_flusher::MemtableFlusher::new(
                    &status_manager,
                )),
                write_tx,
                db_metrics,
                fp_registry,
                None,
                status_manager,
                segment_extractor,
            )
            .await
            .unwrap(),
        )
    }

    fn freeze_imm(
        db: &DbInner,
        key: &[u8],
        value: &[u8],
        seq: u64,
    ) -> Arc<crate::mem_table::ImmutableMemtable> {
        let mut guard = db.state.write();
        guard.memtable().put(RowEntry::new_value(key, value, seq));
        guard.freeze_memtable(0);
        guard.state().imm_memtable.front().cloned().unwrap()
    }

    fn next_upload_job(db: &DbInner, key: &[u8], value: &[u8], seq: u64) -> UploadJob {
        let imm_memtable = freeze_imm(db, key, value, seq);
        UploadJob::new(imm_memtable)
    }

    struct TestUploader {
        uploader: Uploader,
        tracker_rx: async_channel::Receiver<TrackerMessage>,
        executor: Arc<crate::dispatcher::MessageHandlerExecutor>,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    }

    impl TestUploader {
        /// Wait for the executor to report a closed result (error or clean shutdown).
        async fn await_closed(&self) -> Result<(), SlateDBError> {
            self.closed_result.reader().await_value().await
        }

        async fn shutdown(&self) {
            Uploader::shutdown(&self.executor).await;
        }
    }

    impl std::ops::Deref for TestUploader {
        type Target = Uploader;
        fn deref(&self) -> &Self::Target {
            &self.uploader
        }
    }

    fn start_test_uploader(db: &Arc<DbInner>) -> TestUploader {
        let closed_result: WatchableOnceCell<Result<(), SlateDBError>> = WatchableOnceCell::new();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let (tracker_tx, tracker_rx) =
            crate::utils::SafeSender::unbounded_channel(closed_result.result_reader());
        let executor = Arc::new(crate::dispatcher::MessageHandlerExecutor::new(
            Arc::new(closed_result.clone()),
            system_clock,
        ));
        let uploader = Uploader::start(
            Arc::clone(db),
            &closed_result,
            tracker_tx,
            &executor,
            &Handle::current(),
        )
        .unwrap();
        executor.monitor_on(&Handle::current()).unwrap();
        TestUploader {
            uploader,
            tracker_rx,
            executor,
            closed_result,
        }
    }

    #[tokio::test]
    async fn should_emit_uploaded_event_for_successful_job() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_success",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let job = next_upload_job(&db, b"key", b"value", 1);

        let test = start_test_uploader(&db);
        test.submit(job).unwrap();

        let msg = timeout(Duration::from_secs(5), test.tracker_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let TrackerMessage::UploadComplete(event) = msg else {
            panic!("expected UploadComplete");
        };
        assert_eq!(event.first_seq, 1);
        assert_eq!(event.last_seq, 1);

        // Verify the uploaded SST contains the expected key-value entry.
        assert_eq!(event.segments.len(), 1);
        let segment = &event.segments[0];
        assert!(segment.prefix.is_empty());
        let sst_view = SsTableView::identity(
            db.table_store
                .open_sst(&segment.sst_handle.id)
                .await
                .unwrap(),
        );
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_view,
            Arc::clone(&db.table_store),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("expected non-empty SST");
        let entry = iter
            .next()
            .await
            .unwrap()
            .expect("expected at least one entry");
        assert_eq!(entry.key.as_ref(), b"key");
        assert_eq!(entry.value, ValueDeletable::Value(Bytes::from("value")));
        assert_eq!(entry.seq, 1);
        assert!(
            iter.next().await.unwrap().is_none(),
            "expected exactly one entry"
        );

        test.shutdown().await;
    }

    #[tokio::test]
    async fn should_retry_upload_failures_until_success() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "1*off->return",
        )
        .unwrap();
        let db = setup_db("/tmp/test_parallel_l0_flush_uploader_retry", fp_registry).await;
        let job = next_upload_job(&db, b"key", b"value", 1);

        let test = start_test_uploader(&db);
        test.submit(job).unwrap();

        let msg = timeout(Duration::from_secs(5), test.tracker_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let TrackerMessage::UploadComplete(event) = msg else {
            panic!("expected UploadComplete");
        };
        assert_eq!(event.first_seq, 1);
        assert_eq!(event.last_seq, 1);

        test.shutdown().await;
    }

    #[tokio::test]
    async fn should_emit_fatal_event_for_build_failure() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_build_failure",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        {
            let mut guard = db.state.write();
            guard.memtable().put(crate::types::RowEntry::new_merge(
                b"key",
                b"merge_operand",
                1,
            ));
            guard.freeze_memtable(0);
        }
        let imm_memtable = db
            .state
            .read()
            .state()
            .imm_memtable
            .front()
            .cloned()
            .unwrap();
        let job = UploadJob::new(imm_memtable);

        let test = start_test_uploader(&db);
        test.submit(job).unwrap();

        // The worker returns a fatal error. Wait for the executor to
        // propagate it to closed_result.
        let result = timeout(Duration::from_secs(5), test.await_closed())
            .await
            .expect("timed out waiting for fatal error");
        assert!(result.is_err());
        assert!(
            !matches!(result, Err(SlateDBError::Closed)),
            "expected specific error, got Closed"
        );
    }

    #[tokio::test]
    async fn should_process_multiple_jobs() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_multiple",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let job1 = next_upload_job(&db, b"key1", b"value1", 1);
        let job2 = next_upload_job(&db, b"key2", b"value2", 2);

        let test = start_test_uploader(&db);
        test.submit(job1).unwrap();
        test.submit(job2).unwrap();

        // Collect both upload completions.
        let mut uploaded_seqs = Vec::new();
        for _ in 0..2 {
            let msg = timeout(Duration::from_secs(5), test.tracker_rx.recv())
                .await
                .unwrap()
                .unwrap();
            if let TrackerMessage::UploadComplete(uploaded) = msg {
                uploaded_seqs.push(uploaded.last_seq);
            }
        }
        uploaded_seqs.sort();
        assert_eq!(uploaded_seqs, vec![1, 2]);

        test.shutdown().await;
    }

    #[tokio::test]
    async fn submit_should_fail_after_worker_fatal_error() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_submit_after_fatal",
            Arc::new(FailPointRegistry::new()),
        )
        .await;

        // Create a merge entry without a merge operator — this causes a
        // fatal build error in the worker.
        {
            let mut guard = db.state.write();
            guard
                .memtable()
                .put(crate::types::RowEntry::new_merge(b"key", b"operand", 1));
            guard.freeze_memtable(0);
        }
        let imm_memtable = db
            .state
            .read()
            .state()
            .imm_memtable
            .front()
            .cloned()
            .unwrap();
        let bad_job = UploadJob::new(imm_memtable);

        let test = start_test_uploader(&db);
        test.submit(bad_job).unwrap();

        // Wait for the executor to propagate the error to closed_result.
        let result = timeout(Duration::from_secs(5), test.await_closed())
            .await
            .expect("timed out waiting for fatal error");
        assert!(result.is_err());

        // Subsequent submits should fail with the specific error.
        let err = test
            .submit(next_upload_job(&db, b"key2", b"value2", 2))
            .unwrap_err();
        assert!(
            !matches!(err, SlateDBError::Closed),
            "expected specific error, got Closed"
        );
    }

    #[tokio::test]
    async fn should_stop_retrying_on_shutdown_when_wal_enabled() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_shutdown_retry",
            fp_registry,
        )
        .await;
        assert!(db.wal_enabled);
        let job = next_upload_job(&db, b"key", b"value", 1);

        let test = start_test_uploader(&db);
        test.submit(job).unwrap();

        // Mark the database as closed (simulates Db::close()).
        db.status_manager.write_result(Ok(()));

        // The uploader should give up retrying and report the error.
        let result = timeout(Duration::from_secs(5), test.await_closed())
            .await
            .expect("uploader should stop retrying on shutdown");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_emit_one_segment_handle_per_prefix() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let db = setup_db_with_extractor(
            "/tmp/test_parallel_l0_flush_uploader_multi_segment",
            fp_registry,
            Some(Arc::new(FixedThreeBytePrefixExtractor)),
        )
        .await;
        // Stage three segments worth of entries in a single memtable, then
        // freeze it. The uploader should emit a single UploadComplete with
        // three SegmentHandles, sorted by prefix.
        {
            let mut guard = db.state.write();
            for (key, value, seq) in [
                (&b"aaa-1"[..], b"v1", 1),
                (&b"aaa-2"[..], b"v2", 2),
                (&b"bbb-1"[..], b"v3", 3),
                (&b"ccc-1"[..], b"v4", 4),
            ] {
                guard.memtable().put(RowEntry::new_value(key, value, seq));
            }
            guard.freeze_memtable(0);
        }
        let imm_memtable = db
            .state
            .read()
            .state()
            .imm_memtable
            .front()
            .cloned()
            .unwrap();
        let job = UploadJob::new(imm_memtable);

        let test = start_test_uploader(&db);
        test.submit(job).unwrap();

        let msg = timeout(Duration::from_secs(5), test.tracker_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let TrackerMessage::UploadComplete(uploaded) = msg else {
            panic!("expected UploadComplete");
        };
        assert_eq!(uploaded.first_seq, 1);
        assert_eq!(uploaded.last_seq, 4);
        let prefixes: Vec<&[u8]> = uploaded
            .segments
            .iter()
            .map(|s| s.prefix.as_ref())
            .collect();
        assert_eq!(prefixes, vec![&b"aaa"[..], &b"bbb"[..], &b"ccc"[..]]);

        // Each SST exists in object storage with a unique id.
        let mut ids = std::collections::HashSet::new();
        for segment in &uploaded.segments {
            db.table_store
                .open_sst(&segment.sst_handle.id)
                .await
                .expect("uploaded SST should be readable");
            assert!(ids.insert(segment.sst_handle.id));
        }

        test.shutdown().await;
    }

    #[tokio::test]
    async fn should_abort_concurrent_segment_uploads_on_shutdown_when_wal_enabled() {
        // With multiple segments uploading concurrently via try_join_all,
        // every per-segment retry loop must independently observe the
        // shutdown signal and bail out — otherwise one stuck upload would
        // hold the worker open. Configure the fail point to fail every
        // upload and verify the worker reports an error after the db is
        // closed.
        let fp_registry = Arc::new(FailPointRegistry::new());
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        let db = setup_db_with_extractor(
            "/tmp/test_parallel_l0_flush_uploader_multi_segment_shutdown",
            fp_registry,
            Some(Arc::new(FixedThreeBytePrefixExtractor)),
        )
        .await;
        assert!(db.wal_enabled);
        // Stage entries that route to three distinct segments.
        {
            let mut guard = db.state.write();
            for (key, value, seq) in [
                (&b"aaa-1"[..], b"v1", 1),
                (&b"bbb-1"[..], b"v2", 2),
                (&b"ccc-1"[..], b"v3", 3),
            ] {
                guard.memtable().put(RowEntry::new_value(key, value, seq));
            }
            guard.freeze_memtable(0);
        }
        let imm_memtable = db
            .state
            .read()
            .state()
            .imm_memtable
            .front()
            .cloned()
            .unwrap();
        let job = UploadJob::new(imm_memtable);

        let test = start_test_uploader(&db);
        test.submit(job).unwrap();

        // Mark the database as closed (simulates Db::close()). Every
        // in-flight per-segment upload future should bail out.
        db.status_manager.write_result(Ok(()));

        let result = timeout(Duration::from_secs(5), test.await_closed())
            .await
            .expect("uploader should stop retrying on shutdown");
        assert!(result.is_err());
    }
}
