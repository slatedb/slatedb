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
use super::FlushEpoch;
use crate::db::DbInner;
use crate::db_state::{SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::format::sst::EncodedSsTable;
use crate::mem_table::ImmutableMemtable;
use crate::utils::safe_async_channel;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

/// One immutable-memtable upload request submitted to the uploader.
pub(crate) struct UploadJob {
    /// Ordering token assigned by the memtable flusher.
    pub(crate) epoch: FlushEpoch,
    /// Immutable memtable to build into an SST.
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    /// Preallocated SST id to use for the uploaded table.
    pub(crate) sst_id: SsTableId,
}

impl UploadJob {
    /// Creates a new upload job.
    pub(crate) fn new(
        epoch: FlushEpoch,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_id: SsTableId,
    ) -> Self {
        Self {
            epoch,
            imm_memtable,
            sst_id,
        }
    }
}

#[derive(Clone)]
/// Result of a successfully uploaded immutable memtable.
pub(crate) struct UploadedMemtable {
    /// Ordering token assigned by the memtable flusher.
    pub(crate) epoch: FlushEpoch,
    /// Same immutable memtable that was uploaded.
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    /// Handle for the uploaded SST in object storage.
    pub(crate) sst_handle: SsTableHandle,
    /// Highest sequence number present in the immutable memtable.
    pub(crate) last_seq: u64,
}

impl UploadedMemtable {
    #[cfg(test)]
    pub(crate) fn new(
        epoch: FlushEpoch,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_handle: SsTableHandle,
        last_seq: u64,
    ) -> Self {
        Self {
            epoch,
            imm_memtable,
            sst_handle,
            last_seq,
        }
    }
}

pub(crate) struct Uploader {
    jobs_tx: safe_async_channel::SafeSender<UploadJob>,
    aborted: CancellationToken,
    supervisor: Option<JoinHandle<Result<(), SlateDBError>>>,
}

impl Uploader {
    /// Starts the uploader subsystem and spawns `worker_count` worker tasks plus
    /// a supervisor task on the provided runtime handle.
    ///
    /// Upload attempts use a fixed retry backoff. Fatal worker failures close
    /// the events channel; the error is returned by [`close`](Self::close).
    pub(crate) fn start(
        db: Arc<DbInner>,
        worker_count: usize,
        retry_backoff: Duration,
        handle: &Handle,
        tracker_tx: safe_async_channel::SafeSender<TrackerMessage>,
    ) -> Self {
        let (jobs_tx, jobs_rx) = safe_async_channel::unbounded_channel();
        let aborted = CancellationToken::new();
        let mut workers = JoinSet::new();

        for worker_id in 0..worker_count {
            let worker = UploadWorker::new(Arc::clone(&db), aborted.clone(), retry_backoff);
            let jobs = jobs_rx.clone();
            let tracker = tracker_tx.clone();
            workers.spawn(async move { worker.run(worker_id, jobs, tracker).await });
        }

        let supervisor = handle.spawn(Self::run_supervisor(
            workers,
            jobs_rx,
            aborted.clone(),
            tracker_tx,
        ));

        Self {
            jobs_tx,
            aborted,
            supervisor: Some(supervisor),
        }
    }

    /// Submits a new upload job.
    ///
    /// Returns an error if the uploader has already been poisoned or if the job
    /// channel has been closed.
    pub(crate) async fn submit(&self, job: UploadJob) -> Result<(), SlateDBError> {
        self.jobs_tx.send(job)
    }

    /// Closes the uploader.
    ///
    /// When `graceful` is true, queued jobs are allowed to finish before
    /// workers exit. When false, workers are aborted immediately.
    pub(crate) async fn close(&mut self, graceful: bool) -> Result<(), SlateDBError> {
        self.jobs_tx.close();
        if !graceful {
            self.aborted.cancel();
        }
        let result = if let Some(supervisor) = self.supervisor.take() {
            match supervisor.await {
                Ok(result) => result,
                Err(join_err) if join_err.is_cancelled() => Ok(()),
                Err(join_err) if join_err.is_panic() => Err(SlateDBError::BackgroundTaskPanic(
                    "parallel_l0_flush_uploader".into(),
                )),
                Err(_) => Err(SlateDBError::BackgroundTaskCancelled(
                    "parallel_l0_flush_uploader".into(),
                )),
            }
        } else {
            Ok(())
        };
        result
    }

    async fn run_supervisor(
        mut workers: JoinSet<Result<(), SlateDBError>>,
        jobs_rx: safe_async_channel::SafeReceiver<UploadJob>,
        aborted: CancellationToken,
        tracker_tx: safe_async_channel::SafeSender<TrackerMessage>,
    ) -> Result<(), SlateDBError> {
        let result = loop {
            tokio::select! {
                _ = aborted.cancelled() => {
                    break Self::drain_workers(workers, jobs_rx, Ok(())).await;
                }
                maybe_result = workers.join_next() => {
                    if let Some(result) = maybe_result {
                        let resolved_result = Self::resolve(result);
                        break Self::drain_workers(workers, jobs_rx, resolved_result).await;
                    }
                }
            }
        };
        let _ = tracker_tx.send(TrackerMessage::UploaderShutdown(result.clone()));
        result
    }

    fn resolve(result: Result<Result<(), SlateDBError>, JoinError>) -> Result<(), SlateDBError> {
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(join_err) if join_err.is_panic() => Err(SlateDBError::BackgroundTaskPanic(
                "parallel_l0_flush_uploader_worker".into(),
            )),
            Err(_) => Err(SlateDBError::BackgroundTaskCancelled(
                "parallel_l0_flush_uploader_worker".into(),
            )),
        }
    }

    async fn drain_workers(
        mut workers: JoinSet<Result<(), SlateDBError>>,
        jobs_rx: safe_async_channel::SafeReceiver<UploadJob>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let mut joined_result = result;
        workers.abort_all();
        while let Some(result) = workers.join_next().await {
            let worker_result = Self::resolve(result);
            if joined_result.is_ok() && worker_result.is_err() {
                joined_result = worker_result;
            }
        }
        jobs_rx.close(joined_result.clone().map(|_| ()));
        joined_result
    }
}

struct UploadWorker {
    db: Arc<DbInner>,
    aborted: CancellationToken,
    retry_backoff: Duration,
}

impl UploadWorker {
    fn new(db: Arc<DbInner>, aborted: CancellationToken, retry_backoff: Duration) -> Self {
        Self {
            db,
            aborted,
            retry_backoff,
        }
    }

    async fn run(
        self,
        worker_id: usize,
        jobs: safe_async_channel::SafeReceiver<UploadJob>,
        tracker_tx: safe_async_channel::SafeSender<TrackerMessage>,
    ) -> Result<(), SlateDBError> {
        info!("l0 uploader worker started [worker_id={}]", worker_id);
        let result = self.recv_loop(jobs, tracker_tx).await;
        match &result {
            Ok(()) => info!("l0 uploader worker stopped [worker_id={}]", worker_id),
            Err(err) => error!(
                "l0 uploader worker failed [worker_id={}, error={:?}]",
                worker_id, err
            ),
        }
        result
    }

    async fn recv_loop(
        &self,
        jobs: safe_async_channel::SafeReceiver<UploadJob>,
        tracker_tx: safe_async_channel::SafeSender<TrackerMessage>,
    ) -> Result<(), SlateDBError> {
        loop {
            let idle_start = self.db.system_clock.now();
            tokio::select! {
                _ = self.aborted.cancelled() => return Ok(()),
                recv_result = jobs.recv() => {
                    let idle_elapsed = self.db.system_clock.now()
                        .signed_duration_since(idle_start)
                        .to_std()
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    self.db.db_stats.l0_upload_idle_millis.increment(idle_elapsed);
                    let Some(job) = recv_result else {
                        return Ok(());
                    };

                    let busy_start = self.db.system_clock.now();
                    let success = self.upload_with_retry(job).await?;
                    let busy_elapsed = self.db.system_clock.now()
                        .signed_duration_since(busy_start)
                        .to_std()
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    self.db.db_stats.l0_upload_busy_millis.increment(busy_elapsed);
                    let _ = tracker_tx.send(TrackerMessage::UploadComplete(success));
                }
            }
        }
    }

    async fn upload_with_retry(&self, job: UploadJob) -> Result<UploadedMemtable, SlateDBError> {
        loop {
            let encoded_sst = self.db.build_imm_sst(job.imm_memtable.table()).await?;
            tokio::select! {
                _ = self.aborted.cancelled() => return Err(SlateDBError::Closed),
                result = self.try_upload_once(&job, encoded_sst) => match result {
                    Ok(success) => return Ok(success),
                    Err(_) => {
                        tokio::select! {
                            _ = self.aborted.cancelled() => return Err(SlateDBError::Closed),
                            _ = self.db.system_clock.sleep(self.retry_backoff) => {}
                        }
                    }
                }
            }
        }
    }

    async fn try_upload_once(
        &self,
        job: &UploadJob,
        encoded_sst: EncodedSsTable,
    ) -> Result<UploadedMemtable, SlateDBError> {
        // TODO: consider changing the low-level upload path so failed uploads
        // return ownership of the built SST. That would let the worker build
        // once and retry uploads without rebuilding.
        let last_seq = job
            .imm_memtable
            .table()
            .last_seq()
            .expect("flush of l0 with no entries");
        let written_bytes = encoded_sst.remaining_len() as u64;
        let sst_handle = self
            .db
            .upload_compacted_sst(&job.sst_id, job.imm_memtable.table(), encoded_sst, true)
            .await?;
        self.db.db_stats.l0_flush_bytes.increment(written_bytes);
        Ok(UploadedMemtable {
            epoch: job.epoch,
            imm_memtable: Arc::clone(&job.imm_memtable),
            sst_handle,
            last_seq,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{FlushEpoch, TrackerMessage, UploadJob, Uploader};
    use crate::config::Settings;
    use crate::db::DbInner;
    use crate::db_state::{ManifestCore, SsTableId};
    use crate::error::SlateDBError;
    use crate::format::sst::SsTableFormat;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::utils::safe_async_channel;
    use crate::utils::IdGenerator;
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
        let (write_tx, _) = tokio::sync::mpsc::unbounded_channel();
        Arc::new(
            DbInner::new(
                settings,
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                stored_manifest.prepare_dirty().unwrap(),
                Arc::new(crate::memtable_flusher::MemtableFlusher::new()),
                write_tx,
                db_metrics,
                fp_registry,
                None,
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
        guard.freeze_memtable(0).unwrap();
        guard.state().imm_memtable.front().cloned().unwrap()
    }

    fn next_upload_job(db: &DbInner, epoch: u64, key: &[u8], value: &[u8], seq: u64) -> UploadJob {
        let imm_memtable = freeze_imm(db, key, value, seq);
        let sst_id = SsTableId::Compacted(db.rand.rng().gen_ulid(db.system_clock.as_ref()));
        UploadJob::new(FlushEpoch(epoch), imm_memtable, sst_id)
    }

    #[tokio::test]
    async fn should_emit_uploaded_event_for_successful_job() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_success",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let job = next_upload_job(&db, 1, b"key", b"value", 1);

        let (tracker_tx, tracker_rx) = safe_async_channel::unbounded_channel();
        let mut uploader = Uploader::start(
            Arc::clone(&db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
            tracker_tx,
        );
        uploader.submit(job).await.unwrap();

        let msg = timeout(Duration::from_secs(5), tracker_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let TrackerMessage::UploadComplete(event) = msg else {
            panic!("expected UploadComplete");
        };
        assert_eq!(event.epoch, FlushEpoch(1));
        assert_eq!(event.last_seq, 1);

        uploader.close(true).await.unwrap();
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
        let job = next_upload_job(&db, 7, b"key", b"value", 1);

        let (tracker_tx, tracker_rx) = safe_async_channel::unbounded_channel();
        let mut uploader = Uploader::start(
            Arc::clone(&db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
            tracker_tx,
        );
        uploader.submit(job).await.unwrap();

        let msg = timeout(Duration::from_secs(5), tracker_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let TrackerMessage::UploadComplete(event) = msg else {
            panic!("expected UploadComplete");
        };
        assert_eq!(event.epoch, FlushEpoch(7));

        uploader.close(true).await.unwrap();
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
            guard.freeze_memtable(0).unwrap();
        }
        let imm_memtable = db
            .state
            .read()
            .state()
            .imm_memtable
            .front()
            .cloned()
            .unwrap();
        let sst_id = SsTableId::Compacted(db.rand.rng().gen_ulid(db.system_clock.as_ref()));
        let job = UploadJob::new(FlushEpoch(3), imm_memtable, sst_id);

        let (tracker_tx, tracker_rx) = safe_async_channel::unbounded_channel();
        let mut uploader = Uploader::start(
            Arc::clone(&db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
            tracker_tx,
        );
        uploader.submit(job).await.unwrap();

        // The worker fails and sends UploaderShutdown.
        let msg = timeout(Duration::from_secs(5), tracker_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(msg, TrackerMessage::UploaderShutdown(Err(_))));

        // close() returns the worker's specific error.
        let close_result = uploader.close(false).await;
        assert!(close_result.is_err());
        assert!(!matches!(close_result, Err(SlateDBError::Closed)));
    }

    #[tokio::test]
    async fn close_should_drain_queued_jobs() {
        let db = setup_db(
            "/tmp/test_parallel_l0_flush_uploader_close_drains",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let job1 = next_upload_job(&db, 1, b"key1", b"value1", 1);
        let job2 = next_upload_job(&db, 2, b"key2", b"value2", 2);

        let (tracker_tx, tracker_rx) = safe_async_channel::unbounded_channel();
        let mut uploader = Uploader::start(
            Arc::clone(&db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
            tracker_tx,
        );
        uploader.submit(job1).await.unwrap();
        uploader.submit(job2).await.unwrap();

        uploader.close(true).await.unwrap();

        let mut uploaded_epochs = Vec::new();
        // Drain all upload completions. After graceful close, the UploaderShutdown
        // message marks the end of upload events.
        while let Some(msg) = tracker_rx.recv().await {
            match msg {
                TrackerMessage::UploadComplete(uploaded) => {
                    uploaded_epochs.push(uploaded.epoch);
                }
                TrackerMessage::UploaderShutdown(_) => break,
                _ => {}
            }
        }

        assert_eq!(uploaded_epochs, vec![FlushEpoch(1), FlushEpoch(2)]);
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
            guard.freeze_memtable(0).unwrap();
        }
        let imm_memtable = db
            .state
            .read()
            .state()
            .imm_memtable
            .front()
            .cloned()
            .unwrap();
        let sst_id = SsTableId::Compacted(db.rand.rng().gen_ulid(db.system_clock.as_ref()));
        let bad_job = UploadJob::new(FlushEpoch(1), imm_memtable, sst_id);

        let (tracker_tx, tracker_rx) = safe_async_channel::unbounded_channel();
        let uploader = Uploader::start(
            Arc::clone(&db),
            1,
            Duration::from_millis(1),
            &Handle::current(),
            tracker_tx,
        );
        uploader.submit(bad_job).await.unwrap();

        // The worker fails and sends UploaderShutdown.
        let msg = timeout(Duration::from_secs(5), tracker_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(msg, TrackerMessage::UploaderShutdown(Err(_))));

        // A subsequent submit should fail with the worker's error, not a
        // generic Closed error.
        let err = uploader
            .submit(next_upload_job(&db, 2, b"key2", b"value2", 2))
            .await
            .unwrap_err();
        assert!(
            !matches!(err, SlateDBError::Closed),
            "expected specific error, got Closed"
        );
    }
}
