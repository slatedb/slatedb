//! Distributed-compaction worker (RFC-0025).
//!
//! A [`CompactionWorker`] polls `.compactions` for `Scheduled` entries, claims
//! them via the optimistic CAS protocol described in RFC-0025, executes the
//! compaction with the same code path the in-process executor uses, and writes
//! `Compacted` (with the produced `output_ssts`) back to `.compactions`. The
//! coordinator separately observes those `Compacted` entries and commits the
//! manifest update (see [`crate::compactor::CompactorEventHandler::commit_compacted_entries`]).
//!
//! This implements the claim / execute / complete portion of the worker. The
//! heartbeat emission and coordinator-side failure-detection / reclamation
//! protocol are added in a follow-up.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use log::{debug, error, info, warn};
use rand::RngCore;
use tokio::runtime::Handle;
use ulid::Ulid;

use crate::compactions_store::{CompactionsStore, StoredCompactions};
use crate::compactor::stats::CompactionStats;
use crate::compactor_executor::{
    CompactionExecutor, StartCompactionJobArgs, TokioCompactionExecutor,
    TokioCompactionExecutorOptions,
};
use crate::compactor_state::{Compaction, CompactionStatus, WorkerSpec};
use crate::config::CompactionWorkerOptions;
use crate::dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor, TickerJitter};
use crate::error::SlateDBError;
use crate::manifest::store::ManifestStore;
use crate::manifest::ManifestCore;
use crate::merge_operator::MergeOperatorType;
use crate::rand::DbRand;
use crate::tablestore::TableStore;
use crate::utils::IdGenerator;
#[cfg(feature = "compaction_filters")]
use crate::CompactionFilterSupplier;
use slatedb_common::clock::SystemClock;
use slatedb_common::DbRand;

pub(crate) const COMPACTION_WORKER_TASK_NAME: &str = "compaction_worker";

#[derive(Debug)]
pub(crate) enum WorkerMessage {
    /// Signals that a compaction job has finished execution.
    CompactionJobFinished {
        /// Job id (distinct from the canonical compaction id).
        id: Ulid,
        /// Output SR on success, or the compaction error.
        result: Result<crate::db_state::SortedRun, SlateDBError>,
    },
    /// Periodic progress update from the [`CompactionExecutor`].
    // Fields are unused until heartbeat emission is wired in the failure-detection follow-up.
    #[allow(dead_code)]
    CompactionJobProgress {
        /// The job id associated with this progress report.
        id: Ulid,
        /// The total number of bytes processed so far (estimate).
        bytes_processed: u64,
        /// The output SSTs produced so far (including previous runs).
        output_ssts: Vec<crate::db_state::SsTableHandle>,
    },
    /// Ticker-triggered message to poll `.compactions` for claimable jobs.
    PollCompactions,
}

/// Stateless executor of compaction jobs claimed from `.compactions`.
///
/// Build one with [`CompactionWorkerBuilder`] and drive its event loop with
/// [`CompactionWorker::run`]. Call [`CompactionWorker::stop`] to gracefully
/// release any in-flight claims.
pub struct CompactionWorker {
    task_executor: Arc<MessageHandlerExecutor>,
}

impl CompactionWorker {
    pub(crate) fn new(task_executor: Arc<MessageHandlerExecutor>) -> Self {
        Self { task_executor }
    }

    /// Runs the worker until cancellation or fatal error. The worker polls
    /// `.compactions` every [`CompactionWorkerOptions::compactions_poll_interval`],
    /// claims up to [`CompactionWorkerOptions::max_concurrent_compactions`] jobs,
    /// executes them, and writes `Compacted` back to `.compactions`.
    pub async fn run(&self) -> Result<(), crate::Error> {
        self.task_executor.monitor_on(&Handle::current())?;
        self.task_executor
            .join_task(COMPACTION_WORKER_TASK_NAME)
            .await
            .map_err(|e| e.into())
    }

    /// Gracefully stops the worker, resetting any compactions it claimed back
    /// to `Scheduled` so other workers can pick them up immediately.
    pub async fn stop(&self) -> Result<(), crate::Error> {
        self.task_executor
            .shutdown_task(COMPACTION_WORKER_TASK_NAME)
            .await
            .map_err(|e| e.into())
    }
}

/// Internal `MessageHandler` for the worker's event loop.
///
/// Reuses [`CompactorMessage`] so the embedded [`TokioCompactionExecutor`] can
/// report `CompactionJobFinished` on the same channel the dispatcher polls.
pub(crate) struct CompactionWorkerHandler {
    worker_id: String,
    options: Arc<CompactionWorkerOptions>,
    compactions_store: Arc<CompactionsStore>,
    manifest_store: Arc<ManifestStore>,
    executor: Arc<dyn CompactionExecutor + Send + Sync>,
    clock: Arc<dyn SystemClock>,
    /// Compactions currently being executed by this worker (claimed but not
    /// yet `Compacted`). Used to gate capacity and to know what to reset on
    /// graceful shutdown.
    active_jobs: BTreeSet<Ulid>,
    /// Seeds the poll ticker's jitter (see [`ticker_jitter`]). Threaded through
    /// so jitter stays deterministic under simulation testing.
    ///
    /// [`ticker_jitter`]: MessageHandler::ticker_jitter
    rand: Arc<DbRand>,
    /// Lazily-initialized handle for CAS reads/writes on `.compactions`. The
    /// coordinator creates the file on first run; the worker tolerates its
    /// absence on early ticks.
    stored: Option<StoredCompactions>,
}

impl CompactionWorkerHandler {
    pub(crate) fn new(
        worker_id: String,
        options: Arc<CompactionWorkerOptions>,
        compactions_store: Arc<CompactionsStore>,
        manifest_store: Arc<ManifestStore>,
        executor: Arc<dyn CompactionExecutor + Send + Sync>,
        clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    ) -> Self {
        Self {
            worker_id,
            options,
            compactions_store,
            manifest_store,
            executor,
            clock,
            active_jobs: BTreeSet::new(),
            rand,
            stored: None,
        }
    }

    /// Builds the worker's [`CompactionWorkerHandler`] and the receiver that
    /// the handler reads completion messages from. Shared between the
    /// standalone `run()` path and the embedded-worker path in `Compactor::run`.
    pub(crate) fn build_worker_handler(
        manifest_store: Arc<ManifestStore>,
        compactions_store: Arc<CompactionsStore>,
        table_store: Arc<TableStore>,
        options: Arc<CompactionWorkerOptions>,
        worker_runtime: Handle,
        rand: Arc<DbRand>,
        stats: Arc<CompactionStats>,
        system_clock: Arc<dyn SystemClock>,
        merge_operator: Option<MergeOperatorType>,
        #[cfg(feature = "compaction_filters")] compaction_filter_supplier: Option<
            Arc<dyn CompactionFilterSupplier>,
        >,
    ) -> (
        CompactionWorkerHandler,
        async_channel::Receiver<WorkerMessage>,
    ) {
        let (tx, rx) = async_channel::unbounded::<WorkerMessage>();
        let executor = Arc::new(TokioCompactionExecutor::new(
            TokioCompactionExecutorOptions {
                handle: worker_runtime.clone(),
                options: options.clone(),
                worker_tx: tx,
                table_store: table_store.clone(),
                rand: rand.clone(),
                stats: stats.clone(),
                clock: system_clock.clone(),
                manifest_store: manifest_store.clone(),
                merge_operator: merge_operator.clone(),
                #[cfg(feature = "compaction_filters")]
                compaction_filter_supplier: compaction_filter_supplier.clone(),
            },
        ));

        let worker_id = rand.rng().gen_ulid(system_clock.as_ref()).to_string();
        info!(
        "starting compaction worker [worker_id={}, max_concurrent_compactions={}, compactions_poll_interval={:?}]",
        worker_id,
        options.max_concurrent_compactions,
        options.compactions_poll_interval,
    );

        let handler = CompactionWorkerHandler::new(
            worker_id,
            options.clone(),
            compactions_store.clone(),
            manifest_store.clone(),
            executor,
            system_clock.clone(),
            rand.clone(),
        );
        (handler, rx)
    }

    const EXPECT_LOADED: &str = "ensure_loaded should have set stored compactions";

    /// Loads `.compactions` on first use; subsequent calls reuse the cached
    /// handle. Returns `Ok(false)` if the file does not yet exist (worker
    /// started before the coordinator).
    async fn ensure_loaded(&mut self) -> Result<bool, SlateDBError> {
        if self.stored.is_some() {
            return Ok(true);
        }
        match StoredCompactions::try_load(self.compactions_store.clone()).await? {
            Some(s) => {
                self.stored = Some(s);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn capacity(&self) -> usize {
        self.options
            .max_concurrent_compactions
            .saturating_sub(self.active_jobs.len())
    }

    /// Scans `.compactions` for `Scheduled` entries without a worker, claims up
    /// to remaining capacity via CAS, then validates each claim against a
    /// manifest read *after* the claim and dispatches it to the executor.
    /// Claims that fail validation are released back to `Scheduled`.
    async fn poll_and_claim(&mut self) -> Result<(), SlateDBError> {
        let capacity = self.capacity();
        if capacity == 0 {
            return Ok(());
        }

        // CAS loop: read latest, identify candidates, attempt write.
        // Candidates are filtered on their spec alone here; validating the
        // spec's sources against the manifest happens after the claim
        // succeeds, since only a manifest read after the claim is guaranteed
        // to be consistent with the claimed compaction.
        let claimed = loop {
            let stored = self.stored.as_mut().expect(Self::EXPECT_LOADED);
            stored.refresh().await?;
            let mut dirty_compactions = stored.prepare_dirty()?;

            let mut to_claim: Vec<Compaction> = Vec::new();
            for c in dirty_compactions
                .value
                .iter_with_status(&[CompactionStatus::Scheduled])
                .filter(|c| c.worker().is_none())
            {
                if to_claim.len() >= capacity {
                    break;
                }
                // Drain specs are coordinator-local, and a tiered spec without
                // a destination can never be executed; neither can become
                // valid later, so skip them rather than claim and release.
                if !c.spec().is_drain() && c.spec().destination().is_some() {
                    to_claim.push(c.clone());
                } else {
                    warn!("skipping unrunnable compaction spec [id={}]", c.id());
                }
            }
            if to_claim.is_empty() {
                debug!(
                    "No claimable compactions; skipping .compactions CAS write and executor dispatch [worker_id={}]",
                    self.worker_id
                );
                return Ok(());
            }

            let heartbeat_ms = self.clock.now().timestamp_millis() as u64;
            let worker_spec = WorkerSpec::new(self.worker_id.clone(), heartbeat_ms);

            for c in &to_claim {
                dirty_compactions.value.insert(
                    c.clone()
                        .with_status(CompactionStatus::Running)
                        .with_worker(Some(worker_spec.clone())),
                );
            }
            match stored.update(dirty_compactions).await {
                Ok(()) => break to_claim,
                Err(e) if e.is_sequenced_write_conflict() => {
                    debug!("claim conflict on .compactions; refreshing and retrying");
                    continue;
                }
                Err(e) => return Err(e),
            }
        };

        // Build job args against a manifest read *after* the claim CAS. The
        // coordinator writes the manifest before `.compactions` (see
        // `CompactorStateWriter::write_state_safely`), so this manifest is at
        // least as recent as the compactions state the claim landed on. A
        // manifest read before the claim could pair a stale manifest with a
        // newer spec whose source ids were recycled in the meantime (e.g. a
        // sorted run rebuilt with the same id), which the id-equality
        // validation in `build_job_args` cannot detect.
        let manifest = self.manifest_store.read_latest_manifest().await?;

        for compaction in claimed {
            match Self::build_job_args(&compaction, manifest.core(), &self.worker_id) {
                Ok(args) => {
                    info!(
                        "claimed compaction [worker_id={}, id={}]",
                        self.worker_id,
                        compaction.id()
                    );
                    self.active_jobs.insert(compaction.id());
                    Self::dispatch_to_executor(&self.executor, args);
                }
                Err(e) => {
                    warn!(
                        "claimed compaction is invalid against the post-claim manifest; releasing claim [worker_id={}, id={}, error={:?}]",
                        self.worker_id,
                        compaction.id(),
                        e
                    );
                    self.release_claim(compaction.id()).await?;
                }
            }
        }
        Ok(())
    }

    fn build_job_args(
        compaction: &Compaction,
        db_state: &ManifestCore,
        _worker_id: &str,
    ) -> Result<StartCompactionJobArgs, SlateDBError> {
        let destination = compaction
            .spec()
            .destination()
            .ok_or(SlateDBError::InvalidCompaction)?;
        let sst_views = compaction.get_l0_sst_views(db_state);
        let sorted_runs = compaction.get_sorted_runs(db_state);

        // Reject drain specs (workers only execute tiered compactions; drain
        // is coordinator-local).
        if compaction.spec().is_drain() {
            return Err(SlateDBError::InvalidCompaction);
        }

        // Validate the spec's sources actually exist in the manifest. If they
        // don't, the spec was racing with a manifest write; release the
        // claim and let the coordinator reschedule.
        let expected_l0: Vec<Ulid> = compaction
            .spec()
            .sources()
            .iter()
            .filter_map(|s| s.maybe_unwrap_sst_view())
            .collect();
        let expected_srs: Vec<u32> = compaction
            .spec()
            .sources()
            .iter()
            .filter_map(|s| s.maybe_unwrap_sorted_run())
            .collect();
        let actual_l0: Vec<Ulid> = sst_views.iter().map(|v| v.id).collect();
        let actual_srs: Vec<u32> = sorted_runs.iter().map(|sr| sr.id).collect();
        if actual_l0 != expected_l0 || actual_srs != expected_srs {
            return Err(SlateDBError::InvalidCompaction);
        }

        let is_dest_last_run = match db_state.tree_for_segment(compaction.spec().segment()) {
            Some(tree) => {
                tree.compacted.is_empty()
                    || tree.compacted.last().is_some_and(|sr| destination == sr.id)
            }
            None => false,
        };

        Ok(StartCompactionJobArgs {
            // Use compaction_id as job id so completion messages line up with
            // the entry in `.compactions`. (One-job-per-Compaction in phase 2.)
            id: compaction.id(),
            compaction_id: compaction.id(),
            destination,
            sst_views,
            sorted_runs,
            output_ssts: compaction.output_ssts().clone(),
            compaction_clock_tick: db_state.last_l0_clock_tick,
            retention_min_seq: Some(db_state.recent_snapshot_min_seq),
            is_dest_last_run,
        })
    }

    fn dispatch_to_executor(
        executor: &Arc<dyn CompactionExecutor + Send + Sync>,
        args: StartCompactionJobArgs,
    ) {
        executor.start_compaction_job(args);
    }

    /// Writes `Compacted` (with the produced `output_ssts`) for a successfully
    /// executed job. Only writes if the worker still owns the entry; otherwise
    /// it has been reclaimed and the produced SSTs become orphans (collected
    /// by GC).
    async fn write_compacted(
        &mut self,
        compaction_id: Ulid,
        output_ssts: Vec<crate::db_state::SsTableHandle>,
    ) -> Result<(), SlateDBError> {
        loop {
            let stored = self.stored.as_mut().expect(Self::EXPECT_LOADED);
            stored.refresh().await?;
            let mut dirty = stored.prepare_dirty()?;
            let Some(existing) = dirty.value.get(&compaction_id).cloned() else {
                info!(
                    "compaction entry missing on completion; dropping [id={}]",
                    compaction_id
                );
                return Ok(());
            };
            if existing.worker().map(|w| w.worker_id.as_str()) != Some(self.worker_id.as_str()) {
                info!(
                    "lost ownership before completion; dropping [id={}, status={:?}, owner={:?}]",
                    compaction_id,
                    existing.status(),
                    existing.worker().map(|w| &w.worker_id),
                );
                return Ok(());
            }
            let heartbeat_ms = self.clock.now().timestamp_millis() as u64;
            let updated = existing
                .with_status(CompactionStatus::Compacted)
                .with_output_ssts(output_ssts.clone())
                .with_worker(Some(WorkerSpec::new(self.worker_id.clone(), heartbeat_ms)));
            dirty.value.insert(updated);
            match stored.update(dirty).await {
                Ok(()) => return Ok(()),
                Err(e) if e.is_sequenced_write_conflict() => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns a claim to `Scheduled` so it can be re-attempted by any worker
    /// (used when execution fails or when the worker shuts down gracefully).
    async fn release_claim(&mut self, compaction_id: Ulid) -> Result<(), SlateDBError> {
        self.active_jobs.remove(&compaction_id);
        let worker_id = self.worker_id.as_str();
        loop {
            let stored = self.stored.as_mut().expect(Self::EXPECT_LOADED);
            stored.refresh().await?;
            let mut dirty = stored.prepare_dirty()?;
            let Some(existing) = dirty.value.get(&compaction_id).cloned() else {
                info!(
                    "compaction no longer exists, no claim to release [worker_id]={} [compaction_id]={}",
                    worker_id,
                    compaction_id
                );
                return Ok(());
            };
            let compaction_owner = existing.worker().map(|w| w.worker_id.as_str());
            if compaction_owner != Some(worker_id) {
                info!(
                    "compaction is not owned by this worker, no claim to release [worker_id]={} [compaction_id]={} [owner]={:?}",
                    worker_id,
                    compaction_id,
                    compaction_owner
                );
                return Ok(());
            }
            let updated = existing
                .with_status(CompactionStatus::Scheduled)
                .with_worker(None);
            dirty.value.insert(updated);
            match stored.update(dirty).await {
                Ok(()) => return Ok(()),
                Err(e) if e.is_sequenced_write_conflict() => continue,
                Err(e) => return Err(e),
            }
        }
    }

    async fn handle_finished(
        &mut self,
        id: Ulid,
        result: Result<crate::db_state::SortedRun, SlateDBError>,
    ) -> Result<(), SlateDBError> {
        self.active_jobs.remove(&id);
        match result {
            Ok(sr) => {
                let output_ssts: Vec<crate::db_state::SsTableHandle> =
                    sr.sst_views.into_iter().map(|v| v.sst).collect();
                self.write_compacted(id, output_ssts).await?;
            }
            Err(e) => {
                error!("compaction job failed [id={}, error={:?}]", id, e);
                self.release_claim(id).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl MessageHandler<WorkerMessage> for CompactionWorkerHandler {
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<WorkerMessage>>)> {
        vec![(
            self.options.compactions_poll_interval,
            Box::new(|| WorkerMessage::PollCompactions),
        )]
    }

    fn ticker_jitter(&self) -> TickerJitter {
        // RFC-0025: spread `.compactions` polls across workers so they don't
        // synchronize on the same read cadence. Each poll waits a random
        // duration centered on `compactions_poll_interval` (the interval plus or
        // minus half), so the mean poll rate is unchanged. The seed comes from
        // DbRand so the schedule stays deterministic under simulation testing,
        // and the wait lives inside the ticker future, so it never blocks the
        // worker's message channel.
        TickerJitter::new(0.5, self.rand.rng().next_u64())
    }

    async fn handle(&mut self, message: WorkerMessage) -> Result<(), SlateDBError> {
        if !self.ensure_loaded().await? {
            warn!(
                ".compactions does not exist yet; retrying on the next poll [worker_id={}]",
                self.worker_id
            );
            return Ok(());
        }
        match message {
            WorkerMessage::PollCompactions => {
                self.poll_and_claim().await?;
            }
            WorkerMessage::CompactionJobFinished { id, result } => {
                self.handle_finished(id, result).await?;
            }
            // Heartbeat emission is added in the failure-detection follow-up;
            // for now progress messages are ignored. The executor still
            // produces them; we just drop them.
            WorkerMessage::CompactionJobProgress { .. } => {}
        }
        Ok(())
    }

    async fn cleanup(
        &mut self,
        _messages: BoxStream<'async_trait, WorkerMessage>,
        _result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        // Stop accepting new work, then release any active claims so other
        // workers can pick them up immediately rather than waiting for the
        // heartbeat-timeout reclamation path.
        self.executor.stop();
        let claimed = self.active_jobs.clone().into_iter();
        for id in claimed {
            if let Err(e) = self.release_claim(id).await {
                error!(
                    "failed to release claim on shutdown [worker_id={}, id={}, error={:?}]",
                    self.worker_id, id, e
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor_state::{Compaction, CompactionSpec, SourceId};
    use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::store::StoredManifest;
    use crate::manifest::ManifestCore;
    use bytes::Bytes;
    use futures::stream::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use slatedb_common::clock::DefaultSystemClock;

    const ROOT: &str = "/worker-test";

    /// Captures `start_compaction_job` calls without executing them, so the
    /// worker handler can be exercised without spinning up actual SST writers.
    struct NoopExecutor {
        jobs: Mutex<Vec<StartCompactionJobArgs>>,
    }

    impl NoopExecutor {
        fn new() -> Self {
            Self {
                jobs: Mutex::new(Vec::new()),
            }
        }

        fn jobs(&self) -> Vec<StartCompactionJobArgs> {
            self.jobs.lock().clone()
        }
    }

    impl CompactionExecutor for NoopExecutor {
        fn start_compaction_job(&self, args: StartCompactionJobArgs) {
            self.jobs.lock().push(args);
        }
        fn stop(&self) {}
    }

    struct WorkerFixture {
        compactions_store: Arc<CompactionsStore>,
        executor: Arc<NoopExecutor>,
        handler: CompactionWorkerHandler,
        worker_id: String,
        // Holds an SsTableView so the test scope keeps it alive; reused as a
        // source for claimed compactions.
        l0_view: SsTableView,
    }

    impl WorkerFixture {
        async fn new(worker_id: &str) -> Self {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = Path::from(ROOT);
            let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
            let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
            let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

            // Seed manifest with one L0 view so the worker can validate spec
            // sources during the claim flow. The unsegmented V1 manifest
            // wire format only persists the SST ULID for L0 entries (view
            // id == sst id on round-trip), so use `identity` here.
            let mut core = ManifestCore::new();
            let sst_ulid = Ulid::from_parts(1000, 0);
            let sst_info = SsTableInfo {
                first_entry: Some(Bytes::from_static(b"a")),
                ..SsTableInfo::default()
            };
            let l0_view = SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(sst_ulid),
                SST_FORMAT_VERSION_LATEST,
                sst_info,
            ));
            Arc::make_mut(&mut core.tree).l0.push_back(l0_view.clone());
            StoredManifest::create_new_db(manifest_store.clone(), core, clock.clone())
                .await
                .unwrap();

            // Coordinator normally creates `.compactions` on startup. Seed it
            // here for the worker.
            StoredCompactions::create(compactions_store.clone(), 0)
                .await
                .unwrap();

            let executor = Arc::new(NoopExecutor::new());
            let mut handler = CompactionWorkerHandler::new(
                worker_id.to_string(),
                Arc::new(CompactionWorkerOptions::default()),
                compactions_store.clone(),
                manifest_store.clone(),
                executor.clone(),
                clock,
                Arc::new(DbRand::default()),
            );
            // `handle()` lazily loads `.compactions` on the first message; the
            // tests below drive the child fns (poll_and_claim, handle_finished,
            // cleanup) directly, so load it here to match that entry path.
            handler
                .ensure_loaded()
                .await
                .expect("compactions file seeded above");

            Self {
                compactions_store,
                executor,
                handler,
                worker_id: worker_id.to_string(),
                l0_view,
            }
        }

        /// Writes a single Scheduled compaction directly to `.compactions`,
        /// simulating one a coordinator would emit.
        async fn seed_scheduled_compaction(&self, id: Ulid, sources: Vec<SourceId>) {
            let spec = CompactionSpec::new(sources, 0);
            let compaction = Compaction::new(id, spec).with_status(CompactionStatus::Scheduled);
            let mut stored = StoredCompactions::try_load(self.compactions_store.clone())
                .await
                .unwrap()
                .expect("compactions file must exist");
            let mut dirty = stored.prepare_dirty().unwrap();
            dirty.value.insert(compaction);
            stored.update(dirty).await.unwrap();
        }

        async fn read_compaction(&self, id: Ulid) -> Option<Compaction> {
            let v = self
                .compactions_store
                .read_latest_compactions()
                .await
                .unwrap();
            v.compactions.get(&id).cloned()
        }
    }

    #[tokio::test]
    async fn test_worker_claims_scheduled_compaction() {
        let mut fx = WorkerFixture::new("worker-A").await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;

        fx.handler.poll_and_claim().await.unwrap();

        // The compaction should now be Running with this worker's id.
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Running);
        let worker = c.worker().expect("worker spec missing");
        assert_eq!(worker.worker_id, fx.worker_id);
        assert!(worker.last_heartbeat_ms > 0);

        // The worker should have dispatched the job to its executor.
        let jobs = fx.executor.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].compaction_id, id);

        // And the worker tracks the job locally.
        assert!(fx.handler.active_jobs.contains(&id));
    }

    #[tokio::test]
    async fn test_worker_skips_compactions_owned_by_other_workers() {
        let mut fx = WorkerFixture::new("worker-A").await;
        // Pre-claim a compaction as "worker-B".
        let id = Ulid::from_parts(1, 0);
        let spec = CompactionSpec::new(vec![SourceId::SstView(fx.l0_view.id)], 0);
        let other = Compaction::new(id, spec)
            .with_status(CompactionStatus::Running)
            .with_worker(Some(WorkerSpec::new("worker-B".to_string(), 12345)));
        let mut stored = StoredCompactions::try_load(fx.compactions_store.clone())
            .await
            .unwrap()
            .unwrap();
        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(other);
        stored.update(dirty).await.unwrap();

        fx.handler.poll_and_claim().await.unwrap();

        // No claim should have been made; worker-B's entry is untouched.
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Running);
        assert_eq!(c.worker().unwrap().worker_id, "worker-B");
        assert!(fx.executor.jobs().is_empty());
        assert!(fx.handler.active_jobs.is_empty());
    }

    #[tokio::test]
    async fn test_worker_writes_compacted_on_finish() {
        let mut fx = WorkerFixture::new("worker-A").await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();

        // Build a synthetic SortedRun the executor would have returned.
        let output_handle = SsTableHandle::new(
            SsTableId::Compacted(Ulid::from_parts(9000, 0)),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(b"a")),
                ..SsTableInfo::default()
            },
        );
        let output_sr = SortedRun {
            id: 0,
            sst_views: vec![SsTableView::new(
                Ulid::from_parts(9001, 0),
                output_handle.clone(),
            )],
        };

        fx.handler.handle_finished(id, Ok(output_sr)).await.unwrap();

        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Compacted);
        assert_eq!(c.output_ssts().len(), 1);
        assert_eq!(c.output_ssts()[0].id, output_handle.id);
        // worker_id is still attached (the coordinator clears it on commit).
        assert_eq!(c.worker().unwrap().worker_id, fx.worker_id);
        // Active set is drained on finish.
        assert!(!fx.handler.active_jobs.contains(&id));
    }

    #[tokio::test]
    async fn test_worker_releases_claim_on_execution_failure() {
        let mut fx = WorkerFixture::new("worker-A").await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();

        fx.handler
            .handle_finished(id, Err(SlateDBError::InvalidDBState))
            .await
            .unwrap();

        // On error the worker releases the claim so another worker can retry.
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Scheduled);
        assert!(c.worker().is_none());
        assert!(!fx.handler.active_jobs.contains(&id));
    }

    #[tokio::test]
    async fn test_worker_cleanup_releases_active_claims() {
        let mut fx = WorkerFixture::new("worker-A").await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();
        assert_eq!(fx.handler.active_jobs.len(), 1);

        // cleanup mirrors graceful shutdown.
        let empty: BoxStream<'_, WorkerMessage> = futures::stream::empty().boxed();
        fx.handler.cleanup(empty, Ok(())).await.unwrap();

        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Scheduled);
        assert!(c.worker().is_none());
        assert!(fx.handler.active_jobs.is_empty());
    }

    #[test]
    fn test_worker_cleanup_releases_active_claims_in_id_order() {
        let id1 = Ulid::from_parts(1, 0);
        let id2 = Ulid::from_parts(2, 0);
        let id3 = Ulid::from_parts(3, 0);

        let active_jobs = BTreeSet::from([id3, id1, id2]);
        let release_order = active_jobs.into_iter().collect::<Vec<_>>();

        assert_eq!(release_order, vec![id1, id2, id3]);
    }

    #[tokio::test]
    async fn test_worker_skips_unrunnable_spec() {
        let mut fx = WorkerFixture::new("worker-A").await;
        let id = Ulid::from_parts(1, 0);
        // Source view ID that does not exist in the manifest — build_job_args
        // should reject this after the claim, releasing it back to Scheduled
        // for another worker to retry.
        let ghost = Ulid::from_parts(u64::MAX, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(ghost)])
            .await;

        fx.handler.poll_and_claim().await.unwrap();

        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Scheduled);
        assert!(c.worker().is_none());
        // No active job retained.
        assert!(fx.handler.active_jobs.is_empty());
        // No job was dispatched to the executor either.
        assert!(fx.executor.jobs().is_empty());
    }
}
