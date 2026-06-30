//! Distributed-compaction worker (RFC-0025).
//!
//! A [`CompactionWorker`] polls `.compactions` for `Scheduled` entries, claims
//! them via the optimistic CAS protocol described in RFC-0025, executes the
//! compaction with the same code path the in-process executor uses, and writes
//! `Compacted` (with the produced output recorded on the job's subcompaction)
//! back to `.compactions`. The coordinator separately observes those
//! `Compacted` entries and commits the manifest update (see
//! [`crate::compactor::CompactorEventHandler::commit_compacted_entries`]).
//!
//! # Deployment patterns
//!
//! Workers run in one of two modes:
//!
//! 1. **Embedded with Hybrid Optionality** a single worker is spawned inside the compaction coordinator's process. (
//!    The coordinator must have `worker: Some(CompactionWorkerOptions))` in its [`crate::config::CompactorOptions`].
//!    This is the default. Additional (non-embedded) workers may be started in addition to the embedded worker to
//!    satisfy scaling needs. This doesn't cause fencing and is an intended usage pattern.
//!
//! 2. **Standalone**: The compaction coordinator runs without an embedded worker and one or
//!    more separate worker processes each run a [`CompactionWorker`]. The coordinator must
//!    have `worker: None` in its [`crate::config::CompactorOptions`].
//!
//! # Heartbeat and failure detection
//!
//! Workers emit heartbeats to prove liveness. A heartbeat is a CAS write that
//! bumps `last_heartbeat_ms` in the worker's `.compactions` entry. Two triggers:
//!
//! 1. **Bytes trigger**: when the cumulative bytes processed since the last
//!    bytes-based heartbeat exceeds `CompactionWorkerOptions::heartbeat_bytes`
//!    *and* at least `heartbeat_min_interval` has elapsed since the last such
//!    write, the worker emits a cheap heartbeat that just refreshes liveness.
//! 2. **Subcompaction trigger**: whenever the per-range subcompaction progress
//!    (RFC-0028) advances — a range produces new output SSTs — the worker
//!    writes a heartbeat carrying the latest progress report, so a reclaiming
//!    worker can resume completed ranges.
//!
//! Both triggers are strictly per-job: a job that stops making progress stops
//! heartbeating and is reclaimed independently of its siblings on the same
//! worker, so a stalled job can be handed off to a less-loaded worker.
//!
//! The coordinator reclaims stale Running compactions whose
//! `last_heartbeat_ms` is older than
//! [`crate::config::CompactorOptions::worker_heartbeat_timeout`].
//! Reclaimed jobs resume from their last persisted state (`output_ssts`) when the
//! next worker picks them up.
//!
//! # Metrics
//!
//! Workers emit the following per-worker metrics labeled `{worker_id=<id>}`:
//!
//! | Metric | Description |
//! |---|---|
//! | `slatedb.compactor.bytes_compacted` | Bytes merged by this worker |
//! | `slatedb.compactor.running_compactions` | Jobs currently in-flight |
//! | `slatedb.compactor.ssts_written` | Output SSTs produced |
//!
//! Supply a recorder via [`CompactionWorkerBuilder::with_metrics_recorder`].
//! The coordinator emits complementary metrics (`jobs_claimed`, `jobs_reclaimed`,
//! `worker_last_heartbeat_ms`) on its own recorder.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use fail_parallel::{fail_point, FailPointRegistry};
use futures::stream::BoxStream;
use log::{debug, error, info, warn};
use tokio::runtime::Handle;
use ulid::Ulid;

use crate::compactions_store::{CompactionsStore, StoredCompactions};
use crate::compactor::stats::{CompactionStats, WorkerStats};
use crate::compactor_executor::{
    CompactionExecutor, StartCompactionJobArgs, TokioCompactionExecutor,
    TokioCompactionExecutorOptions,
};
use crate::compactor_state::{Compaction, CompactionContext, CompactionStatus, WorkerSpec};
use crate::config::CompactionWorkerOptions;
use crate::db_state::SortedRun;
use crate::dispatcher::{MessageHandler, MessageHandlerExecutor, MessageTickerDef};
use crate::error::SlateDBError;
use crate::manifest::store::ManifestStore;
use crate::manifest::ManifestCore;
use crate::merge_operator::MergeOperatorType;
use crate::subcompaction::Subcompaction;
use crate::tablestore::TableStore;
use crate::utils::{format_bytes_si, IdGenerator};
#[cfg(feature = "compaction_filters")]
use crate::CompactionFilterSupplier;
use slatedb_common::clock::SystemClock;
use slatedb_common::metrics::MetricsRecorderHelper;
use slatedb_common::DbRand;

pub(crate) const COMPACTION_WORKER_TASK_NAME: &str = "compaction_worker";

#[derive(Debug)]
pub(crate) enum WorkerMessage {
    /// Signals that a compaction job has finished execution.
    CompactionJobFinished {
        /// Job id (distinct from the canonical compaction id).
        id: Ulid,
        /// Output SR on success, or the compaction error.
        result: Result<SortedRun, SlateDBError>,
    },
    /// Periodic progress update from the [`CompactionExecutor`].
    CompactionJobProgress {
        /// The job id associated with this progress report.
        id: Ulid,
        /// The total number of bytes processed so far (estimate).
        bytes_processed: u64,
        ctx: CompactionContext,
    },
    /// Liveness-only heartbeat from the [`CompactionExecutor`], emitted while a
    /// job is still planning (reading input indexes) and so has no progress to
    /// report yet. Refreshes `last_heartbeat_ms` without touching the persisted
    /// context, so a slow planning step on a healthy worker is not mistaken for
    /// a dead one and reclaimed mid-plan.
    CompactionJobHeartbeat {
        /// The job id to refresh liveness for.
        id: Ulid,
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
        self.start()?;
        self.join().await
    }

    /// Starts the worker's event loop monitor on the current runtime.
    ///
    /// Callers that interleave shutdown with a cancellation signal should call
    /// this before racing [`CompactionWorker::join`] against that signal, so the
    /// task is registered before [`CompactionWorker::stop`] can run. Otherwise a
    /// cancellation that wins the race would invoke `stop` on a worker that was
    /// never started, silently dropping the unstarted event loop. See
    /// [`crate::admin::Admin::run_compaction_worker`].
    pub(crate) fn start(&self) -> Result<(), crate::Error> {
        self.task_executor.monitor_on(&Handle::current())?;
        Ok(())
    }

    /// Waits for the worker's event loop to finish.
    pub(crate) async fn join(&self) -> Result<(), crate::Error> {
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

/// Per-job state used to detect when the per-range subcompaction progress has
/// advanced and when the bytes threshold has been crossed.
struct JobProgressState {
    /// Total bytes processed as of the last bytes-based heartbeat write.
    last_hb_bytes: u64,
    /// Wall-clock timestamp (ms) of this job's most recent heartbeat write
    /// (either trigger). Used to throttle the bytes trigger to at most one
    /// write per `heartbeat_min_interval`, independently of sibling jobs.
    last_hb_ms: u64,
    /// Context as of the last heartbeat write that persisted it. Snapshots
    /// change only when the executor first plans the job or when a range's
    /// output SSTs change (never per byte), so comparing against this lets the
    /// worker persist resumable state only when it actually advances.
    last_hb_ctx: Option<CompactionContext>,
}

/// Total output SSTs recorded across a subcompaction progress (RFC-0028).
fn total_output_ssts(subcompactions: &[Subcompaction]) -> usize {
    subcompactions.iter().map(|s| s.output_ssts().len()).sum()
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
    /// Lazily-initialized handle for CAS reads/writes on `.compactions`. The
    /// coordinator creates the file on first run; the worker tolerates its
    /// absence on early ticks.
    stored: Option<StoredCompactions>,
    rand: Arc<DbRand>,
    fp_registry: Arc<FailPointRegistry>,
    /// Per-job heartbeat bookkeeping. Entry present iff the job is active.
    job_progress: BTreeMap<Ulid, JobProgressState>,
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
        fp_registry: Arc<FailPointRegistry>,
    ) -> Self {
        Self {
            worker_id,
            options,
            compactions_store,
            manifest_store,
            executor,
            clock,
            stored: None,
            rand,
            fp_registry,
            job_progress: BTreeMap::new(),
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
        recorder: MetricsRecorderHelper,
        system_clock: Arc<dyn SystemClock>,
        fp_registry: Arc<FailPointRegistry>,
        merge_operator: Option<MergeOperatorType>,
        #[cfg(feature = "compaction_filters")] compaction_filter_supplier: Option<
            Arc<dyn CompactionFilterSupplier>,
        >,
    ) -> (
        CompactionWorkerHandler,
        async_channel::Receiver<WorkerMessage>,
    ) {
        let (tx, rx) = async_channel::unbounded::<WorkerMessage>();
        let worker_id = rand.rng().gen_ulid(system_clock.as_ref()).to_string();
        info!(
            "starting compaction worker [worker_id={}, max_concurrent_compactions={}, compactions_poll_interval={:?}]",
            worker_id,
            options.max_concurrent_compactions,
            options.compactions_poll_interval,
        );

        let worker_stats = WorkerStats::new(&recorder, &worker_id);
        let executor = Arc::new(TokioCompactionExecutor::new(
            TokioCompactionExecutorOptions {
                handle: worker_runtime.clone(),
                options: options.clone(),
                worker_tx: tx,
                table_store: table_store.clone(),
                rand: rand.clone(),
                stats: stats.clone(),
                worker_stats,
                clock: system_clock.clone(),
                manifest_store: manifest_store.clone(),
                merge_operator: merge_operator.clone(),
                #[cfg(feature = "compaction_filters")]
                compaction_filter_supplier: compaction_filter_supplier.clone(),
            },
        ));

        let handler = CompactionWorkerHandler::new(
            worker_id,
            options.clone(),
            compactions_store.clone(),
            manifest_store.clone(),
            executor,
            system_clock.clone(),
            rand.clone(),
            fp_registry,
        );
        (handler, rx)
    }

    const EXPECT_LOADED: &'static str = "ensure_loaded should have set stored compactions";

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
            .saturating_sub(self.job_progress.len())
    }

    /// Scans `.compactions` for `Scheduled` entries without a worker, claims up
    /// to remaining capacity via CAS, then validates each claim against a
    /// manifest read *after* the claim and dispatches it to the executor.
    /// Claims that fail validation are released back to `Scheduled`.
    async fn poll_and_claim(&mut self) -> Result<(), SlateDBError> {
        let capacity = self.capacity();

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
                if c.spec().is_drain() || c.spec().destination().is_none() {
                    // Drain specs are coordinator-local, and a tiered spec without
                    // a destination can never be executed; neither can become
                    // valid later, so skip them rather than claim and release.
                    warn!("skipping unrunnable compaction spec [id={}]", c.id());
                } else if self.job_progress.contains_key(&c.id()) {
                    // It's possible this worker tries to claim a job that it's already running.
                    // This can happen if coordinator hasn't seen this worker's heartbeat yet,
                    // and transitions the job back to `Scheduled`. We don't attempt to reclaim
                    // the job since its context might have diverged or this worker might be
                    // misbehaving. Instead, stop the local execution so a subsequent poll can
                    // claim only after local bookkeeping has been cleared.
                    debug!(
                        "skipping; this compaction is already running [worker_id={}, id={}]",
                        self.worker_id,
                        c.id()
                    );
                    Self::stop_compaction_job(&self.executor, &mut self.job_progress, c.id());
                } else if to_claim.len() < capacity {
                    to_claim.push(c.clone());
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
                    self.job_progress.insert(
                        compaction.id(),
                        JobProgressState {
                            last_hb_bytes: 0,
                            last_hb_ms: self.clock.now().timestamp_millis() as u64,
                            last_hb_ctx: compaction.ctx().cloned(),
                        },
                    );
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

    /// Writes a heartbeat for `compaction_id`, updating `last_heartbeat_ms`
    /// and (when provided) the current context snapshot.
    /// Only writes if this worker still owns the entry.
    ///
    /// Returns `Ok(Some(ms))` with the persisted heartbeat timestamp iff a
    /// heartbeat was actually written, or `Ok(None)` if the write was skipped
    /// (entry gone or ownership lost). Callers use the returned timestamp to
    /// advance their in-memory progress bookkeeping consistently with what was
    /// durably recorded, and treat `None` as "do not advance" so they never
    /// mark progress as heartbeated that was never persisted.
    ///
    /// We only ever heartbeat a compaction this worker has already claimed, and
    /// claiming requires `.compactions` to be loaded, so `self.stored` is
    /// guaranteed to be `Some` here (expected via `EXPECT_LOADED` below).
    async fn write_heartbeat(
        &mut self,
        compaction_id: Ulid,
        ctx: Option<CompactionContext>,
    ) -> Result<Option<u64>, SlateDBError> {
        loop {
            let stored = self.stored.as_mut().expect(Self::EXPECT_LOADED);
            stored.refresh().await?;
            let mut dirty = stored.prepare_dirty()?;
            let Some(existing) = dirty.value.get(&compaction_id).cloned() else {
                debug!(
                    "heartbeat: compaction entry missing [worker_id={}, compaction_id={}]; skipping",
                    self.worker_id,
                    compaction_id
                );
                return Ok(None);
            };
            if existing.worker().map(|w| w.worker_id.as_str()) != Some(self.worker_id.as_str()) {
                debug!(
                    "heartbeat: no longer owner of compaction, stopping execution [worker_id={} compaction_id={}]; skipping",
                    self.worker_id,
                    compaction_id
                );
                Self::stop_compaction_job(&self.executor, &mut self.job_progress, compaction_id);
                return Ok(None);
            }
            let now_ms = self.clock.now().timestamp_millis() as u64;
            let new_spec = WorkerSpec::new(self.worker_id.clone(), now_ms);
            let mut updated_compaction = existing.with_worker(Some(new_spec));
            if let Some(ctx) = ctx.clone() {
                updated_compaction.set_ctx(Some(ctx));
            }
            dirty.value.insert(updated_compaction);
            match stored.update(dirty).await {
                Ok(()) => {
                    debug!(
                        "wrote heartbeat [worker_id={}, id={}, now_ms={}]",
                        self.worker_id, compaction_id, now_ms
                    );
                    return Ok(Some(now_ms));
                }
                Err(e) if e.is_sequenced_write_conflict() => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Handles a progress update from the executor. Triggers:
    /// - A **bytes heartbeat** when cumulative bytes since the last bytes-hb
    ///   exceeds `heartbeat_bytes` and `heartbeat_min_interval` has elapsed.
    /// - A **subcompaction write** when the per-range subcompaction progress
    ///   (RFC-0028) changed since the one last persisted, so a reclaiming
    ///   worker can resume completed ranges. This bypasses the bytes throttle
    ///   because progress changes only on range output transitions, not per
    ///   byte.
    ///
    /// `bytes_processed` is *cumulative* per job (the running byte total), not
    /// a delta.
    ///
    /// Per-job progress bookkeeping (`last_hb_bytes`, `last_hb_ms`, and
    /// `last_hb_ctx`) is only advanced after `write_heartbeat`
    /// confirms a durable write, so a skipped write (entry gone / ownership
    /// lost) does not mark un-persisted progress as heartbeated.
    async fn handle_progress(
        &mut self,
        id: Ulid,
        bytes_processed: u64,
        ctx: CompactionContext,
    ) -> Result<(), SlateDBError> {
        // Compute both triggers from a single borrow, then bail if this job is
        // unknown (stale progress message). The borrow ends before the async
        // `write_heartbeat`; state is advanced afterwards only on a confirmed
        // durable write.
        //
        // Bytes-trigger liveness is tied to *this job's own* progress: both the
        // threshold (`last_hb_bytes`) and the throttle (`last_hb_ms`) are
        // per-job, so a job that stops making byte progress stops heartbeating
        // and is reclaimed even while its siblings on this worker stay busy.
        let now_ms = self.clock.now().timestamp_millis() as u64;
        let (bytes_trigger, ctx_changed, prev_sst_count) = {
            let Some(state) = self.job_progress.get(&id) else {
                return Ok(());
            };
            let bytes_trigger = bytes_processed.saturating_sub(state.last_hb_bytes)
                >= self.options.heartbeat_bytes
                && now_ms.saturating_sub(state.last_hb_ms)
                    >= self.options.heartbeat_min_interval.as_millis() as u64;
            // Persist the full compaction context whenever it advances. This captures
            // both the initial plan/retention choice and later output progress.
            let ctx_changed = state.last_hb_ctx.as_ref() != Some(&ctx);
            (
                bytes_trigger,
                ctx_changed,
                state
                    .last_hb_ctx
                    .as_ref()
                    .map(|ctx| total_output_ssts(ctx.subcompactions()))
                    .unwrap_or(0),
            )
        };

        if !bytes_trigger && !ctx_changed {
            return Ok(());
        }

        // Carry the compaction context only when it changed; a bytes-only
        // heartbeat just refreshes liveness (`last_heartbeat_ms`). On a
        // confirmed write, record the timestamp that was actually persisted so
        // in-memory throttling stays consistent with the durable entry.
        let new_sst_count = total_output_ssts(ctx.subcompactions());
        let new_ctx = ctx_changed.then(|| ctx.clone());
        if let Some(hb_ms) = self.write_heartbeat(id, new_ctx).await? {
            let state = self
                .job_progress
                .get_mut(&id)
                .expect("active job must have progress bookkeeping");
            if ctx_changed {
                state.last_hb_ctx = Some(ctx);
            }
            state.last_hb_bytes = bytes_processed;
            state.last_hb_ms = hb_ms;
            info!(
                "progress heartbeat [worker_id={}, id={}, bytes={}, output_ssts={}, new_output_ssts={}]",
                self.worker_id,
                id,
                format_bytes_si(bytes_processed),
                new_sst_count,
                new_sst_count.saturating_sub(prev_sst_count)
            );
            if new_sst_count == 1 {
                let fp_registry = Arc::clone(&self.fp_registry);
                let _ = &fp_registry;
                fail_point!(
                    fp_registry,
                    "compactor-progress-after-first-output-sst",
                    |_| { Err(SlateDBError::Fenced) }
                );
            }
        }
        Ok(())
    }

    /// Handles a liveness-only heartbeat emitted while a job is still planning,
    /// before it produces any progress (see
    /// [`WorkerMessage::CompactionJobHeartbeat`]). Refreshes `last_heartbeat_ms`
    /// without touching the persisted context, and advances the job's
    /// `last_hb_ms` on a confirmed write so the bytes-trigger throttle stays
    /// consistent. A skipped write (entry gone / ownership lost) is a no-op, so
    /// this never resurrects a job that was already reclaimed mid-plan.
    async fn handle_planning_heartbeat(&mut self, id: Ulid) -> Result<(), SlateDBError> {
        if let Some(hb_ms) = self.write_heartbeat(id, None).await? {
            if let Some(state) = self.job_progress.get_mut(&id) {
                state.last_hb_ms = hb_ms;
            }
            debug!(
                "planning heartbeat [worker_id={}, id={}, now_ms={}]",
                self.worker_id, id, hb_ms
            );
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
        let l0_sst_views = compaction.get_l0_sst_views(db_state);
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
        let actual_l0: Vec<Ulid> = l0_sst_views.iter().map(|v| v.id).collect();
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
            l0_sst_views,
            sorted_runs,
            compaction_clock_tick: db_state.last_l0_clock_tick,
            is_dest_last_run,
            retention_min_seq: Some(db_state.recent_snapshot_min_seq),
            // Resume from the persisted compaction context (RFC-0028): a
            // reclaim preserves it, so feeding it back lets the executor continue
            // the same SST list and the progress it reports next still extends
            // the persisted one
            ctx: compaction.ctx().cloned(),
        })
    }

    fn dispatch_to_executor(
        executor: &Arc<dyn CompactionExecutor + Send + Sync>,
        args: StartCompactionJobArgs,
    ) {
        executor.start_compaction_job(args);
    }

    /// Writes `Compacted` (with the produced output recorded on the job's
    /// subcompaction) for a successfully executed job. Only writes if the worker
    /// still owns the entry; otherwise it has been reclaimed and the produced
    /// SSTs become orphans (collected by GC).
    async fn write_compacted(
        &mut self,
        compaction_id: Ulid,
        sorted_run: SortedRun,
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
                .with_output_ssts(sorted_run.sst_views.iter().map(|v| v.sst.clone()).collect())
                .with_worker(Some(WorkerSpec::new(self.worker_id.clone(), heartbeat_ms)))
                .with_ctx(None);
            dirty.value.insert(updated);
            match stored.update(dirty).await {
                Ok(()) => return Ok(()),
                Err(e) if e.is_sequenced_write_conflict() => continue,
                Err(e) => return Err(e),
            }
        }
    }

    // Stops a compaction job that is currently executing on this worker, removing
    // it from the active jobs and progress bookkeeping. This is used when a
    // job is no longer owned by this worker (e.g., it was reclaimed by the
    // coordinator due to a heartbeat timeout). The function returns without waiting
    // for the executor to finish stopping the job.
    //
    // To release ownership of a job, use `release_claim` instead.
    fn stop_compaction_job(
        executor: &Arc<dyn CompactionExecutor + Send + Sync>,
        job_progress: &mut BTreeMap<Ulid, JobProgressState>,
        compaction_id: Ulid,
    ) {
        executor.stop_compaction_job(compaction_id);
        job_progress.remove(&compaction_id);
    }

    /// Returns a claim to `Scheduled` so it can be re-attempted by any worker
    /// (used when execution fails or when the worker shuts down gracefully).
    async fn release_claim(&mut self, compaction_id: Ulid) -> Result<(), SlateDBError> {
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
        result: Result<SortedRun, SlateDBError>,
    ) -> Result<(), SlateDBError> {
        self.job_progress.remove(&id);
        match result {
            Ok(sorted_run) => self.write_compacted(id, sorted_run).await?,
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
    fn tickers(&mut self) -> Vec<MessageTickerDef<WorkerMessage>> {
        // RFC-0025: spread `.compactions` polls across workers so they don't
        // synchronize on the same read cadence. Each poll waits a random
        // duration centered on `compactions_poll_interval` (the interval plus or
        // minus half), so the mean poll rate is unchanged.
        vec![MessageTickerDef::new(
            self.options.compactions_poll_interval,
            Box::new(|| WorkerMessage::PollCompactions),
        )
        .with_jitter(0.5, self.rand.clone())]
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
            WorkerMessage::CompactionJobProgress {
                id,
                bytes_processed,
                ctx,
            } => {
                self.handle_progress(id, bytes_processed, ctx).await?;
            }
            WorkerMessage::CompactionJobHeartbeat { id } => {
                self.handle_planning_heartbeat(id).await?;
            }
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
        // heartbeat-timeout reclamation path. Stopping the executor also
        // decrements `running_compactions` for cancelled tasks.
        self.executor.stop();
        let claimed = std::mem::take(&mut self.job_progress);
        for id in claimed.into_keys() {
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
    use std::time::Duration;

    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::compactor_state::{Compaction, CompactionSpec, SourceId};
    use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo, SsTableView};
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
    use slatedb_common::MockSystemClock;

    const ROOT: &str = "/worker-test";

    /// Captures `start_compaction_job` calls without executing them, so the
    /// worker handler can be exercised without spinning up actual SST writers.
    struct NoopExecutor {
        jobs: Mutex<Vec<StartCompactionJobArgs>>,
        stopped_jobs: Mutex<Vec<Ulid>>,
    }

    impl NoopExecutor {
        fn new() -> Self {
            Self {
                jobs: Mutex::new(Vec::new()),
                stopped_jobs: Mutex::new(Vec::new()),
            }
        }

        fn jobs(&self) -> Vec<StartCompactionJobArgs> {
            self.jobs.lock().clone()
        }

        fn stopped_jobs(&self) -> Vec<Ulid> {
            self.stopped_jobs.lock().clone()
        }
    }

    impl CompactionExecutor for NoopExecutor {
        fn start_compaction_job(&self, args: StartCompactionJobArgs) {
            self.jobs.lock().push(args);
        }
        fn stop_compaction_job(&self, id: Ulid) -> bool {
            self.stopped_jobs.lock().push(id);
            true
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
            let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
            Self::new_with_clock(worker_id, clock, CompactionWorkerOptions::default()).await
        }

        async fn new_with_clock(
            worker_id: &str,
            clock: Arc<dyn SystemClock>,
            options: CompactionWorkerOptions,
        ) -> Self {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = Path::from(ROOT);
            let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
            let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));

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
                Arc::new(options),
                compactions_store.clone(),
                manifest_store.clone(),
                executor.clone(),
                clock,
                Arc::new(DbRand::new(0)),
                Arc::new(FailPointRegistry::new()),
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
        assert!(fx.handler.job_progress.contains_key(&id));
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
        assert!(fx.handler.job_progress.is_empty());
    }

    #[tokio::test]
    async fn test_worker_stops_rescheduled_job_already_active_locally_on_poll() {
        let mut fx = WorkerFixture::new_with_clock(
            "worker-A",
            Arc::new(DefaultSystemClock::new()),
            CompactionWorkerOptions {
                max_concurrent_compactions: 1,
                ..CompactionWorkerOptions::default()
            },
        )
        .await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();
        assert_eq!(fx.executor.jobs().len(), 1);
        assert!(fx.handler.job_progress.contains_key(&id));

        // Simulate the coordinator reclaiming this worker's still-running job:
        // the persisted entry is Scheduled again while the local executor still
        // has it active. The worker should not re-claim it in the same poll,
        // but it should stop local execution and clear local bookkeeping.
        let mut stored = StoredCompactions::try_load(fx.compactions_store.clone())
            .await
            .unwrap()
            .unwrap();
        stored.refresh().await.unwrap();
        let mut dirty = stored.prepare_dirty().unwrap();
        let rescheduled = dirty
            .value
            .get(&id)
            .cloned()
            .expect("claimed compaction missing")
            .with_status(CompactionStatus::Scheduled)
            .with_worker(None);
        dirty.value.insert(rescheduled);
        stored.update(dirty).await.unwrap();

        fx.handler.poll_and_claim().await.unwrap();

        assert_eq!(
            fx.executor.jobs().len(),
            1,
            "worker must not dispatch a duplicate execution"
        );
        assert_eq!(fx.executor.stopped_jobs(), vec![id]);
        assert!(!fx.handler.job_progress.contains_key(&id));
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Scheduled);
        assert!(c.worker().is_none());
    }

    #[tokio::test]
    async fn test_worker_stops_active_job_when_heartbeat_loses_ownership() {
        let mut fx = WorkerFixture::new_with_clock(
            "worker-A",
            Arc::new(DefaultSystemClock::new()),
            CompactionWorkerOptions {
                max_concurrent_compactions: 1,
                ..CompactionWorkerOptions::default()
            },
        )
        .await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();
        assert!(fx.handler.job_progress.contains_key(&id));

        // Simulate another worker taking ownership before this worker's next
        // heartbeat. The heartbeat must not rewrite the entry; it should just
        // request local execution stop and clear local capacity bookkeeping.
        let mut stored = StoredCompactions::try_load(fx.compactions_store.clone())
            .await
            .unwrap()
            .unwrap();
        stored.refresh().await.unwrap();
        let mut dirty = stored.prepare_dirty().unwrap();
        let stolen = dirty
            .value
            .get(&id)
            .cloned()
            .expect("claimed compaction missing")
            .with_worker(Some(WorkerSpec::new("worker-B".to_string(), 12345)));
        dirty.value.insert(stolen);
        stored.update(dirty).await.unwrap();

        fx.handler.handle_planning_heartbeat(id).await.unwrap();

        assert_eq!(fx.executor.stopped_jobs(), vec![id]);
        assert!(!fx.handler.job_progress.contains_key(&id));
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Running);
        assert_eq!(c.worker().unwrap().worker_id, "worker-B");
        assert_eq!(c.worker().unwrap().last_heartbeat_ms, 12345);

        let next_id = Ulid::from_parts(2, 0);
        fx.seed_scheduled_compaction(next_id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();

        let jobs = fx.executor.jobs();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[1].compaction_id, next_id);
        assert!(fx.handler.job_progress.contains_key(&next_id));
    }

    #[tokio::test]
    async fn test_worker_writes_compacted_on_finish() {
        let mut fx = WorkerFixture::new("worker-A").await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();

        // Build a synthetic sorted run the executor would have returned.
        let output_handle = SsTableHandle::new(
            SsTableId::Compacted(Ulid::from_parts(9000, 0)),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(b"a")),
                ..SsTableInfo::default()
            },
        );
        let sorted_run = SortedRun {
            id: 0,
            sst_views: vec![SsTableView::identity(output_handle.clone())],
        };

        fx.handler
            .handle_finished(id, Ok(sorted_run))
            .await
            .unwrap();

        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Compacted);
        assert_eq!(c.output_ssts().len(), 1);
        assert_eq!(c.output_ssts()[0].id, output_handle.id);
        // worker_id is still attached (the coordinator clears it on commit).
        assert_eq!(c.worker().unwrap().worker_id, fx.worker_id);
        // Active set is drained on finish.
        assert!(!fx.handler.job_progress.contains_key(&id));
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
        assert!(!fx.handler.job_progress.contains_key(&id));
    }

    #[tokio::test]
    async fn test_worker_cleanup_releases_active_claims() {
        let mut fx = WorkerFixture::new("worker-A").await;
        let id = Ulid::from_parts(1, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();
        assert_eq!(fx.handler.job_progress.len(), 1);

        // cleanup mirrors graceful shutdown.
        let empty: BoxStream<'_, WorkerMessage> = futures::stream::empty().boxed();
        fx.handler.cleanup(empty, Ok(())).await.unwrap();

        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Scheduled);
        assert!(c.worker().is_none());
        assert!(fx.handler.job_progress.is_empty());
    }

    #[test]
    fn test_worker_cleanup_releases_active_claims_in_id_order() {
        let id1 = Ulid::from_parts(1, 0);
        let id2 = Ulid::from_parts(2, 0);
        let id3 = Ulid::from_parts(3, 0);

        let job_progress = BTreeMap::from([
            (
                id3,
                JobProgressState {
                    last_hb_bytes: 0,
                    last_hb_ms: 0,
                    last_hb_ctx: None,
                },
            ),
            (
                id1,
                JobProgressState {
                    last_hb_bytes: 0,
                    last_hb_ms: 0,
                    last_hb_ctx: None,
                },
            ),
            (
                id2,
                JobProgressState {
                    last_hb_bytes: 0,
                    last_hb_ms: 0,
                    last_hb_ctx: None,
                },
            ),
        ]);
        let release_order = job_progress.into_keys().collect::<Vec<_>>();

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
        assert!(fx.handler.job_progress.is_empty());
        // No job was dispatched to the executor either.
        assert!(fx.executor.jobs().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_worker_start_then_stop() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from(ROOT);
        let worker = crate::db::builder::CompactionWorkerBuilder::new(path, object_store)
            .build()
            .await
            .expect("failed to build compaction worker");

        // Mirrors the standalone-worker lifecycle: register the event loop with
        // `start`, then shut it down. `stop` must succeed against the task that
        // `start` registered, rather than silently no-op on an unstarted worker.
        worker.start().expect("failed to start compaction worker");
        worker
            .stop()
            .await
            .expect("failed to stop compaction worker");
    }

    /// Builds a throwaway output SST handle for tests.
    fn fake_output_handle(ulid: Ulid) -> SsTableHandle {
        SsTableHandle::new(
            SsTableId::Compacted(ulid),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(b"a")),
                ..SsTableInfo::default()
            },
        )
    }

    /// When the bytes-processed counter crosses `heartbeat_bytes` and enough
    /// time has elapsed since the last bytes-based heartbeat, the worker must
    /// write a heartbeat without touching the per-range subcompaction progress.
    #[tokio::test]
    async fn test_worker_emits_bytes_heartbeat_on_threshold() {
        use tokio::time::pause;
        pause();

        // given: a claimed compaction and a worker whose bytes threshold is tiny
        // (so any byte progress crosses it), with the clock advanced past the
        // heartbeat min-interval.
        let mock_clock = Arc::new(MockSystemClock::new());
        mock_clock.set(1000);
        let options = CompactionWorkerOptions {
            heartbeat_bytes: 1,
            heartbeat_min_interval: Duration::from_millis(1),
            ..CompactionWorkerOptions::default()
        };
        let clock: Arc<dyn SystemClock> = mock_clock.clone();
        let mut fx = WorkerFixture::new_with_clock("worker-hb2", clock, options).await;
        let id = Ulid::from_parts(2, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();
        mock_clock.advance(Duration::from_secs(2)).await;

        // when: progress reports bytes over the threshold and the executor's
        // first planned context, but no output SSTs yet.
        let ctx =
            CompactionContext::new(vec![Subcompaction::new(BytesRange::unbounded())], Some(0));
        fx.handler
            .handle_progress(id, 1000, ctx.clone())
            .await
            .unwrap();

        // then: a heartbeat is written (last_heartbeat_ms bumped) but no
        // per-range output progress is persisted.
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Running);
        let worker = c.worker().expect("worker spec missing");
        assert!(
            worker.last_heartbeat_ms > 1000,
            "heartbeat_ms should have been updated; got {}",
            worker.last_heartbeat_ms
        );
        assert_eq!(c.ctx(), Some(&ctx));
        let ctx = c.ctx().unwrap();
        assert_eq!(
            0,
            ctx.subcompactions()
                .iter()
                .map(|s| s.output_ssts().len())
                .sum::<usize>()
        );
    }

    /// When the executor reports per-range subcompaction progress (RFC-0028),
    /// the worker must persist the progress to `.compactions` so a reclaiming
    /// worker can resume — even when the bytes trigger did not fire. A later
    /// report that extends a range's output SSTs must also be persisted.
    #[tokio::test]
    async fn test_worker_persists_subcompaction_progress() {
        use tokio::time::pause;
        pause();

        // given: a claimed compaction and a worker whose bytes trigger is
        // disabled, so only a subcompaction progress report change drives a
        // write.
        let mock_clock = Arc::new(MockSystemClock::new());
        mock_clock.set(1000);
        let options = CompactionWorkerOptions {
            heartbeat_bytes: u64::MAX,
            ..CompactionWorkerOptions::default()
        };
        let clock: Arc<dyn SystemClock> = mock_clock.clone();
        let mut fx = WorkerFixture::new_with_clock("worker-subc", clock, options).await;
        let id = Ulid::from_parts(3, 0);
        fx.seed_scheduled_compaction(id, vec![SourceId::SstView(fx.l0_view.id)])
            .await;
        fx.handler.poll_and_claim().await.unwrap();
        mock_clock.advance(Duration::from_secs(5)).await;

        // when: progress carries a subcompaction (bytes trigger off).
        let sst1 = fake_output_handle(Ulid::from_parts(9000, 0));
        let subcompactions =
            vec![Subcompaction::new(BytesRange::unbounded()).with_output_ssts(vec![sst1.clone()])];
        let ctx = CompactionContext::new(subcompactions.clone(), Some(0));
        fx.handler.handle_progress(id, 123, ctx).await.unwrap();

        // then: the progress is persisted and the worker heartbeat is refreshed.
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.status(), CompactionStatus::Running);
        assert!(c.worker().expect("worker spec missing").last_heartbeat_ms > 1000);
        assert_eq!(c.subcompactions(), &subcompactions);

        // when: a later progress report extends the range's output SSTs.
        let sst2 = fake_output_handle(Ulid::from_parts(9001, 0));
        let extended =
            vec![Subcompaction::new(BytesRange::unbounded()).with_output_ssts(vec![sst1, sst2])];
        let extended_ctx = CompactionContext::new(extended.clone(), Some(0));
        fx.handler
            .handle_progress(id, 456, extended_ctx)
            .await
            .unwrap();

        // then: the extended progress is also persisted (plan unchanged, so the
        // checked setter accepts it).
        let c = fx.read_compaction(id).await.expect("compaction missing");
        assert_eq!(c.subcompactions(), &extended);
    }

    /// A reclaimed compaction preserves its subcompaction progress, and a
    /// worker that re-claims it must resume the executor from the per-range
    /// output SSTs (RFC-0028) rather than restarting from scratch.
    #[tokio::test]
    async fn test_worker_resumes_from_subcompaction_progress() {
        // given: a reclaimed compaction (Scheduled, no worker) that still
        // carries per-range progress from a prior attempt.
        let mut fx = WorkerFixture::new("worker-resume").await;
        let id = Ulid::from_parts(4, 0);
        let sst1 = fake_output_handle(Ulid::from_parts(9100, 0));
        let spec = CompactionSpec::new(vec![SourceId::SstView(fx.l0_view.id)], 0);
        let compaction = Compaction::new(id, spec)
            .with_status(CompactionStatus::Scheduled)
            .with_ctx(Some(CompactionContext::new(
                vec![Subcompaction::new(BytesRange::unbounded())
                    .with_output_ssts(vec![sst1.clone()])],
                Some(0),
            )));
        let mut stored = StoredCompactions::try_load(fx.compactions_store.clone())
            .await
            .unwrap()
            .unwrap();
        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(compaction);
        stored.update(dirty).await.unwrap();

        // when: a worker claims it.
        fx.handler.poll_and_claim().await.unwrap();

        // then: the dispatched job resumes from the persisted subcompaction
        // output rather than starting from an empty list.
        let jobs = fx.executor.jobs();
        assert_eq!(jobs.len(), 1);
        let ctx = jobs[0].ctx.as_ref().expect("missing context");
        assert_eq!(ctx.subcompactions().len(), 1);
        assert_eq!(ctx.subcompactions()[0].output_ssts().len(), 1);
        assert_eq!(ctx.subcompactions()[0].output_ssts()[0].id, sst1.id);
    }
}
