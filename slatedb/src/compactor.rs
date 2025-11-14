//! Compactor Orchestration and Naming Overview
//!
//! This module implements the event loop ("compactor") that orchestrates database
//! compactions. It coordinates three concerns:
//!
//! 1. deciding what to compact,
//! 2. executing the work,
//! 3. and persisting the effects.
//!
//! The key types follow a consistent naming hierarchy to distinguish “what should happen”
//! from “what is currently running”.
//!
//! Naming hierarchy (logical → runtime):
//! - [`CompactionSpec`]: Description of a compaction to perform. Pure specification of
//!   inputs and destination (e.g., L0 SST ULIDs and/or Sorted Run ids → destination SR id).
//! - [`Compaction`]: A concrete compaction entity the system decided to run. It has a
//!   stable id (ULID) and references a [`CompactionSpec`]. Compactions live in the
//!   process-local [`CompactorState`] and represent the lifecycle of a single planned
//!   transformation of the DB’s structure. Today each [`Compaction`] has exactly one
//!   runtime job; future retries would attach multiple jobs to a single [`Compaction`].
//! - Compaction Job: A single job attempt the executor runs. Currently there isn't a separate model
//!   type that tracks the Compaction Job entity. When starting a job the compactor creates a
//!   [`StartCompactionJobArgs`] instance which materializes the execution-time inputs (opened
//!   SST handles and Sorted Runs), carries runtime context like the logical compaction clock tick,
//!   and sends this to the executor.
//!   Currently there is a 1:1 mapping between [`Compaction`] and a Compaction Job, but
//!   if retries are introduced, multiple Compaction Jobs would be associated with a single
//!   [`Compaction`].
//!
//! Roles:
//! - [`CompactionScheduler`]: Policy that proposes [`CompactionSpec`] values based on the
//!   current [`CompactorState`] (e.g., size-tiered scheduling). It validates logical
//!   invariants (such as consecutive sources and destination rules).
//! - [`CompactionExecutor`]: Runs [`CompactionJobSpec`] attempts, merges inputs, applies
//!   merge and retention logic, writes output SSTs, and reports progress/finish.
//! - [`CompactorEventHandler`]: Event-driven controller. It polls the manifest, asks
//!   the scheduler for new [`CompactionSpec`] proposals, registers [`Compaction`] entities,
//!   spawns [`CompactionJobSpec`] attempts on the executor, and persists results by
//!   updating the manifest.
//!
//! Progress and GC safety:
//! - Progress is tracked per compaction as `bytes_processed` (approximate, for observability).
//! - The lowest start time among active compaction ids (ULIDs) is exported as a
//!   “low-watermark” hint so GC can avoid deleting inputs required by running work.
//!
//! High-level flow:
//! 1) Poll manifest and merge remote state into local [`CompactorState`].
//! 2) Ask the [`CompactionScheduler`] for candidate [`CompactionSpec`] values.
//! 3) For each accepted spec, create a [`Compaction`] (id + spec), derive a
//!    [`StartCompactionJobArgs`] with concrete inputs, and pass it to the executor.
//! 4) Upon completion, update [`CompactorState`], write a new manifest, and repeat.
//!
//! These names are used consistently across modules so it is clear whether a type
//! represents a description (Spec), a durable decision (Compaction), or a running
//! attempt (JobSpec).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use log::{debug, error, info, warn};
use tokio::runtime::Handle;
use tracing::instrument;
use ulid::Ulid;

use crate::clock::SystemClock;
use crate::compactor::stats::CompactionStats;
use crate::compactor_executor::{
    CompactionExecutor, StartCompactionJobArgs, TokioCompactionExecutor,
};
use crate::compactor_state::{Compaction, CompactionSpec, CompactorState, SourceId};
use crate::config::{CheckpointOptions, CompactorOptions};
use crate::db_state::SortedRun;
use crate::dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor};
use crate::error::{Error, SlateDBError};
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::merge_operator::MergeOperatorType;
use crate::rand::DbRand;
pub use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::utils::{IdGenerator, WatchableOnceCell};

pub(crate) const COMPACTOR_TASK_NAME: &str = "compactor";

/// Supplies a concrete [`CompactionScheduler`] implementation.
///
/// This indirection lets SlateDB plug different scheduling policies (e.g. size-tiered,
/// leveled, manual) without coupling the compactor to a specific strategy.
pub trait CompactionSchedulerSupplier: Send + Sync {
    /// Creates a new [`CompactionScheduler`] using the provided options.
    ///
    /// ## Arguments
    /// - `options`: Compactor runtime tuning knobs (e.g. concurrency, polling interval).
    ///
    /// ## Returns
    /// - A boxed scheduler that decides which compactions to run next.
    fn compaction_scheduler(
        &self,
        options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync>;
}

/// Policy that decides when and what to compact.
///
/// The compactor periodically invokes the scheduler with the latest [`CompactorState`].
/// Implementations return one or more candidate compaction specs, which the compactor then
/// validates and submits to the executor.
pub trait CompactionScheduler: Send + Sync {
    /// Proposes compactions for the current state.
    ///
    /// ## Arguments
    /// - `state`: Process-local view of the DB's manifest and in-flight compactions.
    ///
    /// ## Returns
    /// - A list of [`CompactionSpec`] describing what to compact and where to write.
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<CompactionSpec>;

    /// Validates a candidate compaction against scheduler-specific invariants. The
    /// default implementation accepts everything. Schedulers can override to enforce
    /// policy-specific constraints prior to execution (e.g. level rules, overlaps).
    ///
    /// Not all compactions have to originate from scheduler. Admin/“manual” compactions,
    /// persisted/resumed plans, or tests can construct a [`CompactionSpec`]`. This
    /// method can be used to validate external compaction specs against scheduler
    /// specific invariants.
    ///
    /// ## Arguments
    /// - `_state`: Current [`CompactorState`].
    /// - `_compaction`: Proposed [`CompactionSpec`].
    ///
    /// ## Returns
    /// - `Ok(())` if valid, or an [`Error`] if invalid.
    fn validate_compaction(
        &self,
        _state: &CompactorState,
        _compaction: &CompactionSpec,
    ) -> Result<(), Error> {
        Ok(())
    }
}

// Progress is tracked per Compaction via its `bytes_processed` field.

/// Messages exchanged between the compactor event loop and its collaborators.
#[derive(Debug)]
pub(crate) enum CompactorMessage {
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
    },
    /// Ticker-triggered message to log DB runs and in-flight job state.
    LogStats,
    /// Ticker-triggered message to refresh the manifest and schedule compactions.
    PollManifest,
}

/// The compactor is responsible for taking groups of sorted runs (this doc uses the term
/// sorted run to refer to both sorted runs and l0 ssts) and compacting them together to
/// reduce space amplification (by removing old versions of rows that have been updated/deleted)
/// and read amplification (by reducing the number of sorted runs that need to be searched on
/// a read). It's made up of a few different components:
///
/// - [`Compactor`]: The main event loop that orchestrates the compaction process.
/// - [`CompactorEventHandler`]: The event handler that handles events from the compactor.
/// - [`CompactionScheduler`]: The scheduler that discovers compactions that should be performed.
/// - [`CompactionExecutor`]: The executor that runs the compaction tasks.
///
/// The main event loop listens on the manifest poll ticker to react to manifest poll ticks, the
/// executor worker channel to react to updates about running compactions, and the shutdown
/// channel to discover when it should terminate. It doesn't actually implement the logic for
/// reacting to these events. This is implemented by [`CompactorEventHandler`].
///
/// The Scheduler is responsible for deciding what sorted runs should be compacted together.
/// It implements the [`CompactionScheduler`] trait. The implementation is specified by providing
/// an implementation of [`crate::config::CompactionSchedulerSupplier`] so different scheduling
/// policies can be plugged into slatedb. Currently, the only implemented policy is the size-tiered
/// scheduler supplied by [`crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier`]
///
/// The Executor does the actual work of compacting sorted runs by sort-merging them into a new
/// sorted run. It implements the [`CompactionExecutor`] trait. Currently, the only implementation
/// is the [`TokioCompactionExecutor`] which runs compaction on a local tokio runtime.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct Compactor {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: Arc<CompactorOptions>,
    scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
    task_executor: Arc<MessageHandlerExecutor>,
    compactor_runtime: Handle,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    system_clock: Arc<dyn SystemClock>,
    merge_operator: Option<MergeOperatorType>,
}

impl Compactor {
    pub(crate) fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: CompactorOptions,
        scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
        compactor_runtime: Handle,
        rand: Arc<DbRand>,
        stat_registry: Arc<StatRegistry>,
        system_clock: Arc<dyn SystemClock>,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        merge_operator: Option<MergeOperatorType>,
    ) -> Self {
        let stats = Arc::new(CompactionStats::new(stat_registry));
        let task_executor = Arc::new(MessageHandlerExecutor::new(
            closed_result.clone(),
            system_clock.clone(),
        ));
        Self {
            manifest_store,
            table_store,
            options: Arc::new(options),
            scheduler_supplier,
            task_executor,
            compactor_runtime,
            rand,
            stats,
            system_clock,
            merge_operator,
        }
    }

    /// Starts the compactor. This method runs the actual compaction event loop. The
    /// compactor runs until the cancellation token is cancelled. The compactor's event
    /// loop always runs on the current runtime, while the compactor executor runs on the
    /// provided runtime in the [`Compactor::new`] constructor. This is to keep
    /// long-running compaction tasks from blocking the main runtime.
    ///
    /// ## Returns
    /// - `Ok(())` when the compactor task exits cleanly, or [`SlateDBError`] on failure.
    #[allow(dead_code)]
    pub async fn run_async_task(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let scheduler = Arc::from(self.scheduler_supplier.compaction_scheduler(&self.options));
        let executor = Arc::new(TokioCompactionExecutor::new(
            self.compactor_runtime.clone(),
            self.options.clone(),
            tx,
            self.table_store.clone(),
            self.rand.clone(),
            self.stats.clone(),
            self.system_clock.clone(),
            self.manifest_store.clone(),
            self.merge_operator.clone(),
        ));
        let handler = CompactorEventHandler::new(
            self.manifest_store.clone(),
            self.options.clone(),
            scheduler,
            executor,
            self.rand.clone(),
            self.stats.clone(),
            self.system_clock.clone(),
        )
        .await?;
        self.task_executor
            .add_handler(
                COMPACTOR_TASK_NAME.to_string(),
                Box::new(handler),
                rx,
                &Handle::current(),
            )
            .expect("failed to spawn compactor task");
        self.task_executor.monitor_on(&Handle::current())?;
        self.task_executor.join_task(COMPACTOR_TASK_NAME).await
    }

    /// Gracefully stops the compactor task and waits for it to finish.
    ///
    /// ## Returns
    /// - `Ok(())` once the task has shut down, or [`SlateDBError`] if shutdown fails.
    #[allow(dead_code)]
    pub async fn stop(&self) -> Result<(), SlateDBError> {
        self.task_executor.shutdown_task(COMPACTOR_TASK_NAME).await
    }
}

/// Event-driven controller for compaction orchestration.
///
/// The [`CompactorEventHandler`] implements [`MessageHandler<CompactorMessage>`] and
/// runs inside the compactor task's message loop. It reacts to periodic tickers and
/// executor callbacks, and coordinates scheduling and execution of compactions.
pub(crate) struct CompactorEventHandler {
    state: CompactorState,
    manifest: FenceableManifest,
    options: Arc<CompactorOptions>,
    scheduler: Arc<dyn CompactionScheduler + Send + Sync>,
    executor: Arc<dyn CompactionExecutor + Send + Sync>,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    system_clock: Arc<dyn SystemClock>,
}

#[async_trait]
impl MessageHandler<CompactorMessage> for CompactorEventHandler {
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<CompactorMessage>>)> {
        vec![
            (
                self.options.poll_interval,
                Box::new(|| CompactorMessage::PollManifest),
            ),
            (
                Duration::from_secs(10),
                Box::new(|| CompactorMessage::LogStats),
            ),
        ]
    }

    async fn handle(&mut self, message: CompactorMessage) -> Result<(), SlateDBError> {
        match message {
            CompactorMessage::LogStats => self.handle_log_ticker(),
            CompactorMessage::PollManifest => self.handle_ticker().await,
            CompactorMessage::CompactionJobFinished { id, result } => match result {
                Ok(sr) => self
                    .finish_compaction(id, sr)
                    .await
                    .expect("fatal error finishing compaction"),
                Err(err) => {
                    error!("error executing compaction [error={:#?}]", err);
                    self.finish_failed_compaction(id);
                }
            },
            CompactorMessage::CompactionJobProgress {
                id,
                bytes_processed,
            } => {
                self.state
                    .update_compaction(&id, |c| c.set_bytes_processed(bytes_processed));
            }
        }
        Ok(())
    }

    async fn cleanup(
        &mut self,
        mut _messages: BoxStream<'async_trait, CompactorMessage>,
        _result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        // CompactionJobFinished triggers new jobs and other msgs don't matter, so don't
        // process any remaining messages. Just stop the executor.
        self.stop_executor().await?;
        Ok(())
    }
}

impl CompactorEventHandler {
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        options: Arc<CompactorOptions>,
        scheduler: Arc<dyn CompactionScheduler + Send + Sync>,
        executor: Arc<dyn CompactionExecutor + Send + Sync>,
        rand: Arc<DbRand>,
        stats: Arc<CompactionStats>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let stored_manifest = StoredManifest::load(manifest_store.clone()).await?;
        let manifest = FenceableManifest::init_compactor(
            stored_manifest,
            options.manifest_update_timeout,
            system_clock.clone(),
        )
        .await?;
        let state = CompactorState::new(manifest.prepare_dirty()?);
        Ok(Self {
            state,
            manifest,
            options,
            scheduler,
            executor,
            rand,
            stats,
            system_clock,
        })
    }

    /// Emits the current compaction state and per-job progress.
    fn handle_log_ticker(&self) {
        self.log_compaction_state();
    }

    /// Handles a polling tick by refreshing the manifest and possibly scheduling compactions.
    async fn handle_ticker(&mut self) {
        if !self.is_executor_stopped() {
            let result = self.load_manifest().await;
            if result.is_err() {
                error!("error loading manifest [error={:#?}]", result.unwrap_err());
                panic!("error loading manifest");
            }
        }
    }

    /// Stops the underlying compaction executor, aborting the executor and waiting for any
    /// in-flight tasks to stop gracefully.
    async fn stop_executor(&self) -> Result<(), SlateDBError> {
        let this_executor = self.executor.clone();
        // Explicitly allow spawn_blocking for compactors since we can't trust them
        // not to block the runtime. This could cause non-determinism, since it creates
        // a race between the executor's first .await call and the runtime awaiting
        // on the join handle. We use tokio::spawn for DST since we need full determinism.
        #[cfg(not(dst))]
        #[allow(clippy::disallowed_methods)]
        let result = tokio::task::spawn_blocking(move || {
            this_executor.stop();
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        #[cfg(dst)]
        let result = tokio::spawn(async move {
            this_executor.stop();
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        result
    }

    /// ## Returns
    /// - `true` if the executor has been stopped (but still might be in the process of
    ///   shutting down).
    fn is_executor_stopped(&self) -> bool {
        self.executor.is_stopped()
    }

    /// Refreshes the manifest and updates the local compactor state with any remote
    /// changes.
    async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        self.refresh_db_state().await?;
        Ok(())
    }

    /// Persists the updated manifest after a compaction finishes.
    ///
    /// A checkpoint with a 15-minute lifetime is written first to prevent GC from
    /// deleting SSTs that are about to be removed. This is to keep them around for a
    /// while in case any in-flight operations (such as iterator scans) are still using
    /// them.
    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        // write the checkpoint first so that it points to the manifest with the ssts
        // being removed
        let checkpoint_id = self.rand.rng().gen_uuid();
        self.manifest
            .write_checkpoint(
                checkpoint_id,
                &CheckpointOptions {
                    // TODO(rohan): for now, just write a checkpoint with 15-minute expiry
                    //              so that it's extremely unlikely for the gc to delete ssts
                    //              out from underneath the writer. In a follow up, we'll write
                    //              a checkpoint with no expiry and with metadata indicating its
                    //              a compactor checkpoint. Then, the gc will delete the checkpoint
                    //              based on a configurable timeout
                    lifetime: Some(Duration::from_secs(900)),
                    ..CheckpointOptions::default()
                },
            )
            .await?;
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        let dirty = self.state.manifest().clone();
        self.manifest.update_manifest(dirty).await
    }

    /// Writes the manifest, retrying on version conflicts by reloading and retrying.
    async fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.load_manifest().await?;
            match self.write_manifest().await {
                Ok(_) => return Ok(()),
                Err(SlateDBError::ManifestVersionExists) => {
                    debug!("conflicting manifest version. updating and retrying write again.");
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Performs pre-execution validation for a proposed compaction and defers policy-specific
    /// checks to the scheduler via [`CompactionScheduler::validate_compaction`]. Invariants
    /// checked in this function:
    ///
    /// - Compaction has sources
    /// - Compactions with only L0 sources must have a destination > highest existing SR ID
    fn validate_compaction(&self, compaction: &CompactionSpec) -> Result<(), SlateDBError> {
        // Validate compaction sources exist
        if compaction.sources().is_empty() {
            warn!("submitted compaction is empty: {:?}", compaction.sources());
            return Err(SlateDBError::InvalidCompaction);
        }

        let has_only_l0 = compaction
            .sources()
            .iter()
            .all(|s| matches!(s, SourceId::Sst(_)));

        if has_only_l0 {
            // L0-only: must create new SR with id > highest_existing
            let highest_id = self
                .state
                .db_state()
                .compacted
                .first()
                .map_or(0, |sr| sr.id + 1);
            if compaction.destination() < highest_id {
                warn!("compaction destination is lesser than the expected L0-only highest_id: {:?} {:?}",
                compaction.destination(), highest_id);
                return Err(SlateDBError::InvalidCompaction);
            }
        }

        self.scheduler
            .validate_compaction(&self.state, compaction)
            .map_err(|_e| SlateDBError::InvalidCompaction)
    }

    /// Invokes the scheduler and starts accepted compactions, provided that there are fewer than
    /// [`CompactorOptions::max_concurrent_compactions`] currently running.
    async fn maybe_schedule_compactions(&mut self) -> Result<(), SlateDBError> {
        let mut specs = self.scheduler.maybe_schedule_compaction(&self.state);
        for spec in specs.drain(..) {
            let active_compactions = self.state.compactions().count();
            if active_compactions >= self.options.max_concurrent_compactions {
                info!(
                    "already running {} compactions, which is at the max {}. Won't run compaction {:?}",
                    active_compactions,
                    self.options.max_concurrent_compactions,
                    spec
                );
                break;
            }
            let compaction_id = self.rand.rng().gen_ulid(self.system_clock.as_ref());
            let compaction = Compaction::new(compaction_id, spec);
            self.submit_compaction(compaction).await?;
        }
        Ok(())
    }

    /// Creates a [`StartCompactionJobArgs`] for a [`Compaction`] and asks the executor to run it.
    async fn start_compaction(
        &mut self,
        job_id: Ulid,
        compaction: Compaction,
    ) -> Result<(), SlateDBError> {
        self.log_compaction_state();
        let db_state = self.state.db_state();

        let ssts = compaction.get_ssts(db_state);
        let sorted_runs = compaction.get_sorted_runs(db_state);
        let spec = compaction.spec();
        // if there are no SRs when we compact L0 then the resulting SR is the last sorted run.
        let is_dest_last_run = db_state.compacted.is_empty()
            || db_state
                .compacted
                .last()
                .is_some_and(|sr| spec.destination() == sr.id);

        let job_args = StartCompactionJobArgs {
            id: job_id,
            compaction_id: compaction.id(),
            destination: spec.destination(),
            ssts,
            sorted_runs,
            compaction_logical_clock_tick: db_state.last_l0_clock_tick,
            retention_min_seq: Some(db_state.recent_snapshot_min_seq),
            is_dest_last_run,
        };

        // TODO(sujeetsawala): Add job attempt to compaction
        let this_executor = self.executor.clone();
        // Explicitly allow spawn_blocking for compactors since we can't trust them
        // not to block the runtime. This could cause non-determinism, since it creates
        // a race between the executor's first .await call and the runtime awaiting
        // on the join handle. We use tokio::spawn for DST since we need full determinism.
        #[cfg(not(dst))]
        #[allow(clippy::disallowed_methods)]
        let result = tokio::task::spawn_blocking(move || {
            this_executor.start_compaction_job(job_args);
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        #[cfg(dst)]
        let result = tokio::spawn(async move {
            this_executor.start_compaction_job(job_args);
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        result
    }

    //
    // state writers
    //

    /// Records a failed compaction attempt.
    fn finish_failed_compaction(&mut self, id: Ulid) {
        self.state.remove_compaction(&id);
        self.update_compaction_low_watermark();
    }

    /// Records a successful compaction, persists the manifest, and checks for new compactions
    /// to schedule.
    #[instrument(level = "debug", skip_all, fields(id = %id))]
    async fn finish_compaction(
        &mut self,
        id: Ulid,
        output_sr: SortedRun,
    ) -> Result<(), SlateDBError> {
        self.state.finish_compaction(id, output_sr);
        self.log_compaction_state();
        self.write_manifest_safely().await?;
        self.maybe_schedule_compactions().await?;
        self.stats
            .last_compaction_ts
            .set(self.system_clock.now().timestamp() as u64);
        self.update_compaction_low_watermark();
        Ok(())
    }

    /// Validates and submits a compaction for execution.
    ///
    /// Currently, compactions are executed with a 1:1 [`Compaction`]:Compaction Job
    /// mapping. Future work may add retries or resume logic with multiple jobs per
    /// compaction.
    #[instrument(level = "debug", skip_all, fields(id = tracing::field::Empty))]
    async fn submit_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        // Validate the candidate compaction; skip invalid ones
        if let Err(e) = self.validate_compaction(compaction.spec()) {
            warn!("invalid compaction [error={:?}]", e);
            return Ok(());
        }

        self.state.add_compaction(compaction.clone())?;
        self.update_compaction_low_watermark();
        // Compactions and jobs are 1:1 right now.
        let job_id = compaction.id();
        tracing::Span::current().record("id", tracing::field::display(&job_id));
        self.start_compaction(job_id, compaction).await?;
        Ok(())
    }

    /// Merges the remote manifest view into local state and checks for new compactions
    /// to schedule.
    async fn refresh_db_state(&mut self) -> Result<(), SlateDBError> {
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        // TODO(sujeetsawala): Fetch and Run Pending Compactions from object store
        // self.run_pending_compactions().await?;
        self.maybe_schedule_compactions().await?;
        Ok(())
    }

    /// Logs the current DB runs and in-flight compactions.
    fn log_compaction_state(&self) {
        self.state.db_state().log_db_runs();
        let compactions = self.state.compactions();
        for compaction in compactions {
            info!("in-flight compaction [compaction={}]", compaction);
        }
    }

    /// Updates the [`stats::COMPACTION_LOW_WATERMARK_TS`] gauge with the earliest
    /// (oldest) ULID timestamp among active compactions.
    ///
    /// This serves as a GC safety barrier: the GC should not delete any compacted SST
    /// whose ULID timestamp is greater than or equal to this value.
    ///
    /// This is a process-local coordination mechanism that only works when the compactor
    /// and garbage collector run in the same process and share the same StatRegistry. It's
    /// a hack until we have proper compactor persistence (so GC can retrieve the compactor
    /// state from the object store). See #604 for details.
    fn update_compaction_low_watermark(&self) {
        let mut min_ts = u64::MAX;
        for compaction in self.state.compactions() {
            let ts = compaction
                .id()
                .datetime()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .expect("invalid duration");
            min_ts = min_ts.min(ts);
        }
        self.stats.compaction_low_watermark_ts.set(min_ts);
    }
}

pub mod stats {
    use crate::stats::{Counter, Gauge, StatRegistry};
    use std::sync::Arc;

    macro_rules! compactor_stat_name {
        ($suffix:expr) => {
            crate::stat_name!("compactor", $suffix)
        };
    }

    pub const BYTES_COMPACTED: &str = compactor_stat_name!("bytes_compacted");
    pub const LAST_COMPACTION_TS_SEC: &str = compactor_stat_name!("last_compaction_timestamp_sec");
    pub const RUNNING_COMPACTIONS: &str = compactor_stat_name!("running_compactions");
    /// The earliest (oldest) ULID timestamp among active compactions. See
    /// [super::CompactorEventHandler::update_longest_running_start_metric] for details.
    pub const COMPACTION_LOW_WATERMARK_TS: &str =
        compactor_stat_name!("compaction_low_watermark_ts");

    pub(crate) struct CompactionStats {
        pub(crate) last_compaction_ts: Arc<Gauge<u64>>,
        pub(crate) running_compactions: Arc<Gauge<i64>>,
        pub(crate) bytes_compacted: Arc<Counter>,
        pub(crate) compaction_low_watermark_ts: Arc<Gauge<u64>>,
    }

    impl CompactionStats {
        /// Registers and returns a new set of compactor metrics in the provided registry.
        ///
        /// ## Metrics
        /// - `last_compaction_timestamp_sec`: Unix timestamp of the last completed compaction.
        /// - `running_compactions`: Gauge tracking active compaction attempts.
        /// - `bytes_compacted`: Counter of bytes written by the executor.
        /// - `compaction_low_watermark_ts`: Earliest ULID timestamp among active compactions (GC hint).
        pub(crate) fn new(stat_registry: Arc<StatRegistry>) -> Self {
            let stats = Self {
                last_compaction_ts: Arc::new(Gauge::default()),
                running_compactions: Arc::new(Gauge::default()),
                bytes_compacted: Arc::new(Counter::default()),
                compaction_low_watermark_ts: Arc::new(Gauge::default()),
            };
            stat_registry.register(LAST_COMPACTION_TS_SEC, stats.last_compaction_ts.clone());
            stat_registry.register(RUNNING_COMPACTIONS, stats.running_compactions.clone());
            stat_registry.register(BYTES_COMPACTED, stats.bytes_compacted.clone());
            stat_registry.register(
                COMPACTION_LOW_WATERMARK_TS,
                stats.compaction_low_watermark_ts.clone(),
            );
            stats
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use rand::RngCore;
    use ulid::Ulid;

    use super::*;
    use crate::clock::DefaultSystemClock;
    use crate::compactor::stats::CompactionStats;
    use crate::compactor_executor::{CompactionExecutor, TokioCompactionExecutor};
    use crate::compactor_state::{CompactorState, SourceId};
    use crate::compactor_stats::LAST_COMPACTION_TS_SEC;
    use crate::config::{
        PutOptions, Settings, SizeTieredCompactionSchedulerOptions, Ttl, WriteOptions,
    };
    use crate::db::Db;
    use crate::db_state::{CoreDbState, SortedRun};
    use crate::error::SlateDBError;
    use crate::iter::KeyValueIterator;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::merge_operator::{MergeOperator, MergeOperatorError};
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::rng;
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, TestClock};
    use crate::types::RowEntry;
    use bytes::Bytes;

    const PATH: &str = "/test/db";

    struct StringConcatMergeOperator;

    impl MergeOperator for StringConcatMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            value: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            let mut result = existing_value.unwrap_or_default().as_ref().to_vec();
            result.extend_from_slice(&value);
            Ok(Bytes::from(result))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compactor_compacts_l0() {
        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
            SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: 1,
                max_compaction_sources: 999,
                include_size_threshold: 4.0,
            },
        ));
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .build()
            .await
            .unwrap();

        let (_, table_store) = build_test_stores(os.clone());
        let mut expected = HashMap::<Vec<u8>, Vec<u8>>::new();
        for i in 0..4 {
            let k = vec![b'a' + i as u8; 16];
            let v = vec![b'b' + i as u8; 48];
            expected.insert(k.clone(), v.clone());
            db.put(&k, &v).await.unwrap();
            let k = vec![b'j' + i as u8; 16];
            let v = vec![b'k' + i as u8; 48];
            db.put(&k, &v).await.unwrap();
            expected.insert(k.clone(), v.clone());
        }

        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        for run in db_state.compacted {
            for sst in run.ssts {
                let mut iter = SstIterator::new_borrowed_initialized(
                    ..,
                    &sst,
                    table_store.clone(),
                    SstIteratorOptions::default(),
                )
                .await
                .unwrap()
                .expect("Expected Some(iter) but got None");

                // remove the key from the expected map and verify that the db matches
                while let Some(kv) = iter.next().await.unwrap() {
                    let expected_v = expected
                        .remove(kv.key.as_ref())
                        .expect("removing unexpected key");
                    let db_v = db.get(kv.key.as_ref()).await.unwrap().unwrap();
                    assert_eq!(expected_v, db_v.as_ref());
                }
            }
        }
        assert!(expected.is_empty());
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_tombstones_in_l0() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| {
                // compact when there are at least 2 SSTs in L0 (one for key 'a' and one for key 'b')
                state.db_state().l0.len() == 2 ||
                // or when there is one SST in L0 and one in L1 (one for delete key 'a' and one for compacted key 'a'+'b')
                (state.db_state().l0.len() == 1 && state.db_state().compacted.len() == 1)
            },
        )));

        let mut options = db_options(Some(compactor_options()));
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(scheduler.clone())
            .build()
            .await
            .unwrap();

        let (manifest_store, table_store) = build_test_stores(os.clone());

        // put key 'a' into L1 (and key 'b' so that when we delete 'a' the SST is non-empty)
        // since these are both await_durable=true, we're guaranteed to have one L0 SST for each.
        db.put(&[b'a'; 16], &[b'a'; 32]).await.unwrap();
        db.put(&[b'b'; 16], &[b'a'; 32]).await.unwrap();
        db.flush().await.unwrap();
        let db_state = await_compaction(&db).await.unwrap();
        assert_eq!(db_state.compacted.len(), 1);
        assert_eq!(db_state.l0.len(), 0, "{:?}", db_state.l0);

        // put tombstone for key a into L0
        db.delete_with_options(
            &[b'a'; 16],
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        // Then:
        // we should now have a tombstone in L0 and a value in L1
        let db_state = get_db_state(manifest_store.clone()).await;
        assert_eq!(db_state.l0.len(), 1, "{:?}", db_state.l0);
        assert_eq!(db_state.compacted.len(), 1);

        let l0 = db_state.l0.front().unwrap();
        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            l0,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        let tombstone = iter.next_entry().await.unwrap();
        assert!(tombstone.unwrap().value.is_tombstone());

        let db_state = await_compacted_compaction(manifest_store.clone(), db_state.compacted)
            .await
            .unwrap();
        assert_eq!(db_state.compacted.len(), 1);

        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // should be no tombstone for key 'a' because it was filtered
        // out of the last run
        let next = iter.next().await.unwrap();
        assert_eq!(next.unwrap().key.as_ref(), &[b'b'; 16]);
        let next = iter.next().await.unwrap();
        assert!(next.is_none());
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_not_filter_tombstone_with_snapshot() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| {
                // compact when there are at least 2 SSTs in L0
                state.db_state().l0.len() == 2 ||
                // or when there is one SST in L0 and one in L1
                (state.db_state().l0.len() == 1 && state.db_state().compacted.len() == 1)
            },
        )));

        let mut options = db_options(Some(compactor_options()));
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(scheduler.clone())
            .build()
            .await
            .unwrap();

        let (manifest_store, table_store) = build_test_stores(os.clone());

        // Write and flush key 'a' first (seq=1)
        db.put(&[b'a'; 16], &[b'a'; 32]).await.unwrap();
        db.put(&[b'b'; 16], &[b'a'; 32]).await.unwrap();

        // Create a snapshot after first flush. This protects seq >= 1
        let _snapshot = db.snapshot().await.unwrap();
        db.flush().await.unwrap();

        // Compact L0 to L1
        let db_state = await_compaction(&db).await.unwrap();
        assert_eq!(db_state.compacted.len(), 1);
        assert_eq!(db_state.l0.len(), 0, "{:?}", db_state.l0);

        // Now delete key 'a', creating a tombstone (seq=3)
        db.delete_with_options(
            &[b'a'; 16],
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        // We should now have a tombstone for 'a' in L0 and both 'a' and 'b' values in L1
        let db_state = get_db_state(manifest_store.clone()).await;
        assert_eq!(db_state.l0.len(), 1, "{:?}", db_state.l0);
        assert_eq!(db_state.compacted.len(), 1);

        // Trigger compaction of L0 (tombstone) + L1 (values)
        let db_state = await_compacted_compaction(manifest_store.clone(), db_state.compacted)
            .await
            .unwrap();
        assert_eq!(db_state.compacted.len(), 1);

        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // After compaction with an active snapshot (protecting seq >= 1):
        // - Key 'a': Both the tombstone(seq=3) and original value(seq=1) are protected
        //   The SST contains both versions, but the iterator returns the latest (tombstone)
        // - Key 'b': value(seq=2) is protected
        // Expected result: both 'a' (as tombstone) and 'b' (as value) should be present
        let next = iter.next().await.unwrap();
        let entry_a = next.unwrap();
        assert_eq!(entry_a.key.as_ref(), &[b'a'; 16]);

        let next = iter.next().await.unwrap();
        let entry_b = next.unwrap();
        assert_eq!(entry_b.key.as_ref(), &[b'b'; 16]);

        let next = iter.next().await.unwrap();
        assert!(
            next.is_none(),
            "Expected two keys (a and b) in the compacted SST"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_apply_merge_during_l0_compaction() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.db_state().l0.len() >= 2,
        )));
        let options = db_options(Some(compactor_options()));

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, table_store) = build_test_stores(os.clone());

        // write merge operations across multiple L0 SSTs
        db.merge(b"key1", b"a").await.unwrap();
        db.merge(b"key1", b"b").await.unwrap();
        db.put(&vec![b'x'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        db.merge(b"key1", b"c").await.unwrap();
        db.merge(b"key2", b"x").await.unwrap();
        db.put(&vec![b'y'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.compacted.len(), 1);
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_merge(b"key1", b"abc", 4).with_create_ts(0),
                RowEntry::new_merge(b"key2", b"x", 5).with_create_ts(0),
                RowEntry::new_value(&[b'x'; 16], &[b'p'; 128], 3).with_create_ts(0),
                RowEntry::new_value(&[b'y'; 16], &[b'p'; 128], 6).with_create_ts(0),
            ],
        )
        .await;

        // verify the merged values are readable from the db
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result, Some(Bytes::from("abc")));
        let result = db.get(b"key2").await.unwrap();
        assert_eq!(result, Some(Bytes::from("x")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_apply_merge_across_l0_and_sorted_runs() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| !state.db_state().l0.is_empty(),
        )));
        let options = db_options(Some(compactor_options()));

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, table_store) = build_test_stores(os.clone());

        // write initial merge operations and compact to L1
        db.merge(b"key1", b"a").await.unwrap();
        db.merge(b"key1", b"b").await.unwrap();
        db.put(&vec![b'x'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        let db_state = await_compaction(&db).await;
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.compacted.len(), 1);

        // write more merge operations to L0
        db.merge(b"key1", b"c").await.unwrap();
        db.merge(b"key1", b"d").await.unwrap();
        db.put(&vec![b'y'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when: compact L0 with the existing sorted run
        let db_state = await_compaction(&db).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.compacted.len(), 1);
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_merge(b"key1", b"abcd", 5).with_create_ts(0),
                RowEntry::new_value(&[b'x'; 16], &[b'p'; 128], 3).with_create_ts(0),
                RowEntry::new_value(&[b'y'; 16], &[b'p'; 128], 6).with_create_ts(0),
            ],
        )
        .await;

        // verify the merged value is readable from the db
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result, Some(Bytes::from("abcd")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_merge_without_base_value() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.db_state().l0.len() >= 2,
        )));
        let options = db_options(Some(compactor_options()));

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, table_store) = build_test_stores(os.clone());

        // write only merge operations without any base value
        db.merge(b"key1", b"x").await.unwrap();
        db.put(&vec![b'x'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        db.merge(b"key1", b"y").await.unwrap();
        db.merge(b"key1", b"z").await.unwrap();
        db.put(&vec![b'y'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.compacted.len(), 1);
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_merge(b"key1", b"xyz", 4).with_create_ts(0),
                RowEntry::new_value(&[b'x'; 16], &[b'p'; 128], 2).with_create_ts(0),
                RowEntry::new_value(&[b'y'; 16], &[b'p'; 128], 5).with_create_ts(0),
            ],
        )
        .await;

        // verify the merged value is readable from the db
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result, Some(Bytes::from("xyz")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_preserve_merge_order_across_multiple_ssts() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.db_state().l0.len() >= 3,
        )));
        let options = db_options(Some(compactor_options()));

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, table_store) = build_test_stores(os.clone());

        // write merge operations across three L0 SSTs in specific order
        db.merge(b"key1", b"1").await.unwrap();
        db.put(&vec![b'a'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        db.merge(b"key1", b"2").await.unwrap();
        db.put(&vec![b'b'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        db.merge(b"key1", b"3").await.unwrap();
        db.put(&vec![b'c'; 16], &vec![b'p'; 128]).await.unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.compacted.len(), 1);
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // merges should be applied in chronological order: 1, 2, 3
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(&[b'a'; 16], &[b'p'; 128], 2).with_create_ts(0),
                RowEntry::new_value(&[b'b'; 16], &[b'p'; 128], 4).with_create_ts(0),
                RowEntry::new_value(&[b'c'; 16], &[b'p'; 128], 6).with_create_ts(0),
                RowEntry::new_merge(b"key1", b"123", 5).with_create_ts(0),
            ],
        )
        .await;

        // verify the merged value is readable from the db
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result, Some(Bytes::from("123")));
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_not_compact_expired_merge_operations_in_last_run() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let insert_clock = Arc::new(TestClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.db_state().l0.len() >= 2,
        )));

        let mut options = db_options(Some(compactor_options()));
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(insert_clock.clone())
            .with_compaction_scheduler_supplier(scheduler)
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, table_store) = build_test_stores(os.clone());

        // ticker time = 0, expire time = 10
        insert_clock.ticker.store(0, atomic::Ordering::SeqCst);
        db.merge_with_options(
            b"key1",
            &[b'a'; 32],
            &crate::config::MergeOptions {
                ttl: Ttl::ExpireAfter(10),
            },
            &WriteOptions {
                await_durable: true,
            },
        )
        .await
        .unwrap();

        // ticker time = 20, no expire time
        insert_clock.ticker.store(20, atomic::Ordering::SeqCst);
        db.merge_with_options(
            b"key1",
            &[b'b'; 32],
            &crate::config::MergeOptions { ttl: Ttl::NoExpiry },
            &WriteOptions {
                await_durable: true,
            },
        )
        .await
        .unwrap();

        let db_state = await_compaction(&db).await.unwrap();
        assert_eq!(db_state.compacted.len(), 1);
        assert_eq!(db_state.last_l0_clock_tick, 20);

        // then: the compacted SST should only contain the non-expired merge
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // only the non-expired merge "b" should be present
        assert_iterator(
            &mut iter,
            vec![RowEntry::new_merge(b"key1", &[b'b'; 32], 2).with_create_ts(20)],
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_merge_and_then_overwrite_with_put() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.db_state().l0.len() >= 2,
        )));
        let options = db_options(Some(compactor_options()));

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (manifest_store, _table_store) = build_test_stores(os.clone());

        // write merge operations
        db.merge(b"key1", b"a").await.unwrap();
        db.merge(b"key1", b"b").await.unwrap();
        db.put(&vec![b'x'; 16], &vec![b'p'; 128]).await.unwrap(); // padding
        db.flush().await.unwrap();

        // then overwrite with a put
        db.put(b"key1", b"new_value").await.unwrap();
        db.put(&vec![b'y'; 16], &vec![b'p'; 128]).await.unwrap(); // padding
        db.flush().await.unwrap();

        // when:
        // Wait a bit for compaction to occur
        tokio::time::sleep(Duration::from_secs(2)).await;

        // then: verify the put value overwrote the merge
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result, Some(Bytes::from("new_value")));

        // verify the compacted state in manifest
        let stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        let db_state = stored_manifest.db_state();
        assert!(
            !db_state.compacted.is_empty(),
            "compaction should have occurred"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_not_merge_operations_with_different_expire_times() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.db_state().l0.len() >= 2,
        )));
        let options = db_options(Some(compactor_options()));

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (manifest_store, table_store) = build_test_stores(os.clone());

        // write merge operations with different TTLs
        db.merge_with_options(
            b"key1",
            b"a",
            &crate::config::MergeOptions {
                ttl: Ttl::ExpireAfter(100),
            },
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.put(&vec![b'x'; 16], &vec![b'p'; 128]).await.unwrap(); // padding
        db.flush().await.unwrap();

        db.merge_with_options(
            b"key1",
            b"b",
            &crate::config::MergeOptions {
                ttl: Ttl::ExpireAfter(200),
            },
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.put(&vec![b'y'; 16], &vec![b'p'; 128]).await.unwrap(); // padding
        db.flush().await.unwrap();

        // when:
        // Wait a bit for compaction to occur
        tokio::time::sleep(Duration::from_secs(2)).await;

        // then: verify that merges with different expire times are NOT merged together
        // Reading should get only the latest merge since they weren't combined
        let stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        let db_state = stored_manifest.db_state();
        assert!(
            !db_state.compacted.is_empty(),
            "compaction should have occurred"
        );

        // The compacted sorted run should contain both merge operations separately
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // merge operations should be kept separate due to different expire times
        let mut key1_entries = vec![];
        while let Some(entry) = iter.next_entry().await.unwrap() {
            if entry.key.as_ref() == b"key1" {
                key1_entries.push(entry);
            }
        }
        // We should have at least 1 merge operation for key1
        assert!(
            !key1_entries.is_empty(),
            "should have merge operations for key1"
        );
        // All entries for key1 should be merge operations
        assert!(key1_entries
            .iter()
            .all(|e| matches!(e.value, crate::types::ValueDeletable::Merge(_))));
        // If there are 2 entries, they should have different expire times
        if key1_entries.len() == 2 {
            assert_ne!(
                key1_entries[0].expire_ts, key1_entries[1].expire_ts,
                "separate merge operations should have different expire times"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_compact_expired_entries() {
        // given:
        let os = Arc::new(InMemory::new());
        let insert_clock = Arc::new(TestClock::new());

        let compaction_scheduler = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
            SizeTieredCompactionSchedulerOptions {
                // We'll do exactly two flushes in this test, resulting in 2 L0 files.
                min_compaction_sources: 2,
                max_compaction_sources: 2,
                include_size_threshold: 4.0,
            },
        ));

        let mut options = db_options(Some(compactor_options()));
        options.default_ttl = Some(50);
        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(insert_clock.clone())
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .build()
            .await
            .unwrap();

        let (_, table_store) = build_test_stores(os.clone());

        let value = &[b'a'; 64];

        // ticker time = 0, expire time = 10
        insert_clock.ticker.store(0, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[1; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAfter(10),
            },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        // ticker time = 10, expire time = 60 (using default TTL)
        insert_clock.ticker.store(10, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[2; 16],
            value,
            &PutOptions { ttl: Ttl::Default },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        db.flush().await.unwrap();

        // ticker time = 30, no expire time
        insert_clock.ticker.store(30, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[3; 16],
            value,
            &PutOptions { ttl: Ttl::NoExpiry },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        // this revives key 1
        // ticker time = 70, expire time 80
        insert_clock.ticker.store(70, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[1; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAfter(80),
            },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert!(db_state.l0_last_compacted.is_some());
        assert_eq!(db_state.compacted.len(), 1);
        assert_eq!(db_state.last_l0_clock_tick, 70);
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();
        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(&[1; 16], value, 4)
                    .with_create_ts(70)
                    .with_expire_ts(150),
                // no tombstone for &[2; 16] because this is the last layer of the tree,
                RowEntry::new_value(&[3; 16], value, 3).with_create_ts(30),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_compaction_low_watermark_uses_min_ulid_time() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;

        // Two artificial compactions with known ULID timestamps
        let older_ts_ms: u64 = 1_000;
        let newer_ts_ms: u64 = 2_000;
        let compaction_old = Compaction::new(
            Ulid::from_parts(older_ts_ms, 0),
            CompactionSpec::new(vec![], 10),
        );
        let compaction_new = Compaction::new(
            Ulid::from_parts(newer_ts_ms, 0),
            CompactionSpec::new(vec![], 11),
        );

        fixture
            .handler
            .state
            .add_compaction(compaction_old)
            .expect("failed to add old compaction");
        fixture
            .handler
            .state
            .add_compaction(compaction_new)
            .expect("failed to add new compaction");

        // Update the metric and verify it matches the oldest compaction's ULID time
        fixture.handler.update_compaction_low_watermark();
        assert_eq!(
            fixture.handler.stats.compaction_low_watermark_ts.value(),
            older_ts_ms
        );
    }

    struct CompactorEventHandlerTestFixture {
        manifest: StoredManifest,
        manifest_store: Arc<ManifestStore>,
        options: Settings,
        db: Db,
        scheduler: Arc<MockScheduler>,
        executor: Arc<MockExecutor>,
        real_executor: Arc<dyn CompactionExecutor>,
        real_executor_rx: tokio::sync::mpsc::UnboundedReceiver<CompactorMessage>,
        stats_registry: Arc<StatRegistry>,
        handler: CompactorEventHandler,
    }

    impl CompactorEventHandlerTestFixture {
        async fn new() -> Self {
            let compactor_options = Arc::new(compactor_options());
            let options = db_options(None);

            let os = Arc::new(InMemory::new());
            let (manifest_store, table_store) = build_test_stores(os.clone());
            let db = Db::builder(PATH, os.clone())
                .with_settings(options.clone())
                .build()
                .await
                .unwrap();

            let scheduler = Arc::new(MockScheduler::new());
            let executor = Arc::new(MockExecutor::new());
            let (real_executor_tx, real_executor_rx) = tokio::sync::mpsc::unbounded_channel();
            let rand = Arc::new(DbRand::default());
            let stats_registry = Arc::new(StatRegistry::new());
            let compactor_stats = Arc::new(CompactionStats::new(stats_registry.clone()));
            let real_executor = Arc::new(TokioCompactionExecutor::new(
                Handle::current(),
                compactor_options.clone(),
                real_executor_tx,
                table_store,
                rand.clone(),
                compactor_stats.clone(),
                Arc::new(DefaultSystemClock::new()),
                manifest_store.clone(),
                options.merge_operator.clone(),
            ));
            let handler = CompactorEventHandler::new(
                manifest_store.clone(),
                compactor_options.clone(),
                scheduler.clone(),
                executor.clone(),
                rand.clone(),
                compactor_stats.clone(),
                Arc::new(DefaultSystemClock::new()),
            )
            .await
            .unwrap();
            let manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
            Self {
                manifest,
                manifest_store,
                options,
                db,
                scheduler,
                executor,
                real_executor_rx,
                real_executor,
                stats_registry,
                handler,
            }
        }

        async fn latest_db_state(&mut self) -> CoreDbState {
            self.manifest.refresh().await.unwrap().core.clone()
        }

        async fn write_l0(&mut self) {
            let mut rng = rng::new_test_rng(None);
            let manifest = self.manifest.refresh().await.unwrap();
            let l0s = manifest.core.l0.len();
            // TODO: add an explicit flush_memtable fn to db and use that instead
            let mut k = vec![0u8; self.options.l0_sst_size_bytes];
            rng.fill_bytes(&mut k);
            self.db.put(&k, &[b'x'; 10]).await.unwrap();
            self.db.flush().await.unwrap();
            loop {
                let manifest = self.manifest.refresh().await.unwrap().clone();
                if manifest.core.l0.len() > l0s {
                    break;
                }
            }
        }

        async fn build_l0_compaction(&mut self) -> CompactionSpec {
            let db_state = self.latest_db_state().await;
            let l0_ids_to_compact: Vec<SourceId> = db_state
                .l0
                .iter()
                .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
                .collect();
            CompactionSpec::new(l0_ids_to_compact, 0)
        }

        fn assert_started_compaction(&self, num: usize) -> Vec<StartCompactionJobArgs> {
            let attempts = self.executor.pop_jobs();
            assert_eq!(num, attempts.len());
            attempts
        }

        fn assert_and_forward_compactions(&self, num: usize) {
            for c in self.assert_started_compaction(num) {
                self.real_executor.start_compaction_job(c)
            }
        }
    }

    #[tokio::test]
    async fn test_should_record_last_compaction_ts() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), async {
            match fixture.real_executor_rx.recv().await {
                Some(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Some(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                None => panic!("channel closed before receiving any message"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");
        let starting_last_ts = fixture
            .stats_registry
            .lookup(LAST_COMPACTION_TS_SEC)
            .unwrap()
            .get();

        // when:
        fixture
            .handler
            .handle(msg)
            .await
            .expect("fatal error handling compaction message");

        // then:
        assert!(
            fixture
                .stats_registry
                .lookup(LAST_COMPACTION_TS_SEC)
                .unwrap()
                .get()
                > starting_last_ts
        );
    }

    #[tokio::test]
    async fn test_should_write_manifest_safely() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), async {
            match fixture.real_executor_rx.recv().await {
                Some(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Some(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                None => panic!("channel closed before receiving any message"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");
        // write an l0 before handling compaction finished
        fixture.write_l0().await;

        // when:
        fixture
            .handler
            .handle(msg)
            .await
            .expect("fatal error handling compaction message");

        // then:
        let db_state = fixture.latest_db_state().await;
        assert_eq!(db_state.l0.len(), 1);
        assert_eq!(db_state.compacted.len(), 1);
        let l0_id = db_state.l0.front().unwrap().id.unwrap_compacted_id();
        let compacted_l0s: Vec<Ulid> = db_state
            .compacted
            .first()
            .unwrap()
            .ssts
            .iter()
            .map(|sst| sst.id.unwrap_compacted_id())
            .collect();
        assert!(!compacted_l0s.contains(&l0_id));
        assert_eq!(
            db_state.l0_last_compacted.unwrap(),
            compaction
                .sources()
                .first()
                .and_then(|id| id.maybe_unwrap_sst())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_should_clear_compaction_on_failure_and_retry() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        let job = fixture.assert_started_compaction(1).pop().unwrap();
        let msg = CompactorMessage::CompactionJobFinished {
            id: job.id,
            result: Err(SlateDBError::InvalidDBState),
        };

        // when:
        fixture
            .handler
            .handle(msg)
            .await
            .expect("fatal error handling compaction message");

        // then:
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_started_compaction(1);
    }

    #[tokio::test]
    async fn test_should_not_schedule_conflicting_compaction() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_started_compaction(1);
        fixture.write_l0().await;
        fixture.scheduler.inject_compaction(compaction.clone());

        // when:
        fixture.handler.handle_ticker().await;

        // then:
        assert_eq!(0, fixture.executor.pop_jobs().len())
    }

    #[tokio::test]
    async fn test_should_leave_checkpoint_when_removing_ssts_after_compaction() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), async {
            match fixture.real_executor_rx.recv().await {
                Some(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Some(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                None => panic!("channel closed before receiving any message"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");
        // when:
        fixture
            .handler
            .handle(msg)
            .await
            .expect("fatal error handling compaction message");

        // then:
        let current_dbstate = fixture.latest_db_state().await;
        let checkpoint = current_dbstate.checkpoints.last().unwrap();
        let old_manifest = fixture
            .manifest_store
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();
        let l0_ids: Vec<SourceId> = old_manifest
            .core
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .collect();
        assert_eq!(&l0_ids, compaction.sources());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(feature = "zstd")]
    async fn test_compactor_compressed_block_size() {
        use crate::compactor_stats::BYTES_COMPACTED;
        use crate::config::{CompressionCodec, SstBlockSize};

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
            SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: 1,
                max_compaction_sources: 999,
                include_size_threshold: 4.0,
            },
        ));

        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 128;
        options.compression_codec = Some(CompressionCodec::Zstd);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_sst_block_size(SstBlockSize::Other(128))
            .build()
            .await
            .unwrap();

        for i in 0..4 {
            let k = vec![b'a' + i as u8; 16];
            let v = vec![b'b' + i as u8; 48];
            db.put(&k, &v).await.unwrap();
            let k = vec![b'j' + i as u8; 16];
            let v = vec![b'k' + i as u8; 48];
            db.put(&k, &v).await.unwrap();
        }

        db.flush().await.unwrap();

        // when:
        await_compaction(&db).await.expect("db was not compacted");

        // then:
        let metrics = db.metrics();
        let bytes_compacted = metrics.lookup(BYTES_COMPACTED).unwrap().get();

        assert!(bytes_compacted > 0, "bytes_compacted: {}", bytes_compacted);
    }

    #[tokio::test]
    async fn test_validate_compaction_empty_sources_rejected() {
        let fixture = CompactorEventHandlerTestFixture::new().await;
        let c = CompactionSpec::new(Vec::new(), 0);
        let err = fixture.handler.validate_compaction(&c).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    #[tokio::test]
    async fn test_validate_compaction_l0_only_ok_when_no_sr() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        // ensure at least one L0 exists
        fixture.write_l0().await;
        let c = fixture.build_l0_compaction().await;
        fixture.handler.validate_compaction(&c).unwrap();
    }

    #[tokio::test]
    async fn test_validate_compaction_l0_only_rejects_when_dest_below_highest_sr() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        // write L0 and compact to create SR id 0
        fixture.write_l0().await;
        let c1 = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(c1.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), async {
            match fixture.real_executor_rx.recv().await {
                Some(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Some(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                None => panic!("channel closed before receiving any message"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");
        fixture.handler.handle(msg).await.unwrap();

        // now highest_id should be 1; build L0-only compaction with dest 0 (below highest)
        fixture.write_l0().await;
        let c2 = fixture.build_l0_compaction().await; // destination 0
        let err = fixture.handler.validate_compaction(&c2).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    #[tokio::test]
    async fn test_validate_compaction_mixed_l0_and_sr_deferred_to_scheduler() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        // create one SR so we can reference its id
        fixture.write_l0().await;
        let c1 = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(c1.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), async {
            match fixture.real_executor_rx.recv().await {
                Some(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Some(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                None => panic!("channel closed before receiving any message"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");
        fixture.handler.handle(msg).await.unwrap();

        // prepare a mixed compaction: one SR source and one L0 source
        fixture.write_l0().await;
        let state = fixture.latest_db_state().await;
        let sr_id = state.compacted.first().unwrap().id;
        let l0_ulid = state.l0.front().unwrap().id.unwrap_compacted_id();
        let mixed = CompactionSpec::new(
            vec![SourceId::SortedRun(sr_id), SourceId::Sst(l0_ulid)],
            sr_id,
        );
        // Compactor-level validation should not reject (scheduler default validate returns Ok(()))
        fixture.handler.validate_compaction(&mixed).unwrap();
    }

    async fn run_for<T, F>(duration: Duration, f: impl Fn() -> F) -> Option<T>
    where
        F: Future<Output = Option<T>>,
    {
        #[allow(clippy::disallowed_methods)]
        let now = SystemTime::now();
        while now.elapsed().unwrap() < duration {
            let maybe_result = f().await;
            if maybe_result.is_some() {
                return maybe_result;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    fn build_test_stores(os: Arc<dyn ObjectStore>) -> (Arc<ManifestStore>, Arc<TableStore>) {
        let sst_format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(PATH),
            os.clone(),
            Arc::new(DefaultSystemClock::new()),
        ));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            sst_format,
            Path::from(PATH),
            None,
        ));
        (manifest_store, table_store)
    }

    /// Waits until all writes have made their way to L1 or below. No data is allowed in
    /// in-memory WALs, in-memory memtables, or L0 SSTs on object storage.
    async fn await_compaction(db: &Db) -> Option<CoreDbState> {
        run_for(Duration::from_secs(10), || async {
            let (empty_wal, empty_memtable, core_db_state) = {
                let db_state = db.inner.state.read();
                let cow_db_state = db_state.state();
                (
                    db.inner.wal_buffer.is_empty(),
                    db_state.memtable().is_empty() && cow_db_state.imm_memtable.is_empty(),
                    db_state.state().core().clone(),
                )
            };

            let empty_l0 = core_db_state.l0.is_empty();
            let compaction_ran = !core_db_state.compacted.is_empty();
            if empty_wal && empty_memtable && empty_l0 && compaction_ran {
                return Some(core_db_state);
            }
            None
        })
        .await
    }

    #[allow(unused)] // only used with feature(wal_disable)
    async fn await_compacted_compaction(
        manifest_store: Arc<ManifestStore>,
        old_compacted: Vec<SortedRun>,
    ) -> Option<CoreDbState> {
        run_for(Duration::from_secs(10), || async {
            let db_state = get_db_state(manifest_store.clone()).await;
            if !db_state.compacted.eq(&old_compacted) {
                return Some(db_state);
            }
            None
        })
        .await
    }

    async fn get_db_state(manifest_store: Arc<ManifestStore>) -> CoreDbState {
        let stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        stored_manifest.db_state().clone()
    }

    fn db_options(compactor_options: Option<CompactorOptions>) -> Settings {
        Settings {
            flush_interval: Some(Duration::from_millis(100)),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            manifest_update_timeout: Duration::from_secs(300),
            l0_sst_size_bytes: 256,
            l0_max_ssts: 8,
            compactor_options,
            ..Settings::default()
        }
    }

    fn compactor_options() -> CompactorOptions {
        CompactorOptions {
            poll_interval: Duration::from_millis(100),
            max_concurrent_compactions: 1,
            ..CompactorOptions::default()
        }
    }

    struct MockSchedulerInner {
        compaction: Vec<CompactionSpec>,
    }

    #[derive(Clone)]
    struct MockScheduler {
        inner: Arc<Mutex<MockSchedulerInner>>,
    }

    impl MockScheduler {
        fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockSchedulerInner { compaction: vec![] })),
            }
        }

        fn inject_compaction(&self, compaction: CompactionSpec) {
            let mut inner = self.inner.lock();
            inner.compaction.push(compaction);
        }
    }

    impl CompactionScheduler for MockScheduler {
        fn maybe_schedule_compaction(&self, _state: &CompactorState) -> Vec<CompactionSpec> {
            let mut inner = self.inner.lock();
            std::mem::take(&mut inner.compaction)
        }
    }

    struct MockExecutorInner {
        jobs: Vec<StartCompactionJobArgs>,
    }

    #[derive(Clone)]
    struct MockExecutor {
        inner: Arc<Mutex<MockExecutorInner>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockExecutorInner { jobs: vec![] })),
            }
        }

        fn pop_jobs(&self) -> Vec<StartCompactionJobArgs> {
            let mut guard = self.inner.lock();
            std::mem::take(&mut guard.jobs)
        }
    }

    impl CompactionExecutor for MockExecutor {
        fn start_compaction_job(&self, compaction: StartCompactionJobArgs) {
            let mut guard = self.inner.lock();
            guard.jobs.push(compaction);
        }

        fn stop(&self) {}

        fn is_stopped(&self) -> bool {
            false
        }
    }
}
