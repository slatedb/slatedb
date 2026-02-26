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
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tracing::instrument;
use ulid::Ulid;

#[cfg(feature = "compaction_filters")]
use crate::compaction_filter::CompactionFilterSupplier;
use crate::compactions_store::{CompactionsStore, StoredCompactions};
use crate::compactor::stats::CompactionStats;
use crate::compactor_executor::{
    CompactionExecutor, StartCompactionJobArgs, TokioCompactionExecutor,
    TokioCompactionExecutorOptions,
};
use crate::compactor_state_protocols::CompactorStateWriter;
use crate::config::CompactorOptions;
use crate::db_state::SortedRun;
use crate::dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor};
use crate::error::{Error, SlateDBError};
use crate::manifest::store::ManifestStore;
use crate::manifest::SsTableHandle;
use crate::merge_operator::MergeOperatorType;
use crate::rand::DbRand;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::utils::{format_bytes_si, IdGenerator, WatchableOnceCell};
use slatedb_common::clock::SystemClock;

pub use crate::compactor_state::{
    Compaction, CompactionSpec, CompactionStatus, CompactionsCore, CompactorState, SourceId,
};
pub use crate::compactor_state_protocols::CompactorStateView;
pub use crate::db::builder::CompactorBuilder;
pub use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;

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
    /// Proposes compaction specs for the current state.
    ///
    /// ## Arguments
    /// - `state`: Process-local view of the DB's manifest and compactions.
    ///
    /// ## Returns
    /// - A list of [`CompactionSpec`] describing what to compact and where to write.
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec>;

    /// Validates a candidate compaction spec against scheduler-specific invariants. The
    /// default implementation accepts everything. Schedulers can override to enforce
    /// policy-specific constraints prior to execution (e.g. level rules, overlaps).
    ///
    /// Not all compactions have to originate from scheduler. Admin/“manual” compactions,
    /// persisted/resumed plans, or tests can construct a [`CompactionSpec`]`. This
    /// method can be used to validate external compaction specs against scheduler
    /// specific invariants.
    ///
    /// ## Arguments
    /// - `_state`: Process-local view of the DB's manifest and compactions.
    /// - `_spec`: Proposed [`CompactionSpec`].
    ///
    /// ## Returns
    /// - `Ok(())` if valid, or an [`Error`] if invalid.
    fn validate(&self, _state: &CompactorStateView, _spec: &CompactionSpec) -> Result<(), Error> {
        Ok(())
    }

    /// Generate a compaction based on a compaction request.
    ///
    /// - If the request is a `CompactionRequest::Spec`, it simply returns the provided spec.
    /// - If the request is a `CompactionRequest::Full`, it generates a compaction spec that
    ///   includes all current L0 SSTs and sorted runs as sources, and selects the lowest
    ///   existing sorted run ID as the destination.
    ///
    /// ## Arguments
    /// - `state`: Process-local view of the DB's manifest and compactions.
    /// - `request`: The compaction request to plan.
    ///
    /// ## Returns
    /// - A list of [`CompactionSpec`] describing what to compact and where to write.
    /// - An [`Error`] if the request is invalid.
    fn generate(
        &self,
        state: &CompactorStateView,
        request: &CompactionRequest,
    ) -> Result<Vec<CompactionSpec>, Error> {
        match request {
            CompactionRequest::Spec(spec) => Ok(vec![spec.clone()]),
            CompactionRequest::Full => {
                let manifest = state.manifest();
                let sources = manifest
                    .l0
                    .iter()
                    .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
                    .chain(
                        manifest
                            .compacted
                            .iter()
                            .map(|sr| SourceId::SortedRun(sr.id)),
                    )
                    .collect::<Vec<_>>();
                if sources.is_empty() {
                    return Err(crate::Error::from(SlateDBError::InvalidCompaction));
                }
                let destination = manifest.compacted.iter().map(|sr| sr.id).min().unwrap_or(0);
                Ok(vec![CompactionSpec::new(sources, destination)])
            }
        }
    }
}

/// Request to submit a compaction for execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CompactionRequest {
    /// Compact all current L0 SSTs and sorted runs.
    Full,
    /// Use a caller-provided compaction spec.
    Spec(CompactionSpec),
}

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
        /// The output SSTs produced so far (including previous runs).
        output_ssts: Vec<SsTableHandle>,
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
pub struct Compactor {
    manifest_store: Arc<ManifestStore>,
    compactions_store: Arc<CompactionsStore>,
    table_store: Arc<TableStore>,
    options: Arc<CompactorOptions>,
    scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
    task_executor: Arc<MessageHandlerExecutor>,
    compactor_runtime: Handle,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    system_clock: Arc<dyn SystemClock>,
    merge_operator: Option<MergeOperatorType>,
    #[cfg(feature = "compaction_filters")]
    compaction_filter_supplier: Option<Arc<dyn CompactionFilterSupplier>>,
}

impl Compactor {
    #[cfg_attr(not(feature = "compaction_filters"), allow(clippy::too_many_arguments))]
    pub(crate) fn new(
        manifest_store: Arc<ManifestStore>,
        compactions_store: Arc<CompactionsStore>,
        table_store: Arc<TableStore>,
        options: CompactorOptions,
        scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
        compactor_runtime: Handle,
        rand: Arc<DbRand>,
        stat_registry: Arc<StatRegistry>,
        system_clock: Arc<dyn SystemClock>,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        merge_operator: Option<MergeOperatorType>,
        #[cfg(feature = "compaction_filters")] compaction_filter_supplier: Option<
            Arc<dyn CompactionFilterSupplier>,
        >,
    ) -> Self {
        let stats = Arc::new(CompactionStats::new(stat_registry));
        let task_executor = Arc::new(MessageHandlerExecutor::new(
            closed_result,
            system_clock.clone(),
        ));
        Self {
            manifest_store,
            compactions_store,
            table_store,
            options: Arc::new(options),
            scheduler_supplier,
            task_executor,
            compactor_runtime,
            rand,
            stats,
            system_clock,
            merge_operator,
            #[cfg(feature = "compaction_filters")]
            compaction_filter_supplier,
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
    pub async fn run(&self) -> Result<(), crate::Error> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let scheduler = Arc::from(self.scheduler_supplier.compaction_scheduler(&self.options));
        let executor = Arc::new(TokioCompactionExecutor::new(
            TokioCompactionExecutorOptions {
                handle: self.compactor_runtime.clone(),
                options: self.options.clone(),
                worker_tx: tx,
                table_store: self.table_store.clone(),
                rand: self.rand.clone(),
                stats: self.stats.clone(),
                clock: self.system_clock.clone(),
                manifest_store: self.manifest_store.clone(),
                merge_operator: self.merge_operator.clone(),
                #[cfg(feature = "compaction_filters")]
                compaction_filter_supplier: self.compaction_filter_supplier.clone(),
            },
        ));
        let handler = CompactorEventHandler::new(
            self.manifest_store.clone(),
            self.compactions_store.clone(),
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
        self.task_executor
            .join_task(COMPACTOR_TASK_NAME)
            .await
            .map_err(|e| e.into())
    }

    /// Gracefully stops the compactor task and waits for it to finish.
    ///
    /// ## Returns
    /// - `Ok(())` once the task has shut down, or [`SlateDBError`] if shutdown fails.
    pub async fn stop(&self) -> Result<(), crate::Error> {
        self.task_executor
            .shutdown_task(COMPACTOR_TASK_NAME)
            .await
            .map_err(|e| e.into())
    }

    /// Persist a [`CompactionSpec`] as a new [`Compaction`] in the compactions store.
    ///
    /// ## Returns
    /// - `Ok(Ulid)` with the submitted compaction id.
    /// - `SlateDBError::InvalidDBState` if the compactions file doesn't exist.
    /// - `SlateDBError::InvalidCompaction` if a full compaction has no sources.
    /// - `SlateDBError` if the compaction could not be persisted.
    pub(crate) async fn submit(
        spec: CompactionSpec,
        compactions_store: Arc<CompactionsStore>,
        rand: Arc<DbRand>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Ulid, crate::Error> {
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction = Compaction::new(compaction_id, spec);
        let mut stored_compactions =
            match StoredCompactions::try_load(compactions_store.clone()).await? {
                Some(stored) => stored,
                None => return Err(crate::Error::from(SlateDBError::InvalidDBState)),
            };

        loop {
            let mut dirty = stored_compactions.prepare_dirty()?;
            dirty.value.insert(compaction.clone());
            match stored_compactions.update(dirty).await {
                Ok(()) => return Ok(compaction_id),
                Err(SlateDBError::TransactionalObjectVersionExists) => {
                    stored_compactions.refresh().await?;
                }
                Err(err) => return Err(crate::Error::from(err)),
            }
        }
    }
}

/// Event-driven controller for compaction orchestration.
///
/// The [`CompactorEventHandler`] implements [`MessageHandler<CompactorMessage>`] and
/// runs inside the compactor task's message loop. It reacts to periodic tickers and
/// executor callbacks, and coordinates scheduling and execution of compactions.
pub(crate) struct CompactorEventHandler {
    state_writer: CompactorStateWriter,
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
            CompactorMessage::PollManifest => self.handle_ticker().await?,
            CompactorMessage::CompactionJobFinished { id, result } => {
                match result {
                    Ok(sr) => self.finish_compaction(id, sr).await?,
                    Err(err) => {
                        error!("error executing compaction [error={:#?}]", err);
                        self.finish_failed_compaction(id).await?;
                    }
                }
                self.maybe_schedule_compactions().await?;
                self.maybe_start_compactions().await?;
            }
            CompactorMessage::CompactionJobProgress {
                id,
                bytes_processed,
                output_ssts,
            } => {
                let compaction = self.state().compactions().value.get(&id);
                let update_output_ssts =
                    compaction.is_some_and(|c| c.output_ssts().len() != output_ssts.len());
                self.state_mut().update_compaction(&id, |c| {
                    c.set_bytes_processed(bytes_processed);
                    if update_output_ssts {
                        c.set_output_ssts(output_ssts);
                    }
                });
                // To prevent excessive writes, only persist if output SSTs changed.
                if update_output_ssts {
                    self.state_writer.write_compactions_safely().await?;
                }
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
        compactions_store: Arc<CompactionsStore>,
        options: Arc<CompactorOptions>,
        scheduler: Arc<dyn CompactionScheduler + Send + Sync>,
        executor: Arc<dyn CompactionExecutor + Send + Sync>,
        rand: Arc<DbRand>,
        stats: Arc<CompactionStats>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let state_writer = CompactorStateWriter::new(
            manifest_store,
            compactions_store,
            system_clock.clone(),
            options.as_ref(),
            rand.clone(),
        )
        .await?;
        Ok(Self {
            state_writer,
            options,
            scheduler,
            executor,
            rand,
            stats,
            system_clock,
        })
    }

    fn state(&self) -> &CompactorState {
        &self.state_writer.state
    }

    fn state_mut(&mut self) -> &mut CompactorState {
        &mut self.state_writer.state
    }

    /// Emits the current compaction state and per-job progress.
    fn handle_log_ticker(&self) {
        self.log_compaction_state();
        self.log_compaction_throughput();
    }

    /// Logs per-job compaction progress and updates aggregate throughput metrics.
    fn log_compaction_throughput(&self) {
        let current_time = self.system_clock.now();
        let current_time_ms = current_time.timestamp_millis() as u64;
        let db_state = self.state().db_state();
        let mut total_estimated_bytes = 0u64;
        let mut total_bytes_processed = 0u64;
        let mut total_elapsed_secs = 0.0f64;

        for compaction in self.state().active_compactions() {
            let estimated_source_bytes =
                Self::calculate_estimated_source_bytes(compaction, db_state);
            total_estimated_bytes += estimated_source_bytes;
            total_bytes_processed += compaction.bytes_processed();

            // Calculate elapsed time using ULID timestamp
            let start_time_ms = compaction
                .id()
                .datetime()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("invalid duration")
                .as_millis() as u64;
            let elapsed_secs = if start_time_ms > 0 {
                (current_time_ms as f64 - start_time_ms as f64) / 1000.0
            } else {
                0.0
            };
            total_elapsed_secs += elapsed_secs;

            // Per-job throughput for logging
            let throughput = if elapsed_secs > 0.0 {
                compaction.bytes_processed() as f64 / elapsed_secs
            } else {
                0.0
            };

            let percentage = if estimated_source_bytes > 0 {
                (compaction.bytes_processed() * 100 / estimated_source_bytes) as u32
            } else {
                0
            };
            debug!(
                "compaction progress [id={}, progress={}%, processed_bytes={}, estimated_source_bytes={}, elapsed={:.2}s, throughput={}/s]",
                compaction.id(),
                percentage,
                format_bytes_si(compaction.bytes_processed()),
                format_bytes_si(estimated_source_bytes),
                elapsed_secs,
                format_bytes_si(throughput as u64),
            );
        }

        let total_throughput = if total_elapsed_secs > 0.0 {
            total_bytes_processed as f64 / total_elapsed_secs
        } else {
            0.0
        };

        self.stats
            .total_bytes_being_compacted
            .set(total_estimated_bytes);
        self.stats.total_throughput.set(total_throughput as u64);
    }

    /// Calculates the estimated total source bytes for a compaction.
    fn calculate_estimated_source_bytes(
        compaction: &Compaction,
        db_state: &crate::db_state::ManifestCore,
    ) -> u64 {
        use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
        use std::collections::HashMap;

        let ssts_by_id: HashMap<Ulid, &SsTableHandle> = db_state
            .l0
            .iter()
            .map(|sst| match sst.id {
                SsTableId::Compacted(id) => (id, sst),
                SsTableId::Wal(_) => unreachable!("L0 SSTs should never have SsTableId::Wal"),
            })
            .collect();
        let srs_by_id: HashMap<u32, &SortedRun> =
            db_state.compacted.iter().map(|sr| (sr.id, sr)).collect();

        compaction
            .spec()
            .sources()
            .iter()
            .map(|source| match source {
                SourceId::Sst(id) => ssts_by_id
                    .get(id)
                    .expect("compaction source SST not found in L0")
                    .estimate_size(),
                SourceId::SortedRun(id) => srs_by_id
                    .get(id)
                    .expect("compaction source sorted run not found")
                    .estimate_size(),
            })
            .sum()
    }

    /// Handles a polling tick by refreshing compactions and the manifest, then possibly scheduling compactions.
    async fn handle_ticker(&mut self) -> Result<(), SlateDBError> {
        if !self.is_executor_stopped() {
            self.state_writer.refresh().await?;
            self.maybe_schedule_compactions().await?;
            self.maybe_start_compactions().await?;
        }
        Ok(())
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

    /// Performs pre-execution validation for a proposed compaction and defers policy-specific
    /// checks to the scheduler via [`CompactionScheduler::validate_compaction`]. Invariants
    /// checked in this function:
    ///
    /// - Compaction has sources
    /// - Compaction sources exist in DB state
    /// - Compactions with only L0 sources must have a destination > highest existing SR ID
    fn validate_compaction(&self, compaction: &CompactionSpec) -> Result<(), SlateDBError> {
        // Validate compaction sources exist
        if compaction.sources().is_empty() {
            warn!("submitted compaction is empty: {:?}", compaction.sources());
            return Err(SlateDBError::InvalidCompaction);
        }

        // Validate compaction sources exist in DB state
        let db_state = self.state().db_state();
        let l0_ids = db_state
            .l0
            .iter()
            .filter_map(|sst| match sst.id {
                crate::db_state::SsTableId::Compacted(id) => Some(id),
                crate::db_state::SsTableId::Wal(_) => None,
            })
            .collect::<std::collections::HashSet<_>>();
        let sr_ids = db_state
            .compacted
            .iter()
            .map(|sr| sr.id)
            .collect::<std::collections::HashSet<_>>();

        if let Some(missing) = compaction.sources().iter().find(|source| match source {
            SourceId::Sst(id) => !l0_ids.contains(id),
            SourceId::SortedRun(id) => !sr_ids.contains(id),
        }) {
            warn!("compaction source missing from db state: {:?}", missing);
            return Err(SlateDBError::InvalidCompaction);
        }

        // Validate L0-only compactions create a new SR with id > highest existing
        let has_only_l0 = compaction
            .sources()
            .iter()
            .all(|s| matches!(s, SourceId::Sst(_)));

        if has_only_l0 {
            // L0-only: must create new SR with id > highest_existing
            let highest_id = self
                .state()
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
            .validate(&self.state().into(), compaction)
            .map_err(|_e| SlateDBError::InvalidCompaction)
    }

    /// Requests new compactions from the scheduler, validates them, and adds them to the
    /// state up to the the max concurrency limit. This method does not actually start
    /// the compactions; that is done in [`CompactorEventHandler::maybe_start_compactions`].
    async fn maybe_schedule_compactions(&mut self) -> Result<(), SlateDBError> {
        let running_compaction_count = self.running_compaction_count();
        let available_capacity = self.options.max_concurrent_compactions - running_compaction_count;

        if available_capacity == 0 {
            debug!(
                "skipping compaction scheduling since at capacity [running_compactions={}, max_concurrent_compactions={}]",
                running_compaction_count,
                self.options.max_concurrent_compactions
            );
            return Ok(());
        }

        let mut specs = self.scheduler.propose(&self.state().into());

        // Add new compactions up to the max concurrency limit
        let num_specs_added = specs
            .drain(..available_capacity.min(specs.len()))
            .map(|spec| -> Result<(), SlateDBError> {
                let compaction_id = self.rand.rng().gen_ulid(self.system_clock.as_ref());
                debug!(
                    "scheduling new compaction [spec={:?}, id={}]",
                    spec, compaction_id
                );
                self.state_mut()
                    .add_compaction(Compaction::new(compaction_id, spec))?;
                Ok(())
            })
            .count();

        // Save newly added compactions
        if num_specs_added > 0 {
            self.state_writer.write_compactions_safely().await?;
        }

        Ok(())
    }

    /// Starts (valid) submitted compactions up to the max concurrency limit. Invalid
    /// compactions are marked as failed. Successfully started compactions are marked
    /// as running. State changes are persisted after processing all submitted
    /// compactions.
    async fn maybe_start_compactions(&mut self) -> Result<(), SlateDBError> {
        let submitted_compactions = self
            .state()
            .compactions_with_status(CompactionStatus::Submitted)
            .cloned()
            .collect::<Vec<_>>();

        let result = self
            .start_submitted_compactions(&submitted_compactions)
            .await;

        if !submitted_compactions.is_empty() {
            self.state_writer.write_compactions_safely().await?;
        }

        result
    }

    /// Starts (valid) submitted compactions up to the max concurrency limit. Invalid
    /// compactions are marked as failed. Successfully started compactions are marked
    /// as running. This function modifies the state directly but does not persist it;
    /// the caller is responsible for persisting state after this function returns.
    async fn start_submitted_compactions(
        &mut self,
        submitted_compactions: &[Compaction],
    ) -> Result<(), SlateDBError> {
        for compaction in submitted_compactions {
            assert!(
                compaction.status() == CompactionStatus::Submitted,
                "expected submitted compaction, got {:?}",
                compaction.status()
            );
            // Make sure we have capacity to start a new compaction.
            let running_compaction_count = self.running_compaction_count();
            if running_compaction_count >= self.options.max_concurrent_compactions {
                info!(
                    "skipping compaction since capacity is exceeded [running_compactions={}, max_concurrent_compactions={}, compaction={:?}]",
                    running_compaction_count,
                    self.options.max_concurrent_compactions,
                    compaction
                );
                break;
            }

            // Validate the candidate compaction; mark as failed if invalid.
            if let Err(e) = self.validate_compaction(compaction.spec()) {
                error!(
                    "compaction validation failed [error={:?}, compaction={:?}]",
                    compaction, e
                );
                self.state_mut().update_compaction(&compaction.id(), |c| {
                    c.set_status(CompactionStatus::Failed)
                });
                continue;
            }

            // Compaction is valid and there is capacity, so start it.
            match self
                .start_compaction(compaction.id(), compaction.clone())
                .await
            {
                Ok(_) => {
                    self.state_mut().update_compaction(&compaction.id(), |c| {
                        c.set_status(CompactionStatus::Running)
                    });
                }
                Err(e) => {
                    self.state_mut().update_compaction(&compaction.id(), |c| {
                        c.set_status(CompactionStatus::Failed)
                    });
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Creates a [`StartCompactionJobArgs`] for a [`Compaction`] and asks the executor to run it.
    #[instrument(level = "debug", skip_all, fields(id = %job_id))]
    async fn start_compaction(
        &mut self,
        job_id: Ulid,
        compaction: Compaction,
    ) -> Result<(), SlateDBError> {
        self.log_compaction_state();
        let db_state = self.state().db_state();

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
            output_ssts: compaction.output_ssts().clone(),
            compaction_clock_tick: db_state.last_l0_clock_tick,
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
    async fn finish_failed_compaction(&mut self, id: Ulid) -> Result<(), SlateDBError> {
        self.state_mut()
            .update_compaction(&id, |c| c.set_status(CompactionStatus::Failed));
        self.state_writer.write_compactions_safely().await?;
        Ok(())
    }

    /// Records a successful compaction, persists the manifest, and checks for new compactions
    /// to schedule.
    #[instrument(level = "debug", skip_all, fields(id = %id))]
    async fn finish_compaction(
        &mut self,
        id: Ulid,
        output_sr: SortedRun,
    ) -> Result<(), SlateDBError> {
        self.state_mut().finish_compaction(id, output_sr);
        self.log_compaction_state();
        self.state_writer.write_state_safely().await?;
        self.maybe_schedule_compactions().await?;
        self.maybe_start_compactions().await?;
        self.stats
            .last_compaction_ts
            .set(self.system_clock.now().timestamp() as u64);
        Ok(())
    }

    /// Logs the current DB runs and in-flight compactions.
    fn log_compaction_state(&self) {
        self.state().db_state().log_db_runs();
        let compactions = self.state().active_compactions();
        for compaction in compactions {
            if log::log_enabled!(log::Level::Debug) {
                debug!("in-flight compaction [compaction={:?}]", compaction);
            } else {
                info!("in-flight compaction [compaction={}]", compaction);
            }
        }
    }

    fn running_compaction_count(&self) -> usize {
        self.state()
            .active_compactions()
            .filter(|c| c.status() == CompactionStatus::Running)
            .count()
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
    pub const TOTAL_BYTES_BEING_COMPACTED: &str =
        compactor_stat_name!("total_bytes_being_compacted");
    pub const TOTAL_THROUGHPUT_BYTES_PER_SEC: &str =
        compactor_stat_name!("total_throughput_bytes_per_sec");

    pub(crate) struct CompactionStats {
        pub(crate) last_compaction_ts: Arc<Gauge<u64>>,
        pub(crate) running_compactions: Arc<Gauge<i64>>,
        pub(crate) bytes_compacted: Arc<Counter>,
        pub(crate) total_bytes_being_compacted: Arc<Gauge<u64>>,
        pub(crate) total_throughput: Arc<Gauge<u64>>,
    }

    impl CompactionStats {
        /// Registers and returns a new set of compactor metrics in the provided registry.
        ///
        /// ## Metrics
        /// - `last_compaction_timestamp_sec`: Unix timestamp of the last completed compaction.
        /// - `running_compactions`: Gauge tracking active compaction attempts.
        /// - `bytes_compacted`: Counter of bytes written by the executor.
        /// - `total_bytes_being_compacted`: Total bytes across all running compactions.
        /// - `total_throughput_bytes_per_sec`: Combined throughput across all running compactions.
        pub(crate) fn new(stat_registry: Arc<StatRegistry>) -> Self {
            let stats = Self {
                last_compaction_ts: Arc::new(Gauge::default()),
                running_compactions: Arc::new(Gauge::default()),
                bytes_compacted: Arc::new(Counter::default()),
                total_bytes_being_compacted: Arc::new(Gauge::default()),
                total_throughput: Arc::new(Gauge::default()),
            };
            stat_registry.register(LAST_COMPACTION_TS_SEC, stats.last_compaction_ts.clone());
            stat_registry.register(RUNNING_COMPACTIONS, stats.running_compactions.clone());
            stat_registry.register(BYTES_COMPACTED, stats.bytes_compacted.clone());
            stat_registry.register(
                TOTAL_BYTES_BEING_COMPACTED,
                stats.total_bytes_being_compacted.clone(),
            );
            stat_registry.register(
                TOTAL_THROUGHPUT_BYTES_PER_SEC,
                stats.total_throughput.clone(),
            );
            stats
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::future::Future;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use rand::RngCore;
    use slatedb_common::MockSystemClock;
    use ulid::Ulid;

    use super::*;
    use crate::compactions_store::{FenceableCompactions, StoredCompactions};
    use crate::compactor::stats::CompactionStats;
    use crate::compactor::stats::LAST_COMPACTION_TS_SEC;
    use crate::compactor_executor::{
        CompactionExecutor, TokioCompactionExecutor, TokioCompactionExecutorOptions,
    };
    use crate::compactor_state::CompactionStatus;
    use crate::compactor_state::SourceId;
    use crate::config::{
        MergeOptions, PutOptions, Settings, SizeTieredCompactionSchedulerOptions, Ttl, WriteOptions,
    };
    use crate::db::Db;
    use crate::db_state::{ManifestCore, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
    use crate::error::SlateDBError;
    use crate::format::sst::{SsTableFormat, SST_FORMAT_VERSION_LATEST};
    use crate::iter::KeyValueIterator;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use crate::merge_operator::{MergeOperator, MergeOperatorError};
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::rng;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::test_utils::assert_iterator;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};

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
        let system_clock = Arc::new(MockSystemClock::new());
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 128;
        let scheduler_options = SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 1,
            max_compaction_sources: 999,
            include_size_threshold: 4.0,
        }
        .into();
        options
            .compactor_options
            .as_mut()
            .expect("compactor options must be set")
            .scheduler_options = scheduler_options;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .build()
            .await
            .unwrap();

        let (_, _, table_store) = build_test_stores(os.clone());
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
        let db_state = await_compaction(&db, Some(system_clock.clone())).await;

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
        let system_clock = Arc::new(MockSystemClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| {
                // compact when there are at least 2 SSTs in L0 (one for key 'a' and one for key 'b')
                state.manifest().l0.len() == 2 ||
                // or when there is one SST in L0 and one in L1 (one for delete key 'a' and one for compacted key 'a'+'b')
                (state.manifest().l0.len() == 1 && state.manifest().compacted.len() == 1)
            },
        )));

        let mut options = db_options(None);
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                scheduler.clone(),
            ))
            .build()
            .await
            .unwrap();

        let (manifest_store, _, table_store) = build_test_stores(os.clone());

        // put key 'a' into L1 (and key 'b' so that when we delete 'a' the SST is non-empty)
        // since these are both await_durable=true, we're guaranteed to have one L0 SST for each.
        db.put(&[b'a'; 16], &[b'a'; 32]).await.unwrap();
        db.put(&[b'b'; 16], &[b'a'; 32]).await.unwrap();
        db.flush().await.unwrap();
        let db_state = await_compaction(&db, Some(system_clock.clone()))
            .await
            .unwrap();
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

        let db_state = await_compacted_compaction(
            manifest_store.clone(),
            db_state.compacted,
            Some(system_clock.clone()),
        )
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
        let system_clock = Arc::new(MockSystemClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| {
                // compact when there are at least 2 SSTs in L0
                state.manifest().l0.len() == 2 ||
                // or when there is one SST in L0 and one in L1
                (state.manifest().l0.len() == 1 && state.manifest().compacted.len() == 1)
            },
        )));

        let mut options = db_options(None);
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                scheduler.clone(),
            ))
            .build()
            .await
            .unwrap();

        let (manifest_store, _, table_store) = build_test_stores(os.clone());

        // Write and flush key 'a' first (seq=1)
        db.put(&[b'a'; 16], &[b'a'; 32]).await.unwrap();
        db.put(&[b'b'; 16], &[b'a'; 32]).await.unwrap();

        // Create a snapshot after first flush. This protects seq >= 1
        let _snapshot = db.snapshot().await.unwrap();
        db.flush().await.unwrap();

        // Compact L0 to L1
        let db_state = await_compaction(&db, Some(system_clock.clone()))
            .await
            .unwrap();
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
        let db_state = await_compacted_compaction(
            manifest_store.clone(),
            db_state.compacted,
            Some(system_clock.clone()),
        )
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
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, _, table_store) = build_test_stores(os.clone());

        // write merge operations across multiple L0 SSTs
        db.merge_with_options(
            b"key1",
            b"a",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            b"key1",
            b"b",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'x'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        db.merge_with_options(
            b"key1",
            b"c",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            b"key2",
            b"x",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'y'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(system_clock)).await;

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
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| !state.manifest().l0.is_empty(),
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, _, table_store) = build_test_stores(os.clone());

        // write initial merge operations and compact to L1
        db.merge_with_options(
            b"key1",
            b"a",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            b"key1",
            b"b",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'x'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        let db_state = await_compaction(&db, Some(system_clock.clone())).await;
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.compacted.len(), 1);
        // Save current tick since we advanced it in `await_compaction`. We'll use it
        // later to verify the create_ts of the merged and normal entries.
        let expected_tick = system_clock.now().timestamp_millis();

        // write more merge operations to L0
        db.merge_with_options(
            b"key1",
            b"c",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            b"key1",
            b"d",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'y'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when: compact L0 with the existing sorted run
        let db_state = await_compaction(&db, Some(system_clock)).await;

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
                RowEntry::new_merge(b"key1", b"abcd", 5).with_create_ts(expected_tick),
                RowEntry::new_value(&[b'x'; 16], &[b'p'; 128], 3).with_create_ts(0),
                RowEntry::new_value(&[b'y'; 16], &[b'p'; 128], 6).with_create_ts(expected_tick),
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
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, _, table_store) = build_test_stores(os.clone());

        // write only merge operations without any base value
        db.merge_with_options(
            b"key1",
            b"x",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.put_with_options(
            &vec![b'x'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        db.merge_with_options(
            b"key1",
            b"y",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            b"key1",
            b"z",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'y'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(system_clock)).await;

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
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().l0.len() >= 3,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, _, table_store) = build_test_stores(os.clone());

        // write merge operations across three L0 SSTs in specific order
        db.merge_with_options(
            b"key1",
            b"1",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'a'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        db.merge_with_options(
            b"key1",
            b"2",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'b'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        db.merge_with_options(
            b"key1",
            b"3",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'c'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(system_clock)).await;

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
        let insert_clock = Arc::new(MockSystemClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().l0.len() >= 2,
        )));

        let mut options = db_options(None);
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(insert_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                scheduler.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (_manifest_store, _, table_store) = build_test_stores(os.clone());

        // ticker time = 0, expire time = 10
        insert_clock.set(0);
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
        insert_clock.set(20);
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

        let db_state = await_compaction(&db, Some(insert_clock.clone()))
            .await
            .unwrap();
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
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (manifest_store, _compactions_store, _table_store) = build_test_stores(os.clone());

        // write merge operations
        db.merge_with_options(
            b"key1",
            b"a",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            b"key1",
            b"b",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'x'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding
        db.flush().await.unwrap();

        // then overwrite with a put
        db.put_with_options(
            b"key1",
            b"new_value",
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'y'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding
        db.flush().await.unwrap();

        // when:
        let _ = await_compaction(&db, Some(system_clock)).await;

        // then: verify the put value overwrote the merge
        let result = db.get(b"key1").await.unwrap();
        assert_eq!(result, Some(Bytes::from("new_value")));

        // verify the compacted state in manifest
        let stored_manifest =
            StoredManifest::load(manifest_store.clone(), Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
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
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (manifest_store, _compactions_store, table_store) = build_test_stores(os.clone());

        // write merge operations with different TTLs
        db.merge_with_options(
            b"key1",
            b"a",
            &crate::config::MergeOptions {
                ttl: Ttl::ExpireAfter(100),
            },
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'x'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding
        db.flush().await.unwrap();

        db.merge_with_options(
            b"key1",
            b"b",
            &crate::config::MergeOptions {
                ttl: Ttl::ExpireAfter(200),
            },
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put_with_options(
            &vec![b'y'; 16],
            &vec![b'p'; 128],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap(); // padding
        db.flush().await.unwrap();

        // when:
        // Wait a bit for compaction to occur
        let _ = await_compaction(&db, Some(system_clock)).await;

        // then: verify that merges with different expire times are NOT merged together
        // Reading should get only the latest merge since they weren't combined
        let stored_manifest =
            StoredManifest::load(manifest_store.clone(), Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
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
        let insert_clock = Arc::new(MockSystemClock::new());

        let scheduler_options = SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            max_compaction_sources: 2,
            include_size_threshold: 4.0,
        }
        .into();
        let mut options = db_options(Some(compactor_options()));
        options.default_ttl = Some(50);
        options
            .compactor_options
            .as_mut()
            .expect("compactor options missing")
            .scheduler_options = scheduler_options;
        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(insert_clock.clone())
            .build()
            .await
            .unwrap();

        let (_, _, table_store) = build_test_stores(os.clone());

        let value = &[b'a'; 64];

        // ticker time = 0, expire time = 10
        insert_clock.set(0);
        db.put_with_options(
            &[1; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAfter(10),
            },
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();

        // ticker time = 10, expire time = 60 (using default TTL)
        insert_clock.set(10);
        db.put_with_options(
            &[2; 16],
            value,
            &PutOptions { ttl: Ttl::Default },
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();

        db.flush().await.unwrap();

        // ticker time = 30, no expire time
        insert_clock.set(30);
        db.put_with_options(
            &[3; 16],
            value,
            &PutOptions { ttl: Ttl::NoExpiry },
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();

        // this revives key 1
        // ticker time = 70, expire time 80
        insert_clock.set(70);
        db.put_with_options(
            &[1; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAfter(80),
            },
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();

        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(insert_clock)).await;

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
    async fn test_should_track_total_bytes_and_throughput() {
        use crate::compactor::stats::{
            TOTAL_BYTES_BEING_COMPACTED, TOTAL_THROUGHPUT_BYTES_PER_SEC,
        };
        use chrono::DateTime;

        let mut fixture = CompactorEventHandlerTestFixture::new().await;

        let current_time = fixture.handler.system_clock.now();
        let current_time_ms = current_time.timestamp_millis() as u64;
        let start_time_1 =
            DateTime::from_timestamp_millis((current_time_ms - 2000) as i64).unwrap();
        let start_time_2 =
            DateTime::from_timestamp_millis((current_time_ms - 1000) as i64).unwrap();

        let mut compaction_1 = Compaction::new(
            Ulid::from_parts(start_time_1.timestamp_millis() as u64, 0),
            CompactionSpec::new(vec![], 10),
        );
        compaction_1.set_bytes_processed(500);

        let mut compaction_2 = Compaction::new(
            Ulid::from_parts(start_time_2.timestamp_millis() as u64, 0),
            CompactionSpec::new(vec![], 11),
        );
        compaction_2.set_bytes_processed(1000);

        fixture
            .handler
            .state_mut()
            .add_compaction(compaction_1)
            .expect("failed to add compaction 1");
        fixture
            .handler
            .state_mut()
            .add_compaction(compaction_2)
            .expect("failed to add compaction 2");

        fixture.handler.handle_log_ticker();

        let total_bytes = fixture
            .stats_registry
            .lookup(TOTAL_BYTES_BEING_COMPACTED)
            .unwrap()
            .get();
        assert_eq!(total_bytes, 0);

        let throughput = fixture
            .stats_registry
            .lookup(TOTAL_THROUGHPUT_BYTES_PER_SEC)
            .unwrap()
            .get();
        assert!(
            throughput > 0,
            "Expected throughput > 0, got {}",
            throughput
        );
    }

    #[tokio::test]
    async fn test_should_track_per_job_throughput() {
        let start_time_ms = 1000u64;
        let current_time_ms = 3000u64;
        let processed_bytes = 1000u64;

        let mut compaction = Compaction::new(
            Ulid::from_parts(start_time_ms, 0),
            CompactionSpec::new(vec![], 10),
        );
        compaction.set_bytes_processed(processed_bytes);

        // Calculate throughput manually using ULID timestamp
        let elapsed_secs = (current_time_ms as f64 - start_time_ms as f64) / 1000.0;
        let throughput = processed_bytes as f64 / elapsed_secs;
        assert_eq!(throughput, 500.0);

        // At start time, throughput should be 0
        let elapsed_zero = (start_time_ms as f64 - start_time_ms as f64) / 1000.0;
        let throughput_zero = if elapsed_zero > 0.0 {
            processed_bytes as f64 / elapsed_zero
        } else {
            0.0
        };
        assert_eq!(throughput_zero, 0.0);
    }

    #[tokio::test]
    async fn test_should_track_running_compactions_count() {
        use crate::compactor::stats::RUNNING_COMPACTIONS;

        let mut fixture = CompactorEventHandlerTestFixture::new().await;

        assert_eq!(
            fixture
                .stats_registry
                .lookup(RUNNING_COMPACTIONS)
                .unwrap()
                .get(),
            0
        );

        let compaction = Compaction::new(Ulid::new(), CompactionSpec::new(vec![], 10));
        fixture
            .handler
            .state_mut()
            .add_compaction(compaction)
            .expect("failed to add compaction");

        assert_eq!(fixture.handler.state().active_compactions().count(), 1);
    }

    #[tokio::test]
    async fn test_submit_persists_compaction() {
        let os = Arc::new(InMemory::new());
        let (manifest_store, compactions_store, _table_store) = build_test_stores(os.clone());
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        StoredManifest::create_new_db(
            manifest_store.clone(),
            ManifestCore::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();
        let stored_manifest = StoredManifest::load(manifest_store.clone(), system_clock.clone())
            .await
            .unwrap();
        StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
        .await
        .unwrap();

        let spec = CompactionSpec::new(vec![SourceId::SortedRun(0)], 0);
        let compaction_id = Compactor::submit(
            spec.clone(),
            compactions_store.clone(),
            Arc::new(DbRand::default()),
            system_clock.clone(),
        )
        .await
        .unwrap();

        let (_, compactions) = compactions_store.read_latest_compactions().await.unwrap();
        let stored = compactions
            .get(&compaction_id)
            .expect("missing submitted compaction");

        assert_eq!(stored.spec(), &spec);
        assert_eq!(stored.status(), CompactionStatus::Submitted);
    }

    #[tokio::test]
    async fn test_submit_full_compaction_uses_all_sources() {
        let os = Arc::new(InMemory::new());
        let (manifest_store, compactions_store, _table_store) = build_test_stores(os.clone());
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        StoredManifest::create_new_db(
            manifest_store.clone(),
            ManifestCore::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();
        let mut stored_manifest =
            StoredManifest::load(manifest_store.clone(), system_clock.clone())
                .await
                .unwrap();

        let l0_newest = Ulid::new();
        let l0_oldest = Ulid::new();
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        let l0_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"a")),
            ..SsTableInfo::default()
        };
        let sr_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"m")),
            ..SsTableInfo::default()
        };
        dirty.value.core.l0 = VecDeque::from(vec![
            SsTableHandle::new(
                SsTableId::Compacted(l0_newest),
                SST_FORMAT_VERSION_LATEST,
                l0_info.clone(),
            ),
            SsTableHandle::new(
                SsTableId::Compacted(l0_oldest),
                SST_FORMAT_VERSION_LATEST,
                l0_info.clone(),
            ),
        ]);
        dirty.value.core.compacted = vec![
            SortedRun {
                id: 2,
                ssts: vec![SsTableHandle::new(
                    SsTableId::Compacted(Ulid::new()),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info.clone(),
                )],
            },
            SortedRun {
                id: 1,
                ssts: vec![SsTableHandle::new(
                    SsTableId::Compacted(Ulid::new()),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info.clone(),
                )],
            },
        ];
        stored_manifest.update(dirty).await.unwrap();

        StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
        .await
        .unwrap();

        let scheduler = MockScheduler::new();
        let specs = scheduler
            .generate(
                &CompactorStateView {
                    compactions: None,
                    manifest: (0, stored_manifest.manifest().clone()),
                },
                &CompactionRequest::Full,
            )
            .unwrap();
        assert_eq!(specs.len(), 1);
        let compaction_id = Compactor::submit(
            specs[0].clone(),
            compactions_store.clone(),
            Arc::new(DbRand::default()),
            system_clock.clone(),
        )
        .await
        .unwrap();

        let (_, compactions) = compactions_store.read_latest_compactions().await.unwrap();
        let stored = compactions
            .get(&compaction_id)
            .expect("missing submitted compaction");
        let expected_sources = vec![
            SourceId::Sst(l0_newest),
            SourceId::Sst(l0_oldest),
            SourceId::SortedRun(2),
            SourceId::SortedRun(1),
        ];

        assert_eq!(stored.spec().sources(), &expected_sources);
        assert_eq!(stored.spec().destination(), 1);
        assert_eq!(stored.status(), CompactionStatus::Submitted);
    }

    #[test]
    fn test_plan_spec_returns_spec_clone() {
        let scheduler = MockScheduler::new();
        let state = CompactorStateView {
            compactions: None,
            manifest: (0, Manifest::initial(ManifestCore::new())),
        };
        let spec = CompactionSpec::new(vec![SourceId::SortedRun(7)], 7);

        let planned = scheduler
            .generate(&state, &CompactionRequest::Spec(spec.clone()))
            .unwrap();

        assert_eq!(planned, vec![spec]);
    }

    #[test]
    fn test_plan_full_uses_all_sources_and_min_destination() {
        let scheduler = MockScheduler::new();
        let l0_first = Ulid::from_parts(1, 0);
        let l0_second = Ulid::from_parts(2, 0);
        let mut core = ManifestCore::new();
        let l0_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"a")),
            ..SsTableInfo::default()
        };
        let sr_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"m")),
            ..SsTableInfo::default()
        };
        core.l0 = VecDeque::from(vec![
            SsTableHandle::new(
                SsTableId::Compacted(l0_first),
                SST_FORMAT_VERSION_LATEST,
                l0_info.clone(),
            ),
            SsTableHandle::new(
                SsTableId::Compacted(l0_second),
                SST_FORMAT_VERSION_LATEST,
                l0_info,
            ),
        ]);
        core.compacted = vec![
            SortedRun {
                id: 5,
                ssts: vec![SsTableHandle::new(
                    SsTableId::Compacted(Ulid::from_parts(10, 0)),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info.clone(),
                )],
            },
            SortedRun {
                id: 2,
                ssts: vec![SsTableHandle::new(
                    SsTableId::Compacted(Ulid::from_parts(11, 0)),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info,
                )],
            },
        ];
        let state = CompactorStateView {
            compactions: None,
            manifest: (0, Manifest::initial(core)),
        };

        let planned = scheduler
            .generate(&state, &CompactionRequest::Full)
            .unwrap();

        let expected_sources = vec![
            SourceId::Sst(l0_first),
            SourceId::Sst(l0_second),
            SourceId::SortedRun(5),
            SourceId::SortedRun(2),
        ];
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].sources(), &expected_sources);
        assert_eq!(planned[0].destination(), 2);
    }

    struct CompactorEventHandlerTestFixture {
        manifest: StoredManifest,
        manifest_store: Arc<ManifestStore>,
        compactions_store: Arc<CompactionsStore>,
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
            let (manifest_store, compactions_store, table_store) = build_test_stores(os.clone());
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
                TokioCompactionExecutorOptions {
                    handle: Handle::current(),
                    options: compactor_options.clone(),
                    worker_tx: real_executor_tx,
                    table_store,
                    rand: rand.clone(),
                    stats: compactor_stats.clone(),
                    clock: Arc::new(DefaultSystemClock::new()),
                    manifest_store: manifest_store.clone(),
                    merge_operator: options.merge_operator.clone(),
                    #[cfg(feature = "compaction_filters")]
                    compaction_filter_supplier: None,
                },
            ));
            let handler = CompactorEventHandler::new(
                manifest_store.clone(),
                compactions_store.clone(),
                compactor_options.clone(),
                scheduler.clone(),
                executor.clone(),
                rand.clone(),
                compactor_stats.clone(),
                Arc::new(DefaultSystemClock::new()),
            )
            .await
            .unwrap();
            let manifest =
                StoredManifest::load(manifest_store.clone(), Arc::new(DefaultSystemClock::new()))
                    .await
                    .unwrap();
            Self {
                manifest,
                manifest_store,
                compactions_store,
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

        async fn latest_db_state(&mut self) -> ManifestCore {
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
        fixture.handler.handle_ticker().await.unwrap();
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
        fixture.handler.handle_ticker().await.unwrap();
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
        fixture.handler.handle_ticker().await.unwrap();
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
        fixture.handler.handle_ticker().await.unwrap();
        fixture.assert_started_compaction(1);
    }

    #[tokio::test]
    async fn test_should_persist_compactions_on_start_and_finish() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());

        // when: schedule compaction -> compactions are persisted
        fixture.handler.handle_ticker().await.unwrap();

        let (_, stored_compactions) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        assert_eq!(
            stored_compactions
                .iter()
                .collect::<Vec<&Compaction>>()
                .len(),
            1
        );
        let running_id = stored_compactions
            .iter()
            .next()
            .expect("compaction should be persisted")
            .id();
        let state_id = fixture
            .handler
            .state()
            .active_compactions()
            .next()
            .expect("state missing compaction")
            .id();
        assert_eq!(running_id, state_id);

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

        // then: finishing compaction clears persisted state
        fixture.handler.handle(msg).await.unwrap();
        let (_, stored_compactions) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        let mut stored_compactions_iter = stored_compactions.iter();
        assert_eq!(
            stored_compactions_iter
                .next()
                .expect("compactions should not be empty after finish")
                .id(),
            running_id,
        );
        assert!(
            stored_compactions_iter.next().is_none(),
            "expected only one retained finished compaction for GC"
        );
    }

    #[tokio::test]
    async fn test_progress_persists_output_ssts() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        fixture.handler.state_writer.refresh().await.unwrap();
        let spec = fixture.build_l0_compaction().await;
        let compaction_id = Ulid::new();
        fixture
            .handler
            .state_mut()
            .add_compaction(Compaction::new(compaction_id, spec))
            .expect("failed to add compaction");

        fixture.handler.maybe_start_compactions().await.unwrap();

        let sst_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"a")),
            ..SsTableInfo::default()
        };
        let output_sst = SsTableHandle::new(
            SsTableId::Compacted(Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            sst_info,
        );
        let output_ssts = vec![output_sst.clone()];

        fixture
            .handler
            .handle(CompactorMessage::CompactionJobProgress {
                id: compaction_id,
                bytes_processed: 123,
                output_ssts: output_ssts.clone(),
            })
            .await
            .expect("fatal error handling progress message");

        let (_, stored_compactions) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        let stored = stored_compactions
            .get(&compaction_id)
            .expect("missing stored compaction");
        assert_eq!(stored.output_ssts(), &output_ssts);

        let state_compaction = fixture
            .handler
            .state()
            .active_compactions()
            .find(|c| c.id() == compaction_id)
            .expect("missing compaction in state");
        assert_eq!(state_compaction.output_ssts(), &output_ssts);
    }

    #[tokio::test]
    async fn test_maybe_schedule_compactions_only_submits() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction);

        fixture.handler.maybe_schedule_compactions().await.unwrap();

        assert_eq!(fixture.executor.pop_jobs().len(), 0);
        let mut compactions = fixture.handler.state().active_compactions();
        let scheduled = compactions.next().expect("missing compaction");
        assert_eq!(scheduled.status(), CompactionStatus::Submitted);
        assert!(compactions.next().is_none());

        let (_, stored_compactions) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        let stored = stored_compactions
            .iter()
            .next()
            .expect("compaction should be persisted");
        assert_eq!(stored.status(), CompactionStatus::Submitted);
    }

    #[tokio::test]
    async fn test_maybe_start_compactions_starts_submitted() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        fixture.handler.state_writer.refresh().await.unwrap();
        let spec = fixture.build_l0_compaction().await;
        let compaction_id = Ulid::new();
        fixture
            .handler
            .state_mut()
            .add_compaction(Compaction::new(compaction_id, spec))
            .expect("failed to add compaction");

        fixture.handler.maybe_start_compactions().await.unwrap();

        let jobs = fixture.executor.pop_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].compaction_id, compaction_id);
        let compactions = &fixture.handler.state_writer.state.compactions().value;
        assert_eq!(
            compactions
                .get(&compaction_id)
                .expect("missing compaction")
                .status(),
            CompactionStatus::Running
        );
        let (_, stored_compactions) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        assert_eq!(
            stored_compactions
                .get(&compaction_id)
                .expect("missing stored compaction")
                .status(),
            CompactionStatus::Running
        );
    }

    #[tokio::test]
    async fn test_maybe_start_compactions_respects_capacity() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await; // defaults to capacity of 1
        fixture.write_l0().await;
        fixture.handler.state_writer.refresh().await.unwrap();
        let spec = fixture.build_l0_compaction().await;
        let spec_alt = CompactionSpec::new(spec.sources().clone(), spec.destination() + 1);
        let first_id = Ulid::from_parts(1, 0);
        let second_id = Ulid::from_parts(2, 0);
        fixture
            .handler
            .state_mut()
            .add_compaction(Compaction::new(first_id, spec))
            .expect("failed to add compaction");
        fixture
            .handler
            .state_mut()
            .add_compaction(Compaction::new(second_id, spec_alt))
            .expect("failed to add compaction");

        fixture.handler.maybe_start_compactions().await.unwrap();

        let jobs = fixture.executor.pop_jobs();
        assert_eq!(jobs.len(), 1);
        let compactions = &fixture.handler.state_writer.state.compactions().value;
        assert_eq!(
            compactions
                .get(&first_id)
                .expect("missing first compaction")
                .status(),
            CompactionStatus::Running
        );
        assert_eq!(
            compactions
                .get(&second_id)
                .expect("missing second compaction")
                .status(),
            CompactionStatus::Submitted
        );
    }

    #[tokio::test]
    async fn test_maybe_start_compactions_marks_invalid_failed() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        let compaction_id = Ulid::new();
        fixture
            .handler
            .state_mut()
            .add_compaction(Compaction::new(
                compaction_id,
                CompactionSpec::new(Vec::new(), 0), // invalid: no sources
            ))
            .expect("failed to add compaction");

        fixture.handler.maybe_start_compactions().await.unwrap();

        assert_eq!(fixture.executor.pop_jobs().len(), 0);
        let compactions = &fixture.handler.state_writer.state.compactions().value;
        assert_eq!(
            compactions
                .get(&compaction_id)
                .expect("missing compaction")
                .status(),
            CompactionStatus::Failed
        );
        let (_, stored_compactions) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        assert_eq!(
            stored_compactions
                .get(&compaction_id)
                .expect("missing stored compaction")
                .status(),
            CompactionStatus::Failed
        );
    }

    #[tokio::test]
    async fn test_handle_ticker_starts_preexisting_submitted_compaction() {
        let compactor_options = Arc::new(compactor_options());
        let options = db_options(None);

        let os = Arc::new(InMemory::new());
        let (manifest_store, compactions_store, _table_store) = build_test_stores(os.clone());
        let db = Db::builder(PATH, os.clone())
            .with_settings(options.clone())
            .build()
            .await
            .unwrap();

        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let mut stored_manifest =
            StoredManifest::load(manifest_store.clone(), system_clock.clone())
                .await
                .unwrap();

        // Create an L0 so the submitted compaction has valid sources.
        let mut rng = rng::new_test_rng(None);
        let manifest = stored_manifest.refresh().await.unwrap();
        let l0s = manifest.core.l0.len();
        let mut k = vec![0u8; options.l0_sst_size_bytes];
        rng.fill_bytes(&mut k);
        db.put(&k, &[b'x'; 10]).await.unwrap();
        db.flush().await.unwrap();
        loop {
            let manifest = stored_manifest.refresh().await.unwrap().clone();
            if manifest.core.l0.len() > l0s {
                break;
            }
        }

        // Seed the compactions store with a Submitted compaction before handler startup.
        let db_state = stored_manifest.refresh().await.unwrap().core.clone();
        let sources = db_state
            .l0
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect::<Vec<_>>();
        let spec = CompactionSpec::new(sources, 0);
        let compaction_id = Ulid::new();
        let compaction = Compaction::new(compaction_id, spec);

        let mut stored_compactions = StoredCompactions::create(
            compactions_store.clone(),
            stored_manifest.manifest().compactor_epoch,
        )
        .await
        .unwrap();
        let mut dirty = stored_compactions.prepare_dirty().unwrap();
        dirty.value.insert(compaction);
        stored_compactions.update(dirty).await.unwrap();

        // Build the handler and trigger a ticker refresh to pick up external compactions.
        let scheduler = Arc::new(MockScheduler::new());
        let executor = Arc::new(MockExecutor::new());
        let rand = Arc::new(DbRand::default());
        let stats_registry = Arc::new(StatRegistry::new());
        let compactor_stats = Arc::new(CompactionStats::new(stats_registry));
        let mut handler = CompactorEventHandler::new(
            manifest_store,
            compactions_store.clone(),
            compactor_options,
            scheduler,
            executor.clone(),
            rand,
            compactor_stats,
            system_clock,
        )
        .await
        .unwrap();

        handler.handle_ticker().await.unwrap();

        // The pre-existing Submitted compaction should be started.
        let jobs = executor.pop_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].compaction_id, compaction_id);

        // Finish the compaction and ensure status is persisted as Completed.
        let db_state = handler.state().db_state().clone();
        let output_sr = SortedRun {
            id: jobs[0].destination,
            ssts: db_state.l0.iter().cloned().collect(),
        };
        let msg = CompactorMessage::CompactionJobFinished {
            id: compaction_id,
            result: Ok(output_sr),
        };
        handler.handle(msg).await.unwrap();

        let (_, stored_compactions) = compactions_store.read_latest_compactions().await.unwrap();
        assert_eq!(
            stored_compactions
                .get(&compaction_id)
                .expect("missing stored compaction")
                .status(),
            CompactionStatus::Completed
        );
    }

    #[tokio::test]
    async fn test_should_fail_when_compactions_store_fences_compactor() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());

        // fence the handler by bumping the compactions epoch elsewhere
        let stored_compactions = StoredCompactions::load(fixture.compactions_store.clone())
            .await
            .unwrap();
        FenceableCompactions::init(
            stored_compactions,
            fixture.handler.options.manifest_update_timeout,
            fixture.handler.system_clock.clone(),
        )
        .await
        .unwrap();

        // when:
        let result = fixture.handler.handle_ticker().await;

        // then:
        assert!(matches!(result, Err(SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_should_update_failed_compaction_status() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await.unwrap();
        let job = fixture.assert_started_compaction(1).pop().unwrap();

        // sanity: compaction persisted
        let (_, stored) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        assert_eq!(stored.iter().collect::<Vec<&Compaction>>().len(), 1);

        // when: job fails
        let msg = CompactorMessage::CompactionJobFinished {
            id: job.id,
            result: Err(SlateDBError::InvalidDBState),
        };
        fixture.handler.handle(msg).await.unwrap();

        // then: compaction marked failed in store
        let (_, stored_after) = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap();
        assert_eq!(
            stored_after
                .iter()
                .next()
                .expect("compaction should be persisted after failure")
                .status(),
            CompactionStatus::Failed,
            "compaction should be marked failed after failure"
        );
    }

    #[tokio::test]
    async fn test_should_error_when_finishing_if_compactions_fenced() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await.unwrap();
        let job = fixture.assert_started_compaction(1).pop().unwrap();

        // fence compactions before finish
        let stored_compactions = StoredCompactions::load(fixture.compactions_store.clone())
            .await
            .unwrap();
        FenceableCompactions::init(
            stored_compactions,
            fixture.handler.options.manifest_update_timeout,
            fixture.handler.system_clock.clone(),
        )
        .await
        .unwrap();

        // Build a minimal successful result
        let db_state = fixture.latest_db_state().await;
        let output_sr = SortedRun {
            id: compaction.destination(),
            ssts: db_state.l0.iter().cloned().collect(),
        };
        let msg = CompactorMessage::CompactionJobFinished {
            id: job.id,
            result: Ok(output_sr),
        };

        // when:
        let result = fixture.handler.handle(msg).await;

        // then:
        assert!(matches!(result, Err(SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_should_not_schedule_conflicting_compaction() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await.unwrap();
        fixture.assert_started_compaction(1);
        fixture.write_l0().await;
        fixture.scheduler.inject_compaction(compaction.clone());

        // when:
        fixture.handler.handle_ticker().await.unwrap();

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
        fixture.handler.handle_ticker().await.unwrap();
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
        use crate::compactor::stats::BYTES_COMPACTED;
        use crate::config::{CompressionCodec, SstBlockSize};

        // given:
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let scheduler_options = SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 1,
            max_compaction_sources: 999,
            include_size_threshold: 4.0,
        }
        .into();
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 128;
        options.compression_codec = Some(CompressionCodec::Zstd);
        options
            .compactor_options
            .as_mut()
            .expect("compactor options missing")
            .scheduler_options = scheduler_options;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
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
        await_compaction(&db, Some(system_clock.clone()))
            .await
            .expect("db was not compacted");

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
    async fn test_validate_compaction_rejects_missing_l0_source() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.handler.handle_ticker().await.unwrap();
        let c = CompactionSpec::new(vec![SourceId::Sst(Ulid::new())], 0);
        let err = fixture.handler.validate_compaction(&c).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    #[tokio::test]
    async fn test_validate_compaction_rejects_missing_sr_source() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.handler.handle_ticker().await.unwrap();
        let c = CompactionSpec::new(vec![SourceId::SortedRun(42)], 42);
        let err = fixture.handler.validate_compaction(&c).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    #[tokio::test]
    async fn test_validate_compaction_l0_only_ok_when_no_sr() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        // ensure at least one L0 exists
        fixture.write_l0().await;
        fixture.handler.handle_ticker().await.unwrap();
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
        fixture.handler.handle_ticker().await.unwrap();
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
        fixture.handler.handle_ticker().await.unwrap();
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
        fixture.handler.handle_ticker().await.unwrap();
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
        fixture.handler.handle_ticker().await.unwrap();
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

    fn build_test_stores(
        os: Arc<dyn ObjectStore>,
    ) -> (Arc<ManifestStore>, Arc<CompactionsStore>, Arc<TableStore>) {
        let sst_format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(PATH), os.clone()));
        let compactions_store = Arc::new(CompactionsStore::new(&Path::from(PATH), os.clone()));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            sst_format,
            Path::from(PATH),
            None,
        ));
        (manifest_store, compactions_store, table_store)
    }

    /// Waits until all writes have made their way to L1 or below. No data is allowed in
    /// in-memory WALs, in-memory memtables, or L0 SSTs on object storage.
    ///
    /// If a clock is provided, it will be advanced the clock by 60 seconds on each iteration to
    /// trigger time-based flushes and compactions.
    async fn await_compaction(
        db: &Db,
        clock: Option<Arc<dyn SystemClock>>,
    ) -> Option<ManifestCore> {
        run_for(Duration::from_secs(10), || async {
            if let Some(clock) = &clock {
                clock.as_ref().advance(Duration::from_millis(60000)).await;
            }
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

    /// If a clock is provided, it will be advanced the clock by 60 seconds on each iteration to
    /// trigger time-based flushes and compactions.
    #[allow(unused)] // only used with feature(wal_disable)
    async fn await_compacted_compaction(
        manifest_store: Arc<ManifestStore>,
        old_compacted: Vec<SortedRun>,
        clock: Option<Arc<dyn SystemClock>>,
    ) -> Option<ManifestCore> {
        run_for(Duration::from_secs(10), || async {
            if let Some(clock) = &clock {
                clock.as_ref().advance(Duration::from_millis(60000)).await;
            }
            let db_state = get_db_state(manifest_store.clone()).await;
            if !db_state.compacted.eq(&old_compacted) {
                return Some(db_state);
            }
            None
        })
        .await
    }

    async fn get_db_state(manifest_store: Arc<ManifestStore>) -> ManifestCore {
        let stored_manifest =
            StoredManifest::load(manifest_store.clone(), Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
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
            scheduler_options: Default::default(),
            ..CompactorOptions::default()
        }
    }

    fn compactor_builder_with_scheduler(
        os: Arc<dyn ObjectStore>,
        scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
    ) -> CompactorBuilder<&'static str> {
        CompactorBuilder::new(PATH, os)
            .with_options(compactor_options())
            .with_scheduler_supplier(scheduler_supplier)
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
        fn propose(&self, _state: &CompactorStateView) -> Vec<CompactionSpec> {
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
