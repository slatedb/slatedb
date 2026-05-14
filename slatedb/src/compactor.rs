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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
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
use crate::db_state::{SortedRun, SsTableView};
use crate::db_status::ClosedResultWriter;
use crate::dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor};
use crate::error::{Error, SlateDBError};
use crate::manifest::store::ManifestStore;
use crate::manifest::{LsmTreeState, ManifestCore, SsTableHandle};
use crate::merge_operator::MergeOperatorType;
use crate::rand::DbRand;
use crate::tablestore::TableStore;
use crate::utils::{format_bytes_si, IdGenerator};
use slatedb_common::clock::SystemClock;
use slatedb_common::metrics::MetricsRecorderHelper;

pub use crate::compactor_state::{
    Compaction, CompactionSpec, CompactionStatus, CompactorState, SourceId,
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

    /// Generate one or more compaction specs from a [`CompactionRequest`].
    ///
    /// - [`CompactionRequest::Spec`] returns the caller-provided spec verbatim.
    /// - [`CompactionRequest::FullSegment`] targets a single tree (root or a
    ///   named segment): every sorted run becomes a source, the tree's lowest
    ///   sorted-run id is the destination. Returns an error if the segment
    ///   is unknown or the tree has no compacted sorted runs.
    /// - [`CompactionRequest::Full`] applies the same per-tree rule to every
    ///   tree in the DB (root + every named segment), best-effort: trees
    ///   without compacted sorted runs are silently skipped and an empty
    ///   `Vec` is returned if no tree is eligible.
    ///
    /// L0 SSTs are intentionally excluded so the size-tiered scheduler can
    /// keep compacting new flush output in parallel. Including L0 would
    /// mark those SSTs as in-use for the duration of the compaction,
    /// eventually stalling writers once L0 reaches `l0_max_ssts`.
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
            CompactionRequest::FullSegment { segment } => {
                let manifest = state.manifest().core();
                let Some(tree) = manifest.tree_for_segment(segment) else {
                    error!(
                        "rejected full-segment compaction: unknown segment {:?}",
                        segment
                    );
                    return Err(crate::Error::from(SlateDBError::InvalidCompaction));
                };
                match plan_full_tree(segment, tree) {
                    Some(spec) => Ok(vec![spec]),
                    None => {
                        if !tree.l0.is_empty() {
                            error!(
                                "rejected full-segment compaction: segment {:?} has L0 SSTs but no sorted runs to merge; \
                                 L0 SSTs are not eligible inputs",
                                segment
                            );
                        }
                        Err(crate::Error::from(SlateDBError::InvalidCompaction))
                    }
                }
            }
            CompactionRequest::Full => {
                let manifest = state.manifest().core();
                let specs = manifest
                    .trees_with_prefix()
                    .filter_map(|(prefix, tree)| plan_full_tree(&prefix, tree))
                    .collect();
                Ok(specs)
            }
        }
    }
}

/// Build a [`CompactionSpec`] that compacts every sorted run in `tree` into
/// the tree's lowest-id sorted run. Returns `None` when the tree has no
/// compacted sorted runs — L0 SSTs are not eligible inputs.
fn plan_full_tree(prefix: &Bytes, tree: &LsmTreeState) -> Option<CompactionSpec> {
    if tree.compacted.is_empty() {
        return None;
    }
    let sources = tree
        .compacted
        .iter()
        .map(|sr| SourceId::SortedRun(sr.id))
        .collect::<Vec<_>>();
    let destination = sources
        .iter()
        .map(|s| s.unwrap_sorted_run())
        .min()
        .expect("at least one sorted run");
    Some(CompactionSpec::for_segment(
        prefix.clone(),
        sources,
        destination,
    ))
}

/// Request to submit a compaction for execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CompactionRequest {
    /// Compact every tree in the DB — the root tree plus each named segment
    /// (RFC-0024) — by merging every sorted run in each tree into that
    /// tree's lowest-id sorted run. Trees with no compacted sorted runs are
    /// silently skipped; an empty `Vec` is returned when no tree is eligible.
    ///
    /// L0 SSTs are left to regular size-tiered compaction so memtable flushes
    /// can continue producing new sorted runs while this compaction is in
    /// flight. Pulling L0 into the merge would mark those SSTs as in-use
    /// for the duration of the compaction, eventually stalling writers once
    /// L0 reaches `l0_max_ssts`.
    Full,
    /// Compact a single tree: every sorted run in the tree at `segment` is
    /// merged into the tree's lowest-id sorted run. `segment: Bytes::new()`
    /// targets the compatibility-encoded root tree (the only valid segment
    /// on a database without a segment extractor); a non-empty prefix names
    /// a segment. Rejected if the segment is unknown or the tree has no
    /// compacted sorted runs. L0 SSTs are excluded for the same reason
    /// documented on `Full`.
    FullSegment { segment: Bytes },
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
        recorder: &MetricsRecorderHelper,
        system_clock: Arc<dyn SystemClock>,
        closed_result: Arc<dyn ClosedResultWriter>,
        merge_operator: Option<MergeOperatorType>,
        #[cfg(feature = "compaction_filters")] compaction_filter_supplier: Option<
            Arc<dyn CompactionFilterSupplier>,
        >,
    ) -> Self {
        let stats = Arc::new(CompactionStats::new(recorder));
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
        let (tx, rx) = async_channel::unbounded();
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
    /// - `SlateDBError::InvalidCompaction` if a full compaction has no sorted run sources.
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
        let compactor_epoch = state_writer.state.manifest().value.compactor_epoch;
        stats.compactor_epoch.set(compactor_epoch as i64);
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
            .set(total_estimated_bytes as i64);
        self.stats.total_throughput.set(total_throughput as i64);
    }

    /// Calculates the estimated total source bytes for a compaction.
    fn calculate_estimated_source_bytes(compaction: &Compaction, db_state: &ManifestCore) -> u64 {
        let tree = db_state
            .tree_for_segment(compaction.spec().segment())
            .expect("compaction target segment missing from manifest");

        let views_by_id: HashMap<Ulid, &SsTableView> =
            tree.l0.iter().map(|view| (view.id, view)).collect();
        let srs_by_id: HashMap<u32, &SortedRun> =
            tree.compacted.iter().map(|sr| (sr.id, sr)).collect();

        compaction
            .spec()
            .sources()
            .iter()
            .map(|source| match source {
                SourceId::SstView(id) => views_by_id
                    .get(id)
                    .expect("compaction source view not found in L0")
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

    /// Validates a Submitted compaction against the current manifest before starting it.
    ///
    /// Runs on every Submitted spec regardless of origin (internal scheduler, admin
    /// submission, reloaded `.compactions`), so this is the canonical gate for
    /// spec-against-current-state validity. Cross-compaction conflicts (destination
    /// collisions across active compactions, concurrent drains on the same segment) are
    /// enforced upstream in [`CompactorState::add_compaction`] and are not re-checked here.
    ///
    /// Invariants checked:
    /// - Compaction has sources
    /// - Drain specs do not target the empty-prefix (root) segment
    /// - The target segment exists in the manifest
    /// - All sources exist in the target segment's tree
    /// - L0-only tiered compactions have a destination > highest SR id across all trees
    /// - A tiered destination does not overwrite a committed SR in any tree unless the SR is among sources
    /// - Drain L0 sources cover every L0 at or below the newest drained L0 in the target tree
    /// - At most one L0 compaction is Running per segment
    /// - Scheduler-specific policy via [`CompactionScheduler::validate_compaction`]
    fn validate_compaction(&self, compaction: &CompactionSpec) -> Result<(), SlateDBError> {
        // Validate compaction sources exist
        if compaction.sources().is_empty() {
            warn!("submitted compaction is empty: {:?}", compaction.sources());
            return Err(SlateDBError::InvalidCompaction);
        }

        // Drain specs cannot target the empty-prefix segment (RFC-0024): the
        // root-tree compatibility encoding is not retired by drain.
        if compaction.is_drain() && compaction.segment().is_empty() {
            warn!("rejected drain compaction targeting the empty-prefix segment");
            return Err(SlateDBError::InvalidCompaction);
        }

        // Validate compaction sources exist in the spec's target segment tree.
        // RFC-0024: every spec names exactly one segment, and its sources must
        // live in that segment's tree.
        let db_state = self.state().db_state();
        let Some(tree) = db_state.tree_for_segment(compaction.segment()) else {
            warn!(
                "submitted compaction targets unknown segment: {:?}",
                compaction.segment()
            );
            return Err(SlateDBError::InvalidCompaction);
        };
        let l0_view_ids = tree
            .l0
            .iter()
            .map(|view| view.id)
            .collect::<std::collections::HashSet<_>>();
        let sr_ids = tree
            .compacted
            .iter()
            .map(|sr| sr.id)
            .collect::<std::collections::HashSet<_>>();

        if let Some(missing) = compaction.sources().iter().find(|source| match source {
            SourceId::SstView(id) => !l0_view_ids.contains(id),
            SourceId::SortedRun(id) => !sr_ids.contains(id),
        }) {
            warn!("compaction source missing from db state: {:?}", missing);
            return Err(SlateDBError::InvalidCompaction);
        }

        // Validate L0-only compactions create a new SR with id > highest existing
        // across all segment trees. SR ids are globally unique (RFC-0024) and the
        // scheduler allocates new L0 → SR destinations strictly above the global
        // max; admin- or reload-submitted specs must observe the same contract.
        if compaction.has_l0_sources() && !compaction.has_sr_sources() {
            let highest_id = db_state
                .trees()
                .flat_map(|t| t.compacted.iter())
                .map(|sr| sr.id)
                .max()
                .map_or(0, |id| id + 1);
            // Drain specs have no destination and aren't subject to this check.
            if let Some(dst) = compaction.destination() {
                if dst < highest_id {
                    warn!(
                        "compaction destination is lesser than the expected L0-only highest_id: {:?} {:?}",
                        dst, highest_id
                    );
                    return Err(SlateDBError::InvalidCompaction);
                }
            }
        }

        Self::validate_destination_overwrite(compaction, db_state)?;
        Self::validate_drain_watermark_advance(compaction, tree)?;

        // Reject parallel L0 compactions within the same segment. Each
        // segment owns its own `last_compacted_l0_sst_view_id` watermark
        // (RFC-0024), so out-of-order completion is only a hazard for
        // compactions sharing a target tree. L0 compactions in disjoint
        // segments — including drain specs in different segments — are
        // safe to run concurrently.
        if compaction.has_l0_sources() {
            let target_segment = compaction.segment();
            let running_l0_in_same_segment = self
                .state()
                .compactions_with_status(CompactionStatus::Running)
                .any(|c| c.spec().has_l0_sources() && c.spec().segment() == target_segment);
            if running_l0_in_same_segment {
                warn!(
                    "rejected compaction: parallel L0 compaction already running in segment {:?}",
                    target_segment
                );
                return Err(SlateDBError::InvalidCompaction);
            }
        }

        self.scheduler
            .validate(&self.state().into(), compaction)
            .map_err(|_e| SlateDBError::InvalidCompaction)
    }

    /// Rejects a tiered compaction whose destination SR id already exists as a
    /// committed SR anywhere in the manifest unless that SR is among the spec's
    /// sources (i.e. the compaction overwrites it as part of the merge). SR ids
    /// are globally unique across all segment trees (RFC-0024). Drain specs
    /// produce no output SR and pass through unchecked.
    fn validate_destination_overwrite(
        compaction: &CompactionSpec,
        db_state: &ManifestCore,
    ) -> Result<(), SlateDBError> {
        let Some(dst) = compaction.destination() else {
            return Ok(());
        };
        let sr_exists_anywhere = db_state
            .trees()
            .flat_map(|t| t.compacted.iter())
            .any(|sr| sr.id == dst);
        let sr_in_sources = compaction.sources().iter().any(|src| match src {
            SourceId::SortedRun(sr) => *sr == dst,
            SourceId::SstView(_) => false,
        });
        if sr_exists_anywhere && !sr_in_sources {
            warn!(
                "compaction destination overwrites committed SR not in sources: {:?}",
                dst
            );
            return Err(SlateDBError::InvalidCompaction);
        }
        Ok(())
    }

    /// Rejects a drain spec that would leave one or more L0s below the new
    /// watermark. The watermark advances to the newest drained L0; every L0
    /// at or below that point in `tree.l0` (which is sorted newest-first)
    /// must be listed among the drain sources, or it would survive in
    /// `tree.l0` while being eclipsed by the watermark — a manifest state
    /// the writer cannot merge. Tiered specs do not advance the watermark
    /// and pass through unchecked.
    fn validate_drain_watermark_advance(
        compaction: &CompactionSpec,
        tree: &LsmTreeState,
    ) -> Result<(), SlateDBError> {
        if !compaction.is_drain() {
            return Ok(());
        }
        let drained_l0_ids: std::collections::HashSet<ulid::Ulid> = compaction
            .sources()
            .iter()
            .filter_map(|s| match s {
                SourceId::SstView(id) => Some(*id),
                SourceId::SortedRun(_) => None,
            })
            .collect();
        if drained_l0_ids.is_empty() {
            return Ok(());
        }
        // tree.l0 is sorted newest-first. Find the newest drained L0's index;
        // every L0 at or after that index (older or equal) must also be drained.
        let Some(newest_drained_idx) = tree
            .l0
            .iter()
            .position(|view| drained_l0_ids.contains(&view.id))
        else {
            return Ok(());
        };
        for view in tree.l0.iter().skip(newest_drained_idx) {
            if !drained_l0_ids.contains(&view.id) {
                warn!(
                    "drain spec leaves L0 below the new watermark: segment={:?}, surviving_l0={:?}",
                    compaction.segment(),
                    view.id
                );
                return Err(SlateDBError::InvalidCompaction);
            }
        }
        Ok(())
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
    /// compactions. Drain specs mutate the manifest directly (no executor merge),
    /// so the persistence step writes both the manifest and the compactions file
    /// when any submission was a drain; otherwise it writes the compactions file alone.
    async fn maybe_start_compactions(&mut self) -> Result<(), SlateDBError> {
        let submitted_compactions = self
            .state()
            .compactions_with_status(CompactionStatus::Submitted)
            .cloned()
            .collect::<Vec<_>>();
        let any_drain = submitted_compactions.iter().any(|c| c.spec().is_drain());

        let result = self
            .start_submitted_compactions(&submitted_compactions)
            .await;

        if !submitted_compactions.is_empty() {
            if any_drain {
                self.state_writer.write_state_safely().await?;
            } else {
                self.state_writer.write_compactions_safely().await?;
            }
        }

        result
    }

    /// Starts (valid) submitted compactions up to the max concurrency limit. Invalid
    /// compactions are marked as failed. Successfully started compactions are marked
    /// as running. Drain compactions short-circuit the executor (they perform no merge)
    /// and are applied directly to the in-memory manifest, transitioning straight to
    /// Completed. This function modifies the state directly but does not persist it;
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

            // Capacity gates only tiered compactions, which run on the
            // executor. Drain specs mutate the manifest directly and never
            // enter `Running`, so they bypass the check.
            if !compaction.spec().is_drain() {
                let running_compaction_count = self.running_compaction_count();
                if running_compaction_count >= self.options.max_concurrent_compactions {
                    info!(
                        "skipping compaction since capacity is exceeded [running_compactions={}, max_concurrent_compactions={}, compaction={:?}]",
                        running_compaction_count,
                        self.options.max_concurrent_compactions,
                        compaction
                    );
                    continue;
                }
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

            // Drain specs apply the watermark advance and SR removal
            // directly, marking the compaction Completed. Tiered specs
            // dispatch to the executor.
            if compaction.spec().is_drain() {
                self.state_mut().finish_drain_compaction(compaction.id());
            } else {
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

        let sst_views = compaction.get_l0_sst_views(db_state);
        let sorted_runs = compaction.get_sorted_runs(db_state);
        let spec = compaction.spec();
        // Drain specs are intercepted in `start_submitted_compactions` before
        // they reach here, so `start_compaction` is tiered-only and the
        // destination is always set.
        let destination = spec
            .destination()
            .expect("start_compaction reached with a drain spec — should have been intercepted");
        // If there are no SRs in the target tree when we compact L0 then the
        // resulting SR is the last sorted run. The check is scoped to the
        // spec's target tree (RFC-0024); if the target segment is missing
        // from the manifest, fall back to false — the commit path will no-op.
        let is_dest_last_run = match db_state.tree_for_segment(spec.segment()) {
            Some(tree) => {
                tree.compacted.is_empty()
                    || tree.compacted.last().is_some_and(|sr| destination == sr.id)
            }
            None => false,
        };

        let job_args = StartCompactionJobArgs {
            id: job_id,
            compaction_id: compaction.id(),
            destination,
            sst_views,
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
            .set(self.system_clock.now().timestamp());
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
    use slatedb_common::metrics::{CounterFn, GaugeFn, MetricsRecorderHelper, UpDownCounterFn};
    use std::sync::Arc;

    pub use crate::merge_operator::MERGE_OPERATOR_OPERANDS;

    use crate::merge_operator::{
        MERGE_OPERATOR_COMPACT_PATH, MERGE_OPERATOR_OPERANDS_DESCRIPTION, MERGE_OPERATOR_PATH_LABEL,
    };

    macro_rules! compactor_stat_name {
        ($suffix:expr) => {
            concat!("slatedb.compactor.", $suffix)
        };
    }

    pub const BYTES_COMPACTED: &str = compactor_stat_name!("bytes_compacted");
    pub const COMPACTOR_EPOCH: &str = compactor_stat_name!("epoch");
    pub const LAST_COMPACTION_TS_SEC: &str = compactor_stat_name!("last_compaction_timestamp_sec");
    pub const RUNNING_COMPACTIONS: &str = compactor_stat_name!("running_compactions");
    pub const TOTAL_BYTES_BEING_COMPACTED: &str =
        compactor_stat_name!("total_bytes_being_compacted");
    pub const TOTAL_THROUGHPUT_BYTES_PER_SEC: &str =
        compactor_stat_name!("total_throughput_bytes_per_sec");
    pub const EXPIRED_ENTRIES_PURGED: &str = compactor_stat_name!("expired_entries_purged");
    pub const EXPIRED_ENTRIES_PURGED_DESCRIPTION: &str =
        "Count of expired entries purged by the RetentionIterator. \
         `entry_type=\"value\"` counts expired value entries rewritten as tombstones; \
         `entry_type=\"merge\"` counts expired merge entries dropped without a tombstone.";
    pub const ENTRY_TYPE_LABEL: &str = "entry_type";
    pub const ENTRY_TYPE_VALUE: &str = "value";
    pub const ENTRY_TYPE_MERGE: &str = "merge";

    pub(crate) struct CompactionStats {
        pub(crate) compactor_epoch: Arc<dyn GaugeFn>,
        pub(crate) last_compaction_ts: Arc<dyn GaugeFn>,
        pub(crate) running_compactions: Arc<dyn UpDownCounterFn>,
        pub(crate) bytes_compacted: Arc<dyn CounterFn>,
        pub(crate) total_bytes_being_compacted: Arc<dyn GaugeFn>,
        pub(crate) total_throughput: Arc<dyn GaugeFn>,
        pub(crate) merge_operator_compact_operands: Arc<dyn CounterFn>,
        pub(crate) expired_entries_purged_value: Arc<dyn CounterFn>,
        pub(crate) expired_entries_purged_merge: Arc<dyn CounterFn>,
    }

    impl CompactionStats {
        pub(crate) fn new(recorder: &MetricsRecorderHelper) -> Self {
            Self {
                compactor_epoch: recorder.gauge(COMPACTOR_EPOCH).register(),
                last_compaction_ts: recorder.gauge(LAST_COMPACTION_TS_SEC).register(),
                running_compactions: recorder.up_down_counter(RUNNING_COMPACTIONS).register(),
                bytes_compacted: recorder.counter(BYTES_COMPACTED).register(),
                total_bytes_being_compacted: recorder.gauge(TOTAL_BYTES_BEING_COMPACTED).register(),
                total_throughput: recorder.gauge(TOTAL_THROUGHPUT_BYTES_PER_SEC).register(),
                merge_operator_compact_operands: recorder
                    .counter(MERGE_OPERATOR_OPERANDS)
                    .labels(&[(MERGE_OPERATOR_PATH_LABEL, MERGE_OPERATOR_COMPACT_PATH)])
                    .description(MERGE_OPERATOR_OPERANDS_DESCRIPTION)
                    .register(),
                expired_entries_purged_value: recorder
                    .counter(EXPIRED_ENTRIES_PURGED)
                    .labels(&[(ENTRY_TYPE_LABEL, ENTRY_TYPE_VALUE)])
                    .description(EXPIRED_ENTRIES_PURGED_DESCRIPTION)
                    .register(),
                expired_entries_purged_merge: recorder
                    .counter(EXPIRED_ENTRIES_PURGED)
                    .labels(&[(ENTRY_TYPE_LABEL, ENTRY_TYPE_MERGE)])
                    .description(EXPIRED_ENTRIES_PURGED_DESCRIPTION)
                    .register(),
            }
        }

        pub(crate) fn retention_metrics(&self) -> crate::retention_iterator::RetentionMetrics {
            crate::retention_iterator::RetentionMetrics {
                expired_entries_purged_value: self.expired_entries_purged_value.clone(),
                expired_entries_purged_merge: self.expired_entries_purged_merge.clone(),
            }
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
    use crate::compactor::stats::COMPACTOR_EPOCH;
    use crate::compactor::stats::LAST_COMPACTION_TS_SEC;
    use crate::compactor_executor::{
        CompactionExecutor, TokioCompactionExecutor, TokioCompactionExecutorOptions,
    };
    use crate::compactor_state::CompactionStatus;
    use crate::compactor_state::SourceId;
    use crate::config::{
        FlushOptions, FlushType, MergeOptions, PutOptions, Settings,
        SizeTieredCompactionSchedulerOptions, Ttl, WriteOptions,
    };
    use crate::db::Db;
    use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::error::SlateDBError;
    use crate::format::sst::{SsTableFormat, SST_FORMAT_VERSION_LATEST};
    use crate::iter::RowEntryIterator;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::{LsmTreeState, Manifest, ManifestCore, Segment, VersionedManifest};
    use crate::merge_operator::{MergeOperator, MergeOperatorError};
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::rng;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, FixedThreeBytePrefixExtractor};
    use crate::types::KeyValue;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};

    const PATH: &str = "/test/db";

    #[derive(Clone)]
    struct SegmentTestScheduler {
        segment: Bytes,
        min_l0_sources: usize,
    }

    impl CompactionScheduler for SegmentTestScheduler {
        fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
            let db_state = state.manifest().core();
            let Some(tree) = db_state.tree_for_segment(self.segment.as_ref()) else {
                return vec![];
            };
            if tree.l0.len() < self.min_l0_sources {
                return vec![];
            }

            let mut sources: Vec<SourceId> = tree
                .l0
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect();
            sources.extend(tree.compacted.iter().map(|sr| SourceId::SortedRun(sr.id)));

            let destination = db_state
                .trees()
                .flat_map(|tree| tree.compacted.iter().map(|sr| sr.id))
                .max()
                .map(|id| id.saturating_add(1))
                .unwrap_or(0);

            vec![CompactionSpec::for_segment(
                self.segment.clone(),
                sources,
                destination,
            )]
        }
    }

    struct SegmentTestSchedulerSupplier {
        scheduler: SegmentTestScheduler,
    }

    impl SegmentTestSchedulerSupplier {
        fn new(segment: Bytes, min_l0_sources: usize) -> Self {
            Self {
                scheduler: SegmentTestScheduler {
                    segment,
                    min_l0_sources,
                },
            }
        }
    }

    impl CompactionSchedulerSupplier for SegmentTestSchedulerSupplier {
        fn compaction_scheduler(
            &self,
            _options: &CompactorOptions,
        ) -> Box<dyn CompactionScheduler + Send + Sync> {
            Box::new(self.scheduler.clone())
        }
    }

    /// Test scheduler that proposes a single drain spec for the target
    /// segment, listing every observed L0 and SR as a source. Used to drive
    /// the segment-drain e2e path; there is no admin-triggered drain API.
    #[derive(Clone)]
    struct SegmentDrainTestScheduler {
        segment: Bytes,
    }

    impl CompactionScheduler for SegmentDrainTestScheduler {
        fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
            let db_state = state.manifest().core();
            let Some(tree) = db_state.tree_for_segment(self.segment.as_ref()) else {
                return vec![];
            };
            if tree.l0.is_empty() && tree.compacted.is_empty() {
                return vec![];
            }
            let active_drain = state
                .compactions()
                .map(|compactions| {
                    compactions.recent_compactions().any(|c| {
                        c.active() && c.spec().is_drain() && c.spec().segment() == &self.segment
                    })
                })
                .unwrap_or(false);
            if active_drain {
                return vec![];
            }
            let mut sources: Vec<SourceId> = tree
                .l0
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect();
            sources.extend(tree.compacted.iter().map(|sr| SourceId::SortedRun(sr.id)));
            vec![CompactionSpec::drain_segment(self.segment.clone(), sources)]
        }
    }

    struct SegmentDrainTestSchedulerSupplier {
        scheduler: SegmentDrainTestScheduler,
    }

    impl SegmentDrainTestSchedulerSupplier {
        fn new(segment: Bytes) -> Self {
            Self {
                scheduler: SegmentDrainTestScheduler { segment },
            }
        }
    }

    impl CompactionSchedulerSupplier for SegmentDrainTestSchedulerSupplier {
        fn compaction_scheduler(
            &self,
            _options: &CompactorOptions,
        ) -> Box<dyn CompactionScheduler + Send + Sync> {
            Box::new(self.scheduler.clone())
        }
    }

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
        // Bigger than all writes so we keep all writes in a single SST.
        // This makes it easier to verify all KV pairs are in that SST.
        options.l0_sst_size_bytes = 512;
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
            db.put_with_options(
                &k,
                &v,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let k = vec![b'j' + i as u8; 16];
            let v = vec![b'k' + i as u8; 48];
            db.put_with_options(
                &k,
                &v,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            expected.insert(k.clone(), v.clone());
        }

        // Force all writes to a single L0 SST.
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        // when:
        let db_state = await_compaction(&db, Some(system_clock.clone())).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        for run in db_state.tree.compacted {
            for sst in run.sst_views {
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
                while let Some(kv) = iter.next().await.unwrap().map(KeyValue::from) {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compactor_compacts_only_target_segment() {
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let path = "/tmp/test_compactor_compacts_only_target_segment";
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 512;
        options.flush_interval = None;

        let db = Db::builder(path, os.clone())
            .with_settings(options.clone())
            .with_system_clock(system_clock.clone())
            .with_segment_extractor(Arc::new(FixedThreeBytePrefixExtractor))
            .with_compactor_builder(
                CompactorBuilder::new(path, os.clone())
                    .with_options(compactor_options())
                    .with_scheduler_supplier(Arc::new(SegmentTestSchedulerSupplier::new(
                        Bytes::from_static(b"aaa"),
                        2,
                    ))),
            )
            .build()
            .await
            .unwrap();

        for (key, value) in [
            (b"aaa-001".as_slice(), b"v1".as_slice()),
            (b"aaa-002".as_slice(), b"v2".as_slice()),
            (b"aaa-003".as_slice(), b"v3".as_slice()),
            (b"bbb-001".as_slice(), b"v4".as_slice()),
        ] {
            put_and_flush_memtable(&db, key, value).await;
        }

        let db_state = run_for(Duration::from_secs(10), || async {
            system_clock
                .as_ref()
                .advance(Duration::from_millis(60000))
                .await;
            let core = read_db_state_core(&db);
            let aaa = core
                .segments
                .iter()
                .find(|segment| segment.prefix.as_ref() == b"aaa")
                .expect("missing segment aaa");
            let bbb = core
                .segments
                .iter()
                .find(|segment| segment.prefix.as_ref() == b"bbb")
                .expect("missing segment bbb");
            if aaa.tree.l0.is_empty()
                && !aaa.tree.compacted.is_empty()
                && bbb.tree.l0.len() == 1
                && bbb.tree.compacted.is_empty()
            {
                Some(core)
            } else {
                None
            }
        })
        .await
        .expect("segment-scoped compaction did not complete");

        let aaa = db_state
            .segments
            .iter()
            .find(|segment| segment.prefix.as_ref() == b"aaa")
            .expect("missing segment aaa");
        let bbb = db_state
            .segments
            .iter()
            .find(|segment| segment.prefix.as_ref() == b"bbb")
            .expect("missing segment bbb");
        assert!(aaa.tree.l0.is_empty());
        assert_eq!(aaa.tree.compacted.len(), 1);
        assert_eq!(bbb.tree.l0.len(), 1);
        assert!(bbb.tree.compacted.is_empty());

        for (key, value) in [
            (b"aaa-001".as_slice(), b"v1".as_slice()),
            (b"aaa-002".as_slice(), b"v2".as_slice()),
            (b"aaa-003".as_slice(), b"v3".as_slice()),
            (b"bbb-001".as_slice(), b"v4".as_slice()),
        ] {
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Bytes::copy_from_slice(value))
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compactor_compacts_all_segments_with_size_tiered_scheduler() {
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let path = "/tmp/test_compactor_compacts_all_segments_with_size_tiered_scheduler";
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 512;
        options.flush_interval = None;
        let scheduler_options = SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            max_compaction_sources: 999,
            include_size_threshold: 4.0,
        }
        .into();
        let compactor_opts = options
            .compactor_options
            .as_mut()
            .expect("compactor options must be set");
        compactor_opts.scheduler_options = scheduler_options;
        compactor_opts.max_concurrent_compactions = 3;

        let db = Db::builder(path, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_segment_extractor(Arc::new(FixedThreeBytePrefixExtractor))
            .build()
            .await
            .unwrap();

        let entries: &[(&[u8], &[u8])] = &[
            (b"aaa-001", b"v1"),
            (b"aaa-002", b"v2"),
            (b"bbb-001", b"v3"),
            (b"bbb-002", b"v4"),
            (b"ccc-001", b"v5"),
            (b"ccc-002", b"v6"),
        ];

        for (key, value) in entries {
            put_and_flush_memtable(&db, key, value).await;
        }

        let prefixes: [&[u8]; 3] = [b"aaa", b"bbb", b"ccc"];

        let db_state = run_for(Duration::from_secs(10), || async {
            system_clock
                .as_ref()
                .advance(Duration::from_millis(60000))
                .await;
            let core = read_db_state_core(&db);
            let all_compacted = prefixes.iter().all(|prefix| {
                core.segments
                    .iter()
                    .find(|segment| segment.prefix.as_ref() == *prefix)
                    .map(|segment| segment.tree.l0.is_empty() && !segment.tree.compacted.is_empty())
                    .unwrap_or(false)
            });
            if all_compacted {
                Some(core)
            } else {
                None
            }
        })
        .await
        .expect("not every segment compacted via size-tiered scheduler");

        for prefix in prefixes {
            let segment = db_state
                .segments
                .iter()
                .find(|segment| segment.prefix.as_ref() == prefix)
                .unwrap_or_else(|| panic!("missing segment {:?}", prefix));
            assert!(
                segment.tree.l0.is_empty(),
                "segment {:?} still has L0 SSTs",
                prefix
            );
            assert_eq!(
                segment.tree.compacted.len(),
                1,
                "segment {:?} should have a single sorted run",
                prefix
            );
        }

        for (key, value) in entries {
            assert_eq!(
                db.get(key).await.unwrap(),
                Some(Bytes::copy_from_slice(value))
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compactor_drains_segment() {
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let path = "/tmp/test_compactor_drains_segment";
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 512;
        options.flush_interval = None;

        let db = Db::builder(path, os.clone())
            .with_settings(options.clone())
            .with_system_clock(system_clock.clone())
            .with_segment_extractor(Arc::new(FixedThreeBytePrefixExtractor))
            .with_compactor_builder(
                CompactorBuilder::new(path, os.clone())
                    .with_options(compactor_options())
                    .with_scheduler_supplier(Arc::new(SegmentDrainTestSchedulerSupplier::new(
                        Bytes::from_static(b"aaa"),
                    ))),
            )
            .build()
            .await
            .unwrap();

        for (key, value) in [
            (b"aaa-001".as_slice(), b"v1".as_slice()),
            (b"aaa-002".as_slice(), b"v2".as_slice()),
            (b"bbb-001".as_slice(), b"v3".as_slice()),
        ] {
            put_and_flush_memtable(&db, key, value).await;
        }

        // The marker is pruned on the writer's next commit, so periodically
        // write a fresh bbb-* key to nudge the writer-side merge.
        let nudge = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let db_state = run_for(Duration::from_secs(10), || {
            let db = &db;
            let system_clock = system_clock.clone();
            let nudge_value = nudge.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                system_clock
                    .as_ref()
                    .advance(Duration::from_millis(60000))
                    .await;
                let key = format!("bbb-nudge-{:04}", nudge_value);
                put_and_flush_memtable(db, key.as_bytes(), b"nudge").await;
                let core = read_db_state_core(db);
                let aaa_gone = !core
                    .segments
                    .iter()
                    .any(|segment| segment.prefix.as_ref() == b"aaa");
                let bbb_present = core
                    .segments
                    .iter()
                    .any(|segment| segment.prefix.as_ref() == b"bbb");
                if aaa_gone && bbb_present {
                    Some(core)
                } else {
                    None
                }
            }
        })
        .await
        .expect("drain marker for segment aaa was not pruned");

        assert!(
            !db_state
                .segments
                .iter()
                .any(|segment| segment.prefix.as_ref() == b"aaa"),
            "aaa segment should be gone from manifest"
        );
        let bbb = db_state
            .segments
            .iter()
            .find(|segment| segment.prefix.as_ref() == b"bbb")
            .expect("missing segment bbb");
        assert!(
            !bbb.tree.l0.is_empty() || !bbb.tree.compacted.is_empty(),
            "bbb segment should still hold data"
        );

        assert_eq!(db.get(b"aaa-001").await.unwrap(), None);
        assert_eq!(db.get(b"aaa-002").await.unwrap(), None);
        assert_eq!(
            db.get(b"bbb-001").await.unwrap(),
            Some(Bytes::from_static(b"v3"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_scan_across_segments_after_compaction() {
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let path = "/tmp/test_scan_across_segments_after_compaction";
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 512;
        options.flush_interval = None;
        let scheduler_options = SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            max_compaction_sources: 999,
            include_size_threshold: 4.0,
        }
        .into();
        let compactor_opts = options
            .compactor_options
            .as_mut()
            .expect("compactor options must be set");
        compactor_opts.scheduler_options = scheduler_options;
        compactor_opts.max_concurrent_compactions = 3;

        let db = Db::builder(path, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_segment_extractor(Arc::new(FixedThreeBytePrefixExtractor))
            .build()
            .await
            .unwrap();

        // Two keys per segment, each flushed to its own L0 SST. Written in
        // intra-segment lex order so the post-compaction scan order is
        // deterministic without re-sorting on the test side.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = [
            b"aaa-001".as_slice(),
            b"aaa-002",
            b"bbb-001",
            b"bbb-002",
            b"ccc-001",
            b"ccc-002",
        ]
        .iter()
        .enumerate()
        .map(|(i, k)| (k.to_vec(), format!("v{}", i + 1).into_bytes()))
        .collect();

        for (key, value) in &entries {
            put_and_flush_memtable(&db, key, value).await;
        }

        let prefixes: [&[u8]; 3] = [b"aaa", b"bbb", b"ccc"];
        let _db_state = run_for(Duration::from_secs(10), || async {
            system_clock
                .as_ref()
                .advance(Duration::from_millis(60000))
                .await;
            let core = read_db_state_core(&db);
            let all_compacted = prefixes.iter().all(|prefix| {
                core.segments
                    .iter()
                    .find(|segment| segment.prefix.as_ref() == *prefix)
                    .map(|segment| segment.tree.l0.is_empty() && !segment.tree.compacted.is_empty())
                    .unwrap_or(false)
            });
            if all_compacted {
                Some(core)
            } else {
                None
            }
        })
        .await
        .expect("not every segment compacted before scan");

        // Full ascending scan crosses every segment in prefix order.
        let mut iter = db.scan::<Vec<u8>, _>(..).await.unwrap();
        let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some(kv) = iter.next().await.unwrap() {
            collected.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        assert_eq!(
            collected, entries,
            "full scan should yield every key in order"
        );

        // Per-segment prefix scan should return only that segment's keys.
        for prefix in prefixes {
            let mut iter = db.scan_prefix(prefix).await.unwrap();
            let mut keys: Vec<Vec<u8>> = Vec::new();
            while let Some(kv) = iter.next().await.unwrap() {
                keys.push(kv.key.to_vec());
            }
            let expected: Vec<Vec<u8>> = entries
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, _)| k.clone())
                .collect();
            assert_eq!(keys, expected, "prefix scan for {:?}", prefix);
        }

        // Cross-segment range scan: spans the tail of aaa, all of bbb, and stops
        // before the head of ccc.
        let mut iter = db
            .scan::<Vec<u8>, _>(b"aaa-002".to_vec()..b"ccc-001".to_vec())
            .await
            .unwrap();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        while let Some(kv) = iter.next().await.unwrap() {
            keys.push(kv.key.to_vec());
        }
        assert_eq!(
            keys,
            vec![
                b"aaa-002".to_vec(),
                b"bbb-001".to_vec(),
                b"bbb-002".to_vec(),
            ],
            "range scan spanning aaa and bbb"
        );
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
                state.manifest().core().tree.l0.len() == 2 ||
                // or when there is one SST in L0 and one in L1 (one for delete key 'a' and one for compacted key 'a'+'b')
                (state.manifest().core().tree.l0.len() == 1
                    && state.manifest().core().tree.compacted.len() == 1)
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
                system_clock.clone(),
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
        assert_eq!(db_state.tree.compacted.len(), 1);
        assert_eq!(db_state.tree.l0.len(), 0, "{:?}", db_state.tree.l0);

        // put tombstone for key a into L0
        db.delete_with_options(
            &[b'a'; 16],
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        // Then:
        // we should now have a tombstone in L0 and a value in L1
        let db_state = get_db_state(manifest_store.clone()).await;
        assert_eq!(db_state.tree.l0.len(), 1, "{:?}", db_state.tree.l0);
        assert_eq!(db_state.tree.compacted.len(), 1);

        let l0 = db_state.tree.l0.front().unwrap();
        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            l0,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        let tombstone = iter.next().await.unwrap();
        assert!(tombstone.unwrap().value.is_tombstone());

        let db_state = await_compacted_compaction(
            manifest_store.clone(),
            db_state.tree.compacted,
            Some(system_clock.clone()),
        )
        .await
        .unwrap();
        assert_eq!(db_state.tree.compacted.len(), 1);

        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
        let next = iter.next().await.unwrap().map(KeyValue::from);
        assert_eq!(next.unwrap().key.as_ref(), &[b'b'; 16]);
        let next = iter.next().await.unwrap().map(KeyValue::from);
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
                state.manifest().core().tree.l0.len() == 2 ||
                // or when there is one SST in L0 and one in L1
                (state.manifest().core().tree.l0.len() == 1
                    && state.manifest().core().tree.compacted.len() == 1)
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
                system_clock.clone(),
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
        assert_eq!(db_state.tree.compacted.len(), 1);
        assert_eq!(db_state.tree.l0.len(), 0, "{:?}", db_state.tree.l0);

        // Now delete key 'a', creating a tombstone (seq=3)
        db.delete_with_options(
            &[b'a'; 16],
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        // We should now have a tombstone for 'a' in L0 and both 'a' and 'b' values in L1
        let db_state = get_db_state(manifest_store.clone()).await;
        assert_eq!(db_state.tree.l0.len(), 1, "{:?}", db_state.tree.l0);
        assert_eq!(db_state.tree.compacted.len(), 1);

        // Trigger compaction of L0 (tombstone) + L1 (values)
        let db_state = await_compacted_compaction(
            manifest_store.clone(),
            db_state.tree.compacted,
            Some(system_clock.clone()),
        )
        .await
        .unwrap();
        assert_eq!(db_state.tree.compacted.len(), 1);

        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
        //   The SST contains both versions
        // - Key 'b': value(seq=2) is protected
        // Expected result: both 'a' (as tombstone) and 'b' (as value) should be present
        // Note: next() returns all entries including tombstones and duplicates,
        // so we need to track whether we've seen each key and verify the expected type
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key.as_ref(), &[b'a'; 16]);
        assert!(next.value.is_tombstone());

        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key.as_ref(), &[b'a'; 16]);
        assert_eq!(next.value.as_bytes().unwrap().as_ref(), &[b'a'; 32]);

        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key.as_ref(), &[b'b'; 16]);
        assert_eq!(next.value.as_bytes().unwrap().as_ref(), &[b'a'; 32]);

        let next = iter.next().await.unwrap();
        assert!(next.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_apply_merge_during_l0_compaction() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().core().tree.l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
                system_clock.clone(),
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(system_clock)).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.tree.compacted.len(), 1);
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
    async fn should_record_merge_operator_operands_on_compact_path() {
        use crate::merge_operator::MERGE_OPERATOR_COMPACT_PATH;
        use crate::test_utils::{
            lookup_merge_operator_operands, OnDemandCompactionSchedulerSupplier,
        };
        use slatedb_common::metrics::DefaultMetricsRecorder;

        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().core().tree.l0.len() >= 2,
        )));
        let metrics_recorder = Arc::new(DefaultMetricsRecorder::new());

        let db = Db::builder(PATH, os.clone())
            .with_settings(db_options(None))
            .with_system_clock(system_clock.clone())
            .with_metrics_recorder(metrics_recorder.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler,
                system_clock.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        assert_eq!(
            lookup_merge_operator_operands(&metrics_recorder, MERGE_OPERATOR_COMPACT_PATH),
            Some(0)
        );

        db.merge_with_options(
            b"key1",
            b"a",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        db.merge_with_options(
            b"key1",
            b"b",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        let db_state = await_compaction(&db, Some(system_clock)).await;
        assert!(db_state.is_some(), "db was not compacted");
        assert!(
            lookup_merge_operator_operands(&metrics_recorder, MERGE_OPERATOR_COMPACT_PATH)
                .is_some_and(|value| value > 0)
        );

        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_apply_merge_across_l0_and_sorted_runs() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| !state.manifest().core().tree.l0.is_empty(),
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
                system_clock.clone(),
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        let db_state = await_compaction(&db, Some(system_clock.clone())).await;
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.tree.compacted.len(), 1);
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when: compact L0 with the existing sorted run
        let db_state = await_compaction(&db, Some(system_clock)).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.tree.compacted.len(), 1);
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
            |state| state.manifest().core().tree.l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
                system_clock.clone(),
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(system_clock)).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.tree.compacted.len(), 1);
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
            |state| state.manifest().core().tree.l0.len() >= 3,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
                system_clock.clone(),
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .await
        .unwrap(); // padding to exceed 256 bytes
        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(system_clock)).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert_eq!(db_state.tree.compacted.len(), 1);
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
            |state| state.manifest().core().tree.l0.len() >= 2,
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
                insert_clock.clone(),
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let db_state = await_compaction(&db, Some(insert_clock.clone()))
            .await
            .unwrap();
        assert_eq!(db_state.tree.compacted.len(), 1);
        assert_eq!(db_state.last_l0_clock_tick, 20);

        // then: the compacted SST should only contain the non-expired merge
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
            |state| state.manifest().core().tree.l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
                system_clock.clone(),
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
            !db_state.tree.compacted.is_empty(),
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
            |state| state.manifest().core().tree.l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
                system_clock.clone(),
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
            !db_state.tree.compacted.is_empty(),
            "compaction should have occurred"
        );

        // The compacted sorted run should contain both merge operations separately
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
        while let Some(entry) = iter.next().await.unwrap() {
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
    async fn should_merge_operations_with_same_expire_at() {
        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        // given:
        let os = Arc::new(InMemory::new());
        let system_clock = Arc::new(MockSystemClock::new());
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
            |state| state.manifest().core().tree.l0.len() >= 2,
        )));
        let options = db_options(None);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_compactor_builder(compactor_builder_with_scheduler(
                os.clone(),
                compaction_scheduler.clone(),
                system_clock.clone(),
            ))
            .with_merge_operator(Arc::new(StringConcatMergeOperator))
            .build()
            .await
            .unwrap();

        let (manifest_store, _compactions_store, table_store) = build_test_stores(os.clone());

        let flush_opts = FlushOptions {
            flush_type: FlushType::MemTable,
        };

        // write merge operations with the SAME ExpireAt timestamp at different clock times
        system_clock.set(100);
        db.merge_with_options(
            b"key1",
            b"a",
            &MergeOptions {
                ttl: Ttl::ExpireAt(1000),
            },
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush_with_options(flush_opts.clone()).await.unwrap();

        system_clock.set(200);
        db.merge_with_options(
            b"key1",
            b"b",
            &MergeOptions {
                ttl: Ttl::ExpireAt(1000),
            },
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush_with_options(flush_opts.clone()).await.unwrap();

        // when:
        let _ = await_compaction(&db, Some(system_clock)).await;

        // then: verify in the compacted SST that all merge operations were combined
        let stored_manifest =
            StoredManifest::load(manifest_store.clone(), Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let db_state = stored_manifest.db_state();
        assert!(
            !db_state.tree.compacted.is_empty(),
            "compaction should have occurred"
        );

        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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

        // collect key1 entries from the compacted SST
        let mut key1_entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            if entry.key.as_ref() == b"key1" {
                key1_entries.push(entry);
            }
        }

        // there should be exactly one merged entry for key1
        assert_eq!(
            key1_entries.len(),
            1,
            "expected a single merged entry for key1, got {}",
            key1_entries.len()
        );
        let merged = &key1_entries[0];
        assert_eq!(merged.expire_ts, Some(1000));
        assert!(
            matches!(&merged.value, crate::types::ValueDeletable::Merge(v) if v.as_ref() == b"ab"),
            "expected merged value 'ab', got {:?}",
            merged.value
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_compact_expired_expire_at_entries() {
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
        let flush_opts = FlushOptions {
            flush_type: FlushType::MemTable,
        };

        // ticker time = 0, expire at 10
        insert_clock.set(0);
        db.put_with_options(
            &[1; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAt(10),
            },
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // ticker time = 5, expire at far future (well beyond compaction time)
        insert_clock.set(5);
        db.put_with_options(
            &[2; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAt(i64::MAX),
            },
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        db.flush_with_options(flush_opts.clone()).await.unwrap();

        // ticker time = 10, no expire time
        insert_clock.set(10);
        db.put_with_options(
            &[3; 16],
            value,
            &PutOptions { ttl: Ttl::NoExpiry },
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        db.flush_with_options(flush_opts.clone()).await.unwrap();

        // when: await_compaction advances clock by 60s per iteration,
        // so compaction_start_ts will be well past expire_at=10
        let db_state = await_compaction(&db, Some(insert_clock)).await;

        // then: key 1 should be expired (expire_at=10 < compaction_time),
        //       key 2 should survive (expire_at=i64::MAX), key 3 has no expiry
        let db_state = db_state.expect("db was not compacted");
        assert!(db_state.tree.last_compacted_l0_sst_view_id.is_some());
        assert_eq!(db_state.tree.compacted.len(), 1);
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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
                RowEntry::new_value(&[2; 16], value, 2)
                    .with_create_ts(5)
                    .with_expire_ts(i64::MAX),
                RowEntry::new_value(&[3; 16], value, 3).with_create_ts(10),
            ],
        )
        .await;
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        )
        .await
        .unwrap();

        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, Some(insert_clock)).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert!(db_state.tree.last_compacted_l0_sst_view_id.is_some());
        assert_eq!(db_state.tree.compacted.len(), 1);
        assert_eq!(db_state.last_l0_clock_tick, 70);
        let compacted = &db_state.tree.compacted.first().unwrap().sst_views;
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

        let total_bytes = slatedb_common::metrics::lookup_metric(
            &fixture.test_recorder,
            TOTAL_BYTES_BEING_COMPACTED,
        )
        .expect("metric not found");
        assert_eq!(total_bytes, 0);

        let throughput = slatedb_common::metrics::lookup_metric(
            &fixture.test_recorder,
            TOTAL_THROUGHPUT_BYTES_PER_SEC,
        )
        .expect("metric not found");
        assert!(
            throughput > 0,
            "Expected throughput > 0, got {}",
            throughput
        );
    }

    #[test]
    fn test_calculate_estimated_source_bytes_uses_target_segment_tree() {
        let segment_l0 = SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(b"seg/a")),
                index_offset: 11,
                index_len: 13,
                ..SsTableInfo::default()
            },
        ));
        let segment_sr_view = SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(b"seg/m")),
                index_offset: 17,
                index_len: 19,
                ..SsTableInfo::default()
            },
        ));
        let segment_sr = SortedRun {
            id: 7,
            sst_views: vec![segment_sr_view.clone()],
        };

        let mut core = ManifestCore::new();
        core.segments = vec![Segment {
            prefix: Bytes::from_static(b"seg"),
            tree: LsmTreeState {
                l0: VecDeque::from(vec![segment_l0.clone()]),
                compacted: vec![segment_sr.clone()],
                ..LsmTreeState::default()
            },
        }];

        let compaction = Compaction::new(
            Ulid::new(),
            CompactionSpec::for_segment(
                Bytes::from_static(b"seg"),
                vec![
                    SourceId::SstView(segment_l0.id),
                    SourceId::SortedRun(segment_sr.id),
                ],
                9,
            ),
        );

        let expected = segment_l0.estimate_size() + segment_sr.estimate_size();
        let actual = CompactorEventHandler::calculate_estimated_source_bytes(&compaction, &core);
        assert_eq!(actual, expected);
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

        let running =
            slatedb_common::metrics::lookup_metric(&fixture.test_recorder, RUNNING_COMPACTIONS)
                .expect("metric not found");
        assert_eq!(running, 0);

        let compaction = Compaction::new(Ulid::new(), CompactionSpec::new(vec![], 10));
        fixture
            .handler
            .state_mut()
            .add_compaction(compaction)
            .expect("failed to add compaction");

        assert_eq!(fixture.handler.state().active_compactions().count(), 1);
    }

    #[tokio::test]
    async fn test_should_record_compactor_epoch() {
        let fixture = CompactorEventHandlerTestFixture::new().await;

        let compactor_epoch =
            slatedb_common::metrics::lookup_metric(&fixture.test_recorder, COMPACTOR_EPOCH)
                .expect("metric not found");

        assert_eq!(
            compactor_epoch,
            fixture.handler.state().manifest().value.compactor_epoch as i64
        );
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

        let compactions = compactions_store.read_latest_compactions().await.unwrap();
        let stored = compactions
            .compactions
            .get(&compaction_id)
            .expect("missing submitted compaction");

        assert_eq!(stored.spec(), &spec);
        assert_eq!(stored.status(), CompactionStatus::Submitted);
    }

    #[tokio::test]
    async fn test_submit_full_compaction_uses_sorted_run_sources_only() {
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

        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        let l0_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"a")),
            ..SsTableInfo::default()
        };
        let sr_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"m")),
            ..SsTableInfo::default()
        };
        let l0_view_newest: SsTableView = SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            l0_info.clone(),
        ));
        let l0_view_oldest: SsTableView = SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            l0_info.clone(),
        ));
        dirty.value.core.tree.l0 = VecDeque::from(vec![l0_view_newest, l0_view_oldest]);
        dirty.value.core.tree.compacted = vec![
            SortedRun {
                id: 2,
                sst_views: vec![SsTableView::identity(SsTableHandle::new(
                    SsTableId::Compacted(Ulid::new()),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info.clone(),
                ))],
            },
            SortedRun {
                id: 1,
                sst_views: vec![SsTableView::identity(SsTableHandle::new(
                    SsTableId::Compacted(Ulid::new()),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info.clone(),
                ))],
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
                    manifest: VersionedManifest::from_manifest(
                        0,
                        stored_manifest.manifest().clone(),
                    ),
                },
                &CompactionRequest::FullSegment {
                    segment: Bytes::new(),
                },
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

        let compactions = compactions_store.read_latest_compactions().await.unwrap();
        let stored = compactions
            .compactions
            .get(&compaction_id)
            .expect("missing submitted compaction");
        let expected_sources = vec![SourceId::SortedRun(2), SourceId::SortedRun(1)];

        assert_eq!(stored.spec().sources(), &expected_sources);
        assert_eq!(stored.spec().destination(), Some(1));
        assert_eq!(stored.status(), CompactionStatus::Submitted);
    }

    #[test]
    fn test_plan_spec_returns_spec_clone() {
        let scheduler = MockScheduler::new();
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(ManifestCore::new())),
        };
        let spec = CompactionSpec::new(vec![SourceId::SortedRun(7)], 7);

        let planned = scheduler
            .generate(&state, &CompactionRequest::Spec(spec.clone()))
            .unwrap();

        assert_eq!(planned, vec![spec]);
    }

    #[test]
    fn test_plan_full_segment_root_uses_sorted_runs_and_min_destination() {
        let scheduler = MockScheduler::new();
        let mut core = ManifestCore::new();
        let l0_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"a")),
            ..SsTableInfo::default()
        };
        let sr_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"m")),
            ..SsTableInfo::default()
        };
        let l0_view_first: SsTableView = SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(Ulid::from_parts(1, 0)),
            SST_FORMAT_VERSION_LATEST,
            l0_info.clone(),
        ));
        let l0_view_second: SsTableView = SsTableView::identity(SsTableHandle::new(
            SsTableId::Compacted(Ulid::from_parts(2, 0)),
            SST_FORMAT_VERSION_LATEST,
            l0_info,
        ));
        core.tree.l0 = VecDeque::from(vec![l0_view_first, l0_view_second]);
        core.tree.compacted = vec![
            SortedRun {
                id: 5,
                sst_views: vec![SsTableView::identity(SsTableHandle::new(
                    SsTableId::Compacted(Ulid::from_parts(10, 0)),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info.clone(),
                ))],
            },
            SortedRun {
                id: 2,
                sst_views: vec![SsTableView::identity(SsTableHandle::new(
                    SsTableId::Compacted(Ulid::from_parts(11, 0)),
                    SST_FORMAT_VERSION_LATEST,
                    sr_info,
                ))],
            },
        ];
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(core)),
        };

        let planned = scheduler
            .generate(
                &state,
                &CompactionRequest::FullSegment {
                    segment: Bytes::new(),
                },
            )
            .unwrap();

        let expected_sources = vec![SourceId::SortedRun(5), SourceId::SortedRun(2)];
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].sources(), &expected_sources);
        assert_eq!(planned[0].destination(), Some(2));
    }

    #[test]
    fn test_plan_full_segment_root_without_sorted_runs_is_invalid() {
        let scheduler = MockScheduler::new();
        let mut core = ManifestCore::new();
        let l0_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"a")),
            ..SsTableInfo::default()
        };
        core.tree.l0 = VecDeque::from(vec![
            SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(Ulid::from_parts(1, 0)),
                SST_FORMAT_VERSION_LATEST,
                l0_info.clone(),
            )),
            SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(Ulid::from_parts(2, 0)),
                SST_FORMAT_VERSION_LATEST,
                l0_info,
            )),
        ]);
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(core)),
        };

        let err = scheduler
            .generate(
                &state,
                &CompactionRequest::FullSegment {
                    segment: Bytes::new(),
                },
            )
            .expect_err(
                "full-segment should reject empty or L0-only inputs because L0 SSTs are excluded",
            );

        assert_eq!(err.kind(), crate::ErrorKind::Invalid);
        assert_eq!(err.to_string(), "Invalid error: invalid compaction");
    }

    #[test]
    fn test_plan_full_segment_targets_named_segment() {
        // FullSegment against a named segment must pull sources from that
        // segment's tree, not the root tree. RFC-0024 routes named-segment
        // SRs through `core.segments[..].tree.compacted`, so a request
        // against `b"key"` should ignore root-tree SRs entirely.
        let scheduler = MockScheduler::new();
        let mut core = ManifestCore::new();
        core.segment_extractor_name = Some("test".into());
        let sr_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"key1")),
            ..SsTableInfo::default()
        };
        core.segments = vec![Segment {
            prefix: Bytes::from_static(b"key"),
            tree: LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: VecDeque::new(),
                compacted: vec![
                    SortedRun {
                        id: 7,
                        sst_views: vec![SsTableView::identity(SsTableHandle::new(
                            SsTableId::Compacted(Ulid::from_parts(70, 0)),
                            SST_FORMAT_VERSION_LATEST,
                            sr_info.clone(),
                        ))],
                    },
                    SortedRun {
                        id: 3,
                        sst_views: vec![SsTableView::identity(SsTableHandle::new(
                            SsTableId::Compacted(Ulid::from_parts(30, 0)),
                            SST_FORMAT_VERSION_LATEST,
                            sr_info,
                        ))],
                    },
                ],
            },
        }];
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(core)),
        };

        let planned = scheduler
            .generate(
                &state,
                &CompactionRequest::FullSegment {
                    segment: Bytes::from_static(b"key"),
                },
            )
            .unwrap();

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].segment().as_ref(), b"key");
        assert_eq!(
            planned[0].sources(),
            &vec![SourceId::SortedRun(7), SourceId::SortedRun(3)]
        );
        assert_eq!(planned[0].destination(), Some(3));
    }

    #[test]
    fn test_plan_full_segment_unknown_segment_is_invalid() {
        // Asking to compact a non-empty segment prefix on an unsegmented DB
        // (root has sorted runs, no segments configured) must be rejected.
        // A segment-blind implementation that ignored the prefix would
        // happily return root SRs here — this asserts the routing actually
        // looks the segment up.
        let scheduler = MockScheduler::new();
        let mut core = ManifestCore::new();
        let sr_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"r")),
            ..SsTableInfo::default()
        };
        core.tree.compacted = vec![SortedRun {
            id: 9,
            sst_views: vec![SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(Ulid::from_parts(90, 0)),
                SST_FORMAT_VERSION_LATEST,
                sr_info,
            ))],
        }];
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(core)),
        };

        let err = scheduler
            .generate(
                &state,
                &CompactionRequest::FullSegment {
                    segment: Bytes::from_static(b"missing"),
                },
            )
            .expect_err("full-segment against unknown segment should fail");

        assert_eq!(err.kind(), crate::ErrorKind::Invalid);
    }

    #[test]
    fn test_plan_full_segment_single_sorted_run_is_allowed() {
        // A tree with exactly one SR is a legitimate target — the merge
        // pipeline still applies retention, TTL, compaction filters, and
        // merge-operator collapses. Reject only when there are no SRs at all.
        let scheduler = MockScheduler::new();
        let mut core = ManifestCore::new();
        let sr_info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"a")),
            ..SsTableInfo::default()
        };
        core.tree.compacted = vec![SortedRun {
            id: 4,
            sst_views: vec![SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(Ulid::from_parts(40, 0)),
                SST_FORMAT_VERSION_LATEST,
                sr_info,
            ))],
        }];
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(core)),
        };

        let planned = scheduler
            .generate(
                &state,
                &CompactionRequest::FullSegment {
                    segment: Bytes::new(),
                },
            )
            .unwrap();

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].sources(), &vec![SourceId::SortedRun(4)]);
        assert_eq!(planned[0].destination(), Some(4));
    }

    #[test]
    fn test_plan_full_sweeps_root_and_every_named_segment() {
        // Full must produce one spec per eligible tree, ordered as
        // `trees_with_prefix()` yields them: root first, then segments by
        // ascending prefix.
        let scheduler = MockScheduler::new();
        let mut core = ManifestCore::new();
        let info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"x")),
            ..SsTableInfo::default()
        };
        core.tree.compacted = vec![
            SortedRun {
                id: 8,
                sst_views: vec![SsTableView::identity(SsTableHandle::new(
                    SsTableId::Compacted(Ulid::from_parts(80, 0)),
                    SST_FORMAT_VERSION_LATEST,
                    info.clone(),
                ))],
            },
            SortedRun {
                id: 4,
                sst_views: vec![SsTableView::identity(SsTableHandle::new(
                    SsTableId::Compacted(Ulid::from_parts(40, 0)),
                    SST_FORMAT_VERSION_LATEST,
                    info.clone(),
                ))],
            },
        ];
        core.segment_extractor_name = Some("test".into());
        core.segments = vec![
            Segment {
                prefix: Bytes::from_static(b"a/"),
                tree: LsmTreeState {
                    last_compacted_l0_sst_view_id: None,
                    last_compacted_l0_sst_id: None,
                    l0: VecDeque::new(),
                    compacted: vec![SortedRun {
                        id: 3,
                        sst_views: vec![SsTableView::identity(SsTableHandle::new(
                            SsTableId::Compacted(Ulid::from_parts(30, 0)),
                            SST_FORMAT_VERSION_LATEST,
                            info.clone(),
                        ))],
                    }],
                },
            },
            Segment {
                prefix: Bytes::from_static(b"b/"),
                tree: LsmTreeState {
                    last_compacted_l0_sst_view_id: None,
                    last_compacted_l0_sst_id: None,
                    l0: VecDeque::new(),
                    compacted: vec![
                        SortedRun {
                            id: 9,
                            sst_views: vec![SsTableView::identity(SsTableHandle::new(
                                SsTableId::Compacted(Ulid::from_parts(90, 0)),
                                SST_FORMAT_VERSION_LATEST,
                                info.clone(),
                            ))],
                        },
                        SortedRun {
                            id: 6,
                            sst_views: vec![SsTableView::identity(SsTableHandle::new(
                                SsTableId::Compacted(Ulid::from_parts(60, 0)),
                                SST_FORMAT_VERSION_LATEST,
                                info,
                            ))],
                        },
                    ],
                },
            },
        ];
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(core)),
        };

        let planned = scheduler
            .generate(&state, &CompactionRequest::Full)
            .unwrap();

        assert_eq!(planned.len(), 3);
        assert_eq!(planned[0].segment().as_ref(), b"");
        assert_eq!(
            planned[0].sources(),
            &vec![SourceId::SortedRun(8), SourceId::SortedRun(4)]
        );
        assert_eq!(planned[0].destination(), Some(4));
        assert_eq!(planned[1].segment().as_ref(), b"a/");
        assert_eq!(planned[1].sources(), &vec![SourceId::SortedRun(3)]);
        assert_eq!(planned[1].destination(), Some(3));
        assert_eq!(planned[2].segment().as_ref(), b"b/");
        assert_eq!(
            planned[2].sources(),
            &vec![SourceId::SortedRun(9), SourceId::SortedRun(6)]
        );
        assert_eq!(planned[2].destination(), Some(6));
    }

    #[test]
    fn test_plan_full_skips_trees_without_sorted_runs() {
        // Best-effort semantics: a tree with no compacted SRs is silently
        // skipped (even if it has L0 SSTs), but eligible peers still produce
        // specs. This is the key behavioral difference from FullSegment,
        // which errors when explicitly asked to compact such a tree.
        let scheduler = MockScheduler::new();
        let mut core = ManifestCore::new();
        let info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"x")),
            ..SsTableInfo::default()
        };
        core.tree.compacted = vec![SortedRun {
            id: 5,
            sst_views: vec![SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(Ulid::from_parts(50, 0)),
                SST_FORMAT_VERSION_LATEST,
                info.clone(),
            ))],
        }];
        core.segment_extractor_name = Some("test".into());
        core.segments = vec![
            // L0-only: still skipped because L0 SSTs are ineligible inputs.
            Segment {
                prefix: Bytes::from_static(b"l0only/"),
                tree: LsmTreeState {
                    last_compacted_l0_sst_view_id: None,
                    last_compacted_l0_sst_id: None,
                    l0: VecDeque::from(vec![SsTableView::identity(SsTableHandle::new(
                        SsTableId::Compacted(Ulid::from_parts(1, 0)),
                        SST_FORMAT_VERSION_LATEST,
                        info,
                    ))]),
                    compacted: vec![],
                },
            },
            // Fully empty tree: also skipped.
            Segment {
                prefix: Bytes::from_static(b"none/"),
                tree: LsmTreeState {
                    last_compacted_l0_sst_view_id: None,
                    last_compacted_l0_sst_id: None,
                    l0: VecDeque::new(),
                    compacted: vec![],
                },
            },
        ];
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(core)),
        };

        let planned = scheduler
            .generate(&state, &CompactionRequest::Full)
            .unwrap();

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].segment().as_ref(), b"");
        assert_eq!(planned[0].sources(), &vec![SourceId::SortedRun(5)]);
    }

    #[test]
    fn test_plan_full_returns_empty_when_no_tree_is_eligible() {
        // Empty DB / no compacted SRs anywhere → Ok(vec![]), not an error.
        let scheduler = MockScheduler::new();
        let state = CompactorStateView {
            compactions: None,
            manifest: VersionedManifest::from_manifest(0, Manifest::initial(ManifestCore::new())),
        };

        let planned = scheduler
            .generate(&state, &CompactionRequest::Full)
            .unwrap();

        assert!(planned.is_empty(), "expected empty plan, got {:?}", planned);
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
        real_executor_rx: async_channel::Receiver<CompactorMessage>,
        test_recorder: Arc<slatedb_common::metrics::DefaultMetricsRecorder>,
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
            let (real_executor_tx, real_executor_rx) = async_channel::unbounded();
            let rand = Arc::new(DbRand::default());
            let test_recorder = Arc::new(slatedb_common::metrics::DefaultMetricsRecorder::new());
            let recorder = MetricsRecorderHelper::new(
                test_recorder.clone() as Arc<dyn slatedb_common::metrics::MetricsRecorder>,
                slatedb_common::metrics::MetricLevel::default(),
            );
            let compactor_stats = Arc::new(CompactionStats::new(&recorder));
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
                    merge_operator: None,
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
                test_recorder,
                handler,
            }
        }

        async fn latest_db_state(&mut self) -> ManifestCore {
            self.manifest.refresh().await.unwrap().core.clone()
        }

        async fn write_l0(&mut self) {
            let mut rng = rng::new_test_rng(None);
            let manifest = self.manifest.refresh().await.unwrap();
            let l0s = manifest.core.tree.l0.len();
            // TODO: add an explicit flush_memtable fn to db and use that instead
            let mut k = vec![0u8; self.options.l0_sst_size_bytes];
            rng.fill_bytes(&mut k);
            self.db.put(&k, &[b'x'; 10]).await.unwrap();
            self.db.flush().await.unwrap();
            loop {
                let manifest = self.manifest.refresh().await.unwrap().clone();
                if manifest.core.tree.l0.len() > l0s {
                    break;
                }
            }
        }

        async fn build_l0_compaction(&mut self) -> CompactionSpec {
            let db_state = self.latest_db_state().await;
            let l0_ids_to_compact: Vec<SourceId> = db_state
                .tree
                .l0
                .iter()
                .map(|h| SourceId::SstView(h.id))
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
                Ok(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Ok(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                Err(e) => panic!("channel closed before receiving any message: {e}"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");
        let starting_last_ts =
            slatedb_common::metrics::lookup_metric(&fixture.test_recorder, LAST_COMPACTION_TS_SEC)
                .expect("metric not found");

        // when:
        fixture
            .handler
            .handle(msg)
            .await
            .expect("fatal error handling compaction message");

        // then:
        let last_ts =
            slatedb_common::metrics::lookup_metric(&fixture.test_recorder, LAST_COMPACTION_TS_SEC)
                .expect("metric not found");
        assert!(last_ts > starting_last_ts);
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
                Ok(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Ok(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                Err(e) => panic!("channel closed before receiving any message: {e}"),
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
        assert_eq!(db_state.tree.l0.len(), 1);
        assert_eq!(db_state.tree.compacted.len(), 1);
        let l0_id = db_state
            .tree
            .l0
            .front()
            .unwrap()
            .sst
            .id
            .unwrap_compacted_id();
        let compacted_l0s: Vec<Ulid> = db_state
            .tree
            .compacted
            .first()
            .unwrap()
            .sst_views
            .iter()
            .map(|view| view.sst.id.unwrap_compacted_id())
            .collect();
        assert!(!compacted_l0s.contains(&l0_id));
        assert_eq!(
            db_state.tree.last_compacted_l0_sst_view_id.unwrap(),
            compaction
                .sources()
                .first()
                .and_then(|id| id.maybe_unwrap_sst_view())
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

        let stored_compactions = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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
                Ok(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Ok(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                Err(e) => panic!("channel closed before receiving any message: {e}"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");

        // then: finishing compaction clears persisted state
        fixture.handler.handle(msg).await.unwrap();
        let stored_compactions = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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

        let stored_compactions = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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
        // Refresh so add_compaction sees the just-flushed L0; the source-
        // isolation check needs the spec's source view to be present in the
        // compactor's local tree.
        fixture.handler.state_writer.refresh().await.unwrap();
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction);

        fixture.handler.maybe_schedule_compactions().await.unwrap();

        assert_eq!(fixture.executor.pop_jobs().len(), 0);
        let mut compactions = fixture.handler.state().active_compactions();
        let scheduled = compactions.next().expect("missing compaction");
        assert_eq!(scheduled.status(), CompactionStatus::Submitted);
        assert!(compactions.next().is_none());

        let stored_compactions = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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
        let stored_compactions = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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
        let spec_alt = CompactionSpec::new(
            spec.sources().to_vec(),
            spec.destination().expect("tiered spec") + 1,
        );
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
        let stored_compactions = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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
        let l0s = manifest.core.tree.l0.len();
        let mut k = vec![0u8; options.l0_sst_size_bytes];
        rng.fill_bytes(&mut k);
        db.put(&k, &[b'x'; 10]).await.unwrap();
        db.flush().await.unwrap();
        loop {
            let manifest = stored_manifest.refresh().await.unwrap().clone();
            if manifest.core.tree.l0.len() > l0s {
                break;
            }
        }

        // Seed the compactions store with a Submitted compaction before handler startup.
        let db_state = stored_manifest.refresh().await.unwrap().core.clone();
        let sources = db_state
            .tree
            .l0
            .iter()
            .map(|h| SourceId::SstView(h.id))
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
        let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
        let compactor_stats = Arc::new(CompactionStats::new(&recorder));
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
            sst_views: db_state.tree.l0.iter().cloned().collect(),
        };
        let msg = CompactorMessage::CompactionJobFinished {
            id: compaction_id,
            result: Ok(output_sr),
        };
        handler.handle(msg).await.unwrap();

        let stored_compactions = compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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
        let stored = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
        assert_eq!(stored.iter().collect::<Vec<&Compaction>>().len(), 1);

        // when: job fails
        let msg = CompactorMessage::CompactionJobFinished {
            id: job.id,
            result: Err(SlateDBError::InvalidDBState),
        };
        fixture.handler.handle(msg).await.unwrap();

        // then: compaction marked failed in store
        let stored_after = fixture
            .compactions_store
            .read_latest_compactions()
            .await
            .unwrap()
            .compactions;
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
            id: compaction.destination().expect("tiered spec"),
            sst_views: db_state.tree.l0.iter().cloned().collect(),
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
                Ok(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Ok(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                Err(e) => panic!("channel closed before receiving any message: {e}"),
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
            .tree
            .l0
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .collect();
        assert_eq!(&l0_ids, compaction.sources());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(feature = "zstd")]
    async fn test_compactor_compressed_block_size() {
        use crate::compactor::stats::BYTES_COMPACTED;
        use crate::config::{CompressionCodec, SstBlockSize};
        use slatedb_common::metrics::{lookup_metric, DefaultMetricsRecorder};

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

        let metrics_recorder = Arc::new(DefaultMetricsRecorder::new());
        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_system_clock(system_clock.clone())
            .with_sst_block_size(SstBlockSize::Other(128))
            .with_metrics_recorder(metrics_recorder.clone())
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
        let bytes_compacted = lookup_metric(&metrics_recorder, BYTES_COMPACTED).unwrap();

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
        let c = CompactionSpec::new(vec![SourceId::SstView(Ulid::new())], 0);
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
                Ok(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Ok(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                Err(e) => panic!("channel closed before receiving any message: {e}"),
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

    /// The L0-only monotonicity check is global across all segment trees, not
    /// just the target tree. A spec whose destination is above the target
    /// tree's local max but below the global max (an SR in some other
    /// segment) must be rejected.
    #[tokio::test]
    async fn test_validate_compaction_l0_only_rejects_when_dest_below_global_highest_sr() {
        use crate::manifest::{LsmTreeState, Segment};

        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        let prefix = Bytes::from_static(b"seg/");
        let l0_view = Ulid::from_parts(1, 0);
        let make_view = |id: Ulid| {
            SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(id),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo::default(),
            ))
        };
        let core = &mut fixture
            .handler
            .state_writer
            .state
            .manifest_mut_for_test()
            .value
            .core;
        // Root tree holds SR(7) — the global max. The segment-targeted spec
        // below proposes dst=3, which is above the segment's local max (0)
        // but below the global max.
        core.tree.compacted = vec![SortedRun {
            id: 7,
            sst_views: Vec::new(),
        }];
        core.segments = vec![Segment {
            prefix: prefix.clone(),
            tree: LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: VecDeque::from(vec![make_view(l0_view)]),
                compacted: Vec::new(),
            },
        }];

        let spec = CompactionSpec::for_segment(prefix, vec![SourceId::SstView(l0_view)], 3);
        let err = fixture.handler.validate_compaction(&spec).unwrap_err();
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
                Ok(m @ CompactorMessage::CompactionJobFinished { .. }) => m,
                Ok(_) => fixture
                    .real_executor_rx
                    .recv()
                    .await
                    .expect("channel closed before CompactionJobAttemptFinished"),
                Err(e) => panic!("channel closed before receiving any message: {e}"),
            }
        })
        .await
        .expect("timeout waiting for CompactionJobAttemptFinished");
        fixture.handler.handle(msg).await.unwrap();

        // prepare a mixed compaction: one SR source and one L0 source
        fixture.write_l0().await;
        fixture.handler.handle_ticker().await.unwrap();
        let state = fixture.latest_db_state().await;
        let sr_id = state.tree.compacted.first().unwrap().id;
        let l0_view_id = state.tree.l0.front().unwrap().id;
        let mixed = CompactionSpec::new(
            vec![SourceId::SortedRun(sr_id), SourceId::SstView(l0_view_id)],
            sr_id,
        );
        // Compactor-level validation should not reject (scheduler default validate returns Ok(()))
        fixture.handler.validate_compaction(&mixed).unwrap();
    }

    #[tokio::test]
    async fn test_validate_compaction_rejects_parallel_l0() {
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        // write two L0s so we can build two disjoint L0 compactions
        fixture.write_l0().await;
        fixture.write_l0().await;
        fixture.handler.handle_ticker().await.unwrap();

        let state = fixture.latest_db_state().await;
        assert!(state.tree.l0.len() >= 2);

        // Build first L0 compaction from the oldest L0
        let first_l0 =
            CompactionSpec::new(vec![SourceId::SstView(state.tree.l0.back().unwrap().id)], 0);
        // Inject and schedule it so it becomes active
        fixture.scheduler.inject_compaction(first_l0.clone());
        fixture.handler.handle_ticker().await.unwrap();

        // Build second L0 compaction from the newest L0 (disjoint sources)
        let second_l0 = CompactionSpec::new(
            vec![SourceId::SstView(state.tree.l0.front().unwrap().id)],
            1,
        );
        let err = fixture.handler.validate_compaction(&second_l0).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    /// L0 compactions in disjoint segments may run concurrently — each segment
    /// owns its own watermark, so cross-segment parallelism is safe (RFC-0024).
    /// The parallel-L0 rejection in `validate_compaction` must scope itself to
    /// the spec's target segment.
    ///
    /// This test inspects `validate_compaction` directly by seeding the
    /// compactor's local state with two segments and a Running spec — going
    /// through the full schedule/start round-trip would require a writer
    /// configured with a segment extractor, which the fixture doesn't model.
    #[tokio::test]
    async fn test_validate_compaction_allows_parallel_l0_in_disjoint_segments() {
        use crate::manifest::{LsmTreeState, Segment};
        use bytes::Bytes;

        let mut fixture = CompactorEventHandlerTestFixture::new().await;

        let prefix_a = Bytes::from_static(b"seg-a/");
        let prefix_b = Bytes::from_static(b"seg-b/");
        let l0_a = Ulid::from_parts(1, 0);
        let l0_b = Ulid::from_parts(2, 0);
        let make_segment = |prefix: Bytes, l0_view_id: Ulid| Segment {
            prefix,
            tree: LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: VecDeque::from(vec![SsTableView::identity(SsTableHandle::new(
                    SsTableId::Compacted(l0_view_id),
                    SST_FORMAT_VERSION_LATEST,
                    SsTableInfo::default(),
                ))]),
                compacted: Vec::new(),
            },
        };

        // Seed the compactor's local state with the two segments. We never
        // call handle_ticker again, so the writer/compactor merge protocol
        // (which would reject compactor-only segments with data) doesn't
        // run.
        fixture
            .handler
            .state_writer
            .state
            .manifest_mut_for_test()
            .value
            .core
            .segments = vec![
            make_segment(prefix_a.clone(), l0_a),
            make_segment(prefix_b.clone(), l0_b),
        ];

        // Seed a Running L0 compaction in segment A.
        let running_id = Ulid::from_parts(100, 0);
        let running_spec =
            CompactionSpec::for_segment(prefix_a.clone(), vec![SourceId::SstView(l0_a)], 200);
        fixture
            .handler
            .state_writer
            .state
            .insert_compaction_for_test(
                Compaction::new(running_id, running_spec).with_status(CompactionStatus::Running),
            );

        // L0 in a different segment must validate successfully — its
        // watermark is independent of segment A's.
        let spec_b =
            CompactionSpec::for_segment(prefix_b.clone(), vec![SourceId::SstView(l0_b)], 201);
        fixture
            .handler
            .validate_compaction(&spec_b)
            .expect("L0 in disjoint segment must be allowed");

        // Sanity check: a second L0 spec in the SAME segment is still rejected.
        let spec_a_dup =
            CompactionSpec::for_segment(prefix_a.clone(), vec![SourceId::SstView(l0_a)], 202);
        let err = fixture
            .handler
            .validate_compaction(&spec_a_dup)
            .unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    /// `validate_compaction` rejects a spec whose target segment does not exist in
    /// the manifest. The empty-prefix segment (root tree) always exists, so only
    /// non-empty prefixes can fail this lookup.
    #[tokio::test]
    async fn test_validate_compaction_rejects_unknown_segment() {
        let fixture = CompactorEventHandlerTestFixture::new().await;
        let spec = CompactionSpec::for_segment(
            Bytes::from_static(b"missing/"),
            vec![SourceId::SortedRun(0)],
            0,
        );
        let err = fixture.handler.validate_compaction(&spec).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    /// RFC-0024: a compaction must operate within a single segment. A spec whose
    /// sources reference a sorted run that lives in a different tree is rejected
    /// because the source is absent from the target segment's tree.
    #[tokio::test]
    async fn test_validate_compaction_rejects_sources_from_other_segment() {
        use crate::manifest::{LsmTreeState, Segment};

        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        // SR(99) lives in the root tree; segment "seg/" is empty.
        let core = &mut fixture
            .handler
            .state_writer
            .state
            .manifest_mut_for_test()
            .value
            .core;
        core.tree.compacted = vec![SortedRun {
            id: 99,
            sst_views: Vec::new(),
        }];
        let prefix = Bytes::from_static(b"seg/");
        core.segments = vec![Segment {
            prefix: prefix.clone(),
            tree: LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: VecDeque::new(),
                compacted: Vec::new(),
            },
        }];

        // Segment-targeted spec lists SR(99) as a source — but SR(99) lives in
        // the root tree, not in "seg/". Must be rejected.
        let spec = CompactionSpec::for_segment(prefix, vec![SourceId::SortedRun(99)], 0);
        let err = fixture.handler.validate_compaction(&spec).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    /// SR ids are globally unique across all trees (RFC-0024), so a segment-targeted
    /// spec must be rejected if its destination already exists as an SR anywhere in
    /// the manifest — including the root tree — and the SR is not listed among its
    /// sources.
    #[tokio::test]
    async fn test_validate_compaction_destination_overwrite_check_is_global() {
        use crate::manifest::{LsmTreeState, Segment};

        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        let core = &mut fixture
            .handler
            .state_writer
            .state
            .manifest_mut_for_test()
            .value
            .core;
        // Place SR(7) in the root tree. The segment-targeted spec below uses 7 as
        // its destination but does not list it among its sources.
        core.tree.compacted = vec![SortedRun {
            id: 7,
            sst_views: Vec::new(),
        }];
        // Seed SR(99) into the segment so the source-existence check passes and
        // destination-overwrite is the rejection reason.
        let prefix = Bytes::from_static(b"seg/");
        core.segments = vec![Segment {
            prefix: prefix.clone(),
            tree: LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: VecDeque::new(),
                compacted: vec![SortedRun {
                    id: 99,
                    sst_views: Vec::new(),
                }],
            },
        }];

        let spec = CompactionSpec::for_segment(prefix, vec![SourceId::SortedRun(99)], 7);
        let err = fixture.handler.validate_compaction(&spec).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    /// Drain specs cannot target the empty-prefix segment (RFC-0024): the
    /// root-tree compatibility encoding is not retired by drain.
    #[tokio::test]
    async fn test_validate_compaction_rejects_drain_for_empty_prefix() {
        let fixture = CompactorEventHandlerTestFixture::new().await;
        let spec = CompactionSpec::drain_segment(Bytes::new(), vec![SourceId::SortedRun(0)]);
        let err = fixture.handler.validate_compaction(&spec).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    /// A drain spec must list every L0 at or below the newest drained L0 in
    /// the target tree — otherwise the watermark advances over an un-listed
    /// L0 and leaves it orphaned in `tree.l0` below the watermark.
    #[tokio::test]
    async fn test_validate_compaction_rejects_drain_with_non_contiguous_l0_sources() {
        use crate::manifest::{LsmTreeState, Segment};

        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        let prefix = Bytes::from_static(b"seg/");
        // tree.l0 is newest-first: [L0_3, L0_2, L0_1]. Drain spec names
        // L0_3 and L0_1 but skips L0_2 — the watermark would advance to L0_3
        // and L0_2 would be left below it.
        let l0_3 = Ulid::from_parts(3, 0);
        let l0_2 = Ulid::from_parts(2, 0);
        let l0_1 = Ulid::from_parts(1, 0);
        let make_view = |id: Ulid| {
            SsTableView::identity(SsTableHandle::new(
                SsTableId::Compacted(id),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo::default(),
            ))
        };
        fixture
            .handler
            .state_writer
            .state
            .manifest_mut_for_test()
            .value
            .core
            .segments = vec![Segment {
            prefix: prefix.clone(),
            tree: LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: VecDeque::from(vec![make_view(l0_3), make_view(l0_2), make_view(l0_1)]),
                compacted: Vec::new(),
            },
        }];

        let spec = CompactionSpec::drain_segment(
            prefix,
            vec![SourceId::SstView(l0_3), SourceId::SstView(l0_1)],
        );
        let err = fixture.handler.validate_compaction(&spec).unwrap_err();
        assert!(matches!(err, SlateDBError::InvalidCompaction));
    }

    async fn put_and_flush_memtable(db: &Db, key: &[u8], value: &[u8]) {
        db.put_with_options(
            key,
            value,
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }

    fn read_db_state_core(db: &Db) -> ManifestCore {
        let db_state = db.inner.state.read();
        db_state.state().core().clone()
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

            let empty_l0 = core_db_state.tree.l0.is_empty();
            let compaction_ran = !core_db_state.tree.compacted.is_empty();
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
            if !db_state.tree.compacted.eq(&old_compacted) {
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
        system_clock: Arc<dyn SystemClock>,
    ) -> CompactorBuilder<&'static str> {
        CompactorBuilder::new(PATH, os)
            .with_system_clock(system_clock)
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
