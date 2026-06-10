use std::collections::BTreeMap;
use std::mem;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use bytes::Bytes;
use chrono::TimeDelta;
use futures::future::{join, join_all};
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;

use crate::bytes_range::BytesRange;
#[cfg(feature = "compaction_filters")]
use crate::compaction_filter::CompactionFilterSupplier;
#[cfg(feature = "compaction_filters")]
use crate::compaction_filter_iterator::CompactionFilterIterator;
use crate::compactor::CompactorMessage;
use crate::compactor_state::{CompactionStatus, Subcompaction};
use crate::config::CompactorOptions;
use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableView};
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, RowEntryIterator, TrackedRowEntryIterator};
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::merge_iterator::MergeIterator;
use crate::merge_operator::{
    instrument_merge_operator, MergeOperatorIterator, MergeOperatorRequiredIterator,
    MergeOperatorType,
};
use crate::peeking_iterator::PeekingIterator;
use crate::rand::DbRand;
use crate::retention_iterator::RetentionIterator;
use crate::seq_tracker::SequenceTracker;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::subcompaction::plan_subcompaction_ranges;
use crate::tablestore::TableStore;
use slatedb_common::clock::SystemClock;

use crate::compactor::stats::CompactionStats;
use crate::utils::{
    build_concurrent, compute_max_parallel, estimate_bytes_before_key, last_written_key_and_seq,
    spawn_bg_task, IdGenerator,
};
use log::{debug, error};
use tracing::instrument;
use ulid::Ulid;

/// Arguments for starting a compaction job.
///
/// - `id` is the job id (ULID) and uniquely identifies a single job. This is
///   used as the runtime key in `scheduled_compactions`.
/// - `job_id` is the canonical plan id (ULID) that ties this job back to its
///   `Compaction`
///
/// Jobs carry fully materialized inputs (L0 `ssts` and `sorted_runs`) along with execution-time
/// metadata for progress reporting, retention, and resume logic.
#[derive(Clone, PartialEq)]
pub(crate) struct StartCompactionJobArgs {
    /// Job id. Unique per job.
    pub(crate) id: Ulid,
    /// Canonical compaction job id this job belongs to.
    pub(crate) compaction_id: Ulid,
    /// Destination sorted run id to be produced by this job.
    pub(crate) destination: u32,
    /// Input L0 SSTs for this job.
    pub(crate) sst_views: Vec<SsTableView>,
    /// Input existing sorted runs for this job.
    pub(crate) sorted_runs: Vec<SortedRun>,
    /// Output SSTs already written for this compaction when resuming.
    pub(crate) output_ssts: Vec<SsTableHandle>,
    /// The clock tick representing the time the compaction occurs. This is used
    /// to make decisions about retention of expiring records.
    pub(crate) compaction_clock_tick: i64,
    /// Whether the destination sorted run is the last (newest) run after compaction.
    pub(crate) is_dest_last_run: bool,
    /// Optional minimum sequence to retain; lower sequences may be dropped by retention.
    pub(crate) retention_min_seq: Option<u64>,
    /// Subcompactions persisted for this compaction when resuming (RFC-0028).
    /// Empty when the compaction has no persisted subcompaction plan; the
    /// executor then plans one itself (or runs a single merge).
    pub(crate) subcompactions: Vec<Subcompaction>,
}

impl std::fmt::Debug for StartCompactionJobArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartCompactionJobArgs")
            .field("id", &self.id)
            .field("job_id", &self.compaction_id)
            .field("destination", &self.destination)
            .field("ssts", &self.sst_views)
            .field("sorted_runs", &self.sorted_runs)
            .field("output_ssts", &self.output_ssts)
            .field("compaction_clock_tick", &self.compaction_clock_tick)
            .field("is_dest_last_run", &self.is_dest_last_run)
            .field("retention_min_seq", &self.retention_min_seq)
            .field("subcompactions", &self.subcompactions)
            .finish()
    }
}

/// Iterator adapter that can resume after a persisted compaction output SST.
struct ResumingIterator<T: RowEntryIterator> {
    iterator: PeekingIterator<T>,
    start: Option<(Bytes, u64)>,
}

impl<T: RowEntryIterator> ResumingIterator<T> {
    /// Create a new resuming iterator that wraps the provided iterator. The iterator
    /// must be initialized prior to calling this method.
    ///
    /// This method first seeks to the specified `key` using the underlying iterator's
    /// `seek` method. It then enters a loop where it peeks at the next entry without
    /// advancing the iterator. If the peeked entry's key does not match the specified
    /// `key`, the loop breaks, as we have moved past the desired key. If the keys match,
    /// it checks the sequence number of the entry. Since entries for the same key are
    /// sorted in descending order by sequence, if the entry's sequence is less than the
    /// specified `seq`, the loop breaks, indicating we have found the position to resume.
    ///
    /// ## Arguments
    /// - `iterator`: the iterator to wrap.
    /// - `resume_cursor`: a (key, seq) tuple. when present, seeks to the key and drains
    ///   entries for that key until the iterator is positioned after the last written sequence.
    ///
    /// ## Returns
    /// - `ResumingIterator<T>`: the wrapper around `iterator`.
    /// - `SlateDBError::IteratorNotInitialized` if the iterator has not been initialized.
    /// - `SlateDBError`: any error returned by the underlying iterator.
    async fn new(iterator: T, resume_cursor: Option<(Bytes, u64)>) -> Result<Self, SlateDBError> {
        let mut resuming_iter = Self {
            iterator: PeekingIterator::new(iterator),
            start: resume_cursor.clone(),
        };
        if let Some((key, seq)) = resume_cursor {
            resuming_iter.iterator.seek(key.as_ref()).await?;
            loop {
                let Some(entry) = resuming_iter.iterator.peek().await? else {
                    break;
                };
                if entry.key.as_ref() != key.as_ref() {
                    break;
                }
                if entry.seq < seq {
                    break;
                }
                resuming_iter.iterator.next().await?;
            }
        }
        Ok(resuming_iter)
    }

    /// Returns the resume start cursor, if any. This is the (key, seq) tuple
    /// used to initialize the iterator.
    fn start(&self) -> Option<&(Bytes, u64)> {
        self.start.as_ref()
    }
}

#[async_trait::async_trait]
impl<T: RowEntryIterator> RowEntryIterator for ResumingIterator<T> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.iterator.init().await
    }

    async fn next(&mut self) -> Result<Option<crate::types::RowEntry>, SlateDBError> {
        self.iterator.next().await
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.iterator.seek(next_key).await
    }
}

impl<T: TrackedRowEntryIterator> TrackedRowEntryIterator for ResumingIterator<T> {
    fn bytes_processed(&self) -> u64 {
        self.iterator.bytes_processed()
    }
}

/// Executes compaction jobs produced by the compactor.
pub(crate) trait CompactionExecutor {
    /// Starts executing a compaction job asynchronously.
    fn start_compaction_job(&self, compaction: StartCompactionJobArgs);

    /// Stops the executor and cancels any in-flight tasks, waiting for them to finish.
    fn stop(&self);

    /// Returns true if the executor has been stopped (but not necessarily finished).
    fn is_stopped(&self) -> bool;
}

/// Implemented by message types that the executor sends on job completion and progress.
pub(crate) trait ExecutorMessage: Send + 'static {
    fn job_finished(id: Ulid, result: Result<SortedRun, SlateDBError>) -> Self;
    fn job_progress(
        id: Ulid,
        bytes_processed: u64,
        output_ssts: Vec<SsTableHandle>,
        subcompactions: Vec<Subcompaction>,
    ) -> Self;
}

impl ExecutorMessage for CompactorMessage {
    fn job_finished(id: Ulid, result: Result<SortedRun, SlateDBError>) -> Self {
        CompactorMessage::CompactionJobFinished { id, result }
    }

    fn job_progress(
        id: Ulid,
        bytes_processed: u64,
        output_ssts: Vec<SsTableHandle>,
        subcompactions: Vec<Subcompaction>,
    ) -> Self {
        CompactorMessage::CompactionJobProgress {
            id,
            bytes_processed,
            output_ssts,
            subcompactions,
        }
    }
}

/// Options for creating a [`TokioCompactionExecutor`].
pub(crate) struct TokioCompactionExecutorOptions<M = CompactorMessage> {
    pub handle: tokio::runtime::Handle,
    pub options: Arc<CompactorOptions>,
    pub worker_tx: async_channel::Sender<M>,
    pub table_store: Arc<TableStore>,
    pub rand: Arc<DbRand>,
    pub stats: Arc<CompactionStats>,
    pub clock: Arc<dyn SystemClock>,
    pub manifest_store: Arc<ManifestStore>,
    pub merge_operator: Option<MergeOperatorType>,
    #[cfg(feature = "compaction_filters")]
    pub compaction_filter_supplier: Option<Arc<dyn CompactionFilterSupplier>>,
}

pub(crate) struct TokioCompactionExecutor<M = CompactorMessage> {
    inner: Arc<TokioCompactionExecutorInner<M>>,
}

impl<M: ExecutorMessage> TokioCompactionExecutor<M> {
    pub(crate) fn new(opts: TokioCompactionExecutorOptions<M>) -> Self {
        let stats = opts.stats;
        let merge_operator = opts.merge_operator.map(|merge_operator| {
            instrument_merge_operator(
                merge_operator,
                stats.merge_operator_compact_operands.clone(),
            )
        });

        Self {
            inner: Arc::new(TokioCompactionExecutorInner {
                options: opts.options,
                handle: opts.handle,
                worker_tx: opts.worker_tx,
                table_store: opts.table_store,
                rand: opts.rand,
                tasks: Arc::new(Mutex::new(BTreeMap::new())),
                stats,
                clock: opts.clock,
                is_stopped: AtomicBool::new(false),
                manifest_store: opts.manifest_store,
                merge_operator,
                #[cfg(feature = "compaction_filters")]
                compaction_filter_supplier: opts.compaction_filter_supplier,
            }),
        }
    }
}

impl<M: ExecutorMessage> CompactionExecutor for TokioCompactionExecutor<M> {
    fn start_compaction_job(&self, compaction: StartCompactionJobArgs) {
        self.inner.start_compaction_job(compaction);
    }

    fn stop(&self) {
        self.inner.stop()
    }

    fn is_stopped(&self) -> bool {
        self.inner.is_stopped()
    }
}

struct TokioCompactionTask {
    task: JoinHandle<Result<SortedRun, SlateDBError>>,
}

/// Balances the `running_subcompactions` gauge across every subcompaction
/// task outcome. The guard is owned by the task future, so the decrement
/// runs when the future is dropped — whether it ran to completion, panicked,
/// or was aborted before (or while) being polled.
struct RunningSubcompactionGuard {
    stats: Arc<CompactionStats>,
}

impl RunningSubcompactionGuard {
    fn new(stats: Arc<CompactionStats>) -> Self {
        stats.running_subcompactions.increment(1);
        Self { stats }
    }
}

impl Drop for RunningSubcompactionGuard {
    fn drop(&mut self) {
        self.stats.running_subcompactions.increment(-1);
    }
}

/// Progress and completion events sent from subcompaction tasks to their
/// parent compaction job (RFC-0028). `index` identifies the subcompaction
/// within the parent's plan.
enum SubcompactionEvent {
    Progress {
        index: usize,
        bytes_processed: u64,
        output_ssts: Vec<SsTableHandle>,
    },
    Finished {
        index: usize,
        result: Result<Vec<SsTableHandle>, SlateDBError>,
    },
}

pub(crate) struct TokioCompactionExecutorInner<M = CompactorMessage> {
    options: Arc<CompactorOptions>,
    handle: tokio::runtime::Handle,
    worker_tx: async_channel::Sender<M>,
    table_store: Arc<TableStore>,
    tasks: Arc<Mutex<BTreeMap<u32, TokioCompactionTask>>>,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    clock: Arc<dyn SystemClock>,
    is_stopped: AtomicBool,
    manifest_store: Arc<ManifestStore>,
    merge_operator: Option<MergeOperatorType>,
    #[cfg(feature = "compaction_filters")]
    compaction_filter_supplier: Option<Arc<dyn CompactionFilterSupplier>>,
}

impl<M: ExecutorMessage> TokioCompactionExecutorInner<M> {
    /// Builds input iterators for all sources (L0 and SR) restricted to `range` and
    /// wraps them with optional merge, retention, and compaction filter logic.
    ///
    /// `output_ssts` are the output SSTs already written by a previous attempt of
    /// this (sub)compaction; the returned iterator is positioned just past the last
    /// entry they contain. `sequence_tracker` is the manifest's sequence tracker,
    /// loaded once per job so concurrent subcompactions don't each re-read the
    /// manifest.
    async fn load_iterators<'a>(
        &self,
        job_args: &'a StartCompactionJobArgs,
        range: &'a BytesRange,
        output_ssts: &[SsTableHandle],
        sequence_tracker: Arc<SequenceTracker>,
    ) -> Result<ResumingIterator<Box<dyn TrackedRowEntryIterator + 'a>>, SlateDBError> {
        let resume_cursor = match output_ssts.last() {
            Some(output_sst) => {
                last_written_key_and_seq(self.table_store.clone(), output_sst).await?
            }
            None => None,
        };
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: self.options.max_fetch_tasks,
            blocks_to_fetch: self
                .table_store
                .bytes_to_blocks(self.options.bytes_to_fetch),
            cache_blocks: false, // don't clobber the cache
            eager_spawn: true,
            order: IterationOrder::Ascending,
            prefix: None,
            filter_context: None,
        };

        let max_parallel = compute_max_parallel(job_args.sst_views.len(), &job_args.sorted_runs, 4);
        // L0 (borrowed)
        let l0_iters_futures = build_concurrent(job_args.sst_views.iter(), max_parallel, |h| {
            let sst_iter_options = sst_iter_options.clone();
            SstIterator::new_borrowed_initialized(
                range.clone(),
                h,
                self.table_store.clone(),
                sst_iter_options,
            )
        });

        // SR (borrowed)
        let slice_range: (Bound<&[u8]>, Bound<&[u8]>) = (
            range.start_bound().map(|b| b.as_ref()),
            range.end_bound().map(|b| b.as_ref()),
        );
        let sr_iters_futures = build_concurrent(job_args.sorted_runs.iter(), max_parallel, |sr| {
            let sst_iter_options = sst_iter_options.clone();
            async move {
                SortedRunIterator::new_borrowed(
                    slice_range,
                    sr,
                    self.table_store.clone(),
                    sst_iter_options,
                )
                .await
                .map(Some)
            }
        });

        let (l0_iters_res, sr_iters_res) = join(l0_iters_futures, sr_iters_futures).await;
        let l0_iters = l0_iters_res?;
        let sr_iters = sr_iters_res?;

        let l0_merge_iter = MergeIterator::new(l0_iters)?.with_dedup(false);
        let sr_merge_iter = MergeIterator::new(sr_iters)?.with_dedup(false);

        let merge_iter = MergeIterator::new([l0_merge_iter, sr_merge_iter])?.with_dedup(false);
        let merge_iter: Box<dyn TrackedRowEntryIterator> =
            if let Some(merge_operator) = self.merge_operator.clone() {
                Box::new(MergeOperatorIterator::new(
                    merge_operator,
                    merge_iter,
                    false,
                    job_args.retention_min_seq,
                ))
            } else {
                Box::new(MergeOperatorRequiredIterator::new(merge_iter))
            };

        let mut retention_iter = RetentionIterator::new(
            merge_iter,
            None,
            job_args.retention_min_seq,
            job_args.is_dest_last_run,
            job_args.compaction_clock_tick,
            self.clock.clone(),
            sequence_tracker,
            Some(self.stats.retention_metrics()),
        )
        .await?;
        retention_iter.init().await?;

        // Apply compaction filter if configured
        #[cfg(feature = "compaction_filters")]
        if let Some(supplier) = &self.compaction_filter_supplier {
            use crate::compaction_filter::CompactionJobContext;
            let context = CompactionJobContext {
                destination: job_args.destination,
                is_dest_last_run: job_args.is_dest_last_run,
                compaction_clock_tick: job_args.compaction_clock_tick,
                retention_min_seq: job_args.retention_min_seq,
            };
            let filter = supplier.create_compaction_filter(&context).await?;
            let filter_iter = CompactionFilterIterator::new(retention_iter, filter);
            let boxed: Box<dyn TrackedRowEntryIterator> = Box::new(filter_iter);
            let resuming_iter = ResumingIterator::new(boxed, resume_cursor).await?;
            return Ok(resuming_iter);
        }

        let boxed: Box<dyn TrackedRowEntryIterator> = Box::new(retention_iter);
        let resuming_iter = ResumingIterator::new(boxed, resume_cursor).await?;
        Ok(resuming_iter)
    }

    fn send_compaction_progress(
        &self,
        id: Ulid,
        bytes_processed: u64,
        output_ssts: &[SsTableHandle],
        subcompactions: Vec<Subcompaction>,
    ) {
        // Allow send() because we are treating the executor like an external
        // component. They can do what they want. If the send fails (e.g., during
        // DB shutdown), we log it and continue with the compaction work.
        #[allow(clippy::disallowed_methods)]
        if let Err(e) = self.worker_tx.try_send(M::job_progress(
            id,
            bytes_processed,
            output_ssts.to_vec(),
            subcompactions,
        )) {
            debug!(
                "failed to send compaction progress (likely DB shutdown) [error={:?}]",
                e
            );
        }
    }

    /// Awaits an in-flight SST close, records its stats, and appends the
    /// resulting handle to `output_ssts`. Handles are appended in spawn order,
    /// which preserves the ascending-key ordering required within a sorted run.
    async fn collect_close(
        &self,
        pending: AbortOnDropHandle<Result<SsTableHandle, SlateDBError>>,
        output_ssts: &mut Vec<SsTableHandle>,
    ) -> Result<(), SlateDBError> {
        let sst = pending.await.map_err(|e| {
            let name = "compactor_sst_close".to_string();
            if e.is_cancelled() {
                SlateDBError::BackgroundTaskCancelled(name)
            } else {
                SlateDBError::BackgroundTaskPanic(name)
            }
        })??;
        self.stats.bytes_compacted.increment(sst.info.filter_offset);
        output_ssts.push(sst);
        Ok(())
    }

    /// Executes a single compaction job and returns the resulting [`SortedRun`].
    ///
    /// ## Steps
    /// - Splits the job into subcompactions when enabled and worthwhile (RFC-0028)
    /// - Streams and merges input keys across all sources (per range)
    /// - Applies merge and retention policies
    /// - Writes output SSTs up to `max_sst_size`, reporting periodic progress
    ///
    /// ## Returns
    /// - The destination [`SortedRun`] with all output SST handles.
    #[instrument(level = "debug", skip_all, fields(id = %args.id))]
    async fn execute_compaction_job(
        self: &Arc<Self>,
        args: StartCompactionJobArgs,
    ) -> Result<SortedRun, SlateDBError> {
        debug!("executing compaction [job_args={:?}]", args);
        // Load the manifest's sequence tracker once per job; every range of a
        // split job shares it rather than re-reading the manifest.
        let stored_manifest =
            StoredManifest::load(self.manifest_store.clone(), self.clock.clone()).await?;
        let sequence_tracker = Arc::new(stored_manifest.db_state().sequence_tracker.clone());

        if let Some(subcompactions) = self.plan_or_resume_subcompactions(&args) {
            return self
                .execute_subcompactions(args, subcompactions, sequence_tracker)
                .await;
        }

        let initial_output_ssts = args.output_ssts.clone();
        let progress = |bytes_processed: u64, output_ssts: &[SsTableHandle]| {
            self.send_compaction_progress(args.id, bytes_processed, output_ssts, Vec::new());
        };
        let output_ssts = self
            .run_subcompaction_merge(
                &args,
                &BytesRange::unbounded(),
                initial_output_ssts,
                sequence_tracker,
                &progress,
            )
            .await?;

        Ok(SortedRun {
            id: args.destination,
            sst_views: output_ssts
                .into_iter()
                .map(|sst| {
                    let id = self.rand.rng().gen_ulid(self.clock.as_ref());
                    SsTableView::new(id, sst)
                })
                .collect(),
        })
    }

    /// Determines whether a job should run with subcompactions (RFC-0028).
    ///
    /// ## Returns
    /// - `Some` with the persisted subcompactions when the job is resuming a
    ///   subcompaction plan. The persisted plan is reused verbatim — never
    ///   re-planned — so output SSTs recorded against its ranges stay valid.
    /// - `Some` with a freshly planned set of subcompactions when the job is
    ///   starting fresh, subcompactions are enabled, and the planner finds a
    ///   worthwhile split.
    /// - `None` when the job should run as a single merge over the full
    ///   keyspace. This includes resuming a job whose previous attempt ran
    ///   (and persisted output) without subcompactions.
    fn plan_or_resume_subcompactions(
        &self,
        args: &StartCompactionJobArgs,
    ) -> Option<Vec<Subcompaction>> {
        if !args.subcompactions.is_empty() {
            return Some(args.subcompactions.clone());
        }
        if self.options.max_subcompactions <= 1 || !args.output_ssts.is_empty() {
            return None;
        }
        let ranges = plan_subcompaction_ranges(
            &args.sst_views,
            &args.sorted_runs,
            self.options.max_subcompactions,
            self.options.min_subcompaction_input_bytes as u64,
        );
        if ranges.len() <= 1 {
            return None;
        }
        Some(ranges.into_iter().map(Subcompaction::new).collect())
    }

    /// Executes a compaction job that is split into subcompactions (RFC-0028),
    /// running every incomplete range concurrently.
    ///
    /// The plan is reported (and therefore persisted by the orchestrator)
    /// before any range output is recorded against it; per-range output SSTs
    /// and statuses are then reported as they advance so an interrupted
    /// compaction can resume at range granularity. On the first range failure
    /// the remaining subcompactions are aborted and the job fails; progress
    /// persisted by then remains resumable after a restart.
    async fn execute_subcompactions(
        self: &Arc<Self>,
        args: StartCompactionJobArgs,
        mut subcompactions: Vec<Subcompaction>,
        sequence_tracker: Arc<SequenceTracker>,
    ) -> Result<SortedRun, SlateDBError> {
        debug!(
            "executing compaction with subcompactions [id={}, subcompactions={}]",
            args.id,
            subcompactions.len()
        );
        // A non-empty `args.subcompactions` means this job resumes a persisted
        // plan rather than starting one fresh.
        let resumed_plan = !args.subcompactions.is_empty();
        if !resumed_plan {
            self.stats
                .subcompactions_per_compaction
                .record(subcompactions.len() as f64);
        }

        // Estimate bytes already processed by completed ranges (resume), so
        // aggregate progress doesn't regress after a restart.
        let mut bytes_processed_by_sub = vec![0u64; subcompactions.len()];
        for (index, subcompaction) in subcompactions.iter().enumerate() {
            if subcompaction.is_complete() {
                bytes_processed_by_sub[index] = subcompaction
                    .output_ssts()
                    .iter()
                    .map(|sst| sst.estimate_size())
                    .sum();
            }
        }

        // Report the plan immediately so it is persisted before any range
        // output is recorded against it.
        self.send_compaction_progress(
            args.id,
            bytes_processed_by_sub.iter().sum(),
            &[],
            subcompactions.clone(),
        );

        let args = Arc::new(args);
        let (sub_tx, mut sub_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sub_tasks = Vec::new();
        for (index, subcompaction) in subcompactions.iter_mut().enumerate() {
            if subcompaction.is_complete() {
                continue;
            }
            if resumed_plan {
                self.stats.subcompactions_resumed.increment(1);
            }
            subcompaction.set_status(CompactionStatus::Running);

            let this = self.clone();
            let args = args.clone();
            let range = subcompaction.range().clone();
            let initial_output_ssts = subcompaction.output_ssts().clone();
            let sequence_tracker = sequence_tracker.clone();
            let event_tx = sub_tx.clone();
            let finished_tx = sub_tx.clone();
            let stats = self.stats.clone();
            let clock = self.clock.clone();
            let started_at = self.clock.now();
            // The guard is moved into the task future so the gauge is
            // decremented even when the task is aborted (e.g. a sibling
            // range failed) and the cleanup fn below never runs.
            let running_guard = RunningSubcompactionGuard::new(self.stats.clone());
            let task = spawn_bg_task(
                format!("subcompaction:{}:{}", args.compaction_id, index),
                &self.handle,
                move |result: &Result<Vec<SsTableHandle>, SlateDBError>| {
                    let elapsed = clock.now().signed_duration_since(started_at);
                    stats
                        .subcompaction_duration_sec
                        .record(elapsed.num_milliseconds() as f64 / 1000.0);
                    // Allow send() because this channel is internal to the
                    // parent job: the receiver is dropped only after the
                    // parent has aborted (or collected) every subcompaction
                    // task, so a failed send needs no handling.
                    #[allow(clippy::disallowed_methods)]
                    let _ = finished_tx.send(SubcompactionEvent::Finished {
                        index,
                        result: result.clone(),
                    });
                },
                async move {
                    let _running_guard = running_guard;
                    let progress = move |bytes_processed: u64, output_ssts: &[SsTableHandle]| {
                        // Allow send() for the same reason as the cleanup fn
                        // above: a dropped receiver means the parent job is
                        // aborting this task anyway.
                        #[allow(clippy::disallowed_methods)]
                        let _ = event_tx.send(SubcompactionEvent::Progress {
                            index,
                            bytes_processed,
                            output_ssts: output_ssts.to_vec(),
                        });
                    };
                    this.run_subcompaction_merge(
                        &args,
                        &range,
                        initial_output_ssts,
                        sequence_tracker,
                        &progress,
                    )
                    .await
                },
            );
            sub_tasks.push(AbortOnDropHandle::new(task));
        }
        drop(sub_tx);

        let mut pending = sub_tasks.len();
        let mut first_error: Option<SlateDBError> = None;
        while pending > 0 {
            let Some(event) = sub_rx.recv().await else {
                break;
            };
            match event {
                SubcompactionEvent::Progress {
                    index,
                    bytes_processed,
                    output_ssts,
                } => {
                    bytes_processed_by_sub[index] = bytes_processed;
                    subcompactions[index].set_output_ssts(output_ssts);
                    let total_bytes = bytes_processed_by_sub.iter().sum();
                    self.send_compaction_progress(
                        args.id,
                        total_bytes,
                        &[],
                        subcompactions.clone(),
                    );
                }
                SubcompactionEvent::Finished { index, result } => {
                    pending -= 1;
                    match result {
                        Ok(output_ssts) => {
                            subcompactions[index].set_output_ssts(output_ssts);
                            subcompactions[index].set_status(CompactionStatus::Completed);
                            let total_bytes = bytes_processed_by_sub.iter().sum();
                            self.send_compaction_progress(
                                args.id,
                                total_bytes,
                                &[],
                                subcompactions.clone(),
                            );
                        }
                        Err(e) => {
                            subcompactions[index].set_status(CompactionStatus::Failed);
                            first_error = Some(e);
                            break;
                        }
                    }
                }
            }
        }
        // Abort any subcompactions still running after a failure, and wait
        // for them to wind down so the job stops writing to the object store
        // before it is reported failed. On success every task has already
        // completed, so this returns immediately.
        sub_tasks.iter().for_each(|task| task.abort());
        let _ = join_all(sub_tasks).await;

        if let Some(e) = first_error {
            return Err(e);
        }
        if subcompactions.iter().any(|sub| !sub.is_complete()) {
            // The event channel closed before every subcompaction reported a
            // result; treat the job as failed so it can be rescheduled.
            return Err(SlateDBError::CompactionExecutorFailed);
        }

        // Subcompactions are in ascending range order and each range's output
        // is internally sorted, so concatenating yields a sorted run.
        Ok(SortedRun {
            id: args.destination,
            sst_views: subcompactions
                .iter()
                .flat_map(|sub| sub.output_ssts().iter())
                .map(|sst| {
                    let id = self.rand.rng().gen_ulid(self.clock.as_ref());
                    SsTableView::new(id, sst.clone())
                })
                .collect(),
        })
    }

    /// Runs the merge for one key range of a compaction job and returns the
    /// output SSTs, including any `initial_output_ssts` written by a previous
    /// attempt. This is the shared execution path for whole compactions (an
    /// unbounded range) and subcompactions (RFC-0028).
    async fn run_subcompaction_merge(
        &self,
        args: &StartCompactionJobArgs,
        range: &BytesRange,
        initial_output_ssts: Vec<SsTableHandle>,
        sequence_tracker: Arc<SequenceTracker>,
        progress: &(dyn Fn(u64, &[SsTableHandle]) + Send + Sync),
    ) -> Result<Vec<SsTableHandle>, SlateDBError> {
        let mut output_ssts = initial_output_ssts;
        let mut all_iter = self
            .load_iterators(args, range, &output_ssts, sequence_tracker)
            .await?;
        let mut current_writer = self.table_store.table_writer(SsTableId::Compacted(
            self.rand.rng().gen_ulid(self.clock.as_ref()),
        ));
        let mut bytes_written = 0usize;
        let mut last_progress_report = self.clock.now();
        // Estimate bytes processed within this range before the resume point,
        // if any. For an unbounded range this is the estimate of everything
        // before the resume key; for a subcompaction range the bytes before
        // the range's start key are excluded since they belong to sibling
        // ranges.
        let start_bytes_processed = all_iter.start().map_or(0, |(k, _s)| {
            let before_key = estimate_bytes_before_key(args.sorted_runs.as_slice(), k);
            let before_range = match range.start_bound() {
                Bound::Included(s) | Bound::Excluded(s) => {
                    estimate_bytes_before_key(args.sorted_runs.as_slice(), s)
                }
                Bound::Unbounded => 0,
            };
            before_key.saturating_sub(before_range)
        });

        // At most one SST close runs in the background at a time (depth-1
        // pipeline). While a finished SST flushes to the object store we keep
        // merging input and filling the next SST, instead of stalling the loop
        // on close(). Only one buffered SST is held outstanding, which bounds
        // memory and applies backpressure when the object store is slow.
        let mut pending_close: Option<AbortOnDropHandle<Result<SsTableHandle, SlateDBError>>> =
            None;

        while let Some(kv) = all_iter.next().await? {
            // Opportunistically collect a finished background close without
            // stalling the merge loop to promptly lets us report progress
            // and persist the newly uploaded output SST in compactor state
            // as soon as the close finishes, rather than waiting until the
            // next SST fills up.
            if let Some(pending) = pending_close.take_if(|p| p.is_finished()) {
                self.collect_close(pending, &mut output_ssts).await?;
                let total_bytes = start_bytes_processed + all_iter.bytes_processed();
                progress(total_bytes, &output_ssts);
                last_progress_report = self.clock.now();
            }

            let duration_since_last_report =
                self.clock.now().signed_duration_since(last_progress_report);
            if duration_since_last_report > TimeDelta::seconds(1) {
                let total_bytes = start_bytes_processed + all_iter.bytes_processed();
                progress(total_bytes, &output_ssts);
                last_progress_report = self.clock.now();
            }

            if let Some(block_size) = current_writer.add(kv).await? {
                bytes_written += block_size;
            }

            if bytes_written > self.options.max_sst_size {
                // Depth-1 backpressure: drain the previous close (collecting its
                // handle in order) before starting another.
                if let Some(pending) = pending_close.take() {
                    self.collect_close(pending, &mut output_ssts).await?;
                }
                let finished_writer = mem::replace(
                    &mut current_writer,
                    self.table_store.table_writer(SsTableId::Compacted(
                        self.rand.rng().gen_ulid(self.clock.as_ref()),
                    )),
                );
                pending_close = Some(AbortOnDropHandle::new(spawn_bg_task(
                    format!("compactor_sst_close:{:?}", finished_writer.id()),
                    &self.handle,
                    |_| {},
                    async move { finished_writer.close().await },
                )));
                bytes_written = 0;
                let total_bytes = start_bytes_processed + all_iter.bytes_processed();
                progress(total_bytes, &output_ssts);
                last_progress_report = self.clock.now();
            }
        }

        // Drain the in-flight close, then flush the final partial SST. Order
        // matters: the pending SST's keys all precede the final writer's keys.
        if let Some(pending) = pending_close.take() {
            self.collect_close(pending, &mut output_ssts).await?;
        }
        if !current_writer.is_drained() {
            let sst = current_writer.close().await?;

            self.stats.bytes_compacted.increment(sst.info.filter_offset);
            output_ssts.push(sst);
        }

        Ok(output_ssts)
    }

    /// Starts a background task to run the compaction job.
    fn start_compaction_job(self: &Arc<Self>, args: StartCompactionJobArgs) {
        let mut tasks = self.tasks.lock();
        if self.is_stopped.load(atomic::Ordering::SeqCst) {
            return;
        }
        let dst = args.destination;
        self.stats.running_compactions.increment(1);
        assert!(!tasks.contains_key(&dst));

        let id = args.id;

        // TODO(sujeetsawala): Add compaction plan to object store with InProgress status

        let this = self.clone();
        let this_cleanup = self.clone();
        let task = spawn_bg_task(
            "compactor_executor".to_string(),
            &self.handle,
            move |result| {
                let result = result.clone();
                {
                    let mut tasks = this_cleanup.tasks.lock();
                    tasks.remove(&dst);
                }
                // Allow send() because we are treating the executor like an external
                // component. They can do what they want. If the send fails (e.g., during
                // DB shutdown), we log it and continue with cleanup.
                #[allow(clippy::disallowed_methods)]
                if let Err(e) = this_cleanup.worker_tx.try_send(M::job_finished(id, result)) {
                    debug!(
                        "failed to send compaction finished msg (likely DB shutdown) [error={:?}]",
                        e
                    );
                }
                this_cleanup.stats.running_compactions.increment(-1);
            },
            async move { this.execute_compaction_job(args).await },
        );
        tasks.insert(dst, TokioCompactionTask { task });
    }

    /// Cancels all active compaction tasks and waits for their termination.
    fn stop(&self) {
        // Drain all tasks and abort them, then release tasks lock so
        // the cleanup function in spawn_bg_task (above) can take the
        // lock and remove the task from the map.
        let task_handles = {
            let mut tasks = self.tasks.lock();
            // BTreeMap keeps abort/join order stable by destination. HashMap
            // iteration order is randomized, which can feed the DST runtime a
            // process-dependent shutdown order.
            let mut task_handles = Vec::with_capacity(tasks.len());
            while let Some((_, task)) = tasks.pop_first() {
                task.task.abort();
                task_handles.push(task.task);
            }
            task_handles
        };

        let wait_for_task_termination = async move {
            let results = join_all(task_handles).await;
            for result in results {
                match result {
                    Err(e) if !e.is_cancelled() => {
                        error!("shutdown error in compaction task [error={:?}]", e);
                    }
                    _ => {}
                }
            }
        };

        #[cfg(dst)]
        self.handle.spawn(wait_for_task_termination);
        #[cfg(not(dst))]
        self.handle.block_on(wait_for_task_termination);
        self.is_stopped.store(true, atomic::Ordering::SeqCst);
    }

    /// Returns true if the executor has been stopped (but not necessarily finished).
    fn is_stopped(&self) -> bool {
        self.is_stopped.load(atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::format::sst::SsTableFormat;
    use crate::manifest::ManifestCore;
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::arbitrary;
    use crate::sst_iter::SstView;
    use crate::test_utils::StringConcatMergeOperator;
    use crate::test_utils::{build_row_entries, build_sorted_runs, write_ssts, GatedObjectStore};
    use crate::types::{RowEntry, ValueDeletable};
    use crate::Db;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use proptest::prelude::Just;
    use proptest::strategy::Strategy;
    use proptest::test_runner::Config;
    use proptest::{prop_assume, prop_oneof, proptest};
    use rstest::rstest;
    use slatedb_common::clock::DefaultSystemClock;
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::time::Duration;

    async fn write_sst(
        table_store: &Arc<TableStore>,
        entries: &[RowEntry],
        max_sst_size: usize,
    ) -> Vec<SsTableHandle> {
        if entries.is_empty() {
            return Vec::new();
        }

        // Write entries into one or more SSTs, splitting on the same block-size
        // accounting used by the compactor's output writer.
        let mut output_ssts = Vec::new();
        let mut writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
        let mut bytes_written = 0usize;

        for (index, entry) in entries.iter().cloned().enumerate() {
            if let Some(block_size) = writer.add(entry).await.unwrap() {
                bytes_written += block_size;
            }

            if bytes_written > max_sst_size {
                output_ssts.push(writer.close().await.unwrap());
                bytes_written = 0;

                if index + 1 < entries.len() {
                    writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
                } else {
                    return output_ssts;
                }
            }
        }

        output_ssts.push(writer.close().await.unwrap());
        output_ssts
    }

    fn has_duplicate_key_seq_specs(
        l0_specs: &[Vec<(Bytes, u64, ValueDeletable)>],
        sr_specs: &[Vec<(Bytes, u64, ValueDeletable)>],
    ) -> bool {
        let mut seen = HashSet::new();
        l0_specs
            .iter()
            .chain(sr_specs.iter())
            .flatten()
            .any(|(key, seq, _value)| !seen.insert((key.clone(), *seq)))
    }

    #[rstest]
    #[case::l0_only(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_value(b"k01", b"k01-101", 101),
                RowEntry::new_value(b"k02", b"k02-102", 102),
                RowEntry::new_value(b"k03", b"k03-30", 30),
                RowEntry::new_value(b"k03", b"k03-20", 20),
                RowEntry::new_value(b"k04", b"k04-104", 104),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k05", b"k05-105", 105),
                RowEntry::new_value(b"k06", b"k06-106", 106),
                RowEntry::new_value(b"k07", b"k07-107", 107),
                RowEntry::new_value(b"k08", b"k08-108", 108),
                RowEntry::new_value(b"k09", b"k09-109", 109),
                RowEntry::new_value(b"k10", b"k10-110", 110),
            ],
        ],
        Vec::new(),
        4096,
        67_108_864,
        Some(3),
        None,
        vec![
            RowEntry::new_value(b"k03", b"k03-20", 20),
            RowEntry::new_value(b"k04", b"k04-104", 104),
            RowEntry::new_value(b"k05", b"k05-105", 105),
            RowEntry::new_value(b"k06", b"k06-106", 106),
            RowEntry::new_value(b"k07", b"k07-107", 107),
            RowEntry::new_value(b"k08", b"k08-108", 108),
            RowEntry::new_value(b"k09", b"k09-109", 109),
            RowEntry::new_value(b"k10", b"k10-110", 110),
        ]
    )]
    #[case::l0_only_multi_block(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_value(b"k01", b"k01-101", 101),
                RowEntry::new_value(b"k02", b"k02-102", 102),
                RowEntry::new_value(b"k03", b"k03-103", 103),
                RowEntry::new_value(b"k04", b"k04-104", 104),
                RowEntry::new_value(b"k05", b"k05-105", 105),
                RowEntry::new_value(b"k06", b"k06-106", 106),
                RowEntry::new_value(b"k07", b"k07-107", 107),
                RowEntry::new_value(b"k08", b"k08-80", 80),
                RowEntry::new_value(b"k08", b"k08-70", 70),
                RowEntry::new_value(b"k09", b"k09-109", 109),
                RowEntry::new_value(b"k10", b"k10-110", 110),
                RowEntry::new_value(b"k11", b"k11-111", 111),
                RowEntry::new_value(b"k12", b"k12-112", 112),
            ],
        ],
        Vec::new(),
        128,
        1,
        Some(9),
        None,
        vec![
            RowEntry::new_value(b"k09", b"k09-109", 109),
            RowEntry::new_value(b"k10", b"k10-110", 110),
            RowEntry::new_value(b"k11", b"k11-111", 111),
            RowEntry::new_value(b"k12", b"k12-112", 112),
        ]
    )]
    #[case::sr_only(
        Vec::new(),
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k00", b"k00-300", 300),
                    RowEntry::new_value(b"k01", b"k01-301", 301),
                    RowEntry::new_value(b"k02", b"k02-302", 302),
                    RowEntry::new_value(b"k03", b"k03-303", 303),
                    RowEntry::new_value(b"k03", b"k03-33", 33),
                    RowEntry::new_value(b"k04", b"k04-304", 304),
                    RowEntry::new_value(b"k05", b"k05-305", 305),
                ],
            ],
            vec![ // sr(1)
                vec![  // sr(1) sst(0)
                    RowEntry::new_value(b"k06", b"k06-406", 406),
                    RowEntry::new_value(b"k07", b"k07-407", 407),
                    RowEntry::new_value(b"k08", b"k08-408", 408),
                    RowEntry::new_value(b"k09", b"k09-409", 409),
                    RowEntry::new_value(b"k10", b"k10-410", 410),
                    RowEntry::new_value(b"k11", b"k11-411", 411),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(4),
        None,
        vec![
            RowEntry::new_value(b"k04", b"k04-304", 304),
            RowEntry::new_value(b"k05", b"k05-305", 305),
            RowEntry::new_value(b"k06", b"k06-406", 406),
            RowEntry::new_value(b"k07", b"k07-407", 407),
            RowEntry::new_value(b"k08", b"k08-408", 408),
            RowEntry::new_value(b"k09", b"k09-409", 409),
            RowEntry::new_value(b"k10", b"k10-410", 410),
            RowEntry::new_value(b"k11", b"k11-411", 411),
        ]
    )]
    #[case::l0_and_sr(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-500", 500),
                RowEntry::new_value(b"k01", b"k01-501", 501),
                RowEntry::new_value(b"k02", b"k02-502", 502),
                RowEntry::new_value(b"k03", b"k03-503", 503),
                RowEntry::new_value(b"k04", b"k04-504", 504),
                RowEntry::new_value(b"k05", b"k05-90", 90),
                RowEntry::new_value(b"k05", b"k05-80", 80),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k06", b"k06-606", 606),
                RowEntry::new_value(b"k07", b"k07-607", 607),
                RowEntry::new_value(b"k08", b"k08-608", 608),
                RowEntry::new_value(b"k09", b"k09-609", 609),
                RowEntry::new_value(b"k10", b"k10-610", 610),
                RowEntry::new_value(b"k11", b"k11-611", 611),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k12", b"k12-720", 720),
                    RowEntry::new_value(b"k13", b"k13-721", 721),
                    RowEntry::new_value(b"k14", b"k14-722", 722),
                    RowEntry::new_value(b"k15", b"k15-120", 120),
                    RowEntry::new_value(b"k15", b"k15-110", 110),
                    RowEntry::new_value(b"k16", b"k16-726", 726),
                    RowEntry::new_value(b"k17", b"k17-727", 727),
                ],
            ],
            vec![ // sr(1)
                vec![ // sr(1) sst(0)
                    RowEntry::new_value(b"k18", b"k18-830", 830),
                    RowEntry::new_value(b"k19", b"k19-831", 831),
                    RowEntry::new_value(b"k20", b"k20-832", 832),
                    RowEntry::new_value(b"k21", b"k21-833", 833),
                    RowEntry::new_value(b"k22", b"k22-834", 834),
                    RowEntry::new_value(b"k23", b"k23-835", 835),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(5),
        None,
        vec![
            RowEntry::new_value(b"k05", b"k05-80", 80),
            RowEntry::new_value(b"k06", b"k06-606", 606),
            RowEntry::new_value(b"k07", b"k07-607", 607),
            RowEntry::new_value(b"k08", b"k08-608", 608),
            RowEntry::new_value(b"k09", b"k09-609", 609),
            RowEntry::new_value(b"k10", b"k10-610", 610),
            RowEntry::new_value(b"k11", b"k11-611", 611),
            RowEntry::new_value(b"k12", b"k12-720", 720),
            RowEntry::new_value(b"k13", b"k13-721", 721),
            RowEntry::new_value(b"k14", b"k14-722", 722),
            RowEntry::new_value(b"k15", b"k15-120", 120),
            RowEntry::new_value(b"k15", b"k15-110", 110),
            RowEntry::new_value(b"k16", b"k16-726", 726),
            RowEntry::new_value(b"k17", b"k17-727", 727),
            RowEntry::new_value(b"k18", b"k18-830", 830),
            RowEntry::new_value(b"k19", b"k19-831", 831),
            RowEntry::new_value(b"k20", b"k20-832", 832),
            RowEntry::new_value(b"k21", b"k21-833", 833),
            RowEntry::new_value(b"k22", b"k22-834", 834),
            RowEntry::new_value(b"k23", b"k23-835", 835),
        ]
    )]
    #[case::l0_and_sr_overlap(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-900", 900),
                RowEntry::new_value(b"k01", b"k01-901", 901),
                RowEntry::new_value(b"k02", b"k02-902", 902),
                RowEntry::new_value(b"k03", b"k03-903", 903),
                RowEntry::new_value(b"k04", b"k04-904", 904),
                RowEntry::new_value(b"k05", b"k05-905", 905),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k02", b"k02-920", 920),
                RowEntry::new_value(b"k03", b"k03-921", 921),
                RowEntry::new_value(b"k04", b"k04-922", 922),
                RowEntry::new_value(b"k06", b"k06-923", 923),
                RowEntry::new_value(b"k07", b"k07-924", 924),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k01", b"k01-800", 800),
                    RowEntry::new_value(b"k03", b"k03-801", 801),
                    RowEntry::new_value(b"k05", b"k05-802", 802),
                    RowEntry::new_value(b"k08", b"k08-803", 803),
                    RowEntry::new_value(b"k09", b"k09-804", 804),
                ],
            ],
            vec![ // sr(1)
                vec![ // sr(1) sst(0)
                    RowEntry::new_value(b"k02", b"k02-850", 850),
                    RowEntry::new_value(b"k04", b"k04-851", 851),
                    RowEntry::new_value(b"k05", b"k05-852", 852),
                    RowEntry::new_value(b"k07", b"k07-853", 853),
                    RowEntry::new_value(b"k10", b"k10-854", 854),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(7),
        None,
        vec![
            RowEntry::new_value(b"k03", b"k03-801", 801),
            RowEntry::new_value(b"k04", b"k04-922", 922),
            RowEntry::new_value(b"k04", b"k04-904", 904),
            RowEntry::new_value(b"k04", b"k04-851", 851),
            RowEntry::new_value(b"k05", b"k05-905", 905),
            RowEntry::new_value(b"k05", b"k05-852", 852),
            RowEntry::new_value(b"k05", b"k05-802", 802),
            RowEntry::new_value(b"k06", b"k06-923", 923),
            RowEntry::new_value(b"k07", b"k07-924", 924),
            RowEntry::new_value(b"k07", b"k07-853", 853),
            RowEntry::new_value(b"k08", b"k08-803", 803),
            RowEntry::new_value(b"k09", b"k09-804", 804),
            RowEntry::new_value(b"k10", b"k10-854", 854),
        ]
    )]
    #[case::tombstones_resume(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_value(b"k01", b"k01-101", 101),
                RowEntry::new_tombstone(b"k02", 105),
                RowEntry::new_value(b"k02", b"k02-103", 103),
                RowEntry::new_value(b"k03", b"k03-106", 106),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k01", b"k01-90", 90),
                    RowEntry::new_value(b"k02", b"k02-95", 95),
                    RowEntry::new_value(b"k04", b"k04-107", 107),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(3),
        None,
        vec![
            RowEntry::new_value(b"k02", b"k02-103", 103),
            RowEntry::new_value(b"k02", b"k02-95", 95),
            RowEntry::new_value(b"k03", b"k03-106", 106),
            RowEntry::new_value(b"k04", b"k04-107", 107),
        ]
    )]
    #[case::no_output_ssts(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-10", 10),
                RowEntry::new_value(b"k02", b"k02-25", 25),
                RowEntry::new_value(b"k02", b"k02-20", 20),
            ],
            vec![ // l0(1)
                RowEntry::new_value(b"k01", b"k01-30", 30),
                RowEntry::new_value(b"k03", b"k03-40", 40),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_value(b"k01", b"k01-15", 15),
                    RowEntry::new_value(b"k02", b"k02-22", 22),
                    RowEntry::new_value(b"k04", b"k04-50", 50),
                ],
            ],
        ],
        4096,
        67_108_864,
        None,
        None,
        vec![
            RowEntry::new_value(b"k00", b"k00-10", 10),
            RowEntry::new_value(b"k01", b"k01-30", 30),
            RowEntry::new_value(b"k01", b"k01-15", 15),
            RowEntry::new_value(b"k02", b"k02-25", 25),
            RowEntry::new_value(b"k02", b"k02-22", 22),
            RowEntry::new_value(b"k02", b"k02-20", 20),
            RowEntry::new_value(b"k03", b"k03-40", 40),
            RowEntry::new_value(b"k04", b"k04-50", 50),
        ]
    )]
    #[case::resume_at_end(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-10", 10),
                RowEntry::new_value(b"k01", b"k01-11", 11),
            ],
        ],
        Vec::new(),
        4096,
        67_108_864,
        Some(1),
        None,
        vec![]
    )]
    #[case::merge_operator_resume(
        vec![
            vec![ // l0(0)
                RowEntry::new_value(b"k00", b"k00-100", 100),
                RowEntry::new_merge(b"k01", b"b", 5),
                RowEntry::new_merge(b"k02", b"y", 6),
                RowEntry::new_value(b"k03", b"k03-103", 103),
            ],
        ],
        vec![
            vec![ // sr(0)
                vec![ // sr(0) sst(0)
                    RowEntry::new_merge(b"k01", b"a", 4),
                    RowEntry::new_value(b"k01", b"base", 3),
                    RowEntry::new_merge(b"k02", b"x", 4),
                    RowEntry::new_value(b"k02", b"base2", 2),
                ],
            ],
        ],
        4096,
        67_108_864,
        Some(0),
        Some(Arc::new(StringConcatMergeOperator {}) as MergeOperatorType),
        vec![
            RowEntry::new_value(b"k01", b"baseab", 5),
            RowEntry::new_value(b"k02", b"base2xy", 6),
            RowEntry::new_value(b"k03", b"k03-103", 103),
        ]
    )]
    #[case::merge_operator_resume_skip_merged_operands(
        vec![
            vec![ // l0(0)
                RowEntry::new_merge(b"k00", b"10", 10),
                RowEntry::new_merge(b"k00", b"4", 4),
                RowEntry::new_merge(b"k00", b"1", 1),
                RowEntry::new_value(b"k01", b"k01-5", 5),
            ],
        ],
        Vec::new(),
        4096,
        67_108_864,
        Some(0),
        Some(Arc::new(StringConcatMergeOperator {}) as MergeOperatorType),
        vec![
            RowEntry::new_value(b"k01", b"k01-5", 5),
        ]
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_iterators_resume(
        #[case] l0_entry_sets: Vec<Vec<RowEntry>>,
        #[case] sr_entry_sets: Vec<Vec<Vec<RowEntry>>>,
        #[case] block_size: usize,
        #[case] output_max_sst_size: usize,
        #[case] resume_index: Option<usize>,
        #[case] merge_operator: Option<MergeOperatorType>,
        #[case] expected_rows: Vec<RowEntry>,
    ) {
        let handle = tokio::runtime::Handle::current();
        let options = Arc::new(CompactorOptions::default());
        let (tx, _rx) = async_channel::unbounded::<CompactorMessage>();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root_path = Path::from("testdb-load-iterators");
        let clock = Arc::new(DefaultSystemClock::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat {
                block_size,
                ..SsTableFormat::default()
            },
            root_path.clone(),
            None,
        ));
        let manifest_store = Arc::new(ManifestStore::new(&root_path, object_store.clone()));
        StoredManifest::create_new_db(manifest_store.clone(), ManifestCore::new(), clock.clone())
            .await
            .unwrap();
        let retention_min_seq = if merge_operator.is_some() {
            None
        } else {
            Some(0)
        };
        let executor = TokioCompactionExecutor::new(TokioCompactionExecutorOptions {
            handle,
            options,
            worker_tx: tx,
            table_store: table_store.clone(),
            rand: Arc::new(DbRand::new(100u64)),
            stats: {
                let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
                Arc::new(CompactionStats::new(&recorder))
            },
            clock: clock.clone(),
            manifest_store,
            merge_operator,
            #[cfg(feature = "compaction_filters")]
            compaction_filter_supplier: None,
        });

        // Materialize L0 SSTs from the provided entry sets. Use a huge max size so
        // each entry set stays in a single SST, keeping the inputs predictable.
        let mut l0_sst_views = Vec::new();
        let mut sorted_runs = Vec::new();
        let mut all_entries = Vec::new();

        for entries in &l0_entry_sets {
            let ssts = write_sst(&table_store, entries, usize::MAX).await;
            l0_sst_views.extend(ssts.into_iter().map(SsTableView::identity));
            all_entries.extend(entries.iter().cloned());
        }

        // Materialize sorted-run SSTs and group them into multiple SortedRun inputs,
        // again keeping each entry set as a single SST by using a huge max size.
        if !sr_entry_sets.is_empty() {
            for (sr_id, sr_sst_sets) in sr_entry_sets.iter().enumerate() {
                let mut sr_ssts = Vec::new();
                for entries in sr_sst_sets {
                    let ssts = write_sst(&table_store, entries, usize::MAX).await;
                    sr_ssts.extend(ssts);
                    all_entries.extend(entries.iter().cloned());
                }
                sorted_runs.push(SortedRun {
                    id: sr_id as u32,
                    sst_views: sr_ssts.into_iter().map(SsTableView::identity).collect(),
                });
            }
        }

        // Sort the merged inputs to match the iterator order (key asc, seq desc).
        let mut sorted_entries = all_entries.clone();
        sorted_entries.sort_by(|left, right| match left.key.cmp(&right.key) {
            Ordering::Equal => right.seq.cmp(&left.seq),
            other => other,
        });
        // Persist the "already-written" output SSTs when resuming from a prefix.
        let output_ssts = if let Some(resume_index) = resume_index {
            assert!(resume_index < sorted_entries.len());
            write_sst(
                &table_store,
                &sorted_entries[..=resume_index],
                output_max_sst_size,
            )
            .await
        } else {
            Vec::new()
        };

        // Build compaction args that resume from the output SST starting point and expect
        // the iterator to continue at the correct next row.
        let job_args = StartCompactionJobArgs {
            id: Ulid::new(),
            compaction_id: Ulid::new(),
            destination: 0,
            sst_views: l0_sst_views,
            sorted_runs,
            output_ssts,
            compaction_clock_tick: 0,
            is_dest_last_run: false,
            retention_min_seq,
            subcompactions: vec![],
        };

        // Verify the resumed iterator yields all remaining rows, starting immediately
        // after the persisted prefix and continuing in sorted order.
        let full_range = BytesRange::unbounded();
        let sequence_tracker = {
            let stored_manifest =
                StoredManifest::load(executor.inner.manifest_store.clone(), clock.clone())
                    .await
                    .unwrap();
            Arc::new(stored_manifest.db_state().sequence_tracker.clone())
        };
        let mut iter = executor
            .inner
            .load_iterators(
                &job_args,
                &full_range,
                &job_args.output_ssts,
                sequence_tracker,
            )
            .await
            .unwrap();
        let mut resumed_entries = Vec::new();
        while let Some(entry) = iter.next().await.unwrap() {
            resumed_entries.push(entry);
        }
        assert_eq!(resumed_entries, expected_rows);
    }

    // Property-based test to verify that executing a compaction job
    // with resume points produces the same final output as executing
    // the full compaction job in one go.
    //
    // The test generates random L0 and sorted-run entry sets,
    // writes them to SSTs, and runs the compaction job twice:
    //
    // 1. Full compaction job without resuming.
    // 2. Compaction job with resume points inserted at various
    //    positions in the output.
    //
    // The outputs of both runs are compared to ensure they match.
    #[test]
    fn test_execute_compaction_job_resume_matches_full() {
        const PROPTEST_CASES: u32 = 256;
        const KEY_SIZE: usize = 4;
        const VALUE_SIZE: usize = 6;
        const MAX_SEQ: u64 = 2000;
        const MAX_ENTRY_SET_LEN: usize = 64;
        const MAX_L0_SETS: usize = 3;
        const MAX_SR_SETS: usize = 3;
        const RESUME_RANGE_MAX: usize = 16;
        const RESUME_POINTS_MIN: usize = 1;
        const RESUME_POINTS_MAX: usize = 3;
        const MAX_SST_SIZE: usize = 128;
        const BLOCK_SIZE: usize = 64;

        proptest!(Config::with_cases(PROPTEST_CASES), |(
            l0_specs in proptest::collection::vec(
                proptest::collection::vec(
                    (
                        arbitrary::nonempty_bytes(KEY_SIZE),
                        0u64..MAX_SEQ,
                        prop_oneof![
                            arbitrary::nonempty_bytes(VALUE_SIZE).prop_map(ValueDeletable::Value),
                            arbitrary::nonempty_bytes(VALUE_SIZE).prop_map(ValueDeletable::Merge),
                            Just(ValueDeletable::Tombstone),
                        ],
                    ),
                    1..=MAX_ENTRY_SET_LEN,
                ),
                1..=MAX_L0_SETS,
            ),
            sr_specs in proptest::collection::vec(
                proptest::collection::vec(
                    (
                        arbitrary::nonempty_bytes(KEY_SIZE),
                        0u64..MAX_SEQ,
                        prop_oneof![
                            arbitrary::nonempty_bytes(VALUE_SIZE).prop_map(ValueDeletable::Value),
                            arbitrary::nonempty_bytes(VALUE_SIZE).prop_map(ValueDeletable::Merge),
                            Just(ValueDeletable::Tombstone),
                        ],
                    ),
                    1..=MAX_ENTRY_SET_LEN,
                ),
                0..=MAX_SR_SETS,
            ),
            resume_points in proptest::collection::vec(
                0usize..RESUME_RANGE_MAX,
                RESUME_POINTS_MIN..=RESUME_POINTS_MAX,
            ),
        )| {
            prop_assume!(!has_duplicate_key_seq_specs(&l0_specs, &sr_specs));
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let l0_entry_sets = l0_specs
                    .into_iter()
                    .map(build_row_entries)
                    .collect::<Vec<_>>();
                let sr_entry_sets = sr_specs
                    .into_iter()
                    .map(|entries| vec![build_row_entries(entries)])
                    .collect::<Vec<_>>();

                let has_merge = l0_entry_sets
                    .iter()
                    .flatten()
                    .chain(sr_entry_sets.iter().flatten().flatten())
                    .any(|entry| matches!(entry.value, ValueDeletable::Merge(_)));
                let merge_operator = if has_merge {
                    Some(Arc::new(StringConcatMergeOperator {}) as MergeOperatorType)
                } else {
                    None
                };
                let retention_min_seq = if merge_operator.is_some() {
                    None
                } else {
                    Some(0)
                };

                let handle = tokio::runtime::Handle::current();
                let options = CompactorOptions {
                    max_sst_size: MAX_SST_SIZE,
                    ..Default::default()
                };
                let options = Arc::new(options);
                let (tx, _rx) = async_channel::unbounded::<CompactorMessage>();
                let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let root_path = Path::from("testdb-exec-resume");
                let clock = Arc::new(DefaultSystemClock::new());
                let table_store = Arc::new(TableStore::new(
                    ObjectStores::new(object_store.clone(), None),
                    SsTableFormat {
                        block_size: BLOCK_SIZE,
                        ..SsTableFormat::default()
                    },
                    root_path.clone(),
                    None,
                ));
                let manifest_store = Arc::new(ManifestStore::new(&root_path, object_store.clone()));
                StoredManifest::create_new_db(
                    manifest_store.clone(),
                    ManifestCore::new(),
                    clock.clone(),
                )
                .await
                .unwrap();

                let executor = TokioCompactionExecutor::new(TokioCompactionExecutorOptions {
                    handle,
                    options,
                    worker_tx: tx,
                    table_store: table_store.clone(),
                    rand: Arc::new(DbRand::new(100u64)),
                    stats: {
                        let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
                        Arc::new(CompactionStats::new(&recorder))
                    },
                    clock,
                    manifest_store,
                    merge_operator,
                    #[cfg(feature = "compaction_filters")]
                    compaction_filter_supplier: None,
                });

                let mut l0_ssts = Vec::new();
                for entries in &l0_entry_sets {
                    let ssts = write_ssts(&table_store, entries, usize::MAX).await;
                    l0_ssts.extend(ssts.into_iter().map(SsTableView::identity));
                }

                let sorted_runs = build_sorted_runs(&table_store, &sr_entry_sets, usize::MAX).await;

                let full_run = executor
                    .inner
                    .execute_compaction_job(StartCompactionJobArgs {
                        id: Ulid::new(),
                        compaction_id: Ulid::new(),
                        destination: 0,
                        sst_views: l0_ssts.clone(),
                        sorted_runs: sorted_runs.clone(),
                        output_ssts: Vec::new(),
                        compaction_clock_tick: 0,
                        is_dest_last_run: false,
                        retention_min_seq,
                        subcompactions: vec![],
                    })
                    .await
                    .unwrap();

                let mut expected_entries = Vec::new();
                for sst in &full_run.sst_views {
                    let mut iter = SstIterator::new(
                        SstView::Borrowed(sst, BytesRange::from(..)),
                        table_store.clone(),
                        SstIteratorOptions::default(),
                    )
                    .unwrap();
                    iter.init().await.unwrap();
                    while let Some(entry) = iter.next().await.unwrap() {
                        expected_entries.push(entry);
                    }
                }

                assert!(!expected_entries.is_empty());
                let mut resume_indices = resume_points
                    .into_iter()
                    .map(|index| index % expected_entries.len())
                    .collect::<Vec<_>>();
                resume_indices.sort_unstable();
                resume_indices.dedup();

                for resume_index in resume_indices {
                    let output_ssts = write_ssts(
                        &table_store,
                        &expected_entries[..=resume_index],
                        MAX_SST_SIZE,
                    )
                    .await;
                    let resumed_run = executor
                        .inner
                        .execute_compaction_job(StartCompactionJobArgs {
                            id: Ulid::new(),
                            compaction_id: Ulid::new(),
                            destination: 0,
                            sst_views: l0_ssts.clone(),
                            sorted_runs: sorted_runs.clone(),
                            output_ssts,
                            compaction_clock_tick: 0,
                            is_dest_last_run: false,
                            retention_min_seq,
                            subcompactions: vec![],
                        })
                        .await
                        .unwrap();

                    let mut resumed_entries = Vec::new();
                    for sst in &resumed_run.sst_views {
                        let mut iter = SstIterator::new(
                            SstView::Borrowed(sst, BytesRange::from(..)),
                            table_store.clone(),
                            SstIteratorOptions::default(),
                        )
                        .unwrap();
                        iter.init().await.unwrap();
                        while let Some(entry) = iter.next().await.unwrap() {
                            resumed_entries.push(entry);
                        }
                    }

                    assert_eq!(resumed_entries, expected_entries);
                }

            });
        });
    }

    /// Reads back every entry from a sorted run's SSTs, in order.
    async fn read_run_entries(table_store: &Arc<TableStore>, run: &SortedRun) -> Vec<RowEntry> {
        let mut entries = Vec::new();
        for sst in &run.sst_views {
            let mut iter = SstIterator::new(
                SstView::Borrowed(sst, BytesRange::from(..)),
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .unwrap();
            iter.init().await.unwrap();
            while let Some(entry) = iter.next().await.unwrap() {
                entries.push(entry);
            }
        }
        entries
    }

    /// End-to-end coverage for subcompactions (RFC-0028): a compaction split
    /// into parallel key ranges must produce exactly the same merged entries
    /// as a single-threaded merge, and resuming from any persisted
    /// subcompaction snapshot must converge to the same result while reusing
    /// the output of already-completed ranges.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_subcompactions_match_single_merge_and_resume() {
        const SST_SIZE: usize = 512;

        let handle = tokio::runtime::Handle::current();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root_path = Path::from("testdb-subcompactions");
        let clock = Arc::new(DefaultSystemClock::new());
        // A small block size produces many candidate split points in the
        // input SSTs, so the planner reliably splits the job.
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat {
                block_size: 256,
                ..SsTableFormat::default()
            },
            root_path.clone(),
            None,
        ));
        let manifest_store = Arc::new(ManifestStore::new(&root_path, object_store.clone()));
        StoredManifest::create_new_db(manifest_store.clone(), ManifestCore::new(), clock.clone())
            .await
            .unwrap();

        let build_executor = |options: CompactorOptions| {
            let (tx, rx) = async_channel::unbounded::<CompactorMessage>();
            let executor = TokioCompactionExecutor::new(TokioCompactionExecutorOptions {
                handle: handle.clone(),
                options: Arc::new(options),
                worker_tx: tx,
                table_store: table_store.clone(),
                rand: Arc::new(DbRand::new(100u64)),
                stats: {
                    let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
                    Arc::new(CompactionStats::new(&recorder))
                },
                clock: clock.clone(),
                manifest_store: manifest_store.clone(),
                merge_operator: None,
                #[cfg(feature = "compaction_filters")]
                compaction_filter_supplier: None,
            });
            (executor, rx)
        };

        // given: overlapping inputs across two L0s and two sorted runs, each
        // spanning multiple SSTs.
        let l0_a: Vec<RowEntry> = (0..400u64)
            .step_by(2)
            .map(|i| {
                RowEntry::new_value(
                    format!("key{i:05}").as_bytes(),
                    format!("l0a-{i}").as_bytes(),
                    10_000 + i,
                )
            })
            .collect();
        let l0_b: Vec<RowEntry> = (1..400u64)
            .step_by(2)
            .map(|i| {
                RowEntry::new_value(
                    format!("key{i:05}").as_bytes(),
                    format!("l0b-{i}").as_bytes(),
                    20_000 + i,
                )
            })
            .collect();
        let sr_a: Vec<RowEntry> = (0..400u64)
            .map(|i| {
                RowEntry::new_value(
                    format!("key{i:05}").as_bytes(),
                    format!("sra-{i}").as_bytes(),
                    1 + i,
                )
            })
            .collect();
        let sr_b: Vec<RowEntry> = (0..400u64)
            .step_by(5)
            .map(|i| {
                RowEntry::new_value(
                    format!("key{i:05}").as_bytes(),
                    format!("srb-{i}").as_bytes(),
                    5_000 + i,
                )
            })
            .collect();
        let mut sst_views = Vec::new();
        for entries in [&l0_a, &l0_b] {
            let ssts = write_ssts(&table_store, entries, SST_SIZE).await;
            sst_views.extend(ssts.into_iter().map(SsTableView::identity));
        }
        let sorted_runs =
            build_sorted_runs(&table_store, &[vec![sr_a], vec![sr_b]], SST_SIZE).await;

        let job_args = |subcompactions: Vec<Subcompaction>| StartCompactionJobArgs {
            id: Ulid::new(),
            compaction_id: Ulid::new(),
            destination: 0,
            sst_views: sst_views.clone(),
            sorted_runs: sorted_runs.clone(),
            output_ssts: vec![],
            compaction_clock_tick: 0,
            is_dest_last_run: false,
            retention_min_seq: Some(0),
            subcompactions,
        };

        // when: the job runs as a single merge.
        let (single_executor, _single_rx) = build_executor(CompactorOptions {
            max_sst_size: SST_SIZE,
            max_subcompactions: 1,
            ..CompactorOptions::default()
        });
        let expected_run = single_executor
            .inner
            .execute_compaction_job(job_args(vec![]))
            .await
            .unwrap();
        let expected_entries = read_run_entries(&table_store, &expected_run).await;
        assert!(!expected_entries.is_empty());

        // when: the same job runs split into subcompactions.
        let (sub_executor, sub_rx) = build_executor(CompactorOptions {
            max_sst_size: SST_SIZE,
            max_subcompactions: 4,
            min_subcompaction_input_bytes: 1,
            ..CompactorOptions::default()
        });
        let sub_run = sub_executor
            .inner
            .execute_compaction_job(job_args(vec![]))
            .await
            .unwrap();

        // then: the merged output is identical to the single merge.
        assert_eq!(
            read_run_entries(&table_store, &sub_run).await,
            expected_entries
        );

        // then: progress messages carried persistable subcompaction
        // snapshots with a stable plan, ending fully complete.
        let mut snapshots: Vec<Vec<Subcompaction>> = Vec::new();
        while let Ok(msg) = sub_rx.try_recv() {
            if let CompactorMessage::CompactionJobProgress { subcompactions, .. } = msg {
                if !subcompactions.is_empty() {
                    snapshots.push(subcompactions);
                }
            }
        }
        assert!(!snapshots.is_empty(), "expected subcompaction snapshots");
        let plan_len = snapshots[0].len();
        assert!(plan_len >= 2, "expected the job to split, got {plan_len}");
        assert!(snapshots.iter().all(|s| s.len() == plan_len));
        let final_snapshot = snapshots.last().unwrap();
        assert!(final_snapshot.iter().all(|s| s.is_complete()));

        // when/then: resuming from any persisted snapshot — the bare plan, a
        // mid-flight state, and the fully-complete state — converges to the
        // same merged output and reuses the output SSTs of completed ranges.
        for index in [0, snapshots.len() / 2, snapshots.len() - 1] {
            let snapshot = snapshots[index].clone();
            let resumed_run = sub_executor
                .inner
                .execute_compaction_job(job_args(snapshot.clone()))
                .await
                .unwrap();
            assert_eq!(
                read_run_entries(&table_store, &resumed_run).await,
                expected_entries,
                "resume from snapshot {index} diverged"
            );
            for subcompaction in snapshot.iter().filter(|s| s.is_complete()) {
                for sst in subcompaction.output_ssts() {
                    assert!(
                        resumed_run.sst_views.iter().any(|v| v.sst.id == sst.id),
                        "completed subcompaction output SST was not reused"
                    );
                }
            }
        }
    }

    /// When one subcompaction fails, the job must abort its sibling ranges
    /// and report the failure (rather than hanging on them or committing
    /// partial output). Uses a compaction filter that errors only on keys in
    /// the upper part of the keyspace, so only the range(s) covering those
    /// keys fail while lower ranges run normally.
    #[cfg(feature = "compaction_filters")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_subcompaction_failure_fails_job() {
        use crate::compaction_filter::{
            CompactionFilter, CompactionFilterDecision, CompactionFilterError,
            CompactionFilterSupplier, CompactionJobContext,
        };

        struct FailUpperRangeFilter;

        #[async_trait::async_trait]
        impl CompactionFilter for FailUpperRangeFilter {
            async fn filter(
                &mut self,
                entry: &RowEntry,
            ) -> Result<CompactionFilterDecision, CompactionFilterError> {
                if entry.key.as_ref() >= b"key00300".as_slice() {
                    Err(CompactionFilterError::FilterError(
                        "injected failure".into(),
                    ))
                } else {
                    Ok(CompactionFilterDecision::Keep)
                }
            }

            async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
                Ok(())
            }
        }

        struct FailUpperRangeFilterSupplier;

        #[async_trait::async_trait]
        impl CompactionFilterSupplier for FailUpperRangeFilterSupplier {
            async fn create_compaction_filter(
                &self,
                _context: &CompactionJobContext,
            ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
                Ok(Box::new(FailUpperRangeFilter))
            }
        }

        const SST_SIZE: usize = 512;
        let handle = tokio::runtime::Handle::current();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root_path = Path::from("testdb-subcompaction-failure");
        let clock = Arc::new(DefaultSystemClock::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat {
                block_size: 256,
                ..SsTableFormat::default()
            },
            root_path.clone(),
            None,
        ));
        let manifest_store = Arc::new(ManifestStore::new(&root_path, object_store.clone()));
        StoredManifest::create_new_db(manifest_store.clone(), ManifestCore::new(), clock.clone())
            .await
            .unwrap();

        let (tx, _rx) = async_channel::unbounded::<CompactorMessage>();
        let executor = TokioCompactionExecutor::new(TokioCompactionExecutorOptions {
            handle,
            options: Arc::new(CompactorOptions {
                max_sst_size: SST_SIZE,
                max_subcompactions: 4,
                min_subcompaction_input_bytes: 1,
                ..CompactorOptions::default()
            }),
            worker_tx: tx,
            table_store: table_store.clone(),
            rand: Arc::new(DbRand::new(100u64)),
            stats: {
                let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
                Arc::new(CompactionStats::new(&recorder))
            },
            clock,
            manifest_store,
            merge_operator: None,
            compaction_filter_supplier: Some(Arc::new(FailUpperRangeFilterSupplier)),
        });

        let sr: Vec<RowEntry> = (0..400u64)
            .map(|i| {
                RowEntry::new_value(
                    format!("key{i:05}").as_bytes(),
                    format!("v-{i}").as_bytes(),
                    1 + i,
                )
            })
            .collect();
        let sorted_runs = build_sorted_runs(&table_store, &[vec![sr]], SST_SIZE).await;

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            executor
                .inner
                .execute_compaction_job(StartCompactionJobArgs {
                    id: Ulid::new(),
                    compaction_id: Ulid::new(),
                    destination: 0,
                    sst_views: vec![],
                    sorted_runs,
                    output_ssts: vec![],
                    compaction_clock_tick: 0,
                    is_dest_last_run: false,
                    retention_min_seq: Some(0),
                    subcompactions: vec![],
                }),
        )
        .await
        .expect("job should fail promptly rather than hang on aborted siblings");

        let err = result.expect_err("job with a failing subcompaction should fail");
        assert!(
            matches!(err, SlateDBError::CompactionFilterError(_)),
            "expected CompactionFilterError, got: {err:?}"
        );
    }

    /// Resuming a job whose previous attempt ran as a single merge (persisted
    /// `output_ssts` but no subcompaction plan) must stay on the single-merge
    /// path: splitting now would invalidate the existing resume point.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_should_not_plan_subcompactions_when_resuming_single_merge() {
        let ctx = TestContextBuilder::new("testdb-no-replan").build().await;
        let resume_output_sst = SsTableHandle::new(
            SsTableId::Compacted(Ulid::new()),
            1,
            crate::db_state::SsTableInfo::default(),
        );
        let args = StartCompactionJobArgs {
            id: Ulid::new(),
            compaction_id: Ulid::new(),
            destination: 0,
            sst_views: vec![],
            sorted_runs: vec![],
            output_ssts: vec![resume_output_sst],
            compaction_clock_tick: 0,
            is_dest_last_run: false,
            retention_min_seq: Some(0),
            subcompactions: vec![],
        };

        assert!(ctx
            .executor
            .inner
            .plan_or_resume_subcompactions(&args)
            .is_none());

        // A persisted plan, by contrast, is always resumed verbatim.
        let persisted = vec![
            Subcompaction::new(BytesRange::from_slice(..b"m".as_slice())),
            Subcompaction::new(BytesRange::from_slice(b"m".as_slice()..)),
        ];
        let args = StartCompactionJobArgs {
            output_ssts: vec![],
            subcompactions: persisted.clone(),
            ..args
        };
        assert_eq!(
            ctx.executor.inner.plan_or_resume_subcompactions(&args),
            Some(persisted)
        );
    }

    /// A blocked SST `close()` (the object-store flush of a finished output SST)
    /// must not stall the merge loop. We gate the object store's `put` so the
    /// first output SST's flush blocks indefinitely, then assert the executor
    /// still makes progress (emits a progress message) while that flush is in
    /// flight. Before `close()` was pipelined off the loop, the loop would be
    /// parked inside `close()` and emit nothing until the flush completed.
    #[tokio::test(flavor = "multi_thread")]
    async fn should_keep_processing_while_sst_flush_is_blocked() {
        // given: an executor writing through a gated object store, with a tiny
        // max_sst_size so each finished block rolls into a new output SST
        // (guaranteeing several SST boundaries, and thus background closes).
        let handle = tokio::runtime::Handle::current();
        let options = Arc::new(CompactorOptions {
            max_sst_size: 1,
            ..CompactorOptions::default()
        });
        let (tx, rx) = async_channel::unbounded::<CompactorMessage>();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(inner));
        let object_store: Arc<dyn ObjectStore> = gated.clone();
        let root_path = Path::from("testdb-flush-no-block");
        let clock = Arc::new(DefaultSystemClock::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat {
                block_size: 64,
                ..SsTableFormat::default()
            },
            root_path.clone(),
            None,
        ));
        let manifest_store = Arc::new(ManifestStore::new(&root_path, object_store.clone()));
        StoredManifest::create_new_db(manifest_store.clone(), ManifestCore::new(), clock.clone())
            .await
            .unwrap();

        let executor = TokioCompactionExecutor::new(TokioCompactionExecutorOptions {
            handle,
            options,
            worker_tx: tx,
            table_store: table_store.clone(),
            rand: Arc::new(DbRand::new(100u64)),
            stats: {
                let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
                Arc::new(CompactionStats::new(&recorder))
            },
            clock,
            manifest_store,
            merge_operator: None,
            #[cfg(feature = "compaction_filters")]
            compaction_filter_supplier: None,
        });

        // given: a single L0 SST with enough rows to span many output SSTs.
        let entries: Vec<RowEntry> = (0u64..64)
            .map(|i| {
                RowEntry::new_value(
                    format!("key{i:04}").as_bytes(),
                    format!("val{i:04}").as_bytes(),
                    i + 1,
                )
            })
            .collect();
        let input_ssts = write_sst(&table_store, &entries, usize::MAX).await;
        let sst_views: Vec<SsTableView> =
            input_ssts.into_iter().map(SsTableView::identity).collect();

        // given: the object-store flush (`put`) for output SSTs is blocked. Setup
        // writes above ran with the gate open (the default), so only the
        // compaction's output flushes are affected. The gate counts every call
        // (even pass-throughs), so baseline the setup puts to reason relatively.
        let setup_puts = gated.put_opts_gate.arrivals();
        gated.put_opts_gate.close();

        // when: the compaction job runs.
        executor.start_compaction_job(StartCompactionJobArgs {
            id: Ulid::new(),
            compaction_id: Ulid::new(),
            destination: 0,
            sst_views,
            sorted_runs: vec![],
            output_ssts: vec![],
            compaction_clock_tick: 0,
            is_dest_last_run: false,
            retention_min_seq: Some(0),
            subcompactions: vec![],
        });

        // when: the first output SST's flush is parked at the closed gate.
        tokio::time::timeout(
            Duration::from_secs(5),
            gated.put_opts_gate.wait_for_arrivals(setup_puts + 1),
        )
        .await
        .expect("a close() should reach the blocked put gate");

        // then: the loop keeps running and emits a progress message while the
        // flush is blocked. A `Finished` message here would mean the job somehow
        // completed despite the blocked flush.
        let progressed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match rx.recv().await.unwrap() {
                    CompactorMessage::CompactionJobProgress { .. } => break,
                    CompactorMessage::CompactionJobFinished { .. } => {
                        panic!("job finished while its SST flush was blocked")
                    }
                    _ => {}
                }
            }
        })
        .await;
        assert!(
            progressed.is_ok(),
            "expected a progress message while the SST flush was blocked"
        );

        // then: depth-1 backpressure holds — only one close is in flight at a
        // time, since the loop parks at the next boundary draining it.
        assert_eq!(gated.put_opts_gate.arrivals(), setup_puts + 1);

        // when: the blocked flush is released.
        gated.put_opts_gate.release();
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let CompactorMessage::CompactionJobFinished { result, .. } =
                    rx.recv().await.unwrap()
                {
                    return result;
                }
            }
        })
        .await
        .expect("job should finish after the gate is released")
        .expect("compaction should succeed");

        // then: multiple output SSTs were produced (proving real boundaries and
        // background closes ran) ...
        assert!(
            result.sst_views.len() >= 2,
            "expected multiple output SSTs, got {}",
            result.sst_views.len()
        );
        // ... and the merged output preserves every key in ascending order.
        let mut read_back = Vec::new();
        for sst in &result.sst_views {
            let mut iter = SstIterator::new(
                SstView::Borrowed(sst, BytesRange::from(..)),
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .unwrap();
            iter.init().await.unwrap();
            while let Some(entry) = iter.next().await.unwrap() {
                read_back.push(entry.key);
            }
        }
        let expected: Vec<Bytes> = entries.iter().map(|e| e.key.clone()).collect();
        assert_eq!(read_back, expected);
    }

    /// Test context for compaction executor tests.
    struct TestContext {
        executor: TokioCompactionExecutor,
        table_store: Arc<TableStore>,
        rx: async_channel::Receiver<CompactorMessage>,
    }

    /// Builder for creating test context with configurable options.
    struct TestContextBuilder {
        path: String,
        merge_operator: Option<MergeOperatorType>,
        #[cfg(feature = "compaction_filters")]
        compaction_filter_supplier: Option<Arc<dyn CompactionFilterSupplier>>,
    }

    impl TestContextBuilder {
        fn new(path: &str) -> Self {
            Self {
                path: path.to_string(),
                merge_operator: None,
                #[cfg(feature = "compaction_filters")]
                compaction_filter_supplier: None,
            }
        }

        fn with_merge_operator(mut self, merge_operator: MergeOperatorType) -> Self {
            self.merge_operator = Some(merge_operator);
            self
        }

        #[cfg(feature = "compaction_filters")]
        fn with_compaction_filter_supplier(
            mut self,
            supplier: Arc<dyn CompactionFilterSupplier>,
        ) -> Self {
            self.compaction_filter_supplier = Some(supplier);
            self
        }

        async fn build(self) -> TestContext {
            let handle = tokio::runtime::Handle::current();
            let options = Arc::new(CompactorOptions::default());
            let (tx, rx) = async_channel::unbounded();
            let os = Arc::new(InMemory::new());
            let clock = Arc::new(DefaultSystemClock::new());
            let db = Db::builder(self.path.clone(), os.clone())
                .with_system_clock(clock.clone())
                .build()
                .await
                .unwrap();
            let table_store = db.inner.table_store.clone();
            let manifest_store = Arc::new(ManifestStore::new(
                &Path::from(self.path.as_str()),
                os.clone(),
            ));

            let executor = TokioCompactionExecutor::new(TokioCompactionExecutorOptions {
                handle,
                options,
                worker_tx: tx,
                table_store: table_store.clone(),
                rand: Arc::new(DbRand::new(100u64)),
                stats: {
                    let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
                    Arc::new(CompactionStats::new(&recorder))
                },
                clock,
                manifest_store,
                merge_operator: self.merge_operator,
                #[cfg(feature = "compaction_filters")]
                compaction_filter_supplier: self.compaction_filter_supplier,
            });

            TestContext {
                executor,
                table_store,
                rx,
            }
        }
    }

    impl TestContext {
        /// Runs a compaction job and waits for the result.
        async fn run_compaction(
            self,
            ssts: Vec<SsTableHandle>,
            is_dest_last_run: bool,
            retention_min_seq: Option<u64>,
        ) -> Result<SortedRun, SlateDBError> {
            let compaction = StartCompactionJobArgs {
                id: Ulid::new(),
                compaction_id: Ulid::new(),
                destination: 0,
                sst_views: ssts.into_iter().map(SsTableView::identity).collect(),
                sorted_runs: vec![],
                output_ssts: vec![],
                compaction_clock_tick: 0,
                is_dest_last_run,
                retention_min_seq,
                subcompactions: vec![],
            };
            self.executor.start_compaction_job(compaction);

            tokio::time::timeout(Duration::from_secs(5), async move {
                loop {
                    let msg = self.rx.recv().await.unwrap();
                    if let CompactorMessage::CompactionJobFinished { id: _, result } = msg {
                        return result;
                    }
                }
            })
            .await
            .unwrap()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_job_should_retain_merges_newer_than_retention_min_seq_num() {
        let ctx = TestContextBuilder::new("testdb")
            .with_merge_operator(Arc::new(StringConcatMergeOperator {}))
            .build()
            .await;
        let table_store = ctx.table_store.clone();

        // write some merges
        let mut sst_builder = table_store.table_builder();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"3", 4))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"2", 3))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"1", 2))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"0", 1))
            .await
            .unwrap();
        let encoded_sst = sst_builder.build().await.unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let l0 = table_store
            .write_sst(&id, &encoded_sst, false)
            .await
            .unwrap();
        let retention_min_seq_num = 2;

        let result = ctx
            .run_compaction(vec![l0], false, Some(retention_min_seq_num))
            .await
            .unwrap();

        assert_eq!(1, result.sst_views.len());
        let sst = result.sst_views[0].clone();
        let mut iter = SstIterator::new(
            SstView::Borrowed(&sst, BytesRange::from(..)),
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .unwrap();
        iter.init().await.unwrap();
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"3".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num + 2);
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"2".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num + 1);
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"01".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num);
        assert!(iter.next().await.unwrap().is_none());
    }

    #[cfg(feature = "compaction_filters")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_job_with_filter_success() {
        use crate::compaction_filter::{
            CompactionFilter, CompactionFilterDecision, CompactionFilterError,
            CompactionFilterSupplier, CompactionJobContext,
        };

        /// A filter that tests all decision types:
        /// - "drop:" prefix -> Drop (remove entry)
        /// - "modify:" prefix -> Modify (append "_modified" suffix)
        /// - "tombstone:" prefix -> Modify to Tombstone
        /// - other prefixes -> Keep (unchanged)
        struct TestFilter;

        #[async_trait::async_trait]
        impl CompactionFilter for TestFilter {
            async fn filter(
                &mut self,
                entry: &RowEntry,
            ) -> Result<CompactionFilterDecision, CompactionFilterError> {
                if entry.key.starts_with(b"drop:") {
                    Ok(CompactionFilterDecision::Drop)
                } else if entry.key.starts_with(b"modify:") {
                    if let Some(value) = entry.value.as_bytes() {
                        let mut new_value = value.to_vec();
                        new_value.extend_from_slice(b"_modified");
                        Ok(CompactionFilterDecision::Modify(ValueDeletable::Value(
                            Bytes::from(new_value),
                        )))
                    } else {
                        Ok(CompactionFilterDecision::Keep)
                    }
                } else if entry.key.starts_with(b"tombstone:") {
                    Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone))
                } else {
                    Ok(CompactionFilterDecision::Keep)
                }
            }

            async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
                Ok(())
            }
        }

        struct TestFilterSupplier;

        #[async_trait::async_trait]
        impl CompactionFilterSupplier for TestFilterSupplier {
            async fn create_compaction_filter(
                &self,
                _context: &CompactionJobContext,
            ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
                Ok(Box::new(TestFilter))
            }
        }

        let ctx = TestContextBuilder::new("testdb_filter_all_decisions")
            .with_compaction_filter_supplier(Arc::new(TestFilterSupplier))
            .build()
            .await;
        let table_store = ctx.table_store.clone();

        // Write entries with different prefixes
        // Note: entries must be added in sorted key order
        // Lexicographic order: drop: < keep: < modify: < tombstone:
        let mut sst_builder = table_store.table_builder();
        sst_builder
            .add(RowEntry::new_value(b"drop:key1", b"value1", 1))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_value(b"drop:key2", b"value2", 2))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_value(b"keep:key3", b"value3", 3))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_value(b"keep:key4", b"value4", 4))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_value(b"modify:key5", b"value5", 5))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_value(b"modify:key6", b"value6", 6))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_value(b"tombstone:key7", b"value7", 7))
            .await
            .unwrap();
        sst_builder
            .add(RowEntry::new_value(b"tombstone:key8", b"value8", 8))
            .await
            .unwrap();
        let encoded_sst = sst_builder.build().await.unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let l0 = table_store
            .write_sst(&id, &encoded_sst, false)
            .await
            .unwrap();

        let result = ctx.run_compaction(vec![l0], true, None).await.unwrap();

        // Verify the output SST
        assert_eq!(1, result.sst_views.len());
        let sst = result.sst_views[0].clone();
        let mut iter = SstIterator::new(
            SstView::Borrowed(&sst, BytesRange::from(..)),
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .unwrap();
        iter.init().await.unwrap();

        // drop:key1 and drop:key2 should be DROPPED (not present)

        // keep:key3 - unchanged (Keep decision)
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"keep:key3".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Value(Bytes::from(b"value3".as_slice()))
        );

        // keep:key4 - unchanged (Keep decision)
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"keep:key4".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Value(Bytes::from(b"value4".as_slice()))
        );

        // modify:key5 - value modified (Modify decision)
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"modify:key5".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Value(Bytes::from(b"value5_modified".as_slice()))
        );

        // modify:key6 - value modified (Modify decision)
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"modify:key6".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Value(Bytes::from(b"value6_modified".as_slice()))
        );

        // tombstone:key7 - converted to tombstone (Modify to Tombstone decision)
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"tombstone:key7".as_slice()));
        assert!(next.value.is_tombstone());

        // tombstone:key8 - converted to tombstone (Modify to Tombstone decision)
        let next = iter.next().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"tombstone:key8".as_slice()));
        assert!(next.value.is_tombstone());

        // No more entries
        assert!(iter.next().await.unwrap().is_none());
    }

    #[cfg(feature = "compaction_filters")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_job_aborts_on_filter_creation_error() {
        use crate::compaction_filter::{
            CompactionFilter, CompactionFilterError, CompactionFilterSupplier, CompactionJobContext,
        };

        struct FailingFilterSupplier;

        #[async_trait::async_trait]
        impl CompactionFilterSupplier for FailingFilterSupplier {
            async fn create_compaction_filter(
                &self,
                _context: &CompactionJobContext,
            ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
                Err(CompactionFilterError::CreationError(
                    "intentional failure".into(),
                ))
            }
        }

        let ctx = TestContextBuilder::new("testdb_filter_fail")
            .with_compaction_filter_supplier(Arc::new(FailingFilterSupplier))
            .build()
            .await;
        let table_store = ctx.table_store.clone();

        // Write a simple entry
        let mut sst_builder = table_store.table_builder();
        sst_builder
            .add(RowEntry::new_value(b"key1", b"value1", 1))
            .await
            .unwrap();
        let encoded_sst = sst_builder.build().await.unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let l0 = table_store
            .write_sst(&id, &encoded_sst, false)
            .await
            .unwrap();

        let result = ctx.run_compaction(vec![l0], true, None).await;

        // The compaction should have failed
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SlateDBError::CompactionFilterError(_)));
    }

    #[cfg(feature = "compaction_filters")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_job_aborts_on_compaction_end_error() {
        use crate::compaction_filter::{
            CompactionFilter, CompactionFilterDecision, CompactionFilterError,
            CompactionFilterSupplier, CompactionJobContext,
        };

        /// A filter that keeps all entries but fails on on_compaction_end.
        struct FailOnEndFilter;

        #[async_trait::async_trait]
        impl CompactionFilter for FailOnEndFilter {
            async fn filter(
                &mut self,
                _entry: &RowEntry,
            ) -> Result<CompactionFilterDecision, CompactionFilterError> {
                Ok(CompactionFilterDecision::Keep)
            }

            async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
                Err(CompactionFilterError::CompactionEndError(
                    "intentional failure on compaction end".into(),
                ))
            }
        }

        struct FailOnEndFilterSupplier;

        #[async_trait::async_trait]
        impl CompactionFilterSupplier for FailOnEndFilterSupplier {
            async fn create_compaction_filter(
                &self,
                _context: &CompactionJobContext,
            ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
                Ok(Box::new(FailOnEndFilter))
            }
        }

        let ctx = TestContextBuilder::new("testdb_filter_end_fail")
            .with_compaction_filter_supplier(Arc::new(FailOnEndFilterSupplier))
            .build()
            .await;
        let table_store = ctx.table_store.clone();

        // Write a simple entry
        let mut sst_builder = table_store.table_builder();
        sst_builder
            .add(RowEntry::new_value(b"key1", b"value1", 1))
            .await
            .unwrap();
        let encoded_sst = sst_builder.build().await.unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let l0 = table_store
            .write_sst(&id, &encoded_sst, false)
            .await
            .unwrap();

        let result = ctx.run_compaction(vec![l0], true, None).await;

        // The compaction should have failed due to on_compaction_end error
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SlateDBError::CompactionFilterError(_)),
            "Expected CompactionFilterError, got: {:?}",
            err
        );
    }
}
