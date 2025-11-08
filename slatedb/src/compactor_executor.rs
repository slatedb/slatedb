use std::collections::HashMap;
use std::mem;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use chrono::TimeDelta;
use futures::future::{join, join_all};
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::clock::SystemClock;
use crate::compactor::CompactorMessage;
use crate::compactor::CompactorMessage::CompactionJobAttemptFinished;
use crate::config::CompactorOptions;
use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::merge_iterator::MergeIterator;
use crate::merge_operator::{
    MergeOperatorIterator, MergeOperatorRequiredIterator, MergeOperatorType,
};
use crate::rand::DbRand;
use crate::retention_iterator::RetentionIterator;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;

use crate::compactor::stats::CompactionStats;
use crate::utils::{build_concurrent, compute_max_parallel, spawn_bg_task, IdGenerator};
use log::{debug, error};
use tracing::instrument;
use ulid::Ulid;

/// Execution unit (attempt) for a compaction plan.
///
/// - `id` is the job id (ULID) and uniquely identifies a single execution attempt. This is
///   used as the runtime key in `scheduled_compactions`.
/// - `job_id` is the canonical plan id (ULID) that ties this job attempt back to its
///   `CompactionJob` entry in the compactor's canonical map.
///
/// Jobs carry fully materialized inputs (L0 `ssts` and `sorted_runs`) along with execution-time
/// metadata for progress reporting, retention, and resume logic.
#[derive(Clone, PartialEq)]
pub(crate) struct CompactorJobAttempt {
    /// Job attempt id. Unique per attempt and used for scheduling/routing.
    pub(crate) id: Ulid,
    /// Canonical compaction job id this job belongs to.
    pub(crate) job_id: Ulid,
    /// Destination sorted run id to be produced by this job.
    pub(crate) destination: u32,
    /// Input L0 SSTs for this attempt.
    pub(crate) ssts: Vec<SsTableHandle>,
    /// Input existing sorted runs for this attempt.
    pub(crate) sorted_runs: Vec<SortedRun>,
    /// Compaction timestamp used by the executor (e.g., for retention decisions).
    pub(crate) attempt_ts: i64,
    /// Whether the destination sorted run is the last (newest) run after compaction.
    pub(crate) is_dest_last_run: bool,
    /// Optional minimum sequence to retain; lower sequences may be dropped by retention.
    pub(crate) retention_min_seq: Option<u64>,
}

impl std::fmt::Debug for CompactorJobAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactorJobAttempt")
            .field("id", &self.id)
            .field("job_id", &self.job_id)
            .field("destination", &self.destination)
            .field("ssts", &self.ssts)
            .field("sorted_runs", &self.sorted_runs)
            .field("attempt_ts", &self.attempt_ts)
            .field("is_dest_last_run", &self.is_dest_last_run)
            .field("estimated_source_bytes", &self.estimated_source_bytes())
            .field("retention_min_seq", &self.retention_min_seq)
            .finish()
    }
}

impl CompactorJobAttempt {
    /// Estimates the total number of input bytes (L0 SSTs + Sorted Runs).
    ///
    /// Used by the compactor to track progress percentages for reporting.
    pub(crate) fn estimated_source_bytes(&self) -> u64 {
        let sst_size = self.ssts.iter().map(|sst| sst.estimate_size()).sum::<u64>();
        let sr_size = self
            .sorted_runs
            .iter()
            .map(|sr| sr.estimate_size())
            .sum::<u64>();
        sst_size + sr_size
    }
}

/// Executes compaction attempts produced by the compactor.
pub(crate) trait CompactionExecutor {
    /// Starts executing a compaction attempt asynchronously.
    fn start_compaction(&self, compaction: CompactorJobAttempt);

    /// Stops the executor and cancels any in-flight tasks, waiting for them to finish.
    fn stop(&self);

    /// Returns true if the executor has been stopped (but not necessarily finished).
    fn is_stopped(&self) -> bool;
}

pub(crate) struct TokioCompactionExecutor {
    inner: Arc<TokioCompactionExecutorInner>,
}

impl TokioCompactionExecutor {
    pub(crate) fn new(
        handle: tokio::runtime::Handle,
        options: Arc<CompactorOptions>,
        worker_tx: tokio::sync::mpsc::UnboundedSender<CompactorMessage>,
        table_store: Arc<TableStore>,
        rand: Arc<DbRand>,
        stats: Arc<CompactionStats>,
        clock: Arc<dyn SystemClock>,
        manifest_store: Arc<ManifestStore>,
        merge_operator: Option<MergeOperatorType>,
    ) -> Self {
        Self {
            inner: Arc::new(TokioCompactionExecutorInner {
                options,
                handle,
                worker_tx,
                table_store,
                rand,
                tasks: Arc::new(Mutex::new(HashMap::new())),
                stats,
                clock,
                is_stopped: AtomicBool::new(false),
                manifest_store,
                merge_operator,
            }),
        }
    }
}

impl CompactionExecutor for TokioCompactionExecutor {
    fn start_compaction(&self, compaction: CompactorJobAttempt) {
        self.inner.start_compaction(compaction);
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

pub(crate) struct TokioCompactionExecutorInner {
    options: Arc<CompactorOptions>,
    handle: tokio::runtime::Handle,
    worker_tx: tokio::sync::mpsc::UnboundedSender<CompactorMessage>,
    table_store: Arc<TableStore>,
    tasks: Arc<Mutex<HashMap<u32, TokioCompactionTask>>>,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    clock: Arc<dyn SystemClock>,
    is_stopped: AtomicBool,
    manifest_store: Arc<ManifestStore>,
    merge_operator: Option<MergeOperatorType>,
}

impl TokioCompactionExecutorInner {
    /// Builds input iterators for all sources (L0 and SR) and wraps them with optional
    /// merge and retention logic.
    async fn load_iterators<'a>(
        &self,
        compaction: &'a CompactorJobAttempt,
    ) -> Result<RetentionIterator<Box<dyn KeyValueIterator + 'a>>, SlateDBError> {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 4,
            blocks_to_fetch: 256,
            cache_blocks: false, // don't clobber the cache
            eager_spawn: true,
        };

        let max_parallel = compute_max_parallel(compaction.ssts.len(), &compaction.sorted_runs, 4);
        // L0 (borrowed)
        let l0_iters_futures = build_concurrent(compaction.ssts.iter(), max_parallel, |h| {
            SstIterator::new_borrowed_initialized(.., h, self.table_store.clone(), sst_iter_options)
        });

        // SR (borrowed)
        let sr_iters_futures =
            build_concurrent(compaction.sorted_runs.iter(), max_parallel, |sr| async {
                SortedRunIterator::new_borrowed(.., sr, self.table_store.clone(), sst_iter_options)
                    .await
                    .map(Some)
            });

        let (l0_iters_res, sr_iters_res) = join(l0_iters_futures, sr_iters_futures).await;
        let l0_iters = l0_iters_res?;
        let sr_iters = sr_iters_res?;

        let l0_merge_iter = MergeIterator::new(l0_iters)?.with_dedup(false);
        let sr_merge_iter = MergeIterator::new(sr_iters)?.with_dedup(false);

        let merge_iter = MergeIterator::new([l0_merge_iter, sr_merge_iter])?.with_dedup(false);
        let merge_iter = if let Some(merge_operator) = self.merge_operator.clone() {
            Box::new(MergeOperatorIterator::new(
                merge_operator,
                merge_iter,
                false,
                compaction.attempt_ts,
            ))
        } else {
            Box::new(MergeOperatorRequiredIterator::new(merge_iter)) as Box<dyn KeyValueIterator>
        };

        let stored_manifest = StoredManifest::load(self.manifest_store.clone()).await?;
        let mut retention_iter = RetentionIterator::new(
            merge_iter,
            None,
            None,
            compaction.is_dest_last_run,
            compaction.attempt_ts,
            self.clock.clone(),
            Arc::new(stored_manifest.db_state().sequence_tracker.clone()),
        )
        .await?;
        retention_iter.init().await?;
        Ok(retention_iter)
    }

    /// Executes a single compaction attempt and returns the resulting [`SortedRun`].
    ///
    /// ## Steps
    /// - Streams and merges input keys across all sources
    /// - Applies merge and retention policies
    /// - Writes output SSTs up to `max_sst_size`, reporting periodic progress
    ///
    /// ## Returns
    /// - The destination [`SortedRun`] with all output SST handles.
    #[instrument(level = "debug", skip_all, fields(id = %attempt.id))]
    async fn execute_compaction(
        &self,
        attempt: CompactorJobAttempt,
    ) -> Result<SortedRun, SlateDBError> {
        debug!("executing compaction [attempt={:?}]", attempt);
        let mut all_iter = self.load_iterators(&attempt).await?;
        let mut output_ssts = Vec::new();
        let mut current_writer = self.table_store.table_writer(SsTableId::Compacted(
            self.rand.rng().gen_ulid(self.clock.as_ref()),
        ));
        let mut bytes_written = 0usize;
        let mut last_progress_report = self.clock.now();

        while let Some(kv) = all_iter.next_entry().await? {
            let duration_since_last_report =
                self.clock.now().signed_duration_since(last_progress_report);
            if duration_since_last_report > TimeDelta::seconds(1) {
                // Allow send() because we are treating the executor like an external
                // component. They can do what they want. The send().expect() will raise
                // a SendErr, which will be caught in the cleanup_fn and set if there's
                // not already an error (i.e. if the DB is not already shut down).
                #[allow(clippy::disallowed_methods)]
                self.worker_tx
                    .send(CompactorMessage::CompactionJobAttemptProgress {
                        id: attempt.id,
                        bytes_processed: all_iter.total_bytes_processed(),
                    })
                    .expect("failed to send compaction progress");
                last_progress_report = self.clock.now();
            }

            if let Some(block_size) = current_writer.add(kv).await? {
                bytes_written += block_size;
            }

            if bytes_written > self.options.max_sst_size {
                let finished_writer = mem::replace(
                    &mut current_writer,
                    self.table_store.table_writer(SsTableId::Compacted(
                        self.rand.rng().gen_ulid(self.clock.as_ref()),
                    )),
                );
                let sst = finished_writer.close().await?;

                self.stats.bytes_compacted.add(sst.info.filter_offset);
                output_ssts.push(sst);
                bytes_written = 0;
            }
        }

        if !current_writer.is_drained() {
            let sst = current_writer.close().await?;

            self.stats.bytes_compacted.add(sst.info.filter_offset);
            output_ssts.push(sst);
        }

        Ok(SortedRun {
            id: attempt.destination,
            ssts: output_ssts,
        })
    }

    /// Starts a background task to run the compaction attempt.
    fn start_compaction(self: &Arc<Self>, attempt: CompactorJobAttempt) {
        let mut tasks = self.tasks.lock();
        if self.is_stopped.load(atomic::Ordering::SeqCst) {
            return;
        }
        let dst = attempt.destination;
        self.stats.running_compactions.inc();
        assert!(!tasks.contains_key(&dst));

        let id = attempt.id;

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
                // component. They can do what they want. The send().expect() will raise
                // a SendErr, which will be caught in the cleanup_fn and set if there's
                // not already an error (i.e. if the DB is not already shut down).
                #[allow(clippy::disallowed_methods)]
                this_cleanup
                    .worker_tx
                    .send(CompactionJobAttemptFinished { id, result })
                    .expect("failed to send compaction finished msg");
                this_cleanup.stats.running_compactions.dec();
            },
            async move { this.execute_compaction(attempt).await },
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
            for task in tasks.values() {
                task.task.abort();
            }
            tasks.drain().map(|(_, task)| task.task).collect::<Vec<_>>()
        };

        self.handle.block_on(async {
            let results = join_all(task_handles).await;
            for result in results {
                match result {
                    Err(e) if !e.is_cancelled() => {
                        error!("shutdown error in compaction task [error={:?}]", e);
                    }
                    _ => {}
                }
            }
        });

        self.is_stopped.store(true, atomic::Ordering::SeqCst);
    }

    /// Returns true if the executor has been stopped (but not necessarily finished).
    fn is_stopped(&self) -> bool {
        self.is_stopped.load(atomic::Ordering::SeqCst)
    }
}
