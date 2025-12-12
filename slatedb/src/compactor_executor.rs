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
use crate::compactor::CompactorMessage::CompactionJobFinished;
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
    pub(crate) ssts: Vec<SsTableHandle>,
    /// Input existing sorted runs for this job.
    pub(crate) sorted_runs: Vec<SortedRun>,
    /// The logical clock tick representing the logical time the compaction occurs. This is used
    /// to make decisions about retention of expiring records.
    pub(crate) compaction_logical_clock_tick: i64,
    /// Whether the destination sorted run is the last (newest) run after compaction.
    pub(crate) is_dest_last_run: bool,
    /// Optional minimum sequence to retain; lower sequences may be dropped by retention.
    pub(crate) retention_min_seq: Option<u64>,
    /// Estimated total source bytes for this compaction.
    pub(crate) estimated_source_bytes: u64,
}

impl std::fmt::Debug for StartCompactionJobArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartCompactionJobArgs")
            .field("id", &self.id)
            .field("job_id", &self.compaction_id)
            .field("destination", &self.destination)
            .field("ssts", &self.ssts)
            .field("sorted_runs", &self.sorted_runs)
            .field(
                "compaction_logical_clock_tick",
                &self.compaction_logical_clock_tick,
            )
            .field("is_dest_last_run", &self.is_dest_last_run)
            .field("estimated_source_bytes", &self.estimated_source_bytes)
            .field("retention_min_seq", &self.retention_min_seq)
            .finish()
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
        job_args: &'a StartCompactionJobArgs,
    ) -> Result<RetentionIterator<Box<dyn KeyValueIterator + 'a>>, SlateDBError> {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 4,
            blocks_to_fetch: 256,
            cache_blocks: false, // don't clobber the cache
            eager_spawn: true,
        };

        let max_parallel = compute_max_parallel(job_args.ssts.len(), &job_args.sorted_runs, 4);
        // L0 (borrowed)
        let l0_iters_futures = build_concurrent(job_args.ssts.iter(), max_parallel, |h| {
            SstIterator::new_borrowed_initialized(.., h, self.table_store.clone(), sst_iter_options)
        });

        // SR (borrowed)
        let sr_iters_futures =
            build_concurrent(job_args.sorted_runs.iter(), max_parallel, |sr| async {
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
                job_args.compaction_logical_clock_tick,
                job_args.retention_min_seq,
            ))
        } else {
            Box::new(MergeOperatorRequiredIterator::new(merge_iter)) as Box<dyn KeyValueIterator>
        };

        let stored_manifest =
            StoredManifest::load(self.manifest_store.clone(), self.clock.clone()).await?;
        let mut retention_iter = RetentionIterator::new(
            merge_iter,
            None,
            job_args.retention_min_seq,
            job_args.is_dest_last_run,
            job_args.compaction_logical_clock_tick,
            self.clock.clone(),
            Arc::new(stored_manifest.db_state().sequence_tracker.clone()),
        )
        .await?;
        retention_iter.init().await?;
        Ok(retention_iter)
    }

    /// Executes a single compaction job and returns the resulting [`SortedRun`].
    ///
    /// ## Steps
    /// - Streams and merges input keys across all sources
    /// - Applies merge and retention policies
    /// - Writes output SSTs up to `max_sst_size`, reporting periodic progress
    ///
    /// ## Returns
    /// - The destination [`SortedRun`] with all output SST handles.
    #[instrument(level = "debug", skip_all, fields(id = %args.id))]
    async fn execute_compaction_job(
        &self,
        args: StartCompactionJobArgs,
    ) -> Result<SortedRun, SlateDBError> {
        debug!("executing compaction [job_args={:?}]", args);
        let mut all_iter = self.load_iterators(&args).await?;
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
                // component. They can do what they want. If the send fails (e.g., during
                // DB shutdown), we log it and continue with the compaction work.
                #[allow(clippy::disallowed_methods)]
                if let Err(e) = self
                    .worker_tx
                    .send(CompactorMessage::CompactionJobProgress {
                        id: args.id,
                        bytes_processed: all_iter.total_bytes_processed(),
                    })
                {
                    debug!(
                        "failed to send compaction progress (likely DB shutdown) [error={:?}]",
                        e
                    );
                }
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
            id: args.destination,
            ssts: output_ssts,
        })
    }

    /// Starts a background task to run the compaction job.
    fn start_compaction_job(self: &Arc<Self>, args: StartCompactionJobArgs) {
        let mut tasks = self.tasks.lock();
        if self.is_stopped.load(atomic::Ordering::SeqCst) {
            return;
        }
        let dst = args.destination;
        self.stats.running_compactions.inc();
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
                if let Err(e) = this_cleanup
                    .worker_tx
                    .send(CompactionJobFinished { id, result })
                {
                    debug!(
                        "failed to send compaction finished msg (likely DB shutdown) [error={:?}]",
                        e
                    );
                }
                this_cleanup.stats.running_compactions.dec();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::clock::DefaultSystemClock;
    use crate::sst_iter::SstView;
    use crate::stats::StatRegistry;
    use crate::test_utils::StringConcatMergeOperator;
    use crate::types::{RowEntry, ValueDeletable};
    use crate::Db;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_job_should_retain_merges_newer_than_retention_min_seq_num() {
        let handle = tokio::runtime::Handle::current();
        let options = Arc::new(CompactorOptions::default());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let os = Arc::new(InMemory::new());
        let path = "testdb".to_string();
        let clock = Arc::new(DefaultSystemClock::new());
        let db = Db::builder(path.clone(), os.clone())
            .with_system_clock(clock.clone())
            .build()
            .await
            .unwrap();
        let table_store = db.inner.table_store.clone();
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path.as_str()), os.clone()));
        let executor = TokioCompactionExecutor::new(
            handle,
            options,
            tx,
            table_store.clone(),
            Arc::new(DbRand::new(100u64)),
            Arc::new(CompactionStats::new(Arc::new(StatRegistry::new()))),
            clock,
            manifest_store.clone(),
            Some(Arc::new(StringConcatMergeOperator {})),
        );

        // write some merges
        let mut sst_builder = table_store.table_builder();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"3", 4))
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"2", 3))
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"1", 2))
            .unwrap();
        sst_builder
            .add(RowEntry::new_merge(b"foo", b"0", 1))
            .unwrap();
        let encoded_sst = sst_builder.build().unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let l0 = table_store
            .write_sst(&id, encoded_sst, false)
            .await
            .unwrap();
        let retention_min_seq_num = 2;

        // start a compaction of a single sst
        let compaction = StartCompactionJobArgs {
            id: Ulid::new(),
            compaction_id: Ulid::new(),
            destination: 0,
            ssts: vec![l0],
            sorted_runs: vec![],
            compaction_logical_clock_tick: 0,
            is_dest_last_run: false,
            retention_min_seq: Some(retention_min_seq_num),
            estimated_source_bytes: 0,
        };
        executor.start_compaction_job(compaction);

        // wait for it to finish
        let result = tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                let msg = rx.recv().await.unwrap();
                if let CompactorMessage::CompactionJobFinished { id: _, result } = msg {
                    return result;
                }
            }
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(1, result.ssts.len());
        let sst = result.ssts[0].clone();
        let mut iter = SstIterator::new(
            SstView::Borrowed(&sst, BytesRange::from(..)),
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .unwrap();
        iter.init().await.unwrap();
        let next = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"3".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num + 2);
        let next = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"2".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num + 1);
        let next = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(next.key, Bytes::from(b"foo".as_slice()));
        assert_eq!(
            next.value,
            ValueDeletable::Merge(Bytes::from(b"01".as_slice()))
        );
        assert_eq!(next.seq, retention_min_seq_num);
        assert!(iter.next_entry().await.unwrap().is_none());
    }
}
