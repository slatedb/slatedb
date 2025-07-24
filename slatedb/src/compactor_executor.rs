use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::clock::SystemClock;
use crate::compactor::WorkerToOrchestratorMsg;
use crate::compactor::WorkerToOrchestratorMsg::CompactionFinished;
use crate::config::CompactorOptions;
use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::merge_iterator::MergeIterator;
use crate::rand::DbRand;
use crate::retention_iterator::RetentionIterator;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;

use crate::compactor::stats::CompactionStats;
use crate::utils::{spawn_bg_task, IdGenerator};
use tracing::{debug, error, instrument};
use uuid::Uuid;

pub(crate) struct CompactionJob {
    pub(crate) id: Uuid,
    pub(crate) destination: u32,
    pub(crate) ssts: Vec<SsTableHandle>,
    pub(crate) sorted_runs: Vec<SortedRun>,
    pub(crate) compaction_ts: i64,
    pub(crate) is_dest_last_run: bool,
}

impl std::fmt::Debug for CompactionJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionJob")
            .field("id", &self.id)
            .field("destination", &self.destination)
            .field("ssts", &self.ssts)
            .field("sorted_runs", &self.sorted_runs)
            .field("compaction_ts", &self.compaction_ts)
            .field("is_dest_last_run", &self.is_dest_last_run)
            .field("estimated_source_bytes", &self.estimated_source_bytes())
            .finish()
    }
}

impl CompactionJob {
    /// Estimates the total size of the source SSTs and sorted runs.
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

pub(crate) trait CompactionExecutor {
    fn start_compaction(&self, compaction: CompactionJob);
    fn stop(&self);
    fn is_stopped(&self) -> bool;
}

pub(crate) struct TokioCompactionExecutor {
    inner: Arc<TokioCompactionExecutorInner>,
}

impl TokioCompactionExecutor {
    pub(crate) fn new(
        handle: tokio::runtime::Handle,
        options: Arc<CompactorOptions>,
        worker_tx: tokio::sync::mpsc::UnboundedSender<WorkerToOrchestratorMsg>,
        table_store: Arc<TableStore>,
        rand: Arc<DbRand>,
        stats: Arc<CompactionStats>,
        clock: Arc<dyn SystemClock>,
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
            }),
        }
    }
}

impl CompactionExecutor for TokioCompactionExecutor {
    fn start_compaction(&self, compaction: CompactionJob) {
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
    worker_tx: tokio::sync::mpsc::UnboundedSender<WorkerToOrchestratorMsg>,
    table_store: Arc<TableStore>,
    tasks: Arc<Mutex<HashMap<u32, TokioCompactionTask>>>,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    clock: Arc<dyn SystemClock>,
    is_stopped: AtomicBool,
}

impl TokioCompactionExecutorInner {
    async fn load_iterators<'a>(
        &self,
        compaction: &'a CompactionJob,
    ) -> Result<RetentionIterator<MergeIterator<'a>>, SlateDBError> {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 4,
            blocks_to_fetch: 256,
            cache_blocks: false, // don't clobber the cache
            eager_spawn: true,
        };

        let mut l0_iters = VecDeque::new();
        for l0 in compaction.ssts.iter() {
            let maybe_iter =
                SstIterator::new_borrowed(.., l0, self.table_store.clone(), sst_iter_options)
                    .await?;
            if let Some(iter) = maybe_iter {
                l0_iters.push_back(iter);
            }
        }
        let l0_merge_iter = MergeIterator::new(l0_iters).await?.with_dedup(false);

        let mut sr_iters = VecDeque::new();
        for sr in compaction.sorted_runs.iter() {
            let iter =
                SortedRunIterator::new_borrowed(.., sr, self.table_store.clone(), sst_iter_options)
                    .await?;
            sr_iters.push_back(iter);
        }
        let sr_merge_iter = MergeIterator::new(sr_iters).await?.with_dedup(false);

        let merge_iter = MergeIterator::new([l0_merge_iter, sr_merge_iter])
            .await?
            .with_dedup(false);
        let retention_iter = RetentionIterator::new(
            merge_iter,
            None,
            None,
            compaction.is_dest_last_run,
            compaction.compaction_ts,
            self.clock.clone(),
        )
        .await?;
        Ok(retention_iter)
    }

    #[instrument(level = "debug", skip_all, fields(id = %compaction.id))]
    async fn execute_compaction(
        &self,
        compaction: CompactionJob,
    ) -> Result<SortedRun, SlateDBError> {
        debug!(?compaction, "executing compaction");
        let mut all_iter = self.load_iterators(&compaction).await?;
        let mut output_ssts = Vec::new();
        let mut current_writer = self
            .table_store
            .table_writer(SsTableId::Compacted(self.rand.rng().gen_ulid()));

        let mut compacted_block_size = 0usize;
        let mut last_progress_report = self.clock.now();

        while let Some(kv) = all_iter.next_entry().await? {
            let duration_since_last_report = self
                .clock
                .now()
                .duration_since(last_progress_report)
                .unwrap_or(Duration::from_secs(0));
            if duration_since_last_report > Duration::from_secs(1) {
                // Allow send() because we are treating the executor like an external
                // component. They can do what they want. The send().expect() will raise
                // a SendErr, which will be caught in the cleanup_fn and set if there's
                // not already an error (i.e. if the DB is not already shut down).
                #[allow(clippy::disallowed_methods)]
                self.worker_tx
                    .send(WorkerToOrchestratorMsg::CompactionProgress {
                        id: compaction.id,
                        bytes_processed: all_iter.total_bytes_processed(),
                    })
                    .expect("failed to send compaction progress");
                last_progress_report = self.clock.now();
            }

            if let Some(block_size) = current_writer.add(kv).await? {
                compacted_block_size += block_size;
            }

            if compacted_block_size > self.options.max_sst_size {
                let finished_writer = mem::replace(
                    &mut current_writer,
                    self.table_store
                        .table_writer(SsTableId::Compacted(self.rand.rng().gen_ulid())),
                );
                let closed_sst = finished_writer.close().await?;

                output_ssts.push(closed_sst.sst);
                self.stats.bytes_compacted.add(compacted_block_size as u64);
                if let Some(drained_blocks_size) = closed_sst.drained_blocks_size {
                    self.stats.bytes_compacted.add(drained_blocks_size as u64);
                }

                compacted_block_size = 0;
            }
        }

        if !current_writer.is_drained() {
            let closed_sst = current_writer.close().await?;
            output_ssts.push(closed_sst.sst);

            if let Some(drained_blocks_size) = closed_sst.drained_blocks_size {
                self.stats.bytes_compacted.add(drained_blocks_size as u64);
            }
        }

        Ok(SortedRun {
            id: compaction.destination,
            ssts: output_ssts,
        })
    }

    fn start_compaction(self: &Arc<Self>, compaction: CompactionJob) {
        let mut tasks = self.tasks.lock();
        if self.is_stopped.load(atomic::Ordering::SeqCst) {
            return;
        }
        let dst = compaction.destination;
        self.stats.running_compactions.inc();
        assert!(!tasks.contains_key(&dst));

        let id = compaction.id;

        let this = self.clone();
        let this_cleanup = self.clone();
        let task = spawn_bg_task(
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
                    .send(CompactionFinished { id, result })
                    .expect("failed to send compaction finished msg");
                this_cleanup.stats.running_compactions.dec();
            },
            async move { this.execute_compaction(compaction).await },
        );
        tasks.insert(dst, TokioCompactionTask { task });
    }

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
                        error!("Shutdown error in compaction task: {:?}", e);
                    }
                    _ => {}
                }
            }
        });

        self.is_stopped.store(true, atomic::Ordering::SeqCst);
    }

    fn is_stopped(&self) -> bool {
        self.is_stopped.load(atomic::Ordering::SeqCst)
    }
}
