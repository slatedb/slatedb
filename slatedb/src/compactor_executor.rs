use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use futures::future::join_all;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

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
use crate::types::RowEntry;
use crate::types::ValueDeletable::Tombstone;
use crate::utils::{spawn_bg_task, IdGenerator};
use tracing::error;
use uuid::Uuid;

pub(crate) struct CompactionJob {
    pub(crate) id: Uuid,
    pub(crate) destination: u32,
    pub(crate) ssts: Vec<SsTableHandle>,
    pub(crate) sorted_runs: Vec<SortedRun>,
    pub(crate) compaction_ts: i64,
    pub(crate) is_dest_last_run: bool,
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

        let merge_iter = MergeIterator::new([l0_merge_iter, sr_merge_iter]).await?;
        let retention_iter =
            RetentionIterator::new(merge_iter, self.options.retention_time).await?;
        Ok(retention_iter)
    }

    async fn execute_compaction(
        &self,
        compaction: CompactionJob,
    ) -> Result<SortedRun, SlateDBError> {
        let mut all_iter = self.load_iterators(&compaction).await?;
        let mut output_ssts = Vec::new();
        let mut current_writer = self
            .table_store
            .table_writer(SsTableId::Compacted(self.rand.thread_rng().gen_ulid()));
        let mut current_size = 0usize;

        while let Some(raw_kv) = all_iter.next_entry().await? {
            // filter out any expired entries -- eventually we can consider
            // abstracting this away into generic, pluggable compaction filters
            // but for now we do it inline
            let kv = match raw_kv.expire_ts {
                Some(expire_ts) if expire_ts <= compaction.compaction_ts => {
                    // insert a tombstone instead of just filtering out the
                    // value in the iterator because this may otherwise "revive"
                    // an older version of the KV pair that has a larger TTL in
                    // a lower level of the LSM tree
                    RowEntry {
                        key: raw_kv.key,
                        value: Tombstone,
                        seq: raw_kv.seq,
                        expire_ts: None,
                        create_ts: raw_kv.create_ts,
                    }
                }
                _ => raw_kv,
            };

            // it contains multiple versions now, we can only skip the last tombstone
            if compaction.is_dest_last_run && kv.value.is_tombstone() {
                continue;
            }

            // Add to SST
            let key_len = kv.key.len();
            let value_len = kv.value.len();
            current_writer.add(kv).await?;
            current_size += key_len + value_len;
            if current_size > self.options.max_sst_size {
                current_size = 0;
                let finished_writer = mem::replace(
                    &mut current_writer,
                    self.table_store
                        .table_writer(SsTableId::Compacted(self.rand.thread_rng().gen_ulid())),
                );
                output_ssts.push(finished_writer.close().await?);
                self.stats.bytes_compacted.add(current_size as u64);
            }
        }
        if current_size > 0 {
            output_ssts.push(current_writer.close().await?);
            self.stats.bytes_compacted.add(current_size as u64);
        }
        Ok(SortedRun {
            id: compaction.destination,
            ssts: output_ssts,
        })
    }

    // Allow send() because we are treating the executor like an external
    // component. They can do what they want. The send().expect() will raise
    // a SendErr, which will be caught in the cleanup_fn and set if there's
    // not already an error (i.e. if the DB is not already shut down).
    #[allow(clippy::disallowed_methods)]
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
