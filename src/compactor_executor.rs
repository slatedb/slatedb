use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use futures::future::join_all;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::compactor::WorkerToOrchestratorMsg;
use crate::compactor::WorkerToOrchestratorMsg::CompactionFinished;
use crate::config::CompactorOptions;
use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;

use crate::metrics::DbStats;
use crate::types::RowEntry;
use crate::types::ValueDeletable::Tombstone;
use tracing::error;

pub(crate) struct CompactionJob {
    pub(crate) destination: u32,
    pub(crate) ssts: Vec<SsTableHandle>,
    pub(crate) sorted_runs: Vec<SortedRun>,
    pub(crate) compaction_ts: i64,
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
        worker_tx: crossbeam_channel::Sender<WorkerToOrchestratorMsg>,
        table_store: Arc<TableStore>,
        db_stats: Arc<DbStats>,
    ) -> Self {
        Self {
            inner: Arc::new(TokioCompactionExecutorInner {
                options,
                handle,
                worker_tx,
                table_store,
                tasks: Arc::new(Mutex::new(HashMap::new())),
                db_stats,
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
    task: JoinHandle<()>,
}

pub(crate) struct TokioCompactionExecutorInner {
    options: Arc<CompactorOptions>,
    handle: tokio::runtime::Handle,
    worker_tx: crossbeam_channel::Sender<WorkerToOrchestratorMsg>,
    table_store: Arc<TableStore>,
    tasks: Arc<Mutex<HashMap<u32, TokioCompactionTask>>>,
    db_stats: Arc<DbStats>,
    is_stopped: AtomicBool,
}

impl TokioCompactionExecutorInner {
    async fn load_iterators<'a>(
        &self,
        compaction: &'a CompactionJob,
    ) -> Result<
        TwoMergeIterator<MergeIterator<SstIterator<'a>>, MergeIterator<SortedRunIterator<'a>>>,
        SlateDBError,
    > {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 4,
            blocks_to_fetch: 256,
            cache_blocks: false, // don't clobber the cache
            eager_spawn: false,
        };

        let mut l0_iters = VecDeque::new();
        // TODO: No need to copy l0 SST
        for l0 in compaction.ssts.iter() {
            l0_iters.push_back(SstIterator::new_owned(
                l0.clone(),
                ..,
                self.table_store.clone(),
                sst_iter_options,
            ).await?);
        }
        let l0_merge_iter = MergeIterator::new(l0_iters).await?;

        let mut sr_iters = VecDeque::new();
        for sr in compaction.sorted_runs.iter() {
            let iter = SortedRunIterator::new_borrowed(
                sr,
                ..,
                self.table_store.clone(),
                sst_iter_options,
            ).await?;
            sr_iters.push_back(iter);
        }
        let sr_merge_iter = MergeIterator::new(sr_iters).await?;
        Ok(TwoMergeIterator::new(l0_merge_iter, sr_merge_iter).await?)
    }

    async fn execute_compaction(
        self: &Arc<Self>,
        compaction: CompactionJob,
    ) -> Result<SortedRun, SlateDBError> {
        let mut all_iter = self.load_iterators(
            &compaction
        ).await?;
        let mut output_ssts = Vec::new();
        let mut current_writer = self
            .table_store
            .table_writer(SsTableId::Compacted(Ulid::new()));
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
                        .table_writer(SsTableId::Compacted(Ulid::new())),
                );
                output_ssts.push(finished_writer.close().await?);
                self.db_stats.bytes_compacted.add(current_size as u64);
            }
        }
        if current_size > 0 {
            output_ssts.push(current_writer.close().await?);
            self.db_stats.bytes_compacted.add(current_size as u64);
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
        self.db_stats.running_compactions.inc();
        assert!(!tasks.contains_key(&dst));
        let this = self.clone();
        let task = self.handle.spawn(async move {
            let dst = compaction.destination;
            let result = this.execute_compaction(compaction).await;
            this.worker_tx
                .send(CompactionFinished(result))
                .expect("failed to send compaction finished msg");
            let mut tasks = this.tasks.lock();
            tasks.remove(&dst);
            this.db_stats.running_compactions.dec();
        });
        tasks.insert(dst, TokioCompactionTask { task });
    }

    fn stop(&self) {
        let mut tasks = self.tasks.lock();

        for task in tasks.values() {
            task.task.abort();
        }

        self.handle.block_on(async {
            let results = join_all(tasks.drain().map(|(_, task)| task.task)).await;
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
