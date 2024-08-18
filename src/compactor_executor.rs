use crate::compactor::WorkerToOrchestoratorMsg;
use crate::compactor::WorkerToOrchestoratorMsg::CompactionFinished;
use crate::config::CompactorOptions;
use crate::db_state::{SSTableHandle, SortedRun, SsTableId};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::SstIterator;
use crate::tablestore::TableStore;
use futures::future::join_all;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use tokio::task::JoinHandle;
use ulid::Ulid;

pub(crate) struct CompactionJob {
    pub(crate) destination: u32,
    pub(crate) ssts: Vec<SSTableHandle>,
    pub(crate) sorted_runs: Vec<SortedRun>,
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
        worker_tx: crossbeam_channel::Sender<WorkerToOrchestoratorMsg>,
        table_store: Arc<TableStore>,
    ) -> Self {
        Self {
            inner: Arc::new(TokioCompactionExecutorInner {
                options,
                handle,
                worker_tx,
                table_store,
                tasks: Arc::new(Mutex::new(HashMap::new())),
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
    worker_tx: crossbeam_channel::Sender<WorkerToOrchestoratorMsg>,
    table_store: Arc<TableStore>,
    tasks: Arc<Mutex<HashMap<u32, TokioCompactionTask>>>,
    is_stopped: AtomicBool,
}

impl TokioCompactionExecutorInner {
    async fn execute_compaction(
        &self,
        compaction: CompactionJob,
    ) -> Result<SortedRun, SlateDBError> {
        let l0_iters: VecDeque<SstIterator> = compaction
            .ssts
            .iter()
            .map(|l0| SstIterator::new_spawn(l0, self.table_store.clone(), 4, 256))
            .collect();
        let l0_merge_iter = MergeIterator::new(l0_iters).await?;
        let sr_iters: VecDeque<SortedRunIterator> = compaction
            .sorted_runs
            .iter()
            .map(|sr| SortedRunIterator::new_spawn(sr, self.table_store.clone(), 4, 256))
            .collect();
        let sr_merge_iter = MergeIterator::new(sr_iters).await?;
        let mut all_iter = TwoMergeIterator::new(l0_merge_iter, sr_merge_iter).await?;
        let mut output_ssts = Vec::new();
        let mut current_writer = self
            .table_store
            .table_writer(SsTableId::Compacted(Ulid::new()));
        let mut current_size = 0usize;
        while let Some(kv) = all_iter.next_entry().await? {
            // Add to SST
            let value = kv.value.into_option();
            current_writer
                .add(kv.key.as_ref(), value.as_ref().map(|b| b.as_ref()))
                .await?;
            current_size += kv.key.len() + value.map_or(0, |b| b.len());
            if current_size > self.options.max_sst_size {
                println!("finish one sst");
                current_size = 0;
                let finished_writer = mem::replace(
                    &mut current_writer,
                    self.table_store
                        .table_writer(SsTableId::Compacted(Ulid::new())),
                );
                output_ssts.push(finished_writer.close().await?);
            }
        }
        if current_size > 0 {
            output_ssts.push(current_writer.close().await?);
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
                        eprintln!("Shutdown error in compaction task: {:?}", e);
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
