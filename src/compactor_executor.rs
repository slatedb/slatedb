use crate::compactor::WorkerToOrchestoratorMsg::CompactionFinished;
use crate::compactor::{CompactorOptions, WorkerToOrchestoratorMsg};
use crate::db_state::SortedRun;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst::EncodedSsTableBuilder;
use crate::sst_iter::SstIterator;
use crate::tablestore::{SSTableHandle, SsTableId, TableStore};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::mem;
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
            }),
        }
    }
}

impl CompactionExecutor for TokioCompactionExecutor {
    fn start_compaction(&self, compaction: CompactionJob) {
        self.inner.start_compaction(compaction);
    }
}

struct TokioCompactionTask {
    #[allow(dead_code)]
    task: JoinHandle<()>,
}

pub(crate) struct TokioCompactionExecutorInner {
    options: Arc<CompactorOptions>,
    handle: tokio::runtime::Handle,
    worker_tx: crossbeam_channel::Sender<WorkerToOrchestoratorMsg>,
    table_store: Arc<TableStore>,
    tasks: Arc<Mutex<HashMap<u32, TokioCompactionTask>>>,
}

impl TokioCompactionExecutorInner {
    async fn finish_sst(
        &self,
        builder: EncodedSsTableBuilder<'_>,
        ssts: &mut Vec<SSTableHandle>,
    ) -> Result<(), SlateDBError> {
        let encoded_sst = builder.build()?;
        let sst_id = SsTableId::Compacted(Ulid::new());
        ssts.push(self.table_store.write_sst(&sst_id, encoded_sst).await?);
        Ok(())
    }

    async fn execute_compaction(
        &self,
        compaction: CompactionJob,
    ) -> Result<SortedRun, SlateDBError> {
        let l0_iters: VecDeque<SstIterator> = compaction
            .ssts
            .iter()
            .map(|l0| SstIterator::new(l0, self.table_store.as_ref()))
            .collect();
        let l0_merge_iter = MergeIterator::new(l0_iters).await?;
        let sr_iters: VecDeque<SortedRunIterator> = compaction
            .sorted_runs
            .iter()
            .map(|sr| SortedRunIterator::new(sr, self.table_store.as_ref()))
            .collect();
        let sr_merge_iter = MergeIterator::new(sr_iters).await?;
        let mut all_iter = TwoMergeIterator::new(l0_merge_iter, sr_merge_iter).await?;
        let mut output_ssts = Vec::new();
        let mut current_builder = self.table_store.table_builder();
        let mut current_size = 0usize;
        while let Some(kv) = all_iter.next_entry().await? {
            // Add to SST
            let value = kv.value.into_option();
            current_builder.add(kv.key.as_ref(), value.as_ref().map(|b| b.as_ref()))?;
            current_size += kv.key.len() + value.map_or(0, |b| b.len());
            // todo: turn into option
            if current_size > self.options.max_sst_size {
                current_size = 0;
                let finished_builder =
                    mem::replace(&mut current_builder, self.table_store.table_builder());
                self.finish_sst(finished_builder, &mut output_ssts).await?;
            }
        }
        if current_size > 0 {
            self.finish_sst(current_builder, &mut output_ssts).await?;
        }
        Ok(SortedRun {
            id: compaction.destination,
            ssts: output_ssts,
        })
    }

    fn start_compaction(self: &Arc<Self>, compaction: CompactionJob) {
        let mut tasks = self.tasks.lock();
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
}
