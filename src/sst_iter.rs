use crate::error::SlateDBError;
use crate::{
    block::Block,
    block_iterator::BlockIterator,
    iter::KeyValueIterator,
    tablestore::{SSTableHandle, TableStore},
    types::KeyValueDeletable,
};
use std::cmp::min;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinHandle;

enum FetchTask {
    InFlight(JoinHandle<Result<VecDeque<Block>, SlateDBError>>),
    Finished(VecDeque<Block>),
}

pub(crate) struct SstIterator<'a> {
    table: &'a SSTableHandle,
    current_iter: Option<BlockIterator<Block>>,
    next_block_idx_to_fetch: usize,
    fetch_tasks: VecDeque<FetchTask>,
    max_fetch_tasks: usize,
    blocks_to_fetch: usize,
    table_store: Arc<TableStore>,
}

impl<'a> SstIterator<'a> {
    pub(crate) fn new(
        table: &'a SSTableHandle,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Self {
        assert!(max_fetch_tasks > 0);
        assert!(blocks_to_fetch > 0);
        Self {
            table,
            current_iter: None,
            next_block_idx_to_fetch: 0,
            fetch_tasks: VecDeque::new(),
            max_fetch_tasks,
            blocks_to_fetch,
            table_store,
        }
    }

    pub(crate) fn new_spawn(
        table: &'a SSTableHandle,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        spawn: bool,
    ) -> Self {
        let mut iter = Self::new(table, table_store, max_fetch_tasks, blocks_to_fetch);
        if spawn {
            iter.spawn_fetches();
        }
        iter
    }

    fn spawn_fetches(&mut self) {
        let num_blocks = self.table.info.borrow().block_meta().len();
        while self.fetch_tasks.len() < self.max_fetch_tasks
            && self.next_block_idx_to_fetch < num_blocks
        {
            let blocks_to_fetch = min(
                self.blocks_to_fetch,
                num_blocks - self.next_block_idx_to_fetch,
            );
            let table = self.table.clone();
            let table_store = self.table_store.clone();
            let blocks_start = self.next_block_idx_to_fetch;
            let blocks_end = self.next_block_idx_to_fetch + blocks_to_fetch;
            self.fetch_tasks
                .push_back(FetchTask::InFlight(tokio::spawn(async move {
                    table_store
                        .read_blocks(&table, blocks_start..blocks_end)
                        .await
                })));
            self.next_block_idx_to_fetch = blocks_end;
        }
    }

    async fn next_iter(&mut self) -> Result<Option<BlockIterator<Block>>, SlateDBError> {
        loop {
            self.spawn_fetches();
            if let Some(fetch_task) = self.fetch_tasks.front_mut() {
                match fetch_task {
                    FetchTask::InFlight(jh) => {
                        let blocks = jh.await.expect("join task failed")?;
                        *fetch_task = FetchTask::Finished(blocks);
                    }
                    FetchTask::Finished(blocks) => {
                        if let Some(block) = blocks.pop_front() {
                            return Ok(Some(BlockIterator::from_first_key(block)));
                        } else {
                            self.fetch_tasks.pop_front();
                        }
                    }
                }
            } else {
                assert!(self.fetch_tasks.is_empty());
                assert_eq!(
                    self.next_block_idx_to_fetch,
                    self.table.info.borrow().block_meta().len()
                );
                return Ok(None);
            }
        }
    }
}

impl<'a> KeyValueIterator for SstIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
        loop {
            let current_iter = if let Some(current_iter) = self.current_iter.as_mut() {
                current_iter
            } else if let Some(next_iter) = self.next_iter().await? {
                self.current_iter.insert(next_iter)
            } else {
                return Ok(None);
            };

            let kv = current_iter.next_entry().await?;

            match kv {
                Some(kv) => return Ok(Some(kv)),
                None => {
                    self.current_iter = None;
                    // We have exhausted the current block, but not necessarily the entire SST,
                    // so we fall back to the top to check if we have more blocks to read.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SsTableFormat;
    use crate::tablestore::SsTableId;
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_one_block_sst_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let mut builder = table_store.table_builder();
        builder.add(b"key1", Some(b"value1")).unwrap();
        builder.add(b"key2", Some(b"value2")).unwrap();
        builder.add(b"key3", Some(b"value3")).unwrap();
        builder.add(b"key4", Some(b"value4")).unwrap();
        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(sst_handle.info.borrow().block_meta().len(), 1);

        let mut iter = SstIterator::new(&sst_handle, table_store.clone(), 1, 1);
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"key4".as_slice());
        assert_eq!(kv.value, b"value4".as_slice());
        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn test_many_block_sst_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(4096, 3);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let mut builder = table_store.table_builder();

        for i in 0..1000 {
            builder
                .add(
                    format!("key{}", i).as_bytes(),
                    Some(format!("value{}", i).as_bytes()),
                )
                .unwrap();
        }

        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        assert_eq!(sst_handle.info.borrow().block_meta().len(), 6);

        let mut iter = SstIterator::new(&sst_handle, table_store.clone(), 3, 3);
        for i in 0..1000 {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, format!("key{}", i));
            assert_eq!(kv.value, format!("value{}", i));
        }

        let next = iter.next().await.unwrap();
        assert!(next.is_none());
    }
}
