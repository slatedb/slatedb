use crate::db_state::SSTableHandle;
use crate::error::SlateDBError;
use crate::{
    block::Block, block_iterator::BlockIterator, iter::KeyValueIterator, tablestore::TableStore,
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
    from_key: Option<&'a [u8]>,
    next_block_idx_to_fetch: usize,
    fetch_tasks: VecDeque<FetchTask>,
    max_fetch_tasks: usize,
    blocks_to_fetch: usize,
    table_store: Arc<TableStore>,
}

impl<'a> SstIterator<'a> {
    fn first_block_with_data_including_or_after_key(sst: &SSTableHandle, key: &[u8]) -> usize {
        let handle = sst.info.borrow();
        // search for the block that could contain the key.
        let mut low = 0;
        let mut high = handle.block_meta().len() - 1;
        // if the key is less than all the blocks' first key, scan the whole sst
        let mut found_block_id = 0;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_block_first_key = handle.block_meta().get(mid).first_key().bytes();
            match mid_block_first_key.cmp(key) {
                std::cmp::Ordering::Less => {
                    low = mid + 1;
                    found_block_id = mid;
                }
                std::cmp::Ordering::Greater => {
                    if mid > 0 {
                        high = mid - 1;
                    } else {
                        break;
                    }
                }
                std::cmp::Ordering::Equal => return mid,
            }
        }
        found_block_id
    }

    pub(crate) fn new_from_key(
        table: &'a SSTableHandle,
        table_store: Arc<TableStore>,
        from_key: &'a [u8],
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Self {
        Self::new_opts(
            table,
            Some(from_key),
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
        )
    }

    pub(crate) fn new_spawn(
        table: &'a SSTableHandle,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Self {
        Self::new_opts(
            table,
            None,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            true,
        )
    }

    pub(crate) fn new(
        table: &'a SSTableHandle,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
    ) -> Self {
        Self::new_opts(
            table,
            None,
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
        )
    }

    pub(crate) fn new_opts(
        table: &'a SSTableHandle,
        from_key: Option<&'a [u8]>,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        spawn: bool,
    ) -> Self {
        assert!(max_fetch_tasks > 0);
        assert!(blocks_to_fetch > 0);
        let next_block_idx_to_fetch = from_key
            .map(|k| Self::first_block_with_data_including_or_after_key(table, k))
            .unwrap_or(0);
        let mut iter = Self {
            table,
            current_iter: None,
            next_block_idx_to_fetch,
            from_key,
            fetch_tasks: VecDeque::new(),
            max_fetch_tasks,
            blocks_to_fetch,
            table_store,
        };
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
                            let first_key = self.from_key.take();
                            return match first_key {
                                None => Ok(Some(BlockIterator::from_first_key(block))),
                                Some(k) => Ok(Some(BlockIterator::from_key(block, k))),
                            };
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
    use crate::db_state::SsTableId;
    use crate::sst::SsTableFormat;
    use crate::test_utils::{assert_kv, OrderedBytesGenerator};
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

    #[tokio::test]
    async fn test_iter_from_key() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(128, 1);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let first_key = [b'a'; 16];
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'z');
        let mut test_case_key_gen = key_gen.clone();
        let first_val = [1u8; 16];
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let mut test_case_val_gen = val_gen.clone();
        let (sst, nkeys) = build_sst_with_n_blocks(3, table_store.clone(), key_gen, val_gen).await;

        // iterate over all keys and make sure we iterate from that key
        for i in 0..nkeys {
            let mut expected_key_gen = test_case_key_gen.clone();
            let mut expected_val_gen = test_case_val_gen.clone();
            let from_key = test_case_key_gen.next();
            let _ = test_case_val_gen.next();
            let mut iter =
                SstIterator::new_from_key(&sst, table_store.clone(), from_key.as_ref(), 1, 1);
            for _ in 0..nkeys - i {
                let e = iter.next().await.unwrap().unwrap();
                assert_kv(
                    &e,
                    expected_key_gen.next().as_ref(),
                    expected_val_gen.next().as_ref(),
                );
            }
            assert!(iter.next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_iter_from_key_smaller_than_first() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(128, 1);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let first_key = [b'b'; 16];
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'y');
        let mut expected_key_gen = key_gen.clone();
        let first_val = [2u8; 16];
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let mut expected_val_gen = val_gen.clone();
        let (sst, nkeys) = build_sst_with_n_blocks(2, table_store.clone(), key_gen, val_gen).await;

        let mut iter = SstIterator::new_from_key(&sst, table_store.clone(), &[b'a'; 16], 1, 1);

        for _ in 0..nkeys {
            let e = iter.next().await.unwrap().unwrap();
            assert_kv(
                &e,
                expected_key_gen.next().as_ref(),
                expected_val_gen.next().as_ref(),
            );
        }
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iter_from_key_larger_than_last() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::new(128, 1);
        let table_store = Arc::new(TableStore::new(object_store, format, root_path.clone()));
        let first_key = [b'b'; 16];
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'y');
        let first_val = [2u8; 16];
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let (sst, _) = build_sst_with_n_blocks(2, table_store.clone(), key_gen, val_gen).await;

        let mut iter = SstIterator::new_from_key(&sst, table_store.clone(), &[b'z'; 16], 1, 1);

        assert!(iter.next().await.unwrap().is_none());
    }

    async fn build_sst_with_n_blocks(
        n: usize,
        ts: Arc<TableStore>,
        mut key_gen: OrderedBytesGenerator,
        mut val_gen: OrderedBytesGenerator,
    ) -> (SSTableHandle, usize) {
        let mut writer = ts.table_writer(SsTableId::Wal(0));
        let mut nkeys = 0usize;
        while writer.blocks_written() < n {
            writer
                .add(key_gen.next().as_ref(), Some(val_gen.next().as_ref()))
                .await
                .unwrap();
            nkeys += 1;
        }
        (writer.close().await.unwrap(), nkeys)
    }
}
