use bytes::Bytes;
use std::cmp::min;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::bytes_range::BytesRange;
use crate::db_iter::SeekToKey;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::flatbuffer_types::{SsTableIndex, SsTableIndexOwned};
use crate::{
    block::Block, block_iterator::BlockIterator, iter::KeyValueIterator, tablestore::TableStore,
    types::RowEntry,
};

enum FetchTask {
    InFlight(JoinHandle<Result<VecDeque<Arc<Block>>, SlateDBError>>),
    Finished(VecDeque<Arc<Block>>),
}

pub(crate) struct SstIterator<'a, H: AsRef<SsTableHandle> = &'a SsTableHandle> {
    // We use a trait bound `H` here instead of the concrete type `SstTableHandle` to
    // make it easier for the users of this API to pass table handles wrapped in
    // smart pointers thereby making it easier to workaround some lifetime constraints.
    // An example of this can be found in `DbInner::replay_wal`.
    table: H,
    index: Arc<SsTableIndexOwned>,
    current_iter: Option<BlockIterator<Arc<Block>>>,
    range: BytesRange,
    next_block_idx_to_fetch: usize,
    fetch_tasks: VecDeque<FetchTask>,
    max_fetch_tasks: usize,
    blocks_to_fetch: usize,
    table_store: Arc<TableStore>,
    cache_blocks: bool,
    _marker: PhantomData<&'a H>,
}

impl<'a, H: AsRef<SsTableHandle>> SstIterator<'a, H> {
    fn first_block_with_data_including_or_after_key(index: &SsTableIndex, key: &[u8]) -> usize {
        // search for the block that could contain the key.
        let mut low = 0;
        let mut high = index.block_meta().len() - 1;
        // if the key is less than all the blocks' first key, scan the whole sst
        let mut found_block_id = 0;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_block_first_key = index.block_meta().get(mid).first_key().bytes();
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

    pub(crate) async fn new_from_key(
        table: H,
        table_store: Arc<TableStore>,
        from_key: Bytes,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        Self::new_opts(
            table,
            BytesRange::from(from_key..),
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
            cache_blocks,
        )
        .await
    }

    pub(crate) async fn new_spawn(
        table: H,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        Self::new_opts(
            table,
            BytesRange::from(..),
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            true,
            cache_blocks,
        )
        .await
    }

    pub(crate) async fn new(
        table: H,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        Self::new_opts(
            table,
            BytesRange::from(..),
            table_store,
            max_fetch_tasks,
            blocks_to_fetch,
            false,
            cache_blocks,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new_opts(
        table: H,
        range: BytesRange,
        table_store: Arc<TableStore>,
        max_fetch_tasks: usize,
        blocks_to_fetch: usize,
        spawn: bool,
        cache_blocks: bool,
    ) -> Result<Self, SlateDBError> {
        assert!(max_fetch_tasks > 0);
        assert!(blocks_to_fetch > 0);
        let index = table_store.read_index(table.as_ref()).await?;
        let next_block_idx_to_fetch = match range.start_bound() {
            Unbounded => 0,
            Included(k) | Excluded(k) => {
                Self::first_block_with_data_including_or_after_key(&index.borrow(), k.as_ref())
            }
        };
        let mut iter = Self {
            table,
            index,
            current_iter: None,
            next_block_idx_to_fetch,
            range,
            fetch_tasks: VecDeque::new(),
            max_fetch_tasks,
            blocks_to_fetch,
            table_store,
            cache_blocks,
            _marker: PhantomData,
        };
        if spawn {
            iter.spawn_fetches();
        }
        Ok(iter)
    }

    fn spawn_fetches(&mut self) {
        let num_blocks = self.index.borrow().block_meta().len();
        while self.fetch_tasks.len() < self.max_fetch_tasks
            && self.next_block_idx_to_fetch < num_blocks
        {
            let blocks_to_fetch = min(
                self.blocks_to_fetch,
                num_blocks - self.next_block_idx_to_fetch,
            );
            let table = self.table.as_ref().clone();
            let table_store = self.table_store.clone();
            let blocks_start = self.next_block_idx_to_fetch;
            let blocks_end = self.next_block_idx_to_fetch + blocks_to_fetch;
            let index = self.index.clone();
            let cache_blocks = self.cache_blocks;
            self.fetch_tasks
                .push_back(FetchTask::InFlight(tokio::spawn(async move {
                    table_store
                        .read_blocks_using_index(
                            &table,
                            index,
                            blocks_start..blocks_end,
                            cache_blocks,
                        )
                        .await
                })));
            self.next_block_idx_to_fetch = blocks_end;
        }
    }

    async fn next_iter(&mut self) -> Result<Option<BlockIterator<Arc<Block>>>, SlateDBError> {
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
                            let start_bound = self.range.start_bound();
                            let iter = match start_bound {
                                Unbounded => BlockIterator::from_first_key(block),
                                Included(key) | Excluded(key) => {
                                    BlockIterator::from_key(block, key).await
                                }
                            };
                            return Ok(Some(iter))
                        } else {
                            self.fetch_tasks.pop_front();
                        }
                    }
                }
            } else {
                assert!(self.fetch_tasks.is_empty());
                assert_eq!(
                    self.next_block_idx_to_fetch,
                    self.index.borrow().block_meta().len()
                );
                return Ok(None);
            }
        }
    }

    pub(crate) fn range_covers_key(&self, key: &Bytes) -> bool {
        self.table.as_ref().range_covers_key(key)
    }

    fn end_iteration(&mut self) {
        let num_blocks = self.index.borrow().block_meta().len();
        self.next_block_idx_to_fetch = num_blocks;
        self.current_iter = None
    }
}

impl<'a, H: AsRef<SsTableHandle>> KeyValueIterator for SstIterator<'a, H> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
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
                Some(kv) if self.range.contains(&kv.key) => return Ok(Some(kv)),
                Some(kv) => match self.range.end_bound() {
                    Unbounded => continue,
                    Included(end_key) | Excluded(end_key) => {
                        if kv.key > end_key {
                            self.end_iteration();
                            return Ok(None);
                        } else {
                            continue;
                        }
                    }
                },
                None => {
                    self.current_iter = None;
                    // We have exhausted the current block, but not necessarily the entire SST,
                    // so we fall back to the top to check if we have more blocks to read.
                }
            }
        }
    }
}

impl<'a, H: AsRef<SsTableHandle>> SeekToKey for SstIterator<'a, H> {
    async fn seek(&mut self, next_key: &Bytes) -> Result<(), SlateDBError> {
        if !self.range_covers_key(next_key) {
            self.end_iteration();
            return Ok(());
        }

        loop {
            let current_iter = if let Some(current_iter) = self.current_iter.as_mut() {
                current_iter
            } else if let Some(next_iter) = self.next_iter().await? {
                self.current_iter.insert(next_iter)
            } else {
                return Ok(());
            };
            current_iter.seek(next_key).await?;
            if current_iter.is_empty() {
                self.current_iter = None;
            } else {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::SsTableId;
    use crate::sst::SsTableFormat;
    use crate::test_utils::{assert_kv, gen_attrs, OrderedBytesGenerator};
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_one_block_sst_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let mut builder = table_store.table_builder();
        builder
            .add_kv(b"key1", Some(b"value1"), gen_attrs(1))
            .unwrap();
        builder
            .add_kv(b"key2", Some(b"value2"), gen_attrs(2))
            .unwrap();
        builder
            .add_kv(b"key3", Some(b"value3"), gen_attrs(3))
            .unwrap();
        builder
            .add_kv(b"key4", Some(b"value4"), gen_attrs(4))
            .unwrap();
        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle).await.unwrap();
        assert_eq!(index.borrow().block_meta().len(), 1);

        let mut iter = SstIterator::new(&sst_handle, table_store.clone(), 1, 1, true)
            .await
            .unwrap();
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
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let mut builder = table_store.table_builder();

        for i in 0..1000 {
            builder
                .add_kv(
                    format!("key{}", i).as_bytes(),
                    Some(format!("value{}", i).as_bytes()),
                    gen_attrs(i),
                )
                .unwrap();
        }

        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle).await.unwrap();
        assert_eq!(index.borrow().block_meta().len(), 10);

        let mut iter = SstIterator::new(&sst_handle, table_store.clone(), 3, 3, true)
            .await
            .unwrap();
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
        let format = SsTableFormat {
            block_size: 128,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
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
                SstIterator::new_from_key(&sst, table_store.clone(), from_key.clone(), 1, 1, false)
                    .await
                    .unwrap();
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
        let format = SsTableFormat {
            block_size: 128,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let first_key = [b'b'; 16];
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'y');
        let mut expected_key_gen = key_gen.clone();
        let first_val = [2u8; 16];
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let mut expected_val_gen = val_gen.clone();
        let (sst, nkeys) = build_sst_with_n_blocks(2, table_store.clone(), key_gen, val_gen).await;

        let mut iter = SstIterator::new_from_key(
            &sst,
            table_store.clone(),
            Bytes::from_static(&[b'a'; 16]),
            1,
            1,
            false,
        )
        .await
        .unwrap();

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
        let format = SsTableFormat {
            block_size: 128,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store,
            format,
            root_path.clone(),
            None,
        ));
        let first_key = [b'b'; 16];
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'y');
        let first_val = [2u8; 16];
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let (sst, _) = build_sst_with_n_blocks(2, table_store.clone(), key_gen, val_gen).await;

        let mut iter = SstIterator::new_from_key(
            &sst,
            table_store.clone(),
            Bytes::from_static(&[b'z'; 16]),
            1,
            1,
            false,
        )
        .await
        .unwrap();

        assert!(iter.next().await.unwrap().is_none());
    }

    async fn build_sst_with_n_blocks(
        n: usize,
        ts: Arc<TableStore>,
        mut key_gen: OrderedBytesGenerator,
        mut val_gen: OrderedBytesGenerator,
    ) -> (SsTableHandle, usize) {
        let mut writer = ts.table_writer(SsTableId::Wal(0));
        let mut nkeys = 0usize;
        while writer.blocks_written() < n {
            let entry = RowEntry::new(key_gen.next(), Some(val_gen.next()), 0, None, None);
            writer.add(entry).await.unwrap();
            nkeys += 1;
        }
        (writer.close().await.unwrap(), nkeys)
    }
}
