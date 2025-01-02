use bytes::Bytes;
use std::cmp::min;
use std::collections::VecDeque;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::bytes_range::BytesRange;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::flatbuffer_types::{SsTableIndex, SsTableIndexOwned};
use crate::iter::SeekToKey;
use crate::{
    block::Block, block_iterator::BlockIterator, iter::KeyValueIterator, tablestore::TableStore,
    types::RowEntry,
};

enum FetchTask {
    InFlight(JoinHandle<Result<VecDeque<Arc<Block>>, SlateDBError>>),
    Finished(VecDeque<Arc<Block>>),
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct SstIteratorOptions {
    pub(crate) max_fetch_tasks: usize,
    pub(crate) blocks_to_fetch: usize,
    pub(crate) cache_blocks: bool,
    pub(crate) eager_spawn: bool,
}

impl Default for SstIteratorOptions {
    fn default() -> Self {
        SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: 1,
            cache_blocks: false,
            eager_spawn: false,
        }
    }
}

/// This enum encapsulates access to an SST and corresponding ownership requirements.
/// For example, [`SstView::Owned`] allows the table handle to be owned, which is
/// needed for [`crate::db::Db::scan`] since it returns the iterator, while [`SstView::Key`]
/// accommodates single-key  access by reference which is needed for [`crate::db::Db::get`].
pub(crate) enum SstView<'a> {
    Owned(SsTableHandle, BytesRange),
    Borrowed(&'a SsTableHandle, (Bound<&'a [u8]>, Bound<&'a [u8]>)),
}

impl SstView<'_> {
    fn start_key(&self) -> Bound<&[u8]> {
        match self {
            SstView::Owned(_, r) => r.start_bound().map(|b| b.as_ref()),
            SstView::Borrowed(_, (start, _)) => *start,
        }
    }

    fn end_key(&self) -> Bound<&[u8]> {
        match self {
            SstView::Owned(_, r) => r.end_bound().map(|b| b.as_ref()),
            SstView::Borrowed(_, (_, end)) => *end,
        }
    }

    fn table_as_ref(&self) -> &SsTableHandle {
        match self {
            SstView::Owned(t, _) => t,
            SstView::Borrowed(t, _) => t,
        }
    }

    /// Check whether a key is contained within this view.
    fn contains(&self, key: &[u8]) -> bool {
        match self {
            SstView::Owned(_, r) => r.contains(key),
            SstView::Borrowed(_, r) => {
                <(Bound<&[u8]>, Bound<&[u8]>) as RangeBounds<[u8]>>::contains::<[u8]>(r, key)
            }
        }
    }

    /// Check whether a key exceeds the range of this view.
    fn key_exceeds(&self, key: &[u8]) -> bool {
        match self.end_key() {
            Included(end) => key > end,
            Excluded(end) => key >= end,
            Unbounded => false,
        }
    }
}

struct IteratorState {
    initialized: bool,
    current_iter: Option<BlockIterator<Arc<Block>>>,
}

impl IteratorState {
    fn new() -> Self {
        Self {
            initialized: false,
            current_iter: None,
        }
    }

    fn is_finished(&self) -> bool {
        self.initialized && self.current_iter.is_none()
    }

    fn advance(&mut self, iterator: BlockIterator<Arc<Block>>) {
        self.initialized = true;
        self.current_iter = Some(iterator);
    }

    fn stop(&mut self) {
        self.initialized = true;
        self.current_iter = None;
    }
}

pub(crate) struct SstIterator<'a> {
    view: SstView<'a>,
    index: Arc<SsTableIndexOwned>,
    state: IteratorState,
    next_block_idx_to_fetch: usize,
    block_idx_range: Range<usize>,
    fetch_tasks: VecDeque<FetchTask>,
    table_store: Arc<TableStore>,
    options: SstIteratorOptions,
}

impl<'a> SstIterator<'a> {
    pub(crate) async fn new(
        view: SstView<'a>,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        assert!(options.max_fetch_tasks > 0);
        assert!(options.blocks_to_fetch > 0);
        let index = table_store.read_index(view.table_as_ref()).await?;
        let block_idx_range = SstIterator::blocks_covering_view(&index.borrow(), &view);

        let mut iter = Self {
            view,
            index,
            state: IteratorState::new(),
            next_block_idx_to_fetch: block_idx_range.start,
            block_idx_range,
            fetch_tasks: VecDeque::new(),
            table_store,
            options,
        };

        if options.eager_spawn {
            iter.spawn_fetches();
        }
        Ok(iter)
    }

    pub(crate) async fn new_owned<T: RangeBounds<Bytes>>(
        range: T,
        table: SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let view = SstView::Owned(table, BytesRange::from(range));
        Self::new(view, table_store.clone(), options).await
    }

    pub(crate) async fn new_borrowed<T: RangeBounds<&'a [u8]>>(
        range: T,
        table: &'a SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        let view = SstView::Borrowed(table, bounds);
        Self::new(view, table_store.clone(), options).await
    }

    pub(crate) async fn for_key(
        table: &'a SsTableHandle,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        Self::new_borrowed(key..=key, table, table_store, options).await
    }

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

    fn blocks_covering_view(index: &SsTableIndex, view: &SstView) -> Range<usize> {
        let start_block_id = match view.start_key() {
            Included(k) | Excluded(k) => {
                Self::first_block_with_data_including_or_after_key(index, k)
            }
            Unbounded => 0,
        };

        let end_block_id_exclusive = match view.end_key() {
            Included(k) => Self::first_block_with_data_including_or_after_key(index, k) + 1,
            Excluded(k) => {
                let block_index = Self::first_block_with_data_including_or_after_key(index, k);
                let block = index.block_meta().get(block_index);
                if k == block.first_key().bytes() {
                    block_index
                } else {
                    block_index + 1
                }
            }
            Unbounded => index.block_meta().len(),
        };

        start_block_id..end_block_id_exclusive
    }

    fn spawn_fetches(&mut self) {
        while self.fetch_tasks.len() < self.options.max_fetch_tasks
            && self.block_idx_range.contains(&self.next_block_idx_to_fetch)
        {
            let blocks_to_fetch = min(
                self.options.blocks_to_fetch,
                self.block_idx_range.end - self.next_block_idx_to_fetch,
            );
            let table = self.view.table_as_ref().clone();
            let table_store = self.table_store.clone();
            let blocks_start = self.next_block_idx_to_fetch;
            let blocks_end = self.next_block_idx_to_fetch + blocks_to_fetch;
            let index = self.index.clone();
            let cache_blocks = self.options.cache_blocks;
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
                            return Ok(Some(BlockIterator::new(block)));
                        } else {
                            self.fetch_tasks.pop_front();
                        }
                    }
                }
            } else {
                assert!(self.fetch_tasks.is_empty());
                assert_eq!(self.next_block_idx_to_fetch, self.block_idx_range.end);
                return Ok(None);
            }
        }
    }

    async fn advance_block(&mut self) -> Result<(), SlateDBError> {
        if !self.state.is_finished() {
            if let Some(mut iter) = self.next_iter().await? {
                match self.view.start_key() {
                    Included(start_key) | Excluded(start_key) => iter.seek(start_key).await?,
                    Unbounded => (),
                }
                self.state.advance(iter);
            } else {
                self.state.stop();
            }
        }
        Ok(())
    }

    fn stop(&mut self) {
        let num_blocks = self.index.borrow().block_meta().len();
        self.next_block_idx_to_fetch = num_blocks;
        self.state.stop();
    }
}

impl KeyValueIterator for SstIterator<'_> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while !self.state.is_finished() {
            let next_entry = if let Some(iter) = self.state.current_iter.as_mut() {
                iter.next_entry().await?
            } else {
                None
            };

            match next_entry {
                Some(kv) => {
                    if self.view.contains(&kv.key) {
                        return Ok(Some(kv));
                    } else if self.view.key_exceeds(&kv.key) {
                        self.stop()
                    }
                }
                None => self.advance_block().await?,
            }
        }
        Ok(None)
    }
}

impl SeekToKey for SstIterator<'_> {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if !self.view.contains(next_key) {
            return Err(SlateDBError::InvalidArgument {
                msg: "FIXME".to_string(),
            });
        }

        while !self.state.is_finished() {
            if let Some(iter) = self.state.current_iter.as_mut() {
                iter.seek(next_key).await?;
                if !iter.is_empty() {
                    break;
                }
            }
            self.advance_block().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::OrderedBytesGenerator;
    use crate::db_state::SsTableId;
    use crate::sst::SsTableFormat;
    use crate::test_utils::{assert_kv, gen_attrs};
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
        builder.add_value(b"key1", b"value1", gen_attrs(1)).unwrap();
        builder.add_value(b"key2", b"value2", gen_attrs(2)).unwrap();
        builder.add_value(b"key3", b"value3", gen_attrs(3)).unwrap();
        builder.add_value(b"key4", b"value4", gen_attrs(4)).unwrap();
        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle).await.unwrap();
        assert_eq!(index.borrow().block_meta().len(), 1);

        // TODO: Need to verify argument types
        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            ..SstIteratorOptions::default()
        };
        let mut iter =
            SstIterator::new_owned(.., sst_handle, table_store.clone(), sst_iter_options)
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
                .add_value(
                    format!("key{}", i).as_bytes(),
                    format!("value{}", i).as_bytes(),
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

        // TODO: verify cache_blocks=true is intended
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 3,
            blocks_to_fetch: 3,
            cache_blocks: true,
            ..SstIteratorOptions::default()
        };
        let mut iter =
            SstIterator::new_owned(.., sst_handle, table_store.clone(), sst_iter_options)
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
            let mut iter = SstIterator::new_borrowed(
                from_key.as_ref()..,
                &sst,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
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

        let mut iter = SstIterator::new_borrowed(
            [b'a'; 16].as_ref()..,
            &sst,
            table_store.clone(),
            SstIteratorOptions::default(),
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

        let mut iter = SstIterator::new_borrowed(
            [b'z'; 16].as_ref()..,
            &sst,
            table_store.clone(),
            SstIteratorOptions::default(),
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
            let entry = RowEntry::new_value(key_gen.next().as_ref(), val_gen.next().as_ref(), 0);
            writer.add(entry).await.unwrap();
            nkeys += 1;
        }
        (writer.close().await.unwrap(), nkeys)
    }
}
