use crate::bytes_range::BytesRange;
use crate::db_state::SortedRun;
use crate::db_state::SsTableHandle;
use crate::error::SlateDBError;
use crate::flatbuffer_types::{SsTableIndex, SsTableIndexOwned};
use crate::mem_table::VecDequeKeyValueIterator;
use crate::row_codec::SstRowCodecV0;
use crate::types::RowEntry;
use crate::types::{KeyValue, ValueDeletable};
use crate::utils::is_not_expired;
use crate::{block::Block, tablestore::TableStore};
use bytes::Buf;
use bytes::Bytes;
use std::cmp::min;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;
use tokio::task::JoinHandle;
use IterationOrder::Ascending;
use IterationOrder::Descending;

#[derive(Clone, Copy, Debug)]
pub(crate) enum IterationOrder {
    Ascending,
    #[allow(dead_code)]
    Descending,
}

/// Note: this is intentionally its own trait instead of an Iterator<Item=KeyValue>,
/// because next will need to be made async to support SSTs, which are loaded over
/// the network.
/// See: https://github.com/slatedb/slatedb/issues/12
pub trait KeyValueIterator {
    /// Returns the next non-deleted key-value pair in the iterator.
    async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        loop {
            let entry = self.next_entry().await?;
            if let Some(kv) = entry {
                match kv.value {
                    ValueDeletable::Value(v) => {
                        return Ok(Some(KeyValue {
                            key: kv.key,
                            value: v,
                        }))
                    }
                    ValueDeletable::Merge(_) => todo!(),
                    ValueDeletable::Tombstone => continue,
                }
            } else {
                return Ok(None);
            }
        }
    }

    /// Returns the next entry in the iterator, which may be a key-value pair or
    /// a tombstone of a deleted key-value pair.
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError>;
}

pub(crate) trait SeekToKey {
    /// Seek to the next (inclusive) key
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError>;
}

pub trait BlockLike {
    fn data(&self) -> &Bytes;
    fn offsets(&self) -> &[u16];
}

impl BlockLike for Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        &self.offsets
    }
}

impl BlockLike for &Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        &self.offsets
    }
}

impl BlockLike for Arc<Block> {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        &self.offsets
    }
}

pub struct BlockIterator<B: BlockLike> {
    block: B,
    off_off: usize,
    // first key in the block, because slateDB does not support multi version of keys
    // so we use `Bytes` temporarily
    first_key: Bytes,
    ordering: IterationOrder,
}

impl<B: BlockLike> KeyValueIterator for BlockIterator<B> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let result = self.load_at_current_off();
        match result {
            Ok(None) => Ok(None),
            Ok(key_value) => {
                self.advance();
                Ok(key_value)
            }
            Err(e) => Err(e),
        }
    }
}

impl<B: BlockLike> SeekToKey for BlockIterator<B> {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        loop {
            let result = self.load_at_current_off();
            match result {
                Ok(None) => return Ok(()),
                Ok(Some(kv)) => {
                    if kv.key < next_key {
                        self.advance();
                    } else {
                        return Ok(());
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl<B: BlockLike> BlockIterator<B> {
    pub fn new(block: B, ordering: IterationOrder) -> Self {
        BlockIterator {
            first_key: BlockIterator::decode_first_key(&block),
            block,
            off_off: 0,
            ordering,
        }
    }

    pub fn new_ascending(block: B) -> Self {
        Self::new(block, Ascending)
    }

    fn advance(&mut self) {
        self.off_off += 1;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.off_off >= self.block.offsets().len()
    }

    fn load_at_current_off(&self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.is_empty() {
            return Ok(None);
        }
        let off_off = match self.ordering {
            Ascending => self.off_off,
            Descending => self.block.offsets().len() - 1 - self.off_off,
        };

        let off = self.block.offsets()[off_off];
        let off_usz = off as usize;
        // TODO: bounds checks to avoid panics? (paulgb)
        let mut cursor = self.block.data().slice(off_usz..);
        let codec = SstRowCodecV0::new();
        let sst_row = codec.decode(&mut cursor)?;
        Ok(Some(RowEntry::new(
            sst_row.restore_full_key(&self.first_key),
            sst_row.value,
            sst_row.seq,
            sst_row.create_ts,
            sst_row.expire_ts,
        )))
    }

    pub fn decode_first_key(block: &B) -> Bytes {
        let mut buf = block.data().slice(..);
        let overlap_len = buf.get_u16() as usize;
        assert_eq!(overlap_len, 0, "first key overlap should be 0");
        let key_len = buf.get_u16() as usize;
        let first_key = &buf[..key_len];
        Bytes::copy_from_slice(first_key)
    }
}

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
            cache_blocks: true,
            eager_spawn: false,
        }
    }
}

/// This enum encapsulates access to an SST and corresponding ownership requirements.
/// For example, [`SstView::Owned`] allows the table handle to be owned, which is
/// needed for [`crate::db::Db::scan`] since it returns the iterator, while [`SstView::Borrowed`]
/// accommodates access by reference which is useful for [`crate::db::Db::get`].
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
                            return Ok(Some(BlockIterator::new_ascending(block)));
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
                msg: format!("Cannot seek to a key '{:?}' which is outside the iterator range (start: {:?}, end: {:?})",
                             next_key, self.view.start_key(), self.view.end_key())
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

#[derive(Debug)]
enum SortedRunView<'a> {
    Owned(VecDeque<SsTableHandle>, BytesRange),
    Borrowed(
        VecDeque<&'a SsTableHandle>,
        (Bound<&'a [u8]>, Bound<&'a [u8]>),
    ),
}

impl<'a> SortedRunView<'a> {
    fn pop_sst(&mut self) -> Option<SstView<'a>> {
        match self {
            SortedRunView::Owned(tables, r) => tables
                .pop_front()
                .map(|table| SstView::Owned(table, r.clone())),
            SortedRunView::Borrowed(tables, r) => {
                tables.pop_front().map(|table| SstView::Borrowed(table, *r))
            }
        }
    }

    pub(crate) async fn build_next_iter(
        &mut self,
        table_store: Arc<TableStore>,
        sst_iterator_options: SstIteratorOptions,
    ) -> Result<Option<SstIterator<'a>>, SlateDBError> {
        let next_iter = if let Some(view) = self.pop_sst() {
            Some(SstIterator::new(view, table_store.clone(), sst_iterator_options).await?)
        } else {
            None
        };
        Ok(next_iter)
    }

    fn peek_next_table(&self) -> Option<&SsTableHandle> {
        match self {
            SortedRunView::Owned(tables, _) => tables.front(),
            SortedRunView::Borrowed(tables, _) => tables.front().copied(),
        }
    }
}

pub(crate) struct SortedRunIterator<'a> {
    table_store: Arc<TableStore>,
    sst_iter_options: SstIteratorOptions,
    view: SortedRunView<'a>,
    current_iter: Option<SstIterator<'a>>,
}

impl<'a> SortedRunIterator<'a> {
    async fn new(
        view: SortedRunView<'a>,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let mut res = Self {
            table_store,
            sst_iter_options,
            view,
            current_iter: None,
        };
        res.advance_table().await?;
        Ok(res)
    }

    pub(crate) async fn new_owned<T: RangeBounds<Bytes>>(
        range: T,
        sorted_run: SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let range = BytesRange::from(range);
        let tables = sorted_run.into_tables_covering_range(range.as_ref());
        let view = SortedRunView::Owned(tables, range);
        SortedRunIterator::new(view, table_store, sst_iter_options).await
    }

    pub(crate) async fn new_borrowed<T: RangeBounds<&'a [u8]>>(
        range: T,
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let range = (range.start_bound().cloned(), range.end_bound().cloned());
        let tables = sorted_run.tables_covering_range(range);
        let view = SortedRunView::Borrowed(tables, range);
        SortedRunIterator::new(view, table_store, sst_iter_options).await
    }

    pub(crate) async fn for_key(
        sorted_run: &'a SortedRun,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<SortedRunIterator<'a>, SlateDBError> {
        Self::new_borrowed(key..=key, sorted_run, table_store, sst_iter_options).await
    }

    async fn advance_table(&mut self) -> Result<(), SlateDBError> {
        self.current_iter = self
            .view
            .build_next_iter(self.table_store.clone(), self.sst_iter_options)
            .await?;
        Ok(())
    }
}

impl KeyValueIterator for SortedRunIterator<'_> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(iter) = &mut self.current_iter {
            if let Some(kv) = iter.next_entry().await? {
                return Ok(Some(kv));
            } else {
                self.advance_table().await?;
            }
        }
        Ok(None)
    }
}

impl SeekToKey for SortedRunIterator<'_> {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        while let Some(next_table) = self.view.peek_next_table() {
            let next_table_first_key = next_table.info.first_key.as_ref();
            match next_table_first_key {
                Some(key) if key < next_key => self.advance_table().await?,
                _ => break,
            }
        }
        if let Some(iter) = &mut self.current_iter {
            iter.seek(next_key).await?;
        }
        Ok(())
    }
}

pub(crate) struct TwoMergeIterator<T1: KeyValueIterator, T2: KeyValueIterator> {
    iterator1: (T1, Option<RowEntry>),
    iterator2: (T2, Option<RowEntry>),
}

impl<T1: KeyValueIterator, T2: KeyValueIterator> TwoMergeIterator<T1, T2> {
    pub(crate) async fn new(mut iterator1: T1, mut iterator2: T2) -> Result<Self, SlateDBError> {
        let next1 = iterator1.next_entry().await?;
        let next2 = iterator2.next_entry().await?;
        Ok(Self {
            iterator1: (iterator1, next1),
            iterator2: (iterator2, next2),
        })
    }

    async fn advance1(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.iterator1.1.is_none() {
            return Ok(None);
        }
        Ok(std::mem::replace(
            &mut self.iterator1.1,
            self.iterator1.0.next_entry().await?,
        ))
    }

    async fn advance2(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.iterator2.1.is_none() {
            return Ok(None);
        }
        Ok(std::mem::replace(
            &mut self.iterator2.1,
            self.iterator2.0.next_entry().await?,
        ))
    }

    fn peek1(&self) -> Option<&RowEntry> {
        self.iterator1.1.as_ref()
    }

    fn peek2(&self) -> Option<&RowEntry> {
        self.iterator2.1.as_ref()
    }

    fn peek_inner(&self) -> Option<&RowEntry> {
        match (self.peek1(), self.peek2()) {
            (None, None) => None,
            (Some(v1), None) => Some(v1),
            (None, Some(v2)) => Some(v2),
            (Some(v1), Some(v2)) => {
                if v1.key < v2.key {
                    Some(v1)
                } else {
                    Some(v2)
                }
            }
        }
    }

    async fn advance_inner(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match (self.peek1(), self.peek2()) {
            (None, None) => Ok(None),
            (Some(_), None) => self.advance1().await,
            (None, Some(_)) => self.advance2().await,
            (Some(next1), Some(next2)) => {
                if next1.key < next2.key {
                    self.advance1().await
                } else {
                    self.advance2().await
                }
            }
        }
    }
}

impl<T1, T2> TwoMergeIterator<T1, T2>
where
    T1: KeyValueIterator + SeekToKey,
    T2: KeyValueIterator + SeekToKey,
{
    async fn seek1(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &self.iterator1.1 {
            None => Ok(()),
            Some(val) => {
                if val.key < next_key {
                    self.iterator1.0.seek(next_key).await?;
                    self.iterator1.1 = self.iterator1.0.next_entry().await?;
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn seek2(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &self.iterator2.1 {
            None => Ok(()),
            Some(val) => {
                if val.key < next_key {
                    self.iterator2.0.seek(next_key).await?;
                    self.iterator2.1 = self.iterator2.0.next_entry().await?;
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl<T1, T2> SeekToKey for TwoMergeIterator<T1, T2>
where
    T1: KeyValueIterator + SeekToKey,
    T2: KeyValueIterator + SeekToKey,
{
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.seek1(next_key).await?;
        self.seek2(next_key).await
    }
}

impl<T1: KeyValueIterator, T2: KeyValueIterator> KeyValueIterator for TwoMergeIterator<T1, T2> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let mut current_kv = match self.advance_inner().await? {
            Some(kv) => kv,
            None => return Ok(None),
        };
        while let Some(peeked_kv) = self.peek_inner() {
            if peeked_kv.key != current_kv.key {
                break;
            }
            if peeked_kv.seq > current_kv.seq {
                current_kv = peeked_kv.clone();
            }
            self.advance_inner().await?;
        }
        Ok(Some(current_kv))
    }
}

struct MergeIteratorHeapEntry<T: KeyValueIterator> {
    next_kv: RowEntry,
    index: u32,
    iterator: T,
}

impl<T: KeyValueIterator + SeekToKey> MergeIteratorHeapEntry<T> {
    /// Seek the iterator and return a new heap entry
    async fn seek(
        mut self,
        next_key: &[u8],
    ) -> Result<Option<MergeIteratorHeapEntry<T>>, SlateDBError> {
        if self.next_kv.key >= next_key {
            Ok(Some(self))
        } else {
            self.iterator.seek(next_key).await?;
            if let Some(next_kv) = self.iterator.next_entry().await? {
                Ok(Some(MergeIteratorHeapEntry {
                    next_kv,
                    index: self.index,
                    iterator: self.iterator,
                }))
            } else {
                Ok(None)
            }
        }
    }
}

impl<T: KeyValueIterator> Eq for MergeIteratorHeapEntry<T> {}

impl<T: KeyValueIterator> PartialEq<Self> for MergeIteratorHeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.next_kv == other.next_kv
    }
}

impl<T: KeyValueIterator> PartialOrd<Self> for MergeIteratorHeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: KeyValueIterator> Ord for MergeIteratorHeapEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we'll wrap a Reverse in the BinaryHeap, so the cmp here is in increasing order.
        // after Reverse is wrapped, it will return the entries with higher seqnum first.
        (&self.next_kv.key, self.next_kv.seq).cmp(&(&other.next_kv.key, other.next_kv.seq))
    }
}

pub(crate) struct MergeIterator<T: KeyValueIterator> {
    current: Option<MergeIteratorHeapEntry<T>>,
    iterators: BinaryHeap<Reverse<MergeIteratorHeapEntry<T>>>,
}

impl<T: KeyValueIterator> MergeIterator<T> {
    pub(crate) async fn new(mut iterators: VecDeque<T>) -> Result<Self, SlateDBError> {
        let mut heap = BinaryHeap::new();
        let mut index = 0;
        while let Some(mut iterator) = iterators.pop_front() {
            if let Some(kv) = iterator.next_entry().await? {
                heap.push(Reverse(MergeIteratorHeapEntry {
                    next_kv: kv,
                    index,
                    iterator,
                }));
            }
            index += 1;
        }
        Ok(Self {
            current: heap.pop().map(|r| r.0),
            iterators: heap,
        })
    }

    fn peek(&self) -> Option<&RowEntry> {
        self.current.as_ref().map(|c| &c.next_kv)
    }

    async fn advance(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(mut iterator_state) = self.current.take() {
            let current_kv = iterator_state.next_kv;
            if let Some(kv) = iterator_state.iterator.next_entry().await? {
                iterator_state.next_kv = kv;
                self.iterators.push(Reverse(iterator_state));
            }
            self.current = self.iterators.pop().map(|r| r.0);
            return Ok(Some(current_kv));
        }
        Ok(None)
    }
}

impl<T: KeyValueIterator> KeyValueIterator for MergeIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let mut current_kv = match self.advance().await? {
            Some(kv) => kv,
            None => return Ok(None),
        };

        // iterate until we find a key that is not the same as the current key,
        // find the one with the highest seqnum.
        while let Some(peeked_entry) = self.peek() {
            if peeked_entry.key != current_kv.key {
                break;
            }
            if peeked_entry.seq > current_kv.seq {
                current_kv = peeked_entry.clone();
            }
            self.advance().await?;
        }
        Ok(Some(current_kv))
    }
}

impl<T: KeyValueIterator + SeekToKey> SeekToKey for MergeIterator<T> {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        let mut seek_futures = VecDeque::new();
        if let Some(iterator) = self.current.take() {
            seek_futures.push_back(iterator.seek(next_key))
        }

        for iterator in self.iterators.drain() {
            seek_futures.push_back(iterator.0.seek(next_key));
        }

        for seek_result in futures::future::join_all(seek_futures).await {
            if let Some(seeked_iterator) = seek_result? {
                self.iterators.push(Reverse(seeked_iterator));
            }
        }

        self.current = self.iterators.pop().map(|r| r.0);
        Ok(())
    }
}

pub(crate) struct FilterIterator<T: KeyValueIterator> {
    iterator: T,
    predicate: Box<dyn Fn(&RowEntry) -> bool + Send>,
}

impl<T: KeyValueIterator> FilterIterator<T> {
    pub(crate) fn new(iterator: T, predicate: Box<dyn Fn(&RowEntry) -> bool + Send>) -> Self {
        Self {
            predicate,
            iterator,
        }
    }

    pub(crate) fn wrap_ttl_filter_iterator(iterator: T, now: i64) -> Self {
        let filter_entry = move |entry: &RowEntry| is_not_expired(entry, now);
        Self::new(iterator, Box::new(filter_entry))
    }
}

impl<T: KeyValueIterator> KeyValueIterator for FilterIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(entry) = self.iterator.next_entry().await? {
            if (self.predicate)(&entry) {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}

type ScanIterator<'a> = TwoMergeIterator<
    VecDequeKeyValueIterator,
    TwoMergeIterator<MergeIterator<SstIterator<'a>>, MergeIterator<SortedRunIterator<'a>>>,
>;

pub struct DbIterator<'a> {
    range: BytesRange,
    iter: ScanIterator<'a>,
    invalidated_error: Option<SlateDBError>,
    last_key: Option<Bytes>,
}

impl<'a> DbIterator<'a> {
    pub(crate) async fn new(
        range: BytesRange,
        mem_iter: VecDequeKeyValueIterator,
        l0_iters: VecDeque<SstIterator<'a>>,
        sr_iters: VecDeque<SortedRunIterator<'a>>,
    ) -> Result<Self, SlateDBError> {
        let (l0_iter, sr_iter) =
            tokio::join!(MergeIterator::new(l0_iters), MergeIterator::new(sr_iters),);
        let sst_iter = TwoMergeIterator::new(l0_iter?, sr_iter?).await?;
        let iter = TwoMergeIterator::new(mem_iter, sst_iter).await?;
        Ok(DbIterator {
            range,
            iter,
            invalidated_error: None,
            last_key: None,
        })
    }

    /// Get the next record in the scan.
    ///
    /// # Errors
    ///
    /// Returns [`SlateDBError::InvalidatedIterator`] if the iterator has been invalidated
    ///  due to an underlying error
    pub async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        if let Some(error) = self.invalidated_error.clone() {
            Err(SlateDBError::InvalidatedIterator(Box::new(error)))
        } else {
            let result = self.iter.next().await;
            self.maybe_invalidate(result)
        }
    }

    fn maybe_invalidate<T: Clone>(
        &mut self,
        result: Result<T, SlateDBError>,
    ) -> Result<T, SlateDBError> {
        if let Err(error) = &result {
            self.invalidated_error = Some(error.clone());
        }
        result
    }

    /// Seek ahead to the next key. The next key must be larger than the
    /// last key returned by the iterator and less than the end bound specified
    /// in the `scan` arguments.
    ///
    /// After a successful seek, the iterator will return the next record
    /// with a key greater than or equal to `next_key`.
    ///
    /// # Errors
    ///
    /// Returns [`SlateDBError::InvalidArgument`] in the following cases:
    ///
    /// - if `next_key` comes before the current iterator position
    /// - if `next_key` is beyond the upper bound specified in the original
    ///   [`crate::db::Db::scan`] parameters
    ///
    /// Returns [`SlateDBError::InvalidatedIterator`] if the iterator has been
    ///  invalidated in order to reclaim resources.
    pub async fn seek<K: AsRef<[u8]>>(&mut self, next_key: K) -> Result<(), SlateDBError> {
        let next_key = next_key.as_ref();
        if let Some(error) = self.invalidated_error.clone() {
            Err(SlateDBError::InvalidatedIterator(Box::new(error)))
        } else if !self.range.contains(&next_key) {
            Err(SlateDBError::InvalidArgument {
                msg: format!(
                    "Cannot seek to a key '{:?}' which is outside the iterator range {:?}",
                    next_key, self.range
                ),
            })
        } else if self
            .last_key
            .clone()
            .is_some_and(|last_key| next_key <= last_key)
        {
            Err(SlateDBError::InvalidArgument {
                msg: "Cannot seek to a key less than the last returned key".to_string(),
            })
        } else {
            let result = self.iter.seek(next_key).await;
            self.maybe_invalidate(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockBuilder;
    use crate::bytes_generator::OrderedBytesGenerator;
    use crate::bytes_range::BytesRange;
    use crate::db_state::SsTableId;
    use crate::mem_table::VecDequeKeyValueIterator;
    use crate::proptest_util::{arbitrary, sample};
    use crate::sst::SsTableFormat;
    use crate::test_utils::TestIterator;
    use crate::test_utils::{assert_iterator, assert_next_entry, gen_empty_attrs};
    use crate::test_utils::{assert_kv, gen_attrs};
    use crate::types::RowEntry;
    use crate::{proptest_util, test_utils};
    use bytes::Bytes;
    use bytes::{BufMut, BytesMut};
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use proptest::test_runner::TestRng;
    use rand::distributions::uniform::SampleRange;
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::vec;
    use tokio::runtime::Runtime;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_block_iterator() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"donkey", b"kong");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_block_iterator_seek_to_existing_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"kratos").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_block_iterator_seek_to_nonexisting_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"ka").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_block_iterator_seek_to_key_beyond_last_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"zzz").await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_block_iterator_seek_to_key_skips_records_prior_to_next_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_empty_attrs()));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_empty_attrs()));
        assert!(block_builder.add_value(b"super", b"mario", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_next_entry(&mut iter, &RowEntry::new_value(b"donkey", b"kong", 0)).await;
        iter.seek(b"s").await.unwrap();
        assert_iterator(&mut iter, vec![RowEntry::new_value(b"super", b"mario", 0)]).await;
    }

    #[tokio::test]
    async fn test_block_iterator_seek_to_key_with_iterator_at_seek_point() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_empty_attrs()));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_empty_attrs()));
        assert!(block_builder.add_value(b"super", b"mario", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_next_entry(&mut iter, &RowEntry::new_value(b"donkey", b"kong", 0)).await;
        iter.seek(b"kratos").await.unwrap();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"kratos", b"atreus", 0),
                RowEntry::new_value(b"super", b"mario", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_block_iterator_seek_to_key_beyond_last_key_in_block() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        iter.seek(b"zelda".as_ref()).await.unwrap();
        assert_iterator(&mut iter, Vec::new()).await;
    }

    #[test]
    fn block_iterator_should_iterate_arbitrary_range() {
        let mut runner = proptest_util::runner::new(file!(), None);
        let runtime = Runtime::new().unwrap();
        let sample_table = sample::table(runner.rng(), 5, 10);

        let mut block_builder = BlockBuilder::new(1024);
        for (key, value) in &sample_table {
            block_builder.add_value(key, value, gen_empty_attrs());
        }
        let block = Arc::new(block_builder.build().unwrap());

        runner
            .run(&arbitrary::iteration_order(), |ordering| {
                let mut iter = BlockIterator::new(block.clone(), ordering);
                runtime.block_on(test_utils::assert_ranged_kv_scan(
                    &sample_table,
                    &BytesRange::from(..),
                    ordering,
                    &mut iter,
                ));
                Ok(())
            })
            .unwrap();
    }

    #[tokio::test]
    async fn test_one_block_sst_iterator() {
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
    async fn test_many_block_sst_iterator() {
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
    async fn test_sst_iterator_iter_from_key() {
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
    async fn test_sst_iterator_iter_from_key_smaller_than_first() {
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
    async fn test_sst_iterator_iter_from_key_larger_than_last() {
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

    #[tokio::test]
    async fn test_one_sst_sr_iter() {
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
        let encoded = builder.build().unwrap();
        let id = SsTableId::Compacted(Ulid::new());
        let handle = table_store.write_sst(&id, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle],
        };

        let mut iter =
            SortedRunIterator::new_owned(.., sr, table_store, SstIteratorOptions::default())
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
        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn test_many_sst_sr_iter() {
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
        let encoded = builder.build().unwrap();
        let id1 = SsTableId::Compacted(Ulid::new());
        let handle1 = table_store.write_sst(&id1, encoded).await.unwrap();
        let mut builder = table_store.table_builder();
        builder.add_value(b"key3", b"value3", gen_attrs(3)).unwrap();
        let encoded = builder.build().unwrap();
        let id2 = SsTableId::Compacted(Ulid::new());
        let handle2 = table_store.write_sst(&id2, encoded).await.unwrap();
        let sr = SortedRun {
            id: 0,
            ssts: vec![handle1, handle2],
        };

        let mut iter = SortedRunIterator::new_owned(
            ..,
            sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
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
        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn test_sr_iter_from_key() {
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
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let mut test_case_key_gen = key_gen.clone();
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let mut test_case_val_gen = val_gen.clone();
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        for i in 0..30 {
            let mut expected_key_gen = test_case_key_gen.clone();
            let mut expected_val_gen = test_case_val_gen.clone();
            let from_key = test_case_key_gen.next();
            _ = test_case_val_gen.next();
            let mut iter = SortedRunIterator::new_borrowed(
                from_key.as_ref()..,
                &sr,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await
            .unwrap();
            for _ in 0..30 - i {
                assert_kv(
                    &iter.next().await.unwrap().unwrap(),
                    expected_key_gen.next().as_ref(),
                    expected_val_gen.next().as_ref(),
                );
            }
            assert!(iter.next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_sr_iter_from_key_lower_than_range() {
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
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let mut expected_key_gen = key_gen.clone();
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let mut expected_val_gen = val_gen.clone();
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;
        let mut iter = SortedRunIterator::new_borrowed(
            [b'a', 10].as_ref()..,
            &sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        for _ in 0..30 {
            assert_kv(
                &iter.next().await.unwrap().unwrap(),
                expected_key_gen.next().as_ref(),
                expected_val_gen.next().as_ref(),
            );
        }
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sr_iter_from_key_higher_than_range() {
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
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        let mut iter = SortedRunIterator::new_borrowed(
            [b'z', 30].as_ref()..,
            &sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_through_sorted_run() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            object_store,
            SsTableFormat::default(),
            root_path.clone(),
            None,
        ));

        let mut rng = proptest_util::rng::new_test_rng(None);
        let table = sample::table(&mut rng, 400, 10);
        let max_entries_per_sst = 20;
        let entries_per_sst = 1..max_entries_per_sst;
        let sr =
            build_sorted_run_from_table(&table, table_store.clone(), entries_per_sst, &mut rng)
                .await;
        let mut sr_iter = SortedRunIterator::new_owned(
            ..,
            sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();
        let mut table_iter = table.iter();
        loop {
            let skip = rng.gen::<usize>() % (max_entries_per_sst * 2);
            let run = rng.gen::<usize>() % (max_entries_per_sst * 2);

            let Some((k, _)) = table_iter.nth(skip) else {
                break;
            };
            let seek_key = increment_length(k);
            sr_iter.seek(&seek_key).await.unwrap();

            for (key, value) in table_iter.by_ref().take(run) {
                let kv = sr_iter.next().await.unwrap().unwrap();
                assert_eq!(*key, kv.key);
                assert_eq!(*value, kv.value);
            }
        }
    }

    fn increment_length(b: &[u8]) -> Bytes {
        let mut buf = BytesMut::from(b);
        buf.put_u8(u8::MIN);
        buf.freeze()
    }

    async fn build_sorted_run_from_table<R: SampleRange<usize> + Clone>(
        table: &BTreeMap<Bytes, Bytes>,
        table_store: Arc<TableStore>,
        entries_per_sst: R,
        rng: &mut TestRng,
    ) -> SortedRun {
        let mut ssts = Vec::new();
        let mut entries = table.iter();
        loop {
            let sst_len = rng.gen_range(entries_per_sst.clone());
            let mut builder = table_store.table_builder();

            let sst_kvs: Vec<(&Bytes, &Bytes)> = entries.by_ref().take(sst_len).collect();
            if sst_kvs.is_empty() {
                break;
            }

            for (key, value) in sst_kvs {
                builder.add_value(key, value, gen_attrs(0)).unwrap();
            }

            let encoded = builder.build().unwrap();
            let id = SsTableId::Compacted(Ulid::new());
            let handle = table_store.write_sst(&id, encoded).await.unwrap();
            ssts.push(handle);
        }

        SortedRun { id: 0, ssts }
    }

    async fn build_sr_with_ssts(
        table_store: Arc<TableStore>,
        n: usize,
        keys_per_sst: usize,
        mut key_gen: OrderedBytesGenerator,
        mut val_gen: OrderedBytesGenerator,
    ) -> SortedRun {
        let mut ssts = Vec::<SsTableHandle>::new();
        for _ in 0..n {
            let mut writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
            for _ in 0..keys_per_sst {
                let entry =
                    RowEntry::new_value(key_gen.next().as_ref(), val_gen.next().as_ref(), 0);
                writer.add(entry).await.unwrap();
            }
            ssts.push(writer.close().await.unwrap());
        }
        SortedRun { id: 0, ssts }
    }

    #[tokio::test]
    async fn test_merge_iterator_should_include_entries_in_order() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aaaa", b"1111", 0)
                .with_entry(b"cccc", b"3333", 0)
                .with_entry(b"zzzz", b"26262626", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"bbbb", b"2222", 0)
                .with_entry(b"xxxx", b"24242424", 0)
                .with_entry(b"yyyy", b"25252525", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"dddd", b"4444", 0)
                .with_entry(b"eeee", b"5555", 0)
                .with_entry(b"gggg", b"7777", 0),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"bbbb", b"2222", 0),
                RowEntry::new_value(b"cccc", b"3333", 0),
                RowEntry::new_value(b"dddd", b"4444", 0),
                RowEntry::new_value(b"eeee", b"5555", 0),
                RowEntry::new_value(b"gggg", b"7777", 0),
                RowEntry::new_value(b"xxxx", b"24242424", 0),
                RowEntry::new_value(b"yyyy", b"25252525", 0),
                RowEntry::new_value(b"zzzz", b"26262626", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_iterator_should_write_one_entry_with_given_key() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aaaa", b"0000", 5)
                .with_entry(b"aaaa", b"1111", 6)
                .with_entry(b"cccc", b"use this one c", 5),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"cccc", b"badc1", 1)
                .with_entry(b"xxxx", b"use this one x", 4),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"bbbb", b"2222", 3)
                .with_entry(b"cccc", b"badc2", 3)
                .with_entry(b"xxxx", b"badx1", 3),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 6),
                RowEntry::new_value(b"bbbb", b"2222", 3),
                RowEntry::new_value(b"cccc", b"use this one c", 5),
                RowEntry::new_value(b"xxxx", b"use this one x", 4),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_merge_iterator_should_include_entries_in_order() {
        let iter1 = TestIterator::new()
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"cccc", b"3333", 0)
            .with_entry(b"zzzz", b"26262626", 0);
        let iter2 = TestIterator::new()
            .with_entry(b"bbbb", b"2222", 0)
            .with_entry(b"xxxx", b"24242424", 0)
            .with_entry(b"yyyy", b"25252525", 0);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"bbbb", b"2222", 0),
                RowEntry::new_value(b"cccc", b"3333", 0),
                RowEntry::new_value(b"xxxx", b"24242424", 0),
                RowEntry::new_value(b"yyyy", b"25252525", 0),
                RowEntry::new_value(b"zzzz", b"26262626", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_merge_iterator_should_write_one_entry_with_given_key() {
        let iter1 = TestIterator::new()
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"cccc", b"use this one c", 5);
        let iter2 = TestIterator::new()
            .with_entry(b"cccc", b"badc1", 2)
            .with_entry(b"xxxx", b"24242424", 3);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"cccc", b"use this one c", 5),
                RowEntry::new_value(b"xxxx", b"24242424", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_merge_iter() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa1", 0)
                .with_entry(b"bb", b"bb1", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa2", 0)
                .with_entry(b"bb", b"bb2", 0)
                .with_entry(b"cc", b"cc2", 0),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();
        merge_iter.seek(b"bb".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"bb", b"bb1", 0),
                RowEntry::new_value(b"cc", b"cc2", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_merge_iter_to_current_key() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa1", 0)
                .with_entry(b"bb", b"bb1", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa2", 0)
                .with_entry(b"bb", b"bb2", 0)
                .with_entry(b"cc", b"cc2", 0),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();
        assert_next_entry(&mut merge_iter, &RowEntry::new_value(b"aa", b"aa1", 0)).await;

        merge_iter.seek(b"bb".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"bb", b"bb1", 0),
                RowEntry::new_value(b"cc", b"cc2", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_merge_seek() {
        let iter1 = TestIterator::new()
            .with_entry(b"aa", b"aa1", 1)
            .with_entry(b"bb", b"bb0", 1)
            .with_entry(b"bb", b"bb1", 2)
            .with_entry(b"dd", b"dd1", 3);
        let iter2 = TestIterator::new()
            .with_entry(b"aa", b"aa2", 4)
            .with_entry(b"bb", b"bb2", 5)
            .with_entry(b"cc", b"cc0", 5)
            .with_entry(b"cc", b"cc2", 6)
            .with_entry(b"ee", b"ee2", 7);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();
        merge_iter.seek(b"b".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"bb", b"bb2", 5),
                RowEntry::new_value(b"cc", b"cc2", 6),
                RowEntry::new_value(b"dd", b"dd1", 3),
                RowEntry::new_value(b"ee", b"ee2", 7),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_filter_iterator_should_return_only_matching_entries() {
        let iter = crate::test_utils::TestIterator::new()
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"bbbb", b"", 0)
            .with_entry(b"cccc", b"3333", 0)
            .with_entry(b"d", b"4444", 0)
            .with_entry(b"eeee", b"5", 0)
            .with_entry(b"ffff", b"6666", 0)
            .with_entry(b"g", b"7", 0);

        let filter_entry =
            move |entry: &RowEntry| -> bool { entry.key.len() == 4 && entry.value.len() == 4 };

        let mut filter_iter = FilterIterator::new(iter, Box::new(filter_entry));

        assert_iterator(
            &mut filter_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"cccc", b"3333", 0),
                RowEntry::new_value(b"ffff", b"6666", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_filter_iterator_should_return_none_with_no_matches() {
        let iter = crate::test_utils::TestIterator::new()
            .with_entry(b"", b"1", 0)
            .with_entry(b"b", b"2", 0)
            .with_entry(b"c", b"3", 0);

        let filter_entry =
            move |entry: &RowEntry| -> bool { entry.key.len() == 4 && entry.value.len() == 4 };

        let mut filter_iter = FilterIterator::new(iter, Box::new(filter_entry));

        assert_eq!(filter_iter.next().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_db_iterator_invalidated_iterator() {
        let mut iter = DbIterator::new(
            BytesRange::from(..),
            VecDequeKeyValueIterator::new(VecDeque::new()),
            VecDeque::new(),
            VecDeque::new(),
        )
        .await
        .unwrap();

        iter.invalidated_error = Some(SlateDBError::ChecksumMismatch);

        let result = iter.next().await;
        let err = result.expect_err("Failed to return invalidated iterator");
        assert_invalidated_iterator_error(err);

        let result = iter.seek(Bytes::new()).await;
        let err = result.expect_err("Failed to return invalidated iterator");
        assert_invalidated_iterator_error(err);
    }

    fn assert_invalidated_iterator_error(err: SlateDBError) {
        let SlateDBError::InvalidatedIterator(from_err) = err else {
            panic!("Unexpected error")
        };
        assert!(matches!(*from_err, SlateDBError::ChecksumMismatch));
    }
}
