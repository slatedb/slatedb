use async_trait::async_trait;
use bytes::Bytes;
use std::cmp::min;
use std::collections::VecDeque;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::block_iterator::BlockLike;
use crate::block_iterator_v2::BlockIteratorV2;
use crate::bytes_range::BytesRange;
use crate::db_state::{SsTableHandle, SsTableId};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::filter::{self, BloomFilter};
use crate::flatbuffer_types::{SsTableIndex, SsTableIndexOwned};
use crate::format::block::Block;
use crate::format::sst::{SST_FORMAT_VERSION, SST_FORMAT_VERSION_V2};
use crate::{
    block_iterator::BlockIterator,
    iter::{init_optional_iterator, IterationOrder, KeyValueIterator},
    partitioned_keyspace,
    tablestore::TableStore,
    types::RowEntry,
};

enum FetchTask {
    InFlight(JoinHandle<Result<VecDeque<Arc<Block>>, SlateDBError>>),
    Finished(VecDeque<Arc<Block>>),
}

enum DataBlockIterator<B: BlockLike> {
    V1(BlockIterator<B>),
    V2(BlockIteratorV2<B>),
}

impl<B: BlockLike> DataBlockIterator<B> {
    fn new(block: B, sst_version: u16, order: IterationOrder) -> Result<Self, SlateDBError> {
        match sst_version {
            SST_FORMAT_VERSION => Ok(Self::V1(BlockIterator::new(block, order))),
            SST_FORMAT_VERSION_V2 => Ok(Self::V2(BlockIteratorV2::new(block, order))),
            _ => Err(SlateDBError::InvalidVersion {
                format_name: "SST",
                supported_versions: vec![SST_FORMAT_VERSION, SST_FORMAT_VERSION_V2],
                actual_version: sst_version,
            }),
        }
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match self {
            Self::V1(iter) => iter.next_entry().await,
            Self::V2(iter) => iter.next_entry().await,
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match self {
            Self::V1(iter) => iter.seek(next_key).await,
            Self::V2(iter) => iter.seek(next_key).await,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::V1(iter) => iter.is_empty(),
            Self::V2(iter) => iter.is_empty(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct SstIteratorOptions {
    pub(crate) max_fetch_tasks: usize,
    pub(crate) blocks_to_fetch: usize,
    pub(crate) cache_blocks: bool,
    pub(crate) eager_spawn: bool,
    pub(crate) order: IterationOrder,
}

impl Default for SstIteratorOptions {
    fn default() -> Self {
        SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: 1,
            cache_blocks: true,
            eager_spawn: false,
            order: IterationOrder::Ascending,
        }
    }
}

/// This enum encapsulates access to an SST and corresponding ownership requirements.
/// For example, [`SstView::Owned`] allows the table handle to be owned, which is
/// needed for [`crate::db::Db::scan`] since it returns the iterator, while [`SstView::Borrowed`]
/// accommodates access by reference which is useful for [`crate::db::Db::get`].
pub(crate) enum SstView<'a> {
    Owned(Box<SsTableHandle>, BytesRange),
    Borrowed(&'a SsTableHandle, BytesRange),
}

impl SstView<'_> {
    fn start_key(&self) -> Bound<&[u8]> {
        match self {
            SstView::Owned(_, r) | SstView::Borrowed(_, r) => r.start_bound().map(|b| b.as_ref()),
        }
    }

    fn end_key(&self) -> Bound<&[u8]> {
        match self {
            SstView::Owned(_, r) | SstView::Borrowed(_, r) => r.end_bound().map(|b| b.as_ref()),
        }
    }

    fn point_key(&self) -> Option<&[u8]> {
        match (self.start_key(), self.end_key()) {
            (Bound::Included(start), Bound::Included(end)) if start == end => Some(start),
            _ => None,
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
            SstView::Borrowed(_, r) => r.contains(key),
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

    /// Check whether a key is below the range of this view.
    fn key_precedes(&self, key: &[u8]) -> bool {
        match self.start_key() {
            Included(start) => key < start,
            Excluded(start) => key <= start,
            Unbounded => false,
        }
    }
}

struct IteratorState {
    initialized: bool,
    current_iter: Option<DataBlockIterator<Arc<Block>>>,
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

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn advance(&mut self, iterator: DataBlockIterator<Arc<Block>>) {
        self.initialized = true;
        self.current_iter = Some(iterator);
    }

    fn stop(&mut self) {
        self.initialized = true;
        self.current_iter = None;
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FilterState {
    NotChecked,
    NoFilter,
    Positive,
    Negative,
}

struct BloomFilterEvaluator {
    key: Bytes,
    db_stats: Option<DbStats>,
    state: FilterState,
    found_key: bool,
    false_positive_recorded: bool,
}

impl BloomFilterEvaluator {
    fn new(key: Bytes, db_stats: Option<DbStats>) -> Self {
        Self {
            key,
            db_stats,
            state: FilterState::NotChecked,
            found_key: false,
            false_positive_recorded: false,
        }
    }
}

impl BloomFilterEvaluator {
    /// Evaluate the bloom filter against the key.
    ///
    /// ## Arguments
    /// - `maybe_filter`: An optional bloom filter to evaluate against.
    async fn evaluate(&mut self, maybe_filter: Option<Arc<BloomFilter>>) {
        if self.state != FilterState::NotChecked {
            return;
        }

        let key_hash = filter::filter_hash(self.key.as_ref());

        match maybe_filter {
            Some(filter) => {
                if filter.might_contain(key_hash) {
                    if let Some(stats) = &self.db_stats {
                        stats.sst_filter_positives.inc();
                    }
                    self.state = FilterState::Positive;
                } else {
                    if let Some(stats) = &self.db_stats {
                        stats.sst_filter_negatives.inc();
                    }
                    self.state = FilterState::Negative;
                }
            }
            None => {
                self.state = FilterState::NoFilter;
            }
        }
    }

    fn is_filtered_out(&self) -> bool {
        self.state == FilterState::Negative
    }

    fn notify_key_found(&mut self, key: &[u8]) {
        if key == self.key.as_ref() {
            self.found_key = true;
        }
    }

    fn notify_finished_iteration(&mut self) {
        if self.state == FilterState::Positive && !self.found_key && !self.false_positive_recorded {
            if let Some(stats) = &self.db_stats {
                stats.sst_filter_false_positives.inc();
            }
            self.false_positive_recorded = true;
        }
    }
}

pub(crate) struct InternalSstIterator<'a> {
    view: SstView<'a>,
    index: Option<Arc<SsTableIndexOwned>>,
    state: IteratorState,
    next_block_idx_to_fetch: usize,
    block_idx_range: Range<usize>,
    fetch_tasks: VecDeque<FetchTask>,
    table_store: Arc<TableStore>,
    options: SstIteratorOptions,
    /// Buffer for descending iteration to maintain correct sequence order within keys.
    descending_buffer: Option<VecDeque<RowEntry>>,
    /// Pending entry that was read ahead but belongs to the next key group.
    /// Only used in descending mode.
    pending_entry: Option<RowEntry>,
}

impl<'a> InternalSstIterator<'a> {
    fn new(
        view: SstView<'a>,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        assert!(options.max_fetch_tasks > 0);
        assert!(options.blocks_to_fetch > 0);

        let descending_buffer = match options.order {
            IterationOrder::Descending => Some(VecDeque::new()),
            IterationOrder::Ascending => None,
        };

        Ok(Self {
            view,
            index: None,
            state: IteratorState::new(),
            next_block_idx_to_fetch: 0,
            block_idx_range: 0..0,
            fetch_tasks: VecDeque::new(),
            table_store,
            options,
            descending_buffer,
            pending_entry: None,
        })
    }

    fn table_id(&self) -> SsTableId {
        self.view.table_as_ref().id
    }

    fn view(&self) -> &SstView<'a> {
        &self.view
    }

    fn table_store(&self) -> &Arc<TableStore> {
        &self.table_store
    }

    fn new_owned<T: RangeBounds<Bytes>>(
        range: T,
        table: SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        let Some(view_range) = table.calculate_view_range(BytesRange::from(range)) else {
            return Ok(None);
        };
        let view = SstView::Owned(Box::new(table), view_range);
        Self::new(view, table_store, options).map(Some)
    }

    async fn new_owned_initialized<T: RangeBounds<Bytes>>(
        range: T,
        table: SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        let iter = Self::new_owned(range, table, table_store, options)?;
        init_optional_iterator(iter).await
    }

    fn new_borrowed<T: RangeBounds<Bytes>>(
        range: T,
        table: &'a SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        let Some(view_range) = table.calculate_view_range(BytesRange::from(range)) else {
            return Ok(None);
        };
        let view = SstView::Borrowed(table, view_range);
        Self::new(view, table_store, options).map(Some)
    }

    async fn new_borrowed_initialized<T: RangeBounds<Bytes>>(
        range: T,
        table: &'a SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        let iter = Self::new_borrowed(range, table, table_store, options)?;
        init_optional_iterator(iter).await
    }

    fn for_key(
        table: &'a SsTableHandle,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        Self::new_borrowed(
            BytesRange::from_slice(key..=key),
            table,
            table_store,
            options,
        )
    }

    async fn for_key_initialized(
        table: &'a SsTableHandle,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        let iter = Self::for_key(table, key, table_store, options)?;
        init_optional_iterator(iter).await
    }

    fn last_block_with_data_including_key(index: &SsTableIndex, key: &[u8]) -> Option<usize> {
        partitioned_keyspace::last_partition_including_key(index, key)
    }

    fn first_block_with_data_including_or_after_key(index: &SsTableIndex, key: &[u8]) -> usize {
        partitioned_keyspace::first_partition_including_or_after_key(index, key)
    }

    fn blocks_covering_view(index: &SsTableIndex, view: &SstView) -> Range<usize> {
        let start_block_id = match view.start_key() {
            Included(k) | Excluded(k) => {
                Self::first_block_with_data_including_or_after_key(index, k)
            }
            Unbounded => 0,
        };

        let end_block_id_exclusive = match view.end_key() {
            Included(k) => Self::last_block_with_data_including_key(index, k)
                .map(|b| b + 1)
                .unwrap_or(start_block_id),
            Excluded(k) => {
                let block_index = Self::last_block_with_data_including_key(index, k);
                match block_index {
                    None => start_block_id,
                    Some(block_index) => {
                        let block = index.block_meta().get(block_index);
                        if k == block.first_key().bytes() {
                            block_index
                        } else {
                            block_index + 1
                        }
                    }
                }
            }
            Unbounded => index.block_meta().len(),
        };

        start_block_id..end_block_id_exclusive
    }

    /// Spawns fetch tasks for blocks based on iteration order.
    ///
    /// For ascending order: Fetches blocks forward from `next_block_idx_to_fetch`, incrementing it
    /// as blocks are scheduled. Stops when reaching `block_idx_range.end`.
    ///
    /// For descending order: Fetches blocks backward from `next_block_idx_to_fetch - 1`,
    /// decrementing `next_block_idx_to_fetch` as blocks are scheduled. Stops when reaching
    /// `block_idx_range.start`.
    fn spawn_fetches(&mut self) {
        let Some(index) = self.index.as_ref() else {
            return;
        };

        match self.options.order {
            IterationOrder::Ascending => {
                // Fetch blocks forward: next_block_idx_to_fetch advances toward block_idx_range.end
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
                    let index = index.clone();
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
            IterationOrder::Descending => {
                // Fetch blocks backward: next_block_idx_to_fetch retreats toward block_idx_range.start
                while self.fetch_tasks.len() < self.options.max_fetch_tasks
                    && self.next_block_idx_to_fetch > self.block_idx_range.start
                {
                    let blocks_to_fetch = min(
                        self.options.blocks_to_fetch,
                        self.next_block_idx_to_fetch - self.block_idx_range.start,
                    );
                    let table = self.view.table_as_ref().clone();
                    let table_store = self.table_store.clone();
                    let blocks_end = self.next_block_idx_to_fetch;
                    let blocks_start = blocks_end - blocks_to_fetch;
                    let index = index.clone();
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
                    self.next_block_idx_to_fetch = blocks_start;
                }
            }
        }
    }

    async fn next_iter(
        &mut self,
        spawn_fetches: bool,
    ) -> Result<Option<DataBlockIterator<Arc<Block>>>, SlateDBError> {
        if self.index.is_none() {
            return Ok(None);
        }
        let sst_version = self.view.table_as_ref().format_version;
        loop {
            if spawn_fetches {
                self.spawn_fetches();
            }
            if let Some(fetch_task) = self.fetch_tasks.front_mut() {
                match fetch_task {
                    FetchTask::InFlight(jh) => {
                        let blocks = jh.await.expect("join task failed")?;
                        *fetch_task = FetchTask::Finished(blocks);
                    }
                    FetchTask::Finished(blocks) => {
                        // For descending order, pop from back; for ascending, pop from front
                        let block = match self.options.order {
                            IterationOrder::Ascending => blocks.pop_front(),
                            IterationOrder::Descending => blocks.pop_back(),
                        };

                        if let Some(block) = block {
                            return Ok(Some(DataBlockIterator::new(
                                block,
                                sst_version,
                                self.options.order,
                            )?));
                        } else {
                            self.fetch_tasks.pop_front();
                        }
                    }
                }
            } else {
                assert!(self.fetch_tasks.is_empty());
                // For descending order, check that we've gone back to start
                match self.options.order {
                    IterationOrder::Ascending => {
                        assert_eq!(self.next_block_idx_to_fetch, self.block_idx_range.end);
                    }
                    IterationOrder::Descending => {
                        assert_eq!(self.next_block_idx_to_fetch, self.block_idx_range.start);
                    }
                }
                return Ok(None);
            }
        }
    }

    async fn advance_block(&mut self) -> Result<(), SlateDBError> {
        self.ensure_metadata_loaded().await?;
        if !self.state.is_finished() {
            if let Some(mut iter) = self.next_iter(true).await? {
                // Only seek on the first block to position at the range boundary.
                // For subsequent blocks, iterate through the entire block in the specified order.
                if !self.state.is_initialized() {
                    match self.options.order {
                        IterationOrder::Ascending => match self.view.start_key() {
                            Included(start_key) | Excluded(start_key) => {
                                iter.seek(start_key).await?
                            }
                            Unbounded => (),
                        },
                        IterationOrder::Descending => match self.view.end_key() {
                            Included(end_key) | Excluded(end_key) => iter.seek(end_key).await?,
                            Unbounded => (),
                        },
                    }
                }
                self.state.advance(iter);
            } else {
                self.state.stop();
            }
        }
        Ok(())
    }

    fn stop(&mut self) {
        if let Some(index) = self.index.as_ref() {
            // For ascending order, stopping means we've gone to the end
            // For descending order, stopping means we've gone to the beginning
            match self.options.order {
                IterationOrder::Ascending => {
                    let num_blocks = index.borrow().block_meta().len();
                    self.next_block_idx_to_fetch = num_blocks;
                }
                IterationOrder::Descending => {
                    self.next_block_idx_to_fetch = 0;
                }
            }
        }
        self.state.stop();
    }

    async fn ensure_metadata_loaded(&mut self) -> Result<(), SlateDBError> {
        if self.index.is_none() {
            let index = self
                .table_store
                .read_index(self.view.table_as_ref(), self.options.cache_blocks)
                .await?;
            let block_idx_range =
                InternalSstIterator::blocks_covering_view(&index.borrow(), &self.view);
            self.block_idx_range = block_idx_range.clone();
            // For descending order, start from the end and work backwards
            self.next_block_idx_to_fetch = match self.options.order {
                IterationOrder::Ascending => block_idx_range.start,
                IterationOrder::Descending => block_idx_range.end,
            };
            self.index = Some(index);
            if self.options.eager_spawn {
                self.spawn_fetches();
            }
        }
        Ok(())
    }

    async fn fill_descending_buffer(&mut self) -> Result<(), SlateDBError> {
        let mut temp_buffer = Vec::new();
        let mut target_key: Option<Bytes> = None;

        if let Some(pending) = self.pending_entry.take() {
            target_key = Some(pending.key.clone());
            temp_buffer.push(pending);
        }

        loop {
            let next_entry = if let Some(iter) = self.state.current_iter.as_mut() {
                iter.next_entry().await?
            } else {
                None
            };

            match next_entry {
                Some(kv) => {
                    if !self.view.contains(&kv.key) {
                        if self.view.key_precedes(&kv.key) {
                            self.stop();
                            break;
                        }
                        continue;
                    }

                    if target_key.is_none() {
                        target_key = Some(kv.key.clone());
                        temp_buffer.push(kv);
                    } else if Some(&kv.key) == target_key.as_ref() {
                        temp_buffer.push(kv);
                    } else {
                        self.pending_entry = Some(kv);
                        break;
                    }
                }
                None => {
                    self.advance_block().await?;
                    if self.state.is_finished() {
                        break;
                    }
                }
            }
        }

        temp_buffer.reverse();
        self.descending_buffer
            .as_mut()
            .expect("descending_buffer must exist in descending mode")
            .extend(temp_buffer);

        Ok(())
    }
}

#[async_trait]
impl KeyValueIterator for InternalSstIterator<'_> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        if !self.state.is_initialized() {
            self.advance_block().await?;
        }
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if !self.state.is_initialized() {
            return Err(SlateDBError::IteratorNotInitialized);
        }

        match self.options.order {
            IterationOrder::Descending => {
                if let Some(buffer) = &mut self.descending_buffer {
                    if let Some(entry) = buffer.pop_front() {
                        return Ok(Some(entry));
                    }
                }

                self.fill_descending_buffer().await?;

                return Ok(self
                    .descending_buffer
                    .as_mut()
                    .expect("descending_buffer must exist in descending mode")
                    .pop_front());
            }
            IterationOrder::Ascending => {}
        }

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
                        self.stop();
                    }
                }
                None => self.advance_block().await?,
            }
        }
        Ok(None)
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if !self.state.is_initialized() {
            return Err(SlateDBError::IteratorNotInitialized);
        }
        if !self.view.contains(next_key) {
            if self.view.key_exceeds(next_key) {
                match self.options.order {
                    IterationOrder::Ascending => {
                        // Seeking beyond the end of the view range in ascending order
                        // means there are no more results.
                        self.stop();
                        return Ok(());
                    }
                    IterationOrder::Descending => {
                        // Seeking beyond the end in descending order means "start
                        // from the last key and go backwards" — fall through to
                        // the normal seek logic.
                    }
                }
            } else {
                return Err(SlateDBError::SeekKeyOutOfKeyRange {
                    key: next_key.to_vec(),
                    start_key: self.view.start_key().map(|b| b.to_vec()),
                    end_key: self.view.end_key().map(|b| b.to_vec()),
                });
            }
        }
        if !self.state.is_finished() {
            if let Some(iter) = self.state.current_iter.as_mut() {
                iter.seek(next_key).await?;
                if !iter.is_empty() {
                    return Ok(());
                }
            }

            let index = self
                .index
                .as_ref()
                .expect("metadata must be initialized")
                .clone();

            // For descending order, find the last block with the key
            // For ascending order, find the first block with or after the key
            let block_idx = match self.options.order {
                IterationOrder::Ascending => {
                    Self::first_block_with_data_including_or_after_key(&index.borrow(), next_key)
                }
                IterationOrder::Descending => {
                    Self::last_block_with_data_including_key(&index.borrow(), next_key)
                        .unwrap_or(self.block_idx_range.start)
                }
            };

            // Check if block is in the already-fetched direction
            let already_fetched = match self.options.order {
                IterationOrder::Ascending => block_idx < self.next_block_idx_to_fetch,
                IterationOrder::Descending => block_idx >= self.next_block_idx_to_fetch,
            };

            if already_fetched {
                while let Some(mut block_iter) = self.next_iter(false).await? {
                    block_iter.seek(next_key).await?;
                    if !block_iter.is_empty() {
                        self.state.advance(block_iter);
                        return Ok(());
                    }
                }
            }

            self.fetch_tasks.clear();
            self.next_block_idx_to_fetch = match self.options.order {
                IterationOrder::Ascending => block_idx,
                IterationOrder::Descending => block_idx + 1,
            };

            if let Some(mut block_iter) = self.next_iter(true).await? {
                block_iter.seek(next_key).await?;
                self.state.advance(block_iter);
            } else {
                self.state.stop();
            }
        }
        Ok(())
    }
}

struct BloomFilterIterator<'a> {
    inner: InternalSstIterator<'a>,
    filter: BloomFilterEvaluator,
    initialized: bool,
}

impl<'a> BloomFilterIterator<'a> {
    fn new(inner: InternalSstIterator<'a>, filter: BloomFilterEvaluator) -> Self {
        Self {
            inner,
            filter,
            initialized: false,
        }
    }

    fn table_id(&self) -> SsTableId {
        self.inner.table_id()
    }

    fn is_filtered_out(&self) -> bool {
        self.filter.is_filtered_out()
    }
}

#[async_trait]
impl KeyValueIterator for BloomFilterIterator<'_> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        if !self.initialized {
            let maybe_filter = self
                .inner
                .table_store()
                .read_filter(
                    self.inner.view().table_as_ref(),
                    self.inner.options.cache_blocks,
                )
                .await?;
            self.filter.evaluate(maybe_filter).await;

            if self.is_filtered_out() {
                return Ok(());
            }

            // make sure initializing the inner iterator only happens after
            // the filter is evaluated to avoid unnecessary work
            self.inner.init().await?;
            self.initialized = true;
        }

        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.is_filtered_out() {
            self.filter.notify_finished_iteration();
            return Ok(None);
        }

        let next = self.inner.next_entry().await?;
        if let Some(entry) = next.as_ref() {
            self.filter.notify_key_found(entry.key.as_ref());
        } else {
            self.filter.notify_finished_iteration();
        }

        Ok(next)
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if self.is_filtered_out() {
            return Ok(());
        }

        self.inner.seek(next_key).await
    }
}

enum SstIteratorDelegate<'a> {
    Direct(InternalSstIterator<'a>),
    Bloom(BloomFilterIterator<'a>),
}

pub(crate) struct SstIterator<'a> {
    delegate: SstIteratorDelegate<'a>,
}

impl<'a> SstIterator<'a> {
    fn from_internal(internal: InternalSstIterator<'a>, db_stats: Option<DbStats>) -> Self {
        let point_key = internal.view().point_key().map(Bytes::copy_from_slice);
        let delegate = match point_key {
            Some(key) => {
                let filter = BloomFilterEvaluator::new(key, db_stats);
                SstIteratorDelegate::Bloom(BloomFilterIterator::new(internal, filter))
            }
            None => SstIteratorDelegate::Direct(internal),
        };
        Self { delegate }
    }

    pub(crate) fn new(
        view: SstView<'a>,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let internal = InternalSstIterator::new(view, table_store, options)?;
        Ok(Self::from_internal(internal, None))
    }

    #[allow(dead_code)]
    pub(crate) fn new_owned_with_stats<T: RangeBounds<Bytes>>(
        range: T,
        table: SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Option<Self>, SlateDBError> {
        let internal = InternalSstIterator::new_owned(range, table, table_store, options)?;
        Ok(internal.map(|iter| Self::from_internal(iter, db_stats.clone())))
    }

    #[allow(dead_code)]
    pub(crate) fn new_owned<T: RangeBounds<Bytes>>(
        range: T,
        table: SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        Self::new_owned_with_stats(range, table, table_store, options, None)
    }

    pub(crate) async fn new_owned_initialized<T: RangeBounds<Bytes>>(
        range: T,
        table: SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        let internal =
            InternalSstIterator::new_owned_initialized(range, table, table_store, options).await?;
        match internal {
            Some(inner) => {
                let mut iterator = Self::from_internal(inner, None);
                if let SstIteratorDelegate::Bloom(inner) = &mut iterator.delegate {
                    inner.init().await?;
                    if inner.is_filtered_out() {
                        return Ok(None);
                    }
                }
                Ok(Some(iterator))
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    fn new_borrowed_with_stats<T: RangeBounds<Bytes>>(
        range: T,
        table: &'a SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Option<Self>, SlateDBError> {
        let internal = InternalSstIterator::new_borrowed(range, table, table_store, options)?;
        Ok(internal.map(|iter| Self::from_internal(iter, db_stats.clone())))
    }

    #[allow(dead_code)]
    pub(crate) fn new_borrowed<T: RangeBounds<Bytes>>(
        range: T,
        table: &'a SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        Self::new_borrowed_with_stats(range, table, table_store, options, None)
    }

    pub(crate) async fn new_borrowed_initialized<T: RangeBounds<Bytes>>(
        range: T,
        table: &'a SsTableHandle,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Option<Self>, SlateDBError> {
        let internal =
            InternalSstIterator::new_borrowed_initialized(range, table, table_store, options)
                .await?;
        match internal {
            Some(inner) => {
                let mut iterator = Self::from_internal(inner, None);
                if let SstIteratorDelegate::Bloom(inner) = &mut iterator.delegate {
                    inner.init().await?;
                    if inner.is_filtered_out() {
                        return Ok(None);
                    }
                }
                Ok(Some(iterator))
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn for_key_with_stats(
        table: &'a SsTableHandle,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Option<Self>, SlateDBError> {
        let internal = InternalSstIterator::for_key(table, key, table_store, options)?;
        Ok(internal.map(|iter| Self::from_internal(iter, db_stats.clone())))
    }

    #[allow(dead_code)]
    pub(crate) async fn for_key_with_stats_initialized(
        table: &'a SsTableHandle,
        key: &'a [u8],
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Option<Self>, SlateDBError> {
        let internal =
            InternalSstIterator::for_key_initialized(table, key, table_store, options).await?;
        match internal {
            Some(inner) => {
                let mut iterator = Self::from_internal(inner, db_stats);
                if let SstIteratorDelegate::Bloom(inner) = &mut iterator.delegate {
                    inner.init().await?;
                    if inner.is_filtered_out() {
                        return Ok(None);
                    }
                }
                Ok(Some(iterator))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn table_id(&self) -> SsTableId {
        match &self.delegate {
            SstIteratorDelegate::Direct(inner) => inner.table_id(),
            SstIteratorDelegate::Bloom(inner) => inner.table_id(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_filtered_out(&self) -> bool {
        match &self.delegate {
            SstIteratorDelegate::Direct(_) => false,
            SstIteratorDelegate::Bloom(inner) => inner.is_filtered_out(),
        }
    }
}

#[async_trait]
impl KeyValueIterator for SstIterator<'_> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        match &mut self.delegate {
            SstIteratorDelegate::Direct(inner) => inner.init().await,
            SstIteratorDelegate::Bloom(inner) => inner.init().await,
        }
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match &mut self.delegate {
            SstIteratorDelegate::Direct(inner) => inner.next_entry().await,
            SstIteratorDelegate::Bloom(inner) => inner.next_entry().await,
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &mut self.delegate {
            SstIteratorDelegate::Direct(inner) => inner.seek(next_key).await,
            SstIteratorDelegate::Bloom(inner) => inner.seek(next_key).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_generator::OrderedBytesGenerator;
    use crate::db_cache::test_utils::TestCache;
    use crate::db_cache::DbCache;
    use crate::db_cache::SplitCache;
    use crate::db_state::SsTableId;
    use crate::db_stats::DbStats;
    use crate::filter;
    use crate::format::sst::SsTableFormat;
    use crate::object_stores::ObjectStores;
    use crate::sst_builder::BlockFormat;
    use crate::stats::{ReadableStat, StatRegistry};
    use crate::test_utils::{assert_kv, gen_attrs};
    use crate::types::ValueDeletable;
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_one_block_sst_iter() {
        test_one_block_sst_iter_with_order(IterationOrder::Ascending).await;
        test_one_block_sst_iter_with_order(IterationOrder::Descending).await;
    }

    async fn test_one_block_sst_iter_with_order(order: IterationOrder) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
        ));
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        builder
            .add_value(b"key3", b"value3", gen_attrs(3))
            .await
            .unwrap();
        builder
            .add_value(b"key4", b"value4", gen_attrs(4))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        assert_eq!(index.borrow().block_meta().len(), 1);

        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            order,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // Expected keys based on order
        let expected_keys = match order {
            IterationOrder::Ascending => vec![b"key1", b"key2", b"key3", b"key4"],
            IterationOrder::Descending => vec![b"key4", b"key3", b"key2", b"key1"],
        };
        let expected_values = match order {
            IterationOrder::Ascending => vec![b"value1", b"value2", b"value3", b"value4"],
            IterationOrder::Descending => vec![b"value4", b"value3", b"value2", b"value1"],
        };

        for (expected_key, expected_value) in expected_keys.iter().zip(expected_values.iter()) {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, expected_key.as_slice());
            assert_eq!(kv.value, expected_value.as_slice());
        }
        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn should_record_bloom_filter_positive_for_single_key() {
        // given
        let registry = StatRegistry::new();
        let db_stats = DbStats::new(&registry);
        let table_store = bloom_filter_enabled_table_store(10);
        let sst_handle = build_single_block_sst(&table_store, &[b"k1", b"k2"]).await;

        // when
        let mut iter = SstIterator::for_key_with_stats_initialized(
            &sst_handle,
            b"k1",
            table_store.clone(),
            SstIteratorOptions::default(),
            Some(db_stats.clone()),
        )
        .await
        .expect("iterator construction should succeed")
        .expect("expected iterator for present key");
        let entry = iter
            .next_entry()
            .await
            .expect("iteration should succeed")
            .expect("expected entry for present key");

        // then
        assert_eq!(entry.key.as_ref(), b"k1");
        match entry.value {
            ValueDeletable::Value(value) => assert_eq!(value.as_ref(), b"v_k1"),
            other => panic!("expected value, found {other:?}"),
        }
        assert_eq!(db_stats.sst_filter_positives.get(), 1);
        assert_eq!(db_stats.sst_filter_false_positives.get(), 0);
    }

    #[tokio::test]
    async fn should_record_bloom_filter_negative_for_missing_key() {
        // given
        let registry = StatRegistry::new();
        let db_stats = DbStats::new(&registry);
        let table_store = bloom_filter_enabled_table_store(10);
        let sst_handle = build_single_block_sst(&table_store, &[b"k1", b"k3"]).await;

        // when
        let iter = SstIterator::for_key_with_stats_initialized(
            &sst_handle,
            b"k2",
            table_store,
            SstIteratorOptions::default(),
            Some(db_stats.clone()),
        )
        .await
        .expect("iterator construction should succeed");

        // then
        assert!(iter.is_none(), "negative bloom result should skip iterator");
        assert_eq!(db_stats.sst_filter_negatives.get(), 1);
        assert_eq!(db_stats.sst_filter_false_positives.get(), 0);
    }

    #[tokio::test]
    async fn should_record_bloom_filter_false_positive_for_single_key() {
        // given
        let registry = StatRegistry::new();
        let db_stats = DbStats::new(&registry);
        let table_store = bloom_filter_enabled_table_store(2);
        // these keys share the same bucket in the bloom filter (hard coded)
        // after testing with the SIP13 algorithm. The collision key must be
        // within the SST's key range [k1, k3] for range pruning.
        let existing_keys = [b"k1".as_slice(), b"k3".as_slice()];
        let sst_handle = build_single_block_sst(&table_store, &existing_keys).await;

        let filter = table_store
            .read_filter(&sst_handle, true)
            .await
            .expect("filter read should succeed")
            .expect("filter should exist");

        let collision_key = b"k12";
        let hash = filter::filter_hash(collision_key);
        assert!(
            filter.might_contain(hash),
            "bloom filter should report collision for hard-coded key"
        );

        // when
        let mut iter = SstIterator::for_key_with_stats_initialized(
            &sst_handle,
            collision_key,
            table_store.clone(),
            SstIteratorOptions::default(),
            Some(db_stats.clone()),
        )
        .await
        .expect("iterator construction should succeed")
        .expect("filter positive should yield iterator");

        let entry = iter.next_entry().await.expect("iteration should succeed");

        // then
        assert!(entry.is_none(), "false positive must return no entry");
        assert_eq!(db_stats.sst_filter_positives.get(), 1);
        assert_eq!(db_stats.sst_filter_false_positives.get(), 1);
        assert_eq!(db_stats.sst_filter_negatives.get(), 0);
    }

    #[tokio::test]
    async fn test_bloom_filter_iterator_honors_cache_blocks() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let writer = TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            format.clone(),
            root_path.clone(),
            None,
        );
        let mut builder = writer.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        let handle = writer
            .write_sst(
                &SsTableId::Compacted(ulid::Ulid::new()),
                builder.build().await.unwrap(),
                false,
            )
            .await
            .unwrap();

        let meta_cache = Arc::new(TestCache::new());
        let cache = Arc::new(
            SplitCache::new()
                .with_meta_cache(Some(meta_cache.clone()))
                .build(),
        );
        let reader = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            Some(cache),
        ));

        let no_cache_options = SstIteratorOptions {
            cache_blocks: false,
            ..Default::default()
        };
        let mut iter = SstIterator::for_key_with_stats_initialized(
            &handle,
            b"key1",
            reader.clone(),
            no_cache_options,
            None,
        )
        .await
        .expect("iterator construction should succeed")
        .expect("expected iterator for present key");
        let _ = iter.next_entry().await.unwrap();

        assert!(meta_cache
            .get_filter(&(handle.id, handle.info.filter_offset).into())
            .await
            .unwrap()
            .is_none());

        let cache_options = SstIteratorOptions {
            cache_blocks: true,
            ..Default::default()
        };
        let mut iter = SstIterator::for_key_with_stats_initialized(
            &handle,
            b"key1",
            reader,
            cache_options,
            None,
        )
        .await
        .expect("iterator construction should succeed")
        .expect("expected iterator for present key");
        let _ = iter.next_entry().await.unwrap();

        assert!(meta_cache
            .get_filter(&(handle.id, handle.info.filter_offset).into())
            .await
            .unwrap()
            .is_some());
    }

    fn bloom_filter_enabled_table_store(filter_bits_per_key: u32) -> Arc<TableStore> {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 1,
            filter_bits_per_key,
            ..SsTableFormat::default()
        };
        Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        ))
    }

    async fn build_single_block_sst(
        table_store: &Arc<TableStore>,
        keys: &[&[u8]],
    ) -> SsTableHandle {
        let mut builder = table_store.table_builder();
        for key in keys {
            let value = format!("v_{}", String::from_utf8_lossy(key));
            builder
                .add_value(key, value.as_bytes(), gen_attrs(0))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        table_store.write_sst(&id, encoded, false).await.unwrap()
    }

    #[tokio::test]
    async fn test_many_block_sst_iter() {
        test_many_block_sst_iter_with_order(IterationOrder::Ascending).await;
        test_many_block_sst_iter_with_order(IterationOrder::Descending).await;
    }

    async fn test_many_block_sst_iter_with_order(order: IterationOrder) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
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
                .await
                .unwrap();
        }

        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
            .await
            .unwrap();
        let sst_handle = table_store.open_sst(&SsTableId::Wal(0)).await.unwrap();
        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        assert_eq!(index.borrow().block_meta().len(), 8);

        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 3,
            blocks_to_fetch: 3,
            cache_blocks: true,
            order,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        match order {
            IterationOrder::Ascending => {
                for i in 0..1000 {
                    let kv = iter.next().await.unwrap().unwrap();
                    assert_eq!(kv.key, format!("key{}", i));
                    assert_eq!(kv.value, format!("value{}", i));
                }
            }
            IterationOrder::Descending => {
                for i in (0..1000).rev() {
                    let kv = iter.next().await.unwrap().unwrap();
                    assert_eq!(kv.key, format!("key{}", i));
                    assert_eq!(kv.value, format!("value{}", i));
                }
            }
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
            ObjectStores::new(object_store, None),
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
            let mut iter = SstIterator::new_borrowed_initialized(
                BytesRange::from_slice(from_key.as_ref()..),
                &sst,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await
            .unwrap()
            .expect("Expected Some(iter) but got None");
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
            ObjectStores::new(object_store, None),
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

        let mut iter = SstIterator::new_borrowed_initialized(
            BytesRange::from_slice([b'a'; 16].as_ref()..),
            &sst,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

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
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
        ));
        let first_key = [b'b'; 16];
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'y');
        let first_val = [2u8; 16];
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let (sst, _) = build_sst_with_n_blocks(2, table_store.clone(), key_gen, val_gen).await;

        let iter = SstIterator::new_borrowed_initialized(
            BytesRange::from_slice([b'z'; 16].as_ref()..),
            &sst,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        // The SST's key range doesn't overlap with the query range starting at 'z',
        // so the iterator should be pruned (None).
        assert!(iter.is_none());
    }

    #[tokio::test]
    async fn test_descending_seek_beyond_last_key() {
        test_descending_seek_beyond_last_key_with_format(BlockFormat::V1).await;
        test_descending_seek_beyond_last_key_with_format(BlockFormat::V2).await;
        test_descending_seek_beyond_last_key_with_format(BlockFormat::Latest).await;
    }

    async fn test_descending_seek_beyond_last_key_with_format(block_format: BlockFormat) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 128,
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
        ));

        // Build SST with specified format (keys 0-99)
        let builder = table_store.table_builder();
        let mut builder = match block_format {
            BlockFormat::V1 => builder,
            BlockFormat::V2 => builder.with_block_format(BlockFormat::V2),
            BlockFormat::Latest => builder.with_block_format(BlockFormat::Latest),
        };

        for i in 0..100 {
            builder
                .add_value(
                    format!("key{:03}", i).as_bytes(),
                    format!("value{:03}", i).as_bytes(),
                    gen_attrs(i),
                )
                .await
                .unwrap();
        }

        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst_handle = table_store.write_sst(&id, encoded, false).await.unwrap();

        // Initialize iterator in descending order with full range
        let mut iter = SstIterator::new_borrowed_initialized(
            ..,
            &sst_handle,
            table_store.clone(),
            SstIteratorOptions {
                order: IterationOrder::Descending,
                ..SstIteratorOptions::default()
            },
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // Seek to key999 (beyond the last key which is key099)
        iter.seek(b"key999").await.unwrap();

        // Should iterate backwards from key099
        let kv1 = iter
            .next()
            .await
            .unwrap()
            .expect("Expected first key but got None");
        let kv2 = iter
            .next()
            .await
            .unwrap()
            .expect("Expected second key but got None");

        assert_eq!(kv1.key.as_ref(), b"key099");
        assert_eq!(kv2.key.as_ref(), b"key098");
    }

    #[tokio::test]
    async fn test_iter_seek_through_sst() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 128,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
        ));
        let first_key = [b'b'; 16];
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'y');
        let first_val = [2u8; 16];
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let (sst, nkeys) =
            build_sst_with_n_blocks(256, table_store.clone(), key_gen, val_gen).await;

        let mut iter_large_fetch = SstIterator::new_borrowed_initialized(
            ..,
            &sst,
            table_store.clone(),
            SstIteratorOptions {
                max_fetch_tasks: 32,
                blocks_to_fetch: 256,
                cache_blocks: true,
                eager_spawn: false,
                order: IterationOrder::Ascending,
            },
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        let mut iter_small_fetch = SstIterator::new_borrowed_initialized(
            ..,
            &sst,
            table_store.clone(),
            SstIteratorOptions {
                max_fetch_tasks: 1,
                blocks_to_fetch: 1,
                cache_blocks: true,
                eager_spawn: false,
                order: IterationOrder::Ascending,
            },
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        let mut key_gen = OrderedBytesGenerator::new_with_byte_range(&first_key, b'a', b'y');
        let mut val_gen = OrderedBytesGenerator::new_with_byte_range(&first_val, 1u8, 26u8);
        let mut key_values = Vec::new();
        for _ in 0..nkeys {
            key_values.push((key_gen.next(), val_gen.next()));
        }

        for i in (0..nkeys).step_by(100) {
            iter_large_fetch.seek(&key_values[i].0).await.unwrap();
            let kv_large_fetch = iter_large_fetch.next().await.unwrap().unwrap();

            iter_small_fetch.seek(&key_values[i].0).await.unwrap();
            let kv_small_fetch = iter_small_fetch.next().await.unwrap().unwrap();

            assert_eq!(kv_large_fetch.key, key_values[i].0);
            assert_eq!(kv_large_fetch.value, key_values[i].1);
            assert_eq!(kv_small_fetch.key, key_values[i].0);
            assert_eq!(kv_small_fetch.value, key_values[i].1);
        }
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
        let sst = writer.close().await.unwrap();
        (sst, nkeys)
    }

    #[tokio::test]
    #[cfg(feature = "moka")]
    async fn test_sst_iter_cache_blocks() {
        use crate::db_cache::moka::MokaCache;
        use crate::db_cache::DbCache;
        use crate::db_cache::SplitCache;

        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let block_cache = Arc::new(MokaCache::new());
        let meta_cache = Arc::new(MokaCache::new());
        let split_cache = Arc::new(
            SplitCache::new()
                .with_block_cache(Some(block_cache.clone()))
                .with_meta_cache(Some(meta_cache))
                .build(),
        );
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            Some(split_cache.clone()),
        ));

        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", gen_attrs(1))
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", gen_attrs(2))
            .await
            .unwrap();
        builder
            .add_value(b"key3", b"value3", gen_attrs(3))
            .await
            .unwrap();
        builder
            .add_value(b"key4", b"value4", gen_attrs(4))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        table_store.write_sst(&id, encoded, false).await.unwrap();
        let sst_handle = table_store.open_sst(&id).await.unwrap();

        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        for i in 1..=4 {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, format!("key{}", i).as_bytes());
            assert_eq!(kv.value, format!("value{}", i).as_bytes());
        }

        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());

        // verify that block was cached
        assert!(block_cache
            .get_block(&(id, 0).into())
            .await
            .unwrap_or(None)
            .is_some());

        // remove block from cache and verify that it is not cached when iterating with cache_blocks=false
        block_cache.remove(&(id, 0).into()).await;
        let sst_handle = table_store.open_sst(&id).await.unwrap();
        let sst_iter_options = SstIteratorOptions {
            cache_blocks: false,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        for i in 1..=4 {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, format!("key{}", i).as_bytes());
            assert_eq!(kv.value, format!("value{}", i).as_bytes());
        }

        let kv = iter.next().await.unwrap();
        assert!(kv.is_none());

        // verify that block is not cached
        assert!(block_cache
            .get_block(&(id, 0).into())
            .await
            .unwrap()
            .is_none());
    }

    async fn build_v2_sst(
        table_store: &Arc<TableStore>,
        keys_and_values: &[(&[u8], &[u8])],
    ) -> SsTableHandle {
        let mut builder = table_store
            .table_builder()
            .with_block_format(BlockFormat::V2);
        for (key, value) in keys_and_values {
            builder.add_value(key, value, gen_attrs(0)).await.unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        table_store.write_sst(&id, encoded, false).await.unwrap()
    }

    #[tokio::test]
    async fn should_iterate_v2_sst_scan() {
        // given: a V2 SST with multiple keys
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        ));

        let keys_and_values = vec![
            (b"key1".as_slice(), b"value1".as_slice()),
            (b"key2".as_slice(), b"value2".as_slice()),
            (b"key3".as_slice(), b"value3".as_slice()),
            (b"key4".as_slice(), b"value4".as_slice()),
        ];
        let sst_handle = build_v2_sst(&table_store, &keys_and_values).await;

        // when: iterating over the SST
        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // then: all keys should be returned in order
        for (expected_key, expected_value) in &keys_and_values {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, *expected_key);
            assert_eq!(kv.value, *expected_value);
        }
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_v2_sst_for_key() {
        // given: a V2 SST with multiple keys
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        ));

        let keys_and_values = vec![
            (b"key1".as_slice(), b"value1".as_slice()),
            (b"key2".as_slice(), b"value2".as_slice()),
            (b"key3".as_slice(), b"value3".as_slice()),
            (b"key4".as_slice(), b"value4".as_slice()),
        ];
        let sst_handle = build_v2_sst(&table_store, &keys_and_values).await;

        // when: searching for key2
        let mut iter = SstIterator::for_key_with_stats_initialized(
            &sst_handle,
            b"key2",
            table_store.clone(),
            SstIteratorOptions::default(),
            None,
        )
        .await
        .expect("iterator construction should succeed")
        .expect("expected iterator for present key");

        // then: key2 should be found
        let entry = iter
            .next_entry()
            .await
            .expect("iteration should succeed")
            .expect("expected entry for present key");
        assert_eq!(entry.key.as_ref(), b"key2");
        match entry.value {
            ValueDeletable::Value(value) => assert_eq!(value.as_ref(), b"value2"),
            other => panic!("expected value, found {other:?}"),
        }
    }

    #[tokio::test]
    async fn should_iterate_v2_sst_with_many_keys() {
        // given: a V2 SST with many keys to test prefix compression
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 256,
            min_filter_keys: 1000,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        ));

        // Create keys with shared prefixes to exercise prefix compression
        let mut builder = table_store
            .table_builder()
            .with_block_format(BlockFormat::V2);

        let num_keys = 100;
        for i in 0..num_keys {
            let key = format!("prefix_{:04}", i);
            let value = format!("value_{:04}", i);
            builder
                .add_value(key.as_bytes(), value.as_bytes(), gen_attrs(i))
                .await
                .unwrap();
        }

        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst_handle = table_store.write_sst(&id, encoded, false).await.unwrap();

        // when: iterating over all keys
        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // then: all keys should be returned in order
        for i in 0..num_keys {
            let kv = iter.next().await.unwrap().unwrap();
            let expected_key = format!("prefix_{:04}", i);
            let expected_value = format!("value_{:04}", i);
            assert_eq!(kv.key, expected_key.as_bytes());
            assert_eq!(kv.value, expected_value.as_bytes());
        }
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_v2_sst_across_multiple_blocks() {
        // given: a V2 SST with small block_size to force multiple blocks
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 128, // Small block size to force multiple blocks
            min_filter_keys: 1000,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        ));

        // Create keys that will span multiple blocks
        let mut builder = table_store
            .table_builder()
            .with_block_format(BlockFormat::V2);

        let num_keys = 50;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            builder
                .add_value(key.as_bytes(), value.as_bytes(), gen_attrs(i))
                .await
                .unwrap();
        }

        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst_handle = table_store.write_sst(&id, encoded, false).await.unwrap();

        // Verify we have multiple blocks
        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        assert!(
            index.borrow().block_meta().len() > 1,
            "Expected multiple blocks but got {}",
            index.borrow().block_meta().len()
        );

        // when: seeking to a key in a later block (key_0030)
        let seek_key = b"key_0030";
        let mut iter = SstIterator::new_borrowed_initialized(
            BytesRange::from_slice(seek_key.as_ref()..),
            &sst_handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // then: should iterate from key_0030 onwards
        for i in 30..num_keys {
            let kv = iter.next().await.unwrap().unwrap();
            let expected_key = format!("key_{:04}", i);
            let expected_value = format!("value_{:04}", i);
            assert_eq!(kv.key, expected_key.as_bytes());
            assert_eq!(kv.value, expected_value.as_bytes());
        }
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_return_none_for_missing_key_in_v2_sst() {
        // given: a V2 SST with multiple blocks
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 128,
            min_filter_keys: 1000,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        ));

        let mut builder = table_store
            .table_builder()
            .with_block_format(BlockFormat::V2);

        // Add keys with gaps (only even numbers)
        for i in (0..50).step_by(2) {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            builder
                .add_value(key.as_bytes(), value.as_bytes(), gen_attrs(i))
                .await
                .unwrap();
        }

        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst_handle = table_store.write_sst(&id, encoded, false).await.unwrap();

        // when: searching for a non-existent key (odd number)
        let mut iter = SstIterator::for_key_with_stats_initialized(
            &sst_handle,
            b"key_0025", // This key doesn't exist
            table_store.clone(),
            SstIteratorOptions::default(),
            None,
        )
        .await
        .expect("iterator construction should succeed")
        .expect("expected iterator");

        // then: should return None since key doesn't exist
        let entry = iter.next_entry().await.expect("iteration should succeed");
        assert!(entry.is_none(), "expected None for missing key");
    }

    #[tokio::test]
    async fn should_seek_past_last_key_in_v2_sst() {
        // given: a V2 SST
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 128,
            min_filter_keys: 1000,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path,
            None,
        ));

        let mut builder = table_store
            .table_builder()
            .with_block_format(BlockFormat::V2);

        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            builder
                .add_value(key.as_bytes(), value.as_bytes(), gen_attrs(i))
                .await
                .unwrap();
        }

        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst_handle = table_store.write_sst(&id, encoded, false).await.unwrap();

        // when: seeking past the last key
        let iter = SstIterator::new_borrowed_initialized(
            BytesRange::from_slice(b"zzz".as_ref()..),
            &sst_handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        // then: the SST should be pruned entirely since "zzz" is beyond the last key
        assert!(iter.is_none());
    }

    /// Test: iteration with both start and end bounds where the keys are in the middle of blocks
    /// and neither the first nor last block of the SST is included in the range.
    #[tokio::test]
    async fn test_range_iteration_middle_blocks_ascending() {
        test_range_iteration_middle_blocks_with_order(IterationOrder::Ascending).await;
    }

    #[tokio::test]
    async fn test_range_iteration_middle_blocks_descending() {
        test_range_iteration_middle_blocks_with_order(IterationOrder::Descending).await;
    }

    async fn test_range_iteration_middle_blocks_with_order(order: IterationOrder) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 128, // Small block size to ensure multiple blocks
            min_filter_keys: 100,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
        ));

        // Build an SST with enough keys to span multiple blocks
        // Using key pattern: key000, key001, ..., key099
        let mut builder = table_store.table_builder();
        for i in 0..100 {
            builder
                .add_value(
                    format!("key{:03}", i).as_bytes(),
                    format!("value{:03}", i).as_bytes(),
                    gen_attrs(i),
                )
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        table_store.write_sst(&id, encoded, false).await.unwrap();
        let sst_handle = table_store.open_sst(&id).await.unwrap();

        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        let num_blocks = index.borrow().block_meta().len();
        assert!(
            num_blocks >= 4,
            "Test requires at least 4 blocks, got {}",
            num_blocks
        );

        // Use start and end keys that:
        // 1. Are not at the first key of any block (using middle range key020..key079)
        // 2. Exclude the first block entirely (start > first block's last key)
        // 3. Exclude the last block entirely (end < last block's first key)
        // 4. Are guaranteed to exist in the SST (key020 through key079)
        let start_key = b"key020";
        let end_key = b"key079";

        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 3,
            blocks_to_fetch: 3,
            cache_blocks: true,
            eager_spawn: false,
            order,
        };
        let mut iter = SstIterator::new_owned_initialized(
            BytesRange::from_slice(start_key.as_ref()..=end_key.as_ref()),
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // Expected keys based on order
        let start_idx = 20;
        let end_idx = 79; // inclusive
        let expected_count = end_idx - start_idx + 1;

        let mut count = 0;
        match order {
            IterationOrder::Ascending => {
                for i in start_idx..=end_idx {
                    let kv = iter.next().await.unwrap().unwrap_or_else(|| {
                        panic!(
                            "Expected key{:03} in ascending order, but got None. Count so far: {}",
                            i, count
                        )
                    });
                    assert_eq!(
                        kv.key,
                        format!("key{:03}", i).as_bytes(),
                        "Key mismatch in ascending order at position {}",
                        count
                    );
                    assert_eq!(kv.value, format!("value{:03}", i).as_bytes());
                    count += 1;
                }
            }
            IterationOrder::Descending => {
                for i in (start_idx..=end_idx).rev() {
                    let kv = iter.next().await.unwrap().unwrap_or_else(|| {
                        panic!(
                            "Expected key{:03} in descending order, but got None. Count so far: {}",
                            i, count
                        )
                    });
                    assert_eq!(
                        kv.key,
                        format!("key{:03}", i).as_bytes(),
                        "Key mismatch in descending order at position {}, expected key{:03}",
                        count,
                        i
                    );
                    assert_eq!(kv.value, format!("value{:03}", i).as_bytes());
                    count += 1;
                }
            }
        }

        assert_eq!(
            count, expected_count,
            "Should iterate exactly {} keys",
            expected_count
        );
        assert!(
            iter.next().await.unwrap().is_none(),
            "Should have no more keys"
        );
    }

    #[tokio::test]
    async fn test_full_descending_iteration() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 128, // Small block size to ensure multiple blocks
            min_filter_keys: 100,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
        ));

        // Build an SST with enough data for multiple blocks
        let mut builder = table_store.table_builder();
        for i in 0..30 {
            builder
                .add_value(
                    format!("key{:03}", i).as_bytes(),
                    format!("value{:03}", i).as_bytes(),
                    gen_attrs(i),
                )
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        table_store.write_sst(&id, encoded, false).await.unwrap();
        let sst_handle = table_store.open_sst(&id).await.unwrap();

        let index = table_store.read_index(&sst_handle, true).await.unwrap();
        let num_blocks = index.borrow().block_meta().len();
        assert!(
            num_blocks >= 2,
            "Test requires at least 2 blocks, got {}",
            num_blocks
        );

        // Full iteration in descending order
        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            order: IterationOrder::Descending,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // Should iterate backwards from key029 to key000
        for i in (0..30).rev() {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key, format!("key{:03}", i).as_bytes());
            assert_eq!(kv.value, format!("value{:03}", i).as_bytes());
        }

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_duplicate_keys_iteration() {
        test_duplicate_keys_iteration_with_order(IterationOrder::Ascending).await;
        test_duplicate_keys_iteration_with_order(IterationOrder::Descending).await;
    }

    async fn test_duplicate_keys_iteration_with_order(order: IterationOrder) {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
        ));

        let mut writer = table_store.table_writer(SsTableId::Wal(0));
        writer
            .add(RowEntry::new_value(b"key_a", b"value_100", 100))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(b"key_a", b"value_95", 95))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(b"key_a", b"value_90", 90))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(b"key_b", b"value_80", 80))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(b"key_b", b"value_70", 70))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(b"key_c", b"value_50", 50))
            .await
            .unwrap();
        let handle = writer.close().await.unwrap();

        let mut iter = SstIterator::new_owned_initialized(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions {
                order,
                ..SstIteratorOptions::default()
            },
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        let expected = match order {
            IterationOrder::Ascending => vec![
                (b"key_a".as_slice(), b"value_100".as_slice(), 100),
                (b"key_a".as_slice(), b"value_95".as_slice(), 95),
                (b"key_a".as_slice(), b"value_90".as_slice(), 90),
                (b"key_b".as_slice(), b"value_80".as_slice(), 80),
                (b"key_b".as_slice(), b"value_70".as_slice(), 70),
                (b"key_c".as_slice(), b"value_50".as_slice(), 50),
            ],
            IterationOrder::Descending => vec![
                (b"key_c".as_slice(), b"value_50".as_slice(), 50),
                (b"key_b".as_slice(), b"value_80".as_slice(), 80),
                (b"key_b".as_slice(), b"value_70".as_slice(), 70),
                (b"key_a".as_slice(), b"value_100".as_slice(), 100),
                (b"key_a".as_slice(), b"value_95".as_slice(), 95),
                (b"key_a".as_slice(), b"value_90".as_slice(), 90),
            ],
        };

        for (expected_key, expected_value, expected_seq) in expected {
            let entry = iter
                .next_entry()
                .await
                .expect("iteration should succeed")
                .expect("expected entry");
            assert_eq!(entry.key.as_ref(), expected_key, "key mismatch");
            assert_eq!(entry.seq, expected_seq, "sequence number mismatch");
            match entry.value {
                ValueDeletable::Value(value) => {
                    assert_eq!(value.as_ref(), expected_value, "value mismatch")
                }
                other => panic!("expected value, found {other:?}"),
            }
        }

        let entry = iter.next_entry().await.expect("iteration should succeed");
        assert!(entry.is_none(), "expected end of iteration");
    }
}
