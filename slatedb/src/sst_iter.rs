use async_trait::async_trait;
use bytes::Bytes;
use std::cmp::min;
use std::collections::VecDeque;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::bytes_range::BytesRange;
use crate::db_state::{SsTableHandle, SsTableId};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::filter;
use crate::flatbuffer_types::{SsTableIndex, SsTableIndexOwned};
use crate::{
    block::Block,
    block_iterator::BlockIterator,
    iter::{init_optional_iterator, KeyValueIterator},
    partitioned_keyspace,
    tablestore::TableStore,
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

    fn is_initialized(&self) -> bool {
        self.initialized
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
    async fn evaluate(
        &mut self,
        view: &SstView<'_>,
        table_store: &Arc<TableStore>,
    ) -> Result<(), SlateDBError> {
        if self.state != FilterState::NotChecked {
            return Ok(());
        }

        let key_hash = filter::filter_hash(self.key.as_ref());
        let maybe_filter = table_store.read_filter(view.table_as_ref()).await?;

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

        Ok(())
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
}

impl<'a> InternalSstIterator<'a> {
    fn new(
        view: SstView<'a>,
        table_store: Arc<TableStore>,
        options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        assert!(options.max_fetch_tasks > 0);
        assert!(options.blocks_to_fetch > 0);

        Ok(Self {
            view,
            index: None,
            state: IteratorState::new(),
            next_block_idx_to_fetch: 0,
            block_idx_range: 0..0,
            fetch_tasks: VecDeque::new(),
            table_store,
            options,
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

    fn spawn_fetches(&mut self) {
        let Some(index) = self.index.as_ref() else {
            return;
        };
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

    async fn next_iter(
        &mut self,
        spawn_fetches: bool,
    ) -> Result<Option<BlockIterator<Arc<Block>>>, SlateDBError> {
        if self.index.is_none() {
            return Ok(None);
        }
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
        self.fetch_index().await?;
        if !self.state.is_finished() {
            if let Some(mut iter) = self.next_iter(true).await? {
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
        if let Some(index) = self.index.as_ref() {
            let num_blocks = index.borrow().block_meta().len();
            self.next_block_idx_to_fetch = num_blocks;
        }
        self.state.stop();
    }

    async fn fetch_index(&mut self) -> Result<(), SlateDBError> {
        if self.index.is_none() {
            let index = self
                .table_store
                .read_index(self.view.table_as_ref())
                .await?;
            let block_idx_range =
                InternalSstIterator::blocks_covering_view(&index.borrow(), &self.view);
            self.block_idx_range = block_idx_range.clone();
            self.next_block_idx_to_fetch = block_idx_range.start;
            self.index = Some(index);
            if self.options.eager_spawn {
                self.spawn_fetches();
            }
        }
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

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if !self.state.is_initialized() {
            return Err(SlateDBError::IteratorNotInitialized);
        }
        if !self.view.contains(next_key) {
            return Err(SlateDBError::SeekKeyOutOfKeyRange {
                key: next_key.to_vec(),
                start_key: self.view.start_key().map(|b| b.to_vec()),
                end_key: self.view.end_key().map(|b| b.to_vec()),
            });
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
            let block_idx =
                Self::first_block_with_data_including_or_after_key(&index.borrow(), next_key);
            if block_idx < self.next_block_idx_to_fetch {
                while let Some(mut block_iter) = self.next_iter(false).await? {
                    block_iter.seek(next_key).await?;
                    if !block_iter.is_empty() {
                        self.state.advance(block_iter);
                        return Ok(());
                    }
                }
            }

            self.fetch_tasks.clear();
            self.next_block_idx_to_fetch = block_idx;
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
            self.filter
                .evaluate(self.inner.view(), self.inner.table_store())
                .await?;

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
    use crate::db_state::SsTableId;
    use crate::db_stats::DbStats;
    use crate::filter;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::stats::{ReadableStat, StatRegistry};
    use crate::test_utils::{assert_kv, gen_attrs};
    use crate::types::ValueDeletable;
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
            ObjectStores::new(object_store, None),
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
            .write_sst(&SsTableId::Wal(0), encoded, false)
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
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");
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
        // these two keys share the same bucket in the bloom filter (hard coded)
        // after testing with the SIP13 algorithm
        let existing_keys = [b"k1".as_slice(), b"k3".as_slice()];
        let sst_handle = build_single_block_sst(&table_store, &existing_keys).await;

        let filter = table_store
            .read_filter(&sst_handle)
            .await
            .expect("filter read should succeed")
            .expect("filter should exist");

        let collision_key = b"k6";
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
                .unwrap();
        }
        let encoded = builder.build().unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        table_store.write_sst(&id, encoded, false).await.unwrap()
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
                .unwrap();
        }

        let encoded = builder.build().unwrap();
        table_store
            .write_sst(&SsTableId::Wal(0), encoded, false)
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
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            sst_handle,
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");
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

        let mut iter = SstIterator::new_borrowed_initialized(
            BytesRange::from_slice([b'z'; 16].as_ref()..),
            &sst,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        assert!(iter.next().await.unwrap().is_none());
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
}
