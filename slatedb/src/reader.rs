use crate::batch::{WriteBatch, WriteBatchIterator};
use crate::bytes_range::BytesRange;
use crate::clock::MonotonicClock;
use crate::config::{DurabilityLevel, ReadOptions, ScanOptions};
use crate::db_state::{CoreDbState, SortedRun, SsTableHandle};
use crate::db_stats::DbStats;
use crate::filter_iterator::FilterIterator;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::mem_table::{ImmutableMemtable, KVTable, MemTableIterator};
use crate::oracle::Oracle;
use crate::reader::SstFilterResult::{
    FilterNegative, FilterPositive, RangeNegative, RangePositive,
};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::{RowEntry, ValueDeletable};
use crate::utils::{build_concurrent, compute_max_parallel};
use crate::utils::{get_now_for_read, is_not_expired};
use crate::{db_iter::DbIteratorRangeTracker, error::SlateDBError, filter, DbIterator};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join;
use std::collections::VecDeque;
use std::sync::Arc;

enum SstFilterResult {
    RangeNegative,
    RangePositive,
    FilterPositive,
    FilterNegative,
}

impl SstFilterResult {
    pub(crate) fn might_contain_key(&self) -> bool {
        match self {
            RangeNegative | FilterNegative => false,
            RangePositive | FilterPositive => true,
        }
    }
}

pub(crate) trait DbStateReader {
    fn memtable(&self) -> Arc<KVTable>;
    fn imm_memtable(&self) -> &VecDeque<Arc<ImmutableMemtable>>;
    fn core(&self) -> &CoreDbState;
}

pub(crate) struct Reader {
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) db_stats: DbStats,
    pub(crate) mono_clock: Arc<MonotonicClock>,
    pub(crate) oracle: Arc<Oracle>,
}

impl Reader {
    /// Determines the maximum sequence number for read operations (get and scan). Read operations will filter
    /// out entries with sequence numbers greater than the returned value.
    ///
    /// The method considers:
    /// - User-provided sequence number (e.g., from a Snapshot)
    /// - Durability requirements (Remote vs Local)
    /// - Whether dirty reads are allowed
    ///
    /// Returns the minimum sequence number that satisfies all constraints, or None (read without filtering max seq)
    /// if no constraints apply.
    fn prepare_max_seq(
        &self,
        max_seq_by_user: Option<u64>,
        durability_filter: DurabilityLevel,
        dirty: bool,
    ) -> Option<u64> {
        let mut max_seq: Option<u64> = None;

        // if it's required to only read persisted data, we can only read up to the last remote persisted seq
        if matches!(durability_filter, DurabilityLevel::Remote) {
            max_seq = Some(self.oracle.last_remote_persisted_seq.load());
        }

        // if dirty read is not allowed, we can only read up to the last committed seq
        if !dirty {
            max_seq = max_seq
                .map(|seq| seq.min(self.oracle.last_committed_seq.load()))
                .or(Some(self.oracle.last_committed_seq.load()));
        }

        // if user provide a max seq (mostly from a Snapshot)
        if let Some(max_seq_by_user) = max_seq_by_user {
            max_seq = max_seq
                .map(|seq| seq.min(max_seq_by_user))
                .or(Some(max_seq_by_user));
        }

        max_seq
    }

    /// Get the value for the given key.
    ///
    /// Returns `Ok(Some(value))` if a non-expired value exists for `key`,
    /// `Ok(None)` if the key is deleted or the latest visible value is expired,
    /// and an error if the read fails.
    ///
    /// Arguments:
    /// - `key`: The user key to read. Any type that can be viewed as a byte
    ///   slice is accepted.
    /// - `options`: Options for the read, including durability constraint or
    ///   dirty read.
    /// - `db_state`: Read-only view over in-memory state (memtables) and on-disk
    ///   states (level-0 SSTs and compacted sorted runs).
    /// - `write_batch`: Optional `WriteBatch` to consult first. It's only used when
    ///   operating within a Transaction.
    /// - `max_seq`: Optional upper bound on the sequence number visibility. If
    ///   provided, the read will not return entries with a sequence number
    ///   greater than this value. The final bound is the minimum of this value
    ///   and the bound derived from `options` (e.g., durability, dirty read).
    pub(crate) async fn get_with_options<K: AsRef<[u8]>>(
        &self,
        key: K,
        options: &ReadOptions,
        db_state: &(dyn DbStateReader + Sync + Send),
        write_batch: Option<&WriteBatch>,
        max_seq: Option<u64>,
    ) -> Result<Option<Bytes>, SlateDBError> {
        let now = get_now_for_read(self.mono_clock.clone(), options.durability_filter).await?;
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);
        let get = LevelGet {
            key: key.as_ref(),
            max_seq,
            db_state,
            table_store: self.table_store.clone(),
            db_stats: self.db_stats.clone(),
            write_batch,
            now,
        };
        get.get().await
    }

    /// Create an iterator over a key range.
    ///
    /// Produces a merged iterator over the provided `write_batch` (if any),
    /// in-memory memtables, level-0 SSTs, and compacted sorted runs, honoring
    /// the maximum visible sequence number. The iterator yields only non-
    /// expired, non-tombstone values.
    ///
    /// Arguments
    /// - `range`: The half-open key range to scan (start inclusive, end
    ///   exclusive).
    /// - `options`: Options for the scan, including read-ahead, caching, and the
    ///   maximum number of concurrent fetch tasks.
    /// - `db_state`: Read-only view over in-memory state (memtables) and access to on-disk
    ///   data (level-0 SSTs and compacted sorted runs) needed to construct iterators.
    /// - `write_batch`: Optional `WriteBatch` to include in the merged scan. It's only used when
    ///   operating within a Transaction.
    /// - `max_seq`: Optional upper bound on the sequence number visibility for
    ///   the scan. If provided, entries with a greater sequence number are
    ///   filtered out by the iterator construction.
    pub(crate) async fn scan_with_options<'a>(
        &self,
        range: BytesRange,
        options: &ScanOptions,
        db_state: &(dyn DbStateReader + Sync),
        write_batch: Option<&'a WriteBatch>,
        max_seq: Option<u64>,
        range_tracker: Option<Arc<DbIteratorRangeTracker>>,
    ) -> Result<DbIterator<'a>, SlateDBError> {
        let mut memtables = VecDeque::new();
        memtables.push_back(db_state.memtable());
        for memtable in db_state.imm_memtable() {
            memtables.push_back(memtable.table());
        }
        let memtable_iters: Vec<MemTableIterator> = memtables
            .iter()
            .map(|t| t.range_ascending(range.clone()))
            .collect();

        let read_ahead_blocks = self.table_store.bytes_to_blocks(options.read_ahead_bytes);

        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: options.max_fetch_tasks,
            blocks_to_fetch: read_ahead_blocks,
            cache_blocks: options.cache_blocks,
            eager_spawn: true,
        };

        let max_parallel =
            compute_max_parallel(db_state.core().l0.len(), &db_state.core().compacted, 4);

        let l0_iters_futures =
            build_concurrent(db_state.core().l0.iter().cloned(), max_parallel, |sst| {
                SstIterator::new_owned(
                    range.clone(),
                    sst,
                    self.table_store.clone(),
                    sst_iter_options,
                )
            });

        // SR (owned)
        let sr_iters_futures = build_concurrent(
            db_state.core().compacted.iter().cloned(),
            max_parallel,
            |sr| async {
                SortedRunIterator::new_owned(
                    range.clone(),
                    sr,
                    self.table_store.clone(),
                    sst_iter_options,
                )
                .await
                .map(Some)
            },
        );

        let (l0_iters_res, sr_iters_res) = join(l0_iters_futures, sr_iters_futures).await;
        let l0_iters = l0_iters_res?;
        let sr_iters = sr_iters_res?;

        // Create WriteBatchIterator if write_batch is provided
        let write_batch_iter = write_batch
            .map(|batch| WriteBatchIterator::new(batch, range.clone(), IterationOrder::Ascending));

        DbIterator::new(
            range,
            write_batch_iter,
            memtable_iters,
            l0_iters,
            sr_iters,
            max_seq,
            range_tracker,
        )
        .await
    }
}

/// [`LevelGet`] is a helper struct for [`Reader::get_with_options`], it encapsulates the
/// read path of getting a value for a given key.
///
/// This struct implements the multi-level read strategy that checks data sources
/// in priority order: write batch → memtables → L0 SSTs → compacted sorted runs.
/// The first valid (non-tombstone, non-expired) value found is returned.
struct LevelGet<'a> {
    /// The key to get.
    key: &'a [u8],
    /// The maximum sequence number to filter the entries, this field is used inside Snapshot and Transaction.
    max_seq: Option<u64>,
    /// The reference to in-memory state (memtables) and on-disk states (level-0 SSTs and compacted sorted runs).
    db_state: &'a (dyn DbStateReader + Sync + Send),
    /// The table store to read the data from.
    table_store: Arc<TableStore>,
    /// The reference to the database statistics.
    db_stats: DbStats,
    /// The current time, it's used to check if the entry is expired.
    now: i64,
    /// The optional write batch to read the data from. It's only used when operating within a Transaction.
    write_batch: Option<&'a WriteBatch>,
}

impl<'a> LevelGet<'a> {
    async fn get(&'a self) -> Result<Option<Bytes>, SlateDBError> {
        let mut getters: Vec<Box<dyn KeyValueIterator + 'a>> = vec![];

        // WriteBatch has highest priority
        if let Some(write_batch) = self.write_batch {
            getters.push(Box::new(WriteBatchKeyValueIterator {
                write_batch,
                key: self.key,
            }));
        }

        let bloom_filter = BloomFilter {
            table_store: self.table_store.clone(),
            db_stats: self.db_stats.clone(),
        };

        let l0 = self.db_state.core().l0.clone();
        let compacted = self.db_state.core().compacted.clone();
        let key_hash = filter::filter_hash(self.key);

        getters.extend(vec![
            Box::new(MemtableKeyValueIterator::new(
                self.db_state,
                self.key,
                self.max_seq,
            )) as Box<dyn KeyValueIterator>,

            Box::new(L0KeyValueIterator::new(
                self.table_store.clone(),
                bloom_filter.clone(),
                l0,
                self.key,
                key_hash,
                self.max_seq,
            )) as Box<dyn KeyValueIterator>,

            Box::new(CompactedKeyValueIterator::new(
                compacted,
                self.key,
                key_hash,
                self.max_seq,
                bloom_filter,
                self.table_store.clone(),
            )) as Box<dyn KeyValueIterator>,
        ]);

        self.get_inner(getters.into()).await
    }

    async fn get_inner(
        &'a self,
        mut getters: VecDeque<Box<dyn KeyValueIterator + 'a>>,
    ) -> Result<Option<Bytes>, SlateDBError> {
        while let Some(mut getter) = getters.pop_front() {
            getter.seek(self.key).await?;
            let result = match getter.as_mut().next_entry().await? {
                Some(result) => result,
                None => continue,
            };

            // expired is semantically equivalent to a tombstone. tombstone does not have an expiration.
            let is_tombstone = matches!(result.value, ValueDeletable::Tombstone);
            let is_expired = !is_not_expired(&result, self.now);
            if is_tombstone || is_expired {
                return Ok(None);
            }
            return Ok(result.value.as_bytes());
        }
        Ok(None)
    }
}

struct WriteBatchKeyValueIterator<'a> {
    write_batch: &'a WriteBatch,
    key: &'a [u8],
}

#[async_trait]
impl<'a> KeyValueIterator for WriteBatchKeyValueIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match self.write_batch.get_op(&self.key) {
            Some(op) => {
                let entry = op.to_row_entry(u64::MAX, None, None);
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}

struct MemtableKeyValueIterator<'a> {
    memtables: VecDeque<Arc<KVTable>>,
    key: &'a [u8],
    max_seq: Option<u64>,
}

impl<'a> MemtableKeyValueIterator<'a> {
    fn new(
        db_state: &'a (dyn DbStateReader + Sync + Send),
        key: &'a [u8],
        max_seq: Option<u64>,
    ) -> Self {
        let memtables = std::iter::once(db_state.memtable())
            .chain(db_state.imm_memtable().iter().map(|imm| imm.table()))
            .collect();

        Self {
            memtables,
            key,
            max_seq,
        }
    }
}

#[async_trait]
impl<'a> KeyValueIterator for MemtableKeyValueIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(table) = self.memtables.pop_front() {
            if let Some(val) = table.get(self.key, self.max_seq) {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}

#[derive(Clone)]
struct BloomFilter {
    table_store: Arc<TableStore>,
    db_stats: DbStats,
}

impl BloomFilter {
    /// Check if the given key might be in the range of the SST. Checks if the key is
    /// in the range of the sst and if the filter might contain the key.
    /// ## Arguments
    /// - `sst`: the sst to check
    /// - `key`: the key to check
    /// - `key_hash`: the hash of the key (used for filter, to avoid recomputing the hash)
    /// ## Returns
    /// - `SstFilterResult` indicating whether the key was found or was not in range
    async fn sst_might_include_key(
        &self,
        sst: &SsTableHandle,
        key: &[u8],
        key_hash: u64,
    ) -> Result<SstFilterResult, SlateDBError> {
        if !sst.range_covers_key(key) {
            Ok(RangeNegative)
        } else {
            self.apply_filter(sst, key_hash).await
        }
    }

    /// Check if the given key might be in the range of the sorted run (SR). Checks if the key
    /// is in the range of the SSTs in the run and if the SST's filter might contain the key.
    /// ## Arguments
    /// - `sr`: the sorted run to check
    /// - `key`: the key to check
    /// - `key_hash`: the hash of the key (used for filter, to avoid recomputing the hash)
    /// ## Returns
    /// - `SstFilterResult` indicating whether the key was found or not
    async fn sr_might_include_key(
        &self,
        sr: &SortedRun,
        key: &[u8],
        key_hash: u64,
    ) -> Result<SstFilterResult, SlateDBError> {
        let Some(sst) = sr.find_sst_with_range_covering_key(key) else {
            return Ok(RangeNegative);
        };
        self.apply_filter(sst, key_hash).await
    }

    async fn apply_filter(
        &self,
        sst: &SsTableHandle,
        key_hash: u64,
    ) -> Result<SstFilterResult, SlateDBError> {
        if let Some(filter) = self.table_store.read_filter(sst).await? {
            return if filter.might_contain(key_hash) {
                Ok(FilterPositive)
            } else {
                Ok(FilterNegative)
            };
        }
        Ok(RangePositive)
    }

    fn record_filter_result(&self, result: &SstFilterResult) {
        if matches!(result, FilterPositive) {
            self.db_stats.sst_filter_positives.inc();
        } else if matches!(result, FilterNegative) {
            self.db_stats.sst_filter_negatives.inc();
        }
    }

    fn record_false_positive(&self) {
        self.db_stats.sst_filter_false_positives.inc();
    }
}

fn default_sst_iter_options() -> SstIteratorOptions {
    SstIteratorOptions {
        cache_blocks: true,
        eager_spawn: true,
        ..SstIteratorOptions::default()
    }
}

struct L0KeyValueIterator<'a> {
    table_store: Arc<TableStore>,
    bloom_filter: BloomFilter,
    l0: VecDeque<SsTableHandle>,
    key: &'a [u8],
    key_hash: u64,
    max_seq: Option<u64>,
    current_sst: usize,
}

impl<'a> L0KeyValueIterator<'a> {
    fn new(
        table_store: Arc<TableStore>,
        bloom_filter: BloomFilter,
        l0: VecDeque<SsTableHandle>,
        key: &'a [u8],
        key_hash: u64,
        max_seq: Option<u64>,
    ) -> Self {
        Self {
            table_store,
            bloom_filter,
            l0,
            key,
            key_hash,
            max_seq,
            current_sst: 0,
        }
    }
}

#[async_trait]
impl<'a> KeyValueIterator for L0KeyValueIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(sst) = self.l0.get(self.current_sst) {
            let filter_result = self
                .bloom_filter
                .sst_might_include_key(sst, self.key, self.key_hash)
                .await?;
            self.bloom_filter.record_filter_result(&filter_result);

            if filter_result.might_contain_key() {
                let maybe_iter = SstIterator::for_key(
                    sst,
                    self.key,
                    self.table_store.clone(),
                    default_sst_iter_options(),
                )
                .await?;

                if let Some(iter) = maybe_iter {
                    let mut iter = FilterIterator::new_with_max_seq(iter, self.max_seq);
                    if let Some(entry) = iter.next_entry().await? {
                        if entry.key == self.key {
                            return Ok(Some(entry));
                        }
                    }
                }

                if matches!(filter_result, FilterPositive) {
                    self.bloom_filter.record_false_positive();
                }
            }

            self.current_sst += 1;
        }

        Ok(None)
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}

struct CompactedKeyValueIterator<'a> {
    compacted: Vec<SortedRun>,
    key: &'a [u8],
    key_hash: u64,
    max_seq: Option<u64>,
    current_sr: usize,
    bloom_filter: BloomFilter,
    table_store: Arc<TableStore>,
}

impl<'a> CompactedKeyValueIterator<'a> {
    fn new(
        compacted: Vec<SortedRun>,
        key: &'a [u8],
        key_hash: u64,
        max_seq: Option<u64>,
        bloom_filter: BloomFilter,
        table_store: Arc<TableStore>,
    ) -> Self {
        Self {
            compacted,
            key,
            key_hash,
            max_seq,
            current_sr: 0,
            bloom_filter,
            table_store,
        }
    }
}

#[async_trait]
impl<'a> KeyValueIterator for CompactedKeyValueIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(sr) = self.compacted.get(self.current_sr) {
            let filter_result = self
                .bloom_filter
                .sr_might_include_key(sr, self.key, self.key_hash)
                .await?;
            self.bloom_filter.record_filter_result(&filter_result);

            if filter_result.might_contain_key() {
                let iter = SortedRunIterator::for_key(
                    sr,
                    self.key,
                    self.table_store.clone(),
                    default_sst_iter_options(),
                )
                .await?;
                let mut iter = FilterIterator::new_with_max_seq(iter, self.max_seq);
                if let Some(entry) = iter.next_entry().await? {
                    if entry.key == self.key {
                        return Ok(Some(entry));
                    }
                }

                if matches!(filter_result, FilterPositive) {
                    self.bloom_filter.record_false_positive();
                }
            }

            self.current_sr += 1;
        }
        Ok(None)
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{WriteBatch, WriteOp};
    use crate::config::PutOptions;
    use crate::db_state::{SortedRun, SsTableId};
    use crate::mem_table::WritableKVTable;
    use crate::object_stores::ObjectStores;
    use crate::stats::ReadableStat;
    use crate::{sst::SsTableFormat, stats::StatRegistry, types::ValueDeletable};
    use object_store::{memory::InMemory, path::Path};
    use rstest::rstest;

    /// A mock DbState for testing that can represent any combination of:
    /// - memtable
    /// - immutable memtables
    /// - L0 SSTs and compacted sorted runs (via CoreDbState)
    struct MockDbState {
        memtable: Arc<KVTable>,
        imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
        core: CoreDbState,
    }

    impl MockDbState {
        /// Create an empty mock db state (useful for tests that don't need L0/compacted)
        fn new() -> Self {
            Self {
                memtable: Arc::new(KVTable::new()),
                imm_memtable: VecDeque::new(),
                core: CoreDbState::new(),
            }
        }

        /// Create a mock db state with all levels populated
        fn with_all_levels(
            memtable: Arc<KVTable>,
            imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
            core: CoreDbState,
        ) -> Self {
            Self {
                memtable,
                imm_memtable,
                core,
            }
        }
    }

    impl DbStateReader for MockDbState {
        fn memtable(&self) -> Arc<KVTable> {
            self.memtable.clone()
        }

        fn imm_memtable(&self) -> &VecDeque<Arc<ImmutableMemtable>> {
            &self.imm_memtable
        }

        fn core(&self) -> &CoreDbState {
            &self.core
        }
    }

    fn mock_level_getters<'a>(
        row_entries: Vec<Option<RowEntry>>,
    ) -> Vec<Box<dyn KeyValueIterator + 'a>> {
        row_entries
            .into_iter()
            .map(|entry| Box::new(SingleEntryIterator::new(entry) ) as Box<dyn KeyValueIterator + 'a>)
            .collect()
    }

    struct SingleEntryIterator {
        entry: Option<RowEntry>,
    }
    
    impl SingleEntryIterator {
        fn new(entry: Option<RowEntry>) -> Self {
            Self { entry }
        }
    }

    #[async_trait]
    impl KeyValueIterator for SingleEntryIterator {
        async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            Ok(self.entry.take())
        }

        async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
            Ok(())
        }
    }

    struct LevelGetExpireTestCase {
        entries: Vec<Option<RowEntry>>,
        expected: Option<Bytes>,
    }

    #[tokio::test]
    #[rstest]
    #[case(LevelGetExpireTestCase {
        entries: vec![
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v1")),
                10,
                Some(10000 - 2000),
                Some(10000 - 1000),
            )), // already expired, should be None
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v2")),
                9,
                Some(10000 - 3000),
                Some(10000 + 4000),
            )), // not expired
        ],
        expected: None,
    })]
    #[case(LevelGetExpireTestCase {
        entries: vec![
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v1")),
                10,
                Some(10000 - 2000),
                Some(10000 + 1000),
            )), // not expired
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v2")),
                9,
                Some(10000 - 5000),
                Some(10000 - 4000),
            )), // expired
        ],
        expected: Some(Bytes::from_static(b"v1")),
    })]
    #[case(LevelGetExpireTestCase {
        entries: vec![
            None,
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v2")),
                9,
                Some(10000 - 5000),
                Some(10000 + 4000),
            )), // not expired
        ],
        expected: Some(Bytes::from_static(b"v2")),
    })]
    #[case(LevelGetExpireTestCase {
        entries: vec![
            None,
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Tombstone,
                9,
                Some(10000 - 5000),
                Some(10000 + 4000),
            )), // tombstone
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v2")),
                9,
                Some(10000 - 5000),
                None, // no expiration
            )), // not expired
        ],
        expected: None,
    })]
    #[case(LevelGetExpireTestCase {
        entries: vec![
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Tombstone,
                9,
                Some(10000 - 5000),
                Some(10000 + 4000),
            )), // tombstone
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v2")),
                9,
                Some(10000 - 5000),
                None, // no expiration
            )), // not expired
        ],
        expected: None,
    })]
    #[case(LevelGetExpireTestCase {
        entries: vec![
            None,
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"v1")),
                9,
                Some(10000 - 5000),
                Some(10000 + 4000), // not expired
            )),
            None,
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Tombstone,
                9,
                Some(10000 - 5000),
                None, // no expiration
            )), // tombstone
        ],
        expected: Some(Bytes::from_static(b"v1")),
    })]
    async fn test_level_get_handles_expire(
        #[case] test_case: LevelGetExpireTestCase,
    ) -> Result<(), SlateDBError> {
        let mock_read_db_state = MockDbState::new();
        let stat_registry = StatRegistry::new();
        let get = LevelGet {
            key: b"key",
            max_seq: None,
            db_state: &mock_read_db_state,
            table_store: Arc::new(TableStore::new(
                ObjectStores::new(Arc::new(InMemory::new()), None),
                SsTableFormat::default(),
                Path::from(""),
                None,
            )),
            db_stats: DbStats::new(&stat_registry),
            write_batch: None,
            now: 10000,
        };

        let result = get.get_inner(mock_level_getters(test_case.entries).into()).await?;
        assert_eq!(result, test_case.expected);
        Ok(())
    }

    struct LevelGetWriteBatchTestCase {
        write_batch_ops: Vec<WriteOp>,
        entries: Vec<Option<RowEntry>>, // order: memtable, l0, compacted, ...
        key: Bytes,
        expected: Option<Bytes>,
    }

    #[tokio::test]
    #[rstest]
    #[case(LevelGetWriteBatchTestCase {
        write_batch_ops: vec![WriteOp::Put(Bytes::from_static(b"key"), Bytes::from_static(b"wb_value"), PutOptions::default())],
        entries: vec![
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"mem_value")),
                10,
                Some(10000),
                None,
            )),
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"l0_value")),
                9,
                Some(10000),
                None,
            )),
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"compact_value")),
                8,
                Some(10000),
                None,
            )),
        ],
        key: Bytes::from_static(b"key"),
        expected: Some(Bytes::from_static(b"wb_value")),
    })]
    #[case(LevelGetWriteBatchTestCase {
        write_batch_ops: vec![WriteOp::Delete(Bytes::from_static(b"key"))],
        entries: vec![
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"mem_value")),
                10,
                Some(10000),
                None,
            )),
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"l0_value")),
                9,
                Some(10000),
                None,
            )),
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"compact_value")),
                8,
                Some(10000),
                None,
            )),
        ],
        key: Bytes::from_static(b"key"),
        expected: None, // Delete tombstones all lower levels
    })]
    #[case(LevelGetWriteBatchTestCase {
        write_batch_ops: vec![], // No WriteBatch operations
        entries: vec![
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"mem_value")),
                10,
                Some(10000),
                None,
            )),
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"l0_value")),
                9,
                Some(10000),
                None,
            )),
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"compact_value")),
                8,
                Some(10000),
                None,
            )),
        ],
        key: Bytes::from_static(b"key"),
        expected: Some(Bytes::from_static(b"mem_value")), // Should get memtable value
    })]
    #[case(LevelGetWriteBatchTestCase {
        write_batch_ops: vec![WriteOp::Put(Bytes::from_static(b"key"), Bytes::from_static(b"wb_value"), PutOptions::default())],
        entries: vec![],
        key: Bytes::from_static(b"key"),
        expected: Some(Bytes::from_static(b"wb_value")), // WriteBatch only
    })]
    #[case(LevelGetWriteBatchTestCase {
        write_batch_ops: vec![WriteOp::Put(Bytes::from_static(b"different_key"), Bytes::from_static(b"wb_value"), PutOptions::default())],
        entries: vec![
            Some(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from_static(b"mem_value")),
                10,
                Some(10000),
                None,
            )),
        ],
        key: Bytes::from_static(b"key"),
        expected: Some(Bytes::from_static(b"mem_value")), // WriteBatch doesn't affect different key
    })]
    async fn test_level_get_with_writebatch(
        #[case] test_case: LevelGetWriteBatchTestCase,
    ) -> Result<(), SlateDBError> {
        let mock_read_db_state = MockDbState::new();
        let stat_registry = StatRegistry::new();

        // Create WriteBatch from provided operations
        let write_batch = if test_case.write_batch_ops.is_empty() {
            None
        } else {
            let mut batch = WriteBatch::new();
            for op in &test_case.write_batch_ops {
                match op {
                    WriteOp::Put(key, value, opts) => batch.put_with_options(key, value, opts),
                    WriteOp::Delete(key) => batch.delete(key),
                }
            }
            Some(batch)
        };

        let get = LevelGet {
            key: &test_case.key,
            max_seq: None,
            db_state: &mock_read_db_state,
            table_store: Arc::new(TableStore::new(
                ObjectStores::new(Arc::new(InMemory::new()), None),
                SsTableFormat::default(),
                Path::from(""),
                None,
            )),
            db_stats: DbStats::new(&stat_registry),
            write_batch: write_batch.as_ref(),
            now: 10000,
        };

        let mut getters: Vec<Box<dyn KeyValueIterator>> = vec![];

        // Add WriteBatch getter first (highest priority)
        if let Some(batch) = write_batch.as_ref() {
            getters.push(Box::new(WriteBatchKeyValueIterator {
                write_batch: batch,
                key: &test_case.key,
            }));
        }

        // Add provided entries as subsequent getters in order
        getters.extend(mock_level_getters(test_case.entries.clone()));

        let result = get.get_inner(getters.into()).await?;
        assert_eq!(result, test_case.expected);
        Ok(())
    }

    #[test]
    fn test_scan_options_builder_pattern() {
        // Test that the builder pattern works correctly for max_fetch_tasks
        let options = ScanOptions::default()
            .with_max_fetch_tasks(4)
            .with_cache_blocks(true)
            .with_read_ahead_bytes(1024);

        assert_eq!(options.max_fetch_tasks, 4);
        assert!(options.cache_blocks);
        assert_eq!(options.read_ahead_bytes, 1024);
    }

    // Helper to create an SST with specific keys
    async fn create_test_sst(
        table_store: &Arc<TableStore>,
        keys_and_values: Vec<(&[u8], &[u8])>,
    ) -> Result<SsTableHandle, SlateDBError> {
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let mut builder = table_store.table_builder();
        
        for (key, value) in keys_and_values {
            let entry = RowEntry::new_value(key, value, 1);
            builder.add(entry)?;
        }
        
        let sst = builder.build()?;
        let handle = table_store.write_sst(&id, sst, true).await?;
        Ok(handle)
    }

    // Helper to create a sorted run with specific SSTs
    fn create_test_sorted_run(ssts: Vec<SsTableHandle>) -> SortedRun {
        SortedRun {
            id: 0,
            ssts,
        }
    }

    #[tokio::test]
    async fn test_l0_iterator_finds_key_in_first_sst() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create SST with key "key1"
        let sst1 = create_test_sst(&table_store, vec![(b"key1", b"value1")]).await?;
        let mut l0 = VecDeque::new();
        l0.push_back(sst1);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        let key = b"key1";
        let key_hash = filter::filter_hash(key);
        let mut iterator = L0KeyValueIterator::new(
            table_store.clone(),
            bloom_filter,
            l0,
            key,
            key_hash,
            None,
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key1"));
        assert_eq!(entry.value, ValueDeletable::Value(Bytes::from_static(b"value1")));

        Ok(())
    }

    #[tokio::test]
    async fn test_l0_iterator_key_not_in_range() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create SST with keys "key2" and "key3"
        let sst1 = create_test_sst(&table_store, vec![(b"key2", b"value2"), (b"key3", b"value3")]).await?;
        let mut l0 = VecDeque::new();
        l0.push_back(sst1);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        // Look for "key1" which is before the range of the SST
        let key = b"key1";
        let key_hash = filter::filter_hash(key);
        let mut iterator = L0KeyValueIterator::new(
            table_store.clone(),
            bloom_filter.clone(),
            l0,
            key,
            key_hash,
            None,
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_l0_iterator_searches_multiple_ssts() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create first SST with "key1"
        let sst1 = create_test_sst(&table_store, vec![(b"key1", b"value1")]).await?;
        // Create second SST with "key2"
        let sst2 = create_test_sst(&table_store, vec![(b"key2", b"value2")]).await?;
        
        let mut l0 = VecDeque::new();
        l0.push_back(sst1);
        l0.push_back(sst2);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        // Look for "key2" which is in the second SST
        let key = b"key2";
        let key_hash = filter::filter_hash(key);
        let mut iterator = L0KeyValueIterator::new(
            table_store.clone(),
            bloom_filter,
            l0,
            key,
            key_hash,
            None,
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key2"));
        assert_eq!(entry.value, ValueDeletable::Value(Bytes::from_static(b"value2")));

        Ok(())
    }

    #[tokio::test]
    async fn test_l0_iterator_respects_max_seq() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create SST with entry at seq 10
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let mut builder = table_store.table_builder();
        let entry = RowEntry::new_value(b"key1", b"value1", 10);
        builder.add(entry)?;
        let sst = builder.build()?;
        let handle = table_store.write_sst(&id, sst, true).await?;

        let mut l0 = VecDeque::new();
        l0.push_back(handle);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        let key = b"key1";
        let key_hash = filter::filter_hash(key);
        
        // Try to read with max_seq = 5 (should not find the entry)
        let mut iterator = L0KeyValueIterator::new(
            table_store.clone(),
            bloom_filter,
            l0.clone(),
            key,
            key_hash,
            Some(5),
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_none(), "Should not find entry with seq 10 when max_seq is 5");

        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_iterator_finds_key_in_sorted_run() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create SST with key "key1"
        let sst1 = create_test_sst(&table_store, vec![(b"key1", b"value1")]).await?;
        let sr = create_test_sorted_run(vec![sst1]);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        let key = b"key1";
        let key_hash = filter::filter_hash(key);
        let mut iterator = CompactedKeyValueIterator::new(
            vec![sr],
            key,
            key_hash,
            None,
            bloom_filter,
            table_store.clone(),
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key1"));
        assert_eq!(entry.value, ValueDeletable::Value(Bytes::from_static(b"value1")));

        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_iterator_key_not_in_range() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create SST with keys "key2" and "key3"
        let sst1 = create_test_sst(&table_store, vec![(b"key2", b"value2"), (b"key3", b"value3")]).await?;
        let sr = create_test_sorted_run(vec![sst1]);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        // Look for "key1" which is before the range of the SST
        let key = b"key1";
        let key_hash = filter::filter_hash(key);
        let mut iterator = CompactedKeyValueIterator::new(
            vec![sr],
            key,
            key_hash,
            None,
            bloom_filter,
            table_store.clone(),
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_iterator_searches_multiple_sorted_runs() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create first sorted run with "key1"
        let sst1 = create_test_sst(&table_store, vec![(b"key1", b"value1")]).await?;
        let sr1 = create_test_sorted_run(vec![sst1]);

        // Create second sorted run with "key2"
        let sst2 = create_test_sst(&table_store, vec![(b"key2", b"value2")]).await?;
        let sr2 = create_test_sorted_run(vec![sst2]);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        // Look for "key2" which is in the second sorted run
        let key = b"key2";
        let key_hash = filter::filter_hash(key);
        let mut iterator = CompactedKeyValueIterator::new(
            vec![sr1, sr2],
            key,
            key_hash,
            None,
            bloom_filter,
            table_store.clone(),
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key2"));
        assert_eq!(entry.value, ValueDeletable::Value(Bytes::from_static(b"value2")));

        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_iterator_respects_max_seq() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create SST with entry at seq 10
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let mut builder = table_store.table_builder();
        let entry = RowEntry::new_value(b"key1", b"value1", 10);
        builder.add(entry)?;
        let sst = builder.build()?;
        let handle = table_store.write_sst(&id, sst, true).await?;

        let sr = create_test_sorted_run(vec![handle]);

        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
        };

        let key = b"key1";
        let key_hash = filter::filter_hash(key);

        // Try to read with max_seq = 5 (should not find the entry)
        let mut iterator = CompactedKeyValueIterator::new(
            vec![sr],
            key,
            key_hash,
            Some(5),
            bloom_filter,
            table_store.clone(),
        );

        let result = iterator.next_entry().await?;
        assert!(result.is_none(), "Should not find entry with seq 10 when max_seq is 5");

        Ok(())
    }

    #[tokio::test]
    async fn test_bloom_filter_stats_recorded() -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Create SST with key "key1"
        let sst1 = create_test_sst(&table_store, vec![(b"key1", b"value1")]).await?;
        let mut l0 = VecDeque::new();
        l0.push_back(sst1);

        let db_stats = DbStats::new(&stat_registry);
        let bloom_filter = BloomFilter {
            table_store: table_store.clone(),
            db_stats: db_stats.clone(),
        };

        let key = b"key1";
        let key_hash = filter::filter_hash(key);
        let mut iterator = L0KeyValueIterator::new(
            table_store.clone(),
            bloom_filter,
            l0,
            key,
            key_hash,
            None,
        );

        let initial_positives = db_stats.sst_filter_positives.get();
        iterator.next_entry().await?;
        
        // Should have recorded at least one filter positive (either RangePositive or FilterPositive)
        assert!(db_stats.sst_filter_positives.get() > initial_positives);

        Ok(())
    }

    // Test data structure for multi-level LevelGet tests
    struct LevelGetMultiLevelTestCase {
        name: &'static str,
        write_batch_value: Option<&'static [u8]>,
        memtable_value: Option<&'static [u8]>,
        imm_memtable_value: Option<&'static [u8]>,
        l0_value: Option<&'static [u8]>,
        compacted_value: Option<&'static [u8]>,
        expected: Option<&'static [u8]>,
    }

    async fn setup_multi_level_db_state(
        table_store: &Arc<TableStore>,
        test_case: &LevelGetMultiLevelTestCase,
    ) -> Result<(CoreDbState, Arc<KVTable>, VecDeque<Arc<ImmutableMemtable>>), SlateDBError> {
        let mut core = CoreDbState::new();

        // Create L0 SST if needed
        if let Some(value) = test_case.l0_value {
            let sst = create_test_sst(table_store, vec![(b"test_key", value)]).await?;
            core.l0.push_back(sst);
        }

        // Create compacted SST if needed
        if let Some(value) = test_case.compacted_value {
            let sst = create_test_sst(table_store, vec![(b"test_key", value)]).await?;
            let sr = create_test_sorted_run(vec![sst]);
            core.compacted.push(sr);
        }

        // Create memtable
        let memtable = Arc::new(KVTable::new());
        if let Some(value) = test_case.memtable_value {
            let entry = RowEntry::new_value(b"test_key", value, 1);
            memtable.put(entry);
        }

        // Create immutable memtable
        let mut imm_memtable = VecDeque::new();
        if let Some(value) = test_case.imm_memtable_value {
            let writable = WritableKVTable::new();
            let entry = RowEntry::new_value(b"test_key", value, 1);
            writable.put(entry);
            imm_memtable.push_back(Arc::new(ImmutableMemtable::new(writable, 1)));
        }

        Ok((core, memtable, imm_memtable))
    }

    #[tokio::test]
    #[rstest]
    #[case(LevelGetMultiLevelTestCase {
        name: "key_in_write_batch_only",
        write_batch_value: Some(b"wb_value"),
        memtable_value: None,
        imm_memtable_value: None,
        l0_value: None,
        compacted_value: None,
        expected: Some(b"wb_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "key_in_memtable_only",
        write_batch_value: None,
        memtable_value: Some(b"mem_value"),
        imm_memtable_value: None,
        l0_value: None,
        compacted_value: None,
        expected: Some(b"mem_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "key_in_imm_memtable_only",
        write_batch_value: None,
        memtable_value: None,
        imm_memtable_value: Some(b"imm_value"),
        l0_value: None,
        compacted_value: None,
        expected: Some(b"imm_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "key_in_l0_only",
        write_batch_value: None,
        memtable_value: None,
        imm_memtable_value: None,
        l0_value: Some(b"l0_value"),
        compacted_value: None,
        expected: Some(b"l0_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "key_in_compacted_only",
        write_batch_value: None,
        memtable_value: None,
        imm_memtable_value: None,
        l0_value: None,
        compacted_value: Some(b"compacted_value"),
        expected: Some(b"compacted_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "write_batch_overrides_all",
        write_batch_value: Some(b"wb_value"),
        memtable_value: Some(b"mem_value"),
        imm_memtable_value: Some(b"imm_value"),
        l0_value: Some(b"l0_value"),
        compacted_value: Some(b"compacted_value"),
        expected: Some(b"wb_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "memtable_overrides_lower_levels",
        write_batch_value: None,
        memtable_value: Some(b"mem_value"),
        imm_memtable_value: Some(b"imm_value"),
        l0_value: Some(b"l0_value"),
        compacted_value: Some(b"compacted_value"),
        expected: Some(b"mem_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "imm_memtable_overrides_sst_levels",
        write_batch_value: None,
        memtable_value: None,
        imm_memtable_value: Some(b"imm_value"),
        l0_value: Some(b"l0_value"),
        compacted_value: Some(b"compacted_value"),
        expected: Some(b"imm_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "l0_overrides_compacted",
        write_batch_value: None,
        memtable_value: None,
        imm_memtable_value: None,
        l0_value: Some(b"l0_value"),
        compacted_value: Some(b"compacted_value"),
        expected: Some(b"l0_value"),
    })]
    #[case(LevelGetMultiLevelTestCase {
        name: "key_not_found_anywhere",
        write_batch_value: None,
        memtable_value: None,
        imm_memtable_value: None,
        l0_value: None,
        compacted_value: None,
        expected: None,
    })]
    async fn test_level_get_multi_level(
        #[case] test_case: LevelGetMultiLevelTestCase,
    ) -> Result<(), SlateDBError> {
        let os = Arc::new(InMemory::new());
        let stat_registry = StatRegistry::new();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os, None),
            SsTableFormat::default(),
            Path::from("/test"),
            None,
        ));

        // Setup all levels
        let (core, memtable, imm_memtable) = setup_multi_level_db_state(&table_store, &test_case).await?;
        
        let db_state = MockDbState::with_all_levels(memtable, imm_memtable, core);

        // Setup WriteBatch if needed
        let write_batch = test_case.write_batch_value.map(|value| {
            let mut batch = WriteBatch::new();
            batch.put(b"test_key", value);
            batch
        });

        let get = LevelGet {
            key: b"test_key",
            max_seq: None,
            db_state: &db_state,
            table_store: table_store.clone(),
            db_stats: DbStats::new(&stat_registry),
            write_batch: write_batch.as_ref(),
            now: 10000,
        };

        let result = get.get().await?;

        // Verify the result matches expectations
        match (result, test_case.expected) {
            (Some(actual), Some(expected)) => {
                assert_eq!(
                    actual,
                    Bytes::from_static(expected),
                    "Test case '{}' failed: expected {:?}, got {:?}",
                    test_case.name,
                    expected,
                    actual
                );
            }
            (None, None) => {
                // Both None, test passes
            }
            (Some(actual), None) => {
                panic!(
                    "Test case '{}' failed: expected None, got {:?}",
                    test_case.name, actual
                );
            }
            (None, Some(expected)) => {
                panic!(
                    "Test case '{}' failed: expected {:?}, got None",
                    test_case.name, expected
                );
            }
        }

        Ok(())
    }
}
