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

use bytes::Bytes;
use futures::future::{join, BoxFuture};
use futures::FutureExt;
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
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);
        let now = get_now_for_read(self.mono_clock.clone(), options.durability_filter).await?;

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
            now,
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
    fn get_write_batch(&'a self) -> BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>> {
        async move {
            let batch = match self.write_batch {
                Some(batch) => batch,
                None => return Ok(None),
            };

            match batch.get_op(self.key) {
                None => Ok(None),
                Some(op) => {
                    // place a highest seq number as the placeholder.
                    let entry = op.to_row_entry(u64::MAX, None, None);
                    Ok(Some(entry))
                }
            }
        }
        .boxed()
    }

    async fn get(&'a self) -> Result<Option<Bytes>, SlateDBError> {
        let mut getters: Vec<BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>>> = vec![];

        // WriteBatch has highest priority
        if self.write_batch.is_some() {
            getters.push(self.get_write_batch());
        }

        getters.extend(vec![
            self.get_memtable(),
            self.get_l0(),
            self.get_compacted(),
        ]);

        self.get_inner(getters).await
    }

    async fn get_inner(
        &'a self,
        getters: Vec<BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>>>,
    ) -> Result<Option<Bytes>, SlateDBError> {
        for getter in getters {
            let result = match getter.await? {
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

    fn get_memtable(&'a self) -> BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>> {
        async move {
            let maybe_val = std::iter::once(self.db_state.memtable())
                .chain(self.db_state.imm_memtable().iter().map(|imm| imm.table()))
                .find_map(|memtable| memtable.get(self.key, self.max_seq));
            if let Some(val) = maybe_val {
                return Ok(Some(val));
            }

            Ok(None)
        }
        .boxed()
    }

    fn get_l0(&'a self) -> BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>> {
        async move {
            // cache blocks that are being read
            let sst_iter_options = SstIteratorOptions {
                cache_blocks: true,
                eager_spawn: true,
                ..SstIteratorOptions::default()
            };

            let key_hash = filter::filter_hash(self.key);

            for sst in &self.db_state.core().l0 {
                let filter_result = self.sst_might_include_key(sst, self.key, key_hash).await?;
                self.record_filter_result(&filter_result);

                if filter_result.might_contain_key() {
                    let maybe_iter = SstIterator::for_key(
                        sst,
                        self.key,
                        self.table_store.clone(),
                        sst_iter_options,
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
                        self.db_stats.sst_filter_false_positives.inc();
                    }
                }
            }
            Ok(None)
        }
        .boxed()
    }

    fn get_compacted(&'a self) -> BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>> {
        async move {
            // cache blocks that are being read
            let sst_iter_options = SstIteratorOptions {
                cache_blocks: true,
                eager_spawn: true,
                ..SstIteratorOptions::default()
            };
            let key_hash = filter::filter_hash(self.key);

            for sr in &self.db_state.core().compacted {
                let filter_result = self.sr_might_include_key(sr, self.key, key_hash).await?;
                self.record_filter_result(&filter_result);

                if filter_result.might_contain_key() {
                    let iter = SortedRunIterator::for_key(
                        sr,
                        self.key,
                        self.table_store.clone(),
                        sst_iter_options,
                    )
                    .await?;

                    let mut iter = FilterIterator::new_with_max_seq(iter, self.max_seq);
                    if let Some(entry) = iter.next_entry().await? {
                        if entry.key == self.key {
                            return Ok(Some(entry));
                        }
                    }
                    if matches!(filter_result, FilterPositive) {
                        self.db_stats.sst_filter_false_positives.inc();
                    }
                }
            }
            Ok(None)
        }
        .boxed()
    }

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RowEntry, ValueDeletable};
    use rstest::rstest;

    use crate::batch::WriteBatch;
    use crate::clock::{DefaultSystemClock, LogicalClock, MonotonicClock};
    use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
    use crate::object_stores::ObjectStores;
    use crate::oracle::Oracle;
    use crate::sst::SsTableFormat;
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::test_utils::TestClock;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use std::collections::HashMap;
    use ulid::Ulid;

    /// Test database state that can be populated with entries
    struct TestDbState {
        memtable: Arc<KVTable>,
        imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
        core: CoreDbState,
        table_store: Arc<TableStore>,
    }

    impl TestDbState {
        async fn new() -> Self {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let table_store = Arc::new(TableStore::new(
                ObjectStores::new(object_store, None),
                SsTableFormat::default(),
                Path::from("/test"),
                None,
            ));

            Self {
                memtable: Arc::new(KVTable::new()),
                imm_memtable: VecDeque::new(),
                core: CoreDbState::new(),
                table_store,
            }
        }

        /// Add entries to the memtable
        fn add_to_memtable(&mut self, entries: Vec<RowEntry>) {
            for entry in entries {
                self.memtable.put(entry);
            }
        }

        /// Create an immutable memtable from entries
        fn add_immutable_memtable(&mut self, entries: Vec<RowEntry>) {
            let writable = crate::mem_table::WritableKVTable::new();
            for entry in entries {
                writable.put(entry);
            }
            let imm = Arc::new(ImmutableMemtable::new(writable, 0));
            self.imm_memtable.push_back(imm);
        }

        /// Create an SST from entries and add to L0
        async fn add_to_l0(&mut self, entries: Vec<RowEntry>) -> Result<(), SlateDBError> {
            if entries.is_empty() {
                return Ok(());
            }
            let sst_handle = self.build_sst(entries).await?;
            self.core.l0.push_front(sst_handle);
            Ok(())
        }

        /// Create an SST from entries and add to a sorted run
        async fn add_to_sorted_run(
            &mut self,
            sr_id: u32,
            entries: Vec<RowEntry>,
        ) -> Result<(), SlateDBError> {
            if entries.is_empty() {
                return Ok(());
            }
            let sst_handle = self.build_sst(entries).await?;

            // Find or create the sorted run
            if let Some(sr) = self.core.compacted.iter_mut().find(|sr| sr.id == sr_id) {
                sr.ssts.push(sst_handle);
            } else {
                let new_sr = SortedRun {
                    id: sr_id,
                    ssts: vec![sst_handle],
                };
                self.core.compacted.push(new_sr);
            }
            Ok(())
        }

        /// Build an SST with the given entries
        async fn build_sst(
            &self,
            mut entries: Vec<RowEntry>,
        ) -> Result<SsTableHandle, SlateDBError> {
            // Sort entries by key (required for SST builder)
            entries.sort_by(|a, b| a.key.cmp(&b.key));

            let mut builder = self.table_store.table_builder();

            for entry in entries {
                builder.add(entry)?;
            }

            let encoded = builder.build()?;
            let id = SsTableId::Compacted(Ulid::new());
            self.table_store.write_sst(&id, encoded, false).await
        }
    }

    impl DbStateReader for TestDbState {
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

    /// Enum to specify which layer an entry belongs to
    #[derive(Debug, Clone)]
    enum LayerLocation {
        WriteBatch,
        Memtable,
        ImmutableMemtable(usize), // index in the imm queue
        L0Sst(usize),             // SST index in L0 (0 = oldest)
        SortedRun(u32),           // sorted run ID (0 = oldest)
    }

    /// A test entry with its location and data
    #[derive(Debug, Clone)]
    struct TestEntry {
        location: LayerLocation,
        key: &'static [u8],
        value: Option<&'static [u8]>, // None = tombstone
        seq: u64,
        expire_ts: Option<i64>, // None = no expiration
    }

    impl TestEntry {
        fn to_row_entry(&self) -> RowEntry {
            let value = match self.value {
                Some(v) => ValueDeletable::Value(Bytes::from_static(v)),
                None => ValueDeletable::Tombstone,
            };
            RowEntry::new(
                Bytes::from_static(self.key),
                value,
                self.seq,
                None,
                self.expire_ts,
            )
        }
    }

    /// Helper to populate a TestDbState with entries organized by layer
    async fn populate_db_state(
        test_db_state: &mut TestDbState,
        entries: Vec<TestEntry>,
    ) -> Result<Option<WriteBatch>, SlateDBError> {
        // Group entries by layer
        let mut wb_batch: Option<WriteBatch> = None;
        let mut mem_entries = Vec::new();
        let mut imm_entries: HashMap<usize, Vec<RowEntry>> = HashMap::new();
        let mut l0_entries: HashMap<usize, Vec<RowEntry>> = HashMap::new();
        let mut sr_entries: HashMap<u32, Vec<RowEntry>> = HashMap::new();

        for entry in entries {
            let row_entry = entry.to_row_entry();
            match entry.location {
                LayerLocation::WriteBatch => {
                    if wb_batch.is_none() {
                        wb_batch = Some(WriteBatch::new());
                    }
                    if let Some(ref mut batch) = wb_batch {
                        if let Some(value) = entry.value {
                            batch.put(entry.key, value);
                        } else {
                            batch.delete(entry.key);
                        }
                    }
                }
                LayerLocation::Memtable => mem_entries.push(row_entry),
                LayerLocation::ImmutableMemtable(idx) => {
                    imm_entries.entry(idx).or_default().push(row_entry);
                }
                LayerLocation::L0Sst(idx) => {
                    l0_entries.entry(idx).or_default().push(row_entry);
                }
                LayerLocation::SortedRun(sr_id) => {
                    sr_entries.entry(sr_id).or_default().push(row_entry);
                }
            }
        }

        // Populate the database state

        // Add to memtable
        if !mem_entries.is_empty() {
            test_db_state.add_to_memtable(mem_entries);
        }

        // Add immutable memtables (in order)
        let mut imm_indices: Vec<_> = imm_entries.keys().copied().collect();
        imm_indices.sort();
        for idx in imm_indices {
            if let Some(entries) = imm_entries.remove(&idx) {
                test_db_state.add_immutable_memtable(entries);
            }
        }

        // Add L0 SSTs (higher index = newer, add in ascending order so highest ends up at front)
        let mut l0_indices: Vec<_> = l0_entries.keys().copied().collect();
        l0_indices.sort();
        for idx in l0_indices {
            if let Some(entries) = l0_entries.remove(&idx) {
                test_db_state.add_to_l0(entries).await?;
            }
        }

        // Add sorted runs (higher ID = newer, add in descending order so highest is checked first)
        let mut sr_ids: Vec<_> = sr_entries.keys().copied().collect();
        sr_ids.sort();
        sr_ids.reverse();
        for sr_id in sr_ids {
            if let Some(entries) = sr_entries.remove(&sr_id) {
                test_db_state.add_to_sorted_run(sr_id, entries).await?;
            }
        }

        Ok(wb_batch)
    }

    struct LayerPriorityTestCase {
        /// Test entries with their layer locations
        entries: Vec<TestEntry>,
        /// Key to query
        query_key: &'static [u8],
        /// Expected result
        expected: Option<&'static [u8]>,
        /// Test description
        description: &'static str,
        /// Current time for TTL filtering (None = 0)
        now: Option<i64>,
        /// Whether to allow dirty reads (default: false for realistic testing)
        dirty: bool,
        /// Oracle's last committed sequence (None = u64::MAX for all committed)
        last_committed_seq: Option<u64>,
        /// Maximum sequence number to read (for snapshot testing, None = no limit)
        max_seq: Option<u64>,
    }

    #[tokio::test]
    #[rstest]
    // Test 1: Write batch overrides all other layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::WriteBatch, key: b"key1", value: Some(b"wb_value"), seq: 100, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_value"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_value"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"wb_value"),
        description: "write batch should override memtable and L0", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 2: Memtable overrides L0 and sorted runs
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_value"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_value"), seq: 40, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr_value"), seq: 30, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"mem_value"),
        description: "memtable should override L0 and sorted run", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 3: Tombstone in write batch hides all lower layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::WriteBatch, key: b"key1", value: None, seq: 100, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_value"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_value"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone in write batch should hide all values", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 4: Tombstone in memtable hides L0
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: None, seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_value"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone in memtable should hide L0 value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 5: Tombstone in L0 hides sorted run
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: None, seq: 40, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr_value"), seq: 30, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone in L0 should hide sorted run value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 6: Value after tombstone (higher seq number)
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"new_value"), seq: 60, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: None, seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"new_value"),
        description: "newer value should override older tombstone", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 7: Multiple L0 SSTs - newest wins
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key1", value: Some(b"l0_newer"), seq: 45, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_older"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"l0_newer"),
        description: "newer L0 SST should win over older L0 SST", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 8: L0 overrides sorted run
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_value"), seq: 40, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr_value"), seq: 30, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"l0_value"),
        description: "L0 value should override sorted run value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 9: Nonexistent key returns None
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"other_key", value: Some(b"value"), seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "nonexistent key should return None", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 10: Only tombstone, no value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: None, seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone with no previous value should return None", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 11: Multiple layers all with same key, write batch wins
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::WriteBatch, key: b"key1", value: Some(b"wb"), seq: 100, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem"), seq: 90, expire_ts: None },
            TestEntry { location: LayerLocation::ImmutableMemtable(0), key: b"key1", value: Some(b"imm"), seq: 80, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0"), seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr"), seq: 60, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"wb"),
        description: "write batch should win", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 12: Multiple entries per L0 SST
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key1", value: Some(b"l0_0_val1"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key2", value: Some(b"l0_0_val2"), seq: 51, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_1_val1"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"l0_0_val1"),
        description: "first L0 SST entry should win", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 13: Multiple sorted runs
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::SortedRun(1), key: b"key1", value: Some(b"sr0"), seq: 30, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr1"), seq: 20, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"sr0"),
        description: "first sorted run should win", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 14: Multiple immutable memtables
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::ImmutableMemtable(0), key: b"key1", value: Some(b"imm0"), seq: 60, expire_ts: None },
            TestEntry { location: LayerLocation::ImmutableMemtable(1), key: b"key1", value: Some(b"imm1"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"imm0"),
        description: "first immutable memtable should win over second and L0", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 15: Complex scenario with multiple entries across all layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            // WriteBatch has multiple keys but not key1
            TestEntry { location: LayerLocation::WriteBatch, key: b"key2", value: Some(b"wb2"), seq: 100, expire_ts: None },
            // Memtable has key1 with high seq
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem"), seq: 90, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key3", value: Some(b"mem3"), seq: 91, expire_ts: None },
            // L0 has older versions
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key1", value: Some(b"l0_0"), seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_1"), seq: 60, expire_ts: None },
            // SR has oldest
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr"), seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"mem"),
        description: "memtable value should win in complex multi-layer scenario", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 16: Expired value in memtable should return None
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_value"), seq: 50, expire_ts: Some(100) },
        ],
        query_key: b"key1",
        expected: None,
        description: "expired value should return None", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 17: Expired value in L0 should not return older value from SR
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_value"), seq: 50, expire_ts: Some(100) },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr_old_value"), seq: 30, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "expired newer value should not revive older value", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 18: Non-expired value should be returned when now < expire_ts
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_value"), seq: 50, expire_ts: Some(200) },
        ],
        query_key: b"key1",
        expected: Some(b"mem_value"),
        description: "non-expired value should be returned", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 19: Expired value in memtable should not expose L0 value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_value"), seq: 60, expire_ts: Some(100) },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_value"), seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "expired memtable value should not expose L0", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 20: Mixed expired and non-expired values across layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key1", value: Some(b"l0_new_expired"), seq: 60, expire_ts: Some(100) },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_old_valid"), seq: 50, expire_ts: Some(200) },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr_value"), seq: 30, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "expired newer L0 should not expose older L0 even if valid", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 21: Tombstone prevents revival even when newer value expires
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key1", value: None, seq: 60, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_old_value"), seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone should prevent returning older value regardless of TTL", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 22: Value with no expiration should always be returned
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_value"), seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"mem_value"),
        description: "value with no expiration should be returned at any time", now: Some(1000000), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 23: Committed read filters out uncommitted data in memtable
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"uncommitted"), seq: 100, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "committed read should not see uncommitted data", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 24: Committed read sees committed data
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"committed"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"committed"),
        description: "committed read should see data within committed range", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 25: Uncommitted value doesn't hide older committed value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"uncommitted"), seq: 100, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"committed"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"committed"),
        description: "committed read should see older committed value when newer is uncommitted", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 26: Snapshot with max_seq filters newer values
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"newer"), seq: 80, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"older"), seq: 50, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"older"),
        description: "snapshot read should only see values up to max_seq", now: None, dirty: true, last_committed_seq: None, max_seq: Some(60),
    })]
    // Test 27: Snapshot with max_seq returns None when all values are newer
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"newer"), seq: 80, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "snapshot should return None when all values exceed max_seq", now: None, dirty: true, last_committed_seq: None, max_seq: Some(60),
    })]
    // Test 28: Combined max_seq and last_committed_seq filtering
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"v1"), seq: 100, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key1", value: Some(b"v2"), seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"v3"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"v3"),
        description: "should respect both max_seq and committed_seq constraints", now: None, dirty: false, last_committed_seq: Some(50), max_seq: Some(60),
    })]
    // Test 29: Tombstone within sequence bounds hides older values
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: None, seq: 45, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"old_value"), seq: 30, expire_ts: None },
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone within seq bounds should hide older values", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 30: Newer tombstone filtered out doesn't prevent reading older value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: None, seq: 100, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"old_value"), seq: 40, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"old_value"),
        description: "filtered tombstone should not hide visible older value", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 31: Sequence filtering works across all layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem"), seq: 90, expire_ts: None },
            TestEntry { location: LayerLocation::ImmutableMemtable(0), key: b"key1", value: Some(b"imm"), seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"sr"), seq: 30, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"l0"),
        description: "sequence filtering should work uniformly across all layers", now: None, dirty: false, last_committed_seq: Some(60), max_seq: None,
    })]
    // Test 32: Dirty read sees all uncommitted data
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"uncommitted"), seq: 100, expire_ts: None },
        ],
        query_key: b"key1",
        expected: Some(b"uncommitted"),
        description: "dirty read should see uncommitted data", now: None, dirty: true, last_committed_seq: Some(50), max_seq: None,
    })]
    async fn test_get_with_options_layer_priority(
        #[case] test_case: LayerPriorityTestCase,
    ) -> Result<(), SlateDBError> {
        // Create test database state and populate it
        let mut test_db_state = TestDbState::new().await;
        let write_batch = populate_db_state(&mut test_db_state, test_case.entries).await?;

        // Create Reader with test clock
        let stat_registry = StatRegistry::new();
        let db_stats = DbStats::new(&stat_registry);
        let test_clock = Arc::new(TestClock::new());
        // Set the clock to the test case's "now" value for TTL filtering
        if let Some(now) = test_case.now {
            test_clock.set(now);
        }
        let mono_clock = Arc::new(MonotonicClock::new(test_clock as Arc<dyn LogicalClock>, 0));

        // Create Oracle with appropriate last_committed_seq
        let oracle = Arc::new(Oracle::new(
            crate::utils::MonotonicSeq::new(0),
            Arc::new(DefaultSystemClock::new()),
        ));
        let last_committed_seq = test_case.last_committed_seq.unwrap_or(u64::MAX);
        oracle.last_committed_seq.store(last_committed_seq);

        let reader = Reader {
            table_store: test_db_state.table_store.clone(),
            db_stats,
            mono_clock,
            oracle,
        };

        // Call the actual get_with_options method
        let read_options = ReadOptions::default().with_dirty(test_case.dirty);
        let result = reader
            .get_with_options(
                test_case.query_key,
                &read_options,
                &test_db_state,
                write_batch.as_ref(),
                test_case.max_seq,
            )
            .await?;

        let actual = result.as_ref().map(|b| b.as_ref());
        let expected = test_case.expected;
        assert_eq!(actual, expected, "Failed test: {}", test_case.description);

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

    /// Test case for scan_with_options
    struct ScanTestCase {
        /// Test entries with their layer locations
        entries: Vec<TestEntry>,
        /// Start of range (inclusive)
        range_start: &'static [u8],
        /// End of range (exclusive)
        range_end: &'static [u8],
        /// Expected results in order
        expected: Vec<(&'static [u8], &'static [u8])>,
        /// Test description
        description: &'static str,
        /// Current time for TTL filtering (None = 0)
        now: Option<i64>,
        /// Whether to allow dirty reads (default: false for realistic testing)
        dirty: bool,
        /// Oracle's last committed sequence (None = u64::MAX for all committed)
        last_committed_seq: Option<u64>,
        /// Maximum sequence number to read (for snapshot testing, None = no limit)
        max_seq: Option<u64>,
    }

    #[tokio::test]
    #[rstest]
    // Test 1: Scan returns keys in order from single layer
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"val1"), seq: 10, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: Some(b"val2"), seq: 10, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key3", value: Some(b"val3"), seq: 10, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"val1"), (b"key2", b"val2"), (b"key3", b"val3")],
        description: "scan should return keys in order", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 2: Scan respects range boundaries
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"val1"), seq: 10, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: Some(b"val2"), seq: 10, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key3", value: Some(b"val3"), seq: 10, expire_ts: None },
        ],
        range_start: b"key2",
        range_end: b"key3",
        expected: vec![(b"key2", b"val2")],
        description: "scan should respect range boundaries (end exclusive)", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 3: Higher layer values override lower layers
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem_val"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"l0_val"), seq: 40, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key2", value: Some(b"l0_val2"), seq: 40, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"mem_val"), (b"key2", b"l0_val2")],
        description: "scan should prefer higher layer values", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 4: Tombstones hide values in lower layers
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"val1"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: None, seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key2", value: Some(b"old_val2"), seq: 40, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key3", value: Some(b"val3"), seq: 50, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"val1"), (b"key3", b"val3")],
        description: "tombstones should hide values from lower layers", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 5: Expired values are filtered out
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"val1"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: Some(b"expired"), seq: 50, expire_ts: Some(100) },
            TestEntry { location: LayerLocation::Memtable, key: b"key3", value: Some(b"val3"), seq: 50, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"val1"), (b"key3", b"val3")],
        description: "expired values should not appear in scan", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 6: Expired value doesn't revive older value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"expired"), seq: 60, expire_ts: Some(100) },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"old_value"), seq: 40, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key2",
        expected: vec![],
        description: "expired value should not revive older value in scan", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 7: Uncommitted values filtered in committed read
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"committed"), seq: 40, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: Some(b"uncommitted"), seq: 60, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key3", value: Some(b"committed"), seq: 30, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"committed"), (b"key3", b"committed")],
        description: "committed scan should filter uncommitted values", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 8: Uncommitted value doesn't hide older committed value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"uncommitted"), seq: 60, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"committed"), seq: 40, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key2",
        expected: vec![(b"key1", b"committed")],
        description: "uncommitted value should not hide committed value in scan", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 9: Snapshot with max_seq
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"v1_new"), seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"v1_old"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: Some(b"v2"), seq: 55, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"v1_old"), (b"key2", b"v2")],
        description: "snapshot scan should respect max_seq", now: None, dirty: true, last_committed_seq: None, max_seq: Some(60),
    })]
    // Test 10: Multiple layers with proper deduplication
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"mem"), seq: 80, expire_ts: None },
            TestEntry { location: LayerLocation::ImmutableMemtable(0), key: b"key2", value: Some(b"imm"), seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(1), key: b"key3", value: Some(b"l0_new"), seq: 60, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key3", value: Some(b"l0_old"), seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key4", value: Some(b"sr"), seq: 40, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key5",
        expected: vec![(b"key1", b"mem"), (b"key2", b"imm"), (b"key3", b"l0_new"), (b"key4", b"sr")],
        description: "scan should properly deduplicate across all layers", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 11: Empty range returns no results
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"val1"), seq: 10, expire_ts: None },
        ],
        range_start: b"key5",
        range_end: b"key9",
        expected: vec![],
        description: "scan of empty range should return no results", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 12: Scan with all keys deleted
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: None, seq: 50, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: None, seq: 50, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![],
        description: "scan with all tombstones should return empty", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 13: Complex sequence filtering across range
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: Some(b"uncommitted1"), seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"committed1"), seq: 40, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: Some(b"committed2"), seq: 45, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key3", value: Some(b"uncommitted3"), seq: 80, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"committed1"), (b"key2", b"committed2")],
        description: "complex committed scan should filter correctly", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 14: Tombstone within seq bounds prevents reading old value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: None, seq: 45, expire_ts: None },
            TestEntry { location: LayerLocation::SortedRun(0), key: b"key1", value: Some(b"old"), seq: 30, expire_ts: None },
            TestEntry { location: LayerLocation::Memtable, key: b"key2", value: Some(b"val2"), seq: 40, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key2", b"val2")],
        description: "tombstone in bounds should hide older value", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 15: Filtered tombstone doesn't hide visible value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry { location: LayerLocation::Memtable, key: b"key1", value: None, seq: 70, expire_ts: None },
            TestEntry { location: LayerLocation::L0Sst(0), key: b"key1", value: Some(b"visible"), seq: 40, expire_ts: None },
        ],
        range_start: b"key1",
        range_end: b"key2",
        expected: vec![(b"key1", b"visible")],
        description: "filtered tombstone should not hide visible value", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    async fn test_scan_with_options_layer_priority(
        #[case] test_case: ScanTestCase,
    ) -> Result<(), SlateDBError> {
        // Create test database state and populate it
        let mut test_db_state = TestDbState::new().await;
        let write_batch = populate_db_state(&mut test_db_state, test_case.entries).await?;

        // Create Reader with test clock
        let stat_registry = StatRegistry::new();
        let db_stats = DbStats::new(&stat_registry);
        let test_clock = Arc::new(TestClock::new());
        if let Some(now) = test_case.now {
            test_clock.set(now);
        }
        let mono_clock = Arc::new(MonotonicClock::new(test_clock as Arc<dyn LogicalClock>, 0));

        // Create Oracle with appropriate last_committed_seq
        let oracle = Arc::new(Oracle::new(
            crate::utils::MonotonicSeq::new(0),
            Arc::new(DefaultSystemClock::new()),
        ));
        let last_committed_seq = test_case.last_committed_seq.unwrap_or(u64::MAX);
        oracle.last_committed_seq.store(last_committed_seq);

        let reader = Reader {
            table_store: test_db_state.table_store.clone(),
            db_stats,
            mono_clock,
            oracle,
        };

        // Create range
        let range = BytesRange::from_slice(test_case.range_start..test_case.range_end);

        // Call the actual scan_with_options method
        let scan_options = ScanOptions::default().with_dirty(test_case.dirty);
        let mut iter = reader
            .scan_with_options(
                range,
                &scan_options,
                &test_db_state,
                write_batch.as_ref(),
                test_case.max_seq,
                None,
            )
            .await?;

        // Collect results
        let mut actual = Vec::new();
        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| SlateDBError::IoError(Arc::new(std::io::Error::other(e))))?
        {
            actual.push((kv.key.to_vec(), kv.value.to_vec()));
        }

        // Compare with expected
        let expected: Vec<(Vec<u8>, Vec<u8>)> = test_case
            .expected
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();

        assert_eq!(
            actual,
            expected,
            "Failed test: {}\nActual keys: {:?}\nExpected keys: {:?}",
            test_case.description,
            actual
                .iter()
                .map(|(k, _)| String::from_utf8_lossy(k))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(k, _)| String::from_utf8_lossy(k))
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
