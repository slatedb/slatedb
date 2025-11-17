use crate::batch::{WriteBatch, WriteBatchIterator};
use crate::bytes_range::BytesRange;
use crate::clock::MonotonicClock;
use crate::config::{DurabilityLevel, ReadOptions, ScanOptions};
use crate::db_state::CoreDbState;
use crate::db_stats::DbStats;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::mem_table::{ImmutableMemtable, KVTable};
use crate::oracle::Oracle;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::utils::get_now_for_read;
use crate::utils::{build_concurrent, compute_max_parallel};
use crate::{db_iter::DbIteratorRangeTracker, error::SlateDBError, DbIterator};

use bytes::Bytes;
use futures::future::join;
use std::collections::VecDeque;
use std::sync::Arc;

pub(crate) trait DbStateReader {
    fn memtable(&self) -> Arc<KVTable>;
    fn imm_memtable(&self) -> &VecDeque<Arc<ImmutableMemtable>>;
    fn core(&self) -> &CoreDbState;
}

struct IteratorSources {
    write_batch_iter: Option<WriteBatchIterator>,
    mem_iters: Vec<Box<dyn KeyValueIterator + 'static>>,
    l0_iters: VecDeque<Box<dyn KeyValueIterator + 'static>>,
    sr_iters: VecDeque<Box<dyn KeyValueIterator + 'static>>,
}

pub(crate) struct Reader {
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) db_stats: DbStats,
    pub(crate) mono_clock: Arc<MonotonicClock>,
    pub(crate) oracle: Arc<Oracle>,
    pub(crate) merge_operator: Option<crate::merge_operator::MergeOperatorType>,
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

    async fn build_iterator_sources(
        &self,
        range: &BytesRange,
        db_state: &(dyn DbStateReader + Sync),
        write_batch: Option<WriteBatch>,
        sst_iter_options: SstIteratorOptions,
        point_lookup_stats: Option<DbStats>,
    ) -> Result<IteratorSources, SlateDBError> {
        let write_batch_iter = write_batch
            .map(|batch| WriteBatchIterator::new(batch, range.clone(), IterationOrder::Ascending));

        let mut memtables = VecDeque::new();
        memtables.push_back(db_state.memtable());
        for memtable in db_state.imm_memtable() {
            memtables.push_back(memtable.table());
        }
        let mem_iters = memtables
            .iter()
            .map(|table| {
                Box::new(table.range_ascending(range.clone()))
                    as Box<dyn KeyValueIterator + 'static>
            })
            .collect::<Vec<_>>();

        let max_parallel =
            compute_max_parallel(db_state.core().l0.len(), &db_state.core().compacted, 4);

        let (l0_iters, sr_iters) = if let Some(point_key) = range.as_point().cloned() {
            let l0 = self.build_point_l0_iters(
                range,
                db_state,
                sst_iter_options,
                point_lookup_stats.clone(),
            )?;
            let sr = self.build_point_sr_iters(
                range,
                &point_key,
                db_state,
                sst_iter_options,
                point_lookup_stats,
            )?;
            (l0, sr)
        } else {
            let l0_future =
                self.build_range_l0_iters(range, db_state, sst_iter_options, max_parallel);
            let sr_future =
                self.build_range_sr_iters(range, db_state, sst_iter_options, max_parallel);
            let (l0_res, sr_res) = join(l0_future, sr_future).await;
            (l0_res?, sr_res?)
        };

        Ok(IteratorSources {
            write_batch_iter,
            mem_iters,
            l0_iters,
            sr_iters,
        })
    }

    fn build_point_l0_iters<'a>(
        &self,
        range: &BytesRange,
        db_state: &(dyn DbStateReader + Sync),
        sst_iter_options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<VecDeque<Box<dyn KeyValueIterator + 'a>>, SlateDBError> {
        let mut iters = VecDeque::new();
        for sst in &db_state.core().l0 {
            let iterator = SstIterator::new_owned_with_stats(
                range.clone(),
                sst.clone(),
                self.table_store.clone(),
                sst_iter_options,
                db_stats.clone(),
            )?;
            if let Some(iterator) = iterator {
                iters.push_back(Box::new(iterator) as Box<dyn KeyValueIterator + 'a>);
            }
        }
        Ok(iters)
    }

    fn build_point_sr_iters<'a>(
        &self,
        range: &BytesRange,
        key: &Bytes,
        db_state: &(dyn DbStateReader + Sync),
        sst_iter_options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<VecDeque<Box<dyn KeyValueIterator + 'a>>, SlateDBError> {
        let mut iters = VecDeque::new();
        for sr in &db_state.core().compacted {
            if let Some(handle) = sr.find_sst_with_range_covering_key(key.as_ref()) {
                let iterator = SstIterator::new_owned_with_stats(
                    range.clone(),
                    handle.clone(),
                    self.table_store.clone(),
                    sst_iter_options,
                    db_stats.clone(),
                )?;
                if let Some(iterator) = iterator {
                    iters.push_back(Box::new(iterator) as Box<dyn KeyValueIterator + 'a>);
                }
            }
        }
        Ok(iters)
    }

    async fn build_range_l0_iters<'a>(
        &self,
        range: &BytesRange,
        db_state: &(dyn DbStateReader + Sync),
        sst_iter_options: SstIteratorOptions,
        max_parallel: usize,
    ) -> Result<VecDeque<Box<dyn KeyValueIterator + 'a>>, SlateDBError> {
        let range_clone = range.clone();
        let table_store = self.table_store.clone();
        build_concurrent(
            db_state.core().l0.iter().cloned(),
            max_parallel,
            move |sst| {
                let table_store = table_store.clone();
                let range = range_clone.clone();
                async move {
                    SstIterator::new_owned_initialized(
                        range.clone(),
                        sst,
                        table_store,
                        sst_iter_options,
                    )
                    .await
                    .map(|maybe_iter| {
                        maybe_iter.map(|iter| Box::new(iter) as Box<dyn KeyValueIterator + 'a>)
                    })
                }
            },
        )
        .await
    }

    async fn build_range_sr_iters<'a>(
        &self,
        range: &BytesRange,
        db_state: &(dyn DbStateReader + Sync),
        sst_iter_options: SstIteratorOptions,
        max_parallel: usize,
    ) -> Result<VecDeque<Box<dyn KeyValueIterator + 'a>>, SlateDBError> {
        let range_clone = range.clone();
        let table_store = self.table_store.clone();
        build_concurrent(
            db_state.core().compacted.iter().cloned(),
            max_parallel,
            move |sr| {
                let table_store = table_store.clone();
                let range = range_clone.clone();
                async move {
                    SortedRunIterator::new_owned_initialized(
                        range.clone(),
                        sr,
                        table_store,
                        sst_iter_options,
                    )
                    .await
                    .map(|iter| Some(Box::new(iter) as Box<dyn KeyValueIterator + 'a>))
                }
            },
        )
        .await
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
    ///   dirty read or cache blocks.
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
        write_batch: Option<WriteBatch>,
        max_seq: Option<u64>,
    ) -> Result<Option<Bytes>, SlateDBError> {
        let now = get_now_for_read(self.mono_clock.clone(), options.durability_filter).await?;
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);
        let key_slice = key.as_ref();
        let target_key = Bytes::copy_from_slice(key_slice);
        let range = BytesRange::from_slice(key_slice..=key_slice);

        let sst_iter_options = SstIteratorOptions {
            cache_blocks: options.cache_blocks,
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };

        let IteratorSources {
            write_batch_iter,
            mem_iters,
            l0_iters,
            sr_iters,
        } = self
            .build_iterator_sources(
                &range,
                db_state,
                write_batch,
                sst_iter_options,
                Some(self.db_stats.clone()),
            )
            .await?;

        let mut iterator = DbIterator::new(
            range,
            write_batch_iter,
            mem_iters,
            l0_iters,
            sr_iters,
            max_seq,
            None,
            now,
            self.merge_operator.clone(),
        )
        .await?;

        if let Some(entry) = iterator.next_key_value().await? {
            if entry.key == target_key {
                return Ok(Some(entry.value));
            }
        }

        Ok(None)
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
    pub(crate) async fn scan_with_options(
        &self,
        range: BytesRange,
        options: &ScanOptions,
        db_state: &(dyn DbStateReader + Sync),
        write_batch: Option<WriteBatch>,
        max_seq: Option<u64>,
        range_tracker: Option<Arc<DbIteratorRangeTracker>>,
    ) -> Result<DbIterator, SlateDBError> {
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);
        let now = get_now_for_read(self.mono_clock.clone(), options.durability_filter).await?;

        let read_ahead_blocks = self.table_store.bytes_to_blocks(options.read_ahead_bytes);

        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: options.max_fetch_tasks,
            blocks_to_fetch: read_ahead_blocks,
            cache_blocks: options.cache_blocks,
            eager_spawn: true,
        };

        let IteratorSources {
            write_batch_iter,
            mem_iters,
            l0_iters,
            sr_iters,
        } = self
            .build_iterator_sources(&range, db_state, write_batch, sst_iter_options, None)
            .await?;

        DbIterator::new(
            range,
            write_batch_iter,
            mem_iters,
            l0_iters,
            sr_iters,
            max_seq,
            range_tracker,
            now,
            self.merge_operator.clone(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_operator::{MergeOperator, MergeOperatorError};
    use crate::types::{RowEntry, ValueDeletable};
    use rstest::rstest;

    use crate::batch::WriteBatch;
    use crate::clock::{LogicalClock, MonotonicClock};
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

    /// A simple merge operator for testing that concatenates byte strings
    struct StringConcatMergeOperator;

    impl MergeOperator for StringConcatMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operand: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            match existing_value {
                Some(base) => {
                    let mut merged = base.to_vec();
                    merged.extend_from_slice(&operand);
                    Ok(Bytes::from(merged))
                }
                None => Ok(operand),
            }
        }
    }

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
        value: ValueDeletable,
        seq: u64,
        expire_ts: Option<i64>, // None = no expiration
    }

    impl TestEntry {
        fn value(key: &'static [u8], val: &'static [u8], seq: u64) -> Self {
            Self {
                location: LayerLocation::Memtable,
                key,
                value: ValueDeletable::Value(Bytes::from_static(val)),
                seq,
                expire_ts: None,
            }
        }

        fn tombstone(key: &'static [u8], seq: u64) -> Self {
            Self {
                location: LayerLocation::Memtable,
                key,
                value: ValueDeletable::Tombstone,
                seq,
                expire_ts: None,
            }
        }

        fn merge(key: &'static [u8], val: &'static [u8], seq: u64) -> Self {
            Self {
                location: LayerLocation::Memtable,
                key,
                value: ValueDeletable::Merge(Bytes::from_static(val)),
                seq,
                expire_ts: None,
            }
        }

        fn with_location(mut self, location: LayerLocation) -> Self {
            self.location = location;
            self
        }

        fn with_expire_ts(mut self, expire_ts: i64) -> Self {
            self.expire_ts = Some(expire_ts);
            self
        }

        fn to_row_entry(&self) -> RowEntry {
            RowEntry::new(
                Bytes::from_static(self.key),
                self.value.clone(),
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
                        match &entry.value {
                            ValueDeletable::Value(v) => {
                                batch.put(entry.key, v.as_ref());
                            }
                            ValueDeletable::Tombstone => {
                                batch.delete(entry.key);
                            }
                            ValueDeletable::Merge(v) => {
                                batch.merge(entry.key, v.as_ref());
                            }
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
            TestEntry::value(b"key1", b"wb_value", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"mem_value", 50),
            TestEntry::value(b"key1", b"l0_value", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"wb_value"),
        description: "write batch should override memtable and L0", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 2: Memtable overrides L0 and sorted runs
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem_value", 50),
            TestEntry::value(b"key1", b"l0_value", 40).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"sr_value", 30).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"mem_value"),
        description: "memtable should override L0 and sorted run", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 3: Tombstone in write batch hides all lower layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"mem_value", 50),
            TestEntry::value(b"key1", b"l0_value", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone in write batch should hide all values", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 4: Tombstone in memtable hides L0
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 50),
            TestEntry::value(b"key1", b"l0_value", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone in memtable should hide L0 value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 5: Tombstone in L0 hides sorted run
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 40).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"sr_value", 30).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone in L0 should hide sorted run value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 6: Value after tombstone (higher seq number)
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"new_value", 60),
            TestEntry::tombstone(b"key1", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"new_value"),
        description: "newer value should override older tombstone", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 7: Multiple L0 SSTs - newest wins
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"l0_newer", 45).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key1", b"l0_older", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"l0_newer"),
        description: "newer L0 SST should win over older L0 SST", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 8: L0 overrides sorted run
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"l0_value", 40).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"sr_value", 30).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"l0_value"),
        description: "L0 value should override sorted run value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 9: Nonexistent key returns None
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"other_key", b"value", 50),
        ],
        query_key: b"key1",
        expected: None,
        description: "nonexistent key should return None", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 10: Only tombstone, no value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 50),
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone with no previous value should return None", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 11: Multiple layers all with same key, write batch wins
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"wb", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"mem", 90),
            TestEntry::value(b"key1", b"imm", 80).with_location(LayerLocation::ImmutableMemtable(0)),
            TestEntry::value(b"key1", b"l0", 70).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"sr", 60).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"wb"),
        description: "write batch should win", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 12: Multiple entries per L0 SST
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"l0_0_val1", 50).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key2", b"l0_0_val2", 51).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key1", b"l0_1_val1", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"l0_0_val1"),
        description: "first L0 SST entry should win", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 13: Multiple sorted runs
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"sr0", 30).with_location(LayerLocation::SortedRun(1)),
            TestEntry::value(b"key1", b"sr1", 20).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"sr0"),
        description: "first sorted run should win", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 14: Multiple immutable memtables
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"imm0", 60).with_location(LayerLocation::ImmutableMemtable(0)),
            TestEntry::value(b"key1", b"imm1", 50).with_location(LayerLocation::ImmutableMemtable(1)),
            TestEntry::value(b"key1", b"l0", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"imm0"),
        description: "first immutable memtable should win over second and L0", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 15: Complex scenario with multiple entries across all layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            // WriteBatch has multiple keys but not key1
            TestEntry::value(b"key2", b"wb2", 100).with_location(LayerLocation::WriteBatch),
            // Memtable has key1 with high seq
            TestEntry::value(b"key1", b"mem", 90),
            TestEntry::value(b"key3", b"mem3", 91),
            // L0 has older versions
            TestEntry::value(b"key1", b"l0_0", 70).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key1", b"l0_1", 60).with_location(LayerLocation::L0Sst(0)),
            // SR has oldest
            TestEntry::value(b"key1", b"sr", 50).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"mem"),
        description: "memtable value should win in complex multi-layer scenario", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 16: Expired value in memtable should return None
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem_value", 50).with_expire_ts(100),
        ],
        query_key: b"key1",
        expected: None,
        description: "expired value should return None", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 17: Expired value in L0 should not return older value from SR
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"l0_value", 50).with_location(LayerLocation::L0Sst(0)).with_expire_ts(100),
            TestEntry::value(b"key1", b"sr_old_value", 30).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "expired newer value should not revive older value", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 18: Non-expired value should be returned when now < expire_ts
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem_value", 50).with_expire_ts(200),
        ],
        query_key: b"key1",
        expected: Some(b"mem_value"),
        description: "non-expired value should be returned", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 19: Expired value in memtable should not expose L0 value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem_value", 60).with_expire_ts(100),
            TestEntry::value(b"key1", b"l0_value", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "expired memtable value should not expose L0", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 20: Mixed expired and non-expired values across layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"l0_new_expired", 60).with_location(LayerLocation::L0Sst(1)).with_expire_ts(100),
            TestEntry::value(b"key1", b"l0_old_valid", 50).with_location(LayerLocation::L0Sst(0)).with_expire_ts(200),
            TestEntry::value(b"key1", b"sr_value", 30).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "expired newer L0 should not expose older L0 even if valid", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 21: Tombstone prevents revival even when newer value expires
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 60).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key1", b"l0_old_value", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone should prevent returning older value regardless of TTL", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 22: Value with no expiration should always be returned
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem_value", 50),
        ],
        query_key: b"key1",
        expected: Some(b"mem_value"),
        description: "value with no expiration should be returned at any time", now: Some(1000000), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 23: Committed read filters out uncommitted data in memtable
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"uncommitted", 100),
        ],
        query_key: b"key1",
        expected: None,
        description: "committed read should not see uncommitted data", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 24: Committed read sees committed data
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"committed", 40),
        ],
        query_key: b"key1",
        expected: Some(b"committed"),
        description: "committed read should see data within committed range", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 25: Uncommitted value doesn't hide older committed value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"uncommitted", 100),
            TestEntry::value(b"key1", b"committed", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"committed"),
        description: "committed read should see older committed value when newer is uncommitted", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 26: Snapshot with max_seq filters newer values
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"newer", 80),
            TestEntry::value(b"key1", b"older", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"older"),
        description: "snapshot read should only see values up to max_seq", now: None, dirty: true, last_committed_seq: None, max_seq: Some(60),
    })]
    // Test 27: Snapshot with max_seq returns None when all values are newer
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"newer", 80),
        ],
        query_key: b"key1",
        expected: None,
        description: "snapshot should return None when all values exceed max_seq", now: None, dirty: true, last_committed_seq: None, max_seq: Some(60),
    })]
    // Test 28: Combined max_seq and last_committed_seq filtering
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"v1", 100),
            TestEntry::value(b"key1", b"v2", 70).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key1", b"v3", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"v3"),
        description: "should respect both max_seq and committed_seq constraints", now: None, dirty: false, last_committed_seq: Some(50), max_seq: Some(60),
    })]
    // Test 29: Tombstone within sequence bounds hides older values
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 45).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"old_value", 30).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: None,
        description: "tombstone within seq bounds should hide older values", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 30: Newer tombstone filtered out doesn't prevent reading older value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 100),
            TestEntry::value(b"key1", b"old_value", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"old_value"),
        description: "filtered tombstone should not hide visible older value", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 31: Sequence filtering works across all layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem", 90),
            TestEntry::value(b"key1", b"imm", 70).with_location(LayerLocation::ImmutableMemtable(0)),
            TestEntry::value(b"key1", b"l0", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"sr", 30).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"l0"),
        description: "sequence filtering should work uniformly across all layers", now: None, dirty: false, last_committed_seq: Some(60), max_seq: None,
    })]
    // Test 32: Dirty read sees all uncommitted data
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"uncommitted", 100),
        ],
        query_key: b"key1",
        expected: Some(b"uncommitted"),
        description: "dirty read should see uncommitted data", now: None, dirty: true, last_committed_seq: Some(50), max_seq: None,
    })]
    // NOTE: for tests that use WriteBatch, the order in which the merge operations are listed
    // in the test case is important. The first merge should be the "oldest" that happened because
    // the WriteBatch assumes all sequence numbers are u64::MAX.
    // Test 33: WriteBatch with merge operations merges with memtable value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"wb_merge", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"mem_value", 50),
            TestEntry::value(b"key1", b"l0_value", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"mem_valuewb_merge"),  // Merge("mem_value", "wb_merge") = "mem_valuewb_merge"
        description: "[MERGE] write batch merge should merge with memtable value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 34: WriteBatch with multiple merge operations for same key
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"merge1", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"merge2", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"base", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"basemerge1merge2"),  // Merge(base, merge1, merge2) = "basemerge1merge2"
        description: "[MERGE] multiple write batch merges should merge with base value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 35: WriteBatch merge with tombstone clears history
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"old_merge", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::tombstone(b"key1", 90).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"new_merge", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"base", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"new_merge"),  // Tombstone clears history, only merge after tombstone applies
        description: "[MERGE] write batch tombstone should clear merge history", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 36: WriteBatch merge with value acts as new base
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"old_merge", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"new_base", 90).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"merge", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"old_base", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"new_basemerge"),  // Value acts as barrier, only Merge("new_base", "merge") applies
        description: "[MERGE] write batch value should act as new base for subsequent merges", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 37: WriteBatch merges without base values
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"merge1", u64::MAX).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"merge2", u64::MAX).with_location(LayerLocation::WriteBatch),
        ],
        query_key: b"key1",
        expected: Some(b"merge1merge2"),  // Only merge operands, no base value
        description: "[MERGE] write batch merges without base values should merge together", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 33: Single merge operand without base value acts as base
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"a", 50),
        ],
        query_key: b"key1",
        expected: Some(b"a"),  // Single merge operand with no base returns the operand
        description: "[MERGE] single merge operand without base should be returned as-is", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 34: Merge operand with base value should merge
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"b", 60),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"ab"),  // Merge("a", "b") = "ab" (concatenation)
        description: "[MERGE] merge operand should be merged with base value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 35: Multiple merge operands should be applied in sequence number order
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 70),
            TestEntry::merge(b"key1", b"b", 60).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"abc"),  // Merge(Merge("a", "b"), "c") = "abc"
        description: "[MERGE] multiple merge operands should be applied in sequence order", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 36: Tombstone clears all merge history
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"d", 80),
            TestEntry::tombstone(b"key1", 70).with_location(LayerLocation::L0Sst(1)),
            TestEntry::merge(b"key1", b"c", 60).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"ab", 50).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"d"),  // Tombstone acts as barrier, only merge after tombstone is applied
        description: "[MERGE] tombstone should clear merge history", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 37: Value after merges acts as new base
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"z", 90),
            TestEntry::value(b"key1", b"y", 80).with_location(LayerLocation::L0Sst(1)),
            TestEntry::merge(b"key1", b"x", 70).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"w", 60).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"yz"),  // Value("y") acts as barrier, only Merge("y", "z") is applied
        description: "[MERGE] value should act as new base for subsequent merges", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 38: Merges across multiple layers
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"d", 100),
            TestEntry::merge(b"key1", b"c", 90).with_location(LayerLocation::ImmutableMemtable(0)),
            TestEntry::merge(b"key1", b"b", 80).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"a", 70).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"abcd"),  // Merge(Merge(Merge("a", "b"), "c"), "d") = "abcd"
        description: "[MERGE] merges should work across all layers", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 39: Expired merge operand should be filtered
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 70).with_expire_ts(100),
            TestEntry::merge(b"key1", b"b", 60).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"ab"),  // Expired merge is filtered, only Merge("a", "b") is applied
        description: "[MERGE] expired merge operand should be filtered out", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 40: Expired base value with merge operands still applies merges
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"b", 70),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::L0Sst(0)).with_expire_ts(100),
        ],
        query_key: b"key1",
        expected: Some(b"b"),  // Base value expired (None passed to merge operator), merge operands still applied
        description: "[MERGE] expired base value passes None to merge operator, merge operands still apply", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 41: Mixed TTLs - only non-expired merges are applied
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"d", 80).with_expire_ts(200),  // Not expired
            TestEntry::merge(b"key1", b"c", 70).with_location(LayerLocation::L0Sst(1)).with_expire_ts(100),  // Expired
            TestEntry::merge(b"key1", b"b", 60).with_location(LayerLocation::L0Sst(0)),  // No TTL
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"abd"),  // Merge(Merge("a", "b"), "d") = "abd" (expired "c" is filtered)
        description: "[MERGE] only non-expired merge operands should be applied", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 42: Merge with sequence filtering
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 100),  // Filtered (uncommitted)
            TestEntry::merge(b"key1", b"b", 45),  // Visible
            TestEntry::value(b"key1", b"a", 40).with_location(LayerLocation::L0Sst(0)),  // Visible
        ],
        query_key: b"key1",
        expected: Some(b"ab"),  // Merge("a", "b") = "ab" (seq 100 filtered)
        description: "[MERGE] merge operands should respect sequence filtering", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 43: Snapshot isolation with merge operands
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 80),  // Filtered by snapshot
            TestEntry::merge(b"key1", b"b", 55),  // Visible in snapshot
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::L0Sst(0)),  // Visible in snapshot
        ],
        query_key: b"key1",
        expected: Some(b"ab"),  // Merge("a", "b") = "ab" (seq 80 filtered by snapshot)
        description: "[MERGE] snapshot should filter merge operands by max_seq", now: None, dirty: true, last_committed_seq: None, max_seq: Some(60),
    })]
    // Test 44: Only merge operands, no tombstone/value base
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 70),
            TestEntry::merge(b"key1", b"b", 60).with_location(LayerLocation::L0Sst(0)),
            TestEntry::merge(b"key1", b"a", 50).with_location(LayerLocation::SortedRun(0)),
        ],
        query_key: b"key1",
        expected: Some(b"abc"),  // String concatenation: "" + "a" + "b" + "c" = "abc"
        description: "[MERGE] multiple merge operands without base should merge together", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 45: Tombstone after value prevents merge from applying to old value
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"new", 80),
            TestEntry::tombstone(b"key1", 70).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key1", b"old", 60).with_location(LayerLocation::L0Sst(0)),
        ],
        query_key: b"key1",
        expected: Some(b"new"),  // Tombstone blocks old value, merge becomes new base
        description: "[MERGE] tombstone should prevent merge from applying to older value", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 46: Merge operands in same layer (memtable)
    #[case(LayerPriorityTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 70),
            TestEntry::merge(b"key1", b"b", 60),
            TestEntry::merge(b"key1", b"a", 50),
        ],
        query_key: b"key1",
        expected: Some(b"abc"),  // All merges in memtable: Merge(Merge("a", "b"), "c") = "abc"
        description: "[MERGE] multiple merge operands in same layer should merge in seq order", now: None, dirty: true, last_committed_seq: None, max_seq: None,
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
        let oracle = Arc::new(Oracle::new(crate::utils::MonotonicSeq::new(0)));
        let last_committed_seq = test_case.last_committed_seq.unwrap_or(u64::MAX);
        oracle.last_committed_seq.store(last_committed_seq);

        // Enable merge operator if the test description contains "[MERGE]"
        let merge_operator = if test_case.description.contains("[MERGE]") {
            Some(Arc::new(StringConcatMergeOperator) as Arc<dyn MergeOperator + Send + Sync>)
        } else {
            None
        };

        let reader = Reader {
            table_store: test_db_state.table_store.clone(),
            db_stats,
            mono_clock,
            oracle,
            merge_operator,
        };

        // Call the actual get_with_options method
        let read_options = ReadOptions::default().with_dirty(test_case.dirty);
        let result = reader
            .get_with_options(
                test_case.query_key,
                &read_options,
                &test_db_state,
                write_batch,
                test_case.max_seq,
            )
            .await?;

        let actual = result.as_ref().map(|b| b.as_ref());
        let expected = test_case.expected;
        assert_eq!(
            actual,
            expected,
            "Failed test: {}\nActual: {:?}\nExpected: {:?}",
            test_case.description,
            actual.map(|b| String::from_utf8_lossy(b)),
            expected.map(|b| String::from_utf8_lossy(b))
        );

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
            TestEntry::value(b"key1", b"val1", 10),
            TestEntry::value(b"key2", b"val2", 10),
            TestEntry::value(b"key3", b"val3", 10),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"val1"), (b"key2", b"val2"), (b"key3", b"val3")],
        description: "scan should return keys in order", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 2: Scan respects range boundaries
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"val1", 10),
            TestEntry::value(b"key2", b"val2", 10),
            TestEntry::value(b"key3", b"val3", 10),
        ],
        range_start: b"key2",
        range_end: b"key3",
        expected: vec![(b"key2", b"val2")],
        description: "scan should respect range boundaries (end exclusive)", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 3: Higher layer values override lower layers
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem_val", 50),
            TestEntry::value(b"key1", b"l0_val", 40).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"l0_val2", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"mem_val"), (b"key2", b"l0_val2")],
        description: "scan should prefer higher layer values", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 4: Tombstones hide values in lower layers
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"val1", 50),
            TestEntry::tombstone(b"key2", 50),
            TestEntry::value(b"key2", b"old_val2", 40).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key3", b"val3", 50),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"val1"), (b"key3", b"val3")],
        description: "tombstones should hide values from lower layers", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 5: Expired values are filtered out
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"val1", 50),
            TestEntry::value(b"key2", b"expired", 50).with_expire_ts(100),
            TestEntry::value(b"key3", b"val3", 50),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"val1"), (b"key3", b"val3")],
        description: "expired values should not appear in scan", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 6: Expired value doesn't revive older value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"expired", 60).with_expire_ts(100),
            TestEntry::value(b"key1", b"old_value", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key2",
        expected: vec![],
        description: "expired value should not revive older value in scan", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 7: Uncommitted values filtered in committed read
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"committed", 40),
            TestEntry::value(b"key2", b"uncommitted", 60),
            TestEntry::value(b"key3", b"committed", 30),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"committed"), (b"key3", b"committed")],
        description: "committed scan should filter uncommitted values", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 8: Uncommitted value doesn't hide older committed value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"uncommitted", 60),
            TestEntry::value(b"key1", b"committed", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key2",
        expected: vec![(b"key1", b"committed")],
        description: "uncommitted value should not hide committed value in scan", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 9: Snapshot with max_seq
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"v1_new", 70),
            TestEntry::value(b"key1", b"v1_old", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"v2", 55),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"v1_old"), (b"key2", b"v2")],
        description: "snapshot scan should respect max_seq", now: None, dirty: true, last_committed_seq: None, max_seq: Some(60),
    })]
    // Test 10: Multiple layers with proper deduplication
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"mem", 80),
            TestEntry::value(b"key2", b"imm", 70).with_location(LayerLocation::ImmutableMemtable(0)),
            TestEntry::value(b"key3", b"l0_new", 60).with_location(LayerLocation::L0Sst(1)),
            TestEntry::value(b"key3", b"l0_old", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key4", b"sr", 40).with_location(LayerLocation::SortedRun(0)),
        ],
        range_start: b"key1",
        range_end: b"key5",
        expected: vec![(b"key1", b"mem"), (b"key2", b"imm"), (b"key3", b"l0_new"), (b"key4", b"sr")],
        description: "scan should properly deduplicate across all layers", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 11: Empty range returns no results
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"val1", 10),
        ],
        range_start: b"key5",
        range_end: b"key9",
        expected: vec![],
        description: "scan of empty range should return no results", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 12: Scan with all keys deleted
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 50),
            TestEntry::tombstone(b"key2", 50),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![],
        description: "scan with all tombstones should return empty", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 13: Complex sequence filtering across range
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::value(b"key1", b"uncommitted1", 70),
            TestEntry::value(b"key1", b"committed1", 40).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"committed2", 45),
            TestEntry::value(b"key3", b"uncommitted3", 80),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"committed1"), (b"key2", b"committed2")],
        description: "complex committed scan should filter correctly", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 14: Tombstone within seq bounds prevents reading old value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 45).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"old", 30).with_location(LayerLocation::SortedRun(0)),
            TestEntry::value(b"key2", b"val2", 40),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key2", b"val2")],
        description: "tombstone in bounds should hide older value", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 15: Filtered tombstone doesn't hide visible value
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::tombstone(b"key1", 70),
            TestEntry::value(b"key1", b"visible", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key2",
        expected: vec![(b"key1", b"visible")],
        description: "filtered tombstone should not hide visible value", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // ========================================
    // MERGE OPERATOR TESTS FOR SCAN OPERATIONS
    // ========================================
    // Test 16: Scan with merge operands
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"b", 60),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"x", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"ab"), (b"key2", b"x")],  // key1: Merge("a", "b") = "ab"
        description: "[MERGE SCAN] should merge operands with base values during scan", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 17: Scan with multiple keys having merge operands
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"2", 60),
            TestEntry::value(b"key1", b"1", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::merge(b"key2", b"b", 60),
            TestEntry::value(b"key2", b"a", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::merge(b"key3", b"y", 60),
            TestEntry::value(b"key3", b"x", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"12"), (b"key2", b"ab"), (b"key3", b"xy")],
        description: "[MERGE SCAN] should merge operands for multiple keys", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 18: Scan with tombstone clearing merge history
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"new", 70),
            TestEntry::tombstone(b"key1", 60).with_location(LayerLocation::L0Sst(1)),
            TestEntry::merge(b"key1", b"c", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"x", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"new"), (b"key2", b"x")],  // Tombstone clears history
        description: "[MERGE SCAN] tombstone should clear merge history in scan", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 19: Scan with expired merge operands
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 70).with_expire_ts(100),  // Expired
            TestEntry::merge(b"key1", b"b", 60).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::SortedRun(0)),
            TestEntry::value(b"key2", b"x", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"ab"), (b"key2", b"x")],  // Expired merge filtered
        description: "[MERGE SCAN] expired merge operands should be filtered in scan", now: Some(150), dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 20: Scan with only merge operands (no base values)
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"b", 60),
            TestEntry::merge(b"key1", b"a", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::merge(b"key2", b"y", 60),
            TestEntry::merge(b"key2", b"x", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"ab"), (b"key2", b"xy")],
        description: "[MERGE SCAN] should merge operands without base values", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 21: Scan with merge operands across layers
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"d", 80),
            TestEntry::merge(b"key1", b"c", 70).with_location(LayerLocation::ImmutableMemtable(0)),
            TestEntry::merge(b"key1", b"b", 60).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::SortedRun(0)),
        ],
        range_start: b"key1",
        range_end: b"key2",
        expected: vec![(b"key1", b"abcd")],  // Merge across all layers: "a"+"b"+"c"+"d" = "abcd"
        description: "[MERGE SCAN] should merge operands across multiple layers", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 22: Scan with sequence filtering and merge operands
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"c", 100),  // Filtered (uncommitted)
            TestEntry::merge(b"key1", b"b", 45),
            TestEntry::value(b"key1", b"a", 40).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"x", 40).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"ab"), (b"key2", b"x")],  // seq 100 filtered
        description: "[MERGE SCAN] should filter merge operands by sequence number", now: None, dirty: false, last_committed_seq: Some(50), max_seq: None,
    })]
    // Test 23: Scan with mixed values, merges, and tombstones
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"b", 60),
            TestEntry::value(b"key1", b"a", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::tombstone(b"key2", 60),
            TestEntry::merge(b"key2", b"z", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::merge(b"key3", b"y", 60),
            TestEntry::merge(b"key3", b"x", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"ab"), (b"key3", b"xy")],  // key2 tombstoned
        description: "[MERGE SCAN] should handle mix of values, merges, and tombstones", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 24: Scan with WriteBatch merge operations
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"wb_merge", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"base", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"normal", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"basewb_merge"), (b"key2", b"normal")],  // key1: Merge("base", "wb_merge")
        description: "[MERGE SCAN] should merge write batch operands with base values", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 25: Scan with multiple WriteBatch merge operations for same key
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"merge1", 80).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"merge2", 90).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"merge3", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"base", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"normal", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"basemerge1merge2merge3"), (b"key2", b"normal")],  // Multiple merges applied in sequence
        description: "[MERGE SCAN] should apply multiple write batch merges in sequence", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 26: Scan with WriteBatch tombstone clearing merge history
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"old_merge", 80).with_location(LayerLocation::WriteBatch),
            TestEntry::tombstone(b"key1", 90).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"new_merge", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"base", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"normal", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"new_merge"), (b"key2", b"normal")],  // Tombstone clears history
        description: "[MERGE SCAN] write batch tombstone should clear merge history", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 27: Scan with WriteBatch value acting as new base for merges
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"old_merge", 80).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"new_base", 90).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"final_merge", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"old_base", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::value(b"key2", b"normal", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"new_basefinal_merge"), (b"key2", b"normal")],  // Value acts as barrier
        description: "[MERGE SCAN] write batch value should act as new base for subsequent merges", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 28: Scan with WriteBatch merges across multiple keys
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"1_merge", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key1", b"1_base", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::merge(b"key2", b"2_merge", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key2", b"2_base", 50).with_location(LayerLocation::L0Sst(0)),
            TestEntry::merge(b"key3", b"3_merge", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key3", b"3_base", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key4",
        expected: vec![(b"key1", b"1_base1_merge"), (b"key2", b"2_base2_merge"), (b"key3", b"3_base3_merge")],
        description: "[MERGE SCAN] should merge write batch operands for multiple keys", now: None, dirty: true, last_committed_seq: None, max_seq: None,
    })]
    // Test 29: Scan with WriteBatch merges without base values
    #[case(ScanTestCase {
        entries: vec![
            TestEntry::merge(b"key1", b"merge1", 90).with_location(LayerLocation::WriteBatch),
            TestEntry::merge(b"key1", b"merge2", 100).with_location(LayerLocation::WriteBatch),
            TestEntry::value(b"key2", b"normal", 50).with_location(LayerLocation::L0Sst(0)),
        ],
        range_start: b"key1",
        range_end: b"key3",
        expected: vec![(b"key1", b"merge1merge2"), (b"key2", b"normal")],  // Only merge operands, no base value
        description: "[MERGE SCAN] should merge write batch operands without base values", now: None, dirty: true, last_committed_seq: None, max_seq: None,
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
        let oracle = Arc::new(Oracle::new(crate::utils::MonotonicSeq::new(0)));
        let last_committed_seq = test_case.last_committed_seq.unwrap_or(u64::MAX);
        oracle.last_committed_seq.store(last_committed_seq);

        // Enable merge operator if the test description contains "[MERGE"
        let merge_operator = if test_case.description.contains("[MERGE") {
            Some(Arc::new(StringConcatMergeOperator) as Arc<dyn MergeOperator + Send + Sync>)
        } else {
            None
        };

        let reader = Reader {
            table_store: test_db_state.table_store.clone(),
            db_stats,
            mono_clock,
            oracle,
            merge_operator,
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
                write_batch,
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
            "Failed test: {}\nActual entries: {:?}\nExpected entries: {:?}",
            test_case.description,
            actual
                .iter()
                .map(|(k, v)| format!(
                    "({}, {})",
                    String::from_utf8_lossy(k),
                    String::from_utf8_lossy(v)
                ))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(k, v)| format!(
                    "({}, {})",
                    String::from_utf8_lossy(k),
                    String::from_utf8_lossy(v)
                ))
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
