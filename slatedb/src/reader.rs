use crate::bytes_range::BytesRange;
use crate::clock::MonotonicClock;
use crate::config::{DurabilityLevel, ReadOptions, ScanOptions};
use crate::db_state::{CoreDbState, SortedRun, SsTableHandle};
use crate::db_stats::DbStats;
use crate::filter_iterator::FilterIterator;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableMemtable, KVTable, MemTableIterator};
use crate::oracle::Oracle;
use crate::reader::SstFilterResult::{
    FilterNegative, FilterPositive, RangeNegative, RangePositive,
};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::{RowEntry, ValueDeletable};
use crate::utils::{get_now_for_read, is_not_expired};
use crate::{filter, DbIterator, SlateDBError};
use bytes::Bytes;
use futures::future::BoxFuture;
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

pub(crate) trait ReadSnapshot {
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

    /// Get the value for the given key, and return None if the value is expired.
    pub(crate) async fn get_with_options<K: AsRef<[u8]>>(
        &self,
        key: K,
        options: &ReadOptions,
        snapshot: &(dyn ReadSnapshot + Sync + Send),
        max_seq: Option<u64>,
    ) -> Result<Option<Bytes>, SlateDBError> {
        let now = get_now_for_read(self.mono_clock.clone(), options.durability_filter).await?;
        let max_seq = self.prepare_max_seq(max_seq, options.durability_filter, options.dirty);
        let get = LevelGet {
            key: key.as_ref(),
            max_seq,
            snapshot,
            table_store: self.table_store.clone(),
            db_stats: self.db_stats.clone(),
            now,
        };
        get.get().await
    }

    pub(crate) async fn scan_with_options<'a>(
        &'a self,
        range: BytesRange,
        options: &ScanOptions,
        snapshot: &(dyn ReadSnapshot + Sync),
        max_seq: Option<u64>,
    ) -> Result<DbIterator<'a>, SlateDBError> {
        let mut memtables = VecDeque::new();
        memtables.push_back(snapshot.memtable());
        for memtable in snapshot.imm_memtable() {
            memtables.push_back(memtable.table());
        }
        let memtable_iters: Vec<MemTableIterator> = memtables
            .iter()
            .map(|t| t.range_ascending(range.clone()))
            .collect();

        let read_ahead_blocks = self.table_store.bytes_to_blocks(options.read_ahead_bytes);

        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: read_ahead_blocks,
            cache_blocks: options.cache_blocks,
            eager_spawn: true,
        };

        let mut l0_iters = VecDeque::new();
        for sst in &snapshot.core().l0 {
            if let Some(iter) = SstIterator::new_owned(
                range.clone(),
                sst.clone(),
                self.table_store.clone(),
                sst_iter_options,
            )
            .await?
            {
                l0_iters.push_back(iter);
            }
        }

        let mut sr_iters = VecDeque::new();
        for sr in &snapshot.core().compacted {
            let iter = SortedRunIterator::new_owned(
                range.clone(),
                sr.clone(),
                self.table_store.clone(),
                sst_iter_options,
            )
            .await?;
            sr_iters.push_back(iter);
        }

        DbIterator::new(range.clone(), memtable_iters, l0_iters, sr_iters, max_seq).await
    }
}

struct LevelGet<'a> {
    key: &'a [u8],
    max_seq: Option<u64>,
    snapshot: &'a (dyn ReadSnapshot + Sync + Send),
    table_store: Arc<TableStore>,
    db_stats: DbStats,
    now: i64,
}

impl<'a> LevelGet<'a> {
    async fn get(&'a self) -> Result<Option<Bytes>, SlateDBError> {
        let getters: Vec<BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>>> =
            vec![self.get_memtable(), self.get_l0(), self.get_compacted()];

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
            let maybe_val = std::iter::once(self.snapshot.memtable())
                .chain(self.snapshot.imm_memtable().iter().map(|imm| imm.table()))
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

            for sst in &self.snapshot.core().l0 {
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

            for sr in &self.snapshot.core().compacted {
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
    use crate::object_stores::ObjectStores;
    use crate::{sst::SsTableFormat, stats::StatRegistry, types::ValueDeletable};
    use object_store::{memory::InMemory, path::Path};
    use rstest::rstest;

    struct MockReadSnapshot {
        memtable: Arc<KVTable>,
        imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    }

    impl ReadSnapshot for MockReadSnapshot {
        fn memtable(&self) -> Arc<KVTable> {
            self.memtable.clone()
        }

        fn imm_memtable(&self) -> &VecDeque<Arc<ImmutableMemtable>> {
            &self.imm_memtable
        }

        fn core(&self) -> &CoreDbState {
            todo!()
        }
    }

    fn mock_read_snapshot() -> MockReadSnapshot {
        MockReadSnapshot {
            memtable: Arc::new(KVTable::new()),
            imm_memtable: VecDeque::new(),
        }
    }

    fn mock_level_getters<'a>(
        row_entries: Vec<Option<RowEntry>>,
    ) -> Vec<BoxFuture<'a, Result<Option<RowEntry>, SlateDBError>>> {
        row_entries
            .into_iter()
            .map(|entry| async move { Ok(entry) }.boxed())
            .collect()
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
        use crate::db_cache::DbCacheWrapper;

        let mock_read_snapshot = mock_read_snapshot();
        let stat_registry = StatRegistry::new();
        let get = LevelGet {
            key: b"key",
            max_seq: None,
            snapshot: &mock_read_snapshot,
            table_store: Arc::new(TableStore::new(
                ObjectStores::new(Arc::new(InMemory::new()), None),
                SsTableFormat::default(),
                Path::from(""),
                Arc::new(DbCacheWrapper::new(None, None, &StatRegistry::new())),
            )),
            db_stats: DbStats::new(&stat_registry),
            now: 10000,
        };

        let result = get.get_inner(mock_level_getters(test_case.entries)).await?;
        assert_eq!(result, test_case.expected);
        Ok(())
    }
}
