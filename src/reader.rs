use crate::bytes_range::BytesRange;
use crate::config::{DurabilityLevel, ReadOptions, ScanOptions};
use crate::db_state::{CoreDbState, SortedRun, SsTableHandle};
use crate::db_stats::DbStats;
use crate::filter_iterator::FilterIterator;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableMemtable, ImmutableWal, KVTable, VecDequeKeyValueIterator};
use crate::reader::SstFilterResult::{
    FilterNegative, FilterPositive, RangeNegative, RangePositive,
};
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::utils::{get_now_for_read, is_not_expired, MonotonicClock};
use crate::{filter, DbIterator, SlateDBError};
use bytes::Bytes;
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
    pub(crate) wal_enabled: bool,
}

impl Reader {
    fn include_wal_memtables(&self, durability_filter: DurabilityLevel) -> bool {
        matches!(durability_filter, DurabilityLevel::Memory)
    }

    fn include_memtables(&self, durability_filter: DurabilityLevel) -> bool {
        if self.wal_enabled {
            true
        } else {
            matches!(durability_filter, DurabilityLevel::Memory)
        }
    }

    pub(crate) async fn get_with_options<K: AsRef<[u8]>>(
        &self,
        key: K,
        options: &ReadOptions,
        snapshot: &(dyn ReadSnapshot + Sync),
    ) -> Result<Option<Bytes>, SlateDBError> {
        let key = key.as_ref();
        let ttl_now = get_now_for_read(self.mono_clock.clone(), options.durability_filter).await?;

        if self.include_wal_memtables(options.durability_filter) {
            // TODO(flaneur): FIX THIS BEFORE MERGING
            /*
            let maybe_val = std::iter::once(snapshot.wal())
                .chain(snapshot.imm_wal().iter().map(|imm| imm.table()))
                .find_map(|memtable| memtable.get(key));
            if let Some(val) = maybe_val {
                return Ok(Self::unwrap_value_if_not_expired(&val, ttl_now));
            }
            */
        }

        if self.include_memtables(options.durability_filter) {
            let maybe_val = std::iter::once(snapshot.memtable())
                .chain(snapshot.imm_memtable().iter().map(|imm| imm.table()))
                .find_map(|memtable| memtable.get(key));
            if let Some(val) = maybe_val {
                return Ok(Self::unwrap_value_if_not_expired(&val, ttl_now));
            }
        }

        // Since the key remains unchanged during the point query, we only need to compute
        // the hash value once and pass it to the filter to avoid unnecessary hash computation
        let key_hash = filter::filter_hash(key);

        // cache blocks that are being read
        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };

        for sst in &snapshot.core().l0 {
            let filter_result = self.sst_might_include_key(sst, key, key_hash).await?;
            self.record_filter_result(&filter_result);

            if filter_result.might_contain_key() {
                let iter =
                    SstIterator::for_key(sst, key, self.table_store.clone(), sst_iter_options)
                        .await?;

                let mut ttl_iter = FilterIterator::wrap_ttl_filter_iterator(iter, ttl_now);
                if let Some(entry) = ttl_iter.next_entry().await? {
                    if entry.key == key {
                        return Ok(entry.value.as_bytes());
                    }
                }
                if matches!(filter_result, FilterPositive) {
                    self.db_stats.sst_filter_false_positives.inc();
                }
            }
        }

        for sr in &snapshot.core().compacted {
            let filter_result = self.sr_might_include_key(sr, key, key_hash).await?;
            self.record_filter_result(&filter_result);

            if filter_result.might_contain_key() {
                let iter =
                    SortedRunIterator::for_key(sr, key, self.table_store.clone(), sst_iter_options)
                        .await?;

                let mut ttl_iter = FilterIterator::wrap_ttl_filter_iterator(iter, ttl_now);
                if let Some(entry) = ttl_iter.next_entry().await? {
                    if entry.key == key {
                        return Ok(entry.value.as_bytes());
                    }
                }
                if matches!(filter_result, FilterPositive) {
                    self.db_stats.sst_filter_false_positives.inc();
                }
            }
        }
        Ok(None)
    }

    pub(crate) async fn scan_with_options<'a>(
        &'a self,
        range: BytesRange,
        options: &ScanOptions,
        snapshot: &(dyn ReadSnapshot + Sync),
    ) -> Result<DbIterator<'a>, SlateDBError> {
        let mut memtables = VecDeque::new();

        if self.include_wal_memtables(options.durability_filter) {
            // TODO(flaneur): FIX THIS BEFORE MERGING
            /*
            memtables.push_back(Arc::clone(&snapshot.wal()));
            for imm_wal in snapshot.imm_wal() {
                memtables.push_back(imm_wal.table());
            }
            */
            todo!()
        }

        if self.include_memtables(options.durability_filter) {
            memtables.push_back(Arc::clone(&snapshot.memtable()));
            for memtable in snapshot.imm_memtable() {
                memtables.push_back(memtable.table());
            }
        }

        let mem_iter =
            VecDequeKeyValueIterator::materialize_range(memtables, range.clone()).await?;

        let read_ahead_blocks = self.table_store.bytes_to_blocks(options.read_ahead_bytes);

        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: read_ahead_blocks,
            cache_blocks: options.cache_blocks,
            eager_spawn: true,
        };

        let mut l0_iters = VecDeque::new();
        for sst in &snapshot.core().l0 {
            let iter = SstIterator::new_owned(
                range.clone(),
                sst.clone(),
                self.table_store.clone(),
                sst_iter_options,
            )
            .await?;
            l0_iters.push_back(iter);
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

        // TODO(flaneur): load max committed seq here
        DbIterator::new(range.clone(), mem_iter, l0_iters, sr_iters, None).await
    }

    fn unwrap_value_if_not_expired(entry: &RowEntry, now_ttl: i64) -> Option<Bytes> {
        if is_not_expired(entry, now_ttl) {
            entry.value.as_bytes()
        } else {
            None
        }
    }

    fn record_filter_result(&self, result: &SstFilterResult) {
        if matches!(result, FilterPositive) {
            self.db_stats.sst_filter_positives.inc();
        } else if matches!(result, FilterNegative) {
            self.db_stats.sst_filter_negatives.inc();
        }
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
}
