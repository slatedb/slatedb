use crate::bytes_range::BytesRange;
use crate::config::ReadLevel::Uncommitted;
use crate::config::{ReadOptions, ScanOptions};
use crate::db_state::DbStateSnapshot;
use crate::db_stats::DbStats;
use crate::filter_iterator::FilterIterator;
use crate::iter::KeyValueIterator;
use crate::mem_table::VecDequeKeyValueIterator;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::SstFilterResult::{FilterNegative, FilterPositive};
use crate::tablestore::{SstFilterResult, TableStore};
use crate::types::RowEntry;
use crate::utils::{get_now_for_read, is_not_expired, MonotonicClock};
use crate::{filter, DbIterator, SlateDBError};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;

pub(crate) trait ReaderStateSupplier {
    fn supply(&self) -> DbStateSnapshot;
}

pub(crate) struct Reader {
    pub(crate) supplier: Arc<dyn ReaderStateSupplier + Send + Sync>,
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) db_stats: DbStats,
    pub(crate) mono_clock: Arc<MonotonicClock>,
}

impl Reader {
    pub(crate) async fn get_with_options<K: AsRef<[u8]>>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        let key = key.as_ref();
        let snapshot = self.supplier.supply();
        let ttl_now = get_now_for_read(self.mono_clock.clone(), options.read_level).await?;

        if matches!(options.read_level, Uncommitted) {
            let maybe_val = std::iter::once(snapshot.wal)
                .chain(snapshot.state.imm_wal.iter().map(|imm| imm.table()))
                .find_map(|memtable| memtable.get(key));
            if let Some(val) = maybe_val {
                return Ok(Self::unwrap_value_if_not_expired(&val, ttl_now));
            }
        }

        let maybe_val = std::iter::once(snapshot.memtable)
            .chain(snapshot.state.imm_memtable.iter().map(|imm| imm.table()))
            .find_map(|memtable| memtable.get(key));
        if let Some(val) = maybe_val {
            return Ok(Self::unwrap_value_if_not_expired(&val, ttl_now));
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

        for sst in &snapshot.state.core().l0 {
            let filter_result = self
                .table_store
                .sst_might_include_key(sst, key, key_hash)
                .await?;
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

        for sr in &snapshot.state.core().compacted {
            let filter_result = self
                .table_store
                .sr_might_include_key(sr, key, key_hash)
                .await?;
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
    ) -> Result<DbIterator<'a>, SlateDBError> {
        let snapshot = self.supplier.supply();
        let mut memtables = VecDeque::new();

        if matches!(options.read_level, Uncommitted) {
            memtables.push_back(Arc::clone(&snapshot.wal));
            for imm_wal in &snapshot.state.imm_wal {
                memtables.push_back(imm_wal.table());
            }
        }

        memtables.push_back(Arc::clone(&snapshot.memtable));
        for memtable in &snapshot.state.imm_memtable {
            memtables.push_back(memtable.table());
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
        for sst in &snapshot.state.core().l0 {
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
        for sr in &snapshot.state.core().compacted {
            let iter = SortedRunIterator::new_owned(
                range.clone(),
                sr.clone(),
                self.table_store.clone(),
                sst_iter_options,
            )
            .await?;
            sr_iters.push_back(iter);
        }

        DbIterator::new(range.clone(), mem_iter, l0_iters, sr_iters).await
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
}
