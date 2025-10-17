use crate::db_state::{CoreDbState, SsTableId};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::WritableKVTable;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio::task;
use tokio::task::JoinHandle;

pub(crate) struct WalReplayOptions {
    /// The number of SSTs to preload while replaying
    pub(crate) sst_batch_size: usize,

    /// The minimum number of bytes in each returned table
    /// (save the final table, which may be arbitrarily small).
    pub(crate) min_memtable_bytes: usize,

    /// The maximum number of bytes in each returned table
    pub(crate) max_memtable_bytes: usize,

    /// Options to pass through to underlying SST iterators
    pub(crate) sst_iter_options: SstIteratorOptions,
}

impl Default for WalReplayOptions {
    fn default() -> Self {
        Self {
            sst_batch_size: 4,
            min_memtable_bytes: 64 * 1024 * 1024,
            max_memtable_bytes: 128 * 1024 * 1024,
            sst_iter_options: SstIteratorOptions::default(),
        }
    }
}

pub(crate) struct ReplayedMemtable {
    pub(crate) table: WritableKVTable,
    pub(crate) last_tick: i64,
    pub(crate) last_seq: u64,
    pub(crate) last_wal_id: u64,
}

struct IteratorHolder<T> {
    initialized: bool,
    current_iter: Option<T>,
}

impl<T> IteratorHolder<T> {
    fn new() -> Self {
        Self {
            initialized: false,
            current_iter: None,
        }
    }

    fn is_finished(&self) -> bool {
        self.initialized && self.current_iter.is_none()
    }

    fn advance(&mut self, iterator: Option<T>) {
        self.initialized = true;
        self.current_iter = iterator;
    }
}

struct ReplayedRow {
    row_entry: RowEntry,
    wal_id: u64,
}

pub(crate) struct WalReplayIterator<'a> {
    options: WalReplayOptions,
    wal_id_range: Range<u64>,
    table_store: Arc<TableStore>,
    current_iter: IteratorHolder<SstIterator<'a>>,
    next_iters: VecDeque<JoinHandle<Result<Option<SstIterator<'a>>, SlateDBError>>>,
    overflow_row: Option<ReplayedRow>,
    last_tick: i64,
    last_seq: u64,
    min_seq: u64,
    next_wal_id: u64,
}

impl WalReplayIterator<'_> {
    pub(crate) async fn range(
        wal_id_range: Range<u64>,
        db_state: &CoreDbState,
        options: WalReplayOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        let sst_batch_size = options.sst_batch_size;
        if sst_batch_size < 1 {
            return Err(SlateDBError::InvalidSSTBatchSize(sst_batch_size));
        }

        // load the last seq number from manifest, and use it as the starting seq number to avoid
        // replaying the entries that are already in the L0 SST. while replaying the WALs, we'll
        // update the last seq number to the max seq number, and this final `last_seq` will be passed
        // to the db_state for the further writes.
        let min_seq = db_state.last_l0_seq;
        let last_seq = db_state.last_l0_seq;
        let last_tick = db_state.last_l0_clock_tick;
        let next_wal_id = wal_id_range.start;

        let mut replay_iter = WalReplayIterator {
            options,
            wal_id_range,
            table_store: Arc::clone(&table_store),
            current_iter: IteratorHolder::new(),
            next_iters: VecDeque::new(),
            overflow_row: None,
            last_tick,
            last_seq,
            min_seq,
            next_wal_id,
        };

        for _ in 0..sst_batch_size {
            if !replay_iter.maybe_load_next_iter() {
                break;
            }
        }

        Ok(replay_iter)
    }

    pub(crate) async fn new(
        db_state: &CoreDbState,
        options: WalReplayOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        let wal_id_start = db_state.replay_after_wal_id + 1;
        let wal_id_end = table_store.last_seen_wal_id().await?;
        let wal_id_range = wal_id_start..(wal_id_end + 1);
        Self::range(wal_id_range, db_state, options, table_store).await
    }

    fn maybe_load_next_iter(&mut self) -> bool {
        if !self.wal_id_range.contains(&self.next_wal_id)
            || self.next_iters.len() >= self.options.sst_batch_size
        {
            return false;
        }

        let next_wal_id = self.next_wal_id;
        self.next_wal_id += 1;

        async fn load_iter<'a>(
            wal_id: u64,
            sst_iter_options: SstIteratorOptions,
            table_store: Arc<TableStore>,
        ) -> Result<Option<SstIterator<'a>>, SlateDBError> {
            let sst = table_store.open_sst(&SsTableId::Wal(wal_id)).await?;
            SstIterator::new_owned_initialized(.., sst, Arc::clone(&table_store), sst_iter_options)
                .await
        }

        let handle = task::spawn(load_iter(
            next_wal_id,
            self.options.sst_iter_options,
            Arc::clone(&self.table_store),
        ));
        self.next_iters.push_back(handle);
        true
    }

    async fn advance_current_iter(&mut self) -> Result<(), SlateDBError> {
        let next_iter = if let Some(join_handle) = self.next_iters.pop_front() {
            match join_handle.await {
                Ok(Ok(sst_iter)) => sst_iter,
                Ok(Err(slate_err)) => return Err(slate_err),
                Err(join_err) => {
                    return Err(SlateDBError::BackgroundTaskPanic(Arc::new(Mutex::new(
                        join_err.try_into_panic().unwrap_or_else(|_| {
                            Box::new("Load of SST iterator panicked or was cancelled")
                        }),
                    ))))
                }
            }
        } else {
            None
        };
        self.current_iter.advance(next_iter);
        Ok(())
    }

    /// Get the next table replayed from the WAL. The next table is guaranteed to
    /// have a size at least as large as [`WalReplayOptions::min_memtable_bytes`]
    /// unless it is the final table replayed from the WAL. The final table may
    /// even be empty since writers use an empty WAL to fence zombie writers.
    /// The empty table must still be returned so that replay logic can account for
    /// the latest WAL ID.
    pub(crate) async fn next(&mut self) -> Result<Option<ReplayedMemtable>, SlateDBError> {
        if self.current_iter.is_finished() && self.overflow_row.is_none() {
            return Ok(None);
        }

        let table = WritableKVTable::new();
        let mut last_wal_id = 0;

        if let Some(overflow_row) = self.overflow_row.take() {
            let row_entry = overflow_row.row_entry;
            if let Some(ts) = row_entry.create_ts {
                self.last_tick = self.last_tick.max(ts);
            }
            self.last_seq = self.last_seq.max(row_entry.seq);
            table.put(row_entry);
            last_wal_id = overflow_row.wal_id;
        }

        while !self.current_iter.is_finished() {
            if let Some(sst_iter) = &mut self.current_iter.current_iter {
                let wal_id = sst_iter.table_id().unwrap_wal_id();
                while let Some(row_entry) = sst_iter.next_entry().await? {
                    // skip the entries that are already in the L0 SST.
                    if row_entry.seq <= self.min_seq {
                        continue;
                    }

                    // if the table is full, we'll overflow the row to the next iterator.
                    let meta = table.metadata();
                    if self.table_store.estimate_encoded_size(
                        meta.entry_num + 1,
                        meta.entries_size_in_bytes + row_entry.estimated_size(),
                    ) > self.options.max_memtable_bytes
                    {
                        self.overflow_row.replace(ReplayedRow { row_entry, wal_id });
                        break;
                    }

                    if let Some(ts) = row_entry.create_ts {
                        self.last_tick = self.last_tick.max(ts);
                    }
                    self.last_seq = self.last_seq.max(row_entry.seq);
                    table.put(row_entry);
                }

                let table_overflowed = self.overflow_row.is_some();
                if !table.is_empty() || !table_overflowed {
                    last_wal_id = wal_id;
                }

                let meta = table.metadata();
                if table_overflowed
                    || self
                        .table_store
                        .estimate_encoded_size(meta.entry_num, meta.entries_size_in_bytes)
                        > self.options.min_memtable_bytes
                {
                    break;
                }
            }

            self.maybe_load_next_iter();
            self.advance_current_iter().await?
        }

        if last_wal_id > 0 {
            Ok(Some(ReplayedMemtable {
                table,
                last_tick: self.last_tick,
                last_seq: self.last_seq,
                last_wal_id,
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WalReplayIterator, WalReplayOptions};
    use crate::bytes_range::BytesRange;
    use crate::db_state::{CoreDbState, SsTableId};
    use crate::iter::{IterationOrder, KeyValueIterator};
    use crate::mem_table::WritableKVTable;
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::{rng, sample};
    use crate::sst::SsTableFormat;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::{error::SlateDBError, test_utils};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use proptest::test_runner::TestRng;
    use rand::Rng;
    use std::cmp::min;
    use std::collections::btree_map::Iter;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_replay_empty_wal() {
        let table_store = test_table_store();
        write_empty_wal(1, Arc::clone(&table_store)).await.unwrap();
        let mut replay_iter = WalReplayIterator::new(
            &CoreDbState::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(table) = replay_iter.next().await.unwrap() else {
            panic!("Expected empty table to be returned from iterator")
        };

        assert_eq!(table.last_wal_id, 1);
        assert_eq!(table.last_seq, 0);
        assert!(table.table.is_empty());
        assert_eq!(table.last_tick, i64::MIN);
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_replay_all_entries() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let entries = sample::table(&mut rng, 1000, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::new(
            &CoreDbState::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id + 1, next_wal_id);

        let mut imm_table_iter = replayed_table.table.table().iter();
        test_utils::assert_ranged_kv_scan(
            &entries,
            &BytesRange::from(..),
            IterationOrder::Ascending,
            &mut imm_table_iter,
        )
        .await;
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_enforce_min_memtable_bytes() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let num_entries = 5000;
        let entries = sample::table(&mut rng, num_entries, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        let min_memtable_bytes = 1024;
        let mut replay_iter = WalReplayIterator::new(
            &CoreDbState::new(),
            WalReplayOptions {
                min_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let full_replayed_table = WritableKVTable::new();
        let mut last_wal_id = 0;
        let mut replayed_entries = 0;

        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            last_wal_id = replayed_table.last_wal_id;
            replayed_entries += replayed_table.table.metadata().entries_size_in_bytes;

            // The last table may be less than `min_memtable_bytes`
            if replayed_entries < num_entries {
                assert!(replayed_table.table.metadata().entries_size_in_bytes > min_memtable_bytes);
            }

            let mut iter = replayed_table.table.table().iter();
            while let Some(next_entry) = iter.next_entry().await.unwrap() {
                full_replayed_table.put(next_entry);
            }
        }
        assert_eq!(last_wal_id + 1, next_wal_id);

        let mut full_replayed_iter = full_replayed_table.table().iter();
        test_utils::assert_ranged_kv_scan(
            &entries,
            &BytesRange::from(..),
            IterationOrder::Ascending,
            &mut full_replayed_iter,
        )
        .await;
    }

    #[tokio::test]
    async fn should_enforce_max_memtable_bytes() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let num_entries = 5000;
        let entries = sample::table(&mut rng, num_entries, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        let max_memtable_bytes = 1024;
        let mut replay_iter = WalReplayIterator::new(
            &CoreDbState::new(),
            WalReplayOptions {
                min_memtable_bytes: usize::MAX,
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let full_replayed_table = WritableKVTable::new();
        let mut last_wal_id = 0;

        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            last_wal_id = replayed_table.last_wal_id;
            assert!(replayed_table.table.metadata().entries_size_in_bytes <= max_memtable_bytes);

            let mut iter = replayed_table.table.table().iter();
            while let Some(next_entry) = iter.next_entry().await.unwrap() {
                full_replayed_table.put(next_entry);
            }
        }
        assert_eq!(last_wal_id + 1, next_wal_id);

        let mut full_replayed_iter = full_replayed_table.table().iter();
        test_utils::assert_ranged_kv_scan(
            &entries,
            &BytesRange::from(..),
            IterationOrder::Ascending,
            &mut full_replayed_iter,
        )
        .await;
    }

    #[tokio::test]
    async fn should_only_replay_wals_after_last_l0_flushed_wal_id() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let compacted_entries = sample::table(&mut rng, 1000, 10);
        let mut next_wal_id = 1;

        next_wal_id = write_wals(
            &compacted_entries,
            next_wal_id,
            &mut rng,
            200,
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let replay_after_wal_id = next_wal_id - 1;
        let non_compacted_entries = sample::table(&mut rng, 1000, 10);
        next_wal_id = write_wals(
            &non_compacted_entries,
            next_wal_id,
            &mut rng,
            200,
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut db_state = CoreDbState::new();
        db_state.replay_after_wal_id = replay_after_wal_id;
        db_state.next_wal_sst_id = replay_after_wal_id + 1;

        let mut replay_iter = WalReplayIterator::new(
            &db_state,
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id + 1, next_wal_id);

        let mut imm_table_iter = replayed_table.table.table().iter();
        test_utils::assert_ranged_kv_scan(
            &non_compacted_entries,
            &BytesRange::from(..),
            IterationOrder::Ascending,
            &mut imm_table_iter,
        )
        .await;
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_replay_wals_after_min_seq() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let entries = sample::table(&mut rng, 1000, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        // Set min_seq to skip the first half of entries
        let min_seq = 500;
        let mut db_state = CoreDbState::new();
        db_state.last_l0_seq = min_seq;
        db_state.last_l0_clock_tick = 0;

        let mut replay_iter = WalReplayIterator::new(
            &db_state,
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id + 1, next_wal_id);

        // Verify that only entries with seq > min_seq are replayed
        let mut imm_table_iter = replayed_table.table.table().iter();
        let mut replayed_entries = BTreeMap::new();
        let mut total = 0;
        while let Some(entry) = imm_table_iter.next_entry().await.unwrap() {
            assert!(entry.seq > min_seq);
            replayed_entries.insert(entry.key.clone(), entry.value);
            total += 1;
        }
        assert_eq!(total, 500);
    }

    fn test_table_store() -> Arc<TableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat::default(),
            path.clone(),
            None,
        ))
    }

    /// Write a sequence of WALs with a random (bounded) number of entries.
    /// Return the ID of the next WAL.
    async fn write_wals(
        entries: &BTreeMap<Bytes, Bytes>,
        next_wal_id: u64,
        rng: &mut TestRng,
        max_wal_entries: usize,
        table_store: Arc<TableStore>,
    ) -> Result<u64, SlateDBError> {
        let mut iter = entries.iter();
        let mut next_seq = 1;
        let mut total_wal_entries = 0;
        let mut next_wal_id = next_wal_id;

        while total_wal_entries < entries.len() {
            let wal_entries = min(
                entries.len() - total_wal_entries,
                rng.random_range(0..max_wal_entries),
            );
            next_seq = write_wal(
                next_wal_id,
                next_seq,
                &mut iter,
                wal_entries,
                Arc::clone(&table_store),
            )
            .await?;
            next_wal_id += 1;
            total_wal_entries += wal_entries;
        }
        Ok(next_wal_id)
    }

    async fn write_empty_wal(
        wal_id: u64,
        table_store: Arc<TableStore>,
    ) -> Result<(), SlateDBError> {
        let empty_entries = BTreeMap::new();
        let mut empty_iter = empty_entries.iter();
        let _ = write_wal(wal_id, 0, &mut empty_iter, 0, table_store).await?;
        Ok(())
    }

    async fn write_wal(
        wal_id: u64,
        next_seq: u64,
        entries: &mut Iter<'_, Bytes, Bytes>,
        max_entries: usize,
        table_store: Arc<TableStore>,
    ) -> Result<u64, SlateDBError> {
        let mut writer = table_store.table_writer(SsTableId::Wal(wal_id));
        let mut next_seq = next_seq;
        while next_seq < next_seq + (max_entries as u64) {
            let Some((key, value)) = entries.next() else {
                break;
            };
            writer
                .add(RowEntry::new_value(key, value, next_seq))
                .await?;
            next_seq += 1;
        }
        writer.close().await?;
        Ok(next_seq)
    }
}
