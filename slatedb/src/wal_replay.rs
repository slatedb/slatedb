use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::iter::{EmptyIterator, RowEntryIterator};
use crate::manifest::ManifestCore;
use crate::manifest::SsTableView;
use crate::mem_table::WritableKVTable;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::utils::panic_string;
use log::error;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::task;
use tokio::task::JoinHandle;
use crate::wal;
use crate::wal::{WalError, WalIterator as WalIteratorTrait, WalRows};

pub(crate) struct WalReplayOptions {
    /// The number of SSTs to preload while replaying
    pub(crate) sst_batch_size: usize,

    /// The target maximum number of bytes in each returned table. WAL replay only
    /// splits between write batches (all rows of one commit stay in one table), so
    /// a returned table may exceed this if a single write batch is larger.
    pub(crate) max_memtable_bytes: usize,

    /// Options to pass through to underlying SST iterators
    pub(crate) sst_iter_options: SstIteratorOptions,

    /// The minimum seq number to replay. If unset, will replay all
    /// entries after `last_l0_seq` in the manifest.
    pub(crate) min_seq: Option<u64>,
}

impl Default for WalReplayOptions {
    fn default() -> Self {
        Self {
            sst_batch_size: 4,
            max_memtable_bytes: 64 * 1024 * 1024,
            sst_iter_options: SstIteratorOptions::default(),
            min_seq: None,
        }
    }
}

pub(crate) struct ReplayedMemtable {
    pub(crate) table: WritableKVTable,
    pub(crate) last_tick: i64,
    pub(crate) last_seq: u64,
    pub(crate) last_wal_id: u64,
}

pub(crate) struct WalReplayIterator {
    options: WalReplayOptions,
    table_store: Arc<TableStore>,
    wal_iter: Box<dyn WalIteratorTrait>,
    /// An error from the underlying WAL iterator, held back so that write batches
    /// already applied to the current table are returned first. `DbReader` treats a
    /// missing WAL file as the end of the WAL, so rows replayed before the error
    /// must not be dropped with it. The error is surfaced on the following
    /// [`Self::next`] call.
    pending_error: Option<SlateDBError>,
    /// The greatest WAL ID such that it and every WAL file before it in the replay
    /// range are fully applied to returned tables. Tables are tagged with this
    /// conservative watermark so that a table ending mid-file never claims a WAL
    /// file it only partially contains.
    last_completed_wal_id: u64,
    last_tick: i64,
    last_seq: u64,
    min_seq: u64,
}

impl WalReplayIterator {
    pub(crate) async fn range(
        wal_id_range: Range<u64>,
        db_state: &ManifestCore,
        options: WalReplayOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        // Everything before the replay range is already covered by the manifest,
        // so the conservative WAL watermark starts just below the range.
        let wal_iter = WalIterator::range(
            wal_id_range,
            options.sst_batch_size,
            options.sst_iter_options.clone(),
            Arc::clone(&table_store),
        )
            .await?;
        Self::for_wal_iterator(
            Box::new(wal_iter),
            db_state,
            options,
            table_store,
        )
    }

    pub(crate) fn for_wal_iterator(
        wal_iter: Box<dyn WalIteratorTrait>,
        db_state: &ManifestCore,
        options: WalReplayOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        // load the last seq number from manifest, and use it as the starting seq number to avoid
        // replaying the entries that are already in the L0 SST. while replaying the WALs, we'll
        // update the last seq number to the max seq number, and this final `last_seq` will be passed
        // to the db_state for the further writes.
        let min_seq = options.min_seq.unwrap_or(db_state.last_l0_seq);
        let last_seq = db_state.last_l0_seq;
        let last_tick = db_state.last_l0_clock_tick;

        Ok(WalReplayIterator {
            options,
            table_store,
            wal_iter,
            pending_error: None,
            last_completed_wal_id: db_state.replay_after_wal_id,
            last_tick,
            last_seq,
            min_seq,
        })
    }

    /// Get the next table replayed from the WAL. Replay accumulates write batches
    /// until the returned table reaches [`WalReplayOptions::max_memtable_bytes`].
    /// Tables are only split between write batches — all rows sharing a commit seq
    /// stay in one table, and batches are applied in ascending seq order — so a
    /// returned table may exceed the target when a single write batch is larger.
    ///
    /// The returned table's `last_wal_id` is a conservative watermark: the greatest
    /// WAL ID such that it and every WAL file before it are fully contained in the
    /// tables returned so far. A table that ends mid-file is tagged with the last
    /// fully replayed WAL ID, so replaying from `last_wal_id + 1` and dropping rows
    /// with seq <= the table's `last_seq` never misses or duplicates a commit.
    ///
    /// The final table may even be empty since writers use an empty WAL to fence
    /// zombie writers. The empty table must still be returned so that replay logic
    /// can account for the latest WAL ID.
    pub(crate) async fn next(&mut self) -> Result<Option<ReplayedMemtable>, SlateDBError> {
        if let Some(err) = self.pending_error.take() {
            return Err(err);
        }

        let table = WritableKVTable::new();
        let mut applied_any = false;

        loop {
            let writes = match self.wal_iter.next().await {
                Ok(Some(writes)) => writes,
                Ok(None) => break,
                // Hold the error back so the write batches already applied to this
                // table are returned first. `DbReader` treats a missing WAL file as
                // the end of the WAL, so rows replayed before the error must not be
                // dropped with it.
                Err(err) if applied_any => {
                    self.pending_error = Some(err.into());
                    break;
                }
                Err(err) => return Err(err.into()),
            };

            applied_any = true;
            // A batch from WAL file `wal_id` proves every earlier file was fully
            // applied (files are emitted in order and each yields at least one
            // batch), so the watermark advances to `wal_id - 1` without relying on
            // `last_in_file`. `last_in_file` merely sharpens the watermark to cover
            // the current file at its final batch; without it the watermark lags
            // one file behind, retaining at most one extra WAL file on recovery.
            self.last_completed_wal_id = self
                .last_completed_wal_id
                .max(writes.last_wal_file_id.saturating_sub(1));
            if writes.last_in_file {
                assert!(writes.last_wal_file_id > self.last_completed_wal_id);
                self.last_completed_wal_id = writes.last_wal_file_id;
            }

            for row_entry in writes.rows {
                // skip the entries that are already in the L0 SST.
                if row_entry.seq <= self.min_seq {
                    continue;
                }

                if let Some(ts) = row_entry.create_ts {
                    self.last_tick = self.last_tick.max(ts);
                }
                self.last_seq = self.last_seq.max(row_entry.seq);
                table.put(row_entry);
            }

            if !table.is_empty() {
                let meta = table.metadata();
                let estimated_bytes = self
                    .table_store
                    .estimate_encoded_size_compacted(meta.entry_num, meta.entries_size_in_bytes);
                if estimated_bytes >= self.options.max_memtable_bytes {
                    break;
                }
            }
        }

        if applied_any {
            Ok(Some(ReplayedMemtable {
                table,
                last_tick: self.last_tick,
                last_seq: self.last_seq,
                last_wal_id: self.last_completed_wal_id,
            }))
        } else {
            Ok(None)
        }
    }
}

/// A WAL file's id along with an iterator over its rows.
struct WalFileIter {
    wal_id: u64,
    iter: Box<dyn RowEntryIterator + 'static>,
}

/// Iterates over the writes in a range of WAL files, preloading up to
/// `sst_batch_size` WAL SSTs concurrently. Returns the rows of one WAL file per
/// [`WalRows`], and verifies that files carry strictly increasing seq
/// ranges — the ordering callers rely on to split and tag memtables safely.
///
/// Preloading only opens each WAL SST (footer, index, and any eagerly fetched
/// blocks); a file's rows are read out only when it is returned from
/// [`Self::next`], so at most one file's rows are materialized at a time.
pub(crate) struct WalIterator {
    /// The number of SSTs to preload while replaying
    sst_batch_size: usize,
    /// Options to pass through to underlying SST iterators
    sst_iter_options: SstIteratorOptions,
    /// Range of WAL IDs to iterate over
    wal_id_range: Range<u64>,
    table_store: Arc<TableStore>,
    next_files: VecDeque<JoinHandle<Result<WalFileIter, SlateDBError>>>,
    next_wal_id: u64,
    /// The greatest seq returned so far, used to verify that WAL files arrive
    /// with strictly increasing seq ranges.
    last_seq: Option<u64>,
    /// Set once iteration has ended, either because the range was exhausted or
    /// because an error was returned.
    finished: bool,
}

impl WalIterator {
    async fn range(
        wal_id_range: Range<u64>,
        sst_batch_size: usize,
        sst_iter_options: SstIteratorOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        if sst_batch_size < 1 {
            return Err(SlateDBError::InvalidSSTBatchSize(sst_batch_size));
        }

        let next_wal_id = wal_id_range.start;
        Ok(WalIterator {
            sst_batch_size,
            sst_iter_options,
            wal_id_range,
            table_store,
            next_files: VecDeque::new(),
            next_wal_id,
            last_seq: None,
            finished: false,
        })
    }

    fn maybe_load_next_file(&mut self) -> bool {
        if !self.wal_id_range.contains(&self.next_wal_id)
            || self.next_files.len() >= self.sst_batch_size
        {
            return false;
        }

        let next_wal_id = self.next_wal_id;
        self.next_wal_id += 1;

        async fn open_file_iter(
            wal_id: u64,
            sst_iter_options: SstIteratorOptions,
            table_store: Arc<TableStore>,
        ) -> Result<WalFileIter, SlateDBError> {
            let sst = match table_store.open_sst(&SsTableId::Wal(wal_id)).await {
                Ok(sst) => sst,
                Err(SlateDBError::EmptySSTable) => {
                    // Zero-byte WAL files are fence markers; replay them as empty WALs
                    // so the last replayed WAL ID still advances past the marker.
                    return Ok(WalFileIter {
                        wal_id,
                        iter: Box::new(EmptyIterator::new()),
                    });
                }
                Err(err) => return Err(err),
            };
            let iter = SstIterator::new_owned_initialized(
                ..,
                SsTableView::identity(sst),
                Arc::clone(&table_store),
                sst_iter_options,
            )
                .await?;
            // An unbounded, unfiltered scan over a WAL SST always yields an
            // iterator. `None` means the file cannot be read, and replay must
            // fail rather than silently end early and drop the remaining WALs.
            let Some(iter) = iter else {
                error!(
                    "could not construct row iterator over WAL SST. [wal_id={}]",
                    wal_id
                );
                return Err(SlateDBError::InvalidDBState);
            };
            Ok(WalFileIter {
                wal_id,
                iter: Box::new(iter) as Box<dyn RowEntryIterator + 'static>,
            })
        }

        let handle = task::spawn(open_file_iter(
            next_wal_id,
            self.sst_iter_options.clone(),
            Arc::clone(&self.table_store),
        ));
        self.next_files.push_back(handle);
        true
    }

    /// Await the next preloaded WAL file and return an iterator over its rows.
    /// Returns `None` when there are no more files to read.
    async fn take_next_file(&mut self) -> Result<Option<WalFileIter>, SlateDBError> {
        let Some(join_handle) = self.next_files.pop_front() else {
            return Ok(None);
        };
        match join_handle.await {
            Ok(result) => result.map(Some),
            Err(join_err) => {
                let task_name = format!("wal_replay[{:?}]", self.wal_id_range);
                if let Ok(panic_err) = join_err.try_into_panic() {
                    error!(
                        "wal_replay task panicked unexpectedly. [task_name={}, panic={}]",
                        task_name,
                        panic_string(&panic_err),
                    );
                    return Err(SlateDBError::BackgroundTaskPanic(task_name));
                }
                Err(SlateDBError::BackgroundTaskCancelled(task_name))
            }
        }
    }
}

#[async_trait]
impl WalIteratorTrait for WalIterator {
    /// Get the next set of writes from the WAL files in the range. Each returned
    /// [`WalRows`] holds the rows of one WAL file; a WAL file with no rows
    /// yields a batch with empty `rows`. Returns `None` once all WAL files in the
    /// range have been read. It is an error if a WAL file in the range is not
    /// present. Errors are returned only on calls that return no batch, so rows
    /// read from earlier WAL files are never dropped with a later file's error.
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        if self.finished {
            return Ok(None);
        }

        while self.maybe_load_next_file() {}
        let mut file_iter = match self.take_next_file().await {
            Ok(Some(file_iter)) => file_iter,
            Ok(None) => {
                self.finished = true;
                return Ok(None);
            }
            Err(err) => {
                self.finished = true;
                return Err(err.into());
            }
        };

        // Read out the file's rows. Only the file being returned is materialized;
        // preloaded files just hold opened iterators.
        let mut rows = Vec::new();
        loop {
            match file_iter.iter.next().await {
                Ok(Some(row)) => rows.push(row),
                Ok(None) => break,
                Err(err) => {
                    self.finished = true;
                    return Err(err.into());
                }
            }
        }

        // Verify that WAL files carry strictly increasing seq ranges. Replay
        // relies on this ordering to split and tag memtables safely: a commit seq
        // spanning two WAL files, or files with overlapping seq ranges, would
        // break recovery's (wal_id, seq) watermark filtering.
        if let Some(min_seq) = rows.iter().map(|row| row.seq).min() {
            if let Some(last_seq) = self.last_seq {
                if min_seq <= last_seq {
                    error!(
                        "WAL replay saw out-of-order seqs across WAL files. \
                         [wal_id={}, min_seq={}, last_seq={}]",
                        file_iter.wal_id, min_seq, last_seq,
                    );
                    self.finished = true;
                    return Err(WalError::SlateDBError(Arc::new(SlateDBError::InvalidDBState)));
                }
            }
            let max_seq = rows
                .iter()
                .map(|row| row.seq)
                .max()
                .expect("non-empty rows have a max seq");
            self.last_seq = Some(max_seq);
        }

        Ok(Some(WalRows {
            rows,
            last_wal_file_id: file_iter.wal_id,
            last_in_file: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{WalReplayIterator, WalReplayOptions};
    use crate::bytes_range::BytesRange;
    use crate::db_state::SsTableId;
    use crate::format::sst::SsTableFormat;
    use crate::iter::{IterationOrder, RowEntryIterator};
    use crate::manifest::ManifestCore;
    use crate::mem_table::WritableKVTable;
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::{rng, sample};
    use crate::tablestore::{TableStore, TableStoreKind};
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

    impl WalReplayIterator {
        async fn all_wal_ids(
            db_state: &ManifestCore,
            options: WalReplayOptions,
            table_store: Arc<TableStore>,
        ) -> Result<Self, SlateDBError> {
            let wal_id_start = db_state.replay_after_wal_id + 1;
            let wal_id_end = table_store
                .last_seen_wal_id(db_state.replay_after_wal_id)
                .await?;
            let wal_id_range = wal_id_start..(wal_id_end + 1);
            Self::range(wal_id_range, db_state, options, table_store).await
        }
    }

    #[tokio::test]
    async fn should_replay_empty_wal() {
        let table_store = test_table_store();
        write_empty_wal(1, Arc::clone(&table_store)).await.unwrap();
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
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
    async fn should_replay_zero_byte_wal_fence() {
        let table_store = test_table_store();
        table_store.write_wal_fence(1).await.unwrap();
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
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
    async fn should_replay_zero_byte_wal_fence_before_real_wal() {
        let table_store = test_table_store();
        table_store.write_wal_fence(1).await.unwrap();

        let row = RowEntry::new_value(b"key", b"value", 1);
        let mut builder = table_store.wal_table_builder();
        builder.add(row.clone()).await.unwrap();
        let encoded_sst = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(2), &encoded_sst, false)
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id, 2);
        assert_eq!(replayed_table.last_seq, 1);

        let mut iter = replayed_table.table.table().iter();
        test_utils::assert_iterator(&mut iter, vec![row]).await;
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

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
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
    async fn should_enforce_max_memtable_bytes() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let num_entries = 5000;
        let entries = sample::table(&mut rng, num_entries, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        let max_memtable_bytes = 1024;
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let full_replayed_table = WritableKVTable::new();
        let mut last_wal_id = 0;
        let mut replayed_entry_count = 0;

        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            last_wal_id = replayed_table.last_wal_id;
            let metadata = replayed_table.table.metadata();
            replayed_entry_count += metadata.entry_num;

            // The last table may be less than `max_memtable_bytes`.
            if replayed_entry_count < num_entries {
                let estimated_bytes = table_store.estimate_encoded_size_compacted(
                    metadata.entry_num,
                    metadata.entries_size_in_bytes,
                );
                assert!(estimated_bytes >= max_memtable_bytes);
            }

            let mut iter = replayed_table.table.table().iter();
            while let Some(next) = iter.next().await.unwrap() {
                full_replayed_table.put(next);
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
    async fn should_apply_max_memtable_bytes_at_wal_boundaries() {
        let table_store = test_table_store();
        let wal_entries = [
            vec![RowEntry::new_value(b"key_001", &[b'x'; 128], 1)],
            vec![RowEntry::new_value(b"key_002", &[b'x'; 128], 2)],
            vec![RowEntry::new_value(b"key_003", &[b'x'; 128], 3)],
        ];
        let single_row_size = wal_entries[0][0].estimated_size();
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, single_row_size) + 1;

        for (wal_id, entries) in wal_entries.into_iter().enumerate() {
            let mut builder = table_store.wal_table_builder();
            for entry in entries {
                builder.add(entry).await.unwrap();
            }
            let encoded_sst = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id as u64 + 1), &encoded_sst, false)
                .await
                .unwrap();
        }

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed_last_wal_ids = Vec::new();
        let mut replayed_table_sizes = Vec::new();
        let mut replayed_seqs = Vec::new();

        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            replayed_last_wal_ids.push(replayed_table.last_wal_id);
            let metadata = replayed_table.table.metadata();
            replayed_table_sizes.push(table_store.estimate_encoded_size_compacted(
                metadata.entry_num,
                metadata.entries_size_in_bytes,
            ));
            let mut iter = replayed_table.table.table().iter();
            while let Some(next) = iter.next().await.unwrap() {
                replayed_seqs.push(next.seq);
            }
        }

        assert_eq!(replayed_last_wal_ids, vec![2, 3]);
        assert!(
            replayed_table_sizes[0] > max_memtable_bytes,
            "first replayed table should exceed the target rather than split a WAL SST"
        );
        assert_eq!(replayed_seqs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn should_not_split_one_commit_seq_across_replayed_memtables() {
        let table_store = test_table_store();
        let commit_seq = 42;

        // Simulate one committed write batch. Every row gets the same commit
        // sequence, which means replay must not split these rows into separate
        // memtable layers.
        let entries = (0..8)
            .map(|i| {
                RowEntry::new_value(format!("key_{i:03}").as_bytes(), &[b'x'; 128], commit_seq)
            })
            .collect::<Vec<_>>();

        // Size replayed memtables so one real row fits, but the second row
        // overflows into the next replayed memtable.
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, entries[0].estimated_size());

        // Use the real WAL SST builder so the fixture matches WAL flushes.
        let mut builder = table_store.wal_table_builder();
        for entry in entries {
            builder.add(entry).await.unwrap();
        }
        let encoded_sst = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded_sst, false)
            .await
            .unwrap();

        // Replay the single WAL SST into in-memory tables. If the replay code
        // can split a single commit sequence, it will do so here.
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed_seq_ranges = Vec::new();
        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            let metadata = replayed_table.table.metadata();
            replayed_seq_ranges.push((metadata.first_seq, metadata.last_seq));
        }

        // This guards against producing multiple replayed memtables with the same
        // sequence range, which can make later replay logic treat part of the write
        // batch as already committed.
        assert_eq!(
            replayed_seq_ranges,
            vec![(commit_seq, commit_seq)],
            "WAL replay split one commit seq across replayed memtables: {replayed_seq_ranges:?}"
        );
    }

    #[tokio::test]
    async fn should_replay_memtables_in_sequence_order() {
        let table_store = test_table_store();

        // Write one WAL with entries whose sequence numbers do not match key
        // order. Replay must not expose a later memtable whose sequence range
        // starts before the previous memtable's sequence range ends.
        let entries = vec![
            RowEntry::new_value(b"key_000", &[b'x'; 128], 100),
            RowEntry::new_value(b"key_001", &[b'x'; 128], 10),
            RowEntry::new_value(b"key_002", &[b'x'; 128], 110),
        ];

        // Size replayed memtables so one real row fits, but the second row
        // overflows into the next replayed memtable.
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, entries[0].estimated_size());

        // Use the real WAL SST builder so replay sees the same entry order as a
        // flushed WAL.
        let mut builder = table_store.wal_table_builder();
        for entry in entries {
            builder.add(entry).await.unwrap();
        }
        let encoded_sst = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded_sst, false)
            .await
            .unwrap();

        // Replay the single WAL SST into in-memory tables.
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed_seq_ranges = Vec::new();
        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            let metadata = replayed_table.table.metadata();
            replayed_seq_ranges.push((metadata.first_seq, metadata.last_seq));
        }

        // This guards against returning the seq=10 row in a later replayed
        // memtable after already returning seq=100.
        for adjacent in replayed_seq_ranges.windows(2) {
            let previous_last_seq = adjacent[0].1;
            let later_first_seq = adjacent[1].0;
            assert!(
                later_first_seq >= previous_last_seq,
                "WAL replay returned out-of-order memtable sequence ranges: {replayed_seq_ranges:?}"
            );
        }
    }

    #[tokio::test]
    async fn should_tag_tables_with_last_fully_replayed_wal_id() {
        let table_store = test_table_store();

        // Two WAL files with two single-row write batches each. Size replayed
        // tables so each holds one write batch, forcing splits mid-file.
        let wal_entries = [
            vec![
                RowEntry::new_value(b"key_001", &[b'x'; 128], 1),
                RowEntry::new_value(b"key_002", &[b'x'; 128], 2),
            ],
            vec![
                RowEntry::new_value(b"key_003", &[b'x'; 128], 3),
                RowEntry::new_value(b"key_004", &[b'x'; 128], 4),
            ],
        ];
        let single_row_size = wal_entries[0][0].estimated_size();
        let max_memtable_bytes = table_store.estimate_encoded_size_compacted(1, single_row_size);

        for (i, entries) in wal_entries.into_iter().enumerate() {
            let mut builder = table_store.wal_table_builder();
            for entry in entries {
                builder.add(entry).await.unwrap();
            }
            let encoded_sst = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(i as u64 + 1), &encoded_sst, false)
                .await
                .unwrap();
        }

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed = Vec::new();
        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            let metadata = replayed_table.table.metadata();
            replayed.push((
                replayed_table.last_wal_id,
                metadata.first_seq,
                metadata.last_seq,
            ));
        }

        // Each table must be tagged with the last fully replayed WAL ID:
        // replaying from `last_wal_id + 1` and dropping rows with seq <= the
        // table's max applied seq reproduces exactly the missing rows. While
        // `WalIterator` returns one batch per WAL file, tables split at file
        // boundaries, so each table's tag is the id of its final file.
        assert_eq!(replayed, vec![(1, 1, 2), (2, 3, 4)]);
    }

    #[tokio::test]
    async fn should_keep_rows_with_same_seq_in_one_table_when_file_is_not_seq_ordered() {
        let table_store = test_table_store();

        // A WAL file whose physical order is key order rather than seq order, with
        // one commit (seq 2) split around another (seq 1). Older SlateDB versions
        // wrote WAL SSTs in key order, so replay must regroup rows by seq.
        let entries = vec![
            RowEntry::new_value(b"key_001", &[b'x'; 128], 2),
            RowEntry::new_value(b"key_002", &[b'x'; 128], 1),
            RowEntry::new_value(b"key_003", &[b'x'; 128], 2),
        ];
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, entries[0].estimated_size());

        let mut builder = table_store.wal_table_builder();
        for entry in entries {
            builder.add(entry).await.unwrap();
        }
        let encoded_sst = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded_sst, false)
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed = Vec::new();
        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            let metadata = replayed_table.table.metadata();
            replayed.push((metadata.entry_num, metadata.first_seq, metadata.last_seq));
        }

        // Both seq=2 rows must land in one table even though a seq=1 row sits
        // between them in the file. While `WalIterator` returns one batch per
        // WAL file this holds trivially; this guards the contract if sub-file
        // batching is reintroduced.
        assert_eq!(replayed, vec![(3, 1, 2)]);
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

        let mut db_state = ManifestCore::new();
        db_state.replay_after_wal_id = replay_after_wal_id;
        db_state.next_wal_sst_id = replay_after_wal_id + 1;

        let mut replay_iter = WalReplayIterator::all_wal_ids(
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
        let mut db_state = ManifestCore::new();
        db_state.last_l0_seq = min_seq;
        db_state.last_l0_clock_tick = 0;

        let mut replay_iter = WalReplayIterator::all_wal_ids(
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
        while let Some(entry) = imm_table_iter.next().await.unwrap() {
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
            path,
            None,
            TableStoreKind::Main,
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
