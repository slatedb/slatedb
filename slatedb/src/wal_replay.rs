use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::iter::{EmptyIterator, RowEntryIterator};
use crate::manifest::ManifestCore;
use crate::manifest::SsTableView;
use crate::mem_table::WritableKVTable;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::utils::panic_string;
use crate::wal::{WalError, WalIterator as WalIteratorTrait, WalRows};
use crate::RowEntry;
use async_trait::async_trait;
use log::error;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use tokio::task;
use tokio::task::JoinHandle;

pub(crate) struct WalIteratorOptions {
    /// The number of SSTs to preload while replaying
    pub(crate) sst_batch_size: usize,

    /// Options to pass through to underlying SST iterators
    pub(crate) sst_iter_options: SstIteratorOptions,
}

impl Default for WalIteratorOptions {
    fn default() -> Self {
        Self {
            sst_batch_size: 4,
            sst_iter_options: SstIteratorOptions::default(),
        }
    }
}

pub(crate) struct WalReplayOptions {
    /// The target maximum number of bytes in each returned table. WAL replay only
    /// splits between write batches (all rows of one commit stay in one table), so
    /// a returned table may exceed this if a single write batch is larger.
    pub(crate) max_memtable_bytes: usize,

    /// The minimum seq number to replay. If unset, will replay all
    /// entries after `last_l0_seq` in the manifest.
    pub(crate) min_seq: Option<u64>,
}

impl Default for WalReplayOptions {
    fn default() -> Self {
        Self {
            max_memtable_bytes: 64 * 1024 * 1024,
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
    terminal_result: Option<Result<(), SlateDBError>>,
    /// The greatest WAL ID such that it and every WAL file before it in the replay
    /// range are fully applied to returned tables. Tables are tagged with this
    /// conservative watermark so that a table ending mid-file never claims a WAL
    /// file it only partially contains.
    last_consumed_wal_file_id: u64,
    last_tick: i64,
    last_seq: u64,
    min_seq: u64,
}

impl WalReplayIterator {
    #[cfg(test)]
    pub(crate) fn range(
        wal_id_range: Range<u64>,
        db_state: &ManifestCore,
        iterator_options: WalIteratorOptions,
        replay_options: WalReplayOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        let wal_iter =
            WalIterator::range(wal_id_range, iterator_options, Arc::clone(&table_store))?;
        Self::for_wal_iterator(Box::new(wal_iter), db_state, replay_options, table_store)
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
            terminal_result: None,
            last_consumed_wal_file_id: db_state.replay_after_wal_id,
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
    pub(crate) async fn next(&mut self) -> Result<Option<ReplayedMemtable>, SlateDBError> {
        if let Some(terminal_result) = self.terminal_result.clone() {
            return terminal_result.map(|_v| None);
        }

        let table = WritableKVTable::new();
        let mut applied_any = false;

        loop {
            let writes = match self.wal_iter.next().await {
                Ok(Some(writes)) => writes,
                Ok(None) => {
                    // we've reached the end of iteration, mark the iterator as done
                    self.terminal_result = Some(Ok(()));
                    break;
                }
                // Hold the error back so the write batches already applied to this
                // table are returned first. `DbReader` treats a missing WAL file as
                // the end of the WAL, so rows replayed before the error must not be
                // dropped with it.
                Err(err) => {
                    self.terminal_result = Some(Err(err.into()));
                    break;
                }
            };

            applied_any = true;
            assert!(
                writes.last_consumed_wal_file_id >= self.last_consumed_wal_file_id,
                "WAL iterator moved its consumed file watermark backwards"
            );
            self.last_consumed_wal_file_id = writes.last_consumed_wal_file_id;

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
            // we use the applied_any check here rather than checking non-empty table size to
            // ensure that if we replayed values with a lower seq num we still carry the
            // wal id in an empty replayed memtable
            Ok(Some(ReplayedMemtable {
                table,
                last_tick: self.last_tick,
                last_seq: self.last_seq,
                last_wal_id: self.last_consumed_wal_file_id,
            }))
        } else {
            self.terminal_result
                .clone()
                .expect("applied_any false but no terminal result")
                .map(|_v| None)
        }
    }
}

struct WalRowsCollector {
    wal_id: u64,
    iter: Box<dyn RowEntryIterator + 'static>,
    rows: Vec<RowEntry>,
    drained: bool,
}

impl WalRowsCollector {
    fn new(wal_id: u64, iter: Box<dyn RowEntryIterator + 'static>) -> Self {
        Self {
            wal_id,
            iter,
            rows: vec![],
            drained: false,
        }
    }

    async fn collect(&mut self) -> Result<(), WalError> {
        loop {
            match self.iter.next().await {
                Ok(Some(row)) => self.rows.push(row),
                Ok(None) => {
                    self.drained = true;
                    break Ok(());
                }
                Err(err) if err.has_object_store_not_found() => {
                    break Err(WalError::WalTruncated(self.wal_id));
                }
                Err(err) => {
                    break Err(err.into());
                }
            }
        }
    }
}

impl From<WalRowsCollector> for WalRows {
    fn from(reader: WalRowsCollector) -> Self {
        assert!(reader.drained);
        WalRows {
            last_consumed_wal_file_id: reader.wal_id,
            rows: reader.rows,
        }
    }
}

struct CurrentWalFile {
    initialized: bool,
    collector: Option<WalRowsCollector>,
}

impl CurrentWalFile {
    fn initial() -> Self {
        Self {
            initialized: false,
            collector: None,
        }
    }

    fn initialized(&self) -> bool {
        self.initialized
    }

    async fn collect(&mut self) -> Result<Option<WalRows>, WalError> {
        assert!(self.initialized);
        let Some(collector) = &mut self.collector else {
            return Ok(None);
        };
        collector.collect().await?;
        let collector = self.collector.take().expect("unreachable");
        self.initialized = false;
        Ok(Some(collector.into()))
    }

    fn advance(&mut self, collector: WalRowsCollector) {
        assert!(!self.initialized);
        self.initialized = true;
        self.collector = Some(collector);
    }

    fn finish(&mut self) {
        self.initialized = true;
        self.collector = None;
    }
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
    options: WalIteratorOptions,
    /// Range of WAL IDs to iterate over
    wal_id_range: Range<u64>,
    table_store: Arc<TableStore>,
    next_files: VecDeque<JoinHandle<Result<WalRowsCollector, WalError>>>,
    next_wal_id: u64,
    /// The greatest seq returned so far, used to verify that WAL files arrive
    /// with strictly increasing seq ranges.
    last_seq: Option<u64>,
    /// Set once iteration has ended, either because the range was exhausted or
    /// because an error was returned.
    terminal_result: Option<Result<Option<WalRows>, WalError>>,
    current_file: CurrentWalFile,
}

impl WalIterator {
    pub(crate) fn range(
        wal_id_range: Range<u64>,
        options: WalIteratorOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        if options.sst_batch_size < 1 {
            return Err(SlateDBError::InvalidSSTBatchSize(options.sst_batch_size));
        }

        let next_wal_id = wal_id_range.start;
        Ok(WalIterator {
            options,
            wal_id_range,
            table_store,
            next_files: VecDeque::new(),
            next_wal_id,
            last_seq: None,
            terminal_result: None,
            current_file: CurrentWalFile::initial(),
        })
    }

    fn maybe_spawn_open(&mut self) -> bool {
        if !self.wal_id_range.contains(&self.next_wal_id)
            || self.next_files.len() >= self.options.sst_batch_size
        {
            return false;
        }

        let next_wal_id = self.next_wal_id;
        self.next_wal_id += 1;

        async fn try_open_file_iter(
            wal_id: u64,
            sst_iter_options: SstIteratorOptions,
            table_store: Arc<TableStore>,
        ) -> Result<WalRowsCollector, SlateDBError> {
            let sst = match table_store.open_sst(&SsTableId::Wal(wal_id)).await {
                Ok(sst) => sst,
                Err(SlateDBError::EmptySSTable) => {
                    // Zero-byte WAL files are fence markers; replay them as empty WALs
                    // so the last replayed WAL ID still advances past the marker.
                    return Ok(WalRowsCollector::new(
                        wal_id,
                        Box::new(EmptyIterator::new()),
                    ));
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
            Ok(WalRowsCollector::new(wal_id, Box::new(iter)))
        }

        async fn open_file_iter(
            wal_id: u64,
            sst_iter_options: SstIteratorOptions,
            table_store: Arc<TableStore>,
        ) -> Result<WalRowsCollector, WalError> {
            match try_open_file_iter(wal_id, sst_iter_options, table_store).await {
                Ok(iter) => Ok(iter),
                Err(err) if err.has_object_store_not_found() => Err(WalError::WalTruncated(wal_id)),
                Err(err) => Err(err.into()),
            }
        }

        let handle = task::spawn(open_file_iter(
            next_wal_id,
            self.options.sst_iter_options.clone(),
            Arc::clone(&self.table_store),
        ));
        self.next_files.push_back(handle);
        true
    }

    /// Await the next preloaded WAL file and return an iterator over its rows.
    /// Returns `None` when there are no more files to read.
    async fn load_next_file(&mut self) -> Result<(), WalError> {
        if self.current_file.initialized() {
            return Ok(());
        }
        // await a mutable ref to the task so that next remains cancel-safe
        // see https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html#cancel-safety
        let Some(join_handle) = self.next_files.front_mut() else {
            self.current_file.finish();
            return Ok(());
        };
        let result = join_handle.await;
        self.next_files.pop_front();
        match result {
            Ok(result) => {
                self.current_file.advance(result?);
                Ok(())
            }
            Err(join_err) => {
                let task_name = format!("wal_replay[{:?}]", self.wal_id_range);
                let msg = if let Ok(panic_err) = join_err.try_into_panic() {
                    format!(
                        "wal_replay task panicked unexpectedly. [task_name={}, panic={}]",
                        task_name,
                        panic_string(&panic_err),
                    )
                } else {
                    format!("wal_replay task cancelled. [task_name={}]", task_name)
                };
                error!("{}", msg);
                let error = Arc::from(Box::<dyn std::error::Error + Send + Sync>::from(msg));
                Err(WalError::InternalError(error))
            }
        }
    }

    fn terminate(
        &mut self,
        result: Result<Option<WalRows>, WalError>,
    ) -> Result<Option<WalRows>, WalError> {
        self.terminal_result = Some(result.clone());
        for task in self.next_files.drain(..) {
            task.abort();
        }
        result
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
        if let Some(result) = self.terminal_result.clone() {
            return result;
        }

        while self.maybe_spawn_open() {}
        if let Err(err) = self.load_next_file().await {
            return self.terminate(Err(err));
        }
        match self.current_file.collect().await {
            Err(err) => self.terminate(Err(err)),
            Ok(None) => self.terminate(Ok(None)),
            Ok(Some(rows)) => {
                // Verify that WAL files carry strictly increasing seq ranges. Replay
                // relies on this ordering to split and tag memtables safely: a commit seq
                // spanning two WAL files, or files with overlapping seq ranges, would
                // break recovery's (wal_id, seq) watermark filtering.
                if let Some(min_seq) = rows.rows.iter().map(|row| row.seq).min() {
                    if let Some(last_seq) = self.last_seq {
                        if min_seq <= last_seq {
                            let msg = format!(
                                "WAL replay saw out-of-order seqs across WAL files. \
                                [wal_id={}, min_seq={}, last_seq={}]",
                                rows.last_consumed_wal_file_id, min_seq, last_seq,
                            );
                            error!("{}", &msg);
                            let error =
                                Arc::from(Box::<dyn std::error::Error + Send + Sync>::from(msg));
                            return self.terminate(Err(WalError::InternalError(error)));
                        }
                    }
                    let max_seq = rows
                        .rows
                        .iter()
                        .map(|row| row.seq)
                        .max()
                        .expect("non-empty rows have a max seq");
                    self.last_seq = Some(max_seq);
                }
                Ok(Some(rows))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WalIterator, WalIteratorOptions, WalReplayIterator, WalReplayOptions};
    use crate::block_cache_policy::BlockCachePolicy;
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
    use crate::wal::{WalError, WalIterator as WalIteratorTrait, WalRows};
    use crate::{error::SlateDBError, test_utils};
    use async_trait::async_trait;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use proptest::test_runner::TestRng;
    use rand::Rng;
    use std::cmp::min;
    use std::collections::btree_map::Iter;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::sync::Arc;

    struct ScriptedWalIterator {
        results: VecDeque<Result<Option<WalRows>, WalError>>,
    }

    #[async_trait]
    impl WalIteratorTrait for ScriptedWalIterator {
        async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
            self.results.pop_front().unwrap_or(Ok(None))
        }
    }

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
            Self::range(
                wal_id_range,
                db_state,
                WalIteratorOptions::default(),
                options,
                table_store,
            )
        }
    }

    #[tokio::test]
    async fn should_return_replayed_rows_before_repeating_terminal_error() {
        let table_store = test_table_store();
        let first_row = RowEntry::new_value(b"key_001", b"value_001", 1);
        let later_row = RowEntry::new_value(b"key_002", b"value_002", 2);
        let wal_iter = ScriptedWalIterator {
            results: VecDeque::from([
                Ok(Some(WalRows {
                    rows: vec![first_row],
                    last_consumed_wal_file_id: 1,
                })),
                Err(WalError::WalTruncated(2)),
                // A terminal error must prevent the replay iterator from resuming
                // the underlying iterator on later calls.
                Ok(Some(WalRows {
                    rows: vec![later_row],
                    last_consumed_wal_file_id: 3,
                })),
            ]),
        };
        let mut replay_iter = WalReplayIterator::for_wal_iterator(
            Box::new(wal_iter),
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes: usize::MAX,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .unwrap();

        let replayed = replay_iter.next().await.unwrap().unwrap();
        assert_eq!(replayed.last_wal_id, 1);
        assert_eq!(replayed.last_seq, 1);
        assert_eq!(replayed.table.metadata().entry_num, 1);

        assert!(matches!(
            replay_iter.next().await,
            Err(SlateDBError::WalTruncated(2))
        ));
        assert!(matches!(
            replay_iter.next().await,
            Err(SlateDBError::WalTruncated(2))
        ));
    }

    #[tokio::test]
    async fn should_repeat_terminal_none_for_wal_replay_iterator() {
        let table_store = test_table_store();
        let wal_iter = ScriptedWalIterator {
            results: VecDeque::from([
                Ok(None),
                // Normal termination must prevent the replay iterator from
                // resuming the underlying iterator on later calls.
                Ok(Some(WalRows {
                    rows: vec![RowEntry::new_value(b"key", b"value", 1)],
                    last_consumed_wal_file_id: 1,
                })),
            ]),
        };
        let mut replay_iter = WalReplayIterator::for_wal_iterator(
            Box::new(wal_iter),
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .unwrap();

        assert!(replay_iter.next().await.unwrap().is_none());
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_repeat_terminal_error_for_wal_iterator() {
        let table_store = test_table_store();
        let mut wal_iter = WalIterator::range(
            1..2,
            WalIteratorOptions::default(),
            Arc::clone(&table_store),
        )
        .unwrap();

        assert!(matches!(
            wal_iter.next().await,
            Err(WalError::WalTruncated(1))
        ));
        assert!(matches!(
            wal_iter.next().await,
            Err(WalError::WalTruncated(1))
        ));
    }

    #[tokio::test]
    async fn should_repeat_terminal_none_for_wal_iterator() {
        let table_store = test_table_store();
        let mut wal_iter = WalIterator::range(
            1..1,
            WalIteratorOptions::default(),
            Arc::clone(&table_store),
        )
        .unwrap();

        assert!(wal_iter.next().await.unwrap().is_none());
        assert!(wal_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_use_last_consumed_wal_file_id_as_replay_watermark() {
        let table_store = test_table_store();
        let first_row = RowEntry::new_value(b"key_001", &[b'x'; 128], 1);
        let second_row = RowEntry::new_value(b"key_002", &[b'x'; 128], 2);
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, first_row.estimated_size());
        let wal_iter = ScriptedWalIterator {
            results: VecDeque::from([
                Ok(Some(WalRows {
                    rows: vec![first_row],
                    // The first batch ends partway through WAL file 1.
                    last_consumed_wal_file_id: 0,
                })),
                Ok(Some(WalRows {
                    rows: vec![second_row],
                    // The second batch consumes the rest of WAL file 1.
                    last_consumed_wal_file_id: 1,
                })),
            ]),
        };
        let mut replay_iter = WalReplayIterator::for_wal_iterator(
            Box::new(wal_iter),
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .unwrap();

        let first = replay_iter.next().await.unwrap().unwrap();
        assert_eq!(first.last_wal_id, 0);
        assert_eq!(first.last_seq, 1);

        let second = replay_iter.next().await.unwrap().unwrap();
        assert_eq!(second.last_wal_id, 1);
        assert_eq!(second.last_seq, 2);
        assert!(replay_iter.next().await.unwrap().is_none());
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
            .write_sst(&SsTableId::Wal(2), &encoded_sst)
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
                .write_sst(&SsTableId::Wal(wal_id as u64 + 1), &encoded_sst)
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
            .write_sst(&SsTableId::Wal(1), &encoded_sst)
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
            .write_sst(&SsTableId::Wal(1), &encoded_sst)
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
    async fn should_return_atomic_wal_rows_in_increasing_seq_order() {
        let table_store = test_table_store();
        // Each file contains out-of-order rows and a sequence that appears twice.
        // The iterator must keep both rows for a sequence in one batch, while the
        // sequence range of the second batch must follow the first.
        let wal_entries = [
            vec![
                RowEntry::new_value(b"key_001", &[b'x'; 128], 2),
                RowEntry::new_value(b"key_002", &[b'x'; 128], 1),
                RowEntry::new_value(b"key_003", &[b'x'; 128], 2),
            ],
            vec![
                RowEntry::new_value(b"key_004", &[b'x'; 128], 4),
                RowEntry::new_value(b"key_005", &[b'x'; 128], 3),
                RowEntry::new_value(b"key_006", &[b'x'; 128], 4),
            ],
        ];
        let mut expected_rows = BTreeMap::new();
        let mut expected_rows_by_seq = BTreeMap::<u64, BTreeSet<Bytes>>::new();
        let wal_file_count = wal_entries.len() as u64;
        for (file_index, entries) in wal_entries.iter().enumerate() {
            let wal_id = file_index as u64 + 1;
            for row in entries {
                expected_rows.insert(row.key.clone(), (row.clone(), wal_id));
                expected_rows_by_seq
                    .entry(row.seq)
                    .or_default()
                    .insert(row.key.clone());
            }
        }
        for (index, entries) in wal_entries.into_iter().enumerate() {
            let mut builder = table_store.wal_table_builder();
            for entry in entries {
                builder.add(entry).await.unwrap();
            }
            let encoded_sst = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(index as u64 + 1), &encoded_sst)
                .await
                .unwrap();
        }
        let mut wal_iter = WalIterator::range(
            1..(wal_file_count + 1),
            WalIteratorOptions::default(),
            Arc::clone(&table_store),
        )
        .unwrap();

        let mut returned_rows = BTreeMap::new();
        let mut previous_max_seq = None;
        let mut last_consumed_wal_file_id = 0;
        while let Some(batch) = wal_iter.next().await.unwrap() {
            let batch_min_seq = batch.rows.iter().map(|r| r.seq).min().unwrap();
            let batch_max_seq = batch.rows.iter().map(|r| r.seq).max().unwrap();
            if let Some(previous_max_seq) = previous_max_seq {
                assert!(
                    batch_min_seq > previous_max_seq,
                    "consecutive WAL batches have overlapping sequence ranges"
                );
            }
            previous_max_seq = Some(batch_max_seq);

            let mut batch_rows_by_seq = BTreeMap::<u64, BTreeSet<Bytes>>::new();
            for row in &batch.rows {
                assert!(
                    returned_rows.insert(row.key.clone(), row.clone()).is_none(),
                    "row was returned more than once: {:?}",
                    row.key
                );
                batch_rows_by_seq
                    .entry(row.seq)
                    .or_default()
                    .insert(row.key.clone());
            }
            for (seq, batch_rows) in batch_rows_by_seq {
                assert_eq!(
                    expected_rows_by_seq.get(&seq),
                    Some(&batch_rows),
                    "rows for seq {seq} were split across WAL batches"
                );
            }

            assert!(
                batch.last_consumed_wal_file_id >= last_consumed_wal_file_id,
                "consumed WAL file watermark moved backwards"
            );
            assert!(batch.last_consumed_wal_file_id <= wal_file_count);
            for wal_id in 1..=batch.last_consumed_wal_file_id {
                let file_fully_returned =
                    expected_rows.iter().all(|(key, (_, expected_wal_id))| {
                        *expected_wal_id != wal_id || returned_rows.contains_key(key)
                    });
                assert!(
                    file_fully_returned,
                    "WAL file {wal_id} was marked consumed before all its rows were returned"
                );
            }
            last_consumed_wal_file_id = batch.last_consumed_wal_file_id;
        }

        let expected_returned_rows = expected_rows
            .into_iter()
            .map(|(key, (row, _wal_id))| (key, row))
            .collect();
        assert_eq!(returned_rows, expected_returned_rows);
        assert_eq!(last_consumed_wal_file_id, wal_file_count);
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
            BlockCachePolicy::default(),
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
        let end_seq = next_seq + (max_entries as u64);
        while next_seq < end_seq {
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
