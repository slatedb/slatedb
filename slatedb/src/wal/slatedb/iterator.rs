use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use log::error;
use tokio::task;
use tokio::task::JoinHandle;

use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::iter::{EmptyIterator, RowEntryIterator};
use crate::manifest::SsTableView;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::utils::panic_string;
use crate::wal::{WalError, WalIterator as WalIteratorTrait, WalRows};
use crate::RowEntry;

pub(crate) struct SlateDbWalIteratorOptions {
    /// The number of SSTs to preload while replaying
    pub(crate) sst_batch_size: usize,

    /// Options to pass through to underlying SST iterators
    pub(crate) sst_iter_options: SstIteratorOptions,
}

impl Default for SlateDbWalIteratorOptions {
    fn default() -> Self {
        Self {
            sst_batch_size: 4,
            sst_iter_options: SstIteratorOptions::default(),
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
pub(crate) struct SlateDbWalIterator {
    options: SlateDbWalIteratorOptions,
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

impl SlateDbWalIterator {
    pub(crate) fn range(
        wal_id_range: Range<u64>,
        options: SlateDbWalIteratorOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        if options.sst_batch_size < 1 {
            return Err(SlateDBError::InvalidSSTBatchSize(options.sst_batch_size));
        }

        let next_wal_id = wal_id_range.start;
        Ok(Self {
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
impl WalIteratorTrait for SlateDbWalIterator {
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
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    use super::{SlateDbWalIterator, SlateDbWalIteratorOptions};
    use crate::block_cache_policy::BlockCachePolicy;
    use crate::db_state::SsTableId;
    use crate::format::sst::SsTableFormat;
    use crate::object_stores::ObjectStores;
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::types::RowEntry;
    use crate::wal::{WalError, WalIterator as _};

    #[tokio::test]
    async fn should_repeat_terminal_error_for_wal_iterator() {
        let table_store = test_table_store();
        let mut wal_iter = SlateDbWalIterator::range(
            1..2,
            SlateDbWalIteratorOptions::default(),
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
        let mut wal_iter = SlateDbWalIterator::range(
            1..1,
            SlateDbWalIteratorOptions::default(),
            Arc::clone(&table_store),
        )
        .unwrap();

        assert!(wal_iter.next().await.unwrap().is_none());
        assert!(wal_iter.next().await.unwrap().is_none());
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
        let mut wal_iter = SlateDbWalIterator::range(
            1..(wal_file_count + 1),
            SlateDbWalIteratorOptions::default(),
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
}
