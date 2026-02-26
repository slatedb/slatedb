use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use log::{error, trace};
use parking_lot::RwLock;
use tokio::{
    runtime::Handle,
    select,
    sync::{mpsc, oneshot},
};
use tracing::instrument;

use crate::clock::MonotonicClock;
use crate::db_state::{DbState, SsTableId};
use crate::db_stats::DbStats;
use crate::dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor};
use crate::error::SlateDBError;
use crate::oracle::{DbOracle, Oracle};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::utils::{format_bytes_si, SendSafely, WatchableOnceCell, WatchableOnceCellReader};
use crate::wal_id::WalIdStore;

pub(crate) const WAL_BUFFER_TASK_NAME: &str = "wal_writer";

/// [`WalBufferManager`] buffers write operations in memory before flushing them to persistent storage.
/// The flush operation only targets Remote storage right now, later we can add an option to flush to local
/// storage.
///
/// It maintains a `current_wal` buffer for active writes and a queue of immutable WALs pending flush.
///
/// By default, it offers a best-effort durability guarantee based on:
///
/// - `max_wal_size`: Flushes when `max_wal_size` bytes is exceeded
/// - `max_flush_interval`: Flushes after `max_flush_interval` elapses, if set
///
/// For strict durability requirements on synchronous writes, use [`WalBufferManager::flush()`] to explicitly
/// trigger a flush operation and await the result. This will flush ALL the in memory WALs (including the
/// current WAL) to remote storage.
///
/// The manager is thread-safe and can be safely shared across multiple threads.
///
/// Please note:
///
/// - the size limit (`max_wal_size`) is a soft threshold. WAL entries within a single write batch are
///   guaranteed to be written atomically to the same WAL file.
/// - Fatal errors during flush operations are stored internally and propagated to all subsequent
///   operations. The manager becomes unusable after encountering a fatal error.
pub(crate) struct WalBufferManager {
    inner: Arc<parking_lot::RwLock<WalBufferManagerInner>>,
    wal_id_incrementor: Arc<dyn WalIdStore + Send + Sync>,
    db_state: Arc<RwLock<DbState>>,
    db_stats: DbStats,
    mono_clock: Arc<MonotonicClock>,
    table_store: Arc<TableStore>,
    max_wal_bytes_size: usize,
    max_flush_interval: Option<Duration>,
}

struct WalBufferManagerInner {
    current_wal: WalBuffer,
    /// When the current WAL is ready to be flushed, it'll be moved to the `immutable_wals`.
    /// The flusher will try flush all the immutable wals to remote storage.
    immutable_wals: VecDeque<(u64, Arc<WalBuffer>)>,
    /// The channel to send the flush work to the background worker.
    flush_tx: Option<mpsc::UnboundedSender<WalFlushWork>>,
    /// task executor for the background worker.
    task_executor: Option<Arc<MessageHandlerExecutor>>,
    /// Whenever a WAL is applied to Memtable and successfully flushed to remote storage,
    /// the immutable wal can be recycled in memory.
    last_applied_seq: Option<u64>,
    /// The flusher will update the recent_flushed_wal_id and last_flushed_seq when the flush is done.
    recent_flushed_wal_id: u64,
    /// The oracle to track the last flushed sequence number.
    oracle: Arc<DbOracle>,
}

/// Stores entries to the write-ahead log (WAL) in memory.
///
/// In contrast to the [`KVTable`], the `WalBuffer` does not sort the entries according to the key,
/// but keeps the order in which the entries were added.
/// The assumption is that the entries are added in order of the sequence number to the WAL.
/// Ordering by sequence number is sufficient for replaying entries from the WAL in case of a
/// failure.
/// Since the `WalBuffer` does not maintain the order by key it saves some CPU cycles compared to
/// a [`KVTable`].
struct WalBuffer {
    /// queue for the entries
    entries: VecDeque<RowEntry>,
    /// watcher to await durability
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    /// this corresponds to the timestamp of the most recent addition to this WAL buffer
    last_tick: i64,
    /// the sequence number of the most recent addition to this WAL buffer
    last_seq: u64,
    /// size of the entries that has been added to the WAL buffer in bytes
    entries_size: usize,
}

/// An iterator over entries in a WalBuffer.
struct WalBufferIterator {
    /// The entries to iterate over (owned).
    entries: std::vec::IntoIter<RowEntry>,
}

impl WalBufferManager {
    pub(crate) fn new(
        wal_id_incrementor: Arc<dyn WalIdStore + Send + Sync>,
        db_state: Arc<RwLock<DbState>>,
        db_stats: DbStats,
        recent_flushed_wal_id: u64,
        oracle: Arc<DbOracle>,
        table_store: Arc<TableStore>,
        mono_clock: Arc<MonotonicClock>,
        max_wal_bytes_size: usize,
        max_flush_interval: Option<Duration>,
    ) -> Self {
        let current_wal = WalBuffer::new();
        let immutable_wals = VecDeque::new();
        let inner = WalBufferManagerInner {
            current_wal,
            immutable_wals,
            last_applied_seq: None,
            recent_flushed_wal_id,
            flush_tx: None,
            task_executor: None,
            oracle,
        };
        Self {
            inner: Arc::new(parking_lot::RwLock::new(inner)),
            wal_id_incrementor,
            db_state,
            db_stats,
            table_store,
            mono_clock,
            max_wal_bytes_size,
            max_flush_interval,
        }
    }

    pub(crate) async fn init(
        self: &Arc<Self>,
        task_executor: Arc<MessageHandlerExecutor>,
    ) -> Result<(), SlateDBError> {
        let (flush_tx, flush_rx) = mpsc::unbounded_channel();
        {
            let mut inner = self.inner.write();
            inner.flush_tx = Some(flush_tx);
        }
        let wal_flush_handler = WalFlushHandler {
            max_flush_interval: self.max_flush_interval,
            wal_buffer_manager: self.clone(),
        };

        let result = task_executor.add_handler(
            WAL_BUFFER_TASK_NAME.to_string(),
            Box::new(wal_flush_handler),
            flush_rx,
            &Handle::current(),
        );
        {
            let mut inner = self.inner.write();
            inner.task_executor = Some(task_executor);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn buffered_wal_entries_count(&self) -> usize {
        let guard = self.inner.read();
        let flushing_wal_entries_count = guard
            .immutable_wals
            .iter()
            .map(|(_, wal)| wal.len())
            .sum::<usize>();
        guard.current_wal.len() + flushing_wal_entries_count
    }

    pub(crate) fn recent_flushed_wal_id(&self) -> u64 {
        let inner = self.inner.read();
        inner.recent_flushed_wal_id
    }

    #[cfg(test)] // used in compactor.rs
    pub(crate) fn is_empty(&self) -> bool {
        let inner = self.inner.read();
        inner.current_wal.is_empty() && inner.immutable_wals.is_empty()
    }

    /// Returns the total size of all unflushed WALs in bytes.
    pub(crate) fn estimated_bytes(&self) -> Result<usize, SlateDBError> {
        let inner = self.inner.read();
        let current_wal_size = self
            .table_store
            .estimate_encoded_size_wal(inner.current_wal.len(), inner.current_wal.size());

        let imm_wal_size = inner
            .immutable_wals
            .iter()
            .map(|(_, wal)| {
                self.table_store
                    .estimate_encoded_size_wal(wal.len(), wal.size())
            })
            .sum::<usize>();

        Ok(current_wal_size + imm_wal_size)
    }

    /// Append row entries to the current WAL. Returns a watcher for durability notification.
    /// TODO: validate the seq number is always increasing.
    pub(crate) fn append(
        &self,
        entries: &[RowEntry],
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        // TODO: check if the wal buffer is in a fatal error state.

        let mut inner = self.inner.write();
        for entry in entries {
            inner.current_wal.append(entry.clone());
        }
        Ok(inner.current_wal.durable_watcher())
    }

    /// Check if we need to flush the wal with considering max_wal_size. the checking over `max_wal_size`
    /// is not very strict, we have to ensure a write batch into a single WAL file.
    ///
    /// It's the caller's duty to call `maybe_trigger_flush` after calling `append`.
    pub(crate) fn maybe_trigger_flush(
        &self,
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        // check the size of the current wal
        let (durable_watcher, need_flush, flush_tx) = {
            let inner = self.inner.read();
            let current_wal_size = self
                .table_store
                .estimate_encoded_size_wal(inner.current_wal.len(), inner.current_wal.size());
            trace!(
                "checking flush trigger [current_wal_size={}, max_wal_bytes_size={}]",
                format_bytes_si(current_wal_size as u64),
                format_bytes_si(self.max_wal_bytes_size as u64),
            );
            let need_flush = current_wal_size >= self.max_wal_bytes_size;
            (
                inner.current_wal.durable_watcher(),
                need_flush,
                inner.flush_tx.clone(),
            )
        };
        if need_flush {
            flush_tx
                .as_ref()
                .expect("flush_tx not initialized, please call init first.")
                .send_safely(
                    self.db_state.read().closed_result_reader(),
                    WalFlushWork { result_tx: None },
                )?
        }

        let estimated_bytes = self.estimated_bytes()?;
        self.db_stats
            .wal_buffer_estimated_bytes
            .set(estimated_bytes as i64);
        Ok(durable_watcher)
    }

    /// Returns a watcher to await durability of the oldest unflushed WAL.
    /// If there are immutable WALs, it returns a watcher for the oldest immutable WAL.
    /// Otherwise, it returns a watcher for the current WAL if it's not empty.
    /// Returns None if there are no unflushed WALs.
    pub(crate) fn watcher_for_oldest_unflushed_wal(
        &self,
    ) -> Option<WatchableOnceCellReader<Result<(), SlateDBError>>> {
        let guard = self.inner.read();
        if let Some((_, wal)) = guard.immutable_wals.front() {
            Some(wal.durable_watcher())
        } else if !guard.current_wal.is_empty() {
            Some(guard.current_wal.durable_watcher())
        } else {
            None
        }
    }

    #[instrument(level = "trace", skip_all, err(level = tracing::Level::DEBUG))]
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        let flush_tx = self
            .inner
            .read()
            .flush_tx
            .clone()
            .expect("flush_tx not initialized, please call init first.");
        let (result_tx, result_rx) = oneshot::channel();
        flush_tx.send_safely(
            self.db_state.read().closed_result_reader(),
            WalFlushWork {
                result_tx: Some(result_tx),
            },
        )?;
        select! {
            result = result_rx => {
                result?
            }
        }
    }

    /// Returns the list of immutable WALs that need to be flushed.
    /// Used by the handler to determine which WALs to write to storage.
    fn flushing_wals(&self) -> Vec<(u64, Arc<WalBuffer>)> {
        let inner = self.inner.read();
        let mut flushing_wals = Vec::new();
        for (wal_id, wal) in inner.immutable_wals.iter() {
            if *wal_id > inner.recent_flushed_wal_id {
                flushing_wals.push((*wal_id, wal.clone()));
            }
        }
        flushing_wals
    }

    #[instrument(level = "trace", skip_all, err(level = tracing::Level::DEBUG))]
    async fn do_flush(&self) -> Result<(), SlateDBError> {
        self.freeze_current_wal()?;
        let flushing_wals = self.flushing_wals();

        if flushing_wals.is_empty() {
            return Ok(());
        }

        for (wal_id, wal) in flushing_wals.iter() {
            let result = self.do_flush_one_wal(*wal_id, wal.clone()).await;
            if let Err(e) = &result {
                // a WAL buffer can be retried to flush multiple times, but WatchableOnceCell is only set once.
                // we do NOT call `wal.notify_durable` as soon as encountered any error here, but notify
                // the error when we're sure enters fatal state in `do_cleanup`.
                error!("failed to flush WAL [wal_id={}]", wal_id);
                return Err(e.clone());
            }

            // increment the last flushed wal id, and last flushed seq
            {
                let mut inner = self.inner.write();
                inner.recent_flushed_wal_id = *wal_id;
                if let Some(seq) = wal.last_seq() {
                    inner.oracle.advance_durable_seq(seq);
                }
            }

            // notify durable only when the flush is successful.
            wal.notify_durable(result.clone());
        }

        self.maybe_release_immutable_wals();
        Ok(())
    }

    async fn do_flush_one_wal(&self, wal_id: u64, wal: Arc<WalBuffer>) -> Result<(), SlateDBError> {
        self.db_stats.wal_buffer_flushes.inc();

        let mut sst_builder = self.table_store.wal_table_builder();
        let (mut iter, last_tick) = (wal.iter(), wal.last_tick());
        while let Some(entry) = iter.next() {
            sst_builder.add(entry).await?;
        }

        let encoded_sst = sst_builder.build().await?;
        self.table_store
            .write_sst(&SsTableId::Wal(wal_id), encoded_sst, false)
            .await?;

        self.mono_clock.fetch_max_last_durable_tick(last_tick);
        Ok(())
    }

    fn freeze_current_wal(&self) -> Result<(), SlateDBError> {
        let is_empty = self.inner.read().current_wal.is_empty();
        if is_empty {
            return Ok(());
        }

        let next_wal_id = self.wal_id_incrementor.next_wal_id();
        let mut inner = self.inner.write();
        let current_wal = std::mem::replace(&mut inner.current_wal, WalBuffer::new());
        inner
            .immutable_wals
            .push_back((next_wal_id, Arc::new(current_wal)));
        Ok(())
    }

    /// Track the last applied sequence number. It's called when some WAL entries are applied to the memtable.
    /// This information of the last applied seq is used to determine if the immutable wals can be recycled.
    ///
    /// It's the caller's duty to ensure the seq is monotonically increasing.
    pub(crate) fn track_last_applied_seq(&self, seq: u64) {
        {
            let mut inner = self.inner.write();
            inner.last_applied_seq = Some(seq);
        }
        self.maybe_release_immutable_wals();
    }

    /// Recycle the immutable WALs that are flushed to the remote storage.
    fn maybe_release_immutable_wals(&self) {
        let mut inner = self.inner.write();

        let last_applied_seq = match inner.last_applied_seq {
            Some(seq) => seq,
            None => return,
        };

        let last_flushed_seq = inner.oracle.last_remote_persisted_seq();

        let mut releaseable_count = 0;
        for (_, wal) in inner.immutable_wals.iter() {
            if wal
                .last_seq()
                .map(|seq| seq <= last_applied_seq && seq <= last_flushed_seq)
                .unwrap_or(false)
            {
                releaseable_count += 1;
            } else {
                break;
            }
        }

        if releaseable_count > 0 {
            trace!(
                "draining immutable wals [releaseable_count={}]",
                releaseable_count
            );
            inner.immutable_wals.drain(..releaseable_count);
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn close(&self) -> Result<(), SlateDBError> {
        let task_executor = {
            let inner = self.inner.read();
            inner
                .task_executor
                .clone()
                .expect("task executor should be initialized")
        };
        task_executor.shutdown_task(WAL_BUFFER_TASK_NAME).await
    }
}

impl WalBuffer {
    /// Creates a new empty `WalBuffer`.
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            durable: WatchableOnceCell::new(),
            last_tick: i64::MIN,
            last_seq: 0,
            entries_size: 0,
        }
    }

    fn append(&mut self, entry: RowEntry) {
        if let Some(ts) = entry.create_ts {
            self.last_tick = ts;
        }
        self.last_seq = entry.seq;
        self.entries_size += entry.estimated_size();
        self.entries.push_back(entry);
    }

    /// Returns an iterator to iterate over the entries.
    fn iter(&self) -> WalBufferIterator {
        WalBufferIterator::new(self)
    }

    /// Returns a watcher that can be used to await durability.
    fn durable_watcher(&self) -> WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.durable.reader()
    }

    /// Awaits until the WAL is durable (flushed to storage).
    #[cfg(test)]
    async fn await_durable(&self) -> Result<(), SlateDBError> {
        self.durable.reader().await_value().await
    }

    /// Notifies that the WAL has been made durable (or failed).
    fn notify_durable(&self, result: Result<(), SlateDBError>) {
        self.durable.write(result);
    }

    /// Returns true if the buffer is empty.
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of entries in the buffer.
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the size of entries in bytes.
    fn size(&self) -> usize {
        self.entries_size
    }

    /// Returns the last sequence number written to this buffer.
    fn last_seq(&self) -> Option<u64> {
        if self.last_seq == 0 {
            None
        } else {
            Some(self.last_seq)
        }
    }

    /// Returns the last tick (timestamp) written to this buffer.
    fn last_tick(&self) -> i64 {
        self.last_tick
    }
}

impl WalBufferIterator {
    /// Creates a new iterator from a WalBuffer.
    /// This clones all entries from the buffer.
    pub(crate) fn new(wal_buffer: &WalBuffer) -> Self {
        let entries = wal_buffer.entries.iter().cloned().collect::<Vec<_>>();
        Self {
            entries: entries.into_iter(),
        }
    }

    pub(crate) fn next(&mut self) -> Option<RowEntry> {
        self.entries.next()
    }
}

#[derive(Debug)]
struct WalFlushWork {
    result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
}

struct WalFlushHandler {
    max_flush_interval: Option<Duration>,
    wal_buffer_manager: Arc<WalBufferManager>,
}

#[async_trait]
impl MessageHandler<WalFlushWork> for WalFlushHandler {
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<WalFlushWork>>)> {
        if let Some(max_flush_interval) = self.max_flush_interval {
            return vec![(
                max_flush_interval,
                Box::new(|| WalFlushWork { result_tx: None }),
            )];
        }
        vec![]
    }

    async fn handle(&mut self, message: WalFlushWork) -> Result<(), SlateDBError> {
        let WalFlushWork { result_tx } = message;
        if let Some(result_tx) = result_tx {
            let result = self.wal_buffer_manager.do_flush().await;
            result_tx
                .send(result.clone())
                .expect("failed to send flush result");
            result
        } else {
            self.wal_buffer_manager.do_flush().await
        }
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, WalFlushWork>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let error = result.err().unwrap_or(SlateDBError::Closed);

        // drain remaining messages
        while let Some(WalFlushWork { result_tx }) = messages.next().await {
            if let Some(result_tx) = result_tx {
                result_tx
                    .send(Err(error.clone()))
                    .expect("failed to send flush result");
            }
        }

        // notify all the flushing wals to be finished with fatal error or shutdown
        // error. we need ensure all the wal tables finally get notified. freeze current
        // WAL to notify writers in the subsequent flushing_wals loop.
        self.wal_buffer_manager.freeze_current_wal()?;

        let flushing_wals = self.wal_buffer_manager.flushing_wals();
        for (_, wal) in flushing_wals.iter() {
            wal.notify_durable(Err(error.clone()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MonotonicClock;
    use crate::db_status::DbStatusReporter;
    use crate::format::sst::SsTableFormat;
    use crate::iter::KeyValueIterator;
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::object_stores::ObjectStores;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::MockSystemClock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn make_entry(key: &str, value: &str, seq: u64, create_ts: Option<i64>) -> RowEntry {
        RowEntry::new(
            Bytes::from(key.to_string()),
            ValueDeletable::Value(Bytes::from(value.to_string())),
            seq,
            create_ts,
            None,
        )
    }

    #[test]
    fn test_new_buffer_initial_state() {
        let buffer = WalBuffer::new();

        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.size(), 0);
        assert_eq!(buffer.last_seq(), None);
        assert_eq!(buffer.last_tick(), i64::MIN);
    }

    #[test]
    fn test_append_single_entry() {
        let mut buffer = WalBuffer::new();
        let entry = make_entry("key1", "value1", 42, Some(1000));
        let expected_size = entry.estimated_size();

        buffer.append(entry);

        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.size(), expected_size);
        assert_eq!(buffer.last_seq(), Some(42));
        assert_eq!(buffer.last_tick(), 1000);
    }

    #[test]
    fn test_append_multiple_entries() {
        let mut buffer = WalBuffer::new();

        let entry1 = make_entry("key1", "value1", 10, Some(100));
        let entry2 = make_entry("key2", "value2", 20, Some(200));
        let entry3 = make_entry("key3", "value3", 30, Some(300));
        let entry4 = make_entry("key4", "value4", 40, None);

        let size1 = entry1.estimated_size();
        let size2 = entry2.estimated_size();
        let size3 = entry3.estimated_size();
        let size4 = entry4.estimated_size();

        buffer.append(entry1);
        buffer.append(entry2);
        buffer.append(entry3);
        buffer.append(entry4);

        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.size(), size1 + size2 + size3 + size4);
        assert_eq!(buffer.last_seq(), Some(40));
        assert_eq!(buffer.last_tick(), 300);
    }

    #[tokio::test]
    async fn test_notify_durable_success() {
        let mut buffer = WalBuffer::new();
        buffer.append(make_entry("key", "value", 1, None));

        buffer.notify_durable(Ok(()));

        let result = buffer.await_durable().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_notify_durable_error() {
        let mut buffer = WalBuffer::new();
        buffer.append(make_entry("key", "value", 1, None));

        buffer.notify_durable(Err(SlateDBError::Closed));

        let result = buffer.await_durable().await;
        assert!(matches!(result, Err(SlateDBError::Closed)));
    }

    #[tokio::test]
    async fn test_durable_watcher_returns_reader() {
        let mut buffer = WalBuffer::new();
        buffer.append(make_entry("key", "value", 1, None));

        let mut reader = buffer.durable_watcher();
        buffer.notify_durable(Ok(()));

        let result = reader.await_value().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_notify_durable_only_sets_once() {
        let mut buffer = WalBuffer::new();
        buffer.append(make_entry("key", "value", 1, None));

        buffer.notify_durable(Ok(()));
        buffer.notify_durable(Err(SlateDBError::Closed));

        let result = buffer.await_durable().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_iter() {
        let mut buffer = WalBuffer::new();
        let mut iter = buffer.iter();
        assert!(iter.next().is_none());

        let entry1 = make_entry("key1", "value1", 1, Some(100));
        let entry2 = make_entry("key2", "value2", 2, Some(200));
        let entry3 = make_entry("key3", "value3", 3, Some(300));

        buffer.append(entry1.clone());
        buffer.append(entry2.clone());
        buffer.append(entry3.clone());

        let mut iter = buffer.iter();
        let read1 = iter.next().unwrap();
        assert_eq!(read1.key, entry1.key);
        assert_eq!(read1.seq, entry1.seq);

        let read2 = iter.next().unwrap();
        assert_eq!(read2.key, entry2.key);
        assert_eq!(read2.seq, entry2.seq);

        let read3 = iter.next().unwrap();
        assert_eq!(read3.key, entry3.key);
        assert_eq!(read3.seq, entry3.seq);

        assert!(iter.next().is_none());

        let mut iter = buffer.iter();
        buffer.append(make_entry("key4", "value4", 4, None));

        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);

        let mut iter = buffer.iter();
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 4);
    }

    #[test]
    fn test_large_entry_size() {
        let mut buffer = WalBuffer::new();

        let large_key = "k".repeat(10_000);
        let large_value = "v".repeat(100_000);
        let entry = RowEntry::new(
            Bytes::from(large_key),
            ValueDeletable::Value(Bytes::from(large_value)),
            1,
            None,
            None,
        );
        let expected_size = entry.estimated_size();

        buffer.append(entry);

        assert_eq!(buffer.size(), expected_size);
        assert!(buffer.size() > 100_000);
    }

    struct MockWalIdStore {
        next_id: AtomicU64,
    }

    impl WalIdStore for MockWalIdStore {
        fn next_wal_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        }
    }

    async fn setup_wal_buffer() -> (Arc<WalBufferManager>, Arc<TableStore>, Arc<MockSystemClock>) {
        setup_wal_buffer_with_flush_interval(Duration::from_millis(10)).await
    }

    async fn setup_wal_buffer_with_flush_interval(
        flush_interval: Duration,
    ) -> (Arc<WalBufferManager>, Arc<TableStore>, Arc<MockSystemClock>) {
        let wal_id_store: Arc<dyn WalIdStore + Send + Sync> = Arc::new(MockWalIdStore {
            next_id: AtomicU64::new(1),
        });
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            Path::from("/root"),
            None,
        ));
        let test_clock = Arc::new(MockSystemClock::new());
        let mono_clock = Arc::new(MonotonicClock::new(test_clock.clone(), 0));
        let system_clock = Arc::new(DefaultSystemClock::new());
        let status_reporter = crate::db_status::DbStatusReporter::new(0);
        let oracle = Arc::new(DbOracle::new(0, 0, 0, status_reporter));
        let db_state = Arc::new(RwLock::new(DbState::new(
            new_dirty_manifest(),
            DbStatusReporter::new(0),
        )));
        let wal_buffer = Arc::new(WalBufferManager::new(
            wal_id_store,
            db_state.clone(),
            DbStats::new(&StatRegistry::new()),
            0, // recent_flushed_wal_id
            oracle,
            table_store.clone(),
            mono_clock,
            1000,                 // max_wal_bytes_size
            Some(flush_interval), // max_flush_interval
        ));
        let task_executor = Arc::new(MessageHandlerExecutor::new(
            db_state.read().closed_result(),
            system_clock.clone(),
        ));
        wal_buffer.init(task_executor.clone()).await.unwrap();
        task_executor
            .monitor_on(&Handle::current())
            .expect("failed to monitor executor");
        (wal_buffer, table_store, test_clock)
    }

    #[tokio::test]
    async fn test_basic_append_and_flush_operations() {
        let (wal_buffer, table_store, _) = setup_wal_buffer().await;

        // Append some entries
        let entry1 = make_entry("key1", "value1", 1, None);
        let entry2 = make_entry("key2", "value2", 2, None);

        wal_buffer.append(std::slice::from_ref(&entry1)).unwrap();
        wal_buffer.append(std::slice::from_ref(&entry2)).unwrap();

        // Flush the buffer
        wal_buffer.flush().await.unwrap();

        // Verify entries were written to storage
        let sst_iter_options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            table_store.open_sst(&SsTableId::Wal(1)).await.unwrap(),
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .unwrap();

        let read_entry1 = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(read_entry1.key, entry1.key);
        assert_eq!(read_entry1.value, entry1.value);
        assert_eq!(read_entry1.seq, entry1.seq);

        let read_entry2 = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(read_entry2.key, entry2.key);
        assert_eq!(read_entry2.value, entry2.value);
        assert_eq!(read_entry2.seq, entry2.seq);

        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_size_based_flush_triggering() {
        let (wal_buffer, _, _) = setup_wal_buffer_with_flush_interval(Duration::MAX).await;

        // Append entries until we exceed the size threshold
        let mut seq = 1;
        while wal_buffer.estimated_bytes().unwrap() < wal_buffer.max_wal_bytes_size {
            let entry = make_entry(&format!("key{}", seq), &format!("value{}", seq), seq, None);
            wal_buffer.append(&[entry]).unwrap();
            seq += 1;
        }
        let mut reader = wal_buffer.maybe_trigger_flush().unwrap();
        reader.await_value().await.unwrap();

        assert_eq!(wal_buffer.recent_flushed_wal_id(), 1);
    }

    #[tokio::test]
    async fn test_immutable_wal_reclaim() {
        let (wal_buffer, _, _) = setup_wal_buffer().await;

        // Append entries to create multiple WALs
        for i in 0..100 {
            let seq = i + 1;
            let entry = make_entry(&format!("key{}", i), &format!("value{}", i), seq, None);
            wal_buffer.append(&[entry]).unwrap();
            wal_buffer.flush().await.unwrap();
        }
        assert_eq!(wal_buffer.recent_flushed_wal_id(), 100);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 100);

        wal_buffer.track_last_applied_seq(50);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 50);
    }

    #[tokio::test]
    async fn test_immutable_wal_reclaim_with_flush_check() {
        let (wal_buffer, _, _) = setup_wal_buffer().await;

        // Append entries to create multiple WALs
        for i in 0..100 {
            let seq = i + 1;
            let entry = make_entry(&format!("key{}", i), &format!("value{}", i), seq, None);
            wal_buffer.append(&[entry]).unwrap();
            wal_buffer.flush().await.unwrap();
        }
        wal_buffer.track_last_applied_seq(50);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 50);
        assert_eq!(wal_buffer.recent_flushed_wal_id(), 100);

        // set flush seq to 80, and track last applied seq to 90, it should release 20 wals
        {
            let inner = wal_buffer.inner.write();
            inner.oracle.set_durable_seq_unsafe(80);
        }
        wal_buffer.track_last_applied_seq(90);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 20);
    }
}
