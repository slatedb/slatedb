use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::db_state::SsTableId;
use crate::db_status::ClosedResultWriter;
use crate::dispatcher::{MessageHandler, MessageHandlerExecutor, MessageTickerDef};
use crate::error::SlateDBError;
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::utils::SafeSender;
use crate::utils::{format_bytes_si, WatchableOnceCell, WatchableOnceCellReader};
use crate::wal_buffer_stats::WalBufferStats;
use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use log::{error, trace, warn};
use slatedb_common::metrics::MetricsRecorderHelper;
use tokio::{runtime::Handle, sync::oneshot};
use tracing::instrument;

pub(crate) const WAL_BUFFER_TASK_NAME: &str = "wal_writer";

pub(crate) type WalStatusListener = Arc<dyn Fn(WalEvent) + Send + Sync + 'static>;

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
    stats: Arc<WalBufferStats>,
    table_store: Arc<TableStore>,
    max_wal_bytes_size: usize,
    max_flush_interval: Option<Duration>,
    /// The largest flush_epoch for which a size-triggered flush request has been
    /// sent. Compared against `flush_epoch` in the inner struct to avoid sending
    /// redundant flush requests for the same WAL.
    last_flush_requested_epoch: AtomicU64,
    /// The channel to send the flush work to the background worker.
    flush_tx: SafeSender<WalFlushWork>,
    /// The channel that the flush task waits on to receive work. Will be consumed by init
    flush_rx: Option<async_channel::Receiver<WalFlushWork>>,
    /// task executor for the background worker.
    task_executor: Option<Arc<MessageHandlerExecutor>>,
}

struct WalBufferManagerInner {
    current_wal: WalBuffer,
    /// When the current WAL is ready to be flushed, it'll be moved to the `immutable_wals`.
    /// The flusher will try flush all the immutable wals to remote storage.
    immutable_wals: VecDeque<(u64, Arc<WalBuffer>)>,
    /// The next wal id that will be generated
    next_wal_id: u64,
    /// Monotonically increasing epoch incremented each time the current WAL is
    /// frozen. Used with `last_flush_requested_epoch` to deduplicate size-triggered
    /// flush requests.
    flush_epoch: u64,
    /// The flusher will update the last_flushed_wal_id and last_flushed_seq when the flush is done.
    last_flushed_wal_id: u64,
    /// The last seq that was flushed to the WAL. This value will be None until the first flush.
    last_flushed_seq: Option<u64>,
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
        status_manager: crate::db_status::DbStatusManager,
        recorder: &MetricsRecorderHelper,
        last_flushed_wal_id: u64,
        table_store: Arc<TableStore>,
        max_wal_bytes_size: usize,
        max_flush_interval: Option<Duration>,
    ) -> Self {
        let current_wal = WalBuffer::new();
        let immutable_wals = VecDeque::new();
        let inner = WalBufferManagerInner {
            current_wal,
            immutable_wals,
            flush_epoch: 1,
            last_flushed_wal_id,
            next_wal_id: last_flushed_wal_id + 1,
            last_flushed_seq: None,
        };
        let (flush_tx, flush_rx) = SafeSender::unbounded_channel(status_manager.result_reader());
        Self {
            inner: Arc::new(parking_lot::RwLock::new(inner)),
            stats: Arc::new(WalBufferStats::new(recorder)),
            table_store,
            max_wal_bytes_size,
            max_flush_interval,
            last_flush_requested_epoch: AtomicU64::new(0),
            flush_tx,
            flush_rx: Some(flush_rx),
            task_executor: None,
        }
    }

    // todo: consider consolidating with new
    pub(crate) async fn init(
        &mut self,
        task_executor: Arc<MessageHandlerExecutor>,
    ) -> Result<(), SlateDBError> {
        let Some(flush_rx) = self.flush_rx.take() else {
            error!("WalBufferManager#init called multiple times");
            return Err(SlateDBError::InvalidDBState);
        };
        assert!(self.task_executor.is_none());
        let wal_flush_handler = WalFlushHandler {
            max_flush_interval: self.max_flush_interval,
            inner: self.inner.clone(),
            table_store: self.table_store.clone(),
            stats: self.stats.clone(),
            listener: None,
        };
        let result = task_executor.add_handler(
            WAL_BUFFER_TASK_NAME.to_string(),
            Box::new(wal_flush_handler),
            flush_rx,
            &Handle::current(),
        );
        self.task_executor = Some(task_executor);
        result
    }

    pub(crate) fn last_flushed_wal_id(&self) -> u64 {
        let inner = self.inner.read();
        inner.last_flushed_wal_id
    }

    pub(crate) fn status(&self) -> WalStatus {
        self.inner.read().status(&self.table_store)
    }

    /// Append row entries to the current WAL. Returns a watcher for durability notification.
    pub(crate) fn append(
        &self,
        entries: &[RowEntry],
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        // TODO: check if the wal buffer is in a fatal error state.
        self.inner.write().append(entries)
    }

    /// Check if we need to flush the wal with considering max_wal_size. the checking over `max_wal_size`
    /// is not very strict, we have to ensure a write batch into a single WAL file.
    ///
    /// It's the caller's duty to call `maybe_trigger_flush` after calling `append`.
    pub(crate) fn maybe_trigger_flush(
        &self,
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        let (durable_watcher, need_flush, flush_epoch) = {
            let inner = self.inner.read();
            // checks the size of the current wal
            let (need_flush, flush_epoch) =
                inner.needs_flush(&self.table_store, self.max_wal_bytes_size);
            (inner.current_wal.durable_watcher(), need_flush, flush_epoch)
        };
        if need_flush {
            // Only send a flush request if one hasn't already been sent for this epoch.
            // compare_exchange ensures only one writer wins per epoch.
            let last = self.last_flush_requested_epoch.load(Ordering::Relaxed);
            if last < flush_epoch
                && self
                    .last_flush_requested_epoch
                    .compare_exchange(last, flush_epoch, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                self.send_flush_request(None)?;
            }
        }

        let status = self.status();
        self.stats
            .estimated_bytes
            .set(status.estimated_bytes as i64);
        Ok(durable_watcher)
    }

    pub(crate) fn observer(&self) -> WalObserver {
        WalObserver {
            inner: self.inner.clone(),
            table_store: self.table_store.clone(),
            flush_tx: self.flush_tx.clone(),
        }
    }

    /// Send a flush request to the background flush worker.
    fn send_flush_request(
        &self,
        result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
    ) -> Result<(), SlateDBError> {
        self.stats.flush_requests.increment(1);
        self.flush_tx.send(WalFlushWork::Flush { result_tx })
    }

    pub(crate) fn flush(
        &self,
    ) -> Result<oneshot::Receiver<Result<(), SlateDBError>>, SlateDBError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_flush_request(Some(result_tx))?;
        Ok(result_rx)
    }

    #[allow(dead_code)]
    pub(crate) async fn close(&self) -> Result<(), SlateDBError> {
        let task_executor = self
            .task_executor
            .as_ref()
            .expect("task executor should be initialized");
        task_executor.shutdown_task(WAL_BUFFER_TASK_NAME).await
    }
}

impl WalBufferManagerInner {
    fn append(
        &mut self,
        entries: &[RowEntry],
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        // TODO: validate the seq number is always increasing.
        for entry in entries {
            self.current_wal.append(entry.clone());
        }
        Ok(self.current_wal.durable_watcher())
    }

    fn needs_flush(&self, table_store: &TableStore, max_wal_bytes_size: usize) -> (bool, u64) {
        // check the size of the current wal
        let current_wal_size =
            table_store.estimate_encoded_size_wal(self.current_wal.len(), self.current_wal.size());
        trace!(
            "checking flush trigger [current_wal_size={}, max_wal_bytes_size={}]",
            format_bytes_si(current_wal_size as u64),
            format_bytes_si(max_wal_bytes_size as u64),
        );
        let need_flush = current_wal_size >= max_wal_bytes_size;
        (need_flush, self.flush_epoch)
    }

    /// Returns the list of immutable WALs that need to be flushed.
    /// Used by the handler to determine which WALs to write to storage.
    fn flushing_wals(&self) -> Vec<(u64, Arc<WalBuffer>)> {
        let flushing_wals: Vec<_> = self.immutable_wals.iter().cloned().collect();
        for (wal_id, _wal) in flushing_wals.iter() {
            assert!(*wal_id > self.last_flushed_wal_id);
        }
        flushing_wals
    }

    /// Returns the total size of all unflushed WALs in bytes.
    fn estimated_bytes(&self, table_store: &TableStore) -> usize {
        let current_wal_size =
            table_store.estimate_encoded_size_wal(self.current_wal.len(), self.current_wal.size());
        let imm_wal_size = self
            .immutable_wals
            .iter()
            .map(|(_, wal)| table_store.estimate_encoded_size_wal(wal.len(), wal.size()))
            .sum::<usize>();
        current_wal_size + imm_wal_size
    }

    fn status(&self, table_store: &TableStore) -> WalStatus {
        let flushing_wal_entries_count = self
            .immutable_wals
            .iter()
            .map(|(_, wal)| wal.len())
            .sum::<usize>();
        let buffered_wal_entries_count = self.current_wal.len() + flushing_wal_entries_count;
        WalStatus {
            estimated_bytes: self.estimated_bytes(table_store),
            last_flushed_wal_id: self.last_flushed_wal_id,
            last_flushed_seq: self.last_flushed_seq,
            buffered_wal_entries_count,
        }
    }

    fn freeze_current_wal(&mut self) {
        if self.current_wal.is_empty() {
            return;
        }
        let next_wal_id = self.next_wal_id;
        self.next_wal_id += 1;
        let current_wal = std::mem::replace(&mut self.current_wal, WalBuffer::new());
        self.flush_epoch += 1;
        self.immutable_wals
            .push_back((next_wal_id, Arc::new(current_wal)));
    }

    fn record_flushed_wal(&mut self, flushed_wal_id: u64, flushed_wal: &Arc<WalBuffer>) {
        let (front_wal_id, front_wal_buffer) = self
            .immutable_wals
            .pop_front()
            .expect("no immutable wals found to pop");
        assert_eq!(front_wal_id, flushed_wal_id);
        assert!(Arc::ptr_eq(&front_wal_buffer, flushed_wal));
        assert_eq!(
            flushed_wal_id,
            self.last_flushed_wal_id + 1,
            "flushed wal id {} not next wal id after previous flushed {}",
            flushed_wal_id,
            self.last_flushed_wal_id
        );
        self.last_flushed_wal_id = flushed_wal_id;
        if let Some(seq) = flushed_wal.last_seq() {
            if let Some(last_flushed_seq) = self.last_flushed_seq {
                assert!(seq >= last_flushed_seq);
            }
            self.last_flushed_seq = Some(seq);
        }
    }
}

impl WalBuffer {
    /// Creates a new empty `WalBuffer`.
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            durable: WatchableOnceCell::new(),
            last_seq: 0,
            entries_size: 0,
        }
    }

    fn append(&mut self, entry: RowEntry) {
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

enum WalFlushWork {
    Flush {
        result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
    },
    Subscribe {
        listener: WalStatusListener,
    },
}

impl Debug for WalFlushWork {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WalFlushWork::Flush { .. } => f.write_str("Flush"),
            WalFlushWork::Subscribe { .. } => f.write_str("Subscribe"),
        }
    }
}

struct WalFlushHandler {
    max_flush_interval: Option<Duration>,
    inner: Arc<parking_lot::RwLock<WalBufferManagerInner>>,
    table_store: Arc<TableStore>,
    stats: Arc<WalBufferStats>,
    listener: Option<WalStatusListener>,
}

impl WalFlushHandler {
    #[instrument(level = "trace", skip_all, err(level = tracing::Level::DEBUG))]
    async fn do_flush(&self) -> Result<(), SlateDBError> {
        let flushing_wals = {
            let mut inner = self.inner.write();
            inner.freeze_current_wal();
            inner.flushing_wals()
        };

        for (wal_id, wal) in flushing_wals {
            let result = self.do_flush_one_wal(wal_id, wal.clone()).await;
            if let Err(e) = &result {
                // a WAL buffer can be retried to flush multiple times, but WatchableOnceCell is only set once.
                // we do NOT call `wal.notify_durable` as soon as encountered any error here, but notify
                // the error when we're sure enters fatal state in `do_cleanup`.
                error!("failed to flush WAL [wal_id={}]", wal_id);
                return Err(e.clone());
            }

            // increment the last flushed wal id, and last flushed seq
            let status = {
                let mut inner = self.inner.write();
                inner.record_flushed_wal(wal_id, &wal);
                inner.status(&self.table_store)
            };

            // we notify the listener first since that updates the oracle, and then notify
            // the table waiters. blocked writes wait on the table, so we have to update the oracle
            // first to preserve read-your-writes. This does mean that there is a small window
            // after notifying flushed before the wal memory is actually released.
            // TODO: once we change writes to block on the durable seq num from the oracle we
            //       can simplify this and fully drop the wal before notifying listeners
            self.notify_listener(WalEvent::WalFlushed(status));
            wal.notify_durable(result.clone());
            if Arc::strong_count(&wal) > 1 {
                warn!("outstanding references to wal id {} after flushing", wal_id);
            }
            drop(wal);
        }

        Ok(())
    }

    async fn do_flush_one_wal(&self, wal_id: u64, wal: Arc<WalBuffer>) -> Result<(), SlateDBError> {
        self.stats.flushes.increment(1);

        let mut sst_builder = self.table_store.wal_table_builder();
        let mut iter = wal.iter();
        while let Some(entry) = iter.next() {
            sst_builder.add(entry).await?;
        }

        let encoded_sst = sst_builder.build().await?;
        let written_bytes = encoded_sst.remaining_len() as u64;
        self.table_store
            .write_sst(&SsTableId::Wal(wal_id), &encoded_sst, false)
            .await?;
        self.stats.flush_bytes.increment(written_bytes);
        Ok(())
    }

    fn notify_listener(&self, event: WalEvent) {
        if let Some(l) = self.listener.as_ref() {
            (*l)(event);
        }
    }
}

#[async_trait]
impl MessageHandler<WalFlushWork> for WalFlushHandler {
    fn tickers(&mut self) -> Vec<MessageTickerDef<WalFlushWork>> {
        if let Some(max_flush_interval) = self.max_flush_interval {
            return vec![MessageTickerDef::new(
                max_flush_interval,
                Box::new(|| WalFlushWork::Flush { result_tx: None }),
            )];
        }
        vec![]
    }

    async fn handle(&mut self, message: WalFlushWork) -> Result<(), SlateDBError> {
        match message {
            WalFlushWork::Flush { result_tx } => {
                if let Some(result_tx) = result_tx {
                    let result = self.do_flush().await;
                    let _ = result_tx.send(result.clone());
                    result
                } else {
                    self.do_flush().await
                }
            }
            WalFlushWork::Subscribe { listener } => {
                // TODO: support multiple listeners. For now, the db listener is the only one
                assert!(self.listener.is_none());
                self.listener = Some(listener);
                Ok(())
            }
        }
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, WalFlushWork>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let error = result.err().unwrap_or(SlateDBError::Closed);

        // drain remaining messages
        while let Some(msg) = messages.next().await {
            match msg {
                WalFlushWork::Flush { result_tx } => {
                    if let Some(result_tx) = result_tx {
                        let _ = result_tx.send(Err(error.clone()));
                    }
                }
                WalFlushWork::Subscribe { listener: _ } => {}
            }
        }

        // notify all the flushing wals to be finished with fatal error or shutdown
        // error. we need ensure all the wal tables finally get notified. freeze current
        // WAL to notify writers in the subsequent flushing_wals loop.
        let flushing_wals = {
            let mut inner = self.inner.write();
            inner.freeze_current_wal();
            inner.flushing_wals()
        };
        for (_, wal) in flushing_wals.iter() {
            wal.notify_durable(Err(error.clone()));
        }
        Ok(())
    }
}

/// Interface for getting information about the current state of the Wal
#[derive(Clone)]
pub(crate) struct WalObserver {
    inner: Arc<parking_lot::RwLock<WalBufferManagerInner>>,
    table_store: Arc<TableStore>,
    flush_tx: SafeSender<WalFlushWork>,
}

/// Describes the current status of the WAL
#[derive(Debug, Clone)]
pub(crate) struct WalStatus {
    /// The estimated in-memory bytes used by the WAL to buffer unflushed writes.
    pub(crate) estimated_bytes: usize,
    /// The id of the last WAL file that was durably flushed
    pub(crate) last_flushed_wal_id: u64,
    /// The last sequence number that was durably flushed
    pub(crate) last_flushed_seq: Option<u64>,
    /// The number of writes currently buffered
    #[allow(dead_code)]
    pub(crate) buffered_wal_entries_count: usize,
}

/// An event emitted by [`WalBufferManager`] to subscribers.
#[derive(Debug, Clone)]
pub(crate) enum WalEvent {
    /// Emitted when a WAL file is durably flushed to storage. On receipt of this event, SlateDB
    /// notifies write tasks blocked on [`crate::config::WriteOptions::await_durable`]
    WalFlushed(WalStatus),
}

impl WalObserver {
    /// Gets information about the Wal buffer's current state
    pub(crate) fn status(&self) -> WalStatus {
        self.inner.read().status(self.table_store.as_ref())
    }

    pub(crate) fn subscribe(&self, listener: WalStatusListener) -> Result<(), SlateDBError> {
        self.flush_tx
            .send(WalFlushWork::Subscribe { listener })
            .map_err(|_err| SlateDBError::Closed)
    }
}

pub mod stats {
    use slatedb_common::metrics::{CounterFn, GaugeFn, MetricsRecorderHelper};
    use std::sync::Arc;

    macro_rules! wal_stat_name {
        ($suffix:expr) => {
            concat!("slatedb.wal.", $suffix)
        };
    }

    pub const WAL_BUFFER_FLUSHES: &str = wal_stat_name!("wal_buffer_flushes");
    pub const WAL_BUFFER_FLUSH_REQUESTS: &str = wal_stat_name!("wal_buffer_flush_requests");
    pub const WAL_BUFFER_ESTIMATED_BYTES: &str = wal_stat_name!("wal_buffer_estimated_bytes");
    pub const WAL_FLUSH_BYTES: &str = wal_stat_name!("wal_flush_bytes");

    pub(super) struct WalBufferStats {
        pub(super) estimated_bytes: Arc<dyn GaugeFn>,
        pub(super) flushes: Arc<dyn CounterFn>,
        pub(super) flush_requests: Arc<dyn CounterFn>,
        pub(super) flush_bytes: Arc<dyn CounterFn>,
    }

    impl WalBufferStats {
        pub(super) fn new(recorder: &MetricsRecorderHelper) -> Self {
            Self {
                estimated_bytes: recorder.gauge(WAL_BUFFER_ESTIMATED_BYTES).register(),
                flushes: recorder.counter(WAL_BUFFER_FLUSHES).register(),
                flush_requests: recorder.counter(WAL_BUFFER_FLUSH_REQUESTS).register(),
                flush_bytes: recorder.counter(WAL_FLUSH_BYTES).register(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_status::DbStatusManager;
    use crate::format::sst::SsTableFormat;
    use crate::iter::RowEntryIterator;
    use crate::manifest::SsTableView;
    use crate::object_stores::ObjectStores;
    use crate::oracle::DbOracle;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::metrics::{
        lookup_metric, DefaultMetricsRecorder, MetricLevel, MetricsRecorderHelper,
    };
    use slatedb_common::MockSystemClock;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
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

    async fn setup_wal_buffer() -> (
        WalBufferManager,
        Arc<TableStore>,
        Arc<MockSystemClock>,
        Arc<DefaultMetricsRecorder>,
    ) {
        setup_wal_buffer_with_flush_interval(Duration::from_millis(10)).await
    }

    async fn setup_wal_buffer_with_flush_interval(
        flush_interval: Duration,
    ) -> (
        WalBufferManager,
        Arc<TableStore>,
        Arc<MockSystemClock>,
        Arc<DefaultMetricsRecorder>,
    ) {
        setup_wal_buffer_with_args(flush_interval, Arc::new(|_status| {})).await
    }

    async fn setup_wal_buffer_with_args(
        flush_interval: Duration,
        listener: WalStatusListener,
    ) -> (
        WalBufferManager,
        Arc<TableStore>,
        Arc<MockSystemClock>,
        Arc<DefaultMetricsRecorder>,
    ) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            Path::from("/root"),
            None,
            TableStoreKind::Main,
        ));
        let test_clock = Arc::new(MockSystemClock::new());
        let system_clock = Arc::new(DefaultSystemClock::new());
        let status_manager = DbStatusManager::new(0);
        let oracle = Arc::new(DbOracle::new(0, 0, 0, status_manager.clone()));
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let mut wal_buffer = WalBufferManager::new(
            status_manager.clone(),
            &helper,
            0, // recent_flushed_wal_id
            table_store.clone(),
            1000,                 // max_wal_bytes_size
            Some(flush_interval), // max_flush_interval
        );
        let observer = wal_buffer.observer();
        observer
            .subscribe(Arc::new(move |status| {
                (*listener)(status.clone());
                let WalEvent::WalFlushed(status) = status;
                oracle.advance_durable_seq(status.last_flushed_seq.unwrap_or(0))
            }))
            .unwrap();
        let task_executor = Arc::new(MessageHandlerExecutor::new(
            Arc::new(status_manager),
            system_clock.clone(),
        ));
        wal_buffer.init(task_executor.clone()).await.unwrap();
        task_executor
            .monitor_on(&Handle::current())
            .expect("failed to monitor executor");
        (wal_buffer, table_store, test_clock, recorder)
    }

    #[tokio::test]
    async fn test_basic_append_and_flush_operations() {
        let (wal_buffer, table_store, _, _) = setup_wal_buffer().await;

        // Append some entries
        let entry1 = make_entry("key1", "value1", 1, None);
        let entry2 = make_entry("key2", "value2", 2, None);

        wal_buffer.append(std::slice::from_ref(&entry1)).unwrap();
        wal_buffer.append(std::slice::from_ref(&entry2)).unwrap();

        // Flush the buffer
        wal_buffer.flush().unwrap().await.unwrap().unwrap();

        // Verify entries were written to storage
        let sst_iter_options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            SsTableView::identity(table_store.open_sst(&SsTableId::Wal(1)).await.unwrap()),
            table_store.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .unwrap();

        let read_entry1 = iter.next().await.unwrap().unwrap();
        assert_eq!(read_entry1.key, entry1.key);
        assert_eq!(read_entry1.value, entry1.value);
        assert_eq!(read_entry1.seq, entry1.seq);

        let read_entry2 = iter.next().await.unwrap().unwrap();
        assert_eq!(read_entry2.key, entry2.key);
        assert_eq!(read_entry2.value, entry2.value);
        assert_eq!(read_entry2.seq, entry2.seq);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_size_based_flush_triggering() {
        let (wal_buffer, _, _, _) = setup_wal_buffer_with_flush_interval(Duration::MAX).await;

        // Append entries until we exceed the size threshold
        let mut seq = 1;
        while wal_buffer.status().estimated_bytes < wal_buffer.max_wal_bytes_size {
            let entry = make_entry(&format!("key{}", seq), &format!("value{}", seq), seq, None);
            wal_buffer.append(&[entry]).unwrap();
            seq += 1;
        }
        let mut reader = wal_buffer.maybe_trigger_flush().unwrap();
        reader.await_value().await.unwrap();

        assert_eq!(wal_buffer.last_flushed_wal_id(), 1);
    }

    #[tokio::test]
    async fn test_immutable_wal_reclaim() {
        let (wal_buffer, _, _, _) = setup_wal_buffer().await;

        // Append entries to create multiple WALs
        for i in 0..100 {
            let seq = i + 1;
            let entry = make_entry(&format!("key{}", i), &format!("value{}", i), seq, None);
            wal_buffer.append(&[entry]).unwrap();
            wal_buffer.flush().unwrap().await.unwrap().unwrap();
        }
        assert_eq!(wal_buffer.last_flushed_wal_id(), 100);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_maybe_trigger_flush_spams_flush_requests() {
        let (wal_buffer, _, _, recorder) =
            setup_wal_buffer_with_flush_interval(Duration::MAX).await;

        // Simulate many writers each appending a small entry and calling
        // maybe_trigger_flush, just like the real write path does. Under
        // load the WAL stays over the size threshold across many calls,
        // and each one would send a redundant flush request without the fix.
        let num_writes: u64 = 100;
        for seq in 1..=num_writes {
            let entry = make_entry(&format!("key{}", seq), &format!("value{}", seq), seq, None);
            wal_buffer.append(&[entry]).unwrap();
            wal_buffer.maybe_trigger_flush().unwrap();
        }

        let size_triggered_requests =
            lookup_metric(&recorder, stats::WAL_BUFFER_FLUSH_REQUESTS).unwrap();

        // Explicitly flush to drain everything, including any partial current WAL.
        wal_buffer.flush().unwrap().await.unwrap().unwrap();

        let actual_flushes = lookup_metric(&recorder, stats::WAL_BUFFER_FLUSHES).unwrap();

        // With the flush_requested flag, the number of size-triggered requests
        // should be bounded by the number of WALs, not by the number of writes.
        // Before the fix this was ~41 requests for 2 flushes (with 100 writes).
        assert!(
            actual_flushes >= 1,
            "expected at least one flush but got {}",
            actual_flushes,
        );
        assert!(
            size_triggered_requests <= actual_flushes,
            "size_triggered_requests ({}) should not exceed actual_flushes ({})",
            size_triggered_requests,
            actual_flushes,
        );
    }

    fn recording_listener() -> (WalStatusListener, Arc<Mutex<Vec<WalEvent>>>) {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = events.clone();
        let listener = Arc::new(move |event| {
            recorder.lock().unwrap().push(event);
        });
        (listener, events)
    }

    #[tokio::test]
    async fn test_listener_notified_when_flush_task_flushes_wal() {
        // given:
        let (listener, events) = recording_listener();
        let (wal_buffer, _, _, _) = setup_wal_buffer_with_args(Duration::MAX, listener).await;

        // when: Append an entry and explicitly flush it, driving the background flush task.
        wal_buffer
            .append(&[make_entry("key1", "value1", 1, None)])
            .unwrap();
        wal_buffer.flush().unwrap().await.unwrap().unwrap();

        // then: the listener should have been notified that wal 1 was flushed.
        let recorded = events.lock().unwrap().clone();
        let mut flushed: Vec<_> = recorded
            .iter()
            .map(|e| {
                let WalEvent::WalFlushed(status) = e;
                status
            })
            .collect();
        assert_eq!(flushed.len(), 1);
        let status = flushed.pop().unwrap();
        assert_eq!(status.last_flushed_wal_id, 1);
        assert_eq!(status.last_flushed_seq, Some(1));
    }

    #[tokio::test]
    async fn test_listener_notified_before_table_waiters() {
        // given:
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            Path::from("/root"),
            None,
            TableStoreKind::Main,
        ));
        let system_clock = Arc::new(DefaultSystemClock::new());
        let status_manager = DbStatusManager::new(0);
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let mut wal_buffer = WalBufferManager::new(
            status_manager.clone(),
            &helper,
            0,
            table_store.clone(),
            1000,
            Some(Duration::MAX),
        );
        let task_executor = Arc::new(MessageHandlerExecutor::new(
            Arc::new(status_manager),
            system_clock.clone(),
        ));
        wal_buffer.init(task_executor.clone()).await.unwrap();
        task_executor
            .monitor_on(&Handle::current())
            .expect("failed to monitor executor");
        let waiter = wal_buffer
            .append(&[make_entry("key1", "value1", 1, None)])
            .unwrap();
        let called = Arc::new(AtomicBool::new(false));
        let this_called = called.clone();
        let listener = move |event| {
            this_called.store(true, Ordering::SeqCst);
            assert!(matches!(event, WalEvent::WalFlushed(_)));
            // verifies that the table is not yet notified
            assert!(waiter.read().is_none())
        };
        wal_buffer.observer().subscribe(Arc::new(listener)).unwrap();

        // when/then:
        wal_buffer.flush().unwrap().await.unwrap().unwrap();
        assert!(called.load(Ordering::SeqCst));
    }
}
