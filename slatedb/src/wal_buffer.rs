use std::{collections::VecDeque, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use log::{error, trace};
use parking_lot::RwLock;
use tokio::{
    runtime::Handle,
    select,
    sync::{
        mpsc::{self},
        oneshot,
    },
};
use tracing::instrument;

use crate::oracle::Oracle;
use crate::{
    clock::MonotonicClock,
    db_state::{DbState, SsTableId},
    db_stats::DbStats,
    dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor},
    error::SlateDBError,
    iter::KeyValueIterator,
    mem_table::KVTable,
    oracle::DbOracle,
    tablestore::TableStore,
    types::RowEntry,
    utils::SendSafely,
    wal_id::WalIdStore,
};

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
    current_wal: Arc<KVTable>,
    /// When the current WAL is ready to be flushed, it'll be moved to the `immutable_wals`.
    /// The flusher will try flush all the immutable wals to remote storage.
    immutable_wals: VecDeque<(u64, Arc<KVTable>)>,
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

impl WalBufferManager {
    pub fn new(
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
        let current_wal = Arc::new(KVTable::new());
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

    pub async fn init(
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
            wal_buffer: self.clone(),
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
    pub fn buffered_wal_entries_count(&self) -> usize {
        let guard = self.inner.read();
        let flushing_wal_entries_count = guard
            .immutable_wals
            .iter()
            .map(|(_, wal)| wal.metadata().entry_num)
            .sum::<usize>();
        let current_wal_entries_count = guard.current_wal.metadata().entry_num;
        current_wal_entries_count + flushing_wal_entries_count
    }

    pub fn recent_flushed_wal_id(&self) -> u64 {
        let inner = self.inner.read();
        inner.recent_flushed_wal_id
    }

    #[cfg(test)] // used in compactor.rs
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read();
        inner.current_wal.is_empty() && inner.immutable_wals.is_empty()
    }

    /// Returns the total size of all unflushed WALs in bytes.
    pub fn estimated_bytes(&self) -> Result<usize, SlateDBError> {
        let inner = self.inner.read();
        let current_wal_size = self.table_store.estimate_encoded_size(
            inner.current_wal.metadata().entry_num,
            inner.current_wal.metadata().entries_size_in_bytes,
        );

        let imm_wal_size = inner
            .immutable_wals
            .iter()
            .map(|(_, wal)| {
                let metadata = wal.metadata();
                self.table_store
                    .estimate_encoded_size(metadata.entry_num, metadata.entries_size_in_bytes)
            })
            .sum::<usize>();

        Ok(current_wal_size + imm_wal_size)
    }

    /// Append row entries to the current WAL. return the last seq number of the WAL.
    /// TODO: validate the seq number is always increasing.
    pub fn append(&self, entries: &[RowEntry]) -> Result<Arc<KVTable>, SlateDBError> {
        // TODO: check if the wal buffer is in a fatal error state.

        let inner = self.inner.write();
        for entry in entries {
            inner.current_wal.put(entry.clone());
        }
        Ok(inner.current_wal.clone())
    }

    /// Check if we need to flush the wal with considering max_wal_size. the checking over `max_wal_size`
    /// is not very strict, we have to ensure a write batch into a single WAL file.
    ///
    /// It's the caller's duty to call `maybe_trigger_flush` after calling `append`.
    pub fn maybe_trigger_flush(&self) -> Result<Arc<KVTable>, SlateDBError> {
        // check the size of the current wal
        let (current_wal, need_flush, flush_tx) = {
            let inner = self.inner.read();
            let current_wal_size = self.table_store.estimate_encoded_size(
                inner.current_wal.metadata().entry_num,
                inner.current_wal.metadata().entries_size_in_bytes,
            );
            trace!(
                "checking flush trigger [current_wal_size={}, max_wal_bytes_size={}]",
                current_wal_size,
                self.max_wal_bytes_size,
            );
            let need_flush = current_wal_size >= self.max_wal_bytes_size;
            (
                inner.current_wal.clone(),
                need_flush,
                inner.flush_tx.clone(),
            )
        };
        if need_flush {
            flush_tx
                .as_ref()
                .expect("flush_tx not initialized, please call start_background first.")
                .send_safely(
                    self.db_state.read().closed_result_reader(),
                    WalFlushWork { result_tx: None },
                )?
        }

        let estimated_bytes = self.estimated_bytes()?;
        self.db_stats
            .wal_buffer_estimated_bytes
            .set(estimated_bytes as i64);
        Ok(current_wal)
    }

    /// Waits for a WAL to be flushed. If there are immutable WALs, it will
    /// wait for the oldest immutable WAL to be flushed. Otherwise, it will
    /// wait for the current WAL to be flushed. If both are empty, it will
    /// still block on the current WAL to ensure that the WAL buffer is not
    /// empty.
    pub(crate) fn oldest_unflushed_wal(&self) -> Option<Arc<KVTable>> {
        let (current_wal, oldest_immutable_wal) = {
            let guard = self.inner.read();
            let current_wal = guard.current_wal.clone();
            let maybe_oldest_immutable_wal =
                guard.immutable_wals.front().map(|(_, wal)| wal).cloned();
            (current_wal, maybe_oldest_immutable_wal)
        };
        if let Some(oldest_immutable_wal) = oldest_immutable_wal {
            Some(oldest_immutable_wal)
        } else if !current_wal.is_empty() {
            Some(current_wal)
        } else {
            None
        }
    }

    #[instrument(level = "trace", skip_all, err(level = tracing::Level::DEBUG))]
    pub async fn flush(&self) -> Result<(), SlateDBError> {
        let flush_tx = self
            .inner
            .read()
            .flush_tx
            .clone()
            .expect("flush_tx not initialized, please call start_background first.");
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

    // flush the wal from previous flush wal id to the last immutable wal
    fn flushing_wals(&self) -> Vec<(u64, Arc<KVTable>)> {
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
                // a KV table can be retried to flush multiple times, but WatchableOnceCell is only set once.
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
                    inner.oracle.last_remote_persisted_seq.store_if_greater(seq);
                }
            }

            // notify durable only when the flush is successful.
            wal.notify_durable(result.clone());
        }

        self.maybe_release_immutable_wals();
        Ok(())
    }

    async fn do_flush_one_wal(&self, wal_id: u64, wal: Arc<KVTable>) -> Result<(), SlateDBError> {
        self.db_stats.wal_buffer_flushes.inc();

        let mut sst_builder = self.table_store.table_builder();
        let mut iter = wal.iter();
        while let Some(entry) = iter.next_entry().await? {
            sst_builder.add(entry)?;
        }

        let encoded_sst = sst_builder.build()?;
        self.table_store
            .write_sst(&SsTableId::Wal(wal_id), encoded_sst, false)
            .await?;

        self.mono_clock.fetch_max_last_durable_tick(wal.last_tick());
        Ok(())
    }

    fn freeze_current_wal(&self) -> Result<(), SlateDBError> {
        let is_empty = self.inner.read().current_wal.is_empty();
        if is_empty {
            return Ok(());
        }

        let next_wal_id = self.wal_id_incrementor.next_wal_id();
        let mut inner = self.inner.write();
        let current_wal = std::mem::replace(&mut inner.current_wal, Arc::new(KVTable::new()));
        inner.immutable_wals.push_back((next_wal_id, current_wal));
        Ok(())
    }

    /// Track the last applied sequence number. It's called when some WAL entries are applied to the memtable.
    /// This information of the last applied seq is used to determine if the immutable wals can be recycled.
    ///
    /// It's the caller's duty to ensure the seq is monotonically increasing.
    pub fn track_last_applied_seq(&self, seq: u64) {
        {
            let mut inner = self.inner.write();
            inner.last_applied_seq = Some(seq);
        }
        self.maybe_release_immutable_wals();
    }

    /// Recycle the immutable WALs that are applied to the memtable and flushed to the remote storage.
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
    pub async fn close(&self) -> Result<(), SlateDBError> {
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

#[derive(Debug)]
struct WalFlushWork {
    result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
}

struct WalFlushHandler {
    max_flush_interval: Option<Duration>,
    wal_buffer: Arc<WalBufferManager>,
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
            let result = self.wal_buffer.do_flush().await;
            result_tx
                .send(result.clone())
                .expect("failed to send flush result");
            result
        } else {
            self.wal_buffer.do_flush().await
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
        self.wal_buffer.freeze_current_wal()?;

        let flushing_wals = self.wal_buffer.flushing_wals();
        for (_, wal) in flushing_wals.iter() {
            wal.notify_durable(Err(error.clone()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{DefaultSystemClock, MonotonicClock};
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::test_utils::TestClock;
    use crate::types::{RowEntry, ValueDeletable};
    use crate::utils::MonotonicSeq;
    use bytes::Bytes;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    struct MockWalIdStore {
        next_id: AtomicU64,
    }

    impl WalIdStore for MockWalIdStore {
        fn next_wal_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        }
    }

    async fn setup_wal_buffer() -> (Arc<WalBufferManager>, Arc<TableStore>, Arc<TestClock>) {
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
        let test_clock = Arc::new(TestClock::new());
        let mono_clock = Arc::new(MonotonicClock::new(test_clock.clone(), 0));
        let system_clock = Arc::new(DefaultSystemClock::new());
        let oracle = Arc::new(DbOracle::new(
            MonotonicSeq::new(0),
            MonotonicSeq::new(0),
            MonotonicSeq::new(0),
        ));
        let db_state = Arc::new(RwLock::new(DbState::new(new_dirty_manifest())));
        let wal_buffer = Arc::new(WalBufferManager::new(
            wal_id_store,
            db_state.clone(),
            DbStats::new(&StatRegistry::new()),
            0, // recent_flushed_wal_id
            oracle,
            table_store.clone(),
            mono_clock,
            1000,                            // max_wal_bytes_size
            Some(Duration::from_millis(10)), // max_flush_interval
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
        let entry1 = RowEntry::new(
            Bytes::from("key1"),
            ValueDeletable::Value(Bytes::from("value1")),
            1,
            None,
            None,
        );
        let entry2 = RowEntry::new(
            Bytes::from("key2"),
            ValueDeletable::Value(Bytes::from("value2")),
            2,
            None,
            None,
        );

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
        let (wal_buffer, _, _) = setup_wal_buffer().await;

        // Append entries until we exceed the size threshold
        let mut seq = 1;
        while wal_buffer.estimated_bytes().unwrap() < 115 * 10 {
            let entry = RowEntry::new(
                Bytes::from(format!("key{}", seq)),
                ValueDeletable::Value(Bytes::from(format!("value{}", seq))),
                seq,
                None,
                None,
            );
            wal_buffer.append(&[entry]).unwrap();
            wal_buffer
                .maybe_trigger_flush()
                .unwrap()
                .await_durable()
                .await
                .unwrap();
            seq += 1;
        }

        // Wait for background flush
        assert_eq!(wal_buffer.recent_flushed_wal_id(), 10);
    }

    #[tokio::test]
    async fn test_immutable_wal_reclaim() {
        let (wal_buffer, _, _) = setup_wal_buffer().await;

        // Append entries to create multiple WALs
        for i in 0..100 {
            let seq = i + 1;
            let entry = RowEntry::new(
                Bytes::from(format!("key{}", i)),
                ValueDeletable::Value(Bytes::from(format!("value{}", i))),
                seq,
                None,
                None,
            );
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
            let entry = RowEntry::new(
                Bytes::from(format!("key{}", i)),
                ValueDeletable::Value(Bytes::from(format!("value{}", i))),
                seq,
                None,
                None,
            );
            wal_buffer.append(&[entry]).unwrap();
            wal_buffer.flush().await.unwrap();
        }
        wal_buffer.track_last_applied_seq(50);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 50);
        assert_eq!(wal_buffer.recent_flushed_wal_id(), 100);

        // set flush seq to 80, and track last applied seq to 90, it should release 20 wals
        {
            let inner = wal_buffer.inner.write();
            inner.oracle.last_remote_persisted_seq.store(80);
        }
        wal_buffer.track_last_applied_seq(90);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 20);
    }
}
