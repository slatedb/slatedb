use std::{collections::VecDeque, future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    clock::MonotonicClock,
    db_state::SsTableId,
    iter::KeyValueIterator,
    mem_table::KVTable,
    oracle::Oracle,
    tablestore::TableStore,
    types::RowEntry,
    utils::{spawn_bg_task, WatchableOnceCell, WatchableOnceCellReader},
    wal_id::WalIdStore,
    SlateDBError,
};

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
    quit_once: WatchableOnceCell<Result<(), SlateDBError>>,
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
    flush_tx: Option<mpsc::Sender<WalFlushWork>>,
    /// task handle of the background worker.
    background_task: Option<JoinHandle<Result<(), SlateDBError>>>,
    /// Whenever a WAL is applied to Memtable and successfully flushed to remote storage,
    /// the immutable wal can be recycled in memory.
    last_applied_seq: Option<u64>,
    /// The flusher will update the recent_flushed_wal_id and last_flushed_seq when the flush is done.
    recent_flushed_wal_id: u64,
    /// The oracle to track the last flushed sequence number.
    oracle: Arc<Oracle>,
}

impl WalBufferManager {
    pub fn new(
        wal_id_incrementor: Arc<dyn WalIdStore + Send + Sync>,
        recent_flushed_wal_id: u64,
        oracle: Arc<Oracle>,
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
            background_task: None,
            oracle,
        };
        Self {
            inner: Arc::new(parking_lot::RwLock::new(inner)),
            wal_id_incrementor,
            quit_once: WatchableOnceCell::new(),
            table_store,
            mono_clock,
            max_wal_bytes_size,
            max_flush_interval,
        }
    }

    pub async fn start_background(self: &Arc<Self>) -> Result<(), SlateDBError> {
        if self.inner.read().background_task.is_some() {
            return Err(SlateDBError::UnexpectedError {
                msg: "wal_buffer already started".to_string(),
            });
        }

        let (flush_tx, flush_rx) = mpsc::channel(1);
        {
            let mut inner = self.inner.write();
            inner.flush_tx = Some(flush_tx);
        }
        let max_flush_interval = self.max_flush_interval;
        let background_fut =
            self.clone()
                .do_background_work(flush_rx, self.quit_once.reader(), max_flush_interval);
        let self_clone = self.clone();
        let task_handle = spawn_bg_task(
            &tokio::runtime::Handle::current(),
            move |result| {
                Self::do_cleanup(self_clone, result.clone());
            },
            background_fut,
        );
        {
            let mut inner = self.inner.write();
            inner.background_task = Some(task_handle);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn buffered_wal_entries_count(&self) -> usize {
        let flushing_wal_entries_count = self
            .inner
            .read()
            .immutable_wals
            .iter()
            .map(|(_, wal)| wal.metadata().entry_num)
            .sum::<usize>();
        let current_wal_entries_count = self.inner.read().current_wal.metadata().entry_num;
        current_wal_entries_count + flushing_wal_entries_count
    }

    pub fn recent_flushed_wal_id(&self) -> u64 {
        let inner = self.inner.read();
        inner.recent_flushed_wal_id
    }

    #[allow(unused)] // used in compactor.rs
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read();
        inner.current_wal.is_empty() && inner.immutable_wals.is_empty()
    }

    /// Returns the total size of all unflushed WALs in bytes.
    pub async fn estimated_bytes(&self) -> Result<usize, SlateDBError> {
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
    pub async fn append(&self, entries: &[RowEntry]) -> Result<Option<u64>, SlateDBError> {
        // TODO: check if the wal buffer is in a fatal error state.

        let inner = self.inner.read();
        for entry in entries {
            inner.current_wal.put(entry.clone());
        }
        Ok(entries.last().map(|entry| entry.seq))
    }

    /// Check if we need to flush the wal with considering max_wal_size. the checking over `max_wal_size`
    /// is not very strict, we have to ensure a write batch into a single WAL file.
    ///
    /// It's the caller's duty to call `maybe_trigger_flush` after calling `append`.
    pub async fn maybe_trigger_flush(&self) -> Result<Arc<KVTable>, SlateDBError> {
        // check the size of the current wal
        let (current_wal, need_flush, flush_tx) = {
            let inner = self.inner.read();
            let current_wal_size = self.table_store.estimate_encoded_size(
                inner.current_wal.metadata().entry_num,
                inner.current_wal.metadata().entries_size_in_bytes,
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
                .send(WalFlushWork { result_tx: None })
                .await
                .map_err(|_| SlateDBError::BackgroundTaskShutdown)?;
        }
        Ok(current_wal)
    }

    // await the pending wals to be flushed to remote storage.
    pub async fn await_flush(&self) -> Result<(), SlateDBError> {
        let current_wal = self.inner.read().current_wal.clone();
        if current_wal.is_empty() {
            return Ok(());
        }
        current_wal.await_durable().await
    }

    pub async fn flush(&self) -> Result<(), SlateDBError> {
        let flush_tx = self
            .inner
            .read()
            .flush_tx
            .clone()
            .expect("flush_tx not initialized, please call start_background first.");
        let (result_tx, result_rx) = oneshot::channel();
        flush_tx
            .send(WalFlushWork {
                result_tx: Some(result_tx),
            })
            .await
            .map_err(|_| SlateDBError::BackgroundTaskShutdown)?;
        let mut quit_rx = self.quit_once.reader();
        // TODO: it's good to have a timeout here.
        select! {
            result = result_rx => {
                result?
            }
            result = quit_rx.await_value() => {
                match result {
                    Ok(_) => Err(SlateDBError::BackgroundTaskShutdown),
                    Err(e) => Err(e)
                }
            },
        }
    }

    async fn do_background_work(
        self: Arc<Self>,
        mut work_rx: mpsc::Receiver<WalFlushWork>,
        mut quit_rx: WatchableOnceCellReader<Result<(), SlateDBError>>,
        max_flush_interval: Option<Duration>,
    ) -> Result<(), SlateDBError> {
        let mut contiguous_failures_count = 0;
        loop {
            let mut flush_interval_fut: Pin<Box<dyn Future<Output = ()> + Send>> =
                match max_flush_interval {
                    Some(duration) => Box::pin(tokio::time::sleep(duration)),
                    None => Box::pin(std::future::pending()),
                };

            let result = select! {
                work = work_rx.recv() => {
                    let result_tx = match work {
                        None => break,
                        Some(work) => work.result_tx,
                    };
                    let result = self.do_flush().await;
                    // notify the result of do_flush to the caller if needed.
                    if let Some(result_tx) = result_tx {
                        result_tx.send(result.clone()).ok();
                    }
                    result
                }
                _ = quit_rx.await_value() => {
                    return Ok(());
                }
                _ = &mut flush_interval_fut => {
                    self.do_flush().await
                }
            };

            // not all the flush error is fatal. on temporary network errors, we can retry later.
            // After a few continuous failures, we'll return the error, and set it as fatal error
            // in cleanup.
            match result {
                Ok(_) => {
                    contiguous_failures_count = 0;
                }
                Err(e) => {
                    contiguous_failures_count += 1;
                    if contiguous_failures_count > 3 {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    fn do_cleanup(self: Arc<Self>, result: Result<(), SlateDBError>) {
        // There are two possible paths to exit the background loop:
        //
        // 1. Got fatal error
        // 2. Got shutdown signal
        //
        // In both cases, we need to notify all the flushing WALs to be finished with fatal error or shutdown error.
        // If we got a fatal error, we need to set it in quit_once to notify the database to enter fatal state.
        if let Err(e) = &result {
            self.quit_once.write(Err(e.clone()));
        }
        // notify all the flushing wals to be finished with fatal error or shutdown error. we need ensure all the wal
        // tables finally get notified.
        let fatal_or_shutdown = result.err().unwrap_or(SlateDBError::BackgroundTaskShutdown);
        let flushing_wals = self.flushing_wals();
        for (_, wal) in flushing_wals.iter() {
            wal.notify_durable(Err(fatal_or_shutdown.clone()));
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

    async fn do_flush(&self) -> Result<(), SlateDBError> {
        self.freeze_current_wal().await?;
        let flushing_wals = self.flushing_wals();

        if flushing_wals.is_empty() {
            return Ok(());
        }

        for (wal_id, wal) in flushing_wals.iter() {
            let result = self.do_flush_one_wal(*wal_id, wal.clone()).await;
            // a kv table can be retried to flush multiple times, but WatchableOnceCell is only set once.
            // let's notify Ok(()) as soon as possible, while the error will be notified when it goes into
            // fatal state.
            if result.is_ok() {
                wal.notify_durable(result.clone());
            }
            result?;

            // increment the last flushed wal id, and last flushed seq
            {
                let mut inner = self.inner.write();
                inner.recent_flushed_wal_id = *wal_id;
                if let Some(seq) = wal.last_seq() {
                    inner.oracle.last_remote_persisted_seq.store_if_greater(seq);
                }
            }
        }

        self.maybe_release_immutable_wals().await;
        Ok(())
    }

    async fn do_flush_one_wal(&self, wal_id: u64, wal: Arc<KVTable>) -> Result<(), SlateDBError> {
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

    async fn freeze_current_wal(&self) -> Result<(), SlateDBError> {
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
    /// This infomation of the last applied seq is used to determine if the immutable wals can be recycled.
    ///
    /// It's the caller's duty to ensure the seq is monotonically increasing.
    pub async fn track_last_applied_seq(&self, seq: u64) {
        {
            let mut inner = self.inner.write();
            inner.last_applied_seq = Some(seq);
        }
        self.maybe_release_immutable_wals().await;
    }

    /// Recycle the immutable WALs that are applied to the memtable and flushed to the remote storage.
    async fn maybe_release_immutable_wals(&self) {
        let mut inner = self.inner.write();

        let last_applied_seq = match inner.last_applied_seq {
            Some(seq) => seq,
            None => return,
        };

        let last_flushed_seq = inner.oracle.last_remote_persisted_seq.load();

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

        inner.immutable_wals.drain(..releaseable_count);
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        self.quit_once.write(Ok(()));

        Ok(())
    }
}

struct WalFlushWork {
    result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MonotonicClock;
    use crate::object_stores::ObjectStores;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
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
        let oracle = Arc::new(Oracle::new(MonotonicSeq::new(0)));
        let wal_buffer = Arc::new(WalBufferManager::new(
            wal_id_store,
            0, // recent_flushed_wal_id
            oracle,
            table_store.clone(),
            mono_clock,
            1000,                         // max_wal_bytes_size
            Some(Duration::from_secs(1)), // max_flush_interval
        ));
        wal_buffer.start_background().await.unwrap();
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

        wal_buffer.append(&[entry1.clone()]).await.unwrap();
        wal_buffer.append(&[entry2.clone()]).await.unwrap();

        // Flush the buffer
        wal_buffer.flush().await.unwrap();

        // Verify entries were written to storage
        let sst_iter_options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        let mut iter = SstIterator::new_owned(
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

    #[tokio::test]
    async fn test_size_based_flush_triggering() {
        let (wal_buffer, _, _) = setup_wal_buffer().await;

        // Append entries until we exceed the size threshold
        let mut seq = 1;
        while wal_buffer.estimated_bytes().await.unwrap() < 1024 * 16 {
            let entry = RowEntry::new(
                Bytes::from(format!("key{}", seq)),
                ValueDeletable::Value(Bytes::from(format!("value{}", seq))),
                seq,
                None,
                None,
            );
            wal_buffer.append(&[entry]).await.unwrap();
            wal_buffer.maybe_trigger_flush().await.unwrap();
            seq += 1;
        }

        // Wait for background flush
        wal_buffer.await_flush().await.unwrap();
        assert_eq!(wal_buffer.recent_flushed_wal_id(), 17);
    }

    #[tokio::test]
    async fn test_flush_error_handling_and_retry() {
        // TODO: use failpoint to inject hanging on flushing WAL
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
            wal_buffer.append(&[entry]).await.unwrap();
            wal_buffer.flush().await.unwrap();
        }
        assert_eq!(wal_buffer.recent_flushed_wal_id(), 100);
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 100);

        wal_buffer.track_last_applied_seq(50).await;
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
            wal_buffer.append(&[entry]).await.unwrap();
            wal_buffer.flush().await.unwrap();
        }
        wal_buffer.track_last_applied_seq(50).await;
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 50);
        assert_eq!(wal_buffer.recent_flushed_wal_id(), 100);

        // set flush seq to 80, and track last applied seq to 90, it should release 20 wals
        {
            let inner = wal_buffer.inner.write();
            inner.oracle.last_remote_persisted_seq.store(80);
        }
        wal_buffer.track_last_applied_seq(90).await;
        assert_eq!(wal_buffer.inner.read().immutable_wals.len(), 20);
    }
}
