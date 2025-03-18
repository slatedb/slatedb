use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use tokio::{
    sync::{mpsc, oneshot, Mutex},
    time::Instant,
};

use crate::{
    db_state::SsTableId,
    mem_table::KVTable,
    tablestore::TableStore,
    types::RowEntry,
    utils::{MonotonicClock, WatchableOnceCell, WatchableOnceCellReader},
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
/// For strict durability requirements on synchronous writes, use [`WalManager::trigger_flush_with_watch()`] to explicitly
/// trigger and await a flush operation. This will flush ALL the in memory WALs to remote storage.
///
/// The manager is thread-safe and can be safely shared across multiple threads.
///
/// Please note:
///
/// - the size limit (`max_wal_size`) is a soft threshold. WAL entries within a single write batch are
///   guaranteed to be written atomically to the same WAL file.
/// - Fatal errors during flush operations are stored internally and propagated to all subsequent
///   operations. The manager becomes unusable after encountering a fatal error.
pub struct WalBufferManager {
    inner: Arc<Mutex<WalBufferManagerInner>>,
    quit_tx: Option<oneshot::Sender<()>>,
    flush_tx: Option<mpsc::Sender<WalFlushWork>>,
    table_store: Arc<TableStore>,
    mono_clock: Arc<MonotonicClock>,
}

struct WalBufferManagerInner {
    current_wal: Arc<KVTable>,
    current_wal_id: u64,
    /// When the current WAL is ready to be flushed, it'll be moved to the `immutable_wals`.
    /// The flusher will try flush all the immutable wals to remote storage.
    immutable_wals: VecDeque<(u64, Arc<KVTable>)>,
    /// fatal error includes permission denied, permanent IO errors.
    fatal: Option<SlateDBError>,
    /// Whenever a WAL is applied to Memtable and successfully flushed to remote storage,
    /// the immutable wal can be recycled in memory.
    last_applied_seq: u64,
    /// The flusher will update the last_flushed_wal_id and last_flushed_seq when the flush is done.
    last_flushed_wal_id: Option<u64>,
    /// The last flushed sequence number.
    last_flushed_seq: u64,
    /// The last time the flush is triggered.
    last_flush_triggered_at: Option<Instant>,
    /// max wal size.
    max_wal_size: u64,
    /// max flush interval.
    max_flush_interval_secs: Option<u64>,
}

impl WalBufferManager {
    /// Append row entries to the current WAL.
    /// TODO: validate the seq number is always increasing.
    pub async fn append(&self, entries: &[RowEntry]) -> Result<(), SlateDBError> {
        let inner = self.inner.lock().await;

        for entry in entries {
            inner.current_wal.put(entry.clone());
        }
        Ok(())
    }

    /// Check if we need to flush the wal with considering max_wal_size and max_flush_interval.
    /// the checking over `max_wal_size` is not very strict, we have to ensure a write batch
    /// into a single WAL file.
    ///
    /// It's the caller's duty to call `maybe_trigger_flush` after calling `append`.
    pub async fn maybe_trigger_flush(&self) -> Result<(), SlateDBError> {
        // check the size of the current wal
        let need_flush = {
            let inner = self.inner.lock().await;
            inner.current_wal.size() >= inner.max_wal_size as usize
        };
        if need_flush {
            self.flush_tx
                .as_ref()
                .unwrap()
                .send(WalFlushWork { result_tx: None })
                .await
                .map_err(|_| SlateDBError::BackgroundTaskShutdown)?;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), SlateDBError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.flush_tx
            .as_ref()
            .unwrap()
            .send(WalFlushWork {
                result_tx: Some(result_tx),
            })
            .await
            .map_err(|_| SlateDBError::BackgroundTaskShutdown)?;
        result_rx.await?
    }

    async fn do_flush(&self) -> Result<(), SlateDBError> {
        self.freeze_current_wal().await?;
        // flush the wal from previous flush wal id to the last immutable wal
        Ok(())
    }

    async fn freeze_current_wal(&self) -> Result<(), SlateDBError> {
        let mut inner = self.inner.lock().await;
        if !inner.current_wal.is_empty() {
            let current_wal_id = inner.current_wal_id;
            let current_wal = std::mem::replace(&mut inner.current_wal, Arc::new(KVTable::new()));
            inner
                .immutable_wals
                .push_back((current_wal_id, current_wal));
            inner.current_wal_id += 1;
        }
        Ok(())
    }

    /// Track the last applied sequence number. It's called when some WAL entries are applied to the memtable.
    /// This infomation of the last applied seq is used to determine if the immutable wals can be recycled.
    ///
    /// It's the caller's duty to ensure the seq is monotonically increasing.
    pub async fn track_last_applied_seq(&self, seq: u64) {
        let mut inner = self.inner.lock().await;
        inner.last_applied_seq = seq;
    }

    /// Recycle the immutable WALs that are applied to the memtable and flushed to the remote storage.
    fn maybe_release_immutable_wals(&self) {
        todo!()
    }

    /// Scan the WAL from the given sequence number. If the seq is None, it'll include the latest
    /// WALs. The scan includes the current WAL and the immutable WALs.
    /// TODO: return an kv iterator.
    pub fn scan(&self, seq: Option<u64>) -> Result<(), SlateDBError> {
        todo!()
    }

    pub async fn close(&mut self) -> Result<(), SlateDBError> {
        if let Some(quit_tx) = self.quit_tx.take() {
            quit_tx.send(()).unwrap();
        }
        Ok(())
    }
}

struct WalFlushWork {
    result_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
}
