use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use chrono::Duration;
use tokio::{
    sync::{oneshot, Mutex},
    time::Instant,
};

use crate::{
    db_state::SsTableId,
    mem_table::KVTable,
    types::RowEntry,
    utils::{WatchableOnceCell, WatchableOnceCellReader},
    SlateDBError,
};

/// [`WalBufferManager`] buffers write operations in memory before flushing them to persistent storage. It
/// maintains a `current_wal` buffer for active writes and a queue of immutable WALs pending flush.
///
/// By default, it offers a best-effort durability guarantee based on:
///
/// - `max_wal_size`: Flushes when `max_wal_size` bytes is exceeded
/// - `max_flush_interval`: Flushes after `max_flush_interval` elapses, if set
///
/// For strict durability requirements on synchronous writes, use [`WalManager::start_flush()`] to explicitly
/// trigger and await a flush operation. This will flush ALL the in memory WALs to remote storage.
///
/// The manager is thread-safe and can be safely shared across multiple threads.
///
/// Please note:
///
/// - WAL entries within a single write batch are guaranteed to be written atomically to the same WAL
///   file
/// - The size limit (`max_wal_size`) is a soft threshold - write batches are never split across WALs
/// - Fatal errors during flush operations are stored internally and propagated to all subsequent
///   operations. The manager becomes unusable after encountering a fatal error.
pub struct WalBufferManager {
    inner: Arc<Mutex<WalBufferManagerInner>>,
}

struct WalBufferManagerInner {
    current_wal: KVTable,
    current_wal_id: u64,
    /// When the current WAL is ready to be flushed, it'll be moved to the `immutable_wals`.
    /// The flusher will try flush all the immutable wals to remote storage.
    immutable_wals: VecDeque<(u64, KVTable)>,
    flush_tx: tokio::sync::mpsc::Sender<WalFlushWork>,
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
        if let Some(err) = &inner.fatal {
            return Err(err.clone());
        }

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
        let need_flush = self.need_flush().await;
        if need_flush {
            self.trigger_flush_with_watch().await?;
        }
        Ok(())
    }

    /// Start a flush operation and return a watchable cell that will be notified when the flush is done.
    ///
    pub async fn trigger_flush_with_watch(
        &self,
    ) -> Result<WatchableOnceCellReader<Result<(), SlateDBError>>, SlateDBError> {
        let mut inner = self.inner.lock().await;
        if let Some(err) = &inner.fatal {
            return Err(err.clone());
        }

        // move current wal to immutable wals, place a new current wal and increase the current wal id.
        let current_wal_id = inner.current_wal_id;
        let current_wal = std::mem::replace(&mut inner.current_wal, KVTable::new());
        let current_watch_cell = current_wal.watch_durable();
        inner
            .immutable_wals
            .push_back((current_wal_id, current_wal));
        inner.current_wal_id += 1;

        // trigger flusher to work
        inner
            .flush_tx
            .send(WalFlushWork {
                flush_until_wal_id: current_wal_id,
            })
            .await
            .map_err(|_| SlateDBError::BackgroundTaskShutdown)?;

        // return a watchable cell that will be notified when the flush is done.
        Ok(current_watch_cell)
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

    async fn need_flush(&self) -> bool {
        let inner = self.inner.lock().await;

        // if max_flush_interval is set, check if the time since the last flush is greater than the interval
        if let Some(max_flush_interval_secs) = inner.max_flush_interval_secs {
            let time_exceeded = inner
                .last_flush_triggered_at
                .map(|t| t.elapsed().as_secs() > max_flush_interval_secs)
                .unwrap_or(false);
            if time_exceeded {
                return true;
            }
        }

        // check the size of the current wal
        if inner.current_wal.size() >= inner.max_wal_size as usize {
            return true;
        }

        false
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        todo!()
    }
}

struct WalFlushWork {
    flush_until_wal_id: u64,
}

struct WalFlusher {
    flush_rx: tokio::sync::mpsc::Receiver<WalFlushWork>,
    mgr: Arc<Mutex<WalBufferManagerInner>>,
}
