use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use chrono::Duration;
use tokio::sync::{oneshot, Mutex};

use crate::{
    db_state::SsTableId, mem_table::KVTable, types::RowEntry, utils::WatchableOnceCell,
    SlateDBError,
};

/// [`WalManager`] manages write-ahead logging (WAL) for durability and crash recovery.
///
/// The WAL manager buffers write operations in memory before flushing them to persistent storage. It
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
pub struct WalManager {
    inner: Arc<Mutex<WalManagerInner>>,
}

struct WalManagerInner {
    current_wal: KVTable,
    immutable_wals: VecDeque<KVTable>,
    flush_tx: tokio::sync::mpsc::Sender<WalFlushWork>,
    fatal: Option<SlateDBError>,
    // The flusher will take one immutable wal from the `immutable_wals` from immutable wals.
    // It'll update the last_flushed_wal_id and last_flushed_seq when the flush is done.
    last_flushed_wal_id: Option<u64>,
    /// The last flushed sequence number. Updated by WalFlusher.
    last_flushed_seq: u64,
    /// Whenever a WAl is applied to Memtable and successfully flushed to remote storage,
    /// the immutable wal can be recycled.
    last_applied_seq: u64,
    max_wal_size: u64,
    max_flush_interval: Option<Duration>,
}

impl WalManager {
    pub async fn append(&self, kventry: RowEntry) -> Result<(), SlateDBError> {
        todo!()
    }

    /// Check if we need to flush the wal with considering max_wal_size and max_flush_interval.
    /// the checking over `max_wal_size` is not very strict, we have to ensure a write batch
    /// into a single WAL file.
    ///
    /// It's the caller's duty to call `maybe_start_flush` after calling `append`.
    ///
    /// Both `maybe_start_flush` and `flush` are asynchronous.
    pub async fn maybe_start_flush(&self) -> Result<(), SlateDBError> {
        todo!()
    }

    /// Flush the wal to the remote storage.
    pub async fn start_flush(
        &self,
    ) -> Result<Arc<WatchableOnceCell<Result<(), SlateDBError>>>, SlateDBError> {
        todo!()
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        todo!()
    }
}

struct WalFlushWork {
    wals: Vec<KVTable>,
}

struct WalFlusher {
    // parallel: bool,
    last_flushed_wal_id: Option<u64>,
    flush_rx: tokio::sync::mpsc::Receiver<WalFlushWork>,
    fatal: Option<SlateDBError>,
}
