use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    time::{Instant, Interval},
};

use crate::{
    db_state::SsTableId,
    iter::KeyValueIterator,
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
pub struct WalBufferManager {
    inner: Arc<Mutex<WalBufferManagerInner>>,
    quit_tx: Option<oneshot::Sender<()>>,
    flush_tx: Option<mpsc::Sender<WalFlushWork>>,
    fatal_once: WatchableOnceCell<SlateDBError>,
    table_store: Arc<TableStore>,
    mono_clock: Arc<MonotonicClock>,
    max_wal_size: u64,
    max_flush_interval_secs: u64,
}

struct WalBufferManagerInner {
    current_wal: Arc<KVTable>,
    current_wal_id: u64,
    /// When the current WAL is ready to be flushed, it'll be moved to the `immutable_wals`.
    /// The flusher will try flush all the immutable wals to remote storage.
    immutable_wals: VecDeque<(u64, Arc<KVTable>)>,
    /// Whenever a WAL is applied to Memtable and successfully flushed to remote storage,
    /// the immutable wal can be recycled in memory.
    last_applied_seq: u64,
    /// The flusher will update the last_flushed_wal_id and last_flushed_seq when the flush is done.
    last_flushed_wal_id: Option<u64>,
    /// The last flushed sequence number.
    last_flushed_seq: Option<u64>,
    /// The last time the flush is triggered.
    last_flush_triggered_at: Option<Instant>,
}

impl WalBufferManager {
    pub fn spawn_background() {}

    /// Append row entries to the current WAL. return the last seq number of the WAL.
    /// TODO: validate the seq number is always increasing.
    pub async fn append(&self, entries: &[RowEntry]) -> Result<Option<u64>, SlateDBError> {
        // TODO: check if the wal buffer is in a fatal error state.

        let inner = self.inner.lock().await;

        for entry in entries {
            inner.current_wal.put(entry.clone());
        }
        Ok(entries.last().map(|entry| entry.seq))
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
            inner.current_wal.size() >= self.max_wal_size as usize
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

    async fn do_background_work(
        &self,
        mut flush_rx: mpsc::Receiver<WalFlushWork>,
        mut quit_rx: oneshot::Receiver<()>,
        mut max_flush_interval: Option<Interval>,
    ) {
        let mut ticker_fut: Pin<Box<dyn Future<Output = Instant>>> =
            match max_flush_interval.as_mut() {
                Some(interval) => Box::pin(interval.tick()),
                None => Box::pin(std::future::pending()),
            };

        let mut contiguous_failures_count = 0;
        let mut fatal = None;
        loop {
            let result = select! {
                work = flush_rx.recv() => {
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
                _ = &mut ticker_fut => {
                    self.do_flush().await
                }
                _ = &mut quit_rx => {
                    break;
                }
            };

            // not all the flush error is fatal. on temporary network errors, we can retry later.
            // After a few continuous failures, we'll set it into fatal state.
            match result {
                Ok(_) => {
                    contiguous_failures_count = 0;
                }
                Err(e) => {
                    contiguous_failures_count += 1;
                    if contiguous_failures_count > 3 {
                        fatal = Some(e.clone());
                        break;
                    }
                }
            }
        }

        // There are two possible paths to exit the loop:
        //
        // 1. Got fatal error
        // 2. Got shutdown signal
        //
        // In both cases, we need to notify all the flushing WALs to be finished with fatal error or shutdown error.
        // If we got a fatal error, we need to set it in fatal_once to notify the database to enter fatal state.
        if let Some(e) = &fatal {
            self.fatal_once.write(e.clone());
        }
        // notify all the flushing wals to be finished with fatal error or shutdown error. we need ensure all the wal
        // tables finally get notified.
        let flushing_wals = self.flushing_wals().await;
        for (_, wal) in flushing_wals.iter() {
            wal.notify_durable(Err(fatal
                .clone()
                .unwrap_or(SlateDBError::BackgroundTaskShutdown)));
        }
    }

    // flush the wal from previous flush wal id to the last immutable wal
    async fn flushing_wals(&self) -> Vec<(u64, Arc<KVTable>)> {
        let inner = self.inner.lock().await;
        let mut flushing_wals = Vec::new();
        for (wal_id, wal) in inner.immutable_wals.iter() {
            if *wal_id > inner.last_flushed_wal_id.unwrap_or(0) {
                flushing_wals.push((*wal_id, wal.clone()));
            }
        }
        flushing_wals
    }

    async fn do_flush(&self) -> Result<(), SlateDBError> {
        self.freeze_current_wal().await?;
        let flushing_wals = self.flushing_wals().await;

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
                let mut inner = self.inner.lock().await;
                inner.last_flushed_wal_id = Some(*wal_id);
                if let Some(seq) = wal.last_seq() {
                    inner.last_flushed_seq = Some(seq);
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
            .write_sst(&SsTableId::Wal(wal_id), encoded_sst)
            .await?;

        self.mono_clock.fetch_max_last_durable_tick(wal.last_tick());
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
        {
            let mut inner = self.inner.lock().await;
            inner.last_applied_seq = seq;
        }
        self.maybe_release_immutable_wals().await;
    }

    /// Recycle the immutable WALs that are applied to the memtable and flushed to the remote storage.
    async fn maybe_release_immutable_wals(&self) {
        let mut inner = self.inner.lock().await;

        let mut releaseable_count = 0;
        for (_, wal) in inner.immutable_wals.iter() {
            if wal
                .last_seq()
                .map(|seq| seq <= inner.last_applied_seq)
                .unwrap_or(false)
            {
                releaseable_count += 1;
            } else {
                break;
            }
        }

        inner.immutable_wals.drain(..releaseable_count);
    }

    /// Scan the WAL from the given sequence number. If the seq is None, it'll include the latest
    /// WALs. The scan includes the current WAL and the immutable WALs.
    /// it's used for dirty read. will be implemented in another PR.
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
