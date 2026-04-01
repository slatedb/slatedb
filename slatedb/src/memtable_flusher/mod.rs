//! Parallel L0 memtable flusher pipeline.
//!
//! Coordinates the flush of immutable memtables to L0 SSTs in object storage.
//! The pipeline consists of three components:
//! - **tracker**: event loop for flush requests, dispatch, and waiter tracking
//! - **uploader**: parallel worker pool for SST build and upload
//! - **manifest_writer**: ordered manifest retirement and checkpoint creation

mod manifest_writer;
mod tracker;
mod uploader;

pub(crate) use manifest_writer::FlushResult;

use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::memtable_flusher::manifest_writer::ManifestWriter;
use crate::memtable_flusher::tracker::FlushTracker;
use crate::memtable_flusher::uploader::Uploader;
use crate::utils::safe_async_channel;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Monotonic ordering token assigned by the parallel L0 memtable flusher.
///
/// Workers carry this through upload completion so the manifest writer can restore
/// the original immutable-memtable retirement order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FlushEpoch(pub(crate) u64);

/// Flush request target exposed by the memtable flusher.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FlushTarget {
    /// Flush as many pending immutable memtables as L0 capacity allows. Used for
    /// writer backpressure relief and WAL-enabled checkpoint creation, where making
    /// some progress is sufficient.
    BestEffort,
    /// Return the current durability frontier without initiating new flush work.
    /// Used for `CheckpointScope::Durable` when the caller just needs a consistent
    /// snapshot of what is already durable.
    CurrentDurable,
    /// Wait until all currently known immutable memtables are durably flushed. Used
    /// for explicit `flush()` calls and WAL-disabled checkpoint creation, where full
    /// durability is required before proceeding.
    All,
}

/// Parallel L0 memtable flusher subsystem.
pub(crate) struct MemtableFlusher {
    messages_tx: safe_async_channel::SafeSender<tracker::TrackerMessage>,
    messages_rx: Mutex<Option<safe_async_channel::SafeReceiver<tracker::TrackerMessage>>>,
    tracker_handle: Mutex<Option<JoinHandle<Result<(), SlateDBError>>>>,
    cancellation_token: CancellationToken,
}

impl MemtableFlusher {
    /// Creates a new memtable flusher.
    /// Call [`start`](Self::start) to spawn the background task.
    pub(crate) fn new() -> Self {
        let (messages_tx, messages_rx) = safe_async_channel::unbounded_channel();
        Self {
            messages_tx,
            messages_rx: Mutex::new(Some(messages_rx)),
            tracker_handle: Mutex::new(None),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub(crate) fn start(&self, inner: Arc<DbInner>, manifest: FenceableManifest, handle: &Handle) {
        let messages_rx = self
            .messages_rx
            .lock()
            .take()
            .expect("start called more than once");
        let uploader = Uploader::start(
            Arc::clone(&inner),
            inner.settings.l0_flush_parallelism,
            inner.settings.manifest_poll_interval,
            handle,
            self.messages_tx.clone(),
        );
        let manifest_writer = ManifestWriter::start(
            Arc::clone(&inner),
            manifest,
            inner.settings.manifest_poll_interval,
            handle,
            self.messages_tx.clone(),
        );
        let tracker_handle = handle.spawn(
            FlushTracker::new(
                inner,
                uploader,
                manifest_writer,
                messages_rx,
                self.cancellation_token.clone(),
            )
            .run(),
        );
        *self.tracker_handle.lock() = Some(tracker_handle);
    }

    /// Processes one flush request using the requested target.
    pub(crate) async fn flush(&self, target: FlushTarget) -> Result<FlushResult, SlateDBError> {
        let (tx, rx) = oneshot::channel();
        self.send_flush_command(target, Some(tx))?;
        rx.await.map_err(SlateDBError::ReadChannelError)?
    }

    /// Sends a flush request without awaiting its result.
    pub(crate) fn request_flush(&self, target: FlushTarget) -> Result<(), SlateDBError> {
        self.send_flush_command(target, None)
    }

    fn send_flush_command(
        &self,
        target: FlushTarget,
        sender: Option<oneshot::Sender<Result<FlushResult, SlateDBError>>>,
    ) -> Result<(), SlateDBError> {
        self.messages_tx
            .send(tracker::TrackerMessage::FlushRequest { target, sender })
    }

    /// Creates a checkpoint using the memtable flusher's flush semantics.
    pub(crate) async fn create_checkpoint(
        &self,
        target: FlushTarget,
        options: CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let (tx, rx) = oneshot::channel();
        self.messages_tx
            .send(tracker::TrackerMessage::CheckpointRequest {
                target,
                options,
                sender: tx,
            })?;
        rx.await.map_err(SlateDBError::ReadChannelError)?
    }

    /// Closes the flusher and any owned subsystems.
    pub(crate) async fn close(&self) -> Result<(), SlateDBError> {
        self.cancellation_token.cancel();
        let tracker_handle = self.tracker_handle.lock().take();
        if let Some(tracker_handle) = tracker_handle {
            match tracker_handle.await {
                Ok(result) => result,
                Err(join_err) if join_err.is_cancelled() => Ok(()),
                Err(join_err) if join_err.is_panic() => {
                    Err(SlateDBError::BackgroundTaskPanic("memtable_flusher".into()))
                }
                Err(_) => Err(SlateDBError::BackgroundTaskCancelled(
                    "memtable_flusher".into(),
                )),
            }
        } else {
            Ok(())
        }
    }
}
