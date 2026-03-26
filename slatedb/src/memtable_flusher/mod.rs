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
use crate::utils::{SendSafely, WatchableOnceCell};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Monotonic ordering token assigned by the parallel L0 memtable flusher.
///
/// Workers carry this through upload completion so the manifest writer can restore
/// the original immutable-memtable retirement order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct FlushEpoch(pub(crate) u64);

/// Flush request target exposed by the memtable flusher.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FlushTarget {
    /// Attempt to make progress without waiting for a specific durability frontier.
    BestEffort,
    /// Operate against the currently durable frontier without requiring new flush work.
    CurrentDurable,
    /// Wait until all currently known immutable memtables are durably flushed.
    All,
}

pub(crate) enum FlushCommand {
    Flush {
        target: FlushTarget,
        sender: Option<oneshot::Sender<Result<FlushResult, SlateDBError>>>,
    },
    CreateCheckpoint {
        target: FlushTarget,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
    Shutdown,
}

type FlushCommandSender = mpsc::UnboundedSender<FlushCommand>;
pub(crate) type FlushCommandReceiver = mpsc::UnboundedReceiver<FlushCommand>;

/// Parallel L0 memtable flusher subsystem.
pub(crate) struct MemtableFlusher {
    commands: FlushCommandSender,
    commands_rx: Mutex<Option<FlushCommandReceiver>>,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    tracker_handle: Mutex<Option<JoinHandle<Result<(), SlateDBError>>>>,
}

impl MemtableFlusher {
    /// Creates a new memtable flusher.
    /// Call [`start`](Self::start) to spawn the background task.
    pub(crate) fn new() -> Self {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        Self {
            commands: commands_tx,
            commands_rx: Mutex::new(Some(commands_rx)),
            closed_result: WatchableOnceCell::new(),
            tracker_handle: Mutex::new(None),
        }
    }

    pub(crate) fn start(&self, inner: Arc<DbInner>, manifest: FenceableManifest, handle: &Handle) {
        let commands_rx = self
            .commands_rx
            .lock()
            .take()
            .expect("start called more than once");
        let uploader = Uploader::start(
            Arc::clone(&inner),
            inner.settings.l0_flush_parallelism,
            inner.settings.manifest_poll_interval,
            handle,
        );
        let manifest_writer = ManifestWriter::start(
            Arc::clone(&inner),
            manifest,
            inner.settings.manifest_poll_interval,
            handle,
        );
        let tracker_handle = handle.spawn(
            FlushTracker::new(
                inner,
                uploader,
                manifest_writer,
                self.closed_result.clone(),
                commands_rx,
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
        self.send_command(FlushCommand::Flush { target, sender })
    }

    /// Creates a checkpoint using the memtable flusher's flush semantics.
    pub(crate) async fn create_checkpoint(
        &self,
        target: FlushTarget,
        options: CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let (tx, rx) = oneshot::channel();
        self.send_command(FlushCommand::CreateCheckpoint {
            target,
            options,
            sender: tx,
        })?;
        rx.await.map_err(SlateDBError::ReadChannelError)?
    }

    fn send_command(&self, command: FlushCommand) -> Result<(), SlateDBError> {
        self.commands
            .send_safely(self.closed_result.reader(), command)
    }

    /// Closes the flusher and any owned subsystems.
    pub(crate) async fn close(&self) -> Result<(), SlateDBError> {
        // Ignore send errors — the tracker may already be gone.
        let _ = self.commands.send(FlushCommand::Shutdown);
        let tracker_handle = self.tracker_handle.lock().take();
        let result = if let Some(tracker_handle) = tracker_handle {
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
        };
        self.closed_result.write(result.clone().map(|_| ()));
        result
    }
}
