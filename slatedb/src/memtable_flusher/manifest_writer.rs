//! Parallel L0 flush manifest manifest_writer.
//!
//! The manifest_writer owns ordered retirement of uploaded L0 tables:
//! - restore flush order using sequence ranges
//! - apply ordered in-memory manifest state transitions
//! - persist manifest updates
//! - report durable progress
//! - create checkpoints against manifest-owned state
//!
//! It does not own:
//! - upload execution
//! - flush request semantics
//! - flush waiter bookkeeping

use log::debug;

use super::tracker::TrackerMessage;
use super::uploader::UploadedMemtable;
use crate::checkpoint::{
    CheckpointCreateResult, CheckpointLifecycle, CheckpointRequest, CheckpointResult,
};
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::{collect_touched_segments, DbState, SsTableId, SsTableView};
use crate::dispatcher::MessageHandler;
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::manifest::Manifest;
use crate::memtable_flusher::CheckpointCursor;
use crate::oracle::Oracle;
use crate::utils::SafeSender;
use crate::VersionedManifest;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use parking_lot::RwLockWriteGuard;
use slatedb_common::clock::SystemClock;
use slatedb_txn_obj::DirtyObject;
use std::cmp;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

/// Result reported for a completed flush request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushResult {
    /// Highest durable sequence number covered by the completed flush (inclusive).
    pub(crate) durable_seq: u64,
}

/// Command submitted to the manifest_writer.
enum ManifestWriterCommand {
    /// One uploaded table is ready for ordered retirement.
    Uploaded(Box<UploadedMemtable>),
    /// Wait for a sequence to become durable, then respond with FlushResult.
    AwaitFlush {
        through_seq: Option<u64>,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    },
    /// Create a checkpoint against the current durable manifest state.
    CreateCheckpoint {
        options: CheckpointOptions,
        request: CheckpointRequest,
    },
    /// Periodic manifest poll to pick up remote changes (e.g. compaction).
    PollManifest {
        done: Option<oneshot::Sender<Result<(), SlateDBError>>>,
    },
    /// The WAL durable sequence advanced; retry any batch blocked on WAL durability.
    DurableSeqAdvanced,
}

impl std::fmt::Debug for ManifestWriterCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uploaded(u) => {
                write!(
                    f,
                    "Uploaded(first_seq={}, last_seq={})",
                    u.first_seq, u.last_seq
                )
            }
            Self::AwaitFlush { through_seq, .. } => {
                write!(f, "AwaitFlush({through_seq:?})")
            }
            Self::CreateCheckpoint { request, .. } => {
                write!(f, "CreateCheckpoint({})", request.id)
            }
            Self::PollManifest { .. } => write!(f, "PollManifest"),
            Self::DurableSeqAdvanced => write!(f, "DurableSeqAdvanced"),
        }
    }
}

pub(super) const MANIFEST_WRITER_TASK_NAME: &str = "l0_manifest_writer";

/// First delay between manifest polls while the flush tracker is stalled on L0.
const MIN_L0_STALL_POLL_BACKOFF: Duration = Duration::from_millis(50);
/// Longest delay between manifest polls while the flush tracker is stalled on L0.
const MAX_L0_STALL_POLL_BACKOFF: Duration = Duration::from_secs(1);

/// Ordered L0 retirement and manifest update subsystem.
pub(crate) struct ManifestWriter {
    commands_tx: SafeSender<ManifestWriterCommand>,
}

impl ManifestWriter {
    /// Starts the manifest_writer subsystem by registering with the executor.
    pub(crate) fn start(
        db: Arc<DbInner>,
        manifest: FenceableManifest,
        manifest_poll_interval: Duration,
        closed_result: &dyn crate::db_status::ClosedResultWriter,
        executor: &crate::dispatcher::MessageHandlerExecutor,
        tokio_handle: &Handle,
        tracker_tx: SafeSender<TrackerMessage>,
        l0_stall_rx: watch::Receiver<u64>,
    ) -> Result<Self, SlateDBError> {
        let (commands_tx, commands_rx) =
            SafeSender::unbounded_channel(closed_result.result_reader());
        let handler = ManifestWriterHandler::new(
            db,
            manifest,
            manifest_poll_interval,
            tracker_tx,
            l0_stall_rx,
        );
        executor.add_handler(
            MANIFEST_WRITER_TASK_NAME.to_string(),
            Box::new(handler),
            commands_rx,
            tokio_handle,
        )?;
        Ok(Self { commands_tx })
    }

    /// Notifies the manifest_writer that one uploaded table is ready for ordered retirement.
    pub(crate) async fn notify_uploaded(
        &self,
        uploaded_memtable: UploadedMemtable,
    ) -> Result<(), SlateDBError> {
        self.commands_tx
            .send(ManifestWriterCommand::Uploaded(Box::new(uploaded_memtable)))
    }

    /// Sends a flush request to the manifest_writer. The manifest_writer will respond
    /// once all sequences up to and including `through_seq` are durable (or
    /// immediately if `None`).
    pub(crate) fn send_flush(
        &self,
        through_seq: Option<u64>,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        self.commands_tx.send_or_handle_closed(
            ManifestWriterCommand::AwaitFlush {
                through_seq,
                sender,
            },
            |message, err| {
                if let ManifestWriterCommand::AwaitFlush { sender, .. } = message {
                    let _ = sender.send(Err(err.clone()));
                }
            },
        )
    }

    /// Sends a checkpoint request to the manifest writer.
    /// The manifest writer computes the bound of the checkpoint and accepts the request.
    /// It writes the checkpoint when every write through the bound is durable.
    pub(crate) fn begin_checkpoint(
        &self,
        options: CheckpointOptions,
        request: CheckpointRequest,
    ) -> Result<(), SlateDBError> {
        self.commands_tx.send_or_handle_closed(
            ManifestWriterCommand::CreateCheckpoint { options, request },
            |message, err| {
                if let ManifestWriterCommand::CreateCheckpoint { request, .. } = message {
                    request.lifecycle.fail(err.clone());
                }
            },
        )
    }

    #[cfg(test)]
    fn send_checkpoint(
        &self,
        through_seq: Option<u64>,
        options: CheckpointOptions,
        sender: oneshot::Sender<CheckpointResult>,
    ) -> Result<(), SlateDBError> {
        let id = Uuid::new_v4();
        let (lifecycle, result_rx, ready_rx) = CheckpointLifecycle::new();
        let request = CheckpointRequest {
            id,
            floor: through_seq.map(|seq| CheckpointCursor {
                seq,
                wal_file: Some(0),
            }),
            wait_for_l0: through_seq.is_some(),
            lifecycle,
        };
        let result = self.begin_checkpoint(options, request);
        tokio::spawn(async move {
            if ready_rx.await.is_err() {
                return;
            }
            if let Ok(result) = result_rx.await {
                let _ = sender.send(result);
            }
        });
        result
    }

    /// Enqueues a manifest poll; the result is delivered to `sender` on completion.
    pub(crate) fn send_poll(
        &self,
        sender: oneshot::Sender<Result<(), SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        self.commands_tx.send_or_handle_closed(
            ManifestWriterCommand::PollManifest { done: Some(sender) },
            |message, err| {
                if let ManifestWriterCommand::PollManifest { done: Some(sender) } = message {
                    let _ = sender.send(Err(err.clone()));
                }
            },
        )
    }

    pub(crate) async fn shutdown(executor: &crate::dispatcher::MessageHandlerExecutor) {
        if let Err(e) = executor.shutdown_task(MANIFEST_WRITER_TASK_NAME).await {
            log::warn!("failed to shutdown l0 manifest writer [error={:?}]", e);
        }
    }
}

struct ManifestWriterHandler {
    db: Arc<DbInner>,
    manifest: FenceableManifest,
    manifest_poll_interval: Duration,
    tracker_tx: SafeSender<TrackerMessage>,
    /// Uploaded memtables waiting to retire in immutable-memtable order, keyed by first_seq.
    ready: BTreeMap<u64, UploadedMemtable>,
    /// Highest last_seq that has been durably written to the manifest (inclusive).
    durable_seq: u64,
    /// Watches the database status so the manifest write can wait for
    /// WAL durability without blocking uploads.
    db_status_rx: watch::Receiver<crate::db_status::DbStatus>,
    /// Watches the flush tracker's L0 stall state so the manifest is polled
    /// while a writer is stalled. See [`L0StallPollNotifier`].
    l0_stall_rx: watch::Receiver<u64>,
    pending_flushes: Vec<PendingFlush>,
    pending_checkpoints: Vec<PendingCheckpoint>,
    pending_manifest_refreshes: Vec<oneshot::Sender<Result<(), SlateDBError>>>,
}

#[async_trait]
impl MessageHandler<ManifestWriterCommand> for ManifestWriterHandler {
    fn tickers(&mut self) -> Vec<crate::dispatcher::MessageTickerDef<ManifestWriterCommand>> {
        vec![crate::dispatcher::MessageTickerDef::new(
            self.manifest_poll_interval,
            Box::new(|| ManifestWriterCommand::PollManifest { done: None }),
        )]
    }

    fn notifiers(&mut self) -> Vec<Box<dyn crate::dispatcher::Notifier<ManifestWriterCommand>>> {
        vec![
            Box::new(DurableSeqNotifier {
                rx: self.db_status_rx.clone(),
            }),
            Box::new(L0StallPollNotifier::new(
                self.l0_stall_rx.clone(),
                Arc::clone(&self.db.system_clock),
                self.manifest_poll_interval,
            )),
        ]
    }

    async fn handle(&mut self, command: ManifestWriterCommand) -> Result<(), SlateDBError> {
        match command {
            ManifestWriterCommand::Uploaded(uploaded_memtable) => {
                self.handle_uploaded(*uploaded_memtable).await?;
            }
            ManifestWriterCommand::AwaitFlush {
                through_seq,
                sender,
            } => {
                self.handle_flush(through_seq, sender);
            }
            ManifestWriterCommand::CreateCheckpoint { options, request } => {
                self.handle_create_checkpoint(options, request)?;
            }
            ManifestWriterCommand::PollManifest { done } => {
                self.refresh_manifest_progress(done).await?;
            }
            ManifestWriterCommand::DurableSeqAdvanced => {}
        }
        self.process_ready_work().await
    }

    async fn cleanup(
        &mut self,
        commands: BoxStream<'async_trait, ManifestWriterCommand>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let mut commands = commands.fuse();
        let close_result = self.try_graceful_cleanup(&mut commands, &result).await;
        let error = result
            .and(close_result.clone())
            .err()
            .unwrap_or(SlateDBError::Closed);
        // Fail requests that remain after cleanup with the database error.
        while let Some(command) = commands.next().await {
            self.collect_pending_waiter(command, &error);
        }
        self.fail_pending_flushes(&error);
        self.fail_pending_checkpoints(&error);
        self.fail_pending_manifest_refreshes(&error);
        close_result
    }
}

impl ManifestWriterHandler {
    fn new(
        db: Arc<DbInner>,
        manifest: FenceableManifest,
        manifest_poll_interval: Duration,
        tracker_tx: SafeSender<TrackerMessage>,
        l0_stall_rx: watch::Receiver<u64>,
    ) -> Self {
        let durable_seq = db.oracle.last_remote_persisted_seq();
        let db_status_rx = db.status_manager.subscribe();
        Self {
            db,
            manifest,
            manifest_poll_interval,
            tracker_tx,
            pending_flushes: Vec::new(),
            ready: BTreeMap::new(),
            durable_seq,
            db_status_rx,
            l0_stall_rx,
            pending_checkpoints: Vec::new(),
            pending_manifest_refreshes: Vec::new(),
        }
    }

    async fn handle_uploaded(
        &mut self,
        uploaded_memtable: UploadedMemtable,
    ) -> Result<(), SlateDBError> {
        if self
            .ready
            .insert(uploaded_memtable.first_seq, uploaded_memtable)
            .is_some()
        {
            return Err(SlateDBError::InvalidDBState);
        }
        Ok(())
    }

    fn handle_flush(
        &mut self,
        through_seq: Option<u64>,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    ) {
        if self.is_durable(through_seq) {
            let _ = sender.send(Ok(self.flush_result()));
        } else {
            self.pending_flushes.push(PendingFlush {
                through_seq,
                sender,
            });
        }
    }

    fn is_durable(&self, through_seq: Option<u64>) -> bool {
        match through_seq {
            None => true,
            Some(seq) => self.durable_seq >= seq,
        }
    }

    fn flush_result(&self) -> FlushResult {
        FlushResult {
            durable_seq: self.durable_seq,
        }
    }

    fn handle_create_checkpoint(
        &mut self,
        options: CheckpointOptions,
        request: CheckpointRequest,
    ) -> Result<(), SlateDBError> {
        let CheckpointRequest {
            id,
            floor,
            wait_for_l0,
            mut lifecycle,
        } = request;
        let durable = match self.durable_checkpoint_cursor() {
            Ok(durable) => durable,
            Err(error) => {
                lifecycle.fail(error);
                return Ok(());
            }
        };
        let floor = floor.unwrap_or(durable);
        // If the durable cursor is past the floor, the bound is the durable cursor. The caller
        // does not have its handle yet, so the checkpoint can include every durable write.
        // This also keeps next_wal_sst_id from moving back, because earlier manifests never
        // pass the durable WAL file. Otherwise, the bound is the floor.
        let bound = if durable.seq > floor.seq {
            if let (Some(durable_wal_file), Some(floor_wal_file)) =
                (durable.wal_file, floor.wal_file)
            {
                // The last write of the floor WAL file is floor.seq, so a later write is in a
                // later WAL file.
                assert!(
                    durable_wal_file > floor_wal_file,
                    "durable cursor is past the checkpoint floor, but its WAL file is not \
                     [durable={:?}, floor={:?}]",
                    durable,
                    floor
                );
            }
            durable
        } else {
            floor
        };
        let l0_through_seq = if self.db.wal_enabled {
            wait_for_l0.then_some(floor.seq)
        } else {
            // Without a WAL, a write is durable only after it is in L0.
            Some(bound.seq)
        };
        fail_parallel::fail_point!(
            Arc::clone(&self.db.fp_registry),
            "checkpoint-after-bound",
            |_| { Ok(()) }
        );
        // Register the bound before the next command, so that no later manifest write passes it.
        lifecycle.accept();
        // Requests can arrive out of bound order, so keep the queue sorted by bound. A request
        // goes after the requests with the same bound.
        let index = self.pending_checkpoints.partition_point(|pending| {
            (pending.bound.seq, pending.bound.wal_file) <= (bound.seq, bound.wal_file)
        });
        // The WAL file of a cursor ends with the write at its sequence number, so bounds with the
        // same sequence number name the same WAL file.
        let conflict = self.pending_checkpoints.iter().find(|pending| {
            pending.bound.seq == bound.seq && pending.bound.wal_file != bound.wal_file
        });
        assert!(
            conflict.is_none(),
            "checkpoint bounds with the same sequence number have different WAL files \
             [bound={:?}, pending={:?}]",
            bound,
            conflict.map(|pending| pending.bound)
        );
        self.pending_checkpoints.insert(
            index,
            PendingCheckpoint {
                id,
                bound,
                l0_through_seq,
                options,
                lifecycle,
            },
        );
        Ok(())
    }

    /// Returns the cursor of the last durable write.
    ///
    /// If the WAL is enabled, the WAL status gives the cursor. Otherwise, the durable sequence
    /// number of the oracle gives the cursor, because L0 holds the only durable copy of a write.
    fn durable_checkpoint_cursor(&self) -> Result<CheckpointCursor, SlateDBError> {
        if !self.db.wal_enabled {
            return Ok(CheckpointCursor {
                seq: self.db.oracle.last_remote_persisted_seq(),
                wal_file: None,
            });
        }
        let status = self.db.wal_observer.status()?;
        Ok(CheckpointCursor {
            // The WAL reports no sequence number until its first flush. The durable sequence
            // number of the oracle then covers the writes that recovery replayed.
            seq: status
                .last_flushed_seq
                .unwrap_or_else(|| self.db.oracle.last_remote_persisted_seq()),
            wal_file: Some(status.last_flushed_wal_id),
        })
    }

    async fn process_ready_work(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.write_ready_checkpoints().await?;
            let Some(staged_batch) = self.take_next_ready_batch() else {
                return Ok(());
            };
            let through_seq = staged_batch
                .last()
                .map(|uploaded| uploaded.last_seq)
                .expect("staged batch should not be empty");
            let attached_checkpoints = self.take_ready_checkpoints(through_seq)?;
            self.apply_ready_batch(staged_batch, attached_checkpoints, through_seq)
                .await?;
        }
    }

    /// Returns the next batch of uploaded tables to apply, oldest first.
    fn take_next_ready_batch(&mut self) -> Option<Vec<UploadedMemtable>> {
        let durable_seq = self.db_status_rx.borrow().durable_seq;
        // L0 must not pass the bound of a pending checkpoint.
        let checkpoint_boundary = self
            .pending_checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.options.source.is_none())
            .map(|checkpoint| checkpoint.bound.seq)
            .min();
        let imm_memtables: Vec<_> = {
            let guard = self.db.state.read();
            guard.state().imm_memtable.iter().rev().cloned().collect()
        };
        let mut batch = Vec::new();

        for imm_memtable in &imm_memtables {
            let first_seq = imm_memtable
                .table()
                .first_seq()
                .expect("immutable memtable has no entries");
            let Some(uploaded) = self.ready.get(&first_seq) else {
                break;
            };
            assert!(
                Arc::ptr_eq(&uploaded.imm_memtable, imm_memtable),
                "uploaded memtable identity mismatch for first_seq {}",
                first_seq
            );
            // WAL SSTs must be durable before the manifest is updated (see #1255).
            if self.db.wal_enabled && uploaded.last_seq > durable_seq {
                break;
            }
            if checkpoint_boundary.is_some_and(|boundary| uploaded.last_seq > boundary) {
                break;
            }

            let uploaded = self.ready.remove(&first_seq).expect("peeked entry missing");
            let reached_boundary = checkpoint_boundary == Some(uploaded.last_seq);
            batch.push(uploaded);
            if reached_boundary {
                break;
            }
        }

        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }

    /// Removes the checkpoints that the next manifest write can include.
    ///
    /// `l0_last_seq` is the last sequence number in L0 after that write. A checkpoint with a
    /// bound cannot leave before a pending checkpoint with a lower bound, because the write
    /// limits its WAL range to the lowest pending bound. A checkpoint with the same bound as a
    /// pending one can leave. A source checkpoint has no bound. It leaves when its source is
    /// not pending. The removed checkpoints with a bound share one WAL bound.
    fn take_ready_checkpoints(
        &mut self,
        l0_last_seq: u64,
    ) -> Result<Vec<PendingCheckpoint>, SlateDBError> {
        let last_flushed_wal_id = self.db.wal_observer.status()?.last_flushed_wal_id;
        let mut wal_id_last_seen = None;
        let mut stalled_at: Option<CheckpointCursor> = None;
        let mut selected = vec![false; self.pending_checkpoints.len()];
        for (index, checkpoint) in self.pending_checkpoints.iter().enumerate() {
            if checkpoint.options.source.is_some() {
                continue;
            }
            if stalled_at.is_some_and(|bound| bound != checkpoint.bound) {
                break;
            }
            if checkpoint.is_ready(last_flushed_wal_id, l0_last_seq)
                && checkpoint.matches_or_sets_wal_boundary(&mut wal_id_last_seen)
            {
                selected[index] = true;
            } else if stalled_at.is_none() {
                stalled_at = Some(checkpoint.bound);
            }
        }
        for index in 0..self.pending_checkpoints.len() {
            let Some(source) = self.pending_checkpoints[index].options.source else {
                continue;
            };
            let source_pending = self
                .pending_checkpoints
                .iter()
                .enumerate()
                .any(|(other_index, other)| other.id == source && !selected[other_index]);
            if !source_pending {
                selected[index] = true;
            }
        }
        let mut ready = Vec::new();
        let mut ready_sources = Vec::new();
        let mut remaining = Vec::with_capacity(self.pending_checkpoints.len());
        for (checkpoint, take) in self.pending_checkpoints.drain(..).zip(selected) {
            if !take {
                remaining.push(checkpoint);
            } else if checkpoint.options.source.is_some() {
                ready_sources.push(checkpoint);
            } else {
                ready.push(checkpoint);
            }
        }
        self.pending_checkpoints = remaining;
        ready.extend(ready_sources);
        let mut bounds = ready
            .iter()
            .filter(|checkpoint| checkpoint.options.source.is_none())
            .map(|checkpoint| checkpoint.bound);
        if let Some(bound) = bounds.next() {
            // The ready checkpoints share a WAL file, and a WAL file ends with one write.
            // Without a WAL, L0 cannot pass the lowest bound, so only that bound can be ready.
            assert!(
                bounds.all(|other| other == bound),
                "ready checkpoints have different bounds [bounds={:?}]",
                ready
                    .iter()
                    .map(|checkpoint| checkpoint.bound)
                    .collect::<Vec<_>>()
            );
            // L0 never passes the bound of a pending checkpoint.
            assert!(
                l0_last_seq <= bound.seq,
                "L0 passed the checkpoint bound [l0_last_seq={}, bound={:?}]",
                l0_last_seq,
                bound
            );
            // Without a WAL, L0 holds the only durable copy of a write, so the checkpoint waits
            // until L0 reaches its bound.
            assert!(
                self.db.wal_enabled || l0_last_seq == bound.seq,
                "L0 did not reach the checkpoint bound [l0_last_seq={}, bound={:?}]",
                l0_last_seq,
                bound
            );
        }
        Ok(ready)
    }

    async fn apply_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        through_seq: u64,
    ) -> Result<(), SlateDBError> {
        if let Err(err) = self.apply_uploaded_state(&staged_batch) {
            self.fail_ready_batch(staged_batch, attached_checkpoints, err.clone())
                .await?;
            return Err(err);
        }

        for uploaded in &staged_batch {
            uploaded.imm_memtable.notify_uploaded(Ok(()));
        }
        self.db
            .db_stats
            .immutable_memtable_flushes
            .increment(staged_batch.len() as u64);

        match self
            .write_manifest_update_safely(&attached_checkpoints.iter().collect::<Vec<_>>())
            .await
        {
            Ok(checkpoint_results) => {
                self.finish_ready_batch(
                    staged_batch,
                    attached_checkpoints,
                    checkpoint_results,
                    through_seq,
                )
                .await
            }
            Err(err) => {
                self.fail_ready_batch(staged_batch, attached_checkpoints, err.clone())
                    .await?;
                Err(err)
            }
        }
    }

    fn apply_uploaded_state(&self, staged_batch: &[UploadedMemtable]) -> Result<(), SlateDBError> {
        let segmented = self.db.segment_extractor.is_some();
        let mut guard = self.db.state.write();
        let manifest = guard.modify(|modifier| {
            for uploaded in staged_batch {
                let uploaded_tracker = uploaded.imm_memtable.sequence_tracker();
                let popped = modifier
                    .state
                    .imm_memtable
                    .pop_back()
                    .expect("expected imm memtable");
                assert!(Arc::ptr_eq(&popped, &uploaded.imm_memtable));
                let core = &mut modifier.state.manifest.value.core;
                // `segments` may legitimately be empty when retention
                // pruned every entry: no builders open → no SSTs
                // uploaded. The memtable's seq/tick bookkeeping below
                // still advances. (This is true with or without an
                // extractor configured.)
                for segment in &uploaded.segments {
                    // Identity view: the view id is the physical SST ULID, so
                    // the timestamp `last_compacted_l0_sst_view_id` reads
                    // equals the one GC deletion reads (RFC-0029).
                    let view = SsTableView::identity(segment.sst_handle.clone());
                    let tree = if segmented {
                        // Extractor configured — every flush handle, including
                        // any with empty prefix, is routed into `segments`.
                        core.maybe_insert_tree(&segment.prefix)?
                    } else if segment.prefix.is_empty() {
                        // No extractor — singleton compatibility-encoded
                        // `prefix=""` segment lives in the top-level tree.
                        Arc::make_mut(&mut core.tree)
                    } else {
                        return Err(SlateDBError::InvalidSegmentPrefix {
                            prefix: segment.prefix.clone(),
                            conflict: Bytes::new(),
                        });
                    };
                    tree.l0.push_front(view);
                }
                core.replay_after_wal_id = uploaded.imm_memtable.recent_flushed_wal_id();

                let memtable_tick = uploaded.imm_memtable.table().last_tick();
                core.last_l0_clock_tick = cmp::max(core.last_l0_clock_tick, memtable_tick);
                if core.last_l0_clock_tick != memtable_tick {
                    return Err(SlateDBError::InvalidClockTick {
                        last_tick: core.last_l0_clock_tick,
                        next_tick: memtable_tick,
                    });
                }

                // The same sequence number can't span multiple L0' SSTs--only SSTs in SRs
                // can do that. So assert `>` rather than `>=`.
                assert!(uploaded.last_seq > core.last_l0_seq);
                core.last_l0_seq = uploaded.last_seq;
                core.sequence_tracker.extend_from(uploaded_tracker);
            }
            Ok(modifier.state.manifest.clone())
        })?;

        self.report_to_status_manager(&guard, manifest.into());
        Ok(())
    }

    /// Reports manifest and memtable updates to the status manager to notify subscriptions
    /// (i.e., `Db::subscribe()`) about the changes.
    ///
    /// `report_manifest_and_memtable_segments()` needs to be synchronized with the write path.
    ///  Otherwise, the following might happen:
    ///  ```ascii
    ///  T1: collect_touched_segments() collects touched segments from the memtables.
    ///  T2: A new segment is added to the active memtable and reported to the status manager
    ///  on the write path.
    ///  T3: The segments collected in T1 are reported to the status manager and overwrite the
    ///  change from T2 -> new segment in the active memtable would not be part of
    ///  the result of DbStatus::list_segments() until the active memtable is flushed.
    ///  ```
    fn report_to_status_manager(
        &self,
        guarded_db_state: &RwLockWriteGuard<DbState>,
        manifest: VersionedManifest,
    ) {
        let segments = collect_touched_segments(&guarded_db_state.view());
        self.db
            .status_manager
            .report_manifest_and_memtable_segments(manifest, segments);
    }

    async fn write_manifest_update_safely(
        &mut self,
        checkpoints: &[&PendingCheckpoint],
    ) -> Result<Vec<CheckpointResult>, SlateDBError> {
        loop {
            let result = self.write_manifest_update(checkpoints).await;
            if matches!(result.as_ref(), Err(err) if err.is_sequenced_write_conflict()) {
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    async fn write_manifest_update(
        &mut self,
        checkpoints: &[&PendingCheckpoint],
    ) -> Result<Vec<CheckpointResult>, SlateDBError> {
        let wal_boundary = self.checkpoint_wal_id_last_seen(checkpoints)?;
        let mut dirty = self.prepare_local_manifest_for_write(wal_boundary)?;
        let mut checkpoint_results = Vec::with_capacity(checkpoints.len());
        for pending in checkpoints {
            let created =
                self.manifest
                    .new_checkpoint_in(&dirty.value.core, pending.id, &pending.options);
            checkpoint_results.push(created.map(|checkpoint| {
                let manifest_id = checkpoint.manifest_id;
                dirty.value.core.checkpoints.push(checkpoint);
                CheckpointCreateResult {
                    id: pending.id,
                    manifest_id,
                }
            }));
        }
        self.manifest.update(dirty).await?;
        Ok(checkpoint_results)
    }

    fn checkpoint_wal_id_last_seen(
        &self,
        checkpoints: &[&PendingCheckpoint],
    ) -> Result<Option<u64>, SlateDBError> {
        let mut boundary = None;
        for checkpoint in checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.options.source.is_none())
        {
            match boundary {
                Some(current) => {
                    if checkpoint.bound.wal_file != current {
                        return Err(SlateDBError::InvalidDBState);
                    }
                }
                None => {
                    boundary = Some(checkpoint.bound.wal_file);
                }
            }
        }
        Ok(boundary.flatten())
    }

    async fn write_current_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            let result = self.write_current_manifest().await;
            if matches!(result.as_ref(), Err(err) if err.is_sequenced_write_conflict()) {
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    async fn write_current_manifest(&mut self) -> Result<(), SlateDBError> {
        let dirty = self.prepare_local_manifest_for_write(None)?;
        self.manifest.update(dirty.clone()).await?;
        self.db.status_manager.report_manifest(dirty.into());
        Ok(())
    }

    fn prepare_local_manifest_for_write(
        &self,
        attached_wal_boundary: Option<u64>,
    ) -> Result<DirtyObject<Manifest>, SlateDBError> {
        let min_active_snapshot_seq = [
            self.db.snapshot_manager.min_active_seq(),
            self.db.txn_manager.min_active_seq(),
        ]
        .into_iter()
        .flatten()
        .min();
        // A closed WAL still reports the last durable file for the final manifest write.
        let wal_status = self
            .db
            .wal_observer
            .status()
            .unwrap_or_else(|status| status);
        let mut guard = self.db.state.write();
        // Pending checkpoints limit every manifest write until their boundaries are published.
        let wal_boundary = self
            .pending_checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.options.source.is_none())
            .filter_map(|checkpoint| checkpoint.bound.wal_file)
            .chain(attached_wal_boundary)
            .min();
        let last_wal_id = wal_boundary.map_or(wal_status.last_flushed_wal_id, |boundary| {
            boundary.min(wal_status.last_flushed_wal_id)
        });
        let next_wal_id = last_wal_id
            .checked_add(1)
            .ok_or(SlateDBError::InvalidDBState)?;
        if next_wal_id < guard.state().core().next_wal_sst_id {
            return Err(SlateDBError::InvalidDBState);
        }
        guard.set_next_wal_id(next_wal_id);
        let manifest = guard.modify(|modifier| {
            let core = &mut modifier.state.manifest.value.core;
            core.recent_snapshot_min_seq = min_active_snapshot_seq.unwrap_or(core.last_l0_seq);
            modifier.state.manifest.clone()
        });
        self.report_to_status_manager(&guard, manifest.clone().into());
        Ok(manifest)
    }

    async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        let remote_dirty = self.manifest.prepare_dirty()?;
        self.merge_remote_manifest(remote_dirty);
        Ok(())
    }

    async fn refresh_manifest_progress(
        &mut self,
        done: Option<oneshot::Sender<Result<(), SlateDBError>>>,
    ) -> Result<(), SlateDBError> {
        let result = async {
            self.manifest.refresh().await?;
            let remote_dirty = self.manifest.prepare_dirty()?;
            self.merge_remote_manifest(remote_dirty);
            let _ = self.tracker_tx.send(TrackerMessage::ManifestRefreshed);
            Ok(())
        }
        .await;
        if let Some(tx) = done {
            let _ = tx.send(result.clone());
        }
        result
    }

    fn merge_remote_manifest(&self, remote_dirty: DirtyObject<Manifest>) {
        let manifest = {
            let mut wguard_state = self.db.state.write();
            wguard_state.merge_remote_manifest(remote_dirty);
            wguard_state.state().manifest.clone()
        };
        self.update_stats_for_manifest(&manifest);
        self.db.status_manager.report_manifest(manifest.into());
    }

    fn update_stats_for_manifest(&self, manifest: &DirtyObject<Manifest>) {
        let mut l0_ssts = 0usize;
        let mut segment_max_l0_ssts = 0usize;
        let mut sorted_runs = 0usize;
        let mut sst_views = 0usize;
        let mut distinct_ssts: HashSet<SsTableId> = HashSet::new();
        for tree in manifest.value.core.trees() {
            l0_ssts += tree.l0.len();
            // Track the largest single tree: backpressure is driven by `segment_max_l0_sst_count`
            // because `l0_max_ssts` is enforced per-tree.
            segment_max_l0_ssts = segment_max_l0_ssts.max(tree.l0.len());
            sorted_runs += tree.compacted.len();
            let all_views = tree
                .l0
                .iter()
                .chain(tree.compacted.iter().flat_map(|run| run.sst_views().iter()));
            for view in all_views {
                sst_views += 1;
                // Dedupe by physical SST id: a range clone/rescale can project one SST into
                // several views, so `sst_count <= sst_view_count`.
                distinct_ssts.insert(view.sst.id);
            }
        }
        self.db.db_stats.l0_sst_count.set(l0_ssts as i64);
        self.db
            .db_stats
            .segment_max_l0_sst_count
            .set(segment_max_l0_ssts as i64);
        self.db.db_stats.sorted_run_count.set(sorted_runs as i64);
        self.db.db_stats.sst_view_count.set(sst_views as i64);
        self.db.db_stats.sst_count.set(distinct_ssts.len() as i64);
        self.db
            .db_stats
            .external_db_count
            .set(manifest.value.external_dbs.len() as i64);
    }

    async fn write_checkpoints_safely(
        &mut self,
        checkpoints: &[&PendingCheckpoint],
    ) -> Result<Vec<CheckpointResult>, SlateDBError> {
        self.load_manifest().await?;
        self.write_manifest_update_safely(checkpoints).await
    }

    /// Writes the checkpoints that are ready with the current L0 state.
    async fn write_ready_checkpoints(&mut self) -> Result<(), SlateDBError> {
        loop {
            let l0_last_seq = self.db.state.read().state().core().last_l0_seq;
            let ready = self.take_ready_checkpoints(l0_last_seq)?;
            if ready.is_empty() {
                return Ok(());
            }

            let checkpoint_refs = ready.iter().collect::<Vec<_>>();
            match self.write_checkpoints_safely(&checkpoint_refs).await {
                Ok(results) => Self::complete_checkpoints(ready, results),
                Err(err) => {
                    for checkpoint in ready {
                        checkpoint.send_error(err.clone());
                    }
                    return Err(err);
                }
            }
        }
    }

    fn complete_checkpoints(
        checkpoints: Vec<PendingCheckpoint>,
        results: Vec<CheckpointResult>,
    ) {
        for (checkpoint, result) in checkpoints.into_iter().zip(results) {
            match result {
                Ok(result) => {
                    debug!("checkpoint created [id={}]", result.id);
                    checkpoint.lifecycle.complete(result);
                }
                Err(error) => checkpoint.send_error(error),
            }
        }
    }

    async fn finish_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        checkpoint_results: Vec<CheckpointResult>,
        through_seq: u64,
    ) -> Result<(), SlateDBError> {
        debug!(
            "l0 flush batch written to manifest [batch_size={}, through_seq={}]",
            staged_batch.len(),
            through_seq,
        );
        self.durable_seq = through_seq;
        for uploaded in &staged_batch {
            uploaded.imm_memtable.table().notify_durable(Ok(()));
            self.db.oracle.advance_durable_seq(uploaded.last_seq);
        }
        self.resolve_pending_flushes();
        Self::complete_checkpoints(attached_checkpoints, checkpoint_results);
        let _ = self
            .tracker_tx
            .send(TrackerMessage::FlushComplete { through_seq });
        Ok(())
    }

    fn resolve_pending_flushes(&mut self) {
        let flush_result = self.flush_result();
        let pending = std::mem::take(&mut self.pending_flushes);
        let mut still_pending = Vec::with_capacity(pending.len());
        for flush in pending {
            if self.is_durable(flush.through_seq) {
                let _ = flush.sender.send(Ok(flush_result.clone()));
            } else {
                still_pending.push(flush);
            }
        }
        self.pending_flushes = still_pending;
    }

    async fn fail_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        err: SlateDBError,
    ) -> Result<(), SlateDBError> {
        for uploaded in staged_batch {
            uploaded.imm_memtable.notify_uploaded(Err(err.clone()));
            uploaded
                .imm_memtable
                .table()
                .notify_durable(Err(err.clone()));
        }
        for checkpoint in attached_checkpoints {
            checkpoint.send_error(err.clone());
        }
        Ok(())
    }

    /// Fail remaining flush waiters on exit. Any waiter still pending at
    /// shutdown was never satisfied by a durable epoch advance, so it
    /// always receives an error.
    fn fail_pending_flushes(&mut self, err: &SlateDBError) {
        for flush in self.pending_flushes.drain(..) {
            let _ = flush.sender.send(Err(err.clone()));
        }
    }

    /// Fail remaining checkpoint waiters on exit. Any waiter still pending
    /// at shutdown was never satisfied, so it always receives an error.
    fn fail_pending_checkpoints(&mut self, err: &SlateDBError) {
        for checkpoint in self.pending_checkpoints.drain(..) {
            checkpoint.send_error(err.clone());
        }
    }

    /// Fail remaining manifest refresh waiters on exit. If a poll request was
    /// queued but never processed, callers still need the terminal DB error.
    fn fail_pending_manifest_refreshes(&mut self, err: &SlateDBError) {
        for sender in self.pending_manifest_refreshes.drain(..) {
            let _ = sender.send(Err(err.clone()));
        }
    }

    /// Extract waiters from a command without processing uploads. Used during
    /// error shutdown to ensure waiters get a proper error.
    fn collect_pending_waiter(&mut self, command: ManifestWriterCommand, error: &SlateDBError) {
        match command {
            ManifestWriterCommand::AwaitFlush {
                through_seq,
                sender,
            } => {
                self.pending_flushes.push(PendingFlush {
                    through_seq,
                    sender,
                });
            }
            ManifestWriterCommand::CreateCheckpoint { request, .. } => {
                request.lifecycle.fail(error.clone());
            }
            ManifestWriterCommand::PollManifest { done: Some(sender) } => {
                self.pending_manifest_refreshes.push(sender);
            }
            _ => {}
        }
    }

    async fn try_graceful_cleanup(
        &mut self,
        commands: &mut (impl futures::Stream<Item = ManifestWriterCommand> + Unpin),
        result: &Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        if result.is_ok() {
            while let Some(message) = commands.next().await {
                self.handle(message).await?;
            }
        }

        // Unfinished checkpoints no longer limit the final WAL endpoint.
        let unfinished_checkpoints = std::mem::take(&mut self.pending_checkpoints);
        let write_result = if matches!(result, Err(SlateDBError::Fenced)) {
            Ok(())
        } else {
            self.write_current_manifest_safely().await
        };
        let error = result
            .as_ref()
            .err()
            .or_else(|| write_result.as_ref().err())
            .cloned()
            .unwrap_or(SlateDBError::Closed);
        for checkpoint in unfinished_checkpoints {
            checkpoint.send_error(error.clone());
        }
        write_result
    }
}

struct PendingFlush {
    through_seq: Option<u64>,
    sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
}

struct PendingCheckpoint {
    id: Uuid,
    /// The last write that the checkpoint includes.
    bound: CheckpointCursor,
    /// If set, the checkpoint waits until L0 holds every write through this sequence number.
    l0_through_seq: Option<u64>,
    options: CheckpointOptions,
    lifecycle: CheckpointLifecycle,
}

impl PendingCheckpoint {
    /// Returns true if the manifest writer can write this checkpoint.
    ///
    /// `l0_last_seq` is the last sequence number in L0. L0 holds every write through it.
    fn is_ready(&self, last_flushed_wal_id: u64, l0_last_seq: u64) -> bool {
        // A checkpoint from a source refers to the manifest of that source.
        if self.options.source.is_some() {
            return true;
        }
        let wal_durable = self
            .bound
            .wal_file
            .is_none_or(|wal_file| wal_file <= last_flushed_wal_id);
        let l0_covered = self.l0_through_seq.is_none_or(|seq| seq <= l0_last_seq);
        wal_durable && l0_covered
    }

    fn matches_or_sets_wal_boundary(&self, boundary: &mut Option<Option<u64>>) -> bool {
        if self.options.source.is_some() {
            return true;
        }
        match boundary {
            Some(boundary) => *boundary == self.bound.wal_file,
            None => {
                *boundary = Some(self.bound.wal_file);
                true
            }
        }
    }

    fn send_error(self, err: SlateDBError) {
        self.lifecycle.fail(err);
    }
}

/// Adapts a [`DbStatus`](crate::db_status::DbStatus) watch into a [Notifier]
/// that produces [ManifestWriterCommand::DurableSeqAdvanced] whenever the
/// database status changes (which includes WAL durable sequence advances).
struct DurableSeqNotifier {
    rx: watch::Receiver<crate::db_status::DbStatus>,
}

#[async_trait]
impl crate::dispatcher::Notifier<ManifestWriterCommand> for DurableSeqNotifier {
    async fn notify(&mut self) -> ManifestWriterCommand {
        // changed() returns Err only when the sender is dropped. In that case
        // the database is shutting down and the dispatcher's cancellation token
        // will break the select loop, so we can just block forever.
        if self.rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        ManifestWriterCommand::DurableSeqAdvanced
    }
}

/// Polls the manifest while the flush tracker is stalled on L0, so a slot
/// freed by compaction is seen before the next `manifest_poll_interval` tick.
///
/// `rx` carries 0 while dispatch is not stalled and an id that is distinct
/// for every stall otherwise. A new id polls at once and restarts the
/// backoff; while the same id persists, each poll waits twice as long as
/// the one before, up to `max_backoff`. Nothing runs while `rx` is 0.
struct L0StallPollNotifier {
    rx: watch::Receiver<u64>,
    clock: Arc<dyn SystemClock>,
    max_backoff: Duration,
    /// The stall being polled and the delay before its next poll.
    /// `None` while not stalled.
    stall: Option<(u64, Duration)>,
}

impl L0StallPollNotifier {
    fn new(
        rx: watch::Receiver<u64>,
        clock: Arc<dyn SystemClock>,
        manifest_poll_interval: Duration,
    ) -> Self {
        Self {
            rx,
            clock,
            max_backoff: MAX_L0_STALL_POLL_BACKOFF.min(manifest_poll_interval),
            stall: None,
        }
    }
}

#[async_trait]
impl crate::dispatcher::Notifier<ManifestWriterCommand> for L0StallPollNotifier {
    async fn notify(&mut self) -> ManifestWriterCommand {
        loop {
            let stall_id = *self.rx.borrow_and_update();
            if stall_id == 0 {
                self.stall = None;
                // As in DurableSeqNotifier, Err means the sender is gone and
                // the database is closing.
                if self.rx.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
                continue;
            }
            let delay = match self.stall {
                Some((id, delay)) if id == stall_id => delay,
                _ => {
                    let first = MIN_L0_STALL_POLL_BACKOFF.min(self.max_backoff);
                    self.stall = Some((stall_id, first));
                    return ManifestWriterCommand::PollManifest { done: None };
                }
            };
            tokio::select! {
                _ = self.clock.sleep(delay) => {
                    self.stall = Some((stall_id, (delay * 2).min(self.max_backoff)));
                    return ManifestWriterCommand::PollManifest { done: None };
                }
                changed = self.rx.changed() => {
                    if changed.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        L0StallPollNotifier, ManifestWriter, ManifestWriterCommand, ManifestWriterHandler,
        TrackerMessage,
    };
    use crate::block_cache_policy::BlockCachePolicy;
    use crate::checkpoint::{
        CheckpointHandle, CheckpointLifecycle, CheckpointRequest, CheckpointResult,
    };
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_status::{ClosedResultWriter, DbStatusManager};
    use crate::error::SlateDBError;
    use crate::flush::SegmentedSstHandle;
    use crate::format::sst::SsTableFormat;
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::manifest::{ManifestCore, VersionedManifest};
    use crate::memtable_flusher::uploader::UploadedMemtable;
    use crate::memtable_flusher::CheckpointCursor;
    use crate::paths::PathResolver;
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::types::RowEntry;
    use crate::utils::WatchableOnceCell;

    use crate::dispatcher::Notifier;
    use crate::wal::test_utils::FakeWalWriter;
    use crate::wal::{WalError, WalObserver, WalStatus, WalStatusListener, WalWriter};
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::{DefaultSystemClock, MockSystemClock, SystemClock};
    use slatedb_common::metrics::MetricsRecorderHelper;
    use slatedb_common::DbRand;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::sync::{oneshot, watch};
    use tokio::time::timeout;
    use uuid::Uuid;

    struct StartedManifestWriter {
        writer: ManifestWriter,
        executor: crate::dispatcher::MessageHandlerExecutor,
        tracker_rx: async_channel::Receiver<TrackerMessage>,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
        _l0_stall_tx: watch::Sender<u64>,
    }

    impl StartedManifestWriter {
        async fn shutdown(&self) {
            ManifestWriter::shutdown(&self.executor).await;
        }

        /// Wait for the executor to report a closed result (error or clean shutdown).
        async fn await_closed(&self) -> Result<(), SlateDBError> {
            self.closed_result.reader().await_value().await
        }
    }

    impl std::ops::Deref for StartedManifestWriter {
        type Target = ManifestWriter;
        fn deref(&self) -> &Self::Target {
            &self.writer
        }
    }

    struct TestCheckpointRequest {
        id: Uuid,
        ready_rx: oneshot::Receiver<Result<(), SlateDBError>>,
        result_rx: oneshot::Receiver<CheckpointResult>,
    }

    fn cursor(seq: u64, wal_file: u64) -> CheckpointCursor {
        CheckpointCursor {
            seq,
            wal_file: Some(wal_file),
        }
    }

    fn new_test_checkpoint(
        floor: impl Into<Option<CheckpointCursor>>,
        wait_for_l0: bool,
    ) -> (CheckpointRequest, TestCheckpointRequest) {
        let id = Uuid::new_v4();
        let (lifecycle, result_rx, ready_rx) = CheckpointLifecycle::new();
        (
            CheckpointRequest {
                id,
                floor: floor.into(),
                wait_for_l0,
                lifecycle,
            },
            TestCheckpointRequest {
                id,
                ready_rx,
                result_rx,
            },
        )
    }

    fn begin_test_checkpoint(
        writer: &ManifestWriter,
        floor: impl Into<Option<CheckpointCursor>>,
        wait_for_l0: bool,
    ) -> TestCheckpointRequest {
        let (request, receivers) = new_test_checkpoint(floor, wait_for_l0);
        writer
            .begin_checkpoint(CheckpointOptions::default(), request)
            .unwrap();
        receivers
    }

    /// Waits until the manifest writer handles every command sent before this call.
    async fn sync_manifest_writer(writer: &ManifestWriter) {
        let (tx, rx) = oneshot::channel();
        writer.send_poll(tx).unwrap();
        timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn start_manifest_writer(
        inner: Arc<DbInner>,
        manifest: FenceableManifest,
        poll_interval: Duration,
    ) -> StartedManifestWriter {
        let closed_result = WatchableOnceCell::new();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let executor = crate::dispatcher::MessageHandlerExecutor::new(
            Arc::new(closed_result.clone()),
            system_clock,
        );
        let (tracker_tx, tracker_rx) =
            crate::utils::SafeSender::unbounded_channel(closed_result.result_reader());
        let (l0_stall_tx, l0_stall_rx) = watch::channel(0);
        let writer = ManifestWriter::start(
            inner,
            manifest,
            poll_interval,
            &closed_result,
            &executor,
            &Handle::current(),
            tracker_tx,
            l0_stall_rx,
        )
        .unwrap();
        executor.monitor_on(&Handle::current()).unwrap();
        StartedManifestWriter {
            writer,
            executor,
            tracker_rx,
            closed_result,
            _l0_stall_tx: l0_stall_tx,
        }
    }

    fn stall_notifier(
        clock: Arc<MockSystemClock>,
        manifest_poll_interval: Duration,
    ) -> (watch::Sender<u64>, L0StallPollNotifier) {
        let (tx, rx) = watch::channel(0);
        (
            tx,
            L0StallPollNotifier::new(rx, clock, manifest_poll_interval),
        )
    }

    /// Drives one `notify` call on the mock clock: it must stay pending until
    /// the clock has advanced by `expected`, then produce a `PollManifest`
    /// with no reply channel. A zero `expected` demands a poll at once.
    async fn assert_poll_after(
        notifier: &mut L0StallPollNotifier,
        clock: &MockSystemClock,
        expected: Duration,
    ) {
        let notify = notifier.notify();
        tokio::pin!(notify);
        if !expected.is_zero() {
            assert!(timeout(Duration::from_millis(20), &mut notify)
                .await
                .is_err());
            clock.advance(expected - Duration::from_millis(1)).await;
            assert!(
                timeout(Duration::from_millis(20), &mut notify)
                    .await
                    .is_err(),
                "polled before {expected:?} elapsed"
            );
            clock.advance(Duration::from_millis(1)).await;
        }
        let command = timeout(Duration::from_secs(5), &mut notify)
            .await
            .unwrap_or_else(|_| panic!("no poll after {expected:?}"));
        assert!(matches!(
            command,
            ManifestWriterCommand::PollManifest { done: None }
        ));
    }

    #[tokio::test]
    async fn l0_stall_notifier_is_idle_while_not_stalled() {
        let clock = Arc::new(MockSystemClock::new());
        let (_tx, mut notifier) = stall_notifier(Arc::clone(&clock), Duration::from_secs(60));
        let notify = notifier.notify();
        tokio::pin!(notify);
        assert!(timeout(Duration::from_millis(50), &mut notify)
            .await
            .is_err());
        clock.advance(Duration::from_secs(120)).await;
        assert!(timeout(Duration::from_millis(50), &mut notify)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn l0_stall_notifier_polls_at_once_then_backs_off_to_the_cap() {
        let clock = Arc::new(MockSystemClock::new());
        let (tx, mut notifier) = stall_notifier(Arc::clone(&clock), Duration::from_secs(60));
        tx.send(1).unwrap();
        assert_poll_after(&mut notifier, &clock, Duration::ZERO).await;
        for millis in [50, 100, 200, 400, 800, 1000, 1000] {
            assert_poll_after(&mut notifier, &clock, Duration::from_millis(millis)).await;
        }
    }

    #[tokio::test]
    async fn l0_stall_notifier_backoff_is_capped_at_manifest_poll_interval() {
        let clock = Arc::new(MockSystemClock::new());
        let (tx, mut notifier) = stall_notifier(Arc::clone(&clock), Duration::from_millis(20));
        tx.send(1).unwrap();
        assert_poll_after(&mut notifier, &clock, Duration::ZERO).await;
        for _ in 0..3 {
            assert_poll_after(&mut notifier, &clock, Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn l0_stall_notifier_goes_idle_when_the_stall_clears_and_restarts_backoff() {
        let clock = Arc::new(MockSystemClock::new());
        let (tx, mut notifier) = stall_notifier(Arc::clone(&clock), Duration::from_secs(60));
        tx.send(1).unwrap();
        assert_poll_after(&mut notifier, &clock, Duration::ZERO).await;
        assert_poll_after(&mut notifier, &clock, Duration::from_millis(50)).await;
        assert_poll_after(&mut notifier, &clock, Duration::from_millis(100)).await;

        {
            // The stall clears while the 200 ms sleep is in flight: the sleep
            // is abandoned and no poll follows, however far the clock moves.
            let notify = notifier.notify();
            tokio::pin!(notify);
            assert!(timeout(Duration::from_millis(20), &mut notify)
                .await
                .is_err());
            tx.send(0).unwrap();
            assert!(timeout(Duration::from_millis(20), &mut notify)
                .await
                .is_err());
            clock.advance(Duration::from_secs(10)).await;
            assert!(timeout(Duration::from_millis(50), &mut notify)
                .await
                .is_err());

            // A new stall polls at once.
            tx.send(2).unwrap();
            let command = timeout(Duration::from_secs(5), &mut notify).await.unwrap();
            assert!(matches!(
                command,
                ManifestWriterCommand::PollManifest { done: None }
            ));
        }
        // And the backoff starts over.
        assert_poll_after(&mut notifier, &clock, Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn l0_stall_notifier_restarts_backoff_for_a_stall_it_never_saw_clear() {
        let clock = Arc::new(MockSystemClock::new());
        let (tx, mut notifier) = stall_notifier(Arc::clone(&clock), Duration::from_secs(60));
        tx.send(1).unwrap();
        assert_poll_after(&mut notifier, &clock, Duration::ZERO).await;
        assert_poll_after(&mut notifier, &clock, Duration::from_millis(50)).await;
        assert_poll_after(&mut notifier, &clock, Duration::from_millis(100)).await;

        // Between two notify calls the stall clears and a new one starts; the
        // notifier only ever reads the new id.
        tx.send(0).unwrap();
        tx.send(2).unwrap();
        assert_poll_after(&mut notifier, &clock, Duration::ZERO).await;
        assert_poll_after(&mut notifier, &clock, Duration::from_millis(50)).await;
    }

    async fn assert_no_flush_event(
        tracker_rx: &async_channel::Receiver<TrackerMessage>,
        duration: Duration,
    ) {
        let deadline = tokio::time::Instant::now() + duration;
        loop {
            match timeout(deadline - tokio::time::Instant::now(), tracker_rx.recv()).await {
                Err(_) => return, // timed out — no flush event, as expected
                Ok(Ok(TrackerMessage::ManifestRefreshed)) => continue,
                Ok(Ok(TrackerMessage::FlushComplete { .. })) => {
                    panic!("unexpected flushed event")
                }
                Ok(Err(_)) => panic!("tracker channel closed"),
                Ok(Ok(_)) => continue,
            }
        }
    }

    async fn expect_flushed(tracker_rx: &async_channel::Receiver<TrackerMessage>) -> u64 {
        loop {
            let msg = timeout(Duration::from_secs(5), tracker_rx.recv())
                .await
                .expect("timed out waiting for flushed event")
                .expect("tracker channel closed");
            match msg {
                TrackerMessage::FlushComplete { through_seq } => return through_seq,
                _ => continue,
            }
        }
    }

    struct TestHarness {
        inner: Arc<DbInner>,
        manifest: FenceableManifest,
        object_store: Arc<dyn ObjectStore>,
        path: String,
    }

    async fn setup_harness(path: &str, fp_registry: Arc<FailPointRegistry>) -> TestHarness {
        setup_harness_with_extractor(path, fp_registry, None).await
    }

    fn new_handler_from_harness(harness: TestHarness) -> ManifestWriterHandler {
        let closed_result = WatchableOnceCell::new();
        let (tracker_tx, _) =
            crate::utils::SafeSender::unbounded_channel(closed_result.result_reader());
        ManifestWriterHandler::new(
            harness.inner,
            harness.manifest,
            Duration::from_secs(3600),
            tracker_tx,
            watch::channel(0).1,
        )
    }

    async fn setup_harness_with_extractor(
        path: &str,
        fp_registry: Arc<FailPointRegistry>,
        segment_extractor: Option<Arc<dyn crate::prefix_extractor::PrefixExtractor>>,
    ) -> TestHarness {
        setup_harness_with_wal_observer(
            path,
            fp_registry,
            segment_extractor,
            FakeWalWriter::new(0).observer(),
        )
        .await
    }

    async fn setup_harness_with_wal_observer(
        path: &str,
        fp_registry: Arc<FailPointRegistry>,
        segment_extractor: Option<Arc<dyn crate::prefix_extractor::PrefixExtractor>>,
        wal_observer: Box<dyn WalObserver>,
    ) -> TestHarness {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        setup_harness_with_dependencies(
            path,
            fp_registry,
            segment_extractor,
            object_store,
            wal_observer,
        )
        .await
    }

    async fn setup_harness_with_dependencies(
        path: &str,
        fp_registry: Arc<FailPointRegistry>,
        segment_extractor: Option<Arc<dyn crate::prefix_extractor::PrefixExtractor>>,
        object_store: Arc<dyn ObjectStore>,
        wal_observer: Box<dyn WalObserver>,
    ) -> TestHarness {
        let path = path.to_string();
        let settings = Settings::default();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand = Arc::new(DbRand::new(42));
        let db_metrics = MetricsRecorderHelper::noop();
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(path.clone()),
            Arc::clone(&object_store),
        ));
        let stored_manifest = StoredManifest::create_new_db(
            Arc::clone(&manifest_store),
            ManifestCore::new_with_wal_object_store(None),
            Arc::clone(&system_clock),
        )
        .await
        .unwrap();
        let manifest_dirty = stored_manifest.prepare_dirty().unwrap();
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            Arc::clone(&object_store),
            SsTableFormat::default(),
            PathResolver::from_root(Path::from(path.clone())),
            Arc::clone(&fp_registry),
            None,
            TableStoreKind::Main,
            BlockCachePolicy::default(),
        ));
        let status_manager = DbStatusManager::new(0);
        let (write_tx, _) =
            crate::utils::SafeSender::unbounded_channel(status_manager.result_reader());
        let inner = Arc::new(
            DbInner::new(
                settings.clone(),
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                manifest_dirty,
                Arc::new(crate::memtable_flusher::MemtableFlusher::new(
                    &WatchableOnceCell::new(),
                )),
                write_tx,
                wal_observer,
                db_metrics,
                fp_registry,
                None,
                Arc::new(status_manager),
                segment_extractor,
            )
            .await
            .unwrap(),
        );
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(path.clone()),
            Arc::clone(&object_store),
        ));
        let stored_manifest =
            StoredManifest::load(manifest_store, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let manifest = FenceableManifest::init_writer(
            stored_manifest,
            Duration::from_secs(300),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        TestHarness {
            inner,
            manifest,
            object_store,
            path,
        }
    }

    async fn load_writer_manifest(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> FenceableManifest {
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store));
        let stored_manifest =
            StoredManifest::load(manifest_store, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        FenceableManifest::init_writer(
            stored_manifest,
            Duration::from_secs(300),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap()
    }

    async fn latest_manifest_checkpoint_count(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> usize {
        let manifest_store = ManifestStore::new(&Path::from(path), object_store);
        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        manifest.manifest.core.checkpoints.len()
    }

    async fn stage_manifest_boundary_conflict(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> u64 {
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store));
        let mut external =
            StoredManifest::load(manifest_store.clone(), Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let start_id = external.id();

        // The external writer wins the stale handler's intended id.
        external
            .update(external.prepare_dirty().unwrap())
            .await
            .unwrap();
        assert_eq!(external.id(), start_id + 1);

        // A second external write leaves a live latest version above the future boundary,
        // so the stale handler can make progress after refreshing.
        external
            .update(external.prepare_dirty().unwrap())
            .await
            .unwrap();
        assert_eq!(external.id(), start_id + 2);

        // GC fences and removes the stale handler's next id, but preserves the live latest
        // version at start_id + 2.
        manifest_store.advance_boundary(start_id + 1).await.unwrap();
        manifest_store.delete_manifest(start_id + 1).await.unwrap();

        let latest = manifest_store.read_latest_manifest().await.unwrap().id;
        assert_eq!(latest, start_id + 2);
        start_id
    }

    fn freeze_imm(
        inner: &Arc<DbInner>,
        key: &[u8],
        value: &[u8],
    ) -> Arc<crate::mem_table::ImmutableMemtable> {
        let seq = inner.oracle.next_seq();
        let mut guard = inner.state.write();
        guard.memtable().put(RowEntry::new_value(key, value, seq));
        guard.freeze_memtable(0);
        guard.state().imm_memtable.front().cloned().unwrap()
    }

    /// Build an uploaded memtable without advancing the WAL durable sequence.
    async fn next_uploaded_memtable_no_wal(
        inner: &Arc<DbInner>,
        key: &[u8],
        value: &[u8],
    ) -> UploadedMemtable {
        let imm_memtable = freeze_imm(inner, key, value);
        let handles = inner.flush_l0_for_test(imm_memtable.table()).await.unwrap();
        let sst_handle = handles.into_iter().next().expect("expected single SST");
        let first_seq = imm_memtable.table().first_seq().unwrap();
        let last_seq = imm_memtable.table().last_seq().unwrap();
        UploadedMemtable::new(imm_memtable, sst_handle, first_seq, last_seq)
    }

    /// Build an uploaded memtable and simulate WAL flush completing.
    async fn next_uploaded_memtable(
        inner: &Arc<DbInner>,
        key: &[u8],
        value: &[u8],
    ) -> UploadedMemtable {
        let uploaded = next_uploaded_memtable_no_wal(inner, key, value).await;
        inner.oracle.advance_durable_seq(uploaded.last_seq);
        uploaded
    }

    #[rstest::rstest]
    #[case(None, None, 30)]
    #[case(Some(10), None, 10)]
    #[case(None, Some(20), 20)]
    #[case(Some(10), Some(20), 10)]
    #[case(Some(20), Some(10), 10)]
    #[tokio::test]
    async fn manifest_writes_refresh_snapshot_min_seq_without_uploaded_tables(
        #[case] snapshot_seq: Option<u64>,
        #[case] txn_seq: Option<u64>,
        #[case] expected_min_seq: u64,
    ) {
        let harness = setup_harness(
            "/tmp/test_manifest_writes_refresh_snapshot_min_seq",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let manifest_store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            Arc::clone(&harness.object_store),
        );
        let mut handler = new_handler_from_harness(harness);
        handler.load_manifest().await.unwrap();
        let snapshot =
            snapshot_seq.map(|seq| handler.db.snapshot_manager.new_snapshot(Some(seq)).0);
        let txn = txn_seq.map(|seq| {
            handler.db.oracle.advance_committed_seq(seq);
            handler.db.txn_manager.new_transaction().0
        });
        handler.db.oracle.advance_committed_seq(30);
        handler.db.state.write().modify(|modifier| {
            modifier.state.manifest.value.core.last_l0_seq = 30;
        });

        handler.write_current_manifest_safely().await.unwrap();
        assert_eq!(
            handler
                .db
                .state
                .read()
                .state()
                .core()
                .recent_snapshot_min_seq,
            expected_min_seq
        );
        assert_eq!(
            manifest_store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .recent_snapshot_min_seq,
            expected_min_seq
        );

        if let Some(snapshot) = snapshot {
            handler.db.snapshot_manager.drop_snapshot(&snapshot);
        }
        if let Some(txn) = txn {
            handler.db.txn_manager.drop_txn(&txn);
        }
        handler
            .write_manifest_update_safely(&[&durable_pending_checkpoint()])
            .await
            .unwrap();
        assert_eq!(
            handler
                .db
                .state
                .read()
                .state()
                .core()
                .recent_snapshot_min_seq,
            30
        );
        assert_eq!(
            manifest_store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .recent_snapshot_min_seq,
            30
        );
    }

    fn durable_pending_checkpoint() -> super::PendingCheckpoint {
        super::PendingCheckpoint {
            id: Uuid::new_v4(),
            bound: CheckpointCursor {
                seq: 0,
                wal_file: None,
            },
            l0_through_seq: None,
            options: CheckpointOptions::default(),
            lifecycle: CheckpointLifecycle::new().0,
        }
    }

    struct AdvancingWalObserver {
        next_flushed_wal_id: Arc<AtomicU64>,
    }

    impl WalObserver for AdvancingWalObserver {
        fn status(&self) -> Result<WalStatus, WalStatus> {
            Ok(WalStatus {
                last_flushed_wal_id: self.next_flushed_wal_id.fetch_add(1, Ordering::SeqCst),
                last_flushed_seq: None,
                estimated_bytes: 0,
                buffered_wal_entries_count: 0,
                closed_reason: None,
            })
        }

        fn subscribe(&self, _listener: WalStatusListener) -> Result<(), WalError> {
            Ok(())
        }
    }

    /// Reports a WAL status that a test can advance. WAL file `n` holds the write with
    /// sequence number `n`.
    struct MutableWalObserver {
        last_flushed_wal_id: Arc<AtomicU64>,
    }

    impl WalObserver for MutableWalObserver {
        fn status(&self) -> Result<WalStatus, WalStatus> {
            let last_flushed_wal_id = self.last_flushed_wal_id.load(Ordering::SeqCst);
            let mut status = FakeWalWriter::new(last_flushed_wal_id).status()?;
            status.last_flushed_seq = Some(last_flushed_wal_id);
            Ok(status)
        }

        fn subscribe(&self, _listener: WalStatusListener) -> Result<(), WalError> {
            Ok(())
        }
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn manifest_write_retry_samples_new_wal_end(#[case] checkpoint: bool) {
        let next_flushed_wal_id = Arc::new(AtomicU64::new(0));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_manifest_write_retry_samples_new_wal_end",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(AdvancingWalObserver {
                next_flushed_wal_id: Arc::clone(&next_flushed_wal_id),
            }),
        )
        .await;
        let manifest_store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            Arc::clone(&harness.object_store),
        );
        let mut handler = new_handler_from_harness(harness);
        handler.load_manifest().await.unwrap();
        handler
            .manifest
            .update(handler.manifest.prepare_dirty().unwrap())
            .await
            .unwrap();
        next_flushed_wal_id.store(10, Ordering::SeqCst);

        if checkpoint {
            handler
                .write_manifest_update_safely(&[&durable_pending_checkpoint()])
                .await
                .unwrap();
        } else {
            handler.write_current_manifest_safely().await.unwrap();
        }

        assert_eq!(next_flushed_wal_id.load(Ordering::SeqCst), 12);
        assert_eq!(handler.db.state.read().state().core().next_wal_sst_id, 12);
        assert_eq!(
            manifest_store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .next_wal_sst_id,
            12
        );
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn replay_leaves_wal_end_to_manifest_writer(#[case] sample_before_replay: bool) {
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_replay_leaves_wal_end_to_manifest_writer",
            Arc::new(FailPointRegistry::new()),
            None,
            FakeWalWriter::new(10).observer(),
        )
        .await;
        let handler = new_handler_from_harness(harness);
        if sample_before_replay {
            handler.prepare_local_manifest_for_write(None).unwrap();
        }
        let next_wal_id_before_replay = handler.db.state.read().state().core().next_wal_sst_id;
        let table = crate::mem_table::WritableKVTable::new();
        table.put(RowEntry::new_value(b"key", b"value", 1));
        handler
            .db
            .replay_memtable(
                0,
                crate::wal_replay::ReplayedMemtable {
                    table,
                    last_tick: 0,
                    last_seq: 1,
                    last_wal_id: 3,
                },
            )
            .unwrap();

        assert_eq!(
            handler.db.state.read().state().core().next_wal_sst_id,
            next_wal_id_before_replay
        );
        assert_eq!(
            handler
                .prepare_local_manifest_for_write(None)
                .unwrap()
                .value
                .core
                .next_wal_sst_id,
            11
        );
        assert_eq!(handler.db.state.read().state().core().next_wal_sst_id, 11);
    }

    #[tokio::test]
    async fn prepare_local_manifest_for_write_reports_to_status_manager() {
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_prepare_local_manifest_for_write_reports_to_status_manager",
            Arc::new(FailPointRegistry::new()),
            None,
            FakeWalWriter::new(10).observer(),
        )
        .await;
        let handler = new_handler_from_harness(harness);
        let mut status_rx = handler.db.status_manager.subscribe();

        let prepared = handler.prepare_local_manifest_for_write(None).unwrap();

        // Subscribers see the prepared manifest before the manifest writer writes it.
        assert!(status_rx.has_changed().unwrap());
        let current_manifest = status_rx.borrow_and_update().current_manifest.clone();
        assert_eq!(current_manifest.core().next_wal_sst_id, 11);
        assert_eq!(current_manifest, VersionedManifest::from(prepared));
    }

    #[tokio::test]
    async fn write_current_manifest_safely_retries_on_boundary_conflict() {
        // Build a manifest writer handler backed by an in-memory manifest store.
        let harness = setup_harness(
            "/tmp/test_manifest_writer_current_boundary_conflict",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(path.clone()),
            Arc::clone(&object_store),
        ));
        let mut handler = new_handler_from_harness(harness);
        handler.load_manifest().await.unwrap();

        // The handler is stale while an external writer creates a live newer version, then
        // GC deletes and fences the handler's next id.
        let start_id = stage_manifest_boundary_conflict(&path, Arc::clone(&object_store)).await;

        // The safe write path should reload the live newer manifest and retry safely.
        handler.write_current_manifest_safely().await.unwrap();

        let final_id = manifest_store.read_latest_manifest().await.unwrap().id;
        assert_eq!(start_id + 3, final_id);
    }

    #[tokio::test]
    async fn write_manifest_update_safely_retries_on_boundary_conflict() {
        // Build a manifest writer handler backed by an in-memory manifest store.
        let harness = setup_harness(
            "/tmp/test_manifest_writer_update_boundary_conflict",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let manifest_store = Arc::new(ManifestStore::new(
            &Path::from(path.clone()),
            Arc::clone(&object_store),
        ));
        let mut handler = new_handler_from_harness(harness);
        handler.load_manifest().await.unwrap();

        // The handler is stale while an external writer creates a live newer version, then
        // GC deletes and fences the handler's next id.
        let start_id = stage_manifest_boundary_conflict(&path, Arc::clone(&object_store)).await;

        // The safe update path should reload the live newer manifest and retry safely.
        let checkpoints = handler.write_manifest_update_safely(&[]).await.unwrap();

        let final_id = manifest_store.read_latest_manifest().await.unwrap().id;
        assert!(checkpoints.is_empty());
        assert_eq!(start_id + 3, final_id);
    }

    #[tokio::test]
    async fn should_emit_flushed_event_for_contiguous_uploads() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_flush_event",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded = next_uploaded_memtable(&inner, b"k1", b"v1").await;
        started.notify_uploaded(uploaded).await.unwrap();

        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, 1);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_flush_after_skipped_seq() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_skipped_seq",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        let uploaded1 = next_uploaded_memtable(&inner, b"k1", b"v1").await;
        started.notify_uploaded(uploaded1).await.unwrap();
        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, 1);

        let skipped_seq = inner.oracle.next_seq();
        assert_eq!(skipped_seq, 2);

        let uploaded2 = next_uploaded_memtable(&inner, b"k3", b"v3").await;
        assert_eq!(uploaded2.first_seq, 3);
        started.notify_uploaded(uploaded2).await.unwrap();
        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, 3);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_wait_for_older_imm_before_flushing() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_oldest_imm",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded1 = next_uploaded_memtable(&inner, b"k1", b"v1").await;
        let uploaded2 = next_uploaded_memtable(&inner, b"k2", b"v2").await;
        started.notify_uploaded(uploaded2).await.unwrap();
        assert_no_flush_event(&started.tracker_rx, Duration::from_millis(100)).await;

        started.notify_uploaded(uploaded1).await.unwrap();
        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, 2);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn checkpoint_stops_manifest_at_its_sequence_boundary() {
        let wal_end = Arc::new(AtomicU64::new(0));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoint_manifest_sequence_boundary",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded1 = next_uploaded_memtable(&inner, b"k1", b"v1").await;
        let uploaded2 = next_uploaded_memtable(&inner, b"k2", b"v2").await;

        // The WAL file of the floor is not durable yet, so the bound is the floor.
        let request = begin_test_checkpoint(&started, cursor(1, 1), true);
        request.ready_rx.await.unwrap().unwrap();
        started.notify_uploaded(uploaded2).await.unwrap();
        assert_no_flush_event(&started.tracker_rx, Duration::from_millis(100)).await;

        wal_end.store(1, Ordering::SeqCst);
        started.notify_uploaded(uploaded1).await.unwrap();
        assert_eq!(expect_flushed(&started.tracker_rx).await, 1);
        let checkpoint = request.result_rx.await.unwrap().unwrap();
        let checkpoint_manifest = ManifestStore::new(&Path::from(path), object_store)
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();
        assert_eq!(checkpoint_manifest.core.last_l0_seq, 1);
        assert_eq!(checkpoint_manifest.core.next_wal_sst_id, 2);
        assert_eq!(expect_flushed(&started.tracker_rx).await, 2);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn checkpoint_includes_replay_points_only_through_its_sequence_boundary() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoint_pending_replay_points",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let manifest_store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            Arc::clone(&harness.object_store),
        );
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let mut uploads = Vec::new();
        for wal_id in 1..=2 {
            let seq = inner.oracle.next_seq();
            let imm = {
                let mut guard = inner.state.write();
                guard
                    .memtable()
                    .put(RowEntry::new_value(b"key", b"value", seq));
                guard.freeze_memtable(wal_id);
                guard.state().imm_memtable.front().cloned().unwrap()
            };
            let sst = inner
                .flush_l0_for_test(imm.table())
                .await
                .unwrap()
                .pop()
                .unwrap();
            uploads.push(UploadedMemtable::new(imm, sst, seq, seq));
        }
        inner.oracle.advance_durable_seq(2);

        let request = begin_test_checkpoint(&started, cursor(1, 1), true);
        request.ready_rx.await.unwrap().unwrap();
        // The WAL can advance while this checkpoint waits for table uploads.
        wal_end.store(2, Ordering::SeqCst);
        started
            .notify_uploaded(uploads.pop().unwrap())
            .await
            .unwrap();
        started
            .notify_uploaded(uploads.pop().unwrap())
            .await
            .unwrap();
        let checkpoint = request.result_rx.await.unwrap().unwrap();
        let manifest = manifest_store
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();
        assert_eq!(manifest.core.last_l0_seq, 1);
        assert_eq!(manifest.core.replay_after_wal_id, 1);
        assert_eq!(manifest.core.next_wal_sst_id, 2);
        assert_eq!(expect_flushed(&started.tracker_rx).await, 1);
        assert_eq!(expect_flushed(&started.tracker_rx).await, 2);
        started.shutdown().await;
    }

    #[tokio::test]
    async fn durable_checkpoint_keeps_wal_end_from_request() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_durable_checkpoint_wal_end_from_request",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            harness.object_store.clone(),
        );
        let mut handler = new_handler_from_harness(harness);
        let (request, receivers) = new_test_checkpoint(None, false);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), request)
            .unwrap();
        receivers.ready_rx.await.unwrap().unwrap();

        // A WAL file that becomes durable after the request is not in the checkpoint.
        wal_end.store(2, Ordering::SeqCst);
        handler.process_ready_work().await.unwrap();
        let result = receivers.result_rx.await.unwrap().unwrap();
        let manifest = store.read_manifest(result.manifest_id).await.unwrap();
        assert_eq!(manifest.core.next_wal_sst_id, 2);
    }

    #[tokio::test]
    async fn checkpoint_stops_wal_at_its_begin_boundary() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoint_manifest_wal_boundary",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded = next_uploaded_memtable(&inner, b"before", b"v1").await;
        let request = begin_test_checkpoint(&started, cursor(uploaded.last_seq, 1), true);
        request.ready_rx.await.unwrap().unwrap();

        wal_end.store(2, Ordering::SeqCst);
        started.notify_uploaded(uploaded).await.unwrap();
        assert_eq!(expect_flushed(&started.tracker_rx).await, 1);

        let result = request.result_rx.await.unwrap().unwrap();
        let manifest = ManifestStore::new(&Path::from(path), object_store)
            .read_manifest(result.manifest_id)
            .await
            .unwrap();
        assert_eq!(manifest.core.next_wal_sst_id, 2);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_releases_failed_checkpoint_wal_boundary() {
        let wal_end = Arc::new(AtomicU64::new(0));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_shutdown_releases_checkpoint_wal_boundary",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            harness.object_store.clone(),
        );
        let mut handler = new_handler_from_harness(harness);
        let (request, receivers) = new_test_checkpoint(cursor(1, 1), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), request)
            .unwrap();
        receivers.ready_rx.await.unwrap().unwrap();
        wal_end.store(2, Ordering::SeqCst);
        handler.write_current_manifest_safely().await.unwrap();
        assert_eq!(
            store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .next_wal_sst_id,
            2
        );
        crate::dispatcher::MessageHandler::cleanup(
            &mut handler,
            Box::pin(futures::stream::empty()),
            Ok(()),
        )
        .await
        .unwrap();
        assert!(matches!(
            receivers.result_rx.await.unwrap(),
            Err(SlateDBError::Closed)
        ));
        let latest = store.read_latest_manifest().await.unwrap();
        assert_eq!(latest.manifest.core.next_wal_sst_id, 3);
        assert!(latest.manifest.core.checkpoints.is_empty());
    }

    #[tokio::test]
    async fn pending_checkpoint_caps_all_manifest_writes_and_retries() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_pending_checkpoint_monotonic_wal_end",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let store = Arc::new(ManifestStore::new(
            &Path::from(harness.path.clone()),
            harness.object_store.clone(),
        ));
        let inner = harness.inner.clone();
        let mut handler = new_handler_from_harness(harness);
        let (tracker_tx, _tracker_rx) =
            crate::utils::SafeSender::unbounded_channel(inner.status_manager.result_reader());
        handler.tracker_tx = tracker_tx;
        handler.load_manifest().await.unwrap();
        handler.write_current_manifest_safely().await.unwrap();
        let first_upload = next_uploaded_memtable(&inner, b"first", b"v1").await;
        let second_upload = next_uploaded_memtable(&inner, b"second", b"v2").await;
        let (request, receivers) = new_test_checkpoint(cursor(second_upload.last_seq, 1), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), request)
            .unwrap();
        receivers.ready_rx.await.unwrap().unwrap();
        wal_end.store(2, Ordering::SeqCst);

        // An earlier table upload must preserve the pending checkpoint's WAL endpoint.
        handler.handle_uploaded(first_upload).await.unwrap();
        handler.process_ready_work().await.unwrap();
        assert_eq!(
            store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .next_wal_sst_id,
            2
        );

        // A durable checkpoint and a retry must preserve that endpoint too.
        handler
            .write_manifest_update_safely(&[&durable_pending_checkpoint()])
            .await
            .unwrap();
        let mut external = StoredManifest::load(store.clone(), Arc::new(DefaultSystemClock::new()))
            .await
            .unwrap();
        external
            .update(external.prepare_dirty().unwrap())
            .await
            .unwrap();
        handler.write_current_manifest_safely().await.unwrap();
        assert_eq!(
            store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .next_wal_sst_id,
            2
        );

        handler.handle_uploaded(second_upload).await.unwrap();
        handler.process_ready_work().await.unwrap();
        let checkpoint = receivers.result_rx.await.unwrap().unwrap();
        let manifest = store.read_manifest(checkpoint.manifest_id).await.unwrap();
        assert_eq!(manifest.core.next_wal_sst_id, 2);
        assert_eq!(manifest.core.last_l0_seq, 2);
        handler.write_current_manifest_safely().await.unwrap();
        assert_eq!(
            store
                .read_latest_manifest()
                .await
                .unwrap()
                .manifest
                .core
                .next_wal_sst_id,
            3
        );

        let mut previous_end = 0;
        for metadata in store.list_manifests(..).await.unwrap() {
            let manifest = store.read_manifest(metadata.id).await.unwrap();
            assert!(manifest.core.next_wal_sst_id >= previous_end);
            previous_end = manifest.core.next_wal_sst_id;
        }
    }

    #[tokio::test]
    async fn checkpoint_bound_includes_durable_wal_files_past_its_floor() {
        let wal_end = Arc::new(AtomicU64::new(2));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoint_bound_includes_durable_wal_files",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded1 = next_uploaded_memtable(&inner, b"k1", b"v1").await;
        let uploaded2 = next_uploaded_memtable(&inner, b"k2", b"v2").await;

        // WAL file 2 is durable before the request, so the bound moves to it.
        let request = begin_test_checkpoint(&started, cursor(1, 1), true);
        request.ready_rx.await.unwrap().unwrap();
        // A WAL file that becomes durable after the request does not move the bound.
        wal_end.store(3, Ordering::SeqCst);
        started.notify_uploaded(uploaded1).await.unwrap();
        started.notify_uploaded(uploaded2).await.unwrap();

        let result = request.result_rx.await.unwrap().unwrap();
        let manifest = ManifestStore::new(&Path::from(path), object_store)
            .read_manifest(result.manifest_id)
            .await
            .unwrap();
        assert_eq!(manifest.core.next_wal_sst_id, 3);
        assert!(manifest.core.last_l0_seq <= 2);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn checkpoints_with_different_wal_boundaries_use_different_manifests() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_concurrent_checkpoint_wal_boundaries",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        // Each WAL file ends with the write of one uploaded memtable.
        let mut uploads = Vec::new();
        for key in [b"k1", b"k2", b"k3"] {
            uploads.push(next_uploaded_memtable(&inner, key, b"v").await);
        }
        let first = begin_test_checkpoint(&started, cursor(uploads[0].last_seq, 1), true);
        first.ready_rx.await.unwrap().unwrap();
        let second = begin_test_checkpoint(&started, cursor(uploads[1].last_seq, 2), true);
        second.ready_rx.await.unwrap().unwrap();
        let third = begin_test_checkpoint(&started, cursor(uploads[2].last_seq, 3), true);
        third.ready_rx.await.unwrap().unwrap();

        wal_end.store(3, Ordering::SeqCst);
        for uploaded in uploads {
            started.notify_uploaded(uploaded).await.unwrap();
        }
        for through_seq in 1..=3 {
            assert_eq!(expect_flushed(&started.tracker_rx).await, through_seq);
        }

        let first_result = first.result_rx.await.unwrap().unwrap();
        let second_result = second.result_rx.await.unwrap().unwrap();
        let third_result = third.result_rx.await.unwrap().unwrap();
        assert_ne!(first_result.manifest_id, second_result.manifest_id);
        assert_ne!(second_result.manifest_id, third_result.manifest_id);

        let store = ManifestStore::new(&Path::from(path), object_store);
        let first_manifest = store.read_manifest(first_result.manifest_id).await.unwrap();
        let second_manifest = store
            .read_manifest(second_result.manifest_id)
            .await
            .unwrap();
        let third_manifest = store.read_manifest(third_result.manifest_id).await.unwrap();
        assert_eq!(first_manifest.core.last_l0_seq, 1);
        assert_eq!(first_manifest.core.next_wal_sst_id, 2);
        assert_eq!(second_manifest.core.last_l0_seq, 2);
        assert_eq!(second_manifest.core.next_wal_sst_id, 3);
        assert_eq!(third_manifest.core.last_l0_seq, 3);
        assert_eq!(third_manifest.core.next_wal_sst_id, 4);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn durable_checkpoint_does_not_wait_for_a_stalled_checkpoint_with_its_bound() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_durable_checkpoint_does_not_wait_for_stalled_checkpoint",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let mut handler = new_handler_from_harness(harness);

        let (stalled_request, mut stalled) = new_test_checkpoint(cursor(1, 1), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), stalled_request)
            .unwrap();
        let (durable_request, mut durable) = new_test_checkpoint(None, false);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), durable_request)
            .unwrap();
        handler.process_ready_work().await.unwrap();

        assert!(durable.result_rx.try_recv().unwrap().is_ok());
        assert!(stalled.result_rx.try_recv().is_err());
        assert_eq!(handler.pending_checkpoints.len(), 1);
    }

    #[tokio::test]
    async fn checkpoint_with_a_higher_bound_waits_for_a_stalled_checkpoint() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoint_with_higher_bound_waits_for_stalled_checkpoint",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let mut handler = new_handler_from_harness(harness);

        let (stalled_request, mut stalled) = new_test_checkpoint(cursor(1, 1), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), stalled_request)
            .unwrap();
        wal_end.store(2, Ordering::SeqCst);
        let (durable_request, mut durable) = new_test_checkpoint(None, false);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), durable_request)
            .unwrap();
        handler.process_ready_work().await.unwrap();

        assert!(durable.result_rx.try_recv().is_err());
        assert!(stalled.result_rx.try_recv().is_err());
        assert_eq!(handler.pending_checkpoints.len(), 2);
    }

    #[tokio::test]
    async fn source_checkpoint_does_not_wait_for_an_unrelated_stalled_checkpoint() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_source_checkpoint_does_not_wait_for_stalled_checkpoint",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let mut handler = new_handler_from_harness(harness);
        let source_id = Uuid::new_v4();
        let existing_checkpoint = handler
            .manifest
            .write_checkpoint(source_id, &CheckpointOptions::default())
            .await
            .unwrap();

        let (stalled_request, mut stalled) = new_test_checkpoint(cursor(1, 1), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), stalled_request)
            .unwrap();
        let (source_request, mut from_source) = new_test_checkpoint(None, false);
        handler
            .handle_create_checkpoint(
                CheckpointOptions {
                    source: Some(source_id),
                    ..CheckpointOptions::default()
                },
                source_request,
            )
            .unwrap();
        handler.process_ready_work().await.unwrap();

        let result = from_source.result_rx.try_recv().unwrap().unwrap();
        assert_eq!(result.manifest_id, existing_checkpoint.manifest_id);
        assert!(stalled.result_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn source_checkpoint_waits_for_its_pending_source() {
        let wal_end = Arc::new(AtomicU64::new(0));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_source_checkpoint_waits_for_pending_source",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            harness.object_store.clone(),
        );
        let inner = harness.inner.clone();
        let mut handler = new_handler_from_harness(harness);
        let (tracker_tx, _tracker_rx) =
            crate::utils::SafeSender::unbounded_channel(inner.status_manager.result_reader());
        handler.tracker_tx = tracker_tx;
        let upload = next_uploaded_memtable(&inner, b"key", b"value").await;

        let (source_request, mut source) = new_test_checkpoint(cursor(upload.last_seq, 1), true);
        let source_id = source_request.id;
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), source_request)
            .unwrap();
        let (copy_request, mut copy) = new_test_checkpoint(None, false);
        handler
            .handle_create_checkpoint(
                CheckpointOptions {
                    source: Some(source_id),
                    ..CheckpointOptions::default()
                },
                copy_request,
            )
            .unwrap();
        handler.process_ready_work().await.unwrap();
        assert!(source.result_rx.try_recv().is_err());
        assert!(copy.result_rx.try_recv().is_err());
        assert_eq!(handler.pending_checkpoints.len(), 2);

        wal_end.store(1, Ordering::SeqCst);
        handler.handle_uploaded(upload).await.unwrap();
        handler.process_ready_work().await.unwrap();

        let source_result = source.result_rx.try_recv().unwrap().unwrap();
        let copy_result = copy.result_rx.try_recv().unwrap().unwrap();
        assert_eq!(copy_result.manifest_id, source_result.manifest_id);
        let latest = store.read_latest_manifest().await.unwrap();
        assert_eq!(latest.manifest.core.checkpoints.len(), 2);
    }

    #[tokio::test]
    async fn source_checkpoint_with_a_missing_source_fails_alone() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_source_checkpoint_with_missing_source_fails_alone",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let mut handler = new_handler_from_harness(harness);
        let missing_id = Uuid::new_v4();

        let (missing_request, mut missing) = new_test_checkpoint(None, false);
        handler
            .handle_create_checkpoint(
                CheckpointOptions {
                    source: Some(missing_id),
                    ..CheckpointOptions::default()
                },
                missing_request,
            )
            .unwrap();
        let (durable_request, mut durable) = new_test_checkpoint(None, false);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), durable_request)
            .unwrap();
        handler.process_ready_work().await.unwrap();

        assert!(matches!(
            missing.result_rx.try_recv().unwrap(),
            Err(SlateDBError::CheckpointMissing(id)) if id == missing_id
        ));
        assert!(durable.result_rx.try_recv().unwrap().is_ok());
    }

    #[tokio::test]
    async fn checkpoints_leave_the_queue_in_bound_order() {
        let wal_end = Arc::new(AtomicU64::new(0));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoints_leave_the_queue_in_bound_order",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            harness.object_store.clone(),
        );
        let inner = harness.inner.clone();
        let mut handler = new_handler_from_harness(harness);
        let (tracker_tx, _tracker_rx) =
            crate::utils::SafeSender::unbounded_channel(inner.status_manager.result_reader());
        handler.tracker_tx = tracker_tx;
        let first_upload = next_uploaded_memtable(&inner, b"first", b"v1").await;
        let second_upload = next_uploaded_memtable(&inner, b"second", b"v2").await;

        // The request with the higher bound arrives first.
        let (later_request, mut later) =
            new_test_checkpoint(cursor(second_upload.last_seq, 2), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), later_request)
            .unwrap();
        let (earlier_request, mut earlier) =
            new_test_checkpoint(cursor(first_upload.last_seq, 1), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), earlier_request)
            .unwrap();

        wal_end.store(2, Ordering::SeqCst);
        handler.handle_uploaded(first_upload).await.unwrap();
        handler.handle_uploaded(second_upload).await.unwrap();
        handler.process_ready_work().await.unwrap();

        // Each checkpoint stops at its own bound.
        let earlier_result = earlier.result_rx.try_recv().unwrap().unwrap();
        let earlier_manifest = store
            .read_manifest(earlier_result.manifest_id)
            .await
            .unwrap();
        assert_eq!(earlier_manifest.core.last_l0_seq, 1);
        assert_eq!(earlier_manifest.core.next_wal_sst_id, 2);
        let later_result = later.result_rx.try_recv().unwrap().unwrap();
        let later_manifest = store.read_manifest(later_result.manifest_id).await.unwrap();
        assert_eq!(later_manifest.core.last_l0_seq, 2);
        assert_eq!(later_manifest.core.next_wal_sst_id, 3);
    }

    #[tokio::test]
    async fn checkpoint_waits_for_l0_when_its_floor_is_in_the_active_memtable() {
        let wal_end = Arc::new(AtomicU64::new(1));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoint_floor_in_active_memtable",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let store = ManifestStore::new(
            &Path::from(harness.path.clone()),
            harness.object_store.clone(),
        );
        let inner = harness.inner.clone();
        let mut handler = new_handler_from_harness(harness);
        let (tracker_tx, _tracker_rx) =
            crate::utils::SafeSender::unbounded_channel(inner.status_manager.result_reader());
        handler.tracker_tx = tracker_tx;

        // The floor write is in the active memtable, and no immutable memtable exists.
        let seq = inner.oracle.next_seq();
        inner
            .state
            .write()
            .memtable()
            .put(RowEntry::new_value(b"key", b"value", seq));
        let (request, mut receivers) = new_test_checkpoint(cursor(seq, 1), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), request)
            .unwrap();
        handler.process_ready_work().await.unwrap();
        assert!(matches!(
            receivers.result_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // The manifest writer writes the checkpoint after L0 holds the floor write.
        let imm = {
            let mut guard = inner.state.write();
            guard.freeze_memtable(1);
            guard.state().imm_memtable.front().cloned().unwrap()
        };
        let sst = inner
            .flush_l0_for_test(imm.table())
            .await
            .unwrap()
            .pop()
            .unwrap();
        inner.oracle.advance_durable_seq(seq);
        handler
            .handle_uploaded(UploadedMemtable::new(imm, sst, seq, seq))
            .await
            .unwrap();
        handler.process_ready_work().await.unwrap();
        let result = receivers.result_rx.try_recv().unwrap().unwrap();
        let manifest = store.read_manifest(result.manifest_id).await.unwrap();
        assert_eq!(manifest.core.last_l0_seq, seq);
    }

    #[tokio::test]
    async fn abandoned_checkpoint_observers_preserve_the_boundary() {
        for abandon_before_accept in [false, true] {
            let wal_end = Arc::new(AtomicU64::new(0));
            let harness = setup_harness_with_wal_observer(
                "/tmp/test_abandoned_checkpoint_observers",
                Arc::new(FailPointRegistry::new()),
                None,
                Box::new(MutableWalObserver {
                    last_flushed_wal_id: wal_end.clone(),
                }),
            )
            .await;
            let manifest_store = ManifestStore::new(
                &Path::from(harness.path.clone()),
                Arc::clone(&harness.object_store),
            );
            let inner = Arc::clone(&harness.inner);
            let started = start_manifest_writer(
                Arc::clone(&inner),
                harness.manifest,
                Duration::from_secs(3600),
            );
            let uploaded1 = next_uploaded_memtable(&inner, b"before", b"v1").await;
            let uploaded2 = next_uploaded_memtable(&inner, b"after", b"v2").await;
            let TestCheckpointRequest {
                id,
                ready_rx,
                result_rx,
            } = begin_test_checkpoint(&started, cursor(1, 1), true);
            if abandon_before_accept {
                drop(ready_rx);
                drop(result_rx);
            } else {
                ready_rx.await.unwrap().unwrap();
                let mut wait = Box::pin(CheckpointHandle::new(id, result_rx).wait());
                assert!(futures::poll!(&mut wait).is_pending());
                drop(wait);
            }

            started.notify_uploaded(uploaded2).await.unwrap();
            assert_no_flush_event(&started.tracker_rx, Duration::from_millis(100)).await;
            // Make sure that the manifest writer computed the bound before the WAL advances.
            sync_manifest_writer(&started).await;
            wal_end.store(1, Ordering::SeqCst);
            started.notify_uploaded(uploaded1).await.unwrap();
            assert_eq!(expect_flushed(&started.tracker_rx).await, 1);
            assert_eq!(expect_flushed(&started.tracker_rx).await, 2);

            let manifest = manifest_store.read_latest_manifest().await.unwrap();
            let checkpoint = manifest
                .manifest
                .core
                .checkpoints
                .iter()
                .find(|checkpoint| checkpoint.id == id)
                .unwrap();
            let boundary = manifest_store
                .read_manifest(checkpoint.manifest_id)
                .await
                .unwrap();
            assert_eq!(boundary.core.last_l0_seq, 1);
            started.shutdown().await;
        }
    }

    #[tokio::test]
    async fn should_create_checkpoint_immediately_when_no_barrier_is_required() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_checkpoint_immediate",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;

        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        let (tx, rx) = oneshot::channel();
        started
            .send_checkpoint(None, CheckpointOptions::default(), tx)
            .unwrap();
        let checkpoint = timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let after =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        assert_eq!(after, before + 1);
        assert!(checkpoint.manifest_id > 0);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_wait_for_checkpoint_barrier_and_attach_to_flush_batch() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_checkpoint_barrier",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;

        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded = next_uploaded_memtable(&inner, b"k1", b"v1").await;

        let (tx, rx) = oneshot::channel();
        started
            .send_checkpoint(Some(1), CheckpointOptions::default(), tx)
            .unwrap();

        tokio::task::yield_now().await;
        assert_no_flush_event(&started.tracker_rx, Duration::from_millis(100)).await;

        started.notify_uploaded(uploaded).await.unwrap();

        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, 1);

        let checkpoint = rx.await.unwrap().unwrap();
        let after =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        assert_eq!(after, before + 1);
        assert!(checkpoint.manifest_id > 0);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_emit_fatal_event_when_manifest_writer_is_fenced() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded = next_uploaded_memtable(&inner, b"k1", b"v1").await;

        let _fence = load_writer_manifest(&path, object_store).await;
        started.notify_uploaded(uploaded).await.unwrap();

        // The manifest writer detects the fence and writes the error to closed_result.
        let result = timeout(Duration::from_secs(5), started.await_closed())
            .await
            .expect("timed out waiting for fenced error");
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result
        );

        started.shutdown().await;
    }

    #[tokio::test]
    async fn pending_flush_waiter_receives_error_on_fenced_shutdown() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_pending_flush_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded = next_uploaded_memtable(&inner, b"k1", b"v1").await;

        // Send a flush request for epoch 1, which hasn't been uploaded yet.
        let (tx, rx) = oneshot::channel();
        started.send_flush(Some(1), tx).unwrap();

        // Fence the manifest so the next write fails.
        let _fence = load_writer_manifest(&path, object_store).await;

        // Trigger a manifest write by uploading — this discovers the fence.
        started.notify_uploaded(uploaded).await.unwrap();

        // The pending flush waiter should receive the fencing error.
        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("channel dropped");
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result
        );

        started.shutdown().await;
    }

    #[tokio::test]
    async fn pending_checkpoint_waiter_receives_error_on_fenced_shutdown() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_pending_checkpoint_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let path = harness.path.clone();
        let object_store = Arc::clone(&harness.object_store);
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );
        let uploaded = next_uploaded_memtable(&inner, b"k1", b"v1").await;

        // Send a checkpoint request for epoch 1, which hasn't been uploaded yet.
        let (tx, rx) = oneshot::channel();
        started
            .send_checkpoint(Some(1), CheckpointOptions::default(), tx)
            .unwrap();

        // Fence and trigger a manifest write.
        let _fence = load_writer_manifest(&path, object_store).await;
        started.notify_uploaded(uploaded).await.unwrap();

        // The pending checkpoint waiter should receive the fencing error.
        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("channel dropped");
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result
        );

        started.shutdown().await;
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn checkpoint_waits_for_its_wal_file(#[case] drop_observer: bool) {
        let wal_end = Arc::new(AtomicU64::new(0));
        let harness = setup_harness_with_wal_observer(
            "/tmp/test_checkpoint_waits_for_its_wal_file",
            Arc::new(FailPointRegistry::new()),
            None,
            Box::new(MutableWalObserver {
                last_flushed_wal_id: wal_end.clone(),
            }),
        )
        .await;
        let manifest_store = ManifestStore::new(&Path::from(harness.path), harness.object_store);
        let started =
            start_manifest_writer(harness.inner, harness.manifest, Duration::from_secs(3600));
        let request = begin_test_checkpoint(&started, cursor(1, 1), false);
        timeout(Duration::from_secs(5), request.ready_rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut wait = Some(Box::pin(
            CheckpointHandle::new(request.id, request.result_rx).wait_inner(),
        ));
        assert!(futures::poll!(wait.as_mut().unwrap()).is_pending());
        sync_manifest_writer(&started).await;
        let id = request.id;
        let has_checkpoint = |checkpoints: &[crate::checkpoint::Checkpoint]| {
            checkpoints.iter().any(|checkpoint| checkpoint.id == id)
        };
        let latest = manifest_store.read_latest_manifest().await.unwrap();
        assert!(!has_checkpoint(&latest.manifest.core.checkpoints));
        if drop_observer {
            drop(wait.take());
        }

        wal_end.store(1, Ordering::SeqCst);
        // The first poll writes the checkpoint. The second poll waits for the first one.
        sync_manifest_writer(&started).await;
        sync_manifest_writer(&started).await;
        if let Some(wait) = wait {
            timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap();
        }
        let latest = manifest_store.read_latest_manifest().await.unwrap();
        assert!(has_checkpoint(&latest.manifest.core.checkpoints));
        started.shutdown().await;
    }

    #[tokio::test]
    async fn attached_checkpoint_receives_uploaded_state_error() {
        let harness = setup_harness(
            "/tmp/test_checkpoint_uploaded_state_error",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let mut handler = new_handler_from_harness(harness);
        let mut uploaded = next_uploaded_memtable(&inner, b"key", b"value").await;
        let prefix = Bytes::from_static(b"unexpected");
        uploaded.segments[0].prefix = prefix.clone();
        let imm_memtable = Arc::clone(&uploaded.imm_memtable);
        let durable = imm_memtable.table().durable_watcher();
        let (request, receivers) = new_test_checkpoint(cursor(uploaded.last_seq, 0), true);
        handler
            .handle_create_checkpoint(CheckpointOptions::default(), request)
            .unwrap();
        receivers.ready_rx.await.unwrap().unwrap();
        handler.handle_uploaded(uploaded).await.unwrap();

        let result = handler.process_ready_work().await;
        let checkpoint_result = CheckpointHandle::new(receivers.id, receivers.result_rx)
            .wait_inner()
            .await
            .map(|_| ());
        let upload_result = timeout(Duration::from_secs(5), imm_memtable.await_uploaded())
            .await
            .unwrap();
        let durable_result = durable.read().expect("durability waiter was not notified");
        for result in [result, checkpoint_result, upload_result, durable_result] {
            assert!(matches!(
                result,
                Err(SlateDBError::InvalidSegmentPrefix { prefix: actual, conflict })
                    if actual == prefix && conflict.is_empty()
            ));
        }
    }

    #[tokio::test]
    async fn pending_manifest_refresh_waiter_receives_error_on_fenced_shutdown() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_pending_poll_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let mut handler = new_handler_from_harness(harness);

        let (tx, rx) = oneshot::channel();
        let messages =
            futures::stream::iter(vec![ManifestWriterCommand::PollManifest { done: Some(tx) }]);
        crate::dispatcher::MessageHandler::cleanup(
            &mut handler,
            Box::pin(messages),
            Err(SlateDBError::Fenced),
        )
        .await
        .unwrap();

        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("channel dropped");
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn manifest_refresh_waiter_receives_error_when_manifest_writer_channel_closed() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_closed_poll_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        started
            .closed_result
            .write_result(Err(SlateDBError::Fenced));
        started.shutdown().await;

        let (tx, rx) = oneshot::channel();
        let err = started.send_poll(tx).unwrap_err();
        assert!(
            matches!(err, SlateDBError::Fenced),
            "expected Fenced, got {:?}",
            err
        );

        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("channel dropped");
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn flush_waiter_receives_error_when_manifest_writer_channel_closed() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_closed_flush_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        started
            .closed_result
            .write_result(Err(SlateDBError::Fenced));
        started.shutdown().await;

        let (tx, rx) = oneshot::channel();
        let err = started.send_flush(Some(1), tx).unwrap_err();
        assert!(
            matches!(err, SlateDBError::Fenced),
            "expected Fenced, got {:?}",
            err
        );

        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("channel dropped");
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn checkpoint_waiter_receives_error_when_manifest_writer_channel_closed() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_closed_checkpoint_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        started
            .closed_result
            .write_result(Err(SlateDBError::Fenced));
        started.shutdown().await;

        let (tx, rx) = oneshot::channel();
        let err = started
            .send_checkpoint(Some(1), CheckpointOptions::default(), tx)
            .unwrap_err();
        assert!(
            matches!(err, SlateDBError::Fenced),
            "expected Fenced, got {:?}",
            err
        );

        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("channel dropped");
        assert!(
            matches!(result, Err(SlateDBError::Fenced)),
            "expected Fenced, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn flush_waiter_in_channel_receives_error_on_clean_shutdown() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_channel_flush_clean",
            Arc::new(FailPointRegistry::new()),
        )
        .await;

        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        // Send a flush request for an epoch that will never be uploaded.
        let (tx, rx) = oneshot::channel();
        started.send_flush(Some(1), tx).unwrap();

        // Shut down cleanly — the flush waiter should get Closed.
        started.shutdown().await;

        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("timed out")
            .expect("channel dropped");
        assert!(
            matches!(result, Err(SlateDBError::Closed)),
            "expected Closed, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn should_wait_for_wal_durable_seq_before_writing_manifest() {
        let harness = setup_harness(
            "/tmp/test_manifest_writer_wal_durable_barrier",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        // Upload a memtable without advancing the WAL durable sequence.
        let uploaded = next_uploaded_memtable_no_wal(&inner, b"k1", b"v1").await;
        let last_seq = uploaded.last_seq;
        started.notify_uploaded(uploaded).await.unwrap();

        // The manifest should NOT be written yet — WAL is not durable.
        assert_no_flush_event(&started.tracker_rx, Duration::from_millis(100)).await;

        // Now simulate the WAL flush completing.
        inner.oracle.advance_durable_seq(last_seq);

        // The manifest writer should now process the batch.
        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, last_seq);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_flush_partial_batch_up_to_durable_seq() {
        let harness = setup_harness(
            "/tmp/test_manifest_writer_partial_durable_batch",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        // Upload two memtables without advancing WAL durable seq.
        let uploaded1 = next_uploaded_memtable_no_wal(&inner, b"k1", b"v1").await;
        let last_seq1 = uploaded1.last_seq;
        let uploaded2 = next_uploaded_memtable_no_wal(&inner, b"k2", b"v2").await;
        let last_seq2 = uploaded2.last_seq;
        started.notify_uploaded(uploaded1).await.unwrap();
        started.notify_uploaded(uploaded2).await.unwrap();

        // Advance durable seq to cover only the first memtable.
        inner.oracle.advance_durable_seq(last_seq1);

        // Only the first memtable should be flushed.
        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, last_seq1);

        // The second memtable is still blocked.
        assert_no_flush_event(&started.tracker_rx, Duration::from_millis(100)).await;

        // Advance durable seq to cover the second memtable.
        inner.oracle.advance_durable_seq(last_seq2);

        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, last_seq2);

        started.shutdown().await;
    }

    /// Construct an `UploadedMemtable` whose flush output spans
    /// multiple named segments. Each segment handle is an
    /// independently-uploaded SST tagged with one of the requested
    /// `prefixes`. The apply path routes by prefix without
    /// validating SST contents, so the synthetic fabrication is
    /// sound for manifest-writer tests.
    ///
    /// Goes around `build_imm_ssts` because the synthetic setup
    /// (one key, many fake prefixes) can't satisfy that path's
    /// requirement that recorded prefixes match the keys.
    async fn next_uploaded_memtable_with_segments(
        inner: &Arc<DbInner>,
        key: &[u8],
        value: &[u8],
        prefixes: &[&[u8]],
    ) -> UploadedMemtable {
        use crate::utils::IdGenerator;
        let imm_memtable = freeze_imm(inner, key, value);
        let first_seq = imm_memtable.table().first_seq().unwrap();
        let last_seq = imm_memtable.table().last_seq().unwrap();
        let mut segments = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            // Each synthetic SST needs at least one entry so the
            // resulting handle can construct an `SsTableView` in the
            // apply path. The actual contents don't matter — the
            // manifest writer routes by `prefix` from the surrounding
            // `SegmentedSstHandle`, not by the SST's keys.
            let mut builder = inner.table_store.table_builder();
            let row = RowEntry::new_value(prefix, value, first_seq);
            builder.add(row).await.unwrap();
            let encoded_sst = builder.build().await.unwrap();
            let id = crate::db_state::SsTableId::from(
                inner.rand.rng().gen_ulid(inner.system_clock.as_ref()),
            );
            let sst_handle = inner
                .upload_sst(&id, &encoded_sst, Bytes::copy_from_slice(prefix))
                .await
                .unwrap();
            segments.push(SegmentedSstHandle {
                prefix: Bytes::copy_from_slice(prefix),
                sst_handle,
                encoded_bytes: encoded_sst.remaining_len() as u64,
            });
        }
        inner.oracle.advance_durable_seq(last_seq);
        UploadedMemtable {
            imm_memtable,
            segments,
            first_seq,
            last_seq,
        }
    }

    /// Test-only extractor used solely as a marker that
    /// `DbInner::segment_extractor.is_some()` so the apply path routes into
    /// `core.segments`. The flush bookkeeping under test does not actually
    /// invoke this extractor's logic.
    struct StubExtractor;
    impl crate::prefix_extractor::PrefixExtractor for StubExtractor {
        fn name(&self) -> &str {
            "stub"
        }
        fn prefix_len(&self, _target: &crate::prefix_extractor::PrefixTarget) -> Option<usize> {
            Some(0)
        }
    }

    #[tokio::test]
    async fn should_route_segment_handles_into_named_segments() {
        let harness = setup_harness_with_extractor(
            "/tmp/test_manifest_writer_segment_routing",
            Arc::new(FailPointRegistry::new()),
            Some(Arc::new(StubExtractor)),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        // Two segments — "aaa" and "bbb" — published from a single flush.
        let uploaded =
            next_uploaded_memtable_with_segments(&inner, b"aaa-key", b"v1", &[b"aaa", b"bbb"])
                .await;
        let last_seq = uploaded.last_seq;
        let aaa_id = uploaded.segments[0].sst_handle.id;
        let bbb_id = uploaded.segments[1].sst_handle.id;
        started.notify_uploaded(uploaded).await.unwrap();

        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, last_seq);

        // Verify the manifest now has both segments, sorted by prefix, each
        // with one L0. The top-level tree stays empty.
        let core = inner.state.read().state().core().clone();
        assert!(core.tree.l0.is_empty(), "root tree should be empty");
        assert_eq!(core.segments.len(), 2);
        assert_eq!(core.segments[0].prefix.as_ref(), b"aaa");
        assert_eq!(core.segments[1].prefix.as_ref(), b"bbb");
        assert_eq!(core.segments[0].tree.l0.len(), 1);
        assert_eq!(core.segments[1].tree.l0.len(), 1);
        assert_eq!(core.segments[0].tree.l0[0].sst.id, aaa_id);
        assert_eq!(core.segments[1].tree.l0[0].sst.id, bbb_id);
        // Newly flushed L0s are identity views: the view id equals the
        // physical SST ULID (RFC-0029).
        assert_eq!(core.segments[0].tree.l0[0].id, aaa_id.value());
        assert_eq!(core.segments[1].tree.l0[0].id, bbb_id.value());

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_create_identity_l0_view_on_flush() {
        let harness = setup_harness(
            "/tmp/test_manifest_writer_identity_l0_view",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        let uploaded = next_uploaded_memtable(&inner, b"k1", b"v1").await;
        let physical_id = uploaded.segments[0].sst_handle.id;
        started.notify_uploaded(uploaded).await.unwrap();
        let _ = expect_flushed(&started.tracker_rx).await;

        // The published L0 view id must equal the physical SST ULID so the
        // timestamp `last_compacted_l0_sst_view_id` reads matches the one GC
        // deletion reads (RFC-0029).
        let core = inner.state.read().state().core().clone();
        assert_eq!(core.tree.l0.len(), 1);
        let view = &core.tree.l0[0];
        assert_eq!(view.sst.id, physical_id);
        assert_eq!(view.id, physical_id.value());

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_append_to_existing_segment_l0() {
        let harness = setup_harness_with_extractor(
            "/tmp/test_manifest_writer_segment_append",
            Arc::new(FailPointRegistry::new()),
            Some(Arc::new(StubExtractor)),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        // First flush creates segment "aaa".
        let uploaded1 =
            next_uploaded_memtable_with_segments(&inner, b"aaa-1", b"v1", &[b"aaa"]).await;
        started.notify_uploaded(uploaded1).await.unwrap();
        let _ = expect_flushed(&started.tracker_rx).await;

        // Second flush adds another L0 to "aaa" alongside a new "bbb".
        let uploaded2 =
            next_uploaded_memtable_with_segments(&inner, b"aaa-2", b"v2", &[b"aaa", b"bbb"]).await;
        started.notify_uploaded(uploaded2).await.unwrap();
        let _ = expect_flushed(&started.tracker_rx).await;

        let core = inner.state.read().state().core().clone();
        assert_eq!(core.segments.len(), 2);
        assert_eq!(core.segments[0].prefix.as_ref(), b"aaa");
        assert_eq!(core.segments[1].prefix.as_ref(), b"bbb");
        assert_eq!(core.segments[0].tree.l0.len(), 2);
        assert_eq!(core.segments[1].tree.l0.len(), 1);

        started.shutdown().await;
    }

    #[tokio::test]
    async fn should_advance_progress_when_segments_is_empty() {
        // With an extractor configured and post-retention pruning that
        // drops every entry, the upload pipeline yields an UploadedMemtable
        // with an empty segments Vec. The manifest writer must still
        // advance per-memtable bookkeeping (last_l0_seq, replay frontier)
        // even though no SSTs land in any tree.
        let harness = setup_harness_with_extractor(
            "/tmp/test_manifest_writer_empty_segments",
            Arc::new(FailPointRegistry::new()),
            Some(Arc::new(StubExtractor)),
        )
        .await;
        let inner = Arc::clone(&harness.inner);
        let started = start_manifest_writer(
            Arc::clone(&inner),
            harness.manifest,
            Duration::from_secs(3600),
        );

        let uploaded = next_uploaded_memtable_with_segments(&inner, b"k1", b"v1", &[]).await;
        let last_seq = uploaded.last_seq;
        started.notify_uploaded(uploaded).await.unwrap();

        let through_seq = expect_flushed(&started.tracker_rx).await;
        assert_eq!(through_seq, last_seq);

        // No SST landed in any tree, but the per-memtable progress
        // markers still advanced.
        let core = inner.state.read().state().core().clone();
        assert!(core.tree.l0.is_empty(), "root tree should be empty");
        assert!(core.segments.is_empty(), "no segments should be created");
        assert_eq!(core.last_l0_seq, last_seq);

        started.shutdown().await;
    }
}
