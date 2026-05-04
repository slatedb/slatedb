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
use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::SsTableView;
use crate::dispatcher::MessageHandler;
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::oracle::Oracle;
use crate::utils::IdGenerator;
use crate::utils::SafeSender;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::sync::watch;

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
        through_seq: Option<u64>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
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
            Self::CreateCheckpoint { through_seq, .. } => {
                write!(f, "CreateCheckpoint({through_seq:?})")
            }
            Self::PollManifest { .. } => write!(f, "PollManifest"),
            Self::DurableSeqAdvanced => write!(f, "DurableSeqAdvanced"),
        }
    }
}

pub(super) const MANIFEST_WRITER_TASK_NAME: &str = "l0_manifest_writer";

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
    ) -> Result<Self, SlateDBError> {
        let (commands_tx, commands_rx) =
            SafeSender::unbounded_channel(closed_result.result_reader());
        let handler = ManifestWriterHandler::new(db, manifest, manifest_poll_interval, tracker_tx);
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
        self.commands_tx.send(ManifestWriterCommand::AwaitFlush {
            through_seq,
            sender,
        })
    }

    /// Sends a checkpoint request to the manifest_writer. The manifest_writer will write
    /// the checkpoint once all sequences up to and including `through_seq` are
    /// durable (or immediately if `None`) and respond via `sender`.
    pub(crate) fn send_checkpoint(
        &self,
        through_seq: Option<u64>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        self.commands_tx
            .send(ManifestWriterCommand::CreateCheckpoint {
                through_seq,
                options,
                sender,
            })
    }

    /// Enqueues a manifest poll; the result is delivered to `sender` on completion.
    pub(crate) fn send_poll(
        &self,
        sender: oneshot::Sender<Result<(), SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        self.commands_tx
            .send(ManifestWriterCommand::PollManifest { done: Some(sender) })
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
    pending_flushes: Vec<PendingFlush>,
    pending_checkpoints: Vec<PendingCheckpoint>,
}

#[async_trait]
impl MessageHandler<ManifestWriterCommand> for ManifestWriterHandler {
    fn tickers(
        &mut self,
    ) -> Vec<(
        Duration,
        Box<crate::dispatcher::MessageFactory<ManifestWriterCommand>>,
    )> {
        vec![(
            self.manifest_poll_interval,
            Box::new(|| ManifestWriterCommand::PollManifest { done: None }),
        )]
    }

    fn notifiers(&mut self) -> Vec<Box<dyn crate::dispatcher::Notifier<ManifestWriterCommand>>> {
        vec![Box::new(DurableSeqNotifier {
            rx: self.db_status_rx.clone(),
        })]
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
            ManifestWriterCommand::CreateCheckpoint {
                through_seq,
                options,
                sender,
            } => {
                self.handle_create_checkpoint(through_seq, options, sender)
                    .await?;
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
        // Drain any commands not consumed by graceful cleanup, collecting
        // flush/checkpoint waiters so they receive a proper error.
        while let Some(command) = commands.next().await {
            self.collect_pending_waiter(command);
        }
        // Any remaining pending waiters must fail with a concrete error.
        let error = result
            .and(close_result.clone())
            .err()
            .unwrap_or(SlateDBError::Closed);
        self.fail_pending_flushes(&error);
        self.fail_pending_checkpoints(&error);
        close_result
    }
}

impl ManifestWriterHandler {
    fn new(
        db: Arc<DbInner>,
        manifest: FenceableManifest,
        manifest_poll_interval: Duration,
        tracker_tx: SafeSender<TrackerMessage>,
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
            pending_checkpoints: Vec::new(),
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

    async fn handle_create_checkpoint(
        &mut self,
        through_seq: Option<u64>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        if self.is_durable(through_seq) {
            let result = self.write_checkpoint_safely(&options).await;
            let _ = sender.send(result.clone());
            return result.map(|_| ());
        }

        self.pending_checkpoints.push(PendingCheckpoint {
            through_seq,
            options,
            sender,
        });
        Ok(())
    }

    async fn process_ready_work(&mut self) -> Result<(), SlateDBError> {
        loop {
            let Some(staged_batch) = self.take_next_ready_batch() else {
                return Ok(());
            };
            let through_seq = staged_batch
                .last()
                .map(|uploaded| uploaded.last_seq)
                .expect("staged batch should not be empty");
            let attached_checkpoints = self.take_satisfied_pending_checkpoints(through_seq);
            self.apply_ready_batch(staged_batch, attached_checkpoints, through_seq)
                .await?;
        }
    }

    fn take_next_ready_batch(&mut self) -> Option<Vec<UploadedMemtable>> {
        let durable_seq = self.db_status_rx.borrow().durable_seq;
        let imm_memtables: Vec<_> = {
            let guard = self.db.state.read();
            guard.state().imm_memtable.iter().rev().cloned().collect()
        };
        let mut batch = Vec::new();

        for imm_memtable in imm_memtables {
            let first_seq = imm_memtable
                .table()
                .first_seq()
                .expect("immutable memtable has no entries");
            let Some(uploaded) = self.ready.get(&first_seq) else {
                break;
            };
            assert!(
                Arc::ptr_eq(&uploaded.imm_memtable, &imm_memtable),
                "uploaded memtable identity mismatch for first_seq {}",
                first_seq
            );
            // WAL SSTs must be durable before the manifest is updated (see #1255).
            if self.db.wal_enabled && uploaded.last_seq > durable_seq {
                break;
            }

            let uploaded = self.ready.remove(&first_seq).expect("peeked entry missing");
            batch.push(uploaded);
        }

        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }

    fn take_satisfied_pending_checkpoints(&mut self, through_seq: u64) -> Vec<PendingCheckpoint> {
        let mut satisfied = Vec::new();
        let mut pending = Vec::with_capacity(self.pending_checkpoints.len());
        for checkpoint in self.pending_checkpoints.drain(..) {
            if checkpoint
                .through_seq
                .is_none_or(|required_seq| required_seq <= through_seq)
            {
                satisfied.push(checkpoint);
            } else {
                pending.push(checkpoint);
            }
        }
        self.pending_checkpoints = pending;
        satisfied
    }

    async fn apply_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        through_seq: u64,
    ) -> Result<(), SlateDBError> {
        self.apply_uploaded_state(&staged_batch)?;

        for uploaded in &staged_batch {
            uploaded.imm_memtable.notify_uploaded(Ok(()));
        }
        self.db
            .db_stats
            .immutable_memtable_flushes
            .increment(staged_batch.len() as u64);

        match self
            .write_manifest_update_safely(
                &attached_checkpoints
                    .iter()
                    .map(|c| &c.options)
                    .collect::<Vec<_>>(),
            )
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
        let min_active_snapshot_seq = [
            self.db.snapshot_manager.min_active_seq(),
            self.db.txn_manager.min_active_seq(),
        ]
        .into_iter()
        .flatten()
        .min();
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
                // `segments` may legitimately be empty when an extractor
                // is configured and retention pruned every entry: no
                // builders open → no SSTs uploaded. We still apply the
                // memtable's seq/tick bookkeeping below so progress
                // advances.
                let segmented = self.db.segment_extractor.is_some();
                for segment in &uploaded.segments {
                    let view = SsTableView::new(
                        self.db.rand.rng().gen_ulid(self.db.system_clock.as_ref()),
                        segment.sst_handle.clone(),
                    );
                    let core = &mut modifier.state.manifest.value.core;
                    let tree: &mut crate::manifest::LsmTreeState = if segmented {
                        // Extractor configured — every flush handle, including
                        // any with empty prefix, is routed into `segments`.
                        core.maybe_insert_tree(&segment.prefix)?
                    } else {
                        // No extractor — singleton compatibility-encoded
                        // `prefix=""` segment lives in the top-level tree.
                        debug_assert!(
                            segment.prefix.is_empty(),
                            "non-empty prefix produced without an extractor"
                        );
                        &mut core.tree
                    };
                    tree.l0.push_front(view);
                }
                modifier.state.manifest.value.core.replay_after_wal_id =
                    uploaded.imm_memtable.recent_flushed_wal_id();

                let memtable_tick = uploaded.imm_memtable.table().last_tick();
                modifier.state.manifest.value.core.last_l0_clock_tick = cmp::max(
                    modifier.state.manifest.value.core.last_l0_clock_tick,
                    memtable_tick,
                );
                if modifier.state.manifest.value.core.last_l0_clock_tick != memtable_tick {
                    return Err(SlateDBError::InvalidClockTick {
                        last_tick: modifier.state.manifest.value.core.last_l0_clock_tick,
                        next_tick: memtable_tick,
                    });
                }

                // The same sequence number can't span multiple L0' SSTs--only SSTs in SRs
                // can do that. So assert `>` rather than `>=`.
                assert!(uploaded.last_seq > modifier.state.manifest.value.core.last_l0_seq);
                modifier.state.manifest.value.core.last_l0_seq = uploaded.last_seq;
                modifier.state.manifest.value.core.recent_snapshot_min_seq =
                    min_active_snapshot_seq.unwrap_or(uploaded.last_seq);

                modifier
                    .state
                    .manifest
                    .value
                    .core
                    .sequence_tracker
                    .extend_from(uploaded_tracker);
            }
            Ok(modifier.state.manifest.clone())
        })?;

        drop(guard);
        self.db.status_manager.report_manifest(manifest.into());
        Ok(())
    }

    async fn write_manifest_update_safely(
        &mut self,
        checkpoint_options: &[&CheckpointOptions],
    ) -> Result<Vec<CheckpointCreateResult>, SlateDBError> {
        loop {
            let result = self.write_manifest_update(checkpoint_options).await;
            if matches!(result, Err(SlateDBError::TransactionalObjectVersionExists)) {
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    async fn write_manifest_update(
        &mut self,
        checkpoint_options: &[&CheckpointOptions],
    ) -> Result<Vec<CheckpointCreateResult>, SlateDBError> {
        let mut dirty = self.clone_local_manifest_for_write();
        let mut checkpoint_results = Vec::new();
        for options in checkpoint_options {
            let id = self.db.rand.rng().gen_uuid();
            let checkpoint = self.manifest.new_checkpoint(id, options)?;
            let manifest_id = checkpoint.manifest_id;
            dirty.value.core.checkpoints.push(checkpoint);
            checkpoint_results.push(CheckpointCreateResult { id, manifest_id });
        }
        self.manifest.update(dirty).await?;
        Ok(checkpoint_results)
    }

    async fn write_current_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            let result = self.write_current_manifest().await;
            if matches!(result, Err(SlateDBError::TransactionalObjectVersionExists)) {
                self.load_manifest().await?;
            } else {
                return result;
            }
        }
    }

    async fn write_current_manifest(&mut self) -> Result<(), SlateDBError> {
        let dirty = self.clone_local_manifest_for_write();
        self.manifest.update(dirty.clone()).await?;
        self.db.status_manager.report_manifest(dirty.into());
        Ok(())
    }

    fn clone_local_manifest_for_write(
        &self,
    ) -> slatedb_txn_obj::DirtyObject<crate::manifest::Manifest> {
        let dirty = {
            let rguard_state = self.db.state.read();
            rguard_state.state().manifest.clone()
        };
        dirty
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
        self.manifest.refresh().await?;
        let remote_dirty = self.manifest.prepare_dirty()?;
        self.merge_remote_manifest(remote_dirty);
        if let Some(tx) = done {
            let _ = tx.send(Ok(()));
        }
        let _ = self.tracker_tx.send(TrackerMessage::ManifestRefreshed);
        Ok(())
    }

    fn merge_remote_manifest(
        &self,
        remote_dirty: slatedb_txn_obj::DirtyObject<crate::manifest::Manifest>,
    ) {
        let dirty_manifest = {
            let mut wguard_state = self.db.state.write();
            wguard_state.merge_remote_manifest(remote_dirty);
            let cow = wguard_state.state();
            self.db
                .db_stats
                .l0_sst_count
                .set(cow.core().tree.l0.len() as i64);
            cow.manifest.clone()
        };
        self.db
            .status_manager
            .report_manifest(dirty_manifest.into());
    }

    async fn write_checkpoint_safely(
        &mut self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        self.load_manifest().await?;
        let mut results = self.write_manifest_update_safely(&[options]).await?;
        Ok(results
            .pop()
            .expect("checkpoint write should return exactly one result"))
    }

    async fn finish_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        checkpoint_results: Vec<CheckpointCreateResult>,
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
        for (checkpoint, result) in attached_checkpoints
            .into_iter()
            .zip(checkpoint_results.into_iter())
        {
            debug!("checkpoint created [id={}]", result.id);
            let _ = checkpoint.sender.send(Ok(result));
        }
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
            uploaded
                .imm_memtable
                .table()
                .notify_durable(Err(err.clone()));
        }
        for checkpoint in attached_checkpoints {
            let _ = checkpoint.sender.send(Err(err.clone()));
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
            let _ = checkpoint.sender.send(Err(err.clone()));
        }
    }

    /// Extract flush/checkpoint waiters from a command without processing
    /// uploads. Used during error shutdown to ensure waiters get a proper error.
    fn collect_pending_waiter(&mut self, command: ManifestWriterCommand) {
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
            ManifestWriterCommand::CreateCheckpoint {
                through_seq,
                options,
                sender,
            } => {
                self.pending_checkpoints.push(PendingCheckpoint {
                    through_seq,
                    options,
                    sender,
                });
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

        // Persist the local manifest on shutdown to advance next_wal_sst_id
        // and any other locally updated fields. Skip if fenced since another
        // writer owns the manifest.
        if !matches!(result, Err(SlateDBError::Fenced)) {
            self.write_current_manifest_safely().await?;
        }

        Ok(())
    }
}

struct PendingFlush {
    through_seq: Option<u64>,
    sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
}

struct PendingCheckpoint {
    through_seq: Option<u64>,
    options: CheckpointOptions,
    sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
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

#[cfg(test)]
mod tests {
    use super::{ManifestWriter, TrackerMessage};
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_state::SsTableId;
    use crate::db_status::{ClosedResultWriter, DbStatusManager};
    use crate::error::SlateDBError;
    use crate::format::sst::SsTableFormat;
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::manifest::ManifestCore;
    use crate::memtable_flusher::uploader::{SegmentHandle, UploadedMemtable};
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::utils::{IdGenerator, WatchableOnceCell};
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::clock::SystemClock;
    use slatedb_common::metrics::MetricsRecorderHelper;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    struct StartedManifestWriter {
        writer: ManifestWriter,
        executor: crate::dispatcher::MessageHandlerExecutor,
        tracker_rx: async_channel::Receiver<TrackerMessage>,
        closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
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
        let writer = ManifestWriter::start(
            inner,
            manifest,
            poll_interval,
            &closed_result,
            &executor,
            &Handle::current(),
            tracker_tx,
        )
        .unwrap();
        executor.monitor_on(&Handle::current()).unwrap();
        StartedManifestWriter {
            writer,
            executor,
            tracker_rx,
            closed_result,
        }
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

    async fn setup_harness_with_extractor(
        path: &str,
        fp_registry: Arc<FailPointRegistry>,
        segment_extractor: Option<Arc<dyn crate::prefix_extractor::PrefixExtractor>>,
    ) -> TestHarness {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            PathResolver::new(Path::from(path.clone())),
            Arc::clone(&fp_registry),
            None,
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
                db_metrics,
                fp_registry,
                None,
                status_manager,
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
        let sst_id = SsTableId::Compacted(inner.rand.rng().gen_ulid(inner.system_clock.as_ref()));
        let sst_handle = inner
            .flush_imm_table(&sst_id, imm_memtable.table(), true)
            .await
            .unwrap();
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

        let (tx, rx) = tokio::sync::oneshot::channel();
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

    /// Construct an UploadedMemtable whose flush output spans multiple named
    /// segments. The same underlying SST is reused for each handle — the
    /// apply path routes by prefix and does not validate SST contents
    /// against the prefix.
    async fn next_uploaded_memtable_with_segments(
        inner: &Arc<DbInner>,
        key: &[u8],
        value: &[u8],
        prefixes: &[&[u8]],
    ) -> UploadedMemtable {
        let imm_memtable = freeze_imm(inner, key, value);
        let first_seq = imm_memtable.table().first_seq().unwrap();
        let last_seq = imm_memtable.table().last_seq().unwrap();
        let mut segments = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            let sst_id =
                SsTableId::Compacted(inner.rand.rng().gen_ulid(inner.system_clock.as_ref()));
            let sst_handle = inner
                .flush_imm_table(&sst_id, imm_memtable.table(), true)
                .await
                .unwrap();
            segments.push(SegmentHandle {
                prefix: Bytes::copy_from_slice(prefix),
                sst_handle,
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
}
