//! Parallel L0 flush manifest manifest_writer.
//!
//! The manifest_writer owns ordered retirement of uploaded L0 tables:
//! - restore flush order using [`FlushEpoch`]
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

use super::uploader::UploadedMemtable;
use super::FlushEpoch;
use crate::checkpoint::CheckpointCreateResult;
use crate::config::CheckpointOptions;
use crate::db::DbInner;
use crate::db_state::SsTableView;
use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::utils::safe_mpsc;
use crate::utils::IdGenerator;
use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Result reported for a completed flush request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushResult {
    /// Highest durable WAL id covered by the completed flush.
    pub(crate) durable_through_wal_id: Option<u64>,
    /// Highest durable sequence number covered by the completed flush.
    pub(crate) durable_through_seq: Option<u64>,
}

/// Command submitted to the manifest_writer.
enum ManifestWriterCommand {
    /// One uploaded table is ready for ordered retirement.
    Uploaded(Box<UploadedMemtable>),
    /// Wait for an epoch to become durable, then respond with FlushResult.
    AwaitFlush {
        through_epoch: Option<FlushEpoch>,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    },
    /// Create a checkpoint against the current durable manifest state.
    CreateCheckpoint {
        through_epoch: Option<FlushEpoch>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    },
}

/// Event emitted by the manifest_writer.
#[derive(Clone, Debug)]
pub(crate) enum ManifestWriterEvent {
    /// Durable progress advanced through a new contiguous flush frontier.
    Flushed {
        /// Highest contiguous flush epoch durably reflected in the manifest.
        through_epoch: FlushEpoch,
    },
    /// Remote manifest changes were merged into local state.
    ManifestRefreshed,
}

/// Result of closing the manifest writer.
pub(crate) enum ManifestWriterCloseResult {
    /// Clean shutdown.
    Ok {
        last_durable_epoch: Option<FlushEpoch>,
    },
    /// Fenced by another writer.
    Fenced {
        last_durable_epoch: Option<FlushEpoch>,
    },
    /// Unexpected error. The durable frontier is unknown because the
    /// failed manifest write may have silently succeeded.
    Err(SlateDBError),
}

impl ManifestWriterCloseResult {
    /// Returns the error if the close was not clean.
    pub(crate) fn err(&self) -> Option<SlateDBError> {
        match self {
            ManifestWriterCloseResult::Ok { .. } => None,
            ManifestWriterCloseResult::Fenced { .. } => Some(SlateDBError::Fenced),
            ManifestWriterCloseResult::Err(err) => Some(err.clone()),
        }
    }
}

/// Ordered L0 retirement and manifest update subsystem.
pub(crate) struct ManifestWriter {
    commands_tx: safe_mpsc::SafeSender<ManifestWriterCommand>,
    events_rx: safe_mpsc::SafeReceiver<ManifestWriterEvent>,
    task: Option<JoinHandle<ManifestWriterCloseResult>>,
}

impl ManifestWriter {
    /// Starts the manifest_writer subsystem.
    pub(crate) fn start(
        db: Arc<DbInner>,
        manifest: FenceableManifest,
        manifest_poll_interval: Duration,
        handle: &Handle,
    ) -> Self {
        let (commands_tx, commands_rx) = safe_mpsc::unbounded_channel();
        let (events_tx, events_rx) = safe_mpsc::unbounded_channel();
        let task = handle.spawn(
            ManifestWriterTask::new(db, manifest, manifest_poll_interval, commands_rx, events_tx)
                .run(),
        );

        Self {
            commands_tx,
            events_rx,
            task: Some(task),
        }
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
    /// once `through_epoch` is durable (or immediately if `None`).
    pub(crate) fn send_flush(
        &self,
        through_epoch: Option<FlushEpoch>,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        self.commands_tx.send(ManifestWriterCommand::AwaitFlush {
            through_epoch,
            sender,
        })
    }

    /// Sends a checkpoint request to the manifest_writer. The manifest_writer will write
    /// the checkpoint once `through_epoch` is durable (or immediately if
    /// `None`) and respond via `sender`.
    pub(crate) fn send_checkpoint(
        &self,
        through_epoch: Option<FlushEpoch>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        self.commands_tx
            .send(ManifestWriterCommand::CreateCheckpoint {
                through_epoch,
                options,
                sender,
            })
    }

    /// Returns the shared manifest_writer event receiver.
    pub(crate) fn events(&mut self) -> &mut safe_mpsc::SafeReceiver<ManifestWriterEvent> {
        &mut self.events_rx
    }

    /// Closes the manifest_writer.
    pub(crate) async fn close(&mut self) -> ManifestWriterCloseResult {
        self.commands_tx.close();
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(result) => result,
                Err(join_err) if join_err.is_cancelled() => ManifestWriterCloseResult::Ok {
                    last_durable_epoch: None,
                },
                Err(join_err) if join_err.is_panic() => ManifestWriterCloseResult::Err(
                    SlateDBError::BackgroundTaskPanic("parallel_l0_flush_manifest_writer".into()),
                ),
                Err(_) => ManifestWriterCloseResult::Err(SlateDBError::BackgroundTaskCancelled(
                    "parallel_l0_flush_manifest_writer".into(),
                )),
            }
        } else {
            ManifestWriterCloseResult::Ok {
                last_durable_epoch: None,
            }
        }
    }
}

struct ManifestWriterTask {
    db: Arc<DbInner>,
    manifest: FenceableManifest,
    manifest_poll_interval: Duration,
    commands_rx: safe_mpsc::SafeReceiver<ManifestWriterCommand>,
    events_tx: safe_mpsc::SafeSender<ManifestWriterEvent>,
    ready: BTreeMap<FlushEpoch, UploadedMemtable>,
    next_epoch: FlushEpoch,
    durable_through: Option<(FlushEpoch, u64)>,
    durable_wal_id: Option<u64>,
    pending_flushes: Vec<PendingFlush>,
    pending_checkpoints: Vec<PendingCheckpoint>,
}

impl ManifestWriterTask {
    fn new(
        db: Arc<DbInner>,
        manifest: FenceableManifest,
        manifest_poll_interval: Duration,
        commands_rx: safe_mpsc::SafeReceiver<ManifestWriterCommand>,
        events_tx: safe_mpsc::SafeSender<ManifestWriterEvent>,
    ) -> Self {
        Self {
            db,
            manifest,
            manifest_poll_interval,
            commands_rx,
            events_tx,
            durable_wal_id: None,
            pending_flushes: Vec::new(),
            ready: BTreeMap::new(),
            next_epoch: FlushEpoch(1),
            durable_through: None,
            pending_checkpoints: Vec::new(),
        }
    }

    async fn run(mut self) -> ManifestWriterCloseResult {
        let clock = Arc::clone(&self.db.system_clock);
        let mut poll = clock.ticker(self.manifest_poll_interval);
        let result = loop {
            tokio::select! {
                _ = poll.tick() => {
                    if let Err(err) = self.refresh_manifest_progress().await {
                        break Err(err);
                    }
                }
                maybe_command = self.commands_rx.recv() => {
                    let Some(command) = maybe_command else {
                        let _ = self.write_current_manifest_safely().await;
                        break Ok(());
                    };
                    let commands = self.drain_ready_commands(command);
                    if let Err(err) = self.handle_commands(commands).await {
                        break Err(err);
                    }
                }
            }
        };
        let last_durable_epoch = self.durable_through.map(|(epoch, _)| epoch);
        self.fail_pending_flushes(&result, last_durable_epoch);
        self.fail_pending_checkpoints(&result);
        match result {
            Ok(()) => ManifestWriterCloseResult::Ok { last_durable_epoch },
            Err(SlateDBError::Fenced) => ManifestWriterCloseResult::Fenced { last_durable_epoch },
            Err(err) => ManifestWriterCloseResult::Err(err),
        }
    }

    fn drain_ready_commands(
        &mut self,
        first_command: ManifestWriterCommand,
    ) -> Vec<ManifestWriterCommand> {
        let mut commands = vec![first_command];
        while let Some(command) = self.commands_rx.try_recv() {
            commands.push(command);
        }
        commands
    }

    async fn handle_commands(
        &mut self,
        commands: Vec<ManifestWriterCommand>,
    ) -> Result<(), SlateDBError> {
        for command in commands {
            match command {
                ManifestWriterCommand::Uploaded(uploaded_memtable) => {
                    self.handle_uploaded(*uploaded_memtable).await?;
                }
                ManifestWriterCommand::AwaitFlush {
                    through_epoch,
                    sender,
                } => {
                    self.handle_flush(through_epoch, sender);
                }
                ManifestWriterCommand::CreateCheckpoint {
                    through_epoch,
                    options,
                    sender,
                } => {
                    self.handle_create_checkpoint(through_epoch, options, sender)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_uploaded(
        &mut self,
        uploaded_memtable: UploadedMemtable,
    ) -> Result<(), SlateDBError> {
        if self
            .ready
            .insert(uploaded_memtable.epoch, uploaded_memtable)
            .is_some()
        {
            return Err(SlateDBError::InvalidDBState);
        }
        self.process_ready_work().await
    }

    fn handle_flush(
        &mut self,
        through_epoch: Option<FlushEpoch>,
        sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
    ) {
        if self.flush_requirement_satisfied(through_epoch) {
            let _ = sender.send(Ok(self.flush_result()));
        } else {
            self.pending_flushes.push(PendingFlush {
                through_epoch,
                sender,
            });
        }
    }

    fn flush_requirement_satisfied(&self, through_epoch: Option<FlushEpoch>) -> bool {
        match through_epoch {
            None => true,
            Some(epoch) => self
                .durable_through
                .is_some_and(|(durable_epoch, _)| durable_epoch >= epoch),
        }
    }

    fn flush_result(&self) -> FlushResult {
        FlushResult {
            durable_through_wal_id: self.durable_wal_id,
            durable_through_seq: self.durable_through.map(|(_, seq)| seq),
        }
    }

    async fn handle_create_checkpoint(
        &mut self,
        through_epoch: Option<FlushEpoch>,
        options: CheckpointOptions,
        sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
    ) -> Result<(), SlateDBError> {
        if self.checkpoint_requirement_satisfied(through_epoch) {
            let result = self.write_checkpoint_safely(&options).await;
            let _ = sender.send(result.clone());
            return result.map(|_| ());
        }

        self.pending_checkpoints.push(PendingCheckpoint {
            through_epoch,
            options,
            sender,
        });
        self.process_ready_work().await
    }

    async fn process_ready_work(&mut self) -> Result<(), SlateDBError> {
        loop {
            let Some(staged_batch) = self.take_next_ready_batch() else {
                return Ok(());
            };
            let (through_epoch, through_seq) = staged_batch
                .last()
                .map(|uploaded| (uploaded.epoch, uploaded.last_seq))
                .expect("staged batch should not be empty");
            let attached_checkpoints = self.take_satisfied_pending_checkpoints(through_epoch);
            self.apply_ready_batch(
                staged_batch,
                attached_checkpoints,
                through_epoch,
                through_seq,
            )
            .await?;
        }
    }

    fn take_next_ready_batch(&mut self) -> Option<Vec<UploadedMemtable>> {
        let mut epoch = self.next_epoch;
        let mut batch = Vec::new();
        while let Some(uploaded) = self.ready.remove(&epoch) {
            batch.push(uploaded);
            epoch = FlushEpoch(epoch.0 + 1);
        }

        if batch.is_empty() {
            None
        } else {
            self.next_epoch = epoch;
            Some(batch)
        }
    }

    fn take_satisfied_pending_checkpoints(
        &mut self,
        through_epoch: FlushEpoch,
    ) -> Vec<PendingCheckpoint> {
        let mut satisfied = Vec::new();
        let mut pending = Vec::with_capacity(self.pending_checkpoints.len());
        for checkpoint in self.pending_checkpoints.drain(..) {
            if checkpoint
                .through_epoch
                .is_none_or(|required_epoch| required_epoch <= through_epoch)
            {
                satisfied.push(checkpoint);
            } else {
                pending.push(checkpoint);
            }
        }
        self.pending_checkpoints = pending;
        satisfied
    }

    fn checkpoint_requirement_satisfied(&self, through_epoch: Option<FlushEpoch>) -> bool {
        match through_epoch {
            None => true,
            Some(required_epoch) => self
                .durable_through
                .is_some_and(|(durable_epoch, _)| durable_epoch >= required_epoch),
        }
    }

    async fn apply_ready_batch(
        &mut self,
        staged_batch: Vec<UploadedMemtable>,
        attached_checkpoints: Vec<PendingCheckpoint>,
        through_epoch: FlushEpoch,
        through_seq: u64,
    ) -> Result<(), SlateDBError> {
        self.apply_uploaded_state(&staged_batch)?;

        for uploaded in &staged_batch {
            uploaded.imm_memtable.notify_flush_to_l0(Ok(()));
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
                    through_epoch,
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
        let min_active_snapshot_seq = self.db.txn_manager.min_active_seq();
        let mut guard = self.db.state.write();
        guard.modify(|modifier| {
            for uploaded in staged_batch {
                let uploaded_tracker = uploaded.imm_memtable.sequence_tracker();
                let popped = modifier
                    .state
                    .imm_memtable
                    .pop_back()
                    .expect("expected imm memtable");
                assert!(Arc::ptr_eq(&popped, &uploaded.imm_memtable));
                modifier
                    .state
                    .manifest
                    .value
                    .core
                    .l0
                    .push_front(SsTableView::new(
                        self.db.rand.rng().gen_ulid(self.db.system_clock.as_ref()),
                        uploaded.sst_handle.clone(),
                    ));
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

                assert!(uploaded.last_seq >= modifier.state.manifest.value.core.last_l0_seq);
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
            Ok(())
        })
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
        self.manifest.update(dirty).await
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

    async fn refresh_manifest_progress(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        let remote_dirty = self.manifest.prepare_dirty()?;
        self.merge_remote_manifest(remote_dirty);
        let _ = self.events_tx.send(ManifestWriterEvent::ManifestRefreshed);
        Ok(())
    }

    fn merge_remote_manifest(
        &self,
        remote_dirty: slatedb_txn_obj::DirtyObject<crate::manifest::Manifest>,
    ) {
        let mut wguard_state = self.db.state.write();
        wguard_state.merge_remote_manifest(remote_dirty);
        self.db
            .db_stats
            .l0_sst_count
            .set(wguard_state.state().core().l0.len() as i64);
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
        through_epoch: FlushEpoch,
        through_seq: u64,
    ) -> Result<(), SlateDBError> {
        debug!(
            "l0 flush batch written to manifest [through_epoch={:?}, batch_size={}, through_seq={}]",
            through_epoch,
            staged_batch.len(),
            through_seq,
        );
        self.durable_through = Some((through_epoch, through_seq));
        for uploaded in &staged_batch {
            self.durable_wal_id = Some(uploaded.imm_memtable.recent_flushed_wal_id());
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
            .events_tx
            .send(ManifestWriterEvent::Flushed { through_epoch });
        Ok(())
    }

    fn resolve_pending_flushes(&mut self) {
        let flush_result = self.flush_result();
        let pending = std::mem::take(&mut self.pending_flushes);
        let mut still_pending = Vec::with_capacity(pending.len());
        for flush in pending {
            if self.flush_requirement_satisfied(flush.through_epoch) {
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

    /// Resolve pending flush waiters on exit. Flushes whose target epoch was
    /// reached get `Ok`; others get `Closed` (clean exit) or the specific error.
    fn fail_pending_flushes(
        &mut self,
        result: &Result<(), SlateDBError>,
        last_durable_epoch: Option<FlushEpoch>,
    ) {
        let flush_result = self.flush_result();
        for flush in self.pending_flushes.drain(..) {
            let response = match result {
                Ok(()) => {
                    let reached = flush
                        .through_epoch
                        .is_none_or(|epoch| last_durable_epoch.is_some_and(|d| d >= epoch));
                    if reached {
                        Ok(flush_result.clone())
                    } else {
                        Err(SlateDBError::Closed)
                    }
                }
                Err(err) => Err(err.clone()),
            };
            let _ = flush.sender.send(response);
        }
    }

    /// Fail pending checkpoint waiters on exit. Checkpoints require a manifest
    /// write which cannot happen during shutdown, so they always fail.
    fn fail_pending_checkpoints(&mut self, result: &Result<(), SlateDBError>) {
        for checkpoint in self.pending_checkpoints.drain(..) {
            let err = match result {
                Ok(()) => SlateDBError::Closed,
                Err(err) => err.clone(),
            };
            let _ = checkpoint.sender.send(Err(err));
        }
    }
}

struct PendingFlush {
    through_epoch: Option<FlushEpoch>,
    sender: oneshot::Sender<Result<FlushResult, SlateDBError>>,
}

struct PendingCheckpoint {
    through_epoch: Option<FlushEpoch>,
    options: CheckpointOptions,
    sender: oneshot::Sender<Result<CheckpointCreateResult, SlateDBError>>,
}

#[cfg(test)]
mod tests {
    use super::{
        FlushEpoch, ManifestWriter, ManifestWriterCloseResult, ManifestWriterCommand,
        ManifestWriterEvent,
    };
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_state::{ManifestCore, SsTableId};
    use crate::format::sst::SsTableFormat;
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::memtable_flusher::uploader::UploadedMemtable;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::utils::IdGenerator;
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
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    async fn assert_no_flush_event(manifest_writer: &mut ManifestWriter, duration: Duration) {
        let deadline = tokio::time::Instant::now() + duration;
        loop {
            match timeout(
                deadline - tokio::time::Instant::now(),
                manifest_writer.events().recv(),
            )
            .await
            {
                Err(_) => return, // timed out — no flush event, as expected
                Ok(Some(ManifestWriterEvent::ManifestRefreshed)) => continue,
                Ok(Some(ManifestWriterEvent::Flushed { .. })) => {
                    panic!("unexpected flushed event")
                }
                Ok(None) => panic!("manifest_writer event channel closed"),
            }
        }
    }

    async fn expect_flushed(manifest_writer: &mut ManifestWriter) -> FlushEpoch {
        loop {
            let event = timeout(Duration::from_secs(5), manifest_writer.events().recv())
                .await
                .expect("timed out waiting for flushed event")
                .expect("manifest_writer event channel closed");
            match event {
                ManifestWriterEvent::Flushed { through_epoch } => return through_epoch,
                ManifestWriterEvent::ManifestRefreshed => continue,
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
        let (write_tx, _) = mpsc::unbounded_channel();
        let inner = Arc::new(
            DbInner::new(
                settings.clone(),
                Arc::clone(&system_clock),
                Arc::clone(&rand),
                Arc::clone(&table_store),
                manifest_dirty,
                Arc::new(crate::memtable_flusher::MemtableFlusher::new()),
                write_tx,
                db_metrics,
                fp_registry,
                None,
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
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        manifest.core.checkpoints.len()
    }

    fn freeze_imm(
        harness: &TestHarness,
        key: &[u8],
        value: &[u8],
        seq: u64,
    ) -> Arc<crate::mem_table::ImmutableMemtable> {
        let mut guard = harness.inner.state.write();
        guard.memtable().put(RowEntry::new_value(key, value, seq));
        guard.freeze_memtable(0).unwrap();
        guard.state().imm_memtable.front().cloned().unwrap()
    }

    async fn next_uploaded_memtable(
        harness: &TestHarness,
        epoch: u64,
        key: &[u8],
        value: &[u8],
        seq: u64,
    ) -> UploadedMemtable {
        let imm_memtable = freeze_imm(harness, key, value, seq);
        let sst_id = SsTableId::Compacted(
            harness
                .inner
                .rand
                .rng()
                .gen_ulid(harness.inner.system_clock.as_ref()),
        );
        let sst_handle = harness
            .inner
            .flush_imm_table(&sst_id, imm_memtable.table(), true)
            .await
            .unwrap();
        let last_seq = imm_memtable.table().last_seq().unwrap();
        UploadedMemtable::new(FlushEpoch(epoch), imm_memtable, sst_handle, last_seq)
    }

    #[tokio::test]
    async fn should_emit_flushed_event_for_contiguous_uploads() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_flush_event",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;

        let mut manifest_writer = ManifestWriter::start(
            Arc::clone(&harness.inner),
            harness.manifest,
            Duration::from_secs(3600),
            &Handle::current(),
        );
        manifest_writer.notify_uploaded(uploaded).await.unwrap();

        let through_epoch = expect_flushed(&mut manifest_writer).await;
        assert_eq!(through_epoch, FlushEpoch(1));

        assert!(matches!(
            manifest_writer.close().await,
            ManifestWriterCloseResult::Ok { .. }
        ));
    }

    #[tokio::test]
    async fn should_wait_for_missing_epoch_before_flushing() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_gap",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded1 = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;
        let uploaded2 = next_uploaded_memtable(&harness, 2, b"k2", b"v2", 2).await;

        let mut manifest_writer = ManifestWriter::start(
            Arc::clone(&harness.inner),
            harness.manifest,
            Duration::from_secs(3600),
            &Handle::current(),
        );
        manifest_writer.notify_uploaded(uploaded2).await.unwrap();
        assert_no_flush_event(&mut manifest_writer, Duration::from_millis(100)).await;

        manifest_writer.notify_uploaded(uploaded1).await.unwrap();
        let through_epoch = expect_flushed(&mut manifest_writer).await;
        assert_eq!(through_epoch, FlushEpoch(2));

        assert!(matches!(
            manifest_writer.close().await,
            ManifestWriterCloseResult::Ok { .. }
        ));
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
        let mut manifest_writer = ManifestWriter::start(
            Arc::clone(&harness.inner),
            harness.manifest,
            Duration::from_secs(3600),
            &Handle::current(),
        );

        let (tx, rx) = tokio::sync::oneshot::channel();
        manifest_writer
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

        assert!(matches!(
            manifest_writer.close().await,
            ManifestWriterCloseResult::Ok { .. }
        ));
    }

    #[tokio::test]
    async fn should_wait_for_checkpoint_barrier_and_attach_to_flush_batch() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_checkpoint_barrier",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;

        let before =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        let mut manifest_writer = ManifestWriter::start(
            Arc::clone(&harness.inner),
            harness.manifest,
            Duration::from_secs(3600),
            &Handle::current(),
        );

        let (tx, rx) = oneshot::channel();
        manifest_writer
            .commands_tx
            .send(ManifestWriterCommand::CreateCheckpoint {
                through_epoch: Some(FlushEpoch(1)),
                options: CheckpointOptions::default(),
                sender: tx,
            })
            .unwrap();

        tokio::task::yield_now().await;
        assert_no_flush_event(&mut manifest_writer, Duration::from_millis(100)).await;

        manifest_writer.notify_uploaded(uploaded).await.unwrap();

        let through_epoch = expect_flushed(&mut manifest_writer).await;
        assert_eq!(through_epoch, FlushEpoch(1));

        let checkpoint = rx.await.unwrap().unwrap();
        let after =
            latest_manifest_checkpoint_count(&harness.path, Arc::clone(&harness.object_store))
                .await;
        assert_eq!(after, before + 1);
        assert!(checkpoint.manifest_id > 0);

        assert!(matches!(
            manifest_writer.close().await,
            ManifestWriterCloseResult::Ok { .. }
        ));
    }

    #[tokio::test]
    async fn should_emit_fatal_event_when_manifest_writer_is_fenced() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_fenced",
            Arc::new(FailPointRegistry::new()),
        )
        .await;
        let uploaded = next_uploaded_memtable(&harness, 1, b"k1", b"v1", 1).await;

        let mut manifest_writer = ManifestWriter::start(
            Arc::clone(&harness.inner),
            harness.manifest,
            Duration::from_secs(3600),
            &Handle::current(),
        );

        let _fence = load_writer_manifest(&harness.path, Arc::clone(&harness.object_store)).await;
        manifest_writer.notify_uploaded(uploaded).await.unwrap();

        // The manifest writer exits on fence — events channel closes.
        let event = timeout(Duration::from_secs(5), manifest_writer.events().recv())
            .await
            .unwrap();
        assert!(event.is_none());

        // close() returns Fenced.
        assert!(matches!(
            manifest_writer.close().await,
            ManifestWriterCloseResult::Fenced { .. }
        ));
    }
}
