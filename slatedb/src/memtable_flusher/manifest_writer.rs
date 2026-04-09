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
    PollManifest,
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
            Self::PollManifest => write!(f, "PollManifest"),
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

    /// Requests an immediate manifest poll instead of waiting for the
    /// internal `manifest_poll_interval` ticker.
    pub(crate) fn poll_manifest_now(&self) -> Result<(), SlateDBError> {
        self.commands_tx.send(ManifestWriterCommand::PollManifest)
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
    /// Uploaded memtables waiting for contiguous ordering, keyed by first_seq.
    ready: BTreeMap<u64, UploadedMemtable>,
    /// The first_seq we expect for the next memtable to process.
    next_seq: u64,
    /// Highest last_seq that has been durably written to the manifest (inclusive).
    durable_seq: u64,
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
            Box::new(|| ManifestWriterCommand::PollManifest),
        )]
    }

    async fn handle(&mut self, command: ManifestWriterCommand) -> Result<(), SlateDBError> {
        match command {
            ManifestWriterCommand::Uploaded(uploaded_memtable) => {
                self.handle_uploaded(*uploaded_memtable).await
            }
            ManifestWriterCommand::AwaitFlush {
                through_seq,
                sender,
            } => {
                self.handle_flush(through_seq, sender);
                Ok(())
            }
            ManifestWriterCommand::CreateCheckpoint {
                through_seq,
                options,
                sender,
            } => {
                self.handle_create_checkpoint(through_seq, options, sender)
                    .await
            }
            ManifestWriterCommand::PollManifest => self.refresh_manifest_progress().await,
        }
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
        let next_seq = db.oracle.peek_next_seq();
        Self {
            db,
            manifest,
            manifest_poll_interval,
            tracker_tx,
            pending_flushes: Vec::new(),
            ready: BTreeMap::new(),
            next_seq,
            durable_seq,
            pending_checkpoints: Vec::new(),
        }
    }

    async fn handle_uploaded(
        &mut self,
        uploaded_memtable: UploadedMemtable,
    ) -> Result<(), SlateDBError> {
        assert!(
            uploaded_memtable.first_seq >= self.next_seq,
            "uploaded memtable first_seq ({}) is behind next_seq ({})",
            uploaded_memtable.first_seq,
            self.next_seq,
        );
        if self
            .ready
            .insert(uploaded_memtable.first_seq, uploaded_memtable)
            .is_some()
        {
            return Err(SlateDBError::InvalidDBState);
        }
        self.process_ready_work().await
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
        self.process_ready_work().await
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
        let mut next_seq = self.next_seq;
        let mut batch = Vec::new();
        while let Some(uploaded) = self.ready.remove(&next_seq) {
            next_seq = uploaded.last_seq + 1;
            batch.push(uploaded);
        }

        if batch.is_empty() {
            None
        } else {
            self.next_seq = next_seq;
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
        let _ = self.tracker_tx.send(TrackerMessage::ManifestRefreshed);
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

#[cfg(test)]
mod tests {
    use super::{ManifestWriter, TrackerMessage};
    use crate::config::{CheckpointOptions, Settings};
    use crate::db::DbInner;
    use crate::db_state::{ManifestCore, SsTableId};
    use crate::db_status::{ClosedResultWriter, DbStatusManager};
    use crate::error::SlateDBError;
    use crate::format::sst::SsTableFormat;
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::memtable_flusher::uploader::UploadedMemtable;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::rand::DbRand;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::utils::{IdGenerator, WatchableOnceCell};
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

    async fn next_uploaded_memtable(
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
    async fn should_wait_for_missing_seq_before_flushing() {
        let harness = setup_harness(
            "/tmp/test_parallel_l0_flush_manifest_writer_gap",
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
}
