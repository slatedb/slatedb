use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_skiplist::SkipMap;
use log::{debug, error, info, warn};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use ulid::Ulid;
use uuid::Uuid;

use crate::clock::SystemClock;
use crate::compactor::stats::CompactionStats;
use crate::compactor_executor::{CompactionExecutor, CompactionJob, TokioCompactionExecutor};
use crate::compactor_state::{Compaction, CompactorState};
use crate::config::{CheckpointOptions, CompactorOptions};
use crate::db_state::{SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::rand::DbRand;
pub use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::utils::IdGenerator;

pub trait CompactionSchedulerSupplier: Send + Sync {
    fn compaction_scheduler(
        &self,
        options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync>;
}

pub trait CompactionScheduler: Send + Sync {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction>;
}

/// Tracks progress of various compactions. Progress is updated from the executor with
/// [`CompactionProgressTracker::update_progress`]. This isn't guaranteed to be fully accurate.
///
/// **WARN: This will definitely be inaccurate if compression is used.**
pub(crate) struct CompactionProgressTracker {
    /// Tracks the number of bytes processed and total bytes (all source SSTs and sorted runs)
    /// for each compaction job. Can be an estimate.
    ///
    /// (processed_bytes, total_bytes)
    processed_bytes: SkipMap<Uuid, (u64, u64)>,
}

impl CompactionProgressTracker {
    pub fn new() -> Self {
        Self {
            processed_bytes: SkipMap::new(),
        }
    }

    /// Adds a new compaction job to the tracker.
    ///
    /// # Arguments
    /// * `id` - The ID of the compaction job.
    /// * `total_bytes` - The total number of bytes to be processed for the compaction job.
    pub fn add_job(&mut self, id: Uuid, total_bytes: u64) {
        self.processed_bytes.insert(id, (0, total_bytes));
    }

    /// Removes a compaction job from the tracker.
    ///
    /// # Arguments
    /// * `id` - The ID of the compaction job.
    pub fn remove_job(&mut self, id: Uuid) {
        self.processed_bytes.remove(&id);
    }

    /// Overwrites the progress for a compaction job with the latest processed bytes.
    ///
    /// # Arguments
    /// * `id` - The ID of the compaction job.
    /// * `bytes_processed` - The total number of bytes processed so far.
    pub fn update_progress(&mut self, id: Uuid, bytes_processed: u64) {
        if let Some((_, total_bytes)) = self.processed_bytes.get(&id).map(|entry| *entry.value()) {
            self.processed_bytes
                .insert(id, (bytes_processed, total_bytes));
        } else {
            warn!(id:%; "compaction progress tracker missing for job");
        }
    }

    /// Outputs the progress of each compaction job to debug.
    pub fn log_progress(&self) {
        for entry in self.processed_bytes.iter() {
            let id = entry.key();
            let (processed_bytes, total_bytes) = entry.value();
            let percentage = (processed_bytes * 100 / total_bytes) as u32;
            debug!(
                id:%,
                current_percentage:% = format!("{percentage}%"),
                processed_bytes,
                estimated_total_bytes = total_bytes;
                "compaction progress"
            );
        }
    }
}

pub(crate) enum WorkerToOrchestratorMsg {
    CompactionFinished {
        id: Uuid,
        result: Result<SortedRun, SlateDBError>,
    },
    /// Sent when an [`CompactionExecutor`] wishes to alert the compactor of progress. This
    /// information is only used for reporting purposes, and can be an estimate.
    CompactionProgress {
        id: Uuid,
        /// The total number of bytes processed so far.
        bytes_processed: u64,
    },
}

/// The compactor is responsible for taking groups of sorted runs (this doc uses the term
/// sorted run to refer to both sorted runs and l0 ssts) and compacting them together to
/// reduce space amplification (by removing old versions of rows that have been updated/deleted)
/// and read amplification (by reducing the number of sorted runs that need to be searched on
/// a read). It's made up of a few different components:
///
/// - [`Compactor`]: The main event loop that orchestrates the compaction process.
/// - [`CompactorEventHandler`]: The event handler that handles events from the compactor.
/// - [`CompactionScheduler`]: The scheduler that discovers compactions that should be performed.
/// - [`CompactionExecutor`]: The executor that runs the compaction tasks.
///
/// The main event loop listens on the manifest poll ticker to react to manifest poll ticks, the
/// executor worker channel to react to updates about running compactions, and the shutdown
/// channel to discover when it should terminate. It doesn't actually implement the logic for
/// reacting to these events. This is implemented by [`CompactorEventHandler`].
///
/// The Scheduler is responsible for deciding what sorted runs should be compacted together.
/// It implements the [`CompactionScheduler`] trait. The implementation is specified by providing
/// an implementation of [`crate::config::CompactionSchedulerSupplier`] so different scheduling
/// policies can be plugged into slatedb. Currently, the only implemented policy is the size-tiered
/// scheduler supplied by [`crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier`]
///
/// The Executor does the actual work of compacting sorted runs by sort-merging them into a new
/// sorted run. It implements the [`CompactionExecutor`] trait. Currently, the only implementation
/// is the [`TokioCompactionExecutor`] which runs compaction on a local tokio runtime.
#[derive(Clone)]
pub(crate) struct Compactor {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: Arc<CompactorOptions>,
    scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    system_clock: Arc<dyn SystemClock>,
    cancellation_token: CancellationToken,
}

impl Compactor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: CompactorOptions,
        scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
        rand: Arc<DbRand>,
        stat_registry: Arc<StatRegistry>,
        system_clock: Arc<dyn SystemClock>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let stats = Arc::new(CompactionStats::new(stat_registry));
        Self {
            manifest_store,
            table_store,
            options: Arc::new(options),
            scheduler_supplier,
            rand,
            stats,
            cancellation_token,
            system_clock,
        }
    }

    /// Starts the compactor. This method performs the actual compaction event loop.
    /// The compactor runs until the cancellation token is cancelled. The compactor's
    /// event loop always runs on the current runtime, while the compactor executor
    /// runs on the provided runtime. This is to keep long-running compaction tasks
    /// from blocking the main runtime.
    ///
    /// ## Arguments
    /// * `compactor_runtime` - The runtime to use for running the compactor executor.
    ///
    /// ## Returns
    /// * `Result<(), SlateDBError>` - The result of the compaction event loop.
    pub async fn run_async_task(&self, compactor_runtime: Handle) -> Result<(), SlateDBError> {
        let mut db_runs_log_ticker = self.system_clock.ticker(Duration::from_secs(10));
        let mut manifest_poll_ticker = self.system_clock.ticker(self.options.poll_interval);
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::unbounded_channel();
        let scheduler = Arc::from(self.scheduler_supplier.compaction_scheduler(&self.options));
        let executor = Arc::new(TokioCompactionExecutor::new(
            compactor_runtime,
            self.options.clone(),
            worker_tx,
            self.table_store.clone(),
            self.rand.clone(),
            self.stats.clone(),
            self.system_clock.clone(),
        ));
        let mut handler = CompactorEventHandler::new(
            self.manifest_store.clone(),
            self.options.clone(),
            scheduler,
            executor,
            self.rand.clone(),
            self.stats.clone(),
            self.system_clock.clone(),
        )
        .await?;

        // Stop the loop when the executor is shut down *and* all remaining
        // `worker_rx` messages have been drained.
        while !(self.cancellation_token.is_cancelled() && worker_rx.is_empty()) {
            tokio::select! {
                biased;
                // check the cancellation token first to avoid starting new compaction tasks when the runtime is shutting down
                _ = self.cancellation_token.cancelled() => {
                    // Stop the executor. Don't return because there might
                    // still be messages in `worker_rx`. Let the loop continue
                    // to drain them until empty.
                    handler.stop_executor().await?;
                }
                _ = db_runs_log_ticker.tick() => {
                    handler.handle_log_ticker();
                }
                _ = manifest_poll_ticker.tick() => {
                    handler.handle_ticker().await;
                }
                msg = worker_rx.recv() => {
                    handler.handle_worker_rx(msg.expect("fatal error receiving worker msg")).await;
                }
            }
        }

        Ok(())
    }
}

struct CompactorEventHandler {
    state: CompactorState,
    manifest: FenceableManifest,
    options: Arc<CompactorOptions>,
    scheduler: Arc<dyn CompactionScheduler + Send + Sync>,
    executor: Arc<dyn CompactionExecutor + Send + Sync>,
    rand: Arc<DbRand>,
    stats: Arc<CompactionStats>,
    system_clock: Arc<dyn SystemClock>,
    progress_tracker: CompactionProgressTracker,
}

impl CompactorEventHandler {
    async fn new(
        manifest_store: Arc<ManifestStore>,
        options: Arc<CompactorOptions>,
        scheduler: Arc<dyn CompactionScheduler + Send + Sync>,
        executor: Arc<dyn CompactionExecutor + Send + Sync>,
        rand: Arc<DbRand>,
        stats: Arc<CompactionStats>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let stored_manifest = StoredManifest::load(manifest_store.clone()).await?;
        let manifest = FenceableManifest::init_compactor(
            stored_manifest,
            options.manifest_update_timeout,
            system_clock.clone(),
        )
        .await?;
        let state = CompactorState::new(manifest.prepare_dirty()?);
        Ok(Self {
            state,
            manifest,
            options,
            scheduler,
            executor,
            rand,
            stats,
            system_clock,
            progress_tracker: CompactionProgressTracker::new(),
        })
    }

    fn handle_log_ticker(&self) {
        self.log_compaction_state();
        self.progress_tracker.log_progress();
    }

    async fn handle_ticker(&mut self) {
        if !self.is_executor_stopped() {
            self.load_manifest()
                .await
                .expect("fatal error loading manifest");
        }
    }

    async fn handle_worker_rx(&mut self, msg: WorkerToOrchestratorMsg) {
        match msg {
            WorkerToOrchestratorMsg::CompactionFinished { id, result } => match result {
                Ok(sr) => self
                    .finish_compaction(id, sr)
                    .await
                    .expect("fatal error finishing compaction"),
                Err(err) => {
                    error!("error executing compaction: {:#?}", err);
                    self.finish_failed_compaction(id);
                }
            },
            WorkerToOrchestratorMsg::CompactionProgress {
                id,
                bytes_processed,
            } => {
                self.progress_tracker.update_progress(id, bytes_processed);
            }
        }
    }

    async fn stop_executor(&self) -> Result<(), SlateDBError> {
        let this_executor = self.executor.clone();
        // Explicitly allow spawn_blocking for compactors since we can't trust them
        // not to block the runtime. This could cause non-determinism, since it creates
        // a race between the executor's first .await call and the runtime awaiting
        // on the join handle. We use tokio::spawn for DST since we need full determinism.
        #[cfg(not(dst))]
        #[allow(clippy::disallowed_methods)]
        let result = tokio::task::spawn_blocking(move || {
            this_executor.stop();
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        #[cfg(dst)]
        let result = tokio::spawn(async move {
            this_executor.stop();
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        result
    }

    fn is_executor_stopped(&self) -> bool {
        self.executor.is_stopped()
    }

    async fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.manifest.refresh().await?;
        self.refresh_db_state().await?;
        Ok(())
    }

    async fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        // write the checkpoint first so that it points to the manifest with the ssts
        // being removed
        let checkpoint_id = self.rand.rng().gen_uuid();
        self.manifest
            .write_checkpoint(
                checkpoint_id,
                &CheckpointOptions {
                    // TODO(rohan): for now, just write a checkpoint with 15-minute expiry
                    //              so that it's extremely unlikely for the gc to delete ssts
                    //              out from underneath the writer. In a follow up, we'll write
                    //              a checkpoint with no expiry and with metadata indicating its
                    //              a compactor checkpoint. Then, the gc will delete the checkpoint
                    //              based on a configurable timeout
                    lifetime: Some(Duration::from_secs(900)),
                    ..CheckpointOptions::default()
                },
            )
            .await?;
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        let dirty = self.state.manifest().clone();
        self.manifest.update_manifest(dirty).await
    }

    async fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.load_manifest().await?;
            match self.write_manifest().await {
                Ok(_) => return Ok(()),
                Err(SlateDBError::ManifestVersionExists) => {
                    warn!("conflicting manifest version. updating and retrying write again.");
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn maybe_schedule_compactions(&mut self) -> Result<(), SlateDBError> {
        let compactions = self.scheduler.maybe_schedule_compaction(&self.state);
        for compaction in compactions.iter() {
            if self.state.num_compactions() >= self.options.max_concurrent_compactions {
                info!(
                    "already running {} compactions, which is at the max {}. Won't run compaction {:?}",
                    self.state.num_compactions(),
                    self.options.max_concurrent_compactions,
                    compaction
                );
                break;
            }
            self.submit_compaction(compaction.clone()).await?;
        }
        Ok(())
    }

    async fn start_compaction(
        &mut self,
        id: Uuid,
        compaction: Compaction,
    ) -> Result<(), SlateDBError> {
        self.log_compaction_state();
        let db_state = self.state.db_state();
        let compacted_sst_iter = db_state.compacted.iter().flat_map(|sr| sr.ssts.iter());
        let ssts_by_id: HashMap<Ulid, &SsTableHandle> = db_state
            .l0
            .iter()
            .chain(compacted_sst_iter)
            .map(|sst| (sst.id.unwrap_compacted_id(), sst))
            .collect();
        let srs_by_id: HashMap<u32, &SortedRun> =
            db_state.compacted.iter().map(|sr| (sr.id, sr)).collect();
        let ssts: Vec<SsTableHandle> = compaction
            .sources
            .iter()
            .filter_map(|s| s.maybe_unwrap_sst())
            .filter_map(|ulid| ssts_by_id.get(&ulid).map(|t| (*t).clone()))
            .collect();
        let sorted_runs: Vec<SortedRun> = compaction
            .sources
            .iter()
            .filter_map(|s| s.maybe_unwrap_sorted_run())
            .filter_map(|id| srs_by_id.get(&id).map(|t| (*t).clone()))
            .collect();
        // if there are no SRs when we compact L0 then the resulting SR is the last sorted run.
        let is_dest_last_run = db_state.compacted.is_empty()
            || db_state
                .compacted
                .last()
                .is_some_and(|sr| compaction.destination == sr.id);
        let job = CompactionJob {
            id,
            destination: compaction.destination,
            ssts,
            sorted_runs,
            compaction_ts: db_state.last_l0_clock_tick,
            is_dest_last_run,
        };
        self.progress_tracker
            .add_job(id, job.estimated_source_bytes());
        let this_executor = self.executor.clone();
        // Explicitly allow spawn_blocking for compactors since we can't trust them
        // not to block the runtime. This could cause non-determinism, since it creates
        // a race between the executor's first .await call and the runtime awaiting
        // on the join handle. We use tokio::spawn for DST since we need full determinism.
        #[cfg(not(dst))]
        #[allow(clippy::disallowed_methods)]
        let result = tokio::task::spawn_blocking(move || {
            this_executor.start_compaction(job);
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        #[cfg(dst)]
        let result = tokio::spawn(async move {
            this_executor.start_compaction(job);
        })
        .await
        .map_err(|_| SlateDBError::CompactionExecutorFailed);
        result
    }

    // state writers
    fn finish_failed_compaction(&mut self, id: Uuid) {
        self.state.finish_failed_compaction(id);
        self.progress_tracker.remove_job(id);
    }

    #[instrument(level = "debug", skip_all, fields(id = %id))]
    async fn finish_compaction(
        &mut self,
        id: Uuid,
        output_sr: SortedRun,
    ) -> Result<(), SlateDBError> {
        self.state.finish_compaction(id, output_sr);
        self.progress_tracker.remove_job(id);
        self.log_compaction_state();
        self.write_manifest_safely().await?;
        self.maybe_schedule_compactions().await?;
        self.stats
            .last_compaction_ts
            .set(self.system_clock.now().timestamp() as u64);
        Ok(())
    }

    #[instrument(level = "debug", skip_all, fields(id = tracing::field::Empty))]
    async fn submit_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        let id = self.rand.rng().gen_uuid();
        tracing::Span::current().record("id", tracing::field::display(&id));
        let result = self.state.submit_compaction(id, compaction.clone());
        match result {
            Ok(_) => {
                self.start_compaction(id, compaction).await?;
            }
            Err(err) => {
                warn!("invalid compaction: {:?}", err);
            }
        }
        Ok(())
    }

    async fn refresh_db_state(&mut self) -> Result<(), SlateDBError> {
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        self.maybe_schedule_compactions().await?;
        Ok(())
    }

    fn log_compaction_state(&self) {
        self.state.db_state().log_db_runs();
        let compactions = self.state.compactions();
        for compaction in compactions.iter() {
            info!("in-flight compaction: {}", compaction);
        }
    }
}

pub mod stats {
    use crate::stats::{Counter, Gauge, StatRegistry};
    use std::sync::Arc;

    macro_rules! compactor_stat_name {
        ($suffix:expr) => {
            crate::stat_name!("compactor", $suffix)
        };
    }

    pub const BYTES_COMPACTED: &str = compactor_stat_name!("bytes_compacted");
    pub const LAST_COMPACTION_TS_SEC: &str = compactor_stat_name!("last_compaction_timestamp_sec");
    pub const RUNNING_COMPACTIONS: &str = compactor_stat_name!("running_compactions");

    pub(crate) struct CompactionStats {
        pub(crate) last_compaction_ts: Arc<Gauge<u64>>,
        pub(crate) running_compactions: Arc<Gauge<i64>>,
        pub(crate) bytes_compacted: Arc<Counter>,
    }

    impl CompactionStats {
        pub(crate) fn new(stat_registry: Arc<StatRegistry>) -> Self {
            let stats = Self {
                last_compaction_ts: Arc::new(Gauge::default()),
                running_compactions: Arc::new(Gauge::default()),
                bytes_compacted: Arc::new(Counter::default()),
            };
            stat_registry.register(LAST_COMPACTION_TS_SEC, stats.last_compaction_ts.clone());
            stat_registry.register(RUNNING_COMPACTIONS, stats.running_compactions.clone());
            stat_registry.register(BYTES_COMPACTED, stats.bytes_compacted.clone());
            stats
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use rand::RngCore;
    use ulid::Ulid;

    use super::*;
    use crate::clock::DefaultSystemClock;
    use crate::compactor::stats::CompactionStats;
    use crate::compactor_executor::{CompactionExecutor, CompactionJob, TokioCompactionExecutor};
    use crate::compactor_state::{Compaction, CompactorState, SourceId};
    use crate::compactor_stats::LAST_COMPACTION_TS_SEC;
    use crate::config::{
        PutOptions, Settings, SizeTieredCompactionSchedulerOptions, Ttl, WriteOptions,
    };
    use crate::db::Db;
    use crate::db_state::{CoreDbState, SortedRun};
    use crate::error::SlateDBError;
    use crate::iter::KeyValueIterator;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::rng;
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, TestClock};
    use crate::types::RowEntry;

    const PATH: &str = "/test/db";

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_compactor_compacts_l0() {
        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
            SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: 1,
                max_compaction_sources: 999,
                include_size_threshold: 4.0,
            },
        ));
        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .build()
            .await
            .unwrap();

        let (manifest_store, table_store) = build_test_stores(os.clone());
        let mut expected = HashMap::<Vec<u8>, Vec<u8>>::new();
        for i in 0..4 {
            let k = vec![b'a' + i as u8; 16];
            let v = vec![b'b' + i as u8; 48];
            expected.insert(k.clone(), v.clone());
            db.put(&k, &v).await.unwrap();
            let k = vec![b'j' + i as u8; 16];
            let v = vec![b'k' + i as u8; 48];
            db.put(&k, &v).await.unwrap();
            expected.insert(k.clone(), v.clone());
        }

        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, manifest_store).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        for run in db_state.compacted {
            for sst in run.ssts {
                let mut iter = SstIterator::new_borrowed(
                    ..,
                    &sst,
                    table_store.clone(),
                    SstIteratorOptions::default(),
                )
                .await
                .unwrap()
                .expect("Expected Some(iter) but got None");

                // remove the key from the expected map and verify that the db matches
                while let Some(kv) = iter.next().await.unwrap() {
                    let expected_v = expected
                        .remove(kv.key.as_ref())
                        .expect("removing unexpected key");
                    let db_v = db.get(kv.key.as_ref()).await.unwrap().unwrap();
                    assert_eq!(expected_v, db_v.as_ref());
                }
            }
        }
        assert!(expected.is_empty());
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_tombstones_in_l0() {
        use std::sync::atomic::Ordering;

        use crate::test_utils::OnDemandCompactionSchedulerSupplier;

        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new());

        let mut options = db_options(Some(compactor_options()));
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(scheduler.clone())
            .build()
            .await
            .unwrap();

        let (manifest_store, table_store) = build_test_stores(os.clone());

        // put key 'a' into L1 (and key 'b' so that when we delete 'a' the SST is non-empty)
        db.put(&[b'a'; 16], &[b'a'; 32]).await.unwrap();
        db.put(&[b'b'; 16], &[b'a'; 32]).await.unwrap();
        db.flush().await.unwrap();
        scheduler
            .scheduler
            .should_compact
            .store(true, Ordering::SeqCst);
        let db_state = await_compaction(&db, manifest_store.clone()).await.unwrap();
        assert_eq!(db_state.compacted.len(), 1);
        assert_eq!(db_state.l0.len(), 0, "{:?}", db_state.l0);

        // put tombstone for key a into L0
        db.delete_with_options(
            &[b'a'; 16],
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        // Then:
        // we should now have a tombstone in L0 and a value in L1
        let db_state = get_db_state(manifest_store.clone()).await;
        assert_eq!(db_state.l0.len(), 1, "{:?}", db_state.l0);
        assert_eq!(db_state.compacted.len(), 1);

        let l0 = db_state.l0.front().unwrap();
        let mut iter =
            SstIterator::new_borrowed(.., l0, table_store.clone(), SstIteratorOptions::default())
                .await
                .unwrap()
                .expect("Expected Some(iter) but got None");

        let tombstone = iter.next_entry().await.unwrap();
        assert!(tombstone.unwrap().value.is_tombstone());

        scheduler
            .scheduler
            .should_compact
            .store(true, Ordering::SeqCst);
        let db_state = await_compacted_compaction(manifest_store.clone(), db_state.compacted)
            .await
            .unwrap();
        assert_eq!(db_state.compacted.len(), 1);

        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();

        let mut iter = SstIterator::new_borrowed(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        // should be no tombstone for key 'a' because it was filtered
        // out of the last run
        let next = iter.next().await.unwrap();
        assert_eq!(next.unwrap().key.as_ref(), &[b'b'; 16]);
        let next = iter.next().await.unwrap();
        assert!(next.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_compact_expired_entries() {
        // given:
        let os = Arc::new(InMemory::new());
        let insert_clock = Arc::new(TestClock::new());

        let compaction_scheduler = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
            SizeTieredCompactionSchedulerOptions {
                // We'll do exactly two flushes in this test, resulting in 2 L0 files.
                min_compaction_sources: 2,
                max_compaction_sources: 2,
                include_size_threshold: 4.0,
            },
        ));

        let mut options = db_options(Some(compactor_options()));
        options.default_ttl = Some(50);
        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(insert_clock.clone())
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .build()
            .await
            .unwrap();

        let (manifest_store, table_store) = build_test_stores(os.clone());

        let value = &[b'a'; 64];

        // ticker time = 0, expire time = 10
        insert_clock.ticker.store(0, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[1; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAfter(10),
            },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        // ticker time = 10, expire time = 60 (using default TTL)
        insert_clock.ticker.store(10, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[2; 16],
            value,
            &PutOptions { ttl: Ttl::Default },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        db.flush().await.unwrap();

        // ticker time = 30, no expire time
        insert_clock.ticker.store(30, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[3; 16],
            value,
            &PutOptions { ttl: Ttl::NoExpiry },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        // this revives key 1
        // ticker time = 70, expire time 80
        insert_clock.ticker.store(70, atomic::Ordering::SeqCst);
        db.put_with_options(
            &[1; 16],
            value,
            &PutOptions {
                ttl: Ttl::ExpireAfter(80),
            },
            &WriteOptions::default(),
        )
        .await
        .unwrap();

        db.flush().await.unwrap();

        // when:
        let db_state = await_compaction(&db, manifest_store).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert!(db_state.l0_last_compacted.is_some());
        assert_eq!(db_state.compacted.len(), 1);
        assert_eq!(db_state.last_l0_clock_tick, 70);
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();
        let mut iter = SstIterator::new_borrowed(
            ..,
            handle,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(&[1; 16], value, 4)
                    .with_create_ts(70)
                    .with_expire_ts(150),
                // no tombstone for &[2; 16] because this is the last layer of the tree,
                RowEntry::new_value(&[3; 16], value, 3).with_create_ts(30),
            ],
        )
        .await;
    }

    struct CompactorEventHandlerTestFixture {
        manifest: StoredManifest,
        manifest_store: Arc<ManifestStore>,
        options: Settings,
        db: Db,
        scheduler: Arc<MockScheduler>,
        executor: Arc<MockExecutor>,
        real_executor: Arc<dyn CompactionExecutor>,
        real_executor_rx: tokio::sync::mpsc::UnboundedReceiver<WorkerToOrchestratorMsg>,
        stats_registry: Arc<StatRegistry>,
        handler: CompactorEventHandler,
    }

    impl CompactorEventHandlerTestFixture {
        async fn new() -> Self {
            let compactor_options = Arc::new(compactor_options());
            let options = db_options(None);

            let os = Arc::new(InMemory::new());
            let (manifest_store, table_store) = build_test_stores(os.clone());
            let db = Db::builder(PATH, os.clone())
                .with_settings(options.clone())
                .build()
                .await
                .unwrap();

            let scheduler = Arc::new(MockScheduler::new());
            let executor = Arc::new(MockExecutor::new());
            let (real_executor_tx, real_executor_rx) = tokio::sync::mpsc::unbounded_channel();
            let rand = Arc::new(DbRand::default());
            let stats_registry = Arc::new(StatRegistry::new());
            let compactor_stats = Arc::new(CompactionStats::new(stats_registry.clone()));
            let real_executor = Arc::new(TokioCompactionExecutor::new(
                Handle::current(),
                compactor_options.clone(),
                real_executor_tx,
                table_store,
                rand.clone(),
                compactor_stats.clone(),
                Arc::new(DefaultSystemClock::new()),
            ));
            let handler = CompactorEventHandler::new(
                manifest_store.clone(),
                compactor_options.clone(),
                scheduler.clone(),
                executor.clone(),
                rand.clone(),
                compactor_stats.clone(),
                Arc::new(DefaultSystemClock::new()),
            )
            .await
            .unwrap();
            let manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
            Self {
                manifest,
                manifest_store,
                options,
                db,
                scheduler,
                executor,
                real_executor_rx,
                real_executor,
                stats_registry,
                handler,
            }
        }

        async fn latest_db_state(&mut self) -> CoreDbState {
            self.manifest.refresh().await.unwrap().core.clone()
        }

        async fn write_l0(&mut self) {
            let mut rng = rng::new_test_rng(None);
            let manifest = self.manifest.refresh().await.unwrap();
            let l0s = manifest.core.l0.len();
            // TODO: add an explicit flush_memtable fn to db and use that instead
            let mut k = vec![0u8; self.options.l0_sst_size_bytes];
            rng.fill_bytes(&mut k);
            self.db.put(&k, &[b'x'; 10]).await.unwrap();
            self.db.flush().await.unwrap();
            loop {
                let manifest = self.manifest.refresh().await.unwrap().clone();
                if manifest.core.l0.len() > l0s {
                    break;
                }
            }
        }

        async fn build_l0_compaction(&mut self) -> Compaction {
            let l0_ids_to_compact: Vec<SourceId> = self
                .latest_db_state()
                .await
                .l0
                .iter()
                .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
                .collect();
            Compaction::new(l0_ids_to_compact, 0)
        }

        fn assert_started_compaction(&self, num: usize) -> Vec<CompactionJob> {
            let compactions = self.executor.pop_jobs();
            assert_eq!(num, compactions.len());
            compactions
        }

        fn assert_and_forward_compactions(&self, num: usize) {
            for c in self.assert_started_compaction(num) {
                self.real_executor.start_compaction(c)
            }
        }
    }

    #[tokio::test]
    async fn test_should_record_last_compaction_ts() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), fixture.real_executor_rx.recv())
            .await
            .unwrap()
            .expect("timeout");
        let starting_last_ts = fixture
            .stats_registry
            .lookup(LAST_COMPACTION_TS_SEC)
            .unwrap()
            .get();

        // when:
        fixture.handler.handle_worker_rx(msg).await;

        // then:
        assert!(
            fixture
                .stats_registry
                .lookup(LAST_COMPACTION_TS_SEC)
                .unwrap()
                .get()
                > starting_last_ts
        );
    }

    #[tokio::test]
    async fn test_should_write_manifest_safely() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), fixture.real_executor_rx.recv())
            .await
            .unwrap()
            .expect("timeout");
        // write an l0 before handling compaction finished
        fixture.write_l0().await;

        // when:
        fixture.handler.handle_worker_rx(msg).await;

        // then:
        let db_state = fixture.latest_db_state().await;
        assert_eq!(db_state.l0.len(), 1);
        assert_eq!(db_state.compacted.len(), 1);
        let l0_id = db_state.l0.front().unwrap().id.unwrap_compacted_id();
        let compacted_l0s: Vec<Ulid> = db_state
            .compacted
            .first()
            .unwrap()
            .ssts
            .iter()
            .map(|sst| sst.id.unwrap_compacted_id())
            .collect();
        assert!(!compacted_l0s.contains(&l0_id));
        assert_eq!(
            db_state.l0_last_compacted.unwrap(),
            compaction
                .sources
                .first()
                .and_then(|id| id.maybe_unwrap_sst())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_should_clear_compaction_on_failure_and_retry() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        let job = fixture.assert_started_compaction(1).pop().unwrap();
        let msg = WorkerToOrchestratorMsg::CompactionFinished {
            id: job.id,
            result: Err(SlateDBError::InvalidDBState),
        };

        // when:
        fixture.handler.handle_worker_rx(msg).await;

        // then:
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_started_compaction(1);
    }

    #[tokio::test]
    async fn test_should_not_schedule_conflicting_compaction() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_started_compaction(1);
        fixture.write_l0().await;
        fixture.scheduler.inject_compaction(compaction.clone());

        // when:
        fixture.handler.handle_ticker().await;

        // then:
        assert_eq!(0, fixture.executor.pop_jobs().len())
    }

    #[tokio::test]
    async fn test_should_leave_checkpoint_when_removing_ssts_after_compaction() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new().await;
        fixture.write_l0().await;
        let compaction = fixture.build_l0_compaction().await;
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker().await;
        fixture.assert_and_forward_compactions(1);
        let msg = tokio::time::timeout(Duration::from_millis(10), fixture.real_executor_rx.recv())
            .await
            .unwrap()
            .expect("timeout");

        // when:
        fixture.handler.handle_worker_rx(msg).await;

        // then:
        let current_dbstate = fixture.latest_db_state().await;
        let checkpoint = current_dbstate.checkpoints.last().unwrap();
        let old_manifest = fixture
            .manifest_store
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();
        let l0_ids: Vec<SourceId> = old_manifest
            .core
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .collect();
        assert_eq!(l0_ids, compaction.sources);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg(feature = "zstd")]
    async fn test_compactor_compressed_block_size() {
        use crate::compactor_stats::BYTES_COMPACTED;
        use crate::config::{CompressionCodec, SstBlockSize};

        // given:
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());
        let compaction_scheduler = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
            SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: 1,
                max_compaction_sources: 999,
                include_size_threshold: 4.0,
            },
        ));

        let mut options = db_options(Some(compactor_options()));
        options.l0_sst_size_bytes = 128;
        options.compression_codec = Some(CompressionCodec::Zstd);

        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_logical_clock(logical_clock)
            .with_compaction_scheduler_supplier(compaction_scheduler)
            .with_sst_block_size(SstBlockSize::Other(128))
            .build()
            .await
            .unwrap();

        let (manifest_store, _) = build_test_stores(os.clone());
        for i in 0..4 {
            let k = vec![b'a' + i as u8; 16];
            let v = vec![b'b' + i as u8; 48];
            db.put(&k, &v).await.unwrap();
            let k = vec![b'j' + i as u8; 16];
            let v = vec![b'k' + i as u8; 48];
            db.put(&k, &v).await.unwrap();
        }

        db.flush().await.unwrap();

        // when:
        await_compaction(&db, manifest_store)
            .await
            .expect("db was not compacted");

        // then:
        let metrics = db.metrics();
        let bytes_compacted = metrics.lookup(BYTES_COMPACTED).unwrap().get();

        assert!(bytes_compacted > 0, "bytes_compacted: {}", bytes_compacted);
    }

    async fn run_for<T, F>(duration: Duration, f: impl Fn() -> F) -> Option<T>
    where
        F: Future<Output = Option<T>>,
    {
        #[allow(clippy::disallowed_methods)]
        let now = SystemTime::now();
        while now.elapsed().unwrap() < duration {
            let maybe_result = f().await;
            if maybe_result.is_some() {
                return maybe_result;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    fn build_test_stores(os: Arc<dyn ObjectStore>) -> (Arc<ManifestStore>, Arc<TableStore>) {
        let sst_format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(PATH), os.clone()));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            sst_format,
            Path::from(PATH),
            None,
        ));
        (manifest_store, table_store)
    }

    /// Waits until all writes have made their way to L1 or below. No data is allowed in
    /// in-memory WALs, in-memory memtables, or L0 SSTs on object storage.
    async fn await_compaction(db: &Db, manifest_store: Arc<ManifestStore>) -> Option<CoreDbState> {
        run_for(Duration::from_secs(10), || async {
            let (empty_wal, empty_memtable) = {
                let mut db_state = db.inner.state.write();
                let cow_db_state = db_state.state();
                (
                    db.inner.wal_buffer.is_empty(),
                    db_state.memtable().is_empty() && cow_db_state.imm_memtable.is_empty(),
                )
            };

            let core_db_state = get_db_state(manifest_store.clone()).await;
            let empty_l0 = core_db_state.l0.is_empty();
            let compaction_ran = core_db_state.l0_last_compacted.is_some();

            if empty_wal && empty_memtable && empty_l0 && compaction_ran {
                return Some(core_db_state);
            }
            None
        })
        .await
    }

    #[allow(unused)] // only used with feature(wal_disable)
    async fn await_compacted_compaction(
        manifest_store: Arc<ManifestStore>,
        old_compacted: Vec<SortedRun>,
    ) -> Option<CoreDbState> {
        run_for(Duration::from_secs(10), || async {
            let db_state = get_db_state(manifest_store.clone()).await;
            if !db_state.compacted.eq(&old_compacted) {
                return Some(db_state);
            }
            None
        })
        .await
    }

    async fn get_db_state(manifest_store: Arc<ManifestStore>) -> CoreDbState {
        let stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        stored_manifest.db_state().clone()
    }

    fn db_options(compactor_options: Option<CompactorOptions>) -> Settings {
        Settings {
            flush_interval: Some(Duration::from_millis(100)),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            manifest_update_timeout: Duration::from_secs(300),
            l0_sst_size_bytes: 256,
            l0_max_ssts: 8,
            compactor_options,
            ..Settings::default()
        }
    }

    fn compactor_options() -> CompactorOptions {
        CompactorOptions {
            poll_interval: Duration::from_millis(100),
            max_concurrent_compactions: 1,
            ..CompactorOptions::default()
        }
    }

    struct MockSchedulerInner {
        compaction: Vec<Compaction>,
    }

    #[derive(Clone)]
    struct MockScheduler {
        inner: Arc<Mutex<MockSchedulerInner>>,
    }

    impl MockScheduler {
        fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockSchedulerInner { compaction: vec![] })),
            }
        }

        fn inject_compaction(&self, compaction: Compaction) {
            let mut inner = self.inner.lock();
            inner.compaction.push(compaction);
        }
    }

    impl CompactionScheduler for MockScheduler {
        fn maybe_schedule_compaction(&self, _state: &CompactorState) -> Vec<Compaction> {
            let mut inner = self.inner.lock();
            std::mem::take(&mut inner.compaction)
        }
    }

    struct MockExecutorInner {
        jobs: Vec<CompactionJob>,
    }

    #[derive(Clone)]
    struct MockExecutor {
        inner: Arc<Mutex<MockExecutorInner>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockExecutorInner { jobs: vec![] })),
            }
        }

        fn pop_jobs(&self) -> Vec<CompactionJob> {
            let mut guard = self.inner.lock();
            std::mem::take(&mut guard.jobs)
        }
    }

    impl CompactionExecutor for MockExecutor {
        fn start_compaction(&self, compaction: CompactionJob) {
            let mut guard = self.inner.lock();
            guard.jobs.push(compaction);
        }

        fn stop(&self) {}

        fn is_stopped(&self) -> bool {
            false
        }
    }
}
