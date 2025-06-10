use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use tokio::runtime::Handle;
use tracing::{error, info, warn};
use ulid::Ulid;
use uuid::Uuid;

use crate::clock::SystemClock;
use crate::compactor::stats::CompactionStats;
use crate::compactor::CompactorMainMsg::Shutdown;
use crate::compactor_executor::{CompactionExecutor, CompactionJob, TokioCompactionExecutor};
use crate::compactor_state::{Compaction, CompactorState};
use crate::config::{CheckpointOptions, CompactorOptions};
use crate::db_state::{SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
pub use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::utils::spawn_bg_thread;

pub trait CompactionSchedulerSupplier: Send + Sync {
    fn compaction_scheduler(&self) -> Box<dyn CompactionScheduler>;
}

pub trait CompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction>;
}

enum CompactorMainMsg {
    Shutdown,
}

pub(crate) enum WorkerToOrchestratorMsg {
    CompactionFinished {
        id: Uuid,
        result: Result<SortedRun, SlateDBError>,
    },
}

/// The compactor is responsible for taking groups of sorted runs (this doc uses the term
/// sorted run to refer to both sorted runs and l0 ssts) and compacting them together to
/// reduce space amplification (by removing old versions of rows that have been updated/deleted)
/// and read amplification (by reducing the number of sorted runs that need to be searched on
/// a read). It's made up of a few different components:
///
/// The Orchestrator is responsible for orchestrating the ongoing process of compacting
/// the db. It periodically polls the manifest for changes, calls into the Scheduler
/// to discover compactions that should be performed, schedules those compactions on the
/// Executor, and when they are completed, updates the manifest. The Orchestrator is made
/// up of [`CompactorOrchestrator`] and [`CompactorEventHandler`]. [`CompactorOrchestrator`]
/// runs the main event loop on a background thread. The event loop listens on the manifest
/// poll ticker to react to manifest poll ticks, the executor worker channel to react to
/// updates about running compactions, and the shutdown channel to discover when it should
/// terminate. It doesn't actually implement the logic for reacting to these events. This
/// is implemented by [`CompactorEventHandler`].
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
pub(crate) struct Compactor {
    main_tx: crossbeam_channel::Sender<CompactorMainMsg>,
    main_thread: Option<JoinHandle<Result<(), SlateDBError>>>,
}

impl Compactor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: CompactorOptions,
        scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
        tokio_handle: Handle,
        stat_registry: &StatRegistry,
        cleanup_fn: impl FnOnce(&Result<(), SlateDBError>) + Send + 'static,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let (err_tx, err_rx) = tokio::sync::oneshot::channel();
        let stats = Arc::new(CompactionStats::new(stat_registry));
        let main_thread = spawn_bg_thread("slatedb-compactor", cleanup_fn, move || {
            let load_result = CompactorOrchestrator::new(
                options,
                manifest_store.clone(),
                table_store.clone(),
                scheduler_supplier,
                tokio_handle,
                external_rx,
                stats,
                system_clock,
            );
            let mut orchestrator = match load_result {
                Ok(orchestrator) => orchestrator,
                Err(err) => {
                    err_tx.send(Err(err)).expect("err channel failure");
                    return Ok(());
                }
            };
            err_tx.send(Ok(())).expect("err channel failure");
            orchestrator.run()
        });
        err_rx.await.expect("err channel failure")?;
        Ok(Self {
            main_thread: Some(main_thread),
            main_tx: external_tx,
        })
    }

    pub(crate) async fn close(mut self) {
        if let Some(main_thread) = self.main_thread.take() {
            self.main_tx.send(Shutdown).expect("main tx disconnected");
            // Wait on a separate thread to avoid blocking the tokio runtime
            tokio::task::spawn_blocking(move || {
                let result = main_thread
                    .join()
                    .expect("failed to stop main compactor thread");
                info!("compactor thread exited with: {:?}", result)
            });
        }
    }
}

struct CompactorOrchestrator {
    // TODO: We need to migrate this to tokio::time::Instant for DST
    // The current orchestrator implementation does not use the tokio runtime
    // for for its run loop, so we can't make a tokio ticker yet.
    #[allow(clippy::disallowed_types)]
    ticker: crossbeam_channel::Receiver<std::time::Instant>,
    external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
    worker_rx: crossbeam_channel::Receiver<WorkerToOrchestratorMsg>,
    handler: CompactorEventHandler,
}

impl CompactorOrchestrator {
    #[allow(clippy::too_many_arguments)]
    fn new(
        options: CompactorOptions,
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
        tokio_handle: Handle,
        external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
        stats: Arc<CompactionStats>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let options = Arc::new(options);
        let ticker = crossbeam_channel::tick(options.poll_interval);
        let scheduler = scheduler_supplier.compaction_scheduler();
        let (worker_tx, worker_rx) = crossbeam_channel::unbounded();
        let executor = Box::new(TokioCompactionExecutor::new(
            tokio_handle.clone(),
            options.clone(),
            worker_tx,
            table_store.clone(),
            stats.clone(),
        ));
        let handler = CompactorEventHandler::new(
            tokio_handle,
            &manifest_store,
            options,
            scheduler,
            executor,
            stats,
            system_clock,
        )?;
        let orchestrator = Self {
            ticker,
            external_rx,
            worker_rx,
            handler,
        };
        Ok(orchestrator)
    }

    fn run(&mut self) -> Result<(), SlateDBError> {
        let db_runs_log_ticker = crossbeam_channel::tick(Duration::from_secs(10));

        // Stop the loop when the executor is shut down *and* all remaining
        // `worker_rx` messages have been drained.
        while !(self.handler.is_executor_stopped() && self.worker_rx.is_empty()) {
            crossbeam_channel::select! {
                recv(db_runs_log_ticker) -> _ => {
                    self.handler.handle_log_ticker();
                }
                recv(self.ticker) -> _ => {
                    self.handler.handle_ticker();
                }
                recv(self.worker_rx) -> msg => {
                    self.handler.handle_worker_rx(msg.expect("fatal error receiving worker msg"));
                }
                recv(self.external_rx) -> _ => {
                    // Stop the executor. Don't return because there might
                    // still be messages in `worker_rx`. Let the loop continue
                    // to drain them until empty.
                    self.handler.stop_executor();
                }
            }
        }
        Ok(())
    }
}

struct CompactorEventHandler {
    tokio_handle: tokio::runtime::Handle,
    state: CompactorState,
    manifest: FenceableManifest,
    options: Arc<CompactorOptions>,
    scheduler: Box<dyn CompactionScheduler>,
    executor: Box<dyn CompactionExecutor>,
    stats: Arc<CompactionStats>,
    system_clock: Arc<dyn SystemClock>,
}

impl CompactorEventHandler {
    fn new(
        tokio_handle: tokio::runtime::Handle,
        manifest_store: &Arc<ManifestStore>,
        options: Arc<CompactorOptions>,
        scheduler: Box<dyn CompactionScheduler>,
        executor: Box<dyn CompactionExecutor>,
        stats: Arc<CompactionStats>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let stored_manifest =
            tokio_handle.block_on(StoredManifest::load(manifest_store.clone()))?;
        let manifest = tokio_handle.block_on(FenceableManifest::init_compactor(
            stored_manifest,
            options.manifest_update_timeout,
        ))?;
        let state = CompactorState::new(manifest.prepare_dirty()?);
        Ok(Self {
            tokio_handle,
            state,
            manifest,
            options,
            scheduler,
            executor,
            stats,
            system_clock,
        })
    }

    fn handle_log_ticker(&self) {
        self.log_compaction_state();
    }

    fn handle_ticker(&mut self) {
        if !self.is_executor_stopped() {
            self.load_manifest().expect("fatal error loading manifest");
        }
    }

    fn handle_worker_rx(&mut self, msg: WorkerToOrchestratorMsg) {
        let WorkerToOrchestratorMsg::CompactionFinished { id, result } = msg;
        match result {
            Ok(sr) => self
                .finish_compaction(id, sr)
                .expect("fatal error finishing compaction"),
            Err(err) => {
                error!("error executing compaction: {:#?}", err);
                self.finish_failed_compaction(id);
            }
        }
    }

    fn stop_executor(&self) {
        self.executor.stop();
    }

    fn is_executor_stopped(&self) -> bool {
        self.executor.is_stopped()
    }

    fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        self.tokio_handle.block_on(self.manifest.refresh())?;
        self.refresh_db_state()?;
        Ok(())
    }

    fn write_manifest(&mut self) -> Result<(), SlateDBError> {
        // write the checkpoint first so that it points to the manifest with the ssts
        // being removed
        self.tokio_handle.block_on(self.manifest.write_checkpoint(
            None,
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
        ))?;
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        let dirty = self.state.manifest().clone();
        self.tokio_handle
            .block_on(self.manifest.update_manifest(dirty))
    }

    fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.load_manifest()?;
            match self.write_manifest() {
                Ok(_) => return Ok(()),
                Err(SlateDBError::ManifestVersionExists) => {
                    warn!("conflicting manifest version. updating and retrying write again.");
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn maybe_schedule_compactions(&mut self) -> Result<(), SlateDBError> {
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
            self.submit_compaction(compaction.clone())?;
        }
        Ok(())
    }

    fn start_compaction(&mut self, id: Uuid, compaction: Compaction) {
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
        self.executor.start_compaction(CompactionJob {
            id,
            destination: compaction.destination,
            ssts,
            sorted_runs,
            compaction_ts: db_state.last_l0_clock_tick,
            is_dest_last_run,
        });
    }

    // state writers
    fn finish_failed_compaction(&mut self, id: Uuid) {
        self.state.finish_failed_compaction(id);
    }

    fn finish_compaction(&mut self, id: Uuid, output_sr: SortedRun) -> Result<(), SlateDBError> {
        self.state.finish_compaction(id, output_sr);
        self.log_compaction_state();
        self.write_manifest_safely()?;
        self.maybe_schedule_compactions()?;
        self.stats.last_compaction_ts.set(
            self.system_clock
                .now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
        Ok(())
    }

    fn submit_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        let result = self.state.submit_compaction(compaction.clone());
        let Ok(id) = result.as_ref() else {
            warn!("invalid compaction: {:?}", result);
            return Ok(());
        };
        self.start_compaction(*id, compaction);
        Ok(())
    }

    fn refresh_db_state(&mut self) -> Result<(), SlateDBError> {
        self.state
            .merge_remote_manifest(self.manifest.prepare_dirty()?);
        self.maybe_schedule_compactions()?;
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
        pub(crate) fn new(stat_registry: &StatRegistry) -> Self {
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
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use rand::RngCore;
    use tokio::runtime::Runtime;
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
    use crate::{DbContext, DbContextBuilder, SlateDBError};

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

        let db_context = DbContextBuilder::new()
            .with_logical_clock(logical_clock)
            .build();
        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_context(db_context)
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
                .unwrap();

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
        let os = Arc::new(InMemory::new());
        let logical_clock = Arc::new(TestClock::new());

        let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new());

        let mut options = db_options(Some(compactor_options()));
        options.wal_enabled = false;
        options.l0_sst_size_bytes = 128;

        let db_context = DbContextBuilder::new()
            .with_logical_clock(logical_clock)
            .build();
        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_context(db_context)
            .with_compaction_scheduler_supplier(scheduler.clone())
            .build()
            .await
            .unwrap();

        let (manifest_store, table_store) = build_test_stores(os.clone());

        // put key 'a' into L1 (and key 'b' so that when we delete 'a' the SST is non-empty)
        db.put(&[b'a'; 16], &[b'a'; 32]).await.unwrap();
        db.put(&[b'b'; 16], &[b'a'; 32]).await.unwrap();
        db.flush().await.unwrap();
        scheduler.scheduler.should_compact.store(true, SeqCst);
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
                .unwrap();

        let tombstone = iter.next_entry().await.unwrap();
        assert!(tombstone.unwrap().value.is_tombstone());

        scheduler.scheduler.should_compact.store(true, SeqCst);
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
        .unwrap();

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

        let db_context = DbContextBuilder::new()
            .with_logical_clock(insert_clock.clone())
            .build();
        let mut options = db_options(Some(compactor_options()));
        options.default_ttl = Some(50);
        let db = Db::builder(PATH, os.clone())
            .with_settings(options)
            .with_context(db_context)
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
        .unwrap();

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
        rt: Runtime,
        manifest: StoredManifest,
        manifest_store: Arc<ManifestStore>,
        options: Settings,
        db: Db,
        scheduler: Box<MockScheduler>,
        executor: Box<MockExecutor>,
        real_executor: Box<dyn CompactionExecutor>,
        real_executor_rx: crossbeam_channel::Receiver<WorkerToOrchestratorMsg>,
        stats_registry: StatRegistry,
        handler: CompactorEventHandler,
    }

    impl CompactorEventHandlerTestFixture {
        fn new() -> Self {
            let rt = build_runtime();
            let compactor_options = Arc::new(compactor_options());
            let options = db_options(None);

            let os = Arc::new(InMemory::new());
            let (manifest_store, table_store) = build_test_stores(os.clone());
            let db = rt
                .block_on(
                    Db::builder(PATH, os.clone())
                        .with_settings(options.clone())
                        .build(),
                )
                .unwrap();

            let scheduler = Box::new(MockScheduler::new());
            let executor = Box::new(MockExecutor::new());
            let (real_executor_tx, real_executor_rx) = crossbeam_channel::unbounded();
            let stats_registry = StatRegistry::new();
            let compactor_stats = Arc::new(CompactionStats::new(&stats_registry));
            let real_executor = Box::new(TokioCompactionExecutor::new(
                rt.handle().clone(),
                compactor_options.clone(),
                real_executor_tx,
                table_store,
                compactor_stats.clone(),
            ));
            let handler = CompactorEventHandler::new(
                rt.handle().clone(),
                &manifest_store,
                compactor_options,
                scheduler.clone(),
                executor.clone(),
                compactor_stats,
                Arc::new(DefaultSystemClock::new()),
            )
            .unwrap();
            let manifest = rt
                .block_on(StoredManifest::load(manifest_store.clone()))
                .unwrap();
            Self {
                rt,
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

        fn latest_db_state(&mut self) -> CoreDbState {
            self.rt
                .block_on(self.manifest.refresh())
                .unwrap()
                .core
                .clone()
        }

        fn write_l0(&mut self) {
            let fut = async {
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
            };
            self.rt.block_on(fut);
        }

        fn build_l0_compaction(&mut self) -> Compaction {
            let l0_ids_to_compact: Vec<SourceId> = self
                .latest_db_state()
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

    #[test]
    fn test_should_record_last_compaction_ts() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new();
        fixture.write_l0();
        let compaction = fixture.build_l0_compaction();
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker();
        fixture.assert_and_forward_compactions(1);
        let msg = fixture
            .real_executor_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let starting_last_ts = fixture
            .stats_registry
            .lookup(LAST_COMPACTION_TS_SEC)
            .unwrap()
            .get();

        // when:
        fixture.handler.handle_worker_rx(msg);

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

    #[test]
    fn test_should_write_manifest_safely() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new();
        fixture.write_l0();
        let compaction = fixture.build_l0_compaction();
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker();
        fixture.assert_and_forward_compactions(1);
        let msg = fixture
            .real_executor_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        // write an l0 before handling compaction finished
        fixture.write_l0();

        // when:
        fixture.handler.handle_worker_rx(msg);

        // then:
        let db_state = fixture.latest_db_state();
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

    #[test]
    fn test_should_clear_compaction_on_failure_and_retry() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new();
        fixture.write_l0();
        let compaction = fixture.build_l0_compaction();
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker();
        let job = fixture.assert_started_compaction(1).pop().unwrap();
        let msg = WorkerToOrchestratorMsg::CompactionFinished {
            id: job.id,
            result: Err(SlateDBError::InvalidDBState),
        };

        // when:
        fixture.handler.handle_worker_rx(msg);

        // then:
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker();
        fixture.assert_started_compaction(1);
    }

    #[test]
    fn test_should_not_schedule_conflicting_compaction() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new();
        fixture.write_l0();
        let compaction = fixture.build_l0_compaction();
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker();
        fixture.assert_started_compaction(1);
        fixture.write_l0();
        fixture.scheduler.inject_compaction(compaction.clone());

        // when:
        fixture.handler.handle_ticker();

        // then:
        assert_eq!(0, fixture.executor.pop_jobs().len())
    }

    #[test]
    fn test_should_leave_checkpoint_when_removing_ssts_after_compaction() {
        // given:
        let mut fixture = CompactorEventHandlerTestFixture::new();
        fixture.write_l0();
        let compaction = fixture.build_l0_compaction();
        fixture.scheduler.inject_compaction(compaction.clone());
        fixture.handler.handle_ticker();
        fixture.assert_and_forward_compactions(1);
        let msg = fixture.real_executor_rx.recv().unwrap();

        // when:
        fixture.handler.handle_worker_rx(msg);

        // then:
        let current_dbstate = fixture.latest_db_state();
        let checkpoint = current_dbstate.checkpoints.last().unwrap();
        let old_manifest = fixture
            .rt
            .block_on(fixture.manifest_store.read_manifest(checkpoint.manifest_id))
            .unwrap();
        let l0_ids: Vec<SourceId> = old_manifest
            .core
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .collect();
        assert_eq!(l0_ids, compaction.sources);
    }

    fn build_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
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
                    db_state.wal().is_empty() && cow_db_state.imm_wal.is_empty(),
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

    #[allow(unused)] // only used with feature(wal_disable)
    #[derive(Clone)]
    struct OnDemandCompactionScheduler {
        should_compact: Arc<AtomicBool>,
    }

    #[allow(unused)] // only used with feature(wal_disable)
    impl OnDemandCompactionScheduler {
        fn new() -> Self {
            Self {
                should_compact: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl CompactionScheduler for OnDemandCompactionScheduler {
        fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction> {
            if !self.should_compact.load(SeqCst) {
                return vec![];
            }

            // this compactor will only compact if there are L0s,
            // it won't compact only lower levels for simplicity
            let db_state = state.db_state();
            if db_state.l0.is_empty() {
                return vec![];
            }

            self.should_compact.store(false, SeqCst);

            // always compact into sorted run 0
            let next_sr_id = 0;

            // Create a compaction of all SSTs from L0 and all sorted runs
            let mut sources: Vec<SourceId> = db_state
                .l0
                .iter()
                .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
                .collect();

            // Add SSTs from all sorted runs
            for sr in &db_state.compacted {
                sources.push(SourceId::SortedRun(sr.id));
            }

            vec![Compaction::new(sources, next_sr_id)]
        }
    }

    #[allow(unused)] // only used with feature(wal_disable)
    struct OnDemandCompactionSchedulerSupplier {
        scheduler: OnDemandCompactionScheduler,
    }

    #[allow(unused)] // only used with feature(wal_disable)
    impl OnDemandCompactionSchedulerSupplier {
        fn new() -> Self {
            Self {
                scheduler: OnDemandCompactionScheduler::new(),
            }
        }
    }

    impl CompactionSchedulerSupplier for OnDemandCompactionSchedulerSupplier {
        fn compaction_scheduler(&self) -> Box<dyn CompactionScheduler> {
            Box::new(self.scheduler.clone())
        }
    }
}
