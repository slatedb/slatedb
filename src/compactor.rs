use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;
use tracing::{error, info, warn};
use ulid::Ulid;
use uuid::Uuid;

use crate::compactor::stats::CompactionStats;
use crate::compactor::CompactorMainMsg::Shutdown;
use crate::compactor_executor::{CompactionExecutor, CompactionJob, TokioCompactionExecutor};
use crate::compactor_state::{Compaction, CompactorState};
use crate::config::CompactorOptions;
use crate::db_state::{SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::manifest_store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::utils::spawn_bg_thread;

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

pub(crate) struct Compactor {
    main_tx: crossbeam_channel::Sender<CompactorMainMsg>,
    main_thread: Option<JoinHandle<Result<(), SlateDBError>>>,
}

impl Compactor {
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: CompactorOptions,
        tokio_handle: Handle,
        stat_registry: &StatRegistry,
        cleanup_fn: impl FnOnce(&Result<(), SlateDBError>) + Send + 'static,
    ) -> Result<Self, SlateDBError> {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let (err_tx, err_rx) = tokio::sync::oneshot::channel();
        let tokio_handle = options.compaction_runtime.clone().unwrap_or(tokio_handle);
        let stats = Arc::new(CompactionStats::new(stat_registry));
        let main_thread = spawn_bg_thread("slatedb-compactor", cleanup_fn, move || {
            let load_result = CompactorOrchestrator::new(
                options,
                manifest_store.clone(),
                table_store.clone(),
                tokio_handle,
                external_rx,
                stats,
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
            let result = main_thread
                .join()
                .expect("failed to stop main compactor thread");
            info!("compactor thread exited with: {:?}", result)
        }
    }
}

struct CompactorOrchestrator {
    ticker: crossbeam_channel::Receiver<std::time::Instant>,
    external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
    worker_rx: crossbeam_channel::Receiver<WorkerToOrchestratorMsg>,
    handler: CompactorEventHandler,
}

impl CompactorOrchestrator {
    fn new(
        options: CompactorOptions,
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        tokio_handle: Handle,
        external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
        stats: Arc<CompactionStats>,
    ) -> Result<Self, SlateDBError> {
        let options = Arc::new(options);
        let ticker = crossbeam_channel::tick(options.poll_interval);
        let scheduler = Self::load_compaction_scheduler(options.as_ref());
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
        )?;
        let orchestrator = Self {
            ticker,
            external_rx,
            worker_rx,
            handler,
        };
        Ok(orchestrator)
    }

    fn load_compaction_scheduler(options: &CompactorOptions) -> Box<dyn CompactionScheduler> {
        options.compaction_scheduler.compaction_scheduler()
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
}

impl CompactorEventHandler {
    fn new(
        tokio_handle: tokio::runtime::Handle,
        manifest_store: &Arc<ManifestStore>,
        options: Arc<CompactorOptions>,
        scheduler: Box<dyn CompactionScheduler>,
        executor: Box<dyn CompactionExecutor>,
        stats: Arc<CompactionStats>,
    ) -> Result<Self, SlateDBError> {
        let stored_manifest =
            tokio_handle.block_on(StoredManifest::load(manifest_store.clone()))?;
        let manifest = tokio_handle.block_on(FenceableManifest::init_compactor(stored_manifest))?;
        let db_state = manifest.db_state()?;
        let state = CompactorState::new(db_state.clone());
        Ok(Self {
            tokio_handle,
            state,
            manifest,
            options,
            scheduler,
            executor,
            stats,
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
        let core = self.state.db_state().clone();
        self.tokio_handle
            .block_on(self.manifest.update_db_state(core))
    }

    fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        loop {
            self.load_manifest()?;
            match self.write_manifest() {
                Ok(_) => return Ok(()),
                Err(SlateDBError::ManifestVersionExists) => {
                    warn!("conflicting manifest version. retry write");
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
        self.executor.start_compaction(CompactionJob {
            id,
            destination: compaction.destination,
            ssts,
            sorted_runs,
            compaction_ts: db_state.last_l0_clock_tick,
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
            SystemTime::now()
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
        self.state.merge_db_state(self.manifest.db_state()?);
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
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use rand::RngCore;
    use tokio::runtime::Runtime;
    use ulid::Ulid;

    use crate::compactor::stats::CompactionStats;
    use crate::compactor::{
        CompactionScheduler, CompactorEventHandler, CompactorOptions, WorkerToOrchestratorMsg,
    };
    use crate::compactor_executor::{CompactionExecutor, CompactionJob, TokioCompactionExecutor};
    use crate::compactor_state::{Compaction, CompactorState, SourceId};
    use crate::compactor_stats::LAST_COMPACTION_TS_SEC;
    use crate::config::{
        DbOptions, PutOptions, SizeTieredCompactionSchedulerOptions, Ttl, WriteOptions,
    };
    use crate::db::Db;
    use crate::db_state::CoreDbState;
    use crate::iter::KeyValueIterator;
    use crate::manifest_store::{ManifestStore, StoredManifest};

    use crate::proptest_util::rng;
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::stats::StatRegistry;
    use crate::tablestore::TableStore;
    use crate::test_utils::{assert_iterator, TestClock};
    use crate::types::RowEntry;
    use crate::SlateDBError;

    const PATH: &str = "/test/db";

    #[tokio::test]
    async fn test_compactor_compacts_l0() {
        // given:
        let clock = Arc::new(TestClock::new());
        let options = db_options(Some(compactor_options()), clock.clone());
        let (_, manifest_store, table_store, db) = build_test_db(options).await;
        for i in 0..4 {
            db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48])
                .await
                .unwrap();
            db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48])
                .await
                .unwrap();
        }

        // when:
        let db_state = await_compaction(manifest_store).await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert!(db_state.l0_last_compacted.is_some());
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
        for i in 0..4 {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key.as_ref(), &[b'a' + i as u8; 16]);
            assert_eq!(kv.value.as_ref(), &[b'b' + i as u8; 48]);
        }
        for i in 0..4 {
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key.as_ref(), &[b'j' + i as u8; 16]);
            assert_eq!(kv.value.as_ref(), &[b'k' + i as u8; 48]);
        }
        assert!(iter.next().await.unwrap().is_none());
        // todo: test that the db can read the k/vs (once we implement reading from compacted)
    }

    #[tokio::test]
    async fn test_should_compact_expired_entries() {
        // given:
        let insert_clock = Arc::new(TestClock::new());

        let compactor_opts = CompactorOptions {
            compaction_scheduler: Arc::new(SizeTieredCompactionSchedulerSupplier::new(
                SizeTieredCompactionSchedulerOptions {
                    // We'll do exactly two flushes in this test, resulting in 2 L0 files.
                    min_compaction_sources: 2,
                    max_compaction_sources: 2,
                    include_size_threshold: 4.0,
                },
            )),
            ..compactor_options()
        };
        let options = DbOptions {
            default_ttl: Some(50),
            ..db_options(Some(compactor_opts), insert_clock.clone())
        };
        let (_, manifest_store, table_store, db) = build_test_db(options).await;

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
        let db_state = await_compaction(manifest_store).await;

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
                RowEntry::new_tombstone(&[2; 16], 2).with_create_ts(10),
                RowEntry::new_value(&[3; 16], value, 3).with_create_ts(30),
            ],
        )
        .await;
    }

    struct CompactorEventHandlerTestFixture {
        rt: Runtime,
        manifest: StoredManifest,
        options: DbOptions,
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
            let options = db_options(None, Arc::new(TestClock::new()));
            let (_, manifest_store, table_store, db) = rt.block_on(build_test_db(options.clone()));
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
            )
            .unwrap();
            let manifest = rt
                .block_on(StoredManifest::load(manifest_store.clone()))
                .unwrap();
            Self {
                rt,
                manifest,
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
            self.rt.block_on(self.manifest.refresh()).unwrap().clone()
        }

        fn write_l0(&mut self) {
            let fut = async {
                let mut rng = rng::new_test_rng(None);
                let state = self.manifest.refresh().await.unwrap();
                let l0s = state.l0.len();
                // TODO: add an explicit flush_memtable fn to db and use that instead
                let mut k = vec![0u8; self.options.l0_sst_size_bytes];
                rng.fill_bytes(&mut k);
                self.db.put(&k, &[b'x'; 10]).await.unwrap();
                self.db.flush().await.unwrap();
                loop {
                    let state = self.manifest.refresh().await.unwrap().clone();
                    if state.l0.len() > l0s {
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

    async fn build_test_db(
        options: DbOptions,
    ) -> (
        Arc<dyn ObjectStore>,
        Arc<ManifestStore>,
        Arc<TableStore>,
        Db,
    ) {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open_with_opts(Path::from(PATH), options.clone(), os.clone())
            .await
            .unwrap();
        let sst_format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 10,
            compression_codec: options.compression_codec,
            ..SsTableFormat::default()
        };
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(PATH), os.clone()));
        let table_store = Arc::new(TableStore::new(
            os.clone(),
            sst_format,
            Path::from(PATH),
            None,
        ));
        (os, manifest_store, table_store, db)
    }

    async fn await_compaction(manifest_store: Arc<ManifestStore>) -> Option<CoreDbState> {
        run_for(Duration::from_secs(10), || async {
            let stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
            let db_state = stored_manifest.db_state();
            if db_state.l0_last_compacted.is_some() {
                return Some(db_state.clone());
            }
            None
        })
        .await
    }

    fn db_options(compactor_options: Option<CompactorOptions>, clock: Arc<TestClock>) -> DbOptions {
        DbOptions {
            flush_interval: Duration::from_millis(100),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            l0_sst_size_bytes: 128,
            l0_max_ssts: 8,
            compactor_options,
            clock,
            ..DbOptions::default()
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
