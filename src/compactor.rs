use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;
use tracing::{error, info, warn};
use ulid::Ulid;

use crate::compactor::CompactorMainMsg::Shutdown;
use crate::compactor_executor::{CompactionExecutor, CompactionJob, TokioCompactionExecutor};
use crate::compactor_state::{Compaction, CompactorState};
use crate::config::CompactorOptions;
use crate::db_state::{SSTableHandle, SortedRun};
use crate::error::SlateDBError;
use crate::manifest_store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::metrics::DbStats;
use crate::tablestore::TableStore;

pub trait CompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction>;
}

enum CompactorMainMsg {
    Shutdown,
}

pub(crate) enum WorkerToOrchestratorMsg {
    CompactionFinished(Result<SortedRun, SlateDBError>),
}

pub(crate) struct Compactor {
    main_tx: crossbeam_channel::Sender<CompactorMainMsg>,
    main_thread: Option<JoinHandle<()>>,
}

impl Compactor {
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: CompactorOptions,
        tokio_handle: Handle,
        db_stats: Arc<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let (err_tx, err_rx) = tokio::sync::oneshot::channel();
        let tokio_handle = options.compaction_runtime.clone().unwrap_or(tokio_handle);
        let main_thread = thread::spawn(move || {
            let load_result = CompactorOrchestrator::new(
                options,
                manifest_store.clone(),
                table_store.clone(),
                tokio_handle,
                external_rx,
                db_stats,
            );
            let mut orchestrator = match load_result {
                Ok(orchestrator) => orchestrator,
                Err(err) => {
                    err_tx.send(Err(err)).expect("err channel failure");
                    return;
                }
            };
            err_tx.send(Ok(())).expect("err channel failure");
            orchestrator.run();
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
            main_thread
                .join()
                .expect("failed to stop main compactor thread");
        }
    }
}

struct CompactorOrchestrator {
    options: Arc<CompactorOptions>,
    manifest: FenceableManifest,
    tokio_handle: Handle,
    state: CompactorState,
    scheduler: Box<dyn CompactionScheduler>,
    executor: Box<dyn CompactionExecutor>,
    external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
    worker_rx: crossbeam_channel::Receiver<WorkerToOrchestratorMsg>,
    db_stats: Arc<DbStats>,
}

impl CompactorOrchestrator {
    fn new(
        options: CompactorOptions,
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        tokio_handle: Handle,
        external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
        db_stats: Arc<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let options = Arc::new(options);
        let stored_manifest =
            tokio_handle.block_on(StoredManifest::load(manifest_store.clone()))?;
        let Some(stored_manifest) = stored_manifest else {
            return Err(SlateDBError::InvalidDBState);
        };
        let manifest = tokio_handle.block_on(FenceableManifest::init_compactor(stored_manifest))?;
        let state = Self::load_state(&manifest)?;
        let scheduler = Self::load_compaction_scheduler(options.as_ref());
        let (worker_tx, worker_rx) = crossbeam_channel::unbounded();
        let executor = TokioCompactionExecutor::new(
            tokio_handle.clone(),
            options.clone(),
            worker_tx,
            table_store.clone(),
            db_stats.clone(),
        );
        let orchestrator = Self {
            options,
            manifest,
            tokio_handle,
            state,
            scheduler,
            executor: Box::new(executor),
            external_rx,
            worker_rx,
            db_stats,
        };
        Ok(orchestrator)
    }

    fn load_compaction_scheduler(options: &CompactorOptions) -> Box<dyn CompactionScheduler> {
        options.compaction_scheduler.compaction_scheduler()
    }

    fn load_state(stored_manifest: &FenceableManifest) -> Result<CompactorState, SlateDBError> {
        let db_state = stored_manifest.db_state()?;
        Ok(CompactorState::new(db_state.clone()))
    }

    fn run(&mut self) {
        let ticker = crossbeam_channel::tick(self.options.poll_interval);
        let db_runs_log_ticker = crossbeam_channel::tick(Duration::from_secs(10));

        // Stop the loop when the executor is shut down *and* all remaining
        // `worker_rx` messages have been drained.
        while !(self.executor.is_stopped() && self.worker_rx.is_empty()) {
            crossbeam_channel::select! {
                recv(db_runs_log_ticker) -> _ => {
                    self.log_compaction_state();
                }
                recv(ticker) -> _ => {
                    if !self.executor.is_stopped() {
                        self.load_manifest().expect("fatal error loading manifest");
                    }
                }
                recv(self.worker_rx) -> msg => {
                    let WorkerToOrchestratorMsg::CompactionFinished(result) = msg.expect("fatal error receiving worker msg");
                    match result {
                        Ok(sr) => self.finish_compaction(sr).expect("fatal error finishing compaction"),
                        Err(err) => error!("error executing compaction: {:#?}", err)
                    }
                }
                recv(self.external_rx) -> _ => {
                    // Stop the executor. Don't return because there might
                    // still be messages in `worker_rx`. Let the loop continue
                    // to drain them until empty.
                    self.executor.stop();
                }
            }
        }
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
                println!(
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

    fn start_compaction(&mut self, compaction: Compaction) {
        self.log_compaction_state();
        let db_state = self.state.db_state();
        let compacted_sst_iter = db_state.compacted.iter().flat_map(|sr| sr.ssts.iter());
        let ssts_by_id: HashMap<Ulid, &SSTableHandle> = db_state
            .l0
            .iter()
            .chain(compacted_sst_iter)
            .map(|sst| (sst.id.unwrap_compacted_id(), sst))
            .collect();
        let srs_by_id: HashMap<u32, &SortedRun> =
            db_state.compacted.iter().map(|sr| (sr.id, sr)).collect();
        let ssts: Vec<SSTableHandle> = compaction
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
            destination: compaction.destination,
            ssts,
            sorted_runs,
        });
    }

    // state writers

    fn finish_compaction(&mut self, output_sr: SortedRun) -> Result<(), SlateDBError> {
        self.state.finish_compaction(output_sr);
        self.log_compaction_state();
        self.write_manifest_safely()?;
        self.maybe_schedule_compactions()?;
        self.db_stats.last_compaction_ts.set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
        Ok(())
    }

    fn submit_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        let result = self.state.submit_compaction(compaction.clone());
        if result.is_err() {
            warn!("invalid compaction: {:?}", result);
            return Ok(());
        }
        self.start_compaction(compaction);
        Ok(())
    }

    fn refresh_db_state(&mut self) -> Result<(), SlateDBError> {
        self.state.refresh_db_state(self.manifest.db_state()?);
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use tokio::runtime::Runtime;
    use ulid::Ulid;

    use crate::compactor::{CompactorOptions, CompactorOrchestrator, WorkerToOrchestratorMsg};
    use crate::compactor_state::{Compaction, SourceId};
    use crate::config::{DbOptions, ObjectStoreCacheOptions, SizeTieredCompactionSchedulerOptions};
    use crate::db::Db;
    use crate::iter::KeyValueIterator;
    use crate::manifest_store::{ManifestStore, StoredManifest};
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::SstIterator;
    use crate::tablestore::TableStore;

    const PATH: &str = "/test/db";

    #[tokio::test]
    async fn test_compactor_compacts_l0() {
        // given:
        let options = db_options(Some(compactor_options()));
        let (_, manifest_store, table_store, db) = build_test_db(options).await;
        for i in 0..4 {
            db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48]).await;
            db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48]).await;
        }

        // when:
        let db_state = run_for(Duration::from_secs(10), || async {
            let stored_manifest = StoredManifest::load(manifest_store.clone())
                .await
                .unwrap()
                .unwrap();
            let db_state = stored_manifest.db_state();
            if db_state.l0_last_compacted.is_some() {
                return Some(db_state.clone());
            }
            None
        })
        .await;

        // then:
        let db_state = db_state.expect("db was not compacted");
        assert!(db_state.l0_last_compacted.is_some());
        assert_eq!(db_state.compacted.len(), 1);
        let compacted = &db_state.compacted.first().unwrap().ssts;
        assert_eq!(compacted.len(), 1);
        let handle = compacted.first().unwrap();
        let mut iter = SstIterator::new(handle, table_store.clone(), 1, 1, false)
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

    #[test]
    fn test_should_write_manifest_safely() {
        // given:
        // write an l0
        let options = db_options(None);
        let rt = build_runtime();
        let (os, manifest_store, table_store, db) = rt.block_on(build_test_db(options.clone()));
        let mut stored_manifest = rt
            .block_on(StoredManifest::load(manifest_store.clone()))
            .unwrap()
            .unwrap();
        rt.block_on(db.put(&[b'a'; 32], &[b'b'; 96]));
        rt.block_on(db.close()).unwrap();
        let (_, external_rx) = crossbeam_channel::unbounded();
        let mut orchestrator = CompactorOrchestrator::new(
            compactor_options(),
            manifest_store.clone(),
            table_store.clone(),
            rt.handle().clone(),
            external_rx,
            db.metrics(),
        )
        .unwrap();
        let l0_ids_to_compact: Vec<SourceId> = orchestrator
            .state
            .db_state()
            .l0
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();
        // write another l0
        let db = rt
            .block_on(Db::open_with_opts(
                Path::from(PATH),
                options.clone(),
                os.clone(),
            ))
            .unwrap();
        rt.block_on(db.put(&[b'j'; 32], &[b'k'; 96]));
        rt.block_on(db.close()).unwrap();
        orchestrator
            .submit_compaction(Compaction::new(l0_ids_to_compact.clone(), 0))
            .unwrap();
        let msg = orchestrator.worker_rx.recv().unwrap();
        let WorkerToOrchestratorMsg::CompactionFinished(Ok(sr)) = msg else {
            panic!("compaction failed")
        };

        // when:
        orchestrator.finish_compaction(sr).unwrap();

        // then:
        let db_state = rt.block_on(stored_manifest.refresh()).unwrap();
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
            l0_ids_to_compact.first().unwrap().unwrap_sst()
        );
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
        let sst_format = SsTableFormat::new(32, 10, options.compression_codec);
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(PATH), os.clone()));
        let table_store = Arc::new(TableStore::new(
            os.clone(),
            sst_format,
            Path::from(PATH),
            None,
        ));
        (os, manifest_store, table_store, db)
    }

    fn db_options(compactor_options: Option<CompactorOptions>) -> DbOptions {
        DbOptions {
            flush_interval: Duration::from_millis(100),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            min_filter_keys: 0,
            l0_sst_size_bytes: 128,
            max_unflushed_memtable: 2,
            l0_max_ssts: 8,
            compactor_options,
            compression_codec: None,
            object_store_cache_options: ObjectStoreCacheOptions::default(),
            block_cache_options: None,
        }
    }

    fn compactor_options() -> CompactorOptions {
        CompactorOptions {
            poll_interval: Duration::from_millis(100),
            max_sst_size: 1024 * 1024 * 1024,
            compaction_scheduler: Arc::new(SizeTieredCompactionSchedulerSupplier::new(
                SizeTieredCompactionSchedulerOptions::default(),
            )),
            max_concurrent_compactions: 1,
            compaction_runtime: None,
        }
    }
}
