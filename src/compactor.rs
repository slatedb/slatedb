use crate::compactor::CompactorMainMsg::Shutdown;
use crate::compactor_state::{Compaction, CompactorState};
use crate::db_state::SortedRun;
use crate::error::SlateDBError;
use crate::flatbuffer_types::ManifestV1Owned;
use crate::size_tiered_compaction::SizeTieredCompactionScheduler;
use crate::tablestore::{SSTableHandle, TableStore};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::Handle;
use ulid::Ulid;

const DEFAULT_COMPACTOR_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) trait CompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction>;
}

#[derive(Clone)]
pub struct CompactorOptions {
    poll_interval: Duration,
}

impl CompactorOptions {
    pub fn default() -> Self {
        Self {
            poll_interval: DEFAULT_COMPACTOR_POLL_INTERVAL,
        }
    }
}

enum CompactorMainMsg {
    Shutdown,
}

#[allow(dead_code)]
enum WorkerToOrchestoratorMsg {
    CompactionFinished(SortedRun),
}

pub(crate) struct Compactor {
    main_tx: crossbeam_channel::Sender<CompactorMainMsg>,
    main_thread: Option<JoinHandle<()>>,
}

impl Compactor {
    pub(crate) async fn new(
        table_store: Arc<TableStore>,
        options: CompactorOptions,
        tokio_handle: Handle,
    ) -> Result<Self, SlateDBError> {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let (err_tx, err_rx) = tokio::sync::oneshot::channel();
        let main_thread = thread::spawn(move || {
            let load_result =
                CompactorOrchestrator::new(options, table_store.clone(), tokio_handle, external_rx);
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
    options: CompactorOptions,
    table_store: Arc<TableStore>,
    tokio_handle: Handle,
    state: CompactorState,
    scheduler: Box<dyn CompactionScheduler>,
    external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
    worker_tx: crossbeam_channel::Sender<WorkerToOrchestoratorMsg>,
    worker_rx: crossbeam_channel::Receiver<WorkerToOrchestoratorMsg>,
}

impl CompactorOrchestrator {
    fn new(
        options: CompactorOptions,
        table_store: Arc<TableStore>,
        tokio_handle: Handle,
        external_rx: crossbeam_channel::Receiver<CompactorMainMsg>,
    ) -> Result<Self, SlateDBError> {
        let state = Self::load_state(table_store.clone(), &tokio_handle)?;
        let scheduler = Self::load_compaction_scheduler(&options);
        let (worker_tx, worker_rx) = crossbeam_channel::unbounded();
        let orchestrator = Self {
            options,
            table_store,
            tokio_handle,
            state,
            scheduler,
            external_rx,
            worker_tx,
            worker_rx,
        };
        Ok(orchestrator)
    }

    fn load_compaction_scheduler(_options: &CompactorOptions) -> Box<dyn CompactionScheduler> {
        // todo: return the right type based on the configured scheduler
        Box::new(SizeTieredCompactionScheduler {})
    }

    fn load_state(
        table_store: Arc<TableStore>,
        tokio_handle: &Handle,
    ) -> Result<CompactorState, SlateDBError> {
        let maybe_manifest = tokio_handle.block_on(table_store.open_latest_manifest())?;
        if let Some(manifest) = maybe_manifest {
            // todo: bump epoch here
            let state = CompactorState::new(manifest, table_store.as_ref(), tokio_handle)?;
            Ok(state)
        } else {
            Err(SlateDBError::InvalidDBState)
        }
    }

    fn run(&mut self) {
        let ticker = crossbeam_channel::tick(self.options.poll_interval);
        loop {
            crossbeam_channel::select! {
                recv(ticker) -> _ => {
                    self.load_manifest().expect("fatal error loading manifest");
                }
                recv(self.worker_rx) -> msg => {
                    let WorkerToOrchestoratorMsg::CompactionFinished(sr) = msg.expect("fatal error receiving worker msg");
                    self.finish_compaction(sr).expect("fatal error finishing compaction");
                }
                recv(self.external_rx) -> _ => {
                    return;
                }
            }
        }
    }

    fn load_manifest(&mut self) -> Result<(), SlateDBError> {
        let manifest = self
            .tokio_handle
            .block_on(self.table_store.open_latest_manifest())?;
        // todo: check epoch here
        self.refresh_db_state(manifest.expect("manifest must exist"))?;
        Ok(())
    }

    fn write_manifest(&self) -> Result<(), SlateDBError> {
        let manifest = self.state.manifest();
        self.tokio_handle
            .block_on(self.table_store.write_manifest(manifest))
    }

    fn write_manifest_safely(&mut self) -> Result<(), SlateDBError> {
        // todo: run this in a loop until either the write succeeds or we get fenced
        // read the manifest first to pull in any writer changes and check for compactor fencing
        self.load_manifest()?;
        self.write_manifest()?;
        Ok(())
    }

    fn maybe_schedule_compactions(&mut self) -> Result<(), SlateDBError> {
        let compactions = self.scheduler.maybe_schedule_compaction(&self.state);
        for compaction in compactions.iter() {
            self.submit_compaction(compaction.clone())?;
        }
        Ok(())
    }

    fn start_compaction(&mut self, compaction: Compaction) {
        // todo: spawn compaction tasks on the runtime instead
        // just complete the compaction for now by trivially writing a new run with
        // the l0 SSTs
        let l0s: HashSet<Ulid> = compaction.sources.iter().map(|s| s.unwrap_sst()).collect();
        let compacted: Vec<SSTableHandle> = self
            .state
            .db_state()
            .l0
            .iter()
            .filter(|h| {
                let ulid = h.id.unwrap_compacted_id();
                l0s.contains(&ulid)
            })
            .cloned()
            .collect();
        self.worker_tx
            .send(WorkerToOrchestoratorMsg::CompactionFinished(SortedRun {
                id: compaction.destination,
                ssts: compacted,
            }))
            .expect("failed to send compaction finished msg");
    }

    // state writers

    fn finish_compaction(&mut self, output_sr: SortedRun) -> Result<(), SlateDBError> {
        self.state.finish_compaction(output_sr);
        self.write_manifest_safely()?;
        self.maybe_schedule_compactions()?;
        Ok(())
    }

    fn submit_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        let result = self.state.submit_compaction(compaction.clone());
        if result.is_err() {
            println!("invalid compaction: {:#?}", result);
            return Ok(());
        }
        self.start_compaction(compaction);
        Ok(())
    }

    fn refresh_db_state(&mut self, manifest: ManifestV1Owned) -> Result<(), SlateDBError> {
        let result =
            self.state
                .refresh_db_state(manifest, self.table_store.clone(), &self.tokio_handle);
        if result.is_err() {
            println!("error merging writer manifest update: {:#?}", result);
            return Ok(());
        }
        self.maybe_schedule_compactions()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::compactor::{CompactorOptions, CompactorOrchestrator, WorkerToOrchestoratorMsg};
    use crate::compactor_state::{Compaction, SourceId};
    use crate::db::{Db, DbOptions};
    use crate::sst::SsTableFormat;
    use crate::tablestore::TableStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::future::Future;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use tokio::runtime::Runtime;
    use ulid::Ulid;

    const PATH: &str = "/test/db";
    const DEFAULT_OPTIONS: DbOptions = DbOptions {
        flush_ms: 100,
        min_filter_keys: 0,
        l0_sst_size_bytes: 128,
        compactor_options: Some(CompactorOptions {
            poll_interval: Duration::from_millis(100),
        }),
    };

    #[tokio::test]
    async fn test_compactor_compacts_l0() {
        // given:
        let (_, table_store, db) = build_test_db().await;
        for i in 0..4 {
            db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48]).await;
            db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48]).await;
        }

        // when:
        let maybe_manifest_owned = run_for(Duration::from_secs(10), || async {
            let manifest = table_store.open_latest_manifest().await.unwrap().unwrap();
            if manifest.borrow().l0_last_compacted().is_some() {
                return Some(manifest);
            }
            None
        })
        .await;

        // then:
        let manifest_owned = maybe_manifest_owned.expect("db was not compacted");
        let manifest = manifest_owned.borrow();
        assert!(manifest.l0_last_compacted().is_some());
        assert_eq!(manifest.compacted().len(), 1);
        assert_eq!(manifest.compacted().get(0).ssts().len(), 4);
        // todo: test that the db can read the k/vs (once we implement reading from compacted)
    }

    #[test]
    fn test_should_write_manifest_safely() {
        // given:
        // write an l0
        let rt = build_runtime();
        let (os, table_store, db) = rt.block_on(build_test_db());
        rt.block_on(db.put(&[b'a'; 32], &[b'b'; 96]));
        rt.block_on(db.close()).unwrap();
        let options = DEFAULT_OPTIONS.clone();
        let (_, external_rx) = crossbeam_channel::unbounded();
        let mut orchestrator = CompactorOrchestrator::new(
            options.compactor_options.unwrap(),
            table_store.clone(),
            rt.handle().clone(),
            external_rx,
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
            .block_on(Db::open(Path::from(PATH), DEFAULT_OPTIONS, os.clone()))
            .unwrap();
        rt.block_on(db.put(&[b'j'; 32], &[b'k'; 96]));
        rt.block_on(db.close()).unwrap();
        orchestrator
            .submit_compaction(Compaction::new(l0_ids_to_compact.clone(), 0))
            .unwrap();
        let msg = orchestrator.worker_rx.recv().unwrap();
        let WorkerToOrchestoratorMsg::CompactionFinished(sr) = msg;

        // when:
        orchestrator.finish_compaction(sr).unwrap();

        // then:
        let manifest_owned = rt
            .block_on(table_store.open_latest_manifest())
            .unwrap()
            .unwrap();
        let manifest = manifest_owned.borrow();
        assert_eq!(manifest.l0().len(), 1);
        assert_eq!(manifest.compacted().len(), 1);
        let l0_id = manifest.l0().get(0).id().unwrap().ulid();
        let compacted_l0s: Vec<Ulid> = manifest
            .compacted()
            .get(0)
            .ssts()
            .iter()
            .map(|sst| sst.id().unwrap().ulid())
            .collect();
        assert!(!compacted_l0s.contains(&l0_id));
        assert_eq!(
            manifest.l0_last_compacted().unwrap().ulid(),
            compacted_l0s.first().unwrap().clone()
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

    async fn build_test_db() -> (Arc<dyn ObjectStore>, Arc<TableStore>, Db) {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open(Path::from(PATH), DEFAULT_OPTIONS, os.clone())
            .await
            .unwrap();
        let sst_format = SsTableFormat::new(4096, 10);
        let table_store = Arc::new(TableStore::new(os.clone(), sst_format, Path::from(PATH)));
        (os, table_store, db)
    }
}
