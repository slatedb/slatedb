use crate::compactor::CompactorMainMsg::Shutdown;
use crate::compactor_state::{
    CompactionWriter, CompactorStateHolder, CompactorStateListener, CompactorStateReader,
};
use crate::db_state::SortedRun;
use crate::error::SlateDBError;
use crate::size_tiered_compaction::SizeTieredCompactionSchedulerFactory;
use crate::tablestore::{SSTableHandle, TableStore};
use crossbeam_channel::{Receiver, Sender};
use futures::executor::block_on;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use ulid::Ulid;

const DEFAULT_COMPACTOR_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) trait CompactionSchedulerFactory {
    /*
     * Notifies the scheduler that it should start evaluating the db for compaction. This method
     * receives a channel over which the Executor sends the Scheduler updates about changes
     * to the database. This includes updates about changes to the database (e.g. arrival of
     * new L0 files), and completion of compactions.
     */
    fn create(
        &self,
        state: Rc<dyn CompactorStateReader>,
        compaction_writer: Rc<dyn CompactionWriter>,
    ) -> Rc<dyn CompactorStateListener>;
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
    CompactionStatus,
}

pub(crate) struct Compactor {
    main_tx: Sender<CompactorMainMsg>,
    main_thread: RefCell<Option<JoinHandle<()>>>,
}

impl Compactor {
    pub(crate) fn new(
        table_store: Arc<TableStore>,
        options: CompactorOptions,
    ) -> Result<Self, SlateDBError> {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let (err_tx, err_rx) = crossbeam_channel::unbounded();
        let main_thread = std::thread::spawn(move || {
            let (_, worker_orch_rx) = crossbeam_channel::unbounded();
            let load_result = CompactorOrchestrator::new(
                options,
                table_store.clone(),
                external_rx,
                worker_orch_rx,
            );
            let orchestrator = match load_result {
                Ok(orchestrator) => orchestrator,
                Err(err) => {
                    err_tx.send(Err(err)).expect("err channel failure");
                    return;
                }
            };
            err_tx.send(Ok(())).expect("err channel failure");
            orchestrator.run();
        });
        err_rx.recv().expect("err channel failure")?;
        Ok(Self {
            main_thread: RefCell::new(Some(main_thread)),
            main_tx: external_tx,
        })
    }

    pub(crate) fn close(&self) {
        let mut maybe_main_thread = self.main_thread.borrow_mut();
        if let Some(main_thread) = maybe_main_thread.take() {
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
    state: Rc<CompactorStateHolder>,
    external_rx: Receiver<CompactorMainMsg>,
    worker_rx: Receiver<WorkerToOrchestoratorMsg>,
}

impl CompactorOrchestrator {
    fn new(
        options: CompactorOptions,
        table_store: Arc<TableStore>,
        external_rx: Receiver<CompactorMainMsg>,
        worker_rx: Receiver<WorkerToOrchestoratorMsg>,
    ) -> Result<Rc<Self>, SlateDBError> {
        let state = Self::load_state(table_store.clone())?;
        let scheduler =
            Self::load_compaction_scheduler(&options).create(state.clone(), state.clone());
        let orchestrator = Rc::new(Self {
            options,
            table_store,
            state: state.clone(),
            external_rx,
            worker_rx,
        });
        state.add_listener(orchestrator.clone());
        state.add_listener(scheduler);
        state.add_listener(Rc::new(LoggingCompactorStateListener {
            state: state.clone(),
        }));
        Ok(orchestrator)
    }

    fn load_compaction_scheduler(
        _options: &CompactorOptions,
    ) -> Box<dyn CompactionSchedulerFactory> {
        // todo: return the right type based on the configured scheduler
        Box::new(SizeTieredCompactionSchedulerFactory {})
    }

    fn load_state(table_store: Arc<TableStore>) -> Result<Rc<CompactorStateHolder>, SlateDBError> {
        let maybe_manifest = block_on(table_store.open_latest_manifest())?;
        if let Some(manifest) = maybe_manifest {
            // todo: bump epoch here
            let state = Rc::new(CompactorStateHolder::new(manifest, table_store.as_ref())?);
            Ok(state)
        } else {
            Err(SlateDBError::InvalidDBState)
        }
    }

    fn run(&self) {
        let ticker = crossbeam_channel::tick(self.options.poll_interval);
        loop {
            crossbeam_channel::select! {
                recv(ticker) -> _ => {
                    self.load_manifest();
                }
                recv(self.worker_rx) -> _ => {
                    // todo: update compaction status
                }
                recv(self.external_rx) -> _ => {
                    return;
                }
            }
        }
    }

    fn load_manifest(&self) {
        let manifest_read_result = block_on(self.table_store.open_latest_manifest());
        match manifest_read_result {
            Ok(manifest) => {
                // todo: check epoch here
                let merge_result = self.state.merge_writer_update(
                    manifest.expect("manifest must exist"),
                    self.table_store.clone(),
                );
                if merge_result.is_err() {
                    println!(
                        "error merging writer manifest update: {:#?}",
                        merge_result.err()
                    )
                }
            }
            Err(err) => {
                println!("error reading manifest: {:#?}", err)
            }
        }
    }

    fn write_manifest_safely(&self) {
        // todo: run this in a loop until either the write succeeds or we get fenced
        // read the manifest first to pull in any writer changes and check for compactor fencing
        self.load_manifest();
        self.write_manifest();
    }

    fn write_manifest(&self) {
        let manifest = self.state.manifest();
        let result = block_on(self.table_store.write_manifest(manifest.as_ref()));
        if result.is_err() {
            println!("error writing updated manifest: {:#?}", result.err())
        }
    }
}

impl CompactorStateListener for CompactorOrchestrator {
    fn on_compactor_db_state_update(&self) {
        self.write_manifest_safely();
    }

    fn on_compaction_submitted(&self) {
        // todo: send compactions to the compaction workers instead
        // just complete the compaction for now by trivially writing a new run with
        // the l0 SSTs
        for compaction in self.state.compactions() {
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
            self.state.finish_compaction(SortedRun {
                id: compaction.destination,
                ssts: compacted,
            })
        }
    }
}

struct LoggingCompactorStateListener {
    state: Rc<dyn CompactorStateReader>,
}

impl CompactorStateListener for LoggingCompactorStateListener {
    fn on_writer_db_state_update(&self) {
        println!("compactor db state merged with writer db state");
    }

    fn on_compactor_db_state_update(&self) {
        println!("db state updated by compactor")
    }

    fn on_compaction_submitted(&self) {
        println!(
            "new compaction submitted. currently running compactions: {:#?}",
            self.state.compactions()
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::compactor::{CompactorOptions, CompactorOrchestrator};
    use crate::compactor_state::{CompactionWriter, CompactorStateReader, SourceId};
    use crate::db::{Db, DbOptions};
    use crate::sst::SsTableFormat;
    use crate::tablestore::TableStore;
    use futures::executor::block_on;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
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

    #[test]
    fn test_compactor_compacts_l0() {
        // given:
        let (_, table_store, db) = build_test_db();
        for i in 0..4 {
            block_on(db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48]));
            block_on(db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48]));
        }

        // when:
        let maybe_manifest_owned = run_for(Duration::from_secs(10), || {
            let manifest = block_on(table_store.open_latest_manifest())
                .unwrap()
                .unwrap();
            if manifest.borrow().l0_last_compacted().is_some() {
                return Some(manifest);
            }
            None
        });

        // then:
        let manifest_owned = maybe_manifest_owned.expect("db was not compacted");
        let manifest = manifest_owned.borrow();
        assert!(manifest.l0_last_compacted().is_some());
        assert!(manifest.compacted().is_some());
        assert_eq!(manifest.compacted().as_ref().unwrap().len(), 1);
        assert_eq!(
            manifest
                .compacted()
                .as_ref()
                .unwrap()
                .get(0)
                .ssts()
                .unwrap()
                .len(),
            4
        );
        // todo: test that the db can read the k/vs (once we implement reading from compacted)
    }

    #[test]
    fn test_should_write_manifest_safely() {
        // given:
        // write an l0
        let (os, table_store, db) = build_test_db();
        block_on(db.put(&[b'a'; 32], &[b'b'; 96]));
        block_on(db.close()).unwrap();
        let options = DEFAULT_OPTIONS.clone();
        let (_, external_rx) = crossbeam_channel::unbounded();
        let (_, worker_rx) = crossbeam_channel::unbounded();
        let orchestrator = CompactorOrchestrator::new(
            options.compactor_options.unwrap(),
            table_store.clone(),
            external_rx,
            worker_rx,
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
        let db = block_on(Db::open(Path::from(PATH), DEFAULT_OPTIONS, os.clone())).unwrap();
        block_on(db.put(&[b'j'; 32], &[b'k'; 96]));
        block_on(db.close()).unwrap();

        // when:
        orchestrator
            .state
            .submit_compaction(l0_ids_to_compact.clone(), 0)
            .unwrap();

        // then:
        let manifest_owned = block_on(table_store.open_latest_manifest())
            .unwrap()
            .unwrap();
        let manifest = manifest_owned.borrow();
        let manifest_l0 = manifest.l0().unwrap();
        assert_eq!(manifest_l0.len(), 1);
        let manifest_compacted = manifest.compacted().unwrap();
        assert_eq!(manifest_compacted.len(), 1);
        let l0_id = manifest_l0.get(0).id().unwrap().ulid();
        let compacted_l0s: Vec<Ulid> = manifest_compacted
            .get(0)
            .ssts()
            .unwrap()
            .iter()
            .map(|sst| sst.id().unwrap().ulid())
            .collect();
        assert!(!compacted_l0s.contains(&l0_id));
        assert_eq!(
            manifest.l0_last_compacted().unwrap().ulid(),
            compacted_l0s.first().unwrap().clone()
        );
    }

    fn run_for<T, F>(duration: Duration, mut f: F) -> Option<T>
    where
        F: FnMut() -> Option<T>,
    {
        let now = SystemTime::now();
        while now.elapsed().unwrap() < duration {
            let maybe_result = f();
            if maybe_result.is_some() {
                return maybe_result;
            }
            sleep(Duration::from_millis(100));
        }
        None
    }

    fn build_test_db() -> (Arc<dyn ObjectStore>, Arc<TableStore>, Db) {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = block_on(Db::open(Path::from(PATH), DEFAULT_OPTIONS, os.clone())).unwrap();
        let sst_format = SsTableFormat::new(4096, 10);
        let table_store = Arc::new(TableStore::new(os.clone(), sst_format, Path::from(PATH)));
        (os, table_store, db)
    }
}
