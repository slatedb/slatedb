#![doc = include_str!("../README.md")]
#![cfg(tokio_unstable)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::oneshot;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::RwLock;
use tokio::runtime::RngSeed;
use tokio::task::JoinSet;

use fail_parallel::FailPointRegistry;
use slatedb::{Db, DbRand, Error};
use slatedb_common::clock::SystemClock;
use slatedb_common::MockSystemClock;

mod clocked_object_store;
pub mod failing_object_store;

use self::clocked_object_store::ClockedObjectStore;
pub use self::failing_object_store::{
    FailingObjectStore, FailingObjectStoreController, HttpFailBefore, HttpStatusError, Operation,
    StreamDirection, Toxic, ToxicKind,
};

type DbFactoryFuture = Pin<Box<dyn Future<Output = Result<Arc<Db>, Error>> + Send + 'static>>;
type ActorFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;
type StartupFactory = Box<dyn FnOnce(StartupCtx) -> DbFactoryFuture + Send + 'static>;
type ActorFn = Arc<dyn Fn(ActorCtx) -> ActorFuture + Send + Sync + 'static>;

#[derive(Clone, Debug)]
pub struct Config {
    seed: u64,
}

impl Config {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }
}

pub struct Harness;

impl Harness {
    pub fn builder(config: Config) -> HarnessBuilder {
        HarnessBuilder::new(config)
    }
}

pub struct HarnessBuilder {
    config: Config,
    path: Option<Path>,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    startup_factory: Option<StartupFactory>,
    actors: Vec<ActorRegistration>,
}

struct ActorRegistration {
    role: String,
    count: usize,
    actor: ActorFn,
}

#[derive(Clone)]
pub struct StartupCtx {
    path: Path,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    system_clock: Arc<dyn SystemClock>,
    fp_registry: Arc<FailPointRegistry>,
    rand: Arc<DbRand>,
}

impl StartupCtx {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn main_object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.main_object_store)
    }

    pub fn wal_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        self.wal_object_store.clone()
    }

    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        Arc::clone(&self.system_clock)
    }

    pub fn fp_registry(&self) -> Arc<FailPointRegistry> {
        Arc::clone(&self.fp_registry)
    }

    pub fn rand(&self) -> &DbRand {
        self.rand.as_ref()
    }
}

#[derive(Clone)]
struct SharedHarness {
    path: Path,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    system_clock: Arc<dyn SystemClock>,
    fp_registry: Arc<FailPointRegistry>,
    failures: Arc<FailingObjectStoreController>,
    db_slot: Arc<RwLock<Arc<Db>>>,
}

#[derive(Clone)]
pub struct ActorCtx {
    role: String,
    instance: usize,
    rand: Arc<DbRand>,
    shared: SharedHarness,
}

impl ActorCtx {
    pub fn role(&self) -> &str {
        &self.role
    }

    pub fn instance(&self) -> usize {
        self.instance
    }

    pub fn rand(&self) -> &DbRand {
        self.rand.as_ref()
    }

    pub fn db(&self) -> Arc<Db> {
        Arc::clone(&self.shared.db_slot.read())
    }

    pub fn swap_db(&self, new_db: Arc<Db>) -> Arc<Db> {
        let mut guard = self.shared.db_slot.write();
        std::mem::replace(&mut *guard, new_db)
    }

    pub async fn advance_time(&self, duration: Duration) {
        self.shared.system_clock.advance(duration).await;
    }

    pub fn failures(&self) -> &FailingObjectStoreController {
        self.shared.failures.as_ref()
    }

    pub fn path(&self) -> &Path {
        &self.shared.path
    }

    pub fn main_object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.shared.main_object_store)
    }

    pub fn wal_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        self.shared.wal_object_store.clone()
    }

    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        Arc::clone(&self.shared.system_clock)
    }

    pub fn fp_registry(&self) -> Arc<FailPointRegistry> {
        Arc::clone(&self.shared.fp_registry)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RunResult;

impl HarnessBuilder {
    fn new(config: Config) -> Self {
        Self {
            config,
            path: None,
            main_object_store: Arc::new(InMemory::new()),
            wal_object_store: None,
            startup_factory: None,
            actors: Vec::new(),
        }
    }

    pub fn with_path(mut self, path: impl Into<Path>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn with_main_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.main_object_store = store;
        self
    }

    pub fn with_wal_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(store);
        self
    }

    pub fn with_db<F, Fut>(mut self, factory: F) -> Self
    where
        F: FnOnce(StartupCtx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<Db>, Error>> + Send + 'static,
    {
        self.startup_factory = Some(Box::new(move |ctx| Box::pin(factory(ctx))));
        self
    }

    pub fn actor<F, Fut>(mut self, role: impl Into<String>, count: usize, actor: F) -> Self
    where
        F: Fn(ActorCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        self.actors.push(ActorRegistration {
            role: role.into(),
            count,
            actor: Arc::new(move |ctx| Box::pin(actor(ctx))),
        });
        self
    }

    pub fn actor_with_state<T, F, Fut>(
        mut self,
        role: impl Into<String>,
        count: usize,
        state: Arc<T>,
        actor: F,
    ) -> Self
    where
        T: Send + Sync + 'static,
        F: Fn(ActorCtx, Arc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        self.actors.push(ActorRegistration {
            role: role.into(),
            count,
            actor: Arc::new(move |ctx| {
                let state = Arc::clone(&state);
                Box::pin(actor(ctx, state))
            }),
        });
        self
    }

    pub async fn run(self) -> Result<RunResult, Error> {
        if self.startup_factory.is_none() {
            return Err(Error::invalid(
                "dst harness requires with_db(...) before run()".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("slatedb-dst-harness".to_string())
            .spawn(move || {
                let _ = tx.send(self.run_blocking());
            })
            .map_err(|error| {
                Error::internal(format!(
                    "failed to spawn dst harness runtime thread: {error}"
                ))
            })?;

        rx.await.map_err(|_| {
            Error::internal("dst harness runtime terminated before producing a result".to_string())
        })?
    }

    fn run_blocking(self) -> Result<RunResult, Error> {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_all();
        builder.rng_seed(RngSeed::from_bytes(&self.config.seed.to_le_bytes()));

        let runtime = builder.build().map_err(|error| {
            Error::internal(format!("failed to build dst harness runtime: {error}"))
        })?;
        runtime.block_on(self.run_inner())
    }

    async fn run_inner(self) -> Result<RunResult, Error> {
        let path = self.path.unwrap_or_else(|| default_path(self.config.seed));
        let system_clock: Arc<dyn SystemClock> = Arc::new(MockSystemClock::new());
        let fp_registry = Arc::new(FailPointRegistry::new());
        let failures = Arc::new(FailingObjectStoreController::new(Arc::new(DbRand::new(
            derive_seed(self.config.seed, "failures", "", 0),
        ))));

        let main_object_store = wrap_store(
            self.main_object_store,
            Arc::clone(&system_clock),
            failures.as_ref().clone(),
        );
        let wal_object_store = self
            .wal_object_store
            .map(|store| wrap_store(store, Arc::clone(&system_clock), failures.as_ref().clone()));

        let startup_ctx = StartupCtx {
            path: path.clone(),
            main_object_store: Arc::clone(&main_object_store),
            wal_object_store: wal_object_store.clone(),
            system_clock: Arc::clone(&system_clock),
            fp_registry: Arc::clone(&fp_registry),
            rand: Arc::new(DbRand::new(derive_seed(self.config.seed, "startup", "", 0))),
        };

        let db = self
            .startup_factory
            .expect("validated before runtime startup")(startup_ctx)
            .await?;

        let shared = SharedHarness {
            path,
            main_object_store,
            wal_object_store,
            system_clock,
            fp_registry,
            failures,
            db_slot: Arc::new(RwLock::new(db)),
        };

        let mut join_set = JoinSet::new();
        for registration in self.actors {
            for instance in 0..registration.count {
                let role = registration.role.clone();
                let actor = Arc::clone(&registration.actor);
                let ctx = ActorCtx {
                    role: role.clone(),
                    instance,
                    rand: Arc::new(DbRand::new(derive_seed(
                        self.config.seed,
                        "actor",
                        &role,
                        instance as u64,
                    ))),
                    shared: shared.clone(),
                };
                join_set.spawn(async move { actor(ctx).await });
            }
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    join_set.abort_all();
                    return Err(error);
                }
                Err(error) => {
                    join_set.abort_all();
                    return Err(Error::internal(format!(
                        "dst actor task failed to join: {error}"
                    )));
                }
            }
        }

        Ok(RunResult)
    }
}

fn wrap_store(
    base: Arc<dyn ObjectStore>,
    system_clock: Arc<dyn SystemClock>,
    failures: FailingObjectStoreController,
) -> Arc<dyn ObjectStore> {
    let clocked: Arc<dyn ObjectStore> =
        Arc::new(ClockedObjectStore::new(base, system_clock.clone()));
    Arc::new(FailingObjectStore::new(clocked, failures, system_clock))
}

fn default_path(seed: u64) -> Path {
    Path::from(format!(
        "dst/seed-{:016x}",
        derive_seed(seed, "path", "", 0)
    ))
}

fn derive_seed(root: u64, namespace: &str, role: &str, instance: u64) -> u64 {
    let mut state = root ^ 0x9e37_79b9_7f4a_7c15;
    for byte in namespace
        .bytes()
        .chain(std::iter::once(0xff))
        .chain(role.bytes())
        .chain(std::iter::once(0xfe))
        .chain(instance.to_le_bytes())
    {
        state = splitmix64(state ^ u64::from(byte));
    }
    splitmix64(state)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
