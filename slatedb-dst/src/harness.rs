//! Deterministic scenario test harness primitives for orchestrating seeded
//! SlateDB actors.

use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::info;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::RwLock;
use rand::{Rng, RngCore};
use tokio::runtime::RngSeed;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use fail_parallel::FailPointRegistry;
use slatedb::compactor::Compactor;
use slatedb::{Db, DbRand, Error};
use slatedb_common::clock::SystemClock;
use slatedb_common::MockSystemClock;

use crate::clocked_object_store::ClockedObjectStore;

type DbFactoryFuture = Pin<Box<dyn Future<Output = Result<Arc<Db>, Error>> + Send + 'static>>;
type StartupFactory = Box<dyn FnOnce(StartupCtx) -> DbFactoryFuture + Send + 'static>;

#[async_trait]
pub trait Actor: Send + 'static {
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error>;

    async fn finish(&mut self, _ctx: &ActorCtx) -> Result<(), Error> {
        Ok(())
    }
}

struct ActorRegistration {
    name: String,
    actor: Box<dyn Actor>,
}

/// Per-actor context passed to each registered harness task.
///
/// The context exposes deterministic randomness, the shared database slot,
/// clock-wrapped object stores, the shared shutdown token, and test
/// infrastructure such as the failpoint registry and mock clock.
#[derive(Clone)]
pub struct ActorCtx {
    name: String,
    rand: Arc<DbRand>,
    shared: HarnessCtx,
}

impl ActorCtx {
    /// Returns the logical name assigned when the actor was registered.
    ///
    /// ## Returns
    /// - `&str`: The unique actor name for this harness run.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the actor-local deterministic random number generator.
    ///
    /// ## Returns
    /// - `&DbRand`: The seeded RNG derived from the harness seed for this actor.
    pub fn rand(&self) -> &DbRand {
        self.rand.as_ref()
    }

    /// Returns the startup context shared by this harness run.
    ///
    /// This exposes the database path, object stores, clock, failpoint
    /// registry, and startup RNG used by the database factory.
    pub fn startup_ctx(&self) -> &StartupCtx {
        &self.shared.startup_ctx
    }

    /// Returns the current database handle from the shared harness slot.
    ///
    /// ## Returns
    /// - `Arc<Db>`: A clone of the currently installed database handle.
    pub fn db(&self) -> Arc<Db> {
        Arc::clone(&self.shared.db_slot.read())
    }

    /// Replaces the shared database handle for all actors.
    ///
    /// ## Arguments
    /// - `new_db`: The new database handle to install in the shared slot.
    ///
    /// ## Returns
    /// - `Arc<Db>`: The previously installed database handle.
    pub fn swap_db(&self, new_db: Arc<Db>) -> Arc<Db> {
        let mut guard = self.shared.db_slot.write();
        std::mem::replace(&mut *guard, new_db)
    }

    /// Replaces the shared standalone compactor handle for harness shutdown.
    ///
    /// ## Arguments
    /// - `new_compactor`: The standalone compactor handle to install.
    ///
    /// ## Returns
    /// - `Option<Compactor>`: The previously installed compactor handle, if any.
    pub fn swap_compactor(&self, new_compactor: Compactor) -> Option<Compactor> {
        let mut guard = self.shared.compactor_slot.write();
        guard.replace(new_compactor)
    }

    /// Advances the shared mock system clock by the provided duration.
    ///
    /// ## Arguments
    /// - `duration`: The amount of simulated time to add.
    pub async fn advance_time(&self, duration: Duration) {
        self.shared.startup_ctx.system_clock.advance(duration).await;
    }

    /// Returns the root path used to open the database under test.
    ///
    /// ## Returns
    /// - `&Path`: The path configured for the harness run.
    pub fn path(&self) -> &Path {
        self.shared.startup_ctx.path()
    }

    /// Returns the wrapped main object store used by the harness.
    ///
    /// ## Returns
    /// - `Arc<dyn ObjectStore>`: The main object store wrapped with
    ///   deterministic clock behavior.
    pub fn main_object_store(&self) -> Arc<dyn ObjectStore> {
        self.shared.startup_ctx.main_object_store()
    }

    /// Returns the wrapped WAL object store, if one was configured.
    ///
    /// ## Returns
    /// - `Option<Arc<dyn ObjectStore>>`: The WAL store wrapped with
    ///   deterministic clock behavior, or `None`.
    pub fn wal_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        self.shared.startup_ctx.wal_object_store()
    }

    /// Returns the shared system clock used by the harness.
    ///
    /// ## Returns
    /// - `Arc<dyn SystemClock>`: The clock backing time-sensitive test behavior.
    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        self.shared.startup_ctx.system_clock()
    }

    /// Returns the shared failpoint registry for the harness run.
    ///
    /// ## Returns
    /// - `Arc<FailPointRegistry>`: The registry used to configure failpoints in
    ///   participating components.
    pub fn fp_registry(&self) -> Arc<FailPointRegistry> {
        self.shared.startup_ctx.fp_registry()
    }

    /// Returns the shared shutdown token for the current harness run.
    ///
    /// Actors may call `cancel()` to request harness shutdown, or observe the
    /// token to exit cleanly once shutdown has started.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shared.shutdown_token.clone()
    }
}

#[derive(Clone)]
/// Startup context passed to the database factory configured with
/// [`Harness::new`].
///
/// The startup context exposes clock-wrapped object stores and shared
/// infrastructure before actors begin running.
pub struct StartupCtx {
    path: Path,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    system_clock: Arc<dyn SystemClock>,
    fp_registry: Arc<FailPointRegistry>,
    rand: Arc<DbRand>,
}

impl StartupCtx {
    /// Returns the root path that the database factory should open.
    ///
    /// ## Returns
    /// - `&Path`: The harness path for this run.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the wrapped main object store for database startup.
    ///
    /// ## Returns
    /// - `Arc<dyn ObjectStore>`: The main object store wrapped for
    ///   deterministic clock behavior.
    pub fn main_object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.main_object_store)
    }

    /// Returns the wrapped WAL object store for database startup, if present.
    ///
    /// ## Returns
    /// - `Option<Arc<dyn ObjectStore>>`: The wrapped WAL object store, or
    ///   `None` when the harness was configured with only a main store.
    pub fn wal_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        self.wal_object_store.clone()
    }

    /// Returns the shared system clock for the harness run.
    ///
    /// ## Returns
    /// - `Arc<dyn SystemClock>`: The clock used by the harness and wrapped
    ///   object stores.
    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        Arc::clone(&self.system_clock)
    }

    /// Returns the shared failpoint registry for the harness run.
    ///
    /// ## Returns
    /// - `Arc<FailPointRegistry>`: The registry used to configure failpoints in
    ///   the database under test.
    pub fn fp_registry(&self) -> Arc<FailPointRegistry> {
        Arc::clone(&self.fp_registry)
    }

    /// Returns the startup-local deterministic random number generator.
    ///
    /// ## Returns
    /// - `&DbRand`: The seeded RNG reserved for database initialization.
    pub fn rand(&self) -> &DbRand {
        self.rand.as_ref()
    }
}

#[derive(Clone)]
struct HarnessCtx {
    startup_ctx: StartupCtx,
    db_slot: Arc<RwLock<Arc<Db>>>,
    compactor_slot: Arc<RwLock<Option<Compactor>>>,
    shutdown_token: CancellationToken,
}

struct ClockDriver {
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl ClockDriver {
    fn spawn(
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
        clock_advance_ms: RangeInclusive<u64>,
    ) -> Self {
        let shutdown = CancellationToken::new();
        let mut steps = 0u64;
        let task = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                while !shutdown.is_cancelled() {
                    let advance_ms = rand.rng().random_range(clock_advance_ms.clone());
                    system_clock
                        .advance(Duration::from_millis(advance_ms))
                        .await;
                    steps += 1;
                    if steps % 100_000 == 0 {
                        info!(
                            "clock driver advanced [steps={steps}, time={:?}]",
                            system_clock.now()
                        );
                    }
                }
            }
        });

        Self {
            shutdown,
            task: Some(task),
        }
    }

    async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            if let Err(error) = task.await {
                panic!("dst harness clock task failed: {error}");
            }
        }
    }
}

impl Drop for ClockDriver {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Builder and executor for deterministic SlateDB scenario tests.
///
/// A harness owns the seeded runtime configuration, shared mock clock,
/// failpoint registry, clock-wrapped object stores, and the shared database
/// slot visible to all registered actors.
pub struct Harness {
    name: String,
    rand: Arc<DbRand>,
    system_clock: Arc<MockSystemClock>,
    path: Option<Path>,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    clock_advance_ms: RangeInclusive<u64>,
    startup_factory: StartupFactory,
    actors: Vec<ActorRegistration>,
}

impl Harness {
    /// Creates a new deterministic harness builder.
    ///
    /// ## Arguments
    /// - `name`: Scenario name used when deriving the default database path.
    /// - `seed`: Root seed used to derive deterministic randomness for startup
    ///   and actor-local RNGs.
    /// - `factory`: A function that receives a [`StartupCtx`] and returns the
    ///   database handle that actors should share.
    ///
    /// ## Returns
    /// - `Harness`: A harness builder with an in-memory main object store and no
    ///   WAL object store configured.
    pub fn new<F, Fut>(name: impl Into<String>, seed: u64, factory: F) -> Self
    where
        F: FnOnce(StartupCtx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<Db>, Error>> + Send + 'static,
    {
        Self {
            name: name.into(),
            rand: Arc::new(DbRand::new(seed)),
            system_clock: Arc::new(MockSystemClock::new()),
            path: None,
            main_object_store: Arc::new(InMemory::new()),
            wal_object_store: None,
            clock_advance_ms: 1..=5,
            startup_factory: Box::new(move |ctx| Box::pin(factory(ctx))),
            actors: Vec::new(),
        }
    }

    /// Overrides the default database path derived from the harness name and seed.
    ///
    /// ## Arguments
    /// - `path`: The root path to use for opening the database under test.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn with_path(mut self, path: impl Into<Path>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Overrides the root deterministic random number generator.
    ///
    /// ## Arguments
    /// - `rand`: The RNG to use for deriving runtime, startup, and actor-local
    ///   seeds.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn with_rand(mut self, rand: Arc<DbRand>) -> Self {
        self.rand = rand;
        self
    }

    /// Overrides the shared mock system clock used by the harness run.
    ///
    /// ## Arguments
    /// - `system_clock`: The mock clock to share across startup, actors, and
    ///   clock-wrapped object stores.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn with_system_clock(mut self, system_clock: Arc<MockSystemClock>) -> Self {
        self.system_clock = system_clock;
        self
    }

    /// Overrides the inclusive millisecond range used by the background clock driver.
    ///
    /// ## Arguments
    /// - `clock_advance_ms`: Inclusive millisecond range to sample for each
    ///   clock step.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    ///
    /// # Panics
    /// Panics if the range starts at zero or is empty.
    pub fn with_clock_advance(mut self, clock_advance_ms: RangeInclusive<u64>) -> Self {
        assert!(
            *clock_advance_ms.start() > 0,
            "dst harness clock advance minimum must be greater than zero"
        );
        assert!(
            clock_advance_ms.start() <= clock_advance_ms.end(),
            "dst harness clock advance range must not be empty"
        );
        self.clock_advance_ms = clock_advance_ms;
        self
    }

    /// Replaces the default in-memory main object store.
    ///
    /// ## Arguments
    /// - `store`: The object store to wrap and expose as the harness main store.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn with_main_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.main_object_store = store;
        self
    }

    /// Configures an optional WAL object store for the harness.
    ///
    /// ## Arguments
    /// - `store`: The object store to wrap and expose as the harness WAL store.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn with_wal_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(store);
        self
    }

    /// Registers one actor task under a unique logical name.
    ///
    /// ## Arguments
    /// - `name`: Unique logical name for the actor task.
    /// - `actor`: Actor instance to run until the shared shutdown token is cancelled.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    ///
    /// # Panics
    /// Panics if the supplied name has already been registered on this harness.
    pub fn actor<A>(mut self, name: impl Into<String>, actor: A) -> Self
    where
        A: Actor,
    {
        let name = name.into();
        assert!(
            self.actors.iter().all(|registered| registered.name != name),
            "dst harness actor names must be unique: {name}"
        );

        self.actors.push(ActorRegistration {
            name,
            actor: Box::new(actor),
        });
        self
    }

    /// Runs the harness to completion on a seeded current-thread Tokio runtime.
    ///
    /// ## Returns
    /// - `Ok(())`: All actors completed successfully, or an actor requested
    ///   shutdown and all remaining actor exits were neutral.
    /// - `Err(Error)`: Database startup failed, no actors were configured, or
    ///   an actor returned or joined with an error.
    pub fn run(self) -> Result<(), Error> {
        let runtime_seed = self.rand.rng().next_u64();
        let startup_seed = self.rand.rng().next_u64();
        let clock_seed = self.rand.rng().next_u64();
        let system_clock: Arc<dyn SystemClock> = self.system_clock.clone();
        let clock_advance_ms = self.clock_advance_ms.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .rng_seed(RngSeed::from_bytes(&runtime_seed.to_le_bytes()))
            .build_local(Default::default())
            .expect("failed to build dst harness runtime");
        runtime.block_on(async move {
            let clock_driver = ClockDriver::spawn(
                system_clock,
                Arc::new(DbRand::new(clock_seed)),
                clock_advance_ms,
            );
            let result = self.run_inner(startup_seed).await;
            clock_driver.shutdown().await;
            result
        })
    }

    async fn run_inner(self, startup_seed: u64) -> Result<(), Error> {
        let Harness {
            name,
            rand,
            system_clock,
            path,
            main_object_store,
            wal_object_store,
            clock_advance_ms: _,
            startup_factory,
            actors,
        } = self;

        assert!(
            !actors.is_empty(),
            "dst harness requires at least one actor"
        );

        let seed = rand.seed();
        let path = path.unwrap_or_else(|| Path::from(format!("dst/{name}/seed-{seed:016x}")));
        let system_clock: Arc<dyn SystemClock> = system_clock;
        let fp_registry = Arc::new(FailPointRegistry::new());
        let main_object_store: Arc<dyn ObjectStore> = Arc::new(ClockedObjectStore::new(
            main_object_store.clone(),
            system_clock.clone(),
        ));
        let wal_object_store = wal_object_store.map(|store| {
            Arc::new(ClockedObjectStore::new(store.clone(), system_clock.clone()))
                as Arc<dyn ObjectStore>
        });

        let startup_ctx = StartupCtx {
            path: path.clone(),
            main_object_store: Arc::clone(&main_object_store),
            wal_object_store: wal_object_store.clone(),
            system_clock: Arc::clone(&system_clock),
            fp_registry: Arc::clone(&fp_registry),
            rand: Arc::new(DbRand::new(startup_seed)),
        };

        let db = startup_factory(startup_ctx.clone()).await?;

        let shared = HarnessCtx {
            startup_ctx,
            db_slot: Arc::new(RwLock::new(db)),
            compactor_slot: Arc::new(RwLock::new(None)),
            shutdown_token: CancellationToken::new(),
        };

        // Spawn one task per registered actor and track them in a JoinSet.
        let mut join_set = JoinSet::new();
        let mut actor_names = HashMap::new();
        for registration in actors {
            let actor_seed = rand.rng().next_u64();
            let name = registration.name;
            let mut actor = registration.actor;
            let ctx = ActorCtx {
                name: name.clone(),
                rand: Arc::new(DbRand::new(actor_seed)),
                shared: shared.clone(),
            };
            let handle = join_set.spawn(async move {
                let shutdown_token = ctx.shutdown_token();
                while !shutdown_token.is_cancelled() {
                    actor.run(&ctx).await?;
                    // Keep hot actors from monopolizing the current-thread runtime when
                    // their awaited operations complete without parking.
                    tokio::task::yield_now().await;
                }
                actor.finish(&ctx).await
            });
            actor_names.insert(handle.id(), name);
        }

        // Wait for actors to complete and check their results as they finish.
        while let Some(result) = join_set.join_next_with_id().await {
            match result {
                // An actor completed successfully.
                Ok((id, Ok(()))) => {
                    actor_names.remove(&id);
                }
                // An actor returned an error.
                Ok((id, Err(error))) => {
                    actor_names.remove(&id);
                    shared.shutdown_token.cancel();
                    join_set.abort_all();
                    return Err(error);
                }
                // An actor panicked or was aborted.
                Err(error) => {
                    let id = error.id();
                    let actor_name = actor_names
                        .remove(&id)
                        .unwrap_or_else(|| format!("task {id:?}"));
                    // If task was aborted due to shutdown, it's OK.
                    if shared.shutdown_token.is_cancelled() && error.is_cancelled() {
                        continue;
                    }
                    join_set.abort_all();
                    panic!("dst harness actor {actor_name} failed: {error}");
                }
            }
        }

        let db_result = shared.db_slot.write().close().await;
        let compactor_result = match shared.compactor_slot.write().take() {
            Some(compactor) => compactor.stop().await,
            None => Ok(()),
        };
        db_result?;
        compactor_result
    }
}
