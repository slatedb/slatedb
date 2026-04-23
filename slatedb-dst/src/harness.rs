//! Deterministic scenario test harness primitives for orchestrating seeded
//! SlateDB actors.

use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

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
use slatedb::{Db, DbRand, Error};
use slatedb_common::clock::SystemClock;
use slatedb_common::MockSystemClock;

use crate::clocked_object_store::ClockedObjectStore;

type DbFactoryFuture = Pin<Box<dyn Future<Output = Result<Arc<Db>, Error>> + Send + 'static>>;
type ActorFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;
type StartupFactory = Box<dyn FnOnce(StartupCtx) -> DbFactoryFuture + Send + 'static>;
type ActorFn = Arc<dyn Fn(ActorCtx) -> ActorFuture + Send + Sync + 'static>;

struct ActorRegistration {
    role: String,
    count: usize,
    actor: ActorFn,
}

#[derive(Clone)]
/// Per-actor context passed to each registered harness task.
///
/// The context exposes deterministic randomness, the shared database slot,
/// clock-wrapped object stores, the shared shutdown token, and test
/// infrastructure such as the failpoint registry and mock clock.
pub struct ActorCtx {
    role: String,
    instance: usize,
    rand: Arc<DbRand>,
    shared: HarnessCtx,
}

impl ActorCtx {
    /// Returns the logical role assigned when the actor was registered.
    ///
    /// ## Returns
    /// - `&str`: The role label shared by all instances in the registration.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Returns the zero-based instance index within the actor's role.
    ///
    /// ## Returns
    /// - `usize`: The actor instance number for this role.
    pub fn instance(&self) -> usize {
        self.instance
    }

    /// Returns the actor-local deterministic random number generator.
    ///
    /// ## Returns
    /// - `&DbRand`: The seeded RNG derived from the harness seed for this actor.
    pub fn rand(&self) -> &DbRand {
        self.rand.as_ref()
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

    /// Advances the shared mock system clock by the provided duration.
    ///
    /// ## Arguments
    /// - `duration`: The amount of simulated time to add.
    pub async fn advance_time(&self, duration: Duration) {
        self.shared.system_clock.advance(duration).await;
    }

    /// Returns the root path used to open the database under test.
    ///
    /// ## Returns
    /// - `&Path`: The path configured for the harness run.
    pub fn path(&self) -> &Path {
        &self.shared.path
    }

    /// Returns the wrapped main object store used by the harness.
    ///
    /// ## Returns
    /// - `Arc<dyn ObjectStore>`: The main object store wrapped with
    ///   deterministic clock behavior.
    pub fn main_object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.shared.main_object_store)
    }

    /// Returns the wrapped WAL object store, if one was configured.
    ///
    /// ## Returns
    /// - `Option<Arc<dyn ObjectStore>>`: The WAL store wrapped with
    ///   deterministic clock behavior, or `None`.
    pub fn wal_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        self.shared.wal_object_store.clone()
    }

    /// Returns the shared system clock used by the harness.
    ///
    /// ## Returns
    /// - `Arc<dyn SystemClock>`: The clock backing time-sensitive test behavior.
    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        Arc::clone(&self.shared.system_clock)
    }

    /// Returns the shared failpoint registry for the harness run.
    ///
    /// ## Returns
    /// - `Arc<FailPointRegistry>`: The registry used to configure failpoints in
    ///   participating components.
    pub fn fp_registry(&self) -> Arc<FailPointRegistry> {
        Arc::clone(&self.shared.fp_registry)
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
    path: Path,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    system_clock: Arc<dyn SystemClock>,
    fp_registry: Arc<FailPointRegistry>,
    db_slot: Arc<RwLock<Arc<Db>>>,
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
                    if steps % 100 == 0 {
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

    /// Registers a group of actor tasks that share the same role label.
    ///
    /// ## Arguments
    /// - `role`: Logical name assigned to each actor in the registration.
    /// - `count`: Number of actor instances to spawn for the role.
    /// - `actor`: Async actor function to execute once per instance.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
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

    /// Registers a group of actor tasks that receive shared external state.
    ///
    /// ## Arguments
    /// - `role`: Logical name assigned to each actor in the registration.
    /// - `count`: Number of actor instances to spawn for the role.
    /// - `state`: State value cloned into each actor invocation.
    /// - `actor`: Async actor function that receives an [`ActorCtx`] and the
    ///   cloned state.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn actor_with_state<T, F, Fut>(
        mut self,
        role: impl Into<String>,
        count: usize,
        state: T,
        actor: F,
    ) -> Self
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(ActorCtx, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        self.actors.push(ActorRegistration {
            role: role.into(),
            count,
            actor: Arc::new(move |ctx| {
                let state = state.clone();
                Box::pin(actor(ctx, state))
            }),
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
    ///
    /// # Panics
    /// Panics if the runtime cannot be created.
    pub fn run(self) -> Result<(), Error> {
        let runtime_seed = self.rand.rng().next_u64();
        let startup_seed = self.rand.rng().next_u64();
        let clock_seed = self.rand.rng().next_u64();
        let system_clock: Arc<dyn SystemClock> = self.system_clock.clone();
        let clock_advance_ms = self.clock_advance_ms.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
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

        let db = startup_factory(startup_ctx).await?;

        let shared = HarnessCtx {
            path,
            main_object_store,
            wal_object_store,
            system_clock: system_clock.clone(),
            fp_registry,
            db_slot: Arc::new(RwLock::new(db)),
            shutdown_token: CancellationToken::new(),
        };

        let mut join_set = JoinSet::new();
        let mut actor_names = HashMap::new();
        for registration in actors {
            for instance in 0..registration.count {
                let actor_seed = rand.rng().next_u64();
                let actor = Arc::clone(&registration.actor);
                let role = registration.role.clone();
                let ctx = ActorCtx {
                    role: role.clone(),
                    instance,
                    rand: Arc::new(DbRand::new(actor_seed)),
                    shared: shared.clone(),
                };
                let actor_name = format!("{role}[{instance}]");
                let handle = join_set.spawn(async move { actor(ctx).await });
                actor_names.insert(handle.id(), actor_name);
            }
        }

        let shutdown_token = shared.shutdown_token.clone();
        let mut shutdown_requested = false;
        while let Some(result) = {
            if shutdown_requested {
                join_set.join_next_with_id().await
            } else {
                tokio::select! {
                    biased;
                    result = join_set.join_next_with_id() => result,
                    _ = shutdown_token.cancelled() => {
                        shutdown_requested = true;
                        join_set.abort_all();
                        join_set.join_next_with_id().await
                    }
                }
            }
        } {
            if !shutdown_requested && shutdown_token.is_cancelled() {
                shutdown_requested = true;
                join_set.abort_all();
            }

            match result {
                Ok((id, Ok(()))) => {
                    actor_names.remove(&id);
                }
                Err(error) => {
                    let id = error.id();
                    let actor_name = actor_names
                        .remove(&id)
                        .unwrap_or_else(|| format!("task {id:?}"));
                    if shutdown_requested && error.is_cancelled() {
                        continue;
                    }
                    join_set.abort_all();
                    panic!("dst harness actor {actor_name} failed: {error}");
                }
                Ok((id, Err(error))) => {
                    actor_names.remove(&id);
                    join_set.abort_all();
                    return Err(error);
                }
            }
        }

        let db = Arc::clone(&shared.db_slot.read());
        db.close().await
    }
}
