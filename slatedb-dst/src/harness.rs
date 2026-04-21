//! Deterministic scenario test harness primitives for orchestrating seeded
//! SlateDB actors.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::RwLock;
use rand::RngCore;
use tokio::runtime::RngSeed;
use tokio::task::JoinSet;

use fail_parallel::FailPointRegistry;
use slatedb::{Db, DbRand, Error};
use slatedb_common::clock::SystemClock;
use slatedb_common::MockSystemClock;

use crate::clocked_object_store::ClockedObjectStore;

type DbFactoryFuture = Pin<Box<dyn Future<Output = Result<Arc<Db>, Error>> + Send + 'static>>;
type ActorFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;
type StartupFactory = Box<dyn FnOnce(StartupCtx) -> DbFactoryFuture + Send + 'static>;
type ActorFn = Arc<dyn Fn(ActorCtx) -> ActorFuture + Send + Sync + 'static>;

/// Controls whether an actor drives scenario completion or is aborted once all
/// foreground actors finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorType {
    /// Completion-driving actor. The harness waits for all foreground actors to
    /// finish successfully before considering the scenario complete.
    Foreground,
    /// Long-running support actor. Background actors may run indefinitely and
    /// are aborted when the foreground actors finish successfully.
    Background,
}

struct ActorRegistration {
    role: String,
    actor_type: ActorType,
    count: usize,
    actor: ActorFn,
}

struct ActorExit {
    actor_type: ActorType,
    result: Result<(), Error>,
}

#[derive(Clone)]
/// Per-actor context passed to each registered harness task.
///
/// The context exposes deterministic randomness, the shared database slot,
/// clock-wrapped object stores, and shared test infrastructure such as the
/// failpoint registry and mock clock.
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
}

#[derive(Clone)]
/// Startup context passed to the database factory configured with
/// [`Harness::with_db`].
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
    startup_factory: Option<StartupFactory>,
    actors: Vec<ActorRegistration>,
}

impl Harness {
    /// Creates a new deterministic harness builder.
    ///
    /// ## Arguments
    /// - `name`: Scenario name used when deriving the default database path.
    /// - `seed`: Root seed used to derive deterministic randomness for startup
    ///   and actor-local RNGs.
    ///
    /// ## Returns
    /// - `Harness`: A harness builder with an in-memory main object store and no
    ///   WAL object store configured.
    pub fn new(name: impl Into<String>, seed: u64) -> Self {
        Self {
            name: name.into(),
            rand: Arc::new(DbRand::new(seed)),
            system_clock: Arc::new(MockSystemClock::new()),
            path: None,
            main_object_store: Arc::new(InMemory::new()),
            wal_object_store: None,
            startup_factory: None,
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

    /// Installs the database startup factory executed before actors begin running.
    ///
    /// ## Arguments
    /// - `factory`: A function that receives a [`StartupCtx`] and returns the
    ///   database handle that actors should share.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn with_db<F, Fut>(mut self, factory: F) -> Self
    where
        F: FnOnce(StartupCtx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<Db>, Error>> + Send + 'static,
    {
        self.startup_factory = Some(Box::new(move |ctx| Box::pin(factory(ctx))));
        self
    }

    /// Registers a group of actor tasks that share the same role label.
    ///
    /// ## Arguments
    /// - `role`: Logical name assigned to each actor in the registration.
    /// - `actor_type`: Whether the registered actors drive completion or run as
    ///   abortable background support tasks.
    /// - `count`: Number of actor instances to spawn for the role.
    /// - `actor`: Async actor function to execute once per instance.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn actor<F, Fut>(
        mut self,
        role: impl Into<String>,
        actor_type: ActorType,
        count: usize,
        actor: F,
    ) -> Self
    where
        F: Fn(ActorCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        self.actors.push(ActorRegistration {
            role: role.into(),
            actor_type,
            count,
            actor: Arc::new(move |ctx| Box::pin(actor(ctx))),
        });
        self
    }

    /// Registers a group of actor tasks that receive shared external state.
    ///
    /// ## Arguments
    /// - `role`: Logical name assigned to each actor in the registration.
    /// - `actor_type`: Whether the registered actors drive completion or run as
    ///   abortable background support tasks.
    /// - `count`: Number of actor instances to spawn for the role.
    /// - `state`: Shared state cloned into each actor invocation.
    /// - `actor`: Async actor function that receives an [`ActorCtx`] and the
    ///   shared state.
    ///
    /// ## Returns
    /// - `Harness`: The updated harness builder.
    pub fn actor_with_state<T, F, Fut>(
        mut self,
        role: impl Into<String>,
        actor_type: ActorType,
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
            actor_type,
            count,
            actor: Arc::new(move |ctx| {
                let state = Arc::clone(&state);
                Box::pin(actor(ctx, state))
            }),
        });
        self
    }

    /// Runs the harness to completion on a seeded current-thread Tokio runtime.
    ///
    /// ## Returns
    /// - `Ok(())`: All foreground actors completed successfully.
    /// - `Err(Error)`: Database startup failed, no foreground actors were
    ///   configured, or an actor returned an error.
    ///
    /// # Panics
    /// Panics if [`Harness::with_db`] was not configured, if the runtime cannot
    /// be created, or if an actor task fails to join.
    pub fn run(self) -> Result<(), Error> {
        assert!(
            self.startup_factory.is_some(),
            "dst harness requires with_db(...) before run()"
        );
        let seed = self.rand.rng().next_u64();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
            .build_local(Default::default())
            .expect("failed to build dst harness runtime");
        runtime.block_on(self.run_inner())
    }

    async fn run_inner(self) -> Result<(), Error> {
        let Harness {
            name,
            rand,
            system_clock,
            path,
            main_object_store,
            wal_object_store,
            startup_factory,
            actors,
        } = self;

        let seed = rand.seed();
        let path = path.unwrap_or_else(|| Path::from(format!("dst/{name}/seed-{seed:016x}")));
        let system_clock: Arc<dyn SystemClock> = system_clock;
        let fp_registry = Arc::new(FailPointRegistry::new());
        let startup_seed = rand.rng().next_u64();
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

        let db = startup_factory.expect("validated before runtime startup")(startup_ctx).await?;

        let shared = HarnessCtx {
            path,
            main_object_store,
            wal_object_store,
            system_clock,
            fp_registry,
            db_slot: Arc::new(RwLock::new(db)),
        };

        let mut foreground_remaining = actors
            .iter()
            .filter(|registration| registration.actor_type == ActorType::Foreground)
            .map(|registration| registration.count)
            .sum::<usize>();
        let mut success_shutdown = false;
        let mut join_set = JoinSet::new();
        for registration in actors {
            for instance in 0..registration.count {
                let actor_seed = rand.rng().next_u64();
                let actor_type = registration.actor_type;
                let actor = Arc::clone(&registration.actor);
                let ctx = ActorCtx {
                    role: registration.role.clone(),
                    instance,
                    rand: Arc::new(DbRand::new(actor_seed)),
                    shared: shared.clone(),
                };
                join_set.spawn(async move {
                    let result = actor(ctx).await;
                    ActorExit { actor_type, result }
                });
            }
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(exit) => match exit.result {
                    Ok(()) => {
                        if exit.actor_type == ActorType::Foreground {
                            foreground_remaining -= 1;
                            if foreground_remaining == 0 && !success_shutdown {
                                success_shutdown = true;
                                join_set.abort_all();
                            }
                        }
                    }
                    Err(error) => {
                        join_set.abort_all();
                        return Err(error);
                    }
                },
                Err(error) if success_shutdown && error.is_cancelled() => {}
                Err(error) => {
                    join_set.abort_all();
                    panic!("dst actor task failed to join: {error}");
                }
            }
        }

        let db = Arc::clone(&shared.db_slot.read());
        db.close().await?;

        Ok(())
    }
}
