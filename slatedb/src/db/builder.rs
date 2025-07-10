//! This module provides a builder for creating new Db instances.
//!
//! The `DbBuilder` struct is used to configure and open a SlateDB database.
//! It provides a fluent API for setting various options and components.
//!
//! # Examples
//!
//! Basic usage of the `DbBuilder` struct:
//!
//! ```
//! use slatedb::{Db, SlateDBError};
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SlateDBError> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::builder("test_db", object_store)
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!
//! Example with custom settings:
//!
//! ```
//! use slatedb::{Db, config::Settings, SlateDBError};
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SlateDBError> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::builder("test_db", object_store)
//!         .with_settings(Settings {
//!             min_filter_keys: 2000,
//!             ..Default::default()
//!         })
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!
//! Example with a custom block cache:
//!
//! ```
//! use slatedb::{Db, SlateDBError};
//! use slatedb::object_store::memory::InMemory;
//! use slatedb::db_cache::moka::MokaCache;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SlateDBError> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::builder("test_db", object_store)
//!         .with_block_cache(Arc::new(MokaCache::new()))
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!
//! Example with a custom clock:
//!
//! ```
//! use slatedb::{Db, SlateDBError};
//! use slatedb::clock::DefaultLogicalClock;
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SlateDBError> {
//!     let object_store = Arc::new(InMemory::new());
//!     let clock = Arc::new(DefaultLogicalClock::new());
//!     let db = Db::builder("test_db", object_store)
//!         .with_logical_clock(clock)
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!
//! Example with a custom SST block size:
//!
//! ```
//! use slatedb::{Db, SlateDBError};
//! use slatedb::config::SstBlockSize;
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SlateDBError> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::builder("test_db", object_store)
//!         .with_sst_block_size(SstBlockSize::Block8Kib) // 8KiB blocks
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!
use std::collections::HashMap;
use std::sync::Arc;

use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::admin::Admin;
use crate::cached_object_store::stats::CachedObjectStoreStats;
use crate::cached_object_store::CachedObjectStore;
use crate::cached_object_store::FsCacheStorage;
use crate::clock::DefaultLogicalClock;
use crate::clock::DefaultSystemClock;
use crate::clock::LogicalClock;
use crate::clock::SystemClock;
use crate::compactor::SizeTieredCompactionSchedulerSupplier;
use crate::compactor::{CompactionSchedulerSupplier, Compactor};
use crate::config::default_block_cache;
use crate::config::CompactorOptions;
use crate::config::GarbageCollectorOptions;
use crate::config::{Settings, SstBlockSize};
use crate::db::Db;
use crate::db::DbInner;
use crate::db_cache::{DbCache, DbCacheWrapper};
use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use crate::garbage_collector::GarbageCollector;
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::object_stores::ObjectStores;
use crate::paths::PathResolver;
use crate::rand::DbRand;
use crate::sst::SsTableFormat;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::utils::bg_task_result_into_err;
use crate::utils::spawn_bg_task;

/// A builder for creating a new Db instance.
///
/// This builder provides a fluent API for configuring and opening a SlateDB database.
/// It separates the concerns of configuration options (settings) and components.
pub struct DbBuilder<P: Into<Path>> {
    path: P,
    settings: Settings,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    block_cache: Option<Arc<dyn DbCache>>,
    logical_clock: Option<Arc<dyn LogicalClock>>,
    system_clock: Option<Arc<dyn SystemClock>>,
    gc_runtime: Option<Handle>,
    compaction_runtime: Option<Handle>,
    compaction_scheduler_supplier: Option<Arc<dyn CompactionSchedulerSupplier>>,
    fp_registry: Arc<FailPointRegistry>,
    cancellation_token: CancellationToken,
    seed: Option<u64>,
    sst_block_size: Option<SstBlockSize>,
}

impl<P: Into<Path>> DbBuilder<P> {
    /// Creates a new builder for a database at the given path.
    pub fn new(path: P, main_object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            main_object_store,
            settings: Settings::default(),
            wal_object_store: None,
            block_cache: None,
            logical_clock: None,
            system_clock: None,
            gc_runtime: None,
            compaction_runtime: None,
            compaction_scheduler_supplier: None,
            fp_registry: Arc::new(FailPointRegistry::new()),
            cancellation_token: CancellationToken::new(),
            seed: None,
            sst_block_size: None,
        }
    }

    /// Sets the database settings.
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Sets the separate object store dedicated specifically for WAL.
    ///
    /// NOTE: WAL durability and availability properties depend on the properties
    /// of the underlying object store. Make sure the configured object store is
    /// durable and available enough for your use case.
    pub fn with_wal_object_store(mut self, wal_object_store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(wal_object_store);
        self
    }

    /// Sets the block cache to use for the database.
    pub fn with_block_cache(mut self, block_cache: Arc<dyn DbCache>) -> Self {
        self.block_cache = Some(block_cache);
        self
    }

    /// Sets the logical clock to use for the database. Logical timestamps are used for
    /// TTL expiration. If unset, SlateDB defaults to using system time.
    pub fn with_logical_clock(mut self, clock: Arc<dyn LogicalClock>) -> Self {
        self.logical_clock = Some(clock);
        self
    }

    /// Sets the system clock to use for the database. System timestamps are used for
    /// scheduling operations such as compaction and garbage collection.
    pub fn with_system_clock(mut self, clock: Arc<dyn SystemClock>) -> Self {
        self.system_clock = Some(clock);
        self
    }

    /// Sets the garbage collection runtime to use for the database.
    pub fn with_gc_runtime(mut self, gc_runtime: Handle) -> Self {
        self.gc_runtime = Some(gc_runtime);
        self
    }

    /// Sets the compaction runtime to use for the database.
    pub fn with_compaction_runtime(mut self, compaction_runtime: Handle) -> Self {
        self.compaction_runtime = Some(compaction_runtime);
        self
    }

    pub fn with_compaction_scheduler_supplier(
        mut self,
        compaction_scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
    ) -> Self {
        self.compaction_scheduler_supplier = Some(compaction_scheduler_supplier);
        self
    }

    /// Sets the fail point registry to use for the database.
    pub fn with_fp_registry(mut self, fp_registry: Arc<FailPointRegistry>) -> Self {
        self.fp_registry = fp_registry;
        self
    }

    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = cancellation_token;
        self
    }

    /// Sets the seed to use for the database's random number generator. All random behavior
    /// in SlateDB will use randomm number generators based off of this seed. This includes
    /// random bytes for UUIDs and ULIDS, as well as random pickers in cache eviction policies.
    ///
    /// If not set, SlateDB uses the OS's random number generator to generate a seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Sets the block size for SSTable blocks. Blocks are the unit of reading
    /// and caching in SlateDB. Smaller blocks can reduce read amplification but
    /// may increase metadata overhead. Larger blocks are more efficient for
    /// sequential scans but may waste bandwidth for point lookups.
    ///
    /// Note: When compression is enabled, blocks are compressed individually.
    /// Larger blocks typically achieve better compression ratios.
    ///
    /// # Arguments
    ///
    /// * `block_size` - The block size variant to use (1KB, 2KB, 4KB, 8KB, 16KB, 32KB, or 64KB).
    ///
    /// # Returns
    ///
    /// The builder instance for chaining.
    pub fn with_sst_block_size(mut self, block_size: SstBlockSize) -> Self {
        self.sst_block_size = Some(block_size);
        self
    }

    /// Builds and opens the database.
    pub async fn build(self) -> Result<Db, SlateDBError> {
        let path = self.path.into();
        // TODO: proper URI generation, for now it works just as a flag
        let wal_object_store_uri = self.wal_object_store.as_ref().map(|_| String::new());

        // Log the database opening
        if let Ok(settings_json) = self.settings.to_json_string() {
            info!(?path, settings = settings_json, "Opening SlateDB database");
        } else {
            info!(?path, ?self.settings, "Opening SlateDB database");
        }

        let rand = Arc::new(self.seed.map(DbRand::new).unwrap_or_default());

        let logical_clock = self
            .logical_clock
            .unwrap_or_else(|| Arc::new(DefaultLogicalClock::new()));
        let block_cache = self.block_cache.or_else(default_block_cache);

        let system_clock = self
            .system_clock
            .unwrap_or_else(|| Arc::new(DefaultSystemClock::new()));

        // Setup the components
        let stat_registry = Arc::new(StatRegistry::new());
        let sst_format = SsTableFormat {
            min_filter_keys: self.settings.min_filter_keys,
            filter_bits_per_key: self.settings.filter_bits_per_key,
            compression_codec: self.settings.compression_codec,
            block_size: self.sst_block_size.unwrap_or_default().as_bytes(),
            ..SsTableFormat::default()
        };

        // Setup object store with optional caching
        let maybe_cached_main_object_store =
            match &self.settings.object_store_cache_options.root_folder {
                None => self.main_object_store.clone(),
                Some(cache_root_folder) => {
                    let stats = Arc::new(CachedObjectStoreStats::new(stat_registry.as_ref()));
                    let cache_storage = Arc::new(FsCacheStorage::new(
                        cache_root_folder.clone(),
                        self.settings
                            .object_store_cache_options
                            .max_cache_size_bytes,
                        self.settings.object_store_cache_options.scan_interval,
                        stats.clone(),
                        system_clock.clone(),
                        rand.clone(),
                    ));

                    let cached_main_object_store = CachedObjectStore::new(
                        self.main_object_store.clone(),
                        cache_storage,
                        self.settings.object_store_cache_options.part_size_bytes,
                        stats.clone(),
                    )?;
                    cached_main_object_store.start_evictor().await;
                    cached_main_object_store
                }
            };

        // Setup the manifest store and load latest manifest
        let manifest_store = Arc::new(ManifestStore::new(
            &path,
            maybe_cached_main_object_store.clone(),
        ));
        let latest_manifest = StoredManifest::try_load(manifest_store.clone()).await?;

        // Validate WAL object store configuration
        if let Some(latest_manifest) = &latest_manifest {
            if latest_manifest.db_state().wal_object_store_uri != wal_object_store_uri {
                return Err(SlateDBError::Unsupported(String::from(
                    "WAL object store reconfiguration is not supported",
                )));
            }
        }

        // Extract external SSTs from manifest if available
        let external_ssts = match &latest_manifest {
            Some(latest_stored_manifest) => {
                let mut external_ssts = HashMap::new();
                for external_db in &latest_stored_manifest.manifest().external_dbs {
                    for id in &external_db.sst_ids {
                        external_ssts.insert(*id, external_db.path.clone().into());
                    }
                }
                external_ssts
            }
            None => HashMap::new(),
        };

        // Create path resolver and table store
        let path_resolver = PathResolver::new_with_external_ssts(path.clone(), external_ssts);
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(
                maybe_cached_main_object_store.clone(),
                self.wal_object_store.clone(),
            ),
            sst_format.clone(),
            path_resolver.clone(),
            self.fp_registry.clone(),
            block_cache.as_ref().map(|c| {
                Arc::new(DbCacheWrapper::new(c.clone(), stat_registry.as_ref())) as Arc<dyn DbCache>
            }),
        ));

        // Get next WAL ID before writing manifest
        let replay_after_wal_id = match &latest_manifest {
            Some(latest_stored_manifest) => latest_stored_manifest.db_state().replay_after_wal_id,
            None => 0,
        };
        let next_wal_id = table_store.next_wal_sst_id(replay_after_wal_id).await?;

        // Initialize the database
        let stored_manifest = match latest_manifest {
            Some(manifest) => manifest,
            None => {
                let state = CoreDbState::new_with_wal_object_store(wal_object_store_uri);
                StoredManifest::create_new_db(manifest_store.clone(), state).await?
            }
        };
        let mut manifest =
            FenceableManifest::init_writer(stored_manifest, self.settings.manifest_update_timeout)
                .await?;

        // Setup communication channels
        let (memtable_flush_tx, memtable_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let (write_tx, write_rx) = tokio::sync::mpsc::unbounded_channel();

        // Create the database inner state
        let inner = Arc::new(
            DbInner::new(
                self.settings.clone(),
                logical_clock,
                system_clock.clone(),
                rand.clone(),
                table_store.clone(),
                manifest.prepare_dirty()?,
                memtable_flush_tx,
                write_tx,
                stat_registry,
            )
            .await?,
        );

        // Fence writers if WAL is enabled
        if inner.wal_enabled {
            inner.fence_writers(&mut manifest, next_wal_id).await?;
        }

        // Setup background tasks
        let tokio_handle = Handle::current();
        if inner.wal_enabled {
            inner.wal_buffer.start_background().await?;
        };

        let memtable_flush_task =
            inner.spawn_memtable_flush_task(manifest, memtable_flush_rx, &tokio_handle);
        let write_task = inner.spawn_write_task(write_rx, &tokio_handle);

        // Not to pollute the cache during compaction or GC
        let uncached_table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(
                self.main_object_store.clone(),
                self.wal_object_store.clone(),
            ),
            sst_format,
            path_resolver,
            self.fp_registry.clone(),
            None,
        ));

        // To keep backwards compatibility, check if the compaction_scheduler_supplier or compactor_options are set.
        // If either are set, we need to initialize the compactor.
        let compactor_task = if self.compaction_scheduler_supplier.is_some()
            || self.settings.compactor_options.is_some()
        {
            let compactor_options = self.settings.compactor_options.unwrap_or_default();
            let compaction_handle = self
                .compaction_runtime
                .unwrap_or_else(|| tokio_handle.clone());
            let scheduler_supplier = self
                .compaction_scheduler_supplier
                .unwrap_or_else(|| Arc::new(SizeTieredCompactionSchedulerSupplier::default()));
            let cleanup_inner = inner.clone();
            let compactor = Compactor::new(
                manifest_store.clone(),
                uncached_table_store.clone(),
                compactor_options.clone(),
                scheduler_supplier,
                rand.clone(),
                inner.stat_registry.clone(),
                system_clock.clone(),
                self.cancellation_token.clone(),
            );
            let compactor_task = spawn_bg_task(
                // Spawn the main event loop on the main tokio runtime
                &tokio_handle,
                move |result: &Result<(), SlateDBError>| {
                    let err = bg_task_result_into_err(result);
                    warn!("compactor thread exited with {:?}", err);
                    let mut state = cleanup_inner.state.write();
                    state.record_fatal_error(err.clone())
                },
                // Spawn the compactor on the compaction runtime
                async move { compactor.run_async_task(compaction_handle).await },
            );
            Some(compactor_task)
        } else {
            None
        };

        // To keep backwards compatibility, check if the gc_runtime or garbage_collector_options are set.
        // If either are set, we need to initialize the garbage collector.
        let garbage_collector_task =
            if self.settings.garbage_collector_options.is_some() || self.gc_runtime.is_some() {
                let gc_options = self.settings.garbage_collector_options.unwrap_or_default();
                let gc_handle = self.gc_runtime.unwrap_or_else(|| tokio_handle.clone());
                let cleanup_inner = inner.clone();
                let gc = GarbageCollector::new(
                    manifest_store.clone(),
                    uncached_table_store.clone(),
                    gc_options,
                    inner.stat_registry.clone(),
                    system_clock.clone(),
                    self.cancellation_token.clone(),
                );
                let garbage_collector_task = spawn_bg_task(
                    &gc_handle,
                    move |result| {
                        let err = bg_task_result_into_err(result);
                        warn!("GC thread exited with {:?}", err);
                        let mut state = cleanup_inner.state.write();
                        state.record_fatal_error(err.clone())
                    },
                    async move { gc.run_async_task().await },
                );
                Some(garbage_collector_task)
            } else {
                None
            };

        // Replay WAL
        inner.replay_wal().await?;

        // Create and return the Db instance
        Ok(Db {
            inner,
            memtable_flush_task: Mutex::new(memtable_flush_task),
            write_task: Mutex::new(write_task),
            compactor_task: Mutex::new(compactor_task),
            garbage_collector_task: Mutex::new(garbage_collector_task),
            cancellation_token: self.cancellation_token,
        })
    }
}

/// Builder for creating new Admin instances.
///
/// This provides a fluent API for configuring an Admin object.
pub struct AdminBuilder<P: Into<Path>> {
    path: P,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
}

impl<P: Into<Path>> AdminBuilder<P> {
    /// Creates a new AdminBuilder with the given path and object store.
    pub fn new(path: P, main_object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            main_object_store,
            wal_object_store: None,
            system_clock: Arc::new(DefaultSystemClock::new()),
            rand: Arc::new(DbRand::default()),
        }
    }

    /// Sets the system clock to use for administrative functions.
    pub fn with_system_clock(mut self, system_clock: Arc<dyn SystemClock>) -> Self {
        self.system_clock = system_clock;
        self
    }

    /// Sets the random number generator to use for randomness.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rand = Arc::new(DbRand::new(seed));
        self
    }

    /// Builds and returns an Admin instance.
    pub fn build(self) -> Admin {
        Admin {
            path: self.path.into(),
            object_stores: ObjectStores::new(self.main_object_store, self.wal_object_store),
            system_clock: self.system_clock,
            rand: self.rand,
        }
    }
}

/// Builder for creating new GarbageCollector instances.
///
/// This provides a fluent API for configuring a GarbageCollector object.
pub struct GarbageCollectorBuilder<P: Into<Path>> {
    path: P,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    options: GarbageCollectorOptions,
    stat_registry: Arc<StatRegistry>,
    cancellation_token: CancellationToken,
    system_clock: Arc<dyn SystemClock>,
}

impl<P: Into<Path>> GarbageCollectorBuilder<P> {
    pub fn new(path: P, main_object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            main_object_store,
            wal_object_store: None,
            options: GarbageCollectorOptions::default(),
            stat_registry: Arc::new(StatRegistry::new()),
            cancellation_token: CancellationToken::new(),
            system_clock: Arc::new(DefaultSystemClock::default()),
        }
    }

    /// Sets the options to use for the garbage collector.
    pub fn with_options(mut self, options: GarbageCollectorOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the stats registry to use for the garbage collector.
    #[allow(unused)]
    pub fn with_stat_registry(mut self, stat_registry: Arc<StatRegistry>) -> Self {
        self.stat_registry = stat_registry;
        self
    }

    /// Sets the system clock to use for the garbage collector.
    pub fn with_system_clock(mut self, system_clock: Arc<dyn SystemClock>) -> Self {
        self.system_clock = system_clock;
        self
    }

    /// Sets the cancellation token to use for the garbage collector.
    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = cancellation_token;
        self
    }

    /// Sets the WAL object store to use for the garbage collector.
    #[allow(unused)]
    pub fn with_wal_object_store(mut self, wal_object_store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(wal_object_store);
        self
    }

    /// Builds and returns a GarbageCollector instance.
    pub fn build(self) -> GarbageCollector {
        let path: Path = self.path.into();
        let manifest_store = Arc::new(ManifestStore::new(&path, self.main_object_store.clone()));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(
                self.main_object_store.clone(),
                self.wal_object_store.clone(),
            ),
            SsTableFormat::default(), // read only SSTs can use default
            path,
            None, // no need for cache in GC
        ));
        GarbageCollector::new(
            manifest_store,
            table_store,
            self.options,
            self.stat_registry,
            self.system_clock,
            self.cancellation_token,
        )
    }
}

/// Builder for creating new Compactor instances.
///
/// This provides a fluent API for configuring a Compactor object.
pub struct CompactorBuilder<P: Into<Path>> {
    path: P,
    main_object_store: Arc<dyn ObjectStore>,
    tokio_handle: Handle,
    options: CompactorOptions,
    scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
    rand: Arc<DbRand>,
    stat_registry: Arc<StatRegistry>,
    cancellation_token: CancellationToken,
    system_clock: Arc<dyn SystemClock>,
}

#[allow(unused)]
impl<P: Into<Path>> CompactorBuilder<P> {
    pub fn new(path: P, main_object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            main_object_store,
            tokio_handle: Handle::current(),
            options: CompactorOptions::default(),
            scheduler_supplier: Arc::new(SizeTieredCompactionSchedulerSupplier::default()),
            rand: Arc::new(DbRand::default()),
            stat_registry: Arc::new(StatRegistry::new()),
            cancellation_token: CancellationToken::new(),
            system_clock: Arc::new(DefaultSystemClock::default()),
        }
    }

    /// Sets the tokio handle to use for background tasks.
    #[allow(unused)]
    pub fn with_tokio_handle(mut self, tokio_handle: Handle) -> Self {
        self.tokio_handle = tokio_handle;
        self
    }

    /// Sets the options to use for the compactor.
    pub fn with_options(mut self, options: CompactorOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the stats registry to use for the compactor.
    #[allow(unused)]
    pub fn with_stat_registry(mut self, stat_registry: Arc<StatRegistry>) -> Self {
        self.stat_registry = stat_registry;
        self
    }

    /// Sets the system clock to use for the compactor.
    pub fn with_system_clock(mut self, system_clock: Arc<dyn SystemClock>) -> Self {
        self.system_clock = system_clock;
        self
    }

    /// Sets the cancellation token to use for the compactor.
    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = cancellation_token;
        self
    }

    /// Sets the random number generator to use for the compactor.
    pub fn with_rand(mut self, rand: Arc<DbRand>) -> Self {
        self.rand = rand;
        self
    }

    /// Builds and returns a Compactor instance.
    pub fn build(self) -> Compactor {
        let path: Path = self.path.into();
        let manifest_store = Arc::new(ManifestStore::new(&path, self.main_object_store.clone()));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(self.main_object_store.clone(), None),
            SsTableFormat::default(), // read only SSTs can use default
            path,
            None, // no need for cache in GC
        ));
        Compactor::new(
            manifest_store,
            table_store,
            self.options,
            self.scheduler_supplier,
            self.rand,
            self.stat_registry,
            self.system_clock,
            self.cancellation_token,
        )
    }
}
