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
//! use slatedb::{Db, Error};
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
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
//! use slatedb::{Db, config::Settings, Error};
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
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
//! use slatedb::{Db, Error};
//! use slatedb::db_cache::foyer::FoyerCache;
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::builder("test_db", object_store)
//!         .with_memory_cache(Arc::new(FoyerCache::new()))
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!
//! Example with a custom clock:
//!
//! ```
//! use slatedb::{Db, Error};
//! use slatedb::clock::DefaultLogicalClock;
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
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
//! use slatedb::{Db, Error};
//! use slatedb::config::SstBlockSize;
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
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
use log::info;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::admin::Admin;
use crate::batch_write::WriteBatchEventHandler;
use crate::batch_write::WRITE_BATCH_TASK_NAME;
use crate::cached_object_store::stats::CachedObjectStoreStats;
use crate::cached_object_store::CachedObjectStore;
use crate::cached_object_store::FsCacheStorage;
use crate::clock::DefaultLogicalClock;
use crate::clock::DefaultSystemClock;
use crate::clock::LogicalClock;
use crate::clock::SystemClock;
use crate::compactor::CompactorEventHandler;
use crate::compactor::SizeTieredCompactionSchedulerSupplier;
use crate::compactor::COMPACTOR_TASK_NAME;
use crate::compactor::{CompactionSchedulerSupplier, Compactor};
use crate::compactor_executor::TokioCompactionExecutor;
use crate::compactor_stats::CompactionStats;
use crate::config::default_block_cache;
use crate::config::default_meta_cache;
use crate::config::CompactorOptions;
use crate::config::GarbageCollectorOptions;
use crate::config::SizeTieredCompactionSchedulerOptions;
use crate::config::{Settings, SstBlockSize};
use crate::db::Db;
use crate::db::DbInner;
use crate::db_cache::SplitCache;
use crate::db_cache::{DbCache, DbCacheWrapper};
use crate::db_state::CoreDbState;
use crate::dispatcher::MessageHandlerExecutor;
use crate::error::SlateDBError;
use crate::garbage_collector::GarbageCollector;
use crate::garbage_collector::GC_TASK_NAME;
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::mem_table_flush::MemtableFlusher;
use crate::mem_table_flush::MEMTABLE_FLUSHER_TASK_NAME;
use crate::merge_operator::MergeOperatorType;
use crate::object_stores::ObjectStores;
use crate::paths::PathResolver;
use crate::rand::DbRand;
use crate::retrying_object_store::RetryingObjectStore;
use crate::sst::SsTableFormat;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::transactional_object::TransactionalObject;
use crate::utils::WatchableOnceCell;

/// A builder for creating a new Db instance.
///
/// This builder provides a fluent API for configuring and opening a SlateDB database.
/// It separates the concerns of configuration options (settings) and components.
pub struct DbBuilder<P: Into<Path>> {
    path: P,
    settings: Settings,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    memory_cache: Option<Arc<dyn DbCache>>,
    logical_clock: Option<Arc<dyn LogicalClock>>,
    system_clock: Option<Arc<dyn SystemClock>>,
    gc_runtime: Option<Handle>,
    compaction_runtime: Option<Handle>,
    compaction_scheduler_supplier: Option<Arc<dyn CompactionSchedulerSupplier>>,
    fp_registry: Arc<FailPointRegistry>,
    seed: Option<u64>,
    sst_block_size: Option<SstBlockSize>,
    merge_operator: Option<MergeOperatorType>,
}

impl<P: Into<Path>> DbBuilder<P> {
    /// Creates a new builder for a database at the given path.
    pub fn new(path: P, main_object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            main_object_store,
            settings: Settings::default(),
            wal_object_store: None,
            memory_cache: None,
            logical_clock: None,
            system_clock: None,
            gc_runtime: None,
            compaction_runtime: None,
            compaction_scheduler_supplier: None,
            fp_registry: Arc::new(FailPointRegistry::new()),
            seed: None,
            sst_block_size: None,
            merge_operator: None,
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

    /// Sets the memory cache to use for the database.
    ///
    /// SlateDB uses a cache to efficiently store and retrieve blocks and SST metadata locally.
    /// [`slatedb::db_cache::SplitCache`] is used by default.
    pub fn with_memory_cache(mut self, memory_cache: Arc<dyn DbCache>) -> Self {
        self.memory_cache = Some(memory_cache);
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

    /// Sets the merge operator to use for the database. The merge operator allows
    /// applications to bypass the traditional read/modify/write cycle by expressing
    /// partial updates using an associative operator.
    ///
    /// # Arguments
    ///
    /// * `merge_operator` - An Arc-wrapped merge operator implementation.
    ///
    /// # Returns
    ///
    /// The builder instance for chaining.
    pub fn with_merge_operator(mut self, merge_operator: MergeOperatorType) -> Self {
        self.merge_operator = Some(merge_operator);
        self
    }

    /// Builds and opens the database.
    pub async fn build(self) -> Result<Db, crate::Error> {
        let path = self.path.into();
        // TODO: proper URI generation, for now it works just as a flag
        let wal_object_store_uri = self.wal_object_store.as_ref().map(|_| String::new());

        let retrying_main_object_store = Arc::new(RetryingObjectStore::new(self.main_object_store));
        let retrying_wal_object_store: Option<Arc<dyn ObjectStore>> = self
            .wal_object_store
            .map(|s| Arc::new(RetryingObjectStore::new(s)) as Arc<dyn ObjectStore>);

        // Log the database opening
        if let Ok(settings_json) = self.settings.to_json_string() {
            info!(
                "opening SlateDB database [path={}, settings={}]",
                path, settings_json
            );
        } else {
            info!(
                "opening SlateDB database [path={}, settings={:?}]",
                path, self.settings
            );
        }

        let rand = Arc::new(self.seed.map(DbRand::new).unwrap_or_default());

        let logical_clock = self
            .logical_clock
            .unwrap_or_else(|| Arc::new(DefaultLogicalClock::new()));
        let memory_cache = self.memory_cache.or_else(|| {
            let block_cache = default_block_cache();
            let meta_cache = default_meta_cache();
            Some(Arc::new(
                SplitCache::new()
                    .with_block_cache(block_cache)
                    .with_meta_cache(meta_cache)
                    .build(),
            ))
        });

        let system_clock = self
            .system_clock
            .unwrap_or_else(|| Arc::new(DefaultSystemClock::new()));

        let merge_operator = self.merge_operator.or(self.settings.merge_operator.clone());

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
        let cached_object_store = match &self.settings.object_store_cache_options.root_folder {
            None => None,
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

                let cached_object_store = CachedObjectStore::new(
                    retrying_main_object_store.clone(),
                    cache_storage,
                    self.settings.object_store_cache_options.part_size_bytes,
                    self.settings.object_store_cache_options.cache_puts,
                    stats.clone(),
                )?;
                cached_object_store.start_evictor().await;
                Some(cached_object_store)
            }
        };

        let maybe_cached_main_object_store: Arc<dyn ObjectStore> = match &cached_object_store {
            Some(cached_store) => cached_store.clone(),
            None => retrying_main_object_store.clone(),
        };

        // Setup the manifest store and load latest manifest
        let manifest_store = Arc::new(ManifestStore::new(
            &path,
            maybe_cached_main_object_store.clone(),
            system_clock.clone(),
        ));
        let latest_manifest = StoredManifest::try_load(manifest_store.clone()).await?;

        // Validate WAL object store configuration
        if let Some(latest_manifest) = &latest_manifest {
            if latest_manifest.db_state().wal_object_store_uri != wal_object_store_uri {
                return Err(SlateDBError::WalStoreReconfigurationError.into());
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
                retrying_wal_object_store.clone(),
            ),
            sst_format.clone(),
            path_resolver.clone(),
            self.fp_registry.clone(),
            memory_cache.as_ref().map(|c| {
                Arc::new(DbCacheWrapper::new(
                    c.clone(),
                    stat_registry.as_ref(),
                    system_clock.clone(),
                )) as Arc<dyn DbCache>
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
        let mut manifest = FenceableManifest::init_writer(
            stored_manifest,
            self.settings.manifest_update_timeout,
            system_clock.clone(),
        )
        .await?;

        // Setup communication channels
        let (memtable_flush_tx, memtable_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let (write_tx, write_rx) = tokio::sync::mpsc::unbounded_channel();

        // Create the database inner state
        let mut settings = self.settings.clone();
        settings.merge_operator = merge_operator.clone();
        let inner = Arc::new(
            DbInner::new(
                settings,
                logical_clock,
                system_clock.clone(),
                rand.clone(),
                table_store.clone(),
                manifest.manifest().clone(),
                memtable_flush_tx,
                write_tx,
                stat_registry,
                self.fp_registry.clone(),
                merge_operator.clone(),
            )
            .await?,
        );

        // Fence writers if WAL is enabled
        if inner.wal_enabled {
            inner.fence_writers(&mut manifest, next_wal_id).await?;
        }

        // Setup background tasks
        let tokio_handle = Handle::current();
        let task_executor = Arc::new(MessageHandlerExecutor::new(
            inner.clone().state.read().closed_result(),
            system_clock.clone(),
        ));
        if inner.wal_enabled {
            inner
                .wal_buffer
                .start_flush_task(task_executor.clone())
                .await?;
        };
        task_executor
            .add_handler(
                MEMTABLE_FLUSHER_TASK_NAME.to_string(),
                Box::new(MemtableFlusher::new(inner.clone(), manifest)?),
                memtable_flush_rx,
                &tokio_handle,
            )
            .expect("failed to spawn memtable flusher task");
        task_executor
            .add_handler(
                WRITE_BATCH_TASK_NAME.to_string(),
                Box::new(WriteBatchEventHandler::new(inner.clone())),
                write_rx,
                &tokio_handle,
            )
            .expect("failed to spawn write batch event handler task");

        // Not to pollute the cache during compaction or GC
        let uncached_table_store = Arc::new(TableStore::new_with_fp_registry(
            ObjectStores::new(
                retrying_main_object_store.clone(),
                retrying_wal_object_store.clone(),
            ),
            sst_format,
            path_resolver.clone(),
            self.fp_registry.clone(),
            None,
        ));

        // To keep backwards compatibility, check if the compaction_scheduler_supplier or compactor_options are set.
        // If either are set, we need to initialize the compactor.
        if self.compaction_scheduler_supplier.is_some() || self.settings.compactor_options.is_some()
        {
            let compactor_options = Arc::new(self.settings.compactor_options.unwrap_or_default());
            let compaction_handle = self
                .compaction_runtime
                .unwrap_or_else(|| tokio_handle.clone());
            let scheduler_supplier = self
                .compaction_scheduler_supplier
                .unwrap_or_else(default_compaction_scheduler_supplier);
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let scheduler = Arc::from(scheduler_supplier.compaction_scheduler(&compactor_options));
            let stats = Arc::new(CompactionStats::new(inner.stat_registry.clone()));
            let executor = Arc::new(TokioCompactionExecutor::new(
                compaction_handle,
                compactor_options.clone(),
                tx,
                uncached_table_store.clone(),
                rand.clone(),
                stats.clone(),
                system_clock.clone(),
                manifest_store.clone(),
                merge_operator.clone(),
            ));
            let handler = CompactorEventHandler::new(
                manifest_store.clone(),
                compactor_options.clone(),
                scheduler,
                executor,
                rand.clone(),
                stats.clone(),
                system_clock.clone(),
            )
            .await
            .expect("failed to create compactor");
            task_executor
                .add_handler(
                    COMPACTOR_TASK_NAME.to_string(),
                    Box::new(handler),
                    rx,
                    &tokio_handle,
                )
                .expect("failed to spawn compactor task");
        }

        // To keep backwards compatibility, check if the gc_runtime or garbage_collector_options are set.
        // If either are set, we need to initialize the garbage collector.
        if self.settings.garbage_collector_options.is_some() || self.gc_runtime.is_some() {
            let gc_options = self.settings.garbage_collector_options.unwrap_or_default();
            let gc = GarbageCollector::new(
                manifest_store.clone(),
                uncached_table_store.clone(),
                gc_options,
                inner.stat_registry.clone(),
                system_clock.clone(),
            );
            // Garbage collector only uses tickers, so pass in a dummy rx channel
            let (_, rx) = mpsc::unbounded_channel();
            task_executor
                .add_handler(GC_TASK_NAME.to_string(), Box::new(gc), rx, &tokio_handle)
                .expect("failed to spawn garbage collector task");
        }

        // Monitor background tasks
        task_executor.monitor_on(&tokio_handle)?;

        // Replay WAL
        let last_wal_id = inner.replay_wal().await?;
        inner.wal_buffer.init(last_wal_id);

        // Preload cache if enabled
        if let Some(cached_obj_store) = cached_object_store {
            inner
                .preload_cache(&cached_obj_store, &path_resolver)
                .await?;
        }

        // Create and return the Db instance
        Ok(Db {
            inner,
            task_executor,
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

    /// Sets the WAL object store to use for administrative functions.
    ///
    /// When configured, administrative operations that need to access WAL data
    /// (such as garbage collection) will use this object store instead of the
    /// main object store.
    pub fn with_wal_object_store(mut self, wal_object_store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(wal_object_store);
        self
    }

    /// Sets the random number generator to use for randomness.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rand = Arc::new(DbRand::new(seed));
        self
    }

    /// Builds and returns an Admin instance.
    pub fn build(self) -> Admin {
        // No retrying object stores here, since we don't want to retry admin operations
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

    /// Sets the WAL object store to use for the garbage collector.
    #[allow(unused)]
    pub fn with_wal_object_store(mut self, wal_object_store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(wal_object_store);
        self
    }

    /// Builds and returns a GarbageCollector instance.
    pub fn build(self) -> GarbageCollector {
        let path: Path = self.path.into();
        let retrying_main_object_store = Arc::new(RetryingObjectStore::new(self.main_object_store));
        let retrying_wal_object_store = self
            .wal_object_store
            .map(|s| Arc::new(RetryingObjectStore::new(s)) as Arc<dyn ObjectStore>);
        let manifest_store = Arc::new(ManifestStore::new(
            &path,
            retrying_main_object_store.clone(),
            self.system_clock.clone(),
        ));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(
                retrying_main_object_store.clone(),
                retrying_wal_object_store.clone(),
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
    scheduler_supplier: Option<Arc<dyn CompactionSchedulerSupplier>>,
    rand: Arc<DbRand>,
    stat_registry: Arc<StatRegistry>,
    system_clock: Arc<dyn SystemClock>,
    closed_result: WatchableOnceCell<Result<(), SlateDBError>>,
    merge_operator: Option<MergeOperatorType>,
}

#[allow(unused)]
impl<P: Into<Path>> CompactorBuilder<P> {
    pub fn new(path: P, main_object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            main_object_store,
            tokio_handle: Handle::current(),
            options: CompactorOptions::default(),
            scheduler_supplier: None,
            rand: Arc::new(DbRand::default()),
            stat_registry: Arc::new(StatRegistry::new()),
            system_clock: Arc::new(DefaultSystemClock::default()),
            closed_result: WatchableOnceCell::new(),
            merge_operator: None,
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

    /// Sets the random number generator to use for the compactor.
    pub fn with_rand(mut self, rand: Arc<DbRand>) -> Self {
        self.rand = rand;
        self
    }

    /// Sets the compaction scheduler supplier to use for the compactor.
    pub fn with_scheduler_supplier(
        mut self,
        scheduler_supplier: Arc<dyn CompactionSchedulerSupplier>,
    ) -> Self {
        self.scheduler_supplier = Some(scheduler_supplier);
        self
    }

    /// Sets the merge operator to use for the compactor.
    pub fn with_merge_operator(mut self, merge_operator: MergeOperatorType) -> Self {
        self.merge_operator = Some(merge_operator);
        self
    }

    /// Builds and returns a Compactor instance.
    pub fn build(self) -> Compactor {
        let path: Path = self.path.into();
        let retrying_main_object_store = Arc::new(RetryingObjectStore::new(self.main_object_store));
        let manifest_store = Arc::new(ManifestStore::new(
            &path,
            retrying_main_object_store.clone(),
            self.system_clock.clone(),
        ));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(retrying_main_object_store.clone(), None),
            SsTableFormat::default(), // read only SSTs can use default
            path,
            None, // no need for cache in GC
        ));

        let scheduler_supplier = self
            .scheduler_supplier
            .unwrap_or_else(default_compaction_scheduler_supplier);

        Compactor::new(
            manifest_store,
            table_store,
            self.options,
            scheduler_supplier,
            self.tokio_handle,
            self.rand,
            self.stat_registry,
            self.system_clock,
            self.closed_result,
            self.merge_operator,
        )
    }
}

fn default_compaction_scheduler_supplier() -> Arc<dyn CompactionSchedulerSupplier> {
    Arc::new(SizeTieredCompactionSchedulerSupplier::new(
        SizeTieredCompactionSchedulerOptions::default(),
    ))
}
