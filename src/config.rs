//! Configuration options for SlateDB.
//!
//! This module provides structures and functions to manage database configuration options,
//! including reading from files and environment variables.
//!
//! # Examples
//!
//! Loading the default configuration for `DbOptions`:
//!
//! ```rust
//! use slatedb::config::DbOptions;
//! let config = DbOptions::default();
//! ```
//!
//! Loading `DbOptions` from a specific file:
//!
//! ```rust
//! use slatedb::config::DbOptions;
//! let config = DbOptions::from_file("config.toml").expect("Failed to load options from file");
//! ```
//!
//! Loading `DbOptions` from environment variables:
//!
//! ```rust
//! use slatedb::config::DbOptions;
//! let config = DbOptions::from_env("SLATEDB_").expect("Failed to load options from env");
//! ```
//!
//! Loading `DbOptions` from predefined files, SlateDb.toml, SlateDb.json, SlateDb.yaml, or SlateDb.yml.
//! This method also merges any environment variable that starts with `SLATEDB_` to the final `DbOptions` struct:
//!
//! ```rust
//! use slatedb::config::DbOptions;
//! let config = DbOptions::load().expect("Failed to load options");
//! ```
//!
//! # Configuration formats
//!
//! SlateDB supports three configuration formats: TOML, JSON, and YAML.
//! Duration options in the configuration are represented as human-friendly strings,
//! allowing you to specify time intervals in a more intuitive way, such as "100ms" or "1s".
//!
//! Representing `DbOptions` with TOML:
//!
//! ```toml
//! flush_interval = "100ms"
//! wal_enabled = false
//! manifest_poll_interval = "1s"
//! min_filter_keys = 1000
//! filter_bits_per_key = 10
//! l0_sst_size_bytes = 67108864
//! l0_max_ssts = 8
//! max_unflushed_memtable = 2
//!
//! [compactor_options]
//! poll_interval = "5s"
//! max_sst_size = 1073741824
//! max_concurrent_compactions = 4
//!
//! [object_store_cache_options]
//! root_folder = "/tmp/slatedb-cache"
//! max_cache_size_bytes = 17179869184
//! part_size_bytes = 4194304
//! scan_interval = "3600s"
//!
//! [garbage_collector_options.manifest_options]
//! poll_interval = "300s"
//! min_age = "86400s"
//!
//! [garbage_collector_options.wal_options]
//! poll_interval = "60s"
//! min_age = "60s"
//!
//! [garbage_collector_options.compacted_options]
//! poll_interval = "300s"
//! min_age = "86400s"
//! ```
//!
//! Representing `DbOptions` with JSON:
//!
//! ```json
//!{
//!  "flush_interval": "100ms",
//!  "wal_enabled": false,
//!  "manifest_poll_interval": "1s",
//!  "min_filter_keys": 1000,
//!  "filter_bits_per_key": 10,
//!  "l0_sst_size_bytes": 67108864,
//!  "l0_max_ssts": 8,
//!  "max_unflushed_memtable": 2,
//!  "compactor_options": {
//!    "poll_interval": "5s",
//!    "max_sst_size": 1073741824,
//!    "max_concurrent_compactions": 4
//!  },
//!  "compression_codec": null,
//!  "object_store_cache_options": {
//!    "root_folder": "/tmp/slatedb-cache",
//!    "max_cache_size_bytes": 17179869184,
//!    "part_size_bytes": 4194304,
//!    "scan_interval": "3600s"
//!  },
//!  "garbage_collector_options": {
//!    "manifest_options": {
//!      "poll_interval": "300s",
//!      "min_age": "86400s"
//!    },
//!    "wal_options": {
//!      "poll_interval": "60s",
//!      "min_age": "60s"
//!    },
//!    "compacted_options": {
//!      "poll_interval": "300s",
//!      "min_age": "86400s"
//!    }
//!  }
//!}
//!```
//!
//! Representing `DbOptions` with YAML:
//!
//! ```yaml
//! flush_interval: '100ms'
//! wal_enabled: false
//! manifest_poll_interval: '1s'
//! min_filter_keys: 1000
//! filter_bits_per_key: 10
//! l0_sst_size_bytes: 67108864
//! l0_max_ssts: 8
//! max_unflushed_memtable: 2
//! compactor_options:
//!   poll_interval: '5s'
//!   max_sst_size: 1073741824
//!   max_concurrent_compactions: 4
//! compression_codec: null
//! object_store_cache_options:
//!   root_folder: /tmp/slatedb-cache
//!   max_cache_size_bytes: 17179869184
//!   part_size_bytes: 4194304
//!   scan_interval: '3600s'
//! garbage_collector_options:
//!   manifest_options:
//!     poll_interval: '300s'
//!     min_age: '86400s'
//!   wal_options:
//!     poll_interval: '60s'
//!     min_age: '60s'
//!   compacted_options:
//!     poll_interval: '300s'
//!     min_age: '86400s'
//! ```
//!
use duration_str::{deserialize_duration, deserialize_option_duration};
use figment::providers::{Env, Format, Json, Toml, Yaml};
use figment::{Figment, Metadata, Provider};
use serde::{Deserialize, Serialize, Serializer};
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{str::FromStr, time::Duration};
use tokio::runtime::Handle;

use crate::compactor::CompactionScheduler;
use crate::error::{DbOptionsError, SlateDBError};

use crate::db_cache::DbCache;
use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;

pub const DEFAULT_READ_OPTIONS: &ReadOptions = &ReadOptions::default();
pub const DEFAULT_WRITE_OPTIONS: &WriteOptions = &WriteOptions::default();

/// Whether reads see only writes that have been committed durably to the DB.  A
/// write is considered durably committed if all future calls to read are guaranteed
/// to serve the data written by the write, until some later durably committed write
/// updates the same key.
pub enum ReadLevel {
    /// Client reads will only see data that's been committed durably to the DB.
    Commited,

    /// Clients will see all writes, including those not yet durably committed to the
    /// DB.
    Uncommitted,
}

/// Configuration for client read operations. `ReadOptions` is supplied for each
/// read call and controls the behavior of the read.
pub struct ReadOptions {
    /// The read commit level for read operations.
    pub read_level: ReadLevel,
}

impl ReadOptions {
    /// Create a new ReadOptions with `read_level` set to `Commited`.
    const fn default() -> Self {
        Self {
            read_level: ReadLevel::Commited,
        }
    }
}

/// Configuration for client write operations. `WriteOptions` is supplied for each
/// write call and controls the behavior of the write.
#[derive(Clone)]
pub struct WriteOptions {
    /// Whether `put` calls should block until the write has been durably committed
    /// to the DB.
    pub await_durable: bool,
}

impl WriteOptions {
    /// Create a new `WriteOptions`` with `await_durable` set to `true`.
    const fn default() -> Self {
        Self {
            await_durable: true,
        }
    }
}

/// defines the clock that SlateDB will use during this session
pub trait Clock {
    /// Returns a timestamp (typically measured in millis since the unix epoch),
    /// must return monotonically increasing numbers (this is enforced
    /// at runtime and will panic if the invariant is broken)
    ///
    /// Note that this clock does not need to return a number that
    /// represents the unix timestamp; the only requirement is that
    /// it represents a sequence that can attribute a logical ordering
    /// to actions on the database
    fn now(&self) -> i64;
}

/// contains the default implementation of the Clock, and will return the system time
pub struct SystemClock {
    last_tick: AtomicI64,
}

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        // since SystemTime is not guaranteed to be monotonic, we enforce it here
        let tick = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs() as i64, // Time is after the epoch
            Err(e) => -(e.duration().as_secs() as i64), // Time is before the epoch, return negative
        };
        self.last_tick.fetch_max(tick, SeqCst);
        self.last_tick.load(SeqCst)
    }
}

fn default_clock() -> Arc<dyn Clock + Send + Sync> {
    Arc::new(SystemClock {
        last_tick: AtomicI64::new(i64::MIN),
    })
}

/// Configuration options for the database. These options are set on client startup.
#[derive(Clone, Deserialize, Serialize)]
pub struct DbOptions {
    /// How frequently to flush the write-ahead log to object storage.
    ///
    /// When setting this configuration, users must consider:
    ///
    /// * **Latency**: The higher the flush interval, the longer it will take for
    ///   writes to be committed to object storage. Writers blocking on `put` calls
    ///   will wait longer for the write. Readers reading committed writes will also
    ///   see data later.
    /// * **API cost**: The lower the flush interval, the more frequently PUT calls
    ///   will be made to object storage. This can increase your object storage costs.
    ///
    /// We recommend setting this value based on your cost and latency tolerance. A
    /// 100ms flush interval should result in $130/month in PUT costs on S3 standard.
    ///
    /// Keep in mind that the flush interval does not include the network latency. A
    /// 100ms flush interval will result in a 100ms + the time it takes to send the
    /// bytes to object storage.
    #[serde(deserialize_with = "deserialize_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub flush_interval: Duration,

    /// If set to false, SlateDB will disable the WAL and write directly into the memtable
    #[cfg(feature = "wal_disable")]
    pub wal_enabled: bool,

    /// How frequently to poll for new manifest files. Refreshing the manifest file
    /// allows writers to detect fencing operations and allows readers to detect newly
    /// compacted data.
    ///
    /// **NOTE: SlateDB secondary readers (i.e. non-writer clients) do not currently
    /// read from the WAL. Such readers only read from L0+. The manifest poll intervals
    /// allows such readers to detect new L0+ files.**
    #[serde(deserialize_with = "deserialize_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub manifest_poll_interval: Duration,

    /// Write SSTables with a bloom filter if the number of keys in the SSTable
    /// is greater than or equal to this value. Reads on small SSTables might be
    /// faster without a bloom filter.
    pub min_filter_keys: u32,

    /// The number of bits to use per key for bloom filters. We recommend setting this
    /// to the default value of 10, which yields a filter with an expected fpp of ~.0082
    /// Note that this is evaluated per-sorted-run, so the expected number of false positives
    /// per request is the fpp * number of sorted runs. So for large dbs with lots of runs,
    /// you may benefit from setting this higher (if you have enough memory available)
    pub filter_bits_per_key: u32,

    /// The minimum size a memtable needs to be before it is frozen and flushed to
    /// L0 object storage. Writes will still be flushed to the object storage WAL
    /// (based on flush_interval) regardless of this value. Memtable sizes are checked
    /// every `flush_interval`.
    ///
    /// When setting this configuration, users must consider:
    ///
    /// * **Recovery time**: The larger the L0 SSTable size threshold, the less
    ///   frequently it will be written. As a result, the more recovery data there
    ///   will be in the WAL if a process restarts.
    /// * **Number of L0 SSTs/SRs**: The smaller the L0 SSTable size threshold, the
    ///   more SSTs and Sorted Runs there will be. L0 SSTables are not range
    ///   partitioned; each is its own sorted table. Similarly, each Sorted Run also
    ///   stores the entire keyspace. As such, reads that don't hit the WAL or memtable
    ///   may need to scan all L0 SSTables and Sorted Runs. The more there are, the
    ///   slower the scan will be.
    /// * **Memory usage**: The larger the L0 SSTable size threshold, the larger the
    ///   unflushed in-memory memtable will grow. This shouldn't be a concern for most
    ///   workloads, but it's worth considering for workloads with very high L0
    ///   SSTable sizes.
    /// * **API cost**: Smaller L0 SSTable sizes will result in more frequent writes
    ///   to object storage. This can increase your object storage costs.
    /// * **Secondary reader latency**: Secondary (non-writer) clients only see L0+
    ///   writes; they don't see WAL writes. Thus, the higher the L0 SSTable size, the
    ///   less frequently they will be written, and the longer it will take for
    ///   secondary readers to see new data.
    pub l0_sst_size_bytes: usize,

    /// Defines the max number of SSTs in l0. Memtables will not be flushed if there are more
    /// l0 ssts than this value, until compaction can compact the ssts into compacted.
    pub l0_max_ssts: usize,

    /// Defines the max number of unflushed memtables. Writes will be paused if there
    /// are more unflushed memtables than this value
    pub max_unflushed_memtable: usize,

    /// Configuration options for the compactor.
    pub compactor_options: Option<CompactorOptions>,

    /// The compression algorithm to use for SSTables.
    pub compression_codec: Option<CompressionCodec>,

    /// The object store cache options.
    pub object_store_cache_options: ObjectStoreCacheOptions,

    /// The block cache instance used to cache SSTable blocks, indexes and bloom filters.
    #[serde(skip)]
    pub block_cache: Option<Arc<dyn DbCache>>,

    /// Configuration options for the garbage collector.
    pub garbage_collector_options: Option<GarbageCollectorOptions>,

    /// The Clock to use for insertion timestamps
    ///
    /// Default: the default clock uses the local system time on the machine
    #[serde(skip)]
    #[serde(default = "default_clock")]
    pub clock: Arc<dyn Clock + Send + Sync>,
}

impl DbOptions {
    /// Loads DbOptions from a file.
    ///
    /// This function attempts to read and parse a configuration file to create a DbOptions instance.
    /// The file format is determined by its extension:
    /// - ".json" for JSON format
    /// - ".toml" for TOML format
    /// - ".yaml" or ".yml" for YAML format
    ///
    /// # Arguments
    ///
    /// * `path` - A path-like object pointing to the configuration file.
    ///
    /// # Returns
    ///
    /// * `Ok(DbOptions)` if the file was successfully read and parsed.
    /// * `Err(DbOptionsError)` if there was an error reading or parsing the file.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The file extension is not recognized (not json, toml, yaml, or yml).
    /// - The file cannot be read or parsed according to its presumed format.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::config::DbOptions;
    /// use std::path::Path;
    ///
    /// let config = DbOptions::from_file("config.toml").expect("Failed to load options from file");
    /// ```
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<DbOptions, DbOptionsError> {
        let path = path.as_ref();
        let Some(ext) = path.extension() else {
            return Err(DbOptionsError::UnknownFormat(path.into()));
        };

        let mut builder = Figment::from(DbOptions::default());
        match ext.to_str().unwrap_or_default() {
            "json" => builder = builder.merge(Json::file(path)),
            "toml" => builder = builder.merge(Toml::file(path)),
            "yaml" | "yml" => builder = builder.merge(Yaml::file(path)),
            _ => return Err(DbOptionsError::UnknownFormat(path.into())),
        }
        builder.extract().map_err(Into::into)
    }

    /// Loads DbOptions from environment variables with a specified prefix.
    ///
    /// This function attempts to create a DbOptions instance by reading environment variables
    /// that start with the given prefix. Nested options are separated by a dot (.) in the environment variable names.
    ///
    /// For example, if the prefix is "SLATEDB_" and there's an environment variable named "SLATEDB_DB_FLUSH_INTERVAL",
    /// it would correspond to the `flush_interval` field within the `DbOptions` struct.
    /// If there is an environment variable named "SLATEDB_OBJECT_STORE_CACHE_OPTIONS.ROOT_FOLDER",
    /// it would correspond to the `root_folder` field within the `ObjectStoreCacheOptions` within `DbOptions`".
    ///
    /// # Arguments
    ///
    /// * `prefix` - A string that specifies the prefix for the environment variables to be considered.
    ///
    /// # Returns
    ///
    /// * `Ok(DbOptions)` if the environment variables were successfully read and parsed.
    /// * `Err(DbOptionsError)` if there was an error reading or parsing the environment variables.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::config::DbOptions;
    ///
    /// // Assuming environment variables like SLATEDB_FLUSH_INTERVAL, SLATEDB_WAL_ENABLED, etc. are set
    /// let config = DbOptions::from_env("SLATEDB_").expect("Failed to load options from env");
    /// ```
    pub fn from_env(prefix: &str) -> Result<DbOptions, DbOptionsError> {
        Figment::from(DbOptions::default())
            .merge(Env::prefixed(prefix))
            .extract()
            .map_err(Into::into)
    }

    /// Loads DbOptions from multiple configuration sources in a specific order.
    ///
    /// This function attempts to create a DbOptions instance by merging configurations
    /// from various sources in the following order:
    /// 1. Default options
    /// 2. JSON file ("SlateDb.json")
    /// 3. TOML file ("SlateDb.toml")
    /// 4. YAML files ("SlateDb.yaml" and "SlateDb.yml")
    /// 5. Environment variables prefixed with "SLATEDB_"
    ///
    /// Each subsequent source overrides the values from the previous sources if they exist.
    ///
    /// # Returns
    ///
    /// * `Ok(DbOptions)` if the configuration was successfully loaded and parsed.
    /// * `Err(DbOptionsError)` if there was an error reading or parsing the configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::config::DbOptions;
    ///
    /// let config = DbOptions::load().expect("Failed to load options");
    /// ```
    pub fn load() -> Result<DbOptions, DbOptionsError> {
        Figment::from(DbOptions::default())
            .merge(Json::file("SlateDb.json"))
            .merge(Toml::file("SlateDb.toml"))
            .merge(Yaml::file("SlateDb.yaml"))
            .merge(Yaml::file("SlateDb.yml"))
            .admerge(Env::prefixed("SLATEDB_"))
            .extract()
            .map_err(Into::into)
    }
}

impl Provider for DbOptions {
    fn metadata(&self) -> figment::Metadata {
        Metadata::named("SlateDb configuration options")
    }

    fn data(
        &self,
    ) -> Result<figment::value::Map<figment::Profile, figment::value::Dict>, figment::Error> {
        figment::providers::Serialized::defaults(DbOptions::default()).data()
    }
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(100),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_secs(1),
            min_filter_keys: 1000,
            l0_sst_size_bytes: 64 * 1024 * 1024,
            max_unflushed_memtable: 2,
            l0_max_ssts: 8,
            compactor_options: Some(CompactorOptions::default()),
            compression_codec: None,
            object_store_cache_options: ObjectStoreCacheOptions::default(),
            block_cache: default_block_cache(),
            garbage_collector_options: Some(GarbageCollectorOptions::default()),
            filter_bits_per_key: 10,
            clock: default_clock(),
        }
    }
}

#[allow(unreachable_code)]
fn default_block_cache() -> Option<Arc<dyn DbCache>> {
    #[cfg(feature = "moka")]
    {
        return Some(Arc::new(crate::db_cache::moka::MokaCache::new()));
    }
    #[cfg(feature = "foyer")]
    {
        return Some(Arc::new(crate::db_cache::foyer::FoyerCache::new()));
    }
    None
}

/// The compression algorithm to use for SSTables.
#[derive(Clone, Copy, Deserialize, PartialEq, Debug, Serialize)]
pub enum CompressionCodec {
    #[cfg(feature = "snappy")]
    /// Snappy compression algorithm.
    Snappy,
    #[cfg(feature = "zlib")]
    /// Zlib compression algorithm.
    Zlib,
    #[cfg(feature = "lz4")]
    /// Lz4 compression algorithm.
    Lz4,
    #[cfg(feature = "zstd")]
    /// Zstd compression algorithm.
    Zstd,
}

impl FromStr for CompressionCodec {
    type Err = SlateDBError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            #[cfg(feature = "snappy")]
            "snappy" => Ok(Self::Snappy),
            #[cfg(feature = "zlib")]
            "zlib" => Ok(Self::Zlib),
            #[cfg(feature = "lz4")]
            "lz4" => Ok(Self::Lz4),
            #[cfg(feature = "zstd")]
            "zstd" => Ok(Self::Zstd),
            _ => Err(SlateDBError::InvalidCompressionCodec),
        }
    }
}

pub trait CompactionSchedulerSupplier: Send + Sync {
    fn compaction_scheduler(&self) -> Box<dyn CompactionScheduler>;
}

/// Options for the compactor.
#[derive(Clone, Deserialize, Serialize)]
pub struct CompactorOptions {
    /// The interval at which the compactor checks for a new manifest and decides
    /// if a compaction must be scheduled
    #[serde(deserialize_with = "deserialize_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub poll_interval: Duration,

    /// A compacted SSTable's maximum size (in bytes). If more data needs to be
    /// written to a Sorted Run during a compaction, a new SSTable will be created
    /// in the Sorted Run when this size is exceeded.
    pub max_sst_size: usize,

    /// Supplies the compaction scheduler to use to select the compactions that should be
    /// scheduled. Currently, the only provided implementation is
    /// SizeTieredCompactionSchedulerSupplier
    #[serde(skip, default = "default_compaction_scheduler")]
    pub compaction_scheduler: Arc<dyn CompactionSchedulerSupplier>,

    /// The maximum number of concurrent compactions to execute at once
    pub max_concurrent_compactions: usize,

    /// An optional tokio runtime handle to use for scheduling compaction work. You can use
    /// this to isolate compactions to a dedicated thread pool.
    #[serde(skip)]
    pub compaction_runtime: Option<Handle>,
}

/// Default options for the compactor. Currently, only a
/// `SizeTieredCompactionScheduler` compaction strategy is implemented.
impl Default for CompactorOptions {
    /// Returns a `CompactorOptions` with a 5 second poll interval and a 1GB max
    /// SSTable size.
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            max_sst_size: 1024 * 1024 * 1024,
            compaction_scheduler: default_compaction_scheduler(),
            max_concurrent_compactions: 4,
            compaction_runtime: None,
        }
    }
}

/// Returns the default compaction scheduler supplier.
///
/// This function creates and returns an `Arc<dyn CompactionSchedulerSupplier>` containing
/// a `SizeTieredCompactionSchedulerSupplier` with default options. It is used as the default
/// value for the `compaction_scheduler` field in `CompactorOptions`.
///
/// # Returns
///
/// An `Arc<dyn CompactionSchedulerSupplier>` wrapping a `SizeTieredCompactionSchedulerSupplier`
/// with the default configuration.
fn default_compaction_scheduler() -> Arc<dyn CompactionSchedulerSupplier> {
    Arc::new(SizeTieredCompactionSchedulerSupplier::default())
}

/// Options for the Size-Tiered Compaction Scheduler
#[derive(Clone)]
pub struct SizeTieredCompactionSchedulerOptions {
    /// The minimum number of sources to include together in a single compaction step.
    pub min_compaction_sources: usize,

    /// The maximum number of sources to include together in a single compaction step.
    pub max_compaction_sources: usize,

    /// The size threshold that the scheduler will use to determine if a sorted run should
    /// be included in a given compaction. A sorted run S will be added to a compaction C if S's
    /// size is less than this value times the min size of the runs currently included in C.
    pub include_size_threshold: f32,
}

impl Default for SizeTieredCompactionSchedulerOptions {
    fn default() -> Self {
        Self {
            min_compaction_sources: 4,
            max_compaction_sources: 8,
            include_size_threshold: 4.0,
        }
    }
}

/// Garbage collector options.
#[derive(Clone, Deserialize, Serialize)]
pub struct GarbageCollectorOptions {
    /// Garbage collection options for the manifest directory.
    pub manifest_options: Option<GarbageCollectorDirectoryOptions>,

    /// Garbage collection options for the WAL directory.
    pub wal_options: Option<GarbageCollectorDirectoryOptions>,

    /// Garbage collection options for the compacted directory.
    pub compacted_options: Option<GarbageCollectorDirectoryOptions>,

    /// An optional tokio runtime handle to use for scheduling garbage collection. You can use
    /// this to isolate garbage collection to a dedicated thread pool.
    #[serde(skip)]
    pub gc_runtime: Option<Handle>,
}

impl Default for GarbageCollectorDirectoryOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(300),
            min_age: Duration::from_secs(86_400),
        }
    }
}

/// Garbage collector options for a directory.
#[derive(Clone, Copy, Deserialize, Serialize)]
pub struct GarbageCollectorDirectoryOptions {
    /// The interval at which the garbage collector checks for files to garbage collect.
    #[serde(deserialize_with = "deserialize_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub poll_interval: Duration,

    /// The minimum age of a file before it can be garbage collected.
    #[serde(deserialize_with = "deserialize_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub min_age: Duration,
}

/// Default options for the garbage collector. The default options are:
/// * Manifest options: interval of 60 seconds, min age of 1 day
/// * WAL options: interval of 60 seconds, min age of 1 minute
/// * Compacted options: interval of 60 seconds, min age of 1 day
impl Default for GarbageCollectorOptions {
    fn default() -> Self {
        Self {
            manifest_options: Some(Default::default()),
            wal_options: Some(GarbageCollectorDirectoryOptions {
                poll_interval: Duration::from_secs(60),
                min_age: Duration::from_secs(60),
            }),
            compacted_options: Some(Default::default()),
            gc_runtime: None,
        }
    }
}

/// Options for the object store cache. This cache is not enabled unless an explicit cache
/// root folder is set. The object store cache will split an object into align-sized parts
/// in the local, and save them into the local cache storage.
///
/// The local cache default uses file system as storage, it can also be extended to use other
/// like RocksDB, Redis, etc. in the future.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectStoreCacheOptions {
    /// The root folder where the cache files are stored. If not set, the cache will be
    /// disabled.
    pub root_folder: Option<std::path::PathBuf>,

    /// The limit of the cache size in bytes, the default value is 16gb.
    pub max_cache_size_bytes: Option<usize>,

    /// The size of each part file, the part size is expected to be aligned with 1kb,
    /// its default value is 4mb.
    pub part_size_bytes: usize,

    /// Interval to scan the cache directory to rebuild the in-memory map for evictor.
    /// The default value is 1 hour. If set to None, the cache directory will be only
    /// scanned once on start up.
    #[serde(deserialize_with = "deserialize_option_duration")]
    #[serde(
        serialize_with = "serialize_option_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub scan_interval: Option<Duration>,
}

impl Default for ObjectStoreCacheOptions {
    fn default() -> Self {
        Self {
            root_folder: None,
            max_cache_size_bytes: Some(16 * 1024 * 1024 * 1024),
            part_size_bytes: 4 * 1024 * 1024,
            scan_interval: Some(Duration::from_secs(3600)),
        }
    }
}

// Custom serializer for Duration
fn serialize_duration<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    let duration_str = if secs > 0 && millis > 0 {
        format!("{secs}s+{millis:03}ms")
    } else if millis > 0 {
        format!("{millis:03}ms")
    } else {
        format!("{secs}s")
    };
    serializer.serialize_str(&duration_str)
}

// Custom serializer for Option<Duration>
fn serialize_option_duration<S>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match duration {
        Some(d) => serialize_duration(d, serializer),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_db_options_load_from_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("SLATEDB_FLUSH_INTERVAL", "1s");
            jail.set_env(
                "SLATEDB_OBJECT_STORE_CACHE_OPTIONS.ROOT_FOLDER",
                "/tmp/slatedb-root",
            );

            let options = DbOptions::from_env("SLATEDB_")
                .expect("failed to load db options from environment");
            assert_eq!(Duration::from_secs(1), options.flush_interval);
            assert_eq!(
                Some(PathBuf::from("/tmp/slatedb-root")),
                options.object_store_cache_options.root_folder
            );

            Ok(())
        });
    }

    #[test]
    fn test_db_options_load_from_json_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.json",
                r#"
{
    "flush_interval": "1s",
    "object_store_cache_options": {
        "root_folder": "/tmp/slatedb-root"
    } 
}
"#,
            )
            .expect("failed to create db options config file");

            let options = DbOptions::from_file("config.json")
                .expect("failed to load db options from environment");
            assert_eq!(Duration::from_secs(1), options.flush_interval);
            assert_eq!(
                Some(PathBuf::from("/tmp/slatedb-root")),
                options.object_store_cache_options.root_folder
            );
            Ok(())
        });
    }

    #[test]
    fn test_db_options_load_from_toml_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
flush_interval = "1s"
[object_store_cache_options]
root_folder = "/tmp/slatedb-root"
"#,
            )
            .expect("failed to create db options config file");

            let options = DbOptions::from_file("config.toml")
                .expect("failed to load db options from environment");
            assert_eq!(Duration::from_secs(1), options.flush_interval);
            assert_eq!(
                Some(PathBuf::from("/tmp/slatedb-root")),
                options.object_store_cache_options.root_folder
            );
            Ok(())
        });
    }

    #[test]
    fn test_db_options_load_from_yaml_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.yaml",
                r#"
flush_interval: "1s"
object_store_cache_options:
    root_folder: "/tmp/slatedb-root"
"#,
            )
            .expect("failed to create db options config file");

            let options = DbOptions::from_file("config.yaml")
                .expect("failed to load db options from environment");
            assert_eq!(Duration::from_secs(1), options.flush_interval);
            assert_eq!(
                Some(PathBuf::from("/tmp/slatedb-root")),
                options.object_store_cache_options.root_folder
            );
            Ok(())
        });
    }

    #[test]
    fn test_db_options_load_with_default_locations() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("SLATEDB_FLUSH_INTERVAL", "1s");

            jail.create_file(
                "SlateDb.yaml",
                r#"
object_store_cache_options:
    root_folder: "/tmp/slatedb-root"
"#,
            )
            .expect("failed to create db options config file");

            let options = DbOptions::load().expect("failed to load db options from environment");
            assert_eq!(Duration::from_secs(1), options.flush_interval);
            assert_eq!(
                Some(PathBuf::from("/tmp/slatedb-root")),
                options.object_store_cache_options.root_folder
            );
            Ok(())
        });
    }
}
