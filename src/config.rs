use std::sync::Arc;
use std::{str::FromStr, time::Duration};

use serde::Serialize;
use tokio::runtime::Handle;

use crate::compactor::CompactionScheduler;
use crate::error::SlateDBError;

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

/// Configuration options for the database. These options are set on client startup.
#[derive(Clone)]
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
    pub block_cache: Option<Arc<dyn DbCache>>,

    /// Configuration options for the garbage collector.
    pub garbage_collector_options: Option<GarbageCollectorOptions>,
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
#[derive(Clone, Copy, PartialEq, Debug, Serialize)]
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
#[derive(Clone)]
pub struct CompactorOptions {
    /// The interval at which the compactor checks for a new manifest and decides
    /// if a compaction must be scheduled
    pub poll_interval: Duration,

    /// A compacted SSTable's maximum size (in bytes). If more data needs to be
    /// written to a Sorted Run during a compaction, a new SSTable will be created
    /// in the Sorted Run when this size is exceeded.
    pub max_sst_size: usize,

    /// Supplies the compaction scheduler to use to select the compactions that should be
    /// scheduled. Currently, the only provided implementation is
    /// SizeTieredCompactionSchedulerSupplier
    pub compaction_scheduler: Arc<dyn CompactionSchedulerSupplier>,

    /// The maximum number of concurrent compactions to execute at once
    pub max_concurrent_compactions: usize,

    /// An optional tokio runtime handle to use for scheduling compaction work. You can use
    /// this to isolate compactions to a dedicated thread pool.
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
            compaction_scheduler: Arc::new(SizeTieredCompactionSchedulerSupplier::new(
                SizeTieredCompactionSchedulerOptions::default(),
            )),
            max_concurrent_compactions: 4,
            compaction_runtime: None,
        }
    }
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

impl SizeTieredCompactionSchedulerOptions {
    pub const fn default() -> Self {
        Self {
            min_compaction_sources: 4,
            max_compaction_sources: 8,
            include_size_threshold: 4.0,
        }
    }
}

/// Garbage collector options.
#[derive(Clone)]
pub struct GarbageCollectorOptions {
    /// Garbage collection options for the manifest directory.
    pub manifest_options: Option<GarbageCollectorDirectoryOptions>,

    /// Garbage collection options for the WAL directory.
    pub wal_options: Option<GarbageCollectorDirectoryOptions>,

    /// Garbage collection options for the compacted directory.
    pub compacted_options: Option<GarbageCollectorDirectoryOptions>,

    /// An optional tokio runtime handle to use for scheduling garbage collection. You can use
    /// this to isolate garbage collection to a dedicated thread pool.
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
#[derive(Clone, Copy)]
pub struct GarbageCollectorDirectoryOptions {
    /// The interval at which the garbage collector checks for files to garbage collect.
    pub poll_interval: Duration,

    /// The minimum age of a file before it can be garbage collected.
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
#[derive(Clone, Debug)]
pub struct ObjectStoreCacheOptions {
    /// The root folder where the cache files are stored. If not set, the cache will be
    /// disabled.
    pub root_folder: Option<std::path::PathBuf>,

    /// The size of each part file, the part size is expected to be aligned with 1kb,
    /// its default value is 4mb.
    pub part_size_bytes: usize,
}

impl Default for ObjectStoreCacheOptions {
    fn default() -> Self {
        Self {
            root_folder: None,
            part_size_bytes: 4 * 1024 * 1024,
        }
    }
}
