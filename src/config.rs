use std::time::Duration;

pub const DEFAULT_READ_OPTIONS: &ReadOptions = &ReadOptions::default();
pub const DEFAULT_WRITE_OPTIONS: &WriteOptions = &WriteOptions::default();

#[allow(dead_code)]
pub const DEFAULT_COMPACTOR_OPTIONS: &CompactorOptions = &CompactorOptions::default();

/// Whether reads see data that's been written to object storage.
pub enum ReadLevel {
    /// Client reads will only see data that's been written to object storage.
    Commited,

    /// Clients will see all writes, including those not yet written to object
    /// storage.
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
pub struct WriteOptions {
    /// Whether `put` calls should block until the write has been written to
    /// object storage.
    pub await_flush: bool,
}

impl WriteOptions {
    /// Create a new `WriteOptions`` with `await_flush` set to `true`.
    const fn default() -> Self {
        Self { await_flush: true }
    }
}

/// Configuration options for the database. These options are set on client startup.
#[derive(Clone)]
pub struct DbOptions {
    /// How frequently to flush the write-ahead log to object storage (in
    /// milliseconds).
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
    pub flush_ms: usize,

    /// How frequently to poll for new manifest files (in milliseconds). Refreshing
    /// the manifest file allows writers to detect fencing operations and allows
    /// readers to detect newly compacted data.
    ///
    /// **NOTE: SlateDB secondary readers (i.e. non-writer clients) do not currently
    /// read from the WAL. Such readers only read from L0+. The manifest poll intervals
    /// allows such readers to detect new L0+ files.**
    pub manifest_poll_interval: Duration,

    /// Write SSTables with a bloom filter if the number of keys in the SSTable
    /// is greater than or equal to this value. Reads on small SSTables might be
    /// faster without a bloom filter.
    pub min_filter_keys: u32,

    /// The minimum size a memtable needs to be before it is frozen and flushed to
    /// L0 object storage. Writes will still be flushed to the object storage WAL
    /// (based on flush_ms) regardless of this value. Memtable sizes are checked
    /// every `flush_ms` milliseconds.
    ///
    /// When setting this configuration, users must consider:
    ///
    /// * **Recovery time**: The larger the L0 SSTable size threshold, the less
    ///   frequently it will be written. As a result, the more recovery data there
    ///   will be in the WAL if a process restarts.
    /// * **Number of L0 SSTs**: The smaller the L0 SSTable size threshold, the more
    ///   L0 SSTables there will be. L0 SSTables are not range partitioned; each is its
    ///   own sorted table. As such, reads that don't hit the WAL or memtable will need
    ///   to scan all L0 SSTables. The more there are, the slower the scan will be.
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
    ///
    /// We recommend setting this value to a size that will result in one L0 SSTable
    /// per-second. With a default compaction interval of 5 seconds, this will result
    /// in 4 or 5 L0 SSTables per compaction. Thus, a writer putting 10MiB/s of data
    /// would configure this value to 10 * 1024 * 1024 = 10_485_760 bytes.
    pub l0_sst_size_bytes: usize,

    /// Configuration options for the compactor.
    pub compactor_options: Option<CompactorOptions>,
}

/// Options for the compactor.
#[derive(Clone)]
pub struct CompactorOptions {
    /// The interval at which the compactor checks for a new manifest and decides
    /// if a compaction must be scheduled
    pub(crate) poll_interval: Duration,

    /// A compacted SSTable's maximum size (in bytes). If more data needs to be
    /// written during a compaction, a new SSTable will be created when this size
    /// is exceeded.
    pub(crate) max_sst_size: usize,
}

/// Default options for the compactor. Currently, only a
/// `SizeTieredCompactionScheduler` compaction strategy is implemented.
impl CompactorOptions {
    /// Returns a `CompactorOptions` with a 5 second poll interval and a 1GB max
    /// SSTable size.
    pub const fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            max_sst_size: 1024 * 1024 * 1024,
        }
    }
}
