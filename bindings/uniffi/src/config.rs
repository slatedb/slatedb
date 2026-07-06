use crate::error::{Error, SlateDbError};
use crate::filter_policy::FilterContext;
use crate::types::try_checkpoint_id_from_str;
use std::time::Duration;

/// Minimum durability level required for data returned by reads and scans.
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum DurabilityLevel {
    /// Return only data that has been flushed to remote object storage.
    Remote,
    /// Return both remote data and newer in-memory data.
    #[default]
    Memory,
}

impl From<DurabilityLevel> for slatedb::config::DurabilityLevel {
    fn from(value: DurabilityLevel) -> Self {
        match value {
            DurabilityLevel::Remote => Self::Remote,
            DurabilityLevel::Memory => Self::Memory,
        }
    }
}

/// Storage layer targeted by an explicit flush.
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FlushType {
    /// Flush the active memtable and any immutable memtables to object storage.
    MemTable,
    /// Flush the active WAL and any immutable WAL segments to object storage.
    #[default]
    Wal,
}

impl From<FlushType> for slatedb::config::FlushType {
    fn from(value: FlushType) -> Self {
        match value {
            FlushType::MemTable => Self::MemTable,
            FlushType::Wal => Self::Wal,
        }
    }
}

/// Isolation level used when starting a transaction.
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum IsolationLevel {
    /// Reads see a stable snapshot without full serializable conflict checking.
    #[default]
    Snapshot,
    /// Reads see a stable snapshot with serializable conflict detection.
    SerializableSnapshot,
}

impl From<IsolationLevel> for slatedb::IsolationLevel {
    fn from(value: IsolationLevel) -> Self {
        match value {
            IsolationLevel::Snapshot => Self::Snapshot,
            IsolationLevel::SerializableSnapshot => Self::SerializableSnapshot,
        }
    }
}

/// Block size used for newly written SSTable blocks.
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum SstBlockSize {
    /// 1 KiB blocks.
    Block1Kib,
    /// 2 KiB blocks.
    Block2Kib,
    /// 4 KiB blocks.
    #[default]
    Block4Kib,
    /// 8 KiB blocks.
    Block8Kib,
    /// 16 KiB blocks.
    Block16Kib,
    /// 32 KiB blocks.
    Block32Kib,
    /// 64 KiB blocks.
    Block64Kib,
}

impl From<SstBlockSize> for slatedb::SstBlockSize {
    fn from(value: SstBlockSize) -> Self {
        match value {
            SstBlockSize::Block1Kib => Self::Block1Kib,
            SstBlockSize::Block2Kib => Self::Block2Kib,
            SstBlockSize::Block4Kib => Self::Block4Kib,
            SstBlockSize::Block8Kib => Self::Block8Kib,
            SstBlockSize::Block16Kib => Self::Block16Kib,
            SstBlockSize::Block32Kib => Self::Block32Kib,
            SstBlockSize::Block64Kib => Self::Block64Kib,
        }
    }
}

/// Time-to-live policy applied to an inserted value or merge operand.
#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum Ttl {
    /// Use the database default TTL.
    #[default]
    Default,
    /// Store the value without expiration.
    NoExpiry,
    /// Expire the value after the given number of clock ticks.
    ExpireAfterTicks(u64),
    /// Expire the value at the given absolute timestamp (clock ticks).
    ExpireAt(i64),
}

impl From<Ttl> for slatedb::config::Ttl {
    fn from(value: Ttl) -> Self {
        match value {
            Ttl::Default => Self::Default,
            Ttl::NoExpiry => Self::NoExpiry,
            Ttl::ExpireAfterTicks(ttl) => Self::ExpireAfter(ttl),
            Ttl::ExpireAt(ts) => Self::ExpireAt(ts),
        }
    }
}

/// Options that control a point read.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ReadOptions {
    /// Minimum durability level a returned row must satisfy.
    pub durability_filter: DurabilityLevel,
    /// Whether uncommitted dirty data may be returned.
    pub dirty: bool,
    /// Whether fetched data blocks should be inserted into the block cache.
    /// SST metadata is cached independently.
    pub cache_blocks: bool,
    /// Optional context forwarded to custom filter policies; ignored by
    /// built-in filters.
    #[uniffi(default = None)]
    pub filter_context: Option<FilterContext>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            cache_blocks: true,
            filter_context: None,
        }
    }
}

impl From<ReadOptions> for slatedb::config::ReadOptions {
    fn from(value: ReadOptions) -> Self {
        slatedb::config::ReadOptions {
            durability_filter: value.durability_filter.into(),
            dirty: value.dirty,
            cache_blocks: value.cache_blocks,
            filter_context: value.filter_context.map(Into::into),
        }
    }
}

/// Options for opening a [`crate::DbReader`].
#[derive(Clone, Debug, uniffi::Record)]
pub struct ReaderOptions {
    /// How often the reader polls for new manifests and WAL data, in milliseconds.
    pub manifest_poll_interval_ms: u64,
    /// Lifetime of an internally managed checkpoint, in milliseconds.
    pub checkpoint_lifetime_ms: u64,
    /// Maximum size of one in-memory table used while replaying WAL data.
    pub max_memtable_bytes: u64,
    /// Whether WAL replay should be skipped entirely.
    pub skip_wal_replay: bool,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            manifest_poll_interval_ms: 10_000,
            checkpoint_lifetime_ms: 600_000,
            max_memtable_bytes: 64 * 1024 * 1024,
            skip_wal_replay: false,
        }
    }
}

impl From<ReaderOptions> for slatedb::config::DbReaderOptions {
    fn from(value: ReaderOptions) -> Self {
        slatedb::config::DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(value.manifest_poll_interval_ms),
            checkpoint_lifetime: Duration::from_millis(value.checkpoint_lifetime_ms),
            max_memtable_bytes: value.max_memtable_bytes,
            skip_wal_replay: value.skip_wal_replay,
            ..Default::default()
        }
    }
}

/// The iteration order for a scan.
#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum IterationOrder {
    #[default]
    Ascending,
    Descending,
}

impl From<IterationOrder> for slatedb::IterationOrder {
    fn from(value: IterationOrder) -> Self {
        match value {
            IterationOrder::Ascending => slatedb::IterationOrder::Ascending,
            IterationOrder::Descending => slatedb::IterationOrder::Descending,
        }
    }
}

/// Options that control range scans and prefix scans.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScanOptions {
    /// Minimum durability level a returned row must satisfy.
    pub durability_filter: DurabilityLevel,
    /// Whether uncommitted dirty data may be returned.
    pub dirty: bool,
    /// Number of bytes to read ahead while scanning.
    pub read_ahead_bytes: u64,
    /// Whether fetched data blocks should be inserted into the block cache.
    /// SST metadata is cached independently.
    pub cache_blocks: bool,
    /// Maximum number of concurrent fetch tasks used by the scan.
    pub max_fetch_tasks: u64,
    /// The iteration order for the scan. Defaults to ascending when not set.
    #[uniffi(default = None)]
    pub order: Option<IterationOrder>,
    /// Optional context forwarded to custom filter policies; ignored by
    /// built-in filters. Only consulted for prefix scans.
    #[uniffi(default = None)]
    pub filter_context: Option<FilterContext>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
            order: None,
            filter_context: None,
        }
    }
}

impl TryFrom<ScanOptions> for slatedb::config::ScanOptions {
    type Error = Error;

    fn try_from(value: ScanOptions) -> Result<Self, Self::Error> {
        Ok(slatedb::config::ScanOptions {
            durability_filter: value.durability_filter.into(),
            dirty: value.dirty,
            read_ahead_bytes: usize::try_from(value.read_ahead_bytes).map_err(|_| {
                Error::from(SlateDbError::ValueTooLargeForUsize {
                    field: "read_ahead_bytes",
                })
            })?,
            cache_blocks: value.cache_blocks,
            max_fetch_tasks: usize::try_from(value.max_fetch_tasks).map_err(|_| {
                Error::from(SlateDbError::ValueTooLargeForUsize {
                    field: "max_fetch_tasks",
                })
            })?,
            order: value.order.unwrap_or_default().into(),
            filter_context: value.filter_context.map(Into::into),
        })
    }
}

/// Options that control durability behavior for writes and commits.
#[derive(Clone, Debug, uniffi::Record)]
pub struct WriteOptions {
    /// Whether the call waits for the write to become durable before returning.
    pub await_durable: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            await_durable: true,
        }
    }
}

impl From<WriteOptions> for slatedb::config::WriteOptions {
    fn from(value: WriteOptions) -> Self {
        slatedb::config::WriteOptions {
            await_durable: value.await_durable,
            ..Default::default()
        }
    }
}

/// Options applied to a put operation.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct PutOptions {
    /// TTL policy for the inserted value.
    pub ttl: Ttl,
}

impl From<PutOptions> for slatedb::config::PutOptions {
    fn from(value: PutOptions) -> Self {
        slatedb::config::PutOptions {
            ttl: value.ttl.into(),
        }
    }
}

/// Options applied to a merge operation.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct MergeOptions {
    /// TTL policy for the inserted merge operand.
    pub ttl: Ttl,
}

impl From<MergeOptions> for slatedb::config::MergeOptions {
    fn from(value: MergeOptions) -> Self {
        slatedb::config::MergeOptions {
            ttl: value.ttl.into(),
        }
    }
}

/// Options for an explicit flush request.
#[derive(Clone, Debug, uniffi::Record, Default)]
pub struct FlushOptions {
    /// Which storage layer should be flushed.
    pub flush_type: FlushType,
}

impl From<FlushOptions> for slatedb::config::FlushOptions {
    fn from(value: FlushOptions) -> Self {
        slatedb::config::FlushOptions {
            flush_type: value.flush_type.into(),
        }
    }
}

/// Garbage collector options for one age-thresholded directory.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GarbageCollectorDirectoryOptions {
    /// How often recurring garbage collection runs, in milliseconds.
    ///
    /// Ignored by [`crate::Admin::run_gc_once`], but preserved so the same option
    /// shape matches SlateDB's core garbage collector configuration.
    #[uniffi(default = None)]
    pub interval_ms: Option<u64>,
    /// Minimum file age before it can be garbage collected, in milliseconds.
    pub min_age_ms: u64,
    /// Whether to log files that would be deleted without deleting them.
    pub dry_run: bool,
}

impl Default for GarbageCollectorDirectoryOptions {
    fn default() -> Self {
        let core = slatedb::config::GarbageCollectorDirectoryOptions::default();
        Self {
            interval_ms: core.interval.map(|duration| duration.as_millis() as u64),
            min_age_ms: core.min_age.as_millis() as u64,
            dry_run: core.dry_run,
        }
    }
}

impl From<GarbageCollectorDirectoryOptions> for slatedb::config::GarbageCollectorDirectoryOptions {
    fn from(value: GarbageCollectorDirectoryOptions) -> Self {
        Self {
            interval: value.interval_ms.map(Duration::from_millis),
            min_age: Duration::from_millis(value.min_age_ms),
            dry_run: value.dry_run,
        }
    }
}

/// Schedule options for a garbage collector task without a file-age threshold.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GarbageCollectorScheduleOptions {
    /// How often recurring garbage collection runs, in milliseconds.
    ///
    /// Ignored by [`crate::Admin::run_gc_once`].
    #[uniffi(default = None)]
    pub interval_ms: Option<u64>,
}

impl Default for GarbageCollectorScheduleOptions {
    fn default() -> Self {
        let core = slatedb::config::GarbageCollectorScheduleOptions::default();
        Self {
            interval_ms: core.interval.map(|duration| duration.as_millis() as u64),
        }
    }
}

impl From<GarbageCollectorScheduleOptions> for slatedb::config::GarbageCollectorScheduleOptions {
    fn from(value: GarbageCollectorScheduleOptions) -> Self {
        Self {
            interval: value.interval_ms.map(Duration::from_millis),
        }
    }
}

/// Options controlling which garbage collector tasks run.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GarbageCollectorOptions {
    /// Options for manifest files. `None` disables manifest garbage collection.
    #[uniffi(default = None)]
    pub manifest_options: Option<GarbageCollectorDirectoryOptions>,
    /// Options for WAL SST files. `None` disables WAL garbage collection.
    #[uniffi(default = None)]
    pub wal_options: Option<GarbageCollectorDirectoryOptions>,
    /// Options for zero-byte WAL fence objects. `None` disables WAL fence garbage collection.
    #[uniffi(default = None)]
    pub wal_fence_options: Option<GarbageCollectorDirectoryOptions>,
    /// Options for compacted SST files. `None` disables compacted SST garbage collection.
    #[uniffi(default = None)]
    pub compacted_options: Option<GarbageCollectorDirectoryOptions>,
    /// Options for compactor job state files. `None` disables compactions garbage collection.
    #[uniffi(default = None)]
    pub compactions_options: Option<GarbageCollectorDirectoryOptions>,
    /// Options for detaching clone references. `None` disables detach garbage collection.
    #[uniffi(default = None)]
    pub detach_options: Option<GarbageCollectorScheduleOptions>,
}

impl Default for GarbageCollectorOptions {
    fn default() -> Self {
        let core = slatedb::config::GarbageCollectorOptions::default();
        Self {
            manifest_options: core.manifest_options.map(Into::into),
            wal_options: core.wal_options.map(Into::into),
            wal_fence_options: core.wal_fence_options.map(Into::into),
            compacted_options: core.compacted_options.map(Into::into),
            compactions_options: core.compactions_options.map(Into::into),
            detach_options: core.detach_options.map(Into::into),
        }
    }
}

impl From<slatedb::config::GarbageCollectorDirectoryOptions> for GarbageCollectorDirectoryOptions {
    fn from(value: slatedb::config::GarbageCollectorDirectoryOptions) -> Self {
        Self {
            interval_ms: value.interval.map(|duration| duration.as_millis() as u64),
            min_age_ms: value.min_age.as_millis() as u64,
            dry_run: value.dry_run,
        }
    }
}

impl From<slatedb::config::GarbageCollectorScheduleOptions> for GarbageCollectorScheduleOptions {
    fn from(value: slatedb::config::GarbageCollectorScheduleOptions) -> Self {
        Self {
            interval_ms: value.interval.map(|duration| duration.as_millis() as u64),
        }
    }
}

impl From<GarbageCollectorOptions> for slatedb::config::GarbageCollectorOptions {
    fn from(value: GarbageCollectorOptions) -> Self {
        Self {
            manifest_options: value.manifest_options.map(Into::into),
            wal_options: value.wal_options.map(Into::into),
            wal_fence_options: value.wal_fence_options.map(Into::into),
            compacted_options: value.compacted_options.map(Into::into),
            compactions_options: value.compactions_options.map(Into::into),
            detach_options: value.detach_options.map(Into::into),
            metric_level: None,
        }
    }
}

/// Specify options to provide when creating a checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record, Default)]
pub struct CheckpointOptions {
    /// Optionally specifies the lifetime of the checkpoint to create. The expire time will be
    /// set to the current wallclock time plus the specified lifetime. If lifetime is None, then
    /// the checkpoint is created without an expiry time.
    pub lifetime_ms: Option<u64>,

    /// Optionally specifies an existing checkpoint to use as the source for this checkpoint. This
    /// is useful for users to establish checkpoints from existing checkpoints, but with a different
    /// lifecycle and/or metadata.
    pub source: Option<String>,

    /// Optionally specifies a name for the checkpoint. Can be used to list the checkpoints.
    pub name: Option<String>,
}

impl TryFrom<&CheckpointOptions> for slatedb::config::CheckpointOptions {
    type Error = Error;

    fn try_from(
        value: &CheckpointOptions,
    ) -> Result<slatedb::config::CheckpointOptions, Self::Error> {
        Ok(slatedb::config::CheckpointOptions {
            lifetime: value.lifetime_ms.map(Duration::from_millis),
            source: value
                .source
                .as_ref()
                .map(|v| try_checkpoint_id_from_str(v))
                .transpose()?,
            name: value.name.clone(),
        })
    }
}
