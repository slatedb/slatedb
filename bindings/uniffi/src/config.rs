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
    /// Expire the value after the given number of milliseconds.
    ExpireAfterMillis(u64),
    /// Expire the value at the given Unix timestamp in milliseconds.
    ExpireAtMillis(i64),
}

impl From<Ttl> for slatedb::config::Ttl {
    fn from(value: Ttl) -> Self {
        match value {
            Ttl::Default => Self::Default,
            Ttl::NoExpiry => Self::NoExpiry,
            Ttl::ExpireAfterMillis(ttl_millis) => Self::ExpireAfterMillis(ttl_millis),
            Ttl::ExpireAtMillis(timestamp_millis) => Self::ExpireAtMillis(timestamp_millis),
        }
    }
}

/// Options for tracing a read operation.
#[derive(Clone, Debug, uniffi::Record)]
pub struct TracingOptions {
    pub trace_id: String,
}

impl From<TracingOptions> for slatedb::config::TracingOptions {
    fn from(value: TracingOptions) -> Self {
        Self {
            trace_id: value.trace_id,
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
    /// Optional caller-supplied tracing settings.
    #[uniffi(default = None)]
    pub tracing_options: Option<TracingOptions>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            cache_blocks: true,
            filter_context: None,
            tracing_options: None,
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
            tracing_options: value.tracing_options.map(Into::into),
        }
    }
}

/// Determines how a [`crate::DbReader`] chooses and refreshes database state.
#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum ReaderMode {
    /// Create and maintain checkpoints while following the latest database state.
    #[default]
    ManagedCheckpoint,
    /// Remain pinned to the database state referenced by the supplied checkpoint UUID string.
    Checkpoint(String),
    /// Follow the latest manifest without creating or maintaining a checkpoint.
    FollowLatest,
}

impl TryFrom<ReaderMode> for slatedb::DbReaderMode {
    type Error = Error;

    fn try_from(value: ReaderMode) -> Result<Self, Self::Error> {
        Ok(match value {
            ReaderMode::ManagedCheckpoint => Self::ManagedCheckpoint,
            ReaderMode::Checkpoint(checkpoint_id) => {
                Self::Checkpoint(try_checkpoint_id_from_str(&checkpoint_id)?)
            }
            ReaderMode::FollowLatest => Self::FollowLatest,
        })
    }
}

/// Which SSTs to preload into the local disk cache when a database or reader opens.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum PreloadLevel {
    /// Preload only L0 SSTs (the most recently written files).
    L0Sst,
    /// Preload all SSTs (both L0 and compacted levels).
    AllSst,
}

impl From<PreloadLevel> for slatedb::config::PreloadLevel {
    fn from(value: PreloadLevel) -> Self {
        match value {
            PreloadLevel::L0Sst => Self::L0Sst,
            PreloadLevel::AllSst => Self::AllSst,
        }
    }
}

/// Options for the local-disk cache that sits in front of the object store.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ObjectStoreCacheOptions {
    /// Root folder where cache files are stored. `None` (default) disables the cache.
    #[uniffi(default = None)]
    pub root_folder: Option<String>,
    /// Limit of the cache size in bytes. `None` means unbounded.
    pub max_cache_size_bytes: Option<u64>,
    /// Size of each cached part file in bytes; expected to be aligned to 1 KiB.
    pub part_size_bytes: u64,
    /// Whether SSTs produced by memtable flushes are written to the cache.
    pub cache_on_flush: bool,
    /// Whether SSTs produced by compaction are written to the cache.
    pub cache_on_compaction: bool,
    /// Which SSTs to preload into the cache on startup, up to the cache size limit.
    /// `None` (default) preloads nothing.
    #[uniffi(default = None)]
    pub preload_disk_cache_on_startup: Option<PreloadLevel>,
    /// How often the cache directory is rescanned to rebuild the evictor's in-memory
    /// map, in milliseconds. `None` scans only once on startup.
    pub scan_interval_ms: Option<u64>,
    /// Maximum number of file handles kept open by the file handle cache.
    pub max_open_file_handles: u64,
}

impl Default for ObjectStoreCacheOptions {
    fn default() -> Self {
        let core = slatedb::config::ObjectStoreCacheOptions::default();
        Self {
            root_folder: None,
            max_cache_size_bytes: core.max_cache_size_bytes.map(|v| v as u64),
            part_size_bytes: core.part_size_bytes as u64,
            cache_on_flush: core.cache_on_flush,
            cache_on_compaction: core.cache_on_compaction,
            preload_disk_cache_on_startup: None,
            scan_interval_ms: core.scan_interval.map(|d| d.as_millis() as u64),
            max_open_file_handles: core.max_open_file_handles as u64,
        }
    }
}

impl TryFrom<ObjectStoreCacheOptions> for slatedb::config::ObjectStoreCacheOptions {
    type Error = Error;

    fn try_from(value: ObjectStoreCacheOptions) -> Result<Self, Self::Error> {
        Ok(slatedb::config::ObjectStoreCacheOptions {
            root_folder: value.root_folder.map(std::path::PathBuf::from),
            max_cache_size_bytes: value
                .max_cache_size_bytes
                .map(|v| {
                    usize::try_from(v).map_err(|_| {
                        Error::from(SlateDbError::ValueTooLargeForUsize {
                            field: "max_cache_size_bytes",
                        })
                    })
                })
                .transpose()?,
            part_size_bytes: usize::try_from(value.part_size_bytes).map_err(|_| {
                Error::from(SlateDbError::ValueTooLargeForUsize {
                    field: "part_size_bytes",
                })
            })?,
            cache_on_flush: value.cache_on_flush,
            cache_on_compaction: value.cache_on_compaction,
            preload_disk_cache_on_startup: value.preload_disk_cache_on_startup.map(Into::into),
            scan_interval: value.scan_interval_ms.map(Duration::from_millis),
            max_open_file_handles: usize::try_from(value.max_open_file_handles).map_err(|_| {
                Error::from(SlateDbError::ValueTooLargeForUsize {
                    field: "max_open_file_handles",
                })
            })?,
        })
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
    /// Maximum number of wrapper-level retries for a single object-store
    /// operation, on top of the `object_store` client's own HTTP retries.
    /// `None` (default) retries transient errors indefinitely; `Some(n)` gives
    /// up after `n` retries and surfaces the underlying error.
    #[uniffi(default = None)]
    pub object_store_max_retries: Option<u32>,
    /// Optional local-disk object-store cache settings. `None` (default) uses
    /// the core defaults, which leave the cache disabled.
    #[uniffi(default = None)]
    pub object_store_cache_options: Option<ObjectStoreCacheOptions>,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            manifest_poll_interval_ms: 10_000,
            checkpoint_lifetime_ms: 600_000,
            max_memtable_bytes: 64 * 1024 * 1024,
            skip_wal_replay: false,
            object_store_max_retries: None,
            object_store_cache_options: None,
        }
    }
}

impl TryFrom<ReaderOptions> for slatedb::config::DbReaderOptions {
    type Error = Error;

    fn try_from(value: ReaderOptions) -> Result<Self, Self::Error> {
        Ok(slatedb::config::DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(value.manifest_poll_interval_ms),
            checkpoint_lifetime: Duration::from_millis(value.checkpoint_lifetime_ms),
            max_memtable_bytes: value.max_memtable_bytes,
            skip_wal_replay: value.skip_wal_replay,
            object_store_max_retries: value.object_store_max_retries,
            object_store_cache_options: value
                .object_store_cache_options
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
            ..Default::default()
        })
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
    /// Optional caller-supplied tracing settings.
    #[uniffi(default = None)]
    pub tracing_options: Option<TracingOptions>,
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
            tracing_options: None,
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
            tracing_options: value.tracing_options.map(Into::into),
        })
    }
}

/// Options that control writes and commits.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct WriteOptions {
    /// Optional caller-supplied sequence number. Zero uses SlateDB's sequence oracle.
    #[uniffi(default = 0)]
    pub seqnum: u64,
}

impl From<WriteOptions> for slatedb::config::WriteOptions {
    fn from(value: WriteOptions) -> Self {
        slatedb::config::WriteOptions {
            seqnum: value.seqnum,
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

/// Options controlling how a database is shut down.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CloseOptions {
    /// The final flush to perform before shutdown. When `None`, no final flush is
    /// triggered and writes that are not durable may be lost.
    pub flush_type: Option<FlushType>,
}

impl Default for CloseOptions {
    fn default() -> Self {
        Self {
            flush_type: Some(FlushType::MemTable),
        }
    }
}

impl From<CloseOptions> for slatedb::config::CloseOptions {
    fn from(value: CloseOptions) -> Self {
        slatedb::config::CloseOptions {
            flush_type: value.flush_type.map(Into::into),
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
    /// Whether GC should delete eligible manifest/compactions metadata without advancing boundary
    /// files. This supports object stores without conditional overwrites (`If-Match`), but allows a
    /// SlateDB client or compactor to begin updating a manifest or compactions file, stop making
    /// progress (for example, because its process or host is suspended), then resume after GC's
    /// `min_age`. It can then recreate a deleted metadata ID and incorrectly report its stale update
    /// as successful. Set `min_age` longer than the maximum lifetime of a stale process, and use the
    /// same setting for every GC operating on the database.
    #[uniffi(default = false)]
    pub disable_boundary_files: bool,
    /// Maximum number of wrapper-level retries for a single object-store
    /// operation, on top of the `object_store` client's own HTTP retries.
    /// `None` (default) retries transient errors indefinitely; `Some(n)` gives
    /// up after `n` retries and surfaces the underlying error.
    #[uniffi(default = None)]
    pub object_store_max_retries: Option<u32>,
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
            disable_boundary_files: !core.boundary_files_enabled,
            object_store_max_retries: core.object_store_max_retries,
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
            boundary_files_enabled: !value.disable_boundary_files,
            object_store_max_retries: value.object_store_max_retries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CloseOptions, FlushType, GarbageCollectorOptions, ObjectStoreCacheOptions, PreloadLevel,
        ReaderOptions,
    };
    use std::time::Duration;

    #[test]
    fn close_options_default_flushes_memtable() {
        let options: slatedb::config::CloseOptions = CloseOptions::default().into();

        assert!(matches!(
            options.flush_type,
            Some(slatedb::config::FlushType::MemTable)
        ));
    }

    #[test]
    fn close_options_can_flush_wal_only() {
        let options: slatedb::config::CloseOptions = CloseOptions {
            flush_type: Some(FlushType::Wal),
        }
        .into();

        assert!(matches!(
            options.flush_type,
            Some(slatedb::config::FlushType::Wal)
        ));
    }

    #[test]
    fn close_options_can_skip_final_flush() {
        let options: slatedb::config::CloseOptions = CloseOptions { flush_type: None }.into();

        assert!(options.flush_type.is_none());
    }

    #[test]
    fn boundary_files_are_enabled_by_default() {
        let gc: slatedb::config::GarbageCollectorOptions =
            GarbageCollectorOptions::default().into();

        assert!(gc.boundary_files_enabled);
    }

    #[test]
    fn boundary_files_can_be_disabled() {
        let gc: slatedb::config::GarbageCollectorOptions = GarbageCollectorOptions {
            disable_boundary_files: true,
            ..GarbageCollectorOptions::default()
        }
        .into();

        assert!(!gc.boundary_files_enabled);
    }

    #[test]
    fn gc_object_store_max_retries_defaults_to_unbounded() {
        let gc: slatedb::config::GarbageCollectorOptions =
            GarbageCollectorOptions::default().into();

        assert_eq!(gc.object_store_max_retries, None);
    }

    #[test]
    fn gc_object_store_max_retries_threads_through() {
        let gc: slatedb::config::GarbageCollectorOptions = GarbageCollectorOptions {
            object_store_max_retries: Some(3),
            ..GarbageCollectorOptions::default()
        }
        .into();

        assert_eq!(gc.object_store_max_retries, Some(3));
    }

    #[test]
    fn reader_object_store_max_retries_defaults_to_unbounded() {
        let reader: slatedb::config::DbReaderOptions = ReaderOptions::default().try_into().unwrap();

        assert_eq!(reader.object_store_max_retries, None);
    }

    #[test]
    fn reader_object_store_max_retries_threads_through() {
        let reader: slatedb::config::DbReaderOptions = ReaderOptions {
            object_store_max_retries: Some(5),
            ..ReaderOptions::default()
        }
        .try_into()
        .unwrap();

        assert_eq!(reader.object_store_max_retries, Some(5));
    }

    #[test]
    fn reader_object_store_cache_options_default_to_core_defaults() {
        let reader: slatedb::config::DbReaderOptions = ReaderOptions::default().try_into().unwrap();
        let core = slatedb::config::ObjectStoreCacheOptions::default();

        assert_eq!(reader.object_store_cache_options.root_folder, None);
        assert_eq!(
            reader.object_store_cache_options.part_size_bytes,
            core.part_size_bytes
        );
        assert_eq!(
            reader.object_store_cache_options.max_cache_size_bytes,
            core.max_cache_size_bytes
        );
        assert_eq!(
            reader.object_store_cache_options.scan_interval,
            core.scan_interval
        );
        assert_eq!(
            reader.object_store_cache_options.max_open_file_handles,
            core.max_open_file_handles
        );
    }

    #[test]
    fn object_store_cache_options_default_mirrors_core_defaults() {
        let converted: slatedb::config::ObjectStoreCacheOptions =
            ObjectStoreCacheOptions::default().try_into().unwrap();
        let core = slatedb::config::ObjectStoreCacheOptions::default();

        assert_eq!(converted.root_folder, core.root_folder);
        assert_eq!(converted.max_cache_size_bytes, core.max_cache_size_bytes);
        assert_eq!(converted.part_size_bytes, core.part_size_bytes);
        assert_eq!(converted.cache_on_flush, core.cache_on_flush);
        assert_eq!(converted.cache_on_compaction, core.cache_on_compaction);
        assert_eq!(
            converted.preload_disk_cache_on_startup,
            core.preload_disk_cache_on_startup
        );
        assert_eq!(converted.scan_interval, core.scan_interval);
        assert_eq!(converted.max_open_file_handles, core.max_open_file_handles);
    }

    #[test]
    fn reader_object_store_cache_options_thread_through() {
        let reader: slatedb::config::DbReaderOptions = ReaderOptions {
            object_store_cache_options: Some(ObjectStoreCacheOptions {
                root_folder: Some("/tmp/slatedb-cache".to_string()),
                max_cache_size_bytes: None,
                part_size_bytes: 1024,
                cache_on_flush: true,
                cache_on_compaction: true,
                preload_disk_cache_on_startup: Some(PreloadLevel::AllSst),
                scan_interval_ms: Some(5_000),
                max_open_file_handles: 16,
            }),
            ..ReaderOptions::default()
        }
        .try_into()
        .unwrap();
        let cache = reader.object_store_cache_options;

        assert_eq!(
            cache.root_folder,
            Some(std::path::PathBuf::from("/tmp/slatedb-cache"))
        );
        assert_eq!(cache.max_cache_size_bytes, None);
        assert_eq!(cache.part_size_bytes, 1024);
        assert!(cache.cache_on_flush);
        assert!(cache.cache_on_compaction);
        assert_eq!(
            cache.preload_disk_cache_on_startup,
            Some(slatedb::config::PreloadLevel::AllSst)
        );
        assert_eq!(cache.scan_interval, Some(Duration::from_secs(5)));
        assert_eq!(cache.max_open_file_handles, 16);
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
