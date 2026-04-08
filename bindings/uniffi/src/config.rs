use std::time::Duration;

use crate::error::{Error, SlateDbError};
use ulid::Ulid;

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
#[derive(Clone, Debug, uniffi::Enum)]
pub enum ReadSource {
    /// Read from in-memory memtables.
    Memtable,
    /// Read from the L0 SST with this ULID.
    L0(String),
    /// Read from the sorted run with this manifest id.
    SortedRun(u32),
}

impl TryFrom<ReadSource> for slatedb::config::ReadSource {
    type Error = Error;

    fn try_from(value: ReadSource) -> Result<Self, Self::Error> {
        match value {
            ReadSource::Memtable => Ok(Self::Memtable),
            ReadSource::L0(id) => {
                let id = Ulid::from_string(&id)
                    .map_err(|source| Error::from(SlateDbError::InvalidL0SstId { source }))?;
                Ok(Self::L0(id))
            }
            ReadSource::SortedRun(id) => Ok(Self::SortedRun(id)),
        }
    }
}

/// Source set consulted by point reads and scans.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct ReadSources {
    /// Sources that should be included. An empty list means "all sources".
    pub sources: Vec<ReadSource>,
}

impl TryFrom<ReadSources> for slatedb::config::ReadSources {
    type Error = Error;

    fn try_from(value: ReadSources) -> Result<Self, Self::Error> {
        let mut read_sources = slatedb::config::ReadSources::new(Vec::new());
        for source in value.sources {
            read_sources = read_sources.with_source(source.try_into()?);
        }
        Ok(read_sources)
    }
}

/// Options that control a point read.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ReadOptions {
    /// Minimum durability level a returned row must satisfy.
    pub durability_filter: DurabilityLevel,
    /// Whether uncommitted dirty data may be returned.
    pub dirty: bool,
    /// Whether fetched blocks should be inserted into the block cache.
    pub cache_blocks: bool,
    /// Sources consulted by this read. An empty list means "all sources".
    pub read_sources: ReadSources,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            cache_blocks: true,
            read_sources: ReadSources::default(),
        }
    }
}

impl TryFrom<ReadOptions> for slatedb::config::ReadOptions {
    type Error = Error;

    fn try_from(value: ReadOptions) -> Result<Self, Self::Error> {
        Ok(slatedb::config::ReadOptions {
            durability_filter: value.durability_filter.into(),
            dirty: value.dirty,
            cache_blocks: value.cache_blocks,
            read_sources: value.read_sources.try_into()?,
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

/// Options that control range scans and prefix scans.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScanOptions {
    /// Minimum durability level a returned row must satisfy.
    pub durability_filter: DurabilityLevel,
    /// Whether uncommitted dirty data may be returned.
    pub dirty: bool,
    /// Number of bytes to read ahead while scanning.
    pub read_ahead_bytes: u64,
    /// Whether fetched blocks should be inserted into the block cache.
    pub cache_blocks: bool,
    /// Maximum number of concurrent fetch tasks used by the scan.
    pub max_fetch_tasks: u64,
    /// Sources consulted by this scan. An empty list means "all sources".
    pub read_sources: ReadSources,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
            read_sources: ReadSources::default(),
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
            read_sources: value.read_sources.try_into()?,
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
