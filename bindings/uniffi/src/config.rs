use std::time::Duration;

use crate::error::{Error, SlateDbError};

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum DurabilityLevel {
    Remote,
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

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FlushType {
    MemTable,
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

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum IsolationLevel {
    #[default]
    Snapshot,
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

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum SstBlockSize {
    Block1Kib,
    Block2Kib,
    #[default]
    Block4Kib,
    Block8Kib,
    Block16Kib,
    Block32Kib,
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

#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum Ttl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfterTicks(u64),
}

impl From<Ttl> for slatedb::config::Ttl {
    fn from(value: Ttl) -> Self {
        match value {
            Ttl::Default => Self::Default,
            Ttl::NoExpiry => Self::NoExpiry,
            Ttl::ExpireAfterTicks(ttl) => Self::ExpireAfter(ttl),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct ReadOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            cache_blocks: true,
        }
    }
}

impl From<ReadOptions> for slatedb::config::ReadOptions {
    fn from(value: ReadOptions) -> Self {
        slatedb::config::ReadOptions {
            durability_filter: value.durability_filter.into(),
            dirty: value.dirty,
            cache_blocks: value.cache_blocks,
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct ReaderOptions {
    pub manifest_poll_interval_ms: u64,
    pub checkpoint_lifetime_ms: u64,
    pub max_memtable_bytes: u64,
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

#[derive(Clone, Debug, uniffi::Record)]
pub struct ScanOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub read_ahead_bytes: u64,
    pub cache_blocks: bool,
    pub max_fetch_tasks: u64,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
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
        })
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct WriteOptions {
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

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct PutOptions {
    pub ttl: Ttl,
}

impl From<PutOptions> for slatedb::config::PutOptions {
    fn from(value: PutOptions) -> Self {
        slatedb::config::PutOptions {
            ttl: value.ttl.into(),
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct MergeOptions {
    pub ttl: Ttl,
}

impl From<MergeOptions> for slatedb::config::MergeOptions {
    fn from(value: MergeOptions) -> Self {
        slatedb::config::MergeOptions {
            ttl: value.ttl.into(),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record, Default)]
pub struct FlushOptions {
    pub flush_type: FlushType,
}

impl From<FlushOptions> for slatedb::config::FlushOptions {
    fn from(value: FlushOptions) -> Self {
        slatedb::config::FlushOptions {
            flush_type: value.flush_type.into(),
        }
    }
}
