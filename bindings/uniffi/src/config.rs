use std::time::Duration;

use crate::error::SlateDbError;

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum DurabilityLevel {
    Remote,
    #[default]
    Memory,
}

impl DurabilityLevel {
    pub(crate) fn into_core(self) -> slatedb::config::DurabilityLevel {
        match self {
            Self::Remote => slatedb::config::DurabilityLevel::Remote,
            Self::Memory => slatedb::config::DurabilityLevel::Memory,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FlushType {
    MemTable,
    #[default]
    Wal,
}

impl FlushType {
    pub(crate) fn into_core(self) -> slatedb::config::FlushType {
        match self {
            Self::MemTable => slatedb::config::FlushType::MemTable,
            Self::Wal => slatedb::config::FlushType::Wal,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum IsolationLevel {
    #[default]
    Snapshot,
    SerializableSnapshot,
}

impl IsolationLevel {
    pub(crate) fn into_core(self) -> slatedb::IsolationLevel {
        match self {
            Self::Snapshot => slatedb::IsolationLevel::Snapshot,
            Self::SerializableSnapshot => slatedb::IsolationLevel::SerializableSnapshot,
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

impl SstBlockSize {
    pub(crate) fn into_core(self) -> slatedb::SstBlockSize {
        match self {
            Self::Block1Kib => slatedb::SstBlockSize::Block1Kib,
            Self::Block2Kib => slatedb::SstBlockSize::Block2Kib,
            Self::Block4Kib => slatedb::SstBlockSize::Block4Kib,
            Self::Block8Kib => slatedb::SstBlockSize::Block8Kib,
            Self::Block16Kib => slatedb::SstBlockSize::Block16Kib,
            Self::Block32Kib => slatedb::SstBlockSize::Block32Kib,
            Self::Block64Kib => slatedb::SstBlockSize::Block64Kib,
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

impl Ttl {
    pub(crate) fn into_core(self) -> slatedb::config::Ttl {
        match self {
            Self::Default => slatedb::config::Ttl::Default,
            Self::NoExpiry => slatedb::config::Ttl::NoExpiry,
            Self::ExpireAfterTicks(ttl) => slatedb::config::Ttl::ExpireAfter(ttl),
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

impl ReadOptions {
    pub(crate) fn into_core(self) -> slatedb::config::ReadOptions {
        slatedb::config::ReadOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            cache_blocks: self.cache_blocks,
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

impl ReaderOptions {
    pub(crate) fn into_core(self) -> slatedb::config::DbReaderOptions {
        slatedb::config::DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(self.manifest_poll_interval_ms),
            checkpoint_lifetime: Duration::from_millis(self.checkpoint_lifetime_ms),
            max_memtable_bytes: self.max_memtable_bytes,
            skip_wal_replay: self.skip_wal_replay,
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

impl ScanOptions {
    pub(crate) fn into_core(self) -> Result<slatedb::config::ScanOptions, SlateDbError> {
        Ok(slatedb::config::ScanOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            read_ahead_bytes: usize::try_from(self.read_ahead_bytes).map_err(|_| {
                SlateDbError::ValueTooLargeForUsize {
                    field: "read_ahead_bytes",
                }
            })?,
            cache_blocks: self.cache_blocks,
            max_fetch_tasks: usize::try_from(self.max_fetch_tasks).map_err(|_| {
                SlateDbError::ValueTooLargeForUsize {
                    field: "max_fetch_tasks",
                }
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

impl WriteOptions {
    pub(crate) fn into_core(self) -> slatedb::config::WriteOptions {
        slatedb::config::WriteOptions {
            await_durable: self.await_durable,
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct PutOptions {
    pub ttl: Ttl,
}

impl PutOptions {
    pub(crate) fn into_core(self) -> slatedb::config::PutOptions {
        slatedb::config::PutOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct MergeOptions {
    pub ttl: Ttl,
}

impl MergeOptions {
    pub(crate) fn into_core(self) -> slatedb::config::MergeOptions {
        slatedb::config::MergeOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record, Default)]
pub struct FlushOptions {
    pub flush_type: FlushType,
}

impl FlushOptions {
    pub(crate) fn into_core(self) -> slatedb::config::FlushOptions {
        slatedb::config::FlushOptions {
            flush_type: self.flush_type.into_core(),
        }
    }
}
