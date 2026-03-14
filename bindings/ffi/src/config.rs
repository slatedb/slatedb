use std::time::Duration;

use slatedb::{IsolationLevel, SstBlockSize};

use crate::error::FfiSlateDbError;
use crate::validation::try_usize;

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FfiDurabilityLevel {
    Remote,
    #[default]
    Memory,
}

impl FfiDurabilityLevel {
    pub(crate) fn into_core(self) -> slatedb::config::DurabilityLevel {
        match self {
            Self::Remote => slatedb::config::DurabilityLevel::Remote,
            Self::Memory => slatedb::config::DurabilityLevel::Memory,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FfiFlushType {
    MemTable,
    #[default]
    Wal,
}

impl FfiFlushType {
    pub(crate) fn into_core(self) -> slatedb::config::FlushType {
        match self {
            Self::MemTable => slatedb::config::FlushType::MemTable,
            Self::Wal => slatedb::config::FlushType::Wal,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FfiIsolationLevel {
    #[default]
    Snapshot,
    SerializableSnapshot,
}

impl FfiIsolationLevel {
    pub(crate) fn into_core(self) -> IsolationLevel {
        match self {
            Self::Snapshot => IsolationLevel::Snapshot,
            Self::SerializableSnapshot => IsolationLevel::SerializableSnapshot,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FfiSstBlockSize {
    Block1Kib,
    Block2Kib,
    #[default]
    Block4Kib,
    Block8Kib,
    Block16Kib,
    Block32Kib,
    Block64Kib,
}

impl FfiSstBlockSize {
    pub(crate) fn into_core(self) -> SstBlockSize {
        match self {
            Self::Block1Kib => SstBlockSize::Block1Kib,
            Self::Block2Kib => SstBlockSize::Block2Kib,
            Self::Block4Kib => SstBlockSize::Block4Kib,
            Self::Block8Kib => SstBlockSize::Block8Kib,
            Self::Block16Kib => SstBlockSize::Block16Kib,
            Self::Block32Kib => SstBlockSize::Block32Kib,
            Self::Block64Kib => SstBlockSize::Block64Kib,
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum FfiTtl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfterTicks(u64),
}

impl FfiTtl {
    pub(crate) fn into_core(self) -> slatedb::config::Ttl {
        match self {
            Self::Default => slatedb::config::Ttl::Default,
            Self::NoExpiry => slatedb::config::Ttl::NoExpiry,
            Self::ExpireAfterTicks(ttl) => slatedb::config::Ttl::ExpireAfter(ttl),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiReadOptions {
    pub durability_filter: FfiDurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
}

impl Default for FfiReadOptions {
    fn default() -> Self {
        Self {
            durability_filter: FfiDurabilityLevel::default(),
            dirty: false,
            cache_blocks: true,
        }
    }
}

impl FfiReadOptions {
    pub(crate) fn into_core(self) -> slatedb::config::ReadOptions {
        slatedb::config::ReadOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            cache_blocks: self.cache_blocks,
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiReaderOptions {
    pub manifest_poll_interval_ms: u64,
    pub checkpoint_lifetime_ms: u64,
    pub max_memtable_bytes: u64,
    pub skip_wal_replay: bool,
}

impl Default for FfiReaderOptions {
    fn default() -> Self {
        Self {
            manifest_poll_interval_ms: 10_000,
            checkpoint_lifetime_ms: 600_000,
            max_memtable_bytes: 64 * 1024 * 1024,
            skip_wal_replay: false,
        }
    }
}

impl FfiReaderOptions {
    pub(crate) fn into_core(self) -> slatedb::config::DbReaderOptions {
        let mut options = slatedb::config::DbReaderOptions::default();
        options.manifest_poll_interval = Duration::from_millis(self.manifest_poll_interval_ms);
        options.checkpoint_lifetime = Duration::from_millis(self.checkpoint_lifetime_ms);
        options.max_memtable_bytes = self.max_memtable_bytes;
        options.skip_wal_replay = self.skip_wal_replay;
        options
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiScanOptions {
    pub durability_filter: FfiDurabilityLevel,
    pub dirty: bool,
    pub read_ahead_bytes: u64,
    pub cache_blocks: bool,
    pub max_fetch_tasks: u64,
}

impl Default for FfiScanOptions {
    fn default() -> Self {
        Self {
            durability_filter: FfiDurabilityLevel::default(),
            dirty: false,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
        }
    }
}

impl FfiScanOptions {
    pub(crate) fn into_core(self) -> Result<slatedb::config::ScanOptions, FfiSlateDbError> {
        Ok(slatedb::config::ScanOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            read_ahead_bytes: try_usize(self.read_ahead_bytes, "read_ahead_bytes")?,
            cache_blocks: self.cache_blocks,
            max_fetch_tasks: try_usize(self.max_fetch_tasks, "max_fetch_tasks")?,
        })
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiWriteOptions {
    pub await_durable: bool,
}

impl Default for FfiWriteOptions {
    fn default() -> Self {
        Self {
            await_durable: true,
        }
    }
}

impl FfiWriteOptions {
    pub(crate) fn into_core(self) -> slatedb::config::WriteOptions {
        slatedb::config::WriteOptions {
            await_durable: self.await_durable,
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiPutOptions {
    pub ttl: FfiTtl,
}

impl FfiPutOptions {
    pub(crate) fn into_core(self) -> slatedb::config::PutOptions {
        slatedb::config::PutOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiMergeOptions {
    pub ttl: FfiTtl,
}

impl FfiMergeOptions {
    pub(crate) fn into_core(self) -> slatedb::config::MergeOptions {
        slatedb::config::MergeOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct FfiFlushOptions {
    pub flush_type: FfiFlushType,
}

impl Default for FfiFlushOptions {
    fn default() -> Self {
        Self {
            flush_type: FfiFlushType::default(),
        }
    }
}

impl FfiFlushOptions {
    pub(crate) fn into_core(self) -> slatedb::config::FlushOptions {
        slatedb::config::FlushOptions {
            flush_type: self.flush_type.into_core(),
        }
    }
}
