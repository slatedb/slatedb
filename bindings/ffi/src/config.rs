//! FFI-safe configuration types shared across the SlateDB wrapper.

use std::ops::Bound;
use std::time::Duration;

use slatedb::{IsolationLevel, KeyValue, SstBlockSize, WriteHandle};

use crate::error::FfiSlatedbError;
use crate::validation::try_usize;

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FfiDurabilityLevel {
    Remote,
    #[default]
    Memory,
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FfiFlushType {
    MemTable,
    #[default]
    Wal,
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FfiIsolationLevel {
    #[default]
    Snapshot,
    SerializableSnapshot,
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

#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum FfiTtl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfterTicks(u64),
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

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiPutOptions {
    pub ttl: FfiTtl,
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiMergeOptions {
    pub ttl: FfiTtl,
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

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct FfiKeyRange {
    pub start: Option<Vec<u8>>,
    pub start_inclusive: bool,
    pub end: Option<Vec<u8>>,
    pub end_inclusive: bool,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum FfiWriteOperation {
    Put {
        key: Vec<u8>,
        value_bytes: Vec<u8>,
        options: FfiPutOptions,
    },
    Merge {
        key: Vec<u8>,
        operand: Vec<u8>,
        options: FfiMergeOptions,
    },
    Delete {
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiKeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    pub create_ts: i64,
    pub expire_ts: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FfiWriteHandle {
    pub seqnum: u64,
    pub create_ts: i64,
}

impl FfiDurabilityLevel {
    pub(crate) fn into_core(self) -> slatedb::config::DurabilityLevel {
        match self {
            Self::Remote => slatedb::config::DurabilityLevel::Remote,
            Self::Memory => slatedb::config::DurabilityLevel::Memory,
        }
    }
}

impl FfiFlushType {
    pub(crate) fn into_core(self) -> slatedb::config::FlushType {
        match self {
            Self::MemTable => slatedb::config::FlushType::MemTable,
            Self::Wal => slatedb::config::FlushType::Wal,
        }
    }
}

impl FfiIsolationLevel {
    pub(crate) fn into_core(self) -> IsolationLevel {
        match self {
            Self::Snapshot => IsolationLevel::Snapshot,
            Self::SerializableSnapshot => IsolationLevel::SerializableSnapshot,
        }
    }
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

impl FfiTtl {
    pub(crate) fn into_core(self) -> slatedb::config::Ttl {
        match self {
            Self::Default => slatedb::config::Ttl::Default,
            Self::NoExpiry => slatedb::config::Ttl::NoExpiry,
            Self::ExpireAfterTicks(ttl) => slatedb::config::Ttl::ExpireAfter(ttl),
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

impl FfiScanOptions {
    pub(crate) fn into_core(self) -> Result<slatedb::config::ScanOptions, FfiSlatedbError> {
        Ok(slatedb::config::ScanOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            read_ahead_bytes: try_usize(self.read_ahead_bytes, "read_ahead_bytes")?,
            cache_blocks: self.cache_blocks,
            max_fetch_tasks: try_usize(self.max_fetch_tasks, "max_fetch_tasks")?,
        })
    }
}

impl FfiWriteOptions {
    pub(crate) fn into_core(self) -> slatedb::config::WriteOptions {
        slatedb::config::WriteOptions {
            await_durable: self.await_durable,
        }
    }
}

impl FfiPutOptions {
    pub(crate) fn into_core(self) -> slatedb::config::PutOptions {
        slatedb::config::PutOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

impl FfiMergeOptions {
    pub(crate) fn into_core(self) -> slatedb::config::MergeOptions {
        slatedb::config::MergeOptions {
            ttl: self.ttl.into_core(),
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

impl FfiKeyRange {
    pub(crate) fn into_bounds(self) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>), FfiSlatedbError> {
        if self.start.as_ref().is_some_and(|start| start.is_empty()) {
            return Err(FfiSlatedbError::Invalid {
                message: "range start cannot be empty".to_owned(),
            });
        }
        if self.end.as_ref().is_some_and(|end| end.is_empty()) {
            return Err(FfiSlatedbError::Invalid {
                message: "range end cannot be empty".to_owned(),
            });
        }

        if let (Some(start), Some(end)) = (&self.start, &self.end) {
            match start.cmp(end) {
                std::cmp::Ordering::Greater => {
                    return Err(FfiSlatedbError::Invalid {
                        message: "range start must not be greater than range end".to_owned(),
                    });
                }
                std::cmp::Ordering::Equal if !(self.start_inclusive && self.end_inclusive) => {
                    return Err(FfiSlatedbError::Invalid {
                        message: "range must be non-empty".to_owned(),
                    });
                }
                _ => {}
            }
        }

        Ok((
            match self.start {
                Some(start) if self.start_inclusive => Bound::Included(start),
                Some(start) => Bound::Excluded(start),
                None => Bound::Unbounded,
            },
            match self.end {
                Some(end) if self.end_inclusive => Bound::Included(end),
                Some(end) => Bound::Excluded(end),
                None => Bound::Unbounded,
            },
        ))
    }
}

impl FfiKeyValue {
    pub(crate) fn from_core(value: KeyValue) -> Self {
        Self {
            key: value.key.to_vec(),
            value: value.value.to_vec(),
            seq: value.seq,
            create_ts: value.create_ts,
            expire_ts: value.expire_ts,
        }
    }
}

impl FfiWriteHandle {
    pub(crate) fn from_core(value: WriteHandle) -> Self {
        Self {
            seqnum: value.seqnum(),
            create_ts: value.create_ts(),
        }
    }
}
