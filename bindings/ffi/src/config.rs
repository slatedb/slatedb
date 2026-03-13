use std::ops::Bound;

use slatedb::config as core_config;
use slatedb::{
    IsolationLevel as CoreIsolationLevel, KeyValue as CoreKeyValue,
    SstBlockSize as CoreSstBlockSize, WriteHandle as CoreWriteHandle,
};

use crate::error::SlatedbError;
use crate::validation::try_usize;

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum DurabilityLevel {
    Remote,
    #[default]
    Memory,
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FlushType {
    MemTable,
    #[default]
    Wal,
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum IsolationLevel {
    #[default]
    Snapshot,
    SerializableSnapshot,
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

#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum Ttl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfterTicks(u64),
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbReadOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
}

impl Default for DbReadOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            cache_blocks: true,
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbScanOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub read_ahead_bytes: u64,
    pub cache_blocks: bool,
    pub max_fetch_tasks: u64,
}

impl Default for DbScanOptions {
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

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbWriteOptions {
    pub await_durable: bool,
}

impl Default for DbWriteOptions {
    fn default() -> Self {
        Self {
            await_durable: true,
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbPutOptions {
    pub ttl: Ttl,
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbMergeOptions {
    pub ttl: Ttl,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbFlushOptions {
    pub flush_type: FlushType,
}

impl Default for DbFlushOptions {
    fn default() -> Self {
        Self {
            flush_type: FlushType::default(),
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbKeyRange {
    pub start: Option<Vec<u8>>,
    pub start_inclusive: bool,
    pub end: Option<Vec<u8>>,
    pub end_inclusive: bool,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum DbWriteOperation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbPutOptions,
    },
    Merge {
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbMergeOptions,
    },
    Delete {
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    pub create_ts: i64,
    pub expire_ts: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WriteHandle {
    pub seqnum: u64,
    pub create_ts: i64,
}

impl DurabilityLevel {
    pub(crate) fn into_core(self) -> core_config::DurabilityLevel {
        match self {
            Self::Remote => core_config::DurabilityLevel::Remote,
            Self::Memory => core_config::DurabilityLevel::Memory,
        }
    }
}

impl FlushType {
    pub(crate) fn into_core(self) -> core_config::FlushType {
        match self {
            Self::MemTable => core_config::FlushType::MemTable,
            Self::Wal => core_config::FlushType::Wal,
        }
    }
}

impl IsolationLevel {
    pub(crate) fn into_core(self) -> CoreIsolationLevel {
        match self {
            Self::Snapshot => CoreIsolationLevel::Snapshot,
            Self::SerializableSnapshot => CoreIsolationLevel::SerializableSnapshot,
        }
    }
}

impl SstBlockSize {
    pub(crate) fn into_core(self) -> CoreSstBlockSize {
        match self {
            Self::Block1Kib => CoreSstBlockSize::Block1Kib,
            Self::Block2Kib => CoreSstBlockSize::Block2Kib,
            Self::Block4Kib => CoreSstBlockSize::Block4Kib,
            Self::Block8Kib => CoreSstBlockSize::Block8Kib,
            Self::Block16Kib => CoreSstBlockSize::Block16Kib,
            Self::Block32Kib => CoreSstBlockSize::Block32Kib,
            Self::Block64Kib => CoreSstBlockSize::Block64Kib,
        }
    }
}

impl Ttl {
    pub(crate) fn into_core(self) -> core_config::Ttl {
        match self {
            Self::Default => core_config::Ttl::Default,
            Self::NoExpiry => core_config::Ttl::NoExpiry,
            Self::ExpireAfterTicks(ttl) => core_config::Ttl::ExpireAfter(ttl),
        }
    }
}

impl DbReadOptions {
    pub(crate) fn into_core(self) -> core_config::ReadOptions {
        core_config::ReadOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            cache_blocks: self.cache_blocks,
        }
    }
}

impl DbScanOptions {
    pub(crate) fn into_core(self) -> Result<core_config::ScanOptions, SlatedbError> {
        Ok(core_config::ScanOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            read_ahead_bytes: try_usize(self.read_ahead_bytes, "read_ahead_bytes")?,
            cache_blocks: self.cache_blocks,
            max_fetch_tasks: try_usize(self.max_fetch_tasks, "max_fetch_tasks")?,
        })
    }
}

impl DbWriteOptions {
    pub(crate) fn into_core(self) -> core_config::WriteOptions {
        core_config::WriteOptions {
            await_durable: self.await_durable,
        }
    }
}

impl DbPutOptions {
    pub(crate) fn into_core(self) -> core_config::PutOptions {
        core_config::PutOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

impl DbMergeOptions {
    pub(crate) fn into_core(self) -> core_config::MergeOptions {
        core_config::MergeOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

impl DbFlushOptions {
    pub(crate) fn into_core(self) -> core_config::FlushOptions {
        core_config::FlushOptions {
            flush_type: self.flush_type.into_core(),
        }
    }
}

impl DbKeyRange {
    pub(crate) fn into_bounds(self) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>), SlatedbError> {
        if self.start.as_ref().is_some_and(|start| start.is_empty()) {
            return Err(SlatedbError::Invalid {
                message: "range start cannot be empty".to_owned(),
            });
        }
        if self.end.as_ref().is_some_and(|end| end.is_empty()) {
            return Err(SlatedbError::Invalid {
                message: "range end cannot be empty".to_owned(),
            });
        }

        if let (Some(start), Some(end)) = (&self.start, &self.end) {
            match start.cmp(end) {
                std::cmp::Ordering::Greater => {
                    return Err(SlatedbError::Invalid {
                        message: "range start must not be greater than range end".to_owned(),
                    });
                }
                std::cmp::Ordering::Equal if !(self.start_inclusive && self.end_inclusive) => {
                    return Err(SlatedbError::Invalid {
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

impl KeyValue {
    pub(crate) fn from_core(value: CoreKeyValue) -> Self {
        Self {
            key: value.key.to_vec(),
            value: value.value.to_vec(),
            seq: value.seq,
            create_ts: value.create_ts,
            expire_ts: value.expire_ts,
        }
    }
}

impl WriteHandle {
    pub(crate) fn from_core(value: CoreWriteHandle) -> Self {
        Self {
            seqnum: value.seqnum(),
            create_ts: value.create_ts(),
        }
    }
}
