//! FFI-safe configuration types shared across the SlateDB wrapper.

use std::ops::Bound;

use slatedb::config as core_config;
use slatedb::{
    IsolationLevel as CoreIsolationLevel, KeyValue as CoreKeyValue,
    SstBlockSize as CoreSstBlockSize, WriteHandle as CoreWriteHandle,
};

use crate::error::SlatedbError;
use crate::validation::try_usize;

/// Controls which durability level reads are allowed to observe.
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum DurabilityLevel {
    /// Return only data durable in remote object storage.
    Remote,
    /// Return the latest visible data, including in-memory state.
    #[default]
    Memory,
}

/// Selects which in-memory structures should be flushed.
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FlushType {
    /// Flush the memtable contents.
    MemTable,
    /// Flush the WAL contents.
    #[default]
    Wal,
}

/// Isolation level used when starting a transaction.
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum IsolationLevel {
    /// Snapshot isolation.
    #[default]
    Snapshot,
    /// Serializable snapshot isolation.
    SerializableSnapshot,
}

/// SST block sizes that can be selected on [`crate::DbBuilder`].
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum SstBlockSize {
    /// Use 1 KiB SST blocks.
    Block1Kib,
    /// Use 2 KiB SST blocks.
    Block2Kib,
    /// Use 4 KiB SST blocks.
    #[default]
    Block4Kib,
    /// Use 8 KiB SST blocks.
    Block8Kib,
    /// Use 16 KiB SST blocks.
    Block16Kib,
    /// Use 32 KiB SST blocks.
    Block32Kib,
    /// Use 64 KiB SST blocks.
    Block64Kib,
}

/// Time-to-live configuration for put and merge operations.
#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum Ttl {
    /// Use the database default TTL behavior.
    #[default]
    Default,
    /// Store the value without an expiry.
    NoExpiry,
    /// Expire the value after the provided number of clock ticks.
    ExpireAfterTicks(u64),
}

/// Options for point reads.
#[derive(Clone, Debug, uniffi::Record)]
pub struct DbReadOptions {
    /// The durability level that the read must observe.
    pub durability_filter: DurabilityLevel,
    /// Whether dirty state may be returned.
    pub dirty: bool,
    /// Whether fetched blocks should be inserted into the cache.
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

/// Options for range scans and prefix scans.
#[derive(Clone, Debug, uniffi::Record)]
pub struct DbScanOptions {
    /// The durability level that the scan must observe.
    pub durability_filter: DurabilityLevel,
    /// Whether dirty state may be returned.
    pub dirty: bool,
    /// The number of bytes to read ahead while scanning.
    pub read_ahead_bytes: u64,
    /// Whether fetched blocks should be inserted into the cache.
    pub cache_blocks: bool,
    /// The maximum number of background fetch tasks.
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

/// Options that control write durability.
#[derive(Clone, Debug, uniffi::Record)]
pub struct DbWriteOptions {
    /// Whether the call should wait for the write to become durable.
    pub await_durable: bool,
}

impl Default for DbWriteOptions {
    fn default() -> Self {
        Self {
            await_durable: true,
        }
    }
}

/// Options for put operations.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbPutOptions {
    /// TTL to apply to the written value.
    pub ttl: Ttl,
}

/// Options for merge operations.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbMergeOptions {
    /// TTL to apply to the merged value.
    pub ttl: Ttl,
}

/// Options for manual flushes.
#[derive(Clone, Debug, uniffi::Record)]
pub struct DbFlushOptions {
    /// The flush mode to execute.
    pub flush_type: FlushType,
}

impl Default for DbFlushOptions {
    fn default() -> Self {
        Self {
            flush_type: FlushType::default(),
        }
    }
}

/// A range of keys used for scans.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbKeyRange {
    /// The optional lower bound of the range.
    pub start: Option<Vec<u8>>,
    /// Whether the lower bound is inclusive.
    pub start_inclusive: bool,
    /// The optional upper bound of the range.
    pub end: Option<Vec<u8>>,
    /// Whether the upper bound is inclusive.
    pub end_inclusive: bool,
}

/// A single operation in a batch write.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum DbWriteOperation {
    /// Put a value for a key.
    Put {
        /// The key to write.
        key: Vec<u8>,
        /// The value to write.
        value: Vec<u8>,
        /// Per-operation put options.
        options: DbPutOptions,
    },
    /// Merge an operand into a key.
    Merge {
        /// The key to merge into.
        key: Vec<u8>,
        /// The merge operand.
        value: Vec<u8>,
        /// Per-operation merge options.
        options: DbMergeOptions,
    },
    /// Delete a key.
    Delete {
        /// The key to delete.
        key: Vec<u8>,
    },
}

/// A key-value pair returned by reads and iterators.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyValue {
    /// The row key.
    pub key: Vec<u8>,
    /// The row value.
    pub value: Vec<u8>,
    /// The sequence number that produced this row.
    pub seq: u64,
    /// The creation timestamp assigned by SlateDB.
    pub create_ts: i64,
    /// The optional expiry timestamp assigned by SlateDB.
    pub expire_ts: Option<i64>,
}

/// Metadata returned from a successful write.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WriteHandle {
    /// The sequence number assigned to the write.
    pub seqnum: u64,
    /// The creation timestamp assigned to the write.
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
