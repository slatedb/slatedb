use std::ops::{Bound, RangeBounds};

use slatedb::bytes::Bytes;
use slatedb::compactor::{
    Compaction as CoreCompaction, CompactionSpec as CoreCompactionSpec,
    CompactionStatus as CoreCompactionStatus, CompactorStateView as CoreCompactorStateView,
    SourceId as CoreSourceId,
};
use slatedb::config::CompressionCodec as CoreCompressionCodec;
use slatedb::manifest::{
    ExternalDb as CoreExternalDb, SortedRun as CoreSortedRun, SsTableHandle as CoreSsTableHandle,
    SsTableId as CoreSsTableId, SsTableInfo as CoreSsTableInfo, SsTableView as CoreSsTableView,
    VersionedManifest as CoreVersionedManifest,
};
use slatedb::CacheTarget as CoreCacheTarget;
use slatedb::ValueDeletable;
use slatedb::{Checkpoint as CoreCheckpoint, VersionedCompactions as CoreVersionedCompactions};
use ulid::Ulid;

use crate::error::{CloseReason, SlateDbError};

type KeyBound = Bound<Vec<u8>>;
type KeyBounds = (KeyBound, KeyBound);
/// A half-open or closed byte-key range used by scan APIs.
#[derive(Clone, Debug, Default, PartialEq, Eq, uniffi::Record)]
pub struct KeyRange {
    /// Inclusive or exclusive lower bound. `None` means unbounded.
    pub start: Option<Vec<u8>>,
    /// Whether `start` is inclusive when present.
    pub start_inclusive: bool,
    /// Inclusive or exclusive upper bound. `None` means unbounded.
    pub end: Option<Vec<u8>>,
    /// Whether `end` is inclusive when present.
    pub end_inclusive: bool,
}

impl KeyRange {
    pub(crate) fn from_bounds<R>(range: R) -> Self
    where
        R: RangeBounds<Bytes>,
    {
        let start = match range.start_bound() {
            Bound::Included(start) | Bound::Excluded(start) => Some(start.to_vec()),
            Bound::Unbounded => None,
        };
        let start_inclusive = matches!(range.start_bound(), Bound::Included(_));
        let end = match range.end_bound() {
            Bound::Included(end) | Bound::Excluded(end) => Some(end.to_vec()),
            Bound::Unbounded => None,
        };
        let end_inclusive = matches!(range.end_bound(), Bound::Included(_));

        Self {
            start,
            start_inclusive,
            end,
            end_inclusive,
        }
    }

    pub(crate) fn into_bounds(self) -> Result<KeyBounds, SlateDbError> {
        if let (Some(start), Some(end)) = (&self.start, &self.end) {
            match start.cmp(end) {
                std::cmp::Ordering::Greater => {
                    return Err(SlateDbError::RangeStartGreaterThanEnd);
                }
                std::cmp::Ordering::Equal if !(self.start_inclusive && self.end_inclusive) => {
                    return Err(SlateDbError::EmptyRange);
                }
                _ => {}
            }
        }

        Ok(self.into_bounds_unchecked())
    }

    /// Converts to bounds without scan-style non-empty validation. Intended for
    /// cache-warming paths where the core layer treats empty or reversed ranges
    /// as a no-op.
    pub(crate) fn into_bounds_unchecked(self) -> KeyBounds {
        (
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
        )
    }
}

/// Metadata returned by a successful write.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WriteHandle {
    /// Sequence number assigned to the write.
    pub seqnum: u64,
    /// Creation timestamp assigned to the write.
    pub create_ts: i64,
}

impl From<slatedb::WriteHandle> for WriteHandle {
    fn from(value: slatedb::WriteHandle) -> Self {
        Self {
            seqnum: value.seqnum(),
            create_ts: value.create_ts(),
        }
    }
}

/// Snapshot of the current database lifecycle and durability state.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct DbStatus {
    /// Highest durable sequence number observed by this handle.
    pub durable_seq: u64,
    /// Present once the handle has been closed.
    pub close_reason: Option<CloseReason>,
}

impl From<slatedb::DbStatus> for DbStatus {
    fn from(value: slatedb::DbStatus) -> Self {
        Self {
            durable_seq: value.durable_seq,
            close_reason: value.close_reason.map(Into::into),
        }
    }
}

/// A key/value pair together with the row version metadata that produced it.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyValue {
    /// Row key.
    pub key: Vec<u8>,
    /// Row value bytes.
    pub value: Vec<u8>,
    /// Sequence number of the row version.
    pub seq: u64,
    /// Creation timestamp of the row version.
    pub create_ts: i64,
    /// Expiration timestamp, if the row has a TTL.
    pub expire_ts: Option<i64>,
}

impl From<slatedb::KeyValue> for KeyValue {
    fn from(value: slatedb::KeyValue) -> Self {
        Self {
            key: value.key.to_vec(),
            value: value.value.to_vec(),
            seq: value.seq,
            create_ts: value.create_ts,
            expire_ts: value.expire_ts,
        }
    }
}

/// Kind of row entry stored in WAL iteration results.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum RowEntryKind {
    /// A regular value row.
    Value,
    /// A delete tombstone.
    Tombstone,
    /// A merge operand row.
    Merge,
}

/// A raw row entry returned from WAL inspection.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RowEntry {
    /// Encoded row kind.
    pub kind: RowEntryKind,
    /// Row key.
    pub key: Vec<u8>,
    /// Row value for value and merge entries. `None` for tombstones.
    pub value: Option<Vec<u8>>,
    /// Sequence number of the entry.
    pub seq: u64,
    /// Creation timestamp if present in the WAL entry.
    pub create_ts: Option<i64>,
    /// Expiration timestamp if present in the WAL entry.
    pub expire_ts: Option<i64>,
}

impl From<slatedb::RowEntry> for RowEntry {
    fn from(entry: slatedb::RowEntry) -> Self {
        let (kind, value) = match entry.value {
            ValueDeletable::Value(value) => (RowEntryKind::Value, Some(value.to_vec())),
            ValueDeletable::Tombstone => (RowEntryKind::Tombstone, None),
            ValueDeletable::Merge(value) => (RowEntryKind::Merge, Some(value.to_vec())),
        };

        Self {
            kind,
            key: entry.key.to_vec(),
            value,
            seq: entry.seq,
            create_ts: entry.create_ts,
            expire_ts: entry.expire_ts,
        }
    }
}

/// Read-only compactor state view.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CompactorStateView {
    /// Latest compactions file, if present.
    pub compactions: Option<VersionedCompactions>,
    /// Latest manifest file.
    pub manifest: VersionedManifest,
}

impl From<&CoreCompactorStateView> for CompactorStateView {
    fn from(value: &CoreCompactorStateView) -> Self {
        Self {
            compactions: value.compactions().map(VersionedCompactions::from),
            manifest: value.manifest().into(),
        }
    }
}

/// A manifest snapshot paired with its version ID.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct VersionedManifest {
    /// Manifest version ID.
    pub id: u64,
    /// Writer epoch stored in the manifest.
    pub writer_epoch: u64,
    /// Compactor epoch stored in the manifest.
    pub compactor_epoch: u64,
    /// Referenced external databases.
    pub external_dbs: Vec<ExternalDb>,
    /// Whether initialization has completed.
    pub initialized: bool,
    /// Last compacted L0 SST view ID, if any.
    pub last_compacted_l0_sst_view_id: Option<String>,
    /// Last compacted L0 SST ID, if any.
    pub last_compacted_l0_sst_id: Option<String>,
    /// Current L0 SST views.
    pub l0: Vec<SsTableView>,
    /// Current compacted sorted runs.
    pub compacted: Vec<SortedRun>,
    /// Next WAL SST ID to assign.
    pub next_wal_sst_id: u64,
    /// WAL replay watermark.
    pub replay_after_wal_id: u64,
    /// Last persisted L0 clock tick.
    pub last_l0_clock_tick: i64,
    /// Last persisted L0 sequence number.
    pub last_l0_seq: u64,
    /// Minimum sequence number still visible to recent snapshots.
    pub recent_snapshot_min_seq: u64,
    /// Tracked checkpoints.
    pub checkpoints: Vec<Checkpoint>,
    /// Dedicated WAL object store URI, if any.
    pub wal_object_store_uri: Option<String>,
}

impl From<&CoreVersionedManifest> for VersionedManifest {
    fn from(value: &CoreVersionedManifest) -> Self {
        Self {
            id: value.id(),
            writer_epoch: value.writer_epoch(),
            compactor_epoch: value.compactor_epoch(),
            external_dbs: value.external_dbs().iter().map(ExternalDb::from).collect(),
            initialized: value.initialized(),
            last_compacted_l0_sst_view_id: value
                .last_compacted_l0_sst_view_id()
                .map(|id| id.to_string()),
            last_compacted_l0_sst_id: value.last_compacted_l0_sst_id().map(|id| id.to_string()),
            l0: value.l0().iter().map(SsTableView::from).collect(),
            compacted: value.compacted().iter().map(SortedRun::from).collect(),
            next_wal_sst_id: value.next_wal_sst_id(),
            replay_after_wal_id: value.replay_after_wal_id(),
            last_l0_clock_tick: value.last_l0_clock_tick(),
            last_l0_seq: value.last_l0_seq(),
            recent_snapshot_min_seq: value.recent_snapshot_min_seq(),
            checkpoints: value.checkpoints().iter().map(Checkpoint::from).collect(),
            wal_object_store_uri: value.wal_object_store_uri().map(str::to_owned),
        }
    }
}

/// A compactions snapshot paired with its version ID.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct VersionedCompactions {
    /// Compactions file version ID.
    pub id: u64,
    /// Compactor epoch recorded in this file.
    pub compactor_epoch: u64,
    /// Recent compactions tracked in this file.
    pub recent_compactions: Vec<Compaction>,
}

impl From<&CoreVersionedCompactions> for VersionedCompactions {
    fn from(value: &CoreVersionedCompactions) -> Self {
        Self {
            id: value.id(),
            compactor_epoch: value.compactor_epoch(),
            recent_compactions: value.recent_compactions().map(Compaction::from).collect(),
        }
    }
}

/// Checkpoint metadata stored in a manifest.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct Checkpoint {
    /// Checkpoint UUID string.
    pub id: String,
    /// Referenced manifest ID.
    pub manifest_id: u64,
    /// Expiration time as Unix UTC seconds, if present.
    pub expire_time_secs: Option<i64>,
    /// Creation time as Unix UTC seconds.
    pub create_time_secs: i64,
    /// Optional checkpoint name.
    pub name: Option<String>,
}

impl From<&CoreCheckpoint> for Checkpoint {
    fn from(value: &CoreCheckpoint) -> Self {
        Self {
            id: value.id.to_string(),
            manifest_id: value.manifest_id,
            expire_time_secs: value.expire_time.map(|ts| ts.timestamp()),
            create_time_secs: value.create_time.timestamp(),
            name: value.name.clone(),
        }
    }
}

/// External DB reference recorded in a manifest.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ExternalDb {
    /// External database path.
    pub path: String,
    /// Source checkpoint UUID string.
    pub source_checkpoint_id: String,
    /// Final checkpoint UUID string, if present.
    pub final_checkpoint_id: Option<String>,
    /// SST IDs referenced from the external DB.
    pub sst_ids: Vec<SsTableId>,
}

impl From<&CoreExternalDb> for ExternalDb {
    fn from(value: &CoreExternalDb) -> Self {
        Self {
            path: value.path.clone(),
            source_checkpoint_id: value.source_checkpoint_id.to_string(),
            final_checkpoint_id: value.final_checkpoint_id.map(|id| id.to_string()),
            sst_ids: value.sst_ids.iter().map(SsTableId::from).collect(),
        }
    }
}

/// Compaction lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CompactionStatus {
    Submitted,
    Running,
    Completed,
    Failed,
}

impl From<CoreCompactionStatus> for CompactionStatus {
    fn from(value: CoreCompactionStatus) -> Self {
        match value {
            CoreCompactionStatus::Submitted => Self::Submitted,
            CoreCompactionStatus::Running => Self::Running,
            CoreCompactionStatus::Completed => Self::Completed,
            CoreCompactionStatus::Failed => Self::Failed,
        }
    }
}

/// Compaction input source identifier.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SourceId {
    /// Existing sorted run ID.
    SortedRun(u32),
    /// L0 SST view ULID string.
    SstView(String),
}

impl From<&CoreSourceId> for SourceId {
    fn from(value: &CoreSourceId) -> Self {
        match value {
            CoreSourceId::SortedRun(id) => Self::SortedRun(*id),
            CoreSourceId::SstView(id) => Self::SstView(id.to_string()),
        }
    }
}

/// Immutable compaction specification.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CompactionSpec {
    /// Ordered compaction sources.
    pub sources: Vec<SourceId>,
    /// Destination sorted run ID.
    pub destination: u32,
    /// Whether any input source is an L0 SST view.
    pub has_l0_sources: bool,
    /// Whether any input source is a sorted run.
    pub has_sr_sources: bool,
}

impl From<&CoreCompactionSpec> for CompactionSpec {
    fn from(value: &CoreCompactionSpec) -> Self {
        Self {
            sources: value.sources().iter().map(SourceId::from).collect(),
            destination: value.destination(),
            has_l0_sources: value.has_l0_sources(),
            has_sr_sources: value.has_sr_sources(),
        }
    }
}

/// Canonical compaction record.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct Compaction {
    /// Compaction ULID string.
    pub id: String,
    /// Compaction spec.
    pub spec: CompactionSpec,
    /// Bytes processed so far.
    pub bytes_processed: u64,
    /// Current compaction status.
    pub status: CompactionStatus,
    /// Output SSTs produced so far.
    pub output_ssts: Vec<SsTableHandle>,
    /// Whether the compaction is active.
    pub active: bool,
}

impl From<&CoreCompaction> for Compaction {
    fn from(value: &CoreCompaction) -> Self {
        Self {
            id: value.id().to_string(),
            spec: value.spec().into(),
            bytes_processed: value.bytes_processed(),
            status: value.status().into(),
            output_ssts: value
                .output_ssts()
                .iter()
                .map(SsTableHandle::from)
                .collect(),
            active: value.active(),
        }
    }
}

/// SSTable identifier.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SsTableId {
    /// WAL SST identified by numeric WAL ID.
    Wal(u64),
    /// Compacted SST identified by ULID string.
    Compacted(String),
}

impl From<&CoreSsTableId> for SsTableId {
    fn from(value: &CoreSsTableId) -> Self {
        match value {
            CoreSsTableId::Wal(id) => Self::Wal(*id),
            CoreSsTableId::Compacted(id) => Self::Compacted(id.to_string()),
        }
    }
}

impl SsTableId {
    pub(crate) fn into_core(self) -> Result<CoreSsTableId, SlateDbError> {
        Ok(match self {
            SsTableId::Wal(id) => CoreSsTableId::Wal(id),
            SsTableId::Compacted(id) => {
                let ulid = Ulid::from_string(&id)
                    .map_err(|source| SlateDbError::InvalidSsTableId { source })?;
                CoreSsTableId::Compacted(ulid)
            }
        })
    }
}

/// Cache content that [`crate::Db::warm_sst`] should populate.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CacheTarget {
    /// Warm all filters on the SST, if any exist.
    Filters,
    /// Warm the SST index.
    Index,
    /// Warm the SST stats block, if one exists.
    Stats,
    /// Warm the SST data blocks that overlap `range`. Also warms the index,
    /// since block planning depends on it.
    Data { range: KeyRange },
}

impl CacheTarget {
    pub(crate) fn into_core(self) -> CoreCacheTarget {
        match self {
            CacheTarget::Filters => CoreCacheTarget::Filters,
            CacheTarget::Index => CoreCacheTarget::Index,
            CacheTarget::Stats => CoreCacheTarget::Stats,
            CacheTarget::Data { range } => {
                let (start, end) = range.into_bounds_unchecked();
                let start = match start {
                    Bound::Included(v) => Bound::Included(Bytes::from(v)),
                    Bound::Excluded(v) => Bound::Excluded(Bytes::from(v)),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let end = match end {
                    Bound::Included(v) => Bound::Included(Bytes::from(v)),
                    Bound::Excluded(v) => Bound::Excluded(Bytes::from(v)),
                    Bound::Unbounded => Bound::Unbounded,
                };
                CoreCacheTarget::Data((start, end))
            }
        }
    }
}

/// Compression codec used for an SSTable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CompressionCodec {
    Snappy,
    Zlib,
    Lz4,
    Zstd,
}

impl From<CoreCompressionCodec> for CompressionCodec {
    fn from(value: CoreCompressionCodec) -> Self {
        match value {
            CoreCompressionCodec::Snappy => Self::Snappy,
            CoreCompressionCodec::Zlib => Self::Zlib,
            CoreCompressionCodec::Lz4 => Self::Lz4,
            CoreCompressionCodec::Zstd => Self::Zstd,
            _ => unreachable!("unexpected CompressionCodec variant"),
        }
    }
}

/// Physical SSTable type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SstType {
    Compacted,
    Wal,
}

/// Filter block format stored in SST metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FilterFormat {
    Legacy,
    Composite,
}

/// SSTable metadata.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SsTableInfo {
    /// First entry in the SSTable, if any.
    pub first_entry: Option<Vec<u8>>,
    /// Last entry in the SSTable, if any.
    pub last_entry: Option<Vec<u8>>,
    /// Index block offset in bytes.
    pub index_offset: u64,
    /// Index block length in bytes.
    pub index_len: u64,
    /// Filter block offset in bytes.
    pub filter_offset: u64,
    /// Filter block length in bytes.
    pub filter_len: u64,
    /// Compression codec, if any.
    pub compression_codec: Option<CompressionCodec>,
    /// Physical SSTable type.
    pub sst_type: SstType,
    /// Stats block offset in bytes.
    pub stats_offset: u64,
    /// Stats block length in bytes.
    pub stats_len: u64,
    /// Filter block format.
    pub filter_format: FilterFormat,
}

impl From<&CoreSsTableInfo> for SsTableInfo {
    fn from(value: &CoreSsTableInfo) -> Self {
        Self {
            first_entry: value.first_entry.clone().map(|entry| entry.to_vec()),
            last_entry: value.last_entry.clone().map(|entry| entry.to_vec()),
            index_offset: value.index_offset,
            index_len: value.index_len,
            filter_offset: value.filter_offset,
            filter_len: value.filter_len,
            compression_codec: value.compression_codec.map(CompressionCodec::from),
            sst_type: sst_type_from_debug(&value.sst_type),
            stats_offset: value.stats_offset,
            stats_len: value.stats_len,
            filter_format: filter_format_from_debug(&value.filter_format),
        }
    }
}

/// A handle to a physical SSTable.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SsTableHandle {
    /// SST ID.
    pub id: SsTableId,
    /// SST metadata.
    pub info: SsTableInfo,
    /// Estimated on-disk size in bytes.
    pub estimated_size_bytes: u64,
}

impl From<&CoreSsTableHandle> for SsTableHandle {
    fn from(value: &CoreSsTableHandle) -> Self {
        Self {
            id: (&value.id).into(),
            info: (&value.info).into(),
            estimated_size_bytes: value.estimate_size(),
        }
    }
}

/// Projected SST view used by manifests and sorted runs.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SsTableView {
    /// View ULID string.
    pub id: String,
    /// Underlying SST handle.
    pub sst: SsTableHandle,
    /// Optional projected visible key range.
    pub visible_range: Option<KeyRange>,
    /// Estimated on-disk size in bytes.
    pub estimated_size_bytes: u64,
}

impl From<&CoreSsTableView> for SsTableView {
    fn from(value: &CoreSsTableView) -> Self {
        Self {
            id: value.id.to_string(),
            sst: (&value.sst).into(),
            visible_range: value.visible_range().map(KeyRange::from_bounds),
            estimated_size_bytes: value.estimate_size(),
        }
    }
}

/// A sorted run made up of one or more SST views.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SortedRun {
    /// Sorted run ID.
    pub id: u32,
    /// SST views in this run.
    pub sst_views: Vec<SsTableView>,
    /// Estimated total size in bytes.
    pub estimated_size_bytes: u64,
}

impl From<&CoreSortedRun> for SortedRun {
    fn from(value: &CoreSortedRun) -> Self {
        Self {
            id: value.id,
            sst_views: value.sst_views.iter().map(SsTableView::from).collect(),
            estimated_size_bytes: value.estimate_size(),
        }
    }
}

fn sst_type_from_debug(value: &impl std::fmt::Debug) -> SstType {
    match format!("{value:?}").as_str() {
        "Compacted" => SstType::Compacted,
        "Wal" => SstType::Wal,
        other => unreachable!("unexpected SstType variant: {other}"),
    }
}

fn filter_format_from_debug(value: &impl std::fmt::Debug) -> FilterFormat {
    match format!("{value:?}").as_str() {
        "Legacy" => FilterFormat::Legacy,
        "Composite" => FilterFormat::Composite,
        other => unreachable!("unexpected FilterFormat variant: {other}"),
    }
}
