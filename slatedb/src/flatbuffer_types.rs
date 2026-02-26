use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use flatbuffers::{
    FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Vector, VerifierOptions, WIPOffset,
};
use ulid::Ulid;

use crate::bytes_range::BytesRange;
use crate::checkpoint;
use crate::compactor_state::{
    Compaction as CompactorCompaction, CompactionSpec as CompactorCompactionSpec, CompactionStatus,
    Compactions as CompactorCompactions, SourceId,
};
use crate::db_state::{self, SsTableInfo, SsTableInfoCodec, SstType};
use crate::db_state::{ManifestCore, SsTableHandle};

#[path = "./generated/root_generated.rs"]
#[allow(warnings, clippy::disallowed_macros, clippy::disallowed_types, clippy::disallowed_methods, unreachable_pub)]
#[rustfmt::skip]
mod root_generated;
pub(crate) use root_generated::{
    BlockMeta, BlockMetaArgs, ManifestV1, ManifestV1Args, SsTableIndex, SsTableIndexArgs,
    SsTableInfo as FbSsTableInfo, SsTableInfoArgs,
};

use crate::config::CompressionCodec;
use crate::db_state::SsTableId;
use crate::db_state::SsTableId::Compacted;
use crate::error::SlateDBError;
use crate::flatbuffer_types::root_generated::{
    BoundType, Checkpoint, CheckpointArgs, CheckpointMetadata, CompactedSsTable,
    CompactedSsTableArgs, Compaction as FbCompaction, CompactionArgs as FbCompactionArgs,
    CompactionSpec as FbCompactionSpec, CompactionStatus as FbCompactionStatus, CompactionsV1,
    CompactionsV1Args, CompressionFormat, SortedRun, SortedRunArgs, SstType as FbSstType,
    TieredCompactionSpec, TieredCompactionSpecArgs, Ulid as FbUlid, UlidArgs as FbUlidArgs, Uuid,
    UuidArgs,
};
use crate::format::sst::SST_FORMAT_VERSION;
use crate::manifest::{ExternalDb, Manifest};
use crate::partitioned_keyspace::RangePartitionedKeySpace;
use crate::seq_tracker::SequenceTracker;
use crate::utils::clamp_allocated_size_bytes;
use slatedb_txn_obj::ObjectCodec;

pub(crate) const MANIFEST_FORMAT_VERSION: u16 = 1;
pub(crate) const COMPACTIONS_FORMAT_VERSION: u16 = 1;
pub(crate) const ORIGINAL_SST_FORMAT_VERSION: u16 = SST_FORMAT_VERSION;

/// FlatBuffer verifier options with increased table limit.
/// The default limit is 1M tables, but with compression enabled, SST files
/// can have many more blocks than the uncompressed file size would suggest.
fn verifier_options() -> VerifierOptions {
    VerifierOptions {
        max_tables: 1_000_000_000,
        ..Default::default()
    }
}

/// A wrapper around a `Bytes` buffer containing a FlatBuffer-encoded `SsTableIndex`.
#[derive(PartialEq, Eq, Clone)]
pub(crate) struct SsTableIndexOwned {
    data: Bytes,
}

impl SsTableIndexOwned {
    pub(crate) fn new(data: Bytes) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root_with_opts::<SsTableIndex>(&verifier_options(), &data)?;
        Ok(Self { data })
    }

    pub(crate) fn borrow(&self) -> SsTableIndex<'_> {
        let raw = &self.data;
        unsafe { flatbuffers::root_unchecked::<SsTableIndex>(raw) }
    }

    pub(crate) fn data(&self) -> Bytes {
        self.data.clone()
    }

    pub(crate) fn clamp_allocated_size(&self) -> Self {
        Self::new(clamp_allocated_size_bytes(&self.data))
            .expect("clamped buffer could not be decoded to index")
    }

    /// Returns the size of the SSTable index in bytes.
    pub(crate) fn size(&self) -> usize {
        self.data.len()
    }
}

impl RangePartitionedKeySpace for SsTableIndex<'_> {
    fn partitions(&self) -> usize {
        self.block_meta().len()
    }

    fn partition_first_key(&self, partition: usize) -> &[u8] {
        self.block_meta().get(partition).first_key().bytes()
    }
}

#[derive(Clone)]
pub(crate) struct FlatBufferSsTableInfoCodec {}

impl SsTableInfoCodec for FlatBufferSsTableInfoCodec {
    fn encode(&self, info: &SsTableInfo) -> Bytes {
        Self::create_from_sst_info(info)
    }

    fn decode(&self, bytes: &Bytes) -> Result<SsTableInfo, SlateDBError> {
        let info = flatbuffers::root_with_opts::<FbSsTableInfo>(&verifier_options(), bytes)?;
        Ok(Self::sst_info(&info))
    }

    fn clone_box(&self) -> Box<dyn SsTableInfoCodec> {
        Box::new(Self {})
    }
}

impl FlatBufferSsTableInfoCodec {
    pub(crate) fn sst_info(info: &FbSsTableInfo) -> SsTableInfo {
        let first_entry: Option<Bytes> = info
            .first_entry()
            .map(|entry| Bytes::copy_from_slice(entry.bytes()));
        let last_entry: Option<Bytes> = info
            .last_entry()
            .map(|entry| Bytes::copy_from_slice(entry.bytes()));

        SsTableInfo {
            first_entry,
            last_entry,
            index_offset: info.index_offset(),
            index_len: info.index_len(),
            filter_offset: info.filter_offset(),
            filter_len: info.filter_len(),
            compression_codec: info.compression_format().into(),
            sst_type: info.sst_type().into(),
        }
    }

    pub(crate) fn create_from_sst_info(info: &SsTableInfo) -> Bytes {
        let builder = FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        db_fb_builder.create_sst_info(info)
    }
}

pub(crate) struct FlatBufferManifestCodec {}

impl ObjectCodec<Manifest> for FlatBufferManifestCodec {
    fn encode(&self, manifest: &Manifest) -> Bytes {
        Self::create_from_manifest(manifest)
    }

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, Box<dyn std::error::Error + Send + Sync>> {
        if bytes.len() < 2 {
            return Err(Box::new(SlateDBError::EmptyManifest));
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != MANIFEST_FORMAT_VERSION {
            return Err(Box::new(SlateDBError::InvalidVersion {
                format_name: "manifest",
                supported_versions: vec![MANIFEST_FORMAT_VERSION],
                actual_version: version,
            }));
        }
        let unversioned_bytes = bytes.slice(2..);
        let manifest = flatbuffers::root_with_opts::<ManifestV1>(
            &verifier_options(),
            unversioned_bytes.as_ref(),
        )?;
        Ok(Self::manifest(&manifest))
    }
}

impl FlatBufferManifestCodec {
    fn decode_uuid(uuid: Uuid) -> uuid::Uuid {
        uuid::Uuid::from_u64_pair(uuid.high(), uuid.low())
    }

    fn decode_bytes_range(range: root_generated::BytesRange) -> BytesRange {
        let start_key = Self::decode_bytes_bound(range.start_bound());
        let end_key = Self::decode_bytes_bound(range.end_bound());
        BytesRange::new(start_key, end_key)
    }

    fn decode_bytes_bound(bound: root_generated::BytesBound) -> Bound<Bytes> {
        match (bound.bound_type(), bound.key()) {
            (BoundType::Included, Some(key)) => {
                Bound::Included(Bytes::copy_from_slice(key.bytes()))
            }
            (BoundType::Excluded, Some(key)) => {
                Bound::Excluded(Bytes::copy_from_slice(key.bytes()))
            }
            (BoundType::Unbounded, None) => Bound::Unbounded,
            (bound_type, key) => {
                unreachable!("Unsupported bound type: {:?}, key: {:?}", bound_type, key)
            }
        }
    }

    pub(crate) fn manifest(manifest: &ManifestV1) -> Manifest {
        let l0_last_compacted = manifest.l0_last_compacted().map(|id| id.ulid());
        let mut l0 = VecDeque::new();

        for man_sst in manifest.l0().iter() {
            let sst_id = Compacted(man_sst.id().ulid());
            let format_version = man_sst
                .format_version()
                .unwrap_or(ORIGINAL_SST_FORMAT_VERSION);
            let sst_info = FlatBufferSsTableInfoCodec::sst_info(&man_sst.info());
            let l0_sst = SsTableHandle::new_compacted(
                sst_id,
                format_version,
                sst_info,
                man_sst.visible_range().map(Self::decode_bytes_range),
            );
            l0.push_back(l0_sst);
        }
        let mut compacted = Vec::new();
        for manifest_sr in manifest.compacted().iter() {
            let mut ssts = Vec::new();
            for manifest_sst in manifest_sr.ssts().iter() {
                let id = Compacted(manifest_sst.id().ulid());
                let format_version = manifest_sst
                    .format_version()
                    .unwrap_or(ORIGINAL_SST_FORMAT_VERSION);
                let info = FlatBufferSsTableInfoCodec::sst_info(&manifest_sst.info());
                ssts.push(SsTableHandle::new_compacted(
                    id,
                    format_version,
                    info,
                    manifest_sst.visible_range().map(Self::decode_bytes_range),
                ));
            }
            compacted.push(db_state::SortedRun {
                id: manifest_sr.id(),
                ssts,
            })
        }
        let checkpoints: Vec<checkpoint::Checkpoint> = manifest
            .checkpoints()
            .iter()
            .map(|cp| checkpoint::Checkpoint {
                id: Self::decode_uuid(cp.id()),
                manifest_id: cp.manifest_id(),
                expire_time: match cp.checkpoint_expire_time_s() {
                    0 => None,
                    _ => Some(
                        DateTime::<Utc>::from_timestamp(cp.checkpoint_expire_time_s() as i64, 0)
                            .expect("invalid timestamp"),
                    ),
                },
                create_time: DateTime::<Utc>::from_timestamp(
                    cp.checkpoint_create_time_s() as i64,
                    0,
                )
                .expect("invalid timestamp"),
                name: cp.name().map(|s| s.to_string()),
            })
            .collect();
        let sequence_tracker = match manifest.sequence_tracker() {
            Some(bytes) => SequenceTracker::from_bytes(bytes.bytes())
                .expect("Invalid encoding of sequence tracker in manifest."),
            None => SequenceTracker::new(),
        };
        let core = ManifestCore {
            initialized: manifest.initialized(),
            l0_last_compacted,
            l0,
            compacted,
            next_wal_sst_id: manifest.wal_id_last_seen() + 1,
            replay_after_wal_id: manifest.replay_after_wal_id(),
            last_l0_seq: manifest.last_l0_seq(),
            last_l0_clock_tick: manifest.last_l0_clock_tick(),
            checkpoints,
            wal_object_store_uri: manifest.wal_object_store_uri().map(|uri| uri.to_string()),
            recent_snapshot_min_seq: manifest.recent_snapshot_min_seq(),
            sequence_tracker,
        };
        let external_dbs = manifest.external_dbs().map(|external_dbs| {
            external_dbs
                .iter()
                .map(|db| ExternalDb {
                    path: db.path().to_string(),
                    source_checkpoint_id: Self::decode_uuid(db.source_checkpoint_id()),
                    final_checkpoint_id: db.final_checkpoint_id().map(|id| Self::decode_uuid(id)),
                    sst_ids: db.sst_ids().iter().map(|id| Compacted(id.ulid())).collect(),
                })
                .collect()
        });

        Manifest {
            external_dbs: external_dbs.unwrap_or_default(),
            core,
            writer_epoch: manifest.writer_epoch(),
            compactor_epoch: manifest.compactor_epoch(),
        }
    }

    pub(crate) fn create_from_manifest(manifest: &Manifest) -> Bytes {
        let builder = FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        db_fb_builder.create_manifest(manifest)
    }
}

pub(crate) struct FlatBufferCompactionsCodec {}

impl ObjectCodec<CompactorCompactions> for FlatBufferCompactionsCodec {
    fn encode(&self, compactions: &CompactorCompactions) -> Bytes {
        Self::create_from_compactions(compactions)
    }

    fn decode(
        &self,
        bytes: &Bytes,
    ) -> Result<CompactorCompactions, Box<dyn std::error::Error + Send + Sync>> {
        Self::decode_compactions(bytes).map_err(|e| Box::new(e) as _)
    }
}

impl FlatBufferCompactionsCodec {
    pub(crate) fn decode_compactions(bytes: &Bytes) -> Result<CompactorCompactions, SlateDBError> {
        if bytes.len() < 2 {
            return Err(SlateDBError::InvalidCompaction);
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != COMPACTIONS_FORMAT_VERSION {
            return Err(SlateDBError::InvalidVersion {
                format_name: "compactions",
                supported_versions: vec![COMPACTIONS_FORMAT_VERSION],
                actual_version: version,
            });
        }
        let unversioned_bytes = bytes.slice(2..);
        let fb = flatbuffers::root_with_opts::<CompactionsV1>(
            &verifier_options(),
            unversioned_bytes.as_ref(),
        )?;
        Self::compactions(&fb)
    }

    pub(crate) fn compactions(
        compactions: &CompactionsV1,
    ) -> Result<CompactorCompactions, SlateDBError> {
        let recent_compactions = compactions
            .recent_compactions()
            .iter()
            .map(|c| Self::compaction(&c))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CompactorCompactions::new(compactions.compactor_epoch())
            .with_compactions(recent_compactions))
    }

    fn compaction(compaction: &FbCompaction) -> Result<CompactorCompaction, SlateDBError> {
        let spec = Self::compaction_spec(compaction)?;
        let status = CompactionStatus::from(compaction.status());
        let output_ssts = compaction
            .output_ssts()
            .map(|ssts| ssts.iter().map(Self::compacted_sst).collect())
            .unwrap_or_default();
        Ok(CompactorCompaction::new(compaction.id().ulid(), spec)
            .with_status(status)
            .with_output_ssts(output_ssts))
    }

    fn compaction_spec(compaction: &FbCompaction) -> Result<CompactorCompactionSpec, SlateDBError> {
        match compaction.spec_type() {
            FbCompactionSpec::TieredCompactionSpec => {
                let Some(spec) = compaction.spec_as_tiered_compaction_spec() else {
                    return Err(SlateDBError::InvalidCompaction);
                };
                let mut sources = Vec::new();
                if let Some(ssts) = spec.ssts() {
                    sources.extend(ssts.iter().map(|s| SourceId::Sst(s.ulid())));
                }
                let sorted_runs: Vec<u32> = spec
                    .sorted_runs()
                    .map(|runs| runs.iter().collect())
                    .unwrap_or_default();
                sources.extend(sorted_runs.iter().copied().map(SourceId::SortedRun));
                // Destination is not explicitly encoded in the flatbuffer; derive it from SR inputs.
                let destination = sorted_runs.iter().copied().min().unwrap_or(0);
                Ok(CompactorCompactionSpec::new(sources, destination))
            }
            _ => Err(SlateDBError::InvalidCompaction),
        }
    }

    fn compacted_sst(compacted_sst: CompactedSsTable) -> SsTableHandle {
        let id = Compacted(compacted_sst.id().ulid());
        let format_version = compacted_sst
            .format_version()
            .unwrap_or(ORIGINAL_SST_FORMAT_VERSION);
        let info = FlatBufferSsTableInfoCodec::sst_info(&compacted_sst.info());
        let visible_range = compacted_sst
            .visible_range()
            .map(FlatBufferManifestCodec::decode_bytes_range);
        SsTableHandle::new_compacted(id, format_version, info, visible_range)
    }

    pub(crate) fn create_from_compactions(compactions: &CompactorCompactions) -> Bytes {
        let builder = FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        db_fb_builder.create_compactions(compactions)
    }
}

impl FbUlid<'_> {
    pub(crate) fn ulid(&self) -> Ulid {
        Ulid::from((self.high(), self.low()))
    }
}

struct DbFlatBufferBuilder<'b> {
    builder: FlatBufferBuilder<'b>,
}

impl<'b> DbFlatBufferBuilder<'b> {
    fn new(builder: FlatBufferBuilder<'b>) -> Self {
        Self { builder }
    }

    fn add_sst_info(&mut self, info: &SsTableInfo) -> WIPOffset<FbSsTableInfo<'b>> {
        let first_entry = match info.first_entry.as_ref() {
            None => None,
            Some(first_entry_vector) => Some(self.builder.create_vector(first_entry_vector)),
        };
        let last_entry = match info.last_entry.as_ref() {
            None => None,
            Some(last_entry_vector) => Some(self.builder.create_vector(last_entry_vector)),
        };

        FbSsTableInfo::create(
            &mut self.builder,
            &SsTableInfoArgs {
                first_entry,
                last_entry,
                index_offset: info.index_offset,
                index_len: info.index_len,
                filter_offset: info.filter_offset,
                filter_len: info.filter_len,
                compression_format: info.compression_codec.into(),
                sst_type: info.sst_type.into(),
            },
        )
    }

    fn add_ulid(&mut self, ulid: &Ulid) -> WIPOffset<FbUlid<'b>> {
        let uidu128 = ulid.0;
        let high = (uidu128 >> 64) as u64;
        let low = ((uidu128 << 64) >> 64) as u64;
        FbUlid::create(&mut self.builder, &FbUlidArgs { high, low })
    }

    fn add_compacted_sst_id(&mut self, ulid: &Ulid) -> WIPOffset<FbUlid<'b>> {
        self.add_ulid(ulid)
    }

    fn add_compacted_sst_ids<'a, I>(
        &mut self,
        sst_ids: I,
    ) -> WIPOffset<Vector<'b, ForwardsUOffset<FbUlid<'b>>>>
    where
        I: Iterator<Item = &'a SsTableId>,
    {
        let sst_ids: Vec<WIPOffset<FbUlid>> = sst_ids
            .map(|id| id.unwrap_compacted_id())
            .map(|id| self.add_compacted_sst_id(&id))
            .collect();
        self.builder.create_vector(sst_ids.as_ref())
    }

    fn add_ulids<'a, I>(&mut self, ulids: I) -> WIPOffset<Vector<'b, ForwardsUOffset<FbUlid<'b>>>>
    where
        I: Iterator<Item = &'a Ulid>,
    {
        let ulids: Vec<WIPOffset<FbUlid>> = ulids.map(|ulid| self.add_ulid(ulid)).collect();
        self.builder.create_vector(ulids.as_ref())
    }

    fn add_compacted_sst(&mut self, handle: &SsTableHandle) -> WIPOffset<CompactedSsTable<'b>> {
        let ulid = match handle.id {
            SsTableId::Wal(_) => {
                unreachable!("cannot pass WAL SST handle to create compacted sst")
            }
            SsTableId::Compacted(ulid) => ulid,
        };
        let compacted_sst_id = self.add_compacted_sst_id(&ulid);
        let compacted_sst_info = self.add_sst_info(&handle.info);
        let visible_range = handle
            .visible_range
            .as_ref()
            .map(|r| self.add_bytes_range(r));
        CompactedSsTable::create(
            &mut self.builder,
            &CompactedSsTableArgs {
                id: Some(compacted_sst_id),
                info: Some(compacted_sst_info),
                visible_range,
                format_version: Some(handle.format_version),
            },
        )
    }

    fn add_compacted_ssts<'a, I>(
        &mut self,
        ssts: I,
    ) -> WIPOffset<Vector<'b, ForwardsUOffset<CompactedSsTable<'b>>>>
    where
        I: Iterator<Item = &'a SsTableHandle>,
    {
        let compacted_ssts: Vec<WIPOffset<CompactedSsTable>> =
            ssts.map(|sst| self.add_compacted_sst(sst)).collect();
        self.builder.create_vector(compacted_ssts.as_ref())
    }

    fn add_sorted_run(&mut self, sorted_run: &db_state::SortedRun) -> WIPOffset<SortedRun<'b>> {
        let ssts = self.add_compacted_ssts(sorted_run.ssts.iter());
        SortedRun::create(
            &mut self.builder,
            &SortedRunArgs {
                id: sorted_run.id,
                ssts: Some(ssts),
            },
        )
    }

    fn add_sorted_runs(
        &mut self,
        sorted_runs: &[db_state::SortedRun],
    ) -> WIPOffset<Vector<'b, ForwardsUOffset<SortedRun<'b>>>> {
        let sorted_runs_fbs: Vec<WIPOffset<SortedRun>> = sorted_runs
            .iter()
            .map(|sr| self.add_sorted_run(sr))
            .collect();
        self.builder.create_vector(sorted_runs_fbs.as_ref())
    }

    fn add_compaction_spec(
        &mut self,
        spec: &CompactorCompactionSpec,
    ) -> (FbCompactionSpec, WIPOffset<flatbuffers::UnionWIPOffset>) {
        let mut ssts = Vec::new();
        let mut sorted_runs = Vec::new();
        for source in spec.sources().iter() {
            match source {
                SourceId::Sst(id) => ssts.push(*id),
                SourceId::SortedRun(id) => sorted_runs.push(*id),
            }
        }
        let ssts = (!ssts.is_empty()).then(|| self.add_ulids(ssts.iter()));
        let sorted_runs =
            (!sorted_runs.is_empty()).then(|| self.builder.create_vector(sorted_runs.as_slice()));
        let tiered_spec = TieredCompactionSpec::create(
            &mut self.builder,
            &TieredCompactionSpecArgs { ssts, sorted_runs },
        );
        (
            FbCompactionSpec::TieredCompactionSpec,
            tiered_spec.as_union_value(),
        )
    }

    fn add_compaction(&mut self, compaction: &CompactorCompaction) -> WIPOffset<FbCompaction<'b>> {
        let id = self.add_ulid(&compaction.id());
        let (spec_type, spec) = self.add_compaction_spec(compaction.spec());
        let status = FbCompactionStatus::from(compaction.status());
        let output_ssts = (!compaction.output_ssts().is_empty())
            .then(|| self.add_compacted_ssts(compaction.output_ssts().iter()));
        FbCompaction::create(
            &mut self.builder,
            &FbCompactionArgs {
                id: Some(id),
                spec_type,
                spec: Some(spec),
                status,
                output_ssts,
            },
        )
    }

    fn add_compactions<'a, I>(
        &mut self,
        compactions: I,
    ) -> WIPOffset<Vector<'b, ForwardsUOffset<FbCompaction<'b>>>>
    where
        I: Iterator<Item = &'a CompactorCompaction>,
    {
        let compactions: Vec<WIPOffset<FbCompaction>> =
            compactions.map(|c| self.add_compaction(c)).collect();
        self.builder.create_vector(compactions.as_ref())
    }

    fn add_uuid(&mut self, uuid: uuid::Uuid) -> WIPOffset<Uuid<'b>> {
        let (high, low) = uuid.as_u64_pair();
        Uuid::create(&mut self.builder, &UuidArgs { high, low })
    }

    fn add_checkpoint(&mut self, checkpoint: &checkpoint::Checkpoint) -> WIPOffset<Checkpoint<'b>> {
        let id = self.add_uuid(checkpoint.id);
        let checkpoint_expire_time_s =
            checkpoint.expire_time.map(|t| t.timestamp()).unwrap_or(0) as u32;
        let checkpoint_create_time_s = checkpoint.create_time.timestamp() as u32;
        let name = checkpoint
            .name
            .as_ref()
            .map(|n| self.builder.create_string(n));
        Checkpoint::create(
            &mut self.builder,
            &CheckpointArgs {
                id: Some(id),
                manifest_id: checkpoint.manifest_id,
                checkpoint_expire_time_s,
                checkpoint_create_time_s,
                metadata: None,
                metadata_type: CheckpointMetadata::NONE,
                name,
            },
        )
    }

    fn add_checkpoints(
        &mut self,
        checkpoints: &[checkpoint::Checkpoint],
    ) -> WIPOffset<Vector<'b, ForwardsUOffset<Checkpoint<'b>>>> {
        let checkpoints_fb_vec: Vec<WIPOffset<Checkpoint>> =
            checkpoints.iter().map(|c| self.add_checkpoint(c)).collect();
        self.builder.create_vector(checkpoints_fb_vec.as_ref())
    }

    fn add_bytes_range(&mut self, range: &BytesRange) -> WIPOffset<root_generated::BytesRange<'b>> {
        let start_bound = self.add_bytes_bound(range.start_bound());
        let end_bound = self.add_bytes_bound(range.end_bound());
        root_generated::BytesRange::create(
            &mut self.builder,
            &root_generated::BytesRangeArgs {
                start_bound: Some(start_bound),
                end_bound: Some(end_bound),
            },
        )
    }

    fn add_bytes_bound(
        &mut self,
        bound: Bound<&Bytes>,
    ) -> WIPOffset<root_generated::BytesBound<'b>> {
        let (bound_type, key) = match bound {
            Bound::Included(key) => (BoundType::Included, Some(self.builder.create_vector(key))),
            Bound::Excluded(key) => (BoundType::Excluded, Some(self.builder.create_vector(key))),
            Bound::Unbounded => (BoundType::Unbounded, None),
        };
        root_generated::BytesBound::create(
            &mut self.builder,
            &root_generated::BytesBoundArgs { key, bound_type },
        )
    }

    fn create_compactions(&mut self, compactions: &CompactorCompactions) -> Bytes {
        let compactions_fb = self.add_compactions(compactions.iter());
        let compactions = CompactionsV1::create(
            &mut self.builder,
            &CompactionsV1Args {
                compactor_epoch: compactions.compactor_epoch,
                recent_compactions: Some(compactions_fb),
            },
        );
        self.builder.finish(compactions, None);
        let mut bytes = BytesMut::new();
        bytes.put_u16(COMPACTIONS_FORMAT_VERSION);
        bytes.put_slice(self.builder.finished_data());
        bytes.into()
    }

    fn create_manifest(&mut self, manifest: &Manifest) -> Bytes {
        let core = &manifest.core;
        let l0 = self.add_compacted_ssts(core.l0.iter());
        let mut l0_last_compacted = None;
        if let Some(ulid) = core.l0_last_compacted.as_ref() {
            l0_last_compacted = Some(self.add_compacted_sst_id(ulid))
        }
        let compacted = self.add_sorted_runs(&core.compacted);
        let checkpoints = self.add_checkpoints(&core.checkpoints);
        let external_dbs = if manifest.external_dbs.is_empty() {
            None
        } else {
            let external_dbs: Vec<WIPOffset<root_generated::ExternalDb>> = manifest
                .external_dbs
                .iter()
                .map(|external_db| {
                    let db_external_db_args = root_generated::ExternalDbArgs {
                        path: Some(self.builder.create_string(&external_db.path)),
                        source_checkpoint_id: Some(self.add_uuid(external_db.source_checkpoint_id)),
                        final_checkpoint_id: external_db
                            .final_checkpoint_id
                            .map(|id| self.add_uuid(id)),
                        sst_ids: Some(self.add_compacted_sst_ids(external_db.sst_ids.iter())),
                    };
                    root_generated::ExternalDb::create(&mut self.builder, &db_external_db_args)
                })
                .collect();
            Some(self.builder.create_vector(external_dbs.as_ref()))
        };

        let wal_object_store_uri = core
            .wal_object_store_uri
            .as_ref()
            .map(|uri| self.builder.create_string(uri));
        let sequence_tracker_data = core.sequence_tracker.to_bytes();
        let sequence_tracker = self.builder.create_vector(sequence_tracker_data.as_slice());

        let manifest = ManifestV1::create(
            &mut self.builder,
            &ManifestV1Args {
                manifest_id: 0, // todo: get rid of me
                external_dbs,
                initialized: core.initialized,
                writer_epoch: manifest.writer_epoch,
                compactor_epoch: manifest.compactor_epoch,
                replay_after_wal_id: core.replay_after_wal_id,
                wal_id_last_seen: core.next_wal_sst_id - 1,
                l0_last_compacted,
                l0: Some(l0),
                compacted: Some(compacted),
                last_l0_clock_tick: core.last_l0_clock_tick,
                checkpoints: Some(checkpoints),
                last_l0_seq: core.last_l0_seq,
                wal_object_store_uri,
                recent_snapshot_min_seq: core.recent_snapshot_min_seq,
                sequence_tracker: Some(sequence_tracker),
            },
        );
        self.builder.finish(manifest, None);
        let mut bytes = BytesMut::new();
        bytes.put_u16(MANIFEST_FORMAT_VERSION);
        bytes.put_slice(self.builder.finished_data());
        bytes.into()
    }

    fn create_sst_info(&mut self, info: &SsTableInfo) -> Bytes {
        let copy = self.add_sst_info(info);
        self.builder.finish(copy, None);
        Bytes::copy_from_slice(self.builder.finished_data())
    }
}

impl From<Option<CompressionCodec>> for CompressionFormat {
    fn from(value: Option<CompressionCodec>) -> Self {
        match value {
            None => CompressionFormat::None,
            Some(codec) => match codec {
                #[cfg(feature = "snappy")]
                CompressionCodec::Snappy => CompressionFormat::Snappy,
                #[cfg(feature = "lz4")]
                CompressionCodec::Lz4 => CompressionFormat::Lz4,
                #[cfg(feature = "zlib")]
                CompressionCodec::Zlib => CompressionFormat::Zlib,
                #[cfg(feature = "zstd")]
                CompressionCodec::Zstd => CompressionFormat::Zstd,
            },
        }
    }
}

impl From<SstType> for FbSstType {
    fn from(value: SstType) -> Self {
        match value {
            SstType::Compacted => FbSstType::Compacted,
            SstType::Wal => FbSstType::Wal,
        }
    }
}

impl From<FbSstType> for SstType {
    fn from(value: FbSstType) -> Self {
        match value {
            FbSstType::Compacted => SstType::Compacted,
            FbSstType::Wal => SstType::Wal,
            _ => unreachable!("unknown SstType value: {:?}", value),
        }
    }
}

impl From<CompressionFormat> for Option<CompressionCodec> {
    fn from(value: CompressionFormat) -> Self {
        match value {
            #[cfg(feature = "snappy")]
            CompressionFormat::Snappy => Some(CompressionCodec::Snappy),
            #[cfg(feature = "lz4")]
            CompressionFormat::Lz4 => Some(CompressionCodec::Lz4),
            #[cfg(feature = "zlib")]
            CompressionFormat::Zlib => Some(CompressionCodec::Zlib),
            #[cfg(feature = "zstd")]
            CompressionFormat::Zstd => Some(CompressionCodec::Zstd),
            _ => None,
        }
    }
}

impl From<CompactionStatus> for FbCompactionStatus {
    fn from(value: CompactionStatus) -> Self {
        match value {
            CompactionStatus::Submitted => FbCompactionStatus::Submitted,
            CompactionStatus::Running => FbCompactionStatus::Running,
            CompactionStatus::Completed => FbCompactionStatus::Completed,
            CompactionStatus::Failed => FbCompactionStatus::Failed,
        }
    }
}

impl From<FbCompactionStatus> for CompactionStatus {
    fn from(value: FbCompactionStatus) -> Self {
        match value {
            FbCompactionStatus::Submitted => CompactionStatus::Submitted,
            FbCompactionStatus::Running => CompactionStatus::Running,
            FbCompactionStatus::Completed => CompactionStatus::Completed,
            FbCompactionStatus::Failed => CompactionStatus::Failed,
            _ => CompactionStatus::Submitted,
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::flatbuffer_types::SsTableIndexOwned;

    pub(crate) fn assert_index_clamped(index1: &SsTableIndexOwned, index2: &SsTableIndexOwned) {
        assert_eq!(index1.data, index2.data);
        assert_ne!(index1.data.as_ptr(), index2.data.as_ptr());
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::compactor_state::{
        Compaction, CompactionSpec, CompactionStatus, Compactions, SourceId,
    };
    use crate::db_state::{
        ManifestCore, SortedRun, SsTableHandle, SsTableId, SsTableInfo, SstType,
    };
    use crate::flatbuffer_types::{
        FlatBufferCompactionsCodec, FlatBufferManifestCodec, SsTableIndexOwned,
    };
    use crate::manifest::{ExternalDb, Manifest};
    use crate::{checkpoint, error::SlateDBError};
    use slatedb_txn_obj::ObjectCodec;
    use std::collections::VecDeque;

    use crate::flatbuffer_types::test_utils::assert_index_clamped;
    use crate::format::sst::{SsTableFormat, SST_FORMAT_VERSION_LATEST};
    use crate::test_utils::build_test_sst;
    use bytes::{BufMut, Bytes, BytesMut};
    use chrono::{DateTime, Utc};

    use super::{root_generated, COMPACTIONS_FORMAT_VERSION, MANIFEST_FORMAT_VERSION};

    #[test]
    fn test_should_encode_decode_manifest_checkpoints() {
        // given:
        let mut core = ManifestCore::new();
        core.checkpoints = vec![
            checkpoint::Checkpoint {
                id: uuid::Uuid::new_v4(),
                manifest_id: 1,
                expire_time: None,
                create_time: DateTime::<Utc>::from_timestamp(100, 0).expect("invalid timestamp"),
                name: None,
            },
            checkpoint::Checkpoint {
                id: uuid::Uuid::new_v4(),
                manifest_id: 2,
                expire_time: Some(
                    DateTime::<Utc>::from_timestamp(1000, 0).expect("invalid timestamp"),
                ),
                create_time: DateTime::<Utc>::from_timestamp(200, 0).expect("invalid timestamp"),
                name: Some("test_checkpoint".to_string()),
            },
        ];
        let manifest = Manifest::initial(core);
        let codec = FlatBufferManifestCodec {};

        // when:
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).expect("failed to decode manifest");

        // then:
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_should_encode_decode_external_dbs() {
        // given:
        let mut manifest = Manifest::initial(ManifestCore::new());
        manifest.external_dbs = vec![
            ExternalDb {
                path: "/path/to/external/first".to_string(),
                source_checkpoint_id: uuid::Uuid::new_v4(),
                final_checkpoint_id: Some(uuid::Uuid::new_v4()),
                sst_ids: vec![
                    SsTableId::Compacted(ulid::Ulid::new()),
                    SsTableId::Compacted(ulid::Ulid::new()),
                ],
            },
            ExternalDb {
                path: "/path/to/external/second".to_string(),
                source_checkpoint_id: uuid::Uuid::new_v4(),
                final_checkpoint_id: Some(uuid::Uuid::new_v4()),
                sst_ids: vec![SsTableId::Compacted(ulid::Ulid::new())],
            },
        ];
        let codec = FlatBufferManifestCodec {};

        // when:
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).expect("failed to decode manifest");

        // then:
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_should_encode_decode_ssts_with_visible_ranges() {
        fn new_sst_handle(first_entry: &[u8], visible_range: Option<BytesRange>) -> SsTableHandle {
            SsTableHandle::new_compacted(
                SsTableId::Compacted(ulid::Ulid::new()),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo {
                    first_entry: Some(Bytes::copy_from_slice(first_entry)),
                    ..Default::default()
                },
                visible_range,
            )
        }

        // given:
        let mut manifest = Manifest::initial(ManifestCore::new());
        manifest.core.l0 = VecDeque::from(vec![
            new_sst_handle(b"a", None),
            new_sst_handle(b"a", Some(BytesRange::from_ref("c"..="d"))),
        ]);
        manifest.core.compacted = vec![
            SortedRun {
                id: 0,
                ssts: vec![
                    new_sst_handle(b"a", None),
                    new_sst_handle(b"d", Some(BytesRange::from_ref("e".."f"))),
                ],
            },
            SortedRun {
                id: 0,
                ssts: vec![
                    new_sst_handle(b"a", None),
                    new_sst_handle(b"c", Some(BytesRange::from_ref("c"..))),
                    new_sst_handle(b"d", Some(BytesRange::from_ref("e".."f"))),
                ],
            },
        ];

        let codec = FlatBufferManifestCodec {};

        // when:
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).expect("failed to decode manifest");

        // then:
        assert_eq!(manifest, decoded);
    }

    #[tokio::test]
    async fn test_should_set_compacted_sst_type_for_regular_builder() {
        // given/when:
        let format = SsTableFormat::default();
        let sst = build_test_sst(&format, 1).await;

        // then:
        assert_eq!(sst.info.sst_type, SstType::Compacted);
    }

    #[tokio::test]
    async fn test_should_clamp_index_alloc() {
        let format = SsTableFormat::default();
        let sst = build_test_sst(&format, 3).await;
        let data = sst.remaining_as_bytes();
        let start_off = sst.info.index_offset as usize;
        let end_off = sst.info.index_offset as usize + sst.info.index_len as usize;
        let index_bytes = data.slice(start_off..end_off);
        let index = SsTableIndexOwned::new(index_bytes).unwrap();

        let clamped = index.clamp_allocated_size();

        assert_index_clamped(&clamped, &index);
    }

    #[test]
    fn test_should_validate_manifest_version() {
        let codec = FlatBufferManifestCodec {};

        // Create a valid manifest with current version
        let mut fbb = flatbuffers::FlatBufferBuilder::new();

        // Create minimal required fields for ManifestV1
        let l0 = fbb.create_vector::<flatbuffers::WIPOffset<_>>(&[]);
        let compacted = fbb.create_vector::<flatbuffers::WIPOffset<_>>(&[]);
        let checkpoints = fbb.create_vector::<flatbuffers::WIPOffset<_>>(&[]);

        let manifest = root_generated::ManifestV1::create(
            &mut fbb,
            &root_generated::ManifestV1Args {
                manifest_id: 0,
                external_dbs: None,
                initialized: false,
                writer_epoch: 0,
                compactor_epoch: 0,
                replay_after_wal_id: 0,
                wal_id_last_seen: 0,
                l0_last_compacted: None,
                l0: Some(l0),
                compacted: Some(compacted),
                checkpoints: Some(checkpoints),
                last_l0_clock_tick: 0,
                last_l0_seq: 0,
                wal_object_store_uri: None,
                recent_snapshot_min_seq: 0,
                sequence_tracker: None,
            },
        );
        fbb.finish(manifest, None);
        let fb_data = fbb.finished_data();

        let mut bytes = BytesMut::with_capacity(2 + fb_data.len());
        bytes.put_u16(MANIFEST_FORMAT_VERSION);
        bytes.put_slice(fb_data);
        let valid_bytes = bytes.freeze();

        // Test valid version
        match codec.decode(&valid_bytes) {
            Ok(_) => { /* Expected success with valid flatbuffer data */ }
            Err(e) => panic!("Should succeed with valid flatbuffer data: {:?}", e),
        }

        // Test invalid version
        let mut bytes = BytesMut::with_capacity(2 + fb_data.len());
        bytes.put_u16(MANIFEST_FORMAT_VERSION + 1);
        bytes.put_slice(fb_data);
        let invalid_bytes = bytes.freeze();

        match codec.decode(&invalid_bytes) {
            Err(err) => {
                let Some(SlateDBError::InvalidVersion {
                    format_name,
                    supported_versions,
                    actual_version,
                }) = err.downcast_ref()
                else {
                    panic!("Expected SlateDBError::InvalidVersion but got {:?}", err);
                };
                assert_eq!(*format_name, "manifest");
                assert_eq!(*supported_versions, vec![MANIFEST_FORMAT_VERSION]);
                assert_eq!(*actual_version, MANIFEST_FORMAT_VERSION + 1);
            }
            _ => panic!("Should fail with version mismatch"),
        }
    }

    #[test]
    fn test_should_encode_decode_wal_object_store_uri() {
        let mut manifest = Manifest::initial(ManifestCore::new());
        manifest.core.wal_object_store_uri = Some("s3://bucket/path".to_string());

        let codec = FlatBufferManifestCodec {};
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).unwrap();

        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_should_encode_decode_retention_min_seq() {
        let mut manifest = Manifest::initial(ManifestCore::new());
        manifest.core.last_l0_seq = 11111;
        manifest.core.recent_snapshot_min_seq = 12345;

        let codec = FlatBufferManifestCodec {};
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).unwrap();

        assert_eq!(decoded.core.recent_snapshot_min_seq, 12345);

        // Test None case
        let mut manifest_none = Manifest::initial(ManifestCore::new());
        manifest_none.core.recent_snapshot_min_seq = 0;

        let bytes_none = codec.encode(&manifest_none);
        let decoded_none = codec.decode(&bytes_none).unwrap();

        assert_eq!(decoded_none.core.recent_snapshot_min_seq, 0);
    }

    #[test]
    fn test_should_encode_decode_compactions() {
        fn new_output_sst(first_key: &[u8], visible_range: Option<BytesRange>) -> SsTableHandle {
            SsTableHandle::new_compacted(
                SsTableId::Compacted(ulid::Ulid::new()),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo {
                    first_entry: Some(Bytes::copy_from_slice(first_key)),
                    ..Default::default()
                },
                visible_range,
            )
        }

        let output_ssts = vec![
            new_output_sst(b"a", None),
            new_output_sst(b"m", Some(BytesRange::from_ref("n"..="z"))),
        ];
        let compaction_l0 = Compaction::new(
            ulid::Ulid::new(),
            CompactionSpec::new(vec![SourceId::Sst(ulid::Ulid::new())], 0),
        )
        .with_status(CompactionStatus::Running)
        .with_output_ssts(output_ssts);
        let compaction_sr = Compaction::new(
            ulid::Ulid::new(),
            CompactionSpec::new(vec![SourceId::SortedRun(5), SourceId::SortedRun(3)], 3),
        )
        .with_status(CompactionStatus::Failed);
        let mut compactions = Compactions::new(9);
        compactions.insert(compaction_l0.clone());
        compactions.insert(compaction_sr.clone());

        let codec = FlatBufferCompactionsCodec {};
        let bytes = codec.encode(&compactions);
        let decoded = codec.decode(&bytes).expect("failed to decode compactions");

        assert_eq!(decoded.compactor_epoch, compactions.compactor_epoch);
        assert_eq!(
            decoded
                .get(&compaction_l0.id())
                .expect("missing l0 compaction"),
            &compaction_l0
        );
        assert_eq!(
            decoded
                .get(&compaction_sr.id())
                .expect("missing sr compaction"),
            &compaction_sr
        );
    }

    #[test]
    fn test_should_round_trip_compaction_statuses() {
        let statuses = [
            CompactionStatus::Submitted,
            CompactionStatus::Running,
            CompactionStatus::Completed,
            CompactionStatus::Failed,
        ];

        for status in statuses {
            let fb_status = super::FbCompactionStatus::from(status);
            let round_trip = CompactionStatus::from(fb_status);
            assert_eq!(round_trip, status);
        }
    }

    #[test]
    fn test_should_validate_compactions_version() {
        let codec = FlatBufferCompactionsCodec {};
        let compactions = Compactions::new(1);
        let bytes = codec.encode(&compactions);

        let invalid_version = super::COMPACTIONS_FORMAT_VERSION + 1;
        let mut invalid_bytes = bytes.to_vec();
        invalid_bytes[0] = (invalid_version >> 8) as u8;
        invalid_bytes[1] = invalid_version as u8;

        let result = codec.decode(&Bytes::from(invalid_bytes));

        match result {
            Err(err) => {
                let Some(SlateDBError::InvalidVersion {
                    format_name,
                    supported_versions,
                    actual_version,
                }) = err.downcast_ref()
                else {
                    panic!("Expected SlateDBError::InvalidVersion but got {:?}", err);
                };
                assert_eq!(*format_name, "compactions");
                assert_eq!(*supported_versions, vec![COMPACTIONS_FORMAT_VERSION]);
                assert_eq!(*actual_version, invalid_version);
            }
            _ => panic!("Should fail with version mismatch"),
        }
    }

    #[test]
    fn test_should_round_trip_sst_type_wal() {
        // given:
        use crate::db_state::SsTableInfoCodec;
        let codec = super::FlatBufferSsTableInfoCodec {};
        let info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"key1")),
            sst_type: SstType::Wal,
            ..Default::default()
        };

        // when:
        let bytes = codec.encode(&info);
        let decoded = codec.decode(&bytes).unwrap();

        // then:
        assert_eq!(decoded.sst_type, SstType::Wal);
        assert_eq!(decoded, info);
    }

    #[test]
    fn test_should_round_trip_sst_type_compacted() {
        // given:
        use crate::db_state::SsTableInfoCodec;
        let codec = super::FlatBufferSsTableInfoCodec {};
        let info = SsTableInfo {
            first_entry: Some(Bytes::from_static(b"key1")),
            sst_type: SstType::Compacted,
            ..Default::default()
        };

        // when:
        let bytes = codec.encode(&info);
        let decoded = codec.decode(&bytes).unwrap();

        // then:
        assert_eq!(decoded.sst_type, SstType::Compacted);
        assert_eq!(decoded, info);
    }

    #[test]
    fn test_should_default_sst_type_to_compacted_for_missing_field() {
        // given: manually build an SsTableInfo FlatBuffer without calling add_sst_type,
        // simulating a legacy SST that was written before the sst_type field existed
        use super::root_generated::SsTableInfoBuilder;
        use crate::db_state::SsTableInfoCodec;

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let first_entry = fbb.create_vector(b"key1");
        let mut builder = SsTableInfoBuilder::new(&mut fbb);
        builder.add_first_entry(first_entry);
        // deliberately not calling builder.add_sst_type(...)
        let info = builder.finish();
        fbb.finish(info, None);
        let bytes = Bytes::copy_from_slice(fbb.finished_data());

        // when:
        let codec = super::FlatBufferSsTableInfoCodec {};
        let decoded = codec.decode(&bytes).unwrap();

        // then:
        assert_eq!(decoded.sst_type, SstType::Compacted);
    }

    #[test]
    fn test_should_encode_decode_manifest_sst_with_version_set() {
        // given: a manifest with one L0 SST and one sorted run SST
        let mut manifest = Manifest::initial(ManifestCore::new());
        manifest.core.l0 = VecDeque::from(vec![SsTableHandle::new_compacted(
            SsTableId::Compacted(ulid::Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(b"l0key")),
                ..Default::default()
            },
            None,
        )]);
        manifest.core.compacted = vec![SortedRun {
            id: 1,
            ssts: vec![SsTableHandle::new_compacted(
                SsTableId::Compacted(ulid::Ulid::new()),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo {
                    first_entry: Some(Bytes::from_static(b"srkey")),
                    ..Default::default()
                },
                None,
            )],
        }];
        let codec = FlatBufferManifestCodec {};

        // when:
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).expect("failed to decode manifest");

        // then:
        assert_eq!(decoded.core.l0[0].format_version, SST_FORMAT_VERSION_LATEST);
        assert_eq!(
            decoded.core.compacted[0].ssts[0].format_version,
            SST_FORMAT_VERSION_LATEST
        );
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_should_decode_sst_with_no_version_set_using_original_version() {
        // given: manually build a manifest flatbuffer without setting format_version
        // on CompactedSsTable entries, simulating a legacy manifest
        use super::root_generated::{
            CompactedSsTable, CompactedSsTableArgs, ManifestV1, ManifestV1Args,
            SortedRun as FbSortedRun, SortedRunArgs, SsTableInfo as FbSsTableInfo, SsTableInfoArgs,
        };
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        // Build an L0 SST without format_version
        let first_entry = fbb.create_vector(b"l0key");
        let l0_info = FbSsTableInfo::create(
            &mut fbb,
            &SsTableInfoArgs {
                first_entry: Some(first_entry),
                ..Default::default()
            },
        );
        let l0_ulid = ulid::Ulid::new();
        let l0_id = super::root_generated::Ulid::create(
            &mut fbb,
            &super::root_generated::UlidArgs {
                high: (l0_ulid.0 >> 64) as u64,
                low: ((l0_ulid.0 << 64) >> 64) as u64,
            },
        );
        let l0_sst = CompactedSsTable::create(
            &mut fbb,
            &CompactedSsTableArgs {
                id: Some(l0_id),
                info: Some(l0_info),
                visible_range: None,
                format_version: None, // deliberately not set
            },
        );
        let l0_vec = fbb.create_vector(&[l0_sst]);
        // Build a sorted run SST without format_version
        let sr_first_entry = fbb.create_vector(b"srkey");
        let sr_info = FbSsTableInfo::create(
            &mut fbb,
            &SsTableInfoArgs {
                first_entry: Some(sr_first_entry),
                ..Default::default()
            },
        );
        let sr_ulid = ulid::Ulid::new();
        let sr_id = super::root_generated::Ulid::create(
            &mut fbb,
            &super::root_generated::UlidArgs {
                high: (sr_ulid.0 >> 64) as u64,
                low: ((sr_ulid.0 << 64) >> 64) as u64,
            },
        );
        let sr_sst = CompactedSsTable::create(
            &mut fbb,
            &CompactedSsTableArgs {
                id: Some(sr_id),
                info: Some(sr_info),
                visible_range: None,
                format_version: None, // deliberately not set
            },
        );
        let sr_ssts_vec = fbb.create_vector(&[sr_sst]);
        let sorted_run = FbSortedRun::create(
            &mut fbb,
            &SortedRunArgs {
                id: 1,
                ssts: Some(sr_ssts_vec),
            },
        );
        let compacted_vec = fbb.create_vector(&[sorted_run]);
        let checkpoints_vec = fbb
            .create_vector::<flatbuffers::ForwardsUOffset<super::root_generated::Checkpoint>>(&[]);
        let manifest = ManifestV1::create(
            &mut fbb,
            &ManifestV1Args {
                l0: Some(l0_vec),
                compacted: Some(compacted_vec),
                checkpoints: Some(checkpoints_vec),
                ..Default::default()
            },
        );
        fbb.finish(manifest, None);
        let mut bytes = BytesMut::new();
        bytes.put_u16(MANIFEST_FORMAT_VERSION);
        bytes.put_slice(fbb.finished_data());
        let bytes = bytes.freeze();

        // when:
        let codec = FlatBufferManifestCodec {};
        let decoded = codec.decode(&bytes).expect("failed to decode manifest");

        // then: format_version should default to ORIGINAL_SST_FORMAT_VERSION
        assert_eq!(
            decoded.core.l0[0].format_version,
            super::ORIGINAL_SST_FORMAT_VERSION
        );
        assert_eq!(
            decoded.core.compacted[0].ssts[0].format_version,
            super::ORIGINAL_SST_FORMAT_VERSION
        );
    }

    #[test]
    fn test_should_encode_decode_compaction_output_sst_with_version_set() {
        // given: a compaction with one output SST
        let output_sst = SsTableHandle::new_compacted(
            SsTableId::Compacted(ulid::Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(b"key1")),
                ..Default::default()
            },
            None,
        );
        let compaction = Compaction::new(
            ulid::Ulid::new(),
            CompactionSpec::new(vec![SourceId::Sst(ulid::Ulid::new())], 0),
        )
        .with_status(CompactionStatus::Running)
        .with_output_ssts(vec![output_sst]);
        let mut compactions = Compactions::new(1);
        compactions.insert(compaction.clone());
        let codec = FlatBufferCompactionsCodec {};

        // when:
        let bytes = codec.encode(&compactions);
        let decoded = codec.decode(&bytes).expect("failed to decode compactions");

        // then:
        let decoded_compaction = decoded.get(&compaction.id()).expect("missing compaction");
        assert_eq!(
            decoded_compaction.output_ssts()[0].format_version,
            SST_FORMAT_VERSION_LATEST
        );
        assert_eq!(decoded_compaction, &compaction);
    }

    #[test]
    fn test_should_decode_compaction_output_sst_with_no_version_set_using_original_version() {
        // given: manually build a compactions flatbuffer with an output SST
        // that has no format_version set, simulating a legacy compactions file
        use super::root_generated::{
            CompactedSsTable, CompactedSsTableArgs, Compaction as FbCompaction,
            CompactionArgs as FbCompactionArgs, CompactionSpec as FbCompactionSpec,
            CompactionStatus as FbCompactionStatus, CompactionsV1, CompactionsV1Args,
            SsTableInfo as FbSsTableInfo, SsTableInfoArgs, TieredCompactionSpec,
            TieredCompactionSpecArgs,
        };
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        // Build an output SST without format_version
        let first_entry = fbb.create_vector(b"key1");
        let sst_info = FbSsTableInfo::create(
            &mut fbb,
            &SsTableInfoArgs {
                first_entry: Some(first_entry),
                ..Default::default()
            },
        );
        let sst_ulid = ulid::Ulid::new();
        let sst_id = super::root_generated::Ulid::create(
            &mut fbb,
            &super::root_generated::UlidArgs {
                high: (sst_ulid.0 >> 64) as u64,
                low: ((sst_ulid.0 << 64) >> 64) as u64,
            },
        );
        let output_sst = CompactedSsTable::create(
            &mut fbb,
            &CompactedSsTableArgs {
                id: Some(sst_id),
                info: Some(sst_info),
                visible_range: None,
                format_version: None, // deliberately not set
            },
        );
        let output_ssts_vec = fbb.create_vector(&[output_sst]);
        // Build a compaction with a tiered spec
        let source_ulid = ulid::Ulid::new();
        let source_id = super::root_generated::Ulid::create(
            &mut fbb,
            &super::root_generated::UlidArgs {
                high: (source_ulid.0 >> 64) as u64,
                low: ((source_ulid.0 << 64) >> 64) as u64,
            },
        );
        let source_ssts = fbb.create_vector(&[source_id]);
        let spec = TieredCompactionSpec::create(
            &mut fbb,
            &TieredCompactionSpecArgs {
                ssts: Some(source_ssts),
                sorted_runs: None,
            },
        );
        let compaction_ulid = ulid::Ulid::new();
        let compaction_id = super::root_generated::Ulid::create(
            &mut fbb,
            &super::root_generated::UlidArgs {
                high: (compaction_ulid.0 >> 64) as u64,
                low: ((compaction_ulid.0 << 64) >> 64) as u64,
            },
        );
        let compaction = FbCompaction::create(
            &mut fbb,
            &FbCompactionArgs {
                id: Some(compaction_id),
                spec_type: FbCompactionSpec::TieredCompactionSpec,
                spec: Some(spec.as_union_value()),
                status: FbCompactionStatus::Running,
                output_ssts: Some(output_ssts_vec),
            },
        );
        let compactions_vec = fbb.create_vector(&[compaction]);
        let compactions = CompactionsV1::create(
            &mut fbb,
            &CompactionsV1Args {
                compactor_epoch: 1,
                recent_compactions: Some(compactions_vec),
            },
        );
        fbb.finish(compactions, None);
        let mut bytes = BytesMut::new();
        bytes.put_u16(COMPACTIONS_FORMAT_VERSION);
        bytes.put_slice(fbb.finished_data());
        let bytes = bytes.freeze();

        // when:
        let codec = FlatBufferCompactionsCodec {};
        let decoded = codec.decode(&bytes).expect("failed to decode compactions");

        // then: format_version should default to ORIGINAL_SST_FORMAT_VERSION
        let decoded_compaction = decoded.get(&compaction_ulid).expect("missing compaction");
        assert_eq!(
            decoded_compaction.output_ssts()[0].format_version,
            super::ORIGINAL_SST_FORMAT_VERSION
        );
    }
}
