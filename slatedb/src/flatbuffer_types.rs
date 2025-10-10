use std::collections::{VecDeque, HashMap};
use std::ops::{Bound, RangeBounds};

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Vector, WIPOffset, UnionWIPOffset};
use ulid::Ulid;

use crate::bytes_range::BytesRange;
use crate::checkpoint;
use crate::db_state::{self, SsTableInfo, SsTableInfoCodec};
use crate::db_state::{CoreDbState, SsTableHandle};

#[path = "./generated/root_generated.rs"]
#[allow(warnings, clippy::disallowed_macros, clippy::disallowed_types, clippy::disallowed_methods)]
#[rustfmt::skip]
mod root_generated;
pub use root_generated::{
    BlockMeta, BlockMetaArgs, ManifestV1, ManifestV1Args, SsTableIndex, SsTableIndexArgs,
    SsTableInfo as FbSsTableInfo, SsTableInfoArgs, CompactionState as FbCompactionState, 
    Compaction as FbCompaction, CompactionSpec as FbCompactionSpec, CompactionJob as FbCompactionJob, CompactionType as FbCompactionType,
    CompactionJobSpec as FbCompactionJobSpec, LinearCompactionJob,
};

use crate::config::CompressionCodec;
use crate::db_state::SsTableId;
use crate::db_state::SsTableId::Compacted;
use crate::error::SlateDBError;
use crate::flatbuffer_types::root_generated::{
    BoundType, Checkpoint, CheckpointArgs, CheckpointMetadata, CompactedSsTable,
    CompactedSsTableArgs, CompactedSstId, CompactedSstIdArgs, CompressionFormat, SortedRun,
    SortedRunArgs, Uuid, UuidArgs, Ulid as FbUlid, UlidArgs, LinearCompactionJobArgs,
    CompactionArgs, CompactionStateArgs, CompactionBuilder,CompactionJobBuilder,
    CompactionStatus as FbCompactionStatus, SortedRunCompaction, SortedRunCompactionArgs,
};
use crate::manifest::{ExternalDb, Manifest};
use crate::partitioned_keyspace::RangePartitionedKeySpace;
use crate::record::RecordCodec;
use crate::seq_tracker::SequenceTracker;
use crate::utils::clamp_allocated_size_bytes;
use crate::compactor_state::{Compaction, CompactionSpec};

pub(crate) const MANIFEST_FORMAT_VERSION: u16 = 1;

/// A wrapper around a `Bytes` buffer containing a FlatBuffer-encoded `SsTableIndex`.
#[derive(PartialEq, Eq, Clone)]
pub(crate) struct SsTableIndexOwned {
    data: Bytes,
}

impl SsTableIndexOwned {
    pub fn new(data: Bytes) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<SsTableIndex>(&data)?;
        Ok(Self { data })
    }

    pub fn borrow(&self) -> SsTableIndex<'_> {
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
        let info = flatbuffers::root::<FbSsTableInfo>(bytes)?;
        Ok(Self::sst_info(&info))
    }

    fn clone_box(&self) -> Box<dyn SsTableInfoCodec> {
        Box::new(Self {})
    }
}

impl FlatBufferSsTableInfoCodec {
    pub fn sst_info(info: &FbSsTableInfo) -> SsTableInfo {
        let first_key: Option<Bytes> = info
            .first_key()
            .map(|key| Bytes::copy_from_slice(key.bytes()));

        SsTableInfo {
            first_key,
            index_offset: info.index_offset(),
            index_len: info.index_len(),
            filter_offset: info.filter_offset(),
            filter_len: info.filter_len(),
            compression_codec: info.compression_format().into(),
        }
    }

    pub fn create_from_sst_info(info: &SsTableInfo) -> Bytes {
        let builder = FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        db_fb_builder.create_sst_info(info)
    }
}

pub(crate) struct FlatBufferManifestCodec {}

impl RecordCodec<Manifest> for FlatBufferManifestCodec {
    fn encode(&self, manifest: &Manifest) -> Bytes {
        Self::create_from_manifest(manifest)
    }

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError> {
        if bytes.len() < 2 {
            return Err(SlateDBError::EmptyManifest);
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != MANIFEST_FORMAT_VERSION {
            return Err(SlateDBError::InvalidVersion {
                expected_version: MANIFEST_FORMAT_VERSION,
                actual_version: version,
            });
        }
        let unversioned_bytes = bytes.slice(2..);
        let manifest = flatbuffers::root::<ManifestV1>(unversioned_bytes.as_ref())?;
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

    pub fn manifest(manifest: &ManifestV1) -> Manifest {
        let l0_last_compacted = manifest
            .l0_last_compacted()
            .map(|id| Ulid::from((id.high(), id.low())));
        let mut l0 = VecDeque::new();

        for man_sst in manifest.l0().iter() {
            let man_sst_id = man_sst.id();
            let sst_id = Compacted(Ulid::from((man_sst_id.high(), man_sst_id.low())));

            let sst_info = FlatBufferSsTableInfoCodec::sst_info(&man_sst.info());
            let l0_sst = SsTableHandle::new_compacted(
                sst_id,
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
                let info = FlatBufferSsTableInfoCodec::sst_info(&manifest_sst.info());
                ssts.push(SsTableHandle::new_compacted(
                    id,
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
            })
            .collect();
        let sequence_tracker = match manifest.sequence_tracker() {
            Some(bytes) => SequenceTracker::from_bytes(bytes.bytes())
                .expect("Invalid encoding of sequence tracker in manifest."),
            None => SequenceTracker::new(),
        };
        let core = CoreDbState {
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

    pub fn create_from_manifest(manifest: &Manifest) -> Bytes {
        let builder = FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        db_fb_builder.create_manifest(manifest)
    }
}

pub(crate) struct FlatBufferCompactionStateCodec {}

impl RecordCodec<CompactionState> for FlatBufferCompactionStateCodec {
    fn encode(&self, compaction_state: &CompactionState) -> Bytes {
        Self::create_compaction_state(compaction_state)
    }

    fn decode(&self, bytes: &Bytes) -> Result<CompactionState, SlateDBError> {
       let fb_compaction_state = flatbuffers::root::<FbCompactionState>(bytes.as_ref())?;
       Ok(Self::compaction_state(&fb_compaction_state))
    }
}

impl FlatBufferCompactionStateCodec {
    fn compaction_state(fb_compaction_state: &FbCompactionState) -> CompactionState {
        CompactionState {
            compactor_epoch: fb_compaction_state.compactor_epoch(),
            compactions: fb_compaction_state
            .recent_compactions()
            .iter()
            .map(|c| {
                let fb_ulid = c.compaction_id().expect("compaction_id missing");
                let key = Ulid::from((fb_ulid.high(), fb_ulid.low()));
                let comp = Compaction {
                id: key,
                status: Self::get_status(c.status()),
                sources: Self::get_sources(&c),
                destination: c.destination(),
                compaction_type: Self::get_compaction_type(c.compaction_type()),
                job_attempts: Self:: get_job_attempts(c.job_attempts()),
                spec: Self::get_spec(c.spec_type()),
            };
            (key, comp)
            })
            .collect(),
        }
    }

    fn get_status(status: FbCompactionStatus) -> CompactionStatus {
        match status {
            FbCompactionStatus::Submitted => CompactionStatus::Submitted,
            FbCompactionStatus::InProgress => CompactionStatus::InProgress,
            FbCompactionStatus::Completed => CompactionStatus::Completed,
            _ => CompactionStatus::Failed,
        }
    }

    fn get_sources(compaction: &FbCompaction) -> Vec<SourceId> {
        if compaction.spec_type() == FbCompactionSpec::SortedRunCompaction {
            if let Some(s) = compaction.spec_as_sorted_run_compaction() {
                let mut sources = Vec::new();
                if let Some(ssts) = s.ssts() {
                    for sst in ssts {
                        sources.push(SourceId::Sst(sst.id().ulid()));
                    }
                }
                if let Some(srs) = s.sorted_runs() {
                    for sr in srs {
                        sources.push(SourceId::SortedRun(sr.id()));
                    }
                }
                return sources;
            }
        }
        Vec::new()
    }

    fn get_compaction_type(compaction_type: FbCompactionType) -> CompactionType {
        match compaction_type {
            FbCompactionType::Internal => CompactionType::Internal,
            FbCompactionType::External => CompactionType::External,
            _ => CompactionType::Internal,
        }
    }

    fn get_job_attempts(_jobs: flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<FbCompactionJob<'_>>>) -> Vec<CompactionJob> {
        // Minimal decode for now; fill in from FB if/when needed.
        Vec::new()
    }

    fn get_spec(_spec: FbCompactionSpec) -> CompactionSpec {
        // Minimal decode: return empty sources; extend as needed
        CompactionSpec::SortedRunCompaction { ssts: Vec::new(), sorted_runs: Vec::new() }
    }

    fn create_compaction_state(compaction_state: &CompactionState) -> Bytes {
        let builder = FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        db_fb_builder.create_compaction_state(compaction_state)
    }
}

impl CompactedSstId<'_> {
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
        let first_key = match info.first_key.as_ref() {
            None => None,
            Some(first_key_vector) => Some(self.builder.create_vector(first_key_vector)),
        };

        FbSsTableInfo::create(
            &mut self.builder,
            &SsTableInfoArgs {
                first_key,
                index_offset: info.index_offset,
                index_len: info.index_len,
                filter_offset: info.filter_offset,
                filter_len: info.filter_len,
                compression_format: info.compression_codec.into(),
            },
        )
    }

    fn add_compacted_sst_id(&mut self, ulid: &Ulid) -> WIPOffset<CompactedSstId<'b>> {
        let (high, low) = Self::ulid_parts(ulid);
        CompactedSstId::create(&mut self.builder, &CompactedSstIdArgs { high, low })
    }

    fn add_compacted_sst_ids<'a, I>(
        &mut self,
        sst_ids: I,
    ) -> WIPOffset<Vector<'b, ForwardsUOffset<CompactedSstId<'b>>>>
    where
        I: Iterator<Item = &'a SsTableId>,
    {
        let sst_ids: Vec<WIPOffset<CompactedSstId>> = sst_ids
            .map(|id| id.unwrap_compacted_id())
            .map(|id| self.add_compacted_sst_id(&id))
            .collect();
        self.builder.create_vector(sst_ids.as_ref())
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

    fn add_uuid(&mut self, uuid: uuid::Uuid) -> WIPOffset<Uuid<'b>> {
        let (high, low) = uuid.as_u64_pair();
        Uuid::create(&mut self.builder, &UuidArgs { high, low })
    }

    #[inline]
    fn ulid_parts(u: &ulid::Ulid) -> (u64, u64) {
        let v = u.0;
        ((v >> 64) as u64, (v & 0xFFFF_FFFF_FFFF_FFFF) as u64)
    }

    fn add_ulid(&mut self, ulid: ulid::Ulid) -> WIPOffset<FbUlid<'b>> {
        let (high, low) = Self::ulid_parts(&ulid);
        FbUlid::create(&mut self.builder, &UlidArgs { high, low })
    }

    fn add_checkpoint(&mut self, checkpoint: &checkpoint::Checkpoint) -> WIPOffset<Checkpoint<'b>> {
        let id = self.add_uuid(checkpoint.id);
        let checkpoint_expire_time_s =
            checkpoint.expire_time.map(|t| t.timestamp()).unwrap_or(0) as u32;
        let checkpoint_create_time_s = checkpoint.create_time.timestamp() as u32;
        Checkpoint::create(
            &mut self.builder,
            &CheckpointArgs {
                id: Some(id),
                manifest_id: checkpoint.manifest_id,
                checkpoint_expire_time_s,
                checkpoint_create_time_s,
                metadata: None,
                metadata_type: CheckpointMetadata::NONE,
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

    fn create_compaction_state(&mut self, compaction_state: &CompactionState) -> Bytes {
        let compactions = self.add_compactions(&compaction_state.compactions);
        let compaction_state = FbCompactionState::create(
            &mut self.builder,
            &CompactionStateArgs {
                compactor_epoch: compaction_state.compactor_epoch,
                recent_compactions: Some(compactions),
            },
        );
        self.builder.finish(compaction_state, None);
        let mut bytes = BytesMut::new();
        bytes.put_slice(self.builder.finished_data());
        bytes.into()
    }

    fn add_compactions(&mut self, compactions: &HashMap<Ulid, Compaction>) -> WIPOffset<Vector<'b, ForwardsUOffset<FbCompaction<'b>>>> { 
        let mut compactions_fb_vec: Vec<WIPOffset<FbCompaction>> = Vec::<WIPOffset<FbCompaction>>::with_capacity(compactions.len());
        for compaction in compactions.values() {
            compactions_fb_vec.push(self.add_compaction(compaction));
        }
        self.builder.create_vector(compactions_fb_vec.as_ref())
    }

    fn add_compaction(&mut self, compaction: &Compaction) -> WIPOffset<FbCompaction<'b>> {
        let compaction_id = self.add_ulid(compaction.id);
        let ssts = self.builder.create_vector::<WIPOffset<CompactedSsTable<'b>>>(&[]);
        let output_sorted_run = SortedRun::create(&mut self.builder, &SortedRunArgs { id: compaction.destination, ssts: Some(ssts) });
        let compaction_type = match compaction.compaction_type {
            CompactionType::Internal => FbCompactionType::Internal,
            CompactionType::External => FbCompactionType::External,
        };
        let job_attempts = self.add_job_attempts(&compaction.job_attempts);
        let (spec_type, spec) = self.add_compaction_spec(&compaction.spec);
        
        let mut builder = CompactionBuilder::new(&mut self.builder);
        builder.add_compaction_id(compaction_id);
        builder.add_compaction_type(compaction_type);
        let fb_status = match compaction.status {
            CompactionStatus::Submitted => FbCompactionStatus::Submitted,
            CompactionStatus::InProgress => FbCompactionStatus::InProgress,
            CompactionStatus::Completed => FbCompactionStatus::Completed,
            CompactionStatus::Failed => FbCompactionStatus::Failed,
        };
        builder.add_status(fb_status);
        builder.add_destination(compaction.destination);
        builder.add_output_sorted_run(output_sorted_run);
        builder.add_job_attempts(job_attempts);
        builder.add_spec_type(spec_type);
        builder.add_spec(spec);
        builder.finish()
    }

    fn add_job_attempts(&mut self, job_attempts: &[CompactionJob]) -> WIPOffset<Vector<'b, ForwardsUOffset<FbCompactionJob<'b>>>> {
        let job_attempts_fb_vec: Vec<WIPOffset<FbCompactionJob>> =
            job_attempts.iter().map(|job_attempt| self.add_job_attempt(job_attempt)).collect();
        self.builder.create_vector(job_attempts_fb_vec.as_ref())
    }

    fn add_job_attempt(&mut self, job_attempt: &CompactionJob) -> WIPOffset<FbCompactionJob<'b>> {
        let job_id = self.add_ulid(job_attempt.id);
        let compaction_id = self.add_ulid(job_attempt.compaction_id);
        let (spec_type, spec) = self.add_compaction_job_spec(&job_attempt.spec);

        let mut builder = CompactionJobBuilder::new(&mut self.builder);
        builder.add_job_id(job_id);
        builder.add_compaction_id(compaction_id);
        builder.add_spec_type(spec_type);
        builder.add_spec(spec);
        builder.finish()
    }

    fn add_compaction_spec(&mut self, spec: &CompactionSpec) -> (FbCompactionSpec, WIPOffset<UnionWIPOffset>) {
        match spec {
            CompactionSpec::SortedRunCompaction { ssts, sorted_runs } => {
                let ssts = self.add_compacted_ssts(ssts.iter());
                let sorted_runs = self.add_sorted_runs(sorted_runs);
                let table = SortedRunCompaction::create(
                    &mut self.builder,
                    &SortedRunCompactionArgs { ssts: Some(ssts), sorted_runs: Some(sorted_runs) },
                );
                (FbCompactionSpec::SortedRunCompaction, WIPOffset::new(table.value()))
            }
        }
    }

    fn add_compaction_job_spec(&mut self, spec: &CompactionJobSpec) -> (FbCompactionJobSpec, WIPOffset<UnionWIPOffset>) {
        match spec {
            CompactionJobSpec::LinearCompactionJob { completed_input_sst_ids, completed_input_sr_ids } => {
                // Build CompactedSstId directly from Ulids
                let sst_ids: Vec<WIPOffset<CompactedSstId>> = completed_input_sst_ids
                    .iter()
                    .map(|ulid| {
                        let v = ulid.0;
                        let high = (v >> 64) as u64;
                        let low = (v & 0xFFFF_FFFF_FFFF_FFFF) as u64;
                        CompactedSstId::create(&mut self.builder, &CompactedSstIdArgs { high, low })
                    })
                    .collect();
                let ssts = self.builder.create_vector(sst_ids.as_ref());
                let sr_ids = self.builder.create_vector::<u32>(completed_input_sr_ids.as_slice());
                let table = LinearCompactionJob::create(
                    &mut self.builder,
                    &LinearCompactionJobArgs {
                        completed_input_sst_ids: Some(ssts),
                        completed_input_sr_ids: Some(sr_ids),
                    },
                );
                (FbCompactionJobSpec::LinearCompactionJob, WIPOffset::new(table.value()))
            }
        }
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
    use crate::db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
    use crate::flatbuffer_types::{FlatBufferManifestCodec, SsTableIndexOwned};
    use crate::manifest::{ExternalDb, Manifest};
    use crate::record::RecordCodec;
    use crate::{checkpoint, error::SlateDBError};
    use std::collections::VecDeque;

    use crate::flatbuffer_types::test_utils::assert_index_clamped;
    use crate::sst::SsTableFormat;
    use crate::test_utils::build_test_sst;
    use bytes::{BufMut, Bytes, BytesMut};
    use chrono::{DateTime, Utc};

    use super::{root_generated, MANIFEST_FORMAT_VERSION};

    #[test]
    fn test_should_encode_decode_manifest_checkpoints() {
        // given:
        let mut core = CoreDbState::new();
        core.checkpoints = vec![
            checkpoint::Checkpoint {
                id: uuid::Uuid::new_v4(),
                manifest_id: 1,
                expire_time: None,
                create_time: DateTime::<Utc>::from_timestamp(100, 0).expect("invalid timestamp"),
            },
            checkpoint::Checkpoint {
                id: uuid::Uuid::new_v4(),
                manifest_id: 2,
                expire_time: Some(
                    DateTime::<Utc>::from_timestamp(1000, 0).expect("invalid timestamp"),
                ),
                create_time: DateTime::<Utc>::from_timestamp(200, 0).expect("invalid timestamp"),
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
        let mut manifest = Manifest::initial(CoreDbState::new());
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
        fn new_sst_handle(first_key: &[u8], visible_range: Option<BytesRange>) -> SsTableHandle {
            SsTableHandle::new_compacted(
                SsTableId::Compacted(ulid::Ulid::new()),
                SsTableInfo {
                    first_key: Some(Bytes::copy_from_slice(first_key)),
                    ..Default::default()
                },
                visible_range,
            )
        }

        // given:
        let mut manifest = Manifest::initial(CoreDbState::new());
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

    #[test]
    fn test_should_clamp_index_alloc() {
        let format = SsTableFormat::default();
        let sst = build_test_sst(&format, 3);
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
            Err(SlateDBError::InvalidVersion {
                expected_version,
                actual_version,
            }) => {
                assert_eq!(expected_version, MANIFEST_FORMAT_VERSION);
                assert_eq!(actual_version, MANIFEST_FORMAT_VERSION + 1);
            }
            _ => panic!("Should fail with version mismatch"),
        }
    }

    #[test]
    fn test_should_encode_decode_wal_object_store_uri() {
        let mut manifest = Manifest::initial(CoreDbState::new());
        manifest.core.wal_object_store_uri = Some("s3://bucket/path".to_string());

        let codec = FlatBufferManifestCodec {};
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).unwrap();

        assert_eq!(manifest, decoded);
    }

    #[test]
    fn test_should_encode_decode_retention_min_seq() {
        let mut manifest = Manifest::initial(CoreDbState::new());
        manifest.core.last_l0_seq = 11111;
        manifest.core.recent_snapshot_min_seq = 12345;

        let codec = FlatBufferManifestCodec {};
        let bytes = codec.encode(&manifest);
        let decoded = codec.decode(&bytes).unwrap();

        assert_eq!(decoded.core.recent_snapshot_min_seq, 12345);

        // Test None case
        let mut manifest_none = Manifest::initial(CoreDbState::new());
        manifest_none.core.recent_snapshot_min_seq = 0;

        let bytes_none = codec.encode(&manifest_none);
        let decoded_none = codec.decode(&bytes_none).unwrap();

        assert_eq!(decoded_none.core.recent_snapshot_min_seq, 0);
    }
}
