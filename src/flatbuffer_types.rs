use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Vector, WIPOffset};
use ulid::Ulid;

use crate::checkpoint;
use crate::db_state::{self, SsTableInfo, SsTableInfoCodec};
use crate::db_state::{CoreDbState, SsTableHandle};

#[path = "./generated/manifest_generated.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod manifest_generated;
pub use manifest_generated::{
    BlockMeta, BlockMetaArgs, ManifestV1, ManifestV1Args, SsTableIndex, SsTableIndexArgs,
    SsTableInfo as FbSsTableInfo, SsTableInfoArgs,
};

use crate::config::CompressionCodec;
use crate::db_state::SsTableId;
use crate::db_state::SsTableId::Compacted;
use crate::error::SlateDBError;
use crate::flatbuffer_types::manifest_generated::{
    Checkpoint, CheckpointArgs, CheckpointMetadata, CompactedSsTable, CompactedSsTableArgs,
    CompactedSstId, CompactedSstIdArgs, CompressionFormat, SortedRun, SortedRunArgs, Uuid,
    UuidArgs,
};
use crate::manifest::{ExternalDb, Manifest, ManifestCodec};
use crate::partitioned_keyspace::RangePartitionedKeySpace;
use crate::utils::clamp_allocated_size_bytes;

pub(crate) const MANIFEST_FORMAT_VERSION: u16 = 1;

/// A wrapper around a `Bytes` buffer containing a FlatBuffer-encoded `SsTableIndex`.
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

impl ManifestCodec for FlatBufferManifestCodec {
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
    fn unix_ts_to_time(unix_ts: u32) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(unix_ts as u64)
    }

    fn maybe_unix_ts_to_time(unix_ts: u32) -> Option<SystemTime> {
        if unix_ts == 0 {
            None
        } else {
            Some(Self::unix_ts_to_time(unix_ts))
        }
    }

    fn decode_uuid(uuid: Uuid) -> uuid::Uuid {
        uuid::Uuid::from_u64_pair(uuid.high(), uuid.low())
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
            let l0_sst = SsTableHandle::new(sst_id, sst_info);
            l0.push_back(l0_sst);
        }
        let mut compacted = Vec::new();
        for manifest_sr in manifest.compacted().iter() {
            let mut ssts = Vec::new();
            for manifest_sst in manifest_sr.ssts().iter() {
                let id = Compacted(manifest_sst.id().ulid());
                let info = FlatBufferSsTableInfoCodec::sst_info(&manifest_sst.info());
                ssts.push(SsTableHandle::new(id, info));
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
                expire_time: Self::maybe_unix_ts_to_time(cp.checkpoint_expire_time_s()),
                create_time: Self::unix_ts_to_time(cp.checkpoint_create_time_s()),
            })
            .collect();
        let core = CoreDbState {
            initialized: manifest.initialized(),
            l0_last_compacted,
            l0,
            compacted,
            next_wal_sst_id: manifest.wal_id_last_seen() + 1,
            last_compacted_wal_sst_id: manifest.wal_id_last_compacted(),
            last_l0_seq: manifest.last_l0_seq(),
            last_l0_clock_tick: manifest.last_l0_clock_tick(),
            checkpoints,
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
        let uidu128 = ulid.0;
        let high = (uidu128 >> 64) as u64;
        let low = ((uidu128 << 64) >> 64) as u64;
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

    #[allow(clippy::panic)]
    fn add_compacted_sst(
        &mut self,
        id: &SsTableId,
        info: &SsTableInfo,
    ) -> WIPOffset<CompactedSsTable<'b>> {
        let ulid = match id {
            SsTableId::Wal(_) => {
                panic!("cannot pass WAL SST handle to create compacted sst")
            }
            SsTableId::Compacted(ulid) => *ulid,
        };
        let compacted_sst_id = self.add_compacted_sst_id(&ulid);
        let compacted_sst_info = self.add_sst_info(info);
        CompactedSsTable::create(
            &mut self.builder,
            &CompactedSsTableArgs {
                id: Some(compacted_sst_id),
                info: Some(compacted_sst_info),
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
        let compacted_ssts: Vec<WIPOffset<CompactedSsTable>> = ssts
            .map(|sst| self.add_compacted_sst(&sst.id, &sst.info))
            .collect();
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

    fn time_to_unix_ts(time: &SystemTime) -> u32 {
        time.duration_since(UNIX_EPOCH)
            .expect("manifest expire time cannot be earlier than epoch")
            .as_secs() as u32 // TODO: check bounds
    }

    fn maybe_time_to_unix_ts(time: Option<&SystemTime>) -> u32 {
        time.map(Self::time_to_unix_ts).unwrap_or(0)
    }

    fn add_checkpoint(&mut self, checkpoint: &checkpoint::Checkpoint) -> WIPOffset<Checkpoint<'b>> {
        let id = self.add_uuid(checkpoint.id);
        let checkpoint_expire_time_s = Self::maybe_time_to_unix_ts(checkpoint.expire_time.as_ref());
        let checkpoint_create_time_s = Self::time_to_unix_ts(&checkpoint.create_time);
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
            let external_dbs: Vec<WIPOffset<manifest_generated::ExternalDb>> = manifest
                .external_dbs
                .iter()
                .map(|external_db| {
                    let db_external_db_args = manifest_generated::ExternalDbArgs {
                        path: Some(self.builder.create_string(&external_db.path)),
                        source_checkpoint_id: Some(self.add_uuid(external_db.source_checkpoint_id)),
                        final_checkpoint_id: external_db
                            .final_checkpoint_id
                            .map(|id| self.add_uuid(id)),
                        sst_ids: Some(self.add_compacted_sst_ids(external_db.sst_ids.iter())),
                    };
                    manifest_generated::ExternalDb::create(&mut self.builder, &db_external_db_args)
                })
                .collect();
            Some(self.builder.create_vector(external_dbs.as_ref()))
        };

        let manifest = ManifestV1::create(
            &mut self.builder,
            &ManifestV1Args {
                manifest_id: 0, // todo: get rid of me
                external_dbs,
                initialized: core.initialized,
                writer_epoch: manifest.writer_epoch,
                compactor_epoch: manifest.compactor_epoch,
                wal_id_last_compacted: core.last_compacted_wal_sst_id,
                wal_id_last_seen: core.next_wal_sst_id - 1,
                l0_last_compacted,
                l0: Some(l0),
                compacted: Some(compacted),
                last_l0_clock_tick: core.last_l0_clock_tick,
                checkpoints: Some(checkpoints),
                last_l0_seq: core.last_l0_seq,
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
    use crate::db_state::{CoreDbState, SsTableId};
    use crate::flatbuffer_types::{FlatBufferManifestCodec, SsTableIndexOwned};
    use crate::manifest::{ExternalDb, Manifest, ManifestCodec};
    use crate::{checkpoint, SlateDBError};
    use std::time::{Duration, SystemTime};

    use crate::flatbuffer_types::test_utils::assert_index_clamped;
    use crate::sst::SsTableFormat;
    use crate::test_utils::build_test_sst;
    use bytes::{BufMut, BytesMut};
    use ulid::Ulid;
    use uuid::Uuid;

    use super::{manifest_generated, MANIFEST_FORMAT_VERSION};

    #[test]
    fn test_should_encode_decode_manifest_checkpoints() {
        // given:
        let mut core = CoreDbState::new();
        core.checkpoints = vec![
            checkpoint::Checkpoint {
                id: uuid::Uuid::new_v4(),
                manifest_id: 1,
                expire_time: None,
                create_time: SystemTime::UNIX_EPOCH + Duration::from_secs(100),
            },
            checkpoint::Checkpoint {
                id: uuid::Uuid::new_v4(),
                manifest_id: 2,
                expire_time: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)),
                create_time: SystemTime::UNIX_EPOCH + Duration::from_secs(200),
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
                source_checkpoint_id: Uuid::new_v4(),
                final_checkpoint_id: Some(Uuid::new_v4()),
                sst_ids: vec![
                    SsTableId::Compacted(Ulid::new()),
                    SsTableId::Compacted(Ulid::new()),
                ],
            },
            ExternalDb {
                path: "/path/to/external/second".to_string(),
                source_checkpoint_id: Uuid::new_v4(),
                final_checkpoint_id: Some(Uuid::new_v4()),
                sst_ids: vec![SsTableId::Compacted(Ulid::new())],
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
        let start_off = sst.info.index_offset as usize;
        let end_off = sst.info.index_offset as usize + sst.info.index_len as usize;
        let index_bytes = sst.data.slice(start_off..end_off);
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

        let manifest = manifest_generated::ManifestV1::create(
            &mut fbb,
            &manifest_generated::ManifestV1Args {
                manifest_id: 0,
                external_dbs: None,
                initialized: false,
                writer_epoch: 0,
                compactor_epoch: 0,
                wal_id_last_compacted: 0,
                wal_id_last_seen: 0,
                l0_last_compacted: None,
                l0: Some(l0),
                compacted: Some(compacted),
                checkpoints: Some(checkpoints),
                last_l0_clock_tick: 0,
                last_l0_seq: 0,
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
}
