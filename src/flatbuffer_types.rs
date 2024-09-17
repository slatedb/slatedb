use std::collections::VecDeque;

use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Vector, WIPOffset};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use ulid::Ulid;

use crate::db_state;
use crate::db_state::{CoreDbState, SsTableHandle};

#[path = "./generated/manifest_generated.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod manifest_generated;
pub use manifest_generated::{
    BlockMeta, BlockMetaArgs, ManifestV1, ManifestV1Args, SsTableIndex, SsTableIndexArgs,
    SsTableInfo, SsTableInfoArgs,
};

use crate::config::CompressionCodec;
use crate::db_state::SsTableId;
use crate::db_state::SsTableId::Compacted;
use crate::error::SlateDBError;
use crate::flatbuffer_types::manifest_generated::{
    CompactedSsTable, CompactedSsTableArgs, CompactedSstId, CompactedSstIdArgs, CompressionFormat,
    SortedRun, SortedRunArgs,
};
use crate::manifest::{Manifest, ManifestCodec};

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct SsTableInfoOwned {
    data: Bytes,
}

impl Serialize for SsTableInfoOwned {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("SsTableInfoOwned", 2)?;
        s.serialize_field("data_length", &self.data.len())?;
        s.end()
    }
}

impl SsTableInfoOwned {
    pub fn new(data: Bytes) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<SsTableInfo>(&data)?;
        Ok(Self { data })
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn borrow(&self) -> SsTableInfo<'_> {
        let raw = &self.data;
        // This is safe, because we validated the flatbuffer on construction and the
        // memory is immutable once we construct the handle.
        unsafe { flatbuffers::root_unchecked::<SsTableInfo>(raw) }
    }

    pub fn create_copy(sst_info: &SsTableInfo) -> Self {
        let builder = flatbuffers::FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        Self {
            data: db_fb_builder.create_sst_info_copy(sst_info),
        }
    }
}

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
}

pub(crate) struct FlatBufferManifestCodec {}

impl ManifestCodec for FlatBufferManifestCodec {
    fn encode(&self, manifest: &Manifest) -> Bytes {
        self.create_from_manifest(manifest)
    }

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError> {
        let manifest = flatbuffers::root::<ManifestV1>(bytes)?;
        Ok(self.manifest(&manifest))
    }
}

impl FlatBufferManifestCodec {
    pub fn manifest(&self, manifest: &ManifestV1) -> Manifest {
        let l0_last_compacted = manifest
            .l0_last_compacted()
            .map(|id| Ulid::from((id.high(), id.low())));
        let mut l0 = VecDeque::new();
        for man_sst in manifest.l0().iter() {
            let man_sst_id = man_sst.id();
            let sst_id = Compacted(Ulid::from((man_sst_id.high(), man_sst_id.low())));
            let sst_info = SsTableInfoOwned::create_copy(&man_sst.info());
            let l0_sst = SsTableHandle::new(sst_id, sst_info);
            l0.push_back(l0_sst);
        }
        let mut compacted = Vec::new();
        for manifest_sr in manifest.compacted().iter() {
            let mut ssts = Vec::new();
            for manifest_sst in manifest_sr.ssts().iter() {
                let id = Compacted(manifest_sst.id().ulid());
                let info = SsTableInfoOwned::create_copy(&manifest_sst.info());
                ssts.push(SsTableHandle::new(id, info));
            }
            compacted.push(db_state::SortedRun {
                id: manifest_sr.id(),
                ssts,
            })
        }
        let core = CoreDbState {
            l0_last_compacted,
            l0,
            compacted,
            next_wal_sst_id: manifest.wal_id_last_seen() + 1,
            last_compacted_wal_sst_id: manifest.wal_id_last_compacted(),
        };
        Manifest {
            core,
            writer_epoch: manifest.writer_epoch(),
            compactor_epoch: manifest.compactor_epoch(),
        }
    }

    pub fn create_from_manifest(&self, manifest: &Manifest) -> Bytes {
        let builder = FlatBufferBuilder::new();
        let mut db_fb_builder = DbFlatBufferBuilder::new(builder);
        db_fb_builder.create_manifest(manifest)
    }
}

impl<'b> CompactedSstId<'b> {
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

    fn add_sst_info_copy(&mut self, info: &SsTableInfo) -> WIPOffset<SsTableInfo<'b>> {
        let first_key = match info.first_key() {
            None => None,
            Some(first_key_vector) => Some(self.builder.create_vector(first_key_vector.bytes())),
        };
        SsTableInfo::create(
            &mut self.builder,
            &SsTableInfoArgs {
                first_key,
                index_offset: info.index_offset(),
                index_len: info.index_len(),
                filter_offset: info.filter_offset(),
                filter_len: info.filter_len(),
                compression_format: info.compression_format(),
            },
        )
    }

    fn add_compacted_sst_id(&mut self, ulid: &Ulid) -> WIPOffset<CompactedSstId<'b>> {
        let uidu128 = ulid.0;
        let high = (uidu128 >> 64) as u64;
        let low = ((uidu128 << 64) >> 64) as u64;
        CompactedSstId::create(&mut self.builder, &CompactedSstIdArgs { high, low })
    }

    #[allow(clippy::panic)]
    fn add_compacted_sst(
        &mut self,
        id: &SsTableId,
        info: &SsTableInfoOwned,
    ) -> WIPOffset<CompactedSsTable<'b>> {
        let ulid = match id {
            SsTableId::Wal(_) => {
                panic!("cannot pass WAL SST handle to create compacted sst")
            }
            SsTableId::Compacted(ulid) => *ulid,
        };
        let compacted_sst_id = self.add_compacted_sst_id(&ulid);
        let compacted_sst_info = self.add_sst_info_copy(&info.borrow());
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

    fn create_manifest(&mut self, manifest: &Manifest) -> Bytes {
        let core = &manifest.core;
        let l0 = self.add_compacted_ssts(core.l0.iter());
        let mut l0_last_compacted = None;
        if let Some(ulid) = core.l0_last_compacted.as_ref() {
            l0_last_compacted = Some(self.add_compacted_sst_id(ulid))
        }
        let compacted = self.add_sorted_runs(&core.compacted);
        let manifest = ManifestV1::create(
            &mut self.builder,
            &ManifestV1Args {
                manifest_id: 0, // todo: get rid of me
                writer_epoch: manifest.writer_epoch,
                compactor_epoch: manifest.compactor_epoch,
                wal_id_last_compacted: core.last_compacted_wal_sst_id,
                wal_id_last_seen: core.next_wal_sst_id - 1,
                l0_last_compacted,
                l0: Some(l0),
                compacted: Some(compacted),
                snapshots: None,
            },
        );
        self.builder.finish(manifest, None);
        Bytes::copy_from_slice(self.builder.finished_data())
    }

    fn create_sst_info_copy(&mut self, sst_info: &SsTableInfo) -> Bytes {
        let copy = self.add_sst_info_copy(sst_info);
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
