use crate::db_state::CoreDbState;
use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer, Vector, WIPOffset};
use std::collections::VecDeque;

#[path = "./generated/manifest_generated.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod manifest_generated;
use crate::flatbuffer_types::manifest_generated::{
    CompactedSsTable, CompactedSsTableArgs, CompactedSstId, CompactedSstIdArgs,
};
use crate::tablestore::{SSTableHandle, SsTableId};
pub use manifest_generated::{
    BlockMeta, BlockMetaArgs, ManifestV1, ManifestV1Args, SsTableInfo, SsTableInfoArgs,
};

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct SsTableInfoOwned {
    data: Bytes,
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

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ManifestV1Owned {
    data: Bytes,
}

impl ManifestV1Owned {
    pub fn new(data: Bytes) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<ManifestV1>(&data)?;
        Ok(Self { data })
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn borrow(&self) -> ManifestV1<'_> {
        let raw = &self.data;
        // This is safe, because we validated the flatbuffer on construction and the
        // memory is immutable once we construct the handle.
        unsafe { flatbuffers::root_unchecked::<ManifestV1>(raw) }
    }

    pub fn create_new() -> Self {
        let builder = &mut FlatBufferBuilder::new();
        let manifest = ManifestV1::create(
            builder,
            &ManifestV1Args {
                manifest_id: 1,
                writer_epoch: 1,
                compactor_epoch: 0,
                wal_id_last_compacted: 0,
                wal_id_last_seen: 0,
                l0: None,
                compacted: None,
                snapshots: None,
            },
        );
        builder.finish(manifest, None);
        let data = Bytes::copy_from_slice(builder.finished_data());
        Self { data }
    }

    pub fn create_updated_manifest(&self, compacted_db_state: &CoreDbState) -> ManifestV1Owned {
        let old_manifest = self.borrow();
        let builder = flatbuffers::FlatBufferBuilder::new();
        let mut manifest_builder = DbFlatBufferBuilder::new(builder);
        Self {
            data: manifest_builder
                .create_manifest_from_compacted_dbstate(old_manifest, compacted_db_state),
        }
    }
}

struct DbFlatBufferBuilder<'b> {
    builder: FlatBufferBuilder<'b>,
}

impl<'b> DbFlatBufferBuilder<'b> {
    fn new(builder: FlatBufferBuilder<'b>) -> Self {
        Self { builder }
    }

    fn add_block_meta_copy(&mut self, block_meta: &BlockMeta) -> WIPOffset<BlockMeta<'b>> {
        let first_key = self.builder.create_vector(block_meta.first_key().bytes());
        BlockMeta::create(
            &mut self.builder,
            &BlockMetaArgs {
                offset: block_meta.offset(),
                first_key: Some(first_key),
            },
        )
    }

    fn add_sst_info_copy(&mut self, info: &SsTableInfo) -> WIPOffset<SsTableInfo<'b>> {
        let first_key = match info.first_key() {
            None => None,
            Some(first_key_vector) => Some(self.builder.create_vector(first_key_vector.bytes())),
        };
        let block_meta_vec: Vec<WIPOffset<BlockMeta>> = info
            .block_meta()
            .iter()
            .map(|block_meta| self.add_block_meta_copy(&block_meta))
            .collect();
        let block_meta = self.builder.create_vector(block_meta_vec.as_ref());
        SsTableInfo::create(
            &mut self.builder,
            &SsTableInfoArgs {
                first_key,
                block_meta: Some(block_meta),
                filter_offset: info.filter_offset(),
                filter_len: info.filter_len(),
            },
        )
    }

    #[allow(clippy::panic)]
    fn add_compacted_sst(
        &mut self,
        id: &SsTableId,
        info: &SsTableInfoOwned,
    ) -> WIPOffset<CompactedSsTable<'b>> {
        let uidu128 = match id {
            SsTableId::Wal(_) => {
                panic!("cannot pass WAL SST handle to create compacted sst")
            }
            SsTableId::Compacted(uid) => uid.0,
        };
        let high = (uidu128 >> 64) as u64;
        let low = ((uidu128 << 64) >> 64) as u64;
        let compacted_sst_id =
            CompactedSstId::create(&mut self.builder, &CompactedSstIdArgs { high, low });
        let compacted_sst_info = self.add_sst_info_copy(&info.borrow());
        CompactedSsTable::create(
            &mut self.builder,
            &CompactedSsTableArgs {
                id: Some(compacted_sst_id),
                info: Some(compacted_sst_info),
            },
        )
    }

    fn add_compacted_ssts(
        &mut self,
        ssts: &VecDeque<SSTableHandle>,
    ) -> WIPOffset<Vector<'b, ForwardsUOffset<CompactedSsTable<'b>>>> {
        let compacted_ssts: Vec<WIPOffset<CompactedSsTable>> = ssts
            .iter()
            .map(|sst| self.add_compacted_sst(&sst.id, &sst.info))
            .collect();
        self.builder.create_vector(compacted_ssts.as_ref())
    }

    fn create_manifest_from_compacted_dbstate(
        &mut self,
        old_manifest: ManifestV1,
        compacted_db_state: &CoreDbState,
    ) -> Bytes {
        let l0 = self.add_compacted_ssts(&compacted_db_state.l0);
        let manifest = ManifestV1::create(
            &mut self.builder,
            &ManifestV1Args {
                manifest_id: old_manifest.manifest_id() + 1,
                writer_epoch: old_manifest.writer_epoch(),
                compactor_epoch: old_manifest.compactor_epoch(),
                wal_id_last_compacted: compacted_db_state.last_compacted_wal_sst_id,
                wal_id_last_seen: compacted_db_state.next_wal_sst_id - 1,
                l0: Some(l0),
                compacted: None,
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
