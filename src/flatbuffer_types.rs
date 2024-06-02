use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;

#[path = "./generated/manifest_generated.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod manifest_generated;
pub use manifest_generated::{BlockMeta, BlockMetaArgs, SsTableInfo, SsTableInfoArgs, Manifest, ManifestArgs};


#[derive(Clone, PartialEq, Debug)]
pub(crate) struct OwnedSsTableInfo {
    data: Bytes,
}

impl OwnedSsTableInfo {
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
}

#[derive(Clone, PartialEq, Debug)]
pub (crate) struct ManifestOwned {
    data: Bytes,
}

impl ManifestOwned {
    pub fn new(data: Bytes) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<Manifest>(&data)?;
        Ok(Self { data })
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn borrow(&self) -> Manifest<'_> {
        let raw = &self.data;
        // This is safe, because we validated the flatbuffer on construction and the
        // memory is immutable once we construct the handle.
        unsafe { flatbuffers::root_unchecked::<Manifest>(raw) }
    }

    pub fn create_new() -> Self {
        let builder = &mut flatbuffers::FlatBufferBuilder::new();
        let manifest = Manifest::create(builder, &ManifestArgs {
            manifest_format_version: 1,
            manifest_id: 1,
            writer_epoch: 1,
            compactor_epoch: 0,
            wal_id_last_compacted: 0,
            wal_id_last_seen: 0,
            leveled_ssts: None,
            snapshots: None,
        });
        builder.finish(manifest, None);
        let data = Bytes::copy_from_slice(builder.finished_data());
        Self { data }
    }
}
