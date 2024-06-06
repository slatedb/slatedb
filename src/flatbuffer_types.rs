use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;

#[path = "./generated/manifest_generated.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod manifest_generated;
pub use manifest_generated::{
    BlockMeta, BlockMetaArgs, Manifest, ManifestArgs, SsTableInfo, SsTableInfoArgs,
};

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
pub(crate) struct ManifestOwned {
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
        let manifest = Manifest::create(
            builder,
            &ManifestArgs {
                manifest_format_version: 1,
                manifest_id: 1,
                writer_epoch: 1,
                compactor_epoch: 0,
                wal_id_last_compacted: 0,
                wal_id_last_seen: 0,
                leveled_ssts: None,
                snapshots: None,
            },
        );
        builder.finish(manifest, None);
        let data = Bytes::copy_from_slice(builder.finished_data());
        Self { data }
    }

    pub fn update_wal_id_last_seen(&self, new_wal_id_last_seen: u64) -> ManifestOwned {
        let old_manifest = self.borrow();

        // TODO:- Update method to also copy rest of the fields from current manifest.
        // There doesn't seem to be a way to mutate flatbuffers. Every update requires constructing a new flatbuffer.
        // Update this after initial PR discussions.
        let builder = &mut flatbuffers::FlatBufferBuilder::new();
        let manifest = Manifest::create(
            builder,
            &ManifestArgs {
                manifest_format_version: old_manifest.manifest_format_version(),
                manifest_id: old_manifest.manifest_id() + 1,
                writer_epoch: old_manifest.writer_epoch(),
                compactor_epoch: old_manifest.compactor_epoch(),
                wal_id_last_compacted: old_manifest.wal_id_last_compacted(),
                wal_id_last_seen: new_wal_id_last_seen,
                leveled_ssts: None,
                snapshots: None,
            },
        );

        builder.finish(manifest, None);
        let data = Bytes::copy_from_slice(builder.finished_data());
        Self { data }
    }
}
