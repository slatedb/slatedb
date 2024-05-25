use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;

#[path = "./generated/sst_generated.rs"]
mod sst_generated;

pub use sst_generated::{BlockMeta, BlockMetaArgs, SsTableInfo, SsTableInfoArgs};

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