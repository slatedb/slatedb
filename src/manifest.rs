use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use bytes::Bytes;
use serde::Serialize;

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Manifest {
    pub(crate) core: CoreDbState,
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
}

pub(crate) trait ManifestCodec: Send + Sync {
    fn encode(&self, manifest: &Manifest) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError>;
}

impl Manifest {
    pub(crate) fn has_wal_sst_reference(&self, wal_sst_id: u64) -> bool {
        wal_sst_id > self.core.last_compacted_wal_sst_id && wal_sst_id < self.core.next_wal_sst_id
    }
}
