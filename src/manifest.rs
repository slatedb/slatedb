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
