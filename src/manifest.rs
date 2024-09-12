use std::sync::{atomic::AtomicU64, Arc};

use bytes::Bytes;

use crate::db_state::CoreDbState;
use crate::error::SlateDBError;

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) core: CoreDbState,
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
    snapshot_reference_count: Arc<AtomicU64>,
}

impl Manifest {
    pub fn new(core: CoreDbState, writer_epoch: u64, compactor_epoch: u64) -> Self {
        Self {
            core,
            writer_epoch,
            compactor_epoch,
            snapshot_reference_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn increasement_snapshot_reference_count(&mut self) {
        self.snapshot_reference_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn decreasement_snapshot_reference_count(&mut self) {
        self.snapshot_reference_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub(crate) trait ManifestCodec: Send + Sync {
    fn encode(&self, manifest: &Manifest) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError>;
}
