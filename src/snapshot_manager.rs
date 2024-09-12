use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use crate::{
    db_state::CoreDbState, error::SlateDBError, manifest::Manifest, manifest_store::ManifestStore,
};

pub struct SnapshotManager {
    snapshots: RwLock<HashMap<u64, Snapshot>>,
    next_snapshot_id: AtomicU64,
    manifest_store: Arc<ManifestStore>,
}

#[derive(Copy, Clone)]
pub struct Snapshot {
    id: u64,
    pub(crate) manifest_id: u64,
}

impl SnapshotManager {
    pub fn new(manifest_store: Arc<ManifestStore>) -> Self {
        Self {
            snapshots: RwLock::new(HashMap::new()),
            next_snapshot_id: AtomicU64::new(0),
            manifest_store,
        }
    }

    pub async fn create_snapshot(&self, core: CoreDbState) -> Result<Snapshot, SlateDBError> {
        let next_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(mut m) = self.manifest_store.read_latest_manifest().await? {
            let snapshot = Snapshot {
                id: next_id,
                manifest_id: m.0,
            };
            self.snapshots.write().insert(next_id, snapshot);
            m.1.increasement_snapshot_reference_count();
            self.manifest_store.write_manifest(m.0, &m.1).await?;
            Ok(snapshot)
        } else {
            let m = &mut Manifest::new(core, 0, 0);
            m.increasement_snapshot_reference_count();
            self.manifest_store.write_manifest(0, m).await?;
            let snapshot = Snapshot {
                id: next_id,
                manifest_id: 0,
            };
            self.snapshots.write().insert(next_id, snapshot);
            Ok(snapshot)
        }
    }

    pub async fn release_snapshot(&self, snapshot_id: u64) -> Result<(), SlateDBError> {
        let mut snapshots = self.snapshots.write();
        if let Some(snapshot) = snapshots.get_mut(&snapshot_id) {
            if let Some(mut m) = self
                .manifest_store
                .read_manifest_with_id(snapshot.manifest_id)
                .await?
            {
                m.decreasement_snapshot_reference_count();
                self.manifest_store
                    .write_manifest(snapshot.manifest_id, &m)
                    .await
                    .unwrap();
            }
            snapshots.remove(&snapshot_id);
        }
        Ok(())
    }
}
