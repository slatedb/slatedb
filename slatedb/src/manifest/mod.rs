use std::collections::HashSet;

use crate::db_state::{CoreDbState, SsTableId};
use crate::error::SlateDBError;
use bytes::Bytes;
use serde::Serialize;
use uuid::Uuid;

pub(crate) mod store;

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Manifest {
    pub(crate) external_dbs: Vec<ExternalDb>,
    pub(crate) core: CoreDbState,
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
}

impl Manifest {
    pub(crate) fn initial(core: CoreDbState) -> Self {
        Self {
            external_dbs: vec![],
            core,
            writer_epoch: 0,
            compactor_epoch: 0,
        }
    }

    /// Create an initial manifest for a new clone. The returned
    /// manifest will set `initialized=false` to allow for additional
    /// initialization (such as copying wals).
    pub(crate) fn cloned(
        parent_manifest: &Manifest,
        parent_path: String,
        source_checkpoint_id: Uuid,
    ) -> Self {
        let mut parent_external_sst_ids = HashSet::<SsTableId>::new();
        let mut clone_external_dbs = vec![];

        for parent_external_db in &parent_manifest.external_dbs {
            parent_external_sst_ids.extend(&parent_external_db.sst_ids);
            clone_external_dbs.push(ExternalDb {
                path: parent_external_db.path.clone(),
                source_checkpoint_id: parent_external_db.source_checkpoint_id,
                final_checkpoint_id: Some(Uuid::new_v4()),
                sst_ids: parent_external_db.sst_ids.clone(),
            });
        }

        let parent_owned_sst_ids = parent_manifest
            .core
            .compacted
            .iter()
            .flat_map(|sr| sr.ssts.iter().map(|s| s.id))
            .chain(parent_manifest.core.l0.iter().map(|s| s.id))
            .filter(|id| !parent_external_sst_ids.contains(id))
            .collect();

        clone_external_dbs.push(ExternalDb {
            path: parent_path,
            source_checkpoint_id,
            final_checkpoint_id: Some(Uuid::new_v4()),
            sst_ids: parent_owned_sst_ids,
        });

        Self {
            external_dbs: clone_external_dbs,
            core: parent_manifest.core.init_clone_db(),
            writer_epoch: parent_manifest.writer_epoch,
            compactor_epoch: parent_manifest.compactor_epoch,
        }
    }
}

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct ExternalDb {
    pub(crate) path: String,
    pub(crate) source_checkpoint_id: Uuid,
    pub(crate) final_checkpoint_id: Option<Uuid>,
    pub(crate) sst_ids: Vec<SsTableId>,
}

pub(crate) trait ManifestCodec: Send + Sync {
    fn encode(&self, manifest: &Manifest) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError>;
}

impl Manifest {
    pub(crate) fn has_wal_sst_reference(&self, wal_sst_id: u64) -> bool {
        wal_sst_id > self.core.replay_after_wal_id && wal_sst_id < self.core.next_wal_sst_id
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::store::{ManifestStore, StoredManifest};

    use crate::config::CheckpointOptions;
    use crate::db_state::CoreDbState;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_init_clone_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parent_path = Path::from("/tmp/test_parent");
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_manifest =
            StoredManifest::create_new_db(parent_manifest_store, CoreDbState::new())
                .await
                .unwrap();
        let checkpoint = parent_manifest
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();

        let clone_path = Path::from("/tmp/test_clone");
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let clone_stored_manifest = StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_manifest.manifest(),
            parent_path.to_string(),
            checkpoint.id,
        )
        .await
        .unwrap();

        let clone_manifest = clone_stored_manifest.manifest();

        // There should be single external db, since parent is not deeply nested.
        assert_eq!(clone_manifest.external_dbs.len(), 1);
        assert_eq!(clone_manifest.external_dbs[0].path, parent_path.to_string());
        assert_eq!(
            clone_manifest.external_dbs[0].source_checkpoint_id,
            checkpoint.id
        );
        assert!(clone_manifest.external_dbs[0].final_checkpoint_id.is_some());

        // The clone manifest should not be initialized
        assert!(!clone_manifest.core.initialized);

        // Check epoch has been carried over
        assert_eq!(
            parent_manifest.manifest().writer_epoch,
            clone_manifest.writer_epoch
        );
        assert_eq!(
            parent_manifest.manifest().compactor_epoch,
            clone_manifest.compactor_epoch
        );
    }

    #[tokio::test]
    async fn test_write_new_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let path = Path::from("/tmp/test_db");
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut manifest =
            StoredManifest::create_new_db(Arc::clone(&manifest_store), CoreDbState::new())
                .await
                .unwrap();

        let checkpoint = manifest
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();

        let latest_manifest_id = manifest_store.read_latest_manifest().await.unwrap().0;
        assert_eq!(latest_manifest_id, checkpoint.manifest_id);
        assert_eq!(None, checkpoint.expire_time);
    }
}
