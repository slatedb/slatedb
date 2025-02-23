use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use bytes::Bytes;
use serde::Serialize;
use uuid::Uuid;

pub(crate) mod store;

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Manifest {
    pub(crate) parent: Option<ParentDb>,
    pub(crate) core: CoreDbState,
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
}

impl Manifest {
    pub(crate) fn initial(core: CoreDbState) -> Self {
        Self {
            parent: None,
            core,
            writer_epoch: 0,
            compactor_epoch: 0,
        }
    }

    /// Create an initial manifest for a new clone. The returned
    /// manifest will set `initialized=false` to allow for additional
    /// initialization (such as copying wals).
    pub(crate) fn cloned(parent_db: ParentDb, parent_manifest: &Manifest) -> Self {
        let clone_core = parent_manifest.core.init_clone_db();
        Self {
            parent: Some(parent_db),
            core: clone_core,
            writer_epoch: parent_manifest.writer_epoch,
            compactor_epoch: parent_manifest.compactor_epoch,
        }
    }
}

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct ParentDb {
    pub(crate) path: String,
    pub(crate) checkpoint_id: Uuid,
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

#[cfg(test)]
mod tests {
    use crate::manifest::store::{ManifestStore, StoredManifest};

    use crate::config::CheckpointOptions;
    use crate::db_state::CoreDbState;
    use crate::manifest::ParentDb;
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
            StoredManifest::create_new_db(Arc::clone(&parent_manifest_store), CoreDbState::new())
                .await
                .unwrap();
        let checkpoint = parent_manifest
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();

        let parent_link = ParentDb {
            path: parent_path.to_string(),
            checkpoint_id: checkpoint.id,
        };
        let clone_path = Path::from("/tmp/test_clone");
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let clone_stored_manifest = StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_link.clone(),
            parent_manifest.manifest(),
        )
        .await
        .unwrap();

        let clone_manifest = clone_stored_manifest.manifest();
        assert_eq!(Some(parent_link), clone_manifest.parent);
        assert!(!clone_manifest.core.initialized);
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
