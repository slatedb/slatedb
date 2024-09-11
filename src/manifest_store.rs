use std::ops::RangeBounds;
use std::sync::Arc;

use chrono::Utc;
use futures::StreamExt;
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::ObjectStore;
use tracing::warn;

use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use crate::error::SlateDBError::InvalidDBState;
use crate::flatbuffer_types::FlatBufferManifestCodec;
use crate::manifest::{Manifest, ManifestCodec};
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};

pub(crate) struct FenceableManifest {
    stored_manifest: StoredManifest,
    local_epoch: u64,
    stored_epoch: Box<dyn Fn(&Manifest) -> u64 + Send>,
}

// This type wraps StoredManifest, and fences other conflicting writers by incrementing
// the relevant epoch when initialized. It also detects when the current writer has been
// fenced and fails all operations with SlateDBError::Fenced.
impl FenceableManifest {
    pub(crate) async fn init_writer(stored_manifest: StoredManifest) -> Result<Self, SlateDBError> {
        Self::init(stored_manifest, Box::new(|m| m.writer_epoch), |m, e| {
            m.writer_epoch = e
        })
        .await
    }

    pub(crate) async fn init_compactor(
        stored_manifest: StoredManifest,
    ) -> Result<Self, SlateDBError> {
        Self::init(stored_manifest, Box::new(|m| m.compactor_epoch), |m, e| {
            m.compactor_epoch = e
        })
        .await
    }

    async fn init(
        mut stored_manifest: StoredManifest,
        stored_epoch: Box<dyn Fn(&Manifest) -> u64 + Send>,
        set_epoch: impl Fn(&mut Manifest, u64),
    ) -> Result<Self, SlateDBError> {
        let mut manifest = stored_manifest.manifest.clone();
        let local_epoch = stored_epoch(&manifest) + 1;
        set_epoch(&mut manifest, local_epoch);
        stored_manifest.update_manifest(manifest).await?;
        Ok(Self {
            stored_manifest,
            local_epoch,
            stored_epoch,
        })
    }

    pub(crate) fn db_state(&self) -> Result<&CoreDbState, SlateDBError> {
        self.check_epoch()?;
        Ok(self.stored_manifest.db_state())
    }

    pub(crate) async fn refresh(&mut self) -> Result<&CoreDbState, SlateDBError> {
        self.stored_manifest.refresh().await?;
        self.db_state()
    }

    pub(crate) async fn update_db_state(
        &mut self,
        db_state: CoreDbState,
    ) -> Result<(), SlateDBError> {
        self.check_epoch()?;
        self.stored_manifest.update_db_state(db_state).await
    }

    #[allow(clippy::panic)]
    fn check_epoch(&self) -> Result<(), SlateDBError> {
        let stored_epoch = (self.stored_epoch)(&self.stored_manifest.manifest);
        if self.local_epoch < stored_epoch {
            return Err(SlateDBError::Fenced);
        }
        if self.local_epoch > stored_epoch {
            panic!("the stored epoch is lower than the local epoch")
        }
        Ok(())
    }
}

// Represents the manifest stored in the object store. This type tracks the current
// contents and id of the stored manifest, and allows callers to read the db state
// stored therein. Callers can also use this type to update the db state stored in the
// manifest. The update is done with the next consecutive id, and is conditional on
// no other writer having made an update to the manifest using that id. Finally, callers
// can use the `refresh` method to refresh the locally stored manifest+id with the latest
// manifest stored in the object store.
pub(crate) struct StoredManifest {
    id: u64,
    manifest: Manifest,
    manifest_store: Arc<ManifestStore>,
}

impl StoredManifest {
    pub(crate) async fn init_new_db(
        store: Arc<ManifestStore>,
        core: CoreDbState,
    ) -> Result<Self, SlateDBError> {
        let manifest = Manifest {
            core,
            writer_epoch: 0,
            compactor_epoch: 0,
        };
        store.write_manifest(1, &manifest).await?;
        Ok(Self {
            id: 1,
            manifest,
            manifest_store: store,
        })
    }

    pub(crate) async fn load(store: Arc<ManifestStore>) -> Result<Option<Self>, SlateDBError> {
        let Some((id, manifest)) = store.read_latest_manifest().await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            id,
            manifest,
            manifest_store: store,
        }))
    }

    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.manifest.core
    }

    pub(crate) async fn refresh(&mut self) -> Result<&CoreDbState, SlateDBError> {
        let Some((id, manifest)) = self.manifest_store.read_latest_manifest().await? else {
            return Err(InvalidDBState);
        };
        self.manifest = manifest;
        self.id = id;
        Ok(&self.manifest.core)
    }

    pub(crate) async fn update_db_state(&mut self, core: CoreDbState) -> Result<(), SlateDBError> {
        let manifest = Manifest {
            core,
            writer_epoch: self.manifest.writer_epoch,
            compactor_epoch: self.manifest.compactor_epoch,
        };
        self.update_manifest(manifest).await
    }

    async fn update_manifest(&mut self, manifest: Manifest) -> Result<(), SlateDBError> {
        let new_id = self.id + 1;
        self.manifest_store
            .write_manifest(new_id, &manifest)
            .await?;
        self.manifest = manifest;
        self.id = new_id;
        Ok(())
    }
}

pub(crate) struct ManifestFileMetadata {
    pub(crate) id: u64,
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: usize,
}

pub(crate) struct ManifestStore {
    object_store: Box<dyn TransactionalObjectStore>,
    codec: Box<dyn ManifestCodec>,
    manifest_suffix: &'static str,
}

impl ManifestStore {
    pub(crate) fn new(root_path: &Path, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store: Box::new(DelegatingTransactionalObjectStore::new(
                root_path.child("manifest"),
                object_store,
            )),
            codec: Box::new(FlatBufferManifestCodec {}),
            manifest_suffix: "manifest",
        }
    }

    pub(crate) async fn write_manifest(
        &self,
        id: u64,
        manifest: &Manifest,
    ) -> Result<(), SlateDBError> {
        let manifest_path = &self.get_manifest_path(id);

        self.object_store
            .put_if_not_exists(manifest_path, self.codec.encode(manifest))
            .await
            .map_err(|err| {
                if let AlreadyExists { path: _, source: _ } = err {
                    SlateDBError::ManifestVersionExists
                } else {
                    SlateDBError::ObjectStoreError(err)
                }
            })?;

        Ok(())
    }

    // TODO add tests
    /// Delete a manifest from the object store.
    pub(crate) async fn delete_manifest(&self, id: u64) -> Result<(), SlateDBError> {
        // TODO add safety checks to prevent deletions of active manifests
        let manifest_path = &self.get_manifest_path(id);
        self.object_store.delete(manifest_path).await?;
        Ok(())
    }

    pub(crate) async fn list_manifests<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<ManifestFileMetadata>, SlateDBError> {
        let manifest_path = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(manifest_path));
        let mut manifests = Vec::new();

        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
        } {
            match self.parse_id(&file.location, "manifest") {
                Ok(id) if id_range.contains(&id) => {
                    manifests.push(ManifestFileMetadata {
                        id,
                        location: file.location,
                        last_modified: file.last_modified,
                        size: file.size,
                    });
                }
                Err(_) => warn!("Unknwon file in manifest directory: {:?}", file.location),
                _ => {}
            }
        }

        manifests.sort_by_key(|m| m.id);
        Ok(manifests)
    }

    pub(crate) async fn read_latest_manifest(
        &self,
    ) -> Result<Option<(u64, Manifest)>, SlateDBError> {
        let manifest_metadatas_list = self.list_manifests(..).await?;

        match manifest_metadatas_list.last() {
            Some(metadata) => {
                let manifest_bytes = match self.object_store.get(&metadata.location).await {
                    Ok(manifest) => match manifest.bytes().await {
                        Ok(bytes) => bytes,
                        Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
                    },
                    Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
                };

                self.codec
                    .decode(&manifest_bytes)
                    .map(|m| Some((metadata.id, m)))
            }
            None => Ok(None),
        }
    }

    fn parse_id(&self, path: &Path, expected_extension: &str) -> Result<u64, SlateDBError> {
        match path.extension() {
            Some(ext) if ext == expected_extension => path
                .filename()
                .expect("invalid filename")
                .split('.')
                .next()
                .ok_or_else(|| InvalidDBState)?
                .parse()
                .map_err(|_| InvalidDBState),
            _ => Err(InvalidDBState),
        }
    }

    fn get_manifest_path(&self, id: u64) -> Path {
        Path::from(format!("{:020}.{}", id, self.manifest_suffix))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::path::Path;

    use crate::db_state::CoreDbState;
    use crate::error;
    use crate::manifest_store::{FenceableManifest, ManifestStore, StoredManifest};

    const ROOT: &str = "/root/path";

    #[tokio::test]
    async fn test_should_fail_write_on_version_conflict() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let state = CoreDbState::new();
        let mut sm = StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut sm2 = StoredManifest::load(ms.clone()).await.unwrap().unwrap();
        sm.update_db_state(state.clone()).await.unwrap();

        let result = sm2.update_db_state(state.clone()).await;

        assert!(matches!(
            result.unwrap_err(),
            error::SlateDBError::ManifestVersionExists
        ));
    }

    #[tokio::test]
    async fn test_should_write_with_new_version() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let state = CoreDbState::new();
        let mut sm = StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        sm.update_db_state(state.clone()).await.unwrap();

        let (version, _) = ms.read_latest_manifest().await.unwrap().unwrap();

        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn test_should_update_local_state_on_write() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let mut state = CoreDbState::new();
        let mut sm = StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        state.next_wal_sst_id = 123;
        sm.update_db_state(state.clone()).await.unwrap();

        assert_eq!(sm.db_state().next_wal_sst_id, 123);
    }

    #[tokio::test]
    async fn test_should_refresh() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let mut state = CoreDbState::new();
        let mut sm = StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut sm2 = StoredManifest::load(ms.clone()).await.unwrap().unwrap();
        state.next_wal_sst_id = 123;
        sm.update_db_state(state.clone()).await.unwrap();

        let refreshed = sm2.refresh().await.unwrap();

        assert_eq!(refreshed.next_wal_sst_id, 123);
        assert_eq!(sm2.db_state().next_wal_sst_id, 123);
    }

    #[tokio::test]
    async fn test_should_bump_writer_epoch() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let state = CoreDbState::new();
        StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        for i in 1..5 {
            let sm = StoredManifest::load(ms.clone()).await.unwrap().unwrap();
            FenceableManifest::init_writer(sm).await.unwrap();
            let (_, manifest) = ms.read_latest_manifest().await.unwrap().unwrap();
            assert_eq!(manifest.writer_epoch, i);
        }
    }

    #[tokio::test]
    async fn test_should_fail_on_writer_fenced() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let mut state = CoreDbState::new();
        let sm = StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut writer1 = FenceableManifest::init_writer(sm).await.unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap().unwrap();

        let mut writer2 = FenceableManifest::init_writer(sm2).await.unwrap();

        let result = writer1.refresh().await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
        state.next_wal_sst_id = 123;
        let result = writer1.update_db_state(state.clone()).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
        let refreshed = writer2.refresh().await.unwrap();
        assert_eq!(refreshed.next_wal_sst_id, 1);
    }

    #[tokio::test]
    async fn test_should_bump_compactor_epoch() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let state = CoreDbState::new();
        StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        for i in 1..5 {
            let sm = StoredManifest::load(ms.clone()).await.unwrap().unwrap();
            FenceableManifest::init_compactor(sm).await.unwrap();
            let (_, manifest) = ms.read_latest_manifest().await.unwrap().unwrap();
            assert_eq!(manifest.compactor_epoch, i);
        }
    }

    #[tokio::test]
    async fn test_should_fail_on_compactor_fenced() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let mut state = CoreDbState::new();
        let sm = StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut compactor1 = FenceableManifest::init_compactor(sm).await.unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap().unwrap();

        let mut compactor2 = FenceableManifest::init_compactor(sm2).await.unwrap();

        let result = compactor1.refresh().await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
        state.next_wal_sst_id = 123;
        let result = compactor1.update_db_state(state.clone()).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
        let refreshed = compactor2.refresh().await.unwrap();
        assert_eq!(refreshed.next_wal_sst_id, 1);
    }

    #[tokio::test]
    async fn test_list_manifests_unbounded() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let state = CoreDbState::new();
        let mut sm = StoredManifest::init_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        sm.update_db_state(state.clone()).await.unwrap();

        // Check unbounded
        let manifests = ms.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);

        // Check bounded
        let manifests = ms.list_manifests(1..2).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 1);

        // Check left bounded
        let manifests = ms.list_manifests(2..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 2);

        // Check right bounded
        let manifests = ms.list_manifests(..2).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 1);
    }
}
