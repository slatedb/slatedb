use crate::checkpoint::Checkpoint;
use crate::config::CheckpointOptions;
use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use crate::error::SlateDBError::{
    CheckpointMissing, InvalidDBState, LatestManifestMissing, ManifestMissing,
};
use crate::flatbuffer_types::FlatBufferManifestCodec;
use crate::manifest::{Manifest, ManifestCodec, ParentDb};
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
use chrono::Utc;
use futures::StreamExt;
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::{Error, ObjectStore};
use serde::Serialize;
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::warn;
use uuid::Uuid;

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

    pub(crate) fn new_checkpoint(
        &mut self,
        checkpoint_id: Uuid,
        options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        self.stored_manifest.new_checkpoint(checkpoint_id, options)
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
    async fn init(store: Arc<ManifestStore>, manifest: Manifest) -> Result<Self, SlateDBError> {
        store.write_manifest(1, &manifest).await?;
        Ok(Self {
            id: 1,
            manifest,
            manifest_store: store,
        })
    }

    /// Create the initial manifest for a new database.
    pub(crate) async fn create_new_db(
        store: Arc<ManifestStore>,
        core: CoreDbState,
    ) -> Result<Self, SlateDBError> {
        let manifest = Manifest::initial(core);
        Self::init(store, manifest).await
    }

    /// Create a new manifest for a new cloned database. The initial manifest
    /// will be written with the `initialized` field set to false in order to allow
    /// for the rest of the clone state to be initialized
    pub(crate) async fn create_uninitialized_clone(
        clone_manifest_store: Arc<ManifestStore>,
        parent_db: ParentDb,
        parent_manifest: &Manifest,
    ) -> Result<Self, SlateDBError> {
        let manifest = Manifest::cloned(parent_db, parent_manifest);
        Self::init(clone_manifest_store, manifest).await
    }

    /// Load the current manifest from the supplied manifest store. If there is no db at the
    /// manifest store's path then this fn returns None. Otherwise, on success it returns a
    /// Result with an instance of StoredManifest.
    pub(crate) async fn try_load(store: Arc<ManifestStore>) -> Result<Option<Self>, SlateDBError> {
        let Some((id, manifest)) = store.try_read_latest_manifest().await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            id,
            manifest,
            manifest_store: store,
        }))
    }

    /// Load the current manifest from the supplied manifest store. If successful,
    /// this method returns a [`Result`] with an instance of [`StoredManifest`].
    /// If no manifests could be found, the error [`LatestManifestMissing`] is returned.
    pub(crate) async fn load(store: Arc<ManifestStore>) -> Result<Self, SlateDBError> {
        Self::try_load(store).await?.ok_or(LatestManifestMissing)
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.manifest.core
    }

    pub(crate) async fn refresh(&mut self) -> Result<&CoreDbState, SlateDBError> {
        let Some((id, manifest)) = self.manifest_store.try_read_latest_manifest().await? else {
            return Err(InvalidDBState);
        };
        self.manifest = manifest;
        self.id = id;
        Ok(&self.manifest.core)
    }

    fn next_id(&self) -> u64 {
        self.id + 1
    }

    /// Create a new checkpoint from the latest manifest state. This only creates
    /// the checkpoint struct, but does not persist it in the manifest.
    pub(crate) fn new_checkpoint(
        &self,
        checkpoint_id: Uuid,
        options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        let expire_time = options.lifetime.map(|l| SystemTime::now() + l);
        let db_state = self.db_state();
        let manifest_id = match options.source {
            Some(source_checkpoint_id) => {
                let Some(source_checkpoint) = db_state.find_checkpoint(&source_checkpoint_id)
                else {
                    return Err(CheckpointMissing(source_checkpoint_id));
                };
                source_checkpoint.manifest_id
            }
            None => {
                if !db_state.initialized {
                    return Err(InvalidDBState);
                }
                self.next_id()
            }
        };
        Ok(Checkpoint {
            id: checkpoint_id,
            manifest_id,
            expire_time,
            create_time: SystemTime::now(),
        })
    }

    pub(crate) async fn write_checkpoint(
        &mut self,
        checkpoint_id: Option<Uuid>,
        options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        let checkpoint_id = checkpoint_id.unwrap_or(Uuid::new_v4());
        self.maybe_apply_db_state_update(|stored_manifest| {
            let checkpoint = stored_manifest.new_checkpoint(checkpoint_id, options)?;
            let mut updated_db_state = stored_manifest.db_state().clone();
            updated_db_state.checkpoints.push(checkpoint);
            Ok(Some(updated_db_state))
        })
        .await?;
        let checkpoint = self
            .db_state()
            .find_checkpoint(&checkpoint_id)
            .expect("update applied but checkpoint not found")
            .clone();
        Ok(checkpoint)
    }

    pub(crate) async fn rewrite_parent_db(
        &mut self,
        parent_db: ParentDb,
    ) -> Result<(), SlateDBError> {
        // Do not allow the parent to be rewritten if the manifest finished initialization.
        if self.manifest.core.initialized {
            return Err(InvalidDBState);
        }

        // Also do not allow the parent path to be changed.
        let Some(current_parent) = self.manifest.parent.as_ref()
        else {
            return Err(InvalidDBState);
        };

        if current_parent.path != parent_db.path {
            return Err(InvalidDBState);
        }

        let mut manifest = self.manifest.clone();
        manifest.parent = Some(parent_db);
        self.update_manifest(manifest).await
    }

    pub(crate) async fn update_db_state(&mut self, core: CoreDbState) -> Result<(), SlateDBError> {
        let manifest = Manifest {
            parent: self.manifest.parent.clone(),
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

    /// Apply an update to a stored manifest repeatedly retrying the update
    /// if the write fails due to a manifest version conflict caused by another client
    /// updating the manifest at the same time. The update to be applied is specified by
    /// the mutator parameter, which is a function that takes a &StoredManifest and returns
    /// an optional [`CoreDbState`]. If the mutator returns `None`, then no update will
    /// be applied.
    pub(crate) async fn maybe_apply_db_state_update<F>(
        &mut self,
        mutator: F,
    ) -> Result<(), SlateDBError>
    where
        F: Fn(&StoredManifest) -> Result<Option<CoreDbState>, SlateDBError>,
    {
        loop {
            let Some(mutated_db_state) = mutator(self)? else {
                return Ok(());
            };

            return match self.update_db_state(mutated_db_state).await {
                Err(SlateDBError::ManifestVersionExists) => {
                    self.refresh().await?;
                    continue;
                }
                Err(e) => Err(e),
                Ok(()) => Ok(()),
            };
        }
    }
}

/// Represents the metadata of a manifest file stored in the object store.
#[derive(Serialize, Debug)]
pub(crate) struct ManifestFileMetadata {
    pub(crate) id: u64,
    #[serde(serialize_with = "serialize_path")]
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: usize,
}

fn serialize_path<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(path.as_ref())
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
                    SlateDBError::from(err)
                }
            })?;

        Ok(())
    }

    /// Delete a manifest from the object store.
    pub(crate) async fn delete_manifest(&self, id: u64) -> Result<(), SlateDBError> {
        let (active_id, manifest) = self.read_latest_manifest().await?;
        if active_id == id {
            return Err(SlateDBError::InvalidDeletion);
        }

        if manifest
            .core
            .checkpoints
            .iter()
            .any(|ck| ck.manifest_id == id)
        {
            return Err(SlateDBError::InvalidDeletion);
        }

        let manifest_path = &self.get_manifest_path(id);
        self.object_store.delete(manifest_path).await?;
        Ok(())
    }

    /// Read a manifest from the object store. The last element in an unbounded
    /// range is the current manifest.
    /// # Arguments
    /// * `id_range` - The range of IDs to list
    pub(crate) async fn list_manifests<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<ManifestFileMetadata>, SlateDBError> {
        let manifest_path = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(manifest_path));
        let mut manifests = Vec::new();

        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::from(e)),
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
                Err(_) => warn!("Unknown file in manifest directory: {:?}", file.location),
                _ => {}
            }
        }

        manifests.sort_by_key(|m| m.id);
        Ok(manifests)
    }

    /// Active manifests include the latest manifest and all manifests referenced
    /// by checkpoints in the latest manifest.
    pub(crate) async fn read_active_manifests(
        &self,
    ) -> Result<BTreeMap<u64, Manifest>, SlateDBError> {
        let (latest_manifest_id, latest_manifest) = self.read_latest_manifest().await?;

        let mut active_manifests = BTreeMap::new();
        active_manifests.insert(latest_manifest_id, latest_manifest.clone());

        for checkpoint in &latest_manifest.core.checkpoints {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                active_manifests.entry(checkpoint.manifest_id)
            {
                let checkpoint_manifest = self.read_manifest(checkpoint.manifest_id).await?;
                entry.insert(checkpoint_manifest);
            }
        }

        Ok(active_manifests)
    }

    pub(crate) async fn try_read_latest_manifest(
        &self,
    ) -> Result<Option<(u64, Manifest)>, SlateDBError> {
        let manifest_metadatas_list = self.list_manifests(..).await?;
        let latest_manifest = if let Some(metadata) = manifest_metadatas_list.last() {
            let manifest = self.try_read_manifest(metadata.id).await?;
            manifest.map(|manifest| (metadata.id, manifest))
        } else {
            None
        };
        Ok(latest_manifest)
    }

    pub(crate) async fn read_latest_manifest(&self) -> Result<(u64, Manifest), SlateDBError> {
        self.try_read_latest_manifest()
            .await?
            .ok_or(LatestManifestMissing)
    }

    pub(crate) async fn try_read_manifest(
        &self,
        id: u64,
    ) -> Result<Option<Manifest>, SlateDBError> {
        let manifest_path = &self.get_manifest_path(id);
        match self.object_store.get(manifest_path).await {
            Ok(manifest) => match manifest.bytes().await {
                Ok(bytes) => {
                    let manifest = self.codec.decode(&bytes)?;
                    Ok(Some(manifest))
                }
                Err(e) => Err(SlateDBError::from(e)),
            },
            Err(e) => match e {
                Error::NotFound { .. } => Ok(None),
                _ => Err(SlateDBError::from(e)),
            },
        }
    }

    pub(crate) async fn read_manifest(&self, id: u64) -> Result<Manifest, SlateDBError> {
        self.try_read_manifest(id).await?.ok_or(ManifestMissing(id))
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
    use crate::checkpoint::Checkpoint;
    use crate::config::CheckpointOptions;
    use crate::db_state::CoreDbState;
    use crate::error;
    use crate::error::SlateDBError;
    use crate::manifest::{Manifest, ParentDb};
    use crate::manifest_store::{FenceableManifest, ManifestStore, StoredManifest};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use uuid::Uuid;
    use crate::error::SlateDBError::InvalidDBState;

    const ROOT: &str = "/root/path";

    #[tokio::test]
    async fn test_should_fail_write_on_version_conflict() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut sm2 = StoredManifest::load(ms.clone()).await.unwrap();
        sm.update_db_state(state.clone()).await.unwrap();

        let result = sm2.update_db_state(state.clone()).await;

        assert!(matches!(
            result.unwrap_err(),
            error::SlateDBError::ManifestVersionExists
        ));
    }

    #[tokio::test]
    async fn test_should_write_with_new_version() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        sm.update_db_state(state.clone()).await.unwrap();

        let (version, _) = ms.read_latest_manifest().await.unwrap();

        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn test_should_update_local_state_on_write() {
        let ms = new_memory_manifest_store();
        let mut state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        state.next_wal_sst_id = 123;
        sm.update_db_state(state.clone()).await.unwrap();

        assert_eq!(sm.db_state().next_wal_sst_id, 123);
    }

    #[tokio::test]
    async fn test_should_refresh() {
        let ms = new_memory_manifest_store();
        let mut state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut sm2 = StoredManifest::load(ms.clone()).await.unwrap();
        state.next_wal_sst_id = 123;
        sm.update_db_state(state.clone()).await.unwrap();

        let refreshed = sm2.refresh().await.unwrap();

        assert_eq!(refreshed.next_wal_sst_id, 123);
        assert_eq!(sm2.db_state().next_wal_sst_id, 123);
    }

    #[tokio::test]
    async fn test_should_bump_writer_epoch() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        for i in 1..5 {
            let sm = StoredManifest::load(ms.clone()).await.unwrap();
            FenceableManifest::init_writer(sm).await.unwrap();
            let (_, manifest) = ms.read_latest_manifest().await.unwrap();
            assert_eq!(manifest.writer_epoch, i);
        }
    }

    #[tokio::test]
    async fn test_should_fail_on_writer_fenced() {
        let ms = new_memory_manifest_store();
        let mut state = CoreDbState::new();
        let sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut writer1 = FenceableManifest::init_writer(sm).await.unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap();

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
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        for i in 1..5 {
            let sm = StoredManifest::load(ms.clone()).await.unwrap();
            FenceableManifest::init_compactor(sm).await.unwrap();
            let (_, manifest) = ms.read_latest_manifest().await.unwrap();
            assert_eq!(manifest.compactor_epoch, i);
        }
    }

    #[tokio::test]
    async fn test_should_fail_on_compactor_fenced() {
        let ms = new_memory_manifest_store();
        let mut state = CoreDbState::new();
        let sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut compactor1 = FenceableManifest::init_compactor(sm).await.unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap();

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
    async fn test_should_read_specific_manifest() {
        // Given
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()));
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let mut updated_state = state.clone();
        updated_state.checkpoints.push(new_checkpoint(sm.id));
        sm.update_db_state(updated_state).await.unwrap();

        // When
        let manifest = ms.try_read_manifest(2).await.unwrap().unwrap();

        // Then:
        assert_eq!(1, manifest.core.checkpoints.len());
    }

    #[tokio::test]
    async fn test_list_manifests_unbounded() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
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

    #[tokio::test]
    async fn test_delete_manifest() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        sm.update_db_state(state.clone()).await.unwrap();
        let manifests = ms.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);

        ms.delete_manifest(1).await.unwrap();
        let manifests = ms.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].id, 2);
    }

    #[tokio::test]
    async fn test_delete_active_manifest_should_fail() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        sm.update_db_state(state.clone()).await.unwrap();
        let manifests = ms.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);

        let result = ms.delete_manifest(2).await;
        assert!(matches!(result, Err(error::SlateDBError::InvalidDeletion)));
    }

    fn new_memory_manifest_store() -> Arc<ManifestStore> {
        let os = Arc::new(InMemory::new());
        Arc::new(ManifestStore::new(&Path::from(ROOT), os.clone()))
    }

    fn now_rounded_to_nearest_sec() -> SystemTime {
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        SystemTime::UNIX_EPOCH + Duration::from_secs(now_secs)
    }

    fn new_checkpoint(manifest_id: u64) -> Checkpoint {
        Checkpoint {
            id: Uuid::new_v4(),
            manifest_id,
            expire_time: None,
            create_time: now_rounded_to_nearest_sec(),
        }
    }

    #[tokio::test]
    async fn test_read_active_manifests_should_consider_checkpoints() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let initial_manifest = sm.manifest.clone();
        let initial_manifest_id = sm.id;
        let active_manifests = ms.read_active_manifests().await.unwrap();
        assert_eq!(1, active_manifests.len());
        assert_eq!(
            Some(&initial_manifest),
            active_manifests.get(&initial_manifest_id)
        );

        // Add a checkpoint referencing the latest manifest
        let mut updated_state = state.clone();
        updated_state.checkpoints.push(new_checkpoint(sm.id));
        sm.update_db_state(updated_state).await.unwrap();
        let active_manifests = ms.read_active_manifests().await.unwrap();
        assert_eq!(2, active_manifests.len());
        assert_eq!(
            Some(&initial_manifest),
            active_manifests.get(&initial_manifest_id)
        );
        assert_eq!(Some(&sm.manifest), active_manifests.get(&sm.id));

        // Remove the checkpoint and verify that only the latest manifest is active
        let mut updated_state = state.clone();
        updated_state.checkpoints.clear();
        sm.update_db_state(updated_state).await.unwrap();
        let active_manifests = ms.read_active_manifests().await.unwrap();
        assert_eq!(1, active_manifests.len());
        assert_eq!(Some(&sm.manifest), active_manifests.get(&sm.id));
    }

    #[tokio::test]
    async fn test_maybe_apply_state_update() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let initial_id = sm.id;
        sm.maybe_apply_db_state_update(|_| Ok(None)).await.unwrap();
        assert_eq!(initial_id, sm.id);

        sm.maybe_apply_db_state_update(|sm| Ok(Some(sm.db_state().clone())))
            .await
            .unwrap();
        assert_eq!(initial_id + 1, sm.id);
    }

    #[tokio::test]
    async fn test_deletion_of_manifest_with_checkpoint_reference_not_allowed() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let checkpoint1 = sm
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();

        let _ = sm
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();

        assert!(matches!(
            ms.delete_manifest(checkpoint1.manifest_id).await,
            Err(SlateDBError::InvalidDeletion)
        ));
    }

    #[tokio::test]
    async fn should_safely_rewrite_parent_db() {
        let parent_path = "/parent/path";
        let mut sm = create_uninitialized_clone(parent_path).await;

        let rewrite_checkpoint_id = Uuid::new_v4();
        let parent_db = ParentDb {
            path: parent_path.to_string(),
            checkpoint_id: rewrite_checkpoint_id,
        };
        sm.rewrite_parent_db(parent_db).await.unwrap();
        assert_eq!(2, sm.id());
        assert!(!sm.db_state().initialized);
        assert_eq!(
            Some(rewrite_checkpoint_id),
            sm.manifest.parent.map(|p| p.checkpoint_id)
        );
    }

    #[tokio::test]
    async fn should_not_rewrite_parent_for_initialized_clone() {
        let parent_path = "/parent/path";
        let mut sm = create_uninitialized_clone(parent_path).await;

        let mut initialized_core = sm.db_state().clone();
        initialized_core.initialized = true;
        sm.update_db_state(initialized_core).await.unwrap();

        let parent_db = ParentDb {
            path: parent_path.to_string(),
            checkpoint_id: Uuid::new_v4(),
        };
        assert!(matches!(sm.rewrite_parent_db(parent_db).await.unwrap_err(), InvalidDBState));
    }

    #[tokio::test]
    async fn should_not_rewrite_parent_db_with_different_path() {
        let initial_parent_path = "/initial/parent/path";
        let mut sm = create_uninitialized_clone(initial_parent_path).await;

        let mut initialized_core = sm.db_state().clone();
        initialized_core.initialized = true;
        sm.update_db_state(initialized_core).await.unwrap();

        let updated_parent_path = "/updated/parent/path";
        let parent_db = ParentDb {
            path: updated_parent_path.to_string(),
            checkpoint_id: Uuid::new_v4(),
        };
        assert!(matches!(sm.rewrite_parent_db(parent_db).await.unwrap_err(), InvalidDBState));
    }

    #[tokio::test]
    async fn should_not_rewrite_parent_db_for_noncloned_db() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let parent_db = ParentDb {
            path: "/parent/path".to_string(),
            checkpoint_id: Uuid::new_v4(),
        };
        assert!(matches!(sm.rewrite_parent_db(parent_db).await.unwrap_err(), InvalidDBState));
    }

    async fn create_uninitialized_clone(
        parent_path: &str,
    ) -> StoredManifest {
        let parent_manifest = Manifest::initial(CoreDbState::new());
        let parent_db = ParentDb {
            path: parent_path.to_string(),
            checkpoint_id: Uuid::new_v4(),
        };

        let clone_manifest_store = new_memory_manifest_store();
        StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_db,
            &parent_manifest,
        )
            .await
            .unwrap()
    }
}
