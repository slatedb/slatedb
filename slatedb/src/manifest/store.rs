use crate::checkpoint::Checkpoint;
use crate::clock::SystemClock;
use crate::config::CheckpointOptions;
use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use crate::error::SlateDBError::ManifestVersionExists;
use crate::error::SlateDBError::{
    CheckpointMissing, InvalidDBState, LatestManifestMissing, ManifestMissing,
};
use crate::flatbuffer_types::FlatBufferManifestCodec;
use crate::manifest::{ExternalDb, Manifest, ManifestCodec};
use crate::rand::DbRand;
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
use crate::utils;
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use log::debug;
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::{Error, ObjectStore};
use serde::Serialize;
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// Generic codec to serialize/deserialize versioned records stored as files
pub(crate) trait RecordCodec<T>: Send + Sync {
    fn encode(&self, value: &T) -> Bytes;
    fn decode(&self, bytes: &[u8]) -> Result<T, SlateDBError>;
}

// Adapter: use the existing FlatBufferManifestCodec as a RecordCodec for Manifest
impl RecordCodec<Manifest> for FlatBufferManifestCodec {
    fn encode(&self, value: &Manifest) -> Bytes {
        // Delegate to ManifestCodec impl
        ManifestCodec::encode(self, value)
    }

    fn decode(&self, bytes: &[u8]) -> Result<Manifest, SlateDBError> {
        // Existing codec expects &Bytes; wrap without changing behavior
        let b = Bytes::copy_from_slice(bytes);
        ManifestCodec::decode(self, &b)
    }
}

// Generic, versioned store for records persisted as numbered files under a directory
pub(crate) struct RecordStore<T> {
    object_store: Box<dyn TransactionalObjectStore>,
    codec: Box<dyn RecordCodec<T>>,
    file_suffix: &'static str,
    clock: Arc<dyn SystemClock>,
}

impl<T> RecordStore<T> {
    pub(crate) fn new(
        root_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn SystemClock>,
        subdir: &str,
        file_suffix: &'static str,
        codec: Box<dyn RecordCodec<T>>,
    ) -> Self {
        Self {
            object_store: Box::new(DelegatingTransactionalObjectStore::new(
                root_path.child(subdir),
                object_store,
            )),
            codec,
            file_suffix,
            clock,
        }
    }

    fn path_for(&self, id: u64) -> Path {
        Path::from(format!("{:020}.{}", id, self.file_suffix))
    }

    fn parse_id(&self, path: &Path) -> Result<u64, SlateDBError> {
        match path.extension() {
            Some(ext) if ext == self.file_suffix => path
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

    pub(crate) async fn write(&self, id: u64, value: &T) -> Result<(), SlateDBError> {
        let path = self.path_for(id);
        self.object_store
            .put_if_not_exists(&path, self.codec.encode(value))
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

    pub(crate) async fn try_read(&self, id: u64) -> Result<Option<T>, SlateDBError> {
        let path = self.path_for(id);
        match self.object_store.get(&path).await {
            Ok(obj) => match obj.bytes().await {
                Ok(bytes) => self.codec.decode(bytes.as_ref()).map(Some),
                Err(e) => Err(SlateDBError::from(e)),
            },
            Err(e) => match e {
                Error::NotFound { .. } => Ok(None),
                _ => Err(SlateDBError::from(e)),
            },
        }
    }

    pub(crate) async fn try_read_latest(&self) -> Result<Option<(u64, T)>, SlateDBError> {
        let base = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(base));
        let mut max_id: u64 = 0;
        while let Some(file) = files_stream
            .next()
            .await
            .transpose()
            .map_err(SlateDBError::from)?
        {
            if let Ok(id) = self.parse_id(&file.location) {
                if id > max_id {
                    max_id = id;
                }
            }
        }
        if max_id == 0 {
            return Ok(None);
        }
        match self.try_read(max_id).await? {
            Some(val) => Ok(Some((max_id, val))),
            None => Ok(None),
        }
    }

    // List files for this record type in an id range
    pub(crate) async fn list<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<GenericFileMetadata>, SlateDBError> {
        let base = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(base));
        let mut items = Vec::new();
        while let Some(file) = files_stream
            .next()
            .await
            .transpose()
            .map_err(SlateDBError::from)?
        {
            match self.parse_id(&file.location) {
                Ok(id) if id_range.contains(&id) => items.push(GenericFileMetadata {
                    id,
                    location: file.location,
                    last_modified: file.last_modified,
                    size: file.size as u32,
                }),
                _ => {}
            }
        }
        items.sort_by_key(|m| m.id);
        Ok(items)
    }

    // Delete a specific versioned file (no additional validation)
    pub(crate) async fn delete(&self, id: u64) -> Result<(), SlateDBError> {
        let path = self.path_for(id);
        debug!("deleting manifest [id={}]", id);
        self.object_store
            .delete(&path)
            .await
            .map_err(SlateDBError::from)
    }
}

// Generic file metadata for versioned records
#[derive(Debug)]
pub(crate) struct GenericFileMetadata {
    pub(crate) id: u64,
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: u32,
}

// Generic stored record with optimistic versioned updates
#[derive(Clone)]
pub(crate) struct StoredRecord<T> {
    id: u64,
    record: T,
    store: Arc<RecordStore<T>>,
}

impl<T: Clone> StoredRecord<T> {
    pub(crate) async fn init(store: Arc<RecordStore<T>>, value: T) -> Result<Self, SlateDBError> {
        store.write(1, &value).await?;
        Ok(Self {
            id: 1,
            record: value,
            store,
        })
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    pub(crate) fn record(&self) -> &T {
        &self.record
    }

    fn next_id(&self) -> u64 {
        self.id + 1
    }

    pub(crate) async fn refresh(&mut self) -> Result<&T, SlateDBError> {
        let Some((id, new_val)) = self.store.try_read_latest().await? else {
            return Err(InvalidDBState);
        };
        self.id = id;
        self.record = new_val;
        Ok(&self.record)
    }

    pub(crate) async fn update_dirty(&mut self, dirty: DirtyRecord<T>) -> Result<(), SlateDBError> {
        if dirty.id != self.id {
            return Err(ManifestVersionExists);
        }
        let next = self.next_id();
        self.store.write(next, &dirty.value).await?;
        self.id = next;
        self.record = dirty.value;
        Ok(())
    }

    pub(crate) async fn maybe_apply_update<F>(&mut self, mutator: F) -> Result<(), SlateDBError>
    where
        F: Fn(&StoredRecord<T>) -> Result<Option<DirtyRecord<T>>, SlateDBError>,
    {
        loop {
            let Some(dirty) = mutator(self)? else {
                return Ok(());
            };
            match self.update_dirty(dirty).await {
                Err(SlateDBError::ManifestVersionExists) => {
                    self.refresh().await?;
                    continue;
                }
                Err(e) => return Err(e),
                Ok(()) => return Ok(()),
            }
        }
    }
}

// Local mutable view of a versioned record
#[derive(Clone, Debug)]
pub(crate) struct DirtyRecord<T> {
    id: u64,
    value: T,
}

impl<T> DirtyRecord<T> {
    #[allow(dead_code)]
    fn id(&self) -> u64 {
        self.id
    }
    #[allow(dead_code)]
    fn into_value(self) -> T {
        self.value
    }
}

// Generic fenceable wrapper using epoch getters/setters
pub(crate) struct FenceableRecord<T: Clone> {
    stored: StoredRecord<T>,
    local_epoch: u64,
    get_epoch: fn(&T) -> u64,
    #[allow(dead_code)]
    set_epoch: fn(&mut T, u64),
}

impl<T: Clone + Send + Sync> FenceableRecord<T> {
    pub(crate) async fn init(
        mut stored: StoredRecord<T>,
        record_update_timeout: Duration,
        system_clock: Arc<dyn SystemClock>,
        get_epoch: fn(&T) -> u64,
        set_epoch: fn(&mut T, u64),
    ) -> Result<Self, SlateDBError> {
        utils::timeout(
            system_clock,
            record_update_timeout,
            "record update",
            async {
                loop {
                    let local_epoch = get_epoch(stored.record()) + 1;
                    let mut new_val = stored.record().clone();
                    set_epoch(&mut new_val, local_epoch);
                    let dirty = DirtyRecord {
                        id: stored.id(),
                        value: new_val,
                    };
                    match stored.update_dirty(dirty).await {
                        Err(ManifestVersionExists) => {
                            stored.refresh().await?;
                            continue;
                        }
                        Err(err) => return Err(err),
                        Ok(()) => {
                            return Ok(Self {
                                stored,
                                local_epoch,
                                get_epoch,
                                set_epoch,
                            })
                        }
                    }
                }
            },
        )
        .await
        .map_err(|_| SlateDBError::Timeout {
            op: "record update",
            backoff: Duration::from_secs(1),
        })
    }

    pub(crate) async fn refresh(&mut self) -> Result<(), SlateDBError> {
        self.stored.refresh().await?;
        self.check_epoch()
    }

    pub(crate) async fn update_dirty(&mut self, dirty: DirtyRecord<T>) -> Result<(), SlateDBError> {
        self.check_epoch()?;
        self.stored.update_dirty(dirty).await
    }

    fn check_epoch(&self) -> Result<(), SlateDBError> {
        let stored_epoch = (self.get_epoch)(self.stored.record());
        if self.local_epoch < stored_epoch {
            return Err(SlateDBError::Fenced);
        }
        if self.local_epoch > stored_epoch {
            panic!("the stored epoch is lower than the local epoch");
        }
        Ok(())
    }
}

/// Represents a local view of the manifest that is in the process of being updated
#[derive(Clone, Debug)]
pub(crate) struct DirtyManifest {
    id: u64,
    external_dbs: Vec<ExternalDb>,
    pub(crate) core: CoreDbState,
    writer_epoch: u64,
    compactor_epoch: u64,
}

impl From<DirtyManifest> for Manifest {
    fn from(manifest: DirtyManifest) -> Manifest {
        Manifest {
            external_dbs: manifest.external_dbs,
            core: manifest.core,
            writer_epoch: manifest.writer_epoch,
            compactor_epoch: manifest.compactor_epoch,
        }
    }
}

impl DirtyManifest {
    pub(crate) fn new(id: u64, manifest: Manifest) -> Self {
        Self {
            id,
            external_dbs: manifest.external_dbs,
            core: manifest.core,
            writer_epoch: manifest.writer_epoch,
            compactor_epoch: manifest.compactor_epoch,
        }
    }

    #[allow(dead_code)]
    fn id(&self) -> u64 {
        self.id
    }
}

pub(crate) struct FenceableManifest {
    stored_manifest: FenceableRecord<Manifest>,
    local_epoch: u64,
    stored_epoch: fn(&Manifest) -> u64,
}

// This type wraps StoredManifest, and fences other conflicting writers by incrementing
// the relevant epoch when initialized. It also detects when the current writer has been
// fenced and fails all operations with SlateDBError::Fenced.
impl FenceableManifest {
    pub(crate) async fn init_writer(
        stored_manifest: StoredManifest,
        manifest_update_timeout: Duration,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        // Initialize generic fenceable record using writer epoch
        let fr = FenceableRecord::init(
            stored_manifest.inner,
            manifest_update_timeout,
            system_clock,
            |m: &Manifest| m.writer_epoch,
            |m: &mut Manifest, e: u64| m.writer_epoch = e,
        )
        .await?;
        let local_epoch = fr.local_epoch;
        Ok(Self {
            stored_manifest: fr,
            local_epoch,
            stored_epoch: |m| m.writer_epoch,
        })
    }

    pub(crate) async fn init_compactor(
        stored_manifest: StoredManifest,
        manifest_update_timeout: Duration,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let fr = FenceableRecord::init(
            stored_manifest.inner,
            manifest_update_timeout,
            system_clock,
            |m: &Manifest| m.compactor_epoch,
            |m: &mut Manifest, e: u64| m.compactor_epoch = e,
        )
        .await?;
        let local_epoch = fr.local_epoch;
        Ok(Self {
            stored_manifest: fr,
            local_epoch,
            stored_epoch: |m| m.compactor_epoch,
        })
    }

    pub(crate) async fn refresh(&mut self) -> Result<(), SlateDBError> {
        self.stored_manifest.refresh().await?;
        self.check_epoch()
    }

    pub(crate) fn prepare_dirty(&self) -> Result<DirtyManifest, SlateDBError> {
        self.check_epoch()?;
        let id = self.stored_manifest.stored.id();
        let manifest = self.stored_manifest.stored.record().clone();
        Ok(DirtyManifest::new(id, manifest))
    }

    pub(crate) async fn update_manifest(
        &mut self,
        manifest: DirtyManifest,
    ) -> Result<(), SlateDBError> {
        self.check_epoch()?;
        let dirty = DirtyRecord {
            id: manifest.id(),
            value: Manifest::from(manifest),
        };
        self.stored_manifest.update_dirty(dirty).await
    }

    pub(crate) fn new_checkpoint(
        &self,
        checkpoint_id: Uuid,
        options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        let clock = self.stored_manifest.stored.store.clock.clone();
        let db_state = &self.stored_manifest.stored.record().core;
        let manifest_id = match options.source {
            Some(source_checkpoint_id) => {
                let Some(source_checkpoint) = db_state.find_checkpoint(source_checkpoint_id) else {
                    return Err(CheckpointMissing(source_checkpoint_id));
                };
                source_checkpoint.manifest_id
            }
            None => {
                if !db_state.initialized {
                    return Err(InvalidDBState);
                }
                self.stored_manifest.stored.id() + 1
            }
        };
        Ok(Checkpoint {
            id: checkpoint_id,
            manifest_id,
            expire_time: options.lifetime.map(|l| clock.now() + l),
            create_time: clock.now(),
        })
    }

    pub(crate) async fn write_checkpoint(
        &mut self,
        checkpoint_id: Uuid,
        options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        self.maybe_apply_manifest_update(|fm| {
            let checkpoint = fm.new_checkpoint(checkpoint_id, options)?;
            let mut dirty = fm.prepare_dirty()?;
            dirty.core.checkpoints.push(checkpoint);
            Ok(Some(dirty))
        })
        .await?;
        let checkpoint = self
            .stored_manifest
            .stored
            .record()
            .core
            .find_checkpoint(checkpoint_id)
            .expect("update applied but checkpoint not found")
            .clone();
        Ok(checkpoint)
    }

    pub(crate) async fn maybe_apply_manifest_update<F>(
        &mut self,
        mutator: F,
    ) -> Result<(), SlateDBError>
    where
        F: Fn(&FenceableManifest) -> Result<Option<DirtyManifest>, SlateDBError>,
    {
        loop {
            let Some(dirty) = mutator(self)? else {
                return Ok(());
            };
            match self.update_manifest(dirty).await {
                Err(SlateDBError::ManifestVersionExists) => {
                    self.refresh().await?;
                    continue;
                }
                Err(e) => return Err(e),
                Ok(()) => return Ok(()),
            }
        }
    }

    fn check_epoch(&self) -> Result<(), SlateDBError> {
        Self::check_epoch_against_manifest(
            self.local_epoch,
            self.stored_epoch,
            self.stored_manifest.stored.record(),
        )
    }

    #[allow(clippy::panic)]
    fn check_epoch_against_manifest(
        local_epoch: u64,
        stored_epoch: fn(&Manifest) -> u64,
        manifest: &Manifest,
    ) -> Result<(), SlateDBError> {
        let stored_epoch = stored_epoch(manifest);
        if local_epoch < stored_epoch {
            return Err(SlateDBError::Fenced);
        }
        if local_epoch > stored_epoch {
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
    inner: StoredRecord<Manifest>,
}

impl StoredManifest {
    async fn init(store: Arc<ManifestStore>, manifest: Manifest) -> Result<Self, SlateDBError> {
        // Preserve original behavior: write via ManifestStore (object-store path and semantics)
        let inner = StoredRecord::init(Arc::clone(&store.inner), manifest.clone()).await?;
        Ok(Self {
            id: inner.id,
            manifest,
            inner,
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
        parent_manifest: &Manifest,
        parent_path: String,
        source_checkpoint_id: Uuid,
        rand: Arc<DbRand>,
    ) -> Result<Self, SlateDBError> {
        let manifest = Manifest::cloned(parent_manifest, parent_path, source_checkpoint_id, rand);
        Self::init(clone_manifest_store, manifest).await
    }

    /// Load the current manifest from the supplied manifest store. If there is no db at the
    /// manifest store's path then this fn returns None. Otherwise, on success it returns a
    /// Result with an instance of StoredManifest.
    pub(crate) async fn try_load(store: Arc<ManifestStore>) -> Result<Option<Self>, SlateDBError> {
        let Some((id, manifest)) = store.inner.try_read_latest().await? else {
            return Ok(None);
        };
        let inner = StoredRecord {
            id,
            record: manifest.clone(),
            store: Arc::clone(&store.inner),
        };
        Ok(Some(Self {
            id,
            manifest,
            inner,
        }))
    }

    /// Load the current manifest from the supplied manifest store. If successful,
    /// this method returns a [`Result`] with an instance of [`StoredManifest`].
    /// If no manifests could be found, the error [`LatestManifestMissing`] is returned.
    pub(crate) async fn load(store: Arc<ManifestStore>) -> Result<Self, SlateDBError> {
        Self::try_load(store).await?.ok_or(LatestManifestMissing)
    }

    #[allow(dead_code)]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub(crate) fn prepare_dirty(&self) -> DirtyManifest {
        DirtyManifest::new(self.id, self.manifest.clone())
    }

    pub(crate) fn db_state(&self) -> &CoreDbState {
        &self.manifest.core
    }

    pub(crate) async fn refresh(&mut self) -> Result<&Manifest, SlateDBError> {
        self.inner.refresh().await?;
        self.id = self.inner.id();
        self.manifest = self.inner.record().clone();
        Ok(&self.manifest)
    }

    fn next_id(&self) -> u64 {
        self.id + 1
    }

    fn new_checkpoint(
        manifest: &Manifest,
        current_id: u64,
        clock: &Arc<dyn SystemClock>,
        checkpoint_id: Uuid,
        options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        let manifest_id = match options.source {
            Some(source_checkpoint_id) => {
                let Some(source_checkpoint) = manifest.core.find_checkpoint(source_checkpoint_id)
                else {
                    return Err(CheckpointMissing(source_checkpoint_id));
                };
                source_checkpoint.manifest_id
            }
            None => {
                if !manifest.core.initialized {
                    return Err(InvalidDBState);
                }
                current_id + 1
            }
        };
        Ok(Checkpoint {
            id: checkpoint_id,
            manifest_id,
            expire_time: options.lifetime.map(|l| clock.now() + l),
            create_time: clock.now(),
        })
    }

    pub(crate) async fn write_checkpoint(
        &mut self,
        checkpoint_id: Uuid,
        options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        let clock = self.inner.store.clock.clone();
        self.inner
            .maybe_apply_update(|sr| {
                let mut new_val = sr.record().clone();
                let checkpoint =
                    Self::new_checkpoint(&new_val, sr.id(), &clock, checkpoint_id, options)?;
                new_val.core.checkpoints.push(checkpoint);
                Ok(Some(DirtyRecord {
                    id: sr.id(),
                    value: new_val,
                }))
            })
            .await?;
        // sync from inner
        self.id = self.inner.id();
        self.manifest = self.inner.record().clone();
        Ok(self
            .db_state()
            .find_checkpoint(checkpoint_id)
            .expect("update applied but checkpoint not found")
            .clone())
    }

    pub(crate) async fn delete_checkpoint(
        &mut self,
        checkpoint_id: Uuid,
    ) -> Result<(), SlateDBError> {
        self.inner
            .maybe_apply_update(|sr| {
                let mut new_val = sr.record().clone();
                let before = new_val.core.checkpoints.len();
                new_val.core.checkpoints.retain(|cp| cp.id != checkpoint_id);
                if new_val.core.checkpoints.len() == before {
                    Ok(None)
                } else {
                    Ok(Some(DirtyRecord {
                        id: sr.id(),
                        value: new_val,
                    }))
                }
            })
            .await?;
        // sync from inner
        self.id = self.inner.id();
        self.manifest = self.inner.record().clone();
        Ok(())
    }

    /// Replace an existing checkpoint with a new checkpoint. If the old checkpoint
    /// is no longer present, then the new checkpoint will still be added.
    /// This is useful when establishing a new checkpoint (e.g. in a reader) in
    /// order to avoid two manifest updates.
    pub(crate) async fn replace_checkpoint(
        &mut self,
        old_checkpoint_id: Uuid,
        new_checkpoint_id: Uuid,
        new_checkpoint_options: &CheckpointOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        let clock = self.inner.store.clock.clone();
        self.inner
            .maybe_apply_update(|sr| {
                let mut new_val = sr.record().clone();
                // compute new checkpoint
                let checkpoint = Self::new_checkpoint(
                    &new_val,
                    sr.id(),
                    &clock,
                    new_checkpoint_id,
                    new_checkpoint_options,
                )?;
                new_val
                    .core
                    .checkpoints
                    .retain(|cp| cp.id != old_checkpoint_id);
                new_val.core.checkpoints.push(checkpoint);
                Ok(Some(DirtyRecord {
                    id: sr.id(),
                    value: new_val,
                }))
            })
            .await?;
        // sync from inner
        self.id = self.inner.id();
        self.manifest = self.inner.record().clone();
        let new_checkpoint = self
            .db_state()
            .find_checkpoint(new_checkpoint_id)
            .expect("update applied but checkpoint not found")
            .clone();
        Ok(new_checkpoint)
    }

    pub(crate) async fn refresh_checkpoint(
        &mut self,
        checkpoint_id: Uuid,
        new_lifetime: Duration,
    ) -> Result<Checkpoint, SlateDBError> {
        let clock = self.inner.store.clock.clone();
        self.inner
            .maybe_apply_update(|sr| {
                let mut new_val = sr.record().clone();
                let Some(cp) = new_val
                    .core
                    .checkpoints
                    .iter_mut()
                    .find(|c| c.id == checkpoint_id)
                else {
                    return Err(CheckpointMissing(checkpoint_id));
                };
                cp.expire_time = Some(clock.now() + new_lifetime);
                Ok(Some(DirtyRecord {
                    id: sr.id(),
                    value: new_val,
                }))
            })
            .await?;
        // sync from inner
        self.id = self.inner.id();
        self.manifest = self.inner.record().clone();
        let checkpoint = self
            .db_state()
            .find_checkpoint(checkpoint_id)
            .expect("update applied but checkpoint not found")
            .clone();
        Ok(checkpoint)
    }

    pub(crate) async fn update_manifest(
        &mut self,
        manifest: DirtyManifest,
    ) -> Result<(), SlateDBError> {
        if manifest.id() != self.id {
            return Err(ManifestVersionExists);
        }
        let next_id = self.next_id();
        let manifest = manifest.into();
        // Preserve original behavior by writing through ManifestStore
        self.inner.store.write(next_id, &manifest).await?;
        self.inner.record = manifest.clone();
        self.inner.id = next_id;
        self.manifest = manifest;
        self.id = next_id;
        Ok(())
    }

    /// Apply an update to a stored manifest repeatedly retrying the update
    /// if the write fails due to a manifest version conflict caused by another client
    /// updating the manifest at the same time. The update to be applied is specified by
    /// the mutator parameter, which is a function that takes a &StoredManifest and returns
    /// an optional [`CoreDbState`]. If the mutator returns `None`, then no update will
    /// be applied.
    pub(crate) async fn maybe_apply_manifest_update<F>(
        &mut self,
        mutator: F,
    ) -> Result<(), SlateDBError>
    where
        F: Fn(&StoredManifest) -> Result<Option<DirtyManifest>, SlateDBError>,
    {
        loop {
            let Some(dirty) = mutator(self)? else {
                return Ok(());
            };

            return match self.update_manifest(dirty).await {
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
    pub(crate) size: u32,
}

fn serialize_path<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(path.as_ref())
}

pub(crate) struct ManifestStore {
    inner: Arc<RecordStore<Manifest>>,
}

impl ManifestStore {
    pub(crate) fn new(
        root_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        let inner = Arc::new(RecordStore::new(
            root_path,
            object_store,
            clock.clone(),
            "manifest",
            "manifest",
            Box::new(FlatBufferManifestCodec {}),
        ));
        Self { inner }
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

        debug!("deleting manifest [id={}]", id);
        self.inner.delete(id).await
    }

    /// Read a manifest from the object store. The last element in an unbounded
    /// range is the current manifest.
    /// # Arguments
    /// * `id_range` - The range of IDs to list
    pub(crate) async fn list_manifests<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<ManifestFileMetadata>, SlateDBError> {
        let files = self.inner.list(id_range).await?;
        let mut out = Vec::with_capacity(files.len());
        for f in files {
            out.push(ManifestFileMetadata {
                id: f.id,
                location: f.location,
                last_modified: f.last_modified,
                size: f.size,
            });
        }
        Ok(out)
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
        Ok(self.inner.try_read_latest().await?)
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
        self.inner.try_read(id).await
    }

    pub(crate) async fn read_manifest(&self, id: u64) -> Result<Manifest, SlateDBError> {
        self.try_read_manifest(id).await?.ok_or(ManifestMissing(id))
    }

    /// Validates that no dedicated WAL object store is configured in the latest
    /// manifest of this `ManifestStore`.
    ///
    /// Used to disallow certain currently unsupported operations like cloning.
    pub(crate) async fn validate_no_wal_object_store_configured(&self) -> Result<(), SlateDBError> {
        let (_, manifest) = self.read_latest_manifest().await?;
        if manifest.core.wal_object_store_uri.is_some() {
            return Err(SlateDBError::Unsupported(
                "dedicated WAL object store is not supported".into(),
            ));
        }
        Ok(())
    }

    // parse_id and get_manifest_path no longer needed; RecordStore handles naming.
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::db_state::CoreDbState;
    use crate::manifest::store::DirtyManifest;
    use crate::manifest::Manifest;

    pub(crate) fn new_dirty_manifest() -> DirtyManifest {
        DirtyManifest::new(1u64, Manifest::initial(CoreDbState::new()))
    }
}

#[cfg(test)]
mod tests {
    use crate::checkpoint::Checkpoint;
    use crate::clock::{DefaultSystemClock, SystemClock};
    use crate::config::CheckpointOptions;
    use crate::db_state::CoreDbState;
    use crate::error;
    use crate::error::SlateDBError;
    use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
    use crate::retrying_object_store::RetryingObjectStore;
    use crate::test_utils::FlakyObjectStore;
    use chrono::Timelike;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    const ROOT: &str = "/root/path";

    #[tokio::test]
    async fn test_should_fail_write_on_version_conflict() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let mut sm2 = StoredManifest::load(ms.clone()).await.unwrap();
        sm.update_manifest(sm.prepare_dirty()).await.unwrap();

        let result = sm2.update_manifest(sm2.prepare_dirty()).await;

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
        sm.update_manifest(sm.prepare_dirty()).await.unwrap();

        let (version, _) = ms.read_latest_manifest().await.unwrap();

        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn test_should_update_local_state_on_write() {
        let ms = new_memory_manifest_store();
        let mut sm = StoredManifest::create_new_db(ms.clone(), CoreDbState::new())
            .await
            .unwrap();
        let mut dirty = sm.prepare_dirty();
        dirty.core.next_wal_sst_id = 123;
        sm.update_manifest(dirty).await.unwrap();

        assert_eq!(sm.db_state().next_wal_sst_id, 123);
    }

    #[tokio::test]
    async fn test_should_refresh() {
        let ms = new_memory_manifest_store();
        let mut sm = StoredManifest::create_new_db(ms.clone(), CoreDbState::new())
            .await
            .unwrap();
        let mut sm2 = StoredManifest::load(ms.clone()).await.unwrap();
        let mut dirty = sm.prepare_dirty();
        dirty.core.next_wal_sst_id = 123;
        sm.update_manifest(dirty).await.unwrap();

        let refreshed = sm2.refresh().await.unwrap();

        assert_eq!(refreshed.core.next_wal_sst_id, 123);
        assert_eq!(sm2.db_state().next_wal_sst_id, 123);
    }

    #[tokio::test]
    async fn test_should_bump_writer_epoch() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let timeout = Duration::from_secs(300);
        for i in 1..5 {
            let sm = StoredManifest::load(ms.clone()).await.unwrap();
            FenceableManifest::init_writer(sm, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
            let (_, manifest) = ms.read_latest_manifest().await.unwrap();
            assert_eq!(manifest.writer_epoch, i);
        }
    }

    #[tokio::test]
    async fn test_should_fail_refresh_on_writer_fenced() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let timeout = Duration::from_secs(300);
        let mut writer1 =
            FenceableManifest::init_writer(sm, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap();

        FenceableManifest::init_writer(sm2, timeout, Arc::new(DefaultSystemClock::new()))
            .await
            .unwrap();

        let result = writer1.refresh().await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_should_bump_compactor_epoch() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let timeout = Duration::from_secs(300);
        for i in 1..5 {
            let sm = StoredManifest::load(ms.clone()).await.unwrap();
            FenceableManifest::init_compactor(sm, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
            let (_, manifest) = ms.read_latest_manifest().await.unwrap();
            assert_eq!(manifest.compactor_epoch, i);
        }
    }

    #[tokio::test]
    async fn test_should_fail_refresh_on_compactor_fenced() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let timeout = Duration::from_secs(300);
        let mut compactor1 =
            FenceableManifest::init_compactor(sm, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap();

        FenceableManifest::init_compactor(sm2, timeout, Arc::new(DefaultSystemClock::new()))
            .await
            .unwrap();

        let result = compactor1.refresh().await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_should_fail_manifest_write_of_stale_dirty_manifest() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        let stale = sm.prepare_dirty();
        sm.update_manifest(sm.prepare_dirty()).await.unwrap();

        let result = sm.update_manifest(stale).await;

        assert!(matches!(result, Err(SlateDBError::ManifestVersionExists)));
    }

    #[tokio::test]
    async fn test_should_fail_write_checkpoint_when_fenced() {
        let ms = new_memory_manifest_store();
        let sm = StoredManifest::create_new_db(ms.clone(), CoreDbState::new())
            .await
            .unwrap();
        let timeout = Duration::from_secs(300);
        let mut compactor1 =
            FenceableManifest::init_compactor(sm, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap();
        let mut compactor2 =
            FenceableManifest::init_compactor(sm2, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();

        let result = compactor1
            .write_checkpoint(uuid::Uuid::new_v4(), &CheckpointOptions::default())
            .await;

        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
        assert_state_not_updated(&mut compactor2).await;
    }

    #[tokio::test]
    async fn test_should_fail_state_update_when_fenced() {
        let ms = new_memory_manifest_store();
        let sm = StoredManifest::create_new_db(ms.clone(), CoreDbState::new())
            .await
            .unwrap();
        let timeout = Duration::from_secs(300);
        let mut fm1 =
            FenceableManifest::init_writer(sm, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let sm2 = StoredManifest::load(ms.clone()).await.unwrap();
        let mut fm2 =
            FenceableManifest::init_writer(sm2, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();

        let result = fm1
            .maybe_apply_manifest_update(|fm| {
                let mut dirty = fm.prepare_dirty()?;
                dirty.core.last_l0_seq += 1;
                Ok(Some(dirty))
            })
            .await;

        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert_state_not_updated(&mut fm2).await;
    }

    async fn assert_state_not_updated(fm: &mut FenceableManifest) {
        let original_db_state = fm.stored_manifest.stored.record().core.clone();
        fm.refresh().await.unwrap();
        let refreshed_db_state = fm.stored_manifest.stored.record().core.clone();
        assert_eq!(refreshed_db_state, original_db_state);
    }

    #[tokio::test]
    async fn test_should_read_specific_manifest() {
        // Given
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            os.clone(),
            Arc::new(DefaultSystemClock::new()),
        ));
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let mut dirty = sm.prepare_dirty();
        dirty.core.checkpoints.push(new_checkpoint(sm.id));
        sm.update_manifest(dirty).await.unwrap();

        // When
        let manifest = ms.try_read_manifest(2).await.unwrap().unwrap();

        // Then:
        assert_eq!(1, manifest.core.checkpoints.len());
    }

    #[tokio::test]
    async fn test_retry_write_manifest_on_timeout() {
        // Given a flaky store that times out on the first write
        let base = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(base.clone(), 1));
        let retrying = Arc::new(RetryingObjectStore::new(flaky.clone()));
        let ms = Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            retrying.clone(),
            Arc::new(DefaultSystemClock::new()),
        ));

        // When creating a new DB (initial manifest write under retry)
        let core = CoreDbState::new();
        let _sm = StoredManifest::create_new_db(ms.clone(), core.clone())
            .await
            .unwrap();

        // Then: a retry happened and the manifest matches input
        assert!(flaky.put_attempts() >= 2);
        let written = ms.try_read_manifest(1).await.unwrap().unwrap();
        assert_eq!(written, super::super::Manifest::initial(core));
    }

    #[tokio::test]
    async fn test_list_manifests_unbounded() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();
        sm.update_manifest(sm.prepare_dirty()).await.unwrap();

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
        sm.update_manifest(sm.prepare_dirty()).await.unwrap();
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
        sm.update_manifest(sm.prepare_dirty()).await.unwrap();
        let manifests = ms.list_manifests(..).await.unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].id, 1);
        assert_eq!(manifests[1].id, 2);

        let result = ms.delete_manifest(2).await;
        assert!(matches!(result, Err(error::SlateDBError::InvalidDeletion)));
    }

    fn new_memory_manifest_store() -> Arc<ManifestStore> {
        let os = Arc::new(InMemory::new());
        Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            os.clone(),
            Arc::new(DefaultSystemClock::new()),
        ))
    }

    fn new_checkpoint(manifest_id: u64) -> Checkpoint {
        let create_time = DefaultSystemClock::default()
            .now()
            .with_nanosecond(0)
            .unwrap();
        Checkpoint {
            id: uuid::Uuid::new_v4(),
            manifest_id,
            expire_time: None,
            create_time,
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
        let mut dirty = sm.prepare_dirty();
        dirty.core.checkpoints.push(new_checkpoint(sm.id));
        sm.update_manifest(dirty).await.unwrap();
        let active_manifests = ms.read_active_manifests().await.unwrap();
        assert_eq!(2, active_manifests.len());
        assert_eq!(
            Some(&initial_manifest),
            active_manifests.get(&initial_manifest_id)
        );
        assert_eq!(Some(&sm.manifest), active_manifests.get(&sm.id));

        // Remove the checkpoint and verify that only the latest manifest is active
        let mut dirty = sm.prepare_dirty();
        dirty.core.checkpoints.clear();
        sm.update_manifest(dirty).await.unwrap();
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
        sm.maybe_apply_manifest_update(|_| Ok(None)).await.unwrap();
        assert_eq!(initial_id, sm.id);

        sm.maybe_apply_manifest_update(|sm| Ok(Some(sm.prepare_dirty())))
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
            .write_checkpoint(uuid::Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();

        let _ = sm
            .write_checkpoint(uuid::Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();

        assert!(matches!(
            ms.delete_manifest(checkpoint1.manifest_id).await,
            Err(SlateDBError::InvalidDeletion)
        ));
    }

    #[tokio::test]
    async fn should_refresh_checkpoint() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let options = CheckpointOptions {
            lifetime: Some(Duration::from_secs(100)),
            ..CheckpointOptions::default()
        };

        let checkpoint = sm
            .write_checkpoint(uuid::Uuid::new_v4(), &options)
            .await
            .unwrap();
        let expire_time = checkpoint.expire_time.unwrap();

        let refreshed_checkpoint = sm
            .refresh_checkpoint(checkpoint.id, Duration::from_secs(500))
            .await
            .unwrap();
        let refreshed_expire_time = refreshed_checkpoint.expire_time.unwrap();
        assert!(refreshed_expire_time > expire_time);

        assert_eq!(
            Some(&refreshed_checkpoint),
            sm.manifest.core.find_checkpoint(checkpoint.id)
        );
    }

    #[tokio::test]
    async fn should_fail_refresh_if_checkpoint_missing() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let checkpoint_id = uuid::Uuid::new_v4();
        let result = sm
            .refresh_checkpoint(checkpoint_id, Duration::from_secs(100))
            .await;

        if let Err(SlateDBError::CheckpointMissing(missing_id)) = result {
            assert_eq!(checkpoint_id, missing_id);
        } else {
            panic!("Unexpected result {result:?}")
        }
    }

    #[tokio::test]
    async fn should_replace_checkpoint() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let checkpoint = sm
            .write_checkpoint(uuid::Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();

        let replaced_checkpoint = sm
            .replace_checkpoint(
                checkpoint.id,
                uuid::Uuid::new_v4(),
                &CheckpointOptions::default(),
            )
            .await
            .unwrap();
        assert_ne!(checkpoint.id, replaced_checkpoint.id);
        assert_eq!(None, sm.manifest.core.find_checkpoint(checkpoint.id));
        assert_eq!(
            Some(&replaced_checkpoint),
            sm.manifest.core.find_checkpoint(replaced_checkpoint.id),
        );
    }

    #[tokio::test]
    async fn should_ignore_missing_checkpoint_if_replacing() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let missing_checkpoint_id = uuid::Uuid::new_v4();
        let replaced_checkpoint = sm
            .replace_checkpoint(
                uuid::Uuid::new_v4(),
                missing_checkpoint_id,
                &CheckpointOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(
            Some(&replaced_checkpoint),
            sm.manifest.core.find_checkpoint(replaced_checkpoint.id),
        );
    }

    #[tokio::test]
    async fn should_delete_checkpoint() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let checkpoint = sm
            .write_checkpoint(uuid::Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();

        sm.delete_checkpoint(checkpoint.id).await.unwrap();
        assert_eq!(None, sm.manifest.core.find_checkpoint(checkpoint.id));
    }

    #[tokio::test]
    async fn should_ignore_missing_checkpoint_if_deleting() {
        let ms = new_memory_manifest_store();
        let state = CoreDbState::new();
        let mut sm = StoredManifest::create_new_db(ms.clone(), state.clone())
            .await
            .unwrap();

        let checkpoint_id = uuid::Uuid::new_v4();
        let manifest_id = sm.id;
        sm.delete_checkpoint(checkpoint_id).await.unwrap();
        sm.refresh().await.unwrap();
        assert_eq!(manifest_id, sm.id);
    }

    #[tokio::test]
    async fn test_should_cretry_epoch_bump_if_manifest_version_exists() {
        let os = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new(
            &Path::from(ROOT),
            os.clone(),
            Arc::new(DefaultSystemClock::default()),
        ));
        let state = CoreDbState::new();

        // Mimic two writers A and B that try to bump the epoch at the same time
        let sm_a = StoredManifest::create_new_db(Arc::clone(&ms), state.clone())
            .await
            .unwrap();

        let sm_b = StoredManifest::load(Arc::clone(&ms)).await.unwrap();
        let timeout = Duration::from_secs(300);

        let mut fm_b =
            FenceableManifest::init_writer(sm_b, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        assert_eq!(1, fm_b.local_epoch);

        // The last writer always wins
        let mut fm_a =
            FenceableManifest::init_writer(sm_a, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        assert_eq!(2, fm_a.local_epoch);

        assert!(matches!(
            fm_b.refresh().await.err(),
            Some(SlateDBError::Fenced)
        ));
        assert!(fm_a.refresh().await.is_ok());
    }
}
