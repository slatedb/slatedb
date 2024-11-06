use crate::config::CheckpointOptions;
use crate::db::Db;
use crate::db_state::Checkpoint;
use crate::error::SlateDBError;
use crate::manifest_store::{apply_db_state_update, ManifestStore, StoredManifest};
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

#[derive(Debug)]
pub struct CheckpointCreateResult {
    /// The id of the created checkpoint.
    pub id: uuid::Uuid,
    /// The manifest id referenced by the created checkpoint.
    pub manifest_id: u64,
}

impl Db {
    /// Creates a checkpoint of the db stored in the object store at the specified path using the
    /// provided options. Note that the scope option does not impact the behaviour of this method.
    /// The checkpoint will reference the current active manifest of the db.
    pub async fn create_checkpoint(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        let manifest_store = Arc::new(ManifestStore::new(path, object_store));
        let Some(mut stored_manifest) = StoredManifest::load(manifest_store).await? else {
            return Err(SlateDBError::ManifestMissing);
        };
        let id = uuid::Uuid::new_v4();
        apply_db_state_update(&mut stored_manifest, |stored_manifest| {
            let expire_time = options.lifetime.map(|l| SystemTime::now() + l);
            let db_state = stored_manifest.db_state();
            let manifest_id = match options.source {
                Some(source_checkpoint_id) => {
                    let Some(source_checkpoint) = db_state
                        .checkpoints
                        .iter()
                        .find(|c| c.id == source_checkpoint_id)
                    else {
                        return Err(SlateDBError::InvalidDBState);
                    };
                    source_checkpoint.manifest_id
                }
                None => {
                    if !db_state.initialized {
                        return Err(SlateDBError::InvalidDBState);
                    }
                    stored_manifest.id()
                }
            };
            let checkpoint = Checkpoint {
                id,
                manifest_id,
                expire_time,
                create_time: SystemTime::now(),
            };
            let mut updated_db_state = db_state.clone();
            updated_db_state.checkpoints.push(checkpoint);
            Ok(updated_db_state)
        })
        .await?;
        let checkpoint = stored_manifest
            .db_state()
            .checkpoints
            .iter()
            .find(|c| c.id == id)
            .expect("update applied but checkpoint not found");
        Ok(CheckpointCreateResult {
            id,
            manifest_id: checkpoint.manifest_id,
        })
    }

    /// Refresh the lifetime of an existing checkpoint. Takes the id of an existing checkpoint
    /// and a lifetime, and sets the lifetime of the checkpoint to the specified lifetime. If
    /// there is no checkpoint with the specified id, then this fn fails with
    /// SlateDBError::InvalidDbState
    pub async fn refresh_checkpoint(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        id: Uuid,
        lifetime: Option<Duration>,
    ) -> Result<(), SlateDBError> {
        let manifest_store = Arc::new(ManifestStore::new(path, object_store));
        let Some(mut stored_manifest) = StoredManifest::load(manifest_store).await? else {
            return Err(SlateDBError::ManifestMissing);
        };
        apply_db_state_update(&mut stored_manifest, |stored_manifest| {
            let mut db_state = stored_manifest.db_state().clone();
            let expire_time = lifetime.map(|l| SystemTime::now() + l);
            let Some(_) = db_state.checkpoints.iter_mut().find_map(|c| {
                if c.id == id {
                    c.expire_time = expire_time;
                    return Some(());
                }
                None
            }) else {
                return Err(SlateDBError::InvalidDBState);
            };
            Ok(db_state)
        })
        .await
    }

    /// Deletes the checkpoint with the specified id.
    pub async fn delete_checkpoint(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        id: Uuid,
    ) -> Result<(), SlateDBError> {
        let manifest_store = Arc::new(ManifestStore::new(path, object_store));
        let Some(mut stored_manifest) = StoredManifest::load(manifest_store).await? else {
            return Err(SlateDBError::ManifestMissing);
        };
        apply_db_state_update(&mut stored_manifest, |stored_manifest| {
            let mut db_state = stored_manifest.db_state().clone();
            let checkpoints: Vec<Checkpoint> = db_state
                .checkpoints
                .iter()
                .filter(|c| c.id != id)
                .cloned()
                .collect();
            db_state.checkpoints = checkpoints;
            Ok(db_state)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::checkpoint::CheckpointCreateResult;
    use crate::config::{CheckpointOptions, DbOptions};
    use crate::db::Db;
    use crate::db_state::Checkpoint;
    use crate::error::SlateDBError;
    use crate::manifest_store::ManifestStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn test_should_create_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        // open and close the db to init the manifest and trigger another write
        let db = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
            .await
            .unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let (manifest_id, before_checkpoint) = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap();

        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: checkpoint_manifest_id,
        } = Db::create_checkpoint(&path, object_store.clone(), &CheckpointOptions::default())
            .await
            .unwrap();

        let (_, manifest) = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manifest_id, checkpoint_manifest_id);
        let checkpoints = &manifest.core.checkpoints;
        assert_eq!(
            before_checkpoint.core.checkpoints.len() + 1,
            checkpoints.len()
        );
        let checkpoint = checkpoints.iter().find(|c| c.id == checkpoint_id).unwrap();
        assert_eq!(checkpoint.manifest_id, manifest_id);
        assert_eq!(checkpoint.expire_time, None);
    }

    #[tokio::test]
    async fn test_should_create_checkpoint_with_expiry() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        // open and close the db to init the manifest and trigger another write
        let db = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
            .await
            .unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let checkpoint_time = SystemTime::now();

        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: _,
        } = Db::create_checkpoint(
            &path,
            object_store.clone(),
            &CheckpointOptions {
                lifetime: Some(Duration::from_secs(3600)),
                ..CheckpointOptions::default()
            },
        )
        .await
        .unwrap();

        let (_, manifest) = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap();
        let checkpoints = &manifest.core.checkpoints;
        let checkpoint = checkpoints.iter().find(|c| c.id == checkpoint_id).unwrap();
        assert!(checkpoint.expire_time.is_some());
        let expire_time = checkpoint.expire_time.unwrap();
        let expected = checkpoint_time + Duration::from_secs(3600);
        // check that expire time is close to the expected value (account for delay/time adjustment)
        if expire_time >= expected {
            assert!(expire_time.duration_since(expected).unwrap() < Duration::from_secs(5))
        } else {
            assert!(expected.duration_since(expire_time).unwrap() < Duration::from_secs(5))
        }
    }

    #[tokio::test]
    async fn test_should_create_checkpoint_from_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
            .await
            .unwrap();
        db.close().await.unwrap();
        let CheckpointCreateResult {
            id: source_checkpoint_id,
            manifest_id: source_checkpoint_manifest_id,
        } = Db::create_checkpoint(&path, object_store.clone(), &CheckpointOptions::default())
            .await
            .unwrap();

        let CheckpointCreateResult {
            id: _,
            manifest_id: checkpoint_manifest_id,
        } = Db::create_checkpoint(
            &path,
            object_store.clone(),
            &CheckpointOptions {
                source: Some(source_checkpoint_id),
                ..CheckpointOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(checkpoint_manifest_id, source_checkpoint_manifest_id);
    }

    #[tokio::test]
    async fn test_should_fail_create_checkpoint_from_missing_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        // open and close the db to init the manifest and trigger another write
        let _ = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
            .await
            .unwrap();

        let result = Db::create_checkpoint(
            &path,
            object_store.clone(),
            &CheckpointOptions {
                source: Some(uuid::Uuid::new_v4()),
                ..CheckpointOptions::default()
            },
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SlateDBError::InvalidDBState));
    }

    #[tokio::test]
    async fn test_should_fail_create_checkpoint_no_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let result =
            Db::create_checkpoint(&path, object_store.clone(), &CheckpointOptions::default()).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SlateDBError::ManifestMissing));
    }

    #[tokio::test]
    async fn test_should_refresh_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let _ = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
            .await
            .unwrap();
        let CheckpointCreateResult { id, manifest_id: _ } = Db::create_checkpoint(
            &path,
            object_store.clone(),
            &CheckpointOptions {
                lifetime: Some(Duration::from_secs(100)),
                ..CheckpointOptions::default()
            },
        )
        .await
        .unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let (_, manifest) = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap();
        let checkpoint = manifest
            .core
            .checkpoints
            .iter()
            .find(|c| c.id == id)
            .unwrap();
        let expire_time = checkpoint.expire_time.unwrap();

        Db::refresh_checkpoint(
            &path,
            object_store.clone(),
            id,
            Some(Duration::from_secs(1000)),
        )
        .await
        .unwrap();

        let (_, manifest) = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap();
        let found: Vec<&Checkpoint> = manifest
            .core
            .checkpoints
            .iter()
            .filter(|c| c.id == id)
            .collect();
        assert_eq!(1, found.len());
        let refreshed_expire_time = found.first().unwrap().expire_time.unwrap();
        assert!(refreshed_expire_time > expire_time);
    }

    #[tokio::test]
    async fn test_should_fail_refresh_checkpoint_if_checkpoint_missing() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let _ = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
            .await
            .unwrap();

        let result = Db::refresh_checkpoint(
            &path,
            object_store.clone(),
            uuid::Uuid::new_v4(),
            Some(Duration::from_secs(1000)),
        )
        .await;

        assert!(matches!(result, Err(SlateDBError::InvalidDBState)));
    }

    #[tokio::test]
    async fn test_should_delete_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let _ = Db::open_with_opts(path.clone(), DbOptions::default(), object_store.clone())
            .await
            .unwrap();
        let CheckpointCreateResult { id, manifest_id: _ } =
            Db::create_checkpoint(&path, object_store.clone(), &CheckpointOptions::default())
                .await
                .unwrap();

        Db::delete_checkpoint(&path, object_store.clone(), id)
            .await
            .unwrap();

        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let (_, manifest) = manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .unwrap();
        assert!(!manifest.core.checkpoints.iter().any(|c| c.id == id));
    }
}
