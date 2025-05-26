use crate::config::{CheckpointOptions, CheckpointScope};
use crate::db::Db;
use crate::error::SlateDBError;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::mem_table_flush::MemtableFlushMsg;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

#[non_exhaustive]
#[derive(Clone, PartialEq, Serialize, Debug)]
pub struct Checkpoint {
    pub id: Uuid,
    pub manifest_id: u64,
    pub expire_time: Option<SystemTime>,
    pub create_time: SystemTime,
}

#[non_exhaustive]
#[derive(Debug)]
pub struct CheckpointCreateResult {
    /// The id of the created checkpoint.
    pub id: Uuid,
    /// The manifest id referenced by the created checkpoint.
    pub manifest_id: u64,
}

impl Db {
    /// Creates a checkpoint of an opened db using the provided options. Returns the ID of the created
    /// checkpoint and the id of the referenced manifest.
    pub async fn create_checkpoint(
        &self,
        scope: CheckpointScope,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        if let CheckpointScope::All { force_flush } = scope {
            if force_flush {
                self.flush().await?;
            } else {
                self.await_flush().await?;
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inner
            .memtable_flush_notifier
            .send(MemtableFlushMsg::CreateCheckpoint {
                options: options.clone(),
                sender: tx,
            })
            .map_err(|_| SlateDBError::CheckpointChannelError)?;

        rx.await?
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
        let mut stored_manifest = StoredManifest::load(manifest_store).await?;
        stored_manifest
            .maybe_apply_manifest_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty();
                let expire_time = lifetime.map(|l| SystemTime::now() + l);
                let Some(_) = dirty.core.checkpoints.iter_mut().find_map(|c| {
                    if c.id == id {
                        c.expire_time = expire_time;
                        return Some(());
                    }
                    None
                }) else {
                    return Err(SlateDBError::InvalidDBState);
                };
                Ok(Some(dirty))
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
        let mut stored_manifest = StoredManifest::load(manifest_store).await?;
        stored_manifest
            .maybe_apply_manifest_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty();
                let checkpoints: Vec<Checkpoint> = dirty
                    .core
                    .checkpoints
                    .iter()
                    .filter(|c| c.id != id)
                    .cloned()
                    .collect();
                dirty.core.checkpoints = checkpoints;
                Ok(Some(dirty))
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::checkpoint::Checkpoint;
    use crate::checkpoint::CheckpointCreateResult;
    use crate::config::{CheckpointOptions, CheckpointScope, Settings};
    use crate::db::Db;
    use crate::db_state::SsTableId;
    use crate::error::SlateDBError;
    use crate::iter::KeyValueIterator;
    use crate::manifest::store::ManifestStore;
    use crate::manifest::Manifest;
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::{rng, sample};
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::TableStore;
    use crate::{admin, test_utils};
    use bytes::Bytes;
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
        let db = Db::open(path.clone(), object_store.clone()).await.unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let (_, before_checkpoint) = manifest_store.read_latest_manifest().await.unwrap();

        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: checkpoint_manifest_id,
        } = admin::create_checkpoint(path, object_store.clone(), &CheckpointOptions::default())
            .await
            .unwrap();

        let (latest_manifest_id, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        assert_eq!(latest_manifest_id, checkpoint_manifest_id);
        let checkpoints = &manifest.core.checkpoints;
        assert_eq!(
            before_checkpoint.core.checkpoints.len() + 1,
            checkpoints.len()
        );
        let checkpoint = checkpoints.iter().find(|c| c.id == checkpoint_id).unwrap();
        assert_eq!(checkpoint.manifest_id, latest_manifest_id);
        assert_eq!(checkpoint.expire_time, None);
    }

    #[tokio::test]
    async fn test_should_create_checkpoint_with_expiry() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        // open and close the db to init the manifest and trigger another write
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let checkpoint_time = SystemTime::now();

        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: _,
        } = admin::create_checkpoint(
            path,
            object_store.clone(),
            &CheckpointOptions {
                lifetime: Some(Duration::from_secs(3600)),
                ..CheckpointOptions::default()
            },
        )
        .await
        .unwrap();

        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
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
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        db.close().await.unwrap();
        let CheckpointCreateResult {
            id: source_checkpoint_id,
            manifest_id: source_checkpoint_manifest_id,
        } = admin::create_checkpoint(
            path.clone(),
            object_store.clone(),
            &CheckpointOptions::default(),
        )
        .await
        .unwrap();

        let CheckpointCreateResult {
            id: _,
            manifest_id: checkpoint_manifest_id,
        } = admin::create_checkpoint(
            path,
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
        let path = "/tmp/test_kv_store";
        // open and close the db to init the manifest and trigger another write
        let _ = Db::builder(path, object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();

        let source_checkpoint_id = uuid::Uuid::new_v4();
        let result = admin::create_checkpoint(
            path,
            object_store.clone(),
            &CheckpointOptions {
                source: Some(source_checkpoint_id),
                ..CheckpointOptions::default()
            },
        )
        .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), SlateDBError::CheckpointMissing(id) if id == source_checkpoint_id)
        );
    }

    #[tokio::test]
    async fn test_should_fail_create_checkpoint_no_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";

        let result =
            admin::create_checkpoint(path, object_store.clone(), &CheckpointOptions::default())
                .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SlateDBError::LatestManifestMissing
        ));
    }

    #[tokio::test]
    async fn test_should_refresh_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let _ = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        let CheckpointCreateResult { id, manifest_id: _ } = admin::create_checkpoint(
            path.clone(),
            object_store.clone(),
            &CheckpointOptions {
                lifetime: Some(Duration::from_secs(100)),
                ..CheckpointOptions::default()
            },
        )
        .await
        .unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
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

        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
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
        let _ = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
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
        let _ = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        let CheckpointCreateResult { id, manifest_id: _ } = admin::create_checkpoint(
            path.clone(),
            object_store.clone(),
            &CheckpointOptions::default(),
        )
        .await
        .unwrap();

        Db::delete_checkpoint(&path, object_store.clone(), id)
            .await
            .unwrap();

        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();
        assert!(!manifest.core.checkpoints.iter().any(|c| c.id == id));
    }

    #[tokio::test]
    async fn test_checkpoint_scope_with_force_flush() {
        let db_options = Settings {
            flush_interval: Some(Duration::from_millis(5000)),
            ..Settings::default()
        };
        test_checkpoint_scope_all(db_options, true, |manifest| {
            SsTableId::Wal(manifest.core.next_wal_sst_id - 1)
        })
        .await;
    }

    #[tokio::test]
    async fn test_checkpoint_scope_with_no_force_flush() {
        let db_options = Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Settings::default()
        };
        test_checkpoint_scope_all(db_options, false, |manifest| {
            SsTableId::Wal(manifest.core.next_wal_sst_id - 1)
        })
        .await;
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_checkpoint_scope_with_force_flush_wal_disabled() {
        let db_options = Settings {
            flush_interval: Some(Duration::from_millis(5000)),
            wal_enabled: false,
            ..Settings::default()
        };
        test_checkpoint_scope_all(db_options, true, |manifest| {
            manifest.core.l0.front().unwrap().id
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    #[cfg(feature = "wal_disable")]
    async fn test_checkpoint_scope_with_no_force_flush_wal_disabled() {
        let db_options = Settings {
            flush_interval: Some(Duration::from_millis(10)),
            wal_enabled: false,
            ..Settings::default()
        };

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let db = Arc::new(
            Db::builder(path, object_store.clone())
                .with_settings(db_options)
                .build()
                .await
                .unwrap(),
        );

        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 1000, 10);
        test_utils::seed_database(&db, &table, false).await.unwrap();

        // Under the current implementation, when the WAL is disabled, we have to wait for
        // either an explicit flush or for enough accumulated new data to force a flush of
        // the current memtable.
        let db_clone = Arc::clone(&db);
        let checkpoint_handle = tokio::spawn(async move {
            db_clone
                .create_checkpoint(
                    CheckpointScope::All { force_flush: false },
                    &CheckpointOptions::default(),
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        db.flush().await.unwrap();

        let checkpoint = tokio::join!(checkpoint_handle).0.unwrap().unwrap();

        let manifest_store = ManifestStore::new(&Path::from(path), object_store.clone());
        let manifest = manifest_store
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();

        let last_written_kv = table.last_key_value().unwrap();
        assert_flushed_entry(
            object_store.clone(),
            Path::from(path),
            &manifest.core.l0.front().unwrap().id,
            last_written_kv,
        )
        .await
    }

    async fn test_checkpoint_scope_all<F: FnOnce(Manifest) -> SsTableId>(
        db_options: Settings,
        force_flush: bool,
        last_flushed_table: F,
    ) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(db_options)
            .build()
            .await
            .unwrap();

        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 1000, 10);
        test_utils::seed_database(&db, &table, false).await.unwrap();

        let checkpoint = db
            .create_checkpoint(
                CheckpointScope::All { force_flush },
                &CheckpointOptions::default(),
            )
            .await
            .unwrap();

        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let manifest = manifest_store
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();

        let last_written_kv = table.last_key_value().unwrap();
        let last_flushed_table_id = last_flushed_table(manifest);
        assert_flushed_entry(
            Arc::clone(&object_store),
            path,
            &last_flushed_table_id,
            last_written_kv,
        )
        .await;
    }

    async fn assert_flushed_entry(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        table_id: &SsTableId,
        kv: (&Bytes, &Bytes),
    ) {
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(Arc::clone(&object_store), None),
            SsTableFormat::default(),
            path.clone(),
            None,
        ));
        let last_checkpoint_wal = table_store.open_sst(table_id).await.unwrap();

        let mut wal_iter = SstIterator::for_key(
            &last_checkpoint_wal,
            kv.0,
            Arc::clone(&table_store),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        let wal_entry = wal_iter.next().await.unwrap().unwrap();
        assert_eq!(*kv.1, wal_entry.value)
    }
}
