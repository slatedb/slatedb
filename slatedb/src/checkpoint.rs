use crate::config::{CheckpointOptions, CheckpointScope};
use crate::db::Db;
use crate::error::SlateDBError;
use crate::mem_table_flush::MemtableFlushMsg;
use serde::Serialize;
use std::time::SystemTime;
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
        // flush all the data into SSTs
        if let CheckpointScope::All = scope {
            if self.inner.wal_enabled {
                self.inner.flush_wals().await?;
            }
            self.inner.flush_memtables().await?;
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
}

#[cfg(test)]
mod tests {
    use crate::admin::AdminBuilder;
    use crate::checkpoint::Checkpoint;
    use crate::checkpoint::CheckpointCreateResult;
    use crate::clock::DefaultSystemClock;
    use crate::clock::SystemClock;
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
    use crate::test_utils;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_should_create_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        // open and close the db to init the manifest and trigger another write
        let db = Db::open(path.clone(), object_store.clone()).await.unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let (_, before_checkpoint) = manifest_store.read_latest_manifest().await.unwrap();

        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: checkpoint_manifest_id,
        } = admin
            .create_checkpoint(&CheckpointOptions::default())
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
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        // open and close the db to init the manifest and trigger another write
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let checkpoint_time = DefaultSystemClock::default().now();

        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: _,
        } = admin
            .create_checkpoint(&CheckpointOptions {
                lifetime: Some(Duration::from_secs(3600)),
                ..CheckpointOptions::default()
            })
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
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        db.close().await.unwrap();
        let CheckpointCreateResult {
            id: source_checkpoint_id,
            manifest_id: source_checkpoint_manifest_id,
        } = admin
            .create_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();

        let CheckpointCreateResult {
            id: _,
            manifest_id: checkpoint_manifest_id,
        } = admin
            .create_checkpoint(&CheckpointOptions {
                source: Some(source_checkpoint_id),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        assert_eq!(checkpoint_manifest_id, source_checkpoint_manifest_id);
    }

    #[tokio::test]
    async fn test_should_fail_create_checkpoint_from_missing_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let admin = AdminBuilder::new(path, object_store.clone()).build();
        // open and close the db to init the manifest and trigger another write
        let _ = Db::builder(path, object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();

        let source_checkpoint_id = crate::utils::uuid();
        let result = admin
            .create_checkpoint(&CheckpointOptions {
                source: Some(source_checkpoint_id),
                ..CheckpointOptions::default()
            })
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
        let admin = AdminBuilder::new(path, object_store.clone()).build();
        let result = admin.create_checkpoint(&CheckpointOptions::default()).await;

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
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        let _ = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        let CheckpointCreateResult { id, manifest_id: _ } = admin
            .create_checkpoint(&CheckpointOptions {
                lifetime: Some(Duration::from_secs(100)),
                ..CheckpointOptions::default()
            })
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

        admin
            .refresh_checkpoint(id, Some(Duration::from_secs(1000)))
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
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        let _ = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();

        let result = admin
            .refresh_checkpoint(crate::utils::uuid(), Some(Duration::from_secs(1000)))
            .await;

        assert!(matches!(result, Err(SlateDBError::InvalidDBState)));
    }

    #[tokio::test]
    async fn test_should_delete_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        let _ = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        let CheckpointCreateResult { id, manifest_id: _ } = admin
            .create_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();

        admin.delete_checkpoint(id).await.unwrap();

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
        test_checkpoint_scope_all(db_options, |manifest| manifest.core.l0.front().unwrap().id)
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
        test_checkpoint_scope_all(db_options, |manifest| manifest.core.l0.front().unwrap().id)
            .await;
    }

    async fn test_checkpoint_scope_all<F: FnOnce(Manifest) -> SsTableId>(
        db_options: Settings,
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
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
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
        let sst_handle = table_store.open_sst(table_id).await.unwrap();

        let mut sst_iter = SstIterator::for_key(
            &sst_handle,
            kv.0,
            Arc::clone(&table_store),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        let sst_entry = sst_iter.next().await.unwrap().unwrap();
        assert_eq!(*kv.1, sst_entry.value)
    }
}
