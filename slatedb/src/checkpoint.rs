use crate::config::{CheckpointOptions, CheckpointScope};
use crate::db::Db;
use crate::error::SlateDBError;
use crate::memtable_flusher::{FlushTarget, TrackerMessage};
use crate::utils::{IdGenerator, SafeSender};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::oneshot;
use uuid::Uuid;

#[non_exhaustive]
#[derive(Clone, PartialEq, Serialize, Debug)]
pub struct Checkpoint {
    pub id: Uuid,
    pub manifest_id: u64,
    pub expire_time: Option<DateTime<Utc>>,
    pub create_time: DateTime<Utc>,
    pub name: Option<String>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CheckpointCreateResult {
    /// The id of the created checkpoint.
    pub id: Uuid,
    /// The manifest id referenced by the created checkpoint.
    pub manifest_id: u64,
}

pub(crate) type CheckpointResult = Result<CheckpointCreateResult, SlateDBError>;

pub(crate) struct CheckpointRequest {
    pub(crate) id: Uuid,
    pub(crate) wal_id_last_seen: Option<u64>,
    pub(crate) lifecycle: CheckpointLifecycle,
}

pub(crate) struct CheckpointLifecycle {
    result_tx: oneshot::Sender<CheckpointResult>,
    ready_tx: Option<oneshot::Sender<Result<(), SlateDBError>>>,
}

impl CheckpointLifecycle {
    pub(crate) fn new() -> (
        Self,
        oneshot::Receiver<CheckpointResult>,
        oneshot::Receiver<Result<(), SlateDBError>>,
    ) {
        let (result_tx, result_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        (
            Self {
                result_tx,
                ready_tx: Some(ready_tx),
            },
            result_rx,
            ready_rx,
        )
    }

    pub(crate) fn accept(&mut self) -> bool {
        self.ready_tx
            .take()
            .is_some_and(|ready_tx| ready_tx.send(Ok(())).is_ok())
    }

    pub(crate) fn complete(self, result: CheckpointCreateResult) -> bool {
        self.result_tx.send(Ok(result)).is_ok()
    }

    pub(crate) fn fail(mut self, error: SlateDBError) {
        if let Some(ready_tx) = self.ready_tx.take() {
            let _ = ready_tx.send(Err(error.clone()));
        }
        let _ = self.result_tx.send(Err(error));
    }
}

/// A checkpoint request that separates its state boundary from durable storage work.
pub struct CheckpointHandle {
    id: Uuid,
    result_rx: oneshot::Receiver<CheckpointResult>,
    control_tx: SafeSender<TrackerMessage>,
    cancel_on_drop: bool,
}

impl CheckpointHandle {
    pub(crate) fn new(
        id: Uuid,
        result_rx: oneshot::Receiver<CheckpointResult>,
        control_tx: SafeSender<TrackerMessage>,
    ) -> Self {
        Self {
            id,
            result_rx,
            control_tx,
            cancel_on_drop: true,
        }
    }

    /// Returns the identifier reserved for this checkpoint.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Waits until the checkpoint manifest is durable.
    /// This method consumes the handle and keeps a completed checkpoint.
    pub async fn wait(self) -> Result<CheckpointCreateResult, crate::Error> {
        self.wait_inner().await.map_err(Into::into)
    }

    pub(crate) async fn wait_inner(mut self) -> CheckpointResult {
        let result = match (&mut self.result_rx).await {
            Ok(result) => result,
            Err(_) => return Err(checkpoint_outcome_unknown(self.id)),
        };
        self.cancel_on_drop = false;
        result
    }

    /// Requests cancellation without waiting for durable cleanup.
    ///
    /// This method consumes the handle.
    /// This method does not report cleanup errors. Use [`Self::cancel_and_wait`] before closing
    /// the database when the caller must know that cleanup finished.
    pub fn cancel(mut self) {
        let _ = self.control_tx.send(TrackerMessage::CancelCheckpoint {
            id: self.id,
            done: None,
        });
        self.cancel_on_drop = false;
    }

    /// Requests cancellation and waits for cleanup to finish.
    ///
    /// Call this method before [`Db::close`] to receive the cleanup result.
    pub async fn cancel_and_wait(mut self) -> Result<(), crate::Error> {
        let (done_tx, done_rx) = oneshot::channel();
        self.control_tx
            .send(TrackerMessage::CancelCheckpoint {
                id: self.id,
                done: Some(done_tx),
            })
            .map_err(crate::Error::from)?;
        self.cancel_on_drop = false;
        done_rx
            .await
            .map_err(SlateDBError::ReadChannelError)?
            .map_err(Into::into)
    }
}

impl Drop for CheckpointHandle {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            let _ = self.control_tx.send(TrackerMessage::CancelCheckpoint {
                id: self.id,
                done: None,
            });
        }
    }
}

pub(crate) fn checkpoint_cancelled(id: Uuid) -> SlateDBError {
    SlateDBError::BackgroundTaskCancelled(format!("checkpoint {id}"))
}

pub(crate) fn checkpoint_outcome_unknown(id: Uuid) -> SlateDBError {
    SlateDBError::CheckpointOutcomeUnknown(id)
}

impl Db {
    /// Captures the checkpoint boundary and starts the durable storage work.
    ///
    /// [`CheckpointScope::All`] freezes the active memtable before this method returns.
    /// Call [`CheckpointHandle::wait`] to wait for the checkpoint manifest.
    pub async fn begin_checkpoint(
        &self,
        scope: CheckpointScope,
        options: &CheckpointOptions,
    ) -> Result<CheckpointHandle, crate::Error> {
        let target = match scope {
            CheckpointScope::All => {
                self.inner.request_batch_writer_flush(true).await?;
                FlushTarget::All
            }
            CheckpointScope::Durable => FlushTarget::CurrentDurable,
        };
        let id = self.inner.rand.rng().gen_uuid();

        self.inner
            .memtable_flusher()
            .begin_checkpoint(id, target, options.clone())
            .await
            .map_err(Into::into)
    }

    /// Creates a checkpoint of an opened db using the provided options. Returns the ID of the created
    /// checkpoint and the id of the referenced manifest.
    pub async fn create_checkpoint(
        &self,
        scope: CheckpointScope,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, crate::Error> {
        self.begin_checkpoint(scope, options).await?.wait().await
    }
}

#[cfg(test)]
mod tests {
    use crate::admin::AdminBuilder;
    use crate::block_cache_policy::BlockCachePolicy;
    use crate::checkpoint::Checkpoint;
    use crate::checkpoint::CheckpointCreateResult;
    use crate::checkpoint::{CheckpointHandle, CheckpointLifecycle};
    use crate::config::{CheckpointOptions, CheckpointScope, Settings};
    use crate::db::Db;
    use crate::db_state::{SsTableId, SsTableView};
    use crate::db_status::ClosedResultWriter;
    use crate::error::SlateDBError;
    use crate::format::sst::SsTableFormat;
    use crate::iter::RowEntryIterator;
    use crate::manifest::store::ManifestStore;
    use crate::manifest::Manifest;
    use crate::proptest_util::{rng, sample};
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::test_utils;
    use crate::utils::{SafeSender, WatchableOnceCell};
    use bytes::Bytes;
    use chrono::TimeDelta;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::clock::SystemClock;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_dropped_result_sender_reports_unknown_outcome() {
        let id = uuid::Uuid::new_v4();
        let closed_result = WatchableOnceCell::<Result<(), SlateDBError>>::new();
        let (control_tx, _control_rx) =
            SafeSender::unbounded_channel(closed_result.result_reader());
        let (lifecycle, result_rx, _ready_rx) = CheckpointLifecycle::new();
        drop(lifecycle);

        let error = CheckpointHandle::new(id, result_rx, control_tx)
            .wait()
            .await
            .unwrap_err();
        assert!(error.to_string().contains(&format!(
            "checkpoint outcome is unknown. checkpoint_id=`{id}`"
        )));
    }

    #[tokio::test]
    async fn test_should_create_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        // open and close the db to init the manifest and trigger another write
        let db = Db::open(path.clone(), object_store.clone()).await.unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let before_checkpoint = manifest_store.read_latest_manifest().await.unwrap();

        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: checkpoint_manifest_id,
        } = admin
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();

        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        assert_eq!(manifest.id, checkpoint_manifest_id);
        let checkpoints = &manifest.manifest.core.checkpoints;
        assert_eq!(
            before_checkpoint.manifest.core.checkpoints.len() + 1,
            checkpoints.len()
        );
        let checkpoint = checkpoints.iter().find(|c| c.id == checkpoint_id).unwrap();
        assert_eq!(checkpoint.manifest_id, manifest.id);
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
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime: Some(Duration::from_secs(3600)),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        let checkpoints = &manifest.manifest.core.checkpoints;
        let checkpoint = checkpoints.iter().find(|c| c.id == checkpoint_id).unwrap();
        assert!(checkpoint.expire_time.is_some());
        let expire_time = checkpoint.expire_time.unwrap();
        let expected = checkpoint_time + Duration::from_secs(3600);
        // check that expire time is close to the expected value (account for delay/time adjustment)
        if expire_time >= expected {
            assert!(expire_time.signed_duration_since(expected) < TimeDelta::seconds(5))
        } else {
            assert!(expected.signed_duration_since(expire_time) < TimeDelta::seconds(5))
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
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();

        let CheckpointCreateResult {
            id: _,
            manifest_id: checkpoint_manifest_id,
        } = admin
            .create_detached_checkpoint(&CheckpointOptions {
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

        let source_checkpoint_id = uuid::Uuid::new_v4();
        let result = admin
            .create_detached_checkpoint(&CheckpointOptions {
                source: Some(source_checkpoint_id),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap_err();

        assert_eq!(
            result.to_string(),
            format!(
                "Data error: checkpoint missing. checkpoint_id=`{}`",
                source_checkpoint_id
            )
        );
    }

    #[tokio::test]
    async fn test_should_fail_create_checkpoint_no_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let admin = AdminBuilder::new(path, object_store.clone()).build();
        let result = admin
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap_err();

        assert_eq!(
            result.to_string(),
            "Data error: failed to find latest transactional object (e.g. manifest) version"
        );
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
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime: Some(Duration::from_secs(100)),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        let checkpoint = manifest
            .manifest
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

        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        let found: Vec<&Checkpoint> = manifest
            .manifest
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
            .refresh_checkpoint(uuid::Uuid::new_v4(), Some(Duration::from_secs(1000)))
            .await
            .unwrap_err();

        assert_eq!(result.to_string(), "Data error: invalid DB state error");
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
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();

        admin.delete_checkpoint(id).await.unwrap();

        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        assert!(!manifest
            .manifest
            .core
            .checkpoints
            .iter()
            .any(|c| c.id == id));
    }

    #[tokio::test]
    async fn test_checkpoint_scope_with_force_flush() {
        let db_options = Settings {
            flush_interval: Some(Duration::from_millis(5000)),
            ..Settings::default()
        };
        test_checkpoint_scope_all(db_options, |manifest| {
            manifest.core.tree.l0.front().unwrap().clone()
        })
        .await;
    }

    #[tokio::test]
    async fn test_checkpoint_scope_all_flushes_current_memtable_into_latest_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_checkpoint_scope_all_flushes_current_memtable");
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings {
                flush_interval: Some(Duration::from_millis(5000)),
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();

        db.put(b"k1", b"v1").await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();

        let checkpoint = db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();

        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let latest_manifest = manifest_store.read_latest_manifest().await.unwrap();

        assert_eq!(latest_manifest.id, checkpoint.manifest_id);
        assert_eq!(latest_manifest.manifest.core.tree.l0.len(), 1);
        assert!(
            latest_manifest.manifest.core.last_l0_seq >= 2,
            "expected checkpoint flush to advance last_l0_seq, got {}",
            latest_manifest.manifest.core.last_l0_seq
        );

        assert_flushed_entry(
            Arc::clone(&object_store),
            path,
            &latest_manifest
                .manifest
                .core
                .tree
                .l0
                .front()
                .unwrap()
                .sst
                .id,
            (&Bytes::from_static(b"k2"), &Bytes::from_static(b"v2")),
        )
        .await;

        db.close().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_begin_checkpoint_excludes_later_writes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_begin_checkpoint_excludes_later_writes");
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings {
                wal_enabled: false,
                flush_interval: Some(Duration::from_secs(3600)),
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();

        db.put(b"before", b"v1").await.unwrap();
        let checkpoint = db
            .begin_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        db.put(b"after", b"v2").await.unwrap();

        let (checkpoint_result, flush_result) = tokio::join!(checkpoint.wait(), db.flush());
        let checkpoint_result = checkpoint_result.unwrap();
        flush_result.unwrap();

        let checkpoint_manifest = ManifestStore::new(&path, object_store)
            .read_manifest(checkpoint_result.manifest_id)
            .await
            .unwrap();
        assert_eq!(checkpoint_manifest.core.last_l0_seq, 1);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_keeps_completed_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_wait_keeps_completed_checkpoint");
        let db = Db::builder(path.clone(), object_store.clone())
            .build()
            .await
            .unwrap();
        db.put(b"key", b"value").await.unwrap();
        let checkpoint = db
            .begin_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();

        let result = checkpoint.wait().await.unwrap();
        let manifest = ManifestStore::new(&path, object_store)
            .read_latest_manifest()
            .await
            .unwrap();
        assert!(manifest
            .manifest
            .core
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.id == result.id));

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_checkpoint_handle_removes_unclaimed_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_drop_checkpoint_handle_removes_unclaimed_checkpoint");
        let db = Db::builder(path.clone(), object_store.clone())
            .build()
            .await
            .unwrap();
        db.put(b"key", b"value").await.unwrap();
        db.flush().await.unwrap();
        let checkpoint = db
            .begin_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap();
        let checkpoint_id = checkpoint.id();
        db.inner
            .memtable_flusher()
            .refresh_manifest()
            .await
            .unwrap();

        drop(checkpoint);
        db.inner
            .memtable_flusher()
            .refresh_manifest()
            .await
            .unwrap();
        let manifest = ManifestStore::new(&path, object_store)
            .read_latest_manifest()
            .await
            .unwrap();
        assert!(!manifest
            .manifest
            .core
            .checkpoints
            .iter()
            .any(|entry| entry.id == checkpoint_id));

        db.close().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_cancel_checkpoint_removes_pending_request() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_cancel_checkpoint_removes_pending_request");
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings {
                wal_enabled: false,
                flush_interval: Some(Duration::from_secs(3600)),
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();

        db.put(b"key", b"value").await.unwrap();
        let checkpoint = db
            .begin_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        let checkpoint_id = checkpoint.id();
        checkpoint.cancel_and_wait().await.unwrap();

        db.inner
            .memtable_flusher()
            .refresh_manifest()
            .await
            .unwrap();
        let manifest = ManifestStore::new(&path, object_store)
            .read_latest_manifest()
            .await
            .unwrap();
        assert!(!manifest
            .manifest
            .core
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.id == checkpoint_id));

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_checkpoint_removes_committed_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_cancel_checkpoint_removes_committed_checkpoint");
        let db = Db::builder(path.clone(), object_store.clone())
            .build()
            .await
            .unwrap();

        db.put(b"key", b"value").await.unwrap();
        db.flush().await.unwrap();
        let checkpoint = db
            .begin_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap();
        let checkpoint_id = checkpoint.id();

        db.inner
            .memtable_flusher()
            .refresh_manifest()
            .await
            .unwrap();
        let manifest_store = ManifestStore::new(&path, object_store);
        let committed = manifest_store.read_latest_manifest().await.unwrap();
        assert!(committed
            .manifest
            .core
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.id == checkpoint_id));

        checkpoint.cancel_and_wait().await.unwrap();
        let deleted = manifest_store.read_latest_manifest().await.unwrap();
        assert!(!deleted
            .manifest
            .core
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.id == checkpoint_id));

        db.close().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_checkpoint_scope_with_force_flush_wal_disabled() {
        let db_options = Settings {
            flush_interval: Some(Duration::from_millis(5000)),
            wal_enabled: false,
            ..Settings::default()
        };
        test_checkpoint_scope_all(db_options, |manifest| {
            manifest.core.tree.l0.front().unwrap().clone()
        })
        .await;
    }

    async fn test_checkpoint_scope_all<F: FnOnce(Manifest) -> SsTableView>(
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
            &last_flushed_table_id.sst.id,
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
            Arc::clone(&object_store),
            SsTableFormat::default(),
            path.clone(),
            None,
            TableStoreKind::Main,
            BlockCachePolicy::default(),
        ));
        let sst_handle = SsTableView::identity(
            table_store
                .open_sst(table_id, Some(Bytes::new()))
                .await
                .unwrap(),
        );

        let mut sst_iter = SstIterator::for_key_with_stats_initialized(
            &sst_handle,
            kv.0,
            Arc::clone(&table_store),
            SstIteratorOptions::default(),
            None,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");

        let sst_entry = sst_iter.next().await.unwrap().unwrap();
        let val = match sst_entry.value {
            crate::types::ValueDeletable::Value(v) => v,
            _ => panic!("Expected a Value"),
        };
        assert_eq!(*kv.1, val)
    }

    #[tokio::test]
    async fn test_should_create_checkpoint_with_name() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        let db = Db::open(path.clone(), object_store.clone()).await.unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());

        let checkpoint_name = "my_checkpoint".to_string();
        let CheckpointCreateResult {
            id: checkpoint_id,
            manifest_id: _,
        } = admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some(checkpoint_name.clone()),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        let checkpoint = manifest
            .manifest
            .core
            .checkpoints
            .iter()
            .find(|c| c.id == checkpoint_id)
            .unwrap();
        assert_eq!(checkpoint.name, Some(checkpoint_name));
    }

    #[tokio::test]
    async fn test_should_allow_multiple_checkpoints_with_no_name() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        let db = Db::open(path.clone(), object_store.clone()).await.unwrap();
        db.close().await.unwrap();
        let manifest_store = ManifestStore::new(&path, object_store.clone());

        // Create multiple checkpoints without names
        admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: None,
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: None,
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        let manifest = manifest_store.read_latest_manifest().await.unwrap();
        let unnamed_checkpoints: Vec<_> = manifest
            .manifest
            .core
            .checkpoints
            .iter()
            .filter(|c| c.name.is_none())
            .collect();
        assert!(unnamed_checkpoints.len() >= 2);
    }

    #[tokio::test]
    async fn test_should_list_checkpoints_filtered_by_name() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();
        let db = Db::open(path.clone(), object_store.clone()).await.unwrap();
        db.close().await.unwrap();

        // Create checkpoints with different names
        let name1 = "checkpoint_1".to_string();
        let name2 = "checkpoint_2".to_string();
        let name3 = "".to_string();

        admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some(name1.clone()),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some(name2.clone()),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: None,
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some(name3.clone()),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();

        // List all checkpoints
        let all_checkpoints = admin.list_checkpoints(None).await.unwrap();
        assert!(all_checkpoints.len() >= 4);

        // List checkpoints filtered by empty name
        let filtered_checkpoints = admin.list_checkpoints(Some("")).await.unwrap();
        assert_eq!(filtered_checkpoints.len(), 2);
        assert!(filtered_checkpoints
            .iter()
            .all(|cp| cp.name.is_none() || cp.name.as_deref() == Some("")));

        // List checkpoints filtered by name1
        let filtered_checkpoints = admin.list_checkpoints(Some(&name1)).await.unwrap();
        assert_eq!(filtered_checkpoints.len(), 1);
        assert_eq!(filtered_checkpoints[0].name, Some(name1.clone()));

        // List checkpoints filtered by name2
        let filtered_checkpoints = admin.list_checkpoints(Some(&name2)).await.unwrap();
        assert_eq!(filtered_checkpoints.len(), 1);
        assert_eq!(filtered_checkpoints[0].name, Some(name2.clone()));

        // List checkpoints filtered by non-existent name
        let filtered_checkpoints = admin.list_checkpoints(Some("non_existent")).await.unwrap();
        assert_eq!(filtered_checkpoints.len(), 0);
    }
}
