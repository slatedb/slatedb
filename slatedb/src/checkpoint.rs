use crate::config::{CheckpointOptions, CheckpointScope};
use crate::db::Db;
use crate::error::SlateDBError;
use crate::utils::IdGenerator;
use crate::wal::FlushResultFuture;
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct CheckpointBoundary {
    pub(crate) through_seq: Option<u64>,
    pub(crate) wal_id_last_seen: Option<u64>,
}

pub(crate) struct CheckpointRequest {
    pub(crate) id: Uuid,
    pub(crate) boundary: CheckpointBoundary,
    pub(crate) lifecycle: CheckpointLifecycle,
    pub(crate) wal_flush: Option<FlushResultFuture>,
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

    pub(crate) fn accept(&mut self) {
        if let Some(ready_tx) = self.ready_tx.take() {
            let _ = ready_tx.send(Ok(()));
        }
    }

    pub(crate) fn complete(self, result: CheckpointCreateResult) {
        let _ = self.result_tx.send(Ok(result));
    }

    pub(crate) fn fail(mut self, error: SlateDBError) {
        if let Some(ready_tx) = self.ready_tx.take() {
            let _ = ready_tx.send(Err(error.clone()));
        }
        let _ = self.result_tx.send(Err(error));
    }
}

/// Observes a checkpoint that the database owns.
///
/// Dropping this handle does not cancel or delete the checkpoint.
/// Use [`CheckpointOptions::lifetime`] to limit retention of abandoned checkpoints.
pub struct CheckpointHandle {
    id: Uuid,
    result_rx: oneshot::Receiver<CheckpointResult>,
}

impl CheckpointHandle {
    pub(crate) fn new(id: Uuid, result_rx: oneshot::Receiver<CheckpointResult>) -> Self {
        Self { id, result_rx }
    }

    /// Returns the identifier reserved for this checkpoint.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Waits until the checkpoint manifest is durable.
    ///
    /// This method consumes the handle without changing the checkpoint lifetime.
    /// Dropping the returned future does not cancel or delete the checkpoint.
    pub async fn wait(self) -> Result<CheckpointCreateResult, crate::Error> {
        self.wait_inner().await.map_err(Into::into)
    }

    pub(crate) async fn wait_inner(self) -> CheckpointResult {
        self.result_rx
            .await
            .unwrap_or_else(|_| Err(checkpoint_outcome_unknown(self.id)))
    }
}

pub(crate) fn checkpoint_outcome_unknown(id: Uuid) -> SlateDBError {
    SlateDBError::CheckpointOutcomeUnknown(id)
}

impl Db {
    /// Captures the checkpoint boundary and starts the durable storage work.
    ///
    /// [`CheckpointScope::All`] freezes the active memtable, the in-memory write buffer, before this method returns.
    /// Concurrent writes before this method returns can be included.
    /// Writes issued after this method returns are excluded from that checkpoint.
    /// Call [`CheckpointHandle::wait`] to wait for the checkpoint manifest.
    /// WAL upload completion and its errors are reported through that handle.
    ///
    /// The database owns the request once it enters the flush pipeline.
    /// Dropping this future or the handle does not cancel an accepted request.
    /// Use a finite [`CheckpointOptions::lifetime`] to limit retention after a timeout or crash.
    /// With `lifetime: None`, abandoned checkpoints remain until explicit deletion.
    ///
    /// Save [`CheckpointHandle::id`] before waiting if recovery needs the checkpoint identifier.
    /// After the application durably records ownership, call
    /// [`Admin::refresh_checkpoint`](crate::admin::Admin::refresh_checkpoint) with `None` to remove expiration.
    /// Recovery must retry that update before expiration if the application stops between those steps.
    /// Waiting for completion does not remove expiration.
    pub async fn begin_checkpoint(
        &self,
        scope: CheckpointScope,
        options: &CheckpointOptions,
    ) -> Result<CheckpointHandle, crate::Error> {
        let (boundary, wal_flush) = match scope {
            CheckpointScope::All => {
                let (wal_flush, boundary) = self.inner.begin_batch_writer_flush(true).await?;
                (
                    boundary.expect("a memtable freeze must return a checkpoint boundary"),
                    Some(wal_flush),
                )
            }
            CheckpointScope::Durable => {
                let guard = self.inner.state.read();
                let state = guard.state();
                let core = state.core();
                let boundary = CheckpointBoundary {
                    through_seq: None,
                    wal_id_last_seen: if self.inner.wal_enabled {
                        Some(
                            core.next_wal_sst_id
                                .checked_sub(1)
                                .ok_or(SlateDBError::InvalidDBState)?,
                        )
                    } else {
                        None
                    },
                };
                (boundary, None)
            }
        };
        let id = self.inner.rand.rng().gen_uuid();

        self.inner
            .memtable_flusher()
            .begin_checkpoint(id, boundary, options.clone(), wal_flush)
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
    use crate::config::{
        CheckpointOptions, CheckpointScope, FlushOptions, FlushType, GarbageCollectorOptions,
        Settings,
    };
    use crate::db::Db;
    use crate::db_reader::{DbReader, DbReaderMode};
    use crate::db_state::{SsTableId, SsTableView};
    use crate::format::sst::SsTableFormat;
    use crate::iter::RowEntryIterator;
    use crate::manifest::store::ManifestStore;
    use crate::manifest::Manifest;
    use crate::proptest_util::{rng, sample};
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::test_utils;
    use bytes::Bytes;
    use chrono::TimeDelta;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::SystemClock;
    use slatedb_common::clock::{DefaultSystemClock, MockSystemClock};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_dropped_result_sender_reports_unknown_outcome() {
        let id = uuid::Uuid::new_v4();
        let (lifecycle, result_rx, _ready_rx) = CheckpointLifecycle::new();
        drop(lifecycle);

        let error = CheckpointHandle::new(id, result_rx)
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
    async fn test_checkpoint_retention_after_recovery() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_checkpoint_retention_after_recovery");
        let clock = Arc::new(MockSystemClock::new());
        let db = Db::builder(path.clone(), object_store.clone())
            .with_system_clock(clock.clone())
            .with_settings(Settings {
                garbage_collector_options: None,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        db.put(b"key", b"value").await.unwrap();
        let options = CheckpointOptions {
            lifetime: Some(Duration::from_secs(60)),
            ..CheckpointOptions::default()
        };
        let retained = db
            .begin_checkpoint(CheckpointScope::All, &options)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let abandoned = db
            .begin_checkpoint(CheckpointScope::Durable, &options)
            .await
            .unwrap();
        let abandoned_id = abandoned.id();
        drop(abandoned);
        db.close().await.unwrap();

        let admin = AdminBuilder::new(path, object_store)
            .with_system_clock(clock.clone())
            .build();
        let before = admin.list_checkpoints(None).await.unwrap();
        for id in [retained.id, abandoned_id] {
            assert!(before
                .iter()
                .find(|cp| cp.id == id)
                .unwrap()
                .expire_time
                .is_some());
        }
        clock.advance(Duration::from_secs(30)).await;
        admin.refresh_checkpoint(retained.id, None).await.unwrap();
        admin.refresh_checkpoint(retained.id, None).await.unwrap();
        clock.advance(Duration::from_secs(31)).await;
        admin
            .run_gc_once(GarbageCollectorOptions::default())
            .await
            .unwrap();

        let after = admin.list_checkpoints(None).await.unwrap();
        assert!(!after.iter().any(|cp| cp.id == abandoned_id));
        let checkpoint = after.iter().find(|cp| cp.id == retained.id).unwrap();
        assert_eq!(checkpoint.expire_time, None);
        assert_eq!(checkpoint.manifest_id, retained.manifest_id);
        assert!(admin.refresh_checkpoint(abandoned_id, None).await.is_err());
        admin.delete_checkpoint(retained.id).await.unwrap();
        assert!(!admin
            .list_checkpoints(None)
            .await
            .unwrap()
            .iter()
            .any(|cp| cp.id == retained.id));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_begin_checkpoint_returns_before_wal_upload() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_begin_checkpoint_returns_before_wal_upload");
        let fp_registry = Arc::new(FailPointRegistry::new());
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings {
                flush_interval: None,
                ..Settings::default()
            })
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();
        db.put(b"before", b"v1").await.unwrap();
        let started = tokio::time::timeout(
            Duration::from_secs(5),
            db.begin_checkpoint(CheckpointScope::All, &CheckpointOptions::default()),
        )
        .await;
        let handle = match started {
            Ok(Ok(handle)) => handle,
            _ => {
                fail_parallel::remove(fp_registry, "write-wal-sst-io-error");
                panic!("checkpoint creation did not return while the WAL upload was paused");
            }
        };
        let mut completion = Box::pin(handle.wait());
        let completion_pending = futures::poll!(&mut completion).is_pending();
        let flush_pending = tokio::time::timeout(Duration::from_millis(50), db.flush())
            .await
            .is_err();
        fail_parallel::remove(fp_registry, "write-wal-sst-io-error");
        assert!(completion_pending);
        assert!(flush_pending);

        db.put(b"after", b"v2").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        let checkpoint = completion.await.unwrap();
        let reader = DbReader::builder(path, object_store)
            .with_reader_mode(DbReaderMode::Checkpoint(checkpoint.id))
            .build()
            .await
            .unwrap();
        assert_eq!(
            reader.get(b"before").await.unwrap(),
            Some(Bytes::from_static(b"v1"))
        );
        assert_eq!(reader.get(b"after").await.unwrap(), None);
        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delayed_checkpoint_survives_concurrent_table_flushes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_delayed_checkpoint_survives_concurrent_table_flushes");
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings {
                flush_interval: Some(Duration::from_secs(3600)),
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        db.put(b"before", b"v1").await.unwrap();
        let options = CheckpointOptions::default();
        let mut checkpoint = Box::pin(db.begin_checkpoint(CheckpointScope::All, &options));
        assert!(futures::poll!(&mut checkpoint).is_pending());

        // Finish the requested freeze before advancing the table state again.
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.put(b"during", b"v2").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        let handle = checkpoint.await.unwrap();
        db.put(b"after", b"v3").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        let result = handle.wait().await.unwrap();
        let reader = DbReader::builder(path, object_store)
            .with_reader_mode(DbReaderMode::Checkpoint(result.id))
            .build()
            .await
            .unwrap();
        assert_eq!(
            reader.get(b"before").await.unwrap(),
            Some(Bytes::from_static(b"v1"))
        );
        assert_eq!(
            reader.get(b"during").await.unwrap(),
            Some(Bytes::from_static(b"v2"))
        );
        assert_eq!(reader.get(b"after").await.unwrap(), None);
        assert_eq!(
            db.get(b"after").await.unwrap(),
            Some(Bytes::from_static(b"v3"))
        );
        reader.close().await.unwrap();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_begin_checkpoint_keeps_the_wal_boundary_from_its_freeze() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_begin_checkpoint_keeps_wal_boundary");
        let fp_registry = Arc::new(FailPointRegistry::new());
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(Settings {
                flush_interval: Some(Duration::from_secs(3600)),
                ..Settings::default()
            })
            .with_fp_registry(Arc::clone(&fp_registry))
            .build()
            .await
            .unwrap();

        db.put(b"before", b"v1").await.unwrap();
        let checkpoint_reached = Arc::new(AtomicBool::new(false));
        let release_checkpoint = Arc::new(AtomicBool::new(false));
        let callback_reached = Arc::clone(&checkpoint_reached);
        let callback_release = Arc::clone(&release_checkpoint);
        fail_parallel::cfg_callback(
            Arc::clone(&fp_registry),
            "checkpoint-after-reconcile",
            move || {
                callback_reached.store(true, Ordering::Release);
                while !callback_release.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            },
        )
        .unwrap();
        let checkpoint_db = db.clone();
        let checkpoint_task = tokio::spawn(async move {
            checkpoint_db
                .begin_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap()
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            while !checkpoint_reached.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let wal_id_before_later_write = db.inner.wal_observer.status().unwrap().last_flushed_wal_id;

        db.put(b"after", b"v2").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();
        let later_wal_id = db.inner.wal_observer.status().unwrap().last_flushed_wal_id;
        assert!(later_wal_id > wal_id_before_later_write);

        release_checkpoint.store(true, Ordering::Release);
        fail_parallel::remove(Arc::clone(&fp_registry), "checkpoint-after-reconcile");
        let checkpoint = tokio::time::timeout(Duration::from_secs(5), checkpoint_task)
            .await
            .unwrap()
            .unwrap();
        let manifest = ManifestStore::new(&path, object_store)
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();
        assert!(manifest.core.next_wal_sst_id <= later_wal_id);

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
    async fn test_drop_checkpoint_handle_keeps_completed_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_drop_checkpoint_handle_keeps_completed_checkpoint");
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
        assert!(manifest
            .manifest
            .core
            .checkpoints
            .iter()
            .any(|entry| entry.id == checkpoint_id));

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
