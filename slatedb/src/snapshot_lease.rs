use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use parking_lot::Mutex;
use slatedb_common::clock::SystemClock;
use slatedb_txn_obj::TransactionalObject;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::config::CheckpointOptions;
use crate::dispatcher::{MessageHandler, MessageHandlerExecutor, MessageTickerDef};
use crate::error::SlateDBError;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::Checkpoint;

pub(crate) const SNAPSHOT_LEASE_TASK_NAME: &str = "snapshot_leases";

#[derive(Debug)]
struct LeaseState {
    checkpoint: Checkpoint,
    provisional: bool,
    error: Option<SlateDBError>,
}

/// Shared ownership of one managed checkpoint generation.
#[derive(Debug)]
pub(crate) struct SnapshotLease {
    state: Mutex<LeaseState>,
    clock: Arc<dyn SystemClock>,
    lifetime: Duration,
    changed: Notify,
    cancelled: CancellationToken,
    tasks: TaskTracker,
    released: async_channel::Sender<()>,
}

impl SnapshotLease {
    pub(crate) fn checkpoint(&self) -> Checkpoint {
        self.state.lock().checkpoint.clone()
    }

    fn lost(state: &LeaseState) -> SlateDBError {
        SlateDBError::SnapshotLeaseLost {
            checkpoint_id: state.checkpoint.id,
            manifest_id: state.checkpoint.manifest_id,
        }
    }

    fn deadline(&self, state: &LeaseState) -> DateTime<Utc> {
        // Keep one eighth of the lifetime as a margin before durable expiration.
        state
            .checkpoint
            .expire_time
            .expect("managed checkpoint expiration")
            - self.lifetime / 8
    }

    fn check_state(&self, state: &mut LeaseState) -> Result<(), SlateDBError> {
        if state.error.is_none() && self.clock.now() >= self.deadline(state) {
            state.error = Some(Self::lost(state));
            self.cancelled.cancel();
            self.tasks.close();
        }
        match &state.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn check(&self) -> Result<(), SlateDBError> {
        self.check_state(&mut self.state.lock())
    }

    pub(crate) fn invalidate(&self, error: SlateDBError) {
        let mut state = self.state.lock();
        if state.error.is_none() {
            state.error = Some(error);
        }
        self.cancelled.cancel();
        self.tasks.close();
    }

    pub(crate) fn invalidate_missing(&self) {
        let error = Self::lost(&self.state.lock());
        self.invalidate(error);
    }

    fn acknowledge(&self, checkpoint: Checkpoint) -> Result<(), SlateDBError> {
        let mut state = self.state.lock();
        self.check_state(&mut state)?;
        state.checkpoint = checkpoint;
        state.provisional = false;
        self.changed.notify_waiters();
        self.check_state(&mut state)
    }

    async fn invalidated(&self) -> SlateDBError {
        loop {
            let changed = self.changed.notified();
            let duration = {
                let mut state = self.state.lock();
                if let Err(error) = self.check_state(&mut state) {
                    return error;
                }
                (self.deadline(&state) - self.clock.now())
                    .to_std()
                    .unwrap_or_default()
            };
            tokio::select! {
                _ = self.cancelled.cancelled() => {},
                _ = changed => {},
                _ = self.clock.sleep(duration) => {},
            }
        }
    }

    pub(crate) async fn protect<F, T>(
        lease: Option<Arc<Self>>,
        future: F,
    ) -> Result<T, SlateDBError>
    where
        F: Future<Output = Result<T, SlateDBError>>,
    {
        match lease {
            Some(lease) => tokio::select! {
                biased;
                error = lease.invalidated() => Err(error),
                result = future => {
                    lease.check()?;
                    result
                }
            },
            None => future.await,
        }
    }

    /// Register child tasks before spawning, under the invalidation lock.
    pub(crate) fn protect_task<F, T>(
        lease: Option<Arc<Self>>,
        future: F,
    ) -> impl Future<Output = Result<T, SlateDBError>>
    where
        F: Future<Output = Result<T, SlateDBError>>,
    {
        let registration = lease
            .as_ref()
            .map(|lease| {
                let mut state = lease.state.lock();
                lease.check_state(&mut state)?;
                Ok::<_, SlateDBError>(lease.tasks.token())
            })
            .transpose();
        async move {
            let _registration = registration?;
            Self::protect(lease, future).await
        }
    }
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        let _ = self.released.try_send(());
    }
}

#[derive(Default)]
struct Registry {
    leases: BTreeMap<Uuid, Weak<SnapshotLease>>,
    closed: Option<SlateDBError>,
}

pub(crate) struct SnapshotLeaseManager {
    registry: Mutex<Registry>,
    store: Arc<ManifestStore>,
    clock: Arc<dyn SystemClock>,
    lifetime: Duration,
    pub(crate) stopped: CancellationToken,
    released: async_channel::Sender<()>,
    receiver: async_channel::Receiver<()>,
}

impl SnapshotLeaseManager {
    pub(crate) fn new(
        store: Arc<ManifestStore>,
        clock: Arc<dyn SystemClock>,
        lifetime: Duration,
    ) -> Arc<Self> {
        let (released, receiver) = async_channel::bounded(1);
        Arc::new(Self {
            registry: Mutex::new(Registry::default()),
            store,
            clock,
            lifetime,
            stopped: CancellationToken::new(),
            released,
            receiver,
        })
    }

    fn expiration(&self) -> DateTime<Utc> {
        let expiration = self.clock.now() + self.lifetime;
        let nanos = expiration.timestamp_subsec_nanos();
        // The manifest stores whole seconds. Round upward before the write.
        if nanos == 0 {
            expiration
        } else {
            expiration + Duration::from_nanos(u64::from(1_000_000_000 - nanos))
        }
    }

    pub(crate) async fn create(
        &self,
        manifest: &mut StoredManifest,
        id: Uuid,
    ) -> Result<Arc<SnapshotLease>, SlateDBError> {
        let lease = Arc::new(SnapshotLease {
            state: Mutex::new(LeaseState {
                checkpoint: Checkpoint {
                    id,
                    manifest_id: manifest.id() + 1,
                    expire_time: Some(self.expiration()),
                    create_time: self.clock.now(),
                    name: None,
                },
                provisional: true,
                error: None,
            }),
            clock: self.clock.clone(),
            lifetime: self.lifetime,
            changed: Notify::new(),
            cancelled: CancellationToken::new(),
            tasks: TaskTracker::new(),
            released: self.released.clone(),
        });
        {
            let mut registry = self.registry.lock();
            if let Some(error) = &registry.closed {
                return Err(error.clone());
            }
            registry.leases.insert(id, Arc::downgrade(&lease));
        }
        let options = CheckpointOptions {
            lifetime: Some(self.lifetime),
            ..CheckpointOptions::default()
        };
        SnapshotLease::protect(
            Some(lease.clone()),
            manifest.maybe_apply_update(|stored| {
                lease.check()?;
                let mut dirty = stored.prepare_dirty()?;
                let mut checkpoint = StoredManifest::new_checkpoint(
                    stored.object(),
                    stored.id().into(),
                    self.clock.as_ref(),
                    id,
                    &options,
                )?;
                checkpoint.expire_time = Some(self.expiration());
                dirty.value.core.checkpoints.push(checkpoint);
                Ok(Some(dirty))
            }),
        )
        .await?;
        let checkpoint = manifest
            .db_state()
            .find_checkpoint(id)
            .expect("created checkpoint")
            .clone();
        lease.acknowledge(checkpoint)?;
        Ok(lease)
    }

    pub(crate) fn spawn(
        self: &Arc<Self>,
        executor: &MessageHandlerExecutor,
    ) -> Result<(), SlateDBError> {
        executor.add_handler(
            SNAPSHOT_LEASE_TASK_NAME.into(),
            Box::new(LeaseMaintenance(self.clone())),
            self.receiver.clone(),
            &Handle::current(),
        )
    }

    pub(crate) fn stop(&self, error: SlateDBError) {
        let mut registry = self.registry.lock();
        if registry.closed.is_none() {
            registry.closed = Some(error.clone());
        }
        for lease in registry.leases.values().filter_map(Weak::upgrade) {
            lease.invalidate(error.clone());
        }
        self.stopped.cancel();
    }

    pub(crate) async fn maintain(&self) -> Result<(), SlateDBError> {
        let (live, retired): (Vec<_>, Vec<_>) = {
            let registry = self.registry.lock();
            if registry.closed.is_some() {
                return Ok(());
            }
            let mut live = Vec::new();
            let mut retired = Vec::new();
            for (id, owner) in &registry.leases {
                match owner.upgrade() {
                    Some(lease) => live.push(lease),
                    None => retired.push(*id),
                }
            }
            (live, retired)
        };
        let due = live
            .iter()
            .filter(|lease| {
                let mut state = lease.state.lock();
                lease.check_state(&mut state).is_ok()
                    && !state.provisional
                    && self.clock.now()
                        >= state
                            .checkpoint
                            .expire_time
                            .expect("managed checkpoint expiration")
                            - self.lifetime / 2
            })
            .cloned()
            .collect::<Vec<_>>();
        if due.is_empty() && retired.is_empty() {
            return Ok(());
        }
        let timeout = live
            .iter()
            .filter_map(|lease| {
                let mut state = lease.state.lock();
                lease.check_state(&mut state).ok().map(|_| {
                    (lease.deadline(&state) - self.clock.now())
                        .to_std()
                        .unwrap_or_default()
                })
            })
            .min()
            .unwrap_or(self.lifetime / 4);
        let update = async {
            let mut manifest = StoredManifest::load(self.store.clone(), self.clock.clone()).await?;
            manifest
                .maybe_apply_update(|stored| {
                    let mut dirty = stored.prepare_dirty()?;
                    let checkpoints = &mut dirty.value.core.checkpoints;
                    checkpoints.retain(|checkpoint| !retired.contains(&checkpoint.id));
                    // Fence prepared creates before forgetting their retired IDs,
                    // including writes whose outcome was unknown after cancellation.
                    let mut changed = !retired.is_empty();
                    for lease in &due {
                        let mut state = lease.state.lock();
                        if lease.check_state(&mut state).is_err() {
                            continue;
                        }
                        let Some(checkpoint) = checkpoints
                            .iter_mut()
                            .find(|checkpoint| checkpoint.id == state.checkpoint.id)
                        else {
                            drop(state);
                            lease.invalidate_missing();
                            continue;
                        };
                        if checkpoint
                            .expire_time
                            .is_none_or(|expiration| expiration <= self.clock.now())
                        {
                            drop(state);
                            lease.invalidate_missing();
                            continue;
                        }
                        checkpoint.expire_time = Some(self.expiration());
                        changed = true;
                    }
                    Ok(changed.then_some(dirty))
                })
                .await?;
            for lease in &due {
                if let Some(checkpoint) = manifest.db_state().find_checkpoint(lease.checkpoint().id)
                {
                    let _ = lease.acknowledge(checkpoint.clone());
                }
            }
            self.registry
                .lock()
                .leases
                .retain(|id, _| !retired.contains(id));
            Ok(())
        };
        tokio::select! {
            biased;
            _ = self.stopped.cancelled() => Ok(()),
            _ = self.clock.sleep(timeout) => {
                for lease in &live { let _ = lease.check(); }
                Ok(())
            },
            result = update => result,
        }
    }

    async fn cleanup(&self, result: Result<(), SlateDBError>) -> Result<(), SlateDBError> {
        self.stop(result.err().unwrap_or(SlateDBError::Closed));
        let (ids, live): (Vec<_>, Vec<_>) = {
            let registry = self.registry.lock();
            (
                registry.leases.keys().copied().collect(),
                registry.leases.values().filter_map(Weak::upgrade).collect(),
            )
        };
        for lease in live {
            lease.tasks.wait().await;
        }
        if ids.is_empty() {
            return Ok(());
        }
        let remove = async {
            let mut manifest = StoredManifest::load(self.store.clone(), self.clock.clone()).await?;
            manifest
                .maybe_apply_update(|stored| {
                    let mut dirty = stored.prepare_dirty()?;
                    let checkpoints = &mut dirty.value.core.checkpoints;
                    checkpoints.retain(|checkpoint| !ids.contains(&checkpoint.id));
                    // Advance the manifest even when a registered checkpoint is absent.
                    // A prepared create must conflict if it completes after cleanup.
                    Ok(Some(dirty))
                })
                .await
        };
        tokio::select! {
            result = remove => result,
            _ = self.clock.sleep(self.lifetime / 4) => Ok(()),
        }
    }
}

struct LeaseMaintenance(Arc<SnapshotLeaseManager>);

#[async_trait]
impl MessageHandler<()> for LeaseMaintenance {
    fn tickers(&mut self) -> Vec<MessageTickerDef<()>> {
        vec![MessageTickerDef::new(self.0.lifetime / 4, Box::new(|| ()))]
    }

    async fn handle(&mut self, _: ()) -> Result<(), SlateDBError> {
        self.0.maintain().await
    }

    async fn cleanup(
        &mut self,
        _: BoxStream<'async_trait, ()>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        self.0.cleanup(result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestCore;
    use crate::test_utils::GatedObjectStore;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb_common::clock::MockSystemClock;

    async fn fixture() -> (
        Arc<SnapshotLeaseManager>,
        StoredManifest,
        Arc<MockSystemClock>,
        Arc<GatedObjectStore>,
    ) {
        let clock = Arc::new(MockSystemClock::new());
        let object_store = Arc::new(GatedObjectStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = object_store.clone();
        let store = Arc::new(ManifestStore::new(&Path::from("lease-tests"), store));
        let manifest =
            StoredManifest::create_new_db(store.clone(), ManifestCore::new(), clock.clone())
                .await
                .unwrap();
        let manager = SnapshotLeaseManager::new(store, clock.clone(), Duration::from_secs(1));
        (manager, manifest, clock, object_store)
    }

    #[tokio::test]
    async fn renews_live_generations_at_half_lifetime_and_retires_final_owner() {
        let (manager, mut manifest, clock, _) = fixture().await;
        let old = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        let other_owner = old.clone();
        let current = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        let before = manager.store.list_manifests(..).await.unwrap().len();
        clock.advance(Duration::from_millis(499)).await;
        manager.maintain().await.unwrap();
        assert_eq!(
            before,
            manager.store.list_manifests(..).await.unwrap().len()
        );
        clock.advance(Duration::from_millis(1)).await;
        manager.maintain().await.unwrap();
        assert_eq!(
            before + 1,
            manager.store.list_manifests(..).await.unwrap().len()
        );
        assert_eq!(
            old.checkpoint().expire_time,
            current.checkpoint().expire_time
        );
        assert_eq!(
            old.checkpoint().expire_time.unwrap().timestamp_millis(),
            2000
        );
        let durable = manager.store.read_latest_manifest().await.unwrap();
        assert_eq!(
            durable
                .manifest
                .core
                .find_checkpoint(old.checkpoint().id)
                .unwrap()
                .expire_time,
            old.checkpoint().expire_time
        );
        for _ in 0..4 {
            clock.advance(Duration::from_millis(500)).await;
            manager.maintain().await.unwrap();
            old.check().unwrap();
        }
        let old_id = old.checkpoint().id;
        drop(old);
        manager.maintain().await.unwrap();
        assert!(manager
            .store
            .read_latest_manifest()
            .await
            .unwrap()
            .manifest
            .core
            .find_checkpoint(old_id)
            .is_some());
        drop(other_owner);
        manager.maintain().await.unwrap();
        let latest = manager.store.read_latest_manifest().await.unwrap();
        assert!(latest.manifest.core.find_checkpoint(old_id).is_none());
        assert!(latest
            .manifest
            .core
            .find_checkpoint(current.checkpoint().id)
            .is_some());
    }

    #[tokio::test]
    async fn expiration_is_terminal_at_the_safe_deadline() {
        let (manager, mut manifest, clock, _) = fixture().await;
        let lease = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        clock.advance(Duration::from_millis(874)).await;
        lease.check().unwrap();
        clock.advance(Duration::from_millis(1)).await;
        assert!(matches!(
            lease.check(),
            Err(SlateDBError::SnapshotLeaseLost { .. })
        ));
        let mut late = lease.checkpoint();
        late.expire_time = Some(clock.now() + Duration::from_secs(10));
        assert!(lease.acknowledge(late).is_err());
        manager.maintain().await.unwrap();
        assert!(lease.check().is_err());
        assert_eq!(
            lease.checkpoint().expire_time.unwrap().timestamp_millis(),
            1000
        );
    }

    #[tokio::test]
    async fn missing_checkpoint_invalidates_only_its_generation() {
        let (manager, mut manifest, clock, _) = fixture().await;
        let old = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        let current = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        manifest
            .delete_checkpoint(old.checkpoint().id)
            .await
            .unwrap();
        clock.advance(Duration::from_millis(500)).await;
        manager.maintain().await.unwrap();
        assert!(matches!(
            old.check(),
            Err(SlateDBError::SnapshotLeaseLost { .. })
        ));
        current.check().unwrap();
    }

    #[tokio::test]
    async fn blocked_renewal_stops_at_the_deadline() {
        let (manager, mut manifest, clock, object_store) = fixture().await;
        let lease = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        clock.advance(Duration::from_millis(500)).await;
        object_store.put_opts_gate.close();
        let maintenance = manager.maintain();
        tokio::pin!(maintenance);
        assert!(futures::poll!(&mut maintenance).is_pending());
        clock.advance(Duration::from_millis(375)).await;
        maintenance.await.unwrap();
        assert!(matches!(
            lease.check(),
            Err(SlateDBError::SnapshotLeaseLost { .. })
        ));
        object_store.put_opts_gate.release();
        assert_eq!(
            lease.checkpoint().expire_time.unwrap().timestamp_millis(),
            1000
        );
    }

    #[tokio::test]
    async fn close_cancels_children_without_waiting_for_retained_caller_futures() {
        let (manager, mut manifest, _, _) = fixture().await;
        let lease = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        let retained = SnapshotLease::protect(
            Some(lease.clone()),
            std::future::pending::<Result<(), SlateDBError>>(),
        );
        tokio::pin!(retained);
        assert!(futures::poll!(&mut retained).is_pending());
        let child = tokio::spawn(SnapshotLease::protect_task(
            Some(lease.clone()),
            std::future::pending::<Result<(), SlateDBError>>(),
        ));
        manager.cleanup(Ok(())).await.unwrap();
        assert!(matches!(child.await.unwrap(), Err(SlateDBError::Closed)));
        assert!(matches!(retained.await, Err(SlateDBError::Closed)));
        assert!(matches!(
            SnapshotLease::protect_task(Some(lease), async { Ok(()) }).await,
            Err(SlateDBError::Closed)
        ));
        assert!(manager
            .store
            .read_latest_manifest()
            .await
            .unwrap()
            .manifest
            .core
            .checkpoints
            .is_empty());
    }
    #[tokio::test]
    async fn fractional_creation_and_conflict_retry_acknowledge_durable_seconds() {
        let (manager, mut stale, clock, _) = fixture().await;
        let mut concurrent = StoredManifest::load(manager.store.clone(), clock.clone())
            .await
            .unwrap();
        concurrent
            .write_checkpoint(Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();
        clock.advance(Duration::from_millis(123)).await;
        let lease = manager.create(&mut stale, Uuid::new_v4()).await.unwrap();
        assert_eq!(lease.checkpoint().manifest_id, concurrent.id() + 1);
        for now in [123, 1500, 2500, 3500] {
            clock.set(now);
            manager.maintain().await.unwrap();
            lease.check().unwrap();
            let durable = manager.store.read_latest_manifest().await.unwrap();
            let durable = durable
                .manifest
                .core
                .find_checkpoint(lease.checkpoint().id)
                .unwrap();
            assert_eq!(durable.expire_time, lease.checkpoint().expire_time);
            assert_eq!(durable.expire_time.unwrap().timestamp_subsec_nanos(), 0);
            assert!(durable.expire_time.unwrap() > clock.now());
        }
    }

    #[tokio::test]
    async fn renewal_retry_does_not_restore_a_removed_checkpoint() {
        let clock = Arc::new(MockSystemClock::new());
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(raw.clone()));
        let path = Path::from("renewal-conflict");
        let store = Arc::new(ManifestStore::new(&path, gated.clone()));
        let direct = Arc::new(ManifestStore::new(&path, raw));
        let mut manifest =
            StoredManifest::create_new_db(store.clone(), ManifestCore::new(), clock.clone())
                .await
                .unwrap();
        let manager = SnapshotLeaseManager::new(store, clock.clone(), Duration::from_secs(1));
        let lease = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        clock.advance(Duration::from_millis(500)).await;
        let arrivals = gated.put_opts_gate.arrivals();
        gated.put_opts_gate.close();
        let renewal = manager.maintain();
        tokio::pin!(renewal);
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = &mut renewal => panic!("renewal completed before the gate: {result:?}"),
                _ = gated.put_opts_gate.wait_for_arrivals(arrivals + 1) => {},
            }
        })
        .await
        .unwrap();
        let mut concurrent = StoredManifest::load(direct, clock).await.unwrap();
        concurrent
            .delete_checkpoint(lease.checkpoint().id)
            .await
            .unwrap();
        gated.put_opts_gate.release();
        renewal.await.unwrap();
        assert!(matches!(
            lease.check(),
            Err(SlateDBError::SnapshotLeaseLost { .. })
        ));
        assert!(manager
            .store
            .read_latest_manifest()
            .await
            .unwrap()
            .manifest
            .core
            .find_checkpoint(lease.checkpoint().id)
            .is_none());
    }

    #[tokio::test]
    async fn exhausted_storage_error_remains_the_terminal_error() {
        let (manager, mut manifest, clock, store) = fixture().await;
        let lease = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        clock.advance(Duration::from_millis(500)).await;
        store
            .put_opts_gate
            .set_error(|| object_store::Error::Generic {
                store: "lease-test",
                source: Box::new(std::io::Error::other("renewal failed")),
            });
        let error = manager.maintain().await.unwrap_err();
        assert!(matches!(error, SlateDBError::ObjectStoreError(_)));
        store.put_opts_gate.clear_error();
        manager.cleanup(Err(error.clone())).await.unwrap();
        assert_eq!(lease.check().unwrap_err().to_string(), error.to_string());
    }

    #[rstest::rstest]
    #[case::cleanup(false)]
    #[case::retirement(true)]
    #[tokio::test]
    async fn cleanup_fences_prepared_checkpoint_write_when_registered_id_is_absent(
        #[case] retire_before_cleanup: bool,
    ) {
        let (manager, mut manifest, clock, _) = fixture().await;
        let lease = manager.create(&mut manifest, Uuid::new_v4()).await.unwrap();
        let checkpoint = lease.checkpoint();
        manifest.delete_checkpoint(checkpoint.id).await.unwrap();

        let mut late = StoredManifest::load(manager.store.clone(), clock)
            .await
            .unwrap();
        let mut dirty = late.prepare_dirty().unwrap();
        dirty.value.core.checkpoints.push(checkpoint.clone());

        if retire_before_cleanup {
            drop(lease);
            manager.maintain().await.unwrap();
        } else {
            manager.cleanup(Ok(())).await.unwrap();
        }
        let late_result = late.update(dirty).await;
        let latest = manager.store.read_latest_manifest().await.unwrap();
        assert!(
            late_result.is_err() && latest.manifest.core.find_checkpoint(checkpoint.id).is_none(),
            "lease retirement did not fence a prepared checkpoint write; retired before cleanup: {retire_before_cleanup}; late result: {late_result:?}; checkpoint present: {}",
            latest
                .manifest
                .core
                .find_checkpoint(checkpoint.id)
                .is_some()
        );
    }
}
