use crate::cached_object_store::CachedObjectStore;
use crate::compactor_state::{Compactions, VersionedCompactions};
use crate::error::SlateDBError;
#[allow(dead_code)]
use crate::error::SlateDBError::LatestTransactionalObjectVersionMissing;
use crate::flatbuffer_types::FlatBufferCompactionsCodec;
use chrono::Utc;
use log::warn;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;
use slatedb_common::clock::SystemClock;
use slatedb_txn_obj::object_store::ObjectStoreSequencedStorageProtocol;
use slatedb_txn_obj::{
    DirtyObject, FenceableTransactionalObject, MonotonicId, SequencedStorageProtocol,
    SimpleTransactionalObject, TransactionalObject, TransactionalObjectError,
    TransactionalStorageProtocol,
};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

const COMPACTIONS_SUBDIR: &str = "compactions";
const COMPACTIONS_FILE_SUFFIX: &str = "compactions";

fn is_checksum_mismatch(err: &TransactionalObjectError) -> bool {
    if let TransactionalObjectError::CallbackError(inner) = err {
        if let Some(e) = inner.downcast_ref::<SlateDBError>() {
            return matches!(e, SlateDBError::ChecksumMismatch);
        }
    }
    false
}

/// Represents the compactions stored in the object store. This type tracks the current
/// contents and id of the stored compactions state, and allows callers to read and update
/// it using transactional semantics.
pub(crate) struct StoredCompactions {
    inner: SimpleTransactionalObject<Compactions>,
}

impl StoredCompactions {
    async fn init(
        store: Arc<CompactionsStore>,
        compactions: Compactions,
    ) -> Result<Self, SlateDBError> {
        let inner = SimpleTransactionalObject::<Compactions>::init(
            Arc::clone(&store.inner)
                as Arc<dyn TransactionalStorageProtocol<Compactions, MonotonicId>>,
            compactions.clone(),
        )
        .await?;
        Ok(Self { inner })
    }

    /// Create a new compactions record with the supplied compactor epoch and no compactions.
    pub(crate) async fn create(
        store: Arc<CompactionsStore>,
        compactor_epoch: u64,
    ) -> Result<Self, SlateDBError> {
        let compactions = Compactions::new(compactor_epoch);
        Self::init(store, compactions).await
    }

    /// Load the current compactions state from the supplied compactions store. If there is no
    /// compactions state stored, this fn returns None. Otherwise, on success it returns a
    /// Result with an instance of StoredCompactions.
    pub(crate) async fn try_load(
        store: Arc<CompactionsStore>,
    ) -> Result<Option<Self>, SlateDBError> {
        let Some(inner) =
            SimpleTransactionalObject::<Compactions>::try_load(Arc::clone(&store.inner)
                as Arc<dyn TransactionalStorageProtocol<Compactions, MonotonicId>>)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self { inner }))
    }

    /// Load the current compactions state from the supplied compactions store. If successful,
    /// this method returns a [`Result`] with an instance of [`StoredCompactions`].
    /// If no compactions could be found, the error [`LatestTransactionalObjectVersionMissing`] is returned.
    #[cfg(test)]
    pub(crate) async fn load(store: Arc<CompactionsStore>) -> Result<Self, SlateDBError> {
        SimpleTransactionalObject::<Compactions>::try_load(Arc::clone(&store.inner)
            as Arc<dyn TransactionalStorageProtocol<Compactions, MonotonicId>>)
        .await?
        .map(|inner| Self { inner })
        .ok_or(LatestTransactionalObjectVersionMissing)
    }

    #[allow(dead_code)]
    pub(crate) fn id(&self) -> u64 {
        self.inner.id().into()
    }

    #[cfg(test)]
    pub(crate) fn compactions(&self) -> &Compactions {
        self.inner.object()
    }

    #[allow(dead_code)]
    pub(crate) fn prepare_dirty(&self) -> Result<DirtyObject<Compactions>, SlateDBError> {
        Ok(self.inner.prepare_dirty()?)
    }

    #[allow(dead_code)]
    pub(crate) async fn refresh(&mut self) -> Result<&Compactions, SlateDBError> {
        Ok(self.inner.refresh().await?)
    }

    #[allow(dead_code)]
    pub(crate) async fn update(
        &mut self,
        dirty: DirtyObject<Compactions>,
    ) -> Result<(), SlateDBError> {
        Ok(self.inner.update(dirty).await?)
    }
}

pub(crate) struct FenceableCompactions {
    inner: FenceableTransactionalObject<Compactions>,
}

// This type wraps StoredCompactions, and fences other conflicting writers by incrementing
// the compactor epoch when initialized. It also detects when the current writer has been
// fenced and fails all operations with SlateDBError::Fenced.
impl FenceableCompactions {
    #[cfg(test)]
    pub(crate) async fn init(
        stored_compactions: StoredCompactions,
        compactions_update_timeout: Duration,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<Self, SlateDBError> {
        let fr = FenceableTransactionalObject::init(
            stored_compactions.inner,
            compactions_update_timeout,
            system_clock,
            |c: &Compactions| c.compactor_epoch,
            |c: &mut Compactions, e: u64| c.compactor_epoch = e,
        )
        .await?;
        Ok(Self { inner: fr })
    }

    pub(crate) async fn init_with_epoch(
        stored_compactions: StoredCompactions,
        compactions_update_timeout: Duration,
        system_clock: Arc<dyn SystemClock>,
        compactor_epoch: u64,
    ) -> Result<Self, SlateDBError> {
        let fr = FenceableTransactionalObject::init_with_epoch(
            stored_compactions.inner,
            compactions_update_timeout,
            system_clock,
            compactor_epoch,
            |c: &Compactions| c.compactor_epoch,
            |c: &mut Compactions, e: u64| c.compactor_epoch = e,
        )
        .await?;
        Ok(Self { inner: fr })
    }

    pub(crate) async fn refresh(&mut self) -> Result<&Compactions, SlateDBError> {
        Ok(self.inner.refresh().await?)
    }

    pub(crate) fn prepare_dirty(&self) -> Result<DirtyObject<Compactions>, SlateDBError> {
        Ok(self.inner.prepare_dirty()?)
    }

    pub(crate) async fn update(
        &mut self,
        dirty: DirtyObject<Compactions>,
    ) -> Result<(), SlateDBError> {
        Ok(self.inner.update(dirty).await?)
    }

    #[cfg(test)]
    pub(crate) async fn maybe_apply_update<F>(&mut self, mutator: F) -> Result<(), SlateDBError>
    where
        F: Fn(
                &FenceableTransactionalObject<Compactions>,
            ) -> Result<Option<DirtyObject<Compactions>>, SlateDBError>
            + Send
            + Sync,
    {
        Ok(self.inner.maybe_apply_update(mutator).await?)
    }
}

/// Represents the metadata of a compactions file stored in the object store.
#[derive(Serialize, Debug)]
#[allow(dead_code)]
pub(crate) struct CompactionsFileMetadata {
    pub(crate) id: u64,
    #[serde(serialize_with = "serialize_path")]
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: u32,
}

#[allow(dead_code)]
fn serialize_path<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(path.as_ref())
}

pub(crate) struct CompactionsStore {
    inner: Arc<dyn SequencedStorageProtocol<Compactions>>,
    compactions_dir: Path,
    cached_object_store: Option<Arc<CachedObjectStore>>,
}

impl CompactionsStore {
    pub(crate) fn new(root_path: &Path, object_store: Arc<dyn ObjectStore>) -> Self {
        let inner = Arc::new(ObjectStoreSequencedStorageProtocol::<Compactions>::new(
            root_path,
            object_store,
            COMPACTIONS_SUBDIR,
            COMPACTIONS_FILE_SUFFIX,
            Box::new(FlatBufferCompactionsCodec {}),
        ));
        Self {
            inner,
            compactions_dir: root_path.child(COMPACTIONS_SUBDIR),
            cached_object_store: None,
        }
    }

    /// Attach a `CachedObjectStore` so reads can self-heal from a corrupt
    /// cache entry (e.g. after an unclean shutdown) by invalidating it and
    /// refetching from the remote object store.
    pub(crate) fn with_cache_invalidator(
        mut self,
        cached_object_store: Arc<CachedObjectStore>,
    ) -> Self {
        self.cached_object_store = Some(cached_object_store);
        self
    }

    fn compactions_path(&self, id: MonotonicId) -> Path {
        self.compactions_dir
            .child(format!("{:020}.{}", id.id(), COMPACTIONS_FILE_SUFFIX))
    }

    async fn invalidate_on_checksum_mismatch(
        &self,
        id: MonotonicId,
        err: &TransactionalObjectError,
    ) -> bool {
        let Some(cache) = self.cached_object_store.as_ref() else {
            return false;
        };
        if !is_checksum_mismatch(err) {
            return false;
        }
        let path = self.compactions_path(id);
        warn!(
            "compactions failed checksum on read, invalidating cache and retrying once [id={}, path={}]",
            id.id(),
            path
        );
        if let Err(invalidate_err) = cache.invalidate(&path).await {
            warn!(
                "failed to invalidate cached compactions [path={}, error={}]",
                path, invalidate_err
            );
        }
        true
    }

    async fn try_read_with_retry(
        &self,
        id: MonotonicId,
    ) -> Result<Option<Compactions>, TransactionalObjectError> {
        match self.inner.try_read(id).await {
            Err(err) if self.invalidate_on_checksum_mismatch(id, &err).await => {
                self.inner.try_read(id).await
            }
            other => other,
        }
    }

    /// Delete a compactions file from the object store.
    pub(crate) async fn delete_compactions(&self, id: u64) -> Result<(), SlateDBError> {
        Ok(self.inner.delete(MonotonicId::new(id)).await?)
    }

    /// Read a compactions file from the object store. The last element in an unbounded
    /// range is the current compactions state.
    /// # Arguments
    /// * `id_range` - The range of IDs to list
    pub(crate) async fn list_compactions<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<CompactionsFileMetadata>, SlateDBError> {
        let compactions = self
            .inner
            .list(
                id_range.start_bound().map(|b| (*b).into()),
                id_range.end_bound().map(|b| (*b).into()),
            )
            .await?
            .into_iter()
            .map(|f| CompactionsFileMetadata {
                id: f.id.into(),
                location: f.location,
                last_modified: f.last_modified,
                size: f.size,
            })
            .collect::<Vec<_>>();
        Ok(compactions)
    }

    pub(crate) async fn try_read_latest_compactions(
        &self,
    ) -> Result<Option<VersionedCompactions>, SlateDBError> {
        // Mirror `ObjectStoreSequencedStorageProtocol::try_read_latest`:
        // list, take the newest id, read via the retry path. Re-list on
        // `Ok(None)` to handle the existing race with GC.
        loop {
            let files = self
                .inner
                .list(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
                .await?;
            let Some(file) = files.last() else {
                return Ok(None);
            };
            match self.try_read_with_retry(file.id).await? {
                Some(compactions) => {
                    return Ok(Some(VersionedCompactions::from_compactions(
                        file.id.into(),
                        compactions,
                    )));
                }
                None => {
                    warn!(
                        "listed compactions file missing on read, retrying [location={}]",
                        file.location
                    );
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn read_latest_compactions(
        &self,
    ) -> Result<VersionedCompactions, SlateDBError> {
        self.try_read_latest_compactions()
            .await?
            .ok_or(LatestTransactionalObjectVersionMissing)
    }

    #[allow(dead_code)]
    pub(crate) async fn try_read_compactions(
        &self,
        id: u64,
    ) -> Result<Option<Compactions>, SlateDBError> {
        Ok(self.try_read_with_retry(MonotonicId::new(id)).await?)
    }

    #[allow(dead_code)]
    pub(crate) async fn read_compactions(&self, id: u64) -> Result<Compactions, SlateDBError> {
        self.try_read_compactions(id)
            .await?
            .ok_or(LatestTransactionalObjectVersionMissing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor_state::{Compaction, CompactionSpec, SourceId};
    use crate::error;
    use crate::rand::DbRand;
    use crate::retrying_object_store::RetryingObjectStore;
    use crate::test_utils::FlakyObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use slatedb_common::clock::DefaultSystemClock;
    use std::time::Duration;
    use ulid::Ulid;

    const ROOT: &str = "/root/path";

    #[tokio::test]
    async fn test_should_fail_write_on_version_conflict() {
        let store = new_memory_compactions_store();
        let mut sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        let mut sc2 = StoredCompactions::load(store.clone()).await.unwrap();
        sc.update(sc.prepare_dirty().unwrap()).await.unwrap();

        let result = sc2.update(sc2.prepare_dirty().unwrap()).await;

        assert!(matches!(
            result.unwrap_err(),
            error::SlateDBError::TransactionalObjectVersionExists
        ));
    }

    #[tokio::test]
    async fn test_should_write_with_new_version() {
        let store = new_memory_compactions_store();
        let mut sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        sc.update(sc.prepare_dirty().unwrap()).await.unwrap();

        let version = store.read_latest_compactions().await.unwrap().id;

        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn test_should_update_local_state_on_write() {
        let store = new_memory_compactions_store();
        let mut sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        let compaction = new_compaction();
        let mut dirty = sc.prepare_dirty().unwrap();
        dirty.value.insert(compaction.clone());
        sc.update(dirty).await.unwrap();

        assert!(sc.compactions().contains(&compaction.id()));
    }

    #[tokio::test]
    async fn test_should_refresh() {
        let store = new_memory_compactions_store();
        let mut sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        let mut sc2 = StoredCompactions::load(store.clone()).await.unwrap();
        let compaction = new_compaction();
        let mut dirty = sc.prepare_dirty().unwrap();
        dirty.value.insert(compaction.clone());
        sc.update(dirty).await.unwrap();

        let refreshed = sc2.refresh().await.unwrap();

        assert!(refreshed.contains(&compaction.id()));
        assert!(sc2.compactions().contains(&compaction.id()));
    }

    #[tokio::test]
    async fn test_should_bump_compactor_epoch() {
        let store = new_memory_compactions_store();
        StoredCompactions::create(store.clone(), 0).await.unwrap();
        let timeout = Duration::from_secs(300);
        for i in 1..5 {
            let sc = StoredCompactions::load(store.clone()).await.unwrap();
            FenceableCompactions::init(sc, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
            let compactions = store.read_latest_compactions().await.unwrap();
            assert_eq!(compactions.compactions.compactor_epoch, i);
        }
    }

    #[tokio::test]
    async fn test_should_fail_refresh_on_compactor_fenced() {
        let store = new_memory_compactions_store();
        let sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        let timeout = Duration::from_secs(300);
        let mut compactor1 =
            FenceableCompactions::init(sc, timeout, Arc::new(DefaultSystemClock::new()))
                .await
                .unwrap();
        let sc2 = StoredCompactions::load(store.clone()).await.unwrap();

        FenceableCompactions::init(sc2, timeout, Arc::new(DefaultSystemClock::new()))
            .await
            .unwrap();

        let result = compactor1.refresh().await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_should_fail_state_update_when_fenced() {
        let store = new_memory_compactions_store();
        let sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        let timeout = Duration::from_secs(300);
        let mut fc1 = FenceableCompactions::init(sc, timeout, Arc::new(DefaultSystemClock::new()))
            .await
            .unwrap();
        let sc2 = StoredCompactions::load(store.clone()).await.unwrap();
        let mut fc2 = FenceableCompactions::init(sc2, timeout, Arc::new(DefaultSystemClock::new()))
            .await
            .unwrap();

        let result = fc1
            .maybe_apply_update(|compactions| {
                let mut dirty = compactions.prepare_dirty()?;
                dirty.value.compactor_epoch += 1;
                Ok(Some(dirty))
            })
            .await;

        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert_state_not_updated(&mut fc2).await;
    }

    #[tokio::test]
    async fn test_should_fail_write_of_stale_dirty_compactions() {
        let store = new_memory_compactions_store();
        let mut sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        let stale = sc.prepare_dirty().unwrap();
        sc.update(sc.prepare_dirty().unwrap()).await.unwrap();

        let result = sc.update(stale).await;

        assert!(matches!(
            result,
            Err(SlateDBError::TransactionalObjectVersionExists)
        ));
    }

    #[tokio::test]
    async fn test_list_compactions() {
        let store = new_memory_compactions_store();
        let mut sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        sc.update(sc.prepare_dirty().unwrap()).await.unwrap();

        // Check unbounded
        let compactions = store.list_compactions(..).await.unwrap();
        assert_eq!(compactions.len(), 2);
        assert_eq!(compactions[0].id, 1);
        assert_eq!(compactions[1].id, 2);

        // Check bounded
        let compactions = store.list_compactions(1..2).await.unwrap();
        assert_eq!(compactions.len(), 1);
        assert_eq!(compactions[0].id, 1);

        // Check left bounded
        let compactions = store.list_compactions(2..).await.unwrap();
        assert_eq!(compactions.len(), 1);
        assert_eq!(compactions[0].id, 2);

        // Check right bounded
        let compactions = store.list_compactions(..2).await.unwrap();
        assert_eq!(compactions.len(), 1);
        assert_eq!(compactions[0].id, 1);
    }

    #[tokio::test]
    async fn test_delete_compactions() {
        let store = new_memory_compactions_store();
        let mut sc = StoredCompactions::create(store.clone(), 0).await.unwrap();
        sc.update(sc.prepare_dirty().unwrap()).await.unwrap();
        let compactions = store.list_compactions(..).await.unwrap();
        assert_eq!(compactions.len(), 2);

        store.delete_compactions(1).await.unwrap();
        let compactions = store.list_compactions(..).await.unwrap();
        assert_eq!(compactions.len(), 1);
        assert_eq!(compactions[0].id, 2);
    }

    #[tokio::test]
    async fn test_retry_write_compactions_on_timeout() {
        // Given a flaky store that times out on the first write
        let base = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(base.clone(), 1));
        let retrying = Arc::new(RetryingObjectStore::new(
            flaky.clone(),
            Arc::new(DbRand::default()),
            Arc::new(DefaultSystemClock::new()),
        ));
        let store = Arc::new(CompactionsStore::new(&Path::from(ROOT), retrying.clone()));

        // When creating new compactions (initial write under retry)
        let _sc = StoredCompactions::create(store.clone(), 0).await.unwrap();

        // Then: a retry happened and the compactions match input
        assert!(flaky.put_attempts() >= 2);
        let written = store.try_read_compactions(1).await.unwrap().unwrap();
        assert_eq!(written.compactor_epoch, 0);
    }

    fn new_memory_compactions_store() -> Arc<CompactionsStore> {
        let os = Arc::new(InMemory::new());
        Arc::new(CompactionsStore::new(&Path::from(ROOT), os))
    }

    fn new_compaction() -> Compaction {
        Compaction::new(
            Ulid::new(),
            CompactionSpec::new(vec![SourceId::SortedRun(0)], 0),
        )
    }

    async fn assert_state_not_updated(fc: &mut FenceableCompactions) {
        let original_epoch = fc.inner.object().compactor_epoch;
        fc.refresh().await.unwrap();
        let refreshed_epoch = fc.inner.object().compactor_epoch;
        assert_eq!(refreshed_epoch, original_epoch);
    }
}
