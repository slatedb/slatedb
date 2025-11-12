//! SlateDB Transactional Object
//!
//! This module provides generic, reusable primitives for persisting data
//! as objects in a durable store, with optimistic concurrency control and optional
//! epoch-based fencing.
//!
//! Core types
//! ----------
//! - `TransactionalObjectOps<T, Id>`: A trait that defines how a transactional object should
//!   interact with backing storage. Implementations implement some protocol for providing
//!   transactional guarantees. The trait supports reading the latest object and its version id
//!   and writing a new version conditional on the current id.
//! - `SequencedObjectOps<T>`: Extends TransactionalObjectOps<T, MonotonicId> by requiring that
//!   the protocol persist objects as a series of versions with monotonically increasing IDs. This
//!   is useful if it's important to observe earlier versions of the object.
//! - `ObjectStoreSequencedObjectOps<T>`: Implements SequencedObjectOps<T> on Object Stores.
//! - `MonotonicId`: A monotonically increasing version ID.
//! - `SimpleTransactionalObject<T>`: In-memory view of the latest known `{ id, object }`. Supports:
//!   - `refresh()` to load the current latest version from storage
//!   - `update(DirtyObject<T>)` to perform a CAS write given the dirty object's version id
//!   - `maybe_apply_update(mutator)` to loop: mutate -> write -> on conflict refresh and retry
//! - `DirtyObject<T>`: A local, mutable candidate `{ id, value }` to be written.
//! - `FenceableTransactionalObject<T>`: Wraps `SimpleTransactionalObject<T>` and enforces epoch
//!   fencing for writers. On `init`, it bumps the epoch field (via provided `get_epoch`/`set_epoch`
//!   fns) and writes that update, fencing out stale writers. Subsequent operations check the stored
//!   epoch and return `Fenced` if the local epoch is behind.
//!
//! Error semantics
//! ---------------
//! - `FileVersionExists` is returned when a CAS write fails because a concurrent writer
//!   created the target id first. Callers typically handle this by `refresh()` and retrying.
//! - `InvalidDBState` may be returned when an expected record is missing or file names are
//!   malformed.
//!
//! Example (Manifest)
//! ------------------
//! A `ManifestStore` composes `ObjectStoreSequencedObjectOps<Manifest>` with suffix `"manifest"`.
//! File names look  like `00000000000000000001.manifest`, `00000000000000000002.manifest`,
//! etc. `StoredManifest` is a thin wrapper around `SimpleTransactionalObject<Manifest>` that adds
//! domain-specific helpers (e.g. checkpoint calculations) and maps generic CAS conflicts to
//! `ManifestVersionExists`.
//!
//! Concurrency
//! -----------
//! - Use `maybe_apply_update` for optimistic updates that automatically retry on conflicts.
//! - For writers that must be fenced, initialize a `FenceableTransactionalObject` to atomically
//!   bump the  epoch, then rely on `check_epoch` during subsequent updates.
//!
//! Timing and timeouts
//! -------------------
//! - Operations that must complete within a bounded time (like epoch bump on init) can be
//!   wrapped with `utils::timeout`, using the provided `SystemClock`.
//!
//! The goal is to keep this module fully generic and free of slatedb-specific logic; For example,
//! manifest semantics live in `manifest/store.rs` and use these primitives by delegation.

use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use log::{debug, warn};
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::{Error, ObjectStore, PutMode, PutOptions, PutPayload};
use std::ops::Bound::Unbounded;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::time::Duration;

use crate::clock::SystemClock;
use crate::error::SlateDBError;
use crate::error::SlateDBError::{FileVersionExists, InvalidDBState};
use crate::record::ObjectCodec;
use crate::utils;

/// A monotonically increasing object version ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MonotonicId {
    id: u64,
}

impl PartialEq<u64> for MonotonicId {
    fn eq(&self, other: &u64) -> bool {
        self.id == *other
    }
}

impl PartialEq<MonotonicId> for u64 {
    fn eq(&self, other: &MonotonicId) -> bool {
        *self == other.id
    }
}

impl std::ops::Add<u64> for MonotonicId {
    type Output = MonotonicId;

    fn add(self, rhs: u64) -> Self::Output {
        MonotonicId::new(self.id + rhs)
    }
}

impl MonotonicId {
    pub(crate) fn initial() -> Self {
        Self { id: 1 }
    }

    pub(crate) fn new(id: u64) -> Self {
        Self { id }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn next(&self) -> Self {
        Self { id: self.id + 1 }
    }
}

impl From<u64> for MonotonicId {
    fn from(id: u64) -> Self {
        Self::new(id)
    }
}

impl From<MonotonicId> for u64 {
    fn from(id: MonotonicId) -> Self {
        id.id
    }
}

/// Generic file metadata for versioned objects
#[derive(Debug)]
pub(crate) struct GenericObjectMetadata<Id: Copy = MonotonicId> {
    pub(crate) id: Id,
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: u32,
}

/// A local view of a transactional object, possibly with local mutations
#[derive(Clone, Debug)]
pub(crate) struct DirtyObject<T, Id: Copy = MonotonicId> {
    /// This ID of the object from which this `DirtyObject` was created
    id: Id,
    pub(crate) value: T,
}

impl<T, Id: Copy> DirtyObject<T, Id> {
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> Id {
        self.id
    }

    #[allow(dead_code)]
    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

/// An in-memory datum that is backed by durable storage and can be
/// transactionally updated.
#[async_trait::async_trait]
pub(crate) trait TransactionalObject<T: Clone, Id: Copy = MonotonicId> {
    /// Returns the version ID of the in-memory view of the object
    fn id(&self) -> Id;

    /// Returns the in-memory view of the object
    fn object(&self) -> &T;

    /// Returns a `DirtyObject` with the current version ID and object which can be
    /// modified locally and passed to `update` to persist mutations durably.
    fn prepare_dirty(&self) -> Result<DirtyObject<T, Id>, SlateDBError>;

    /// Refreshes the in-memory view of the object with the latest version stored durably.
    /// This may result in a different in-memory view returned by `object` (and different id
    /// returned by `id`) if another process has successfully updated the object.
    async fn refresh(&mut self) -> Result<&T, SlateDBError>;

    /// Transactionally update the object. Will succeed iff the version id in durable storage
    /// matches the version id of the provided `DirtyObject`. If the versions don't match
    /// then this fn returns `FileVersionExists`.
    async fn update(&mut self, dirty: DirtyObject<T, Id>) -> Result<(), SlateDBError>;

    /// Transactionally update the object using the supplied mutator, if the mutator returns
    /// `Some`. This fn will indefinitely retry the mutation on a version conflict by refreshing
    /// and re-applying the mutation.
    async fn maybe_apply_update<F>(&mut self, mutator: F) -> Result<(), SlateDBError>
    where
        F: Fn(&Self) -> Result<Option<DirtyObject<T, Id>>, SlateDBError> + Send + Sync,
    {
        loop {
            let Some(dirty) = mutator(self)? else {
                return Ok(());
            };
            match self.update(dirty).await {
                Err(SlateDBError::FileVersionExists) => {
                    self.refresh().await?;
                    continue;
                }
                Err(e) => return Err(e),
                Ok(()) => return Ok(()),
            }
        }
    }
}

/// Wraps `SimpleTransactionalObject` with epoch-based fencing to provide mutually-exclusive
/// access to the object. When creating a `FenceableTransactionalObject` the caller supplied
/// `get_epoch` and `set_epoch` fns for getting and setting the epoch in the contained object.
/// The epoch is a monotonically increasing u64. `set_epoch` is called from
/// `FenceableTransactionalObject#init` to set the epoch to the next value. Once the epoch is set
/// it is never reset. Before any update, and after every refresh, this type checks whether the
/// epoch stored in the object is higher than the epoch stored in `init`. If it is, then the
/// corresponding `update` or `refresh` fails with`Fenced`.
pub(crate) struct FenceableTransactionalObject<T: Clone, Id: Copy = MonotonicId> {
    delegate: SimpleTransactionalObject<T, Id>,
    local_epoch: u64,
    get_epoch: fn(&T) -> u64,
}

impl<T: Clone + Send + Sync> FenceableTransactionalObject<T, MonotonicId> {
    pub(crate) async fn init(
        mut delegate: SimpleTransactionalObject<T, MonotonicId>,
        object_update_timeout: Duration,
        system_clock: Arc<dyn SystemClock>,
        get_epoch: fn(&T) -> u64,
        set_epoch: fn(&mut T, u64),
    ) -> Result<Self, SlateDBError> {
        utils::timeout(
            system_clock.clone(),
            object_update_timeout,
            || SlateDBError::ManifestUpdateTimeout {
                timeout: object_update_timeout,
            },
            async {
                loop {
                    let local_epoch = get_epoch(delegate.object()) + 1;
                    let mut new_val = delegate.object().clone();
                    set_epoch(&mut new_val, local_epoch);
                    let mut dirty = delegate.prepare_dirty()?;
                    dirty.value = new_val;
                    match delegate.update(dirty).await {
                        Err(SlateDBError::FileVersionExists) => {
                            delegate.refresh().await?;
                            continue;
                        }
                        Err(err) => return Err(err),
                        Ok(()) => {
                            return Ok(Self {
                                delegate,
                                local_epoch,
                                get_epoch,
                            })
                        }
                    }
                }
            },
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn local_epoch(&self) -> u64 {
        self.local_epoch
    }

    #[allow(clippy::panic)]
    fn check_epoch(&self) -> Result<(), SlateDBError> {
        let stored_epoch = (self.get_epoch)(self.delegate.object());
        if self.local_epoch < stored_epoch {
            return Err(SlateDBError::Fenced);
        }
        if self.local_epoch > stored_epoch {
            panic!("the stored epoch is lower than the local epoch");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: Clone + Send + Sync> TransactionalObject<T>
    for FenceableTransactionalObject<T, MonotonicId>
{
    fn id(&self) -> MonotonicId {
        self.delegate.id()
    }

    fn object(&self) -> &T {
        self.delegate.object()
    }

    fn prepare_dirty(&self) -> Result<DirtyObject<T, MonotonicId>, SlateDBError> {
        self.check_epoch()?;
        self.delegate.prepare_dirty()
    }

    async fn refresh(&mut self) -> Result<&T, SlateDBError> {
        self.delegate.refresh().await?;
        self.check_epoch()?;
        Ok(self.object())
    }

    async fn update(&mut self, dirty: DirtyObject<T, MonotonicId>) -> Result<(), SlateDBError> {
        self.check_epoch()?;
        self.delegate.update(dirty).await
    }
}

/// A basic transactional object that uses `TransactionalObjectOps` to provide transactional
/// updates to an object.
#[derive(Clone)]
pub(crate) struct SimpleTransactionalObject<T, Id: Copy = MonotonicId> {
    id: Id,
    object: T,
    ops: Arc<dyn TransactionalObjectOps<T, Id>>,
}

impl<T: Clone, Id: Copy> SimpleTransactionalObject<T, Id> {
    pub(crate) async fn init(
        store: Arc<dyn TransactionalObjectOps<T, Id>>,
        value: T,
    ) -> Result<SimpleTransactionalObject<T, Id>, SlateDBError> {
        let id = store.write(None, &value).await?;
        Ok(SimpleTransactionalObject {
            id,
            object: value,
            ops: store,
        })
    }

    /// Attempts to load the latest object using the given `TransactionalObjectOps`.
    ///
    /// Returns `Ok(Some(SimpleTransactionalObject<T>))` when an object exists, or `Ok(None)` when
    /// no object is present in the store. This method does not create any new
    /// objects and is useful when callers need to proceed conditionally based on
    /// the presence of persisted state.
    ///
    /// For a variant that treats a missing record as an error, use [`load`], which
    /// maps the absence of a record to `SlateDBError::LatestRecordMissing`.
    pub(crate) async fn try_load(
        store: Arc<dyn TransactionalObjectOps<T, Id>>,
    ) -> Result<Option<SimpleTransactionalObject<T, Id>>, SlateDBError> {
        let Some((id, val)) = store.try_read_latest().await? else {
            return Ok(None);
        };
        Ok(Some(SimpleTransactionalObject {
            id,
            object: val,
            ops: store,
        }))
    }

    /// Load the current object using the supplied `TransactionalObjectOps`. If successful,
    /// this method returns a [`Result`] with an instance of [`SimpleTransactionalObject`].
    /// If no objects could be found, the error [`LatestRecordMissing`] is returned.
    #[allow(dead_code)]
    pub(crate) async fn load(
        store: Arc<dyn TransactionalObjectOps<T, Id>>,
    ) -> Result<SimpleTransactionalObject<T, Id>, SlateDBError> {
        Self::try_load(store)
            .await?
            .ok_or_else(|| SlateDBError::LatestRecordMissing)
    }
}

#[async_trait::async_trait]
impl<T: Clone + Send + Sync, Id: Copy + PartialEq + Send + Sync> TransactionalObject<T, Id>
    for SimpleTransactionalObject<T, Id>
{
    fn id(&self) -> Id {
        self.id
    }

    fn object(&self) -> &T {
        &self.object
    }

    fn prepare_dirty(&self) -> Result<DirtyObject<T, Id>, SlateDBError> {
        Ok(DirtyObject {
            id: self.id,
            value: self.object.clone(),
        })
    }

    async fn refresh(&mut self) -> Result<&T, SlateDBError> {
        let Some((id, new_val)) = self.ops.try_read_latest().await? else {
            return Err(InvalidDBState);
        };
        self.id = id;
        self.object = new_val;
        Ok(&self.object)
    }

    async fn update(&mut self, dirty: DirtyObject<T, Id>) -> Result<(), SlateDBError> {
        if dirty.id != self.id {
            return Err(FileVersionExists);
        }
        self.id = self.ops.write(Some(dirty.id), &dirty.value).await?;
        self.object = dirty.value;
        Ok(())
    }
}

/// Provides an abstraction for a protocol for transactionally writing an object in durable
/// storage. Reads return both the current value of the object and a version ID. Writes specify
/// the expected latest version ID and fail if the current version ID in durable storage does not
/// match.
#[async_trait]
pub(crate) trait TransactionalObjectOps<T, Id: Copy>: Send + Sync {
    /// Write the object given the expected current version ID. If the version ID is None then
    /// `write` expects that no object currently exists in durable storage. If the version condition
    /// fails then this fn returns `FileVersionExists`
    async fn write(&self, current_id: Option<Id>, new_value: &T) -> Result<Id, SlateDBError>;

    /// Read the latest version of the object and return it along with its version ID. If no
    /// object is found, returns `Ok(None)`
    async fn try_read_latest(&self) -> Result<Option<(Id, T)>, SlateDBError>;
}

/// Extends TransactionalObjectOps<T, MonotonicId> by requiring that the protocol persist objects
/// as a series of versions with monotonically increasing IDs. This is useful if it's important to
/// observe earlier versions of the object.
#[async_trait]
pub(crate) trait SequencedObjectOps<T>: TransactionalObjectOps<T, MonotonicId> {
    async fn try_read(&self, id: MonotonicId) -> Result<Option<T>, SlateDBError>;

    async fn list(
        &self,
        // use explicit from/to params here because RangeBounds is not object safe (so can't use
        // &dyn), and leaving the bound type as a type-parameter makes SequencedObjectOps not
        // object-safe
        from: Bound<MonotonicId>,
        to: Bound<MonotonicId>,
    ) -> Result<Vec<GenericObjectMetadata>, SlateDBError>;

    async fn delete(&self, id: MonotonicId) -> Result<(), SlateDBError>;
}

/// Implements `SequencedObjectOps<T>` on object storage.
///
/// File layout and naming
/// ----------------------
/// - Objects are stored under a root directory and logical subdirectory provided at
///   construction time (see `ObjectStoreSequencedObjectOps::new`).
/// - Each version is a single file whose name is a zero-padded 20-digit decimal id
///   followed by a fixed suffix, e.g. `00000000000000000001.manifest`.
/// - New versions must use the next consecutive id (`current_id + 1`).
/// - We rely on `put_if_not_exists` to enforce CAS at the storage layer. If a file with
///   the same id already exists, the write fails with `FileVersionExists`.
pub(crate) struct ObjectStoreSequencedObjectOps<T> {
    object_store: Box<dyn ObjectStore>,
    codec: Box<dyn ObjectCodec<T>>,
    file_suffix: &'static str,
}

impl<T> ObjectStoreSequencedObjectOps<T> {
    pub(crate) fn new(
        root_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        subdir: &str,
        file_suffix: &'static str,
        codec: Box<dyn ObjectCodec<T>>,
    ) -> Self {
        Self {
            object_store: Box::new(object_store::prefix::PrefixStore::new(
                object_store,
                root_path.child(subdir),
            )),
            codec,
            file_suffix,
        }
    }

    fn path_for(&self, id: MonotonicId) -> Path {
        Path::from(format!("{:020}.{}", id.id(), self.file_suffix))
    }

    fn parse_id(&self, path: &Path) -> Result<MonotonicId, SlateDBError> {
        match path.extension() {
            Some(ext) if ext == self.file_suffix => path
                .filename()
                .expect("invalid filename")
                .split('.')
                .next()
                .ok_or_else(|| InvalidDBState)?
                .parse()
                .map(MonotonicId::new)
                .map_err(|_| InvalidDBState),
            _ => Err(InvalidDBState),
        }
    }
}

#[async_trait]
impl<T: Send + Sync> TransactionalObjectOps<T, MonotonicId> for ObjectStoreSequencedObjectOps<T> {
    async fn write(
        &self,
        current_id: Option<MonotonicId>,
        new_value: &T,
    ) -> Result<MonotonicId, SlateDBError> {
        let id = current_id
            .map(|id| id.next())
            .unwrap_or(MonotonicId::initial());
        let path = self.path_for(id);
        self.object_store
            .put_opts(
                &path,
                PutPayload::from_bytes(self.codec.encode(new_value)),
                PutOptions::from(PutMode::Create),
            )
            .await
            .map_err(|err| {
                if let AlreadyExists { path: _, source: _ } = err {
                    SlateDBError::FileVersionExists
                } else {
                    SlateDBError::from(err)
                }
            })?;
        Ok(id)
    }

    async fn try_read_latest(&self) -> Result<Option<(MonotonicId, T)>, SlateDBError> {
        let files = self.list(Unbounded, Unbounded).await?;
        if let Some(file) = files.last() {
            return self
                .try_read(file.id)
                .await
                .map(|opt| opt.map(|v| (file.id, v)));
        }
        Ok(None)
    }
}

#[async_trait]
impl<T: Send + Sync> SequencedObjectOps<T> for ObjectStoreSequencedObjectOps<T> {
    async fn try_read(&self, id: MonotonicId) -> Result<Option<T>, SlateDBError> {
        let path = self.path_for(id);
        match self.object_store.get(&path).await {
            Ok(obj) => match obj.bytes().await {
                Ok(bytes) => self.codec.decode(&bytes).map(Some),
                Err(e) => Err(SlateDBError::from(e)),
            },
            Err(e) => match e {
                Error::NotFound { .. } => Ok(None),
                _ => Err(SlateDBError::from(e)),
            },
        }
    }

    // List files for this object type within an id range
    async fn list(
        &self,
        from: Bound<MonotonicId>,
        to: Bound<MonotonicId>,
    ) -> Result<Vec<GenericObjectMetadata>, SlateDBError> {
        let base = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(base));
        let mut items = Vec::new();
        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::from(e)),
        } {
            let id_range = (from, to);
            match self.parse_id(&file.location) {
                Ok(id) if id_range.contains(&id) => {
                    items.push(GenericObjectMetadata {
                        id,
                        location: file.location,
                        last_modified: file.last_modified,
                        size: file.size as u32,
                    });
                }
                Err(_) => warn!("unknown file in directory [location={:?}]", file.location),
                _ => {}
            }
        }
        items.sort_by_key(|m| m.id);
        Ok(items)
    }

    // Delete a specific versioned file (no additional validation)
    async fn delete(&self, id: MonotonicId) -> Result<(), SlateDBError> {
        let path = self.path_for(id);
        debug!("deleting object [record_path={}]", path);
        self.object_store
            .delete(&path)
            .await
            .map_err(SlateDBError::from)
    }
}

#[cfg(test)]
mod tests {
    use crate::clock::DefaultSystemClock;
    use crate::record::store::{
        FenceableTransactionalObject, MonotonicId, ObjectCodec, ObjectStoreSequencedObjectOps,
        SequencedObjectOps, SimpleTransactionalObject, TransactionalObject, TransactionalObjectOps,
    };
    use crate::record::SlateDBError;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::sync::Arc;
    use tokio::time::Duration as TokioDuration;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestVal {
        epoch: u64,
        payload: u64,
    }

    struct TestValCodec;

    impl ObjectCodec<TestVal> for TestValCodec {
        fn encode(&self, value: &TestVal) -> Bytes {
            // simple "epoch:payload" encoding
            Bytes::from(format!("{}:{}", value.epoch, value.payload))
        }

        fn decode(&self, bytes: &Bytes) -> Result<TestVal, SlateDBError> {
            let s = std::str::from_utf8(bytes).map_err(|_| SlateDBError::InvalidDBState)?;
            let mut parts = s.split(':');
            let epoch = parts
                .next()
                .ok_or(SlateDBError::InvalidDBState)?
                .parse()
                .map_err(|_| SlateDBError::InvalidDBState)?;
            let payload = parts
                .next()
                .ok_or(SlateDBError::InvalidDBState)?
                .parse()
                .map_err(|_| SlateDBError::InvalidDBState)?;
            Ok(TestVal { epoch, payload })
        }
    }

    fn new_store() -> Arc<ObjectStoreSequencedObjectOps<TestVal>> {
        let os = Arc::new(InMemory::new());
        Arc::new(ObjectStoreSequencedObjectOps::new(
            &Path::from("/root"),
            os,
            "test",
            "val",
            Box::new(TestValCodec),
        ))
    }

    #[tokio::test]
    async fn test_init_write_and_read_latest() {
        let store = new_store();
        let mut sr = SimpleTransactionalObject::<TestVal>::init(
            Arc::clone(&store) as Arc<dyn TransactionalObjectOps<TestVal, MonotonicId>>,
            TestVal {
                epoch: 0,
                payload: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(1, sr.id());
        assert_eq!(
            TestVal {
                epoch: 0,
                payload: 1
            },
            *sr.object()
        );

        // update to next id
        let mut dirty = sr.prepare_dirty().unwrap();
        dirty.value = TestVal {
            epoch: 0,
            payload: 2,
        };
        sr.update(dirty).await.unwrap();
        assert_eq!(2, sr.id());
        assert_eq!(
            TestVal {
                epoch: 0,
                payload: 2
            },
            *sr.object()
        );

        // try_read_latest matches stored
        let latest = store.try_read_latest().await.unwrap().unwrap();
        assert_eq!(2, latest.0);
        assert_eq!(
            TestVal {
                epoch: 0,
                payload: 2
            },
            latest.1
        );
    }

    #[tokio::test]
    async fn test_update_dirty_version_conflict() {
        let store = new_store();
        let mut a = SimpleTransactionalObject::<TestVal>::init(
            Arc::clone(&store) as Arc<dyn TransactionalObjectOps<TestVal, MonotonicId>>,
            TestVal {
                epoch: 0,
                payload: 10,
            },
        )
        .await
        .unwrap();

        // Create another view B from latest
        let (id_b, val_b) = store.try_read_latest().await.unwrap().unwrap();
        let mut b: SimpleTransactionalObject<TestVal> = SimpleTransactionalObject {
            id: id_b,
            object: val_b,
            ops: Arc::clone(&store) as Arc<dyn TransactionalObjectOps<TestVal, MonotonicId>>,
        };

        // A updates first
        let mut dirty = a.prepare_dirty().unwrap();
        dirty.value = TestVal {
            epoch: 0,
            payload: 11,
        };
        a.update(dirty).await.unwrap();

        // B attempts update based on stale id; maybe_apply_update should refresh and succeed
        b.maybe_apply_update(|sr| {
            let mut next = sr.object().clone();
            next.payload = 12;
            let mut dirty = sr.prepare_dirty().unwrap();
            dirty.value = next;
            Ok(Some(dirty))
        })
        .await
        .unwrap();

        let latest = store.try_read_latest().await.unwrap().unwrap();
        assert_eq!(
            TestVal {
                epoch: 0,
                payload: 12
            },
            latest.1
        );
    }

    #[tokio::test]
    async fn test_list_ranges_sorted() {
        let store = new_store();
        let mut sr = SimpleTransactionalObject::<TestVal>::init(
            Arc::clone(&store) as Arc<dyn TransactionalObjectOps<TestVal, MonotonicId>>,
            TestVal {
                epoch: 0,
                payload: 1,
            },
        )
        .await
        .unwrap();
        for p in 2..=4u64 {
            let mut dirty = sr.prepare_dirty().unwrap();
            dirty.value = TestVal {
                epoch: 0,
                payload: p,
            };
            sr.update(dirty).await.unwrap();
        }

        let all = store.list(Unbounded, Unbounded).await.unwrap();
        assert_eq!(4, all.len());
        assert!(all.windows(2).all(|w| w[0].id < w[1].id));

        let right_bounded = store.list(Unbounded, Excluded(3.into())).await.unwrap();
        assert_eq!(2, right_bounded.len());
        assert_eq!(1, right_bounded[0].id);
        assert_eq!(2, right_bounded[1].id);

        let left_bounded = store.list(Included(3.into()), Unbounded).await.unwrap();
        assert_eq!(2, left_bounded.len());
        assert_eq!(3, left_bounded[0].id);
        assert_eq!(4, left_bounded[1].id);
    }

    #[tokio::test]
    async fn test_update_dirty_id_mismatch_errors() {
        let store = new_store();
        let sr = SimpleTransactionalObject::<TestVal>::init(
            Arc::clone(&store) as Arc<dyn TransactionalObjectOps<TestVal, MonotonicId>>,
            TestVal {
                epoch: 0,
                payload: 1,
            },
        )
        .await
        .unwrap();
        // Force mismatch
        let mut dirty = sr.prepare_dirty().unwrap();
        dirty.value = TestVal {
            epoch: 0,
            payload: 2,
        };
        sr.ops.write(Some(dirty.id()), &dirty.value).await.unwrap();
        let err = sr
            .ops
            .write(Some(dirty.id()), &dirty.value)
            .await
            .unwrap_err();
        assert!(matches!(err, SlateDBError::FileVersionExists));
    }

    #[tokio::test]
    async fn test_fenceable_record_epoch_bump_and_fence() {
        let store = new_store();
        // initial record
        let sr = SimpleTransactionalObject::<TestVal>::init(
            Arc::clone(&store) as Arc<dyn TransactionalObjectOps<TestVal, MonotonicId>>,
            TestVal {
                epoch: 0,
                payload: 0,
            },
        )
        .await
        .unwrap();

        // writer A bumps to epoch 1
        let mut fa = FenceableTransactionalObject::init(
            sr.clone(),
            TokioDuration::from_secs(5),
            Arc::new(DefaultSystemClock::new()),
            |v: &TestVal| v.epoch,
            |v: &mut TestVal, e: u64| v.epoch = e,
        )
        .await
        .unwrap();

        let (_, v1) = store.try_read_latest().await.unwrap().unwrap();
        assert_eq!(1, v1.epoch);

        // writer B bumps to epoch 2
        let (id_b, val_b) = store.try_read_latest().await.unwrap().unwrap();
        let sb = SimpleTransactionalObject {
            id: id_b,
            object: val_b,
            ops: Arc::clone(&store) as Arc<dyn TransactionalObjectOps<TestVal, MonotonicId>>,
        };
        let mut fb = FenceableTransactionalObject::init(
            sb,
            TokioDuration::from_secs(5),
            Arc::new(DefaultSystemClock::new()),
            |v: &TestVal| v.epoch,
            |v: &mut TestVal, e: u64| v.epoch = e,
        )
        .await
        .unwrap();

        let (_, v2) = store.try_read_latest().await.unwrap().unwrap();
        assert_eq!(2, v2.epoch);

        // A is now fenced
        let res = fa.refresh().await;
        assert!(matches!(res, Err(SlateDBError::Fenced)));

        // B can refresh
        assert!(fb.refresh().await.is_ok());
    }
}
