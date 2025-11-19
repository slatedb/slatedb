//! # SlateDB Transactional Object
//!
//! This module provides generic, reusable primitives for reading/writing an object in a durable
//! store, with optimistic concurrency control and optional epoch-based fencing.
//!
//! The main interface callers will interact with is `TransactionalObject`. A `TransactionalObject`
//! is an in-memory register representing a view of an object stored in durable storage. Further,
//! it supports atomic updates that are isolated from concurrent updates to durable storage.
//! Atomicity/isolation are implemented using some protocol on the durable storage system. The
//! specific protocol and storage system is flexible. The operations the protocol+storage system
//! must support are defined by the `TransactionalStorageProtocol`. Callers provide an
//! implementation of `TransactionalStorageProtocol` when creating transactional objects.
//!
//! ## Core types
//! - `TransactionalStorageProtocol<T, Id>`: A trait that defines how a transactional object
//!   interacts with backing storage. Implementations implement some protocol for providing
//!   transactional guarantees. The trait supports reading the latest object and its version id
//!   and writing a new version conditional on the existing stored version matching the current id.
//! - `SequencedStorageProtocol<T>`: Extends TransactionalStorageProtocol<T, MonotonicId> by
//!   requiring that the protocol persist objects as a series of versions with monotonically
//!   increasing IDs. This  is useful if it's important to observe earlier versions of the object.
//! - `ObjectStoreSequencedStorageProtocol<T>`: Implements SequencedStorageProtocol<T> on
//!   Object Stores.
//! - `MonotonicId`: A monotonically increasing version ID.
//! - `TransactionalObject`: An in-memory register that is backed by durable storage and can be
//!   transactionally updated. Supports:
//!     - `refresh()` to load the current latest version of the object from storage
//!     - `update(DirtyObject<T>)` to perform a write conditional on the dirty object's version id
//!     - `maybe_apply_update(mutator)` to loop: mutate -> write -> on conflict refresh and retry
//! - `SimpleTransactionalObject<T>`: Implements `TransactionalObject` using a provided
//!   `TransactionalStorageProtocol`.
//! - `FenceableTransactionalObject<T>`: Wraps `SimpleTransactionalObject<T>` and enforces epoch
//!   fencing for writers. On `init`, it bumps the epoch field (via provided `get_epoch`/`set_epoch`
//!   fns) and writes that update, fencing out stale writers. Subsequent operations check the stored
//!   epoch and return `Fenced` if the local epoch is behind.
//! - `DirtyObject<T>`: A local, mutable candidate `{ id, value }` to be written. Callers get an
//!   instance of `DirtyObject` by calling `TransactionalObject#prepare_dirty`. They can then apply
//!   local mutations and persist them by calling `TransactionalObject#update`.
//!
//! ## Error semantics
//! - `ObjectVersionExists` is returned when a CAS write fails because a concurrent writer
//!   created the target id first. Callers typically handle this by `refresh()` and retrying.
//! - `InvalidState` may be returned when an expected record is missing or file names are
//!   malformed.
//!
//! ## Example (Manifest)
//! A `ManifestStore` composes `ObjectStoreSequencedStorageProtocol<Manifest>` with suffix
//! `"manifest"`.
//! File names look  like `00000000000000000001.manifest`, `00000000000000000002.manifest`,
//! etc. `StoredManifest` is a thin wrapper around `SimpleTransactionalObject<Manifest>` that adds
//! domain-specific helpers (e.g. checkpoint calculations) and maps generic CAS conflicts to
//! `ManifestVersionExists`.
//!
//! The goal is to keep this module fully generic and free of slatedb-specific logic; For example,
//! manifest semantics live in `manifest/store.rs` and use these primitives by delegation.

pub(crate) mod object_store;
pub(crate) mod view;

use crate::clock::SystemClock;
use crate::transactional_object::TransactionalObjectError::CallbackError;
use crate::utils;
use ::object_store::path::Path;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub(crate) enum TransactionalObjectError {
    #[error("io error")]
    IoError(#[from] Arc<std::io::Error>),

    #[error("object store error")]
    ObjectStoreError(#[from] ::object_store::Error),

    #[error("object update timed out")]
    ObjectUpdateTimeout { timeout: Duration },

    #[error("failed to find latest record")]
    LatestRecordMissing,

    #[error("object version exists")]
    ObjectVersionExists,

    #[error("detected newer client")]
    Fenced,

    #[error("object in store is in an unexpected state")]
    InvalidObjectState,

    // used to pass through errors from callbacks like codecs and mutators
    #[error("callback error")]
    CallbackError(Box<dyn std::error::Error + Send + Sync>),
}

// Generic codec to serialize/deserialize versioned records stored as files
pub(crate) trait ObjectCodec<T>: Send + Sync {
    fn encode(&self, value: &T) -> Bytes;
    fn decode(&self, bytes: &Bytes) -> Result<T, Box<dyn std::error::Error + Send + Sync>>;
}

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
    fn prepare_dirty(&self) -> Result<DirtyObject<T, Id>, TransactionalObjectError>;

    /// Refreshes the in-memory view of the object with the latest version stored durably.
    /// This may result in a different in-memory view returned by `object` (and different id
    /// returned by `id`) if another process has successfully updated the object.
    async fn refresh(&mut self) -> Result<&T, TransactionalObjectError>;

    /// Transactionally update the object. Will succeed iff the version id in durable storage
    /// matches the version id of the provided `DirtyObject`. If the versions don't match
    /// then this fn returns `ObjectVersionExists`.
    async fn update(&mut self, dirty: DirtyObject<T, Id>) -> Result<(), TransactionalObjectError>;

    /// Transactionally update the object using the supplied mutator, if the mutator returns
    /// `Some`. This fn will indefinitely retry the mutation on a version conflict by refreshing
    /// and re-applying the mutation.
    async fn maybe_apply_update<F, Err>(
        &mut self,
        mutator: F,
    ) -> Result<(), TransactionalObjectError>
    where
        Err: std::error::Error + Send + Sync + 'static,
        F: Fn(&Self) -> Result<Option<DirtyObject<T, Id>>, Err> + Send + Sync,
    {
        loop {
            let Some(dirty) = mutator(self).map_err(|e| CallbackError(Box::new(e)))? else {
                return Ok(());
            };
            match self.update(dirty).await {
                Err(TransactionalObjectError::ObjectVersionExists) => {
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
    ) -> Result<Self, TransactionalObjectError> {
        utils::timeout(
            system_clock.clone(),
            object_update_timeout,
            || TransactionalObjectError::ObjectUpdateTimeout {
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
                        Err(TransactionalObjectError::ObjectVersionExists) => {
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
    fn check_epoch(&self) -> Result<(), TransactionalObjectError> {
        let stored_epoch = (self.get_epoch)(self.delegate.object());
        if self.local_epoch < stored_epoch {
            return Err(TransactionalObjectError::Fenced);
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

    fn prepare_dirty(&self) -> Result<DirtyObject<T, MonotonicId>, TransactionalObjectError> {
        self.check_epoch()?;
        self.delegate.prepare_dirty()
    }

    async fn refresh(&mut self) -> Result<&T, TransactionalObjectError> {
        self.delegate.refresh().await?;
        self.check_epoch()?;
        Ok(self.object())
    }

    async fn update(
        &mut self,
        dirty: DirtyObject<T, MonotonicId>,
    ) -> Result<(), TransactionalObjectError> {
        self.check_epoch()?;
        self.delegate.update(dirty).await
    }
}

/// A basic transactional object that uses `TransactionalStorageProtocol` to provide transactional
/// updates to an object.
#[derive(Clone)]
pub(crate) struct SimpleTransactionalObject<T, Id: Copy = MonotonicId> {
    id: Id,
    object: T,
    ops: Arc<dyn TransactionalStorageProtocol<T, Id>>,
}

impl<T: Clone, Id: Copy> SimpleTransactionalObject<T, Id> {
    pub(crate) async fn init(
        store: Arc<dyn TransactionalStorageProtocol<T, Id>>,
        value: T,
    ) -> Result<SimpleTransactionalObject<T, Id>, TransactionalObjectError> {
        let id = store.write(None, &value).await?;
        Ok(SimpleTransactionalObject {
            id,
            object: value,
            ops: store,
        })
    }

    /// Attempts to load the latest object using the given `TransactionalStorageProtocol`.
    ///
    /// Returns `Ok(Some(SimpleTransactionalObject<T>))` when an object exists, or `Ok(None)` when
    /// no object is present in the store. This method does not create any new
    /// objects and is useful when callers need to proceed conditionally based on
    /// the presence of persisted state.
    ///
    /// For a variant that treats a missing record as an error, use [`load`], which
    /// maps the absence of a record to `TransactionalObjectError::LatestRecordMissing`.
    pub(crate) async fn try_load(
        store: Arc<dyn TransactionalStorageProtocol<T, Id>>,
    ) -> Result<Option<SimpleTransactionalObject<T, Id>>, TransactionalObjectError> {
        let Some((id, val)) = store.try_read_latest().await? else {
            return Ok(None);
        };
        Ok(Some(SimpleTransactionalObject {
            id,
            object: val,
            ops: store,
        }))
    }

    /// Load the current object using the supplied `TransactionalStorageProtocol`. If successful,
    /// this method returns a [`Result`] with an instance of [`SimpleTransactionalObject`].
    /// If no objects could be found, the error [`LatestRecordMissing`] is returned.
    #[allow(dead_code)]
    pub(crate) async fn load(
        store: Arc<dyn TransactionalStorageProtocol<T, Id>>,
    ) -> Result<SimpleTransactionalObject<T, Id>, TransactionalObjectError> {
        Self::try_load(store)
            .await?
            .ok_or_else(|| TransactionalObjectError::LatestRecordMissing)
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

    fn prepare_dirty(&self) -> Result<DirtyObject<T, Id>, TransactionalObjectError> {
        Ok(DirtyObject {
            id: self.id,
            value: self.object.clone(),
        })
    }

    async fn refresh(&mut self) -> Result<&T, TransactionalObjectError> {
        let Some((id, new_val)) = self.ops.try_read_latest().await? else {
            return Err(TransactionalObjectError::InvalidObjectState);
        };
        self.id = id;
        self.object = new_val;
        Ok(&self.object)
    }

    async fn update(&mut self, dirty: DirtyObject<T, Id>) -> Result<(), TransactionalObjectError> {
        if dirty.id != self.id {
            return Err(TransactionalObjectError::ObjectVersionExists);
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
pub(crate) trait TransactionalStorageProtocol<T, Id: Copy>: Send + Sync {
    /// Write the object given the expected current version ID. If the version ID is None then
    /// `write` expects that no object currently exists in durable storage. If the version condition
    /// fails then this fn returns `ObjectVersionExists`
    async fn write(
        &self,
        current_id: Option<Id>,
        new_value: &T,
    ) -> Result<Id, TransactionalObjectError>;

    /// Read the latest version of the object and return it along with its version ID. If no
    /// object is found, returns `Ok(None)`
    async fn try_read_latest(&self) -> Result<Option<(Id, T)>, TransactionalObjectError>;
}

/// Extends TransactionalStorageProtocol<T, MonotonicId> by requiring that the protocol persist objects
/// as a series of versions with monotonically increasing IDs. This is useful if it's important to
/// observe earlier versions of the object.
#[async_trait]
pub(crate) trait SequencedStorageProtocol<T>:
    TransactionalStorageProtocol<T, MonotonicId>
{
    async fn try_read(&self, id: MonotonicId) -> Result<Option<T>, TransactionalObjectError>;

    async fn list(
        &self,
        // use explicit from/to params here because RangeBounds is not object safe (so can't use
        // &dyn), and leaving the bound type as a type-parameter makes SequencedStorageProtocol not
        // object-safe
        from: Bound<MonotonicId>,
        to: Bound<MonotonicId>,
    ) -> Result<Vec<GenericObjectMetadata>, TransactionalObjectError>;

    async fn delete(&self, id: MonotonicId) -> Result<(), TransactionalObjectError>;
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::transactional_object::DirtyObject;

    pub(crate) fn new_dirty_object<T>(id: u64, value: T) -> DirtyObject<T> {
        DirtyObject {
            id: id.into(),
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::clock::DefaultSystemClock;
    use crate::transactional_object::object_store::ObjectStoreSequencedStorageProtocol;
    use crate::transactional_object::TransactionalObjectError;
    use crate::transactional_object::{
        FenceableTransactionalObject, MonotonicId, ObjectCodec, SimpleTransactionalObject,
        TransactionalObject, TransactionalStorageProtocol,
    };
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc;
    use tokio::time::Duration as TokioDuration;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(in crate::transactional_object) struct TestVal {
        pub(in crate::transactional_object) epoch: u64,
        pub(in crate::transactional_object) payload: u64,
    }

    pub(in crate::transactional_object) struct TestValCodec;

    impl ObjectCodec<TestVal> for TestValCodec {
        fn encode(&self, value: &TestVal) -> Bytes {
            // simple "epoch:payload" encoding
            Bytes::from(format!("{}:{}", value.epoch, value.payload))
        }

        fn decode(
            &self,
            bytes: &Bytes,
        ) -> Result<TestVal, Box<dyn std::error::Error + Send + Sync>> {
            let s = std::str::from_utf8(bytes).unwrap();
            let mut parts = s.split(':');
            let epoch = parts.next().unwrap().parse().unwrap();
            let payload = parts.next().unwrap().parse().unwrap();
            Ok(TestVal { epoch, payload })
        }
    }

    pub(in crate::transactional_object) fn new_store(
    ) -> Arc<ObjectStoreSequencedStorageProtocol<TestVal>> {
        let os = Arc::new(InMemory::new());
        Arc::new(ObjectStoreSequencedStorageProtocol::new(
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
            Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<TestVal, MonotonicId>>,
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
            Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<TestVal, MonotonicId>>,
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
            ops: Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<TestVal, MonotonicId>>,
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
            Ok::<_, TransactionalObjectError>(Some(dirty))
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
    async fn test_update_dirty_id_mismatch_errors() {
        let store = new_store();
        let sr = SimpleTransactionalObject::<TestVal>::init(
            Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<TestVal, MonotonicId>>,
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
        assert!(matches!(err, TransactionalObjectError::ObjectVersionExists));
    }

    #[tokio::test]
    async fn test_fenceable_record_epoch_bump_and_fence() {
        let store = new_store();
        // initial record
        let sr = SimpleTransactionalObject::<TestVal>::init(
            Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<TestVal, MonotonicId>>,
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
            ops: Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<TestVal, MonotonicId>>,
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
        assert!(matches!(res, Err(TransactionalObjectError::Fenced)));

        // B can refresh
        assert!(fb.refresh().await.is_ok());
    }
}
