//! SlateDB Transactional Object
//!
//! This module provides generic, reusable primitives for persisting data
//! as objects in a durable store, with optimistic concurrency control and optional
//! epoch-based fencing.
//!
//! Core types
//! ----------
//! - `ObjectStoreSequencedObjectOps<T>`: Implements a protocol for transactionally writing
//!   an object as a sequence of object version ids. Supports reading the latest or a given
//!   version id of an object, and then writing it under the condition that a newer version
//!   has not been written.
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
//! The goal is to keep this module fully generic and free of manifest-specific logic; all
//! manifest semantics live in `manifest/store.rs` and use these primitives by delegation.

use chrono::Utc;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use log::{debug, warn};
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::{Error, ObjectStore, PutMode, PutOptions, PutPayload};

use crate::clock::SystemClock;
use crate::error::SlateDBError;
use crate::error::SlateDBError::{FileVersionExists, InvalidDBState};
use crate::record::ObjectCodec;
use crate::utils;

// Generic file metadata for versioned records
#[derive(Debug)]
pub(crate) struct GenericObjectMetadata {
    pub(crate) id: u64,
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: u32,
}

// View of a transactional object with local mutations
#[derive(Clone, Debug)]
pub(crate) struct DirtyObject<T> {
    id: u64,
    pub(crate) value: T,
}

impl<T> DirtyObject<T> {
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    #[allow(dead_code)]
    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

// Generic fenceable wrapper of `SimpleTransactionalObject` using epoch getters/setters
pub(crate) struct FenceableTransactionalObject<T: Clone> {
    delegate: SimpleTransactionalObject<T>,
    local_epoch: u64,
    get_epoch: fn(&T) -> u64,
    #[allow(dead_code)]
    set_epoch: fn(&mut T, u64),
}

impl<T: Clone + Send + Sync> FenceableTransactionalObject<T> {
    pub(crate) async fn init(
        mut delegate: SimpleTransactionalObject<T>,
        record_update_timeout: Duration,
        system_clock: Arc<dyn SystemClock>,
        get_epoch: fn(&T) -> u64,
        set_epoch: fn(&mut T, u64),
    ) -> Result<Self, SlateDBError> {
        utils::timeout(
            system_clock.clone(),
            record_update_timeout,
            || SlateDBError::ManifestUpdateTimeout {
                timeout: record_update_timeout,
            },
            async {
                loop {
                    let local_epoch = get_epoch(delegate.object()) + 1;
                    let mut new_val = delegate.object().clone();
                    set_epoch(&mut new_val, local_epoch);
                    let mut dirty = delegate.prepare_dirty();
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
                                set_epoch,
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

    pub(crate) fn next_id(&self) -> u64 {
        self.delegate.next_id()
    }

    pub(crate) fn object(&self) -> &T {
        self.delegate.object()
    }

    pub(crate) fn prepare_dirty(&self) -> Result<DirtyObject<T>, SlateDBError> {
        self.check_epoch()?;
        Ok(self.delegate.prepare_dirty())
    }

    // The file may have been updated by a readers, another process, or
    // we may have gotten this error after successfully updating
    // if we failed to get the response. Either way, refresh
    // the file and try the bump again.
    pub(crate) async fn refresh(&mut self) -> Result<(), SlateDBError> {
        self.delegate.refresh().await?;
        self.check_epoch()
    }

    pub(crate) async fn update(&mut self, dirty: DirtyObject<T>) -> Result<(), SlateDBError> {
        self.check_epoch()?;
        self.delegate.update(dirty).await
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

// Basic transactional object
#[derive(Clone)]
pub(crate) struct SimpleTransactionalObject<T> {
    id: u64,
    record: T,
    ops: Arc<ObjectStoreSequencedObjectOps<T>>,
}

impl<T: Clone> SimpleTransactionalObject<T> {
    pub(crate) async fn init(
        store: Arc<ObjectStoreSequencedObjectOps<T>>,
        value: T,
    ) -> Result<Self, SlateDBError> {
        store.write(1, &value).await?;
        Ok(Self {
            id: 1,
            record: value,
            ops: store,
        })
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    pub(crate) fn object(&self) -> &T {
        &self.record
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.id + 1
    }

    /// Create a dirty snapshot of the current record for mutation-and-write flows
    pub(crate) fn prepare_dirty(&self) -> DirtyObject<T> {
        DirtyObject {
            id: self.id,
            value: self.record.clone(),
        }
    }

    /// Refreshes this `SimpleTransactionalObject` with the latest value from the backing store.
    ///
    /// On success, updates this instance's id and value to the latest persisted
    /// version and returns a reference to the updated value.
    ///
    /// Returns `SlateDBError::InvalidDBState` if no record currently exists in the
    /// store. This typically indicates that the record has not been initialized or
    /// the underlying data was removed unexpectedly.
    ///
    /// This method does not create any records; it is commonly used after a
    /// version conflict to load the current latest state before retrying.
    pub(crate) async fn refresh(&mut self) -> Result<&T, SlateDBError> {
        let Some((id, new_val)) = self.ops.try_read_latest().await? else {
            return Err(InvalidDBState);
        };
        self.id = id;
        self.record = new_val;
        Ok(&self.record)
    }

    /// Attempts to load the latest stored record from the given `ObjectStoreSequencedObjectOps`.
    ///
    /// Returns `Ok(Some(SimpleTransactionalObject<T>))` when a record exists, or `Ok(None)` when
    /// no records are present in the store. This method does not create any new
    /// records and is useful when callers need to proceed conditionally based on
    /// the presence of persisted state.
    ///
    /// For a variant that treats a missing record as an error, use [`load`], which
    /// maps the absence of a record to `SlateDBError::LatestRecordMissing`.
    pub(crate) async fn try_load(
        store: Arc<ObjectStoreSequencedObjectOps<T>>,
    ) -> Result<Option<Self>, SlateDBError> {
        let Some((id, val)) = store.try_read_latest().await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            id,
            record: val,
            ops: store,
        }))
    }

    /// Load the current record from the supplied record store. If successful,
    /// this method returns a [`Result`] with an instance of [`SimpleTransactionalObject`].
    /// If no records could be found, the error [`LatestRecordMissing`] is returned.
    #[allow(dead_code)]
    pub(crate) async fn load(
        store: Arc<ObjectStoreSequencedObjectOps<T>>,
    ) -> Result<Self, SlateDBError> {
        Self::try_load(store)
            .await?
            .ok_or_else(|| SlateDBError::LatestRecordMissing)
    }

    pub(crate) async fn update(&mut self, dirty: DirtyObject<T>) -> Result<(), SlateDBError> {
        if dirty.id != self.id {
            return Err(FileVersionExists);
        }
        let next = self.next_id();
        self.ops.write(next, &dirty.value).await?;
        self.id = next;
        self.record = dirty.value;
        Ok(())
    }

    pub(crate) async fn maybe_apply_update<F>(&mut self, mutator: F) -> Result<(), SlateDBError>
    where
        F: Fn(&SimpleTransactionalObject<T>) -> Result<Option<DirtyObject<T>>, SlateDBError>,
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

// Implements basic operations on transactional objects using a log-based algorithm.
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
            .put_opts(
                &path,
                PutPayload::from_bytes(self.codec.encode(value)),
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
        Ok(())
    }

    pub(crate) async fn try_read(&self, id: u64) -> Result<Option<T>, SlateDBError> {
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

    /// lists unbounded (`..`), takes the last entry (highest id), and reads it.
    pub(crate) async fn try_read_latest(&self) -> Result<Option<(u64, T)>, SlateDBError> {
        let files = self.list(..).await?;
        if let Some(file) = files.last() {
            return self
                .try_read(file.id)
                .await
                .map(|opt| opt.map(|v| (file.id, v)));
        }
        Ok(None)
    }

    /// scans the subdirectory, parses ids from filenames  matching the configured suffix,
    /// filters by `RangeBounds`, sorts ascending by id, and returns metadata.
    pub(crate) async fn list<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<GenericObjectMetadata>, SlateDBError> {
        let base = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(base));
        let mut items = Vec::new();
        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::from(e)),
        } {
            match self.parse_id(&file.location) {
                Ok(id) if id_range.contains(&id) => {
                    items.push(GenericObjectMetadata {
                        id,
                        location: file.location,
                        last_modified: file.last_modified,
                        size: file.size as u32,
                    });
                }
                Err(_) => warn!(
                    "unknown file in record directory [location={:?}]",
                    file.location
                ),
                _ => {}
            }
        }
        items.sort_by_key(|m| m.id);
        Ok(items)
    }

    // Delete a specific versioned file (no additional validation)
    pub(crate) async fn delete(&self, id: u64) -> Result<(), SlateDBError> {
        let path = self.path_for(id);
        debug!("deleting record [record_path={}]", path);
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
        FenceableTransactionalObject, ObjectCodec, ObjectStoreSequencedObjectOps,
        SimpleTransactionalObject,
    };
    use crate::record::SlateDBError;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
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
        let mut sr = SimpleTransactionalObject::init(
            Arc::clone(&store),
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
        let mut dirty = sr.prepare_dirty();
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
        let mut a = SimpleTransactionalObject::init(
            Arc::clone(&store),
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
            record: val_b,
            ops: Arc::clone(&store),
        };

        // A updates first
        let mut dirty = a.prepare_dirty();
        dirty.value = TestVal {
            epoch: 0,
            payload: 11,
        };
        a.update(dirty).await.unwrap();

        // B attempts update based on stale id; maybe_apply_update should refresh and succeed
        b.maybe_apply_update(|sr| {
            let mut next = sr.object().clone();
            next.payload = 12;
            let mut dirty = sr.prepare_dirty();
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
        let mut sr = SimpleTransactionalObject::init(
            Arc::clone(&store),
            TestVal {
                epoch: 0,
                payload: 1,
            },
        )
        .await
        .unwrap();
        for p in 2..=4u64 {
            let mut dirty = sr.prepare_dirty();
            dirty.value = TestVal {
                epoch: 0,
                payload: p,
            };
            sr.update(dirty).await.unwrap();
        }

        let all = store.list(..).await.unwrap();
        assert_eq!(4, all.len());
        assert!(all.windows(2).all(|w| w[0].id < w[1].id));

        let right_bounded = store.list(..3).await.unwrap();
        assert_eq!(2, right_bounded.len());
        assert_eq!(1, right_bounded[0].id);
        assert_eq!(2, right_bounded[1].id);

        let left_bounded = store.list(3..).await.unwrap();
        assert_eq!(2, left_bounded.len());
        assert_eq!(3, left_bounded[0].id);
        assert_eq!(4, left_bounded[1].id);
    }

    #[tokio::test]
    async fn test_update_dirty_id_mismatch_errors() {
        let store = new_store();
        let sr = SimpleTransactionalObject::init(
            Arc::clone(&store),
            TestVal {
                epoch: 0,
                payload: 1,
            },
        )
        .await
        .unwrap();
        // Force mismatch
        let mut dirty = sr.prepare_dirty();
        dirty.value = TestVal {
            epoch: 0,
            payload: 2,
        };
        let err = sr.ops.write(dirty.id(), &dirty.value).await.unwrap_err();
        assert!(matches!(err, SlateDBError::FileVersionExists));
    }

    #[tokio::test]
    async fn test_fenceable_record_epoch_bump_and_fence() {
        let store = new_store();
        // initial record
        let sr = SimpleTransactionalObject::init(
            Arc::clone(&store),
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
            record: val_b,
            ops: Arc::clone(&store),
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
