//! SlateDB Generic Record Store
//!
//! This module provides generic, reusable primitives for persisting versioned records
//! as flat files in an object store, with optimistic concurrency control and optional
//! epoch-based fencing.
//!
//! File layout and naming
//! ----------------------
//! - Records are stored under a root directory and logical subdirectory provided at
//!   construction time (see `RecordStore::new`).
//! - Each version is a single file whose name is a zero-padded 20-digit decimal id
//!   followed by a fixed suffix, e.g. `00000000000000000001.manifest`.
//! - New versions must use the next consecutive id (`current_id + 1`).
//! - We rely on `put_if_not_exists` to enforce CAS at the storage layer. If a file with
//!   the same id already exists, the write fails with `FileVersionExists`.
//!
//! Core types
//! ----------
//! - `RecordStore<T>`: Owns the object store handle, codec, file suffix, and clock. It
//!   can `write(id, &T)`, `try_read(id)`, `try_read_latest()`, `list(range)`, and
//!   `delete(id)`.
//! - `StoredRecord<T>`: In-memory view of the latest known `{ id, record }`. Supports:
//!   - `refresh()` to load the current latest version from storage
//!   - `update(DirtyRecord<T>)` to perform a CAS write to `next_id()`
//!   - `maybe_apply_update(mutator)` to loop: mutate -> write -> on conflict refresh and retry
//! - `DirtyRecord<T>`: A local, mutable candidate `{ id, value }` to be written.
//! - `FenceableRecord<T>`: Wraps `StoredRecord<T>` and enforces epoch fencing for writers.
//!   On `init`, it bumps the epoch field (via provided `get_epoch`/`set_epoch` fns) and writes
//!   that update, fencing out stale writers. Subsequent operations check the stored epoch and
//!   return `Fenced` if the local epoch is behind.
//!
//! Error semantics
//! ---------------
//! - `FileVersionExists` is returned when a CAS write fails because a concurrent writer
//!   created the target id first. Callers typically handle this by `refresh()` and retrying.
//! - `InvalidDBState` may be returned when an expected record is missing or file names are
//!   malformed.
//!
//! Listing and latest
//! ------------------
//! - `RecordStore::list(range)` scans the subdirectory, parses ids from filenames matching the
//!   configured suffix, filters by `RangeBounds`, sorts ascending by id, and returns metadata.
//! - `try_read_latest()` lists unbounded (`..`), takes the last entry (highest id), and reads it.
//!
//! Example (Manifest)
//! ------------------
//! A `ManifestStore` composes `RecordStore<Manifest>` with suffix `"manifest"`. File names look
//! like `00000000000000000001.manifest`, `00000000000000000002.manifest`, etc. `StoredManifest`
//! is a thin wrapper around `StoredRecord<Manifest>` that adds domain-specific helpers (e.g.
//! checkpoint calculations) and maps generic CAS conflicts to `ManifestVersionExists`.
//!
//! Concurrency
//! -----------
//! - Use `maybe_apply_update` for optimistic updates that automatically retry on conflicts.
//! - For writers that must be fenced, initialize a `FenceableRecord` to atomically bump the
//!   epoch, then rely on `check_epoch` during subsequent updates.
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
use object_store::{Error, ObjectStore};

use crate::clock::SystemClock;
use crate::error::SlateDBError;
use crate::error::SlateDBError::{FileVersionExists, InvalidDBState};
use crate::record::RecordCodec;
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
use crate::utils;

// Generic file metadata for versioned records
#[derive(Debug)]
pub(crate) struct GenericFileMetadata {
    pub(crate) id: u64,
    pub(crate) location: Path,
    pub(crate) last_modified: chrono::DateTime<Utc>,
    #[allow(dead_code)]
    pub(crate) size: u32,
}

// Local, mutable view of a versioned record
#[derive(Clone, Debug)]
pub(crate) struct DirtyRecord<T> {
    id: u64,
    pub(crate) value: T,
}

// Generic fenceable wrapper using epoch getters/setters
pub(crate) struct FenceableRecord<T: Clone> {
    stored: StoredRecord<T>,
    local_epoch: u64,
    get_epoch: fn(&T) -> u64,
    #[allow(dead_code)]
    set_epoch: fn(&mut T, u64),
}

// Generic stored record with optimistic versioned updates
#[derive(Clone)]
pub(crate) struct StoredRecord<T> {
    id: u64,
    record: T,
    store: Arc<RecordStore<T>>,
}

// Generic, versioned store for records persisted as numbered files under a directory
pub(crate) struct RecordStore<T> {
    object_store: Box<dyn TransactionalObjectStore>,
    codec: Box<dyn RecordCodec<T>>,
    file_suffix: &'static str,
}

impl<T> DirtyRecord<T> {
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    #[allow(dead_code)]
    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

impl<T: Clone + Send + Sync> FenceableRecord<T> {
    pub(crate) async fn init(
        mut stored: StoredRecord<T>,
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
                    let local_epoch = get_epoch(stored.record()) + 1;
                    let mut new_val = stored.record().clone();
                    set_epoch(&mut new_val, local_epoch);
                    let mut dirty = stored.prepare_dirty();
                    dirty.value = new_val;
                    match stored.update(dirty).await {
                        Err(SlateDBError::FileVersionExists) => {
                            stored.refresh().await?;
                            continue;
                        }
                        Err(err) => return Err(err),
                        Ok(()) => {
                            return Ok(Self {
                                stored,
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
        self.stored.next_id()
    }

    pub(crate) fn record(&self) -> &T {
        self.stored.record()
    }

    pub(crate) fn prepare_dirty(&self) -> Result<DirtyRecord<T>, SlateDBError> {
        self.check_epoch()?;
        Ok(self.stored.prepare_dirty())
    }

    // The file may have been updated by a readers, another process, or
    // we may have gotten this error after successfully updating
    // if we failed to get the response. Either way, refresh
    // the file and try the bump again.
    pub(crate) async fn refresh(&mut self) -> Result<(), SlateDBError> {
        self.stored.refresh().await?;
        self.check_epoch()
    }

    pub(crate) async fn update(&mut self, dirty: DirtyRecord<T>) -> Result<(), SlateDBError> {
        self.check_epoch()?;
        self.stored.update(dirty).await
    }

    #[allow(clippy::panic)]
    fn check_epoch(&self) -> Result<(), SlateDBError> {
        let stored_epoch = (self.get_epoch)(self.stored.record());
        if self.local_epoch < stored_epoch {
            return Err(SlateDBError::Fenced);
        }
        if self.local_epoch > stored_epoch {
            panic!("the stored epoch is lower than the local epoch");
        }
        Ok(())
    }
}

impl<T: Clone> StoredRecord<T> {
    pub(crate) async fn init(store: Arc<RecordStore<T>>, value: T) -> Result<Self, SlateDBError> {
        store.write(1, &value).await?;
        Ok(Self {
            id: 1,
            record: value,
            store,
        })
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    pub(crate) fn record(&self) -> &T {
        &self.record
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.id + 1
    }

    /// Create a dirty snapshot of the current record for mutation-and-write flows
    pub(crate) fn prepare_dirty(&self) -> DirtyRecord<T> {
        DirtyRecord {
            id: self.id,
            value: self.record.clone(),
        }
    }

    pub(crate) async fn refresh(&mut self) -> Result<&T, SlateDBError> {
        let Some((id, new_val)) = self.store.try_read_latest().await? else {
            return Err(InvalidDBState);
        };
        self.id = id;
        self.record = new_val;
        Ok(&self.record)
    }

    pub(crate) async fn try_load(store: Arc<RecordStore<T>>) -> Result<Option<Self>, SlateDBError> {
        let Some((id, val)) = store.try_read_latest().await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            id,
            record: val,
            store,
        }))
    }

    /// Load the current record from the supplied record store. If successful,
    /// this method returns a [`Result`] with an instance of [`StoredRecord`].
    /// If no records could be found, the error [`LatestRecordMissing`] is returned.
    #[allow(dead_code)]
    pub(crate) async fn load(store: Arc<RecordStore<T>>) -> Result<Self, SlateDBError> {
        Self::try_load(store)
            .await?
            .ok_or_else(|| SlateDBError::LatestRecordMissing)
    }

    pub(crate) async fn update(&mut self, dirty: DirtyRecord<T>) -> Result<(), SlateDBError> {
        if dirty.id != self.id {
            return Err(FileVersionExists);
        }
        let next = self.next_id();
        self.store.write(next, &dirty.value).await?;
        self.id = next;
        self.record = dirty.value;
        Ok(())
    }

    pub(crate) async fn maybe_apply_update<F>(&mut self, mutator: F) -> Result<(), SlateDBError>
    where
        F: Fn(&StoredRecord<T>) -> Result<Option<DirtyRecord<T>>, SlateDBError>,
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

impl<T> RecordStore<T> {
    pub(crate) fn new(
        root_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        subdir: &str,
        file_suffix: &'static str,
        codec: Box<dyn RecordCodec<T>>,
    ) -> Self {
        Self {
            object_store: Box::new(DelegatingTransactionalObjectStore::new(
                root_path.child(subdir),
                object_store,
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
            .put_if_not_exists(&path, self.codec.encode(value))
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

    // List files for this record type within an id range
    pub(crate) async fn list<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<GenericFileMetadata>, SlateDBError> {
        let base = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(base));
        let mut items = Vec::new();
        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::from(e)),
        } {
            match self.parse_id(&file.location) {
                Ok(id) if id_range.contains(&id) => {
                    items.push(GenericFileMetadata {
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
    use crate::record::store::{FenceableRecord, RecordCodec, RecordStore, StoredRecord};
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

    impl RecordCodec<TestVal> for TestValCodec {
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

    fn new_store() -> Arc<RecordStore<TestVal>> {
        let os = Arc::new(InMemory::new());
        Arc::new(RecordStore::new(
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
        let mut sr = StoredRecord::init(
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
            *sr.record()
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
            *sr.record()
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
        let mut a = StoredRecord::init(
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
        let mut b: StoredRecord<TestVal> = StoredRecord {
            id: id_b,
            record: val_b,
            store: Arc::clone(&store),
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
            let mut next = sr.record().clone();
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
        let mut sr = StoredRecord::init(
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
        let sr = StoredRecord::init(
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
        let err = sr.store.write(dirty.id(), &dirty.value).await.unwrap_err();
        assert!(matches!(err, SlateDBError::FileVersionExists));
    }

    #[tokio::test]
    async fn test_fenceable_record_epoch_bump_and_fence() {
        let store = new_store();
        // initial record
        let sr = StoredRecord::init(
            Arc::clone(&store),
            TestVal {
                epoch: 0,
                payload: 0,
            },
        )
        .await
        .unwrap();

        // writer A bumps to epoch 1
        let mut fa = FenceableRecord::init(
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
        let sb = StoredRecord {
            id: id_b,
            record: val_b,
            store: Arc::clone(&store),
        };
        let mut fb = FenceableRecord::init(
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
