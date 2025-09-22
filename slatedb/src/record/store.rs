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
    value: T,
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
    clock: Arc<dyn SystemClock>,
}

impl<T> DirtyRecord<T> {
    /// Create a new dirty record with the given id and value
    pub(crate) fn new(id: u64, value: T) -> Self {
        Self { id, value }
    }

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
            system_clock,
            record_update_timeout,
            "record update",
            async {
                loop {
                    let local_epoch = get_epoch(stored.record()) + 1;
                    let mut new_val = stored.record().clone();
                    set_epoch(&mut new_val, local_epoch);
                    let dirty = DirtyRecord::new(stored.id(), new_val);
                    match stored.update_dirty(dirty).await {
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
        .map_err(|_| SlateDBError::Timeout {
            op: "record update",
            backoff: Duration::from_secs(1),
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn local_epoch(&self) -> u64 {
        self.local_epoch
    }

    pub(crate) fn id(&self) -> u64 {
        self.stored.id()
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.stored.next_id()
    }

    pub(crate) fn record(&self) -> &T {
        self.stored.record()
    }

    pub(crate) fn clock_arc(&self) -> Arc<dyn SystemClock> {
        self.stored.clock_arc()
    }

    pub(crate) fn prepare_dirty(&self) -> Result<DirtyRecord<T>, SlateDBError> {
        self.check_epoch()?;
        let id = self.id();
        let record = self.record().clone();
        Ok(DirtyRecord::new(id, record))
    }

    // The file may have been updated by a readers, another process, or
    // we may have gotten this error after successfully updating
    // if we failed to get the response. Either way, refresh
    // the file and try the bump again.
    pub(crate) async fn refresh(&mut self) -> Result<(), SlateDBError> {
        self.stored.refresh().await?;
        self.check_epoch()
    }

    pub(crate) async fn update_dirty(&mut self, dirty: DirtyRecord<T>) -> Result<(), SlateDBError> {
        self.check_epoch()?;
        self.stored.update_dirty(dirty).await
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

    pub(crate) fn new(id: u64, record: T, store: Arc<RecordStore<T>>) -> Self {
        Self { id, record, store }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    pub(crate) fn record(&self) -> &T {
        &self.record
    }

    pub(crate) fn clock_arc(&self) -> Arc<dyn SystemClock> {
        self.store.clock.clone()
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.id + 1
    }

    pub(crate) async fn refresh(&mut self) -> Result<&T, SlateDBError> {
        let Some((id, new_val)) = self.store.try_read_latest().await? else {
            return Err(InvalidDBState);
        };
        self.id = id;
        self.record = new_val;
        Ok(&self.record)
    }

    pub(crate) async fn update_dirty(&mut self, dirty: DirtyRecord<T>) -> Result<(), SlateDBError> {
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
            match self.update_dirty(dirty).await {
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
        clock: Arc<dyn SystemClock>,
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
            clock,
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
        debug!("deleting record [id={}]", id);
        self.object_store
            .delete(&path)
            .await
            .map_err(SlateDBError::from)
    }
}
