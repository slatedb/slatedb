use crate::transactional_object::TransactionalObjectError::CallbackError;
use crate::transactional_object::{
    GenericObjectMetadata, MonotonicId, ObjectCodec, SequencedStorageProtocol,
    TransactionalObjectError, TransactionalStorageProtocol,
};
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, warn};
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::{Error, ObjectStore, PutMode, PutOptions, PutPayload};
use std::collections::Bound;
use std::collections::Bound::Unbounded;
use std::ops::RangeBounds;
use std::sync::Arc;

/// Implements `SequencedStorageProtocol<T>` on object storage.
///
/// File layout and naming
/// ----------------------
/// - Objects are stored under a root directory and logical subdirectory provided at
///   construction time (see `ObjectStoreSequencedStorageProtocol::new`).
/// - Each version is a single file whose name is a zero-padded 20-digit decimal id
///   followed by a fixed suffix, e.g. `00000000000000000001.manifest`.
/// - New versions must use the next consecutive id (`current_id + 1`).
/// - We rely on `put_if_not_exists` to enforce CAS at the storage layer. If a file with
///   the same id already exists, the write fails with `ObjectVersionExists`.
pub(crate) struct ObjectStoreSequencedStorageProtocol<T> {
    object_store: Box<dyn ObjectStore>,
    codec: Box<dyn ObjectCodec<T>>,
    file_suffix: &'static str,
}

impl<T> ObjectStoreSequencedStorageProtocol<T> {
    pub(crate) fn new(
        root_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        subdir: &str,
        file_suffix: &'static str,
        codec: Box<dyn ObjectCodec<T>>,
    ) -> Self {
        Self {
            object_store: Box::new(::object_store::prefix::PrefixStore::new(
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

    fn parse_id(&self, path: &Path) -> Result<MonotonicId, TransactionalObjectError> {
        match path.extension() {
            Some(ext) if ext == self.file_suffix => path
                .filename()
                .expect("invalid filename")
                .split('.')
                .next()
                .ok_or_else(|| TransactionalObjectError::InvalidObjectState)?
                .parse()
                .map(MonotonicId::new)
                .map_err(|_| TransactionalObjectError::InvalidObjectState),
            _ => Err(TransactionalObjectError::InvalidObjectState),
        }
    }
}

#[async_trait]
impl<T: Send + Sync> TransactionalStorageProtocol<T, MonotonicId>
    for ObjectStoreSequencedStorageProtocol<T>
{
    async fn write(
        &self,
        current_id: Option<MonotonicId>,
        new_value: &T,
    ) -> Result<MonotonicId, TransactionalObjectError> {
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
                    TransactionalObjectError::ObjectVersionExists
                } else {
                    TransactionalObjectError::from(err)
                }
            })?;
        Ok(id)
    }

    async fn try_read_latest(&self) -> Result<Option<(MonotonicId, T)>, TransactionalObjectError> {
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
impl<T: Send + Sync> SequencedStorageProtocol<T> for ObjectStoreSequencedStorageProtocol<T> {
    async fn try_read(&self, id: MonotonicId) -> Result<Option<T>, TransactionalObjectError> {
        let path = self.path_for(id);
        match self.object_store.get(&path).await {
            Ok(obj) => match obj.bytes().await {
                Ok(bytes) => self.codec.decode(&bytes).map(Some).map_err(CallbackError),
                Err(e) => Err(TransactionalObjectError::from(e)),
            },
            Err(e) => match e {
                Error::NotFound { .. } => Ok(None),
                _ => Err(TransactionalObjectError::from(e)),
            },
        }
    }

    // List files for this object type within an id range
    async fn list(
        &self,
        from: Bound<MonotonicId>,
        to: Bound<MonotonicId>,
    ) -> Result<Vec<GenericObjectMetadata>, TransactionalObjectError> {
        let base = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(base));
        let mut items = Vec::new();
        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(TransactionalObjectError::from(e)),
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
    async fn delete(&self, id: MonotonicId) -> Result<(), TransactionalObjectError> {
        let path = self.path_for(id);
        debug!("deleting object [record_path={}]", path);
        self.object_store
            .delete(&path)
            .await
            .map_err(TransactionalObjectError::from)
    }
}

#[cfg(test)]
mod tests {
    use crate::transactional_object::tests::{new_store, TestVal};
    use crate::transactional_object::{
        MonotonicId, SequencedStorageProtocol, SimpleTransactionalObject, TransactionalObject,
        TransactionalStorageProtocol,
    };
    use std::collections::Bound::{Excluded, Included, Unbounded};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_list_ranges_sorted() {
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
}
