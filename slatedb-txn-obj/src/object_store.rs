use crate::TransactionalObjectError::CallbackError;
use crate::{
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
/// ## File layout and naming
/// - Objects are stored under a root directory and logical subdirectory provided at
///   construction time (see `ObjectStoreSequencedStorageProtocol::new`).
/// - Each version is a single file whose name is a zero-padded 20-digit decimal id
///   followed by a fixed suffix, e.g. `00000000000000000001.manifest`.
/// - New versions must use the next consecutive id (`current_id + 1`).
/// - We rely on `put_if_not_exists` to enforce CAS at the storage layer. If a file with
///   the same id already exists, the write fails with `ObjectVersionExists`.
pub struct ObjectStoreSequencedStorageProtocol<T> {
    object_store: Box<dyn ObjectStore>,
    codec: Box<dyn ObjectCodec<T>>,
    file_suffix: &'static str,
}

impl<T> ObjectStoreSequencedStorageProtocol<T> {
    pub fn new(
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
        loop {
            let files = self.list(Unbounded, Unbounded).await?;
            if let Some(file) = files.last() {
                let result = self
                    .try_read(file.id)
                    .await
                    .map(|opt| opt.map(|v| (file.id, v)));
                match result {
                    // File listed but not found. Probably deleted by GC. Retry list/read.
                    // See https://github.com/slatedb/slatedb/issues/1215 for more details.
                    Ok(None) => {
                        warn!(
                            "listed file missing on read, retrying [location={}]",
                            file.location,
                        );
                    }
                    _ => return result,
                }
            } else {
                // No files found, so return None
                break;
            }
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
        let id_range = (from, to);
        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(TransactionalObjectError::from(e)),
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
                Err(e) => warn!(
                    "unknown file in directory [base={}, location={}, object_store={}, error={:?}]",
                    base, file.location, self.object_store, e,
                ),
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
    use super::ObjectStoreSequencedStorageProtocol;
    use crate::tests::{new_store, TestVal, TestValCodec};
    use crate::{
        MonotonicId, ObjectCodec, SequencedStorageProtocol, SimpleTransactionalObject,
        TransactionalObject, TransactionalStorageProtocol,
    };
    use chrono::Utc;
    use futures::stream::{self, BoxStream};
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    };
    use std::collections::Bound::{Excluded, Included, Unbounded};
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A flaky object store that simulates a missing file on the first list() call.
    /// On the first call to list(), it returns a file with `missing_id`. On subsequent
    /// calls, it returns a file with `present_id`. This allows testing retry logic in
    /// `try_read_latest` when a listed file is missing on read. This can happen if the
    /// garbage collector deletes a file between the list and get calls.
    #[derive(Debug)]
    struct FlakyListStore {
        inner: InMemory,
        list_calls: AtomicUsize,
        missing_id: u64,
        present_id: u64,
        file_suffix: &'static str,
    }

    impl FlakyListStore {
        fn new(
            inner: InMemory,
            missing_id: u64,
            present_id: u64,
            file_suffix: &'static str,
        ) -> Self {
            Self {
                inner,
                list_calls: AtomicUsize::new(0),
                missing_id,
                present_id,
                file_suffix,
            }
        }

        fn path_for(&self, id: u64) -> Path {
            Path::from(format!("{:020}.{}", id, self.file_suffix))
        }
    }

    impl fmt::Display for FlakyListStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FlakyListStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FlakyListStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            let call = self.list_calls.fetch_add(1, Ordering::SeqCst);
            let id = if call == 0 {
                self.missing_id
            } else {
                self.present_id
            };
            let meta = ObjectMeta {
                location: self.path_for(id),
                last_modified: Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            };
            stream::iter(vec![Ok(meta)]).boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

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

    #[tokio::test]
    async fn test_try_read_missing_returns_none() {
        let store = new_store();

        let missing = store.try_read(1.into()).await.unwrap();

        assert!(missing.is_none());
    }

    /// Validate that try_read_latest retries when a listed file is missing on read.
    #[tokio::test]
    async fn test_try_read_latest_retries_missing_listed_file() {
        let expected = TestVal {
            epoch: 7,
            payload: 42,
        };
        let missing_id = 1u64;
        let present_id = 2u64;

        let inner = InMemory::new();
        let codec = TestValCodec;
        let present_path = Path::from(format!("{:020}.val", present_id));
        inner
            .put(
                &present_path,
                PutPayload::from_bytes(codec.encode(&expected)),
            )
            .await
            .unwrap();

        let flaky_store = Arc::new(FlakyListStore::new(inner, missing_id, present_id, "val"));
        let object_store: Arc<dyn ObjectStore> = flaky_store.clone();
        let store = ObjectStoreSequencedStorageProtocol {
            object_store: Box::new(object_store),
            codec: Box::new(TestValCodec),
            file_suffix: "val",
        };

        let latest = store.try_read_latest().await.unwrap().unwrap();
        assert_eq!(present_id, latest.0.id());
        assert_eq!(expected, latest.1);
        assert!(
            flaky_store.list_calls.load(Ordering::SeqCst) >= 2,
            "expected try_read_latest to retry after a missing read"
        );
    }
}
