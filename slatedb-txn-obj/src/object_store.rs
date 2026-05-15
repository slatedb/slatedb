use crate::TransactionalObjectError::CallbackError;
use crate::{
    BoundaryObject, GenericObjectMetadata, MonotonicId, ObjectCodec, SequencedStorageProtocol,
    TransactionalObjectError,
};
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, warn};
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::{
    Error, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use parking_lot::Mutex;
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
    boundary: Arc<dyn BoundaryObject>,
}

impl<T> ObjectStoreSequencedStorageProtocol<T> {
    pub fn new(
        root_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        subdir: &str,
        file_suffix: &'static str,
        codec: Box<dyn ObjectCodec<T>>,
    ) -> Self {
        let boundary = Arc::new(ObjectStoreBoundaryObject::new(
            root_path,
            object_store.clone(),
            subdir,
        ));
        Self::new_with_boundary(
            root_path,
            object_store,
            subdir,
            file_suffix,
            codec,
            boundary,
        )
    }

    pub fn new_with_boundary(
        root_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        subdir: &str,
        file_suffix: &'static str,
        codec: Box<dyn ObjectCodec<T>>,
        boundary: Arc<dyn BoundaryObject>,
    ) -> Self {
        Self {
            object_store: Box::new(::object_store::prefix::PrefixStore::new(
                object_store,
                root_path.child(subdir),
            )),
            codec,
            file_suffix,
            boundary,
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

/// Implements [`BoundaryObject`] on object storage.
///
/// The boundary is stored as a single ASCII-encoded `u64` at
/// `<root_path>/gc/<name>.boundary`. A missing boundary file is treated as `0`
/// until this process has observed the boundary file at least once.
///
/// Successful reads cache the boundary value along with object-store version
/// metadata. Later reads pass the cached ETag as `if-none-match`, allowing stores
/// that support conditional GETs to return `NotModified`; in that case the cached
/// boundary is reused without fetching the object body again.
pub struct ObjectStoreBoundaryObject {
    object_store: Box<dyn ObjectStore>,
    filepath: Path,
    /// Caches the last observed boundary and object-store version metadata for
    /// conditional reads.
    cache: Mutex<Option<(MonotonicId, UpdateVersion)>>,
}

impl ObjectStoreBoundaryObject {
    pub fn new(root_path: &Path, object_store: Arc<dyn ObjectStore>, name: &str) -> Self {
        Self {
            object_store: Box::new(::object_store::prefix::PrefixStore::new(
                object_store,
                root_path.child("gc"),
            )),
            filepath: Path::from(format!("{name}.boundary")),
            cache: Mutex::new(None),
        }
    }

    /// Updates the cached boundary and version metadata without moving the cached
    /// boundary backward.
    ///
    /// ## Arguments
    ///
    /// * `boundary` - The boundary value read from or written to object storage.
    /// * `version` - The object-store version metadata associated with `boundary`.
    ///
    /// ## Returns
    ///
    /// The boundary and optional version metadata that callers should use after
    /// applying the cache's monotonicity rule.
    fn update_cache(
        &self,
        boundary: MonotonicId,
        version: UpdateVersion,
    ) -> (MonotonicId, Option<UpdateVersion>) {
        let mut cache = self.cache.lock();
        if let Some((cached_id, cached_version)) = cache.as_ref() {
            if *cached_id > boundary {
                return (*cached_id, Some(cached_version.clone()));
            }
        }

        *cache = Some((boundary, version.clone()));
        (boundary, Some(version))
    }

    /// Reads the durable boundary, using a conditional GET when cached version
    /// metadata is present.
    async fn read_boundary(
        &self,
    ) -> Result<(MonotonicId, Option<UpdateVersion>), TransactionalObjectError> {
        let cached = self.cache.lock().clone();
        let opts = GetOptions {
            if_none_match: cached
                .as_ref()
                .and_then(|(_, version)| version.e_tag.clone()),
            ..GetOptions::default()
        };

        match self.object_store.get_opts(&self.filepath, opts).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result.bytes().await?;
                let boundary = MonotonicId::new(
                    std::str::from_utf8(&bytes)
                        .map_err(|_| TransactionalObjectError::InvalidObjectState)?
                        .trim()
                        .parse()
                        .map_err(|_| TransactionalObjectError::InvalidObjectState)?,
                );
                Ok(self.update_cache(boundary, version))
            }
            Err(Error::NotModified { .. }) => match self.cache.lock().clone() {
                Some((boundary, version)) => Ok((boundary, Some(version))),
                // NotModified implies we have a cache, since we need the
                // version's ETag for the conditional GET. If cache is missing,
                // treat as invalid state.
                None => Err(TransactionalObjectError::InvalidObjectState),
            },
            Err(Error::NotFound { .. }) => match self.cache.lock().clone() {
                // Once observed, the boundary file disappearing means durable state
                // regressed; do not treat it as an initial zero boundary.
                Some(_) => Err(TransactionalObjectError::InvalidObjectState),
                // Default to zero boundary if file is missing and we've never
                // observed it before.
                None => Ok((MonotonicId::new(0), None)),
            },
            Err(e) => Err(TransactionalObjectError::from(e)),
        }
    }

    /// Checks that the given id is above the boundary, returning an error if not.
    ///
    /// ## Arguments
    /// - `id` - The id to check against the boundary.
    /// - `boundary` - The boundary to check against.
    ///
    /// ## Returns
    /// - `Ok(())` if the id is above the boundary.
    /// - `Err(TransactionalObjectError::ObjectVersionExists)` if the id is at or
    ///   below the boundary.
    async fn check_boundary(
        &self,
        id: MonotonicId,
        boundary: MonotonicId,
    ) -> Result<(), TransactionalObjectError> {
        if id <= boundary {
            warn!(
                "object version is behind boundary: id={:?}, boundary={:?}",
                id.id(),
                boundary.id()
            );
            return Err(TransactionalObjectError::ObjectVersionExists);
        }
        Ok(())
    }
}

#[async_trait]
impl BoundaryObject for ObjectStoreBoundaryObject {
    async fn check(&self, id: MonotonicId) -> Result<(), TransactionalObjectError> {
        // Check the cache first to avoid an object store call when the boundary is stable.
        let cached_boundary = self.cache.lock().clone();
        if let Some((boundary, _)) = cached_boundary {
            self.check_boundary(id, boundary).await?;
        }
        // If cache passed, double check against object store using GET If-None-Match.
        let (boundary, _) = self.read_boundary().await?;
        self.check_boundary(id, boundary).await
    }

    async fn advance(&self, boundary: MonotonicId) -> Result<(), TransactionalObjectError> {
        loop {
            // Use the cache if it's available. If it's stale, we'll refresh the cache when
            // we check the `put_result`.
            let cached_boundary = self.cache.lock().clone();
            let (current_boundary, current_version) =
                if let Some((boundary, version)) = cached_boundary {
                    (boundary, Some(version))
                } else {
                    // No cache, so we have to go to the object store.
                    self.read_boundary().await?
                };

            if current_boundary >= boundary {
                return Ok(());
            }

            let put_result = match current_version {
                Some(version) => {
                    self.object_store
                        .put_opts(
                            &self.filepath,
                            PutPayload::from(boundary.id().to_string()),
                            PutOptions::from(PutMode::Update(version)),
                        )
                        .await
                }
                None => {
                    self.object_store
                        .put_opts(
                            &self.filepath,
                            PutPayload::from(boundary.id().to_string()),
                            PutOptions::from(PutMode::Create),
                        )
                        .await
                }
            };

            match put_result {
                Ok(result) => {
                    self.update_cache(boundary, result.into());
                    return Ok(());
                }
                // Try again if the boundary was concurrently updated by another process.
                Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {
                    // Refresh the cache so re-attempts always use the fresh boundary.
                    self.read_boundary().await?;
                }
                Err(e) => return Err(TransactionalObjectError::from(e)),
            }
        }
    }
}

#[async_trait]
impl<T: Send + Sync> BoundaryObject for ObjectStoreSequencedStorageProtocol<T> {
    async fn check(&self, id: MonotonicId) -> Result<(), TransactionalObjectError> {
        self.boundary.check(id).await
    }

    async fn advance(&self, boundary: MonotonicId) -> Result<(), TransactionalObjectError> {
        self.boundary.advance(boundary).await
    }
}

#[async_trait]
impl<T: Send + Sync> SequencedStorageProtocol<T> for ObjectStoreSequencedStorageProtocol<T> {
    async fn write_unchecked(
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

    async fn try_read_latest_unchecked(
        &self,
    ) -> Result<Option<(MonotonicId, T)>, TransactionalObjectError> {
        loop {
            let files = self.list(Unbounded, Unbounded).await?;
            if let Some(file) = files.last() {
                let result = self
                    .try_read_unchecked(file.id)
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

    async fn try_read_unchecked(
        &self,
        id: MonotonicId,
    ) -> Result<Option<T>, TransactionalObjectError> {
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
    async fn delete_unchecked(&self, id: MonotonicId) -> Result<(), TransactionalObjectError> {
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
    use super::{ObjectStoreBoundaryObject, ObjectStoreSequencedStorageProtocol};
    use crate::tests::{new_store, TestVal, TestValCodec};
    use crate::{
        BoundaryObject, MonotonicId, ObjectCodec, SequencedStorageProtocol,
        SimpleTransactionalObject, TransactionalObject, TransactionalObjectError,
        TransactionalStorageProtocol,
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

    #[derive(Debug)]
    struct CountingGetStore {
        inner: InMemory,
        get_opts_calls: AtomicUsize,
        if_none_match_gets: AtomicUsize,
    }

    impl CountingGetStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                get_opts_calls: AtomicUsize::new(0),
                if_none_match_gets: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Display for CountingGetStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingGetStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingGetStore {
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
            self.get_opts_calls.fetch_add(1, Ordering::SeqCst);
            if options.if_none_match.is_some() {
                self.if_none_match_gets.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
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
    async fn test_boundary_check_allows_missing_boundary() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store, "manifest");

        boundary.check(MonotonicId::new(1)).await.unwrap();
    }

    #[tokio::test]
    async fn test_boundary_advance_creates_boundary_and_rejects_at_or_below_it() {
        let object_store = Arc::new(InMemory::new());
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");

        boundary.advance(MonotonicId::new(2)).await.unwrap();

        let err = boundary.check(MonotonicId::new(2)).await.unwrap_err();
        assert!(matches!(err, TransactionalObjectError::ObjectVersionExists));
        boundary.check(MonotonicId::new(3)).await.unwrap();

        let raw_boundary = object_store
            .get(&Path::from("/root/gc/manifest.boundary"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("2", std::str::from_utf8(&raw_boundary).unwrap());
    }

    #[tokio::test]
    async fn test_boundary_advance_is_monotonic() {
        let object_store = Arc::new(InMemory::new());
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");

        boundary.advance(MonotonicId::new(3)).await.unwrap();
        boundary.advance(MonotonicId::new(2)).await.unwrap();

        let err = boundary.check(MonotonicId::new(3)).await.unwrap_err();
        assert!(matches!(err, TransactionalObjectError::ObjectVersionExists));

        let raw_boundary = object_store
            .get(&Path::from("/root/gc/manifest.boundary"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("3", std::str::from_utf8(&raw_boundary).unwrap());
    }

    #[tokio::test]
    async fn test_boundary_check_reuses_cache_on_not_modified() {
        let counting_store = Arc::new(CountingGetStore::new());
        let object_store: Arc<dyn ObjectStore> = counting_store.clone();
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");

        boundary.advance(MonotonicId::new(2)).await.unwrap();

        let if_none_match_gets = counting_store.if_none_match_gets.load(Ordering::SeqCst);
        boundary.check(MonotonicId::new(3)).await.unwrap();

        assert_eq!(
            if_none_match_gets + 1,
            counting_store.if_none_match_gets.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_boundary_check_rejects_from_cache_without_get() {
        let counting_store = Arc::new(CountingGetStore::new());
        let object_store: Arc<dyn ObjectStore> = counting_store.clone();
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");

        boundary.advance(MonotonicId::new(2)).await.unwrap();

        let get_opts_calls = counting_store.get_opts_calls.load(Ordering::SeqCst);
        let err = boundary.check(MonotonicId::new(2)).await.unwrap_err();

        assert!(matches!(err, TransactionalObjectError::ObjectVersionExists));
        assert_eq!(
            get_opts_calls,
            counting_store.get_opts_calls.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_boundary_check_refreshes_cache_when_etag_changes() {
        let counting_store = Arc::new(CountingGetStore::new());
        let object_store: Arc<dyn ObjectStore> = counting_store.clone();
        let first_boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");
        let second_boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store, "manifest");

        first_boundary.advance(MonotonicId::new(2)).await.unwrap();
        second_boundary.advance(MonotonicId::new(4)).await.unwrap();

        let err = first_boundary.check(MonotonicId::new(4)).await.unwrap_err();

        assert!(matches!(err, TransactionalObjectError::ObjectVersionExists));
    }

    #[tokio::test]
    async fn test_boundary_advance_retries_after_conflicting_boundary_update() {
        let object_store = Arc::new(InMemory::new());
        let first_boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");
        let second_boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");

        first_boundary.advance(MonotonicId::new(2)).await.unwrap();
        second_boundary.advance(MonotonicId::new(3)).await.unwrap();

        first_boundary.advance(MonotonicId::new(4)).await.unwrap();

        let raw_boundary = object_store
            .get(&Path::from("/root/gc/manifest.boundary"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("4", std::str::from_utf8(&raw_boundary).unwrap());
    }

    #[tokio::test]
    async fn test_boundary_check_returns_invalid_state_when_observed_boundary_disappears() {
        let object_store = Arc::new(InMemory::new());
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");

        boundary.advance(MonotonicId::new(2)).await.unwrap();
        object_store
            .delete(&Path::from("/root/gc/manifest.boundary"))
            .await
            .unwrap();

        let err = boundary.check(MonotonicId::new(3)).await.unwrap_err();

        assert!(matches!(err, TransactionalObjectError::InvalidObjectState));
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
    async fn test_try_read_unchecked_missing_returns_none() {
        let store = new_store();

        let missing = store.try_read_unchecked(1.into()).await.unwrap();

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
            boundary: Arc::new(ObjectStoreBoundaryObject::new(
                &Path::from("/root"),
                flaky_store.clone(),
                "test",
            )),
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
