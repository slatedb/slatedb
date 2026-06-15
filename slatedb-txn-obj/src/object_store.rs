use crate::TransactionalObjectError::CallbackError;
use crate::{
    BoundaryObject, MonotonicId, ObjectCodec, SequencedStorageProtocol, TransactionalObjectError,
};
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, warn};
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::{Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parking_lot::Mutex;
use slatedb_common::object_metadata::IdentifiedObjectMetadata;
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
    object_store: Arc<dyn ObjectStore>,
    dir_path: Path,
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
            object_store,
            dir_path: root_path.clone().join(subdir),
            codec,
            file_suffix,
            boundary,
        }
    }

    fn path_for(&self, id: MonotonicId) -> Path {
        self.dir_path
            .clone()
            .join(format!("{:020}.{}", id.id(), self.file_suffix))
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
/// ## Why versioned, create-only markers (not a single overwritten object)
///
/// Earlier versions stored the boundary as a single object at
/// `<root_path>/gc/<name>.boundary` and advanced it with a conditional
/// *overwrite* — `PutMode::Update(version)`, i.e. an `If-Match` precondition on
/// the object's ETag. That requires the object store to support conditional
/// overwrite-PUT. Several S3-compatible stores support conditional *create*
/// (`If-None-Match: *`) but NOT conditional *overwrite* (`If-Match`) — notably
/// DigitalOcean Spaces, which returns `412 PreconditionFailed` on every
/// `If-Match` PUT. On those stores the very first boundary write (a `Create`)
/// succeeded, but every later advance was an `Update` that 412'd forever: GC
/// hot-looped (conditional GET -> 304, conditional PUT -> 412), the boundary
/// never advanced, and nothing was ever deleted.
///
/// The boundary value only ever moves forward (monotonic), so we don't need to
/// mutate one object. Instead each boundary value is recorded as its own
/// immutable, create-only marker `<root_path>/gc/<name>/<id:020>.boundary`,
/// written with `PutMode::Create` (`If-None-Match: *`) — the same primitive the
/// manifest and compactions sequences already use. The current boundary is the
/// highest marker id present. Superseded (strictly lower) markers are deleted
/// best-effort after each advance, so the directory holds ~one object in steady
/// state.
///
/// This needs only conditional *create*, which every supported store provides,
/// so it works on stores that lack conditional overwrite while remaining correct
/// on those that have it.
///
/// ## Backward compatibility
///
/// A database created by an older version has a single `<name>.boundary` object
/// and no marker directory. [`Self::read_boundary`] falls back to reading that
/// legacy object when no markers exist, preserving the boundary across upgrade.
/// The first advance writes a marker (id strictly greater than the legacy value,
/// since advances below the current boundary are no-ops) and best-effort deletes
/// the legacy object; subsequent reads use markers only.
pub struct ObjectStoreBoundaryObject {
    /// Object store rooted at `<root_path>/gc`.
    object_store: Box<dyn ObjectStore>,
    /// Logical boundary name (e.g. "manifest", "compactions", "wal"); also the
    /// subdirectory under `gc/` that holds this boundary's markers.
    name: String,
    /// Legacy single-object path `<name>.boundary`, read for backward compat.
    legacy_filepath: Path,
    /// Caches the last observed boundary so `check` can reject ids at or below a
    /// known boundary without an object-store round trip. Never moves backward.
    cache: Mutex<Option<MonotonicId>>,
}

impl ObjectStoreBoundaryObject {
    pub fn new(root_path: &Path, object_store: Arc<dyn ObjectStore>, name: &str) -> Self {
        Self {
            object_store: Box::new(::object_store::prefix::PrefixStore::new(
                object_store,
                root_path.clone().join("gc"),
            )),
            name: name.to_string(),
            legacy_filepath: Path::from(format!("{name}.boundary")),
            cache: Mutex::new(None),
        }
    }

    /// Path of the immutable marker for boundary value `id`, relative to the
    /// `gc/` prefix store: `<name>/<id:020>.boundary`.
    fn marker_path(&self, id: MonotonicId) -> Path {
        Path::from(format!("{}/{:020}.boundary", self.name, id.id()))
    }

    /// Prefix used to list this boundary's markers: `<name>`. Note this matches
    /// the `<name>/...` marker directory but NOT the legacy `<name>.boundary`
    /// object, which is a distinct single path segment.
    fn markers_prefix(&self) -> Path {
        Path::from(self.name.clone())
    }

    /// Parse the boundary value from a marker path's filename, or `None` if the
    /// path is not a `<id:020>.boundary` marker.
    fn parse_marker_id(path: &Path) -> Option<MonotonicId> {
        let filename = path.filename()?;
        let (stem, ext) = filename.rsplit_once('.')?;
        if ext != "boundary" {
            return None;
        }
        stem.parse().ok().map(MonotonicId::new)
    }

    /// Update the cached boundary, never moving it backward. Returns the
    /// effective (max) boundary after applying the monotonicity rule.
    ///
    /// Flooring at the cache is safe: the durable boundary is globally monotonic
    /// (markers are only added, and cleanup never removes the maximum), so any
    /// previously cached value is `<=` the true current boundary.
    fn update_cache(&self, boundary: MonotonicId) -> MonotonicId {
        let mut cache = self.cache.lock();
        let effective = match *cache {
            Some(cached) if cached >= boundary => cached,
            _ => boundary,
        };
        *cache = Some(effective);
        effective
    }

    /// Read the legacy single-object boundary value, or `None` if absent.
    async fn read_legacy_boundary(&self) -> Result<Option<MonotonicId>, TransactionalObjectError> {
        match self.object_store.get(&self.legacy_filepath).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let value: u64 = std::str::from_utf8(&bytes)
                    .map_err(|_| TransactionalObjectError::InvalidObjectState)?
                    .trim()
                    .parse()
                    .map_err(|_| TransactionalObjectError::InvalidObjectState)?;
                Ok(Some(MonotonicId::new(value)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(TransactionalObjectError::from(e)),
        }
    }

    /// List boundary markers and return the highest value present, or `None`.
    async fn read_max_marker(&self) -> Result<Option<MonotonicId>, TransactionalObjectError> {
        let prefix = self.markers_prefix();
        let mut stream = self.object_store.list(Some(&prefix));
        let mut max: Option<MonotonicId> = None;
        while let Some(meta) = stream
            .next()
            .await
            .transpose()
            .map_err(TransactionalObjectError::from)?
        {
            if let Some(id) = Self::parse_marker_id(&meta.location) {
                max = Some(max.map_or(id, |m| m.max(id)));
            }
        }
        Ok(max)
    }

    /// Read the durable boundary: the highest marker id, or — if no markers
    /// exist yet — the legacy single-object value, or `0` if neither is present.
    /// Updates (and is floored by) the cache.
    ///
    /// Once any marker exists, its id is strictly greater than the legacy value
    /// (advances below the current boundary are no-ops), so the legacy object is
    /// only consulted while no marker exists — a single GET that disappears after
    /// the first advance.
    async fn read_boundary(&self) -> Result<MonotonicId, TransactionalObjectError> {
        let boundary = match self.read_max_marker().await? {
            Some(id) => id,
            None => self
                .read_legacy_boundary()
                .await?
                .unwrap_or_else(|| MonotonicId::new(0)),
        };
        Ok(self.update_cache(boundary))
    }

    /// Best-effort deletion of markers strictly below `keep` and the legacy
    /// object. Errors are ignored: stale markers are harmless (reads take the
    /// max) and will be retried on the next advance. The current maximum is never
    /// deleted (every caller only removes ids `< keep`, and `keep`'s own marker
    /// is created before this runs), so the boundary cannot dip.
    async fn cleanup_below(&self, keep: MonotonicId) {
        let prefix = self.markers_prefix();
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            let Ok(meta) = meta else { continue };
            if let Some(id) = Self::parse_marker_id(&meta.location) {
                if id < keep {
                    let _ = self.object_store.delete(&meta.location).await;
                }
            }
        }
        // The legacy object is superseded once any marker exists.
        let _ = self.object_store.delete(&self.legacy_filepath).await;
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
    fn check_boundary(
        &self,
        id: MonotonicId,
        boundary: MonotonicId,
    ) -> Result<(), TransactionalObjectError> {
        if id <= boundary {
            debug!(
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
        // Check the cache first to avoid an object store call when the id is
        // already at or below a known boundary (the boundary only grows, so this
        // can never become a false reject).
        let cached_boundary = *self.cache.lock();
        if let Some(boundary) = cached_boundary {
            self.check_boundary(id, boundary)?;
        }
        // If the cache passed (or was empty), double check against the store.
        let boundary = self.read_boundary().await?;
        self.check_boundary(id, boundary)
    }

    async fn advance(&self, target: MonotonicId) -> Result<(), TransactionalObjectError> {
        // Fast path: a cached boundary already at/above the target.
        if let Some(cached) = *self.cache.lock() {
            if cached >= target {
                return Ok(());
            }
        }
        loop {
            let current = self.read_boundary().await?;
            if current >= target {
                return Ok(());
            }

            // Record the new boundary as an immutable, create-only marker. This
            // uses `If-None-Match: *` (conditional create) rather than the old
            // `If-Match` (conditional overwrite), so it works on stores that lack
            // conditional overwrite.
            let put_result = self
                .object_store
                .put_opts(
                    &self.marker_path(target),
                    PutPayload::from(target.id().to_string()),
                    PutOptions::from(PutMode::Create),
                )
                .await;

            match put_result {
                // Created the marker, or another writer created exactly this
                // marker concurrently (idempotent). Either way `target` is now
                // durable. (A marker `>= target` cannot pre-exist here: we only
                // reach this point when `current < target`.)
                Ok(_) | Err(Error::AlreadyExists { .. }) => {
                    self.update_cache(target);
                    self.cleanup_below(target).await;
                    return Ok(());
                }
                // Some stores surface a create conflict as `Precondition` rather
                // than `AlreadyExists`. Re-read; a concurrent advance to `target`
                // (or higher) will satisfy the loop's early return next pass.
                Err(Error::Precondition { .. }) => {
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
                            file.metadata.location,
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
    ) -> Result<Vec<IdentifiedObjectMetadata<MonotonicId>>, TransactionalObjectError> {
        let mut files_stream = self.object_store.list(Some(&self.dir_path));
        let mut items = Vec::new();
        let id_range = (from, to);
        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(TransactionalObjectError::from(e)),
        } {
            match self.parse_id(&file.location) {
                Ok(id) if id_range.contains(&id) => {
                    items.push(IdentifiedObjectMetadata::from_object_meta(id, file));
                }
                Err(e) => warn!(
                    "unknown file in directory [base={}, location={}, object_store={}, error={:?}]",
                    self.dir_path, file.location, self.object_store, e,
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
        CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, Result as ObjectStoreResult,
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

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
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

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Counts `get_opts` calls so tests can assert that `check` is served from
    /// the in-memory cache without an object-store round trip.
    #[derive(Debug)]
    struct CountingGetStore {
        inner: InMemory,
        get_opts_calls: AtomicUsize,
    }

    impl CountingGetStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                get_opts_calls: AtomicUsize::new(0),
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
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
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

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// An object store that supports conditional *create* (`PutMode::Create` /
    /// `If-None-Match: *`) but REJECTS conditional *overwrite*
    /// (`PutMode::Update` / `If-Match`) with `Precondition` — modeling
    /// DigitalOcean Spaces, where the old single-object boundary hot-looped.
    /// `overwrite_attempts` records any conditional-overwrite PUT so a test can
    /// assert the boundary never issues one.
    #[derive(Debug)]
    struct CreateOnlyStore {
        inner: InMemory,
        overwrite_attempts: AtomicUsize,
    }

    impl CreateOnlyStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                overwrite_attempts: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Display for CreateOnlyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CreateOnlyStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CreateOnlyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            if matches!(opts.mode, PutMode::Update(_)) {
                self.overwrite_attempts.fetch_add(1, Ordering::SeqCst);
                return Err(ObjectStoreError::Precondition {
                    path: location.to_string(),
                    source: "conditional overwrite (If-Match) not supported".into(),
                });
            }
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

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
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

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
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

        // The boundary is recorded as a create-only marker named after its value.
        let raw_boundary = object_store
            .get(&Path::from(
                "/root/gc/manifest/00000000000000000002.boundary",
            ))
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
        // Advancing to a lower value is a no-op (does not create a marker).
        boundary.advance(MonotonicId::new(2)).await.unwrap();

        let err = boundary.check(MonotonicId::new(3)).await.unwrap_err();
        assert!(matches!(err, TransactionalObjectError::ObjectVersionExists));

        // Only the highest marker remains; no marker was written for the lower
        // (no-op) advance.
        let raw_boundary = object_store
            .get(&Path::from(
                "/root/gc/manifest/00000000000000000003.boundary",
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("3", std::str::from_utf8(&raw_boundary).unwrap());
        assert!(object_store
            .get(&Path::from(
                "/root/gc/manifest/00000000000000000002.boundary"
            ))
            .await
            .is_err());
    }

    /// Advancing must never use a conditional *overwrite* (`If-Match`), so the
    /// boundary works on stores (e.g. DigitalOcean Spaces) that support
    /// conditional *create* (`If-None-Match`) but reject conditional overwrite.
    #[tokio::test]
    async fn test_boundary_advance_never_overwrites() {
        let create_only = Arc::new(CreateOnlyStore::new());
        let object_store: Arc<dyn ObjectStore> = create_only.clone();
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store, "manifest");

        // A store that rejects overwrite-PUT (like DO Spaces) must still advance
        // repeatedly — the old `PutMode::Update` path would 412 forever here.
        boundary.advance(MonotonicId::new(2)).await.unwrap();
        boundary.advance(MonotonicId::new(5)).await.unwrap();
        boundary.advance(MonotonicId::new(9)).await.unwrap();

        boundary.check(MonotonicId::new(9)).await.unwrap_err();
        boundary.check(MonotonicId::new(10)).await.unwrap();
        assert_eq!(
            0,
            create_only.overwrite_attempts.load(Ordering::SeqCst),
            "boundary advance must never attempt a conditional overwrite"
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
    async fn test_boundary_check_refreshes_cache_when_marker_advances() {
        let counting_store = Arc::new(CountingGetStore::new());
        let object_store: Arc<dyn ObjectStore> = counting_store.clone();
        let first_boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");
        let second_boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store, "manifest");

        // `first` caches boundary=2; `second` (a separate cache) advances to 4.
        first_boundary.advance(MonotonicId::new(2)).await.unwrap();
        second_boundary.advance(MonotonicId::new(4)).await.unwrap();

        // `first`'s stale cache (2) lets the check past the cache gate, but the
        // store read observes the advanced marker (4) and rejects.
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
            .get(&Path::from(
                "/root/gc/manifest/00000000000000000004.boundary",
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("4", std::str::from_utf8(&raw_boundary).unwrap());
    }

    /// The cached boundary is a monotonic floor: an empty store (no markers, no
    /// legacy object) must not regress a process's known boundary back to 0.
    #[tokio::test]
    async fn test_boundary_read_floors_at_cache_when_store_empty() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store, "manifest");

        *boundary.cache.lock() = Some(MonotonicId::new(2));

        let observed = boundary.read_boundary().await.unwrap();
        assert_eq!(MonotonicId::new(2), observed);
    }

    /// A freshly opened boundary over an empty store reads as 0.
    #[tokio::test]
    async fn test_boundary_read_zero_when_empty_and_uncached() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store, "manifest");

        let observed = boundary.read_boundary().await.unwrap();
        assert_eq!(MonotonicId::new(0), observed);
    }

    /// Backward compatibility: a DB written by an older version has a single
    /// `<name>.boundary` object and no marker directory. Reads must honor it, and
    /// the first advance must migrate to a marker and drop the legacy object.
    #[tokio::test]
    async fn test_boundary_reads_legacy_object_and_migrates_on_advance() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        object_store
            .put(
                &Path::from("/root/gc/manifest.boundary"),
                PutPayload::from("7"),
            )
            .await
            .unwrap();
        let boundary =
            ObjectStoreBoundaryObject::new(&Path::from("/root"), object_store.clone(), "manifest");

        // Legacy value is observed.
        assert_eq!(MonotonicId::new(7), boundary.read_boundary().await.unwrap());
        boundary.check(MonotonicId::new(7)).await.unwrap_err();
        boundary.check(MonotonicId::new(8)).await.unwrap();

        // Advancing migrates to a marker and removes the legacy object.
        boundary.advance(MonotonicId::new(9)).await.unwrap();
        assert!(object_store
            .get(&Path::from("/root/gc/manifest.boundary"))
            .await
            .is_err());
        let raw = object_store
            .get(&Path::from(
                "/root/gc/manifest/00000000000000000009.boundary",
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!("9", std::str::from_utf8(&raw).unwrap());
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
        assert_eq!(
            Path::from("/root/test/00000000000000000001.val"),
            all[0].metadata.location
        );
        assert_eq!(
            Path::from("/root/test/00000000000000000004.val"),
            all[3].metadata.location
        );

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
            object_store,
            dir_path: Path::default(),
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
