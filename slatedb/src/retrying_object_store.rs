use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable, Sleeper};
use futures::stream::BoxStream;
use futures::{stream, StreamExt, TryStreamExt};
use log::{debug, info};
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    Attribute, CopyOptions, Extensions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartId, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

use crate::utils::IdGenerator;
use slatedb_common::clock::SystemClock;
use slatedb_common::DbRand;

/// Metadata key used to store the ULID for put operations.
/// This is used to verify if a failed put actually succeeded.
/// There's not separator between "slatedb", "put", and "id" to avoid issues
/// with object stores that restrict metadata keys.
const PUT_ID_ATTRIBUTE: &str = "slatedbputid";

type SystemClockSleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Backon's sleeper interface backed by SlateDB's configured system clock.
#[derive(Debug, Clone)]
struct SystemClockSleeper {
    clock: Arc<dyn SystemClock>,
}

impl Sleeper for SystemClockSleeper {
    type Sleep = SystemClockSleep;

    fn sleep(&self, dur: Duration) -> Self::Sleep {
        let clock = Arc::clone(&self.clock);
        Box::pin(async move {
            clock.sleep(dur).await;
        })
    }
}

/// A thin wrapper around an `ObjectStore` that retries transient errors with
/// exponential backoff using the configured [`SystemClock`] for sleeps.
///
/// Retries are unbounded by default; a bound can be configured via
/// `max_retries`, in which case an operation that keeps failing eventually
/// returns its underlying error instead of retrying forever. This applies to
/// both foreground and background object-store operations, since both go
/// through this wrapper.
#[derive(Clone)]
pub(crate) struct RetryingObjectStore {
    inner: Arc<dyn ObjectStore>,
    /// Low-level multipart handle for the same backend as `inner`. When set,
    /// part uploads are retried in place; without it, parts get only the
    /// inner client's retries (`MultipartUpload` can't re-send a failed part).
    multipart_store: Option<Arc<dyn MultipartStore>>,
    rand: Arc<DbRand>,
    clock: Arc<dyn SystemClock>,
    /// Maximum wrapper-level retries per operation. `None` = unbounded.
    max_retries: Option<u32>,
}

impl RetryingObjectStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        rand: Arc<DbRand>,
        clock: Arc<dyn SystemClock>,
        max_retries: Option<u32>,
    ) -> Self {
        Self {
            inner,
            multipart_store: None,
            rand,
            clock,
            max_retries,
        }
    }

    /// Enables in-place part-upload retries. `multipart_store` must be
    /// backed by the same storage as the wrapped store.
    pub(crate) fn with_multipart_store(mut self, multipart_store: Arc<dyn MultipartStore>) -> Self {
        self.multipart_store = Some(multipart_store);
        self
    }

    #[inline]
    fn retry_builder(&self) -> ExponentialBuilder {
        let builder = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(1));
        match self.max_retries {
            Some(max_retries) => builder.with_max_times(max_retries as usize),
            None => builder.without_max_times(),
        }
    }

    #[inline]
    fn sleeper(&self) -> SystemClockSleeper {
        SystemClockSleeper {
            clock: Arc::clone(&self.clock),
        }
    }

    #[inline]
    fn notify(err: &object_store::Error, duration: Duration) {
        info!(
            "retrying object store operation [error={:?}, duration={:?}]",
            err, duration
        );
    }

    #[inline]
    pub(crate) fn should_retry(err: &object_store::Error) -> bool {
        let retry = !matches!(
            err,
            object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotModified { .. }
                | object_store::Error::NotFound { .. }
                | object_store::Error::NotImplemented { .. }
                | object_store::Error::NotSupported { .. }
        );
        if !retry {
            debug!("not retrying object store operation [error={:?}]", err);
        }
        retry
    }

    /// Checks if a failed put actually succeeded by verifying the ULID in remote metadata.
    ///
    /// When a put operation times out after the file was successfully written,
    /// a retry would encounter an AlreadyExists or Precondition error. This method
    /// checks if the object in the store has our ULID, indicating our write succeeded.
    ///
    /// Returns `Some(ObjectMeta)` if verification succeeds, `None` otherwise.
    async fn verify_put_succeeded(&self, location: &Path, expected_id: &str) -> Option<ObjectMeta> {
        let get_opts = GetOptions {
            head: true,
            ..Default::default()
        };
        let result = (|| async { self.inner.get_opts(location, get_opts.clone()).await })
            .retry(self.retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await;

        match result {
            Ok(get_result) => {
                let key = Attribute::Metadata(Cow::Borrowed(PUT_ID_ATTRIBUTE));
                if get_result
                    .attributes
                    .get(&key)
                    .is_some_and(|v| v.as_ref() == expected_id)
                {
                    Some(get_result.meta)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    /// Creates a new Attributes with our ULID attribute merged with existing attributes.
    fn with_put_id(attrs: object_store::Attributes, put_id: &str) -> object_store::Attributes {
        let mut new_attrs = object_store::Attributes::new();
        for (key, value) in attrs.iter() {
            new_attrs.insert(key.clone(), value.clone());
        }

        new_attrs.insert(
            Attribute::Metadata(Cow::Owned(PUT_ID_ATTRIBUTE.to_string())),
            object_store::AttributeValue::from(put_id.to_string()),
        );
        new_attrs
    }

    /// Converts `result` into success if the object in the store carries our
    /// put id, else returns it unchanged.
    async fn resolved_by_put_id(
        &self,
        location: &Path,
        put_id: &str,
        result: object_store::Result<PutResult>,
    ) -> object_store::Result<PutResult> {
        if let Some(meta) = self.verify_put_succeeded(location, put_id).await {
            return Ok(PutResult {
                e_tag: meta.e_tag,
                version: meta.version,
                extensions: Extensions::new(),
            });
        }
        result
    }

    /// Converts an AlreadyExists/Precondition result into success if the
    /// object in the store carries our put id (a timeout-after-write).
    async fn verified_result(
        &self,
        location: &Path,
        put_id: &str,
        result: object_store::Result<PutResult>,
    ) -> object_store::Result<PutResult> {
        match &result {
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {
                self.resolved_by_put_id(location, put_id, result).await
            }
            _ => result,
        }
    }

    /// Like [`Self::verified_result`], but for multipart completes NotFound
    /// is ambiguous too: a complete whose response was lost leaves the upload
    /// finished server-side, so a retry fails with e.g. S3's NoSuchUpload.
    async fn verified_complete_result(
        &self,
        location: &Path,
        put_id: &str,
        result: object_store::Result<PutResult>,
    ) -> object_store::Result<PutResult> {
        match &result {
            Err(object_store::Error::NotFound { .. }) => {
                self.resolved_by_put_id(location, put_id, result).await
            }
            _ => self.verified_result(location, put_id, result).await,
        }
    }

    /// Runs `op` under this store's retry policy.
    async fn with_retries<T, F, Fut>(&self, op: F) -> object_store::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        op.retry(self.retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    /// Runs `op` with the put-id attribute merged into `attributes`, under
    /// the retry policy. If the store rejects attributes as unsupported,
    /// retries once more with the original attributes.
    async fn with_put_id_fallback<T, F, Fut>(
        &self,
        attributes: &object_store::Attributes,
        put_id: &str,
        op: F,
    ) -> object_store::Result<T>
    where
        F: Fn(object_store::Attributes) -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        let result = self
            .with_retries(|| op(Self::with_put_id(attributes.clone(), put_id)))
            .await;
        match result {
            Err(
                object_store::Error::NotSupported { .. }
                | object_store::Error::NotImplemented { .. },
            ) => self.with_retries(|| op(attributes.clone())).await,
            result => result,
        }
    }

    /// Creates a multipart upload via the low-level [`MultipartStore`] API
    /// with the put-id attribute.
    async fn create_multipart_id(
        &self,
        multipart_store: &Arc<dyn MultipartStore>,
        location: &Path,
        opts: &PutMultipartOptions,
        put_id: &str,
    ) -> object_store::Result<MultipartId> {
        self.with_put_id_fallback(&opts.attributes, put_id, |attributes| {
            let opts = PutMultipartOptions {
                attributes,
                ..opts.clone()
            };
            async move { multipart_store.create_multipart_opts(location, opts).await }
        })
        .await
    }

    /// Compute the expected byte length of a range read, truncating at the
    /// actual file size. This handles the case where a `GetRange::Bounded`
    /// end exceeds the file length.
    fn expected_range_len(range: &GetRange, file_size: u64) -> u64 {
        match range {
            GetRange::Bounded(r) => {
                let end = r.end.min(file_size);
                end.saturating_sub(r.start)
            }
            GetRange::Offset(offset) => file_size.saturating_sub(*offset),
            GetRange::Suffix(suffix) => (*suffix).min(file_size),
        }
    }
}

impl std::fmt::Display for RetryingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetryingObjectStore({})", self.inner)
    }
}

impl std::fmt::Debug for RetryingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryingObjectStore")
            .field("inner", &self.inner)
            .field("multipart_store", &self.multipart_store.is_some())
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

/// MultipartUpload wrapper that adds put-id verification on complete().
/// Parts delegate to the inner upload and can't be retried here (see
/// [`RetryingObjectStore::multipart_store`]); [`MultipartStoreUpload`] can.
struct RetryingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    retrying_store: RetryingObjectStore,
    location: Path,
    put_id: String,
}

impl std::fmt::Debug for RetryingMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryingMultipartUpload")
            .field("location", &self.location)
            .field("put_id", &self.put_id)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for RetryingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let result = self.inner.complete().await;
        self.retrying_store
            .verified_complete_result(&self.location, &self.put_id, result)
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

/// MultipartUpload backed by the low-level [`MultipartStore`] API: parts
/// carry explicit indexes, so a failed part is retried in place. Dropping
/// an unfinished upload aborts it best-effort.
struct MultipartStoreUpload {
    retrying_store: RetryingObjectStore,
    multipart_store: Arc<dyn MultipartStore>,
    location: Path,
    multipart_id: MultipartId,
    put_id: String,
    /// Part ids indexed by part index, filled in as part futures resolve.
    parts: Arc<Mutex<Vec<Option<PartId>>>>,
    /// Set once complete succeeds or abort runs; suppresses the drop abort.
    finished: bool,
}

impl Drop for MultipartStoreUpload {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let store = Arc::clone(&self.multipart_store);
        let location = self.location.clone();
        let id = self.multipart_id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                store.abort_multipart(&location, &id).await.ok();
            });
        }
    }
}

impl std::fmt::Debug for MultipartStoreUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultipartStoreUpload")
            .field("location", &self.location)
            .field("multipart_id", &self.multipart_id)
            .field("put_id", &self.put_id)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for MultipartStoreUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let part_idx = {
            let mut parts = self.parts.lock().expect("lock poisoned");
            parts.push(None);
            parts.len() - 1
        };

        let store = self.retrying_store.clone();
        let multipart_store = Arc::clone(&self.multipart_store);
        let location = self.location.clone();
        let multipart_id = self.multipart_id.clone();
        let parts = Arc::clone(&self.parts);
        Box::pin(async move {
            let part = store
                .with_retries(|| async {
                    multipart_store
                        .put_part(&location, &multipart_id, part_idx, data.clone())
                        .await
                })
                .await?;
            parts.lock().expect("lock poisoned")[part_idx] = Some(part);
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        // Read, don't drain: a failed complete leaves the slots intact so it
        // can be retried once in-flight parts settle.
        let parts = self
            .parts
            .lock()
            .expect("lock poisoned")
            .iter()
            .cloned()
            .map(|part| {
                part.ok_or_else(|| object_store::Error::Generic {
                    store: "retrying_object_store",
                    source: "complete() called before all part uploads finished".into(),
                })
            })
            .collect::<object_store::Result<Vec<_>>>()?;

        let store = &self.retrying_store;
        let result = store
            .with_retries(|| async {
                self.multipart_store
                    .complete_multipart(&self.location, &self.multipart_id, parts.clone())
                    .await
            })
            .await;
        let result = store
            .verified_complete_result(&self.location, &self.put_id, result)
            .await;
        self.finished |= result.is_ok();
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.finished = true;
        self.multipart_store
            .abort_multipart(&self.location, &self.multipart_id)
            .await
    }
}

#[async_trait]
impl ObjectStore for RetryingObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // For range reads, drain the body inside the retry closure so a
        // transient mid-stream error retries the entire range. Pre-0.13
        // SlateDB explicitly overrode `ObjectStore::get_range`, which
        // returned `Bytes` and therefore retried the whole read; in 0.13
        // `get_range` lives on `ObjectStoreExt` and reads the body after
        // `get_opts` returns, so without this buffering the body bytes
        // would fall outside the retry loop.
        (|| async {
            // Options and location must be owned per-attempt.
            let options = options.clone();
            let options_range = options.range.clone();
            let result = self.inner.get_opts(location, options).await?;
            let meta = result.meta.clone();
            let file_size = meta.size;

            if options_range.is_none() {
                // No range requested — don't buffer the body. The buffer size
                // can't be validated wihtout buffering.
                return Ok(result);
            }

            // Range read: buffer the body and validate against the expected
            // size (requested range truncated at file size).
            let expected_len =
                Self::expected_range_len(&options_range.expect("range is set"), file_size);
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = result.extensions.clone();
            let bytes = result.bytes().await?;
            let bytes_len = bytes.len() as u64;

            if bytes_len != expected_len {
                return Err(object_store::Error::Generic {
                    store: "retrying_object_store",
                    source: format!(
                        "Size check failed: {bytes_len} bytes read, but expected \
                         {expected_len} bytes (requested range truncated at file \
                         size {file_size})"
                    )
                    .into(),
                });
            }

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
                meta,
                range,
                attributes,
                extensions,
            })
        })
        .retry(self.retry_builder())
        .sleep(self.sleeper())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // Only add ULID metadata for conditional puts (Create/Update) where
        // we need to verify if a timeout-after-write actually succeeded.
        // For Overwrite mode, retries are safe without verification.
        let is_conditional = !matches!(opts.mode, object_store::PutMode::Overwrite);

        let put_id = if is_conditional {
            Some(self.rand.rng().gen_ulid(self.clock.as_ref()).to_string())
        } else {
            None
        };

        let opts_with_id = if let Some(ref id) = put_id {
            PutOptions {
                attributes: Self::with_put_id(opts.attributes.clone(), id),
                ..opts.clone()
            }
        } else {
            opts.clone()
        };

        let result = (|| async {
            self.inner
                .put_opts(location, payload.clone(), opts_with_id.clone())
                .await
        })
        .retry(self.retry_builder())
        .sleep(self.sleeper())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await;

        // If attributes aren't supported, fall back to put without ULID
        if matches!(
            &result,
            Err(object_store::Error::NotSupported { .. }
                | object_store::Error::NotImplemented { .. })
        ) && put_id.is_some()
        {
            return (|| async {
                self.inner
                    .put_opts(location, payload.clone(), opts.clone())
                    .await
            })
            .retry(self.retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await;
        }

        match (&result, &put_id) {
            (Err(object_store::Error::AlreadyExists { .. }), Some(id))
            | (Err(object_store::Error::Precondition { .. }), Some(id)) => {
                if let Some(meta) = self.verify_put_succeeded(location, id).await {
                    Ok(PutResult {
                        e_tag: meta.e_tag,
                        version: meta.version,
                        extensions: Extensions::new(),
                    })
                } else {
                    result
                }
            }
            _ => result,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let put_id = self.rand.rng().gen_ulid(self.clock.as_ref()).to_string();

        if let Some(multipart_store) = &self.multipart_store {
            let multipart_id = self
                .create_multipart_id(multipart_store, location, &opts, &put_id)
                .await?;
            return Ok(Box::new(MultipartStoreUpload {
                retrying_store: self.clone(),
                multipart_store: Arc::clone(multipart_store),
                location: location.clone(),
                multipart_id,
                put_id,
                parts: Arc::new(Mutex::new(Vec::new())),
                finished: false,
            }));
        }

        let opts_with_id = PutMultipartOptions {
            attributes: Self::with_put_id(opts.attributes.clone(), &put_id),
            ..opts.clone()
        };

        let result = (|| async {
            self.inner
                .put_multipart_opts(location, opts_with_id.clone())
                .await
        })
        .retry(self.retry_builder())
        .sleep(self.sleeper())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await;

        // If attributes aren't supported, fall back without ULID
        let inner = match result {
            Ok(inner) => inner,
            Err(
                object_store::Error::NotSupported { .. }
                | object_store::Error::NotImplemented { .. },
            ) => {
                (|| async { self.inner.put_multipart_opts(location, opts.clone()).await })
                    .retry(self.retry_builder())
                    .sleep(self.sleeper())
                    .notify(Self::notify)
                    .when(Self::should_retry)
                    .await?
            }
            Err(e) => return Err(e),
        };

        Ok(Box::new(RetryingMultipartUpload {
            inner,
            retrying_store: self.clone(),
            location: location.clone(),
            put_id,
        }))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let sleeper = self.sleeper();
        let retry_builder = self.retry_builder();
        locations
            .then(move |loc| {
                let inner = Arc::clone(&inner);
                let sleeper = sleeper.clone();
                async move {
                    let loc = loc?;
                    (|| async { inner.delete(&loc).await })
                        .retry(retry_builder)
                        .sleep(sleeper)
                        .notify(Self::notify)
                        .when(Self::should_retry)
                        .await?;
                    Ok(loc)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let sleeper = self.sleeper();
        let retry_builder = self.retry_builder();
        let prefix_owned = prefix.cloned();

        // list() is a little more complex than the other functions because:
        // 1. it's sync, not async
        // 2. it paginates and returns a stream of results
        //
        // (2) is particularly challenging because it means it returns before we know the full
        // result. This is problematic--because we can't easily retry half-way through the
        // iteration.
        //
        // To get around this, we convert the entire list into a vector in a single attempt,
        // and then return a stream of those results.
        stream::once(async move {
            (|| async {
                let stream = inner.list(prefix_owned.as_ref());
                // Any error in the stream will return an error for try_collect
                stream.try_collect::<Vec<_>>().await
            })
            .retry(retry_builder)
            .sleep(sleeper)
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
        })
        .map_ok(|entries| {
            // If the list() call succeeded, we need to convert the vector back into
            // a stream of results.
            stream::iter(
                entries
                    .into_iter()
                    .map(Ok::<ObjectMeta, object_store::Error>),
            )
            .boxed()
        })
        .try_flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let sleeper = self.sleeper();
        let retry_builder = self.retry_builder();
        let prefix_owned = prefix.cloned();
        let offset_owned = offset.clone();

        // See the comment in list() for details on why we do this.
        stream::once(async move {
            (|| async {
                let stream = inner.list_with_offset(prefix_owned.as_ref(), &offset_owned);
                stream.try_collect::<Vec<_>>().await
            })
            .retry(retry_builder)
            .sleep(sleeper)
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
        })
        .map_ok(|entries| {
            stream::iter(
                entries
                    .into_iter()
                    .map(Ok::<ObjectMeta, object_store::Error>),
            )
            .boxed()
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        (|| async { self.inner.list_with_delimiter(prefix).await })
            .retry(self.retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        (|| async { self.inner.copy_opts(from, to, options.clone()).await })
            .retry(self.retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        (|| async { self.inner.rename_opts(from, to, options.clone()).await })
            .retry(self.retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::RetryingObjectStore;
    use crate::test_utils::{ExtensionMarker, ExtensionObjectStore, FlakyObjectStore};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
    use slatedb_common::DbRand;
    use slatedb_common::MockSystemClock;
    use std::sync::Arc;
    use std::time::Duration;

    fn test_rand() -> Arc<DbRand> {
        Arc::new(DbRand::default())
    }

    fn test_clock() -> Arc<DefaultSystemClock> {
        Arc::new(DefaultSystemClock::new())
    }

    #[tokio::test]
    async fn test_put_opts_retries_transient_until_success() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 1));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);

        let path = Path::from("/data/obj");
        retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello")),
                PutOptions::default(),
            )
            .await
            .expect("put should succeed after retries");

        // 1 failure + 1 success
        assert_eq!(flaky.put_attempts(), 2);

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn test_put_opts_preserves_extensions() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let marking: Arc<dyn ObjectStore> = Arc::new(ExtensionObjectStore::new(inner));
        let retrying = RetryingObjectStore::new(marking, test_rand(), test_clock(), None);

        let path = Path::from("/data/extension-put");
        let result = retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello")),
                PutOptions::default(),
            )
            .await
            .expect("put should succeed");

        assert!(result.extensions.get::<ExtensionMarker>().is_some());
    }

    #[tokio::test]
    async fn test_get_opts_range_preserves_extensions() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/data/extension-get");
        inner
            .put(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello world")),
            )
            .await
            .unwrap();
        let marking: Arc<dyn ObjectStore> = Arc::new(ExtensionObjectStore::new(inner));
        let retrying = RetryingObjectStore::new(marking, test_rand(), test_clock(), None);

        let result = retrying
            .get_opts(
                &path,
                GetOptions {
                    range: Some((0..5).into()),
                    ..GetOptions::default()
                },
            )
            .await
            .expect("range read should succeed");

        assert!(result.extensions.get::<ExtensionMarker>().is_some());
        assert_eq!(result.bytes().await.unwrap(), Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn test_put_opts_retry_sleep_uses_system_clock() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 1));
        let clock = Arc::new(MockSystemClock::new());
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), clock.clone(), None);
        let path = Path::from("/data/obj");

        let handle = tokio::spawn({
            let retrying = retrying.clone();
            let path = path.clone();
            async move {
                retrying
                    .put_opts(
                        &path,
                        PutPayload::from_bytes(Bytes::from_static(b"hello")),
                        PutOptions::default(),
                    )
                    .await
            }
        });

        flaky.wait_for_put_attempts(1).await;
        assert_eq!(flaky.put_attempts(), 1);
        assert!(!handle.is_finished());

        clock.advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(flaky.put_attempts(), 1);
        assert!(!handle.is_finished());

        clock.advance(Duration::from_millis(1)).await;

        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "put should succeed after clock-driven retry"
        );
        assert_eq!(flaky.put_attempts(), 2);

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn test_put_opts_does_not_retry_on_already_exists() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 0));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);
        let path = Path::from("/data/obj");

        retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"v1")),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();

        let attempts_before = flaky.put_attempts();
        let err = retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"v2")),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect_err("second put should fail with AlreadyExists");

        // Should be AlreadyExists
        match err {
            object_store::Error::AlreadyExists { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }

        // Should not retry on AlreadyExists → exactly one new attempt
        assert_eq!(flaky.put_attempts(), attempts_before + 1);
    }

    #[tokio::test]
    async fn test_head_retries_transient_until_success() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/x");
        inner
            .put(&path, PutPayload::from_bytes(Bytes::from_static(b"data")))
            .await
            .unwrap();

        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_head_failures(1));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);

        let meta = retrying.head(&path).await.expect("head should succeed");
        assert_eq!(meta.size, 4);
        assert_eq!(flaky.head_attempts(), 2);
    }

    #[tokio::test]
    async fn test_put_opts_does_not_retry_on_precondition() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let failing = Arc::new(FlakyObjectStore::new(inner, 0).with_put_precondition_always());
        let retrying = RetryingObjectStore::new(failing.clone(), test_rand(), test_clock(), None);
        let path = Path::from("/p");

        let err = retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"x")),
                PutOptions::default(),
            )
            .await
            .expect_err("expected precondition error");

        match err {
            object_store::Error::Precondition { .. } => {}
            e => panic!("unexpected error: {e:?}"),
        }
        assert_eq!(failing.put_attempts(), 1);
    }

    #[tokio::test]
    async fn test_get_opts_does_not_retry_on_not_modified() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let retrying = RetryingObjectStore::new(inner.clone(), test_rand(), test_clock(), None);
        let path = Path::from("/data/obj");

        retrying
            .put(&path, PutPayload::from_bytes(Bytes::from_static(b"data")))
            .await
            .unwrap();
        let e_tag = retrying.head(&path).await.unwrap().e_tag.unwrap();

        let err = retrying
            .get_opts(
                &path,
                GetOptions {
                    if_none_match: Some(e_tag),
                    ..GetOptions::default()
                },
            )
            .await
            .expect_err("matching if-none-match should surface NotModified");

        match err {
            object_store::Error::NotModified { .. } => {}
            e => panic!("unexpected error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn test_list_retries_transient_until_success() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let paths = [
            Path::from("/items/a"),
            Path::from("/items/b"),
            Path::from("/items/c"),
        ];
        for (idx, path) in paths.iter().enumerate() {
            inner
                .put(
                    path,
                    PutPayload::from_bytes(Bytes::from(format!("val-{idx}").into_bytes())),
                )
                .await
                .unwrap();
        }

        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_list_failures(1, 1));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);

        let listed: Vec<_> = retrying
            .list(None)
            .try_collect()
            .await
            .expect("list should eventually succeed");
        assert_eq!(listed.len(), paths.len());
        let mut names: Vec<_> = listed.into_iter().map(|m| m.location.to_string()).collect();
        names.sort();
        let mut expected: Vec<_> = paths.iter().map(|p| p.to_string()).collect();
        expected.sort();
        assert_eq!(names, expected);
        assert_eq!(flaky.list_attempts(), 2);
    }

    #[tokio::test]
    async fn test_list_with_offset_retries_transient_until_success() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let paths = [
            Path::from("/items/a"),
            Path::from("/items/b"),
            Path::from("/items/c"),
        ];
        for (idx, path) in paths.iter().enumerate() {
            inner
                .put(
                    path,
                    PutPayload::from_bytes(Bytes::from(format!("val-{idx}").into_bytes())),
                )
                .await
                .unwrap();
        }

        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_list_with_offset_failures(1, 1));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);
        let offset = Path::from("/items/a");

        let listed: Vec<_> = retrying
            .list_with_offset(None, &offset)
            .try_collect()
            .await
            .expect("list_with_offset should eventually succeed");

        // Expect entries after the offset (at least b and c)
        let mut names: Vec<_> = listed.into_iter().map(|m| m.location.to_string()).collect();
        names.sort();
        assert!(names.contains(&"items/b".to_string()));
        assert!(names.contains(&"items/c".to_string()));
        assert_eq!(flaky.list_with_offset_attempts(), 2);
    }

    #[tokio::test]
    async fn test_put_opts_succeeds_on_matching_ulid() {
        // Simulate: put succeeds but returns AlreadyExists error (timeout after write)
        // The ULID in the object's metadata should match, so we return success
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(
            FlakyObjectStore::new(inner, 0).with_put_succeeds_but_returns_already_exists(),
        );
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);
        let path = Path::from("/data/obj");

        // Must use PutMode::Create to trigger ULID verification
        let result = retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello")),
                PutOptions::from(PutMode::Create),
            )
            .await;

        assert!(result.is_ok(), "put should succeed via ULID verification");

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn test_put_opts_fails_on_mismatched_ulid() {
        // First write a file with different ULID (simulating another client's write)
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/data/obj");

        // Write directly to inner store (no ULID from RetryingObjectStore)
        inner
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"other client data")),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();

        // Now try to write via RetryingObjectStore - should fail because ULID won't match
        let retrying = RetryingObjectStore::new(inner.clone(), test_rand(), test_clock(), None);
        let err = retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"my data")),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect_err("put should fail because file exists with different ULID");

        match err {
            object_store::Error::AlreadyExists { .. } => {}
            e => panic!("unexpected error: {e:?}"),
        }

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(
            got.bytes().await.unwrap(),
            Bytes::from_static(b"other client data")
        );
    }

    #[tokio::test]
    async fn test_get_range_retries_transient_until_success() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/data/obj");
        inner
            .put(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello world")),
            )
            .await
            .unwrap();

        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_get_range_failures(2));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);

        let result = retrying
            .get_range(&path, 0..5)
            .await
            .expect("should succeed after retries");
        assert_eq!(result, Bytes::from_static(b"hello"));
        // 2 failures + 1 success
        assert_eq!(flaky.get_range_attempts(), 3);
    }

    #[tokio::test]
    async fn test_get_ranges_retries_transient_until_success() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/data/obj");
        inner
            .put(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello world")),
            )
            .await
            .unwrap();

        // get_ranges calls get_range internally, so flaky get_range failures will trigger retries
        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_get_range_failures(2));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);

        let ranges = vec![0..5, 6..11];
        let result = retrying
            .get_ranges(&path, &ranges)
            .await
            .expect("should succeed after retries");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Bytes::from_static(b"hello"));
        assert_eq!(result[1], Bytes::from_static(b"world"));
    }

    #[tokio::test]
    async fn test_put_opts_preserves_user_attributes() {
        use object_store::{Attribute, Attributes, GetOptions};
        use std::borrow::Cow;

        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let retrying = RetryingObjectStore::new(inner.clone(), test_rand(), test_clock(), None);
        let path = Path::from("/data/obj");

        let mut user_attrs = Attributes::new();
        user_attrs.insert(
            Attribute::ContentType,
            object_store::AttributeValue::from("application/json"),
        );
        user_attrs.insert(
            Attribute::Metadata(Cow::Owned("custom-key".to_string())),
            object_store::AttributeValue::from("custom-value"),
        );

        // Must use PutMode::Create to trigger ULID attribute addition
        let opts = PutOptions {
            attributes: user_attrs,
            mode: PutMode::Create,
            ..Default::default()
        };

        retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"{}")),
                opts,
            )
            .await
            .unwrap();

        let get_opts = GetOptions {
            head: true,
            ..Default::default()
        };
        let result = inner.get_opts(&path, get_opts).await.unwrap();

        assert_eq!(
            result.attributes.get(&Attribute::ContentType),
            Some(&object_store::AttributeValue::from("application/json"))
        );
        assert_eq!(
            result
                .attributes
                .get(&Attribute::Metadata(Cow::Borrowed("custom-key"))),
            Some(&object_store::AttributeValue::from("custom-value"))
        );

        assert!(result
            .attributes
            .get(&Attribute::Metadata(Cow::Borrowed(super::PUT_ID_ATTRIBUTE)))
            .is_some());
    }

    #[tokio::test]
    async fn test_get_opts_range_read_size_check_passes() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let retrying = RetryingObjectStore::new(inner.clone(), test_rand(), test_clock(), None);
        let path = Path::from("/data/obj");

        inner
            .put(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello world")),
            )
            .await
            .unwrap();

        let result = retrying
            .get_opts(
                &path,
                GetOptions {
                    range: Some((0..5).into()),
                    ..GetOptions::default()
                },
            )
            .await
            .expect("range read should pass size check");

        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn test_get_opts_range_read_size_mismatch_returns_error() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/data/obj");
        inner
            .put(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello world")),
            )
            .await
            .unwrap();

        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_truncate_get_range_bytes(1, 1));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);

        // First attempt returns truncated body (1 byte vs 5 expected),
        // triggering the size check error. The retry succeeds normally.
        let result = retrying
            .get_opts(
                &path,
                GetOptions {
                    range: Some((0..5).into()),
                    ..GetOptions::default()
                },
            )
            .await
            .expect("should succeed after retrying past truncated response");

        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"hello"));
        // 1 truncated attempt + 1 successful retry
        assert_eq!(flaky.get_range_attempts(), 2);
    }

    /// Builds a retrying store over a flaky store with the low-level
    /// multipart path enabled, backed by a shared InMemory. `configure` adds
    /// failure injection to the flaky store.
    fn multipart_fixture(
        max_retries: Option<u32>,
        configure: impl FnOnce(FlakyObjectStore) -> FlakyObjectStore,
    ) -> (Arc<FlakyObjectStore>, RetryingObjectStore) {
        let inner = Arc::new(InMemory::new());
        let flaky = FlakyObjectStore::new(inner.clone(), 0).with_multipart_store(inner);
        let flaky = Arc::new(configure(flaky));
        let retrying =
            RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), max_retries)
                .with_multipart_store(flaky.clone());
        (flaky, retrying)
    }

    #[tokio::test]
    async fn test_multipart_store_part_transient_failure_retried_in_place() {
        let (flaky, retrying) = multipart_fixture(None, |f| f.with_put_part_failures(1));
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .expect("part should succeed after in-place retry");
        upload
            .put_part(PutPayload::from_static(b"part1"))
            .await
            .unwrap();
        upload.complete().await.expect("complete should succeed");

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(
            got.bytes().await.unwrap(),
            Bytes::from_static(b"part0part1")
        );
        // 1 failure + retry of the same index + second part.
        assert_eq!(flaky.put_part_attempts(), 3);
        // The upload is created exactly once; no restart.
        assert_eq!(flaky.multipart_attempts(), 1);
        assert_eq!(flaky.multipart_complete_attempts(), 1);
    }

    #[tokio::test]
    async fn test_multipart_store_part_failures_exhaust_bounded_retries() {
        let (flaky, retrying) =
            multipart_fixture(Some(2), |f| f.with_put_part_failures(usize::MAX));
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        let err = upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .expect_err("bounded retries should exhaust and surface at the part");
        assert!(matches!(err, object_store::Error::Generic { .. }));

        // Initial attempt + 2 retries.
        assert_eq!(flaky.put_part_attempts(), 3);
    }

    #[tokio::test]
    async fn test_multipart_store_part_permanent_error_fails_fast() {
        // Unbounded retries: a permanent error must still surface immediately.
        let (flaky, retrying) = multipart_fixture(None, |f| f.with_put_part_permanent_failures(1));
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        let err = upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .expect_err("permanent error should fail fast");
        assert!(matches!(err, object_store::Error::NotSupported { .. }));
        assert_eq!(flaky.put_part_attempts(), 1);
    }

    #[tokio::test]
    async fn test_multipart_create_falls_back_when_attributes_unsupported() {
        let (flaky, retrying) =
            multipart_fixture(None, |f| f.with_multipart_create_attributes_unsupported());
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .expect("create should fall back to attribute-less options");
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), Bytes::from_static(b"part0"));
        // Attributed attempt rejected with NotSupported, then the fallback.
        assert_eq!(flaky.multipart_attempts(), 2);
    }

    #[tokio::test]
    async fn test_multipart_lost_complete_surfaces_without_put_id_attribute() {
        // With the attribute fallback in play the object carries no put id,
        // so a complete whose response was lost cannot be resolved and the
        // NotFound must surface instead of being converted to success.
        let (_, retrying) = multipart_fixture(None, |f| {
            f.with_multipart_create_attributes_unsupported()
                .with_multipart_complete_succeeds_but_returns_not_found()
        });
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .unwrap();
        let err = upload
            .complete()
            .await
            .expect_err("lost complete should surface without a put id to verify");
        assert!(matches!(err, object_store::Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn test_multipart_store_complete_with_in_flight_part_does_not_panic() {
        let (_, retrying) = multipart_fixture(None, |f| f);
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        let part_fut = upload.put_part(PutPayload::from_static(b"part0"));

        // Misuse: complete before the part future resolved. It must error...
        let err = upload
            .complete()
            .await
            .expect_err("complete with an unresolved part should error");
        assert!(matches!(err, object_store::Error::Generic { .. }));

        // ...without disturbing the racing part future or the slots: once
        // the part settles, complete can be retried successfully.
        part_fut.await.unwrap();
        upload
            .complete()
            .await
            .expect("complete should succeed once the part settled");

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), Bytes::from_static(b"part0"));
    }

    #[tokio::test]
    async fn test_multipart_store_complete_transient_failure_retried_without_reupload() {
        let (flaky, retrying) = multipart_fixture(None, |f| f.with_multipart_complete_failures(1));
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part1"))
            .await
            .unwrap();
        upload
            .complete()
            .await
            .expect("complete should succeed after retry");

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(
            got.bytes().await.unwrap(),
            Bytes::from_static(b"part0part1")
        );
        assert_eq!(flaky.multipart_complete_attempts(), 2);
        // Parts are not re-uploaded when only complete fails.
        assert_eq!(flaky.put_part_attempts(), 2);
    }

    #[tokio::test]
    async fn test_multipart_store_complete_succeeds_on_matching_ulid() {
        // Complete finishes server-side but the response is lost (NotFound on
        // the surfaced error); the put-id check should resolve it to success.
        let (flaky, retrying) = multipart_fixture(None, |f| {
            f.with_multipart_complete_succeeds_but_returns_not_found()
        });
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .unwrap();
        upload
            .complete()
            .await
            .expect("complete should succeed via ULID verification");

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), Bytes::from_static(b"part0"));
        assert_eq!(flaky.multipart_complete_attempts(), 1);
    }

    #[tokio::test]
    async fn test_multipart_store_abort_forwards_to_multipart_api() {
        let (flaky, retrying) = multipart_fixture(None, |f| f);
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .unwrap();
        upload.abort().await.expect("abort should succeed");
        assert_eq!(flaky.multipart_abort_attempts(), 1);

        // An explicitly aborted upload is not aborted again on drop.
        drop(upload);
        assert_eq!(wait_for_abort_attempts(&flaky, 1).await, 1);
    }

    /// Yields until the spawned drop-guard abort lands (or a bounded number
    /// of yields passes) and returns the observed abort count.
    async fn wait_for_abort_attempts(flaky: &FlakyObjectStore, expected: usize) -> usize {
        for _ in 0..100 {
            if flaky.multipart_abort_attempts() == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
        flaky.multipart_abort_attempts()
    }

    #[tokio::test]
    async fn test_multipart_store_dropped_unfinished_upload_aborts_best_effort() {
        let (flaky, retrying) = multipart_fixture(None, |f| f);
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .unwrap();

        // Dropped without complete/abort: the guard aborts the upload so it
        // isn't stranded server-side (or as a mirror in the caching layer).
        drop(upload);
        assert_eq!(wait_for_abort_attempts(&flaky, 1).await, 1);
    }

    #[tokio::test]
    async fn test_multipart_store_dropped_completed_upload_does_not_abort() {
        let (flaky, retrying) = multipart_fixture(None, |f| f);
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        drop(upload);
        assert_eq!(wait_for_abort_attempts(&flaky, 1).await, 0);
    }

    #[tokio::test]
    async fn test_buf_writer_recovers_from_transient_part_failures() {
        use tokio::io::AsyncWriteExt;

        let (flaky, retrying) = multipart_fixture(None, |f| f.with_put_part_failures(2));
        let retrying: Arc<dyn ObjectStore> = Arc::new(retrying);
        let path = Path::from("/data/sst");

        let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut writer =
            object_store::buffered::BufWriter::with_capacity(retrying.clone(), path.clone(), 1024);
        writer.write_all(&data).await.unwrap();
        writer.shutdown().await.unwrap();

        let got = retrying.get(&path).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), Bytes::from(data));
        assert!(flaky.put_part_attempts() > 2);
        assert_eq!(flaky.multipart_attempts(), 1);
    }

    #[tokio::test]
    async fn test_multipart_without_multipart_store_does_not_retry_parts() {
        // Without the low-level handle, part failures surface to the caller;
        // only the inner client's own retries apply.
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_put_part_failures(1));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), None);
        let path = Path::from("/data/multipart");

        let mut upload = retrying
            .put_multipart_opts(&path, Default::default())
            .await
            .unwrap();
        let err = upload
            .put_part(PutPayload::from_static(b"part0"))
            .await
            .expect_err("part failure should surface without a MultipartStore");
        assert!(matches!(err, object_store::Error::Generic { .. }));
        assert_eq!(flaky.put_part_attempts(), 1);
    }

    #[tokio::test]
    async fn test_bounded_max_retries_gives_up_instead_of_retrying_forever() {
        // Store fails more times (5) than the configured retry bound (2).
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 5));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock(), Some(2));

        let path = Path::from("/data/obj");
        let err = retrying
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"hello")),
                PutOptions::default(),
            )
            .await
            .expect_err("bounded retries should exhaust and surface the underlying error");

        // The underlying transient error is returned rather than being retried
        // forever, so callers/background tasks can fail fast.
        assert!(matches!(err, object_store::Error::Generic { .. }));
        // 1 initial attempt + 2 retries = 3 total attempts.
        assert_eq!(flaky.put_attempts(), 3);
    }
}
