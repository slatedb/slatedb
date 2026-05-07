use std::borrow::Cow;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, Retryable, Sleeper};
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use log::{debug, info};
use object_store::path::Path;
use object_store::{
    Attribute, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::rand::DbRand;
use crate::utils::IdGenerator;
use slatedb_common::clock::SystemClock;

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
/// exponential backoff forever using the configured [`SystemClock`] for sleeps.
#[derive(Debug, Clone)]
pub(crate) struct RetryingObjectStore {
    inner: Arc<dyn ObjectStore>,
    rand: Arc<DbRand>,
    clock: Arc<dyn SystemClock>,
}

impl RetryingObjectStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        rand: Arc<DbRand>,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self { inner, rand, clock }
    }

    #[inline]
    fn retry_builder() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .without_max_times()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(1))
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
    fn should_retry(err: &object_store::Error) -> bool {
        let retry = !matches!(
            err,
            object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotFound { .. }
                | object_store::Error::NotImplemented
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
            .retry(Self::retry_builder())
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

    /// Retrying list stream that resumes pagination after a transient error.
    ///
    /// `list()` paginates and returns a stream of results, so the underlying call can
    /// fail mid-iteration. Rather than discarding partial progress and restarting from
    /// page 1 (which costs us the latency for every already-paginated page on every
    /// retry), we track the last successfully yielded path and, on a retryable error,
    /// sleep for backoff and re-open the stream via `list_with_offset(prefix, last)` —
    /// `list_with_offset` returns objects with location strictly greater than `offset`,
    /// so the caller observes a single, continuous, deduplicated stream.
    ///
    /// If `initial_offset` is `Some`, the first attempt is itself a `list_with_offset`,
    /// reproducing the user-requested offset semantics. After we've yielded any item,
    /// `last` advances past `initial_offset` and is used for subsequent retries.
    fn retrying_list_stream(
        inner: Arc<dyn ObjectStore>,
        clock: Arc<dyn SystemClock>,
        prefix: Option<Path>,
        initial_offset: Option<Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        struct StreamState {
            stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
            last: Option<Path>,
            initial_offset: Option<Path>,
            backoff: ExponentialBackoff,
        }

        fn open_stream(
            inner: &Arc<dyn ObjectStore>,
            prefix: Option<&Path>,
            offset: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            match offset {
                Some(o) => inner.list_with_offset(prefix, o),
                None => inner.list(prefix),
            }
        }

        let initial_stream = open_stream(&inner, prefix.as_ref(), initial_offset.as_ref());
        let state = Some(StreamState {
            stream: initial_stream,
            last: None,
            initial_offset,
            backoff: Self::retry_builder().build(),
        });

        stream::unfold(state, move |state| {
            let inner = Arc::clone(&inner);
            let clock = Arc::clone(&clock);
            let prefix = prefix.clone();
            async move {
                let mut s = state?;
                loop {
                    match s.stream.next().await {
                        None => return None,
                        Some(Ok(item)) => {
                            s.last = Some(item.location.clone());
                            return Some((Ok(item), Some(s)));
                        }
                        Some(Err(e)) => {
                            if !Self::should_retry(&e) {
                                return Some((Err(e), None));
                            }
                            let delay = s.backoff.next().unwrap_or(Duration::from_secs(1));
                            Self::notify(&e, delay);
                            clock.sleep(delay).await;
                            let resume_from = s.last.as_ref().or(s.initial_offset.as_ref());
                            s.stream = open_stream(&inner, prefix.as_ref(), resume_from);
                        }
                    }
                }
            }
        })
        .boxed()
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
}

impl std::fmt::Display for RetryingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetryingObjectStore({})", self.inner)
    }
}

/// Wrapper around MultipartUpload that adds ULID verification on complete() failure.
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

        match &result {
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {
                if let Some(meta) = self
                    .retrying_store
                    .verify_put_succeeded(&self.location, &self.put_id)
                    .await
                {
                    return Ok(PutResult {
                        e_tag: meta.e_tag,
                        version: meta.version,
                    });
                }
                result
            }
            _ => result,
        }
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for RetryingObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        (|| async {
            // Options and location must be owned per-attempt.
            self.inner.get_opts(location, options.clone()).await
        })
        .retry(Self::retry_builder())
        .sleep(self.sleeper())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        (|| async { self.inner.get_range(location, range.clone()).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        (|| async { self.inner.get_ranges(location, ranges).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        (|| async { self.inner.head(location).await })
            .retry(Self::retry_builder())
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
        .retry(Self::retry_builder())
        .sleep(self.sleeper())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await;

        // If attributes aren't supported, fall back to put without ULID
        if matches!(
            &result,
            Err(object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented)
        ) && put_id.is_some()
        {
            return (|| async {
                self.inner
                    .put_opts(location, payload.clone(), opts.clone())
                    .await
            })
            .retry(Self::retry_builder())
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
                    })
                } else {
                    result
                }
            }
            _ => result,
        }
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default())
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let put_id = self.rand.rng().gen_ulid(self.clock.as_ref()).to_string();
        let opts_with_id = PutMultipartOptions {
            attributes: Self::with_put_id(opts.attributes.clone(), &put_id),
            ..opts.clone()
        };

        let result = (|| async {
            self.inner
                .put_multipart_opts(location, opts_with_id.clone())
                .await
        })
        .retry(Self::retry_builder())
        .sleep(self.sleeper())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await;

        // If attributes aren't supported, fall back without ULID
        let inner = match result {
            Ok(inner) => inner,
            Err(object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented) => {
                (|| async { self.inner.put_multipart_opts(location, opts.clone()).await })
                    .retry(Self::retry_builder())
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

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        (|| async { self.inner.delete(location).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        Self::retrying_list_stream(
            Arc::clone(&self.inner),
            Arc::clone(&self.clock),
            prefix.cloned(),
            None,
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        Self::retrying_list_stream(
            Arc::clone(&self.inner),
            Arc::clone(&self.clock),
            prefix.cloned(),
            Some(offset.clone()),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        (|| async { self.inner.list_with_delimiter(prefix).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.copy(from, to).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.rename(from, to).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.copy_if_not_exists(from, to).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.rename_if_not_exists(from, to).await })
            .retry(Self::retry_builder())
            .sleep(self.sleeper())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::RetryingObjectStore;
    use crate::rand::DbRand;
    use crate::test_utils::FlakyObjectStore;
    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());

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
    async fn test_put_opts_retry_sleep_uses_system_clock() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 1));
        let clock = Arc::new(MockSystemClock::new());
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), clock.clone());
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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());
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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());

        let meta = retrying.head(&path).await.expect("head should succeed");
        assert_eq!(meta.size, 4);
        assert_eq!(flaky.head_attempts(), 2);
    }

    #[tokio::test]
    async fn test_put_opts_does_not_retry_on_precondition() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let failing = Arc::new(FlakyObjectStore::new(inner, 0).with_put_precondition_always());
        let retrying = RetryingObjectStore::new(failing.clone(), test_rand(), test_clock());
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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());

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
        // Initial list() failed after 1 item; retry resumed via list_with_offset.
        assert_eq!(flaky.list_attempts(), 1);
        assert_eq!(flaky.list_with_offset_attempts(), 1);
    }

    #[tokio::test]
    async fn test_list_failure_mid_pagination_resumes_from_offset() {
        // Verifies the fix for the bug where a single transient error mid-pagination
        // caused RetryingObjectStore::list to restart pagination from page 1, wasting
        // all work already done. The fix: track the last successfully yielded path and
        // resume via list_with_offset on retry, so progress is durable across attempts.
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let paths: Vec<Path> = (0..6)
            .map(|i| Path::from(format!("/items/{i:02}")))
            .collect();
        for (idx, path) in paths.iter().enumerate() {
            inner
                .put(
                    path,
                    PutPayload::from_bytes(Bytes::from(format!("val-{idx}").into_bytes())),
                )
                .await
                .unwrap();
        }

        // Fail once after the inner stream has yielded 3 items.
        let flaky = Arc::new(FlakyObjectStore::new(inner, 0).with_list_failures(1, 3));
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());

        let listed: Vec<_> = retrying
            .list(None)
            .try_collect()
            .await
            .expect("list should eventually succeed");

        // Caller sees exactly the 6 distinct objects.
        assert_eq!(listed.len(), 6);
        let mut names: Vec<_> = listed.into_iter().map(|m| m.location.to_string()).collect();
        names.sort();
        let mut expected: Vec<_> = paths.iter().map(|p| p.to_string()).collect();
        expected.sort();
        assert_eq!(names, expected);

        // Initial attempt called list() once (and failed mid-pagination); the retry
        // resumed via list_with_offset(prefix, last_seen_path).
        assert_eq!(flaky.list_attempts(), 1);
        assert_eq!(flaky.list_with_offset_attempts(), 1);

        // No wasted pagination: the inner stream produced 3 items on the failed attempt,
        // then 3 more on the resumed attempt — total 6, matching the object count.
        // Before the fix this would have been 9 (3 failed + 6 full restart).
        assert_eq!(flaky.list_items_yielded(), 6);
    }

    #[tokio::test]
    async fn test_list_propagates_non_retryable_error() {
        // A non-retryable error (e.g. NotFound) should be surfaced to the caller after
        // any items already yielded, not retried indefinitely.
        struct OneItemThenNotFound;

        #[async_trait::async_trait]
        impl object_store::ObjectStore for OneItemThenNotFound {
            async fn put_opts(
                &self,
                _: &Path,
                _: PutPayload,
                _: PutOptions,
            ) -> object_store::Result<object_store::PutResult> {
                unimplemented!()
            }
            async fn put_multipart_opts(
                &self,
                _: &Path,
                _: object_store::PutMultipartOptions,
            ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
                unimplemented!()
            }
            async fn get_opts(
                &self,
                _: &Path,
                _: object_store::GetOptions,
            ) -> object_store::Result<object_store::GetResult> {
                unimplemented!()
            }
            async fn delete(&self, _: &Path) -> object_store::Result<()> {
                unimplemented!()
            }
            fn list(
                &self,
                _: Option<&Path>,
            ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
            {
                use futures::StreamExt;
                let item = Ok(object_store::ObjectMeta {
                    location: Path::from("/x"),
                    last_modified: chrono::Utc::now(),
                    size: 0,
                    e_tag: None,
                    version: None,
                });
                let err = Err(object_store::Error::NotFound {
                    path: "/missing".into(),
                    source: Box::new(std::io::Error::other("missing")),
                });
                futures::stream::iter(vec![item, err]).boxed()
            }
            fn list_with_offset(
                &self,
                _: Option<&Path>,
                _: &Path,
            ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
            {
                unimplemented!("retry should not be issued for non-retryable error")
            }
            async fn list_with_delimiter(
                &self,
                _: Option<&Path>,
            ) -> object_store::Result<object_store::ListResult> {
                unimplemented!()
            }
            async fn copy(&self, _: &Path, _: &Path) -> object_store::Result<()> {
                unimplemented!()
            }
            async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> object_store::Result<()> {
                unimplemented!()
            }
        }

        impl std::fmt::Display for OneItemThenNotFound {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "OneItemThenNotFound")
            }
        }
        impl std::fmt::Debug for OneItemThenNotFound {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "OneItemThenNotFound")
            }
        }

        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(OneItemThenNotFound);
        let retrying = RetryingObjectStore::new(inner, test_rand(), test_clock());

        let mut stream = retrying.list(None);
        let first = stream.next().await.expect("first item");
        assert!(first.is_ok());
        let second = stream.next().await.expect("error");
        match second {
            Err(object_store::Error::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
        assert!(stream.next().await.is_none(), "stream should terminate");
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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());
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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());
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
        let retrying = RetryingObjectStore::new(inner.clone(), test_rand(), test_clock());
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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());

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
        let retrying = RetryingObjectStore::new(flaky.clone(), test_rand(), test_clock());

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
        let retrying = RetryingObjectStore::new(inner.clone(), test_rand(), test_clock());
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
}
