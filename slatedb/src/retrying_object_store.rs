use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use futures::stream::BoxStream;
use futures::{stream, StreamExt, TryStreamExt};
use log::{debug, info};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// A thin wrapper around an `ObjectStore` that retries transient errors with
/// exponential backoff for up to 5 minutes.
#[derive(Debug, Clone)]
pub(crate) struct RetryingObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl RetryingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    #[inline]
    fn retry_builder() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .without_max_times()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(1))
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
                | object_store::Error::NotImplemented
        );
        if !retry {
            debug!("not retrying object store operation [error={:?}]", err);
        }
        retry
    }
}

impl std::fmt::Display for RetryingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetryingObjectStore({})", self.inner)
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
        .notify(Self::notify)
        .when(Self::should_retry)
        .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        (|| async { self.inner.head(location).await })
            .retry(Self::retry_builder())
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
        (|| async {
            self.inner
                .put_opts(location, payload.clone(), opts.clone())
                .await
        })
        .retry(Self::retry_builder())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        (|| async { self.inner.put_multipart(location).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        (|| async { self.inner.put_multipart_opts(location, opts.clone()).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        (|| async { self.inner.delete(location).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
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
            .retry(Self::retry_builder())
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
                    .map(|meta| Ok::<ObjectMeta, object_store::Error>(meta)),
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
        let prefix_owned = prefix.cloned();
        let offset_owned = offset.clone();

        // See the comment in list() for details on why we do this.
        stream::once(async move {
            (|| async {
                let stream = inner.list_with_offset(prefix_owned.as_ref(), &offset_owned);
                stream.try_collect::<Vec<_>>().await
            })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
        })
        .map_ok(|entries| {
            stream::iter(
                entries
                    .into_iter()
                    .map(|meta| Ok::<ObjectMeta, object_store::Error>(meta)),
            )
            .boxed()
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        (|| async { self.inner.list_with_delimiter(prefix).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.copy(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.rename(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.copy_if_not_exists(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.rename_if_not_exists(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::RetryingObjectStore;
    use crate::test_utils::FlakyObjectStore;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_put_opts_retries_transient_until_success() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 1));
        let retrying = RetryingObjectStore::new(flaky.clone());

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
    async fn test_put_opts_does_not_retry_on_already_exists() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(inner, 0));
        let retrying = RetryingObjectStore::new(flaky.clone());
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
        let retrying = RetryingObjectStore::new(flaky.clone());

        let meta = retrying.head(&path).await.expect("head should succeed");
        assert_eq!(meta.size, 4);
        assert_eq!(flaky.head_attempts(), 2);
    }

    #[tokio::test]
    async fn test_put_opts_does_not_retry_on_precondition() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let failing = Arc::new(FlakyObjectStore::new(inner, 0).with_put_precondition_always());
        let retrying = RetryingObjectStore::new(failing.clone());
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
        let retrying = RetryingObjectStore::new(flaky.clone());

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
        let retrying = RetryingObjectStore::new(flaky.clone());
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
}
