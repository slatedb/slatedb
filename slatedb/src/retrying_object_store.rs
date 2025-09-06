use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use futures::stream::BoxStream;
use log::{debug, info, warn};
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
        ExponentialBuilder::default().with_total_delay(Some(Duration::from_secs(300)))
    }

    #[inline]
    fn notify(err: &object_store::Error, duration: Duration) {
        info!(
            "retrying object store operation [error={:?}, duration={:?}]",
            err, duration
        );
    }

    #[inline]
    fn should_retry_os_error(err: &object_store::Error) -> bool {
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
            self.inner.get_opts(&location, options.clone()).await
        })
        .retry(Self::retry_builder())
        .notify(Self::notify)
        .when(Self::should_retry_os_error)
        .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        (|| async { self.inner.head(&location).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
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
                .put_opts(&location, payload.clone(), opts.clone())
                .await
        })
        .retry(Self::retry_builder())
        .notify(Self::notify)
        .when(Self::should_retry_os_error)
        .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        (|| async { self.inner.put_multipart(&location).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        (|| async { self.inner.put_multipart_opts(&location, opts.clone()).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        (|| async { self.inner.delete(&location).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // Delegates directly; listing returns a stream so per-page retry is out of scope here.
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // Delegates directly; listing returns a stream so per-page retry is out of scope here.
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        (|| async { self.inner.list_with_delimiter(prefix).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.copy(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.rename(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.copy_if_not_exists(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        (|| async { self.inner.rename_if_not_exists(from, to).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry_os_error)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::RetryingObjectStore;
    use crate::test_utils::FlakyObjectStore;
    use bytes::Bytes;
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
}
