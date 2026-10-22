use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::utils::spawn_with_optional_subscriber;

#[derive(Debug, Default)]
struct UploadState {
    upload: Mutex<Option<Box<dyn MultipartUpload>>>,
    cancel: CancellationToken,
}

impl UploadState {
    async fn abort(&self) -> object_store::Result<()> {
        self.cancel.cancel();
        if let Some(mut upload) = self.upload.lock().await.take() {
            upload.abort().await?;
        }
        Ok(())
    }
}

/// Keeps the upload handle available after `BufWriter` starts shutdown.
pub(crate) struct SstUploadGuard {
    state: Arc<UploadState>,
    armed: bool,
}

impl SstUploadGuard {
    pub(crate) fn new(store: Arc<dyn ObjectStore>) -> (Arc<dyn ObjectStore>, Self) {
        let state = Arc::new(UploadState::default());
        let store = Arc::new(UploadTrackingStore {
            inner: store,
            state: Arc::clone(&state),
        });
        (store, Self { state, armed: true })
    }

    pub(crate) async fn abort(&mut self) -> object_store::Result<()> {
        self.armed = false;
        self.state.abort().await
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SstUploadGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.state.cancel.cancel();
        if tokio::runtime::Handle::try_current().is_ok() {
            let state = Arc::clone(&self.state);
            spawn_with_optional_subscriber(async move {
                if let Err(error) = state.abort().await {
                    log::warn!("failed to abort dropped SST upload [error={error:?}]");
                }
            });
        }
    }
}

#[derive(Debug)]
struct TrackedMultipartUpload {
    state: Arc<UploadState>,
}

#[async_trait]
impl MultipartUpload for TrackedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // Parts start before completion. Cancellation drops the writer first.
        self.state
            .upload
            .try_lock()
            .expect("multipart upload is not completing")
            .as_mut()
            .expect("multipart upload is active")
            .put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut guard = self.state.upload.lock().await;
        let result = guard
            .as_mut()
            .expect("multipart upload is active")
            .complete()
            .await;
        if result.is_ok() {
            guard.take();
        }
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.state.abort().await
    }
}

#[derive(Debug)]
struct UploadTrackingStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<UploadState>,
}

impl fmt::Display for UploadTrackingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UploadTrackingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for UploadTrackingStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let store = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        let path = path.clone();
        // Keep creation alive if the caller cancels before it returns a handle.
        spawn_with_optional_subscriber(async move {
            let upload = store.put_multipart_opts(&path, options).await?;
            *state.upload.lock().await = Some(upload);
            if state.cancel.is_cancelled() {
                if let Err(error) = state.abort().await {
                    log::warn!("failed to abort cancelled SST upload creation [error={error:?}]");
                    return Err(error);
                }
                return Err(object_store::Error::Generic {
                    store: "SST upload",
                    source: "SST upload was cancelled".into(),
                });
            }
            Ok(Box::new(TrackedMultipartUpload { state }) as Box<dyn MultipartUpload>)
        })
        .await
        .map_err(|source| {
            if source.is_panic() {
                std::panic::resume_unwind(source.into_panic());
            }
            object_store::Error::JoinError { source }
        })?
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
