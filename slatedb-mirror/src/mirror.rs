//! `ObjectStoreMirror`, its builder, and its `ObjectStore` implementation.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};
use slatedb_common::clock::{DefaultSystemClock, SystemClock};

use crate::error::MIRROR_STORE_NAME;
use crate::inner::{local_error, Inner};
use crate::layout::{LocalName, LocalObject, LOCK_FILE};
use crate::multipart::{self, committed};
use crate::ordering::OpKind;
use crate::retry::Retry;
use crate::startup::recover;
use crate::vfs::{StdVfs, Vfs};
use crate::{scan, MirrorError, MirrorHandle, MirrorPolicy, ReadRoute, WriteRoute};

/// The default for [`ObjectStoreMirrorBuilder::with_download_concurrency`].
pub const DEFAULT_DOWNLOAD_CONCURRENCY: usize = 8;

/// The default for [`ObjectStoreMirrorBuilder::with_remote_scan_interval`].
pub const DEFAULT_REMOTE_SCAN_INTERVAL: Duration = Duration::from_secs(600);

/// Wraps a route error from the policy in `MirrorError::Policy`, unless it's
/// already a mirror error.
fn policy_error(err: object_store::Error) -> object_store::Error {
    if MirrorError::find(&err).is_some() {
        err
    } else {
        MirrorError::Policy {
            source: Box::new(err),
        }
        .into()
    }
}

fn range_error(source: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: MIRROR_STORE_NAME,
        source: Box::new(source),
    }
}

fn bytes_payload(bytes: Bytes) -> GetResultPayload {
    GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed())
}

/// A whole-object local mirror of an object store.
///
/// A [`MirrorPolicy`] picks a route for every GET, HEAD, PUT, and multipart
/// upload. The mirror keeps local copies byte-for-byte identical to the remote
/// objects, and applies operations on the same path in the order they were
/// issued.
///
/// Dropping the mirror stops the remote scan and `MirrorPolicy::run`.
pub struct ObjectStoreMirror {
    handle: MirrorHandle,
}

impl ObjectStoreMirror {
    pub fn builder(
        local_dir: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
        policy: Arc<dyn MirrorPolicy>,
    ) -> ObjectStoreMirrorBuilder {
        ObjectStoreMirrorBuilder {
            local_dir: local_dir.into(),
            remote: object_store,
            policy,
            vfs: Arc::new(StdVfs::new()),
            clock: Arc::new(DefaultSystemClock::new()),
            download_concurrency: DEFAULT_DOWNLOAD_CONCURRENCY,
            remote_scan_interval: Some(DEFAULT_REMOTE_SCAN_INTERVAL),
            max_retries: None,
        }
    }

    /// A handle to this mirror, the same one the policy gets.
    pub fn handle(&self) -> &MirrorHandle {
        &self.handle
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.handle.inner
    }

    async fn get_observed(
        &self,
        location: &Path,
        mut options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let inner = self.inner();
        if options.head {
            return inner.remote.get_opts(location, options).await;
        }
        // `observe` needs the whole object. A `NotModified` or other error
        // returns here, before `observe`.
        let range = options.range.take();
        let result = inner.remote.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let bytes = result.bytes().await?;
        let range = match range {
            Some(range) => range.as_range(bytes.len() as u64).map_err(range_error)?,
            None => 0..bytes.len() as u64,
        };
        inner.policy.observe(location, &bytes, &self.handle).await?;
        Ok(GetResult {
            payload: bytes_payload(bytes.slice(range.start as usize..range.end as usize)),
            meta,
            range,
            attributes,
            extensions,
        })
    }

    async fn get_local(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let inner = self.inner();
        let Some(object) = inner.local(location) else {
            return Err(MirrorError::NotLocal {
                path: location.clone(),
            }
            .into());
        };
        inner.read_local(location, &object, &options).await
    }

    async fn get_refetched(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let inner = self.inner();
        let ticket = inner
            .ordering
            .register(location, OpKind::Download)
            .expect("downloads are always queued");
        ticket.ready().await;
        let object = inner.download(location).await?;
        // Keep the ticket until the read is done, so an eviction can't remove
        // the file underneath it.
        let result = inner.read_local(location, &object, &options).await;
        drop(ticket);
        result
    }

    async fn put_mirrored(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let inner = self.inner();
        let name = LocalName::new(location)?;
        inner.prefixes.lock().check_or_insert(&name)?;
        let ticket = inner
            .ordering
            .register(location, OpKind::Write)
            .expect("writes are always queued");
        ticket.ready().await;

        let size = payload.content_length() as u64;
        let attributes = opts.attributes.clone();
        let temp = inner.temp_file(&name);
        let (local, remote) = future::join(
            inner.write_payload(&temp.path, &payload),
            inner.remote.put_opts(location, payload.clone(), opts),
        )
        .await;
        let put = match remote {
            Ok(put) => put,
            Err(err) => {
                temp.remove().await;
                return Err(err);
            }
        };
        if let Err(err) = local {
            temp.remove().await;
            return Err(committed(location, local_error(err)));
        }

        let object = LocalObject {
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: inner.clock.now(),
                size,
                e_tag: put.e_tag.clone(),
                version: put.version.clone(),
            },
            attributes,
        };
        inner
            .install(location, &name, temp, object)
            .await
            .map_err(|err| committed(location, err))?;
        drop(ticket);
        Ok(put)
    }
}

impl Drop for ObjectStoreMirror {
    fn drop(&mut self) {
        self.inner().shutdown.cancel();
    }
}

impl fmt::Debug for ObjectStoreMirror {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreMirror")
            .field("root", &self.inner().root)
            .field("remote", &self.inner().remote)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ObjectStoreMirror {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectStoreMirror({})", self.inner().remote)
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreMirror {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let inner = self.inner();
        let route = inner
            .policy
            .put_route(location, &opts)
            .map_err(policy_error)?;
        match route {
            WriteRoute::Remote => inner.remote.put_opts(location, payload, opts).await,
            WriteRoute::Observe => {
                let bytes = Bytes::from(payload.clone());
                let put = inner.remote.put_opts(location, payload, opts).await?;
                inner
                    .policy
                    .observe(location, &bytes, &self.handle)
                    .await
                    .map_err(|err| committed(location, err))?;
                Ok(put)
            }
            WriteRoute::Mirror => self.put_mirrored(location, payload, opts).await,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let inner = self.inner();
        let route = inner
            .policy
            .put_multipart_route(location, &opts)
            .map_err(policy_error)?;
        match route {
            WriteRoute::Remote => inner.remote.put_multipart_opts(location, opts).await,
            WriteRoute::Observe => Err(MirrorError::Unsupported {
                operation: "multipart upload with the Observe route",
            }
            .into()),
            WriteRoute::Mirror => multipart::start(Arc::clone(inner), location, opts).await,
        }
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let inner = self.inner();
        let route = inner
            .policy
            .read_route(location, &options)
            .map_err(policy_error)?;
        match route {
            ReadRoute::Remote => inner.remote.get_opts(location, options).await,
            ReadRoute::Observe => self.get_observed(location, options).await,
            ReadRoute::Local => self.get_local(location, options).await,
            ReadRoute::Refetch => self.get_refetched(location, options).await,
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(self.inner());
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                async move {
                    let location = location?;
                    // Neither side's failure stops the other.
                    let (remote, local) = future::join(
                        inner.remote.delete(&location),
                        inner.delete_local(&location),
                    )
                    .await;
                    remote?;
                    local?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner().remote.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner().remote.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner().remote.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(MirrorError::Unsupported { operation: "copy" }.into())
    }

    async fn rename_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: RenameOptions,
    ) -> object_store::Result<()> {
        Err(MirrorError::Unsupported {
            operation: "rename",
        }
        .into())
    }
}

/// Configures and builds an [`ObjectStoreMirror`].
pub struct ObjectStoreMirrorBuilder {
    local_dir: PathBuf,
    remote: Arc<dyn ObjectStore>,
    policy: Arc<dyn MirrorPolicy>,
    vfs: Arc<dyn Vfs>,
    clock: Arc<dyn SystemClock>,
    download_concurrency: usize,
    remote_scan_interval: Option<Duration>,
    max_retries: Option<u32>,
}

impl fmt::Debug for ObjectStoreMirrorBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreMirrorBuilder")
            .field("local_dir", &self.local_dir)
            .field("remote", &self.remote)
            .field("vfs", &self.vfs)
            .field("download_concurrency", &self.download_concurrency)
            .field("remote_scan_interval", &self.remote_scan_interval)
            .field("max_retries", &self.max_retries)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreMirrorBuilder {
    /// Sets the virtual filesystem used for local I/O. The default is
    /// [`StdVfs`].
    pub fn with_vfs(mut self, vfs: Arc<dyn Vfs>) -> Self {
        self.vfs = vfs;
        self
    }

    /// Sets the maximum number of concurrent downloads used by `fetch`,
    /// `prefetch`, and `Refetch`. The default is
    /// [`DEFAULT_DOWNLOAD_CONCURRENCY`].
    pub fn with_download_concurrency(mut self, concurrency: usize) -> Self {
        self.download_concurrency = concurrency;
        self
    }

    /// Sets the interval of the remote scan that removes local copies of
    /// objects deleted remotely. The default is
    /// [`DEFAULT_REMOTE_SCAN_INTERVAL`]. `None` disables the scan.
    ///
    /// The scan assumes strongly consistent reads, writes, deletes, and
    /// listings. Disable it if the remote store doesn't provide them.
    pub fn with_remote_scan_interval(mut self, interval: Option<Duration>) -> Self {
        self.remote_scan_interval = interval;
        self
    }

    /// Sets the clock used for retry backoff, the remote scan, and the
    /// last-modified time of mirrored writes. The default uses Tokio's clock.
    pub fn with_system_clock(mut self, clock: Arc<dyn SystemClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets how many times the mirror retries a transient error from its own
    /// remote calls (downloads and remote-scan LISTs). The default, `None`,
    /// retries forever, like SlateDB's `object_store_max_retries`.
    pub fn with_max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Validates the configuration, acquires the cache-directory lock, cleans
    /// invalid local entries, and spawns the remote scan and
    /// `MirrorPolicy::run`.
    pub async fn build(self) -> object_store::Result<Arc<ObjectStoreMirror>> {
        let invalid = |message: &str| -> object_store::Error {
            MirrorError::InvalidConfig {
                message: message.to_string(),
            }
            .into()
        };
        if self.download_concurrency == 0 {
            return Err(invalid("download_concurrency must be greater than zero"));
        }
        if self.remote_scan_interval == Some(Duration::ZERO) {
            return Err(invalid("remote_scan_interval must be greater than zero"));
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| invalid("the mirror must be built inside a Tokio runtime"))?;

        self.vfs
            .create_dir_all(&self.local_dir)
            .await
            .map_err(local_error)?;
        let lock = self
            .vfs
            .lock(&self.local_dir.join(LOCK_FILE))
            .await
            .map_err(local_error)?;
        let recovered = recover(self.vfs.as_ref(), &self.local_dir).await?;

        let inner = Arc::new(Inner::new(
            self.local_dir,
            self.remote,
            self.policy,
            self.vfs,
            Arc::clone(&self.clock),
            Retry::new(self.clock, self.max_retries),
            runtime,
            recovered.entries,
            recovered.prefixes,
            self.download_concurrency,
            lock,
        ));
        if let Some(interval) = self.remote_scan_interval {
            inner.spawn(scan::run(Arc::clone(&inner), interval));
        }
        let handle = MirrorHandle {
            inner: Arc::clone(&inner),
        };
        inner.spawn(Arc::clone(&inner.policy).run(handle.clone()));
        Ok(Arc::new(ObjectStoreMirror { handle }))
    }
}

/// Mirrors every object. For unit tests.
#[cfg(test)]
struct MirrorAll;

#[cfg(test)]
#[async_trait]
impl MirrorPolicy for MirrorAll {
    fn read_route(&self, _path: &Path, _options: &GetOptions) -> object_store::Result<ReadRoute> {
        Ok(ReadRoute::Local)
    }

    fn put_route(&self, _path: &Path, _options: &PutOptions) -> object_store::Result<WriteRoute> {
        Ok(WriteRoute::Mirror)
    }

    fn put_multipart_route(
        &self,
        _path: &Path,
        _options: &PutMultipartOptions,
    ) -> object_store::Result<WriteRoute> {
        Ok(WriteRoute::Mirror)
    }
}

#[cfg(test)]
impl ObjectStoreMirror {
    /// Builds a mirror of `remote` in `root` that mirrors every object and has
    /// no remote scan. For unit tests.
    pub(crate) async fn for_test(
        root: &std::path::Path,
        remote: Arc<dyn ObjectStore>,
    ) -> Arc<Self> {
        Self::builder(root, remote, Arc::new(MirrorAll))
            .with_remote_scan_interval(None)
            .build()
            .await
            .unwrap()
    }
}
