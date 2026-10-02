//! The policy that decides which objects the mirror keeps.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, PutMultipartOptions, PutOptions};

use crate::MirrorHandle;

/// Where a GET or HEAD is served from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRoute {
    /// Wrapped store only.
    Remote,
    /// Wrapped store. The whole object goes to [`MirrorPolicy::observe`]
    /// before the caller gets a result. HEADs and GETs that return no body
    /// skip `observe`.
    Observe,
    /// Local copy only. A miss is a `MirrorError::NotLocal`.
    Local,
    /// Download the whole object again, replace the local copy, and serve the
    /// request from it.
    Refetch,
}

/// Where a PUT or multipart upload goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRoute {
    /// Wrapped store only.
    Remote,
    /// Wrapped store, then the payload goes to [`MirrorPolicy::observe`].
    /// Not supported for multipart uploads.
    Observe,
    /// Write to both. Returns after the upload succeeds and the local file is
    /// in place.
    Mirror,
}

/// Decides which objects an `ObjectStoreMirror` keeps locally.
///
/// Policies must only route paths that are never overwritten to
/// [`ReadRoute::Local`], [`ReadRoute::Refetch`], or [`WriteRoute::Mirror`].
///
/// Route errors that don't already carry a `MirrorError` reach the caller as
/// `MirrorError::Policy`. `observe` errors reach the caller unchanged on reads
/// and as `MirrorError::WriteCommitted` on writes.
#[async_trait]
pub trait MirrorPolicy: Send + Sync + 'static {
    /// Runs on every GET and HEAD. `options.head` is true for HEAD. Must be
    /// cheap.
    fn read_route(&self, path: &Path, options: &GetOptions) -> object_store::Result<ReadRoute>;

    /// Runs on every PUT. Must be cheap.
    fn put_route(&self, path: &Path, options: &PutOptions) -> object_store::Result<WriteRoute>;

    /// Runs on every multipart upload. Must be cheap. [`WriteRoute::Observe`]
    /// isn't supported here and fails the upload with
    /// `MirrorError::Unsupported`.
    fn put_multipart_route(
        &self,
        path: &Path,
        options: &PutMultipartOptions,
    ) -> object_store::Result<WriteRoute>;

    /// Runs for `Observe` routes, after the remote call succeeds and before the
    /// caller gets the result. `bytes` is the whole object.
    async fn observe(
        &self,
        path: &Path,
        bytes: &Bytes,
        mirror: &MirrorHandle,
    ) -> object_store::Result<()> {
        let _ = (path, bytes, mirror);
        Ok(())
    }

    /// Runs in the background for the life of the mirror. `build()` spawns
    /// this task and it's cancelled when the mirror is dropped.
    ///
    /// The handle keeps the mirror's local state (and its `LOCK`) alive, so a
    /// policy shouldn't store it anywhere that outlives this call.
    async fn run(self: Arc<Self>, mirror: MirrorHandle) {
        let _ = mirror;
    }
}
