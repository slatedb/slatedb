//! Internal object-store wrapper that records object timestamps using a shared
//! deterministic clock.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::RwLock;
use slatedb_common::clock::SystemClock;

/// ObjectStore wrapper that overrides metadata times using a provided SystemClock.
/// - Records timestamps for mutating operations (put, copy, rename, delete).
/// - Uses recorded timestamps for `last_modified` in `head` and `list` responses.
#[derive(Clone)]
pub(crate) struct ClockedObjectStore {
    inner: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
    times: Arc<RwLock<HashMap<Path, DateTime<Utc>>>>,
}

impl ClockedObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, clock: Arc<dyn SystemClock>) -> Self {
        Self {
            inner,
            clock,
            times: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn record_modified(&self, path: &Path) -> DateTime<Utc> {
        let now = self.clock.now();
        let mut guard = self.times.write();
        guard
            .entry(path.clone())
            .and_modify(|time| *time = now)
            .or_insert(now);
        now
    }

    fn remove(&self, path: &Path) {
        self.times.write().remove(path);
    }

    fn with_recorded_times(&self, meta: ObjectMeta) -> ObjectMeta {
        match self.times.read().get(&meta.location).copied() {
            Some(last_modified) => ObjectMeta {
                last_modified,
                ..meta
            },
            None => meta,
        }
    }
}

impl fmt::Debug for ClockedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClockedObjectStore({})", self.inner)
    }
}

impl fmt::Display for ClockedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClockedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ClockedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        result.meta = self.with_recorded_times(result.meta);
        Ok(result)
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        Ok(self.with_recorded_times(self.inner.head(location).await?))
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.record_modified(location);
        Ok(result)
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.record_modified(location);
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.record_modified(location);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await?;
        self.remove(location);
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let times = Arc::clone(&self.times);
        self.inner
            .list(prefix)
            .map(move |result| {
                result.map(|meta| match times.read().get(&meta.location).copied() {
                    Some(last_modified) => ObjectMeta {
                        last_modified,
                        ..meta
                    },
                    None => meta,
                })
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let times = Arc::clone(&self.times);
        self.inner
            .list_with_offset(prefix, offset)
            .map(move |result| {
                result.map(|meta| match times.read().get(&meta.location).copied() {
                    Some(last_modified) => ObjectMeta {
                        last_modified,
                        ..meta
                    },
                    None => meta,
                })
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        let times = self.times.read();
        result.objects = result
            .objects
            .into_iter()
            .map(|meta| match times.get(&meta.location).copied() {
                Some(last_modified) => ObjectMeta {
                    last_modified,
                    ..meta
                },
                None => meta,
            })
            .collect();
        Ok(result)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await?;
        self.record_modified(to);
        self.times
            .write()
            .entry(from.clone())
            .or_insert_with(|| self.clock.now());
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await?;
        self.remove(from);
        self.record_modified(to);
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await?;
        self.record_modified(to);
        self.times
            .write()
            .entry(from.clone())
            .or_insert_with(|| self.clock.now());
        Ok(())
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await?;
        self.remove(from);
        self.record_modified(to);
        Ok(())
    }
}
