//! The handle policies use to drive the mirror.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use futures::future::{join_all, try_join_all};
use log::warn;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::inner::Inner;
use crate::ordering::{OpKind, Ticket};

/// Lets a [`MirrorPolicy`](crate::MirrorPolicy) download, evict, and inspect
/// local copies.
///
/// Holding a handle keeps the mirror's local state (and its `LOCK`) alive,
/// even after the `ObjectStoreMirror` is dropped.
#[derive(Clone)]
pub struct MirrorHandle {
    pub(crate) inner: Arc<Inner>,
}

impl fmt::Debug for MirrorHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MirrorHandle")
            .field("root", &self.inner.root)
            .finish_non_exhaustive()
    }
}

impl MirrorHandle {
    /// Downloads any paths that aren't local. The request is ordered when this
    /// is called, not when the returned future is first polled: an eviction
    /// of one of `paths` issued before this call either is cancelled or
    /// finishes before the download starts.
    ///
    /// The future resolves when every path is local, or one download fails.
    /// It holds its paths' place in the ordering until it resolves or is
    /// dropped.
    pub fn fetch(
        &self,
        paths: impl IntoIterator<Item = Path>,
    ) -> impl Future<Output = object_store::Result<()>> + Send + 'static {
        let requests: Vec<(Path, Ticket)> = paths
            .into_iter()
            .map(|path| {
                let ticket = self
                    .inner
                    .ordering
                    .register(&path, OpKind::Download)
                    .expect("downloads are always queued");
                (path, ticket)
            })
            .collect();
        let inner = Arc::clone(&self.inner);
        async move {
            try_join_all(
                requests
                    .into_iter()
                    .map(|(path, ticket)| fetch_one(&inner, path, ticket)),
            )
            .await?;
            Ok(())
        }
    }

    /// Downloads in the background, best effort. Failures are logged.
    pub fn prefetch(&self, paths: impl IntoIterator<Item = Path>) {
        let fetch = self.fetch(paths);
        self.inner.spawn(async move {
            if let Err(err) = fetch.await {
                warn!("mirror prefetch failed [error={}]", err);
            }
        });
    }

    /// Queues removal of local copies. Never touches the remote store.
    pub fn evict(&self, paths: impl IntoIterator<Item = Path>) {
        let evictions: Vec<(Path, Ticket)> = paths
            .into_iter()
            .filter_map(|path| {
                let ticket = self.inner.ordering.register(&path, OpKind::Evict)?;
                Some((path, ticket))
            })
            .collect();
        if evictions.is_empty() {
            return;
        }
        let inner = Arc::clone(&self.inner);
        self.inner.spawn(async move {
            join_all(evictions.into_iter().map(|(path, ticket)| {
                let inner = &inner;
                async move {
                    if !ticket.ready().await {
                        return;
                    }
                    if let Err(err) = inner.remove_local(&path).await {
                        warn!("mirror eviction failed [path={}, error={}]", path, err);
                    }
                }
            }))
            .await;
        });
    }

    /// Returns true if `path` has a complete local copy.
    pub fn contains(&self, path: &Path) -> bool {
        self.inner.entries.lock().contains_key(path)
    }

    /// The wrapped store. Calls made here skip the policy.
    pub fn remote(&self) -> &Arc<dyn ObjectStore> {
        &self.inner.remote
    }
}

async fn fetch_one(inner: &Inner, path: Path, ticket: Ticket) -> object_store::Result<()> {
    // Downloads are never cancelled.
    ticket.ready().await;
    if inner.entries.lock().contains_key(&path) {
        return Ok(());
    }
    inner.download(&path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;
    use crate::ObjectStoreMirror;

    const SST: &str = "db/compacted/01A.sst";

    /// Polls `condition` until it holds, yielding to background tasks.
    async fn eventually(mut condition: impl FnMut() -> bool) {
        for _ in 0..1000 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition never held");
    }

    #[tokio::test]
    async fn should_order_fetch_after_earlier_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mirror = ObjectStoreMirror::for_test(dir.path(), Arc::new(InMemory::new())).await;
        let path = Path::from(SST);
        mirror
            .put(&path, PutPayload::from_static(b"x"))
            .await
            .unwrap();

        mirror.handle().evict([path.clone()]);
        mirror.handle().fetch([path.clone()]).await.unwrap();
        assert!(mirror.handle().contains(&path));
        eventually(|| mirror.inner().ordering.is_empty()).await;
        assert!(mirror.handle().contains(&path));
    }

    #[tokio::test]
    async fn should_order_fetch_when_called_not_when_polled() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let mirror = ObjectStoreMirror::for_test(dir.path(), Arc::clone(&store) as _).await;
        let path = Path::from(SST);
        store
            .put(&path, PutPayload::from_static(b"x"))
            .await
            .unwrap();

        // The fetch is issued before the eviction, so the eviction runs after it
        // even though the fetch isn't polled until later.
        let fetch = mirror.handle().fetch([path.clone()]);
        mirror.handle().evict([path.clone()]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        fetch.await.unwrap();
        eventually(|| mirror.inner().ordering.is_empty()).await;
        assert!(!mirror.handle().contains(&path));
    }
}
