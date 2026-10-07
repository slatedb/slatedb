//! Downloads whole objects into the mirror.
//!
//! Concurrent downloads of the same path share one remote GET. Downloads of
//! different paths are limited by a semaphore. Transient remote errors are
//! retried, since `RetryingObjectStore` sits above the mirror and never sees
//! these calls.

use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::GetOptions;
use slatedb_common::single_flight::SingleFlight;
use tokio::sync::Semaphore;

use crate::error::MIRROR_STORE_NAME;
use crate::inner::{local_error, Inner, TempFile};
use crate::layout::{LocalName, LocalObject};

pub(crate) struct Downloads {
    flights: SingleFlight<Path, Arc<LocalObject>>,
    permits: Semaphore,
}

impl Downloads {
    pub(crate) fn new(concurrency: usize) -> Self {
        Self {
            flights: SingleFlight::new(),
            permits: Semaphore::new(concurrency),
        }
    }
}

impl Inner {
    /// Downloads `path` and installs it, replacing any local copy. The caller
    /// must hold a download ticket for `path`.
    pub(crate) async fn download(&self, path: &Path) -> object_store::Result<Arc<LocalObject>> {
        let name = LocalName::new(path)?;
        self.downloads
            .flights
            .call(path.clone(), || async {
                let _permit = self
                    .downloads
                    .permits
                    .acquire()
                    .await
                    .expect("download semaphore is never closed");
                self.retry.run(|| self.download_once(path, &name)).await
            })
            .await
    }

    async fn download_once(
        &self,
        path: &Path,
        name: &LocalName,
    ) -> object_store::Result<Arc<LocalObject>> {
        let result = self.remote.get_opts(path, GetOptions::default()).await?;
        let mut meta = result.meta.clone();
        meta.location = path.clone();
        let attributes = result.attributes.clone();

        let temp = self.temp_file(name);
        let size = match self.write_stream(&temp, result.into_stream()).await {
            Ok(size) => size,
            Err(err) => {
                temp.remove().await;
                return Err(err);
            }
        };
        if size != meta.size {
            temp.remove().await;
            return Err(object_store::Error::Generic {
                store: MIRROR_STORE_NAME,
                source: format!(
                    "downloaded {size} bytes of `{path}`, but its metadata says {}",
                    meta.size
                )
                .into(),
            });
        }

        Ok(self
            .install(path, name, temp, LocalObject { meta, attributes })
            .await?)
    }

    /// Streams `chunks` into `temp` and syncs it. Returns the byte count.
    async fn write_stream(
        &self,
        temp: &TempFile,
        mut chunks: futures::stream::BoxStream<'static, object_store::Result<bytes::Bytes>>,
    ) -> object_store::Result<u64> {
        let mut writer = self.vfs.create(&temp.path).await.map_err(local_error)?;
        let mut size = 0;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            size += chunk.len() as u64;
            writer.write(chunk).await.map_err(local_error)?;
        }
        writer.finish().await.map_err(local_error)?;
        Ok(size)
    }
}
