use std::sync::Arc;

use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStoreExt};
use slatedb::{admin, Db};
use tokio::sync::Mutex;

use crate::error::{Error, SlateDbError};
use crate::types::ObjectMetadata;

/// Object store handle used when opening databases, readers, and WAL readers.
#[derive(uniffi::Object)]
pub struct ObjectStore {
    pub(crate) inner: Arc<dyn object_store::ObjectStore>,
}

fn path(location: String) -> Result<Path, Error> {
    Path::parse(location).map_err(|source| object_store::Error::InvalidPath { source }.into())
}

fn metadata(meta: object_store::ObjectMeta) -> ObjectMetadata {
    slatedb::ObjectMetadata::new(meta).into()
}

#[uniffi::export]
impl ObjectStore {
    /// Resolves an object store from a URL understood by SlateDB.
    #[uniffi::constructor]
    pub fn resolve(url: String) -> Result<Arc<Self>, Error> {
        let inner = Db::resolve_object_store(&url)?;
        Ok(Arc::new(Self { inner }))
    }

    /// Builds an object store from environment configuration.
    ///
    /// When `env_file` is provided, environment variables are loaded from that
    /// file before constructing the store.
    #[uniffi::constructor]
    pub fn from_env(env_file: Option<String>) -> Result<Arc<Self>, Error> {
        let inner = admin::load_object_store_from_env(env_file)?;
        Ok(Arc::new(Self { inner }))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl ObjectStore {
    /// A successful put replaces the whole object at `location`.
    pub async fn put(&self, location: String, value: Vec<u8>) -> Result<(), Error> {
        self.inner.put(&path(location)?, value.into()).await?;
        Ok(())
    }

    /// Missing objects return `None`. Empty objects return `Some` with no bytes.
    pub async fn get(&self, location: String) -> Result<Option<Vec<u8>>, Error> {
        let result = match self.inner.get(&path(location)?).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(result.bytes().await?.to_vec()))
    }

    /// The byte range starts at `start` and ends before `end`.
    pub async fn get_range(
        &self,
        location: String,
        start: u64,
        end: u64,
    ) -> Result<Option<Vec<u8>>, Error> {
        if start >= end {
            return Err(SlateDbError::EmptyRange.into());
        }
        match self.inner.get_range(&path(location)?, start..end).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Missing objects return `None`.
    pub async fn head(&self, location: String) -> Result<Option<ObjectMetadata>, Error> {
        match self.inner.head(&path(location)?).await {
            Ok(meta) => Ok(Some(metadata(meta))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Deleting a missing object succeeds.
    pub async fn delete(&self, location: String) -> Result<(), Error> {
        match self.inner.delete(&path(location)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// The prefix follows object-store path segments. Results have no fixed order.
    pub fn list(&self, prefix: Option<String>) -> Result<Arc<ObjectStoreIterator>, Error> {
        let prefix = prefix.map(path).transpose()?;
        Ok(Arc::new(ObjectStoreIterator {
            inner: Mutex::new(self.inner.list(prefix.as_ref()).fuse().boxed()),
        }))
    }
}

#[derive(uniffi::Object)]
pub struct ObjectStoreIterator {
    inner: Mutex<BoxStream<'static, object_store::Result<object_store::ObjectMeta>>>,
}

#[uniffi::export(async_runtime = "tokio")]
impl ObjectStoreIterator {
    /// Each call consumes one result. Calls after exhaustion return `None`.
    pub async fn next(&self) -> Result<Option<ObjectMetadata>, Error> {
        Ok(self.inner.lock().await.try_next().await?.map(metadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn object_bytes_and_metadata_share_the_database_store() {
        let store = ObjectStore::resolve("memory:///".into()).unwrap();
        let location = "segments/a.parquet".to_string();
        store.put(location.clone(), vec![0, 255, 1]).await.unwrap();
        let raw = store
            .inner
            .get(&Path::from(location.as_str()))
            .await
            .unwrap();
        assert_eq!(raw.bytes().await.unwrap().as_ref(), &[0, 255, 1]);
        assert_eq!(
            store.get_range(location.clone(), 1, 3).await.unwrap(),
            Some(vec![255, 1])
        );
        let meta = store.head(location.clone()).await.unwrap().unwrap();
        assert_eq!(meta.size, 3);
        assert_eq!(meta.location, location);
        assert_eq!(
            meta,
            metadata(
                store
                    .inner
                    .head(&Path::from(location.as_str()))
                    .await
                    .unwrap()
            )
        );
        store.put(location.clone(), vec![]).await.unwrap();
        assert_eq!(store.get(location.clone()).await.unwrap(), Some(vec![]));
        store.delete(location.clone()).await.unwrap();
        assert_eq!(store.get(location.clone()).await.unwrap(), None);
        assert_eq!(store.get_range(location.clone(), 0, 1).await.unwrap(), None);
        assert_eq!(store.head(location.clone()).await.unwrap(), None);
        store.delete(location).await.unwrap();
    }

    #[tokio::test]
    async fn object_list_keeps_path_prefixes_and_exhaustion() {
        let store = ObjectStore::resolve("memory:///".into()).unwrap();
        for location in ["segments/a", "segments/sub/b", "segments-other/c"] {
            store.put(location.into(), vec![1]).await.unwrap();
        }
        let iter = store.list(Some("segments".into())).unwrap();
        let (a, b) = tokio::join!(iter.next(), iter.next());
        let mut locations = vec![a.unwrap().unwrap().location, b.unwrap().unwrap().location];
        locations.sort();
        assert_eq!(locations, ["segments/a", "segments/sub/b"]);
        assert_eq!(iter.next().await.unwrap(), None);
        assert_eq!(iter.next().await.unwrap(), None);
        let all = store.list(None).unwrap();
        let mut count = 0;
        while all.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn invalid_object_paths_are_refused() {
        let store = ObjectStore::resolve("memory:///".into()).unwrap();
        let location = "segments/../outside".to_string();
        assert!(matches!(
            store.put(location.clone(), vec![]).await,
            Err(Error::Invalid { .. })
        ));
        assert!(matches!(
            store.get(location.clone()).await,
            Err(Error::Invalid { .. })
        ));
        assert!(matches!(
            store.get_range(location.clone(), 0, 1).await,
            Err(Error::Invalid { .. })
        ));
        for (start, end) in [(2, 1), (1, 1)] {
            assert!(matches!(
                store.get_range("segments/a".into(), start, end).await,
                Err(Error::Invalid { .. })
            ));
        }
        assert!(matches!(
            store.head(location.clone()).await,
            Err(Error::Invalid { .. })
        ));
        assert!(matches!(
            store.delete(location.clone()).await,
            Err(Error::Invalid { .. })
        ));
        assert!(matches!(
            store.list(Some(location)),
            Err(Error::Invalid { .. })
        ));
    }

    #[tokio::test]
    async fn object_list_reports_provider_errors() {
        let iter = ObjectStoreIterator {
            inner: Mutex::new(
                futures::stream::iter([Err(object_store::Error::Generic {
                    store: "test",
                    source: std::io::Error::other("offline").into(),
                })])
                .fuse()
                .boxed(),
            ),
        };
        assert!(
            matches!(iter.next().await, Err(Error::Unavailable { message }) if message.contains("offline"))
        );
        assert_eq!(iter.next().await.unwrap(), None);
    }
}
