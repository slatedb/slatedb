use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    path, Error, GetResult, ObjectMeta, PutMode, PutOptions, PutPayload, PutResult,
};

use crate::disk_cache::{CacheableGetOptions, CacheableObjectStoreRef};

// Implements transactional object inserts using some safe protocol
#[async_trait]
pub(crate) trait TransactionalObjectStore: Send + Sync {
    async fn put_if_not_exists(&self, path: &Path, data: Bytes) -> Result<PutResult, Error>;

    async fn get(&self, path: &Path) -> Result<GetResult, Error>;

    fn list(&self, path: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta, Error>>;
}

// An implementation of TransactionalObjectStore that delegates the transactional
// protocol to the ObjectStore implementation. This implementation is sufficient for
// object stores that support CAS like Azure and GCP. The object_store S3 impl supports CAS
// but uses an algorithm that is based on time-expiring locks that are susceptible to either
// lockout or races, if the lock timeout is set too high or low, respectively. So this type
// is generally not appropriate for S3.
pub(crate) struct DelegatingTransactionalObjectStore {
    root_path: Path,
    object_store: CacheableObjectStoreRef,
}

impl DelegatingTransactionalObjectStore {
    pub(crate) fn new(root_path: Path, object_store: CacheableObjectStoreRef) -> Self {
        Self {
            root_path,
            object_store,
        }
    }

    fn path(&self, path: &Path) -> Path {
        Path::from(format!("{}/{}", self.root_path, path))
    }

    fn strip_root(&self, path: &Path) -> Result<Path, Error> {
        let root_raw = self.root_path.to_string();
        let path_raw = path.to_string();
        // Path ensures there are no empty delimiters, so it should be safe to just do
        // a raw prefix strip
        if let Some(stripped) = path_raw.strip_prefix(root_raw.as_str()) {
            return Ok(Path::from(stripped));
        }
        Err(Error::InvalidPath {
            source: path::Error::PrefixMismatch {
                path: path.to_string(),
                prefix: self.root_path.to_string(),
            },
        })
    }
}

#[async_trait]
impl TransactionalObjectStore for DelegatingTransactionalObjectStore {
    async fn put_if_not_exists(&self, path: &Path, data: Bytes) -> Result<PutResult, Error> {
        let path = self.path(path);
        self.object_store
            .put_opts(
                &path,
                PutPayload::from_bytes(data),
                PutOptions::from(PutMode::Create),
            )
            .await
    }

    async fn get(&self, path: &Path) -> Result<GetResult, Error> {
        let path = self.path(path);
        self.object_store
            .get_opts(&path, CacheableGetOptions::default())
            .await
    }

    fn list(&self, path: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta, Error>> {
        let path = path.map_or(self.root_path.clone(), |p| self.path(p));
        self.object_store
            .list(Some(&path))
            .map(|r| match r {
                Ok(om) => Ok(ObjectMeta {
                    location: self.strip_root(&om.location)?,
                    last_modified: om.last_modified,
                    size: om.size,
                    e_tag: om.e_tag,
                    version: om.version,
                }),
                Err(err) => Err(err),
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, PutPayload};

    use crate::transactional_object_store::{
        DelegatingTransactionalObjectStore, TransactionalObjectStore,
    };

    const ROOT_PATH: &str = "/root/path";

    #[tokio::test]
    async fn test_delegating_should_fail_put_if_exists() {
        // given:
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let txnl_os = DelegatingTransactionalObjectStore::new(Path::from(ROOT_PATH), os.into());
        txnl_os
            .put_if_not_exists(
                &Path::from("obj"),
                Bytes::copy_from_slice("data1".as_bytes()),
            )
            .await
            .unwrap();

        // when:
        let result = txnl_os
            .put_if_not_exists(
                &Path::from("obj"),
                Bytes::copy_from_slice("data2".as_bytes()),
            )
            .await;

        // then:
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            object_store::Error::AlreadyExists { path: _, source: _ }
        ));
        let result = txnl_os.get(&Path::from("obj")).await.unwrap();
        assert_eq!(
            result.bytes().await.unwrap(),
            Bytes::copy_from_slice("data1".as_bytes())
        );
    }

    #[tokio::test]
    async fn test_delegating_should_get_put() {
        // given:
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let txnl_os = DelegatingTransactionalObjectStore::new(Path::from(ROOT_PATH), os.into());
        txnl_os
            .put_if_not_exists(
                &Path::from("obj"),
                Bytes::copy_from_slice("data1".as_bytes()),
            )
            .await
            .unwrap();

        // when:
        let result = txnl_os.get(&Path::from("obj")).await.unwrap();

        // then:
        assert_eq!(
            result.bytes().await.unwrap(),
            Bytes::copy_from_slice("data1".as_bytes())
        );
    }

    #[tokio::test]
    async fn test_delegating_should_list() {
        // given:
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let txnl_os =
            DelegatingTransactionalObjectStore::new(Path::from(ROOT_PATH), os.clone().into());
        txnl_os
            .put_if_not_exists(
                &Path::from("obj"),
                Bytes::copy_from_slice("data1".as_bytes()),
            )
            .await
            .unwrap();
        txnl_os
            .put_if_not_exists(
                &Path::from("foo/bar"),
                Bytes::copy_from_slice("data1".as_bytes()),
            )
            .await
            .unwrap();
        os.put(&Path::from("biz/baz"), PutPayload::from("data1".as_bytes()))
            .await
            .unwrap();

        // when:
        let mut listing = txnl_os.list(None);

        let item = listing.next().await.unwrap().unwrap();
        assert_eq!(item.location, Path::from("foo/bar"));
        let item = listing.next().await.unwrap().unwrap();
        assert_eq!(item.location, Path::from("obj"));
        assert!(listing.next().await.is_none());
    }

    #[tokio::test]
    async fn test_delegating_should_put_with_prefix() {
        // given:
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let txnl_os =
            DelegatingTransactionalObjectStore::new(Path::from(ROOT_PATH), os.clone().into());
        txnl_os
            .put_if_not_exists(
                &Path::from("obj"),
                Bytes::copy_from_slice("data1".as_bytes()),
            )
            .await
            .unwrap();

        // when:
        let result = os.get(&Path::from("root/path/obj")).await.unwrap();

        // then:
        assert_eq!(
            result.bytes().await.unwrap(),
            Bytes::copy_from_slice("data1".as_bytes())
        );
    }
}
