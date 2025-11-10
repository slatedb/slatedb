use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    path, Error, GetResult, ObjectMeta, ObjectStore, PutOptions, PutPayload, PutResult,
};

/// A simple wrapper around ObjectStore that exposes the same api but access objects within
/// the scope of a specific path prefix
pub(crate) struct PathScopedObjectStoreAccessor {
    root_path: Path,
    object_store: Arc<dyn ObjectStore>,
}

impl PathScopedObjectStoreAccessor {
    pub(crate) fn new(root_path: Path, object_store: Arc<dyn ObjectStore>) -> Self {
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

    pub(crate) async fn put_opts(
        &self,
        path: &Path,
        data: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, Error> {
        let path = self.path(path);
        self.object_store.put_opts(&path, data, opts).await
    }

    pub(crate) async fn get(&self, path: &Path) -> Result<GetResult, Error> {
        let path = self.path(path);
        self.object_store.get(&path).await
    }

    pub(crate) async fn delete(&self, path: &Path) -> Result<(), Error> {
        let path = self.path(path);
        self.object_store.delete(&path).await
    }

    pub(crate) fn list(&self, path: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta, Error>> {
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
    use object_store::{ObjectStore, PutOptions, PutPayload};

    use crate::record::path_scoped_object_store::PathScopedObjectStoreAccessor;

    const ROOT_PATH: &str = "/root/path";

    #[tokio::test]
    async fn test_path_scoped_should_get_put() {
        // given:
        let os = Arc::new(InMemory::new());
        let psos = PathScopedObjectStoreAccessor::new(Path::from(ROOT_PATH), os.clone());
        psos.put_opts(
            &Path::from("obj"),
            PutPayload::from_bytes(Bytes::copy_from_slice("data1".as_bytes())),
            PutOptions::default(),
        )
        .await
        .unwrap();

        // when:
        let result = psos.get(&Path::from("obj")).await.unwrap();

        // then:
        assert_eq!(
            result.bytes().await.unwrap(),
            Bytes::copy_from_slice("data1".as_bytes())
        );
    }

    #[tokio::test]
    async fn test_path_scoped_should_list() {
        // given:
        let os = Arc::new(InMemory::new());
        let psos = PathScopedObjectStoreAccessor::new(Path::from(ROOT_PATH), os.clone());
        psos.put_opts(
            &Path::from("obj"),
            PutPayload::from_bytes(Bytes::copy_from_slice("data1".as_bytes())),
            PutOptions::default(),
        )
        .await
        .unwrap();
        psos.put_opts(
            &Path::from("foo/bar"),
            PutPayload::from_bytes(Bytes::copy_from_slice("data1".as_bytes())),
            PutOptions::default(),
        )
        .await
        .unwrap();
        os.put(&Path::from("biz/baz"), PutPayload::from("data1".as_bytes()))
            .await
            .unwrap();

        // when:
        let mut listing = psos.list(None);

        let item = listing.next().await.unwrap().unwrap();
        assert_eq!(item.location, Path::from("foo/bar"));
        let item = listing.next().await.unwrap().unwrap();
        assert_eq!(item.location, Path::from("obj"));
        assert!(listing.next().await.is_none());
    }

    #[tokio::test]
    async fn test_path_scoped_should_put_with_prefix() {
        // given:
        let os = Arc::new(InMemory::new());
        let psos = PathScopedObjectStoreAccessor::new(Path::from(ROOT_PATH), os.clone());
        psos.put_opts(
            &Path::from("obj"),
            PutPayload::from_bytes(Bytes::copy_from_slice("data1".as_bytes())),
            PutOptions::default(),
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

    #[tokio::test]
    async fn test_path_scoped_object_store_delete() {
        // given:
        let os = Arc::new(InMemory::new());
        let psos = PathScopedObjectStoreAccessor::new(Path::from(ROOT_PATH), os.clone());
        psos.put_opts(
            &Path::from("obj"),
            PutPayload::from_bytes(Bytes::copy_from_slice("data1".as_bytes())),
            PutOptions::default(),
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

        // when:
        psos.delete(&Path::from("obj")).await.unwrap();

        // then:
        let result = os.get(&Path::from("root/path/obj")).await;
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            object_store::Error::NotFound { path: _, source: _ }
        ));
    }
}
