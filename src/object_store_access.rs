use std::sync::Arc;

use futures::stream::BoxStream;
use object_store::{
    buffered::BufWriter, path::Path, GetRange, GetResult, ObjectMeta, ObjectStore, PutOptions,
    PutPayload, PutResult,
};

pub(crate) struct GetOptions {
    /// the range of the object to get, if None, the entire object will be fetched.
    pub range: Option<GetRange>,
    /// the cache strategy to use for this request, you can choose to
    /// disable caching.
    pub cache_enabled: bool,
    /// the lower the rank, the less likely the object will be evicted.
    /// if set to Some(0), the object will never be evicted.
    pub cache_rank: Option<u8>,
}

impl Default for GetOptions {
    fn default() -> Self {
        Self {
            range: None,
            cache_enabled: true,
            cache_rank: None,
        }
    }
}

/// ObjectStoreAccess is to strip down the object store trait to only the
/// methods that are needed by SlateDB. and also to allow us to slightly
/// extend the object store trait to capabilities like cache control.
#[async_trait::async_trait]
pub trait ObjectStoreAccess: Send + Sync + 'static + std::fmt::Debug {
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta>;

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult>;

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult>;

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>>;

    fn buf_writer(&self, path: &Path) -> BufWriter;
}

#[async_trait::async_trait]
impl ObjectStoreAccess for Arc<dyn ObjectStore> {
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.as_ref().head(location).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let obj_store = self as &dyn object_store::ObjectStore;
        let opts = object_store::GetOptions {
            range: opts.range,
            ..Default::default()
        };
        self.as_ref().get_opts(location, opts).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.as_ref().list(prefix)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.as_ref().put_opts(location, payload, opts).await
    }

    fn buf_writer(&self, path: &Path) -> BufWriter {
        BufWriter::new(self.clone(), path.to_owned())
    }
}
