use object_store::{
    path::Path, GetRange, GetResult, ObjectMeta, PutOptions, PutPayload, PutResult,
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
pub trait ObjectStoreAccess: Send + Sync + 'static {
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta>;

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult>;

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult>;
}

#[async_trait::async_trait]
impl<T: object_store::ObjectStore> ObjectStoreAccess for T {
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let obj_store = self as &dyn object_store::ObjectStore;
        obj_store.head(location).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let obj_store = self as &dyn object_store::ObjectStore;
        let opts = object_store::GetOptions {
            range: opts.range,
            ..Default::default()
        };
        obj_store.get_opts(location, opts).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let obj_store = self as &dyn object_store::ObjectStore;
        obj_store.put_opts(location, payload, opts).await
    }
}
