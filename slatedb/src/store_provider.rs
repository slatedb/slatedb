use crate::cached_object_store::CachedObjectStore;
use crate::db_cache::DbCache;
use crate::format::sst::{BlockTransformer, SsTableFormat};
use crate::manifest::store::ManifestStore;
use crate::object_stores::ObjectStores;
use crate::tablestore::TableStore;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

pub(crate) trait StoreProvider: Send + Sync {
    fn table_store(&self) -> Arc<TableStore>;
    fn manifest_store(&self) -> Arc<ManifestStore>;
}

pub(crate) struct DefaultStoreProvider {
    pub(crate) path: Path,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) wal_object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) block_cache: Option<Arc<dyn DbCache>>,
    pub(crate) block_transformer: Option<Arc<dyn BlockTransformer>>,
    /// Concrete handle to the local disk cache, when one is configured,
    /// so that the `ManifestStore` and `TableStore` built here can
    /// invalidate cached entries and refetch from the remote object store
    /// on a `ChecksumMismatch`.
    pub(crate) cached_object_store: Option<Arc<CachedObjectStore>>,
}

impl StoreProvider for DefaultStoreProvider {
    fn table_store(&self) -> Arc<TableStore> {
        let sst_format = SsTableFormat {
            block_transformer: self.block_transformer.clone(),
            ..SsTableFormat::default()
        };
        let mut object_stores = ObjectStores::new(
            Arc::clone(&self.object_store),
            self.wal_object_store.clone(),
        );
        if let Some(cache) = &self.cached_object_store {
            object_stores = object_stores.with_main_cache(cache.clone());
        }
        Arc::new(TableStore::new(
            object_stores,
            sst_format,
            self.path.clone(),
            self.block_cache.clone(),
        ))
    }

    fn manifest_store(&self) -> Arc<ManifestStore> {
        let store = ManifestStore::new(&self.path, Arc::clone(&self.object_store));
        match &self.cached_object_store {
            Some(cache) => Arc::new(store.with_cache_invalidator(cache.clone())),
            None => Arc::new(store),
        }
    }
}
