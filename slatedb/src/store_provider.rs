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
}

impl StoreProvider for DefaultStoreProvider {
    fn table_store(&self) -> Arc<TableStore> {
        let sst_format = SsTableFormat {
            block_transformer: self.block_transformer.clone(),
            ..SsTableFormat::default()
        };
        Arc::new(TableStore::new(
            ObjectStores::new(
                Arc::clone(&self.object_store),
                self.wal_object_store.clone(),
            ),
            sst_format,
            self.path.clone(),
            self.block_cache.clone(),
        ))
    }

    fn manifest_store(&self) -> Arc<ManifestStore> {
        Arc::new(ManifestStore::new(
            &self.path,
            Arc::clone(&self.object_store),
        ))
    }
}
