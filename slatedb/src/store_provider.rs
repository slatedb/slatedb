use crate::db_cache::DbCache;
use crate::manifest::store::ManifestStore;
use crate::sst::SsTableFormat;
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
    pub(crate) block_cache: Option<Arc<dyn DbCache>>,
}

impl StoreProvider for DefaultStoreProvider {
    fn table_store(&self) -> Arc<TableStore> {
        Arc::new(TableStore::new(
            Arc::clone(&self.object_store),
            None,
            SsTableFormat::default(),
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
