use crate::cached_object_store::CachedObjectStore;
use crate::db_state::SsTableId;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

/// Resolves object stores for different [object store types](ObjectStoreType).
pub(crate) struct ObjectStores {
    /// The main object store used for everything that doesn't have a more
    /// specific object store configured.
    main_object_store: Arc<dyn ObjectStore>,
    /// Optional WAL object store dedicated specifically for WAL.
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    /// Concrete handle to the main-side local disk cache, if one is in use.
    /// Needed so higher layers can drop a corrupt cached entry after a
    /// checksum mismatch and refetch from the remote object store. The WAL
    /// path is never routed through `CachedObjectStore` today, so there is
    /// no corresponding WAL invalidator.
    main_cache: Option<Arc<CachedObjectStore>>,
}

/// Whether the object store holds the main data path or the WAL.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ObjectStoreType {
    /// The primary object store (SSTs, manifests, compacted data).
    Main,
    /// The dedicated WAL object store, when configured separately.
    Wal,
}

impl ObjectStoreType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Wal => "wal",
        }
    }
}

impl ObjectStores {
    pub(crate) fn new(
        main_object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Self {
        Self {
            main_object_store,
            wal_object_store,
            main_cache: None,
        }
    }

    pub(crate) fn with_main_cache(mut self, cache: Arc<CachedObjectStore>) -> Self {
        self.main_cache = Some(cache);
        self
    }

    /// Invalidate the cached copy (if any) of the given path on the store
    /// that serves the supplied SST id. Used by SST readers to drop a
    /// corrupt cache entry after the SST-level checksum check fails.
    pub(crate) async fn invalidate_cache_for(&self, id: &SsTableId, path: &Path) {
        let cache = match id {
            SsTableId::Compacted(..) => self.main_cache.as_ref(),
            // WAL is not served through the local disk cache.
            SsTableId::Wal(..) => None,
        };
        if let Some(cache) = cache {
            if let Err(err) = cache.invalidate(path).await {
                log::warn!(
                    "failed to invalidate cached SST entry [path={}, error={}]",
                    path,
                    err
                );
            }
        }
    }

    pub(crate) fn store_of(&self, store_type: ObjectStoreType) -> &Arc<dyn ObjectStore> {
        match store_type {
            ObjectStoreType::Main => &self.main_object_store,
            ObjectStoreType::Wal => {
                if let Some(wal_object_store) = &self.wal_object_store {
                    wal_object_store
                } else {
                    &self.main_object_store
                }
            }
        }
    }

    pub(crate) fn has_wal_object_store(&self) -> bool {
        self.wal_object_store.is_some()
    }

    pub(crate) fn store_for(&self, id: &SsTableId) -> Arc<dyn ObjectStore> {
        match id {
            SsTableId::Wal(..) => self.store_of(ObjectStoreType::Wal).clone(),
            SsTableId::Compacted(..) => self.store_of(ObjectStoreType::Main).clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn test_main_object_store_only_setup() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stores = ObjectStores::new(main_store.clone(), None);

        assert!(Arc::ptr_eq(
            stores.store_of(ObjectStoreType::Main),
            &main_store
        ));
        assert!(Arc::ptr_eq(
            stores.store_of(ObjectStoreType::Wal),
            &main_store
        ));

        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Wal(0)),
            &main_store
        ));
        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Compacted(ulid::Ulid::new())),
            &main_store
        ));
    }

    #[test]
    fn test_main_and_wal_object_stores_setup() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stores = ObjectStores::new(main_store.clone(), Some(wal_store.clone()));

        assert!(Arc::ptr_eq(
            stores.store_of(ObjectStoreType::Main),
            &main_store
        ));
        assert!(Arc::ptr_eq(
            stores.store_of(ObjectStoreType::Wal),
            &wal_store
        ));

        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Wal(0)),
            &wal_store
        ));
        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Compacted(ulid::Ulid::new())),
            &main_store
        ));
    }
}
