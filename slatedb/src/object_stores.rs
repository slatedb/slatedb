use crate::db_state::SsTableId;
use object_store::ObjectStore;
use std::sync::Arc;

/// Resolves object stores for different [object store types](ObjectStoreType).
pub(crate) struct ObjectStores {
    /// The main object store used for everything that doesn't have a more
    /// specific object store configured.
    main_object_store: Arc<dyn ObjectStore>,
    /// Optional WAL object store dedicated specifically for WAL.
    wal_object_store: Option<Arc<dyn ObjectStore>>,
}

pub(crate) enum ObjectStoreType {
    Main,
    Wal,
}

impl ObjectStores {
    pub(crate) fn new(
        main_object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Self {
        Self {
            main_object_store,
            wal_object_store,
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
    use ulid::Ulid;

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
            &stores.store_for(&SsTableId::Compacted(Ulid::new())),
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
            &stores.store_for(&SsTableId::Compacted(Ulid::new())),
            &main_store
        ));
    }
}
