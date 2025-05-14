use crate::db_state::SsTableId;
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
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

    /// The transactional store of the main object store used for everything
    /// that doesn't have a more specific object store configured.
    main_transactional_store: Arc<dyn TransactionalObjectStore>,
    /// Optional transactional store of the WAL object store dedicated
    /// specifically for WAL.
    wal_transactional_store: Option<Arc<dyn TransactionalObjectStore>>,
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
            main_object_store: main_object_store.clone(),
            wal_object_store: wal_object_store.clone(),
            main_transactional_store: Arc::new(DelegatingTransactionalObjectStore::new(
                Path::from("/"),
                main_object_store,
            )),
            wal_transactional_store: wal_object_store.map(|wal_object_store| {
                Arc::new(DelegatingTransactionalObjectStore::new(
                    Path::from("/"),
                    wal_object_store,
                )) as Arc<dyn TransactionalObjectStore>
            }),
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

    pub(crate) fn transactional_store_of(
        &self,
        store_type: ObjectStoreType,
    ) -> &Arc<dyn TransactionalObjectStore> {
        match store_type {
            ObjectStoreType::Main => &self.main_transactional_store,
            ObjectStoreType::Wal => {
                if let Some(wal_transactional_store) = &self.wal_transactional_store {
                    wal_transactional_store
                } else {
                    &self.main_transactional_store
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

    pub(crate) fn transactional_store_for(
        &self,
        id: &SsTableId,
    ) -> Arc<dyn TransactionalObjectStore> {
        match id {
            SsTableId::Wal(..) => self.transactional_store_of(ObjectStoreType::Wal).clone(),
            SsTableId::Compacted(..) => self.transactional_store_of(ObjectStoreType::Main).clone(),
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

        let transactional_store = stores.transactional_store_of(ObjectStoreType::Main);
        assert!(Arc::ptr_eq(
            stores.transactional_store_of(ObjectStoreType::Wal),
            transactional_store
        ));

        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Wal(0)),
            &main_store
        ));
        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Compacted(Ulid::new())),
            &main_store
        ));

        let transactional_store = stores.transactional_store_for(&SsTableId::Wal(0));
        assert!(Arc::ptr_eq(
            &stores.transactional_store_for(&SsTableId::Compacted(Ulid::new())),
            &transactional_store
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

        let transactional_store = stores.transactional_store_of(ObjectStoreType::Main);
        assert!(!Arc::ptr_eq(
            stores.transactional_store_of(ObjectStoreType::Wal),
            transactional_store
        ));

        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Wal(0)),
            &wal_store
        ));
        assert!(Arc::ptr_eq(
            &stores.store_for(&SsTableId::Compacted(Ulid::new())),
            &main_store
        ));

        let transactional_store = stores.transactional_store_for(&SsTableId::Wal(0));
        assert!(!Arc::ptr_eq(
            &stores.transactional_store_for(&SsTableId::Compacted(Ulid::new())),
            &transactional_store
        ));
    }
}
