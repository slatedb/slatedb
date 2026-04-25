use std::sync::Arc;

use slatedb::{admin, Db};

use crate::error::Error;

/// Object store handle used when opening databases, readers, and WAL readers.
#[derive(uniffi::Object)]
pub struct ObjectStore {
    pub(crate) inner: Arc<dyn slatedb::object_store::ObjectStore>,
}

#[uniffi::export]
impl ObjectStore {
    /// Resolves an object store from a URL understood by SlateDB.
    #[uniffi::constructor]
    pub fn resolve(url: String) -> Result<Arc<Self>, Error> {
        let inner = Db::resolve_object_store(&url)?;
        Ok(Arc::new(Self { inner }))
    }

    /// Builds an object store from environment configuration.
    ///
    /// When `env_file` is provided, environment variables are loaded from that
    /// file before constructing the store.
    #[uniffi::constructor]
    pub fn from_env(env_file: Option<String>) -> Result<Arc<Self>, Error> {
        let inner = admin::load_object_store_from_env(env_file)?;
        Ok(Arc::new(Self { inner }))
    }
}
