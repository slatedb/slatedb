use std::sync::Arc;

use slatedb::{admin, Db};

use crate::error::{Error, SlateDbError};

#[derive(uniffi::Object)]
pub struct ObjectStore {
    pub(crate) inner: Arc<dyn slatedb::object_store::ObjectStore>,
}

#[uniffi::export]
impl ObjectStore {
    #[uniffi::constructor]
    pub fn resolve(url: String) -> Result<Arc<Self>, Error> {
        let inner = Db::resolve_object_store(&url)?;
        Ok(Arc::new(Self { inner }))
    }

    #[uniffi::constructor]
    pub fn from_env(env_file: Option<String>) -> Result<Arc<Self>, Error> {
        let inner = admin::load_object_store_from_env(env_file).map_err(SlateDbError::from)?;
        Ok(Arc::new(Self { inner }))
    }
}
