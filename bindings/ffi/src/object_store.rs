use std::sync::Arc;

use slatedb::{admin, Db};

use crate::error::{FfiError, FfiSlateDbError};

#[derive(uniffi::Object)]
pub struct FfiObjectStore {
    pub(crate) inner: Arc<dyn slatedb::object_store::ObjectStore>,
}

#[uniffi::export]
impl FfiObjectStore {
    #[uniffi::constructor]
    pub fn resolve(url: String) -> Result<Arc<Self>, FfiError> {
        let inner = Db::resolve_object_store(&url)?;
        Ok(Arc::new(Self { inner }))
    }

    #[uniffi::constructor]
    pub fn from_env(env_file: Option<String>) -> Result<Arc<Self>, FfiError> {
        let inner = admin::load_object_store_from_env(env_file).map_err(FfiSlateDbError::from)?;
        Ok(Arc::new(Self { inner }))
    }
}
