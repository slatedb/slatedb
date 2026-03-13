use std::sync::Arc;

use slatedb::Db;

use crate::error::FfiSlatedbError;

#[derive(uniffi::Object)]
pub struct FfiObjectStore {
    pub(crate) inner: Arc<dyn slatedb::object_store::ObjectStore>,
}

#[uniffi::export]
impl FfiObjectStore {
    #[uniffi::constructor]
    pub fn resolve(url: String) -> Result<Arc<Self>, FfiSlatedbError> {
        let inner = Db::resolve_object_store(&url)?;
        Ok(Arc::new(Self { inner }))
    }
}
