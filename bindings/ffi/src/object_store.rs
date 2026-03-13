use std::sync::Arc;

use slatedb::Db;

use crate::error::FfiSlatedbError;

#[derive(uniffi::Object)]
pub struct FfiObjectStore {
    pub(crate) inner: Arc<dyn slatedb::object_store::ObjectStore>,
}

#[uniffi::export]
pub fn ffi_resolve_object_store(url: String) -> Result<Arc<FfiObjectStore>, FfiSlatedbError> {
    let inner = Db::resolve_object_store(&url)?;
    Ok(Arc::new(FfiObjectStore { inner }))
}
