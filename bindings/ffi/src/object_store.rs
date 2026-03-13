use std::sync::Arc;

use slatedb::object_store::ObjectStore as ObjectStoreTrait;
use slatedb::Db as CoreDb;

use crate::error::SlatedbError;

#[derive(uniffi::Object)]
pub struct ObjectStore {
    pub(crate) inner: Arc<dyn ObjectStoreTrait>,
}

#[uniffi::export]
pub fn resolve_object_store(url: String) -> Result<Arc<ObjectStore>, SlatedbError> {
    let inner = CoreDb::resolve_object_store(&url)?;
    Ok(Arc::new(ObjectStore { inner }))
}
