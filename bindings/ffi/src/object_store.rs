//! Object-store handles and helpers for resolving them from URLs.

use std::sync::Arc;

use slatedb::object_store::ObjectStore as ObjectStoreTrait;
use slatedb::Db as CoreDb;

use crate::error::FfiSlatedbError;

/// A resolved object-store handle.
///
/// Use [`resolve_object_store`] to create one of these handles and then pass it
/// into [`crate::DbBuilder`] for the main database store or the WAL store.
#[derive(uniffi::Object)]
pub struct ObjectStore {
    pub(crate) inner: Arc<dyn ObjectStoreTrait>,
}

/// Resolve an object store from a URL.
///
/// ## Arguments
/// - `url`: the object-store URL, for example `memory:///` or `s3://bucket/prefix`.
///
/// ## Returns
/// - `Result<Arc<ObjectStore>, FfiSlatedbError>`: the resolved object-store handle.
///
/// ## Errors
/// - `FfiSlatedbError`: if the URL cannot be parsed or the object-store backend is unsupported.
#[uniffi::export]
pub fn resolve_object_store(url: String) -> Result<Arc<ObjectStore>, FfiSlatedbError> {
    let inner = CoreDb::resolve_object_store(&url)?;
    Ok(Arc::new(ObjectStore { inner }))
}
