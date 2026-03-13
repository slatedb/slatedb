//! Object-store handles and helpers for resolving them from URLs.

use std::sync::Arc;

use slatedb::Db;

use crate::error::FfiSlatedbError;

/// A resolved object-store handle.
///
/// Use [`ffi_resolve_object_store`] to create one of these handles and then pass it
/// into [`crate::FfiDbBuilder`] for the main database store or the WAL store.
#[derive(uniffi::Object)]
pub struct FfiObjectStore {
    pub(crate) inner: Arc<dyn slatedb::object_store::ObjectStore>,
}

/// Resolve an object store from a URL.
///
/// ## Arguments
/// - `url`: the object-store URL, for example `memory:///` or `s3://bucket/prefix`.
///
/// ## Returns
/// - `Result<Arc<FfiObjectStore>, FfiSlatedbError>`: the resolved object-store handle.
///
/// ## Errors
/// - `FfiSlatedbError`: if the URL cannot be parsed or the object-store backend is unsupported.
#[uniffi::export]
pub fn ffi_resolve_object_store(url: String) -> Result<Arc<FfiObjectStore>, FfiSlatedbError> {
    let inner = Db::resolve_object_store(&url)?;
    Ok(Arc::new(FfiObjectStore { inner }))
}
