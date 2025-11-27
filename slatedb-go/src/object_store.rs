use object_store::memory::InMemory;
use slatedb::admin::load_object_store_from_env;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use std::sync::Arc;

// Object store creation helper
pub fn create_object_store(
    url: Option<&str>,
    env_file: Option<String>,
) -> Result<Arc<dyn ObjectStore>, crate::error::CSdbResult> {
    use crate::error::{create_error_result, CSdbError};
    if let Some(url) = url {
        return Db::resolve_object_store(url).map_err(|e| {
            create_error_result(
                CSdbError::InternalError,
                &format!("Failed to resolve object store: {}", e),
            )
        });
    }
    if let Some(env) = env_file {
        return load_object_store_from_env(Some(env)).map_err(|e| {
            create_error_result(
                CSdbError::InternalError,
                &format!("Failed to load object store from environment: {}", e),
            )
        });
    }
    Ok(Arc::new(InMemory::new()))
}
