use crate::error::{create_error_result, CSdbError};
use slatedb::admin::load_object_store_from_env;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use std::sync::Arc;

// Object store creation helper
pub fn create_object_store(
    url: Option<&str>,
    env_file: Option<String>,
) -> Result<Arc<dyn ObjectStore>, crate::error::CSdbResult> {
    if let Some(url) = url {
        return Db::resolve_object_store(url).map_err(|e| {
            create_error_result(
                CSdbError::InternalError,
                &format!("Failed to resolve object store: {}", e),
            )
        });
    }
    load_object_store_from_env(env_file).map_err(|e| {
        create_error_result(
            CSdbError::InternalError,
            &format!("Failed to load object store from environment: {}", e),
        )
    })
}
