//! Helpers for working with serialized SlateDB settings.

use crate::error::SlatedbError;

/// Return the default [`slatedb::Settings`] value as JSON.
///
/// This is useful for FFI callers that want to start from the Rust default
/// configuration, modify selected fields, and pass the full JSON document back
/// to [`crate::DbBuilder::with_settings_json`].
///
/// ## Returns
/// - `Result<String, SlatedbError>`: the default settings encoded as JSON.
#[uniffi::export]
pub fn default_settings_json() -> Result<String, SlatedbError> {
    slatedb::Settings::default()
        .to_json_string()
        .map_err(|error| SlatedbError::Internal {
            message: error.to_string(),
        })
}
