//! Helpers for working with serialized SlateDB settings.

use crate::error::FfiSlatedbError;

/// Return the default [`slatedb::Settings`] value as JSON.
///
/// This is useful for FFI callers that want to start from the Rust default
/// configuration, modify selected fields, and pass the full JSON document back
/// to [`crate::FfiDbBuilder::with_settings_json`].
///
/// ## Returns
/// - `Result<String, FfiSlatedbError>`: the default settings encoded as JSON.
#[uniffi::export]
pub fn ffi_default_settings_json() -> Result<String, FfiSlatedbError> {
    slatedb::Settings::default()
        .to_json_string()
        .map_err(|error| FfiSlatedbError::Internal {
            message: error.to_string(),
        })
}
