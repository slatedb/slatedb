//! Helpers for working with serialized SlateDB settings.

use crate::error::FfiSlatedbError;

#[uniffi::export]
pub fn ffi_default_settings_json() -> Result<String, FfiSlatedbError> {
    slatedb::Settings::default()
        .to_json_string()
        .map_err(|error| FfiSlatedbError::Internal {
            message: error.to_string(),
        })
}
