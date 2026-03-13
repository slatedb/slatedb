use crate::error::SlatedbError;

#[uniffi::export]
pub fn default_settings_json() -> Result<String, SlatedbError> {
    slatedb::Settings::default()
        .to_json_string()
        .map_err(|error| SlatedbError::Internal {
            message: error.to_string(),
        })
}
