use crate::error::FfiSlateDbError;

pub(crate) fn validate_key(key: &[u8]) -> Result<(), FfiSlateDbError> {
    if key.is_empty() {
        return Err(FfiSlateDbError::EmptyKey);
    }
    if u16::try_from(key.len()).is_err() {
        return Err(FfiSlateDbError::KeyTooLarge);
    }
    Ok(())
}

pub(crate) fn validate_key_value(key: &[u8], value: &[u8]) -> Result<(), FfiSlateDbError> {
    validate_key(key)?;
    if u32::try_from(value.len()).is_err() {
        return Err(FfiSlateDbError::ValueTooLarge);
    }
    Ok(())
}
