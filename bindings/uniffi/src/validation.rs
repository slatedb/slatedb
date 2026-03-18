use crate::error::SlateDbError;

pub(crate) fn validate_key(key: &[u8]) -> Result<(), SlateDbError> {
    if key.is_empty() {
        return Err(SlateDbError::EmptyKey);
    }
    if u16::try_from(key.len()).is_err() {
        return Err(SlateDbError::KeyTooLarge);
    }
    Ok(())
}

pub(crate) fn validate_key_value(key: &[u8], value: &[u8]) -> Result<(), SlateDbError> {
    validate_key(key)?;
    if u32::try_from(value.len()).is_err() {
        return Err(SlateDbError::ValueTooLarge);
    }
    Ok(())
}
