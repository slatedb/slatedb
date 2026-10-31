use crate::error::SlateDbError;

pub(crate) fn validate_key(key: &[u8]) -> Result<(), SlateDbError> {
    if key.is_empty() {
        return Err(SlateDbError::EmptyKey);
    }
    if u32::try_from(key.len()).is_err() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accepts_key_larger_than_u16_max() {
        let key = vec![b'k'; u16::MAX as usize + 1];
        assert!(validate_key(&key).is_ok());
    }
}
