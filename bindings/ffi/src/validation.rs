use crate::error::FfiSlatedbError;

pub(crate) fn validate_key(key: &[u8]) -> Result<(), FfiSlatedbError> {
    if key.is_empty() {
        return Err(FfiSlatedbError::Invalid {
            message: "key cannot be empty".to_owned(),
        });
    }
    if u16::try_from(key.len()).is_err() {
        return Err(FfiSlatedbError::Invalid {
            message: "key size must be <= u16::MAX".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn validate_key_value(key: &[u8], value: &[u8]) -> Result<(), FfiSlatedbError> {
    validate_key(key)?;
    if u32::try_from(value.len()).is_err() {
        return Err(FfiSlatedbError::Invalid {
            message: "value size must be <= u32::MAX".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn try_usize(value: u64, field: &str) -> Result<usize, FfiSlatedbError> {
    usize::try_from(value).map_err(|_| FfiSlatedbError::Invalid {
        message: format!("{field} is too large"),
    })
}

pub(crate) fn builder_consumed() -> FfiSlatedbError {
    FfiSlatedbError::Invalid {
        message: "builder has already been consumed".to_owned(),
    }
}

pub(crate) fn transaction_completed() -> FfiSlatedbError {
    FfiSlatedbError::Invalid {
        message: "transaction has already been completed".to_owned(),
    }
}

pub(crate) fn write_batch_consumed() -> FfiSlatedbError {
    FfiSlatedbError::Invalid {
        message: "write batch has already been consumed".to_owned(),
    }
}
