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

pub(crate) fn try_usize(value: u64, field: &'static str) -> Result<usize, FfiSlateDbError> {
    usize::try_from(value).map_err(|_| FfiSlateDbError::ValueTooLargeForUsize { field })
}

pub(crate) fn builder_consumed() -> FfiSlateDbError {
    FfiSlateDbError::BuilderConsumed
}

pub(crate) fn transaction_completed() -> FfiSlateDbError {
    FfiSlateDbError::TransactionCompleted
}

pub(crate) fn write_batch_consumed() -> FfiSlateDbError {
    FfiSlateDbError::WriteBatchConsumed
}
