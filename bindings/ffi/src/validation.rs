use slatedb::WriteBatch;

use crate::config::FfiWriteOperation;
use crate::error::FfiSlatedbError;

pub(crate) fn build_write_batch(
    operations: Vec<FfiWriteOperation>,
) -> Result<WriteBatch, FfiSlatedbError> {
    let mut batch = WriteBatch::new();

    for operation in operations {
        match operation {
            FfiWriteOperation::Put {
                key,
                value_bytes,
                options,
            } => {
                validate_key_value(&key, &value_bytes)?;
                let options = options.into_core();
                batch.put_with_options(key, value_bytes, &options);
            }
            FfiWriteOperation::Merge {
                key,
                operand,
                options,
            } => {
                validate_key_value(&key, &operand)?;
                let options = options.into_core();
                batch.merge_with_options(key, operand, &options);
            }
            FfiWriteOperation::Delete { key } => {
                validate_key(&key)?;
                batch.delete(key);
            }
        }
    }

    Ok(batch)
}

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

pub(crate) fn write_batch_closed() -> FfiSlatedbError {
    FfiSlatedbError::Invalid {
        message: "write batch is already closed".to_owned(),
    }
}
