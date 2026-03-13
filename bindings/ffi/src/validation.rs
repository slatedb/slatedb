use slatedb::WriteBatch as CoreWriteBatch;

use crate::config::DbWriteOperation;
use crate::error::SlatedbError;

pub(crate) fn build_write_batch(
    operations: Vec<DbWriteOperation>,
) -> Result<CoreWriteBatch, SlatedbError> {
    let mut batch = CoreWriteBatch::new();

    for operation in operations {
        match operation {
            DbWriteOperation::Put {
                key,
                value,
                options,
            } => {
                validate_key_value(&key, &value)?;
                let options = options.into_core();
                batch.put_with_options(key, value, &options);
            }
            DbWriteOperation::Merge {
                key,
                value,
                options,
            } => {
                validate_key_value(&key, &value)?;
                let options = options.into_core();
                batch.merge_with_options(key, value, &options);
            }
            DbWriteOperation::Delete { key } => {
                validate_key(&key)?;
                batch.delete(key);
            }
        }
    }

    Ok(batch)
}

pub(crate) fn validate_key(key: &[u8]) -> Result<(), SlatedbError> {
    if key.is_empty() {
        return Err(SlatedbError::Invalid {
            message: "key cannot be empty".to_owned(),
        });
    }
    if key.len() > u16::MAX as usize {
        return Err(SlatedbError::Invalid {
            message: "key size must be <= u16::MAX".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn validate_key_value(key: &[u8], value: &[u8]) -> Result<(), SlatedbError> {
    validate_key(key)?;
    if value.len() > u32::MAX as usize {
        return Err(SlatedbError::Invalid {
            message: "value size must be <= u32::MAX".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn try_usize(value: u64, field: &str) -> Result<usize, SlatedbError> {
    usize::try_from(value).map_err(|_| SlatedbError::Invalid {
        message: format!("{field} is too large"),
    })
}

pub(crate) fn builder_consumed() -> SlatedbError {
    SlatedbError::Invalid {
        message: "builder has already been consumed".to_owned(),
    }
}

pub(crate) fn transaction_completed() -> SlatedbError {
    SlatedbError::Invalid {
        message: "transaction has already been completed".to_owned(),
    }
}
