//! First-class write batch wrapper exposed by UniFFI.

use std::sync::{Arc, Mutex};

use slatedb::WriteBatch as CoreWriteBatch;

use crate::config::{FfiMergeOptions, FfiPutOptions};
use crate::error::FfiSlatedbError;
use crate::validation::{
    validate_key, validate_key_value, write_batch_closed, write_batch_consumed,
};

/// A mutable batch of write operations that can be written atomically.
#[derive(uniffi::Object)]
pub struct FfiWriteBatch {
    state: Mutex<WriteBatchState>,
}

enum WriteBatchState {
    Open(CoreWriteBatch),
    Consumed,
    Closed,
}

impl FfiWriteBatch {
    fn with_open<T>(
        &self,
        update: impl FnOnce(&mut CoreWriteBatch) -> T,
    ) -> Result<T, FfiSlatedbError> {
        let mut guard = self.state.lock().map_err(|_| FfiSlatedbError::Internal {
            message: "write batch mutex poisoned".to_owned(),
        })?;
        match &mut *guard {
            WriteBatchState::Open(batch) => Ok(update(batch)),
            WriteBatchState::Consumed => Err(write_batch_consumed()),
            WriteBatchState::Closed => Err(write_batch_closed()),
        }
    }

    pub(crate) fn take_for_write(&self) -> Result<CoreWriteBatch, FfiSlatedbError> {
        let mut guard = self.state.lock().map_err(|_| FfiSlatedbError::Internal {
            message: "write batch mutex poisoned".to_owned(),
        })?;

        match std::mem::replace(&mut *guard, WriteBatchState::Consumed) {
            WriteBatchState::Open(batch) => Ok(batch),
            state @ WriteBatchState::Consumed => {
                *guard = state;
                Err(write_batch_consumed())
            }
            state @ WriteBatchState::Closed => {
                *guard = state;
                Err(write_batch_closed())
            }
        }
    }
}

#[uniffi::export]
impl FfiWriteBatch {
    /// Create a new empty write batch.
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(WriteBatchState::Open(CoreWriteBatch::new())),
        })
    }

    /// Append a put operation using default put options.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), FfiSlatedbError> {
        validate_key_value(&key, &value)?;
        self.with_open(|batch| batch.put(key, value))
    }

    /// Append a put operation using explicit put options.
    pub fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: FfiPutOptions,
    ) -> Result<(), FfiSlatedbError> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        self.with_open(|batch| batch.put_with_options(key, value, &options))
    }

    /// Append a merge operation using default merge options.
    pub fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<(), FfiSlatedbError> {
        validate_key_value(&key, &operand)?;
        self.with_open(|batch| batch.merge(key, operand))
    }

    /// Append a merge operation using explicit merge options.
    pub fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        options: FfiMergeOptions,
    ) -> Result<(), FfiSlatedbError> {
        validate_key_value(&key, &operand)?;
        let options = options.into_core();
        self.with_open(|batch| batch.merge_with_options(key, operand, &options))
    }

    /// Append a delete operation.
    pub fn delete(&self, key: Vec<u8>) -> Result<(), FfiSlatedbError> {
        validate_key(&key)?;
        self.with_open(|batch| batch.delete(key))
    }

    /// Explicitly close the batch handle.
    pub fn close(&self) -> Result<(), FfiSlatedbError> {
        let mut guard = self.state.lock().map_err(|_| FfiSlatedbError::Internal {
            message: "write batch mutex poisoned".to_owned(),
        })?;
        if matches!(&*guard, WriteBatchState::Closed) {
            Err(write_batch_closed())
        } else {
            *guard = WriteBatchState::Closed;
            Ok(())
        }
    }
}
