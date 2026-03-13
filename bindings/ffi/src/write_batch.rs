use std::sync::Arc;

use parking_lot::Mutex;

use crate::config::{FfiMergeOptions, FfiPutOptions};
use crate::error::FfiSlatedbError;
use crate::validation::{
    validate_key, validate_key_value, write_batch_closed, write_batch_consumed,
};

enum WriteBatchState {
    Open(slatedb::WriteBatch),
    Consumed,
    Closed,
}

#[derive(uniffi::Object)]
pub struct FfiWriteBatch {
    state: Mutex<WriteBatchState>,
}

impl FfiWriteBatch {
    fn with_open<T>(
        &self,
        update: impl FnOnce(&mut slatedb::WriteBatch) -> T,
    ) -> Result<T, FfiSlatedbError> {
        let mut guard = self.state.lock();
        match &mut *guard {
            WriteBatchState::Open(batch) => Ok(update(batch)),
            WriteBatchState::Consumed => Err(write_batch_consumed()),
            WriteBatchState::Closed => Err(write_batch_closed()),
        }
    }

    pub(crate) fn take_for_write(&self) -> Result<slatedb::WriteBatch, FfiSlatedbError> {
        let mut guard = self.state.lock();

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
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(WriteBatchState::Open(slatedb::WriteBatch::new())),
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), FfiSlatedbError> {
        validate_key_value(&key, &value)?;
        self.with_open(|batch| batch.put(key, value))
    }

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

    pub fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<(), FfiSlatedbError> {
        validate_key_value(&key, &operand)?;
        self.with_open(|batch| batch.merge(key, operand))
    }

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

    pub fn delete(&self, key: Vec<u8>) -> Result<(), FfiSlatedbError> {
        validate_key(&key)?;
        self.with_open(|batch| batch.delete(key))
    }

    pub fn close(&self) -> Result<(), FfiSlatedbError> {
        let mut guard = self.state.lock();
        if matches!(&*guard, WriteBatchState::Closed) {
            Err(write_batch_closed())
        } else {
            *guard = WriteBatchState::Closed;
            Ok(())
        }
    }
}
