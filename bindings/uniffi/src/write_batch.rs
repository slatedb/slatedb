use std::sync::Arc;

use parking_lot::Mutex;

use crate::config::{MergeOptions, PutOptions};
use crate::error::{DbError, SlateDbError};
use crate::validation::{validate_key, validate_key_value};

#[derive(uniffi::Object)]
pub struct WriteBatch {
    state: Mutex<Option<slatedb::WriteBatch>>,
}

impl WriteBatch {
    fn with_open<T>(
        &self,
        update: impl FnOnce(&mut slatedb::WriteBatch) -> T,
    ) -> Result<T, SlateDbError> {
        let mut guard = self.state.lock();
        let batch = guard.as_mut().ok_or(SlateDbError::WriteBatchConsumed)?;
        Ok(update(batch))
    }

    pub(crate) fn take_for_write(&self) -> Result<slatedb::WriteBatch, SlateDbError> {
        let mut guard = self.state.lock();
        guard.take().ok_or(SlateDbError::WriteBatchConsumed)
    }
}

#[uniffi::export]
impl WriteBatch {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some(slatedb::WriteBatch::new())),
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DbError> {
        validate_key_value(&key, &value)?;
        self.with_open(|batch| batch.put(key, value))
            .map_err(Into::into)
    }

    pub fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<(), DbError> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        self.with_open(|batch| batch.put_with_options(key, value, &options))
            .map_err(Into::into)
    }

    pub fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<(), DbError> {
        validate_key_value(&key, &operand)?;
        self.with_open(|batch| batch.merge(key, operand))
            .map_err(Into::into)
    }

    pub fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        options: MergeOptions,
    ) -> Result<(), DbError> {
        validate_key_value(&key, &operand)?;
        let options = options.into_core();
        self.with_open(|batch| batch.merge_with_options(key, operand, &options))
            .map_err(Into::into)
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), DbError> {
        validate_key(&key)?;
        self.with_open(|batch| batch.delete(key))
            .map_err(Into::into)
    }
}
