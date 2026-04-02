use std::sync::Arc;

use parking_lot::Mutex;

use crate::config::{MergeOptions, PutOptions};
use crate::error::{Error, SlateDbError};
use crate::validation::{validate_key, validate_key_value};

/// Mutable batch of write operations applied atomically by [`crate::Db::write`].
///
/// A batch is single-use once submitted to the database.
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
    /// Creates an empty write batch.
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some(slatedb::WriteBatch::new())),
        })
    }

    /// Appends a put operation to the batch.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        validate_key_value(&key, &value)?;
        self.with_open(|batch| batch.put(key, value))
            .map_err(Into::into)
    }

    /// Appends a put operation with custom put options.
    pub fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<(), Error> {
        validate_key_value(&key, &value)?;
        let options = options.into();
        self.with_open(|batch| batch.put_with_options(key, value, &options))
            .map_err(Into::into)
    }

    /// Appends a merge operation to the batch.
    pub fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<(), Error> {
        validate_key_value(&key, &operand)?;
        self.with_open(|batch| batch.merge(key, operand))
            .map_err(Into::into)
    }

    /// Appends a merge operation with custom merge options.
    pub fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        options: MergeOptions,
    ) -> Result<(), Error> {
        validate_key_value(&key, &operand)?;
        let options = options.into();
        self.with_open(|batch| batch.merge_with_options(key, operand, &options))
            .map_err(Into::into)
    }

    /// Appends a delete operation to the batch.
    pub fn delete(&self, key: Vec<u8>) -> Result<(), Error> {
        validate_key(&key)?;
        self.with_open(|batch| batch.delete(key))
            .map_err(Into::into)
    }
}
