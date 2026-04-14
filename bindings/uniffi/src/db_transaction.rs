use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{MergeOptions, PutOptions, ViewReadOptions, ViewScanOptions, WriteOptions};
use crate::error::{Error, SlateDbError};
use crate::iterator::DbIterator;
use crate::types::{KeyRange, KeyValue, WriteHandle};
use crate::validation::{validate_key, validate_key_value};

/// Transaction handle returned by [`crate::Db::begin`].
///
/// A transaction becomes unusable after `commit`, `commit_with_options`, or
/// `rollback`.
#[derive(uniffi::Object)]
pub struct DbTransaction {
    inner: Mutex<Option<slatedb::DbTransaction>>,
    id: String,
    seqnum: u64,
}

impl DbTransaction {
    pub(crate) fn new(inner: slatedb::DbTransaction) -> Self {
        Self {
            id: inner.id().to_string(),
            seqnum: inner.seqnum(),
            inner: Mutex::new(Some(inner)),
        }
    }
}

#[uniffi::export]
impl DbTransaction {
    /// Returns the sequence number assigned when the transaction started.
    pub fn seqnum(&self) -> u64 {
        self.seqnum
    }

    /// Returns the transaction identifier as a UUID string.
    pub fn id(&self) -> String {
        self.id.clone()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbTransaction {
    /// Buffers a put inside the transaction.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        validate_key_value(&key, &value)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.put(key, value).map_err(Into::into)
    }

    /// Buffers a put inside the transaction using custom put options.
    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<(), Error> {
        validate_key_value(&key, &value)?;
        let options = options.into();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.put_with_options(key, value, &options)
            .map_err(Into::into)
    }

    /// Buffers a delete inside the transaction.
    pub async fn delete(&self, key: Vec<u8>) -> Result<(), Error> {
        validate_key(&key)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.delete(key).map_err(Into::into)
    }

    /// Buffers a merge operand inside the transaction.
    pub async fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<(), Error> {
        validate_key_value(&key, &operand)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.merge(key, operand).map_err(Into::into)
    }

    /// Buffers a merge operand inside the transaction using custom merge options.
    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        options: MergeOptions,
    ) -> Result<(), Error> {
        validate_key_value(&key, &operand)?;
        let options = options.into();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.merge_with_options(key, operand, &options)
            .map_err(Into::into)
    }

    /// Marks keys as read for conflict detection.
    pub async fn mark_read(&self, keys: Vec<Vec<u8>>) -> Result<(), Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.mark_read(keys).map_err(Into::into)
    }

    /// Excludes written keys from transaction conflict detection.
    pub async fn unmark_write(&self, keys: Vec<Vec<u8>>) -> Result<(), Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.unmark_write(keys).map_err(Into::into)
    }

    /// Rolls back the transaction and marks it completed.
    pub async fn rollback(&self) -> Result<(), Error> {
        let mut guard = self.inner.lock().await;
        let tx = guard.take().ok_or(SlateDbError::TransactionCompleted)?;
        tx.rollback();
        Ok(())
    }

    /// Reads the value visible to this transaction for `key`.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx.get(key).await?.map(|value| value.to_vec()))
    }

    /// Reads the value visible to this transaction for `key` using custom read options.
    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ViewReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        let options = options.into();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    /// Reads the row version visible to this transaction for `key`.
    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, Error> {
        validate_key(&key)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx.get_key_value(key).await?.map(KeyValue::from))
    }

    /// Reads the row version visible to this transaction for `key` using custom options.
    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: ViewReadOptions,
    ) -> Result<Option<KeyValue>, Error> {
        validate_key(&key)?;
        let options = options.into();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from))
    }

    /// Scans rows inside `range` as visible to this transaction.
    pub async fn scan(&self, range: KeyRange) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows inside `range` as visible to this transaction using custom options.
    pub async fn scan_with_options(
        &self,
        range: KeyRange,
        options: ViewScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let options = options.try_into()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows whose keys start with `prefix` as visible to this transaction.
    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows whose keys start with `prefix` as visible to this transaction using custom options.
    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: ViewScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let options = options.try_into()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Commits the transaction.
    ///
    /// Returns `None` when the transaction performed no writes.
    pub async fn commit(&self) -> Result<Option<WriteHandle>, Error> {
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or(SlateDbError::TransactionCompleted)?
        };
        Ok(tx.commit().await?.map(WriteHandle::from))
    }

    /// Commits the transaction using custom write options.
    ///
    /// Returns `None` when the transaction performed no writes.
    pub async fn commit_with_options(
        &self,
        options: WriteOptions,
    ) -> Result<Option<WriteHandle>, Error> {
        let options = options.into();
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or(SlateDbError::TransactionCompleted)?
        };
        Ok(tx
            .commit_with_options(&options)
            .await?
            .map(WriteHandle::from))
    }
}
