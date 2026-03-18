use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{MergeOptions, PutOptions, ReadOptions, ScanOptions, WriteOptions};
use crate::error::{Error, SlateDbError};
use crate::iterator::DbIterator;
use crate::types::{KeyRange, KeyValue, WriteHandle};
use crate::validation::{validate_key, validate_key_value};

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
    pub fn seqnum(&self) -> u64 {
        self.seqnum
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbTransaction {
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        validate_key_value(&key, &value)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.put(key, value).map_err(Into::into)
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<(), Error> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.put_with_options(key, value, &options)
            .map_err(Into::into)
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<(), Error> {
        validate_key(&key)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.delete(key).map_err(Into::into)
    }

    pub async fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<(), Error> {
        validate_key_value(&key, &operand)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.merge(key, operand).map_err(Into::into)
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        options: MergeOptions,
    ) -> Result<(), Error> {
        validate_key_value(&key, &operand)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.merge_with_options(key, operand, &options)
            .map_err(Into::into)
    }

    pub async fn mark_read(&self, keys: Vec<Vec<u8>>) -> Result<(), Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.mark_read(keys).map_err(Into::into)
    }

    pub async fn unmark_write(&self, keys: Vec<Vec<u8>>) -> Result<(), Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        tx.unmark_write(keys).map_err(Into::into)
    }

    pub async fn rollback(&self) -> Result<(), Error> {
        let mut guard = self.inner.lock().await;
        let tx = guard.take().ok_or(SlateDbError::TransactionCompleted)?;
        tx.rollback();
        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx.get_key_value(key).await?.map(KeyValue::from))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<KeyValue>, Error> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        Ok(tx
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from))
    }

    pub async fn scan(&self, range: KeyRange) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, Error> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or(SlateDbError::TransactionCompleted)?;
        let iter = tx.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn commit(&self) -> Result<Option<WriteHandle>, Error> {
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or(SlateDbError::TransactionCompleted)?
        };
        Ok(tx.commit().await?.map(WriteHandle::from))
    }

    pub async fn commit_with_options(
        &self,
        options: WriteOptions,
    ) -> Result<Option<WriteHandle>, Error> {
        let options = options.into_core();
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
