use std::sync::Arc;

use slatedb::DbTransaction as CoreDbTransaction;
use tokio::sync::Mutex as AsyncMutex;

use crate::config::{
    DbKeyRange, DbMergeOptions, DbPutOptions, DbReadOptions, DbScanOptions, DbWriteOptions,
    KeyValue, WriteHandle,
};
use crate::error::SlatedbError;
use crate::iterator::DbIterator;
use crate::validation::{transaction_completed, validate_key, validate_key_value};

#[derive(uniffi::Object)]
pub struct DbTransaction {
    inner: AsyncMutex<Option<CoreDbTransaction>>,
    id: String,
    seqnum: u64,
}

impl DbTransaction {
    pub(crate) fn new(inner: CoreDbTransaction) -> Self {
        Self {
            id: inner.id().to_string(),
            seqnum: inner.seqnum(),
            inner: AsyncMutex::new(Some(inner)),
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
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.put(key, value).map_err(Into::into)
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbPutOptions,
    ) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.put_with_options(key, value, &options).map_err(Into::into)
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<(), SlatedbError> {
        validate_key(&key)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.delete(key).map_err(Into::into)
    }

    pub async fn merge(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.merge(key, value).map_err(Into::into)
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbMergeOptions,
    ) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.merge_with_options(key, value, &options)
            .map_err(Into::into)
    }

    pub async fn mark_read(&self, keys: Vec<Vec<u8>>) -> Result<(), SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.mark_read(keys).map_err(Into::into)
    }

    pub async fn unmark_write(&self, keys: Vec<Vec<u8>>) -> Result<(), SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.unmark_write(keys).map_err(Into::into)
    }

    pub async fn rollback(&self) -> Result<(), SlatedbError> {
        let mut guard = self.inner.lock().await;
        let tx = guard.take().ok_or_else(transaction_completed)?;
        tx.rollback();
        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<Vec<u8>>, SlatedbError> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx.get_key_value(key).await?.map(KeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<KeyValue>, SlatedbError> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn scan(&self, range: DbKeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: DbKeyRange,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn commit(&self) -> Result<Option<WriteHandle>, SlatedbError> {
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or_else(transaction_completed)?
        };
        Ok(tx.commit().await?.map(WriteHandle::from_core))
    }

    pub async fn commit_with_options(
        &self,
        options: DbWriteOptions,
    ) -> Result<Option<WriteHandle>, SlatedbError> {
        let options = options.into_core();
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or_else(transaction_completed)?
        };
        Ok(tx
            .commit_with_options(&options)
            .await?
            .map(WriteHandle::from_core))
    }
}
