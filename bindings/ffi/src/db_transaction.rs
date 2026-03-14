use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::{
    FfiMergeOptions, FfiPutOptions, FfiReadOptions, FfiScanOptions, FfiWriteOptions,
};
use crate::error::{FfiError, FfiSlateDbError};
use crate::iterator::FfiDbIterator;
use crate::types::{FfiKeyRange, FfiKeyValue, FfiWriteHandle};
use crate::validation::{validate_key, validate_key_value};

#[derive(uniffi::Object)]
pub struct FfiDbTransaction {
    inner: Mutex<Option<slatedb::DbTransaction>>,
    id: String,
    seqnum: u64,
}

impl FfiDbTransaction {
    pub(crate) fn new(inner: slatedb::DbTransaction) -> Self {
        Self {
            id: inner.id().to_string(),
            seqnum: inner.seqnum(),
            inner: Mutex::new(Some(inner)),
        }
    }
}

#[uniffi::export]
impl FfiDbTransaction {
    pub fn seqnum(&self) -> u64 {
        self.seqnum
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbTransaction {
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), FfiError> {
        validate_key_value(&key, &value)?;
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.put(key, value).map_err(Into::into)
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: FfiPutOptions,
    ) -> Result<(), FfiError> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.put_with_options(key, value, &options)
            .map_err(Into::into)
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<(), FfiError> {
        validate_key(&key)?;
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.delete(key).map_err(Into::into)
    }

    pub async fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<(), FfiError> {
        validate_key_value(&key, &operand)?;
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.merge(key, operand).map_err(Into::into)
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        options: FfiMergeOptions,
    ) -> Result<(), FfiError> {
        validate_key_value(&key, &operand)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.merge_with_options(key, operand, &options)
            .map_err(Into::into)
    }

    pub async fn mark_read(&self, keys: Vec<Vec<u8>>) -> Result<(), FfiError> {
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.mark_read(keys).map_err(Into::into)
    }

    pub async fn unmark_write(&self, keys: Vec<Vec<u8>>) -> Result<(), FfiError> {
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.unmark_write(keys).map_err(Into::into)
    }

    pub async fn rollback(&self) -> Result<(), FfiError> {
        let mut guard = self.inner.lock().await;
        let tx = guard.take().ok_or(FfiSlateDbError::TransactionCompleted)?;
        tx.rollback();
        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, FfiError> {
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        Ok(tx.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<Vec<u8>>, FfiError> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        Ok(tx
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<FfiKeyValue>, FfiError> {
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        Ok(tx.get_key_value(key).await?.map(FfiKeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<FfiKeyValue>, FfiError> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        Ok(tx
            .get_key_value_with_options(key, &options)
            .await?
            .map(FfiKeyValue::from_core))
    }

    pub async fn scan(&self, range: FfiKeyRange) -> Result<Arc<FfiDbIterator>, FfiError> {
        let range = range.into_bounds()?;
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        let iter = tx.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: FfiKeyRange,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        let iter = tx.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<FfiDbIterator>, FfiError> {
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        let iter = tx.scan_prefix(prefix).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiError> {
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard
            .as_ref()
            .ok_or(FfiSlateDbError::TransactionCompleted)?;
        let iter = tx.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn commit(&self) -> Result<Option<FfiWriteHandle>, FfiError> {
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or(FfiSlateDbError::TransactionCompleted)?
        };
        Ok(tx.commit().await?.map(FfiWriteHandle::from_core))
    }

    pub async fn commit_with_options(
        &self,
        options: FfiWriteOptions,
    ) -> Result<Option<FfiWriteHandle>, FfiError> {
        let options = options.into_core();
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or(FfiSlateDbError::TransactionCompleted)?
        };
        Ok(tx
            .commit_with_options(&options)
            .await?
            .map(FfiWriteHandle::from_core))
    }
}
