use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{
    FfiFlushOptions, FfiIsolationLevel, FfiMergeOptions, FfiPutOptions, FfiReadOptions,
    FfiScanOptions, FfiWriteOptions,
};
use crate::error::FfiSlatedbError;
use crate::iterator::FfiDbIterator;
use crate::transaction::FfiDbTransaction;
use crate::types::{FfiKeyRange, FfiKeyValue, FfiWriteHandle};
use crate::validation::{validate_key, validate_key_value};
use crate::write_batch::FfiWriteBatch;

#[derive(uniffi::Object)]
pub struct FfiDb {
    inner: slatedb::Db,
}

impl FfiDb {
    pub(crate) fn new(inner: slatedb::Db) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl FfiDb {
    pub fn status(&self) -> Result<(), FfiSlatedbError> {
        self.inner.status().map_err(Into::into)
    }

    pub fn metrics(&self) -> Result<HashMap<String, i64>, FfiSlatedbError> {
        let registry = self.inner.metrics();
        let mut snapshot = HashMap::new();
        for name in registry.names() {
            if let Some(stat) = registry.lookup(name) {
                snapshot.insert(name.to_owned(), stat.get());
            }
        }
        Ok(snapshot)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDb {
    // `shutdown` because `close` is reserved by uniffi for the destructor
    #[uniffi::method(name = "shutdown")]
    pub async fn close(&self) -> Result<(), FfiSlatedbError> {
        self.inner.close().await.map_err(Into::into)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, FfiSlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<Vec<u8>>, FfiSlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        Ok(self
            .inner
            .get_key_value(key)
            .await?
            .map(FfiKeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(FfiKeyValue::from_core))
    }

    pub async fn scan(&self, range: FfiKeyRange) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: FfiKeyRange,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_with_options::<Vec<u8>, _>(range, &options)
            .await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &value)?;
        Ok(FfiWriteHandle::from_core(self.inner.put(key, value).await?))
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        put_options: FfiPutOptions,
        write_options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &value)?;
        let put_options = put_options.into_core();
        let write_options = write_options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner
                .put_with_options(key, value, &put_options, &write_options)
                .await?,
        ))
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key(&key)?;
        Ok(FfiWriteHandle::from_core(self.inner.delete(key).await?))
    }

    pub async fn delete_with_options(
        &self,
        key: Vec<u8>,
        options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key(&key)?;
        let options = options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner.delete_with_options(key, &options).await?,
        ))
    }

    pub async fn merge(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &operand)?;
        Ok(FfiWriteHandle::from_core(
            self.inner.merge(key, operand).await?,
        ))
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        merge_options: FfiMergeOptions,
        write_options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &operand)?;
        let merge_options = merge_options.into_core();
        let write_options = write_options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner
                .merge_with_options(key, operand, &merge_options, &write_options)
                .await?,
        ))
    }

    pub async fn write(
        &self,
        batch: Arc<FfiWriteBatch>,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        let batch = batch.take_for_write()?;
        Ok(FfiWriteHandle::from_core(self.inner.write(batch).await?))
    }

    pub async fn write_with_options(
        &self,
        batch: Arc<FfiWriteBatch>,
        options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        let batch = batch.take_for_write()?;
        let options = options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner.write_with_options(batch, &options).await?,
        ))
    }

    pub async fn flush(&self) -> Result<(), FfiSlatedbError> {
        self.inner.flush().await.map_err(Into::into)
    }

    pub async fn flush_with_options(
        &self,
        options: FfiFlushOptions,
    ) -> Result<(), FfiSlatedbError> {
        self.inner
            .flush_with_options(options.into_core())
            .await
            .map_err(Into::into)
    }

    pub async fn snapshot(&self) -> Result<Arc<FfiDbSnapshot>, FfiSlatedbError> {
        Ok(Arc::new(FfiDbSnapshot {
            inner: self.inner.snapshot().await?,
        }))
    }

    pub async fn begin(
        &self,
        isolation_level: FfiIsolationLevel,
    ) -> Result<Arc<FfiDbTransaction>, FfiSlatedbError> {
        let tx = self.inner.begin(isolation_level.into_core()).await?;
        Ok(Arc::new(FfiDbTransaction::new(tx)))
    }
}

#[derive(uniffi::Object)]
pub struct FfiDbSnapshot {
    inner: Arc<slatedb::DbSnapshot>,
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbSnapshot {
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, FfiSlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<Vec<u8>>, FfiSlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        Ok(self
            .inner
            .get_key_value(key)
            .await?
            .map(FfiKeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(FfiKeyValue::from_core))
    }

    pub async fn scan(&self, range: FfiKeyRange) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: FfiKeyRange,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_with_options::<Vec<u8>, _>(range, &options)
            .await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }
}
