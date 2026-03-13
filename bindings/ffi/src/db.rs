use std::sync::Arc;

use slatedb::{Db as CoreDb, DbSnapshot as CoreDbSnapshot};

use crate::config::{
    DbFlushOptions, DbKeyRange, DbMergeOptions, DbPutOptions, DbReadOptions, DbScanOptions,
    DbWriteOperation, DbWriteOptions, IsolationLevel, KeyValue, WriteHandle,
};
use crate::error::SlatedbError;
use crate::iterator::DbIterator;
use crate::transaction::DbTransaction;
use crate::validation::{build_write_batch, validate_key, validate_key_value};

#[derive(uniffi::Object)]
pub struct Db {
    inner: CoreDb,
}

#[derive(uniffi::Object)]
pub struct DbSnapshot {
    inner: Arc<CoreDbSnapshot>,
}

impl Db {
    pub(crate) fn new(inner: CoreDb) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl Db {
    pub fn status(&self) -> Result<(), SlatedbError> {
        self.inner.status().map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Db {
    pub async fn close(&self) -> Result<(), SlatedbError> {
        self.inner.close().await.map_err(Into::into)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, SlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<Vec<u8>>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, SlatedbError> {
        Ok(self.inner.get_key_value(key).await?.map(KeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<KeyValue>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn scan(&self, range: DbKeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: DbKeyRange,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self.inner.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, SlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let options = options.into_core()?;
        let iter = self.inner.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        Ok(WriteHandle::from_core(self.inner.put(key, value).await?))
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        put_options: DbPutOptions,
        write_options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        let put_options = put_options.into_core();
        let write_options = write_options.into_core();
        Ok(WriteHandle::from_core(
            self.inner
                .put_with_options(key, value, &put_options, &write_options)
                .await?,
        ))
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<WriteHandle, SlatedbError> {
        validate_key(&key)?;
        Ok(WriteHandle::from_core(self.inner.delete(key).await?))
    }

    pub async fn delete_with_options(
        &self,
        key: Vec<u8>,
        options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key(&key)?;
        let options = options.into_core();
        Ok(WriteHandle::from_core(
            self.inner.delete_with_options(key, &options).await?,
        ))
    }

    pub async fn merge(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        Ok(WriteHandle::from_core(self.inner.merge(key, value).await?))
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        merge_options: DbMergeOptions,
        write_options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        let merge_options = merge_options.into_core();
        let write_options = write_options.into_core();
        Ok(WriteHandle::from_core(
            self.inner
                .merge_with_options(key, value, &merge_options, &write_options)
                .await?,
        ))
    }

    pub async fn write(
        &self,
        operations: Vec<DbWriteOperation>,
    ) -> Result<WriteHandle, SlatedbError> {
        let batch = build_write_batch(operations)?;
        Ok(WriteHandle::from_core(self.inner.write(batch).await?))
    }

    pub async fn write_with_options(
        &self,
        operations: Vec<DbWriteOperation>,
        options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        let batch = build_write_batch(operations)?;
        let options = options.into_core();
        Ok(WriteHandle::from_core(
            self.inner.write_with_options(batch, &options).await?,
        ))
    }

    pub async fn flush(&self) -> Result<(), SlatedbError> {
        self.inner.flush().await.map_err(Into::into)
    }

    pub async fn flush_with_options(&self, options: DbFlushOptions) -> Result<(), SlatedbError> {
        self.inner
            .flush_with_options(options.into_core())
            .await
            .map_err(Into::into)
    }

    pub async fn snapshot(&self) -> Result<Arc<DbSnapshot>, SlatedbError> {
        Ok(Arc::new(DbSnapshot {
            inner: self.inner.snapshot().await?,
        }))
    }

    pub async fn begin(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<DbTransaction>, SlatedbError> {
        let tx = self.inner.begin(isolation_level.into_core()).await?;
        Ok(Arc::new(DbTransaction::new(tx)))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbSnapshot {
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, SlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<Vec<u8>>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, SlatedbError> {
        Ok(self.inner.get_key_value(key).await?.map(KeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<KeyValue>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn scan(&self, range: DbKeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: DbKeyRange,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self.inner.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, SlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let options = options.into_core()?;
        let iter = self.inner.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }
}
