use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{
    FlushOptions, IsolationLevel, MergeOptions, PutOptions, ReadOptions, ScanOptions, WriteOptions,
};
use crate::db_snapshot::DbSnapshot;
use crate::db_transaction::DbTransaction;
use crate::error::DbError;
use crate::iterator::DbIterator;
use crate::types::{KeyRange, KeyValue, WriteHandle};
use crate::validation::{validate_key, validate_key_value};
use crate::write_batch::WriteBatch;

#[derive(uniffi::Object)]
pub struct Db {
    inner: slatedb::Db,
}

impl Db {
    pub(crate) fn new(inner: slatedb::Db) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl Db {
    pub fn status(&self) -> Result<(), DbError> {
        self.inner.status().map_err(Into::into)
    }

    pub fn metrics(&self) -> Result<HashMap<String, i64>, DbError> {
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
impl Db {
    // `shutdown` because `close` is reserved by uniffi for the destructor
    #[uniffi::method(name = "shutdown")]
    pub async fn close(&self) -> Result<(), DbError> {
        self.inner.close().await.map_err(Into::into)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>, DbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, DbError> {
        Ok(self
            .inner
            .get_key_value(key)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<KeyValue>, DbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn scan(&self, range: KeyRange) -> Result<Arc<DbIterator>, DbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, DbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_with_options::<Vec<u8>, _>(range, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, DbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, DbError> {
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<WriteHandle, DbError> {
        validate_key_value(&key, &value)?;
        Ok(WriteHandle::from_core(self.inner.put(key, value).await?))
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        put_options: PutOptions,
        write_options: WriteOptions,
    ) -> Result<WriteHandle, DbError> {
        validate_key_value(&key, &value)?;
        let put_options = put_options.into_core();
        let write_options = write_options.into_core();
        Ok(WriteHandle::from_core(
            self.inner
                .put_with_options(key, value, &put_options, &write_options)
                .await?,
        ))
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<WriteHandle, DbError> {
        validate_key(&key)?;
        Ok(WriteHandle::from_core(self.inner.delete(key).await?))
    }

    pub async fn delete_with_options(
        &self,
        key: Vec<u8>,
        options: WriteOptions,
    ) -> Result<WriteHandle, DbError> {
        validate_key(&key)?;
        let options = options.into_core();
        Ok(WriteHandle::from_core(
            self.inner.delete_with_options(key, &options).await?,
        ))
    }

    pub async fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<WriteHandle, DbError> {
        validate_key_value(&key, &operand)?;
        Ok(WriteHandle::from_core(
            self.inner.merge(key, operand).await?,
        ))
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        merge_options: MergeOptions,
        write_options: WriteOptions,
    ) -> Result<WriteHandle, DbError> {
        validate_key_value(&key, &operand)?;
        let merge_options = merge_options.into_core();
        let write_options = write_options.into_core();
        Ok(WriteHandle::from_core(
            self.inner
                .merge_with_options(key, operand, &merge_options, &write_options)
                .await?,
        ))
    }

    pub async fn write(&self, batch: Arc<WriteBatch>) -> Result<WriteHandle, DbError> {
        let batch = batch.take_for_write()?;
        Ok(WriteHandle::from_core(self.inner.write(batch).await?))
    }

    pub async fn write_with_options(
        &self,
        batch: Arc<WriteBatch>,
        options: WriteOptions,
    ) -> Result<WriteHandle, DbError> {
        let batch = batch.take_for_write()?;
        let options = options.into_core();
        Ok(WriteHandle::from_core(
            self.inner.write_with_options(batch, &options).await?,
        ))
    }

    pub async fn flush(&self) -> Result<(), DbError> {
        self.inner.flush().await.map_err(Into::into)
    }

    pub async fn flush_with_options(&self, options: FlushOptions) -> Result<(), DbError> {
        self.inner
            .flush_with_options(options.into_core())
            .await
            .map_err(Into::into)
    }

    pub async fn snapshot(&self) -> Result<Arc<DbSnapshot>, DbError> {
        Ok(Arc::new(DbSnapshot::new(self.inner.snapshot().await?)))
    }

    pub async fn begin(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<DbTransaction>, DbError> {
        let tx = self.inner.begin(isolation_level.into_core()).await?;
        Ok(Arc::new(DbTransaction::new(tx)))
    }
}
