use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{
    FlushOptions, IsolationLevel, MergeOptions, PutOptions, ReadOptions, ScanOptions, WriteOptions,
};
use crate::db_snapshot::DbSnapshot;
use crate::db_transaction::DbTransaction;
use crate::error::Error;
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
    pub fn status(&self) -> Result<(), Error> {
        self.inner.status().map_err(Into::into)
    }

    pub fn metrics(&self) -> Result<HashMap<String, i64>, Error> {
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
    pub async fn close(&self) -> Result<(), Error> {
        self.inner.close().await.map_err(Into::into)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        let options = options.into();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, Error> {
        Ok(self.inner.get_key_value(key).await?.map(KeyValue::from))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<KeyValue>, Error> {
        let options = options.into();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from))
    }

    pub async fn scan(&self, range: KeyRange) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let options = options.try_into()?;
        let iter = self
            .inner
            .scan_with_options::<Vec<u8>, _>(range, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, Error> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let options = options.try_into()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<WriteHandle, Error> {
        validate_key_value(&key, &value)?;
        Ok(self.inner.put(key, value).await?.into())
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        put_options: PutOptions,
        write_options: WriteOptions,
    ) -> Result<WriteHandle, Error> {
        validate_key_value(&key, &value)?;
        let put_options = put_options.into();
        let write_options = write_options.into();
        Ok(self
            .inner
            .put_with_options(key, value, &put_options, &write_options)
            .await?
            .into())
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<WriteHandle, Error> {
        validate_key(&key)?;
        Ok(self.inner.delete(key).await?.into())
    }

    pub async fn delete_with_options(
        &self,
        key: Vec<u8>,
        options: WriteOptions,
    ) -> Result<WriteHandle, Error> {
        validate_key(&key)?;
        let options = options.into();
        Ok(self.inner.delete_with_options(key, &options).await?.into())
    }

    pub async fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> Result<WriteHandle, Error> {
        validate_key_value(&key, &operand)?;
        Ok(self.inner.merge(key, operand).await?.into())
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        merge_options: MergeOptions,
        write_options: WriteOptions,
    ) -> Result<WriteHandle, Error> {
        validate_key_value(&key, &operand)?;
        let merge_options = merge_options.into();
        let write_options = write_options.into();
        Ok(self
            .inner
            .merge_with_options(key, operand, &merge_options, &write_options)
            .await?
            .into())
    }

    pub async fn write(&self, batch: Arc<WriteBatch>) -> Result<WriteHandle, Error> {
        let batch = batch.take_for_write()?;
        Ok(self.inner.write(batch).await?.into())
    }

    pub async fn write_with_options(
        &self,
        batch: Arc<WriteBatch>,
        options: WriteOptions,
    ) -> Result<WriteHandle, Error> {
        let batch = batch.take_for_write()?;
        let options = options.into();
        Ok(self.inner.write_with_options(batch, &options).await?.into())
    }

    pub async fn flush(&self) -> Result<(), Error> {
        self.inner.flush().await.map_err(Into::into)
    }

    pub async fn flush_with_options(&self, options: FlushOptions) -> Result<(), Error> {
        self.inner
            .flush_with_options(options.into())
            .await
            .map_err(Into::into)
    }

    pub async fn snapshot(&self) -> Result<Arc<DbSnapshot>, Error> {
        Ok(Arc::new(DbSnapshot::new(self.inner.snapshot().await?)))
    }

    pub async fn begin(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<DbTransaction>, Error> {
        let tx = self.inner.begin(isolation_level.into()).await?;
        Ok(Arc::new(DbTransaction::new(tx)))
    }
}
