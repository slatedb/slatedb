use std::sync::Arc;

use crate::config::{ReadOptions, ScanOptions};
use crate::error::Error;
use crate::iterator::DbIterator;
use crate::types::{KeyRange, KeyValue};

#[derive(uniffi::Object)]
pub struct DbSnapshot {
    inner: Arc<slatedb::DbSnapshot>,
}

impl DbSnapshot {
    pub(crate) fn new(inner: Arc<slatedb::DbSnapshot>) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbSnapshot {
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, Error> {
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
    ) -> Result<Option<KeyValue>, Error> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
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
        let options = options.into_core()?;
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
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }
}
