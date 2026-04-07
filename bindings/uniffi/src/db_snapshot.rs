use std::sync::Arc;

use crate::config::{ViewReadOptions, ViewScanOptions};
use crate::error::Error;
use crate::iterator::DbIterator;
use crate::types::{KeyRange, KeyValue};
use crate::validation::validate_key;

/// Read-only snapshot representing a consistent view of the database.
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
    /// Reads the value visible in this snapshot for `key`.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    /// Reads the value visible in this snapshot for `key` using custom read options.
    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ViewReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        let options = options.into();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    /// Reads the row version visible in this snapshot for `key`.
    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, Error> {
        validate_key(&key)?;
        Ok(self.inner.get_key_value(key).await?.map(KeyValue::from))
    }

    /// Reads the row version visible in this snapshot for `key` using custom read options.
    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: ViewReadOptions,
    ) -> Result<Option<KeyValue>, Error> {
        validate_key(&key)?;
        let options = options.into();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from))
    }

    /// Scans rows inside `range` as of this snapshot.
    pub async fn scan(&self, range: KeyRange) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows inside `range` as of this snapshot using custom scan options.
    pub async fn scan_with_options(
        &self,
        range: KeyRange,
        options: ViewScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let options = options.try_into()?;
        let iter = self
            .inner
            .scan_with_options::<Vec<u8>, _>(range, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows whose keys start with `prefix` as of this snapshot.
    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, Error> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows whose keys start with `prefix` as of this snapshot using custom options.
    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: ViewScanOptions,
    ) -> Result<Arc<DbIterator>, Error> {
        let options = options.try_into()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }
}
