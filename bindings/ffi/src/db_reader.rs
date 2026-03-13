//! Read-only database reader wrapper.

use std::sync::Arc;

use slatedb::DbReader as CoreDbReader;

use crate::config::{KeyRange, ReadOptions, ScanOptions};
use crate::error::SlatedbError;
use crate::iterator::DbIterator;

/// A read-only database reader.
#[derive(uniffi::Object)]
pub struct DbReader {
    inner: CoreDbReader,
}

impl DbReader {
    pub(crate) fn new(inner: CoreDbReader) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbReader {
    /// Get the value for a key using default read options.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, SlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    /// Get the value for a key using custom read options.
    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    /// Scan a key range using default scan options.
    pub async fn scan(&self, range: KeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scan a key range using custom scan options.
    pub async fn scan_with_options(
        &self,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_with_options::<Vec<u8>, _>(range, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scan all keys that share the provided prefix.
    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, SlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scan all keys that share the provided prefix using custom scan options.
    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: ScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Close the reader.
    pub async fn close(&self) -> Result<(), SlatedbError> {
        self.inner.close().await.map_err(Into::into)
    }
}
