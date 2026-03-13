//! Read-only database reader wrapper.

use std::sync::Arc;

use slatedb::DbReader as CoreDbReader;

use crate::config::{FfiKeyRange, FfiReadOptions, FfiScanOptions};
use crate::error::FfiSlatedbError;
use crate::iterator::FfiDbIterator;

/// A read-only database reader.
#[derive(uniffi::Object)]
pub struct FfiDbReader {
    inner: CoreDbReader,
}

impl FfiDbReader {
    pub(crate) fn new(inner: CoreDbReader) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbReader {
    /// Get the value for a key using default read options.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, FfiSlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    /// Get the value for a key using custom read options.
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

    /// Scan a key range using default scan options.
    pub async fn scan(&self, range: FfiKeyRange) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    /// Scan a key range using custom scan options.
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

    /// Scan all keys that share the provided prefix.
    pub async fn scan_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    /// Scan all keys that share the provided prefix using custom scan options.
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

    /// Close the reader.
    pub async fn close(&self) -> Result<(), FfiSlatedbError> {
        self.inner.close().await.map_err(Into::into)
    }
}
