use std::sync::Arc;

use crate::config::{FfiReadOptions, FfiScanOptions};
use crate::error::FfiError;
use crate::iterator::FfiDbIterator;
use crate::types::FfiKeyRange;

#[derive(uniffi::Object)]
pub struct FfiDbReader {
    inner: slatedb::DbReader,
}

impl FfiDbReader {
    pub(crate) fn new(inner: slatedb::DbReader) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbReader {
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, FfiError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<Vec<u8>>, FfiError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn scan(&self, range: FfiKeyRange) -> Result<Arc<FfiDbIterator>, FfiError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: FfiKeyRange,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_with_options::<Vec<u8>, _>(range, &options)
            .await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<FfiDbIterator>, FfiError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: FfiScanOptions,
    ) -> Result<Arc<FfiDbIterator>, FfiError> {
        let options = options.into_core()?;
        let iter = self
            .inner
            .scan_prefix_with_options(prefix, &options)
            .await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    pub async fn close(&self) -> Result<(), FfiError> {
        self.inner.close().await.map_err(Into::into)
    }
}
