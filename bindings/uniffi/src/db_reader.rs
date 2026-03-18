use std::sync::Arc;

use crate::config::{ReadOptions, ScanOptions};
use crate::error::DbError;
use crate::iterator::DbIterator;
use crate::types::KeyRange;

#[derive(uniffi::Object)]
pub struct DbReader {
    inner: slatedb::DbReader,
}

impl DbReader {
    pub(crate) fn new(inner: slatedb::DbReader) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbReader {
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

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    #[uniffi::method(name = "shutdown")]
    pub async fn close(&self) -> Result<(), DbError> {
        self.inner.close().await.map_err(Into::into)
    }
}
