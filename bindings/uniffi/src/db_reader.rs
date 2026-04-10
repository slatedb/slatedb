use std::sync::Arc;
use std::time::Duration;

use crate::config::{ReadOptions, ScanOptions};
use crate::error::Error;
use crate::iterator::DbIterator;
use crate::types::KeyRange;
use crate::validation::validate_key;

/// Read-only database handle opened by [`crate::DbReaderBuilder`].
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
    /// Reads the current value for `key`.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    /// Reads the current value for `key` using custom read options.
    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        validate_key(&key)?;
        let timeout_ms = options.timeout_ms;
        let read_options = options.into();
        let fut = self.inner.get_with_options(key, &read_options);
        let result = match timeout_ms {
            Some(ms) => tokio::time::timeout(Duration::from_millis(ms), fut)
                .await
                .map_err(|_| Error::Timeout {
                    message: format!("get timed out after {}ms", ms),
                })?,
            None => fut.await,
        }?;
        Ok(result.map(|value| value.to_vec()))
    }

    /// Scans rows inside `range`.
    pub async fn scan(&self, range: KeyRange) -> Result<Arc<DbIterator>, Error> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows inside `range` using custom scan options.
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

    /// Scans rows whose keys start with `prefix`.
    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, Error> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scans rows whose keys start with `prefix` using custom scan options.
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

    // `shutdown` because `close` is reserved by uniffi for the destructor.
    /// Closes the reader.
    #[uniffi::method(name = "shutdown")]
    pub async fn close(&self) -> Result<(), Error> {
        self.inner.close().await.map_err(Into::into)
    }
}
