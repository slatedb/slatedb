use std::sync::Arc;
use std::time::Duration;

use crate::config::{ReadOptions, ScanOptions};
use crate::error::Error;
use crate::iterator::DbIterator;
use crate::types::{CacheTarget, DbStatus, KeyRange, SsTableId};
use crate::validation::validate_key;
use crate::KeyValue;
use slatedb::DbCacheManagerOps;

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

#[uniffi::export]
impl DbReader {
    /// Returns the latest reader status snapshot.
    pub fn status(&self) -> DbStatus {
        self.inner.status().into()
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

    /// Reads the current row version for `key`, including metadata.
    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, Error> {
        validate_key(&key)?;
        Ok(self.inner.get_key_value(key).await?.map(KeyValue::from))
    }

    /// Reads the current row version for `key` using custom read options.
    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: ReadOptions,
    ) -> Result<Option<KeyValue>, Error> {
        validate_key(&key)?;
        let options = options.into();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from))
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

    /// Warms selected cache content for one SST.
    ///
    /// Returns `Err` on the first failing target. If no block cache is
    /// configured, or if the SST is not reachable from the current manifest,
    /// the call is a no-op that returns `Ok(())`.
    pub async fn warm_sst(
        &self,
        sst_id: SsTableId,
        targets: Vec<CacheTarget>,
    ) -> Result<(), Error> {
        let sst_id = sst_id.into_core()?;
        let targets: Vec<_> = targets.into_iter().map(CacheTarget::into_core).collect();
        self.inner.warm_sst(sst_id, &targets).await?;
        Ok(())
    }

    /// Best-effort eviction of block-cache entries for one SST.
    ///
    /// If no block cache is configured, returns `Ok(())`.
    pub async fn evict_cached_sst(&self, sst_id: SsTableId) -> Result<(), Error> {
        let sst_id = sst_id.into_core()?;
        self.inner.evict_cached_sst(sst_id).await?;
        Ok(())
    }
}
