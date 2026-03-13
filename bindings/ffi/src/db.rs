//! Database and snapshot handles exposed by the FFI wrapper.

use std::collections::HashMap;
use std::sync::Arc;

use slatedb::{Db as CoreDb, DbSnapshot as CoreDbSnapshot};

use crate::config::{
    FfiFlushOptions, FfiIsolationLevel, FfiKeyRange, FfiKeyValue, FfiMergeOptions, FfiPutOptions,
    FfiReadOptions, FfiScanOptions, FfiWriteHandle, FfiWriteOperation, FfiWriteOptions,
};
use crate::error::FfiSlatedbError;
use crate::iterator::FfiDbIterator;
use crate::transaction::FfiDbTransaction;
use crate::validation::{build_write_batch, validate_key, validate_key_value};
use crate::write_batch::FfiWriteBatch;

/// Handle to an open SlateDB database.
///
/// Instances of this type are created by [`crate::FfiDbBuilder::build`].
#[derive(uniffi::Object)]
pub struct FfiDb {
    inner: CoreDb,
}

/// A stable point-in-time view of a database.
#[derive(uniffi::Object)]
pub struct FfiDbSnapshot {
    inner: Arc<CoreDbSnapshot>,
}

impl FfiDb {
    pub(crate) fn new(inner: CoreDb) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl FfiDb {
    /// Check whether the database is still open.
    ///
    /// ## Returns
    /// - `Result<(), FfiSlatedbError>`: `Ok(())` if the database is open.
    pub fn status(&self) -> Result<(), FfiSlatedbError> {
        self.inner.status().map_err(Into::into)
    }

    /// Snapshot the current database metrics registry.
    pub fn metrics(&self) -> Result<HashMap<String, i64>, FfiSlatedbError> {
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
impl FfiDb {
    /// Close the database.
    #[uniffi::method(name = "shutdown")]
    pub async fn close(&self) -> Result<(), FfiSlatedbError> {
        self.inner.close().await.map_err(Into::into)
    }

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

    /// Get the full row metadata for a key using default read options.
    pub async fn get_key_value(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        Ok(self
            .inner
            .get_key_value(key)
            .await?
            .map(FfiKeyValue::from_core))
    }

    /// Get the full row metadata for a key using custom read options.
    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(FfiKeyValue::from_core))
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

    /// Put a value for a key using default options.
    ///
    /// ## Errors
    /// - `FfiSlatedbError::Invalid`: if the key is empty or exceeds SlateDB limits.
    pub async fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &value)?;
        Ok(FfiWriteHandle::from_core(self.inner.put(key, value).await?))
    }

    /// Put a value for a key using custom put and write options.
    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        put_options: FfiPutOptions,
        write_options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &value)?;
        let put_options = put_options.into_core();
        let write_options = write_options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner
                .put_with_options(key, value, &put_options, &write_options)
                .await?,
        ))
    }

    /// Delete a key using default write options.
    pub async fn delete(&self, key: Vec<u8>) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key(&key)?;
        Ok(FfiWriteHandle::from_core(self.inner.delete(key).await?))
    }

    /// Delete a key using custom write options.
    pub async fn delete_with_options(
        &self,
        key: Vec<u8>,
        options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key(&key)?;
        let options = options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner.delete_with_options(key, &options).await?,
        ))
    }

    /// Merge an operand into a key using default options.
    pub async fn merge(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &operand)?;
        Ok(FfiWriteHandle::from_core(
            self.inner.merge(key, operand).await?,
        ))
    }

    /// Merge an operand into a key using custom merge and write options.
    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        operand: Vec<u8>,
        merge_options: FfiMergeOptions,
        write_options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        validate_key_value(&key, &operand)?;
        let merge_options = merge_options.into_core();
        let write_options = write_options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner
                .merge_with_options(key, operand, &merge_options, &write_options)
                .await?,
        ))
    }

    /// Apply a batch of operations atomically using default write options.
    pub async fn write(
        &self,
        operations: Vec<FfiWriteOperation>,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        let batch = build_write_batch(operations)?;
        Ok(FfiWriteHandle::from_core(self.inner.write(batch).await?))
    }

    /// Apply a batch of operations atomically using custom write options.
    pub async fn write_with_options(
        &self,
        operations: Vec<FfiWriteOperation>,
        options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        let batch = build_write_batch(operations)?;
        let options = options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner.write_with_options(batch, &options).await?,
        ))
    }

    /// Apply an existing write batch atomically using default write options.
    pub async fn write_batch(
        &self,
        batch: Arc<FfiWriteBatch>,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        let batch = batch.take_for_write()?;
        Ok(FfiWriteHandle::from_core(self.inner.write(batch).await?))
    }

    /// Apply an existing write batch atomically using custom write options.
    pub async fn write_batch_with_options(
        &self,
        batch: Arc<FfiWriteBatch>,
        options: FfiWriteOptions,
    ) -> Result<FfiWriteHandle, FfiSlatedbError> {
        let batch = batch.take_for_write()?;
        let options = options.into_core();
        Ok(FfiWriteHandle::from_core(
            self.inner.write_with_options(batch, &options).await?,
        ))
    }

    /// Flush in-memory state using the database defaults.
    pub async fn flush(&self) -> Result<(), FfiSlatedbError> {
        self.inner.flush().await.map_err(Into::into)
    }

    /// Flush in-memory state using explicit flush options.
    pub async fn flush_with_options(
        &self,
        options: FfiFlushOptions,
    ) -> Result<(), FfiSlatedbError> {
        self.inner
            .flush_with_options(options.into_core())
            .await
            .map_err(Into::into)
    }

    /// Create a point-in-time snapshot of the database.
    pub async fn snapshot(&self) -> Result<Arc<FfiDbSnapshot>, FfiSlatedbError> {
        Ok(Arc::new(FfiDbSnapshot {
            inner: self.inner.snapshot().await?,
        }))
    }

    /// Begin a new transaction at the requested isolation level.
    pub async fn begin(
        &self,
        isolation_level: FfiIsolationLevel,
    ) -> Result<Arc<FfiDbTransaction>, FfiSlatedbError> {
        let tx = self.inner.begin(isolation_level.into_core()).await?;
        Ok(Arc::new(FfiDbTransaction::new(tx)))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbSnapshot {
    /// Get the value for a key from the snapshot using default read options.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, FfiSlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

    /// Get the value for a key from the snapshot using custom read options.
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

    /// Get the full row metadata for a key from the snapshot using default read options.
    pub async fn get_key_value(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        Ok(self
            .inner
            .get_key_value(key)
            .await?
            .map(FfiKeyValue::from_core))
    }

    /// Get the full row metadata for a key from the snapshot using custom read options.
    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: FfiReadOptions,
    ) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(FfiKeyValue::from_core))
    }

    /// Scan a key range from the snapshot using default scan options.
    pub async fn scan(&self, range: FfiKeyRange) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    /// Scan a key range from the snapshot using custom scan options.
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

    /// Scan all keys with the provided prefix from the snapshot.
    pub async fn scan_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> Result<Arc<FfiDbIterator>, FfiSlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(FfiDbIterator::new(iter)))
    }

    /// Scan all keys with the provided prefix from the snapshot using custom scan options.
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
}
