//! Read-only database reader and builder wrappers.

use std::sync::{Arc, Mutex as StdMutex};

use slatedb::{DbReader as CoreDbReader, DbReaderBuilder as CoreDbReaderBuilder};
use uuid::Uuid;

use crate::config::{DbKeyRange, DbReadOptions, DbReaderOptions, DbScanOptions};
use crate::error::SlatedbError;
use crate::iterator::DbIterator;
use crate::object_store::ObjectStore;
use crate::validation::builder_consumed;

/// Builder used to configure and open a [`DbReader`].
#[derive(uniffi::Object)]
pub struct DbReaderBuilder {
    builder: StdMutex<Option<CoreDbReaderBuilder<String>>>,
}

/// A read-only database reader.
#[derive(uniffi::Object)]
pub struct DbReader {
    inner: CoreDbReader,
}

impl DbReaderBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(CoreDbReaderBuilder<String>) -> CoreDbReaderBuilder<String>,
    ) -> Result<(), SlatedbError> {
        let mut guard = self.builder.lock().map_err(|_| SlatedbError::Internal {
            message: "reader builder mutex poisoned".to_owned(),
        })?;
        let builder = guard.take().ok_or_else(builder_consumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<CoreDbReaderBuilder<String>, SlatedbError> {
        let mut guard = self.builder.lock().map_err(|_| SlatedbError::Internal {
            message: "reader builder mutex poisoned".to_owned(),
        })?;
        guard.take().ok_or_else(builder_consumed)
    }
}

impl DbReader {
    pub(crate) fn new(inner: CoreDbReader) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl DbReaderBuilder {
    /// Create a new builder for a read-only database reader.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: StdMutex::new(Some(CoreDbReader::builder(
                path,
                object_store.inner.clone(),
            ))),
        })
    }

    /// Set the checkpoint UUID for the reader and validate it immediately.
    pub fn with_checkpoint_id(&self, checkpoint_id: String) -> Result<(), SlatedbError> {
        let checkpoint_id =
            Uuid::parse_str(&checkpoint_id).map_err(|err| SlatedbError::Invalid {
                message: format!("invalid checkpoint_id UUID: {err}"),
            })?;
        self.update_builder(|builder| builder.with_checkpoint_id(checkpoint_id))
    }

    /// Set reader options.
    pub fn with_options(&self, options: DbReaderOptions) -> Result<(), SlatedbError> {
        let options = options.into_core();
        self.update_builder(|builder| builder.with_options(options))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbReaderBuilder {
    /// Build the configured database reader.
    pub async fn build(&self) -> Result<Arc<DbReader>, SlatedbError> {
        let builder = self.take_builder()?;
        let reader = builder.build().await?;
        Ok(Arc::new(DbReader::new(reader)))
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
        options: DbReadOptions,
    ) -> Result<Option<Vec<u8>>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    /// Scan a key range using default scan options.
    pub async fn scan(&self, range: DbKeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    /// Scan a key range using custom scan options.
    pub async fn scan_with_options(
        &self,
        range: DbKeyRange,
        options: DbScanOptions,
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
        options: DbScanOptions,
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
