//! Database and reader builder interfaces.

use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::from_str;
use slatedb::{
    Db as CoreDb, DbBuilder as CoreDbBuilder, DbReader as CoreDbReader,
    DbReaderBuilder as CoreDbReaderBuilder,
};
use uuid::Uuid;

use crate::config::{FfiReaderOptions, FfiSstBlockSize};
use crate::db::FfiDb;
use crate::db_reader::FfiDbReader;
use crate::error::FfiSlatedbError;
use crate::merge_operator::{adapt_merge_operator, FfiMergeOperator};
use crate::object_store::FfiObjectStore;
use crate::validation::builder_consumed;

/// Builder used to configure and open a [`FfiDb`].
#[derive(uniffi::Object)]
pub struct FfiDbBuilder {
    builder: Mutex<Option<CoreDbBuilder<String>>>,
}

/// Builder used to configure and open a [`FfiDbReader`].
#[derive(uniffi::Object)]
pub struct FfiDbReaderBuilder {
    builder: Mutex<Option<CoreDbReaderBuilder<String>>>,
}

impl FfiDbBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(CoreDbBuilder<String>) -> CoreDbBuilder<String>,
    ) -> Result<(), FfiSlatedbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or_else(builder_consumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<CoreDbBuilder<String>, FfiSlatedbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or_else(builder_consumed)
    }
}

impl FfiDbReaderBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(CoreDbReaderBuilder<String>) -> CoreDbReaderBuilder<String>,
    ) -> Result<(), FfiSlatedbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or_else(builder_consumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<CoreDbReaderBuilder<String>, FfiSlatedbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or_else(builder_consumed)
    }
}

#[uniffi::export]
impl FfiDbBuilder {
    /// Create a new builder for a database.
    ///
    /// ## Arguments
    /// - `path`: the database path within the object store.
    /// - `object_store`: the object store that will back the database.
    ///
    /// ## Returns
    /// - `Arc<FfiDbBuilder>`: a new builder instance.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<FfiObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(CoreDb::builder(path, object_store.inner.clone()))),
        })
    }

    /// Replace the default database settings with a JSON-encoded [`slatedb::Settings`] document.
    ///
    /// ## Arguments
    /// - `settings_json`: the full settings document encoded as JSON.
    ///
    /// ## Errors
    /// - `FfiSlatedbError::Invalid`: if the JSON cannot be parsed.
    pub fn with_settings_json(&self, settings_json: String) -> Result<(), FfiSlatedbError> {
        let settings = from_str::<slatedb::Settings>(&settings_json)?;
        self.update_builder(|builder| builder.with_settings(settings))
    }

    /// Configure a separate object store for WAL data.
    ///
    /// ## Arguments
    /// - `wal_object_store`: the object store to use for WAL files.
    pub fn with_wal_object_store(
        &self,
        wal_object_store: Arc<FfiObjectStore>,
    ) -> Result<(), FfiSlatedbError> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
    }

    /// Disable the database-level cache created by the builder.
    pub fn with_db_cache_disabled(&self) -> Result<(), FfiSlatedbError> {
        self.update_builder(CoreDbBuilder::with_db_cache_disabled)
    }

    /// Set the random seed used by the database.
    ///
    /// ## Arguments
    /// - `seed`: the seed to use when constructing the database.
    pub fn with_seed(&self, seed: u64) -> Result<(), FfiSlatedbError> {
        self.update_builder(|builder| builder.with_seed(seed))
    }

    /// Override the SST block size used for new SSTs.
    ///
    /// ## Arguments
    /// - `sst_block_size`: the block size to use.
    pub fn with_sst_block_size(
        &self,
        sst_block_size: FfiSstBlockSize,
    ) -> Result<(), FfiSlatedbError> {
        let sst_block_size = sst_block_size.into_core();
        self.update_builder(|builder| builder.with_sst_block_size(sst_block_size))
    }

    /// Configure the merge operator used for merge reads and writes.
    ///
    /// ## Arguments
    /// - `merge_operator`: the callback implementation to use.
    pub fn with_merge_operator(
        &self,
        merge_operator: Box<dyn FfiMergeOperator>,
    ) -> Result<(), FfiSlatedbError> {
        self.update_builder(|builder| {
            builder.with_merge_operator(adapt_merge_operator(merge_operator))
        })
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbBuilder {
    /// Open the database using the builder's current configuration.
    ///
    /// This consumes the builder state. Reusing the same builder after a
    /// successful or failed call to `build()` returns an error.
    ///
    /// ## Returns
    /// - `Result<Arc<FfiDb>, FfiSlatedbError>`: the opened database handle.
    ///
    /// ## Errors
    /// - `FfiSlatedbError`: if the builder was already consumed or the database cannot be opened.
    pub async fn build(&self) -> Result<Arc<FfiDb>, FfiSlatedbError> {
        let builder = self.take_builder()?;
        let db = builder.build().await?;
        Ok(Arc::new(FfiDb::new(db)))
    }
}

#[uniffi::export]
impl FfiDbReaderBuilder {
    /// Create a new builder for a read-only database reader.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<FfiObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(CoreDbReader::builder(
                path,
                object_store.inner.clone(),
            ))),
        })
    }

    /// Set the checkpoint UUID for the reader and validate it immediately.
    pub fn with_checkpoint_id(&self, checkpoint_id: String) -> Result<(), FfiSlatedbError> {
        let checkpoint_id =
            Uuid::parse_str(&checkpoint_id).map_err(|err| FfiSlatedbError::Invalid {
                message: format!("invalid checkpoint_id UUID: {err}"),
            })?;
        self.update_builder(|builder| builder.with_checkpoint_id(checkpoint_id))
    }

    /// Set reader options.
    pub fn with_options(&self, options: FfiReaderOptions) -> Result<(), FfiSlatedbError> {
        let options = options.into_core();
        self.update_builder(|builder| builder.with_options(options))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbReaderBuilder {
    /// Build the configured database reader.
    pub async fn build(&self) -> Result<Arc<FfiDbReader>, FfiSlatedbError> {
        let builder = self.take_builder()?;
        let reader = builder.build().await?;
        Ok(Arc::new(FfiDbReader::new(reader)))
    }
}
