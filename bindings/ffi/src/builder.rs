//! Database builder and merge-operator interfaces.

use std::sync::{Arc, Mutex};

use serde_json::from_str;
use slatedb::bytes::Bytes;
use slatedb::{
    Db as CoreDb, DbBuilder as CoreDbBuilder, MergeOperator as CoreMergeOperatorTrait,
    MergeOperatorError as CoreMergeOperatorError,
};

use crate::config::SstBlockSize;
use crate::db::Db;
use crate::error::{MergeOperatorCallbackError, SlatedbError};
use crate::object_store::ObjectStore;
use crate::validation::builder_consumed;

/// Callback interface for SlateDB merge operators.
///
/// Merge operators are configured on [`DbBuilder`] and are used by merge reads
/// and writes to combine an existing value with a new operand.
#[uniffi::export(callback_interface)]
pub trait MergeOperator: Send + Sync {
    /// Merge a new operand into the existing value for a key.
    ///
    /// ## Arguments
    /// - `key`: the key being merged.
    /// - `existing_value`: the current value, if one exists.
    /// - `operand`: the new merge operand.
    ///
    /// ## Returns
    /// - `Result<Vec<u8>, MergeOperatorCallbackError>`: the merged value that
    ///   should become visible for the key.
    fn merge(
        &self,
        key: Vec<u8>,
        existing_value: Option<Vec<u8>>,
        operand: Vec<u8>,
    ) -> Result<Vec<u8>, MergeOperatorCallbackError>;
}

/// Builder used to configure and open a [`Db`].
#[derive(uniffi::Object)]
pub struct DbBuilder {
    builder: Mutex<Option<CoreDbBuilder<String>>>,
}

struct MergeOperatorAdapter {
    inner: Arc<dyn MergeOperator>,
}

impl CoreMergeOperatorTrait for MergeOperatorAdapter {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, CoreMergeOperatorError> {
        self.inner
            .merge(
                key.to_vec(),
                existing_value.map(|value| value.to_vec()),
                operand.to_vec(),
            )
            .map(Bytes::from)
            .map_err(|error| CoreMergeOperatorError::Callback {
                message: error.to_string(),
            })
    }
}

impl DbBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(CoreDbBuilder<String>) -> CoreDbBuilder<String>,
    ) -> Result<(), SlatedbError> {
        let mut guard = self.builder.lock().map_err(|_| SlatedbError::Internal {
            message: "builder mutex poisoned".to_owned(),
        })?;
        let builder = guard.take().ok_or_else(builder_consumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<CoreDbBuilder<String>, SlatedbError> {
        let mut guard = self.builder.lock().map_err(|_| SlatedbError::Internal {
            message: "builder mutex poisoned".to_owned(),
        })?;
        guard.take().ok_or_else(builder_consumed)
    }
}

#[uniffi::export]
impl DbBuilder {
    /// Create a new builder for a database.
    ///
    /// ## Arguments
    /// - `path`: the database path within the object store.
    /// - `object_store`: the object store that will back the database.
    ///
    /// ## Returns
    /// - `Arc<DbBuilder>`: a new builder instance.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
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
    /// - `SlatedbError::Invalid`: if the JSON cannot be parsed.
    pub fn with_settings_json(&self, settings_json: String) -> Result<(), SlatedbError> {
        let settings = from_str::<slatedb::Settings>(&settings_json)?;
        self.update_builder(|builder| builder.with_settings(settings))
    }

    /// Configure a separate object store for WAL data.
    ///
    /// ## Arguments
    /// - `wal_object_store`: the object store to use for WAL files.
    pub fn with_wal_object_store(
        &self,
        wal_object_store: Arc<ObjectStore>,
    ) -> Result<(), SlatedbError> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
    }

    /// Disable the database-level cache created by the builder.
    pub fn with_db_cache_disabled(&self) -> Result<(), SlatedbError> {
        self.update_builder(CoreDbBuilder::with_db_cache_disabled)
    }

    /// Set the random seed used by the database.
    ///
    /// ## Arguments
    /// - `seed`: the seed to use when constructing the database.
    pub fn with_seed(&self, seed: u64) -> Result<(), SlatedbError> {
        self.update_builder(|builder| builder.with_seed(seed))
    }

    /// Override the SST block size used for new SSTs.
    ///
    /// ## Arguments
    /// - `sst_block_size`: the block size to use.
    pub fn with_sst_block_size(&self, sst_block_size: SstBlockSize) -> Result<(), SlatedbError> {
        let sst_block_size = sst_block_size.into_core();
        self.update_builder(|builder| builder.with_sst_block_size(sst_block_size))
    }

    /// Configure the merge operator used for merge reads and writes.
    ///
    /// ## Arguments
    /// - `merge_operator`: the callback implementation to use.
    pub fn with_merge_operator(
        &self,
        merge_operator: Box<dyn MergeOperator>,
    ) -> Result<(), SlatedbError> {
        let merge_operator = Arc::new(MergeOperatorAdapter {
            inner: merge_operator.into(),
        });
        self.update_builder(|builder| builder.with_merge_operator(merge_operator))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbBuilder {
    /// Open the database using the builder's current configuration.
    ///
    /// This consumes the builder state. Reusing the same builder after a
    /// successful or failed call to `build()` returns an error.
    ///
    /// ## Returns
    /// - `Result<Arc<Db>, SlatedbError>`: the opened database handle.
    ///
    /// ## Errors
    /// - `SlatedbError`: if the builder was already consumed or the database cannot be opened.
    pub async fn build(&self) -> Result<Arc<Db>, SlatedbError> {
        let builder = self.take_builder()?;
        let db = builder.build().await?;
        Ok(Arc::new(Db::new(db)))
    }
}
