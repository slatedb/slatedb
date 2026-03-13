//! Database builder and merge-operator interfaces.

use std::sync::{Arc, Mutex as StdMutex};

use serde_json::from_str;
use slatedb::bytes::Bytes;
use slatedb::{
    Db as CoreDb, MergeOperator as CoreMergeOperatorTrait,
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
    /// - `value`: the new merge operand.
    ///
    /// ## Returns
    /// - `Result<Vec<u8>, MergeOperatorCallbackError>`: the merged value that
    ///   should become visible for the key.
    fn merge(
        &self,
        key: Vec<u8>,
        existing_value: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<Vec<u8>, MergeOperatorCallbackError>;
}

/// Builder used to configure and open a [`Db`].
#[derive(uniffi::Object)]
pub struct DbBuilder {
    state: StdMutex<Option<BuilderState>>,
}

struct BuilderState {
    path: String,
    object_store: Arc<ObjectStore>,
    wal_object_store: Option<Arc<ObjectStore>>,
    settings: Option<slatedb::Settings>,
    db_cache_disabled: bool,
    seed: Option<u64>,
    sst_block_size: Option<SstBlockSize>,
    merge_operator: Option<Arc<dyn MergeOperator>>,
}

struct MergeOperatorAdapter {
    inner: Arc<dyn MergeOperator>,
}

impl CoreMergeOperatorTrait for MergeOperatorAdapter {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, CoreMergeOperatorError> {
        self.inner
            .merge(
                key.to_vec(),
                existing_value.map(|value| value.to_vec()),
                value.to_vec(),
            )
            .map(Bytes::from)
            .map_err(|error| CoreMergeOperatorError::Callback {
                message: error.to_string(),
            })
    }
}

impl DbBuilder {
    fn with_state<T>(
        &self,
        update: impl FnOnce(&mut BuilderState) -> Result<T, SlatedbError>,
    ) -> Result<T, SlatedbError> {
        let mut guard = self.state.lock().map_err(|_| SlatedbError::Internal {
            message: "builder mutex poisoned".to_owned(),
        })?;
        let state = guard.as_mut().ok_or_else(builder_consumed)?;
        update(state)
    }

    fn take_state(&self) -> Result<BuilderState, SlatedbError> {
        let mut guard = self.state.lock().map_err(|_| SlatedbError::Internal {
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
            state: StdMutex::new(Some(BuilderState {
                path,
                object_store,
                wal_object_store: None,
                settings: None,
                db_cache_disabled: false,
                seed: None,
                sst_block_size: None,
                merge_operator: None,
            })),
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
        self.with_state(|state| {
            state.settings = Some(settings);
            Ok(())
        })
    }

    /// Configure a separate object store for WAL data.
    ///
    /// ## Arguments
    /// - `wal_object_store`: the object store to use for WAL files.
    pub fn with_wal_object_store(
        &self,
        wal_object_store: Arc<ObjectStore>,
    ) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.wal_object_store = Some(wal_object_store);
            Ok(())
        })
    }

    /// Disable the database-level cache created by the builder.
    pub fn with_db_cache_disabled(&self) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.db_cache_disabled = true;
            Ok(())
        })
    }

    /// Set the random seed used by the database.
    ///
    /// ## Arguments
    /// - `seed`: the seed to use when constructing the database.
    pub fn with_seed(&self, seed: u64) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.seed = Some(seed);
            Ok(())
        })
    }

    /// Override the SST block size used for new SSTs.
    ///
    /// ## Arguments
    /// - `sst_block_size`: the block size to use.
    pub fn with_sst_block_size(&self, sst_block_size: SstBlockSize) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.sst_block_size = Some(sst_block_size);
            Ok(())
        })
    }

    /// Configure the merge operator used for merge reads and writes.
    ///
    /// ## Arguments
    /// - `merge_operator`: the callback implementation to use.
    pub fn with_merge_operator(
        &self,
        merge_operator: Box<dyn MergeOperator>,
    ) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.merge_operator = Some(merge_operator.into());
            Ok(())
        })
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
        let state = self.take_state()?;

        let mut builder = CoreDb::builder(state.path, state.object_store.inner.clone());

        if let Some(settings) = state.settings {
            builder = builder.with_settings(settings);
        }
        if let Some(wal_object_store) = state.wal_object_store {
            builder = builder.with_wal_object_store(wal_object_store.inner.clone());
        }
        if state.db_cache_disabled {
            builder = builder.with_db_cache_disabled();
        }
        if let Some(seed) = state.seed {
            builder = builder.with_seed(seed);
        }
        if let Some(sst_block_size) = state.sst_block_size {
            builder = builder.with_sst_block_size(sst_block_size.into_core());
        }
        if let Some(merge_operator) = state.merge_operator {
            let merge_operator = Arc::new(MergeOperatorAdapter {
                inner: merge_operator,
            });
            builder = builder.with_merge_operator(merge_operator);
        }

        let db = builder.build().await?;
        Ok(Arc::new(Db::new(db)))
    }
}
