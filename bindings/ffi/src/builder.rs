use std::sync::{Arc, Mutex as StdMutex};

use serde_json::from_str;
use slatedb::bytes::Bytes;
use slatedb::{
    Db as CoreDb, MergeOperator as CoreMergeOperatorTrait,
    MergeOperatorError as CoreMergeOperatorError,
};

use crate::config::SstBlockSize;
use crate::db::Db;
use crate::error::SlatedbError;
use crate::object_store::ObjectStore;
use crate::validation::builder_consumed;

#[uniffi::export(callback_interface)]
pub trait MergeOperator: Send + Sync {
    fn merge(&self, key: Vec<u8>, existing_value: Option<Vec<u8>>, value: Vec<u8>) -> Vec<u8>;
}

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
        Ok(Bytes::from(self.inner.merge(
            key.to_vec(),
            existing_value.map(|value| value.to_vec()),
            value.to_vec(),
        )))
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

    pub fn with_settings_json(&self, settings_json: String) -> Result<(), SlatedbError> {
        let settings = from_str::<slatedb::Settings>(&settings_json)?;
        self.with_state(|state| {
            state.settings = Some(settings);
            Ok(())
        })
    }

    pub fn with_wal_object_store(
        &self,
        wal_object_store: Arc<ObjectStore>,
    ) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.wal_object_store = Some(wal_object_store);
            Ok(())
        })
    }

    pub fn with_db_cache_disabled(&self) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.db_cache_disabled = true;
            Ok(())
        })
    }

    pub fn with_seed(&self, seed: u64) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.seed = Some(seed);
            Ok(())
        })
    }

    pub fn with_sst_block_size(&self, sst_block_size: SstBlockSize) -> Result<(), SlatedbError> {
        self.with_state(|state| {
            state.sst_block_size = Some(sst_block_size);
            Ok(())
        })
    }

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
