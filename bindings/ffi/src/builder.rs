use std::sync::Arc;

use parking_lot::Mutex;
use uuid::Uuid;

use crate::config::{FfiReaderOptions, FfiSstBlockSize};
use crate::db::FfiDb;
use crate::db_reader::FfiDbReader;
use crate::error::{FfiError, FfiSlateDbError};
use crate::merge_operator::{adapt_merge_operator, FfiMergeOperator};
use crate::object_store::FfiObjectStore;
use crate::settings::FfiSettings;

#[derive(uniffi::Object)]
pub struct FfiDbBuilder {
    builder: Mutex<Option<slatedb::DbBuilder<String>>>,
}

impl FfiDbBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(slatedb::DbBuilder<String>) -> slatedb::DbBuilder<String>,
    ) -> Result<(), FfiSlateDbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or(FfiSlateDbError::BuilderConsumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::DbBuilder<String>, FfiSlateDbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or(FfiSlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl FfiDbBuilder {
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<FfiObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(slatedb::Db::builder(path, object_store.inner.clone()))),
        })
    }

    pub fn with_settings(&self, settings: Arc<FfiSettings>) -> Result<(), FfiError> {
        let settings = settings.inner();
        self.update_builder(|builder| builder.with_settings(settings))
            .map_err(Into::into)
    }

    pub fn with_wal_object_store(
        &self,
        wal_object_store: Arc<FfiObjectStore>,
    ) -> Result<(), FfiError> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
            .map_err(Into::into)
    }

    pub fn with_db_cache_disabled(&self) -> Result<(), FfiError> {
        self.update_builder(slatedb::DbBuilder::with_db_cache_disabled)
            .map_err(Into::into)
    }

    pub fn with_seed(&self, seed: u64) -> Result<(), FfiError> {
        self.update_builder(|builder| builder.with_seed(seed))
            .map_err(Into::into)
    }

    pub fn with_sst_block_size(&self, sst_block_size: FfiSstBlockSize) -> Result<(), FfiError> {
        let sst_block_size = sst_block_size.into_core();
        self.update_builder(|builder| builder.with_sst_block_size(sst_block_size))
            .map_err(Into::into)
    }

    pub fn with_merge_operator(
        &self,
        merge_operator: Box<dyn FfiMergeOperator>,
    ) -> Result<(), FfiError> {
        self.update_builder(|builder| {
            builder.with_merge_operator(adapt_merge_operator(merge_operator))
        })
        .map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbBuilder {
    pub async fn build(&self) -> Result<Arc<FfiDb>, FfiError> {
        let builder = self.take_builder()?;
        let db = builder.build().await?;
        Ok(Arc::new(FfiDb::new(db)))
    }
}

#[derive(uniffi::Object)]
pub struct FfiDbReaderBuilder {
    builder: Mutex<Option<slatedb::DbReaderBuilder<String>>>,
}

impl FfiDbReaderBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(slatedb::DbReaderBuilder<String>) -> slatedb::DbReaderBuilder<String>,
    ) -> Result<(), FfiSlateDbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or(FfiSlateDbError::BuilderConsumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::DbReaderBuilder<String>, FfiSlateDbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or(FfiSlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl FfiDbReaderBuilder {
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<FfiObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(slatedb::DbReader::builder(
                path,
                object_store.inner.clone(),
            ))),
        })
    }

    pub fn with_checkpoint_id(&self, checkpoint_id: String) -> Result<(), FfiError> {
        let checkpoint_id = Uuid::parse_str(&checkpoint_id)
            .map_err(|source| FfiSlateDbError::InvalidCheckpointId { source })?;
        self.update_builder(|builder| builder.with_checkpoint_id(checkpoint_id))
            .map_err(Into::into)
    }

    pub fn with_options(&self, options: FfiReaderOptions) -> Result<(), FfiError> {
        let options = options.into_core();
        self.update_builder(|builder| builder.with_options(options))
            .map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbReaderBuilder {
    pub async fn build(&self) -> Result<Arc<FfiDbReader>, FfiError> {
        let builder = self.take_builder()?;
        let reader = builder.build().await?;
        Ok(Arc::new(FfiDbReader::new(reader)))
    }
}
