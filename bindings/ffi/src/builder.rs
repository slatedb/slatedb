use std::sync::Arc;

use parking_lot::Mutex;
use uuid::Uuid;

use crate::config::{FfiReaderOptions, FfiSstBlockSize};
use crate::db::FfiDb;
use crate::db_reader::FfiDbReader;
use crate::error::FfiSlatedbError;
use crate::merge_operator::{adapt_merge_operator, FfiMergeOperator};
use crate::object_store::FfiObjectStore;
use crate::settings::FfiSettings;
use crate::validation::builder_consumed;

#[derive(uniffi::Object)]
pub struct FfiDbBuilder {
    builder: Mutex<Option<slatedb::DbBuilder<String>>>,
}

impl FfiDbBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(slatedb::DbBuilder<String>) -> slatedb::DbBuilder<String>,
    ) -> Result<(), FfiSlatedbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or_else(builder_consumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::DbBuilder<String>, FfiSlatedbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or_else(builder_consumed)
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

    pub fn with_settings(&self, settings: Arc<FfiSettings>) -> Result<(), FfiSlatedbError> {
        let settings = settings.inner();
        self.update_builder(|builder| builder.with_settings(settings))
    }

    pub fn with_wal_object_store(
        &self,
        wal_object_store: Arc<FfiObjectStore>,
    ) -> Result<(), FfiSlatedbError> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
    }

    pub fn with_db_cache_disabled(&self) -> Result<(), FfiSlatedbError> {
        self.update_builder(slatedb::DbBuilder::with_db_cache_disabled)
    }

    pub fn with_seed(&self, seed: u64) -> Result<(), FfiSlatedbError> {
        self.update_builder(|builder| builder.with_seed(seed))
    }

    pub fn with_sst_block_size(
        &self,
        sst_block_size: FfiSstBlockSize,
    ) -> Result<(), FfiSlatedbError> {
        let sst_block_size = sst_block_size.into_core();
        self.update_builder(|builder| builder.with_sst_block_size(sst_block_size))
    }

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
    pub async fn build(&self) -> Result<Arc<FfiDb>, FfiSlatedbError> {
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
    ) -> Result<(), FfiSlatedbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or_else(builder_consumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::DbReaderBuilder<String>, FfiSlatedbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or_else(builder_consumed)
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

    pub fn with_checkpoint_id(&self, checkpoint_id: String) -> Result<(), FfiSlatedbError> {
        let checkpoint_id =
            Uuid::parse_str(&checkpoint_id).map_err(|err| FfiSlatedbError::Invalid {
                message: format!("invalid checkpoint_id UUID: {err}"),
            })?;
        self.update_builder(|builder| builder.with_checkpoint_id(checkpoint_id))
    }

    pub fn with_options(&self, options: FfiReaderOptions) -> Result<(), FfiSlatedbError> {
        let options = options.into_core();
        self.update_builder(|builder| builder.with_options(options))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbReaderBuilder {
    pub async fn build(&self) -> Result<Arc<FfiDbReader>, FfiSlatedbError> {
        let builder = self.take_builder()?;
        let reader = builder.build().await?;
        Ok(Arc::new(FfiDbReader::new(reader)))
    }
}
