use std::sync::Arc;

use parking_lot::Mutex;
use uuid::Uuid;

use crate::config::{ReaderOptions, SstBlockSize};
use crate::db::Db;
use crate::db_reader::DbReader;
use crate::error::{Error, SlateDbError};
use crate::merge_operator::{adapt_merge_operator, MergeOperator};
use crate::object_store::ObjectStore;
use crate::settings::Settings;

#[derive(uniffi::Object)]
pub struct DbBuilder {
    builder: Mutex<Option<slatedb::DbBuilder<String>>>,
}

impl DbBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(slatedb::DbBuilder<String>) -> slatedb::DbBuilder<String>,
    ) -> Result<(), SlateDbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or(SlateDbError::BuilderConsumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::DbBuilder<String>, SlateDbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or(SlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl DbBuilder {
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(slatedb::Db::builder(path, object_store.inner.clone()))),
        })
    }

    pub fn with_settings(&self, settings: Arc<Settings>) -> Result<(), Error> {
        let settings = settings.inner();
        self.update_builder(|builder| builder.with_settings(settings))
            .map_err(Into::into)
    }

    pub fn with_wal_object_store(&self, wal_object_store: Arc<ObjectStore>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
            .map_err(Into::into)
    }

    pub fn with_db_cache_disabled(&self) -> Result<(), Error> {
        self.update_builder(slatedb::DbBuilder::with_db_cache_disabled)
            .map_err(Into::into)
    }

    pub fn with_seed(&self, seed: u64) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_seed(seed))
            .map_err(Into::into)
    }

    pub fn with_sst_block_size(&self, sst_block_size: SstBlockSize) -> Result<(), Error> {
        let sst_block_size = sst_block_size.into_core();
        self.update_builder(|builder| builder.with_sst_block_size(sst_block_size))
            .map_err(Into::into)
    }

    pub fn with_merge_operator(
        &self,
        merge_operator: Arc<dyn MergeOperator>,
    ) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_merge_operator(adapt_merge_operator(merge_operator))
        })
        .map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbBuilder {
    pub async fn build(&self) -> Result<Arc<Db>, Error> {
        let builder = self.take_builder()?;
        let db = builder.build().await?;
        Ok(Arc::new(Db::new(db)))
    }
}

#[derive(uniffi::Object)]
pub struct DbReaderBuilder {
    builder: Mutex<Option<slatedb::DbReaderBuilder<String>>>,
}

impl DbReaderBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(slatedb::DbReaderBuilder<String>) -> slatedb::DbReaderBuilder<String>,
    ) -> Result<(), SlateDbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or(SlateDbError::BuilderConsumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::DbReaderBuilder<String>, SlateDbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or(SlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl DbReaderBuilder {
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(slatedb::DbReader::builder(
                path,
                object_store.inner.clone(),
            ))),
        })
    }

    pub fn with_checkpoint_id(&self, checkpoint_id: String) -> Result<(), Error> {
        let checkpoint_id = Uuid::parse_str(&checkpoint_id)
            .map_err(|source| SlateDbError::InvalidCheckpointId { source })?;
        self.update_builder(|builder| builder.with_checkpoint_id(checkpoint_id))
            .map_err(Into::into)
    }

    pub fn with_wal_object_store(&self, wal_object_store: Arc<ObjectStore>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
            .map_err(Into::into)
    }

    pub fn with_merge_operator(
        &self,
        merge_operator: Arc<dyn MergeOperator>,
    ) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_merge_operator(adapt_merge_operator(merge_operator))
        })
        .map_err(Into::into)
    }

    pub fn with_options(&self, options: ReaderOptions) -> Result<(), Error> {
        let options = options.into_core();
        self.update_builder(|builder| builder.with_options(options))
            .map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbReaderBuilder {
    pub async fn build(&self) -> Result<Arc<DbReader>, Error> {
        let builder = self.take_builder()?;
        let reader = builder.build().await?;
        Ok(Arc::new(DbReader::new(reader)))
    }
}
