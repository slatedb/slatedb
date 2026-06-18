use std::sync::Arc;

use crate::admin::Admin;
use crate::config::{ReaderOptions, SstBlockSize};
use crate::db::Db;
use crate::db_cache::DbCache;
use crate::db_reader::DbReader;
use crate::error::{Error, SlateDbError};
use crate::filter_policy::{
    adapt_prefix_extractor, collect_filter_policies, FilterPolicy, PrefixExtractor,
};
use crate::merge_operator::{adapt_merge_operator, MergeOperator};
use crate::metrics::adapt_metrics_recorder;
use crate::object_store::ObjectStore;
use crate::runtime;
use crate::settings::Settings;
use crate::types::{CloneSourceSpec, KeyRange};
use crate::MetricsRecorder;
use parking_lot::Mutex;
use uuid::Uuid;

/// Builder for opening a writable [`crate::Db`].
///
/// Builders are single-use: calling [`DbBuilder::build`] consumes the builder.
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
    /// Creates a new database builder for `path` in `object_store`.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(slatedb::Db::builder(path, object_store.inner.clone()))),
        })
    }

    /// Applies a [`crate::Settings`] object to the builder.
    pub fn with_settings(&self, settings: Arc<Settings>) -> Result<(), Error> {
        let settings = settings.inner();
        self.update_builder(|builder| builder.with_settings(settings))
            .map_err(Into::into)
    }

    /// Uses a separate object store for WAL files.
    pub fn with_wal_object_store(&self, wal_object_store: Arc<ObjectStore>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
            .map_err(Into::into)
    }

    /// Disables the SST block and metadata cache.
    pub fn with_db_cache_disabled(&self) -> Result<(), Error> {
        self.update_builder(slatedb::DbBuilder::with_db_cache_disabled)
            .map_err(Into::into)
    }

    /// Sets DB cache.
    pub fn with_db_cache(&self, db_cache: Arc<DbCache>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_db_cache(db_cache.inner.clone()))
            .map_err(Into::into)
    }

    /// Sets the seed used for SlateDB's internal random number generation.
    pub fn with_seed(&self, seed: u64) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_seed(seed))
            .map_err(Into::into)
    }

    /// Sets the SSTable block size used for newly written tables.
    pub fn with_sst_block_size(&self, sst_block_size: SstBlockSize) -> Result<(), Error> {
        let sst_block_size = sst_block_size.into();
        self.update_builder(|builder| builder.with_sst_block_size(sst_block_size))
            .map_err(Into::into)
    }

    /// Installs an application-defined merge operator.
    pub fn with_merge_operator(&self, merge_operator: Arc<dyn MergeOperator>) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_merge_operator(adapt_merge_operator(merge_operator))
        })
        .map_err(Into::into)
    }

    /// Installs an application-defined metrics recorder.
    pub fn with_metrics_recorder(
        &self,
        metrics_recorder: Arc<dyn MetricsRecorder>,
    ) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_metrics_recorder(adapt_metrics_recorder(metrics_recorder))
        })
        .map_err(Into::into)
    }

    /// Sets the filter policies used for SST filter construction and evaluation.
    ///
    /// Pass an empty vec to disable filters entirely. When unset, the default
    /// is a single bloom filter with 10 bits per key.
    pub fn with_filter_policies(&self, policies: Vec<Arc<FilterPolicy>>) -> Result<(), Error> {
        let policies = collect_filter_policies(policies);
        self.update_builder(|builder| builder.with_filter_policies(policies))
            .map_err(Into::into)
    }

    /// Sets the segment extractor (RFC-0024). When configured, every write is
    /// routed through the extractor and the database tracks per-segment LSM
    /// state. The extractor must be configured at database creation time and
    /// cannot be changed thereafter.
    pub fn with_segment_extractor(&self, extractor: Arc<dyn PrefixExtractor>) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_segment_extractor(adapt_prefix_extractor(extractor))
        })
        .map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbBuilder {
    /// Opens the database and consumes this builder.
    pub async fn build(&self) -> Result<Arc<Db>, Error> {
        let builder = self.take_builder()?;
        let db = runtime::enter(async move { builder.build().await.map_err(Error::from) }).await?;
        Ok(Arc::new(Db::new(db)))
    }
}

/// Builder for opening a read-only [`crate::DbReader`].
///
/// Builders are single-use: calling [`DbReaderBuilder::build`] consumes the builder.
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
    /// Creates a new reader builder for `path` in `object_store`.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(slatedb::DbReader::builder(
                path,
                object_store.inner.clone(),
            ))),
        })
    }

    /// Pins the reader to an existing checkpoint UUID string.
    pub fn with_checkpoint_id(&self, checkpoint_id: String) -> Result<(), Error> {
        let checkpoint_id = Uuid::parse_str(&checkpoint_id)
            .map_err(|source| SlateDbError::InvalidCheckpointId { source })?;
        self.update_builder(|builder| builder.with_checkpoint_id(checkpoint_id))
            .map_err(Into::into)
    }

    /// Uses a separate object store for WAL files.
    pub fn with_wal_object_store(&self, wal_object_store: Arc<ObjectStore>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
            .map_err(Into::into)
    }

    /// Installs an application-defined merge operator used while reading merge rows.
    pub fn with_merge_operator(&self, merge_operator: Arc<dyn MergeOperator>) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_merge_operator(adapt_merge_operator(merge_operator))
        })
        .map_err(Into::into)
    }

    /// Applies custom reader options.
    pub fn with_options(&self, options: ReaderOptions) -> Result<(), Error> {
        let options = options.into();
        self.update_builder(|builder| builder.with_options(options))
            .map_err(Into::into)
    }

    /// Installs an application-defined metrics recorder.
    pub fn with_metrics_recorder(
        &self,
        metrics_recorder: Arc<dyn MetricsRecorder>,
    ) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_metrics_recorder(adapt_metrics_recorder(metrics_recorder))
        })
        .map_err(Into::into)
    }

    /// Sets the filter policies used when decoding SST filter blocks.
    ///
    /// Must match (or be a superset of) the writer's policies so SST filter
    /// sub-blocks can be decoded; unrecognized policy names are silently
    /// skipped. Defaults to a single bloom filter with 10 bits per key.
    pub fn with_filter_policies(&self, policies: Vec<Arc<FilterPolicy>>) -> Result<(), Error> {
        let policies = collect_filter_policies(policies);
        self.update_builder(|builder| builder.with_filter_policies(policies))
            .map_err(Into::into)
    }

    /// Sets the segment extractor (RFC-0024). A reader opening a segmented
    /// database must configure an extractor matching the one the database
    /// was created with.
    pub fn with_segment_extractor(&self, extractor: Arc<dyn PrefixExtractor>) -> Result<(), Error> {
        self.update_builder(|builder| {
            builder.with_segment_extractor(adapt_prefix_extractor(extractor))
        })
        .map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbReaderBuilder {
    /// Opens the reader and consumes this builder.
    pub async fn build(&self) -> Result<Arc<DbReader>, Error> {
        let builder = self.take_builder()?;
        let reader =
            runtime::enter(async move { builder.build().await.map_err(Error::from) }).await?;
        Ok(Arc::new(DbReader::new(reader)))
    }
}

/// Builder for opening an administrative [`crate::Admin`] handle.
///
/// Builders are single-use: calling [`AdminBuilder::build`] consumes the builder.
#[derive(uniffi::Object)]
pub struct AdminBuilder {
    builder: Mutex<Option<slatedb::admin::AdminBuilder<String>>>,
}

impl AdminBuilder {
    fn update_builder(
        &self,
        update: impl FnOnce(
            slatedb::admin::AdminBuilder<String>,
        ) -> slatedb::admin::AdminBuilder<String>,
    ) -> Result<(), SlateDbError> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or(SlateDbError::BuilderConsumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::admin::AdminBuilder<String>, SlateDbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or(SlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl AdminBuilder {
    /// Creates a new admin builder for `path` in `object_store`.
    #[uniffi::constructor]
    pub fn new(path: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(slatedb::admin::Admin::builder(
                path,
                object_store.inner.clone(),
            ))),
        })
    }

    /// Uses a separate object store for WAL-backed administrative operations.
    pub fn with_wal_object_store(&self, wal_object_store: Arc<ObjectStore>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
            .map_err(Into::into)
    }

    /// Sets the seed used for SlateDB's internal random number generation.
    pub fn with_seed(&self, seed: u64) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_seed(seed))
            .map_err(Into::into)
    }

    /// Builds the admin handle and consumes this builder.
    pub fn build(&self) -> Result<Arc<Admin>, Error> {
        let builder = self.take_builder()?;
        let admin = builder.build();
        Ok(Arc::new(Admin { inner: admin }))
    }
}

#[derive(uniffi::Object)]
pub struct CloneBuilder {
    builder: Mutex<Option<slatedb::admin::CloneBuilder>>,
}

impl CloneBuilder {
    pub(crate) fn new(inner: slatedb::admin::CloneBuilder) -> Arc<Self> {
        Arc::new(Self {
            builder: Mutex::new(Some(inner)),
        })
    }

    fn update_builder(
        &self,
        update: impl FnOnce(slatedb::admin::CloneBuilder) -> slatedb::admin::CloneBuilder,
    ) -> Result<(), Error> {
        let mut guard = self.builder.lock();
        let builder = guard.take().ok_or(SlateDbError::BuilderConsumed)?;
        *guard = Some(update(builder));
        Ok(())
    }

    fn take_builder(&self) -> Result<slatedb::admin::CloneBuilder, SlateDbError> {
        let mut guard = self.builder.lock();
        guard.take().ok_or(SlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl CloneBuilder {
    pub fn with_clone_path(&self, clone_path: String) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_clone_path(clone_path.into()))
    }

    pub fn with_source(&self, source: CloneSourceSpec) -> Result<(), Error> {
        let slatedb_source = source.try_into()?;
        self.update_builder(|builder| builder.with_source(slatedb_source))
    }

    pub fn with_object_store(&self, object_store: Arc<ObjectStore>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_object_store(object_store.inner.clone()))
    }

    pub fn with_wal_object_store(&self, wal_object_store: Arc<ObjectStore>) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_wal_object_store(wal_object_store.inner.clone()))
    }

    pub fn with_projection_range(&self, projection_range: Option<KeyRange>) -> Result<(), Error> {
        let projection_range_bounds = projection_range
            .map(|key_range| key_range.into_range_bounds())
            .transpose()?;
        self.update_builder(|builder| builder.with_projection_range(projection_range_bounds))
    }

    pub fn with_seed(&self, seed: u64) -> Result<(), Error> {
        self.update_builder(|builder| builder.with_seed(seed))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl CloneBuilder {
    /// Runs the clone operation and consumes this builder.
    pub async fn build(&self) -> Result<(), Error> {
        let builder = self.take_builder()?;
        builder.build().await.map_err(Into::into)
    }
}
