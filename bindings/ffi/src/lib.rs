use std::ops::Bound;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::from_str;
use slatedb::bytes::Bytes;
use slatedb::config;
use slatedb::object_store::ObjectStore as ObjectStoreTrait;
use slatedb::{
    Db as CoreDb, DbSnapshot as CoreDbSnapshot, DbTransaction as CoreDbTransaction,
    IsolationLevel as CoreIsolationLevel, KeyValue as CoreKeyValue,
    MergeOperator as CoreMergeOperatorTrait, MergeOperatorError as CoreMergeOperatorError,
    SstBlockSize as CoreSstBlockSize, WriteBatch as CoreWriteBatch,
    WriteHandle as CoreWriteHandle,
};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

uniffi::setup_scaffolding!("slatedb");

#[derive(Debug, Error, uniffi::Error)]
pub enum SlatedbError {
    #[error("{message}")]
    Transaction { message: String },

    #[error("{message}")]
    Closed { message: String },

    #[error("{message}")]
    Unavailable { message: String },

    #[error("{message}")]
    Invalid { message: String },

    #[error("{message}")]
    Data { message: String },

    #[error("{message}")]
    Internal { message: String },
}

impl From<slatedb::Error> for SlatedbError {
    fn from(error: slatedb::Error) -> Self {
        let message = error.to_string();
        match error.kind() {
            slatedb::ErrorKind::Transaction => Self::Transaction { message },
            slatedb::ErrorKind::Closed(_) => Self::Closed { message },
            slatedb::ErrorKind::Unavailable => Self::Unavailable { message },
            slatedb::ErrorKind::Invalid => Self::Invalid { message },
            slatedb::ErrorKind::Data => Self::Data { message },
            slatedb::ErrorKind::Internal => Self::Internal { message },
            _ => Self::Internal { message },
        }
    }
}

impl From<serde_json::Error> for SlatedbError {
    fn from(error: serde_json::Error) -> Self {
        SlatedbError::Invalid {
            message: error.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum DurabilityLevel {
    Remote,
    #[default]
    Memory,
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum FlushType {
    MemTable,
    #[default]
    Wal,
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum IsolationLevel {
    #[default]
    Snapshot,
    SerializableSnapshot,
}

#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum SstBlockSize {
    Block1Kib,
    Block2Kib,
    #[default]
    Block4Kib,
    Block8Kib,
    Block16Kib,
    Block32Kib,
    Block64Kib,
}

#[derive(Clone, Debug, Default, uniffi::Enum)]
pub enum Ttl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfterTicks(u64),
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbReadOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
}

impl Default for DbReadOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            cache_blocks: true,
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbScanOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub read_ahead_bytes: u64,
    pub cache_blocks: bool,
    pub max_fetch_tasks: u64,
}

impl Default for DbScanOptions {
    fn default() -> Self {
        Self {
            durability_filter: DurabilityLevel::default(),
            dirty: false,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbWriteOptions {
    pub await_durable: bool,
}

impl Default for DbWriteOptions {
    fn default() -> Self {
        Self {
            await_durable: true,
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbPutOptions {
    pub ttl: Ttl,
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbMergeOptions {
    pub ttl: Ttl,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct DbFlushOptions {
    pub flush_type: FlushType,
}

impl Default for DbFlushOptions {
    fn default() -> Self {
        Self {
            flush_type: FlushType::default(),
        }
    }
}

#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct DbKeyRange {
    pub start: Option<Vec<u8>>,
    pub start_inclusive: bool,
    pub end: Option<Vec<u8>>,
    pub end_inclusive: bool,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum DbWriteOperation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbPutOptions,
    },
    Merge {
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbMergeOptions,
    },
    Delete {
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
    pub create_ts: i64,
    pub expire_ts: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WriteHandle {
    pub seqnum: u64,
    pub create_ts: i64,
}

#[uniffi::export(callback_interface)]
pub trait MergeOperator: Send + Sync {
    fn merge(&self, key: Vec<u8>, existing_value: Option<Vec<u8>>, value: Vec<u8>) -> Vec<u8>;
}

#[derive(uniffi::Object)]
pub struct ObjectStore {
    inner: Arc<dyn ObjectStoreTrait>,
}

#[derive(uniffi::Object)]
pub struct DbBuilder {
    state: StdMutex<Option<BuilderState>>,
}

#[derive(uniffi::Object)]
pub struct Db {
    inner: CoreDb,
}

#[derive(uniffi::Object)]
pub struct DbSnapshot {
    inner: Arc<CoreDbSnapshot>,
}

#[derive(uniffi::Object)]
pub struct DbTransaction {
    inner: AsyncMutex<Option<CoreDbTransaction>>,
    id: String,
    seqnum: u64,
}

#[derive(uniffi::Object)]
pub struct DbIterator {
    inner: AsyncMutex<slatedb::DbIterator>,
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

#[uniffi::export]
pub fn default_settings_json() -> Result<String, SlatedbError> {
    slatedb::Settings::default()
        .to_json_string()
        .map_err(|error| SlatedbError::Internal {
            message: error.to_string(),
        })
}

#[uniffi::export]
pub fn resolve_object_store(url: String) -> Result<Arc<ObjectStore>, SlatedbError> {
    let inner = CoreDb::resolve_object_store(&url)?;
    Ok(Arc::new(ObjectStore { inner }))
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
        Ok(Arc::new(Db { inner: db }))
    }
}

#[uniffi::export]
impl Db {
    pub fn status(&self) -> Result<(), SlatedbError> {
        self.inner.status().map_err(Into::into)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Db {
    pub async fn close(&self) -> Result<(), SlatedbError> {
        self.inner.close().await.map_err(Into::into)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, SlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

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

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, SlatedbError> {
        Ok(self.inner.get_key_value(key).await?.map(KeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<KeyValue>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn scan(&self, range: DbKeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: DbKeyRange,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self.inner.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, SlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let options = options.into_core()?;
        let iter = self.inner.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        Ok(WriteHandle::from_core(self.inner.put(key, value).await?))
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        put_options: DbPutOptions,
        write_options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        let put_options = put_options.into_core();
        let write_options = write_options.into_core();
        Ok(WriteHandle::from_core(
            self.inner
                .put_with_options(key, value, &put_options, &write_options)
                .await?,
        ))
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<WriteHandle, SlatedbError> {
        validate_key(&key)?;
        Ok(WriteHandle::from_core(self.inner.delete(key).await?))
    }

    pub async fn delete_with_options(
        &self,
        key: Vec<u8>,
        options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key(&key)?;
        let options = options.into_core();
        Ok(WriteHandle::from_core(
            self.inner.delete_with_options(key, &options).await?,
        ))
    }

    pub async fn merge(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        Ok(WriteHandle::from_core(self.inner.merge(key, value).await?))
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        merge_options: DbMergeOptions,
        write_options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        validate_key_value(&key, &value)?;
        let merge_options = merge_options.into_core();
        let write_options = write_options.into_core();
        Ok(WriteHandle::from_core(
            self.inner
                .merge_with_options(key, value, &merge_options, &write_options)
                .await?,
        ))
    }

    pub async fn write(
        &self,
        operations: Vec<DbWriteOperation>,
    ) -> Result<WriteHandle, SlatedbError> {
        let batch = build_write_batch(operations)?;
        Ok(WriteHandle::from_core(self.inner.write(batch).await?))
    }

    pub async fn write_with_options(
        &self,
        operations: Vec<DbWriteOperation>,
        options: DbWriteOptions,
    ) -> Result<WriteHandle, SlatedbError> {
        let batch = build_write_batch(operations)?;
        let options = options.into_core();
        Ok(WriteHandle::from_core(
            self.inner.write_with_options(batch, &options).await?,
        ))
    }

    pub async fn flush(&self) -> Result<(), SlatedbError> {
        self.inner.flush().await.map_err(Into::into)
    }

    pub async fn flush_with_options(&self, options: DbFlushOptions) -> Result<(), SlatedbError> {
        self.inner
            .flush_with_options(options.into_core())
            .await
            .map_err(Into::into)
    }

    pub async fn snapshot(&self) -> Result<Arc<DbSnapshot>, SlatedbError> {
        Ok(Arc::new(DbSnapshot {
            inner: self.inner.snapshot().await?,
        }))
    }

    pub async fn begin(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<DbTransaction>, SlatedbError> {
        let tx = self.inner.begin(isolation_level.into_core()).await?;
        Ok(Arc::new(DbTransaction::new(tx)))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbSnapshot {
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, SlatedbError> {
        Ok(self.inner.get(key).await?.map(|value| value.to_vec()))
    }

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

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, SlatedbError> {
        Ok(self.inner.get_key_value(key).await?.map(KeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<KeyValue>, SlatedbError> {
        let options = options.into_core();
        Ok(self
            .inner
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn scan(&self, range: DbKeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let iter = self.inner.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: DbKeyRange,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let iter = self.inner.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, SlatedbError> {
        let iter = self.inner.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let options = options.into_core()?;
        let iter = self.inner.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }
}

impl DbTransaction {
    fn new(inner: CoreDbTransaction) -> Self {
        Self {
            id: inner.id().to_string(),
            seqnum: inner.seqnum(),
            inner: AsyncMutex::new(Some(inner)),
        }
    }
}

#[uniffi::export]
impl DbTransaction {
    pub fn seqnum(&self) -> u64 {
        self.seqnum
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbTransaction {
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.put(key, value).map_err(Into::into)
    }

    pub async fn put_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbPutOptions,
    ) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.put_with_options(key, value, &options).map_err(Into::into)
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<(), SlatedbError> {
        validate_key(&key)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.delete(key).map_err(Into::into)
    }

    pub async fn merge(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.merge(key, value).map_err(Into::into)
    }

    pub async fn merge_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: DbMergeOptions,
    ) -> Result<(), SlatedbError> {
        validate_key_value(&key, &value)?;
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.merge_with_options(key, value, &options)
            .map_err(Into::into)
    }

    pub async fn mark_read(&self, keys: Vec<Vec<u8>>) -> Result<(), SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.mark_read(keys).map_err(Into::into)
    }

    pub async fn unmark_write(&self, keys: Vec<Vec<u8>>) -> Result<(), SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        tx.unmark_write(keys).map_err(Into::into)
    }

    pub async fn rollback(&self) -> Result<(), SlatedbError> {
        let mut guard = self.inner.lock().await;
        let tx = guard.take().ok_or_else(transaction_completed)?;
        tx.rollback();
        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx.get(key).await?.map(|value| value.to_vec()))
    }

    pub async fn get_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<Vec<u8>>, SlatedbError> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx
            .get_with_options(key, &options)
            .await?
            .map(|value| value.to_vec()))
    }

    pub async fn get_key_value(&self, key: Vec<u8>) -> Result<Option<KeyValue>, SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx.get_key_value(key).await?.map(KeyValue::from_core))
    }

    pub async fn get_key_value_with_options(
        &self,
        key: Vec<u8>,
        options: DbReadOptions,
    ) -> Result<Option<KeyValue>, SlatedbError> {
        let options = options.into_core();
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        Ok(tx
            .get_key_value_with_options(key, &options)
            .await?
            .map(KeyValue::from_core))
    }

    pub async fn scan(&self, range: DbKeyRange) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan::<Vec<u8>, _>(range).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_with_options(
        &self,
        range: DbKeyRange,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let range = range.into_bounds()?;
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan_with_options::<Vec<u8>, _>(range, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Arc<DbIterator>, SlatedbError> {
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan_prefix(prefix).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn scan_prefix_with_options(
        &self,
        prefix: Vec<u8>,
        options: DbScanOptions,
    ) -> Result<Arc<DbIterator>, SlatedbError> {
        let options = options.into_core()?;
        let guard = self.inner.lock().await;
        let tx = guard.as_ref().ok_or_else(transaction_completed)?;
        let iter = tx.scan_prefix_with_options(prefix, &options).await?;
        Ok(Arc::new(DbIterator::new(iter)))
    }

    pub async fn commit(&self) -> Result<Option<WriteHandle>, SlatedbError> {
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or_else(transaction_completed)?
        };
        Ok(tx.commit().await?.map(WriteHandle::from_core))
    }

    pub async fn commit_with_options(
        &self,
        options: DbWriteOptions,
    ) -> Result<Option<WriteHandle>, SlatedbError> {
        let options = options.into_core();
        let tx = {
            let mut guard = self.inner.lock().await;
            guard.take().ok_or_else(transaction_completed)?
        };
        Ok(tx
            .commit_with_options(&options)
            .await?
            .map(WriteHandle::from_core))
    }
}

impl DbIterator {
    fn new(inner: slatedb::DbIterator) -> Self {
        Self {
            inner: AsyncMutex::new(inner),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbIterator {
    pub async fn next(&self) -> Result<Option<KeyValue>, SlatedbError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(KeyValue::from_core))
    }

    pub async fn seek(&self, key: Vec<u8>) -> Result<(), SlatedbError> {
        if key.is_empty() {
            return Err(SlatedbError::Invalid {
                message: "seek key cannot be empty".to_owned(),
            });
        }
        let mut guard = self.inner.lock().await;
        guard.seek(key).await.map_err(Into::into)
    }
}

impl DurabilityLevel {
    fn into_core(self) -> config::DurabilityLevel {
        match self {
            Self::Remote => config::DurabilityLevel::Remote,
            Self::Memory => config::DurabilityLevel::Memory,
        }
    }
}

impl FlushType {
    fn into_core(self) -> config::FlushType {
        match self {
            Self::MemTable => config::FlushType::MemTable,
            Self::Wal => config::FlushType::Wal,
        }
    }
}

impl IsolationLevel {
    fn into_core(self) -> CoreIsolationLevel {
        match self {
            Self::Snapshot => CoreIsolationLevel::Snapshot,
            Self::SerializableSnapshot => CoreIsolationLevel::SerializableSnapshot,
        }
    }
}

impl SstBlockSize {
    fn into_core(self) -> CoreSstBlockSize {
        match self {
            Self::Block1Kib => CoreSstBlockSize::Block1Kib,
            Self::Block2Kib => CoreSstBlockSize::Block2Kib,
            Self::Block4Kib => CoreSstBlockSize::Block4Kib,
            Self::Block8Kib => CoreSstBlockSize::Block8Kib,
            Self::Block16Kib => CoreSstBlockSize::Block16Kib,
            Self::Block32Kib => CoreSstBlockSize::Block32Kib,
            Self::Block64Kib => CoreSstBlockSize::Block64Kib,
        }
    }
}

impl Ttl {
    fn into_core(self) -> config::Ttl {
        match self {
            Self::Default => config::Ttl::Default,
            Self::NoExpiry => config::Ttl::NoExpiry,
            Self::ExpireAfterTicks(ttl) => config::Ttl::ExpireAfter(ttl),
        }
    }
}

impl DbReadOptions {
    fn into_core(self) -> config::ReadOptions {
        config::ReadOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            cache_blocks: self.cache_blocks,
        }
    }
}

impl DbScanOptions {
    fn into_core(self) -> Result<config::ScanOptions, SlatedbError> {
        Ok(config::ScanOptions {
            durability_filter: self.durability_filter.into_core(),
            dirty: self.dirty,
            read_ahead_bytes: try_usize(self.read_ahead_bytes, "read_ahead_bytes")?,
            cache_blocks: self.cache_blocks,
            max_fetch_tasks: try_usize(self.max_fetch_tasks, "max_fetch_tasks")?,
        })
    }
}

impl DbWriteOptions {
    fn into_core(self) -> config::WriteOptions {
        config::WriteOptions {
            await_durable: self.await_durable,
        }
    }
}

impl DbPutOptions {
    fn into_core(self) -> config::PutOptions {
        config::PutOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

impl DbMergeOptions {
    fn into_core(self) -> config::MergeOptions {
        config::MergeOptions {
            ttl: self.ttl.into_core(),
        }
    }
}

impl DbFlushOptions {
    fn into_core(self) -> config::FlushOptions {
        config::FlushOptions {
            flush_type: self.flush_type.into_core(),
        }
    }
}

impl DbKeyRange {
    fn into_bounds(self) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>), SlatedbError> {
        if self.start.as_ref().is_some_and(|start| start.is_empty()) {
            return Err(SlatedbError::Invalid {
                message: "range start cannot be empty".to_owned(),
            });
        }
        if self.end.as_ref().is_some_and(|end| end.is_empty()) {
            return Err(SlatedbError::Invalid {
                message: "range end cannot be empty".to_owned(),
            });
        }

        if let (Some(start), Some(end)) = (&self.start, &self.end) {
            match start.cmp(end) {
                std::cmp::Ordering::Greater => {
                    return Err(SlatedbError::Invalid {
                        message: "range start must not be greater than range end".to_owned(),
                    });
                }
                std::cmp::Ordering::Equal if !(self.start_inclusive && self.end_inclusive) => {
                    return Err(SlatedbError::Invalid {
                        message: "range must be non-empty".to_owned(),
                    });
                }
                _ => {}
            }
        }

        Ok((
            match self.start {
                Some(start) if self.start_inclusive => Bound::Included(start),
                Some(start) => Bound::Excluded(start),
                None => Bound::Unbounded,
            },
            match self.end {
                Some(end) if self.end_inclusive => Bound::Included(end),
                Some(end) => Bound::Excluded(end),
                None => Bound::Unbounded,
            },
        ))
    }
}

impl KeyValue {
    fn from_core(value: CoreKeyValue) -> Self {
        Self {
            key: value.key.to_vec(),
            value: value.value.to_vec(),
            seq: value.seq,
            create_ts: value.create_ts,
            expire_ts: value.expire_ts,
        }
    }
}

impl WriteHandle {
    fn from_core(value: CoreWriteHandle) -> Self {
        Self {
            seqnum: value.seqnum(),
            create_ts: value.create_ts(),
        }
    }
}

fn build_write_batch(operations: Vec<DbWriteOperation>) -> Result<CoreWriteBatch, SlatedbError> {
    let mut batch = CoreWriteBatch::new();

    for operation in operations {
        match operation {
            DbWriteOperation::Put {
                key,
                value,
                options,
            } => {
                validate_key_value(&key, &value)?;
                let options = options.into_core();
                batch.put_with_options(key, value, &options);
            }
            DbWriteOperation::Merge {
                key,
                value,
                options,
            } => {
                validate_key_value(&key, &value)?;
                let options = options.into_core();
                batch.merge_with_options(key, value, &options);
            }
            DbWriteOperation::Delete { key } => {
                validate_key(&key)?;
                batch.delete(key);
            }
        }
    }

    Ok(batch)
}

fn validate_key(key: &[u8]) -> Result<(), SlatedbError> {
    if key.is_empty() {
        return Err(SlatedbError::Invalid {
            message: "key cannot be empty".to_owned(),
        });
    }
    if key.len() > u16::MAX as usize {
        return Err(SlatedbError::Invalid {
            message: "key size must be <= u16::MAX".to_owned(),
        });
    }
    Ok(())
}

fn validate_key_value(key: &[u8], value: &[u8]) -> Result<(), SlatedbError> {
    validate_key(key)?;
    if value.len() > u32::MAX as usize {
        return Err(SlatedbError::Invalid {
            message: "value size must be <= u32::MAX".to_owned(),
        });
    }
    Ok(())
}

fn try_usize(value: u64, field: &str) -> Result<usize, SlatedbError> {
    usize::try_from(value).map_err(|_| SlatedbError::Invalid {
        message: format!("{field} is too large"),
    })
}

fn builder_consumed() -> SlatedbError {
    SlatedbError::Invalid {
        message: "builder has already been consumed".to_owned(),
    }
}

fn transaction_completed() -> SlatedbError {
    SlatedbError::Invalid {
        message: "transaction has already been completed".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::path::Path;
    use tempfile::tempdir;

    struct CounterMergeOperator;

    impl MergeOperator for CounterMergeOperator {
        fn merge(&self, _key: Vec<u8>, existing_value: Option<Vec<u8>>, value: Vec<u8>) -> Vec<u8> {
            let existing = existing_value
                .map(|value| u64::from_le_bytes(value.try_into().expect("8-byte existing value")))
                .unwrap_or(0);
            let delta = u64::from_le_bytes(value.try_into().expect("8-byte delta"));
            (existing + delta).to_le_bytes().to_vec()
        }
    }

    #[tokio::test]
    async fn resolves_memory_object_store() {
        let store = resolve_object_store("memory:///".to_owned()).unwrap();
        let location = Path::from("hello");
        let payload = Bytes::from_static(b"world");

        store.inner.put(&location, payload.clone().into()).await.unwrap();
        let result = store.inner.get(&location).await.unwrap().bytes().await.unwrap();

        assert_eq!(result, payload);
    }

    #[tokio::test]
    async fn resolves_local_object_store() {
        let temp_dir = tempdir().unwrap();
        let prefix_path = temp_dir.path().join("prefix-store");
        let url = format!("file://{}", prefix_path.display());
        let store = resolve_object_store(url).unwrap();
        let location = Path::from("nested/file.txt");
        let payload = Bytes::from_static(b"payload");

        store.inner.put(&location, payload.clone().into()).await.unwrap();

        let stored = tokio::fs::read(prefix_path.join("nested").join("file.txt"))
            .await
            .unwrap();
        assert_eq!(stored, payload.to_vec());
    }

    #[tokio::test]
    async fn builds_database_and_rejects_builder_reuse() {
        let store = resolve_object_store("memory:///".to_owned()).unwrap();
        let builder = DbBuilder::new("test-db".to_owned(), store);
        let db = builder.build().await.unwrap();

        assert!(builder.with_seed(7).is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn supports_put_get_snapshot_and_close() {
        let store = resolve_object_store("memory:///".to_owned()).unwrap();
        let builder = DbBuilder::new("test-db".to_owned(), store);
        let db = builder.build().await.unwrap();

        db.put(b"k1".to_vec(), b"v1".to_vec()).await.unwrap();
        let snapshot = db.snapshot().await.unwrap();
        db.put(b"k2".to_vec(), b"v2".to_vec()).await.unwrap();

        assert_eq!(db.get(b"k1".to_vec()).await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2".to_vec()).await.unwrap(), Some(b"v2".to_vec()));
        assert_eq!(snapshot.get(b"k2".to_vec()).await.unwrap(), None);

        db.close().await.unwrap();
        assert!(db.status().is_err());
    }

    #[tokio::test]
    async fn supports_merge_operator_and_merge_writes() {
        let store = resolve_object_store("memory:///".to_owned()).unwrap();
        let builder = DbBuilder::new("merge-db".to_owned(), store);
        builder
            .with_merge_operator(Box::new(CounterMergeOperator))
            .unwrap();
        let db = builder.build().await.unwrap();

        db.put(b"counter".to_vec(), 1_u64.to_le_bytes().to_vec())
            .await
            .unwrap();
        db.merge(b"counter".to_vec(), 2_u64.to_le_bytes().to_vec())
            .await
            .unwrap();

        let value = db.get(b"counter".to_vec()).await.unwrap().unwrap();
        assert_eq!(u64::from_le_bytes(value.try_into().unwrap()), 3);
    }

    #[tokio::test]
    async fn supports_scan_iterator_seek_and_transaction_lifecycle() {
        let store = resolve_object_store("memory:///".to_owned()).unwrap();
        let builder = DbBuilder::new("scan-db".to_owned(), store);
        let db = builder.build().await.unwrap();

        db.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();
        db.put(b"b".to_vec(), b"2".to_vec()).await.unwrap();
        db.put(b"c".to_vec(), b"3".to_vec()).await.unwrap();

        let iter = db
            .scan(DbKeyRange {
                start: Some(b"a".to_vec()),
                start_inclusive: true,
                end: Some(b"z".to_vec()),
                end_inclusive: false,
            })
            .await
            .unwrap();

        assert_eq!(iter.next().await.unwrap().unwrap().key, b"a".to_vec());
        iter.seek(b"c".to_vec()).await.unwrap();
        assert_eq!(iter.next().await.unwrap().unwrap().key, b"c".to_vec());
        assert!(iter.seek(Vec::new()).await.is_err());

        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        txn.put(b"tx".to_vec(), b"value".to_vec()).await.unwrap();
        assert_eq!(txn.get(b"tx".to_vec()).await.unwrap(), Some(b"value".to_vec()));
        let handle = txn.commit().await.unwrap().unwrap();
        assert!(handle.seqnum > 0);
        assert!(txn.commit().await.is_err());
    }

    #[tokio::test]
    async fn rejects_invalid_settings_json() {
        let store = resolve_object_store("memory:///".to_owned()).unwrap();
        let builder = DbBuilder::new("test-db".to_owned(), store);
        assert!(builder.with_settings_json("{not-json}".to_owned()).is_err());
    }
}
