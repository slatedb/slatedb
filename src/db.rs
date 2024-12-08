//! This module provides the core database functionality for SlateDB.
//! It provides methods for reading and writing to the database, as well as for flushing the database to disk.
//!
//! The `Db` struct represents a database.
//!
//! # Examples
//!
//! Basic usage of the `Db` struct:
//!
//! ```
//! use slatedb::{db::Db, error::SlateDBError};
//! use slatedb::object_store::{ObjectStore, memory::InMemory};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SlateDBError> {
//!     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//!     let db = Db::open("test_db", object_store).await?;
//!     Ok(())
//! }
//! ```
use std::collections::VecDeque;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedSender;

use crate::batch::WriteBatch;
use crate::batch_write::{WriteBatchMsg, WriteBatchRequest};
use crate::bytes_range::BytesRange;
use crate::cached_object_store::CachedObjectStore;
use crate::cached_object_store::FsCacheStorage;
use crate::compactor::Compactor;
use crate::config::ReadLevel::Uncommitted;
use crate::config::{
    DbOptions, PutOptions, ReadOptions, ScanOptions, WriteOptions, DEFAULT_READ_OPTIONS,
    DEFAULT_SCAN_OPTIONS, DEFAULT_WRITE_OPTIONS,
};
use crate::db_iter::DbIterator;
use crate::db_state::{CoreDbState, DbState, SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::filter;
use crate::flush::WalFlushThreadMsg;
use crate::garbage_collector::GarbageCollector;
use crate::iter::KeyValueIterator;
use crate::manifest_store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::mem_table::{VecDequeKeyValueIterator, WritableKVTable};
use crate::mem_table_flush::MemtableFlushThreadMsg;
use crate::metrics::DbStats;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst::SsTableFormat;
use crate::sst_iter::SstIterator;
use crate::tablestore::TableStore;
use crate::types::{RowAttributes, ValueDeletable};
use std::rc::Rc;

pub(crate) type FlushSender = tokio::sync::oneshot::Sender<Result<(), SlateDBError>>;
pub(crate) type FlushMsg<T> = (Option<FlushSender>, T);

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<DbState>>,
    pub(crate) options: DbOptions,
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) wal_flush_notifier: UnboundedSender<FlushMsg<WalFlushThreadMsg>>,
    pub(crate) memtable_flush_notifier: UnboundedSender<FlushMsg<MemtableFlushThreadMsg>>,
    pub(crate) write_notifier: UnboundedSender<WriteBatchMsg>,
    pub(crate) db_stats: Arc<DbStats>,
    pub(crate) error: RwLock<Option<SlateDBError>>,
}

impl DbInner {
    pub async fn new(
        options: DbOptions,
        table_store: Arc<TableStore>,
        core_db_state: CoreDbState,
        wal_flush_notifier: UnboundedSender<FlushMsg<WalFlushThreadMsg>>,
        memtable_flush_notifier: UnboundedSender<FlushMsg<MemtableFlushThreadMsg>>,
        write_notifier: UnboundedSender<WriteBatchMsg>,
        db_stats: Arc<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let state = DbState::new(core_db_state);
        let db_inner = Self {
            state: Arc::new(RwLock::new(state)),
            options,
            table_store,
            wal_flush_notifier,
            memtable_flush_notifier,
            write_notifier,
            db_stats,
            error: RwLock::new(None),
        };
        Ok(db_inner)
    }

    /// Get the value for a given key.
    pub async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.check_error()?;
        let snapshot = self.state.read().snapshot();

        if matches!(options.read_level, Uncommitted) {
            let maybe_val = std::iter::once(snapshot.wal)
                .chain(snapshot.state.imm_wal.iter().map(|imm| imm.table()))
                .find_map(|memtable| memtable.get(key));
            if let Some(val) = maybe_val {
                return Ok(val.value.into_option());
            }
        }

        let maybe_val = std::iter::once(snapshot.memtable)
            .chain(snapshot.state.imm_memtable.iter().map(|imm| imm.table()))
            .find_map(|memtable| memtable.get(key));
        if let Some(val) = maybe_val {
            return Ok(val.value.into_option());
        }

        // Since the key remains unchanged during the point query, we only need to compute
        // the hash value once and pass it to the filter to avoid unnecessary hash computation
        let key_hash = filter::filter_hash(key);
        let key_bytes = Bytes::copy_from_slice(key);

        for sst in &snapshot.state.core.l0 {
            if self
                .sst_might_include_key(sst, &key_bytes, key_hash)
                .await?
            {
                let mut iter = SstIterator::new_from_key(
                    sst,
                    self.table_store.clone(),
                    key_bytes.clone(),
                    1,
                    1,
                    true,
                )
                .await?; // cache blocks that are being read
                if let Some(entry) = iter.next_entry().await? {
                    if entry.key == key {
                        return Ok(entry.value.into_option());
                    }
                }
            }
        }
        for sr in &snapshot.state.core.compacted {
            if self.sr_might_include_key(sr, key, key_hash).await? {
                let mut iter: SortedRunIterator<&SsTableHandle> = SortedRunIterator::new_from_key(
                    sr,
                    key_bytes.clone(),
                    self.table_store.clone(),
                    1,
                    1,
                    true,
                ) // cache blocks
                .await?;
                if let Some(entry) = iter.next_entry().await? {
                    if entry.key == key {
                        return Ok(entry.value.into_option());
                    }
                }
            }
        }
        Ok(None)
    }

    pub async fn scan_with_options<'a>(
        &'a self,
        range: BytesRange,
        options: &ScanOptions,
    ) -> Result<DbIterator<'a>, SlateDBError> {
        self.check_error()?;
        let snapshot = Arc::new(self.state.read().snapshot());
        let mut memtables = VecDeque::new();

        if matches!(options.read_level, Uncommitted) {
            memtables.push_back(snapshot.wal.clone());
            for imm_wal in &snapshot.state.imm_wal {
                memtables.push_back(imm_wal.table());
            }
        }

        memtables.push_back(snapshot.memtable.clone());
        for memtable in &snapshot.state.imm_memtable {
            memtables.push_back(memtable.table());
        }

        let mem_iter =
            VecDequeKeyValueIterator::materialize_range(memtables, range.clone()).await?;

        let state = snapshot.state.as_ref().clone();
        let mut l0_iters = VecDeque::new();
        let blocks_to_fetch = self.table_store.bytes_to_blocks(options.read_ahead_bytes);

        for sst in state.core.l0 {
            let iter = SstIterator::new_opts(
                Box::new(sst),
                range.clone(),
                self.table_store.clone(),
                1,
                blocks_to_fetch,
                true,
                options.cache_blocks,
            )
            .await?;
            l0_iters.push_back(iter);
        }

        let mut sr_iters: VecDeque<SortedRunIterator<Box<SsTableHandle>>> = VecDeque::new();
        for sr in state.core.compacted {
            let sorted_run_iter = SortedRunIterator::new_from_range(
                sr,
                range.clone(),
                self.table_store.clone(),
                1,
                blocks_to_fetch,
                options.cache_blocks,
            )
            .await?;
            sr_iters.push_back(sorted_run_iter);
        }

        DbIterator::new(range.clone(), mem_iter, l0_iters, sr_iters).await
    }

    async fn fence_writers(
        &self,
        manifest: &mut FenceableManifest,
        next_wal_id: u64,
    ) -> Result<(), SlateDBError> {
        let mut empty_wal_id = next_wal_id;

        loop {
            let empty_wal = WritableKVTable::new();
            match self
                .flush_imm_table(&SsTableId::Wal(empty_wal_id), empty_wal.table().clone())
                .await
            {
                Ok(_) => {
                    return Ok(());
                }
                Err(SlateDBError::Fenced) => {
                    let updated_state = manifest.refresh().await?;
                    self.state.write().refresh_db_state(updated_state);
                    empty_wal_id += 1;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    /// Check if the given key might be in the range of the SST. Checks if the key is
    /// in the range of the sst and if the filter might contain the key.
    /// ## Arguments
    /// - `sst`: the sst to check
    /// - `key`: the key to check
    /// - `key_hash`: the hash of the key (used for filter, to avoid recomputing the hash)
    /// ## Returns
    /// - `true` if the key is in the range of the sst.
    async fn sst_might_include_key(
        &self,
        sst: &SsTableHandle,
        key: &[u8],
        key_hash: u64,
    ) -> Result<bool, SlateDBError> {
        if !sst.range_covers_key(key) {
            return Ok(false);
        }
        if let Some(filter) = self.table_store.read_filter(sst).await? {
            return Ok(filter.might_contain(key_hash));
        }
        Ok(true)
    }

    /// Check if the given key might be in the range of the sorted run (SR). Checks if the key
    /// is in the range of the SSTs in the run and if the SST's filter might contain the key.
    /// ## Arguments
    /// - `sr`: the sorted run to check
    /// - `key`: the key to check
    /// - `key_hash`: the hash of the key (used for filter, to avoid recomputing the hash)
    /// ## Returns
    /// - `true` if the key is in the range of the sst.
    async fn sr_might_include_key(
        &self,
        sr: &SortedRun,
        key: &[u8],
        key_hash: u64,
    ) -> Result<bool, SlateDBError> {
        if let Some(sst) = sr.find_sst_with_range_covering_key(key) {
            if let Some(filter) = self.table_store.read_filter(sst).await? {
                return Ok(filter.might_contain(key_hash));
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn wal_enabled(&self) -> bool {
        #[cfg(feature = "wal_disable")]
        return self.options.wal_enabled;
        #[cfg(not(feature = "wal_disable"))]
        return true;
    }

    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        self.check_error()?;

        if batch.ops.is_empty() {
            return Ok(());
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch_msg = WriteBatchMsg::WriteBatch(WriteBatchRequest { batch, done: tx });

        self.maybe_apply_backpressure().await;
        self.write_notifier
            .send(batch_msg)
            .expect("write notifier closed");

        let current_table = rx.await??;

        if options.await_durable {
            current_table.await_durable().await;
        }

        Ok(())
    }

    #[inline]
    pub(crate) async fn maybe_apply_backpressure(&self) {
        loop {
            let mem_size_bytes = {
                let guard = self.state.read();
                // Exclude active memtable and WAL to avoid a write lock.
                let imm_wal_size = guard
                    .state()
                    .imm_wal
                    .iter()
                    .map(|imm| imm.table().size())
                    .sum::<usize>();
                let imm_memtable_size = guard
                    .state()
                    .imm_memtable
                    .iter()
                    .map(|imm| imm.table().size())
                    .sum::<usize>();
                imm_wal_size + imm_memtable_size
            };
            if mem_size_bytes >= self.options.max_unflushed_bytes {
                let (wal_table, mem_table) = {
                    let guard = self.state.read();
                    (
                        guard.state().imm_wal.back().map(|imm| imm.table().clone()),
                        guard.state().imm_memtable.back().cloned(),
                    )
                };
                tracing::warn!(
                    "Unflushed memtable and WAL size {} >= max_unflushed_bytes {}. Applying backpressure.",
                    mem_size_bytes, self.options.max_unflushed_bytes,
                );
                match (wal_table, mem_table) {
                    (Some(wal_table), Some(mem_table)) => {
                        tokio::select! {
                            _ = wal_table.await_durable() => {}
                            _ = mem_table.await_flush_to_l0() => {}
                        }
                    }
                    (Some(wal_table), None) => {
                        wal_table.await_durable().await;
                    }
                    (None, Some(mem_table)) => {
                        mem_table.await_flush_to_l0().await;
                    }
                    _ => {
                        // No tables to flush, so backpressure is no longer needed.
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    async fn flush_wals(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.wal_flush_notifier
            .send((Some(tx), WalFlushThreadMsg::FlushImmutableWals))
            .map_err(|_| SlateDBError::WalFlushChannelError)?;
        rx.await?
    }

    // use to manually flush memtables
    async fn flush_memtables(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.memtable_flush_notifier
            .send((Some(tx), MemtableFlushThreadMsg::FlushImmutableMemtables))
            .map_err(|_| SlateDBError::MemtableFlushChannelError)?;
        rx.await?
    }

    async fn replay_wal(&self) -> Result<(), SlateDBError> {
        async fn load_sst_iters(
            db_inner: &DbInner,
            sst_id: u64,
        ) -> Result<(SstIterator<'_, Rc<SsTableHandle>>, u64), SlateDBError> {
            let sst = Rc::new(
                db_inner
                    .table_store
                    .open_sst(&SsTableId::Wal(sst_id))
                    .await?,
            );
            let id = match &sst.id {
                SsTableId::Wal(id) => *id,
                SsTableId::Compacted(_) => return Err(SlateDBError::InvalidDBState),
            };
            let iter =
                SstIterator::new_spawn(Rc::clone(&sst), db_inner.table_store.clone(), 1, 256, true)
                    .await?;
            Ok((iter, id))
        }

        let wal_id_last_compacted = self.state.read().state().core.last_compacted_wal_sst_id;
        let mut wal_sst_list = self
            .table_store
            .list_wal_ssts((wal_id_last_compacted + 1)..)
            .await?
            .into_iter()
            .map(|wal_sst| wal_sst.id.unwrap_wal_id())
            .collect::<Vec<_>>();
        let mut last_sst_id = wal_id_last_compacted;
        let sst_batch_size = 4;

        let mut remaining_sst_list = Vec::new();
        if wal_sst_list.len() > sst_batch_size {
            remaining_sst_list = wal_sst_list.split_off(sst_batch_size);
        }
        let mut remaining_sst_iter = remaining_sst_list.iter();

        // Load the first N ssts and instantiate their iterators
        let mut sst_iterators = VecDeque::new();
        for sst_id in wal_sst_list {
            sst_iterators.push_back(load_sst_iters(self, sst_id).await?);
        }

        while let Some((mut sst_iter, sst_id)) = sst_iterators.pop_front() {
            last_sst_id = sst_id;
            // iterate over the WAL SSTs in reverse order to ensure we recover in write-order
            // buffer the WAL entries to bulk replay them into the memtable.
            let mut wal_replay_buf = Vec::new();
            while let Some(kv) = sst_iter.next_entry().await? {
                wal_replay_buf.push(kv);
            }
            // Build the memtable
            {
                let mut guard = self.state.write();
                for kv in wal_replay_buf.iter() {
                    if let Some(ts) = kv.create_ts {
                        guard.update_clock_tick(ts)?;
                    }

                    match &kv.value {
                        ValueDeletable::Value(value) => {
                            guard.memtable().put(
                                kv.key.clone(),
                                value.clone(),
                                RowAttributes {
                                    ts: kv.create_ts,
                                    expire_ts: kv.expire_ts,
                                },
                            );
                        }
                        ValueDeletable::Tombstone => guard.memtable().delete(
                            kv.key.clone(),
                            RowAttributes {
                                ts: kv.create_ts,
                                expire_ts: kv.expire_ts,
                            },
                        ),
                    }
                }
                self.maybe_freeze_memtable(&mut guard, sst_id)?;
                if guard.state().core.next_wal_sst_id == sst_id {
                    guard.increment_next_wal_id();
                }
            }

            // feed the remaining SstIterators into the vecdeque
            if let Some(sst_id) = remaining_sst_iter.next() {
                sst_iterators.push_back(load_sst_iters(self, *sst_id).await?);
            }
        }

        // assert that we didn't have any gaps in the wal
        assert_eq!(
            last_sst_id + 1,
            self.state.read().state().core.next_wal_sst_id
        );

        Ok(())
    }

    /// Return an error if the state has encountered
    /// an unrecoverable error.
    pub(crate) fn check_error(&self) -> Result<(), SlateDBError> {
        let error = self.error.read();
        if let Some(err) = error.as_ref() {
            Err(err.clone())
        } else {
            Ok(())
        }
    }

    /// Set the state error if it is not already set.
    ///
    /// We only care about the first error because
    /// subsequent errors are likely to be the result of
    /// the first error.
    pub(crate) fn set_error_if_none(&self, new_error: SlateDBError) {
        let mut error = self.error.write();
        if error.is_none() {
            *error = Some(new_error);
        }
    }
}

pub struct Db {
    inner: Arc<DbInner>,
    /// The handle for the flush thread.
    wal_flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    memtable_flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    write_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    compactor: Mutex<Option<Compactor>>,
    garbage_collector: Mutex<Option<GarbageCollector>>,
}

impl Db {
    /// Open a new database with default options.
    ///
    /// ## Arguments
    /// - `path`: the path to the database
    /// - `object_store`: the object store to use for the database
    ///
    /// ## Returns
    /// - `Db`: the database
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error opening the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open<P: Into<Path>>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, SlateDBError> {
        Self::open_with_opts(path, DbOptions::default(), object_store).await
    }

    /// Open a new database with custom `DbOptions`.
    ///
    /// ## Arguments
    /// - `path`: the path to the database
    /// - `options`: the options to use for the database
    /// - `object_store`: the object store to use for the database
    ///
    /// ## Returns
    /// - `Db`: the database
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error opening the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, config::DbOptions, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open_with_opts("test_db", DbOptions::default(), object_store).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_with_opts<P: Into<Path>>(
        path: P,
        options: DbOptions,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, SlateDBError> {
        Self::open_with_fp_registry(
            path,
            options,
            object_store,
            Arc::new(FailPointRegistry::new()),
        )
        .await
    }

    /// Open a new database with a custom `FailPointRegistry`.
    ///
    /// ## Arguments
    /// - `path`: the path to the database
    /// - `options`: the options to use for the database
    /// - `object_store`: the object store to use for the database
    /// - `fp_registry`: the failpoint registry to use for the database
    ///
    /// ## Returns
    /// - `Db`: the database
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error opening the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, config::DbOptions, error::SlateDBError};
    /// use slatedb::fail_parallel::FailPointRegistry;
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let fp_registry = Arc::new(FailPointRegistry::new());
    ///     let db = Db::open_with_fp_registry("test_db", DbOptions::default(), object_store, fp_registry).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_with_fp_registry<P: Into<Path>>(
        path: P,
        options: DbOptions,
        object_store: Arc<dyn ObjectStore>,
        fp_registry: Arc<FailPointRegistry>,
    ) -> Result<Self, SlateDBError> {
        let path = path.into();
        if let Ok(options_json) = options.to_json_string() {
            tracing::info!(?path, options = options_json, "Opening SlateDB database");
        } else {
            tracing::info!(?path, ?options, "Opening SlateDB database");
        }

        let db_stats = Arc::new(DbStats::new());
        let sst_format = SsTableFormat {
            min_filter_keys: options.min_filter_keys,
            filter_bits_per_key: options.filter_bits_per_key,
            compression_codec: options.compression_codec,
            ..SsTableFormat::default()
        };
        let maybe_cached_object_store = match &options.object_store_cache_options.root_folder {
            None => object_store.clone(),
            Some(cache_root_folder) => {
                let cache_storage = Arc::new(FsCacheStorage::new(
                    cache_root_folder.clone(),
                    options.object_store_cache_options.max_cache_size_bytes,
                    options.object_store_cache_options.scan_interval,
                    db_stats.clone(),
                ));

                let cached_object_store = CachedObjectStore::new(
                    object_store.clone(),
                    cache_storage,
                    options.object_store_cache_options.part_size_bytes,
                    db_stats.clone(),
                )?;
                cached_object_store.start_evictor().await;
                cached_object_store
            }
        };

        let table_store = Arc::new(TableStore::new_with_fp_registry(
            maybe_cached_object_store.clone(),
            sst_format.clone(),
            path.clone(),
            fp_registry.clone(),
            options.block_cache.clone(),
        ));

        let manifest_store = Arc::new(ManifestStore::new(&path, maybe_cached_object_store.clone()));
        let latest_manifest = StoredManifest::try_load(manifest_store.clone()).await?;

        // get the next wal id before writing manifest.
        let wal_id_last_compacted = match &latest_manifest {
            Some(latest_stored_manifest) => {
                latest_stored_manifest.db_state().last_compacted_wal_sst_id
            }
            None => 0,
        };
        let next_wal_id = table_store.next_wal_sst_id(wal_id_last_compacted).await?;

        let mut manifest = Self::init_db(&manifest_store, latest_manifest).await?;
        let (memtable_flush_tx, memtable_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let (wal_flush_tx, wal_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let (write_tx, write_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(
            DbInner::new(
                options.clone(),
                table_store.clone(),
                manifest.db_state()?.clone(),
                wal_flush_tx,
                memtable_flush_tx,
                write_tx,
                db_stats,
            )
            .await?,
        );
        if inner.wal_enabled() {
            inner.fence_writers(&mut manifest, next_wal_id).await?;
        }
        inner.replay_wal().await?;
        let tokio_handle = Handle::current();
        let flush_thread = if inner.wal_enabled() {
            inner.spawn_flush_task(wal_flush_rx, &tokio_handle)
        } else {
            None
        };
        let memtable_flush_task =
            inner.spawn_memtable_flush_task(manifest, memtable_flush_rx, &tokio_handle);
        let write_task = inner.spawn_write_task(write_rx, &tokio_handle);
        let mut compactor = None;
        if let Some(compactor_options) = &inner.options.compactor_options {
            // not to pollute the cache during compaction
            let uncached_table_store = Arc::new(TableStore::new_with_fp_registry(
                object_store.clone(),
                sst_format,
                path.clone(),
                fp_registry.clone(),
                None,
            ));
            compactor = Some(
                Compactor::new(
                    manifest_store.clone(),
                    uncached_table_store.clone(),
                    compactor_options.clone(),
                    Handle::current(),
                    inner.db_stats.clone(),
                )
                .await?,
            )
        }
        let mut garbage_collector = None;
        if let Some(gc_options) = &inner.options.garbage_collector_options {
            garbage_collector = Some(
                GarbageCollector::new(
                    manifest_store.clone(),
                    table_store.clone(),
                    gc_options.clone(),
                    Handle::current(),
                    inner.db_stats.clone(),
                )
                .await,
            )
        };
        Ok(Self {
            inner,
            wal_flush_task: Mutex::new(flush_thread),
            memtable_flush_task: Mutex::new(memtable_flush_task),
            write_task: Mutex::new(write_task),
            compactor: Mutex::new(compactor),
            garbage_collector: Mutex::new(garbage_collector),
        })
    }

    async fn init_db(
        manifest_store: &Arc<ManifestStore>,
        latest_stored_manifest: Option<StoredManifest>,
    ) -> Result<FenceableManifest, SlateDBError> {
        let stored_manifest = match latest_stored_manifest {
            Some(manifest) => manifest,
            None => StoredManifest::init_new_db(manifest_store.clone(), CoreDbState::new()).await?,
        };
        FenceableManifest::init_writer(stored_manifest).await
    }

    /// Close the database.
    ///
    /// ## Returns
    /// - `Result<(), SlateDBError>`: if there was an error closing the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.close().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn close(&self) -> Result<(), SlateDBError> {
        if let Some(compactor) = {
            let mut maybe_compactor = self.compactor.lock();
            maybe_compactor.take()
        } {
            compactor.close().await;
        }

        if let Some(gc) = {
            let mut maybe_gc = self.garbage_collector.lock();
            maybe_gc.take()
        } {
            gc.close().await;
        }

        // Shutdown the write batch thread.
        self.inner.write_notifier.send(WriteBatchMsg::Shutdown).ok();

        if let Some(write_task) = {
            let mut write_task = self.write_task.lock();
            write_task.take()
        } {
            write_task.await.expect("Failed to join write thread");
        }

        // Shutdown the WAL flush thread.
        self.inner
            .wal_flush_notifier
            .send((None, WalFlushThreadMsg::Shutdown))
            .ok();

        if let Some(flush_task) = {
            let mut flush_task = self.wal_flush_task.lock();
            flush_task.take()
        } {
            flush_task.await.expect("Failed to join flush thread");
        }

        // Shutdown the memtable flush thread.
        self.inner
            .memtable_flush_notifier
            .send((None, MemtableFlushThreadMsg::Shutdown))
            .ok();

        if let Some(memtable_flush_task) = {
            let mut memtable_flush_task = self.memtable_flush_task.lock();
            memtable_flush_task.take()
        } {
            memtable_flush_task
                .await
                .expect("Failed to join memtable flush thread");
        }

        Ok(())
    }

    /// Get a value from the database with default read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     assert_eq!(db.get(b"key").await?, Some(Bytes::from_static(b"value")));
    ///     Ok(())
    /// }
    /// ```
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        self.inner.get_with_options(key, DEFAULT_READ_OPTIONS).await
    }

    /// Get a value from the database with custom read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use slatedb::{db::Db, config::ReadOptions, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     assert_eq!(db.get_with_options(b"key", &ReadOptions::default()).await?, Some(Bytes::from_static(b"value")));
    ///     Ok(())
    /// }
    /// ```
    pub async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.inner.get_with_options(key, options).await
    }

    /// Scan a range of keys using the default options [`DEFAULT_SCAN_OPTIONS`].
    ///
    /// returns a `DbIterator`
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error scanning the range of keys
    ///
    /// ## Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"a", b"a_value").await?;
    ///     db.put(b"b", b"b_value").await?;
    ///
    ///     let mut iter = db.scan(..).await?;
    ///     assert_eq!(Some((b"a" as &[u8], b"a_value" as &[u8]).into()) , iter.next().await?);
    ///     assert_eq!(Some((b"b" as &[u8], b"b_value" as &[u8]).into()) , iter.next().await?);
    ///     assert_eq!(None , iter.next().await?);
    ///     Ok(())
    /// }
    /// ```
    pub async fn scan<T: RangeBounds<Bytes>>(&self, range: T) -> Result<DbIterator, SlateDBError> {
        self.inner
            .scan_with_options(BytesRange::from(range), DEFAULT_SCAN_OPTIONS)
            .await
    }

    /// Scan a range of keys with the provided options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error scanning the range of keys
    ///
    /// ## Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use slatedb::{db::Db, config::ScanOptions, config::ReadLevel, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"a", b"a_value").await?;
    ///     db.put(b"b", b"b_value").await?;
    ///
    ///     let mut iter = db.scan_with_options(.., &ScanOptions {
    ///         read_level: ReadLevel::Uncommitted,
    ///         ..ScanOptions::default()
    ///     }).await?;
    ///     assert_eq!(Some((b"a" as &[u8], b"a_value" as &[u8]).into()) , iter.next().await?);
    ///     assert_eq!(Some((b"b" as &[u8], b"b_value" as &[u8]).into()) , iter.next().await?);
    ///     assert_eq!(None , iter.next().await?);
    ///     Ok(())
    /// }
    /// ```
    pub async fn scan_with_options<T: RangeBounds<Bytes>>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError> {
        self.inner
            .scan_with_options(BytesRange::from(range), options)
            .await
    }

    /// Write a value into the database with default `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to write
    /// - `value`: the value to write
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error writing the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), SlateDBError> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(batch).await
    }

    /// Write a value into the database with custom `PutOptions` and `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to write
    /// - `value`: the value to write
    /// - `put_opts`: the put options to use
    /// - `write_opts`: the write options to use
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error writing the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, config::{PutOptions, WriteOptions}, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put_with_options(b"key", b"value", &PutOptions::default(), &WriteOptions::default()).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn put_with_options(
        &self,
        key: &[u8],
        value: &[u8],
        put_opts: &PutOptions,
        write_opts: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        let mut batch = WriteBatch::new();
        batch.put_with_options(key, value, put_opts);
        self.write_with_options(batch, write_opts).await
    }

    /// Delete a key from the database with default `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to delete
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error deleting the key
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.delete(b"key").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn delete(&self, key: &[u8]) -> Result<(), SlateDBError> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(batch).await
    }

    /// Delete a key from the database with custom `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to delete
    /// - `options`: the write options to use
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error deleting the key
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, config::WriteOptions, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.delete_with_options(b"key", &WriteOptions::default()).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn delete_with_options(
        &self,
        key: &[u8],
        options: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_with_options(batch, options).await
    }

    /// Write a batch of put/delete operations atomically to the database. Batch writes
    /// block other gets and writes until the batch is written to the WAL (or memtable if
    /// WAL is disabled).
    ///
    /// ## Arguments
    /// - `batch`: the batch of put/delete operations to write
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error writing the batch
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{batch::WriteBatch, db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///
    ///     let mut batch = WriteBatch::new();
    ///     batch.put(b"key1", b"value1");
    ///     batch.put(b"key2", b"value2");
    ///     batch.delete(b"key1");
    ///     db.write(batch).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn write(&self, batch: WriteBatch) -> Result<(), SlateDBError> {
        self.write_with_options(batch, DEFAULT_WRITE_OPTIONS).await
    }

    /// Write a batch of put/delete operations atomically to the database. Batch writes
    /// block other gets and writes until the batch is written to the WAL (or memtable if
    /// WAL is disabled).
    ///
    /// ## Arguments
    /// - `batch`: the batch of put/delete operations to write
    /// - `options`: the write options to use
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error writing the batch
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{batch::WriteBatch, db::Db, config::WriteOptions, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///
    ///     let mut batch = WriteBatch::new();
    ///     batch.put(b"key1", b"value1");
    ///     batch.put(b"key2", b"value2");
    ///     batch.delete(b"key1");
    ///     db.write_with_options(batch, &WriteOptions::default()).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        self.inner.write_with_options(batch, options).await
    }

    /// Flush the database to disk.
    /// If WAL is enabled, flushes the WAL to disk.
    /// If WAL is disabled, flushes the memtables to disk.
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error flushing the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{db::Db, error::SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.flush().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn flush(&self) -> Result<(), SlateDBError> {
        if self.inner.wal_enabled() {
            self.inner.flush_wals().await
        } else {
            self.inner.flush_memtables().await
        }
    }

    pub fn metrics(&self) -> Arc<DbStats> {
        self.inner.db_stats.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::Bound::Included;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;
    use crate::cached_object_store::FsCacheStorage;
    use crate::config::{
        CompactorOptions, ObjectStoreCacheOptions, SizeTieredCompactionSchedulerOptions,
        DEFAULT_PUT_OPTIONS,
    };
    use crate::proptest_util::sample;
    use crate::proptest_util::{arbitrary, rng};
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst_iter::SstIterator;
    #[cfg(feature = "wal_disable")]
    use crate::test_utils::assert_iterator;
    use crate::test_utils::{gen_attrs, TestClock};

    use futures::{future::join_all, StreamExt};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use proptest::test_runner::{Config, TestRng, TestRunner};
    use tokio::runtime::Runtime;
    use tracing::info;

    #[tokio::test]
    async fn test_put_get_delete() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options(0, 1024, None),
            object_store,
        )
        .await
        .unwrap();
        let key = b"test_key";
        let value = b"test_value";
        kv_store.put(key, value).await.unwrap();
        kv_store.flush().await.unwrap();

        assert_eq!(
            kv_store.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
        kv_store.delete(key).await.unwrap();
        assert_eq!(None, kv_store.get(key).await.unwrap());
        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_with_object_store_cache_metrics() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut opts = test_db_options(0, 1024, None);
        let temp_dir = tempfile::Builder::new()
            .prefix("objstore_cache_test_")
            .tempdir()
            .unwrap();

        opts.object_store_cache_options.root_folder = Some(temp_dir.into_path());
        opts.object_store_cache_options.part_size_bytes = 1024;
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store_with_cache_metrics"),
            opts,
            object_store.clone(),
        )
        .await
        .unwrap();

        let access_count0 = kv_store.metrics().object_store_cache_part_access.get();
        let key = b"test_key";
        let value = b"test_value";
        kv_store.put(key, value).await.unwrap();
        kv_store.flush().await.unwrap();

        let got = kv_store.get(key).await.unwrap();
        let access_count1 = kv_store.metrics().object_store_cache_part_access.get();
        assert_eq!(got, Some(Bytes::from_static(value)));
        assert!(access_count1 > 0);
        assert!(access_count1 >= access_count0);
        assert!(kv_store.metrics().object_store_cache_part_hits.get() >= 1);
    }

    #[tokio::test]
    async fn test_get_with_object_store_cache_stored_files() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut opts = test_db_options(0, 1024, None);
        let temp_dir = tempfile::Builder::new()
            .prefix("objstore_cache_test_")
            .tempdir()
            .unwrap();
        let db_stats = Arc::new(DbStats::new());
        let part_size = 1024;
        let cache_storage = Arc::new(FsCacheStorage::new(
            temp_dir.path().to_path_buf(),
            None,
            None,
            db_stats.clone(),
        ));

        let cached_object_store = CachedObjectStore::new(
            object_store.clone(),
            cache_storage,
            part_size,
            db_stats.clone(),
        )
        .unwrap();

        opts.object_store_cache_options.root_folder = Some(temp_dir.into_path());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store_with_cache_stored_files"),
            opts,
            cached_object_store.clone(),
        )
        .await
        .unwrap();
        let key = b"test_key";
        let value = b"test_value";
        kv_store.put(key, value).await.unwrap();
        kv_store.flush().await.unwrap();

        assert_eq!(
            cached_object_store
                .list(None)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .filter_map(Result::ok)
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
            vec![
                "tmp/test_kv_store_with_cache_stored_files/manifest/00000000000000000001.manifest"
                    .to_string(),
                "tmp/test_kv_store_with_cache_stored_files/manifest/00000000000000000002.manifest"
                    .to_string(),
                "tmp/test_kv_store_with_cache_stored_files/wal/00000000000000000001.sst"
                    .to_string(),
                "tmp/test_kv_store_with_cache_stored_files/wal/00000000000000000002.sst"
                    .to_string(),
            ],
        );

        // check the files are cached as expected
        let tests = vec![
            (
                "tmp/test_kv_store_with_cache_stored_files/manifest/00000000000000000001.manifest",
                0,
            ),
            (
                "tmp/test_kv_store_with_cache_stored_files/manifest/00000000000000000002.manifest",
                0,
            ),
            (
                "tmp/test_kv_store_with_cache_stored_files/wal/00000000000000000001.sst",
                2,
            ),
            (
                "tmp/test_kv_store_with_cache_stored_files/wal/00000000000000000002.sst",
                0,
            ),
        ];
        for (path, expected) in tests {
            let entry = cached_object_store
                .cache_storage
                .entry(&object_store::path::Path::from(path), part_size);
            assert_eq!(
                entry.cached_parts().await.unwrap().len(),
                expected,
                "{}",
                path
            );
        }
    }

    async fn assert_ordered_scan_in_range(
        table: &BTreeMap<Bytes, Bytes>,
        range: &BytesRange,
        iter: &mut DbIterator<'_>,
    ) {
        let mut expected = table.range((range.start_bound().cloned(), range.end_bound().cloned()));

        loop {
            match (expected.next(), iter.next().await.unwrap()) {
                (None, None) => break,
                (Some((expected_key, expected_value)), Some(actual)) => {
                    assert_eq!(expected_key, &actual.key);
                    assert_eq!(expected_value, &actual.value);
                }
                (Some(expected_record), None) => {
                    panic!("Expected record {expected_record:?} missing from scan result")
                }
                (None, Some(actual)) => panic!("Unexpected record {actual:?} in scan result"),
            }
        }
    }

    async fn seed_database(db: &Db, table: &BTreeMap<Bytes, Bytes>, await_durable: bool) {
        let put_options = PutOptions::default();
        let write_options = &WriteOptions {
            await_durable,
            ..WriteOptions::default()
        };

        for (key, value) in table.iter() {
            db.put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
    }

    async fn build_database_from_table(table: &BTreeMap<Bytes, Bytes>, await_durable: bool) -> Db {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options(0, 1024, None),
            object_store,
        )
        .await
        .unwrap();

        seed_database(&db, &table, false).await;

        if await_durable {
            db.flush().await.unwrap();
        }

        db
    }

    async fn assert_empty_scan(db: &Db, range: BytesRange) {
        let mut iter = db
            .inner
            .scan_with_options(range.clone(), &ScanOptions::default())
            .await
            .unwrap();
        assert_eq!(None, iter.next().await.unwrap());
    }

    #[test]
    fn test_empty_scan_range_returns_empty_iterator() {
        let mut runner = new_proptest_runner(None);
        let table = sample::table(runner.rng(), 1000, 5);

        let runtime = Runtime::new().unwrap();
        let db = runtime.block_on(build_database_from_table(&table, true));

        runner
            .run(&arbitrary::empty_range(10), |range| {
                runtime.block_on(assert_empty_scan(&db, range));
                Ok(())
            })
            .unwrap();
    }

    async fn assert_records_in_range(
        table: &BTreeMap<Bytes, Bytes>,
        db: &Db,
        scan_options: &ScanOptions,
        range: BytesRange,
    ) {
        let mut iter = db
            .inner
            .scan_with_options(range.clone(), scan_options)
            .await
            .unwrap();
        assert_ordered_scan_in_range(&table, &range, &mut iter).await;
    }

    #[test]
    fn test_scan_returns_records_in_range() {
        let mut runner = new_proptest_runner(None);
        let table = sample::table(runner.rng(), 1000, 5);

        let runtime = Runtime::new().unwrap();
        let db = runtime.block_on(build_database_from_table(&table, true));

        runner
            .run(&arbitrary::nonempty_range(10), |range| {
                runtime.block_on(assert_records_in_range(
                    &table,
                    &db,
                    &ScanOptions::default(),
                    range,
                ));
                Ok(())
            })
            .unwrap();
    }

    fn new_proptest_runner(rng_seed: Option<[u8; 32]>) -> TestRunner {
        let rng = rng::new_test_rng(rng_seed);
        let mut config = proptest::test_runner::contextualize_config(Config::default().clone());
        config.source_file = Some(file!().into());
        TestRunner::new_with_rng(config, rng)
    }

    #[test]
    fn test_scan_returns_uncommitted_records_if_read_level_uncommitted() {
        let mut runner = new_proptest_runner(None);
        let table = sample::table(runner.rng(), 1000, 5);

        let runtime = Runtime::new().unwrap();
        let db = runtime.block_on(build_database_from_table(&table, false));

        runner
            .run(&arbitrary::nonempty_range(10), |range| {
                let scan_options = ScanOptions {
                    read_level: Uncommitted,
                    ..ScanOptions::default()
                };
                runtime.block_on(assert_records_in_range(&table, &db, &scan_options, range));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_seek_outside_of_range_returns_invalid_argument() {
        let mut runner = new_proptest_runner(None);
        let table = sample::table(runner.rng(), 1000, 10);

        let runtime = Runtime::new().unwrap();
        let db = runtime.block_on(build_database_from_table(&table, true));

        runner
            .run(
                &(arbitrary::nonempty_bytes(10), arbitrary::rng()),
                |(arbitrary_key, mut rng)| {
                    runtime.block_on(assert_out_of_bound_seek_returns_invalid_argument(
                        &db,
                        &mut rng,
                        arbitrary_key,
                    ));
                    Ok(())
                },
            )
            .unwrap();

        async fn assert_out_of_bound_seek_returns_invalid_argument(
            db: &Db,
            rng: &mut TestRng,
            arbitrary_key: Bytes,
        ) {
            let mut iter = db
                .scan_with_options(..arbitrary_key.clone(), &ScanOptions::default())
                .await
                .unwrap();

            let lower_bounded_range = BytesRange::from(arbitrary_key.clone()..);
            let value = sample::bytes_in_range(rng, &lower_bounded_range);
            assert!(matches!(
                iter.seek(value).await,
                Err(SlateDBError::InvalidArgument { msg: _ })
            ));

            let mut iter = db
                .scan_with_options(arbitrary_key.clone().., &ScanOptions::default())
                .await
                .unwrap();

            let upper_bounded_range = BytesRange::from(..arbitrary_key.clone());
            let value = sample::bytes_in_range(rng, &upper_bounded_range.into());
            assert!(matches!(
                iter.seek(value).await,
                Err(SlateDBError::InvalidArgument { msg: _ })
            ));
        }
    }

    #[test]
    fn test_seek_fast_forwards_iterator() {
        let mut runner = new_proptest_runner(None);
        let table = sample::table(runner.rng(), 1000, 10);

        let runtime = Runtime::new().unwrap();
        let db = runtime.block_on(build_database_from_table(&table, true));

        runner
            .run(
                &(arbitrary::nonempty_range(5), arbitrary::rng()),
                |(range, mut rng)| {
                    runtime.block_on(assert_seek_fast_forwards_iterator(
                        &table, &db, &range, &mut rng,
                    ));
                    Ok(())
                },
            )
            .unwrap();

        async fn assert_seek_fast_forwards_iterator(
            table: &BTreeMap<Bytes, Bytes>,
            db: &Db,
            scan_range: &BytesRange,
            rng: &mut TestRng,
        ) {
            let mut iter = db
                .inner
                .scan_with_options(scan_range.clone(), &ScanOptions::default())
                .await
                .unwrap();

            let seek_key = sample::bytes_in_range(rng, &scan_range);
            iter.seek(seek_key.clone()).await.unwrap();

            let seek_range = BytesRange::new(Included(seek_key), scan_range.end_bound().cloned());
            assert_ordered_scan_in_range(&table, &seek_range, &mut iter).await;
        }
    }

    #[tokio::test]
    async fn test_write_batch() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options(0, 1024, None),
            object_store,
        )
        .await
        .unwrap();

        // Create a new WriteBatch
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key1");

        // Write the batch
        kv_store.write(batch).await.expect("write batch failed");

        // Read back keys
        assert_eq!(kv_store.get(b"key1").await.unwrap(), None);
        assert_eq!(
            kv_store.get(b"key2").await.unwrap(),
            Some(Bytes::from_static(b"value2"))
        );

        kv_store.close().await.unwrap();
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn test_write_batch_without_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Use a very small l0 size to force flushes so await is notified
        let mut options = test_db_options(0, 8, None);

        // Disable WAL
        options.wal_enabled = false;

        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store_without_wal"),
            options,
            object_store,
        )
        .await
        .unwrap();

        // Create a new WriteBatch
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key1");

        // Write the batch
        kv_store.write(batch).await.expect("write batch failed");

        // Read back keys
        assert_eq!(kv_store.get(b"key1").await.unwrap(), None);
        assert_eq!(
            kv_store.get(b"key2").await.unwrap(),
            Some(Bytes::from_static(b"value2"))
        );

        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_batch_with_empty_key() {
        let mut batch = WriteBatch::new();
        let result = std::panic::catch_unwind(move || {
            batch.put(b"", b"value");
        });
        assert!(
            result.is_err(),
            "Expected panic when using empty key in put operation"
        );

        let mut batch = WriteBatch::new();
        let result = std::panic::catch_unwind(move || {
            batch.delete(b"");
        });
        assert!(
            result.is_err(),
            "Expected panic when using empty key in delete operation"
        );
    }

    /// Test that batch writes are atomic. Test does the following:
    ///
    /// - A set of, say 100 keys, 1-100
    /// - Two tasks writing, one writing value to be same key, and other
    ///   writing it to be key*2.
    /// - We wait for both to complete.
    /// - Assert that either all values are same as key, or all values key*2.
    /// - Repeat above loop few times.
    ///
    /// _Note: This test is non-deterministic because it depends on the async
    /// runtime to schedule the tasks in a way that the writes are concurrent._
    #[tokio::test]
    async fn test_concurrent_batch_writes_consistency() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Arc::new(
            Db::open_with_opts(
                Path::from("/tmp/test_concurrent_kv_store"),
                test_db_options(
                    0,
                    1024,
                    // Enable compactor to prevent l0 from filling up and
                    // applying backpressure indefinitely.
                    Some(CompactorOptions {
                        poll_interval: Duration::from_millis(100),
                        max_sst_size: 256,
                        compaction_scheduler: Arc::new(SizeTieredCompactionSchedulerSupplier::new(
                            SizeTieredCompactionSchedulerOptions::default(),
                        )),
                        max_concurrent_compactions: 1,
                        compaction_runtime: None,
                    }),
                ),
                object_store.clone(),
            )
            .await
            .unwrap(),
        );

        const NUM_KEYS: usize = 100;
        const NUM_ROUNDS: usize = 20;

        for _ in 0..NUM_ROUNDS {
            // Write two tasks that write to the same keys
            let task1 = {
                let store = kv_store.clone();
                tokio::spawn(async move {
                    let mut batch = WriteBatch::new();
                    for key in 1..=NUM_KEYS {
                        batch.put(&key.to_be_bytes(), &key.to_be_bytes());
                    }
                    store.write(batch).await.expect("write batch failed");
                })
            };

            let task2 = {
                let store = kv_store.clone();
                tokio::spawn(async move {
                    let mut batch = WriteBatch::new();
                    for key in 1..=NUM_KEYS {
                        let value = (key * 2).to_be_bytes();
                        batch.put(&key.to_be_bytes(), &value);
                    }
                    store.write(batch).await.expect("write batch failed");
                })
            };

            // Wait for both tasks to complete
            join_all(vec![task1, task2]).await;

            // Ensure consistency: all values must be either key or key * 2
            let mut all_key = true;
            let mut all_key2 = true;

            for key in 1..=NUM_KEYS {
                let value = kv_store.get(&key.to_be_bytes()).await.unwrap();
                let value = value.expect("Value should exist");

                if value.as_ref() != key.to_be_bytes() {
                    all_key = false;
                }
                if value.as_ref() != (key * 2).to_be_bytes() {
                    all_key2 = false;
                }
            }

            // Assert that the result is consistent: either all key or all key * 2
            assert!(
                all_key || all_key2,
                "Inconsistent state: not all values match either key or key * 2"
            );
        }

        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_disable_wal_after_wal_enabled() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        // open a db and write a wal entry
        let options = test_db_options(0, 32, None);
        let db = Db::open_with_opts(path.clone(), options, object_store.clone())
            .await
            .unwrap();
        db.put(&[b'a'; 4], &[b'j'; 4]).await.unwrap();
        db.put(&[b'b'; 4], &[b'k'; 4]).await.unwrap();
        db.close().await.unwrap();

        // open a db with wal disabled and write a memtable
        let mut options = test_db_options(0, 32, None);
        options.wal_enabled = false;
        let db = Db::open_with_opts(path.clone(), options.clone(), object_store.clone())
            .await
            .unwrap();
        db.delete_with_options(
            &[b'b'; 4],
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.put(&[b'a'; 4], &[b'z'; 64]).await.unwrap();
        db.close().await.unwrap();

        // ensure we don't overwrite the values we just put on a reload
        let db = Db::open_with_opts(path.clone(), options.clone(), object_store.clone())
            .await
            .unwrap();
        let val = db.get(&[b'a'; 4]).await.unwrap();
        assert_eq!(val.unwrap(), Bytes::copy_from_slice(&[b'z'; 64]));
        let val = db.get(&[b'b'; 4]).await.unwrap();
        assert!(val.is_none());
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn test_wal_disabled() {
        use crate::test_utils::gen_empty_attrs;

        let clock = Arc::new(TestClock::new());
        let mut options = test_db_options_with_clock(0, 128, None, clock.clone());
        options.wal_enabled = false;
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let sst_format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            object_store.clone(),
            sst_format,
            path.clone(),
            None,
        ));
        let db = Db::open_with_opts(path.clone(), options, object_store.clone())
            .await
            .unwrap();
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        let write_options = WriteOptions {
            await_durable: false,
        };

        db.put_with_options(
            &[b'a'; 32],
            &[b'j'; 32],
            DEFAULT_PUT_OPTIONS,
            &write_options,
        )
        .await
        .unwrap();
        db.delete_with_options(&[b'b'; 32], &write_options)
            .await
            .unwrap();
        let write_options = WriteOptions {
            await_durable: true,
        };
        clock.ticker.store(10, Ordering::SeqCst);
        db.put_with_options(
            &[b'c'; 32],
            &[b'l'; 32],
            DEFAULT_PUT_OPTIONS,
            &write_options,
        )
        .await
        .unwrap();

        let state = wait_for_manifest_condition(
            &mut stored_manifest,
            |s| !s.l0.is_empty(),
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(state.l0.len(), 1);

        let l0 = state.l0.front().unwrap();
        let mut iter = SstIterator::new(l0, table_store.clone(), 1, 1, false)
            .await
            .unwrap();
        assert_iterator(
            &mut iter,
            &[
                (
                    vec![b'a'; 32],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[b'j'; 32])),
                    gen_attrs(0),
                ),
                (vec![b'b'; 32], ValueDeletable::Tombstone, gen_empty_attrs()),
                (
                    vec![b'c'; 32],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[b'l'; 32])),
                    gen_attrs(10),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_put_flushes_memtable() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        let sst_format = SsTableFormat {
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            object_store.clone(),
            sst_format,
            path.clone(),
            None,
        ));

        // Write data a few times such that each loop results in a memtable flush
        let mut last_compacted = 0;
        for i in 0..3 {
            let key = [b'a' + i; 16];
            let value = [b'b' + i; 50];
            kv_store.put(&key, &value).await.unwrap();
            let key = [b'j' + i; 16];
            let value = [b'k' + i; 50];
            kv_store.put(&key, &value).await.unwrap();
            let db_state = wait_for_manifest_condition(
                &mut stored_manifest,
                |s| s.last_compacted_wal_sst_id > last_compacted,
                Duration::from_secs(30),
            )
            .await;

            // 1 empty wal at startup + 2 wal per iteration.
            assert_eq!(db_state.last_compacted_wal_sst_id, 1 + (i as u64) * 2 + 2);
            last_compacted = db_state.last_compacted_wal_sst_id
        }

        let db_state = stored_manifest.refresh().await.unwrap();
        let l0 = &db_state.l0;
        assert_eq!(l0.len(), 3);
        for i in 0u8..3u8 {
            let sst1 = l0.get(2 - i as usize).unwrap();
            let mut iter = SstIterator::new(sst1, table_store.clone(), 1, 1, true)
                .await
                .unwrap();
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key.as_ref(), [b'a' + i; 16]);
            assert_eq!(kv.value.as_ref(), [b'b' + i; 50]);
            let kv = iter.next().await.unwrap().unwrap();
            assert_eq!(kv.key.as_ref(), [b'j' + i; 16]);
            assert_eq!(kv.value.as_ref(), [b'k' + i; 50]);
            let kv = iter.next().await.unwrap();
            assert!(kv.is_none());
        }
        assert!(kv_store.metrics().immutable_memtable_flushes.get() > 0);
    }

    // 2 threads so we can can wait on the write_with_options (main) thread
    // while the write_batch (background) thread is blocked on writing the
    // WAL SST.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_apply_wal_memory_backpressure() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let mut options = test_db_options(0, 1, None);
        options.max_unflushed_bytes = 1;
        let db = Db::open_with_fp_registry(
            path.clone(),
            options,
            object_store.clone(),
            fp_registry.clone(),
        )
        .await
        .unwrap();

        let write_opts = WriteOptions {
            await_durable: false,
        };

        // Block WAL flush
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();

        // 1 imm_wal in memory
        db.put_with_options(b"key1", b"val1", &PutOptions::default(), &write_opts)
            .await
            .unwrap();

        let snapshot = db.inner.state.read().snapshot();

        // Unblock WAL flush so runtime shuts down nicely even if we have a failure
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "off").unwrap();

        // WAL should pile up in memory since it can't be flushed
        assert_eq!(snapshot.state.imm_wal.len(), 1);
    }

    #[tokio::test]
    async fn test_apply_backpressure_to_memtable_flush() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut options = test_db_options(0, 1, None);
        options.l0_max_ssts = 4;
        let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
            .await
            .unwrap();
        db.put(b"key1", b"val1").await.unwrap();
        db.put(b"key2", b"val2").await.unwrap();
        db.put(b"key3", b"val3").await.unwrap();
        db.put(b"key4", b"val4").await.unwrap();
        db.put(b"key5", b"val5").await.unwrap();

        db.flush().await.unwrap();

        let snapshot = db.inner.state.read().snapshot();
        assert_eq!(snapshot.state.imm_memtable.len(), 1);
    }

    #[tokio::test]
    async fn test_put_empty_value() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options(0, 1024, None),
            object_store,
        )
        .await
        .unwrap();
        let key = b"test_key";
        let value = b"";
        kv_store.put(key, value).await.unwrap();
        kv_store.flush().await.unwrap();

        assert_eq!(
            kv_store.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
    }

    #[tokio::test]
    async fn test_flush_while_iterating() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options(0, 1024, None),
            object_store,
        )
        .await
        .unwrap();

        let memtable = {
            let mut lock = kv_store.inner.state.write();
            lock.wal().put(
                Bytes::copy_from_slice(b"abc1111"),
                Bytes::copy_from_slice(b"value1111"),
                gen_attrs(1),
            );
            lock.wal().put(
                Bytes::copy_from_slice(b"abc2222"),
                Bytes::copy_from_slice(b"value2222"),
                gen_attrs(2),
            );
            lock.wal().put(
                Bytes::copy_from_slice(b"abc3333"),
                Bytes::copy_from_slice(b"value3333"),
                gen_attrs(3),
            );
            lock.wal().table().clone()
        };

        let mut iter = memtable.iter();
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc1111".as_slice());

        kv_store.flush().await.unwrap();

        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc2222".as_slice());

        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc3333".as_slice());
    }

    #[tokio::test]
    async fn test_basic_restore() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let mut next_wal_id = 1;
        let kv_store = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        // increment wal id for the empty wal
        next_wal_id += 1;

        // do a few writes that will result in l0 flushes
        let l0_count: u64 = 3;
        for i in 0..l0_count {
            kv_store
                .put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48])
                .await
                .unwrap();
            kv_store
                .put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48])
                .await
                .unwrap();
            next_wal_id += 2;
        }

        // write some smaller keys so that we populate wal without flushing to l0
        let sst_count: u64 = 5;
        for i in 0..sst_count {
            kv_store
                .put(&i.to_be_bytes(), &i.to_be_bytes())
                .await
                .unwrap();
            kv_store.flush().await.unwrap();
            next_wal_id += 1;
        }

        kv_store.close().await.unwrap();

        // recover and validate that sst files are loaded on recovery.
        let kv_store_restored = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        // increment wal id for the empty wal
        next_wal_id += 1;

        for i in 0..l0_count {
            let val = kv_store_restored.get(&[b'a' + i as u8; 16]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[b'b' + i as u8; 48])));
            let val = kv_store_restored.get(&[b'j' + i as u8; 16]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[b'k' + i as u8; 48])));
        }
        for i in 0..sst_count {
            let val = kv_store_restored.get(&i.to_be_bytes()).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&i.to_be_bytes())));
        }
        kv_store_restored.close().await.unwrap();

        // validate that the manifest file exists.
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let stored_manifest = StoredManifest::load(manifest_store).await.unwrap();
        let db_state = stored_manifest.db_state();
        assert_eq!(db_state.next_wal_sst_id, next_wal_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_should_read_uncommitted_data_if_read_level_uncommitted() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open_with_fp_registry(
            path.clone(),
            test_db_options(0, 1024, None),
            object_store.clone(),
            fp_registry.clone(),
        )
        .await
        .unwrap();

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();
        kv_store
            .put_with_options(
                "foo".as_bytes(),
                "bar".as_bytes(),
                DEFAULT_PUT_OPTIONS,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Validate uncommitted read
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions {
                    read_level: Uncommitted,
                },
            )
            .await
            .unwrap();
        assert_eq!(val, Some(Bytes::from("bar")));

        // Validate committed read should still return None
        let val = kv_store.get("foo".as_bytes()).await.unwrap();
        assert_eq!(val, None);

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "off").unwrap();
        kv_store.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_should_read_only_committed_data() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open_with_fp_registry(
            path.clone(),
            test_db_options(0, 1024, None),
            object_store.clone(),
            fp_registry.clone(),
        )
        .await
        .unwrap();

        kv_store
            .put("foo".as_bytes(), "bar".as_bytes())
            .await
            .unwrap();
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();
        kv_store
            .put_with_options(
                "foo".as_bytes(),
                "bla".as_bytes(),
                DEFAULT_PUT_OPTIONS,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let val = kv_store.get("foo".as_bytes()).await.unwrap();
        assert_eq!(val, Some(Bytes::from("bar")));
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions {
                    read_level: Uncommitted,
                },
            )
            .await
            .unwrap();
        assert_eq!(val, Some(Bytes::from("bla")));

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "off").unwrap();
        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_should_delete_without_awaiting_flush() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open_with_fp_registry(
            path.clone(),
            test_db_options(0, 1024, None),
            object_store.clone(),
            fp_registry.clone(),
        )
        .await
        .unwrap();

        kv_store
            .put("foo".as_bytes(), "bar".as_bytes())
            .await
            .unwrap();
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();
        kv_store
            .delete_with_options(
                "foo".as_bytes(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let val = kv_store.get("foo".as_bytes()).await.unwrap();
        assert_eq!(val, Some(Bytes::from("bar")));
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions {
                    read_level: Uncommitted,
                },
            )
            .await
            .unwrap();
        assert_eq!(val, None);

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "off").unwrap();
        kv_store.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_recover_imm_from_wal() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "pause").unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let mut next_wal_id = 1;
        let db = Db::open_with_fp_registry(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
            fp_registry.clone(),
        )
        .await
        .unwrap();
        next_wal_id += 1;

        // write a few keys that will result in memtable flushes
        let key1 = [b'a'; 32];
        let value1 = [b'b'; 96];
        db.put(&key1, &value1).await.unwrap();
        next_wal_id += 1;
        let key2 = [b'c'; 32];
        let value2 = [b'd'; 96];
        db.put(&key2, &value2).await.unwrap();
        next_wal_id += 1;

        let reader = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        // increment wal id for the empty wal
        next_wal_id += 1;

        // verify that we reload imm
        let snapshot = reader.inner.state.read().snapshot();
        assert_eq!(snapshot.state.imm_memtable.len(), 2);

        // one empty wal and two wals for the puts
        assert_eq!(
            snapshot.state.imm_memtable.front().unwrap().last_wal_id(),
            1 + 2
        );
        assert_eq!(snapshot.state.imm_memtable.get(1).unwrap().last_wal_id(), 2);
        assert_eq!(snapshot.state.core.next_wal_sst_id, next_wal_id);
        assert_eq!(
            reader.get(&key1).await.unwrap(),
            Some(Bytes::copy_from_slice(&value1))
        );
        assert_eq!(
            reader.get(&key2).await.unwrap(),
            Some(Bytes::copy_from_slice(&value2))
        );

        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();
        db.close().await.unwrap();
        reader.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_recover_imm_from_wal_after_flush_error() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        fail_parallel::cfg(
            fp_registry.clone(),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Db::open_with_fp_registry(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
            fp_registry.clone(),
        )
        .await
        .unwrap();

        // write a few keys that will result in memtable flushes
        let key1 = [b'a'; 32];
        let value1 = [b'b'; 96];
        let result = db.put(&key1, &value1).await;
        assert!(result.is_ok(), "Failed to write key1");

        let flush_result = db.inner.flush_memtables().await;
        match flush_result {
            Err(e) => assert!(matches!(e, SlateDBError::IoError(_))),
            _ => panic!("Expected flush error"),
        }
        db.close().await.unwrap();

        // reload the db
        let db = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        // verify that we reload imm
        let snapshot = db.inner.state.read().snapshot();
        assert_eq!(snapshot.state.imm_memtable.len(), 1);

        // one empty wal and one wal for the first put
        assert_eq!(
            snapshot.state.imm_memtable.front().unwrap().last_wal_id(),
            1 + 1
        );
        assert!(snapshot.state.imm_memtable.get(1).is_none());

        assert_eq!(snapshot.state.core.next_wal_sst_id, 4);
        assert_eq!(
            db.get(&key1).await.unwrap(),
            Some(Bytes::copy_from_slice(&value1))
        );
    }

    async fn do_test_should_read_compacted_db(options: DbOptions) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Db::open_with_opts(path.clone(), options, object_store.clone())
            .await
            .unwrap();
        let ms = ManifestStore::new(&path, object_store.clone());
        let mut sm = StoredManifest::load(Arc::new(ms)).await.unwrap();

        // write enough to fill up a few l0 SSTs
        for i in 0..4 {
            db.put(&[b'a' + i; 32], &[1u8 + i; 32]).await.unwrap();
            db.put(&[b'm' + i; 32], &[13u8 + i; 32]).await.unwrap();
        }
        // wait for compactor to compact them
        wait_for_manifest_condition(
            &mut sm,
            |s| s.l0_last_compacted.is_some() && s.l0.is_empty(),
            Duration::from_secs(10),
        )
        .await;
        info!(
            "1 l0: {} {}",
            db.inner.state.read().state().core.l0.len(),
            db.inner.state.read().state().core.compacted.len()
        );
        // write more l0s and wait for compaction
        for i in 0..4 {
            db.put(&[b'f' + i; 32], &[6u8 + i; 32]).await.unwrap();
            db.put(&[b's' + i; 32], &[19u8 + i; 32]).await.unwrap();
        }
        wait_for_manifest_condition(
            &mut sm,
            |s| s.l0_last_compacted.is_some() && s.l0.is_empty(),
            Duration::from_secs(10),
        )
        .await;
        info!(
            "2 l0: {} {}",
            db.inner.state.read().state().core.l0.len(),
            db.inner.state.read().state().core.compacted.len()
        );
        // write another l0
        db.put(&[b'a'; 32], &[128u8; 32]).await.unwrap();
        db.put(&[b'm'; 32], &[129u8; 32]).await.unwrap();

        let val = db.get(&[b'a'; 32]).await.unwrap();
        assert_eq!(val, Some(Bytes::copy_from_slice(&[128u8; 32])));
        let val = db.get(&[b'm'; 32]).await.unwrap();
        assert_eq!(val, Some(Bytes::copy_from_slice(&[129u8; 32])));
        for i in 1..4 {
            info!(
                "3 l0: {} {}",
                db.inner.state.read().state().core.l0.len(),
                db.inner.state.read().state().core.compacted.len()
            );
            let val = db.get(&[b'a' + i; 32]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[1u8 + i; 32])));
            let val = db.get(&[b'm' + i; 32]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[13u8 + i; 32])));
        }
        for i in 0..4 {
            let val = db.get(&[b'f' + i; 32]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[6u8 + i; 32])));
            let val = db.get(&[b's' + i; 32]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[19u8 + i; 32])));
        }
        let neg_lookup = db.get(b"abc").await;
        assert!(neg_lookup.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_should_read_from_compacted_db() {
        do_test_should_read_compacted_db(test_db_options(
            0,
            127,
            Some(CompactorOptions {
                poll_interval: Duration::from_millis(100),
                max_sst_size: 256,
                compaction_scheduler: Arc::new(SizeTieredCompactionSchedulerSupplier::new(
                    SizeTieredCompactionSchedulerOptions::default(),
                )),
                max_concurrent_compactions: 1,
                compaction_runtime: None,
            }),
        ))
        .await;
    }

    #[tokio::test]
    async fn test_should_read_from_compacted_db_no_filters() {
        do_test_should_read_compacted_db(test_db_options(
            u32::MAX,
            127,
            Some(CompactorOptions {
                poll_interval: Duration::from_millis(100),
                max_sst_size: 256,
                compaction_scheduler: Arc::new(SizeTieredCompactionSchedulerSupplier::new(
                    SizeTieredCompactionSchedulerOptions::default(),
                )),
                max_concurrent_compactions: 1,
                compaction_runtime: None,
            }),
        ))
        .await
    }

    #[tokio::test]
    async fn test_db_open_should_write_empty_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        // assert that open db writes an empty wal.
        let db = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        assert_eq!(db.inner.state.read().state().core.next_wal_sst_id, 2);
        db.put(b"1", b"1").await.unwrap();
        // assert that second open writes another empty wal.
        let db = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        assert_eq!(db.inner.state.read().state().core.next_wal_sst_id, 4);
    }

    #[tokio::test]
    async fn test_empty_wal_should_fence_old_writer() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        async fn do_put(db: &Db, key: &[u8], val: &[u8]) -> Result<(), SlateDBError> {
            db.put_with_options(
                key,
                val,
                DEFAULT_PUT_OPTIONS,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await?;
            db.flush().await
        }

        // open db1 and assert that it can write.
        let db1 = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        do_put(&db1, b"1", b"1").await.unwrap();

        // open db2, causing it to write an empty wal and fence db1.
        let db2 = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        // assert that db1 can no longer write.
        let err = do_put(&db1, b"1", b"1").await;
        assert!(matches!(err, Err(SlateDBError::Fenced)));

        do_put(&db2, b"2", b"2").await.unwrap();
        assert_eq!(db2.inner.state.read().state().core.next_wal_sst_id, 5);
    }

    #[tokio::test]
    async fn test_invalid_clock_progression() {
        // Given:
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let clock = Arc::new(TestClock::new());
        let db = Db::open_with_opts(
            path.clone(),
            DbOptions {
                clock: clock.clone(),
                ..test_db_options(0, 128, None)
            },
            object_store.clone(),
        )
        .await
        .unwrap();

        // When:
        // put with time = 10
        clock.ticker.store(10, Ordering::SeqCst);
        db.put(b"1", b"1").await.unwrap();

        // Then:
        // put with time goes backwards, should fail
        clock.ticker.store(5, Ordering::SeqCst);
        match db.put(b"1", b"1").await {
            Ok(_) => panic!("expected an error on inserting backwards time"),
            Err(e) => assert!(
                e.to_string().contains("Last tick: 10, Next tick: 5"),
                "{}",
                e.to_string()
            ),
        }
    }

    #[tokio::test]
    async fn test_invalid_clock_progression_across_db_instances() {
        // Given:
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let clock = Arc::new(TestClock::new());
        let db = Db::open_with_opts(
            path.clone(),
            DbOptions {
                clock: clock.clone(),
                ..test_db_options(0, 128, None)
            },
            object_store.clone(),
        )
        .await
        .unwrap();

        // When:
        // put with time = 10
        clock.ticker.store(10, Ordering::SeqCst);
        db.put(b"1", b"1").await.unwrap();
        db.flush().await.unwrap();

        let db2 = Db::open_with_opts(
            path.clone(),
            DbOptions {
                clock: clock.clone(),
                ..test_db_options(0, 128, None)
            },
            object_store.clone(),
        )
        .await
        .unwrap();
        clock.ticker.store(5, Ordering::SeqCst);
        match db2.put(b"1", b"1").await {
            Ok(_) => panic!("expected an error on inserting backwards time"),
            Err(e) => assert!(
                e.to_string().contains("Last tick: 10, Next tick: 5"),
                "{}",
                e.to_string()
            ),
        }
    }

    async fn wait_for_manifest_condition(
        sm: &mut StoredManifest,
        cond: impl Fn(&CoreDbState) -> bool,
        timeout: Duration,
    ) -> CoreDbState {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let db_state = sm.refresh().await.unwrap();
            if cond(db_state) {
                return db_state.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("manifest condition took longer than timeout")
    }

    fn test_db_options(
        min_filter_keys: u32,
        l0_sst_size_bytes: usize,
        compactor_options: Option<CompactorOptions>,
    ) -> DbOptions {
        test_db_options_with_clock(
            min_filter_keys,
            l0_sst_size_bytes,
            compactor_options,
            Arc::new(TestClock::new()),
        )
    }

    fn test_db_options_with_clock(
        min_filter_keys: u32,
        l0_sst_size_bytes: usize,
        compactor_options: Option<CompactorOptions>,
        clock: Arc<TestClock>,
    ) -> DbOptions {
        DbOptions {
            flush_interval: Duration::from_millis(100),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            max_unflushed_bytes: 134_217_728,
            l0_max_ssts: 8,
            min_filter_keys,
            filter_bits_per_key: 10,
            l0_sst_size_bytes,
            compactor_options,
            compression_codec: None,
            object_store_cache_options: ObjectStoreCacheOptions::default(),
            block_cache: None,
            garbage_collector_options: None,
            clock,
            default_ttl: None,
        }
    }
}
