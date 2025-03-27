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
//! use slatedb::{Db, SlateDBError};
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

use std::collections::HashMap;
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
use crate::cached_object_store::stats::CachedObjectStoreStats;
use crate::cached_object_store::CachedObjectStore;
use crate::cached_object_store::FsCacheStorage;
use crate::compactor::Compactor;
use crate::config::{DbOptions, PutOptions, ReadOptions, ScanOptions, WriteOptions};
use crate::db_cache::{DbCache, DbCacheWrapper};
use crate::db_iter::DbIterator;
use crate::db_state::{CoreDbState, DbState, SsTableId};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::flush::WalFlushMsg;
use crate::garbage_collector::GarbageCollector;
use crate::manifest::store::{DirtyManifest, FenceableManifest, ManifestStore, StoredManifest};
use crate::mem_table::WritableKVTable;
use crate::mem_table_flush::MemtableFlushMsg;
use crate::paths::PathResolver;
use crate::reader::Reader;
use crate::sst::SsTableFormat;
use crate::sst_iter::SstIteratorOptions;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::utils::{bg_task_result_into_err, MonotonicClock};
use crate::wal_replay::{WalReplayIterator, WalReplayOptions};
use tracing::{info, warn};

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<DbState>>,
    pub(crate) options: DbOptions,
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) wal_flush_notifier: UnboundedSender<WalFlushMsg>,
    pub(crate) memtable_flush_notifier: UnboundedSender<MemtableFlushMsg>,
    pub(crate) write_notifier: UnboundedSender<WriteBatchMsg>,
    pub(crate) db_stats: DbStats,
    pub(crate) stat_registry: Arc<StatRegistry>,
    pub(crate) mono_clock: Arc<MonotonicClock>,
    pub(crate) reader: Reader,
}

impl DbInner {
    pub async fn new(
        options: DbOptions,
        table_store: Arc<TableStore>,
        manifest: DirtyManifest,
        wal_flush_notifier: UnboundedSender<WalFlushMsg>,
        memtable_flush_notifier: UnboundedSender<MemtableFlushMsg>,
        write_notifier: UnboundedSender<WriteBatchMsg>,
        stat_registry: Arc<StatRegistry>,
    ) -> Result<Self, SlateDBError> {
        let mono_clock = Arc::new(MonotonicClock::new(
            options.clock.clone(),
            manifest.core.last_l0_clock_tick,
        ));
        let state = Arc::new(RwLock::new(DbState::new(manifest)));
        let db_stats = DbStats::new(stat_registry.as_ref());

        let reader = Reader {
            table_store: Arc::clone(&table_store),
            db_stats: db_stats.clone(),
            mono_clock: Arc::clone(&mono_clock),
            wal_enabled: DbInner::wal_enabled_in_options(&options)
        };

        let db_inner = Self {
            state,
            options,
            table_store,
            wal_flush_notifier,
            memtable_flush_notifier,
            write_notifier,
            db_stats,
            mono_clock,
            stat_registry,
            reader,
        };
        Ok(db_inner)
    }

    /// Get the value for a given key.
    pub async fn get_with_options<K: AsRef<[u8]>>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.check_error()?;
        let snapshot = self.state.read().snapshot();
        self.reader.get_with_options(key, options, &snapshot).await
    }

    pub async fn scan_with_options<'a>(
        &'a self,
        range: BytesRange,
        options: &ScanOptions,
    ) -> Result<DbIterator<'a>, SlateDBError> {
        self.check_error()?;
        let snapshot = self.state.read().snapshot();
        self.reader
            .scan_with_options(range, options, &snapshot)
            .await
    }

    /// Fences all writers with an older epoch than the provided `manifest` by flushing an empty WAL file that acts
    /// as a barrier. Any parallel old writers will fail with `SlateDBError::Fenced` when trying to "re-write" this file.
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
                    manifest.refresh().await?;
                    self.state
                        .write()
                        .merge_remote_manifest(manifest.prepare_dirty()?);
                    empty_wal_id += 1;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub(crate) fn wal_enabled(&self) -> bool {
        Self::wal_enabled_in_options(&self.options)
    }

    pub(crate) fn wal_enabled_in_options(options: &DbOptions) -> bool {
        #[cfg(feature = "wal_disable")]
        return options.wal_enabled;
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

        self.maybe_apply_backpressure().await?;
        self.write_notifier
            .send(batch_msg)
            .expect("write notifier closed");

        // if the write pipeline task exits then this call to rx.await will fail because tx is dropped
        let current_table = rx.await??;

        if options.await_durable {
            current_table.await_durable().await?;
        }

        Ok(())
    }

    #[inline]
    pub(crate) async fn maybe_apply_backpressure(&self) -> Result<(), SlateDBError> {
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
                            result = wal_table.await_durable() => {
                                result?;
                            }
                            result = mem_table.await_flush_to_l0() => {
                                result?;
                            }
                        }
                    }
                    (Some(wal_table), None) => {
                        wal_table.await_durable().await?;
                    }
                    (None, Some(mem_table)) => {
                        mem_table.await_flush_to_l0().await?;
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
        Ok(())
    }

    async fn flush_wals(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.wal_flush_notifier
            .send(WalFlushMsg::FlushImmutableWals { sender: Some(tx) })
            .map_err(|_| SlateDBError::WalFlushChannelError)?;
        rx.await?
    }

    // use to manually flush memtables
    async fn flush_immutable_memtables(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.memtable_flush_notifier
            .send(MemtableFlushMsg::FlushImmutableMemtables { sender: Some(tx) })
            .map_err(|_| SlateDBError::MemtableFlushChannelError)?;
        rx.await?
    }

    async fn flush_memtables(&self) -> Result<(), SlateDBError> {
        {
            let mut guard = self.state.write();
            if !guard.memtable().is_empty() {
                let last_wal_id = guard.last_written_wal_id();
                guard.freeze_memtable(last_wal_id)?;
            }
        }
        self.flush_immutable_memtables().await
    }

    async fn replay_wal(&self) -> Result<(), SlateDBError> {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: 256,
            cache_blocks: true,
            eager_spawn: true,
        };

        let replay_options = WalReplayOptions {
            sst_batch_size: 4,
            min_memtable_bytes: self.options.l0_sst_size_bytes,
            max_memtable_bytes: usize::MAX,
            sst_iter_options,
        };

        let db_state = self.state.read().state().core().clone();
        let mut replay_iter =
            WalReplayIterator::new(&db_state, replay_options, Arc::clone(&self.table_store))
                .await?;

        while let Some(replayed_table) = replay_iter.next().await? {
            self.replay_memtable(replayed_table)?;
        }

        Ok(())
    }

    /// Return an error if the state has encountered
    /// an unrecoverable error.
    pub(crate) fn check_error(&self) -> Result<(), SlateDBError> {
        let error_reader = {
            let state = self.state.read();
            state.error_reader()
        };
        if let Some(error) = error_reader.read() {
            return Err(error.clone());
        }
        Ok(())
    }
}

pub struct Db {
    pub(crate) inner: Arc<DbInner>,
    /// The handle for the flush thread.
    wal_flush_task: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
    memtable_flush_task: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
    write_task: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
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
    /// use slatedb::{Db, SlateDBError};
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
    /// use slatedb::{Db, config::DbOptions, SlateDBError};
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
    /// use slatedb::{Db, config::DbOptions, SlateDBError};
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

        let stat_registry = Arc::new(StatRegistry::new());
        let sst_format = SsTableFormat {
            min_filter_keys: options.min_filter_keys,
            filter_bits_per_key: options.filter_bits_per_key,
            compression_codec: options.compression_codec,
            ..SsTableFormat::default()
        };
        let maybe_cached_object_store = match &options.object_store_cache_options.root_folder {
            None => object_store.clone(),
            Some(cache_root_folder) => {
                let stats = Arc::new(CachedObjectStoreStats::new(stat_registry.as_ref()));
                let cache_storage = Arc::new(FsCacheStorage::new(
                    cache_root_folder.clone(),
                    options.object_store_cache_options.max_cache_size_bytes,
                    options.object_store_cache_options.scan_interval,
                    stats.clone(),
                ));

                let cached_object_store = CachedObjectStore::new(
                    object_store.clone(),
                    cache_storage,
                    options.object_store_cache_options.part_size_bytes,
                    stats.clone(),
                )?;
                cached_object_store.start_evictor().await;
                cached_object_store
            }
        };

        let manifest_store = Arc::new(ManifestStore::new(&path, maybe_cached_object_store.clone()));
        let latest_manifest = StoredManifest::try_load(manifest_store.clone()).await?;

        let external_ssts = match &latest_manifest {
            Some(latest_stored_manifest) => {
                let mut external_ssts = HashMap::new();
                for external_db in &latest_stored_manifest.manifest().external_dbs {
                    for id in &external_db.sst_ids {
                        external_ssts.insert(*id, external_db.path.clone().into());
                    }
                }
                external_ssts
            }
            None => HashMap::new(),
        };

        let path_resolver = PathResolver::new_with_external_ssts(path.clone(), external_ssts);
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            maybe_cached_object_store.clone(),
            sst_format.clone(),
            path_resolver.clone(),
            fp_registry.clone(),
            options.block_cache.as_ref().map(|c| {
                Arc::new(DbCacheWrapper::new(c.clone(), stat_registry.as_ref())) as Arc<dyn DbCache>
            }),
        ));

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
                manifest.prepare_dirty()?,
                wal_flush_tx,
                memtable_flush_tx,
                write_tx,
                stat_registry,
            )
            .await?,
        );
        if inner.wal_enabled() {
            inner.fence_writers(&mut manifest, next_wal_id).await?;
        }
        inner.replay_wal().await?;
        let tokio_handle = Handle::current();
        let flush_task = if inner.wal_enabled() {
            Some(inner.spawn_flush_task(wal_flush_rx, &tokio_handle))
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
                path_resolver,
                fp_registry.clone(),
                None,
            ));
            let cleanup_inner = inner.clone();
            compactor = Some(
                Compactor::new(
                    manifest_store.clone(),
                    uncached_table_store.clone(),
                    compactor_options.clone(),
                    Handle::current(),
                    inner.stat_registry.as_ref(),
                    move |result: &Result<(), SlateDBError>| {
                        let err = bg_task_result_into_err(result);
                        warn!("compactor thread exited with {:?}", err);
                        let mut state = cleanup_inner.state.write();
                        state.record_fatal_error(err.clone())
                    },
                )
                .await?,
            )
        }
        let mut garbage_collector = None;
        if let Some(gc_options) = &inner.options.garbage_collector_options {
            let cleanup_inner = inner.clone();
            garbage_collector = Some(
                GarbageCollector::new(
                    manifest_store.clone(),
                    table_store.clone(),
                    gc_options.clone(),
                    Handle::current(),
                    inner.stat_registry.clone(),
                    move |result| {
                        let err = bg_task_result_into_err(result);
                        warn!("GC thread exited with {:?}", err);
                        let mut state = cleanup_inner.state.write();
                        state.record_fatal_error(err.clone())
                    },
                )
                .await,
            )
        };
        Ok(Self {
            inner,
            wal_flush_task: Mutex::new(flush_task),
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
            None => {
                StoredManifest::create_new_db(manifest_store.clone(), CoreDbState::new()).await?
            }
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
    /// use slatedb::{Db, SlateDBError};
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
            let result = write_task.await.expect("Failed to join write thread");
            info!("write task exited with {:?}", result);
        }

        // Shutdown the WAL flush thread.
        self.inner
            .wal_flush_notifier
            .send(WalFlushMsg::Shutdown)
            .ok();

        if let Some(flush_task) = {
            let mut flush_task = self.wal_flush_task.lock();
            flush_task.take()
        } {
            let result = flush_task.await.expect("Failed to join flush thread");
            info!("flush task exited with {:?}", result);
        }

        // Shutdown the memtable flush thread.
        self.inner
            .memtable_flush_notifier
            .send(MemtableFlushMsg::Shutdown)
            .ok();

        if let Some(memtable_flush_task) = {
            let mut memtable_flush_task = self.memtable_flush_task.lock();
            memtable_flush_task.take()
        } {
            let result = memtable_flush_task
                .await
                .expect("Failed to join memtable flush thread");
            info!("mem table flush task exited with: {:?}", result);
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
    /// use slatedb::{Db, SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     assert_eq!(db.get(b"key").await?, Some("value".into()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, SlateDBError> {
        self.get_with_options(key, &ReadOptions::default()).await
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
    /// - `options`: the read options to use (Note that [`ReadOptions::read_level`] has no effect for readers, which
    ///    can only observe committed state).
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
    /// use slatedb::{Db, config::ReadOptions, SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     assert_eq!(db.get_with_options(b"key", &ReadOptions::default()).await?, Some("value".into()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.inner.get_with_options(key, options).await
    }

    /// Scan a range of keys using the default scan options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, SlateDBError};
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
    ///     let mut iter = db.scan("a".."b").await?;
    ///     assert_eq!(Some((b"a", b"a_value").into()), iter.next().await?);
    ///     assert_eq!(None, iter.next().await?);
    ///     Ok(())
    /// }
    /// ```
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, SlateDBError>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default()).await
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
    /// use slatedb::{Db, config::ScanOptions, config::DurabilityLevel, SlateDBError};
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
    ///     let mut iter = db.scan_with_options("a".."b", &ScanOptions {
    ///         durability_filter: DurabilityLevel::Memory,
    ///         ..ScanOptions::default()
    ///     }).await?;
    ///     assert_eq!(Some((b"a", b"a_value").into()), iter.next().await?);
    ///     assert_eq!(None, iter.next().await?);
    ///     Ok(())
    /// }
    /// ```
    pub async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let range = (start, end);
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
    /// - `SlateDBError`: if there was an error writing the value.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, SlateDBError};
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
    pub async fn put<K, V>(&self, key: K, value: V) -> Result<(), SlateDBError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
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
    /// - `SlateDBError`: if there was an error writing the value.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, config::{PutOptions, WriteOptions}, SlateDBError};
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
    pub async fn put_with_options<K, V>(
        &self,
        key: K,
        value: V,
        put_opts: &PutOptions,
        write_opts: &WriteOptions,
    ) -> Result<(), SlateDBError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
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
    /// - `SlateDBError`: if there was an error deleting the key.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, SlateDBError};
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
    pub async fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), SlateDBError> {
        let mut batch = WriteBatch::new();
        batch.delete(key.as_ref());
        self.write(batch).await
    }

    /// Delete a key from the database with custom `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to delete
    /// - `options`: the write options to use
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error deleting the key.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, config::WriteOptions, SlateDBError};
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
    pub async fn delete_with_options<K: AsRef<[u8]>>(
        &self,
        key: K,
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
    /// - `SlateDBError`: if there was an error writing the batch.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{WriteBatch, Db, SlateDBError};
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
        self.write_with_options(batch, &WriteOptions::default())
            .await
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
    /// - `SlateDBError`: if there was an error writing the batch.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{WriteBatch, Db, config::WriteOptions, SlateDBError};
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
    /// use slatedb::{Db, SlateDBError};
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

    pub(crate) async fn await_flush(&self) -> Result<(), SlateDBError> {
        let table = {
            let guard = self.inner.state.read();
            let snapshot = guard.snapshot();
            if self.inner.wal_enabled() {
                snapshot.wal.clone()
            } else {
                snapshot.memtable.clone()
            }
        };
        if table.is_empty() {
            return Ok(());
        }
        table.await_durable().await
    }

    pub fn metrics(&self) -> Arc<StatRegistry> {
        self.inner.stat_registry.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::Bound::Included;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;
    use crate::cached_object_store::stats::{
        OBJECT_STORE_CACHE_PART_ACCESS, OBJECT_STORE_CACHE_PART_HITS,
    };
    use crate::cached_object_store::FsCacheStorage;
    use crate::config::DurabilityLevel::{Remote, Memory};
    use crate::config::{
        CompactorOptions, ObjectStoreCacheOptions, SizeTieredCompactionSchedulerOptions, Ttl,
    };
    use crate::db_stats::IMMUTABLE_MEMTABLE_FLUSHES;
    use crate::iter::KeyValueIterator;
    use crate::proptest_util::arbitrary;
    use crate::proptest_util::sample;
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::test_utils::{assert_iterator, TestClock};
    use crate::types::RowEntry;
    use crate::{proptest_util, test_utils};
    use futures::{future::join_all, StreamExt};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use proptest::test_runner::{TestRng, TestRunner};
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
    async fn test_get_with_default_ttl_and_read_uncommitted() {
        let clock = Arc::new(TestClock::new());
        let ttl = 100;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options_with_ttl(0, 1024, None, clock.clone(), Some(ttl)),
            object_store,
        )
        .await
        .unwrap();

        let key = b"test_key";
        let value = b"test_value";

        // insert at t=0
        kv_store.put(key, value).await.unwrap();

        // advance clock to t=99 --> still returned
        clock.ticker.store(99, Ordering::SeqCst);
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Memory
                    }
                )
                .await
                .unwrap(),
        );

        // advance clock to t=100 --> no longer returned
        clock.ticker.store(100, Ordering::SeqCst);
        assert_eq!(
            None,
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Memory
                    }
                )
                .await
                .unwrap(),
        );

        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_with_row_override_ttl_and_read_uncommitted() {
        let clock = Arc::new(TestClock::new());
        let default_ttl = 100;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options_with_ttl(0, 1024, None, clock.clone(), Some(default_ttl)),
            object_store,
        )
        .await
        .unwrap();

        let key = b"test_key";
        let value = b"test_value";

        // insert at t=0 with row-level override of 50 for ttl
        kv_store
            .put_with_options(
                key,
                value,
                &PutOptions {
                    ttl: Ttl::ExpireAfter(50),
                },
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        // advance clock to t=49 --> still returned
        clock.ticker.store(49, Ordering::SeqCst);
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Memory
                    }
                )
                .await
                .unwrap(),
        );

        // advance clock to t=50 --> no longer returned
        clock.ticker.store(50, Ordering::SeqCst);
        assert_eq!(
            None,
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Memory
                    }
                )
                .await
                .unwrap(),
        );

        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_with_default_ttl_and_read_committed() {
        let clock = Arc::new(TestClock::new());
        let ttl = 100;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options_with_ttl(0, 1024, None, clock.clone(), Some(ttl)),
            object_store,
        )
        .await
        .unwrap();

        let key = b"test_key";
        let key_other = b"time_advancing_key";
        let value = b"test_value";

        // insert at t=0
        kv_store.put(key, value).await.unwrap();

        // advance clock to t=99 --> still returned
        clock.ticker.store(99, Ordering::SeqCst);
        kv_store.put(key_other, value).await.unwrap(); // fake data to advance clock
        kv_store.flush().await.unwrap();
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Remote
                    }
                )
                .await
                .unwrap(),
        );

        // advance clock to t=100 without flushing --> still returned
        clock.ticker.store(100, Ordering::SeqCst);
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Remote
                    }
                )
                .await
                .unwrap(),
        );

        // advance durable clock time to t=100 by flushing -- no longer returned
        kv_store.put(key_other, value).await.unwrap(); // fake data to advance clock
        kv_store.flush().await.unwrap();
        assert_eq!(
            None,
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Remote
                    }
                )
                .await
                .unwrap(),
        );

        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_get_with_durability_level_when_wal_disabled() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut options = test_db_options(0, 1024 * 1024, None);
        options.wal_enabled = false;
        let db = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            options.clone(),
            object_store,
        )
            .await
            .unwrap();
        let put_options = PutOptions::default();
        let write_options = WriteOptions{await_durable: false};
        let get_memory_options = ReadOptions{durability_filter: Memory};
        let get_remote_options = ReadOptions{durability_filter: Remote};

        db.put_with_options(b"foo", b"bar", &put_options, &write_options).await.unwrap();
        let val_bytes = Bytes::copy_from_slice(b"bar");
        assert_eq!(None, db.get_with_options(b"foo", &get_remote_options).await.unwrap());
        assert_eq!(Some(val_bytes.clone()), db.get_with_options(b"foo", &get_memory_options).await.unwrap());
        db.flush().await.unwrap();
        assert_eq!(Some(val_bytes.clone()), db.get_with_options(b"foo", &get_remote_options).await.unwrap());
        assert_eq!(Some(val_bytes.clone()), db.get_with_options(b"foo", &get_memory_options).await.unwrap());
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_find_with_multiple_repeated_keys() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut options = test_db_options(0, 1024 * 1024, None);
        options.wal_enabled = false;
        let db = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            options.clone(),
            object_store,
        )
        .await
        .unwrap();

        // write enough rows with the same key that we yield an L0 SST with multiple blocks
        let mut last_val: String = "foo".to_string();
        for x in 0..4096 {
            let val = format!("val{}", x);
            db.put_with_options(
                b"key",
                val.as_bytes(),
                &PutOptions {
                    ttl: Default::default(),
                },
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();
            last_val = val;
            if db.inner.state.write().memtable().size() > (SsTableFormat::default().block_size * 3)
            {
                break;
            }
        }
        assert_eq!(
            Some(Bytes::copy_from_slice(last_val.as_bytes())),
            db.get_with_options(b"key", &ReadOptions{durability_filter: Memory}).await.unwrap()
        );
        db.flush().await.unwrap();

        let state = db.inner.state.read().snapshot();
        assert_eq!(1, state.state.manifest.core.l0.len());
        let sst = state.state.manifest.core.l0.front().unwrap();
        let index = db.inner.table_store.read_index(sst).await.unwrap();
        assert!(index.borrow().block_meta().len() >= 3);
        assert_eq!(
            Some(Bytes::copy_from_slice(last_val.as_bytes())),
            db.get(b"key").await.unwrap()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_with_row_override_ttl_and_read_committed() {
        let clock = Arc::new(TestClock::new());
        let ttl = 100;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open_with_opts(
            Path::from("/tmp/test_kv_store"),
            test_db_options_with_ttl(0, 1024, None, clock.clone(), Some(ttl)),
            object_store,
        )
        .await
        .unwrap();

        let key = b"test_key";
        let key_other = b"time_advancing_key";
        let value = b"test_value";

        // insert at t=0 with row-level override of 50 for ttl
        kv_store
            .put_with_options(
                key,
                value,
                &PutOptions {
                    ttl: Ttl::ExpireAfter(50),
                },
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        // advance clock to t=49 --> still returned
        clock.ticker.store(49, Ordering::SeqCst);
        kv_store.put(key_other, value).await.unwrap(); // fake data to advance clock
        kv_store.flush().await.unwrap();
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Remote
                    }
                )
                .await
                .unwrap(),
        );

        // advance clock to t=50 without flushing --> still returned
        clock.ticker.store(50, Ordering::SeqCst);
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Remote
                    }
                )
                .await
                .unwrap(),
        );

        // advance durable clock time to t=100 by flushing -- no longer returned
        kv_store.put(key_other, value).await.unwrap(); // fake data to advance clock
        kv_store.flush().await.unwrap();
        assert_eq!(
            None,
            kv_store
                .get_with_options(
                    key,
                    &ReadOptions {
                        durability_filter: Remote
                    }
                )
                .await
                .unwrap(),
        );

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

        let access_count0 = kv_store
            .metrics()
            .lookup(OBJECT_STORE_CACHE_PART_ACCESS)
            .unwrap()
            .get();
        let key = b"test_key";
        let value = b"test_value";
        kv_store.put(key, value).await.unwrap();
        kv_store.flush().await.unwrap();

        let got = kv_store.get(key).await.unwrap();
        let access_count1 = kv_store
            .metrics()
            .lookup(OBJECT_STORE_CACHE_PART_ACCESS)
            .unwrap()
            .get();
        assert_eq!(got, Some(Bytes::from_static(value)));
        assert!(access_count1 > 0);
        assert!(access_count1 >= access_count0);
        assert!(
            kv_store
                .metrics()
                .lookup(OBJECT_STORE_CACHE_PART_HITS)
                .unwrap()
                .get()
                >= 1
        );
    }

    #[tokio::test]
    async fn test_get_with_object_store_cache_stored_files() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut opts = test_db_options(0, 1024, None);
        let temp_dir = tempfile::Builder::new()
            .prefix("objstore_cache_test_")
            .tempdir()
            .unwrap();
        let stats_registry = StatRegistry::new();
        let cache_stats = Arc::new(CachedObjectStoreStats::new(&stats_registry));
        let part_size = 1024;
        let cache_storage = Arc::new(FsCacheStorage::new(
            temp_dir.path().to_path_buf(),
            None,
            None,
            cache_stats.clone(),
        ));

        let cached_object_store = CachedObjectStore::new(
            object_store.clone(),
            cache_storage,
            part_size,
            cache_stats.clone(),
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

    async fn build_database_from_table(
        table: &BTreeMap<Bytes, Bytes>,
        db_options: DbOptions,
        await_durable: bool,
    ) -> Db {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), db_options, object_store)
            .await
            .unwrap();

        test_utils::seed_database(&db, table, false).await.unwrap();

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
        let db_options = test_db_options(0, 1024, None);
        let db = runtime.block_on(build_database_from_table(&table, db_options, true));

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
        test_utils::assert_ranged_db_scan(table, range, &mut iter).await;
    }

    #[test]
    fn test_scan_returns_records_in_range() {
        let mut runner = new_proptest_runner(None);
        let table = sample::table(runner.rng(), 1000, 5);

        let runtime = Runtime::new().unwrap();
        let db_options = test_db_options(0, 1024, None);
        let db = runtime.block_on(build_database_from_table(&table, db_options, true));

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
        proptest_util::runner::new(file!(), rng_seed)
    }

    #[test]
    fn test_scan_returns_uncommitted_records_if_read_level_uncommitted() {
        let mut runner = new_proptest_runner(None);
        let table = sample::table(runner.rng(), 1000, 5);

        let runtime = Runtime::new().unwrap();
        let mut db_options = test_db_options(0, 1024, None);
        db_options.flush_interval = Some(Duration::from_secs(5));
        let db = runtime.block_on(build_database_from_table(&table, db_options, false));

        runner
            .run(&arbitrary::nonempty_range(10), |range| {
                let scan_options = ScanOptions {
                    durability_filter: Memory,
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
        let db_options = test_db_options(0, 1024, None);
        let db = runtime.block_on(build_database_from_table(&table, db_options, true));

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
            let value = sample::bytes_in_range(rng, &upper_bounded_range);
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
        let db_options = test_db_options(0, 1024, None);
        let db = runtime.block_on(build_database_from_table(&table, db_options, true));

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

            let seek_key = sample::bytes_in_range(rng, scan_range);
            iter.seek(seek_key.clone()).await.unwrap();

            let seek_range = BytesRange::new(Included(seek_key), scan_range.end_bound().cloned());
            test_utils::assert_ranged_db_scan(table, seek_range, &mut iter).await;
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
        assert_eq!(kv_store.get(b"key2").await.unwrap(), Some("value2".into()));

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
                        batch.put(key.to_be_bytes(), key.to_be_bytes());
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
                        batch.put(key.to_be_bytes(), value);
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
                let value = kv_store.get(key.to_be_bytes()).await.unwrap();
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
        use crate::{test_utils::assert_iterator, types::RowEntry};

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
            &PutOptions::default(),
            &write_options,
        )
        .await
        .unwrap();
        db.delete_with_options(&[b'b'; 31], &write_options)
            .await
            .unwrap();

        // ensure the memtable's size is greater than l0_sst_size_bytes, or
        // the memtable will not be flushed to l0, and the test will hang
        // at this put_with_options call.
        let write_options = WriteOptions {
            await_durable: true,
        };
        clock.ticker.store(10, Ordering::SeqCst);
        db.put_with_options(
            &[b'c'; 32],
            &[b'l'; 32],
            &PutOptions::default(),
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
        let mut iter =
            SstIterator::new_borrowed(.., l0, table_store.clone(), SstIteratorOptions::default())
                .await
                .unwrap();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(&[b'a'; 32], &[b'j'; 32], 1).with_create_ts(0),
                RowEntry::new_tombstone(&[b'b'; 31], 2).with_create_ts(0),
                RowEntry::new_value(&[b'c'; 32], &[b'l'; 32], 3).with_create_ts(10),
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

        let manifest = stored_manifest.refresh().await.unwrap();
        let l0 = &manifest.core.l0;
        assert_eq!(l0.len(), 3);
        let sst_iter_options = SstIteratorOptions::default();

        for i in 0u8..3u8 {
            let sst1 = l0.get(2 - i as usize).unwrap();
            let mut iter =
                SstIterator::new_borrowed(.., sst1, table_store.clone(), sst_iter_options)
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
        assert!(
            kv_store
                .metrics()
                .lookup(IMMUTABLE_MEMTABLE_FLUSHES)
                .unwrap()
                .get()
                > 0
        );
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
            lock.wal()
                .put(RowEntry::new_value(b"abc1111", b"value1111", 1));
            lock.wal()
                .put(RowEntry::new_value(b"abc2222", b"value2222", 2));
            lock.wal()
                .put(RowEntry::new_value(b"abc3333", b"value3333", 3));
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
            let val = kv_store_restored.get([b'a' + i as u8; 16]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[b'b' + i as u8; 48])));
            let val = kv_store_restored.get([b'j' + i as u8; 16]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[b'k' + i as u8; 48])));
        }
        for i in 0..sst_count {
            let val = kv_store_restored.get(i.to_be_bytes()).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&i.to_be_bytes())));
        }
        kv_store_restored.close().await.unwrap();

        // validate that the manifest file exists.
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let stored_manifest = StoredManifest::load(manifest_store).await.unwrap();
        let db_state = stored_manifest.db_state();
        assert_eq!(db_state.next_wal_sst_id, next_wal_id);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_restore_seq_number() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        db.put(b"key1", b"val1").await.unwrap();
        db.put(b"key2", b"val2").await.unwrap();
        db.put(b"key3", b"val3").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let db_restored = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        let mut state = db_restored.inner.state.write();
        let memtable = state.memtable();
        let mut iter = memtable.table().iter();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"key1", b"val1", 1).with_create_ts(0),
                RowEntry::new_value(b"key2", b"val2", 2).with_create_ts(0),
                RowEntry::new_value(b"key3", b"val3", 3).with_create_ts(0),
            ],
        )
        .await;
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
                &PutOptions::default(),
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
                    durability_filter: Memory,
                },
            )
            .await
            .unwrap();
        assert_eq!(val, Some("bar".into()));

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
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let val = kv_store.get("foo".as_bytes()).await.unwrap();
        assert_eq!(val, Some("bar".into()));
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions {
                    durability_filter: Memory,
                },
            )
            .await
            .unwrap();
        assert_eq!(val, Some("bla".into()));

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
        assert_eq!(val, Some("bar".into()));
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions {
                    durability_filter: Memory,
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
        db.put(key1, value1).await.unwrap();
        next_wal_id += 1;
        let key2 = [b'c'; 32];
        let value2 = [b'd'; 96];
        db.put(key2, value2).await.unwrap();
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
        assert_eq!(snapshot.state.core().next_wal_sst_id, next_wal_id);
        assert_eq!(
            reader.get(key1).await.unwrap(),
            Some(Bytes::copy_from_slice(&value1))
        );
        assert_eq!(
            reader.get(key2).await.unwrap(),
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

        let flush_result = db.inner.flush_immutable_memtables().await;
        assert!(flush_result.is_err());
        db.close().await.unwrap();

        // pause write-compacted-sst-io-error to prevent immutable tables
        // from being flushed, so we can snapshot the state when there is
        // an immutable table to verify its contents.
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "pause").unwrap();

        // reload the db
        let db = Db::open_with_fp_registry(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
            fp_registry.clone(),
        )
        .await
        .unwrap();

        // verify that we reload imm
        let snapshot = db.inner.state.read().snapshot();

        // resume write-compacted-sst-io-error since we got a snapshot and
        // want to let the test finish.
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();

        assert_eq!(snapshot.state.imm_memtable.len(), 1);

        // one empty wal and one wal for the first put
        assert_eq!(
            snapshot.state.imm_memtable.front().unwrap().last_wal_id(),
            1 + 1
        );
        assert!(snapshot.state.imm_memtable.get(1).is_none());

        assert_eq!(snapshot.state.core().next_wal_sst_id, 4);
        assert_eq!(
            db.get(key1).await.unwrap(),
            Some(Bytes::copy_from_slice(&value1))
        );
    }

    #[tokio::test]
    async fn test_should_fail_write_if_wal_flush_task_panics() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Arc::new(
            Db::open_with_fp_registry(
                path.clone(),
                test_db_options(0, 128, None),
                object_store.clone(),
                fp_registry.clone(),
            )
            .await
            .unwrap(),
        );

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "panic").unwrap();
        let result = db.put(b"foo", b"bar").await;
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
    }

    #[tokio::test]
    async fn test_wal_id_last_seen_should_exist_even_if_wal_write_fails() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Arc::new(
            Db::open_with_fp_registry(
                path.clone(),
                test_db_options(0, 128, None),
                object_store.clone(),
                fp_registry.clone(),
            )
            .await
            .unwrap(),
        );

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "panic").unwrap();

        // Trigger a WAL write, which should not advance the manifest WAL ID
        let result = db.put(b"foo", b"bar").await;
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));

        // Close, which flushes the latest manifest to the object store
        db.close().await.unwrap();

        let manifest_store = ManifestStore::new(&path, object_store.clone());
        let table_store = Arc::new(TableStore::new(
            object_store.clone(),
            SsTableFormat::default(),
            path.clone(),
            None,
        ));

        // Get the next WAL SST ID based on what's currently in the object store
        let next_wal_sst_id = table_store.next_wal_sst_id(0).await.unwrap();

        // Get the latest manifest
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();

        // The manifest's next_wal_sst_id, which uses `wal_id_last_seen + 1`, should
        // be the same as the next WAL SST ID based on what's currently in the object store
        assert_eq!(manifest.core.next_wal_sst_id, next_wal_sst_id);
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
            db.inner.state.read().state().core().l0.len(),
            db.inner.state.read().state().core().compacted.len()
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
            db.inner.state.read().state().core().l0.len(),
            db.inner.state.read().state().core().compacted.len()
        );
        // write another l0
        db.put(&[b'a'; 32], &[128u8; 32]).await.unwrap();
        db.put(&[b'm'; 32], &[129u8; 32]).await.unwrap();

        let val = db.get([b'a'; 32]).await.unwrap();
        assert_eq!(val, Some(Bytes::copy_from_slice(&[128u8; 32])));
        let val = db.get([b'm'; 32]).await.unwrap();
        assert_eq!(val, Some(Bytes::copy_from_slice(&[129u8; 32])));
        for i in 1..4 {
            info!(
                "3 l0: {} {}",
                db.inner.state.read().state().core().l0.len(),
                db.inner.state.read().state().core().compacted.len()
            );
            let val = db.get([b'a' + i; 32]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[1u8 + i; 32])));
            let val = db.get([b'm' + i; 32]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[13u8 + i; 32])));
        }
        for i in 0..4 {
            let val = db.get([b'f' + i; 32]).await.unwrap();
            assert_eq!(val, Some(Bytes::copy_from_slice(&[6u8 + i; 32])));
            let val = db.get([b's' + i; 32]).await.unwrap();
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
        assert_eq!(db.inner.state.read().state().core().next_wal_sst_id, 2);
        db.put(b"1", b"1").await.unwrap();
        // assert that second open writes another empty wal.
        let db = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        assert_eq!(db.inner.state.read().state().core().next_wal_sst_id, 4);
    }

    #[tokio::test]
    async fn test_empty_wal_should_fence_old_writer() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        async fn do_put(db: &Db, key: &[u8], val: &[u8]) -> Result<(), SlateDBError> {
            db.put_with_options(
                key,
                val,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: true,
                },
            )
            .await
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
        assert_eq!(db2.inner.state.read().state().core().next_wal_sst_id, 5);
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

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn should_flush_all_memtables_when_wal_disabled() {
        // Given:
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let db_options = DbOptions {
            wal_enabled: false,
            flush_interval: Some(Duration::from_secs(10)),
            ..DbOptions::default()
        };

        let db = Db::open_with_opts(path.clone(), db_options.clone(), Arc::clone(&object_store))
            .await
            .unwrap();

        let mut rng = proptest_util::rng::new_test_rng(None);
        let table = sample::table(&mut rng, 1000, 5);
        test_utils::seed_database(&db, &table, false).await.unwrap();
        db.flush().await.unwrap();

        // When: reopen the database without closing the old instance
        let reopened_db =
            Db::open_with_opts(path.clone(), db_options.clone(), Arc::clone(&object_store))
                .await
                .unwrap();

        // Then:
        assert_records_in_range(
            &table,
            &reopened_db,
            &ScanOptions::default(),
            BytesRange::from(..),
        )
        .await
    }

    #[tokio::test]
    async fn test_recover_clock_tick_from_wal() {
        let clock = Arc::new(TestClock::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let db = Db::open_with_opts(
            path.clone(),
            test_db_options_with_clock(0, 1024, None, clock.clone()),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        clock.ticker.store(10, Ordering::SeqCst);
        db.put(&[b'a'; 4], &[b'j'; 8])
            .await
            .expect("write batch failed");
        clock.ticker.store(11, Ordering::SeqCst);
        db.put(&[b'b'; 4], &[b'k'; 8])
            .await
            .expect("write batch failed");

        // close the db to flush the manifest
        db.close().await.unwrap();

        // check the last_l0_clock_tick persisted in the manifest, it should be
        // i64::MIN because no WAL SST has yet made its way into L0
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let stored_manifest = StoredManifest::load(manifest_store).await.unwrap();
        let db_state = stored_manifest.db_state();
        let last_clock_tick = db_state.last_l0_clock_tick;
        assert_eq!(last_clock_tick, i64::MIN);

        let clock = Arc::new(TestClock::new());
        let db = Db::open_with_opts(
            path.clone(),
            test_db_options_with_clock(0, 1024, None, clock.clone()),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        assert_eq!(db.inner.mono_clock.last_tick.load(Ordering::SeqCst), 11);
    }

    #[tokio::test]
    async fn test_should_update_manifest_clock_tick_on_l0_flush() {
        let clock = Arc::new(TestClock::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let db = Db::open_with_opts(
            path.clone(),
            test_db_options_with_clock(0, 32, None, clock.clone()),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        // this will exceed the l0_sst_size_bytes, meaning a clean shutdown
        // will update the manifest
        clock.ticker.store(10, Ordering::SeqCst);
        db.put(&[b'a'; 4], &[b'j'; 8])
            .await
            .expect("write batch failed");
        clock.ticker.store(11, Ordering::SeqCst);
        db.put(&[b'b'; 4], &[b'k'; 8])
            .await
            .expect("write batch failed");

        // close the db to flush the manifest
        db.flush().await.unwrap();
        db.close().await.unwrap();

        // check the last_clock_tick persisted in the manifest, it should be
        // i64::MIN because no WAL SST has yet made its way into L0
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let stored_manifest = StoredManifest::load(manifest_store).await.unwrap();
        let db_state = stored_manifest.db_state();
        let last_clock_tick = db_state.last_l0_clock_tick;
        assert_eq!(last_clock_tick, 11);
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_recover_clock_tick_from_manifest() {
        let clock = Arc::new(TestClock::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let mut options = test_db_options_with_clock(0, 32, None, clock.clone());
        options.wal_enabled = false;

        let db = Db::open_with_opts(path.clone(), options, Arc::clone(&object_store))
            .await
            .unwrap();

        clock.ticker.store(10, Ordering::SeqCst);
        db.put(&[b'a'; 4], &[b'j'; 28])
            .await
            .expect("write batch failed");
        clock.ticker.store(11, Ordering::SeqCst);
        db.put(&[b'b'; 4], &[b'k'; 28])
            .await
            .expect("write batch failed");

        // close the db to flush the manifest
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let clock = Arc::new(TestClock::new());
        let mut options = test_db_options_with_clock(0, 32, None, clock.clone());
        options.wal_enabled = false;
        let db = Db::open_with_opts(path.clone(), options, Arc::clone(&object_store))
            .await
            .unwrap();

        assert_eq!(db.inner.mono_clock.last_tick.load(Ordering::SeqCst), 11);
    }

    #[test]
    fn test_write_option_defaults() {
        // This is a regression test for a bug where the defaults for WriteOptions were not being
        // set correctly due to visibility issues.
        let write_options = WriteOptions::default();
        assert!(write_options.await_durable);
    }

    async fn wait_for_manifest_condition(
        sm: &mut StoredManifest,
        cond: impl Fn(&CoreDbState) -> bool,
        timeout: Duration,
    ) -> CoreDbState {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let manifest = sm.refresh().await.unwrap();
            if cond(&manifest.core) {
                return manifest.core.clone();
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
        test_db_options_with_ttl(
            min_filter_keys,
            l0_sst_size_bytes,
            compactor_options,
            clock,
            None,
        )
    }

    fn test_db_options_with_ttl(
        min_filter_keys: u32,
        l0_sst_size_bytes: usize,
        compactor_options: Option<CompactorOptions>,
        clock: Arc<TestClock>,
        ttl: Option<u64>,
    ) -> DbOptions {
        DbOptions {
            flush_interval: Some(Duration::from_millis(100)),
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
            default_ttl: ttl,
        }
    }
}
