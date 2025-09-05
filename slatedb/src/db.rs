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
//! use slatedb::{Db, Error};
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::open("test_db", object_store).await?;
//!     Ok(())
//! }
//! ```

use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::registry::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use crate::batch::WriteBatch;
use crate::batch_write::{WriteBatchMsg, WriteBatchRequest};
use crate::bytes_range::BytesRange;
use crate::cached_object_store::CachedObjectStore;
use crate::clock::MonotonicClock;
use crate::clock::{LogicalClock, SystemClock};
use crate::config::{
    FlushOptions, FlushType, PreloadLevel, PutOptions, ReadOptions, ScanOptions, Settings,
    WriteOptions,
};
use crate::db_iter::DbIterator;
use crate::db_read::DbRead;
use crate::db_snapshot::DbSnapshot;
use crate::db_state::{DbState, SsTableId};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::manifest::store::{DirtyManifest, FenceableManifest};
use crate::mem_table::WritableKVTable;
use crate::mem_table_flush::MemtableFlushMsg;
use crate::oracle::Oracle;
use crate::paths::PathResolver;
use crate::rand::DbRand;
use crate::reader::Reader;
use crate::sst_iter::SstIteratorOptions;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::transaction_manager::TransactionManager;
use crate::utils::{MonotonicSeq, SendSafely};
use crate::wal_buffer::WalBufferManager;
use crate::wal_replay::{WalReplayIterator, WalReplayOptions};
use log::{info, trace, warn};

pub mod builder;
pub use builder::DbBuilder;

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<DbState>>,
    pub(crate) settings: Settings,
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) memtable_flush_notifier: UnboundedSender<MemtableFlushMsg>,
    pub(crate) write_notifier: UnboundedSender<WriteBatchMsg>,
    pub(crate) db_stats: DbStats,
    pub(crate) stat_registry: Arc<StatRegistry>,
    #[allow(dead_code)]
    pub(crate) fp_registry: Arc<FailPointRegistry>,
    /// A clock which is guaranteed to be monotonic. it's previous value is
    /// stored in the manifest and WAL, will be updated after WAL replay.
    pub(crate) mono_clock: Arc<MonotonicClock>,
    pub(crate) system_clock: Arc<dyn SystemClock>,
    pub(crate) rand: Arc<DbRand>,
    pub(crate) oracle: Arc<Oracle>,
    pub(crate) reader: Reader,
    /// [`wal_buffer`] manages the in-memory WAL buffer, it manages the flushing
    /// of the WAL buffer to the remote storage.
    pub(crate) wal_buffer: Arc<WalBufferManager>,
    pub(crate) wal_enabled: bool,
    /// [`txn_manager`] tracks all the live transactions and related metadata.
    pub(crate) txn_manager: Arc<TransactionManager>,
}

impl DbInner {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        settings: Settings,
        logical_clock: Arc<dyn LogicalClock>,
        // TODO replace all system clock usage with this
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
        table_store: Arc<TableStore>,
        manifest: DirtyManifest,
        memtable_flush_notifier: UnboundedSender<MemtableFlushMsg>,
        write_notifier: UnboundedSender<WriteBatchMsg>,
        stat_registry: Arc<StatRegistry>,
        fp_registry: Arc<FailPointRegistry>,
    ) -> Result<Self, SlateDBError> {
        // last_seq, last_committed_seq, next_wal_id will be updated after WAL replay.
        let last_l0_seq = manifest.core.last_l0_seq;
        let last_seq = MonotonicSeq::new(last_l0_seq);
        let last_committed_seq = MonotonicSeq::new(last_l0_seq);
        let last_remote_persisted_seq = MonotonicSeq::new(last_l0_seq);
        let next_wal_id = MonotonicSeq::new(manifest.core.replay_after_wal_id + 1);
        let oracle = Arc::new(
            Oracle::new(last_committed_seq, next_wal_id)
                .with_last_seq(last_seq)
                .with_last_remote_persisted_seq(last_remote_persisted_seq),
        );

        let mono_clock = Arc::new(MonotonicClock::new(
            logical_clock,
            manifest.core.last_l0_clock_tick,
        ));

        // state are mostly manifest, including IMM, L0, etc.
        let state = Arc::new(RwLock::new(DbState::new(manifest)));

        let db_stats = DbStats::new(stat_registry.as_ref());
        let wal_enabled = DbInner::wal_enabled_in_options(&settings);

        let reader = Reader {
            table_store: table_store.clone(),
            db_stats: db_stats.clone(),
            mono_clock: mono_clock.clone(),
            oracle: oracle.clone(),
        };

        let recent_flushed_wal_id = state.read().state().core().replay_after_wal_id;
        let wal_buffer = Arc::new(WalBufferManager::new(
            oracle.clone(),
            state.clone(),
            db_stats.clone(),
            recent_flushed_wal_id,
            table_store.clone(),
            mono_clock.clone(),
            system_clock.clone(),
            settings.l0_sst_size_bytes,
            settings.flush_interval,
        ));

        let txn_manager = Arc::new(TransactionManager::new(rand.clone()));

        let db_inner = Self {
            state,
            settings,
            oracle,
            wal_enabled,
            table_store,
            memtable_flush_notifier,
            wal_buffer,
            write_notifier,
            db_stats,
            mono_clock,
            system_clock,
            rand,
            stat_registry,
            fp_registry,
            reader,
            txn_manager,
        };
        Ok(db_inner)
    }

    /// Get the value for a given key.
    pub async fn get_with_options<K: AsRef<[u8]>>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.db_stats.get_requests.inc();
        self.check_error()?;
        let db_state = self.state.read().view();
        self.reader
            .get_with_options(key, options, &db_state, None)
            .await
    }

    pub async fn scan_with_options(
        &self,
        range: BytesRange,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError> {
        self.db_stats.scan_requests.inc();
        self.check_error()?;
        let db_state = self.state.read().view();
        self.reader
            .scan_with_options(range, options, &db_state, None)
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
                .flush_imm_table(
                    &SsTableId::Wal(empty_wal_id),
                    empty_wal.table().clone(),
                    false,
                )
                .await
            {
                Ok(_) => {
                    self.oracle.next_wal_id.store(empty_wal_id + 1);
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

    #[allow(unused_variables)]
    pub(crate) fn wal_enabled_in_options(settings: &Settings) -> bool {
        #[cfg(feature = "wal_disable")]
        return settings.wal_enabled;
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
        // record write batch and number of operations
        self.db_stats.write_batch_count.inc();
        self.db_stats.write_ops.add(batch.ops.len() as u64);

        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch_msg =
            WriteBatchMsg::WriteBatch(WriteBatchRequest { batch, done: tx }, options.clone());

        self.maybe_apply_backpressure().await?;
        self.write_notifier
            .send_safely(self.state.read().error_reader(), batch_msg)?;

        // TODO: this can be modified as awaiting the last_durable_seq watermark & fatal error.

        let mut durable_watcher = rx.await??;

        if options.await_durable {
            durable_watcher.await_value().await?;
        }

        Ok(())
    }

    #[inline]
    pub(crate) async fn maybe_apply_backpressure(&self) -> Result<(), SlateDBError> {
        loop {
            let (wal_size_bytes, imm_memtable_size_bytes) = {
                let wal_size_bytes = self.wal_buffer.estimated_bytes().await?;
                let imm_memtable_size_bytes = {
                    let guard = self.state.read();
                    // Exclude active memtable to avoid a write lock.
                    guard
                        .state()
                        .imm_memtable
                        .iter()
                        .map(|imm| {
                            let metadata = imm.table().metadata();
                            self.table_store.estimate_encoded_size(
                                metadata.entry_num,
                                metadata.entries_size_in_bytes,
                            )
                        })
                        .sum::<usize>()
                };
                (wal_size_bytes, imm_memtable_size_bytes)
            };
            let total_mem_size_bytes = wal_size_bytes + imm_memtable_size_bytes;

            trace!(
                "checking backpressure [total_mem_size_bytes={}, wal_size_bytes={}, imm_memtable_size_bytes={}, max_unflushed_bytes={}]",
                total_mem_size_bytes,
                wal_size_bytes,
                imm_memtable_size_bytes,
                self.settings.max_unflushed_bytes,
            );

            if total_mem_size_bytes >= self.settings.max_unflushed_bytes {
                self.db_stats.backpressure_count.inc();
                warn!(
                    "unflushed memtable size exceeds max_unflushed_bytes. applying backpressure. [total_mem_size_bytes={}, wal_size_bytes={}, imm_memtable_size_bytes={}, max_unflushed_bytes={}]",
                    total_mem_size_bytes,
                    wal_size_bytes,
                    imm_memtable_size_bytes,
                    self.settings.max_unflushed_bytes,
                );

                let maybe_oldest_unflushed_memtable = {
                    let guard = self.state.read();
                    guard.state().imm_memtable.back().cloned()
                };

                let maybe_oldest_unflushed_wal = self.wal_buffer.oldest_unflushed_wal().await;

                // There is a window of time after mem_size_bytes is larger than max_unflushed_bytes
                // but before we get the memtable and wal table. During that time, if the memtable and/or
                // wal table are fully flushed out, we should short circuit since the select! will always
                // time out.
                if maybe_oldest_unflushed_memtable.is_none() && maybe_oldest_unflushed_wal.is_none()
                {
                    continue;
                }

                let await_flush_memtable = async {
                    if let Some(oldest_unflushed_memtable) = maybe_oldest_unflushed_memtable {
                        oldest_unflushed_memtable.await_flush_to_l0().await
                    } else {
                        std::future::pending().await
                    }
                };

                let await_flush_wal = async {
                    if let Some(oldest_unflushed_wal) = maybe_oldest_unflushed_wal {
                        oldest_unflushed_wal.await_durable().await
                    } else {
                        std::future::pending().await
                    }
                };

                self.flush_immutable_memtables().await?;

                let timeout_fut = self.system_clock.sleep(Duration::from_secs(30));

                tokio::select! {
                    result = await_flush_memtable => result?,
                    result = await_flush_wal => result?,
                    _ = timeout_fut => {
                        warn!("backpressure timeout: waited 30s, no memtable/WAL flushed yet");
                    }
                };
            } else {
                break;
            }
        }
        Ok(())
    }

    pub(crate) async fn flush_wals(&self) -> Result<(), SlateDBError> {
        self.wal_buffer.flush().await
    }

    // use to manually flush memtables
    async fn flush_immutable_memtables(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.memtable_flush_notifier.send_safely(
            self.state.read().error_reader(),
            MemtableFlushMsg::FlushImmutableMemtables { sender: Some(tx) },
        )?;
        rx.await?
    }

    pub(crate) async fn flush_memtables(&self) -> Result<(), SlateDBError> {
        {
            let last_flushed_wal_id = self.wal_buffer.recent_flushed_wal_id();
            let mut guard = self.state.write();
            if !guard.memtable().is_empty() {
                guard.freeze_memtable(last_flushed_wal_id)?;
            }
        }

        self.flush_immutable_memtables().await
    }

    async fn replay_wal(&self) -> Result<(), SlateDBError> {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: 256,
            cache_blocks: false,
            eager_spawn: true,
        };

        let replay_options = WalReplayOptions {
            sst_batch_size: 4,
            min_memtable_bytes: self.settings.l0_sst_size_bytes,
            max_memtable_bytes: usize::MAX,
            sst_iter_options,
        };

        let db_state = self.state.read().state().core().clone();
        let mut replay_iter =
            WalReplayIterator::new(&db_state, replay_options, Arc::clone(&self.table_store))
                .await?;

        while let Some(replayed_table) = replay_iter.next().await? {
            self.maybe_apply_backpressure().await?;
            self.replay_memtable(replayed_table)?;
        }

        // last_committed_seq is updated as WAL is replayed. after replay,
        // the last_committed_seq is considered same as the last_remote_persisted_seq.
        self.oracle
            .last_remote_persisted_seq
            .store(self.oracle.last_committed_seq.load());
        Ok(())
    }

    async fn preload_cache(
        &self,
        cached_obj_store: &CachedObjectStore,
        path_resolver: &PathResolver,
    ) -> Result<(), SlateDBError> {
        let current_state = self.state.read().state();
        let max_cache_size = self
            .settings
            .object_store_cache_options
            .max_cache_size_bytes
            .unwrap_or(usize::MAX);

        match self
            .settings
            .object_store_cache_options
            .preload_disk_cache_on_startup
        {
            Some(PreloadLevel::AllSst) => {
                // Preload both L0 and compacted SSTs
                let l0_count = current_state.manifest.core.l0.len();
                let compacted_count: usize = current_state
                    .manifest
                    .core
                    .compacted
                    .iter()
                    .map(|level| level.ssts.len())
                    .sum();
                let total_capacity = l0_count + compacted_count;

                let mut all_sst_paths: Vec<object_store::path::Path> =
                    Vec::with_capacity(total_capacity);

                // Add L0 SSTs
                all_sst_paths.extend(
                    current_state
                        .manifest
                        .core
                        .l0
                        .iter()
                        .map(|sst_handle| path_resolver.table_path(&sst_handle.id)),
                );

                // Add compacted SSTs
                all_sst_paths.extend(
                    current_state
                        .manifest
                        .core
                        .compacted
                        .iter()
                        .flat_map(|level| &level.ssts)
                        .map(|sst_handle| path_resolver.table_path(&sst_handle.id)),
                );

                if !all_sst_paths.is_empty() {
                    if let Err(e) = cached_obj_store
                        .load_files_to_cache(all_sst_paths, max_cache_size)
                        .await
                    {
                        warn!("Failed to preload all SSTs to cache: {:?}", e);
                    }
                }
            }
            Some(PreloadLevel::L0Sst) => {
                // Preload only L0 SSTs
                let l0_sst_paths: Vec<object_store::path::Path> = current_state
                    .manifest
                    .core
                    .l0
                    .iter()
                    .map(|sst_handle| path_resolver.table_path(&sst_handle.id))
                    .collect();

                if !l0_sst_paths.is_empty() {
                    if let Err(e) = cached_obj_store
                        .load_files_to_cache(l0_sst_paths, max_cache_size)
                        .await
                    {
                        warn!("failed to preload L0 SSTs to cache [error={:?}]", e);
                    }
                }
            }
            None => {
                // No preloading
            }
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
    memtable_flush_task: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
    write_task: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
    compactor_task: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
    garbage_collector_task: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
    cancellation_token: CancellationToken,
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
    /// - `Error`: if there was an error opening the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::memory::InMemory;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open<P: Into<Path>>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, crate::Error> {
        // Use the builder API internally
        Self::builder(path, object_store).build().await
    }

    /// Creates a new builder for a database at the given path.
    ///
    /// ## Arguments
    /// - `path`: the path to the database
    /// - `object_store`: the object store to use for the database
    ///
    /// ## Returns
    /// - `DbBuilder`: the builder to initialize the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::memory::InMemory;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store = Arc::new(InMemory::new());
    ///     let db = Db::builder("/tmp/test_db", object_store)
    ///         .build()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn builder<P: Into<Path>>(path: P, object_store: Arc<dyn ObjectStore>) -> DbBuilder<P> {
        DbBuilder::new(path, object_store)
    }

    /// Close the database.
    ///
    /// ## Returns
    /// - `Result<(), Error>`: if there was an error closing the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.close().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn close(&self) -> Result<(), crate::Error> {
        self.cancellation_token.cancel();

        if let Some(compactor_task) = {
            let mut maybe_compactor_task = self.compactor_task.lock();
            maybe_compactor_task.take()
        } {
            let result = compactor_task.await.expect("failed to join compactor task");
            info!("compactor task exited [result={:?}]", result);
        }

        if let Some(garbage_collector_task) = {
            let mut maybe_garbage_collector_task = self.garbage_collector_task.lock();
            maybe_garbage_collector_task.take()
        } {
            let result = garbage_collector_task
                .await
                .expect("failed to join garbage collector task");
            info!("garbage collector task exited [result={:?}]", result);
        }

        // Shutdown the write batch thread.
        self.inner
            .write_notifier
            .send_safely(
                self.inner.state.read().error_reader(),
                WriteBatchMsg::Shutdown,
            )
            .ok();

        if let Some(write_task) = {
            let mut write_task = self.write_task.lock();
            write_task.take()
        } {
            let result = write_task.await.expect("failed to join write thread");
            info!("write task exited [result={:?}]", result);
        }

        // Shutdown the WAL flush thread.
        self.inner
            .wal_buffer
            .close()
            .await
            .expect("failed to close WAL buffer");

        // Shutdown the memtable flush thread.
        self.inner
            .memtable_flush_notifier
            .send_safely(
                self.inner.state.read().error_reader(),
                MemtableFlushMsg::Shutdown,
            )
            .ok();

        if let Some(memtable_flush_task) = {
            let mut memtable_flush_task = self.memtable_flush_task.lock();
            memtable_flush_task.take()
        } {
            let result = memtable_flush_task
                .await
                .expect("failed to join memtable flush thread");
            info!("mem table flush task exited [result={:?}]", result);
        }

        Ok(())
    }

    /// Create a snapshot of the database.
    ///
    /// ## Returns
    /// - `Result<Arc<DbSnapshot>, Error>`: the snapshot of the database, it represents
    ///   a consistent view of the database at the time of the snapshot.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    /// use bytes::Bytes;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///
    ///     // Write some data and create a snapshot
    ///     db.put(b"key1", b"value1").await?;
    ///     let snapshot = db.snapshot().await?;
    ///
    ///     // Snapshot provides read-only access to database state
    ///     let value = snapshot.get(b"key1").await?;
    ///     assert_eq!(value, Some(Bytes::from(b"value1".as_ref())));
    ///
    ///     // Write more data to original database
    ///     db.put(b"key2", b"value2").await?;
    ///
    ///     // Snapshot still sees old state, original db sees new data
    ///     assert_eq!(snapshot.get(b"key2").await?, None);
    ///     assert_eq!(db.get(b"key2").await?, Some(Bytes::from(b"value2".as_ref())));
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn snapshot(&self) -> Result<Arc<DbSnapshot>, crate::Error> {
        self.inner.check_error()?;
        let seq = self.inner.oracle.last_committed_seq.load();
        let snapshot = DbSnapshot::new(self.inner.clone(), self.inner.txn_manager.clone(), seq);
        Ok(snapshot)
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
    /// - `Result<Option<Bytes>, Error>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `Error`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     assert_eq!(db.get(b"key").await?, Some("value".into()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, crate::Error> {
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
    ///   can only observe committed state).
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`:
    ///   - `Some(Bytes)`: the value if it exists
    ///   - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `Error`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, config::ReadOptions, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
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
    ) -> Result<Option<Bytes>, crate::Error> {
        self.inner
            .get_with_options(key, options)
            .await
            .map_err(Into::into)
    }

    /// Scan a range of keys using the default scan options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Errors
    /// - `Error`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
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
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
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
    /// - `Error`: if there was an error scanning the range of keys
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, config::ScanOptions, config::DurabilityLevel, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
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
    ) -> Result<DbIterator, crate::Error>
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
            .map_err(Into::into)
    }

    /// Write a value into the database with default `WriteOptions`.
    ///
    /// ## Arguments
    /// - `key`: the key to write
    /// - `value`: the value to write
    ///
    /// ## Errors
    /// - `Error`: if there was an error writing the value.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"value").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn put<K, V>(&self, key: K, value: V) -> Result<(), crate::Error>
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
    /// - `Error`: if there was an error writing the value.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, config::{PutOptions, WriteOptions}, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
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
    ) -> Result<(), crate::Error>
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
    /// - `Error`: if there was an error deleting the key.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.delete(b"key").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), crate::Error> {
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
    /// - `Error`: if there was an error deleting the key.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, config::WriteOptions, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
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
    ) -> Result<(), crate::Error> {
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
    /// - `Error`: if there was an error writing the batch.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{WriteBatch, Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
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
    pub async fn write(&self, batch: WriteBatch) -> Result<(), crate::Error> {
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
    /// - `Error`: if there was an error writing the batch.
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{WriteBatch, Db, config::WriteOptions, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
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
    ) -> Result<(), crate::Error> {
        self.inner
            .write_with_options(batch, options)
            .await
            .map_err(Into::into)
    }

    /// Flush in-memory writes to disk. This function blocks until the in-memory
    /// data has been durably written to object storage.
    ///
    /// If WAL is enabled, this method is equivalent to:
    /// `flush_with_options(FlushOptions { flush_type: FlushType::Wal })`
    ///
    /// If WAL is disabled, this method is equivalent to:
    /// `flush_with_options(FlushOptions { flush_type: FlushType::Memtable })`.
    ///
    /// ## Errors
    /// - `Error`: if there was an error flushing the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.flush().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn flush(&self) -> Result<(), crate::Error> {
        if self.inner.wal_enabled {
            self.flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
        } else {
            self.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
        }
    }

    /// Flush in-memory writes to disk with custom options.
    ///
    /// An error will be returned if `options.flush_type` is `FlushType::Wal` and the WAL
    /// is disabled.
    ///
    /// `FlushType::Memtable` is allowed even if WAL is enabled.
    ///
    /// ## Arguments
    /// - `options`: the flush options
    ///
    /// ## Returns
    /// - `Result<(), crate::Error>`: the result of the flush operation.
    ///
    /// ## Errors
    /// - `Error`: if there was an error flushing the database
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::config::{FlushOptions, FlushType};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.flush_with_options(FlushOptions {
    ///         flush_type: FlushType::Wal,
    ///     })
    ///     .await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn flush_with_options(&self, options: FlushOptions) -> Result<(), crate::Error> {
        match options.flush_type {
            FlushType::Wal => {
                if self.inner.wal_enabled {
                    self.inner.flush_wals().await
                } else {
                    Err(SlateDBError::WalDisabled)
                }
            }
            FlushType::MemTable => self.inner.flush_memtables().await,
        }
        .map_err(Into::into)
    }

    /// Get the metrics registry for the database.
    pub fn metrics(&self) -> Arc<StatRegistry> {
        self.inner.stat_registry.clone()
    }

    /// Resolve an object store from a URL.
    ///
    /// ## Arguments
    /// - `url`: the URL to resolve, for example `s3://my-bucket/my-prefix`.
    ///
    /// ## Returns
    /// - `Result<Arc<dyn ObjectStore>, crate::Error>`: the resolved object store
    pub fn resolve_object_store(url: &str) -> Result<Arc<dyn ObjectStore>, crate::Error> {
        let registry = DefaultObjectStoreRegistry::new();
        let url = url
            .try_into()
            .map_err(|e| SlateDBError::InvalidObjectStoreURL(url.to_string(), e))?;
        let (object_store, _) = registry.resolve(&url).map_err(SlateDBError::from)?;
        Ok(object_store)
    }
}

#[async_trait::async_trait]
impl DbRead for Db {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, options).await
    }

    async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, options).await
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use chrono::TimeDelta;
    use fail_parallel::FailPointRegistry;
    use std::collections::BTreeMap;
    use std::collections::Bound::Included;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;
    use crate::cached_object_store::stats::{
        OBJECT_STORE_CACHE_PART_ACCESS, OBJECT_STORE_CACHE_PART_HITS,
    };
    use crate::cached_object_store::{CachedObjectStore, FsCacheStorage};
    use crate::cached_object_store_stats::CachedObjectStoreStats;
    use crate::clock::DefaultSystemClock;
    use crate::config::DurabilityLevel::{Memory, Remote};
    use crate::config::{
        CompactorOptions, ObjectStoreCacheOptions, Settings, SizeTieredCompactionSchedulerOptions,
        Ttl,
    };
    use crate::db_state::CoreDbState;
    use crate::db_stats::IMMUTABLE_MEMTABLE_FLUSHES;
    use crate::iter::KeyValueIterator;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::arbitrary;
    use crate::proptest_util::sample;
    use crate::rand::DbRand;
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::test_utils::{assert_iterator, OnDemandCompactionSchedulerSupplier, TestClock};
    use crate::types::RowEntry;
    use crate::{proptest_util, test_utils, KeyValue};
    use futures::{future, future::join_all, StreamExt};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use proptest::test_runner::{TestRng, TestRunner};
    use tokio::runtime::Runtime;
    use tracing::info;

    #[tokio::test]
    async fn test_put_get_delete() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(test_db_options(0, 1024, None))
            .build()
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

    #[test]
    fn test_get_after_put() {
        let mut runner = new_proptest_runner(None);
        let runtime = Runtime::new().unwrap();

        let table = sample::table(runner.rng(), 1000, 10);
        let db_options = test_db_options(0, 1024, None);
        let db = runtime.block_on(build_database_from_table(&table, db_options, true));

        runner
            .run(
                &(arbitrary::bytes(100), arbitrary::bytes(100)),
                |(key, value)| {
                    runtime.block_on(async {
                        if !key.is_empty() {
                            db.put_with_options(
                                &key,
                                &value,
                                &PutOptions::default(),
                                &WriteOptions {
                                    await_durable: false,
                                },
                            )
                            .await
                            .unwrap();
                            assert_eq!(
                                Some(value),
                                db.get_with_options(
                                    &key,
                                    &ReadOptions {
                                        durability_filter: Memory,
                                        dirty: false,
                                    }
                                )
                                .await
                                .unwrap()
                            );
                        }
                    });
                    Ok(())
                },
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_no_flush_interval() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db_options_no_flush_interval = {
            let mut db_options = test_db_options(0, 1024, None);
            db_options.flush_interval = None;
            db_options
        };
        let kv_store = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(db_options_no_flush_interval)
            .build()
            .await
            .unwrap();
        let key = b"test_key";
        let value = b"test_value";

        kv_store
            .put_with_options(
                key,
                value,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // a sanity check: the wal contains the most recent write
        assert_ne!(
            kv_store.inner.wal_buffer.estimated_bytes().await.unwrap(),
            0
        );

        // and a flush() should clear it
        kv_store.flush().await.unwrap();
        assert_eq!(
            kv_store.inner.wal_buffer.estimated_bytes().await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_get_with_default_ttl_and_read_uncommitted() {
        let clock = Arc::new(TestClock::new());
        let ttl = 100;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(test_db_options_with_ttl(0, 1024, None, Some(ttl)))
            .with_logical_clock(clock.clone())
            .build()
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
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Memory))
                .await
                .unwrap(),
        );

        // advance clock to t=100 --> no longer returned
        clock.ticker.store(100, Ordering::SeqCst);
        assert_eq!(
            None,
            kv_store
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Memory))
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
        let kv_store = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(test_db_options_with_ttl(0, 1024, None, Some(default_ttl)))
            .with_logical_clock(clock.clone())
            .build()
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
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Memory))
                .await
                .unwrap(),
        );

        // advance clock to t=50 --> no longer returned
        clock.ticker.store(50, Ordering::SeqCst);
        assert_eq!(
            None,
            kv_store
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Memory))
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
        let kv_store = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(test_db_options_with_ttl(0, 1024, None, Some(ttl)))
            .with_logical_clock(clock.clone())
            .build()
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
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Remote))
                .await
                .unwrap(),
        );

        // advance clock to t=100 without flushing --> still returned
        clock.ticker.store(100, Ordering::SeqCst);
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Remote))
                .await
                .unwrap(),
        );

        // advance durable clock time to t=100 by flushing -- no longer returned
        kv_store.put(key_other, value).await.unwrap(); // fake data to advance clock
        kv_store.flush().await.unwrap();
        assert_eq!(
            None,
            kv_store
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Remote))
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
        let db = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(options)
            .build()
            .await
            .unwrap();
        let put_options = PutOptions::default();
        let write_options = WriteOptions {
            await_durable: false,
        };
        let get_memory_options = ReadOptions::new().with_durability_filter(Memory);
        let get_remote_options = ReadOptions::new().with_durability_filter(Remote);

        db.put_with_options(b"foo", b"bar", &put_options, &write_options)
            .await
            .unwrap();
        let val_bytes = Bytes::copy_from_slice(b"bar");
        assert_eq!(
            None,
            db.get_with_options(b"foo", &get_remote_options)
                .await
                .unwrap()
        );
        assert_eq!(
            Some(val_bytes.clone()),
            db.get_with_options(b"foo", &get_memory_options)
                .await
                .unwrap()
        );
        db.flush().await.unwrap();
        assert_eq!(
            Some(val_bytes.clone()),
            db.get_with_options(b"foo", &get_remote_options)
                .await
                .unwrap()
        );
        assert_eq!(
            Some(val_bytes.clone()),
            db.get_with_options(b"foo", &get_memory_options)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_find_with_multiple_repeated_keys() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut options = test_db_options(0, 1024 * 1024, None);
        options.wal_enabled = false;
        let db = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(options)
            .build()
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
            if db
                .inner
                .state
                .write()
                .memtable()
                .metadata()
                .entries_size_in_bytes
                > (SsTableFormat::default().block_size * 3)
            {
                break;
            }
        }
        assert_eq!(
            Some(Bytes::copy_from_slice(last_val.as_bytes())),
            db.get_with_options(b"key", &ReadOptions::new().with_durability_filter(Memory))
                .await
                .unwrap()
        );
        db.flush().await.unwrap();

        let state = db.inner.state.read().view();
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
        let kv_store = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(test_db_options_with_ttl(0, 1024, None, Some(ttl)))
            .with_logical_clock(clock.clone())
            .build()
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
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Remote))
                .await
                .unwrap(),
        );

        // advance clock to t=50 without flushing --> still returned
        clock.ticker.store(50, Ordering::SeqCst);
        assert_eq!(
            Some(Bytes::from_static(value)),
            kv_store
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Remote))
                .await
                .unwrap(),
        );

        // advance durable clock time to t=100 by flushing -- no longer returned
        kv_store.put(key_other, value).await.unwrap(); // fake data to advance clock
        kv_store.flush().await.unwrap();
        assert_eq!(
            None,
            kv_store
                .get_with_options(key, &ReadOptions::new().with_durability_filter(Remote))
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

        opts.object_store_cache_options.root_folder = Some(temp_dir.keep());
        opts.object_store_cache_options.part_size_bytes = 1024;
        let kv_store = Db::builder(
            "/tmp/test_kv_store_with_cache_metrics",
            object_store.clone(),
        )
        .with_settings(opts)
        .build()
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

    async fn test_object_store_cache_helper(
        cache_puts_enabled: bool,
        db_path: &str,
        expected_cache_parts: Vec<(&str, usize)>,
    ) -> (Arc<CachedObjectStore>, Db) {
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
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        ));

        let cached_object_store = CachedObjectStore::new(
            object_store.clone(),
            cache_storage,
            part_size,
            cache_puts_enabled,
            cache_stats.clone(),
        )
        .unwrap();

        opts.object_store_cache_options.root_folder = Some(temp_dir.keep());
        opts.object_store_cache_options.cache_puts = cache_puts_enabled;
        let kv_store = Db::builder(db_path, cached_object_store.clone())
            .with_settings(opts)
            .build()
            .await
            .unwrap();
        let key = b"test_key";
        let value = b"test_value";
        kv_store.put(key, value).await.unwrap();
        kv_store.flush().await.unwrap();

        // Verify cache behavior
        for (path, expected_parts) in expected_cache_parts {
            let entry = cached_object_store
                .cache_storage
                .entry(&object_store::path::Path::from(path), part_size);
            assert_eq!(
                entry.cached_parts().await.unwrap().len(),
                expected_parts,
                "Path: {}",
                path
            );
        }

        (cached_object_store, kv_store)
    }

    #[tokio::test]
    async fn test_get_with_object_store_cache_stored_files() {
        let expected_cache_parts =
            vec![
            ("tmp/test_kv_store_with_cache_stored_files/manifest/00000000000000000001.manifest", 0),
            ("tmp/test_kv_store_with_cache_stored_files/manifest/00000000000000000002.manifest", 2),
            ("tmp/test_kv_store_with_cache_stored_files/wal/00000000000000000001.sst", 2),
            ("tmp/test_kv_store_with_cache_stored_files/wal/00000000000000000002.sst", 0),
        ];

        let (_cached_object_store, kv_store) = test_object_store_cache_helper(
            false, // cache_puts disabled
            "/tmp/test_kv_store_with_cache_stored_files",
            expected_cache_parts,
        )
        .await;

        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_with_object_store_cache_put_caching_enabled() {
        let expected_cache_parts =
            vec![
            ("tmp/test_kv_store_with_put_cache_enabled/manifest/00000000000000000001.manifest", 2),
            ("tmp/test_kv_store_with_put_cache_enabled/manifest/00000000000000000002.manifest", 2),
            ("tmp/test_kv_store_with_put_cache_enabled/wal/00000000000000000001.sst", 2),
            ("tmp/test_kv_store_with_put_cache_enabled/wal/00000000000000000002.sst", 2),
        ];

        let (_cached_object_store, kv_store) = test_object_store_cache_helper(
            true, // cache_puts enabled
            "/tmp/test_kv_store_with_put_cache_enabled",
            expected_cache_parts,
        )
        .await;

        kv_store.close().await.unwrap();
    }

    async fn build_database_from_table(
        table: &BTreeMap<Bytes, Bytes>,
        db_options: Settings,
        await_durable: bool,
    ) -> Db {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(db_options)
            .build()
            .await
            .unwrap();

        test_utils::seed_database(&db, table, false).await.unwrap();

        if await_durable {
            db.flush().await.unwrap();
        }

        db
    }

    #[tokio::test]
    async fn test_should_allow_iterating_behind_box_dyn() {
        #[async_trait]
        trait IteratorSupplier {
            async fn iterator<'a>(&'a self) -> Box<dyn IteratorTrait + 'a>;
        }

        struct DbHolder {
            db: Db,
        }

        #[async_trait]
        impl IteratorSupplier for DbHolder {
            async fn iterator<'a>(&'a self) -> Box<dyn IteratorTrait + 'a> {
                let range = BytesRange::new_empty();
                let iter = self
                    .db
                    .inner
                    .scan_with_options(range, &ScanOptions::default())
                    .await
                    .unwrap();
                Box::new(iter)
            }
        }

        #[async_trait]
        trait IteratorTrait {
            async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error>;
        }

        #[async_trait]
        impl IteratorTrait for DbIterator<'_> {
            async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error> {
                DbIterator::next(self).await
            }
        }

        let db_options = test_db_options(0, 1024, None);
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(db_options)
            .build()
            .await
            .unwrap();
        let db_holder = DbHolder { db };
        let mut boxed = db_holder.iterator().await;
        let next = boxed.next().await;
        assert_eq!(next.unwrap(), None);
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
            let err = iter.seek(value.clone()).await.unwrap_err();
            assert!(
                err.to_string()
                    .contains("cannot seek to a key outside the iterator range"),
                "{}",
                err
            );

            let mut iter = db
                .scan_with_options(arbitrary_key.clone().., &ScanOptions::default())
                .await
                .unwrap();

            let upper_bounded_range = BytesRange::from(..arbitrary_key.clone());
            let value = sample::bytes_in_range(rng, &upper_bounded_range);
            let err = iter.seek(value.clone()).await.unwrap_err();
            assert!(
                err.to_string()
                    .contains("cannot seek to a key outside the iterator range"),
                "{}",
                err
            );
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
        let kv_store = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(test_db_options(0, 1024, None))
            .build()
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

        let kv_store = Db::builder("/tmp/test_kv_store_without_wal", object_store.clone())
            .with_settings(options)
            .build()
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
        let compaction_scheduler = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
            SizeTieredCompactionSchedulerOptions::default(),
        ));
        let kv_store = Arc::new(
            Db::builder("/tmp/test_concurrent_kv_store", object_store)
                .with_settings(test_db_options(
                    0,
                    1024,
                    // Enable compactor to prevent l0 from filling up and
                    // applying backpressure indefinitely.
                    Some(CompactorOptions {
                        poll_interval: Duration::from_millis(100),
                        max_sst_size: 256,
                        max_concurrent_compactions: 1,
                        manifest_update_timeout: Duration::from_secs(300),
                    }),
                ))
                .with_compaction_scheduler_supplier(compaction_scheduler)
                .build()
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
        let path = "/tmp/test_kv_store";
        // open a db and write a wal entry
        let options = test_db_options(0, 32, None);
        let db = Db::builder(path, object_store.clone())
            .with_settings(options)
            .build()
            .await
            .unwrap();
        db.put(&[b'a'; 4], &[b'j'; 4]).await.unwrap();
        db.put(&[b'b'; 4], &[b'k'; 4]).await.unwrap();
        db.close().await.unwrap();

        // open a db with wal disabled and write a memtable
        let mut options = test_db_options(0, 32, None);
        options.wal_enabled = false;
        let db = Db::builder(path, object_store.clone())
            .with_settings(options.clone())
            .build()
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
        let db = Db::builder(path, object_store.clone())
            .with_settings(options.clone())
            .build()
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
        let mut options = test_db_options(0, 256, None);
        options.wal_enabled = false;
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let sst_format = SsTableFormat::default();
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            sst_format,
            path.clone(),
            None,
        ));
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(options)
            .with_logical_clock(clock.clone())
            .build()
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
                .unwrap()
                .expect("Expected Some(iter) but got None");
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
        let path = "/tmp/test_kv_store";
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 256, None))
            .build()
            .await
            .unwrap();

        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
        let mut stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        let sst_format = SsTableFormat {
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            sst_format,
            path,
            None,
        ));

        // Write data a few times such that each loop results in a memtable flush
        let mut last_wal_id = 0;
        for i in 0..3 {
            let key = [b'a' + i; 16];
            let value = [b'b' + i; 50];
            kv_store.put(&key, &value).await.unwrap();
            let key = [b'j' + i; 16];
            let value = [b'k' + i; 50];
            kv_store.put(&key, &value).await.unwrap();
            let db_state = wait_for_manifest_condition(
                &mut stored_manifest,
                |s| s.replay_after_wal_id > last_wal_id,
                Duration::from_secs(30),
            )
            .await;

            // 2 wal per iteration.
            assert_eq!(db_state.replay_after_wal_id, (i as u64) * 2 + 2);
            last_wal_id = db_state.replay_after_wal_id
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
                    .unwrap()
                    .expect("Expected Some(iter) but got None");
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

    #[tokio::test]
    async fn test_flush_memtable_with_wal_enabled() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_flush_with_options";
        let mut options = test_db_options(0, 256, None);
        options.flush_interval = Some(Duration::from_secs(u64::MAX));
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(options)
            .build()
            .await
            .unwrap();

        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
        let mut stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        let sst_format = SsTableFormat {
            min_filter_keys: 10,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            sst_format,
            path,
            None,
        ));

        // Write some data to populate the memtable
        let key1 = b"test_key_1";
        let value1 = b"test_value_1";
        kv_store
            .put_with_options(
                key1,
                value1,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let key2 = b"test_key_2";
        let value2 = b"test_value_2";
        kv_store
            .put_with_options(
                key2,
                value2,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Get initial state
        let initial_manifest = stored_manifest.refresh().await.unwrap();
        let initial_l0_count = initial_manifest.core.l0.len();

        let initial_flush_count = kv_store
            .metrics()
            .lookup(IMMUTABLE_MEMTABLE_FLUSHES)
            .unwrap()
            .get();

        // Flush memtable using flush_with_options
        kv_store
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();

        // Wait for the flush to complete and manifest to be updated
        let db_state = wait_for_manifest_condition(
            &mut stored_manifest,
            |s| s.l0.len() > initial_l0_count,
            Duration::from_secs(30),
        )
        .await;

        // Verify that a new SST was created in L0
        assert_eq!(db_state.l0.len(), initial_l0_count + 1);

        // Verify that the flush metrics were updated
        let final_flush_count = kv_store
            .metrics()
            .lookup(IMMUTABLE_MEMTABLE_FLUSHES)
            .unwrap()
            .get();
        assert!(final_flush_count > initial_flush_count);

        // Verify that the WAL has not been flushed
        let recent_flushed_wal_id = kv_store.inner.wal_buffer.recent_flushed_wal_id();
        assert_eq!(recent_flushed_wal_id, 0);

        // Verify that the data is still accessible after flush
        let retrieved_value1 = kv_store.get(key1).await.unwrap().unwrap();
        assert_eq!(retrieved_value1.as_ref(), value1);

        let retrieved_value2 = kv_store.get(key2).await.unwrap().unwrap();
        assert_eq!(retrieved_value2.as_ref(), value2);

        // Verify the data exists in the newly created SST
        let latest_sst = db_state.l0.back().unwrap();
        let sst_iter_options = SstIteratorOptions::default();
        let mut iter =
            SstIterator::new_borrowed(.., latest_sst, table_store.clone(), sst_iter_options)
                .await
                .unwrap()
                .expect("Expected Some(iter) but got None");

        // Collect all key-value pairs from the SST
        let mut found_keys = std::collections::HashSet::new();
        while let Some(kv) = iter.next().await.unwrap() {
            found_keys.insert(kv.key.to_vec());
        }

        // Verify our keys are in the SST
        assert!(found_keys.contains(key1.as_slice()));
        assert!(found_keys.contains(key2.as_slice()));
    }

    #[tokio::test]
    async fn test_flush_with_options_wal() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_flush_with_options_wal";
        let mut options = test_db_options(0, 1024, None);
        // Larger memtable to avoid memtable flushes
        options.flush_interval = Some(Duration::from_secs(u64::MAX));
        // Fail all memtable writes before the DB starts, so we can be sure that
        // only the WAL is flushed.
        fail_parallel::cfg(
            fp_registry.clone(),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(options)
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();

        // Write some data to populate the WAL buffer
        let key1 = b"wal_test_key_1";
        let value1 = b"wal_test_value_1";
        kv_store
            .put_with_options(
                key1,
                value1,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let key2 = b"wal_test_key_2";
        let value2 = b"wal_test_value_2";
        kv_store
            .put_with_options(
                key2,
                value2,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Get initial WAL ID to verify flush occurred
        let initial_wal_id = kv_store.inner.wal_buffer.recent_flushed_wal_id();

        // Flush WAL using flush_with_options - this should succeed without error
        let flush_result = kv_store
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await;

        // Verify the flush operation completed successfully
        assert!(flush_result.is_ok(), "WAL flush should succeed");

        // Verify that the data is still accessible after WAL flush
        let retrieved_value1 = kv_store.get(key1).await.unwrap().unwrap();
        assert_eq!(retrieved_value1.as_ref(), value1);

        let retrieved_value2 = kv_store.get(key2).await.unwrap().unwrap();
        assert_eq!(retrieved_value2.as_ref(), value2);

        // Verify that the WAL buffer is in a consistent state after flush
        // The recent_flushed_wal_id should be at least as high as before
        let final_wal_id = kv_store.inner.wal_buffer.recent_flushed_wal_id();
        assert!(
            final_wal_id >= initial_wal_id,
            "WAL ID should not decrease after flush"
        );

        // Verify that the memtable has not been flushed by checking the db for error state
        assert!(
            kv_store.inner.check_error().is_ok(),
            "DB should not have an error state"
        );
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn test_flush_with_options_wal_disabled_error() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_flush_with_options_wal_disabled";
        let mut options = test_db_options(0, 1024, None);
        options.wal_enabled = false; // Disable WAL
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(options)
            .build()
            .await
            .unwrap();

        // Write some data to the database
        let key1 = b"test_key_1";
        let value1 = b"test_value_1";
        kv_store
            .put_with_options(
                key1,
                value1,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Attempt to flush WAL on a WAL-disabled database
        let flush_result = kv_store
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await;

        // Verify that we get the WalDisabled error
        assert!(flush_result.is_err(), "Expected WalDisabled error");
        let error = flush_result.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("attempted a WAL operation when the WAL is disabled"),
            "Expected WalDisabled error message, got: {}",
            error
        );

        // Verify that memtable flush still works when WAL is disabled
        let memtable_flush_result = kv_store
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await;
        assert!(
            memtable_flush_result.is_ok(),
            "Memtable flush should work even when WAL is disabled"
        );

        // Verify that the data is still accessible
        let retrieved_value1 = kv_store.get(key1).await.unwrap().unwrap();
        assert_eq!(retrieved_value1.as_ref(), value1);
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
        let db = Db::builder(path, object_store.clone())
            .with_settings(options)
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        let db_stats = db.inner.db_stats.clone();
        let write_opts = WriteOptions {
            await_durable: false,
        };

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();

        // Helper function to wait for a condition to be true.
        let wait_for = async move |condition: Box<dyn Fn() -> bool>| {
            for _ in 0..3000 {
                if condition() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        // 1 wal entry in memory
        db.put_with_options(b"key1", b"val1", &PutOptions::default(), &write_opts)
            .await
            .unwrap();

        // Wait for put to end up in the WAL buffer
        let this_wal_buffer = db.inner.wal_buffer.clone();
        wait_for(Box::new(move || {
            this_wal_buffer.buffered_wal_entries_count() > 0
        }))
        .await;

        // Verify that there is now 1 WAL entry in memory.
        assert_eq!(db.inner.wal_buffer.buffered_wal_entries_count(), 1);

        // Put another WAL entry, which should trigger backpressure. Do this in a separate
        // task since the put() is blocked until the WAL is flushed, which isn't happening
        // due to the fail point.
        let join_handle = tokio::spawn(async move {
            db.put_with_options(b"key2", b"val2", &PutOptions::default(), &write_opts)
                .await
                .unwrap();
        });

        let this_stats = db_stats.clone();
        // Wait up to 30s for backpressure to be applied to the second write.
        wait_for(Box::new(move || {
            this_stats.backpressure_count.value.load(Ordering::SeqCst) > 0
        }))
        .await;

        // Verify that backpressure is applied.
        assert!(db_stats.backpressure_count.value.load(Ordering::SeqCst) >= 1);

        // Unblock so put_with_options in join_handle can complete and join_handle.await returns
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "off").unwrap();

        // Shutdown the background task
        join_handle.abort();
        let _ = join_handle.await;
    }

    #[tokio::test]
    async fn test_apply_backpressure_to_memtable_flush() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut options = test_db_options(0, 1, None);
        options.l0_max_ssts = 4;
        let db = Db::builder("/tmp/test_kv_store", object_store.clone())
            .with_settings(options)
            .build()
            .await
            .unwrap();
        db.put(b"key1", b"val1").await.unwrap();
        db.put(b"key2", b"val2").await.unwrap();
        db.put(b"key3", b"val3").await.unwrap();
        db.put(b"key4", b"val4").await.unwrap();
        db.put(b"key5", b"val5").await.unwrap();

        db.flush().await.unwrap();

        let db_state = db.inner.state.read().view();
        assert_eq!(db_state.state.imm_memtable.len(), 1);
    }

    #[tokio::test]
    async fn test_put_empty_value() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::builder("/tmp/test_kv_store", object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .build()
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
        let kv_store = Db::builder("/tmp/test_kv_store", object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_logical_clock(Arc::new(TestClock::new()))
            .build()
            .await
            .unwrap();

        let memtable = {
            let lock = kv_store.inner.state.read();
            lock.memtable()
                .put(RowEntry::new_value(b"abc1111", b"value1111", 1));
            lock.memtable()
                .put(RowEntry::new_value(b"abc2222", b"value2222", 2));
            lock.memtable()
                .put(RowEntry::new_value(b"abc3333", b"value3333", 3));
            lock.memtable().table().clone()
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
        let path = "/tmp/test_kv_store";
        let mut next_wal_id = 1;
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_logical_clock(Arc::new(TestClock::new()))
            .build()
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
        let kv_store_restored = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_logical_clock(Arc::new(TestClock::new()))
            .build()
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
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
        let _stored_manifest = StoredManifest::load(manifest_store).await.unwrap();
        // Use oracle value instead of last_seen_wal_id
        assert_eq!(
            kv_store_restored.inner.oracle.next_wal_id.load(),
            next_wal_id
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_restore_seq_number() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 256, None))
            .with_logical_clock(Arc::new(TestClock::new()))
            .build()
            .await
            .unwrap();

        db.put(b"key1", b"val1").await.unwrap();
        db.put(b"key2", b"val2").await.unwrap();
        db.put(b"key3", b"val3").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let db_restored = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 256, None))
            .with_logical_clock(Arc::new(TestClock::new()))
            .build()
            .await
            .unwrap();

        let state = db_restored.inner.state.read();
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

    #[tokio::test]
    async fn test_all_kv_seq_num_are_greater_than_0() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store_seq_num";
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 1024 * 1024, None))
            .build()
            .await
            .unwrap();

        // Write some data to memtable
        db.put(b"key1", b"value1").await.unwrap();

        let val = db.get(b"key1").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"value1")));

        let state = db.inner.state.read();
        let memtable = state.memtable();
        assert_eq!(memtable.table().last_seq(), Some(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_should_read_uncommitted_data_if_read_level_uncommitted() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_fp_registry(fp_registry.clone())
            .build()
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
                &ReadOptions::new().with_durability_filter(Memory),
            )
            .await
            .unwrap();
        assert_eq!(val, Some("bar".into()));

        // Validate committed read should still return None
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions::new().with_durability_filter(Remote),
            )
            .await
            .unwrap();
        assert_eq!(val, None);

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "off").unwrap();
        kv_store.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_should_read_only_committed_data() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_fp_registry(fp_registry.clone())
            .build()
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

        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions::new().with_durability_filter(Remote),
            )
            .await
            .unwrap();
        assert_eq!(val, Some("bar".into()));
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions::new().with_durability_filter(Memory),
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
        let path = "/tmp/test_kv_store";
        let kv_store = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_fp_registry(fp_registry.clone())
            .build()
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

        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions::new().with_durability_filter(Remote),
            )
            .await
            .unwrap();
        assert_eq!(val, Some("bar".into()));
        let val = kv_store
            .get_with_options(
                "foo".as_bytes(),
                &ReadOptions::new().with_durability_filter(Memory),
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
        let path = "/tmp/test_kv_store";
        let mut next_wal_id = 1;
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_fp_registry(fp_registry.clone())
            .build()
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

        let reader = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();

        // increment wal id for the empty wal
        next_wal_id += 1;

        // verify that we reload imm
        let db_next_wal_id = reader.inner.oracle.next_wal_id.load();
        assert_eq!(db_next_wal_id, next_wal_id);

        let db_state = reader.inner.state.read().view();
        assert_eq!(db_state.state.imm_memtable.len(), 2);

        // one empty wal and two wals for the puts
        assert_eq!(
            db_state
                .state
                .imm_memtable
                .front()
                .unwrap()
                .recent_flushed_wal_id(),
            1 + 2
        );
        assert_eq!(
            db_state
                .state
                .imm_memtable
                .get(1)
                .unwrap()
                .recent_flushed_wal_id(),
            2
        );
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
        let path = "/tmp/test_kv_store";
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();

        // write a few keys that will result in memtable flushes
        let key1 = [b'a'; 32];
        let value1 = [b'b'; 96];
        let result = db.put(&key1, &value1).await;
        assert!(result.is_ok(), "Failed to write key1");
        assert_eq!(db.inner.wal_buffer.recent_flushed_wal_id(), 2);

        let flush_result = db.inner.flush_immutable_memtables().await;
        assert!(flush_result.is_err());
        db.close().await.unwrap();

        // pause write-compacted-sst-io-error to prevent immutable tables
        // from being flushed, so we can snapshot the state when there is
        // an immutable table to verify its contents.
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "pause").unwrap();

        // reload the db
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();

        // verify that we reload imm
        let db_state = db.inner.state.read().view();

        // resume write-compacted-sst-io-error since we got a snapshot and
        // want to let the test finish.
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();

        assert_eq!(db_state.state.imm_memtable.len(), 1);

        // one empty wal and one wal for the first put
        assert_eq!(
            db_state
                .state
                .imm_memtable
                .front()
                .unwrap()
                .recent_flushed_wal_id(),
            1 + 1
        );
        assert!(db_state.state.imm_memtable.get(1).is_none());

        // Use oracle value instead of last_seen_wal_id
        assert_eq!(db.inner.oracle.next_wal_id.load(), 4);
        assert_eq!(
            db.get(key1).await.unwrap(),
            Some(Bytes::copy_from_slice(&value1))
        );
    }

    #[tokio::test]
    async fn test_should_fail_write_if_wal_flush_task_panics() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let db = Arc::new(
            Db::builder(path, object_store.clone())
                .with_settings(test_db_options(0, 128, None))
                .with_fp_registry(fp_registry.clone())
                .build()
                .await
                .unwrap(),
        );

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "panic").unwrap();
        let result = db.put(b"foo", b"bar").await.unwrap_err();
        assert!(result.to_string().contains("background task panic'd"));
    }

    // TODO(flaneur2020): it seems that the mem_table_flusher's background task will get the error
    // propogated when the wal background task panics, and will NOT flush the latest manifest to the
    // object_store after error. there might be other reasons this test could pass, hmm
    #[ignore]
    #[tokio::test]
    async fn test_wal_id_last_seen_should_exist_even_if_wal_write_fails() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let db = Arc::new(
            Db::builder(path, object_store.clone())
                .with_settings(test_db_options(0, 128, None))
                .with_fp_registry(fp_registry.clone())
                .build()
                .await
                .unwrap(),
        );

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "panic").unwrap();

        // Trigger a WAL write, which should not advance the manifest WAL ID
        let result = db.put(b"foo", b"bar").await.unwrap_err();
        assert_eq!(
            result.to_string(),
            "System error: background task panic'd (failpoint write-wal-sst-io-error panic)"
        );

        // Close, which flushes the latest manifest to the object store
        // TODO: it might make sense to return an error if there're unflushed wals in memory
        // on close().
        db.close().await.unwrap();

        let manifest_store = ManifestStore::new(&Path::from(path), object_store.clone());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store.clone(), None),
            SsTableFormat::default(),
            path,
            None,
        ));

        // Get the next WAL SST ID based on what's currently in the object store
        let next_wal_sst_id = table_store.next_wal_sst_id(0).await.unwrap();

        // Get the latest manifest
        let (_, manifest) = manifest_store.read_latest_manifest().await.unwrap();

        // It's possible that there exists buffered multiple wals in memory, so the next_wal_sst_id
        // in manifest is greater than the next_wal_sst_id based on what's currently in the object
        // store unless ALL the wals are flushed.
        assert!(
            manifest.core.last_seen_wal_id > next_wal_sst_id,
            "last_seen_wal_id: {}, next_wal_sst_id: {}",
            manifest.core.last_seen_wal_id,
            next_wal_sst_id
        );
    }

    async fn do_test_should_read_compacted_db(options: Settings) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let compaction_scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new());

        let db = Db::builder(path, object_store.clone())
            .with_settings(options)
            .with_compaction_scheduler_supplier(compaction_scheduler.clone())
            .build()
            .await
            .unwrap();
        let ms = ManifestStore::new(&Path::from(path), object_store.clone());
        let mut sm = StoredManifest::load(Arc::new(ms)).await.unwrap();

        // write enough to fill up a few l0 SSTs
        for i in 0..4 {
            db.put(&[b'a' + i; 32], &[1u8 + i; 32]).await.unwrap();
            db.put(&[b'm' + i; 32], &[13u8 + i; 32]).await.unwrap();
        }
        // wait for compactor to compact them
        wait_for_manifest_condition(
            &mut sm,
            |s| {
                // compact after writing values. include in loop since the on demand scheduler
                // only runs once per `should_compact`, and memtables might still be getting
                // flushed (await_durable in the put()'s above only wait for the writes to hit
                // the WAL before returning).
                compaction_scheduler
                    .scheduler
                    .should_compact
                    .store(true, Ordering::SeqCst);
                s.l0_last_compacted.is_some() && s.l0.is_empty()
            },
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
        // wait for compactor to compact them
        wait_for_manifest_condition(
            &mut sm,
            |s| {
                // compact after writing values. include in loop since the on demand scheduler
                // only runs once per `should_compact`, and memtables might still be getting
                // flushed (await_durable in the put()'s above only wait for the writes to hit
                // the WAL before returning).
                compaction_scheduler
                    .scheduler
                    .should_compact
                    .store(true, Ordering::SeqCst);
                s.l0_last_compacted.is_some() && s.l0.is_empty()
            },
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_read_from_compacted_db() {
        do_test_should_read_compacted_db(test_db_options(
            0,
            127,
            Some(CompactorOptions {
                poll_interval: Duration::from_millis(100),
                max_sst_size: 256,
                max_concurrent_compactions: 1,
                manifest_update_timeout: Duration::from_secs(300),
            }),
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_should_read_from_compacted_db_no_filters() {
        do_test_should_read_compacted_db(test_db_options(
            u32::MAX,
            127,
            Some(CompactorOptions {
                poll_interval: Duration::from_millis(100),
                manifest_update_timeout: Duration::from_secs(300),
                max_sst_size: 256,
                max_concurrent_compactions: 1,
            }),
        ))
        .await
    }

    #[tokio::test]
    async fn test_db_open_should_write_empty_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        // assert that open db writes an empty wal.
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .build()
            .await
            .unwrap();
        assert_eq!(db.inner.oracle.next_wal_id.load(), 2);
        db.put(b"1", b"1").await.unwrap();
        // assert that second open writes another empty wal.
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .build()
            .await
            .unwrap();
        assert_eq!(db.inner.oracle.next_wal_id.load(), 4);
    }

    #[tokio::test]
    async fn test_empty_wal_should_fence_old_writer() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";

        async fn do_put(db: &Db, key: &[u8], val: &[u8]) -> Result<(), crate::Error> {
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
        let db1 = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .build()
            .await
            .unwrap();
        do_put(&db1, b"1", b"1").await.unwrap();

        // open db2, causing it to write an empty wal and fence db1.
        let db2 = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .build()
            .await
            .unwrap();

        // assert that db1 can no longer write.
        let err = do_put(&db1, b"1", b"1").await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Permission error: detected newer DB client"
        );

        do_put(&db2, b"2", b"2").await.unwrap();
        // Use oracle value instead of last_seen_wal_id
        assert_eq!(db2.inner.oracle.next_wal_id.load(), 5);
    }

    #[tokio::test]
    async fn test_invalid_clock_progression() {
        // Given:
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";

        let clock = Arc::new(TestClock::new());
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_logical_clock(clock.clone())
            .build()
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
            Err(e) => assert_eq!(e.to_string(), "Operation error: invalid clock tick, must be monotonic. last_tick=`10`, next_tick=`5`"),
        }
    }

    #[tokio::test]
    async fn test_invalid_clock_progression_across_db_instances() {
        // Given:
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";

        let clock = Arc::new(TestClock::new());
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_logical_clock(clock.clone())
            .build()
            .await
            .unwrap();

        // When:
        // put with time = 10
        clock.ticker.store(10, Ordering::SeqCst);
        db.put(b"1", b"1").await.unwrap();
        db.flush().await.unwrap();

        let db2 = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 128, None))
            .with_logical_clock(clock.clone())
            .build()
            .await
            .unwrap();
        clock.ticker.store(5, Ordering::SeqCst);
        match db2.put(b"1", b"1").await {
            Ok(_) => panic!("expected an error on inserting backwards time"),
            Err(e) => assert_eq!(e.to_string(), "Operation error: invalid clock tick, must be monotonic. last_tick=`10`, next_tick=`5`"),
        }
    }

    #[tokio::test]
    #[cfg(feature = "wal_disable")]
    async fn should_flush_all_memtables_when_wal_disabled() {
        // Given:
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";

        let db_options = Settings {
            wal_enabled: false,
            flush_interval: Some(Duration::from_secs(10)),
            ..Settings::default()
        };

        let db = Db::builder(path, object_store.clone())
            .with_settings(db_options.clone())
            .build()
            .await
            .unwrap();

        let mut rng = proptest_util::rng::new_test_rng(None);
        let table = sample::table(&mut rng, 1000, 5);
        test_utils::seed_database(&db, &table, false).await.unwrap();
        db.flush().await.unwrap();

        // When: reopen the database without closing the old instance
        let reopened_db = Db::builder(path, object_store.clone())
            .with_settings(db_options.clone())
            .build()
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
        let path = "/tmp/test_kv_store";

        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_logical_clock(clock.clone())
            .build()
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
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
        let stored_manifest = StoredManifest::load(manifest_store).await.unwrap();
        let db_state = stored_manifest.db_state();
        let last_clock_tick = db_state.last_l0_clock_tick;
        assert_eq!(last_clock_tick, i64::MIN);

        let clock = Arc::new(TestClock::new());
        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_logical_clock(clock.clone())
            .build()
            .await
            .unwrap();

        assert_eq!(db.inner.mono_clock.last_tick.load(Ordering::SeqCst), 11);
    }

    #[tokio::test]
    async fn test_should_update_manifest_clock_tick_on_l0_flush() {
        let clock = Arc::new(TestClock::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";

        let db = Db::builder(path, object_store.clone())
            .with_settings(test_db_options(0, 32, None))
            .with_logical_clock(clock.clone())
            .build()
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
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
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
        let path = "/tmp/test_kv_store";
        let mut options = test_db_options(0, 32, None);
        options.wal_enabled = false;

        let db = Db::builder(path, object_store.clone())
            .with_settings(options)
            .with_logical_clock(clock.clone())
            .build()
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
        let mut options = test_db_options(0, 32, None);
        options.wal_enabled = false;
        let db = Db::builder(path, object_store.clone())
            .with_settings(options)
            .with_logical_clock(clock.clone())
            .build()
            .await
            .unwrap();

        assert_eq!(db.inner.mono_clock.last_tick.load(Ordering::SeqCst), 11);
    }

    #[tokio::test]
    async fn test_put_get_reopen_delete_with_separate_wal_store() {
        async fn count_ssts_in(store: &Arc<InMemory>) -> usize {
            store
                .list(None)
                .filter(|r| {
                    future::ready(
                        r.as_ref()
                            .unwrap()
                            .location
                            .extension()
                            .unwrap()
                            .to_lowercase()
                            == "sst",
                    )
                })
                .count()
                .await
        }

        let fp_registry = Arc::new(FailPointRegistry::new());

        let main_object_store = Arc::new(InMemory::new());
        let wal_object_store = Arc::new(InMemory::new());
        let kv_store = Db::builder("/tmp/test_kv_store", main_object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_wal_object_store(wal_object_store.clone())
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(count_ssts_in(&main_object_store).await, 0);
        assert_eq!(count_ssts_in(&wal_object_store).await, 1);

        let key = b"test_key";
        let value = b"test_value";

        // pause memtable flushes
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "pause").unwrap();
        kv_store.put(key, value).await.unwrap();
        kv_store.flush().await.unwrap();
        assert_eq!(count_ssts_in(&main_object_store).await, 0);
        assert_eq!(count_ssts_in(&wal_object_store).await, 2);
        assert_eq!(
            kv_store.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
        // resume memtable flushes
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();

        // write some data to force L0 SST creation
        let mut batch = WriteBatch::default();
        for i in 0u32..128 {
            batch.put(i.to_be_bytes(), i.to_be_bytes());
        }
        kv_store.write(batch).await.unwrap();
        kv_store.flush().await.unwrap();
        assert_eq!(count_ssts_in(&main_object_store).await, 1);
        assert_eq!(count_ssts_in(&wal_object_store).await, 3);
        assert_eq!(
            kv_store.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );

        kv_store.close().await.unwrap();
        assert_eq!(count_ssts_in(&wal_object_store).await, 3);

        let kv_store = Db::builder("/tmp/test_kv_store", main_object_store)
            .with_settings(test_db_options(0, 1024, None))
            .with_wal_object_store(wal_object_store.clone())
            .build()
            .await
            .unwrap();

        assert_eq!(
            kv_store.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );

        kv_store.delete(key).await.unwrap();
        assert_eq!(None, kv_store.get(key).await.unwrap());

        kv_store.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_memtable_flush_cleanup_when_fenced() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_flush_cleanup";
        let fp_registry = Arc::new(FailPointRegistry::new());

        let mut options = test_db_options(0, 32, None);
        options.flush_interval = None;
        options.manifest_poll_interval = TimeDelta::MAX.to_std().unwrap();

        let db1 = Db::builder(path, object_store.clone())
            .with_settings(options.clone())
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();

        // Allow WAL flushes, but pause compacted (L0 SST) flushes. Have to do this because the
        // WAL sometimes triggers a maybe_memtable_flush. We don't want the memtable flush to
        // proceed until the fence happens below..
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "pause").unwrap();
        db1.put(b"k", b"v").await.unwrap();

        // Fence the db by opening a new one
        let manifest_store = Arc::new(ManifestStore::new(&Path::from(path), object_store.clone()));
        let stored_manifest = StoredManifest::load(manifest_store.clone()).await.unwrap();
        FenceableManifest::init_writer(
            stored_manifest,
            Duration::from_secs(300),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        // Unpause to allow L0 SST writes to proceed
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();

        // Try to flush memtables, but they should fail due to the fence
        let result = db1.inner.flush_memtables().await;
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(db1
            .inner
            .table_store
            .list_compacted_ssts(..)
            .await
            .unwrap()
            .is_empty());

        db1.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_store_reconfiguration_fails() {
        let object_store = Arc::new(InMemory::new());
        let wal_object_store = Arc::new(InMemory::new());

        let kv_store = Db::builder("/tmp/test_kv_store", object_store.clone())
            .with_settings(test_db_options(0, 1024, None))
            .with_wal_object_store(wal_object_store.clone())
            .build()
            .await
            .unwrap();
        kv_store.close().await.unwrap();

        let result = Db::builder("/tmp/test_kv_store", object_store)
            .with_settings(test_db_options(0, 1024, None))
            .build()
            .await;
        match result {
            Err(err) => {
                assert!(err.to_string().contains("unsupported"));
            }
            _ => panic!("expected Unsupported error"),
        }
    }

    #[test]
    fn test_write_option_defaults() {
        // This is a regression test for a bug where the defaults for WriteOptions were not being
        // set correctly due to visibility issues.
        let write_options = WriteOptions::default();
        assert!(write_options.await_durable);
    }

    #[tokio::test]
    #[cfg(feature = "zstd")]
    async fn test_compression_overflow_bug() {
        // This test reproduces the bug reported in https://github.com/slatedb/slatedb/issues/555
        // where re-opening a DB using zstd compression causes "attempt to subtract with overflow"
        // error in Block::decode

        use crate::config::CompressionCodec;
        use std::str::FromStr;

        // Create and load initial database
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let compress = CompressionCodec::from_str("zstd").unwrap();
        let db_builder = Db::builder("/tmp/test_kv_store", os.clone()).with_settings(Settings {
            compression_codec: Some(compress),
            ..Settings::default()
        });
        let db = db_builder.build().await.unwrap();

        for i in 0..1000 {
            let key = format!("k{}", i);
            let value = format!("{}{}", "v".repeat(i), i);
            let put_option = PutOptions::default();
            let write_option = WriteOptions {
                await_durable: false,
            };
            db.put_with_options(key.as_bytes(), value.clone(), &put_option, &write_option)
                .await
                .expect("failed to put");
        }
        db.flush().await.expect("flush failed");
        db.close().await.expect("failed to close db");

        // Reload DB and read a value to trigger error
        let db_builder = Db::builder("/tmp/test_kv_store", os.clone()).with_settings(Settings {
            compression_codec: Some(compress),
            ..Settings::default()
        });
        let db = db_builder.build().await.unwrap();
        let v = db.get("k1").await.expect("get failed").unwrap();
        assert_eq!(v.as_ref(), b"v1");

        db.close().await.expect("failed to close db");
    }

    async fn wait_for_manifest_condition(
        sm: &mut StoredManifest,
        cond: impl Fn(&CoreDbState) -> bool,
        timeout: Duration,
    ) -> CoreDbState {
        let start = tokio::time::Instant::now();
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
    ) -> Settings {
        test_db_options_with_ttl(min_filter_keys, l0_sst_size_bytes, compactor_options, None)
    }

    fn test_db_options_with_ttl(
        min_filter_keys: u32,
        l0_sst_size_bytes: usize,
        compactor_options: Option<CompactorOptions>,
        ttl: Option<u64>,
    ) -> Settings {
        Settings {
            flush_interval: Some(Duration::from_millis(100)),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            manifest_update_timeout: Duration::from_secs(300),
            max_unflushed_bytes: 134_217_728,
            l0_max_ssts: 8,
            min_filter_keys,
            filter_bits_per_key: 10,
            l0_sst_size_bytes,
            compactor_options,
            compression_codec: None,
            object_store_cache_options: ObjectStoreCacheOptions::default(),
            garbage_collector_options: None,
            default_ttl: ttl,
        }
    }

    #[tokio::test]
    async fn test_snapshot_basic_functionality() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open("test_db", object_store).await.unwrap();

        // Write some data
        db.put(b"key1", b"value1").await.unwrap();
        db.put(b"key2", b"value2").await.unwrap();

        // Create a snapshot
        let snapshot = db.snapshot().await.unwrap();

        // Verify snapshot can read the data
        assert_eq!(
            snapshot.get(b"key1").await.unwrap(),
            Some(Bytes::from(b"value1".as_ref()))
        );
        assert_eq!(
            snapshot.get(b"key2").await.unwrap(),
            Some(Bytes::from(b"value2".as_ref()))
        );

        // Write more data to the original database
        db.put(b"key3", b"value3").await.unwrap();

        // Snapshot should not see the new data
        assert_eq!(snapshot.get(b"key3").await.unwrap(), None);

        // Original database should see the new data
        assert_eq!(
            db.get(b"key3").await.unwrap(),
            Some(Bytes::from(b"value3".as_ref()))
        );
    }

    #[tokio::test]
    async fn test_recent_snapshot_min_seq_monotonic() {
        let path = "/tmp/test_recent_snapshot_min_seq_monotonic";
        let object_store = Arc::new(InMemory::new());
        let settings = Settings {
            l0_sst_size_bytes: 4 * 1024,   // Smaller to trigger flush more easily
            max_unflushed_bytes: 2 * 1024, // Smaller to trigger flush more easily
            min_filter_keys: 0,
            flush_interval: Some(Duration::from_millis(100)),
            ..Default::default()
        };

        let db = Db::builder(path, object_store)
            .with_settings(settings)
            .build()
            .await
            .unwrap();

        // Initial state: recent_snapshot_min_seq should be 0
        {
            let state = db.inner.state.read();
            assert_eq!(state.state().core().recent_snapshot_min_seq, 0);
        }

        // Test 1: Force memtable flush to update recent_snapshot_min_seq
        db.put(b"key1", b"value1").await.unwrap();
        db.inner.flush_memtables().await.unwrap();

        {
            let state = db.inner.state.read();
            let recent_min_seq = state.state().core().recent_snapshot_min_seq;
            // After flush, recent_snapshot_min_seq should be updated (no active snapshots)
            assert!(
                recent_min_seq > 0,
                "recent_snapshot_min_seq should be > 0 after flush"
            );
        }

        // Test 2: With active snapshots
        let _snapshot = db.snapshot().await.unwrap();
        let snapshot_seq = db.inner.oracle.last_committed_seq.load();

        // Write more data and force flush
        db.put(b"key2", b"value2").await.unwrap();
        db.inner.flush_memtables().await.unwrap();

        // Verify that txn_manager.min_active_seq() returns the snapshot seq
        let min_active_seq = db.inner.txn_manager.min_active_seq();
        assert!(min_active_seq.is_some());
        assert_eq!(min_active_seq.unwrap(), snapshot_seq);

        // Verify recent_snapshot_min_seq should be the minimum active seq after flush
        {
            let state = db.inner.state.read();
            let recent_min_seq = state.state().core().recent_snapshot_min_seq;
            assert_eq!(
                recent_min_seq,
                min_active_seq.unwrap(),
                "recent_snapshot_min_seq should equal min_active_seq after flush"
            );
        }

        // Test 3: Drop snapshot and check update
        drop(_snapshot);

        // Write more data and flush to trigger update
        db.put(b"key3", b"value3").await.unwrap();
        db.inner.flush_memtables().await.unwrap();

        // Now recent_snapshot_min_seq should be updated to higher value (no active snapshots)
        {
            let state = db.inner.state.read();
            let recent_min_seq = state.state().core().recent_snapshot_min_seq;
            let last_l0_seq = state.state().core().last_l0_seq;

            // Should be updated to last_l0_seq since no active snapshots
            assert_eq!(
                recent_min_seq, last_l0_seq,
                "recent_snapshot_min_seq should equal last_l0_seq when no active snapshots"
            );
            assert!(recent_min_seq > snapshot_seq);
        }
    }
}
