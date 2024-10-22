use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::Handle;

use crate::batch::WriteBatch;
use crate::batch_write::{WriteBatchMsg, WriteBatchRequest};
use crate::cached_object_store::CachedObjectStore;
use crate::compactor::Compactor;
use crate::config::ReadLevel::Uncommitted;
use crate::config::{
    DbOptions, ReadOptions, WriteOptions, DEFAULT_READ_OPTIONS, DEFAULT_WRITE_OPTIONS,
};
use crate::db_state::{CoreDbState, DbState, SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::filter;
use crate::flush::WalFlushThreadMsg;
use crate::garbage_collector::GarbageCollector;
use crate::iter::KeyValueIterator;
use crate::manifest_store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::mem_table::WritableKVTable;
use crate::mem_table_flush::MemtableFlushThreadMsg;
use crate::metrics::DbStats;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst::SsTableFormat;
use crate::sst_iter::SstIterator;
use crate::tablestore::TableStore;
use crate::types::{RowAttributes, ValueDeletable};
use std::rc::Rc;

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<DbState>>,
    pub(crate) options: DbOptions,
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) wal_flush_notifier: tokio::sync::mpsc::UnboundedSender<WalFlushThreadMsg>,
    pub(crate) memtable_flush_notifier: tokio::sync::mpsc::UnboundedSender<MemtableFlushThreadMsg>,
    pub(crate) write_notifier: tokio::sync::mpsc::UnboundedSender<WriteBatchMsg>,
    pub(crate) db_stats: Arc<DbStats>,
}

impl DbInner {
    pub async fn new(
        options: DbOptions,
        table_store: Arc<TableStore>,
        core_db_state: CoreDbState,
        wal_flush_notifier: tokio::sync::mpsc::UnboundedSender<WalFlushThreadMsg>,
        memtable_flush_notifier: tokio::sync::mpsc::UnboundedSender<MemtableFlushThreadMsg>,
        write_notifier: tokio::sync::mpsc::UnboundedSender<WriteBatchMsg>,
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
        };
        Ok(db_inner)
    }

    /// Get the value for a given key.
    pub async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
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

        for sst in &snapshot.state.core.l0 {
            if self.sst_might_include_key(sst, key, key_hash).await? {
                let mut iter =
                    SstIterator::new_from_key(sst, self.table_store.clone(), key, 1, 1, true)
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
                let mut iter =
                    SortedRunIterator::new_from_key(sr, key, self.table_store.clone(), 1, 1, true) // cache blocks
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

    /// Put a key-value pair into the database. Key must not be empty.
    #[allow(clippy::panic)]
    pub async fn put_with_options(&self, key: &[u8], value: &[u8], options: &WriteOptions) {
        assert!(!key.is_empty(), "key cannot be empty");

        self.maybe_apply_backpressure().await;

        let key = Bytes::copy_from_slice(key);
        let value = Bytes::copy_from_slice(value);

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_table = if self.wal_enabled() {
            let mut guard = self.state.write();
            let current_wal = guard.wal();
            current_wal.put(
                key,
                value,
                RowAttributes {
                    ts: Some(self.options.clock.now()),
                },
            );
            current_wal.table().clone()
        } else {
            if cfg!(not(feature = "wal_disable")) {
                panic!("wal_disabled feature must be enabled");
            }
            let mut guard = self.state.write();
            let current_memtable = guard.memtable();
            current_memtable.put(
                key,
                value,
                RowAttributes {
                    ts: Some(self.options.clock.now()),
                },
            );
            let table = current_memtable.table().clone();
            let last_wal_id = guard.last_written_wal_id();
            self.maybe_freeze_memtable(&mut guard, last_wal_id);
            table
        };

        if options.await_durable {
            current_table.await_durable().await;
        }
    }

    /// Delete a key from the database. Key must not be empty.
    #[allow(clippy::panic)]
    pub async fn delete_with_options(&self, key: &[u8], options: &WriteOptions) {
        assert!(!key.is_empty(), "key cannot be empty");

        self.maybe_apply_backpressure().await;

        let key = Bytes::copy_from_slice(key);

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_table = if self.wal_enabled() {
            let mut guard = self.state.write();
            let current_wal = guard.wal();
            current_wal.delete(
                key,
                RowAttributes {
                    ts: Some(self.options.clock.now()),
                },
            );
            current_wal.table().clone()
        } else {
            if cfg!(not(feature = "wal_disable")) {
                panic!("wal_disabled feature must be enabled");
            }
            let mut guard = self.state.write();
            let current_memtable = guard.memtable();
            current_memtable.delete(
                key,
                RowAttributes {
                    ts: Some(self.options.clock.now()),
                },
            );
            let table = current_memtable.table().clone();
            let last_wal_id = guard.last_written_wal_id();
            self.maybe_freeze_memtable(&mut guard, last_wal_id);
            table
        };

        if options.await_durable {
            current_table.await_durable().await;
        }
    }

    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        if batch.ops.is_empty() {
            return Ok(());
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch_msg = WriteBatchMsg::WriteBatch(WriteBatchRequest { batch, done: tx });

        self.write_notifier.send(batch_msg).ok();

        let current_table = rx.await.expect("write batch failed")?;

        if options.await_durable {
            current_table.await_durable().await;
        }

        Ok(())
    }

    async fn flush_wals(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.wal_flush_notifier
            .send(WalFlushThreadMsg::FlushImmutableWals(Some(tx)))
            .expect("wal flush hung up");
        rx.await.expect("received error on wal flush")
    }

    // use to manually flush memtables
    async fn flush_memtables(&self) -> Result<(), SlateDBError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.memtable_flush_notifier
            .send(MemtableFlushThreadMsg::FlushImmutableMemtables(Some(tx)))
            .expect("memtable flush hung up");
        rx.await.expect("receive error on memtable flush")
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
                    match &kv.value {
                        ValueDeletable::Value(value) => {
                            guard.memtable().put(
                                kv.key.clone(),
                                value.clone(),
                                kv.attributes.clone(),
                            );
                        }
                        ValueDeletable::Tombstone => guard
                            .memtable()
                            .delete(kv.key.clone(), kv.attributes.clone()),
                    }
                }
                self.maybe_freeze_memtable(&mut guard, sst_id);
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
    pub async fn open(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, SlateDBError> {
        Self::open_with_opts(path, DbOptions::default(), object_store).await
    }

    pub async fn open_with_opts(
        path: Path,
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

    pub async fn open_with_fp_registry(
        path: Path,
        options: DbOptions,
        object_store: Arc<dyn ObjectStore>,
        fp_registry: Arc<FailPointRegistry>,
    ) -> Result<Self, SlateDBError> {
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
                let part_size_bytes = options.object_store_cache_options.part_size_bytes;
                let cached_object_store = CachedObjectStore::new(
                    object_store.clone(),
                    cache_root_folder.clone(),
                    options.object_store_cache_options.max_cache_size_bytes,
                    part_size_bytes,
                    options.object_store_cache_options.scan_interval,
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
        let latest_manifest = StoredManifest::load(manifest_store.clone()).await?;

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
            .send(WalFlushThreadMsg::Shutdown)
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
            .send(MemtableFlushThreadMsg::Shutdown)
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

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        self.inner.get_with_options(key, DEFAULT_READ_OPTIONS).await
    }

    pub async fn get_with_options(
        &self,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.inner.get_with_options(key, options).await
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) {
        // TODO move the put into an async block by blocking on the memtable flush
        self.inner
            .put_with_options(key, value, DEFAULT_WRITE_OPTIONS)
            .await;
    }

    pub async fn put_with_options(&self, key: &[u8], value: &[u8], options: &WriteOptions) {
        self.inner.put_with_options(key, value, options).await;
    }

    pub async fn delete(&self, key: &[u8]) {
        // TODO move the put into an async block by blocking on the memtable flush
        self.inner
            .delete_with_options(key, DEFAULT_WRITE_OPTIONS)
            .await;
    }

    pub async fn delete_with_options(&self, key: &[u8], options: &WriteOptions) {
        // TODO move the put into an async block by blocking on the memtable flush
        self.inner.delete_with_options(key, options).await;
    }

    /// Write a batch of put/delete operations atomically to the database. Batch writes
    /// block other gets and writes until the batch is written to the WAL (or memtable if
    /// WAL is disabled).
    pub async fn write(&self, batch: WriteBatch) -> Result<(), SlateDBError> {
        self.write_with_options(batch, DEFAULT_WRITE_OPTIONS).await
    }

    /// Write a batch of put/delete operations atomically to the database. Batch writes
    /// block other gets and writes until the batch is written to the WAL (or memtable if
    /// WAL is disabled).
    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        self.inner.write_with_options(batch, options).await
    }

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
    use std::time::Duration;

    use futures::{future::join_all, StreamExt};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use tracing::info;

    use super::*;
    use crate::config::{
        CompactorOptions, ObjectStoreCacheOptions, SizeTieredCompactionSchedulerOptions,
    };
    use crate::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
    use crate::sst_iter::SstIterator;
    #[cfg(feature = "wal_disable")]
    use crate::test_utils::assert_iterator;
    use crate::test_utils::{gen_attrs, TestClock};

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
        kv_store.put(key, value).await;
        kv_store.flush().await.unwrap();

        assert_eq!(
            kv_store.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
        kv_store.delete(key).await;
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
        kv_store.put(key, value).await;
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
        let cached_object_store = CachedObjectStore::new(
            object_store.clone(),
            temp_dir.path().to_path_buf(),
            None,
            part_size,
            None,
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
        kv_store.put(key, value).await;
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
        db.put(&[b'a'; 4], &[b'j'; 4]).await;
        db.put(&[b'b'; 4], &[b'k'; 4]).await;
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
        .await;
        db.put(&[b'a'; 4], &[b'z'; 64]).await;
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
        let mut options = test_db_options(0, 128, None);
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
        let mut stored_manifest = StoredManifest::load(manifest_store.clone())
            .await
            .unwrap()
            .unwrap();
        let write_options = WriteOptions {
            await_durable: false,
        };

        db.put_with_options(&[b'a'; 32], &[b'j'; 32], &write_options)
            .await;
        db.delete_with_options(&[b'b'; 32], &write_options).await;
        let write_options = WriteOptions {
            await_durable: true,
        };
        db.put_with_options(&[b'c'; 32], &[b'l'; 32], &write_options)
            .await;

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
                (vec![b'b'; 32], ValueDeletable::Tombstone, gen_attrs(1)),
                (
                    vec![b'c'; 32],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[b'l'; 32])),
                    gen_attrs(2),
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
        let mut stored_manifest = StoredManifest::load(manifest_store.clone())
            .await
            .unwrap()
            .unwrap();
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
            kv_store.put(&key, &value).await;
            let key = [b'j' + i; 16];
            let value = [b'k' + i; 50];
            kv_store.put(&key, &value).await;
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

    #[tokio::test]
    async fn test_apply_backpressure_to_memtable_flush() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut options = test_db_options(0, 1, None);
        options.l0_max_ssts = 4;
        let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
            .await
            .unwrap();
        db.put(b"key1", b"val1").await;
        db.put(b"key2", b"val2").await;
        db.put(b"key3", b"val3").await;
        db.put(b"key4", b"val4").await;
        db.put(b"key5", b"val5").await;

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
        kv_store.put(key, value).await;
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
                .await;
            kv_store
                .put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48])
                .await;
            next_wal_id += 2;
        }

        // write some smaller keys so that we populate wal without flushing to l0
        let sst_count: u64 = 5;
        for i in 0..sst_count {
            kv_store.put(&i.to_be_bytes(), &i.to_be_bytes()).await;
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
        let stored_manifest = StoredManifest::load(manifest_store).await.unwrap().unwrap();
        let db_state = stored_manifest.db_state();
        assert_eq!(db_state.next_wal_sst_id, next_wal_id);
    }

    #[tokio::test]
    async fn test_should_read_uncommitted_data_if_read_level_uncommitted() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 1024, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();
        kv_store
            .put_with_options(
                "foo".as_bytes(),
                "bar".as_bytes(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await;

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

        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "off").unwrap();
        kv_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_should_read_only_committed_data() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 1024, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        kv_store.put("foo".as_bytes(), "bar".as_bytes()).await;
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();
        kv_store
            .put_with_options(
                "foo".as_bytes(),
                "bla".as_bytes(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await;

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
        let kv_store = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 1024, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        kv_store.put("foo".as_bytes(), "bar".as_bytes()).await;
        fail_parallel::cfg(fp_registry.clone(), "write-wal-sst-io-error", "pause").unwrap();
        kv_store
            .delete_with_options(
                "foo".as_bytes(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await;

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

    #[tokio::test]
    async fn test_should_recover_imm_from_wal() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        fail_parallel::cfg(
            fp_registry.clone(),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();

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
        db.put(&key1, &value1).await;
        next_wal_id += 1;
        let key2 = [b'c'; 32];
        let value2 = [b'd'; 96];
        db.put(&key2, &value2).await;
        next_wal_id += 1;

        db.close().await.unwrap();

        // reload the db
        let db = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        // increment wal id for the empty wal
        next_wal_id += 1;

        // verify that we reload imm
        let snapshot = db.inner.state.read().snapshot();
        assert_eq!(snapshot.state.imm_memtable.len(), 2);

        // one empty wal and two wals for the puts
        assert_eq!(
            snapshot.state.imm_memtable.front().unwrap().last_wal_id(),
            1 + 2
        );
        assert_eq!(snapshot.state.imm_memtable.get(1).unwrap().last_wal_id(), 2);
        assert_eq!(snapshot.state.core.next_wal_sst_id, next_wal_id);
        assert_eq!(
            db.get(&key1).await.unwrap(),
            Some(Bytes::copy_from_slice(&value1))
        );
        assert_eq!(
            db.get(&key2).await.unwrap(),
            Some(Bytes::copy_from_slice(&value2))
        );
    }

    async fn do_test_should_read_compacted_db(options: DbOptions) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let db = Db::open_with_opts(path.clone(), options, object_store.clone())
            .await
            .unwrap();
        let ms = ManifestStore::new(&path, object_store.clone());
        let mut sm = StoredManifest::load(Arc::new(ms)).await.unwrap().unwrap();

        // write enough to fill up a few l0 SSTs
        for i in 0..4 {
            db.put(&[b'a' + i; 32], &[1u8 + i; 32]).await;
            db.put(&[b'm' + i; 32], &[13u8 + i; 32]).await;
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
            db.put(&[b'f' + i; 32], &[6u8 + i; 32]).await;
            db.put(&[b's' + i; 32], &[19u8 + i; 32]).await;
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
        db.put(&[b'a'; 32], &[128u8; 32]).await;
        db.put(&[b'm'; 32], &[129u8; 32]).await;

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
        db.put(b"1", b"1").await;
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

        // open db1 and assert that it can write.
        let db1 = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        db1.put_with_options(
            b"1",
            b"1",
            &WriteOptions {
                await_durable: false,
            },
        )
        .await;
        db1.flush().await.unwrap();
        // open db2, causing it to write an empty wal and fence db1.
        let db2 = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();
        // assert that db1 can no longer write.
        db1.put_with_options(
            b"1",
            b"1",
            &WriteOptions {
                await_durable: false,
            },
        )
        .await;
        assert!(matches!(db1.flush().await, Err(SlateDBError::Fenced)));
        db2.put_with_options(
            b"2",
            b"2",
            &WriteOptions {
                await_durable: false,
            },
        )
        .await;
        db2.flush().await.unwrap();
        assert_eq!(db2.inner.state.read().state().core.next_wal_sst_id, 5);
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
        DbOptions {
            flush_interval: Duration::from_millis(100),
            #[cfg(feature = "wal_disable")]
            wal_enabled: true,
            manifest_poll_interval: Duration::from_millis(100),
            max_unflushed_memtable: 2,
            l0_max_ssts: 8,
            min_filter_keys,
            filter_bits_per_key: 10,
            l0_sst_size_bytes,
            compactor_options,
            compression_codec: None,
            object_store_cache_options: ObjectStoreCacheOptions::default(),
            block_cache: None,
            garbage_collector_options: None,
            clock: Arc::new(TestClock::new()),
        }
    }
}
