use std::sync::Arc;

use crate::compactor::Compactor;
use crate::config::ReadLevel::Uncommitted;
use crate::config::{
    DbOptions, ReadOptions, WriteOptions, DEFAULT_READ_OPTIONS, DEFAULT_WRITE_OPTIONS,
};
use crate::db_state::{CoreDbState, DbState, SSTableHandle, SortedRun, SsTableId};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::manifest_store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::mem_table_flush::MemtableFlushThreadMsg;
use crate::mem_table_flush::MemtableFlushThreadMsg::Shutdown;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst::SsTableFormat;
use crate::sst_iter::SstIterator;
use crate::tablestore::TableStore;
use crate::types::ValueDeletable;
use bytes::Bytes;
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::Handle;

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<DbState>>,
    pub(crate) options: DbOptions,
    pub(crate) table_store: Arc<TableStore>,
    pub(crate) memtable_flush_notifier: tokio::sync::mpsc::UnboundedSender<MemtableFlushThreadMsg>,
}

impl DbInner {
    pub async fn new(
        options: DbOptions,
        table_store: Arc<TableStore>,
        core_db_state: CoreDbState,
        memtable_flush_notifier: tokio::sync::mpsc::UnboundedSender<MemtableFlushThreadMsg>,
    ) -> Result<Self, SlateDBError> {
        let state = DbState::new(core_db_state);
        let db_inner = Self {
            state: Arc::new(RwLock::new(state)),
            options,
            table_store,
            memtable_flush_notifier,
        };
        db_inner.replay_wal().await?;
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
            let maybe_bytes = std::iter::once(snapshot.wal)
                .chain(snapshot.state.imm_wal.iter().map(|imm| imm.table()))
                .find_map(|memtable| memtable.get(key));
            if let Some(val) = maybe_bytes {
                return Ok(val.into_option());
            }
        }

        let maybe_bytes = std::iter::once(snapshot.memtable)
            .chain(snapshot.state.imm_memtable.iter().map(|imm| imm.table()))
            .find_map(|memtable| memtable.get(key));
        if let Some(val) = maybe_bytes {
            return Ok(val.into_option());
        }

        for sst in &snapshot.state.core.l0 {
            if self.sst_may_include_key(sst, key).await? {
                let mut iter =
                    SstIterator::new_from_key(sst, self.table_store.clone(), key, 1, 1).await?;
                if let Some(entry) = iter.next_entry().await? {
                    if entry.key == key {
                        return Ok(entry.value.into_option());
                    }
                }
            }
        }
        for sr in &snapshot.state.core.compacted {
            if self.sr_may_include_key(sr, key).await? {
                let mut iter =
                    SortedRunIterator::new_from_key(sr, key, self.table_store.clone(), 1, 1)
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

    async fn sst_may_include_key(
        &self,
        sst: &SSTableHandle,
        key: &[u8],
    ) -> Result<bool, SlateDBError> {
        if !sst.range_covers_key(key) {
            return Ok(false);
        }
        if let Some(filter) = self.table_store.read_filter(sst).await? {
            return Ok(filter.has_key(key));
        }
        Ok(true)
    }

    async fn sr_may_include_key(&self, sr: &SortedRun, key: &[u8]) -> Result<bool, SlateDBError> {
        if let Some(sst) = sr.find_sst_with_range_covering_key(key) {
            if let Some(filter) = self.table_store.read_filter(sst).await? {
                return Ok(filter.has_key(key));
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn wal_enabled(&self) -> bool {
        #[cfg(feature = "wal_disable")]
        return self.options.wal_enabled;
        #[cfg(not(feature = "wal_disable"))]
        return true;
    }

    /// Put a key-value pair into the database. Key must not be empty.
    #[allow(clippy::panic)]
    pub async fn put_with_options(&self, key: &[u8], value: &[u8], options: &WriteOptions) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_table = if self.wal_enabled() {
            let mut guard = self.state.write();
            let current_wal = guard.wal();
            current_wal.put(key, value);
            current_wal.table().clone()
        } else {
            if cfg!(not(feature = "wal_disable")) {
                panic!("wal_disabled feature must be enabled");
            }
            let mut guard = self.state.write();
            let current_memtable = guard.memtable();
            current_memtable.put(key, value);
            let table = current_memtable.table().clone();
            let last_compacted = guard.state().core.last_compacted_wal_sst_id;
            self.maybe_freeze_memtable(&mut guard, last_compacted);
            table
        };

        if options.await_flush {
            current_table.await_flush().await;
        }
    }

    /// Delete a key from the database. Key must not be empty.
    #[allow(clippy::panic)]
    pub async fn delete_with_options(&self, key: &[u8], options: &WriteOptions) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_table = if self.wal_enabled() {
            let mut guard = self.state.write();
            let current_wal = guard.wal();
            current_wal.delete(key);
            current_wal.table().clone()
        } else {
            if cfg!(not(feature = "wal_disable")) {
                panic!("wal_disabled feature must be enabled");
            }
            let mut guard = self.state.write();
            let current_memtable = guard.memtable();
            current_memtable.delete(key);
            let table = current_memtable.table().clone();
            let last_compacted = guard.state().core.last_compacted_wal_sst_id;
            self.maybe_freeze_memtable(&mut guard, last_compacted);
            table
        };

        if options.await_flush {
            current_table.await_flush().await;
        }
    }

    async fn replay_wal(&self) -> Result<(), SlateDBError> {
        let wal_id_last_compacted = self.state.read().state().core.last_compacted_wal_sst_id;
        let wal_sst_list = self
            .table_store
            .get_wal_sst_list(wal_id_last_compacted)
            .await?;
        let mut last_sst_id = wal_id_last_compacted;
        for sst_id in wal_sst_list {
            last_sst_id = sst_id;
            let sst = self.table_store.open_sst(&SsTableId::Wal(sst_id)).await?;
            let sst_id = match &sst.id {
                SsTableId::Wal(id) => *id,
                SsTableId::Compacted(_) => return Err(SlateDBError::InvalidDBState),
            };
            let mut iter = SstIterator::new(&sst, self.table_store.clone(), 1, 1).await?;
            // iterate over the WAL SSTs in reverse order to ensure we recover in write-order
            // buffer the WAL entries to bulk replay them into the memtable.
            let mut wal_replay_buf = Vec::new();
            while let Some(kv) = iter.next_entry().await? {
                wal_replay_buf.push(kv);
            }
            {
                let mut guard = self.state.write();
                for kv in wal_replay_buf.iter() {
                    match &kv.value {
                        ValueDeletable::Value(value) => {
                            guard.memtable().put(kv.key.as_ref(), value.as_ref())
                        }
                        ValueDeletable::Tombstone => guard.memtable().delete(kv.key.as_ref()),
                    }
                }
                self.maybe_freeze_memtable(&mut guard, sst_id);
                if guard.state().core.next_wal_sst_id == sst_id {
                    guard.increment_next_wal_id();
                }
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
    /// Notifies the L0 flush thread to stop working.
    flush_notifier: tokio::sync::mpsc::UnboundedSender<()>,
    /// The handle for the flush thread.
    flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    memtable_flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    compactor: Mutex<Option<Compactor>>,
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
        let sst_format =
            SsTableFormat::new(4096, options.min_filter_keys, options.compression_codec);
        let table_store = Arc::new(TableStore::new_with_fp_registry(
            object_store.clone(),
            sst_format,
            path.clone(),
            fp_registry.clone(),
        ));
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let manifest = Self::init_db(&manifest_store).await?;
        let (memtable_flush_tx, memtable_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(
            DbInner::new(
                options.clone(),
                table_store.clone(),
                manifest.db_state()?.clone(),
                memtable_flush_tx,
            )
            .await?,
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tokio_handle = Handle::current();
        let flush_thread = if inner.wal_enabled() {
            inner.spawn_flush_task(rx, &tokio_handle)
        } else {
            None
        };
        let memtable_flush_task =
            inner.spawn_memtable_flush_task(manifest, memtable_flush_rx, &tokio_handle);
        let mut compactor = None;
        if let Some(compactor_options) = &inner.options.compactor_options {
            compactor = Some(
                Compactor::new(
                    manifest_store.clone(),
                    table_store.clone(),
                    compactor_options.clone(),
                    Handle::current(),
                )
                .await?,
            )
        }
        Ok(Self {
            inner,
            flush_notifier: tx,
            flush_task: Mutex::new(flush_thread),
            memtable_flush_task: Mutex::new(memtable_flush_task),
            compactor: Mutex::new(compactor),
        })
    }

    async fn init_db(
        manifest_store: &Arc<ManifestStore>,
    ) -> Result<FenceableManifest, SlateDBError> {
        let stored_manifest = Self::init_stored_manifest(manifest_store).await?;
        FenceableManifest::init_writer(stored_manifest).await
    }

    async fn init_stored_manifest(
        manifest_store: &Arc<ManifestStore>,
    ) -> Result<StoredManifest, SlateDBError> {
        if let Some(stored_manifest) = StoredManifest::load(manifest_store.clone()).await? {
            return Ok(stored_manifest);
        }
        StoredManifest::init_new_db(manifest_store.clone(), CoreDbState::new()).await
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        if let Some(compactor) = {
            let mut maybe_compactor = self.compactor.lock();
            maybe_compactor.take()
        } {
            compactor.close().await;
        }

        // Tell the notifier thread to shut down.
        self.flush_notifier.send(()).ok();

        if let Some(flush_task) = {
            // Scope the flush_thread lock so its lock isn't held while awaiting the join
            // and the final flush below.
            // Wait for the flush thread to finish.
            let mut flush_task = self.flush_task.lock();
            flush_task.take()
        } {
            flush_task.await.expect("Failed to join flush thread");
        }

        // Tell the memtable flush thread to shut down.
        self.inner.memtable_flush_notifier.send(Shutdown).ok();

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

    pub async fn flush(&self) -> Result<(), SlateDBError> {
        self.inner.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompactorOptions;
    use crate::sst_iter::SstIterator;
    #[cfg(feature = "wal_disable")]
    use crate::test_utils::assert_iterator;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::time::Duration;
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

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn test_wal_disabled() {
        let mut options = test_db_options(0, 128, None);
        options.wal_enabled = false;
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let sst_format = SsTableFormat::new(4096, 10, None);
        let table_store = Arc::new(TableStore::new(
            object_store.clone(),
            sst_format,
            path.clone(),
        ));
        let db = Db::open_with_opts(path.clone(), options, object_store.clone())
            .await
            .unwrap();
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut stored_manifest = StoredManifest::load(manifest_store.clone())
            .await
            .unwrap()
            .unwrap();
        let write_options = WriteOptions { await_flush: false };

        db.put_with_options(&[b'a'; 32], &[b'j'; 32], &write_options)
            .await;
        db.delete_with_options(&[b'b'; 32], &write_options).await;
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
        let mut iter = SstIterator::new(l0, table_store.clone(), 1, 1)
            .await
            .unwrap();
        assert_iterator(
            &mut iter,
            &[
                (
                    &[b'a'; 32],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[b'j'; 32])),
                ),
                (&[b'b'; 32], ValueDeletable::Tombstone),
                (
                    &[b'c'; 32],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[b'l'; 32])),
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
        let sst_format = SsTableFormat::new(4096, 10, None);
        let table_store = Arc::new(TableStore::new(
            object_store.clone(),
            sst_format,
            path.clone(),
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
            assert_eq!(db_state.last_compacted_wal_sst_id, (i as u64) * 2 + 2);
            last_compacted = db_state.last_compacted_wal_sst_id
        }

        let db_state = stored_manifest.refresh().await.unwrap();
        let l0 = &db_state.l0;
        assert_eq!(l0.len(), 3);
        for i in 0u8..3u8 {
            let sst1 = l0.get(2 - i as usize).unwrap();
            let mut iter = SstIterator::new(sst1, table_store.clone(), 1, 1)
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
            lock.wal().put(b"abc1111", b"value1111");
            lock.wal().put(b"abc2222", b"value2222");
            lock.wal().put(b"abc3333", b"value3333");
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
        let kv_store = Db::open_with_opts(
            path.clone(),
            test_db_options(0, 128, None),
            object_store.clone(),
        )
        .await
        .unwrap();

        // do a few writes that will result in l0 flushes
        let l0_count: u64 = 3;
        for i in 0..l0_count {
            kv_store
                .put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48])
                .await;
            kv_store
                .put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48])
                .await;
        }

        // write some smaller keys so that we populate wal without flushing to l0
        let sst_count: u64 = 5;
        for i in 0..sst_count {
            kv_store.put(&i.to_be_bytes(), &i.to_be_bytes()).await;
            kv_store.flush().await.unwrap();
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

        // validate that the manifest file exists.
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let stored_manifest = StoredManifest::load(manifest_store).await.unwrap().unwrap();
        let db_state = stored_manifest.db_state();
        assert_eq!(db_state.next_wal_sst_id, sst_count + 2 * l0_count + 1);
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
                &WriteOptions { await_flush: false },
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
                &WriteOptions { await_flush: false },
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
            .delete_with_options("foo".as_bytes(), &WriteOptions { await_flush: false })
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
        db.put(&key1, &value1).await;
        let key2 = [b'c'; 32];
        let value2 = [b'd'; 96];
        db.put(&key2, &value2).await;

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
        assert_eq!(snapshot.state.imm_memtable.len(), 2);
        assert_eq!(
            snapshot.state.imm_memtable.front().unwrap().last_wal_id(),
            2
        );
        assert_eq!(snapshot.state.imm_memtable.get(1).unwrap().last_wal_id(), 1);
        assert_eq!(snapshot.state.core.next_wal_sst_id, 3);
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
        let neg_lookup = db.get(&[b'a', b'b', b'c']).await;
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
            }),
        ))
        .await
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
            min_filter_keys,
            l0_sst_size_bytes,
            compactor_options,
            compression_codec: None,
        }
    }
}
