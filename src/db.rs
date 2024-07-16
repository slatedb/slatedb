use crate::db::ReadLevel::{Commited, Uncommitted};
use crate::db_state::DbState;
use crate::flatbuffer_types::ManifestV1Owned;
use crate::mem_table_flush::MemtableFlushThreadMsg;
use crate::mem_table_flush::MemtableFlushThreadMsg::Shutdown;
use crate::sst::SsTableFormat;
use crate::sst_iter::SstIterator;
use crate::tablestore::{SSTableHandle, SsTableId, TableStore};
use crate::types::ValueDeletable;
use crate::{block::Block, error::SlateDBError};
use crate::{block_iterator::BlockIterator, iter::KeyValueIterator};
use bytes::Bytes;
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use tokio::runtime::Handle;

pub enum ReadLevel {
    Commited,
    Uncommitted,
}

pub struct ReadOptions {
    pub read_level: ReadLevel,
}

impl ReadOptions {
    const fn default() -> Self {
        Self {
            read_level: Commited,
        }
    }
}

const DEFAULT_READ_OPTIONS: &ReadOptions = &ReadOptions::default();

pub struct WriteOptions {
    pub await_flush: bool,
}

impl WriteOptions {
    const fn default() -> Self {
        Self { await_flush: true }
    }
}

const DEFAULT_WRITE_OPTIONS: &WriteOptions = &WriteOptions::default();

pub struct DbOptions {
    pub flush_ms: usize,
    pub min_filter_keys: u32,
    pub l0_sst_size_bytes: usize,
}

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<DbState>>,
    pub(crate) options: DbOptions,
    pub(crate) table_store: TableStore,
    pub(crate) manifest: Arc<RwLock<ManifestV1Owned>>,
    pub(crate) memtable_flush_notifier: tokio::sync::mpsc::UnboundedSender<MemtableFlushThreadMsg>,
}

impl DbInner {
    pub async fn new(
        options: DbOptions,
        table_store: TableStore,
        manifest: ManifestV1Owned,
        memtable_flush_notifier: tokio::sync::mpsc::UnboundedSender<MemtableFlushThreadMsg>,
    ) -> Result<Self, SlateDBError> {
        let state = DbState::load(&manifest, &table_store).await?;
        let db_inner = Self {
            state: Arc::new(RwLock::new(state)),
            options,
            table_store,
            manifest: Arc::new(RwLock::new(manifest)),
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
            if let Some(block_index) = self.find_block_for_key(sst, key).await? {
                let block = self.table_store.read_block(sst, block_index).await?;
                if let Some(val) = self.find_val_in_block(&block, key).await? {
                    return Ok(val.into_option());
                }
            }
        }
        Ok(None)
    }

    async fn find_block_for_key(
        &self,
        sst: &SSTableHandle,
        key: &[u8],
    ) -> Result<Option<usize>, SlateDBError> {
        if let Some(filter) = self.table_store.read_filter(sst).await? {
            if !filter.has_key(key) {
                return Ok(None);
            }
        }

        let handle = sst.info.borrow();
        // search for the block that could contain the key.
        let mut low = 0;
        let mut high = handle.block_meta().len() - 1;
        let mut found_block_id: Option<usize> = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            let current_block_first_key = handle.block_meta().get(mid).first_key().bytes();
            match current_block_first_key.cmp(key) {
                std::cmp::Ordering::Less => {
                    low = mid + 1;
                    found_block_id = Some(mid);
                }
                std::cmp::Ordering::Greater => {
                    if mid > 0 {
                        high = mid - 1;
                    } else {
                        break;
                    }
                }
                std::cmp::Ordering::Equal => return Ok(Some(mid)),
            }
        }

        Ok(found_block_id)
    }

    async fn find_val_in_block(
        &self,
        block: &Block,
        key: &[u8],
    ) -> Result<Option<ValueDeletable>, SlateDBError> {
        let mut iter = BlockIterator::from_first_key(block);
        while let Some(current_key_value) = iter.next_entry().await? {
            if current_key_value.key == key {
                return Ok(Some(current_key_value.value));
            }
        }
        Ok(None)
    }

    /// Put a key-value pair into the database. Key and value must not be empty.
    pub async fn put_with_options(&self, key: &[u8], value: &[u8], options: &WriteOptions) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_wal_table = {
            let mut guard = self.state.write();
            let current_wal = guard.wal();
            current_wal.put(key, value);
            current_wal.table().clone()
        };

        if options.await_flush {
            current_wal_table.await_flush().await;
        }
    }

    /// Delete a key from the database. Key must not be empty.
    pub async fn delete_with_options(&self, key: &[u8], options: &WriteOptions) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_wal_table = {
            let mut guard = self.state.write();
            let current_wal = guard.wal();
            current_wal.delete(key);
            current_wal.table().clone()
        };

        if options.await_flush {
            current_wal_table.await_flush().await;
        }
    }

    async fn replay_wal(&self) -> Result<(), SlateDBError> {
        let wal_id_last_compacted = self.state.read().state().core.last_compacted_wal_sst_id;
        let wal_sst_list = self
            .table_store
            .get_wal_sst_list(wal_id_last_compacted)
            .await?;
        for sst_id in wal_sst_list {
            let sst = self.table_store.open_sst(&SsTableId::Wal(sst_id)).await?;
            let sst_id = match &sst.id {
                SsTableId::Wal(id) => *id,
                SsTableId::Compacted(_) => return Err(SlateDBError::InvalidDBState),
            };
            let mut iter = SstIterator::new(sst, &self.table_store);
            // iterate over the WAL SSTs in reverse order to ensure we recover in write-order
            while let Some(kv) = iter.next_entry().await? {
                // TODO: it's not ideal that we have to take this lock for every kv. We can solve
                //       this by either:
                //       1. detaching this method from self and calling it before initializing
                //          DbInner. The downside is we can't use member methods of DbInner
                //          like maybe_freeze_wal
                //       2. accumulating kv-pairs in memory and bulk-applying the writes
                let mut guard = self.state.write();
                match kv.value {
                    ValueDeletable::Value(value) => {
                        guard.memtable().put(kv.key.as_ref(), value.as_ref())
                    }
                    ValueDeletable::Tombstone => guard.memtable().delete(kv.key.as_ref()),
                }
            }
            {
                let mut guard = self.state.write();
                self.maybe_freeze_memtable(&mut guard, sst_id);
                assert_eq!(guard.state().core.next_wal_sst_id, sst_id);
                guard.increment_next_wal_id();
            }
        }
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
}

impl Db {
    pub async fn open(
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
        let sst_format = SsTableFormat::new(4096, options.min_filter_keys);
        let table_store = TableStore::new_with_fp_registry(
            object_store,
            sst_format,
            path.clone(),
            fp_registry.clone(),
        );
        let manifest = table_store.open_latest_manifest().await?;
        let manifest = match manifest {
            Some(manifest) => manifest,
            None => {
                let manifest = ManifestV1Owned::create_new();
                table_store.write_manifest(&manifest).await?;
                manifest
            }
        };

        let (memtable_flush_tx, memtable_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner =
            Arc::new(DbInner::new(options, table_store, manifest, memtable_flush_tx).await?);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tokio_handle = Handle::current();
        let flush_thread = inner.spawn_flush_task(rx, &tokio_handle);
        let memtable_flush_task = inner.spawn_memtable_flush_task(memtable_flush_rx, &tokio_handle);
        Ok(Self {
            inner,
            flush_notifier: tx,
            flush_task: Mutex::new(flush_thread),
            memtable_flush_task: Mutex::new(memtable_flush_task),
        })
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        // Tell the notifier thread to shut down.
        self.flush_notifier.send(()).ok();
        self.inner.memtable_flush_notifier.send(Shutdown).ok();

        if let Some(flush_task) = {
            // Scope the flush_thread lock so its lock isn't held while awaiting the join
            // and the final flush below.
            // Wait for the flush thread to finish.
            let mut flush_task = self.flush_task.lock();
            flush_task.take()
        } {
            flush_task.await.expect("Failed to join flush thread");
        }

        if let Some(memtable_flush_task) = {
            let mut memtable_flush_task = self.memtable_flush_task.lock();
            memtable_flush_task.take()
        } {
            memtable_flush_task
                .await
                .expect("Failed to join memtable flush thread");
        }

        // Force a final flush on the mutable memtable
        self.inner.flush().await?;
        self.inner.write_manifest().await?;
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
    use crate::sst_iter::SstIterator;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::time::{Duration, SystemTime};
    use tokio::time::sleep;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_put_get_delete() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open(
            Path::from("/tmp/test_kv_store"),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 1024,
            },
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
    async fn test_put_flushes_memtable() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open(
            path.clone(),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 128,
            },
            object_store.clone(),
        )
        .await
        .unwrap();

        let sst_format = SsTableFormat::new(4096, 10);
        let table_store = TableStore::new(object_store.clone(), sst_format, path.clone());

        // Write data a few times such that each loop results in a memtable flush
        let mut last_compacted = 0;
        for i in 0..3 {
            let key = [b'a' + i; 16];
            let value = [b'b' + i; 50];
            kv_store.put(&key, &value).await;
            let key = [b'j' + i; 16];
            let value = [b'k' + i; 50];
            kv_store.put(&key, &value).await;
            let now = SystemTime::now();
            while now.elapsed().unwrap().as_secs() < 30 {
                let manifest_owned = table_store.open_latest_manifest().await.unwrap().unwrap();
                let manifest = manifest_owned.borrow();
                if manifest.wal_id_last_compacted() > last_compacted {
                    assert_eq!(manifest.wal_id_last_compacted(), (i as u64) * 2 + 2);
                    last_compacted = manifest.wal_id_last_compacted();
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        }

        let manifest_owned = table_store.open_latest_manifest().await.unwrap().unwrap();
        let manifest = manifest_owned.borrow();
        let l0 = manifest.l0().unwrap();
        assert_eq!(l0.len(), 3);
        for i in 0u8..3u8 {
            let compacted_sst1 = l0.get(2 - i as usize);
            let ulid = Ulid::from((
                compacted_sst1.id().unwrap().high(),
                compacted_sst1.id().unwrap().low(),
            ));
            let sst1 = table_store
                .open_sst(&SsTableId::Compacted(ulid))
                .await
                .unwrap();
            let mut iter = SstIterator::new(sst1, &table_store);
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
        let kv_store = Db::open(
            Path::from("/tmp/test_kv_store"),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 1024,
            },
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
        let kv_store = Db::open(
            Path::from("/tmp/test_kv_store"),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 1024,
            },
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
        let kv_store = Db::open(
            path.clone(),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 128,
            },
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
        let kv_store_restored = Db::open(
            path.clone(),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 128,
            },
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
        let sst_format = SsTableFormat::new(4096, 10);
        let table_store = TableStore::new(object_store.clone(), sst_format, path);
        let manifest_owned = table_store.open_latest_manifest().await.unwrap().unwrap();
        let manifest = manifest_owned.borrow();
        assert_eq!(manifest.wal_id_last_seen(), sst_count + 2 * l0_count);
    }

    #[tokio::test]
    async fn test_should_read_uncommitted_data_if_read_level_uncommitted() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let kv_store = Db::open(
            path.clone(),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 1024,
            },
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
        let kv_store = Db::open(
            path.clone(),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 1024,
            },
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
        let kv_store = Db::open(
            path.clone(),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 1024,
            },
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
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 128,
            },
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
        let db = Db::open(
            path.clone(),
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
                l0_sst_size_bytes: 128,
            },
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
}
