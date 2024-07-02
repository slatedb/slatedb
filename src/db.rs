use crate::flatbuffer_types::ManifestV1Owned;
use crate::mem_table::{ImmutableMemtable, ImmutableWal, WritableKVTable};
use crate::memtable_flush::MemtableFlushThreadMsg;
use crate::memtable_flush::MemtableFlushThreadMsg::{FlushImmutableMemtables, Shutdown};
use crate::sst::SsTableFormat;
use crate::tablestore::{SSTableHandle, SsTableId, TableStore};
use crate::types::ValueDeletable;
use crate::{block::Block, error::SlateDBError};
use crate::{block_iterator::BlockIterator, iter::KeyValueIterator};
use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use std::{collections::VecDeque, sync::Arc};

pub struct DbOptions {
    pub flush_ms: usize,
    pub min_filter_keys: u32,
    pub l0_sst_size_bytes: usize,
}

pub(crate) struct DbState {
    pub(crate) memtable: WritableKVTable,
    pub(crate) wal: WritableKVTable,
    pub(crate) compacted: Arc<CompactedDbState>,
}

#[derive(Clone)]
pub(crate) struct CompactedDbState {
    pub(crate) imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    pub(crate) imm_wal: VecDeque<Arc<ImmutableWal>>,
    // todo: get rid of me when we read using l0 and imm_memtable
    pub(crate) wal_ssts: VecDeque<SSTableHandle>,
    pub(crate) l0: VecDeque<SSTableHandle>,
    pub(crate) next_wal_sst_id: u64,
    pub(crate) last_compacted_wal_sst_id: u64,
}

impl DbState {
    fn create() -> Self {
        Self {
            memtable: WritableKVTable::new(),
            wal: WritableKVTable::new(),
            compacted: Arc::new(CompactedDbState {
                imm_memtable: VecDeque::new(),
                imm_wal: VecDeque::new(),
                wal_ssts: VecDeque::new(),
                l0: VecDeque::new(),
                next_wal_sst_id: 1,
                last_compacted_wal_sst_id: 0,
            }),
        }
    }
}

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<DbState>>,
    pub(crate) options: DbOptions,
    pub(crate) table_store: TableStore,
    pub(crate) manifest: Arc<RwLock<ManifestV1Owned>>,
    pub(crate) memtable_flush_notifier: crossbeam_channel::Sender<MemtableFlushThreadMsg>,
}

impl DbInner {
    pub async fn new(
        options: DbOptions,
        table_store: TableStore,
        manifest: ManifestV1Owned,
        memtable_flush_notifier: crossbeam_channel::Sender<MemtableFlushThreadMsg>,
    ) -> Result<Self, SlateDBError> {
        let mut db_inner = Self {
            state: Arc::new(RwLock::new(DbState::create())),
            options,
            table_store,
            manifest: Arc::new(RwLock::new(manifest)),
            memtable_flush_notifier,
        };

        db_inner.load_state().await?;

        Ok(db_inner)
    }

    /// Get the value for a given key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        let (memtable, compacted) = {
            let guard = self.state.read();
            (guard.wal.table().clone(), Arc::clone(&guard.compacted))
        };

        let maybe_bytes = std::iter::once(memtable)
            .chain(compacted.imm_wal.iter().map(|imm| imm.table()))
            .find_map(|memtable| memtable.get(key));

        if let Some(val) = maybe_bytes {
            return Ok(val.into_option());
        }

        for sst in &compacted.wal_ssts {
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

    fn maybe_freeze_memtable(&self, guard: &mut RwLockWriteGuard<DbState>) {
        if guard.memtable.size() < self.options.l0_sst_size_bytes {
            return;
        }
        // If there was no active wal with data, that means the flush thread
        // will have just frozen it. In that case, check the imm wal list.
        // If there are no imm wal entries, all wal entries are flushed, so use
        // the last flushed wal's id
        let wal_id = match self.freeze_wal(guard) {
            Some(id) => id,
            None => match guard.compacted.imm_wal.front() {
                Some(imm_wal) => imm_wal.id(),
                None => guard.compacted.next_wal_sst_id - 1,
            },
        };
        self.freeze_memtable(guard, wal_id);
        self.memtable_flush_notifier
            .send(FlushImmutableMemtables)
            .expect("failed to send memtable flush msg");
    }

    pub(crate) fn freeze_wal(&self, guard: &mut RwLockWriteGuard<DbState>) -> Option<u64> {
        if guard.wal.table().is_empty() {
            return None;
        }
        let old_wal = std::mem::replace(&mut guard.wal, WritableKVTable::new());
        let mut compacted_snapshot = guard.compacted.as_ref().clone();
        let imm_wal = Arc::new(ImmutableWal::new(
            compacted_snapshot.next_wal_sst_id,
            old_wal,
        ));
        let id = imm_wal.id();
        compacted_snapshot.imm_wal.push_front(imm_wal);
        compacted_snapshot.next_wal_sst_id += 1;
        guard.compacted = Arc::new(compacted_snapshot);
        Some(id)
    }

    fn freeze_memtable(&self, guard: &mut RwLockWriteGuard<DbState>, wal_id: u64) {
        let old_memtable = std::mem::replace(&mut guard.memtable, WritableKVTable::new());
        let mut compacted_snapshot = guard.compacted.as_ref().clone();
        compacted_snapshot
            .imm_memtable
            .push_front(Arc::new(ImmutableMemtable::new(old_memtable, wal_id)));
        guard.compacted = Arc::new(compacted_snapshot);
    }

    /// Put a key-value pair into the database. Key and value must not be empty.
    pub async fn put(&self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_wal_table = {
            let mut guard = self.state.write();
            let current_memtable = &mut guard.memtable;
            current_memtable.put(key, value);
            let current_wal = &mut guard.wal;
            current_wal.put(key, value);
            let current_wal_table = current_wal.table().clone();
            self.maybe_freeze_memtable(&mut guard);
            current_wal_table
        };

        current_wal_table.await_flush().await;
    }

    /// Delete a key from the database. Key must not be empty.
    pub async fn delete(&self, key: &[u8]) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let current_wal_table = {
            let mut guard = self.state.write();
            let current_memtable = &mut guard.memtable;
            current_memtable.delete(key);
            let current_wal = &mut guard.wal;
            current_wal.delete(key);
            let current_wal_table = current_wal.table().clone();
            self.maybe_freeze_memtable(&mut guard);
            current_wal_table
        };

        current_wal_table.await_flush().await;
    }

    async fn load_state(&mut self) -> Result<(), SlateDBError> {
        let wal_id_last_compacted = self.manifest.read().borrow().wal_id_last_compacted();
        let wal_sst_list = self
            .table_store
            .get_wal_sst_list(wal_id_last_compacted)
            .await?;

        let mut snapshot = {
            let rguard_state = self.state.read();
            rguard_state.compacted.as_ref().clone()
        };

        snapshot.last_compacted_wal_sst_id = wal_id_last_compacted;
        for sst_id in wal_sst_list {
            let sst = self.table_store.open_sst(&SsTableId::Wal(sst_id)).await?;

            // always put the new sst at the front of l0
            snapshot.wal_ssts.push_front(sst);
            snapshot.next_wal_sst_id = sst_id + 1;
        }

        let mut wguard_state = self.state.write();
        wguard_state.compacted = Arc::new(snapshot);

        Ok(())
    }
}

pub struct Db {
    inner: Arc<DbInner>,
    /// Notifies the L0 flush thread to stop working.
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread.
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    memtable_flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Db {
    pub async fn open(
        path: Path,
        options: DbOptions,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, SlateDBError> {
        let sst_format = SsTableFormat::new(4096, options.min_filter_keys);
        let table_store = TableStore::new(object_store, sst_format, path.clone());
        let manifest = table_store.open_latest_manifest().await?;
        let manifest = match manifest {
            Some(manifest) => manifest,
            None => {
                let manifest = ManifestV1Owned::create_new();
                table_store.write_manifest(&manifest).await?;
                manifest
            }
        };

        let (memtable_flush_tx, memtable_flush_rx) = crossbeam_channel::unbounded();
        let inner =
            Arc::new(DbInner::new(options, table_store, manifest, memtable_flush_tx).await?);
        let (tx, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx);
        let memtable_flush_thread = inner.spawn_memtable_flush_thread(memtable_flush_rx);
        Ok(Self {
            inner,
            flush_notifier: tx,
            flush_thread: Mutex::new(flush_thread),
            memtable_flush_thread: Mutex::new(memtable_flush_thread),
        })
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        // Tell the notifier thread to shut down.
        self.flush_notifier.send(()).ok();
        self.inner.memtable_flush_notifier.send(Shutdown).ok();

        // Scope the flush_thread lock so its lock isn't held while awaiting the final flush below.
        {
            // Wait for the flush thread to finish.
            let mut flush_thread = self.flush_thread.lock();
            if let Some(flush_thread) = flush_thread.take() {
                flush_thread.join().expect("Failed to join flush thread");
            }
        }

        {
            let mut memtable_flush_thread = self.memtable_flush_thread.lock();
            if let Some(memtable_flush_thread) = memtable_flush_thread.take() {
                memtable_flush_thread
                    .join()
                    .expect("Failed to join memtable flush thread");
            }
        }

        // Force a final flush on the mutable memtable
        self.inner.flush().await?;
        self.inner.write_manifest().await?;
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        self.inner.get(key).await
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) {
        // TODO move the put into an async block by blocking on the memtable flush
        self.inner.put(key, value).await;
    }

    pub async fn delete(&self, key: &[u8]) {
        // TODO move the put into an async block by blocking on the memtable flush
        self.inner.delete(key).await;
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
            lock.wal.put(b"abc1111", b"value1111");
            lock.wal.put(b"abc2222", b"value2222");
            lock.wal.put(b"abc3333", b"value3333");
            lock.wal.table().clone()
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
                l0_sst_size_bytes: 1024,
            },
            object_store.clone(),
        )
        .await
        .unwrap();

        // write some sst files
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
                l0_sst_size_bytes: 1024,
            },
            object_store.clone(),
        )
        .await
        .unwrap();

        for i in 0..sst_count {
            assert!(kv_store_restored
                .get(&i.to_be_bytes())
                .await
                .unwrap()
                .is_some());
        }

        // validate that the manifest file exists.
        let sst_format = SsTableFormat::new(4096, 10);
        let table_store = TableStore::new(object_store.clone(), sst_format, path);
        let manifest_owned = table_store.open_latest_manifest().await.unwrap().unwrap();
        let manifest = manifest_owned.borrow();
        assert_eq!(manifest.wal_id_last_seen(), sst_count);
    }
}
