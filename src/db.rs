use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{block::Block, error::SlateDBError, iter::KVEntry};
use crate::{block_iterator::BlockIterator, iter::KeyValueIterator};
use bytes::Bytes;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};

use crate::mem_table::MemTable;
use crate::sst::SsTableFormat;
use crate::tablestore::{SSTableHandle, TableStore};

pub struct DbOptions {
    pub flush_ms: usize,
    pub min_filter_keys: u32,
}

#[derive(Clone)]
pub(crate) struct DbState {
    pub(crate) memtable: Arc<MemTable>,
    pub(crate) imm_memtables: VecDeque<Arc<MemTable>>,
    pub(crate) l0: VecDeque<SSTableHandle>,
    pub(crate) next_sst_id: usize,
}

impl DbState {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::new()),
            imm_memtables: VecDeque::new(),
            l0: VecDeque::new(),
            next_sst_id: 0,
        }
    }
}

pub(crate) struct DbInner {
    pub(crate) state: Arc<RwLock<Arc<DbState>>>,
    #[allow(dead_code)] // TODO remove this once we write SSTs to disk
    pub(crate) path: PathBuf,
    pub(crate) options: DbOptions,
    pub(crate) table_store: TableStore,
}

impl DbInner {
    pub fn new(
        path: impl AsRef<Path>,
        options: DbOptions,
        table_store: TableStore,
    ) -> Result<Self, SlateDBError> {
        let path_buf = path.as_ref().to_path_buf();

        if !path_buf.exists() {
            std::fs::create_dir_all(path).map_err(SlateDBError::IoError)?;
        }

        Ok(Self {
            state: Arc::new(RwLock::new(Arc::new(DbState::create()))),
            path: path_buf,
            options,
            table_store,
        })
    }

    /// Get the value for a given key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let maybe_bytes = std::iter::once(&snapshot.memtable)
            .chain(snapshot.imm_memtables.iter())
            .find_map(|memtable| memtable.get(key));

        if let Some(val) = maybe_bytes {
            return Ok(val);
        }

        for sst in &snapshot.as_ref().l0 {
            if let Some(block_index) = self.find_block_for_key(sst, key).await? {
                let block = self.table_store.read_block(sst, block_index).await?;
                if let Some(val) = self.find_val_in_block(&block, key).await? {
                    return Ok(val);
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
        let block_idx = sst.info.block_meta.partition_point(|bm| bm.first_key > key);
        if block_idx == sst.info.block_meta.len() {
            return Ok(None);
        }
        Ok(Some(block_idx))
    }

    async fn find_val_in_block(
        &self,
        block: &Block,
        key: &[u8],
    ) -> Result<Option<Option<Bytes>>, SlateDBError> {
        let mut iter = BlockIterator::from_first_key(block);
        while let Some(current_key_value) = iter.next_entry().await? {
            match current_key_value {
                KVEntry::KeyValue(kv) => {
                    if kv.key == key {
                        return Ok(Some(Some(kv.value)));
                    }
                }
                KVEntry::Tombstone(tombstone_key) => {
                    if tombstone_key == key {
                        return Ok(Some(None));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Put a key-value pair into the database. Key and value must not be empty.
    pub async fn put(&self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let memtable = {
            let guard = self.state.read();
            Arc::clone(&guard.memtable)
        };

        memtable.put(key, value).await;
    }

    /// Delete a key from the database. Key must not be empty.
    pub async fn delete(&self, key: &[u8]) {
        assert!(!key.is_empty(), "key cannot be empty");

        // Clone memtable to avoid a deadlock with flusher thread.
        let memtable = {
            let guard = self.state.read();
            Arc::clone(&guard.memtable)
        };

        memtable.delete(key).await;
    }
}

pub struct Db {
    inner: Arc<DbInner>,
    /// Notifies the L0 flush thread to stop working.
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread.
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Db {
    pub fn open(
        path: impl AsRef<Path>,
        options: DbOptions,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, SlateDBError> {
        let sst_format = SsTableFormat::new(4096, options.min_filter_keys);
        let table_store = TableStore::new(object_store, sst_format);
        let inner = Arc::new(DbInner::new(path, options, table_store)?);
        let (tx, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx);
        Ok(Self {
            inner,
            flush_notifier: tx,
            flush_thread: Mutex::new(flush_thread),
        })
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        // Tell the notifier thread to shut down.
        self.flush_notifier.send(()).ok();

        // Scope the flush_thread lock so its lock isn't held while awaiting the final flush below.
        {
            // Wait for the flush thread to finish.
            let mut flush_thread = self.flush_thread.lock();
            if let Some(flush_thread) = flush_thread.take() {
                flush_thread.join().expect("Failed to join flush thread");
            }
        }

        // Force a final flush on the mutable memtable
        self.inner.flush().await?;
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
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    #[tokio::test]
    async fn test_put_get_delete() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open(
            "/tmp/test_kv_store",
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
            },
            object_store,
        )
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
    async fn test_put_empty_value() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open(
            "/tmp/test_kv_store",
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
            },
            object_store,
        )
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
            "/tmp/test_kv_store",
            DbOptions {
                flush_ms: 100,
                min_filter_keys: 0,
            },
            object_store,
        )
        .unwrap();

        let memtable = {
            let lock = kv_store.inner.state.read();
            lock.memtable.clone()
        };

        memtable.put_optimistic(b"abc1111", b"value1111");
        memtable.put_optimistic(b"abc2222", b"value2222");
        memtable.put_optimistic(b"abc3333", b"value3333");

        let mut iter = memtable.iter();
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc1111".as_slice());

        kv_store.flush().await.unwrap();

        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc2222".as_slice());

        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"abc3333".as_slice());
    }
}
