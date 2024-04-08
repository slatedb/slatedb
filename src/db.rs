use std::{path::{Path, PathBuf}, sync::Arc};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::mem_table::MemTable;

pub struct DbOptions {
  pub flush_ms: usize,
}

#[derive(Clone)]
pub(crate) struct DbState {
  pub(crate) memtable: Arc<MemTable>,
  pub(crate) imm_memtables: Vec<Arc<MemTable>>,
}

impl DbState {
  fn create() -> Self {
    Self {
      memtable: Arc::new(MemTable::new()),
      imm_memtables: Vec::new(),
    }
  }
}

pub(crate) struct DbInner {
  pub(crate) state: Arc<RwLock<Arc<DbState>>>,
  #[allow(dead_code)]  // TODO remove this once we write SSTs to disk
  pub(crate) path: PathBuf,
  pub(crate) options: DbOptions,
}

// TODO Return Result<> for get/put/delete everything... ?
impl DbInner {
  pub fn new(path: impl AsRef<Path>, options: DbOptions) -> Self {
    let path_buf = path.as_ref().to_path_buf();

    if !path_buf.exists() {
      std::fs::create_dir_all(path).unwrap_or_else(|_| {
        panic!("Failed to create directory: {}", path_buf.display());
      });
    }

    Self {
      state: Arc::new(RwLock::new(Arc::new(DbState::create()))),
      path: path_buf,
      options,
    }
  }

  /// Get the value for a given key.
  pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
    let snapshot = {
      let guard = self.state.read();
      Arc::clone(&guard)
    };

    let maybe_bytes = std::iter::once(&snapshot.memtable)
      .chain(snapshot.imm_memtables.iter())
      .find_map(|memtable| {
        memtable.get(key)
      });

    // Filter is needed to remove tombstone deletes.
    maybe_bytes.filter(|value| !value.is_empty())

    // TODO look in SSTs on disk
    // TODO look in SSTs on object storage
  }

  /// Put a key-value pair into the database. Key and value must not be empty.
  pub async fn put(&self, key: &[u8], value: &[u8]) {
    assert!(!key.is_empty(), "key cannot be empty");
    assert!(!value.is_empty(), "value cannot be empty");

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
  pub fn open(path: impl AsRef<Path>, options: DbOptions) -> Self {
    let inner = Arc::new(DbInner::new(path, options));
    let (tx, rx) = crossbeam_channel::unbounded();
    let flush_thread = inner.spawn_flush_thread(rx);
    Self {
      inner,
      flush_notifier: tx,
      flush_thread: Mutex::new(flush_thread),
    }
  }

  pub fn close(&self) {
    // Tell the notifier thread to shut down.
    self.flush_notifier.send(()).ok();

    // Wait for the flush thread to finish.
    let mut flush_thread = self.flush_thread.lock();
    if let Some(flush_thread) = flush_thread.take() {
        flush_thread
            .join()
            .expect("Failed to join flush thread");
    }

    // Force a final flush on the mutable memtable
    self.inner.flush();
  }

  pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
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
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokio::runtime::Runtime;

  #[test]
  fn test_put_get_delete() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let kv_store = Db::open("/tmp/test_kv_store", DbOptions { flush_ms: 100 });
      let key = b"test_key";
      let value = b"test_value";
      kv_store.put(key, value).await;
      assert_eq!(kv_store.get(key).await, Some(Bytes::from_static(value)));
      kv_store.delete(key).await;
      assert!(kv_store.get(key).await.is_none());
    });
  }
}