//! # Batch
//!
//! This module contains types for batch operations. A batch operation is a
//! collection of write operations (puts and/or deletes) that are applied
//! atomically to the database.

/// A batch of write operations (puts and/or deletes). All operations in the batch
/// are applied atomically to the database. If multiple operations appear for a
/// a single key, the last operation will be applied. The others will be dropped.
///
/// # Examples
/// ```rust,no_run,compile_fail
/// use slatedb::batch::{WriteBatch, WriteOp};
///
/// let object_store = Arc::new(LocalFileSystem::new());
/// let db = Db::open("path/to/db".into(), object_store).await;
///
/// let mut batch = WriteBatch::new();
/// batch.put(b"key1", b"value1");
/// batch.put(b"key2", b"value2");
/// batch.delete(b"key3");
///
/// db.write(batch).await;
/// ```
pub struct WriteBatch {
    pub(crate) ops: Vec<WriteOp>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

pub enum WriteOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch { ops: Vec::new() }
    }

    /// Put a key-value pair into the batch. Keys must not be empty.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty(), "key cannot be empty");
        self.ops.push(WriteOp::Put(key.to_vec(), value.to_vec()));
    }

    /// Delete a key-value pair into the batch. Keys must not be empty.
    pub fn delete(&mut self, key: &[u8]) {
        assert!(!key.is_empty(), "key cannot be empty");
        self.ops.push(WriteOp::Delete(key.to_vec()));
    }
}
