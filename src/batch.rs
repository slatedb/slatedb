//! # Batch
//!
//! This module contains types for batch operations. A batch operation is a
//! collection of write operations (puts and/or deletes) that are applied
//! atomically to the database.

use crate::config::PutOptions;
use bytes::Bytes;

/// A batch of write operations (puts and/or deletes). All operations in the
/// batch are applied atomically to the database. If multiple operations appear
/// for a a single key, the last operation will be applied. The others will be
/// dropped.
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
///
/// Note that the `WriteBatch` has an unlimited size. This means that batch
/// writes can exceed `l0_sst_size_bytes` (when `WAL` is disabled). It also
/// means that WAL SSTs could get large if there's a large batch write.
pub struct WriteBatch {
    pub(crate) ops: Vec<WriteOp>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

pub enum WriteOp {
    Put(Bytes, Bytes, PutOptions),
    Delete(Bytes),
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch { ops: Vec::new() }
    }

    /// Put a key-value pair into the batch. Keys must not be empty.
    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(key, value, &PutOptions::default())
    }

    /// Put a key-value pair into the batch. Keys must not be empty.
    pub fn put_with_options<K, V>(&mut self, key: K, value: V, options: &PutOptions)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        assert!(!key.is_empty(), "key cannot be empty");
        self.ops.push(WriteOp::Put(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            options.clone(),
        ));
    }

    /// Delete a key-value pair into the batch. Keys must not be empty.
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        assert!(!key.is_empty(), "key cannot be empty");
        self.ops.push(WriteOp::Delete(Bytes::copy_from_slice(key)));
    }
}
