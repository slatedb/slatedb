//! # Batch
//!
//! This module contains types for batch operations. A batch operation is a
//! collection of write operations (puts and/or deletes) that are applied
//! atomically to the database.

use crate::config::PutOptions;
use bytes::Bytes;
use std::collections::BTreeMap;
use uuid::Uuid;

/// A batch of write operations (puts and/or deletes). All operations in the
/// batch are applied atomically to the database. If multiple operations appear
/// for a a single key, the last operation will be applied. The others will be
/// dropped.
///
/// # Examples
/// ```rust
/// # async fn run() -> Result<(), slatedb::Error> {
/// #     use std::sync::Arc;
/// #     use slatedb::object_store::memory::InMemory;
///     use slatedb::Db;
///     use slatedb::WriteBatch;
///
///     let object_store = Arc::new(InMemory::new());
///     let db = Db::open("path/to/db", object_store).await?;
///
///     let mut batch = WriteBatch::new();
///     batch.put(b"key1", b"value1");
///     batch.put(b"key2", b"value2");
///     batch.delete(b"key3");
///
///     db.write(batch).await;
///     Ok(())
/// # };
/// # tokio_test::block_on(run());
/// ```
///
/// Note that the `WriteBatch` has an unlimited size. This means that batch
/// writes can exceed `l0_sst_size_bytes` (when `WAL` is disabled). It also
/// means that WAL SSTs could get large if there's a large batch write.
#[derive(Clone, Debug)]
pub struct WriteBatch {
    pub(crate) ops: BTreeMap<Bytes, WriteOp>,
    pub(crate) txn_id: Option<Uuid>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// A write operation in a batch.
#[derive(PartialEq, Clone)]
pub(crate) enum WriteOp {
    Put(Bytes, Bytes, PutOptions),
    Delete(Bytes),
}

impl std::fmt::Debug for WriteOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteOp::Put(key, value, options) => {
                let key = if key.len() > 10 {
                    format!("{:?}...", &key[..10])
                } else {
                    format!("{:?}", key)
                };
                let value = if value.len() > 10 {
                    format!("{:?}...", &value[..10])
                } else {
                    format!("{:?}", value)
                };
                write!(f, "Put({key}, {value}, {:?})", options)
            }
            WriteOp::Delete(key) => {
                let key = if key.len() > 10 {
                    format!("{:?}...", &key[..10])
                } else {
                    format!("{:?}", key)
                };
                write!(f, "Delete({key})")
            }
        }
    }
}

impl WriteOp {
    pub fn key(&self) -> &Bytes {
        match self {
            WriteOp::Put(key, _, _) => key,
            WriteOp::Delete(key) => key,
        }
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            ops: BTreeMap::new(),
            txn_id: None,
        }
    }

    /// Put a key-value pair into the batch. Keys must not be empty.
    ///
    /// # Panics
    /// - if the key is empty
    /// - if the key size is larger than u16::MAX
    /// - if the value size is larger than u32::MAX
    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(key, value, &PutOptions::default())
    }

    /// Put a key-value pair into the batch. Keys must not be empty.
    ///
    /// # Panics
    /// - if the key is empty
    /// - if the key size is larger than u16::MAX
    /// - if the value size is larger than u32::MAX
    pub fn put_with_options<K, V>(&mut self, key: K, value: V, options: &PutOptions)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(
            key.len() <= u16::MAX as usize,
            "key size must be <= u16::MAX"
        );
        assert!(
            value.len() <= u32::MAX as usize,
            "value size must be <= u32::MAX"
        );
        self.ops.insert(
            Bytes::copy_from_slice(key),
            WriteOp::Put(
                Bytes::copy_from_slice(key),
                Bytes::copy_from_slice(value),
                options.clone(),
            ),
        );
    }

    /// Delete a key-value pair into the batch. Keys must not be empty.
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(
            key.len() <= u16::MAX as usize,
            "key size must be <= u16::MAX"
        );
        self.ops.insert(
            Bytes::copy_from_slice(key),
            WriteOp::Delete(Bytes::copy_from_slice(key)),
        );
    }

    /// Get operation for specified key (O(log n))
    pub(crate) fn get_op(&self, key: &[u8]) -> Option<&WriteOp> {
        self.ops.get(key)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    struct WriteOpTestCase {
        key: Vec<u8>,
        // None is a delete and options will be ignored
        value: Option<Vec<u8>>,
        options: PutOptions,
    }

    #[rstest]
    #[case(vec![WriteOpTestCase {
        key: b"key".to_vec(),
        value: Some(b"value".to_vec()),
        options: PutOptions::default(),
    }])]
    #[case(vec![WriteOpTestCase {
        key: b"key".to_vec(),
        value: None,
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "key size must be <= u16::MAX")]
    #[case(vec![WriteOpTestCase {
        key: vec![b'k'; 65_536], // 2^16
        value: None,
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "value size must be <= u32::MAX")]
    #[case(vec![WriteOpTestCase {
        key: b"key".to_vec(),
        value: Some(vec![b'x'; u32::MAX as usize + 1]), // 2^32
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "key cannot be empty")]
    #[case(vec![WriteOpTestCase {
        key: b"".to_vec(),
        value: Some(b"value".to_vec()),
        options: PutOptions::default(),
    }])]
    #[should_panic(expected = "key cannot be empty")]
    #[case(vec![WriteOpTestCase {
        key: b"".to_vec(),
        value: None,
        options: PutOptions::default(),
    }])]
    fn test_put_delete_batch(#[case] test_case: Vec<WriteOpTestCase>) {
        let mut batch = WriteBatch::new();
        let mut expected_ops: BTreeMap<Bytes, WriteOp> = BTreeMap::new();
        for test_case in test_case {
            if let Some(value) = test_case.value {
                batch.put_with_options(
                    test_case.key.as_slice(),
                    value.as_slice(),
                    &test_case.options,
                );
                expected_ops.insert(
                    Bytes::from(test_case.key.clone()),
                    WriteOp::Put(
                        Bytes::from(test_case.key),
                        Bytes::from(value),
                        test_case.options,
                    ),
                );
            } else {
                batch.delete(test_case.key.as_slice());
                expected_ops.insert(
                    Bytes::from(test_case.key.clone()),
                    WriteOp::Delete(Bytes::from(test_case.key)),
                );
            }
        }
        assert_eq!(batch.ops, expected_ops);
    }

    #[test]
    fn test_writebatch_deduplication() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key1", b"value2"); // Should overwrite previous

        assert_eq!(batch.ops.len(), 1); // Only one entry due to deduplication
        let op = batch.ops.get(b"key1".as_ref()).unwrap();
        match op {
            WriteOp::Put(_, value, _) => assert_eq!(value.as_ref(), b"value2"),
            _ => panic!("Expected Put operation"),
        }
    }
}
