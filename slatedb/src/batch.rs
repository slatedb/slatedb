//! # Batch
//!
//! This module contains types for batch operations. A batch operation is a
//! collection of write operations (puts and/or deletes) that are applied
//! atomically to the database.

use crate::config::{MergeOptions, PutOptions};
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::mem_table::{KVTableInternalKeyRange, SequencedKey};
use crate::types::{RowEntry, ValueDeletable};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, HashSet};
use std::iter::Peekable;
use std::ops::RangeBounds;
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
    pub(crate) ops: BTreeMap<SequencedKey, WriteOp>,
    pub(crate) txn_id: Option<Uuid>,
    /// due to merges, multiple writes may happen for the same key in one batch,
    /// this write_idx tracks the order in which writes happen within a single
    /// batch (unrelated to the sequence number of the batch, which is assigned
    /// atomically by the oracle when the batch is committed).
    pub(crate) write_idx: u64,
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
    Merge(Bytes, Bytes, MergeOptions),
}

impl std::fmt::Debug for WriteOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn trunc(bytes: &Bytes) -> String {
            if bytes.len() > 10 {
                format!("{:?}...", &bytes[..10])
            } else {
                format!("{:?}", bytes)
            }
        }

        match self {
            WriteOp::Put(key, value, options) => {
                let key = trunc(key);
                let value = trunc(value);
                write!(f, "Put({key}, {value}, {:?})", options)
            }
            WriteOp::Delete(key) => {
                let key = trunc(key);
                write!(f, "Delete({key})")
            }
            WriteOp::Merge(key, value, options) => {
                let key = trunc(key);
                let value = trunc(value);
                write!(f, "Merge({key}, {value}, {:?})", options)
            }
        }
    }
}

impl WriteOp {
    /// Convert WriteOp to RowEntry for queries
    pub(crate) fn to_row_entry(
        &self,
        seq: u64,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> RowEntry {
        match self {
            WriteOp::Put(key, value, _options) => {
                // For queries, we don't need to compute expiration time here
                // since these are uncommitted writes. The expiration will be
                // computed when the batch is actually written.
                RowEntry::new(
                    key.clone(),
                    ValueDeletable::Value(value.clone()),
                    seq,
                    create_ts,
                    expire_ts,
                )
            }
            WriteOp::Delete(key) => RowEntry::new(
                key.clone(),
                ValueDeletable::Tombstone,
                seq,
                create_ts,
                expire_ts,
            ),
            WriteOp::Merge(key, value, _options) => RowEntry::new(
                key.clone(),
                ValueDeletable::Merge(value.clone()),
                seq,
                create_ts,
                expire_ts,
            ),
        }
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            ops: BTreeMap::new(),
            txn_id: None,
            write_idx: 0,
        }
    }

    /// Remove all existing ops for the given user key from the batch.
    fn remove_ops_by_key(&mut self, key: &Bytes) {
        // Find the contiguous range of entries for this user key and delete them in place.
        let start = SequencedKey::new(key.clone(), u64::MAX);
        let end = SequencedKey::new(key.clone(), 0);

        // Collect keys to remove
        let keys_to_remove: Vec<SequencedKey> = self
            .ops
            .range(start..=end)
            .map(|(k, _)| k.clone())
            .collect();

        // Remove them
        for k in keys_to_remove {
            self.ops.remove(&k);
        }
    }

    pub(crate) fn with_txn_id(self, txn_id: Uuid) -> Self {
        Self {
            ops: self.ops,
            txn_id: Some(txn_id),
            write_idx: self.write_idx,
        }
    }

    fn assert_kv<K, V>(&self, key: &K, value: &V)
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
        self.assert_kv(&key, &value);

        let key = Bytes::copy_from_slice(key.as_ref());
        // put will overwrite the existing key so we can safely
        // remove all previous entries.
        self.remove_ops_by_key(&key);
        self.ops.insert(
            SequencedKey::new(key.clone(), self.write_idx),
            WriteOp::Put(
                key.clone(),
                Bytes::copy_from_slice(value.as_ref()),
                options.clone(),
            ),
        );

        self.write_idx += 1;
    }

    /// Merge a key-value pair into the batch. Keys must not be empty.
    pub fn merge<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_with_options(key, value, &MergeOptions::default());
    }

    /// Merge a key-value pair into the batch with custom options.
    pub fn merge_with_options<K, V>(&mut self, key: K, value: V, options: &MergeOptions)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.assert_kv(&key, &value);

        let key = Bytes::copy_from_slice(key.as_ref());
        self.ops.insert(
            SequencedKey::new(key.clone(), self.write_idx),
            WriteOp::Merge(
                key.clone(),
                Bytes::copy_from_slice(value.as_ref()),
                options.clone(),
            ),
        );

        self.write_idx += 1;
    }

    /// Delete a key-value pair into the batch. Keys must not be empty.
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        self.assert_kv(&key, &[]);

        let key = Bytes::copy_from_slice(key.as_ref());

        // delete will overwrite the existing key so we can safely
        // remove all previous entries.
        self.remove_ops_by_key(&key);
        self.ops.insert(
            SequencedKey::new(key.clone(), self.write_idx),
            WriteOp::Delete(key),
        );

        self.write_idx += 1;
    }

    pub(crate) fn keys(&self) -> HashSet<Bytes> {
        self.ops.keys().map(|key| key.user_key.clone()).collect()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// Iterator over WriteBatch entries.
///
/// Holds an owned WriteBatch instead of a reference,
/// allowing it to be used without lifetime constraints.
pub(crate) struct WriteBatchIterator {
    iter: Peekable<Box<dyn Iterator<Item = (SequencedKey, WriteOp)> + Send + Sync>>,
    ordering: IterationOrder,
}

impl WriteBatchIterator {
    pub(crate) fn new(
        batch: WriteBatch,
        range: impl RangeBounds<Bytes>,
        ordering: IterationOrder,
    ) -> Self {
        let range = KVTableInternalKeyRange::from(range);

        // Clone the entries from the BTreeMap into a Vec
        let entries: Vec<(SequencedKey, WriteOp)> = batch
            .ops
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let iter: Box<dyn Iterator<Item = (SequencedKey, WriteOp)> + Send + Sync> = match ordering {
            IterationOrder::Ascending => Box::new(entries.into_iter()),
            IterationOrder::Descending => Box::new(entries.into_iter().rev()),
        };

        Self {
            iter: iter.peekable(),
            ordering,
        }
    }
}

#[async_trait]
impl KeyValueIterator for WriteBatchIterator {
    async fn init(&mut self) -> Result<(), crate::error::SlateDBError> {
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, crate::error::SlateDBError> {
        Ok(self
            .iter
            .next()
            .map(|(_, op)| op.to_row_entry(u64::MAX, None, None)))
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), crate::error::SlateDBError> {
        while let Some((key, _)) = self.iter.peek() {
            if match self.ordering {
                IterationOrder::Ascending => key.user_key.as_ref() < next_key,
                IterationOrder::Descending => key.user_key.as_ref() > next_key,
            } {
                self.iter.next();
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::config::Ttl;
    use crate::test_utils::assert_iterator;
    use crate::types::RowEntry;

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
        let mut expected_ops: BTreeMap<SequencedKey, WriteOp> = BTreeMap::new();
        for (seq, test_case) in test_case.into_iter().enumerate() {
            if let Some(value) = test_case.value {
                batch.put_with_options(
                    test_case.key.as_slice(),
                    value.as_slice(),
                    &test_case.options,
                );
                expected_ops.insert(
                    SequencedKey::new(Bytes::from(test_case.key.clone()), seq as u64),
                    WriteOp::Put(
                        Bytes::from(test_case.key),
                        Bytes::from(value),
                        test_case.options,
                    ),
                );
            } else {
                batch.delete(test_case.key.as_slice());
                expected_ops.insert(
                    SequencedKey::new(Bytes::from(test_case.key.clone()), seq as u64),
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
        let op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 1))
            .unwrap();
        match op {
            WriteOp::Put(_, value, _) => assert_eq!(value.as_ref(), b"value2"),
            _ => panic!("Expected Put operation"),
        }
    }

    #[tokio::test]
    async fn test_writebatch_iterator_basic() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");
        batch.put(b"key2", b"value2");
        batch.delete(b"key4");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
            RowEntry::new_value(b"key2", b"value2", u64::MAX),
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new(
                Bytes::from("key4"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_writebatch_iterator_range() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");
        batch.put(b"key5", b"value5");

        // Test range [key2, key4)
        let mut iter = WriteBatchIterator::new(
            batch.clone(),
            BytesRange::from(Bytes::from_static(b"key2")..Bytes::from_static(b"key4")),
            IterationOrder::Ascending,
        );

        let expected = vec![RowEntry::new_value(b"key3", b"value3", u64::MAX)];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_writebatch_iterator_descending() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");
        batch.put(b"key2", b"value2");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Descending);

        let expected = vec![
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new_value(b"key2", b"value2", u64::MAX),
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_writebatch_iterator_seek_ascending() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");
        batch.put(b"key5", b"value5");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        // Seek to key3
        iter.seek(b"key3").await.unwrap();

        // Should get key3 and key5
        let expected = vec![
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new_value(b"key5", b"value5", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_writebatch_iterator_seek_descending() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");
        batch.put(b"key5", b"value5");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Descending);

        // Seek to key3 (in descending, we want keys <= key3)
        iter.seek(b"key3").await.unwrap();

        // Should get key3 and key1 (in descending order)
        let expected = vec![
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_writebatch_iterator_empty_batch() {
        let batch = WriteBatch::new();
        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        let result = iter.next_entry().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_writebatch_iterator_seek_to_nonexistent() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        // Seek to key2 (doesn't exist)
        iter.seek(b"key2").await.unwrap();

        // Should get key3 (next available)
        let result = iter.next_entry().await.unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key3"));
        assert_eq!(
            entry.value,
            ValueDeletable::Value(Bytes::from_static(b"value3"))
        );
    }

    #[tokio::test]
    async fn test_writebatch_iterator_seek_beyond_end() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        // Seek beyond maximum key
        iter.seek(b"key9").await.unwrap();

        // Should be exhausted
        let result = iter.next_entry().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_writebatch_iterator_multiple_tombstones() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.delete(b"key2");
        batch.put(b"key3", b"value3");
        batch.delete(b"key4");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
            RowEntry::new(
                Bytes::from_static(b"key4"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_writebatch_iterator_seek_before_first() {
        let mut batch = WriteBatch::new();
        batch.put(b"key2", b"value2");
        batch.put(b"key3", b"value3");

        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        // Seek before first key
        iter.seek(b"key1").await.unwrap();

        // Should get key2 (first available)
        let result = iter.next_entry().await.unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, Bytes::from_static(b"key2"));
    }

    #[tokio::test]
    async fn test_writebatch_iterator_range_with_tombstones() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.delete(b"key2");
        batch.put(b"key3", b"value3");
        batch.delete(b"key4");
        batch.put(b"key5", b"value5");

        // Range [key2, key4) should include tombstones
        let mut iter = WriteBatchIterator::new(
            batch.clone(),
            BytesRange::from(Bytes::from_static(b"key2")..Bytes::from_static(b"key4")),
            IterationOrder::Ascending,
        );

        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new_value(b"key3", b"value3", u64::MAX),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[test]
    fn should_create_merge_operation_with_default_options() {
        // Given: an empty WriteBatch
        let mut batch = WriteBatch::new();

        // When: adding a merge operation
        batch.merge(b"key1", b"value1");

        // Then: the batch should contain one merge operation with default options
        assert_eq!(batch.ops.len(), 1);
        let op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 0))
            .unwrap();
        match op {
            WriteOp::Merge(key, value, options) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"value1");
                assert_eq!(options, &MergeOptions::default());
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_create_merge_operation_with_custom_options() {
        // Given: an empty WriteBatch and custom merge options
        let mut batch = WriteBatch::new();
        let merge_options = MergeOptions {
            ttl: Ttl::ExpireAfter(3600), // 1 hour
        };

        // When: adding a merge operation with custom options
        batch.merge_with_options(b"key1", b"value1", &merge_options);

        // Then: the batch should contain one merge operation with the custom options
        assert_eq!(batch.ops.len(), 1);
        let op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 0))
            .unwrap();
        match op {
            WriteOp::Merge(key, value, options) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"value1");
                assert_eq!(options.ttl, Ttl::ExpireAfter(3600));
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_allow_multiple_merges_for_same_key() {
        // Given: an empty WriteBatch
        let mut batch = WriteBatch::new();

        // When: adding multiple merge operations for the same key
        batch.merge(b"key1", b"value1");
        batch.merge(b"key1", b"value2");
        batch.merge(b"key1", b"value3");

        // Then: all merge operations should be preserved (not deduplicated)
        assert_eq!(batch.ops.len(), 3);

        // And: all three operations should exist with different sequence numbers
        let op1 = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 0))
            .unwrap();
        let op2 = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 1))
            .unwrap();
        let op3 = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 2))
            .unwrap();

        match (op1, op2, op3) {
            (WriteOp::Merge(_, v1, _), WriteOp::Merge(_, v2, _), WriteOp::Merge(_, v3, _)) => {
                assert_eq!(v1.as_ref(), b"value1");
                assert_eq!(v2.as_ref(), b"value2");
                assert_eq!(v3.as_ref(), b"value3");
            }
            _ => panic!("Expected all Merge operations"),
        }
    }

    #[test]
    fn should_allow_merges_for_different_keys() {
        // Given: an empty WriteBatch
        let mut batch = WriteBatch::new();

        // When: adding merge operations for different keys
        batch.merge(b"key1", b"value1");
        batch.merge(b"key2", b"value2");
        batch.merge(b"key3", b"value3");

        // Then: all merge operations should be preserved
        assert_eq!(batch.ops.len(), 3);

        // And: all keys should exist with correct sequence numbers
        assert!(batch
            .ops
            .contains_key(&SequencedKey::new(Bytes::from_static(b"key1"), 0)));
        assert!(batch
            .ops
            .contains_key(&SequencedKey::new(Bytes::from_static(b"key2"), 1)));
        assert!(batch
            .ops
            .contains_key(&SequencedKey::new(Bytes::from_static(b"key3"), 2)));
    }

    #[test]
    fn should_preserve_both_put_and_merge_when_merge_comes_after_put() {
        // Given: a WriteBatch with a put operation
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"put_value");

        // When: adding a merge operation for the same key
        batch.merge(b"key1", b"merge_value");

        // Then: both operations should remain (merge accumulates on top of put)
        assert_eq!(batch.ops.len(), 2);

        // Verify the put operation exists
        let put_op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 0))
            .unwrap();
        match put_op {
            WriteOp::Put(key, value, _) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"put_value");
            }
            _ => panic!("Expected Put operation"),
        }

        // Verify the merge operation exists
        let merge_op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 1))
            .unwrap();
        match merge_op {
            WriteOp::Merge(key, value, _) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"merge_value");
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_preserve_both_delete_and_merge_when_merge_comes_after_delete() {
        // Given: a WriteBatch with a delete operation
        let mut batch = WriteBatch::new();
        batch.delete(b"key1");

        // When: adding a merge operation for the same key
        batch.merge(b"key1", b"merge_value");

        // Then: both operations should remain (merge accumulates on top of delete)
        assert_eq!(batch.ops.len(), 2);

        // Verify the delete operation exists
        let delete_op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 0))
            .unwrap();
        match delete_op {
            WriteOp::Delete(key) => {
                assert_eq!(key.as_ref(), b"key1");
            }
            _ => panic!("Expected Delete operation"),
        }

        // Verify the merge operation exists
        let merge_op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 1))
            .unwrap();
        match merge_op {
            WriteOp::Merge(key, value, _) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"merge_value");
            }
            _ => panic!("Expected Merge operation"),
        }
    }

    #[test]
    fn should_deduplicate_merge_when_put_is_added_for_same_key() {
        // Given: a WriteBatch with a merge operation
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge_value");

        // When: adding a put operation for the same key
        batch.put(b"key1", b"put_value");

        // Then: only the put operation should remain (merge is deduplicated by put)
        assert_eq!(batch.ops.len(), 1);

        let op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 1))
            .unwrap();
        match op {
            WriteOp::Put(key, value, _) => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"put_value");
            }
            _ => panic!("Expected Put operation"),
        }
    }

    #[test]
    fn should_deduplicate_merge_when_delete_is_added_for_same_key() {
        // Given: a WriteBatch with a merge operation
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge_value");

        // When: adding a delete operation for the same key
        batch.delete(b"key1");

        // Then: only the delete operation should remain (merge is deduplicated by delete)
        assert_eq!(batch.ops.len(), 1);

        let op = batch
            .ops
            .get(&SequencedKey::new(Bytes::from_static(b"key1"), 1))
            .unwrap();
        match op {
            WriteOp::Delete(key) => {
                assert_eq!(key.as_ref(), b"key1");
            }
            _ => panic!("Expected Delete operation"),
        }
    }

    #[tokio::test]
    async fn should_iterate_over_mixed_operations_including_merges() {
        // Given: a WriteBatch with mixed operations including multiple merges
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.merge(b"key2", b"merge1");
        batch.merge(b"key2", b"merge2");
        batch.delete(b"key3");

        // When: creating an iterator
        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        // Then: the iterator should return all operations in order
        let expected = vec![
            RowEntry::new_value(b"key1", b"value1", u64::MAX),
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Merge(Bytes::from_static(b"merge2")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Merge(Bytes::from_static(b"merge1")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key3"),
                ValueDeletable::Tombstone,
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_iterate_over_multiple_merges_for_same_key() {
        // Given: a WriteBatch with multiple merges for the same key
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge1");
        batch.merge(b"key1", b"merge2");
        batch.merge(b"key1", b"merge3");

        // When: creating an iterator
        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);

        // Then: the iterator should return all merge operations
        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge3")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge2")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge1")),
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_iterate_over_merges_in_descending_order() {
        // Given: a WriteBatch with merges for different keys
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge1");
        batch.merge(b"key2", b"merge2");
        batch.merge(b"key1", b"merge3");

        // When: creating a descending iterator
        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Descending);

        // Then: the iterator should return operations in descending order
        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key2"),
                ValueDeletable::Merge(Bytes::from_static(b"merge2")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge1")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key1"),
                ValueDeletable::Merge(Bytes::from_static(b"merge3")),
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_iterate_over_merges_in_range() {
        // Given: a WriteBatch with merges for different keys
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge1");
        batch.merge(b"key3", b"merge3");
        batch.merge(b"key5", b"merge5");

        // When: creating an iterator with a range filter
        let mut iter = WriteBatchIterator::new(
            batch.clone(),
            BytesRange::from(Bytes::from_static(b"key2")..Bytes::from_static(b"key4")),
            IterationOrder::Ascending,
        );

        // Then: the iterator should only return merges within the range
        let expected = vec![RowEntry::new(
            Bytes::from_static(b"key3"),
            ValueDeletable::Merge(Bytes::from_static(b"merge3")),
            u64::MAX,
            None,
            None,
        )];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn should_seek_to_merge_operations() {
        // Given: a WriteBatch with merges for different keys
        let mut batch = WriteBatch::new();
        batch.merge(b"key1", b"merge1");
        batch.merge(b"key3", b"merge3");
        batch.merge(b"key5", b"merge5");

        // When: creating an iterator and seeking to key3
        let mut iter = WriteBatchIterator::new(batch.clone(), .., IterationOrder::Ascending);
        iter.seek(b"key3").await.unwrap();

        // Then: the iterator should return key3 and key5
        let expected = vec![
            RowEntry::new(
                Bytes::from_static(b"key3"),
                ValueDeletable::Merge(Bytes::from_static(b"merge3")),
                u64::MAX,
                None,
                None,
            ),
            RowEntry::new(
                Bytes::from_static(b"key5"),
                ValueDeletable::Merge(Bytes::from_static(b"merge5")),
                u64::MAX,
                None,
                None,
            ),
        ];

        assert_iterator(&mut iter, expected).await;
    }
}
