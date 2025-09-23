//! # Batch
//!
//! This module contains types for batch operations. A batch operation is a
//! collection of write operations (puts and/or deletes) that are applied
//! atomically to the database.

use crate::config::PutOptions;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::types::{RowEntry, ValueDeletable};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{btree_map, BTreeMap};
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
    /// Convert WriteOp to RowEntry for queries
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub(crate) fn get_op(&self, key: &[u8]) -> Option<&WriteOp> {
        self.ops.get(key)
    }

    /// Create an iterator over the WriteBatch entries in the given range
    #[allow(dead_code)]
    pub(crate) fn iter_range<'a>(
        &'a self,
        range: impl RangeBounds<Bytes>,
    ) -> WriteBatchIterator<'a> {
        WriteBatchIterator::new(self, range, IterationOrder::Ascending)
    }
}

/// Iterator over WriteBatch entries
pub(crate) struct WriteBatchIterator<'a> {
    iter: btree_map::Range<'a, Bytes, WriteOp>,
    ordering: IterationOrder,
    current: Option<(&'a Bytes, &'a WriteOp)>,
}

impl<'a> WriteBatchIterator<'a> {
    pub fn new(
        batch: &'a WriteBatch,
        range: impl RangeBounds<Bytes>,
        ordering: IterationOrder,
    ) -> Self {
        let mut iter = batch.ops.range(range);
        let current = match ordering {
            IterationOrder::Ascending => iter.next(),
            IterationOrder::Descending => iter.next_back(),
        };

        Self {
            iter,
            ordering,
            current,
        }
    }
}

#[async_trait]
impl<'a> KeyValueIterator for WriteBatchIterator<'a> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, crate::error::SlateDBError> {
        // Return current item (similar to MemTableIterator pattern)
        let result = self.current.map(|(_, op)| {
            // Use u64::MAX as placeholder seq to indicate highest priority
            op.to_row_entry(u64::MAX, None, None)
        });

        // Advance to next entry based on ordering (similar to next_entry_sync)
        self.current = match self.ordering {
            IterationOrder::Ascending => self.iter.next(),
            IterationOrder::Descending => self.iter.next_back(),
        };

        Ok(result)
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), crate::error::SlateDBError> {
        // Loop similar to MemTableIterator::seek
        loop {
            let should_advance = match &self.current {
                Some((key, _)) => match self.ordering {
                    // For ascending: advance if current key < target key
                    IterationOrder::Ascending => key.as_ref() < next_key,
                    // For descending: advance if current key > target key
                    IterationOrder::Descending => key.as_ref() > next_key,
                },
                None => return Ok(()), // Iterator exhausted
            };

            if should_advance {
                // Advance similar to calling next_entry_sync in MemTableIterator
                self.current = match self.ordering {
                    IterationOrder::Ascending => self.iter.next(),
                    IterationOrder::Descending => self.iter.next_back(),
                };
            } else {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::bytes_range::BytesRange;
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

    #[tokio::test]
    async fn test_writebatch_iterator_basic() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");
        batch.put(b"key2", b"value2");
        batch.delete(b"key4");

        let mut iter = batch.iter_range(..);

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
        let mut iter = batch.iter_range(BytesRange::from(
            Bytes::from_static(b"key2")..Bytes::from_static(b"key4"),
        ));

        let expected = vec![RowEntry::new_value(b"key3", b"value3", u64::MAX)];

        assert_iterator(&mut iter, expected).await;
    }

    #[tokio::test]
    async fn test_writebatch_iterator_descending() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");
        batch.put(b"key2", b"value2");

        let mut iter = WriteBatchIterator::new(&batch, .., IterationOrder::Descending);

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

        let mut iter = WriteBatchIterator::new(&batch, .., IterationOrder::Ascending);

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

        let mut iter = WriteBatchIterator::new(&batch, .., IterationOrder::Descending);

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
        let mut iter = batch.iter_range(..);

        let result = iter.next_entry().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_writebatch_iterator_seek_to_nonexistent() {
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        batch.put(b"key3", b"value3");

        let mut iter = WriteBatchIterator::new(&batch, .., IterationOrder::Ascending);

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

        let mut iter = WriteBatchIterator::new(&batch, .., IterationOrder::Ascending);

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

        let mut iter = batch.iter_range(..);

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

        let mut iter = WriteBatchIterator::new(&batch, .., IterationOrder::Ascending);

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
        let mut iter = batch.iter_range(BytesRange::from(
            Bytes::from_static(b"key2")..Bytes::from_static(b"key4"),
        ));

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
}
