use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use log::error;

use crate::{
    error::SlateDBError,
    iter::KeyValueIterator,
    types::{RowEntry, ValueDeletable},
    utils::{is_not_expired, merge_options},
};

#[non_exhaustive]
#[derive(Clone, Debug, Error)]
pub enum MergeOperatorError {
    #[error("merge_batch called with empty operands and no existing value")]
    EmptyBatch,
}

/// A trait for implementing custom merge operations in SlateDB.
///
/// The MergeOperator allows applications to bypass the traditional read/modify/update cycle
/// in performance-critical situations where computation can be expressed using an associative operator.
/// This is particularly useful for implementing:
/// - Aggregations (e.g., counters, sums)
/// - Buffering (e.g., append-only lists)
///
/// # Associativity Requirement
/// The merge operation MUST be associative, meaning that for any values a, b, and c:
/// merge(merge(a, b), c) == merge(a, merge(b, c))
///
/// # Examples
/// Here's an example of a counter merge operator:
/// ```
/// use bytes::Bytes;
/// use slatedb::{MergeOperator, MergeOperatorError};
///
/// struct CounterMergeOperator;
///
/// impl MergeOperator for CounterMergeOperator {
///     fn merge(&self, _key: &Bytes, existing_value: Option<Bytes>, operand: Bytes) -> Result<Bytes, MergeOperatorError> {
///         let existing = existing_value
///             .map(|v| u64::from_le_bytes(v.as_ref().try_into().unwrap()))
///             .unwrap_or(0);
///         let increment = u64::from_le_bytes(operand.as_ref().try_into().unwrap());
///         Ok(Bytes::copy_from_slice(&(existing + increment).to_le_bytes()))
///     }
///
///     fn merge_batch(&self, _key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Result<Bytes, MergeOperatorError> {
///         let mut total = existing_value
///             .map(|v| u64::from_le_bytes(v.as_ref().try_into().unwrap()))
///             .unwrap_or(0);
///         
///         for operand in operands {
///             let increment = u64::from_le_bytes(operand.as_ref().try_into().unwrap());
///             total += increment;
///         }
///         
///         Ok(Bytes::copy_from_slice(&total.to_le_bytes()))
///     }
/// }
/// ```
pub trait MergeOperator {
    /// Merges the existing value with a new value to produce a combined result.
    ///
    /// This method is called during reads and compactions to combine multiple merge operands
    /// into a single value. The implementation must be associative to ensure correct behavior.
    ///
    /// # Arguments
    /// * `key` - The key of the entry
    /// * `existing_value` - The current accumulated value
    /// * `value` - The new value to merge with the existing value
    ///
    /// # Returns
    /// * `Ok(Bytes)` - The merged result as bytes
    /// * `Err(MergeOperatorError)` - If the merge operation fails
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError>;

    /// Merges a batch of operands with an optional existing value.
    ///
    /// This method allows for more efficient batch processing of merge operands.
    /// The default implementation applies pairwise merging, but implementations
    /// can override this for better performance (e.g., a counter can sum all values at once).
    ///
    /// # Arguments
    /// * `key` - The key of the entry
    /// * `existing_value` - The current accumulated value (if any)
    /// * `operands` - A slice of operands to merge, ordered from oldest to newest
    ///
    /// # Returns
    /// * `Ok(Bytes)` - The merged result as bytes
    /// * `Err(MergeOperatorError)` - If the merge operation fails
    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut result = existing_value;
        for operand in operands {
            result = Some(self.merge(key, result, operand.clone())?);
        }
        result.ok_or(MergeOperatorError::EmptyBatch)
    }
}

pub(crate) type MergeOperatorType = Arc<dyn MergeOperator + Send + Sync>;
// TODO: Make this configurable
// we can change to better value based on the system memory
const MERGE_BATCH_SIZE: usize = 100;

/// An iterator that ensures merge operands are not returned when no merge operator is configured.
pub(crate) struct MergeOperatorRequiredIterator<T: KeyValueIterator> {
    delegate: T,
}

impl<T: KeyValueIterator> MergeOperatorRequiredIterator<T> {
    pub(crate) fn new(delegate: T) -> Self {
        Self { delegate }
    }
}

#[async_trait]
impl<T: KeyValueIterator> KeyValueIterator for MergeOperatorRequiredIterator<T> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.delegate.init().await
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let next_entry = self.delegate.next_entry().await?;
        if let Some(entry) = next_entry {
            match &entry.value {
                ValueDeletable::Merge(_) => {
                    return Err(SlateDBError::MergeOperatorMissing);
                }
                _ => return Ok(Some(entry)),
            }
        }
        Ok(None)
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.delegate.seek(next_key).await
    }
}

/// An iterator that merges mergeable entries into a single value.
///
/// It is expected that this is the top level iterator in a merge scan, and therefore
/// return a ValueDeletable::Value entry (instead of a Merge even if the resolved value
/// is a merge operand).
pub(crate) struct MergeOperatorIterator<T: KeyValueIterator> {
    merge_operator: MergeOperatorType,
    delegate: T,
    /// Entry from the delegate that we've peeked ahead and buffered.
    buffered_entry: Option<RowEntry>,
    /// Whether to merge entries with different expire timestamps.
    merge_different_expire_ts: bool,
    now: i64,
}

#[derive(Debug, Clone)]
struct MergeTracker {
    max_create_ts: Option<i64>,
    min_expire_ts: Option<i64>,
    seq: u64,
}

impl MergeTracker {
    fn update(&mut self, entry: &RowEntry) -> Result<(), SlateDBError> {
        self.max_create_ts = merge_options(self.max_create_ts, entry.create_ts, i64::max);
        self.min_expire_ts = merge_options(self.min_expire_ts, entry.expire_ts, i64::min);

        // sequence numbers should be descending
        if self.seq < entry.seq {
            error!(
                "Invalid sequence number ordering: {} > {}",
                self.seq, entry.seq
            );
            return Err(SlateDBError::InvalidDBState);
        }

        self.seq = std::cmp::max(self.seq, entry.seq);
        Ok(())
    }
}

#[allow(unused)]
impl<T: KeyValueIterator> MergeOperatorIterator<T> {
    pub(crate) fn new(
        merge_operator: MergeOperatorType,
        delegate: T,
        merge_different_expire_ts: bool,
        now: i64,
    ) -> Self {
        Self {
            merge_operator,
            delegate,
            buffered_entry: None,
            merge_different_expire_ts,
            now,
        }
    }
}

impl<T: KeyValueIterator> MergeOperatorIterator<T> {
    /// Check if an entry matches the current key and expire_ts
    fn is_matching_entry(
        &self,
        entry: &RowEntry,
        key: &Bytes,
        first_expire_ts: Option<i64>,
    ) -> bool {
        entry.key == *key && (self.merge_different_expire_ts || first_expire_ts == entry.expire_ts)
    }

    fn process_batch(
        &self,
        key: &Bytes,
        batch: &mut Vec<RowEntry>,
        merge_tracker: &mut MergeTracker,
    ) -> Result<Bytes, SlateDBError> {
        batch.reverse();
        let mut operands: Vec<Bytes> = Vec::with_capacity(batch.len());
        for entry in &*batch {
            merge_tracker.update(entry)?;
            if let Some(v) = entry.value.as_bytes() {
                operands.push(v);
            }
        }

        let batch_result = self.merge_operator.merge_batch(key, None, &operands)?;
        batch.clear();
        Ok(batch_result)
    }

    async fn merge_with_older_entries(
        &mut self,
        first_entry: RowEntry,
    ) -> Result<Option<RowEntry>, SlateDBError> {
        let key = first_entry.key.clone();
        let first_expire_ts = first_entry.expire_ts;

        let mut merge_tracker = MergeTracker {
            max_create_ts: None,
            min_expire_ts: None,
            seq: first_entry.seq,
        };

        let mut results = Vec::new();
        let mut batch = Vec::with_capacity(MERGE_BATCH_SIZE);

        let mut next = Some(first_entry);

        // this loop returns the "base value" (non merge operand) if it exists
        let base = loop {
            if let Some(entry) = next {
                if !self.is_matching_entry(&entry, &key, first_expire_ts) {
                    self.buffered_entry = Some(entry);
                    break None;
                } else if !matches!(entry.value, ValueDeletable::Merge(_)) {
                    // found a Value or Tombstone, this is the base value
                    break Some(entry);
                } else if is_not_expired(&entry, self.now) {
                    batch.push(entry);
                }

                // if the batch is full, merge it and add the result to the results vector
                if batch.len() >= MERGE_BATCH_SIZE {
                    results.push(self.process_batch(&key, &mut batch, &mut merge_tracker)?);
                }

                next = self.delegate.next_entry().await?;
            } else {
                break None;
            }
        };

        // handle leftovers from the last batch
        if !batch.is_empty() {
            results.push(self.process_batch(&key, &mut batch, &mut merge_tracker)?);
        }

        let found_base = base.is_some();
        if results.is_empty() && !found_base {
            return Ok(None);
        }

        results.reverse();
        let final_result = self.merge_operator.merge_batch(
            &key,
            base.and_then(|b| b.value.as_bytes()),
            &results,
        )?;

        Ok(Some(RowEntry {
            key: key.clone(),
            value: if found_base {
                ValueDeletable::Value(final_result)
            } else {
                ValueDeletable::Merge(final_result)
            },
            seq: merge_tracker.seq,
            create_ts: merge_tracker.max_create_ts,
            expire_ts: merge_tracker.min_expire_ts,
        }))
    }
}

#[async_trait]
impl<T: KeyValueIterator> KeyValueIterator for MergeOperatorIterator<T> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.delegate.init().await
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let next_entry = match self.buffered_entry.take() {
            Some(entry) => Some(entry),
            None => self.delegate.next_entry().await?,
        };
        if let Some(entry) = next_entry {
            match &entry.value {
                ValueDeletable::Merge(_) => {
                    // A mergeable entry, we need to accumulate all mergeable entries
                    // ahead for the same key and merge them into a single value.
                    return self.merge_with_older_entries(entry).await;
                }
                // Not a mergeable entry, just return it.
                _ => return Ok(Some(entry)),
            }
        }
        Ok(None)
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.delegate.seek(next_key).await
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::VecDeque, fmt::Debug};

    use rstest::rstest;

    use crate::test_utils::assert_iterator;

    use super::*;

    struct MockMergeOperator;

    impl MergeOperator for MockMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            value: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            match existing_value {
                Some(existing) => {
                    let mut merged = existing.to_vec();
                    merged.extend_from_slice(&value);
                    Ok(Bytes::from(merged))
                }
                None => Ok(value),
            }
        }
    }

    /// Mock merge operator that tracks whether merge_batch is called
    struct MockBatchedMergeOperator {
        merge_batch_call_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockBatchedMergeOperator {
        fn new() -> (Self, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
            let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            (
                Self {
                    merge_batch_call_count: counter.clone(),
                },
                counter,
            )
        }
    }

    impl MergeOperator for MockBatchedMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            value: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            // Same as MockMergeOperator - concatenate bytes
            match existing_value {
                Some(existing) => {
                    let mut merged = existing.to_vec();
                    merged.extend_from_slice(&value);
                    Ok(Bytes::from(merged))
                }
                None => Ok(value),
            }
        }

        fn merge_batch(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operands: &[Bytes],
        ) -> Result<Bytes, MergeOperatorError> {
            // Increment counter to track that merge_batch was called
            self.merge_batch_call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // Efficiently concatenate all operands at once
            let mut result = existing_value.unwrap_or_default().to_vec();
            for operand in operands {
                result.extend_from_slice(operand);
            }
            Ok(Bytes::from(result))
        }
    }

    #[tokio::test]
    async fn test_merge_operator_iterator() {
        let merge_operator = Arc::new(MockMergeOperator {});
        let data = vec![
            RowEntry::new_merge(b"key1", b"1", 1),
            RowEntry::new_merge(b"key1", b"2", 2),
            RowEntry::new_merge(b"key1", b"3", 3),
            RowEntry::new_merge(b"key1", b"4", 4),
            RowEntry::new_value(b"key2", b"1", 5),
            RowEntry::new_value(b"key3", b"1", 6),
            RowEntry::new_merge(b"key3", b"2", 7),
            RowEntry::new_merge(b"key3", b"3", 8),
        ];
        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            0,
        );
        assert_iterator(
            &mut iterator,
            vec![
                RowEntry::new_merge(b"key1", b"1234", 4),
                RowEntry::new_value(b"key2", b"1", 5),
                RowEntry::new_value(b"key3", b"123", 8),
            ],
        )
        .await;
    }

    #[derive(Debug)]
    struct TestCase {
        unsorted_data: Vec<RowEntry>,
        expected: Vec<RowEntry>,
        merge_different_expire_ts: bool,
    }

    impl Default for TestCase {
        fn default() -> Self {
            Self {
                unsorted_data: vec![],
                expected: vec![],
                merge_different_expire_ts: true,
            }
        }
    }

    #[rstest]
    #[case::different_expire_ts_read_path(TestCase {
        unsorted_data: vec![
            RowEntry::new_merge(b"key1", b"1", 1).with_expire_ts(1),
            RowEntry::new_merge(b"key1", b"2", 2).with_expire_ts(2),
            RowEntry::new_merge(b"key1", b"3", 3).with_expire_ts(3),
            RowEntry::new_value(b"key2", b"1", 4),
            RowEntry::new_merge(b"key3", b"1", 5).with_expire_ts(1),
            RowEntry::new_merge(b"key3", b"2", 6).with_expire_ts(1),
            RowEntry::new_merge(b"key3", b"3", 7).with_expire_ts(2),
        ],
        expected: vec![
            RowEntry::new_merge(b"key1", b"123", 3).with_expire_ts(1),
            RowEntry::new_value(b"key2", b"1", 4),
            RowEntry::new_merge(b"key3", b"123", 7).with_expire_ts(1),
        ],
        ..TestCase::default()
    })]
    #[case::different_expire_ts_write_path(TestCase {
        unsorted_data: vec![
            RowEntry::new_merge(b"key1", b"1", 1).with_expire_ts(1),
            RowEntry::new_merge(b"key1", b"2", 2).with_expire_ts(2),
            RowEntry::new_merge(b"key1", b"3", 3).with_expire_ts(3),
            RowEntry::new_value(b"key2", b"1", 4),
            RowEntry::new_merge(b"key3", b"1", 5).with_expire_ts(1),
            RowEntry::new_merge(b"key3", b"2", 6).with_expire_ts(1),
            RowEntry::new_merge(b"key3", b"3", 7).with_expire_ts(2),
        ],
        expected: vec![
            RowEntry::new_merge(b"key1", b"3", 3).with_expire_ts(3),
            RowEntry::new_merge(b"key1", b"2", 2).with_expire_ts(2),
            RowEntry::new_merge(b"key1", b"1", 1).with_expire_ts(1),
            RowEntry::new_value(b"key2", b"1", 4),
            RowEntry::new_merge(b"key3", b"3", 7).with_expire_ts(2),
            RowEntry::new_merge(b"key3", b"12", 6).with_expire_ts(1),
        ],
        // On write path (compaction, memtable), we don't merge entries
        // with different expire timestamps to allow per-element expiration.
        merge_different_expire_ts: false
    })]
    #[case::merge_with_tombstone(TestCase {
        unsorted_data: vec![
            RowEntry::new_merge(b"key1", b"1", 1),
            RowEntry::new_merge(b"key1", b"2", 2),
            RowEntry::new_tombstone(b"key1", 3),
            RowEntry::new_merge(b"key1", b"3", 4),
            RowEntry::new_value(b"key2", b"1", 5)
        ],
        expected: vec![
            // Merge + Tombstone becomes a value to invalidate older entries.
            RowEntry::new_value(b"key1", b"3", 4),
            RowEntry::new_merge(b"key1", b"12", 2),
            RowEntry::new_value(b"key2", b"1", 5)
        ],
        ..TestCase::default()
    })]
    #[case::multiple_values(TestCase {
        unsorted_data: vec![
            RowEntry::new_value(b"key1", b"1", 1),
            RowEntry::new_value(b"key1", b"2", 2),
        ],
        expected: vec![
            RowEntry::new_value(b"key1", b"2", 2),
            RowEntry::new_value(b"key1", b"1", 1),
        ],
        ..TestCase::default()
    })]
    #[tokio::test]
    async fn test(#[case] test_case: TestCase) {
        let merge_operator = Arc::new(MockMergeOperator {});
        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            test_case.unsorted_data.into(),
            test_case.merge_different_expire_ts,
            0,
        );
        assert_iterator(&mut iterator, test_case.expected).await;
    }

    struct MockKeyValueIterator {
        values: VecDeque<RowEntry>,
    }

    #[async_trait]
    impl KeyValueIterator for MockKeyValueIterator {
        async fn init(&mut self) -> Result<(), SlateDBError> {
            Ok(())
        }

        async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            Ok(self.values.pop_front())
        }

        async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
            self.values.retain(|entry| entry.key == next_key);
            Ok(())
        }
    }

    impl From<Vec<RowEntry>> for MockKeyValueIterator {
        /// Converts a vector of RowEntries into a MockKeyValueIterator. The vector is sorted
        /// by key and reverse sequence number.
        fn from(values: Vec<RowEntry>) -> Self {
            let mut sorted_values = values;
            sorted_values.sort_by(|left, right| {
                let ord = left.key.cmp(&right.key);
                if ord == Ordering::Equal {
                    right.seq.cmp(&left.seq)
                } else {
                    ord
                }
            });
            Self {
                values: sorted_values.into(),
            }
        }
    }

    /// A merge operator that routes to different merge strategies based on key prefix
    struct KeyPrefixMergeOperator;

    impl MergeOperator for KeyPrefixMergeOperator {
        fn merge(
            &self,
            key: &Bytes,
            existing_value: Option<Bytes>,
            value: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            if key.starts_with(b"sum:") {
                // Sum merge for sum keys
                match existing_value {
                    Some(existing) => {
                        let existing_num =
                            u64::from_le_bytes(existing.as_ref().try_into().unwrap());
                        let new_num = u64::from_le_bytes(value.as_ref().try_into().unwrap());
                        Ok(Bytes::copy_from_slice(
                            &(existing_num + new_num).to_le_bytes(),
                        ))
                    }
                    None => Ok(value),
                }
            } else if key.starts_with(b"max:") {
                // Max merge for max keys
                match existing_value {
                    Some(existing) => {
                        let existing_num =
                            u64::from_le_bytes(existing.as_ref().try_into().unwrap());
                        let new_num = u64::from_le_bytes(value.as_ref().try_into().unwrap());
                        Ok(Bytes::copy_from_slice(
                            &existing_num.max(new_num).to_le_bytes(),
                        ))
                    }
                    None => Ok(value),
                }
            } else {
                // Default to concat for unknown prefixes
                match existing_value {
                    Some(existing) => {
                        let mut merged = existing.to_vec();
                        merged.extend_from_slice(&value);
                        Ok(Bytes::from(merged))
                    }
                    None => Ok(value),
                }
            }
        }

        // Override merge_batch to handle batches efficiently with key-based routing
        fn merge_batch(
            &self,
            key: &Bytes,
            existing_value: Option<Bytes>,
            operands: &[Bytes],
        ) -> Result<Bytes, MergeOperatorError> {
            if key.starts_with(b"max:") {
                // For max operator, find the maximum value across all operands
                let mut max_val = existing_value
                    .map(|v| u64::from_le_bytes(v.as_ref().try_into().unwrap()))
                    .unwrap_or(0);

                for operand in operands {
                    let val = u64::from_le_bytes(operand.as_ref().try_into().unwrap());
                    max_val = max_val.max(val);
                }

                Ok(Bytes::copy_from_slice(&max_val.to_le_bytes()))
            } else {
                // For other prefixes, use pairwise merge
                let mut result = existing_value;
                for operand in operands {
                    result = Some(self.merge(key, result, operand.clone())?);
                }
                result.ok_or(MergeOperatorError::EmptyBatch)
            }
        }
    }

    #[tokio::test]
    async fn should_route_merge_based_on_key_prefix() {
        let merge_operator = Arc::new(KeyPrefixMergeOperator {});

        let data = vec![
            // Sum key
            RowEntry::new_merge(b"sum:counter", &5u64.to_le_bytes(), 1),
            RowEntry::new_merge(b"sum:counter", &3u64.to_le_bytes(), 2),
            RowEntry::new_merge(b"sum:counter", &7u64.to_le_bytes(), 3),
            // Max key
            RowEntry::new_merge(b"max:score", &5u64.to_le_bytes(), 4),
            RowEntry::new_merge(b"max:score", &10u64.to_le_bytes(), 5),
            RowEntry::new_merge(b"max:score", &3u64.to_le_bytes(), 6),
        ];

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            0,
        );

        // Expected: max should return 10, sum should return 15
        let max_expected = 10u64.to_le_bytes();
        let sum_expected = 15u64.to_le_bytes();

        assert_iterator(
            &mut iterator,
            vec![
                RowEntry::new_merge(b"max:score", &max_expected, 6),
                RowEntry::new_merge(b"sum:counter", &sum_expected, 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_batched_merge_with_many_operands() {
        let merge_operator = Arc::new(MockMergeOperator {});

        let mut data = vec![];
        for i in 1..=250 {
            data.push(RowEntry::new_merge(b"key1", &[i as u8], i));
        }

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            0,
        );

        let expected_bytes: Vec<u8> = (1..=250).map(|i| i as u8).collect();
        let expected = vec![RowEntry::new_merge(b"key1", &expected_bytes, 250)];

        assert_iterator(&mut iterator, expected).await;
    }

    #[tokio::test]
    async fn test_batched_merge_with_base_value() {
        let merge_operator = Arc::new(MockMergeOperator {});

        let mut data = vec![];
        data.push(RowEntry::new_value(b"key1", b"BASE", 0));
        for i in 1..=150 {
            data.push(RowEntry::new_merge(b"key1", &[i as u8], i));
        }

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            0,
        );

        let mut expected_bytes = b"BASE".to_vec();
        expected_bytes.extend((1..=150).map(|i| i as u8));
        let expected = vec![RowEntry::new_value(b"key1", &expected_bytes, 150)];

        assert_iterator(&mut iterator, expected).await;
    }

    #[tokio::test]
    async fn test_merge_batch_is_actually_called() {
        let (merge_operator, call_count) = MockBatchedMergeOperator::new();
        let merge_operator = Arc::new(merge_operator);

        let mut data = vec![];
        for i in 1..=250 {
            data.push(RowEntry::new_merge(b"key1", &[i as u8], i));
        }

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            0,
        );

        let expected_bytes: Vec<u8> = (1..=250).map(|i| i as u8).collect();
        let expected = vec![RowEntry::new_merge(b"key1", &expected_bytes, 250)];
        assert_iterator(&mut iterator, expected).await;

        let actual_calls = call_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            actual_calls, 4,
            "Expected merge_batch to be called 4 times for 250 operands (3 batches + 1 final merge), but was called {} times",
            actual_calls
        );
    }

    #[tokio::test]
    async fn test_merge_batch_with_base_value_call_count() {
        let (merge_operator, call_count) = MockBatchedMergeOperator::new();
        let merge_operator = Arc::new(merge_operator);

        // Create base value + 150 merge operands (will require 2 batches: 100 + 50)
        let mut data = vec![];
        data.push(RowEntry::new_value(b"key1", b"BASE", 0));
        for i in 1..=150 {
            data.push(RowEntry::new_merge(b"key1", &[i as u8], i));
        }

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            0,
        );

        let mut expected_bytes = b"BASE".to_vec();
        expected_bytes.extend((1..=150).map(|i| i as u8));
        let expected = vec![RowEntry::new_value(b"key1", &expected_bytes, 150)];
        assert_iterator(&mut iterator, expected).await;

        let actual_calls = call_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            actual_calls, 3,
            "Expected merge_batch to be called 3 times for 150 operands (2 batches + 1 final merge), but was called {} times",
            actual_calls
        );
    }

    #[tokio::test]
    async fn test_merge_operator_filters_expired_entries() {
        let merge_operator = Arc::new(MockMergeOperator {});

        // Create entries with different expiration times
        // now = 100, so entries with expire_ts <= 100 are expired
        // Entries are sorted by reverse seq, so we need the first entry (highest seq) to be non-expired
        // to properly initialize the tracker
        let data = vec![
            // Non-expired merge operands (expire_ts > 100) - highest seq first
            RowEntry::new_merge(b"key1", b"4", 4).with_expire_ts(300),
            RowEntry::new_merge(b"key1", b"2", 2).with_expire_ts(150),
            RowEntry::new_merge(b"key1", b"1", 1).with_expire_ts(200),
            // Expired merge operands (expire_ts <= 100) - should be filtered out
            RowEntry::new_merge(b"key1", b"5", 5).with_expire_ts(100),
            RowEntry::new_merge(b"key1", b"3", 3).with_expire_ts(50),
        ];

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            100, // now = 100
        );

        // Only non-expired entries (4, 2, 1) should be merged
        // Entries with expire_ts 50 and 100 should be filtered out
        // Since entries are sorted by reverse seq, first entry is seq=5 (expired)
        // Tracker initializes with seq=5, but max_create_ts and min_expire_ts are None
        // Seq=5 is filtered out (expired), then seq=4, 2, 1 are added to batch
        // process_batch updates tracker: min_expire_ts becomes 150 (min of 300, 150, 200)
        // Batch is [4, 2, 1], reversed to [1, 2, 4], merged to "124"
        // Final seq is 5 (from first entry initialization), min_expire_ts is 150
        assert_iterator(
            &mut iterator,
            vec![RowEntry::new_merge(b"key1", b"124", 5).with_expire_ts(150)], // seq=5 from first entry, min_expire_ts=150 from non-expired entries
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_operator_filters_expired_entries_with_base_value() {
        let merge_operator = Arc::new(MockMergeOperator {});

        // Create entries with a base value and mixed expired/non-expired merge operands
        // Entries are sorted by reverse seq, so base value (seq=0) comes last
        // We need non-expired merge operands with higher seq to come first
        let data = vec![
            // Non-expired merge operands (higher seq first)
            RowEntry::new_merge(b"key1", b"4", 4).with_expire_ts(300),
            RowEntry::new_merge(b"key1", b"2", 2).with_expire_ts(250),
            RowEntry::new_merge(b"key1", b"1", 1).with_expire_ts(150),
            // Expired merge operand - should be filtered out
            RowEntry::new_merge(b"key1", b"3", 3).with_expire_ts(50),
            // Base value (non-expired) - comes last due to seq=0
            RowEntry::new_value(b"key1", b"BASE", 0).with_expire_ts(200),
        ];

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            100, // now = 100
        );

        // Base value + non-expired entries (4, 2, 1) should be merged
        // Entry with expire_ts 50 should be filtered out
        // First entry is seq=4 (non-expired), tracker starts with seq=4, but max_create_ts and min_expire_ts are None
        // Seq=4, 2, 1 are added to batch (all non-expired)
        // process_batch updates tracker: min_expire_ts becomes 150 (min of 300, 250, 150)
        // Batch is [4, 2, 1], reversed to [1, 2, 4], merged to "124", then merged with BASE to "BASE124"
        // Final seq is 4 (from first entry initialization), min_expire_ts is 150
        assert_iterator(
            &mut iterator,
            vec![RowEntry::new_value(b"key1", b"BASE124", 4).with_expire_ts(150)], // seq=4 from first entry, min_expire_ts=150
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_operator_handles_all_expired_entries() {
        let merge_operator = Arc::new(MockMergeOperator {});

        // All merge operands are expired
        let data = vec![
            RowEntry::new_merge(b"key1", b"1", 1).with_expire_ts(50),
            RowEntry::new_merge(b"key1", b"2", 2).with_expire_ts(80),
            RowEntry::new_merge(b"key1", b"3", 3).with_expire_ts(90),
        ];

        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            100, // now = 100, all entries are expired
        );

        // All entries are expired, so nothing should be returned
        assert_iterator(&mut iterator, vec![]).await;
    }
}
