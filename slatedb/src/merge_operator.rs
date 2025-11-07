use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

use crate::{
    error::SlateDBError,
    iter::KeyValueIterator,
    types::{RowEntry, ValueDeletable},
    utils::{is_not_expired, merge_options},
};

#[non_exhaustive]
#[derive(Clone, Debug, Error)]
pub enum MergeOperatorError {}

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
}

pub(crate) type MergeOperatorType = Arc<dyn MergeOperator + Send + Sync>;

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
    async fn merge_with_older_entries(
        &mut self,
        first_entry: RowEntry,
    ) -> Result<Option<RowEntry>, SlateDBError> {
        let key = first_entry.key.clone();
        let mut entries = vec![first_entry];

        // Collect all mergeable entries for the same key until we hit a value or tombstone
        let mut found_base_value = false;
        loop {
            let next = self.delegate.next_entry().await?;
            match next {
                Some(next_entry)
                    if key == next_entry.key
                        && (self.merge_different_expire_ts
                            || entries[0].expire_ts == next_entry.expire_ts) =>
                {
                    // For sequence number, we want to use the maximum. Since all the entries are sorted in descending order,
                    // we just ensure it keeps decreasing.
                    if entries.last().expect("should have at least one entry").seq < next_entry.seq
                    {
                        return Err(SlateDBError::InvalidDBState);
                    }

                    // If we hit a Value or Tombstone, include it but stop collecting
                    found_base_value = !matches!(next_entry.value, ValueDeletable::Merge(_));
                    entries.push(next_entry);
                    if found_base_value {
                        break;
                    }
                }
                Some(next_entry) => {
                    // Different key or expire timestamp. Store it in the buffer.
                    self.buffered_entry = Some(next_entry);
                    break;
                }
                None => {
                    // End of iterator
                    break;
                }
            }
        }

        // Reverse entries so we merge from oldest to newest
        entries.reverse();

        // don't call the merge operator if the base value is a value
        // and instead just set it as the initial merge value
        let mut merged_value: Option<Bytes> =
            if let ValueDeletable::Value(bytes) = &entries[0].value {
                Some(bytes.clone())
            } else {
                None
            };

        let mut max_create_ts = entries[0].create_ts;
        let mut min_expire_ts = entries[0].expire_ts;
        let mut seq = entries[0].seq;

        // a base value can be either a tombstone or a value, in either
        // case we should not apply the merge operator to it (in the former
        // case we just apply merges and in the latter we have already set
        // the merged value to the base value)
        if found_base_value {
            entries.remove(0);
        }

        for entry in entries.iter().filter(|e| is_not_expired(e, self.now)) {
            // Accumulate timestamps
            max_create_ts = merge_options(max_create_ts, entry.create_ts, i64::max);
            min_expire_ts = merge_options(min_expire_ts, entry.expire_ts, i64::min);
            seq = std::cmp::max(seq, entry.seq);

            match &entry.value {
                ValueDeletable::Merge(value) => {
                    merged_value = Some(self.merge_operator.merge(
                        &key,
                        merged_value,
                        value.clone(),
                    )?);
                }
                // we collect at most one Tombstone/Value entry in the loop above, and
                // if we do collect one it will be the first entry (which is removed in
                // the conditional above) so this should never happen
                _ => unreachable!("Should not merge any non-merge entries"),
            }
        }

        if let Some(result_value) = merged_value {
            return Ok(Some(RowEntry::new(
                key,
                if found_base_value {
                    ValueDeletable::Value(result_value)
                } else {
                    ValueDeletable::Merge(result_value)
                },
                seq,
                max_create_ts,
                min_expire_ts,
            )));
        }

        Ok(None)
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
    }

    #[tokio::test]
    async fn should_route_merge_based_on_key_prefix() {
        // given
        let merge_operator = Arc::new(KeyPrefixMergeOperator {});

        let data = vec![
            // Sum key - should sum values
            RowEntry::new_merge(b"sum:counter", &5u64.to_le_bytes(), 1),
            RowEntry::new_merge(b"sum:counter", &3u64.to_le_bytes(), 2),
            RowEntry::new_merge(b"sum:counter", &7u64.to_le_bytes(), 3),
            // Max key - should keep maximum value
            RowEntry::new_merge(b"max:score", &5u64.to_le_bytes(), 4),
            RowEntry::new_merge(b"max:score", &10u64.to_le_bytes(), 5),
            RowEntry::new_merge(b"max:score", &3u64.to_le_bytes(), 6),
        ];

        // when
        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            data.into(),
            true,
            0,
        );

        // then
        assert_iterator(
            &mut iterator,
            vec![
                RowEntry::new_merge(b"max:score", &10u64.to_le_bytes(), 6),
                RowEntry::new_merge(b"sum:counter", &15u64.to_le_bytes(), 3),
            ],
        )
        .await;
    }
}
