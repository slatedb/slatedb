use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;

use crate::{
    error::SlateDBError,
    iter::KeyValueIterator,
    types::{RowEntry, ValueDeletable},
    utils::merge_options,
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
///     fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes, MergeOperatorError> {
///         let existing = u64::from_le_bytes(existing_value.as_ref().try_into().unwrap());
///         let increment = u64::from_le_bytes(value.as_ref().try_into().unwrap());
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
    /// * `existing_value` - The current accumulated value
    /// * `value` - The new value to merge with the existing value
    ///
    /// # Returns
    /// * `Ok(Bytes)` - The merged result as bytes
    /// * `Err(MergeOperatorError)` - If the merge operation fails
    fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes, MergeOperatorError>;
}

pub(crate) type MergeOperatorType = Arc<dyn MergeOperator + Send + Sync>;

#[cfg(test)]
pub(crate) struct AppendingMergeOperator;

#[cfg(test)]
impl AppendingMergeOperator {
    pub(crate) fn new() -> Self {
        AppendingMergeOperator {}
    }
}

#[cfg(test)]
impl MergeOperator for AppendingMergeOperator {
    fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes, MergeOperatorError> {
        let mut merged = value.to_vec();
        merged.extend_from_slice(&existing_value);
        Ok(Bytes::from(merged))
    }
}

pub(crate) struct MergeOperatorIterator<T: KeyValueIterator> {
    accummulator: MergeOperatorAccumulator,
    delegate: T,
}

#[allow(unused)]
impl<T: KeyValueIterator> MergeOperatorIterator<T> {
    pub(crate) fn new(
        merge_operator: MergeOperatorType,
        delegate: T,
        merge_different_expire_ts: bool,
    ) -> Self {
        Self {
            // merge_operator,
            accummulator: MergeOperatorAccumulator::new(merge_operator, merge_different_expire_ts),
            delegate,
        }
    }
}

pub(crate) struct MergeOperatorAccumulator {
    /// The merge operator to use.
    merge_operator: MergeOperatorType,
    /// Entry from the delegate that we've peeked ahead and buffered.
    buffered_entry: Option<RowEntry>,
    /// Whether to merge entries with different expire timestamps. On read path, this
    /// should be always true and during compactions, this should be false.
    merge_different_expire_ts: bool,
}

impl MergeOperatorAccumulator {
    pub(crate) fn new(merge_operator: MergeOperatorType, merge_different_expire_ts: bool) -> Self {
        Self {
            merge_operator,
            buffered_entry: None,
            merge_different_expire_ts,
        }
    }

    pub(crate) fn maybe_merge(
        &mut self,
        entry: Option<RowEntry>,
    ) -> Result<Option<RowEntry>, SlateDBError> {
        match entry {
            Some(entry) => self.merge(entry),
            None => Ok(None),
        }
    }

    pub(crate) fn merge(&mut self, entry: RowEntry) -> Result<Option<RowEntry>, SlateDBError> {
        let buffered_entry = match self.flush() {
            Some(buffered_entry) => {
                if matches!(buffered_entry.value, ValueDeletable::Merge(_)) {
                    buffered_entry
                } else {
                    self.buffered_entry = Some(entry);
                    return Ok(Some(buffered_entry));
                }
            }
            None => {
                if matches!(entry.value, ValueDeletable::Merge(_)) {
                    self.buffered_entry = Some(entry);
                    return Ok(None);
                } else {
                    return Ok(Some(entry));
                }
            }
        };
        let buffered_value = match buffered_entry.value {
            ValueDeletable::Merge(ref v) => v.clone(),
            _ => unreachable!("Entry doesn't contain merge operand."),
        };
        if buffered_entry.key == entry.key
            && (self.merge_different_expire_ts || buffered_entry.expire_ts == entry.expire_ts)
        {
            // Accumulate timestamps. For create_ts we use the maximum (when the accumulated value has last changed),
            // and for expire_ts we use the minimum (when the accumulated becomes invalid).
            let max_create_ts = merge_options(buffered_entry.create_ts, entry.create_ts, i64::max);
            let min_expire_ts = merge_options(buffered_entry.expire_ts, entry.expire_ts, i64::min);
            // For sequence number, we want to use the maximum. Since all the entries are sorted in descending order,
            // we just ensure it keeps decreasing.
            if buffered_entry.seq < entry.seq {
                return Err(SlateDBError::InvalidDBState);
            }

            match entry.value {
                ValueDeletable::Value(value) => {
                    // Final merge with a regular value
                    let final_value = self.merge_operator.merge(buffered_value, value)?;
                    Ok(Some(RowEntry::new(
                        buffered_entry.key,
                        ValueDeletable::Value(final_value),
                        buffered_entry.seq,
                        max_create_ts,
                        min_expire_ts,
                    )))
                }
                ValueDeletable::Merge(value) => {
                    let final_value = self.merge_operator.merge(buffered_value, value)?;
                    self.buffered_entry = Some(RowEntry::new(
                        buffered_entry.key,
                        ValueDeletable::Merge(final_value),
                        buffered_entry.seq,
                        max_create_ts,
                        min_expire_ts,
                    ));
                    Ok(None)
                }
                ValueDeletable::Tombstone => Ok(Some(RowEntry::new(
                    buffered_entry.key,
                    ValueDeletable::Value(buffered_value),
                    buffered_entry.seq,
                    max_create_ts,
                    min_expire_ts,
                ))),
            }
        } else {
            // Different key or expire timestamp. We need to return both entries ...
            let final_result = buffered_entry;
            // Store the different key entry in the look-ahead buffer
            self.buffered_entry = Some(entry);
            // And return the accumulated merge
            Ok(Some(final_result))
        }
    }

    pub(crate) fn flush(&mut self) -> Option<RowEntry> {
        self.buffered_entry.take()
    }
}

impl<T: KeyValueIterator> KeyValueIterator for MergeOperatorIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            let next = self.delegate.next_entry().await?;
            match next {
                Some(next_entry) => match self.accummulator.merge(next_entry)? {
                    Some(merged) => return Ok(Some(merged)),
                    None => continue,
                },
                None => {
                    return Ok(self.accummulator.flush());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::VecDeque, fmt::Debug};

    use rstest::rstest;

    use crate::test_utils::assert_iterator;

    use super::*;

    #[tokio::test]
    async fn test_merge_operator_iterator() {
        let merge_operator = Arc::new(AppendingMergeOperator::new());
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
        let mut iterator =
            MergeOperatorIterator::<MockKeyValueIterator>::new(merge_operator, data.into(), true);
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
        let merge_operator = Arc::new(AppendingMergeOperator::new());
        let mut iterator = MergeOperatorIterator::<MockKeyValueIterator>::new(
            merge_operator,
            test_case.unsorted_data.into(),
            test_case.merge_different_expire_ts,
        );
        assert_iterator(&mut iterator, test_case.expected).await;
    }

    struct MockKeyValueIterator {
        values: VecDeque<RowEntry>,
    }

    impl KeyValueIterator for MockKeyValueIterator {
        async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            Ok(self.values.pop_front())
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
}
