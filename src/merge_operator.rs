use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;

use crate::{
    error::SlateDBError,
    iter::KeyValueIterator,
    types::{RowEntry, ValueDeletable},
    utils::merge_options,
};

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
/// use slatedb::merge_operator::{MergeOperator, MergeOperatorError};
///
/// struct CounterMergeOperator;
///
/// impl MergeOperator for CounterMergeOperator {
///     fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes, MergeOperatorError> {
///         let existing = u64::from_le_bytes(existing_value.as_ref().try_into().unwrap());
///         let increment = u64::from_le_bytes(value.as_ref().try_into().unwrap());
///         Ok(Bytes::copy_from_slice(&(existing + increment).to_be_bytes()))
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

pub(crate) struct MergeOperatorIterator<T: KeyValueIterator> {
    merge_operator: MergeOperatorType,
    delegate: T,
    /// Entry from the delegate that we've peeked ahead and buffered.
    buffered_entry: Option<RowEntry>,
    /// Whether to merge entries with different expire timestamps.
    merge_different_expire_ts: bool,
}

#[allow(unused)]
impl<T: KeyValueIterator> MergeOperatorIterator<T> {

    pub(crate) fn new(merge_operator: MergeOperatorType, delegate: T) -> Self {
        Self {
            merge_operator,
            delegate,
            buffered_entry: None,
            merge_different_expire_ts: true,
        }
    }
}

impl<T: KeyValueIterator> KeyValueIterator for MergeOperatorIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let next_entry = match self.buffered_entry.take() {
            Some(entry) => Some(entry),
            None => self.delegate.next_entry().await?,
        };
        if let Some(entry) = next_entry {
            match &entry.value {
                // A mergeable entry, we need to accumulate all mergeable entries
                // ahead for the same key and merge them into a single value.
                ValueDeletable::Merge(merge_value) => {
                    let current_key = entry.key.clone();
                    let mut current_value = merge_value.clone();
                    let mut max_create_ts = entry.create_ts;
                    let mut min_expire_ts = entry.expire_ts;
                    let mut max_seq = entry.seq;

                    // Keep looking ahead and merging as long as we find mergeable entries
                    loop {
                        let next = self.delegate.next_entry().await?;
                        match next {
                            Some(next_entry)
                                if next_entry.key == current_key
                                    && (self.merge_different_expire_ts
                                        || entry.expire_ts == next_entry.expire_ts) =>
                            {
                                // Update timestamps
                                max_create_ts =
                                    merge_options(max_create_ts, next_entry.create_ts, i64::max);
                                min_expire_ts =
                                    merge_options(min_expire_ts, next_entry.expire_ts, i64::min);
                                // Update sequence number
                                max_seq = max_seq.max(next_entry.seq);

                                match next_entry.value {
                                    ValueDeletable::Value(value) => {
                                        // Final merge with a regular value
                                        let merged =
                                            self.merge_operator.merge(current_value, value)?;
                                        return Ok(Some(RowEntry::new(
                                            current_key,
                                            ValueDeletable::Value(merged),
                                            max_seq,
                                            max_create_ts,
                                            min_expire_ts,
                                        )));
                                    }
                                    ValueDeletable::Merge(value) => {
                                        // Continue merging
                                        current_value =
                                            self.merge_operator.merge(current_value, value)?;
                                        continue;
                                    }
                                    _ => {
                                        // Hit a tombstone or other type, return the accumulated merge
                                        return Ok(Some(RowEntry::new(
                                            current_key,
                                            ValueDeletable::Merge(current_value),
                                            max_seq,
                                            max_create_ts,
                                            min_expire_ts,
                                        )));
                                    }
                                }
                            }
                            Some(next_entry) => {
                                // Different key, need to return both entries
                                // First return our accumulated merge
                                let result = RowEntry::new(
                                    current_key,
                                    ValueDeletable::Merge(current_value),
                                    max_seq,
                                    max_create_ts,
                                    min_expire_ts,
                                );
                                // Store the different key entry in the buffer
                                self.buffered_entry = Some(next_entry);
                                return Ok(Some(result));
                            }
                            None => {
                                // End of iterator, return accumulated merge
                                return Ok(Some(RowEntry::new(
                                    current_key,
                                    ValueDeletable::Merge(current_value),
                                    0,
                                    max_create_ts,
                                    min_expire_ts,
                                )));
                            }
                        }
                    }
                }
                // Not a mergeable entry, just return it.
                _ => return Ok(Some(entry)),
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::test_utils::assert_iterator;

    use super::*;

    struct MockMergeOperator;

    impl MergeOperator for MockMergeOperator {
        fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes, MergeOperatorError> {
            let mut merged = existing_value.to_vec();
            merged.extend_from_slice(&value);
            Ok(Bytes::from(merged))
        }
    }

    #[tokio::test]
    async fn test_merge_operator_iterator() {
        let merge_operator = Arc::new(MockMergeOperator {});
        let data = vec![
            RowEntry::new_merge(b"key1", b"4", 0),
            RowEntry::new_merge(b"key1", b"3", 0),
            RowEntry::new_merge(b"key1", b"2", 0),
            RowEntry::new_merge(b"key1", b"1", 0),
            RowEntry::new_value(b"key2", b"1", 0),
            RowEntry::new_merge(b"key3", b"3", 0),
            RowEntry::new_merge(b"key3", b"2", 0),
            RowEntry::new_value(b"key3", b"1", 0),
        ];
        let mut iterator =
            MergeOperatorIterator::<MockKeyValueIterator>::new(merge_operator, data.into());
        assert_iterator(
            &mut iterator,
            vec![
                RowEntry::new_merge(b"key1", b"4321", 7),
                RowEntry::new_value(b"key2", b"1", 3),
                RowEntry::new_value(b"key3", b"321", 2),
            ],
        )
        .await;
    }

    struct MockKeyValueIterator {
        values: VecDeque<RowEntry>,
        next_seq: u64,
    }

    impl KeyValueIterator for MockKeyValueIterator {
        async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            Ok(self.values.pop_front().map(|v| {
                let seq = self.next_seq;
                if !self.values.is_empty() {
                    self.next_seq -= 1;
                }
                v.with_seq(seq)
            }))
        }
    }

    impl From<Vec<RowEntry>> for MockKeyValueIterator {
        fn from(values: Vec<RowEntry>) -> Self {
            // Sequence numbers are 0-indexed
            let num_records = values.len() as u64 - 1;
            Self {
                values: values.into(),
                next_seq: num_records,
            }
        }
    }
}
