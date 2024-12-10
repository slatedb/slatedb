use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;

use crate::{
    error::SlateDBError,
    iter::KeyValueIterator,
    types::{RowEntry, ValueDeletable},
};

#[derive(Clone, Debug, Error)]
pub enum MergeOperatorError {}

pub trait MergeOperator {
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

impl<T: KeyValueIterator> MergeOperatorIterator<T> {
    /// Merge two options using the provided function.
    fn merge_options<X>(current: Option<X>, next: Option<X>, f: impl Fn(X, X) -> X) -> Option<X> {
        match (current, next) {
            (Some(current), Some(next)) => Some(f(current, next)),
            (None, next) => next,
            (current, None) => current,
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
                                max_create_ts = Self::merge_options(
                                    max_create_ts,
                                    next_entry.create_ts,
                                    i64::max,
                                );
                                min_expire_ts = Self::merge_options(
                                    min_expire_ts,
                                    next_entry.expire_ts,
                                    i64::min,
                                );
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
    async fn test_merge_operator_iterator() -> Result<(), SlateDBError> {
        let merge_operator = Arc::new(MockMergeOperator {});
        let data = vec![
            RowEntry::new_merge(b"key1", b"4", 3),
            RowEntry::new_merge(b"key1", b"3", 2),
            RowEntry::new_merge(b"key1", b"2", 1),
            RowEntry::new_merge(b"key1", b"1", 0),
            RowEntry::new_value(b"key2", b"1", 0),
            RowEntry::new_merge(b"key3", b"3", 0),
            RowEntry::new_merge(b"key3", b"2", 0),
            RowEntry::new_value(b"key3", b"1", 0),
        ];
        let mut iterator =
            MergeOperatorIterator::<MockKeyValueIterator>::new(merge_operator, data.into());
        assert_eq!(
            iterator.next_entry().await?,
            Some(RowEntry::new_merge(b"key1", b"4321", 7))
        );
        assert_eq!(
            iterator.next_entry().await?,
            Some(RowEntry::new_value(b"key2", b"1", 3))
        );
        assert_eq!(
            iterator.next_entry().await?,
            Some(RowEntry::new_value(b"key3", b"321", 2))
        );
        assert_eq!(iterator.next_entry().await?, None);
        Ok(())
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
