//! The keys of one batch and the entries that the walks collect for them.

use std::collections::VecDeque;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;

use crate::batch::{WriteBatch, WriteBatchIterator};
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, RowEntryIterator};
use crate::merge_operator::{
    MergeOperatorIterator, MergeOperatorRequiredIterator, MergeOperatorType,
};
use crate::reader::{DbStateReader, ReadTrace};
use crate::types::{KeyValue, RowEntry, ValueDeletable};

/// A key of the batch that no base has answered yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OpenKey {
    /// The index of the key in the batch.
    pub(super) index: usize,
    pub(super) key: Bytes,
}

/// The entries that one layer walk found for one key, newest first.
#[derive(Debug, PartialEq)]
pub(super) struct FoundEntries {
    /// The index of the key in the batch.
    pub(super) index: usize,
    pub(super) entries: Vec<RowEntry>,
}

/// The entries of one layer walk, in the order of the SSTs of the layer.
#[derive(Debug, Default, PartialEq)]
pub(super) struct LayerEntries {
    found: Vec<FoundEntries>,
}

impl LayerEntries {
    pub(super) fn push(&mut self, index: usize, entries: Vec<RowEntry>) {
        self.found.push(FoundEntries { index, entries });
    }

    pub(super) fn extend(&mut self, other: LayerEntries) {
        self.found.extend(other.found);
    }
}

/// The keys of one batch, sorted and unique, with the state of each key.
pub(super) struct Batch {
    /// The distinct keys in sorted order.
    keys: Vec<Bytes>,
    /// The index in `keys` of each input slot.
    slots: Vec<usize>,
    /// The segment of each key, `None` when no segment holds the key.
    segment: Vec<Option<usize>>,
    /// The entries collected for each key.
    entries: Vec<KeyEntries>,
    /// The sequence bound of the batch.
    max_seq: Option<u64>,
}

impl Batch {
    pub(super) fn new<K: AsRef<[u8]>>(keys: &[K], max_seq: Option<u64>) -> Self {
        let mut sorted: Vec<Bytes> = keys
            .iter()
            .map(|key| Bytes::copy_from_slice(key.as_ref()))
            .collect();
        sorted.sort();
        sorted.dedup();
        let slots = keys
            .iter()
            .map(|key| {
                sorted
                    .binary_search_by(|sorted_key| sorted_key.as_ref().cmp(key.as_ref()))
                    .expect("every input key is in the sorted list")
            })
            .collect();
        let entries = sorted.iter().map(|_| KeyEntries::default()).collect();
        let segment = vec![None; sorted.len()];
        Self {
            keys: sorted,
            slots,
            segment,
            entries,
            max_seq,
        }
    }

    pub(super) fn keys(&self) -> &[Bytes] {
        &self.keys
    }

    pub(super) fn set_segment(&mut self, key: usize, segment: usize) {
        self.segment[key] = Some(segment);
    }

    /// Reads the write batch of a transaction. Its entries skip the sequence bound, as in `get`.
    pub(super) async fn read_write_batch(&mut self, write_batch: Option<&RwLock<WriteBatch>>) {
        let Some(write_batch) = write_batch else {
            return;
        };
        // The iterators copy their entries, so the guard is held for the batch only.
        let iters: Vec<WriteBatchIterator> = {
            let guard = write_batch.read();
            self.keys
                .iter()
                .map(|key| {
                    WriteBatchIterator::new(
                        &guard,
                        key.clone()..=key.clone(),
                        IterationOrder::Ascending,
                        u64::MAX,
                        None,
                        None,
                    )
                })
                .collect()
        };
        for (entries, mut iter) in self.entries.iter_mut().zip(iters) {
            while let Some(entry) = iter
                .next()
                .await
                .expect("a write batch never fails to read")
            {
                entries.push(entry);
            }
        }
    }

    /// Reads the memtable, then the immutable memtables, for every open key.
    pub(super) fn read_memtables(&mut self, db_state: &dyn DbStateReader, read_trace: &ReadTrace) {
        let mut tables = vec![db_state.memtable()];
        tables.extend(db_state.imm_memtable().iter().map(|imm| imm.table()));
        for table in tables {
            for (key, entries) in self.keys.iter().zip(self.entries.iter_mut()) {
                if entries.answered {
                    continue;
                }
                let mut iter = table.range(
                    key.clone()..=key.clone(),
                    IterationOrder::Ascending,
                    read_trace.clone(),
                );
                while let Some(entry) = iter.next_sync() {
                    entries.push_visible(self.max_seq, entry);
                }
            }
        }
    }

    /// The keys of one segment that no base has answered yet.
    pub(super) fn open_keys(&self, segment: usize) -> Vec<OpenKey> {
        self.keys
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                self.segment[*index] == Some(segment) && !self.entries[*index].answered
            })
            .map(|(index, key)| OpenKey {
                index,
                key: key.clone(),
            })
            .collect()
    }

    /// Adds the entries of one layer. A key that a newer layer answered drops them.
    pub(super) fn apply(&mut self, entries: LayerEntries) {
        for found in entries.found {
            for entry in found.entries {
                self.entries[found.index].push_visible(self.max_seq, entry);
            }
        }
    }

    pub(super) fn is_done(&self) -> bool {
        self.entries.iter().all(|entries| entries.answered)
    }

    /// Resolves every key and copies each result into its input slots.
    pub(super) async fn resolve(
        self,
        merge_operator: Option<&MergeOperatorType>,
    ) -> Result<Vec<Option<KeyValue>>, SlateDBError> {
        let mut values = Vec::with_capacity(self.entries.len());
        for entries in self.entries {
            values.push(entries.resolve(merge_operator).await?);
        }
        Ok(self
            .slots
            .iter()
            .map(|slot| values[*slot].clone())
            .collect())
    }
}

/// The entries of one key, newest first, and whether a base answered it.
#[derive(Default)]
struct KeyEntries {
    entries: Vec<RowEntry>,
    answered: bool,
}

impl KeyEntries {
    /// Keeps the entry as `GetIterator` does: a value or a tombstone answers the key.
    fn push(&mut self, entry: RowEntry) {
        if self.answered {
            return;
        }
        match entry.value {
            ValueDeletable::Value(_) => {
                self.entries.push(entry);
                self.answered = true;
            }
            ValueDeletable::Tombstone => self.answered = true,
            ValueDeletable::Merge(_) => self.entries.push(entry),
        }
    }

    /// Same as [`Self::push`], but drops an entry above the sequence bound.
    fn push_visible(&mut self, max_seq: Option<u64>, entry: RowEntry) {
        if max_seq.is_some_and(|max_seq| entry.seq > max_seq) {
            return;
        }
        self.push(entry);
    }

    /// Folds the entries as `DbIterator` does for one key.
    async fn resolve(
        self,
        merge_operator: Option<&MergeOperatorType>,
    ) -> Result<Option<KeyValue>, SlateDBError> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        let entries = Entries(self.entries.into());
        let entry = match merge_operator {
            Some(merge_operator) => {
                MergeOperatorIterator::new(merge_operator.clone(), entries, true, None)
                    .next()
                    .await?
            }
            None => MergeOperatorRequiredIterator::new(entries).next().await?,
        };
        Ok(entry.map(KeyValue::from))
    }
}

/// A `RowEntryIterator` over the collected entries of one key.
struct Entries(VecDeque<RowEntry>);

#[async_trait]
impl RowEntryIterator for Entries {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.0.pop_front())
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rstest::rstest;

    use super::*;
    use crate::test_utils::StringConcatMergeOperator;

    fn layer_entries(index: usize, entries: Vec<RowEntry>) -> LayerEntries {
        let mut layer_entries = LayerEntries::default();
        layer_entries.push(index, entries);
        layer_entries
    }

    #[test]
    fn test_batch_sorts_and_dedups_keys() {
        let batch = Batch::new(&[b"b".as_ref(), b"a", b"b", b"c", b"a"], None);

        assert_eq!(
            batch.keys(),
            [Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
        assert_eq!(batch.slots, vec![1, 0, 1, 2, 0]);
    }

    #[tokio::test]
    async fn test_batch_copies_a_result_into_every_slot() {
        let mut batch = Batch::new(&[b"b".as_ref(), b"a", b"b"], None);
        batch.apply(layer_entries(0, vec![RowEntry::new_value(b"a", b"1", 1)]));
        batch.apply(layer_entries(1, vec![RowEntry::new_value(b"b", b"2", 2)]));

        let values: Vec<Option<Bytes>> = batch
            .resolve(None)
            .await
            .unwrap()
            .into_iter()
            .map(|kv| kv.map(|kv| kv.value))
            .collect();
        assert_eq!(
            values,
            vec![
                Some(Bytes::from("2")),
                Some(Bytes::from("1")),
                Some(Bytes::from("2"))
            ]
        );
    }

    #[rstest]
    #[case::value(vec![RowEntry::new_value(b"k", b"v", 3)], true, Some(b"v".as_ref()))]
    #[case::tombstone(vec![RowEntry::new_tombstone(b"k", 3)], true, None)]
    #[case::merge_then_value(
        vec![RowEntry::new_merge(b"k", b"m", 4), RowEntry::new_value(b"k", b"v", 3)],
        true,
        Some(b"vm".as_ref()),
    )]
    #[case::merge_then_tombstone(
        vec![RowEntry::new_merge(b"k", b"m", 4), RowEntry::new_tombstone(b"k", 3)],
        true,
        Some(b"m".as_ref()),
    )]
    #[case::merge_only(vec![RowEntry::new_merge(b"k", b"m", 4)], true, Some(b"m".as_ref()))]
    #[case::value_hides_older(
        vec![RowEntry::new_value(b"k", b"new", 4), RowEntry::new_value(b"k", b"old", 3)],
        false,
        Some(b"new".as_ref()),
    )]
    #[case::tombstone_hides_older(
        vec![RowEntry::new_tombstone(b"k", 4), RowEntry::new_value(b"k", b"old", 3)],
        false,
        None,
    )]
    #[case::empty(vec![], false, None)]
    #[tokio::test]
    async fn test_key_entries_follow_get(
        #[case] entries: Vec<RowEntry>,
        #[case] merge_operator: bool,
        #[case] expected: Option<&[u8]>,
    ) {
        let merge_operator =
            merge_operator.then(|| Arc::new(StringConcatMergeOperator) as MergeOperatorType);
        let mut key_entries = KeyEntries::default();
        for entry in entries {
            key_entries.push(entry);
        }

        let value = key_entries.resolve(merge_operator.as_ref()).await.unwrap();

        assert_eq!(
            value.map(|kv| kv.value),
            expected.map(Bytes::copy_from_slice)
        );
    }

    #[tokio::test]
    async fn test_merge_without_operator_fails() {
        let mut key_entries = KeyEntries::default();
        key_entries.push(RowEntry::new_merge(b"k", b"m", 4));

        let error = key_entries.resolve(None).await.unwrap_err();

        assert!(matches!(error, SlateDBError::MergeOperatorMissing));
    }

    #[test]
    fn test_push_visible_drops_an_entry_above_the_bound() {
        let mut key_entries = KeyEntries::default();
        key_entries.push_visible(Some(3), RowEntry::new_value(b"k", b"new", 4));
        key_entries.push_visible(Some(3), RowEntry::new_value(b"k", b"old", 3));

        assert!(key_entries.answered);
        assert_eq!(
            key_entries.entries,
            vec![RowEntry::new_value(b"k", b"old", 3)]
        );
    }

    #[test]
    fn test_open_keys_lists_unanswered_keys_of_one_segment() {
        let mut batch = Batch::new(&[b"a".as_ref(), b"b", b"c"], None);
        batch.set_segment(0, 0);
        batch.set_segment(1, 1);
        batch.set_segment(2, 0);
        batch.apply(layer_entries(0, vec![RowEntry::new_tombstone(b"a", 1)]));

        assert_eq!(
            batch.open_keys(0),
            vec![OpenKey {
                index: 2,
                key: Bytes::from("c")
            }]
        );
        assert_eq!(
            batch.open_keys(1),
            vec![OpenKey {
                index: 1,
                key: Bytes::from("b")
            }]
        );
        assert!(!batch.is_done());
    }
}
