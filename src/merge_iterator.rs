use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;

pub(crate) struct TwoMergeIterator<T1: KeyValueIterator, T2: KeyValueIterator> {
    iterator1: (T1, Option<RowEntry>),
    iterator2: (T2, Option<RowEntry>),
}

impl<T1: KeyValueIterator, T2: KeyValueIterator> TwoMergeIterator<T1, T2> {
    pub(crate) async fn new(mut iterator1: T1, mut iterator2: T2) -> Result<Self, SlateDBError> {
        let next1 = iterator1.next_entry().await?;
        let next2 = iterator2.next_entry().await?;
        Ok(Self {
            iterator1: (iterator1, next1),
            iterator2: (iterator2, next2),
        })
    }

    async fn advance1(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.iterator1.1.is_none() {
            return Ok(None);
        }
        Ok(std::mem::replace(
            &mut self.iterator1.1,
            self.iterator1.0.next_entry().await?,
        ))
    }

    async fn advance2(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.iterator2.1.is_none() {
            return Ok(None);
        }
        Ok(std::mem::replace(
            &mut self.iterator2.1,
            self.iterator2.0.next_entry().await?,
        ))
    }
}

impl<T1: KeyValueIterator, T2: KeyValueIterator> KeyValueIterator for TwoMergeIterator<T1, T2> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(next1) = self.iterator1.1.as_ref() {
            if let Some(next2) = self.iterator2.1.as_ref() {
                if next1.key <= next2.key {
                    if next1.key == next2.key {
                        self.advance2().await?;
                    }
                    return self.advance1().await;
                }
                return self.advance2().await;
            }
            return self.advance1().await;
        }
        self.advance2().await
    }
}

struct MergeIteratorHeapEntry<T: KeyValueIterator> {
    next_kv: RowEntry,
    index: u32,
    iterator: T,
}

impl<T: KeyValueIterator> Eq for MergeIteratorHeapEntry<T> {}

impl<T: KeyValueIterator> PartialEq<Self> for MergeIteratorHeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.next_kv == other.next_kv
    }
}

impl<T: KeyValueIterator> PartialOrd<Self> for MergeIteratorHeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: KeyValueIterator> Ord for MergeIteratorHeapEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.next_kv.key.cmp(&other.next_kv.key) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self.index.cmp(&other.index),
        }
    }
}

pub(crate) struct MergeIterator<T: KeyValueIterator> {
    current: Option<MergeIteratorHeapEntry<T>>,
    iterators: BinaryHeap<Reverse<MergeIteratorHeapEntry<T>>>,
}

impl<T: KeyValueIterator> MergeIterator<T> {
    pub(crate) async fn new(mut iterators: VecDeque<T>) -> Result<Self, SlateDBError> {
        let mut heap = BinaryHeap::new();
        let mut index = 0;
        while let Some(mut iterator) = iterators.pop_front() {
            if let Some(kv) = iterator.next_entry().await? {
                heap.push(Reverse(MergeIteratorHeapEntry {
                    next_kv: kv,
                    index,
                    iterator,
                }));
            }
            index += 1;
        }
        Ok(Self {
            current: heap.pop().map(|r| r.0),
            iterators: heap,
        })
    }

    async fn advance(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(mut iterator_state) = self.current.take() {
            let current_kv = iterator_state.next_kv;
            if let Some(kv) = iterator_state.iterator.next_entry().await? {
                iterator_state.next_kv = kv;
                self.iterators.push(Reverse(iterator_state));
            }
            self.current = self.iterators.pop().map(|r| r.0);
            return Ok(Some(current_kv));
        }
        Ok(None)
    }
}

impl<T: KeyValueIterator> KeyValueIterator for MergeIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(kv) = self.advance().await? {
            while let Some(next_entry) = self.current.as_ref() {
                if next_entry.next_kv.key != kv.key {
                    break;
                }
                self.advance().await?;
            }
            return Ok(Some(kv));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::SlateDBError;
    use crate::iter::KeyValueIterator;
    use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
    use crate::test_utils::{assert_iterator, gen_attrs};
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn test_merge_iterator_should_include_entries_in_order() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aaaa", b"1111")
                .with_entry(b"cccc", b"3333")
                .with_entry(b"zzzz", b"26262626"),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"bbbb", b"2222")
                .with_entry(b"xxxx", b"24242424")
                .with_entry(b"yyyy", b"25252525"),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"dddd", b"4444")
                .with_entry(b"eeee", b"5555")
                .with_entry(b"gggg", b"7777"),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            &[
                (
                    "aaaa".into(),
                    ValueDeletable::Value(Bytes::from("1111")),
                    gen_attrs(0),
                ),
                (
                    "bbbb".into(),
                    ValueDeletable::Value(Bytes::from("2222")),
                    gen_attrs(3),
                ),
                (
                    "cccc".into(),
                    ValueDeletable::Value(Bytes::from("3333")),
                    gen_attrs(1),
                ),
                (
                    "dddd".into(),
                    ValueDeletable::Value(Bytes::from("4444")),
                    gen_attrs(6),
                ),
                (
                    "eeee".into(),
                    ValueDeletable::Value(Bytes::from("5555")),
                    gen_attrs(7),
                ),
                (
                    "gggg".into(),
                    ValueDeletable::Value(Bytes::from("7777")),
                    gen_attrs(8),
                ),
                (
                    "xxxx".into(),
                    ValueDeletable::Value(Bytes::from("24242424")),
                    gen_attrs(4),
                ),
                (
                    "yyyy".into(),
                    ValueDeletable::Value(Bytes::from("25252525")),
                    gen_attrs(5),
                ),
                (
                    "zzzz".into(),
                    ValueDeletable::Value(Bytes::from("26262626")),
                    gen_attrs(2),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_iterator_should_write_one_entry_with_given_key() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aaaa", b"1111")
                .with_entry(b"cccc", b"use this one c"),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"cccc", b"badc1")
                .with_entry(b"xxxx", b"use this one x"),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"bbbb", b"2222")
                .with_entry(b"cccc", b"badc2")
                .with_entry(b"xxxx", b"badx1"),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            &[
                (
                    "aaaa".into(),
                    ValueDeletable::Value(Bytes::from("1111")),
                    gen_attrs(0),
                ),
                (
                    "bbbb".into(),
                    ValueDeletable::Value(Bytes::from("2222")),
                    gen_attrs(4),
                ),
                (
                    "cccc".into(),
                    ValueDeletable::Value(Bytes::from("use this one c")),
                    gen_attrs(1),
                ),
                (
                    "xxxx".into(),
                    ValueDeletable::Value(Bytes::from("use this one x")),
                    gen_attrs(3),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_iterator_should_include_entries_in_order() {
        let iter1 = TestIterator::new()
            .with_entry(b"aaaa", b"1111")
            .with_entry(b"cccc", b"3333")
            .with_entry(b"zzzz", b"26262626");
        let iter2 = TestIterator::new()
            .with_entry(b"bbbb", b"2222")
            .with_entry(b"xxxx", b"24242424")
            .with_entry(b"yyyy", b"25252525");

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            &[
                (
                    "aaaa".into(),
                    ValueDeletable::Value(Bytes::from("1111")),
                    gen_attrs(0),
                ),
                (
                    "bbbb".into(),
                    ValueDeletable::Value(Bytes::from("2222")),
                    gen_attrs(3),
                ),
                (
                    "cccc".into(),
                    ValueDeletable::Value(Bytes::from("3333")),
                    gen_attrs(1),
                ),
                (
                    "xxxx".into(),
                    ValueDeletable::Value(Bytes::from("24242424")),
                    gen_attrs(4),
                ),
                (
                    "yyyy".into(),
                    ValueDeletable::Value(Bytes::from("25252525")),
                    gen_attrs(5),
                ),
                (
                    "zzzz".into(),
                    ValueDeletable::Value(Bytes::from("26262626")),
                    gen_attrs(2),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_iterator_should_write_one_entry_with_given_key() {
        let iter1 = TestIterator::new()
            .with_entry(b"aaaa", b"1111")
            .with_entry(b"cccc", b"use this one c");
        let iter2 = TestIterator::new()
            .with_entry(b"cccc", b"badc")
            .with_entry(b"xxxx", b"24242424");

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            &[
                (
                    "aaaa".into(),
                    ValueDeletable::Value(Bytes::from("1111")),
                    gen_attrs(0),
                ),
                (
                    "cccc".into(),
                    ValueDeletable::Value(Bytes::from("use this one c")),
                    gen_attrs(1),
                ),
                (
                    "xxxx".into(),
                    ValueDeletable::Value(Bytes::from("24242424")),
                    gen_attrs(3),
                ),
            ],
        )
        .await;
    }

    struct TestIterator {
        entries: VecDeque<Result<RowEntry, SlateDBError>>,
    }

    impl TestIterator {
        fn new() -> Self {
            Self {
                entries: VecDeque::new(),
            }
        }

        fn with_entry(mut self, key: &'static [u8], val: &'static [u8]) -> Self {
            let entry = RowEntry::new_value(key, val, 0);
            self.entries.push_back(Ok(entry));
            self
        }
    }

    impl KeyValueIterator for TestIterator {
        async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
            self.entries.pop_front().map_or(Ok(None), |e| match e {
                Ok(kv) => Ok(Some(kv)),
                Err(err) => Err(err),
            })
        }
    }
}
