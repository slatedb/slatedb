use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::KeyValueDeletable;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

pub(crate) struct TwoMergeIterator<T1: KeyValueIterator, T2: KeyValueIterator> {
    iterator1: (T1, Option<KeyValueDeletable>),
    iterator2: (T2, Option<KeyValueDeletable>),
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

    async fn advance1(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
        if self.iterator1.1.is_none() {
            return Ok(None);
        }
        Ok(std::mem::replace(
            &mut self.iterator1.1,
            self.iterator1.0.next_entry().await?,
        ))
    }

    async fn advance2(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
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
    async fn next_entry(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
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
    next_kv: KeyValueDeletable,
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

    async fn advance(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
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
    async fn next_entry(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
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
    use crate::types::{KeyValueDeletable, ValueDeletable};
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
                (b"aaaa", ValueDeletable::Value(Bytes::from("1111"))),
                (b"bbbb", ValueDeletable::Value(Bytes::from("2222"))),
                (b"cccc", ValueDeletable::Value(Bytes::from("3333"))),
                (b"dddd", ValueDeletable::Value(Bytes::from("4444"))),
                (b"eeee", ValueDeletable::Value(Bytes::from("5555"))),
                (b"gggg", ValueDeletable::Value(Bytes::from("7777"))),
                (b"xxxx", ValueDeletable::Value(Bytes::from("24242424"))),
                (b"yyyy", ValueDeletable::Value(Bytes::from("25252525"))),
                (b"zzzz", ValueDeletable::Value(Bytes::from("26262626"))),
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
                (b"aaaa", ValueDeletable::Value(Bytes::from("1111"))),
                (b"bbbb", ValueDeletable::Value(Bytes::from("2222"))),
                (
                    b"cccc",
                    ValueDeletable::Value(Bytes::from("use this one c")),
                ),
                (
                    b"xxxx",
                    ValueDeletable::Value(Bytes::from("use this one x")),
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
                (b"aaaa", ValueDeletable::Value(Bytes::from("1111"))),
                (b"bbbb", ValueDeletable::Value(Bytes::from("2222"))),
                (b"cccc", ValueDeletable::Value(Bytes::from("3333"))),
                (b"xxxx", ValueDeletable::Value(Bytes::from("24242424"))),
                (b"yyyy", ValueDeletable::Value(Bytes::from("25252525"))),
                (b"zzzz", ValueDeletable::Value(Bytes::from("26262626"))),
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
                (b"aaaa", ValueDeletable::Value(Bytes::from("1111"))),
                (
                    b"cccc",
                    ValueDeletable::Value(Bytes::from("use this one c")),
                ),
                (b"xxxx", ValueDeletable::Value(Bytes::from("24242424"))),
            ],
        )
        .await;
    }

    struct TestIterator {
        entries: VecDeque<Result<KeyValueDeletable, SlateDBError>>,
    }

    impl TestIterator {
        fn new() -> Self {
            Self {
                entries: VecDeque::new(),
            }
        }

        fn with_entry(mut self, key: &'static [u8], val: &'static [u8]) -> Self {
            self.entries.push_back(Ok(KeyValueDeletable {
                key: Bytes::from(key),
                value: ValueDeletable::Value(Bytes::from(val)),
            }));
            self
        }
    }

    impl KeyValueIterator for TestIterator {
        async fn next_entry(&mut self) -> Result<Option<KeyValueDeletable>, SlateDBError> {
            self.entries.pop_front().map_or(Ok(None), |e| match e {
                Ok(kv) => Ok(Some(kv)),
                Err(err) => Err(err),
            })
        }
    }

    async fn assert_iterator<T: KeyValueIterator>(
        iterator: &mut T,
        entries: &[(&'static [u8], ValueDeletable)],
    ) {
        for (expected_k, expected_v) in entries.iter() {
            if let Some(kv) = iterator.next_entry().await.unwrap() {
                assert_eq!(kv.key, Bytes::from(*expected_k));
                assert_eq!(kv.value, *expected_v);
            } else {
                panic!("expected next_entry to return a value")
            }
        }
        assert!(iterator.next_entry().await.unwrap().is_none());
    }
}
