use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator, SeekToKey};
use crate::merge_iterator::MergeHeap::{MaxHeap, MinHeap};
use crate::types::RowEntry;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

pub(crate) struct TwoMergeIterator<T1: KeyValueIterator, T2: KeyValueIterator> {
    iterator1: (T1, Option<RowEntry>),
    iterator2: (T2, Option<RowEntry>),
}

impl<T1: KeyValueIterator, T2: KeyValueIterator> TwoMergeIterator<T1, T2> {
    pub(crate) async fn new(mut iterator1: T1, mut iterator2: T2) -> Result<Self, SlateDBError> {
        if iterator1.order() != iterator2.order() {
            return Err(SlateDBError::InvalidArgument {
                msg: "Cannot merge tow iterators with inconsistent iteration order".to_string(),
            });
        }

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

    fn peek1(&self) -> Option<&RowEntry> {
        self.iterator1.1.as_ref()
    }

    fn peek2(&self) -> Option<&RowEntry> {
        self.iterator2.1.as_ref()
    }

    fn peek_inner(&self) -> Option<&RowEntry> {
        match (self.peek1(), self.peek2()) {
            (None, None) => None,
            (Some(v1), None) => Some(v1),
            (None, Some(v2)) => Some(v2),
            (Some(v1), Some(v2)) => {
                if self.order().precedes(&v1.key, &v2.key) {
                    Some(v1)
                } else {
                    Some(v2)
                }
            }
        }
    }

    async fn advance_inner(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match (self.peek1(), self.peek2()) {
            (None, None) => Ok(None),
            (Some(_), None) => self.advance1().await,
            (None, Some(_)) => self.advance2().await,
            (Some(next1), Some(next2)) => {
                if self.order().precedes(&next1.key, &next2.key) {
                    self.advance1().await
                } else {
                    self.advance2().await
                }
            }
        }
    }
}

impl<T1, T2> TwoMergeIterator<T1, T2>
where
    T1: KeyValueIterator + SeekToKey,
    T2: KeyValueIterator + SeekToKey,
{
    async fn seek1(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if let Some(val) = &self.iterator1.1 {
            if self.order().precedes(val.key.as_ref(), next_key) {
                self.iterator1.0.seek(next_key).await?;
                self.iterator1.1 = self.iterator1.0.next_entry().await?;
            }
        }
        Ok(())
    }

    async fn seek2(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if let Some(val) = &self.iterator2.1 {
            if self.order().precedes(val.key.as_ref(), next_key) {
                self.iterator2.0.seek(next_key).await?;
                self.iterator2.1 = self.iterator2.0.next_entry().await?;
            }
        }
        Ok(())
    }
}

impl<T1, T2> SeekToKey for TwoMergeIterator<T1, T2>
where
    T1: KeyValueIterator + SeekToKey,
    T2: KeyValueIterator + SeekToKey,
{
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.seek1(next_key).await?;
        self.seek2(next_key).await
    }
}

impl<T1: KeyValueIterator, T2: KeyValueIterator> KeyValueIterator for TwoMergeIterator<T1, T2> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let mut current_kv = match self.advance_inner().await? {
            Some(kv) => kv,
            None => return Ok(None),
        };
        while let Some(peeked_kv) = self.peek_inner() {
            if peeked_kv.key != current_kv.key {
                break;
            }
            if peeked_kv.seq > current_kv.seq {
                current_kv = peeked_kv.clone();
            }
            self.advance_inner().await?;
        }
        Ok(Some(current_kv))
    }

    fn order(&self) -> IterationOrder {
        self.iterator1.0.order()
    }
}

struct MergeIteratorHeapEntry<T: KeyValueIterator> {
    next_kv: RowEntry,
    index: u32,
    iterator: T,
}

impl<T: KeyValueIterator + SeekToKey> MergeIteratorHeapEntry<T> {
    /// Seek the iterator and return a new heap entry
    async fn seek(
        mut self,
        seek_key: &[u8],
    ) -> Result<Option<MergeIteratorHeapEntry<T>>, SlateDBError> {
        if self
            .iterator
            .order()
            .precedes(self.next_kv.key.as_ref(), seek_key)
        {
            self.iterator.seek(seek_key).await?;
            if let Some(next_kv) = self.iterator.next_entry().await? {
                Ok(Some(MergeIteratorHeapEntry {
                    next_kv,
                    index: self.index,
                    iterator: self.iterator,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(Some(self))
        }
    }
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
        // we'll wrap a Reverse in the BinaryHeap, so the cmp here is in increasing order.
        // after Reverse is wrapped, it will return the entries with higher seqnum first.
        (&self.next_kv.key, self.next_kv.seq).cmp(&(&other.next_kv.key, other.next_kv.seq))
    }
}

enum MergeHeap<T: KeyValueIterator> {
    MinHeap(BinaryHeap<Reverse<MergeIteratorHeapEntry<T>>>),
    MaxHeap(BinaryHeap<MergeIteratorHeapEntry<T>>),
}

impl<T: KeyValueIterator> MergeHeap<T> {
    fn new(order: IterationOrder) -> Self {
        match order {
            IterationOrder::Ascending => MinHeap(BinaryHeap::new()),
            IterationOrder::Descending => MaxHeap(BinaryHeap::new()),
        }
    }

    fn push(&mut self, entry: MergeIteratorHeapEntry<T>) {
        match self {
            MinHeap(heap) => heap.push(Reverse(entry)),
            MaxHeap(heap) => heap.push(entry),
        }
    }

    fn pop(&mut self) -> Option<MergeIteratorHeapEntry<T>> {
        match self {
            MinHeap(heap) => heap.pop().map(|Reverse(entry)| entry),
            MaxHeap(heap) => heap.pop(),
        }
    }

    fn drain(&mut self) -> Vec<MergeIteratorHeapEntry<T>> {
        match self {
            MinHeap(heap) => heap.drain().map(|Reverse(entry)| entry).collect(),
            MaxHeap(heap) => heap.drain().collect(),
        }
    }
}

pub(crate) struct MergeIterator<T: KeyValueIterator> {
    current: Option<MergeIteratorHeapEntry<T>>,
    iterators: MergeHeap<T>,
    order: IterationOrder,
}

impl<T: KeyValueIterator> MergeIterator<T> {
    pub(crate) async fn new(
        mut iterators: VecDeque<T>,
        order: IterationOrder,
    ) -> Result<Self, SlateDBError> {
        let mut heap = MergeHeap::new(order);
        let mut index = 0;
        while let Some(mut iterator) = iterators.pop_front() {
            if order != iterator.order() {
                return Err(SlateDBError::InvalidArgument {
                    msg: "Inconsistent ordering found among iterators".to_string(),
                });
            }

            if let Some(kv) = iterator.next_entry().await? {
                heap.push(MergeIteratorHeapEntry {
                    next_kv: kv,
                    index,
                    iterator,
                });
            }
            index += 1;
        }
        Ok(Self {
            current: heap.pop(),
            iterators: heap,
            order,
        })
    }

    fn peek(&self) -> Option<&RowEntry> {
        self.current.as_ref().map(|c| &c.next_kv)
    }

    async fn advance(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(mut iterator_state) = self.current.take() {
            let current_kv = iterator_state.next_kv;
            if let Some(kv) = iterator_state.iterator.next_entry().await? {
                iterator_state.next_kv = kv;
                self.iterators.push(iterator_state);
            }
            self.current = self.iterators.pop();
            return Ok(Some(current_kv));
        }
        Ok(None)
    }
}

impl<T: KeyValueIterator> KeyValueIterator for MergeIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let mut current_kv = match self.advance().await? {
            Some(kv) => kv,
            None => return Ok(None),
        };

        // iterate until we find a key that is not the same as the current key,
        // find the one with the highest seqnum.
        while let Some(peeked_entry) = self.peek() {
            if peeked_entry.key != current_kv.key {
                break;
            }
            if peeked_entry.seq > current_kv.seq {
                current_kv = peeked_entry.clone();
            }
            self.advance().await?;
        }
        Ok(Some(current_kv))
    }

    fn order(&self) -> IterationOrder {
        self.order
    }
}

impl<T: KeyValueIterator + SeekToKey> SeekToKey for MergeIterator<T> {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        let mut seek_futures = VecDeque::new();
        if let Some(iterator) = self.current.take() {
            seek_futures.push_back(iterator.seek(next_key))
        }

        for iterator in self.iterators.drain() {
            seek_futures.push_back(iterator.seek(next_key));
        }

        for seek_result in futures::future::join_all(seek_futures).await {
            if let Some(seeked_iterator) = seek_result? {
                self.iterators.push(seeked_iterator);
            }
        }

        self.current = self.iterators.pop();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::iter::IterationOrder::{Ascending, Descending};
    use crate::iter::SeekToKey;
    use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
    use crate::test_utils::{assert_iterator, assert_next_entry, TestIterator};
    use crate::types::RowEntry;
    use crate::SlateDBError;
    use std::collections::VecDeque;
    use std::vec;

    #[tokio::test]
    async fn test_merge_iterator_should_include_entries_in_order() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"aaaa", b"1111", 0)
                .with_entry(b"cccc", b"3333", 0)
                .with_entry(b"zzzz", b"26262626", 0),
        );
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"bbbb", b"2222", 0)
                .with_entry(b"xxxx", b"24242424", 0)
                .with_entry(b"yyyy", b"25252525", 0),
        );
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"dddd", b"4444", 0)
                .with_entry(b"eeee", b"5555", 0)
                .with_entry(b"gggg", b"7777", 0),
        );

        let mut merge_iter = MergeIterator::new(iters, Ascending).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"bbbb", b"2222", 0),
                RowEntry::new_value(b"cccc", b"3333", 0),
                RowEntry::new_value(b"dddd", b"4444", 0),
                RowEntry::new_value(b"eeee", b"5555", 0),
                RowEntry::new_value(b"gggg", b"7777", 0),
                RowEntry::new_value(b"xxxx", b"24242424", 0),
                RowEntry::new_value(b"yyyy", b"25252525", 0),
                RowEntry::new_value(b"zzzz", b"26262626", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_iterator_should_write_one_entry_with_given_key() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"aaaa", b"0000", 5)
                .with_entry(b"aaaa", b"1111", 6)
                .with_entry(b"cccc", b"use this one c", 5),
        );
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"cccc", b"badc1", 1)
                .with_entry(b"xxxx", b"use this one x", 4),
        );
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"bbbb", b"2222", 3)
                .with_entry(b"cccc", b"badc2", 3)
                .with_entry(b"xxxx", b"badx1", 3),
        );

        let mut merge_iter = MergeIterator::new(iters, Ascending).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 6),
                RowEntry::new_value(b"bbbb", b"2222", 3),
                RowEntry::new_value(b"cccc", b"use this one c", 5),
                RowEntry::new_value(b"xxxx", b"use this one x", 4),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_iterator_should_include_entries_in_order() {
        let iter1 = TestIterator::new(Ascending)
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"cccc", b"3333", 0)
            .with_entry(b"zzzz", b"26262626", 0);
        let iter2 = TestIterator::new(Ascending)
            .with_entry(b"bbbb", b"2222", 0)
            .with_entry(b"xxxx", b"24242424", 0)
            .with_entry(b"yyyy", b"25252525", 0);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"bbbb", b"2222", 0),
                RowEntry::new_value(b"cccc", b"3333", 0),
                RowEntry::new_value(b"xxxx", b"24242424", 0),
                RowEntry::new_value(b"yyyy", b"25252525", 0),
                RowEntry::new_value(b"zzzz", b"26262626", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_iterator_should_write_one_entry_with_given_key() {
        let iter1 = TestIterator::new(Ascending)
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"cccc", b"use this one c", 5);
        let iter2 = TestIterator::new(Ascending)
            .with_entry(b"cccc", b"badc1", 2)
            .with_entry(b"xxxx", b"24242424", 3);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"cccc", b"use this one c", 5),
                RowEntry::new_value(b"xxxx", b"24242424", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_merge_iter() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"aa", b"aa1", 0)
                .with_entry(b"bb", b"bb1", 0),
        );
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"aa", b"aa2", 0)
                .with_entry(b"bb", b"bb2", 0)
                .with_entry(b"cc", b"cc2", 0),
        );

        let mut merge_iter = MergeIterator::new(iters, Ascending).await.unwrap();
        merge_iter.seek(b"bb".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"bb", b"bb1", 0),
                RowEntry::new_value(b"cc", b"cc2", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_merge_iter_to_current_key() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"aa", b"aa1", 0)
                .with_entry(b"bb", b"bb1", 0),
        );
        iters.push_back(
            TestIterator::new(Ascending)
                .with_entry(b"aa", b"aa2", 0)
                .with_entry(b"bb", b"bb2", 0)
                .with_entry(b"cc", b"cc2", 0),
        );

        let mut merge_iter = MergeIterator::new(iters, Ascending).await.unwrap();
        assert_next_entry(&mut merge_iter, &RowEntry::new_value(b"aa", b"aa1", 0)).await;

        merge_iter.seek(b"bb".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"bb", b"bb1", 0),
                RowEntry::new_value(b"cc", b"cc2", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_merge_seek() {
        let iter1 = TestIterator::new(Ascending)
            .with_entry(b"aa", b"aa1", 1)
            .with_entry(b"bb", b"bb0", 1)
            .with_entry(b"bb", b"bb1", 2)
            .with_entry(b"dd", b"dd1", 3);
        let iter2 = TestIterator::new(Ascending)
            .with_entry(b"aa", b"aa2", 4)
            .with_entry(b"bb", b"bb2", 5)
            .with_entry(b"cc", b"cc0", 5)
            .with_entry(b"cc", b"cc2", 6)
            .with_entry(b"ee", b"ee2", 7);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();
        merge_iter.seek(b"b".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"bb", b"bb2", 5),
                RowEntry::new_value(b"cc", b"cc2", 6),
                RowEntry::new_value(b"dd", b"dd1", 3),
                RowEntry::new_value(b"ee", b"ee2", 7),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_merge_descending_iteration() {
        let iter1 = TestIterator::new(Descending)
            .with_entry(b"aa", b"aa1", 1)
            .with_entry(b"bb", b"bb0", 2)
            .with_entry(b"bb", b"bb1", 3)
            .with_entry(b"dd", b"dd1", 4);
        let iter2 = TestIterator::new(Descending)
            .with_entry(b"bb", b"bb2", 5)
            .with_entry(b"cc", b"cc0", 6)
            .with_entry(b"cc", b"cc2", 7)
            .with_entry(b"ee", b"ee2", 8);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();
        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"ee", b"ee2", 8),
                RowEntry::new_value(b"dd", b"dd1", 4),
                RowEntry::new_value(b"cc", b"cc2", 7),
                RowEntry::new_value(b"bb", b"bb2", 5),
                RowEntry::new_value(b"aa", b"aa1", 1),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_two_merge_descending_seek() {
        let iter1 = TestIterator::new(Descending)
            .with_entry(b"aa", b"aa1", 1)
            .with_entry(b"bb", b"bb0", 2)
            .with_entry(b"bb", b"bb1", 3)
            .with_entry(b"dd", b"dd1", 4);
        let iter2 = TestIterator::new(Descending)
            .with_entry(b"bb", b"bb2", 5)
            .with_entry(b"cc", b"cc0", 6)
            .with_entry(b"cc", b"cc2", 7)
            .with_entry(b"ee", b"ee2", 8);

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();
        merge_iter.seek(b"d".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"cc", b"cc2", 7),
                RowEntry::new_value(b"bb", b"bb2", 5),
                RowEntry::new_value(b"aa", b"aa1", 1),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn should_fail_merging_iterators_with_inconsistent_order() {
        let iter1 = TestIterator::new(Descending);
        let iter2 = TestIterator::new(Ascending);
        let Err(err) = TwoMergeIterator::new(iter1, iter2).await else {
            panic!("Expected failure merging iterators")
        };
        assert!(matches!(err, SlateDBError::InvalidArgument { msg: _ }));
    }
}
