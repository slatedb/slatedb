use async_trait::async_trait;

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

pub(crate) struct TwoMergeIterator<T1: KeyValueIterator, T2: KeyValueIterator> {
    iterator1: T1,
    iterator2: T2,
}

impl<T1: KeyValueIterator, T2: KeyValueIterator> TwoMergeIterator<T1, T2> {
    pub(crate) async fn new(iterator1: T1, iterator2: T2) -> Result<Self, SlateDBError> {
        Ok(Self {
            iterator1,
            iterator2,
        })
    }

    async fn advance1(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.iterator1.peek().is_none() {
            return Ok(None);
        }
        self.iterator1.take_and_next_entry().await
    }

    async fn advance2(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.iterator2.peek().is_none() {
            return Ok(None);
        }
        self.iterator2.take_and_next_entry().await
    }

    fn peek1(&self) -> Option<&RowEntry> {
        self.iterator1.peek()
    }

    fn peek2(&self) -> Option<&RowEntry> {
        self.iterator2.peek()
    }

    fn peek_inner(&self) -> Option<&RowEntry> {
        match (self.peek1(), self.peek2()) {
            (None, None) => None,
            (Some(v1), None) => Some(v1),
            (None, Some(v2)) => Some(v2),
            (Some(v1), Some(v2)) => {
                if v1.key < v2.key {
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
                if next1.key < next2.key {
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
    T1: KeyValueIterator,
    T2: KeyValueIterator,
{
    async fn seek1(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &self.iterator1.peek() {
            None => Ok(()),
            Some(val) => {
                if val.key < next_key {
                    self.iterator1.seek(next_key).await?;
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn seek2(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &self.iterator2.peek() {
            None => Ok(()),
            Some(val) => {
                if val.key < next_key {
                    self.iterator2.seek(next_key).await?;
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[async_trait]
impl<T1: KeyValueIterator, T2: KeyValueIterator> KeyValueIterator for TwoMergeIterator<T1, T2> {
    async fn take_and_next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
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

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.seek1(next_key).await?;
        self.seek2(next_key).await
    }

    fn peek(&self) -> Option<&RowEntry> {
        self.peek_inner()
    }
}

struct MergeIteratorHeapEntry<'a> {
    index: u32,
    iterator: Box<dyn KeyValueIterator + 'a>,
}

impl<'a> MergeIteratorHeapEntry<'a> {
    /// Seek the iterator and return a new heap entry
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.iterator.seek(next_key).await?;
        Ok(())
    }
}

impl Eq for MergeIteratorHeapEntry<'_> {}

impl PartialEq<Self> for MergeIteratorHeapEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl PartialOrd<Self> for MergeIteratorHeapEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeIteratorHeapEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we'll wrap a Reverse in the BinaryHeap, so the cmp here is in increasing order.
        // after Reverse is wrapped, it will return the entries with higher seqnum first.
        (&self.iterator.peek().unwrap().key, self.index)
            .cmp(&(&other.iterator.peek().unwrap().key, other.index))
    }
}

pub(crate) struct MergeIterator<'a> {
    current: Option<MergeIteratorHeapEntry<'a>>,
    iterators: BinaryHeap<Reverse<MergeIteratorHeapEntry<'a>>>,
}

impl<'a> MergeIterator<'a> {
    pub(crate) async fn new<T: KeyValueIterator + 'a>(
        mut iterators: VecDeque<T>,
    ) -> Result<Self, SlateDBError> {
        let mut heap = BinaryHeap::new();
        let mut index = 0;
        while let Some(iterator) = iterators.pop_front() {
            if iterator.peek().is_some() {
                heap.push(Reverse(MergeIteratorHeapEntry { index, iterator: Box::new(iterator) }));
            }
            index += 1;
        }
        Ok(Self {
            current: heap.pop().map(|r| r.0),
            iterators: heap,
        })
    }

    fn peek(&self) -> Option<&RowEntry> {
        if let Some(current) = &self.current {
            current.iterator.peek()
        } else {
            None
        }
    }

    async fn take_and_advance(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(mut iterator_state) = self.current.take() {
            let current_kv = iterator_state.iterator.take_and_next_entry().await?;
            if iterator_state.iterator.peek().is_some() {
                self.iterators.push(Reverse(iterator_state));
            }
            self.current = self.iterators.pop().map(|r| r.0);
            return Ok(current_kv);
        }
        Ok(None)
    }
}

#[async_trait]
impl KeyValueIterator for MergeIterator<'_>{
    async fn take_and_next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let mut current_kv = match self.take_and_advance().await? {
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
            self.take_and_advance().await?;
        }
        Ok(Some(current_kv))
    }

    fn peek(&self) -> Option<&RowEntry> {
        self.peek()
    }

    /// need suggestion for this impl（I'm not good at use rust）(delet this doc when change after suggestion)
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        while let Some(entry) = self.peek() {
            if entry.key < next_key {
                let mut heap_entry = self
                    .current
                    .take()
                    .expect("current heap_entry is must not None");
                heap_entry.seek(next_key).await?;
                if heap_entry.iterator.peek().is_some() {
                    self.iterators.push(Reverse(heap_entry));
                }
                if self.iterators.is_empty() {
                    self.current = None;
                } else {
                    self.current = self.iterators.pop().map(|r| r.0);
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::iter::KeyValueIterator;
    use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
    use crate::test_utils::{assert_iterator, assert_next_entry, TestIterator};
    use crate::types::RowEntry;
    use std::collections::VecDeque;
    use std::vec;

    #[tokio::test]
    async fn test_merge_iterator_should_include_entries_in_order() {
        let mut iters: VecDeque<TestIterator> = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aaaa", b"1111", 0)
                .with_entry(b"cccc", b"3333", 0)
                .with_entry(b"zzzz", b"26262626", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"bbbb", b"2222", 0)
                .with_entry(b"xxxx", b"24242424", 0)
                .with_entry(b"yyyy", b"25252525", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"dddd", b"4444", 0)
                .with_entry(b"eeee", b"5555", 0)
                .with_entry(b"gggg", b"7777", 0),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();

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
        let mut iters: VecDeque<TestIterator> = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aaaa", b"0000", 5)
                .with_entry(b"aaaa", b"1111", 6)
                .with_entry(b"cccc", b"use this one c", 5),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"cccc", b"badc1", 1)
                .with_entry(b"xxxx", b"use this one x", 4),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"bbbb", b"2222", 3)
                .with_entry(b"cccc", b"badc2", 3)
                .with_entry(b"xxxx", b"badx1", 3),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();

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
        let iter1 = TestIterator::new()
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"cccc", b"3333", 0)
            .with_entry(b"zzzz", b"26262626", 0);
        let iter2 = TestIterator::new()
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
        let iter1 = TestIterator::new()
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"cccc", b"use this one c", 5);
        let iter2 = TestIterator::new()
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
        let mut iters: VecDeque<TestIterator> = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa1", 0)
                .with_entry(b"bb", b"bb1", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa2", 0)
                .with_entry(b"bb", b"bb2", 0)
                .with_entry(b"cc", b"cc2", 0),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();
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
        let mut iters: VecDeque<TestIterator> = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa1", 0)
                .with_entry(b"bb", b"bb1", 0),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa2", 0)
                .with_entry(b"bb", b"bb2", 0)
                .with_entry(b"cc", b"cc2", 0),
        );

        let mut merge_iter = MergeIterator::new(iters).await.unwrap();
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
        let iter1 = TestIterator::new()
            .with_entry(b"aa", b"aa1", 1)
            .with_entry(b"bb", b"bb0", 1)
            .with_entry(b"bb", b"bb1", 2)
            .with_entry(b"dd", b"dd1", 3);
        let iter2 = TestIterator::new()
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
}
