use async_trait::async_trait;

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

struct MergeIteratorHeapEntry<'a> {
    next_kv: RowEntry,
    index: usize,
    iterator: Box<dyn KeyValueIterator + 'a>,
}

impl<'a> MergeIteratorHeapEntry<'a> {
    /// Seek the iterator and return a new heap entry
    async fn seek(
        mut self,
        next_key: &[u8],
    ) -> Result<Option<MergeIteratorHeapEntry<'a>>, SlateDBError> {
        if self.next_kv.key >= next_key {
            Ok(Some(self))
        } else {
            self.iterator.seek(next_key).await?;
            if let Some(next_kv) = self.iterator.next_entry().await? {
                Ok(Some(MergeIteratorHeapEntry {
                    next_kv,
                    index: self.index,
                    iterator: self.iterator,
                }))
            } else {
                Ok(None)
            }
        }
    }
}

impl Eq for MergeIteratorHeapEntry<'_> {}

impl PartialEq<Self> for MergeIteratorHeapEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.next_kv == other.next_kv
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
        (&self.next_kv.key, self.next_kv.seq).cmp(&(&other.next_kv.key, other.next_kv.seq))
    }
}

pub(crate) struct MergeIterator<'a> {
    current: Option<MergeIteratorHeapEntry<'a>>,
    iterators: BinaryHeap<Reverse<MergeIteratorHeapEntry<'a>>>,
}

impl<'a> MergeIterator<'a> {
    pub(crate) async fn new<T: KeyValueIterator + 'a>(
        iterators: impl IntoIterator<Item = T>,
    ) -> Result<Self, SlateDBError> {
        let mut heap = BinaryHeap::new();
        for (index, mut iterator) in iterators.into_iter().enumerate() {
            if let Some(kv) = iterator.next_entry().await? {
                heap.push(Reverse(MergeIteratorHeapEntry {
                    next_kv: kv,
                    index,
                    iterator: Box::new(iterator),
                }));
            }
        }
        Ok(Self {
            current: heap.pop().map(|r| r.0),
            iterators: heap,
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
                self.iterators.push(Reverse(iterator_state));
            }
            self.current = self.iterators.pop().map(|r| r.0);
            return Ok(Some(current_kv));
        }
        Ok(None)
    }
}

#[async_trait]
impl KeyValueIterator for MergeIterator<'_> {
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

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        let mut seek_futures = VecDeque::new();
        if let Some(iterator) = self.current.take() {
            seek_futures.push_back(iterator.seek(next_key))
        }

        for iterator in self.iterators.drain() {
            seek_futures.push_back(iterator.0.seek(next_key));
        }

        for seek_result in futures::future::join_all(seek_futures).await {
            if let Some(sought_iterator) = seek_result? {
                self.iterators.push(Reverse(sought_iterator));
            }
        }

        self.current = self.iterators.pop().map(|r| r.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::iter::KeyValueIterator;
    use crate::merge_iterator::MergeIterator;
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

        let mut merge_iter = MergeIterator::new([iter1, iter2]).await.unwrap();

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

        let mut merge_iter = MergeIterator::new([iter1, iter2]).await.unwrap();

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

        let mut merge_iter = MergeIterator::new([iter1, iter2]).await.unwrap();
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
