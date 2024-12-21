use crate::error::SlateDBError;
use crate::iter::{KeyValueIterator, SeekToKey};
use crate::types::RowEntry;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

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

impl<T1, T2> TwoMergeIterator<T1, T2>
where
    T1: KeyValueIterator + SeekToKey,
    T2: KeyValueIterator + SeekToKey,
{
    async fn seek1(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &self.iterator1.1 {
            None => Ok(()),
            Some(val) => {
                if val.key < next_key {
                    self.iterator1.0.seek(next_key).await?;
                    self.iterator1.1 = self.iterator1.0.next_entry().await?;
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn seek2(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &self.iterator2.1 {
            None => Ok(()),
            Some(val) => {
                if val.key < next_key {
                    self.iterator2.0.seek(next_key).await?;
                    self.iterator2.1 = self.iterator2.0.next_entry().await?;
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
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
    // when we got two entries with the same key, we'd choose the one with the higher index
    index: u32,
    iterator: T,
}

impl<T: KeyValueIterator + SeekToKey> MergeIteratorHeapEntry<T> {
    /// Seek the iterator and return a new heap entry
    async fn seek(
        mut self,
        next_key: &[u8],
    ) -> Result<Option<MergeIteratorHeapEntry<T>>, SlateDBError> {
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
        // the row entry with higher index, should always has higher seqnum
        // if a key both exists in L0_1 and L0_2, the one in L0_2 should be
        // returned first, as L0_2 is created later than L0_1, and the seqnum
        // is higher.
        //
        // we'll wrap a Reverse in the BinaryHeap, so the cmp here is in
        // increasing order.
        (&self.next_kv.key, self.index, self.next_kv.seq).cmp(&(
            &other.next_kv.key,
            other.index,
            other.next_kv.seq,
        ))
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

impl<T: KeyValueIterator + SeekToKey> SeekToKey for MergeIterator<T> {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        let mut seek_futures = VecDeque::new();
        if let Some(iterator) = self.current.take() {
            seek_futures.push_back(iterator.seek(next_key))
        }

        for iterator in self.iterators.drain() {
            seek_futures.push_back(iterator.0.seek(next_key));
        }

        for seek_result in futures::future::join_all(seek_futures).await {
            if let Some(seeked_iterator) = seek_result? {
                self.iterators.push(Reverse(seeked_iterator));
            }
        }

        self.current = self.iterators.pop().map(|r| r.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::SlateDBError;
    use crate::iter::{KeyValueIterator, SeekToKey};
    use crate::merge_iterator::{MergeIterator, TwoMergeIterator};
    use crate::test_utils::{assert_iterator, assert_next_entry};
    use crate::types::RowEntry;
    use std::collections::VecDeque;
    use std::vec;

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
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"bbbb", b"2222", 0),
                RowEntry::new_value(b"cccc", b"use this one c", 0),
                RowEntry::new_value(b"xxxx", b"use this one x", 0),
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
            .with_entry(b"aaaa", b"1111")
            .with_entry(b"cccc", b"use this one c");
        let iter2 = TestIterator::new()
            .with_entry(b"cccc", b"badc")
            .with_entry(b"xxxx", b"24242424");

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"cccc", b"use this one c", 0),
                RowEntry::new_value(b"xxxx", b"24242424", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_merge_iter() {
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa1")
                .with_entry(b"bb", b"bb1"),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa2")
                .with_entry(b"bb", b"bb2")
                .with_entry(b"cc", b"cc2"),
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
        let mut iters = VecDeque::new();
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa1")
                .with_entry(b"bb", b"bb1"),
        );
        iters.push_back(
            TestIterator::new()
                .with_entry(b"aa", b"aa2")
                .with_entry(b"bb", b"bb2")
                .with_entry(b"cc", b"cc2"),
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
            .with_entry(b"aa", b"aa1")
            .with_entry(b"bb", b"bb1")
            .with_entry(b"dd", b"dd1");
        let iter2 = TestIterator::new()
            .with_entry(b"aa", b"aa2")
            .with_entry(b"bb", b"bb2")
            .with_entry(b"cc", b"cc2")
            .with_entry(b"ee", b"ee2");

        let mut merge_iter = TwoMergeIterator::new(iter1, iter2).await.unwrap();
        merge_iter.seek(b"b".as_ref()).await.unwrap();

        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_value(b"bb", b"bb1", 0),
                RowEntry::new_value(b"cc", b"cc2", 0),
                RowEntry::new_value(b"dd", b"dd1", 0),
                RowEntry::new_value(b"ee", b"ee2", 0),
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

    impl SeekToKey for TestIterator {
        async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
            while let Some(entry_result) = self.entries.front() {
                let entry = entry_result.clone()?;
                if entry.key < next_key {
                    self.entries.pop_front();
                } else {
                    break;
                }
            }
            Ok(())
        }
    }
}
