use std::sync::Arc;

use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::utils::is_not_expired;
use crate::SlateDBError;

pub(crate) struct FilterIterator<T: KeyValueIterator> {
    iterator: T,
    predicate: Arc<dyn Fn(&RowEntry) -> bool + Send + Sync>,
}

impl<T: KeyValueIterator> FilterIterator<T> {
    pub(crate) fn new(
        iterator: T,
        predicate: Arc<dyn Fn(&RowEntry) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            predicate,
            iterator,
        }
    }

    pub(crate) fn new_with_ttl_now(iterator: T, ttl_now: i64) -> Self {
        let predicate = Arc::new(move |entry: &RowEntry| is_not_expired(entry, ttl_now));
        Self::new(iterator, predicate)
    }

    pub(crate) fn new_with_max_seq(iterator: T, max_seq: Option<u64>) -> Self {
        match max_seq {
            Some(max_seq) => {
                let predicate = Arc::new(move |entry: &RowEntry| entry.seq <= max_seq);
                Self::new(iterator, predicate)
            }
            None => Self::new(iterator, Arc::new(|_| true)),
        }
    }
}

impl<T: KeyValueIterator> KeyValueIterator for FilterIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(entry) = self.iterator.next_entry().await? {
            if (self.predicate)(&entry) {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::assert_iterator;
    use crate::types::RowEntry;

    #[tokio::test]
    async fn test_filter_iterator_should_return_only_matching_entries() {
        let iter = crate::test_utils::TestIterator::new()
            .with_entry(b"aaaa", b"1111", 0)
            .with_entry(b"bbbb", b"", 0)
            .with_entry(b"cccc", b"3333", 0)
            .with_entry(b"d", b"4444", 0)
            .with_entry(b"eeee", b"5", 0)
            .with_entry(b"ffff", b"6666", 0)
            .with_entry(b"g", b"7", 0);

        let filter_entry =
            move |entry: &RowEntry| -> bool { entry.key.len() == 4 && entry.value.len() == 4 };

        let mut filter_iter = FilterIterator::new(iter, Arc::new(filter_entry));

        assert_iterator(
            &mut filter_iter,
            vec![
                RowEntry::new_value(b"aaaa", b"1111", 0),
                RowEntry::new_value(b"cccc", b"3333", 0),
                RowEntry::new_value(b"ffff", b"6666", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_filter_iterator_should_return_none_with_no_matches() {
        let iter = crate::test_utils::TestIterator::new()
            .with_entry(b"", b"1", 0)
            .with_entry(b"b", b"2", 0)
            .with_entry(b"c", b"3", 0);

        let filter_entry =
            move |entry: &RowEntry| -> bool { entry.key.len() == 4 && entry.value.len() == 4 };

        let mut filter_iter = FilterIterator::new(iter, Arc::new(filter_entry));

        assert_eq!(filter_iter.next().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_filter_iterator_predicate_builder() {
        let iter = crate::test_utils::TestIterator::new()
            .with_entry(b"a", b"val1", 5)
            .with_entry(b"b", b"val2", 2)
            .with_entry(b"b", b"val2", 10)
            .with_entry(b"c", b"val3", 10)
            .with_entry(b"d", b"val4", 8);

        let mut filter_iter = FilterIterator::new_with_max_seq(iter, Some(9));

        assert_iterator(
            &mut filter_iter,
            vec![
                RowEntry::new_value(b"a", b"val1", 5),
                RowEntry::new_value(b"b", b"val2", 2),
                RowEntry::new_value(b"d", b"val4", 8),
            ],
        )
        .await;
    }
}
