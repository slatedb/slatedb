use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::utils::is_not_expired;
use crate::SlateDBError;

pub(crate) struct FilterIterator<T: KeyValueIterator> {
    iterator: T,
    predicate: Box<dyn Fn(&RowEntry) -> bool>,
}

impl<T: KeyValueIterator> FilterIterator<T> {
    pub(crate) fn new(iterator: T, predicate: Box<dyn Fn(&RowEntry) -> bool>) -> Self {
        Self {
            predicate,
            iterator,
        }
    }

    pub(crate) fn wrap_ttl_filter_iterator(iterator: T, now: i64) -> Self {
        let filter_entry = move |entry: &RowEntry| is_not_expired(entry, now);
        Self::new(iterator, Box::new(filter_entry))
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
