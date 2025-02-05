use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::SlateDBError;

pub(crate) struct FilterIterator<T: KeyValueIterator> {
    iterator: T,
    predicate: Box<dyn Fn(&RowEntry) -> Result<bool, SlateDBError>>
}

impl<T: KeyValueIterator> FilterIterator<T> {
    pub(crate) async fn new(
        iterator: T,
        predicate: Box<dyn Fn(&RowEntry) -> Result<bool, SlateDBError>>
    ) -> Result<Self, SlateDBError> {
        let it = Self {
            predicate,
            iterator
        };
        Ok(it)
    }
}

impl<T: KeyValueIterator> KeyValueIterator for FilterIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(entry) = self.iterator.next_entry().await? {
            if (self.predicate)(&entry)? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}