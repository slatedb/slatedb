use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::SlateDBError;

pub(crate) struct FilterIterator<T: KeyValueIterator> {
    predicate: fn(RowEntry) -> bool,
    iterator: T
}

impl<T> FilterIterator<T> {
    pub(crate) async fn new(
        predicate: fn(RowEntry) -> bool,
        iterator: T
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
        while let Some(entry) = self.iterator.next_entry() {
            if self.predicate(entry) {
                return Ok(Some(entry))
            }
        }
        Ok(None)
    }
}