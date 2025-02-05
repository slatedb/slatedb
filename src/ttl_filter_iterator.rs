use crate::config::ReadLevel;
use crate::filter_iterator::FilterIterator;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::utils::{is_not_expired, MonotonicClock};
use crate::SlateDBError;
use std::sync::Arc;

pub(crate) struct TtlFilterIterator<T: KeyValueIterator> {
    filter_iterator: FilterIterator<T>,
}

impl<T: KeyValueIterator> TtlFilterIterator<T> {
    pub(crate) async fn new(
        iterator: T,
        mono_clock: Arc<MonotonicClock>,
        read_level: ReadLevel,
    ) -> Result<Self, SlateDBError> {
        let filter_entry = move |entry: &RowEntry| is_not_expired(entry, mono_clock.clone(), read_level);
        let filter_it = FilterIterator::new(iterator, Box::new(filter_entry)).await?;
        let it = Self {
            filter_iterator: filter_it,
        };
        Ok(it)
    }
}

impl<T: KeyValueIterator> KeyValueIterator for TtlFilterIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.filter_iterator.next_entry().await
    }
}
