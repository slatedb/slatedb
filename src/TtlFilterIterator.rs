use crate::config::ReadLevel;
use crate::filter_iterator::FilterIterator;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::utils::{filter_expired, MonotonicClock};
use crate::SlateDBError;
use std::sync::Arc;

pub(crate) struct TtlFilterIterator<T: KeyValueIterator> {
    filter_iterator: FilterIterator<T>,
}

impl<T> TtlFilterIterator<T> {
    pub(crate) async fn new(
        iterator: T,
        mono_clock: Arc<MonotonicClock>,
        read_level: ReadLevel,
    ) -> Result<Self, SlateDBError> {
        let filter_entry = |entry: RowEntry| -> Result<Option<RowEntry>, SlateDBError> {
            filter_expired(entry, mono_clock, read_level)
        };
        let filter_it = FilterIterator::new(iterator, filter_entry);
        let it = Self {
            filter_iterator: filter_it,
        };
        Ok(it)
    }
}

