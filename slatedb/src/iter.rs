use std::future::Future;

use crate::error::SlateDBError;
use crate::types::RowEntry;

#[derive(Clone, Copy, Debug)]
pub enum IterationOrder {
    Ascending,
    Descending,
}

/// Note: this is intentionally its own trait instead of an Iterator<Item=KeyValue>,
/// because next will need to be made async to support SSTs, which are loaded over
/// the network.
/// See: https://github.com/slatedb/slatedb/issues/12
pub(crate) trait RowEntryIterator: Send + Sync {
    /// Performs any expensive initialization required before regular iteration.
    ///
    /// This method should be idempotent and can be called multiple times, only
    /// the first initialization should perform expensive operations.
    fn init(&mut self) -> impl Future<Output = Result<(), SlateDBError>> + Send;

    /// Returns the next entry in the iterator, which may be a key-value pair or
    /// a tombstone of a deleted key-value pair.
    ///
    /// Will fail with `SlateDBError::IteratorNotInitialized` if the iterator is
    /// not yet initialized.
    ///
    /// NOTE: we don't initialize the iterator when calling next and instead
    /// require the caller to explicitly initialize the iterator. This is in order
    /// to ensure that optimizations which eagerly initialize the iterator are not
    /// lost in a refactor and instead would throw errors.
    fn next(&mut self) -> impl Future<Output = Result<Option<RowEntry>, SlateDBError>> + Send;

    /// Seek to the next (inclusive) key
    ///
    /// Will fail with `SlateDBError::IteratorNotInitialized` if the iterator is
    /// not yet initialized.
    ///
    /// NOTE: we don't initialize the iterator when calling seek and instead
    /// require the caller to explicitly initialize the iterator. This is in order
    /// to ensure that optimizations which eagerly initialize the iterator are not
    /// lost in a refactor and instead would throw errors.
    fn seek(&mut self, next_key: &[u8]) -> impl Future<Output = Result<(), SlateDBError>> + Send;
}

/// Iterator trait that tracks bytes processed for progress reporting.
///
/// This extends `RowEntryIterator` with progress tracking capability.
/// Only iterators used in the compaction pipeline implement this trait.
/// The bottom-most iterator (MergeIterator) tracks actual bytes, while
/// wrapper iterators delegate to their inner iterator.
pub(crate) trait TrackedRowEntryIterator: RowEntryIterator {
    /// Returns the total bytes processed (key + value length) by this iterator.
    fn bytes_processed(&self) -> u64;
}

/// Defines an enum with one iterator per variant and implements
/// [`RowEntryIterator`] for it by delegating to the variant. Code that holds one
/// of several iterator types holds such an enum instead of boxing them, so that
/// `next` does not box a future per entry. A variant may be `#[cfg(...)]`, such
/// as one for an iterator that only tests build.
macro_rules! row_entry_iterator_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident $(<$lt:lifetime>)? {
            $(
                $(#[doc = $doc:literal])*
                $(#[cfg($cfg:meta)])?
                $variant:ident($ty:ty $(,)?)
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name $(<$lt>)? {
            $(
                $(#[doc = $doc])*
                $(#[cfg($cfg)])?
                $variant($ty)
            ),+
        }

        impl $(<$lt>)? $crate::iter::RowEntryIterator for $name $(<$lt>)? {
            async fn init(&mut self) -> Result<(), $crate::error::SlateDBError> {
                match self {
                    $($(#[cfg($cfg)])? Self::$variant(iter) => iter.init().await,)+
                }
            }

            async fn next(
                &mut self,
            ) -> Result<Option<$crate::types::RowEntry>, $crate::error::SlateDBError> {
                match self {
                    $($(#[cfg($cfg)])? Self::$variant(iter) => iter.next().await,)+
                }
            }

            async fn seek(&mut self, next_key: &[u8]) -> Result<(), $crate::error::SlateDBError> {
                match self {
                    $($(#[cfg($cfg)])? Self::$variant(iter) => iter.seek(next_key).await,)+
                }
            }
        }
    };
}
pub(crate) use row_entry_iterator_enum;

/// A boxed iterator calls the iterator it holds. The type stays concrete, so the
/// calls are dispatched statically and `next` allocates nothing; the box keeps
/// large iterators out of the structures that move them, such as the heap of a
/// merge.
impl<T: RowEntryIterator> RowEntryIterator for Box<T> {
    fn init(&mut self) -> impl Future<Output = Result<(), SlateDBError>> + Send {
        (**self).init()
    }

    fn next(&mut self) -> impl Future<Output = Result<Option<RowEntry>, SlateDBError>> + Send {
        (**self).next()
    }

    fn seek(&mut self, next_key: &[u8]) -> impl Future<Output = Result<(), SlateDBError>> + Send {
        (**self).seek(next_key)
    }
}

impl<T: TrackedRowEntryIterator> TrackedRowEntryIterator for Box<T> {
    fn bytes_processed(&self) -> u64 {
        (**self).bytes_processed()
    }
}

pub(crate) struct EmptyIterator;

impl EmptyIterator {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl RowEntryIterator for EmptyIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(None)
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Ok(())
    }
}
