//! Pluggable logical row cache interface.
//!
//! A [`RowCache`] caches decoded MVCC rows and key ranges above SlateDB's LSM tree. It mirrors
//! [`DbCache`](crate::db_cache::DbCache): SlateDB calls into the cache on writes and reads, and
//! the caller owns the cache lifecycle outside SlateDB.
//!
//! Attach a cache with [`DbBuilder::with_row_cache`](crate::DbBuilder::with_row_cache) or
//! [`DbReaderBuilder::with_row_cache`](crate::DbReaderBuilder::with_row_cache).
//!
//! # Quick start
//!
//! Implement [`RowCache::get`] and [`RowCache::write`]. Other methods default to misses or no-ops,
//! and async iterator methods use plain `async fn` with no macro:
//!
//! ```
//! use std::collections::BTreeMap;
//! use std::sync::{Arc, Mutex};
//!
//! use bytes::Bytes;
//! use slatedb::object_store::memory::InMemory;
//! use slatedb::row_cache::{GetOutcome, RowCache};
//! use slatedb::{Db, Error, KeyValue, RowEntry, ValueDeletable};
//!
//! struct LatestRowCache {
//!     rows: Mutex<BTreeMap<Bytes, (u64, Option<KeyValue>)>>,
//! }
//!
//! impl RowCache for LatestRowCache {
//!     fn write(&self, seq: u64, entries: &[RowEntry]) {
//!         let mut rows = self.rows.lock().unwrap();
//!         for e in entries {
//!             match &e.value {
//!                 ValueDeletable::Value(v) => {
//!                     let kv = KeyValue::new(
//!                         e.key.clone(), v.clone(), seq, e.create_ts.unwrap_or(0), e.expire_ts,
//!                     );
//!                     rows.insert(e.key.clone(), (seq, Some(kv)));
//!                 }
//!                 ValueDeletable::Tombstone => { rows.insert(e.key.clone(), (seq, None)); }
//!                 ValueDeletable::Merge(_) => { rows.remove(&e.key); }
//!             }
//!         }
//!     }
//!
//!     fn get(&self, key: &[u8], seq: u64) -> GetOutcome {
//!         let rows = self.rows.lock().unwrap();
//!         match rows.get(key) {
//!             Some((entry_seq, kv)) if *entry_seq <= seq => GetOutcome::Hit(kv.clone()),
//!             _ => GetOutcome::Miss,
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let cache = Arc::new(LatestRowCache { rows: Mutex::default() });
//!     let db = Db::builder("/tmp/row_cache_example", Arc::new(InMemory::new()))
//!         .with_row_cache(cache)
//!         .build()
//!         .await?;
//!     db.put(b"k1", b"v1").await?;
//!     assert_eq!(db.get(b"k1").await?, Some(Bytes::from_static(b"v1")));
//!     db.close().await
//! }
//! ```
//!
//! # Contract
//!
//! A `RowCache` is non-durable and must match SlateDB's LSM read path on every hit:
//!
//! - **Sequence visibility:** Return only the latest version at or below `seq`. SlateDB passes
//!   `last_committed_seq()` on [`Db`](crate::Db) and [`DbReader`](crate::DbReader), and the
//!   snapshot or transaction sequence number on [`DbSnapshot`](crate::DbSnapshot) and
//!   [`DbTransaction`](crate::DbTransaction).
//! - **Option gating and metrics:** SlateDB skips the cache for dirty reads, remote-durability
//!   reads on [`Db`](crate::Db), descending scans, custom filter contexts, tracing,
//!   `row_cache: false`, and overlapping uncommitted transaction writes. SlateDB updates its own
//!   read and scan request counters on cache hits.
//! - **Deletes, merges, TTLs:** Never emit tombstones or expired rows (`expire_ts <= now_ms`).
//!   Resolve merges with the database's merge operator or return a miss.
//! - **Partial scans:** [`ScanOutcome::Partial`] returns ordered [`ScanSegment`] values that
//!   partition the requested range. SlateDB stitches cached segments and reads each uncached gap
//!   lazily from the LSM tree at the same `seq`.
//! - **Cold loading:** Populate cold ranges outside SlateDB by reading with
//!   `ReadOptions::with_row_cache(false)` or `ScanOptions::with_row_cache(false)` (for example
//!   through a [`DbSnapshot`](crate::DbSnapshot)).
//!
//! # Threading and lifecycle
//!
//! - Write-side callbacks (`write` and `invalidate`) run sequentially on one task: the batch
//!   writer on [`Db`](crate::Db), or the manifest poller on [`DbReader`](crate::DbReader).
//! - Read-side callbacks (`get` and `scan`) run concurrently on caller threads.
//! - `write` runs in ascending sequence order before readers observe `seq`.
//! - The caller owns the cache and stops any background tasks outside SlateDB.
//! - On a polling [`DbReader`](crate::DbReader), an L0 flush with unreplayed writes calls
//!   `invalidate` before the sequence number advances. Caches attached to a polling reader must
//!   override `invalidate`.

use std::fmt;
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::pin::Pin;

use bytes::Bytes;

use crate::bytes_range::{ByteRangeBounds, BytesRange};
use crate::config::{DurabilityLevel, ReadOptions, ScanOptions};
use crate::iter::IterationOrder;
use crate::types::{KeyValue, RowEntry};

/// Pluggable logical row cache attached through
/// [`DbBuilder::with_row_cache`](crate::DbBuilder::with_row_cache) or
/// [`DbReaderBuilder::with_row_cache`](crate::DbReaderBuilder::with_row_cache).
///
/// Implement [`Self::get`] and [`Self::write`]; other methods provide safe defaults. See the
/// [module documentation](self) for the full contract and lifecycle.
pub trait RowCache: Send + Sync + 'static {
    /// Serves a point read synchronously at sequence number `seq`.
    fn get(&self, key: &[u8], seq: u64) -> GetOutcome;

    /// Applies committed rows for `seq` on the write task, before readers observe `seq`.
    ///
    /// Entries arrive in ascending key order with sequence number `seq`. `entries` may be empty
    /// when `seq` advances with no row changes.
    fn write(&self, seq: u64, entries: &[RowEntry]);

    /// Serves an ascending scan synchronously at sequence number `seq`.
    ///
    /// When `prefix` is `Some`, every key in `range` starts with `prefix`.
    fn scan(&self, _range: KeyRange<'_>, _seq: u64, _prefix: Option<&[u8]>) -> ScanOutcome {
        ScanOutcome::Miss
    }

    /// Drops cached rows in `range` when a [`DbReader`](crate::DbReader) observes L0 updates
    /// whose writes it did not replay.
    ///
    /// Runs on the manifest poller before the reader's sequence number advances. `range` is a
    /// new L0 SST key range when SlateDB can isolate it, or [`KeyRange::all`] otherwise. Caches
    /// on a polling `DbReader` must override this method.
    fn invalidate(&self, _range: KeyRange<'_>) {}
}

/// Result of [`RowCache::get`].
#[derive(Debug)]
pub enum GetOutcome {
    /// The cache answered the read (`Some` for a live row, `None` for a known absent key).
    Hit(Option<KeyValue>),
    /// The cache missed; SlateDB falls back to its normal LSM read path.
    Miss,
    /// The cache failed; SlateDB returns this error directly to the caller.
    Error(crate::Error),
}

/// Type-erased [`ScanIterator`] returned inside [`ScanOutcome::Hit`] or [`ScanSegment::Cached`].
pub struct BoxedScanIterator(pub(crate) Box<dyn DynScanIterator>);

impl BoxedScanIterator {
    /// Wraps a concrete [`ScanIterator`] for return from [`RowCache::scan`].
    #[inline]
    pub fn new<I: ScanIterator>(iter: I) -> Self {
        Self(Box::new(iter))
    }
}

impl fmt::Debug for BoxedScanIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedScanIterator").finish_non_exhaustive()
    }
}

/// One ordered slice of a [`ScanOutcome::Partial`] scan.
///
/// Segments must partition the requested range in ascending key order with no overlaps or gaps.
/// Each [`Self::Cached`] iterator must yield only keys up to its `end` bound.
pub enum ScanSegment {
    /// A cached subrange served by `iter`, ending at `end`.
    Cached {
        /// Pre-resolved ascending iterator over this cached subrange.
        iter: BoxedScanIterator,
        /// Upper key bound of this cached subrange.
        end: Bound<Bytes>,
    },
    /// An uncached subrange that SlateDB reads lazily from the LSM tree.
    Gap {
        /// Lower key bound of the uncached subrange.
        start: Bound<Bytes>,
        /// Upper key bound of the uncached subrange.
        end: Bound<Bytes>,
    },
}

impl ScanSegment {
    /// Creates a [`Self::Cached`] segment from a concrete [`ScanIterator`] and upper bound.
    #[inline]
    pub fn cached<I: ScanIterator>(iter: I, end: Bound<Bytes>) -> Self {
        Self::Cached {
            iter: BoxedScanIterator::new(iter),
            end,
        }
    }

    /// Creates a [`Self::Gap`] segment for an uncached key range.
    #[inline]
    pub fn gap(start: Bound<Bytes>, end: Bound<Bytes>) -> Self {
        Self::Gap { start, end }
    }
}

impl fmt::Debug for ScanSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cached { iter, end } => f
                .debug_struct("Cached")
                .field("iter", iter)
                .field("end", end)
                .finish(),
            Self::Gap { start, end } => f
                .debug_struct("Gap")
                .field("start", start)
                .field("end", end)
                .finish(),
        }
    }
}

/// Result of [`RowCache::scan`].
pub enum ScanOutcome {
    /// The cache answered the entire scan with an ascending iterator.
    Hit(BoxedScanIterator),
    /// The cache split the scan into ordered cached slices and uncached LSM gaps.
    Partial(Vec<ScanSegment>),
    /// The cache missed; SlateDB falls back to its normal LSM scan path.
    Miss,
    /// The cache failed; SlateDB returns this error directly to the caller.
    Error(crate::Error),
}

impl ScanOutcome {
    /// Returns a [`Self::Hit`] wrapping a concrete [`ScanIterator`].
    #[inline]
    pub fn hit<I: ScanIterator>(iter: I) -> Self {
        Self::Hit(BoxedScanIterator::new(iter))
    }
}

impl fmt::Debug for ScanOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hit(iter) => f.debug_tuple("Hit").field(iter).finish(),
            Self::Partial(segments) => f.debug_tuple("Partial").field(segments).finish(),
            Self::Miss => f.write_str("Miss"),
            Self::Error(err) => f.debug_tuple("Error").field(err).finish(),
        }
    }
}

/// Borrowed key range passed to [`RowCache::scan`] and [`RowCache::invalidate`].
///
/// Implements [`RangeBounds<[u8]>`] (works with `BTreeMap::range`) and [`ByteRangeBounds`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRange<'a> {
    /// Lower bound of the range.
    pub start: Bound<&'a [u8]>,
    /// Upper bound of the range.
    pub end: Bound<&'a [u8]>,
}

impl<'a> KeyRange<'a> {
    /// Returns an unbounded range covering the entire keyspace.
    #[inline]
    pub const fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    pub(crate) fn from_bytes_range(range: &'a BytesRange) -> Self {
        Self {
            start: ByteRangeBounds::start_bound(range),
            end: ByteRangeBounds::end_bound(range),
        }
    }

    /// Returns `true` if `key` lies within this range.
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        RangeBounds::contains(self, key)
    }

    /// Returns the lower bound key bytes, if bounded.
    #[inline]
    pub const fn start_key(&self) -> Option<&'a [u8]> {
        match self.start {
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
            Bound::Unbounded => None,
        }
    }

    /// Returns the upper bound key bytes, if bounded.
    #[inline]
    pub const fn end_key(&self) -> Option<&'a [u8]> {
        match self.end {
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
            Bound::Unbounded => None,
        }
    }
}

impl RangeBounds<[u8]> for KeyRange<'_> {
    #[inline]
    fn start_bound(&self) -> Bound<&[u8]> {
        self.start
    }
    #[inline]
    fn end_bound(&self) -> Bound<&[u8]> {
        self.end
    }
}

impl ByteRangeBounds for KeyRange<'_> {
    #[inline]
    fn start_bound(&self) -> Bound<&[u8]> {
        self.start
    }
    #[inline]
    fn end_bound(&self) -> Bound<&[u8]> {
        self.end
    }
}

/// Result of [`ScanIterator::next_sync`].
#[derive(Debug)]
pub enum SyncStep {
    /// Yielded the next row synchronously.
    Row(KeyValue),
    /// Reached the end of the iterator.
    Done,
    /// The next row requires an async poll through [`ScanIterator::next`].
    NeedsAsync,
    /// Encountered an error.
    Error(crate::Error),
}

/// Result of [`ScanIterator::next_batch_sync`], carrying the number of rows appended to `out`.
#[derive(Debug)]
pub enum BatchStep {
    /// Appended `count` rows synchronously; more rows may follow.
    More(usize),
    /// Appended `count` rows and reached the end of the iterator.
    Done(usize),
    /// Appended `count` rows; the next row requires [`ScanIterator::next`].
    NeedsAsync(usize),
    /// Appended `count` rows before encountering an error.
    Error(usize, crate::Error),
}

/// Pre-resolved ascending scan iterator returned by [`RowCache::scan`].
///
/// Yield unique non-tombstone, non-expired rows in ascending key order with sequence filtering
/// and merges already resolved. Pure in-memory iterators implement only [`Self::next_sync`] and
/// [`Self::seek`] (using plain `async fn`).
pub trait ScanIterator: Send + Sync + 'static {
    /// Advances to the next row without awaiting.
    fn next_sync(&mut self) -> SyncStep;

    /// Seeks forward to the first key at or after `next_key`.
    fn seek(&mut self, next_key: &[u8]) -> impl Future<Output = Result<(), crate::Error>> + Send;

    /// Appends up to `max` rows to `out` synchronously. Override to avoid per-row dynamic
    /// dispatch during [`DbIterator::next_batch`](crate::DbIterator::next_batch).
    fn next_batch_sync(&mut self, out: &mut Vec<KeyValue>, max: usize) -> BatchStep {
        let mut count = 0;
        while count < max {
            match self.next_sync() {
                SyncStep::Row(kv) => {
                    out.push(kv);
                    count += 1;
                }
                SyncStep::Done => return BatchStep::Done(count),
                SyncStep::NeedsAsync => return BatchStep::NeedsAsync(count),
                SyncStep::Error(err) => return BatchStep::Error(count, err),
            }
        }
        BatchStep::More(count)
    }

    /// Returns the next row asynchronously when [`Self::next_sync`] returns
    /// [`SyncStep::NeedsAsync`].
    fn next(&mut self) -> impl Future<Output = Result<Option<KeyValue>, crate::Error>> + Send {
        async {
            match self.next_sync() {
                SyncStep::Row(kv) => Ok(Some(kv)),
                SyncStep::Done => Ok(None),
                SyncStep::Error(err) => Err(err),
                SyncStep::NeedsAsync => Err(crate::Error::internal(
                    "ScanIterator::next_sync returned NeedsAsync without overriding next"
                        .to_string(),
                )),
            }
        }
    }
}

/// Internal dyn-compatible bridge for [`ScanIterator`]; boxes futures only on `next` and `seek`.
pub(crate) trait DynScanIterator: Send + Sync + 'static {
    fn next_sync(&mut self) -> SyncStep;
    fn next_batch_sync(&mut self, out: &mut Vec<KeyValue>, max: usize) -> BatchStep;
    fn next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<KeyValue>, crate::Error>> + Send + '_>>;
    fn seek<'a>(
        &'a mut self,
        next_key: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::Error>> + Send + 'a>>;
}

impl<I: ScanIterator> DynScanIterator for I {
    #[inline]
    fn next_sync(&mut self) -> SyncStep {
        ScanIterator::next_sync(self)
    }
    #[inline]
    fn next_batch_sync(&mut self, out: &mut Vec<KeyValue>, max: usize) -> BatchStep {
        ScanIterator::next_batch_sync(self, out, max)
    }
    fn next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<KeyValue>, crate::Error>> + Send + '_>> {
        Box::pin(ScanIterator::next(self))
    }
    fn seek<'a>(
        &'a mut self,
        next_key: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::Error>> + Send + 'a>> {
        Box::pin(ScanIterator::seek(self, next_key))
    }
}

/// Returns `true` when `options` allows a point read to consult a [`RowCache`].
#[inline]
pub(crate) fn can_use_row_cache_get(options: &ReadOptions, is_reader: bool) -> bool {
    options.row_cache
        && !options.dirty
        && options.filter_context.is_none()
        && options.tracing_options.is_none()
        && (is_reader || matches!(options.durability_filter, DurabilityLevel::Memory))
}

/// Returns `true` when `options` allows a scan to consult a [`RowCache`].
#[inline]
pub(crate) fn can_use_row_cache_scan(options: &ScanOptions, is_reader: bool) -> bool {
    options.row_cache
        && !options.dirty
        && matches!(options.order, IterationOrder::Ascending)
        && options.filter_context.is_none()
        && options.tracing_options.is_none()
        && (is_reader || matches!(options.durability_filter, DurabilityLevel::Memory))
}
