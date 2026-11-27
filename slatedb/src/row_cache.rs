//! Pluggable logical row cache interface.
//!
//! A [`RowCache`] caches decoded MVCC rows and key ranges above SlateDB's LSM tree (unlike
//! [`DbCache`](crate::db_cache::DbCache), which caches encoded SST blocks, indexes, and filters).
//! Attach one with [`DbBuilder::with_row_cache`](crate::DbBuilder::with_row_cache) or
//! [`DbReaderBuilder::with_row_cache`](crate::DbReaderBuilder::with_row_cache).
//!
//! # Quick start
//!
//! Implement only [`RowCache::get`] and [`RowCache::write`]. Other methods default to safe no-ops
//! or misses, and async methods use plain `async fn` with no macro:
//!
//! ```
//! use std::collections::BTreeMap;
//! use std::sync::{Arc, Mutex, OnceLock};
//!
//! use bytes::Bytes;
//! use slatedb::config::ReadOptions;
//! use slatedb::object_store::memory::InMemory;
//! use slatedb::row_cache::{GetOutcome, Host, OpenContext, RowCache};
//! use slatedb::{Db, Error, KeyValue, RowEntry, ValueDeletable};
//!
//! struct LatestRowCache {
//!     host: OnceLock<Host>,
//!     rows: Mutex<BTreeMap<Bytes, (u64, Option<KeyValue>)>>,
//! }
//!
//! impl RowCache for LatestRowCache {
//!     fn open(&self, ctx: OpenContext) -> Result<(), Error> {
//!         let _ = self.host.set(ctx.host);
//!         Ok(())
//!     }
//!
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
//!     fn get(&self, key: &[u8], options: &ReadOptions, at: Option<u64>) -> GetOutcome {
//!         let Some(host) = self.host.get() else { return GetOutcome::Miss; };
//!         if !host.allows_read(options) { return GetOutcome::Miss; }
//!         let seq = host.read_seq(at);
//!         let rows = self.rows.lock().unwrap();
//!         match rows.get(key) {
//!             Some((entry_seq, kv)) if *entry_seq <= seq => {
//!                 host.record_get_requests(1);
//!                 GetOutcome::Hit(kv.clone())
//!             }
//!             _ => GetOutcome::Miss,
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let cache = Arc::new(LatestRowCache { host: OnceLock::new(), rows: Mutex::default() });
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
//! - **Sequence visibility:** Call [`Host::read_seq`] after pinning internal cache state.
//! - **Options:** Use [`Host::allows_read`] and [`Host::allows_scan`] to miss on dirty reads,
//!   tracing, custom filter contexts, or remote-durability reads on [`Db`](crate::Db). SlateDB
//!   already bypasses the cache for descending scans and overlapping uncommitted transaction
//!   writes.
//! - **Deletes, merges, TTLs:** Never emit tombstones or expired rows (`expire_ts <= now_ms` via
//!   [`OpenContext::clock`]). Resolve merges with [`OpenContext::merge_operator`] or return a
//!   miss.
//! - **Metrics:** Call [`Host::record_get_requests`] or [`Host::record_scan_requests`] on hits.
//!
//! # Threading and lifecycle
//!
//! - Write-side callbacks (`write`, `invalidate`, and [`Host::run_on_writer`] closures) run
//!   sequentially on one task: the batch writer on [`Db`](crate::Db), or the manifest poller on
//!   [`DbReader`](crate::DbReader).
//! - Read-side callbacks (`get`, `scan`) run concurrently on caller threads.
//! - Lifecycle: `open` runs after WAL replay (an error aborts startup); `write` runs in ascending
//!   sequence order before [`Host::last_committed_seq`] advances. The caller owns the cache and
//!   closes any background tasks outside SlateDB (matching [`DbCache`](crate::db_cache::DbCache));
//!   stopping `Db` or `DbReader` fails pending [`Host::run_on_writer`] waiters with a closed
//!   error.
//! - On a polling [`DbReader`](crate::DbReader), an L0 flush with unreplayed writes calls
//!   `invalidate` before the sequence number advances. Caches attached to a polling reader must
//!   override `invalidate`.

use std::fmt;
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::pin::Pin;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use fail_parallel::FailPointRegistry;
use slatedb_common::clock::SystemClock;
use slatedb_common::metrics::MetricsRecorder;
use slatedb_common::DbRand;
use tokio::runtime::Handle;

use crate::batch_write::BatchWriterMessage;
use crate::bytes_range::{ByteRangeBounds, BytesRange};
use crate::config::{DurabilityLevel, ReadOptions, ScanOptions};
use crate::db::DbInner;
use crate::db_iter::DbIterator;
use crate::db_reader::{DbReaderInner, DbReaderMessage, ReaderSnapshotGuard, ReaderState};
use crate::db_snapshot::DbSnapshot;
use crate::db_state::DbStateView;
use crate::db_stats::DbStats;
use crate::db_status::{ClosedResultWriter, DbStatusManager};
use crate::error::SlateDBError;
use crate::iter::IterationOrder;
use crate::manifest::{ManifestCore, SsTableHandle, VersionedManifest};
use crate::merge_operator::MergeOperator;
use crate::oracle::{DbOracle, DbReaderOracle, Oracle};
use crate::read_trace::ReadTrace;
use crate::reader::{DbStateReader, ScanContext};
use crate::sst_reader::SstIndex;
use crate::types::{KeyValue, RowEntry};
use crate::utils::SafeSender;

/// Callback scheduled on the batch writer or manifest poller by [`Host::run_on_writer`].
pub(crate) type WriterTask = Box<dyn FnOnce(u64) + Send + 'static>;

/// Pluggable logical row cache attached through
/// [`DbBuilder::with_row_cache`](crate::DbBuilder::with_row_cache) or
/// [`DbReaderBuilder::with_row_cache`](crate::DbReaderBuilder::with_row_cache).
///
/// Implement only [`Self::get`] and [`Self::write`]; all other methods provide defaults. See the
/// [module documentation](self) for the full contract and lifecycle.
pub trait RowCache: Send + Sync + 'static {
    /// Serves a point read synchronously at `at`, or at the latest committed state when `None`.
    fn get(&self, key: &[u8], options: &ReadOptions, at: Option<u64>) -> GetOutcome;

    /// Applies committed rows for `seq` on the write task, before [`Host::last_committed_seq`]
    /// advances to `seq`.
    ///
    /// Entries arrive in ascending key order with sequence number `seq`. `entries` may be empty
    /// when `seq` advances with no row changes.
    fn write(&self, seq: u64, entries: &[RowEntry]);

    /// Serves an ascending scan synchronously at `at`, or at the latest committed state when
    /// `None`. When `prefix` is `Some`, every key in `range` starts with `prefix`.
    fn scan(
        &self,
        _range: KeyRange<'_>,
        _options: &ScanOptions,
        _prefix: Option<&[u8]>,
        _at: Option<u64>,
    ) -> ScanOutcome {
        ScanOutcome::Miss
    }

    /// Initializes the cache after WAL replay finishes. Returning an error aborts startup.
    fn open(&self, _ctx: OpenContext) -> Result<(), crate::Error> {
        Ok(())
    }

    /// Drops cached rows above `valid_through` when a [`DbReader`](crate::DbReader) moves, before
    /// its sequence number advances, to an L0 holding unreplayed writes up to `new_seq`.
    ///
    /// Reads at or below `valid_through` stay valid; later writes start after `new_seq`. Caches
    /// on a polling `DbReader` must override this method.
    fn invalidate(&self, _valid_through: u64, _new_seq: u64) {}
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

/// Type-erased [`ScanIterator`] returned inside [`ScanOutcome::Hit`].
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

/// Result of [`RowCache::scan`].
pub enum ScanOutcome {
    /// The cache answered the scan with an ascending iterator.
    Hit(BoxedScanIterator),
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
            Self::Miss => f.write_str("Miss"),
            Self::Error(err) => f.debug_tuple("Error").field(err).finish(),
        }
    }
}

/// Borrowed key range passed to [`RowCache::scan`].
///
/// Implements [`RangeBounds<[u8]>`] (works with `BTreeMap::range`) and [`ByteRangeBounds`] (works
/// with [`Host::scan_view`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRange<'a> {
    /// Lower bound of the range.
    pub start: Bound<&'a [u8]>,
    /// Upper bound of the range.
    pub end: Bound<&'a [u8]>,
}

impl<'a> KeyRange<'a> {
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
    /// dispatch during [`DbIterator::next_batch`].
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

/// Startup context passed to [`RowCache::open`] after WAL replay finishes.
pub struct OpenContext {
    /// Handle for reading SlateDB state and scheduling write-serialized callbacks.
    pub host: Host,
    /// System clock configured on the database.
    pub clock: Arc<dyn SystemClock>,
    /// Random number generator configured on the database.
    pub rand: Arc<DbRand>,
    /// Tokio runtime handle used by the database.
    pub runtime: Handle,
    /// Metrics recorder configured on the database.
    pub metrics_recorder: Arc<dyn MetricsRecorder>,
    /// Read-path merge operator configured on the database, instrumented with SlateDB's read
    /// merge counter.
    pub merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>,
    /// Failpoint registry configured on the database.
    pub fp_registry: Arc<FailPointRegistry>,
    /// `true` if a compaction filter supplier is configured on the embedded compactor (always
    /// `false` on a [`DbReader`](crate::DbReader)).
    pub has_compaction_filter: bool,
}

impl OpenContext {
    pub(crate) fn new(
        inner: &Arc<DbInner>,
        runtime: Handle,
        metrics_recorder: Arc<dyn MetricsRecorder>,
        has_compaction_filter: bool,
    ) -> Self {
        Self {
            host: Host::new(inner),
            clock: Arc::clone(&inner.system_clock),
            rand: Arc::clone(&inner.rand),
            runtime,
            metrics_recorder,
            merge_operator: inner.reader.read_merge_operator.clone(),
            fp_registry: Arc::clone(&inner.fp_registry),
            has_compaction_filter,
        }
    }

    pub(crate) fn new_reader(
        inner: &Arc<DbReaderInner>,
        wake_tx: async_channel::Sender<DbReaderMessage>,
        runtime: Handle,
        metrics_recorder: Arc<dyn MetricsRecorder>,
        fp_registry: Arc<FailPointRegistry>,
    ) -> Self {
        Self {
            host: Host::new_reader(inner, wake_tx),
            clock: Arc::clone(&inner.system_clock),
            rand: Arc::clone(&inner.rand),
            runtime,
            metrics_recorder,
            merge_operator: inner.reader.read_merge_operator.clone(),
            fp_registry,
            has_compaction_filter: false,
        }
    }
}

impl fmt::Debug for OpenContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenContext")
            .field("host", &self.host)
            .field("has_merge_operator", &self.merge_operator.is_some())
            .field("has_compaction_filter", &self.has_compaction_filter)
            .finish_non_exhaustive()
    }
}

/// Handle passed to a [`RowCache`] for reading SlateDB state and coordinating with the write task.
///
/// Hot-path methods read direct cycle-free references without upgrading a `Weak` pointer.
/// Cold-path methods upgrade a weak database reference so storing `Host` creates no cycle.
#[derive(Clone)]
pub struct Host {
    status_manager: DbStatusManager,
    db_stats: DbStats,
    kind: HostKind,
}

#[derive(Clone)]
enum HostKind {
    Writer {
        oracle: Arc<DbOracle>,
        write_notifier: SafeSender<BatchWriterMessage>,
        inner: Weak<DbInner>,
    },
    Reader {
        oracle: Arc<DbReaderOracle>,
        wake_tx: async_channel::Sender<DbReaderMessage>,
        inner: Weak<DbReaderInner>,
    },
}

enum Upgraded {
    Writer(Arc<DbInner>),
    Reader(Arc<DbReaderInner>),
}

impl fmt::Debug for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.is_reader() { "reader" } else { "writer" };
        f.debug_struct("Host")
            .field("kind", &kind)
            .field("last_committed_seq", &self.last_committed_seq())
            .finish()
    }
}

impl Host {
    pub(crate) fn new(inner: &Arc<DbInner>) -> Self {
        Self {
            status_manager: (*inner.status_manager).clone(),
            db_stats: inner.db_stats.clone(),
            kind: HostKind::Writer {
                oracle: Arc::clone(&inner.oracle),
                write_notifier: inner.write_notifier.clone(),
                inner: Arc::downgrade(inner),
            },
        }
    }

    pub(crate) fn new_reader(
        inner: &Arc<DbReaderInner>,
        wake_tx: async_channel::Sender<DbReaderMessage>,
    ) -> Self {
        Self {
            status_manager: inner.status_manager.clone(),
            db_stats: inner.reader.db_stats.clone(),
            kind: HostKind::Reader {
                oracle: Arc::clone(&inner.oracle),
                wake_tx,
                inner: Arc::downgrade(inner),
            },
        }
    }

    fn upgrade(&self) -> Result<Upgraded, crate::Error> {
        let upgraded = match &self.kind {
            HostKind::Writer { inner, .. } => inner.upgrade().map(Upgraded::Writer),
            HostKind::Reader { inner, .. } => inner.upgrade().map(Upgraded::Reader),
        };
        upgraded.ok_or_else(|| SlateDBError::Closed.into())
    }

    fn closed_error(&self) -> crate::Error {
        self.check_closed()
            .err()
            .unwrap_or_else(|| SlateDBError::Closed.into())
    }

    /// Returns `true` on [`DbReader`](crate::DbReader) and `false` on [`Db`](crate::Db).
    #[inline]
    pub fn is_reader(&self) -> bool {
        matches!(self.kind, HostKind::Reader { .. })
    }

    /// Returns the newest committed sequence number (on a reader, the durable seq it serves).
    #[inline]
    pub fn last_committed_seq(&self) -> u64 {
        match &self.kind {
            HostKind::Writer { oracle, .. } => oracle.last_committed_seq(),
            HostKind::Reader { oracle, .. } => oracle.last_committed_seq(),
        }
    }

    /// Resolves the effective read sequence number: `last_committed_seq()` or `at.min(committed)`.
    #[inline]
    pub fn read_seq(&self, at: Option<u64>) -> u64 {
        let committed = self.last_committed_seq();
        at.map_or(committed, |seq| seq.min(committed))
    }

    /// Returns `true` if `options` requests a committed, untraced, unfiltered read a row cache
    /// can serve (allowing [`DurabilityLevel::Remote`] on [`DbReader`](crate::DbReader) only).
    #[inline]
    pub fn allows_read(&self, options: &ReadOptions) -> bool {
        !options.dirty
            && options.filter_context.is_none()
            && options.tracing_options.is_none()
            && (self.is_reader() || matches!(options.durability_filter, DurabilityLevel::Memory))
    }

    /// Returns `true` if `options` requests a standard ascending scan a row cache can serve.
    #[inline]
    pub fn allows_scan(&self, options: &ScanOptions) -> bool {
        !options.dirty
            && matches!(options.order, IterationOrder::Ascending)
            && options.filter_context.is_none()
            && options.tracing_options.is_none()
            && (self.is_reader() || matches!(options.durability_filter, DurabilityLevel::Memory))
    }

    /// Returns an error if the database is closed or failed.
    #[inline]
    pub fn check_closed(&self) -> Result<(), crate::Error> {
        if !self.status_manager.maybe_closed() {
            return Ok(());
        }
        if let Some(result) = self.status_manager.result_reader().read() {
            return match result {
                Ok(()) => Err(SlateDBError::Closed.into()),
                Err(err) => Err(err.into()),
            };
        }
        Ok(())
    }

    /// Runs `f` on the single write-side task (batch writer on [`Db`](crate::Db), or manifest
    /// poller on [`DbReader`](crate::DbReader)) between write batches, passing the current
    /// committed sequence number.
    ///
    /// Returns a closed or fenced error if that task stops before running `f`.
    pub async fn run_on_writer<R: Send + 'static>(
        &self,
        f: impl FnOnce(u64) -> R + Send + 'static,
    ) -> Result<R, crate::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task: WriterTask = Box::new(move |seq| {
            let _ = tx.send(f(seq));
        });
        let sent = match &self.kind {
            HostKind::Writer { write_notifier, .. } => write_notifier
                .send(BatchWriterMessage::RowCacheTask(task))
                .is_ok(),
            HostKind::Reader { wake_tx, .. } => wake_tx
                .try_send(DbReaderMessage::RowCacheTask(task))
                .is_ok(),
        };
        if !sent {
            return Err(self.closed_error());
        }
        rx.await.map_err(|_| self.closed_error())
    }

    /// Adds `count` to SlateDB's point-read request counter.
    #[inline]
    pub fn record_get_requests(&self, count: u64) {
        self.db_stats.get_requests.increment(count);
    }

    /// Adds `count` to SlateDB's scan request counter.
    #[inline]
    pub fn record_scan_requests(&self, count: u64) {
        self.db_stats.scan_requests.increment(count);
    }

    /// Captures a consistent view of memtables and manifest (on a reader, the newest poller
    /// state holding every applied write).
    pub fn capture_state_view(&self) -> Result<StateView, crate::Error> {
        let view = match self.upgrade()? {
            Upgraded::Writer(inner) => ViewState::Writer(inner.state.read().view()),
            Upgraded::Reader(inner) => ViewState::Reader(inner.row_cache_view()),
        };
        Ok(StateView(view))
    }

    /// Returns `true` if `view` still points to the current active memtable (on [`Db`](crate::Db))
    /// or the newest poller state (on [`DbReader`](crate::DbReader)).
    pub fn is_view_current(&self, view: &StateView) -> bool {
        match (self.upgrade(), &view.0) {
            (Ok(Upgraded::Writer(inner)), ViewState::Writer(view)) => {
                Arc::ptr_eq(&view.memtable, inner.state.read().memtable().table())
            }
            (Ok(Upgraded::Reader(inner)), ViewState::Reader(state)) => {
                Arc::ptr_eq(state, &inner.row_cache_view())
            }
            _ => false,
        }
    }

    /// Returns `true` if `view` retains every version visible at `seq`.
    ///
    /// Holds when `last_l0_seq <= seq`, or on [`Db`](crate::Db) when an active snapshot,
    /// transaction, or open scan iterator retains a sequence number at or below `seq`.
    pub fn retains_seq(&self, view: &StateView, seq: u64) -> bool {
        if view.core().last_l0_seq <= seq {
            return true;
        }
        match (self.upgrade(), &view.0) {
            (Ok(Upgraded::Writer(inner)), ViewState::Writer(_)) => [
                inner.snapshot_manager.min_active_seq(),
                inner.txn_manager.min_active_seq(),
            ]
            .into_iter()
            .flatten()
            .min()
            .is_some_and(|registered| registered <= seq),
            _ => false,
        }
    }

    /// Scans `range` directly against `view` on SlateDB's normal read path, bypassing the cache.
    pub async fn scan_view<T>(
        &self,
        view: &StateView,
        range: T,
        options: &ViewScanOptions,
    ) -> Result<ViewIterator, crate::Error>
    where
        T: ByteRangeBounds + Send,
    {
        let start = range.start_bound().map(Bytes::copy_from_slice);
        let end = range.end_bound().map(Bytes::copy_from_slice);
        let Some(range) = BytesRange::try_new(start, end) else {
            return Err(crate::Error::invalid("empty scan range".to_string()));
        };
        let iter = match (self.upgrade()?, &view.0) {
            (Upgraded::Writer(inner), ViewState::Writer(state)) => {
                inner
                    .reader
                    .scan_with_options(
                        range,
                        &options.scan_options,
                        ScanContext {
                            db_state: state,
                            write_batch_iter: None,
                            max_seq: options.max_seq,
                            prefix: options.prefix.clone(),
                        },
                    )
                    .await?
            }
            (Upgraded::Reader(inner), ViewState::Reader(state)) => {
                // The view may lead the reader's seq, which would clamp `max_seq`; every row of
                // a reader state is durable.
                let max_seq = options.max_seq.unwrap_or_else(|| self.last_committed_seq());
                let scan_options = ScanOptions {
                    durability_filter: DurabilityLevel::Memory,
                    dirty: true,
                    ..options.scan_options.clone()
                };
                inner
                    .reader
                    .scan_with_options(
                        range,
                        &scan_options,
                        ScanContext {
                            db_state: state.as_ref(),
                            write_batch_iter: None,
                            max_seq: Some(max_seq),
                            prefix: options.prefix.clone(),
                        },
                    )
                    .await?
            }
            _ => {
                return Err(crate::Error::invalid(
                    "state view of another database".to_string(),
                ))
            }
        };
        Ok(ViewIterator { inner: iter })
    }

    /// Opens a retention snapshot at the current committed sequence number. On a reader, waits
    /// while the poller replaces the current checkpoint.
    pub async fn snapshot(&self) -> Result<HostSnapshot, crate::Error> {
        self.check_closed()?;
        let snapshot = match self.upgrade()? {
            Upgraded::Writer(inner) => HostSnapshotKind::Writer(DbSnapshot::new(inner, None)),
            Upgraded::Reader(inner) => HostSnapshotKind::Reader(inner.host_snapshot().await?),
        };
        Ok(HostSnapshot(snapshot))
    }

    /// Returns a snapshot of the database's current manifest.
    pub fn manifest(&self) -> Result<VersionedManifest, crate::Error> {
        Ok(match self.upgrade()? {
            Upgraded::Writer(inner) => inner.manifest(),
            Upgraded::Reader(inner) => VersionedManifest::from(inner.row_cache_view().as_ref()),
        })
    }

    /// Reads the zero-copy block index of `handle` through the table store and block cache.
    pub async fn read_sst_index(&self, handle: &SsTableHandle) -> Result<SstIndex, crate::Error> {
        let table_store = match self.upgrade()? {
            Upgraded::Writer(inner) => Arc::clone(&inner.table_store),
            Upgraded::Reader(inner) => Arc::clone(&inner.table_store),
        };
        let index = table_store
            .read_index(handle, true, Some(Bytes::new()), &ReadTrace::none(), None)
            .await?;
        Ok(SstIndex::from_owned(index))
    }
}

/// Options for [`Host::scan_view`].
#[derive(Debug, Clone, Default)]
pub struct ViewScanOptions {
    /// Underlying SlateDB scan options (for example, block caching and read-ahead).
    pub scan_options: ScanOptions,
    /// Optional prefix hint for prefix bloom filters; every key in `range` must start with it.
    pub prefix: Option<Bytes>,
    /// Upper sequence number bound (`None` reads at the newest committed sequence number).
    pub max_seq: Option<u64>,
}

/// Opaque snapshot of SlateDB's in-memory and manifest state.
pub struct StateView(ViewState);

enum ViewState {
    Writer(DbStateView),
    Reader(Arc<ReaderState>),
}

impl StateView {
    fn core(&self) -> &ManifestCore {
        match &self.0 {
            ViewState::Writer(view) => view.core(),
            ViewState::Reader(state) => state.core(),
        }
    }
}

impl fmt::Debug for StateView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match &self.0 {
            ViewState::Writer(_) => "writer",
            ViewState::Reader(_) => "reader",
        };
        f.debug_struct("StateView")
            .field("kind", &kind)
            .field("last_l0_seq", &self.core().last_l0_seq)
            .finish()
    }
}

/// Iterator returned by [`Host::scan_view`] over SlateDB's normal read path.
pub struct ViewIterator {
    inner: DbIterator,
}

impl fmt::Debug for ViewIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewIterator").finish_non_exhaustive()
    }
}

impl ViewIterator {
    /// Returns the next resolved key-value pair.
    pub async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error> {
        self.inner.next().await
    }

    /// Returns the next resolved [`RowEntry`], preserving
    /// [`ValueDeletable::Merge`](crate::ValueDeletable::Merge) on unbased merges.
    pub async fn next_entry(&mut self) -> Result<Option<RowEntry>, crate::Error> {
        self.inner.next_entry().await.map_err(Into::into)
    }

    /// Seeks forward to the first key at or after `next_key`.
    pub async fn seek<K: AsRef<[u8]>>(&mut self, next_key: K) -> Result<(), crate::Error> {
        self.inner.seek(next_key).await
    }
}

/// Retention snapshot returned by [`Host::snapshot`].
///
/// On [`Db`](crate::Db), keeps every version visible at [`Self::seq`] alive across flush and
/// compaction. On [`DbReader`](crate::DbReader), holds the state and pins any managed checkpoint.
pub struct HostSnapshot(HostSnapshotKind);

enum HostSnapshotKind {
    Writer(Arc<DbSnapshot>),
    Reader(ReaderSnapshotGuard),
}

impl fmt::Debug for HostSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostSnapshot")
            .field("seq", &self.seq())
            .finish()
    }
}

impl HostSnapshot {
    /// Returns the sequence number the snapshot reads at.
    pub fn seq(&self) -> u64 {
        match &self.0 {
            HostSnapshotKind::Writer(snapshot) => snapshot.seq(),
            HostSnapshotKind::Reader(snapshot) => snapshot.seq,
        }
    }
}
