//! Pluggable row cache interface.
//!
//! A [`RowCache`] caches logical MVCC rows and key ranges above SlateDB's LSM tree. It observes
//! committed writes on the batch writer task (or, on a [`DbReader`](crate::DbReader), replayed
//! WAL writes on the manifest poller task), serves committed point reads and ascending scans
//! synchronously from local state, and falls back to SlateDB's normal read path on misses.

use std::future::Future;
use std::num::NonZeroU64;
use std::ops::Bound;
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
use crate::db_state::{DbStateView, SsTableHandle};
use crate::db_stats::DbStats;
use crate::db_status::{ClosedResultWriter, DbStatusManager};
use crate::error::SlateDBError;
use crate::manifest::{ManifestCore, VersionedManifest};
use crate::merge_operator::MergeOperatorType;
use crate::oracle::{DbOracle, DbReaderOracle, Oracle};
use crate::read_trace::ReadTrace;
use crate::reader::{DbStateReader, ScanContext};
use crate::sst_reader::SstIndex;
use crate::types::{KeyValue, RowEntry};
use crate::utils::SafeSender;

/// Opaque inline token returned on a sampled miss and passed back to [`RowCache::record_miss`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MissToken(pub NonZeroU64);

/// Result of [`RowCache::get`].
pub enum GetOutcome {
    /// The row cache answered the point read.
    Hit(Result<Option<KeyValue>, crate::Error>),
    /// The row cache did not serve the read. SlateDB runs the normal read path and passes any
    /// returned token to [`RowCache::record_miss`] after the read finishes.
    Miss(Option<MissToken>),
}

/// Result of [`RowCache::scan`].
pub enum ScanOutcome {
    /// The row cache answered the scan with an ascending iterator.
    Hit(Result<Box<dyn ScanIterator>, crate::Error>),
    /// The row cache did not serve the scan. SlateDB runs the normal scan path and passes any
    /// returned token to [`RowCache::record_miss`] after the scan opens.
    Miss(Option<MissToken>),
}

/// Termination state of [`ScanIterator::next_batch_sync`].
pub enum BatchStatus {
    /// Appended rows synchronously; more rows may follow.
    More,
    /// Reached the end of the iterator.
    Done,
    /// The next row requires an async poll through [`ScanIterator::next`].
    NeedsAsync,
    /// Encountered an error after appending rows.
    Error(crate::Error),
}

/// Result of [`ScanIterator::next_batch_sync`].
pub struct BatchStep {
    /// Number of rows appended to the caller's buffer before `status`.
    pub count: usize,
    /// State after appending `count` rows.
    pub status: BatchStatus,
}

/// Pre-resolved ascending scan iterator returned by [`RowCache::scan`].
///
/// Implementations yield unique non-tombstone rows in ascending key order within the requested
/// range, with sequence filtering and merge operands already resolved.
pub trait ScanIterator: Send + Sync + 'static {
    /// Returns the next row without awaiting, or `None` when the next row needs async I/O.
    fn next_sync(&mut self) -> Option<Result<Option<KeyValue>, crate::Error>>;

    /// Appends up to `max` rows to `out` synchronously.
    ///
    /// The default implementation loops over [`Self::next_sync`]. Implementations can override
    /// this method to avoid per-row dynamic dispatch during [`DbIterator::next_batch`].
    fn next_batch_sync(&mut self, out: &mut Vec<KeyValue>, max: usize) -> BatchStep {
        let mut count = 0;
        while count < max {
            match self.next_sync() {
                Some(Ok(Some(kv))) => {
                    out.push(kv);
                    count += 1;
                }
                Some(Ok(None)) => {
                    return BatchStep {
                        count,
                        status: BatchStatus::Done,
                    };
                }
                Some(Err(err)) => {
                    return BatchStep {
                        count,
                        status: BatchStatus::Error(err),
                    };
                }
                None => {
                    return BatchStep {
                        count,
                        status: BatchStatus::NeedsAsync,
                    };
                }
            }
        }
        BatchStep {
            count,
            status: BatchStatus::More,
        }
    }

    /// Returns the next row asynchronously when [`Self::next_sync`] returns `None`.
    fn next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<KeyValue>, crate::Error>> + Send + '_>>;

    /// Seeks forward to the first key at or after `next_key`.
    fn seek<'a>(
        &'a mut self,
        next_key: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::Error>> + Send + 'a>>;
}

/// Opaque snapshot of SlateDB's in-memory and manifest state.
///
/// Created by [`Host::capture_state_view`] and passed to [`Host::scan_view`],
/// [`Host::is_memtable_current`], and [`Host::retains_seq`].
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

/// Iterator returned by [`Host::scan_view`] over SlateDB's normal read path.
///
/// Wraps [`DbIterator`] and exposes [`Self::next_entry`] so range loaders can distinguish plain
/// values from unbased merge operands.
pub struct ViewIterator {
    inner: DbIterator,
}

impl ViewIterator {
    /// Returns the next resolved key-value pair.
    pub async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error> {
        self.inner.next().await
    }

    /// Returns the next resolved [`RowEntry`], preserving `ValueDeletable::Merge` on unbased
    /// merge results.
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
/// On a [`Db`](crate::Db), flush and compaction keep every version visible at [`Self::seq`]
/// while it lives. On a [`DbReader`](crate::DbReader), it holds the current state and, with a
/// managed checkpoint, keeps that state's checkpoint alive until it drops.
pub struct HostSnapshot(HostSnapshotKind);

enum HostSnapshotKind {
    Writer(Arc<DbSnapshot>),
    Reader(ReaderSnapshotGuard),
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

/// Handle passed to a [`RowCache`] for reading SlateDB state and requesting drains on the task
/// that owns the cache's write side: the batch writer, or a reader's manifest poller.
///
/// Hot-path methods read direct references to cycle-free components without upgrading a `Weak`
/// pointer. Cold-path methods upgrade a weak reference to the database so holding `Host` inside a
/// [`RowCache`] creates no reference cycle.
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

    /// Returns the newest committed sequence number (on a reader, the durable seq it serves).
    #[inline]
    pub fn last_committed_seq(&self) -> u64 {
        match &self.kind {
            HostKind::Writer { oracle, .. } => oracle.last_committed_seq(),
            HostKind::Reader { oracle, .. } => oracle.last_committed_seq(),
        }
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

    /// Schedules a [`RowCache::drain_writer`] call on the task that owns the cache's write side
    /// without blocking.
    ///
    /// Returns `false` if that task has stopped.
    pub fn request_drain(&self) -> bool {
        match &self.kind {
            HostKind::Writer { write_notifier, .. } => write_notifier
                .send(BatchWriterMessage::DrainRowCache)
                .is_ok(),
            HostKind::Reader { wake_tx, .. } => {
                wake_tx.try_send(DbReaderMessage::DrainRowCache).is_ok()
            }
        }
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

    /// Captures a consistent view of the active memtable, immutable memtables, and manifest.
    /// On a reader, this is the newest state the poller built, which holds every applied write.
    pub fn capture_state_view(&self) -> Result<StateView, crate::Error> {
        let view = match self.upgrade()? {
            Upgraded::Writer(inner) => ViewState::Writer(inner.state.read().view()),
            Upgraded::Reader(inner) => ViewState::Reader(inner.row_cache_view()),
        };
        Ok(StateView(view))
    }

    /// Returns `true` if `view` still points to the database's current active memtable.
    /// On a reader, this holds while `view` is the newest state the poller built.
    pub fn is_memtable_current(&self, view: &StateView) -> bool {
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
    /// This holds when no flushed L0 SST contains versions newer than `seq`, or, on a
    /// [`Db`](crate::Db) only, when an active snapshot or transaction (or one of its open scan
    /// iterators) retains a sequence number at or below `seq`. The writer's compactor does not
    /// know a reader's snapshots.
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

    /// Scans `range` directly against `view` on SlateDB's normal read path, bypassing the row
    /// cache.
    ///
    /// `max_seq` bounds the versions the scan sees; `None` reads at the newest committed
    /// sequence number.
    pub async fn scan_view<T>(
        &self,
        view: &StateView,
        range: T,
        options: &ScanOptions,
        prefix: Option<Bytes>,
        max_seq: Option<u64>,
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
                        options,
                        ScanContext {
                            db_state: state,
                            write_batch_iter: None,
                            max_seq,
                            prefix,
                        },
                    )
                    .await?
            }
            (Upgraded::Reader(inner), ViewState::Reader(state)) => {
                // The view may lead the reader's seq, which would clamp `max_seq`; every row of
                // a reader state is durable.
                let max_seq = max_seq.unwrap_or_else(|| self.last_committed_seq());
                let options = ScanOptions {
                    durability_filter: DurabilityLevel::Memory,
                    dirty: true,
                    ..options.clone()
                };
                inner
                    .reader
                    .scan_with_options(
                        range,
                        &options,
                        ScanContext {
                            db_state: state.as_ref(),
                            write_batch_iter: None,
                            max_seq: Some(max_seq),
                            prefix,
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

    /// Opens a retention snapshot at the current committed sequence number. On a reader, this
    /// waits while the poller replaces the current checkpoint.
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

    /// Reads the zero-copy block index of `handle` through the database's table store and block
    /// cache.
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

/// Startup context passed to [`RowCache::open`] after WAL replay finishes.
pub struct OpenContext {
    /// Handle for reading SlateDB state and requesting write-side drains.
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
    pub merge_operator: Option<MergeOperatorType>,
    /// Failpoint registry configured on the database.
    pub fp_registry: Arc<FailPointRegistry>,
    /// True if a compaction filter supplier is configured on the embedded compactor (never on a
    /// reader).
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

/// Pluggable logical row cache attached through
/// [`DbBuilder::with_row_cache`](crate::DbBuilder::with_row_cache) or
/// [`DbReaderBuilder::with_row_cache`](crate::DbReaderBuilder::with_row_cache).
///
/// On a [`DbReader`](crate::DbReader), the manifest poller task plays the batch writer's role,
/// feeding the cache replayed WAL writes one batch per sequence number in ascending order.
pub trait RowCache: Send + Sync + 'static {
    /// Initializes the cache after the database finishes WAL replay.
    ///
    /// Returning an error aborts database open and closes internal tasks cleanly.
    fn open(&self, ctx: OpenContext) -> Result<(), crate::Error>;

    /// Stops background cache tasks during [`Db::close`](crate::Db::close) or
    /// [`DbReader::close`](crate::DbReader::close).
    ///
    /// SlateDB calls this before stopping the batch writer (or reader poller) task so in-flight
    /// background jobs can finish writer handshakes cleanly.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Applies one write batch on the single batch writer task.
    ///
    /// Runs after memtable insertion and strictly before the committed sequence number advances.
    /// `committed_seq` is the committed sequence number before this batch commits.
    fn apply_write(&self, entries: &[RowEntry], committed_seq: u64);

    /// Drains queued control work on the single batch writer task after [`Host::request_drain`].
    ///
    /// `committed_seq` is the current committed sequence number with no write batch in flight.
    /// A reader's poller also calls it with a new state's seq when its last fed batch lies below.
    fn drain_writer(&self, committed_seq: u64);

    /// Drops every cached row above `sealed_at` on a [`DbReader`](crate::DbReader) whose poller
    /// moves, before its seq advances, to a state whose L0 holds writes up to `new_seq` that it
    /// never replayed. Reads at or below `sealed_at` stay valid; later batches chain from
    /// `new_seq`. A flush or compaction of replayed writes calls nothing.
    ///
    /// The default implementation does nothing, which suits caches that never serve reads.
    fn invalidate_all(&self, _sealed_at: u64, _new_seq: u64) {}

    /// Notifies the cache on the batch writer task that the writer is stopping (close or fence).
    /// On a reader, the manifest poller calls it as it stops.
    ///
    /// Runs before the batch writer fails remaining queued write requests. Implementations must
    /// stop serving and refuse queued control requests so waiters do not deadlock.
    fn close_writer(&self);

    /// Serves a point read synchronously at `at` (snapshot or transaction sequence number) or at
    /// the latest committed state when `at` is `None`.
    fn get(&self, key: &[u8], options: &ReadOptions, at: Option<u64>) -> GetOutcome;

    /// Serves a scan synchronously at `at` (snapshot or transaction sequence number) or at the
    /// latest committed state when `at` is `None`.
    fn scan(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        options: &ScanOptions,
        prefix: Option<&[u8]>,
        at: Option<u64>,
    ) -> ScanOutcome;

    /// Records completion of a normal-path read or scan that returned `Some(token)` on miss.
    fn record_miss(&self, token: MissToken, key: &[u8]);
}

/// Returns the starting key of `range` for miss reporting.
pub(crate) fn scan_start_key(range: &BytesRange) -> Bytes {
    match std::ops::RangeBounds::start_bound(range) {
        Bound::Included(key) | Bound::Excluded(key) => key.clone(),
        Bound::Unbounded => Bytes::new(),
    }
}
