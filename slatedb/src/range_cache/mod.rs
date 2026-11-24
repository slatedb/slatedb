//! Local key-range cache for point reads and ascending scans.
//!
//! A *cached range* covers a half-open key interval `[start, end)` completely, so point reads in
//! it are definite hits or definite misses and scans need no other source. Each range has one
//! layer set, newest first: an active write buffer, sealed buffers awaiting flush, up to
//! [`RangeCacheTuning::max_runs`] run files (indexed by one sorted delta view for scans and
//! Bloom filters for point reads), and a base file holding live rows at `base_seq`.
//!
//! Only the batch writer mutates the range index and layer sets; loads, flushes, merges, and
//! removals apply at writer boundaries. Reads pin layers without locking and verify
//! `seq >= layers.base_seq` and `seq <= layers.active.sealed_at`. Read counts accumulate per
//! thread and fold periodically into shared metrics and range access timestamps.

mod buffer;
mod delta;
mod file;
mod filter;
mod hits;
mod maintain;
mod merge;
mod mixed;
mod scan;
pub mod stats;
mod view;
mod writer;

use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use fail_parallel::{fail_point, FailPointRegistry};
use log::warn;
use slatedb_common::clock::SystemClock;
use slatedb_common::metrics::MetricsRecorderHelper;

use crate::bytes_range::{ByteRangeBounds, BytesRange};
use crate::config::{DurabilityLevel, ReadOptions, ScanOptions};
use crate::db::{Db, DbInner};
use crate::db_iter::DbIterator;
use crate::db_snapshot::DbSnapshot;
use crate::error::SlateDBError;
use crate::iter::{EmptyIterator, IterationOrder, RowEntryIterator};
use crate::merge_operator::MergeOperatorType;
use crate::oracle::Oracle;
use crate::read_trace::ReadTrace;
use crate::reader::ScanContext;
use crate::types::{KeyValue, ValueDeletable};

use buffer::Kind;
use delta::{Delta, DeltaIterator, DeltaMemory, DeltaShape};
pub use file::RangeCachePageSource;
use file::{
    FileBudget, FileRead, RangeFile, RangeFileIterator, RangeFileWriter, FILE_EXTENSION,
    TEMP_FILE_EXTENSION,
};
use filter::{hash_key, Filter};
use hits::{HitStats, RangeHits};
pub(crate) use maintain::{Maintainer, RANGE_CACHE_MAINTENANCE_TASK_NAME};
use mixed::{GapSource, MixedScan, Part, PartKind};
use scan::CachedScan;
use stats::{PinOp, RangeCacheStats, ReadCount, RemoveReason, Unfit};
use view::{DeltaView, ViewMemory};
use writer::{Padded, Target, WriterQueue};

/// Max attempts to pin a range before falling back to the normal read path.
const PIN_ATTEMPTS: usize = 3;

/// Valid range for [`RangeCacheTuning::max_runs`] (one selector slot per run in a delta view).
pub(crate) const MAX_RUNS_RANGE: std::ops::RangeInclusive<usize> = 4..=view::VIEW_SLOTS;

/// Options for the range cache. See [`crate::DbBuilder::with_range_cache`].
///
/// The range cache is experimental and its API may change. Build options with
/// [`RangeCacheOptions::new`] and override fields as needed.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RangeCacheOptions {
    /// Directory owned by the range cache. Stale cache files are removed on open.
    pub root_folder: PathBuf,
    /// Max disk bytes of range cache files. When exceeded, least recently read ranges are evicted.
    /// Loads larger than this budget are rejected.
    pub max_disk_bytes: u64,
    /// How the cache reads range cache files.
    pub page_source: RangeCachePageSource,
    /// Internal tuning parameters. See [`RangeCacheTuning`].
    #[doc(hidden)]
    pub tuning: RangeCacheTuning,
}

impl RangeCacheOptions {
    /// Returns options with default values and the given directory.
    pub fn new(root_folder: impl Into<PathBuf>) -> Self {
        Self {
            root_folder: root_folder.into(),
            max_disk_bytes: 1024 * 1024 * 1024,
            page_source: RangeCachePageSource::default(),
            tuning: RangeCacheTuning::default(),
        }
    }
}

/// Internal tuning for the range cache, intended for tests and benchmarks.
#[doc(hidden)]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RangeCacheTuning {
    /// File page size in bytes. Must be a power of two from 512 to 65536.
    pub page_size: usize,
    /// Minimum run bytes required before a major merge starts. `usize::MAX` disables major merges.
    pub min_rebuild_delta_bytes: usize,
    /// Starts a major merge when run bytes reach `major_merge_factor * base_bytes` (and
    /// `min_rebuild_delta_bytes`). Must be positive and finite.
    pub major_merge_factor: f64,
    /// Max run files per range, from 4 to 64. Flushes at the cap merge newest runs to stay within
    /// this limit.
    pub max_runs: usize,
    /// Chunk bytes at which the writer seals an active write buffer for flushing.
    pub range_seal_bytes: usize,
    /// Hard memory cap across all write buffers (chunks and filters). Writes exceeding this cap
    /// evict ranges instead of blocking.
    pub max_recent_bytes: u64,
    /// Minimum active buffer size eligible for early sealing under memory pressure.
    pub min_seal_bytes: usize,
    /// Chunk bytes threshold at which a write buffer allocates a Bloom filter. `usize::MAX`
    /// disables buffer filters.
    pub buffer_filter_bytes: usize,
    /// Target size for splitting large `cache_range` requests at sorted-run block boundaries. `0`
    /// disables splitting.
    pub range_target_bytes: u64,
    /// Heap metadata budget for fence indexes, filters, delta views, and layer sets. `None` uses
    /// `max(max_disk_bytes / 16, 64 MiB)`.
    pub max_meta_bytes: Option<u64>,
    /// Target read-ahead size for object store fetches during range loads.
    pub load_fetch_bytes: usize,
    /// Max concurrent fetch tasks per SST during range loads.
    pub load_fetch_tasks: usize,
    /// Read events per thread before folding counters into shared metrics and range hits. `0`
    /// counts as `1`.
    pub stats_fold_events: u32,
    /// Elapsed time before a reading thread folds its counters into shared state.
    pub stats_fold_interval: Duration,
}

impl Default for RangeCacheTuning {
    fn default() -> Self {
        Self {
            page_size: 4096,
            min_rebuild_delta_bytes: 1024 * 1024,
            major_merge_factor: 0.25,
            max_runs: 64,
            range_seal_bytes: 4 * 1024 * 1024,
            max_recent_bytes: 256 * 1024 * 1024,
            min_seal_bytes: 64 * 1024,
            buffer_filter_bytes: 128 * 1024,
            range_target_bytes: 64 * 1024 * 1024,
            max_meta_bytes: None,
            load_fetch_bytes: 2 * 1024 * 1024,
            load_fetch_tasks: 4,
            stats_fold_events: 4096,
            stats_fold_interval: Duration::from_millis(10),
        }
    }
}

impl RangeCacheTuning {
    fn delta_shape(&self) -> DeltaShape {
        DeltaShape {
            seal_bytes: self.range_seal_bytes as u64,
            filter_from: u64::try_from(self.buffer_filter_bytes).unwrap_or(u64::MAX),
        }
    }

    fn meta_limit(&self, max_disk_bytes: u64) -> u64 {
        self.max_meta_bytes
            .unwrap_or((max_disk_bytes / 16).max(64 * 1024 * 1024))
    }

    /// Buffer memory threshold above which new loads are rejected.
    fn high_water(&self) -> u64 {
        self.max_recent_bytes / 8 * 7
    }

    /// Buffer memory target after overflow removals.
    fn low_water(&self) -> u64 {
        self.max_recent_bytes / 4 * 3
    }

    /// Buffer memory threshold above which maintenance seals large active buffers early.
    fn pressure(&self) -> u64 {
        self.max_recent_bytes / 2
    }

    /// Scan options for range loads. Skips populating the block cache.
    fn bulk_scan_options(&self) -> ScanOptions {
        ScanOptions {
            read_ahead_bytes: self.load_fetch_bytes,
            max_fetch_tasks: self.load_fetch_tasks,
            cache_blocks: false,
            ..ScanOptions::default()
        }
    }
}

/// Snapshot of one cached range's state. See [`Db::cached_ranges`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CachedRangeInfo {
    /// Inclusive start key of the range.
    pub start: Bytes,
    /// Exclusive end key, or `None` if unbounded.
    pub end: Option<Bytes>,
    /// True while the initial load is still in progress.
    pub loading: bool,
    /// True while sealed buffers await flush or a major merge is due or running.
    pub rebuilding: bool,
    /// Size of the base file on disk in bytes.
    pub file_bytes: u64,
    /// Number of rows in the base file.
    pub file_rows: u64,
    /// Estimated size of unflushed writes in memory in bytes.
    pub delta_bytes: u64,
    /// Number of run files above the base.
    pub runs: usize,
    /// Total size of run files on disk in bytes.
    pub run_bytes: u64,
    /// Number of sealed write buffers awaiting flush.
    pub sealed_buffers: usize,
    /// Exact heap memory of active and sealed write buffers in bytes.
    pub buffer_bytes: u64,
}

/// One run of a range: a file of every version of the range with a sequence number in
/// `(lo, hi]`, its key bounds, and its filter on the heap.
pub(crate) struct Run {
    lo: u64,
    hi: u64,
    min_key: Bytes,
    max_key: Bytes,
    filter: Filter,
    file: Arc<RangeFile>,
}

impl Run {
    /// A run of the versions in `(lo, hi]` from a finished run file.
    fn new(mut file: RangeFile, lo: u64, hi: u64) -> io::Result<Self> {
        if !file.is_run() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "range cache run: not a run file",
            ));
        }
        let filter = file
            .take_filter()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "range cache run: filter"))?;
        let min_key = file.first_key().map(Bytes::copy_from_slice);
        let max_key = file.last_key()?;
        Ok(Self {
            lo,
            hi,
            min_key: min_key.unwrap_or_default(),
            max_key: max_key.unwrap_or_default(),
            filter,
            file: Arc::new(file),
        })
    }

    fn file_bytes(&self) -> u64 {
        self.file.file_len()
    }
}

/// Immutable layer set of a cached range, replaced atomically by the batch writer.
///
/// Cache-line aligned so `Arc` reference count updates on scans do not contend with read fields.
#[repr(align(128))]
pub(crate) struct Layers {
    /// The buffer that the writer appends to. Its `sealed_at` is the readers' validity word.
    active: Arc<Delta>,
    /// Sealed buffers that wait for their flush, newest first. Each one's `lo` is the seal of the
    /// next one, and the oldest one's `lo` is `covered_to`.
    sealed: Vec<Arc<Delta>>,
    /// Runs, newest first, at most [`RangeCacheTuning::max_runs`]: run `i` covers
    /// `(lo_i, hi_i]` with `hi_{i+1} = lo_i`, and the oldest one's `lo` is `base_seq`.
    runs: Vec<Arc<Run>>,
    /// The view of exactly `runs`, which scans read them through; `None` when there is no run.
    view: Option<Arc<DeltaView>>,
    /// `None` while the range loads.
    base: Option<Arc<RangeFile>>,
    /// The base holds the state of the range at this sequence number.
    base_seq: u64,
    /// The base and the runs hold every version of the range at or below this sequence number:
    /// the newest run's `hi`, or `base_seq`. The buffers hold every version above it.
    covered_to: u64,
    /// The end of the range, so that a scan can walk adjacent ranges with one reference to
    /// each.
    end: Option<Bytes>,
}

impl Layers {
    /// The buffers, newest first: the active one, then the sealed ones.
    fn buffers(&self) -> impl Iterator<Item = &Arc<Delta>> {
        std::iter::once(&self.active).chain(self.sealed.iter())
    }

    /// Validates layer invariants in debug builds.
    fn check(&self, max_runs: usize) {
        if !cfg!(debug_assertions) {
            return;
        }
        assert!(
            self.runs.len() <= max_runs,
            "a range holds {} runs",
            self.runs.len()
        );
        let mut to = self.covered_to;
        for run in &self.runs {
            assert_eq!(run.hi, to, "runs are not contiguous");
            assert!(run.lo < run.hi || run.lo == run.hi && run.file.rows() == 0);
            to = run.lo;
        }
        assert_eq!(to, self.base_seq, "the runs do not reach the base");
        assert_eq!(
            self.view.is_some(),
            !self.runs.is_empty(),
            "a range with runs and no view, or a view and no runs"
        );
        if let Some(view) = &self.view {
            let in_view = view.runs().count();
            assert_eq!(in_view, self.runs.len(), "the view does not hold the runs");
            assert!(
                self.runs.iter().all(|r| view.holds(r)),
                "the view holds other runs"
            );
            let rows: u64 = self.runs.iter().map(|r| r.file.rows()).sum();
            assert_eq!(
                view.len() as u64,
                rows,
                "the view does not list the runs' rows"
            );
        }
        let mut lo = self.covered_to;
        for buffer in self.sealed.iter().rev() {
            assert_eq!(buffer.lo(), lo, "sealed buffers are not contiguous");
            lo = buffer.sealed_at().load(Ordering::Acquire);
        }
        assert_eq!(
            self.active.lo(),
            lo,
            "the active buffer does not follow the sealed ones"
        );
    }
}

struct CachedRange {
    start: Bytes,
    end: Option<Bytes>,
    layers: ArcSwap<Layers>,
    /// Reusable slot index used by thread-local hit tracking.
    slot: u32,
    /// Folded read count and last access clock for LRU eviction.
    hits: Arc<RangeHits>,
    /// Sequence number of the last batch that wrote to this range.
    last_write_seq: Padded<AtomicU64>,
    /// Maintenance state for in-flight flush and major merge jobs.
    jobs: maintain::RangeJobs,
}

impl CachedRange {
    fn new(start: Bytes, end: Option<Bytes>, layers: Layers, slot: u32) -> Self {
        Self {
            start,
            end,
            layers: ArcSwap::from_pointee(layers),
            slot,
            hits: Arc::new(RangeHits::default()),
            last_write_seq: Padded(AtomicU64::new(0)),
            jobs: maintain::RangeJobs::default(),
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_ref() && self.end.as_ref().is_none_or(|end| key < end.as_ref())
    }

    /// Returns true if the range holds every key of `range`.
    fn contains_range(&self, range: &BytesRange) -> bool {
        match RangeBounds::end_bound(range) {
            Unbounded => self.end.is_none(),
            Included(k) => self.end.as_ref().is_none_or(|end| k < end),
            Excluded(k) => self.end.as_ref().is_none_or(|end| k <= end),
        }
    }

    fn bounds(&self) -> BytesRange {
        let end = match &self.end {
            Some(end) => Excluded(end.clone()),
            None => Unbounded,
        };
        BytesRange::new(Included(self.start.clone()), end)
    }

    fn info(&self, tuning: &RangeCacheTuning) -> CachedRangeInfo {
        let layers = self.layers.load();
        CachedRangeInfo {
            start: self.start.clone(),
            end: self.end.clone(),
            loading: layers.base.is_none(),
            rebuilding: !layers.sealed.is_empty()
                || self.jobs.merging()
                || maintain::major_due(self, &layers, tuning).is_some(),
            file_bytes: layers.base.as_ref().map_or(0, |f| f.file_len()),
            file_rows: layers.base.as_ref().map_or(0, |f| f.rows()),
            delta_bytes: self.delta_bytes(&layers),
            runs: layers.runs.len(),
            run_bytes: layers.runs.iter().map(|r| r.file_bytes()).sum(),
            sealed_buffers: layers.sealed.len(),
            buffer_bytes: layers.buffers().map(|b| b.memory_bytes()).sum(),
        }
    }

    /// The estimated bytes of the range's buffers.
    fn delta_bytes(&self, layers: &Layers) -> u64 {
        layers.buffers().map(|b| b.size() as u64).sum()
    }

    /// The memory of the range's buffers that no run holds yet.
    fn unflushed_bytes(&self) -> u64 {
        self.layers.load().buffers().map(|b| b.memory_bytes()).sum()
    }

    /// The bytes of the range's base and runs, which count against the disk budget.
    fn file_bytes(&self) -> u64 {
        let layers = self.layers.load();
        layers.base.as_ref().map_or(0, |f| f.file_len())
            + layers.runs.iter().map(|r| r.file_bytes()).sum::<u64>()
    }

    /// The heap metadata of the range's files: the fence indexes and filters of its base and
    /// runs.
    fn files_meta_bytes(&self) -> u64 {
        let layers = self.layers.load();
        layers.base.as_ref().map_or(0, |f| f.meta_bytes())
            + layers.runs.iter().map(|r| r.file.meta_bytes()).sum::<u64>()
    }

    /// The heap metadata of the range, which counts against the metadata budget: the fence
    /// indexes and filters of its base and runs, and its layer set with its run handles.
    fn meta_bytes(&self) -> u64 {
        let files = self.files_meta_bytes();
        let layers = self.layers.load();
        let handles = size_of::<CachedRange>()
            + size_of::<Layers>()
            + (layers.sealed.capacity() + layers.runs.capacity()) * size_of::<usize>()
            + layers.runs.len() * size_of::<Run>();
        files + handles as u64 + self.view_bytes()
    }

    /// The heap bytes of the range's delta view, which are part of its metadata.
    fn view_bytes(&self) -> u64 {
        self.layers.load().view.as_ref().map_or(0, |v| v.bytes())
    }
}

/// Cached ranges by start key. Ranges do not overlap.
type RangeIndex = BTreeMap<Bytes, Arc<CachedRange>>;

/// Returns the range that holds `key`.
fn covering<'a>(index: &'a RangeIndex, key: &[u8]) -> Option<&'a Arc<CachedRange>> {
    index
        .range::<[u8], _>((Unbounded, Included(key)))
        .next_back()
        .map(|(_, range)| range)
        .filter(|range| range.contains(key))
}

/// Returns the parts of `[start, end)` that no range in `index` holds.
fn uncovered(
    index: &RangeIndex,
    start: &Bytes,
    end: Option<&Bytes>,
) -> Vec<(Bytes, Option<Bytes>)> {
    let before_end = |key: &Bytes| end.is_none_or(|end| key < end);
    let mut parts = Vec::new();
    // `None` means that ranges hold every key after the cursor.
    let mut cursor = Some(start.clone());
    let first = index
        .range::<[u8], _>((Unbounded, Included(start.as_ref())))
        .next_back();
    let rest = index.range::<[u8], _>((Excluded(start.as_ref()), Unbounded));
    for (_, range) in first.into_iter().chain(rest) {
        let Some(pos) = cursor.clone() else {
            break;
        };
        if !before_end(&range.start) {
            break;
        }
        if range
            .end
            .as_ref()
            .is_some_and(|range_end| range_end <= &pos)
        {
            continue;
        }
        if range.start > pos {
            parts.push((pos, Some(range.start.clone())));
        }
        cursor = range.end.clone();
    }
    if let Some(pos) = cursor {
        if before_end(&pos) {
            parts.push((pos, end.cloned()));
        }
    }
    parts
}

/// Converts `range` to a half-open range `[start, end)`. Returns `None` if it holds no key.
fn half_open(range: &BytesRange) -> Option<(Bytes, Option<Bytes>)> {
    let after = |key: &Bytes| {
        let mut next = key.to_vec();
        next.push(0);
        Bytes::from(next)
    };
    let start = match RangeBounds::start_bound(range) {
        Unbounded => Bytes::new(),
        Included(k) => k.clone(),
        Excluded(k) => after(k),
    };
    let end = match RangeBounds::end_bound(range) {
        Unbounded => None,
        Included(k) => Some(after(k)),
        Excluded(k) => Some(k.clone()),
    };
    match &end {
        Some(end) if *end <= start => None,
        _ => Some((start, end)),
    }
}

/// Range cache state for one database.
pub(crate) struct RangeCache {
    options: RangeCacheOptions,
    index: ArcSwap<RangeIndex>,
    writer: WriterQueue,
    files: Arc<FileBudget>,
    delta_memory: Arc<DeltaMemory>,
    views: Arc<ViewMemory>,
    maintenance: maintain::MaintenanceQueue,
    next_file_id: AtomicU64,
    hits: HitStats,
    stats: RangeCacheStats,
    handle: tokio::runtime::Handle,
    merge_operator: Option<MergeOperatorType>,
    #[allow(dead_code)]
    fp_registry: Arc<FailPointRegistry>,
}

impl RangeCache {
    /// Creates the cache directory if missing and removes stale cache files.
    pub(crate) fn open(
        options: RangeCacheOptions,
        recorder: &MetricsRecorderHelper,
        handle: tokio::runtime::Handle,
        merge_operator: Option<MergeOperatorType>,
        fp_registry: Arc<FailPointRegistry>,
        clock: Arc<dyn SystemClock>,
    ) -> Result<Arc<Self>, SlateDBError> {
        if !file::valid_page_size(options.tuning.page_size) {
            return Err(SlateDBError::InvalidConfiguration(format!(
                "range cache page size {} is not a power of two in 512..=65536",
                options.tuning.page_size
            )));
        }
        if options.tuning.load_fetch_bytes == 0 || options.tuning.load_fetch_tasks == 0 {
            return Err(SlateDBError::InvalidConfiguration(
                "range cache load_fetch_bytes and load_fetch_tasks must be positive".to_string(),
            ));
        }
        if !MAX_RUNS_RANGE.contains(&options.tuning.max_runs) {
            return Err(SlateDBError::InvalidConfiguration(format!(
                "range cache max_runs {} is not in {}..={}",
                options.tuning.max_runs,
                MAX_RUNS_RANGE.start(),
                MAX_RUNS_RANGE.end()
            )));
        }
        let f = options.tuning.major_merge_factor;
        if !f.is_finite() || f <= 0.0 {
            return Err(SlateDBError::InvalidConfiguration(format!(
                "range cache major_merge_factor {f} is not positive and finite"
            )));
        }
        std::fs::create_dir_all(&options.root_folder)?;
        for entry in std::fs::read_dir(&options.root_folder)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let is_cache_file = [FILE_EXTENSION, TEMP_FILE_EXTENSION].iter().any(|ext| {
                name.strip_suffix(ext)
                    .and_then(|stem| stem.strip_suffix('.'))
                    .is_some_and(|stem| {
                        !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit())
                    })
            });
            if is_cache_file {
                std::fs::remove_file(&path)?;
            }
        }
        let hits = HitStats::new(
            stats::read_counters(recorder),
            options.tuning.stats_fold_events,
            options.tuning.stats_fold_interval,
            clock,
        );
        Ok(Arc::new(Self {
            options,
            index: ArcSwap::from_pointee(RangeIndex::new()),
            writer: WriterQueue::new(),
            files: Arc::new(FileBudget::default()),
            delta_memory: DeltaMemory::new(recorder.gauge(stats::DELTA_ALLOCATED_BYTES).register()),
            views: Arc::new(ViewMemory::default()),
            maintenance: maintain::MaintenanceQueue::new(),
            next_file_id: AtomicU64::new(0),
            hits,
            stats: RangeCacheStats::new(recorder),
            handle,
            merge_operator,
            fp_registry,
        }))
    }

    fn new_buffer(&self, lo: u64) -> Arc<Delta> {
        Arc::new(Delta::new(
            &self.delta_memory,
            lo,
            self.options.tuning.delta_shape(),
        ))
    }

    fn new_file_name(&self) -> String {
        let file_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        format!("{file_id:020}")
    }

    fn is_indexed(&self, range: &Arc<CachedRange>) -> bool {
        self.index
            .load()
            .get(&range.start)
            .is_some_and(|r| Arc::ptr_eq(r, range))
    }

    /// Returns eviction candidates in least-recently-read order when budgets are exceeded.
    fn eviction_order(&self, skip: Option<&Arc<CachedRange>>) -> Vec<Arc<CachedRange>> {
        if self.within_budgets() {
            return Vec::new();
        }
        let mut candidates: Vec<_> = self
            .index
            .load()
            .values()
            .filter(|r| r.layers.load().base.is_some())
            .filter(|r| skip.is_none_or(|skip| !Arc::ptr_eq(skip, r)))
            .cloned()
            .collect();
        candidates.sort_by_key(|r| r.hits.last_access());
        candidates
    }

    fn update_gauges(&self) {
        let index = self.index.load();
        self.stats.ranges.set(index.len() as i64);
        let delta_bytes: u64 = index
            .values()
            .map(|r| r.delta_bytes(&r.layers.load()))
            .sum();
        self.stats.delta_bytes.set(delta_bytes as i64);
        self.delta_memory.publish();
        self.stats.file_bytes.set(self.files.disk() as i64);
        // Files and views count while anything holds them; layer sets while they are indexed.
        let layers: u64 = index
            .values()
            .map(|r| {
                r.meta_bytes()
                    .saturating_sub(r.files_meta_bytes() + r.view_bytes())
            })
            .sum();
        let views = self.views.bytes();
        self.stats.view_bytes.set(views as i64);
        self.stats
            .meta_bytes
            .set((self.files.meta() + views + layers) as i64);
    }

    /// Returns true if the cached ranges fit the disk budget and the metadata budget. Only
    /// indexed ranges count: files that only a reader holds go when the reader drops them.
    fn within_budgets(&self) -> bool {
        let index = self.index.load();
        let disk: u64 = index.values().map(|r| r.file_bytes()).sum();
        if disk > self.options.max_disk_bytes {
            return false;
        }
        let meta: u64 = index.values().map(|r| r.meta_bytes()).sum();
        meta <= self.options.tuning.meta_limit(self.options.max_disk_bytes)
    }

    /// Checks whether a read at `seq` can use `layers`. See the sequence number rules. Callers
    /// treat a loading range, which has no base, as uncovered before they call this.
    fn usable(layers: &Layers, seq: u64) -> Result<(), Unfit> {
        if layers.base.is_none() || seq < layers.base_seq {
            return Err(Unfit::TooOld);
        }
        if layers.active.sealed_at().load(Ordering::SeqCst) < seq {
            return Err(Unfit::Sealed);
        }
        Ok(())
    }

    fn count_read(&self, range: &CachedRange, count: ReadCount) {
        self.hits.read(range.slot, &range.hits, count);
    }

    /// Records an unfit pin attempt and returns true if the read should retry (sealed delta only).
    fn count_unfit(&self, op: PinOp, reason: Unfit, attempt: usize) -> bool {
        let retry = reason == Unfit::Sealed && attempt < PIN_ATTEMPTS;
        self.hits.count(if retry {
            ReadCount::retry(op, reason)
        } else {
            ReadCount::unusable(op, reason)
        });
        retry
    }

    /// Reads `key` at the last committed sequence number (or `at` for snapshots/transactions)
    /// without mutating shared cache lines on the fast path.
    pub(crate) fn get(&self, key: &[u8], oracle: &dyn Oracle, at: Option<u64>) -> CacheRead {
        for attempt in 1..=PIN_ATTEMPTS {
            let index = self.index.load();
            let Some(cached) = covering(&index, key) else {
                return CacheRead::Uncovered;
            };
            let layers = cached.layers.load();
            if layers.base.is_none() {
                return CacheRead::Uncovered;
            }
            fail_point!(Arc::clone(&self.fp_registry), "range-cache-pin");
            let seq = read_seq(oracle, at);
            if let Err(reason) = Self::usable(&layers, seq) {
                if self.count_unfit(PinOp::Get, reason, attempt) {
                    continue;
                }
                return CacheRead::Unusable;
            }
            return match lookup(&layers, key, seq) {
                Ok(Found::Value(kv)) => {
                    self.count_read(cached, ReadCount::GetHit);
                    CacheRead::Answer(Some(kv))
                }
                Ok(Found::Absent) => {
                    self.count_read(cached, ReadCount::GetAbsent);
                    CacheRead::Answer(None)
                }
                Ok(Found::Merge) => {
                    self.count_read(cached, ReadCount::GetMerge);
                    CacheRead::Resolve(Pinned {
                        layers: arc_swap::Guard::into_inner(layers),
                        seq,
                    })
                }
                Err(e) => {
                    warn!("range cache read failed [error={:?}]", e);
                    self.request_removal(Target::Range(cached.clone()), RemoveReason::Error);
                    CacheRead::Uncovered
                }
            };
        }
        CacheRead::Unusable
    }

    /// Pins layers for a fully cached scan across one or more adjacent ranges at one sequence
    /// number.
    pub(crate) fn pin_scan(
        &self,
        range: &BytesRange,
        oracle: &dyn Oracle,
        at: Option<u64>,
    ) -> ScanPin {
        match self.try_pin_scan(range, oracle, at) {
            Some(Ok(pinned)) => ScanPin::Pinned(pinned),
            Some(Err(())) => ScanPin::Unusable,
            None => ScanPin::Uncovered,
        }
    }

    /// See [`RangeCache::pin_scan`]. Returns `None` if ready ranges do not hold all of `range`,
    /// and `Some(Err(()))` if they do but fail the sequence number rules.
    fn try_pin_scan(
        &self,
        range: &BytesRange,
        oracle: &dyn Oracle,
        at: Option<u64>,
    ) -> Option<Result<PinnedScan, ()>> {
        if self.index.load().is_empty() {
            return None;
        }
        // The first key of the scan. `BytesRange` is never empty, so only an excluded start
        // needs a new key; the common cases borrow it, without an allocation.
        let excluded_start;
        let start: &[u8] = match RangeBounds::start_bound(range) {
            Unbounded => &[],
            Included(key) => key,
            Excluded(_) => {
                excluded_start = half_open(range)?.0;
                &excluded_start
            }
        };
        for attempt in 1..=PIN_ATTEMPTS {
            let index = self.index.load();
            let first = covering(&index, start)?;
            let mut parts = vec![first.layers.load_full()];
            let mut last = first;
            while !last.contains_range(range) {
                last = index.get(last.end.as_ref()?)?;
                parts.push(last.layers.load_full());
            }
            if parts.iter().any(|l| l.base.is_none()) {
                return None;
            }
            fail_point!(Arc::clone(&self.fp_registry), "range-cache-pin");
            let seq = read_seq(oracle, at);
            if let Some(first) = parts.iter().find_map(|l| Self::usable(l, seq).err()) {
                // One part that is too old makes every attempt fail.
                let too_old = first == Unfit::TooOld
                    || parts
                        .iter()
                        .any(|l| Self::usable(l, seq) == Err(Unfit::TooOld));
                let reason = if too_old { Unfit::TooOld } else { first };
                if self.count_unfit(PinOp::Scan, reason, attempt) {
                    continue;
                }
                return Some(Err(()));
            }
            // Only the first range counts as read: most scans end in it.
            self.count_read(first, ReadCount::Scan);
            return Some(Ok(PinnedScan { parts, seq }));
        }
        Some(Err(()))
    }

    /// Plans a mixed scan of cached parts and gaps at one sequence number. Returns `None` if no
    /// usable cached range intersects `range`.
    fn pin_mixed(
        &self,
        range: &BytesRange,
        oracle: &dyn Oracle,
        at: Option<u64>,
    ) -> Option<MixedPin> {
        let (start, end) = half_open(range)?;
        let index = self.index.load();
        let intersecting: Vec<&Arc<CachedRange>> = covering(&index, &start)
            .into_iter()
            .chain(
                index
                    .range::<[u8], _>((Excluded(start.as_ref()), Unbounded))
                    .map(|(_, r)| r)
                    .take_while(|r| end.as_ref().is_none_or(|end| r.start < *end)),
            )
            .collect();
        if intersecting.is_empty() {
            return None;
        }
        let pinned: Vec<Arc<Layers>> = intersecting.iter().map(|r| r.layers.load_full()).collect();
        fail_point!(Arc::clone(&self.fp_registry), "range-cache-pin");
        let seq = read_seq(oracle, at);

        let scan_end = RangeBounds::end_bound(range).cloned();
        let mut parts = Vec::with_capacity(2 * intersecting.len() + 1);
        let mut push = |kind, start: std::ops::Bound<Bytes>, end: std::ops::Bound<Bytes>| {
            if BytesRange::try_new(start.clone(), end.clone()).is_some() {
                parts.push(Part { kind, start, end });
            }
        };
        let mut cursor = Some(RangeBounds::start_bound(range).cloned());
        let mut first_cached: Option<&Arc<CachedRange>> = None;
        for (cached, layers) in intersecting.iter().zip(pinned) {
            let Some(from) = cursor.take() else {
                break;
            };
            // The gap before this range, if any.
            let before = match &from {
                Unbounded => !cached.start.is_empty(),
                Included(key) | Excluded(key) => *key < cached.start,
            };
            let from = if before {
                push(PartKind::Gap, from, Excluded(cached.start.clone()));
                Included(cached.start.clone())
            } else {
                from
            };
            let to = scan::min_end(&scan_end, &cached.end);
            let kind = if layers.base.is_none() {
                PartKind::Gap
            } else {
                match Self::usable(&layers, seq) {
                    Ok(()) => {
                        first_cached.get_or_insert(*cached);
                        PartKind::Cached(layers)
                    }
                    Err(reason) => {
                        self.hits
                            .count(ReadCount::unusable(PinOp::ScanPart, reason));
                        PartKind::Gap
                    }
                }
            };
            push(kind, from, to);
            cursor = cached.end.clone().map(Included);
        }
        if let Some(from) = cursor {
            push(PartKind::Gap, from, scan_end);
        }
        let first = first_cached?.clone();
        Some(MixedPin { parts, seq, first })
    }

    pub(crate) fn ranges(&self) -> Vec<CachedRangeInfo> {
        let tuning = &self.options.tuning;
        self.index.load().values().map(|r| r.info(tuning)).collect()
    }

    pub(crate) fn buffer_memory(&self) -> u64 {
        self.delta_memory.bytes()
    }

    /// Streams rows of a loading range into a base file, publishes it, and evicts if over budget.
    async fn load(
        &self,
        range: &Arc<CachedRange>,
        mut iter: DbIterator,
        file_seq: u64,
        check_open: &(dyn Fn() -> Result<(), SlateDBError> + Send + Sync),
    ) -> Result<(), SlateDBError> {
        fail_point!(Arc::clone(&self.fp_registry), "range-cache-load");
        let mut writer = RangeFileWriter::create(
            &self.options.root_folder,
            &self.new_file_name(),
            self.options.tuning.page_size,
        )?;
        while let Some(entry) = iter.next_entry().await? {
            if entry.expire_ts.is_some() {
                return Err(SlateDBError::RangeCacheLoadRejected(
                    "the range holds rows with an expire time",
                ));
            }
            writer.push(&entry)?;
            if writer.rows().is_multiple_of(1024) {
                check_open()?;
            }
            if writer.estimated_len() > self.options.max_disk_bytes {
                return Err(SlateDBError::RangeCacheLoadRejected(
                    "the range is larger than the disk budget",
                ));
            }
        }
        let rows = writer.rows();
        let path = writer.finish()?;
        let file = Arc::new(RangeFile::open(
            path,
            self.options.page_source,
            self.files.clone(),
        )?);
        debug_assert_eq!(rows, file.rows());

        if self.attach_base(range, file, file_seq).await {
            self.evict(None).await;
        }
        Ok(())
    }
}

/// Returns the read sequence number, clamped to the last committed sequence number.
fn read_seq(oracle: &dyn Oracle, at: Option<u64>) -> u64 {
    let committed = oracle.last_committed_seq();
    at.map_or(committed, |at| at.min(committed))
}

/// Result of [`RangeCache::get`].
pub(crate) enum CacheRead {
    /// No ready range holds the key, or the cache hit an error.
    Uncovered,
    /// A ready range holds the key, but failed sequence number checks.
    Unusable,
    /// Resolved value or definite miss at the read sequence number.
    Answer(Option<KeyValue>),
    /// Newest version is a merge operand; resolve through [`Pinned::get`].
    Resolve(Pinned),
}

/// The layers of one range, pinned at one sequence number.
pub(crate) struct Pinned {
    layers: Arc<Layers>,
    seq: u64,
}

impl Pinned {
    /// Reads `key` through the normal iterators. This resolves merge operands.
    pub(crate) async fn get(
        &self,
        key: &[u8],
        merge_operator: Option<MergeOperatorType>,
    ) -> Result<Option<KeyValue>, SlateDBError> {
        let key = Bytes::copy_from_slice(key);
        let point = BytesRange::new(Included(key.clone()), Included(key));
        let mut iter = stack_iterator(&self.layers, self.seq, point, merge_operator).await?;
        Ok(iter.next_entry().await?.map(KeyValue::from))
    }
}

/// The result of [`RangeCache::pin_scan`].
pub(crate) enum ScanPin {
    /// Pinned layers that serve the scan.
    Pinned(PinnedScan),
    /// Ready ranges do not hold all of the scan range.
    Uncovered,
    /// Ready ranges hold the scan range, but failed the sequence number rules. See
    /// [`CacheRead::Unusable`].
    Unusable,
}

/// The parts of a mixed scan, with the layers of its cached parts pinned at one sequence
/// number. See [`RangeCache::pin_mixed`].
struct MixedPin {
    parts: Vec<Part>,
    seq: u64,
    /// The first cached part's range, which counts as read if the scan goes ahead.
    first: Arc<CachedRange>,
}

/// The layers of the adjacent cached ranges that hold a scan range, pinned at one sequence
/// number.
pub(crate) struct PinnedScan {
    parts: Vec<Arc<Layers>>,
    seq: u64,
}

impl PinnedScan {
    /// Returns an iterator over `range` at the pinned sequence number. See [`CachedScan`].
    pub(crate) async fn scan(
        self,
        range: BytesRange,
        merge_operator: Option<MergeOperatorType>,
    ) -> Result<DbIterator, SlateDBError> {
        let scan = CachedScan::new(self.parts, self.seq, &range, merge_operator)?;
        DbIterator::new_resolved(range, Box::new(scan), ReadTrace::none()).await
    }
}

/// What a point read found in the layers of a range at its sequence number.
enum Found {
    /// The newest version is a value.
    Value(KeyValue),
    /// The newest version is a tombstone, or the key has no version.
    Absent,
    /// The newest version is a merge operand.
    Merge,
}

/// Finds the newest version of `key` in `layers` at `seq`, checking buffers, runs, then base.
fn lookup(layers: &Layers, key: &[u8], seq: u64) -> io::Result<Found> {
    let mut hash = None;
    let mut hash_of = || *hash.get_or_insert_with(|| hash_key(key));
    for delta in layers.buffers() {
        // Most reads of a range that takes few writes find empty buffers: one load each. A buffer
        // whose versions are all newer than `seq` holds nothing for the read.
        if delta.is_empty() || delta.lo() >= seq {
            continue;
        }
        if let Some(filter) = delta.filter() {
            if !filter.may_contain(hash_of()) {
                continue;
            }
        }
        if let Some(version) = delta.newest(key, seq) {
            debug_assert!(
                version.seq > layers.covered_to,
                "a buffered version is covered"
            );
            return Ok(match version.kind {
                Kind::Value => Found::Value(KeyValue::from(delta::to_row(&version))),
                Kind::Tombstone => Found::Absent,
                Kind::Merge => Found::Merge,
            });
        }
    }
    for run in &layers.runs {
        if run.lo >= seq || key < run.min_key.as_ref() || key > run.max_key.as_ref() {
            continue;
        }
        if !run.filter.may_contain(hash_of()) {
            continue;
        }
        if let Some(found) = run.file.read_version(key, seq)? {
            return Ok(match found {
                FileRead::Value(row) => Found::Value(KeyValue::from(row)),
                FileRead::Tombstone => Found::Absent,
                FileRead::Merge => Found::Merge,
            });
        }
    }
    let row = match &layers.base {
        Some(file) => file.get(key)?,
        None => None,
    };
    Ok(match row {
        Some(row) => match row.value {
            ValueDeletable::Value(_) => Found::Value(KeyValue::from(row)),
            ValueDeletable::Merge(_) => Found::Merge,
            ValueDeletable::Tombstone => Found::Absent,
        },
        None => Found::Absent,
    })
}

/// Opens a normal iterator stack over `layers` at `seq` for merge resolution, skipping runs
/// ruled out by sequence bounds, key bounds, or Bloom filter.
async fn stack_iterator(
    layers: &Layers,
    seq: u64,
    range: BytesRange,
    merge_operator: Option<MergeOperatorType>,
) -> Result<DbIterator, SlateDBError> {
    let mut sources: Vec<Box<dyn RowEntryIterator + 'static>> = layers
        .buffers()
        .map(|delta| {
            Box::new(DeltaIterator::new(delta.clone(), &range))
                as Box<dyn RowEntryIterator + 'static>
        })
        .collect();
    let hash = one_key(&range).map(hash_key);
    for run in &layers.runs {
        if run.lo >= seq
            || run.file.rows() == 0
            || !run_meets(run, &range)
            || hash.is_some_and(|hash| !run.filter.may_contain(hash))
        {
            continue;
        }
        sources.push(Box::new(RangeFileIterator::new(
            run.file.clone(),
            range.clone(),
        )));
    }
    if let Some(file) = &layers.base {
        sources.push(Box::new(RangeFileIterator::new(
            file.clone(),
            range.clone(),
        )));
    }
    DbIterator::new(
        range,
        None,
        sources,
        Box::new(EmptyIterator::new()),
        Some(seq),
        merge_operator,
        IterationOrder::Ascending,
        ReadTrace::none(),
    )
    .await
}

/// The key of `range` if it holds one key only: `[k, k]`, or `[k, k + 0x00)` as a scan's merge
/// resolution reads it.
fn one_key(range: &BytesRange) -> Option<&[u8]> {
    if let Some(key) = range.as_point() {
        return Some(key);
    }
    match (
        RangeBounds::start_bound(range),
        RangeBounds::end_bound(range),
    ) {
        (Included(start), Excluded(end))
            if end.len() == start.len() + 1 && end.starts_with(start) && end.ends_with(&[0]) =>
        {
            Some(start)
        }
        _ => None,
    }
}

/// Returns true if a non-empty run can hold a key of `range`, by its key bounds.
fn run_meets(run: &Run, range: &BytesRange) -> bool {
    let after_start = match RangeBounds::start_bound(range) {
        Unbounded => true,
        Included(start) => run.max_key >= start,
        Excluded(start) => run.max_key > start,
    };
    let before_end = match RangeBounds::end_bound(range) {
        Unbounded => true,
        Included(end) => run.min_key <= end,
        Excluded(end) => run.min_key < end,
    };
    after_start && before_end
}

/// Returns true if a point read with `options` can use the range cache.
///
/// Traced reads bypass the cache so tests can compare cached reads against the normal path.
pub(crate) fn can_serve_get(options: &ReadOptions) -> bool {
    matches!(options.durability_filter, DurabilityLevel::Memory)
        && !options.dirty
        && options.filter_context.is_none()
        && options.tracing_options.is_none()
}

/// Returns true if an ascending scan with `options` can use the range cache.
pub(crate) fn can_serve_scan(options: &ScanOptions) -> bool {
    matches!(options.durability_filter, DurabilityLevel::Memory)
        && !options.dirty
        && matches!(options.order, IterationOrder::Ascending)
        && options.filter_context.is_none()
        && options.tracing_options.is_none()
}

/// Routing result for [`DbInner::range_cache_scan`].
pub(crate) enum ScanRoute {
    /// Served by the range cache (fully or via mixed scan).
    Served(Result<DbIterator, SlateDBError>),
    /// Fall back to the normal read path for this range.
    Normal(BytesRange),
}

impl DbInner {
    /// Serves a point read of `key` from the range cache, or returns `None` to use the normal
    /// read path. `at` is the snapshot/transaction sequence number, or `None` for latest state.
    pub(crate) async fn range_cache_get(
        &self,
        key: &[u8],
        options: &ReadOptions,
        at: Option<u64>,
    ) -> Option<Result<Option<KeyValue>, SlateDBError>> {
        let cache = self.range_cache.as_ref()?;
        if !can_serve_get(options) {
            return None;
        }
        cache.hits.bind_db(&self.db_stats);
        match cache.get(key, self.oracle.as_ref(), at) {
            CacheRead::Uncovered | CacheRead::Unusable => None,
            CacheRead::Answer(kv) => Some(Ok(kv)),
            CacheRead::Resolve(pinned) => Some(
                pinned
                    .get(key, self.reader.read_merge_operator.clone())
                    .await,
            ),
        }
    }

    /// Serves a scan of `range` from the range cache if possible.
    pub(crate) async fn range_cache_scan(
        &self,
        range: BytesRange,
        options: &ScanOptions,
        prefix: Option<&Bytes>,
        at: Option<u64>,
    ) -> ScanRoute {
        let Some(cache) = &self.range_cache else {
            return ScanRoute::Normal(range);
        };
        if !can_serve_scan(options) {
            return ScanRoute::Normal(range);
        }
        cache.hits.bind_db(&self.db_stats);
        match cache.pin_scan(&range, self.oracle.as_ref(), at) {
            ScanPin::Pinned(pinned) => ScanRoute::Served(
                pinned
                    .scan(range, self.reader.read_merge_operator.clone())
                    .await,
            ),
            ScanPin::Uncovered => {
                for _ in 0..PIN_ATTEMPTS {
                    // For latest-state scans, capture the state view before pinning so it retains
                    // all versions at or below the pinned sequence number unless a freeze occurs.
                    let latest_view = at.is_none().then(|| self.state.read().view());
                    let Some(pin) = cache.pin_mixed(&range, self.oracle.as_ref(), at) else {
                        break;
                    };
                    fail_point!(Arc::clone(&cache.fp_registry), "range-cache-mixed-pinned");
                    let view = match latest_view {
                        Some(view) => {
                            let frozen =
                                !Arc::ptr_eq(&view.memtable, self.state.read().memtable().table());
                            if frozen {
                                cache.hits.count(ReadCount::MixedScanRetry);
                                continue;
                            }
                            view
                        }
                        None => self.state.read().view(),
                    };
                    cache.count_read(&pin.first, ReadCount::MixedScan);
                    let gaps = GapSource {
                        reader: self.reader.clone(),
                        view,
                        options: options.clone(),
                        prefix: prefix.cloned(),
                        cache: cache.clone(),
                    };
                    let scan = MixedScan::new(
                        pin.parts,
                        pin.seq,
                        range.as_point().is_some(),
                        self.reader.read_merge_operator.clone(),
                        gaps,
                    );
                    return ScanRoute::Served(
                        DbIterator::new_resolved(range, Box::new(scan), ReadTrace::none()).await,
                    );
                }
                ScanRoute::Normal(range)
            }
            ScanPin::Unusable => ScanRoute::Normal(range),
        }
    }

    /// Loads uncovered parts of `range` into the range cache.
    async fn cache_range(self: &Arc<Self>, range: BytesRange) -> Result<(), SlateDBError> {
        let cache = self
            .range_cache
            .clone()
            .ok_or(SlateDBError::RangeCacheNotConfigured)?;
        self.check_closed()?;
        let Some((start, end)) = half_open(&range) else {
            return Ok(());
        };
        cache.bind_writer(&self.write_notifier);
        let target = cache.options.tuning.range_target_bytes;
        let splits = if target > 0 {
            self.range_split_points(&start, end.as_ref(), target)
                .await?
        } else {
            Vec::new()
        };
        // Hold a retention snapshot before registering so flush and compaction preserve every
        // version at or below the registration boundary `W >= guard.seq()`.
        let guard = DbSnapshot::new(self.clone(), None);
        fail_point!(
            Arc::clone(&cache.fp_registry),
            "range-cache-register-publish"
        );
        let registered = cache.register(start, end, splits).await?;
        debug_assert!(registered.seq >= guard.seq());
        fail_point!(
            Arc::clone(&cache.fp_registry),
            "range-cache-register-snapshot"
        );
        let options = cache.options.tuning.bulk_scan_options();
        let mut result = Ok(());
        for range in &registered.ranges {
            if result.is_err() {
                cache.remove_and_wait(range, RemoveReason::Error).await;
                continue;
            }
            let db_state = self.state.read().view();
            let iter = self
                .reader
                .scan_with_options(
                    range.bounds(),
                    &options,
                    ScanContext {
                        db_state: &db_state,
                        write_batch_iter: None,
                        max_seq: Some(registered.seq),
                        prefix: None,
                    },
                )
                .await;
            result = match iter {
                Ok(iter) => {
                    cache
                        .load(range, iter, registered.seq, &|| self.check_closed())
                        .await
                }
                Err(e) => Err(e),
            };
            if result.is_err() {
                cache.remove_and_wait(range, RemoveReason::Error).await;
            }
        }
        drop(guard);
        result
    }

    /// Computes split keys for `[start, end)` at roughly `target`-byte block boundaries of the
    /// largest sorted run. Returns empty when no sorted run exists or segments are enabled.
    async fn range_split_points(
        &self,
        start: &Bytes,
        end: Option<&Bytes>,
        target: u64,
    ) -> Result<Vec<Bytes>, SlateDBError> {
        let run = {
            let view = self.state.read().view();
            let core = view.state.core();
            if core.segment_extractor_name.is_some() {
                return Ok(Vec::new());
            }
            match core
                .tree
                .compacted
                .iter()
                .max_by_key(|sr| sr.estimate_size())
            {
                Some(run) => run.clone(),
                None => return Ok(Vec::new()),
            }
        };
        let views = run.sst_views();
        let inside = |k: &[u8]| k > start.as_ref() && end.is_none_or(|end| k < end.as_ref());
        let mut splits = Vec::new();
        let mut bytes = 0u64;
        for (i, view) in views.iter().enumerate() {
            // The SST covers keys from its start to the next one's start.
            let sst_start = view.compacted_effective_start_key();
            let below_end = end.is_none_or(|end| sst_start.as_ref() < end.as_ref());
            let above_start = views
                .get(i + 1)
                .is_none_or(|next| next.compacted_effective_start_key() > start);
            if !below_end || !above_start {
                continue;
            }
            let index = self
                .table_store
                .read_index(
                    &view.sst,
                    true,
                    Some(Bytes::new()),
                    &ReadTrace::none(),
                    None,
                )
                .await?;
            let metas = index.borrow().block_meta();
            let n = metas.len();
            let avg = view.sst.estimate_size() / (n.max(1) as u64);
            for b in 0..n {
                let first = metas.get(b).first_key().bytes();
                if bytes >= target && inside(first) {
                    splits.push(Bytes::copy_from_slice(first));
                    bytes = 0;
                }
                let next = (b + 1 < n).then(|| metas.get(b + 1));
                let in_range = next.is_none_or(|next| next.first_key().bytes() > start.as_ref())
                    && end.is_none_or(|end| first < end.as_ref());
                if in_range {
                    bytes += next.map_or(avg, |next| {
                        next.offset().saturating_sub(metas.get(b).offset())
                    });
                }
            }
        }
        Ok(splits)
    }

    fn range_cache_or_err(&self) -> Result<&Arc<RangeCache>, SlateDBError> {
        self.range_cache
            .as_ref()
            .ok_or(SlateDBError::RangeCacheNotConfigured)
    }
}

impl Db {
    /// Loads uncovered parts of `range` into the range cache.
    ///
    /// Concurrent writes go to the range's write buffer so no updates are lost. Committed point
    /// reads and ascending scans serve cached keys from local disk and memory; dirty reads and
    /// snapshots older than the base use the normal read path.
    ///
    /// ## Errors
    /// - [`crate::ErrorKind::Invalid`] if the range cache is disabled, the range contains expiring
    ///   rows, the range exceeds `max_disk_bytes`, or write buffers are near capacity.
    pub async fn cache_range<T>(&self, range: T) -> Result<(), crate::Error>
    where
        T: ByteRangeBounds + Send,
    {
        let start = range.start_bound().map(Bytes::copy_from_slice);
        let end = range.end_bound().map(Bytes::copy_from_slice);
        let Some(range) = BytesRange::try_new(start, end) else {
            return Ok(());
        };
        self.inner.cache_range(range).await.map_err(Into::into)
    }

    /// Queues removal of all cached ranges overlapping `range`.
    ///
    /// The batch writer applies the removal at the next batch or wake-up. Reads stay exact
    /// meanwhile, and subsequent [`Db::cache_range`] calls run after the removal.
    pub fn uncache_range<T>(&self, range: T) -> Result<(), crate::Error>
    where
        T: ByteRangeBounds,
    {
        let cache = self.inner.range_cache_or_err()?;
        let start = range.start_bound().map(Bytes::copy_from_slice);
        let end = range.end_bound().map(Bytes::copy_from_slice);
        if let Some(range) = BytesRange::try_new(start, end) {
            cache.bind_writer(&self.inner.write_notifier);
            cache.request_removal(Target::Overlapping(range), RemoveReason::Explicit);
        }
        Ok(())
    }

    /// Queues removal of all cached ranges. See [`Db::uncache_range`].
    pub fn clear_range_cache(&self) -> Result<(), crate::Error> {
        let cache = self.inner.range_cache_or_err()?;
        cache.bind_writer(&self.inner.write_notifier);
        cache.request_removal(Target::All, RemoveReason::Explicit);
        Ok(())
    }

    /// Waits until the batch writer applies all earlier range cache requests.
    #[doc(hidden)]
    pub async fn wait_for_range_cache(&self) -> Result<(), crate::Error> {
        let cache = self.inner.range_cache_or_err()?;
        cache.bind_writer(&self.inner.write_notifier);
        cache.barrier().await;
        Ok(())
    }

    /// Returns cached ranges in key order, or an empty vector if the range cache is disabled.
    pub fn cached_ranges(&self) -> Vec<CachedRangeInfo> {
        self.inner
            .range_cache
            .as_ref()
            .map(|cache| cache.ranges())
            .unwrap_or_default()
    }

    /// Returns exact memory bytes held by all live range cache write buffers, or `0` if disabled.
    #[doc(hidden)]
    pub fn range_cache_buffer_memory(&self) -> u64 {
        self.inner
            .range_cache
            .as_ref()
            .map_or(0, |cache| cache.buffer_memory())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn index(ranges: &[(&str, Option<&str>)]) -> RangeIndex {
        ranges
            .iter()
            .enumerate()
            .map(|(slot, (start, end))| {
                (
                    b(start),
                    Arc::new(CachedRange::new(
                        b(start),
                        end.map(b),
                        Layers {
                            active: Arc::new(Delta::for_test(&DeltaMemory::detached(), 0)),
                            sealed: Vec::new(),
                            runs: Vec::new(),
                            view: None,
                            base: None,
                            base_seq: 0,
                            covered_to: 0,
                            end: end.map(b),
                        },
                        slot as u32,
                    )),
                )
            })
            .collect()
    }

    fn parts(index: &RangeIndex, start: &str, end: Option<&str>) -> Vec<(Bytes, Option<Bytes>)> {
        uncovered(index, &b(start), end.map(b).as_ref())
    }

    #[test]
    fn should_find_uncovered_parts() {
        let empty = index(&[]);
        assert_eq!(parts(&empty, "a", Some("z")), vec![(b("a"), Some(b("z")))]);
        assert_eq!(parts(&empty, "", None), vec![(b(""), None)]);

        let idx = index(&[("c", Some("e")), ("g", Some("h")), ("x", None)]);
        assert_eq!(
            parts(&idx, "a", Some("z")),
            vec![
                (b("a"), Some(b("c"))),
                (b("e"), Some(b("g"))),
                (b("h"), Some(b("x"))),
            ]
        );
        // Starts inside a range.
        assert_eq!(parts(&idx, "d", Some("f")), vec![(b("e"), Some(b("f")))]);
        // Fully covered.
        assert!(parts(&idx, "c", Some("e")).is_empty());
        assert!(parts(&idx, "c", Some("d")).is_empty());
        assert!(parts(&idx, "y", None).is_empty());
        // Ends where a range starts.
        assert_eq!(parts(&idx, "e", Some("g")), vec![(b("e"), Some(b("g")))]);
        // A range that ends before the start does not count.
        assert_eq!(parts(&idx, "f", Some("g")), vec![(b("f"), Some(b("g")))]);
        // Unbounded end after the last bounded range.
        let idx = index(&[("c", Some("e"))]);
        assert_eq!(
            parts(&idx, "a", None),
            vec![(b("a"), Some(b("c"))), (b("e"), None)]
        );
    }

    #[test]
    fn should_find_the_covering_range() {
        let idx = index(&[("c", Some("e")), ("x", None)]);
        assert!(covering(&idx, b"b").is_none());
        assert_eq!(covering(&idx, b"c").unwrap().start, b("c"));
        assert_eq!(covering(&idx, b"d\xff").unwrap().start, b("c"));
        assert!(covering(&idx, b"e").is_none());
        assert_eq!(covering(&idx, b"zzz").unwrap().start, b("x"));
    }

    #[test]
    fn should_convert_ranges_to_half_open_ranges() {
        let r = |s: Bound<&str>, e: Bound<&str>| half_open(&BytesRange::new(s.map(b), e.map(b)));
        assert_eq!(r(Unbounded, Unbounded), Some((b(""), None)));
        assert_eq!(
            r(Included("a"), Excluded("c")),
            Some((b("a"), Some(b("c"))))
        );
        assert_eq!(
            r(Excluded("a"), Included("c")),
            Some((Bytes::from_static(b"a\0"), Some(Bytes::from_static(b"c\0"))))
        );
    }

    #[test]
    fn should_check_scan_containment() {
        let idx = index(&[("c", Some("e"))]);
        let range = &idx[&b("c")];
        let r = |s: Bound<&str>, e: Bound<&str>| BytesRange::new(s.map(b), e.map(b));
        assert!(range.contains_range(&r(Included("c"), Excluded("e"))));
        assert!(range.contains_range(&r(Included("c"), Included("d"))));
        assert!(!range.contains_range(&r(Included("c"), Included("e"))));
        assert!(!range.contains_range(&r(Included("c"), Unbounded)));
    }

    /// Reads through a database with a range cache, checked against the cache's counters.
    mod reads {
        use super::super::*;
        use crate::db_stats::REQUEST_COUNT;
        use crate::object_store::memory::InMemory;
        use slatedb_common::metrics::{lookup_metric_with_labels, DefaultMetricsRecorder};

        struct TestCache {
            db: Db,
            recorder: Arc<DefaultMetricsRecorder>,
            _dir: tempfile::TempDir,
        }

        impl TestCache {
            fn cache(&self) -> &Arc<RangeCache> {
                self.db.inner.range_cache.as_ref().unwrap()
            }

            fn range(&self, start: &Bytes) -> Arc<CachedRange> {
                self.cache().index.load().get(start).unwrap().clone()
            }

            fn metric(&self, name: &str, labels: &[(&str, &str)]) -> i64 {
                lookup_metric_with_labels(&self.recorder, name, labels).unwrap_or(0)
            }

            /// Hits, absent reads, scans, and the database's get and scan requests.
            fn read_metrics(&self) -> [i64; 5] {
                [
                    self.metric(stats::GET_COUNT, &[("result", "hit")]),
                    self.metric(stats::GET_COUNT, &[("result", "absent")]),
                    self.metric(stats::SCAN_COUNT, &[]),
                    self.metric(REQUEST_COUNT, &[("op", "get")]),
                    self.metric(REQUEST_COUNT, &[("op", "scan")]),
                ]
            }

            fn unfit(&self, name: &str, op: &str, reason: &str) -> i64 {
                self.metric(name, &[("op", op), ("reason", reason)])
            }
        }

        async fn open(tune: impl FnOnce(&mut RangeCacheOptions)) -> TestCache {
            let dir = tempfile::tempdir().unwrap();
            let mut options = RangeCacheOptions::new(dir.path());
            tune(&mut options);
            let recorder = Arc::new(DefaultMetricsRecorder::new());
            let db = Db::builder("range_cache_reads", Arc::new(InMemory::new()))
                .with_range_cache(options)
                .with_metrics_recorder(recorder.clone())
                .build()
                .await
                .unwrap();
            TestCache {
                db,
                recorder,
                _dir: dir,
            }
        }

        fn key(i: usize) -> Bytes {
            Bytes::from(format!("key{i:05}"))
        }

        /// Puts the even keys below `n`.
        async fn put_even(db: &Db, n: usize) {
            for i in (0..n).step_by(2) {
                db.put(key(i), format!("value-{i}")).await.unwrap();
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_count_reads_only_on_their_thread_until_a_fold() {
            let t = open(|o| {
                o.tuning.stats_fold_events = 1_000_000;
                o.tuning.stats_fold_interval = Duration::from_secs(3600);
            })
            .await;
            put_even(&t.db, 200).await;
            t.db.cache_range(key(0)..key(200)).await.unwrap();
            let range = t.range(&key(0));
            let before = t.read_metrics();
            let last_access = range.hits.last_access();
            let clock = t.cache().hits.access_clock().load(Ordering::Relaxed);
            assert!(last_access < clock);

            let handle = tokio::runtime::Handle::current();
            std::thread::scope(|s| {
                let (main, worker) = hits::Turn::pair();
                s.spawn(|| {
                    let worker = worker;
                    handle.block_on(async {
                        for i in 0..400 {
                            t.db.get(key(i % 200)).await.unwrap();
                        }
                        for _ in 0..20 {
                            t.db.scan(key(10)..key(20)).await.unwrap();
                        }
                    });
                    worker.go();
                    worker.wait();
                    t.cache().hits.fold_current_thread();
                    worker.go();
                });
                main.wait();
                assert_eq!(t.read_metrics(), before);
                assert_eq!(range.hits.reads(), 0);
                assert_eq!(range.hits.last_access(), last_access);
                main.go();
                main.wait();
                let [hits, absent, scans, gets, scan_requests] = before;
                assert_eq!(
                    t.read_metrics(),
                    [
                        hits + 200,
                        absent + 200,
                        scans + 20,
                        gets + 400,
                        scan_requests + 20
                    ]
                );
                assert_eq!(range.hits.last_access(), clock);
            });
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_converge_to_the_true_counts_after_threads_exit() {
            let t = open(|_| {}).await;
            put_even(&t.db, 200).await;
            t.db.cache_range(key(0)..key(200)).await.unwrap();
            let range = t.range(&key(0));
            let before = t.read_metrics();
            let handle = tokio::runtime::Handle::current();
            std::thread::scope(|s| {
                let threads: Vec<_> = (0..4)
                    .map(|_| {
                        s.spawn(|| {
                            handle.block_on(async {
                                for i in 0..5_000 {
                                    t.db.get(key(i % 200)).await.unwrap();
                                }
                            })
                        })
                    })
                    .collect();
                for thread in threads {
                    thread.join().unwrap();
                }
            });
            let [hits, absent, scans, gets, scan_requests] = before;
            assert_eq!(
                t.read_metrics(),
                [
                    hits + 10_000,
                    absent + 10_000,
                    scans,
                    gets + 20_000,
                    scan_requests
                ]
            );
            assert_eq!(range.hits.reads(), 20_000);
        }

        #[tokio::test]
        async fn should_count_fallbacks_by_reason() {
            let t = open(|_| {}).await;
            put_even(&t.db, 200).await;
            let old = t.db.snapshot().unwrap();
            // A write after the snapshot, so that the load reads a newer state.
            t.db.put(key(300), "later").await.unwrap();
            t.db.cache_range(key(50)..key(150)).await.unwrap();

            // The snapshot is older than the range's file: a point read and a covered scan fall
            // back at once, since a retry cannot succeed, and a mixed scan makes the cached
            // part a gap.
            assert_eq!(
                old.get(key(60)).await.unwrap(),
                Some(Bytes::from("value-60"))
            );
            old.scan(key(60)..key(70)).await.unwrap();
            old.scan(key(40)..key(70)).await.unwrap();

            // A freeze or a removal that a reader races seals the active delta below the
            // reader's sequence number. Store the seal directly to make every attempt see it.
            let range = t.range(&key(50));
            range
                .layers
                .load()
                .active
                .sealed_at()
                .store(0, Ordering::SeqCst);
            assert_eq!(
                t.db.get(key(60)).await.unwrap(),
                Some(Bytes::from("value-60"))
            );
            t.db.scan(key(60)..key(70)).await.unwrap();
            t.db.scan(key(40)..key(70)).await.unwrap();

            t.cache().hits.fold_current_thread();
            for (reason, retries) in [("too_old", 0), ("sealed", 2)] {
                assert_eq!(
                    t.unfit(stats::PIN_RETRY_COUNT, "get", reason),
                    retries,
                    "{reason}"
                );
                assert_eq!(
                    t.unfit(stats::PIN_RETRY_COUNT, "scan", reason),
                    retries,
                    "{reason}"
                );
                assert_eq!(t.unfit(stats::UNUSABLE_COUNT, "get", reason), 1, "{reason}");
                assert_eq!(
                    t.unfit(stats::UNUSABLE_COUNT, "scan", reason),
                    1,
                    "{reason}"
                );
                assert_eq!(
                    t.unfit(stats::UNUSABLE_COUNT, "scan_part", reason),
                    1,
                    "{reason}"
                );
            }
            // None of them counts as served.
            assert_eq!(t.read_metrics()[..3], [0, 0, 0]);
        }

        #[tokio::test]
        async fn should_evict_the_least_recently_read_range_by_folded_reads() {
            let t = open(|o| {
                o.tuning.page_size = 4096;
                o.max_disk_bytes = 9000;
                o.tuning.stats_fold_interval = Duration::from_secs(3600);
            })
            .await;
            for i in 0..300 {
                t.db.put(key(i), format!("value-{i}")).await.unwrap();
            }
            t.db.cache_range(key(0)..key(50)).await.unwrap();
            t.db.cache_range(key(100)..key(150)).await.unwrap();
            // Enough reads of the first range for a fold at the default fold size. Until a fold,
            // its last access is older than the second range's.
            let fold = RangeCacheTuning::default().stats_fold_events;
            let first = t.range(&key(0));
            for _ in 0..fold {
                t.db.get(key(10)).await.unwrap();
            }
            assert_eq!(first.hits.reads(), u64::from(fold));
            t.db.cache_range(key(200)..key(250)).await.unwrap();
            let starts: Vec<_> = t.db.cached_ranges().into_iter().map(|r| r.start).collect();
            assert_eq!(starts, vec![key(0), key(200)]);
        }
    }
}
