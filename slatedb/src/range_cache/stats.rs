//! Metrics for the range cache.
//!
//! Read-path counters (`get_count`, `scan_count`, `mixed_scan_count`, `scan_gap_count`,
//! `mixed_scan_retry_count`, `pin_retry_count`, and `unusable_count`) accumulate per thread and
//! fold into shared counters periodically. See [`crate::RangeCacheTuning::stats_fold_events`].

use slatedb_common::metrics::{CounterFn, GaugeFn, MetricsRecorderHelper};
use std::sync::Arc;

macro_rules! range_cache_stat_name {
    ($suffix:expr) => {
        concat!("slatedb.range_cache.", $suffix)
    };
}

/// Point reads served by the range cache. `result` is `hit`, `absent`, or `merge`.
pub const GET_COUNT: &str = range_cache_stat_name!("get_count");
/// Scans served completely by the range cache.
pub const SCAN_COUNT: &str = range_cache_stat_name!("scan_count");
/// Scans served in part by cached ranges and in part by the normal read path.
pub const MIXED_SCAN_COUNT: &str = range_cache_stat_name!("mixed_scan_count");
/// Uncached gaps opened by mixed scans on the normal read path.
pub const SCAN_GAP_COUNT: &str = range_cache_stat_name!("scan_gap_count");
/// Mixed-scan planning retries caused by a concurrent memtable freeze.
pub const MIXED_SCAN_RETRY_COUNT: &str = range_cache_stat_name!("mixed_scan_retry_count");
/// Retried attempts to pin a ready cached range. `op` is `get` or `scan`; `reason` is `too_old`
/// or `sealed`.
pub const PIN_RETRY_COUNT: &str = range_cache_stat_name!("pin_retry_count");
/// Reads that fell back to the normal read path because a cached range failed sequence rules.
/// `op` is `get`, `scan`, or `scan_part`; `reason` is `too_old` or `sealed`.
pub const UNUSABLE_COUNT: &str = range_cache_stat_name!("unusable_count");
/// Ranges loaded into the cache.
pub const LOAD_COUNT: &str = range_cache_stat_name!("load_count");
/// Bytes of range cache files written by loads.
pub const LOAD_BYTES: &str = range_cache_stat_name!("load_bytes");
/// Loads rejected before starting. `reason` is `memory`.
pub const LOAD_REJECTED_COUNT: &str = range_cache_stat_name!("load_rejected_count");
/// Major merges that rebuilt a range base from its base and runs.
pub const REBUILD_COUNT: &str = range_cache_stat_name!("rebuild_count");
/// Bytes of base files written by major merges.
pub const REBUILD_BYTES: &str = range_cache_stat_name!("rebuild_bytes");
/// Active write buffers sealed. `reason` is `full` or `pressure`.
pub const SEAL_COUNT: &str = range_cache_stat_name!("seal_count");
/// Flushes of sealed buffers to run files.
pub const FLUSH_COUNT: &str = range_cache_stat_name!("flush_count");
/// Bytes of run files written by plain flushes.
pub const FLUSH_BYTES: &str = range_cache_stat_name!("flush_bytes");
/// Flushes that merged the newest runs at `max_runs` to stay within the run cap.
pub const FLUSH_MERGE_COUNT: &str = range_cache_stat_name!("flush_merge_count");
/// Bytes of run files written by flush-merges.
pub const FLUSH_MERGE_BYTES: &str = range_cache_stat_name!("flush_merge_bytes");
/// Flushed runs or merged bases rejected because the range state changed concurrently.
pub const INSTALL_REJECTED_COUNT: &str = range_cache_stat_name!("install_rejected_count");
/// Ranges removed from the cache. `reason` is `evicted`, `explicit`, `expiring_write`, `error`,
/// `closed`, or `overflow`.
pub const REMOVE_COUNT: &str = range_cache_stat_name!("remove_count");
/// Cached ranges currently indexed, including loading ranges.
pub const RANGE_COUNT: &str = range_cache_stat_name!("range_count");
/// Disk bytes of open range cache files.
pub const FILE_BYTES: &str = range_cache_stat_name!("file_bytes");
/// Estimated bytes of active and sealed write buffers across cached ranges.
pub const DELTA_BYTES: &str = range_cache_stat_name!("delta_bytes");
/// Heap metadata bytes of open files, delta views, and cached range layer sets.
pub const META_BYTES: &str = range_cache_stat_name!("meta_bytes");
/// Exact heap bytes allocated by live write buffers (chunks and filters). Never exceeds
/// `max_recent_bytes`.
pub const DELTA_ALLOCATED_BYTES: &str = range_cache_stat_name!("delta_allocated_bytes");
/// Heap bytes of all live delta views. Also included in `meta_bytes`.
pub const VIEW_BYTES: &str = range_cache_stat_name!("view_bytes");

/// Why the cache removed a range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RemoveReason {
    /// Evicted to stay within disk or metadata budgets.
    Evicted,
    /// Removed by `uncache_range` or `clear_range_cache`.
    Explicit,
    /// Touched by a write with an expire time.
    ExpiringWrite,
    /// Removed after a load, merge, or read error.
    Error,
    /// Removed when the database closed or failed.
    Closed,
    /// Removed to stay within the write buffer memory cap.
    Overflow,
}

/// Why a ready cached range failed sequence number checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unfit {
    /// The read sequence number is below the base sequence number.
    TooOld,
    /// The read sequence number is above the active buffer seal.
    Sealed,
}

/// The kind of read pinning cached ranges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PinOp {
    Get,
    Scan,
    ScanPart,
}

/// A read-path counter accumulated per thread and folded into shared metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadCount {
    GetHit,
    GetAbsent,
    GetMerge,
    Scan,
    MixedScan,
    ScanGap,
    MixedScanRetry,
    RetryGetTooOld,
    RetryGetSealed,
    RetryScanTooOld,
    RetryScanSealed,
    UnusableGetTooOld,
    UnusableGetSealed,
    UnusableScanTooOld,
    UnusableScanSealed,
    UnusablePartTooOld,
    UnusablePartSealed,
}

/// Number of [`ReadCount`] variants.
pub(crate) const READ_COUNTS: usize = 17;

impl ReadCount {
    /// Every variant in discriminant order.
    pub(crate) const ALL: [ReadCount; READ_COUNTS] = [
        ReadCount::GetHit,
        ReadCount::GetAbsent,
        ReadCount::GetMerge,
        ReadCount::Scan,
        ReadCount::MixedScan,
        ReadCount::ScanGap,
        ReadCount::MixedScanRetry,
        ReadCount::RetryGetTooOld,
        ReadCount::RetryGetSealed,
        ReadCount::RetryScanTooOld,
        ReadCount::RetryScanSealed,
        ReadCount::UnusableGetTooOld,
        ReadCount::UnusableGetSealed,
        ReadCount::UnusableScanTooOld,
        ReadCount::UnusableScanSealed,
        ReadCount::UnusablePartTooOld,
        ReadCount::UnusablePartSealed,
    ];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn retry(op: PinOp, reason: Unfit) -> Self {
        match (op, reason) {
            (PinOp::Get, Unfit::TooOld) => ReadCount::RetryGetTooOld,
            (PinOp::Get, Unfit::Sealed) => ReadCount::RetryGetSealed,
            (PinOp::Scan | PinOp::ScanPart, Unfit::TooOld) => ReadCount::RetryScanTooOld,
            (PinOp::Scan | PinOp::ScanPart, Unfit::Sealed) => ReadCount::RetryScanSealed,
        }
    }

    pub(crate) fn unusable(op: PinOp, reason: Unfit) -> Self {
        match (op, reason) {
            (PinOp::Get, Unfit::TooOld) => ReadCount::UnusableGetTooOld,
            (PinOp::Get, Unfit::Sealed) => ReadCount::UnusableGetSealed,
            (PinOp::Scan, Unfit::TooOld) => ReadCount::UnusableScanTooOld,
            (PinOp::Scan, Unfit::Sealed) => ReadCount::UnusableScanSealed,
            (PinOp::ScanPart, Unfit::TooOld) => ReadCount::UnusablePartTooOld,
            (PinOp::ScanPart, Unfit::Sealed) => ReadCount::UnusablePartSealed,
        }
    }

    fn metric(self) -> (&'static str, &'static [(&'static str, &'static str)]) {
        match self {
            ReadCount::GetHit => (GET_COUNT, &[("result", "hit")]),
            ReadCount::GetAbsent => (GET_COUNT, &[("result", "absent")]),
            ReadCount::GetMerge => (GET_COUNT, &[("result", "merge")]),
            ReadCount::Scan => (SCAN_COUNT, &[]),
            ReadCount::MixedScan => (MIXED_SCAN_COUNT, &[]),
            ReadCount::ScanGap => (SCAN_GAP_COUNT, &[]),
            ReadCount::MixedScanRetry => (MIXED_SCAN_RETRY_COUNT, &[]),
            ReadCount::RetryGetTooOld => (PIN_RETRY_COUNT, &[("op", "get"), ("reason", "too_old")]),
            ReadCount::RetryGetSealed => (PIN_RETRY_COUNT, &[("op", "get"), ("reason", "sealed")]),
            ReadCount::RetryScanTooOld => {
                (PIN_RETRY_COUNT, &[("op", "scan"), ("reason", "too_old")])
            }
            ReadCount::RetryScanSealed => {
                (PIN_RETRY_COUNT, &[("op", "scan"), ("reason", "sealed")])
            }
            ReadCount::UnusableGetTooOld => {
                (UNUSABLE_COUNT, &[("op", "get"), ("reason", "too_old")])
            }
            ReadCount::UnusableGetSealed => {
                (UNUSABLE_COUNT, &[("op", "get"), ("reason", "sealed")])
            }
            ReadCount::UnusableScanTooOld => {
                (UNUSABLE_COUNT, &[("op", "scan"), ("reason", "too_old")])
            }
            ReadCount::UnusableScanSealed => {
                (UNUSABLE_COUNT, &[("op", "scan"), ("reason", "sealed")])
            }
            ReadCount::UnusablePartTooOld => (
                UNUSABLE_COUNT,
                &[("op", "scan_part"), ("reason", "too_old")],
            ),
            ReadCount::UnusablePartSealed => {
                (UNUSABLE_COUNT, &[("op", "scan_part"), ("reason", "sealed")])
            }
        }
    }
}

/// Registers read-path counters indexed by [`ReadCount::index`].
pub(crate) fn read_counters(recorder: &MetricsRecorderHelper) -> [Arc<dyn CounterFn>; READ_COUNTS] {
    ReadCount::ALL.map(|count| {
        let (name, labels) = count.metric();
        recorder.counter(name).labels(labels).register()
    })
}

/// Range cache metrics outside the per-thread read counters.
pub(crate) struct RangeCacheStats {
    pub(crate) loads: Arc<dyn CounterFn>,
    pub(crate) load_bytes: Arc<dyn CounterFn>,
    pub(crate) loads_rejected_memory: Arc<dyn CounterFn>,
    pub(crate) rebuilds: Arc<dyn CounterFn>,
    pub(crate) rebuild_bytes: Arc<dyn CounterFn>,
    pub(crate) seals_full: Arc<dyn CounterFn>,
    pub(crate) seals_pressure: Arc<dyn CounterFn>,
    pub(crate) flushes: Arc<dyn CounterFn>,
    pub(crate) flush_bytes: Arc<dyn CounterFn>,
    pub(crate) flush_merges: Arc<dyn CounterFn>,
    pub(crate) flush_merge_bytes: Arc<dyn CounterFn>,
    pub(crate) installs_rejected: Arc<dyn CounterFn>,
    removed_evicted: Arc<dyn CounterFn>,
    removed_explicit: Arc<dyn CounterFn>,
    removed_expiring_write: Arc<dyn CounterFn>,
    removed_error: Arc<dyn CounterFn>,
    removed_closed: Arc<dyn CounterFn>,
    removed_overflow: Arc<dyn CounterFn>,
    pub(crate) ranges: Arc<dyn GaugeFn>,
    pub(crate) file_bytes: Arc<dyn GaugeFn>,
    pub(crate) delta_bytes: Arc<dyn GaugeFn>,
    pub(crate) meta_bytes: Arc<dyn GaugeFn>,
    pub(crate) view_bytes: Arc<dyn GaugeFn>,
}

impl RangeCacheStats {
    pub(crate) fn new(recorder: &MetricsRecorderHelper) -> Self {
        let labeled =
            |name, label, value| recorder.counter(name).labels(&[(label, value)]).register();
        let removed = |reason| labeled(REMOVE_COUNT, "reason", reason);
        Self {
            loads: recorder.counter(LOAD_COUNT).register(),
            load_bytes: recorder.counter(LOAD_BYTES).register(),
            loads_rejected_memory: labeled(LOAD_REJECTED_COUNT, "reason", "memory"),
            rebuilds: recorder.counter(REBUILD_COUNT).register(),
            rebuild_bytes: recorder.counter(REBUILD_BYTES).register(),
            seals_full: labeled(SEAL_COUNT, "reason", "full"),
            seals_pressure: labeled(SEAL_COUNT, "reason", "pressure"),
            flushes: recorder.counter(FLUSH_COUNT).register(),
            flush_bytes: recorder.counter(FLUSH_BYTES).register(),
            flush_merges: recorder.counter(FLUSH_MERGE_COUNT).register(),
            flush_merge_bytes: recorder.counter(FLUSH_MERGE_BYTES).register(),
            installs_rejected: recorder.counter(INSTALL_REJECTED_COUNT).register(),
            removed_evicted: removed("evicted"),
            removed_explicit: removed("explicit"),
            removed_expiring_write: removed("expiring_write"),
            removed_error: removed("error"),
            removed_closed: removed("closed"),
            removed_overflow: removed("overflow"),
            ranges: recorder.gauge(RANGE_COUNT).register(),
            file_bytes: recorder.gauge(FILE_BYTES).register(),
            delta_bytes: recorder.gauge(DELTA_BYTES).register(),
            meta_bytes: recorder.gauge(META_BYTES).register(),
            view_bytes: recorder.gauge(VIEW_BYTES).register(),
        }
    }

    pub(crate) fn removed(&self, reason: RemoveReason) {
        let counter = match reason {
            RemoveReason::Evicted => &self.removed_evicted,
            RemoveReason::Explicit => &self.removed_explicit,
            RemoveReason::ExpiringWrite => &self.removed_expiring_write,
            RemoveReason::Error => &self.removed_error,
            RemoveReason::Closed => &self.removed_closed,
            RemoveReason::Overflow => &self.removed_overflow,
        };
        counter.increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn should_list_each_read_count_once_in_order() {
        for (i, count) in ReadCount::ALL.iter().enumerate() {
            assert_eq!(count.index(), i, "{count:?}");
        }
        let metrics: BTreeSet<_> = ReadCount::ALL.iter().map(|c| c.metric()).collect();
        assert_eq!(metrics.len(), READ_COUNTS, "two read counts share a metric");
    }

    #[test]
    fn should_map_retries_and_fallbacks_to_their_op_and_reason() {
        let cases = [
            (PinOp::Get, Unfit::TooOld, "get", "too_old"),
            (PinOp::Get, Unfit::Sealed, "get", "sealed"),
            (PinOp::Scan, Unfit::TooOld, "scan", "too_old"),
            (PinOp::Scan, Unfit::Sealed, "scan", "sealed"),
        ];
        for (op, reason, op_label, reason_label) in cases {
            let labels = [("op", op_label), ("reason", reason_label)];
            assert_eq!(
                ReadCount::retry(op, reason).metric(),
                (PIN_RETRY_COUNT, &labels[..])
            );
            assert_eq!(
                ReadCount::unusable(op, reason).metric(),
                (UNUSABLE_COUNT, &labels[..])
            );
        }
        assert_eq!(
            ReadCount::unusable(PinOp::ScanPart, Unfit::Sealed).metric(),
            (
                UNUSABLE_COUNT,
                &[("op", "scan_part"), ("reason", "sealed")][..]
            )
        );
    }
}
