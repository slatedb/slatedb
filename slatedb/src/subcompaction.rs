//! # Subcompaction Planning
//!
//! Plans the key-range boundaries that split a single logical compaction into
//! subcompactions (RFC-0028). Each subcompaction covers a disjoint sub-range
//! of the key space so the parent compaction can execute its ranges in
//! parallel and resume at range granularity after a failure.
//!
//! The planner only reads metadata that is already available in the manifest
//! (SST first/last keys and size estimates); it never fetches SST blocks or
//! indexes. Boundaries are restricted to input SST boundary keys, which keeps
//! planning cheap at the cost of less precise splits.

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use bytes::Bytes;

use crate::bytes_range::BytesRange;
use crate::db_state::{SortedRun, SsTableView};

/// Plans the key ranges to split a compaction into subcompactions.
///
/// Implements the RFC-0028 boundary-selection heuristic:
/// 1. Candidate boundaries are the union of all input SSTs' start and end keys.
/// 2. Each interval between adjacent candidates is weighted by the total size
///    of the input SSTs overlapping it.
/// 3. Interval weights are accumulated in key order, emitting a boundary each
///    time the accumulated weight reaches `total_weight / desired` so the
///    resulting ranges are roughly equally sized.
///
/// The number of ranges is capped both by `max_subcompactions` and by the
/// total estimated input size divided by `min_subcompaction_input_bytes`, so
/// small compactions are not split into fragments.
///
/// ## Returns
/// - Ranges in ascending key order that are non-overlapping and together
///   cover the entire key space. A single unbounded range means the
///   compaction should run unsplit.
pub(crate) fn plan_subcompaction_ranges(
    sst_views: &[SsTableView],
    sorted_runs: &[SortedRun],
    max_subcompactions: usize,
    min_subcompaction_input_bytes: u64,
) -> Vec<BytesRange> {
    let views: Vec<&SsTableView> = sst_views
        .iter()
        .chain(sorted_runs.iter().flat_map(|sr| sr.sst_views.iter()))
        .collect();

    let desired = desired_subcompactions(
        &views,
        max_subcompactions,
        min_subcompaction_input_bytes.max(1),
    );
    if desired <= 1 {
        return vec![BytesRange::unbounded()];
    }

    // Candidate boundaries: the union of input SST start and end keys
    // (unbounded ends contribute no candidate).
    let mut candidates = BTreeSet::new();
    for view in &views {
        let range = view.compacted_effective_range();
        if let Included(key) | Excluded(key) = range.start_bound() {
            candidates.insert(key.clone());
        }
        if let Included(key) | Excluded(key) = range.end_bound() {
            candidates.insert(key.clone());
        }
    }
    let candidates: Vec<Bytes> = candidates.into_iter().collect();
    if candidates.len() < 2 {
        return vec![BytesRange::unbounded()];
    }

    // Weight each candidate interval by the size of every input SST that
    // overlaps it. SSTs spanning multiple intervals contribute their full
    // size to each, per the RFC heuristic.
    let intervals: Vec<(BytesRange, u64)> = candidates
        .windows(2)
        .filter_map(|pair| {
            let interval =
                BytesRange::try_new(Included(pair[0].clone()), Excluded(pair[1].clone()))?;
            let weight = views
                .iter()
                .filter(|view| {
                    interval
                        .intersect(view.compacted_effective_range())
                        .is_some()
                })
                .map(|view| view.estimate_size())
                .sum();
            Some((interval, weight))
        })
        .collect();

    let total_weight: u64 = intervals.iter().map(|(_, weight)| weight).sum();
    let target = total_weight / desired as u64;
    if target == 0 {
        return vec![BytesRange::unbounded()];
    }

    // Accumulate interval weights in key order and emit a boundary at the
    // interval's end each time the accumulated weight reaches the target.
    // The final interval never emits a boundary: its end is the global max
    // key, which must remain inside the last range.
    let mut boundaries: Vec<Bytes> = Vec::new();
    let mut accumulated = 0u64;
    for (interval, weight) in intervals.iter().take(intervals.len() - 1) {
        if boundaries.len() + 1 >= desired {
            break;
        }
        accumulated += weight;
        if accumulated >= target {
            let Excluded(end) = interval.end_bound() else {
                unreachable!("candidate intervals always have excluded end bounds")
            };
            boundaries.push(end.clone());
            accumulated = 0;
        }
    }

    if boundaries.is_empty() {
        return vec![BytesRange::unbounded()];
    }

    // Build the covering ranges: (-inf, b1), [b1, b2), ..., [bk, +inf).
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut start = Unbounded;
    for boundary in boundaries {
        ranges.push(BytesRange::new(start, Excluded(boundary.clone())));
        start = Included(boundary);
    }
    ranges.push(BytesRange::new(start, Unbounded));
    ranges
}

/// Returns the desired number of subcompactions for the given inputs: the
/// configured maximum, capped so each subcompaction covers at least
/// `min_subcompaction_input_bytes` of estimated input.
fn desired_subcompactions(
    views: &[&SsTableView],
    max_subcompactions: usize,
    min_subcompaction_input_bytes: u64,
) -> usize {
    let total_input_bytes: u64 = views.iter().map(|view| view.estimate_size()).sum();
    let max_by_size = (total_input_bytes / min_subcompaction_input_bytes) as usize;
    max_subcompactions.min(max_by_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo};
    use ulid::Ulid;

    const FORMAT_VERSION: u16 = 1;

    fn test_sst_view(first_key: &[u8], last_key: &[u8], size: u64) -> SsTableView {
        // `SsTableHandle::estimate_size` is `index_offset + index_len`; encode
        // the desired size estimate through those fields.
        let info = SsTableInfo {
            first_entry: Some(Bytes::copy_from_slice(first_key)),
            last_entry: Some(Bytes::copy_from_slice(last_key)),
            index_offset: size,
            index_len: 0,
            ..SsTableInfo::default()
        };
        let handle = SsTableHandle::new(SsTableId::Compacted(Ulid::new()), FORMAT_VERSION, info);
        SsTableView::identity(handle)
    }

    fn sorted_run(id: u32, views: Vec<SsTableView>) -> SortedRun {
        SortedRun {
            id,
            sst_views: views,
        }
    }

    fn assert_covering_ranges(ranges: &[BytesRange]) {
        assert!(!ranges.is_empty());
        assert_eq!(ranges.first().unwrap().start_bound(), Unbounded);
        assert_eq!(ranges.last().unwrap().end_bound(), Unbounded);
        for pair in ranges.windows(2) {
            let Excluded(prev_end) = pair[0].end_bound() else {
                panic!("non-terminal ranges must have excluded end bounds");
            };
            let Included(next_start) = pair[1].start_bound() else {
                panic!("non-initial ranges must have included start bounds");
            };
            assert_eq!(prev_end, next_start, "adjacent ranges must be contiguous");
        }
    }

    #[test]
    fn test_should_not_split_when_subcompactions_disabled() {
        let sr = sorted_run(
            1,
            vec![
                test_sst_view(b"a", b"k", 100),
                test_sst_view(b"l", b"z", 100),
            ],
        );

        let ranges = plan_subcompaction_ranges(&[], &[sr], 1, 1);

        assert_eq!(ranges, vec![BytesRange::unbounded()]);
    }

    #[test]
    fn test_should_not_split_small_inputs() {
        // given: 200 bytes of input but a 1000-byte minimum per subcompaction
        let sr = sorted_run(
            1,
            vec![
                test_sst_view(b"a", b"k", 100),
                test_sst_view(b"l", b"z", 100),
            ],
        );

        let ranges = plan_subcompaction_ranges(&[], &[sr], 4, 1000);

        assert_eq!(ranges, vec![BytesRange::unbounded()]);
    }

    #[test]
    fn test_should_split_single_sorted_run_at_sst_boundaries() {
        let sr = sorted_run(
            1,
            vec![
                test_sst_view(b"a", b"f", 100),
                test_sst_view(b"g", b"m", 100),
                test_sst_view(b"n", b"z", 100),
            ],
        );

        let ranges = plan_subcompaction_ranges(&[], &[sr], 3, 1);

        assert_covering_ranges(&ranges);
        assert_eq!(ranges.len(), 3);
        // Boundaries land at the start key of the next SST, so each SST
        // falls wholly within one range and no SST is split.
        assert_eq!(ranges[0].end_bound(), Excluded(&Bytes::from_static(b"g")));
        assert_eq!(ranges[1].end_bound(), Excluded(&Bytes::from_static(b"n")));
    }

    #[test]
    fn test_should_cap_splits_at_max_subcompactions() {
        let views: Vec<SsTableView> = (0..10)
            .map(|i| {
                let first = vec![b'a' + i as u8];
                let last = vec![b'a' + i as u8, b'z'];
                test_sst_view(&first, &last, 100)
            })
            .collect();
        let sr = sorted_run(1, views);

        let ranges = plan_subcompaction_ranges(&[], &[sr], 4, 1);

        assert_covering_ranges(&ranges);
        assert!(ranges.len() <= 4, "got {} ranges", ranges.len());
        assert!(ranges.len() > 1);
    }

    #[test]
    fn test_should_weight_intervals_by_overlapping_sst_size() {
        // given: a large SST covering [a, f] and two small SSTs covering
        // [g, m] and [n, z]. The split should isolate the large SST rather
        // than splitting at the midpoint of the candidate keys.
        let sr = sorted_run(
            1,
            vec![
                test_sst_view(b"a", b"f", 1000),
                test_sst_view(b"g", b"m", 10),
                test_sst_view(b"n", b"z", 10),
            ],
        );

        let ranges = plan_subcompaction_ranges(&[], &[sr], 2, 1);

        assert_covering_ranges(&ranges);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].end_bound(), Excluded(&Bytes::from_static(b"g")));
    }

    #[test]
    fn test_should_split_overlapping_inputs_from_multiple_sources() {
        // given: overlapping L0s and sorted runs, where boundaries land
        // inside some inputs (SSTs shared across ranges).
        let l0s = vec![
            test_sst_view(b"a", b"z", 100),
            test_sst_view(b"c", b"q", 100),
        ];
        let srs = vec![
            sorted_run(
                1,
                vec![
                    test_sst_view(b"a", b"h", 100),
                    test_sst_view(b"i", b"p", 100),
                    test_sst_view(b"q", b"z", 100),
                ],
            ),
            sorted_run(
                2,
                vec![
                    test_sst_view(b"b", b"j", 100),
                    test_sst_view(b"k", b"x", 100),
                ],
            ),
        ];

        let ranges = plan_subcompaction_ranges(&l0s, &srs, 4, 1);

        assert_covering_ranges(&ranges);
        assert!(ranges.len() > 1);
        assert!(ranges.len() <= 4);
    }

    #[test]
    fn test_should_not_split_single_sst() {
        let sr = sorted_run(1, vec![test_sst_view(b"a", b"z", 1000)]);

        let ranges = plan_subcompaction_ranges(&[], &[sr], 4, 1);

        // Only two candidate keys (a, z) produce one interval, and a boundary
        // is never emitted at the final interval's end.
        assert_eq!(ranges, vec![BytesRange::unbounded()]);
    }
}
