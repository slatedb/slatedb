//! # Subcompactions
//!
//! In-memory record of a subcompaction (RFC-0028): a compaction over a
//! sub-range of a parent compaction's key space (see [`Subcompaction`]), plus
//! the boundary-selection planner ([`plan_subcompaction_ranges`]) that decides
//! how to split a logical compaction into those sub-ranges.
//!
//! The overall approach mirrors RocksDB's subcompaction boundary selection; see
//! `ApproximateKeyAnchors` and `GenSubcompactionBoundaries` in RocksDB's
//! `db/compaction/compaction_job.cc`.

use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::Serialize;

use crate::bytes_range::BytesRange;
use crate::db_state::{SortedRun, SsTableHandle, SsTableView};
use crate::error::SlateDBError;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::tablestore::TableStore;

/// A compaction over a sub-range of the parent compaction's key space
/// (RFC-0028).
///
/// Subcompactions are only valid within the context of a parent
/// [`Compaction`](crate::compactor_state::Compaction) and let a single logical
/// compaction execute its disjoint ranges in parallel while resuming at range
/// granularity after a failure. A subcompaction carries no lifecycle status:
/// the parent [`Compaction`](crate::compactor_state::Compaction) owns status,
/// and a range's progress is captured entirely by its recorded `output_ssts`.
/// On resume every range is re-run from its persisted output; a range that
/// already finished has nothing left to merge and completes immediately.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Subcompaction {
    /// Key range covered by this subcompaction. The ranges of the
    /// subcompactions within a parent compaction are non-overlapping and
    /// together cover the full key space.
    range: BytesRange,
    /// Output SSTs produced by this subcompaction so far.
    output_ssts: Vec<SsTableHandle>,
}

impl Subcompaction {
    pub(crate) fn new(range: BytesRange) -> Self {
        Self {
            range,
            output_ssts: Vec::new(),
        }
    }

    pub(crate) fn with_output_ssts(mut self, output_ssts: Vec<SsTableHandle>) -> Self {
        self.output_ssts = output_ssts;
        self
    }

    /// Returns the key range covered by this subcompaction.
    pub(crate) fn range(&self) -> &BytesRange {
        &self.range
    }

    /// Returns the output SSTs produced by this subcompaction so far.
    pub fn output_ssts(&self) -> &Vec<SsTableHandle> {
        &self.output_ssts
    }

    /// Sets the output SSTs produced by this subcompaction, enforcing the
    /// extend-only contract: the new list must start with the previous one,
    /// since the executor only ever appends as a range produces more SSTs.
    pub(crate) fn set_output_ssts(&mut self, output_ssts: Vec<SsTableHandle>) {
        assert!(
            output_ssts.starts_with(self.output_ssts.as_slice()),
            "new subcompaction output SSTs must always extend previous output SSTs"
        );
        self.output_ssts = output_ssts;
    }
}

/// The maximum number of anchors sampled from a single SST's block index.
///
/// Capping anchors per SST bounds the planner's candidate set no matter how many
/// blocks an SST has (a 256 MiB SST with 4 KiB blocks has ~65k blocks), so a
/// wide compaction yields tens of thousands of candidates rather than tens of
/// millions.
const MAX_ANCHORS_PER_SST: usize = 128;

/// Plans the key ranges to split a compaction into subcompactions (RFC-0028).
///
/// Reads each input SST's block index, samples it into at most
/// [`MAX_ANCHORS_PER_SST`] weighted anchors, and places boundaries at the keys
/// that fall on the byte-weight quantiles of the merged anchor stream (see
/// [`select_boundaries`]). Index reads run concurrently, bounded by
/// `max_fetch_tasks`, and warm the same block cache the merge will later use.
///
/// ## Returns
/// - Ranges in ascending key order that are non-overlapping and together cover
///   the entire key space. A single unbounded range means the compaction should
///   run unsplit (subcompactions disabled, inputs too small, or no usable split
///   point).
pub(crate) async fn plan_subcompaction_ranges(
    table_store: &Arc<TableStore>,
    sst_views: &[SsTableView],
    sorted_runs: &[SortedRun],
    max_subcompactions: usize,
    max_sst_size: u64,
    max_fetch_tasks: usize,
) -> Result<Vec<BytesRange>, SlateDBError> {
    if max_subcompactions <= 1 {
        return Ok(vec![BytesRange::unbounded()]);
    }

    // Collect owned input handles up front. Each read future then owns its
    // handle and an `Arc<TableStore>` clone rather than borrowing the input
    // slices, which keeps the enclosing (spawned) compaction future `Send`.
    let handles: Vec<SsTableHandle> = sst_views
        .iter()
        .map(|view| view.sst.clone())
        .chain(
            sorted_runs
                .iter()
                .flat_map(|sr| sr.sst_views.iter().map(|view| view.sst.clone())),
        )
        .collect();

    // Read and sample every input SST's index concurrently (bounded by
    // `max_fetch_tasks`). Each SST yields at most MAX_ANCHORS_PER_SST anchors,
    // so the merged candidate set stays bounded regardless of total data size.
    let anchors: Vec<(Bytes, u64)> = stream::iter(handles)
        .map(|handle| {
            let table_store = table_store.clone();
            async move {
                let index = table_store.read_index(&handle, true).await?;
                // `filter_offset` marks the end of the data-block region: the
                // filter, index, and stats blocks all follow it, and when the
                // SST has no filter it aliases `index_offset`. Using
                // `index_offset` here would charge the filter block's bytes to
                // the last anchor, skewing boundaries toward high keys (or
                // pushing a near-floor compaction over the `max_sst_size` line).
                Ok::<_, SlateDBError>(sample_anchors(
                    &index,
                    handle.info.filter_offset,
                    MAX_ANCHORS_PER_SST,
                ))
            }
        })
        .buffer_unordered(max_fetch_tasks.max(1))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect();

    Ok(select_boundaries(anchors, max_subcompactions, max_sst_size))
}

/// Selects covering key ranges from weighted anchors so each range carries a
/// roughly equal share of the input bytes (RFC-0028).
///
/// The target range size is `max(total_bytes / max_subcompactions, max_sst_size)`:
/// the `max_sst_size` floor keeps each subcompaction to at least one output
/// SST's worth of input, so a small compaction is split into fewer (or zero)
/// ranges rather than fragmented. Anchors are swept in key order, and a
/// boundary opens a new range at the first anchor once the running total of the
/// ranges already closed reaches the next threshold; a running threshold (rather
/// than a per-range reset) keeps rounding error from accumulating.
///
/// ## Returns
/// - Ranges in ascending key order, non-overlapping and covering the whole key
///   space. A single unbounded range means the compaction should run unsplit.
fn select_boundaries(
    mut anchors: Vec<(Bytes, u64)>,
    max_subcompactions: usize,
    max_sst_size: u64,
) -> Vec<BytesRange> {
    if max_subcompactions <= 1 {
        return vec![BytesRange::unbounded()];
    }

    let total: u64 = anchors.iter().map(|(_, bytes)| bytes).sum();
    let target = (total / max_subcompactions as u64).max(max_sst_size);
    // Too small to split: a single range already fits within the target.
    if target == 0 || target >= total {
        return vec![BytesRange::unbounded()];
    }

    // Anchors from different SSTs interleave; sort into one key-ordered stream.
    anchors.sort_by(|a, b| a.0.cmp(&b.0));

    // `cumulative` is the bytes in the ranges already closed (everything strictly
    // before the current anchor). When it reaches the next threshold, the current
    // anchor's key opens a new range, so the anchor — and its weight — belongs to
    // that new range.
    let mut boundaries: Vec<Bytes> = Vec::new();
    let mut threshold = target;
    let mut cumulative = 0u64;
    for (key, bytes) in anchors {
        let distinct = match boundaries.last() {
            // Skip a key equal to the previous boundary: anchors from
            // overlapping SSTs can share a key, and emitting it twice would
            // create an empty range. The next distinct key still lands close to
            // the target.
            Some(last) => key > *last,
            None => true,
        };
        if cumulative >= threshold && boundaries.len() < max_subcompactions - 1 && distinct {
            boundaries.push(key);
            threshold += target;
        }
        cumulative += bytes;
    }

    if boundaries.is_empty() {
        return vec![BytesRange::unbounded()];
    }

    // Build covering ranges: (-inf, b1), [b1, b2), ..., [bk, +inf).
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut start: Bound<Bytes> = Unbounded;
    for boundary in boundaries {
        ranges.push(BytesRange::new(start, Excluded(boundary.clone())));
        start = Included(boundary);
    }
    ranges.push(BytesRange::new(start, Unbounded));
    ranges
}

/// Samples an SST's block index into at most `max_anchors` weighted anchors,
/// each `(block first key, bytes the anchor represents)`.
///
/// A block's on-disk size is the gap between its offset and the next block's;
/// the last block runs to `data_end_offset`, the end of the data-block region
/// (the filter, index, and stats blocks follow it). When the SST has more
/// blocks than `max_anchors`, adjacent blocks are grouped so the anchor count
/// stays bounded: the group's key is its first block's first key and its weight
/// is the summed size of the grouped blocks.
fn sample_anchors(
    index: &SsTableIndexOwned,
    data_end_offset: u64,
    max_anchors: usize,
) -> Vec<(Bytes, u64)> {
    let borrowed = index.borrow();
    let blocks = borrowed.block_meta();
    let num_blocks = blocks.len();
    if num_blocks == 0 {
        return Vec::new();
    }

    // End offset of block `i`: the next block's offset, or `data_end_offset`
    // for the last block (the filter/index blocks immediately follow the data).
    let block_end = |i: usize| -> u64 {
        if i + 1 < num_blocks {
            blocks.get(i + 1).offset()
        } else {
            data_end_offset
        }
    };

    let max_anchors = max_anchors.max(1);
    let stride = num_blocks.div_ceil(max_anchors);
    let mut anchors = Vec::with_capacity(num_blocks.div_ceil(stride));
    let mut block = 0;
    while block < num_blocks {
        let first = blocks.get(block);
        let key = Bytes::copy_from_slice(first.first_key().bytes());
        let group_end = (block + stride).min(num_blocks);
        // Blocks are contiguous, so the group's size is the gap from its first
        // block's offset to its last block's end offset.
        let bytes = block_end(group_end - 1).saturating_sub(first.offset());
        anchors.push((key, bytes));
        block = group_end;
    }
    anchors
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::RangeBounds;

    /// A key for keyspace position `i`, fixed-width so byte order matches `i`.
    fn k(i: u64) -> Bytes {
        Bytes::copy_from_slice(format!("k{i:010}").as_bytes())
    }

    fn assert_covering_ranges(ranges: &[BytesRange]) {
        assert!(!ranges.is_empty());
        assert_eq!(RangeBounds::start_bound(ranges.first().unwrap()), Unbounded);
        assert_eq!(RangeBounds::end_bound(ranges.last().unwrap()), Unbounded);
        // Boundaries are pushed in strictly increasing key order, so every range
        // spans distinct keys. Assert it directly: an empty range would mean two
        // boundaries collapsed onto one key (a duplicated split point).
        for range in ranges {
            assert!(range.non_empty(), "range must be non-empty: {range:?}");
        }
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
        // given: anchors worth splitting, but subcompactions disabled (max == 1)
        let anchors = vec![(k(0), 100), (k(100), 100)];

        // when: boundaries are selected
        let ranges = select_boundaries(anchors, 1, 1);

        // then: the compaction runs unsplit
        assert_eq!(ranges, vec![BytesRange::unbounded()]);
    }

    #[test]
    fn test_should_not_split_inputs_below_max_sst_size() {
        // given: 200 bytes of input but a 1000-byte max_sst_size floor
        let anchors = vec![(k(0), 100), (k(100), 100)];

        // when: boundaries are selected with the floor above the total size
        let ranges = select_boundaries(anchors, 4, 1000);

        // then: the compaction stays whole rather than fragmenting
        assert_eq!(ranges, vec![BytesRange::unbounded()]);
    }

    #[test]
    fn test_should_split_evenly_weighted_anchors() {
        // given: four equal anchors at distinct keys (target = 400 / 4 = 100)
        let anchors = vec![(k(0), 100), (k(10), 100), (k(20), 100), (k(30), 100)];

        // when: boundaries are selected for four subcompactions
        let ranges = select_boundaries(anchors, 4, 1);

        // then: each later anchor opens a new range at k(10), k(20), k(30)
        assert_covering_ranges(&ranges);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].end_bound(), Excluded(&k(10)));
        assert_eq!(ranges[1].end_bound(), Excluded(&k(20)));
        assert_eq!(ranges[2].end_bound(), Excluded(&k(30)));
    }

    #[test]
    fn test_should_cap_splits_at_max_subcompactions() {
        // given: many anchors, far more than the subcompaction cap
        let anchors: Vec<(Bytes, u64)> = (0..100).map(|i| (k(i), 100)).collect();

        // when: boundaries are selected with a cap of four
        let ranges = select_boundaries(anchors, 4, 1);

        // then: the split never exceeds max_subcompactions ranges
        assert_covering_ranges(&ranges);
        assert_eq!(ranges.len(), 4, "must not exceed max_subcompactions ranges");
    }

    #[test]
    fn test_should_isolate_heavy_region() {
        // given: a heavy front anchor carrying half the bytes (target = 1040 / 2 = 520)
        let anchors = vec![
            (k(0), 1000),
            (k(10), 10),
            (k(20), 10),
            (k(30), 10),
            (k(40), 10),
        ];

        // when: boundaries are selected for two subcompactions
        let ranges = select_boundaries(anchors, 2, 1);

        // then: the heavy anchor alone exceeds the target, so it gets its own range
        assert_covering_ranges(&ranges);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].end_bound(), Excluded(&k(10)));
    }

    #[test]
    fn test_should_not_split_single_anchor() {
        // given: a single anchor (e.g. a single-block SST) below the floor
        let anchors = vec![(k(0), 1000)];

        // when: boundaries are selected
        let ranges = select_boundaries(anchors, 4, 1000);

        // then: the SST stays whole
        assert_eq!(ranges, vec![BytesRange::unbounded()]);
    }

    #[test]
    fn test_should_handle_duplicate_keys_without_empty_ranges() {
        // given: overlapping SSTs contributing anchors at the same keys
        let anchors = vec![
            (k(0), 500),
            (k(0), 500),
            (k(10), 500),
            (k(10), 500),
            (k(20), 500),
            (k(20), 500),
        ];

        // when: boundaries are selected
        let ranges = select_boundaries(anchors, 4, 1);

        // then: it splits without ever emitting two boundaries at one key.
        // assert_covering_ranges checks every range is non-empty, so a
        // duplicated split point (which would also panic BytesRange::new) fails
        // here explicitly.
        assert_covering_ranges(&ranges);
        assert!(ranges.len() > 1);
    }
}
