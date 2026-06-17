//! Batched per-SST point reads for `multi_get`.
//!
//! [`read_sst_for_keys`] visits a single SST exactly once for every key in a
//! batch that it could satisfy. Unlike calling [`crate::sst_iter::SstIterator`]
//! once per key — which would reload the index and filters and issue a separate
//! object-store GET for every key — it loads the index and filters once, prunes
//! keys with the bloom filter, maps the survivors to blocks, coalesces those
//! blocks into as few object-store reads as possible (reusing
//! [`crate::tablestore::TableStore::read_blocks_using_index`], which already
//! handles caching, range coalescing, and parallel decode), and scans each
//! fetched block for its keys.
//!
//! The reader returns the raw `RowEntry`s each SST holds for a key, newest
//! first. Sequence/merge/tombstone resolution is intentionally left to the
//! orchestrator ([`crate::reader::Reader`]) so the final value is produced by
//! the exact same components the single-key `get` path uses.

use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::block_iterator::DataBlockIterator;
use crate::bytes_range::BytesRange;
use crate::db_state::{SsTableHandle, SsTableView};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::filter_policy::{FilterQuery, NamedFilter};
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::iter::IterationOrder;
use crate::partitioned_keyspace;
use crate::sst_iter::{all_filters_might_match, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::types::RowEntry;

/// Number of intervening unwanted blocks tolerated when coalescing the set of
/// blocks a batch of keys maps to within one SST. Two wanted blocks separated
/// by at most this many gap blocks are fetched as one coalesced object-store
/// range: reading a couple of extra 4 KiB blocks is cheaper than a second round
/// trip on object stores like S3.
const COALESCE_GAP_BLOCKS: usize = 2;

/// A key from the `multi_get` batch paired with the slot it should resolve into
/// (an index into the orchestrator's deduplicated key list), so per-SST results
/// can be scattered back to the correct accumulator.
#[derive(Clone, Debug)]
pub(crate) struct PendingKey {
    pub(crate) idx: usize,
    pub(crate) key: Bytes,
}

/// One key that survived pruning, paired with the contiguous block range that
/// may hold its versions.
struct Candidate {
    idx: usize,
    key: Bytes,
    blocks: Range<usize>,
}

/// Visit one SST once for every key in `keys` it could satisfy.
///
/// Returns, for each key that produced at least one entry, the collected
/// `RowEntry`s in sequence-descending (newest-first) order. Entries are not
/// sequence-filtered or merge-resolved here.
pub(crate) async fn read_sst_for_keys(
    view: &SsTableView,
    keys: &[PendingKey],
    table_store: &Arc<TableStore>,
    options: &SstIteratorOptions,
    db_stats: Option<&DbStats>,
) -> Result<Vec<(usize, Vec<RowEntry>)>, SlateDBError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let handle = &view.sst;

    // Step 1: load the SST index and filters once for the whole batch.
    let index = table_store.read_index(handle, options.cache_blocks).await?;
    let filters = table_store
        .read_filters(handle, options.cache_blocks)
        .await?;
    if index.borrow().block_meta().is_empty() {
        return Ok(Vec::new());
    }

    let candidates = plan_candidates(view, keys, &index, &filters, options, db_stats);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let blocks = fetch_candidate_blocks(
        handle,
        &index,
        &candidates,
        options.cache_blocks,
        table_store,
    )
    .await?;

    scan_candidates(
        handle.format_version,
        &candidates,
        &blocks,
        !filters.is_empty(),
        db_stats,
    )
    .await
}

/// Step 2: prune keys by visible range and bloom filter, then map each survivor
/// to the block range that may hold its versions. Uses the same
/// `partitions_covering_range` the single-key path uses, so a key whose versions
/// span a block boundary is covered correctly. Filter positives/negatives are
/// recorded here; false positives are recorded after scanning (see
/// [`scan_candidates`]).
fn plan_candidates(
    view: &SsTableView,
    keys: &[PendingKey],
    index: &Arc<SsTableIndexOwned>,
    filters: &[NamedFilter],
    options: &SstIteratorOptions,
    db_stats: Option<&DbStats>,
) -> Vec<Candidate> {
    let mut candidates = Vec::with_capacity(keys.len());
    for pk in keys {
        // Visible-range projection (segments / clones). For identity views this
        // also prunes keys outside the SST's physical key range. Keys pruned
        // here are out of range, not filter false positives, so no stats.
        if view
            .calculate_view_range(BytesRange::from_slice(pk.key.as_ref()..=pk.key.as_ref()))
            .is_none()
        {
            continue;
        }

        if !filters.is_empty() {
            let query =
                FilterQuery::point(pk.key.clone()).with_context(options.filter_context.clone());
            if !all_filters_might_match(filters, &query) {
                if let Some(stats) = db_stats {
                    stats.sst_filter_point_negatives.increment(1);
                }
                continue;
            }
            if let Some(stats) = db_stats {
                stats.sst_filter_point_positives.increment(1);
            }
        }

        let blocks = partitioned_keyspace::partitions_covering_range(
            &index.borrow(),
            Included(pk.key.as_ref()),
            Included(pk.key.as_ref()),
        );
        candidates.push(Candidate {
            idx: pk.idx,
            key: pk.key.clone(),
            blocks,
        });
    }
    candidates
}

/// Step 3: collect the union of the candidates' block ranges, coalesce them into
/// contiguous runs, and fetch each run once. `read_blocks_using_index` handles
/// caching, per-run range coalescing of uncached blocks, and parallel decode.
async fn fetch_candidate_blocks(
    handle: &SsTableHandle,
    index: &Arc<SsTableIndexOwned>,
    candidates: &[Candidate],
    cache_blocks: bool,
    table_store: &Arc<TableStore>,
) -> Result<BTreeMap<usize, Arc<Block>>, SlateDBError> {
    let mut needed: Vec<usize> = Vec::new();
    for cand in candidates {
        needed.extend(cand.blocks.clone());
    }
    needed.sort_unstable();
    needed.dedup();

    let mut blocks: BTreeMap<usize, Arc<Block>> = BTreeMap::new();
    for run in coalesce_runs(&needed, COALESCE_GAP_BLOCKS) {
        let fetched = table_store
            .read_blocks_using_index(handle, index.clone(), run.clone(), cache_blocks)
            .await?;
        for (offset, block) in fetched.into_iter().enumerate() {
            blocks.insert(run.start + offset, block);
        }
    }
    Ok(blocks)
}

/// Step 4: scan each candidate's block range for its key, collecting that key's
/// `RowEntry`s newest-first. A candidate that yields nothing despite passing the
/// bloom filter is recorded as a filter false positive.
async fn scan_candidates(
    sst_version: u16,
    candidates: &[Candidate],
    blocks: &BTreeMap<usize, Arc<Block>>,
    filters_present: bool,
    db_stats: Option<&DbStats>,
) -> Result<Vec<(usize, Vec<RowEntry>)>, SlateDBError> {
    let mut out = Vec::with_capacity(candidates.len());
    for cand in candidates {
        let entries = scan_candidate_key(sst_version, cand, blocks).await?;
        if entries.is_empty() {
            if filters_present {
                if let Some(stats) = db_stats {
                    stats.sst_filter_point_false_positives.increment(1);
                }
            }
        } else {
            out.push((cand.idx, entries));
        }
    }
    Ok(out)
}

/// Scan a single candidate's block range for its key, collecting matching
/// entries in iteration (sequence-descending) order. Stops as soon as a larger
/// key is seen, since later blocks only hold larger keys.
async fn scan_candidate_key(
    sst_version: u16,
    cand: &Candidate,
    blocks: &BTreeMap<usize, Arc<Block>>,
) -> Result<Vec<RowEntry>, SlateDBError> {
    let mut entries = Vec::new();
    for bi in cand.blocks.clone() {
        let Some(block) = blocks.get(&bi) else {
            // Every block in a candidate's range was added to `needed` and
            // fetched, so this is unreachable in practice.
            break;
        };
        let mut iter =
            DataBlockIterator::new(block.clone(), sst_version, IterationOrder::Ascending)?;
        iter.seek(cand.key.as_ref()).await?;
        while let Some(entry) = iter.next().await? {
            match entry.key.as_ref().cmp(cand.key.as_ref()) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => entries.push(entry),
                std::cmp::Ordering::Greater => return Ok(entries),
            }
        }
    }
    Ok(entries)
}

/// Group sorted, deduplicated block indices into contiguous fetch ranges,
/// merging two runs separated by at most `gap` intervening blocks.
fn coalesce_runs(sorted_blocks: &[usize], gap: usize) -> Vec<Range<usize>> {
    let mut runs: Vec<Range<usize>> = Vec::new();
    for &b in sorted_blocks {
        if let Some(last) = runs.last_mut() {
            if b < last.end {
                continue; // already covered
            }
            if b - last.end <= gap {
                last.end = b + 1; // extend, absorbing the small gap
                continue;
            }
        }
        runs.push(b..b + 1);
    }
    runs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesce_runs_merges_within_gap_and_splits_beyond() {
        // Adjacent blocks merge.
        assert_eq!(coalesce_runs(&[2, 3], 2), vec![2..4]);
        // Gap within threshold merges (absorbs block 4).
        assert_eq!(coalesce_runs(&[2, 3, 5], 2), vec![2..6]);
        // Gap beyond threshold splits.
        assert_eq!(coalesce_runs(&[2, 3, 9], 2), vec![2..4, 9..10]);
        // Single block.
        assert_eq!(coalesce_runs(&[7], 2), vec![7..8]);
        // Empty.
        assert_eq!(coalesce_runs(&[], 2), Vec::<Range<usize>>::new());
        // One intervening block at gap=1 merges.
        assert_eq!(coalesce_runs(&[0, 2], 1), vec![0..3]);
        // One intervening block at gap=0 splits.
        assert_eq!(coalesce_runs(&[0, 2], 0), vec![0..1, 2..3]);
    }
}
