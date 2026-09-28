//! One SST of a layer walk: the filter probe, the index, the block reads and the entries.

use std::collections::{BTreeSet, HashMap};
use std::ops::{Bound, Range};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use tokio::sync::{Semaphore, SemaphorePermit};
use ulid::Ulid;

use super::keys::{LayerEntries, OpenKey};
use crate::block_iterator::DataBlockIterator;
use crate::config::MultiGetOptions;
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::filter_policy::{FilterQuery, NamedFilter};
use crate::flatbuffer_types::{SsTableIndex, SsTableIndexOwned};
use crate::format::block::Block;
use crate::iter::IterationOrder;
use crate::manifest::SsTableView;
use crate::partitioned_keyspace::partitions_covering_range;
use crate::reader::{ReadTrace, SstTraceLevel};
use crate::tablestore::TableStore;
use crate::types::RowEntry;

/// What every SST read of one batch shares.
pub(super) struct ReadContext<'a> {
    pub(super) table_store: &'a TableStore,
    pub(super) db_stats: &'a DbStats,
    pub(super) read_trace: &'a ReadTrace,
    pub(super) options: &'a MultiGetOptions,
    /// One permit per object store request in flight.
    pub(super) permits: &'a Semaphore,
}

impl ReadContext<'_> {
    async fn permit(&self) -> SemaphorePermit<'_> {
        self.permits
            .acquire()
            .await
            .expect("the semaphore lives as long as the batch")
    }
}

/// One SST of one layer walk, with the keys that reach it.
pub(super) struct SstRead {
    view: SsTableView,
    level: SstTraceLevel,
    segment: Bytes,
    keys: Vec<OpenKey>,
}

impl SstRead {
    pub(super) fn new(
        view: SsTableView,
        level: SstTraceLevel,
        segment: Bytes,
        keys: Vec<OpenKey>,
    ) -> Self {
        Self {
            view,
            level,
            segment,
            keys,
        }
    }

    pub(super) fn view_id(&self) -> Ulid {
        self.view.id
    }

    #[cfg(test)]
    pub(super) fn keys(&self) -> &[OpenKey] {
        &self.keys
    }

    pub(super) fn push_key(&mut self, key: OpenKey) {
        self.keys.push(key);
    }

    /// Reads the entries of every key: filters, then the index, then the blocks.
    pub(super) async fn read(self, ctx: &ReadContext<'_>) -> Result<LayerEntries, SlateDBError> {
        let handle = &self.view.sst;
        // The filters and the index are cached as `get` caches them.
        let filters = {
            let _permit = ctx.permit().await;
            ctx.table_store
                .read_filters(
                    handle,
                    true,
                    Some(self.segment.clone()),
                    ctx.read_trace,
                    Some(&self.level),
                )
                .await?
        };
        let filtered = !filters.is_empty();
        let keys = self.probe_filters(&filters, ctx);
        if keys.is_empty() {
            return Ok(LayerEntries::default());
        }

        let index = {
            let _permit = ctx.permit().await;
            ctx.table_store
                .read_index(
                    handle,
                    true,
                    Some(self.segment.clone()),
                    ctx.read_trace,
                    Some(&self.level),
                )
                .await?
        };
        let key_blocks: Vec<(OpenKey, Range<usize>)> = keys
            .into_iter()
            .map(|key| {
                let blocks = partitions_covering_range(
                    &index.borrow(),
                    Bound::Included(key.key.as_ref()),
                    Bound::Included(key.key.as_ref()),
                );
                (key, blocks)
            })
            .collect();
        let blocks = self.fetch_blocks(&index, &key_blocks, ctx).await?;

        let mut entries = LayerEntries::default();
        for (key, range) in key_blocks {
            let found = self.entries_of(&blocks, &key.key, range).await?;
            if filtered && found.is_empty() {
                ctx.db_stats.sst_filter_point_false_positives.increment(1);
            }
            entries.push(key.index, found);
        }
        Ok(entries)
    }

    /// Keeps the keys that every filter can hold. No filter keeps every key.
    fn probe_filters(&self, filters: &[NamedFilter], ctx: &ReadContext<'_>) -> Vec<OpenKey> {
        if filters.is_empty() {
            return self.keys.clone();
        }
        // One span per filter. Its result is true when any key passes.
        let mut spans: Vec<(tracing::Span, bool)> = filters
            .iter()
            .map(|filter| {
                let span = ctx.read_trace.new_evaluate_filter_span(
                    self.view.sst.id,
                    Some(&self.level),
                    &filter.name,
                );
                (span, false)
            })
            .collect();
        let mut passed = Vec::new();
        let mut negatives = 0;
        for open in &self.keys {
            let query = FilterQuery::point(open.key.clone())
                .with_context(ctx.options.filter_context.clone());
            let might_match = filters
                .iter()
                .zip(spans.iter_mut())
                .all(|(filter, (span, hit))| {
                    let _guard = span.enter();
                    let result = filter.filter.might_match(&query);
                    *hit |= result;
                    result
                });
            if might_match {
                passed.push(open.clone());
            } else {
                negatives += 1;
            }
        }
        for (span, hit) in spans {
            span.record("result", hit);
        }
        ctx.db_stats
            .sst_filter_point_positives
            .increment(passed.len() as u64);
        ctx.db_stats.sst_filter_point_negatives.increment(negatives);
        passed
    }

    /// Reads the distinct blocks of the keys, near blocks in one ranged request.
    async fn fetch_blocks(
        &self,
        index: &Arc<SsTableIndexOwned>,
        key_blocks: &[(OpenKey, Range<usize>)],
        ctx: &ReadContext<'_>,
    ) -> Result<HashMap<usize, Arc<Block>>, SlateDBError> {
        let wanted: BTreeSet<usize> = key_blocks
            .iter()
            .flat_map(|(_, blocks)| blocks.clone())
            .collect();
        let ranges = {
            let borrowed = index.borrow();
            let blocks: Vec<BlockBytes> = wanted
                .into_iter()
                .map(|block| BlockBytes {
                    block,
                    bytes: self.block_bytes(&borrowed, block),
                })
                .collect();
            BlockRanges::plan(
                &blocks,
                ctx.options.coalesce_gap_bytes as u64,
                ctx.options.max_coalesced_bytes as u64,
            )
        };

        let reads = ranges.into_iter().map(|range| async move {
            let _permit = ctx.permit().await;
            let read = ctx
                .table_store
                .read_blocks_using_index(
                    &self.view.sst,
                    index.clone(),
                    range.clone(),
                    ctx.options.cache_blocks,
                    Some(self.segment.clone()),
                )
                .await?;
            Ok::<_, SlateDBError>((range.start, read))
        });
        let mut blocks = HashMap::new();
        for (start, read) in try_join_all(reads).await? {
            for (offset, block) in read.into_iter().enumerate() {
                blocks.insert(start + offset, block);
            }
        }
        Ok(blocks)
    }

    /// The byte range of one block, as `SsTableFormat::block_range` computes it.
    fn block_bytes(&self, index: &SsTableIndex<'_>, block: usize) -> Range<u64> {
        let info = &self.view.sst.info;
        let block_meta = index.block_meta();
        let start = block_meta.get(block).offset();
        let end = if block + 1 < block_meta.len() {
            block_meta.get(block + 1).offset()
        } else if info.filter_len > 0 {
            info.filter_offset
        } else {
            info.index_offset
        };
        start..end
    }

    /// The entries of one key from its blocks, newest first.
    async fn entries_of(
        &self,
        blocks: &HashMap<usize, Arc<Block>>,
        key: &Bytes,
        range: Range<usize>,
    ) -> Result<Vec<RowEntry>, SlateDBError> {
        let mut entries = Vec::new();
        for block in range {
            let block = blocks
                .get(&block)
                .expect("every block of a key was read")
                .clone();
            let mut iter = DataBlockIterator::new(
                block,
                self.view.sst.format_version,
                IterationOrder::Ascending,
            )?;
            iter.seek(key).await?;
            while let Some(entry) = iter.next().await? {
                if entry.key != *key {
                    break;
                }
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

/// One wanted block and its byte range in the SST.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockBytes {
    block: usize,
    bytes: Range<u64>,
}

/// The block ranges of one SST read. Near blocks share one range.
#[derive(Debug, PartialEq, Eq)]
struct BlockRanges(Vec<Range<usize>>);

impl BlockRanges {
    /// Merges sorted, distinct blocks. Two ranges join when the gap between them
    /// is at most `gap_bytes` and the joined range is at most `max_bytes`.
    fn plan(blocks: &[BlockBytes], gap_bytes: u64, max_bytes: u64) -> Self {
        let mut ranges: Vec<Range<usize>> = Vec::new();
        let mut current: Option<(Range<usize>, Range<u64>)> = None;
        for next in blocks {
            if let Some((blocks, bytes)) = current.as_mut() {
                let gap = next.bytes.start.saturating_sub(bytes.end);
                let joined = next.bytes.end.saturating_sub(bytes.start);
                if gap <= gap_bytes && joined <= max_bytes {
                    blocks.end = next.block + 1;
                    bytes.end = next.bytes.end;
                    continue;
                }
                ranges.push(blocks.clone());
            }
            current = Some((next.block..next.block + 1, next.bytes.clone()));
        }
        if let Some((blocks, _)) = current {
            ranges.push(blocks);
        }
        Self(ranges)
    }
}

impl IntoIterator for BlockRanges {
    type Item = Range<usize>;
    type IntoIter = std::vec::IntoIter<Range<usize>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn blocks(bytes: &[(usize, Range<u64>)]) -> Vec<BlockBytes> {
        bytes
            .iter()
            .map(|(block, bytes)| BlockBytes {
                block: *block,
                bytes: bytes.clone(),
            })
            .collect()
    }

    #[rstest]
    #[case::empty(vec![], 0, 1000, vec![])]
    #[case::one_block(vec![(3, 300..400)], 0, 1000, vec![3..4])]
    #[case::adjacent_blocks_join(vec![(0, 0..100), (1, 100..200)], 0, 1000, vec![0..2])]
    #[case::gap_within_limit_joins(vec![(0, 0..100), (2, 200..300)], 100, 1000, vec![0..3])]
    #[case::gap_above_limit_splits(vec![(0, 0..100), (2, 200..300)], 50, 1000, vec![0..1, 2..3])]
    #[case::size_cap_splits(vec![(0, 0..600), (1, 600..1200)], 0, 1000, vec![0..1, 1..2])]
    #[case::block_above_cap_reads_alone(
        vec![(0, 0..2000), (1, 2000..2100), (2, 2100..2200)],
        0,
        1000,
        vec![0..1, 1..3],
    )]
    fn test_block_ranges_plan(
        #[case] wanted: Vec<(usize, Range<u64>)>,
        #[case] gap_bytes: u64,
        #[case] max_bytes: u64,
        #[case] expected: Vec<Range<usize>>,
    ) {
        let ranges = BlockRanges::plan(&blocks(&wanted), gap_bytes, max_bytes);

        assert_eq!(ranges, BlockRanges(expected));
    }
}
