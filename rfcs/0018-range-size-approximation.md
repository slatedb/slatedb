# Range Size Approximation and Estimate Key Count

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [API](#api)
   * [Configuration Options for approximate size](#configuration-options-for-approximate-size)
   * [Implementation Phases](#implementation-phases)
      + [Workstream 1 - `get_approximate_size`](#workstream-1-get_approximate_size)
         - [Phase 1](#phase-1)
         - [Phase 2 - Configurable Options](#phase-2-configurable-options)
         - [Phase 3: Index-Block-Based Refinement](#phase-3-index-block-based-refinement)
      + [Workstream 2 - SST stats block](#workstream-2-sst-stats-block)
         - [Phase 1](#phase-1-1)
         - [Phase 2](#phase-2)
         - [Phase 3](#phase-3)
- [Impact Analysis](#impact-analysis)
   * [Core API & Query Semantics](#core-api-query-semantics)
   * [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   * [Time, Retention, and Derived State](#time-retention-and-derived-state)
   * [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   * [Compaction](#compaction)
   * [Storage Engine Internals](#storage-engine-internals)
   * [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
   * [Performance & Cost](#performance-cost)
   * [Observability](#observability)
   * [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [FiV0](https://github.com/FiV0)

## Summary

The RFC proposes adding two methods to the `Db` API and adding a stats block to the SST footer to better support these methods.

The two methods added to `Db` API will be:
- `get_approximate_size(range)` - a method to get a size estimation (compressed or uncompressed) for a given range
- `estimate_key_count(range)` - a method to get a estimate of the key count for a given range

The stats block in the SST footer will contain:
- num_puts
- num_deletes
- num_merge_ops
- raw_key_size
- raw_val_size

## Motivation

Users want to understand data distribution and storage usage for specific key ranges to support:

- **Cost estimation**: Calculating storage costs or scan costs for specific data partitions. For example, if a key range is an index in a query engine, it is good to know an approximate size when joining with other data.
- **Bulk operations**: Planning exports, migrations, or splits based on estimated sizes
- **Capacity planning**: Understanding storage requirements for different key ranges
- **Data modeling**: Understanding how data is distributed across the key space

Currently there is no better way then to scan the whole range to get an estimate or approximation of the data size.
The two methods serve slightly different purposes. `get_approximate_size` is about data size (compressed or uncompressed) and does not say much about key distribution. `estimate_key_count` gives a rough estimate of number of keys in a range. Together they can be used to get an average key size for a range.

## Goals

- Provide APIs to estimate the on-disk (compressed) and in-memory (uncompressed) sizes of key ranges
- Support configurable accuracy levels, trading I/O for precision
- Allow users to include/exclude memtables and include/exclude SST sizes from estimates
- Allow users to get an approximate key count for a range

## Non-Goals

- Exact size calculation or key counting (would require reading and processing a lot more data)

## Design

### API

The following APIs will be added to `Db`:

```rust
/// Get approximate size with configuration options.
pub async fn get_approximate_size_with_options<K, T>(
    &self,
    range: T,
    options: &SizeApproximationOptions,
) -> Result<u64, crate::Error>
where
    K: AsRef<[u8]> + Send,
    T: RangeBounds<K> + Send,

/// Estimate the number of keys in a key range.
///
/// Returns an approximation of the number of active keys (puts + merges - deletes)
/// within the specified range. This uses statistics from the SST footer stats block.
pub async fn estimate_key_count<K, T>(
    &self,
    range: T,
) -> Result<u64, crate::Error>
where
    K: AsRef<[u8]> + Send,
    T: RangeBounds<K> + Send,
```

### Configuration Options for approximate size

```rust
pub struct SizeApproximationOptions {
    /// Include size from in-memory memtables (current and immutable).
    /// Default: false
    pub include_memtables: bool,

    /// Include size from on-disk SST files (L0 and compacted).
    /// Default: true
    pub include_files: bool,

    /// Error margin for controlling accuracy vs I/O trade-off.
    /// Default: None
    pub error_margin: Option<f64>,
}
```


### Implementation Phases

There are essentially two work streams:
- `get_approximate_size` without stats footer changes (Workstream 1)
- stats footer changes + `estimate_key_count` + adding the stats for usage in `get_approximate_size` (Workstream 2)

#### Workstream 1 - `get_approximate_size`

This almost mirrors what RocksDB does for `get_approximate_size`.

##### Phase 1

In the initial implementation, `get_approximate_size` will:

1. Iterate through all SSTs (L0 and compacted ones) and identify those overlapping the requested range
2. Sum the sizes of all overlapping SSTs using `tables_covering_range` + `estimate_size` for compacted SSTs and simply `intersects_range` + `estimate_size` for L0s.

This requires no additional I/O and used in-memory metadata only. It might be a very crude estimate and becomes worse for small ranges.


##### Phase 2 - Configurable Options

Add support for `SizeApproximationOptions`. Only add `include_memtables` and `include_files` options for now. One might be able to do some
more expensive operations for memtables (there are in memory anyway). This will mix compressed and uncompressed data if both options are set.

##### Phase 3: Index-Block-Based Refinement

Implement the `error_margin` logic for improved accuracy. Here we would be more accurate and do more IO if needed.
Some SST will be fully overlapping the range, so we only look at their SST metadata (take the full size). Let `partial_sst_size` be the total size of all partially overlapping SST files.
If `partial_sst_size / total_estimate <= error_margin` we skip further refinement and return what we calculated so far. If the condition doesn't hold we refine the estimate by looking at index block data.
We binary search the index block where the first key overlaps and take the accumulated size to the left or right (depending on whether we are calculating upper bound or lower bound).
We are trading IO for accuracy. If this new estimate still below the error margin one could potentially bring in the raw data block where the boundary lies and do full
scan (or binary search) in that data block, thereby trading even more I/O for accuracy.

So far all of these steps can be done without any changes to the storage format. Be aware that if compression is enabled the above approach will be an estimate on compressed data size.

#### Workstream 2 - SST stats block

##### Phase 1

The first piece of work would simply add a stats block with the following fields:
- `num_puts` - u32
- `num_deletes` - u32
- `num_merge_ops` - u32
- `raw_key_size` - u64
- `raw_val_size` - u64

For consistency these could all become `u64`, as the space saved by the former ones is quite negligible.

This requires a bit of care as the storage format changes, i.e. the .fsb gets updated.

TBD: Should these changes somehow be backwards compatible?

##### Phase 2

Add `estimate_key_count()` to SST and `estimate_key_count(range)` to `Db`. Use the formula `num_puts + num_merge_ops - num_deletes` to estimate the key count in SST files.
Here we also use overlapping SST files as crude estimate.

Question: Can this be made more accurate similar to workstream 1 by trading IO for accuracy? To make this work one would need the three `num` stats also at block metadata level (`BlockMeta`)
another 24 bytes per block. Another option is to use average key size for the range and extrapolate from the first overlapping block a size for
the partially overlapping SST if higher accuracy is needed. Without uniform key distribution this estimate might be quite wrong.

##### Phase 3

Use `raw_key_size` + `raw_val_size` to get a more accurate uncompressed estimate in workstream 1. Before the SST estimates were purely compressed estimates?

Open Question(s): How to best deal with compressed vs uncompressed in this case? Another option for for `SizeApproximateOptions`? If everything should be using uncompressed
then one needs to also add `raw_key_size` and `raw_value_size` to `BlockMeta` if the error margin should remain accurate once we do binary search over block indices.


## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`) - Adds new read API
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Logical clocks
- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [x] Block cache - May cache index blocks in Phase 3
- [x] Object store cache - Will read index blocks from object storage in Phase 3
- [x] Indexing (bloom filters, metadata) - Uses existing SST index blocks
- [ ] SST format or block format - No format changes required

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc) - New API to expose in bindings
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

Ignoring the cost of the potential memtable scans in the following as they seem negligible.

`get_approximate_size` without stats block changes:
- Phase 1-2: Only reads in-memory SSTableInfo.
- Phase 3: ~300Kb - 500kb I/O for reading index blocks if refinement kicks in

SST stats block footer:
- 28 bytes per SST space amplification. This seems negligible.

Throughput:
- No impact on writes or compaction
- Read throughput for size estimation scales with number of overlapping SSTs

Miscalculation:
- 4-8 x potential miscalculation if most keys sit in recent L0 as these are non compacted
- If keys get frequently written to, every level will likely count the key once. So there is some overestimation.

Latency:
  - As the `error_margin` goes down and the range becomes larger the average latency will go up.

Object store requests:
- `get_approximate_size` - ~300-500kb per partially overlapping SST and the refinement via `error_margin` kicks in. I think the cost per query are negligible.
- `estimate_key_count` - currently proposal only uses stats from footer, so no additional cost.

Space, read, and write amplification:
- Little impact on space amplification
- No impact on write amplification
- No impact on read amplification (excluding the new API's)

### Observability

- non that I can think of

### Compatibility

- As SST files get a new stats block in workstream 2, the new API's won't work with old data files.
- There is still a TODO in `sst.rs` that wants to deal with backwards compatibility. We will not address backwards compatibility when implementing this feature.

- New APIs are additive only
- No breaking changes to existing APIs
- Language bindings will need to expose new methods

## Testing

Unit tests:
- SST overlap detection logic (full vs partial overlaps)
- Index block parsing and `BlockMeta` offset calculations
- Binary search for first/last blocks within range
- Edge cases: empty ranges, single-block SSTs, unbounded ranges
- Option handling: various combinations of `include_memtables`, `include_files`, `error_margin`

Not planning to add any specific tests for
- Integration tests
- Fault-injection/chaos tests
- Deterministic simulation tests
- Formal methods verification
- Performance tests

## Rollout

- The implementation phase outlines clearly 6 commits that can be done quite independently (modulo decisions to be taken).
- Add API to other language bindings
- Docs update

## Alternatives

I think there are quite a few tradeoffs here which need to be addressed via discussion:
- compressed vs uncompressed. I assume some people are interested in size on disk, others in size in-memory.
- IO vs accuracy tradeoffs
- Space amplification vs accuracy - This comes back to better accuracy for `estimate_key

One alternative that has not been mentioned so far is sample-based estimation. Sample some N random blocks and
estimate key counts or compression ratio, finally extrapolate from these.

This proposal does not include things like fragmentation or overhead for memtables. It deals with estimates on the compressed and uncompressed
data.

## Open Questions

- How to best expose compressed and uncompressed data size estimation?
- How to deal with mixing compressed and uncompressed estimates nicely?
- How to best approach the unification of the two work streams?
- How accurate should the API be? For `get_approximate_size` and `estimate_key_count`. Do we just except that for certain cases even with error_margin we might be off?
- What should be the recommended default `error_margin` value for users wanting "balanced" accuracy?

## References

- [Issue #905: Key Count (or Approximate)](https://github.com/slatedb/slatedb/issues/905)
- [Comment proposing this design](https://github.com/slatedb/slatedb/issues/905#issuecomment-3765276221)
- [RocksDB GetApproximateSizes Documentation](https://github.com/facebook/rocksdb/wiki/Approximate-Size)
- [RocksDB BlockBasedTable Format](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format)

## Updates
