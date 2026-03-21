# Partitioned SST Index for SlateDB

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->
- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [Overview](#overview)
   * [Schema changes (`schemas/sst.fbs`)](#schema-changes-schemassstfbs)
   * [Format version](#format-version)
   * [Write path (`EncodedSsTableFooterBuilder::build`)](#write-path-encodedsstablefooterbuilderbuild)
   * [Read path (`SsTableFormat::read_index`)](#read-path-sstableformatread_index)
   * [Metadata cache changes (`slatedb/src/db_cache/mod.rs`)](#metadata-cache-changes-slatedbsrcdb_cachemodrs)
      + [Cache key structure](#cache-key-structure)
      + [Cache stats](#cache-stats)
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
   * [Multi-level manifest with smaller SSTs (rejected)](#multi-level-manifest-with-smaller-ssts-rejected)
   * [Status quo (rejected)](#status-quo-rejected)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)
<!-- TOC end -->

Status: Draft

Authors:

* [Ryan Dielhenn](https://github.com/ryandielhenn)

## Summary

<!-- Briefly explain what this RFC changes and why. Prefer 3–6 sentences. -->

Currently, each SST in SlateDB has a single monolithic index block and bloom filter that must be loaded in full before any data block can be read. For a large SST with 4KB blocks this means loading several MB of metadata to locate a single 4KB value.

This RFC proposes replacing the flat index with a two-level partitioned index: a small top-level directory that points to a set of partition index blocks, each covering a contiguous range of data blocks. Only the partition blocks that are actually accessed need to be kept in cache rather than the full index for every SST, significantly reducing the block cache memory consumed by index metadata.


## Motivation

<!-- What problem are we solving? What user or system pain exists today? Include concrete examples and why "do nothing" is insufficient. -->

With a default 4KB block size, a 1GB SST contains ~256K blocks. The index stores a separator key and an 8-byte block offset per entry — measured at ~8MB uncompressed and ~4.5MB compressed (Snappy) for UUID keys. The in-memory representation is uncompressed and must be held in the block cache in full. The bloom filter adds further overhead on top of that.

This creates two concrete problems:

**Cache inefficiency:** The index and filter for a single SST consume several MB of block cache. For workloads that touch many SSTs concurrently, this metadata overhead crowds out data blocks that are far more likely to be reused.

**Disk cache I/O throughput:** When indexes are not in the block cache they must be read from the local disk cache (NVMe). Because the index is monolithic, every miss reads the full ~4.5MB even though only a small slice is needed. At ~4GB/s NVMe throughput, reading a 4MB index takes ~1ms vs ~0.1ms for a 4KB partition block — a 10x difference. Note that this problem does not apply to object store reads, where latency is ~50-100ms regardless of object size due to first-byte costs.

Both problems scale with SST size. Reducing the max SST size is not a practical mitigation, as it would increase the number of files and drive up compaction and object store overhead.

## Goals

- Reduce block cache memory consumed by SST index and filter metadata.
- Load only the portion of the index (and optionally filter) relevant to a given key range, rather than the full structure.
- Maintain backward compatibility with SSTs written in the current flat-index format.

## Non-Goals

- Changing the data block format or block size defaults.
- Reducing write amplification or compaction I/O.
- Replacing the existing bloom filter implementation. Partitioned filters are a follow-on concern; this RFC may choose to address only the index.

## Design

<!-- A detailed description of the proposed change. Include diagrams, examples, schemas, and pseudo-code as appropriate. -->

### Overview

Today the SST layout is a two-level hierarchy:

```
SsTableInfo (footer)
  └── SsTableIndex (one flat block: all BlockMeta entries)
        └── data blocks
```

This RFC introduces a three-level hierarchy:

```
SsTableInfo (footer)
  └── SsTableIndex (top-level directory: one entry per partition)
        └── PartitionIndex (partition block: BlockMeta entries for a key range)
              └── data blocks
```

The top-level directory is small, with one entry per partition rather than one per data block, and is always loaded on first access. When a point-get or scan arrives, the reader binary searches the top-level directory to find the relevant partition, fetches only that partition block, then binary searches it to find the data block offset. Only the relevant partition block needs to be cached.

### Schema changes (`schemas/sst.fbs`)

A new `SsTableIndexV2` table is introduced for partitioned-index SSTs. The existing `SsTableIndex` table is left entirely unchanged. `SsTableInfo.index_offset` continues to point to the index structure; a new `index_type` field on `SsTableInfo` tells the reader which table to decode. Because FlatBuffers field additions are backward-compatible, old readers silently get the default value (`Flat`) and continue using the existing code path unchanged. The translation between the FlatBuffers representation and the internal Rust types is handled in `flatbuffer_types.rs`.

```fbs
enum IndexType: byte {
    Flat,       // default — existing SsTableIndex
    Partitioned // new SsTableIndexV2
}

// Existing table — unchanged.
table SsTableIndex {
    blocks: [BlockMeta];
}

// New table for partitioned-index SSTs. SsTableInfo.index_offset points here
// when SsTableInfo.index_type == Partitioned.
// Contains only the top-level directory; per-partition data is stored in separate
// PartitionIndex blocks, each covering a contiguous key range.
table SsTableIndexV2 {
    partitions: [PartitionMeta];
}

table PartitionMeta {
    // Byte offset of the PartitionIndex for this partition within the SST file.
    offset: ulong;

    // Length of the PartitionIndex in bytes.
    length: ulong;

    // First key covered by this partition (used for binary search).
    first_key: [ubyte] (required);
}

table PartitionIndex {
    blocks: [BlockMeta] (required);
}
```

`SsTableInfo` gains one new field:

```fbs
table SsTableInfo {
    // ... existing fields unchanged ...

    // Type of index stored at index_offset. Defaults to Flat for backward compatibility.
    index_type: IndexType;
}
```

### Format version

No new format version is introduced. The `index_type` field added to `SsTableInfo` is a backward-compatible FlatBuffers field addition — old readers get the default value (`Flat`) and use the existing code path unchanged. The read path branches on `info.index_type` to choose between the flat and partitioned index structures.

### Write path (`EncodedSsTableFooterBuilder::build`)

Instead of serializing all `block_meta` into a single flat `SsTableIndex`, the builder:

1. Groups `block_meta` entries into partitions of ~4KB of serialized index data each (analogous to RocksDB's default).
2. Serializes each chunk as a `PartitionIndex` and writes it to the file, recording its offset, length, and first key.
3. Serializes an `SsTableIndex` (with `index_type = Partitioned` in `SsTableInfo`) from the recorded partition metadata and writes it to the same location as the flat index.

### Read path (`SsTableFormat::read_index`)

On a read against an SST with partitioned index:

1. Check whether the top-level `SsTableIndexV2` is available (pinned in heap by default; in the block cache when `cache_index_in_block_cache = true`).
   - **Cold miss** (top-level directory not in heap or cache): fetch the top-level directory and all partition blocks in a single object store read. Object store latency is dominated by first-byte latency (~50–100 ms TTFB per request), so issuing one read for the full index incurs lower TTFB than fetching the directory first and then each needed partition separately, each of which would pay its own TTFB. The full index is written to the disk cache. Only the relevant partition block is decoded and promoted into the block cache (keyed by `(sst_id, partition_offset)`); the remaining partition blocks stay on disk and are promoted on demand. The top-level directory is pinned in heap (or inserted into the block cache if `cache_index_in_block_cache = true`).
   - **Warm hit** (top-level directory available): proceed to step 2.
2. Binary search `partitions` by key to find the relevant `PartitionMeta`.
3. Fetch and decode the relevant `PartitionIndex` from the block cache. On a partial miss (top-level directory was warm but this specific partition was evicted), fetch only that partition block from the disk cache. Each `PartitionIndex` has its own cache key `(sst_id, partition_offset)` so partitions can be evicted independently.
4. Binary search the `PartitionIndex.blocks` to find the data block offset, then proceed as today.

On a read against an SST with `index_type = Flat` in the `SsTableInfo` footer, the existing flat-index path is used unchanged.

### Metadata cache changes (`slatedb/src/db_cache/mod.rs`)

Two new variants are added to the internal `CachedItem` enum, SsTableIndexV2 for the cached top-level index directory and PartitionIndex for the cached partitions of the index:

```rust
enum CachedItem {
    Block(Arc<Block>),
    SsTableIndex(Arc<SsTableIndexOwned>),      // existing flat v1 index
    SsTableIndexV2(Arc<SsTableIndexV2Owned>),  // new top-level partition directory
    PartitionIndex(Arc<PartitionIndexOwned>),   // individual partition blocks
    BloomFilter(Arc<BloomFilter>),
    SstStats(Arc<SstStats>),
}
```

Both new types route to the **meta cache** in `SplitCache::insert`, consistent with the existing index and filter entries.

#### Cache key structure

`CachedKey` is `(scope_id, sst_id, block_id)` where `block_id` is a byte offset
within the SST file. Within a single SST file, partition index blocks and data blocks occupy
non-overlapping byte ranges, so their byte offsets are guaranteed to be distinct.
No `CachedKey` collision is possible, and no changes to `CachedKey` are required. The `CachedItem` variant already encodes the
type, providing an additional layer of differentiation.

#### Cache stats

`SsTableIndexV2` lookups reuse the existing `dbcache.index_hit` / `dbcache.index_miss`
counters, since it fills the same conceptual role as the flat index (the top-level
entry point for an SST's index structure).

`PartitionIndex` gets two new counters registered in `DbCacheStats`:

- `dbcache.partition_index_hit`
- `dbcache.partition_index_miss`

These are kept separate because partition block cache effectiveness is the primary
observable outcome of this RFC; lumping them into the existing index counters would
make it impossible to distinguish top-level directory hits from individual partition
block hits.


## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics: the index-loading layer (`read_index` in `tablestore.rs`/`format/sst.rs`) must be updated to load the top-level directory, binary-search it for the requested key range, and eagerly fetch only the covering partition blocks, stitching their `BlockMeta` entries into the flat structure the iterator already expects. `blocks_covering_view`, `spawn_fetches`, and `seek` in `sst_iter.rs` are unaffected.
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

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
- [x] Block cache: the primary motivation for this change; partitioned indexes reduce the amount of RAM consumed by index metadata since only the partition blocks covering the accessed key range need to be cached rather than the full index for every SST.
- [x] Object store cache: smaller, more targeted index fetches mean less data written to the local disk cache on a cold miss, reducing both disk usage and the time to warm the cache.
- [x] Indexing (bloom filters, metadata): the index structure changes from a single flat block to a two-level partitioned structure; bloom filter partitioning is adjacent and may be addressed as a follow-on.
- [x] SST format or block format: the on-disk SST layout must be updated to store partition index blocks and a top-level directory, requiring a versioned format change for backward compatibility.

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- **Read latency:** warm reads (index already cached) see no meaningful change. On a cold miss, all partition blocks are fetched in a single object store read, incurring the same TTFB latency as reading the flat index. These are cached at partition granularity and partitions are evicted as necessary.
- **Write latency:** negligible impact. The footer builder does slightly more work splitting block_meta into partitions, but this is CPU-bound and minor relative to the I/O cost of writing data blocks.
- **Space amplification:** slight increase due to storing the top-level directory alongside the partition blocks, but the overhead is small (one entry per partition, not per block).
- **Small SSTs:** for SSTs with few enough blocks to fit in a single partition, the top-level directory collapses to one entry and the read path is equivalent to today's flat index with negligible additional overhead. No special casing is needed.

### Observability

- **Configuration:** expose a `partition_index_block_size` setting (default ~4KB of index data per partition, analogous to RocksDB's default) to control partition granularity. Expose a `cache_index_in_block_cache` boolean (default `false`) to control whether the top-level directory competes for space in the block cache; by default it is pinned in heap memory so it is always available without a cache lookup.
- **Metrics:** add cache hit/miss counters scoped to partition index blocks, separate from data block and top-level directory hits, so the effectiveness of partition caching is visible.

### Compatibility

- **Existing SSTs:** fully backward compatible. The `index_type` field on `SsTableInfo` defaults to `Flat`, so existing SSTs are read with the existing code path unchanged.
- **New SSTs:** written with `index_type = Partitioned`. An older reader will decode `SsTableInfo` successfully (FlatBuffers ignores unknown fields) but will not know about `index_type` and will attempt to decode the index as `SsTableIndex`.
- **Rolling upgrades:** readers must be upgraded before writers start producing partitioned-index SSTs.

## Testing

<!-- Describe the testing plan for this change. -->

- Unit tests: inline `#[cfg(test)]` modules in `format/sst.rs` and `sst_builder.rs`. Verify that an SST written with a partitioned index can be read back correctly, that binary search on the top-level directory returns the right partition, and that V1/V2 SSTs are still decoded correctly via the flat index path.
- Integration tests: `slatedb/tests/db.rs`. Verify point-gets and range scans return correct results against SSTs with partitioned indexes, including across compaction boundaries.
- Fault-injection/chaos tests: None
- Deterministic simulation tests: None
- Formal methods verification: None
- Performance tests: benchmark point-get latency and block cache memory usage before and after on a large SST to confirm the expected improvements.

## Rollout

<!-- Describe the plan for rolling out this change to production. -->

- Milestones / phases:
- Feature flags / opt-in:
- Docs updates:

## Alternatives

List the serious alternatives and why they were rejected (including "status quo"). Include trade-offs and risks.

### Multi-level manifest with smaller SSTs (rejected)

Store the set of SSTs per sorted run in a separate file that the manifest refers to, making smaller SST sizes tenable. Rejected because it doesn't eliminate the core problem. A monolithic index is still loaded in full on every read regardless of SST size, just smaller. It also trades index overhead for worse problems: more files means higher compaction frequency, more object-store LIST operations, and significant manifest complexity for no benefit beyond what partitioned indexes already provide. 

### Status quo (rejected)

Keep the flat monolithic index. Rejected because the problem scales with SST size. As SSTs grow, so does the index, and every point-get pays the full metadata cost regardless of which key is being accessed. There is no tuning knob that fixes the O(SST size) vs O(1 block) asymmetry without changing the index structure. Reducing the max SST size would partially mitigate cache pressure but increases file count, compaction frequency, and object-store request overhead, trading one problem for several others.

## Open Questions

- ~~Should the top-level index directory be configurable to live in heap memory instead of the block cache?~~ Resolved: the top-level directory is pinned in heap by default. Because it is on the critical path for every read and is small (one entry per partition), heap-pinning eliminates a cache lookup on every access at negligible memory cost. A `cache_index_in_block_cache` option (default `false`) is exposed for operators who prefer to let it compete for block cache space instead.

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- [GitHub Issue #1068: Partitioned index/filter blocks](https://github.com/slatedb/slatedb/issues/1068)
- [RocksDB Wiki: Partitioned Index Filters](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)

## Updates

Log major changes to this RFC over time (optional).
