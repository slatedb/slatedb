# Partitioned Indexes for SlateDB

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

Currently, each SST in SlateDB has a single monolithic index block and bloom filter that must be loaded in full before any data block can be read. For a 256MB SST with 4KB blocks this means loading several MB of metadata to locate a single 4KB value.

This RFC proposes replacing the flat index with a two-level partitioned index: a small top-level directory that points to a set of partition index blocks, each covering a contiguous range of data blocks. On a point-get or short scan, only the relevant partition block needs to be fetched and cached rather than the entire index. This reduces cold read latency by replacing large, all-or-nothing metadata fetches with small targeted object store reads. It also reduces the amount of block cache consumed by index metadata, since only the partition blocks that are actually accessed need to be cached rather than the full index for every SST.


## Motivation

<!-- What problem are we solving? What user or system pain exists today? Include concrete examples and why "do nothing" is insufficient. -->

SlateDB's current max SST size is 256MB. With a default 4KB block size, a full-sized SST contains ~64K blocks. The index stores a separator key and an 8-byte block offset per entry, so for average key sizes of 8–32 bytes the index alone ranges from ~1MB to ~2.5MB. The bloom filter adds further overhead on top of that.

This creates two concrete problems:

1. **Cold read latency:** On a cache miss, a single point-get must first fetch the full index and filter from the object store before it can identify which 4KB data block to read. This means paying O(SST metadata size) in object store I/O to locate a single 4KB data block.

2. **Cache inefficiency:** The index and filter for a single SST consume several MB of block cache. For workloads that touch many SSTs concurrently, this metadata overhead crowds out data blocks that are far more likely to be reused.

Both problems scale with SST size. Reducing the max SST size is not a practical mitigation, as it would increase the number of files and drive up compaction and object store overhead.

## Goals

- Reduce block cache memory consumed by SST index and filter metadata.
- Reduce object store read amplification for point gets and short range scans on large SSTs.
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

A new `IndexType` enum is added to `SsTableIndex` so that the reader can distinguish old flat-index SSTs from new partitioned-index SSTs without needing to inspect `SsTableInfo`. Because FlatBuffers enum fields default to the zero value, `index_type` defaults to `Flat` for V1/V2 SSTs that predate this field, preserving backward compatibility.

`SsTableInfo.index_offset` continues to point to `SsTableIndex` in all format versions. In the flat case `SsTableIndex.blocks` holds all `BlockMeta` entries directly. In the partitioned case `SsTableIndex.partitions` holds the top-level directory and the per-partition data is stored in separate `PartitionIndex` blocks, each covering a contiguous key range.

```fbs
enum IndexType: byte {
    Flat,         // existing format: SsTableIndex.blocks contains all BlockMeta entries
    Partitioned,  // new format: SsTableIndex.partitions is the top-level directory
}

// Top-level index structure. SsTableInfo.index_offset always points here.
table SsTableIndex {
    // Distinguishes between flat and partitioned layouts. Defaults to Flat
    // so that V1/V2 SSTs (which lack this field) are read correctly.
    index_type: IndexType;

    // Flat format: all block metadata in one place (index_type == Flat).
    blocks: [BlockMeta];

    // Partitioned format: one entry per partition (index_type == Partitioned).
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

// Per-partition index fetched lazily. Only used when index_type == Partitioned.
table PartitionIndex {
    blocks: [BlockMeta] (required);
}
```

### Format version

A new `SST_FORMAT_VERSION_V3 = 3` is introduced in `format/sst.rs`. SSTs written with a partitioned index use V3. The existing V1 and V2 readers are unaffected. `is_supported_version` is updated to also accept V3, and the read path branches on `SsTableInfo.index_type` to handle both formats.

### Write path (`EncodedSsTableFooterBuilder::build`)

Instead of serializing all `block_meta` into a single flat `SsTableIndex`, the builder:

1. Groups `block_meta` entries into partitions of ~128KB of serialized index data each (analogous to RocksDB's default).
2. Serializes each chunk as a `PartitionIndex` and writes it to the file, recording its offset, length, and first key.
3. Serializes a `SsTableIndex` (with `index_type = Partitioned`) from the recorded partition metadata and writes it to the same location as the flat index.

### Read path (`SsTableFormat::read_index`)

On a read against a V3 SST:

1. Load and decode the `SsTableIndex` from `index_offset..index_offset+index_len`. This is small (one entry per partition) and is cached as a single cache entry.
2. Binary search `partitions` by key to find the relevant `PartitionMeta`.
3. Fetch and decode only that `PartitionIndex`. Each `PartitionIndex` gets its own cache key `(sst_id, partition_offset)` so individual partitions can be evicted independently.
4. Binary search the `PartitionIndex.blocks` to find the data block offset, then proceed as today.

On a read against a V1 or V2 SST the existing flat-index path is used unchanged.

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

- **Read latency:** point gets and short scans improve on cold reads. Instead of fetching the full index, the reader fetches the small top-level directory then one partition block. Warm reads (index already cached) see no meaningful change.
- **Write latency:** negligible impact. The footer builder does slightly more work splitting block_meta into partitions, but this is CPU-bound and minor relative to the I/O cost of writing data blocks.
- **Object-store requests:** cold reads issue one additional GET (top-level directory + partition block vs. single flat index), but each GET is much smaller. Net object-store cost is lower for workloads that access a small key range within a large SST.
- **Space amplification:** slight increase due to storing the top-level directory alongside the partition blocks, but the overhead is small (one entry per partition, not per block).

### Observability

- **Configuration:** expose a `partition_index_block_size` setting (default ~128KB of index data per partition) to control partition granularity.
- **Metrics:** add cache hit/miss counters scoped to partition index blocks, separate from data block and top-level directory hits, so the effectiveness of partition caching is visible.

### Compatibility

- **Existing SSTs:** fully backward compatible. The `index_type` field in `SsTableIndex` defaults to `Flat`, so V1/V2 SSTs are read with the existing code path unchanged.
- **New SSTs:** written as V3. An older version of SlateDB will reject a V3 SST with an `InvalidVersion` error rather than silently misreading it.
- **Rolling upgrades:** readers must be upgraded before writers start producing V3 SSTs.

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

- Should partitioned bloom filters be included in this RFC or deferred to a follow-on? Partitioned filters would extend the same two-level structure to the bloom filter, with a top-level filter directory pointing to per-partition filter blocks, reducing filter cache pressure in the same way as partitioned indexes. The tradeoff is added scope and complexity now vs. a second format version bump later.
- Should the top-level index directory be configurable to live in head memory instead of the block cache? RocksDB exposes this via `cache_index_and_filter_blocks`. Pinning in heap guarantees the top-level directory is always available without a cache lookup, at the cost of heap memory per open SST. SlateDB has no equivalent setting today. Given that the top-level directory is small, pinning in heap may be the simpler default, but a configuration option would give operators control over the tradeoff.

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- [GitHub Issue #1068: Partitioned index/filter blocks](https://github.com/slatedb/slatedb/issues/1068)
- [RocksDB Wiki: Partitioned Index Filters](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)

## Updates

Log major changes to this RFC over time (optional).
