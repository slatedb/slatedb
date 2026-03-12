# Partitioned indexes for SlateDB

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Ryan Dielhenn](https://github.com/ryandielhenn)

## Summary

<!-- Briefly explain what this RFC changes and why. Prefer 3–6 sentences. -->

Currently, each SST in SlateDB has a single monolithic index block and bloom filter that must be loaded in full before any data block can be read. For a 256MB SST with 4KB blocks this means loading several MB of metadata to locate a single 4KB value, an O(SST size) cost for O(1) data access.

This RFC proposes replacing the flat index with a two-level partitioned index: a small top-level directory that points to a set of partition index blocks, each covering a contiguous range of data blocks. On a point-get or short scan, only the relevant partition block needs to be fetched and cached rather than the entire index. This reduces cold read latency by replacing large, all-or-nothing metadata fetches with small targeted object store reads. It also reduces the amount of block cache consumed by index metadata, since only the partition blocks that are actually accessed need to be cached rather than the full index for every SST.


## Motivation

<!-- What problem are we solving? What user or system pain exists today? Include concrete examples and why "do nothing" is insufficient. -->

SlateDB's current max SST size is 256MB. With a default 4KB block size, a full-sized SST contains ~64K blocks. The index stores a separator key per block, so for a modest average key size of 32 bytes the index alone is ~2.5MB. The bloom filter adds further overhead on top of that.

This creates two concrete problems:

1. **Cold read latency:** On a cache miss, a single point-get must first fetch the full index and filter from the object store before it can identify which 4KB data block to read. This means paying for several MB of object store I/O to access a single 4KB block of data, an O(SST size) metadata cost for O(1) data access.

2. **Cache inefficiency:** The index and filter for a single SST consume several MB of block cache. For workloads that touch many SSTs concurrently, this metadata overhead crowds out data blocks that are far more likely to be reused.

Both problems scale with SST size and key length. For workloads with larger-than-average keys the index grows proportionally, making the asymmetry worse. Reducing the max SST size is not a practical mitigation, as it would increase the number of files and drive up compaction and object store overhead.

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
  └── PartitionedSsTableIndex (top-level directory: one entry per partition)
        └── SsTableIndex (partition block: BlockMeta entries for a key range)
              └── data blocks
```

The top-level directory is small, with one entry per partition rather than one per data block, and is always loaded on first access. When a point-get or scan arrives, the reader binary searches the top-level directory to find the relevant partition, fetches only that partition block, then binary searches it to find the data block offset. Only the relevant partition block needs to be cached.

### Schema changes (`schemas/sst.fbs`)

A new `IndexType` enum is added to `SsTableInfo` so that the reader can distinguish old flat-index SSTs from new partitioned-index SSTs, preserving backward compatibility.

A new `PartitionedSsTableIndex` table replaces the flat `SsTableIndex` as the top-level index structure. Each entry in its `partitions` array describes one partition block: the byte range where it lives in the file and the first key it covers (used for binary search).

The existing `SsTableIndex` (flat array of `BlockMeta`) is reused unchanged as the format for each individual partition block.

```fbs
enum IndexType: byte {
    Flat,         // existing format: single SsTableIndex covering all blocks
    Partitioned,  // new format: PartitionedSsTableIndex top-level directory
}

table PartitionMeta {
    // Byte offset of this partition block within the SST file.
    offset: ulong;

    // Length of this partition block in bytes.
    length: ulong;

    // First key covered by this partition block (used for binary search).
    first_key: [ubyte] (required);
}

table PartitionedSsTableIndex {
    partitions: [PartitionMeta] (required);
}
```

`SsTableInfo` gains one new field:

```fbs
table SsTableInfo {
    // ... existing fields unchanged ...

    // Indicates whether index_offset/index_len points to a flat SsTableIndex
    // or a PartitionedSsTableIndex. Defaults to Flat for backward compatibility
    // with SSTs written before this change.
    index_type: IndexType;
}
```

### Format version

A new `SST_FORMAT_VERSION_V3 = 3` is introduced in `format/sst.rs`. SSTs written with a partitioned index use V3. The existing V1 and V2 readers are unaffected. `is_supported_version` is updated to also accept V3, and the read path branches on `SsTableInfo.index_type` to handle both formats.

### Write path (`EncodedSsTableFooterBuilder::build`)

Instead of serializing all `block_meta` into a single `SsTableIndex`, the builder:

1. Splits `block_meta` into chunks of N blocks each (partition granularity, e.g. 128KB of index data per partition, analogous to RocksDB's default).
2. Serializes each chunk as a `SsTableIndex` and writes it to the file as a separate partition block, recording its offset, length, and first key.
3. Serializes a `PartitionedSsTableIndex` top-level directory from the recorded partition metadata and writes it where the flat index used to go.
4. Sets `index_type = Partitioned` in `SsTableInfo`.

### Read path (`SsTableFormat::read_index`)

On a read against a V3 SST:

1. Load and decode the `PartitionedSsTableIndex` top-level directory from `index_offset..index_offset+index_len`. This is small (one entry per partition) and is cached as a single cache entry.
2. Binary search `partitions` by key to find the relevant `PartitionMeta`.
3. Fetch and decode only that partition block. Each partition block gets its own cache key `(sst_id, partition_offset)` so individual partitions can be evicted independently.
4. Binary search the partition's `SsTableIndex` to find the data block offset, then proceed as today.

On a read against a V1 or V2 SST the existing flat-index path is used unchanged.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
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

- **Existing SSTs:** fully backward compatible. The `index_type` field in `SsTableInfo` defaults to `Flat`, so V1/V2 SSTs are read with the existing code path unchanged.
- **New SSTs:** written as V3. An older version of SlateDB will reject a V3 SST with an `InvalidVersion` error rather than silently misreading it.
- **Rolling upgrades:** readers must be upgraded before writers start producing V3 SSTs. The partition index can be gated behind a configuration flag during rollout to allow a mixed-version window.

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

Store the set of SSTs per sorted run in a separate file that the manifest refers to, making smaller SST sizes tenable. Rejected because it doesn't eliminate the core problem. A monolithic index is still loaded in full on every read regardless of SST size, just smaller. It also trades index overhead for worse problems: more files means higher compaction frequency, more object-store LIST operations, and significant manifest complexity for no benefit beyond what partitioned indexes already provide. Existing large SSTs would still need efficient index reads during any migration window.

### Status quo (rejected)

Keep the flat monolithic index. Rejected because the problem scales with SST size. As SSTs grow, so does the index, and every point-get pays the full metadata cost regardless of which key is being accessed. There is no tuning knob that fixes the O(SST size) vs O(1 block) asymmetry without changing the index structure. Reducing the max SST size would partially mitigate cache pressure but increases file count, compaction frequency, and object-store request overhead, trading one problem for several others.

## Open Questions

- Should partitioned bloom filters be included in this RFC or deferred to a follow-on? Partitioned filters would extend the same two-level structure to the bloom filter, with a top-level filter directory pointing to per-partition filter blocks, reducing filter cache pressure in the same way as partitioned indexes. The tradeoff is added scope and complexity now vs. a second format version bump later.
- Should the top-level index directory be configurable to live in the block cache or always be pinned in heap memory? RocksDB exposes this via `cache_index_and_filter_blocks`. Pinning in heap guarantees the top-level directory is always available without a cache lookup, at the cost of heap memory per open SST. SlateDB has no equivalent setting today. Given that the top-level directory is small, pinning in heap may be the simpler default, but a configuration option would give operators control over the tradeoff.

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- [GitHub Issue #1068: Partitioned index/filter blocks](https://github.com/slatedb/slatedb/issues/1068)
- [RocksDB Wiki: Partitioned Index Filters](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)

## Updates

Log major changes to this RFC over time (optional).
