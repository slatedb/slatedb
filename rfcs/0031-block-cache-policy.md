# Block Cache Policy

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Public API](#public-api)
  - [Compaction Output Behavior](#compaction-output-behavior)
  - [Compaction Input Behavior](#compaction-input-behavior)
  - [Embedded Compactor](#embedded-compactor)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Accepted.

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

This RFC adds a `BlockCachePolicy` to `DbBuilder`. The policy controls:

- Which decoded SST components are requested for insertion into `DbCache` when
  a memtable flush or compaction produces an SST.
- Whether the embedded compactor probes existing decoded cache entries for L0
  or sorted-run inputs.

## Motivation

Several internal operations could benefit from configurable block-cache
behavior. Examples include:

- If L0 blocks are already cached, the embedded compactor can avoid rereading
  and decoding them.
- Some workloads may keep indexes and filters in memory to reduce object-store
  requests for point gets against newly compacted SSTs.
- Workloads using a hybrid block cache may cache compaction output on disk to
  avoid later object-store reads.

This policy lets users configure those behaviors explicitly.

## Goals

- Let users choose which SST components enter the block cache on flush and on
  compaction output.
- Let users choose whether compaction reads probe the block cache.

## Non-Goals

- Change foreground block cache behavior under the default policy. It stays per
  request in `ReadOptions::cache_blocks` and `ScanOptions::cache_blocks`.
- Unify policies across `CachedObjectStore` and `DbCache`.

## Design

### Public API

The policy is a concrete struct value. Components are selected with the
existing `CacheTarget` enum used by `DbCacheManagerOps` (RFC-0023).:

```rust
/// Block-cache policy for controlling block cache behavior during flush and
/// compaction.
#[derive(Clone, Debug)]
pub struct BlockCachePolicy {
    flush_targets: Vec<CacheTarget>,
    compaction_output_targets: Vec<CacheTarget>,
    l0_compaction_cache_probe: bool,
    sorted_run_compaction_cache_probe: bool,
}

impl BlockCachePolicy {
    pub fn with_flush_targets(
        self,
        targets: Vec<CacheTarget>,
    ) -> Self {}

    pub fn with_compaction_output_targets(
        self,
        targets: Vec<CacheTarget>,
    ) -> Self {}

    pub fn with_l0_compaction_cache_probe(self, enabled: bool) -> Self {}

    pub fn with_sorted_run_compaction_cache_probe(self, enabled: bool) -> Self {}
}

impl Default for BlockCachePolicy {
    fn default() -> Self {
        Self {
            flush_targets: vec![
                CacheTarget::data::<&[u8], _>(..),
                CacheTarget::Index,
                CacheTarget::Filters,
            ],
            compaction_output_targets: vec![
                CacheTarget::Index,
                CacheTarget::Filters,
            ],
            l0_compaction_cache_probe: false,
            sorted_run_compaction_cache_probe: false,
        }
    }
}
```


`DbBuilder` gains:

```rust
pub fn with_block_cache_policy(self, policy: BlockCachePolicy) -> Self;
```

### Compaction Output Behavior

- Compaction output data is inserted as it is produced by the streaming writer.
  When `CacheTarget::Data` carries a bounded key range, only the data blocks
  that overlap the range are inserted. Each block streamed to the writer
  will carry its first and last key, so the writer can decide overlap with the
  configured range per block without waiting for the SST index.
- Metadata components are inserted when they become available at writer close.
- If a compaction write fails after entries have been inserted, a best-effort
cleanup removes the inserted entries from the cache. Entries that survive the
cleanup remain until normal eviction or restart. This is safe because the
failed SST is not visible through the manifest.

### Compaction Input Behavior

- Compaction probes existing entries but does not insert misses because
  compaction inputs are short-lived and large scans could pollute the cache.
- The L0 and sorted-run settings independently control whether each input type
  probes the cache.

### Embedded Compactor

`DbBuilder` passes the same scoped `DbCacheWrapper` to the main and embedded-
compactor `TableStore`s so compaction can reuse entries inserted by main table
store and vice versa.

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

Compaction execution I/O is affected, but compaction selection, strategy, and
output semantics are unchanged.

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [x] Block cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- The default policy keeps current flush behavior. It also inserts the index
  and filters of compaction output SSTs, which reduces object-store requests
  for point gets against newly compacted SSTs at the cost of the cache space
  those entries occupy.
- A non-default policy impacts read performance and, when it caches SST
  components on write, write performance.

### Configuration

- New configuration: `BlockCachePolicy` on `DbBuilder`.

### Metrics

- Block-cache hit and miss metrics gain a `TableStoreKind` label to distinguish
  between main and compactor table-store reads.

### Compatibility

- The API is additive, so no compatibility impact.

## Testing

- Unit tests.
- Performance tests for different use cases.

## Alternatives

**Status quo.**
Rejected because of the use cases mentioned in Motivation.


**Trait-based policy (previous design).**
Exposed a user-implemented `BlockCachePolicy` trait along with read and write
source and action types. The types were:

```rust
/// The operation that produced an SST.
pub enum WriteSource {
    /// A memtable flush writing an L0 SST.
    Flush,
    /// Compaction writing an output SST.
    CompactionOutput,
}

/// The operation issuing a read.
pub enum ReadSource {
    /// A foreground get or scan, carrying the per-request cache_blocks
    /// option from ReadOptions or ScanOptions.
    Foreground { cache_blocks: bool },
    /// A compaction read of an L0 input SST.
    CompactionL0Input,
    /// A compaction read of a sorted run input SST.
    CompactionSortedRunInput,
    /// Writer startup replay.
    WalReplay,
    /// DbReader WAL replay, which re-reads the same WAL SSTs when a
    /// partially failed replay retries on the next poll.
    WalTail,
}

/// How a written component interacts with the block cache.
pub enum CacheWriteMode {
    /// Insert the component into the block cache.
    Cache,
    /// Do not insert the component.
    Skip,
}

/// How a read interacts with the block cache.
pub enum CacheReadMode {
    /// No lookup, no insert.
    Bypass,
    /// Serve a hit; on a miss, read from the object store without inserting.
    Probe,
    /// Serve a hit; on a miss, read from the object store and insert.
    ReadThrough,
}

pub trait BlockCachePolicy: Send + Sync + 'static {
    /// How `target` of an SST written by `source` interacts with the
    /// block cache.
    fn write_mode(
        &self,
        source: WriteSource,
        target: CacheTarget,
    ) -> CacheWriteMode;

    /// How a read of `target` issued by `source` interacts with the
    /// block cache.
    fn read_mode(&self, source: ReadSource, target: CacheTarget) -> CacheReadMode;
}
```

This design allows more dynamic control, but it exposes more types and gives
control to all possible uses of the block cache without clear use cases.

The proposed policy can grow with focused builder methods when concrete use
cases arise.

**Coarse knobs.**
Five booleans (`cache_blocks_on_flush`, `cache_metadata_on_flush`, and so on)
or a `CachedSections { None, MetadataOnly, All }` enum per write source.
Rejected because it introduces many knobs, and new scenarios would add more.

## Open Questions

None.

## References

- [Issue #1799: Use block cache for L0 compaction if compactor is running on writer](https://github.com/slatedb/slatedb/issues/1799)
- [RFC-0023: Cache Manager](./0023-cache-manager.md)
- [RFC-0027: Decoupled Pluggable Object Store Cache](./0027-decoupled-object-store-cache.md)
