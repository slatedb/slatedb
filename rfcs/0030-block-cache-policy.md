# Block Cache Policy

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Public API](#public-api)
  - [Wiring](#wiring)
  - [Write admission](#write-admission)
  - [Read resolution](#read-resolution)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Draft

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

Apart from `cache_blocks` in read APIs and the caching warming APIs in `CacheManager`,
SlateDB has a fixed block cache usage today for internal operations (memtable
flush or or SST compaction) where it inserts every block of a flushed SST into
the block cache, and compaction doesn't read or write from the cache. This RFC
adds a `BlockCachePolicy` trait, set on `DbBuilder`, that controls block cache
behavior in flush, compaction reads and writes. The default implementation
preserves current behavior; users implement the trait for different behavior.

## Motivation

There are multiple usecases that can benefit from controlling the block cache
behavior in internal operations, some examples are:
- If L0 blocks are in the block cache, the embedded compactor can be much more
effecient in L0 reads if it attempts reading from the block cache.
- Some usecases might need to keep the indices and filters always in memory to
avoid multiple read requests per point get to read the index and filter for the
new compacted SST.
- For usecases that use the hybrid block cache, they might wish to cache the
compaction output to disk to avoid reading from ObjectStore.

These different needs show that a flexible policy where user can clearily set
their block cache policy can help these scenarios.

## Goals

- Let users choose which SST components enter the block cache on flush and on
compaction output.
- Let users choose to make compaction reads attempt reading from the block cache.

## Non-Goals

- Change foreground block cache behavior under the default policy. It stays per
request in `ReadOptions::cache_blocks` and `ScanOptions::cache_blocks`.

## Design

### Public API

The policy is a trait. It is consulted per SST component, once for writes and
once for reads:

```rust
/// A component of an SST that can be inserted into the block cache.
pub enum CacheComponent {
    Data,
    Index,
    Filters,
    Stats,
}

/// The write operation that produced an SST.
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
    /// Whether `component` of an SST written by `source` is inserted into the
    /// block cache.
    fn write_mode(&self, source: WriteSource, component: CacheComponent) -> CacheWriteMode;

    /// How a read of `component` issued by `source` interacts with the
    /// block cache.
    fn read_mode(&self, source: ReadSource, component: CacheComponent) -> CacheReadMode;
}
```

The default policy, `DefaultBlockCachePolicy`, is an implementation of the
trait that preserves current behavior (its exact answers are the tables in the
next two sections). Users who want different behavior implement the trait. For
example, a policy that lets compaction serve L0 input reads from the cache:

```rust
struct CompactionL0ReadsPolicy;

impl BlockCachePolicy for CompactionL0ReadsPolicy {
    fn write_mode(&self, source: WriteSource, component: CacheComponent) -> CacheWriteMode {
        DefaultBlockCachePolicy.write_mode(source, component)
    }

    fn read_mode(&self, source: ReadSource, component: CacheComponent) -> CacheReadMode {
        match source {
            ReadSource::CompactionL0Input => CacheReadMode::Probe,
            _ => DefaultBlockCachePolicy.read_mode(source, component),
        }
    }
}
```

The policy is set with `DbBuilder::with_block_cache_policy(Arc<dyn BlockCachePolicy>)`.

### Wiring

The builder wraps the configured `DbCache` once and hands the same wrapped
handle to the main and compactor table stores, together with the policy.

### Write admission

For flush and compaction output writes, the table store asks the policy per
component: `write_mode(source, component)`.

The default policy answers:

| Write | Policy input | Default policy |
|---|---|---|
| Memtable flush (main store) | `WriteSource::Flush` | data, index, filters |
| Compaction output (compactor store) | `WriteSource::CompactionOutput` | none |
| WAL SST (any store) | not consulted | never inserted |

### Read resolution

Callers to the table store pass a `ReadSource` tag stating what kind of
operation is reading, and the table store calls `policy.read_mode(source, component)`.
The tag replaces both internal booleans threaded through the read path today
(`cache_blocks` and `cache_metadata` on `SstIteratorOptions` and the `TableStore`
read methods). A store with no block cache resolves everything to `Bypass`
without consulting the policy.

The default policy resolves (metadata means index, filters, and stats):

| ReadSource | Data | Metadata |
|---|---|---|
| `Foreground { cache_blocks: true }` | `ReadThrough` | `ReadThrough` |
| `Foreground { cache_blocks: false }` | `Probe` | `ReadThrough` |
| `CompactionL0Input` | `Bypass` | `Bypass` |
| `CompactionSortedRunInput` | `Bypass` | `Bypass` |
| `WalReplay` | `Bypass` | `Bypass` |
| `WalTail` | `ReadThrough` | `Probe` |


A custom policy receives every source, including `Foreground` with the
per-request option, and may resolve them differently from the table above. The
engine executes whatever mode the policy returns.

This design means the cache read action is resolved in one place at the read
and write locations instead of passing booleans around.

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
- [x] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- The default policy changes nothing on the read or write path.
- The custom policy set by the user will impact the performance of reads and
potentially writes if the cache the data in SST files.

### Configuration

- New configuration: `BlockCachePolicy` on `DbBuilder`.

### Metrics
- Hit and Miss metrics for the block cache gains a label that is `TableStoreKind`
to distinguish between main and compactor table store reads.

### Compatibility

- The API is additive, so no compatibility impact.

## Testing

- Unit tests.
- Performance tests for difference usecases.

## Alternatives

**Status quo.**
Rejected because of the usecases mentioned in Motivation

**A declarative policy struct.**
`BlockCachePolicy` can be a plain struct, with the resolution tables fixed in
the engine and consulting its fields.

```rust
/// A compaction input type.
pub enum CompactionInput {
    L0,
    SortedRuns,
}

pub struct BlockCachePolicy {
    /// Components cached when a memtable flush writes an L0 SST.
    /// Default: data, index, filters.
    pub flush: HashSet<CacheComponent>,

    /// Components cached when compaction writes an output SST.
    /// Default: empty.
    pub compaction_output: HashSet<CacheComponent>,

    /// Compaction inputs whose reads serve hits from the block cache,
    /// without inserting on miss. Default: empty.
    pub compaction_reads_from_cache: HashSet<CompactionInput>,
}
```

This keeps a smaller public surface (no trait, and `ReadSource`,
`WriteSource`, and `CacheReadMode` stay internal) and cannot express invalid
states, but every new caching behavior needs a new field. The proposed design
is more flexible and lets the user to handle all cases for reads and writes.

**Coarse knobs.**
Five booleans (`cache_blocks_on_flush`, `cache_metadata_on_flush`, and so on)
or a `CachedSections { None, MetadataOnly, All }` enum per write source.
Rejected because it's a lot of knobs and new scenarios will just add more knobs.

## Open Questions
- The `CacheComponent` in the design is different from `CacheTarget` in
`CacheManager` because `CacheManager` allows for loading a certain data range
in the cache. The current design doesn't support data range in the cache policy,
but it can be added later if needed, is there any issue here ?

## References

- [Issue #1799: Use block cache for L0 compaction if compactor is running on writer](https://github.com/slatedb/slatedb/issues/1799)
- [RFC-0027: Decoupled Pluggable Object Store Cache](./0027-decoupled-object-store-cache.md)
- [RFC-0023: Cache Manager](./0023-cache-manager.md)
