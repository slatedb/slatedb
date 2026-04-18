# Targeted Cache Warming and Best-Effort Block Cache Eviction

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [Public API](#public-api)
   - [Warm Targets](#warm-targets)
   - [Warm Execution](#warm-execution)
   - [Best-Effort Eviction](#best-effort-eviction)
   - [Interaction with Other Caches](#interaction-with-other-caches)
   - [Failure Model](#failure-model)
- [Impact Analysis](#impact-analysis)
   - [Core API & Query Semantics](#core-api--query-semantics)
   - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   - [Time, Retention, and Derived State](#time-retention-and-derived-state)
   - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   - [Compaction](#compaction)
   - [Storage Engine Internals](#storage-engine-internals)
   - [Ecosystem & Operations](#ecosystem--operations)
- [Operations](#operations)
   - [Performance & Cost](#performance--cost)
   - [Observability](#observability)
   - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Almog Gavra](https://github.com/agavra)

## Summary

This RFC adds low-level APIs for warming and evicting SlateDB block-cache
entries for selected SSTs.

Callers choose the physical SST IDs they want to touch. For warming, callers
also choose the cache content they want to populate: the SST index, the bloom
filter, specific data ranges, or some combination of the three. 

Some additional notes:
1. Warming uses the normal `TableStore` path, so it may populate the
   object-store cache as a side effect. 
1. Eviction is best effort. 
1. If SlateDB cannot recover enough metadata to enumerate an SST's cached
   blocks, the remaining entries stay in the cache until normal pressure
   removes them.

## Motivation

SlateDB depends on object storage for durability, so cache misses are expensive.
Two gaps show up today.

1. SlateDB has no direct way to warm selected block-cache content for known
   SSTs. Applications can fake it with reads, but that path decodes rows and
   reads more data than the cache needs.
2. When compaction removes an SST from the live set, its cached blocks, index,
   filter, and stats can sit in the block cache long after they stop helping
   queries.

## Goals

- Warm selected block-cache content for selected SSTs.
- Let callers warm only the parts of an SST they care about.
- Provide a best-effort `evict_ssts()` API for reclaiming dead block-cache
  entries.

## Non-Goals

- Automatic warming or eviction on manifest changes.
- Object-store cache eviction or invalidation.
- Perfect cleanup across garbage-collection races or restarts.
- New on-disk metadata or SST format changes.
- A public API for raw block reads or direct cache insertion.

## Design

### Public API

`Db` exposes two low-level methods: one for warming selected cache content, and
one for best-effort eviction.

```rust
/// Cache content that `warm_ssts()` should populate for each selected SST.
pub enum CacheTarget {
    /// Warm the SST bloom filter, if the SST has one.
    Bloom,
    /// Warm the SST index.
    Index,
    /// Warm the SST data blocks that overlap the supplied key range.
    ///
    /// This also warms the SST index, since block planning depends on it.
    ///
    /// Callers can select all data blocks in an SST by passing an unbounded
    /// range, for example `(..)`.
    Data(BytesRange),
}

/// Aggregate results returned by `warm_ssts()`.
pub struct WarmStats {
    pub warmed_ssts: usize,
    pub skipped_ssts: usize,
    pub error_ssts: usize,
    pub warmed_blocks: usize,
}

/// Aggregate results returned by `evict_ssts()`.
pub struct EvictStats {
    pub evicted_ssts: usize,
    pub error_ssts: usize,
    pub evicted_blocks: usize,
}

impl Db {
    /// Warms selected cache content for the currently reachable SSTs whose
    /// physical IDs appear in `sst_ids`.
    ///
    /// Returns an error if the block cache is not configured.
    pub async fn warm_ssts(
        &self,
        sst_ids: &[SsTableId],
        targets: &[CacheTarget],
    ) -> Result<WarmStats, crate::Error>;

    /// Best-effort eviction of block-cache entries for the given SST IDs.
    ///
    /// Returns an error if the block cache is not configured.
    pub async fn evict_ssts(
        &self,
        sst_ids: &[SsTableId],
    ) -> Result<EvictStats, crate::Error>;
}
```

This API makes three deliberate choices.

1. The public surface stays at the SST level. Callers pass physical SST IDs,
   not `SsTableView`s or lower-level cache keys.
2. `warm_ssts()` takes explicit targets. We stop overloading "empty range" to
   mean "warm metadata only."
3. The API does not prescribe policy. Callers decide when to warm or evict, and
   how they derive the SST set.

Both APIs return structured results instead of failing the whole request on the
first disappearing SST.

### Warm Targets

`CacheTarget` gives callers control over what `warm_ssts()` populates.

- `CacheTarget::Index` warms the SST index.
- `CacheTarget::Bloom` warms the bloom filter if the SST has one.
- `CacheTarget::Data(BytesRange)` warms the data blocks that overlap the
  supplied key range. It also warms the SST index, since data-block planning
  depends on that index.

`warm_ssts()` accepts multiple targets for the same call. A caller can warm
only metadata, data ranges plus their required indexes, or both.

For data warming, SlateDB uses the same range model it already uses elsewhere:
`BytesRange` describes key intervals, and overlapping target ranges are
coalesced before the read. If the caller provides redundant ranges, SlateDB
should not fetch the same block twice.

`BytesRange` can also express "all data" with an unbounded range, so
`CacheTarget::Data(..)` is enough for full-SST data warming. This RFC does not
need a separate `AllData` target.

`CacheTarget::Data(...)` implies `CacheTarget::Index` as part of the API
contract, because SlateDB cannot warm data blocks without using the index to
plan those reads. It does not imply `CacheTarget::Bloom`. If callers want the
bloom filter warmed too, they should ask for it explicitly.

An empty `targets` slice is a no-op.

### Warm Execution

`warm_ssts()` is request-scoped. This RFC does not add a background task or a
SlateDB-owned subscriber.

The call operates on the manifest that is current when the request runs. For
each requested SST, SlateDB:

1. Checks whether the physical SST is reachable from the current manifest.
2. Gathers the manifest entries that reference that SST. A physical SST can
   appear through more than one projected `SsTableView`, each with its own
   visible range.
3. Unions the visible ranges from those projections.
4. Applies the requested `CacheTarget`s.

For `CacheTarget::Index`, SlateDB reads the index through `TableStore`.

For `CacheTarget::Bloom`, SlateDB reads the bloom filter through `TableStore` if
the SST has one.

For each `CacheTarget::Data(range)`, SlateDB:

1. Intersects the requested key range with the visible ranges for that SST.
2. Skips the target if that intersection is empty.
3. Reads the SST index if it has not already done so for this SST.
4. Uses the index to map the overlapping key range to block intervals.
5. Coalesces overlapping intervals.
6. Reads those blocks through
   `TableStore::read_blocks_using_index(..., cache=true)`.

As a result, any successful data warm also leaves the SST index warm, even if
the caller did not list `CacheTarget::Index` separately.

`warm_ssts()` skips SST IDs that are not reachable from the current manifest. It
does not try to recreate a previous session's cache contents. If a block cache
implementation persists entries across restarts, callers decide whether
rewarming is worth the duplicate reads.

### Best-Effort Eviction

`evict_ssts()` removes SlateDB block-cache entries for the supplied physical SST
IDs.

The method targets all block-cache content associated with each SST:

- data blocks
- index
- bloom filter
- stats

This API does not take `CacheTarget`s. Eviction is the broad operation. 

The cache key includes the physical SST ID and an offset. To remove all data
blocks for one SST, SlateDB needs the SST index so it can enumerate block
offsets. The eviction path therefore reads the index with `cache=false`, then
removes the entries one by one from the block cache.

With current defaults, the cost is easy to estimate. A full-sized compacted SST
is capped at 256 MiB, and the default SST block size is 4 KiB. In the worst
case that is about `256 MiB / 4 KiB = 65,536` data-block cache keys, plus one
index entry, up to one bloom-filter entry, and one stats entry. So a full SST
is on the order of 65.5k `remove()` calls.

This is still best effort.

- If SlateDB can read the index, it removes the block-cache entries for that
  SST.
- If the index read fails because the SST is already gone, or because the read
  fails for some other reason, SlateDB records the failure for that SST and
  moves on.

`evict_ssts()` does not check whether an SST is still live in the current
manifest. The method operates on physical IDs. Callers own that policy.

This RFC does not add a cache-side "remove by SST prefix" API, a persistent
auxiliary index, or a startup sweep for persisted block caches.

### Interaction with Other Caches

Warming goes through `TableStore`, not a side channel. That means any configured
object-store cache can benefit from the underlying object reads. That is useful,
and it matches the real read path.

It is still a side effect, not a goal of this RFC.

- The API only exists when the SlateDB block cache exists.
- Eviction only targets SlateDB block-cache entries.
- Object-store cache behavior stays unchanged.

If a caller wants to combine object-store cache preload with block-cache
warming, it can do that in application code. This RFC does not try to unify the
two caches behind one policy surface.

### Failure Model

These APIs should not put database availability at risk.

- `warm_ssts()` and `evict_ssts()` return `Err` for request-level failures, such
  as database shutdown or invalid request setup.
- Per-SST races and read failures should be reported in the returned stats and
  logs, not by aborting the whole request.
- If an SST disappears while warming or eviction is in flight, SlateDB should
  record that outcome and continue with the remaining SSTs.

Warming is not atomic. If `warm_ssts()` fails partway through, some SSTs or
blocks may already be warm. Eviction is not atomic either. If `evict_ssts()`
fails on one SST, SlateDB may already have evicted entries for earlier SSTs.
That is acceptable. The contract is best effort plus reporting, not
transactional cache state.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

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
- [x] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

This feature adds extra reads only when callers invoke the new APIs.

- Latency (reads/writes/compactions): callers can pay the warm-up cost before
  shifting traffic, after startup, or after compaction events. Queries that hit
  the warmed targets should see less cache-miss latency.
- Throughput (reads/writes/compactions): warming and eviction consume read
  bandwidth and CPU when callers trigger them. They do not add continuous
  background work on their own.
- Object-store request (GET/LIST/PUT) and cost profile: warming adds targeted
  GETs for indexes, bloom filters, and selected block ranges. Best-effort
  eviction may add one index read per SST.
- Space, read, and write amplification: warming spends block-cache space on the
  targets the caller chose. Eviction reduces stale entries when SlateDB can
  enumerate them, but it does not guarantee immediate reclamation.

### Observability

This RFC should include metrics. The names below follow the recorder-based
metrics direction in RFC 0021.

Suggested `Info`-level metrics:

- `slatedb.cache_api.warm_sst_count`
  Labels: `{result=success|skipped|error}`
- `slatedb.cache_api.warm_block_count`
  Labels: `{result=success|miss}`
- `slatedb.cache_api.warm_duration_seconds`
- `slatedb.cache_api.warm_error_count`
  Labels: `{stage=index|bloom|blocks|request}`
- `slatedb.cache_api.warm_target_count`
  Labels: `{target=index|bloom|data}`
- `slatedb.cache_api.evict_sst_count`
  Labels: `{result=success|error}`
- `slatedb.cache_api.evict_block_count`
  Labels: `{result=success|miss}`
- `slatedb.cache_api.evict_duration_seconds`
- `slatedb.cache_api.evict_error_count`
  Labels: `{stage=index|cache_remove|request}`

Logs are still useful:

- Debug logs for warm and eviction planning
- Warn logs for per-SST failures

### Compatibility

- Existing data on object storage / on-disk formats: no format changes
- Existing public APIs (including bindings): adds new APIs, does not change
  existing read or write semantics
- Rolling upgrades / mixed-version behavior (if applicable): safe, because the
  behavior is local to a running process and does not change stored metadata

## Testing

- Unit tests should cover target parsing, range intersection, per-SST warm
  planning, and per-SST eviction enumeration.
- Integration tests should cover `warm_ssts()` with metadata-only targets,
  range-based data targets, mixed targets, and IDs that are no longer reachable
  from the manifest.
- Integration tests should cover `evict_ssts()` for live and dead SST IDs, and
  confirm that the method only touches SlateDB block-cache entries.
- Fault-injection tests should cover object-store read failures, GC races during
  eviction, and partial failures during a multi-SST request.
- Performance tests should measure warm-up cost and hot-range latency
  stability.

## Rollout

- Milestones / phases:
  - land `warm_ssts()` with `CacheTarget`
  - land best-effort `evict_ssts()`
  - document startup warming and application-owned manifest/subscription usage
- Feature flags / opt-in:
  - explicit API use only. There is no builder flag or background manager in
    this RFC
- Docs updates:
  - `Db` API docs
  - examples for metadata-only warming and range-based warming
  - operational guidance for startup warming and external subscription loops

## Alternatives

**Status quo**

Applications can only warm the cache through normal reads, and dead SST entries
stay in the block cache until normal pressure evicts them. That leaves extra
work on the read path and stale data in the cache.

**A SlateDB-owned `CacheManager`**

The earlier draft took this path. SlateDB would subscribe to manifest changes,
own a warm set, and run warming and eviction in the background.

That design would be easier to demo, but it hides policy inside the database.
The discussion on this RFC pushed back on that tradeoff. Reviewers wanted the
ability to experiment with different warming strategies, choose which parts of
an SST to warm, and wire the policy to their own control plane. A smaller API
is a better fit for that.

If we want a batteries-included helper later, we can add it on top of
`warm_ssts()` and `evict_ssts()`.

**Expose lower-level block read and cache insertion primitives**

This goes further in the other direction. Instead of `warm_ssts()`, SlateDB
would expose enough low-level API for applications to read specific block ranges
and insert cache entries directly.

That is more flexible, but it also leaks more internals:

- cache-key structure
- block-range planning from SST indexes
- cached-entry construction
- more of `TableStore` than we want to commit to publicly

`warm_ssts()` is the middle ground. It gives callers control over SST selection
and target selection without turning cache internals into public API.

**Automatic warming on open**

We could bake warming into `Db::open()` or builder setup. This RFC does not do
that. Some users will want to block on warm-up before serving reads. Others will
want to skip it, or make that decision with external state. The lower-level API
keeps that choice with the caller.

## Open Questions

None at the time of writing.

## References

- [Issue #1516: Cache Warming for block-level warming and eviction for the block cache](https://github.com/slatedb/slatedb/issues/1516)
- [Issue #1509: Evict block cache entries for SSTs removed from manifest](https://github.com/slatedb/slatedb/issues/1509)
- [Prototype commit `8191100`: cache manager prototype](https://github.com/slatedb/slatedb/commit/81911002e532a16e5d644029fddf5df41a95a32c)
- Discord discussion on April 10, 2026 about API shape, startup behavior, and
  cache scope

## Updates
