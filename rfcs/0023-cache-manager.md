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

Status: Accepted

Authors:

* [Almog Gavra](https://github.com/agavra)

## Summary

This RFC adds low-level APIs for warming and evicting SlateDB block-cache
entries for one SST at a time.

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
- Provide a best-effort `evict_cached_sst()` API for reclaiming dead block-cache
  entries.

## Non-Goals

- Automatic warming or eviction on manifest changes.
- Object-store cache eviction or invalidation.
- Perfect cleanup across garbage-collection races or restarts.
- New on-disk metadata or SST format changes.
- A public API for raw block reads or direct cache insertion.

## Design

### Public API

Warming and eviction are exposed through a `DbCacheManagerOps` trait, following
the pattern established by `DbMetadataOps` (PR #1559). Both `Db` and `DbReader`
implement the trait, so callers import it once and use the same methods against
either handle.

```rust
/// Cache content that `warm_sst()` should populate.
pub enum CacheTarget {
    /// Warm all filters on the SST, if any exist.
    Filters,
    /// Warm the SST index.
    Index,
    /// Warm the SST stats block, if one exists.
    Stats,
    /// Warm the SST data blocks that overlap the supplied key range.
    ///
    /// This also warms the SST index, since block planning depends on it.
    /// The payload is a standard `(Bound, Bound)` pair that implements
    /// `RangeBounds<Bytes>`.
    Data((Bound<Bytes>, Bound<Bytes>)),
}

impl CacheTarget {
    /// Construct a [`CacheTarget::Data`] from any `RangeBounds<impl AsRef<[u8]>>`,
    /// mirroring the [`Db::scan`] signature. Pass `..` to warm all data blocks.
    pub fn data<K, T>(range: T) -> Self
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>;
}

/// Trait for block-cache warming and eviction operations.
#[async_trait::async_trait]
pub trait DbCacheManagerOps {
    /// Warms selected cache content for one SST. Short-circuits on the first
    /// failing target.
    async fn warm_sst(
        &self,
        sst_id: SsTableId,
        targets: &[CacheTarget],
    ) -> Result<(), crate::Error>;

    /// Best-effort eviction of block-cache entries for one SST.
    async fn evict_cached_sst(&self, sst_id: SsTableId) -> Result<(), crate::Error>;
}
```

Both `Db` and `DbReader` also re-export thin inherent wrappers that delegate to
the trait (e.g. `<Self as DbCacheManagerOps>::warm_sst(self, ...)`), so callers
that already have a concrete handle do not need the trait import to discover the
methods in documentation.

This API makes three deliberate choices.

1. The public surface stays at the SST level. Callers pass a physical SST ID,
   not an `SsTableView` or a lower-level cache key.
2. `warm_sst()` takes explicit targets.
3. Each method operates on a single SST. Callers fan out over SST IDs with
   their own concurrency primitive. This keeps the API small and pushes
   policy (parallelism, rate-limiting, ordering) to the caller.

Both methods return `Result<(), crate::Error>`. Per-target visibility (what was
warmed, what was skipped, how many blocks moved through the cache) belongs in
cache-manager metrics, not the return value. A richer return shape can be added
later in a backwards-compatible way (see Alternatives).

### Warm Targets

`CacheTarget` gives callers control over what `warm_sst()` populates.

- `CacheTarget::Index` warms the SST index.
- `CacheTarget::Filters` warms all filters on the SST, if any exist.
- `CacheTarget::Stats` warms the SST stats block, if one exists.
- `CacheTarget::Data((Bound<Bytes>, Bound<Bytes>))` warms the data blocks that
  overlap the supplied key range. It also warms the SST index, since data-block
  planning depends on that index.

`warm_sst()` accepts multiple targets for the same call. A caller can warm
only metadata, data ranges plus their required indexes, or both. Batching
targets into one call lets SlateDB share the SST index read across
`CacheTarget::Index` and `CacheTarget::Data(_)`.

For data warming, callers pass any standard `RangeBounds` through
`CacheTarget::data(range)`, mirroring the `Db::scan<K, T>(range: T)` signature.
Overlapping target ranges in a single call are not coalesced upfront; the
block cache's per-block lookup in `read_blocks_using_index` prevents duplicate
object-store fetches.

`CacheTarget::data(..)` expresses "all data" with an unbounded range, so
full-SST data warming does not need a separate `AllData` target.

`CacheTarget::Data(...)` implies `CacheTarget::Index` as part of the API
contract, because SlateDB cannot warm data blocks without using the index to
plan those reads. It does not imply `CacheTarget::Filters`. If callers want the
filters warmed too, they should ask for it explicitly.

An empty `targets` slice is a no-op.

### Warm Execution

`warm_sst()` is request-scoped. This RFC does not add a background task or a
SlateDB-owned subscriber. Cross-SST consistency (for example, "all calls see
the same manifest snapshot") is not guaranteed; each call observes the
manifest that is current when it runs.

For one call, SlateDB:

1. Checks whether the physical SST is reachable from the current manifest.
2. Gathers the manifest entries that reference that SST. A physical SST can
   appear through more than one projected `SsTableView`, each with its own
   visible range.
3. Unions the visible ranges from those projections.
4. Applies each requested `CacheTarget`.

For each `CacheTarget::Data(range)`, SlateDB:

1. Intersects the requested key range with the visible ranges for the SST.
2. Skips the target if that intersection is empty.
3. Reads the SST index if it has not already done so for this call.
4. Uses the index to map the overlapping key range to block intervals.

For each `CacheTarget::Data(_)`, SlateDB warms the covered block ranges
through `TableStore::read_blocks_using_index(..., cache=true)`. That reader
consults the block cache per block and fetches only uncached ones, so
overlapping targets in the same call do not double-fetch.

If the SST is not reachable from the current manifest, `warm_sst()` returns
`Ok(())` without doing any work. It does not try to recreate a previous
session's cache contents. If a block cache implementation persists entries
across restarts, callers decide whether rewarming is worth the cost of
redundant cache lookups, which is non-trivial for large ranges even when every
block hits.

### Best-Effort Eviction

`evict_cached_sst()` removes SlateDB block-cache entries for one physical SST.

The method targets all block-cache content associated with the SST:

- data blocks
- index
- filters
- stats

This API does not take `CacheTarget`s because eviction is the broad operation
(if an SST is being evicted, it is likely that it is unreachable).

The cache key includes the physical SST ID and an offset. To remove all data
blocks, SlateDB needs the SST index so it can enumerate block offsets. The
eviction path therefore reads the index with `cache=false`, then removes the
entries one by one from the block cache. The `cache=false` read avoids
repopulating the index entry for an SST that is being evicted.

With current defaults, the cost is easy to estimate. A full-sized compacted SST
is capped at 256 MiB, and the default SST block size is 4 KiB. In the worst
case that is about `256 MiB / 4 KiB = 65,536` data-block cache keys, plus one
index entry, a small number of filter entries, and one stats entry. So a full
SST is on the order of 65.5k `remove()` calls.

This is still best effort. If the index read fails because the SST is already
gone, or because the read fails for some other reason, `evict_cached_sst()`
returns `Err`. Any entries it did not remove stay in the cache until normal
pressure evicts them.

`evict_cached_sst()` does not check whether the SST is still live in the
current manifest. The method operates on the physical ID. Callers own that
policy.

This RFC does not add a cache-side "remove by SST prefix" API, a persistent
auxiliary index, or a startup sweep for persisted block caches.

### Interaction with Other Caches

Warming goes through `TableStore`, not a side channel. That means any configured
object-store cache can benefit from the underlying object reads. That is useful,
and it matches the real read path.

It is still a side effect, not a goal of this RFC.

- If no block cache is configured, both methods log a warning and return
  `Ok(())` without doing any work.
- Eviction only targets SlateDB block-cache entries.
- Object-store cache behavior stays unchanged.

If a caller wants to combine object-store cache preload with block-cache
warming, it can do that in application code. This RFC does not try to unify the
two caches behind one policy surface.

### Failure Model

These APIs should not put database availability at risk.

- `warm_sst()` short-circuits on the first failing target and returns `Err`.
  Targets processed before the failure may already have inserted blocks into
  the cache; that partial state is left alone.
- `evict_cached_sst()` returns `Err` if the index read or any cache removal
  fails. Partial eviction is acceptable.
- Cross-SST orchestration is the caller's responsibility. A caller fanning out
  over many SSTs decides whether one failing `warm_sst()` or
  `evict_cached_sst()` should short-circuit the rest.

Neither method is atomic. The contract is best effort, not transactional
cache state.

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
  GETs for indexes, filters, and selected block ranges. Best-effort eviction
  may add one index read per SST.
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
  Labels: `{stage=index|filters|stats|blocks|request}`
- `slatedb.cache_api.warm_target_count`
  Labels: `{target=index|filters|stats|data}`
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
  existing read or write semantics. The trait methods are also re-exported as
  inherent methods on `Db` and `DbReader`, so callers with a concrete handle do
  not need to import `DbCacheManagerOps`.
- Rolling upgrades / mixed-version behavior (if applicable): safe, because the
  behavior is local to a running process and does not change stored metadata

## Testing

- Unit tests should cover target parsing, range intersection, per-SST warm
  planning, and per-SST eviction enumeration.
- Integration tests should cover `warm_sst()` with metadata-only targets,
  range-based data targets, mixed targets, and IDs that are no longer reachable
  from the manifest.
- Integration tests should cover `evict_cached_sst()` for live and dead SST IDs, and
  confirm that the method only touches SlateDB block-cache entries.
- Fault-injection tests should cover object-store read failures, GC races
  during eviction, and partial per-target failures within a single
  `warm_sst()` call.
- Performance tests should measure warm-up cost and hot-range latency
  stability.

## Rollout

- Milestones / phases:
  - land `warm_sst()` with `CacheTarget`
  - land best-effort `evict_cached_sst()`
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
`warm_sst()` and `evict_cached_sst()`.

**Expose lower-level block read and cache insertion primitives**

This goes further in the other direction. Instead of `warm_sst()`, SlateDB
would expose enough low-level API for applications to read specific block ranges
and insert cache entries directly.

That is more flexible, but it also leaks more internals:

- cache-key structure
- block-range planning from SST indexes
- cached-entry construction
- more of `TableStore` than we want to commit to publicly

`warm_sst()` is the middle ground. It gives callers control over SST selection
and target selection without turning cache internals into public API.

**Automatic warming on open**

We could bake warming into `Db::open()` or builder setup. This RFC does not do
that. Some users will want to block on warm-up before serving reads. Others will
want to skip it, or make that decision with external state. The lower-level API
keeps that choice with the caller.

**Rich return types: `Vec<WarmResult>` with per-target `WarmBlocks` / `EvictBlocks`**

An earlier draft had `warm_sst()` return a `Vec<WarmResult>` (one entry per
target, each with its own `Result<WarmBlocks, Error>` payload listing block
offsets), and `evict_cached_sst()` return an `EvictBlocks` listing every
offset the call attempted to remove. It also meant per-target failures could
be reported without aborting the whole call.

We rejected it because metrics cover the same ground more cheaply: warm/evict
counts, durations, per-target breakdowns, and error counts are all recordable
without shaping a struct into the public API. A `Result<(), Error>` is easier
to commit to for a v1 and can be widened backwards-compatibly later (for
example via a `warm_sst_detailed()` variant, or by returning an opaque handle)
if a real caller shows up needing that detail.

**Plural `warm_ssts(&[SsTableId], &[CacheTarget])`**

An earlier draft took a list of SST IDs in a single call. That version could
share one manifest snapshot across all SSTs and do internal parallelism in one
place. We rejected it because the SST-level fan-out is trivial to write at the
call site, and every caller ends up wanting different concurrency, rate
limiting, and error policies. Keeping the method per-SST pushes that policy
out of SlateDB, matches the "building blocks" philosophy called out above, and
keeps the return type small. Cross-SST manifest consistency is not a
requirement for any warming workload we have today.

## Open Questions

None at the time of writing.

## References

- [Issue #1516: Cache Warming for block-level warming and eviction for the block cache](https://github.com/slatedb/slatedb/issues/1516)
- [Issue #1509: Evict block cache entries for SSTs removed from manifest](https://github.com/slatedb/slatedb/issues/1509)
- [Prototype commit `8191100`: cache manager prototype](https://github.com/slatedb/slatedb/commit/81911002e532a16e5d644029fddf5df41a95a32c)
- Discord discussion on April 10, 2026 about API shape, startup behavior, and
  cache scope

## Updates
