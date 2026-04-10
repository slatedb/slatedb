# Cache Manager for Warming and Best-Effort Block Cache Eviction

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [Public API](#public-api)
   - [Warm Range Model](#warm-range-model)
   - [Lifecycle and Execution Model](#lifecycle-and-execution-model)
   - [Manifest-Driven Warming](#manifest-driven-warming)
   - [Best-Effort Eviction](#best-effort-eviction)
   - [Manual Warming](#manual-warming)
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

This RFC adds a SlateDB-owned `CacheManager` that warms selected key ranges in
the SlateDB block cache and best-effort evicts block-cache entries for SSTs
that leave the manifest. SlateDB owns the manifest subscription and exposes a
runtime handle from `Db` for configuring warm ranges, warming the current
manifest, or warming a selected set of live SSTs. Warming is opt-in. Eviction
is always on when a block cache is configured, but it is opportunistic: if
SlateDB cannot recover enough metadata to enumerate an SST's cached blocks,
stale entries remain until the cache evicts them on its own. The builder can
disable the cache manager entirely.

## Motivation

Since SlateDB depends on object storage for durability, the cost of a cache miss
can cause a query's latency to balloon. This RFC proposes a mechanism to avoid
cache misses for frequently accessed data when compaction causes cached blocks
to be invalidated as well as a mechanism to warm the cache on cold starts.

Today there are two gaps.

1. SlateDB has no direct way to warm selected ranges in the block cache.
   Applications can fake warming by issuing reads, but that path decodes rows
   and reads more data than the cache needs.
2. When compaction removes an SST from the manifest, its cached blocks, index,
   filter, and stats entries can remain in the block cache long after the SST
   stops being reachable.

This RFC keeps the scope narrow. Eviction is best effort and Warming may
populate the object-store cache as a side effect because it uses the normal
read path, but this RFC does not add object-store cache invalidation.

## Goals

- Warm selected key ranges at the block level.
- Update affected caches whenever a manifest change happens
- Expose one runtime API from `Db` for configuring warm ranges and triggering
  whole-manifest or targeted manual warming.
- Make eviction of dead block-cache entries automatic when a block cache is
  configured.
- Allow the cache manager to be disabled via `Settings` for rollout or
  debugging.

## Non-Goals

- Object-store cache eviction or invalidation.
- Perfect cleanup across garbage-collection races or restarts.
- New cache priority tiers, admission rules, or QoS classes.
- New on-disk metadata or SST format changes.
- Exposing SST internals as the primary public API for warming.

## Design

### Public API

`Db` exposes a cheap, cloneable handle when a block cache is configured and the
cache manager is enabled:

```rust
pub struct Settings {
    // ...

    /// Disables the cache manager even if a block cache is configured.
    /// Defaults to `true`.
    pub cache_manager_enabled: bool,

    /// Disables best-effort eviction of block-cache entries for SSTs
    /// removed from the manifest. Warming still works when this is
    /// `false`. Defaults to `true`.
    pub cache_eviction_enabled: bool,
}

impl Db {
    /// Returns `None` when the SlateDB block cache is disabled or the cache
    /// manager is disabled.
    pub fn cache_manager(&self) -> Option<CacheManager>;
}

#[derive(Clone)]
pub struct CacheManager {
    /* internal handle */
}

impl CacheManager {
    /// Replaces the subscribed warm set used for future manifest changes.
    /// Passing an empty vec clears the warm set. This call does not warm the
    /// current manifest.
    pub async fn set_warm_ranges<K, T>(&self, ranges: Vec<T>) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send;

    /// Convenience helper for prefix-based warm sets.
    /// Passing an empty vec clears the warm set.
    pub async fn set_warm_prefixes<P>(&self, prefixes: Vec<P>) -> Result<(), crate::Error>
    where
        P: AsRef<[u8]> + Send;

    /// Warms the current manifest using the warm set active when this request
    /// is processed.
    pub async fn warm_current(&self) -> Result<(), crate::Error>;

    /// Warms the subset of currently reachable SSTs whose physical IDs match
    /// `sst_ids`, using the warm set active when this request is processed.
    pub async fn warm_ssts(&self, sst_ids: &[SsTableId]) -> Result<(), crate::Error>;
}
```

This API makes three choices on purpose.

1. `CacheManager` is a first-class runtime handle. Users do not
   configure warming in the builder and then switch to a different
   object later.
2. The range setter mirrors SlateDB's scan-style range model. Empty input 
   clears the warm set.
3. Manual warming operates at the manifest level. Callers can warm the whole
   manifest or a selected set of physical SST IDs, but they do not pass
   internal SST view objects through the public API.

### Warm Range Model

The public API accepts the same style of key interval that `scan()` accepts.
`set_warm_ranges()` takes a list of key ranges, and `set_warm_prefixes()` covers
the common namespace case such as `b"idx/"`.

Internally the manager normalizes both forms into `Vec<BytesRange>`. Before it
warms an SST, it intersects that normalized warm set with the ranges that are
visible through the current manifest. If multiple warm ranges or manifest
projections overlap the same SST, the manager coalesces the resulting block
intervals before reading. The cache should not fetch the same block twice
because a caller supplied redundant ranges.

The cache key space is still physical SST IDs and offsets. A manifest update
that only changes the projection of a live SST does not trigger eviction for the
blocks that remain reachable through that physical SST.

### Lifecycle and Execution Model

When `Db` opens with a block cache and the builder did not disable the cache
manager, SlateDB creates a cache manager. Its default state is:

- subscribed warm set: empty
- manifest-driven warming: disabled until the warm set is non-empty
- best-effort eviction: enabled (unless `cache_eviction_enabled` is
  `false`)

If `cache_manager_enabled` is `false` in `Settings`, SlateDB does not
start the background task and `db.cache_manager()` returns `None`.

The manager runs as a background task owned by `Db`. It consumes two kinds of
events:

- manifest change notifications from SlateDB's internal status machinery
- API requests such as `set_warm_ranges()`, `set_warm_prefixes()`,
  `warm_current()`, and `warm_ssts()`

The task processes those events in order. That gives the public API a simple
rule: if `set_warm_ranges(...).await` or `set_warm_prefixes(...).await` returns
`Ok(())`, a later `warm_current().await` or `warm_ssts(...).await` will use the
new warm set. The setter methods do not warm anything on their own.

Manifest notifications use the latest visible manifest state when the task
handles them. If several manifest updates arrive back to back, the manager can
collapse them into one diff against the newest state it has not processed yet.

### Manifest-Driven Warming

On each manifest change, the manager diffs the old and new sets of physical
SSTs.

For each newly reachable SST:

1. Gather the manifest entries that reference that SST in the new manifest. A
   physical SST can appear through one or more projected `SsTableView`s, each
   with its own visible range.
2. Intersect the configured warm set with the union of those visible ranges.
3. If the result is empty, skip the SST.
4. Read the SST index through `TableStore`.
5. Read the SST filter through `TableStore` if the SST has one (just to cache
   the filter as well, can be done async).
6. Use the index to map each overlapping key range to block intervals.
7. Coalesce overlapping block intervals.
8. Read those blocks through `TableStore::read_blocks_using_index(..., cache=true)`.

Warming happens at the block layer. The manager does not issue synthetic
`get()` or `scan()` calls, and it does not deserialize rows just to populate the
cache.

The manager warms data blocks, the SST index, and the SST filter. It does not
try to warm every metadata structure that might exist in the file. The target is
the read path that matters for hot-key and hot-prefix workloads.

### Best-Effort Eviction

For each SST removed from the manifest, the manager tries to evict the
corresponding SlateDB block-cache entries:

- data blocks
- index
- filter
- stats

The cache key includes the physical SST ID and an offset. To remove all entries
for one SST, the manager needs the SST index so it can enumerate block offsets
(the current cache options don't have a builtin index that allows us to evict
key prefixes). The eviction path therefore reads the index with `cache=false`,
then deletes the entries one by one from the block cache.

With current defaults, the cost is easy to estimate. A full-sized compacted SST
is capped at 256 MiB, and the default SST block size is 4 KiB. In the worst
case that is about `256 MiB / 4 KiB = 65,536` data-block cache keys, plus one
index entry and up to one filter entry and one stats entry. So a full SST is on
the order of 65.5k `remove()` calls (which is relatively light compared to the
cost of warming new SSTs).

This is best effort.

- If the index read succeeds, SlateDB removes the block-cache entries for that
  SST.
- If the index read fails because the SST is already gone, or because the read
  fails for another reason, SlateDB logs the failure and stops. The remaining
  entries stay in the cache until that cache evicts them.

This RFC does not add a cache-side "remove by SST prefix" API, a persistent
auxiliary index, or a startup sweep for persisted block caches.

### Manual Warming

Manifest-driven warming covers steady-state compaction churn.  `warm_current()`
and `warm_ssts()` exist for the cases where the application or operator wants
to pay the warm cost on demand:

- after process startup
- after cache loss / corruption
- before shifting traffic to a new node

`warm_current()` warms every currently reachable SST that overlaps the warm set.
`warm_ssts()` warms only the currently reachable SSTs whose physical IDs appear
in the request.

Typical startup flow:

```rust
if let Some(cache) = db.cache_manager() {
    cache
        .set_warm_prefixes(vec![Bytes::from_static(b"idx/")])
        .await?;
    cache.warm_current().await?;
}
```

A more selective startup flow can diff a saved manifest snapshot against the
current manifest, then warm only the newly introduced SSTs:

```rust
if let Some(cache) = db.cache_manager() {
    cache
        .set_warm_prefixes(vec![Bytes::from_static(b"idx/")])
        .await?;
    cache.warm_ssts(&new_sst_ids).await?;
}
```

Both manual methods use the manifest that is current when the request is
handled. `warm_ssts()` skips IDs that are not reachable from that manifest. It
does not try to recreate a previous session's cache contents. If a block cache
implementation persists entries across restarts, the caller decides whether
rewarming is worth the duplicate reads.

### Interaction with Other Caches

Warming uses `TableStore`, not a side channel. That means any configured
object-store cache can benefit from the underlying object reads. This is useful,
and it matches the real read path.

It is still a side effect, not a goal of this RFC.

- `CacheManager` exists only when the SlateDB block cache exists.
- Eviction only targets SlateDB block-cache entries.
- The object-store cache keeps its current behavior, including any stale-entry
  handling it already has or does not have.

### Failure Model

The manager should not put database availability at risk.

- Background manifest-driven warming logs failures and continues.
- Background eviction logs failures and continues.
- `set_*` methods return an error if the manager has shut down or the database
  is closing.
- `warm_current()` and `warm_ssts()` return an error if the warm operation
  fails.

Manual warming is not atomic. If `warm_current()` or `warm_ssts()` fails in the
middle, some SSTs or blocks may already be warm. That is acceptable. The API
contract is about best effort to fill the cache, not transactional cache state.

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

This feature adds background reads when the manifest changes and when callers
trigger manual warming.

- Latency (reads/writes/compactions): workloads that repeatedly hit the warm set
  should see less latency variance across compactions. Writes and compactions do
  not block on warming, but they can compete for I/O.
- Throughput (reads/writes/compactions): hot-range read throughput should
  improve when the warm set matches real traffic. Background warming and
  eviction consume read bandwidth and CPU.
- Object-store request (GET/LIST/PUT) and cost profile: warming adds targeted
  GETs for indexes, filters, and selected block ranges. Best-effort eviction may
  add one index read per removed SST.
- Space, read, and write amplification: the system spends block-cache space on
  the warm set by design. Best-effort eviction reduces stale entries when it can
  enumerate them, but it does not guarantee immediate reclamation.

### Observability

This RFC should include metrics. The names below follow the recorder-based
metrics direction in RFC 0021.

Suggested `Info`-level metrics:

- `slatedb.cache_manager.warm_sst_count`
  Labels: `{trigger=manifest|manual, result=success|skipped|error}`
- `slatedb.cache_manager.warm_block_count`
  Labels: `{trigger=manifest|manual}`
- `slatedb.cache_manager.evict_sst_count`
  Labels: `{result=success|skipped|error}`
- `slatedb.cache_manager.evict_block_count`
  Labels: `{result=hit|miss}`
- `slatedb.cache_manager.operation_duration_seconds`
  Labels: `{op=manifest_warm|manual_warm_current|manual_warm_ssts|manifest_evict}`
- `slatedb.cache_manager.error_count`
  Labels: `{op=warm|evict, stage=index|filter|blocks|cache_remove|request}`
- `slatedb.cache_manager.warm_set_size`
  Gauge tracking the number of configured warm ranges

Logs are still useful:

- Debug logs for manifest diffs and warm planning
- Warn logs for warm failures
- Warn logs for eviction failures

### Compatibility

- Existing data on object storage / on-disk formats: no format changes
- Existing public APIs (including bindings): adds a new API surface, does not
  change existing read or write semantics
- Rolling upgrades / mixed-version behavior (if applicable): safe, because the
  behavior is local to a running process and does not change stored metadata

## Testing

- Unit tests should cover range normalization, warm planning, per-SST eviction
  enumeration, and ordering between warm-set updates and manual warm requests.
- Integration tests should cover manifest-driven warming and eviction, targeted
  manual warming via `warm_ssts()`, behavior when the manager is disabled, and
  the fact that warming uses the normal `TableStore` path.
- Fault-injection tests should cover object-store read failures, GC races during
  eviction, and manager shutdown while requests are in flight.
- Performance tests should measure warm-up cost and hot-range latency stability
  across compactions.

## Rollout

- Milestones / phases:
  - land the internal manager with manifest-driven warming and best-effort
    eviction
  - expose the runtime `CacheManager` API from `Db`
  - document startup warming and operator-triggered warming
- Feature flags / opt-in:
  - the entire cache manager can be disabled via
    `Settings::cache_manager_enabled`
  - warming is opt-in because the warm set is empty by default
  - best-effort eviction can be disabled independently via
    `Settings::cache_eviction_enabled`
- Docs updates:
  - `Db` and builder API docs
  - examples for prefix-based warming
  - operational guidance for startup warming and persisted block caches

## Alternatives

**Status quo**

Applications can only warm the cache through normal reads, and dead SST entries
stay in the block cache until normal pressure evicts them. That leaves too much
work on the read path and too much stale data in the cache.

**Builder-time warm configuration plus a separate runtime handle**

The prototype split the feature between builder-time configuration and a narrow
manual-warm handle. It works, but it forces users to learn two surfaces for one
feature. A single runtime `CacheManager` is easier to explain and easier to grow
later.

**No warming API. Expose low-level SST and cache primitives instead**

This is the most serious alternative to a `CacheManager`.

The idea is to avoid a first-class warming API entirely. SlateDB would expose
enough low-level SST read and cache primitives for applications to warm the
cache through their own side channel. A caller would subscribe to manifest
changes, choose SSTs and key ranges, inspect SST index metadata, compute the
matching block offsets, then issue reads or cache inserts on its own.

Why it is attractive:

- it gives advanced applications full control over warm-up policy
- it keeps warming orchestration outside the core database
- it fits systems that already have an external control plane, sidecar, or
  startup coordinator that wants to own cache preparation

Why it is harder in practice:

- the current public surface is not enough. SlateDB exposes `SstReader` for
  metadata and index inspection, and it exposes the `DbCache` trait, but it
  does not expose a public `TableStore`-style API for "read these exact blocks
  and populate the cache". The public cache API also hides cache-key fields and
  cached-entry constructors, so direct cache insertion is not a stable public
  contract today.
- making this alternative work would require more API than this RFC. We would
  likely need to expose block-range planning from SST indexes, low-level block
  reads that go through the normal cache path, and possibly stable cache-key or
  cache-entry construction. That is a larger and more coupled public surface
  than a small `CacheManager`.
- it pushes manifest semantics onto every caller. Applications would need to
  handle physical SSTs versus projected manifest entries, visible ranges, and
  overlap rules. That logic already exists in SlateDB's read path, and warming
  needs to follow the same rules to stay correct.
- it encourages duplicated planning code outside SlateDB. The "range -> index
  lookup -> block interval -> coalesced reads" pipeline would end up
  reimplemented in applications and bindings instead of living next to the
  storage code that already understands SST layout.
- it is a worse fit for language bindings. A compact `CacheManager` API is easy
  to expose over FFI. Low-level SST planning and cache-manipulation primitives
  are harder to expose cleanly and harder to support long term.

The strongest argument for this alternative is policy freedom. If we wanted
SlateDB to expose only storage primitives and let every application build its
own warming service, this would be the consistent design.

I still think the manager is the better default for this RFC. The warming logic
depends on SlateDB-owned concepts that already exist in the read path: manifest
projections, SST index interpretation, and cache-coherent block reads through
`TableStore`. Once we accept that SlateDB already owns those pieces, a small
`CacheManager` is a cheaper and more stable public contract than promoting a
wider set of low-level internals to first-class API.

**Warm the current manifest automatically on open**

That sounds convenient, but it bakes a policy choice into `Db::open()`. Some
applications want startup warming. Others would rather avoid duplicate reads,
especially when the block cache or lower cache layers already have useful data.
The explicit `warm_current()` call keeps that choice with the caller.

## Open Questions

None at the time of writing.

## References

- [Issue #1516: Cache Warming for block-level warming and eviction for the block cache](https://github.com/slatedb/slatedb/issues/1516)
- [Issue #1509: Evict block cache entries for SSTs removed from manifest](https://github.com/slatedb/slatedb/issues/1509)
- [Prototype commit `8191100`: cache manager prototype](https://github.com/slatedb/slatedb/commit/81911002e532a16e5d644029fddf5df41a95a32c)
- Discord discussion on April 10, 2026 about API shape, startup behavior, and
  the intended scope of eviction

## Updates

- 2026-04-10: Rewrote the RFC from first principles. Narrowed scope to warming
  plus best-effort SlateDB block-cache eviction. Removed object-store eviction
  claims and kept targeted manual warming at `SsTableId` rather than
  `SsTableView`.
