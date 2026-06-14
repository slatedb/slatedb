# Lazy Compaction

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Principle](#principle)
  - [Planning](#planning)
  - [Read path](#read-path)
  - [Garbage collection](#garbage-collection)
  - [TTL (RFC-0003)](#ttl-rfc-0003)
  - [Merge operators (RFC-0006)](#merge-operators-rfc-0006)
  - [Compaction filters (RFC-0017)](#compaction-filters-rfc-0017)
  - [Per-segment enablement (RFC-0024)](#per-segment-enablement-rfc-0024)
  - [Resumable compaction (RFC-0013)](#resumable-compaction-rfc-0013)
- [Impact Analysis](#impact-analysis)
  - [Core API & Query Semantics](#core-api-query-semantics)
  - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
  - [Time, Retention, and Derived State](#time-retention-and-derived-state)
  - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
  - [Compaction](#compaction)
  - [Storage Engine Internals](#storage-engine-internals)
  - [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
  - [Performance & Cost](#performance-cost)
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

- [Brice](https://github.com/unbrice)

## Summary

Lazy Compaction avoids rewriting untouched SST regions during compaction. For
non-overlapping inputs, we move the SST
([trivial moves](https://github.com/facebook/rocksdb/wiki/Compaction-Trivial-Move)).
In low-overlap situations, we trade reclaiming storage for avoiding work via a
configurable tradeoff.

It reuses existing `SsTableView` key-bounded projections, so the manifest format
is unchanged. The only new persisted state is the compaction plan in the
compactor's job state for resumability.

## Motivation

No deployment runs a full compaction to physically reclaim every deleted row the
instant it is deleted. Therefore every LSM deployment trades storage space
against write amplification. Today the lever is compaction frequency: compacting
more often reclaims space sooner but rewrites the same data more times;
compacting less often saves writes but lets dead bytes accumulate.

Frequency only moves a deployment *along* a fixed curve, and that curve has a
floor: each compaction rewrites whole SSTs, including regions that overlap
nothing, so even at the best frequency a deployment pays to rewrite untouched
data. Lazy compaction removes that floor: a compaction's cost scales with the
*changed* fraction of its inputs rather than their total size, pushing the whole
space/write-amplification frontier outward. For a given space target it reaches
lower write amplification, and for a given write budget it reclaims more space,
than frequency tuning alone can.

Even where compute and I/O are abundant, this avoids pointless work: today we
rewrite whole SSTs to save ~0 space. The canonical examples are archival and,
more generally, "low-overlap" workloads.

For edge or small deployments, where CPU and/or bandwidth are limited, the
impact is larger still.

## Goals

- **Avoid rewrites of low-overlap inputs:** Rewrite only overlapping blocks,
  keeping the remainder by reference, instead of rewriting whole SSTs to reclaim
  zero space.
- **Adjustable space vs. compute/IO tradeoff:** Let deployments configure how
  much storage to trade for lower compute and I/O.
- **No overhead when unused:** Databases without lazy compaction remain
  byte-identical.
- **Graceful degradation:** Fall back to ordinary rewrites under budget pressure
  to prevent unbounded fragmentation.

## Non-Goals

- **Value separation / blob files:** Separating large values into independent
  blob files (e.g., Titan or BlobDB).

## Design

### Principle

For each block in the compaction's input, we classify it as: fully live, fully
dead, or partially live. We then decide how to output it: either as a **view**
that points back into a **base** SST, or as a `rewrite` that is independent of
the input. Rewriting kept references later, to reclaim a mostly-dead base, is
called **rematerialization**.

### Example

To illustrate the principle, consider a DB whose keys are a single letter and
whose values are large enough that each row is its own block.

We are about to perform lazy compaction on this database to produce the `l0`
run. Initially, `l0` is a single SST, `sst0`, containing
`(A: 0..., B: 0..., C: 0..., D: 0..., E: 0...)`. Because in this example each
value is large enough that each row is its own block, the SST has 5 blocks (keys
`A` to `E`) having all the same value `0...`.

The other inputs are `sst1`: `(E: 1..., F: 1...)` and `sst2`:
`(F: 2..., G: 2..., H: 2..., I: 2..., J: 2...)`, meaning: an SST `1` which
rewrites the value of `E` from `0...` to `1...` and adds the key `F` with value
`1...`, and an SST `2` which rewrites `F` from `1...` to `2...` and adds 4 more
blocks.

With lazy compaction, the compaction would not rewrite every SST. For blocks
whose contents must change (`E`), it writes out independent SSTs called
"**overlays**". For untouched blocks, it updates `SortedRunV2` in the manifest
with "**views**", which are `SsTableView` instances pointing to the original
SSTs. After lazy compaction, `l0` would contain three views: `view0` pointing to
`sst0` with a range `[A, D]`, `view1` pointing to `sst1` with a range `[E, E]`,
`view2` pointing to `sst2` with a range `[F, J]`.

Thus "lazy compaction" is said to have **deferred** the rewriting of all three
inputs. For `sst2` the deferral is free: every block is live, so keeping it
whole is a pure move. For `sst0` and `sst1`, the tradeoff was that we did not
reclaim the space that `sst0[E]` and `sst1[F]` are using. Now, if the next
compaction additionally had as input `sst3`: `(E: 3...)`, we would drop `sst1`,
reclaim the space it was using, and lazy compaction would have saved real work.
This illustrates that the amortized cost can be lower with lazy compaction even
under a given space budget.

In real databases, rows rarely align with blocks, so the algorithm operates at
the block level and classifies each input block as **fully live** (no key in it
is superseded), **fully dead** (every key is superseded), or **partially live**.
Views keep fully live blocks; fully dead blocks are dropped by clamping the
view's range past them; partially live blocks are rewritten into overlays, which
emit only their surviving keys. View boundaries therefore **snap inward** to
block separators: a boundary block that is only partially live is excluded from
the view, and its live keys are absorbed into the adjacent overlay.

### Budgeting

Two budgets limit lazy compaction:

- **Space budget**: Unreferenced bytes from superseded versions and dropped
  records (e.g., bottom-level tombstones). Measured as a fraction of estimated
  live bytes (default: 10%, minimum 200 MiB).
- **Manifest bloat budget**: Extra manifest entries from lazy compaction.
  Measured as a fraction of referenced SSTs (default: 10%, minimum 1000).

Both quantities are measured database-wide and derived from the manifest. They
are a target, not a guarantee: concurrent compactions (RFC-0024 segments,
RFC-0025 distributed compaction) may transiently overshoot a budget they could
not see each other consuming. This is corrected the next time a compaction is
planned.

Except where rewrites are mandatory (e.g., stateful
[compaction filters](#compaction-filters-rfc-0017) or [TTL](#ttl-rfc-0003)),
inputs are kept by reference until limited by the budgets.

### Planning

Planning targets the SSTs and views of the runs being compacted and their base
SSTs. Bases referenced by views outside this compaction are ignored until their
referencing runs are compacted; the scheduler can prioritize runs deferring the
most dead bytes.

Planning reads manifest metadata and each input SST's index block (separators
and offsets) to derive exact block liveness by correlating the view's
`visible_range` with the base's block index. A base's dead bytes equal its fully
dead blocks plus blocks superseded by overlays. Only dropped records (e.g.,
bottom-level tombstones) are estimated from per-block stats. No data blocks are
read.

- Partially live blocks are merged into overlays; fully dead blocks are excluded
  by clamping views; contiguous fully live blocks are kept by reference (or
  moved if the entire input is live).
- To satisfy the space and manifest budgets, the planner rewrites the least
  valuable references (least bytes saved per budget unit consumed).

### Read path

The read path is unchanged, leveraging the existing `effective_range` clamp and
sorted-run routing. Point reads route to at most one view per run before bloom
consultation, avoiding lookups outside the view's range. Kept views use the
base's bloom filter (built over all keys, including dead ones). Since filters
are sized in bits per key, the false-positive rate is unchanged; the larger
filter block read cost is bounded by budget-enforced rewrites of mostly-dead
bases. Range scans may cross more, smaller views, increasing index block reads;
this fragmentation is bounded by the manifest budget.

### Garbage collection

GC is unchanged: it computes the live set by scanning SST IDs referenced by
views in manifests and checkpoints, deleting unreferenced ones.

### TTL (RFC-0003)

TTL reclamation relies on periodic compaction ([RFC-0003](#references)): the
scheduler rewrites SSTs when their `SsTableInfo#timestamp` exceeds
`periodic_compaction_interval`. Lazy compaction never defers periodic-triggered
compactions; these rewrite inputs in full. Without periodic compaction, TTL
enforcement remains best-effort.

### Merge operators (RFC-0006)

Keys with merge operands spanning multiple inputs make their containing blocks
partially live; all operands route to and merge in a single overlay. Keys in
kept views have no other operands to merge against in this compaction.
Cross-level merges are unaffected. Bottom-level operand finalization is deferred
for view keys and resolved during read-time merges; hot merge-heavy keys are
eventually rewritten.

### Compaction filters (RFC-0017)

A filter is **stateless** if it can run on any subset of inputs and produce
identical decisions (e.g., custom TTLs, `NoopFilter`). Filters declare
statelessness by overriding `CompactionFilter::is_stateless() -> bool` (default
`false`). If a filter is stateful, the planner falls back to a full rewrite.

Stateless filters run only on rewritten data; effects on kept regions are
deferred indefinitely. If a filter requires guaranteed execution (e.g.,
compliance purging), its supplier must return `false` for `is_stateless()` or
disable lazy compaction for those segments.

Suppliers can check `CompactionJobContext.lazy_compaction_enabled` to return a
`NoopFilter` for cold segments while using stateful filters on hot segments.

If a filter emits `Delete`, it is treated as a `Tombstone`.

### Resumable compaction (RFC-0013)

Lazy compaction segmentizes linear merges. Resume cursor tracking (RFC-0013)
cannot use the last output's `last_written_key_and_seq` because views lack
sequence numbers.

Replanning on resume cannot be guaranteed deterministic — planning reads
liveness, dead-byte figures, and budget usage from the manifest and input
indexes, which other jobs may have changed in the meantime. The job state
therefore persists the plan itself as `lazy_compaction_plan`: the ordered list
of output decisions (view ranges and overlay key ranges). On resume, the
compactor reloads the plan, correlates already-written overlays in
`output_ssts`, and resumes the active overlay's cursor. If
`lazy_compaction_plan` is missing, resume falls back to rewriting remaining
ranges.

For crash recovery, validation still works: overlays are tracked in
`output_ssts`, and views are recreated from the plan. A base SST is retained in
the manifest and storage (via GC) as long as any view references it.

### Per-segment enablement (RFC-0024)

When segments (RFC-0024) are configured, eligibility can be restricted per
segment to avoid lazy compaction on hot segments up front rather than waiting
for budget exhaustion.

## Impact Analysis

SlateDB features and components impacted by this RFC:

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

- [x] Time to live (TTL)
- [x] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence
- [x] Compaction filters
- [x] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- **Latency (reads/writes/compactions):** Point reads are unaffected (see
  [Read path](#read-path)). Writes and ingest are unchanged. Range scans can
  cross more, smaller views (bounded by the manifest budget). Compaction latency
  decreases for low-overlap workloads.
- **Throughput (reads/writes/compactions):** Compaction throughput improves in
  proportion to bytes not moved; read and write throughput are unchanged.
- **Object-store request (GET/LIST/PUT) and cost profile:** Compaction GET/PUT
  traffic scales with the overwritten fraction plus one planning-time index GET
  per input SST. Countervailing costs are more manifest entries (bounded by
  manifest budget) and more retained base objects increasing GC bucket LIST scan
  overhead (bounded by space budget).
- **Space, read, and write amplification:** Write amplification is reduced.
  Point-read amplification is unchanged. Scan amplification can rise slightly
  due to view fragmentation (bounded by manifest budget). Space amplification
  rises as superseded bytes remain resident until dereferenced or rematerialized
  (bounded by space and manifest budgets).

### Observability

- **Tunables and defaults:**
  - Enable flag (default: off): Enables lazy compaction.
  - Space budget (default `10%` of estimated live bytes, minimum `200 MiB`): Max
    resident dead bytes.
  - Manifest budget (default `10%` of referenced SSTs, minimum `1000`): Max
    extra manifest entries.
- **Metrics:** New metrics under the `slatedb.compactor.*` namespace:
  - `slatedb.compactor.resident_dead_bytes` (gauge): Estimated resident dead
    bytes.
  - `slatedb.compactor.lazy_manifest_entries` (gauge): Extra manifest entries.
  - `slatedb.compactor.rematerialization_count` (counter): Rematerialization
    events.
  - `slatedb.compactor.lazy_compaction_bytes_saved` (counter): Total bytes saved
    by lazily compacting blocks.
- **Logging:** None beyond existing compaction logging.

### Compatibility

- **Existing data on object storage / on-disk formats:** No manifest format
  change; views reuse existing `CompactedSsTableView` and `SortedRunV2`
  structures. The only new persisted state is `lazy_compaction_plan` (see
  [Resumable compaction](#resumable-compaction-rfc-0013)). Databases without
  lazy compaction are byte-identical. The read path
  [derives block ranges in memory](#read-path), matching `effective_range`
  behavior.
- **Existing public APIs (including bindings):** Read/write semantics are
  unchanged. Adds configuration: the enable flag, two budgets, per-segment
  opt-in, and the `is_stateless()` / `lazy_compaction_enabled` filter hooks.
- **Rolling upgrades / mixed-version behavior:** Interoperable. Older compactors
  perform ordinary rewrites. A newer compactor
  [resuming a job](#resumable-compaction-rfc-0013) with a missing or untrusted
  `lazy_compaction_plan` falls back to rewriting remaining ranges.

## Testing

- **No-overhead guarantee:** Manifests and SSTs are byte-identical with the
  enable flag off.
- **Run disjointness and snapping:** Randomized compactions assert view and
  overlay ranges never overlap, all live keys remain reachable, and no
  superseded version is exposed.
- **Resume determinism:** Kill/resume tests assert that the persisted
  `lazy_compaction_plan` is honored, `output_ssts` overlay correlation is
  stable, and missing plans fall back to full rewrites.
- **Budget convergence:** Concurrent compactions that overshoot a budget
  converge back under it, and small databases respect budget floors.
- **Filter semantics:** Stateless filters run only on rewritten data; stateful
  filters force full rewrites; `lazy_compaction_enabled` is surfaced to
  suppliers.

## Rollout

TODO

## Alternatives

- **Whole-SST trivial move only:** Moving inputs by reference only when wholly
  disjoint misses partial-overlap cases. This RFC generalizes trivial moves to
  block-aligned ranges.
- **Explicit reference counting:** Implementing reference counts over base SSTs
  would reclaim dead holes before the entire base is dereferenced, but is
  rejected to keep GC simple (relying on set-membership over object IDs).
- **Read-time overlap resolution:** Letting views of a run overlap and resolving
  precedence at read time would avoid overlays, but adds read-time merges and
  breaks run-disjointness.
- **Persisted block-range field:** Persisting block indexes or ranges in the
  manifest would avoid planning-time index GETs, but changes the manifest format
  for cheap-to-cache metadata.
- **Per-view bloom rebuild:** Rebuilding bloom filters over only a view's live
  keys would shrink the fetched filter block but requires reading the keys lazy
  compaction avoids, costing nearly as much as a rewrite.
- **Per-sorted-run fragmentation limit:** A separate cap on views per run is
  subsumed by the global manifest budget.
- **In-place hole punching / subsetting:** Releasing space by punching holes
  (e.g., using `fallocate(FALLOC_FL_PUNCH_HOLE)` or GCS's Rewrite API) is left
  for future expansion.

## Open Questions

- **Default budget values:** Default space and manifest budgets (10% with floors
  of 200 MiB and 1000 entries) require empirical tuning (see
  [Observability](#observability)).

## References

- [RFC 0001: Manifest Design](https://slatedb.io/rfcs/0001-manifest/)
- [RFC 0002: Compaction](https://slatedb.io/rfcs/0002-compaction/)
- [RFC 0003: Timestamps & Time-To-Live](https://slatedb.io/rfcs/0003-timestamps-and-ttl/)
- [RFC 0004: Checkpoints and Clones](https://slatedb.io/rfcs/0004-checkpoints/)
  — **primary**: the `SsTableView` projection this RFC builds on.
- [RFC 0005: Range Scans](https://slatedb.io/rfcs/0005-range-queries/)
- [RFC 0006: Merge Operator](https://slatedb.io/rfcs/0006-merge-operator/)
- [RFC 0013: Compaction State Persistence](https://slatedb.io/rfcs/0013-compaction-state-persistence/)
- [RFC 0017: Compaction Filters](https://slatedb.io/rfcs/0017-compaction-filters/)
- [RFC 0024: Segment-Oriented Compaction](https://slatedb.io/rfcs/0024-segment-oriented-compaction/)
- [RFC 0025: Distributed Compaction](https://slatedb.io/rfcs/0025-distributed-compaction/)
- [SST format](https://slatedb.io/docs/design/sorted-string-table/) — block
  index / separator structure.
- [RocksDB Compaction Trivial Move](https://github.com/facebook/rocksdb/wiki/Compaction-Trivial-Move)
- [Pebble virtual SSTables (#1683)](https://github.com/cockroachdb/pebble/issues/1683)
  — prior art for the broad technique; SlateDB's `SsTableView` is the analogous
  primitive.

## Updates

Log major changes to this RFC over time (optional).
