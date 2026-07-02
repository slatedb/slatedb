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

- [unBrice](https://github.com/unbrice)

## Summary

Lazy Compaction avoids rewriting untouched SST regions during compaction. For
non-overlapping inputs, we move the SST
([trivial moves](https://github.com/facebook/rocksdb/wiki/Compaction-Trivial-Move)).
In low-overlap situations, we trade reclaiming storage for avoiding work via a
configurable tradeoff.

It reuses existing `SsTableView`, so the manifest format is unchanged. No new
state is persisted; resumability relies on re-deriving the plan from the frozen
`CompactionSpec`.

## Motivation

No deployment runs a full compaction to physically reclaim every deleted row the
instant it is deleted. Therefore every LSM deployment trades storage space
against write amplification. Today the lever is compaction frequency: compacting
more often reclaims space sooner but rewrites the same data more times;
compacting less often saves writes but lets dead bytes accumulate.

Frequency only moves a deployment _along_ a fixed curve, and that curve has a
floor: each compaction rewrites whole SSTs, including regions that overlap
nothing, so even at the best frequency a deployment pays to rewrite untouched
data. Lazy compaction removes that floor: a compaction's cost scales with the
_changed_ fraction of its inputs rather than their total size, pushing the whole
space/write-amplification frontier outward. For a given space target it reaches
lower write amplification, and for a given write budget it reclaims more space,
than frequency tuning alone can.

Even where compute and I/O are abundant, this avoids pointless work where today
we rewrite whole SSTs to save ~0 space. For edge or small deployments, where CPU
and/or bandwidth are limited, the impact is larger still.

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
  blob files.
- **Store specific optimizations:** For some backends (e.g., Linux filesystems,
  GCS), we could later introduce some tricks to reclaim space by punching holes
  or rewriting objects in a way that is cheaper than reading+writing. For now,
  we leave this as a future optimization.

## Design

### Principle

For each block in the compaction's input, we classify it as: fully live, fully
dead, or partially live.

- Partially live blocks are merged into "overlay" SSTs.
- Fully dead blocks are excluded by clamping views.
- Contiguous fully live blocks are kept by reference (or moved if the entire
  input is live, see
  [trivial moves](https://github.com/facebook/rocksdb/wiki/Compaction-Trivial-Move)).

Except where rewrites are mandatory (e.g., stateful
[compaction filters](#compaction-filters-rfc-0017) or with
[TTL](#ttl-rfc-0003)), we keep fully live blocks by reference in the output.

### Example

Consider a DB whose keys are a single letter and whose values are large enough
that each row is its own block. We are about to perform lazy compaction on this
database to produce the `l0` run.

Input to lazy compaction (from oldest to newest):

- `sst0`, with `(A: 0..., B: 0..., C: 0..., D: 0..., E: 0...)`
- `sst1`: `(E: 1..., F: 1...)`
- `sst2`: `(F: 2..., G: 2..., H: 2..., I: 2..., J: 2...)`

In this example we have enough budget such that we don't need to rewrite any
SST. Output is:

- `view0` pointing to `sst0` with a range `[A, D]`
- `view1` pointing to `sst1` with a range `[E, E]`
- `view2` pointing to `sst2` with a range `[F, J]`

Thus "lazy compaction" is said to have **deferred** the rewriting of all three
inputs. For `sst2` the deferral is free: every block is live, so keeping it
whole is a pure move. For `sst0` and `sst1`, the tradeoff was that we did not
reclaim the space that `sst0[E]` and `sst1[F]` are using.

Now if the next compaction additionally has input:

- `sst3`: `(E: 3...)`

We can output:

- `view0` pointing to `sst0` with a range `[A, D]`
- `view1` pointing to `sst3` with a range `[E, E]`
- `view2` pointing to `sst2` with a range `[F, J]`

And drop `sst1`, which is now unreferenced. This illustrates that the amortized
cost can be lower with lazy compaction, when some rows are more stable than
others.

### Budgeting

Two budgets limit lazy compaction:

- **Space budget**: Unreferenced bytes that we could have reclaimed but didn't.
  Measured as a fraction of estimated live bytes (default: 10% with a minimum
  200 MiB).
- **Manifest bloat budget**: Extra manifest entries from lazy compaction.
  Measured as a fraction of referenced SSTs (default: 10% with a minimum of
  1000) (see [Read path](#read-path)).

Both quantities are measured database-wide and derived from the manifest. They
are a target, not a guarantee: concurrent compactions in particular may
transiently reclaim less space than their budget allows. This is corrected the
next time a compaction is planned.

The scheduler reads DB-wide budget headroom once at submit and freezes a per-job
allocation (dead-bytes and extra-views caps) into the immutable
`CompactionSpec`.

### Planning

Planning targets the SSTs + views of the runs being compacted + their base SSTs.

Planning reads manifest metadata and each input SST's index block (separators
and offsets) to derive exact block liveness by correlating the view's
`visible_range` with the base's block index. A base's dead bytes equal its fully
dead blocks + blocks superseded by overlays. Dropped records (e.g., bottom-level
tombstones) are estimated from per-block stats.

Liveness is judged against `retention_min_seq`, not key overlap alone: an older
block overlapped by a newer SST is dead only if the superseding versions fall
below `retention_min_seq` (otherwise an active snapshot may still need it).

The planner reads its budget from the `CompactionSpec` (not from live manifest
state) and rewrites the least valuable references (least bytes saved per budget
unit consumed). Planning is then a pure function of these immutable inputs.

### Read path

The read path is unchanged, leveraging the existing `effective_range` clamp and
sorted-run routing. Point reads cost `O(ln(views))` (binary-search routing to
one view per run) before bloom consultation, avoiding lookups outside the view's
range. Kept views use the base's bloom filter (built over all keys, including
dead ones). Since filters are sized in bits per key, the false-positive rate is
unchanged; the larger filter block read cost is bounded by budget-enforced
rewrites of mostly-dead bases. Range scans cost `O(requests)` independently of
the number of views; this fragmentation is bounded by the manifest budget.

### Garbage collection

GC is unchanged: it computes the live set by scanning SST IDs referenced by
views in manifests and checkpoints, deleting unreferenced ones.

### TTL (RFC-0003)

TTL reclamation relies on periodic/age-triggered compaction
([RFC-0003](#references)), which would guarantee TTL reclaim but does not exist
at HEAD (independent of this RFC). Today, reclaim is opportunistic via
size-tiered compaction, and cold data is best-effort.

To let operators force TTL reclaim, a manual `CompactionRequest` can carry a
knob to force a non-lazy rewrite. When periodic compaction is implemented, lazy
compaction will never defer periodic-triggered compactions; these will rewrite
inputs in full.

### Merge operators (RFC-0006)

Keys with merge operands spanning multiple inputs make their containing blocks
partially live.

### Compaction filters (RFC-0017)

A filter is said to be **stateless** if no state is carried from one entry to
the next. Another way to put it is that `filter(A ∪ B) = filter(A) ∪ filter(B)`.
If a filter is not stateless, the planner falls back to a full rewrite.

Filters declare statelessness by overriding
`CompactionFilter::is_stateless() -> bool` (default `false`).

Filter suppliers can check `CompactionJobContext.lazy_compaction_enabled`, for
example, if they want to opportunistically return a `NoopFilter` for some
segments.

If a filter emits `Delete` while writing an overlay block, it is treated as a
`Tombstone`.

Note: Just like today, blocks are unfiltered until rewritten, and there is no
guarantee this happens by a specific deadline. As with [TTL](#ttl-rfc-0003),
operators who need that guarantee can force an eager (non-lazy) rewrite.

### Resumable compaction (RFC-0013)

Since planning is deterministic (budgets are part of the frozen
`CompactionSpec`), resume re-derives the identical plan.

A base SST is retained in the manifest and storage (via GC) as long as any view
references it.

### Per-segment enablement (RFC-0024)

When segments (RFC-0024) are configured, the scheduler controls eligibility per
segment by setting the budget to zero in the `TieredCompactionSpec` submitted
for a segment.

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

- [ ] Compaction state persistence
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
  structures. No new state is persisted. Databases without lazy compaction are
  byte-identical. The read path [derives block ranges in memory](#read-path),
  matching `effective_range` behavior.
- **Existing public APIs (including bindings):** Read/write semantics are
  unchanged. Adds configuration: the enable flag, two budgets, per-segment
  opt-in, and the `is_stateless()` / `lazy_compaction_enabled` filter hooks.
- **Rolling upgrades / mixed-version behavior:** Interoperable. Older compactors
  perform ordinary rewrites. A newer compactor resuming a job re-derives the
  plan from the frozen `CompactionSpec` (see
  [Resumable compaction](#resumable-compaction-rfc-0013)).

## Testing

- **No-overhead guarantee:** Manifests and SSTs are byte-identical with the
  enable flag off.
- **Run disjointness and snapping:** Randomized compactions assert view and
  overlay ranges never overlap. All live keys remain reachable, and no
  superseded version is exposed, including versions an active snapshot below
  `retention_min_seq` still pins.
- **Resume determinism:** Kill/resume tests assert that the plan re-derived from
  the `CompactionSpec` is identical, `output_ssts` overlay correlation is
  stable, and failed matches fall back to full rewrites.
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
- On some object store (Linux filesystems), we can reclaim space by punching
  holes (e.g., using `fallocate(FALLOC_FL_PUNCH_HOLE)`). Is it worth mentioning?
- On some object store (GCS), we can reclaim space by rewriting objects (e.g.,
  using GCS's Rewrite API) in a way that is cheaper than reading+writing. Is it
  worth mentioning?
- Should we round the 'one block' minimum overlay size up to be greater than
  `read_ahead_bytes`?

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
