# Lazy Compaction

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Principle and terminology](#principle-and-terminology)
  - [Planning](#planning)
  - [Read path](#read-path)
  - [Garbage collection](#garbage-collection)
  - [TTL (RFC-0003)](#ttl-rfc-0003)
  - [Merge operators (RFC-0006)](#merge-operators-rfc-0006)
  - [Compaction filters (RFC-0017)](#compaction-filters-rfc-0017)
  - [Per-segment enablement](#per-segment-enablement)
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

This RFC introduces **Lazy Compaction**. Its purpose is to avoid rewriting
untouched SST regions during compaction. In the best cases, non-overlapping
inputs, we simply move the SST (
[trivial moves](https://github.com/facebook/rocksdb/wiki/Compaction-Trivial-Move)).
In low-overlap situations, we make a tradeoff between reclaiming storage (full
compaction) and avoiding work. That tradeoff is configurable.

It reuses the existing `SsTableView` key-bounded projections already present in
the manifest format, so the manifest format is unchanged. The only new persisted
state is the compaction plan recorded in the compactor's job state for
resumability.

## Motivation

No deployment runs a full compaction to physically reclaim every deleted row the
instant it is deleted. Therefore every LSM deployment trades storage space
against write amplification, and the lever it tunes is **compaction frequency**:
compacting more often reclaims space sooner but rewrites the same data more
times; compacting less often saves writes but lets dead bytes accumulate.

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

- **Avoid pointless rewrites of low-overlap inputs:** Rewrite only overlapping
  blocks, keeping the remainder by reference, instead of rewriting whole SSTs to
  reclaim ~0 space.
- **Make the space vs. compute/IO tradeoff adjustable:** Let deployments choose
  how much storage to trade for lower compute and I/O instead of always
  rewriting inputs in full.
- **No overhead when unused:** Databases without lazy compactions are
  byte-identical to today.
- **Graceful degradation:** Fall back to ordinary rewrites under budget pressure
  to prevent unbounded fragmentation.

## Non-Goals

- **Value separation / blob files:** We do not investigate separating large
  values into independent blob files (e.g., Titan or BlobDB).

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
the view, and its live keys are absorbed into the adjacent overlay. Because
views snap inward and never outward.

### Budgeting

We have two budgets:

- **Space budget**: bytes that no view references: superseded versions, plus
  records that a full rewrite would have dropped, such as bottom-level
  tombstones. It is measured as a fraction of the database's estimated live
  bytes (default: 10%, minimum 200 MiB).
- **Manifest bloat budget**: extra manifest entries caused by lazy compaction.
  It is measured as a fraction of the number of referenced SSTs (default: 10%,
  minimum 1000).

Both quantities are measured database-wide and derived from the manifest. They
are a target, not a guarantee. Concurrent compactions (RFC-0024 segments,
RFC-0025 distributed compaction) may therefore transiently overshoot a budget
they could not see each other consuming. This is transient and will be corrected
next time a compaction is planned.

In some special cases (eg:
["stateful" compaction filters](#compaction-filters-rfc-0017)), we must rewrite
the input. Otherwise we keep as much as possible by reference and let the two
budgets decide what to rewrite.

### Planning

Planning considers the compaction's inputs: the SSTs and views of the runs being
compacted, together with the bases those views point into. Bases referenced by
views in runs *outside* this compaction are left alone. Such bases will be
reclaimed when the runs holding their views are themselves compacted; the
compaction scheduler may prioritize runs whose views defer the most dead bytes.

Planning reads manifest metadata (key ranges, sizes) plus each input SST's index
block — block separators and offsets — and derives block liveness by correlating
each view's `visible_range` with its base's block index. At block granularity
the resulting dead-byte figures are exact, not estimates: block sizes fall out
of index offsets, and a base's dead bytes are precisely its fully dead blocks
plus the blocks superseded by overlays (kept blocks are fully live by
definition). Only the "records a full rewrite would drop" component — e.g.,
bottom-level tombstones inside fully live blocks — is estimated, from the
per-block stats. No data blocks are read.

- Partially live blocks are merged and rewritten as overlays; fully dead blocks
  are dropped by clamping them out of any view; contiguous stretches of fully
  live blocks start as kept references. An input whose blocks are all fully live
  is kept whole — a pure move.
- Keeping a reference saves rewrite I/O but leaves dead bytes resident (the
  space budget) and adds manifest entries (the manifest budget). We rewrite the
  least valuable references — those saving the fewest bytes per unit of the
  binding budget — until both budgets hold.

These two budgets are the only tunables. The per-input cases have no thresholds
of their own: rewriting a tiny, mostly-overwritten, or over-fragmented input,
and rematerializing a mostly-dead base naturally emerge from these budget
constraints rather than requiring individual heuristics.

```
# plan() returns the new sorted run as a list of OutputDecisions, each one a:
#   ViewDecision(fragment)  - a contiguous stretch of fully live blocks kept by
#                             reference, pointing back at the input SST
#   RewriteDecision(blocks) - live data written fresh, independent of the input
# Lazy compaction keeps blocks by reference until that would exceed a budget,
# then rewrites the least valuable fragments to get back under it.

def plan() -> list[OutputDecision]:
    # The runs being compacted; views among them bring the base SSTs they
    # point into. Bases referenced only by runs outside this compaction are
    # reclaimed when those runs are compacted, never here.
    inputs = pick_overlapping_runs()

    # Stateful filters can't run on a subset of the data, so lazy compaction is
    # off for this job: rewrite every input in full.
    if not filter.is_stateless():
        return [RewriteDecision(sst.live_blocks) for sst in inputs]

    # Classify block liveness by correlating key ranges with the inputs' SST
    # indexes (index blocks only; no data reads). Partially live blocks across
    # all inputs are merged and rewritten as overlays, emitting only surviving
    # keys. Fully dead blocks are clamped out of any view. Contiguous stretches
    # of fully live blocks become kept fragments; an input whose blocks are all
    # fully live is a single whole-SST fragment: a pure move.
    decisions = [RewriteDecision(blocks) for blocks in overlay_regions(inputs)]
    kept = [frag for sst in inputs for frag in sst.fully_live_fragments]

    # Keeping a fragment saves rewriting it, but leaves its base's dead bytes
    # resident (space budget) and adds one view entry (manifest budget). While
    # either budget is exceeded -- counting current global usage from the
    # manifest plus this plan's additions -- rewrite the fragment that saves
    # the fewest bytes per unit of the binding budget. This is what rewrites
    # the tiny, mostly-overwritten, and over-fragmented inputs and
    # rematerializes mostly-dead bases -- no per-case thresholds needed.
    while over_budget(kept):
        worst = min(kept, key=bytes_saved_per_budget_unit)
        decisions.append(RewriteDecision(worst.blocks))
        kept.remove(worst)

    return decisions + [ViewDecision(frag) for frag in kept]
```

Compaction strategies must account for run sizes using **live (view-clamped)
sizes**, not the physical bytes a run pins. With views the two diverge: a lazily
compacted run may reference large bases while representing little live data.
Sizing runs by physical bytes would make them look artificially large, causing
strategies like size-tiered compaction to re-pick them and defeating the
deferral. Live sizes are derived from the same block-index correlation that
planning performs.

### Read path

The read path code is unchanged; it leverages the existing `effective_range`
clamp and sorted-run routing. Point reads route to at most one view per run
*before* bloom consultation, so a lookup never consults a view whose range
excludes the key. A kept view uses its base's bloom filter, built over all of
the base's keys including dead ones. Because bloom filters are sized in bits per
key, its false-positive rate matches what a fresh filter at the same sizing
would give; the only cost of sharing it is fetching a filter block larger than a
rewrite would have produced, bounded because the budgets force mostly-dead bases
to be rewritten. Range scans may cross more, smaller views than they would over
an eagerly compacted run, opening more SST indexes; that fragmentation is
bounded by the manifest budget.

### Garbage collection

Reusing `SsTableView` requires no GC changes. GC computes the live set by
scanning SST IDs referenced by views in manifests/checkpoints, deleting
unreferenced ones, as it does today.

### TTL (RFC-0003)

TTL reclamation relies on **periodic compaction** (RFC-0003): the scheduler
compares each SST's `SsTableInfo#timestamp` against
`periodic_compaction_interval` and rewrites cold SSTs on a time cadence, letting
the TTL filter tombstone expired rows and drop them. Lazy compaction defers
rewrites, so a lazily compacted periodic job would leave
expired-but-unsuperseded rows resident behind views and never reclaim them.

Lazy compaction therefore never defers a periodic-triggered job: when a
compaction is scheduled because an SST has aged past
`periodic_compaction_interval` (RFC-0003), its inputs are rewritten in full even
while lazy compaction is enabled. Overlap-triggered jobs stay lazy. This is not
TTL-specific — it covers any effect that depends on a guaranteed rewrite
cadence, such as bottom-level tombstone dropping or compliance filters.

The cadence keeps working under deferral because a kept base is never rewritten,
so its `SsTableInfo#timestamp` keeps aging; periodic selection still picks it up
once it crosses the interval, and a base's effective age resets only when it is
rematerialized. A base referenced by views in several runs is reclaimed
incrementally: each run drops its views when it is itself periodically
compacted, and the base object is removed (via GC) once the last referencing
view is gone.

### Merge operators (RFC-0006)

A key whose merge operands span multiple inputs makes every block containing one
of its records partially live, so all of its operands route to a single overlay
and merge there. Keys in kept views therefore have no other operands in this
compaction to merge against. Cross-level merges are unaffected. Bottom-level
operand finalization is deferred for view keys; read-time merges resolve them,
as they already do for unmerged operands in upper levels. Hot, merge-heavy keys
end up rewritten anyway.

### Compaction filters (RFC-0017)

We introduce the concept of a "stateless" filter. Roughly speaking, a filter is
"stateless" if it can run on any subset of the inputs and give the same
decisions. Example of stateless filters include custom TTLs and `NoopFilter`.

A filter can report being stateless by overriding
**`CompactionFilter::is_stateless() -> bool`** (default `false`). When a job
would lazily compact but its filter is not stateless, the planner falls back to
a full rewrite.

A stateless filter still only runs on data that is actually rewritten. Kept
regions are never filtered, and an untouched region consumes no budget, so
nothing ever forces a rewrite of a cold, never-overlapping region: the filter's
effects there are deferred indefinitely, not merely delayed. This matches the
existing contract — filters run only when compaction touches data — but
suppliers that rely on compaction for guaranteed effects (e.g., purging data for
compliance) must either keep `is_stateless()` returning `false` or disable lazy
compaction for the affected segments.

A supplier can decide to run stateful filters in situations other than lazy
compaction. This is done by checking the new
**`CompactionJobContext.lazy_compaction_enabled`.** If set, the
`CompactionFilterSupplier` can waive filtering by returning a `NoopFilter`
(trivially stateless) for cold segments while keeping its real filter on hot
ones.

If a filter emits `Delete`, that is treated as a `Tombstone`.

### Per-segment enablement

Lazy compaction is configured through the compactor's settings: a global enable
flag (default: off) and the two budgets. When segments (RFC-0024) are
configured, eligibility can additionally be restricted per segment.

SlateDB partitions the keyspace into segments compacted independently
(RFC-0024). Segments with frequent updates (e.g., metadata) gain little from
overlays, consuming their budgets on churn, whereas cold or large-value segments
benefit most. Per-segment opt-in lets deployments restrict lazy compaction to
cold segments.

This provides an up-front choice instead of relying on the budgets being
exhausted to trigger rematerialization on hot segments.

The core lazy compaction mechanism (overlay construction and the two budgets)
operates independently of RFC-0024; only the per-segment opt-in mechanism
depends on RFC-0024's segment structure.

### Resumable compaction (RFC-0013)

With lazy compaction, linear merges become segmented: the executor writes only
impacted ranges as overlays, while non-impacted ranges become views. Resume
cursor tracking (RFC-0013) cannot rely solely on the last output's
`last_written_key_and_seq` since views lack this.

Replanning on resume cannot be guaranteed to be deterministic — planning reads
liveness, dead-byte figures, and budget usage from the manifest and input
indexes, which other jobs may have changed in the meantime — so the job's state
instead persists the plan itself as **`lazy_compaction_plan`**: the ordered list
of output decisions (view ranges and overlay key ranges). On resume, the
compactor reloads the plan, correlates already-written overlays in
`output_ssts`, and resumes the active overlay's cursor. If
`lazy_compaction_plan` is missing (e.g., the job was started by an older
compactor), resume falls back to rewriting the remaining ranges in full.

For crash recovery (RFC-0013), validation still works: overlays are ordinary
SSTs recorded in `output_ssts`, and views are recreated from the persisted plan
at completion. The completion commit replaces the input runs' entries with the
new run's views and overlays. A base SST's top-level `ssts` entry is retained
for as long as any view — in this run or any other — references it; the entry,
and later the object itself (via GC), go away only once the last referencing
view is dropped.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that
apply.

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

- **Latency (reads/writes/compactions):** Point reads are unaffected; kept views
  are served under the `effective_range` clamp, matching existing latency (see
  [Read path](#read-path)). Range scans can cross more, smaller views, bounded
  by the manifest budget. Writes and ingest are unchanged. Compaction latency
  decreases for low-overlap workloads, reading and writing only their changed
  fraction.
- **Throughput (reads/writes/compactions):** Compaction throughput improves in
  proportion to bytes not moved. Read and write throughput are unchanged.
- **Object-store request (GET/LIST/PUT) and cost profile:** Compaction GET and
  PUT traffic scales with the overwritten fraction rather than the whole input,
  plus one index-block GET per input SST at planning time. The countervailing
  costs are more manifest entries (an interior overlay splits a view in two),
  bounded by the manifest budget, and more retained base objects making GC
  bucket-LIST scans heavier, bounded by the space budget.
- **Space, read, and write amplification:** Write amplification is reduced.
  Point-read amplification is unchanged; each key is covered by at most one view
  per run. Scan amplification can rise slightly with view fragmentation, bounded
  by the manifest budget. Space amplification rises temporarily as superseded
  bytes stay resident in a base until fully dereferenced or rematerialized,
  bounded by the space budget (resident dead bytes) and the manifest budget
  (entries).

### Observability

- **Tunables and defaults:**
  - Enable flag (default: off): turns lazy compaction on; without it,
    compactions behave exactly as today.
  - Space budget (default `10%` of estimated live bytes, minimum `200 MiB`): the
    dead bytes lazy compaction may leave resident across the database. Bounds
    space amplification.
  - Manifest budget (default `10%` of referenced SSTs, minimum `1000`): the
    extra manifest entries (views beyond one per referenced SST) lazy compaction
    may add. Bounds metadata growth.
- **Metrics:** New counters and gauges under the `slatedb.compactor.*` namespace
  tracking usage of the two budgets:
  - `slatedb.compactor.resident_dead_bytes` (gauge): Estimated dead bytes held
    resident — usage of the space budget.
  - `slatedb.compactor.lazy_manifest_entries` (gauge): Extra manifest entries
    (views beyond one per referenced SST) — usage of the manifest budget.
  - `slatedb.compactor.rematerialization_count` (counter): Rematerialization
    events (kept references rewritten to reclaim a base).
  - `slatedb.compactor.lazy_compaction_bytes_saved` (counter): Total bytes saved
    by lazily compacting blocks instead of rewriting them.
- **Logging:** None beyond existing compaction logging.

### Compatibility

- **Existing data on object storage / on-disk formats:** No manifest format
  change; views in sorted runs reuse the existing `CompactedSsTableView` /
  `SortedRunV2` structures. The only new persisted state is
  `lazy_compaction_plan` in the compactor's job state (see
  [Resumable compaction](#resumable-compaction-rfc-0013)). Databases without
  lazy compactions are byte-identical to today. The read path
  [derives block ranges in memory](#read-path), matching `effective_range`
  behavior.
- **Existing public APIs (including bindings):** No change to read/write
  semantics. Adds only configuration: the enable flag, the two budgets, the
  per-segment opt-in, and the `is_stateless()` / `lazy_compaction_enabled`
  filter hooks.
- **Rolling upgrades / mixed-version behavior:** Interoperable because the
  manifest format is unchanged. Older compactors perform ordinary rewrites. A
  newer compactor [resuming a job](#resumable-compaction-rfc-0013) with a
  missing or untrusted `lazy_compaction_plan` falls back to rewriting remaining
  ranges, requiring no upgrade ordering.

## Testing

- **No-overhead guarantee:** With the enable flag off, manifests and SSTs are
  byte-identical to today's output.
- **Run disjointness and snapping:** Randomized compactions asserting that the
  view and overlay ranges of a run never overlap, that every live key remains
  reachable, and that no superseded version is re-exposed (inward-snapping
  correctness).
- **Resume determinism:** Kill/resume tests asserting that the persisted
  `lazy_compaction_plan` is honored, overlay correlation in `output_ssts` is
  stable, and the fallback to full rewrites triggers when the plan is missing.
- **Budget convergence:** Concurrent compactions that transiently overshoot a
  budget converge back under it; small databases respect the budget floors.
- **Filter semantics:** Stateless filters run on rewritten data only; stateful
  filters force full rewrites; `lazy_compaction_enabled` is surfaced to
  suppliers.

## Rollout

TODO

## Alternatives

- **Whole-SST trivial move only:** Moving inputs by reference only when wholly
  disjoint misses partial-overlap cases. This RFC generalizes trivial moves to
  block-aligned ranges.
- **Explicit reference counting:** Implementing reference counts over base SSTs
  would reclaim dead holes before the entire base is dereferenced. This is
  rejected for now to keep the garbage collector simple (relying on
  set-membership over object IDs).
- **Read-time overlap resolution:** Letting the views of a run overlap and
  resolving precedence at read time would avoid overlays entirely, but it adds a
  merge to every read and breaks the run-disjointness invariant the read path
  relies on.
- **Persisted block-range field:** Persisting block indexes (or per-view block
  ranges) in the manifest would avoid the planning-time index-block GETs, but
  changes the manifest format for metadata that is cheap to fetch and cache.
- **Per-view bloom rebuild:** Rebuilding a bloom filter over only a view's live
  keys would only shrink the filter block fetched at read time — bloom filters
  are sized in bits per key, so sharing the base's filter does not raise the
  false-positive rate — yet it requires reading the very keys lazy compaction
  avoids reading, costing nearly as much as a rewrite.
- **Per-sorted-run fragmentation limit:** A separate cap on views per run is
  subsumed by the manifest budget, which bounds fragmentation globally without
  adding another tunable.
- **In-place hole punching / subsetting (future expansion):** The space overhead
  of leaving dead holes resident in base files could potentially be alleviated
  in the future by using storage-specific operations that punch holes or subset
  files without a full download/rewrite. Examples include leveraging GCS's
  Rewrite API to subset/re-compose cloud objects, or using UNIX
  `fallocate(FALLOC_FL_PUNCH_HOLE)` on local filesystems.

## Open Questions

- **Default budget values:** the space and manifest budget defaults (10% with
  floors of 200 MiB and 1000 entries) are estimates; production defaults require
  empirical tuning (see [Observability](#observability)).

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
