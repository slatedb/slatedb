# GC-Safe SST ULID Timestamps

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Background](#background)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [SST IDs relevant to GC](#sst-ids-relevant-to-gc)
   - [SST ID Allocator](#sst-id-allocator)
   - [Writer L0 IDs](#writer-l0-ids)
   - [Compaction IDs](#compaction-ids)
   - [Owned Watermarks](#owned-watermarks)
   - [Invariant Checks](#invariant-checks)
   - [Garbage Collection](#garbage-collection)
- [Implementation](#implementation)
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

* [Kaivalya Apte](https://github.com/geeknarrator)

## Summary

SlateDB compacted SST garbage collection uses the timestamp embedded in SST
ULIDs as part of its deletion cutoff. This is unsafe when an uploaded SST has a
ULID timestamp older than the cutoff that GC can compute before the SST becomes
reachable from the manifest or `.compactions` state.

This RFC proposes changes to where these SST IDs (relevant for GC) are minted. The
writer allocates L0 SST IDs before parallel upload, in the same
sequence order that L0s are later published. Newly flushed L0 views use the
physical SST ID as their view ID. Compaction job IDs and compaction output IDs
are minted through the same monotonic allocator.

The allocator only allows minting ULIDs at or above its configured floor.
Manifest and compactions invariants then reject unsafe IDs to enforce monotonic
structure. This keeps the GC a pure deleter and does not require any
changes to the manifest and SST formats.

## Background

Compacted SST GC deletes SST objects that are not referenced by active
manifests or checkpoints and whose physical SST ULID timestamp is below a
calculated cutoff:

```text
cutoff = min(now - min_age, compaction_low_watermark, newest_l0)
delete when: sst_ulid_ts < cutoff && sst is not referenced
```

To decide whether an SST is referenced, GC reads the latest manifest and the
manifests retained by checkpoints. It collects the physical SST IDs from every
L0 view and sorted-run view in those manifests. Any compacted SST object whose
ID is absent from that set is treated as unreferenced. Pending uploads are not
in that set until a manifest commit records them.

The cutoff has three parts:

- `now - min_age`: do not delete objects newer than min_age.
- `compaction_low_watermark`: do not delete possible outputs of active
  compactions.
- `newest_l0`: do not delete possible L0s that are uploaded but not yet
  published.

This assumes every in-flight SST has a physical SST ULID timestamp at or
above any cutoff that GC can compute before the SST becomes reachable.

Manifest V2 also has two ULID domains:

- `SsTableHandle.id`, the physical SST ID used by GC deletion.
- `SsTableView.id`, the view ID used by `last_compacted_l0_sst_view_id`.

If these IDs are minted independently, GC can compare timestamps from different
domains.

## Motivation

The current writer mints physical L0 SST IDs inside the parallel upload worker.
The manifest writer publishes uploaded immutable memtables later, in sequence
number order. Mint order and publish order can therefore diverge.

The manifest writer must preserve sequence number order because `last_l0_seq`
means that every sequence at or below that value is already in L0. If a manifest
published a newer memtable while an older one was missing, it would still advance
`last_l0_seq` past the missing range.

Current WAL replay skips entries at or below `last_l0_seq`, so it would not
recover the missing older range. Supporting out-of-order publish would require a
gap-aware WAL replay protocol. With WAL disabled there is no source from which
to rebuild the missing range.

The unsafe sequence is:

1. Immutable memtable A has lower sequence numbers than immutable memtable B.
2. A and B are submitted to parallel upload workers.
3. A's worker mints and uploads a physical SST, but A is not yet in the
   manifest.
4. The manifest writer still must publish A before B, so B cannot be published
   first even if B's upload finishes earlier.
5. Meanwhile, already committed manifest state, such as another L0 or a
   `last_compacted_l0_sst_view_id`, advances `newest_l0` past A's physical SST
   timestamp.
6. A's SST doesn't exist in the active manifest and checkpoint reference set, and
   its physical SST timestamp is below the GC cutoff.
7. GC deletes A because it is old enough and unreferenced.
8. The writer later publishes A, creating a manifest that references a missing
   object.

Clock skew creates the same type of bug. A writer or compaction worker can
restart on a host whose clock is behind timestamps already committed to durable
state, then mint new SST IDs that are immediately eligible for GC.

## Goals

- Prevent GC from deleting newly flushed L0 SSTs before they are published.
- Prevent GC from deleting compaction outputs while their job is active.
- Preserve ULIDs as SST IDs.
- Avoid object-store copy or rename on the normal write and compaction paths.
- Preserve parallel L0 upload throughput.
- Keep manifest and SST schemas unchanged.
- Make unsafe minting paths fail explicitly instead of creating silent data
  loss.

## Non-Goals

- Redesign compacted SST GC around sequence numbers or manifest IDs.
- Redesign object-store consistency or conditional write behavior.
- Add multi-writer support.
- Fix unrelated full-ULID ordering bugs, such as choosing between two
  same millis compaction IDs by comparing the full random suffix.
- Remove all wall-clock use from SlateDB. This RFC only covers IDs whose
  timestamps participate in compacted SST GC.

## Design

### SST IDs used by GC

These IDs participate in compacted SST GC safety:

- L0 physical SST IDs.
- Compaction job IDs.
- Compaction output SST IDs.
- View IDs that can later feed `last_compacted_l0_sst_view_id`.

They are minted through one SST ID allocator. Newly flushed L0 view
IDs are not minted separately, they are set equal to the physical SST ID.

IDs that do not feed the compacted SST GC cutoff, such as worker IDs and
multipart upload IDs, are unchanged.

### SST ID Allocator

Add an internal allocator for SST IDs whose timestamps are used when deleting old compacted SST files:

```rust
struct MonotonicSstIdAllocator {
    floor_ms: i64,
    last_issued_ms: AtomicI64,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
}

impl MonotonicSstIdAllocator {
    fn new(
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
        floor_ms: i64,
    ) -> Self;
    async fn next_ulid(&self) -> Result<Ulid, SlateDBError>;
}
```

The caller computes `floor_ms` from committed manifest or `.compactions` state
before constructing the allocator. The constructor stores that floor and
initializes `last_issued_ms` to the same value. `next_ulid` refuses to mint an
ID with a timestamp below `max(floor_ms, last_issued_ms)`.
If the clock is behind, it computes the required wait:

```text
wait_ms = max(floor_ms, last_issued_ms) - now_ms
```

If `wait_ms` exceeds the configured skew wait, `next_ulid` returns
`InvalidClockTick` immediately. Otherwise it sleeps for that interval, checks the
clock again, and returns `InvalidClockTick` if the clock is still behind.

`last_issued_ms` catches backward clock steps after the allocator starts. Each
minting role should keep its allocator for the role or job lifetime; creating a
fresh allocator per SST would lose `last_issued_ms`. Tests must use this
allocator for SST IDs so clock-skew bugs exercise the same path as production.

Equal millisecond timestamps are safe for GC. The deletion filter only deletes
SSTs strictly below the cutoff, so two IDs with the same millisecond timestamp
are not ordered for GC safety by their random ULID suffix.

### Writer L0 IDs

Before minting L0 SST IDs, the DB writer computes the owned L0 floor and creates
`MonotonicSstIdAllocator` with that floor. The floor is the maximum owned L0
physical SST timestamp across all trees, falling back to
`last_compacted_l0_sst_view_id` for trees with no live L0s.

Move L0 physical SST ID allocation from the upload worker to
`FlushTracker::dispatch_ready_memtables`.

`dispatch_ready_memtables` already dispatches immutable memtables in sequence
number order. Allocating IDs there makes SST timestamp order match the order in
which the manifest writer is allowed to publish L0s.

`UploadJob` carries the pre-allocated IDs:

```rust
pub(crate) struct UploadJob {
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    pub(crate) segment_sst_ids: BTreeMap<Bytes, Ulid>,
}
```

The uploader writes each segment SST to the pre-allocated ID instead of minting a
new ID inside the parallel upload worker. If retention removes all entries for a
segment before upload, the unused ID is discarded.

When the manifest writer publishes a newly flushed L0, it creates an identity
view: `SsTableView.id` is the same ULID as the physical SST ID. This keeps the
timestamp used by `last_compacted_l0_sst_view_id` same as the
timestamp used by GC deletion.

Split, union, or rescaling can create new L0 views over existing physical SSTs.
Those views do not have to be identity views, because they are not newly
uploaded physical objects. If a newly minted view ID from such an operation can
later become `last_compacted_l0_sst_view_id`, it must be minted through
`MonotonicSstIdAllocator` constructed with the owned L0 floor. For union-ed
manifests where multiple L0s can reference the same source state, the view ID is
a watermark, not the object GC deletes. It is safe for that view ID to be older
than later L0s written by the target DB because later writes allocate after the
split or union operation's floor.

### Compaction IDs

The compactor coordinator reads the maximum existing compaction job ID
timestamp, constructs the allocator with that floor, and then mints compaction
job IDs through it. The coordinator owns job ID creation and may run on a
different machine from the workers.

For each claimed compaction job, the worker constructs a job-scoped output
allocator before writing outputs. The floor is at least:

- the compaction job ID timestamp, and
- any existing output SST timestamps for that job when a job is resumed.

The worker mints compaction output SST IDs and sorted-run view IDs through an
allocator constructed with that floor. If the worker's clock is behind the
floor, the allocator applies the bounded skew wait and returns
`InvalidClockTick` if the clock remains behind. This ensures active compaction
outputs are not below the `.compactions` low watermark that protects them from
GC.

### Owned Watermarks

Clones and unions can make this database reference SSTs that were created by
another database. Those SSTs may have ULID timestamps from another machine's
clock. If a parent or source database minted an SST with a far-future timestamp,
using that timestamp as the local writer floor could make the clone or union
target unable to flush until its own clock catches up.

Writer floors and the `l0_ulid_cutoff` invariant must therefore use only L0 SSTs
owned by this database. They must exclude SSTs referenced through clone parents,
union sources, or other external DB state. This is safe because this database's
GC does not delete external SSTs as newly minted objects in this database's
timeline.

Add an owned-watermark helper, such as
`max_owned_l0_ulid_timestamp_across_trees`. It scans the unsegmented tree and every
segment tree, takes the maximum physical SST ULID timestamp from live L0s owned by
this database, and falls back to the tree's local `last_compacted_l0_sst_view_id` when
that tree has no live owned L0s. It ignores SSTs listed in `external_dbs` and any 
inherited clone or union markers. If there is no owned L0 history, it returns no floor.

Use it in both places:

- constructing the DB writer's L0 SST ID allocator, and
- `l0_ulid_cutoff`, when checking newly added L0 SSTs.

### Invariant Checks

Register invariant checks in the central stored-object construction paths, not
at individual write call sites.

For `.manifest`:

- `l0_ulid_cutoff`: a newly added L0 physical SST ID must have a timestamp at or
  above the current manifest's owned L0 watermark.

For `.compactions`:

- `compaction_job_id_cutoff`: a newly added compaction job ID must have a
  timestamp at or above the maximum existing compaction job ID timestamp.
- `sorted_run_ulid_cutoff`: each output SST ID and sorted-run view ID recorded
  for a compaction must have a timestamp at or above that compaction job ID
  timestamp.

The checks compare timestamp milliseconds, not full ULID ordering. A failure is
reported as `InvalidClockTick`, and the unsafe update is not committed.

### Garbage Collection

This RFC proposes changes to the IDs that writers and compactors produce so the existing GC
cutoff is safe to interpret. Garbage collector remains a pure deleter.

## Implementation

- Add `MonotonicSstIdAllocator` with a constructor that takes `floor_ms` and
  `Arc<DbRand>`, plus `next_ulid`.
- Add `max_owned_l0_ulid_timestamp_across_trees` for constructing the DB writer's
  L0 SST ID allocator and for `l0_ulid_cutoff`.
- Wire writer startup to compute the owned L0 floor and construct the allocator
  with that floor.
- Move L0 SST ID allocation to `FlushTracker::dispatch_ready_memtables`.
- Add pre-allocated segment SST IDs to `UploadJob` and update the uploader to use
  them.
- Update `ManifestWriter::apply_uploaded_state` to create identity L0 views.
- Mint split, union, or rescaling view IDs through the allocator when they can
  feed `last_compacted_l0_sst_view_id`.
- Wire the compactor coordinator and workers to mint job IDs, output SST IDs,
  and sorted-run view IDs through `MonotonicSstIdAllocator`.
- Register manifest and compactions invariants in the shared construction paths
  for loaded and newly created stored objects.
- Return `InvalidClockTick` on invariant failure or allocator clock failure.

## Impact Analysis

SlateDB features and components that this RFC interacts with:

- [x] Error model, API errors
- [x] Sequence numbers
- [x] Manifest format
- [x] Checkpoints
- [x] Clones
- [x] Garbage collection
- [x] Database splitting and merging
- [x] Compaction state persistence
- [x] Compaction strategies
- [x] Distributed compaction
- [x] Compactions format
- [x] SST format or block format
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- L0 upload and compaction output paths still write each SST once.
- L0 dispatch does a small amount of extra CPU work to allocate one ID per
  touched segment.
- Startup or job activation can wait if the local clock is behind the required floor.
- No object-store copy, rename, or extra GC CAS path is added.

### Observability

- Metrics: allocator floor, skew wait count, skew wait duration,
  `InvalidClockTick` count, and invariant failure count.
- Logging: include role, local timestamp, required floor, and floor source when
  skew wait or invariant failure happens.

### Compatibility

- Existing SST IDs remain valid ULIDs.
- Existing manifests remain readable.
- Existing projected views with distinct view IDs remain valid.
- Strict invariants must be enabled only after all SST-minting roles use the
  allocator.

## Testing

- Unit tests for `MonotonicSstIdAllocator`, owned-watermark computation,
  identity L0 views, and manifest/compactions invariants.
- Integration tests for parallel L0 upload where upload completion order differs
  from manifest publish order.
- Integration tests for compaction outputs minted at or above the job ID,
  including resumed jobs with existing outputs.
- Deterministic simulation test for the publish-order race that motivated this
  RFC.
- Fault-injection tests for writer and worker clock skew.

## Rollout

1. Add `MonotonicSstIdAllocator` and owned-watermark helpers.
2. Wire writer startup to compute the owned L0 floor and construct the allocator
   with that floor.
3. Move L0 SST ID allocation to dispatch and pass IDs through `UploadJob`.
4. Make newly flushed L0 views identity views.
5. Wire compactor coordinator and worker ID minting through the allocator.
6. Add invariants, metrics, and logs.
7. Enable strict invariant enforcement after all roles in the deployment are
   upgraded.

## Alternatives

### Increase `min_age`

- Reduces the probability that a staged SST is old enough to delete.
- Rejected because stalls, clock skew, and backpressure can exceed any practical
  value.

### Re-mint and Copy on Invariant Failure

- On manifest invariant failure, copy the already uploaded SST to a newly minted
  ID and retry.
- Rejected as the primary design because it adds object-store copy cost to the
  contended path and treats a safety violation as a normal retry.

### Global Monotonic ULID Allocator

- Use one monotonic ULID allocator for all ULIDs in SlateDB.
- Deferred because only IDs that feed compacted SST GC need this ordering. A
  global allocator would add contention and couple unrelated ID domains.

### Persisted GC Cutoff

- Add a monotonic `gc_sst_cutoff_ms` field to manifest and compactions state.
  GC would persist the cutoff before deleting, and writers would validate new
  references against the persisted value.
- This gives a simple CAS-based safety argument and is a good fallback if
  allocator coverage proves fragile.
- Not recommended here because it requires a schema change and makes GC a
  manifest/compactions writer which feels like breaking abstraction.

### Sequence or Manifest IDs for SSTs

- Replace ULID timestamp safety with sequence-number or manifest-ID safety.
- Rejected for this RFC because SSTs are written before manifest commit, because
  compaction outputs do not naturally belong to the input data sequence, and
  because clone/split/union timelines make ownership rules larger than this fix.

## Open Questions

- What should the default maximum skew wait be?
- Should `InvalidClockTick` close the DB writer or fail only the current flush?
- Should the persisted GC cutoff alternative be promoted if reviewers prefer a
  CAS-based design over allocator discipline?
- Should compacted SST GC eventually use object-store `last_modified` instead of
  the GC host's wall clock for the `now - min_age` term?

## References

- [RFC-0024: Segment-Oriented Compaction](0024-segment-oriented-compaction.md)
- [RFC-0025: Distributed Compaction](0025-distributed-compaction.md)
- [RFC-0026: Garbage Collector Boundary Files for Sequenced Metadata](0026-garbage-collector-boundary.md)
- [Issue #1707: Implement GC cutoff rule enforcement](https://github.com/slatedb/slatedb/issues/1707)
- [PR #1741: add `Invariant<T>` predicates to slatedb-txn-obj](https://github.com/slatedb/slatedb/pull/1741)
- [PR #1747: add `l0_ulid_cutoff` invariant + L0 ULID watermark helper](https://github.com/slatedb/slatedb/pull/1747)
- [PR #1758: enforce `l0_ulid_cutoff` invariant on manifest update](https://github.com/slatedb/slatedb/pull/1758)
- [Issue #356: Use latest manifest timestamp for GC instead of `Utc::now`](https://github.com/slatedb/slatedb/issues/356)
