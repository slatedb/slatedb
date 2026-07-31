# GC-Safe SST ULID Timestamps

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Background](#background)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [Writer L0 IDs](#writer-l0-ids)
   - [Compaction IDs](#compaction-ids)
   - [Invariant Checks](#invariant-checks)
   - [Failure Handling](#failure-handling)
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
ULIDs as part of its deletion cutoff. Today the writer mints L0 SST IDs inside
the parallel upload workers, so mint order can differ from the order in which
L0s are published to the manifest. An uploaded but unpublished SST can then
have a ULID timestamp below the cutoff, and GC can delete it before it is
published.

This RFC fixes the race by changing where IDs are minted, not how:

- The writer allocates L0 physical SST IDs at dispatch, before parallel
  upload, in the same sequence order that L0s are later published.
- Newly flushed L0 views use the physical SST ID as their view ID.

This makes the fix structural. An SST that is already in an active manifest is
never deleted, because GC skips referenced SSTs. An SST that is uploaded but
not yet published always has a timestamp at or above the newest published L0,
so the `newest_l0` cutoff term protects it.

This RFC does not try to solve clock skew. It assumes skew is bounded, and
users set `min_age` for the margin they want against GC-versus-writer skew.

There are no manifest or SST format changes, and GC is unchanged. It stays a
pure deleter.

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

- `now - min_age`: keep objects whose age is less than or equal to `min_age`.
  The default `min_age` is 300 seconds.
- `compaction_low_watermark`: the minimum job ID timestamp across active
  compaction jobs and the most recently finished job, read from
  `.compactions`. It protects possible outputs of active compactions.
- `newest_l0`: the newest L0 physical SST timestamp in the latest manifest,
  falling back to `last_compacted_l0_sst_view_id` for trees with no live L0s.
  It protects L0s that are uploaded but not yet published.

Manifest V2 also has two ULID domains:

- `SsTableHandle.id`, the physical SST ID used by GC deletion.
- `SsTableView.id`, the view ID used by `last_compacted_l0_sst_view_id`.

If these IDs are minted independently, GC can compare timestamps from
different domains.

## Motivation

The writer mints physical L0 SST IDs inside the parallel upload workers. The
manifest writer publishes uploaded immutable memtables in sequence number
order. Mint order and publish order can therefore differ.

Publish order cannot be relaxed. `last_l0_seq` means every sequence at or
below that value is already in L0. Publishing a newer memtable while an older
one is missing would advance `last_l0_seq` past the missing range. WAL replay
skips entries at or below `last_l0_seq`, so it would not recover that range,
and with the WAL disabled there is no source to rebuild it from.

The unsafe sequence is:

1. Immutable memtable A has lower sequence numbers than immutable memtable B.
   Both are submitted to parallel upload workers.
2. B's worker mints its SST ID before A's worker does. B's timestamp is below
   A's, even though B is later by sequence number.
3. A's upload finishes and A is published. B's upload stalls, so B's SST is
   uploaded but not yet in the manifest.
4. `newest_l0` is now A's timestamp, which is above B's timestamp.
5. Once the stall exceeds `min_age` and the compaction watermark is also above
   B's timestamp, B's SST is unreferenced and below the cutoff. GC deletes it.
6. The manifest writer later publishes B, creating a manifest that references
   a missing object.

This is an ordering problem, not a clock problem. It happens on a single
well-behaved clock, because mint order and publish order differ. It was
reproduced by a deterministic simulation test failure in PR #1758.

## Goals

- Prevent GC from deleting newly flushed L0 SSTs before they are published.
- Preserve ULIDs as SST IDs.
- Avoid object-store copy or rename on the normal write path.
- Preserve parallel L0 upload throughput.
- Keep manifest and SST schemas unchanged.
- Make unsafe minting fail explicitly instead of causing silent data loss.

## Non-Goals

- Solve clock skew. Skew is assumed bounded, and users set `min_age` for the
  margin they want against GC-versus-writer skew.
- Redesign compacted SST GC around sequence numbers or manifest IDs.
- Fix unrelated full-ULID ordering bugs, such as choosing between two
  same-millisecond compaction IDs by comparing the full random suffix.

## Design

### Writer L0 IDs

Move L0 physical SST ID allocation from the upload worker to
`FlushTracker::dispatch_ready_memtables`. Dispatch already happens in sequence
number order, so SST timestamp order matches the order in which the manifest
writer publishes L0s. The race above cannot happen: every published L0 was
dispatched before any still-pending L0, so `newest_l0` cannot advance past a
pending SST's timestamp.

`UploadJob` carries the pre-allocated IDs:

```rust
pub(crate) struct UploadJob {
    pub(crate) imm_memtable: Arc<ImmutableMemtable>,
    pub(crate) segment_sst_ids: BTreeMap<Bytes, Ulid>,
}
```

The uploader writes each segment SST to the pre-allocated ID instead of
minting a new ID inside the parallel upload worker. If retention removes all
entries for a segment before upload, the unused ID is discarded.

When the manifest writer publishes a newly flushed L0, it creates an identity
view: `SsTableView.id` is the same ULID as the physical SST ID. This keeps the
timestamp used by `last_compacted_l0_sst_view_id` equal to the timestamp used
by GC deletion.

Views created by split, union, or rescaling reference existing physical SSTs,
not newly uploaded objects. They are unchanged by this RFC.

### Compaction IDs

Compaction job IDs, output SST IDs, and sorted-run view IDs are minted as
today. Outputs of an active job are protected by `compaction_low_watermark`,
and clock skew within `min_age` is covered by the `now - min_age` term. The
`.compactions` invariants below reject IDs that violate the watermark rules.

We do not check the job ID against the manifest's L0 timestamps. The
`.manifest` and `.compactions` files are updated independently, so such a
check would not hold as new L0s arrive, and it would not help anyway: GC does
not compare those timestamps, so a low job ID only lowers the cutoff and makes
GC more cautious, never less.

### Invariant Checks

These are `Invariant<T>` predicates from `slatedb-txn-obj` (PR #1741), not new
fields in the manifest or `db_state.rs`. They are registered in the central
stored object construction paths, not at individual write call sites, so new
update paths don't miss them.

For `.manifest`:

- `l0_ulid_cutoff`: a newly added L0 physical SST ID must have a timestamp at
  or above the newest L0 timestamp already in the manifest.

For `.compactions`:

- `compaction_job_id_cutoff`: a newly added compaction job ID must have a
  timestamp at or above the maximum existing compaction job ID timestamp.
- `sorted_run_ulid_cutoff`: each output SST ID and sorted-run view ID recorded
  for a compaction must have a timestamp at or above that compaction job ID
  timestamp.

The checks compare timestamp milliseconds, not full ULID ordering. Equal
milliseconds are safe because GC only deletes SSTs strictly below the cutoff.
A failure is reported as `InvalidClockTick` and the unsafe update is not
committed. The error message includes the rejected timestamp and the required
watermark.

### Failure Handling

With minting moved to dispatch, an invariant failure means clock skew or a new
minting path that skipped the rules, not a normal race. A minting path that
skips the rules is a bug in SlateDB, but the check only sees timestamps and
cannot tell the two causes apart. If the clocks are fine and the error keeps
happening, the user should report a bug.

- If the error is returned in a user call path, the caller gets the error and
  the `Db` stays open.
- If the error happens in a background task, such as flush or compaction, the
  `Db` is marked closed with a failed state.

Skew across a writer restart can fail the invariant: if a previous writer's
clock was ahead, a new writer's IDs fall below the committed timestamps and
are rejected. `min_age` does not help this case. The fix is to fix the clock
(run NTP) or wait until the wall clock passes the committed timestamps, then
reopen. Retrying without fixing the clock will fail again.

### Garbage Collection

GC does not change. It stays a pure deleter and keeps the same cutoff. All the
work in this RFC is on the minting side, so the IDs GC already reads are safe
to interpret.

## Implementation

- Move L0 physical SST ID allocation to
  `FlushTracker::dispatch_ready_memtables`.
- Add pre-allocated segment SST IDs to `UploadJob` and update the uploader to
  use them.
- Update `ManifestWriter::apply_uploaded_state` to create identity L0 views.
- Register the manifest and `.compactions` invariants in the shared
  construction paths for loaded and newly created stored objects.
- Return `InvalidClockTick` on invariant failure, with the rejected timestamp
  and required watermark in the error message.

## Impact Analysis

SlateDB features and components that this RFC interacts with:

- [x] Error model, API errors
- [ ] Sequence numbers
- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [x] Compaction state persistence
- [ ] Compaction strategies
- [x] Distributed compaction
- [x] Compactions format
- [ ] SST format or block format
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- L0 upload and compaction output paths still write each SST once.
- ID allocation moves from the upload worker to dispatch; the work is the
  same.
- The invariant checks are in-memory timestamp comparisons over the items in
  an update (new L0s, job IDs, outputs). They add no I/O, and their cost is
  small next to the object-store write.
- No object-store copy, rename, or extra GC CAS path is added.

### Observability

- Metrics: invariant failure count.
- Logging: on invariant failure, include the role, the rejected timestamp,
  and the required watermark.

### Compatibility

- Existing SST IDs remain valid ULIDs.
- Existing manifests remain readable.
- Existing projected views with distinct view IDs remain valid.
- Invariants must be enabled only after all writers and compactors in a
  deployment mint L0 IDs at dispatch, otherwise the old race can trip them.

## Testing

- Unit tests for identity L0 views and the manifest and `.compactions`
  invariants.
- Integration tests for parallel L0 upload where upload completion order
  differs from manifest publish order.
- Deterministic simulation test for the publish-order race that motivated
  this RFC.
- Fault-injection tests for writer and worker clock skew, checking that
  invariants fail loudly instead of losing data.

## Rollout

1. Move L0 SST ID allocation to dispatch and pass IDs through `UploadJob`.
2. Make newly flushed L0 views identity views.
3. Add invariants, metrics, and logs.
4. Enable strict invariant enforcement after all roles in the deployment are
   upgraded.

## Alternatives

### Increase `min_age` alone

- Reduces the probability that a staged SST is old enough to delete.
- Rejected as the only fix because upload stalls can exceed any practical
  value. This RFC fixes the publish-order race structurally and keeps
  `min_age` only as a best-effort knob.

### Exclude external SSTs from the cutoff

- Compute `newest_l0` from L0s owned by this database only, ignoring SSTs
  inherited from a clone or union parent. This would stop a parent's
  far-future L0 timestamp from raising this database's cutoff.
- Not taken. That case only happens under clock skew across databases, which
  we assume is bounded and out of scope. Adding it would make the cutoff logic
  more complex for a case we have not seen.

### Calibrate writer clocks against the object store

- Suggested in review: on each PUT, record the local time before and after,
  and check the object's `last_modified` falls within that window plus an
  error bound. This keeps each writer's clock close to the object store's
  clock and bounds skew directly.
- Deferred. It is a reasonable way to bound skew if the bounded-skew
  assumption proves too weak, but it adds a check to every write for a problem
  we are treating as out of scope.

### Monotonic allocator with a timestamp floor

- An earlier draft of this RFC added a `MonotonicSstIdAllocator`. It computed
  a floor from committed manifest and `.compactions` state, refused to mint
  below the floor, waited a bounded time for a lagging clock, and returned
  `InvalidClockTick` if the clock stayed behind.
- Dropped because the publish-order race does not need it, and skew within
  `min_age` is already safe. It added waiting, floor plumbing across the
  writer and compactor roles, and new failure modes for a problem the
  invariants already catch.

### Offset-based ULID generation

- Suggested in review: record the wall clock when the allocator starts, then
  mint timestamps as `max(last_issued_ms, max(0, floor_ms - start_ms) + now_ms)`.
  A lagging clock is shifted forward past the floor instead of waiting.
- Deferred. It is a good fallback if the bounded-skew assumption proves too
  weak. The trade-off is that every GC-relevant timestamp would have to be
  generated this way, and shifted timestamps are written into durable state.

### Run GC inside the `Db`

- Suggested in review: require GC to run inside the `Db` so GC and the writer
  can coordinate directly instead of relying on ID timestamps.
- Not taken because running GC as a separate process remains a supported
  deployment. Worth revisiting if timestamp-based safety proves fragile.

### Persisted GC cutoff

- Add a monotonic `gc_sst_cutoff_ms` field to manifest and compactions state.
  GC would persist the cutoff before deleting, and writers would validate new
  references against the persisted value.
- Not taken because it requires a schema change and makes GC a
  manifest/compactions writer.

### Sequence or manifest IDs for SSTs

- Replace ULID timestamp safety with sequence-number or manifest-ID safety.
- Rejected for this RFC because SSTs are written before manifest commit,
  compaction outputs do not naturally belong to the input data sequence, and
  clone/split/union timelines make ownership rules larger than this fix.

## References

- [RFC-0024: Segment-Oriented Compaction](0024-segment-oriented-compaction.md)
- [RFC-0025: Distributed Compaction](0025-distributed-compaction.md)
- [RFC-0026: Garbage Collector Boundary Files for Sequenced Metadata](0026-garbage-collector-boundary.md)
- [Issue #1707: Implement GC cutoff rule enforcement](https://github.com/slatedb/slatedb/issues/1707)
- [PR #1741: add `Invariant<T>` predicates to slatedb-txn-obj](https://github.com/slatedb/slatedb/pull/1741)
- [PR #1747: add `l0_ulid_cutoff` invariant + L0 ULID watermark helper](https://github.com/slatedb/slatedb/pull/1747)
- [PR #1758: enforce `l0_ulid_cutoff` invariant on manifest update](https://github.com/slatedb/slatedb/pull/1758)
- [Issue #356: Use latest manifest timestamp for GC instead of `Utc::now`](https://github.com/slatedb/slatedb/issues/356)
