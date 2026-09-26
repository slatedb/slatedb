# SlateDB RFC: Parallel L0 Compactions Within a Segment

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Terminology and Current Ordering](#terminology-and-current-ordering)
  - [Safety Invariants](#safety-invariants)
  - [Example: One Parallel L0 Lane](#example-one-parallel-l0-lane)
  - [Planning and Admission](#planning-and-admission)
  - [Worker Execution and Retention](#worker-execution-and-retention)
  - [Ordered Coordinator Commit](#ordered-coordinator-commit)
  - [Failure, Restart, and Recovery](#failure-restart-and-recovery)
  - [Interactions with Other Compactions](#interactions-with-other-compactions)
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

* Ryan Dielhenn

## Summary

SlateDB serializes L0 compactions within each segment. This RFC splits the
oldest L0 inputs into batches that different workers can execute concurrently.
The coordinator commits their outputs from oldest to newest. This preserves
the existing L0 watermark and read precedence without a storage format change.
The first version supports L0-only tiered compactions in the root tree and
named segments.

## Motivation

Segment-oriented compaction (RFC-0024) permits concurrent L0 compactions across
segments. A single hot segment still uses only one L0 compaction worker,
even with spare workers and a larger global concurrency limit.

Subcompactions (RFC-0028) divide one job across tasks within an executor.
This proposal creates separate jobs so that multiple machines can compact
one segment at the same time.

## Goals

- Use multiple workers to compact L0 within one segment.
- Preserve reads, scans, snapshots, deletes, and merge semantics after every commit.
- Reuse existing job persistence, worker claims, and garbage collection (GC).
- Bound parallel L0 work per segment, within the global execution budget.

## Non-Goals

- Parallel L0+sorted-run compactions within one segment.
- Segment drains concurrent with L0 compactions in that segment.
- Changes to public read/write APIs, SST formats, or manifest formats.
- L0 sublevels or new sequence-range metadata.

## Design

This design was reviewed against upstream `main` at
[`c4868b79`](https://github.com/slatedb/slatedb/commit/c4868b79b9201c1f360b3e8587491207392e9e76)
on September 25, 2026.

### Terminology and Current Ordering

An L0 view identifies one SST input in a tree. A batch is an ordered group
of L0 views assigned to one compaction job. A lane is the set of admitted
L0-only batches in one segment. Siblings are batches in that same lane.
The empty-prefix root tree follows the same rules as a named segment.

Each tree lists L0 views from newest to oldest. Its sorted runs also appear
newest first, by descending sorted-run ID. Reads preserve this source
precedence, even when they fetch data concurrently.

The watermark, `last_compacted_l0_sst_view_id`, marks the newest L0 view that
the compactor absorbed. The writer/compactor manifest merge removes that view
and every older view. Therefore, each L0 commit must remove a contiguous
suffix: a group that includes the oldest view and skips no intervening views.

The destination sorted-run ID is the existing `CompactionSpec::destination`
field. It names the output run and determines its position in the sorted-run
list. The job has a separate ULID for identity and retries. No new batch
incarnation or lane ID is needed: persisted source lists, destinations, and
manifest order describe the lane.

### Safety Invariants

These rules apply to admission, execution, and commit:

1. Each batch contains consecutive L0 views in newest-to-oldest order and no
   sorted-run sources.
2. All admitted, uncommitted batches in a lane together cover the oldest L0
   suffix, with no overlaps or gaps.
3. Destination sorted-run IDs increase from older batches to newer batches.
   At allocation, each new L0 destination exceeds all committed and previously
   reserved destination IDs across all trees.
4. A batch can commit only when its sources are the current oldest L0 suffix.
   A completed newer batch waits for its older siblings.
5. A commit advances the watermark only to the newest view in that batch.
   It never skips an uncommitted input.
6. A worker can drop a delete marker (tombstone) only when no older data outside its
   batch can contain the deleted value. For an L0-only batch, this requires
   the oldest L0 suffix and an empty sorted-run list. All other batches receive
   `is_dest_last_run = false`. Normal snapshot retention rules still apply.

Rule 6 prevents a delete in a newer batch from disappearing while an older
batch still contains the value. An empty sorted-run list alone does not prove
that a batch contains all the oldest data.

### Example: One Parallel L0 Lane

Assume one segment has six L0 views and an existing sorted run, `SR(9)`:

```text
L0: [L6, L5, L4, L3, L2, L1]       L1 is oldest
SR: [SR(9)]
```

With two views per batch and a lane limit of three, the scheduler creates:

```text
oldest                                                      newest
  A: [L2, L1] -> SR(10)    B: [L4, L3] -> SR(11)    C: [L6, L5] -> SR(12)
```

Each arrow describes one job that merges its views into one output sorted run.
A, B, and C are siblings. Their destination sorted-run IDs are 10, 11, and 12.

Workers can finish in any order. If B and C finish first, they stay
`Compacted` in `.compactions`, the durable job state. Their inputs remain
visible in L0. Once A finishes, the coordinator can commit A, then B, then C:

```text
after A:  L0 [L6, L5, L4, L3]       SR [SR(10), SR(9)]
after B:  L0 [L6, L5]               SR [SR(11), SR(10), SR(9)]
after C:  L0 []                     SR [SR(12), SR(11), SR(10), SR(9)]
```

The coordinator can apply these steps in memory and persist one manifest.
Each step keeps newer data ahead of older data. Remaining L0 views precede
all sorted runs, and higher destination IDs preserve precedence between batches.

### Planning and Admission

Add `CompactorOptions::max_concurrent_l0_compactions_per_segment`, with a
default of `1` and a minimum of `1`. Its effective limit is capped by
`max_concurrent_compactions`. A value of one keeps L0 execution serial
within each segment.

The per-segment limit counts all reserved batches, including completed jobs
that wait to commit. The global execution budget keeps its current meaning:
`Submitted`, `Scheduled`, and `Running` consume slots, while `Compacted` does
not. Counting waiting batches toward the lane limit bounds their retained
outputs. Lowering the limit stops new admissions until capacity is available.
It does not cancel existing batches.

For each tree, the size-tiered scheduler extends the lane as follows:

1. Identify the L0 suffix reserved by existing batches. With no lane, the
   reserved suffix is empty.
2. Select the oldest remaining individual L0 views directly next to that
   suffix. Take up to `max_compaction_sources` views, preserving their list order.
3. If fewer than `min_compaction_sources` remain, stop adding L0 batches for
   this tree. Also apply the existing conflict and size-tiered backpressure rules.
4. Create one `CompactionSpec` with those views as its sources. Allocate its
   destination sorted-run ID above all committed and active destinations,
   including proposals from this scheduling pass.
5. Reserve those inputs and continue toward newer views on later scheduling
   passes, until a capacity limit or eligibility rule stops admission.

For example, suppose A already reserves `[L2, L1]` in the six-view example.
The unreserved views are `[L6, L5, L4, L3]`. With a maximum of two sources,
the next batch is `[L4, L3]`. These are individual SST views, not existing
batches. The scheduler groups them into one job by placing both IDs in its
source list.

The scheduler already uses round-robin passes in
[`SizeTieredCompactionScheduler::propose`](../slatedb/src/size_tiered_compaction.rs).
It orders trees by decreasing L0 count and picks at most one job per tree
per pass. This proposal preserves that policy. Spare slots allow later passes
to select more batches from a hot tree. This is fairness within a proposal
call, not a rotating starting tree across calls.

Admission must enforce the invariants for internal, manual, and custom-scheduler
submissions. `CompactorState::add_compaction` alone is insufficient because
remote submissions also reach the coordinator through `.compactions` refreshes.
Before promotion to `Scheduled`, coordinator validation must reject overlapping
inputs, gaps, reversed destinations, conflicting drains or mixed compactions,
and admissions above the lane limit.

Process `Submitted` batches in source-age order. A submission can reserve
inputs during planning, but only a valid predecessor can justify admitting a
newer batch. Reject an invalid submission without cancelling an existing valid
lane. Custom schedulers must obey the same rules.

### Worker Execution and Retention

Workers claim separate `Scheduled` jobs through the existing compare-and-swap
protocol. Each worker merges its batch through the normal executor.
If another worker takes over a job, it resumes from the persisted
subcompaction plan and output SST progress. The job retains its source list
and destination sorted-run ID.

The worker must implement invariant 6. Today,
[`build_job_args`](../slatedb/src/compaction_worker.rs) treats an empty
sorted-run list as containing the oldest data since the scheduler always selects the last L0 suffix.
For an L0-only job, replace that test with:

```text
is_dest_last_run =
    tree.compacted.is_empty()
    && job.l0_sources_are_the_current_l0_suffix()
```

The worker uses its manifest read after claiming the job. Consider two
batches with no committed sorted runs:

```text
A (older): k = "value" at sequence 1
B (newer): delete(k)   at sequence 2
```

If B drops the delete marker, B writes no entry for `k`. A then commits
`k = "value"` into its sorted run. B commits afterward and removes its L0
inputs, including the only remaining delete marker. A subsequent read returns
`"value"`, although the latest write deleted it.

With the new test, B keeps the delete marker. Before A commits, B does not
cover the oldest L0 suffix. After A commits, its sorted run makes the
sorted-run list non-empty. This rule remains safe when B retries.

The executor keeps its existing snapshot and merge-operand retention behavior.
The corrected flag controls tombstone removal and the context that compaction
filters receive. It does not replace those other retention rules.

### Ordered Coordinator Commit

The commit frontier is the oldest uncommitted batch. A worker marks its job
`Compacted` when execution finishes. Only a batch at the frontier can change
the manifest.

Replace the current job scan in
[`commit_compacted_entries`](../slatedb/src/compactor.rs) with this loop for
each tree:

```text
while a Compacted L0-only job matches the current oldest L0 suffix:
    validate its sources and destination against the in-memory manifest
    apply its output and advance the watermark to its newest source
    recompute the frontier from the updated L0 list
leave completed newer siblings in Compacted
```

Admission permits a newer batch to run while older batches remain active.
Commit requires the batch to be at the frontier. These gates must be distinct:
waiting for an older sibling is not a validation failure.

The same rule applies to trivial moves, which reuse input SSTs without a
worker. Current main can commit these directly from `Submitted` in
`maybe_validate_submitted_compactions`. Permit that shortcut only for a batch
at the frontier, and schedule a newer non-trivial batch through the worker path.
Otherwise, a trivial move can skip older L0 inputs despite the frontier loop.

Persist the manifest before terminal job states in `.compactions`, as today.
The writer/compactor manifest merge algorithm stays unchanged. The frontier
loop controls which results that algorithm receives and keeps its single
watermark valid. Both existing `last_compacted_l0_*` fields still advance
through the normal finish path.

### Failure, Restart, and Recovery

Worker errors and stale claims return the same job to `Scheduled`. Preserve
its source list and destination sorted-run ID. For example, replacing
`A -> SR(10)` with `A -> SR(12)` while B retains `SR(11)` puts older A ahead
of newer B after commit. Retry A with its original destination instead.

On restart or refresh, first retire stale results whose inputs already left
the manifest. Then reconstruct the accepted lane from `Scheduled`, `Running`,
and `Compacted` jobs and the remaining L0 order. Admit pending submissions
against that lane. Source order, rather than job ULID order, determines the
frontier.

If an accepted lane loses an older batch while its inputs remain in L0,
the coordinator must fail its dependent newer batches before planning replacements.
Their outputs become GC candidates. Already committed outputs remain valid.
This recovery rule also handles gaps or reversed destinations in a malformed
accepted lane.

If a crash precedes the manifest write, the completed batches remain eligible
for ordered commit. If it follows that write but precedes the job-state write,
existing stale-result handling marks the affected records `Failed`.
Their absent inputs must not cause recovery to cancel the remaining valid lane.
Keep local terminal records through persistence retries, as current main does,
so stale worker updates cannot restore cancelled jobs.

Waiting `Compacted` jobs remain active in `.compactions`. The existing GC
cutoff therefore protects their output SSTs. Current main also enforces the
RFC-0029 rule that output SST timestamps cannot precede their job timestamp.
Their input views stay in the manifest until commit reaches them. No new
retention record is needed.

### Interactions with Other Compactions

- L0+sorted-run compactions and segment drains remain serialized with an active
  L0 lane in that segment. A drain conflicts even if its own inputs contain
  only sorted runs.
- Sorted-run-only compactions can continue when source and destination rules
  permit them. They must preserve sorted-run precedence and cannot consume or
  overwrite a pending destination.
- New flushes prepend L0 views. They do not change the reserved suffix or its
  commit order. Existing manifest invariants and GC protections still apply.

## Impact Analysis

SlateDB features and components that this RFC interacts with:

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics

### Consistency, Isolation, and Multi-Versioning

- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format and merge protocol
- [ ] Checkpoints
- [ ] Garbage collection

### Compaction

- [x] Compaction state persistence
- [x] Compaction strategies
- [ ] Distributed compaction
- [x] Compactions format/protocol

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Performance and cost 

Users deciding to opt in to this feature should expect to see throughput increase in hot segments for L0 compactions which can also mitigate backpressure triggers during write heavy workloads. Since parallelizing L0+SR compactions is out of scope for this RFC, it is not expected that backpressure triggers would be eliminated completely, but instead cut down in occurrence.

Since compaction workers would have more work to claim at any point in time, the user may incur heaftier cost-per-unit-time than running L0 compactions serially within a segment. For this reason, the option is left to the user and should not cause increase in cost-per-unit-time if opting out via `max_concurrent_l0_compactions_per_segment=1`.

## Operations

Parallel execution uses more worker capacity and can produce more sorted runs
and temporary output SSTs. A slow oldest batch delays commits from newer
batches, although their execution overlaps. Later sorted-run compactions merge
the outputs and remove delete markers when safe.

Track the number of completed batches waiting to commit and their wait time,
alongside existing throughput and L0 backlog metrics. Logs must identify the
segment, source views, destination sorted-run ID, and blocking predecessor.
Jobs remain independently claimable. Multiple machines need worker capacity
limits that leave jobs available for peers.

The formats remain compatible, but the coordination rules change. An old
coordinator can commit batches out of order. An old worker can drop required
delete markers. Keep fan-out disabled until both support this RFC.

## Testing

The tests must cover the design boundaries:

- Admit disjoint suffix batches in age order, preserve scheduling fairness,
  and enforce both capacity limits, including waiting `Compacted` jobs.
- Force newer jobs to finish first, including trivial moves, and compare
  reads, scans, and snapshots after each commit with serial execution.
- Exercise deletes, TTL, merge operands, and compaction filters across batch
  boundaries, including the deleted-value example above.
- Inject worker failures, cancellation, coordinator restarts, concurrent flushes,
  and crashes around both metadata writes. Make sure that GC protects waiting outputs.
- Measure one hot segment with one, two, and four workers.

## Rollout

Ship the admission, frontier, and worker changes with the per-segment limit
at one. Upgrade all coordinators and workers before enabling parallel batches.
Start with a limit of two and measure throughput, L0 backlog, and commit waits.

Before a downgrade, return the limit to one and let every existing multi-batch
lane finish. Lowering the limit alone does not make pending parallel jobs safe
for older binaries.

## Alternatives

- Keep serial L0 compaction or use only local subcompactions. Neither uses
  multiple machines for a single hot segment.
- Commit a whole group atomically after every batch finishes. This delays all
  L0 relief behind the slowest batch.
- Persist a lane ID and batch ordinal. Existing source lists and destination
  IDs suffice for this scope. Explicit metadata can support more complex
  dependencies in future designs.
- Adopt Pebble-style L0 sublevels and flush splitting for independent key-range
  compactions. This approach can fit the coordinator/worker architecture, but
  its inputs need not form a contiguous suffix in L0 age order. Independent
  commits therefore require a larger watermark redesign to track individual
  removals without dropping unrelated inputs. Sublevels alone do not require
  this redesign. Supporting those commits changes writer/compactor manifest
  merging, recovery, and the associated GC protections. This broader
  change to carries more implementation risk for SlateDB than ordered batches,
  which preserve the existing watermark model.

## Open Questions

- Do Pebble-style key-range compactions improve throughput or read amplification
  enough over ordered batches to justify the larger watermark redesign and its
  implementation risk? Compactinh contiguous suffixes of l0 are not without implementation risk either e.g. head of line blocking.

## References

- [RFC-0013: Compaction State Persistence](./0013-compaction-state-persistence.md)
- [RFC-0024: Segment-Oriented Compaction](./0024-segment-oriented-compaction.md)
- [RFC-0025: Distributed Compaction](./0025-distributed-compaction.md)
- [RFC-0028: Subcompactions](./0028-subcompactions.md)
- [RFC-0029: GC-Safe SST ULID Timestamps](./0029-gc-safe-sst-ulid-timestamps.md)
- [Pebble L0 sublevels discussion](https://github.com/cockroachdb/pebble/issues/609)
