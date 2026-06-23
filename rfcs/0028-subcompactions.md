# Subcompactions

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Example Subcompaction](#example-subcompaction)
  - [Execution Lifecycle](#execution-lifecycle)
  - [Persistence & Schema](#persistence--schema)
  - [Boundary Selection (Heuristic)](#boundary-selection-heuristic)
  - [Configuration](#configuration)
  - [Compaction Filters](#compaction-filters)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
  - [Performance & Cost](#performance--cost)
  - [Observability](#observability)
  - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Accepted

Authors:

- [Almog Gavra](https://www.github.com/agavra)

## Summary

This RFC proposes subcompactions, compactions over non-overlapping sub-ranges
of the key space, persisted durably as part of a logical parent compaction.
Partitioning a logical compaction into subcompactions lets us execute the
sub-ranges in parallel while still still resuming a compaction at subcompaction
granularity after a failure.

## Motivation

SlateDB's current compaction throughput is bottlenecked on CPU with no ability to
concurrently execute compactions. On some [production
workloads](https://discord.com/channels/1232385660460204122/1491155190760476844),
users found Slate compaction limited to processing 20-30 MB/s. This is
especially problematic in a default size-tiered compaction world where large
sorted runs can span dozens to hundreds of gigabytes.

Note that this is complementary to [distributed
compactions](https://github.com/slatedb/slatedb/blob/main/rfcs/0025-distributed-compaction.md).
Distributed compactions allow concurrent compactions to execute on multiple workers
while subcompactions allow an individual worker to parallelize execution of a single
compaction. We can eventually extend SlateDB to allow subcompactions to execute
on separate workers within the distributed worker framework, but that is out of
scope for this RFC.

## Goals

- Parallelize a single logical compaction across multiple key ranges.
- Define subcompaction boundaries aligned with SSTs
- Support persisting subcompactions so that range-level resume is possible.
- Keep backward compatibility with existing `.compactions` files.

## Non-Goals

- Distributed execution of subcompactions

## Design

This RFC proposes a new concept to SlateDB: the **subcompaction**. While a
compaction necessarily covers the entire key range and outputs a complete
`SortedRun`, a subcompaction only covers a partial key range. Subcompactions
are only valid within the context of a parent compaction.

### Example Subcompaction

I find it's easiest to understand the concept by looking at a simple, but complete,
example. This example compacts a single sorted run (e.g. just one input SR, which
can happen if you want to periodically compact the base run). 

In this case, the input sorted run has 3 SSTs that cover the full key range: 91,
92 & 93. The subcompaction scheduler (heuristic discussed later) decided to split this
into two sub-ranges of the full keyspace covering `(-∞, k)` and `[k, +∞)`

```ascii
 ┌───────────────────input: SR(9)───────────────────┐ 
 ├SST(91)─────────┬SST(92)─────────┬SST(93)─────────┤ 
 │████████████████│▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒│ 
 ├────────────────┴───────┬────────┴────────────────┤  
┌┤    subcompaction A     │     subcompaction B     ├┐
│└─ ─ ─ ─ (-∞,k)─ ─ ─ ─ ─ ┴─ ─ ─ ─ ─(k,+∞)─ ─ ─ ─ ─ ┘│
│                                                    │
│                                                    │
│┌SST(93)─────────┬SST(94)┬SST(95)──────────┬SST(96)┐│
└▶████████████████│▓▓▓▓▓▓▓│▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▒│▒▒▒▒▒▒▒◀┘
 ├────────────────┴───────┴─────────────────┴───────┤ 
 └──────────────────output: SR(10)──────────────────┘ 
```

This example will run two sub-compactions:

1. Input SST(91), SST(92) with view range covering keys `(-∞, k)`
2. Input SST(92), SST(93) with view range covering keys `[k, +∞)`

Each sub-compaction will output SSTs that cover the range that was assigned to
it. In this case they will each need to output two SSTs to respect the max sst
size configuration. The parent compaction will then update the manifest with the
result of the compaction, indicating that the new sorted run `SR(10)` has all
four SSTs.

**Note 1**: this demonstrates a limitation of sub-compaction which is that it may
generate more SSTs than required, with multiple having a size less than the max
SST configuration.

**Note 2**: the heuristic proposed later in this RFC can produce a split like this
one because it places boundaries at SST *block* boundaries, not just between SSTs.
It would not cut at an arbitrary key `k`, though — it snaps the boundary to the
nearest block boundary inside SST(92).

### Execution Lifecycle

The below pseudocode outlines the entire subcompaction lifecycle. The key point
is that it builds on top of three existing primitives: (a) the existing compaction
logic runs nearly as-is within the scope of a subcompaction, (b) it reuses the
SST view concept with sst.overlaps() to handle shared-input SSTs to ranges and (c)
it persists the output of subcompactions to the .compactions file that now includes
subcompactions (see spec in the next section).

```
// executes a full compaction with subcompactions
execute_compaction(spec):

  // the state contains information on the current progress of subcompactions,
  // so that we can resume interrupted/failed ones from their last successful
  // state write
  state := fetch(000X.compactions)

  if state.subs does not exist:
    subs := build_subcompactions(spec)
    persist subs in 000X.compactions

  // we can parallelize the subcompactions up to the max configured parallelism
  #parallel(max_concurrent_subcompactions)
  for sub in state.subs.incomplete():
    run_subcompaction(spec, sub)

  if any subcompaction failed:
    mark parent compaction failed
    return

  // the final construction of the Sorted Run is just apply metadata and marking
  // the state as compacted
  result_ssts = concatenate sub.output_ssts in range order
  sr = to_sr(result_ssts)
  mark parent compaction compacted

  // only the parent compaction job will ever attempt to update the manifest (via
  // the coordinator as outlined in RFC-25)
  update_manifest(sr)
  mark parent compaction completed

// runs a subcompaction by applying the subcompaction range to any input SSTs
// and then merging them using the existing compaction path to build output
// SSTs
run_subcompaction(spec, sub):
  mark subcompaction running

  views = []
  for sst in spec.input_ssts:
    if sst.overlaps(sub.range):
      views += sst.range(sub.range)

  writer = Writer::new
  for key in merge(views):
    writer.add(key)
    if writer.sst_full():
      // this goes directly to object storage
      persist SST
      // this goes over an rx/tx channel to the parent compaction, which handles
      // persisting progress to the .compactions file to avoid too much contention
      persist progress
  
  persist subcompaction completed with result_ssts
```

It's worth noting that the main coordinator is still responsible for submitting
the compaction, but the worker is responsible for computing the subcompaction
plan (splits). The planner reads the input SST indexes to choose split points
(see [Boundary Selection](#boundary-selection-heuristic)); doing this on the
worker rather than the coordinator keeps those index reads off the active
serving node's cache in distributed compaction.

### Persistence & Schema

In order to support resuming partial compactions we need to have a durable model
for subcompactions. This reflects the existing Compactions model closely, relying
on `output_ssts` and `status` to track how far along the subcompaction is:

```fbs
table Compaction {
    id: Ulid (required);
    spec: CompactionSpec (required);
    status: CompactionStatus;
    output_ssts: [CompactedSsTable]; // will be empty if running subcompactions
    worker: WorkerSpec;
    subcompactions: [Subcompaction]; // <---- NEW
}

table Subcompaction {
    range: BytesRange (required);
    status: CompactionStatus;
    output_ssts: [CompactedSsTable];
}
```

Resuming subcompactions works roughly the same way that resuming compactions does today.
When we resume a subcompaction, we check the `output_ssts` and seek the merge iterator
to the last key of the last sst in that list.

**Note on GC**: subcompaction output SSTs are protected by the existing compaction low-watermark.
The parent compaction stays in the in-flight set until every subcompaction completes, and the
output SSTs carry ULIDs newer than the parent's id, so they sit above the watermark and cannot be
collected. The [boundary file](https://github.com/slatedb/slatedb/pull/1635) plays only its usual
role of keeping old `.compactions` files around long enough to compute that watermark.

### Boundary Selection (Heuristic)

The goal of boundary selection is to split a compaction into roughly equally
sized sub-ranges, measured in bytes. 

The boundary selection algorithm uses candidates that consist of "anchor points"
within individual SSTs. These points are sampled from the SST index (as opposed
to using every block offset as a potential anchor point) to reduce the number
of candidates.

We then collect the sizes between anchor points until they exceed the threshold
we want for a single subcompaction and mark those as the boundary range.

```
// Per input SST, read its index and sample it down to at most
// MAX_ANCHORS_PER_SST anchors: (key, approx_bytes)
anchors = merge_by_key(sst.index.sampled_anchors(MAX_ANCHORS_PER_SST) for sst in inputs)

// floor each range at the largest single input SST's estimated size, so a
// subcompaction is never smaller than the biggest source SST
min_range_bytes = max(sst.estimate_size() for sst in inputs)

target = max(total_input_bytes / max_subcompactions, min_range_bytes)
if target >= total_input_bytes:
  return [(-∞, +∞)]

boundaries = []
threshold = target
cumulative = 0
for (key, approx_bytes) in anchors:
  cumulative += approx_bytes
  // the anchor that tips the running total past the next threshold ends the
  // current range and starts the next; a running threshold avoids drift
  if cumulative > threshold and len(boundaries) < max_subcompactions - 1:
    boundaries.append(key)
    threshold += target

return covering_ranges(boundaries)         // (-∞,b1),[b1,b2),...,[bk,+∞)
```

When `max_subcompactions <= 1`, or the largest input SST already covers the whole
compaction (so the floor meets the total), or no anchor ever crosses the
threshold, the sweep emits no boundaries and the compaction runs as a single
unbounded range.

### Configuration

- `max_subcompactions` configures the maximum number of subcompactions for a compaction, with
  any number `<=1` disabling subcompactions. Default `4`. The planner targets ranges of
  `max(total_input_bytes / max_subcompactions, max_input_sst_bytes)`, where `max_input_sst_bytes`
  is the largest single input SST's estimated on-disk size. A compaction whose largest input
  SST already accounts for most of the total is split into fewer (or zero) ranges rather than
  fragmented into undersized SSTs.

There is deliberately no separate minimum-input knob: the floor baked into the subcompaction
to make sure the output SSTs are no smaller than the largest input SST.

We could optionally introduce `max_concurrent_subcompactions` to allow scheduling more
subcompactions than we run concurrently actively, but until we decide to integrate this
with the distributed compaction worker framework I think it's better to delay that.

### Compaction Filters

Each subcompaction creates its own `CompactionFilter` via the supplier and invokes
`on_compaction_end` once when its sub-range is exhausted, so a logical compaction now fires
`on_compaction_end` once per subcompaction (concurrently) rather than once overall. This is
safe for the documented use cases of the hook—flushing state, logging statistics, or cleaning
up resources—since each is naturally scoped to the entries that instance observed, and it
generalizes the existing resume contract where a resumed compaction already builds a fresh
filter that observes only a partial key stream. Filters that instead need to aggregate across
the full keyspace must do so via shared state on the `CompactionFilterSupplier`.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [x] Time to live (TTL)
- [x] Compaction filters
- [x] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [x] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection
- [x] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence
- [x] Compaction filters
- [x] Compaction strategies
- [x] Distributed compaction
- [x] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

This is discussed in the main body of the RFC as it's primarily a performance
optimization. It enables better utilization of CPU at the cost of potential
SST fragmentation at subcompaction boundaries.

### Observability

| Metric | Type | Measures |
|---|---|---|
| `slatedb.compactor.running_subcompactions` | up/down counter | Subcompactions executing concurrently right now. |
| `slatedb.compactor.subcompactions_per_compaction` | histogram | Number of subcompactions the heuristic produced per parent compaction. |
| `slatedb.compactor.subcompaction_duration_sec` | histogram | Per-range wall-clock time from start to output SSTs persisted. |
| `slatedb.compactor.subcompactions_resumed` | counter | Subcompactions resumed from persisted state rather than started fresh. |

In place of introducing more throughput metrics, the existing aggregate
`slatedb.compactor.bytes_compacted` and `slatedb.compactor.total_throughput_bytes_per_sec` 
metrics remain authoritative for overall throughput.

### Compatibility

- **Wire format**: `subcompactions` is appended to the end of the `Compaction`
  table, so the change is forward/backward compatible. Old readers skip the
  field; new readers see an empty vector on pre-existing `.compactions` files
  and run the compaction without subcompactions.
- **Rollback**: a pre-subcompaction binary ignores the `subcompactions` field,
  sees empty `output_ssts`, and re-runs the whole compaction single-threaded.
  This is correct but abandons any already-written subcompaction output SSTs.
  Those orphans are never referenced by the manifest and are reclaimed by GC
  (see below). No manual cleanup is required; rolling back only wastes in-flight
  work.
- **Garbage collection**: subcompaction output SSTs are written before the
  manifest commit but carry ULIDs newer than the parent compaction's id. The
  parent stays in the in-flight set until every subcompaction completes, pinning
  the compaction low-watermark above those SSTs, so the existing GC protection
  covers them with no version-coupled change (even when GC is running on an
  older code version).

## Testing

The testing plan beyond the usual set of work includes running
`slatedb/src/compaction_execute_bench.rs` to ensure that subcompactions will, in
practice, improve the throughput of compactions. We could consider modeling this
with Fizzbee, but I don't think it's necessary.

## Rollout

I opted to default `max_subcompactions = 4` instead of off-by-default because I
think it's a pretty foundational feature for large compaction jobs. Note that
this departs from prior art: RocksDB ships with it disabled
(`max_subcompactions = 1`). Like RocksDB, we expose no dedicated minimum-size
knob — the per-range floor (the largest input SST's worth of data) keeps small
compactions from fragmenting, playing the role RocksDB's `MaxFileSizeForLevel`
plays for it.

See the compatibility section for why this is safe to rollout in one go.

## Alternatives

**L0 sublevels (Pebble's approach).** Rejected. Pebble does not split a single
logical compaction across key ranges; it parallelizes by running multiple
independent compactions at once, gated by `MaxConcurrentCompactions`. It makes
this possible with L0 sublevels and flush splitting, which carve the keyspace
into non-overlapping ranges so several L0->Lbase compactions can proceed without
conflicting.

This works for Pebble because it is a leveled engine where the parallelism
opportunity lives at the L0->Lbase boundary. It does not help the case this RFC
targets: under size-tiered compaction a single large sorted-run compaction is
still one indivisible unit of work on one core, and no amount of
cross-compaction concurrency speeds it up. Subcompactions parallelize *within*
that unit, which is the only approach that addresses big SR compactions.

**Manifest-only boundaries.** Rejected. An earlier draft restricted split
points to the per-SST first/last keys in the manifest, avoiding index reads
entirely. It is cheaper to plan, but does not work for compactions where data
is skewed within the SST boundaries. The index-based heuristic in [Boundary
Selection](#boundary-selection-heuristic) keeps this as a coarse fallback (one
"block" per SST) for inputs whose indexes we choose not to read.

As some evidence that the cost of index-based planning is not prohibitive, 
a 16GB compaction had the following time breakdowns for the subcompaciton phase
(ran on a `cg8in.8xlarge`) where planning takes only about 3% of the total time
after splitting it 12 ways:
```
┌───────────────────────────────────┬────────┬──────────┬─────────┬─────────┬────────────┐
│                run                │ total  │ manifest │  plan   │  merge  │ planning % │
├───────────────────────────────────┼────────┼──────────┼─────────┼─────────┼────────────┤
│ tiled sub=1 (uncompressed)        │ 157 s  │ 50 ms    │ 0       │ ~157 s  │ ~0%        │
├───────────────────────────────────┼────────┼──────────┼─────────┼─────────┼────────────┤
│ tiled sub=12 (zstd)               │ 20.2 s │ 50 ms    │ ~0.55 s │ ~19.5 s │ ~3%        │
└───────────────────────────────────┴────────┴──────────┴─────────┴─────────┴────────────┘
```

**Sub-block split precision.** Deferred. Boundary selection splits at SST block
granularity (~4 KiB), which is already fine enough for roughly-equal ranges.
Going finer — splitting within a block at restart points, or sampling actual
keys to balance skewed value sizes — is possible but not worth the added I/O and
complexity now; deferred to a later iteration.

**Parallelizing block construction within a single range.** Deferred/Partial
Rejection. This is a valid alternative to the RFC in that it allows you to have
more threads handling the compaction. To be honest, I think it's hard to tell
whether this is a better alternative without implementing it. It is likely to be
more complicated to implement, and also parallelize less of the overall
compaction workload. In addition, it makes the resuming mechanism a little more
challenging as we need to synchronize the production of sub-ranges which may
cause lagging threads to serialize compaction. I'm inclined to reject.

It is, in addition, not exclusive with the proposal in this RFC and if we wanted
to optimize a single sub-compaction we could also apply parallelism within a
single subcompaction (or a top level compaction, which shares the same logic).

## Open Questions


## References

- [RFC-0002: SlateDB Compaction](0002-compaction.md)
- [RFC-0013: Compaction State Persistence](0013-compaction-state-persistence.md)
- [RFC-0025: Distributed Compaction](0025-distributed-compaction.md)
- RocksDB subcompaction design and implementation:
  - [RocksDB Subcompaction wiki](https://github.com/facebook/rocksdb/wiki/Subcompaction)
  - [RocksDB Compaction wiki](https://github.com/facebook/rocksdb/wiki/Compaction)
  - [RocksDB `CompactionJob::GenSubcompactionBoundaries` and `RunSubcompactions`](https://github.com/facebook/rocksdb/blob/main/db/compaction/compaction_job.cc)
  - [RocksDB `SubcompactionState`](https://github.com/facebook/rocksdb/blob/main/db/compaction/subcompaction_state.h)
- Pebble concurrent compactions (key-range splitting + L0 sublevels rather than subcompactions):
  - [Pebble concurrent compactions write-up](https://mufeezamjad.com/blog/pebble-concurrent-compactions)
  - [Pebble: introduce sublevels in L0 (cockroachdb/pebble#609)](https://github.com/cockroachdb/pebble/issues/609)
  - [Pebble `compaction.go`](https://github.com/cockroachdb/pebble/blob/master/compaction.go)
