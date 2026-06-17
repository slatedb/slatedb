# SlateDB RFC: Batched Point Reads (`multi_get`)

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Public API](#public-api)
  - [Semantics](#semantics)
  - [Read orchestration](#read-orchestration)
  - [Batched per-SST reads](#batched-per-sst-reads)
  - [Precedence by rank instead of a sequential layer walk](#precedence-by-rank-instead-of-a-sequential-layer-walk)
  - [Value resolution](#value-resolution)
  - [Concurrency control](#concurrency-control)
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

* [Roman Grebennikov](https://github.com/shuttie)

## Summary

This RFC adds a batched point-read API, `multi_get`, to `Db`, `DbReader`,
`DbSnapshot`, and `DbTransaction`. A batch of keys is resolved against a single
consistent snapshot in one pass that visits each candidate SST exactly once for
all of its keys, and fans every SST read out concurrently — across L0 and all
sorted runs — instead of walking layers sequentially. Compared to a loop of
`get` calls, this removes repeated index/filter loads, coalesces block fetches
within each SST, and collapses end-to-end latency to roughly one object-store
round trip per batch regardless of LSM depth.

## Motivation

Point lookups against object storage are dominated by round-trip latency.
Applications that need N keys today must issue N independent `get` calls, which
costs:

1. **Repeated metadata I/O.** Each `get` loads the SST index and bloom filters
   of every SST it probes, even when consecutive gets probe the same SSTs.
   These are cacheable, but cold reads and cache pressure both scale with N.
2. **No block coalescing.** Two keys that live in adjacent blocks of the same
   SST are fetched as two object-store GETs instead of one ranged GET.
3. **Serialized latency.** Even if the application issues gets concurrently,
   each individual get still walks the LSM newest-first: L0, then sorted run 0,
   then run 1, … — one dependent round trip per layer until the key is found.
4. **No deduplication or shared snapshot.** Each get re-establishes its
   visibility bounds, and duplicate keys in the workload resolve repeatedly.

A microbenchmark of 1,000 keys spread over ~16 L0 SSTs (in-memory object store,
no block cache) shows a `get` loop at ~10.5 ms versus `multi_get` at ~3.5 ms —
a 3× improvement that comes purely from items (1) and (2); against a real
object store, item (3) dominates and the gap widens with the number of sorted
runs.

## Goals

- A `multi_get` API whose result is **bit-for-bit equivalent** to calling `get`
  for each key against the same snapshot (including tombstones, TTL, merge
  operands, and `ReadOptions` semantics).
- All keys in a batch observe **one consistent snapshot**.
- Visit each candidate SST **once per batch**, loading its index and filters
  once and coalescing its block fetches.
- Batch latency of approximately **one object-store round trip**, independent
  of the number of sorted runs.
- Support on all read surfaces: `Db`, `DbReader`, `DbSnapshot`,
  `DbTransaction` (including reads-through-write-batch and SSI conflict
  tracking).

## Non-Goals

- A streaming or unordered-result API (results are returned positionally, in
  input order).
- Batched writes (already covered by `WriteBatch`).
- Cross-batch caching or memoization beyond the existing block/object caches.
- Changing the on-disk format, manifest, or compaction in any way.

## Design

### Public API

Four methods are added to the `DbReadOps` trait (and therefore to every read
surface), mirroring the existing `get` family:

```rust
async fn multi_get<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K],
) -> Result<Vec<Option<Bytes>>, Error>;

async fn multi_get_with_options<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K], options: &ReadOptions,
) -> Result<Vec<Option<Bytes>>, Error>;

async fn multi_get_key_value<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K],
) -> Result<Vec<Option<KeyValue>>, Error>;

async fn multi_get_key_value_with_options<K: AsRef<[u8]> + Send + Sync>(
    &self, keys: &[K], options: &ReadOptions,
) -> Result<Vec<Option<KeyValue>>, Error>;
```

### Semantics

- One result slot per input key, in input order. `None` marks a key that is
  missing, deleted, or expired.
- Duplicate input keys are deduplicated for I/O and resolved once; the result
  is scattered back to every input position.
- The whole batch reads one snapshot: visibility bounds (`max_seq`, durability
  filter, dirty reads) are computed once up front, exactly as `get` computes
  them per call.
- On `DbTransaction`, the transaction's uncommitted write batch takes
  precedence over the database, and under SSI every key in the batch is added
  to the transaction's read set (each key is tracked as a point range, the same
  as a single `get`).

### Read orchestration

The orchestrator (`Reader::multi_get_with_options` in `reader.rs`) proceeds in
four phases. Phases 1–2 are in-memory and free; phase 3 is the only one that
performs I/O.

```
keys ──dedup──▶ unique keys
                   │
  1. write batch (transactions only)        ─┐  resolve keys early:
  2. memtable + immutable memtables          ─┘  no disk I/O for keys found here
                   │ still-pending keys
  3. fan out: every (SST view, keys) pair across all trees & layers,
     one bounded concurrent batch ──▶ per-SST results, tagged with rank
                   │ sort by rank, apply newest-first per key
  4. per key: replay accumulated versions through GetIterator
     (+ MergeOperatorIterator) ──▶ Option<RowEntry> ──scatter──▶ results
```

Each unique key has an accumulator of `RowEntry` versions (newest-first) and a
`resolved` flag. A key is *resolved* when a `Value` or `Tombstone` visible at
the snapshot has been appended; `Merge` operands do **not** resolve a key,
because operands legitimately span layers and the resolution must keep
descending until it finds the base value. Entries with `seq > max_seq` are
filtered *before* the resolve check, so a too-new value can never hide an older
visible one.

Keys resolved by the write batch or memtables never generate any disk work.

For segmented databases, pending keys are grouped by the LSM tree that covers
them (via `select_segments`); a key that no segment covers has no on-disk data
and is skipped. Trees cover disjoint key ranges, which the rank merge below
relies on.

### Batched per-SST reads

`multi_sst::read_sst_for_keys` visits one SST exactly once for a set of keys:

1. **Load metadata once**: the SST index and bloom filters are read once for
   the whole key set (vs. once per key in a `get` loop).
2. **Prune**: each key is dropped if it falls outside the view's visible range
   (segment/clone projections) or if any bloom filter rejects it. Filter
   positive/negative/false-positive stats are recorded with the same meaning as
   the single-key path.
3. **Plan and coalesce blocks**: surviving keys map to block ranges via the
   same `partitions_covering_range` logic as `get` (a key's versions can span a
   block boundary). The union of all needed blocks is coalesced into contiguous
   fetch runs, tolerating up to 2 intervening unwanted blocks per run
   (`COALESCE_GAP_BLOCKS`): reading a couple of extra ~4 KiB blocks is cheaper
   than another object-store round trip. Fetches go through the existing
   `read_blocks_using_index`, which already handles the block cache, range
   coalescing of uncached spans, and parallel decode.
4. **Scan**: each key's blocks are scanned with a `DataBlockIterator`,
   collecting that key's raw `RowEntry` versions newest-first. No
   sequence/merge/tombstone resolution happens here — the raw entries are
   returned to the orchestrator so resolution stays in one place.

### Precedence by rank instead of a sequential layer walk

A single-key `get` reads layers newest-first and stops at the first hit, which
is I/O-minimal but serializes round trips: a database with R sorted runs costs
up to 1 + R dependent round trips. An earlier iteration of this design kept
that structure for batches (read L0, shrink the pending set, read run 0,
shrink, …) and inherited the same latency profile.

Instead, the orchestrator builds **one flat work list** of `(rank, sst_view,
pending_keys)` items covering every SST that might hold a pending key — all L0
SSTs and, for each sorted run, the specific views covering each key (found by
binary search) — and executes the entire list as a single bounded-concurrency
batch. Newest-first precedence is restored *after* the I/O completes:

- `rank` encodes precedence within a tree: L0 SSTs by position (front =
  newest), then sorted runs in manifest order, then views within a run by
  ascending index (a key's versions may span adjacent views; the single-key
  path reads them in the same order). Rank offsets advance by each run's full
  view count so two runs never alias.
- Ranks from different trees may collide, but trees cover disjoint keys, so any
  given key's results all carry ranks from one tree and a single global sort
  applies them correctly.
- Results are sorted by rank and applied per key in order: versions are
  appended until the first visible `Value`/`Tombstone`, after which that key's
  older-rank results are dropped — reproducing exactly what the sequential walk
  would have kept.

The trade-off is **speculative I/O for latency**: an older run is probed even
for keys that turn out to live in a newer layer. The waste is bounded — the
speculative probe costs an index + filter load (small, aggressively cached),
and the bloom filter then prunes the block fetch for almost all such keys
(false-positive rate ~1% at default settings). In exchange, batch latency is
one round trip instead of one per run. Section [Alternatives](#alternatives)
discusses the sequential variant.

### Value resolution

The final value for each key is *not* computed by reimplementing
tombstone/merge/TTL logic. The key's accumulated versions are replayed through
a `VecRowIterator` feeding the same `GetIterator` +
`MergeOperatorIterator`/`MergeOperatorRequiredIterator` stack the single-key
`get` path uses. Equivalence with `get` is therefore structural, not
coincidental: both paths share one resolution implementation, and the batch
path only changes *how the versions are fetched*.

### Concurrency control

All SST reads across the batch share one concurrency bound
(`MAX_CONCURRENT_SST_READS = 32`, via the existing `build_concurrent` helper).
Reads are object-store-I/O bound, so a generous bound pays off; the constant
caps worst-case in-flight requests per `multi_get` call regardless of batch
size or LSM shape.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [x] Transactions
- [x] Snapshots
- [x] Sequence numbers

### Time, Retention, and Derived State

- [x] Time to live (TTL)
- [ ] Compaction filters
- [x] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [x] Clones
- [ ] Garbage collection
- [x] Database splitting and merging
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
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- **Latency**: ~1 object-store round trip per batch (plus parallel block
  fetches), versus up to `1 + sorted_runs` dependent round trips per key for a
  `get` loop. Microbenchmark (1k keys, ~16 L0 SSTs, in-memory store, no cache):
  3.5 ms vs 10.5 ms.
- **GET request count**: strictly fewer than an equivalent `get` loop for
  metadata (index/filters loaded once per SST per batch) and usually fewer for
  data (block coalescing). The fan-out adds speculative index/filter loads in
  older runs for keys resolved in newer layers; these are small and cached, and
  bloom filters suppress the corresponding block GETs except on false
  positives.
- **Read amplification**: block coalescing may fetch up to `COALESCE_GAP_BLOCKS`
  unwanted blocks between two wanted blocks in the same SST — bytes traded
  for round trips, the same trade `read_blocks_using_index` already makes.
- **Memory**: O(batch size) accumulators of raw `RowEntry`s; bounded per key by
  early termination at the first visible base value.

### Observability

- No configuration changes; `MAX_CONCURRENT_SST_READS` and
  `COALESCE_GAP_BLOCKS` are internal constants (see Open Questions).
- Existing metrics are reused: `get_requests` increments by batch size;
  `sst_filter_point_{positives,negatives,false_positives}` are recorded by the
  batched path with unchanged meaning. Note that the fan-out increases filter
  *negative* counts relative to a `get` loop (speculative probes of older runs
  are pruned by filters) — dashboards tracking filter hit ratios will see a
  shift, not a regression.

### Compatibility

- No on-disk or manifest format changes; works on existing databases.
- Purely additive API (new trait methods with default-option wrappers);
  existing code is unaffected. Bindings can adopt the API incrementally.
- No mixed-version concerns: behavior is reader-local.

## Testing

- **Unit tests** (`reader.rs`): layered scenarios built from a synthetic DB
  state (write batch / memtable / L0 / sorted runs), covering distinct keys
  across layers, duplicates, tombstones, merge operands spanning layers, a
  newer-run base shadowing an older-run stale value under fan-out, sequence
  filtering (`max_seq` must not let a too-new value resolve a key), and empty
  batches. Every unit scenario also asserts the **differential invariant**:
  `multi_get` result == per-key `get` against the same state.
- **Integration tests** (`tests/multi_get.rs`): end-to-end differential checks
  against a real `Db` with and without block cache and merge operator;
  `DbReader`, `DbSnapshot`, and transaction coverage; SSI test asserting all
  batch keys enter the read set (a conflicting concurrent write to *any* batch
  key aborts the transaction).
- **Performance tests**: `benches/db_operations.rs` compares `multi_get` vs an
  equivalent `get` loop (1k keys over many small L0 SSTs, no cache).

## Rollout

- Single PR; no feature flag needed (additive API, no behavior change for
  existing calls).
- Docs updates: API docs on the `DbReadOps` trait methods (included).
- Follow-up: expose `multi_get` in language bindings (Go/Python) once the Rust
  API stabilizes.

## Alternatives

- **Status quo (loop of `get`s)**: simplest, but pays per-key metadata loads,
  no block coalescing, and per-key sequential layer walks. The 3× benchmark gap
  on a cache-less L0-heavy database understates the win against real object
  stores with many sorted runs.
- **Concurrent `get` loop (client-side `join_all`)**: removes wall-clock
  serialization *across* keys but keeps every other inefficiency, plus N×
  snapshot setup. Doesn't require any engine change, but also can't dedupe SST
  visits.
- **Sequential newest-first layer walk with early exit** (the initial
  implementation of this feature): groups keys per SST within one layer, reads
  layers in order, and shrinks the pending set between layers. I/O-minimal — a
  key found in L0 never touches sorted runs — but latency scales with the
  number of sorted runs (dependent round trips), and the bookkeeping (per-tree
  local/global index remapping, per-layer barriers) was the most complex code
  in the feature. Rejected in favor of fan-out: on object storage, round trips
  dominate and bloom filters make the speculative I/O cheap. A future hybrid
  (fan out filters, sequence block reads) is possible if the speculative reads
  ever matter; see Open Questions.
- **Reusing `SstIterator` per key instead of `read_sst_for_keys`**: would avoid
  the new `multi_sst` module but reloads the index per key and cannot coalesce
  blocks across keys — i.e., it forfeits most of the batching benefit inside an
  SST.
- **Reimplementing tombstone/merge/TTL resolution in the batch path**: would
  avoid materializing per-key version vectors, but creates a second copy of the
  trickiest read-path semantics that must be kept in lockstep with `get`.
  Rejected; the batch path deliberately replays versions through the single-key
  resolution stack.

## Open Questions

- Should `MAX_CONCURRENT_SST_READS` (and possibly `COALESCE_GAP_BLOCKS`) be
  exposed in `Settings`, or derived from the object-store profile?
- Is a hybrid mode worthwhile for very read-amplification-sensitive deployments:
  fan out index/filter loads (cheap), then issue block reads newest-first with
  early exit (one extra round trip in the rare false-positive case)?
- Should bindings expose `multi_get` as soon as this merges, or after the Rust
  API has soaked for a release?

## References

- `slatedb/src/reader.rs` — orchestrator (`multi_get_with_options`,
  `build_sst_work`, rank merge, resolution).
- `slatedb/src/multi_sst.rs` — batched per-SST point reads.
- `slatedb/src/ops.rs` — `DbReadOps` API surface.
- `slatedb/tests/multi_get.rs` — differential and SSI integration tests.
- RocksDB `MultiGet` — prior art for batched point reads in an LSM
  (per-level batching with filter-first pruning):
  https://github.com/facebook/rocksdb/wiki/MultiGet-Performance
- [RFC 0006: Merge Operator](0006-merge-operator.md) — operand semantics the
  batch path must preserve.
- [RFC 0011: Transactions](0011-transaction.md) — SSI read-set tracking.
- [RFC 0024: Segment-Oriented Compaction](0024-segment-oriented-compaction.md)
  — the segmented tree layout the orchestrator groups keys by.

## Updates

- 2026-06-09: Initial draft.
