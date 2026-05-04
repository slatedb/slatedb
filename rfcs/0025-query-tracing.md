# SlateDB Query Tracing

Status: Draft

Authors:

* [Almog Gavra](https://github.com/agavra)
* [Bruno Cadonna](https://github.com/cadonna)

## Summary

Add an opt-in, per-query trace context (`QueryTrace`) that accumulates
counters and emits `tracing` spans as a query executes. Users attach a
`QueryTrace` to `ReadOptions` or `ScanOptions` and inspect it after
the query completes for both programmatic stats and timed spans.
`QueryTrace` records the number of memtables and SSTs that were
consulted, how many entries were skipped by read-time visibility rules,
and how many data, filter, and index blocks were fetched from object
storage.

## Motivation

SlateDB tracks read-path statistics (bloom filter hits, request counts)
via global `DbStats` counters backed by the `MetricsRecorder` system
(RFC-0021). These aggregate counters answer "how is the system doing?"
but not "why was *this* query slow?" or "how many SSTs did my point
lookup touch?"

Users today cannot:
- Determine whether a slow get was caused by bloom filter false
  positives, cache misses, or scanning too many L0 SSTs
- Measure how much wall-clock time a scan spent reading blocks from
  object storage vs. serving from cache
- Write tests that assert query execution characteristics (e.g.
  "this get should hit the bloom filter and skip the SST")

## Goals

- Per-query counters for source consulted (memtables/SSTs consulted)
- Per-query counters for discarded work (entries skipped by sequence
  filtering, and tombstone/newer-value shadowing)
- Per-query counters for cache and object-store activity (cache
  hit/miss, data/filter/index blocks and bytes fetched from object
  store)
- Per-query timing via `tracing` spans (bloom filter eval, index read,
  block read), captured by any active tracing subscriber
- Zero overhead when not opted in (single `Option` branch skip)
- No changes to `DbRead` trait signatures or public API beyond adding
  a field to existing options structs

## Non-Goals

- Replacing or duplicating the global `MetricsRecorder` system. Both
  `DbStats` (aggregate) and `QueryTrace` (per-query) report to their
  respective consumers independently.
- Write-path tracing (puts, deletes, flush)
- Distributed tracing context propagation (span IDs, trace IDs). Users
  can parent the `QueryTrace` span under their own span if needed.

## Design

### Overview

This proposal adds four pieces to the read path:

1. A new `QueryTrace` type that is attached to `ReadOptions` or
   `ScanOptions` and shared across the lifetime of a query.
2. Per-query counters that describe source selection, discarded work,
   cache behavior, and object-store activity.
3. Per-query `tracing` spans for the major timed phases of query
   execution.

At a high level, the query path records:

- which memtables and SSTs were consulted
- which entries were skipped and why
- which data, filter, and index reads hit cache vs. fell through to
  object storage

The counters fall into four groups:

1. Source counters: memtables and SSTs consulted.
2. Discarded-work counters: entries skipped by sequence filtering,
   or tombstone/newer-value shadowing.
3. Cache/store counters: block/index/filter cache hits and misses, plus
   reads that fell through to object storage.
4. Timing counters: cumulative duration spent in block, index, filter,
   and merge-operator work.

The spans complement those counters by showing where time was spent in a
single query execution. They are emitted only when a `QueryTrace` is
attached and an active tracing subscriber is present.
Counters show per-query aggregates; spans are meant for
diagnosing timing and sequencing.

### Counters

Definitions used below:

- **consulted**: The source was effectively accessed during the read path for the query. Sources planned to be accessed, but not accessed because the result has been found, do not increment this counter.
- **skipped**: The entry was collected during the read path but discarded
  before becoming part of the final visible answer

| Counter                        | Description                                                                                   |
|--------------------------------|-----------------------------------------------------------------------------------------------|
| `memtables_consulted`          | Number of memtable + imm iterators accessed                                                   |
| `ssts_consulted_l0`            | Number of L0 SSTs accessed                                                                    |
| `ssts_consulted_compacted`     | Number of compacted SSTs accessed                                                             |
| `bloom_filter_positives`       | Number of positive filter results: `might_contain` -> true                                    |
| `bloom_filter_negatives`       | Number of negative filter results: `might_contain` -> false                                   |
| `bloom_filter_false_positives` | Number of positive filter results, that turned out to be wrong                                |
| `block_cache_hits`             | Number of data blocks read from cache                                                         |
| `block_cache_misses`           | Number of data blocks not found in cache                                                      |
| `index_cache_hits`             | Number of index blocks read from cache                                                        |
| `index_cache_misses`           | Number of index blocks not found in cache                                                     |
| `filter_cache_hits`            | Number of filter blocks read from cache                                                       |
| `filter_cache_misses`          | Number of filter blocks not found in cache                                                    |
| `block_fetched_from_store`     | Number of data blocks fetched from object store                                               |
| `filter_fetched_from_store`    | Number of filter blocks fetched from object store                                             |
| `index_fetched_from_store`     | Number of index blocks fetched from object store                                              |
| `bytes_fetched_from_store`     | Number of bytes fetched from object store                                                     |
| `merge_operands`               | Number of operands passed to `merge_batch`                                                    |
| `skipped_entries_seq_filtered` | Number of entries skipped because their sequence number exceeded the query's visibility bound |
| `skipped_entries_shadowed`     | Number of entries skipped because they were hidden by a tombstone or newer value              |

Derived counters:
- `ssts_consulted_total = ssts_consulted_l0 + ssts_consulted_compacted`.
- `bloom_filter_checks = bloom_filter_positives + bloom_filter_negatives`.

### Aggregate durations

| Duration               | Description                        |
|------------------------|------------------------------------|
| `block_read_duration`  | Time spent in data block reads     |
| `index_read_duration`  | Time spent in index reads          |
| `filter_read_duration` | Time spent in filter reads         |
| `merge_duration`       | Time spent in merge operator calls |

Stored as `AtomicU64` microseconds internally. Exposed via accessors
returning `std::time::Duration`. Measured with `Instant::now()` around
each operation, elapsed added atomically. These may overlap when reads
are concurrent (e.g. multiple SSTs read in parallel during a scan), so
they represent *cumulative* time, not wall-clock.

### Tracing spans

Child spans under the parent `slatedb.query`, all at `debug` level:

| Span                         | Recorded fields                        |
|------------------------------|----------------------------------------|
| `slatedb.query`              | parent span                            |
| `slatedb.query.memtable`     | `key`, `value`                         |
| `slatedb.query.bloom_filter` | `sst_id`, `key`, `result`              |
| `slatedb.query.read_index`   | `sst_id`, `cached`                     |
| `slatedb.query.read_filter`  | `sst_id`, `cached`                     |
| `slatedb.query.read_blocks`  | `sst_id`, `cache_hits`, `cache_misses` |
| `slatedb.query.merge`        | `key`, `operands`                      |

The tracing subscriber captures span duration on exit. Each
`read_blocks` span covers one SST's block read, so a tool like Jaeger
shows per-SST timing in a waterfall. Users who want aggregate timing
without a subscriber can use the duration counters above
(`trace.block_read_duration()`, etc.).

Definition of the fields:
- `key`: Key of the query
- `value`: Found value of the `key`
- `sst_id`: ID of the SST
- `cached`: `true` If the block was found in the cache, `false` otherwise
- `cache_hits`: Number of blocks found in the cache
- `cache_misses`: Number of blocks not found in the cache
- `operands`: Operands in a merge operation


## Implementation

### `QueryTrace`

The `QueryTrace` struct is the per-query trace context passed via options to the scan and get queries.
It contains the counters and the span that are updated during query execution.

```rust
#[derive(Clone, Debug, Default)]
pub struct QueryTrace {
    inner: Arc<QueryTraceInner>,
}

impl QueryTrace {
    // Construction
    pub fn new() -> Self;
    
    // Public: counter accessors
    pub fn memtables_consulted(&self) -> u64;
    pub fn ssts_consulted_l0(&self) -> u64;
    pub fn ssts_consulted_compacted(&self) -> u64;
    pub fn ssts_consulted_total(&self) -> u64;

    pub fn bloom_filter_checks(&self) -> u64;
    pub fn bloom_filter_positives(&self) -> u64;
    pub fn bloom_filter_negatives(&self) -> u64;
    pub fn bloom_filter_false_positives(&self) -> u64;

    pub fn block_cache_hits(&self) -> u64;
    pub fn block_cache_misses(&self) -> u64;
    pub fn index_cache_hits(&self) -> u64;
    pub fn index_cache_misses(&self) -> u64;
    pub fn filter_cache_hits(&self) -> u64;
    pub fn filter_cache_misses(&self) -> u64;

    pub fn skipped_entries_seq_filtered(&self) -> u64;
    pub fn skipped_entries_shadowed(&self) -> u64;

    pub fn block_fetched_from_store(&self) -> u64;
    pub fn filter_fetched_from_store(&self) -> u64;
    pub fn index_fetched_from_store(&self) -> u64;
    pub fn bytes_fetched_from_store(&self) -> u64;

    pub fn merge_operands(&self) -> u64;

    // Public: duration accessors
    pub fn block_read_duration(&self) -> Duration;
    pub fn index_read_duration(&self) -> Duration;
    pub fn filter_read_duration(&self) -> Duration;
    pub fn merge_duration(&self) -> Duration;
}
```

The public API consists of a constructor and one read accessor per
counter / duration. Internal call sites use `pub(crate)` increment
helpers and a `span()` accessor for attaching child spans.

### Attaching to options

```rust
// config.rs
pub struct ReadOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
    pub trace: Option<QueryTrace>,  // new
}

pub struct ScanOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub read_ahead_bytes: usize,
    pub cache_blocks: bool,
    pub max_fetch_tasks: usize,
    pub order: IterationOrder,
    pub trace: Option<QueryTrace>,  // new
}
```

Both get a `with_trace(QueryTrace) -> Self` builder method. Default is
`None`. This works with the existing `&ReadOptions`/`&ScanOptions`
references because `QueryTrace` uses interior mutability.

### Usage

```rust
// Point lookup
let trace = QueryTrace::new();
let opts = ReadOptions::default().with_trace(trace.clone());
let val = db.get_with_options(b"key", &opts).await?;

// Programmatic counters
assert_eq!(trace.bloom_filter_negatives(), 3);
assert_eq!(trace.block_cache_hits(), 1);

// Aggregate timing
println!(
    "block reads took {:?}",
    trace.block_read_duration()
);

// With a tracing subscriber active, timed child spans are
// emitted automatically under the parent `slatedb.query` span.
```

For scans, stats accumulate across the full iterator lifetime:

```rust
let trace = QueryTrace::new();
let opts = ScanOptions::default().with_trace(trace.clone());
let mut iter = db.scan_with_options("a".."z", &opts).await?;
while let Some(kv) = iter.next().await? { /* … */ }
println!(
    "total data blocks from store: {}",
    trace.block_fetched_from_store()
);
println!("total block read time: {:?}", trace.block_read_duration());
```

### Consuming trace spans

The programmatic counters (`trace.block_cache_hits()`, etc.) work
without any setup. To also capture timed spans, register a `tracing`
subscriber at program startup.

**Stdout logging:**

```rust
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();

// Spans are now logged on close with durations:
// DEBUG slatedb.query.read_blocks{sst_id=42 cache_hits=1 ..}: close time.busy=1.2ms
```

**Filtered to query spans only (code or env var):**

```rust
use tracing_subscriber::EnvFilter;

tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::new("slatedb.query=debug"))
    .init();
```

```bash
RUST_LOG=slatedb.query=debug cargo run
```

**Jaeger/OTLP (production):**

```rust
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

tracing_subscriber::registry()
    .with(tracing_opentelemetry::layer()
        .with_tracer(otel_tracer))
    .with(tracing_subscriber::fmt::layer())
    .init();
```

## Impact Analysis

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

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
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- When `trace: None` (default), each instrumentation point is a single
  `if let Some(trace)` branch. Predictable skip, zero allocation.
- When active: one `Arc` clone per iterator built, `Relaxed` atomic
  increments at each instrumentation point, `Instant::now()` calls
  around timed phases, `tracing::Span` creation at `debug` level
  (no-op without a subscriber).
- No impact on object store request patterns or amplification.

### Observability

- New `trace: Option<QueryTrace>` field on `ReadOptions` and
  `ScanOptions`.
- No changes to global `DbStats` / `MetricsRecorder`.
- New `slatedb.query.*` spans at `debug` level, only emitted when a
  `QueryTrace` is attached and a subscriber is registered.

### Compatibility

- On-disk/object storage formats: no impact.
- Public API: additive. New `Option<T>` field defaulting to `None`.
  Existing code using `ReadOptions::default()` or
  `ScanOptions::default()` is unaffected. Users who destructure the
  structs will need to add the new field.

## Testing

- Unit tests: Create `QueryTrace`, run get/scan against a DB with
  known SST structure, assert counter values for consulted sources,
  skipped-entry reasons, bloom negative, cache hit/miss, and multi-SST
  scan.
- Integration tests: Verify counters and durations are non-zero for
  object-store-backed reads.
- Existing tests pass unchanged (default options -> `trace: None`).

## Rollout

Single PR. `None` default means zero behavior change for existing
users.

## Alternatives

- **Return stats from the method** (`get_traced` returning
  `(value, trace)`): Doubles method count on `DbRead` (8 to 16).
  For scans, stats accumulate over iterator lifetime so they can't
  be returned upfront.
- **Separate parameter** (`get(key, opts, trace)`): Signature change
  across `DbRead` trait and all four implementations.
- **Tracing-only** (no counters): Requires a tracing subscriber and
  custom layers to extract programmatic stats. Not viable for tests
  or application-level adaptive logic.
- **Counters-only** (no tracing spans): Loses timing information,
  which is the most useful signal for debugging slow queries.

## Open Questions

1. Should `QueryTrace::new()` accept an optional parent
   `tracing::Span` so users can nest it under their application span?
2. Should duration fields track only object store I/O or also include
   cache lookup time?
3. Should we add a `Display` impl on `QueryTrace` that pretty-prints
   all stats for quick debugging?
4. Should we distinguish between:
    - number of sources that were consulted
    - number of sources that contained a candidate
    - number of sources that contained a result
      to assess read amplification?

## References

- RFC-0021 (Metrics): `rfcs/0021-metrics.md`
- `tracing` crate: https://crates.io/crates/tracing
- RocksDB `PerfContext`:
  https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context
- Current `DbStats`: `slatedb/src/db_stats.rs`
- Current `BloomFilterEvaluator`: `slatedb/src/sst_iter.rs`
- Current `TableStore` read methods: `slatedb/src/tablestore.rs`
- [slatedb#797](https://github.com/slatedb/slatedb/issues/797) (query tracing / per-request stats)
- [slatedb#400](https://github.com/slatedb/slatedb/issues/400) (expose read statistics)
