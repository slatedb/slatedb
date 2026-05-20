# SlateDB Query Tracing

Status: Draft

Authors:

* [Almog Gavra](https://github.com/agavra)
* [Bruno Cadonna](https://github.com/cadonna)

## Summary

Instrument the read-path with spans and other `tracing` events as well
as add a query `tracing` layer that subscribes to `tracing` events
and accumulates counters as a query executes.
Users of slateDB can register the query tracing
layer in their applications. By giving an ID to a query via
`ReadOptions` or `ScanOptions`, they can enable creation of the spans during
query execution. The query tracing layer records the number of
memtables and SSTs that were consulted, how many entries were skipped
by read-time visibility rules, and how many data, filter, and index
blocks were fetched from object storage.

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
  block read), captured by the subscribed tracing layers.
- A tracing layer that specifically records spans and computes aggregates.
- Zero overhead when not opted in (single `Option` branch skip)
- No changes to `DbRead` trait signatures or public API beyond adding
  a field to existing options structs

## Non-Goals

- Replacing or duplicating the global `MetricsRecorder` system. Both
  `DbStats` (aggregate) and the query tracing layer
  report to distinct consumers independently.
- Write-path tracing (puts, deletes, flush)


## Design

### Overview

This proposal adds three pieces to the read path:

1. An optional query ID to `ReadOptions` or
   `ScanOptions`.
2. `tracing` spans that are conditionally created when `get()` and `scan()`
   carry a query ID.
3. A `tracing` layer that can be registered with the `tracing` subscriber
   in an application and records the spans created during `get()` and
   `scan()` and computes aggregates per query.

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


### Counters

Definitions used below:

- **consulted**: The source was effectively accessed during the read path for the query.
  Sources planned to be accessed, but not accessed because the result has been found,
  do not increment this counter.
- **skipped**: The entry was collected during the read path but discarded
  before becoming part of the final visible answer

| Counter                        | Description                                                                                             |
|--------------------------------|---------------------------------------------------------------------------------------------------------|
| `memtables_consulted`          | Number of accesses to current memtable + immutable memtable                                             |
| `ssts_consulted_l0`            | Number of L0 SSTs accessed                                                                              |
| `ssts_consulted_compacted`     | Number of compacted SSTs accessed                                                                       |
| `sr_consulted`                 | Number of sorted runs accessed                                                                          |
| `bloom_filter_positives`       | Number of positive filter results: `might_contain` -> true                                              |
| `bloom_filter_negatives`       | Number of negative filter results: `might_contain` -> false                                             |
| `bloom_filter_false_positives` | Number of positive filter results, that turned out to be wrong                                          |
| `block_cache_hits`             | Number of data blocks read from cache                                                                   |
| `block_cache_misses`           | Number of data blocks not found in cache                                                                |
| `index_cache_hits`             | Number of index blocks read from cache                                                                  |
| `index_cache_misses`           | Number of index blocks not found in cache                                                               |
| `filter_cache_hits`            | Number of filter blocks read from cache                                                                 |
| `filter_cache_misses`          | Number of filter blocks not found in cache                                                              |
| `block_fetched_from_store`     | Number of data blocks fetched from object store                                                         |
| `filter_fetched_from_store`    | Number of filter blocks fetched from object store                                                       |
| `index_fetched_from_store`     | Number of index blocks fetched from object store                                                        |
| `bytes_fetched_from_store`     | Number of bytes fetched from object store                                                               |
| `merge_operands`               | Number of operands passed to `merge_batch`                                                              |
| `skipped_entries_seq_filtered` | Number of entries read, but skipped because their sequence number exceeded the query's visibility bound |
| `skipped_entries_shadowed`     | Number of entries read, but skipped because they were hidden by a tombstone or newer value              |

Derived counters:
- `ssts_consulted_total = ssts_consulted_l0 + ssts_consulted_compacted`.
- `bloom_filter_checks = bloom_filter_positives + bloom_filter_negatives`.

### Aggregate durations

| Duration                         | Description                        |
|----------------------------------|------------------------------------|
| `block_read_duration`            | Time spent in data block reads     |
| `index_read_duration`            | Time spent in index reads          |
| `filter_read_duration`           | Time spent in filter reads         |
| `memtable_read_duration`         | Time spent in memtable reads       |
| `bloom_filter_check_duration`    | Time spent checking bloom filters  |
| `merge_duration`                 | Time spent in merge operator calls |

Stored as `AtomicU64` microseconds internally. Exposed via accessors
returning `std::time::Duration`. Measured with `Instant::now()` around
each operation, elapsed added atomically. These may overlap when reads
are concurrent (e.g. multiple SSTs read in parallel during a scan), so
they represent *cumulative* time, not wall-clock.

### Tracing spans

Child spans under the parent `slatedb.query`, all at `debug` level:

| Span                          | Recorded fields                                   |
|-------------------------------|---------------------------------------------------|
| `slatedb.query`               | parent span                                       |
| `slatedb.query.memtable`      | `queryId`, `key`                                  |
| `slatedb.query.read_blocks`   | `queryId`, `sst_id`, `cache_hits`, `cache_misses` |
| `slatedb.query.bloom_filter`  | `sst_id`, `key`, `result`                         |
| `slatedb.query.read_index`    | `sst_id`, `cached`                                |
| `slatedb.query.read_filter`   | `sst_id`, `cached`                                |
| `slatedb.query.merge`         | `key`, `num_operands`                             |

The tracing subscriber captures span duration on exit. Each
`read_blocks` span covers one SST's block read, so a tool like Jaeger
shows per-SST timing in a waterfall. Users who want aggregate timing
without a subscriber can use the duration counters above
(`trace.block_read_duration()`, etc.).

Definition of the fields:
- `key`: Key of the query
- `sst_id`: ID of the SST
- `cached`: `true` If the block was found in the cache, `false` otherwise
- `cache_hits`: Number of blocks found in the cache
- `cache_misses`: Number of blocks not found in the cache
- `num_operands`: Number of operands in a merge operation


## Implementation

Internally, we instrument the read-path to emit spans and other `tracing` events.
Instrumentation is not part of the public API. The public API is extended with a
`tracing` layer in a new module `slatedb-tracing`. Additionally, `ReadOptions` and
`ScanOptions` are extended by an optional query ID.

### Query `tracing` layer

If application developers use the [`tracing` crate](https://docs.rs/tracing/0.1.44/tracing/index.html),
they can set up a `tracing` subscriber in their application that subscribes to spans instrumented
in the application and the used library. An application can only set up one subscriber but
multiple layers can be registered with the subscriber. All layers subscribe to the instrumented spans.
For example, the following subscriber has two layer registered -- one for OpenTelemetry and one for
the `fmt` subscriber that logs `tracing` events:

```rust
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

tracing_subscriber::registry()
    .with(tracing_opentelemetry::layer()
        .with_tracer(otel_tracer))
    .with(tracing_subscriber::fmt::layer())
    .init();
```


In this RFC, we propose a query tracing `tracing` layer, that can be used for
- investigating slow queries,
- write tests that assert query execution characteristics (e.g.
  "this get should hit the bloom filter and skip the SST")

This query `tracing` layer receives events from spans and other
events instrumented in the slateDB code. The query `tracing`
layer processes the received spans and events and updates
aggregates about read SSTs, cache misses, bloom filter false
positives and more. Those aggregates can be consulted to
understand the causes of slow queries and to take actions on
them.
In tests, the query `tracing` layer can be used to verify
intended internal behavior.

The query `tracing` layer is not intended for use in production
settings. It is a tool to explore behavior in debugging sessions
and verify behavior in testing setups.

```rust
#[derive(Clone, Default)]
pub struct QueryTracingLayer { ... }

impl QueryTracingLayer {
    // Construction
    pub fn new() -> Self;

    // Public: counter accessors
    pub fn memtables_consulted(&self, query_id: &str) -> u64;
    pub fn ssts_consulted_l0(&self, query_id: &str) -> u64;
    pub fn ssts_consulted_compacted(&self, query_id: &str) -> u64;
    pub fn ssts_consulted_total(&self, query_id: &str) -> u64;

    pub fn bloom_filter_checks(&self, query_id: &str) -> u64;
    pub fn bloom_filter_positives(&self, query_id: &str) -> u64;
    pub fn bloom_filter_negatives(&self, query_id: &str) -> u64;
    pub fn bloom_filter_false_positives(&self, query_id: &str) -> u64;

    pub fn block_cache_hits(&self, query_id: &str) -> u64;
    pub fn block_cache_misses(&self, query_id: &str) -> u64;
    pub fn index_cache_hits(&self, query_id: &str) -> u64;
    pub fn index_cache_misses(&self, query_id: &str) -> u64;
    pub fn filter_cache_hits(&self, query_id: &str) -> u64;
    pub fn filter_cache_misses(&self, query_id: &str) -> u64;

    pub fn skipped_entries_seq_filtered(&self, query_id: &str) -> u64;
    pub fn skipped_entries_shadowed(&self, query_id: &str) -> u64;

    pub fn block_fetched_from_store(&self, query_id: &str) -> u64;
    pub fn filter_fetched_from_store(&self, query_id: &str) -> u64;
    pub fn index_fetched_from_store(&self, query_id: &str) -> u64;
    pub fn bytes_fetched_from_store(&self, query_id: &str) -> u64;

    pub fn merge_operands(&self, query_id: &str) -> u64;

    // Public: duration accessors
    pub fn block_read_duration(&self, query_id: &str) -> Duration;
    pub fn index_read_duration(&self, query_id: &str) -> Duration;
    pub fn filter_read_duration(&self, query_id: &str) -> Duration;
    pub fn memtable_read_duration(&self, query_id: &str) -> Duration;
    pub fn bloom_filter_check_duration(&self, query_id: &str) -> Duration;
    pub fn merge_duration(&self, query_id: &str) -> Duration;
}
```

The public API consists of a constructor and one read accessor per
counter / duration. `QueryTracingLayer` implements the
`tracing_subscriber::layer::Layer` trait, so that it can be registered
with a `tracing` subscriber.


### `Display` implementation

`QueryTracingLayer` implements `std::fmt::Display` to pretty-print all
counters and durations as a human-readable summary. This is intended
for debugging (`println!("{layer}")`).

```rust
impl std::fmt::Display for QueryTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ...
    }
}
```

Example output:

```text
QueryTracingLayer {
  query: query1 {
      sources: memtables=2 ssts_l0=3 ssts_compacted=5 sr=2
      bloom_filter: checks=8 positives=5 negatives=3 false_positives=1
      cache: block=12h/3m index=5h/0m filter=5h/0m
      object_store: blocks=3 indexes=0 filters=0 bytes=49152
      skipped: seq_filtered=7 shadowed=2
      merge_operands=0
      durations: block=1.2ms index=300µs filter=100µs memtable=50µs merge=0ns
  }
}
```

The exact layout is not part of the API contract — callers that need
stable values should use the typed accessors directly.

### Query ID in `ReadOptions` and `ScanOptions`

`ReadOptions` and `ScanOptions` are extended with an optional query ID.
If the query ID is set, the read-path is instrumented. Otherwise,
slateDB does not create any instrumentation on the read-path.

```rust
// config.rs
pub struct ReadOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
    pub query_id: Option<String>,  // new
}

pub struct ScanOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub read_ahead_bytes: usize,
    pub cache_blocks: bool,
    pub max_fetch_tasks: usize,
    pub order: IterationOrder,
    pub query_id: Option<String>,  // new
}
```

Both get a `with_query_id(String) -> Self` builder method. Default is
`None`.

### Usage

```rust

let query_tracing_layer = QueryTracing::new();
tracing_subscriber::registry()
    .with(query_tracing_layer)
    .init();

// Point lookup
let query_id = "query1";
let opts = ReadOptions::default().with_query_id(query_id.to_string());
let val = db.get_with_options(b"key", &opts).await?;

// Programmatic counters
assert_eq!(trace.bloom_filter_negatives(query_id), 3);
assert_eq!(trace.block_cache_hits(query_id), 1);

// Aggregate timing
println!(
    "block reads took {:?}",
    trace.block_read_duration(query_id)
);
```

### Visualizing spans

To visualize the emitted spans, users can additionally register the
[`tracing_chrome` layer](https://docs.rs/tracing-chrome/latest/tracing_chrome/) with the subscriber:

```rust
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{registry::Registry, prelude::*};

let (chrome_layer, _guard) = ChromeLayerBuilder::new().build();
let query_tracing_layer = QueryTracing::new();

tracing_subscriber::registry()
    .with(query_tracing_layer)
    .with(chrome_layer)
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

- When `query_id: None` (default), each instrumentation point is a single
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
  query ID is specified and the query tracing layer is registered.

### Compatibility

- On-disk/object storage formats: no impact.
- Public API: additive. New `Option<T>` field defaulting to `None`.
  Existing code using `ReadOptions::default()` or
  `ScanOptions::default()` is unaffected. Users who destructure the
  structs will need to add the new field.

## Testing

- Unit tests: Specify a query ID and register the query tracing layer,
  run get/scan against a DB with known SST structure, assert counter
  values for consulted sources, skipped-entry reasons, bloom negative,
  cache hit/miss, and multi-SST scan.
- Integration tests: Verify counters and durations are non-zero for
  object-store-backed reads.
- Existing tests pass unchanged (default options -> `query_id: None`).

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
- **Passing `QueryTrace` struct to `Read-/ScanOptions`**:
  The `QueryTrace` struct is updated during the read-path and would also
  collect spans. That doubles the instrumentation because the fields
  of the `QueryTrace` struct need to be updated and the spans handled.
  Collecting aggregates with help of the spans is less invasive.
- **Counters-only** (no tracing spans): Loses timing information,
  which is the most useful signal for debugging slow queries.

## Open Questions

2. Should duration fields track only object store I/O or also include
   cache lookup time?
3. Should we distinguish between:
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
