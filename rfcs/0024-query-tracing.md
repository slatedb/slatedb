# SlateDB Query Tracing

Status: Draft

Authors:

* [Almog Gavra](https://github.com/agavra)

## Summary

Add an opt-in, per-query trace context (`QueryTrace`) that accumulates
counters and emits `tracing` spans as a query executes. Users attach a
`QueryTrace` to `ReadOptions` or `ScanOptions` and inspect it after
the query completes for both programmatic stats and timed spans.

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

- Per-query counters (SSTs consulted, bloom filter outcomes, cache
  hit/miss, blocks and bytes fetched from object store)
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

### `QueryTrace` type

New file `slatedb/src/query_trace.rs`:

```rust
#[derive(Clone, Debug, Default)]
pub struct QueryTrace {
    inner: Arc<QueryTraceInner>,
}

struct QueryTraceInner {
    span: tracing::Span,

    // Memtables
    memtables_checked: AtomicU64,

    // SSTs
    ssts_consulted_l0: AtomicU64,
    ssts_consulted_compacted: AtomicU64,

    // Bloom filter
    bloom_filter_checks: AtomicU64,
    bloom_filter_positives: AtomicU64,
    bloom_filter_negatives: AtomicU64,
    bloom_filter_false_positives: AtomicU64,

    // Block cache
    block_cache_hits: AtomicU64,
    block_cache_misses: AtomicU64,

    // Index/filter cache
    index_cache_hits: AtomicU64,
    index_cache_misses: AtomicU64,
    filter_cache_hits: AtomicU64,
    filter_cache_misses: AtomicU64,

    // Object store I/O
    blocks_fetched_from_store: AtomicU64,
    bytes_fetched_from_store: AtomicU64,

    // Merge operator
    merge_operands: AtomicU64,

    // Aggregate durations (microseconds)
    block_read_duration_us: AtomicU64,
    index_read_duration_us: AtomicU64,
    filter_read_duration_us: AtomicU64,
    merge_duration_us: AtomicU64,
}
```

- `Arc`-based so `Clone` is a pointer bump. SST iterators may be
  spawned onto different Tokio tasks.
- All atomics use `Ordering::Relaxed`. No cross-counter ordering is
  needed, and the `.await` at query completion provides happens-before.
- Public read accessors return `u64`; `pub(crate)` increment methods
  used internally. Duration accessors return `std::time::Duration`.
- `Default` creates the parent span `slatedb.query` at `debug` level.
  When no tracing subscriber is active, span creation costs almost
  nothing.

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
println!("total blocks from store: {}", trace.blocks_fetched_from_store());
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

### Counters

| Counter | Where incremented | Description |
|---|---|---|
| `memtables_checked` | `Reader::build_iterator_sources` | Memtable + imm iterators created |
| `ssts_consulted_l0` | `build_point_l0_iters`, `build_range_l0_iters` | L0 SSTs consulted |
| `ssts_consulted_compacted` | `build_point_sr_iters`, `build_range_sr_iters` | Compacted SSTs consulted |
| `bloom_filter_checks` | `BloomFilterEvaluator::evaluate` | Every bloom evaluation |
| `bloom_filter_positives` | `BloomFilterEvaluator::evaluate` | `might_contain` -> true |
| `bloom_filter_negatives` | `BloomFilterEvaluator::evaluate` | `might_contain` -> false |
| `bloom_filter_false_positives` | `BloomFilterEvaluator::notify_finished_iteration` | Positive, key absent |
| `block_cache_hits` | `TableStore::read_blocks_using_index` | Blocks from cache |
| `block_cache_misses` | `TableStore::read_blocks_using_index` | Blocks not in cache |
| `index_cache_hits` | `TableStore::read_index` | Index from cache |
| `index_cache_misses` | `TableStore::read_index` | Index from store |
| `filter_cache_hits` | `TableStore::read_filter` | Filter from cache |
| `filter_cache_misses` | `TableStore::read_filter` | Filter from store |
| `blocks_fetched_from_store` | `TableStore::read_blocks_using_index` | Blocks fetched from object store |
| `bytes_fetched_from_store` | `TableStore::read_blocks_using_index` | Bytes fetched from object store |
| `merge_operands` | `MergeOperatorIterator::process_batch` | Operands passed to `merge_batch` |

Derived: `ssts_consulted_total() = l0 + compacted`.

### Aggregate durations

| Duration | Where accumulated | Description |
|---|---|---|
| `block_read_duration` | `TableStore::read_blocks_using_index` | Wall-clock time in block reads |
| `index_read_duration` | `TableStore::read_index` | Wall-clock time in index reads |
| `filter_read_duration` | `TableStore::read_filter` | Wall-clock time in filter reads |
| `merge_duration` | `MergeOperatorIterator::merge_with_older_entries` | Wall-clock time in merge operator calls |

Stored as `AtomicU64` microseconds internally. Exposed via accessors
returning `std::time::Duration`. Measured with `Instant::now()` around
each operation, elapsed added atomically. These may overlap when reads
are concurrent (e.g. multiple SSTs read in parallel during a scan), so
they represent *cumulative* time, not wall-clock.

### Tracing spans

Child spans under the parent `slatedb.query`, all at `debug` level:

| Span | Created in | Recorded fields |
|---|---|---|
| `slatedb.query` | `QueryTrace::new()` | parent span |
| `slatedb.query.bloom_filter` | `BloomFilterEvaluator::evaluate` | `sst_id`, `result` |
| `slatedb.query.read_index` | `TableStore::read_index` | `sst_id`, `cached` |
| `slatedb.query.read_filter` | `TableStore::read_filter` | `sst_id`, `cached` |
| `slatedb.query.read_blocks` | `TableStore::read_blocks_using_index` | `sst_id`, `cache_hits`, `cache_misses` |
| `slatedb.query.merge` | `MergeOperatorIterator::merge_with_older_entries` | `key`, `operands` |

The tracing subscriber captures span duration on exit. Each
`read_blocks` span covers one SST's block read, so a tool like Jaeger
shows per-SST timing in a waterfall. Users who want aggregate timing
without a subscriber can use the duration counters above
(`trace.block_read_duration()`, etc.).

### Internal threading

Follows the existing `DbStats` plumbing:

1. `Reader` extracts `options.trace.clone()` and passes it alongside
   `DbStats` through `build_iterator_sources` ->
   `build_point_*_iters` / `build_range_*_iters`.
2. `SstIterator` / `BloomFilterEvaluator` gain `Option<QueryTrace>`
   alongside existing `Option<DbStats>`. Bloom stats dual-reported.
3. `InternalSstIterator` gains `Option<QueryTrace>`, forwarded to
   `TableStore` read calls.
4. `SortedRunIterator` carries `Option<QueryTrace>`, forwards to
   child `SstIterator` via `build_next_iter`.
5. `TableStore::read_filter`, `read_index`, `read_blocks_using_index`
   gain `query_trace: Option<&QueryTrace>`. All existing callers
   (compaction, flush, etc.) pass `None`.
6. `MergeOperatorIterator` gains `Option<QueryTrace>`. Increments
   `merge_operands` in `process_batch` and accumulates
   `merge_duration` around `merge_with_older_entries`.

### Files modified

| File | Change |
|---|---|
| `slatedb/src/query_trace.rs` | **New.** `QueryTrace` with atomics + span helpers |
| `slatedb/src/lib.rs` | `mod query_trace; pub use query_trace::QueryTrace;` |
| `slatedb/src/config.rs` | `trace: Option<QueryTrace>` + builder on both option structs |
| `slatedb/src/reader.rs` | Extract trace, thread to iterator builders |
| `slatedb/src/sst_iter.rs` | Add trace to evaluator/iterators |
| `slatedb/src/sorted_run_iterator.rs` | Carry and forward trace |
| `slatedb/src/tablestore.rs` | `Option<&QueryTrace>` on read methods, instrumentation |
| `slatedb/src/merge_operator.rs` | Add trace to `MergeOperatorIterator`, instrument `process_batch` |
| `slatedb/src/db_iter.rs` | Pass trace into `MergeOperatorIterator::new` |

No changes to `db.rs`, `db_read.rs`, `db_snapshot.rs`,
`db_transaction.rs`, `db_reader.rs`.

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
  `QueryTrace` is attached.

### Compatibility

- On-disk/object storage formats: no impact.
- Public API: additive. New `Option<T>` field defaulting to `None`.
  Existing code using `ReadOptions::default()` or
  `ScanOptions::default()` is unaffected. Users who destructure the
  structs will need to add the new field.

## Testing

- Unit tests: Create `QueryTrace`, run get/scan against a DB with
  known SST structure, assert counter values for memtable hit, SST
  hit, bloom negative, cache hit/miss, multi-SST scan.
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
