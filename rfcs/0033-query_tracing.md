# SlateDB Query Tracing

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [Overview](#overview)
   * [Query ID in `ReadOptions` and `ScanOptions`](#query-id-in-readoptions-and-scanoptions)
   * [Tracing spans](#tracing-spans)
- [Impact Analysis](#impact-analysis)
   * [Core API & Query Semantics](#core-api--query-semantics)
   * [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   * [Time, Retention, and Derived State](#time-retention-and-derived-state)
   * [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   * [Compaction](#compaction)
   * [Storage Engine Internals](#storage-engine-internals)
   * [Ecosystem & Operations](#ecosystem--operations)
- [Operations](#operations)
   * [Performance & Cost](#performance--cost)
   * [Observability](#observability)
   * [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
   * [Recording aggregations and spans](#recording-aggregations-and-spans)
   * [Recording the aggregations in a tracing subscriber](#recording-the-aggregations-in-a-tracing-subscriber)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Almog Gavra](https://github.com/agavra)
* [Bruno Cadonna](https://github.com/cadonna)

## Summary

This RFC proposes to instrument the read path of SlateDB with the `tracing` library. The read path is instrumented
per query, i.e., per call to a `get` or `scan` method. The instrumentation consists of a root span that
tracks a complete `get` or `scan` operation (including calls on the returned iterator) and child spans that track various
stages of the read path. For example, looking up an entry in the memtable produces a child span. Another example is
reading a filter of an SST.

The instrumentation is disabled by default. It can be enabled per `get` or `scan` operation by setting a query ID in
the options of the operation, i.e., in `ReadOptions` and in `ScanOptions`.

The generated spans contain fields with information about the span. Each span of a specific read
operation contains the query ID set in the options. In addition, the spans contain the requested key or key range
and information specific to the span, such as the ID of the SST if the span traces processing related to an SST.

We do not propose any tracing subscriber. The produced spans can be processed by an existing tracing subscriber.
For example, `tracing-chrome` can be used to visualize the spans.

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

- Per-query instrumentation via `tracing` spans (e.g., filter evaluations, index reads,
  block reads).
- Zero overhead when not opted in (single `Option` branch skip).
- No changes to `DbRead` trait signatures or public API beyond adding
  a field to existing options structs.

## Non-Goals

- Replacing or duplicating the global `MetricsRecorder` system. Both `DbStats` (aggregate) and the tracing spans
  report to distinct consumers independently.
- Write-path tracing (puts, deletes, flush).
- A new `tracing` subscriber/layer. Users should use an existing `tracing` subscriber/layer, such as `tracing-chrome`,
  or implement their own subscriber/layer to process and visualize spans.

## Design

### Overview

This proposal adds two concepts to the read path:

1. An optional query ID to `ReadOptions` and `ScanOptions`. The default of the query ID is `None`, i.e., tracing is
   disabled by default.
2. `tracing` spans that are conditionally created when the options passed to `get*()` and `scan*()` carry a query ID
   that is not `None`.

### Query ID in `ReadOptions` and `ScanOptions`

`ReadOptions` and `ScanOptions` are extended with an optional query ID.
If the query ID is set, the read path is instrumented. Otherwise,
SlateDB does not create any instrumentation on the read path.

```rust
pub struct ReadOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub cache_blocks: bool,
    pub filter_context: Option<FilterContext>,
    pub query_id: Option<String>,  // new
}

pub struct ScanOptions {
    pub durability_filter: DurabilityLevel,
    pub dirty: bool,
    pub read_ahead_bytes: usize,
    pub cache_blocks: bool,
    pub max_fetch_tasks: usize,
    pub order: IterationOrder,
    pub filter_context: Option<FilterContext>,
    pub query_id: Option<String>,  // new
}
```

Both get a `with_query_id(String) -> Self` builder method. Default is
`None`.

### Tracing spans

| Span                            | Recorded fields                                                    |
|---------------------------------|--------------------------------------------------------------------|
| `slatedb.query`                 | `query_id`, `key`                                                  |
| `slatedb.query.memtable`        | `query_id`, `key`                                                  |
| `slatedb.query.read_filters`    | `query_id`, `key`, `sst_id`, `level`, `cached`                     |
| `slatedb.query.evaluate_filter` | `query_id`, `key`, `sst_id`, `level`, `name`, `result`             |
| `slatedb.query.read_index`      | `query_id`, `key`, `sst_id`, `level`, `cached`                     |
| `slatedb.query.read_blocks`     | `query_id`, `key`, `sst_id`, `level`, `cache_hits`, `cache_misses` |
| `slatedb.query.merge`           | `query_id`, `key`, `num_operands`                                  |

The read path spans are structured hierarchically. The root span for the read path is named `slatedb.query`.
All others are direct children of `slatedb.query`. All spans carry the query ID
(`query_id`) and the requested key or key range (`key`) as fields. The spans are all constructed at debug level.
Spans instrumented on a future are entered each time the future is polled by the runtime.

The root span `slatedb.query` traces the entire read operation, which includes all stages of the read path
covered by the child spans and common operations over all sources needed for reading, such as setting up
iterators. For scans, the span also covers the read operations triggered by calls on the returned lazy iterator.

Span `slatedb.query.memtable` traces lookups on the active memtable and the immutable memtables.

Spans `slatedb.query.read_filters` and `slatedb.query.read_index` trace the reading of filters and reading of the index
of an SST, respectively. The spans carry the ID of the SST (`sst_id`) the filters and the index belong to, the
level on which the SST resides (`level=l0` or `level=sorted_run:{id}`), and field `cached`
that records if the filters or index were found in the cache (`cached=true`) or not (`cached=false`). If the cache
is disabled, `cached` will be `false`.

The evaluation of a single filter is tracked by span `slatedb.query.evaluate_filter`. The span exposes fields for
the SST ID, the level of the SST, the name of the filter (`name`), and the result of the evaluation (`result`).
For the built-in bloom filter, the `name` field will contain `_bf`.

Span `slatedb.query.read_blocks` traces the reading of data blocks of an SST. The fields of the span hold the SST ID,
the level of the SST, and how many cache hits and misses were encountered while reading the blocks.

Processing of the merge operator is traced by span `slatedb.query.merge`. Merging is performed in batches. For each
batch a separate span is produced. Each span contains the number of merged operands as a field.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

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
- [x] Merge operator
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
- [x] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

The proposed instrumentation is disabled by default. Reads without a query ID should not change performance or cost.
When the query ID is set but no tracing subscriber/layer is configured, performance should not be significantly affected.
A query ID and a configured tracing subscriber/layer might negatively affect performance. The performance of
writes and compactions should not be affected at all.

### Observability

Observability is extended by a per-query instrumentation that traces the read path. The instrumentation only produces
traces at debug level and only if a tracing subscriber/layer is configured.

### Compatibility

Field `query_id` is added to the public API `ReadOptions` and `ScanOptions`. The field is also exposed in the bindings.
Since the default value of field `query_id` is `None`, read path tracing is disabled for existing queries.

## Testing

- Unit tests:
  - For each kind of span
- Integration tests:
  - With subscriber and different queries
- Performance tests:
  - With and without query ID,
  - With and without subscriber
  - At info and debug level

## Rollout

- Milestones / phases:
  - Adding root span `slatedb.query`.
  - Adding span `slatedb.query.memtable`.
  - Adding span `slatedb.query.read_filters`.
  - Adding span `slatedb.query.evaluate_filter`.
  - Adding span `slatedb.query.read_index`.
  - Adding span `slatedb.query.read_blocks`.
  - Adding span `slatedb.query.merge`.
  - Performance experiments.
- Docs updates:
  - Documentation of spans
  - Usage example

## Alternatives

### Recording aggregations and spans

The idea was to pass a struct to `ReadOptions` and `ScanOptions` to record and aggregate various measurements, such as
the number of accesses to different sources (e.g., memtables and SSTs), cache misses, and cache hits, as well as instrumenting
spans for recording execution times. This was rejected because collecting aggregations overlapped with instrumenting
the code with spans. We decided to consolidate measurement collection on the read path and also wanted to reduce code
complexity.

### Recording the aggregations in a tracing subscriber

This approach consisted of instrumenting the read path and creating a tracing subscriber specifically for SlateDB that
also maintains aggregations. The tracing subscriber would process the instrumented spans and offer an API to read the
recorded data on the read path. This approach was rejected because of the complexity and maintenance burden.
There are already existing tracing subscribers, e.g., `tracing-chrome`, that can be used for analyzing an instrumented
read path. We decided to start with the instrumentation and postpone a dedicated tracing subscriber to the future if
required.

## Open Questions

- How should byte keys be represented in span fields, e.g., base64 or hex?
- Should we add a field to the `read_filters` span that records how many filters are read from the SST?

## References

- `tracing` crate: https://crates.io/crates/tracing
- `tracing-chrome`: https://crates.io/crates/tracing-chrome
- https://github.com/slatedb/slatedb/issues/400
- https://github.com/slatedb/slatedb/issues/797

## Updates
