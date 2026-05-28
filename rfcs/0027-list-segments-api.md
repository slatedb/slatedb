# List Segments API

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [Public API](#public-api)
   - [Durability semantics](#durability-semantics)
   - [Reader-side extractor](#reader-side-extractor)
   - [Memory level without an extractor](#memory-level-without-an-extractor)
   - [Internal mechanics](#internal-mechanics)
   - [Worked examples](#worked-examples)
- [Impact Analysis](#impact-analysis)
   - [Core API & Query Semantics](#core-api--query-semantics)
   - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   - [Time, Retention, and Derived State](#time-retention-and-derived-state)
   - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   - [Compaction](#compaction)
   - [Storage Engine Internals](#storage-engine-internals)
   - [Ecosystem & Operations](#ecosystem--operations)
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

Status: Draft

Authors:

* [Bruno Cadonna](https://github.com/cadonna)

## Summary

RFC-0024 introduced named segments and the per-segment LSM state held in the
manifest, but did not expose a way to enumerate which segments a database
contains. This RFC proposes a read-only API,
`list_segments(DurabilityLevel) -> Result<Vec<Bytes>>`, on `DbMetadataOps`
(implemented by `Db` and `DbReader`). The API returns the existing segments in
the database instance, deduplicated and sorted in ascending prefix order.
A `DurabilityLevel` filter distinguishes prefixes already persisted in the
manifest from prefixes also present in in-memory memtables.
A `DbReader` accepts an optional segment extractor so that it can re-derive
prefixes for WAL-replayed entries.

## Motivation

This RFC is motivated from our use of SlateDB segments in OpenData's
Timeseries database. However, the use case is general enough to be
useful for other systems based on SlateDB.

In _Timeseries_, we organize the timeseries data in
time buckets. A time bucket consists of a start time and a duration.
Start time and duration are a prefix to the records contained in the time
bucket. For locality and easier application of retention constraints, we
want to segment the LSM tree as introduced by RFC-0024 according to the
time bucket prefix. Each segment name would contain the start time and
the duration of the time bucket.

Currently, _Timeseries_ maintains a global record with all existing
time buckets that it needs to load before it can start querying. The
global record consists of merge operands that need to be merged when
the set of existing time buckets is read. For each time bucket that
is created or deleted a merge operand is added to the global record.
Added merge operands contain the start time and the duration of the
time bucket that is updated. That is the same information
that is contained in the segment name for each bucket. So, if
we could list the segments that exist in SlateDB, we could get rid
of the global record with all its merge overhead. Additionally,
time buckets would be self-contained which might be a useful property
for application of retention constraints and merging time buckets.
Most likely, this features will be implemented during compaction.
By using segment names directly and avoiding the global record, the
compactor does not need to look into the user data, but only into the
database state to execute retention and merges during compaction.

We believe this use case is general enough for a change in SlateDB,
because the use case might apply to every database that organizes its data
into buckets -- be it time-based buckets or buckets based on other
criteria.

## Goals

- Provide a stable public API to list the named segment prefixes a
  database knows about.
- Make the API available on both `Db` (the writer handle) and `DbReader`
  (the read-only handle), through the existing `DbMetadataOps` trait.
- List existing segments that are persisted but also the ones that only exist
  in memory.
- Allow to list only the persisted segments, but also the segments that just
  exist in memory before they are flushed. Re-use the existing `DurabilityLevel`
  enum to distinguish between the two sets of segments.
- Re-use the touched-segment bookkeeping the write path already maintains
  (RFC-0024), so the call is `O(segments + |touched_segments|)` and does
  not scan any keys.
- Add the segment extractor to `DbReader` so that it can identify segments
  that only exist in the replayed WAL records.
- If the reader cannot truthfully fulfil a
  `DurabilityLevel::Memory` request, it must return an error rather than
  a partial list.

## Non-Goals

- Exposing the internal `manifest::Segment` type or any per-segment LSM
  state (L0 count, sorted runs). Callers that need that depth
  can continue to use `Db::manifest()`.
- Returning a value type richer than a list of prefixes. Per-segment
  statistics, sizes, key ranges, etc. are out of scope for this RFC and
  may be added later as separate APIs.
- Defining segment retention or lifecycle policy. This RFC only reports
  state; it does not act on it.
- Migrating existing data or changing storage formats. The API reads
  existing manifest and memtable state.

## Design

### Public API

A new method is declared on `DbMetadataOps` (in `slatedb/src/ops.rs`):

```rust
fn list_segments(
    &self,
    durability_filter: DurabilityLevel,
) -> Result<Vec<Bytes>, crate::Error>;
```

`Db` and `DbReader` implement it. Each also exposes a matching method
so callers who have not imported the trait can use it directly:

```rust
impl Db {
    pub fn list_segments(
        &self,
        durability_filter: DurabilityLevel,
    ) -> Result<Vec<Bytes>, crate::Error>;
}

impl DbReader {
    pub fn list_segments(
        &self,
        durability_filter: DurabilityLevel,
    ) -> Result<Vec<Bytes>, crate::Error>;
}
```

The returned `Vec<Bytes>` is:

- deduplicated — a prefix appearing in more than one source (e.g. both
  the manifest and the active memtable) appears once,
- sorted in ascending prefix order (`Bytes::cmp`),
- empty when the database is unsegmented (no extractor was ever
  configured on the writer).

The `DurabilityLevel` argument is required (no default). The enum is
reused unchanged from the read path: `Remote` and `Memory`.

### Durability semantics

`DurabilityLevel` selects which sources contribute prefixes:

- `DurabilityLevel::Remote` — only segments persisted in the manifest
  (`ManifestCore::segments`). These prefixes are durable in object
  storage.
- `DurabilityLevel::Memory` — Remote, plus prefixes touched by writes
  still held in the active and immutable in-memory memtables (the
  `touched_segments` set maintained by the write path, RFC-0024).

A prefix present in both sources is reported once.

### Reader-side extractor

`DbReaderBuilder` gains an optional `with_segment_extractor` method that
mirrors the writer's:

```rust
impl<P: Into<Path>> DbReaderBuilder<P> {
    pub fn with_segment_extractor(
        self,
        extractor: Arc<dyn PrefixExtractor>,
    ) -> Self;
}
```

The reader uses the extractor for one purpose: when WAL-replayed entries
land in an immutable memtable, the reader re-derives each entry's prefix
through the extractor and records the touched-segment set on the table,
exactly as the writer's startup replay does.

At open time, the reader validates a configured extractor against the
manifest's persisted `segment_extractor_name` by calling
`ManifestCore::validate_extractor_configuration(Some(extractor))`. The
rule matches the writer's:

- the configured name must equal the persisted name, and
- every persisted segment prefix must still be recognized by the
  configured extractor.

Unlike the writer, the reader is permitted to open a segmented database
*without* an extractor. The reader can still serve reads (read routing
uses the manifest's segment list, not the extractor), and
`DurabilityLevel::Remote` listings still work; only the in-memory side
of the listing is affected. The validation is therefore guarded:

```rust
if let Some(extractor) = segment_extractor.as_deref() {
    manifest
        .db_state()
        .validate_extractor_configuration(Some(extractor))?;
}
```

### Memory level without an extractor

A reader that has no extractor configured cannot truthfully fulfil
`DurabilityLevel::Memory`: its WAL-replayed memtables carry no touched
prefixes, so the returned list would silently omit unpersisted segments.
The implementation therefore rejects this combination with a typed
error:

```rust
if durability_filter == DurabilityLevel::Memory
    && self.inner.segment_extractor.is_none()
{
    return Err(SlateDBError::SegmentExtractorRequired.into());
}
```

A new error variant is added:

```rust
#[error("listing in-memory segments requires a configured segment extractor")]
SegmentExtractorRequired,
```

This variant maps to `ErrorKind::Invalid` via `Error::invalid(msg)`,
matching the other extractor-related variants.

The writer (`Db`) never raises this error. The writer's open-time
validation in `DbBuilder::build` already guarantees that a segmented
database has its extractor configured; an unsegmented writer
legitimately returns an empty list at any durability level.
`Db::list_segments` is always `Ok`.


### Worked examples

A writer with the 3-byte fixed extractor, segments persisted for `aaa`,
unpersisted writes to `bbb`:

```rust
db.list_segments(DurabilityLevel::Remote)?; // -> [b"aaa"]
db.list_segments(DurabilityLevel::Memory)?; // -> [b"aaa", b"bbb"]
```

An unsegmented writer:

```rust
db.list_segments(DurabilityLevel::Remote)?; // -> []
db.list_segments(DurabilityLevel::Memory)?; // -> []
```

A reader opened with no extractor against the same segmented database:

```rust
reader.list_segments(DurabilityLevel::Remote)?; // -> [b"aaa"]
reader.list_segments(DurabilityLevel::Memory)
// -> Err(SegmentExtractorRequired) [ErrorKind::Invalid]
```

A reader opened with the matching extractor:

```rust
reader.list_segments(DurabilityLevel::Memory)?; // -> [b"aaa", b"bbb"]
```

## Impact Analysis

SlateDB features and components that this RFC interacts with.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors — adds
  `SlateDBError::SegmentExtractorRequired`, mapped to
  `ErrorKind::Invalid`.

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

- [x] Manifest format — no format change; the new method reads existing
  `ManifestCore::segments` and `segment_extractor_name`.
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

- [x] Write-ahead log (WAL) — the reader now re-derives touched-segment
  prefixes for replayed entries when an extractor is configured. The
  underlying replay machinery is unchanged; the existing helper
  `record_replayed_touched_segments` is now invoked from both the
  writer's and the reader's replay paths.
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc) — bindings that mirror `Db` and
  `DbReader` will need to expose the new method to remain at parity;
  out of scope for the first PR.
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- Latency: `O(N)` where `N` is the number of persisted segments plus the
  number of distinct prefixes touched by memtables. No I/O, no key
  scans.
- Throughput: callers can invoke the method as often as needed; it
  takes one read lock and releases it before collection. Concurrent
  writers are unaffected.
- Object-store requests: none. The call reads only in-memory manifest
  and memtable state.
- Read/write/space amplification: unchanged.

### Observability

- Configuration: a new builder method,
  `DbReaderBuilder::with_segment_extractor`. No new fields on
  `Settings` and no environment variables.
- New components/services: none.
- Metrics: none added. Future work may add per-segment metrics; this
  RFC intentionally limits itself to listing.
- Logging: none added.

### Compatibility

- Existing data on object storage / on-disk formats: no change. The new
  method reads existing manifest fields.
- Existing public APIs: `DbMetadataOps` gains a method. This is a
  breaking change at the trait level for any out-of-crate implementor;
  inside the crate only `Db` and `DbReader` implement the trait.
- Rolling upgrades / mixed-version behaviour: not applicable. The
  change is in-process API only.

## Testing

- Unit tests (`slatedb/src/db_reader.rs`):
  - verify different durability levels
  - verify error is returned when durability level `Memory` but no segment
    extractor provided
  - verify list empty if database unsegmented
- Integration tests (`slatedb/tests/db.rs`):
  - verify in-memory segment prefixes, persisted prefixes, and mixed
  - verify errors
  - verify empty list for unsegmented database
- Fault-injection / chaos: none specific; behaviour is deterministic
  over in-memory state.
- Deterministic simulation: not required.
- Formal methods: not required.
- Performance tests: not required (the operation is
  `O(segments + |touched_segments|)` and performs no I/O).

## Rollout

- Milestones / phases:
  - PR with changes in main rust code
  - PR with changes to bindings
- Feature flags / opt-in: none
- Docs updates: doc comments on `DbMetadataOps::list_segments`,
  `Db::list_segments`, and `DbReader::list_segments`. No website or
  CHANGELOG entry beyond standard release notes.

## Alternatives

**Two separate methods (`list_persisted_segments` and
`list_all_segments`).**
Rejected. Two methods double the API surface and would still need
documentation explaining what "all" means in the absence of an
extractor. A single method with a typed `DurabilityLevel` argument
reuses an enum callers already know from `ReadOptions`.

**Silently only list persistent segments for `DbReader` `Memory` without an extractor.**
Rejected. Returning only persisted segments while the filter requests
"also memory" produces a result the caller cannot distinguish from a
fully correct one. Failing with a typed error forces the caller to
either configure the reader correctly or downgrade to `Remote`.

**Strict reader validation (reject configured `None` against a
segmented database).**
Rejected. Matching the writer's strictness here would break readers
that currently open segmented databases without an extractor purely
to read. The proposed rule preserves that capability and only
enforces the extractor when the reader actually asks for the
in-memory list.

**Do nothing.**
Rejected. Without a dedicated API, callers can subscribe to
manifest changes, but miss the segments that are only in memory.

## Open Questions

None at this point.

## References

- [RFC-0024: Segment-Oriented Compaction](./0024-segment-oriented-compaction.md)
  — defines named segments, the prefix extractor, per-segment LSM
  state in the manifest, and the `touched_segments` write-path
  bookkeeping that this RFC reads from.
- [RFC-0007: API errors](./0007-api-errors.md) — establishes the
  `ErrorKind`/`Error::invalid` conventions used by the new
  `SegmentExtractorRequired` variant.