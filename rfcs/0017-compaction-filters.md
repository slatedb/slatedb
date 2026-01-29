# SlateDB Compaction Filters

Table of Contents:

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
- [Limitations](#limitations)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

Status: Accepted

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

This RFC introduces a public API for user-provided compaction filters in SlateDB. Users implement a `CompactionFilterSupplier` that creates a `CompactionFilter` instance for each compaction job. Each filter can inspect entries and decide to keep, drop, convert to tombstone, or modify values. The design makes existing internal types (`RowEntry`, `ValueDeletable`) public and uses `CompactionJobContext` to provide context to the filter.

Compaction filters are gated behind the `compaction_filters` feature flag. Enabling this feature may affect snapshot consistency. See [Limitations](#limitations) for details.

## Motivation

SlateDB has no public API for custom compaction filters. Users need this capability for:

1. **Custom TTL Logic**: Application-specific expiration beyond built-in TTL support.
2. **MVCC Garbage Collection**: Custom policies for user-defined versioning.
3. **Schema Migrations**: Data format conversions during compaction.

This design could also unify internal TTL handling (`RetentionIterator`) with user-provided filters in the future.

## Goals

- Provide a user-friendly API following established SlateDB patterns (`CompactionSchedulerSupplier`).
- Zero overhead when no filter is configured. Existing users are unaffected.
- Design that can potentially be extended to enable internal TTL filtering.

## Non-Goals

- Replacing the internal `RetentionIterator` immediately.
- Modifying the key bytes of an entry key (filters can only modify values).
- Emitting new entries during compaction (filters can only keep, drop, tombstone, or modify existing entries).
- Guaranteeing snapshot consistency when compaction filters are enabled (see [Limitations](#limitations)).

## Design

### Public Types

The following existing internal types are made public to support compaction filters:

```rust
/// Entry in the LSM tree (made public).
pub struct RowEntry {
    pub key: Bytes,
    pub value: ValueDeletable,
    pub seq: u64,
    pub create_ts: Option<i64>,
    pub expire_ts: Option<i64>,
}

/// Value that can be a value, merge operand, or tombstone (made public).
pub enum ValueDeletable {
    Value(Bytes),
    Merge(Bytes),
    Tombstone,
}
```

### CompactionJobContext

```rust
/// Context information about a compaction job.
///
/// This struct provides read-only information about the current compaction job
/// to help filters make informed decisions.
pub struct CompactionJobContext {
    /// The destination sorted run ID for this compaction.
    pub destination: u32,
    /// Whether the destination sorted run is the last (oldest) run after compaction.
    /// When true, tombstones can be safely dropped since there are no older versions below.
    pub is_dest_last_run: bool,
    /// The logical clock tick representing the logical time the compaction occurs.
    /// This is used to make decisions about retention of expiring records.
    pub compaction_clock_tick: i64,
    /// Optional minimum sequence number to retain.
    ///
    /// Entries with sequence numbers at or above this threshold are protected by
    /// active snapshots. Dropping or modifying such entries may cause snapshot
    /// reads to return inconsistent results.
    pub retention_min_seq: Option<u64>,
}
```

### CompactionFilterDecision

```rust
/// Decision returned by a compaction filter for each entry.
pub enum CompactionFilterDecision {
    /// Keep the entry unchanged.
    Keep,
    /// Drop the entry entirely. The entry will not appear in the compaction output.
    ///
    /// WARNING: Dropping an entry removes it completely without leaving a tombstone.
    /// This means older versions of the same key in lower levels of the LSM tree
    /// may become visible again ("resurrection"). Only use Drop when you are certain
    /// there are no older versions that could resurface, or when that behavior is
    /// acceptable for your use case.
    Drop,
    /// Modify the entry's value.
    ///
    /// Pass `ValueDeletable::Tombstone` to convert the entry to a tombstone.
    /// When converting to a tombstone, the entry's `expire_ts` is automatically cleared.
    ///
    /// Pass `ValueDeletable::Value(bytes)` to change the value. Key and other
    /// metadata remain unchanged.
    ///
    /// Note: If `Value` is applied to a tombstone, the entry becomes a regular value
    /// with the tombstone's sequence number, effectively resurrecting the key.
    Modify(ValueDeletable),
}
```

### CompactionFilter Trait

```rust
/// Filter that processes entries during compaction.
///
/// Each filter instance is created for a single compaction job and executes
/// single-threaded on the compactor thread. The filter must be `Send + Sync`
/// to satisfy iterator trait requirements.
#[async_trait]
pub trait CompactionFilter: Send + Sync {
    /// Filter a single entry.
    ///
    /// Return `Ok(decision)` to keep, drop, or modify the entry.
    /// Return `Err(FilterError)` to abort the compaction job.
    ///
    /// This method is async to allow I/O operations (e.g., checking external
    /// services, loading configuration). However, for best performance, prefer
    /// doing I/O in `create_compaction_filter()` or `on_compaction_end()` when
    /// possible, since this method is called for every entry.
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, FilterError>;

    /// Called after processing all entries.
    ///
    /// Use this hook to flush state, log statistics, or clean up resources.
    /// This method is infallible since compaction output has already been written.
    async fn on_compaction_end(&mut self);
}
```

### CompactionFilterSupplier Trait

```rust
/// Supplier that creates a CompactionFilter instance per compaction job.
///
/// The supplier is shared across all compactions and must be thread-safe (`Send + Sync`).
/// It creates a new filter instance for each compaction job, providing isolated state per job.
#[async_trait]
pub trait CompactionFilterSupplier: Send + Sync {
    /// Create a filter for a compaction job. Return Err to abort compaction.
    ///
    /// This is async to allow I/O during initialization (loading config,
    /// connecting to external services, etc.) before the filter processes entries.
    ///
    /// # Arguments
    ///
    /// * `context` - Context about the compaction job (destination, clock tick, etc.)
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CreationError>;
}
```

### Error Types

```rust
/// Error returned by `create_compaction_filter`. Aborts the compaction job.
#[derive(Debug, Error)]
#[error("filter creation failed: {0}")]
pub struct CreationError(#[source] pub Box<dyn std::error::Error + Send + Sync>);

/// Error returned by `filter`. Aborts the compaction job.
#[derive(Debug, Error)]
#[error("filter error: {0}")]
pub struct FilterError(#[source] pub Box<dyn std::error::Error + Send + Sync>);

/// Container for all compaction filter errors.
///
/// Used internally by the compactor to handle errors from both
/// filter creation and per-entry filtering.
#[derive(Debug, Error)]
pub enum CompactionFilterError {
    #[error(transparent)]
    Creation(#[from] CreationError),
    #[error(transparent)]
    Filter(#[from] FilterError),
}
```

These error types wrap the underlying cause, preserving error chains for debugging. The `#[source]` attribute enables `std::error::Error::source()` to return the wrapped error. The `CompactionFilterError` enum provides a unified type for the compactor to handle all filter-related errors.

### Configuration

The `CompactionFilterSupplier` is configured on the component that runs compaction:

```rust
// In DbBuilder (db/builder.rs) - for embedded compactor
pub fn with_compaction_filter_supplier(
    mut self,
    supplier: Arc<dyn CompactionFilterSupplier>,
) -> Self {
    self.compaction_filter_supplier = Some(supplier);
    self
}

// In CompactorBuilder (db/builder.rs) - for standalone compactor
pub fn with_compaction_filter_supplier(
    mut self,
    supplier: Arc<dyn CompactionFilterSupplier>,
) -> Self {
    self.compaction_filter_supplier = Some(supplier);
    self
}
```

When running a standalone compactor (separate from the DB writer), user needs
to ensure the `CompactorBuilder` is configured with the same `CompactionFilterSupplier` as the `DbBuilder`.

### Background: How SlateDB Compaction Works

For a comprehensive overview of SlateDB's compaction design, see
[RFC-0002: Compaction](0002-compaction.md).

SlateDB uses an LSM-tree architecture with two main storage layers:

1. **L0 (Level 0)**: Recently flushed SSTs from the memtable. These may have
   overlapping key ranges.
2. **Sorted Runs**: Compacted SSTs organized into sorted runs, each containing
   non-overlapping key ranges. Sorted runs are identified by ID, where lower
   IDs contain older data.

```
┌─────────────────────────────────────────────────┐
│  L0 (newest data)                               │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                │
│  │SST 4│ │SST 3│ │SST 2│ │SST 1│                │
│  └─────┘ └─────┘ └─────┘ └─────┘                │
├─────────────────────────────────────────────────┤
│  Sorted Runs (compacted, older data below)      │
│                                                 │
│  SR 10 (newest)  ──►  SR 5  ──►  SR 0 (oldest)  │
└─────────────────────────────────────────────────┘
```

**Compaction** merges entries from multiple sources (L0 SSTs and/or sorted runs)
into a single destination sorted run. The compaction executor processes entries
**one at a time** through an iterator pipeline.

The core compaction loop in `execute_compaction_job()` is straightforward:

```rust
while let Some(kv) = all_iter.next_entry().await? {
    current_writer.add(kv).await?;
    // ... handle SST size limits, progress reporting ..etc.
}
```

Each call to `next_entry()` retrieves the next entry from the iterator pipeline,
which handles merging, deduplication, and filtering.

### Iterator Stack Integration

The compaction executor builds an iterator pipeline in `load_iterators()`. Each
layer wraps the previous one, processing entries as they flow through:

```
MergeIterator (L0 + SortedRuns)
    -> MergeOperatorIterator (resolve merge operands)
    -> RetentionIterator (TTL, snapshot retention, tombstone cleanup)
    -> CompactionFilterIterator (user filters)
```

**What each iterator does:**

1. **MergeIterator**: Combines entries from all input sources (L0 SSTs and sorted
   runs) into a single sorted stream. When the same key appears in multiple
   sources, entries are ordered by sequence number (newest first). This is where
   the actual "merge" in merge-sort happens.

2. **MergeOperatorIterator**: Resolves merge operands. If the user of the
database uses a `MergeOperator`, this iterator combines consecutive merge
operands into a single resolved value.

3. **RetentionIterator**: Applies built-in retention policies:
   - Drops expired entries (TTL).
   - Removes old versions not needed by snapshots.
   - Cleans up tombstones at the bottommost level.

4. **CompactionFilterIterator** (this RFC): Applies user-provided filters. This
   is where `CompactionFilter::filter()` method is called for each entry.

This ordering ensures:

1. Merge operands are resolved before filtering.
2. Expired entries and old versions are already removed.
3. User filters only see "live" entries that would otherwise be written.

### Limitations

#### Feature Flag and Snapshot Consistency

Compaction filters are gated behind the `compaction_filters` feature flag:

```toml
[dependencies]
slatedb = { version = "...", features = ["compaction_filters"] }
```

Enabling this feature may affect snapshot consistency.

**Why?**

Protecting snapshot data from arbitrary user filters adds significant complexity.
Not all use cases require snapshot consistency guarantees, so we start simple with
a feature flag to ensure users understand the trade-offs. This design can evolve
if new use cases emerge that require snapshot protection.

RocksDB faced the same challenge and [removed snapshot protection from compaction
filters in v6.0](https://github.com/facebook/rocksdb/wiki/Compaction-Filter),
noting "the feature has a bug which can't be easily fixed."

When using compaction filters with snapshots, be aware that:
- Filters may modify or drop entries that snapshots expect to see
- Snapshot reads may return unexpected results if the filter altered the data
- Users who need consistent snapshots should carefully consider their filter logic

### Potential for Internal TTL Unification

The `CompactionFilter` trait is designed to be general enough that internal TTL
filtering could potentially be refactored to use the same abstraction. However,
the current `RetentionIterator` buffers all versions of a key before applying
retention policies (e.g., keeping boundary values for snapshot consistency).
Unifying these would require either refactoring `RetentionIterator` to work
entry-by-entry, or extending the filter API to receive all versions of a key
at once.

### Error Handling

| Method                       | Error Type      | Behavior                          |
|------------------------------|-----------------|-----------------------------------|
| `create_compaction_filter()` | `CreationError` | Aborts compaction job             |
| `filter()`                   | `FilterError`   | Aborts compaction job             |
| `on_compaction_end()`        | Infallible      | Cleanup cannot fail compaction    |

Creating a fresh filter instance per compaction provides:

1. **Isolation**: No shared mutable state between compaction jobs.
2. **Single-threaded execution**: Filter runs on the same thread as the
compactor, no synchronization needed.
3. **State tracking**: Filters can safely accumulate statistics or state across
all entries in a compaction.
4. **Simplified reasoning**: No concurrent access concerns within a filter.

### Performance Considerations

The `filter()` method is called for every entry during compaction. While the
method is async to allow I/O when needed, frequent I/O per entry can significantly
impact compaction throughput. For best performance:

- **Prefer batching I/O**: Load configuration or external state in
  `create_compaction_filter()` rather than per-entry in `filter()`.
- **Cache decisions**: If checking an external service, cache results to avoid
  repeated calls for similar entries.

**For CPU-intensive filters:**

If your filter performs expensive synchronous computation (e.g., complex parsing,
cryptographic operations), consider using a dedicated compaction runtime to prevent
blocking your application's main runtime:

```rust
let compaction_runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(2)
    .thread_name("compaction")
    .build()?;

let db = Db::builder("mydb", object_store)
    .with_compaction_runtime(compaction_runtime.handle().clone())
    .with_compaction_filter_supplier(Arc::new(MyCpuIntensiveFilter))
    .build()
    .await?;
```


### Usage Example

```rust
use slatedb::{
    CompactionFilter, CompactionFilterSupplier, CompactionJobContext,
    CompactionFilterDecision, CreationError, FilterError, RowEntry, ValueDeletable,
};
use bytes::Bytes;
use std::sync::Arc;
use async_trait::async_trait;

/// A filter that converts all entries with a specific key prefix to tombstones.
struct PrefixDroppingFilter {
    prefix: Bytes,
    dropped_count: u64,
}

#[async_trait]
impl CompactionFilter for PrefixDroppingFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, FilterError> {
        if entry.key.starts_with(&self.prefix) {
            self.dropped_count += 1;
            // Use Tombstone to shadow older versions in lower levels
            return Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone));
        }

        Ok(CompactionFilterDecision::Keep)
    }

    async fn on_compaction_end(&mut self) {
        tracing::info!(
            "Compaction dropped {} entries with prefix {:?}",
            self.dropped_count,
            self.prefix
        );
    }
}

struct PrefixDroppingFilterSupplier {
    prefix: Bytes,
}

#[async_trait]
impl CompactionFilterSupplier for PrefixDroppingFilterSupplier {
    async fn create_compaction_filter(
        &self,
        _context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CreationError> {
        Ok(Box::new(PrefixDroppingFilter {
            prefix: self.prefix.clone(),
            dropped_count: 0,
        }))
    }
}

// Usage
let db = Db::builder("mydb", object_store)
    .with_compaction_filter_supplier(Arc::new(PrefixDroppingFilterSupplier {
        prefix: Bytes::from_static(b"temp:"),
    }))
    .build()
    .await?;
```

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [x] Transactions - **consistency may be affected** when compaction filters are enabled (see [Limitations](#limitations)).
- [x] Snapshots - **consistency may be affected** when compaction filters are enabled (see [Limitations](#limitations)).
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Logical clocks
- [x] Time to live (TTL) - built-in TTL runs before user filters; expired entries are already removed
- [x] Compaction filters - this RFC
- [x] Merge operator - filters run after merge resolution
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection - Drop/Tombstone decisions remove entries
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [x] Compaction filters - this RFC
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

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

## Operations

### Performance & Cost

- **Latency**: `filter()` is async but called per-entry - minimize I/O in hot path
- **Throughput**: For best performance, batch I/O in `create_compaction_filter()` or cache decisions
- **Object-store requests**: No direct impact; filters operate on in-memory data
- **Space amplification**: `Drop`/`Modify(Tombstone)` decisions reduce space; `Modify(Value)` may increase or decrease.
- **Zero overhead when disabled**: Users who do not configure a filter are not impacted.

Well-implemented filters have minimal overhead on compaction throughput.

### Observability

- **Metrics**: No new counters.
- **Logging**: Filter errors logged at WARN level.
- **Configuration changes**: New `compaction_filter_supplier` field in Settings.

### Compatibility

- **Existing data**: no change.
- **Public APIs**: New optional configuration in `Settings` and `DbBuilder`. `RowEntry` and `ValueDeletable` become public.
- **Rolling upgrades**: not needed.

## Testing

- **Unit tests**: Each decision type (Keep/Drop/Tombstone/Modify), error handling, lifecycle hooks.
- **Integration tests**: End-to-end compaction with custom filters, verify data correctness.
- **Fault-injection tests**: Filter errors, initialization failures.
- **Deterministic simulation tests**: Include filter behavior in DST.
- **Performance tests**: Benchmark compaction throughput with/without filters.

## Rollout

### Implementation

- Core traits and iterator integration.
- Make `RowEntry` and `ValueDeletable` public.
- Basic tests and documentation.

### Feature Flags

The `compaction_filters` feature flag gates the `CompactionFilterSupplier` trait.
See [Limitations](#limitations) for why this is behind a feature flag.

### Docs Updates

- Add examples to API documentation.
- Update compaction documentation to describe filter integration point.

## Alternatives

### 1. Replace RetentionIterator entirely

**Deferred**: Built-in retention handles subtle edge cases (snapshot barriers, merge operand expiration). Could be unified in future.

### 2. Batched filter API (all versions of a key at once)

Instead of `filter(entry) -> Decision`, provide `filter(Vec<RowEntry>) -> Vec<Decision>` where all versions of a key are passed together. This would:
- Enable look-ahead logic (matching `RetentionIterator`'s current implementation)
- Allow filters to make decisions based on the full version history

**Deferred**: Adds API complexity and potential allocations. We're starting simple with entry-at-a-time filtering, which covers most use cases. The API can evolve if batched filtering becomes necessary.

### 3. Single filter instance (no factory/supplier)

Supplier provides isolation between jobs and enables single-threaded execution without synchronization.

### 4. Define custom types instead of using RowEntry

Using existing types (`RowEntry`, `CompactionJobContext`) reduces API surface and avoids per-entry allocations for wrapper types.

### 5. Built-in filter chaining

Users who need multiple filters can implement a single `CompactionFilter` that internally chains multiple filters. This keeps SlateDB simpler while still enabling advanced use cases.


## References

- [GitHub Issue #225](https://github.com/slatedb/slatedb/issues/225) - Original feature request
- [GitHub PR #224](https://github.com/slatedb/slatedb/pull/224) - Previous draft implementation (closed)
- [Discord Discussion](https://discord.com/channels/1232385660460204122/1461435444934737940) - Design discussion thread
- [RFC-0003: Timestamps and TTL](0003-timestamps-and-ttl.md) - Built-in TTL implementation
- [RFC-0006: Merge Operator](0006-merge-operator.md) - Pattern for user-provided operators
- [RocksDB Compaction Filter](https://github.com/facebook/rocksdb/wiki/Compaction-Filter) - Reference implementation

## Updates

*Log major changes to this RFC over time.*
