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

Status: Draft

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

This RFC introduces a public API for user-provided compaction filters in SlateDB. Users implement a `CompactionFilterSupplier` that creates a `CompactionFilter` instance for each compaction job. Each filter can inspect entries and decide to keep, drop, convert to tombstone, or modify values. The design makes existing internal types (`RowEntry`, `ValueDeletable`) public and uses `CompactorStateView` to provide context to the filter.

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
- Design that can potentially be extended to enable internal TTL filtering
refactoring.

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
    /// Convert the entry to a tombstone.
    ///
    /// Use this instead of Drop when you need to shadow older versions of the same
    /// key in lower levels. The tombstone will prevent "resurrection" of deleted data.
    ///
    /// Note: For merge operands, Tombstone behaves the same as Drop (the operand is
    /// simply removed without creating a tombstone marker).
    Tombstone,
    /// Modify the entry's value. Key and metadata remain unchanged.
    ///
    /// Note: If applied to a tombstone, the entry becomes a regular value with
    /// the tombstone's sequence number, effectively resurrecting the key with a new value.
    Modify(Bytes),
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
    /// Filter a single entry. Should be fast (synchronous, no I/O).
    ///
    /// The method is infallible to keep it simple and fast.
    fn filter(&mut self, entry: &RowEntry) -> CompactionFilterDecision;

    /// Called after processing all entries.
    ///
    /// Use this hook to flush state, log statistics, or clean up resources.
    /// This method is infallible since compaction output has already been written.
    async fn on_compaction_end(&mut self, state: &CompactorStateView);
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
    /// * `state` - Read-only view of the compactor state (manifest, compactions)
    /// * `is_dest_last_run` - Whether the destination sorted run is the last (oldest) run.
    ///   When true, tombstones can be safely dropped since there are no older versions below.
    async fn create_compaction_filter(
        &self,
        state: &CompactorStateView,
        is_dest_last_run: bool,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError>;
}
```

### Error Type

```rust
/// Errors that can occur during compaction filter operations.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum CompactionFilterError {
    /// Filter creation failed in `create_compaction_filter`. This aborts the compaction.
    #[error("filter creation failed: {0}")]
    CreationError(String),
}
```

### Configuration

```rust
// In DbBuilder (db/builder.rs)
pub fn with_compaction_filter_supplier(
    mut self,
    supplier: Arc<dyn CompactionFilterSupplier>,
) -> Self {
    self.compaction_filter_supplier = Some(supplier);
    self
}
```

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

| Method                       | Error Behavior                                     |
|------------------------------|----------------------------------------------------|
| `create_compaction_filter()` | Returns `Result` - errors abort compaction job     |
| `filter()`                   | Infallible - filters must handle errors internally |
| `on_compaction_end()`        | Infallible - cleanup cannot fail compaction        |

Creating a fresh filter instance per compaction provides:

1. **Isolation**: No shared mutable state between compaction jobs.
2. **Single-threaded execution**: Filter runs on the same thread as the
compactor, no synchronization needed.
3. **State tracking**: Filters can safely accumulate statistics or state across
all entries in a compaction.
4. **Simplified reasoning**: No concurrent access concerns within a filter.

### Performance Considerations

The `filter()` method is called for every entry during compaction, so it should
be fast with minimal overhead possible.

**For CPU-intensive filters:**

If your filter requires expensive computation (e.g., complex parsing, or
decoding), consider one of these approaches:

1. **Use a dedicated compaction runtime**: Configure SlateDB to run compaction
   on a separate async runtime using `DbBuilder::with_compaction_runtime()`.
   This prevents filter work from blocking your application's main runtime.

   ```rust
   let compaction_runtime = tokio::runtime::Builder::new_multi_thread()
       .worker_threads(2)
       .thread_name("compaction")
       .build()?;

   let db = Db::builder("mydb", object_store)
       .with_compaction_runtime(compaction_runtime.handle().clone())
       .with_compaction_filter_supplier(Arc::new(MyExpensiveFilter))
       .build()
       .await?;
   ```

2. **Use `spawn_blocking` for heavy computation**: If only some entries require
   expensive processing, you can offload that work to a blocking thread pool.
   However, note that `filter()` is synchronous, so you'd need to restructure
   your approach (e.g., batch processing in `on_compaction_end`).

### Usage Example

```rust
use slatedb::{
    CompactionFilter, CompactionFilterSupplier, CompactorStateView,
    CompactionFilterDecision, CompactionFilterError, RowEntry,
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
    fn filter(&mut self, entry: &RowEntry) -> CompactionFilterDecision {
        if entry.key.starts_with(&self.prefix) {
            self.dropped_count += 1;
            // Use Tombstone to shadow older versions in lower levels
            return CompactionFilterDecision::Tombstone;
        }

        CompactionFilterDecision::Keep
    }

    async fn on_compaction_end(&mut self, _state: &CompactorStateView) {
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
        _state: &CompactorStateView,
        _is_dest_last_run: bool,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
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

- **Latency**: `filter()` is synchronous in hot path - implementations must be fast
- **Throughput**: Async supplier allows I/O for setup/teardown without blocking compaction
- **Object-store requests**: No impact; filters operate on in-memory data
- **Space amplification**: `Drop`/`Tombstone` decisions reduce space; `Modify` may increase or decrease.
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

### 1. Async `filter()` method

Adds per-entry overhead. Users needing I/O should batch in `create_compaction_filter()`.

### 2. Replace RetentionIterator entirely

**Deferred**: Built-in retention handles subtle edge cases (snapshot barriers, merge operand expiration). Could be unified in future.

### 3. Batched filter API (all versions of a key at once)

Instead of `filter(entry) -> Decision`, provide `filter(Vec<RowEntry>) -> Vec<Decision>` where all versions of a key are passed together. This would:
- Enable look-ahead logic (matching `RetentionIterator`'s current implementation)
- Allow filters to make decisions based on the full version history

**Deferred**: Adds API complexity and potential allocations. We're starting simple with entry-at-a-time filtering, which covers most use cases. The API can evolve if batched filtering becomes necessary.

### 4. Single filter instance (no factory/supplier)

Supplier provides isolation between jobs and enables single-threaded execution without synchronization.

### 5. Define custom types instead of using RowEntry

Using existing types (`RowEntry`, `CompactorStateView`) reduces API surface and avoids per-entry allocations for wrapper types.

### 7. Built-in filter chaining

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
