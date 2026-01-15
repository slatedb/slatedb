# SlateDB Compaction Filters

Table of Contents:

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
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

This RFC introduces a public API for user-provided compaction filters in SlateDB. Users implement a `CompactionFilterFactory` that creates a `CompactionFilter` instance for each compaction job. Each filter can inspect entries and decide to keep, drop, or modify them. Filters execute on the compactor thread with async lifecycle hooks for setup and teardown.

## Motivation

SlateDB has no public API for custom compaction filters. Users need this capability for:

1. **Custom TTL Logic**: Application-specific expiration beyond built-in TTL support.
2. **MVCC Garbage Collection**: Custom policies for user-defined versioning.
3. **Schema Migrations**: Data format conversions during compaction.

This design could also unify internal TTL handling (`RetentionIterator`) with user-provided filters in the future.

## Goals

- Provide a user-friendly API following established SlateDB patterns (`MergeOperator`).
- Zero overhead when no filter is configured - existing users are unaffected.
- Async factory method for setup, async hook for teardown.
- Access to essential compaction metadata.
- Design that could potentially be used for internal TTL filtering.

## Non-Goals

- Replacing the internal `RetentionIterator` immediately.
- Modifying the key bytes of an entry (filters can only modify values).
- Emitting new entries during compaction (filters can only keep, drop, or modify existing entries).

## Design

### Core Traits

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
    /// Modify the entry's value. Key and metadata remain unchanged.
    Modify(Bytes),
}

/// Metadata about the current compaction job.
pub struct CompactionContext {
    /// Destination sorted run ID.
    pub destination: u32,
    /// Logical clock tick for this compaction (used for TTL decisions).
    pub compaction_ts: i64,
    /// Whether this compaction is targeting the bottommost (oldest) sorted run.
    /// When true, there are no older versions of keys below this.
    pub is_bottommost: bool,
}

/// Additional attributes of an entry being filtered.
pub struct EntryAttributes {
    /// The sequence number of this entry.
    pub seq: u64,
    /// The creation timestamp (if set).
    pub create_ts: Option<i64>,
    /// The expiration timestamp (if set).
    pub expire_ts: Option<i64>,
    /// Whether this entry is a merge operand (resolved without a base value).
    pub is_merge: bool,
}

/// Filter that processes entries during compaction.
///
/// Each filter instance is created for a single compaction job and executes
/// on the same thread as the compactor.
#[async_trait]
pub trait CompactionFilter: Send + Sync {
    /// Filter a single entry. Should be fast (synchronous, no I/O).
    ///
    /// This method is called for every entry in the compaction. Keep it
    /// efficient to avoid impacting compaction throughput.
    ///
    /// The method is infallible to force it to be as simple as possible.
    fn filter(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        attrs: &EntryAttributes,
    ) -> CompactionFilterDecision;

    /// Called after processing all entries.
    ///
    /// Use this hook to flush state, log statistics, or clean up resources.
    /// This method is infallible since compaction output has already been written.
    async fn on_compaction_end(&mut self, context: &CompactionContext);
}

/// Factory that creates a CompactionFilter instance per compaction job.
///
/// The factory is shared across all compactions and must be thread-safe (`Send + Sync`).
/// It creates a new filter instance for each compaction job, providing
/// isolated state per job. The async factory method allows for I/O during
/// filter initialization (e.g., loading configuration, connecting to services).
#[async_trait]
pub trait CompactionFilterFactory: Send + Sync {
    /// Create a filter for a compaction job. Return Err to abort compaction.
    ///
    /// This is the place to perform any async initialization (loading config,
    /// connecting to external services, etc.) before the filter processes entries.
    async fn create_compaction_filter(
        &self,
        context: &CompactionContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterInitError>;
}

```

### Error Type

```rust
/// Error returned when filter initialization fails. This aborts the compaction.
#[derive(Debug, Clone, thiserror::Error)]
#[error("compaction filter initialization failed: {0}")]
pub struct CompactionFilterInitError(pub String);
```

### Configuration

```rust
// In DbBuilder (db/builder.rs)
pub fn with_compaction_filter_factory(
    mut self,
    factory: Arc<dyn CompactionFilterFactory>,
) -> Self {
    self.compaction_filter_factory = Some(factory);
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
   is where your `CompactionFilter::filter()` method is called for each entry.

This ordering ensures:

1. Merge operands are resolved before filtering.
2. Expired entries and old versions are already removed.
3. User filters only see "live" entries that would otherwise be written.

### Entry Types and Filter Behavior

The filter behaves differently for different entry types:

- **Values**: Filter is invoked. Normal key-value entries (`is_merge = false`).
- **Merge operands**: Filter is invoked. Entries where merge operands were resolved
  without a base value (`is_merge = true`).
- **Tombstones**: Filter is **skipped**. Passed through unchanged.

**Why skip tombstones?** Tombstones are deletion markers that must be preserved to
shadow older versions of the same key in lower levels. Allowing filters to modify
or drop tombstones could cause deleted data to "resurrect."

### Potential for Internal TTL Unification

Although user filters run after `RetentionIterator`, the `CompactionFilter`
trait is designed to be general enough that internal TTL filtering could
potentially be refactored to use the same abstraction.

The current `RetentionIterator` handles concerns like (snapshot barriers, version retention)
that would need to be handled in the filter.

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

### Usage Example

```rust
use slatedb::{CompactionFilter, CompactionFilterFactory, CompactionContext,
              CompactionFilterDecision, EntryAttributes, CompactionFilterInitError};
use std::sync::Arc;
use async_trait::async_trait;

/// A filter that drops all keys with a specific prefix.
struct PrefixDropFilter {
    prefix: Vec<u8>,
    dropped_count: u64,  // Can track state since we're single-threaded
}

#[async_trait]
impl CompactionFilter for PrefixDropFilter {
    fn filter(&mut self, key: &[u8], _value: Option<&[u8]>, _attrs: &EntryAttributes)
        -> CompactionFilterDecision {
        if key.starts_with(&self.prefix) {
            self.dropped_count += 1;
            CompactionFilterDecision::Drop
        } else {
            CompactionFilterDecision::Keep
        }
    }

    async fn on_compaction_end(&mut self, ctx: &CompactionContext) {
        tracing::info!(
            "Compaction {} dropped {} entries with prefix",
            ctx.destination,
            self.dropped_count
        );
    }
}

struct PrefixDropFilterFactory {
    prefix: Vec<u8>,
}

#[async_trait]
impl CompactionFilterFactory for PrefixDropFilterFactory {
    async fn create_compaction_filter(&self, _ctx: &CompactionContext)
        -> Result<Box<dyn CompactionFilter>, CompactionFilterInitError> {
        Ok(Box::new(PrefixDropFilter {
            prefix: self.prefix.clone(),
            dropped_count: 0,
        }))
    }
}

// Usage
let db = Db::builder("mydb", object_store)
    .with_compaction_filter_factory(Arc::new(PrefixDropFilterFactory {
        prefix: b"test:".to_vec(),
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

- [ ] Transactions
- [x] Snapshots - filters should be aware that dropped entries will impact snapshots.
- [x] Sequence numbers - provided in EntryAttributes.

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
- [x] Garbage collection - Drop decisions remove entries
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
- **Throughput**: Async lifecycle hooks allow I/O for setup/teardown without blocking compaction
- **Object-store requests**: No impact; filters operate on in-memory data
- **Space amplification**: `Drop` decisions reduce space; `Modify` may increase or decrease.
- **Zero overhead when disabled**: Users who do not configure a filter are not impacted.

Well-implemented filters have minimal overhead on compaction throughput.

### Observability

- **Metrics**: No new counters.
- **Logging**: Filter errors logged at WARN level.
- **Configuration changes**: New `compaction_filter_factory` field in Settings.

### Compatibility

- **Existing data**: no change.
- **Public APIs**: New optional configuration in `Settings` and `DbBuilder`.
- **Rolling upgrades**: not needed.

## Testing

- **Unit tests**: Each decision type (Keep/Drop/Modify), error handling, lifecycle hooks.
- **Integration tests**: End-to-end compaction with custom filters, verify data correctness.
- **Fault-injection tests**: Filter errors, initialization failures.
- **Deterministic simulation tests**: Include filter behavior in DST.
- **Performance tests**: Benchmark compaction throughput with/without filters.

## Rollout

### Implementation

- Core traits and iterator integration.
- Basic tests and documentation.

### Feature Flags

No feature flags needed. It's opt-in via configuration.

### Docs Updates

- Add examples to API documentation.
- Update compaction documentation to describe filter integration point.

## Alternatives

### 1. Async `filter()` method

Adds per-entry overhead. Users needing I/O should batch in `on_compaction_start()`.

### 2. Replace RetentionIterator entirely

**Deferred**: Built-in retention handles subtle edge cases (snapshot barriers, merge operand expiration). Could be unified in future.

### 3. Single filter instance (no factory)

Factory provides isolation between jobs and enables single-threaded execution without synchronization.

### 5. Drop converts to tombstone

Having `Drop` convert to tombstone reduces the usefulness of the filter.
Users who want tombstone behavior can call the delete API directly.

### 6. Built-in filter chaining

Users who need multiple filters can implement a single `CompactionFilter` that internally chains multiple filters. This keeps SlateDB simpler while still enabling advanced use cases.

## Open Questions

1. Should internal TTL filtering be migrated to use this API?

## References

- [GitHub Issue #225](https://github.com/slatedb/slatedb/issues/225) - Original feature request
- [GitHub PR #224](https://github.com/slatedb/slatedb/pull/224) - Previous draft implementation (closed)
- [RFC-0003: Timestamps and TTL](0003-timestamps-and-ttl.md) - Built-in TTL implementation
- [RFC-0006: Merge Operator](0006-merge-operator.md) - Pattern for user-provided operators
- [RocksDB Compaction Filter](https://github.com/facebook/rocksdb/wiki/Compaction-Filter) - Reference implementation

## Updates

*Log major changes to this RFC over time.*
