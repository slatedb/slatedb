# Flush Notifier

Table of Contents:

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Trait Definition](#trait-definition)
  - [Builder Integration](#builder-integration)
  - [Call Sites](#call-sites)
  - [Error Handling](#error-handling)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)

Status: Draft

Authors: Pierre Barre

## Summary

Add a `FlushNotifier` trait with two methods — `approve_flush` (pre-flush) and `on_flush_completed` (post-flush) — that SlateDB calls around each flush from memory to the object store. Together these give external systems full visibility into the flush lifecycle: the pre-flush hook lets them block until they are ready, and the post-flush hook confirms the data is now durable. When no notifier is configured, flush behavior is unchanged.

## Motivation

Systems that replicate SlateDB writes whether through a simple active/passive pair, a consensus protocol, or any other scheme typically maintain their own log of writes. This log serves two purposes: recovery after crashes, and bringing replicas up to date.

The replication layer needs to know when it is safe to truncate this log. A log entry can only be discarded once the corresponding data is durably stored outside the log i.e., once SlateDB has flushed it from the memtable to the object store. But today, the replication layer has no visibility into what is in SlateDB's memtable versus what has been flushed. Without this information:

- **Truncate too early**: If the replication layer discards log entries before SlateDB has flushed that data, a crash loses the data. It was only in the memtable (volatile) and is no longer in the log.
- **Truncate too late (or never)**: If the replication layer plays it safe and keeps everything, the log grows without bound.

Solving this requires two pieces of information at two different moments:

1. **Before the flush** (`approve_flush`): The replication layer may need to block the flush until its own bookkeeping is complete. For example, ensuring the replication log has been sent to replicas up to the sequence number about to be flushed. Without this, data could leave the memtable before the replication layer is ready, creating a window where a crash loses data that was only in volatile memory and hadn't been replicated yet.

2. **After the flush** (`on_flush_completed`): The replication layer needs confirmation that data up to a given sequence number is now durable in the object store. Only then can it safely truncate its log. A pre-flush hook alone is insufficient because the flush could fail after approval. The data would still be only in the memtable, but the replication layer might incorrectly believe it's safe to truncate.

### Concrete example

Consider a replicated system (active/passive or otherwise) where both nodes share a replication log and use SlateDB for storage:

```
Client -> Active node -> apply to SlateDB memtable
                      -> append to replication log -> Passive node
```

Full lifecycle:

```
1. SlateDB decides to flush data through seq n
2. approve_flush(n)       -> replication layer ensures log is
                              replicated through seq n
                           -> returns Ok(())
3. SlateDB writes data to object store
4. on_flush_completed(n)  -> replication layer records that seq n
                              is now durable in the object store
                           -> replication layer truncates log <= n
```

```rust
struct ReplicationFlushNotifier { log: Arc<ReplicationLog> }

#[async_trait]
impl FlushNotifier for ReplicationFlushNotifier {
    async fn approve_flush(&self, last_seq: u64) -> Result<(), slatedb::Error> {
        // Ensure our replication log has been sent to replicas up to
        // this point before allowing the data to leave the memtable.
        self.log.ensure_replicated(last_seq).await
            .map_err(|e| slatedb::Error::unavailable(e.to_string()))
    }

    async fn on_flush_completed(&self, last_seq: u64) {
        // Data is now durable in the object store. Safe to truncate.
        self.log.truncate_through(last_seq);
    }
}
```

## Goals

- Provide a pre-flush hook that fires before every flush from memory to the object store (both WAL flushes and memtable-to-L0 flushes).
- Allow the pre-flush hook to block, delaying the flush until the external system is ready.
- Allow the pre-flush hook to return an error, aborting the flush.
- Provide a post-flush hook that fires after every successful flush, confirming the highest sequence number now durable in the object store.
- Zero behavior change when no notifier is configured.
- Minimal API surface: a single trait with two methods.

## Non-Goals

- **Selective flush control.** The notifier cannot choose *which* data to flush or skip; it can only approve or reject the entire flush.
- **Replacing the WAL.** The notifier is not a replacement for the WAL. It is a coordination mechanism that works alongside the WAL.
- **Multi-writer coordination.** This is a single-writer hook. Multi-writer scenarios are out of scope.

## Design

### Trait Definition

A new file `slatedb/src/flush_notifier.rs`:

```rust
use async_trait::async_trait;
use crate::error::Error;

/// Coordinates with external systems around SlateDB's flush lifecycle.
///
/// External systems (e.g. replication layers) can implement this trait
/// to be notified before and after data is flushed from memory to the
/// object store.
///
/// When no `FlushNotifier` is configured, flushes proceed immediately.
#[async_trait]
pub trait FlushNotifier: Send + Sync {
    /// Called before data is flushed from memory to the object store.
    ///
    /// `last_seq` is the highest sequence number about to be flushed.
    /// Implementations can block to delay the flush (e.g. wait for
    /// replication to catch up).
    ///
    /// Returning `Ok(())` allows the flush to proceed. Returning an error
    /// aborts the flush.
    async fn approve_flush(&self, last_seq: u64) -> Result<(), Error>;

    /// Called after data has been successfully flushed to the object store.
    ///
    /// `last_seq` is the highest sequence number that is now durable in
    /// the object store. The implementation can use this to safely advance
    /// its own truncation point.
    ///
    /// This method cannot fail or block the flush path. If the
    /// implementation needs to do async work, it should spawn a task.
    fn on_flush_completed(&self, last_seq: u64);
}
```

The trait uses the public `Error` type (not internal `SlateDBError`) for `approve_flush` since it is part of the public API and will be implemented by external users.

`on_flush_completed` is synchronous (`fn`, not `async fn`) and infallible. The flush has already succeeded — there is nothing to await or fail. If the implementation needs to do async work (e.g. sending a message to a replica), it should spawn its own task.

### Builder Integration

The `FlushNotifier` is configured through `DbBuilder`:

```rust
impl<P: Into<Path>> DbBuilder<P> {
    /// Sets a flush notifier for coordinating with external systems
    /// around SlateDB's flush lifecycle.
    pub fn with_flush_notifier(mut self, notifier: Arc<dyn FlushNotifier>) -> Self {
        self.flush_notifier = Some(notifier);
        self
    }
}
```

Internally, the notifier is stored as `Option<Arc<dyn FlushNotifier>>` in both `DbInner` and `WalBufferManager`.

### Call Sites

The notifier is called at two points in the flush lifecycle. At each point, `approve_flush` is called before the flush and `on_flush_completed` is called after a successful flush.

#### 1. WAL flush (`wal_buffer.rs` — `do_flush_one_wal`)

```rust
async fn do_flush_one_wal(&self, wal_id: u64, wal: Arc<WalBuffer>) -> Result<(), SlateDBError> {
    let last_seq = wal.last_seq().unwrap_or(0);

    // Pre-flush: ask for approval
    if let Some(notifier) = &self.flush_notifier {
        notifier
            .approve_flush(last_seq)
            .await
            .map_err(|_| SlateDBError::FlushNotifierError)?;
    }

    // ... existing flush logic (build SST, write to object store) ...

    // Post-flush: confirm durability
    if let Some(notifier) = &self.flush_notifier {
        notifier.on_flush_completed(last_seq);
    }

    Ok(())
}
```

#### 2. Memtable-to-L0 flush (`mem_table_flush.rs` — `flush_imm_memtables_to_l0`)

The pre-flush call is placed after the WAL durability check (WAL is already durable, but no irreversible L0 work has started). The post-flush call is placed after the manifest has been successfully written:

```rust
async fn flush_imm_memtables_to_l0(&mut self) -> Result<(), SlateDBError> {
    while let Some(imm_memtable) = /* ... */ {
        // WAL durability check (existing code)
        if self.db_inner.wal_enabled {
            // ... ensure WAL is durable ...
        }

        // Pre-flush: ask for approval
        if let Some(notifier) = &self.db_inner.flush_notifier {
            let last_seq = imm_memtable.table().last_seq().unwrap_or(0);
            notifier
                .approve_flush(last_seq)
                .await
                .map_err(|_| SlateDBError::FlushNotifierError)?;
        }

        // Build and write L0 SST, update manifest (existing code)
        // ...

        // Post-flush: confirm durability (after manifest write succeeds)
        if let Some(notifier) = &self.db_inner.flush_notifier {
            let last_seq = imm_memtable.table().last_seq()
                .expect("flush of l0 with no entries");
            notifier.on_flush_completed(last_seq);
        }
    }
}
```

### Error Handling

A new internal error variant is added:

```rust
// In error.rs
pub(crate) enum SlateDBError {
    // ...
    #[error("flush notifier rejected flush")]
    FlushNotifierError,
}
```

This maps to `Error::unavailable(...)` in the public error type, following the same pattern as `BlockTransformError`.

When `approve_flush` returns an error:
- For WAL flushes: the WAL buffer's flush handler propagates the error. The WAL data remains in memory and will be retried on the next flush attempt. `on_flush_completed` is **not** called.
- For L0 flushes: the memtable flusher propagates the error. The immutable memtable remains in the queue and will be retried. `on_flush_completed` is **not** called.

`on_flush_completed` is only called after a successful flush. It cannot fail.

## Impact Analysis

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [x] Sequence numbers

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

- [x] Write-ahead log (WAL)
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

- **Latency**: When no notifier is configured, there is zero overhead (a single `Option::is_none()` check per hook). When a notifier is configured, flush latency increases by the time the notifier takes to approve. The post-flush callback is synchronous and should be fast. This is intentional and controlled by the user.
- **Throughput**: A slow `approve_flush` will back-pressure writes by delaying flushes. Memory usage will grow as unflushed data accumulates. The existing `max_unflushed_bytes` setting provides a natural bound.

### Observability

- **Configuration changes**: New `with_flush_notifier()` builder method.

### Compatibility

- **Public APIs**: Additive only. New `FlushNotifier` trait and `with_flush_notifier()` builder method. No existing APIs are modified.

## Testing

- **Unit tests**: A test with a recording `FlushNotifier` that verifies:
  - `approve_flush` is called before both WAL and L0 flushes.
  - `on_flush_completed` is called after both WAL and L0 flushes.
  - `on_flush_completed` is NOT called when a flush fails.
  - The `last_seq` argument is correct (> 0, matches expected sequence numbers).
  - `on_flush_completed(seq)` is always preceded by `approve_flush(seq)` with the same sequence number.
- **Integration tests**: A test with a blocking `FlushNotifier` that verifies:
  - Blocking in `approve_flush` delays the flush (the flush future does not complete until the notifier returns).
  - The flush completes successfully after the notifier approves.
- **Error tests**: A test with a rejecting `FlushNotifier` that verifies:
  - Returning an error from `approve_flush` propagates through the flush path.
  - `on_flush_completed` is not called when `approve_flush` returns an error.
- **Existing tests**: All existing tests pass without modification (no notifier = no behavior change).

## Rollout

- **Milestones**: Single PR implementing the trait, integration points, and tests.
- **Feature flags**: None needed. The feature is opt-in via `with_flush_notifier()`. When not configured, behavior is identical to today.
- **Docs updates**: Document the `FlushNotifier` trait and `with_flush_notifier()` builder method in API docs.

## Alternatives

### 1. Non-blocking notification (fire-and-forget) for pre-flush

Instead of blocking, the pre-flush notifier would just be informed that a flush is about to happen.

**Rejected because**: This introduces races. The flush could complete before the replication layer finishes its pre-flush bookkeeping (e.g. ensuring the log is replicated), creating a window where a crash could cause data loss. A blocking hook guarantees the replication layer's state is consistent before the flush proceeds.

### 2. Pre-flush hook only (no post-flush)

Provide only `approve_flush`, no `on_flush_completed`.

**Rejected because**: The pre-flush hook tells the replication layer what is *about to* be flushed, but not what *has been* flushed. The flush could fail after approval. Without a post-flush confirmation, the replication layer cannot safely truncate its log, it would have to guess or poll, both of which are fragile.

### 3. Post-flush hook only (no pre-flush)

Provide only `on_flush_completed`, no `approve_flush`.

**Rejected because**: Without a pre-flush hook, the replication layer cannot delay a flush. If SlateDB flushes data before the replication layer has finished replicating it, a crash could lose data that was only in the memtable and hadn't been replicated yet. The pre-flush hook is necessary to ensure the replication log is durable before data leaves the memtable.

### 4. Sequence-number-based callback registration

Instead of a trait, allow registering callbacks for specific sequence number ranges.

**Rejected because**: More complex API, harder to reason about, and doesn't match the mental model of flush lifecycle hooks. The trait-based approach is simpler and more flexible.

### 5. Status quo (do nothing)

**Rejected because**: There is no way for external systems to observe SlateDB's flush lifecycle. Replication layers have no visibility into what is in the memtable versus what has been flushed, so they cannot determine when it is safe to truncate their logs. Users must resort to workarounds like disabling automatic flushes entirely and managing flush timing externally, which is fragile and loses SlateDB's built-in flush optimizations.

## Open Questions

1. **Should `on_flush_completed` be async?** The current design makes it synchronous to avoid adding latency to the flush path. Implementations that need async work can spawn a task. If a compelling use case for async post-flush work arises, this could be revisited.
