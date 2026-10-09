# Prepared Memtable Flushes

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [API](#api)
  - [Prepare](#prepare)
  - [Publish](#publish)
  - [Ordering](#ordering)
  - [Failures and Cancellation](#failures-and-cancellation)
  - [Recovery and Garbage Collection](#recovery-and-garbage-collection)
- [Implementation](#implementation)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
  - [Performance and Cost](#performance-and-cost)
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

* [Pierre Barre](https://github.com/Barre)

## Summary

This RFC adds a two-phase memtable flush API. `Db::prepare_flush` freezes the
current memtable and uploads its L0 SSTs, but does not publish them in the
manifest. It returns a handle whose `publish` method advances the manifest
through the sequence captured by that prepare call.

Applications can use the gap between prepare and publish to make related data
durable outside SlateDB. Existing memtable flush behavior remains available as
prepare followed immediately by publish.

## Motivation

Some applications store large payloads in separate immutable objects and keep
only references to them in SlateDB. Those objects must be durable before a
SlateDB manifest exposes the references to secondary readers.

Today the safe ordering is serial:

```text
upload external objects -> flush SlateDB memtable
```

The external objects and SlateDB SSTs are independent until the manifest is
updated, so their uploads can run concurrently:

```text
external object upload -----+
                             +-> publish manifest
SlateDB SST upload ----------+
```

SlateDB already separates parallel SST upload from ordered manifest updates
inside the memtable flusher. This RFC exposes that boundary without exposing
SST IDs or manifest internals.

## Goals

- Run external durability work concurrently with SlateDB SST upload.
- Keep prepared SSTs out of the manifest until the caller publishes them.
- Publish through the exact sequence captured by `prepare_flush`, excluding
  later writes.
- Preserve sequence ordering, WAL durability ordering, and atomic publication
  of all segment SSTs produced by one memtable.
- Keep existing flush APIs and storage formats compatible.

## Non-Goals

- Provide an atomic transaction across SlateDB and an external object store.
- Add a two-phase WAL flush. WAL flushes do not publish L0 metadata.
- Undo an automatic memtable flush that was published before
  `prepare_flush` installed its barrier.
- Expose the first version through language bindings.

## Design

### API

```rust
/// An uploaded memtable flush waiting for manifest publication.
///
/// This handle is not Clone. Dropping it without publishing marks the Db
/// failed rather than making the prepared data visible.
pub struct PreparedFlush {
    // private
}

impl Db {
    pub async fn prepare_flush(&self) -> Result<PreparedFlush, Error>;
}

impl PreparedFlush {
    /// Highest sequence number included in this flush.
    pub fn seqnum(&self) -> u64;

    /// Publish the prepared SSTs and wait for the manifest update.
    pub async fn publish(self) -> Result<(), Error>;
}
```

`prepare_flush` always means a memtable flush. It does not take
`FlushOptions`, because `FlushType::Wal` has no publication phase.

The initial implementation allows one outstanding `PreparedFlush` per `Db`.
A second call returns an error. This restriction can be relaxed later without
changing the handle API.

Example:

```rust
let (prepared, external_result) = tokio::join!(
    db.prepare_flush(),
    upload_external_segments(),
);

let prepared = prepared?;
if let Err(error) = external_result {
    // Keep the handle alive while retrying the external upload.
    retry_external_segments(error).await?;
}
prepared.publish().await?;
```

### Prepare

`prepare_flush`:

1. Installs a barrier in the manifest writer.
2. Sends a flush through the serialized batch writer. This freezes the active
   memtable and establishes the sequence cut.
3. Flushes the WAL when enabled, preserving the existing rule that WAL data is
   durable before a manifest can reference the corresponding L0 SSTs.
4. Waits for all SST uploads through the sequence cut.
5. Returns `PreparedFlush` without updating the manifest.

The barrier must be installed before freezing the memtable. Otherwise a fast
SST upload could reach the manifest writer before `prepare_flush` returns.

An empty prepare returns a no-op handle. Publishing it does not write a new
manifest.

### Publish

`publish` authorizes one manifest update and waits for it to become durable.
That update includes all unpublished memtables through the handle's sequence
cut and no later memtables. Concurrent compaction changes may be merged into
the same manifest as usual.

After the update succeeds, the barrier is removed. Later uploaded memtables
may then be published by the normal background pipeline.

### Ordering

While a handle is outstanding:

- Writes and later SST uploads may continue.
- The manifest cannot advance through the prepared sequence or any later
  sequence.
- `FlushType::Wal` continues to work.
- Another memtable flush, `CheckpointScope::All`, or a close that requests a
  memtable flush returns an error.
- `CheckpointScope::Durable` may checkpoint the already-published state.

`flush_with_options(FlushType::MemTable)` uses the same path with immediate
publication. `Db::flush()` keeps its current behavior: it flushes the WAL when
WAL is enabled, and its WAL-disabled path flushes the memtable.

The barrier cannot retract a manifest update that completed before it was
installed. Callers relying on this API for external references must call
`prepare_flush` before the relevant memtable can be automatically published.

### Failures and Cancellation

SST upload and manifest failures use the existing flusher error handling.
Prepared SSTs are not published by the current `Db` after a prepare failure.

Dropping a non-empty handle without calling `publish` does not publish it.
SlateDB marks the `Db` failed so a later automatic flush cannot expose data
whose external durability is unknown. Cancelling `prepare_flush` after its
barrier has been installed has the same result.

Calling `publish` grants permission to publish. If its future is then
cancelled, the manifest update may still complete. At that point the caller
has already asserted that the external objects are durable.

There is no `abort` method. SlateDB cannot roll back accepted writes, WAL
records, or values already observed by local readers. If an external upload
fails, the caller should keep the handle and retry or repair the upload.

### Recovery and Garbage Collection

The handle and barrier are process-local. A crash before publish leaves the
uploaded SSTs unreferenced, and normal SST garbage collection eventually
removes them.

## Implementation

The existing pipeline is:

```text
FlushTracker -> parallel Uploader -> ordered ManifestWriter
```

The implementation adds a single optional prepared boundary to this pipeline:

- `FlushTracker` captures the target sequence and waits for uploads through it.
- `ManifestWriter` records the barrier and does not retire L0s past it.
- `publish` authorizes a manifest batch capped at the target sequence.
- Immutable memtables remain in `DbState` until publication, so existing read
  behavior and memory backpressure continue to work.
- Ordinary memtable flush uses the same code path with immediate publication.

## Impact Analysis

SlateDB features and components that this RFC interacts with.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [x] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

CDC behavior does not change, but CDC may observe WAL data before `publish`.

### Metadata, Coordination, and Lifecycles

- [x] Manifest format
- [x] Checkpoints
- [ ] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

The manifest encoding does not change.

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
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance and Cost

If external upload takes `E`, SST upload takes `S`, and manifest publication
takes `M`, latency changes from roughly `E + S + M` to `max(E, S) + M`.
Object-store request counts do not change.

A long-lived handle delays secondary-reader visibility and retains immutable
memtables and L0 reservations, so normal write backpressure may apply.

### Observability

Add counters for prepare, publish, and abandoned handles, plus a gauge for an
outstanding prepared flush. Logs include the sequence cut but no user data.

### Compatibility

The API is additive and the on-storage formats are unchanged. Existing flush
behavior is unchanged when no prepared handle exists. Mixed-version readers
only see ordinary published manifests and require no changes.

## Testing

- Verify that uploaded SSTs are absent from the manifest before `publish`.
- Verify that publication stops at the captured sequence when later writes and
  uploads exist.
- Verify WAL-before-manifest ordering with WAL enabled.
- Verify atomic publication when one memtable produces multiple segment SSTs.
- Test empty prepares, concurrent prepare rejection, dropped handles,
  cancellation, checkpoints, and close.
- Fault-inject SST upload failures, manifest failures, and sequenced manifest
  conflicts.
- Cover the state machine in deterministic simulation tests.

## Rollout

1. Add the internal barrier and make ordinary memtable flush use immediate
   prepare and publish.
2. Add fault-injection and deterministic tests.
3. Expose the Rust API and metrics.

## Alternatives

### Keep the Operations Serialized

This preserves ordering but pays the sum of two independent upload latencies.

### Invoke a User Callback Before Manifest Publication

A callback invoked after SST upload cannot overlap the uploads. Starting it
earlier requires a second completion signal, which is the handle proposed here
with less explicit ownership and cancellation behavior.

## Open Questions

None.

## References

- [RFC-0001: Manifest](0001-manifest.md)
- [RFC-0007: API Errors](0007-api-errors.md)
- [RFC-0024: Segment-Oriented Compaction](0024-segment-oriented-compaction.md)
- [RFC-0029: GC-Safe SST ULID Timestamps](0029-gc-safe-sst-ulid-timestamps.md)
- [RFC-0030: Pluggable WAL](0030-pluggable-wal.md)
