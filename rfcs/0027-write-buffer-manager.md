# Write Buffer Manager

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Phase 1: Live Write Buffer Capacity Enforcement](#phase-1-live-write-buffer-capacity-enforcement)
    - [ByteBudgetSemaphore](#bytebudgetsemaphore)
    - [ByteBufferManager](#bytebuffermanager)
    - [ByteBufferPermit](#bytebufferpermit)
    - [Integration into the Write Path](#integration-into-the-write-path)
    - [Memtable Permit Tracking](#memtable-permit-tracking)
    - [WAL Replay](#wal-replay)
    - [Backpressure Enhancement](#backpressure-enhancement)
    - [Public API and Builder](#public-api-and-builder)
  - [Phase 2: Instance Registry for Intelligent Backpressure (WIP)](#phase-2-instance-registry-for-intelligent-backpressure-wip)
- [Pathological Cases](#pathological-cases)
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
- [Updates](#updates)

<!-- TOC end -->

Status: Implemented

Authors:

* [Zach Schoenberger](https://github.com/zach-schoenberger)

## Summary

This RFC introduces a `ByteBufferManager` — a byte-budget primitive — used to
enforce a per-instance memory budget on in-flight write data. Each SlateDB
instance creates its own `ByteBufferManager` internally; callers do not need to
provide or configure one in Phase 1.

Phase 1 tracks and bounds the memory consumed by write batches and memtables,
complementing the existing `max_unflushed_bytes` backpressure with an explicit
acquire/release mechanism tied to memtable flushes. Phase 2 introduces
`DbBuilder::with_write_buffer_manager()` so that multiple instances can share a
budget, along with an instance registry for intelligent backpressure.

## Motivation

SlateDB's existing backpressure mechanism does a good job of keeping memory
usage in check under most workloads: it polls the aggregate size of WAL +
immutable memtables against `max_unflushed_bytes` and stalls writers when the
threshold is exceeded. Today the check is a point-in-time snapshot
of memory already consumed. Between the check passing and the write landing
in the memtable, concurrent writers can collectively overshoot the intended
budget. A proactive reservation step would let us bound memory *before* the
write is dispatched.

Separately, the current backpressure check reads the database state under a lock
to aggregate sizes. A lock-free tracking primitive would scale more
naturally as we add new pools.

The `ByteBufferManager` addresses these opportunities by:

- **Tracking** the memory reserved by each write batch via a non-blocking
  `force_acquire` that accounts for bytes immediately at dispatch time.
- **Blocking** new writes via backpressure when the budget's high watermark is
  reached, adding an additional layer of protection against memory pressure from
  bursty workloads.
- Automatically **releasing** permits when the owning memtable is dropped
  (i.e., after flush to L0).
- Using lock-free atomics for all budget tracking, complementing the existing
  state-lock-based accounting with a contention-free path.

## Goals

- Enforce a global memory budget on in-flight write data (write batches through
  memtable flush).
- Provide an RAII permit lifecycle: acquire before write, release on memtable
  drop.
- Complement (not replace) the existing `max_unflushed_bytes` backpressure.
- Avoid locking the database state for budget tracking.
- (Phase 2) Allow callers to share a budget across multiple DB instances via
  `DbBuilder::with_write_buffer_manager()` and establish a registry pattern
  for intelligent backpressure.

## Non-Goals

- Replacing the existing `max_unflushed_bytes` backpressure mechanism entirely.
- Tracking block cache or read-path memory.
- Providing per-writer or per-key granularity on budget allocation.
- Guaranteeing exact byte-level accounting (estimates are conservative
  approximations).
- (Phase 2) Per-instance tracking and intelligent backpressure policies — this
  RFC only outlines the direction.

## Design

### Phase 1: Live Write Buffer Capacity Enforcement

Phase 1 introduces three new types and integrates them into the existing write
path.

#### ByteBudgetSemaphore

The core primitive is an async semaphore built on `AtomicUsize` + `tokio::Notify`
that tracks allocations in bytes rather than discrete permits.

```slatedb/slatedb/src/byte_buffer_manager.rs#L174-L178
struct ByteBudgetSemaphore {
    notify: Notify,
    allocated_bytes: AtomicUsize,
    capacity: usize,
}
```

**Why not `tokio::sync::Semaphore`?** Tokio's semaphore enforces a hard capacity
limit — once all permits are issued, acquisitions block until permits are
returned. The `ByteBufferManager` intentionally uses soft capacity tracking:
normal `acquire` calls allow a single writer to overshoot capacity rather than
requiring the reservation to fit within remaining headroom. A custom
`ByteBudgetSemaphore` gives us full control over this soft-cap behavior, which
`tokio::sync::Semaphore` does not support.

The semaphore exposes the following operations:

```slatedb/slatedb/src/byte_buffer_manager.rs#L180-L190
impl ByteBudgetSemaphore {
    fn new(capacity: usize) -> Self;
    async fn acquire(&self, num_bytes: usize);
    fn try_acquire(&self, num_bytes: usize) -> bool;
    fn force_acquire(&self, num_bytes: usize);
    fn release(&self, num_bytes: usize);
    fn available(&self) -> usize;
    fn allocated(&self) -> usize;
    async fn wait_for_allocated_below(&self, num_bytes: usize);
}
```

- **`acquire`** — spins on a CAS loop. When `allocated_bytes >= capacity`, the
  caller awaits a `Notify` signal that fires when any permit is released. On
  success, `allocated_bytes` is incremented by `num_bytes`.
- **`try_acquire`** — non-blocking CAS. Returns `true` if capacity was
  available and the reservation succeeded, `false` otherwise.
- **`force_acquire`** — non-blocking `fetch_add`. Can push `allocated_bytes`
  above `capacity`. Used for WAL replay and the normal write dispatch path.
- **`release`** — subtracts `num_bytes` from `allocated_bytes` via CAS and
  notifies waiters if the budget drops below capacity.
- **`wait_for_allocated_below`** — blocks until allocated bytes drop below the
  given threshold (used by `await_capacity`).

#### ByteBufferManager

A cloneable, generic byte-budget handle wrapping a shared
`Arc<ByteBudgetSemaphore>`. In the write path it is stored
as the `write_buffer_manager` field on `DbInner` because this instance
specifically tracks write buffer memory.

```slatedb/slatedb/src/byte_buffer_manager.rs#L14-L18
#[derive(Clone)]
pub struct ByteBufferManager {
    inner: Arc<ByteBudgetSemaphore>,
    high_watermark: usize,
}
```

The `high_watermark` defines the threshold at which `at_capacity()` returns
`true` and `await_capacity()` blocks. This allows the backpressure system to
trigger before the hard capacity limit is reached, giving the flush pipeline
time to drain.

Methods:

```slatedb/slatedb/src/byte_buffer_manager.rs#L20-L108
impl ByteBufferManager {
    pub fn new(capacity: usize, high_watermark: usize) -> Self;
    pub async fn acquire(&self, num_bytes: usize) -> ByteBufferPermit;
    pub fn try_acquire(&self, num_bytes: usize) -> Option<ByteBufferPermit>;
    pub fn force_acquire(&self, num_bytes: usize) -> ByteBufferPermit;
    pub fn available(&self) -> usize;
    pub fn capacity(&self) -> usize;
    pub fn at_capacity(&self) -> bool;
    pub async fn await_capacity(&self);
}
```

- **`acquire`** — blocks (async) until `allocated_bytes < capacity`, then
  atomically increments `allocated_bytes` by `num_bytes` and returns a permit.
- **`try_acquire`** — attempts to reserve without blocking; returns
  `Some(permit)` if capacity was available, or `None` if exhausted.
- **`force_acquire`** — unconditionally reserves bytes (for dispatch and
  replay). Can push `allocated_bytes` above `capacity`.
- **`available`** — returns `capacity - allocated_bytes` (saturating).
- **`capacity`** — returns the total byte budget capacity.
- **`at_capacity`** — returns `true` if `allocated_bytes >= high_watermark`.
- **`await_capacity`** — waits until allocated bytes drop below the high
  watermark. Does not reserve any bytes.

#### ByteBufferPermit

An RAII guard that releases its byte reservation on drop. Multiple permits can
be consolidated via `merge()` to combine reservations into a single guard.

```slatedb/slatedb/src/byte_buffer_manager.rs#L117-L171
pub struct ByteBufferPermit {
    semaphore: Arc<ByteBudgetSemaphore>,
    reserved_bytes: AtomicUsize,
}

impl ByteBufferPermit {
    pub fn merge(&self, other: &ByteBufferPermit);
    pub fn size(&self) -> usize;
}

impl Drop for ByteBufferPermit {
    fn drop(&mut self); // calls semaphore.release(reserved_bytes)
}
```

The `merge()` operation atomically zeroes the source permit's `reserved_bytes`
and adds that value to the target. This means the source permit's `Drop` is a
no-op, avoiding double-release. This is critical because multiple write batches
can land in the same active memtable, and their permits need to be consolidated
into a single permit that releases when the memtable is dropped.

#### Integration into the Write Path

The permit lifecycle flows through the write path as follows:

```/dev/null/diagram.txt#L1-L14
Writer                  DbInner                      Memtable
  │                       │                            │
  ├─ write_with_options ─►│                            │
  │                       ├─ force_acquire(size)       │
  │                       ├─ batch.set_write_buffer    │
  │                       ├─ dispatch to writer ──────►│
  │                       │                            ├─ add_write_permit
  │                       │                            │  (merge into table)
  │                       ├─ maybe_apply_backpressure  │
  │                       │  (blocks if at_capacity)   │
  │                       │                            │
  │                       │           flush to L0 ────►│ drop memtable
  │                       │                            │ └─ permit.drop()
  │                       │                            │    (releases budget)
```

1. **`WriteBatch`** gains a `write_buffer_permit: Option<Arc<ByteBufferPermit>>`
   field. The `set_write_buffer()` method attaches a permit to the batch.
   `force_acquire` is used because the bytes technically have already been
   allocated. By getting the permit and dispatching to the writer without
   blocking, we guarantee progress can be made. By applying backpressure after
   the write dispatch, we enforce the soft buffer limit.

2. **`DbInner::write_with_options`** calls `force_acquire(estimated_size)` to
   immediately account for the write's memory footprint, attaches the permit to
   the batch, dispatches the batch to the writer, and *then* calls
   `maybe_apply_backpressure()`. This "acquire-then-backpressure" ordering
   ensures the write is tracked before checking whether the system should stall.
   Using `force_acquire` (non-blocking) avoids holding up the dispatch path;
   backpressure is applied reactively after the write is in-flight.

3. **`DbInner::write_entries_to_memtable`** passes the permit to the memtable
   via `add_write_permit`.

#### Memtable Permit Tracking

`KVTable` stores a `OnceCell<Arc<ByteBufferPermit>>`. The first permit is
set directly. Subsequent permits (from concurrent batches landing in the same
memtable) are merged into the existing one via `ByteBufferPermit::merge()`.
This ensures a single release when the table is dropped after flush.

```slatedb/slatedb/src/mem_table.rs#L112-L113
write_buffer_permit: OnceCell<Arc<ByteBufferPermit>>,
```

```slatedb/slatedb/src/mem_table.rs#L571-L576
pub(crate) fn add_write_permit(&self, permit: Arc<ByteBufferPermit>) {
    if let Err(permit) = self.write_buffer_permit.set(permit) {
        self.write_buffer_permit.get().unwrap().merge(&permit);
    }
}
```

#### WAL Replay

During WAL replay, the replay loop must make forward progress to populate the
memtable state. The budget is acquired via `force_acquire` for each replayed
memtable and the resulting permit is attached to it. After each replayed
memtable is integrated, `maybe_apply_backpressure` is called, allowing it to
stall if the budget is exceeded (which triggers flushes of already-replayed
memtables to drain the overage).

#### Backpressure Enhancement

`maybe_apply_backpressure` now has two independent trigger conditions, checked
in sequence:

1. **Existing condition** — `total_mem_size_bytes >= max_unflushed_bytes`:
   waits for the oldest immutable memtable to be uploaded or the oldest WAL to
   be flushed before re-checking.

2. **New condition** — `write_buffer_manager.at_capacity()` (allocated bytes >=
   high watermark): freezes the current memtable to accelerate flushing, then
   awaits `write_buffer_manager.await_capacity()` which blocks until allocated
   bytes drop below the high watermark.

```/dev/null/backpressure.rs#L1-L18
// Condition 1: existing max_unflushed_bytes check
if total_mem_size_bytes >= self.settings.max_unflushed_bytes {
    // wait for oldest memtable upload or WAL flush...
    continue;
}

// Condition 2: write buffer high watermark
if self.write_buffer_manager.at_capacity() {
    let await_capacity = self.write_buffer_manager.await_capacity();
    self.freeze_current_memtable()?;

    tokio::select! {
        _ = await_capacity => { return Ok(()); }
        result = await_closed => result?,
        _ = timeout_fut => { /* 30s timeout warning */ }
    };
    continue;
}
```

The write-buffer remaining budget is also logged in trace and warn messages
for observability.

#### Public API and Builder

- `ByteBufferManager` is re-exported from `lib.rs` as a public type.
- Each instance creates its own `ByteBufferManager` internally with both
  `capacity` and `high_watermark` set to `settings.max_unflushed_bytes`.
- In Phase 2, `DbBuilder` will expose `with_write_buffer_manager()` for
  callers who want to share a budget across multiple DB instances.

### Phase 2: Instance Registry for Intelligent Backpressure (WIP)

Phase 1 enforces budget limits but applies backpressure blindly — when the
budget is exhausted, all writers block regardless of which DB instance holds the
majority of the allocation. Phase 2 proposes a registry pattern that enables
smarter backpressure strategies by tracking which instances share the budget.

**Problem:** When a single `ByteBufferManager` is shared across multiple DB
instances via `DbBuilder::with_write_buffer_manager()`, an instance that has
consumed a disproportionate share of the budget causes all other instances to
stall. Without per-instance tracking there is no mechanism to identify the
heavy consumer or to direct backpressure at it specifically.

**Proposed direction:** Introduce `DbBuilder::with_write_buffer_manager()` to
allow sharing a `ByteBufferManager` across instances, and add an instance
registry within the manager:

- **Registration** — Each DB instance registers itself with the shared
  `ByteBufferManager` on startup and deregisters on shutdown. The registry
  tracks per-instance metadata such as current byte allocation and instance
  identity.
- **Per-instance accounting** — Permits are tagged with their owning instance,
  allowing the manager to report per-instance budget consumption.
- **Backpressure policies** — With per-instance visibility, the manager can
  support smarter strategies such as:
  - *Proportional fairness* — stall only the instance that has exceeded its
    fair share rather than blocking all writers.
  - *Priority-based* — allow high-priority instances to preempt or receive
    larger budget slices.
  - *Targeted flush signaling* — notify the heaviest consumer to trigger an
    early flush, freeing budget for other instances.
- **Observability** — The registry enables per-instance metrics (bytes held,
  permits outstanding, time spent blocked) for debugging shared-budget
  contention.

Detailed design of Phase 2 is deferred to a future RFC, once Phase 1 is
validated in production and multi-instance sharing patterns are better
understood from production usage.

## Pathological Cases

When Phase 2 introduces shared budgets across instances, the following
pathological case can arise: 1 to N instances obtain buffer permits so that the
total allocated buffer is slightly under or equal to the high watermark. This
will cause any write by any other instance to always trigger a small memtable to
be flushed. Essentially making each instance write to any of the instances that
don't own a significant portion of the buffer permits a new memtable. In order
to properly handle this kind of issue, the `ByteBufferManager` needs a way to
know what allocations are being held and a mechanism to trigger their release.
Phase 2's instance registry and intelligent backpressure policies are designed
to address this.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that
apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
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

- **Write latency:** Under normal load, the permit acquisition is a single
  atomic `fetch_add` (`force_acquire`) — sub-microsecond overhead. Under budget
  pressure, writes are dispatched immediately but backpressure is applied
  after dispatch, blocking until memtable flushes free capacity. This is the
  desired behavior: controlled backpressure rather than OOM.
- **Write throughput:** No change under steady-state. Under burst, throughput
  is capped by flush rate, which is the correct bottleneck.
- **Read latency/throughput:** No impact. The write-buffer manager is not on
  the read path.
- **Object-store requests:** No change. Flush behavior is unchanged.
- **Space/read/write amplification:** No change. Data layout is unchanged.

### Observability

- **Configuration changes:** Phase 1 has no new configuration — the budget is
  derived from `max_unflushed_bytes`. Phase 2 adds optional
  `with_write_buffer_manager()` on `DbBuilder`.
- **New components:** `ByteBufferManager` (public), `ByteBufferPermit`
  (public), `ByteBudgetSemaphore` (internal).
- **Metrics:** The existing `backpressure_count` metric now also fires when
  the write-buffer budget is exhausted.
- **Logging:** `maybe_apply_backpressure` trace and warn messages now include
  `write_buffer_remaining` to show the remaining byte budget.

### Compatibility

- **Existing data on object storage / on-disk formats:** No change. This is
  purely an in-memory tracking mechanism.
- **Existing public APIs:** The `ByteBufferManager` is a new public type.
  No existing APIs are changed or removed. `DbBuilder` gains a new optional
  method in Phase 2.
- **Rolling upgrades:** Not applicable — this is a client-side, in-memory
  mechanism with no wire protocol or storage format changes.

## Testing

- **Unit tests:** Comprehensive tests for `ByteBudgetSemaphore` and
  `ByteBufferManager` covering:
  - Full budget availability on creation.
  - Budget reduction on acquire.
  - Budget restoration on permit drop.
  - Blocking when budget is exhausted.
  - Unblocking after permit drop.
  - `merge()` combining sizes correctly.
  - `merge()` preventing double-release on source drop.
  - `merge()` panicking on cross-manager merge.
  - Zero-sized permit drop safety.
  - Multi-permit merge and release.
- **Integration tests:** Existing `DbInner` tests in `manifest_writer`,
  `tracker`, and `uploader` modules are updated to construct the
  `ByteBufferManager` and pass it through, validating that the write path
  compiles and executes correctly end-to-end.
- **Fault-injection/chaos tests:** Not in Phase 1. A future phase could
  inject failures into the flush path to verify that permits are not leaked.
- **Deterministic simulation tests:** Not in Phase 1.
- **Formal methods verification:** Not planned.
- **Performance tests:** Manual benchmarking under bursty write workloads to
  verify that the write-buffer budget prevents memory overshoot without
  degrading steady-state throughput.

## Rollout

- Milestones / phases:
  - **Phase 1:** Land `ByteBufferManager`, `ByteBufferPermit`,
    `ByteBudgetSemaphore`, and write-path integration. Each instance creates
    its own manager with budget equal to `max_unflushed_bytes`.
  - **Phase 2:** Add `DbBuilder::with_write_buffer_manager()` for shared
    budgets, instance registry, per-instance accounting, and intelligent
    backpressure policies.
- Feature flags / opt-in:
  - Phase 1 is always active with a per-instance default budget.
  - Phase 2 is opt-in via `DbBuilder::with_write_buffer_manager()`.
- Docs updates:
  - `ByteBufferManager` and `ByteBufferPermit` type-level documentation.
  - (Phase 2) `DbBuilder` API docs for `with_write_buffer_manager()`.
  - (Phase 2) Operational guidance on tuning the budget for multi-instance
    deployments.

## Alternatives

**Status quo — rely solely on `max_unflushed_bytes`**

The existing backpressure mechanism works well for steady-state workloads. Under
highly concurrent bursty writers, however, there can be a gap between the
point-in-time size check and the actual write, during which total memory usage
may temporarily exceed the intended budget. The `ByteBufferManager` narrows
that gap by tracking writes immediately via `force_acquire` and then applying
backpressure reactively when the high watermark is reached.

**Use `tokio::sync::Semaphore`**

Tokio's semaphore is well-tested and supports async acquisition. However, it
enforces a hard capacity limit: once all permits are issued, further
acquisitions block until permits are returned, and there is no way to
over-allocate. The `ByteBufferManager` relies on soft capacity tracking —
normal `acquire` lets a single writer's reservation overshoot rather than
requiring it to fit within remaining headroom. A custom
`ByteBudgetSemaphore` gives us full control over this soft-cap behavior, which
`tokio::sync::Semaphore` does not support.

**Integrate budget tracking into the database state lock**

Instead of a separate atomic semaphore, we could track the budget inside the
`RwLock`-protected `DbState`. This would avoid introducing a new primitive,
but it would add reservation and release operations to the state lock's
critical section. The lock-free approach keeps budget tracking off the
state-lock path entirely, which is a better fit as more resource pools are
added over time.

**Acquire the permit via async `acquire` before dispatch**

An earlier design proposed using an async `acquire` call before dispatching the
write, which would block the writer until budget was available. The implemented
design instead uses `force_acquire` (non-blocking) to immediately track the
write, dispatches it, and then applies backpressure reactively via
`maybe_apply_backpressure`. This ordering ensures writes are never lost or
reordered due to budget contention, and separates the concerns of tracking
(how much are we using?) from backpressure (should we slow down?).

**Per-writer budgets instead of a global budget**

We could assign each writer its own slice of the budget, avoiding contention
entirely. This would be more complex to configure and would not handle
heterogeneous write sizes well (one writer with large batches would exhaust its
slice while another's sits idle). A global budget with atomic CAS is simple and
handles skewed workloads naturally.

## Open Questions

- What is the right default budget and high watermark? Phase 1 uses
  `max_unflushed_bytes` for both, but this may be too generous or too
  restrictive depending on the workload. Should the budget be a separate
  setting or a fraction of `max_unflushed_bytes`? Should the high watermark
  default to something less than capacity (e.g. 80%)?
- For Phase 2, what backpressure policy should be the default when multiple
  instances share a budget? Proportional fairness, priority-based, or
  something simpler?

## References

- [Issue #1669: Better Memory Management With A WriteBufferManager](https://github.com/slatedb/slatedb/issues/1669)
- [PR #1 (prototype): adding the primitive](https://github.com/zach-schoenberger/slatedb/pull/1)
- RocksDB [`WriteBufferManager`](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager)
  — prior art for global memtable memory budgeting in an LSM engine.

## Updates

- **2025-07-11:** Updated to reflect the implemented design. Renamed
  `WriteBufferManager` → `ByteBufferManager` and `WriteBufferPermit` →
  `ByteBufferPermit`. Updated write path integration to reflect the
  `force_acquire` → dispatch → `maybe_apply_backpressure` ordering. Added
  `high_watermark` field and `at_capacity()`/`await_capacity()` methods.
  Updated backpressure section to describe the two-condition check with
  memtable freezing. Status changed from Draft to Implemented.
