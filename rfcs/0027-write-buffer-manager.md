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
    - [WriteBufferManager](#writebuffermanager)
    - [WriteBufferPermit](#writebufferpermit)
    - [Integration into the Write Path](#integration-into-the-write-path)
    - [Memtable Permit Tracking](#memtable-permit-tracking)
    - [WAL Replay](#wal-replay)
    - [Backpressure Enhancement](#backpressure-enhancement)
    - [Public API and Builder](#public-api-and-builder)
  - [Phase 2: Lock-Free SST and WAL Capacity Tracking](#phase-2-lock-free-sst-and-wal-capacity-tracking)
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

Status: Draft

Authors:

* [Zach Schoenberger](https://github.com/zach-schoenberger)

## Summary

This RFC introduces a `WriteBufferManager` that enforces a global memory budget
on in-flight write data using a permit-based lifecycle. Phase 1 tracks and
bounds the memory consumed by write batches and memtables, complementing the
existing `max_unflushed_bytes` backpressure with an explicit acquire/release
mechanism tied to memtable flushes. Phase 2 outlines how the same pattern can
be extended to SST table and WAL capacity tracking without requiring locks on
the database state.

## Motivation

SlateDB's existing backpressure mechanism does a good job of keeping memory
usage in check under most workloads: it polls the aggregate size of WAL +
immutable memtables against `max_unflushed_bytes` and stalls writers when the
threshold is exceeded. As write concurrency and burst intensity grow, there are
two areas where a tighter feedback loop would help:

1. **Reservation before writing.** Today the check is a point-in-time snapshot
   of memory already consumed. Between the check passing and the write landing
   in the memtable, concurrent writers can collectively overshoot the intended
   budget. A proactive reservation step would let us bound memory *before* the
   write is dispatched.

2. **Release tied to flush.** The budget is recalculated by re-reading live
   memtable and WAL buffer sizes. Adding a direct signal that releases tracked
   memory at the exact moment a memtable is flushed to L0 would make the
   feedback loop tighter and more responsive.

Separately, the current backpressure check reads the database state under a lock
to aggregate sizes. This works well today, but extending the same approach to
additional resource pools (SST block cache memory, WAL buffer memory) would mean
more work under that lock. A lock-free tracking primitive would scale more
naturally as we add new pools.

The `WriteBufferManager` addresses these opportunities by:

- Requiring writers to **acquire** a budget permit (sized to the estimated entry
  footprint) before their batch is dispatched.
- **Blocking** new writes when the budget is exhausted, adding an additional
  layer of protection against memory pressure from bursty workloads.
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
- Support WAL replay without deadlocking.
- Allow callers to share a budget across multiple DB instances or inject a
  custom size via the builder API.
- (Phase 2) Establish a pattern that can be reused for SST and WAL capacity
  tracking.

## Non-Goals

- Replacing the existing `max_unflushed_bytes` backpressure mechanism entirely.
- Tracking block cache or read-path memory.
- Providing per-writer or per-key granularity on budget allocation.
- Guaranteeing exact byte-level accounting (estimates are conservative
  approximations).
- (Phase 2) Full implementation of SST/WAL tracking — this RFC only outlines
  the direction.

## Design

### Phase 1: Live Write Buffer Capacity Enforcement

Phase 1 introduces three new types and integrates them into the existing write
path.

#### ByteBudgetSemaphore

The core primitive is an async semaphore built on `AtomicUsize` + `tokio::Notify`
that tracks allocations in bytes rather than discrete permits.

```slatedb/src/write_buffer_manager.rs#L1-L10
struct ByteBudgetSemaphore {
    notify: Notify,
    allocated_bytes: AtomicUsize,
    capacity: usize,
}
```

**Why not `tokio::sync::Semaphore`?** Tokio's semaphore enforces a hard capacity
limit — once all permits are issued, acquisitions block until permits are
returned. The `WriteBufferManager` intentionally uses soft capacity tracking:
normal `acquire` calls allow a single writer to overshoot capacity rather than
requiring the reservation to fit within remaining headroom. A custom
`ByteBudgetSemaphore` gives us full control over this soft-cap behavior, which
`tokio::sync::Semaphore` does not support.

The semaphore supports one acquisition mode:

- **`acquire(num_bytes)`** — async, spins on a CAS loop. When `allocated_bytes
  >= capacity`, the caller awaits a `Notify` signal that fires when any permit
  is released. On success, `allocated_bytes` is incremented by `num_bytes`.

Release subtracts `num_bytes` from `allocated_bytes` via CAS and notifies
waiters if the budget drops below capacity.

#### WriteBufferManager

A cloneable handle wrapping a shared `Arc<ByteBudgetSemaphore>`. This is the
public entry point.

```slatedb/src/write_buffer_manager.rs#L11-L20
#[derive(Clone)]
pub struct WriteBufferManager {
    inner: Arc<ByteBudgetSemaphore>,
}
```

Methods:

| Method | Blocking | Description |
|--------|----------|-------------|
| `acquire(num_bytes)` | async | Reserves bytes, blocks until budget available |
| `available()` | no | Returns unreserved bytes remaining |

#### WriteBufferPermit

An RAII guard that releases its byte reservation on drop. Multiple permits can
be consolidated via `merge()` to combine reservations into a single guard.

```slatedb/src/write_buffer_manager.rs#L21-L30
pub struct WriteBufferPermit {
    semaphore: Arc<ByteBudgetSemaphore>,
    reserved_bytes: AtomicUsize,
}
```

The `merge()` operation atomically zeroes the source permit's `reserved_bytes`
and adds that value to the target. This means the source permit's `Drop` is a
no-op, avoiding double-release. This is critical because multiple write batches
can land in the same active memtable, and their permits need to be consolidated
into a single permit that releases when the memtable is dropped.

#### Integration into the Write Path

The permit lifecycle flows through the write path as follows:

```/dev/null/diagram.txt#L1-L13
Writer                  DbInner                      Memtable
  │                       │                            │
  ├─ write_with_options ─►│                            │
  │                       ├─ maybe_apply_backpressure  │
  │                       ├─ batch.reserve_write_buffer│
  │                       │  (acquires permit)         │
  │                       ├─ dispatch to writer ──────►│
  │                       │                            ├─ add_write_permit
  │                       │                            │  (merge into table)
  │                       │                            │
  │                       │           flush to L0 ────►│ drop memtable
  │                       │                            │ └─ permit.drop()
  │                       │                            │    (releases budget)
```

1. **`WriteBatch`** gains a `write_buffer_permit: Option<Arc<WriteBufferPermit>>`
   field. The new `reserve_write_buffer()` method acquires a permit sized to the
   batch's `estimated_size()` — a conservative estimate including key bytes,
   value bytes, sequence numbers, and timestamps.

2. **`DbInner::write_with_options`** calls `maybe_apply_backpressure()` first
   (which now also checks `write_buffer_manager.available() == 0`), then calls
   `reserve_write_buffer` on the batch to acquire the permit before dispatching.

3. **`DbInner::write_entries_to_memtable`** passes the permit to the memtable
   via `add_write_permit`.

#### Memtable Permit Tracking

`KVTable` stores a `OnceCell<Arc<WriteBufferPermit>>`. The first permit is
set directly. Subsequent permits (from concurrent batches landing in the same
memtable) are merged into the existing one via `WriteBufferPermit::merge()`.
This ensures a single release when the table is dropped after flush.

```slatedb/src/mem_table.rs#L1-L10
// In KVTable
write_buffer_permit: OnceCell<Arc<WriteBufferPermit>>,

fn add_write_permit(&self, permit: Arc<WriteBufferPermit>) {
    if let Err(permit) = self.write_buffer_permit.set(permit) {
        self.write_buffer_permit.get().unwrap().merge(&permit);
    }
}
```

#### WAL Replay

During WAL replay, the replay loop must make forward progress to populate the
memtable state. The budget is acquired for each replayed memtable and the
resulting permit is attached to it. After replay, `maybe_apply_backpressure`
runs normally and will stall new writes until flushes drain any overage.

#### Backpressure Enhancement

`maybe_apply_backpressure` gains an additional trigger condition:

```/dev/null/backpressure.rs#L1-L4
if total_mem_size_bytes >= self.settings.max_unflushed_bytes
    || write_buffer_remaining == 0
{
    // apply backpressure ...
```

The write-buffer remaining budget is also logged in trace and warn messages
for observability.

#### Public API and Builder

- `WriteBufferManager` is re-exported from `lib.rs` as a public type.
- `DbBuilder` exposes `with_write_buffer_manager()` for callers who want to
  share a budget across multiple DB instances or inject a custom size.
- If not provided, the builder creates a default manager with a budget equal
  to `settings.max_unflushed_bytes`.

### Phase 2: Lock-Free SST and WAL Capacity Tracking

Phase 1 establishes the `ByteBudgetSemaphore` pattern: a lock-free, atomic
byte-budget with async acquisition and RAII release. Phase 2 proposes reusing
this pattern to track additional memory pools without locking the database state.

**Opportunity:** The existing backpressure in `maybe_apply_backpressure` reads
the database state under a `RwLock` to aggregate WAL buffer size and immutable
memtable sizes. This works reliably, but the same lock is also held during
memtable rotation and other state mutations. As we add more tracked resource
pools, moving size accounting out of the lock would reduce contention on the
write path.

**Proposed direction:** Introduce additional `ByteBudgetSemaphore`-backed
managers for:

- **WAL buffer memory** — permits acquired when WAL entries are buffered,
  released when the WAL segment is flushed to object storage.
- **Immutable memtable memory** — permits acquired when a memtable is frozen
  (rotated from mutable to immutable), released when the SST flush to L0
  completes.

This would allow `maybe_apply_backpressure` to check budget availability via
atomic reads (`available()`) instead of locking the database state to sum sizes.
The existing `max_unflushed_bytes` check could be replaced entirely by the
combined budget of these managers, or retained as a secondary safety net.

Detailed design of Phase 2 is deferred to a future RFC or an addendum to this
one, once Phase 1 is validated in production.

### Phase 3: Instance Registry for Intelligent Backpressure (WIP)

Phase 1 and 2 enforce budget limits but apply backpressure blindly — when the
budget is exhausted, all writers block regardless of which DB instance holds the
majority of the allocation. Phase 3 proposes a registry pattern that enables
smarter backpressure strategies by tracking which instances share the budget.

**Problem:** When a single `WriteBufferManager` is shared across multiple DB
instances (e.g. via `DbBuilder::with_write_buffer_manager()`), an instance
that has consumed a disproportionate share of the budget causes all other
instances to stall. There is no mechanism today to identify the heavy consumer
or to direct backpressure at it specifically.

**Proposed direction:** Introduce an instance registry within the
`WriteBufferManager`:

- **Registration** — Each DB instance registers itself with the shared
  `WriteBufferManager` on startup and deregisters on shutdown. The registry
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

Detailed design of Phase 3 is deferred to a future RFC, once Phase 2's
per-pool tracking is in place and multi-instance sharing patterns are better
understood from production usage.

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
  atomic CAS — sub-microsecond overhead. Under budget pressure, writes will
  block until memtable flushes free capacity. This is the desired behavior:
  controlled backpressure rather than OOM.
- **Write throughput:** No change under steady-state. Under burst, throughput
  is capped by flush rate, which is the correct bottleneck.
- **Read latency/throughput:** No impact. The write-buffer manager is not on
  the read path.
- **Object-store requests:** No change. Flush behavior is unchanged.
- **Space/read/write amplification:** No change. Data layout is unchanged.

### Observability

- **Configuration changes:** New optional `with_write_buffer_manager()` on
  `DbBuilder`. Default behavior is unchanged.
- **New components:** `WriteBufferManager` (public), `WriteBufferPermit`
  (public), `ByteBudgetSemaphore` (internal).
- **Metrics:** The existing `backpressure_count` metric now also fires when
  the write-buffer budget is exhausted.
- **Logging:** `maybe_apply_backpressure` trace and warn messages now include
  `write_buffer_remaining` to show the remaining byte budget.

### Compatibility

- **Existing data on object storage / on-disk formats:** No change. This is
  purely an in-memory tracking mechanism.
- **Existing public APIs:** The `WriteBufferManager` is a new public type.
  `DbBuilder` gains a new optional method. No existing APIs are changed or
  removed.
- **Rolling upgrades:** Not applicable — this is a client-side, in-memory
  mechanism with no wire protocol or storage format changes.

## Testing

- **Unit tests:** Comprehensive tests for `ByteBudgetSemaphore` and
  `WriteBufferManager` covering:
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
  `WriteBufferManager` and pass it through, validating that the write path
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
  - **Phase 1:** Land `WriteBufferManager`, `WriteBufferPermit`,
    `ByteBudgetSemaphore`, and write-path integration. Default budget equals
    `max_unflushed_bytes`.
  - **Phase 2:** Extend the pattern to WAL buffer and immutable memtable
    capacity tracking, potentially replacing the lock-based size aggregation
    in `maybe_apply_backpressure`.
- Feature flags / opt-in:
  - Phase 1 is always active with a default budget. Callers can customize via
    `DbBuilder::with_write_buffer_manager()`.
- Docs updates:
  - `DbBuilder` API docs for `with_write_buffer_manager()`.
  - `WriteBufferManager` and `WriteBufferPermit` type-level documentation.
  - Operational guidance on tuning the budget for multi-instance deployments.

## Alternatives

**Status quo — rely solely on `max_unflushed_bytes`**

The existing backpressure mechanism works well for steady-state workloads. Under
highly concurrent bursty writers, however, there can be a gap between the
point-in-time size check and the actual write, during which total memory usage
may temporarily exceed the intended budget. The `WriteBufferManager` narrows
that gap by adding proactive reservation before each write is dispatched.

**Use `tokio::sync::Semaphore`**

Tokio's semaphore is well-tested and supports async acquisition. However, it
enforces a hard capacity limit: once all permits are issued, further
acquisitions block until permits are returned, and there is no way to
over-allocate. The `WriteBufferManager` relies on soft capacity tracking —
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

**Acquire the permit inside `maybe_apply_backpressure`**

Instead of acquiring the permit after backpressure in `write_with_options`, we
could fold the acquisition into the backpressure loop itself. This was
considered but rejected because it conflates two concerns: backpressure
(should we slow down?) and reservation (how much are we about to write?). Keeping
them separate makes each easier to reason about and test independently.

**Per-writer budgets instead of a global budget**

We could assign each writer its own slice of the budget, avoiding contention
entirely. This would be more complex to configure and would not handle
heterogeneous write sizes well (one writer with large batches would exhaust its
slice while another's sits idle). A global budget with atomic CAS is simple and
handles skewed workloads naturally.

## Open Questions

- Should the permit reservation size be passed into the backpressure check so
  that `maybe_apply_backpressure` can account for the upcoming write when
  deciding whether to stall? Currently there is a window where backpressure
  returns OK but the subsequent `reserve_write_buffer` blocks briefly under
  concurrent contention.
- What is the right default budget? Phase 1 uses `max_unflushed_bytes`, but
  this may be too generous or too restrictive depending on the workload. Should
  the budget be a separate setting or a fraction of `max_unflushed_bytes`?
- For Phase 2, should the WAL and immutable memtable pools share a single
  budget with the write buffer, or should they be independent budgets with
  separate capacities?

## References

- [Issue #1669: Better Memory Management With A WriteBufferManager](https://github.com/slatedb/slatedb/issues/1669)
- [PR #1 (prototype): adding the primitive](https://github.com/zach-schoenberger/slatedb/pull/1)
- RocksDB [`WriteBufferManager`](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager)
  — prior art for global memtable memory budgeting in an LSM engine.

## Updates
