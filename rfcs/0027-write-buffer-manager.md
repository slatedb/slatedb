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
  - The memory usage of the allocated write buffers (both current and immutable) should count towards the limit. This should not be double counted if it is one allocation.
  - This will not track buffers used for compaction.
  - DbReader instances should be able to use the memory budget in future interactions. 
- The buffer memory limits should be observed as strictly as possible. Erroring towards over counting if needed. KVTable and other container struct allocation size do not count towards this.
- Provide an RAII permit lifecycle: acquire before write, release on memtable
  drop.
- Complement (not replace) the existing `max_unflushed_bytes` backpressure.
- Avoid locking the database state for budget tracking.
- (Phase 2) Allow callers to share a budget across multiple DB instances via
  `DbBuilder::with_write_buffer_manager()` and establish a registry pattern
  for intelligent backpressure.
- The configuration of the memory manager should be as simple as possible -- ideally just one size. Additional configs can be added in future and then only if they are really needed.


## Non-Goals

- Replacing the existing `max_unflushed_bytes` backpressure mechanism entirely.
- Tracking block cache or read-path memory.
- Track struct byte allocations that will never be released like `DbInner::write_notifier`.
- Enforce a hard limit on memory. 
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

```slatedb/slatedb/src/byte_buffer_manager.rs#L216-L221
struct ByteBudgetSemaphore {
    notify: Notify,
    allocated_bytes: AtomicUsize,
    waiter_cnt: AtomicUsize,
    capacity: usize,
}
```

**Why not `tokio::sync::Semaphore`?** Tokio's semaphore enforces a hard capacity
limit — once all permits are issued, acquisitions block until permits are
returned, and there is no way to over-allocate. The `ByteBufferManager` relies
on soft capacity tracking: `force_acquire` lets writers overshoot capacity
rather than blocking. A custom `ByteBudgetSemaphore` gives us full control over
this soft-cap behavior, which `tokio::sync::Semaphore` does not support.

The semaphore operations used in production:

```slatedb/slatedb/src/byte_buffer_manager.rs#L223-L345
impl ByteBudgetSemaphore {
    fn new(capacity: usize) -> Self;
    fn force_acquire(&self, num_bytes: usize);
    fn release(&self, num_bytes: usize);
    fn available(&self) -> usize;
    fn allocated(&self) -> usize;
    async fn wait_for_allocated_below(&self, num_bytes: usize);
}
```

- **`force_acquire`** — non-blocking `fetch_add`. Can push `allocated_bytes`
  above `capacity`. Used by the write path and WAL replay.
- **`release`** — subtracts `num_bytes` from `allocated_bytes` via `fetch_sub`
  and notifies waiters if the budget drops below capacity.
- **`available`** — returns `capacity - allocated_bytes` (saturating to zero
  when over-allocated).
- **`allocated`** — returns the current `allocated_bytes` count.
- **`wait_for_allocated_below`** — blocks until `allocated_bytes` drops below
  the given threshold (used by `await_capacity`).

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

```slatedb/slatedb/src/byte_buffer_manager.rs#L20-L118
impl ByteBufferManager {
    pub fn new(capacity: usize, high_watermark: usize) -> Self;
    pub fn force_acquire(&self, num_bytes: usize) -> ByteBufferPermit;
    pub fn force_expand(&self, permit: &ByteBufferPermit, num_bytes: usize);
    pub fn available(&self) -> usize;
    pub fn capacity(&self) -> usize;
    pub fn at_capacity(&self) -> bool;
    pub async fn await_capacity(&self);
}
```

- **`force_acquire`** — unconditionally reserves bytes (for the write path,
  table creation, WAL buffer creation, and WAL replay). Can push
  `allocated_bytes` above `capacity`. Never blocks.
- **`force_expand`** — unconditionally adds `num_bytes` to an existing permit's
  reservation. Used by `KVTable::put` (per-entry structural overhead) and
  `WalBuffer::append` (VecDeque capacity growth).
- **`available`** — returns `capacity - allocated_bytes` (saturating).
- **`capacity`** — returns the total byte budget capacity.
- **`at_capacity`** — returns `true` if `allocated_bytes >= high_watermark`.
- **`await_capacity`** — waits until allocated bytes drop below the high
  watermark. Does not reserve any bytes.

#### ByteBufferPermit

An RAII guard that releases its byte reservation on drop. Multiple permits can
be consolidated via `merge()` to combine reservations into a single guard.

```slatedb/slatedb/src/byte_buffer_manager.rs#L128-L195
pub struct ByteBufferPermit {
    semaphore: Arc<ByteBudgetSemaphore>,
    reserved_bytes: AtomicUsize,
}

impl ByteBufferPermit {
    pub fn merge(&self, other: &ByteBufferPermit);
    pub fn take(&self, num_bytes: usize) -> Self;
}

impl Drop for ByteBufferPermit {
    fn drop(&mut self); // calls semaphore.release(reserved_bytes)
}
```

- **`merge`** — atomically zeroes the source permit's `reserved_bytes` and adds
  that value to the target. The source permit's `Drop` becomes a no-op,
  avoiding double-release. Used by `KVTable::add_write_permit` to consolidate
  multiple write batch permits into the table's single permit.
- **`take`** — subtracts `num_bytes` from this permit and returns a new permit
  owning those bytes. Used by `KVTable::put` in the overwrite case to release
  excess pre-allocated structural overhead back to the budget.

#### Integration into the Write Path

The permit lifecycle flows through the write path as follows:

```/dev/null/diagram.txt#L1-L20
Writer                  DbInner                 WalBuffer                KVTable
  │                       │                        │                       │
  ├─ write_with_options ─►│                        │                       │
  │                       ├─ batch.set_write_buffer(&write_buffer_manager) │
  │                       │  (internally calls force_acquire(estimated_size))
  │                       ├─ dispatch to writer ──►│                       │
  │                       │                        ├─ append(entries)      │
  │                       │                        │  (force_expand for    │
  │                       │                        │   VecDeque growth     │
  │                       │                        │   only; NOT kv bytes) │
  │                       ├────────────────────────┼──────────────────────►│
  │                       │                        │                       ├─ add_write_permit
  │                       │                        │                       │  (merge kv permit)
  │                       │                        │                       ├─ put(entry)
  │                       │                        │                       │  (force_expand for
  │                       │                        │                       │   structural overhead)
  │                       ├─ maybe_apply_backpressure                      │
  │                       │  (blocks if at_capacity)                       │
  │                       │                        │                       │
  │                       │  flush WAL to storage ►│ drop WalBuffer        │
  │                       │                        │ └─ permit.drop()      │
  │                       │                        │   (releases struct    │
  │                       │                        │    overhead only)     │
  │                       │                        │                       │
  │                       │           flush to L0 ─┼──────────────────────►│ drop KVTable
  │                       │                        │                       │ └─ permit.drop()
  │                       │                        │                       │   (releases kv bytes
  │                       │                        │                       │    + struct overhead)
```

The fundamental pattern is that the **user provides byte buffers** (keys and
values), and those byte buffer metrics are tracked exclusively in relation to
the `KVTable`. The WAL and dispatch channel do *not* account for the key/value
data bytes themselves — only the `KVTable` does.

1. **`WriteBatch`** has a `write_buffer_permit: Option<Arc<ByteBufferPermit>>`
   field. The `set_write_buffer(&ByteBufferManager)` method takes a reference
   to the write buffer manager, calls `force_acquire(self.estimated_size())`
   internally, and attaches the resulting permit to the batch.
   `force_acquire` is used because the bytes have already been allocated by the
   caller. The permit accounts **only for the key and value byte buffers** that
   the user is storing — it does not account for the `WriteBatch` struct itself,
   the dispatch channel, or any other transient overhead.

2. **`DbInner::write_with_options`** calls
   `batch.set_write_buffer(&self.write_buffer_manager)`, which acquires a permit
   for `estimated_size` bytes (the sum of key/value sizes in the batch, see
   `WriteBatch::estimated_size()`). It then dispatches the batch to the writer
   and calls `maybe_apply_backpressure()`. This "acquire-then-backpressure"
   ordering ensures the user's byte buffers are tracked before checking whether
   the system should stall. Using `force_acquire` (non-blocking) avoids holding
   up the dispatch path; backpressure is applied reactively after the write is
   in-flight.

3. **`DbInner::write_entries_to_memtable`** passes the permit to the `KVTable`
   via `add_write_permit`, which merges it into the table's own permit. From
   this point, the `KVTable` owns the byte buffer budget for those key/value
   bytes.

#### Memory Tracking Responsibilities

Each component tracks a distinct slice of memory:

- **`KVTable` (memtable)** — Tracks *everything* related to its state:
  - The user-provided key/value byte buffers (via the merged write permit)
  - Its own structural overhead: `KVTable` struct size, `SequenceTracker`
    pre-allocation, per-entry `SkipMap` node overhead, and `SequencedKey` +
    `RowEntry` struct sizes
  - On creation, `force_acquire(SEQ_TRACKER_OVERHEAD + KVTABLE_SIZE)` reserves
    the base cost; on each `put()`, `force_expand` adds per-entry structural
    overhead

- **`WalBuffer`** — Tracks only its own structural overhead (the `WalBuffer`
  struct size and `VecDeque` capacity growth as entries are appended). It does
  **not** track the key/value data bytes. Those bytes are shared (via `Bytes`
  reference counting) with the `KVTable`, which is the sole owner of the
  key/value budget.

- **`DbStateView` (read-side `Arc<KVTable>`)** — The `KVTable` can be shared
  via `Arc` for read access (e.g., in `DbStateView`). This shared reference
  does **not** independently track byte buffers — it is a view on the original
  table. The budget for key/value bytes remains with the original `KVTable`'s
  permit and is released only when the last `Arc` reference is dropped (i.e.,
  after flush completes and all readers release the table).

#### Size Estimation Algorithms

Each component uses a specific formula to calculate the bytes it charges
against the write buffer budget.

**Write Batch (user-provided key/value bytes)**

The `WriteBatch::estimated_size()` sums only the raw key and value byte lengths
across all operations in the batch:

```
batch_size = ∑ op.estimated_kv_size()

where estimated_kv_size =
    Put  | Merge : key.len() + value.len()
    Delete       : key.len()
```

This is the size passed to `force_acquire` when the permit is created. It
represents the user-provided byte buffers and nothing else.

**KVTable (memtable)**

The `KVTable` charges two categories of bytes against the budget:

1. *Base overhead* — acquired once at table creation:

   ```
   base = SEQ_TRACKER_OVERHEAD + KVTABLE_SIZE

   where
     SEQ_TRACKER_OVERHEAD = 8192 * size_of::<u64>() * 2   (~128 KiB)
     KVTABLE_SIZE         = size_of::<KVTable>()
   ```

2. *Per-entry structural overhead* — expanded on each `put()`:

   ```
   entry_overhead = SKIPMAP_ENTRY_OVERHEAD
                  + size_of::<SequencedKey>()
                  + size_of::<RowEntry>()

   where
     SKIPMAP_ENTRY_OVERHEAD = 128   (tower pointers, node header, alignment)
     SequencedKey           = Bytes handle + u64 seq
     RowEntry               = key + value + seq + optional timestamps
   ```

   In the overwrite case (same `SequencedKey` already exists), the excess
   structural overhead that was pre-allocated but not needed is released back
   via `permit.take()`:

   ```
   excess = SKIPMAP_ENTRY_OVERHEAD + size_of::<SequencedKey>() + old_entry_size
   ```

The total budget consumed by one `KVTable` is therefore:

```
total = base
      + (num_entries * entry_overhead)
      + user_kv_bytes          (from merged write batch permits)
      - overwrite_corrections  (excess returned via take())
```

**WalBuffer**

The `WalBuffer` charges only its own container overhead:

1. *Base overhead* — acquired once at buffer creation:

   ```
   base = size_of::<WalBuffer>()
   ```

2. *VecDeque capacity growth* — expanded each time the internal `VecDeque`
   reallocates:

   ```
   growth_bytes = (cap_after - cap_before) * size_of::<RowEntry>()
   ```

   This is only charged when the `VecDeque`'s capacity actually increases
   (i.e., when it reallocates its backing buffer to fit more entries).

The total budget consumed by one `WalBuffer` is:

```
total = base + cumulative_growth_bytes
```

Notably, the `WalBuffer` does **not** charge for the key/value data bytes of
the entries it holds. Those bytes are shared via `Bytes` reference counting
with the `KVTable`, which is the sole owner of that portion of the budget.

#### Memtable Permit Tracking

`KVTable` stores an `Arc<ByteBufferPermit>` that is created at construction
time (covering the base structural overhead). When write batches land in the
table, their permits are merged into this single table permit via
`ByteBufferPermit::merge()`. This ensures all tracked bytes — both the
user-provided key/value buffers and the table's own structural allocations —
are released in a single `Drop` when the table is dropped after flush.

```slatedb/slatedb/src/mem_table.rs#L113
write_buffer_permit: Arc<ByteBufferPermit>,
```

```slatedb/slatedb/src/mem_table.rs#L647-L653
/// Merges an external write-buffer budget permit into this table's
/// permit so that a single drop releases the combined reservation.
pub(crate) fn add_write_permit(&self, permit: &ByteBufferPermit) {
    self.write_buffer_permit.merge(permit);
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

> **Consideration:** The `ByteBufferManager` tracks outstanding bytes more
> accurately than the existing `max_unflushed_bytes` check, which relies on a
> point-in-time snapshot of WAL + immutable memtable sizes under a read lock.
> In principle, the byte buffer budget alone could replace Condition 1 entirely,
> since it already accounts for all in-flight write data from dispatch through
> flush. For simplicity, the current implementation retains both conditions:
> the legacy `max_unflushed_bytes` check provides a familiar, battle-tested
> safety net, while the byte buffer high watermark adds a more precise
> second layer. A future iteration may simplify these into a single
> budget-based backpressure mechanism once the byte buffer approach has
> sufficient production mileage.

#### Public API and Builder

- `ByteBufferManager` is re-exported from `lib.rs` as a public type.
- Each instance creates its own `ByteBufferManager` internally with both
  `capacity` and `high_watermark` set to `settings.max_unflushed_bytes`.
- In Phase 2, `DbBuilder` will expose `with_write_buffer_manager()` for
  callers who want to share a budget across multiple DB instances.

#### Defaults

When no explicit `ByteBufferManager` is provided via
`DbBuilder::with_write_buffer_manager()`, the database constructs one with:

- **`capacity`** = `settings.max_unflushed_bytes` (default: 1 GB)
- **`high_watermark`** = `capacity` (i.e., backpressure triggers only when the
  full budget is consumed)

Setting `high_watermark == capacity` means `at_capacity()` returns `true` only
when allocated bytes reach the entire budget. This is the simplest default: the
system allows writes to fill the budget completely before applying
backpressure, relying on the flush pipeline to drain immutable memtables in the
background.

The minimum acceptable capacity is 1 MB (`MIN_WRITE_BUFFER_SIZE`). The builder
rejects any `ByteBufferManager` whose capacity falls below this threshold.
This floor ensures there is always enough headroom to cover the fixed
overhead of a single `KVTable` (primarily the `SequenceTracker`
pre-allocation at ~128 KiB) plus at least one entry, preventing deadlock
where the budget is exhausted before any write can land.

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
`force_acquire` lets a writer's reservation overshoot capacity rather than
blocking. A custom `ByteBudgetSemaphore` gives us full control over this
soft-cap behavior, which `tokio::sync::Semaphore` does not support.

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

- [Issue #1669: Better Memory Management With A ByteBufferManager](https://github.com/slatedb/slatedb/issues/1669)
- [PR #1 (prototype): adding the primitive](https://github.com/zach-schoenberger/slatedb/pull/1)
- RocksDB [`ByteBufferManager`](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager)
  — prior art for global memtable memory budgeting in an LSM engine.

## Updates

- **2025-07-11:** Updated to reflect the implemented design. Renamed
  `WriteBufferManager` → `ByteBufferManager` and `WriteBufferPermit` →
  `ByteBufferPermit`. Updated write path integration to reflect the
  `force_acquire` → dispatch → `maybe_apply_backpressure` ordering. Added
  `high_watermark` field and `at_capacity()`/`await_capacity()` methods.
  Updated backpressure section to describe the two-condition check with
  memtable freezing. Status changed from Draft to Implemented.
