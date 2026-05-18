## Summary

In reference to slatedb#1669

Introduces a `WriteBufferManager` that enforces a global memory budget on in-flight write data. This provides a permit-based mechanism to track and bound the memory consumed by write batches and memtables, complementing the existing `max_unflushed_bytes` backpressure with an explicit acquire/release lifecycle tied to memtable flushes.

## Motivation

The existing backpressure mechanism checks the aggregate size of WAL + immutable memtables against `max_unflushed_bytes`, but it has no way to **reserve** capacity before writing or **release** it precisely when a memtable is flushed. The `WriteBufferManager` fills this gap by:

- Requiring writers to acquire a budget permit (sized to the estimated entry footprint) before their batch is dispatched
- Blocking new writes when the budget is exhausted, preventing OOM from bursty workloads
- Automatically releasing permits when the owning memtable is dropped (i.e., after flush to L0)

## Changes

### New module: `write_buffer_manager.rs`

- **`WriteBufferManager`** — cloneable handle wrapping a shared `ByteBudgetSemaphore`; exposes `acquire` (async, blocks until budget available), `force_acquire` (non-blocking, for replay), and `available()`
- **`WriteBufferPermit`** — RAII guard that releases its byte reservation on drop; supports `merge()` to consolidate multiple permits into one (used when multiple batches land in the same memtable)
- **`ByteBudgetSemaphore`** — internal async semaphore built on `AtomicUsize` + `tokio::Notify`, allowing partial-budget acquisitions without the one-permit-per-byte overhead of `tokio::sync::Semaphore`
- Comprehensive unit tests covering budget exhaustion, blocking/unblocking, merge semantics, and cross-manager safety

### Integration into the write path

- **`WriteBatch`** (`batch.rs`) — gains a `write_buffer_permit` field, `reserve_write_buffer()` to acquire a permit sized to the batch's `estimated_size()`, and `write_buffer_permit()` to hand the permit to the memtable
- **`DbInner::write_with_options`** (`db.rs`) — after the backpressure check, calls `reserve_write_buffer` on the batch so the permit is acquired before dispatch
- **`DbInner::write_entries_to_memtable`** (`batch_write.rs`) — passes the permit to the memtable via `add_write_permit`

### Backpressure enhancement

- `maybe_apply_backpressure` now also triggers when `write_buffer_manager.available() == 0`, and logs the remaining budget for observability

### Memtable permit tracking

- **`KVTable`** (`mem_table.rs`) — stores an `OnceCell<Arc<WriteBufferPermit>>`; subsequent permits are merged into the existing one, ensuring a single release when the table is dropped

### WAL replay

- During replay, each replayed memtable calls `force_acquire` (non-blocking) so the budget reflects memory already consumed, without deadlocking the replay loop that must make forward progress

### Builder / public API

- **`DbBuilder`** — exposes `with_write_buffer_manager()` for callers who want to share a budget across multiple DB instances or inject a custom size; defaults to `WriteBufferManager::new(settings.max_unflushed_bytes)`
- **`WriteBufferManager`** is re-exported from `lib.rs` as a public type

## Notes for Reviewers

- The permit lifecycle is: acquired in `write_with_options` → attached to the active memtable → released on memtable drop (after flush). This means the budget tracks *in-flight* data from batch submission through L0 persistence.
- `force_acquire` can temporarily push `allocated_bytes` above `capacity`. This is intentional for WAL replay — `maybe_apply_backpressure` will stall new writes until flushes drain the overage.
- The `merge` operation on permits zeroes the source permit's `reserved_bytes` so its `Drop` is a no-op, avoiding double-release.

## Current Concerns

- Currently there is a budget acquisition that can block after `maybe_apply_backpressure` returns. Backpressure ensures the budget is not fully exhausted, but `reserve_write_buffer` may still block briefly under concurrent writer contention. Open to suggestions on whether the reservation size should be passed into the backpressure check.

## Checklist

- [x] Small, scoped PR (< 500 total lines excluding tests); or opened as Draft with a plan on how to break it into smaller pieces
- [x] Linked related issue(s) or added context in the description
- [x] Self-reviewed the diff; added comments for tricky parts
- [x] Tests added/updated and passing locally
- [ ] Ran `cargo fmt`, `cargo clippy --all-targets --all-features`, and `cargo nextest run --all-features`
- [x] Called out any breaking changes and provided migration notes
- [x] Considered performance impact; added notes or benchmarks if relevant
