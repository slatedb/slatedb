# Range Metadata and Size Estimation

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [API](#api)
   * [`SizeEstimate` Trait](#sizeestimate-trait)
   * [Internal State for `SizeEstimate`](#internal-state-for-sizeestimate)
   * [`SizeEstimate` Algorithm Details](#sizeestimate-algorithm-details)
      + [`SstMetadata` impl (async, reads index block)](#sstmetadata-impl-async-reads-index-block)
      + [`MemtableMetadata` impl (no I/O)](#memtablemetadata-impl-no-io)
   * [Implementation Phases](#implementation-phases)
      + [Phase 1 - SST Stats Block](#phase-1-sst-stats-block)
      + [Phase 2 - `Db::metadata()` API and `SizeEstimate` Trait](#phase-2-dbmetadata-api-and-sizeestimate-trait)
- [Impact Analysis](#impact-analysis)
   * [Core API & Query Semantics](#core-api-query-semantics)
   * [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   * [Time, Retention, and Derived State](#time-retention-and-derived-state)
   * [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   * [Compaction](#compaction)
   * [Storage Engine Internals](#storage-engine-internals)
   * [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
   * [Performance & Cost](#performance-cost)
   * [Observability](#observability)
   * [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [FiV0](https://github.com/FiV0)

## Summary

This RFC proposes:
1. Adding a stats block to the SST footer containing per-SST statistics (`num_puts`, `num_deletes`, `num_merges`, `raw_key_size`, `raw_val_size`).
2. Exposing a `Db::metadata(range)` API that returns per-SST and per-memtable metadata for a given key range, along with a `SizeEstimate` trait providing `bytes_before(key)` and `bytes_after(key)` methods for finer-grained size estimation.

Rather than providing high-level estimation functions with configuration knobs, this approach exposes lower-level metadata and lets users compute whatever derived statistics they need.

## Motivation

Users want to understand data distribution and storage usage for specific key ranges to support:

- **Cost estimation**: Calculating storage costs or scan costs for specific data partitions. For example, if a key range is an index in a query engine, it is good to know an approximate size when joining with other data.
- **Bulk operations**: Planning exports, migrations, or splits based on estimated sizes
- **Capacity planning**: Understanding storage requirements for different key ranges
- **Data modeling**: Understanding how data is distributed across the key space

Currently there is no better way than to scan the whole range to get an estimate or approximation of the data size.

An earlier revision of this RFC proposed high-level functions (`estimate_size_with_options` and `estimate_key_count`) with configuration options like `error_margin`, `include_memtables`, and `include_files`. However, different use cases demand different knobs — compressed vs uncompressed sizes, varying accuracy vs I/O tradeoffs, inclusion or exclusion of memtables, etc. Baking all of these into the API through configuration options risks making the interface rigid and opinionated. Instead, we expose lower-level per-SST and per-memtable metadata so users can compute whatever they need. For example, a user interested in key counts can derive `num_puts + num_merges - num_deletes` from the metadata themselves, while a user interested in on-disk size can sum `size_stored_bytes`, and one needing finer accuracy can call `bytes_before`/`bytes_after`.

## Goals

- Expose per-SST and per-memtable metadata for a given key range
- Provide a `SizeEstimate` trait with `bytes_before(key)` / `bytes_after(key)` for block-level size estimation within individual SSTs and memtables
- Add an SST stats block to the footer for key/value counts and sizes
- Expose the metadata API on `Db`, `DbReader`, `admin.rs`, and `slatedb-cli`

## Non-Goals

- Exact size calculation or key counting (would require reading and processing a lot more data)
- High-level aggregation functions with configuration options (users derive these from the metadata)

## Design

### API

The following API will be added to `Db`:

```rust
impl Db {
    /// Returns per-SST and per-memtable metadata for the given key range.
    pub async fn metadata<K, T>(
        &self,
        range: T,
    ) -> Result<RangeMetadata, Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send;
}

pub struct RangeMetadata {
    pub memtables: Vec<MemtableMetadata>,
    pub ssts: Vec<SstMetadata>,
}

/// Whether an SST is fully or partially covered by the queried range.
/// For `Full` SSTs, the metadata stats apply directly to the queried range.
/// For `Partial` SSTs, stats represent the whole SST — use `bytes_before`/`bytes_after`
/// to refine estimates for the overlapping portion.
pub enum Coverage {
    Full,
    Partial,
}

pub struct SstMetadata {
    pub id: SsTableId,
    pub num_puts: u64,
    pub num_deletes: u64,
    pub num_merges: u64,
    pub size_raw_key_bytes: u64,
    pub size_raw_value_bytes: u64,
    pub size_stored_bytes: u64,          // on-disk / compressed size
    pub first_key: Option<Bytes>,
    pub compression_codec: Option<CompressionCodec>,
    pub sorted_run: Option<u32>,
    pub visible_range: Option<BytesRange>,
    pub coverage: Coverage,
    // private internal state for I/O (not exposed to users):
    info: SsTableInfo,
    table_store: Arc<TableStore>,
}

pub struct MemtableMetadata {
    pub num_puts: u64,
    pub num_deletes: u64,
    pub num_merges: u64,
    pub size_raw_key_bytes: u64,
    pub size_raw_value_bytes: u64,
    pub is_mutable: bool,
    pub last_tick: i64,
    pub last_seq: u64,
    // private internal state (not exposed to users):
    table: Arc<KVTable>,
}
```

This metadata function will be exposed in the following locations:

- **`DbReader`**: For programmatic access in applications
- **`admin.rs`**: For operational tooling (Admin instantiates a reader to call the metadata function)
- **`slatedb-cli`**: For command-line operational use cases

### `SizeEstimate` Trait

Both `SstMetadata` and `MemtableMetadata` implement the `SizeEstimate` trait, providing a uniform interface for estimating bytes before/after a key within a single storage unit:

```rust
/// Trait for estimating bytes before/after a key within a single storage unit
/// (SST or memtable). Both methods return overestimates.
pub trait SizeEstimate {
    /// Overestimate of bytes before `key` in this storage unit.
    async fn bytes_before(&self, key: &[u8]) -> Result<u64, Error>;
    /// Overestimate of bytes after `key` in this storage unit.
    async fn bytes_after(&self, key: &[u8]) -> Result<u64, Error>;
}

impl SizeEstimate for SstMetadata { /* reads index block via TableStore */ }
impl SizeEstimate for MemtableMetadata { /* iterates skip map, exact on uncompressed data */ }
```

### Internal State for `SizeEstimate`

**`SstMetadata`** needs to read the SST's index block to implement `bytes_before`/`bytes_after`. For this it holds a private `Arc<TableStore>` — the existing `pub(crate)` struct that reads index blocks from object storage, using the block cache when available.

Currently `TableStore::read_index` takes a full `&SsTableHandle`, but it only uses `handle.id` (to resolve the object store path and cache key) and `handle.info` (for `index_offset`/`index_len`/`compression_codec`). It does not use the handle's `visible_range` or `effective_range`. A small refactor to accept `(id: &SsTableId, info: &SsTableInfo)` instead would make the dependency cleaner. Since `SstMetadata` already stores `id`, `first_key`, and `compression_codec` as public fields, the only additional private state needed is `index_offset` and `index_len` (stored internally via `SsTableInfo`).

**`MemtableMetadata`** holds a private `Arc<KVTable>` — the existing skip-map-backed memtable. It supports `range_ascending(range)` which returns an iterator over entries in the given key range.

### `SizeEstimate` Algorithm Details

#### `SstMetadata` impl (async, reads index block)

The call chain is:
1. `bytes_before(key)` / `bytes_after(key)` calls `self.table_store.read_index(...)` which returns `Arc<SsTableIndexOwned>`
2. `SsTableIndexOwned::borrow()` returns an `SsTableIndex` (FlatBuffer view)
3. `SsTableIndex::block_meta()` returns a vector of `BlockMeta { first_key, offset }`
4. Binary search `BlockMeta` by `first_key` to find the block containing `key`
5. Compute overestimate from block offsets

Given block metadata `[B0, B1, ..., Bn]` where each `Bi` has `offset` and `first_key`:
- Binary search to find block `Bi` such that `Bi.first_key <= key < Bi+1.first_key`
- `bytes_before(key)` = offset of `Bi+1` (end of block `Bi`). If `Bi` is the last block, use `index_offset` (= end of all data blocks). **Overestimates** because key may be partway through `Bi`.
- `bytes_after(key)` = `index_offset - offset(Bi)`. **Overestimates** for the same reason — includes the full boundary block.
- Both are overestimates; `bytes_before + bytes_after >= size_stored_bytes`. The overestimate per call is at most one block size (~4KB default).
- Note: these operate on **compressed/stored** block offsets since that's what the index tracks.

There is existing related work in `estimate_bytes_before_key` (in `utils.rs`) that does SST-granularity estimation across sorted runs. The new `bytes_before` goes one level deeper to block-granularity within a single SST.

#### `MemtableMetadata` impl (no I/O)

- Use `KVTable::range_ascending(..key)` to iterate entries before the key, sum entry sizes → `bytes_before`.
- Use `KVTable::range_ascending(key..)` to iterate entries from the key onward, sum entry sizes → `bytes_after`.
- These are **exact** on uncompressed data (memtables are never compressed).
- To maintain the overestimate guarantee consistent with SSTs, both include entries where `user_key == key`, making `bytes_before + bytes_after >= total_memtable_size`.

### Implementation Phases

#### Phase 1 - SST Stats Block

Add a stats block to the SST footer with the following fields (all `u64`):
- `num_puts`
- `num_deletes`
- `num_merges`
- `raw_key_size`
- `raw_val_size`

These are loaded into `SsTableInfo` at SST open time. This requires care as the storage format changes, i.e. the `.fbs` gets updated. For backwards compatibility, the stats block goes at the end of the footer and is optional. Old SSTs without a stats block get zeros for these fields.

#### Phase 2 - `Db::metadata()` API and `SizeEstimate` Trait

Expose the `Db::metadata(range)` function, returning `RangeMetadata` populated from in-memory state:

- **`SstMetadata`**: All public fields populated from `SsTableHandle` / `SsTableInfo`, including stats block fields from Phase 1. `size_stored_bytes` derived from `SsTableHandle::estimate_size()`. `coverage` computed by comparing SST key range against the queried range.
- **`MemtableMetadata`**: Populated from `KVTableMetadata` (entry counts, sizes, `last_tick`, `last_seq`, `is_mutable`). Stats breakdowns (`num_puts` vs `num_deletes` vs `num_merges`) require iterating the memtable or tracking counts separately.

Implement the `SizeEstimate` trait on both `SstMetadata` (reads index blocks) and `MemtableMetadata` (iterates skip map).

Expose on `Db`, `DbReader`, `admin.rs`, and `slatedb-cli`.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`) - Adds new metadata read API
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Logical clocks
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

- [ ] Write-ahead log (WAL)
- [x] Block cache - `SizeEstimate` may cache index blocks
- [x] Object store cache - `SizeEstimate` reads index blocks from object storage
- [x] Indexing (bloom filters, metadata) - Uses existing SST index blocks for `SizeEstimate`
- [x] SST format or block format - Adds stats block to SST footer

### Ecosystem & Operations

- [x] CLI tools - `slatedb-cli` will expose metadata command
- [x] Language bindings (Go/Python/etc) - New API to expose in bindings
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

SST stats block footer:
- 40 bytes per SST space amplification (5 x `u64`). This is negligible.

`Db::metadata()` (without `SizeEstimate` calls):
- Only reads in-memory `SsTableInfo` and `KVTableMetadata`. No I/O.

`SizeEstimate::bytes_before` / `bytes_after`:
- One index block read per SST (~300-500KB), cacheable via the block cache. Cost per call is negligible.
- For memtables: iterates the skip map in memory. No object store I/O.

Throughput:
- No impact on writes or compaction
- Read throughput for metadata scales with number of overlapping SSTs

Miscalculation:
- 4-8x potential overestimation if most keys sit in recent L0 SSTs, as these are not compacted
- If keys get frequently written to, every level will likely count the key once. So there is some overestimation.

Space, read, and write amplification:
- Little impact on space amplification
- No impact on write amplification
- No impact on read amplification (excluding the new API)

### Observability

Users might be interested in exposing metadata for observability purposes. However, we will not modify `stats.rs` for now. Users can call the metadata method directly and expose metrics as they see fit.

### Compatibility

- SST files get a new stats block in Phase 1. Old SSTs without the stats block will report zeros for stats fields or we panic if an SST with no stats footer is read.
- New APIs are additive only
- No breaking changes to existing APIs
- Language bindings will need to expose new types and methods

## Testing

Unit tests:
- SST stats block encoding/decoding and backwards compatibility with old SSTs
- `SstMetadata` and `MemtableMetadata` field population from `SsTableHandle` and `KVTable`
- `SizeEstimate` on SSTs: index block binary search, `bytes_before`/`bytes_after` offset calculations
- `SizeEstimate` on memtables: iteration and size summation
- `Coverage` determination: full vs partial overlap detection
- Edge cases: empty ranges, single-block SSTs, unbounded ranges, empty memtables

Not planning to add any specific tests for:
- Integration tests
- Fault-injection/chaos tests
- Deterministic simulation tests
- Formal methods verification
- Performance tests

## Rollout

- Phase 1 (SST stats block) and Phase 2 (metadata API) can be implemented as separate PRs.
- Add API to other language bindings
- Docs update

## Alternatives

The original revision of this RFC proposed high-level functions (`estimate_size_with_options` with `SizeApproximationOptions` and `estimate_key_count`) that internally aggregated per-SST data and exposed configuration knobs (`error_margin`, `include_memtables`, `include_files`). This was rejected because:
- Different use cases demand different knobs, risking an ever-growing options struct
- Mixing compressed (SST) and uncompressed (memtable) data in a single return value was confusing
- Users couldn't distinguish between on-disk and in-memory sizes without options
- The `error_margin` approach sometimes triggered I/O and sometimes didn't, making performance unpredictable

The metadata approach avoids these issues by letting users decide what to compute from the raw per-SST/per-memtable data.

Another alternative not explored is sample-based estimation: sample N random blocks and extrapolate key counts or compression ratios. This could complement the current approach but adds complexity.

## Open Questions

- How should `MemtableMetadata` track `num_puts` / `num_deletes` / `num_merges` breakdowns? Currently `KVTableMetadata` only tracks total `entry_num` and `entries_size_in_bytes`. Options: iterate the memtable at metadata collection time, or add separate counters to `KVTable`.

## References

- [Issue #905: Key Count (or Approximate)](https://github.com/slatedb/slatedb/issues/905)
- [Comment proposing this design](https://github.com/slatedb/slatedb/issues/905#issuecomment-3765276221)
- [PR #1220: RFC discussion](https://github.com/slatedb/slatedb/pull/1220)
- [criccomini's metadata approach comment](https://github.com/slatedb/slatedb/pull/1220#issuecomment-3838410526)
- [criccomini's struct proposal comment](https://github.com/slatedb/slatedb/pull/1220#issuecomment-3842567040)
- [RocksDB GetApproximateSizes Documentation](https://github.com/facebook/rocksdb/wiki/Approximate-Size)
- [RocksDB BlockBasedTable Format](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format)

## Updates

- **2026-01-30**: Renamed `get_approximate_size_with_options` to `estimate_size_with_options` for consistency with `estimate_key_count` and to avoid autocomplete collision with `get_` methods (PR #1220 review feedback)
- **2026-01-30**: Added note about previous work in `estimate_bytes_before_key` function that does similar metadata-only estimation
- **2026-01-30**: Added paragraph about backwards compatibility for stats block implementation
- **2026-01-30**: Clarified that per-block statistics (24 bytes per block) will not be added; using average extrapolation approach instead to keep block format simple (PR #1220 review feedback)
- **2026-01-30**: Updated observability section to clarify that `stats.rs` will not be modified; users can expose metrics themselves (PR #1220 review feedback)
- **2026-01-30**: Added section specifying that approximation functions will be exposed in `DbReader`, `admin.rs`, and `slatedb-cli` (PR #1220 review feedback)
- **2026-02-05**: Major revision — replaced high-level `estimate_size_with_options` / `estimate_key_count` API with lower-level `Db::metadata(range) -> RangeMetadata` approach exposing per-SST and per-memtable metadata structs plus a `SizeEstimate` trait with `bytes_before(key)` / `bytes_after(key)` methods (PR #1220 review feedback from @criccomini and @agavra). Added `Coverage` enum to indicate full vs partial SST overlap. Consolidated into a single workstream with two phases.
