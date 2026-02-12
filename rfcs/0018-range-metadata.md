# Range Metadata and Size Estimation

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [API](#api)
      + [`Db::manifest()`](#dbmanifest)
      + [`SstReader` and `SstFile`](#sstreader-and-sstfile)
      + [`SstStats`](#sststats)
      + [Visibility changes to existing types](#visibility-changes-to-existing-types)
      + [Memtable stats via `StatRegistry`](#memtable-stats-via-statregistry)
   * [Size and Cardinality Estimation](#size-and-cardinality-estimation)
      + [Coarse estimate — SST-level, no I/O beyond `manifest()`](#coarse-estimate-sst-level-no-io-beyond-manifest)
      + [Refined estimate — block-level for boundary SSTs](#refined-estimate-block-level-for-boundary-ssts)
      + [L0 handling](#l0-handling)
      + [Staleness and GC](#staleness-and-gc)
      + [Memtable contribution](#memtable-contribution)
   * [Implementation Phases](#implementation-phases)
      + [Phase 1 - SST Stats Block](#phase-1-sst-stats-block)
      + [Phase 2 - `SstReader`, `SstFile`, and `Db::manifest()`](#phase-2-sstreader-sstfile-and-dbmanifest)
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
2. Exposing lower-level primitives — `Db::manifest()`, `SstReader`, and `SstFile` — that allow users to walk the manifest, open individual SSTs, and read per-SST stats and index data for size estimation. Memtable stats are exposed via the existing `StatRegistry` (accessible through `Db::metrics()`).

Rather than providing a single high-level `Db::metadata(range)` function, this approach exposes modular building blocks. Users call `db.manifest()` to discover which SSTs exist, then use `SstReader` to open and inspect individual SSTs. This makes SlateDB more composable and avoids coupling the estimation logic to a specific API shape.

## Motivation

Users want to understand data distribution and storage usage for specific key ranges to support:

- **Cost estimation**: Calculating storage costs or scan costs for specific data partitions. For example, if a key range is an index in a query engine, it is good to know an approximate size when joining with other data.
- **Bulk operations**: Planning exports, migrations, or splits based on estimated sizes
- **Capacity planning**: Understanding storage requirements for different key ranges
- **Data modeling**: Understanding how data is distributed across the key space

Currently there is no better way than to scan the whole range to get an estimate or approximation of the data size.

An earlier revision of this RFC proposed high-level functions (`estimate_size_with_options` and `estimate_key_count`) with configuration options like `error_margin`, `include_memtables`, and `include_files`. However, different use cases demand different knobs — compressed vs uncompressed sizes, varying accuracy vs I/O tradeoffs, inclusion or exclusion of memtables, etc. Baking all of these into the API through configuration options risks making the interface rigid and opinionated. Instead, we expose lower-level building blocks so users can compute whatever they need. For example, a user interested in key counts can derive `num_puts + num_merges - num_deletes` from per-SST stats, while a user interested in on-disk size can sum `SsTableHandle::estimate_size()` across covering SSTs, and one needing finer accuracy can use `SstFile::index()` to estimate at block granularity.

## Goals

- Add an SST stats block to the footer for key/value counts and sizes
- Expose `Db::manifest()` so users can discover SSTs covering a key range
- Provide `SstReader` and `SstFile` for opening individual SSTs and reading their stats and index data
- Expose memtable stats as metrics via the existing `StatRegistry` / `Db::metrics()`

## Non-Goals

- Exact size calculation or key counting (would require reading and processing a lot more data)
- High-level aggregation functions with configuration options (users derive these from the metadata)
- Providing a single `Db::metadata(range)` API that pre-computes which SSTs overlap a range (users do this themselves using the manifest)
- Exposing `SstFile::rows()` for reading row data (out of scope for size estimation)

## Design

### API

#### `Db::manifest()`

```rust
impl Db {
    ...
    pub fn manifest(&self) -> &ManifestCore;
    ...
}
```

Returns a clone of the current in-memory `ManifestCore`. No I/O. `ManifestCore` is already public (re-exported via `slatedb::manifest`) .

#### `SstReader` and `SstFile`

```rust
pub struct SstReader {
    object_store: Arc<dyn ObjectStore>,
    root_path: Path,
    cache: Option<Arc<dyn DbCache>>,
}

impl SstReader {
    pub fn new(
        root_path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        cache: Option<Arc<dyn DbCache>>,
    ) -> Self;

    /// Reads the SST footer from object storage (one `head()` + one
    /// `get_range()` call). Errors if the file was GC'd.
    pub async fn open(&self, id: Ulid) -> Result<SstFile, SlateDBError>;
}
```

`SstReader` wraps the read-only functionality currently internal to `TableStore`. SST paths are resolved as `{root}/compacted/{ulid}.sst`. Internally, `open()` needs to decode the SST footer and index block, which requires handling the SST codec and compression. The implementation will need to take care of constructing the appropriate decoding machinery (currently `SsTableFormat`) from the provided configuration.

```rust
pub struct SstFile {
    id: Ulid,
    info: SsTableInfo,
    stats: SstStats,
    reader: SstReader,
}

impl SstFile {
    pub fn id(&self) -> Ulid;

    /// No additional I/O — stats are loaded during `open()`.
    pub fn stats(&self) -> &SstStats;

    /// Returns `(block_offset, first_key)` pairs from the SST index block.
    pub async fn index(&self) -> Result<Vec<(u64, Bytes)>, SlateDBError>;
}
```

`index()` calls `SsTableFormat::read_index()`, which reads `info.index_offset..info.index_offset + info.index_len`, decompresses, and returns an `SsTableIndexOwned`. The method materializes `Vec<(u64, Bytes)>` from the FlatBuffer `BlockMeta` entries (each has `offset()` and `first_key()`). Caching uses `DbCache::get_index` / `insert` keyed by `(sst_id, index_offset)`, matching the existing pattern in `TableStore::read_index()`.

#### `SstStats`

Per-SST statistics from the stats block in the footer (Phase 1). For pre-Phase-1 SSTs, all fields are 0.

```rust
pub struct SstStats {
    pub num_puts: u64,
    pub num_deletes: u64,
    pub num_merges: u64,
    pub size_raw_key_bytes: u64,
    pub size_raw_value_bytes: u64,
}
```

#### Visibility changes to existing types

- **`SortedRun::tables_covering_range()`**: Change from `pub(crate)` to `pub`. Lets users find which SSTs in a sorted run overlap a given key range.
- **`SsTableHandle::estimate_size()`**: Change from `pub(crate)` to `pub`. Enables quick no-I/O size estimates.
- **`SsTableHandle::visible_range`**: Consider changing from `pub(crate)` to `pub` so users can inspect the projected key range.

#### Memtable stats via `StatRegistry`

Rather than exposing per-memtable metadata structs, memtable statistics are tracked as a running aggregate and exposed through the existing `StatRegistry`, accessible via `Db::metrics()`.

There are two layers: per-memtable counters on `KVTable`, and global aggregate gauges in `DbStats`.

**Per-memtable counters** — new atomic fields on `KVTable`:

```rust
num_puts: AtomicU64,
num_deletes: AtomicU64,
num_merges: AtomicU64,
raw_key_bytes: AtomicU64,
raw_value_bytes: AtomicU64,
```

`KVTable::put()` already inspects `RowEntry` to track `entries_size_in_bytes`. We extend this to also check `RowEntry.value` — if `Value(_)` bump `num_puts`, if `Tombstone` bump `num_deletes`, if `Merge(_)` bump `num_merges`, and add key/value lengths to the byte counters. When a previous entry is replaced (the `compare_insert` callback fires), decrement the old entry's type counter and size contributions before incrementing the new entry's (same pattern as the existing `entries_size_in_bytes` handling).

These fields are also added to `KVTableMetadata` so they can be read via `KVTable::metadata()`.

**Global aggregate gauges** — new metrics in `DbStats`, registered in `StatRegistry` at DB startup:

```rust
pub const MEMTABLE_NUM_PUTS: &str = db_stat_name!("memtable_num_puts");
pub const MEMTABLE_NUM_DELETES: &str = db_stat_name!("memtable_num_deletes");
pub const MEMTABLE_NUM_MERGES: &str = db_stat_name!("memtable_num_merges");
pub const MEMTABLE_RAW_KEY_BYTES: &str = db_stat_name!("memtable_raw_key_bytes");
pub const MEMTABLE_RAW_VALUE_BYTES: &str = db_stat_name!("memtable_raw_value_bytes");
```

These are `Gauge<i64>` (not `Counter`) because they need to decrease when memtables are flushed.

**How the two layers connect:**

1. **Write arrives** → `KVTable::put()` updates the per-memtable atomics AND increments the global `DbStats` gauges. Both happen per-entry.
2. **Memtable freezes** → No stat changes. The frozen `KVTable` retains its counters, and the global gauges already include them.
3. **Immutable memtable flushes to L0** → The flusher reads the frozen `KVTable`'s counters (via `metadata()`) and *subtracts* those values from the global gauges.
4. **User reads** → `db.metrics().lookup("db.memtable_num_puts")` returns the current gauge value, representing the aggregate across the active memtable plus all immutable memtables not yet flushed.

Note that these stats are global, not per-range. You cannot ask "how many puts are in memtables for key range X..Y". This is an accepted limitation since memtables are bounded in size (by `max_unflushed_bytes`) and the SST-level stats dominate for most estimation use cases.

### Size and Cardinality Estimation

This section describes how to use the above APIs for common estimation tasks, at three levels of accuracy.

#### Coarse estimate — SST-level, no I/O beyond `manifest()`

Call `db.manifest()`. For each sorted run, use `tables_covering_range(range)` to find the intersecting SSTs. For L0, iterate all handles (L0 SSTs can overlap arbitrarily). Sum `estimate_size()` across all covering SSTs. This gives an upper-bound stored-bytes estimate. No `SstReader` needed. Overestimates because boundary SSTs are counted in full even if the range only touches a small portion.

For cardinality: open each covering SST with `SstReader` to get `SstStats`. Sum `num_puts + num_merges - num_deletes` across all levels. This overestimates because the same key may appear in multiple levels (L0 overlaps, and compaction hasn't merged them yet). The overestimation factor is roughly proportional to the number of levels containing the key.

#### Refined estimate — block-level for boundary SSTs

Most SSTs returned by `tables_covering_range()` are fully contained within the query range — their stats apply directly. Only the first and last SST in each sorted run partially overlap. For these two boundary SSTs, call `sst_file.index()` to get the block index `[(offset, first_key), ...]`. Binary search for the range start key in the first boundary SST to find where the range begins; binary search for the range end key in the last boundary SST to find where it ends.

These offsets are compressed/stored sizes since the block index tracks on-disk offsets.

For proportional stat scaling on boundary SSTs: compute `covered_fraction = covered_bytes / total_sst_stored_bytes` and scale `SstStats` fields proportionally (e.g. `estimated_puts = num_puts * covered_fraction`). This assumes uniform key distribution within the SST.

There is existing related work in `estimate_bytes_before_key` (in `utils.rs`) that does SST-granularity estimation across sorted runs. The block-level approach here goes one level deeper within a single SST.

#### L0 handling

L0 SSTs are not sorted relative to each other and can all overlap with the query range. Every L0 SST must be checked. For each, the same refinement as above applies: if the SST's `first_key` and key range suggest partial overlap, use `index()` to refine. Since L0 SSTs tend to be smaller (pre-compaction), the coarse estimate is often sufficient.

#### Staleness and GC

The manifest snapshot from `manifest()` may diverge from the actual DB state by the time `SstReader::open()` is called. GC may have deleted SSTs. Treat `open()` errors on missing SSTs as best-effort — skip them. For stronger guarantees, create a checkpoint before reading.

#### Memtable contribution

Add memtable stats from `db.metrics()` (`memtable_num_puts`, `memtable_raw_key_bytes`, etc.) to the totals. These are aggregates across all unflushed memtables and are not range-specific.

### Implementation Phases

#### Phase 1 - SST Stats Block

Add a stats block to the SST footer with the following fields (all `u64`):
- `num_puts`
- `num_deletes`
- `num_merges`
- `raw_key_size`
- `raw_val_size`

These are loaded into `SsTableInfo` at SST open time. This requires care as the storage format changes, i.e. the `.fbs` gets updated. For backwards compatibility, the stats block goes at the end of the footer and is optional. Old SSTs without a stats block get zeros for these fields.

#### Phase 2 - `SstReader`, `SstFile`, and `Db::manifest()`

- **`Db::manifest()`**: Returns a clone of the current `ManifestCore`. No I/O.
- **`SstReader`**: New public struct wrapping object store access for SSTs. Internally reuses `SsTableFormat::read_info` and `SsTableFormat::read_index`.
- **`SstFile`**: New public struct returned by `SstReader::open()`. Provides `stats()` (from Phase 1 stats block) and `index()` (block-level offset/key pairs).
- **`SstStats`**: New public struct with per-SST statistics from the footer.
- **`SortedRun::tables_covering_range()`**: Make `pub` (currently `pub(crate)`).
- **`SsTableHandle::estimate_size()`**: Make `pub` (currently `pub(crate)`).
- **Memtable metrics**: Add per-type counters to `KVTable` and register corresponding gauges in `StatRegistry`.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`) - Adds `Db::manifest()` and new `SstReader`/`SstFile` types
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

- [x] Manifest format - `Db::manifest()` exposes the manifest
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection - GC may delete SSTs between `manifest()` and `SstReader::open()`
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
- [x] Block cache - `SstFile::index()` may cache index blocks
- [x] Object store cache - `SstFile::index()` reads index blocks from object storage
- [x] Indexing (bloom filters, metadata) - Uses existing SST index blocks for `SstFile::index()`
- [x] SST format or block format - Adds stats block to SST footer

### Ecosystem & Operations

- [x] CLI tools - `slatedb-cli` will expose SST stats inspection
- [x] Language bindings (Go/Python/etc) - New API to expose in bindings
- [x] Observability (metrics/logging/tracing) - Memtable stats exposed via `StatRegistry`

## Operations

### Performance & Cost

SST stats block footer:
- 40 bytes per SST space amplification (5 x `u64`). This is negligible.

`Db::manifest()`:
- Clones in-memory state. No I/O. Cost proportional to number of SST handles in the manifest.

`SstReader::open()`:
- One object store read per SST to load the footer (~100-500 bytes). Includes stats block from Phase 1.

`SstFile::stats()`:
- No additional I/O beyond `open()`. Stats are loaded from the footer.

`SstFile::index()`:
- One index block read per SST (~300-500KB for a 256MB SST), cacheable via the block cache.
- Returns up to ~65K entries for a 256MB SST with 4KB blocks. Users should downsample if needed.

Memtable metrics via `Db::metrics()`:
- No I/O. Reads atomic counters.

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

Memtable stats (`num_puts`, `num_deletes`, `num_merges`, `raw_key_bytes`, `raw_value_bytes`) are exposed as metrics in `StatRegistry`, accessible via `Db::metrics()`. SST-level stats are accessed directly through `SstFile::stats()`. Users can aggregate and expose these through their own observability systems.

### Compatibility

- SST files get a new stats block in Phase 1. Old SSTs without the stats block will report zeros for stats fields or we panic if an SST with no stats footer is read.
- New APIs are additive only
- No breaking changes to existing APIs
- Language bindings will need to expose new types and methods

## Testing

Unit tests:
- SST stats block encoding/decoding and backwards compatibility with old SSTs
- `SstReader::open()`: loading SST footer and constructing `SstFile`
- `SstFile::stats()`: correct population of `SstStats` fields
- `SstFile::index()`: returns correct `(offset, first_key)` pairs matching the SST's block index
- `Db::manifest()`: returns current manifest state with L0 and sorted runs
- `SortedRun::tables_covering_range()`: full and partial range overlap detection
- Memtable metrics: correct registration, increment on write, decrement on flush
- Edge cases: empty SSTs, SSTs without stats block (pre-Phase 1), GC'd SSTs returning errors
- Size estimation algorithm: binary search on index entries, bytes before/after calculation

Not planning to add any specific tests for:
- Integration tests
- Fault-injection/chaos tests
- Deterministic simulation tests
- Formal methods verification
- Performance tests

## Rollout

- Phase 1 (SST stats block) and Phase 2 (`SstReader`/`SstFile`/`Db::manifest()`) can be implemented as separate PRs.
- `SortedRun::tables_covering_range()` visibility change (`pub(crate)` to `pub`) can be a small standalone PR.
- Add API to other language bindings
- Docs update with usage examples for the estimation workflow

## Alternatives

The original revision of this RFC proposed high-level functions (`estimate_size_with_options` with `SizeApproximationOptions` and `estimate_key_count`) that internally aggregated per-SST data and exposed configuration knobs (`error_margin`, `include_memtables`, `include_files`). This was rejected because:
- Different use cases demand different knobs, risking an ever-growing options struct
- Mixing compressed (SST) and uncompressed (memtable) data in a single return value was confusing
- Users couldn't distinguish between on-disk and in-memory sizes without options
- The `error_margin` approach sometimes triggered I/O and sometimes didn't, making performance unpredictable

The immediately preceding revision proposed a `Db::metadata(range)` API returning `RangeMetadata` with `Vec<SstMetadata>` and `Vec<MemtableMetadata>`, plus a `SizeEstimate` trait with `bytes_before(key)` / `bytes_after(key)` methods. This was replaced because:
- It coupled range-walking logic to the DB, when users could do this themselves using the manifest
- The `SizeEstimate` trait with private `Arc<TableStore>` state leaked internal abstractions
- Memtable metadata required holding `Arc<KVTable>` references, complicating lifecycle management
- The new approach is more modular: `SstReader`/`SstFile` are independent of `Db` and can be used with any `ObjectStore` and optional `DbCache`

Another alternative not explored is sample-based estimation: sample N random blocks and extrapolate key counts or compression ratios. This could complement the current approach but adds complexity.

## Open Questions

- Should `Db` expose a pre-configured `SstReader` via `db.sst_reader()` instead of requiring manual construction?
- Should `SstFile::index()` return raw `SsTableIndexOwned` instead of materializing `Vec<(u64, Bytes)>` to avoid the allocation?
- Is there a need for a `DbReader::files() -> Vec<SstFile>` backed by a checkpoint to protect against GC?

## References

- [Issue #905: Key Count (or Approximate)](https://github.com/slatedb/slatedb/issues/905)
- [Comment proposing this design](https://github.com/slatedb/slatedb/issues/905#issuecomment-3765276221)
- [PR #1220: RFC discussion](https://github.com/slatedb/slatedb/pull/1220)
- [criccomini's metadata approach comment](https://github.com/slatedb/slatedb/pull/1220#issuecomment-3838410526)
- [criccomini's struct proposal comment](https://github.com/slatedb/slatedb/pull/1220#issuecomment-3842567040)
- [criccomini's SstReader/SstFile proposal](https://github.com/slatedb/slatedb/pull/1220#issuecomment-2848206564)
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
- **2026-02-11**: Major revision — replaced `Db::metadata(range)` / `RangeMetadata` / `SizeEstimate` trait approach with lower-level primitives: `Db::manifest()`, `SstReader`, `SstFile` with `stats()` and `index()` methods. Removed `RangeMetadata`, `SstMetadata`, `MemtableMetadata`, `Coverage` enum, and `SizeEstimate` trait. Memtable stats now exposed via `StatRegistry` metrics instead of `MemtableMetadata` structs. Added "Size and Cardinality Estimation" section describing estimation algorithms at three levels of accuracy. (PR #1220 review feedback from @criccomini, Feb 9).
