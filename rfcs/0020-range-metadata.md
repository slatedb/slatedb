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
      + [Visibility changes to existing types](#visibility-changes-to-existing-types)
      + [Memtable stats via `StatRegistry`](#memtable-stats-via-statregistry)
   * [Size and Cardinality Estimation](#size-and-cardinality-estimation)
      + [Coarse estimate — SST-level, no I/O beyond `manifest()`](#coarse-estimate-sst-level-no-io-beyond-manifest)
      + [Refined estimate — block-level for boundary SSTs](#refined-estimate-block-level-for-boundary-ssts)
      + [Record counting — via per-block stats](#record-counting-via-per-block-stats)
      + [L0 handling](#l0-handling)
      + [Staleness and GC](#staleness-and-gc)
      + [Memtable contribution](#memtable-contribution)
   * [Implementation Phases](#implementation-phases)
      + [Phase 1 - SST Format Changes](#phase-1-sst-format-changes)
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

Status: Accepted

Authors:

* [FiV0](https://github.com/FiV0)

## Summary

This RFC proposes:
1. Adding a per-SST stats block to the SST file format containing aggregate statistics (`num_puts`, `num_deletes`, `num_merges`, `raw_key_size`, `raw_val_size`) and per-block statistics (`block_stats`), enabling efficient record counting within key ranges.
2. Exposing lower-level primitives — `DbReader::manifest()`, `SstReader`, and `SstFile` — that allow users to walk the manifest, open individual SSTs, and read per-SST stats and index data for size and cardinality estimation. Memtable stats are exposed via the existing `StatRegistry` (accessible through `Db::metrics()`).

Rather than providing a single high-level `Db::metadata(range)` function, this approach exposes modular building blocks. Users call `db.manifest()` to discover which SSTs exist, then use `SstReader` to open and inspect individual SSTs. This makes SlateDB more composable and avoids coupling the estimation logic to a specific API shape.

## Motivation

Users want to understand data distribution and storage usage for specific key ranges to support:

- **Cost estimation**: Calculating storage costs or scan costs for specific data partitions. For example, if a key range is an index in a query engine, it is good to know an approximate size when joining with other data.
- **Bulk operations**: Planning exports, migrations, or splits based on estimated sizes
- **Capacity planning**: Understanding storage requirements for different key ranges
- **Data modeling**: Understanding how data is distributed across the key space

Currently there is no better way than to scan the whole range to get an estimate or approximation of the data size.

Beyond size estimation, some workloads need to count records in a key range efficiently — for example, append-only logs computing consumption lag, query planners estimating `records_in_range()`, or systems deciding where to split data. Today this requires a full scan. Adding per-block record counts to the stats block enables block-level counting with at most two data block reads per boundary SST.

An earlier revision of this RFC proposed high-level functions (`estimate_size_with_options` and `estimate_key_count`) with configuration options like `error_margin`, `include_memtables`, and `include_files`. However, different use cases demand different knobs — compressed vs uncompressed sizes, varying accuracy vs I/O tradeoffs, inclusion or exclusion of memtables, etc. Baking all of these into the API through configuration options risks making the interface rigid and opinionated. Instead, we expose lower-level building blocks so users can compute whatever they need. For example, a user interested in key counts can derive `num_puts + num_merges - num_deletes` from per-SST stats, while a user interested in on-disk size can sum `SsTableHandle::estimate_size()` across covering SSTs, and one needing finer accuracy can use `SstFile::index()` to estimate at block granularity.

## Goals

- Add a per-SST stats block with aggregate and per-block statistics, referenced by `stats_offset`/`stats_len` in `SsTableInfo` and sst.fbs
- Expose `Db::manifest()` so users can discover SSTs covering a key range
- Provide `SstReader` and `SstFile` for opening individual SSTs and reading their stats (as `SstStats`) and index data
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

Returns a reference to the current in-memory `ManifestCore`. No I/O. `ManifestCore` is already public (re-exported via `slatedb::manifest`) .

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
        object_store_cache_options: Option<ObjectStoreCacheOptions>,
    ) -> Self;

    /// Errors if the file was GC'd.
    pub async fn open(&self, id: Ulid) -> Result<SstFile, crate::Error>;

    /// Creates an `SstFile` from an existing `SsTableHandle` (no I/O needed).
    pub fn open_with_handle(&self, handle: SsTableHandle) -> SstFile;
}
```

`SstReader` wraps the read-only functionality currently internal to `TableStore`. SST paths are resolved as `{root}/compacted/{ulid}.sst`. Internally, `open()` needs to only decode the metadata info, which requires handling the SST codec and compression. The implementation will need to take care of constructing the appropriate decoding machinery (currently `SsTableFormat`) from the provided configuration. If `object_store_cache_options` is provided, the passed-in `object_store` is wrapped in a `CachedObjectStore` (mirroring the behavior of `DbBuilder`). This allows the `SstReader` to share the on-disk object store cache that `DbBuilder` sets up internally. Note that this cache path can be shared between processes.

```rust
pub struct SstFile {
    id: Ulid,
    handle: SsTableHandle,
    tablestore: Arc<TableStore>,
}

impl SstFile {
    /// Returns the SST's ULID identifier.
    pub fn id(&self) -> Ulid;

    /// Returns the SST file metadata
    pub async fn metadata(&self) -> Result<SstFileMetadata, crate::Error>;

    /// Returns SsTableInfo from the metadata block.
    pub fn info(&self) -> &SsTableInfo;

    /// Reads the stats block from object storage. Returns `None` for old
    /// SSTs that were written before the stats block was added.
    pub async fn stats(&self) -> Result<Option<SstStats>, crate::Error>;

    /// Returns `(block_offset, first_key)` pairs from the SST index block.
    pub async fn index(&self) -> Result<Vec<(u64, Bytes)>, crate::Error>;
}
```

```rust
pub struct SstStats {
    pub num_puts: u64,
    pub num_deletes: u64,
    pub num_merges: u64,
    pub raw_key_size: u64,
    pub raw_val_size: u64,
    pub block_stats: Vec<BlockStats>,
}

pub struct BlockStats {
    pub num_puts: u16,
    pub num_deletes: u16,
    pub num_merges: u16,
}
```

Caching the `SsTableHandle` means we can reuse `tablestore.read_index`, which requires an `SsTableHandle`.

The `SstFile::info()` call is primarily for users that don't have access to a `ManifestCore` (e.g. if they listed files in object storage outside of SlateDB and wanted to inspect them). Users with a `ManifestCore` already have `SsTableInfo` available via `SsTableHandle`and can construct a `SstFile` via `SstFile::open_with_handle()`.

The downside is that `open()` requires a read to obtain the `SsTableHandle` even if the caller only wants to call `metadata()`, which doesn't need it. This is a fine tradeoff.

`index()` calls `SsTableFormat::read_index()`, which reads `info.index_offset..info.index_offset + info.index_len`, decompresses, and returns an `SsTableIndexOwned`. The method materializes `Vec<(u64, Bytes)>` from the FlatBuffer `BlockMeta` entries (each has `offset()` and `first_key()`). Caching uses `DbCache::get_index` / `insert` keyed by `(sst_id, index_offset)`, matching the existing pattern in `TableStore::read_index()`.

The existing `SstFileMetadata` struct in `tablestore.rs` (currently `pub(crate)`) is made `pub`.

#### Visibility changes to existing types

- **`SortedRun::tables_covering_range()`**: Change from `pub(crate)` to `pub`. Lets users find which SSTs in a sorted run overlap a given key range.
- **`SsTableHandle::estimate_size()`**: Change from `pub(crate)` to `pub`. Enables quick no-I/O size estimates.
- **`SsTableHandle::visible_range()`**: Transforming the `visible_range` of `SsTableHandle` from the internal `BytesRange` to a `RangeBounds<Bytes>`.

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

For cardinality: open each covering SST with `SstReader` and call `SstFile::stats()` to get `SstStats`. Sum `num_puts + num_merges - num_deletes` across all sorted runs. This overestimates because the same key may appear in multiple sorted runs (L0 overlaps, and compaction hasn't merged them yet). The overestimation factor is roughly proportional to the number of sorted runs containing the key.

#### Refined estimate — block-level for boundary SSTs

Most SSTs returned by `tables_covering_range()` are fully contained within the query range — their stats apply directly. Only the first and last SST in each sorted run partially overlap. For these two boundary SSTs, call `sst_file.index()` to get the index `[(offset, first_key), ...]`. Binary search for the range start key in the first boundary SST to find where the range begins; binary search for the range end key in the last boundary SST to find where it ends.

These offsets are compressed/stored sizes since the block index tracks on-disk offsets.

For proportional stat scaling on boundary SSTs: compute `covered_fraction = covered_bytes / total_sst_stored_bytes` and scale `SstStats` fields proportionally (e.g. `estimated_puts = num_puts * covered_fraction`). This assumes uniform key distribution within the SST.

There is existing related work in `estimate_bytes_before_key` (in `utils.rs`) that does SST-granularity estimation across sorted runs. The block-level approach here goes one level deeper within a single SST.

#### Record counting — via per-block stats

For each overlapping SST, call `sst_file.stats()` to get `SstStats` with its `block_stats` vector. Use `sst_file.index()` to binary search for the range start and end block indices. Interior blocks are counted by summing `num_puts + num_deletes + num_merges` per block from `block_stats`. For exact counts, read the two boundary blocks and count matching entries — at most 2 data block reads per SST. For approximate counts, include boundary blocks in full without reading them, overcounting by at most 2 blocks' worth of records per SST.

Summing across SSTs may count duplicate keys across levels. This is exact for append-only workloads where keys are never overwritten. For general key-value workloads, it is an overestimate — a deduplicated count requires merge iteration.

#### L0 handling

L0 SSTs are not sorted relative to each other and can all overlap with the query range. Every L0 SST must be checked. For each, the same refinement as above applies: if the SST's `first_key` and key range suggest partial overlap, use `index()` to refine. Since L0 SSTs tend to be smaller (pre-compaction), the coarse estimate is often sufficient.

#### Staleness and GC

The manifest snapshot from `manifest()` may diverge from the actual DB state by the time `SstReader::open()` is called. GC may have deleted SSTs. Treat `open()` errors on missing SSTs as best-effort — skip them. For stronger guarantees, create a checkpoint before reading.

#### Memtable contribution

Add memtable stats from `db.metrics()` (`memtable_num_puts`, `memtable_raw_key_bytes`, etc.) to the totals. These are aggregates across all unflushed memtables and are not range-specific.

### Implementation Phases

#### Phase 1 - SST Format Changes

**Stats block.** Add a stats block to the SST file format containing per-SST statistics. This follows the same pattern as the existing index and filter blocks — the SST metadata (`SsTableInfo` in `sst.fbs`) gains `stats_offset` and `stats_len` fields that point to the block's location within the file. Add an `SstStats` struct and a `read_stats` method to `TableStore`.

The stats block is a FlatBuffers-encoded `SstStats` table containing aggregate statistics and per-block statistics:

```flatbuffers
table SstStats {
    num_puts: ulong;
    num_deletes: ulong;
    num_merges: ulong;
    raw_key_size: ulong;
    raw_val_size: ulong;
    block_stats: [BlockStats];
}

table BlockStats {
    num_puts: ushort;
    num_deletes: ushort;
    num_merges: ushort;
}
```

The `block_stats` vector is parallel to the SST index — `block_stats[i]` corresponds to the `i`th `BlockMeta` entry. The builder tracks per-block counters for each record type and records the final counts when finishing each block. `BlockStats` is a FlatBuffers `table` (not `struct`) to allow adding fields in future without breaking compatibility.

The total record count for an SST is `num_puts + num_deletes + num_merges`, derivable from the aggregate fields. The same breakdown at the block level enables finer-grained record counting for boundary SSTs (see "Record counting" in the estimation section).

Since `SsTableInfo` is a FlatBuffers table, the new `stats_offset`/`stats_len` fields can be appended without breaking existing readers — missing fields return their default value (`0` for `ulong`). The `flatc --conform` CI check enforces that schema changes are purely additive. Old SSTs without a stats block will have `stats_offset = 0` and `stats_len = 0`, and `SstFile::stats()` returns `None` for these.

This approach keeps `SsTableInfo` (and therefore the manifest) lean — only 16 bytes per SST are added rather than the full 40 bytes of stats. This matters for large DBs where manifest size is dominated by SST infos. The stats are read from the SST file on demand via `SstFile::stats()`.

- **`Db::manifest()`**: Returns a clone of the current `ManifestCore`. No I/O.

#### Phase 2 - `SstReader`, `SstFile`, and `Db::manifest()`

- **`SstReader`**: New public struct wrapping object store access for SSTs. Internally reuses `SsTableFormat::read_info` and `SsTableFormat::read_index`.
- **`SstFile`**: New public struct returned by `SstReader::open()` or `SstReader::open_with_handle()`. Provides `info()` (returning `SsTableInfo`), `stats()` (reading the stats block with aggregate and per-block stats), and `index()` (block offset/key pairs).
- **`SstFileMetadata`**: Change existing struct in `tablestore.rs` from `pub(crate)` to `pub`. Returned by `SstFile::metadata()`.
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
- [x] SST format or block format - Adds stats block (with per-block stats) to SST files and `stats_offset`/`stats_len` to `SsTableInfo`

### Ecosystem & Operations

- [x] CLI tools - `slatedb-cli` will expose SST stats inspection
- [x] Language bindings (Go/Python/etc) - New API to expose in bindings
- [x] Observability (metrics/logging/tracing) - Memtable stats exposed via `StatRegistry`

## Operations

### Performance & Cost

SST stats block:
- 16 bytes per SST added to `SsTableInfo` (`stats_offset` + `stats_len`). Stats block itself is stored in the SST file, not the manifest.
- Stats block size scales with block count: ~40 bytes for aggregate fields plus ~14 bytes per block for `BlockStats` (FlatBuffers table overhead + 3 × `ushort`). For a 256 MB SST with 4 KB blocks (~65K blocks), the stats block is ~950 KB.

`Db::manifest()`:
- Clones in-memory state. No I/O. Cost proportional to number of SST handles in the manifest.

`SstReader::open()`:
- One object store read per SST to load the metadata. `open_with_handle()` requires no I/O.

`SstFile::stats()`:
- One object store read per SST to load the stats block. Skipped for old SSTs without a stats block.
- Record counting requires both stats (for `block_stats`) and index (for binary search on keys). Approximate count: stats + index reads. Exact count: + at most 2 data block reads per boundary SST.

`SstFile::index()`:
- One index block read per SST, cacheable via the block cache. No changes to `BlockMeta` format.

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

- `SsTableInfo` gets new `stats_offset`/`stats_len` fields in Phase 1. Old SSTs without a stats block will have these fields default to `0` (FlatBuffers default), and `SstFile::stats()` returns `None`.
- New APIs are additive only
- No breaking changes to existing APIs
- Language bindings will need to expose new types and methods

## Testing

Unit tests:
- SST stats block encoding/decoding and backwards compatibility with old SSTs (missing stats block returns `None`)
- `stats_offset`/`stats_len` fields in `SsTableInfo` encoding/decoding
- `SstReader::open()`: loading SST footer and constructing `SstFile`
- `SstReader::open_with_handle()`: constructing `SstFile` from an existing `SsTableHandle`
- `SstFile::stats()`: correct reading and population of `SstStats` from the stats block
- `SstFile::index()`: returns correct `(offset, first_key)` pairs matching the SST's block index
- `block_stats` vector: parallel to index, builder correctly tracks per-block put/delete/merge counts
- Backward compatibility: old SSTs without stats return `None`
- `Db::manifest()`: returns current manifest state with L0 and sorted runs
- `SortedRun::tables_covering_range()`: full and partial range overlap detection
- Memtable metrics: correct registration, increment on write, decrement on flush
- Edge cases: empty SSTs, SSTs without a stats block (pre-Phase 1), GC'd SSTs returning errors
- Size estimation algorithm: binary search on index entries, bytes before/after calculation

Not planning to add any specific tests for:
- Integration tests
- Fault-injection/chaos tests
- Deterministic simulation tests
- Formal methods verification
- Performance tests

## Rollout

- Phase 1 (SST stats block with `stats_offset`/`stats_len` in `SsTableInfo`) and Phase 2 (`SstReader`/`SstFile`/`Db::manifest()`) can be implemented as separate PRs.
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

**Per-block record counts in `BlockMeta` (SST index) instead of `SstStats`.** An earlier revision stored per-block counts directly in the SST index (`BlockMeta`). This avoids needing a separate stats read for record counting, but inflates the index — which is on the hot path for every scan and get. Moving per-block counts to the stats block keeps the index lean and isolates the overhead to estimation use cases.

## Open Questions

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
- **2026-02-16**: Stats fields moved into `SsTableInfo` (in `sst.fbs`) instead of a separate footer block. Removed `SstStats` struct — `SstFile::info()` returns `SsTableInfo` directly. `SstFile` now holds `SsTableHandle` + `Arc<TableStore>`. Added `object_store_cache_options` parameter to `SstReader::new()`. (PR #1220 review feedback from @criccomini).
- **2026-02-19**: Reverted to separate stats block approach. Stats fields moved back out of `SsTableInfo` into a dedicated stats block within the SST file, referenced by `stats_offset`/`stats_len` in `SsTableInfo`. Reintroduced `SstStats` struct and `SstFile::stats()` method. This keeps `SsTableInfo` (and the manifest) lean — 16 bytes per SST vs 40 bytes — which matters for large DBs. Added `SstReader::open_with_handle()` for zero-I/O construction from an existing `SsTableHandle`. (PR #1220 review feedback from @rodesai and @criccomini).
- **2026-02-25**: Added per-block record counts as `block_stats: [BlockStats]` in `SstStats` (stats block). `BlockStats` contains `num_puts`/`num_deletes`/`num_merges`, mirroring the SST-level aggregate fields. `BlockStats` uses a FlatBuffers `table` for future extensibility.