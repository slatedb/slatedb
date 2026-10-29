# Pinned SST Filters and Indexes

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Background: RocksDB](#background-rocksdb)
- [Motivation](#motivation)
  - [Where point-lookup CPU goes today](#where-point-lookup-cpu-goes-today)
  - [Why the block cache is the wrong home for this data](#why-the-block-cache-is-the-wrong-home-for-this-data)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Overview](#overview)
  - [Configuration](#configuration)
  - [Mapping to RocksDB](#mapping-to-rocksdb)
  - [Where pinned data lives](#where-pinned-data-lives)
  - [When pins are filled](#when-pins-are-filled)
  - [Read path](#read-path)
  - [Filter check before iterator construction](#filter-check-before-iterator-construction)
  - [Carrying pins across manifest updates](#carrying-pins-across-manifest-updates)
  - [Memory](#memory)
  - [Interaction with the block cache](#interaction-with-the-block-cache)
  - [Sizing](#sizing)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Draft

Authors:

* [Zach Schoenberger](https://github.com/zach-schoenberger)

## Summary

This RFC adds one option, `MetadataPinning`, modeled on RocksDB. It pins the
decoded filters and index of SSTs in memory for as long as each SST is part of
the database state:

- `MetadataPinning::L0` pins them for L0 SSTs.
- `MetadataPinning::All` pins them for every SST.

A pinned SST keeps its filters and index on its handle, next to its
`SsTableInfo`. The handle is filled from data that is already in memory: the
writer fills it when it creates the SST, and a read fills it when it retrieves
the metadata through today's path. Later reads use the handle directly and
never consult `DbCache`. For point lookups, a pinned filter is checked before
the per-SST iterator is built, so an SST that cannot contain the key costs a
few hash probes and nothing else. The default, `MetadataPinning::None`, leaves
today's behavior unchanged.

## Background: RocksDB

RocksDB's block-based tables have the same three metadata components as SlateDB
SSTs: a filter, an index, and data blocks. Where the filter and index live is
controlled by `BlockBasedTableOptions`:

- **`cache_index_and_filter_blocks = false` (the default).** Each open table
  reader loads its index and filter when it opens the file and holds them for
  the life of the reader, outside the block cache. A lookup reads them through
  the table reader with no cache lookup. Memory is bounded by the number of open
  table readers (`max_open_files`). Deployments commonly set
  `max_open_files = -1`, which opens and pins every live table.
- **`cache_index_and_filter_blocks = true`.** Index and filter blocks go through
  the block cache like data blocks: they are charged to its capacity, can be
  evicted, and every use is a cache lookup.
  `cache_index_and_filter_blocks_with_high_priority` puts them in the cache's
  high-priority pool so data blocks are evicted first.
- **`pin_l0_filter_and_index_blocks_in_cache = true`** (meaningful only with
  the previous option). L0 table readers hold a reference to their index and
  filter cache entries, so the entries stay in the cache, charged to its
  capacity, and are only evicted when the table reader is freed. The reader
  uses them without a further lookup. L0 is singled out because every point
  lookup consults every L0 file, and L0 files are flushed often and so are cold
  in the cache when they appear.
- **`metadata_cache_options`** is the newer, more general form, and the L0
  option above is now deprecated in its favor. `unpartitioned_pinning` takes a
  `PinningTier`: `kNone`, `kFlushedAndSimilar` (L0 tables no larger than about
  1.5 times `write_buffer_size`, which are flushes and flush-like outputs),
  `kAll`, or `kFallback`, which defers to the deprecated option (`false` maps
  to `kNone`, `true` to `kFlushedAndSimilar`). Like the deprecated option, it
  only has an effect when `cache_index_and_filter_blocks = true`; otherwise the
  table reader already holds everything.

RocksDB therefore has two ways to keep every table's metadata pinned: hold it in
table-reader memory (`cache_index_and_filter_blocks = false`), or hold it in the
block cache with an unevictable reference (`cache_index_and_filter_blocks =
true` and `unpartitioned_pinning = kAll`). Either way, reads go through the
table reader's reference and never look the blocks up again.

RocksDB's point lookup (`Version::Get`) walks the candidate files and calls the
table reader's `Get`. That checks the filter first and searches the index only
when the filter passes. A filter-negative file costs a filter probe, with no
iterator and no cache lookup.

SlateDB has the equivalent of `cache_index_and_filter_blocks = true` only:
filters and indexes are always read through `DbCache`. `SplitCache` is the
counterpart of the high-priority pool. Its separate `meta_cache` keeps data
blocks from evicting metadata, but every read is still a cache lookup. SlateDB
has no counterpart to either pinning mode. This RFC adds both.

## Motivation

### Where point-lookup CPU goes today

A `get` builds one `SstIterator` per candidate SST (every L0 SST plus at most
one SST per sorted run) and initializes them through `GetIterator`. For each
candidate, `FilterIterator::init` calls `TableStore::read_filters`, which looks
the filter up in `DbCache`, then evaluates it. Only a filter-positive SST goes
on to `TableStore::read_index` and a data block.

A CPU profile of a production reader doing almost nothing but point lookups,
with every filter and index resident in a foyer cache, shows:

| Component                                   | Share of total CPU |
|---------------------------------------------|--------------------|
| `read_filters` (all of it)                  | 13.5%              |
| … of which the `DbCache` lookup             | 10.0%              |
| … of which the filter probe                 | 2.3%               |
| `read_index`                                | 2.8%               |

The `DbCache` lookup is a shard `RwLock`, hit/miss metrics, an entry
clone/drop, and an `Arc` clone. It costs more than four times the bloom probe it
serves. Filter reads also run about 4.5 times as often as index reads. Index
reads also come from scans and from SSTs that have no filter, so this ratio is
a lower bound: at least roughly 80% of candidate SSTs are filter-negative.
Those SSTs still pay for an iterator, a cache lookup, and an `await`, only to
be discarded.

### Why the block cache is the wrong home for this data

Filters and indexes are immutable for the life of an SST, are needed by every
point lookup that touches the SST, and are small compared to data blocks. A
general cache optimizes for the opposite: mutable residency, eviction, and
admission across many entries of varying value. Paying for those mechanisms on
every probe buys nothing when the workload keeps all filters resident anyway.

SlateDB already offers two ways to keep this metadata resident.
`SplitCache` gives filters and indexes their own `meta_cache`, so data blocks
cannot evict them. [RFC 0031](0031-block-cache-policy.md) lists "keep indexes
and filters in memory" as a motivating workload and controls which entries are
inserted into the cache. Both make sure the metadata is in the cache, but every
read still pays for the cache lookup.

## Goals

- Offer RocksDB's two pinning modes, L0 only and all SSTs, with matching
  semantics.
- Remove the `DbCache` lookup from reads of pinned filters and indexes.
- Skip iterator construction for filter-negative SSTs in point lookups.
- Keep results, filter metrics, and read tracing identical to the unpinned path.
- Make it opt-in; the default behavior is unchanged.

## Non-Goals

- Changing the SST format, filter format, index format, or manifest format.
- Partitioned or two-level indexes and filters (RocksDB
  `partition_filters`/`index_type = kTwoLevelIndexSearch`), and with them
  `pin_top_level_index_and_filter`.
- Reducing `DbCache` overhead for data blocks. That is worth doing separately
  and is complementary.
- Reducing per-SST iterator setup cost for filter-positive SSTs.
- Pinning stats blocks.
- Pinning filters and indexes separately.
- Loading metadata only to pin it. The pin is filled from data that is already
  in memory.

## Design

### Overview

```text
            get(key)
               │
   candidate SSTs (L0 + covering SST per sorted run)
               │
   ┌───────────┴───────────┐
   │ pinned filter?        │
   ├── yes ────────────────┼── no ─────────────────────────┐
   │ probe in place        │                               │
   │  negative → skip SST  │   build SstIterator           │
   │  positive → build     │   FilterIterator::init        │
   │   SstIterator with    │     read_filters ── DbCache / │
   │   evaluator resolved  │       (fills the pin)  object │
   │                       │                        store  │
   └───────────┬───────────┘───────────────────────────────┘
               │
      read_index ── pinned? use it : DbCache / object store (fills the pin)
               │
          data block (DbCache, unchanged)
```

### Configuration

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataPinning {
    /// Do not pin. Filters and indexes are read through `DbCache`.
    #[default]
    None,
    /// Pin the filters and index of every L0 SST for as long as it is live.
    L0,
    /// Pin the filters and index of every SST, L0 and sorted runs, for as long
    /// as it is live.
    All,
}
```

Exposed as `Settings::metadata_pinning` and `DbReaderOptions::metadata_pinning`.
The option reaches the language bindings through `Settings`, so the bindings
need no code change.

- Filters and indexes are pinned together, as in RocksDB. Every candidate SST
  needs its filter, and the filter-positive ones then need their index.
  Pinning only one leaves the other on the cache path for the same lookups.
- Level is the selection criterion, as in RocksDB. In SlateDB, an SST's level
  never changes: L0 SSTs are memtable flushes and sorted-run SSTs are compaction
  outputs, and compaction always writes new SSTs. So "is this SST pinned" is
  decided once, when the SST enters the database state. With segments
  ([RFC 0024](0024-segment-oriented-compaction.md)), L0 means the L0 of every
  segment.

### Mapping to RocksDB

| RocksDB | SlateDB |
|---|---|
| `cache_index_and_filter_blocks = true`, `unpartitioned_pinning = kNone` | `MetadataPinning::None` (today's behavior) |
| `cache_index_and_filter_blocks_with_high_priority = true` | `SplitCache` with a `meta_cache` (exists today) |
| `pin_l0_filter_and_index_blocks_in_cache = true`, or `unpartitioned_pinning = kFlushedAndSimilar` | `MetadataPinning::L0` |
| `unpartitioned_pinning = kAll`, or `cache_index_and_filter_blocks = false` with `max_open_files = -1` | `MetadataPinning::All` |

SlateDB's L0 is the set of memtable flushes, so `MetadataPinning::L0`
selects the same tables as `kFlushedAndSimilar` without its size heuristic.
RocksDB needs that heuristic because intra-L0 compaction and ingestion can put
large files in L0. SlateDB L0 SSTs are, today, only ever memtable flushes. If
SST ingestion lands with ingestion into L0, this mode may need the same size
heuristic.

There is one deliberate difference. RocksDB's in-cache pinning modes keep the
entries inside the block cache and charged to its capacity. In SlateDB, pinned
metadata for both modes lives outside `DbCache`, as with RocksDB's
`cache_index_and_filter_blocks = false`. `DbCache` implementations such as foyer
cannot make an entry unevictable. Holding an entry handle keeps the value alive
after the cache evicts its key, so the cache's capacity accounting would stop
matching memory. Keeping pinned data outside the cache, with its own gauge, is
the honest accounting.

### Where pinned data lives

`SsTableInfo` is part of the manifest (`CompactedSsTableV2.info`), so runtime
state cannot go in it. The pin slot sits beside it on `SsTableHandle`, which
plays the role of RocksDB's table reader:

```rust
pub struct SsTableHandle {
    pub id: SsTableId,
    pub(crate) format_version: u16,
    pub info: SsTableInfo,
    /// Runtime only. `None` when the SST is not selected for pinning.
    /// Shared by every clone of this handle.
    pinned: Option<Arc<PinnedSstMetadata>>,
}

pub(crate) struct PinnedSstMetadata {
    filters: std::sync::OnceLock<Arc<[NamedFilter]>>,
    index: std::sync::OnceLock<Arc<SsTableIndexOwned>>,
}
```

- `SsTableView` holds its handle by value and is cloned freely. Every clone
  shares the slot, so a lookup is one pointer dereference and one atomic load.
  There is no hash, no lock, and no metric.
- The slot is `None` unless the SST is selected. With pinning off, every handle
  costs one extra word and no allocation.
- `SsTableHandle` derives `PartialEq` and `Serialize` today. The new field
  needs a hand-written `PartialEq` that compares `id`, `format_version`, and
  `info`, and `#[serde(skip)]`. `Debug` is already written by hand. The field
  is private, so the public API is unchanged.
- Each `OnceLock` is written once, by whichever path first holds the value in
  memory. A later `set` is a no-op. No path waits on the cell, because no path
  loads only to fill it.
- When the last handle for an SST is dropped, for example after compaction
  removes it and older snapshots are released, the slot drops and its memory is
  freed.

### When pins are filled

RocksDB loads a table's index and filter when it opens the table reader. SlateDB
does not load anything to pin. The slot is filled only when the filters or
index of a selected SST are already in memory:

- When the writer creates an L0 SST. The memtable flusher builds the filters
  and the index in memory to write the SST. `EncodedSsTable` holds both, and
  `TableStore::write_sst` takes it and returns the handle, so the handle enters
  the state with its slot already filled.
- When a read retrieves them. `TableStore::read_filters` and
  `TableStore::read_index` store the value they return in the slot, whether it
  came from `DbCache` or from the object store.

Sorted-run SSTs are compaction outputs, written by the compactor process. With
`MetadataPinning::All`, a reader or writer pins them on its first read of each.
The same holds for every SST in the initial manifest at `Db` and `DbReader`
open. The first read of an SST that this process did not create costs what it
costs today, and later reads are pinned. The slot is carried across manifest
installs (see below), so this happens once per SST lifetime, not once per
compaction.

Manifest installs never wait on metadata I/O, so data visibility for readers is
unchanged. Nothing new runs in the background.

### Read path

`TableStore::read_filters` and `TableStore::read_index` take the handle they
already receive and do the following:

1. If the handle has a slot and the slot holds the value, return it, recording
   `lookup = pinned` when a read trace is active.
2. Otherwise, use today's path unchanged. If the handle has a slot, store the
   result in it before returning.

Because both functions keep their signatures, every caller benefits: point
lookups and scans.

### Filter check before iterator construction

This is the SlateDB counterpart of RocksDB checking the filter inside the table
reader's `Get`. `build_l0_point_iters` and `build_sr_point_iters` consult the
slot before calling `SstIterator::new_owned_with_stats`:

```text
for handle in candidates:
    match table_store.pinned_filters(handle, read_trace, level):
        Some(filters):
            evaluator = FilterEvaluator::new_point(key, filter_context, db_stats)
            evaluator.evaluate(filters, ...)
            if evaluator.is_filtered_out(): continue      // no iterator, no await
            push SstIterator::with_evaluated_filter(handle, evaluator)
        None:
            push SstIterator::new_owned_with_stats(handle, ...)   // today
```

- `TableStore::pinned_filters` is synchronous. It returns the slot value when
  present and opens the same read-filter span that `read_filters` opens, with
  `lookup = pinned`. So the trace of a pinned point lookup has the same spans
  and fields as today.
- Reusing `FilterEvaluator` keeps `FilterContext` handling (pluggable filters,
  [RFC 0022](0022-pluggable-filter.md)), the AND across multiple filters, the
  positive/negative counters, and false-positive accounting unchanged.
- A positive SST receives the already-evaluated evaluator, so
  `FilterIterator::init` does not read the filter a second time.
- Filter-negative SSTs no longer occupy `GetIterator`'s lookahead window, so
  `max_parallel` concurrent initializations go to SSTs that can contain the key.

### Carrying pins across manifest updates

A handle created by cloning an existing handle shares its slot automatically.
Handles decoded from a newly read manifest have no slot. That happens in:

- `DbReader::rebuild_state` on every manifest refresh, and
- `StateModifier::merge_remote_manifest` on the writer, which adopts the
  compactor's sorted runs. The writer's own L0 views are cloned by
  `merge_writer_and_compactor`, so only the sorted-run handles arrive without
  a slot.

Both paths already hold the prior state when they build the new one:
`rebuild_state` clones it on its first line, and `StateModifier::new` clones
the current copy-on-write state. Both call
`carry_pins(prev: &ManifestCore, next: &mut ManifestCore)` before installing
the new state. `carry_pins` builds a temporary map from `SsTableId` to slot out
of the prior state's `sst_views()`, then walks the new state once and, for each
handle:

- If the map has a slot for that SST id, it installs that slot on the handle.
- Otherwise, if the mode selects the SST, it gives the handle a fresh, empty
  slot.

The map is dropped when `carry_pins` returns. The cost is O(number of SSTs) per
manifest install and nothing per read. A retired SST never re-enters the
manifest, so no state outlives the install. Without this step, every refresh
would silently discard every pin. This is the work RocksDB's table cache does
by keeping table readers keyed by file number across versions.

External SSTs (clones and checkpoints) are keyed by `SsTableId`, the same as
`DbCache` keys, so they pin like any other SST.

### Memory

As in RocksDB, pinned memory is proportional to live SSTs and has no eviction:

- With `MetadataPinning::L0`, it is bounded by the L0 size the writer allows.
  `l0_max_ssts` is enforced per tree, so the bound is the number of trees
  times `l0_max_ssts`. A manifest union can push the total above that for a
  time. It stays small.
- With `MetadataPinning::All`, it is the metadata of the entire database.
  This is the same trade RocksDB users make with
  `cache_index_and_filter_blocks = false` and `max_open_files = -1`, and it
  should be sized with the formulas below.

There is no byte limit on pinned memory. SlateDB has no byte accounting outside
the cache layer, and size limits belong there. The L0 mode is the bounded mode.

### Interaction with the block cache

Pinned entries are not read through `DbCache`. The pin is filled by the
existing read path, so cache population, including a hybrid disk tier, behaves
as it does today. Any copy already in the cache (for example, inserted on write
by the `BlockCachePolicy` of RFC 0031) stops receiving hits and is evicted by
the cache's normal policy, so long-term capacity is not double-counted.

### Sizing

- Filters: `keys × filter_bits_per_key / 8`. At 10 bits per key, 100M keys take
  about 125 MB.
- Indexes: about `data_bytes / block_size × (avg_first_key_len + per-entry overhead)`,
  where the per-entry overhead is the `BlockMeta` offset, length, and flatbuffer
  framing. For example, under assumed values of 64 GiB of data, 4 KiB blocks,
  and 32-byte first keys with roughly 24 bytes of overhead, that is about 0.9 GB.
  The real figure depends heavily on the workload and should be measured before
  enabling `MetadataPinning::All` on large databases.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

Results are unchanged. Scans benefit from pinned indexes and from pinned filters
when a range or prefix filter policy is registered.

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
- [x] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

The manifest format is unchanged. Pins are carried across manifest installs by
SST id, which covers external SSTs referenced by clones.

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

The compactor is unchanged. It reads no filters, and its `TableStore` has no
cache to skip.

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [x] Block cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

The option reaches the bindings through `Settings`. No binding code changes.

## Operations

### Performance & Cost

- Latency and CPU: pinned filter and index reads skip the `DbCache` lookup.
  Filter-negative SSTs skip iterator construction. In the motivating profile,
  this targets up to about 11% of total CPU for filters (`read_filters` minus
  the probe) and up to about 2.8% for indexes. The effect of skipping iterator
  construction must be measured. `MetadataPinning::L0` alone captures the
  share that comes from L0 SSTs.
- Object-store requests: unchanged. No request is issued only to pin.
- Memory: the decoded metadata of the selected SSTs, outside `DbCache`.

### Observability

- Configuration: `MetadataPinning`, `None` by default.
- Metrics (names illustrative):
  - `pinned_bytes{level=l0|sorted_run, component=filter|index}` gauge
  - `pinned_ssts{level}` gauge
- Tracing: filter and index read spans record `lookup = pinned` for pinned
  reads.

### Compatibility

- No on-disk or manifest format change.
- Public API: additive (`MetadataPinning`). `SsTableHandle` gains a private
  field; equality and serialization are unchanged.
- Mixed versions: not applicable; pinning is process-local.

## Testing

- Unit tests:
  - `PartialEq` and `Serialize` ignore the slot.
  - The pinned-bytes gauge drops when the last handle for an SST drops.
  - `carry_pins` carries slots across `rebuild_state` and
    `merge_remote_manifest`, gives fresh slots only to selected SSTs, and
    drops slots for retired SSTs.
  - `MetadataPinning::L0` never pins sorted-run SSTs.
  - A flushed L0 SST enters the state with its slot filled.
  - The first read of a selected SST fills the slot, and the second read uses
    it without a cache lookup.
- Differential tests: randomized workloads produce identical `get` and `scan`
  results, and identical filter metrics, with `None`, `L0`, and `All`.
- Deterministic simulation tests: run existing DST scenarios with each pinning
  mode.
- Performance tests: a microbenchmark of `get` across many sorted runs with a
  high filter-negative rate, measuring CPU per lookup for each mode, and a
  profile of a point-lookup-heavy reader.

## Rollout

- Milestones / phases:
  1. Pin slot, `carry_pins`, fill on flush and on read, `MetadataPinning::L0`,
     and the filter check before iterator construction.
  2. `MetadataPinning::All` and the sizing docs.
- Feature flags / opt-in: the option defaults to `None`.
- Docs updates: document the option, the RocksDB mapping, and the sizing
  formulas.

## Alternatives

**Status quo.** Keep filters and indexes in `DbCache` and size the cache to hold
them. The cache lookup remains the dominant cost of a filter-negative SST.

**RFC 0031 `BlockCachePolicy` only.** Controls which entries are inserted, not
how they are read. Every read still pays the lookup.

**A dedicated metadata cache.** This already exists: `SplitCache` routes
filters, indexes, and stats to a separate `meta_cache` and data blocks to
`block_cache`. It keeps data-block churn from evicting metadata, which is the
residency half of the problem. It does not address the CPU half: every probe
is still a `DbCache` lookup (a foyer shard lock, metrics, and an entry and
`Arc` clone), so the cost in the motivating profile is the same whether the
metadata sits in a shared cache or a dedicated one. Pinning is complementary.
`SplitCache` remains the right home for unpinned metadata. A new concurrent map
keyed by SST id would have the same problem on a smaller scale: a hash, a lock
or shard, and refcounting on every probe.

**Pinning inside `DbCache`, as RocksDB's `unpartitioned_pinning` does.** Hold a
cache entry handle on the SST handle instead of a private copy. This would charge pinned
memory to cache capacity as RocksDB does, but `DbCache` has no pin or unevictable
operation. With foyer, the cache would evict the key while the handle keeps the
value alive, so capacity accounting would drift from real memory. Adding a pin
operation to the `DbCache` trait would be required for every implementation.

**Storing the filter and index in `SsTableInfo`.** This would remove the
runtime slot and the hand-written `PartialEq`. But `SsTableInfo` is embedded in
the manifest for every SST, so the manifest would carry every filter and index,
be rewritten with them on every flush and compaction, and be read with them by
every process that opens it. That is a manifest format change and is out of
scope.

**Loading pins on manifest install.** Queue a metadata load for every newly
selected SST when it enters the state, as RocksDB does when it opens a table
reader. This makes the first read of a new SST pinned, but it issues
object-store requests only to pin, and nothing bounds the parallelism of such
loads today. The RFC fills pins only from data that is already in memory. See
Open Questions.

**Per-component byte budgets (an earlier draft of this RFC).** Separate
`filter_budget_bytes` and `index_budget_bytes`. This gives finer control, but
it has no RocksDB analogue, it makes users size two budgets instead of choosing
a level, and SlateDB has no byte accounting outside the cache layer to build it
on. Level-based selection matches RocksDB and the access pattern: every lookup
consults every L0 SST, and every lookup consults one SST per sorted run.

**A persistent registry of slots.** Keep a map from `SsTableId` to a weak
reference to each slot on the `TableStore`, and consult it on every install.
Both install paths already hold the prior state, so a diff against it does the
same work with no weak references, no pruning, and no owner.

**RocksDB-style table cache of open table readers.** Moves all per-SST state,
including parsed footers, into a bounded table-reader cache. That is a larger
restructuring than needed; the SST handle already plays the role of the table
reader, and `carry_pins` plays the role of the table cache's identity map.

**Partitioned filters and indexes.** Reduce memory for very large SSTs by
pinning only a top-level partition, as with RocksDB's
`pin_top_level_index_and_filter`. This requires a format change and is left for
future work.

## Open Questions

- Should manifest installs prefetch the metadata of newly live selected SSTs,
  as RocksDB's table opens do? Nothing bounds the parallelism of such loads
  today. A prefetch needs a bound, and the value of that bound needs
  discussion before it is added.

## References

- [RFC 0022: Pluggable Filter](0022-pluggable-filter.md)
- [RFC 0023: Cache Manager](0023-cache-manager.md)
- [RFC 0024: Segment-Oriented Compaction](0024-segment-oriented-compaction.md)
- [RFC 0031: Block Cache Policy](0031-block-cache-policy.md)
- [RocksDB wiki: Block Cache, caching index, filter, and compression dictionary blocks](https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-filter-and-compression-dictionary-blocks)
- [RocksDB wiki: Memory usage in RocksDB, indexes and filter blocks](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB#indexes-and-filter-blocks)
- [RocksDB `table.h`: `BlockBasedTableOptions`, `MetadataCacheOptions`, `PinningTier`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/table.h)
- [slatedb#2203: Trim per-SST CPU on the point-lookup path](https://github.com/slatedb/slatedb/pull/2203)
