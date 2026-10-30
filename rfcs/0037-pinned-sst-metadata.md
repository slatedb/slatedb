# Pinned SST Filters and Indexes

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
  - [Where point-lookup CPU goes today](#where-point-lookup-cpu-goes-today)
  - [Why the block cache is the wrong home for this data](#why-the-block-cache-is-the-wrong-home-for-this-data)
  - [Prior art: RocksDB](#prior-art-rocksdb)
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

## Motivation

### Where point-lookup CPU goes today

A `get` builds one `SstIterator` per candidate SST (every L0 SST plus at most
one SST per sorted run) and initializes them through `GetIterator`. For each
candidate, `FilterIterator::init` calls `TableStore::read_filters`, which looks
the filter up in `DbCache`, then evaluates it. Only a filter-positive SST goes
on to `TableStore::read_index` and a data block.

A CPU profile of a production reader doing almost nothing but point lookups,
with every filter and index resident in a foyer cache, showed where the time
went. The `DbCache` lookup for a filter, which is a shard `RwLock`, hit and
miss metrics, an entry clone and drop, and an `Arc` clone, cost several times
more than the bloom probe it served. Index reads cost a smaller but visible
share. Filter reads ran far more often than index reads, which means that most
candidate SSTs in that workload were filter-negative. Those SSTs still paid for
an iterator, a cache lookup, and an `await`, only to be discarded.

The exact shares depend on the workload, the cache implementation, and the
rest of the read path, so this RFC does not fix them in writing.

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
inserted into the cache. Both make sure that the metadata is in the cache, but
every read still pays for the cache lookup.

### Prior art: RocksDB

RocksDB block-based tables have the same three metadata components as SlateDB
SSTs: a filter, an index, and data blocks. RocksDB has two ways to keep a
table's filter and index pinned in memory:

- With `cache_index_and_filter_blocks = false`, which is the default, each
  open table reader loads its index and filter when it opens the file and
  holds them for the life of the reader, outside the block cache. Memory is
  bounded by the number of open table readers (`max_open_files`).
- With `cache_index_and_filter_blocks = true`, index and filter blocks go
  through the block cache. `metadata_cache_options.unpartitioned_pinning`
  then selects tables whose entries the table reader holds a reference to, so
  the cache cannot evict them: `kNone`, `kFlushedAndSimilar` (L0 tables that
  are flushes or flush-like outputs), or `kAll`. The older
  `pin_l0_filter_and_index_blocks_in_cache` option is the deprecated form of
  `kFlushedAndSimilar`. Memory is bounded by the cache capacity.

Either way, a lookup reads the filter and index through the table reader's
reference and never looks the blocks up again. RocksDB's point lookup checks
the filter inside the table reader's `Get` before it searches the index, so a
filter-negative file costs a filter probe and nothing else.

SlateDB has the equivalent of `cache_index_and_filter_blocks = true` only.
`SplitCache` is the counterpart of RocksDB's high-priority cache pool. SlateDB
has no counterpart to either pinning mode. This RFC adds both.

## Goals

- Offer RocksDB's two pinning modes, L0 only and all SSTs, with matching
  semantics.
- Remove the `DbCache` lookup from reads of pinned filters and indexes.
- Skip iterator construction for filter-negative SSTs in point lookups.
- Keep results and read tracing identical to the unpinned path.
- Keep filter-positive counts identical to the unpinned path. Filter-negative
  counts can be higher with pinning. See
  [Filter check before iterator construction](#filter-check-before-iterator-construction).
- Make it opt-in. The default behavior is unchanged.

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
- A byte limit on pinned memory. See [Memory](#memory) and
  [Open Questions](#open-questions).

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
#[non_exhaustive]
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
The enum is new, so it is `#[non_exhaustive]` from the start. A later mode,
for example a size limit like RocksDB's `kFlushedAndSimilar` heuristic, is
then not a breaking change. The writer option reaches the language bindings
through `Settings`. The reader option needs a binding change. See
[Impact Analysis](#impact-analysis).

- Filters and indexes are pinned together, as in RocksDB. Every candidate SST
  needs its filter, and the filter-positive ones then need their index.
  Pinning only one leaves the other on the cache path for the same lookups.
- Level is the selection criterion, as in RocksDB. L0 SSTs are memtable
  flushes and sorted-run SSTs are compaction outputs. So "is this SST pinned"
  is decided once, when the SST enters the database state. One exception
  exists: a trivial move (`CompactorOptions::enable_trivial_move`) puts an L0
  SST into a sorted run without rewriting it. Such an SST keeps the pin it
  received in L0, in every mode that gave it one, until compaction retires
  it. RocksDB behaves the same way: it decides pinning when it opens a table
  reader, and a trivial move does not reopen the reader. With segments
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
SST ingestion lands with ingestion into L0, this mode will need the same size
heuristic.

There is one deliberate difference. RocksDB's in-cache pinning modes keep the
entries inside the block cache and charged to its capacity. In SlateDB, pinned
metadata for both modes lives outside `DbCache`, as with RocksDB's
`cache_index_and_filter_blocks = false`. `DbCache` implementations such as foyer
cannot make an entry unevictable. Holding an entry handle keeps the value alive
after the cache evicts its key, so the cache's capacity accounting would stop
matching memory. Pinned data therefore lives outside the cache, with its own
gauge.

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
    /// The level the SST was in when it received this slot. It is the
    /// `level` label of the pinned metrics.
    level: PinLevel,
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
- When the last handle for an SST is dropped, the slot drops and its memory is
  freed. Snapshots, transactions, and checkpoints hold older state, so a
  retired SST stays pinned until they are released. See [Memory](#memory).

The mode lives in a `MetadataPinner` on `TableStore`. The pinner decides which
level a fresh slot is given at, creates slots, and owns the pinned metrics. The
table stores of the compactor and the garbage collector use a pinner that pins
nothing.

### When pins are filled

RocksDB loads a table's index and filter when it opens the table reader. SlateDB
does not load anything to pin. The slot is filled only when the filters or
index of a selected SST are already in memory:

- When the writer creates an L0 SST. The memtable flusher builds the filters
  and the index in memory to write the SST. `EncodedSsTable` holds both, and
  `TableStore::write_sst` takes it and returns the handle, so the handle enters
  the state with its slot already filled. Only the main table store does
  this. The compactor's table store writes sorted-run SSTs and gives them no
  slot.
- When a read retrieves them. `TableStore::read_filters` and
  `TableStore::read_index` store the value they return in the slot, whether it
  came from `DbCache` or from the object store.

Sorted-run SSTs are compaction outputs, written by the compactor process. With
`MetadataPinning::All`, a reader or writer pins them on its first read of each.
The same holds for every SST in the initial manifest at `Db` and `DbReader`
open. The first read of an SST that this process did not create costs what it
costs today, and later reads are pinned. The slot is carried across manifest
installs (see below), so this happens once per SST lifetime, not once per
compaction. The `pinned_sst_count` metric shows how much of the selected set
is filled.

Manifest installs never wait on metadata I/O, so data visibility for readers is
unchanged. Nothing new runs in the background.

### Read path

`TableStore::read_filters` and `TableStore::read_index` take the handle they
already receive and do the following:

1. If the handle has a slot and the slot holds the value, return it, recording
   `pinned = true` on the read span when a read trace is active. The span has
   no `cached` field, because no cache lookup happens.
2. Otherwise, use today's path unchanged. If the handle has a slot, store the
   result in it before returning.

Because both functions keep their signatures, every caller benefits: point
lookups and scans.

### Filter check before iterator construction

This is the SlateDB counterpart of RocksDB checking the filter inside the table
reader's `Get`. `GetIterator::from_lsm_tree` lists the candidate SSTs (every
L0 SST, then the covering SST of each sorted run) and builds each per-SST
iterator lazily, when the walk pulls it. `build_point_iter` consults the slot
before it builds the iterator:

```text
candidates = L0 SSTs, then the covering SST of each sorted run   // cheap list
for (view, level) in candidates, pulled lazily by the walk:
    if the key is outside the view's range: skip                 // today
    match table_store.pinned_filters(view, read_trace, level):
        Some(filters):
            evaluator = FilterEvaluator::new_point(key, filter_context, db_stats)
            evaluator.evaluate(filters, ...)
            if evaluator.is_filtered_out(): skip                 // no iterator, no await
            yield SstIterator::with_evaluated_filter(view, evaluator)
        None:
            yield SstIterator::new_owned_in_view_range(view, ...) // today
```

Lazy construction keeps the walk as it is today. `GetIterator` initializes
the newest source first and stops on a hit, so an SST the walk never reaches
is never built and its filter is never evaluated, pinned or not. A
filter-negative pinned SST is skipped when the walk pulls it, so it never
occupies a lookahead slot. Filter positives are therefore counted for the same
SSTs as before. Filter negatives can be counted for more SSTs: while the
lookahead window fills with sources, the walk evaluates every pinned negative
it passes, including some beyond the SST that holds the key. This is a known
difference in the `filter_negatives` metric between modes. The extra
evaluations are hash probes on pinned data and cost no I/O and no cache
lookup.

- `TableStore::pinned_filters` is synchronous. It returns the slot value when
  the SST has a filter and the slot holds it, and opens the same read-filter
  span that `read_filters` opens, with `pinned = true`. So the trace of a
  pinned point lookup has the same spans as today. The `level` argument is
  the tracing level of the SST, used for the span.
- Reusing `FilterEvaluator` keeps `FilterContext` handling (pluggable filters,
  [RFC 0022](0022-pluggable-filter.md)), the AND across multiple filters, the
  positive and negative counters, and false-positive accounting unchanged.
- A positive SST receives the already-evaluated evaluator, so
  `FilterIterator::init` does not read the filter a second time.
- Filter-negative pinned SSTs never occupy `GetIterator`'s lookahead window,
  so `max_parallel` concurrent initializations go to SSTs that can contain
  the key.

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
`MetadataPinner::carry_pins(prev: Option<&ManifestCore>, next: &mut ManifestCore)`
before installing the new state. Opening a writer or a reader calls it with no
prior state, which gives fresh slots only. `carry_pins` builds a temporary map
from `SsTableId` to slot out of the prior state's `sst_views()` and any slot
already on a handle of the new state. It then walks the new state's L0 views,
then its sorted-run views, and for each handle without a slot:

- If the map has a slot for that SST id, it installs that slot on the handle.
- Otherwise, if the mode selects the level the handle is visited at, it gives
  the handle a fresh, empty slot and adds it to the map.

An SST can have several views, for example projections in different runs.
Because fresh slots go into the map, every view of an SST shares one slot, and
visiting L0 first gives an SST with an L0 view an L0 slot. A handle that
already has a slot, such as an L0 view the writer cloned from its own state,
is left alone.

The map is dropped when `carry_pins` returns. The cost is one pass over the
SSTs of the prior state and two over the new state per manifest install, and
nothing per read. Attaching a slot to a sorted-run SST rebuilds its run, so a
run is rebuilt only when one of its handles gains a slot. In
`MetadataPinning::L0` that is only a run holding a trivially moved SST. A
retired SST never re-enters the manifest, so no state outlives the install.
Without this step, every refresh would silently discard every pin. This is
the work RocksDB's table cache does by keeping table readers keyed by file
number across versions.

External SSTs (clones and checkpoints) are keyed by `SsTableId`, the same as
`DbCache` keys, so they pin like any other SST.

### Memory

As in RocksDB, pinned memory is proportional to live SSTs and has no eviction:

- With `MetadataPinning::L0`, it covers the L0 SSTs and any SST trivially
  moved out of L0, until compaction rewrites it.
  - With trivial moves off (the default), it is bounded by the L0 size the
    writer allows. `l0_max_ssts` is enforced per tree, so the bound is the
    number of trees times `l0_max_ssts`. A manifest union can push the total
    above that for a time. It stays small.
  - With trivial moves on, a moved SST keeps its slot in the sorted run, as in
    RocksDB. A workload whose compactions are mostly trivial moves, such as
    one writing keys in order, can pin much more than L0 holds. The `l0` label
    of the metrics below counts these SSTs, so operators can see it.
- With `MetadataPinning::All`, it is the metadata of the entire database.
  This is the same trade RocksDB users make with
  `cache_index_and_filter_blocks = false` and `max_open_files = -1`, and it
  must be sized with the formulas in [Sizing](#sizing).

Snapshots, transactions, and checkpoints that hold older state keep the handles
of retired SSTs alive, and with them their slots. A long-lived snapshot
therefore extends pinned memory past compaction. The `pinned_sst_count`
metric counts these SSTs until the snapshot is released.

There is no byte limit on pinned memory. SlateDB has no byte accounting outside
the cache layer. `MetadataPinning::L0` with trivial moves off is the bounded
mode. If `MetadataPinning::All` exceeds the memory available to the process,
SlateDB does not degrade. The process runs out of memory. Operators must
measure the pinned bytes on a representative database before they enable `All`
in production, and must watch the `pinned_sst_metadata_bytes` gauge after. See
[Open Questions](#open-questions) for a capped mode.

### Interaction with the block cache

Pinned entries are not read through `DbCache`. The pin is filled by the
existing read path, so cache population, including a hybrid disk tier, behaves
as it does today. Any copy already in the cache (for example, inserted on write
by the `BlockCachePolicy` of RFC 0031) stops receiving hits and is evicted by
the cache's normal policy, so long-term capacity is not double-counted. Until
that eviction, the cache and the slot hold two copies.

SlateDB now has three configuration points that shape where filters and
indexes live. Each answers a different question:

- `SplitCache` with a `meta_cache` decides whether data blocks can evict
  metadata from the cache. Use it when filters and indexes must stay cached
  but the lookup cost is acceptable.
- `BlockCachePolicy` ([RFC 0031](0031-block-cache-policy.md)) decides which
  entries are inserted into the cache at all.
- `MetadataPinning` decides which SSTs skip the cache for their filters and
  indexes.

`MetadataPinning::All` pairs well with a `BlockCachePolicy` that does not
insert filters and indexes into the cache. The first read of each SST then
goes to the object store and fills the slot directly, and the cache holds only
data blocks. `MetadataPinning::L0` pairs well with any policy, because the
writer fills L0 slots on flush and the reader fills them on first read.

### Sizing

- Filters: `keys × filter_bits_per_key / 8`.
- Indexes: about `data_bytes / block_size × (avg_first_key_len + per-entry overhead)`,
  where the per-entry overhead is the `BlockMeta` offset, length, and flatbuffer
  framing.

The real figure depends on the key sizes, the block size, and the filter
configuration, so measure it with the `pinned_sst_metadata_bytes` gauge on a
representative database before you enable `MetadataPinning::All` on a large
one.

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
- [x] Snapshots
- [ ] Sequence numbers

Snapshots and transactions do not change behavior. They hold older state, so
they extend the lifetime of pinned memory for retired SSTs. See
[Memory](#memory).

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [x] Checkpoints
- [x] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

The manifest format is unchanged. Pins are carried across manifest installs by
SST id, which covers external SSTs referenced by clones. A checkpoint holds
older state and extends pinned memory in the same way as a snapshot.

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

The compactor is unchanged. Its table store uses a pinner that pins nothing,
and its state is built without `carry_pins`, so its handles never have a slot.
Its index reads go through `DbCache` as they do today. The compactor reads no
filters and does not receive the `MetadataPinning` option.

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

The writer option reaches the bindings through `Settings`, which they pass as
JSON. The reader option needs a binding change: the UniFFI `ReaderOptions`
gains an optional `metadata_pinning` field and a `MetadataPinning` enum, and
the generated Go package is regenerated.

## Operations

### Performance & Cost

- Latency and CPU: pinned filter and index reads skip the `DbCache` lookup.
  Filter-negative SSTs skip iterator construction. The saving per lookup is
  the cache lookup cost for each candidate SST, plus the iterator setup for
  each filter-negative SST. `MetadataPinning::L0` alone captures the share
  that comes from L0 SSTs.
- Object-store requests: unchanged. No request is issued only to pin.
- Memory: the decoded metadata of the selected SSTs, outside `DbCache`.

### Observability

- Configuration: `MetadataPinning`, `None` by default.
- Metrics (up-down counters):
  - `slatedb.db.pinned_sst_count{level=l0|sorted_run}`: SSTs whose slot holds
    their filters, their index, or both. An empty slot is not counted. `level`
    is the level the SST had when it got its slot, stored on the slot, so a
    trivially moved SST stays under `l0`.
  - `slatedb.db.pinned_sst_metadata_bytes{level=l0|sorted_run,
    component=filter|index}`: bytes of filled slots.
- Tracing: filter and index read spans record `pinned = true` for pinned
  reads, and have no `cached` field.

### Compatibility

- No on-disk or manifest format change.
- Public API: additive (`MetadataPinning`, `#[non_exhaustive]`).
  `SsTableHandle` gains a private field. Equality and serialization are
  unchanged.
- Mixed versions: not applicable. Pinning is process-local.

## Testing

- Unit tests:
  - `PartialEq` and `Serialize` ignore the slot.
  - The pinned-bytes counter drops when the last handle for an SST drops.
  - `carry_pins` carries slots across `rebuild_state` and
    `merge_remote_manifest`, gives fresh slots only to selected SSTs, and
    drops slots for retired SSTs.
  - `MetadataPinning::L0` gives no fresh slot to a sorted-run SST. A trivially
    moved SST keeps the slot it had in L0.
  - `carry_pins` rebuilds only the sorted runs that gain a slot, and leaves a
    handle that already has a slot alone.
  - Every view of an SST shares one slot, whichever level each view is in.
  - The writer's `merge_remote_manifest` keeps the slots of its L0 SSTs and
    of a trivially moved SST, and gives a rewritten SST a fresh slot only in
    `MetadataPinning::All`.
  - `pinned_sst_count` counts a slot once, when it is first filled.
  - A flushed L0 SST enters the state with its slot filled. Only the main
    table store fills a slot on write.
  - The first read of a selected SST fills the slot, and the second read uses
    it after every other copy is removed from the cache.
  - `pinned_filters` returns nothing for an SST without a filter.
- Differential tests: randomized write, flush and compaction sequences run
  against a writer in a random mode, with a live compactor and trivial moves
  on or off, and a `BTreeMap` model. The writer, and a reader in each of
  `None`, `L0`, and `All`, must match the model on every `get`, read twice so
  the second pass reads filled slots, and on a full scan. A fixed-layout test
  with no `DbCache` makes sure that results and filter positives are
  identical across modes and that filter negatives exist in every mode.
  Negative counts can differ between modes, because pinned negatives do not
  occupy the lookahead window. The fixed layout is checked up front for
  filter false positives, because a false positive would change the counts.
- Deterministic simulation tests: DST picks a random pinning mode for each
  writer and reader.
- Read tracing tests: a pinned read records `pinned = true` and no `cached`
  field on its span.

## Rollout

- Milestones / phases: one change delivers the pin slot, `carry_pins`, fill
  on flush and on read, all three modes, the filter check before iterator
  construction, and the docs. Both modes are opt-in, so there is nothing to
  stage.
- Feature flags / opt-in: the option defaults to `None`.
- Docs updates: document the option, the RocksDB mapping, the three
  configuration points, and the sizing formulas.

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
`Arc` clone), so the cost is the same whether the metadata sits in a shared
cache or a dedicated one. Pinning is complementary. `SplitCache` remains the
right home for unpinned metadata. A new concurrent map keyed by SST id would
have the same problem on a smaller scale: a hash, a lock or shard, and
refcounting on every probe.

**Pinning inside `DbCache`, as RocksDB's `unpartitioned_pinning` does.** Hold a
cache entry handle on the SST handle instead of a private copy. This would
charge pinned memory to cache capacity as RocksDB does, but `DbCache` has no
pin or unevictable operation. With foyer, the cache would evict the key while
the handle keeps the value alive, so capacity accounting would drift from real
memory. Adding a pin operation to the `DbCache` trait would be required for
every implementation.

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

**Dropping the pin of a trivially moved SST in `MetadataPinning::L0`.** This
would keep the L0 mode bounded by the L0 size with trivial moves on. The RFC
keeps the pin instead, as RocksDB does, because the moved SST is still read
by every lookup that reaches its run, its metadata is already in memory, and
compaction retires it in time. The `l0` label of the pinned metrics shows how
many such SSTs are pinned.

**Per-component byte budgets.** Separate `filter_budget_bytes` and
`index_budget_bytes`. This gives finer control, but it has no RocksDB
analogue, it makes users size two budgets instead of choosing a level, and
SlateDB has no byte accounting outside the cache layer to build it on.
Level-based selection matches RocksDB and the access pattern: every lookup
consults every L0 SST, and every lookup consults one SST per sorted run.

**A persistent registry of slots.** Keep a map from `SsTableId` to a weak
reference to each slot on the `TableStore`, and consult it on every install.
Both install paths already hold the prior state, so a diff against it does the
same work with no weak references, no pruning, and no owner.

**RocksDB-style table cache of open table readers.** Moves all per-SST state,
including parsed footers, into a bounded table-reader cache. That is a larger
restructuring than needed. The SST handle already plays the role of the table
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
- Should `MetadataPinning` gain a capped mode, so that a database can pin
  more than L0 without pinning everything? Two shapes are possible. A
  size-tiered mode, like RocksDB's `kFlushedAndSimilar`, pins every SST below
  a size threshold. A byte-capped mode pins SSTs in level order until a byte
  budget is spent, and needs the byte accounting that SlateDB does not have
  outside the cache layer today. Either one is a new variant of the
  `#[non_exhaustive]` enum.

## References

- [RFC 0022: Pluggable Filter](0022-pluggable-filter.md)
- [RFC 0023: Cache Manager](0023-cache-manager.md)
- [RFC 0024: Segment-Oriented Compaction](0024-segment-oriented-compaction.md)
- [RFC 0031: Block Cache Policy](0031-block-cache-policy.md)
- [RocksDB wiki: Block Cache, caching index, filter, and compression dictionary blocks](https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-filter-and-compression-dictionary-blocks)
- [RocksDB wiki: Memory usage in RocksDB, indexes and filter blocks](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB#indexes-and-filter-blocks)
- [RocksDB `table.h`: `BlockBasedTableOptions`, `MetadataCacheOptions`, `PinningTier`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/table.h)
- [slatedb#2203: Trim per-SST CPU on the point-lookup path](https://github.com/slatedb/slatedb/pull/2203)
