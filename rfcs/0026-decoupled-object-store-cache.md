# Decoupled Pluggable Object Store Cache

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
   - [Two layers of caching](#two-layers-of-caching)
   - [Mixing the two layers](#mixing-the-two-layers)
   - [The problem today](#the-problem-today)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [The intent protocol](#the-intent-protocol)
   - [Choice of interface](#choice-of-interface)
   - [SlateDB core: protocol contract](#slatedb-core-protocol-contract)
   - [SlateDB core: supporting plumbing](#slatedb-core-supporting-plumbing)
   - [Where the cache will live](#where-the-cache-will-live)
   - [Builder layering](#builder-layering)
   - [Cache wrapper behavior](#cache-wrapper-behavior)
   - [A worked example](#a-worked-example)
   - [Allocation overhead](#allocation-overhead)
- [Implementation plan](#implementation-plan)
   - [Caveats](#caveats)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
   - [Performance and Cost](#performance-and-cost)
   - [Observability](#observability)
   - [Compatibility](#compatibility)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

SlateDB stores its data in object storage, so every cache miss is a network
call. Caching helps at two layers: a block cache for hot blocks (already
pluggable via Foyer) and an object store cache for SST file parts on local
storage. The object store cache today ships inside the SlateDB crate as
`CachedObjectStore`. When enabled, it always admits fetched bytes to the local
cache on a read miss, and a single `cache_puts: bool` knob toggles whether
PUTs are also written through to the local cache. There is no per-object
policy: WAL writes, manifest writes, compaction outputs, and memtable flushes
are treated the same way.

This RFC proposes two changes:

1. **Intent protocol.** Every `ObjectStore` call SlateDB issues will carry a
   typed intent describing the caller's purpose: write kind (WAL, memtable
   flush, compaction output, manifest), read kind (foreground, compaction input,
   warmup), and an optional retry reason (CRC mismatch, block decode error,
   decompression error). A cache wrapper reads the intent and applies its own
   policy.
2. **Pluggable cache placement.** `CachedObjectStore` will no longer be
   explicitly created inside SlateDB. The user passes any `Arc<dyn ObjectStore>`
   to `Db::builder`: a raw backend, the bundled `CachedObjectStore` they
   constructed themselves, or any other intent-aware `ObjectStore`
   implementation. The builder will wrap the passed `ObjectStore` with retry
   and instrumentation.

The protocol will be dependent on `object_store::Extensions`. An alternative is
SlateDB-owned `SlateDbObjectStore` trait was considered and is described in
[Alternatives](#alternatives); this RFC proposes the Extensions route.

The block cache stays as it is. This RFC is about the object store cache.

## Motivation

### Two layers of caching
SlateDB has two places to cache:

**Object store cache.** `CachedObjectStore` splits each SST into fixed-size
**part files** (default 4 MB) and writes one file per part on local disk. A read
for a byte range fetches whichever part files cover that range; missing part
files are fetched from upstream and admitted. The cached bytes are whatever the
backend returned.

**Block cache.** A Foyer cache stores blocks, filters, indexes and stats either
in memory or tiered across memory and disk. Each cached value has a typed
in-memory form (`Arc<Block>`, `Arc<SsTableIndexOwned>`, `Arc<[NamedFilter]>`,
`Arc<SstStats>`) and a corresponding encode/decode pair for the disk tier. The
memory tier holds the typed struct directly; the disk tier holds its encoded
form (post-decompression and post-CRC) so hits in either tier skip the
decompression and CRC work the object store cache pays on every read. A memory
tier hit returns the existing `Arc<T>` with no CPU work; a disk tier hit pays
one serde deserialize and one type-specific decode pass to reconstruct the
struct. Foyer owns admission and eviction. This RFC does not change the block
cache.

They compare as follows:

| Dimension              | Object store cache                        | Block cache (Foyer)                                  |
|------------------------|-------------------------------------------|------------------------------------------------------|
| Cached unit            | Part file (default 4 MB) of an SST        | One block (~4 KiB), filter, index, or stat (typed `Arc<T>` in the memory tier, type-specific encoded form in the disk tier) |
| CPU on hit             | CRC check, offsets parse, decompression if `compression_codec` is set, plus any configured block transformer | Memory tier: refcount bump. Disk tier: serde framing + one type-specific decode pass (the cached form is already decompressed and CRC, verified) |
| Per-SST bookkeeping    | One file per part (e.g. 64 part files for a 256 MB SST at the 4 MB default) | Thousands of entries        |
| Memory footprint       | File metadata only                        | In-memory index grows with cached items (can reach GBs) |
| Restart cost           | Zero, open files on demand                | Rebuild the in-memory index from disk                |


### Mixing the two layers

Both caches can run together; they sit at different levels of the read path.
The right configuration depends on the workload (working-set size, memory
budget, read amplification, tolerance for cold-read latency). There are many
valid setups: object store cache only, Foyer only, Foyer scoped to a subset of
content types (e.g. filters/indexes/stats) layered over an object store cache
for data part files, full Foyer over an object store cache, and so on. This
RFC does not prescribe one.

### The problem today

The object store cache lives inside the SlateDB crate. Several problems follow:

1. **SlateDB owns policy it should not own, and the policy is coarse.** The
   current cache exposes `cache_puts: bool` (write-through on or off) and treats
   every object identically. It cannot distinguish a short-lived WAL write from
   a long-lived compaction output. Refining that policy, or adding any new
   signal, means editing core.
2. **Two caches, two different pluggability levels.** The block cache is already
   pluggable. The object store cache is not.
3. **Cache invalidation forces readers to hold the cache.** Dropping fsync on
   disk cache writes ([#1571](https://github.com/slatedb/slatedb/pull/1571))
   required SST readers to evict the stale entry themselves when a CRC
   mismatch is observed. That works, but it means the reader paths now
   carry a typed `Arc<CachedObjectStore>` next to their `Arc<dyn ObjectStore>`
   and call cache-specific methods on it. Every new component that needs
   similar behavior pays the same plumbing cost.
4. **Compaction is not aware of the object store cache.** Compaction reads
   and writes go straight to the raw object store and never see the cache
   wrapper. To change that (e.g. to skip admitting compaction outputs, or
   to let compaction inputs bypass the cache), every compactor-facing
   component would need both an `Arc<dyn ObjectStore>` and an
   `Arc<CachedObjectStore>`, plus a new set of user-facing options to
   govern which one each call uses. Same shape as problem (3), and the
   same shape will reappear for any future component that wants per-call
   cache behavior.

These all point at the same fix. A generic layer that abstracts the cache
behind a uniform call protocol means every component talks to one
`ObjectStore` and tags each call with its intent; the cache (or whatever
wrapper the user installed) acts on the tag. No component needs to know
whether a cache is present, what kind it is, or what its current policy is,
and no user-facing config has to grow per-component cache toggles.

The `object_store` crate already provides a suitable mechanism. Its
`GetOptions`, `PutOptions`, and `PutMultipartOptions` types each carry an
`extensions` field. SlateDB can attach metadata to it, and a wrapper can read it
back. Backends that do not know about the metadata (S3, GCS, Azure) ignore it.

## Goals

- Define a typed protocol so a cache wrapper knows what each `ObjectStore` call
  is for.
- Allow the user to plug in their own `ObjectStore` (cache or otherwise) without
  SlateDB knowing it is there.
- Keep the bundled `CachedObjectStore` available as one such wrapper, with
  admission policy driven by intent.

## Non-Goals

- Defining an on-disk format for any specific cache.
- Picking a single canonical admission or eviction policy. That is the wrapper's
  job.
- Building automatic warmup or eviction. Warmup stays caller-driven via the
  existing `DbCacheManagerOps::warm_sst` API.
- Splitting the object store cache into its own crate. Discussed in the [Open
  Questions](#open-questions).

## Design

### The intent protocol

The protocol is a small set of types. SlateDB will insert one of them into
`extensions` on every read or write:

```rust
pub struct WriteIntent { pub kind: WriteKind }

pub enum WriteKind {
    Flush,
    CompactionOutput,
    Manifest,
    Wal,
}

pub struct ReadIntent {
    pub kind: ReadKind,
    pub retry: Option<RetryReason>,
}

pub enum ReadKind {
    Foreground,
    CompactionInput,
    Warmup,
}

pub enum RetryReason {
    CrcMismatch,
    BlockDecodeError,
    DecompressionError,
}
```

The protocol covers three things the wrapper needs to know:

1. **What kind of write is this?** (`WriteIntent`) Lets the wrapper decide
   admission per kind. A typical policy keeps data-bearing writes (`Flush`) and
   skips short-lived or tiny ones (`Wal`, `Manifest`) and bulk ones
   (`CompactionOutput`), but the policy is the wrapper's choice.
2. **What kind of read is this?** (`ReadIntent`) Lets the wrapper apply
   different policies to foreground queries, compaction-input scans, and
   warmups.
3. **Is this a retry after a bad cache hit?** (`ReadIntent.retry`) SlateDB will
   set this when an SST decode fails (CRC, block decode, decompression). The
   wrapper decides how to respond; a common action is to evict the local cache
   entry, fetch fresh bytes from upstream, and re-cache. Without this signal,
   a wrapper that keeps serving the same corrupt bytes on every retry would
   trap the caller in an infinite retry loop.

### Choice of interface

This RFC proposes carrying intent via `object_store::Extensions`. SlateDB
inserts a typed intent (`WriteIntent` or `ReadIntent`) into the `extensions`
field on every `GetOptions`, `PutOptions`, and `PutMultipartOptions` it builds;
a wrapper reads it with `extensions.get::<ReadIntent>()` or
`get::<WriteIntent>()`. No new public trait, and the protocol composes directly
with any generic `ObjectStore` wrapper at any level of the chain.

Cost: three small heap allocations per tagged call (the lazily-initialized
`Box<HashMap>` for `Extensions`, the HashMap's bucket array on first insert,
and the `Box<val>` for the intent itself). See
[Allocation overhead](#allocation-overhead).

Two alternative interfaces were considered and rejected for this RFC:
a SlateDB-owned trait that wraps an inner `ObjectStore`, and a SlateDB-owned
trait that replaces the `object_store` dependency entirely. Both are described
in [Alternatives](#alternatives) and remain viable future directions if
allocation pressure, ecosystem coupling, or extensibility needs grow.

One workaround is needed to make this current choice valid: upstream
`object_store::buffered::BufWriter` drops extensions on the single-PUT shutdown
path. See [Caveats](#caveats) and we need to fix it upstream.

### SlateDB core: protocol contract

SlateDB will support tagging every call to ObjectStore as much as possible:

1. **Tag every write with `WriteIntent`.** Every opts-capable write
   (`put_opts` and multipart upload init) will set a `WriteIntent`. WAL writers
   use `Wal`. Memtable flush outputs use `Flush`. Compaction outputs use
   `CompactionOutput`. Manifest and compactions-state writes use `Manifest`.
   Aside from documented upstream/API gaps, opts-capable writes from SlateDB
   core are tagged.
2. **Tag every read with `ReadIntent`.** Every opts-capable `get_opts` will set a
   `ReadIntent`. Foreground user queries use `Foreground`. Compaction inputs use
   `CompactionInput`. The `DbCacheManagerOps::warm_sst` path uses `Warmup`.
   Aside from documented upstream/API gaps, opts-capable reads from SlateDB core
   are tagged.
3. **Retry on validation failure with a retry tag.** If decoding an SST fails
   with a recoverable error (CRC mismatch, bad block, bad decompression),
   SlateDB will reissue the read with `retry = Some(reason)`. SlateDB may retry
   up to a bounded number of times before surfacing the failure. The retry tag
   tells the wrapper to drop the cached file rather than return the same corrupt
   bytes again.

Untagged calls flow through the wrapper unchanged and the wrapper applies its
default behavior. The bundled `CachedObjectStore` treats untagged writes
according to its existing `cache_puts` policy, preserving pre-protocol behavior
for known gaps such as `object_store::buffered::BufWriter`.

### SlateDB core: supporting plumbing

Three `ObjectStore` methods cannot carry an intent because `object_store`
0.13 has no `*_opts` variant for them: `delete`, `list`, and the
convenience `head(path)`. They still flow through whatever wrapper the
user installed, so a cache wrapper has everything it needs without a
separate hook:

- `delete(path)` is enough signal to evict the local cache entry.
  Garbage collection already calls `ObjectStore::delete`, so cache
  eviction on delete happens for free.
- `list(prefix)` is enough for enumeration.
- `head(path)` (convenience form) flows through and can be served from
  cached metadata. Paths that need intent on a head request can use
  `get_opts` with `head` set to true instead.

### Where the cache will live

SlateDB talks to object storage through `Arc<dyn ObjectStore>`. The user passes
one to `Db::builder(path, store)`. That store can be:

- A raw backend (S3, GCS, Azure, `InMemory`, `LocalFileSystem`).
- The bundled `CachedObjectStore` wrapping a raw backend.
- Any other intent-aware `ObjectStore` the user wrote.

If the user passes a raw backend, the protocol is a no-op: SlateDB still
attaches intents on every call, but nothing reads them.

If the user passes `CachedObjectStore` (or another intent-aware wrapper), every
read and write flows through it with a typed intent on the extensions.

### Builder layering

`DbBuilder`, `GarbageCollectorBuilder`, `CompactorBuilder`, and
`DbReaderBuilder` will assemble the main object store the same way, from
innermost to outermost:

```
1. The store the user passed (a raw backend, the bundled
   CachedObjectStore, or a custom intent-aware wrapper)
       |
       v
2. InstrumentedObjectStore (per-operation metrics)
       |
       v
3. RetryingObjectStore (transient failure retries)
       |
       v
   SlateDB core (TableStore, ManifestStore, CompactionsStore, GC)
```

layers 2 and 3 are SlateDB-managed. layer 1 is whatever the user hands to the
builder, all the way down. SlateDB core will not construct a cache on the user's
behalf; the bundled `CachedObjectStore` is one of several things the user may
pass in.

Users who want the bundled cache construct it themselves with
`CachedObjectStore::builder`:

```rust
let cache = CachedObjectStore::builder(raw_backend)
    .root_folder(dir)
    .build()
    .await?;
let db = Db::builder(path, cache).build().await?;
```

`root_folder` is the only required setting. Other settings
(`cache_puts`, `part_size_bytes`, `max_cache_size_bytes`, `scan_interval`,
`max_open_file_handles`) have defaults and are set via builder methods when
needed. SlateDB internals (`metrics_recorder`, `system_clock`, `rand`) are
defaulted as well and only overridden for tests or to share state with a
specific `Db` instance.

`ObjectStoreCacheOptions` will live in `slatedb::cached_object_store`, not in
`Settings`. SlateDB core will have no field that describes the cache; the
protocol on each call is the only contract it knows.

The current layering is the reverse: `CachedObjectStore` is the outermost
layer, with retries underneath. The proposed order, with retries on the
outside, separates the two concerns: the user provides whatever
`dyn ObjectStore` they want at step 1, and SlateDB unconditionally adds retry
and instrumentation on top. Custom wrappers do not need to know about retry
behavior.

A side effect: the cache wrapper now has to handle its own internal errors
because SlateDB cannot tell them apart from upstream errors. A transient disk
I/O failure inside the cache, a corrupt local part, or a missing cache file
look the same to the outer `RetryingObjectStore` as a transient backend
failure, so SlateDB will retry them with the same policy. Wrappers that want
different semantics (e.g. fall back to upstream on local disk failure rather
than surface the error) implement that internally before returning to
SlateDB.

Two consequences worth calling out:

- Compactor and GC builders will only add retry and instrumentation; they will
  not auto-wrap with `CachedObjectStore`. Users who want compactor reads or GC
  operations to go through their own cache pass it directly to those builders.
- Manifest writes and compactions metadata writes will flow through the cache
  wrapper (because there is only one main store handle). They will still skip
  the cache because they carry `WriteIntent::manifest()` and the bundled wrapper
  short-circuits that intent. See [Cache wrapper
  behavior](#cache-wrapper-behavior).

### Cache wrapper behavior

The bundled `CachedObjectStore` initially keeps its current behavior:

- Reads always admit fetched bytes to the local cache on a miss.
- Writes follow the existing `cache_puts: bool` master switch (off by
  default; when on, every PUT is written through).
- Multipart puts pass through (never cached), as today.

Adopting the intent protocol does not change any of that on day one. What it
does change is what the wrapper *can* do without further plumbing into
SlateDB core. Each of these evolutions is now expressible as a small,
isolated wrapper change, and each can land as its own follow-up with its
own benchmarks and trade-off discussion:

| Signal | Wrapper change | Notes |
|---|---|---|
| `WriteIntent::wal()` on PUT | Skip the local write; forward to upstream only. | Only matters when `cache_puts: true`. |
| `WriteIntent::manifest()` on PUT | Skip the local write; forward to upstream only. | Only matters when `cache_puts: true`. |
| `WriteIntent::compaction_output()` on PUT | Skip the local write; forward to upstream only. | Only matters when `cache_puts: true`. Closes [problem 4](#the-problem-today). |
| `ReadIntent::compaction_input()` on GET | Skip cache lookup and skip miss-admit; forward to upstream. | One-shot scans no longer enter the cache. |
| `ReadIntent.retry = Some(_)` on GET | Delete the cache entry for `path`, then run the usual cache-aware fetch. | Closes [problem 3](#the-problem-today); lets the fsync workaround be removed. |

Each future change is local to the wrapper. SlateDB core's contract
(tag every call with intent) does not move.

Concrete policy choices (admission per kind, part sizes, prefetch shape,
on-disk layout, eviction strategy) are owned by the wrapper. Users who want
different choices implement their own `ObjectStore` and pass it to
`Db::builder`.

### A worked example

Simplest form, using the builder:

```rust
let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
let cache = CachedObjectStore::builder(backend)
    .root_folder(cache_dir)
    .build()
    .await?;
let db = Db::builder("my-db", cache).build().await?;
```

With more options set:

```rust
let cache = CachedObjectStore::builder(backend)
    .root_folder(cache_dir)
    .cache_puts(true)
    .part_size_bytes(8 * 1024 * 1024)
    .max_cache_size_bytes(32 * 1024 * 1024 * 1024)
    .build()
    .await?;
```

For tests or advanced wiring (shared metrics recorder, mock clock,
deterministic RNG):

```rust
let cache = CachedObjectStore::builder(backend)
    .root_folder(cache_dir)
    .metrics_recorder(shared_recorder)
    .system_clock(mock_clock)
    .rand(seeded_rand)
    .build()
    .await?;
```

### Allocation overhead

Tagging a single call pays three small heap allocations: the `Box<HashMap>` for
the lazily-initialized `Extensions` map, the HashMap's bucket array on first
insert, and the `Box<val>` for the intent itself. None of the three can be
reused across calls because `ObjectStore::get_opts` and friends take
`GetOptions` by value, and `Extensions::clone()` is a deep clone of all three
pieces.

This does not affect read latency in any meaningful way next to a network or
local-disk round trip. On hot paths (compaction reading thousands of blocks,
warmup loops issuing many GETs) the aggregate allocator pressure is non-zero but
well below the cost of the I/O itself.

A future micro-optimization, if allocations come to matter, is to switch to
`SlateDbObjectStore` (zero allocation overhead, see
[Alternatives](#alternatives)) or to a static-intent-per-handle arrangement if
`object_store` ever changes `get_opts` to take `&GetOptions`.

## Implementation plan

The work is structured as six commits, each compiling and passing tests on its
own. They tell the story in order: foundation, wiring, consumption, structural
cleanup, decoupling, documentation.

### Callers that need to change

- **Protocol module.** A new module defines the intent types, helpers, and a
  test recorder. No production callers yet.
- **`TableStore` and its callers.** Every public read and write method takes
  an `intent` argument. All producer sites (memtable flush, WAL flush,
  compaction reads and writes, foreground reads on `Db` and `DbReader`, the
  warmup path on `DbCacheManagerOps`) update to pass the right intent.
- **Metadata stores.** `ManifestStore` and `CompactionsStore` construct their
  underlying `slatedb-txn-obj` protocol with `WriteIntent::manifest()` on
  writes and `ReadIntent::foreground()` on reads, so manifest and
  compactions-state calls carry intents like everything else.
- **`CachedObjectStore`.** Becomes `pub` and gains a
  `CachedObjectStore::builder(backend)` construction path. It initially keeps
  its current admission behavior; intent-aware policy changes such as skipping
  `Wal`/`Manifest`/`CompactionOutput` writes, bypassing `CompactionInput` reads,
  and evicting on retry-tagged reads can land as isolated follow-ups.
- **Builders.** `DbBuilder`, `DbReaderBuilder`, `CompactorBuilder`, and
  `GarbageCollectorBuilder` consolidate to a single `main_object_store`
  handle that wraps whatever the user passed with instrumentation and retry.
  The cache auto-construct convenience and the startup preload helpers are
  removed; `DbCacheManagerOps::warm_sst` becomes the supported warmup path.
- **`Settings` and `DbReaderOptions`.** `ObjectStoreCacheOptions` moves out
  into the `cached_object_store` module; the corresponding fields are
  removed from the public config types.
- **`slatedb-txn-obj`.** Carries `put_extensions` / `get_extensions` fields
  on the protocol and boundary objects, with constructors that accept
  either a single shared payload or split read/write payloads.

### Caveats

- **Upstream `BufWriter` extensions bug.** SST writes go through
  `object_store::buffered::BufWriter` with extensions attached via
  `.with_extensions(..)`. `BufWriter::poll_shutdown` drops extensions on the
  single-PUT path (payload fits in capacity); the multipart overflow path
  forwards them. Tracked as
  [apache/arrow-rs-object-store#735](https://github.com/apache/arrow-rs-object-store/issues/735).
  Until that fix lands, SST writes below the default 10 MB BufWriter capacity
  will reach a wrapper untagged. Two matrix tests will document the bug
  (`bufwriter_small_payload_drops_write_intent`,
  `bufwriter_large_payload_carries_write_intent`) and act as regression catches
  for when the upstream fix lands.

  Practical effect: in production, small SST writes (memtable flushes that fit
  in 10 MB, occasional small compaction outputs) may reach the wrapper
  untagged. Since the bundled `CachedObjectStore` initially keeps its current
  policy, those writes follow the pre-protocol behavior. WAL and Manifest writes
  do not go through BufWriter and so will always carry their intent correctly.

- **Multipart-part-level intent on the wire.** `WriteIntent` attaches at
  `put_multipart_opts` init, not on individual `put_part` calls. A cache wrapper
  that wraps `MultipartUpload` (which it must do anyway to observe part bytes)
  holds the intent in its own state and applies it to every part. Wrappers that
  do not wrap `MultipartUpload` see only the init call. The bundled cache will
  not admit multipart uploads, so this gap does not affect it.

## Impact Analysis

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
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

- [x] Manifest format: unchanged. Manifest writes will carry
  `WriteIntent::manifest()` via the `slatedb-txn-obj` constructor.
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection: GC deletes will flow through the wrapper, triggering
  cache eviction as a side effect. GC will use the same consolidated main object
  store handle.
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence: writes will carry `WriteIntent::manifest()`
  via the `compactions_store` constructor.
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [x] Compactions format: compaction-output SST writes will carry
  `WriteIntent::compaction_output()`; compaction-input reads will carry
  `ReadIntent::compaction_input()`.

### Storage Engine Internals

- [x] Write-ahead log (WAL): WAL writes will carry `WriteIntent::wal()` so
  wrappers can skip admitting them.
- [x] Block cache: unchanged; the in-process block cache continues to operate
  independently.
- [x] Object store cache: today's `CachedObjectStore` will be decoupled from
  core construction and made user-constructible. Its pre-existing
  `cache_puts: bool` master switch and admission behavior are preserved
  initially.
- [ ] Indexing (bloom filters, metadata)
- [x] SST format or block format: the decode path will issue retry-tagged reads
  on recoverable validation failures.

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing): no new SlateDB metrics. The
  bundled cache wrapper will continue to emit its existing stats; wrappers own
  their own observability.

## Operations

On day one the measurable runtime change is per-call tagging overhead:

- **CPU and memory pressure.** Tagging adds three small heap allocations per
  `ObjectStore` call (the `Box<HashMap>` for `Extensions`, the HashMap's
  bucket array on first insert, and the `Box<val>` for the intent). Latency
  against network or local-disk I/O is unaffected. Aggregate allocator
  traffic on compaction or warmup is non-zero but well below the I/O cost.
  See [Allocation overhead](#allocation-overhead).

Latency, throughput, and object-store cost do not otherwise change on day one
because the bundled wrapper keeps its current behavior (see
[Cache wrapper behavior](#cache-wrapper-behavior)). They can evolve as the
follow-ups in that section land:

- Skipping WAL / manifest / compaction-output write-through reduces local
  disk writes when `cache_puts: true` and leaves more room for hot data.
- Bypassing compaction-input reads keeps one-shot bulk reads out of the
  cache, trading cache-hit savings on compactor reads for a smaller
  working set on foreground reads.
- Evicting on retry-tagged reads lets the fsync workaround go away,
  removing one local disk sync per admitted write at the cost of one
  extra upstream GET on the rare CRC-mismatch path.

### Performance and Cost

As a start performance is unchanged except for the per-call allocation overhead
described in [Allocation overhead](#allocation-overhead). Further changes
and improvements can be picked up incrementally from the follow-ups listed
in [Cache wrapper behavior](#cache-wrapper-behavior).

### Observability

- SlateDB will add no new metrics for the protocol itself.
- The bundled `CachedObjectStore` retains its existing hit-rate, eviction, and
  admission stats.
- User-supplied wrappers own their own metrics.

### Compatibility

- **API.** Users who set
  `Settings::object_store_cache_options.root_folder` to enable the bundled cache
  will need to construct `CachedObjectStore` themselves and pass it to
  `Db::builder` (the auto-construct convenience is removed when the field moves
  out of `Settings`). The bundled cache's initial admission behavior, including
  `cache_puts`, is preserved.
- **On-disk format.** Unchanged. The disk format used by `CachedObjectStore`
  stays the same, so cached files survive the migration.
- **Wire format.** Unchanged.
- **Wrappers and backends that ignore the protocol.** Unaffected.


## Rollout

The work spans two crates: `slatedb-txn-obj` and `slatedb`. `slatedb` depends
on `slatedb-txn-obj`, so the Extensions support on the `slatedb-txn-obj`
protocol and boundary objects lands first; the SlateDB-side changes that use
it follow.

## Alternatives

**Keep the cache in SlateDB.** Works today. Locks policy into SlateDB, makes it
hard to ship alternative caches, and forces every policy change through a
SlateDB release. Rejected.

**A separate `SlateDbObjectStore` trait that wraps an inner `ObjectStore`.**
Define SlateDB's own storage interface as a distinct trait whose methods take
intents as regular arguments, but keep `object_store` as the backend layer the
user constructs:

```rust
pub trait SlateDbObjectStore {
    async fn get(&self, path: &Path, range: Range<usize>, intent: ReadIntent) -> Result<Bytes>;
    async fn put(&self, path: &Path, payload: PutPayload, intent: WriteIntent) -> Result<()>;
    async fn delete(&self, path: &Path) -> Result<()>;
    // ...
}
```

Zero allocation overhead and SlateDB-owned. SlateDB still depends on
`object_store` and the user still constructs an `ObjectStore` backend to hand
in (wrapped by a default `SlateDbObjectStore` adapter, or by a custom one).
Trade-off: a new public trait that SlateDB must version, and generic
intermediate wrappers (rate limiter, encryption) must either sit below the
cache or be rewritten as `SlateDbObjectStore`-aware versions.

Not the proposed approach. The Extensions allocation cost is dwarfed by the
I/O on every realistic call. If that changes, the migration is mechanical:
replace `extensions.insert(intent)` with a typed argument and
`extensions.get::<T>()` with a method parameter.

**Intent-aware cache policy as a thin router above `CachedObjectStore`.** Keep
`CachedObjectStore` as the physical cache, add a layer above that decides
per-intent whether to use the cache, look up only, bypass, evict and refetch, or
write-through; the policy trait would be the public surface.

Not the proposed approach. Reasons: the bundled `CachedObjectStore` can do the
routing internally based on intent, the proposed policy trait would still
require typing the routing decisions, and allowing users to bring a fully custom
cache implementation (different on-disk format, `io_uring`, sharding,
`O_DIRECT`) is more flexible than letting them substitute only the policy.

**A SlateDB-owned trait that does not wrap `ObjectStore` at all.** A bigger
version of the previous alternative. SlateDB defines its own storage trait
(`SlateDbObjectStore`) and ships built-in adapters (e.g. for `object_store`
and `opendal`). The `object_store` crate is no longer a public dependency:
users pick from SlateDB's adapters or implement the trait themselves against
a raw SDK. Zero allocation overhead, full control over the storage contract
(batch ops, richer cache hints, per-call deadlines, etc.), and freedom to
evolve without coordinating with upstream.

Cost: SlateDB owns the adapter surface and any future backend shape changes,
and users who want a backend not yet adapted have to write the adapter
themselves. The biggest structural change of the three trait options, and the
biggest ownership commitment. Not the proposed approach, kept on the table
as a future direction if ecosystem coupling or extensibility needs grow.

**Defer to OpenDAL's cache layer.** OpenDAL recently merged a cache layer RFC
([apache/opendal#6297](https://github.com/apache/opendal/pull/6297)) that
supports chunk caching. With the protocol in this RFC, nothing prevents a user
from wiring an OpenDAL-backed cache as their `ObjectStore`. If OpenDAL's cache
layer reads typed cache hints from `Extensions` or grows an intent-equivalent
argument, this RFC's protocol bridges to it directly. SlateDB does not need to
choose; users do.

**Static intent per `ObjectStore` handle (requires upstream API change).**
Construct multiple internal handles each preconfigured with a fixed intent,
sharing pre-built `GetOptions` / `PutOptions` across many calls. Only valid if
`ObjectStore::get_opts` takes `&GetOptions` by reference instead of by value.
The upstream change is on the `object_store` crate; if it ever lands, this
becomes the cleanest allocation-free path that keeps the Extensions protocol.

## Open Questions

- **Should the object store cache live in its own crate (e.g.
  `slatedb-obj-cache`)?** Not proposed here. With the protocol in place and the
  cache no longer required for correctness, splitting it out is mechanical.
  Decision deferred until there is a second cache implementation or a concrete
  consumer of the standalone crate.
- **Allocation cost in practice.** Not measured. Worth profiling on compaction
  and warmup paths before committing to further optimization (or before
  switching to `SlateDbObjectStore`).

## References

- [`object_store::GetOptions` (carries `extensions: ::http::Extensions`)](https://docs.rs/object_store/latest/object_store/struct.GetOptions.html)
- [`object_store::PutOptions` (carries `extensions: ::http::Extensions`)](https://docs.rs/object_store/latest/object_store/struct.PutOptions.html)
- [RFC 0023: Targeted Cache Warming and Best-Effort Block Cache
  Eviction](0023-cache-manager.md)
- Relevant issue: [slatedb/slatedb#703](https://github.com/slatedb/slatedb/issues/703)
- Existing in-tree `CachedObjectStore` (`slatedb/src/cached_object_store/`)
- Upstream `BufWriter` extensions bug: [apache/arrow-rs-object-store#735](https://github.com/apache/arrow-rs-object-store/issues/735)
- OpenDAL cache layer: [apache/opendal#6297](https://github.com/apache/opendal/pull/6297)
- OpenDAL `FoyerService`: [apache/opendal#7160](https://github.com/apache/opendal/pull/7160)
