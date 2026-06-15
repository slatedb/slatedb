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
   - [TableStore builder and per-store intents](#tablestore-builder-and-per-store-intents)
   - [SlateDB core: supporting plumbing](#slatedb-core-supporting-plumbing)
   - [Builder layering](#builder-layering)
   - [The cache as a wrapper](#the-cache-as-a-wrapper)
   - [Cache wrapper behavior](#cache-wrapper-behavior)
   - [Allocation overhead](#allocation-overhead)
- [Caveats](#caveats)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
   - [Performance and Cost](#performance-and-cost)
   - [Observability](#observability)
   - [Compatibility](#compatibility)
- [Future Evolution](#future-evolution)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Accepted

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

1. **Intent protocol.** SlateDB tags the calls the object store cache cares
   about, compacted SST reads and writes, with a typed intent describing the
   caller's purpose: write kind (memtable flush, compaction output), read kind
   (foreground, compaction input), and an optional retry reason (CRC mismatch,
   block decode error, decompression error). All other calls (WAL, manifests,
   compactions state, the GC boundary file, list, delete) carry no intent. A
   cache wrapper reads the intent and applies its own policy, treating untagged
   calls as not-cacheable. Tagging only compacted SSTs covers the cache's real
   decision, keeps the change small, and leaves the WAL and manifest code paths
   untouched.
2. **Pluggable cache placement.** `CachedObjectStore` will no longer be
   explicitly created inside SlateDB. The user passes the innermost backend to
   `Db::builder` and, optionally, a cache as an `ObjectStoreWrapper`: the bundled
   `CachedObjectStore` exposed as a wrapper, or any other intent-aware
   `ObjectStore`. The builder wraps the backend with instrumentation and retry,
   the same order it uses today, then applies the cache wrapper on top.

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
5. **Cached ObjectStore is not DST friendly.** `CachedObjectStore` directly uses `spawn_blocking` so it can't be tested it in DST.

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

- Define a typed protocol so a cache wrapper knows the purpose of each compacted
  SST call.
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
- Tagging non-SST traffic (WAL, manifests, compactions state). See [Future
  Evolution](#future-evolution).
- Splitting the object store cache into its own crate. Discussed in the [Open
  Questions](#open-questions).

## Design

### The intent protocol

The protocol is a small set of types. SlateDB inserts one of them into
`extensions` on every compacted SST read or write:

```rust
pub struct WriteIntent { pub kind: CompactedSstWriteKind }

pub enum CompactedSstWriteKind {
    Flush,
    CompactionOutput,
}

pub struct ReadIntent {
    pub kind: CompactedSstReadKind,
    pub retry: Option<RetryReason>,
}

pub enum CompactedSstReadKind {
    Foreground,
    CompactionInput,
}

pub enum RetryReason {
    CrcMismatch,
    BlockDecodeError,
    DecompressionError,
}
```

The protocol covers three things the wrapper needs to know:

1. **What kind of write is this?** (`WriteIntent`) Lets the wrapper decide
   admission per kind, for example admitting memtable flushes (`Flush`) while
   skipping bulk compaction outputs (`CompactionOutput`). The policy is the
   wrapper's choice.
2. **What kind of read is this?** (`ReadIntent`) Lets the wrapper apply
   different policies to foreground queries and compaction-input scans.
3. **Is this a retry after a bad cache hit?** (`ReadIntent.retry`) SlateDB will
   set this when an SST decode fails (CRC, block decode, decompression). The
   wrapper decides how to respond; a common action is to evict the local cache
   entry, fetch fresh bytes from upstream, and re-cache. Without this signal,
   a wrapper that keeps serving the same corrupt bytes on every retry would
   trap the caller in an infinite retry loop.

### Choice of interface

This RFC proposes carrying intent via `object_store::Extensions`. SlateDB
inserts a typed intent (`WriteIntent` or `ReadIntent`) into the `extensions`
field of the `GetOptions`, `PutOptions`, and `PutMultipartOptions` it builds for
compacted SST reads and writes; a wrapper reads it with
`extensions.get::<ReadIntent>()` or `get::<WriteIntent>()`. No new public trait, and the protocol composes directly
with any generic `ObjectStore` wrapper at any level of the chain.

Cost: a small per-call heap allocation overhead, described in
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

SlateDB tags the object store calls the cache cares about, compacted SST reads
and writes, and leaves everything else untagged:

1. **Tag compacted SST writes with `WriteIntent`.** Memtable flush outputs use
   `Flush` and compaction outputs use `CompactionOutput`, attached on
   `put_opts` and multipart upload init. WAL writes and fences, manifest
   writes, and compactions-state writes carry no intent.
2. **Tag compacted SST reads with `ReadIntent`.** Foreground user queries use
   `Foreground` and compaction inputs use `CompactionInput`, attached on
   `get_opts`. Cache warmup reads issued via `DbCacheManagerOps::warm_sst` are
   foreground reads and carry `Foreground`. WAL reads and metadata reads carry
   no intent.
3. **Retry on validation failure with a retry tag.** If decoding a compacted
   SST fails with a recoverable error (CRC mismatch, bad block, bad
   decompression), SlateDB reissues the read with `retry = Some(reason)`, up to
   a bounded number of times before surfacing the failure. The retry tag tells
   the wrapper to drop the cached file rather than return the same corrupt bytes
   again.

Every untagged call flows through the wrapper, which treats it as not-cacheable
and serves it from upstream. This covers WAL segments and fences, manifests,
compactions state, the GC boundary file, and the opts-less `delete`, `list`,
and `head` calls. Tagging only compacted SSTs keeps the change non-invasive:
the WAL, manifest, and `slatedb-txn-obj` code paths are untouched.

### TableStore builder and per-store intents

The compacted SST kinds are a property of the component issuing the traffic,
not of an individual call: a foreground read and a compaction-input read of the
same SST are identical on the wire, and only the component that owns the
operation knows which it is. SlateDB already reflects this by running two
`TableStore` instances, one for foreground work and one for compaction. The
foreground store is the Db's; the compaction store is shared by the compactor
and GC and is also cacheless so background scans do not evict the foreground
block cache. WAL reading uses its own store.

The intent kinds attach to that existing split. `TableStore` is constructed
through a builder whose required inputs are the object stores, SST format, and
root path, and whose optional inputs include the two kinds:

```rust
let table_store = TableStore::builder(object_stores, sst_format, root_path)
    .with_compacted_sst_read_kind(CompactedSstReadKind::Foreground)
    .with_compacted_sst_write_kind(CompactedSstWriteKind::Flush)
    .build();
```

A store records its kinds once at construction and derives the per-call intent
from the SST id: a compacted SST carries the store's kind, a WAL SST carries
none. The Db's store uses `Foreground` and `Flush`; the compaction store uses
`CompactionInput` and `CompactionOutput`; the WAL reader's store sets no kinds.

This is a deliberate choice over tagging each call. The compacted SST kind is
not known at the leaf read and write sites: methods like `read_index` and
`read_blocks` are reached through shared, component-agnostic plumbing
(`SstIterator`, `SortedRunIterator`, the cache loaders) that serves every
component alike. A per-call parameter would have to be threaded from each
component down through all of that plumbing and every one of its callers, making
those call sites intent-aware too. That is invasive and easy to get wrong: one
missed or mistyped call site silently mistags its traffic. Declaring the kind
once per store, where the component is composed, confines the decision to a
single place per component, and because the per-component store split already
exists, it costs no new wiring.

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

### Builder layering

`DbBuilder`, `GarbageCollectorBuilder`, `CompactorBuilder`, and
`DbReaderBuilder` will assemble the main object store the same way, from
innermost to outermost:

```
1. The backend the user passed (a raw store: S3, GCS, Azure,
   InMemory, LocalFileSystem)
       |
       v
2. InstrumentedObjectStore (per-operation metrics)
       |
       v
3. RetryingObjectStore (transient failure retries)
       |
       v
4. The cache wrapper the user supplied, if any (the bundled
   CachedObjectStore or a custom intent-aware wrapper)
       |
       v
   SlateDB core (TableStore, ManifestStore, CompactionsStore, GC)
```

Layers 2 and 3 are SlateDB-managed and sit directly on the backend, the same
order SlateDB ships today. Layer 4 is whatever cache wrapper the user supplied,
applied on top. SlateDB core will not construct a cache on the user's behalf;
the bundled `CachedObjectStore` is one of several wrappers the user may supply.

If the user supplies no wrapper, the protocol is a no-op for caching purposes:
SlateDB still attaches intents on its compacted SST calls, but nothing reads
them. If a wrapper is supplied, every read and write flows through it, with
compacted SST calls carrying a typed intent on the extensions and all other
calls carrying none.

Instrumentation and retry stay on the backend for two reasons. The metrics on
layer 2 count only requests that reach the backend, so a cache hit served by
layer 4 is not counted and the numbers continue to measure object store API
traffic. Retry on layer 3 handles only backend errors with the policy designed
for them, and the cache wrapper keeps full control of its own internal error
handling.

Users who want the bundled cache supply it as a wrapper with
`CachedObjectStore::builder`:

```rust
let cache = CachedObjectStore::builder()
    .root_folder(dir)
    .build();
let db = Db::builder(path, raw_backend)
    .with_object_store_wrapper(cache)
    .build()
    .await?;
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

This is the same physical order SlateDB ships today: instrumentation and retry
wrap the backend, and the cache wraps them. What changes is only how the cache
is constructed. Today SlateDB reads `Settings::object_store_cache_options` and
builds `CachedObjectStore` itself. Under this proposal the user supplies the
cache as a wrapper and SlateDB applies it at step 4.

Because retry sits below the cache wrapper, the cache keeps full control of its
own internal error handling. A transient disk I/O failure inside the cache, a
corrupt local part, or a missing cache file are the cache's to resolve (fall
back to upstream, evict and refetch, or surface the error), and SlateDB does
not apply a backend retry policy to them. The cache's own calls to its inner
store still get backend retries, because that inner store is the retrying
store, so a custom wrapper never implements retry for backend calls itself.

Two consequences worth calling out:

- Compactor and GC builders will only add instrumentation and retry on the
  backend; they will not apply a cache wrapper. Users who want compactor reads
  or GC operations to go through their own cache supply the wrapper to those
  builders.
- Manifest writes, compactions state writes, and WAL traffic will flow through
  the cache wrapper (because there is only one main store handle) but carry no
  intent. The bundled wrapper treats untagged calls as not-cacheable, so they
  are served from upstream. See [Cache wrapper behavior](#cache-wrapper-behavior).

### The cache as a wrapper

A cache plugs in as an `ObjectStoreWrapper`. SlateDB owns the seam: it builds the
instrumented, retrying backend first, then calls the wrapper to wrap it. This is
what keeps instrumentation and retry on the backend while leaving the cache
pluggable.

```rust
/// A wrapper SlateDB applies on top of the instrumented, retrying backend.
///
/// SlateDB instruments and wraps the user's backend with retry first, then
/// calls `wrap`, so the returned store sits above retry and below SlateDB core.
#[async_trait]
pub trait ObjectStoreWrapper: Send + Sync {
    async fn wrap(
        &self,
        inner: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn ObjectStore>, SlateDBError>;
}
```

Each builder assembles the main store the same way. The instrument and retry
unit is unchanged from today; the only new step is the optional wrapper on top:

```rust
// Unchanged: Instrument then Retry over the user's backend, with the
// per-builder component and store_type labels.
let base = instrumented_retrying_object_store(
    backend, &recorder, component, store_type, rand.clone(), system_clock.clone(),
);

// The cache wrapper, if supplied, wraps the instrumented, retrying backend.
let main_store = match &self.object_store_wrapper {
    Some(wrapper) => wrapper.wrap(base).await?,
    None => base,
};
```

**The bundled `CachedObjectStore`.** `CachedObjectStore` is now the wrapper the
user constructs: it holds the cache configuration and implements
`ObjectStoreWrapper`. The live cache `ObjectStore` that does the part-file I/O
becomes a private inner type (`CachedObjectStoreInner` below); `wrap` builds it
from the configuration and the inner store. The async setup that
`CachedObjectStore::from_config` does today moves onto that inner type:

```rust
impl CachedObjectStore {
    /// Start building the bundled cache wrapper. Holds config only, no I/O yet.
    pub fn builder() -> CachedObjectStoreBuilder { /* ... */ }
}

#[async_trait]
impl ObjectStoreWrapper for CachedObjectStore {
    async fn wrap(
        &self,
        inner: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn ObjectStore>, SlateDBError> {
        // `inner` is the instrumented, retrying backend for this component.
        // `CachedObjectStoreInner` is the private live cache store.
        let cache = CachedObjectStoreInner::from_config(inner, &self.options, /* ... */).await?;
        Ok(cache)
    }
}
```

In every case the backend, instrumentation, and retry are assembled by SlateDB
in the same order it uses today. The cache is the one wrapper that moves out of
SlateDB and into a user-supplied `ObjectStoreWrapper`.

### Cache wrapper behavior

The bundled `CachedObjectStore` initially keeps its current behavior:

- Reads always admit fetched bytes to the local cache on a miss.
- Writes follow the existing `cache_puts: bool` master switch (off by
  default; when on, every PUT is written through).
- Multipart puts pass through (never cached), as today.

Adopting the intent protocol does not change any of that on day one. What it
does change is what the wrapper *can* do without further plumbing into
SlateDB core. Each of these evolutions is now expressible as a small,
isolated wrapper change, and each can land as its own follow-up:

| intent | Wrapper change |
|-------------------------------------------|----------------|
| Untagged PUT (WAL, manifest, compactions state) | Skip the local write; forward to upstream only. |
| `WriteIntent::compaction_output()` on PUT | Add option to cache on compaction |
| `ReadIntent::compaction_input()` on GET | Add option to skip cache lookup and skip miss-admit; forward to upstream. |
| `ReadIntent.retry = Some(_)` on GET | Delete the cache entry for `path`, then run the usual cache-aware fetch. Closes [problem 3](#the-problem-today); lets the fsync workaround be removed. |

Each future change is local to the wrapper. SlateDB core's contract
(tag compacted SST reads and writes with intent, leave everything else
untagged) does not move.

Concrete policy choices (admission per kind, part sizes, prefetch shape,
on-disk layout, eviction strategy) are owned by the wrapper. Users who want
different choices implement their own `ObjectStore` and pass it to
`Db::builder`.

### Example

Simplest form with the bundled cache:

```rust
let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
let cache = CachedObjectStore::builder()
    .root_folder(cache_dir)
    .build();
let db = Db::builder("my-db", backend)
    .with_object_store_wrapper(cache)
    .build()
    .await?;
```

With more options set:

```rust
let cache = CachedObjectStore::builder()
    .root_folder(cache_dir)
    .cache_puts(true)
    .part_size_bytes(8 * 1024 * 1024)
    .max_cache_size_bytes(32 * 1024 * 1024 * 1024)
    .build();
```

For tests or advanced wiring (shared metrics recorder, mock clock,
deterministic RNG):

```rust
let cache = CachedObjectStore::builder()
    .root_folder(cache_dir)
    .metrics_recorder(shared_recorder)
    .system_clock(mock_clock)
    .rand(seeded_rand)
    .build();
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

A future optimization, if allocations come to matter, is to switch to
`SlateDbObjectStore` (zero allocation overhead, see
[Alternatives](#alternatives)) or to a static-intent-per-handle arrangement if
`object_store` ever changes `get_opts` to take `&GetOptions`.


## Caveats

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

  Practical effect: in production, small compacted SST writes (memtable flushes
  that fit in 10 MB, occasional small compaction outputs) may reach the wrapper
  untagged. Since the bundled `CachedObjectStore` initially keeps its current
  policy, those writes follow the pre-protocol behavior. WAL and manifest
  writes carry no intent regardless, so they are unaffected.

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

- [x] Manifest format: unchanged. Manifest writes carry no intent (untagged)
  and flow through the wrapper, which serves them from upstream.
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection: GC deletes will flow through the wrapper, triggering
  cache eviction as a side effect. GC will use the same consolidated main object
  store handle.
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence: writes carry no intent (untagged); the
  `compactions_store` path is unchanged.
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [x] Compactions format: compaction-output SST writes will carry
  `WriteIntent::compaction_output()`; compaction-input reads will carry
  `ReadIntent::compaction_input()`.

### Storage Engine Internals

- [x] Write-ahead log (WAL): WAL reads and writes carry no intent (untagged), so
  a wrapper that treats untagged calls as not-cacheable does not admit them.
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

### Performance and Cost

Initially, the measurable runtime change is per-call tagging overhead:

- **CPU and memory pressure.** Tagging adds a small per-call heap allocation
  overhead. Latency against network or local-disk I/O is unaffected, and
  aggregate allocator traffic on compaction or warmup stays well below the I/O
  cost. See [Allocation overhead](#allocation-overhead).

Latency, throughput, and object-store cost do not otherwise change initially
because the bundled wrapper keeps its current behavior (see
[Cache wrapper behavior](#cache-wrapper-behavior)). They can evolve as the
follow-ups in that section land:

- Skipping write-through for untagged writes (WAL, manifest, compactions
  state) and for compaction outputs reduces local disk writes when
  `cache_puts: true` and leaves more room for hot data.
- Bypassing compaction-input reads keeps one-shot bulk reads out of the
  cache, trading cache-hit savings on compactor reads for a smaller
  working set on foreground reads.
- Evicting on retry-tagged reads lets the fsync workaround go away,
  removing one local disk sync per admitted write at the cost of one
  extra upstream GET on the rare CRC-mismatch path.

### Observability

- SlateDB will add no new metrics for the protocol itself.
- The bundled `CachedObjectStore` retains its existing hit-rate, eviction, and
  admission stats.
- User-supplied wrappers own their own metrics.

### Compatibility

- **API.** Users who set
  `Settings::object_store_cache_options.root_folder` to enable the bundled cache
  will need to construct a `CachedObjectStore::builder()` and supply it through
  `Db::builder(..).with_object_store_wrapper(..)` (the auto-construct convenience
  is removed when the field moves out of `Settings`). The bundled cache's
  initial admission behavior, including `cache_puts`, is preserved.
- **On-disk format.** Unchanged. The disk format used by `CachedObjectStore`
  stays the same, so cached files survive the migration.
- **Wire format.** Unchanged.
- **Wrappers and backends that ignore the protocol.** Unaffected.




## Future Evolution

This RFC tags only compacted SSTs because that is what the cache needs. A
natural follow-up is to tag every object store call: WAL segments and fences,
manifests, compactions state, and the GC boundary file. That would let a
wrapper apply per-kind policy to all traffic (for example skipping WAL
write-through explicitly rather than by absence of a tag) and observe every
call by purpose.

It is deferred because the cache does not need it and it is more invasive.
Manifest and compactions traffic flows through `slatedb-txn-obj`, whose
protocol and boundary objects would have to carry caller-supplied extensions,
and WAL reads and writes would each need tagging. Taking it on would also add
kinds to the intent enums (`Wal`, `Manifest`, `Warmup`, and so on).

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
  cache no longer required for correctness, but it seems like the right step to
  do after the RFC is executed.
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
- [Draft PR for a prototype for the implementation](https://github.com/nomiero/slatedb/pull/4)
