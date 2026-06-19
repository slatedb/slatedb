# Decoupled Pluggable Object Store Cache

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [The call tag](#the-call-tag)
   - [Scope: TableStore SST calls only](#scope-tablestore-sst-calls-only)
   - [The store kind](#the-store-kind)
   - [Retry on validation failure](#retry-on-validation-failure)
   - [Where the cache lives](#where-the-cache-lives)
   - [Cache wrapper behavior](#cache-wrapper-behavior)
   - [Allocation overhead](#allocation-overhead)
- [Caveats](#caveats)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Accepted

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

SlateDB reads and writes its SSTs through `object_store`, so every cache miss is
a network call. Caching helps at two layers: a block cache for hot blocks
(already pluggable via Foyer) and an object store cache for SST file parts on
local disk. The object store cache today ships inside the SlateDB crate as
`CachedObjectStore`. SlateDB owns its policy, and that policy is coarse: a single
`cache_puts: bool` toggles write-through and every object is treated the same.

This RFC makes two changes:

1. **Call tagging.** Every object store call the `TableStore` issues for an SST
   carries a tag in `object_store::Extensions`: the owning store's kind
   (the call source), the SST type (WAL or compacted), and an optional retry
   reason. A cache wrapper reads the tag and applies its own policy. Only
   TableStore SST calls are tagged; manifest, compaction state, and other
   coordination I/O are left untagged because a cache has no reason to classify
   them.
2. **Pluggable cache placement.** `CachedObjectStore` is no longer constructed
   inside core. The user passes the backend to `Db::builder` and, optionally, a
   cache as an `ObjectStoreWrapper`. The builder wraps the backend with
   instrumentation and retry as it does today, then applies the wrapper on top.

The block cache is unchanged. This RFC is about the object store cache.

## Motivation

SlateDB has two caches: the Foyer block cache (typed blocks, filters, indexes,
stats, already pluggable) and the object store cache (`CachedObjectStore`, which
splits each SST into fixed-size part files on local disk). This RFC changes only
the object store cache.

Some problems follow from the current architecture:

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

## Goals

- Classify each SST call by source, type, and retry state.
- Let the user plug in their own `ObjectStore` (cache or otherwise) without
  SlateDB knowing it is there.
- Keep the bundled `CachedObjectStore` available as one such wrapper.

## Non-Goals

- Defining an on-disk format for any cache.
- Picking a canonical admission or eviction policy. That is the wrapper's job.
- Building automatic warmup or eviction. Warmup stays caller-driven via
  `DbCacheManagerOps::warm_sst`.
- Splitting the object store cache into its own crate. See
  [Open Questions](#open-questions).

## Design

### The call tag

```rust
/// The component whose TableStore issued the call (the call source).
pub enum TableStoreKind { Main, Reader, Compactor, GC }

/// The kind of SST the call targets.
pub enum SstType { Wal, CompactedSst }

/// Why a read is being reissued after a recoverable validation failure.
pub enum RetryReason { CrcMismatch, BlockDecodeError, DecompressionError }

/// Inserted into Extensions on every TableStore SST call.
pub struct ObjectStoreCallTag {
    pub kind: TableStoreKind,
    pub sst_type: SstType,
    pub retry: Option<RetryReason>,
}
```

SlateDB inserts one `ObjectStoreCallTag` into the `extensions` of every
`GetOptions`, `PutOptions`, and `PutMultipartOptions` the TableStore builds for
an SST. A wrapper reads the whole tag with one lookup:

```rust
if let Some(tag) = options.extensions.get::<ObjectStoreCallTag>() {
    // classify by tag.kind, tag.sst_type, tag.retry
}
```

The three fields are everything a cache needs: `kind` says which component
issued the call, `sst_type` separates WAL from compacted bytes, and `retry`
flags a reissued read. Combined with the call type the wrapper already sees.

### Scope: TableStore SST calls only

Only the TableStore's SST calls are tagged:

- compacted SST `get_opts` / `put_opts` / multipart init,
- WAL SST `get_opts` / `put_opts`,
- the SST metadata HEAD (`get_opts` with `head`).

Manifest reads and writes, compaction-state reads and writes, garbage collector
listings and deletes, and any other component's object store traffic are not
tagged. The cache stores SST bytes, so those are the only calls it needs to
classify. Untagged calls flow through a wrapper unchanged and get its default
behavior.

### The store kind

A `TableStore` is constructed with a fixed `kind` that identifies the component
that owns it:

- `Main`: the primary database (foreground reads, memtable flush writes).
- `Reader`: `DbReader` and the standalone WAL/SST readers.
- `Compactor`: compaction input reads and compaction output writes.
- `GC`: the garbage collector's store.

Each builder (`DbBuilder`, `DbReaderBuilder`, `CompactorBuilder`,
`GarbageCollectorBuilder`) wires its TableStore with the matching kind. The
embedded compactor and garbage collector use separate stores so their calls
carry distinct kinds.

### Retry on validation failure

When decoding an SST read fails with a recoverable error (CRC mismatch, block
decode, decompression), the TableStore reissues the read once with
`retry = Some(reason)`. Both compacted and WAL reads are reissued, so a wrapper
caching either kind can drop a corrupt local copy on the retry.

The retry tag tells a wrapper to drop its cached part for the path and refetch
from upstream instead of returning the same corrupt bytes. Without it, a wrapper
that keeps serving a corrupt part would trap the caller in a retry loop. This
also lets the fsync workaround from problem (3) be removed: readers no longer
need to hold the cache to evict on a CRC mismatch.

### Where the cache lives

SlateDB talks to object storage through `Arc<dyn ObjectStore>`. The user passes
the innermost backend to `Db::builder(path, backend)`. A cache (or any other
wrapper) is supplied separately through `.with_object_store_wrapper(..)`:

```rust
#[async_trait]
pub trait ObjectStoreWrapper: Send + Sync {
    async fn wrap(&self, inner: Arc<dyn ObjectStore>)
        -> Result<Arc<dyn ObjectStore>, SlateDBError>;
}
```

Each builder assembles the main store innermost to outermost, the same order
SlateDB ships today:

```
1. backend (S3, GCS, Azure, InMemory, LocalFileSystem)
2. InstrumentedObjectStore (per-operation metrics)
3. RetryingObjectStore (transient failure retries)
4. the user's cache wrapper, if any
   -> SlateDB core (TableStore, ManifestStore, CompactionsStore, GC)
```

Instrumentation and retry stay on the backend: layer 2 then counts only requests
that reach the backend (a cache hit is not counted), and layer 3 retries only
backend errors, leaving the cache in control of its own error handling. The
bundled cache plugs in as an `ObjectStoreWrapper`:

```rust
let cache = CachedObjectStore::builder().root_folder(dir).build();
let db = Db::builder(path, backend)
    .with_object_store_wrapper(cache)
    .build()
    .await?;
```

`ObjectStoreCacheOptions` moves out of `Settings` into
`slatedb::cached_object_store`. Core has no field describing the cache; the tag
on each call is the only contract it knows. If the user supplies no wrapper, the
tags are inert. Two consequences:

- Compactor and GC builders add instrumentation and retry only; users who want
  those reads or deletes cached supply the wrapper to those builders.
- Manifest and compaction-state writes are simply untagged, so the wrapper sees
  them as default-policy calls (they are not SST bytes and need no special
  short-circuit).

### Cache wrapper behavior

The bundled `CachedObjectStore` keeps its current behavior initially (admit on
read miss; write-through governed by `cache_puts`; multipart passes through).
The tag is what lets the wrapper evolve without further plumbing into core, each
as an isolated follow-up:

| Tag on the call                   | Example wrapper policy                              |
|-----------------------------------|----------------------------------------------------|
| `sst_type == Wal` on PUT          | skip the local write; forward upstream only        |
| `sst_type == CompactedSst` on PUT | option to admit compaction outputs                 |
| `kind == Compactor` on GET        | option to bypass the cache for one-shot input scans|
| `retry == Some(_)` on GET         | drop the cached part for the path, then refetch     |

Concrete choices (admission, part size, eviction, on-disk layout) are the
wrapper's. Users who want different choices implement their own `ObjectStore`.

## Caveats

- **Upstream `BufWriter` extensions bug.** Compacted SST writes go through
  `object_store::buffered::BufWriter` with the tag attached via
  `.with_extensions(..)`. `BufWriter::poll_shutdown` dropped extensions on the
  single-PUT path (payload fits in capacity); the multipart overflow path
  forwarded them. The fix is done upstream
  ([apache/arrow-rs-object-store#735](https://github.com/apache/arrow-rs-object-store/issues/735))
  and should be available in the next `object_store` release, after which all
  compacted SST writes carry the tag regardless of size.
- **Multipart tag on the wire.** The tag attaches at `put_multipart_opts` init,
  not on each `put_part`. A wrapper that wraps `MultipartUpload` (which it must
  do to observe part bytes) holds the tag in its own state. The bundled cache
  does not admit multipart uploads, so this does not affect it.

## Impact Analysis

### Metadata, Coordination, and Lifecycles

- [x] Manifest format: unchanged. Manifest I/O is untagged and flows through any
  wrapper with default policy.
- [x] Garbage collection: GC deletes flow through the wrapper and can trigger
  cache eviction as a side effect; GC reads carry `kind = GC`.

### Compaction

- [x] Compactions format: compaction reads and writes carry `kind = Compactor`
  with the appropriate `sst_type`.

### Storage Engine Internals

- [x] Write-ahead log (WAL): WAL reads and writes carry `sst_type = Wal`.
- [x] Block cache: unchanged.
- [x] Object store cache: `CachedObjectStore` is decoupled from core
  construction and made user-constructible; its `cache_puts` behavior is
  preserved initially.
- [x] SST format: the decode path reissues retry-tagged reads on recoverable
  validation failures.

### Ecosystem & Operations

- [x] Observability: no new SlateDB metrics. The bundled cache keeps its existing
  stats; wrappers own their own observability.

## Operations

### Performance and Cost

The only initial runtime change is per-call tagging: one small heap allocation,
negligible against I/O. Latency, throughput, and object-store cost are otherwise
unchanged because the bundled wrapper keeps its current behavior. They evolve as
the follow-ups in [Cache wrapper behavior](#cache-wrapper-behavior) land (skip
WAL write-through, bypass compaction-input reads, evict on retry).

### Compatibility

- **API.** Users who enabled the bundled cache via
  `Settings::object_store_cache_options.root_folder` instead construct
  `CachedObjectStore::builder()` and pass it to
  `Db::builder(..).with_object_store_wrapper(..)`.
- **On-disk format.** Unchanged; cached files survive the migration.
- **Wire format.** Unchanged. Backends and wrappers that ignore the tag are
  unaffected.

## Alternatives

**Keep the cache in SlateDB.** Works today. Locks policy into core and forces
every policy change through a release. Rejected.

**Tag every `ObjectStore` call across all components.** An earlier draft tagged
every read and write SlateDB issues (memtable flush, compaction output, manifest,
compaction state, WAL) with a typed `WriteIntent` / `ReadIntent`, threaded
through every store including the shared `slatedb-txn-obj` boundary. Rejected: it
is invasive, spreading tagging plumbing across many components, and most of those
calls are irrelevant to an object store cache. Manifest and compaction-state
reads through `slatedb-txn-obj`, for example, are small coordination reads a part
cache has no reason to admit or classify. Tagging only the TableStore SST calls
(the bytes a cache actually stores) covers the cache's needs with far less
surface area, which is why this RFC scopes the tag to the TableStore.

**A SlateDB-owned `SlateDbObjectStore` trait.** Define SlateDB's own storage
trait whose methods take the tag as a regular argument, keeping (or dropping)
`object_store` underneath. Zero allocation overhead and SlateDB-owned, at the
cost of a new public trait to version and generic wrappers (rate limiter,
encryption) having to be rewritten as trait-aware. Not proposed; the Extensions
allocation is dwarfed by I/O, and the migration off Extensions is mechanical if
that changes.

**Defer to OpenDAL's cache layer.** OpenDAL merged a cache layer
([apache/opendal#6297](https://github.com/apache/opendal/pull/6297)). A user can
wire an OpenDAL-backed cache as their `ObjectStore`; if it reads typed hints from
`Extensions`, this protocol bridges to it. SlateDB does not need to choose.

**Static tag per `ObjectStore` handle (requires upstream API change).** Prebuild
`GetOptions` / `PutOptions` per store and share them, valid only if `get_opts`
took its options by reference. The change is upstream; if it lands this becomes
the allocation-free path that keeps the Extensions protocol.

## Open Questions

- **Should the object store cache live in its own crate?** Not proposed here.
  With the tag in place and the cache no longer required for correctness, it
  looks like the right next step after this RFC is executed.
- **Allocation cost in practice.** Not measured. Worth profiling on compaction
  and warmup paths before any further optimization.

## References

- [`object_store::GetOptions` (carries `extensions`)](https://docs.rs/object_store/latest/object_store/struct.GetOptions.html)
- [`object_store::PutOptions` (carries `extensions`)](https://docs.rs/object_store/latest/object_store/struct.PutOptions.html)
- [RFC 0023: Targeted Cache Warming and Best-Effort Block Cache Eviction](0023-cache-manager.md)
- Relevant issue: [slatedb/slatedb#703](https://github.com/slatedb/slatedb/issues/703)
- Existing in-tree `CachedObjectStore` (`slatedb/src/cached_object_store/`)
- Upstream `BufWriter` extensions bug: [apache/arrow-rs-object-store#735](https://github.com/apache/arrow-rs-object-store/issues/735)
- OpenDAL cache layer: [apache/opendal#6297](https://github.com/apache/opendal/pull/6297)

## Updates

- Redesigned the protocol from a per-call write-kind / read-kind intent tagged
  across all components to a single `ObjectStoreCallTag` (store kind, SST type,
  retry reason) scoped to the TableStore's SST calls. The wider scheme is kept as
  a rejected alternative.
