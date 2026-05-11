# Decooupled Pluggable Object Store Cache

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
   - [Two layers of caching](#two-layers-of-caching)
   - [The problem today](#the-problem-today)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [Where the cache lives](#where-the-cache-lives)
   - [The intent protocol](#the-intent-protocol)
   - [Choice of interface: `Extensions` vs `SlateDbObjectStore`](#choice-of-interface-extensions-vs-slatedbobjectstore)
   - [SlateDB core: protocol contract](#slatedb-core-protocol-contract)
   - [SlateDB core: supporting plumbing](#slatedb-core-supporting-plumbing)
   - [What the cache wrapper does](#what-the-cache-wrapper-does)
   - [A worked example](#a-worked-example)
   - [Allocation overhead](#allocation-overhead)
- [Impact Analysis](#impact-analysis)
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

Status: Draft

Authors:

* [Hussein Nomier](https://github.com/nomiero)

## Summary

SlateDB stores its data in object storage, so every cache miss is a
network call. Caching helps at two layers: a **block cache** for decoded
hot blocks (already pluggable via Foyer) and an **object store cache**
for compressed SST files on disk. Today the object store cache ships
built in (`CachedObjectStore`), lives inside the SlateDB crate, and
exposes only a coarse on/off knob: cache on read, or cache on read and
on put. It does not distinguish between file kinds (WAL, flush output,
compaction output, manifest), so policy is the same for every object
that flows through.

This RFC proposes two changes. First, give the object store cache a
richer signal so it can apply different policies per case: WAL writes,
manifest writes, compaction inputs and outputs, foreground reads,
warmup reads, and retries after a bad cache hit. SlateDB tags each
read and write with a piece of metadata describing the call's intent
(read kind, write kind, retry reason), and the wrapper uses that
metadata to decide what to cache. Second, decouple the cache from the
SlateDB core implementation and make it extensible: it still ships
with SlateDB (likely in the same crate), but becomes an opt-in wrapper
that users plug in, or replace with their own, rather than being
built into the core engine.

How the metadata reaches the wrapper, whether through
`object_store::Extensions` or through a SlateDB-owned
`SlateDbObjectStore` trait, is an open question. Both options carry
the same typed intents and require the same work from SlateDB core;
they differ on allocation cost and on what kinds of generic wrappers
compose around the cache. The trade-off is discussed in
[Choice of interface](#choice-of-interface-extensions-vs-slatedbobjectstore).

The block cache stays as it is. The focus in this RFC is to decouple the
object store cache from SlateDB core.

## Motivation

### Two layers of caching

SlateDB is an LSM tree on object storage. Reads are slow when they go
over the network, so caching helps. There are two useful layers to cache
at, and they suit different workloads.

**Object store cache (whole compressed SSTs on disk).** Each SST file is
stored as a single object on local disk. Reads are served by `pread`
against the file and return the same compressed bytes object storage
would have returned. The cache is structurally simple: one file per
SST, atomic file deletes, no in-memory index to maintain.

**Block cache (decoded blocks via Foyer hybrid cache).** Foyer is a
mature, high-performance hybrid cache that stores decoded blocks (and
indexes, filters, stats) tiered across memory and disk. A hit returns
a decoded block directly, skipping decompression. Foyer applies its own
admission and eviction policies at the block level.

Both caches save the network round trip on a hit. They differ in what
they cache, what they pay to maintain it, and how they behave under
load:

| Dimension                       | Object store cache (whole SSTs)              | Block cache (Foyer hybrid)                                            |
|---------------------------------|----------------------------------------------|-----------------------------------------------------------------------|
| Cached unit                     | Entire compressed SST file                   | Decoded blocks, filters, index, stats                                 |
| Decompression on hit            | Paid on every read                           | Avoided                                                               |
| Write path                      | One sequential file write per SST            | Memory insert, buffered, then flushed to log blocks on disk           |
| Behavior under write pressure   | Direct disk write; errors surface            | Bounded write buffer; entries may be silently dropped if saturated    |
| On-disk layout                  | One file per SST                             | Log-structured blocks with internal compaction to reclaim holes       |
| Write amplification             | None beyond the file write                   | Log compaction rewrites blocks to reclaim space                       |
| Per-SST overhead (256 MiB SST)  | One file create, one file write              | ~64k block writes plus index entries (at 4 KiB blocks)                |
| Concurrent compactions          | Independent file writes                      | Share the same write buffer; can amplify back-pressure                |
| Eviction of one SST             | Single file delete                           | ~64k index entries removed plus background log compaction             |
| Restart cost                    | Zero: open files on demand                   | Rebuild the in-memory index from on-disk metadata at startup          |
| Memory footprint                | File metadata only                           | In-memory index grows with cached items (can reach GBs)               |
| Admission granularity           | Whole file: cold blocks admitted with hot    | Per-block: only hot units admitted                                    |

**The two caches are not an either-or choice.** They sit at different
levels of the read path and can be combined in whatever mix fits the
workload. A few common shapes:

- **Object store cache only.** Cache whole SSTs on disk; rely on the OS
  page cache for any in-memory reuse. Simple, restart-friendly, and a
  good fit when the working set is too large for memory but fits on SSD.
- **Block cache for metadata only, plus object store cache.** Keep
  filters, indexes, and stats in Foyer (in memory) so every read can
  skip object storage for planning, and serve data block reads from the
  object store cache on disk. Memory footprint stays small (metadata
  only), and data reads still avoid the network.
- **Block cache for hot decoded blocks, plus object store cache for
  everything else.** Foyer holds the small hot set decoded in memory
  (no decompression on hit), the object store cache holds whole SSTs on
  disk for warm reads, and only cold reads pay the network. This is the
  most complete configuration and the most memory-intensive.
- **Block cache only.** Tiered memory and disk via Foyer, with no
  separate file cache. A good fit when tolerating object-storage tail
  latency (often tens of milliseconds) on cold reads is acceptable and
  decoded-block reuse is the primary benefit.

The long-term goal is to keep caching policy out of SlateDB core for
both layers. This RFC focuses on the object store cache. The block
cache is already pluggable (RFC 0023 covers warming and eviction for
it).

### The problem today

The object store cache is built into the SlateDB crate. Two problems
follow:

1. **SlateDB owns policy it shouldn't own, and the policy is coarse.**
   The current cache only exposes an on/off-style config: cache on read,
   or cache on read and on put. It treats every object the same way,
   with no signal to distinguish a short-lived WAL write from a
   long-lived compaction output. Refining that policy, or adding any
   new signal, means editing core.
2. **Two caches, two different pluggability levels.** The block cache is
   already pluggable. The object store cache is not.

The `object_store` crate already provides a suitable mechanism. Its
`GetOptions`, `PutOptions`, and `PutMultipartOptions` types each carry
an `extensions` field. SlateDB can attach metadata to it, and a wrapper
can read it back. Backends that do not know about the metadata (S3,
GCS, Azure) ignore it.

## Goals

- Move the object store cache out of the SlateDB core implementation, so users plug it in as a wrapper rather than getting it bundled into the engine.
- Define a small protocol so the wrapper knows what each call is for.
- Keep SlateDB unaware of whether any cache is installed.

## Non-Goals

- Defining an on-disk format for any specific cache.
- Picking an admission or eviction policy. That is the wrapper's job.
- Changing the in-process block cache.
- Building automatic warmup or eviction. Warmup stays caller-driven.

## Design

### Where the cache lives

SlateDB already talks to object storage through `Arc<dyn ObjectStore>`. The
cache becomes a wrapper that sits in front of the real backend:

```text
              +--------------------+
              |    SlateDB core    |
              +---------+----------+
                        |
                        v
              +--------------------+
              | Arc<dyn ObjectStore> |
              +---------+----------+
                        |
                        v
       +-----------------------------------+
       |   Cache wrapper (optional)        |
       |   (object store cache, tiered,    |
       |    encryption-aware, ...)         |
       +-----------------+-----------------+
                         |
                         v
              +--------------------+
              | Backend (S3/GCS/   |
              |  Azure/local fs)   |
              +--------------------+
```

If the user does not install a wrapper, the handle points straight at the
backend and the protocol is a no-op. If the user installs one, every read
and write flows through it.

Today's `CachedObjectStore` becomes one such wrapper. It continues to
ship with SlateDB, but it lives alongside the core engine rather than
inside it: users opt in by constructing the wrapper and passing it as
their `ObjectStore`.

### The intent protocol

The protocol is a small set of types. SlateDB inserts one of them into
`extensions` on every read or write.

```rust
pub struct WriteIntent { pub kind: WriteKind }

pub enum WriteKind {
    Flush,             // memtable flush to L0
    CompactionOutput,  // compaction output SST
    Manifest,          // manifest write
    Wal,               // write-ahead log
}

pub struct ReadIntent {
    pub kind: ReadKind,
    pub retry: Option<RetryReason>,
}

pub enum ReadKind {
    Foreground,        // user query
    CompactionInput,   // compaction reading an input SST
    Warmup,            // explicit cache warmup
}

pub enum RetryReason {
    CrcMismatch,
    BlockDecodeError,
    DecompressionError,
}
```

The protocol covers three things the wrapper needs to know:

1. **What kind of write is this?** (`WriteIntent`) Lets the wrapper decide
   admission per kind. A typical policy keeps data-bearing writes (`Flush`,
   `CompactionOutput`) and skips short-lived or tiny ones (`Wal`,
   `Manifest`), but the policy is the wrapper's choice.
2. **What kind of read is this?** (`ReadIntent`) Lets the wrapper apply
   different policies to foreground queries, compaction-input scans (which
   can pollute the cache), and explicit warmups.
3. **Is this a retry after a bad cache hit?** (`ReadIntent.retry`)
   SlateDB sets this when an SST decode fails (CRC, block decode,
   decompression). The wrapper must drop the cached file and refetch from
   the backend. Without this, the wrapper would return the same corrupt
   bytes on every retry.

The enums are deliberately not marked `#[non_exhaustive]`. Adding a new
variant is a breaking change for wrapper authors, which is the right
default: every new intent should require wrapper authors to decide how
to handle it.

### Choice of interface: `Extensions` vs `SlateDbObjectStore`

The intent types defined above are independent of how they reach the
wrapper. There are two reasonable interfaces, and this RFC does
**not** pick one. The decision is left as an open question.

**Option A: `Extensions` on `object_store` types.** SlateDB inserts
the intent into `GetOptions::extensions` / `PutOptions::extensions` on
each call. The wrapper retrieves it with `extensions.get::<ReadIntent>()`
or `get::<WriteIntent>()`. No new public trait, and composes directly
with any generic `ObjectStore` wrapper (rate limiter, encryption,
logging) at any level. The cost is three small heap allocations per
call (the `Box<HashMap>` for the lazily-initialized `Extensions` map,
the HashMap's bucket array on first insert, and the `Box<val>` for the
intent itself). See [Allocation overhead](#allocation-overhead).

**Option B: A separate `SlateDbObjectStore` trait.** SlateDB defines
its own storage trait whose methods take intents as typed arguments
(see Alternatives for the sketch). Zero allocation overhead. As a
secondary benefit, the trait is SlateDB-owned and can grow new methods
or arguments (batch operations, richer cache-control hints, telemetry
callbacks, per-call deadlines) without coordinating with the upstream
`object_store` crate, which `Extensions` cannot match. The trade-off
is a new public trait that SlateDB must version, and generic
intermediate wrappers (rate limiter, encryption) must either sit
**below** the cache (between cache and backend, where they compose
as plain `ObjectStore` wrappers) or be rewritten as
`SlateDbObjectStore`-aware versions if they need to sit above.

The two options agree on the intent types and on the responsibilities
of SlateDB core (tag every read and write, retry-on-validation-failure,
list-live-ssts admin API). They differ on the call interface and on
SlateDB's future ability to extend the storage contract.

For concreteness, the rest of this RFC describes the design through
the `Extensions` interface. Switching to `SlateDbObjectStore` later
is a mechanical translation: replace `extensions.insert(intent)` with
a typed argument and `extensions.get::<T>()` with a method
parameter.

### SlateDB core: protocol contract

SlateDB takes on three protocol obligations.

1. **Tag every write with `WriteIntent`.** Every `put_opts` and multipart
   upload sets a `WriteIntent`. WAL writers use `Wal`. Flush outputs use
   `Flush`. Compaction outputs use `CompactionOutput`. Manifest writes use
   `Manifest`. No untagged writes leave SlateDB.

2. **Tag every read with `ReadIntent`.** Every `get_opts` sets a
   `ReadIntent`. Default is `Foreground`. Compaction inputs and warmup
   paths use the matching variants. No untagged reads leave SlateDB.

3. **Retry on validation failure with a retry tag.** If decoding an SST
   fails with a recoverable error (CRC mismatch, bad block, bad
   decompression), SlateDB reissues the read with `retry =
   Some(reason)`. SlateDB may retry up to a bounded number of times
   before surfacing the failure to the caller. The retry tag tells the
   wrapper to drop the cached file rather than return the same corrupt
   bytes again.

That is the whole protocol contract.

### SlateDB core: supporting plumbing

Two pieces of plumbing follow from the protocol but are not part of the
contract itself.

- **Warmup enumeration.** A small admin API returns the paths of live
  SSTs from the latest manifest. A user-written warmup loop reads from
  that list, issues warmup-tagged GETs at its chosen concurrency, and
  the wrapper fills its cache. SlateDB does not need to own the loop, it only
  lists the paths.
- **Deletes flow through the same handle.** Garbage collection already
  calls `ObjectStore::delete`. That call passes through the wrapper, so
  the wrapper can drop its cached file as part of handling the delete.
  No separate hook is needed.

### What the cache wrapper does

The wrapper is a normal `Arc<dyn ObjectStore>`. For each operation:

- **`put_opts` / `put_multipart_opts`:** Read the `WriteIntent`. If the
  wrapper admits this kind, forward to the backend and copy the bytes
  into local storage as they go (for multipart, tee through). A wrapper
  may also cache head metadata at this point and may need to handle
  orphan bytes from aborted multipart uploads, but those are
  wrapper-side concerns, not protocol requirements.
- **`get_opts`:** Read the `ReadIntent`. If `retry` is set, drop the
  cached file for that path and fetch from the backend. Otherwise serve
  from cache when possible, and decide based on `kind` whether to admit
  on a miss.
- **`delete`:** Forward to the backend. On success, drop the cached
  file. SlateDB only deletes SSTs that no live reader can reach, so
  there is no contended delete-vs-read case to handle.
- **`head`:** Serve from cached metadata when available; otherwise
  forward and cache the response.
- **`list` and other operations:** Pass through unchanged.

The wrapper owns concrete policy: which kinds get admitted, part sizes,
prefetch shape, on-disk layout, and eviction strategy.

### A worked example

Sketch of a wrapper that keeps `CompactionOutput` and `Flush` writes,
evicts on retry, and otherwise passes through. Shown using the
`Extensions` interface for concreteness; with `SlateDbObjectStore`
the same logic appears as typed method arguments instead of
extension lookups.

```rust
struct DiskCache {
    upstream: Arc<dyn ObjectStore>,
    local: LocalStore,
}

#[async_trait]
impl ObjectStore for DiskCache {
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        if let Some(intent) = opts.extensions.get::<ReadIntent>() {
            if intent.retry.is_some() {
                self.local.evict(path).await;
                return self.upstream.get_opts(path, opts).await;
            }
        }
        if let Some(hit) = self.local.get(path, &opts).await? {
            return Ok(hit);
        }
        let result = self.upstream.get_opts(path, opts).await?;
        self.local.admit(path, &result).await;
        Ok(result)
    }

    async fn put_opts(&self, path: &Path, payload: PutPayload, opts: PutOptions)
        -> Result<PutResult>
    {
        let admit = matches!(
            opts.extensions.get::<WriteIntent>().map(|i| i.kind),
            Some(WriteKind::CompactionOutput) | Some(WriteKind::Flush),
        );
        let result = self.upstream.put_opts(path, payload.clone(), opts).await?;
        if admit { self.local.write_through(path, payload, &result).await; }
        Ok(result)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.upstream.delete(path).await?;
        self.local.evict(path).await;
        Ok(())
    }
}

let store: Arc<dyn ObjectStore> = Arc::new(DiskCache::new(s3, "/var/cache"));
let db = Db::builder(path, store).build().await?;
```

### Allocation overhead

Tagging a single call pays three small heap allocations: the
`Box<HashMap>` for the lazily-initialized `Extensions` map, the
HashMap's bucket array on first insert, and the `Box<val>` for the
intent itself. None of the three can be reused across calls because
`ObjectStore::get_opts` and friends take `GetOptions` by value, and
`Extensions::clone()` is a deep clone of all three pieces.

This does not affect read latency in any meaningful way next to a
network or local-disk round-trip. The real cost is CPU and memory
pressure on hot paths: compaction reading thousands of blocks and
warmup loops issuing many GETs each pay the three allocations
regardless of whether the read itself is fast.

This allocation cost is specific to the `Extensions` interface. The
`SlateDbObjectStore` option (see
[Choice of interface](#choice-of-interface-extensions-vs-slatedbobjectstore)
and Alternatives) pays zero per-call allocations. Which interface to
pick is one of the open questions.

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

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection: GC deletes flow through the wrapper, triggering cache eviction as a side effect.
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [x] Write-ahead log (WAL): WAL writes carry `WriteIntent::Wal` so wrappers can skip admitting them.
- [x] Block cache: unchanged; the in-process block cache continues to operate independently.
- [x] Object store cache: the core subject; today's `CachedObjectStore` moves out of the core engine into an opt-in wrapper and consumes the typed intent protocol instead of the coarse on/off config it ships with today.
- [ ] Indexing (bloom filters, metadata)
- [x] SST format or block format: the decode path issues retry-tagged reads on recoverable validation failures.

### Ecosystem & Operations

- [x] CLI tools: new admin API (`list_live_sst_paths`) for warmup orchestration.
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing): SlateDB logs at debug level when it sets non-default intents; wrappers own hit-rate and eviction metrics.

## Operations

### Performance & Cost

- **Latency.** Cache hits skip the network. Misses pay one extra local
  write to populate the cache. Retries cost one extra backend GET,
  only when the cached copy was bad.
- **Throughput.** Write-through copying on multipart uploads uses local
  disk bandwidth proportional to compaction output size. Read gains
  scale with hit rate.
- **Object-store cost.** Caching long-lived writes and serving repeats
  from disk cuts upstream GETs and HEADs. PUT counts are unchanged.
- **CPU and memory pressure.** Tagging adds three small heap
  allocations per call: the `Box<HashMap>` for the lazily-initialized
  `Extensions` map, the HashMap's bucket array on first insert, and
  the `Box<val>` for the intent itself. Latency-wise this is
  negligible, but on hot paths (compaction, warmup) the aggregate
  allocator and cache-line traffic is non-zero. See
  [Allocation overhead](#allocation-overhead) for the mitigations.

### Observability

- SlateDB adds no new metrics. The wrapper owns hit rate, eviction,
  bytes cached, and similar.
- SlateDB logs at debug level when it sets a non-default intent (retry,
  warmup, compaction input), so operators can correlate SlateDB logs
  with wrapper metrics.

### Compatibility

- **API.** Breaking. Users who relied on the in-tree `CachedObjectStore`
  configuration surface will need to construct the wrapper explicitly
  and pass it as their `ObjectStore`.
- **On-disk format.** Unchanged. The disk format used by
  `CachedObjectStore` stays the same, so cached files survive the
  migration.
- **Wire format.** Unchanged.
- **Wrappers and backends that ignore the protocol.** Unaffected.

## Testing

TODO

## Rollout

TODO

## Alternatives

**Keep the cache in SlateDB.** Works today. Locks policy into SlateDB,
makes it hard to ship alternative caches, and forces every policy change
through a SlateDB release.

**A separate `SlateDbObjectStore` trait, not an extension of
`ObjectStore`.** Define SlateDB's own storage interface as a distinct
trait whose methods take intents as regular arguments:

```rust
pub trait SlateDbObjectStore {
    async fn get(&self, path: &Path, range: Range<usize>, intent: ReadIntent) -> Result<Bytes>;
    async fn put(&self, path: &Path, payload: PutPayload, intent: WriteIntent) -> Result<()>;
    async fn delete(&self, path: &Path) -> Result<()>;
    // and so on
}
```

This is the **allocation-efficient design**. It removes the `Extensions`
path entirely: intent is passed as a typed argument and no per-call
allocations happen at all. The trait is intentionally **not** an
extension of `ObjectStore` and does **not** rely on downcasting at
construction time. That keeps the contract explicit and avoids the
failure mode where an intermediate generic `ObjectStore` wrapper
silently drops intent because it does not know about a downcast path.

A second benefit, beyond allocation cost, is **extensibility**. The
`object_store` trait is fixed by the upstream crate and intentionally
narrow (get, put, delete, list, head, multipart). `SlateDbObjectStore`
is SlateDB-owned, so it can evolve with new methods or arguments
without coordinating with `object_store`. Possible future additions
that fit naturally on this trait but not on `ObjectStore`:

- Batch reads or writes (multi-path GET/PUT) where a wrapper can
  coalesce or pipeline calls.
- Cache-control hints richer than intent (expected reuse count, TTL,
  pinning, prefetch suggestions).
- Telemetry callbacks (per-call hit/miss signals back to SlateDB).
- Streaming or chunked reads with per-chunk cancellation.
- Operation-specific deadlines or priority.

None of these are part of this RFC. They are listed only to show
that `SlateDbObjectStore` also serves as an evolution point for
SlateDB's storage contract, which `Extensions` (constrained by
`ObjectStore`'s shape) does not provide.

**Default implementation.** SlateDB ships a default adapter that
wraps any `Arc<dyn ObjectStore>` and forwards calls, dropping intent
since plain backends do not act on it:

```rust
/// Default forwarder: implements SlateDbObjectStore on top of any
/// Arc<dyn ObjectStore>. Used when the user passes a plain backend
/// (S3, GCS, Azure, local fs) and does not need intent-aware
/// caching.
pub struct ObjectStoreForwarder {
    upstream: Arc<dyn ObjectStore>,
}

#[async_trait]
impl SlateDbObjectStore for ObjectStoreForwarder {
    async fn get(&self, path: &Path, range: Range<usize>, _intent: ReadIntent)
        -> Result<Bytes>
    {
        let opts = GetOptions { range: Some(range.into()), ..Default::default() };
        let result = self.upstream.get_opts(path, opts).await?;
        Ok(result.bytes().await?)
    }

    async fn put(&self, path: &Path, payload: PutPayload, _intent: WriteIntent)
        -> Result<()>
    {
        self.upstream.put(path, payload).await?;
        Ok(())
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.upstream.delete(path).await
    }
}
```

Users who want intent-aware caching implement `SlateDbObjectStore`
directly on their cache (which internally holds an `Arc<dyn
ObjectStore>` for the upstream backend, but routes calls based on the
typed intent without ever building an `Extensions`).

**How backends are chosen.** Picking a backend (S3, GCS, Azure,
InMemory, LocalFileSystem) does not change: users still construct
an `object_store::ObjectStore` impl. That `ObjectStore` sits one
level below the `SlateDbObjectStore` layer. Sketch of the end-to-end
wiring:

```rust
// 1. Pick a backend, same as today.
let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
// or:    Arc::new(AmazonS3Builder::from_env().build()?);
// or:    Arc::new(LocalFileSystem::new());

// 2. Pick a SlateDbObjectStore. Either the default forwarder (no
//    cache) or a cache wrapper (intent-aware).
let store: Arc<dyn SlateDbObjectStore> =
    Arc::new(ObjectStoreForwarder::new(backend));
// or:
//    Arc::new(CachedSlateDbObjectStore::new(backend, "/var/cache"));

// 3. Hand it to SlateDB.
let db = Db::builder(path, store).build().await?;
```

For the common "no cache" case, the builder can accept an
`Arc<dyn ObjectStore>` directly and wrap it in `ObjectStoreForwarder`
internally, preserving today's API shape:

```rust
let db = Db::builder(path, Arc::new(InMemory::new())).build().await?;
```

The trade-offs are:

- SlateDB now ships a public storage trait and owns its versioning.
- Users who want to compose generic `ObjectStore` wrappers (rate
  limiter, encryption, logging) **above** the cache must write
  `SlateDbObjectStore`-aware versions of those wrappers, or place
  them **below** the cache (between the cache and the backend), where
  they continue to compose as plain `ObjectStore` wrappers.

Whether to ship this trait or stay on `Extensions` is an open
question (see [Choice of interface](#choice-of-interface-extensions-vs-slatedbobjectstore)).

**Static intent per `ObjectStore` handle (requires upstream API change).**
Construct multiple internal handles in SlateDB, each preconfigured for
one intent (foreground, compaction inputs, warmup) and each holding a
pre-built `GetOptions` / `PutOptions` with the intent already
inserted. The savings only materialize if `ObjectStore::get_opts` (and
its put counterparts) take `&GetOptions` by reference instead of
`GetOptions` by value. That would let the handle share its pre-built
options across many calls and skip the three per-call allocations
entirely. With the current by-value API, this approach is purely
ergonomic: each call still rebuilds or clones the `Extensions` and
pays the same three allocations. The upstream change is on the
`object_store` crate; if and when it lands, this becomes the cleanest
allocation-free path that keeps the `Extensions` protocol.

**One unified cache for everything.** Build one cache that handles both
compressed files and decoded blocks. A bigger change with hard policy
questions (one eviction queue across two unit sizes, one admission
policy across two access patterns). Not ruled out long-term, but
orthogonal to the goal of getting policy out of core.

## Open Questions

- **Interface: `Extensions` or `SlateDbObjectStore`?** The intent
  types and the protocol contract are the same in either case. The
  decision comes down to: do we accept three small allocations per
  call in exchange for direct composition with generic `ObjectStore`
  wrappers (Extensions), or do we ship a SlateDB-owned storage trait
  with zero allocation overhead but a new public surface to version
  and a narrower set of wrappers it can compose with
  (`SlateDbObjectStore`)? See
  [Choice of interface](#choice-of-interface-extensions-vs-slatedbobjectstore)
  for the side-by-side and the Alternatives section for the trait
  sketch.
- **Failure modes that need explicit contracts.** The current draft does
  not pin these down; each needs a decision:
  - What does SlateDB do if the wrapper returns errors or panics? Treat
    as a backend failure?
  - What if a wrapper ignores the `retry` hint? SlateDB will hit its
    retry bound and surface the failure, but should it emit a specific
    error variant the operator can act on?
  - What if an upstream delete succeeds but eviction fails? The cache
    holds stale state until LRU reclaims it; is that acceptable?
- **Allocation cost in practice (if we go with `Extensions`).** How
  much do the three per-call allocations actually cost on reads,
  compaction, and warmup? Worth measuring before committing.

## References

- [`object_store::GetOptions` (carries `extensions: ::http::Extensions`)](https://docs.rs/object_store/latest/object_store/struct.GetOptions.html)
- [`object_store::PutOptions` (carries `extensions: ::http::Extensions`)](https://docs.rs/object_store/latest/object_store/struct.PutOptions.html)
- [RFC 0023: Targeted Cache Warming and Best-Effort Block Cache Eviction](0023-cache-manager.md)
- Existing in-tree `CachedObjectStore` (slatedb/src/cached_object_store/)

## Updates
