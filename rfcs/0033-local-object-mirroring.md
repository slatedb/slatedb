# Rewrite CachedObjectStore

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [GET and PUT Policies](#get-and-put-policies)
  - [Public API](#public-api)
  - [Architecture](#architecture)
  - [Whole-SST Local Layout](#whole-sst-local-layout)
  - [Construction and Cleanup](#construction-and-cleanup)
  - [Explicit Warming](#explicit-warming)
  - [Read Semantics](#read-semantics)
  - [Write-Through Semantics](#write-through-semantics)
  - [Write-Back Semantics](#write-back-semantics)
  - [Remote Publication Barriers](#remote-publication-barriers)
  - [Delete Semantics](#delete-semantics)
  - [Reconciliation](#reconciliation)
  - [Failure and Restart](#failure-and-restart)
  - [Replacing the Part Cache](#replacing-the-part-cache)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
  - [Performance and Cost](#performance-and-cost)
  - [Capacity](#capacity)
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

* [Chris Riccomini](https://github.com/criccomini)

## Summary

This RFC rewrites `CachedObjectStore` to store whole compacted SSTs. Two read
modes and two write modes are supported:

- `ReadThrough` fetches a missing range from remote storage and schedules a full
  SST download in the background.
- `LocalOnly` treats a missing local SST as an error.
- `WriteThrough` waits for each compacted SST to be durable locally and remotely
  before returning.
- `WriteBack` acknowledges the write after the SST is complete locally and
  queued for upload, but before the remote write completes.

A cache warming mechanism is also provided for `LocalOnly` readers that want to
ensure that every compacted SST is available locally on startup.

## Motivation

Recent performance testing showed that SlateDB's `CachedObjectStore` is not
useful. In fact, it did more harm than good. It occupies an area between a
best-effort cache and a full local replica, but it does neither job well.

- It splits objects into fixed-size 4MiB (default) parts. This adds latency to
  small reads and creates many files for large writes.
- A single 256MiB SST becomes 64 part files plus metadata. This creates
  eviction pressure and startup scans that rebuild the in-memory index.
- It drops admission events on writes when the evictor is overwhelmed. This can
  leave the newest SSTs uncached.
- It does not provide a mechanism to guarantee local reads for those that want a
  full local mirror of their data.
- It is not clear to users when to use `CachedObjectStore` versus `DbCache` and
  Foyer's `HybridCache`.

Foyer already gives SlateDB a better best-effort cache. `DbCache` stores decoded
data blocks, indexes, filters, and stats. A Foyer `HybridCache` can put those
entries on disk, admit only the blocks SlateDB asks for, and use a mature
eviction policy. It avoids fetching a 4 MiB object-store part to answer a 4 KiB
block read. Users can also implement a prefetching object store similar to
ZeroFS's [prefetching object store](https://github.com/Barre/ZeroFS/blob/main/zerofs/src/object_store_prefetch.rs)
if their workload has spatial locality.

For `LocalOnly` workloads, the `FoyerHybridCache` is not a good fit.

- It's best effort caching, so under load it can drop blocks.
- The in memory index is costly to build at startup and consumes high memory.
- Caching on compaction is costly because you need to break every SST into
  blocks and add them to the block cache. Locality is also lost because SST
  blocks are mixed.
- GC has more cost because you need to delete every block from Foyer as opposed
  to one file unlink.

For `WriteBack` workloads, we currently offer no solution.

## Goals

- TODO

## Non-Goals

- TODO

## Design

### GET Policy

The rewritten cache retains the per-call policy pattern from the current
`CachedObjectStore`. A `GetPolicy` receives the compacted SST's
`ObjectStoreCallTag`, then returns one of four actions:

| GET action | Local hit | Local miss |
|---|---|---|
| `Bypass` | Not checked | Forward the original GET to the wrapped store without local admission |
| `Refetch` | Not checked | Forward the requested GET and schedule a full-SST background fill after the foreground GET succeeds |
| `ReadThrough` | Serve it | Forward the requested GET and schedule a full-SST background fill after the foreground GET succeeds |
| `LocalOnly` | Serve it | Return `LocalCacheError` without accessing remote storage |

`ReadThrough` keeps the full download off the foreground path. A custom policy
can use `Bypass` for call sources such as compaction scans. `Refetch` is used
when corruption is detected. Before forwarding the GET, it deletes the
existing local file so concurrent reads cannot continue serving the corrupt
bytes. After the foreground GET succeeds, it schedules the same single-flight
full-SST fill as `ReadThrough`.

The wrapper invokes the policy only for calls carrying an
`ObjectStoreCallTag` whose `sst_type` is `Compacted`. Tagged WAL calls and
untagged calls bypass before policy dispatch. This is an implementation
invariant, so a custom policy cannot accidentally cache WALs, manifests, or
other coordination objects.

`DefaultCacheGetPolicy` handles validation retries before applying its normal
read action:

| Request | Routing |
|---|---|
| Untagged or tagged WAL | Bypass before policy dispatch |
| Tagged compacted with `tag.retry.is_some()` | `Refetch` |
| Other tagged compacted | Configured action; `ReadThrough` by default |

SlateDB reissues a compacted SST read once with `tag.retry` set after a
recoverable validation failure. Routing that call to `Refetch` is required: a
normal `ReadThrough` lookup could otherwise return the same corrupt local file
again.

A custom GET policy may use `ObjectStoreCallTag::kind` to select different
actions for `Main`, `Reader`, `Compactor`, and `GC` calls. It must not return
`ReadThrough` or `LocalOnly` for a retry-tagged call, because either action may
serve the file that failed validation.

### PUT Policy

The PUT policy returns one of two actions for a tagged compacted SST:

| PUT action | Returns after | Remote write |
|---|---|---|
| `WriteThrough` | The SST is durable locally and remotely | Foreground |
| `WriteBack` | The SST is durable locally | Background |

The wrapper invokes the PUT policy only for tagged compacted SSTs. WAL and
untagged PUTs bypass it. SlateDB writes compacted SSTs through a tagged
`BufWriter`. SSTs smaller than the writer's buffer use `put_opts`; larger SSTs
use `put_multipart_opts`. The cache applies the same PUT policy and local
durability requirements to both paths.

For `WriteBack`, `put_opts` returns after the complete local file has been
synced and queued for background upload. For multipart writes, the cache returns
a multipart implementation that writes parts to the local file without waiting
for remote part uploads. Its `complete()` returns after the complete local file
has been synced and queued; the remote multipart upload may still be in flight.
`WriteThrough` uses the same local and upload paths but waits for remote
completion before returning.

WAL SSTs and fence objects use the conditional single-PUT path with
`PutMode::Create`; they bypass the cache and remain write-through so fencing
errors are returned to the caller.

Any GET policy may be paired with any PUT policy. Both PUT actions store newly
written compacted SSTs as whole local files and use the same upload queue. A
custom PUT policy can choose `WriteThrough` or `WriteBack` based on the
`ObjectStoreCallTag` it receives.

`WriteBack` PUTs may hold a manifest or compactions PUT at the remote
publication barrier described below. This is to keep the remote manifest from
referencing an SST that has not yet been uploaded, and could be lost during a
machine failure.

### Public API

`CachedObjectStore` is a normal `ObjectStore` implementation. Users construct
it with a local cache root and remote store, then pass it through the existing
`DbBuilder::new`/`Db::builder` object-store parameter. We remove the old
`object_store_cache_options` in `config.rs` and `builder.rs`.

```rust
pub enum GetAction {
    /// Do not check the local cache; forward the original GET to the wrapped
    /// store.
    Bypass,
    /// Do not check the local cache; forward the requested GET to the wrapped
    /// store and schedule a full-SST background fill after the foreground GET
    /// succeeds.
    Refetch,
    /// Check the local cache; if the SST is present, serve it. If it is
    /// missing, forward the requested GET to the wrapped store and schedule a
    /// full-SST background fill.
    ReadThrough,
    /// Check the local cache; if the SST is present, serve it. If it is
    /// missing, return `LocalCacheError` without accessing remote storage.
    LocalOnly,
}

pub trait GetPolicy: Send + Sync + Debug + 'static {
    /// Returns the action to take for the given object store call tag.
    fn get_action(&self, tag: &ObjectStoreCallTag) -> GetAction;
}

pub enum PutAction {
    /// Write the SST locally and remotely, returning only after the remote
    /// upload completes.
    WriteThrough,
    /// Write and sync the SST locally, then complete the remote upload in the
    /// background.
    ///
    /// Local writes will remain visible to the current process until the upload
    /// completes. If the upload fails, the cache will enter a poisoned state
    /// and all subsequent writes will fail with `LocalCacheError`.
    WriteBack,
}

pub trait PutPolicy: Send + Sync + Debug + 'static {
    /// Returns the action to take for the given object store call tag.
    fn put_action(&self, tag: &ObjectStoreCallTag) -> PutAction;
}

impl CachedObjectStore {
    pub fn builder(
        local_root_folder: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
    ) -> CachedObjectStoreBuilder;
    /// Populates the cache with compacted SSTs referenced by the latest
    /// manifest.
    pub async fn warm(
        &self,
        db_root: impl Into<object_store::path::Path>,
    ) -> Result<(), Error>;
}

impl CachedObjectStoreBuilder {
    /// Sets the GET policy for tagged compacted SST reads. The default is
    /// `DefaultCacheGetPolicy::new(GetAction::ReadThrough)`.
    pub fn with_get_policy(self, policy: Arc<dyn GetPolicy>) -> Self;
    /// Sets the PUT policy for tagged compacted SST writes. The default is
    /// `DefaultCachePutPolicy::new(PutAction::WriteThrough)`.
    pub fn with_put_policy(self, policy: Arc<dyn PutPolicy>) -> Self;
    /// Sets the maximum number of concurrent background prefetches. The
    /// default is 4.
    pub fn with_prefetch_concurrency(self, concurrency: usize) -> Self;
    /// Sets the maximum number of concurrent background uploads. The default
    /// is 4.
    pub fn with_upload_concurrency(self, concurrency: usize) -> Self;
    /// Sets the interval at which the cache will reconcile its local state
    /// with the remote object store. The default is
    /// `Some(Duration::from_secs(60))`. Passing `None` disables reconciliation.
    ///
    /// Reconciliation only deletes local files that are confirmed to be
    /// missing from the remote store. It does not upload or download files.
    pub fn with_reconciliation_interval(
        self,
        interval: Option<Duration>,
    ) -> Self;
    /// Validates the configuration, prepares and cleans the local directories,
    /// and starts background workers.
    pub async fn build(self) -> Result<Arc<CachedObjectStore>, Error>;
}

#[async_trait]
impl ObjectStore for CachedObjectStore {
    // Standard ObjectStore methods delegate to local and remote storage.
}
```

A complete instantiation looks like this:

```rust
let remote: Arc<dyn ObjectStore> = Arc::new(
    AmazonS3Builder::from_env()
        .with_bucket_name("my-bucket")
        .build()?,
);
let cache = CachedObjectStore::builder("/var/lib/slatedb/cache", remote)
    .with_get_policy(Arc::new(DefaultCacheGetPolicy::new(
        GetAction::ReadThrough,
    )))
    .with_put_policy(Arc::new(DefaultCachePutPolicy::new(
        PutAction::WriteThrough,
    )))
    .with_prefetch_concurrency(4)
    .with_upload_concurrency(4)
    .with_reconciliation_interval(Some(Duration::from_secs(60)))
    .build()
    .await?;
cache.warm("my-db").await?;
let db = Db::builder(db_path, cache).build().await?;
```

### Architecture

The user supplies the cache as the main object store. SlateDB then applies its
existing internal wrappers. The base-to-outer construction order is:

```text
S3ObjectStore -> CachedObjectStore -> InstrumentedObjectStore -> RetryingObjectStore
```

Requests travel in the opposite direction:

```text
RetryingObjectStore -> InstrumentedObjectStore -> CachedObjectStore -> S3ObjectStore
```

The cache owns retries and metrics for background fills, uploads, and
reconciliation because the `RetryingObjectStore` wrapper is outside the cache.

`LocalCacheError` is wrapped in `object_store::Error::Generic` with
`store: "cached_object_store"`. SlateDB's `RetryingObjectStore::should_retry`
must downcast the generic error's source and return `false` for
`LocalCacheError`. Without this explicit check, the current retry classifier
would retry the generic error indefinitely when `object_store_max_retries` is
unset.

WAL operations, reads and writes of manifests, compactions records, and GC
boundaries, along with LIST, untagged HEAD, copy, and other untagged operations,
always go to the wrapped remote store.

Because the wrapper is supplied through the normal `ObjectStore` parameter,
the same type works with `Db`, `DbReader`, and `Compactor`. A cache local
filesystem root belongs to one live `CachedObjectStore`. Separate wrappers,
including wrappers in separate processes, must use separate local filesystem
roots.

### Filesystem Layout

Each cached SST maps to one local file. There are two folders under the cache
root:

- `objects/` contains complete local SSTs. Each file is named with its full
  object-store path relative to `objects/`.
- `uploading/` files that are still being written to disk or uploaded to the
  remote store.

```text
<cache-root>/
  objects/<full-object-store-path>
  uploading/<filename>
  downloading/<full-object-store-path>
```

For example:

```text
<cache-root>/
  objects/tenant1/foo/compacted/AB12C01M05WQZSVG1YFZECTMTTTA3EE.sst
  objects/tenant2/foo/compacted/01M05WR6EZ6ZF44TGFNN5HFTDD.sst
  uploading/tenant1/foo/compacted/01M05WR997G22470E93PBPVAA2.sst
  downloading/tenant1/foo/compacted/01M05WRE0MG8EZY9HJEY36JE4B.sst
```

The cache uses the canonical `object_store::path::Path` directly as a relative
path below `objects/`. It appends the path's existing components without
percent-decoding or otherwise transforming them. `object_store::path::Path` has
no leading or trailing separator, empty component, `.` or `..` component, or
ASCII control character, so a valid path is already relative and cannot
lexically escape `objects/`. External SSTs arrive with their source database's
full object-store path, so they naturally group below that source database path.

No body length, checksum, attributes, or other metadata are stored locally.
Files are byte-for-byte identical with their remote counterparts.

### Read Semantics

The flow for a read is:

1. If the SST is present under `objects/`, serve it.
2. If the SST is missing..
  - `ReadThrough`: forward the requested GET to the wrapped store and schedule a
    full-SST background fill.
  - `LocalOnly`: return `LocalCacheError` without accessing remote storage.

`GetOptions` preconditions pass through to the wrapped store. A tagged
`get_opts` call with `head = true` similarly passes through to the wrapped
store.

On a read-through miss, the cache forwards the call to the wrapped store. The
requested range is returned directly to the caller; the foreground request does
not wait for a full-SST download. In parallel, the cache schedules a separate
full-SST GET. That download writes to `downloading/` and atomically renames the
complete SST into `objects/` after the download completes.

The background full-SST fill is single-flight by SST path. Concurrent
foreground misses still forward their original GETs to the wrapped store and
return their requested ranges independently. Only redundant scheduling of the
background full-SST download is coalesced.

If a background download fails, the foreground read is unaffected. The cache
records the failure, removes the partially downloaded file, and tries again on
a later miss.

`LocalOnly` reads return `LocalCacheError` on a local miss. The caller is
responsible for populating the cache before database instantiation using
`warm` or a custom filesystem copy.

### Write Semantics

The flow for either a single-PUT or multipart write is:

1. Write the SST under `uploading/`. A `WriteBack` multipart write does not wait
   on remote I/O on the caller's part-write path.
2. Call `sync_all` on the complete local file, register its path and operation
   sequence in the pending-upload map, atomically rename it into `objects/`, and
   sync the destination parent directory and any newly created ancestors. This
   is the local durability point.
3. Upload the complete local file to the remote store. The upload worker chooses
   single-PUT or multipart upload as appropriate and owns remote retries.

`WriteBack` returns after step 2, so the local file and its directory entry are
durable before the write is acknowledged while the remote upload may still be
in flight. `WriteThrough` waits for step 3. In both cases, the path remains
registered as pending until the remote upload completes.

A `WriteBack` call returns a `PutResult` without a remote ETag or version because
those fields do not exist until the background upload completes. Current tagged
compacted SST writers discard the `PutResult`; other calls bypass the PUT policy.

`WriteBack` can move local files into `objects/` before the remote write
completes. Before that rename, the cache registers the object path and its
operation sequence in an in-memory pending-upload map. Reads may serve the
complete local file, but reconciliation excludes every path present in this
map. The path remains pending until the remote upload succeeds. Only then does
the cache remove it from the map and resolve its sequence.

To prevent corruption, the cache blocks publication of any manifest or
compactions record until every earlier write-back has completed remotely. A
permanent upload failure poisons the queue and leaves the local file marked
pending for the rest of the process lifetime. The file is never referenced by
published metadata; after a restart, reconciliation may remove it if it is
absent remotely.

TODO: We should really move retries into `object_store`. Hussein mentioned this.

`CachedObjectStore` wraps its internal object store in a `RetryingObjectStore`
for its remote writes. If the upload fails, write-through returns the remote
error, allowing SlateDB's outer retry wrapper to retry the PUT. If local
promotion fails, the wrapper returns `LocalCacheError`.

Tagged WAL and untagged writes pass through to remote storage. Manifest,
compactions, WAL, and GC boundary PUTs therefore retain their existing
conditional-write and fencing behavior.

### Delete Semantics

Delete operations pass through to the wrapped store. Local files in `objects/`
that match the deleted path are removed if they exist. Local and remote
deletions are done in parallel, and a failure in one does not affect the other.

Deletions on an in-flight upload are dropped and file-not-exists is returned.

### Remote Publication Barriers

Write-back must flush a compacted SST to object storage before updating a remote
manifest that can reference it. A manifest that points at a missing SST is
is corrupt.

Every tagged compacted SST write receives an operation sequence. Before the
wrapper forwards a PUT for a `.manifest` or `.compactions` object, it captures
a barrier and waits until all earlier compacted SST PUTs have completed
remotely:

```text
seq 41: compacted/A.sst ----+
seq 42: compacted/B.sst ----+--> remote durable
seq 43: manifest/9          +--> conditional PUT may begin
seq 44: compacted/C.sst     ----> may remain queued
```

A successful sequence is complete only after the remote upload succeeds. The
complete local file may already be visible under `objects/`, but its path
remains in the pending-upload map until remote completion. A failed
write-through sequence may be retired after its waiting caller receives the
failure, because no metadata operation can reference a write that did not
return success. A failed write-back sequence cannot be retired because its
caller was already acknowledged; it poisons the queue instead. Because a
metadata PUT waits for every earlier sequence to complete or retire, a pending
local file can never be referenced by published remote metadata.

The barrier is conservative. It may wait for an unreferenced compaction output,
but it never needs to decode a manifest in the object-store wrapper. The extra
wait is preferable to duplicating manifest interpretation in the write queue.

### Reconciliation

A DELETE issued on another machine--such as a remote garbage collector--does
not pass through this wrapper. By
default, `CachedObjectStore` therefore runs a reconciliation task every minute
regardless of GET policy. `Some(interval)` enables the task and must be
non-zero; `None` disables it. Dropping the wrapper cancels its task without
delaying `Db::close()`.

One reconciliation pass proceeds as follows:

- Snapshot all files in `objects/`, excluding every path currently registered
  in the pending-upload map. A path added after the snapshot is not a candidate
  in the current pass.
- Group them by their remote parent prefix.
- LIST each distinct parent prefix on the wrapped remote store.
- Delete any local file whose path is absent from the remote LIST result.

This will require one LIST per database prefix. A DB with no external DBs will
have a single prefix. One additional LIST will be issued for each external DB
prefix.

### Construction and Cleanup

`CachedObjectStoreBuilder::build` validates its options, prepares the local
directories, and removes files under `uploading/` and `downloading/` before
starting background work. It returns an error if any of this setup fails.
Deleting those incomplete files is safe. The cache does not forward a manifest
or compactions PUT until every earlier asynchronous SST upload has completed
remotely. A local SST left in `objects/` by an acknowledged write-back loses its
pending-map entry after a crash, but it cannot be referenced by published remote
metadata unless its remote upload completed. Reconciliation preserves it if it
exists remotely and removes it otherwise.

The built-in `ReadThrough` policy can start with an empty directory and populate
it on demand. `LocalOnly` assumes required SSTs were written through this cache
or populated explicitly using `warm` or via a filesystem copy.

`Db::close()` requires no cache-specific follow-up. Its default close options
perform a final memtable flush and publish the resulting metadata. The cache's
publication barrier makes that metadata PUT wait for every earlier queued SST,
so close returns only after every SST referenced by the closed database state
has reached remote storage. `close_with_options()` can disable the final flush;
that form does not provide this guarantee. Any later, unpublished compaction
output may still be discarded after a crash.

### Cache Warming

There are three ways to populate the cache:

1. Any successful tagged compacted SST write installs the complete local SST.
2. A tagged compacted `ReadThrough` miss schedules a full-SST background fill.
3. An application can call `warm` to populate the compacted SSTs in a
   database's latest manifest and wait. This must be done before database
   instantiation if the caller wants to avoid `LocalCacheError` on a `LocalOnly`
   read.

Calling `warm` is optional. The utility has no options: it warms every
compacted SST in the latest manifest, uses the cache's existing remote-read
concurrency and retry policy, and returns an error if any SST cannot be
populated. Callers that do not need eager population omit the call.

### Prefix Store Support

When the cache wraps a `PrefixStore`, `db_root` is relative to the prefix
store's logical namespace. For example, warming `foo` through a prefix store
rooted at `tenant1` reads `foo/manifest/...` through the wrapper, which maps to
`tenant1/foo/manifest/...` in the underlying bucket. The cache stores
`objects/foo/compacted/...`; it does not include the hidden prefix. External
SST paths must be reachable through the same wrapped namespace, as they must be
for normal database reads.

### Failure and Restart

TODO

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that
apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

`LocalOnly` adds explicit missing-object errors.

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
- [x] Block cache
- [x] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

The block cache remains the general-purpose best-effort cache. The cache stores
only tagged compacted SSTs as whole local files. WAL SSTs always use remote
storage. `ReadThrough` population is best effort. Applications using
`LocalOnly` may populate the latest manifest explicitly with `warm`.

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

Bindings lose the old cache settings. The cache is available only in APIs that
can supply a custom `ObjectStore`; this RFC does not add binding-specific
configuration or CLI commands.

## Operations

### Performance and Cost

Local hits perform range reads from one whole SST file. A read-through miss
issues the requested range GET on the foreground path and a separate full-SST
GET in the background. Foreground latency waits only for the requested range,
while remote bandwidth includes both requests. Later reads use the local file
after the background download finishes.

`LocalOnly` performs no remote fallback for tagged compacted SST reads.
`warm` may still read remote storage explicitly. WAL and untagged
coordination reads always use remote storage.

`WriteThrough` adds local disk bandwidth to every tagged compacted SST write and
waits for the shared queue's upload result. `WriteBack` moves most remote
latency off compacted SST writes, but manifest and compactions publication still
wait at a remote barrier. WAL writes remain on the foreground remote path.

Whole-SST storage removes the 4 MiB miss amplification, part metadata,
per-part file handles, and per-part eviction work. It can increase local bytes
relative to a hot-block cache because completeness is the contract.

With `ReadThrough`, warmup cost is highest because a miss produces a range GET
plus a full GET. `LocalOnly` issues no remote fallback GET.

One `warm` call reads the latest manifest and issues one full-object GET
for each referenced SST not already local. It may therefore transfer the full
live compacted data set when starting with an empty cache. Existing local SSTs
avoid those GETs. The utility shares the cache's remote-read concurrency with
read-through fills and returns only after every SST in its manifest snapshot is
installed or one fails.

Each reconciliation pass scans complete local objects that are not registered
as pending uploads, issues one remote LIST per distinct remote parent prefix
represented locally, and issues HEAD only for paths missing from a LIST result.
Caches randomize their initial delays to spread this work across the fleet.
Reconciliation never runs on the foreground read or write path.

At the default one-minute interval, and using the S3 Standard rate of $0.005 per
1,000 PUT, COPY, POST, or LIST requests, one listed prefix produces 43,200 LIST
requests in a 30-day month and costs $0.216 per cache per month. With `P`
distinct parent prefixes and `M` caches, the monthly LIST cost is
`$0.216 * P * M`.
Confirmatory HEAD requests for candidates missing from LIST, data transfer, and
provider-specific charges are additional. Setting the reconciliation interval
to `None` eliminates this periodic LIST cost.

### Capacity

The cache has no maximum-size eviction setting in this proposal. Under
`ReadThrough`, the cache may remain partially populated if a fill runs out of
space; the foreground read still succeeds remotely. `LocalOnly`
cannot discard an SST and preserve its no-fallback contract.

Operators size the volume for cached SSTs, temporary files under `uploading/`
and `downloading/`, and the write-back backlog. The backlog has no separate byte
limit, so disk capacity is the admission limit. Abandoned temporary files
consume space only until the next wrapper construction clears those
directories. Complete SSTs deleted remotely on another machine may consume
local space until the next successful reconciliation pass. If reconciliation
is disabled, they remain until a DELETE passes through the cache or the root is
rebuilt.

### Observability

TODO

### Compatibility

TODO

## Testing

TODO

## Rollout

TODO

## Alternatives

TODO

## Open Questions

TODO

## References

- [Issue #1980: Remove `CachedObjectStore`](https://github.com/slatedb/slatedb/issues/1980)
- [RFC 0023: Targeted Cache Warming and Best-Effort Block Cache Eviction](0023-cache-manager.md)
- [RFC 0026: Garbage Collector Boundary](0026-garbage-collector-boundary.md)
- [RFC 0027: Decoupled Pluggable Object Store Cache](0027-decoupled-object-store-cache.md)
- [RFC 0031: Block Cache Policy](0031-block-cache-policy.md)
- [Foyer `HybridCache`](https://docs.rs/foyer/latest/foyer/struct.HybridCache.html)
- [ZeroFS prefetching object store](https://github.com/Barre/ZeroFS/blob/main/zerofs/src/object_store_prefetch.rs)
- [Discord discussion](https://discord.com/channels/1232385660460204122/1531345817246634216)

## Updates

- Reframed the proposal as a rewrite of `CachedObjectStore`.
- Replaced GET-extension-based object population with the public
  manifest-aware `warm_cache` utility. `CacheManager` remains block-cache-only.
- Replaced fixed read and write modes with cache-specific GET and PUT policies
  and documented their dispatch paths.
- Added optional periodic remote reconciliation, enabled at a one-minute
  interval by default, while retaining DELETE passthrough as the immediate local
  cleanup path.
- Initial draft.
