# Add ObjectStoreCache

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [GET Policy](#get-policy)
  - [Public API](#public-api)
  - [Architecture](#architecture)
  - [Filesystem Layout](#filesystem-layout)
  - [Read Semantics](#read-semantics)
  - [Write Semantics](#write-semantics)
  - [Delete Semantics](#delete-semantics)
  - [Reconciliation](#reconciliation)
  - [Construction and Cleanup](#construction-and-cleanup)
  - [Cache Warming](#cache-warming)
  - [Prefix Store Support](#prefix-store-support)
  - [Failure and Restart](#failure-and-restart)
- [Impact Analysis](#impact-analysis)
  - [Core API & Query Semantics](#core-api-query-semantics)
  - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
  - [Time, Retention, and Derived State](#time-retention-and-derived-state)
  - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
  - [Compaction](#compaction)
  - [Storage Engine Internals](#storage-engine-internals)
  - [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
  - [Performance and Cost](#performance-and-cost)
  - [Capacity](#capacity)
  - [Observability](#observability)
  - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
  - [Write-Back](#write-back)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

This RFC adds `ObjectStoreCache`, a whole-file local mirror for compacted SSTs.
It is separate from the existing part-based `CachedObjectStore`. Compacted SST
reads are `LocalOnly` by default, so a missing local SST is an error. `Refetch`
repairs a local SST after a validation failure.

Compacted SST writes are mirrored to local and remote storage and return after
both copies are durable.

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

Rather than change the existing cache in place, this RFC adds
`ObjectStoreCache` under `slatedb/src/object_store_cache`. The name follows
`DbCache`; `CachedObjectStore` remains unchanged and can be removed separately.

## Goals

- Add a whole-SST local mirror for compacted SSTs.
- Support local-only reads, write-through mirroring, and cache warming.
- Make the new cache additive so existing users are unaffected.

## Non-Goals

- Removing or changing `CachedObjectStore`, its configuration, or its bindings.
- Caching WALs, manifests, or other coordination objects.
- Providing size-based eviction or a best-effort block cache.

## Design

### GET Policy

The new cache retains the per-call policy pattern from the current
`CachedObjectStore`. A `GetPolicy` receives the compacted SST's
`ObjectStoreCallTag`, then returns one of two actions:

| GET action | Local hit | Local miss |
|---|---|---|
| `Refetch` | Not checked | Forward the requested GET and schedule a full-SST background fill after the foreground GET succeeds |
| `LocalOnly` | Serve it | Return `LocalCacheError` without accessing remote storage |

`Refetch` is used when corruption is detected. Before forwarding the GET, it
deletes the existing local file so concurrent reads cannot continue serving the
corrupt bytes. After the foreground GET succeeds, it schedules a single-flight
full-SST download to repair the local copy.

The wrapper invokes the policy only for calls carrying an
`ObjectStoreCallTag` whose `sst_type` is `Compacted`. Tagged WAL calls and
untagged calls go directly to the wrapped store before policy dispatch. This
implementation invariant prevents a custom policy from caching WALs, manifests,
or other coordination objects.

`DefaultCacheGetPolicy` handles validation retries before applying its normal
read action:

| Request | Routing |
|---|---|
| Untagged or tagged WAL | Forward to the wrapped store before policy dispatch |
| Tagged compacted with `tag.retry.is_some()` | `Refetch` |
| Other tagged compacted | Configured action; `LocalOnly` by default |

SlateDB reissues a compacted SST read once with `tag.retry` set after a
recoverable validation failure. Routing that call to `Refetch` is required: a
`LocalOnly` lookup could otherwise return the same corrupt local file again.

A custom GET policy may use `ObjectStoreCallTag::kind` to select different
actions for `Main`, `Reader`, `Compactor`, and `GC` calls. It must return
`Refetch` for a retry-tagged call so the failed local file is not served again.

### Public API

`ObjectStoreCache` is a normal `ObjectStore` implementation in
`slatedb/src/object_store_cache`. Users construct it with a local cache root and
remote store, then pass it through the existing `DbBuilder::new`/`Db::builder`
object-store parameter. The existing `CachedObjectStore` and
`object_store_cache_options` remain unchanged.

```rust
pub enum GetAction {
    /// Do not check the local cache; forward the requested GET to the wrapped
    /// store and schedule a full-SST background fill after the foreground GET
    /// succeeds.
    Refetch,
    /// Check the local cache; if the SST is present, serve it. If it is
    /// missing, return `LocalCacheError` without accessing remote storage.
    LocalOnly,
}

pub trait GetPolicy: Send + Sync + Debug + 'static {
    /// Returns the action to take for the given object store call tag.
    fn get_action(&self, tag: &ObjectStoreCallTag) -> GetAction;
}

impl ObjectStoreCache {
    pub fn builder(
        local_root_folder: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
    ) -> ObjectStoreCacheBuilder;
    /// Populates the cache with compacted SSTs referenced by the latest
    /// manifest.
    pub async fn warm(
        &self,
        db_root: impl Into<object_store::path::Path>,
    ) -> Result<(), Error>;
}

impl ObjectStoreCacheBuilder {
    /// Sets the GET policy for tagged compacted SST reads. The default is
    /// `DefaultCacheGetPolicy::new(GetAction::LocalOnly)`.
    pub fn with_get_policy(self, policy: Arc<dyn GetPolicy>) -> Self;
    /// Sets the maximum number of concurrent downloads used by `warm` and
    /// background refetches. The default is 4.
    pub fn with_download_concurrency(self, concurrency: usize) -> Self;
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
    pub async fn build(self) -> Result<Arc<ObjectStoreCache>, Error>;
}

#[async_trait]
impl ObjectStore for ObjectStoreCache {
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
let cache = ObjectStoreCache::builder("/var/lib/slatedb/cache", remote)
    .with_download_concurrency(4)
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
S3ObjectStore -> ObjectStoreCache -> InstrumentedObjectStore -> RetryingObjectStore
```

Requests travel in the opposite direction:

```text
RetryingObjectStore -> InstrumentedObjectStore -> ObjectStoreCache -> S3ObjectStore
```

The cache owns retries and metrics for background refetches, warming, and
reconciliation because the `RetryingObjectStore` wrapper is outside the cache.

`LocalCacheError` is wrapped in `object_store::Error::Generic` with
`store: "object_store_cache"`. SlateDB's `RetryingObjectStore::should_retry`
must downcast the generic error's source and return `false` for
`LocalCacheError`. Without this explicit check, the current retry classifier
would retry the generic error indefinitely when `object_store_max_retries` is
unset.

WAL operations, reads and writes of manifests, compactions records, and GC
boundaries, along with LIST, untagged HEAD, copy, and other untagged operations,
always go to the wrapped remote store.

Because the wrapper is supplied through the normal `ObjectStore` parameter,
the same type works with `Db`, `DbReader`, and `Compactor`. A cache local
filesystem root belongs to one live `ObjectStoreCache`. Separate wrappers,
including wrappers in separate processes, must use separate local filesystem
roots.

### Filesystem Layout

Each cached SST maps to one local file. There are two folders under the cache
root:

- `objects/` contains complete local SSTs. Each file is named with its full
  object-store path relative to `objects/`.
- `uploading/` contains incomplete local SST writes.

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

For a normal tagged compacted SST read, the cache serves the file from
`objects/`. If the file is missing, it returns `LocalCacheError` without
accessing remote storage.

A tagged `get_opts` call with `head = true` goes directly to the wrapped store.

For `Refetch`, the requested range is returned directly from the wrapped store.
The foreground request does not wait for the separate full-SST download, which
writes to `downloading/` and atomically renames the complete SST into `objects/`.
Full-SST refetches are single-flight by SST path. If the download fails, the
foreground retry is unaffected; the cache records the failure and removes the
partial file.

The caller must populate the cache before database instantiation using `warm`
or by manually copying or downloading complete SSTs into `objects/`.

### Write Semantics

Tagged compacted SST writes are write-through. Single-PUT and multipart writes
tee bytes to the remote store and a temporary file under `uploading/`. Multipart
parts continue streaming remotely as they are produced.

The write returns only after the remote upload succeeds, the complete local file
is synced, and the file is atomically renamed into `objects/` with its parent
directories synced. A remote failure removes the temporary file and returns the
remote error. A local failure returns `LocalCacheError`; a remote SST that
already completed is left unreferenced for garbage collection.

Tagged WAL and untagged writes pass through to remote storage. Manifest,
compactions, WAL, and GC boundary PUTs retain their existing conditional-write,
fencing, and publication ordering.

### Delete Semantics

Delete operations pass through to the wrapped store. Local files in `objects/`
that match the deleted path are removed if they exist. Local and remote
deletions are done in parallel, and a failure in one does not affect the other.

### Reconciliation

A DELETE issued on another machine--such as a remote garbage collector--does
not pass through this wrapper. By
default, `ObjectStoreCache` therefore runs a reconciliation task every minute
regardless of GET policy. `Some(interval)` enables the task and must be
non-zero; `None` disables it. Dropping the wrapper cancels its task without
delaying `Db::close()`.

> [!IMPORTANT]
> This design requires a bucket and endpoint with strongly consistent object
> reads, writes, deletes, and listings. Reconciliation treats confirmed remote
> absence as authoritative and may delete the only local copy, so it must be
> disabled when these guarantees are unavailable. In particular:
>
> - Tigris global and dual-region buckets are strongly consistent for requests
>   within one region but eventually consistent across regions. All writers and
>   reconciling caches must access such a bucket from the same region; Tigris
>   multi-region and single-region buckets provide strong consistency globally.
> - Azure RA-GRS and RA-GZRS secondary endpoints are eventually consistent with
>   the primary. Reconciliation must use the primary endpoint and remain
>   disabled while reads are directed to a secondary endpoint.

One reconciliation pass proceeds as follows:

- Snapshot all files in `objects/`.
- Group them by their remote parent prefix.
- LIST each distinct parent prefix on the wrapped remote store.
- Delete any local file whose path is absent from the remote LIST result.

This will require one LIST per database prefix. A DB with no external DBs will
have a single prefix. One additional LIST will be issued for each external DB
prefix.

### Construction and Cleanup

`ObjectStoreCacheBuilder::build` validates its options, prepares the local
directories, and removes files under `uploading/` and `downloading/` before
starting background work. It returns an error if any of this setup fails.
Deleting those incomplete files is safe because files enter `objects/` only
after the corresponding remote upload succeeds.

Before database instantiation, required SSTs must already have been written
through this cache or populated using `warm` or a manual filesystem copy.

`Db::close()` requires no cache-specific follow-up. Existing metadata
publication ordering applies because each compacted SST PUT returns only after
the SST is durable remotely.

### Cache Warming

There are three ways to populate the cache:

1. Any successful tagged compacted SST write installs the complete local SST.
2. An application can call `warm` to populate the compacted SSTs in a
   database's latest manifest and wait. This must be done before database
   instantiation if the caller wants to avoid `LocalCacheError` on a `LocalOnly`
   read.
3. An operator can manually copy or download complete SSTs into `objects/`.

Calling `warm` is optional only when the required SSTs are already local. The
utility has no options: it warms every compacted SST in the latest manifest,
uses the cache's existing remote-read concurrency and retry policy, and returns
an error if any SST cannot be populated.

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
storage. Applications must populate the latest manifest explicitly with `warm`
or manually copy or download the required SSTs.

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

Existing binding cache settings continue to configure `CachedObjectStore`.
`ObjectStoreCache` is initially available only in APIs that can supply a custom
`ObjectStore`; this RFC does not add binding-specific configuration or CLI
commands.

## Operations

### Performance and Cost

Local hits perform range reads from one whole SST file. A local miss returns
`LocalCacheError` without remote fallback. `Refetch` validation retries and
`warm` may read remote storage explicitly. WAL and untagged coordination reads
always use remote storage.

Compacted SST writes stream to local and remote storage concurrently and return
after both copies are durable. WAL writes remain on the remote path.

One `warm` call reads the latest manifest and issues one full-object GET
for each referenced SST not already local. It may therefore transfer the full
live compacted data set when starting with an empty cache. Existing local SSTs
avoid those GETs. The utility shares the cache's remote-read concurrency with
background refetches and returns only after every SST in its manifest snapshot
is installed or one fails.

Each reconciliation pass scans complete local objects, issues one remote LIST
per distinct remote parent prefix represented locally, and issues DELETE only
for paths missing from a LIST result.

At the default one-minute interval, and using the S3 Standard rate of $0.005 per
1,000 PUT, COPY, POST, or LIST requests, one listed prefix produces 43,200 LIST
requests in a 30-day month and costs $0.216 per cache per month. With `P`
distinct parent prefixes and `M` caches, the monthly LIST cost is
`$0.216 * P * M`. Setting the reconciliation interval to `None` eliminates this
periodic LIST cost.

### Capacity

The cache has no maximum-size eviction setting in this proposal. It cannot
discard an SST and preserve its `LocalOnly` contract, so operators must size the
volume for the required working set.

Operators size the volume for cached SSTs and temporary files under `uploading/`
and `downloading/`. Abandoned temporary files consume space only until the next
wrapper construction clears those directories. Complete SSTs deleted remotely
on another machine may consume local space until the next successful
reconciliation pass. If reconciliation is disabled, they remain until a DELETE
passes through the cache or the root is rebuilt.

### Observability

TODO

### Compatibility

This change is additive. `CachedObjectStore`, its module, configuration, and
bindings remain unchanged. `ObjectStoreCache` is a new public type under
`slatedb/src/object_store_cache`.

## Testing

TODO

## Rollout

TODO

## Alternatives

### Write-Back

We considered acknowledging compacted SST writes after the local copy became
durable and uploading them remotely in the background. Manifest and
`.compactions` publication would still have to wait for remote durability, while
SlateDB already parallelizes L0 flushes, multipart uploads, compactions, and
subcompactions. The limited benefit did not justify an upload queue, publication
barriers, and additional failure handling.

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

- Made `LocalOnly` the default and cache population explicit.
- Made the proposal additive by introducing `ObjectStoreCache` alongside
  `CachedObjectStore`.
- Dropped write-back after comparing its publication barrier with SlateDB's
  existing upload parallelism.
- Initial draft.
