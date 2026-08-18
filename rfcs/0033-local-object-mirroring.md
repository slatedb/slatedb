# Add ObjectStoreMirror

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [GET Routing](#get-routing)
  - [Public API](#public-api)
  - [Virtual Filesystem](#virtual-filesystem)
  - [Architecture](#architecture)
  - [Filesystem Layout](#filesystem-layout)
  - [Read Semantics](#read-semantics)
  - [Write Semantics](#write-semantics)
  - [Delete Semantics](#delete-semantics)
  - [Reconciliation](#reconciliation)
  - [Compactor Checkpoint Retention](#compactor-checkpoint-retention)
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
  - [Remote-Only Reconciliation](#remote-only-reconciliation)
  - [GC-Rule Reconciliation](#gc-rule-reconciliation)
  - [Write-Back](#write-back)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

This RFC adds `ObjectStoreMirror`, a whole-file local mirror for compacted SSTs.
It is separate from the existing part-based `CachedObjectStore`. Compacted SST
reads are `LocalOnly` by default, so a missing local SST is an error. `Refetch`
repairs a local SST after a validation failure.

Compacted SST writes are mirrored to local and remote storage and return after
the remote write succeeds and the local file is installed.

A cache warming mechanism is also provided for `LocalOnly` readers that want to
ensure that every compacted SST is available locally on startup.

Normal compaction churn is reclaimed from manifest and `.compactions`
transitions. A periodic remote scan removes files missed because of restart,
cross-process updates, or SSTs that never reached metadata.

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
`ObjectStoreMirror` under `slatedb/src/object_store_mirror`. The name
distinguishes the local mirror from `DbCache`; `CachedObjectStore` remains
available for one deprecation cycle.

## Goals

- Add a whole-SST local mirror for compacted SSTs.
- Support local-only reads, write-through mirroring, and cache warming.
- Reclaim obsolete local SSTs promptly without duplicating GC eligibility.
- Make the new cache additive so existing users are unaffected.

## Non-Goals

- Changing the runtime behavior of `CachedObjectStore`.
- Caching WALs, manifests, or other coordination objects.
- Providing size-based eviction, write-back, or a best-effort caching.

## Design

### GET Routing

GET routing is fixed by the call's `ObjectStoreCallTag`:

| Request | Routing |
|---|---|
| Untagged or tagged WAL | Forward to the wrapped store |
| Tagged compacted with `head = true` | Forward to the wrapped store |
| Tagged compacted with `tag.retry.is_some()` | `Refetch` |
| Other tagged compacted | `LocalOnly` |

SlateDB reissues a compacted SST read once with `tag.retry` set after a
recoverable validation failure. The mirror checks `tag.retry.is_some()`
directly. For `Refetch`, it deletes the existing local file, forwards the GET to
the wrapped store, and schedules a single-flight full-SST download to repair the
local copy. All other tagged compacted SST reads are local-only.

### Public API

`ObjectStoreMirror` is a normal `ObjectStore` implementation in
`slatedb/src/object_store_mirror`. Users construct it with a local cache root and
remote store, then pass it through the existing `DbBuilder::new`/`Db::builder`
object-store parameter. The existing `CachedObjectStore` and
`object_store_cache_options` remain available in this release.

```rust
impl ObjectStoreMirror {
    pub fn builder(
        local_root_folder: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
    ) -> ObjectStoreMirrorBuilder;
    /// Populates the cache with compacted SSTs referenced by the latest
    /// manifest.
    pub async fn warm(
        &self,
        db_root: impl Into<object_store::path::Path>,
    ) -> Result<(), Error>;
}

impl ObjectStoreMirrorBuilder {
    /// Sets the virtual filesystem used for local I/O. The default is
    /// `StdVfs`.
    pub fn with_vfs(self, vfs: Arc<dyn Vfs>) -> Self;
    /// Sets the maximum number of concurrent downloads used by `warm` and
    /// background refetches. The default is 4.
    pub fn with_download_concurrency(self, concurrency: usize) -> Self;
    /// Sets the interval at which the cache will reconcile its local state
    /// with the remote object store. The default is
    /// `Some(Duration::from_secs(600))`. Passing `None` disables periodic
    /// reconciliation but not metadata-driven reclamation.
    ///
    /// Periodic reconciliation only deletes local files confirmed missing
    /// from remote storage. Metadata-driven reclamation remains enabled.
    pub fn with_reconciliation_interval(
        self,
        interval: Option<Duration>,
    ) -> Self;
    /// Validates the configuration, prepares and cleans the local directories,
    /// and starts background workers.
    pub async fn build(self) -> Result<Arc<ObjectStoreMirror>, Error>;
}

#[async_trait]
impl ObjectStore for ObjectStoreMirror {
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
let cache = ObjectStoreMirror::builder("/var/lib/slatedb/cache", remote)
    .with_download_concurrency(4)
    .with_reconciliation_interval(Some(Duration::from_secs(600)))
    .build()
    .await?;
cache.warm("my-db").await?;
let db = Db::builder(db_path, cache).build().await?;
```

### Virtual Filesystem

`ObjectStoreMirror` performs all local I/O through a small asynchronous `Vfs`
trait. The interface is limited to the operations the mirror needs: range
reads, streamed temporary-file writes, directory creation, rename, remove, and
listing. `ObjectStoreMirrorBuilder::with_vfs` replaces the default
implementation.

The design supports three implementations:

1. `StdVfs`, the default implementation based on standard filesystem I/O.
2. `IoUringVfs`, a future Linux implementation based on io_uring.
3. `SimulatedVfs`, a deterministic implementation for simulation tests.

The VFS does not provide caching, eviction, or object-store semantics. It only
abstracts the local filesystem operations used by the mirror.

### Architecture

The user supplies the cache as the main object store. SlateDB then applies its
existing internal wrappers. The base-to-outer construction order is:

```text
S3ObjectStore -> ObjectStoreMirror -> InstrumentedObjectStore -> RetryingObjectStore
```

Requests travel in the opposite direction:

```text
RetryingObjectStore -> InstrumentedObjectStore -> ObjectStoreMirror -> S3ObjectStore
```

The cache owns retries and metrics for background refetches, warming, and
reconciliation because the `RetryingObjectStore` wrapper is outside the cache.

`LocalCacheError` is wrapped in `object_store::Error::Generic` with
`store: "object_store_mirror"`. SlateDB's `RetryingObjectStore::should_retry`
must downcast the generic error's source and return `false` for
`LocalCacheError`. Without this explicit check, the current retry classifier
would retry the generic error indefinitely when `object_store_max_retries` is
unset.

WAL operations, reads and writes of manifests, compactions records, and GC
boundaries, along with LIST, untagged HEAD, copy, and other untagged operations,
always go to the wrapped remote store.

The mirror observes successful manifest and `.compactions` reads and writes to
maintain in-memory reference state. These objects are never served from the
mirror; the state is used only for local reclamation.

Because the wrapper is supplied through the normal `ObjectStore` parameter,
the same type works with `Db`, `DbReader`, and `Compactor`. A cache local
filesystem root belongs to one live `ObjectStoreMirror`. Separate wrappers,
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

The write returns only after the remote upload succeeds and the complete local
file is atomically renamed into `objects/`. A remote failure removes the
temporary file and returns the remote error. A local failure returns
`LocalCacheError`; a remote SST that already completed is left unreferenced for
garbage collection. The local copy is recoverable cache state; validation
failures use `Refetch` to replace it.

Tagged WAL and untagged writes pass through to remote storage. Manifest,
compactions, WAL, and GC boundary PUTs retain their existing conditional-write,
fencing, and publication ordering.

### Delete Semantics

Delete operations pass through to the wrapped store. Local files in `objects/`
that match the deleted path are removed if they exist. Local and remote
deletions are done in parallel, and a failure in one does not affect the other.

### Reconciliation

Reconciliation is the process of removing local SSTs that are no longer
referenced in the database. `ObjectStoreMirror` has two phases: an immediate
metadata-driven phase and a periodic exhaustive remote scan.

The optimistic approach is required to keep disk usage low in high-throughput
workloads. Without an active deletion mechanism, even a minute of writes and
compaction churn can generate hundreds of hundreds of abandoned data. This is
not a concern for object storage, but is for local disks.

Pessimistic reconciliation is required to handle the case where a local SST is
written successfully and then lost before it is recorded in `.compactions` or a
manifest. This can be caused by a crash, a process restart, a failed compaction
job with SST output that never reached the `.compactions` file, and so on. These
are rare cases, but they can leave a local SST that is no longer referenced and
should be deleted. Pessimistic reconciliation is effectively a cheap way to
copy the garbage collector's logic without running it in two places.

#### Optimistic Reconciliation

`ObjectStoreMirror` uses `.manifest` transitions to detect when a local SST is
no longer referenced. If an old `.manifest` references an SST and a new one no
longer does, that SST is safe for deletion. It queues these SSTs for deletion
and removes them in the background.

The mirror keeps the newest `.manifest` state it observes. Reads initialize this
state. After a successful write, the mirror updates the corresponding state and
applies this rule:

- Build full object paths for every SST referenced by the latest manifest and
  its active checkpoint manifests. This includes `ExternalDb.sst_ids`, resolved
  under each external database path. Queue paths present in the old reference
  set but absent from the new one.

This is done asynchronously. Manifests are queued in order. A single deletion
task drains all manifests, diffing from one to the next, until the queue is
drained or a hard-coded threshold is reached. It then deletes what it has and
starts again. If the manifest queue is full, the oldest manifest is dropped
when a new manifest is added. This can cause SSTs to leak if an SST is added
and then removed between two manifest writes that are both dropped. Pessimistic
reconciliation handles these cases.

_The hope here is that there will be less queue pressure than we saw in the
CachedObjectStore eviction algorithm since there should be ~64x fewer files to
track and delete (256MiB vs. 4MiB, roughly). This will need to be tested. If
it can't keep up, we can probably split the deletion queue across multiple
cores._

#### Pessimistic Reconciliation

An SST can be written successfully and then lost before it is recorded in
`.compactions` or a manifest. Metadata diffs cannot discover such files. They
also cannot reconstruct transitions missed across restart. A full remote scan
runs every ten minutes by default to collect these cases:

- Snapshot all files in `objects/`.
- Group them by their remote parent prefix.
- LIST each distinct parent prefix on the wrapped remote store.
- Delete any local file absent from the remote result after rechecking its
  generation and current references.

The periodic scan only deletes files after remote GC has removed them.
`Some(interval)` enables it and must be non-zero; `None` disables only this
backstop. Dropping the wrapper cancels the task without delaying `Db::close()`.

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

### Compactor Checkpoint Retention

`CompactorOptions::checkpoint_lifetime` configures the checkpoint written
before compaction inputs are removed from the manifest. The default remains 15
minutes. Manifest-driven reclamation keeps SSTs referenced by these checkpoints.

The checkpoint protects scans, gets, snapshots, and transactions that began
before the manifest update. Shortening it reduces the mirror's disk requirement
but increases the risk that a long-running read loses an SST. Operators should
set it at least as long as the longest expected in-flight read.

The default 15 minutes means the operator will need 15 minutes worth of disk
for both ingestion and compaction. If a workload is running at 1 GiB/s, the
operator will need 900 GiB of disk for the mirror. A 1 minute checkpoint
lifetime reduces that to roughly 60 GiB.

### Construction and Cleanup

`ObjectStoreMirrorBuilder::build` validates its options, prepares the local
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
including SSTs resolved through `ExternalDb.sst_ids`, uses the cache's existing
remote-read concurrency and retry policy, and returns an error if any SST cannot
be populated.

### Prefix Store Support

When the cache wraps a `PrefixStore`, `db_root` is relative to the prefix
store's logical namespace. For example, warming `foo` through a prefix store
rooted at `tenant1` reads `foo/manifest/...` through the wrapper, which maps to
`tenant1/foo/manifest/...` in the underlying bucket. The cache stores
`objects/foo/compacted/...`; it does not include the hidden prefix. External
SST paths must be reachable through the same wrapped namespace, as they must be
for normal database reads.

### Failure and Restart

Complete files under `objects/` survive restart; incomplete uploads and
downloads are removed during construction. In-memory metadata state is rebuilt
from subsequent reads and writes, which initialize state without producing
deletions. After remote GC, the periodic scan removes SSTs from transitions
missed before restart and SSTs that never reached metadata.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that
apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

`LocalOnly` adds explicit missing-object errors. Compactor checkpoints protect
superseded SSTs used by in-flight iterators.

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
- [x] Checkpoints
- [x] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

No metadata format changes are required. The mirror observes manifests,
checkpoint references, `ExternalDb.sst_ids`, and failed compaction outputs.

### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [x] Distributed compaction
- [x] Compactions format

The compactor checkpoint lifetime becomes configurable. The `.compactions`
format does not change.

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
`ObjectStoreMirror` is initially available only in APIs that can supply a custom
`ObjectStore`; this RFC does not add binding-specific configuration or CLI
commands.

## Operations

### Performance and Cost

Local hits perform range reads from one whole SST file. A local miss returns
`LocalCacheError` without remote fallback. `Refetch` validation retries and
`warm` may read remote storage explicitly. WAL and untagged coordination reads
always use remote storage.

Compacted SST writes stream to local and remote storage concurrently and return
after the remote write succeeds and the local file is installed. WAL writes
remain on the remote path.

One `warm` call reads the latest manifest and issues one full-object GET
for each referenced SST not already local. It may therefore transfer the full
live compacted data set when starting with an empty cache. Existing local SSTs
avoid those GETs. The utility shares the cache's remote-read concurrency with
background refetches and returns only after every SST in its manifest snapshot
is installed or one fails.

Metadata transitions reclaim normal compaction churn without remote SST LISTs.
Every ten minutes by default, the fallback issues one LIST per distinct remote
parent prefix represented locally. Setting the reconciliation interval to
`None` eliminates these periodic LISTs.

### Capacity

The cache has no maximum-size eviction setting in this proposal. It cannot
discard an SST and preserve its `LocalOnly` contract, so operators must size the
volume for the required working set.

Operators size the volume for the live SST set, temporary files under
`uploading/` and `downloading/`, and SSTs retained by active compactor
checkpoints. Retained data follows compaction churn rather than logical ingest
and may be larger due to write amplification.

Never-published SSTs and missed metadata transitions remain until remote GC and
the next periodic scan. If the scan is disabled, they remain until a DELETE
passes through the cache or the root is rebuilt.

### Observability

TODO

### Compatibility

The release that introduces `ObjectStoreMirror` also deprecates
`CachedObjectStore` without changing its runtime behavior. The following
release removes `CachedObjectStore`, its module, configuration, and bindings.

## Testing

- Manifest diffs retain latest, checkpointed, and external SST paths.
- `Running` and `Compacted` outputs remain protected; transitions to `Failed`
  remove partial and final outputs not referenced by a manifest.
- Successful completion, compaction trimming, and initialization after restart
  do not delete SSTs.
- Queued deletion rechecks references and file generation.
- Periodic reconciliation removes local files only after remote absence.

## Rollout

Release `ObjectStoreMirror` and deprecate `CachedObjectStore`. Remove
`CachedObjectStore` in the following release.

## Alternatives

### Remote-Only Reconciliation

We considered relying only on periodic remote LIST. This is simple, but all
normal compaction churn then waits for remote GC and the next mirror scan. The
metadata fast path reclaims that volume once its checkpoint references expire.

### GC-Rule Reconciliation

We considered running compacted-GC eligibility directly against local files.
This would duplicate or tightly couple the mirror to compaction watermarks and
publication rules. Reference transitions handle files that were previously
published; the remote scan handles files that never reached metadata.

### Write-Back

We considered acknowledging compacted SST writes after the local copy was
installed and uploading them remotely in the background. Manifest and
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

- Added metadata-driven reclamation with a periodic remote reconciliation
  backstop and configurable compactor checkpoint retention.
- Added `with_vfs` and a minimal virtual filesystem abstraction.
- Removed the GET policy; routing is fixed by `ObjectStoreCallTag`.
- Added the `CachedObjectStore` deprecation and removal schedule.
- Made `LocalOnly` the default and cache population explicit.
- Made the proposal additive by introducing `ObjectStoreMirror` alongside
  `CachedObjectStore`.
- Dropped write-back after comparing its publication barrier with SlateDB's
  existing upload parallelism.
- Initial draft.
