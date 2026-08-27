# Add ObjectStoreMirror

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

This RFC adds `ObjectStoreMirror`, a whole-file local mirror for compacted SSTs.
It is separate from the existing part-based `CachedObjectStore`, which we will
deprecate and remove.

`ObjectStoreMirror` supports local-only reads, write-through mirroring, cache
warming, and garbage collection. It guarantees all compacted SST reads are from
local files and all writes are durable to object storage before returning
success.

Each `ObjectStoreMirror` serves one database root. The root is detected from
the first `.manifest` read or write. Subsequent `.manifest` operations for a
different root are rejected. External SSTs referenced by the database remain
supported and may reside under other roots.

## Motivation

Recent performance testing showed that SlateDB's `CachedObjectStore` is not
useful. In fact, it did more harm than good. It occupies an area between a
best-effort cache and a full local replica, but it does neither job well.

- It splits objects into fixed-size 4MiB (default) parts. This adds latency to
  small reads and creates many files for large writes.
- A single 256MiB SST becomes 64 part files plus metadata. This creates
  eviction pressure and slows startup scans that rebuild the in-memory index.
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

Some workloads wish to fully cache database SSTs locally to avoid network
latency and bandwidth. `FoyerHybridCache` is not a good fit for these use
cases:

- It's best effort caching, so under load it can drop blocks.
- The in memory index is costly to build at startup and consumes high memory.
- Caching on compaction is costly because you need to break every SST into
  blocks and add them to the block cache. Locality is also lost because SST
  blocks are mixed.
- GC has more cost because you need to delete every block from Foyer as opposed
  to one file unlink.

Rather than change the existing `CachedObjectStore` in place, this RFC adds
`ObjectStoreMirror` under `slatedb/src/object_store_mirror` to address these
issues.

## Goals

- Add a whole-SST local mirror for compacted SSTs.
- Support local-only reads, write-through mirroring, and cache warming.
- Garbage collect obsolete local SSTs promptly without duplicating GC
  eligibility.
- Make the new cache additive so existing users are unaffected.

## Non-Goals

- Changing the runtime behavior of `CachedObjectStore`.
- Caching WALs, manifests, or other coordination objects.
- Providing size-based eviction, write-back, or a best-effort caching.

## Design

### Public API

`ObjectStoreMirror` is a normal `ObjectStore` implementation in
`slatedb/src/object_store_mirror`. Users construct it with a local cache root
and remote store, then pass it through the existing
`DbBuilder::new`/`Db::builder`/`DbReaderBuilder::builder` object store
parameter.

```rust
impl ObjectStoreMirror {
    pub fn builder(
        local_dir: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
    ) -> ObjectStoreMirrorBuilder;
}

impl ObjectStoreMirrorBuilder {
    /// Sets the virtual filesystem used for local I/O. The default is
    /// `StdVfs`.
    pub fn with_vfs(self, vfs: Arc<dyn Vfs>) -> Self;

    /// Sets the maximum number of concurrent downloads used for manifest
    /// warming, `.compactions` prefetching, and refetches. The default is 8.
    pub fn with_download_concurrency(self, concurrency: usize) -> Self;

    /// Sets the predicate used to select which segments are mirrored.
    ///
    /// The predicate receives the latest manifest and the segment prefix being
    /// evaluated. An empty prefix identifies the root segment. The default
    /// predicate selects every segment.
    pub fn with_segment_predicate(
        self,
        predicate: impl Fn(&ManifestCore, &[u8]) -> bool + Send + Sync + 'static,
    ) -> Self;

    /// Sets the interval at which the mirror scans remote storage to GC
    /// obsolete local SSTs. The default is `Some(Duration::from_secs(600))`.
    /// Passing `None` disables periodic GC but not metadata-driven GC.
    pub fn with_gc_interval(
        self,
        interval: Option<Duration>,
    ) -> Self;

    /// Validates the configuration, acquires the cache-directory lock, cleans
    /// invalid local entries, and starts background workers. The database root
    /// is detected from the first `.manifest` read or write.
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
    .with_download_concurrency(8)
    .with_reclamation_interval(Some(Duration::from_secs(600)))
    .build()
    .await?;
let db = Db::builder(db_path, cache).build().await?;
```

### Filesystem layout

Four types of files exist under the cache root:

- `LOCK`: A persistent lock file held exclusively for the lifetime of the
  mirror.
- `01M05WR6EZ6ZF44TGFNN5HFTDD.sst`: The complete SST file, which is byte-for
  byte identical to the remote object.
- `01M05WR6EZ6ZF44TGFNN5HFTDD.sst.1234567890`: A temporary file that is being
  written to either for uploading or downloading purposes. The suffix is an
  atomic counter unique to the process.
- `01M05WR6EZ6ZF44TGFNN5HFTDD.sst.meta`: Metadata for the SST, including its
  canonical object path, ETag, version, and attributes. The path is used to
  recover the remote location after restart and verify the filename hash.

Each SST-related file is prefixed with an MD5-encoding of its object path with
the filename stripped. This protects against filename collisions between
external databases and keeps the cache root flat.

The mirror maintains an in-memory map from each MD5 prefix to its canonical
parent path. Startup reconstructs the map from `.meta` files. Each installation
atomically checks or inserts the mapping before publishing the SST and rejects
a conflicting path.

A directory might look like this:

```text
<cache-root>/
  LOCK
  754128269b532c9827ffa09d3afb6118.01M05WR6EZ6ZF44TGFNN5HFTDD.sst
  754128269b532c9827ffa09d3afb6118.01M05WR6EZ6ZF44TGFNN5HFTDD.sst.meta
  754128269b532c9827ffa09d3afb6118.01M05WR997G22470E93PBPVAA2.sst.3
  4e7dc5d27c63e00966170758c2ff14bf.01M05WRF0MG8EZY9HJEY36JE4B.sst.5
  4e7dc5d27c63e00966170758c2ff14bf.01M05WRF0MG8EZY9HJEY36JE4B.sst.meta
```

This directory contains files for two directories:

- /path/to/db/compacted (754128269b532c9827ffa09d3afb6118)
- /path/to/other/db/compacted (4e7dc5d27c63e00966170758c2ff14bf)

The `754128269b532c9827ffa09d3afb6118` prefix has one fully downloaded SST
(`01M05WR6EZ6ZF44TGFNN5HFTDD.sst`) and one in-flight SST
(`01M05WR997G22470E93PBPVAA2.sst.3`).

The `4e7dc5d27c63e00966170758c2ff14bf` prefix has one partially downloaded SST
(`01M05WRF0MG8EZY9HJEY36JE4B.sst.5`) and its metadata. The SST has not yet been
fully downloaded and renamed.

Upload and download files are undifferentiated. No collision is possible
because the temporary file suffix is unique to the process. Multiple operations
for the same SST should never be in flight.

`build()` takes an exclusive operating-system lock on `LOCK` and holds it until
the mirror is dropped. The file is not removed when the lock is released. If
another mirror owns the cache directory, `build()` fails.

### Warming

The mirror is warmed continuously as new `.manifest` files are read and
written. `ObjectStoreMirror` inspects the path for each object and looks for
`.manifest` files. When it sees one, it decodes the manifest and compares its
referenced SSTs with its own local state. Any missing SSTs are downloaded
synchronously. This happens after the `.manifest` call is forwarded to the
wrapped store, but before returning to the caller.

A large compaction job can finish and update the `.manifest` with gigabytes,
or even terabytes of new SSTs. Blocking the manifest update to download the
entire set could take minutes or even hours. To prevent large stalls, once the
database root is detected, `ObjectStoreMirror` derives its
`.compactions` path and periodically polls it for in-flight job output. It downloads any missing SSTs in the
background. The `.manifest` blocking is therefore a final true-up rather than a
complete download of all output from completed compaction jobs.

This behavior implicitly warms a database when it is first opened. Builders
always read and write manifests in their `build` function. `DbReader`s also
benefit from this approach. As new manifests are polled, the mirror will
download any missing SSTs before forwarding the manifest read. This guarantees
that all reads will come from local disk.

Referenced SSTs are every SST returned by `ManifestCore::all_sst_views()`. This
includes L0 and compacted SSTs in the root tree and all named segments. SST IDs
found in `ExternalDb.sst_ids` are resolved under the external database's path;
all others are resolved under the mirror's database root.

SSTs referenced by the manifest's checkpoints are not considered referenced
since readers only need read SSTs from the current manifest. (Mirror garbage
collection still retains checkpointed SSTs until the checkpoint expires.)

### Writes

Tagged compacted SST writes are write-through. Single-PUT and multipart writes
tee bytes to the remote store and its temporary file.

The write returns only after the remote upload succeeds, its `.meta` file is
written, and the complete local file is atomically renamed. A remote failure
removes the temporary file and returns the remote error. A local failure returns
`LocalCacheError`. Remote SSTs that already completed are left unreferenced for
garbage collection. `RetryingObjectStore` is updated to avoid retrying
`LocalCacheError`. Disk errors are treated as terminal. `Db`'s closed status
will be set with a `Data` error.

Tagged WAL and untagged writes pass through to remote storage. Manifest,
compactions, WAL, and GC boundary PUTs retain their existing conditional-write,
fencing, and publication ordering.

`ObjectStoreMirror` will watch for `.manifest` writes. When it sees an object
with a `.manifest` extension, it will decode the manifest and synchronously
download any compacted SSTs missing from the local cache.

All downloads use `single_flight.rs` to avoid duplicate downloads.

### Reads

`ObjectStoreMirror` has three internal read modes:

- `Bypass` reads directly from the wrapped store and does not use the local
  mirror.
- `Local` reads SSTs from the local filesystem mirror and returns an error
  if any are missing.
- `Refetch` forces a synchronous remote read of the full SST, overwriting the
  local copy if it exists. Returns only the requested range to the caller. This
  is used to repair corrupt files.

`ObjectStoreCallTag` is inspected to determine which mode to use.

| Request | Routing |
|---|---|
| Untagged or tagged WAL | `Bypass` |
| Tagged compacted with `tag.retry.is_some()` | `Refetch` |
| Other tagged compacted | `Local` |

Object metadata (ETag, version, attributes, and so on) are cached as part of
the local SST data so `GetResult` and `PutResult` always contain accurate data.
On cache warm, object metadata is loaded from disk (or the remote store if the
SST is missing) and stored in memory. As new `.meta` files are written, the
mirror updates its in-memory metadata cache.  Metadata-only reads are served
from the in-memory cache.

### Deletes

Delete operations pass through to the wrapped store. Local files that match the
deleted path are removed if they exist. Local and remote deletions are done in
parallel, and a failure in one does not affect the other.

This means a client running a local garbage collector inherits the GC's delete
calls locally. Garbage collectors that run remotely do not directly remove
local files, though. To support remote garbage collection, the mirror needs to
periodically scan the remote store for files that are no longer present.

Deletions also remove any in-memory cache state for the deleted object.

### Garbage collection

`ObjectStoreMirror` has two garbage collection phases: an optimistic immediate
metadata-driven phase and a pessimistic periodic exhaustive remote scan.

The optimistic approach is required to keep disk usage low in high-throughput
workloads. Without an active deletion mechanism, even a minute of writes and
compaction churn can generate hundreds of outdated files. This is not a concern
for object storage, but is for local disks.

Pessimistic GC is required to handle the case where a local SST is
written successfully and then lost before it is recorded in metadata. This can
be caused by a crash, a process restart, a failed compaction job, and so on.
These are rare cases, but they can leave a local SST that is no longer
referenced and should be deleted. Pessimistic GC is effectively a
cheap way to copy the garbage collector's logic without running it in two
places.

Garbage collection also removes any in-memory cache state for the deleted
object.

#### Optimistic garbage collection

`ObjectStoreMirror` uses `.manifest` transitions to detect when a local SST is
no longer referenced. If an old `.manifest` references an SST and a new one no
longer does, that SST is safe for deletion. It queues these SSTs for deletion
and removes them in the background.

The mirror keeps the newest `.manifest` state it observes. Reads and writes
update the state and apply this rule:

- Build full object paths for every SST referenced by the latest manifest and
  its active checkpoint manifests. Queue (for deletion) paths present in the
  old reference set but absent from the new one.
- Remove any in-memory manifests that are no longer referenced by the latest
  manifest or its active checkpoints.

"Reference" here means every SST returned by `ManifestCore::all_sst_views()`
for the latest manifest and each active checkpoint manifest. This includes L0
and compacted SSTs in the root tree and all named segments. SST IDs found in
`ExternalDb.sst_ids` are resolved under the external database's path; all
others are resolved under the mirror's database root.

Missing checkpoint manifests are fetched from the remote store.

On both reads and writes, this is done synchronously after the `.manifest`
is read/written but before it is returned. The return is blocked until both are
complete.

#### Pessimistic garbage collection

An SST can be written successfully and then lost before it is recorded in
`.compactions` or `.manifest` files. Metadata diffs cannot discover such files.
A full remote scan runs every ten minutes by default to collect these cases:

- Snapshot the local `.sst` file list.
- Read their canonical object paths from `.meta` and group them by remote parent
  prefix.
- LIST each distinct parent prefix on the wrapped remote store.
- Delete any local file absent from the remote result.

The periodic scan only deletes files after remote GC has removed them. This
implies that local pessimistic garbage collection will not delete anything younger
than the GC's compacted SST `min_age` setting. It also means the
 `ObjectStoreMirror` will inherit all of the GC's rules.

Pessimistic garbage collection is not necessary if the garbage collector is running
in the same process. The GC's delete calls will remove local files directly
(see Delete Semantics, above). Users may disable the periodic scan by passing `None` to
`ObjectStoreMirrorBuilder::with_reclamation_interval`.

> [!IMPORTANT]
> This design requires a bucket and endpoint with strongly consistent object
> reads, writes, deletes, and listings. Garbage collection treats confirmed remote
> absence as authoritative and may delete the only local copy, so it must be
> disabled when these guarantees are unavailable. In particular:
>
> - Tigris global and dual-region buckets are strongly consistent for requests
>   within one region but eventually consistent across regions. All writers and
>   caches performing garbage collection must access such a bucket from the same
>   region; Tigris
>   multi-region and single-region buckets provide strong consistency globally.
> - Azure RA-GRS and RA-GZRS secondary endpoints are eventually consistent with
>   the primary. Garbage collection must use the primary endpoint and remain
>   disabled while reads are directed to a secondary endpoint.

### Segment Support

`ObjectStoreMirror` supports segment-based routing. This requires two changes:

1. `ObjectStoreCallTag` needs a new `segment` field to indicate the segment prefix for routing.
2. `ObjectStoreMirrorBuilder` needs a new `with_segment_predicate` method to allow users to specify which segments should be mirrored.

The segment field is required because a new SST may not appear in the manifest yet. The `ObjectStoreMirror` needs to know the segment prefix to evaluate the predicate and decide whether to mirror the SST.

```rs
pub struct ObjectStoreCallTag {
    // ...

    // Optional segment prefix for routing.
    pub segment: Option<Bytes>,
}
```

`segment` will be set for all reads and writes. This changes `ObjectStoreCallTag` from a `Copy` type to a `Clone` type and changes to a heap allocation. We invoke SST read/writes infrequently enough that we believe this won't cause CPU performance to degrade.

The `TableStore` must be updated to receive the field in its read and write SST functions. This touches a wide range of files, but the changes are mechanical and straightforward.

`ObjectStoreMirrorBuilder` accepts an optional segment predicate:

```rust
Fn(&ManifestCore, &[u8]) -> bool + Send + Sync + 'static
```

The predicate receives the last seen manifest and the segment prefix being evaluated. It selects every segment by default. An empty prefix identifies the root segment.

When processing a manifest, the mirror evaluates the predicate against the incoming `ManifestCore`. It warms newly selected SSTs before returning the manifest, publishes the new set of mirrored paths, then removes SSTs that are no longer selected as part of optimistic garbage collection (see above).

For `.compactions` entries and writes, the predicate receives the last manifest observed by the mirror. The segment prefix is passed separately, so the predicate can select a new segment before it appears in the manifest. For example, a date-based predicate can recognize a new `YYYYMMDD` prefix when the day rolls over.

Reads also run through the predicate. If the segment is selected, the read is routed to the local mirror. If it is not selected, the read is routed to the wrapped object store. A missing local mirror file is treated as a `LocalCacheError` and does not fall back to the wrapped store.

Selected writes are written locally and remotely. Unselected writes go directly to the wrapped object store. Local garbage collection only retains SSTs referenced by selected segments.

### Compactor Checkpoint Retention

We add a new `CompactorOptions::checkpoint_lifetime` configuration that sets
the (currently hardcoded) checkpoint written before compaction inputs are
removed from the manifest. The default stays 15 minutes (the currently
hardcoded value). Manifest-driven reclamation keeps SSTs referenced by these
checkpoints.

The checkpoint protects scans, gets, snapshots, and transactions that began
before the compaction's manifest update. Shortening it reduces the mirror's
disk requirement but increases the risk that a long-running read loses an SST.
Operators should set it at least as long as the longest expected in-flight
read.

The default 15 minutes means the operator will need 15 minutes worth of disk
for both ingestion and compaction. If a workload is running at 1 GiB/s, the
operator will need 900 GiB of disk for the mirror. A 1 minute checkpoint
lifetime reduces that to roughly 60 GiB.

### Virtual Filesystem

`ObjectStoreMirror` performs all local I/O through a small asynchronous `Vfs`
trait. The interface is limited to the operations the mirror needs: range
reads, streamed temporary-file writes, directory creation, rename, remove, and
listing and file locking. `ObjectStoreMirrorBuilder::with_vfs` replaces the
default implementation.

The design supports three implementations:

1. `StdVfs`, the default implementation based on standard filesystem I/O.
2. `IoUringVfs`, a future Linux implementation based on io_uring.
3. `SimulatedVfs`, a deterministic implementation for simulation tests.

The VFS does not provide caching, eviction, or object-store semantics. It only
abstracts the local filesystem operations used by the mirror.

### Retries and metrics

The user supplies the cache as the main object store. SlateDB then applies its
existing internal wrappers. The base-to-outer construction order is:

```text
S3ObjectStore -> ObjectStoreMirror -> InstrumentedObjectStore -> RetryingObjectStore
```

Requests travel in the opposite direction:

```text
RetryingObjectStore -> InstrumentedObjectStore -> ObjectStoreMirror -> S3ObjectStore
```

Almost all mirror requests are synchronous. The one exception is `.compactions`
pre-fetching. This is done best effort. Failed downloads are simply ignored and
triggered again in subsequent `.compactions` reads or the next `.manifest` update
they appear in.

### Startup

On startup, `ObjectStoreMirrorBuilder::build` :

1. Validates options
2. Makes the local directory if it does not exist
3. Acquires the exclusive `LOCK` file lock
4. Removes any `.sst.[tmp_num]` files (incomplete uploads or downloads)
5. Scans `.sst` and `.meta` pairs, deleting entries with a missing partner,
   malformed metadata, a canonical path that does not match the local filename,
   or a conflicting MD5-to-parent-path mapping
6. Reconstructs the in-memory path and object metadata maps from valid pairs
7. Starts background workers

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that
apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [x] Transactions
- [x] Snapshots
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

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

A binding wrapper will be provided for `ObjectStoreMirror`.

## Operations

### Performance and Cost

Local hits perform range reads from one whole SST file. A local miss returns
`LocalCacheError` without remote fallback. `Refetch` validation retries and
warming reads remote storage explicitly. WAL and untagged coordination reads
always use remote storage.

Compacted SST writes stream to local and remote storage concurrently and return
after the remote write succeeds and the local file is installed. WAL writes
remain on the remote path.

Cache warming reads the latest manifest and issues one full-object GET
for each referenced SST not already local. It may therefore transfer the full
live compacted data set when starting with an empty cache. Existing local SSTs
avoid those GETs. The utility shares the cache's remote-read concurrency with
background refetches and returns only after every SST in its manifest snapshot
is installed or one fails.

Metadata transitions reclaim normal compaction churn without remote SST LISTs.
Every ten minutes by default, the fallback issues one LIST per distinct remote
parent prefix represented locally. Setting the reclamation interval to
`None` eliminates these periodic LISTs.

### Capacity

The cache has no maximum-size eviction setting in this proposal. It cannot
discard an SST and preserve its `Local` contract, so operators must size the
volume to store the entire database, including in-flight compaction SSTs and
ungarbage-collected SSTs.

### Observability

TODO

### Compatibility

The release that introduces `ObjectStoreMirror` also deprecates
`CachedObjectStore` without changing its runtime behavior. The following
release removes `CachedObjectStore`, its module, configuration, and bindings.

## Testing

TODO

## Rollout

Release `ObjectStoreMirror` and deprecate `CachedObjectStore`. Remove
`CachedObjectStore` in the following release.

## Future Work

### Incremental Compaction

The approach in this RFC requires up to 2x disk space when large sorted runs are compacted.
Suppose we have a 100 GiB SR7. We compact SR7 into SR8, which grows to be 75 GiB. In the
current design, warms the 75 GiB SR8 in the background as the job runs. SR7 remains active
in the manifest. Thus, right before the manifest swap, we will have
100 GiB (SR7) + 75 GiB (SR8) = 175 GiB of local disk usage. Once the manifest swap occurs,
SR7 is dropped and disk usage shrinks to 75 GiB.

ScyllaDB has a similar problem and solves it with [incremental compaction](https://www.scylladb.com/2020/01/16/maximizing-disk-utilization-with-incremental-compaction/). We could implement a similar
design.

### Generalized Public API

The current `ObjectStoreMirror` API is designed for SlateDB's use case. We could generalize it to support other use cases. To do so, I think we'd need to make the warming and caching strategies pluggable. Perhaps we could use an event-based approach where the user can register callbacks for certain events (e.g., manifest read, SST write) and implement their own warming and caching logic. This is left to future work if we find demand for it.

## Alternatives

### CachedObjectStore With Eviction Disabled

We could theoretically use `CachedObjectStore` with eviction disabled. This would require:

- Fully warming the cache before starting the database
- Disabling eviction so no SSTs are removed after warming
- Running garbage collection locally so the cache sees deletions and removes old SSTs

This approach would then behave like `ObjectStoreMirror`. However, if you
take that approach, you might as well...

1. Remove .part files since they serve no purpose
2. Optimistically GC to avoid disk pressure
3. Make full SST warming easier
4. Make local SST writes mandatory rather than best-effort
5. Clean up incomplete writes during startup

This is what `ObjectStoreMirror` does.

### Remote-Only Reclamation

We considered relying only on periodic remote LIST. This is simple, but all
normal compaction churn then waits for remote GC and the next mirror scan. The
metadata fast path reclaims that volume once its checkpoint references expire.

### GC-Rule Reclamation

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

### Nested Directory Structure

This RFC used to propose a nested directory structure for the local mirror. The
intent was to reflect the remote object store's directory structure and avoid
collisions.

This meant we might have empty directories floating around, or we need to walk backwards to clean them up. The flat design felt cleaner. It behaves more like an object store: when the last file in a directory disappears, the directory disappears on its own.

The flat approach also side steps any path encoding oddities. I had Sol look into it, and it sounds like `object_store` `PathBuf` is compatible with all major filesystems. But apparently Windows is case insensitive. The flat approach felt a bit safer.

There is, however, an open question around the CPU cost of computing the MD5 prefix for every SST path. The prefix is used to avoid collisions and keep the cache root flat. We could consider using a faster hash function or the nested path design if the CPU cost is significant. We should measure this in practice.

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

- Added metadata-driven reclamation with a periodic remote reclamation
  backstop and configurable compactor checkpoint retention.
- Added `with_vfs` and a minimal virtual filesystem abstraction.
- Removed the GET policy; routing is fixed by `ObjectStoreCallTag`.
- Added the `CachedObjectStore` deprecation and removal schedule.
- Made `Local` the default and cache population explicit.
- Made the proposal additive by introducing `ObjectStoreMirror` alongside
  `CachedObjectStore`.
- Dropped write-back after comparing its publication barrier with SlateDB's
  existing upload parallelism.
- Initial draft.
