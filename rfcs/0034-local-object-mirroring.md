# Add ObjectStoreMirror

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

This RFC adds `ObjectStoreMirror`, a generic whole-file local mirror for object
storage, and `SlateDbMirrorPolicy`, which uses it to mirror compacted SSTs. The
mirror is separate from the existing part-based `CachedObjectStore`, which we
will deprecate and remove.

The mirror decides how objects are kept locally. A `MirrorPolicy` decides which
objects are kept. `ObjectStoreMirror` lives in its own crate and knows nothing
about SlateDB. `SlateDbMirrorPolicy` holds all of SlateDB's rules.

With `SlateDbMirrorPolicy`, the mirror supports local-only reads, write-through
mirroring, cache warming, and garbage collection. It guarantees all compacted
SST reads are from local files and all writes are durable to object storage
before returning success.

Each `SlateDbMirrorPolicy` serves one database root. The root is detected from
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
`ObjectStoreMirror` to address these issues.

Most of a local mirror has nothing to do with SlateDB. File layout, downloads,
write-through, and cleaning up after remote deletes are useful to any
`object_store` user. Only the choice of which objects to keep, and when to drop
them, depends on SlateDB's manifests. Putting that choice behind a trait keeps
the SlateDB logic in one small place and lets other projects reuse the mirror.

## Goals

- Add a whole-SST local mirror for compacted SSTs.
- Support local-only reads, write-through mirroring, and cache warming.
- Garbage collect obsolete local SSTs promptly without duplicating GC
  eligibility.
- Keep the mirror generic, with SlateDB's rules in a pluggable policy.
- Make the new cache additive so existing users are unaffected.

## Non-Goals

- Changing the runtime behavior of `CachedObjectStore`.
- Caching WALs, manifests, or other coordination objects.
- Providing size-based eviction, write-back, or a best-effort caching.
- Mirroring objects that are overwritten in place. The mirror assumes every
  mirrored path is written once. See [Guarantees](#guarantees).

## Design

### Crates

- `slatedb-mirror` is a new workspace crate containing `ObjectStoreMirror`,
  `MirrorPolicy`, `MirrorHandle`, `MirrorError`, and `Vfs`. It doesn't depend
  on `slatedb`. A crate boundary is the only thing that keeps the mirror truly
  generic, because it can't reach `ManifestCore` even by accident.
- `single_flight.rs` moves out of `slatedb`, into either `slatedb-mirror` or
  `slatedb-common`.
- `SlateDbMirrorPolicy` lives in `slatedb`.

### Public API

```rust
pub enum ReadRoute {
    /// Wrapped store only.
    Remote,
    /// Wrapped store. The whole object goes to `MirrorPolicy::observe` before
    /// the caller gets a result.
    Observe,
    /// Local copy only. A miss is an error.
    Local,
    /// Download the whole object again, replace the local copy, and serve the
    /// request from it.
    Refetch,
}

pub enum WriteRoute {
    /// Wrapped store only.
    Remote,
    /// Wrapped store, then the payload goes to `MirrorPolicy::observe`.
    Observe,
    /// Write to both. Returns after the upload succeeds and the local file is
    /// in place.
    Mirror,
}

#[async_trait]
pub trait MirrorPolicy: Send + Sync + 'static {
    /// Runs on every GET and HEAD. `options.head` is true for HEAD. Must be
    /// cheap.
    fn read_route(&self, path: &Path, options: &GetOptions) -> Result<ReadRoute>;

    /// Runs on every PUT. Must be cheap.
    fn put_route(&self, path: &Path, options: &PutOptions) -> Result<WriteRoute>;

    /// Runs on every multipart upload. Must be cheap. `Observe` isn't
    /// supported here and fails the upload.
    fn put_multipart_route(
        &self,
        path: &Path,
        options: &PutMultipartOptions,
    ) -> Result<WriteRoute>;

    /// Runs for `Observe` routes, after the remote call succeeds and before the
    /// caller gets the result.
    async fn observe(&self, path: &Path, bytes: &Bytes, mirror: &MirrorHandle) -> Result<()> {
        Ok(())
    }

    /// Background work. `build()` spawns it; dropping the mirror cancels it.
    async fn run(self: Arc<Self>, mirror: MirrorHandle) {}
}

#[derive(Clone)]
pub struct MirrorHandle { /* Arc<Inner> */ }

impl MirrorHandle {
    /// Downloads any paths that aren't local. The request is ordered when this
    /// is called, not when the returned future is first polled. See
    /// [Ordering](#ordering). Resolves when every path is local, or one
    /// download fails.
    pub fn fetch(
        &self,
        paths: impl IntoIterator<Item = Path>,
    ) -> impl Future<Output = Result<()>> + Send + 'static;
    /// Downloads in the background, best effort.
    pub fn prefetch(&self, paths: impl IntoIterator<Item = Path>);
    /// Queues removal of local copies. Never touches the remote store.
    pub fn evict(&self, paths: impl IntoIterator<Item = Path>);
    pub fn contains(&self, path: &Path) -> bool;
    /// The wrapped store. Calls made here skip the policy.
    pub fn remote(&self) -> &Arc<dyn ObjectStore>;
}

impl ObjectStoreMirror {
    pub fn builder(
        local_dir: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
        policy: Arc<dyn MirrorPolicy>,
    ) -> ObjectStoreMirrorBuilder;
}

impl ObjectStoreMirrorBuilder {
    /// Sets the virtual filesystem used for local I/O. The default is
    /// `StdVfs`.
    pub fn with_vfs(self, vfs: Arc<dyn Vfs>) -> Self;

    /// Sets the maximum number of concurrent downloads used by `fetch`,
    /// `prefetch`, and `Refetch`. The default is 8.
    pub fn with_download_concurrency(self, concurrency: usize) -> Self;

    /// Sets the interval of the remote scan that removes local copies of
    /// objects deleted remotely. The default is
    /// `Some(Duration::from_secs(600))`. `None` disables the scan.
    pub fn with_remote_scan_interval(self, interval: Option<Duration>) -> Self;

    /// Sets the clock used for retry backoff, the remote scan, and the
    /// last-modified time of mirrored writes. The default uses Tokio's clock.
    pub fn with_system_clock(self, clock: Arc<dyn SystemClock>) -> Self;

    /// Sets how many times the mirror retries a transient error from its own
    /// remote calls. The default, `None`, retries forever, like SlateDB's
    /// `object_store_max_retries`.
    pub fn with_max_retries(self, max_retries: Option<u32>) -> Self;

    /// Validates the configuration, acquires the cache-directory lock, cleans
    /// invalid local entries, and spawns `MirrorPolicy::run`.
    pub async fn build(self) -> Result<Arc<ObjectStoreMirror>, Error>;
}

#[async_trait]
impl ObjectStore for ObjectStoreMirror {
    // Standard ObjectStore methods delegate to local and remote storage.
}
```

`SlateDbMirrorPolicy` has its own builder in `slatedb`:

```rust
impl SlateDbMirrorPolicy {
    pub fn builder() -> SlateDbMirrorPolicyBuilder;
}

impl SlateDbMirrorPolicyBuilder {
    /// Sets the predicate used to select which segments are mirrored.
    ///
    /// The predicate receives the latest manifest and the segment prefix being
    /// evaluated. An empty prefix identifies the root segment. The default
    /// predicate selects every segment.
    pub fn with_segment_predicate(
        self,
        predicate: impl Fn(&ManifestCore, &[u8]) -> bool + Send + Sync + 'static,
    ) -> Self;

    pub fn build(self) -> Arc<SlateDbMirrorPolicy>;
}
```

Users pass the mirror through the existing
`DbBuilder::new`/`Db::builder`/`DbReaderBuilder::builder` object store
parameter. A complete instantiation looks like this:

```rust
let remote: Arc<dyn ObjectStore> = Arc::new(
    AmazonS3Builder::from_env()
        .with_bucket_name("my-bucket")
        .build()?,
);
let mirror = ObjectStoreMirror::builder(
    "/var/lib/slatedb/mirror",
    remote,
    SlateDbMirrorPolicy::builder().build(),
)
.with_download_concurrency(8)
.with_remote_scan_interval(Some(Duration::from_secs(600)))
.build()
.await?;
let db = Db::builder(db_path, mirror).build().await?;
```

### ObjectStoreMirror Responsibilities

- File layout, `.meta` files, temporary files, `LOCK`, startup cleanup, and
  `Vfs`.
- Download deduplication (single-flight), the download concurrency limit, and
  the in-memory metadata used for HEAD.
- DELETE removes the local copy too. COPY and RENAME are rejected. LIST goes to
  the remote store.
- The periodic remote scan, which removes local copies of remotely deleted
  objects. This is not a policy decision.
- Per-path ordering of downloads, write-throughs, evictions, and deletes.
- Typed `MirrorError`s, wrapped in `object_store::Error::Generic`.

### Guarantees

The mirror guarantees the following on its own, whatever the policy does:

- A local copy is byte-for-byte identical to the remote object at the time it
  was downloaded or written through the mirror.
- A DELETE through the mirror removes the local copy.
- When the remote scan is enabled, local copies of objects that other clients
  deleted are removed within one scan interval (plus scan time).
- Operations on the same path apply in the order they were issued. See
  [Ordering](#ordering).

The mirror only supports write-once paths. Policies must only
route paths that are never overwritten to `Local`, `Refetch`, or `Mirror`.
SlateDB SSTs meet this: their names are ULIDs and they are never rewritten.

### Filesystem layout

Four types of files exist under the cache root:

- `LOCK`: A persistent lock file held exclusively for the lifetime of the
  mirror.
- `01M05WR6EZ6ZF44TGFNN5HFTDD.sst`: The complete object, which is byte-for
  byte identical to the remote object.
- `01M05WR6EZ6ZF44TGFNN5HFTDD.sst.1234567890`: A temporary file that is being
  written to either for uploading or downloading purposes. The suffix is an
  atomic counter owned by the mirror. Only one mirror can hold a directory's
  `LOCK`, so the suffix is unique within the directory. A per-mirror counter
  (rather than a process-wide one) keeps file names deterministic across
  simulation runs in one process.
- `01M05WR6EZ6ZF44TGFNN5HFTDD.sst.meta`: Metadata for the object, including its
  canonical object path, ETag, version, and attributes. The path is used to
  recover the remote location after restart and verify the filename hash.

The examples use SlateDB SST names, but the mirror stores whatever file name the
object path ends with. Each object-related file is prefixed with an MD5-encoding
of its object path with the filename stripped. This protects against filename
collisions between directories (for example, external databases) and keeps the
cache root flat.

A file name that ends in `.meta` or `.<digits>` (including a bare `meta` or
`7`) can't be told apart from a metadata or temporary file. Object `foo.meta`
would even share a local name with the metadata of object `foo`. The mirror
refuses to keep local copies of such names: a `Local`, `Refetch`, or `Mirror`
route for one, or a `fetch` of one, fails with `MirrorError::Unsupported`
before anything reaches the wrapped store. SlateDB's SST names aren't
affected.

The mirror maintains an in-memory map from each MD5 prefix to its canonical
parent path. Startup reconstructs the map from `.meta` files. Each installation
atomically checks or inserts the mapping before publishing the file and rejects
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
because the temporary file suffix is unique to the process. Downloads are
deduplicated with `single_flight.rs`, and operations on the same path are
serialized (see [Ordering](#ordering)), so multiple operations for the same
object are never in flight.

`build()` takes an exclusive operating-system lock on `LOCK` and holds it until
the mirror is dropped. The file is not removed when the lock is released. If
another mirror owns the cache directory, `build()` fails.

### Reads

The policy's `read_route` picks one of four routes for each GET and HEAD:

- `Remote` reads directly from the wrapped store and does not use the local
  mirror.
- `Observe` reads from the wrapped store, then passes the whole object to
  `MirrorPolicy::observe` before returning to the caller.
- `Local` reads from the local mirror and returns `NotLocal` if the object is
  missing. It does not fall back to the wrapped store.
- `Refetch` forces a synchronous remote read of the full object, overwriting
  the local copy if it exists. Returns only the requested range to the caller.
  This is used to repair corrupt files.

`SlateDbMirrorPolicy` combines `Observe` and `Local` to keep the mirror warm.
Every `.manifest` read and write is routed through `Observe`. The policy decodes
the manifest, downloads any referenced SSTs that aren't local yet, and only
then lets the manifest return to the caller. SlateDB only reads compacted SSTs
that it found in a manifest or wrote itself through a `Mirror` write, so by the
time it issues a compacted SST read, the file is already on disk.
Large compaction outputs are prefetched in the background by polling
`.compactions`, so the manifest download is a final true-up rather than the
whole job's output. See [Warming](#warming) for details.

`observe` always sees the whole object:

- A ranged GET with the `Observe` route downloads the whole object, calls
  `observe`, and returns only the requested range.
- A HEAD with the `Observe` route goes to the wrapped store and does not call
  `observe`. It returns no content, so there's nothing for the caller to act
  on.
- A conditional GET that returns no body (for example, `NotModified`) does not
  call `observe`.

The mirror buffers the whole object in memory for `observe`, so policies should
only use `Observe` for small objects.

`Refetch` applies to HEAD the same way as to GET: it downloads the whole object
again and returns its metadata.

Object metadata (ETag, version, attributes, and so on) is stored with each
local object so `GetResult` and `PutResult` always contain accurate data. On
startup, object metadata is loaded from disk and stored in memory. As new
`.meta` files are written, the mirror updates its in-memory metadata cache.
Metadata-only reads for `Local` objects are served from the in-memory cache.

### Writes

The policy's `put_route` and `put_multipart_route` pick one of three routes for
each PUT and multipart upload:

- `Remote` writes directly to the wrapped store.
- `Observe` writes to the wrapped store, then passes the payload to
  `MirrorPolicy::observe` before returning to the caller.
- `Mirror` tees bytes to the wrapped store and a local temporary file
  (`<name>.<counter>`, see [Filesystem layout](#filesystem-layout)). The
  temporary file is renamed to `<name>` only after the upload succeeds, so a
  partial or failed write never looks like a complete local copy.

`Observe` isn't supported for multipart uploads. If `put_multipart_route`
returns it, `put_multipart` fails with `MirrorError::Unsupported` before
anything reaches the wrapped store. Supporting it would mean buffering every
part in memory, and `Observe` is meant for small objects that go through a
single PUT anyway. SlateDB writes manifests with a single PUT.

A `Mirror` write returns only after the remote upload succeeds, its `.meta`
file is written, and the complete local file is atomically renamed. A remote
failure removes the temporary file and returns the remote error. A local
failure after the upload succeeds returns `MirrorError::WriteCommitted` (see
[Errors and Retries](#errors-and-retries)). A single PUT writes the temporary
file and uploads at the same time, so any local failure there surfaces as
`WriteCommitted` once the upload succeeds. A multipart upload isn't visible
remotely until `complete`, so a local failure in a part or in `complete`
aborts the upload and returns `MirrorError::Local`.

The local copy's last-modified time comes from the mirror's clock, not the
remote store's, since a PUT doesn't return it. Remote objects that already
completed are left for the remote store's owner to clean up (for SlateDB, the
garbage collector).

Conditional-write modes pass through to the wrapped store unchanged, so
manifest, compactions, WAL, and GC boundary PUTs retain their existing
conditional-write, fencing, and publication ordering.

### Errors and Retries

The mirror returns its own errors wrapped in `object_store::Error::Generic`,
with a `MirrorError` as the source:

```rust
pub enum MirrorError {
    /// A `Local` route missed.
    NotLocal { path: Path },
    /// Local disk I/O failed. Nothing was written remotely.
    Local { source: std::io::Error },
    /// A write reached the remote store, but `observe` or local installation
    /// failed afterwards. The remote object exists.
    WriteCommitted { path: Path, source: Box<dyn Error + Send + Sync> },
    /// The policy returned an error from a route or `observe` call.
    Policy { source: Box<dyn Error + Send + Sync> },
    /// The mirror doesn't support this operation (COPY, RENAME, an
    /// `Observe` multipart upload, or a local copy of a reserved file name).
    Unsupported { operation: &'static str },
    /// `ObjectStoreMirrorBuilder::build` was given an invalid option, such as
    /// a download concurrency of zero.
    InvalidConfig { message: String },
}
```

`RetryingObjectStore::should_retry` is updated to never retry a `MirrorError`.
Today it retries every `Generic` error. For `WriteCommitted`, that would be
wrong: a retried manifest PUT hits `AlreadyExists`, `verify_put_succeeded`
finds our ULID, and the retry reports success even though warming never
finished. The caller must see the failure.

Because the mirror sits beneath `RetryingObjectStore`, that wrapper doesn't
cover the mirror's own remote calls (`fetch`, `prefetch`, and `Refetch`). The
mirror retries transient remote errors on those calls itself, with the same
backoff defaults as `RetryingObjectStore`. An error that reaches the caller has
already been retried.

`observe` failures are handled by call type:

- **Reads.** The error goes to the caller. Nothing was committed, so a caller
  that retries the read runs `observe` again.
- **Writes.** The mirror returns `WriteCommitted`. The object is durable, but
  the policy's follow-up work didn't finish, so the caller has to treat the
  write as having happened. SlateDB treats it as fatal: `Db` and the compactor
  close with a `Data` error, the same as local disk errors. On reopen, the
  manifest is read again and `observe` runs again.

### Ordering

The mirror serializes operations per path in the order they were issued. The
operations are `fetch`, `prefetch`, `evict`, `Refetch` downloads, `Mirror`
writes, and deletes. When a fetch, prefetch, or write is issued for a path
with a pending eviction:

- If the eviction hasn't started, it's canceled.
- If it has started, the new operation waits for it to finish, then runs.

So when manifest N+1's fetch of an object completes, the object is local, even
if manifest N's eviction of it was already running. At worst the eviction
removes the file and the fetch downloads it again. This holds as long as the
policy issues N's evictions before N+1's fetches.
`fetch` registers its request when it's called, not when the returned future
is polled. A fetch future that is never polled holds its paths' place in line until
it is dropped.

Adjacent downloads of the same path (`fetch`, `prefetch`, and `Refetch`) run
together rather than one after another, and single-flight collapses them into
one remote GET. This matters most for `Refetch`: several readers that hit the
same corrupt file share one download instead of queueing a full download each.
Writes, deletes, and evictions still run alone.

`Local` reads don't take part in this ordering. A read that races with an
eviction may get `NotLocal`. For SlateDB, this only affects reads of SSTs that
no manifest or active checkpoint references (see
[Compactor Checkpoint Retention](#compactor-checkpoint-retention)).

### Deletes, Copies, Renames, and Lists

Delete operations pass through to the wrapped store. Local files that match the
deleted path are removed if they exist. Local and remote deletions are done in
parallel, and a failure in one does not affect the other. Deletions also remove
any in-memory state for the deleted object.

This means a client running a local garbage collector inherits the GC's delete
calls locally. Garbage collectors that run remotely do not directly remove
local files, though. The [remote scan](#remote-scan) handles that case.

COPY and RENAME fail with `MirrorError::Unsupported` and never reach the
wrapped store. Either one can overwrite a path the policy mirrors, which breaks
the write-once assumption (see [Guarantees](#guarantees)), and a RENAME also
leaves the source's local copy behind. SlateDB doesn't copy or rename SSTs. Its
only COPY is in WAL cloning, so cloning a database with WAL files needs an
`Admin` built on the unwrapped store. LIST always goes to the wrapped store.

### Remote Scan

An object can be deleted by a client that doesn't go through the mirror, for
example a garbage collector running in another process. It can also be written
through the mirror and then abandoned before anything references it, for
example after a crash, a process restart, or a failed compaction job. The
mirror periodically scans the remote store to find these cases. The first scan runs
when the mirror is built, then every ten minutes by default:

- Snapshot the local file list from disk, not the in-memory entry map, so a
  local file the map lost track of still gets cleaned up.
- Recover each file's object path. The MD5-to-parent map covers every prefix
  the mirror has installed or recovered, so the path usually comes straight
  from the file name. For an unknown prefix, the scan reads the path from
  `.meta`.
- Group the paths by remote parent prefix.
- LIST each distinct parent prefix on the wrapped remote store.
- Delete any local file absent from the remote result.

The snapshot is taken before the LIST, and files are only installed after they
exist remotely, so a file installed during the scan is never mistaken for a
deleted one. Deletions go through the mirror's per-path
[ordering](#ordering), like evictions.

The scan only deletes files after they've been removed remotely. For SlateDB,
this means it won't delete anything younger than the garbage collector's
compacted SST `min_age` setting, and it inherits all of the garbage collector's
rules. It's a cheap way to reuse the garbage collector's logic without running
it in two places.

The scan is not necessary if the garbage collector is running in the same
process, since the GC's delete calls remove local files directly. Users may
disable it by passing `None` to
`ObjectStoreMirrorBuilder::with_remote_scan_interval`.

> [!IMPORTANT]
> This design requires a bucket and endpoint with strongly consistent object
> reads, writes, deletes, and listings. The remote scan treats confirmed remote
> absence as authoritative and may delete the only local copy, so it must be
> disabled when these guarantees are unavailable. In particular:
>
> - Tigris global and dual-region buckets are strongly consistent for requests
>   within one region but eventually consistent across regions. All writers and
>   mirrors running the remote scan must access such a bucket from the same
>   region; Tigris
>   multi-region and single-region buckets provide strong consistency globally.
> - Azure RA-GRS and RA-GZRS secondary endpoints are eventually consistent with
>   the primary. The remote scan must use the primary endpoint and remain
>   disabled while reads are directed to a secondary endpoint.

### SlateDB Policy

`SlateDbMirrorPolicy` holds all of SlateDB's mirroring rules. With the split
above, it fits in one fairly small file.

#### Routing

`SlateDbMirrorPolicy` reads `ObjectStoreCallTag` from the `Extensions` in the
call's options. It ignores the other option fields, except that it returns an
error instead of `Mirror` for a `PutMode::Update` write:

| Call | Route |
|---|---|
| GET or PUT of `*.manifest` | `Observe` (error if the database root doesn't match) |
| Tagged compacted SST in a selected segment, with `tag.retry.is_some()` | `Refetch` |
| Tagged compacted SST in a selected segment | `Local` for reads, `Mirror` for writes |
| Everything else (untagged, WAL, unselected segments) | `Remote` |

#### Manifest Processing

The policy keeps this state behind a mutex:

- The database root, recorded from the first manifest it sees.
- The ID of the newest manifest it has processed.
- `retain`: the SST paths that must not be evicted.
- Decoded checkpoint manifests, cached by ID. Manifests are immutable, so these
  never go stale.

`retain` only controls eviction. It doesn't cause anything to be downloaded,
and an SST in `retain` isn't necessarily on disk. Downloads happen only through
`fetch`.

In short, a newer manifest sets `retain` to its own SSTs plus those of its
active checkpoints, fetches its own SSTs, and evicts anything no longer
retained. An older manifest leaves `retain` alone and fetches whichever of its
SSTs are still retained. That second path is how checkpoint readers get warmed.

`observe` for manifest `M`:

1. Decode `M` and build `warm`: `M`'s SSTs, counting only selected segments.
2. Take the lock and check whether `M` is newer than the newest processed
   manifest (or none has been processed yet). If it is, take references to
   the cached manifests of `M`'s active checkpoints and note which ones aren't
   cached. Release the lock.
3. If `M` is newer, read the uncached checkpoint manifests through
   `mirror.remote()` so the reads don't loop back into `observe`.
4. Take the lock.
5. If `M` is still newer than the newest processed manifest:
   1. Add the checkpoint manifests read in step 3 to the cache.
   2. Build the new `retain`: `warm` plus the selected SSTs of each active
      checkpoint.
   3. `mirror.evict(old_retain - retain)`.
   4. Save the new `retain` and `M`'s ID. Drop cached checkpoint manifests
      that are no longer active.
6. Otherwise, leave `retain` alone and narrow `warm` to `warm & retain`.
7. Release the lock.
8. Call `mirror.fetch(warm)` and await it. Paths that are already local are
   skipped.

The SSTs in a manifest are every SST returned by `ManifestCore::all_sst_views()`.
This includes L0 and compacted SSTs in the root tree and all named segments.
SST IDs found in `ExternalDb.sst_ids` are resolved under the external
database's path; all others are resolved under the database root.

Only newer manifests move `retain` forward, so a late or out-of-order read
can't roll eviction state back. An older manifest only warms SSTs that are
already retained. Anything that neither the latest processed manifest nor one
of its active checkpoints references is eligible for GC, so there's no reason
to download or keep it.

Step 8 fetches only `M`'s own SSTs, so on a newer manifest, SSTs referenced
only by its checkpoints are retained but not downloaded. They're fetched when
something reads the checkpoint's manifest. A `DbReader` opened on a checkpoint
does this (`DbReaderInner::new`). The checkpoint's manifest is usually older
than the latest one, so it takes the step 6 path, and `warm & retain` is the
checkpoint's SSTs minus anything that's no longer retained. A caller holding
an older manifest that isn't a checkpoint may get `NotLocal` for SSTs that are
no longer retained, the same as any read of an unreferenced SST (see
[Ordering](#ordering)).

Evictions are issued in step 5 under the lock, so they reach the mirror in
manifest order. Fetches are issued in step 8, after the lock is released.
`observe` for manifest N+1 can only reach step 8 after it has held the lock,
which is after N's evictions were issued. Together with the mirror's
[per-path ordering](#ordering), this means an eviction for manifest N can't
delete an SST that manifest N+1 retains again.

The reverse race is allowed. `observe` for N can issue its fetch after
`observe` for N+1 has evicted some of the same SSTs, which downloads SSTs that
are no longer retained. The policy never evicts them again, since they're not
in `retain`. Nothing references them either, so the garbage collector deletes
them remotely and the [remote scan](#remote-scan) removes the local copies.

The lock is never held across a remote call. A slow checkpoint manifest read
in step 3 or a slow download in step 8 would otherwise stall every other
manifest read and write, including the writer's manifest PUT. The reads don't need the lock: `M`'s active
checkpoints depend only on `M`, and manifests are immutable, so nothing read in
step 3 can go stale. If another `observe` processes a newer manifest while the
reads are in flight, step 5's re-check fails and `M` takes the step 6 path.
Step 2 keeps references to the cached checkpoint manifests it needs, so a
concurrent `observe` pruning the cache can't leave step 5 missing one. Two
`observe` calls that race on the same new checkpoint may both read it. This is
rare, and a per-ID `OnceCell` could dedupe the reads if it matters.

#### Warming

The mirror is warmed continuously as new `.manifest` files are read and
written. `observe` downloads any missing SSTs synchronously (step 8). This
happens after the `.manifest` call is forwarded to the wrapped store, but
before returning to the caller.

A large compaction job can finish and update the `.manifest` with gigabytes,
or even terabytes of new SSTs. Blocking the manifest update to download the
entire set could take minutes or even hours. To prevent large stalls,
`SlateDbMirrorPolicy::run` waits until the database root is known, then polls
the `.compactions` file for in-flight job output and calls
`mirror.prefetch(outputs)`. The `.manifest` blocking is therefore a final
true-up rather than a complete download of all output from completed
compaction jobs.

This behavior implicitly warms a database when it is first opened. Builders
always read and write manifests in their `build` function. `DbReader`s also
benefit from this approach. As new manifests are polled, the mirror will
download any missing SSTs before returning the manifest. This guarantees that
all reads will come from local disk.

SSTs referenced only by the latest manifest's checkpoints are not warmed from
it, since readers of the latest manifest don't need them. They're still
retained until the checkpoint expires. A reader opened on a checkpoint warms
them by reading the checkpoint's manifest.

#### Garbage Collection

Manifest transitions (step 5) are the fast path. If an old manifest retains an
SST and the new one no longer does, the SST is evicted in the background. This
keeps disk usage low in high-throughput workloads. Without an active deletion
mechanism, even a minute of writes and compaction churn can generate hundreds
of outdated files. This is not a concern for object storage, but is for local
disks.

The mirror's [remote scan](#remote-scan) is the backstop. It handles SSTs that
were written locally and then lost before they were recorded in `.compactions`
or `.manifest` files. Manifest transitions can't discover such files.

#### Segment Support

Segment-based routing requires two changes:

1. `ObjectStoreCallTag` needs a new `segment` field to indicate the segment prefix for routing.
2. `SlateDbMirrorPolicyBuilder` needs a `with_segment_predicate` method to allow users to specify which segments should be mirrored.

The segment field is required because a new SST may not appear in the manifest yet. The policy needs to know the segment prefix to evaluate the predicate and decide whether to mirror the SST.

```rs
pub struct ObjectStoreCallTag {
    // ...

    // Optional segment prefix for routing.
    pub segment: Option<Bytes>,
}
```

`segment` will be set for all reads and writes. This changes `ObjectStoreCallTag` from a `Copy` type to a `Clone` type and changes to a heap allocation. We invoke SST read/writes infrequently enough that we believe this won't cause CPU performance to degrade.

The `TableStore` must be updated to receive the field in its read and write SST functions. This touches a wide range of files, but the changes are mechanical and straightforward.

The predicate receives a manifest and the segment prefix being evaluated. It selects every segment by default. An empty prefix identifies the root segment.

When processing a manifest, the policy evaluates the predicate against the incoming `ManifestCore`. It warms newly selected SSTs before returning the manifest, then evicts SSTs that are no longer selected as part of manifest-driven garbage collection (see above).

For `.compactions` entries, reads, and writes, the predicate receives the last manifest observed by the policy. The segment prefix is passed separately, so the predicate can select a new segment before it appears in the manifest. For example, a date-based predicate can recognize a new `YYYYMMDD` prefix when the day rolls over.

Selected reads are routed to the local mirror, and unselected reads to the wrapped object store. Selected writes are written locally and remotely. Unselected writes go directly to the wrapped object store. Only SSTs in selected segments are retained, so an SST whose segment stops being selected is evicted on the next manifest transition. If a later manifest selects it again, the mirror's [ordering](#ordering) cancels the pending eviction or fetches the SST again.

#### Compactor Checkpoint Retention

We add a new `CompactorOptions::checkpoint_lifetime` configuration that sets
the (currently hardcoded) checkpoint written before compaction inputs are
removed from the manifest. The default stays 15 minutes (the currently
hardcoded value). Manifest-driven garbage collection keeps SSTs referenced by
these checkpoints.

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

The user supplies the mirror as the main object store. SlateDB then applies its
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

On startup, `ObjectStoreMirrorBuilder::build`:

1. Validates options
2. Makes the local directory if it does not exist
3. Acquires the exclusive `LOCK` file lock
4. Removes any `.[tmp_num]` files (incomplete uploads or downloads). Files
   without an MD5 prefix aren't part of the layout and are left alone, so
   pointing the mirror at the wrong directory doesn't delete anything
5. Scans object and `.meta` pairs, deleting entries with a missing partner,
   malformed metadata, a canonical path that does not match the local filename,
   a file size that does not match the metadata (a torn write), or a
   conflicting MD5-to-parent-path mapping
6. Reconstructs the in-memory path and object metadata maps from valid pairs
7. Starts the remote scan and spawns `MirrorPolicy::run`

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

A binding wrapper will be provided for `ObjectStoreMirror` with
`SlateDbMirrorPolicy`.

## Operations

### Performance and Cost

Local hits perform range reads from one whole SST file. A local miss returns
`NotLocal` without remote fallback. `Refetch` validation retries and warming
read remote storage explicitly. WAL and untagged coordination reads always use
remote storage.

Compacted SST writes stream to local and remote storage concurrently and return
after the remote write succeeds and the local file is installed. WAL writes
remain on the remote path.

Cache warming reads the latest manifest and issues one full-object GET
for each referenced SST not already local. It may therefore transfer the full
live compacted data set when starting with an empty cache. Existing local SSTs
avoid those GETs. Warming shares the mirror's download concurrency with
prefetches and refetches and returns only after every SST in its manifest
snapshot is installed or one fails.

Manifest transitions reclaim normal compaction churn without remote SST LISTs.
Every ten minutes by default, the remote scan issues one LIST per distinct
remote parent prefix represented locally. Setting the remote scan interval to
`None` eliminates these periodic LISTs.

### Capacity

The mirror has no maximum-size eviction setting in this proposal.
`SlateDbMirrorPolicy` can't discard an SST and preserve its `Local` contract,
so operators must size the volume to store the entire database, including
in-flight compaction SSTs and ungarbage-collected SSTs.

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

## Alternatives

### SlateDB-Specific Mirror

An earlier draft of this RFC put all of the logic, including manifest decoding,
in one `ObjectStoreMirror` inside `slatedb`. That's less code up front, but it
ties the file layout, downloads, and remote scan to SlateDB's metadata, and
nobody else could use it.

### Mirrored ObjectStore Instead of a Vfs

We considered replacing the `Vfs` with a second `ObjectStore` that holds the
local copies: `builder(remote, mirrored, policy)`. Users wanting a local mirror
would pass `object_store`'s `LocalFileSystem`. This had real appeal:

- `ObjectStore` already covers range reads, atomic puts, deletes, and listing.
  `LocalFileSystem` writes through staging files and renames them into place,
  so the mirror wouldn't manage temporary files itself.
- Tests could use `InMemory`, and DST could reuse `DeterministicLocalFilesystem`
  and `FailingObjectStore` instead of a new `SimulatedVfs`.
- `LocalFileSystem`'s automatic cleanup removes empty directories, which would
  allow a nested layout and drop the MD5 prefix.
- The mirrored store wouldn't have to be local. For example, S3 Express One
  Zone could mirror S3 Standard.

We rejected it because of locking. The mirror has to be the only writer of its
local copies. Its in-memory state (the contains map, metadata, per-path
ordering, and single-flight downloads) assumes nothing else changes them. A
second mirror on the same store would evict copies the first one is serving,
and its startup cleanup would delete the first one's files. The first mirror
would then return `NotLocal`, and since its in-memory map still says the copies
are there, later fetches would skip them until it restarts.

`ObjectStore` can't express the lock this needs. The lock must be released when
the process dies, which an OS file lock gives us for free. A lock object written
with `PutMode::Create` survives a crash and has to be removed by hand. Expiring
it needs a lease and a heartbeat, and renewing a lease needs `PutMode::Update`,
which `LocalFileSystem` doesn't support. Even a working lease isn't safe on its
own: a process that stalls past its lease keeps writing, and `ObjectStore` has
no way to fence arbitrary puts and deletes against an epoch. For a non-local
mirrored store, exclusivity would come down to the operator deploying it
correctly.


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

### Read-Through Route

A `ReadThrough` read route would download the whole object on a local miss and
serve it. It's small to add and would make the crate usable as a plain
whole-file cache. SlateDB doesn't need it, so we'd wait until someone asks.

### Size-Based Eviction

A policy could already enforce a size limit from `run` if `MirrorHandle`
exposed entries with their sizes. LRU (least recently used) eviction would need
a hook on every read, which we'd leave out.

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

- Aligned with the `slatedb-mirror` implementation. Reserved file names
  (`.meta` and `.<digits>` suffixes) can't be mirrored. Added
  `MirrorError::InvalidConfig`, `with_system_clock`, and `with_max_retries`.
  Adjacent downloads of a path share one GET. Documented how route and
  `observe` errors reach callers, how PUT and multipart tees report local
  failures, and that the temporary-file counter is per mirror.
- Warm every manifest returned to a caller, including older checkpoint
  manifests. Only newer manifests advance retention, and older manifests only
  warm SSTs that are already retained. `observe` no longer holds its lock
  while reading checkpoint manifests or fetching SSTs. Replaced the
  "subset of remote" guarantee with eventual cleanup and scoped the mirror to
  write-once paths. Added per-path ordering of fetches and evictions, a
  non-retryable `WriteCommitted` error, and whole-object rules for `Observe`.
  COPY, RENAME, and `Observe` multipart uploads now fail with
  `MirrorError::Unsupported`.
- Split the design into a generic `ObjectStoreMirror` crate and a pluggable
  `MirrorPolicy`, with SlateDB's rules in `SlateDbMirrorPolicy`. Renamed the
  periodic GC setting to `with_remote_scan_interval`.
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
