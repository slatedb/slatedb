# Replace the Object Store Cache with a Local Mirror

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Read and Write Modes](#read-and-write-modes)
  - [Public API](#public-api)
  - [Architecture](#architecture)
  - [Whole-SST Local Layout](#whole-sst-local-layout)
  - [Construction and Cleanup](#construction-and-cleanup)
  - [Explicit Population](#explicit-population)
  - [Read Semantics](#read-semantics)
  - [Write-Through Semantics](#write-through-semantics)
  - [Write-Back Semantics](#write-back-semantics)
  - [Remote Publication Barriers](#remote-publication-barriers)
  - [Delete Semantics](#delete-semantics)
  - [Reconciliation](#reconciliation)
  - [Failure and Restart](#failure-and-restart)
  - [Removing CachedObjectStore](#removing-cachedobjectstore)
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

`CachedObjectStore` sits awkwardly between a cache and a replica. It fetches and
evicts fixed-size parts like a best-effort cache, while some workloads expect it
to keep every live SST on local disk. It does neither job particularly well.

This RFC removes `CachedObjectStore`. Best-effort block caching belongs in
SlateDB's `DbCache`, where Foyer's `HybridCache` stores the blocks a read
actually asks for. A new `MirroredObjectStore` stores whole compacted SSTs and
has independent read and write modes.

The read mode controls local misses. `ReadThrough` serves a missing range from
remote storage and fetches the full SST in the background. `LocalOnly`
treats a missing local SST as an error. Both write modes put every compacted
SST written through the wrapper on local disk. WAL SSTs pass through to remote
storage because SlateDB reads them only during recovery.

The write mode controls remote persistence. `WriteThrough` queues each
compacted SST for upload and waits for that upload to finish. `WriteBack` uses
the same queue but may acknowledge the write once the complete SST has been
written and flushed locally and its upload entry has been queued. The remote
upload may still be pending.

Write-back has a remote publication barrier: every earlier compacted SST write
must reach object storage before a manifest or compactions record is published.
The mirror implements this ordering inside the `ObjectStore` wrapper. SlateDB
has no mirror-specific builder hooks or manifest logic.

A remote DELETE that passes through the mirror removes the matching local SST
immediately. By default, each mirror also reconciles its local `objects/` tree
with remote storage every minute, so remote GC running on another machine
eventually removes stale local SSTs as well. Operators may disable
reconciliation.

## Motivation

`CachedObjectStore` splits objects into fixed-size parts, 4 MiB by default. A
small read on a cold cache fetches the enclosing part before it can complete.
Benchmarks show that this can add hundreds of milliseconds to a point read.
Reducing the part size helps, but creates more files and eviction work.

Admission on writes has the opposite problem. Enabling `cache_on_flush` or
`cache_on_compaction` can fill the bounded evictor queue faster than it can
drain. The cache then drops admission events. This is acceptable for a cache,
but it means the newest SSTs may never make it into the cache. Those SSTs are
often the most likely to be read next, and they may contain newer values or
tombstones that supersede data in older, cached SSTs. The data remains durable
in object storage, but subsequent reads pay for a remote miss.

Eviction is expensive as well. One SST becomes a directory of part files plus
metadata. Removing an SST means unlinking every part and updating an in-memory
index. Startup scans rebuild that index, so a large cache also pays in startup
time and memory.

Foyer already gives SlateDB a better best-effort cache. `DbCache` stores decoded
data blocks, indexes, filters, and stats. A Foyer `HybridCache` can put those
entries on disk, admit only the blocks SlateDB asks for, and use a mature
eviction policy. It avoids fetching a 4 MiB object-store part to answer a 4 KiB
block read.

Foyer does not cover the two whole-SST use cases. Workloads with spatial
locality may want a miss to fetch only the requested range on the foreground
path, then copy the full SST locally in the background. Other deployments
provision local storage for SSTs and want any missing file to fail loudly. A
cache miss going to S3 is not a slow read in those systems; it is a failed read.
Best-effort admission and a large in-memory cache index are disqualifying.

The modes divide as follows:

| Requirement | Read mode | Write mode |
|---|---|---|
| Return a missing SST range from remote, then cache the whole SST | `ReadThrough` | Either |
| Treat a missing local SST as an error | `LocalOnly` | Either |
| Persist each SST locally and remotely before returning | Either | `WriteThrough` |
| Acknowledge a compacted SST from the local queue and upload it asynchronously | Either | `WriteBack` |

Best-effort hot block caching is separate from both choices. It belongs in
`DbCache`, backed by Foyer `HybridCache`.

Foyer and the mirror may be layered. Foyer caches decoded blocks; the mirror
stores whole SSTs. The read mode determines whether a local miss falls through
to remote storage or reports a broken invariant. The write mode determines when
an SST write is acknowledged.

## Goals

- Remove the part-based `CachedObjectStore` and its eviction machinery.
- Use Foyer for best-effort block caching and the mirror for whole-SST locality.
- Serve read-through misses without waiting for the background cache fill, and
  keep every newly written compacted SST local instead of dropping admission.
- Let deployments reject local SST misses instead of silently reading
  remotely.
- Offer both synchronous remote persistence and concurrency-limited
  asynchronous write-back.
- Preserve remote consistency by publishing data before metadata that references
  it.
- Support removing local SSTs deleted by remote GC, whether the DELETE passes
  through this mirror or occurs on another machine.
- Keep the mirror behind the standard `ObjectStore` interface so SlateDB does
  not need mirror-specific configuration or lifecycle code.

## Non-Goals

- Replace `DbCache`, `FoyerCache`, or `FoyerHybridCache`.
- Block a read-through miss on the background full-SST download.
- Guarantee that read-through mode has a complete local copy of every live SST.
- Automatically preload or validate a complete existing database in local-only
  mode. Reconciliation only removes local SSTs confirmed absent remotely; a
  normal local miss remains an error and population must be requested
  explicitly.
- Make local disk loss transparent to an in-flight local-only read.
- Recover unpublished write-back work after a process crash. Queued staging
  files are safe to discard because the publication barrier prevents metadata
  from referencing them.
- Coordinate multiple `MirroredObjectStore` instances through one mirror root.
  A root belongs to one live wrapper.
- Change the SlateDB manifest, compactions, WAL, or SST wire formats.
- Mirror WAL SSTs, manifests, compactions records, GC boundaries, or arbitrary
  objects. WAL and untagged coordination traffic passes directly to the wrapped
  store.

## Design

### Read and Write Modes

The mirror exposes two independent choices:

| Read mode | Tagged compacted SST miss | Local guarantee |
|---|---|---|
| `ReadThrough` | Return the requested remote range immediately and fetch the full SST in the background | Best effort |
| `LocalOnly` | Return `LocalMirrorError` | No remote fallback for tagged compacted SST reads |

| Write mode | Tagged compacted SST write completes after | Remote behavior |
|---|---|---|
| `WriteThrough` | The complete SST is durable locally and remotely | The remote write is part of the foreground operation |
| `WriteBack` | The complete compacted SST is in the local upload queue | Upload asynchronously, with a barrier before publishing remote metadata |

All four combinations are valid. Both write modes store newly written compacted
SSTs as whole local files and send them through the same upload queue. The read
mode determines what happens if a local file is later absent. With
`ReadThrough` plus `WriteBack`, remote fallback is available only after a
queued SST has uploaded. Losing its queued staging file poisons the upload
queue because no remote copy exists yet.

The mirror recognizes compacted SST operations through `ObjectStoreCallTag`.
Tagged WAL operations and untagged operations are never read from or written
to the local mirror. They are forwarded to the wrapped store, although
write-back may hold a manifest or compactions PUT at the remote publication
barrier described below.

### Public API

`MirroredObjectStore` is a normal `ObjectStore` implementation. Users construct
it around their remote store and pass it through the existing
`DbBuilder::new`/`Db::builder` object-store parameter. SlateDB does not gain a
mirror setting or a mirror-specific builder method.

```rust
pub enum MirrorReadMode {
    ReadThrough,
    LocalOnly,
}

pub enum MirrorWriteMode {
    WriteThrough,
    WriteBack,
}

pub struct MirroredObjectStoreOptions {
    pub root_folder: PathBuf,
    pub read_mode: MirrorReadMode,
    pub write_mode: MirrorWriteMode,
    pub prefetch_concurrency: usize,
    pub upload_concurrency: usize,
    pub reconciliation_interval: Option<Duration>,
}

impl MirroredObjectStoreOptions {
    pub fn new(root_folder: impl Into<PathBuf>) -> Self;
    pub fn with_read_mode(self, mode: MirrorReadMode) -> Self;
    pub fn with_write_mode(self, mode: MirrorWriteMode) -> Self;
    pub fn with_prefetch_concurrency(self, concurrency: usize) -> Self;
    pub fn with_upload_concurrency(self, concurrency: usize) -> Self;
    pub fn with_reconciliation_interval(self, interval: Option<Duration>) -> Self;
}
impl MirroredObjectStore {
    pub async fn new(
        remote: Arc<dyn ObjectStore>,
        options: MirroredObjectStoreOptions,
    ) -> Result<Self, Error>;
}

#[async_trait]
impl ObjectStore for MirroredObjectStore {
    // Standard ObjectStore methods delegate to local and remote storage.
}
```

`ReadThrough` and `WriteThrough` are the defaults. The reconciliation interval
defaults to `Some(Duration::from_secs(60))`. Passing `None` disables
reconciliation; construction rejects `Some(Duration::ZERO)`:

```rust
let remote: Arc<dyn ObjectStore> = Arc::new(
    AmazonS3Builder::from_env()
        .with_bucket_name("my-bucket")
        .build()?,
);
let mirror = Arc::new(
    MirroredObjectStore::new(
        remote,
        MirroredObjectStoreOptions::new("/var/lib/slatedb/my-db")
            .with_read_mode(MirrorReadMode::ReadThrough)
            .with_write_mode(MirrorWriteMode::WriteThrough),
    )
    .await?,
);
let db = Db::builder(db_path, mirror).build().await?;
```

`Db::close()` requires no mirror-specific follow-up. Its default close options
perform a final memtable flush and publish the resulting metadata. The mirror's
publication barrier makes that metadata PUT wait for every earlier queued SST,
so close returns only after every SST referenced by the closed database state
has reached remote storage. `close_with_options()` can disable the final flush;
that form does not provide this guarantee. Any later, unpublished compaction
output may still be discarded after a crash.

### Architecture

The user supplies the mirror as the main object store. SlateDB then applies its
existing internal wrappers. The base-to-outer construction order is:

```text
S3ObjectStore -> MirroredObjectStore -> InstrumentedObjectStore -> RetryingObjectStore
```

Requests travel in the opposite direction:

```text
RetryingObjectStore -> InstrumentedObjectStore -> MirroredObjectStore -> S3ObjectStore
```

This differs slightly from putting instrumentation outside retrying. SlateDB
intentionally puts `InstrumentedObjectStore` inside `RetryingObjectStore` so
each retry attempt is counted. Metrics record calls into the mirror, not the
extra remote GET that a read-through miss starts in the background. The mirror
therefore owns retries and metrics for background fills, uploads, and
reconciliation.

`LocalMirrorError` must map to a non-retryable `object_store::Error` variant.
Otherwise SlateDB's outer retry wrapper would turn a local-only miss or a
permanent local-disk failure into an unbounded retry loop. This is a wrapper
integration detail, not a mirror-specific branch in SlateDB.

The mirror applies the local data path only to calls carrying an
`ObjectStoreCallTag` whose `sst_type` is `Compacted`:

```text
tagged compacted GET --> MirroredObjectStore --> local hit ------> caller
                            |
                            +--> miss + ReadThrough --> remote range -> caller
                            |                              |
                            |                              +--> background full GET
                            |                                   --> staging --> local
                            |
                            +--> miss + LocalOnly -------> LocalMirrorError

tagged compacted PUT -----------------------------------> local upload queue
tagged WAL or untagged operation -----------------------> wrapped remote store
```

The tag tells the mirror whether to bypass a WAL or mirror a compacted SST,
which component issued the request, and whether the read is a retry after
validation failure. A validation retry invalidates the local compacted SST
before the request continues.

WAL operations, reads and writes of manifests, compactions records, and GC
boundaries, along with LIST, untagged HEAD, copy, and other untagged operations,
always go to the wrapped remote store. In particular, a GC boundary check can
never be answered from the mirror, even if the mirror directory happens to
contain a file at the same encoded path. The wrapper uses the tag, not the
object path, to decide which reads and writes are compacted SST operations.

Because the wrapper is supplied through the normal `ObjectStore` parameter,
the same type works with `Db`, `DbReader`, and `Compactor`. A mirror root belongs
to one live `MirroredObjectStore`. Components for the same database in the same
process may share the wrapper through `Arc`; separate wrappers, including
wrappers in separate processes, must use separate roots. This ownership rule is
a caller contract.

### Whole-SST Local Layout

Each mirrored SST maps to one local file. The full object-store path is encoded
below `objects/`; this preserves compacted, clone, and split namespaces without
allowing a remote path to escape the local root.

```text
<mirror-root>/
  objects/<encoded-object-store-path>
  staging/<operation-id>
```

For a database rooted at `tenant/db`, compacted SST paths decode to paths such
as `tenant/db/compacted/<ulid>.sst`. Clone and split paths arrive as full
object-store paths, so the mirror does not assume every physical SST belongs to
the current database root.

Each file stores the SST body followed by a compact footer containing the
body length, checksum, attributes, and cached `ObjectMeta`. Keeping the body at
offset zero makes range reads direct, while the footer lets tagged local GETs
preserve object metadata without an in-memory index or a second sidecar file.

An SST becomes generally readable locally only after its complete body and
footer are written to `staging/` and atomically renamed into `objects/`.
Read-through fills do this after the full remote GET completes. For writes, the
complete flushed staging file is registered in an in-memory path-to-sequence
map and submitted to the upload queue. Reads through this wrapper can use that
complete staging file while its upload is pending. Partial downloads and
multipart uploads are never visible as live SSTs.

After a queued SST uploads successfully, the mirror records the remote metadata
in its footer, renames the staging file into `objects/`, flushes the parent
directory, and only then marks its sequence resolved. There is no durable
metadata for the queue.

### Construction and Cleanup

`MirroredObjectStore::new` removes abandoned files under `staging/` before
starting background work. It does not alter `objects/` or read or interpret a
SlateDB manifest.

Deleting all staging files at construction is safe. The mirror does not forward
a manifest or compactions PUT until every earlier asynchronous SST upload has
completed and its local file has moved into `objects/`. A file left under
`staging/` after a crash therefore cannot be referenced by published remote
metadata. At worst, remote storage also contains an unreferenced SST whose
upload completed just before the crash.

This cleanup relies on exclusive ownership by contract. Constructing another
wrapper against the same root while one is live is unsupported: the new wrapper
could delete active staging files or race local publication. The implementation
does not attempt to detect that misuse.

This makes the two read modes deliberately simple. `ReadThrough` can start with
an empty directory and populate it on demand. `LocalOnly` assumes required
SSTs were written through this mirror or populated explicitly. Opening an
existing remote database with an empty local-only mirror will fail on the first
missing SST.

SlateDB does not coordinate publication barriers or root ownership on the
mirror's behalf.

### Explicit Population

There are three ways to populate the mirror:

1. Any successful tagged compacted SST write installs the complete local SST.
2. A tagged compacted `ReadThrough` miss schedules a full-SST background fill.
3. `CacheManager` can explicitly request SST population and wait for atomic
   publication.

The existing `DbCacheManagerOps::warm_sst` cannot quite provide the third
contract today. It warms decoded block-cache entries by issuing tagged range
reads. With a read-through mirror, those reads incidentally schedule a full-SST
fill, but `warm_sst()` may return before that fill completes. It
also does no object-store I/O when no `DbCache` is configured.

This RFC adds an explicit population hint to `GetOptions::extensions`, alongside
the required compacted `ObjectStoreCallTag`:

```rust
pub enum ObjectStoreCacheHint {
    Populate,
}
```

This is separate from `ObjectStoreCallTag` because the tag describes where an
SST call came from and why it is being retried; the hint requests a one-off
cache action. Keeping classification and action separate avoids changing the
meaning of ordinary tagged reads.

`CacheTarget::Object` makes `warm_sst()` issue a tagged compacted GET with this
hint. A mirror that recognizes it downloads the complete SST, writes and
flushes the staging file, atomically publishes it, and only then completes the
call. The hint explicitly authorizes that remote download even in `LocalOnly`
mode; ordinary tagged local-only misses still fail. Other object-store wrappers
may ignore the hint. A `Populate` hint without a compacted
`ObjectStoreCallTag` is forwarded without local admission.

`warm_sst()` processes `CacheTarget::Object` even when no `DbCache` is
configured. The existing index, filter, stats, and data targets retain their
current block-cache behavior. The population GET may request a small range;
the mirror performs the full download internally and returns only the range the
caller requested.

### Read Semantics

Tagged compacted SST reads check the whole-file mirror first:

```text
GET compacted/01....sst
    |
    +-- complete local file --> serve requested range
    |
    +-- missing/corrupt
            |
            +-- ReadThrough --> serve requested range from remote
            |                    and prefetch the full SST asynchronously
            |
            +-- LocalOnly ----> LocalMirrorError
```

On a read-through miss, the mirror forwards the original `GetOptions` to the
remote store. The requested range is returned directly to the caller; the
foreground request does not wait for a full-SST download. After the remote GET
succeeds, the mirror schedules a separate full-SST GET. That download writes
to `staging/` and atomically renames the complete SST into place.

Background fills are single-flight by SST path. Concurrent foreground misses
may each issue the range GET they need, but they share one full-SST download.
The prefetch scheduler bounds active downloads by concurrency. Waiting for a
permit happens in the background and never delays the range response.

If a background download fails, the foreground read is unaffected. The mirror
records the failure and tries again on a later miss. A concurrent delete for
the same path prevents the staging file from being published.

In local-only mode, a missing SST simply returns `LocalMirrorError`. The
wrapper does not check remote storage or schedule a repair unless the call
carries the explicit `Populate` hint. The missing SST may mean that the database
was not populated through this mirror, another process published a new SST, the
local file was deleted, or the disk is corrupt. Those causes have the same
tagged-read contract: no remote fallback.

When SlateDB retries an SST read with a validation-failure tag, the mirror
quarantines the local file. Read-through mode treats the request as a miss;
local-only mode returns `LocalMirrorError`. The mirror also validates the stored
length and checksum from its local footer. In either read mode, a pending
write-back SST whose staging file disappears during the process lifetime
poisons the upload queue because the asynchronous upload no longer has a
source file.

Tagged WAL reads and untagged reads bypass the mirror regardless of read mode.
This includes WAL recovery and every manifest, compactions, and GC boundary
read.

A tagged compacted `get_opts` call with `head = true`, such as `TableStore` SST
metadata lookup, can use the cached footer. A tagged WAL metadata lookup and
the untagged `ObjectStore::head` method still pass through to remote storage.

### Write-Through Semantics

Write-through is the default. It makes both the local mirror and remote storage
part of the write contract while keeping remote storage authoritative.

For a tagged compacted SST, the mirror uses the same staging file, upload queue,
and worker path as write-back. Once the complete local file is queued,
write-through waits for that entry's upload result. It returns only after the
remote write succeeds and the local file is promoted into `objects/`. This
keeps one upload implementation for both modes; only the point at which the
caller is released differs.

If the upload fails permanently, write-through removes the unpublished staging
file and returns the remote error, allowing SlateDB's outer retry wrapper to
retry the PUT. Because the failed write was never acknowledged, its sequence is
retired and does not block later publication barriers. If the remote write
succeeds but local promotion fails, the wrapper returns `LocalMirrorError`; the
remote SST remains as an unreferenced orphan.

Tagged WAL and untagged writes pass through to remote storage. Manifest,
compactions, WAL, and GC boundary PUTs therefore retain their existing
conditional-write and fencing behavior without a second local commit point.

Tagged compacted multipart uploads tee bytes into one local staging file.
`complete()` is the commit point; `abort()` removes the staging file. There are
no persistent part files to leak. WAL and untagged multipart uploads, copy, and
rename operations pass through to the wrapped store.

### Write-Back Semantics

Write-back returns after a tagged compacted SST is complete locally and entered
in the in-memory upload queue. It does not wait for remote persistence. Upload
concurrency is bounded, but queued bytes are not: local disk capacity is the
limit, and an ENOSPC error fails the write instead of dropping the entry.

One compacted SST PUT in either write mode proceeds as follows:

1. Assign a monotonically increasing operation sequence.
2. Stream the payload and its provisional metadata footer into `staging/`.
3. Flush the complete staging file and insert an in-memory entry containing the
   sequence, object path, staging path, remote PUT mode and attributes, byte
   length, and checksum.
4. Submit the entry to the upload queue. Write-back returns success here;
   write-through waits for this entry's result. Tagged reads in this process
   can find the complete staging file through the in-memory path map.
5. An upload worker reads the staging file and writes it to the remote store.
6. After remote success, record the returned metadata in the footer, flush the
   file, atomically rename it into `objects/`, flush its parent directory,
   and advance the highest contiguous resolved sequence. A write-through
   caller then receives the remote `PutResult`.

Workers upload independent immutable SSTs with bounded concurrency. A queued
staging file cannot be deleted while its entry exists. If an acknowledged
write-back file disappears during the process lifetime, the queue is poisoned
and metadata publication stops. The same loss on a write-through entry is
returned to its waiting caller and the unacknowledged sequence is retired.

The in-memory queue deliberately does not survive a crash. The next wrapper to
use the root deletes every abandoned staging file during construction. If a
remote upload had not completed, no published manifest or compactions record
can reference the SST. If it had completed, the remote SST is merely an
unreferenced orphan and normal GC can remove it later.

Write-back also cannot return a remote ETag or version before the upload occurs.
It is therefore used only for tagged immutable compacted writes whose callers
do not need those fields in the immediate `PutResult`. A conditional write or
any call that requires a remote version waits for its queue result as
write-through does.

The upload worker owns the retry policy for work acknowledged by write-back
because its requests do not pass back through SlateDB's outer
`RetryingObjectStore`. A permanent write-back error poisons the upload queue,
blocks metadata publication, and surfaces through subsequent compacted writes,
the waiting metadata PUT, and health metrics for the rest of the process
lifetime. A write-through error is instead returned to its waiting caller so
the outer wrapper can retry it. Restart ignores unpublished compacted SST work
and reconstructs database state from the remote manifest and pass-through WALs.

### Remote Publication Barriers

Write-back must flush a compacted SST to object storage before updating a remote
manifest that can reference it. A manifest that points at a missing SST is
corruption, not eventual consistency.

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

A successful sequence is complete only after the remote upload succeeds and the
local file has moved from `staging/` to `objects/`. A failed write-through
sequence may also be retired after its waiting caller receives the failure,
because no metadata operation can reference a write that did not return
success. A failed write-back sequence cannot be retired because its caller was
already acknowledged; it poisons the queue instead. Because a metadata PUT
waits for every earlier sequence to complete or retire, a file left in
`staging/` can never be referenced by published remote metadata and needs no
crash recovery.

The barrier is conservative. It may wait for an unreferenced compaction output,
but it never needs to decode a manifest in the object-store wrapper. The extra
wait is preferable to duplicating manifest interpretation in the write queue.

WAL reads and writes bypass the mirror even when the main and WAL paths use the
same object store. They therefore do not enter the asynchronous queue or need a
barrier shared across the main and WAL stores.

Manifests, compactions records, and GC boundary updates are always written
through. This preserves remote conditional-write and fencing semantics.

### Delete Semantics

`ObjectStore::delete` does not carry `ObjectStoreCallTag`, so the mirror maps
the object path to a possible local compacted SST file. A delete waits for any
earlier upload of the same path, deletes the remote object, and then removes the
local file if one exists. A failed local unlink wastes disk but does not
resurrect the remote object. Reconciliation retries it when enabled. An
in-flight read keeps its file handle until the read ends.

The wrapper does not interpret manifests or run a separate reachability
planner. A DELETE that passes through the wrapper remains the fastest cleanup
path when SlateDB GC uses this wrapper. Deletes for non-SST paths simply pass
through because no matching local file exists.

### Reconciliation

A DELETE issued on another machine does not pass through this wrapper. By
default, `MirroredObjectStore` therefore runs a reconciliation task every minute
in both read modes. `Some(interval)` enables the task and must be non-zero;
`None` disables it. Each enabled mirror randomizes its initial delay to spread
remote LIST requests across the fleet. Dropping the wrapper cancels its task;
reconciliation does not delay `Db::close()`.

One reconciliation pass proceeds as follows:

1. Snapshot the decoded object-store paths of complete files currently under
   `objects/`. Files created after this snapshot are not candidates in this
   pass. Files under `staging/` are never candidates.
2. Group the snapshot by remote parent prefix and LIST each distinct prefix on
   the wrapped remote store. This avoids listing unrelated bucket contents and
   covers compacted SSTs reached through clone and split paths.
3. For each local path absent from its remote LIST result, issue a remote HEAD
   to confirm the object is missing. HEAD confirmations share the background
   remote-read concurrency limit with prefetches.
4. If HEAD returns `NotFound`, unlink the local file. A concurrent local unlink
   is treated as success. Any other LIST, HEAD, or local I/O error preserves the
   local file and is retried on the next pass.

Taking the local snapshot before the remote LIST prevents a newly published
local file from being evaluated against an older remote snapshot. The
confirmatory HEAD prevents an incomplete or eventually consistent LIST result
from deleting an object that still exists remotely. Compacted SST paths are
immutable and never reused, so no generation comparison is required.

Remote storage remains authoritative in both read modes. A confirmed remote
absence therefore authorizes local deletion even in `LocalOnly`. If remote
storage loses an SST that published metadata still references, reconciliation
will remove the local copy and the next tagged read will return
`LocalMirrorError`; preserving a private local copy would make the mirror an
authoritative replica, which is outside this RFC. Reconciliation does not
delete remote objects, decode manifests, or remove SSTs earlier than remote GC.

### Failure and Restart

Failure handling depends on the selected read and write modes, but no
combination exposes a partial local file:

| Failure | Behavior |
|---|---|
| Local disk full during read-through fill | Keep serving the foreground range from remote storage; record the fill failure |
| Local disk full during a compacted SST write | Fail the write under either mode; never drop an acknowledged queue entry |
| Remote write fails in write-through | Remove the queued staging file and return the error so the outer wrapper can retry |
| Remote upload fails in write-back | Retry, then poison the queue on a permanent failure |
| Remote-durable compacted SST is missing in read-through mode | Serve the requested remote range and schedule a full-SST fill |
| Remote-durable compacted SST is missing in local-only mode | Return `LocalMirrorError`; do not access remote storage |
| Queued write-back staging file disappears while running | Return `LocalMirrorError` and poison the upload queue because the upload has lost its source |
| Local SST fails validation | Quarantine it and apply the same read-mode rules as a missing file; poison the queue if it is pending upload |
| Background full-SST GET fails | Keep the foreground result; retry on a later miss |
| Reconciliation LIST or HEAD fails | Preserve the affected local files, record the failure, and retry on the next pass |
| Reconciliation confirms a remote SST is absent | Remove the complete local file in either read mode |
| Reconciliation cannot unlink a stale local SST | Retain the file, record the failure, and retry on the next pass |
| Process crashes with staging files | Delete them when the next wrapper is constructed; published remote metadata cannot reference them |
| Process crashes after an SST upload but before local promotion | Delete the staging file on reconstruction; the remote SST is an unreferenced orphan eligible for normal GC |
| Machine and local disk are lost in write-through | Rebuild from remote storage |
| Machine and local disk are lost in write-back | Rebuild the last published state from remote storage; unpublished compaction work is discarded |

Remote storage remains sufficient to repopulate a write-through mirror, either
by using `ReadThrough` or explicit population. The same is true for write-back:
the publication barrier makes queued staging files unnecessary for recovery,
while remote metadata continues to describe a complete remote state.

### Removing CachedObjectStore

The following code is removed:

- `slatedb::cached_object_store::CachedObjectStore`, its builder, policies,
  storage traits, part files, evictor, and cache-specific metrics,
- `ObjectStoreCacheOptions`,
- `Settings::object_store_cache_options`,
- `cache_on_flush`, `cache_on_compaction`, `part_size_bytes`,
  `max_cache_size_bytes`, `preload_disk_cache_on_startup`, `scan_interval`, and
  `max_open_file_handles`,
- startup preload logic specific to the part cache.

`ObjectStoreCallTag` remains public. It is useful to the mirror, prefetching
stores, and user-supplied object-store policies independently of the removed
cache.

The old cache layout is not migrated into the mirror. Part files cannot be
published as complete local SSTs. Operators should allocate a new mirror
root and delete the old cache directory after the migration.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

KV semantics do not change. Read-through mode preserves remote fallback on a
local miss. Local-only mode adds explicit mirror health and missing-object
errors.

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

- [x] Manifest format
- [x] Checkpoints
- [x] Clones
- [x] Garbage collection
- [x] Database splitting and merging
- [x] Multi-writer

Formats are unchanged. Manifest, compactions, and GC boundary operations bypass
the mirror, so multi-writer fencing, boundary checks, discovery, and conditional
PUT errors are still observed remotely. GC deletes remove the matching local
SST when they pass through this mirror. When enabled, periodic reconciliation
removes the same SST when GC runs on another machine.

### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [x] Distributed compaction
- [x] Compactions format

Compaction formats are unchanged. Outputs are mirrored as whole files and
compactions metadata is published after the upload barrier.

### Storage Engine Internals

- [x] Write-ahead log (WAL)
- [x] Block cache
- [x] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

The block cache remains the general-purpose best-effort cache. The mirror stores
only tagged compacted SSTs as whole local files. WAL SSTs always use remote
storage. Read-through population is best effort; local-only mode populates only
when explicitly requested.

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

Bindings lose the old cache settings. The mirror is available only in APIs that
can supply a custom `ObjectStore`; this RFC does not add binding-specific
configuration or CLI commands.

## Operations

### Performance and Cost

Local hits perform range reads from one whole SST file. A read-through miss
issues the requested range GET on the foreground path and a separate full-SST GET
in the background. Foreground latency waits only for the requested range, while
remote bandwidth includes both requests. Later reads use the local file after
the background download finishes.

Local-only mode performs no remote fallback for tagged compacted SST reads. It
has no automatic warmup cost, but it fails when the requested whole SST is
absent. Explicit population may still read remote storage. WAL and untagged
coordination reads always use remote storage in either mode.

Write-through adds local disk bandwidth to every tagged compacted SST write and
waits for the shared queue's upload result. Write-back moves most remote latency
off compacted SST writes, but manifest and compactions publication still wait
at a remote barrier. WAL writes remain on the foreground remote path.

Whole-SST storage removes the 4 MiB miss amplification, part metadata,
per-part file handles, and per-part eviction work. It can increase local bytes
relative to a hot-block cache because completeness is the contract.

In read-through mode, remote request cost is highest during warmup because a
tagged compacted SST miss produces a range GET plus a full GET. Local-only mode
issues no remote fallback GET on such a miss.

Each reconciliation pass scans complete local objects, issues one remote LIST
per distinct parent prefix, and issues HEAD only for paths missing from a LIST
result. Mirrors randomize their initial delays to spread this work across the
fleet. Reconciliation never runs on the foreground read or write path.

At the default one-minute interval, and using the S3 Standard rate of $0.005 per
1,000 PUT, COPY, POST, or LIST requests, one listed prefix produces 43,200 LIST
requests in a 30-day month and costs $0.216 per mirror per month. With `P`
distinct prefixes and `M` mirrors, the monthly LIST cost is `$0.216 * P * M`.
Confirmatory HEAD requests for candidates missing from LIST, data transfer, and
provider-specific charges are additional. Setting the reconciliation interval
to `None` eliminates this periodic LIST cost.

### Capacity

The mirror has no maximum-size eviction setting in this proposal. A
read-through mirror may remain partially populated if a background fill runs
out of space; the foreground read still succeeds against remote storage. A
local-only mirror cannot discard an SST and preserve its no-fallback contract.

Operators size the volume for mirrored SSTs, temporary staging, and the
write-back backlog. The backlog has no separate byte limit, so disk capacity is
the admission limit. Abandoned staging files consume space only until the next
wrapper construction clears `staging/`. Complete SSTs deleted remotely on
another machine may consume local space until the next successful
reconciliation pass. If reconciliation is disabled, they remain until a DELETE
passes through the mirror or the root is rebuilt.

### Observability

The initial metric set is:

- `slatedb.object_store_mirror.local_hits`
- `slatedb.object_store_mirror.local_misses`
- `slatedb.object_store_mirror.foreground_remote_gets`
- `slatedb.object_store_mirror.prefetch_ssts`
- `slatedb.object_store_mirror.prefetch_bytes`
- `slatedb.object_store_mirror.prefetch_failures`
- `slatedb.object_store_mirror.staging_bytes`
- `slatedb.object_store_mirror.pending_upload_ssts`
- `slatedb.object_store_mirror.pending_upload_bytes`
- `slatedb.object_store_mirror.resolved_upload_sequence`
- `slatedb.object_store_mirror.upload_failures`
- `slatedb.object_store_mirror.local_delete_failures`
- `slatedb.object_store_mirror.reconciliation_runs`
- `slatedb.object_store_mirror.reconciliation_list_requests`
- `slatedb.object_store_mirror.reconciliation_head_requests`
- `slatedb.object_store_mirror.reconciliation_deleted_ssts`
- `slatedb.object_store_mirror.reconciliation_failures`
- `slatedb.object_store_mirror.missing_sst_errors`
- `slatedb.object_store_mirror.health`

Logs include the mirror root, read and write modes, upload barrier waits,
reconciliation summaries, poison errors, and failed local operations. They do
not log one line per successful background fill, upload, or reconciled object at
the default level. These metrics belong to the mirror itself: SlateDB's outer
instrumentation cannot see background GETs, uploads, or reconciliation requests
initiated inside the wrapper.

### Compatibility

- **Object-store data.** Manifest, compactions, WAL, and SST formats are
  unchanged. A database can move between mirrored and non-mirrored operation.
- **Public API.** Removing `CachedObjectStore` and
  `Settings::object_store_cache_options` is a breaking API and configuration
  change. `MirroredObjectStore` is supplied through the existing object-store
  parameter; no `DbBuilder` methods change.
- **Local files.** The old part-cache layout is not compatible with the new
  whole-SST mirror layout.
- **Rolling upgrades.** Mixed versions remain compatible through remote object
  storage. Each live wrapper must use a distinct mirror root.
- **Bindings.** Generated Go, Python, Java, and Node settings lose the old cache
  options. They do not gain mirror settings in this RFC.

## Testing

- Unit tests:
  - local path mapping for compacted, clone, and split SSTs,
  - staging-file publication and cleanup,
  - local file removal after an SST delete,
  - WAL and untagged GET, PUT, multipart, HEAD, LIST, and copy passthrough,
  - queued staging-file publication, in-memory lookup, upload concurrency, and
    contiguous upload completion for both write modes,
  - startup removes every abandoned staging file without altering `objects/`,
  - reconciliation defaults to one minute, `None` disables its background task,
    and a zero interval is rejected,
  - reconciliation snapshots only complete files under `objects/`, groups them
    by decoded remote parent prefix, and ignores staging files,
  - reconciliation removes a local SST only when it is absent from LIST and a
    confirmatory HEAD returns `NotFound`,
  - a successful confirmatory HEAD preserves an SST omitted from LIST,
  - LIST, HEAD, and local unlink failures preserve the local file for a later
    pass,
  - require a compacted `ObjectStoreCallTag` before honoring a `Populate` hint,
  - publication barrier ordering for asynchronous compacted SST uploads,
  - WAL and fence PUTs bypass the mirror in both write modes.
- Integration tests:
  - pass the mirror through the normal `Db::builder` object-store parameter and
    verify the effective wrapper order,
  - miss in read-through mode and verify the requested range returns before the
    background full-SST GET completes,
  - verify the background GET installs one complete local file and later reads
    hit it,
  - issue concurrent range misses and verify they share one background
    full-SST GET,
  - fail a background GET and verify the foreground range still succeeds,
  - read and write WAL SSTs and verify they reach remote storage without
    creating local files,
  - write a compacted SST in each mode and verify both use the same upload
    queue, write-through waits for its result, and write-back returns after
    local enqueue,
  - read and write manifests, compactions records, and GC boundaries and verify
    each operation reaches remote storage without creating a local file,
  - verify a stale local SST cannot affect a GC boundary check,
  - exhaust local disk capacity and verify neither mode drops an acknowledged
    queue entry,
  - open an existing database with an empty local-only mirror and verify the
    first tagged compacted SST miss returns `LocalMirrorError`,
  - assert tagged compacted local-only hits and misses issue no remote fallback
    GET,
  - use `CacheTarget::Object` without a `DbCache` and verify `warm_sst()` waits
    until the complete SST is atomically installed,
  - use the same population target in local-only mode and verify the explicit
    request is allowed to fetch remotely,
  - run GC through the mirror and verify each remote SST delete removes its
    local file,
  - run GC on another machine's raw remote store, run reconciliation against
    the shared remote store, and verify the stale local SST is removed in both
    read modes,
  - publish a new local SST after reconciliation takes its local snapshot and
    verify the pass does not consider that file for deletion,
  - restart with queued write-back files under `staging/` and verify startup
    deletes them without changing published remote state,
  - call default `Db::close()` and verify its final metadata barrier drains all
    earlier queued SSTs; verify disabling the close flush removes that guarantee,
  - rebuild a write-through mirror through read-through misses after deleting
    its local root.
- Fault-injection/chaos tests:
  - crash before and after local enqueue, remote SST PUT, staging-to-objects
    promotion, and manifest PUT,
  - inject ENOSPC, partial downloads, corrupt local files, permanent upload
    errors, and failed deletes,
  - delete a queued write-back staging file and verify the upload queue is
    poisoned,
  - crash after remote upload but before local promotion and verify restart
    leaves only an unreferenced remote orphan,
  - inject incomplete LIST results, transient HEAD failures, and concurrent
    local promotion during reconciliation and verify no remotely present SST is
    unlinked,
  - verify no remote manifest ever references an SST that has not completed its
    remote upload.
- Deterministic simulation tests:
  - model concurrent foreground misses, background fills, writes, deletes,
    publication barriers, and staging cleanup after restart,
  - verify read-through mode never exposes a staging file or blocks a range
    response on a full-SST fill,
  - verify a tagged compacted local-only miss never issues a remote GET while an
    explicit tagged compacted `Populate` request may do so,
  - verify `published_remote_manifest_ssts ⊆ durable_remote_ssts` in write-back
    mode.
- Formal methods verification:
  - add a small FizzBee model for the write-back queue, staging cleanup after
    restart, and the remote publication barrier before enabling write-back
    outside experimental status.
- Performance tests:
  - point-read latency on cold and warm Foyer and in both mirror read modes,
  - foreground range latency while background full-SST downloads run,
  - flush and compaction throughput in write-through and write-back modes,
  - reconciliation LIST and confirmatory HEAD request cost for large mirrors,
  - local delete throughput and startup time versus the old part cache.

## Rollout

1. Add `MirroredObjectStore` as a standalone wrapper with whole-SST
   read-through, foreground range passthrough, and the default write-through
   mode. Mark the API experimental.
2. Verify `Db`, `DbReader`, and `Compactor` accept it through their existing
   object-store parameters without mirror-specific builder code.
3. Add local-only reads and test that misses do not reach remote storage.
4. Add explicit population through `CacheTarget::Object`,
   `ObjectStoreCacheHint::Populate`, and tagged compacted SST reads.
5. Add metrics, startup staging cleanup, local cleanup on SST deletes, and
   periodic remote reconciliation.
6. Add write-back behind an explicit experimental option after the upload-queue
   and publication-barrier model is checked.
7. Document the wrapper order, the common upload queue, single-owner root
   contract, reconciliation interval and opt-out, reconciliation cost, and WAL
   passthrough.
8. Deprecate `CachedObjectStore` and its settings for one release if required by
   compatibility policy, then remove the implementation and old metrics.

## Alternatives

**Keep `CachedObjectStore`.** The current cache works for workloads with enough
spatial locality and a tolerant latency budget. It still duplicates Foyer's
job, amplifies misses to part boundaries, drops admission under pressure, and
cannot promise completeness. We should remove it instead of adding another
round of policy knobs.

**Tune the part size and parallelize eviction.** Smaller parts reduce miss
amplification and parallel deletion makes eviction less painful. Neither change
fixes best-effort admission, startup indexing, or the mismatch between cache
and replica semantics. This would make the current implementation faster
without fixing the abstraction.

**Use only Foyer `HybridCache`.** This is the recommended answer for most
workloads. It is not sufficient when a local miss is a correctness or
availability failure, newly written compacted SSTs must be retained locally, or
SSTs must be managed as whole files. It also does not provide whole-SST
background prefetch for workloads with spatial locality. Foyer should be the
default recommendation, but it cannot be the only mechanism.

**Fill the whole SST synchronously on a read miss.** This is close to the
current part-cache behavior, except the amplification grows from one part to an
entire SST. It makes the miss pay for bytes the caller did not request. The
proposed read-through mode returns the requested range first and moves the full
download to the background.

**Rely only on DELETE passthrough.** This removes a local SST immediately when
GC uses this wrapper, with no periodic remote LIST cost. It leaves stale local
SSTs indefinitely when GC runs on another machine or an out-of-band tool
deletes remote objects. The proposed design retains this fast path but also
reconciles periodically by default.

**Publish a remote deletion feed.** GC could publish durable SST tombstones for
mirrors to consume. This avoids full prefix LISTs and distinguishes GC from
accidental remote loss, but it adds sequencing, retention, and consumer-progress
protocols. LIST plus confirmatory HEAD keeps the mirror compatible with an
unmodified remote object-store layout.

**Integrate the mirror into SlateDB's builders.** SlateDB could construct the
wrapper, reconcile manifests before local-only reads, prune files from manifest
reachability, and manage the mirror's lifecycle. That provides a stronger
complete-local-set guarantee, but it turns an object-store policy into a DB
lifecycle feature. The proposed design keeps `LocalOnly` narrow: it rejects a
miss and leaves population to writes or external provisioning.

**Prune from manifest reachability.** This could delete obsolete local SSTs
before remote GC's `min_age`. It would require the wrapper to decode SlateDB
manifests or SlateDB to manage the mirror explicitly. The standalone wrapper
instead mirrors delete calls and reconciles local files with remote object
existence, so it never prunes earlier than remote GC.

**Make local disk authoritative and remote storage a periodic backup.** This is
simple for one machine but changes SlateDB's coordination and recovery model.
The proposed write-back mode keeps remote manifests authoritative and treats
unpublished local SSTs as disposable work. Making local disk authoritative is
outside this RFC.

## Open Questions

- Should write-back ship in the first implementation, or should the RFC accept
  its design while write-through gets production experience first?
- Should read-through mode add a capacity policy, or should operators rebuild
  the mirror when its volume fills?

## References

- [Issue #1980: Remove `CachedObjectStore`](https://github.com/slatedb/slatedb/issues/1980)
- [RFC 0023: Targeted Cache Warming and Best-Effort Block Cache Eviction](0023-cache-manager.md)
- [RFC 0026: Garbage Collector Boundary](0026-garbage-collector-boundary.md)
- [RFC 0027: Decoupled Pluggable Object Store Cache](0027-decoupled-object-store-cache.md)
- [RFC 0031: Block Cache Policy](0031-block-cache-policy.md)
- [Foyer `HybridCache`](https://docs.rs/foyer/latest/foyer/struct.HybridCache.html)
- [ZeroFS prefetching object store](https://github.com/Barre/ZeroFS/blob/main/zerofs/src/object_store_prefetch.rs)

## Updates

- Added optional periodic remote reconciliation, enabled at a one-minute
  interval by default, while retaining DELETE passthrough as the immediate local
  cleanup path.
- Initial draft.
