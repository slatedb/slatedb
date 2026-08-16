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

`CachedObjectStore` sits awkwardly between a cache and a replica. It fetches and
evicts fixed-size parts like a best-effort cache, while some workloads expect it
to keep every live SST on local disk. It does neither job particularly well.

This RFC rewrites `CachedObjectStore`. Best-effort block caching belongs in
SlateDB's `DbCache`, where Foyer's `HybridCache` stores the blocks a read
actually asks for. The rewritten `CachedObjectStore` stores whole compacted
SSTs. Per-call GET and PUT policies control how the cache routes I/O.

The default GET action, `ReadThrough`, serves a missing range from remote
storage and fetches the full SST in the background. `LocalOnly` instead treats
a missing local SST as an error. Applications may install a custom policy when
different call sources need different behavior. Both PUT actions write each
compacted SST to local disk. WAL SSTs pass through to remote storage because
SlateDB reads them only during recovery.

The PUT policy controls remote persistence. `WriteThrough` queues each
compacted SST for upload and waits for that upload to finish. `WriteBack` uses
the same queue but may acknowledge the write once the complete SST has been
written and flushed locally and its upload entry has been queued. The remote
upload may still be pending.

Write-back has a remote publication barrier: every earlier compacted SST write
must reach object storage before a manifest or compactions record is published.
The cache implements this ordering inside the `ObjectStore` wrapper. SlateDB
has no cache-specific builder hooks. An optional public `warm_cache` utility
reads a database's latest manifest and populates every compacted SST it
references.

A remote DELETE that passes through the cache removes the matching local SST
immediately. By default, each cache also reconciles its local `objects/` tree
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

The built-in policies divide as follows:

| Requirement | GET action | PUT action |
|---|---|---|
| Return a missing SST range from remote, then cache the whole SST | `ReadThrough` | Either |
| Treat a missing local SST as an error | `LocalOnly` | Either |
| Persist each SST locally and remotely before returning | Either | `WriteThrough` |
| Acknowledge a compacted SST from the local queue and upload it asynchronously | Either | `WriteBack` |

Best-effort hot block caching is separate from both choices. It belongs in
`DbCache`, backed by Foyer `HybridCache`.

Foyer and the rewritten cache may be layered. Foyer caches decoded blocks; the
cache stores whole SSTs. The GET policy determines whether a local miss falls
through to remote storage or reports a broken invariant. The PUT policy
determines when an SST write is acknowledged.

## Goals

- Replace `CachedObjectStore`'s part-based implementation and eviction
  machinery with whole-SST storage.
- Use Foyer for best-effort block caching and `CachedObjectStore` for whole-SST
  locality.
- Serve read-through misses without waiting for the background cache fill, and
  keep every newly written compacted SST local instead of dropping admission.
- Let deployments reject local SST misses instead of silently reading
  remotely.
- Offer both synchronous remote persistence and concurrency-limited
  asynchronous write-back.
- Preserve remote consistency by publishing data before metadata that references
  it.
- Support removing local SSTs deleted by remote GC, whether the DELETE passes
  through this cache or occurs on another machine.
- Provide an explicit utility for populating every compacted SST referenced by
  a database's latest manifest.
- Keep the cache behind the standard `ObjectStore` interface so SlateDB does
  not need cache-specific configuration or lifecycle code.

## Non-Goals

- Replace `DbCache`, `FoyerCache`, or `FoyerHybridCache`.
- Block a read-through miss on the background full-SST download.
- Guarantee that `ReadThrough` has a complete local copy of every live SST.
- Automatically preload or validate a complete existing database under
  `LocalOnly`. Reconciliation only removes local SSTs confirmed absent
  remotely; a normal local miss remains an error. Applications may call
  `warm_cache` explicitly.
- Make local disk loss transparent to an in-flight local-only read.
- Recover unpublished write-back work after a process crash. Queued staging
  files are safe to discard because the publication barrier prevents metadata
  from referencing them.
- Coordinate multiple `CachedObjectStore` instances through one cache root.
  A root belongs to one live wrapper.
- Change the SlateDB manifest, compactions, WAL, or SST wire formats.
- Cache WAL SSTs, manifests, compactions records, GC boundaries, or arbitrary
  objects. WAL and untagged coordination traffic passes directly to the wrapped
  store.

## Design

### GET and PUT Policies

The rewritten cache retains the per-call policy pattern from the current
`CachedObjectStore`. A `CacheGetPolicy` receives the compacted SST's
`ObjectStoreCallTag`, then returns one of three actions:

| GET action | Local hit | Local miss |
|---|---|---|
| `Bypass` | Not checked | Forward the original GET to the wrapped store without local admission |
| `ReadThrough` | Serve it | Forward the requested GET and schedule a full-SST background fill after the foreground GET succeeds |
| `LocalOnly` | Serve it | Return `LocalCacheError` without accessing remote storage |

`ReadThrough` keeps the full download off the foreground path. A custom policy
can use `Bypass` for call sources such as compaction scans.

The wrapper invokes the policy only for calls carrying an
`ObjectStoreCallTag` whose `sst_type` is `Compacted`. Tagged WAL calls and
untagged calls bypass before policy dispatch. This is an implementation
invariant, so a custom policy cannot accidentally cache WALs, manifests, or
other coordination objects.

`DefaultCacheGetPolicy` returns its configured action for every tagged
compacted GET:

| Request | Routing |
|---|---|
| Untagged or tagged WAL | Bypass before policy dispatch |
| Tagged compacted | Configured default action |

A custom GET policy may use `ObjectStoreCallTag::kind` to select different
actions for `Main`, `Reader`, `Compactor`, and `GC` calls.

A retry-tagged call quarantines the local file before policy dispatch. There is
no `Refetch` action: invalidation followed by dispatch lets `ReadThrough`
refetch while preserving `LocalOnly`. `head = true` uses the same GET action
rather than a separate HEAD policy.

The PUT policy returns one of two actions for a tagged compacted SST:

| PUT action | Write completes after | Remote behavior |
|---|---|---|
| `WriteThrough` | The complete SST is durable locally and remotely | The remote write is part of the foreground operation |
| `WriteBack` | The complete compacted SST is in the local upload queue | Upload asynchronously, with a barrier before publishing remote metadata |

The wrapper invokes the PUT policy only for tagged compacted SSTs. WAL and
untagged PUTs bypass it. SlateDB currently writes compacted SSTs unconditionally:
buffered single PUTs use `PutMode::Overwrite`, while larger SSTs use multipart
uploads. Both are eligible for `WriteBack`. WAL SSTs use `PutMode::Create` for
fencing and bypass the cache. If a future tagged compacted write is
conditional, selecting `WriteBack` executes as `WriteThrough` so the caller
receives the remote precondition result. There is no PUT `Bypass` action:
storing every tagged compacted SST locally is part of the cache's contract.

Any GET policy may be paired with any PUT policy. Both PUT actions store newly
written compacted SSTs as whole local files and use the same upload queue. A
custom PUT policy can choose `WriteThrough` or `WriteBack` from
`ObjectStoreCallTag::kind`. With `ReadThrough` plus `WriteBack`, remote fallback
is available only after a queued SST has uploaded. Losing its queued staging
file poisons the upload queue because no remote copy exists yet.

Tagged WAL operations and untagged operations are never read from or written
to the local cache. They are forwarded to the wrapped store, although
write-back may hold a manifest or compactions PUT at the remote publication
barrier described below.

### Public API

`CachedObjectStore` is a normal `ObjectStore` implementation. Users construct
it around their remote store and pass it through the existing
`DbBuilder::new`/`Db::builder` object-store parameter. SlateDB does not gain a
cache-specific `DbBuilder` method.

```rust
pub enum CacheGetAction {
    Bypass,
    ReadThrough,
    LocalOnly,
}

pub trait CacheGetPolicy: Send + Sync + Debug + 'static {
    fn get_action(&self, tag: &ObjectStoreCallTag) -> CacheGetAction;
}

pub enum CachePutAction {
    WriteThrough,
    WriteBack,
}

pub trait CachePutPolicy: Send + Sync + Debug + 'static {
    fn put_action(&self, tag: &ObjectStoreCallTag) -> CachePutAction;
}

pub struct DefaultCacheGetPolicy {
    action: CacheGetAction,
}

impl DefaultCacheGetPolicy {
    pub fn new(action: CacheGetAction) -> Self;
}

pub struct DefaultCachePutPolicy {
    action: CachePutAction,
}

impl DefaultCachePutPolicy {
    pub fn new(action: CachePutAction) -> Self;
}

pub struct CachedObjectStoreBuilder {
    // Private fields.
}

impl CachedObjectStore {
    pub fn builder(
        root_folder: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
    ) -> CachedObjectStoreBuilder;
}

impl CachedObjectStoreBuilder {
    pub fn with_get_policy(self, policy: Arc<dyn CacheGetPolicy>) -> Self;
    pub fn with_put_policy(self, policy: Arc<dyn CachePutPolicy>) -> Self;
    pub fn with_prefetch_concurrency(self, concurrency: usize) -> Self;
    pub fn with_upload_concurrency(self, concurrency: usize) -> Self;
    pub fn with_reconciliation_interval(self, interval: Option<Duration>) -> Self;
    pub async fn build(self) -> Result<Arc<CachedObjectStore>, Error>;
}

pub struct WarmCacheResult {
    pub manifest_id: u64,
    pub populated_ssts: usize,
    pub already_present_ssts: usize,
    pub populated_bytes: u64,
}

pub async fn warm_cache(
    cache: &CachedObjectStore,
    db_root: impl Into<object_store::path::Path>,
) -> Result<WarmCacheResult, Error>;

#[async_trait]
impl ObjectStore for CachedObjectStore {
    // Standard ObjectStore methods delegate to local and remote storage.
}
```

The defaults are `DefaultCacheGetPolicy::new(CacheGetAction::ReadThrough)` and
`DefaultCachePutPolicy::new(CachePutAction::WriteThrough)`. Applications use
the policy setters to replace either default.

The reconciliation interval defaults to `Some(Duration::from_secs(60))`.
Passing `None` disables reconciliation; construction rejects
`Some(Duration::ZERO)`:

```rust
let remote: Arc<dyn ObjectStore> = Arc::new(
    AmazonS3Builder::from_env()
        .with_bucket_name("my-bucket")
        .build()?,
);
let cache = CachedObjectStore::builder("/var/lib/slatedb/my-db", remote)
    .build()
    .await?;
let warmed = warm_cache(cache.as_ref(), db_path.clone()).await?;
let db = Db::builder(db_path, cache).build().await?;
```

Calling `warm_cache` is optional. The utility has no options: it warms every
compacted SST in the latest manifest, uses the cache's existing remote-read
concurrency and retry policy, and returns an error if any SST cannot be
populated. Callers that do not need eager population omit the call.

`Db::close()` requires no cache-specific follow-up. Its default close options
perform a final memtable flush and publish the resulting metadata. The cache's
publication barrier makes that metadata PUT wait for every earlier queued SST,
so close returns only after every SST referenced by the closed database state
has reached remote storage. `close_with_options()` can disable the final flush;
that form does not provide this guarantee. Any later, unpublished compaction
output may still be discarded after a crash.

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

This differs slightly from putting instrumentation outside retrying. SlateDB
intentionally puts `InstrumentedObjectStore` inside `RetryingObjectStore` so
each retry attempt is counted. Metrics record calls into the cache, not the
extra remote GET that a read-through miss starts in the background. The cache
therefore owns retries and metrics for background fills, uploads, and
reconciliation.

`LocalCacheError` must map to a non-retryable `object_store::Error` variant.
Otherwise SlateDB's outer retry wrapper would turn a local-only miss or a
permanent local-disk failure into an unbounded retry loop. This is a wrapper
integration detail, not a cache-specific branch in SlateDB.

The cache applies the local data path only to calls carrying an
`ObjectStoreCallTag` whose `sst_type` is `Compacted`. It classifies calls before
policy dispatch, so a custom policy cannot opt WAL or untagged traffic into the
cache. A retry after validation failure quarantines the local file before the
GET policy runs.

WAL operations, reads and writes of manifests, compactions records, and GC
boundaries, along with LIST, untagged HEAD, copy, and other untagged operations,
always go to the wrapped remote store. In particular, a GC boundary check can
never be answered from the cache, even if the cache directory happens to
contain a file at the same local path. The wrapper uses the tag, not the
object path, to decide which reads and writes are compacted SST operations.
After that classification, it maps the object locally by placing its full
canonical object-store path below `objects/`.

Because the wrapper is supplied through the normal `ObjectStore` parameter,
the same type works with `Db`, `DbReader`, and `Compactor`. A cache root belongs
to one live `CachedObjectStore`. Components for the same database in the same
process may share the wrapper through `Arc`; separate wrappers, including
wrappers in separate processes, must use separate roots. This ownership rule is
a caller contract.

### Whole-SST Local Layout

Each cached SST maps to one local file. The cache uses the canonical
`object_store::path::Path` directly as a relative path below `objects/`. It
appends the path's existing components without percent-decoding or otherwise
transforming them. `object_store::path::Path` has no leading or trailing
separator, empty component, `.` or `..` component, or ASCII control character,
so a valid path is already relative and cannot lexically escape `objects/`.

```text
<cache-root>/
  objects/<full-object-store-path>
  staging/<operation-id>
```

Before accessing a local file, the cache verifies that every object-store path
component maps to exactly one native filesystem component and that the result
remains below `objects/`. A path that the local filesystem cannot represent
losslessly returns `LocalCacheError`. This includes an alias with an existing
path on a case-insensitive filesystem.

For example:

```text
<cache-root>/
  objects/
    tenant1/foo/bar/baz/compacted/abc1234.sst
    tenant2/source-db/compacted/def5678.sst
  staging/
```

Clone and split SSTs arrive with their source database's full object-store
path, so they naturally group below that source database path. The cache does
not need to know which database is current or classify an SST as owned versus
external. It does not parse the path to find a database root. One representable
canonical remote object path always maps to the same local path.

Each file stores the SST body followed by a compact footer containing the
body length, checksum, attributes, and cached `ObjectMeta`. Keeping the body at
offset zero makes range reads direct, while the footer lets tagged local GETs
preserve object metadata without an in-memory index or a second sidecar file.
On every local open, the cache verifies that the requested remote path exactly
matches the footer's `ObjectMeta.location`. A mismatch reports
`LocalCacheError` rather than serving an aliased local file.

An SST becomes generally readable locally only after its complete body and
footer are written to `staging/` and atomically renamed into `objects/`.
Read-through fills do this after the full remote GET completes. For writes, the
complete flushed staging file is registered in an in-memory path-to-sequence
map and submitted to the upload queue. Reads through this wrapper can use that
complete staging file while its upload is pending. Partial downloads and
multipart uploads are never visible as live SSTs.

After a queued SST uploads successfully, the cache records the remote metadata
in its footer, renames the staging file into `objects/`, flushes the parent
directory, and only then marks its sequence resolved. There is no durable
metadata for the queue.

### Construction and Cleanup

Building a `CachedObjectStore` removes abandoned files under `staging/` before
starting background work. It does not alter `objects/` or read or interpret a
SlateDB manifest.

Deleting all staging files at construction is safe. The cache does not forward
a manifest or compactions PUT until every earlier asynchronous SST upload has
completed and its local file has moved into `objects/`. A file left under
`staging/` after a crash therefore cannot be referenced by published remote
metadata. At worst, remote storage also contains an unreferenced SST whose
upload completed just before the crash.

This cleanup relies on exclusive ownership by contract. Constructing another
wrapper against the same root while one is live is unsupported: the new wrapper
could delete active staging files or race local publication. The implementation
does not attempt to detect that misuse.

The built-in `ReadThrough` policy can start with an empty directory and populate
it on demand. `LocalOnly` assumes required SSTs were written through this cache
or populated explicitly. Opening an existing remote database with an empty
cache configured for `LocalOnly` will fail on the first missing SST.

SlateDB does not coordinate publication barriers or root ownership on the
cache's behalf.

### Explicit Warming

There are three ways to populate the cache:

1. Any successful tagged compacted SST write installs the complete local SST.
2. A tagged compacted `ReadThrough` miss schedules a full-SST background fill.
3. An application can call `warm_cache` to populate the compacted SSTs in a
   database's latest manifest and wait for atomic publication.

`warm_cache` is a public SlateDB utility rather than a method on
`CachedObjectStore`. This keeps manifest interpretation out of the wrapper's
normal `ObjectStore` implementation while letting the utility use the cache's
private population primitive. `CacheManager` remains responsible for decoded
block-cache entries; this RFC does not add an object target or change
`DbCacheManagerOps::warm_sst`.

The utility takes the existing `CachedObjectStore` and a database root in the
wrapped store's logical namespace. It reads the latest manifest, constructs a
`PathResolver` with the manifest's external SST mappings, enumerates every
compacted SST referenced by L0, sorted runs, and segments, and deduplicates the
resolved object paths. It then populates missing paths with the cache's
existing remote-read concurrency and retry policy. Each population is
single-flight with concurrent read-through fills and warm calls for the same
path.

A path already installed under `objects/`, or available as a complete queued
write through the same wrapper, counts as already present after its footer,
length, and checksum validate. An invalid local file is quarantined and fetched
again. Otherwise the utility downloads the full SST, writes and flushes a
staging file, and atomically renames it into `objects/` before counting it as
populated. A failure returns an error without removing SSTs already populated
by the call. The utility bypasses `CacheGetPolicy`, so it works when ordinary
reads use `LocalOnly` or `Bypass`.

`WarmCacheResult::manifest_id` defines the consistency boundary: success means
every compacted SST referenced by that manifest was locally available when the
utility completed. Another writer may publish a newer manifest concurrently,
so the result does not guarantee that the warmed manifest is still latest.
Applications can compare the returned ID with the latest manifest and call the
utility again when it has advanced, but this does not eliminate the race.
Preventing a newer manifest from becoming visible until its SSTs are local
would require DB lifecycle integration and is outside this RFC.

When the cache wraps a `PrefixStore`, `db_root` is relative to the prefix
store's logical namespace. For example, warming `foo` through a prefix store
rooted at `tenant1` reads `foo/manifest/...` through the wrapper, which maps to
`tenant1/foo/manifest/...` in the underlying bucket. The cache stores
`objects/foo/compacted/...`; it does not include the hidden prefix. External
SST paths must be reachable through the same wrapped namespace, as they must be
for normal database reads.

### Read Semantics

Local hits honor `GetOptions` preconditions using the metadata stored with the
local SST; immutable SSTs do not require remote revalidation. A tagged
`get_opts` call with `head = true` can return metadata from the local footer.

On a read-through miss, the cache forwards the original `GetOptions` to the
remote store. The requested range is returned directly to the caller; the
foreground request does not wait for a full-SST download. After the remote GET
succeeds, the cache schedules a separate full-SST GET. That download writes
to `staging/` and atomically renames the complete SST into place.

Background fills are single-flight by SST path. Concurrent foreground misses
may each issue the range GET they need, but they share one full-SST download.
The prefetch scheduler bounds active downloads by concurrency. Waiting for a
permit happens in the background and never delays the range response.

If a background download fails, the foreground read is unaffected. The cache
records the failure and tries again on a later miss. A concurrent delete for
the same path prevents the staging file from being published.

The `LocalOnly` action returns `LocalCacheError` on a miss without checking
remote storage. The miss may mean that the database was not populated, another
process published a new SST, or the local file is missing or corrupt.
`warm_cache` may still fetch and install SSTs explicitly because it invokes the
private population primitive rather than routing a normal GET through the
policy.

### Write-Through Semantics

`WriteThrough` is the default PUT action. It makes the local cache and remote
storage part of the write contract while keeping remote storage authoritative.

For a tagged compacted SST, the cache uses the same staging file, upload queue,
and worker path as write-back. Once the complete local file is queued,
write-through waits for that entry's upload result. It returns only after the
remote write succeeds and the local file is promoted into `objects/`. This
keeps one upload implementation for both PUT actions; only the point at which the
caller is released differs.

If the upload fails permanently, write-through removes the unpublished staging
file and returns the remote error, allowing SlateDB's outer retry wrapper to
retry the PUT. Because the failed write was never acknowledged, its sequence is
retired and does not block later publication barriers. If the remote write
succeeds but local promotion fails, the wrapper returns `LocalCacheError`; the
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

One compacted SST PUT under either local PUT action proceeds as follows:

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

WAL reads and writes bypass the cache even when the main and WAL paths use the
same object store. They therefore do not enter the asynchronous queue or need a
barrier shared across the main and WAL stores.

Manifests, compactions records, and GC boundary updates are always written
through. This preserves remote conditional-write and fencing semantics.

### Delete Semantics

`ObjectStore::delete` does not carry `ObjectStoreCallTag`, so the cache derives
a possible local file by placing the full canonical object-store path below
`objects/`. A delete waits for any earlier upload of the same path, deletes the
remote object, and then removes the local file if one exists and its footer
location matches the deleted path. A failed local unlink wastes disk but does
not resurrect the remote object. Reconciliation retries it when enabled. An
in-flight read keeps its file handle until the read ends.

The wrapper does not interpret manifests or run a separate reachability
planner. A DELETE that passes through the wrapper remains the fastest cleanup
path when SlateDB GC uses this wrapper. Deletes for non-SST paths simply pass
through because no matching local file exists.

### Reconciliation

A DELETE issued on another machine does not pass through this wrapper. By
default, `CachedObjectStore` therefore runs a reconciliation task every minute
regardless of GET policy. `Some(interval)` enables the task and must be non-zero;
`None` disables it. Each enabled cache randomizes its initial delay to spread
remote LIST requests across the fleet. Dropping the wrapper cancels its task;
reconciliation does not delay `Db::close()`.

One reconciliation pass proceeds as follows:

1. Snapshot the `ObjectMeta.location` values from the footers of complete files
   currently under `objects/`, verifying that each location equals its full
   local path relative to `objects/`. Files created after this snapshot are not
   candidates in this pass. Files under `staging/` are never candidates. A file
   whose footer location does not match is preserved and reported as a local
   error.
2. Group the snapshot by each location's remote parent and LIST each distinct
   parent prefix on the wrapped remote store. This avoids listing unrelated
   bucket contents and requires no knowledge of SlateDB's database path
   structure.
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

Remote storage remains authoritative under every GET policy. A confirmed remote
absence therefore authorizes local deletion even for `LocalOnly`. If remote
storage loses an SST that published metadata still references, reconciliation
will remove the local copy and the next tagged read will return
`LocalCacheError`; preserving a private local copy would make the cache an
authoritative replica, which is outside this RFC. Reconciliation does not
delete remote objects, decode manifests, or remove SSTs earlier than remote GC.

### Failure and Restart

Failure handling depends on the selected GET and PUT actions, but no path
exposes a partial local file:

| Failure | Behavior |
|---|---|
| Local disk full during a `ReadThrough` fill | Keep serving the foreground range from remote storage; record the fill failure |
| Local disk full during a compacted SST write | Fail the write under either local PUT action; never drop an acknowledged queue entry |
| Remote write fails under `WriteThrough` | Remove the queued staging file and return the error so the outer wrapper can retry |
| Remote upload fails under `WriteBack` | Retry, then poison the queue on a permanent failure |
| Remote-durable compacted SST is missing under `ReadThrough` | Serve the requested remote range and schedule a full-SST fill |
| Remote-durable compacted SST is missing under `LocalOnly` | Return `LocalCacheError`; do not access remote storage |
| Queued `WriteBack` staging file disappears while running | Return `LocalCacheError` and poison the upload queue because the upload has lost its source |
| Local SST fails validation | Quarantine it and dispatch the retry through the GET policy; poison the queue if it is pending upload |
| Background full-SST GET fails | Keep the foreground result; retry on a later miss |
| Reconciliation LIST or HEAD fails | Preserve the affected local files, record the failure, and retry on the next pass |
| Reconciliation confirms a remote SST is absent | Remove the complete local file regardless of GET policy |
| Reconciliation cannot unlink a stale local SST | Retain the file, record the failure, and retry on the next pass |
| Process crashes with staging files | Delete them when the next wrapper is constructed; published remote metadata cannot reference them |
| Process crashes after an SST upload but before local promotion | Delete the staging file on reconstruction; the remote SST is an unreferenced orphan eligible for normal GC |
| Machine and local disk are lost under `WriteThrough` | Rebuild from remote storage |
| Machine and local disk are lost under `WriteBack` | Rebuild the last published state from remote storage; unpublished compaction work is discarded |

Remote storage remains sufficient to repopulate a cache using `WriteThrough`,
either through `ReadThrough` or `warm_cache`. The same is true for `WriteBack`:
the publication barrier makes queued staging files unnecessary for recovery,
while remote metadata continues to describe a complete remote state.

### Replacing the Part Cache

`CachedObjectStore` and its `builder(root_folder, object_store)` entry point
remain public. The rewrite removes:

- part-cache-specific policies, storage traits, part files, the in-memory part
  index, the evictor, and part-cache metrics,
- `ObjectStoreCacheOptions`,
- `Settings::object_store_cache_options`,
- `cache_on_flush`, `cache_on_compaction`, `part_size_bytes`,
  `max_cache_size_bytes`, `preload_disk_cache_on_startup`, `scan_interval`, and
  `max_open_file_handles`,
- startup preload logic specific to the part cache.

The rewritten builder replaces the part-cache knobs with the GET and PUT policy,
concurrency, and reconciliation settings described above. `ObjectStoreCallTag`
remains public for policy dispatch.

The old part-file layout is not migrated. Part files cannot be published as
complete local SSTs. Operators should allocate a new cache root and delete the
old cache directory after migration.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

KV semantics do not change. `ReadThrough` preserves remote fallback on a local
miss. `LocalOnly` adds explicit cache health and missing-object errors.

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
the cache, so multi-writer fencing, boundary checks, discovery, and conditional
PUT errors are still observed remotely. GC deletes remove the matching local
SST when they pass through this cache. When enabled, periodic reconciliation
removes the same SST when GC runs on another machine.

### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [x] Distributed compaction
- [x] Compactions format

Compaction formats are unchanged. Outputs are cached as whole files and
compactions metadata is published after the upload barrier.

### Storage Engine Internals

- [x] Write-ahead log (WAL)
- [x] Block cache
- [x] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

The block cache remains the general-purpose best-effort cache. The cache stores
only tagged compacted SSTs as whole local files. WAL SSTs always use remote
storage. `ReadThrough` population is best effort; applications using
`LocalOnly` may populate the latest manifest explicitly with `warm_cache`.

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
issues the requested range GET on the foreground path and a separate full-SST GET
in the background. Foreground latency waits only for the requested range, while
remote bandwidth includes both requests. Later reads use the local file after
the background download finishes.

`LocalOnly` performs no remote fallback for tagged compacted SST reads.
`warm_cache` may still read remote storage explicitly. WAL and untagged
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

One `warm_cache` call reads the latest manifest and issues one full-object GET
for each referenced SST not already local. It may therefore transfer the full
live compacted data set when starting with an empty cache. Existing local SSTs
avoid those GETs. The utility shares the cache's remote-read concurrency with
read-through fills and returns only after every SST in its manifest snapshot is
installed or one fails.

Each reconciliation pass scans complete local objects, issues one remote LIST
per distinct remote parent prefix represented locally, and issues HEAD only for
paths missing from a LIST result. Caches randomize their initial delays to
spread this work across the fleet. Reconciliation never runs on the foreground
read or write path.

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

Operators size the volume for cached SSTs, temporary staging, and the
write-back backlog. The backlog has no separate byte limit, so disk capacity is
the admission limit. Abandoned staging files consume space only until the next
wrapper construction clears `staging/`. Complete SSTs deleted remotely on
another machine may consume local space until the next successful
reconciliation pass. If reconciliation is disabled, they remain until a DELETE
passes through the cache or the root is rebuilt.

### Observability

The initial metric set is:

- `slatedb.object_store_cache.local_hits`
- `slatedb.object_store_cache.local_misses`
- `slatedb.object_store_cache.foreground_remote_gets`
- `slatedb.object_store_cache.prefetch_ssts`
- `slatedb.object_store_cache.prefetch_bytes`
- `slatedb.object_store_cache.prefetch_failures`
- `slatedb.object_store_cache.warm_runs`
- `slatedb.object_store_cache.warm_ssts`
- `slatedb.object_store_cache.warm_bytes`
- `slatedb.object_store_cache.warm_failures`
- `slatedb.object_store_cache.staging_bytes`
- `slatedb.object_store_cache.pending_upload_ssts`
- `slatedb.object_store_cache.pending_upload_bytes`
- `slatedb.object_store_cache.resolved_upload_sequence`
- `slatedb.object_store_cache.upload_failures`
- `slatedb.object_store_cache.local_delete_failures`
- `slatedb.object_store_cache.reconciliation_runs`
- `slatedb.object_store_cache.reconciliation_list_requests`
- `slatedb.object_store_cache.reconciliation_head_requests`
- `slatedb.object_store_cache.reconciliation_deleted_ssts`
- `slatedb.object_store_cache.reconciliation_failures`
- `slatedb.object_store_cache.missing_sst_errors`
- `slatedb.object_store_cache.health`

Logs include the cache root, GET and PUT policies, warming and reconciliation
summaries, upload barrier waits, poison errors, and failed local operations.
They do not log one line per successfully warmed, filled, uploaded, or
reconciled object at the default level. These metrics belong to the cache
itself: SlateDB's outer instrumentation cannot see background GETs, uploads, or
reconciliation requests initiated inside the wrapper.

### Compatibility

- **Object-store data.** Manifest, compactions, WAL, and SST formats are
  unchanged. A database can move between cached and non-cached operation.
- **Public API.** `CachedObjectStore` and its
  `builder(root_folder, object_store)` entry point remain. Its part-specific
  builder methods and `Settings::object_store_cache_options` are removed, which
  is a breaking API and configuration change. The optional `warm_cache` utility
  operates on the concrete wrapper before or after it is passed to
  `Db::builder`; no `DbBuilder` methods change.
- **Local files.** The old part-cache layout is not compatible with the new
  whole-SST cache layout.
- **Rolling upgrades.** Mixed versions remain compatible through remote object
  storage. Each live wrapper must use a distinct cache root.
- **Bindings.** Generated Go, Python, Java, and Node settings lose the old cache
  options. They do not gain cache settings in this RFC.

## Testing

- Unit tests:
  - canonical object-store paths are used directly below `objects/` and cannot
    lexically escape the cache root,
  - percent-encoded path components remain unchanged rather than being decoded,
  - unrepresentable filesystem paths and footer-location aliases return
    `LocalCacheError`,
  - local path mapping preserves the full compacted, clone, and split
    object-store paths without parsing their database roots,
  - staging-file publication and cleanup,
  - local file removal after an SST delete,
  - the default GET policy returns its configured action and a custom policy
    may route by `ObjectStoreCallTag::kind`,
  - the default PUT policy returns its configured action, a custom policy may
    route by call source, and conditional puts selected as `WriteBack` execute
    as `WriteThrough`,
  - WAL and untagged GETs bypass before policy dispatch, while their PUT,
    multipart, HEAD, LIST, and copy operations pass through,
  - retry-tagged reads quarantine local data before policy dispatch, and local
    hits honor `GetOptions` preconditions using footer metadata,
  - queued staging-file publication, in-memory lookup, upload concurrency, and
    contiguous upload completion for both local PUT actions,
  - startup removes every abandoned staging file without altering `objects/`,
  - reconciliation defaults to one minute, `None` disables its background task,
    and a zero interval is rejected,
  - reconciliation snapshots only complete files under `objects/`, groups them
    by the parent of `ObjectMeta.location`, and ignores staging files,
  - reconciliation removes a local SST only when it is absent from LIST and a
    confirmatory HEAD returns `NotFound`,
  - a successful confirmatory HEAD preserves an SST omitted from LIST,
  - LIST, HEAD, and local unlink failures preserve the local file for a later
    pass,
  - the private population primitive is single-flight across read-through fills
    and explicit warm calls,
  - publication barrier ordering for asynchronous compacted SST uploads,
  - WAL and fence PUTs bypass PUT policy dispatch.
- Integration tests:
  - pass the cache through the normal `Db::builder` object-store parameter and
    verify the effective wrapper order,
  - select `ReadThrough` and verify a miss returns the requested range before the
    background full-SST GET completes,
  - verify the background GET installs one complete local file and later reads
    hit it,
  - issue concurrent range misses and verify they share one background
    full-SST GET,
  - fail a background GET and verify the foreground range still succeeds,
  - read and write WAL SSTs and verify they reach remote storage without
    creating local files,
  - write buffered single-PUT and multipart compacted SSTs with each PUT action
    and verify they use the same upload queue, `WriteThrough` waits for its
    result, and `WriteBack` returns after local enqueue,
  - read and write manifests, compactions records, and GC boundaries and verify
    each operation reaches remote storage without creating a local file,
  - verify a stale local SST cannot affect a GC boundary check,
  - exhaust local disk capacity and verify neither PUT action drops an
    acknowledged queue entry,
  - open an existing database with an empty `LocalOnly` cache and verify the
    first tagged compacted SST miss returns `LocalCacheError`,
  - assert tagged compacted `LocalOnly` hits and misses issue no remote fallback
    GET,
  - call `warm_cache` without a `DbCache` and verify it reads the latest
    manifest, resolves internal and external SST paths, and waits until every
    referenced compacted SST is atomically installed,
  - call `warm_cache` when ordinary GETs use `LocalOnly` or `Bypass` and verify
    explicit warming still fetches remotely,
  - verify `warm_cache` returns the warmed manifest ID, counts already-present
    SSTs separately, and leaves successful population in place if another SST
    fails,
  - corrupt an existing local SST and verify `warm_cache` quarantines and
    repopulates it rather than counting it as already present,
  - advance the latest manifest during warming and verify the result covers its
    returned manifest snapshot without claiming to cover the newer manifest,
  - wrap the remote store in a `PrefixStore`, warm a database root relative to
    that namespace, and verify local paths exclude the hidden prefix,
  - run GC through the cache and verify each remote SST delete removes its
    local file,
  - run GC on another machine's raw remote store, run reconciliation against
    the shared remote store, and verify the stale local SST is removed under
    both built-in GET actions,
  - publish a new local SST after reconciliation takes its local snapshot and
    verify the pass does not consider that file for deletion,
  - restart with queued write-back files under `staging/` and verify startup
    deletes them without changing published remote state,
  - call default `Db::close()` and verify its final metadata barrier drains all
    earlier queued SSTs; verify disabling the close flush removes that guarantee,
  - rebuild a `WriteThrough` cache through `ReadThrough` misses after deleting
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
  - verify `ReadThrough` never exposes a staging file or blocks a range
    response on a full-SST fill,
  - verify a tagged compacted `LocalOnly` miss never issues a remote GET while
    `warm_cache` may populate the same SST explicitly,
  - verify `published_remote_manifest_ssts ⊆ durable_remote_ssts` under
    `WriteBack`.
- Formal methods verification:
  - add a small FizzBee model for the write-back queue, staging cleanup after
    restart, and the remote publication barrier before enabling write-back
    outside experimental status.
- Performance tests:
  - point-read latency on cold and warm Foyer with both built-in GET actions,
  - foreground range latency while background full-SST downloads run,
  - full-manifest warming throughput and memory use for large manifests,
  - flush and compaction throughput with `WriteThrough` and `WriteBack`,
  - reconciliation LIST and confirmatory HEAD request cost for large caches,
  - local delete throughput and startup time versus the old part cache.

## Rollout

1. Rewrite `CachedObjectStore` to store whole SSTs, with GET and PUT policies
   defaulting to `ReadThrough` and `WriteThrough`. Mark the new policy API
   experimental.
2. Verify `Db`, `DbReader`, and `Compactor` accept it through their existing
   object-store parameters without cache-specific builder code.
3. Add `LocalOnly` and custom GET/PUT policy tests, including classification
   gates, validation retries, and conditional PUTs.
4. Add the public `warm_cache` utility on top of the cache's private
   single-flight population primitive.
5. Add metrics, startup staging cleanup, local cleanup on SST deletes, and
   periodic remote reconciliation.
6. Add `WriteBack` behind an explicit experimental option after the upload-queue
   and publication-barrier model is checked.
7. Document the wrapper order, policy dispatch, warming snapshot contract,
   `PrefixStore` behavior, common upload queue, single-owner root contract,
   reconciliation cost, and WAL passthrough.
8. Deprecate the part-cache-specific builder methods and settings for one
   release if required by compatibility policy, then remove the old part-cache
   implementation and metrics.

## Alternatives

**Keep the current part-based `CachedObjectStore`.** The current cache works for
workloads with enough spatial locality and a tolerant latency budget. It still
duplicates Foyer's job, amplifies misses to part boundaries, drops admission
under pressure, and cannot promise completeness. We should rewrite it instead
of adding another round of part-cache policy knobs.

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
proposed `ReadThrough` action returns the requested range first and moves the full
download to the background.

**Extend `CacheManager` to populate cached objects.** A new object target
could send a population hint through `GetOptions::extensions`. That mixes
whole-object population into an API designed for decoded block-cache entries,
and an arbitrary `ObjectStore` may ignore the hint without acknowledging that
nothing was populated. The public `warm_cache` utility operates on the
concrete wrapper and uses the manifest to warm the complete database snapshot.

**Put warming on `CachedObjectStore`.** A `warm_db` method could accept the
database root and provide the same behavior. The public utility keeps manifest
decoding out of the wrapper's normal object-store responsibilities while still
using its private population primitive.

**Rely only on DELETE passthrough.** This removes a local SST immediately when
GC uses this wrapper, with no periodic remote LIST cost. It leaves stale local
SSTs indefinitely when GC runs on another machine or an out-of-band tool
deletes remote objects. The proposed design retains this fast path but also
reconciles periodically by default.

**Publish a remote deletion feed.** GC could publish durable SST tombstones for
caches to consume. This avoids full prefix LISTs and distinguishes GC from
accidental remote loss, but it adds sequencing, retention, and consumer-progress
protocols. LIST plus confirmatory HEAD keeps the cache compatible with an
unmodified remote object-store layout.

**Integrate the cache into SlateDB's builders.** SlateDB could construct the
wrapper, reconcile manifests before local-only reads, prune files from manifest
reachability, and manage the cache's lifecycle. That provides a stronger
complete-local-set guarantee, but it turns an object-store policy into a DB
lifecycle feature. The proposed design keeps `LocalOnly` narrow: it rejects a
miss and leaves population to writes or an explicit `warm_cache` call.

**Prune from manifest reachability.** This could delete obsolete local SSTs
before remote GC's `min_age`. It would require the wrapper to decode SlateDB
manifests or SlateDB to manage the cache explicitly. The standalone wrapper
instead removes local files for DELETE calls and reconciles local files with
remote object existence, so it never prunes earlier than remote GC.

**Make local disk authoritative and remote storage a periodic backup.** This is
simple for one machine but changes SlateDB's coordination and recovery model.
The proposed `WriteBack` action keeps remote manifests authoritative and treats
unpublished local SSTs as disposable work. Making local disk authoritative is
outside this RFC.

## Open Questions

- Should `WriteBack` ship in the first implementation, or should the RFC accept
  its design while `WriteThrough` gets production experience first?
- Should `ReadThrough` enforce a capacity limit, or should operators rebuild the
  cache when its volume fills?

## References

- [Issue #1980: Remove `CachedObjectStore`](https://github.com/slatedb/slatedb/issues/1980)
- [RFC 0023: Targeted Cache Warming and Best-Effort Block Cache Eviction](0023-cache-manager.md)
- [RFC 0026: Garbage Collector Boundary](0026-garbage-collector-boundary.md)
- [RFC 0027: Decoupled Pluggable Object Store Cache](0027-decoupled-object-store-cache.md)
- [RFC 0031: Block Cache Policy](0031-block-cache-policy.md)
- [Foyer `HybridCache`](https://docs.rs/foyer/latest/foyer/struct.HybridCache.html)
- [ZeroFS prefetching object store](https://github.com/Barre/ZeroFS/blob/main/zerofs/src/object_store_prefetch.rs)

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
