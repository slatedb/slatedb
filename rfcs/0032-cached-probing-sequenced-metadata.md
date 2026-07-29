# Cached Probing for Sequenced Metadata

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Latest Object Cache](#latest-object-cache)
  - [Cold Reads](#cold-reads)
  - [Warm Reads](#warm-reads)
  - [Write-Through Updates](#write-through-updates)
  - [Boundary Checks and Garbage Collection](#boundary-checks-and-garbage-collection)
  - [Concurrency and Sharing](#concurrency-and-sharing)
  - [Failure Handling](#failure-handling)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
  - [Performance and Cost](#performance-and-cost)
  - [Observability](#observability)
  - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
  - [LIST on Every Latest Read](#list-on-every-latest-read)
  - [Cache Only the Latest ID](#cache-only-the-latest-id)
  - [Unbounded Linear Probing](#unbounded-linear-probing)
  - [Exponential and Binary Probing](#exponential-and-binary-probing)
  - [Parallel Window Probing](#parallel-window-probing)
  - [HEAD Probing](#head-probing)
  - [Adaptive Probe Limit](#adaptive-probe-limit)
  - [CURRENT Pointer](#current-pointer)
  - [Authoritative CURRENT Pointer](#authoritative-current-pointer)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

SlateDB finds the latest sequenced metadata object (`.manifest` or
`.compactions`) by listing every object in the metadata directory, selecting
the highest ID, and reading that object. This RFC replaces LIST on the common
path with a process-local cache and consecutive GET probes.

Each sequenced metadata store (`ManifestStore` and `CompactionsStore`) caches
the highest object ID and its encoded bytes. A latest read starts at the next
ID and issues up to four GETs. A 404 means the last successful GET or cached
object is the latest version. Four consecutive hits fall back to the existing
LIST path. A store with an empty cache also uses LIST to establish its starting
point.

Successful writes update the same cache. Boundary checks from RFC-0026 remain
in place and invalidate cached objects rejected by the GC boundary.

## Motivation

SlateDB stores manifests and compaction state as immutable, consecutively
numbered objects:

```text
manifest/00000000000000000012.manifest
manifest/00000000000000000013.manifest

compactions/00000000000000000007.compactions
compactions/00000000000000000008.compactions
```

`ObjectStoreSequencedStorageProtocol::try_read_latest` currently performs these
operations:

1. LIST the namespace.
2. Sort the returned metadata by object ID.
3. GET the object with the highest ID.
4. Check the object ID against the GC boundary.

The LIST cost repeats on refreshes even when no writer has added a new object.
LIST also returns metadata for the full retained history, although the caller
needs one object.

This pattern is expensive and wasteful for idle processes. A database that's
idle for five minutes incurs 560 LIST requests with default configuration.
This comes out to $24.19 per-month in AWS S3 us-east-1 pricing.

Sequenced metadata already supplies a cheaper lookup mechanism. Once a process
has seen ID `N`, it can GET `N+1`. A 404 means `N` is still latest. If `N+1`
exists, the process continues with `N+2`. The protocol's consecutive-ID
invariant makes the first 404 a valid stopping condition. Probing stops after
four successful GETs so a reader that is far behind catches up with LIST instead
of downloading every intervening object.

## Goals

- Idle databases should cost less than $5 per month.
- Protocol should be friendly to lagging readers.
- Remove LIST when a warm reader is close to the latest version.
- Keep the existing object layout, write protocol, and GC boundary semantics.
- Avoid rereading an unchanged latest object.
- Preserve the existing LIST fallback for cold stores, lagging readers, and GC
  races.

## Non-Goals

- Remove LIST from APIs that enumerate historical versions.
- Add a durable pointer object or change the metadata commit point.
- Share cache state across processes.
- Change manifest or compactions serialization.
- Replace the GC boundary protocol from RFC-0026.

## Design

### Latest Object Cache

Each `ObjectStoreSequencedStorageProtocol<T>` stores one cache entry:

```rust
Mutex<Option<(MonotonicId, Bytes)>>
```

The cache contains encoded bytes rather than `T`. This avoids adding a
`T: Clone` bound and lets the write path reuse the bytes it already encoded.

Cache updates are monotonic. An update replaces the entry only when its ID is
higher than the cached ID:

```rust
fn maybe_cache_latest(id, bytes):
    lock latest
    if latest is empty or id > latest.id:
        latest = (id, bytes)
```

The cache holds one object body per protocol instance. A manifest can be
several MiB, so this changes the steady-state memory footprint by the size of
the latest encoded manifest and compactions object for each live store
instance.

### Cold Reads

A protocol instance starts with an empty cache. Its first latest read keeps the
existing behavior:

```text
LIST namespace
    |
    +-- empty --------------------> return None
    |
    +-- select highest ID N
            |
            +-- GET N succeeds ---> cache (N, bytes), return N
            |
            +-- GET N is 404 ------> repeat LIST
```

The LIST retry covers the race where GC deletes the listed object before the
GET completes. Other object-store errors and codec errors return to the caller.

### Warm Reads

A warm read snapshots the cache, releases the mutex, and starts probing at the
next ID:

```text
cached (N, bytes_N)
    |
    +-- GET N+1 is 404 -----------> decode bytes_N, return N
    |
    +-- GET N+1 succeeds
            |
            +-- cache (N+1, bytes_N+1)
            +-- GET N+2
                    |
                    +-- continue until the first 404 or fourth hit
                            |
                            +-- fourth hit ---> fall back to LIST
```

The implementation never holds the cache mutex across an object-store request.
Concurrent readers may issue duplicate probes. They cannot move the cache
backward.

The first missing successor terminates the probe. It does not trigger LIST.
The cached bytes let the reader return the latest object without rereading it.
Four consecutive successful probes fall through to the cold LIST path.

### Write-Through Updates

The write path encodes a new value once:

1. Compute the next consecutive ID.
2. Encode the value.
3. PUT the object with create-if-absent.
4. If the PUT succeeds, cache the ID and encoded bytes when the ID is higher
   than the current cache entry.
5. Run the existing boundary check.

The generic checked write still decides whether the caller observes success.
`write_unchecked` caches the object after its physical PUT because unchecked
reads expose physically present objects.

A write performed through the same protocol instance moves the read watermark
without a LIST or a successful GET. The next latest read probes the following
ID and returns the cached bytes on 404.

### Boundary Checks and Garbage Collection

RFC-0026 remains part of the protocol. The cache does not make deleted IDs safe
to reuse and does not replace the durable GC boundary.

A cached object can become stale after another process advances the boundary
and GC deletes an old prefix. Checked latest reads already validate their
result against the boundary:

1. The probing path returns cached ID `N`.
2. The boundary rejects `N`.
3. The protocol invalidates the cache entry for `N`.
4. The generic latest-read loop retries.
5. The empty cache sends the retry through LIST.

An object above `N` may appear during the first probe. In that case, probing
advances the cache and can find an object above the boundary without LIST.

Deleting a cached object through the same protocol invalidates the matching
entry after the object-store DELETE succeeds. If another process deletes
objects covered by a newer GC boundary, checked reads reject and invalidate
any cached object covered by that boundary before returning it.

### Concurrency and Sharing

The cache belongs to `ObjectStoreSequencedStorageProtocol`, not the underlying
`ObjectStore`. Components share it only when they share the same protocol
instance, normally through an `Arc<ManifestStore>` or
`Arc<CompactionsStore>`.

Database components constructed together should reuse those store instances
where their lifetimes allow it. A separately constructed GC process, admin
client, or external compactor starts with an empty cache and pays one cold
LIST. There is no process-wide registry keyed by object-store path.

The cache update rule handles concurrent reads and writes:

- A lower ID never replaces a higher cached ID.
- Locks cover only cloning or replacing the cache entry.
- Object bytes are immutable after create-if-absent succeeds.
- A boundary rejection clears only the matching cached ID. It does not discard
  a higher entry installed by another operation.

### Failure Handling

The probing path distinguishes a missing successor from other failures:

- 404 for `N+1`: return cached `N`.
- Successful GET for `N+1`: cache it and continue.
- Four consecutive successful GETs: fall back to LIST.
- Other GET error: return the error.
- Decode error: return the codec error.

## Impact Analysis

SlateDB features and components that this RFC interacts with.

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

- [x] Manifest format
- [x] Checkpoints
- [ ] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

The manifest and compactions payload formats do not change. This RFC changes
how callers locate the latest encoded object.

### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance and Cost

The proposal moves object-store work from LIST to GET without adding a write
request.

| Operation | Existing LIST path | Cached probing |
|---|---:|---:|
| Successful write | 1 object PUT + boundary GET | Same |
| Cold latest read | 1 LIST + object GET + boundary GET | Same |
| Warm unchanged read | 1 LIST + object GET + boundary GET | 1 missing-successor GET + boundary GET |
| Warm read fewer than 4 versions behind | 1 LIST + object GET + boundary GET | `k` GET hits + 1 GET miss + boundary GET |
| Warm read at least 4 versions behind | 1 LIST + object GET + boundary GET | 4 GET hits + 1 LIST + object GET + boundary GET |

Object stores tend to bill LIST with PUT-class requests and GETs at a lower
request rate. Exact prices depend on the provider and region. Probing should
cost less when protocol instances live long enough to amortize their cold LIST
and usually lag by a small number of versions.

A process that falls far behind issues at most four probe GETs before using
LIST. The ratio below captures that workload:

```text
probe_gets / latest_reads
```

A ratio near `1` means most reads issue one 404 probe and return cached bytes.
A ratio near `4` means readers often hit the probe limit and fall back to LIST.

The proposal adds one encoded object body per live protocol instance. It does
not add objects to storage or increase write amplification.

### Observability

Add counters for:

- latest cache hits and cold misses;
- successful and missing probe GETs;
- cache advances from reads and writes; and
- boundary-driven cache invalidations.

Log probe-limit LIST fallbacks at debug level with the object directory, last
seen ID, and probe count.

Record a histogram of probes per latest read. Existing object-store metrics
continue to report request latency and failures.

No configuration is required.

### Compatibility

The object layout and payload formats do not change. Old and new SlateDB
versions can read objects written by either implementation.

Rolling upgrades are safe. Old processes continue to use LIST. New processes
build their cache from LIST and then probe. Each process retains RFC-0026
boundary checks, so mixed versions do not change GC fencing.

No public Rust API or language binding changes.

## Testing

- Unit test to verify reads fall back to LIST after four consecutive GET hits.
- Run benchmarks to verify an idle SlateDB with default configuration stays
  below $5 per month in AWS S3 us-east-1 pricing.

## Rollout

The change is internal and does not require a feature flag.

## Alternatives

### LIST on Every Latest Read

Keep listing the namespace and reading the highest object on every refresh.
This has no process-local state and catches up in one LIST plus one GET,
regardless of lag.

The retained history makes LIST more expensive than the information needed by
the caller. Repeating it when no version changed also adds avoidable request
cost and latency.

### Unbounded Linear Probing

Continue reading `N+1`, `N+2`, and so on until the first 404. A reader that is
`k` versions behind uses `k+1` GETs and avoids LIST. Each successful GET also
supplies the bytes needed if that object is latest.

The request count and latency grow linearly with lag. A process resuming after
a long pause could download hundreds of obsolete objects before returning.
The four-GET limit gives the common case the same behavior while bounding the
catch-up cost.

### Exponential and Binary Probing

`TableStore::last_seen_wal_id` uses exponential probing followed by binary
search to find the latest WAL SST. Starting at `N`, it probes offsets
`1, 2, 4, 8, ...` in parallel groups of eight until one is missing, then
binary searches between the highest hit and the first miss. Contiguous IDs make
the existence test monotonic. A pure binary search would not work because the
reader has no upper bound before the exponential phase.

This finds a frontier `k` versions away with `O(log k)` existence checks. The
reader must then GET the latest object because the search only establishes its
ID. For small `k`, linear GETs use fewer requests and already have the latest
bytes. Exponential probing is a better fit when large gaps are common enough to
justify the extra code and concurrent request fan-out.

### Parallel Window Probing

Issue a fixed window of successor reads concurrently. If the window contains a
404, the object before the first missing ID is latest. If every request
succeeds, issue another window or fall back to LIST. A window of four can cross
four versions in one round trip.

Issuing the full window on every read turns an unchanged warm read from one
request into four. A hybrid can probe `N+1` first and issue a window only after
that request succeeds, but requests beyond the first missing ID do no useful
work. This option trades request count for catch-up latency.

### HEAD Probing

Use HEAD requests to locate the frontier, then GET only the latest object.
This avoids downloading intermediate object bodies when a reader is behind.
An unchanged reader still needs one missing-successor request and can return
its cached bytes.

For small gaps, HEAD probing adds a final GET that linear GET probing avoids.
Many object stores bill HEAD and GET at the same request rate, so this option
reduces transferred bytes without reducing request charges.

### Adaptive Probe Limit

Change the probe limit based on recent reads. A store could raise the limit
after repeated LIST fallbacks and lower it after 404s. This may help workloads
where writers publish bursts that often exceed four versions.

The store would need more cache state and a tuning policy. A fixed limit has a
predictable request bound and keeps behavior consistent across protocol
instances.

### CURRENT Pointer

Store a small mutable `CURRENT` object containing the latest sequence ID.
Readers GET `CURRENT` and then GET the referenced metadata object. With a local
bytes cache, an unchanged pointer can return the cached object after one GET.

A pointer can support racing writes without making the pointer the commit
point. Starting from object `N-1`, a writer races:

1. create object `N` with create-if-absent; and
2. update `CURRENT` from `N-1` to `N`.

The two operations can complete in either order:

| Result | Recovery |
|---|---|
| Both succeed | `N` is current. |
| Object succeeds, pointer fails | Retry `CURRENT=N`. |
| Pointer succeeds, object fails | Readers return `N-1`; writers retry object `N`. |
| Object already exists | Another writer won `N`; refresh and return a write conflict. |

A writer that observes `CURRENT=N` with object `N` missing must retry `N`.
Advancing to `N+1` would create a gap and make reader fallback ambiguous.
Once object `N` exists, it remains part of the sequence after `CURRENT`
advances.

This version of `CURRENT` is an advisory cursor. Object creation still commits
a version, so RFC-0026 boundary files remain necessary. A stale writer could
otherwise recreate an ID deleted by GC.

`CURRENT` removes cold LISTs and the four-probe catch-up fallback. It also adds
a mutable pointer update to every write. Its steady-state request profile is:

| Operation | Cached probing | `CURRENT` cursor |
|---|---:|---:|
| Successful write | 1 object PUT + boundary GET | 1 object PUT + 1 pointer PUT + boundary GET |
| Warm unchanged read | 1 successor GET + boundary GET | 1 pointer GET + boundary GET |
| Cold latest read | 1 LIST + object GET + boundary GET | 1 pointer GET + object GET + boundary GET |

Both designs issue one lookup GET on an unchanged warm read. The pointer pays
an extra PUT on every write to avoid cold LISTs and catch-up probes. Long-lived
SlateDB processes with shared store instances should pay less with probing.
Short-lived readers or processes that lag many versions may favor a pointer.
A pointer that leads a failed object write adds a 404 GET and a fallback GET
until some writer fills the reserved ID.

Useful break-even inputs are:

```text
latest_reads
cold_list_fallbacks
probe_gets
successful_writes
```

The pointer costs less when the LIST and probe requests it avoids cost more
than one extra pointer PUT per successful write, including retries during write
contention.

### Authoritative CURRENT Pointer

`CURRENT` could become the commit point instead of a cursor. A stale writer
would write a candidate object and then conditionally update `CURRENT`. If the
conditional update failed, readers would ignore the candidate. This could
replace the GC boundary because recreating a deleted object would not publish
it.

That design changes the meaning of physical metadata files. A file could exist
without ever becoming a committed historical version. A crash after writing
deterministic object `N+1` but before updating `CURRENT` would also block later
create-if-absent attempts for `N+1`.

Writers could use unique candidate keys or skip occupied IDs, but both choices
change the current layout and historical listing semantics. Racing publication
before the candidate is durable can leave `CURRENT` pointing to a missing
object. Reader fallback can handle the missing object, but the pointer then
needs reservation and recovery rules.

Cached probing keeps object creation as the commit point and preserves
contiguous IDs. Existing tools can continue to treat physical sequenced
objects as history.

## Open Questions

None.

## References

- [RFC-0001: Manifest](0001-manifest.md)
- [RFC-0026: Garbage Collector Boundary Files for Sequenced Metadata](0026-garbage-collector-boundary.md)
- [slatedb/slatedb#1215: listed file missing before read](https://github.com/slatedb/slatedb/issues/1215)
