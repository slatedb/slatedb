# Garbage Collector Boundary Files for Sequenced Metadata

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Background](#background)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [Boundary Files](#boundary-files)
   - [Advancing Boundaries](#advancing-boundaries)
   - [Checking Boundaries After Writes](#checking-boundaries-after-writes)
   - [Garbage Collection](#garbage-collection)
- [Implementation](#implementation)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

SlateDB stores `.manifest` and `.compactions` state in sequenced
object-store files. The filename is the commit point: a writer creates the next
file with a create-if-absent operation, and success means the sequenced update
won.

This protocol is unsafe when a writer stalls longer than the garbage
collector's `min_age` setting. A stalled writer can prepare file `N+1`, another
writer can create and supersede `N+1`, GC can later delete `N+1`, and the
stalled writer can then resume and successfully create the same filename. The
stale writer observes success even though the original create-if-absent fencing
point should have rejected it.

This RFC adds durable boundary files for the `.manifest` and `.compactions`
namespaces. Before GC deletes old sequenced metadata files, it advances the
namespace boundary. After a writer creates a sequenced metadata file, it checks
the boundary before returning success. If the created ID is at or behind the
boundary, the write is treated as failed.

## Background

SlateDB has two sequenced metadata namespaces:

- `.manifest` files, named like `00000000000000000012.manifest`
- `.compactions` files, named like `00000000000000000013.compactions`

Both namespaces use the same object-store sequencing pattern:

1. Read the latest object ID.
2. Compute the next object ID.
3. Write the next object with create-if-absent.
4. Treat create success as the committed update.

GC currently deletes old metadata files using the existing retention rules:

- `.manifest`: delete files older than `min_age` when they are not the latest
  manifest and are not referenced by an active checkpoint.
- `.compactions`: delete files older than `min_age` when they are not the
  latest compactions file.

These rules assume that a writer will not pause between computing the next
object ID and creating that object for longer than `min_age`.

## Motivation

The unsafe sequence is the same for both metadata namespaces:

1. Writer A reads metadata file `N` and prepares an update to file `N+1`.
2. Writer A stalls before creating `N+1`.
3. Writer B creates `N+1`; later writers advance the namespace to `N+2`,
   `N+3`, and so on.
4. GC deletes `N+1` after it becomes old enough and is no longer retained by
   the namespace's normal retention rules.
5. Writer A resumes and creates `N+1` successfully because the object no
   longer exists.
6. Writer A treats the stale update as committed.

The root problem is that object-store create-if-absent only protects against
objects that currently exist. Once GC deletes a sequenced metadata object,
create-if-absent no longer remembers that the ID was already used.

## Goals

- Prevent stale `.manifest` and `.compactions` writes from returning success
  after GC has made their target IDs unsafe to reuse.
- Preserve the existing sequenced metadata write protocol and normal GC
  retention rules.
- Make boundary checks efficient enough for normal metadata writes.
- Make boundary implementation flexible enough to be used for WAL boundaries,
  should we want to add those in the future.

## Non-Goals

- Redesign metadata storage or replace sequenced object filenames as the commit
  point.
- Move metadata deletion into the writer or compactor processes.
- Cover storage namespaces other than `.manifest` and `.compactions`.

## Design

### Boundary Files

Add one boundary file per sequenced metadata namespace:

- `/gc/manifest.boundary`
- `/gc/compactions.boundary`

Each file contains a single ASCII-encoded `u64`:

```text
12
```

The value is an inclusive high-watermark. A boundary value `B` means that
metadata IDs `<= B` in that namespace must be treated as potentially deleted.
Writers must not treat a newly created sequenced metadata file with ID `i <= B`
as successful.

The protocol has two invariants:

1. Before GC may delete sequenced metadata file ID `i`, the durable boundary
   for that namespace must be `>= i`.
2. Before a writer may return success for sequenced metadata file ID `i`, it
   must observe the durable boundary for that namespace as `< i` after the file
   create succeeds.

If a boundary file has never existed, readers use boundary value `0`. If a
process has observed a boundary file and later finds it missing, it panics
because GC must never delete boundary files.

### Advancing Boundaries

GC advances the boundary before deleting old metadata files from a namespace.

For a namespace, GC computes the desired boundary as:

1. List the namespace's metadata files.
2. Remove the most recent metadata file from the list (it must always be kept).
3. Keep files in the list whose object-store `last_modified` timestamp is older
   than `min_age`.
4. Choose the maximum file ID from that filtered list.

If no files are old enough, GC skips the boundary update for that namespace.

Boundary updates are monotonic:

1. Read the current boundary value and object version metadata.
2. If the desired boundary is less than or equal to the current value, the
   boundary is already advanced.
3. If the boundary file is missing, create it with create-if-absent.
4. Otherwise, update it with a conditional object-store update using the
   version metadata from step 1.
5. If a concurrent GC wins the conditional update race, retry until the durable
   boundary is greater than or equal to the desired value.

The boundary file must never move backward.

### Checking Boundaries After Writes

Every sequenced metadata write must check the corresponding boundary after the
create-if-absent operation succeeds:

1. Create the next metadata file with create-if-absent.
2. If create-if-absent fails because the object already exists, return the
   existing sequenced write conflict error.
3. Read the namespace boundary.
4. If the just-created ID is less than or equal to the boundary, return the
   existing sequenced write conflict error and do not report the write as
   committed.
5. Otherwise, return success.

Boundary reads can be optimized with an in-memory cache and conditional GETs
using `If-None-Match`. If the object store returns "not modified", the writer
can reuse the cached boundary value. If it returns a new value, the writer
updates the cache and checks the created ID against that value.

The boundary read must not be served from a stale object cache. Manifest and
compactions stores must not use `CachedObjectStore`.

### Garbage Collection

After advancing a namespace boundary, GC continues to apply the normal deletion
rules:

- `.manifest`: delete files at or behind the boundary when they are older than
  `min_age`, are not the latest manifest, and are not referenced by an active
  checkpoint.
- `.compactions`: delete files at or behind the boundary when they are older
  than `min_age` and are not the latest compactions file.

## Implementation

- Add `BoundaryObject` to the transactional object crate with:
  - `check(id)`: verify that `id` is greater than the durable boundary.
  - `advance(boundary)`: durably advance the boundary to at least `boundary`.
- Add `BoundedSequencedStorage<T>`, a `SequencedStorageProtocol<T>` wrapper
  that delegates the write and then calls `BoundaryObject::check` before
  returning success.
- Add `ObjectStoreBoundaryObject`, stored under `<root>/gc/<name>.boundary`,
  using ASCII `u64` encoding.
- Treat a write that created an ID at or behind the durable boundary as the
  existing sequenced write conflict error.
- Wrap `ManifestStore` with `manifest.boundary`.
- Wrap `CompactionsStore` with `compactions.boundary`.
- Add `advance_boundary` methods to the manifest and compactions stores for GC.
- Update manifest and compactions GC tasks to compute the maximum old-enough ID
  and advance the boundary before deleting files.
- Remove `CachedObjectStore` usage for `ManifestStore` and `CompactionsStore`
  to ensure boundary checks are not served from a stale cache. Enforce this by
  asserting `PutMode::Create` and all `GET`s are unconditional.

## Impact Analysis

SlateDB features and components that this RFC interacts with:

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

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
- [ ] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Block cache
- [x] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

<!-- Describe performance and cost implications of this change. -->

- Latency (reads/writes/compactions): metadata writes add one boundary check
  after each write.
- Throughput (reads/writes/compactions): metadata write throughput may drop with
  the extra object-store round trip.
- Object-store request (GET/LIST/PUT) and cost profile: checks can use cached
  ETags but must still GET; GC may add one conditional boundary update per
  namespace.
- Space, read, and write amplification: adds two small boundary files and no
  data-file amplification.

### Observability

<!-- Describe any operational changes required to support this change. -->

- Configuration changes: none.
- New components/services: none.
- Metrics: track boundary check latency, advance attempts, and rejected stale
  writes.
- Logging: warn on boundary advance failures and stale write rejections.

### Compatibility

<!-- Describe compatibility considerations with existing versions of SlateDB. -->

- Existing data on object storage / on-disk formats: missing boundary files
  read as boundary `0`; new files are additive.
- Existing public APIs (including bindings): no API changes.
- Rolling upgrades / mixed-version behavior (if applicable): mixed versions
  retain the existing stale-writer risk until all writers check boundaries.

## Testing

<!-- Describe the testing plan for this change. -->

- Unit tests: standard unit tests for new code paths.
- Integration tests: None.
- Fault-injection/chaos tests: None.
- Deterministic simulation tests: DST covers this pattern. Draft PR triggered
  expected failures.
- Formal methods verification: SequencedMetadataBoundary.fizz is included in
  the formal verification suite.
- Performance tests: None.

## Rollout

<!-- Describe the plan for rolling out this change to production. -->

- Milestones / phases: None.
- Feature flags / opt-in: None.
- Docs updates: files.mdx and gc.mdx will be updated to explain boundary files
  and their role in GC safety.

## Alternatives

### Status quo

- Keep relying on `min_age` as a writer-stall bound.
- Rejected because pathological stalls can still allow stale sequenced metadata
  writes to commit.

### Increase `min_age`

- Reduces the probability of the bug.
- Rejected because it does not eliminate the failure mode and increases
  metadata retention.

## References

- [slatedb/slatedb#1646: Add `BoundaryObject` GC watermarks to prevent data
  loss in slatedb-txn-obj][pr-1646]
- [slatedb/slatedb#1622: pathological data-loss configuration][issue-1622]
- [OSWALD](https://nvartolomei.com/oswald/): a WAL implementation with a
  similar boundary concept. OSWALD's "snapshot" is our `.manifest` (or
  `.compactions`), and its "manifest" is our boundary file. This is a rough
  analogy, not a direct mapping, but the manifest serves a similar purpose.

[pr-1646]: https://github.com/slatedb/slatedb/pull/1646
[issue-1622]: https://github.com/slatedb/slatedb/issues/1622
