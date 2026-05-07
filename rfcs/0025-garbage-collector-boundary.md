# Garbage Collector Boundary Files

<!-- Replace "RFC Title" with your RFC's short, descriptive title. -->

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

SlateDB currently relies on a combination of heuristics and time boundaries to:

1. Prevent the garbage collector (GC) from deleting files it shouldn't, and
2. Properly detect that a client has been fenced.

If a client exceeds time boundaries, these two guarantees do not hold. In most cases, this is not an issue. A properly configured client should never be stalled for longer than the time boundaries. Still, in [pathological configurations](https://github.com/slatedb/slatedb/issues/1622), it can cause data loss.

This RFC introduces a boundary file to remove timing dependencies from the GC safety and fencing protocols.

## Background

We currently GC four file types:

- `.manifest` (sequential flatbuffer, e.g. 00000000000000000012.manifest)
- `.compactions` (sequential flatbuffer, e.g. 00000000000000000013.compactions)
- WAL files (sequential SSTs, e.g. 00000000000000008495.sst)
- compacted SSTs (ULID-based SSTs, e.g. 01KQWJPHB2GE3KJE07JV5NXGJA.sst)

The files fall into three categories:

1. Sequenced storage protocol files (`.manifest` and `.compactions`)
2. Sequenced SST files (WAL files)
3. ULID-based SST files (compacted SSTs)

These categories follow two write patterns:

1. Sequenced write protocol (00000000000000000012, 00000000000000000013, 00000000000000000014, and so on)
2. ULID-based write protocol (01KQWJPHB2GE3KJE07JV5NXGJA, 01KQWTMTRPZJWAF6M19YZEHJT4, and so on)

The files are written by three writer types:

- The writer, which owns `writer_epoch`
- The compactor, which owns `compactor_epoch`
- External writers such as `admin.rs`, `DbReader`, and `Compactor` (when `Compactor` writes to `.manifest`).

All write patterns currently depend on time windows to protect recent writes against concurrent GC operations:

- `.manifest`: GC deletes all manifest files that are...
    - older than `min_age`, and
    - not the current manifest file, and
    - not referenced by any checkpoint manifest in the current manifest file
- `.compactions`: GC deletes all compactions files that are...
    - older than `min_age` and
    - not the current compactions file
- `/wal`: GC deletes all WAL files that are...
    - older than `min_age`, and
    - older than `manifest.replay_after_wal_id`, and
    - not referenced by an active checkpoint manifest (not between its `replay_after_wal_id` and `next_wal_sst_id`)
- `/compacted`: GC deletes all compacted SST files that are...
    - older than `min_age`, and
    - not referenced by the active manifest, and
    - not referenced by any active checkpoint manifest, and
    - older than the oldest compaction job's `compaction.id.timestamp()` in the current compactions file, and
        - else older than Unix epoch 0 if there has never been any compaction job, and
    - older than the most recent L0 in the latest manifest
        - else older than `l0_last_compacted` if there are no current L0s
        - else older than Unix epoch 0 if there has never been any L0 (fresh DB)

NOTE: `min_age` evaluates against the object store's metadata timestamp for `.manifest`, `.compactions`, and WAL files. For compacted SST files, `min_age` evaluates against the ULID timestamp, which is based on wall-clock time of the writer machine at the time of SST file creation.

## Motivation

Each of the `min_age` time windows creates a vulnerability for stalled writers:

- `.manifest` / `.compactions` GC:
    - t0: writer A reads manifest N and prepares update N -> N+1.
    - t1: writer A stalls before put(Create, N+1).
    - t2: writer B writes N+1; later writers advance to N+2, N+3, ...
    - t3: N+1.manifest is older than `min_age`, not current, and not checkpoint-referenced, so GC deletes it.
    - t4: writer A resumes and put(Create, N+1.manifest) succeeds because N+1 no longer exists.
    - t5: writer A treats its stale manifest update as committed, even though the normal create-if-absent CAS path should have rejected it.
- `/wal`:
    - t0: writer A prepares to write N.sst.
    - t1: writer A stalls before put(Create, N.sst).
    - t2: writer B writes N.sst; later writers advance WALs to N+1, N+2, and writes a new manifest that advances `replay_after_wal_id` to N+1.
    - t3: N.sst is older than `min_age`, older than `manifest.replay_after_wal_id`, and outside any active checkpoint WAL range, so GC deletes it.
    - t4: writer A resumes and put(Create, N.sst) succeeds because N.sst no longer exists.
    - t5: writer A treats its stale WAL write as committed, even though the normal create-if-absent fencing path should have rejected it.
- `/compacted`:
    - t0: writer A uploads ULID SST X.sst to /compacted.
    - t1: writer A stalls before publishing the manifest update that references X.
    - t2: writer B flushes and publishes a newer L0 SST Y.sst whose ULID timestamp is greater than X's timestamp, so the latest manifest's most-recent-L0 cutoff moves past X.
    - t3: the compactor publishes a newer compaction job J whose `compaction.id.timestamp()` is greater than X's timestamp, and any older compaction jobs complete, so the current `.compactions` file's oldest-job cutoff also moves past X.
    - t4: X.sst is older than `min_age`, unreferenced by active/checkpoint manifests, and older than both compacted-SST cutoffs, so GC deletes X.
    - t5: writer A resumes and successfully publishes a manifest update referencing X.
    - t6: the latest manifest now points at a missing SST.

## Goals

- Prevent the garbage collector from deleting files that might subsequently be added to a `.manifest` or `.compactions` file by a stalled writer.
- Prevent stalled writers from returning success for writes when they have already been fenced.

## Non-Goals

- Move garbage collection logic in-process (e.g. have `Db`/`Compactor` delete `.manifest`/`.compactions`/`.sst` files).
- Significantly modify the existing protocols.

## Design

In this RFC, we will fix these unsafe write windows by:

1. Introducing `.boundary` files for files whose names are the create-if-absent commit point
2. Updating each `.boundary` file prior to garbage collection
3. Preventing any boundary-tracked write from returning success if it precedes the current `.boundary` value
4. Enforcing GC cutoff rules when adding compacted SST references to `.manifest` and `.compactions` files

### Boundary files

We will add a `.boundary` file for each storage file type whose filename is the commit point:

- `/gc/manifest.boundary`
- `/gc/compactions.boundary`
- `/gc/wal.boundary`

Each boundary file contains a single ASCII-encoded `u64` integer:

- `/gc/manifest.boundary`: 12
- `/gc/compactions.boundary`: 13
- `/gc/wal.boundary`: 8495

These values are inclusive numeric high-watermarks over the file IDs in each namespace. A boundary value `B` means that GC has durably fenced every boundary-tracked file ID `i <= B`. No boundary-tracked write may be treated as successful unless it observes `B < i` after the write occurred.

The boundary protocol has two invariants:

1. Before GC may delete boundary-tracked file ID `i`, the durable boundary for that namespace must be `>= i`.
2. Before a writer may return success for boundary-tracked file ID `i`, it must observe the durable boundary for that namespace as `< i` after the write occurred.

There is intentionally no separate boundary file for compacted SSTs. Compacted SST writes are tentative data-file writes. They only become visible when a later `.manifest` or `.compactions` file references them; those metadata files are protected by their own boundary files. Compacted SST safety therefore comes from the GC cutoff validation rules defined in _GC cutoff rule enforcement_, below.

Boundary files will always live in the `main` object store (even if a `wal` object store is used for WAL files).

_TODO: Do we want to colocate /gc/wa.boundary in the WAL store? If a WAL object store is used specifically to reduce latency, it would make sense to colocate the boundary file there to keep latency low when doing WAL boundary checks._

#### Boundary file updates

The garbage collector updates the boundary files prior to each GC run.

1. GC calculates the new boundary value (see _Garbage collector changes_, below), which must be numerically greater than the current value.
2. GC reads the current boundary value and ETag.
3. GC verifies the new boundary value is numerically greater than the current boundary value.
    a. If not, the GC is skipped since the current boundary is already greater than or equal to the new boundary.
4. GC updates the boundary file using `PUT If-Match` with the ETag from (2).
    a. If the update succeeds, the new boundary value is in effect.
    b. If the update fails with a precondition error, the GC is skipped since another GC has updated the boundary file.

After the boundary file is updated successfully, the GC may proceed with its deletion process (see _Garbage collector changes_).

Multiple GC processes may run concurrently. If they contend on the boundary file update, one will succeed and the others will skip since they observe the updated boundary.

#### Boundary file checks

Each boundary-tracked writer must follow this protocol:

1. Write the file with a create-if-absent operation.
    a. If the write fails because the file already exists, return a `ObjectVersionExists` error.
2. Read the boundary file.
    a. If the boundary file does not exist, .. ?
3. Verify the just-written file ID is numerically greater than the boundary value read in (2).
    a. If not, return a `ObjectVersionExists` error.
4. Return success.

This protocol can be optimized to:

1. cache the previously read boundary value and ETag in memory, and
2. `GET If-None-Match` the boundary file after each write.
    a. If it's 304, the boundary is unchanged and the cached value is used.
    b. If it's 200
        - If the new value is >= the just-written file ID, return an `ObjectVersionExists` error.
        - Else, update the in-memory boundary value and ETag, and treat the write as successful.

We can further optimize this protocol to poll the boundary file in the background and keep the value updated in memory. Polling is left as future work if the added latency of a boundary file read on each write proves to be a problem in practice.

### GC cutoff rule enforcement

We must enforce GC cutoff rules when adding compacted SST references to `.manifest` and `.compactions` files. This prevents writers from adding references to SSTs that are already past the GC cutoff and thus vulnerable to deletion.

We will enforce the following rules:

- Newly added L0 SSTs in `.manifest` must have an SST ULID timestamp greater than both `last_compacted_l0_sst_view_id.id().timestamp()` and `max(l0.id().timestamp())` across all tree segments (including the root).
- Newly added SR SSTs in `.manifest` or `.compactions` must have an SST ULID timestamp greater than the compaction job's ID timestamp.
- Newly added compaction jobs in `.compactions` must have an ID timestamp greater than the most recent compaction job's ID timestamp in the file.

These rules guarantee:

- Untracked L0s are greater than every tree segment's `last_compacted_l0_sst_view_id` (including the root).
- Untracked SR SSTs are > the oldest compaction job ID in the current `.compactions` file.

Together with `manifest.boundary` and `compactions.boundary`, these rules make a separate compacted SST boundary file unnecessary. If an unreferenced compacted SST has become eligible for deletion, then a later attempt to publish it must either fail the `.manifest`/`.compactions` boundary check or fail cutoff validation.

## Implementation

### Writer changes

The following write paths must be updated to follow the boundary file check protocol described above:

- `ObjectStoreSequencedStorageProtocol::write` (covers `.manifest` and `.compactions` files) must check and return `ObjectVersionExists` if the boundary value has advanced to or past the just-written file ID.
- `TableStore::write_sst` (covers `wal.boundary`) must check and return `Fenced` if the boundary value has advanced to or past the just-written WAL ID.

If no boundary file has ever been seen, the check is skipped. If a boundary file has been seen, but no longer exists, panic.

`TableStore::write_sst` and `TableStore::table_writer(...).close()` do not perform boundary checks for `SsTableId::Compacted`.

Manifest update validation for newly added L0 SSTs should return `InvalidClockTick` when the SST timestamp is not greater than the L0 compaction watermark it must clear. We will need to update the MemtableFlusher to retry `InvalidClockTick` errors with a new SST ULID.

_TODO: More detail on the memtable flusher retry logic._

We must also update the manifest/compactions file writing logic enforce GC cutoff rules. We can do this by adding `TransactionalObject::validate()`. Dirty manifests and compactions can then compare against the current value to enforce the rules defined in _GC cutoff rule enforcement_.

### Garbage collector changes

The GC must be updated to compute the new boundary values for each boundary-tracked file type and update the boundary files prior to deleting any boundary-tracked files. It must also compute the compacted SST GC cutoff prior to deleting compacted SSTs.

#### Computing `manifest.boundary` and `compactions.boundary`

The GC computes the boundary as follows:

1. let `file_list` = `ManifestStore::list_manifests()/CompactionsStore::list_compactions()`
2. let `eligible_by_age` = `file_list.filter(|file| file.timestamp() <= now - min_age)`
3. let `boundary` = `eligible_by_age.map(|file| file.id).max()`

If no files are eligible by age, the `.boundary` update and GC process are skipped.

_NOTE: `timestamp()` represents the timestamp of the object in the object store._

#### Computing `wal.boundary`

The GC computes `wal.boundary` as follows:

1. let `manifest` = `ManifestStore::read_latest_manifest()`
2. let `wal_list` = `TableStore::list_wal_ssts()`
3. let `age_boundary` = `wal_list.filter(|wal| wal.timestamp() <= now - min_age).map(|wal| wal.id).max()`
4. let `replay_boundary` = `manifest.replay_after_wal_id.saturating_sub(1)`
5. let `boundary` = min(`age_boundary`, `replay_boundary`)

If no WAL files are eligible by age, or `manifest.replay_after_wal_id == 0`, the `.boundary` update and GC process are skipped.

_NOTE: `timestamp()` represents the timestamp of the object in the object store._

#### Computing the compacted SST GC cutoff

The GC computes the compacted SST GC cutoff as follows:

1. let `compactor_state` = `CompactorStateReader::read_view()`
2. let `compacted_list` = `TableStore::list_compacted_ssts()`
3. let `min_age_cutoff` = `compacted_list.filter(|sst| sst.id().timestamp() > min_age).min()`
4. let `writer_cutoff` = `manifest.trees.max(last_compacted_l0_sst_view_id.timestamp())`
5. let `compactor_cutoff` = `compactor_state.recent_compactions().map(|compaction| compaction.id.timestamp()).min()`
6. let `cutoff` = min(`min_age_cutoff`, `writer_cutoff`, `compactor_cutoff`)

If no `last_compacted_l0_sst_view_id` exists, the compacted SST GC process is skipped.

We intentionally drop the `manifest.l0` check in (4) that we used to perform, and instead only check `last_compacted_l0_sst_view_id` for all segments. This is a more conservative approach that leaves L0s around for longer. This is done to simplify the protocol.

_NOTE: `timestamp()` represents the 48-bit timestamp component of the SST's ULID._

#### Deletion process

For boundary-tracked files, the GC may delete:

- `.manifest` files with IDs less than or equal to `manifest.boundary` if they are not the latest manifest and are not referenced by any active checkpoint.
- `.compactions` files with IDs less than or equal to `compactions.boundary` if they are not the latest compactions file.
- WAL SST files with IDs less than or equal to `wal.boundary` if they are not referenced by any active manifest. A WAL SST is active when its ID is between `manifest.replay_after_wal_id` and `manifest.next_wal_sst_id`.

For compacted SST files, the GC may delete any SST that:

1. Has an SST ULID timestamp less than the compacted SST GC cutoff, and
2. Is not referenced by any active manifest.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

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

- [x] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

This will add latency to all object store writes, which is managed in two ways:

1. Keeping user-facing operations mostly non-blocking.
   a. Users generally write with `await_durable` set to `false`.
   b. By default, `.manifest` writes (L0 updates, and so on) occur in the background using the memtable flusher.
   c. By default, WAL writes occur in the background using the WAL buffer.
   d. Users that call `flush()` will still need to wait, which will incur the added latency.
2. Reduce the boundary check latency.
   a. Writers can refresh boundary files in the background periodically and save the value and ETag
   b. Writers call `GET If-None-Match` on the boundary file after writes. If it's 304, they can use their in-memory value. The [maximum p999 latency across Azure, GCS, S3, S3E1Z, GCS Rapid, and Tigris is 68ms](https://x.com/Sirupsen/status/2050895383866249618) (max p99=30ms, and max p50=14ms).

(2a) is left as future work if it's actually needed.

- Latency (reads/writes/compactions)
- Throughput (reads/writes/compactions)
- Object-store request (GET/LIST/PUT) and cost profile
- Space, read, and write amplification

### Observability

<!-- Describe any operational changes required to support this change. -->

- Configuration changes
- New components/services
- Metrics
- Logging

### Compatibility

<!-- Describe compatibility considerations with existing versions of SlateDB. -->

- Existing data on object storage / on-disk formats
- Existing public APIs (including bindings)
- Rolling upgrades / mixed-version behavior (if applicable)

## Testing

<!-- Describe the testing plan for this change. -->

- Unit tests:
- Integration tests:
- Fault-injection/chaos tests:
- Deterministic simulation tests:
- Formal methods verification:
- Performance tests:

## Rollout

<!-- Describe the plan for rolling out this change to production. -->

- Milestones / phases:
- Feature flags / opt-in:
- Docs updates:

## Alternatives

List the serious alternatives and why they were rejected (including “status quo”). Include trade-offs and risks.

## Open Questions

- Question 1
- Question 2

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- https://nvartolomei.com/oswald
- https://github.com/slatedb/slatedb/issues/1622

## Updates

Log major changes to this RFC over time (optional).

## TODO

- Add [OSWALD](https://nvartolomei.com/oswald/) reference and explain why it doesn't work (multiple writers editing single file e.g. .manifest)
- Explain why we don't use timing approaches discussed in #352.
- Add periodic boundary file refresher as an optional performance improvement.
- Non-goals: move garbage collection deletion in-process (have `Db` delete .manifest/.compactions/(wal).sst and `Compactor` delete (compacted).ssts), move boundary calculation in-process (have `Db` calculate manifest/compactions/wal boundary and `Compactor` calculate compacted boundary).
- Add rollout plan
- Include FizzBee proof in design.
- Do we care about CDC honoring boundary files?
- How is Db split/merge affected (if at all)?
