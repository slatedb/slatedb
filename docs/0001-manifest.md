# Manifest Design

## File Structure

```
some-bucket/
├─ manifest/
│  ├─ current
│  ├─ 000000000000000000.manifest
│  ├─ 000000000000000001.manifest
│  ├─ 000000000000000002.manifest
├─ level_0/
│  ├─ 000000000000000000.sst
│  ├─ 000000000000000001.sst
│  ├─ ...
├─ compacted/
│  ├─ 000000000000000000.sst
│  ├─ 000000000000000001.sst
│  ├─ ...
```

### `manifest/current`

A file that contains a pointer to the current manifest file.

For example, if `current` contains:

```
1
```

Then `manifest/000000000000000001.manifest` is the current manifest.

All mutations to `manifest/current` must be done using CAS. AWS users will need to use a DynamoDB implementation for manifest management.

### `manifest/000000000000000000.manifest`

A file containing writer, commpaction, and snapshot information--the state of the database at a point in time.

#### Structure

_NOTE: Described as proto3, but the manifest could be any format we choose._

```proto
syntax = "proto3";

message Manifest {
  // The current writer's epoch.
  uint64 writer_epoch = 1;

  // The current compactor's epoch.
  uint64 compactor_epoch = 1;

  // The most recent `level_0` SST that's been compacted. Note that keeping
  // just the `level_0` low-water mark means `level_0` compaction must be
  // sequetional not random. Also note that this SST must alwyas exist in
  // `level-0`; the commpactor must not remove it.
  uint64 last_compacted_l0_sst = 2;

  // A list of the SST IDs that are valid to read in the `compacted` folder.
  repeated uint64 compacted_ssts = 3;

  // A list of snapshots that are currently open.
  repeated Snapshot snapshots = 4;
}

message Snapshot {
  // A random ID that must be unique across all open snapshots.
  uint32 id = 1;

  // The manifest ID that this snapshot is using as its `DbState`.
  uint64 manifest = 2;

  // The UTC seconds timestamp that this snapshot was created at.
  uint32 timestamp_ms = 3;
}
```

#### Size

Manifest size is important because manifest updates must be done transactionally. In object storage, this is done using a compare-and-swap (CAS). The longer the read-modify-write takes, the more likely there is to be a conflict. Consequently, we want our manifest to be as small as possible.

The size calculation (in bytes) for the manifest is:

```
  8                         // writer_epoch
+ 8                         // compactor_epoch
+ 8                         // last_compacted_l0_sst
+ 4 + 8 * compacted_ssts    // array length + compacted_ssts
+ 4 + 16 * snapshots        // array length + snapshots (16 bytes each)
```

Conservatively, a manifest with 1000 snapshots and 100,000 compacted SSTs would be:

```
  8                         // writer_epoch
+ 8                         // compactor_epoch
+ 8                         // last_compacted_l0_sst
+ 4 + 8 * 100000            // array length + compacted_ssts
+ 4 + 16 * 1000             // array length + snapshots (16 bytes each)
= 816032
```

This comes out to 816032 bytes, or ~796KiB. Whether this is reasonable or not depends on how frequently manifests are updated. As we'll see below, updates should be infrequent enough that a 1 MiB manifest isn't a problem.

## Updates

A full read-modify-write Manifest update contains the following steps:

1. Read `manifest/current` to find the current manifest. Let's say it's `3` in this example.
2. Read `manifest/000000000000000003.manifest` to read the current manifest.
3. Update the manifest in memory.
4. Write the updated manifest to `manifest/000000000000000004.manifest` using a "create-if-not-exists" `PUT`.
5. If (4) was successful, write `4` to `manifest/current` using compare-and-swap (CAS) with the `manifest/current` version from (1).

Three different processes update the manifest:

* **Readers**: Readers can update `snapshots` to create or remove a state snapshot. 
* **Writers**: Writers must update `writer_epoch` once on startup.
* **Compactors**: Compactors must update `compactor_epoch` once on startup. Compactors must also update `last_compacted_l0_sst` and `compacted_ssts` after each compaction pass.

The union of these three processes means the manifest is updated whenever:

1. A new snapshot is created
2. An existing snapshot is removed
3. A new writer is starts
4. A new compactor starts
5. A compactor finishes compaction

Let's look at each of these

## Snapshots

SlateDB's state on object storage is constantly changing. The writer is adding new SSTs to `level_0`. The compactor is adding/removing SSTs in `compacted` and removing them SSTs from `level_0`. SlateDB clients keep their in-memory state in a `DbState` object. The client's in-memory state might end up referencing an SST that's been deleted by the compactor.

Clients can ensure SSTs aren't deleted by writing a new manifest with a snapshot containing the current manifest ID. The snapshot's manifest ID signals that the SSTs contained in that manifest are still in use.

A manifest is considered active as long as:

* It's the current manifest
* It's referenced by a snapshot in the current manifest

A compactor may not delete SSTs that are still in use by any active manifest.

The set of SSTs that are considered in use by an active manifest are:

* All `compacted` SST files with IDs listed in the manifest's `compacted_ssts`
* All `level_0` SST files with IDs >= the manifest's `last_compacted_l0_sst`

_NOTE: The inclusive `>=` for `last_compacted_l0_sst` is required so we can get the `writer_epoch` of the `last_compacted_l0_sst` file. This is important to know for the recover process detailed in the Read-only section below._

### Lifecycle

Any client may create a snapshot. Only two processe may remove a snapshot:

* The client that created it
* The compactor

A client that creates a snapshot may remove it at any time by writing a new manifest with the snapshot removed.

The compactor may remove any snapshot from the manifest that has a `timestamp_ms` < now() - `snapshot_timeout_s` (a new compactor config). For example, a compactor with `snapshot_timeout_s` set to `120` is allowed to remove snapshots from the manifest that are older than 120 seconds.

### Renewing

Clients that want long-running snapshots may renew (or heartbeat) their snapshots by writing a new manifest that contains their existing snapshot with an updated `timestamp_ms`. As long as the client does this more frequently than `snapshot_timeout_s`, the SSTs in the manifest they're using will never be deleted.

## Read-only

On startup, a read-only process needs to find the currently available l0 and compacted SSTs in object storage. This requires the following steps:

1. Read `manifest/current` to find the current manifest.
2. Read the current manifest (e.g. `manifest/000000000000000003.manifest`).
3. Store all SST IDs from `compacted_ssts` in memory.
4. List all valid `level-0` SSTs into memory.

To determine the valid SSTs in `level-0`, we run the following logic:

```rust
// Get the writer epoch from the most recently compacted level-0 SST file
let mut current_writer_epoch = table_store.get_writer_epoch(manifest.last_compacted_l0_sst) or 0
let mut current_sst_id = state.last_compacted_l0_sst or 0;

// Read all contiguous SSTs starting from the first SST after last_compacted_l0_sst
while Some(sst_info) = table_store.open_sst(current_sst_id + 1) {
  // We've found a new writer_epoch, so seal all future writes from older ones
  if sst_info.writer_epoch > current_writer_epoch:
    current_writer_epoch = current_writer_epoch
  // We've found a valid write, add the SST to the DbStat's l0
  if sst_info.writer_epoch == current_writer_epoch:
    state.l0.insert(0, sst_info);
  // if sst_info.writer_epoch < current_writer_epoch:
  //   ignore since this is a zombie write (a higher epoch has previously written an SST)

  // Advance to the next contigous SST ID
  current_sst_id += 1;
}
```

At this point, the client has a list of all compacted SST IDs and all valid uncompacted `level-0` SSTs.

From here, read-only processes get to decide how they want to manage their in-memory state using any combination of:

* Creating a snapshot (as described above)
* Re-running (1-5) lazily when an object_store SST read results in a 404 (the SST was deleted)
* Re-running (1-5) periodically
* Periodically listing `level-0` to detect new `level-0` SSTs

## Read-write

On startup, a read-writer process needs to increment the `writer_epoch` in the manifest and create a new read snapshot:

1. Read `manifest/current` to find the current manifest.
2. Read the current manifest (e.g. `manifest/000000000000000003.manifest`).
3. Update the `writer_epoch` and `snapshot` manifest sections in memory.
4. Write the updated manifest to the next manifest (e.g. `manifest/000000000000000004.manifest`) using a "create-if-not-exists" `PUT`.
5. If (4) was successful, write the next manifest ID (e.g. `4`) to `manifest/current` using compare-and-swap (CAS) with the `manifest/current` version from (1).

At this point, the `writer_epoch` in the manifest has been transactionally incremented and a snapshot has been reserved for consistent reads.

Next, the writer must fence all previous writers. It does this by writing an empty SST (with its `writer_epoch`) to next open SST ID slot starting from `last_compacted_l0_sst`.

_NOTE: Sorry about the horrible pseudocode. You get the idea._

```rust
// Get the writer epoch from the most recently compacted level-0 SST file
let mut current_writer_epoch = manifest.writer_epoch
let mut current_sst_id = manifest.last_compacted_l0_sst + 1

// Try and write the SST (using put-if-absent CAS or S3 versioned-based first-writer-wins)
while table_store.write_sst(current_sst_id, empty_sst) match {
  file:
    if file.metadata.writer_epoch > current_writer_epoch:
      panic!("We've been fenced");
    else:
    // If the file was from an older writer, so simply go to the next slot
    current_sst_id += 1;
  none:
    // We found an open slot
    break;
}

next_sst_id = current_sst_id + 1
```

We've now fenced all previous writers. The SSTs are contiguious and monotonically increasing up to the writer's `next_sst_id`.

Now the writer must load the valid `compacted` and `level-0` SSTs from 

1. Following the steps in 

There might still be SSTs ahead of `next_sst_id`; this is unavoidable. Zombie writers might have had parallel writes outstanding, or a subsequent writer might have fenced the current writer out already. We'll deal with this now.

If a writer tries to write an SST, but the SST already exists,  

### Getting Fenced

### Zombie Writers

### Ack'ing

## Compactors

Updates 1-4 will be relatively in frequent (on the order of minutes, or longer).

Compactor updates are important to consider because `level_0` compactions must happen frequently to keep startup times down. SSTs can be created in `level_0` write volume frequently exceeds `flush_bytes` ([#30](https://github.com/slatedb/slatedb/issues/30)) or if `flush_ms` is very low. A writer with a `flush_ms` set to 1 would create 1000 SSTs per-second.

















## Writer

### Startup

On startup, a writer process needs to do several things:

1. Establish a snapshot of the current manifest.
2. Load snapshotted manifest into `DbState`.
3. Recover unsnapshotted `DbState` from the remaining`level_0` SSTs.
4. Fence all previous writers.

#### Snapshotting the Current Manifest

Before the writer can do anything, it needs to snapshot the manifest. Doing so will ensure that the `level_0` SSTs won't be deleted by the garbage collector while we're reading them. Snapshotting requires these steps:

1. Loads `manifest/current` (if it exists)
2. Loads `manifest/<manifest-id>.json` that `manifest/current` points to if (1) exists, else create an empty manifest
3. Add a new entry to the `snapshots` field in the manifest loaded in (2)
4. Write the new manifest from (3) into `manifest/<manifest-id + 1>.json` (or `0.json` if it's the first)
5. Update `manifest/current`

Object stores that support CAS will use CAS for steps (4) and (5).

On startup a writer process:

1. Loads `manifest/current` (if it exists)
2. Loads `manifest/<manifest-id>.json` that `manifest/current` points to (if (1) exists)
3. Insert the SsTableInfos from the `compacted_ssts` field (loaded in (2)) into the in-memory DbState. Otherwise, DbState starts empty.
4. Set DbState.last_compacted_l0_sst from the `last_compacted_l0_sst` field (loaded in (2)).
5. Recover level_0 SSTs:

```rust
let mut current_writer_epoch = state.compacted.map(SsTableInfo::writer_epoch).max();
let mut current_sst_id = state.last_compacted_l0_sst or 0;

// Read all contiguous SSTs starting from the first SST after last_compacted_l0_sst
while Some(sst_info) = table_store.open_sst(current_sst_id + 1) {
  // We've found a new writer_epoch, so seal all future writes from older ones
  if sst_info.writer_epoch > current_writer_epoch:
    current_writer_epoch = current_writer_epoch
  // We've found a valid write, add the SST to the DbStat's l0
  if sst_info.writer_epoch == current_writer_epoch:
    state.l0.insert(0, sst_info);
  // if sst_info.writer_epoch < current_writer_epoch:
  //   ignore since this is a zombie write (a higher epoch has previously written an SST)

  // Advance to the next contigous SST ID
  current_sst_id += 1;
}

state.next_sst_id = current_sst_id;
state.writer_epoch = current_writer_epoch + 1;
```

We now have fully recovered state. There might still be SSTs that we haven't seen if they occur after a gap. For example, if we have l0=[1, 2, 4, 5], current_sst would be set to 3 at this point, and 4 and 5 would not be in state.l0.

Let's consider this set of SSTs with the current manifest containing `last_compacted_l0_sst=000000000000000000.sst`:

```
├─ level_0/
│  ├─ 000000000000000000.sst (writer_epoch=1)
│  ├─ 000000000000000001.sst (writer_epoch=1)
│  ├─ 000000000000000002.sst (writer_epoch=2)
│  ├─ 000000000000000003.sst (writer_epoch=1)
│  ├─ 000000000000000005.sst (writer_epoch=3)
```

The startup protocol would end with:

* `next_sst_id=4` since `000000000000000004.sst` is the first missing SST
* `writer_epoch=3` since the maximum `writer_epoch` with SST ID < `next_sst_id` is `writer_epoch=2` (from 000000000000000002.sst)
* `l0=[000000000000000001.sst, 000000000000000002.sst]`

L0 excludes the following SSTs because:

* `000000000000000000.sst` because it's in the compacted set as defined by `last_compacted_l0_sst`
* `000000000000000003.sst` because its `writer_epoch` is less than a previous SST's (`000000000000000002.sst` in this example)
* `000000000000000005.sst` because a previous SST was missing (`000000000000000004.sst` in this example)

_NOTE: The writer does not snapshot or update the manifest on startup. This can cause two processes to both start simultaneously and end up with the same `writer_epoch`. This is fine; their conflict will get detected and resolved when they write conflicting `level-0` SSTs (see below)._

### Writing

#### `put()`

1. Client calls `put(k, v).await?`.
2. If kv-pair size is >= `skip_memtable_bytes`, a new MemTable is created and inserted directly into `state.imm_memtables`. `inner.flush_imms()` is called.
3. Else if `flush_bytes` is exceeded, `inner.flush()` is called.
4. Else when `flush_ms` is exceeded, `inner.flush()` is called.
5. The `put()` method returns `self.inner.put().await?`.

In all three cases, the kv-pair has been put into an immutable MemTable in `state.imm_memtables` and `flush_imms` has been invoked.

#### `flush()`

We allow parallel MemTable flushing now. Consequently, we have a race condition where later `level_0` SSTs might appear in the object store *before* earlier SSTs are visible. For example, continuing with our prevoous example, we might see:

```
├─ level_0/
│  ├─ 000000000000000000.sst (writer_epoch=1)
│  ├─ 000000000000000001.sst (writer_epoch=1)
│  ├─ 000000000000000002.sst (writer_epoch=2)
│  ├─ 000000000000000003.sst (writer_epoch=1)
│  ├─ 000000000000000005.sst (writer_epoch=3)
```

In this case, a writer would start with a state of `next_sst_id=4` and `writer_epoch=3` (excluding fields like `l0` for brevity).

There are several possible outcomes when we try to write the next SST (`000000000000000004.sst`) to object storage:

1. `000000000000000004.sst` does not exist, and the write goes through
2. `000000000000000004.sst` does exist, but has a `writer_epoch` < our `writer_epoch`
3. `000000000000000004.sst` does exist, and has a `writer_epoch` == our `writer_epoch` but `crc32` != our `crc32`
4. `000000000000000004.sst` does exist, and has a `writer_epoch` == our `writer_epoch` and `crc32` = our `crc32`
5. `000000000000000004.sst` does exist, but has a `writer_epoch` > our `writer_epoch`
6. `000000000000000004.sst` fails because of an object storage issue (e.g. network issue, 503, etc.)

In case (1), we are successful, so we move on to notifying listeners of the successful write (see `await` section below).

In case (2), a zombie process wrote an SST. We must fail all in-flight writes (including this one) and run the startup process again so we get a new valid `next_sst_id` to start from.

In case (3), we've detected that another process also started up while we were running our startup process. This is the race condition mentioned in the "Startup" section above. In this case, we're the zombie because the file's `crc32` doesn't match ours. We must fail all in-flight writes (including this one) and run the startup process again so we get a new valid `next_sst_id` to start from.z

In case (4), we've detected that another process also started up while we were running our startup process. Luckily, we won the race since our `crc32` matches what's in object storage. The other process is the zombie and will restart its startup process to get a new `writer_epoch`.

#### `await`

This is valid and acceptable. Following the startup protocol defined above, the process will determine 

However, it presents a problem:

1. A `put()` should not be acknowledged successfully until its MemTable is written to object storage *and* all previous in-flight MemTables have been written to storage.
2. 
a. The MemTable that's being moved to `state.imm_memtables` must be assigned `state.next_sst_id` and `state.next_sst_id` must be incremented. This is because we're now allowing parallel writes. To maintain strict ordering, the MemTable must be assigned the next contiguous ID as soon as it's frozen (rather than asynchronously in `flush.rs` as it currently is).
b. The new MemTable that's being created must include a pointer to the old MemTable that's being moved to `state.imm_memtables`. This is so the new MemTable can `.await?` for the older MemTable to flush successfully before the new MemTable returns a successful `Result` on its own `put()` calls. This way, if a MemTable flush fails, its failure will cascade to all future MemTable flushes that were in flight.


Asynchronously, `flush()` does the following:

1. Encode 

Asynchronously, `flush()` will encode the MemTable as an SST and write it to `"level_0/{%18d}.sst".format(next_sst_id)`.

If `If-None-Match: "*"` or an equivalent CAS operation is supported, the flusher will use a CAS opeartion to only put if the SST file doesn't exist.

If CAS is not supported, the flusher will 

In all three cases, when the MemTable is moved to `state.imm_memtables`, the MemTable will need to be assigned the SST id `state.next_sst_id`

6. 

2. The kv-pair is inserted into a mutable MemTable.
3. After `flush_ms`, if `flush_bytes` is exceeded, or if the kv-pair size is >= `skip_memtable_bytes`, move the MemTable into state.imm_memtables and create a new state.memtable if needed.
4. 


## Readers

## Compaction

## Garbage Collection

1. Loads latest manifest.
2. Fetches