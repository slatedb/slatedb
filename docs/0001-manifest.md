# Manifest Design

## Current Design

SlateDB currently holds its state in an in-memory structure called `DbState`. `DbState` looks like this:

```rust
pub(crate) struct DbState {
    pub(crate) memtable: Arc<MemTable>,
    pub(crate) imm_memtables: Vec<Arc<MemTable>>,
    pub(crate) l0: Vec<SsTableInfo>,
    pub(crate) next_sst_id: usize,
}
```

These fields act as follows:

* `memtable`: The currently active mutable MemTable. `put()` calls insert key-value pairs into this field.
* `imm_memtables`: An ordered list of MemTables that in the process of being written to objet storage.
* `l0`: An ordered list of level-0 [sorted string tables](https://www.scylladb.com/glossary/sstable/) (SSTs). These SSTs are not range partitioned; they each contain a full range of key-value pairs.
* `next_sst_id`: The SST ID to use when SlateDB decides to freeze `memtable`, move it to `imm_memtables`, and flush it to object storage. This will be come the MemTable's ID on object storage.

SlateDB doesn't have compaction implemented at the moment. As such, we don't have an level-1+ SSTs in the database state.

### Writes

A `put()` is performed by inserting the key-value pair into `memtable`. Once `flush_ms` (a configuration value) has expired, a flusher thread (in `flush.rs`) locks `DbState` and performs the following operations:

1. Replace `memtable` with a new (empty) MemTable.
2. Insert the old `memtable` into index 0 of `imm_memtables`.
3. For every immutable MemTable in `imm_memtables`.
  1. Encode the immutable MemTable as an SST.
  2. Write the SST to object storage.
  3. Remove the MemTable from `imm_memtables`.
  4. Insert the SST's metadata (`SsTableInfo`) into `l0`.
  5. Increment `next_sst_id`.
  6. Nofity listeners that are `await`'ing a `put()` in this MemTable.

The `SsTableInfo` structure is returned when the SST is encoded in 3.1. It looks like this:

```rust
pub(crate) struct SsTableInfo {
    pub(crate) id: usize,
    pub(crate) first_key: Bytes,
    // todo: we probably dont want to keep this here, and instead store this in block cache
    //       and load it from there (ditto for bloom filter when that's implemented)
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) block_meta_offset: usize,
}
```

_NOTE: The block meta information in the `SsTableInfo` struct stores the location and first key for each block in the SST. SSTs are broken into blocks (usually 4096 bytes); each block has a sorted list of key-value pairs. This data can get somewhat large; we'll probably move it out of the in-memory `SsTableInfo`_

### Reads

Reads simply iterate over each MemTable and SST looking for the value for a key. This is done by:

1. Return `get(k)` from `memtable` if it exists.
2. Iterate over each `imm_memtables` (starting from index 0) and return `get(k)` if it exists.
3. Iterate over each `l0` SST (starting from index 0) and return `get(k)` if it exists.

## Problem

SlateDB loses the state of the database when the process stops.



## Solution

## Assumptions

* Only one writer client is allowed at a time
* Multiple reader clients are allowed at the same time
* Only one compactor client is allowed at a time
* Compactor, writer, and readers may all be on different machines
* Writer does not send parallel writes to the object store
* Manifest on object store requires compare-and-swap (CAS) or transactional store (DynamoDB)
* SSTs on object store require CAS or object versioning support

## Non-Goals

* Compaction dealts are out of scope
* Supporting out of order/parellel writes is out of scope

## Compare-and-Swap and S3

When object stores provide CAS (a.k.a. preconditions, conditionals, `If-None-Match`, `If-Match`, and so on), we can use it for all CAS operations.

When an object store does *not* provide CAS, there are two relevant scenarios:

1. The object we wish to CAS is immutable; it exists or it does not, but never changes once it exists ("create if not exists").
2. The object we wish to CAS is mutable.

In the mutable case (2), an object store can't be used to keep reads and writes consistent. A transactional store must be used instead (generally, DynamoDB is used).

In the immutable case (1), we can emulate CAS if the bucket has versioning enabled. Versioning allows multiple versions of an object to be stored. AWS stores versions [in insertion order](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html#API_ListObjectVersions_Example_3). We can leverage this property as well as AWS's [read-after-write](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/) to emulate CAS. Many writers can write to the same object path. Writers can then list all versions of the object. The first version (the earliest) is considered the winner. All other writers have failed the CAS operation. This does require a [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) after the write, but if CAS is needed, this is the best we can do.

This document calls out whether each CAS operation is immutable CAS and mutable CAS.

## Parallel Writes

TODO: Link to Rohan's idea for parallel write fencing.

## File Structure

```
some-bucket/
├─ manifest/
│  ├─ current
│  ├─ 000000000000000000.manifest
│  ├─ 000000000000000001.manifest
│  ├─ 000000000000000002.manifest
├─ wal/
│  ├─ 000000000000000000.sst
│  ├─ 000000000000000001.sst
│  ├─ ...
├─ levels/
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
  // Manifest format version to allow schema evolution.
  uint16 manifest_format_version = 1;

  // The current writer's epoch.
  uint64 writer_epoch = 2;

  // The current compactor's epoch.
  uint64 compactor_epoch = 3;

  // The most recent SST in the WAL that's been commpacted.
  uint64 wal_last_compacted_sst_id = 4;

  // The most recent SST in the WAL at the time Manifest was updated.
  uint64 wal_last_seen_sst_id = 5;

  // A list of the SST table info that are valid to read in the `compacted` folder.
  repeated SstInfo compacted_ssts = 6;

  // A list of snapshots that are currently open.
  repeated Snapshot snapshots = 7;
}

message SstInfo {
  // Globally unique ID for an SST in the `compacted` folder.
  uint64 id = 1;

  // The first key in the SST file.
  string first_key = 3;

  // There could be other fields here depending on the compaction style ...
}

message Snapshot {
  // A random ID that must be unique across all open snapshots.
  uint32 id = 1;

  // The manifest ID that this snapshot is using as its `DbState`.
  uint64 manifest_id = 2;
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

### `level_0/000000000000000000.sst`



#### Attributes

Each SST will have the following [user-defined metadata fields](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata) set:
 
* writer_epoch: This is a u64. Writers will set this when the put a new SST into `level_0`.

## Updates

A full read-modify-write Manifest update contains the following steps:

1. Read `manifest/current` to find the current manifest. Let's say it's `3` in this example.
2. Read `manifest/000000000000000003.manifest` to read the current manifest.
3. Update the manifest in memory.
4. Write the updated manifest to `manifest/000000000000000004.manifest` using a "create-if-not-exists" `PUT`.
5. If (4) was successful, write `4` to `manifest/current` using compare-and-swap (CAS) with the `manifest/current` version from (1).

(4) is a mutable CAS operation. A transactional store must be used if CAS is not available.
(5) is an immutable CAS operation. Object versioning may be used.

Three different processes update the manifest:

* **Readers**: Readers can update `snapshots` to create or remove a state snapshot. 
* **Writers**: Writers must update `writer_epoch` once on startup.
* **Compactors**: Compactors must update `compactor_epoch` once on startup. Compactors must also update `last_compacted_l0_sst` and `compacted_ssts` after each compaction pass.

The union of these three processes means the manifest is updated whenever:

1. A new snapshot is created
2. An existing snapshot is removed
3. A new writer starts
4. A new compactor starts
5. A compactor finishes compaction

Let's look at each of these.

## Snapshots

SlateDB's state on object storage is constantly changing. The writer is adding new SSTs to `level_0`. The compactor is adding/removing SSTs in `compacted` and removing them SSTs from `level_0`. SlateDB clients keep their in-memory state in a `DbState` object. The client's in-memory state might diverge from the SSTs on disk in two cases:

* The compactor deletes SSTs that the client is still referencing
* Another writer has written an unexpected SST in `level_0` (either they're a zombie or we are)

Snapshots solve the first issue. Clients ensure SSTs aren't deleted by writing a new manifest with a snapshot containing the current manifest ID. The snapshot's manifest ID signals that the SSTs contained in that manifest are still in use.

A manifest is considered active as long as either:

* It's the current manifest
* It's referenced by a snapshot in the current manifest

A compactor may not delete SSTs that are still in use by any active manifest.

The set of SSTs that are considered in use by an active manifest are:

* All `compacted` SST files with IDs listed in the manifest's `compacted_ssts`
* All `level_0` SST files with IDs >= the manifest's `last_compacted_l0_sst`

_NOTE: The inclusive `>=` for `last_compacted_l0_sst` is required so we can get the `writer_epoch` of the `last_compacted_l0_sst` file. This is important to know for the recovery process detailed in the Read Clients section below._

### Create

A client may create a snapshot by reading and updating the current manifest using the process defined in Manifest's Snapshot section above. The client will insert a new snapshot into the `snapshots` field of the manifest.

### Remove

Two processe may remove a snapshot:

* The client that created it
* The compactor

A client that creates a snapshot may remove it at any time by writing a new manifest with the snapshot removed.

The compactor may remove any snapshot from the manifest that has a `heartbeat_s` < now() - `snapshot_timeout_s` (a new compactor config). For example, a compactor with `snapshot_timeout_s` set to `120` is allowed to remove snapshots from the manifest that are older than 120 seconds.

Snapshot removal is just the inverse of snapshot creation: the manifest is read, the snapshot is removed, and the manifest is written again (using the process in Manifest's Updates section above).

### Heartbeats

Clients that want long-running snapshots may renew (or heartbeat) their snapshots by writing a new manifest that contains their existing snapshot with an updated `heartbeat_s`. As long as the client does this more frequently than `snapshot_timeout_s`, the SSTs in the manifest they're using will never be deleted.

_NOTE: Clock skew can affect the timing between the compactor and the snapshot clients. We're assuming we have well behaved clocks, [Network Time Protocol](https://www.ntp.org/documentation/4.2.8-series/ntpd/) (NTP) daemon, or [PTP Hardware Cocks](https://aws.amazon.com/blogs/compute/its-about-time-microsecond-accurate-clocks-on-amazon-ec2-instances/) (PHCs)._

## Read Clients

On startup, a read process needs to find the currently available l0 and compacted SSTs in object storage. This requires the following steps:

1. Read `manifest/current` to find the current manifest.
2. Read the current manifest (e.g. `manifest/000000000000000003.manifest`).
3. Store all SST IDs from `compacted_ssts` in memory.
4. Store all valid `level_0` SSTs in memory.

To determine the valid SSTs in `level_0`, we run the following logic:

```rust
// Get the writer epoch from the most recently compacted level_0 SST file
let mut current_writer_epoch = table_store.get_writer_epoch(manifest.last_compacted_l0_sst) or 0
// Start reading from the oldest uncompacted level_0 SST
let mut next_sst_id = state.last_compacted_l0_sst + 1;

// Read all contiguous SSTs starting from the first SST after last_compacted_l0_sst
while Some(sst_info) = table_store.open_sst(next_sst_id) {
  // We've found a new writer_epoch, so fence all future writes from older writers
  if sst_info.writer_epoch > current_writer_epoch:
    current_writer_epoch = current_writer_epoch
  // We've found a valid write, add the SST to the DbStat's l0
  if sst_info.writer_epoch == current_writer_epoch:
    state.l0.insert(0, sst_info);
  // if sst_info.writer_epoch < current_writer_epoch:
  //   ignore since this is a zombie write (a higher epoch was previously written an SST)

  // Advance to the next contigous SST ID
  next_sst_id += 1;
}

state.next_sst_id = next_sst_id
```

At this point, the client has a list of all compacted SST IDs and all valid uncompacted `level_0` SST IDs. The reader client's `next_sst_id` is set to the next SST we expect to be written.

From here, read processes get to decide how they want to manage their in-memory state using any combination of:

* Creating a snapshot (this can also be done after step (2), above)
* Re-running (1-4) lazily when an object_store SST read results in a 404 (the SST was deleted)
* Re-running (1-4) periodically
* Periodically listing `level_0/[next_sst_id]` to detect new `level_0` SSTs

_NOTE: `table_store.open_sst` is expected to return the first (oldest) version of an SST object if the object store doesn't support mutable (real) CAS._

## Read-Write Clients

### Startup

On startup, a read-write process must:

1. Increment the `writer_epoch` in the manifest
2. Creates a read snapshot in the manifest
3. Fence older writers
4. Load `compacted` and valid `level_0` SSTs into memory

Steps (1) and (2) are done following the Manifest's Update section and the Snapshot's Create section above.

Step (3), fencing, is done by writing an empty SST with the current `writer_epoch` to `next_sst_id`. The algorith looks like this:

```rust
loop {
  // Start from the earlist known non-compacted SST
  let mut next_sst_id = manifest.last_compacted_l0_sst + 1;

  match table_store.write_sst(next_sst_id, empty_sst) {
    SlateDBError::SstAlreadyExists(existing_sst) => {
      if existing_sst.writer_epoch > state.writer_epoch {
        panic!("We're a zombie!")
      } else if existing_sst.writer_epoch == state.writer_epoch {
        panic!("Uh.. another writer has the same writer_epoch. Illegal state.")
      } else {
        // An older (soon to be fenced) writer wrote an SST. Try again at the next spot.
        next_sst_id += 1
      }
    }
    // Just try again for now
    Err(_) => continue
    // We fenced. All future writes from older `write_epochs` will be ignored by readers.
    Ok() => break
  }
}
```

_NOTE: An optimization would be to set the next_sst_id to the most recent (not earliest) SST ID by doing a `list` on the `level_0` directory and finding the maximum SST id._

Step (4) is done the same way that we describe in the Read Clients section.

At this point, we now have a writer client that  has:

* Fenced all future writes from older clients
* Loaded the complete list of valid SSTs into memory
* Snapshotted all SSTs so they won't be removed by a compactor

### Writes

After the startup phase, writers follow the same logic that they use in step (3) of the startup, except they don't write `empty_sst` any more; their SSTs now have key/value pairs.

The same logic described above holds true:

1. If no SST exists, the SST write is successful.
2. If the SST exists but is from an older (fenced) write, just ignore it and try again with the subsequent SST ID.
3. If the SST exists but is from a newer writer, the current client is a zombie. It should return an error.

### Ack'ing

A writer client may acknowldge a sucessful `put()` call as soon as an SST with the put's key-value pair has been successfully written to object storage.

For object stores with CAS, "successfully written" means a `create-if-absent' PUT was successful. For object stores with versioning, "successfully written" means the write is the oldest (first) version of the SST object.

## Compactors

A single compactor process runs periodically. What it does in the `compacted` folder is outside the scope of this document.

In `level_0`, it will periodically read all SSTs and compact them into `compacted`. It will then write a new manifest that contains the new `compacted` SSTs and a new `last_compacted_l0_sst` set the to most recent SST it compacted. It will then delete all SSTs in `level_0` that are < `last_compacted_l0_sst`.

Compactor updates are important to consider because `level_0` compactions must happen frequently to keep startup times down. SSTs can be created in `level_0` write volume frequently exceeds `flush_bytes` ([#30](https://github.com/slatedb/slatedb/issues/30)) or if `flush_ms` is very low. A writer with a `flush_ms` set to 1 would create 1000 SSTs per-second.

## Contention

The following actions require a manifest update:

1. A reader creates a snapshot on startup
2. A reader heartbeats a snapshot periodically
3. A reader removes a snapshot when finished
4. A writer increments writer_epoch and creates a snapshot on startup
6. A compactor increments `compactor_epoch` on startup
7. A compactor finishes a compaction periodically

IF these occur too frequently, there might be conflict or starvation.

TODO: I'm running out of steam tonight. We should consider how frequently the manifest is updated.
