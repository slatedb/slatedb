# Manifest Design

Status: "Under Discussion"

Authors:

* [Vignesh Chandramohan](https://github.com/vigneshc)
* [Rohan Desai](https://github.com/rodesai)
* [Chris Riccomini](https://github.com/criccomini)

PRs:

* [#24 Add manifest design document.](https://github.com/slatedb/slatedb/pull/24)
* [#39 Add an alternative manifest design document](https://github.com/slatedb/slatedb/pull/39)

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
  2. Write the SST to object storageat path `sst-{id}`.
  3. Remove the MemTable from `imm_memtables`.
  4. Insert the SST's metadata (`SsTableInfo`) into `l0`.
  5. Increment `next_sst_id`.
  6. Nofity listeners that are `await`'ing a `put()` in this MemTable.

The `SsTableInfo` structure is returned when the SST is encoded in 3.1. It looks like this:

```rust
pub(crate) struct SsTableInfo {
    pub(crate) first_key: Bytes,
    // todo: we probably dont want to keep this here, and instead store this in block cache
    //       and load it from there
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) filter_offset: usize,
    pub(crate) filter_len: usize,
    pub(crate) block_meta_offset: usize,
}
```

The structure contains the `first_key` in the SST, block locations and key information, and the bloom filter location and size.

_NOTE: The block meta information in the `SsTableInfo` struct stores the location and first key for each block in the SST. SSTs are broken into blocks (usually 4096 bytes); each block has a sorted list of key-value pairs. This data can get somewhat large; we'll probably move it out of the in-memory `SsTableInfo`._

### Reads

Reads simply iterate over each MemTable and SST looking for the value for a key. This is done by:

1. Return `get(k)` from `memtable` if it exists.
2. Iterate over each `imm_memtables` (starting from index 0) and return `get(k)` if it exists.
3. Iterate over each `l0` SST (starting from index 0) and return `get(k)` if it exists.

## Problem

There are a couple of problems with the current design:

1. SlateDB loses the state of the database when the process stops.
2. SlateDB does not fence zombie writers.

If the process SlateDB is running in stops, all data in `DbState` is lost since it's not persisted anywhere. SlateDB could read over every SST in object storage and reconstruct `l0`, but it does not. 

Even if SlateDB were to recover its state by reading all SSTs in object storage, the process would be slow and could be wrong. Inaccurate `l0` state occurs if multiple writers write SSTs in parallel to the same SlateDB object store location. SlateDB assumes it's the only writer, but it currently does not implement fencing to keep other writers from corrupting its state.

## Solution

### Proposal

We propose persisting SlateDB's `DbState` in a manifest file to solve problem (1). Such a design is quite common in [log-structured merge-trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree) (LSMs). In particular, RocksDB [follows this pattern](https://github.com/facebook/rocksdb/wiki/MANIFEST). In SlateDB, we will persist `DbState` in a manifest file, which readers and writers will load on startup. Writer, compactor, and readers will all update the manifest in various situations. All updates will be done using CAS.

To prevent zombie writers, we propose using CAS to ensure each SST is written exactly one time. We introduce the concept of writer epochs to determine when the current writer is a zombie and halt the process. The full fencing protocol is described below.

### Assumptions

This design assumes:

* Only one writer client is allowed at a time
* Multiple reader clients are allowed at the same time
* Only one compactor client is allowed at a time
* Compactor, writer, and readers may all be on different machines
* Read clients may snapshot the database state to prevent compaction from deleting SSTs they're reading
* Writer clients may send parallel writes to the object store
* Object storage supports [compare-and-swap](https://en.wikipedia.org/wiki/Compare-and-swap) (CAS) or [object versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
* A transactional store (DynamoDB, MySQL, and so on) is available if the object store does not support CAS

### A Note on CAS

Changes to the manifest file and SST writes both require CAS in this design. Most object stores provide CAS (a.k.a. preconditions, conditionals, `If-None-Match`, `If-Match`, and so on). But S3 does not, and we want to support S3. Fortunately, we can support CAS-like operations using several different patterns.

There are two scenarios to consider when we want to CAS an object:

1. The object we wish to CAS is immutable; it exists or it does not, but never changes once it exists ("create if not exists").
2. The object we wish to CAS is mutable; it might exist and can change over time.

There are four different patterns to achieve CAS for these scenarios:

1. **Use CAS**: If the object store supports CAS, we can use it. This pattern works for both scenarios described above.
2. **Use a transactional store**: A transactional store such as DynamoDB or MySQL can be used to store the object. The object store is not used in this mode. This appraoch works for both scenarios described above, but does not work well for large objects.
3. **Use object versioning**: Object stores that support object versioning can emulate CAS if the bucket has versioning enabled. Versioning allows multiple versions of an object to be stored. AWS stores versions [in insertion order](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html#API_ListObjectVersions_Example_3). We can leverage this property as well as AWS's [read-after-write](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/) to emulate CAS. Many writers can write to the same object path. Writers can then list all versions of the object. The first version (the earliest) is considered the winner. All other writers have failed the CAS operation. This does require a [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) after the write, but if CAS is needed, this is the best we can do. This pattern works for immutable files (scenario (1)), but not mutable files (scenario (2)).
4. **Use a proxy**: A proxy-based solution combines an object store and a transactional store. Two values are stored: an object and a pointer to object. Readers first read the pointer (e.g. object path) from the pointer, then read the object that the pointer points to. Writers read the current object (using the reader steps), update the object, and write it back to the object store at a new location (e.g. a new UUID). Writers then update the pointer to point to the new object. The pointer update is done conditionally if the pointer still points to the file the writer updated. The pointer may reside in a file on an object store that supports CAS, or a transactional store like DynamoDB or MySQL. This pattern works for both immutable and mutable files. Unlike (1), it also works well for large objects.

_NOTE: These patterns are discussed more [here](https://github.com/slatedb/slatedb/pull/39/files#r1588879655)._

Our proposed solution uses the following forms of CAS:

* A proxy pointer (4) for manifest CAS
* CAS (1) for SST writes when the object store supports CAS
* Object versioning (3) for SST writes when the object store does not support CAS

### File Structure

Let's look at SlateDB's file structure on object storage:

```
some-bucket/
├─ manifest/
│  ├─ current
│  ├─ 01HX5JRMM7XYNQB74N3GJYZ4N9.manifest
│  ├─ 01HX5JS57YZ3NXZ3H366XRS47R.manifest
│  ├─ 01HX5JS90BM7099ED7F34E5WTG.manifest
├─ wal/
│  ├─ 000000000000000000.sst
│  ├─ 000000000000000001.sst
│  ├─ ...
├─ levels/
│  ├─ ...
```

_NOTE: Previous iterations of this design referred to the WAL as `l0` or `level_0`, and the first level in `levels` (formerly `compacted`) as `level_1+`. We've since shifted the terminology to match traditional LSMs. This shift is discussed in more detail [here](https://github.com/slatedb/slatedb/pull/39/files#r1589855599)._

#### `manifest/current`

A file that contains a pointer to the current manifest file.

For example, if `current` contains:

```
01HX5JS57YZ3NXZ3H366XRS47R
```

Then `manifest/01HX5JS57YZ3NXZ3H366XRS47R.manifest` is the current manifest.

_NOTE: `manifest/current` only exists if the object store supports CAS. If the object store does not support CAS, the manifest is stored in a transactional store._

#### `manifest/01HX5JS57YZ3NXZ3H366XRS47R.manifest`

A file containing writer, commpaction, and snapshot information--the state of the database at a point in time.

The manifest's name is a [ULID](https://github.com/ulid/spec). ULIDs are time sortable, use only 26 characters, are URL-safe, and provide 80 bits of entropy.

The manifest is written using CAS pattern (1). For object stores that don't support CAS, the object versioning pattern (3) is used. We could avoid using CAS on the `.manifest` files by including an ID with enough entropy to avoid conflicts (e.g. a UUID).

**TODO: Do we want to just use CAS/object versioninig here? 80 bits of entropy + the timestamp should be OK, but we could still get (really) unlucky. We would need to use CAS/object versioning on this object to guarantee safety. This is discussed [here](https://github.com/slatedb/slatedb/pull/24#discussion_r1582420379).**

**TODO: The current design does not address incremental updates to the manifest. This is something that prior designs had. See [here](https://github.com/slatedb/slatedb/pull/24/files#diff-58d53b55614c8db2dd180ac49237f37991d2b378c75e0245d780356e5d0c8135R67) for such a design. Do we want to consider incremental updates? Given that manifests are updated only periodically, I'm inclined to say no (at least for now).**

##### Structure

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
  uint64 wal_id_last_compacted = 4;

  // The most recent SST in the WAL at the time manifest was updated.
  uint64 wal_id_last_seen = 5;

  // A list of the SST table info that are valid to read in the `levels` folder.
  repeated SstInfo leveled_ssts = 6;

  // A list of read snapshots that are currently open.
  repeated Snapshot snapshots = 7;
}

message SstInfo {
  // Globally unique ID for an SST in the `compacted` folder.
  uint64 id = 1;

  // The first key in the SST file.
  string first_key = 2;

  // Bloom filter offset in the SST file.
  uint32 filter_offset = 3;

  // Bloom filter length in the SST file.
  uint32 filter_len = 4;

  // Block metadata offset in the SST file.
  uint32 block_meta_offset = 5;

  // Block metadata offset in the SST file.
  uint32 block_meta_len = 6;
}

message Snapshot {
  // A random ID that must be unique across all open snapshots.
  uint32 id = 1;

  // The manifest ID that this snapshot is using as its `DbState`.
  uint64 manifest_id = 2;

  // The UTC seconds that snapshot was last accessed. Clients may periodically update this value.
  // If `last_access_s` is older than `snapshot_timeout_s`, the snapshot is considered expired.
  // If `last_access_s` is 0, the snapshot will never expire.
  uint32 last_access_s = 3;
}
```

##### Size

Manifest size is important because manifest updates must be done transactionally. The longer the read-modify-write takes, the more likely there is to be a conflict. Consequently, we want our manifest to be as small as possible.

The size calculation (in bytes) for the manifest is:

```
  2                         // manifest_format_version
+ 8                         // writer_epoch
+ 8                         // compactor_epoch
+ 8                         // wal_id_last_compacted
+ 8                         // wal_id_last_seen
+ 4 + ~56 * leveled_ssts    // array length + leveled_ssts (~32 byte key + 24 bytes = ~56 bytes)
+ 4 + 16 * snapshots        // array length + snapshots (16 bytes each)
```

Conservatively, a manifest with 1000 snapshots and 100,000 compacted SSTs would be:

```
  2                         // manifest_format_version
+ 8                         // writer_epoch
+ 8                         // compactor_epoch
+ 8                         // wal_id_last_compacted
+ 8                         // wal_id_last_seen
+ 4 + ~56 * 100000          // array length + leveled_ssts
+ 4 + 16 * 1000             // array length + snapshots
= 5,616,042
```

This comes out to 5,616,042 bytes, or ~5.6 MiB. Whether this is reasonable or not depends on how frequently manifests are updated. As we'll see below, updates should be infrequent enough that a 5.6 MiB manifest isn't a problem.

If a client gets 100MB/s to and from EC2 to S3, it would take 56ms to read, 56ms to write, plus serialization and TCP overhead. This is a reasonable amount of time for a read-modify-write operation.

#### `wal/000000000000000000.sst`

SlateDB's WAL is a sequentially ordered contiguous list of SSTs. Each SST contains zero or more sorted key-value pairs. The WAL is used to store writes that have not yet been compacted.

Traditionally, LSMs store WAL data in a different format from the SSTs; this is because, each `put()` call results in single key-value write to the WAL. But SlateDB doesn't write to the WAL on each `put()`. Instead, `put()`'s are batched together based on `flush_ms`, `flush_bytes` ([#30](https://github.com/slatedb/slatedb/issues/30)), or `skip_memtable_bytes` ([#31](https://github.com/slatedb/slatedb/issues/31)). Based on these configurations, multiple key-value pairs are stored in the WAL in a single write. Thus, SlateDB uses SSTs for both the WAL and compacted files.

_NOTE: We discussed using a differnet format for the WAL [here](https://github.com/slatedb/slatedb/pull/39/files#r1590087062), but decided against it._

This design does not use [prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html). AWS allows, "3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix." This limits a single writer to 3,500 writes and 5,500 reads per-second. With a `flush_ms` set to 1, a client would write 1,000 writes per-second (not including compactions). Wth caching, the client should be far below the 5,500 reads per-second limit.

This design also does not expose writer epochs in WAL SST filenames. We discussed this in detail [here](https://github.com/slatedb/slatedb/pull/39/files#r1588892292) and [here](https://github.com/slatedb/slatedb/pull/24/files#r1585569744). Ultimately, we chose "un-namespaced" filenames (i.e. WAL filenames with no epoch prefix) because it's simpler to reason about.

##### Attributes

Each SST will have the following [user-defined metadata fields](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata) set:
 
* writer_epoch: This is a u64. Writers will set this when they put a new SST into `wal`.

### Manifest Updates

A full read-modify-write Manifest update contains the following steps:

1. Read `manifest/current` to find the current manifest. Let's say it's `01HX5JS57YZ3NXZ3H366XRS47R` in this example.
2. Read `manifest/01HX5JS57YZ3NXZ3H366XRS47R.manifest` to read the current manifest.
3. Update the manifest in memory.
4. Write the updated manifest to `manifest/01HX5JS90BM7099ED7F34E5WTG.manifest` using a "create-if-not-exists" `PUT`.
5. If (4) was successful, write `01HX5JS90BM7099ED7F34E5WTG` to `manifest/current` using compare-and-swap (CAS) with the `manifest/current` version from (1).

(4) is a CAS operation. A transactional store must be used if CAS is not available.
(5) is an CAS operation. Object versioning may be used if CAS is not available.

Three different processes update the manifest:

* **Readers**: Readers can update `snapshots` to create, remove a state snapshot, or update a snapshot. 
* **Writers**: Writers must update `writer_epoch` once on startup.
* **Compactors**: Compactors must update `compactor_epoch` once on startup. Compactors must also update `wal_id_last_compacted`, `wal_id_last_seen`, and `leveled_ssts` after each compaction pass.

The union of these three processes means the manifest is updated whenever:

1. A reader creates a snapshot on startup
2. A reader updates a snapshot periodically
3. A reader removes a snapshot when finished
4. A writer increments `writer_epoch` and creates a snapshot on startup
6. A compactor increments `compactor_epoch` on startup
7. A compactor finishes a compaction periodically

If these occur too frequently, there might be conflict or starvation between the clients. Luckily, all these events except (7) should occur at the minute granularity or larger. Compaction, (7), warrants some additional thought, which we'll discuss below.

Let's look at each of these in turn.

#### Snapshots

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

TODO: Discuss last_access_s vs. heartbeat_s, vs. ref counting. Link to Rohan's comments.

##### Create

A client may create a snapshot by reading and updating the current manifest using the process defined in Manifest's Snapshot section above. The client will insert a new snapshot into the `snapshots` field of the manifest.

##### Remove

Two processe may remove a snapshot:

* The client that created it
* The compactor

A client that creates a snapshot may remove it at any time by writing a new manifest with the snapshot removed.

The compactor may remove any snapshot from the manifest that has a `heartbeat_s` < now() - `snapshot_timeout_s` (a new compactor config). For example, a compactor with `snapshot_timeout_s` set to `120` is allowed to remove snapshots from the manifest that are older than 120 seconds.

Snapshot removal is just the inverse of snapshot creation: the manifest is read, the snapshot is removed, and the manifest is written again (using the process in Manifest's Updates section above).

##### Heartbeats

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

**TODO: Should L0 in `levels` be range partitioned or should each SST be a full a-z range? Full range will make compaction faster and will mirror what LSMs do. Range partitioned would make the WAL look more like L0, and L0 of `levels` would look more like L1 of a traditional LSM. I'm for leaving L0 of `levels` be full a-z ranges; thus compacting the WAL just means merging all its SSTs into one and putting that in L0 of `levels`. If we go with range partitioning in L0 of `levels`, we'd need to do a full `L0` range and merge every time we compact the WAL; this could be expensive.**






## Parallel Writes

TODO: Link to Rohan's idea for parallel write fencing.

## Incremental Manifest Updates

TODO

## Misc

* https://github.com/delta-incubator/dynamodb-lock-rs
* https://github.com/pulumi/pulumi/pull/2697


TODO: Alternate design where all writes CAS on the manifest. This is discussed [here](https://github.com/slatedb/slatedb/pull/39#issuecomment-2094356240)

TODO: Parallel writer fencing, discussed [here](https://github.com/slatedb/slatedb/pull/24/files#r1585894110)