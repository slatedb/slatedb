# Manifest Design

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Current Design](#current-design)
   * [Writes](#writes)
   * [Reads](#reads)
- [Problem](#problem)
- [Solution](#solution)
   * [Goals](#goals)
   * [Proposal](#proposal)
   * [CAS on S3](#cas-on-s3)
   * [File Structure](#file-structure)
   * [Manifest Updates](#manifest-updates)
   * [Snapshots](#snapshots)
   * [Writers](#writers)
   * [Readers](#readers)
   * [Compactors](#compactors)
- [Rejected Solutions](#rejected-solutions)
   * [Update Manifest on Write](#update-manifest-on-write)
   * [Epoch in Object Names](#epoch-in-object-names)
   * [Object Versioning CAS](#object-versioning-cas)
   * [Use a `manifest/current` Proxy Pointer](#use-a-manifestcurrent-proxy-pointer)
   * [Two-Phase Mutable CAS](#two-phase-mutable-cas)
   * [DeltaStream Protocol](#deltastream-protocol)
   * [`object_store` Locking](#object_store-locking)
- [Addendum](#addendum)

<!-- TOC end -->

Status: Under Discussion

Authors:

* [Vignesh Chandramohan](https://github.com/vigneshc)
* [Rohan Desai](https://github.com/rodesai)
* [Chris Riccomini](https://github.com/criccomini)

References:

* Add manifest design document. ([#24](https://github.com/slatedb/slatedb/pull/24))
* Add an alternative manifest design document ([#39](https://github.com/slatedb/slatedb/pull/39))

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
* `imm_memtables`: An ordered list of MemTables that are in the process of being written to object storage.
* `l0`: An ordered list of level-0 [sorted string tables](https://www.scylladb.com/glossary/sstable/) (SSTs). These SSTs are not range partitioned; they each contain a full range of key-value pairs.
* `next_sst_id`: The SST ID to use when SlateDB decides to freeze `memtable`, move it to `imm_memtables`, and flush it to object storage. This will become the MemTable's ID on object storage.

SlateDB doesn't have compaction implemented at the moment. We don't have level-1+ SSTs in the database state.

### Writes

A `put()` is performed by inserting the key-value pair into `memtable`. Once `flush_ms` (a configuration value) has expired, a flusher thread (in `flush.rs`) locks `DbState` and performs the following operations:

1. Replace `memtable` with a new (empty) MemTable.
2. Insert the old `memtable` into index 0 of `imm_memtables`.
3. For every immutable MemTable in `imm_memtables`.
   1. Encode the immutable MemTable as an SST.
   2. Write the SST to object storage path `sst-{next_sst_id}`.
   3. Remove the MemTable from `imm_memtables`.
   4. Insert the SST's metadata (`SsTableInfo`) into `l0`.
   5. Increment `next_sst_id`.
   6. Notify listeners that are `await`'ing a `put()` in this MemTable.

A `SsTableInfo` structure is returned when the SST is encoded in 3.1. It looks like this:

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

### Goals

This design should:

* Work with a single writer, multiple readers, and a single compactor
* Allow the writer, compactor, and all readers to run on separate machines
* Allow parallel writes from a single writer
* Allow readers to snapshot state for consistent reads across time
* Work with object stores that support CAS
* Work with object stores that support object versioning if a transactional store is available (DynamoDB, MySQL, and so on)
* Provide < 100ms-300ms read/write latencies
* Avoid write contention on the manifest
* Work for S3, Google Cloud Storage (GCS), Azure Blob Storage (ABS), MinIO, Radically Reprogrammable (R2) Storage, and Tigris

### Proposal

We propose persisting SlateDB's `DbState` in a manifest file to solve problem (1). Such a design is quite common in [log-structured merge-trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree) (LSMs). In particular, RocksDB [follows this pattern](https://github.com/facebook/rocksdb/wiki/MANIFEST). In SlateDB, we will persist `DbState` in a manifest file, which all clients will load on startup. Writer, compactor, and readers will all update the manifest in various situations. All updates to the manifest will be done using [compare-and-swap](https://en.wikipedia.org/wiki/Compare-and-swap) (CAS).

To prevent zombie writers, we propose using CAS to ensure each SST is written exactly one time. We introduce the concept of writer epochs to determine when the current writer is a zombie and halt the process. The full fencing protocol is described later in the _Writer Protocol_ section.

### CAS on S3

Changes to the manifest file and SST writes both require CAS in this design. Most object stores provide CAS (a.k.a. preconditions, conditionals, `If-None-Match`, `If-Match`, and so on). But S3 does not, and we want to support S3. Fortunately, we can support CAS-like operations using several different patterns.

There are two scenarios to consider when we want to CAS an object:

1. The object we wish to CAS is immutable; it exists or it does not, but never changes once it exists ("create if not exists").
2. The object we wish to CAS is mutable; it might exist and can change over time.

There are four different patterns to achieve CAS for these scenarios:

1. **Use CAS**: If the object store supports CAS, we can use it. This pattern works for both scenarios described above.
2. **Use a transactional store**: A transactional store such as DynamoDB or MySQL can be used to store the object. The object store is not used in this mode. This approach works for both scenarios described above, but does not work well for large objects.
3. **Use object versioning**: Object stores that support [object versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) can emulate CAS if the bucket has versioning enabled. Versioning allows multiple versions of an object to be stored. AWS stores versions [in insertion order](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html#API_ListObjectVersions_Example_3). We can leverage this property as well as AWS's [read-after-write](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/) consistency guarantees to emulate CAS. Many writers can write to the same object path. Writers can then list all versions of the object. The first version (the earliest) is considered the winner. All other writers have failed the CAS operation. This does require a [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) after the write, but if CAS is needed, this is the best we can do. This pattern works for immutable files--scenario (1)--but not mutable files--scenario (2). This pattern does not work for S3 Express One Zone since it does not support object versioning.
4. **Use a proxy**: A proxy-based solution combines an object store and a transactional store. Two values are stored: an object and a pointer to object. Readers first read the pointer (e.g. object path) from the pointer, then read the object that the pointer points to. Writers read the current object (using the reader steps), update the object, and write it back to the object store at a new location (e.g. a new UUID). Writers then update the pointer to point to the new object. The pointer update is done conditionally if the pointer still points to the file the writer read. The pointer may reside in a file on an object store that supports CAS, or a transactional store like DynamoDB or MySQL. This pattern works for both immutable and mutable files. Unlike (1), it also works well for large objects.
5. **Use a two-phase write**: Like the proxy solution (5), a two-phase write consists of a write to both an object storage and a transactional store. Writers first write a new object to a temporary location. The writer then writes a record to a transactional store. The transactional record signals an intent to copy; it contains a source, destination, and a completion flag. "put-if-not-exists" semantics are used on the destination column. The writer then copies the new object from its temporary location to its final destination in object storage. Upon completion, the transactional store's record is set to complete and the temporary file is deleted. If the writer fails before the transactional record is written, the write is abandoned. If the writer fails after the transactional record is written, a recovery process runs in the future to copy the object and set the completion flag. This pattern works for immutable objects. It also works well for large objects.

_NOTE: These patterns are discussed more [here](https://github.com/slatedb/slatedb/pull/39/files#r1588879655) and [here](https://github.com/slatedb/slatedb/pull/43/files#r1597297640)._

Our proposed solution uses the following forms of CAS:

* CAS (1) for manifest and SST writes when the object store supports CAS
* Two-phase write (3) for manifest and SST writes when the object store does not support CAS

#### Two-Phase CAS Protocol

A description of the two-phase CAS protocol is described in this section. There are operations to consider:

1. **Writes**: A process writes an object to object storage and fails if the object already exists.
3. **Deletes**: A process deletes an object from object storage.
2. **Recovery**: A process recovers a failed write or delete.

##### Writes

Suppose a writer wishes to write to the object location `manifest/00000000000000000000.manifest`. The writer follows these steps:

1. Write the new manifest to a temporary location, say `manifest/00000000000000000000.manifest.[UUID].[checksum]`.
2. Write a record to a transactional store, say DynamoDB, with the following fields:
   * `source`: `manifest/00000000000000000000.manifest.[UUID].[checksum]`
   * `destination`: `manifest/00000000000000000000.manifest`
   * `committed`: `false`
3. Copy the object from `manifest/00000000000000000000.manifest.[UUID].[checksum]` to `manifest/00000000000000000000.manifest`.
4. Update the record in DynamoDB to set `phase` to `true`.
5. Delete the object from `manifest/00000000000000000000.manifest.[UUID].[checksum]`.

_NOTE: The source location is arbitrary. If randomness is used in the name--a UUID, ULID, or KSUID--collisions must be considered. In our case, collisions are catastrophic since they lead to corruption. We use a UUID and checksum to attempt to reduce collisions._

Once the record is written to DynamoDB in step (2), the entire write is considered durable. No other writers may insert a record with the same `destination` field.

_NOTE: This design is inefficient on S3. A single SST write includes two S3 `PUT`s, two writes to DynamoDB (or equivalent), and one S3 `DELETE`. See [here](https://github.com/slatedb/slatedb/pull/43#issuecomment-2105368258) for napkin math on API cost. Latency should be minimally impacted since the write is considered durable after one S3 write and one DynamoDB write. Nevertheless, we are gambling that S3 will soon support pure-CAS like every other object store. In the long run, we expect to switch S3 clients to use CAS and drop object versioning._

##### Deletes

See the garbage collection section below for details on deletions.

##### Recovery

A writer or deletion process might fail at any point in the steps outlined above. Client processes may run recoveries at any point in the future to complete partial writes or deletes. The recovery process is as follows:

1. List all records in DynamoDB with `committed` set to `false`.
2. For each record, check if the `destination` object exists in object storage.
   1. If the object exists, check if the `source` object exists.
      1. If the `source` object exists, copy the `source` object to the `destination` object and set `committed` to `true`.
      2. If the `source` object does not exist, delete the record from DynamoDB.
   2. If the object does not exist, delete the record from DynamoDB.

### File Structure

Let's look at SlateDB's file structure on object storage:

```
some-bucket/
├─ manifest/
│  ├─ 00000000000000000000.manifest
│  ├─ 00000000000000000001.manifest
│  ├─ 00000000000000000002.manifest
├─ wal/
│  ├─ 00000000000000000000.sst
│  ├─ 00000000000000000001.sst
│  ├─ ...
├─ levels/
│  ├─ ...
```

_NOTE: Previous iterations of this design referred to the WAL as `l0` or `level_0`, and the first level in `levels` (formerly `compacted`) as `level_1+`. We've since shifted the terminology to match traditional LSMs. This shift is discussed in more detail [here](https://github.com/slatedb/slatedb/pull/39/files#r1589855599)._

#### `manifest/00000000000000000000.manifest`

A file containing writer, compaction, and snapshot information--the state of the database at a point in time.

The manifest's name is formatted as 20 digit zero-padded numbers to fit u64's maximum integer and to support lexicographical sorting. The name represents the manifest's ID. Manifest IDs are monotonically increasing and contiguous. The manifest with the highest ID is considered the current manifest. `00000000000000000002.manifest` is the current manifest in the file structure section above.

_NOTE: The current design does not address incremental updates to the manifest. This is something that prior designs had. See [here](https://github.com/slatedb/slatedb/pull/24/files#diff-58d53b55614c8db2dd180ac49237f37991d2b378c75e0245d780356e5d0c8135R67). We've opted to eschew incremental manifest updates for the time being, given their size (~5MiB) and update frequency (on the order of minutes)._

##### Structure

A `.manifest` file has the following structure.

_NOTE: We describe the structure in a proto3 IDL, but the manifest could be any format we choose. I'm thinking [FlatBuffers](https://github.com/slatedb/slatedb/issues/41). See [#41](https://github.com/slatedb/slatedb/issues/41) for discussion._

```proto
syntax = "proto3";

message Manifest {
  // Manifest format version to allow schema evolution.
  uint16 manifest_format_version = 1;

  // The current writer's epoch.
  uint64 writer_epoch = 2;

  // The current compactor's epoch.
  uint64 compactor_epoch = 3;

  // The most recent SST in the WAL that's been compacted.
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
  // 128-bit UUID that must be unique across all open snapshots.
  uint128 id = 1;

  // The manifest ID that this snapshot is using as its `DbState`.
  uint64 manifest_id = 2;

  // The UTC unix timestamp seconds that a snapshot expires at. Clients may update this value.
  // If `snapshot_expire_time_s` is older than now(), the snapshot is considered expired.
  // If `snapshot_expire_time_s` is 0, the snapshot will never expire.
  uint32 snapshot_expire_time_s = 3;
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
+ 4 + 28 * snapshots        // array length + snapshots (28 bytes each)
```

Conservatively, a manifest with 1000 snapshots and 100,000 compacted SSTs would be:

```
  2                         // manifest_format_version
+ 8                         // writer_epoch
+ 8                         // compactor_epoch
+ 8                         // wal_id_last_compacted
+ 8                         // wal_id_last_seen
+ 4 + ~56 * 100000          // array length + leveled_ssts
+ 4 + 28 * 1000             // array length + snapshots
= 5,628,042
```

This comes out to 5,628,042 bytes, or ~5.6 MiB. If a client gets 100MB/s to and from EC2 to S3, it would take 56ms to read and 56ms to write (plus serialization, network, and TCP overhead). All in, let's say 250-500ms.

Whether this is reasonable or not depends on how frequently manifests are updated. As we'll see below, updates should be infrequent enough that a 5.6 MiB manifest isn't a problem.

#### `wal/00000000000000000000.sst`

SlateDB's WAL is a sequentially ordered contiguous list of SSTs. SST object names are formatted as 20 digit zero-padded numbers to fit u64's maximum integer and to support lexicographical sorting. Each SST contains zero or more sorted key-value pairs. The WAL is used to store writes that have not yet been compacted.

Traditionally, LSMs store WAL data in a different format from the SSTs; this is because each `put()` call results in a single key-value write to the WAL. But SlateDB doesn't write to the WAL on each `put()`. Instead, `put()`'s are batched together based on `flush_ms`, `flush_bytes` ([#30](https://github.com/slatedb/slatedb/issues/30)), or `skip_memtable_bytes` ([#31](https://github.com/slatedb/slatedb/issues/31)). Based on these configurations, multiple key-value pairs are stored in the WAL in a single write. Thus, SlateDB uses SSTs for both the WAL and compacted files.

_NOTE: We discussed using a different format for the WAL [here](https://github.com/slatedb/slatedb/pull/39/files#r1590087062), but decided against it._

This design does not use [prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html). AWS allows, "3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix." This limits a single writer to 3,500 writes and 5,500 reads per-second. With a `flush_ms` set to 1, a client would write 1,000 writes per-second (not including compactions). Wth caching, the client should be far below the 5,500 reads per-second limit.

This design also does not expose writer epochs in WAL SST filenames (e.g. `[writer_epoch].[sequence_number].sst`). We discussed this in detail [here](https://github.com/slatedb/slatedb/pull/39/files#r1588892292) and [here](https://github.com/slatedb/slatedb/pull/24/files#r1585569744). Ultimately, we chose "un-namespaced" filenames (i.e. filenames without epoch prefix) because it's simpler to reason about.

##### Attributes

Each SST will have the following [user-defined metadata fields](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata):
 
* `writer_epoch`: This is a `u64`. Writers will set this on each SST written into `wal`.

### Manifest Updates

A full read-modify-write Manifest update contains the following steps:

1. List `manifest` to find the manifest with the largest ID. Say it's `00000000000000000002.manifest` in this example.
2. Read `manifest/00000000000000000002.manifest` to read the current manifest.
3. Update the manifest in memory.
4. Write the updated manifest to the next manifest slot (`manifest/00000000000000000003.manifest`).

(4) is a CAS operation. Clients will use either real CAS (pattern (1)) or two-phase write (pattern (5)) to update the manifest.

If the CAS write fails in (4), the client must retry the entire process. This is because the client now has. The client must find the (new) latest manifest and re-apply its changes.

Three different processes update the manifest:

* **Readers**: Readers can update `snapshots` to create, remove, or update a snapshot. Readers can also update `wal_id_last_seen` to update the latest `wal` SST ID when a new snapshot is created.
* **Writers**: Writers must update `writer_epoch` once on startup.
* **Compactors**: Compactors must update `compactor_epoch` once on startup. Compactors must also update `wal_id_last_compacted`, `wal_id_last_seen`, and `leveled_ssts` after each compaction pass.

The union of these three processes means the manifest is updated whenever:

1. A reader creates a snapshot on startup
2. A reader updates a snapshot periodically
3. A reader removes a snapshot when finished
4. A writer increments `writer_epoch` and creates a snapshot on startup
6. A compactor increments `compactor_epoch` on startup
7. A compactor finishes a compaction periodically

If these occur too frequently, there might be conflict between the clients. Conflict can lead to starvation, which can slow down startup time and compaction time. Luckily, all events except (7) should occur at the minute granularity or larger. Compaction, (7), warrants some additional thought, which we discuss at the end of this document.

Let's look at each of these in turn.

### Snapshots

This design introduces the concept of snapshots, which allow clients to:

1. Prevent compaction from deleting SSTs that the client is still using
2. Share a consistent view of the database state across multiple clients

A snapshot has three fields: `id`, `manifest_id`, and `snapshot_expire_time_s`.

* `id`: A UUID that must be unique across all open snapshots.
* `manifest_id`: The manifest ID that this snapshot is using as its `DbState`.
* `snapshot_expire_time_s`: The UTC Unix epoch seconds that the snapshot expires at.

Each client will use a different `id` for each snapshot it creates. Similarly, no two clients will share the same `id` for any snapshot. Clients that wish to share the same view of a database will each define different snapshots with the same `manifest_id`.

Clients set the `snapshot_expire_time_s` when the snapshot is created. Clients may update their snapshot's `snapshot_expire_time_s` at their discretion by writing a new manifest with the updated value. A snapshot is considered expired if `snapshot_expire_time_s` is less than the current time. If `snapshot_expire_time_s` is 0, the snapshot will never expire.

_NOTE: Clients that set `snapshot_expire_time_s` to 0 must guarantee that they will eventually remove their snapshot from the manifest. If they do not, the snapshot's SSTs will never be removed._

A client creates a snapshot by creating a new manifest with a new snapshot added to the `snapshots` field.

A client may also update `wal_id_last_seen` in the new manifest to include the most recent SST in the WAL that the client has seen. This allows clients to include the most recent SSTs from the `wal` in a new snapshot. See [here](https://github.com/slatedb/slatedb/pull/39/files#r1588780707) for more details.

A new snapshot may only reference an active manifest (see [here](https://github.com/slatedb/slatedb/pull/43/files#r1594444035) for more details). A manifest is considered active if either:

* It's the current manifest
* It's referenced by a snapshot in the current manifest and the snapshot has not expired

A compactor may not delete SSTs that are referenced by any active manifest.

The set of SSTs that are referenced by a manifest are:

* All `levels` files referenced in the manifest's `leveled_ssts`
* All `wal` files with SST ID >= `wal_id_last_compacted`

_NOTE: The inclusive `>=` for `wal_id_last_compacted` is required so the compactor doesn't delete the most recently compacted file. We need this file to get the `writer_epoch` during the recovery process detailed in the Read Clients section below._

_NOTE: Clock skew can affect the timing between the compactor and the snapshot clients. We're assuming we have well behaved clocks, a [Network Time Protocol](https://www.ntp.org/documentation/4.2.8-series/ntpd/) (NTP) daemon, or [PTP Hardware Cocks](https://aws.amazon.com/blogs/compute/its-about-time-microsecond-accurate-clocks-on-amazon-ec2-instances/) (PHCs)._

_NOTE: A [previous design proposal](https://github.com/slatedb/slatedb/pull/39/files#diff-d589c7beb3d163638e94dbc8e086b3efe093852f0cad96f04cb1283c3bd1eb74R105) used a `heartbeat_s` field that clients would update periodically. After some discussion (see [here](https://github.com/slatedb/slatedb/pull/39/files#r1588896947)), we landed on a design that supports both reference counts and snapshot timeouts. Reference counts are useful for long-lived snapshots that exist indefinitely. Heartbeats are useful for short-lived snapshots that exist for the lifespan of a single process._

_NOTE: This design considers read-only snapshots. Read-write snapshots are discussed [here](https://github.com/slatedb/slatedb/pull/43/files#r1596319141) and in [[#49](https://github.com/slatedb/slatedb/issues/49)]._

### Writers

#### `writer_epoch`

This design introduces the concept of a `writer_epoch`. The `writer_epoch` is a monotonically increasing `u64` that is transactionally incremented by a writer on startup. The `writer_epoch` is used to prevent split-brain and fence zombie writers. A zombie writer is a writer with an epoch that is less than the `writer_epoch` in the current manifest.

`writer_epoch` exists in two places:

1. As a field in the manifest
2. As a user-defined metadata field in each `wal` SST

#### Writer Protocol

On startup, a writer client must increment `writer_epoch`.

1. List `manifest` to find the manifest with the largest ID.
2. Read the current manifest (e.g. `manifest/00000000000000000002.manifest`).
3. Increment the `writer_epoch` in the current manifest in memory.
4. Write the manifest with the updated `writer_epoch` (e.g. `manifest/00000000000000000003.manifest`).

_NOTE: This is the same process described in the _Manifest Updates_ section above._

The writer client must then fence all older clients. This is done by writing an empty SST to the next SST ID in the WAL.

1. Create a new snapshot in the manifest.
2. List the `wal` directory to find the next SST ID.
3. Write an empty SST with the new `writer_epoch` to the next SST ID using CAS or object versioning.

_NOTE: A snapshot is created in (1) to prevent the compactor from deleting `wal` SSTs while the writer has written its fencing SST. See [here](https://github.com/slatedb/slatedb/pull/43/files#r1594460226) for details._

_NOTE: The writer may choose to release its snapshot created in (1) after writing the fencing SST in (3), or it may periodically refresh its snapshot._

At this point, there are four potential outcomes:

1. The write is successful.
2. The write was unsuccessful. Another writer wrote an SST with the same ID and a lower (older) `writer_epoch`.
3. The write was unsuccessful. Another writer wrote an SST with the same ID and a the same `writer_epoch`.
4. The write was unsuccessful. Another writer wrote an SST with the same ID and a higher (newer) `writer_epoch`.

If the write was successful (1), all previous (older) writers have now been fenced.

If an older writer beats the current writer to the SST slot (2), the newer writer increments its SST ID by 1 and tries again. This process repeats until the write is successful.

If another writer has the same `writer_epoch` (3), the client is in an illegal state. This should never happen since clients are transactionally updating `writer_epoch`. If this does happen, the client should panic.

If a newer writer beats the current writer to the SST slot (4), the current writer has been fenced. The current writer should halt.

_NOTE: This protocol implies the invariant that SST IDs in `wal` will be contiguous and monotonically increasing._

#### Examples

Let's consider some examples to illustrate the writer protocol.

Here's an example where a new writer successfully fences an older writer (protocol scenario (1)):

```
time 0, 00000000000000000000.sst, writer_epoch=1
time 1, 00000000000000000001.sst, writer_epoch=1
time 2, 00000000000000000002.sst, writer_epoch=2
time 3, 00000000000000000002.sst, writer_epoch=1
time 4, 00000000000000000003.sst, writer_epoch=2
```

In the example above, writer 1 successfully writes SSTs 0 and 1. At time 2, writer 2 successfully fences writer 1, but writer 1 hasn't yet seen the fence write. When writer 1 attempts to write SST 2, it loses the CAS write and halts because SST 2 has a higher `writer_epoch`. Writer 2 then continues with a successful write to SST 3.

Here's an example where a new writer has to retry its fence write because an older writer took its SST ID location (protocol scenario (2)):

```
time 0, 00000000000000000000.sst, writer_epoch=1
time 1, 00000000000000000001.sst, writer_epoch=1
time 2, 00000000000000000002.sst, writer_epoch=1
time 3, 00000000000000000002.sst, writer_epoch=2
time 4, 00000000000000000003.sst, writer_epoch=2
```

In the example above, writer 1 successfully writes SSTs 0, 1, and 2. Writer 2 tries to fence older writers with a write to SST ID 2, but fails because writer 1 has already written an SST at that location. Writer 2 sees that the SST with ID 2 has a lower `writer_epoch` than its own and retries its fence write at the next SST ID location. This write to SST ID 3 at time 4 is successful. Writer 2 has successfully fenced writer 1. If writer 1 tries to write to SST ID 3, it will see a higher `writer_epoch` and halt.

Here's an example where a new writer gets fenced before it can write its own fencing write (protocol scenario (4)):

```
time 0, 00000000000000000000.sst, writer_epoch=1
time 1, 00000000000000000001.sst, writer_epoch=1
time 2, 00000000000000000002.sst, writer_epoch=3
time 3, 00000000000000000002.sst, writer_epoch=2
time 4, 00000000000000000003.sst, writer_epoch=3
```

In the example above, writer 1 successfully writes SSTs 0 and 1. Writer 2 tries to fence older writers with a write to SST ID 2, but fails because writer 3 has already written an SST at that location. Writer 2 sees that the SST with ID 2 has a higher `writer_epoch` than its own, and halts.

#### Acknowledgements

SlateDB's `put()` API is asynchronous. Clients that want to know when their `put()` has been durably persisted to object storage must call `await`. A writer client will not successfully acknowledge a `put()` call until the key-value pair has been successfully written to object storage in a `wal` SST.

#### Parallel Writes

The writer protocol we describe above assumes that writers are writing SSTs in sequence. That is, a writer will never write SST ID N until SST ID N-1 has been successfully written to object storage by the client.

But we know we will want to support parallel writes in the future. Parallel writes allow a single writer client to write multiple SSTs at the same time. This can reduce latency for the writer client. Our design should not preclude parallel writes.

The current writer protocol can be extended to support parallel writes by defining a `max_parallel_writes` configuration parameter. A new writer must then write `max_parallel_writes` sequential fencing SSTs in a row starting from the first open SST position in the `wal`. `max_parallel_writes` would also need to be stored in the manifest so readers, writers, and compactors can agree on it.

_NOTE: This strategy is discussed in more detail [here](https://github.com/slatedb/slatedb/pull/24/files#r1585894110)._

Consider this example with `max_parallel_writes` set to 2:

```
time 0, 00000000000000000000.sst, writer_epoch=1
time 1, 00000000000000000001.sst, writer_epoch=1
time 2, 00000000000000000002.sst, writer_epoch=2
time 3, 00000000000000000003.sst, writer_epoch=1
time 4, 00000000000000000002.sst, writer_epoch=1
time 5, 00000000000000000003.sst, writer_epoch=2
time 6, 00000000000000000004.sst, writer_epoch=2
time 7, 00000000000000000005.sst, writer_epoch=2
```

Writer 1 successfully writes SSTs 0, 1, and 3. Writer 1's SST 2 write fails at time 4 because writer 2 has already taken that location at time 2. At this point, writer 1 immediately stops writing new SSTs and halts. Existing parallel writes run to completion (either failure or success).

Writer 2 does not know whether it's successfully fenced writer 1 yet. It attempts to write SST 3 at time 5, but fails because writer 1 has already written an SST at that location at time 3. At this point, writer 2 begins again at position 4. Writer 2 successfully writes SST 4, then SST 5. Writer 2 has now written two consecutive SSTs to the `wal`. Since `max_parallel_writes` is set to 2, writer 2 has successfully fenced writer 1; it's guaranteed that writer 1 will see at least one of these fencing writes in its `max_parallel_writes` window. Thus, no writes from writer 1 (or any older writer) will appear after SST 5.

The next question is what to do about writer 1's SST 3 that was written at time 3.

The writer must decide whether to:

1. Block all ack's until all previous SST writes have been successfully written. In this case, `put()`s with key-value pairs in SST 3 would be notified of a failure since SST 2 was not successfully written.
2. Successfully acknowledge SST 3 as soon as it's written.

We propose allowing writers to successfully acknowledge writes as soon as they occur (2). This strategy has slightly looser semantics since it means a put in SST 3 would be durably persisted and readable in the future, while puts in SST 2 would not be durably persisted.

```
a = put(key1, value1)
b = put(key2, value3)
a.await() -> failed
b.await() -> succeeded
```

This should be fine since the client can always retry the failed `put()` call. Clients that want strong ordering across puts can use transactions.

_NOTE: Transactions are not something we've considered in detail. The current idea is to keep all transactional writes within a single SST. Thus, transactions should work with this protocol._

All readers and the compactor must decide whether to:

1. Ignore SST 3 because an SST with a higher `writer_epoch` has been written to a previous slot.
2. Treat SST 3 as valid because writer 2 has not yet written `max_parallel_writes` SSTs in a row.

We propose allowing readers and the compactor to treat SST 3 as valid.

_NOTE: Astute readers might notice that `max_parallel_writes` is equivalent to a Kafka producer's [`max.in.flight.requests.per.connection`](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#max-in-flight-requests-per-connection) setting._

If more than 2 writers are writing in parallel, the protocol is the same. The only difference is that readers will see writes from multiple epochs before the youngest writer wins the fencing race.

To illustrate, consider an example where we have 3 writers. writer 1 is the old writer, while writer 2 and writer 3 are trying to fence:

```
time  0, 00000000000000000000.sst, writer_epoch=1 // success
time  1, 00000000000000000001.sst, writer_epoch=1 // success
time  2, 00000000000000000002.sst, writer_epoch=3 // success
time  3, 00000000000000000003.sst, writer_epoch=1 // success
time  4, 00000000000000000002.sst, writer_epoch=1 // failure (writer 3 won at time 2), writer 1 halts
time  5, 00000000000000000003.sst, writer_epoch=2 // failure (writer 1 won at time 3)
time  6, 00000000000000000003.sst, writer_epoch=3 // failure (writer 1 won at time 3)
time  7, 00000000000000000004.sst, writer_epoch=2 // success
time  8, 00000000000000000004.sst, writer_epoch=3 // failure (writer 2 won at time 7)
time  9, 00000000000000000005.sst, writer_epoch=3 // success
time 10, 00000000000000000005.sst, writer_epoch=2 // failure (writer 3 won at time 9), writer 2 halts
time 11, 00000000000000000006.sst, writer_epoch=3 // success
```

At time 11, writer 3 has successfully fenced writer 1 and writer 2. Readers will consider all SSTs valid from 0-6, though only SSTs 0, 1, and 3 will have key-value pairs since fencing SST writes are always empty.

_NOTE: Writers will only detect that they've been fenced when they go to write. Low-throughput writers can periodically scan `wal` for SSTs with a higher `writer_epoch` to proactively detect that they've been fenced. This would work with the current design, but isn't in scope._

### Readers

#### Reader Protocol

Readers must establish a snapshot on startup and load DbState:

1. List all `wal` to find the maximum contiguous SST
2. Update the manifest to contain a new snapshot and set `wal_id_last_seen` to the ID found in (1)
3. Load all `leveled_sst`s into `DbState.leveled_ssts`
4. Load all `wal` SSTs >= `wal_id_last_compacted` into `DbState.l0`

If (2) fails due to a CAS conflict, restart from (1).

_NOTE: We'll probably want to rename `DbState.l0` to `DbState.wal`._

_NOTE: `DbState.leveled_ssts` doesn't currently exist because SlateDB does not have compaction. When we add compaction, we'll need to introduce this variable._

At this point, a reader has a consistent view of all SSTs in the database.

The reader will then periodically refresh its state by:

1. Polling the `wal` directory to detect new `wal` SSTs
2. Reloading the current manifest and creating a new snapshot to detect new `leveled_ssts`

_NOTE: Readers can also skip snapshot creation if they wish. If they do so, they must deal with SSTs that are no longer in object storage. They may do so by re-reading the manifest and updating their `DbState`, or by simply skipping the SST read. Lazy readers are outside the scope of this document._

#### Parallel Writes

The reader protocol above assumes that all SSTs are contiguous and monotonically increasing. If parallel writes are allowed, a reader might see a `wal` with gaps between SSTs:

```
time 0, 00000000000000000000.sst, writer_epoch=1
time 1, 00000000000000000001.sst, writer_epoch=1
time 2, 00000000000000000003.sst, writer_epoch=1
```

In this example, the reader has two choices:

1. Include SST 3 in its DbState.
2. Wait for SST 2 to be written.

If the reader includes SST 3 in its `DbState`, it will need to periodically check for SST 2 by polling the `wal` or refreshing its `DbState` at some interval.

We propose waiting for SST 2 to be written. This style simplifies polling: the reader simply polls for the next SST (SST 2 in the example above). When SST 2 appears, it's added to `l0` and the reader begins polling for SST 3.

This does mean that SST 3 will not be served by a reader until SST 2 arrives. For the writer client, this is not a problem, since the writer client is fully consistent between its own reads and writes. But secondary reader clients might not see SST 3 for a significant period of time if the writer dies. This is a tradeoff that we are comfortable with. If it proves problematic, we can implement (1), above, and poll for missing SSTs periodically.

_NOTE: There is some interdependence between when `wal_id_last_seen` and the semantics of a snapshot. If `wal_id_last_seen` were to include SST 3 in our example above, we'd have to decide whether to include SST 2 when it eventually arrives. The current design does not have this issue. This is discussed more [here](https://github.com/slatedb/slatedb/pull/43/files#r1594783266)._

### Compactors

On startup, a compactor must increment the `compactor_epoch` in the manifest. This is done in a similar manner to the process described in the _Writer Protocol_ section above.

After startup, compactors periodically do two things:

1. Merge SSTs from the `wal` into `leveled_ssts`
2. Merge SSTs from `leveled_ssts` into higher-level SSTs

To merge SSTs from the `wal` into `leveled_ssts` (1), the compactor must periodically:

1. List all SSTs in the `wal` folder > `wal_id_last_compacted`
2. Create a new merged SST in `leveled_ssts`'s level 0 that contains all `wal` SST key-value pairs
3. Update the manifest with the new `leveled_ssts`, `wal_id_last_compacted`, and `wal_id_last_seen`

_NOTE: This design implies that level 0 for `leveled_ssts` will not be range partitioned. Each SST in level 0 would be a full a-z range. This is how traditional LSMs work. If we were to go with range partitioning in level 0, we'd need to do a full level 0 merge every time we compact `wal`; this could be expensive. This design decision is discussed in more detail [here](https://github.com/slatedb/slatedb/pull/39/files#r1589855599)._

Merging leveled SSTs (2) is outside the scope of this design.

### Garbage Collectors

A garbage collector (GC) must delete both objects from object storage and records from the transactional store (when two-phase CAS is used). The garbage collector must delete inactive manifests and SSTs.

_NOTE: This design considers the compactor and garbage collector (inactive SST deletion) as two separate activities that run for one database in one process--the compactor process. In the future, we might want to run the compactor and garbage collector in separate processes on separate machines. We might also want the garbage collector to run across multiple databases. This should be doable, but is outside this design's scope. The topic is discussed [here](https://github.com/slatedb/slatedb/pull/43/files#r1596319141) and in [[#49](https://github.com/slatedb/slatedb/issues/49)]._

#### Object Deletion

Four classes of objects must be deleted:

* Inactive manifests
* Inactive SSTs
* Orphaned temporary manifests (when two-phase CAS is used)
* Orphaned temporary SSTs (when two-phase CAS is used)

These objects reside in three locations:

* `manifest`
* `wal`
* `levels`

The garbage collector will periodically list all objects in the `manifest`, `wal`, and `levels` directories and delete the following:

1. Any `.manifest` that is not the latest is not referenced by any snapshot in the latest manifest.
2. Any other file in `manifest` that has an object storage creation timestamp older than a day.
3. Any `.sst` in `wal` that is < `wal_id_last_compacted` in the current manifest.
4. Any other file in `wal` that has an object storage creation timestamp older than a day.
5. Any `.sst` in `levels` that is not referenced by the latest manifest or any snapshot in the latest manifest.

_NOTE: These rules follow the definitions for active manifests and active SSTs in the _Snapshots_ section._

#### Transactional Store Deletion

Two classes of records must be deleted from the transactional store:

* Inactive manifests
* Inactive SSTs

The garbage collector will periodically delete all transactional store records with the following criteria:

1. `source` references a `.manifest` that is not the latest and is not referenced by any snapshot in the latest manifest.
2. `source` references an `.sst` in `wal` that is < `wal_id_last_compacted` in the current manifest.

_NOTE: We use `<`  not `<=` for (2) because the compactor must not delete the SST at `wal_id_last_compacted` so readers can recover the `writer_epoch` from the SST._

## Rejected Solutions

### Update Manifest on Write

A previous [manifest design](https://github.com/slatedb/slatedb/pull/24/) proposed transactional manifest updates on every `wal` SST write (discussed [here](https://github.com/slatedb/slatedb/pull/39#issuecomment-2094356240)). This was rejected due to the high cost of transactional writes when using object storage with CAS for the manifest.

### Epoch in Object Names

A previous [manifest design](https://github.com/slatedb/slatedb/pull/24/) proposed namespacing `wal` filenames with a writer epoch prefix (discussed [here](https://github.com/slatedb/slatedb/pull/24/files#diff-58d53b55614c8db2dd180ac49237f37991d2b378c75e0245d780356e5d0c8135R114)). This was rejected because it was deemed complex. We might revisit this decision in the future if we have a need for it.

Another alternative to two-phase CAS was proposed [here](https://github.com/slatedb/slatedb/pull/43/files#r1597308492). This alternative reduced the operation to a single object storage PUT and single DynamoDB write. In doing so, [it required `writer_epoch` in object names](https://github.com/slatedb/slatedb/pull/43/files#r1597317062). This scheme prevented *real* CAS from working. We rejected this design because we decided to favor a design that worked well with real CAS.

### Object Versioning CAS

A [previous version](https://github.com/slatedb/slatedb/blob/6beba6949487a519b8bb6c69a3cf80f06778f540/docs/0001-manifest.md) of this design used object versioning for `wal` SST CAS. This design was rejected because S3 Express One Zone does not support object versioning.

### Use a `manifest/current` Proxy Pointer

A [previous version](https://github.com/slatedb/slatedb/blob/6beba6949487a519b8bb6c69a3cf80f06778f540/docs/0001-manifest.md) of this design contained an additional file: `manifest/current`. Once we decided to use incremental IDs for the manifest, we no longer needed this file.

### Two-Phase Mutable CAS

We [briefly discussed](https://github.com/slatedb/slatedb/pull/43/files#discussion_r1597489292) supporting mutable CAS operations with the two-phase write CAS pattern. There was [some disagreement](https://github.com/slatedb/slatedb/pull/43/files#r1597749655) about whether this was possible. It's conceivable that user-defined attributes might allow the two-phase CAS pattern to handle mutable CAS operations. We did not explore this since we decided to use incremental IDs for the manifest, which require only immutable CAS.

### DeltaStream Protocol

We [considered using a protocol](https://github.com/slatedb/slatedb/pull/43/files#discussion_r1597543706) similar to DeltaStream's. This protocol is similar to two-phase CAS, but uses a DynamoDB pointer to determine the current manifest rather than a LIST operation. This protocol wasn't obviously compatible with SST writes, though. We opted to have a single CAS approach for both the manifest and SSTs, so we rejected this design.

### `object_store` Locking

Rust's `object_store` crate has a [locking mechanism](https://docs.rs/object_store/latest/object_store/aws/enum.S3CopyIfNotExists.html). We briefly looked at this, but [rejected it](https://github.com/slatedb/slatedb/pull/43/files#discussion_r1597551287) because it uses time to live (TTL) timeouts for locks. We wanted a CAS operation that would not time out.

### Atomic Deletes

A [previous version](https://github.com/slatedb/slatedb/pull/43#discussion_r1600912132) of this design proposed using atomic deletes. This turned out to be unnecessary.

## Addendum

We found that Deltalake [has a locking library](https://github.com/delta-incubator/dynamodb-lock-rs). We thought it might be used for its LSM manifest updates, but it's not. We discuss our findings more [here](https://github.com/slatedb/slatedb/pull/24/files#r1582427235).

While exploring CAS with object store versioning, we found that Terraform had explored this idea in their [S3 backend](https://github.com/hashicorp/terraform/issues/27070). Ultimately, they're trying to create file locks, which is different from what we're doing with our manifest.