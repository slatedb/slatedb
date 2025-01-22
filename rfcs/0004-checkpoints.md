# SlateDB Checkpoints and Clones

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Current Implementation](#current-implementation)
- [Problem](#problem)
- [Goals](#goals)
   * [Out Of Scope](#out-of-scope)
- [Proposal](#proposal)
   * [Manifest Schema](#manifest-schema)
   * [Public API](#public-api)
   * [Creating a Checkpoint](#creating-a-checkpoint)
   * [Writers](#writers)
      + [Establishing Initial Checkpoint](#establishing-initial-checkpoint)
      + [Refreshing the Checkpoint](#refreshing-the-checkpoint)
      + [Clones](#clones)
         - [SST Path Resolution](#sst-path-resolution)
         - [Deleting a Database](#deleting-a-database)
   * [Readers](#readers)
      + [Establishing the Checkpoint](#establishing-the-checkpoint)
         - [Checkpoint Maintenance](#checkpoint-maintenance)
      + [Garbage Collector](#garbage-collector)

<!-- TOC end -->

Status: Accepted

Authors:

* [Rohan Desai](https://github.com/rodesai)

## Current Implementation

SlateDB runs a Writer, a Compactor - optionally in a separate process, and a Garbage Collector (GC) - also optionally in
a separate process. The writer writes data to L0 SSTs. The Compactor then goes and compacts these L0 SSTs into Sorted
Runs (SR) under the “compacted” directory, and then further compacts those SRs as they accumulate. Updates to the
current set of L0s and Sorted Runs are synchronized between the Writer and Compactor through updates to the centralized
manifest. When reading, the Writer gets a reference to a copy of the metadata for the current set of sorted runs and
searches them for the requested data. Meanwhile, the GC scans the SSTs in Object Storage and deletes any SSTs that are
not referenced by the current manifest and are older than some threshold.

## Problem

The current implementation poses a few problems and limitations:
1. There is nothing preventing the GC from deleting SSTs out from the Writer while it’s attempting to read their data.
   This isn’t a big deal today as we only support key lookups, which are fast. But this could become a more acute
   problem when we support range scans which can be long.
1. Currently only the Writer process can interact with the database. We’d like to support additional read-only clients,
   called Readers that can read data.
1. Some use cases can benefit from forking the database and establishing a new db that relies on the original DB’s SSTs
   but writes new data to a different set of SSTs from the original writer.

## Goals

In this RFC, we propose adding checkpoints and clones to address this limitation.
- Clients will be able to establish cheap db checkpoints that reference the current version of SlateDB’s manifest.
- Writers will use checkpoints to “pin” a version of the database for reads to avoid the GC from deleting the SSTs.
- Readers will use checkpoints similarly, and many Reader processes can coexist alongside a Writer.
- Finally, we will support creating so-called “clones” from a checkpoint to allow spawning new Writers that write to
  a “fork” of the original database.

### Out Of Scope

This RFC does not propose:
- Any mechanism to provide isolation guarantees from other concurrent reads/writes to the database.

## Proposal

At a high level, this proposal covers a mechanism for establishing database checkpoints, and reviews how the various
processes will interact with this mechanism. Logically, a checkpoint refers to a consistent and durable view of the
database at some point in time. It is consistent in the sense that data read from a checkpoint reflects all writes up
to some point in time, and durable in the sense that this property holds across process restarts. Physically, a
checkpoint refers to a version of SlateDB’s manifest.

### Manifest Schema

Checkpoints themselves are also stored in the manifest. We’ll store them using the following schema:

```

table DbParent {
   // Optional path to a parent database from which this database was cloned
   parent_path: string (required);

   // Optional parent checkpoint ID
   parent_checkpoint: UUID (required);
}

table ManifestV1 {
   // Optional details about the parent checkpoint for the database
   parent: DbParent

   // Flag to indicate whether initialization has finished. When creating the initial manifest for
   // a root db (one that is not a clone), this flag will be set to true. When creating the initial
   // manifest for a clone db, this flag will be set to false and then updated to true once clone
   // initialization has completed.
   initialized: boolean

   // Optional epoch time in seconds that this database was destroyed
   destroyed_at_s: u64;

   ...

   // A list of current checkpoints that may be active (note: this replaces the existing
   // snapshots field)
   checkpoint: [Checkpoints] (required);
}


table WriterCheckpoint {
   epoch: u64;
}

union CheckpointMetadata { WriterCheckpoint }

table Uuid {
   high: uint64;
   low: uint64;
}


// Checkpoint reference to be included in manifest to record active checkpoints.
table Checkpoint {
   // Id that must be unique across all open checkpoints.
   id: Uuid (required);

   // The manifest ID that this checkpoint is using as its `CoreDbState`.
   manifest_id: ulong;

   // The UTC unix timestamp seconds that a checkpoint expires at. Clients may update this value.
   // If `checkpoint_expire_time_s` is older than now(), the checkpoint is considered expired.
   // If `checkpoint_expire_time_s` is 0, the checkpoint will never expire.
   checkpoint_expire_time_s: uint;

   // The unix timestamp seconds that a checkpoint was created.
   checkpoint_create_time_s: uint;

   // Optional metadata associated with the checkpoint. For example, the writer can use this to
   // clean up checkpoints from older writers.
   metadata: CheckpointMetadata;
}
```

### Public API

We’ll make the following changes to the public API to support creating and using checkpoints:

```rust
/// Defines the scope targeted by a given checkpoint. If set to All, then the checkpoint will include
/// all writes that were issued at the time that create_checkpoint is called. If force_flush is true,
/// then SlateDB will force the current wal, or memtable if wal_enabled is false, to flush its data.
/// Otherwise, the database will wait for the current wal or memtable to be flushed due to
/// flush_interval or reaching l0_sst_size_bytes, respectively. If set to Durable, then the
/// checkpoint includes only writes that were durable at the time of the call. This will be faster,
/// but may not include data from recent writes.
enum CheckpointScope {
    All { force_flush: bool },
    Durable
}

/// Specify options to provide when creating a checkpoint.
struct CheckpointOptions {
    /// Optionally specifies the lifetime of the checkpoint to create. The expire time will be set to
    /// the current wallclock time plus the specified lifetime. If lifetime is None, then the checkpoint
    /// is created without an expiry time.
    lifetime: Option<Duration>,

    /// Optionally specifies an existing checkpoint to use as the source for this checkpoint. This is
    /// useful for users to establish checkpoints from existing checkpoints, but with a different lifecycle
    /// and/or metadata.
    source: Option<UUID>
}

#[derive(Debug)]
pub struct CheckpointCreateResult {
    /// The id of the created checkpoint.
    id: uuid::Uuid,
    /// The manifest id referenced by the created checkpoint.
    manifest_id: u64,
}

impl Db {
    /// Clone a Db from a checkpoint. If no db already exists at the specified path, then this will create
    /// a new db under the path that is a clone of the db at parent_path. A clone is a shallow copy of the
    /// parent database - it starts with a manifest that references the same SSTs, but doesn't actually copy
    /// those SSTs, except for the WAL. New writes will be written to the newly created db and will not be
    /// reflected in the parent database. The clone can optionally be created from an existing checkpoint. If
    /// checkpoint is None, then the manifest referenced by parent_checkpoint is used as the base for
    /// the clone db's manifest. Otherwise, this method creates a new checkpoint for the current version of
    /// the parent db.
    pub async fn create_clone(
        &self,
        clone_path: Path,
        checkpoint: Option<Checkpoint>,
    ) -> Result<(), SlateDBError> {
        …
    }

    /// Creates a checkpoint of an opened db using the provided options. Returns the ID of the created
    /// checkpoint and the id of the referenced manifest.
    pub async fn create_checkpoint(
        &self,
        scope: CheckpointScope,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {
        …
    }

    /// Refresh the lifetime of an existing checkpoint. Takes the id of an existing checkpoint
    /// and a lifetime, and sets the lifetime of the checkpoint to the specified lifetime. If
    /// there is no checkpoint with the specified id, then this fn fails with
    /// SlateDBError::InvalidDbState
    pub async fn refresh_checkpoint(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        id: Uuid,
        lifetime: Option<Duration>,
    ) -> Result<(), SlateDBError> {}

    /// Deletes the checkpoint with the specified id.
    pub async fn delete_checkpoint(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        id: Uuid,
    ) -> Result<(), SlateDBError> {}

    /// Called to destroy the database at the given path. If `soft` is true, This method will
    /// set the destroyed_at_s field in the manifest. The GC will clean up the db after some
    /// time has passed, and all checkpoints have either been deleted or expired. As part of
    /// cleaning up the db, the GC will also remove the database’s checkpoint from the parent
    /// database. If `soft` is false, then all cleanup will be performed by the call to this
    /// method. If `soft` is false, the destroy will return SlateDbError::InvalidDeletion if
    /// there are any remaining non-expired checkpoints.
    pub async fn destroy(path: Path, object_store: Arc<dyn ObjectStore>, soft: bool) -> Result<(), SlateDbError> {
        …
    }
}

mod admin {
    /// Creates a checkpoint of the db stored in the object store at the specified path using the provided options.
    /// Note that the scope option does not impact the behaviour of this method. The checkpoint will reference
    /// the current active manifest of the db.
    pub async fn create_checkpoint(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, SlateDBError> {}

    /// Clone a Db from a checkpoint. If no db already exists at the specified path, then this will create
    /// a new db under the path that is a clone of the db at parent_path. A clone is a shallow copy of the
    /// parent database - it starts with a manifest that references the same SSTs, but doesn't actually copy
    /// those SSTs, except for the WAL. New writes will be written to the newly created db and will not be
    /// reflected in the parent database. The clone can optionally be created from an existing checkpoint. If
    /// parent_checkpoint is None, then the manifest referenced by parent_checkpoint is used as the base for
    /// the clone db's manifest. Otherwise, this method creates a new checkpoint for the current version of
    /// the parent db.
    pub async fn create_clone(
        clone_path: Path,
        object_store: Arc<dyn ObjectStore>,
        parent_path: Path,
        parent_checkpoint: Option<Uuid>,
    ) -> Result<(), SlateDBError> {
        …
    }
}

/// Configuration options for the database reader. These options are set on client startup.
#[derive(Clone)]
pub struct DbReaderOptions {
    /// How frequently to poll for new manifest files. Refreshing the manifest file allows readers
    /// to detect newly compacted data.
    pub manifest_poll_interval: Duration,

    /// For readers that refresh their checkpoint, this specifies the lifetime to use for the
    /// created checkpoint. The checkpoint’s expire time will be set to the current time plus
    /// this value. If not specified, then the checkpoint will be created with no expiry, and
    /// must be manually removed. This lifetime must always be greater than
    /// manifest_poll_interval x 2
    pub checkpoint_lifetime: Option<Duration>,

    /// The max size of a single in-memory table used to buffer WAL entries
    /// Defaults to 64MB
    pub max_memtable_bytes: u64
}


pub struct DbReader {
    …
}

impl DbReader {
    /// Creates a database reader that can read the contents of a database (but cannot write any
    /// data). The caller can provide an optional checkpoint. If the checkpoint is provided, the
    /// reader will read using the specified checkpoint and will not periodically refresh the
    /// checkpoint. Otherwise, the reader creates a new checkpoint pointing to the current manifest
    /// and refreshes it periodically as specified in the options. It also removes the previous
    /// checkpoint once any ongoing reads have completed.
    pub async fn open(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        checkpoint: Option<UUID>,
        options: DbReaderOptions
    ) -> Result<Self, SlateDBError> { ... }

    /// Read a key an return the read value, if any.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, SlateDBError> { ... }
}

pub struct GarbageCollectorOptions {
    /// Defines a grace period for deletion of a db. A db for which deletion is handled by the GC
    /// will not be deleted from object storage until this duration has passed after it's deletion
    /// timestamp.
    pub db_delete_grace: Duration,
}
```

Finally, we’ll also allow users to create/refresh/delete/list checkpoints using the admin CLI:

```bash
$ slatedb create-checkpoint --help
Create a new checkpoint pointing to the database's current state

Usage: slatedb --path <PATH> create-checkpoint [OPTIONS]

Options:
  -l, --lifetime <LIFETIME>  Optionally specify a lifetime for the created checkpoint. You can
    specify the lifetime in a human-friendly format that uses years/days/min/s, e.g. "7days 30min 10s".
    The checkpoint's expiry time will be set to the current wallclock time plus the specified
    lifetime. If the lifetime is not specified, then the checkpoint is set with no expiry and
    must be explicitly removed
  -s, --source <UUID> Optionally specify an existing checkpoint to use as the base for the newly
    created checkpoint. If not provided then the checkpoint will be taken against the latest
    manifest.

$ slatedb refresh-checkpoint --help
Refresh an existing checkpoint's expiry time. This command will look for an existing checkpoint
and update its expiry time using the specified lifetime

Usage: slatedb --path <PATH> refresh-checkpoint [OPTIONS] --id <ID>

Options:
  -i, --id <ID>              The UUID of the checkpoint (e.g. 01740ee5-6459-44af-9a45-85deb6e468e3)
  -l, --lifetime <LIFETIME>  Optionally specify a new lifetime for the checkpoint. You can specify
    the lifetime in a human-friendly format that uses years/days/min/s, e.g. "7days 30min 10s". The
    checkpoint's expiry time will be set to the current wallclock time plus the specified lifetime.
    If the lifetime is not specified, then the checkpoint is updated with no expiry and must be
    explicitly removed

$ slatedb delete-checkpoint --help
Delete an existing checkpoint

Usage: slatedb --path <PATH> delete-checkpoint --id <ID>

Options:
  -i, --id <ID>  The UUID of the checkpoint (e.g. 01740ee5-6459-44af-9a45-85deb6e468e3)

$ slatedb list-checkpoints --help
List the current checkpoints of the db.

Usage: slatedb --path <PATH> list-checkpoints


```

### Creating a Checkpoint

Checkpoints can be created using the `create_checkpoint` API, the `create-checkpoint` CLI command, or by the
writer/reader clients as part of their normal operations (see below sections).

Checkpoints can be taken from the current manifest, or from an existing checkpoint. Checkpoints cannot be created from
arbitrary manifest versions.

Generally to take a checkpoint from the current manifest, the client runs a procedure that looks like the following:
1. Get the current manifest M at version V.
2. If M.`initialized` is false, or M.`destroyed_at_s` is set, exit with error.
3. Compute a new v4 UUID id for the checkpoint.
4. Create a new entry in `checkpoints` with:
    - `id` set to the id computed in the previous step
    - `manfiest_id` set to V or V+1. We use V+1 to allow addition of the checkpoint to be included with other updates.
    - other fields set as appropriate (e.g. expiry time based on relevant checkpoint creation params)
5. Write a new manifest M' at version V+1. If CAS fails, go to step 1.

To take a checkpoint from an existing checkpoint with id S, the client runs the following procedure:
1. Get the current manifest M at version V.
2. If `destroyed_at_s` is set, exit with error.
3. Look for the checkpoint C with id S under `checkpoints`. If there is no such checkpoint, exit with error.
4. Check that C is not expired. If it is, then exit with error.
5. Compute a new v4 UUID id for the checkpoint.
6. Create a new entry in `checkpoints` with:
    - `id` set to the id computed in the previous step
    - `manifest_id` set to C.`manifest_id`
    - other fields set as appropriate (e.g. expiry time based on relevant checkpoint creation params)
7. Write a new manifest M' at version V+1. If CAS fails, go to step 1.

The `create-checkpoint` CLI and `create_checkpoint` API will run exactly these procedures when called without
and with a source checkpoint ID, respectively.

Otherwise, the exact procedures used by the writer/reader clients vary slightly depending on the context. For example,
the writer may include other updates along with the update that adds the checkpoint. These variants are detailed in the
sections below.

### Writers

Writers will create checkpoints to “pin” the set of SRs currently being used to serve reads. As the writer consumes
manifest updates, it will re-establish its checkpoint to point to the latest manifest version. Eventually, when we
support range scans, the writer will retain older checkpoints to ensure that long-lived scans can function without
interference from the GC.

#### Establishing Initial Checkpoint

The writer first establishes it’s checkpoint against the current manifest when starting up, as part of incrementing the
epoch:
1. Read the latest manifest version V.
2. Compute the writer’s epoch E
3. Look through the checkpoints under `checkpoint` for any entries with `metadata` of type `WriterCheckpoint`, and
   remove any such entries.
4. Create a new checkpoint with:
   - `id` set to a new v4 uuid
   - `manifest_id` set to the next manifest version (V + 1)
   - `checkpoint_expire_time_s` set to None
   - `metadata` set to `WriterCheckpoint{ epoch: E }`.
5. Set `writer_epoch` to E
6. Write the manifest. If this fails with a CAS error, go back to 1.

#### Refreshing the Checkpoint

The writer will periodically re-establish its checkpoint to pin the latest SSTs and SRs. It will run the following
whenever it detects that the manifest has been updated. This happens either when the manifest is polled
(at `manifest_poll_interval`), or when the writer detects an update when it goes to update the manifest with it’s local
view of the core db state:
1. Read the latest manifest version V. (If the writer is applying an update from the local state then in the first
   iteration it can assume that it’s local version is the latest. If it’s wrong, the CAS will fail and it should reload
   the manifest from Object Storage))
2. Decide whether it needs to re-establish it’s checkpoint. The writer reestablishes the checkpoint whenever the set of
   L0 SSTs, or Sorted Runs has changed. This includes both changes made locally by the Writer, or by the Compactor.
    1. If so, create a new checkpoint with
       - `id` set to a new v4 UUID.
       - `manifest_id` set to V+1.
       - `checkpoint_expire_time` set to None.
       - `metadata` set to `WriterCheckpoint{ epoch: E }`.
    2. Delete the old checkpoint from `checkpoints`. When we support range scans, we’ll instead want to maintain
       references to the checkpoint from the range scans and delete the checkpoint when all references are dropped.
       (We should technically do this for point lookups too, but the likelihood of GC deleting an SST in this case is
       very low).
    3. Apply any local changes
    4. Write the new manifest. If this fails with a CAS error, go back to 1.

#### Clones

The writer will also support initializing a new database from an existing checkpoint. This allows users to “fork” an
instance of slatedb, allowing it to access all the original db’s data from the checkpoint, but isolate writes to a new
db instance.

To support this, we add the `Db::open_from_checkpoint` method, which accepts a `parent_path` and `parent_checkpoint`.
When initializing the db for the first time (detected by the absence of a manifest), the writer will do the following:
1. Read the current manifest M, which may not be present.
2. Read the current parent manifest M_p at `parent_path`.
3. M_p.`initialized` is false, exit with error.
4. If M is present:
   1. If the `parent` field is empty, exit with error.
   2. If `initialized`, then go to step 10.
5. Compute the clone's checkpoint ID
   1. If M is present, use M.`parent.parent_checkpoint` as the checkpoint ID
   2. Otherwise, set checkpoint ID to a new v4 UUID
6. If M_p.`destroyed_at_s` is set:
   1. Delete the checkpoint with checkpoint ID from the parent, if any. If CAS fails, go to step 1.
   2. Exit with error.
7. If `parent_checkpoint` is not None, read it's manifest M_c. Otherwise let M_c be M_p
8. Write a new manifest M'. If CAS fails, go to step 1. M' fields are set to:
   - parent: set to point to checkpoint ID and initialized set to false
   - writer_epoch: set to the epoch from M_c + 1.
   - compactor_epoch: copied from M_c.
   - wal_id_last_compacted: copied from M_c
   - wal_id_last_seen: copied from M_c
   - l0: copied from M_c
   - l0_last_compacted: copied from M_c
   - compacted: copied from M_c
   - checkpoints: empty
9. Create or update the checkpoint with checkpoint ID pointing to M_c in the parent DB. If CAS fails, go to step 1.
10. Copy over any WAL SSTs between M_c.`last_compacted_wal_id` and M_c.`wal_id_last_seen`.
11. Update M' with `initialized` set to true. If CAS fails, go to step 1.

##### SST Path Resolution

The DB may now need to look through multiple paths to find SST files, since it may need to open SST files written by a
parent db. To support this, we’ll walk the manifest tree up to the root, and build a map from SST ID to Path at startup
time, and maintain this in memory.

##### Deleting a Database

With the addition of clones, we need to be a bit more careful about how we delete a database. Previously we could
simply remove it's objects from the Object Store and call it a day. With clones, it's possible that a SlateDB instance
is holding a checkpoint that another database relies upon. So it's not safe to just delete that data. We'll instead
require that users delete a database by calling the `Db::destroy` method. `Db::destroy` can specify that the delete is
either a soft or hard delete by using the `soft` parameter. This way we can support both more conservative deletes that
clean up data from the GC process after some grace period, and a simple path for hard-deleting a db.

A soft delete will simply mark the manifest as deleted, and rely on a separate GC process to delete the db. It requires
that the GC be running in some other long-lived process from the main writer. The GC process will wait for both some
grace period, and for all checkpoints to either expire or be deleted. At that point it will delete the db's SSTs and
exit. This is covered in more detail in the GC section. A soft delete will be processed by the writer as follows:
1. Claim write-ownership of the DB by bumping the epoch and fencing the WAL. The intent here is to fence any active
   writer. We assume that the writer will detect that it's fenced and exit by the time the GC goes to delete the data.
2. Delete the writer's checkpoint, if any.
3. Update the manifest by setting the `destroyed_at_s` field to the current time.

A hard delete will proceed as follows. It is up to the user to ensure that there is no active writer at the time of
deletion:
1. Check that there are no active checkpoints. If there are, return `InvalidDeletion`
2. If `destroyed_at_s` is not set, then update the manifest by setting `destroyed_at_s` field to the current time. If
   CAS fails, go to step 1.
3. Delete all objects under `path` other than the current manifest.
4. Delete the current manifest.

### Readers

Readers are created by calling `DbReader::open`. `open` takes an optional checkpoint. If a checkpoint is provided,
`DbReader` uses the checkpoint and does not try to refresh/re-establish it. If no checkpoint is provided, `DbReader`
establishes its own checkpoint against the latest manifest and periodically polls the manifest at the interval specified
in `manifest_poll_interval` to see if it needs to re-establish it. If the manifest has not changed in a way that
requires the reader to re-establish the checkpoint, then the reader will refresh the checkpoint's expiry time once half
the lifetime has passed. The checkpoints created by `DbReader` use the lifetime specified in  the `checkpoint_lifetime`
option.

#### Establishing the Checkpoint

To establish the checkpoint, `DbReader` will:
1. Read the latest manifest M at version V.
2. Check that M.`initialized` is true and M.`destroyed_at_s` is not set. Otherwise, return error.
3. Create a new checkpoint with id set to a new v4 uuid, `manifest_id` set to the next manifest version (V + 1),
   `checkpoint_expire_time_s` set to the current time plus `checkpoint_lifetime`, and `metadata` set to None.
4. Update the manifest’s `wal_id_last_seen` to the latest WAL id found in the wal directory
5. Write the manifest. If this fails with a CAS error, go back to 1.

Then, the reader will restore data from the WAL. The reader will just replay the WAL data into an in-memory table.
The size of the table will be bounded by `max_memtable_bytes`. When the table fills up, it’ll be added to a list and a
new table will be initialized. The total memory used should be limited by backpressure applied by the writer. This
could still be a lot of memory (e.g. up to 256MB in the default case). In the future we could have the reader read
directly from the WAL SSTs, but I’m leaving that out of this proposal.

##### Checkpoint Maintenance

If the reader is opened without a checkpoint, it will periodically poll the manifest and potentially re-establish the
checkpoint that it creates if the db has changed in a way that requires re-establishing the checkpoint (the set of SSTs
has changed). It does this by re-running the initialization process described above, with the following modifications:
- It will delete the checkpoint that it had previously created (when we support range scans, we’ll need to reference
  count the checkpoint).
   - It will purge any in-memory tables with content from WAL SSTs lower than the new `wal_id_last_compacted`.

Additionally, if the time until `checkpoint_expire_time_s` is less than half of `checkpoint_lifetime` the Reader will
update the `checkpoint_expire_time_s` to the current time plus `checkpoint_lifetime`.

#### Garbage Collector

The GC task will be modified to handle soft deletes and manage checkpoints/clones. The GC tasks's algorithm will now
look like the following:
1. Read the current manifest. If no manifest is found, exit.
2. If `destroyed_at_s` is set, and the current time is after `destroyed_at_s` + `db_delete_grace`, and there are no
   active checkpoints, then:
    1. If the db is a clone, update the parent db's manifest by deleting the checkpoint.
    2. Delete all object's under the db's path other than the current manifest.
    3. Delete the current manifest.
    3. Exit.
3. Garbage collect expired checkpoints
    1. Find any checkpoints that have expired and remove them
    2. Write the new manifest version with checkpoints deleted. If CAS fails, go back to 1 to reload checkpoints that
       may be refreshed or added.
4. Delete garbage manifests. This includes any manifest that is older than `min_age` and is not referenced by a
   checkpoint.
5. Read all manifests referenced by a checkpoint. Call this set M
6. Clean up WAL SSTs.
    1. For a given Manifest n Let referenced_wal(n) be the set of all WAL SSTs between n.`wal_id_last_compacted` and
       n.`wal_id_last_seen`
    2. Let W be the set of all referenced_wal(n) for all n in M AND all WAL SSTs larger than `wal_id_last_compacted`
       of the current manifest.
    2. Delete all WAL SSTs not in W.
7. Clean up SSTs.
    1. Let S be the set of all SSTs from all manifests in M.
    2. Delete all SSTs not in M with age older than `min_age`
8. Detach the clone if possible.
    1. If the DB instance is a clone, and it's manifest and contained checkpoints no longer references any SSTs from
       its `parent_checkpoint` (we can tell this if all SSTs are under the current db's path), then detach it:
        1. Update the parent db's manifest by removing the checkpoint.
        2. Update the db's manifest by removing the `parent` field.

Observe that users can now configure a single GC process that can manage GC for multiple databases that use soft
deletes. Whenever a new database is created, the user needs to spawn a new GC task for that database. When the GC
task completes deleting a database, then the task exits. For now, it's left up to the user to spawn GC tasks for
databases that they have created.
