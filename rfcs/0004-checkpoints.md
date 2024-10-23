# SlateDB Checkpoints and Clones

Status: Under Review

Authors:
* [Rohan Desai](https://github.com/rodesai)

## Current Implementation

SlateDB runs a Writer, a Compactor - optionally in a separate process, and a Garbage Collector (GC) - also optionally in a separate process. The writer writes data to L0 SSTs. The Compactor then goes and compacts these L0 SSTs into Sorted Runs (SR) under the “compacted” directory, and then further compacts those SRs as they accumulate. Updates to the current set of L0s and Sorted Runs are synchronized between the Writer and Compactor through updates to the centralized manifest. When reading, the Writer gets a reference to a copy of the metadata for the current set of sorted runs and searches them for the requested data. Meanwhile, the GC scans the SSTs in Object Storage and deletes any SSTs that are not referenced by the current manifest and are older than some threshold.

## Problem

The current implementation poses a few problems and limitations:
1. There is nothing preventing the GC from deleting SSTs out from the Writer while it’s attempting to read their data. This isn’t a big deal today as we only support key lookups, which are fast. But this could become a more acute problem when we support range scans which can be long.
1. Currently only the Writer process can interact with the database. We’d like to support additional read-only clients, called Readers that can read data.
1. Some use cases can benefit from forking the database and establishing a new db that relies on the original DB’s SSTs but writes new data to a different set of SSTs from the original writer.

## Goals

In this RFC, we propose adding checkpoints and clones to address this limitation.
- Clients will be able to establish cheap db checkpoints that reference the current version of SlateDB’s manifest.
- Writers will use checkpoints to “pin” a version of the database for reads to avoid the GC from deleting the SSTs.
- Readers will use checkpoints similarly, and many Reader processes can coexist alongside a Writer.
- Finally, we will support creating so-called “clones” from a checkpoint to allow spawning new Writers that write to a “fork” of the original database.

### Out Of Scope

This RFC does not propose:
- Any mechanism to provide isolation guarantees from other concurrent reads/writes to the database.

## Proposal

At a high level, this proposal covers a mechanism for establishing database checkpoints, and reviews how the various processes will interact with this mechanism. Logically, a checkpoint refers to a consistent and durable view of the database at some point in time. It is consistent in the sense that data read from a checkpoint reflects all writes up to some point in time, and durable in the sense that this property holds across process restarts. Physically, a checkpoint refers to a version of SlateDB’s manifest.

### Manifest Schema

Checkpoints themselves are also stored in the manifest. We’ll store them using the following schema:

```
table ManifestV1 {
   // Optional path to a source database from which this database was cloned
   source_path: string;


   // Optional source checkpoint ID 
   source_checkpoint: UUID;


   // Optional epoch time in seconds that this database was destroyed
   destroyed_at_s: u64;




   ...
   // A list of current checkpoints that may be active
   checkpoint: [Checkpoints] (required);
}


table WriterCheckpoint {
   epoch: u64;
}

union CheckpointMetadata { WriterCheckpointMetadata }


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


   // Optional metadata associated with the checkpoint. For example, the writer can use this to clean up checkpoints from older writers.
   metadata: CheckpointMetadata;
}
```

### Public API

We’ll make the following changes to the public API to support creating and using checkpoints:

```
/// Defines the scope targeted by a given checkpoint. If set to All, then the checkpoint will include
/// all writes that were issued at the time that create_checkpoint is called. If force_flush is true,
/// then SlateDB will force the current wal, or memtable if wal_enabled is false, to flush its data.
/// Otherwise, the database will wait for the current wal or memtable to be flushed due to
/// flush_interval or reaching l0_sst_size_bytes, respectively. If set to Durable, then the
/// checkpoint includes only writes that were durable at the time of the call. This will be faster, but
/// may not include data from recent writes.
enum CheckpointScope {
    All{force_flush: bool},
    Durable
}

/// Specify options to provide when creating a checkpoint.
struct CheckpointOptions {
    scope: CheckpointScope
}

impl Db {
    /// Opens a Db from a checkpoint. If no database This will create a new db under path from
    /// the checkpoint ID specified in source_checkpoint. SlateDB will look for the manifest
    /// containing the checkpoint under source_path. The newly created database is a shallow
    /// copy of the source database, and contains all the data from source_checkpoint. New writes
    /// will be written to the new database and will not be reflected in the source database.
    pub async fn open_from_checkpoint(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        source_path: Path,
        source_checkpoint: UUID,
    ) -> Result<Self, SlateDBError> {
        …
    }

    /// Create a checkpoint using the provided options. Returns the ID of the created checkpoint.
    pub async fn create_checkpoint(options: &CheckpointOptions) -> Result<uuid::UUID, SlateDBError> {
        …
    }

    /// Called to destroy the database at the given path. If `soft` is true, This method will set the destroyed_at_s field in the manifest. The GC will clean up the db after some time has passed, and all checkpoints have either been deleted or expired. As part of cleaning up the db, the GC will also remove the database’s checkpoint from the source database. If `soft` is false, then all cleanup will be performed by the call to this method. If `soft` is false, the destroy will return SlateDbError::InvalidDeletion if there are any remaining non-expired checkpoints.
   pub async fn destroy(path: Path, object_store: Arc<dyn ObjectStore>, soft: bool) -> Result<(), SlateDbError> {
       …
   }
}

/// Configuration options for the database reader. These options are set on client startup.
#[derive(Clone)]
pub struct DbReaderOptions {
    /// How frequently to poll for new manifest files. Refreshing the manifest file allows readers to detect newly compacted data.
    pub manifest_poll_interval: Duration,


    /// For readers that refresh their checkpoint, this specifies the lifetime to use for the created checkpoint. The checkpoint’s expire time will be set to the current time plus this value. If not specified, then the checkpoint will be created with no expiry, and must be manually removed.
    pub checkpoint_lifetime: Option<Duration>,


    /// The max size of a single in-memory table used to buffer WAL entries
    /// Defaults to 64MB
    pub max_memtable_bytes: u64
}


struct DbReader {
    …
}

impl DbReader {
    /// Creates a database reader that can read the contents of a database (but cannot write any
    /// data). The caller can provide an optional checkpoint. If the checkpoint is provided, the
    /// reader will read using the specified checkpoint and will not periodically refresh the checkpoint. Otherwise, the reader creates a new checkpoint pointing to the current manifest and refreshes it periodically as specified in the options.
    pub async fn open(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        checkpoint: Option<UUID>,
        options: DbReaderOptions
    ) -> Result<Self, SlateDBError> {...}

    /// Read a key an return the read value, if any.  
  pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, SlateDBError> {...}
}
```

Finally, we’ll also allow users to create checkpoints using the admin CLI:

```bash
$ slatedb create-checkpoint --help
Create a new checkpoint pointing to the database's current state

Usage: slatedb --path <PATH> create-checkpoint [OPTIONS]

Options:
  -l, --lifetime <LIFETIME>  Optionally specify a lifetime for the created checkpoint. You can specify the lifetime in a human-friendly format that uses years/days/min/s, e.g. "7days 30min". The checkpoint's expiry time will be set to the current wallclock time plus the specified lifetime. If the lifetime is not specified, then the checkpoint is set with no expiry and must be explicitly removed

$ slatedb refresh-checkpoint --help
Refresh an existing checkpoint's expiry time. This command will look for an existing checkpoint and update its expiry time using the specified lifetime

Usage: slatedb --path <PATH> refresh-checkpoint [OPTIONS] --id <ID>

Options:
  -i, --id <ID>              The UUID of the checkpoint (e.g. 01740ee5-6459-44af-9a45-85deb6e468e3)
  -l, --lifetime <LIFETIME>  Optionally specify a new lifetime for the checkpoint. You can specify the lifetime in a human-friendly format that uses years/days/min/s, e.g. "7days 30min". The checkpoint's expiry time will be set to the current wallclock time plus the specified lifetime. If the lifetime is not specified, then the checkpoint is updated with no expiry and must be explicitly removed

$ slatedb delete-checkpoint --help
Delete an existing checkpoint

Usage: slatedb --path <PATH> delete-checkpoint --id <ID>

Options:
  -i, --id <ID>  The UUID of the checkpoint (e.g. 01740ee5-6459-44af-9a45-85deb6e468e3)
```

### Writers

Writers will create checkpoints to “pin” the set of SRs currently being used to serve reads. As the writer consumes manifest updates, it will re-establish its checkpoint to point to the latest manifest version. Eventually, when we support range scans, the writer will retain older checkpoints to ensure that long-lived scans can function without interference from the GC.

#### Establishing Initial Checkpoint

The writer first establishes it’s checkpoint when starting up, as part of incrementing the epoch:
1. Read the latest manifest version V.
2. Compute the writer’s epoch E
3. Look through the checkpoints under `checkpoint` for any entries with `metadata` of type `WriterCheckpoint`, and remove any such entries.
4. Create a new checkpoint with id set to a new v4 uuid, `manifest_id` set to the next manifest version (V + 1), and `checkpoint_expire_time_s` set to None, and `metadata` set to `WriterCheckpoint{ epoch: E }`.
5. Set `writer_epoch` to E
6. Write the manifest. If this fails with a CAS error, go back to 1.

#### Refreshing the Checkpoint

The writer will periodically re-establish its checkpoint to pin the latest SSTs and SRs. It will run the following whenever it detects that the manifest has been updated. This happens either when the manifest is polled (at `manifest_poll_interval`), or when the writer detects an update when it goes to update the manifest with it’s local view of the core db state:
1. Read the latest manifest version V. (If the writer is applying an update from the local state then in the first iteration it can assume that it’s local version is the latest. If it’s wrong, the CAS will fail and it should reload the manifest from Object Storage))
2. Decide whether it needs to re-establish it’s checkpoint. The writer reestablishes the checkpoint whenever the set of L0 SSTs, or Sorted Runs has changed. This includes both changes made locally by the Writer, or by the Compactor.
    1. If so, create a new checkpoint with `manifest_id` set to V+1, `checkpoint_expire_time` set to None, and `metadata` set to `WriterCheckpoint{ epoch: E }`.
    2. Delete the old checkpoint from `checkpoints`. When we support range scans, we’ll instead want to maintain references to the checkpoint from the range scans and delete the checkpoint when all references are dropped. (We should technically do this for point lookups too, but the likelihood of GC deleting an SST in this case is very low).
    3. Apply any local changes
    4. Write the new manifest. If this fails with a CAS error, go back to 1.

#### Clones

The writer will also support initializing a new database from an existing checkpoint. This allows users to “fork” an instance of slatedb, allowing it to access all the original db’s data from the checkpoint, but isolate writes to a new db instance.

To support this, we add the `Db::open_from_checkpoint` method, which accepts a `source_path` and `source_checkpoint`. When initializing the db for the first time (detected by the presence of a manifest), the writer will do the following:
1. Read the source manifest MC at `source_checkpoint`.
2. Copy over any WAL SSTs between `last_compacted_wal_id` and `wal_id_last_seen`
3. Verify that `source_checkpoint` is present and not expired. Create a new checkpoint in the source database with the `manifest_id` as `source_checkpoint`.
4. Compute an initial manifest for the new DB. The new manifest will set its fields as follows
    - writer_epoch: set to the epoch from the source checkpoint + 1.
    - compactor_epoch: copied from the source checkpoint.
    - wal_id_last_compacted: copied from the source checkpoint
    - wal_id_last_seen: copied from the source checkpoint
    - l0: copied from the source checkpoint
    - l0_last_compacted: copied from the source checkpoint
    - compacted: copied from the source checkpoint
    - checkpoints: contains a single entry with the writer’s checkpoint.
5. If CAS fails, go back to (1) (TODO: we can skip step 2 - we just need to re-validate that the source checkpoint is still valid because the GC could clean it up)


##### SST Path Resolution

The DB may now need to look through multiple paths to find SST files, since it may need to open SST files written by a parent db. To support this, we’ll walk the manifest tree up to the root, and build a map from SST ID to Path at startup time, and maintain this in memory.

##### Deleting a Database

With the addition of clones, we need to be a bit more careful about how we delete a database. Previously we could simply remove it's objects from the Object Store and call it a day. With clones, it's possible that a SlateDB instance is holding a checkpoint that another database relies upon. So it's not safe to just delete that data. We'll instead require that users delete a database by calling the `Db::destroy` method. `Db::destroy` can specify that the delete is either a soft or hard delete by using the `soft` parameter. This way we can support both more conservative deletes that clean up data from the GC process after some grace period, and a simple path for hard-deleting a db.

A soft delete will simply mark the manifest as deleted, and rely on a separate GC process to delete the db. It requires that the GC be running in some other long-lived process from the main writer. The GC process will wait for both some grace period, and for all checkpoints to either expire or be deleted. At that point it will delete the db's SSTs and exit. This is covered in more detail in the GC section. A soft delete will be processed by the writer as follows:
1. Claim write-ownership of the DB by bumping the epoch and fencing the WAL. The intent here is to fence any active writer. We assume that the writer will detect that it's fenced and exit by the time the GC goes to delete the data.
2. Delete the writer's checkpoint, if any.
3. Update the manifest by setting the `destroyed_at_s` field to the current time.

A hard delete will proceed as follows. It is up to the user to ensure that there is no active writer at the time of deletion:
1. Check that there are no active checkpoints. If there are, return `InvalidDeletion`
2. Delete all objects under `path`.

### Readers

Readers are created by calling `DbReader::open`. `open` takes an optional checkpoint. If a checkpoint is provided, `DbReader` uses the provided checkpoint and does not try to refresh it. If no checkpoint is provided, `DbReader` establishes it’s own checkpoint against the latest manifest and periodically re-establishes this checkpoint at the interval specified in `manifest_poll_interval`. The checkpoints created by `DbReader` use the lifetime specified in the `checkpoint_lifetime` option.

#### Establishing the Checkpoint

To establish the checkpoint, `DbReader` will:
1. Read the latest manifest version V.
2. Create a new checkpoint with id set to a new v4 uuid, `manifest_id` set to the next manifest version (V + 1), `checkpoint_expire_time_s` set to the current time plus `checkpoint_lifetime`, and `metadata` set to None.
3. Update the manifest’s `wal_id_last_seen` to the latest WAL id found in the wal directory
4. Write the manifest. If this fails with a CAS error, go back to 1.

Then, the reader will restore data from the WAL. The reader will just replay the WAL data into an in-memory table. The size of the table will be bounded by `max_memtable_bytes`. When the table fills up, it’ll be added to a list and a new table will be initialized. The total memory used should be limited by backpressure applied by the writer. This could still be a lot of memory (e.g. up to 256MB in the default case). In the future we could have the reader read directly from the WAL SSTs, but I’m leaving that out of this proposal.

##### Checkpoint Maintenance

If the reader is opened without a checkpoint, it will periodically re-establish a checkpoint by re-running the initialization process described above, with the following modifications:
- It will delete the checkpoint that it had previously created (when we support range scans, we’ll need to reference count the checkpoint).
- It will purge any in-memory tables with content from WAL SSTs lower than the new `wal_id_last_compacted`.

#### Garbage Collector

The GC task will be modified to handle soft deletes and manage checkpoints/clones. The GC tasks's algorithm will now look like the following:
1. Read the current manifest. If no manifest is found, exit.
2. If `destroyed_at_s` is set, and the current time is after `destroyed_at_s` + `db_delete_grace`, and there are no active checkpoints, then:
    1. If the db is a clone, update the source db's manifest by deleting the checkpoint.
    2. Delete all object's under the db's path.
    3. Exit.
3. Garbage collect expired checkpoints
    1. Find any checkpoints that have expired and remove them
    2. Write the new manifest version with checkpoints deleted. If CAS fails, go back to 1 to reload checkpoints that may be refreshed or added.
4. Delete garbage manifests. This includes any manifest that is older than `min_age` and is not referenced by a checkpoint.
5. Read all manifests referenced by a checkpoint. Call this set M
6. Clean up WAL SSTs.
    1. Let C be the lowest id of all `wal_id_last_compacted` in M.
    2. Delete all WAL SSTs with id lower than C and age older than `min_age`
7. Clean up SSTs.
    1. Let S be the set of all SSTs from all manifests in M.
    2. Delete all SSTs not in M with age older than `min_age`
8. Detach the clone if possible.
    1. If the DB instance is a clone, and it's manifest and contained checkpoints no longer references any SSTs from its `source_checkpoint` (we can tell this if all SSTs are under the current db's path), then detach it:
        1. Update the source db's manifest by removing the checkpoint.
        2. Update the db's manifest by removing the `source_checkpoint` field.
    
Observe that users can now configure a single GC process that can manage GC for multiple databases that use soft deletes. Whenever a new database is created, the user needs to spawn a new GC task for that database. When the GC task completes deleting a database, then the task exits. For now, it's left up to the user to spawn GC tasks for databases that they have created.
