# Introduction and requirements

Slate DB operates directly on object stores like S3, Azure Blob Storage, GCP and more. It does not have a WAL.

Set of SSTs that are part of the DB should be maintained in remote store. This document proposes a design for maintaining DB state in remote store.<br/>
These two issues have the initial discussions on the topic.

* [#14](https://github.com/slatedb/slatedb/issues/14) Recover DBState from object storage on restart.
* [#16](https://github.com/slatedb/slatedb/issues/16) Read SSTableInfo without loading entire SST.

This issue [here](https://github.com/slatedb/slatedb/issues/20) covers the uses cases for slateDB. All the points there have significant influence on the manifest design.

**Goals for this document are**

1. Describe the design for maintaining DBState in remote store.
    * Design needs to work for the use cases described. Specifically - remote compaction,  multi-reader, fencing mechanism for second writer.
    * It needs to work on at least S3, Azure Blob Storage and GCP. It should limit service depedencies to the SST object store and a store that supports CAS. In some cases, these might be the same store.
    * Reduce API costs on object store.
2. Share the incremental implementation sequence.


# Background on related components
**DBState** <br/>
[DB State](https://github.com/slatedb/slatedb/blob/main/src/db.rs) is the in-memory state of the DB. It contains the following
* A set of mutable memtables and immutable memtables.
* Sequence of L0 SST file meta.
* Set of SST files in other levels. (after compaction is implemented)
* Other potential metadata in the future, for example schema of key & value.

**Flush** : [Flush](https://github.com/slatedb/slatedb/blob/main/src/flush.rs) would periodically create new L0 SST files. These files are part of the persisted DB once the writes succeed.

**Compaction**: Compaction will remove few SST files and add few others. This is in development.<br/>
**Snapshot**: This creates a point-in-time copy of the metadata, and users can read an older state of the DB. This is in development.<br/>
**Fencing**: There is a single writer. When a new writer starts up, old writer's writes should no longer be part of the DB and old writer should shutdown eventually. This is part of manifest design.

# Design


## Metadata

Metadata, including manifest is stored in a store that supports CAS. For a store like ABS, this is achieved by [conditional put](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations#Subheading1) with `if-none-match` for the first time and `if-match` afterwards. This section will use such an object store, and a later section will describe implementing the same with a store like DynamoDb.

Metadata is stored in a folder called `_metadata`.  Following are the contents of this folder.

Writer and compactor will have an epoch. New instances will increment the epoch and persist.

**WriterEpoch**
* An integer stored in `_metadata/writerepoch`.
* Writers only ever create L0 SST files. 
* On opening the DB in write mode, writer reads `_metadata/writerepoch`, increments and conditionally writes the new epoch to `_metadata/writerepoch`.
* SST files 
    - L0 files created by writer with epoch `w` are prefixed with `w.`
    - More on this in the L0 files section.
* Writers periodically read `_metadata/writerepoch` file and shutsdown if there has been an update.

**CompactorEpoch**
* An integer stored in `_metadata/compactorepoch`.
* On startup, reads `_metadata/compactorepoch`, increments it and conditionally writes it.
* Modifications done by the compactor are tagged with compactor's epoch number `c`.

**Manifest**
* Contains state of the DB at some point in time.
* Updated by compactor.
* Has the following
    * version: A integer version to handle potential behaviour changes in the future.
    * type: Full / Incremental.
    * base_manifest: string, pointer to base manifest. Null if the type is Full.
    * last_compacted_l0: {writerepoch: integer, sstnumber: integer}
        - only SST files larger `last_compacted_l0` are part of the DB when this manifest is used
    * SSTs: set of SSTInfo. Each SSTInfo has a level, a number and a file name. It is null for `incremental`.
    * Changes: 
        - {deletes: set of deleted ssts (level, number), adds: set of added SSTInfo }
        - null if Type is Full.
    * compactor_epoch: integer
        - this is only for debugging purposes.
* Has the following naming convention `_metadata/manifests/manifest.compactorepoch.<randomid>`
* Written to the store with `put-if-absent` semantics, i.e. write should fail if there is another entry.

**Current Manifest**
* Contains pointer to the current manifest file.
* In a file called `_metadata/current_manifest`.
* Contains the following
    * manifest: string, pointer to the manifest file.
    * metadata:
        - optional.
        - details are TODO.
        - examples include flush_ms, schema, writer_epoch_refresh_ms, last_compacted_time. 

### DynamoDB as the metadata store
* Dynamo DB supports [Conditional put](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html) and strongly consistent [reads](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html). With that, dynamo DB can be one of the meta stores supported.


## Data files

### L0 SSTs
* Are in `l0/` folder.
* Has this naming convention - `{writer epoch}.{sst number}.sst`
    - SST number starts at 0 and increments by 1. It is continuous across writer epochs.
* On startup, writer does the following.
    - writes the `_metadata/writerepoch`.
    - lists all the L0 files, learns the last sst_number.
    - writes an *empty* sst file with next sst_number, so that writes by zombie writers are not included.
    - More on this in **recovery** section.

### Other SST files
* These files are under `ssts/` folder.
* There is an optional naming convention, for debugging purposes - `{compactor epoch}.{random sst number}.sst`.

## Recovery

**Open DB in read/write mode**

1. Read `_metadata/current_manifest`.
2. Read the `_metadata/manifests/manifest.compactorepoch.<manifestid>` that current_manifest points to. Construct SSTInfo for all levels except L0.
3. Read the `_metadata/writerepoch`.
4. Increment and write new epoch to `_metadata/writerepoch`.
5. List all the files under `l0/` folder. Sort them by `{18 digit writer epoch}.{18 digit sst number}` in descending order.
    - Add the files to the list of SST files to read.
    - Write an empty sst file with `{current_writer_epoch}.{next sst number}`
    - As it iterates the list, 
        - reject files that are less than `last_compacted_l0`. Assume this is `1.1` for the example later.
        - reject the files that are not the next expected number (current number - 1). This will eliminate files written by zombie writers.
        - Example:
            - Consider this sequence of operations {writerepoch.sstnumber} : [1.1, 1.2, new writer 2 starts, writer 2 writes empty 2.3, 1.3, 1.4, 2.4, 2.5, 2.6, 3.6, 3.7]
            - Following would be set of ssts to be added: [3.7, 3.6, 2.5, 2.4, 2.3, 1.2]
                - 1.1 is not part of it, because it <= `last_compacted_l0`.
                - Even though 1.3 was written, it will not be part of the DB (as expected) since we already saw 2.3 before it.
    - If the writer fails before writing empty sst file, next writer that does it will take care of fencing out the old writer.
6. Pass the list of l0 files to be loaded to DB. DB does the following.
    - Starts a task to read SSTInfo for these files and add it to the end of L0 list.
    - Tracks this task, and blocks reads that need these files.
    - Errors out reads if the task fails.
    - Writes and reads for newly written data is unblocked at this point.
7. DB periodically checks `_metadata/writerepoch` and fails reads and writes if it changed.

**Open DB in read only mode**

1. Steps will be similar to read/write mode.
2. Steps 1,2,5, 6 from write mode.
3. Store the last L0 number in addition.
4. get_changes
    - Periodically, repeat step 5 to get new files.
    - If L0 file is not continous, due to compaction and garbage collection, read current_manifest again.


## Compaction updates to manifest**

Details on how compaction will be done is out of scope for this document. This section covers just the manifest read & write aspects of it.

1. Increment and update `_metadata/compactorepoch`.
2. Open DB in read only mode. 
3. Compact - this will result in new SSTs added, and some marked for deletion. In memory state will be updated. 
4. Create a new manifest. Details to be included are in **Manifest** section. 
5. Read and conditionaly update `_metadata/current_manifest`.
6. Run get_changes from previous section periodically, and repeat steps 3-5  again if necessary.


**Size calculations for manifest file**

Summary: 
* For about 50 GB of data with 1000 writes per second, for 10kb key and 100 kb values, manifest size will be ~16 MB and every minute there will  be 1kb of meta data updates.
* This is an approximation. With this doubling, metadata writes should still be manageable.




|RowId|Description| Expression| Value|
|------|----|----|---|
|A|Block size in MB| A |32|
|B|DB Size in GB| B| 50|
|C|Number of blocks| B * 1024 / A| 1600|
|D|Size of key in KB|D|10|
|E|Size of one BlockMetadata in bytes| 8 + D*1024| 10248
|H|Size of all blockMetadata in MB| E * C / (1024 *1024) | 16
|F|Flush ms|F|10
|G|L0 count in 1 Minute|60 * 1000 / F | 6000
|I|Compaction frequency in seconds|I|60
|J|Writes per second|J|1000
|K|Value size in KB|K|100
|L|Writes per minute in GB| (D + K) * J * 60 / (1024*1024)| 6.3
|M|% updates|M|60
|N|Data compacted from L0 alone in GB| (100 - M) * L / 100|2.5
|O|New blockmeta count in 1 minute| N * 1024 / A| 80.5
|P|Size of new blockMeta in 1 minute in kb|O * E /1024| 1kb
|Q|Metadata writes per month by compactor| 24 * 3600 * 30 * 2 / J|86400
|R| Cost per 10k writes for object store in $|R|0.065
|S|Cost for compactor metadata writes in $|P * R /1000| 0.56






## Create and read snapshot

1. Read `_metadata/current_manifest` to get the current manifest. Say the file is `manifest.compactorepoch7.100`. If the DB is already open, this value will already be in the memory.
2. Run step 5 of *Open DB in read/write mode*, get the last L0 file.
3. Write a `_metadata/snapshots/manifest.compactorepoch7.100.<random_snapshot_id>` file, marking that a snapshot exists. Write the following in the file.
    - last L0 file to include: string, file retreived in step 2.
4. Open DB in read only mode, pass above manifest, a last L0 file and disable `get_changes`.
5. Delete the snapshot file when done.

## Garbage collection

Deletes the SST files that are not part of the table any more.

1. Read `_metadata/current_manifest` and all the manifests in `_metadata/snapshots/` to construct the files are part of the table.
2. Delete data files that are marked for deletion and are not part of any snapshots. Also skip the files that are less than a configurable period, to avoid deleting files that are part of in progress compaction.
3. List the files under `_metadata/manifests/` and delete the ones that are not part of snapshots, is not the current manifest, and created before a configurable interval.

# Implementation sequence proposal

1. Implement the rules for L0, without fencing.
2. Design consensus.
3. Add the store interface for metadata, add writer fencing logic.
4. Compaction gets implemented separately.
5. Manifest Reads/Writes with compaction, for full manifests.
6. Snapshots.
7. Garbage collection.
8. Incremental manifests.





