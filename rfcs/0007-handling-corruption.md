# Handling Data Corruption

Status: Draft

Authors:

* [Derrick J Wippler](https://github.com/thrawn01)

References:

- https://www.sqlite.org/atomiccommit.html
- https://avi.im/blag/2024/sqlite-bit-flip/
- https://github.com/danthegoodman1/BreakingSQLite
- https://www.sqlite.org/recovery.html
- https://www.postgresql.org/docs/9.1/app-pgresetxlog.html
- https://www.postgresql.org/docs/current/app-initdb.html#APP-INITDB-DATA-CHECKSUMS
- https://www.postgresql.org/docs/current/runtime-config-developer.html#GUC-IGNORE-CHECKSUM-FAILURE
- https://www.postgresql.org/docs/current/runtime-config-developer.html#GUC-ZERO-DAMAGED-PAGES
- https://docs.tigerbeetle.com/about/safety/
- https://github.com/thrawn01/db-corruption

## Motivation
The durability of object storage is a significant factor driving the adoption
of disaggregated storage. However, we should not assume that data corruption
will never occur. Several factors can lead to corruption, including:

- Programming errors in SlateDB
- Malicious intent to corrupt data
- Variations in the logical durability of different object store implementations
- Errors in firmware and drivers

How SlateDB operates in the face of corruption is crucial for its adoption and
for maintaining operator confidence.

Although SlateDB utilizes object stores that typically provide some level of
Logical Availability and Durability, SlateDB should be viewed as a
manifestation of a physical system. This is because SlateDB cannot make
assumptions about the Logical Availability or Durability of the object store it
uses to store data. The object store provided to SlateDB at startup could
simply be a local disk masquerading as a highly available logical store.

### Prioritizing Durability
Prioritizing durability means that in the event of data corruption, the
database will immediately become unavailable until the corruption is repaired
through some form of out-of-band intervention. This approach ensures that the
database cannot lose data without the operator's knowledge.

### Prioritizing Availability
Prioritizing availability means that the database will acknowledge the
corruption, attempt to recover as much data as possible, and warn the operator,
while continuing to remain available without requiring out-of-band
intervention. With this approach, it is the responsibility of the operator
or the logical system to handle any data loss through mechanisms such as
replication or backups.

### Determining Availability or Durability
Determining whether availability or durability is the priority for the operator
is likely beyond the scope of SlateDB.

For a database whose sole purpose is to store end-user profile theme
preferences, availability may be more important to the database operator. In
such cases, the application can fall back to a default theme if user
preferences are not available, resulting in only a minor inconvenience to the
end user.

On the other hand, for a database whose sole purpose is to store financial
data, the loss of even a single transaction can have significant financial
implications and must be avoided at all costs.

Since it is impossible to determine which priority the operator favors, SlateDB
should strive to support both paths equally.

### A Corruption Review
The following is a brief review how other database systems handle data
corruption before discussing my proposal for SlateDB.

#### SQLite
"SQLite is a C-language library that implements a small, fast, self-contained,
high-reliability, full-featured, SQL database engine". SQLite implements
physical durability, and makes no assumptions about the reliability of the disk
it operates upon. SQLite becomes unavailable when corruption occurs, returning
`SQLITE_ERROR` when performing queries against a corrupted file.

> SQLite assumes that the detection and/or correction of bit errors caused by
> cosmic rays, thermal noise, quantum fluctuations, device driver bugs, or
> other mechanisms, is the responsibility of the underlying hardware and
> operating system. SQLite does not add any redundancy to the database file for
> the purpose of detecting corruption or I/O errors. SQLite assumes that the
> data it reads is exactly the same data that it previously wrote. --
> https://www.sqlite.org/atomiccommit.html

##### Silently Skips Faulty WAL Frames
> SQLite has checksums for WAL frames. However, when it detects a corrupt
> frame, it silently ignores the faulty frame and all subsequent frames. --
> https://avi.im/blag/2024/sqlite-bit-flip/
> and https://github.com/danthegoodman1/BreakingSQLite

SQLite has a recovery API which allows users to parse a SQLite file while
skipping corrupted entries. As such the recovered data likely has lost data.

> It is best to think of the recovery API as a salvage undertaking. Recovery
> will extract as much usable data as it can from the wreck of the old
> database, but some parts may be damaged beyond repair and some rework and
> testing should be performed prior to returning the recovered database to
> service. -- https://www.sqlite.org/recovery.html

#### PostgreSQL
"PostgreSQL is a powerful, open source object-relational database system with
over 35 years of active development that has earned it a strong reputation for
reliability, feature robustness, and performance".

PostgreSQL provides physical durability, but can be run in a logically durable
configuration through replication. However, it does not provide logical
durability for the WAL. This is because both logical and physical replication
are based off the WAL of the primary node. If the WAL is corrupted,
transactions associated with that WAL entry are lost and the database becomes
unavailable. See https://www.postgresql.org/docs/9.1/app-pgresetxlog.html

PostgreSQL does not perform checksum on data read from disk by default. It must
be enabled See
https://www.postgresql.org/docs/current/app-initdb.html#APP-INITDB-DATA-CHECKSUMS

Corruption due to checksum mismatch is reported as an error and aborts the
request but this can be disabled via
https://www.postgresql.org/docs/current/runtime-config-developer.html#GUC-IGNORE-CHECKSUM-FAILURE

Corrupted page headers cause PostgreSQL to report an error and abort the
current transaction.
https://www.postgresql.org/docs/current/runtime-config-developer.html#GUC-ZERO-DAMAGED-PAGES

#### Tiger Beetle
"TigerBeetle is a distributed financial accounting database designed for
mission critical safety and performance". It provides high durability through
logical consensus between multiple replicas. If corruption occurs on one of the
physical nodes, the node attempts to recover as much data as possible by
skipping or -- if possible -- reconstructing any corrupted data. The node then
warns replication of the data corruption which then recovers the missing data
from a replica.

If no quorum of replicas is available to reconstruct the data, tiger beetle
becomes unavailable requiring user intervention.

> However, absolute durability is impossible, because all hardware can
> ultimately fail. Data we write today might not be available tomorrow.
> TigerBeetle embraces limited disk reliability and maximizes data durability
> in spite of imperfect disks. We actively work against such entropy by taking
> advantage of cluster-wide storage. A record would need to get corrupted on
> all replicas in a cluster to get lost, and even in that case the system would
> safely halt.

See https://docs.tigerbeetle.com/about/safety/

#### Pebble
Pebble is a key-value store inspired by LevelDB and RocksDB, designed for high
performance and internal use by CockroachDB (CRDB). It is written in Go and
serves as the physical durability layer for CRDB, while CRDB provides logical
durability on top of nodes running Pebble.

##### Truncates Faulty WAL Frames
Similar to SQLite, Pebble truncates all subsequent WAL entries when it detects
corruption in the WAL. However, unlike SQLite, Pebble logs a warning by
default.

```
[JOB 1] WAL file 000002.log with log number 000002 stopped reading at offset: 36289; replayed 869 keys in 869 batches
```

##### Prioritizes Availability
When Pebble encounters corruption, it returns an error for the affected keys in
the SST, such as `pebble/table: invalid table 000004 (checksum mismatch at
1615/1618)`. Despite this, the database remains available. Keys not affected by
the corruption remain accessible, and new keys can be added to the database.
Updating corrupted keys with the same data has no effect.

#### RocksDB
RocksDB is an embeddable persistent key-value store written in C++ and used by
multiple databases for their physical durability layer.

##### Truncates Faulty WAL Frames
Like SQLite and Pebble, RocksDB truncates all subsequent WAL entries when it
finds corruption in the WAL. Upon discovering corruption, it logs a warning,
and the database remains available.

```
[db/db_impl/db_impl_open.cc:1119] 000004.log: dropping 1482 bytes; Corruption: checksum mismatch
```

##### Prioritizes Availability
Similar to Pebble, RocksDB returns an error for keys in the SST affected by
corruption, such as `Corruption: block checksum mismatch: stored(context
removed) = 1930575595, computed = 3064321270, type = 4 in rocksdb/000008.sst
offset 1613 size 1621`. Despite this, the database remains available. Keys not
affected by the corruption remain accessible, and new keys can be added to the
database.

For my research into RocksDB and Pebble, See  https://github.com/thrawn01/db-corruption

## Goals
The goal of this proposal is to develop a robust and flexible error reporting
and corruption handling system. This system will be suitable for building both
durability and availability systems on top of SlateDB.

Goals:
- Ensure the database remains available in the face of corruption.
- Provide the option to report errors immediately when corruption is detected.
- Provide a corruption API that developers and operators can use to repair and
  attempt recovery of corrupted data.
- Enhance the robustness of the WAL implementation by attempting to repair and
  recover as much of the WAL as possible, rather than truncating the remaining
  entries.

### Proposed Solution
I propose adding a callback function called `DBOptions.on_corruption` which is
called when SlateDB encounters corruption during operation. The use of a call
back allows developers to decide their desired behavior when corruption is
detected. The return value of the callback informs SlateDB of the operators
desire to abort the current operation with an error, or to continue operation
attempting to skip the encountered corruption.

To support developers who value availability, the `on_corruption` callback provided
by the operator can be designed to log or notify the operator of the corruption,
but otherwise leaving the database available.

To support developers who value durability, the `on_corruption` callback provided
by the operator can be designed to log or notify the operator of the corruption, 
then return an error, indicating the operators desire to make the database unavailable
in order to avoid losing data which may be recoverable.

Additionally, the call back can be used to notify logical systems built on top
of SlateDB that corruption exists and take appropriate action to repair and
restore corrupted data from replicas or backups.

> NOTE: This RFC assumes that the writer and compaction tasks are always running 
> within the same process. Running compaction separately from the writer makes it 
> impossible to deny writes to the database when corruption is detected during 
> MemTable flush or compaction. Although we cannot enforce this restriction on 
> the operator, the consequences of running compaction as a separate process 
> should be thoroughly documented.

> NOTE: If SlateDB expects a file to exist in the object store, but it does not, 
> this is considered corruption, and the `DBOptions.on_corruption` callback is invoked.

#### Callback Details
The operator or logical system may need to differentiate error handling based on how
the corruption was discovered. To facilitate this, "corruption details" will be provided
to the callback and include the following information:

- The process that identified the corruption, such as WALRecovery, Compaction, Get, or MemTable Flush.
- The type of file affected by the corruption, including WAL, SST, Manifest, or MissingFile.
- A detailed description of the corruption error, such as Checksum error, Missing file, etc.
- The path to the file where the corruption was detected.

```go
  DBOptions.on_corruption = func(d CorruptionDetails) error {
    log.Error("Corruption Detected: %s", d.String())
    // Page the operator
    if err := pageOperator(d); err != nil {
      log.Error("while paging operator: %s", err)
    }

    // Could inform the logical system to repair data
    // via a replica
    if err := signalCorruption(d); err != nil {
      log.Error("while signaling: %s", err)
    }

    // If we want the database to become unavailable until 
    // corruption is fixed, return an error and SlateDB will
    // perform a safe shutdown.
    return fmt.Errorf("Corruption Detected: %s", d.String())

    // Else we return no error, and the database remains available.
    return nil
  }
```

To support durability by default, if `DBOptions.on_corruption` is not defined by
the user, `DBOptions.on_corruption` should default to a function which both
logs the error and always returns an error, thus making the database unavailable
by default.

#### Handling WAL Corruption during Recovery
It is reasonable for a developer to assume that once data is confirmed to be
written to the Write-Ahead Log (WAL), the data or transaction is considered
durably written to storage. However, as discussed in the "Corruption Review"
section of this document, this assumption is only theoretical. In practice,
corruption may only be discovered when recovering from a system failure or
crash, which is when WAL entries are most critical.

It is important to note that during normal LSM operation, the movement of
entries from the WAL to the SSTable occurs in memory. The WAL is only used as a
recovery mechanism in case of a catastrophic failure of the LSM. Therefore, the
loss of committed WAL data can only occur when both of the following conditions are
true.

1. The LSM application exits before compaction can complete.
2. Corruption of the WAL occurs.

Although the probability of both conditions being true is very low, especially
in the context of durable object storage, it is not impossible.

To address this, I propose that the `open_with_opts()` function perform WAL
recovery synchronously and invoke `DBOptions.on_corruption` when WAL corruption
is detected. If `DBOptions.on_corruption` returns an error, the database open
operation should be aborted, and an error should be returned from
`open_with_opts()`. If `DBOptions.on_corruption` does not return an error,
corrupt entries in the WAL should be skipped until a valid entry is found.

The error returned should be of a distinct type that can easily be identified
to determine whether the error is due to non-transient corruption or a
transient issue (such as network connectivity problems).

##### During MemTable Flush
If corruption is detected during a MemTable flush and `DBOptions.on_corruption`
returns an error, the compaction task will abort, and the database will be
closed. Subsequent calls to `get` or `put` on a closed database will result in
a closed database error.

If `DBOptions.on_corruption` does not return an error, the current MemTable
flush will abort, but the database will remain available. MemTable flush
attempts will continue until either the corruption is resolved or the database
is closed.

> NOTE: Corruption encountered during MemTable flush is likely to be a corrupted
> manifest file.

#### Handling SST Corruption During Compaction
Compaction is an asynchronous process that may encounter corruption in Manifest
files, SST files, or missing SST files during operation. If corruption is
detected, the compaction process invokes the `DBOptions.on_corruption`
callback.

If corruption is detected during compaction and `DBOptions.on_corruption`
returns an error, the compaction task will abort, and the database will be
closed. Subsequent calls to `get` or `put` on a closed database will result in
a closed database error.

If `DBOptions.on_corruption` does not return an error, the current compaction
will abort. Future compaction runs will continue to be attempted until either
the corruption is resolved or the database is closed. Since the database
remains available for writes, calls to `put` will cause an accumulation of SST
files in `l0`. This accumulation will continue until the corruption is resolved.

#### Handling SST Corruption During Get
If SlateDB detects corruption while searching for a key during a `get` call,
the `DBOptions.on_corruption` callback is invoked.

- If the callback returns an error, the `get` caller will receive an error that
  includes the error returned by the callback.
- If the callback does not return an error, no error will be returned to the
  `get` caller.

The error returned should be of a distinct type, allowing it to be easily
identified as either non-transient corruption or a transient error (such as
network connectivity issues).

Since `get` calls may be ongoing during corruption, it is possible that the
callback will be called continuously and by multiple threads. Therefore, any
user-defined callback must be thread-safe. Additionally, the callback should
avoid slow or blocking operations to ensure the database remains available if
corruption is encountered.

The caller can choose to abort the application if a corruption error is
returned by `get`. Using the details included in the corruption error, the
application can provide operators with detailed information on the corruption,
including suggestions on how to fix it.

#### Handling Manifest Corruption during Open
If corruption is detected in a manifest when calling `open_with_opts()`, the
callback `DBOptions.on_corruption` is invoked. Regardless of the return value
of the callback, `open_with_opts()` will return an error, and the database open
operation will be aborted. A database without a valid manifest cannot operate.

#### Handling SST Corruption in Cache Files
If SlateDB detects corruption in a cache file, it should invalidate the cache
file and remove the corrupt file. SlateDB will not invoke `DBOptions.on_corruption`
as there is no action for the operator to take.

#### Remaining Available during Corruption
The rationale for keeping the database available during corruption is to
provide options to operators and developers building complex systems on top of
SlateDB.

For instance:
- If corruption is detected during MemTable flush, it might be desirable for
  the developer to flush any outstanding buffers to the WAL before closing the
  database for repair, thereby avoiding data loss.
- If corruption is detected in the leveled SSTs, it might be desirable for the
  developer to read uncorrupted keys from the existing SSTs into a new database
  before fetching the corrupted or missing keys from a replica. This approach
  reduces the time to recovery.
- If corruption is detected, the operator may wish to remain available for
  reads while corruption repair is taking place.

However, leaving the database available for compaction and MemTable flush is a
dangerous edge case that future patches to SlateDB might not account for.
Therefore, this aspect should be thoroughly tested to avoid regression.

Operators implementing a logical system on top of SlateDB should utilize the
`DBOptions.on_corruption` callback to notify other SlateDB read instances of
the corruption and take appropriate action. Operators may also use the
`DBOptions.on_corruption` callback to signal the calling system to perform a
safe database shutdown.

### Corruption Inspection and Repair
#### Sources of possible corruption
- WAL files in `wal/`
- SST files in `levels/`
- Manifest files in `manifest/`
- Referencing files listing in the manifest, but do not exist in `levels/`

#### SST Corruption
The following is a detailed list of possible corruption sources while reading version 0
of an SST file.

- `keyPrefixLen` length exceeds length of first key in block
- `keySuffixLen` length exceeds length of block
- If `flags` indicate record has expired or create fields yet row length is not long enough
- If `flags` indicate record has a value, yet row length is not long enough
- If block checksum fails
- If block decompression fails
- If the (offset_count * 2) - num_offsets is negative
- If the block has no offsets (Empty blocks are not allowed)
- If `SsTableInfo` fails checksum
- If `SsTableIndex.Offset is out of bounds
- If `SsTableIndex` fails checksum
- If Bloom Filter fails checksum
- If Bloom Filter decompression fails

##### SST Repair
To support the repair of SSTs, I propose introducing two functions:
`sst_inspect()` and `sst_repair()`. These functions will provide developers with
tools to inspect the current contents of a specific SST, and repair a specific
SST that has been identified as corrupt.

Details on handling repair of different types of corruption
- **Block Corruption** involves skipping corrupted blocks or key/value entries
  in the block. In this case, data in the skipped block or entry is lost.
- **Filter Corruption** involves rebuilding the filter using the keys in the SST.
  No data lose occurs if this occurs.
- **Info Corruption** involves scanning each block at the start of the SST
  recreating the block by scanning each key/value entries
- **Index Corruption** involves scanning each block at the start of the SST
  recreating the block by scanning each key/value entries

Version 0 of the SST format places the key/value entries at the top of each block,
which is also the top of the SST file. Since each key/value entry includes the length
of the entry, it is possible to scan the sequentially scan the SST extracting entries
as the file is scanned. While it is possible to extract entries, it is not possible to
know when the block data ends and block offset listing begins. Its likely not possible
to even guess as both the block offset and the `KeyPrefixLen` which prefixes each entry
both start with an unsigned int16 (2 bytes) and there is no "magic delimiter" similar to a 
"magic number" currently in use to identify the end of a block of key/value entries.
See https://en.wikipedia.org/wiki/File_format#Magic_number

```
+-----------------------------------------------+
|                    Block                      |
+-----------------------------------------------+
|  +-----------------------------------------+  |
|  |  Block.Data                             |  |
|  |  (List of KeyValues)                    |  |
|  |  +-----------------------------------+  |  |
|  |  | KeyValue Format                   |  |  |
|  |  +-----------------------------------+  |  |
|  |  ...                                    |  |
|  +-----------------------------------------+  |
|  0xCOFFEE
|  +-----------------------------------------+  |
|  |  Block.Offsets                          |  |
|  |  +-----------------------------------+  |  |
|  |  |  Offset of KeyValue (2 bytes)     |  |  |
|  |  +-----------------------------------+  |  |
|  |  ...                                    |  |
|  +-----------------------------------------+  |
|  +-----------------------------------------+  |
|  |  Number of Offsets (2 bytes)            |  |
|  +-----------------------------------------+  |
|  |  Checksum (4 bytes)                     |  |
|  +-----------------------------------------+  |
+-----------------------------------------------+
```
I propose we add a "magic delimiter" of `0xCOFFEE` (6 bytes) to the end of the
block, such that when scanning the next block if we find a `0xCOFFEE = 12648430`
we found the end of the block. This delimiter would ONLY be useful in recovering data
from the SST and would have no impact on SST parsing as it would not be included in the 
`Block.Offsets` calculation. The only impact is that the SST file will be 6
bytes larger (if uncompressed) for each block in the SST file.

In the event of block checksum failure during repair, the user will need to verify the integrity
of the entries manually, either by dumping each block to a file and having the user validate the data,
or providing a terminal prompt accepting each entry in the block.

SST inspection will parse and output Info, Index, Block, Block Checksum, Block Offset Count,
and Block Offsets, along with any Key/Value entries in each block. It should report if any of the 
checksums fail or offsets are invalid. Inspection should scan each block and make use of the
`0xCOFFEE` delimiter to inspect in the event of SST metadata corruption or checksum failure.

##### WAL Inspection and Repair
Since WAL uses the same format as SST files, `sst_inspect()` can be used to inspect WAL files,

To support repair of the WAL, I propose introducing the function `wal_repair()`. Which scans
all the SST files in the WAL skipping any blocks, SSTs or invalid key/value entries.

##### Manifest Inspection and Repair
TODO: This portion is still under investigation, as it might not be possible to
repair the manifest if it is corrupted. For now, I'm leaving this here.

To support the repair of the Manifest, I propose introducing two functions:
`manifest_inspect()` and `manifest_repair()`. These functions will enable
developers to inspect the details of the manifest file if possible and rebuild
the manifest file from the contents of the object store.

Currently, the manifest file is marshalled using FlatBuffers. As such, it may
be possible to extract some information from a corrupted manifest file, which
could be useful in regenerating a new manifest file. Specifically, it may be
possible to recover the Writer and Compactor Epochs from the corrupted manifest
file. If this is not feasible, a previous version of the manifest file could be
used to extract this information. The remainder of the manifest file can be
rebuilt using the SST files in the `levels/` directory.

### Inspection tools
The inspection functions are designed to provide verbose information about the
WAL, Manifest, or SST. This includes reporting the contents of the files and any
corruption found. The goal is to provide operators with tools to inspect the 
file contents, any corruption, and the surrounding data without requiring intimate
knowledge of the current version of file format.

The inspect and repair functions should share the following characteristics:
- Both WAL, Manifest, and SST variants should accept raw file input, such that 
  procurement of the file via SST ID (either WAL or compacted) or out of band file 
  procurement is possible. This allows operators or developers to download files
  out of band and inspect or repair the files using the provided functions
- Repair functions should include a "dry run" option which only reports repairs 
  without actually perform the repair.

#### Repair Tools
We should introduce a new function called `repair()`, which encapsulates 
`sst_repair()`, `manifest_repair()`, `wal_repair()` into a single function. 
This function scans the WAL, Manifest and all SSTs in the database repairing
or truncating corrupt data.

Before performing any repair, the `repair()` function will make a copy of the
original file and store it in a `corrupted/` path on the object store. This
provides operators who run the `repair()` function in haste with the option to
inspect the corrupt data file after the database has been restored to an
operational state.

We should introduce a new function called `validate_db()`, which encapsulates
portions of `sst_inspect()`, `manifest_inspect()` to validate the integrity
of every WAL, SST and manifest file in the database.

### NOTES
We could optionally support running `validate_db()` when
`open_with_opts()` is called through a future `DBOptions` configuration.
Additionally, we can expose the inspection and repair function via a CLI
available to operators.

### TODO
- Understand if manifest repair is possible
- Understand how `admin` currently interacts with the database and craft
admin commands the end user can use to inspect and repair the database.
- Decide if we should include an option to verify MD5 checksums from the
object store
