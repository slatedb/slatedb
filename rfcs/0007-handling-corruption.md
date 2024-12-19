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
`SQLITE_ERROR` when preforming queries against a corrupted file.

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

PostgreSQL does not preform checksum on data read from disk by default. It must
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

If no quorum of replicas is a available to reconstruct the data, tiger beetle
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

- Ensure the database remains available in the face of corruption.
- Report corruption errors directly to the caller when corruption is detected.
- Provide a corruption API that developers and operators can use to repair and
  attempt recovery of corrupted data.
- Enhance the robustness of the WAL implementation by attempting to repair and
  recover as much of the WAL as possible, rather than truncating the remaining
  entries.

### Proposed Solution
To provide support both Availability and Durability, I propose adding a callback
function called `DBOptions.on_corruption` which is called when SlateDB encounters
corruption during operation. The use of a call back allows developers to decide their
desired behavior when corruption is detected. The return value of the callback
then informs SlateDB of the users desire to abort the current operation with an
error, or to continue operation attempting to skip the encountered corruption.

To support developers who value availability, the `on_corruption` callback can
be designed to log or notify the operator of the corruption, but otherwise
remaining available.

To support developers who value durability, the `on_corruption` callback can be
designed to log or notify the operator of the corruption, then return an error,
indicating the operators desire to make the database unavailable in order to 
avoid losing data which may be recoverable.

Additionally, the call back can be used to notify logical systems built on top
of SlateDB that corruption exists and take appropriate action to repair and
restore corrupted data from replicas or backups.

```go
  DBOptions.on_corruption = func(d CorruptionDetails) error {
    log.Error("Corruption Detected: %s", d.String())
    // Could page the operator
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
    // preform a safe shutdown.
    return fmt.Errorf("Corruption Detected: %s", d.String())

    // Else we return no error, and the database remains available.
    return nil
  }
```

If not defined by the user, the `DBOptions.on_corruption` callback should
both log and return an error. This ensures that SlateDB is durable by default.

#### Handling WAL Corruption
It is reasonable for a developer to assume that once data is confirmed to be
written to the WAL, the data or transaction is considered durably written to
storage. However, as can be seen in the "Corruption Review" section of this
document, this assumption is only theoretical. In practice, it is possible for
WAL entries written to storage to be corrupted resulting in data loss.
Additionally, this corruption may only be discovered during compaction or
recovery.

It is important to note that during normal LSM operation, the movement of
entries from the WAL to the SSTable occurs in memory. The WAL is only used as a
recovery mechanism in case of a catastrophic failure of the LSM. Therefore, the
loss of committed WAL data can only occur then following two conditions are
true.

1. The LSM application exits before compaction can complete.
2. Corruption of the WAL occurs.

Although the probability of both conditions being true is very low, especially
in the context of durable object storage, it is not impossible.

To ensure durability by default, I propose that `open_with_opts()` perform
WAL recovery synchronously and return any WAL corruption found 
as errors, aborting the database open process until the corruption is resolved.
The error returned should be a distinct type that can easily be matched to 
determine if the error is due to non-transient corruption or a transient error
(such as network connectivity issues).

#### Handling SST Corruption During Compaction
Compaction is an asynchronous process that may encounter corrupted SST files
during operation. If corruption is detected, the compaction process defers
control to the `DBOptions.on_corruption` callback.

- If the `DBOptions.on_corruption` callback returns an error, the compaction
  operation aborts the current run.
- If the user-defined `DBOptions.on_corruption` callback does not return an
  error, compaction will ignore the corruption to the best of its ability,
  which may result in data loss.

Compaction will continue to attempt future runs, aborting each time corruption
is encountered until the corruption is repaired or compaction is halted. If
compaction were to never run again, it is possible that the operator might
remain unaware that compaction has stopped and fail to restart it after
repairing the corruption.

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

To support operators who value durability, the developer making the `get` call
can choose to abort the application if a corruption error is returned. The
application can then provide operators with detailed information included in
the error returned by SlateDB, including the nature of the corruption and
suggestions on how to fix it.

##### Remaining Available
In the case where corruption is encountered, and `DBOptions.on_corruption` allows
operation to continue, SlateDB should endeavor to return the most recent 
valid data it can.

- If the SST Bloom Filter indicates a value exists in the SST, yet the key was
  not found, and corruption was detected during iteration, the `get` caller
  should receive a Corruption error.
- If the most recent value is in a non-corrupt block, the `get` call should
  return successfully
- If the SST index is corrupt and `get` has not yet found the KV in an earlier
  SST, the call should receive a Corruption error. (This effectively prevents
  reads in SSTs older than the first corrupt SST)
- MORE?

#### Corruption Inspection and Repair
To support the repair of SSTs, I propose introducing two new functions:
`sst_inspect()` and `sst_repair()`. These functions will provide developers with
tools to inspect the current contents of a specific SST, and repair a specific
SST that has been identified as corrupt.

To support the repair of the WAL, I propose introducing two additional functions:
`wal_inspect()` and `wal_repair()`. These functions will allow developers to
inspect the current contents of the WAL, including corrupted entries, and to
repair the WAL to the best of its ability through entity recovery or truncation
of the WAL.

The inspection functions are designed to provide verbose information about the
WAL or SST. This includes reporting the contents of the files and any
corruption found. The goal is to provide operators with tools to inspect the 
file contents, any corruption, and the surrounding data without needing intimate
knowledge of the file format.

The inspect and repair functions should share the following characteristics:
- Both WAL and SST variants should accept raw file input, such that procurement
  of the file via SST ID (either WAL or compacted) or out of band file procurement
  is possible. This allows operators or developers to download files out of band
  and inspect or repair the files using the provided functions
- Repair functions should include a "dry run" option which only reports repairs 
without actually perform the repair.

We should introduce a new function called `repair()`, which encapsulates both
`sst_repair` and `wal_repair` into a single function. This function scans the WAL
and all SSTs in the database to inspect them for corruption and optionally
repairs them if the "dry run" option is not provided.

Before performing any repair, the `repair()` function will make a copy of the
original file and store it in a `corrupted/` path on the object store. This
provides operators who run the `repair()` function in haste with the option to
inspect the corrupt data file after the database has been restored to an
operational state.

#### For Convenience
For convenience:
- We could optionally introduce a new database option called
  `DbOptions.wal_auto_recovery`, similar in naming to `DbOptions.wal_enabled`.
  If set to `true`, then `open_with_opts()` will automatically repair the WAL
  using the `wal_repair()` function. It is only a convenience, as the same
  result can be achieved by calling `open_with_opts()`, immediately calling
  `wal_repair()`, and then calling `open_with_opts()` again.
- We could optionally support verification of the entire database when
  `DB::open_with_opts()` is called through a future `DBOptions` configuration.
  Additionally, we can expose the inspection and repair function via a CLI
  available to operators.
