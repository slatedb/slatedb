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
database will immediately cancel the operation that discovered the corruption.
In this way the database operation is unavailable until the corruption is repaired
through some form of out-of-band intervention. However, operations which
do not encounter corruption will continue to operate normally.

### Prioritizing Availability
Prioritizing availability means that the database will continue operation
by attempting to skip or ignore corruption. This ensures the database will remain
available even if inconsistent data is returned or data is missing.

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

Out Of Scope:
- Repair and Inspection of corrupted files.

### Proposed Solution
Add a callback function called `DBOptions.on_corruption` which is called when
SlateDB encounters corruption during operation. The use of a call back allows
developers to decide their desired behavior when corruption is detected. The
return value of the callback informs SlateDB of the operators desire to abort
the current operation with an error, or to continue operation ignoring the corruption.

To support developers who value availability, the `on_corruption` callback
provided by the operator can be designed to log or notify the operator of the
corruption, but otherwise allowing the operation to continue.

To support developers who value durability, the `on_corruption` callback
provided by the operator can be designed to log or notify the operator of the
corruption, then return an error, indicating the operators desire to cancel the
database operation in order to avoid losing data which may be recoverable via out
of band processes.

The intent of the call back function is to communicate corruption to operators
and logical systems built on top of SlateDB so they can take appropriate action
to repair and restore corrupted data from replicas or backups.

It is implied that SlateDB may lose or return inconsistent data if the provided
`on_corruption` callback does not return an error. SlateDB is expected to avoid 
data loss even while attempting to remain available. However, it may be impossible
for the operation to continue without accepting some data loss or provide inconsistent
data which may have been written but is not currently available due to corruption.
The callback provides information on the nature of the corruption to provide the 
developer choices on how to respond to different kinds of corruption.

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
- Operators implementing a logical system on top of SlateDB can utilize the
  `on_corruption` callback to notify other SlateDB read instances of
  the corruption and take appropriate action.
- Operators may also use the `on_corruption` callback to signal the
  calling system to perform a safe database shutdown.

#### Callback Details
The operator or logical system may need to differentiate error handling based
on the kind of corruption. To facilitate this, following "corruption details"
will be provided to the callback:

- The type of file affected by the corruption. (WAL, SST, Manifest, etc..)
- A detailed description of the corruption error
- The path to the file where the corruption was detected.

##### Golang Example Usage
```go
var opts DBOptions
opts.OnCorruption = func(d corruption.Details) error {
  // Logs all of the corruption details
  log.Error("Corruption Detected: %s", d.String())

  // The call back could be used to page or signal the operator in some way.

  if d.Kind == corruption.KindWAL {
    // Signal partition shutdown, we lost transactions
    return errors.New("Detected WAL corruption, database shutdown")
  }

  if d.Kind == corruption.KindCompacted {
    // Signal bit rot, rebuild the partition from replicas, but allow reads to continue
    if err := signalCorruption(d); err != nil {
      log.Error("while signaling: %s", err)
    }
  }
  // Else we return no error, and the database remains available.
  return nil
}
```
##### Rust Example Usage
```rust
let opts = DBOptions {
  on_corruption: Some(Box::new(|details: CorruptionDetails| -> Result<(), CorruptionError> {
    // Log corruption details
    log::error!("Corruption Detected: {}", details);

    // Handle different kinds of corruption
    match details.kind {
      CorruptionKind::WAL => {
        // Signal partition shutdown due to WAL corruption
        return Err(CorruptionError { details: details.clone() });
      }
      CorruptionKind::SST => {
        // Signal bit rot, rebuild from replicas, allow reads to continue
        if let Err(err) = signal_corruption(&details) {
          log::error!("Error while signaling corruption: {}", err);
        }
        Ok(())
      }
      _ => {
        // For other kinds of corruption, return no error and keep the database available
        Ok(())
      }
    }
  })),
  ..Default::default()
};

let db = Db::open_with_opts("path/to/db", opts, object_store).await;
```

If `on_corruption` is not defined, `on_corruption` will default to 
a function which both logs and returns the error.

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

To address this, I propose that the `DB::open()` function perform WAL
recovery synchronously and invoke `on_corruption` when WAL corruption
is detected. If `on_corruption` returns an error, the database open
operation should be aborted, and `InternalError` will be returned by
`DB::open()`. If `on_corruption` does not return an error,
corrupt entries in the WAL should be skipped until a valid entry is found.

> NOTE: If `on_corruption` does not return an error, it is likely that data 
> loss will occur because the WAL must skip corrupted entries or an entire WAL SST 
> file to become available. If this is not desirable, the `on_corruption` callback
> can handle WAL corruption differently from other corruption by inspecting the `kind`
> field for `CorruptionKind::WAL` and returning an error for this case only, thus
> aborting the WAL replay.

##### During MemTable Flush
If corruption is detected during a MemTable flush and `on_corruption`
returns an error, the compaction task will abort, and the database will be
closed. Subsequent calls to `get` or `put` on a closed database will result in
a closed database error. 

Since a MemTable flush could occur asynchronously, there is no direct way of
signaling the user of a flush error. Closing the database provides a safety net 
for users who have not implemented a `on_corruption` callback and might
not be aware that corruption has occurred.

If `on_corruption` does not return an error, the current MemTable
flush will abort, but the database will remain available. MemTable flush
attempts will continue until the corruption is resolved. Calls to `put` 
will succeed until the max number of un-flushed key/value pair bytes is reached.

> NOTE: Corruption encountered during MemTable flush is likely to be a corrupted
> manifest file. If manifest files are corrupted, there is no safe way to recover
> and continue normal operation. In the case where `on_corruption` does not
> return an error, we entertained the possibility of reverting to a previous manifest
> and accepting data loss. However, there is no guarantee a previous manifest exists.

#### Handling SST Corruption During Compaction
Compaction is an asynchronous process that may encounter corruption in Manifest
files, SST files, or missing SST files during operation.

If corruption is detected during compaction, `on_corruption` is invoked and 
compaction will abort, or it will skip the corruption depending on the compaction
strategy implementation. Future compaction runs will continue to be attempted until
either the corruption is resolved or the compaction is explicitly stopped.

Whether compaction aborts its current run or skips corruption depends on the compaction
strategy used. In some compaction implementations, it may be possible to skip corrupted
data without losing any data. The implementation is responsible for determining the best
approach to remain available while also avoiding data loss. Compaction must never 
permanently remove or truncate corrupted data.

Since compaction could be run out of band via admin CLI, or on a separate node
from the writer, our only recourse when encountering corruption is to report the
corruption and do the right thing to avoid losing data. Signaling writer or reader processes
of the corruption -- which may be running on separate machines -- is currently out of scope.

> NOTE: Since the database remains available for writes during compaction failures, 
> calls to `put` could cause an accumulation of SST files in `l0` until the max number
> of SSTs in l0, or the max number of un-flushed key/value pair bytes is reached.

#### Handling SST Corruption During Get
If SlateDB detects corruption while searching for a key during a `get` call,
the `on_corruption` callback is invoked.

- If the callback returns an error, the `get` caller will receive a `InternalError` error
- If the callback does not return an error, no error will be returned to the
  `get` caller.

In the case were a compacted SST is corrupted, calls to `get` which do not encounter
any corruption will continue to operate normally. This allows for partial availability
even when corruption is encountered else where in the database.

Since `get` calls may be ongoing during corruption, it is possible that the
callback will be called continuously and by multiple threads. Therefore, the
`on_corruption` callback must be thread-safe. Additionally, the callback should
avoid slow or blocking operations to ensure the database remains available if
corruption is encountered.

If the call to `get` returns an `InternalError`, indicating corruption, the caller can 
choose to abort the application or close the database. It is the responsibility of the 
caller to report corruption details to operators with detailed information on the corruption.

#### Handling Manifest Corruption during Open
If corruption is detected in a manifest when calling `DB::open()`, the
callback `on_corruption` is invoked. Regardless of the return value
of the callback, `DB::open()` will return an `InternalError`, and the database open
operation will be aborted. A database without a valid manifest cannot operate.

#### Handling SST Corruption in Cache Files
If SlateDB detects corruption in a cache file, it should invalidate the cache
file and remove the corrupt file. SlateDB will not invoke `on_corruption`
as there is no action for the operator to take.

#### Missing Files
When SlateDB detects a missing file in the object store that should exist, it treats this
as data corruption and triggers the `on_corruption` callback. SlateDB then takes appropriate
action based on the specific operation being performed when the corruption was detected. 

#### Object Store Checksum Failures
It is possible the underlying object store verifies checksums when retrieving
objects from the store. If the object store detects a checksum mismatch the `on_corruption`
is invoked and appropriate action specific operation at hand is taken. 

If a checksum mismatch is detected during retrieval, SlateDB invokes the `on_corruption`
callback. The system then handles the corruption according to operation-specific procedures
defined for each type of operation.

### Golang Details
Proposed definition of the `CorruptionDetails`.
```go
type CorruptionKind int

// String returns the kind as a string
func (k CorruptionKind) String() string { /*...*/ }

const (
	// A list of all the possible kinds of corruption
	KindWAL CorruptionKind = iota
	KindCompacted
	KindManifest
	KindFileNotFound
	KindStoreChecksum
)

type CorruptionDetails struct {
	// Kind is the kind of corruption which occurred
	Kind CorruptionKind
	// Message is the error message reported when corruption was detected
	Message string
	// Path to the file which is corrupted
	Path string
}

// String returns a string including all the details of the corruption
func (d CorruptionDetails) String() string { /*...*/ }

// InternalError is returned by `open()` and `get()` methods to indicate
// the `DBOptions.on_corruption` callback was invoked and returned an error
type InternalError struct{ /*...*/ }
```

Proposed definition of the `OnCorruption` callback.
```go
type DBOptions struct {
  // OnCorruption is an optional callback invoked when SlateDB detects data corruption.
  // If the provided callback returns an error SlateDB cancels the current operation
  // and becomes unavailable. If the provided callback returns nil, SlateDB attempts 
  // to remain available by ignoring the corruption to the best of its ability.
  OnCorruption func(CorruptionDetails) error
}
```

### Rust Details
```rust
// A list of all the possible kinds of corruption
#[derive(Debug)]
enum CorruptionKind {
    Compacted,
    SST,
    Manifest,
    FileNotFound,
    StoreChecksum,
}

impl std::fmt::Display for CorruptionKind { /*...*/ }

// Define a struct to hold corruption details
#[derive(Debug)]
struct CorruptionDetails {
  // kind is the kind of corruption which occurred
  kind: CorruptionKind,
  // message is the error message reported when corruption was detected
  message: String,
  // path to the file which is corrupted
  path: String,
}

impl std::fmt::Display for CorruptionDetails { /*...*/ }

// Method to convert CorruptionDetails into a string for InternalError
impl CorruptionDetails {
  fn to_internal_error(&self) -> InternalError {
    let message = format!("Corruption detected: {}", self);
    InternalError { message }
  }
}

// Returned by `open()` and `get()` methods to indicate the 
// `DBOptions.on_corruption` callback was invoked and returned an error
#[derive(Debug)]
struct InternalError {
  message: String,
}

impl std::error::Error for InternalError {}

impl std::fmt::Display for InternalError { /*...*/ }
```
Proposed definition of the `on_corruption` callback.
```rust
type OnCorruptionCallback = dyn Fn(CorruptionDetails) -> Result<(), InternalError>;

struct DBOptions {
  // An optional callback invoked when SlateDB detects data corruption.
  // If the provided callback returns an error SlateDB cancels the current operation
  // and becomes unavailable. If the provided callback returns nil, SlateDB attempts 
  // to remain available by ignoring the corruption to the best of its ability. 
  on_corruption: Option<Box<OnCorruptionCallback>>,
}
```
