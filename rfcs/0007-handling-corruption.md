# SlateDB Merge Operator

Status: Draft

Authors:

* [Derrick J Wippler](https://github.com/thrawn01)

References:

- https://www.sqlite.org/atomiccommit.html
- https://avi.im/blag/2024/sqlite-bit-flip/
- https://github.com/danthegoodman1/BreakingSQLite
- https://www.sqlite.org/recovery.html
-  https://www.postgresql.org/docs/9.1/app-pgresetxlog.html
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

### Availability versus Durability
When discussing corruption, there are two primary trade-offs to consider:
Availability and Durability.

For this topic, it is useful to distinguish between Physical Availability and
Durability, and Logical Availability and Durability. Logical Availability and
Durability are built on top of Physical Availability and Durability.

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
I am working under the assumption that, similar to other embeddable key-value
stores, SlateDB is intended to serve as the low-level durability layer. This
layer allows developers to build complex logical durability and availability
systems, such as partitions, replication, and backups.

As mentioned earlier, determining whether the developer or operator prioritizes
durability or availability is beyond the scope of SlateDB. Therefore, this
proposal assumes that while maintaining availability is valuable, SlateDB
should also provide actionable errors and recovery tools for those who
prioritize durability over availability.

### Handling WAL Corruption
It is reasonable for a developer to assume that once data is confirmed to be
written to the WAL, the data or transaction is considered durably written to
storage. However, as discussed in the "Corruption Review" section of this
document, this assumption is only theoretical. In practice, it is possible for
WAL entries written to storage to be corrupted, and this corruption may only be
discovered during compaction or recovery processes.

It is important to note that during normal LSM operation, the movement of
entries from the WAL to the SSTable occurs in memory. The WAL is only used as a
recovery mechanism in case of a catastrophic failure of the LSM. Therefore, the
loss of committed WAL data can only occur under the following two conditions:

1. The LSM application exits before compaction can complete.
2. Corruption of the WAL occurs.

Although the probability of both conditions being true is very low, especially
in the context of durable object storage, it is not impossible. Given this low
probability, we should avoid complicating the WAL by coupling `put` calls to
main SSTable writes. The `WriteOptions.AwaitFlush` (Golang) write option should
be sufficient confirmation that a write or transaction is durably written to
the WAL.

SlateDB can improve over other implementations during the recovery process. In
the implementations I tested, WAL recovery occurs asynchronously after the
database is opened. As a result, the only way to warn the developer of WAL
corruption is via a logged warning.

To support operators who value durability and to ensure durability by default,
I propose that `DB::open_with_opts()` perform WAL recovery synchronously. This
method will return any WAL corruption found as errors and abort the database
open process until the corruption is resolved. The error returned should be a
distinct type that can easily be matched to determine if the error is due to
non-transient corruption or a transient error (such as network connectivity
issues).

To support developers who value availability, we will introduce a new database
option called `DbOptions.wal_auto_recovery`, similar in naming to
`DbOptions.wal_enabled`. If set to `true`, then `DB::open_with_opts()` will
perform WAL recovery asynchronously and automatically repair the WAL to the
best of its ability, logging any corruption and repairs performed.

To support the repair of the WAL, I propose introducing two new methods:
`DB::wal_inspect()` and `DB::wal_repair()`. These methods will provide
developers with tools to inspect the current contents of the WAL, including
corrupted entries, and to repair the WAL to the best of its ability via entity
recovery or truncation of the WAL.

NOTE: `DbOptions.wal_auto_recovery` is a convenience for developers, as the
same result can be achieved by calling `DB::open_with_opts()`, immediately
calling `DB::wal_repair()`, and then calling `DB::open_with_opts()` again.

### Handling SST Corruption
Corruption of the SST can occur at any stage before, during, or after a
database operation. Therefore, the most suitable place to report corruption is
during the `get` call to retrieve keys from the database. If a key is NOT found
and SlateDB detects corruption while searching for the key, an error should be
reported. However, if SlateDB encounters corruption while searching for AND
finding a key, it should not return an error. This ensures the database remains
available even in the presence of corruption.

The error returned should be a distinct type which can easily be matched to
determine if the error is due to non transient corruption or a transient error.
(network connectivity, etc..)

The rationale behind this approach is rooted in the assumption that SlateDB's
design is a manifestation of a physical system. Linux system administrators
would not use physical disks that refuse access to all usable sectors due to a
single bad sector. Such a disk would be impractical in many LVM (Logical Volume
Manager) or RAID configurations. Similarly, the ability for developers who wish
to use SlateDB to achieve Logical Durability is severely compromised if access
to uncorrupted data blocks is not allowed.

To support those operators who value durability, the developer making the call
to `get` can decide to abort the application if and corruption error is returned.
The application can then provide operators with detailed information included
in the error returned by SlateDB on the nature of the corruption and suggestions
on how to fix it.

To support those operators who value availability, the error can be logged or
ignored until either the database is deleted, or corruption repaired.

To support repair of the SST, I propose we introduce new methods called
`DB::sst_check()` , `DB::sst_inspect()` and `DB::sst_repair()` providing
developers tools to validate the integrity of the entire database, inspect the
current contents of a specific SST and repair a specific SST which has been
identified as corrupt.

> NOTE: We could optionally support verification of the entire database when
> `DB::open_with_opts()` is called via some future `DBOptions` config. In
> addition, we can expose the inspection and repair methods via a CLI available
> to operators.


