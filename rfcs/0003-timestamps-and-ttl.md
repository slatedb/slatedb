# SlateDB Timestamps & Time-To-Live

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Motivation](#motivation)
- [Goals](#goals)
- [Out of Scope](#out-of-scope)
- [Public API](#public-api)
- [Implementation](#implementation)
    * [Data Format](#data-format)
    * [Compaction](#compaction)
- [Rejected Alternatives](#rejected-alternatives--follow-ups)
- [Updates](#updates)

<!-- TOC end -->

Status: Accepted

Authors:

* [Almog Gavra](https://github.com/agavra)

References:

* https://github.com/slatedb/slatedb/issues/218
* https://github.com/slatedb/slatedb/issues/225
* https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp
* https://github.com/facebook/rocksdb/wiki/Time-to-Live
* https://github.com/facebook/rocksdb/wiki/Compaction-Filter
* https://dl.acm.org/doi/pdf/10.1145/3483840
* https://disc-projects.bu.edu/lethe/
* https://dl.acm.org/doi/10.1145/3555041.3589719

## Motivation

SlateDB does not have a native understanding of time in data operations (it does
in regard to compaction/garbage collection, discussion on this later). This introduces
additional complexity to operators that want to implement semantics that depend on the
insertion/modification time of rows. The most common usages of timestamps are:

1. **Time-To-Live**: many applications, either for compliance or space-saving purposes
   require purging data a certain amount of time after its insertion. Use cases with time
   series data and privacy requirements such as GDPR come to mind here, though ensuring that
   deleted data is collected within a time bound is left for a future exercise.
2. **Resolving Out-of-Order Operations (out of scope)**: some applications may process incoming
   requests in a manner that results in out-of-order insertions to SlateDB (stream processing
   applications are frequently subject to this type of pattern). Using insertion timestamps to
   resolve which version of a key is more recent may be preferable to using the clock at the time of
   insertion.
3. **Historical Reads (out of scope)**: while SlateDB provides snapshot capabilities to
   ensure reads are consistent and repeatable, it requires a snapshot to have been taken at the
   desired time. Increasing the number of snapshots adds significant storage overhead. Instead, it
   is possible to trade read-latency for space amplification by maintaining multiple versions of
   keys at different timestamps to allow for similar "historical" read operations.

## Goals

- Define the API for specifying TTL on writes
- Define the changes to the on-disk format of stored rows
- Outline mechanism to support Time-To-Live (TTL) via Compaction Filters

## Out of Scope

While the following are out of scope for the initial implementation, the design should
not preclude:

- Handling out-of-order insertions is not in scope for this RFC, though we make sure the design
  accounts for the future possibility (see rejected alternatives section)
- Outlining compaction strategies that are optimized for TTL (such as Cassandra's
  [TWCS](https://cassandra.apache.org/doc/stable/cassandra/operating/compaction/twcs.html)). This
  will require additional metadata per-SST and a compaction strategy to match, but is otherwise
  possible with what's outlined in the scope of this design.
- Outlining compaction strategies that target efficient deletes (such as outlined
  in [Lethe](https://disc-projects.bu.edu/lethe/))
- Ensuring deletions are resolved within a time bound (a TTL does not ensure the data is deleted
  from the object store within the specified time)
- A public API for compaction filters (see https://github.com/slatedb/slatedb/issues/225) - this RFC
  will implement TTL using a compaction filter but will not yet expose a public API for plugging in
  custom compaction filters. Instead, the implementation should ensure that such an extension is
  easy.

## Public API

We will make the following changes to `DbOptions`:

- introduce a configuration for a default TTL, which defaults to `None`

SlateDB uses the system clock internally for TTL expiration. The clock returns the system time
in milliseconds since the Unix epoch, and SlateDB enforces monotonicity by tracking the last
tick and sleeping briefly if clock skew is detected.

```rust
pub struct DbOptions {
    // ...

    /// The default time-to-live (TTL) for insertions (note that re-inserting a key
    /// with any value will update the TTL to use the default_ttl)
    ///
    /// Default: no TTL (insertions will remain until deleted)
    default_ttl: Option<u64>
}
```

A custom `SystemClock` implementation can be provided via the `Db::builder` API using
`with_system_clock()`. This is primarily useful for testing or for environments with
non-standard time sources:

```rust
use slatedb::Db;
use slatedb_common::clock::DefaultSystemClock;
use std::sync::Arc;

let db = Db::builder("my_db", object_store)
    .with_system_clock(Arc::new(DefaultSystemClock::new()))
    .build()
    .await?;
```

`Db#put_with_options` can optionally specify a row-level TTL at insertion time
by leveraging the `PutOptions` struct:

```rust
pub enum Ttl {
    /// Use the default TTL configured in DbOptions
    Default,
    /// No expiration for this entry
    NoExpiry,
    /// Expire after the specified duration (in milliseconds)
    ExpireAfter(u64),
}

pub struct PutOptions {
    /// The time-to-live (ttl) for this insertion. If this insert overwrites an existing
    /// database entry, the TTL for the most recent entry will be canonical.
    ///
    /// Default: the TTL configured in DbOptions when opening a SlateDB session
    pub ttl: Ttl,
}
```

For this RFC we will not introduce the ability to disable filtering out TTL-expired records on
retrieval. The default implementation will read the metadata for every retrieved row and filter out
records where `ts + tll < now`. We can, as a future optimization, enable lazy deserialization of the
metadata so that this doesn't need to happen on each row retrieved if the user requests the ability
to retrieve TTL'd rows that have not yet been compacted. This can be part of the API design for
compaction filters (see https://github.com/slatedb/slatedb/issues/225)

## Implementation

### Data Format

Currently, SlateDB does not have a mechanism for backwards-compatibility of the SST data format.

We will first introduce the following fields to the flatbuffer definitions:

- `format_version`: field that specifies the codec version that encoded this SST
- `row_features`: an enumeration of metadata fields that are available with each encoded row. This
  makes it possible in the future to save space per row by disabling row attributes, but that is
  out of scope for this RFC
- `creation_timestamp`: the time at which this SST file was written. For this RFC this timestamp is
  used to schedule follow-up compactions (see [periodic compaction](#periodic-compaction)).
- `min_ts`/`max_ts`:  metadata that indicates the earliest and latest timestamp values in the SST

```fbs
/// the metadata encoded with each row, note that the ordering of this enum is 
/// sensitive and must be maintained
enum RowFeature: byte {
    RowFlags,
    Timestamp,
    TimeToLive,
}

table SsTableInfo {
    // ...
    
    // The current encoding version of the data block
    format_version: uint;
    
    // The metadata attributes that are encoded with each row. These attributes will be encoded
    // in order that they are declared in the RowAttributes enum
    row_features: [RowFeature];
    
    // The time at which this SST was created (milliseconds since Unix epoch)
    creation_timestamp: long;

    // The minimum timestamp of any row in the SST (milliseconds since Unix epoch)
    min_ts: long;

    // The maximum timestamp of any row in the SST (milliseconds since Unix epoch)
    max_ts: long;
}
```

This value will be written when the SST is created. We will read this back using a codec that maps
1:1 with the format version. The version 0 has the following per-value format:

```
|  u16    | var |    u32    |  var  |
|---------|-----|-----------|-------|
| key_len | key | value_len | value |
|---------|-----|-----------|-------|
```

The `format_version` will be bumped to 1 and will be modified to have the following schema:

```
|  u16    | var | var  |    u32    |  var  |
|---------|-----|------|-----------|-------|
| key_len | key | attr | value_len | value |
|---------|-----|------|-----------|-------|
```

The newly introduced `attr` field will be decoded using the `row_features` array specified for
this SST. Which row attributes are included depend on the data that is being written. If
any row in the SST has a `ttl`, the `TimeToLive` feature will be enabled.

As part of this proposal, we will no longer represent tombstones as `INT_MAX` value_len and instead
have an indicator `byte` in the attribute array which specifies the `RowFlags`. `RowFlags` is a
bitmask of descriptors on the row. The first bit in `RowFlags` is set to `1` if the row is a
tombstone. Other bits are reserved for future flags. Later we can add other types of row attributes
as necessary (since flatbuffers does not support single-bit values, a byte is the smallest amount of
data we can store here):

```
                |-- attr --|
|  u16    | var |   byte   |    u32    |  var  |
|---------|-----|----------|-----------|-------|
| key_len | key | row_flag | value_len | value |
|---------|-----|----------|-----------|-------|
```

In cases where both `Timestamp` and `TimeToLive` are enabled, the row will look like:

```
                |-------- attr --------------|
|  u16    | var |    byte  | i64 |    i64    |    u32    |  var  |
|---------|-----|----------------------------|-----------|-------|
| key_len | key | row_flag | ts  | expire_ts | value_len | value |
|---------|-----|----------|-----|-----------|-----------|-------|
```

Both `ts` and `expire_ts` are encoded as `i64` in milliseconds.

The metadata will be returned in the decoded `KeyValueDeletable` which will be modified:

```rust
pub struct KeyValueDeletable {
    pub key: Bytes,
    pub value: ValueDeletable,
    pub attributes: RowAttributes,
}

pub struct RowAttributes {
    pub ts: Option<i64>,
    pub expire_ts: Option<i64>,
}
```

### Compaction

#### TTL Compaction Filter

We will introduce the concept of a compaction filter, which serves as a mechanism for pluggable
participation in the compaction process. Specifically, the filter (if configured) will run on each
key during the compaction process and optionally remove or modify the value. This is independent
of the periodic compaction interval specified in the following section.

For the scope of this RFC, we will be introducing a builtin TS/TTL compaction filter. This filter
will add a tombstone to any value where `now > kv.attributes.expire_ts` (where `now` is the current
system time in milliseconds). Note that tombstones will still contain a timestamp (see
[insertion timestamps](#insertion-timestamps)). If a tombstone is added, the TTL (`expire_ts`) for
the row will be cleared, indicating that a future run of this filter should not remove the tombstone.

Since this RFC does not cover the public API for compaction filters, this filter will simply run
on any SST that specifies `row_features: [Timestamp && TimeToLive]`.

#### Periodic Compaction

If the SST is written with the `TimeToLive` row attribute enabled, the compaction scheduler will
compare the SST's `SsTableInfo#timestamp` with a newly introduced compaction config for periodic
compaction time limits:

```rust
use std::time::Duration;

pub struct CompactorOptions {
    // ...

    // When there are SSTs in L0 (or Sorted Runs with SSTs in other levels) older than 
    // `periodic_compaction_interval`, SlateDB will include them in the next scheduled
    // compaction -- this is a best effort interval and slateDB does not guarantee that
    // compaction will happen within this interval (implementations of the compaction scheduler
    // are free to implement jitters or other mechanisms to ensure that SSTs are not 
    // all compacted simultaneously)
    periodic_compaction_interval: Duration
}
```

This will ensure that Sorted Runs or SSTs that are not otherwise scheduled by the compaction
scheduler will be picked up and TTL / tombstones will be compacted. This is particularly relevant
for the lower levels of the LSM tree.

## Rejected Alternatives & Follow-Ups

### Insertion Timestamps

A previous version of this proposal introduced the concept of row level timestamps -- the ability
to set a timestamp at each insertion. This introduced the ability to have out-of-order insertions,
which in turn caused significant downstream complications.

In order to leave the door open for timestamps on insertion, we encode the clock time with each
row in the SST as opposed to only maintaining the time that it should expire. This comes at the
cost of an extra 4 bytes when cached in memory (compression will probably mean it is stored as
fewer bytes).

Another design consideration in the RFC that leaves the door open for insertion timestamps is the
association of a timestamp with tombstones. This is to handle the scenario where a record with
a TTL is inserted with a lower timestamp than an existing record. Consider the following:

```
seq0: Insert K1 with TS=100 and TTL=100000
seq1: Insert K1 with TS=50 and TTL=100
```

If `seq0` and `seq1` result in writes in different levels of the LSM tree, when a compaction kicks
in at TS=170 a tombstone is generated for `K1`. This tombstone should not result in a deletion of
the original `seq0` insert as it logically happened "after" `seq1`.

## Updates

### September 25, 2024

- Removed out-of-order insertions
- Removed the functionality to retrieve metadata of a row. Without multiple copies of the row the
  insertion time is less interesting. we can consider adding this functionality back in upon
  request.
- Added the requirement for the clock to be monotonically increasing
- Added a timestamp field to `ManifestV1` to seed clock
- Added a creation_timestamp field to `SsTableInfo` and `periodic_compaction_seconds` to the
  `CompactorOptions`
- Added a `RowType` metadata feature and used it to replace the current encoding of tombstones

### October 1, 2024

- Changed `RowType` to `RowFlags` and indicated that the first bit is used to identify tombstones
- Added `min_ts`/`max_ts` into `SsTableInfo`
- Updated the seeding of the clock min timestamp to look at the WAL SSTs as well as the manifest
- Modified the stored value in the SST from `ttl` to `expire_ts`
- Marked `periodic_compaction_interval` as "best effort" to allow for implementations to optimize
  performance characteristics such as adding a jitter or max number of SSTs to compact in one go
- Many misc. improvements to readability and clarifications

### October 6, 2024

- Minor changes and feedback. Most notable is renaming "meta features" to "row attributes"

### January 20, 2026

- **Removed the `LogicalClock` trait and user-defined clock configuration.** The original design
  allowed users to supply a custom clock implementation for TTL timestamps. This was removed because:
  1. The custom clock functionality added complexity that introduced bugs
  2. With the introduction of transactions (see RFC 0011), sequence numbers can solve many of the
     same use cases that user-defined clocks were intended to address (e.g., ordering operations)
- SlateDB now uses the system clock directly for TTL expiration timestamps. The `SystemClock` trait
  (from `slatedb-common`) returns `DateTime<Utc>`, and SlateDB extracts milliseconds since epoch
  internally. A custom `SystemClock` can still be provided via `Db::builder().with_system_clock()`
  for testing or non-standard time sources.
- Updated `DbOptions` to remove the `clock` configuration option
- Updated `WriteOptions` to `PutOptions` with a `Ttl` enum that supports `Default`, `NoExpiry`,
  and `ExpireAfter(u64)` variants
