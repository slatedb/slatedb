# SlateDB Timestamps & Time-To-Live

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Motivation](#motivation)
- [Goals](#goals)
- [Out of Scope](#out-of-scope)
- [Public API](#public-api)
  * [Configuration](#configuration)
  * [Gets](#slatedbget)
- [Implementation](#implementation)
  * [Data Format](#data-format)
  * [Compaction](#compaction)

<!-- TOC end -->

Status: In Discussion

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
2. **Resolving Out-of-Order Operations**: some applications may process incoming requests
   in a manner that results in out-of-order insertions to SlateDB (stream processing applications
   are frequently subject to this type of pattern). Using insertion timestamps to resolve which
   version of a key is more recent may be preferable to using the clock at the time of insertion.
3. **Historical Reads / MVCC**: while SlateDB provides snapshot capabilities to ensure reads are
   consistent and repeatable, it requires a snapshot to have been taken at the desired time. 
   increasing the number of snapshots adds significant storage overhead. Instead, it is possible
   to trade read-latency for space amplification by maintaining multiple versions of keys at
   different timestamps to allow for similar "historical" read operations.

## Goals

- Define the API for specifying timestamps
- Define the changes to the on-disk format of stored rows
- Define the default semantics (and enumerate configurations) for out-of-order insertions
- Outline mechanism to support Time-To-Live (TTL) via Compaction Filters

## Out of Scope

While the following are out of scope for the initial implementation, the design should
not preclude:

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

### Configuration

We will make the following changes to `DbOptions`:
- introduce a configuration for a custom clock, and will default to using the system clock
- introduce a configuration for a default TTL, which defaults to -1

```rust
/// defines the clock that SlateDB will use during this session
pub trait Clock {
    fn now(&self) -> i64;
}

/// contains the default implementation of the Clock, and will return the system time
pub struct SystemClock;

// ...

pub struct DbOptions {
    // ...

    /// the Clock to use for insertion timestamps
    ///
    /// default: the default clock uses the local system time on the machine
    clock: Option<Arc<dyn Clock>>,

    /// the default time-to-live (TTL) for insertions (note that re-inserting a key
    /// with any value will update the TTL to use the default_ttl)
    ///
    /// default: no TTL (insertions will remain until deleted)
    default_ttl: Option<Duration>
}
```

`Db#put_put_with_options` can optionally specify a timestamp and a row-level TTL at insertion time 
by leveraging newly added fields in `WriteOptions`:

```rust
pub struct WriteOptions {
    // ...
   
    /// the timestamp (in millis) at which this operation is considered to be executed - 
    /// note that SlateDB does not automatically resolve out-of-order insertions
    /// (if you need to retrieve the most recent semantic insertion, specify the
    /// option on ReadOptions)
    ///
    /// default: uses the clock time configured in the DbOptions
    pub timestamp_ms: Option<i64>,
   
    /// the time-to-live (ttl) for this insertion. If this insert overwrites an existing
    /// database entry, the TTL for the most recent entry will be canonical.
    ///
    /// default: the TTL configured in DbOptions when opening a SlateDB session
    pub ttl: Option<Duration>,
}
```

`Db#get_with_options` can optionally specify: a grace period with which to continue searching the
LSM tree for a more recent insertion. This allows you to specify an upper bound for what you expect
out-of-order insertions to be.

```rust
pub struct ReadOptions {
    // ...

    /// if set to a positive number, SlateDB will not return the first matching row for
    /// each key lookup and instead will continue to traverse the LSM tree until the next
    /// matching key is older than the found key's timestamp + retrieval_grace_ms. Logically,
    /// this is an "upper bound" on the magnitude of out-of-order insertions.
    ///
    /// Example: if there are three insertions [KeyA@100, KeyA@50, KeyA@150] that are each
    /// in separate layers of the LSM tree, you will see the following behavior:
    /// - if retrieval_grace_ms is <=0, KeyA@100 will be returned, and only the first SST
    ///   will be scanned
    /// - if retrieval_grace_ms is <=50, KeyA@100 will be returned after having scanned
    ///   the SST that contains KeyA@50
    /// - if retrieval_grace_ms is > 50 and <= 100, KeyA@150 will be returned after having
    ///   scanned the SST containing KeyA@150
    /// - otherwise, KeyA@150 will be returned after scanning all levels of the LSM tree
    ///
    /// default: -1 (the first matching key will be retrieved)
    pub retrieval_grace_ms: i64,
}
```

**Callout for TTL**: I have decided that for this RFC we will not introduce the ability to disable
filtering out TTL-expired records on retrieval. The default implementation will read the metadata
for every retrieved row and filter out records where `ts + tll < now`. We can, as a future
optimization, enable lazy deserialization of the metadata so that this doesn't need to happen on
each row retrieved. This can be part of the API design for compaction filters (
see https://github.com/slatedb/slatedb/issues/225)

**Alternative**: if this retrieval grace concept is too complicated, we can also consider specifying
a "base timestamp", that will retrieve the first result >= the configured base timestamp, or the 
most recent one if none are greater than or equal to the base timestamp. This is significantly less
flexible, however, as many rows may not be updated in a long time (though most of those rows will be
in the base layer of the LSM tree anyway, so it may not be so bad).

### SlateDB::Get

SlateDB currently returns a `Option<Bytes>` on a successful read operation. Now that additional
metadata (timestamps in this RFC) are introduced, we will need to expand this API to include
more information in the return value. We have a few options here (TODO: update this section when
we converge on the approach we want):

1. We can return a strongly typed result (such as `Option<Row>` that is a struct with the result as
   well as any additional retrieved metadata)
2. We can introduce a `db#get_with_metadata` that will return a tuple of `Bytes` and corresponding
   metadata
3. Do nothing (make the timestamp opaque to the user and require them to interact with timestamps
   only via the public API)

## Implementation

### Data Format

Currently, SlateDB does not have a mechanism for backwards-compatibility of the SST data format.
We will first introduce the following two fields to the flatbuffer definitions: 
- `format_version`: field that specifies the codec version that encoded this SST
- `meta_features`: an enumeration of metadata fields that are available with each encoded row. This
  makes it possible in the future to save space per row by disabling metadata features, but that is
  out of scope for this RFC

```fbs
enum MetaFeatures: byte {
    Timestamp,
    TimeToLive,
}

table SsTableInfo {
    // ...
    
    // the current encoding version of the data block
    format_version: uint;
    
    // the metadata features that are encoded with each row, in order that they are encoded
    meta_features: [MetaFeatures];
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
| key_len | key | meta | value_len | value |
|---------|-----|------|-----------|-------|
```

The newly introduce `meta` field will be decoded using the `meta_features` array specified for
this SST. In cases where both `Timestamp` and `TimeToLive` are enabled, the row will look like:

```
|  u16    | var | i64 | i64 |    u32    |  var  |
|---------|-----|-----|-----|-----------|-------|
| key_len | key | ts  | ttl | value_len | value |
|---------|-----|-----|-----|-----------|-------|
```

Both `ts` and `ttl` are encoded as `i64` in milliseconds. The expiry time is `ts + ttl`.

The metadata will be returned in the decoded `KeyValueDeletable` which will be modified:

```rust
pub struct KeyValueDeletable {
    pub key: Bytes,
    pub value: ValueDeletable,
    pub meta: KeyValueMeta,
}

pub struct KeyValueMeta {
    pub ts: Option<i64>,
    pub ttl: Option<i64>,
}
```

### Compaction

We will introduce the concept of a compaction filter, which serves as a mechanism for pluggable
participation in the compaction process. Specifically, the filter (if configured) will run on each
key during the compaction process and optionally remove or modify the value.

For the scope of this RFC, we will be introducing a builtin TS/TTL compaction filter. This filter
will add a tombstone to any value where `db.clock.now() > kv.meta.ts + kv.meta.ttl`.

Since this RFC does not cover the public API for compaction filters, this filter will simply run
on any SST that specifies `meta_features: [Timestamp | TimeToLive]`.

There are various interesting interleaving scenarios with timestamps and TTL, outlined below:

1. **(Happy Path)** KeyA is inserted at ts=100 with ttl=100. It will be retrievable (not filtered)
   until ts=200 and will be replaced with a tombstone when compaction runs at ts>=200.
2. **(In Order Overwrite, Newer TTL)** KeyA is inserted at ts=100 with ttl=100, then again at ts=150
   with ttl=100. Any gets called after ts=150 will return the new value, and it will be accessible
   until ts=250.
3. **(In Order Overwrite, Older TTL)** KeyA is inserted at ts=100 with ttl=100, then again at ts=110
   with ttl=40. Any gets called after ts=150 will not return any data (considered expired). If
   compaction happens at ts=160, it will add a tombstone and propagate it down to overwrite the
   original insert.
4. **(Out of Order Overwrites)** KeyA is inserted at ts=100, then again at ts=50. Retrieval
   information is outlined in the previous section depending on the `retrieval_grace_ms` configured
   in the read options. Compaction will prefer the row with the larger timestamp. To guarantee
   consistent time semantics, it is necessary to specify a sufficiently large value for the grace
   period.
5. **(Out of Order Tombstones)** KeyA is inserted at ts=100, then a delete for KeyA at ts=50 is
   executed. The tombstone will be associated with ts=50, and therefore when the compactor merges
   the tombstone and the existing key the tombstone will be ignored.