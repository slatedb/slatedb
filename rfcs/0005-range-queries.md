# SlateDB Range Scans

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Motivation](#motivation)
- [Goals](#goals)
- [Public API](#public-api)
- [Implementation](#implementation)
   * [Checkpoint References](#checkpoint-references)
   * [Memory Usage](#memory-usage)
- [Rejected Alternatives & Follow-Ups](#rejected-alternatives-follow-ups)
- [Updates](#updates)

<!-- TOC end -->

Status: Discussion

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

References:

* https://github.com/slatedb/slatedb/issues/8
* https://github.com/facebook/rocksdb/wiki/Iterator
* https://github.com/facebook/rocksdb/wiki/Iterator-Implementation
* https://github.com/slatedb/slatedb/pull/260
* https://github.com/slatedb/slatedb/blob/main/rfcs/0004-checkpoints.md

## Motivation


The ability to scan the keys in a range is an essential feature for building applications.
For example, this can be used to implement a collections of keys using a common prefix. 
Currently, SlateDB only supports fetching individual values for known keys.

## Goals

The goal of this work is to provide a flexible capability to query a range of keys in the
database. It should be possible to specify unbounded ranges to allow querying all keys,
or bounded ranges for finer precision and efficiency.

Range queries have the potential to be expensive from a resource perspective because they
hold references to existing tables, which prevents garbage collection. Our goal here is
to provide a simple initial approach to state management which optimizes for common cases,
while leaving room for further optimization in the future.

## Public API

We introduce a new `Db::scan` API which accept a `DbRange` defining the range of the scan.
The result will be a `DbIterator` object which iterates the current records within the range.
Users may either provide an explicit starting range in the call to `scan`, or they
may `seek` using the returned iterator.

```rust
impl Db {
    /// Scan a range of keys.
    /// 
    /// returns a `DbIterator`
    pub async fn scan<T>(
        &self,
        range: T
    ) -> Result<DbIterator, SlateDbError> where
        T: RangeBounds<Bytes> 
    {
	    ...
    }
	
}

impl DbIterator {
    /// Get the next record in the scan.
    /// 
    /// returns Ok(None) when the scan is complete
    /// returns Err(InvalidatedIterator) if the iterator has been invalidated
    ///  due to an underlying error
    pub async fn next(
        &mut self,
    ) -> Result<Option<DbRecord>, SlateDbError> {
        ...
    }

    /// Seek to a key ahead of the last key returned from the iterator or
    /// the lower range bound if no records have yet been returned. 
    /// 
    /// returns Ok(()) if the position is successfully advanced
    /// returns SlateDbError::InvalidArgument if `lower_bound` is `Unbounded`
    /// returns SlateDbError::InvalidArgument if the key is comes before the
    ///  current iterator position
    /// returns SlateDbError::InvalidArgument if `lower_bound` is beyond the
    ///  upper bound specified in the original `scan` parameters
    /// returns Err(InvalidatedIterator) if the iterator has been invalidated
    ///  in order to reclaim resources
    pub async fn seek(
        &mut self,
        lower_bound: Bound<&[u8]>
    ) -> Result<(), SlateDbError> {
        ...
    }	

}

struct DbRecord {
    key: Bytes,
    value: Bytes,
}
```

Note that the `DbIterator` may be invalidated internally if any of its referenced
resources must be reclaimed. In this case, a call to `DbIterator::next` or 
`DbIterator::scan` will return `SlateDb::InvalidatedError`. This is discussed in 
more detail below.

We will also provide `Db::scan_with_options`, which allows the user to specify the
query isolation through a `ScanOptions` struct. Similar to `get_with_options`, the
two isolation levels that will be supported are `Committed` and `Uncommitted`.
The default when `scan` is called without explicit options will be `Committed`.

```rust
impl Db {
    /// Scan a range of keys with the provided options.
    ///
    /// returns a `DbIterator`
    pub async fn scan_with_options<T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDbError> where
        T: RangeBounds<Bytes> 
    {
	    ...
    }
}

pub struct ScanOptions {
    /// The read commit level for read operations
    pub read_level: ReadLevel,
    /// The number of bytes to read ahead
    pub read_ahead_size: usize,
    /// Whether or not fetched blocks should be cached
    pub cache_blocks: bool
}
```

## Implementation

A scan must iterate every table which may have keys within the query range.
This includes tables in memory which which have not been flushed and L0/SR tables 
which may need to be loaded from object storage. Internally, we will rely on 
iterators which sort and merge these tables. The latest records will have precedence following
the same rules that a normal `get` query follows.

We will rely on the latest checkpoint state in order to obtain references to memtables, SSTs,
and sorted runs needed for the query. The `DbIterator` will expose the UUID of the checkpoint
that it was generated from. The `DbIterator` will only maintain references to
tables which cover the range specified in the `scan` parameters.

Note that if the `DbReader` was created with a reference to a specific checkpoint,
then the `DbIterator` will only refer to that checkpoint.

The existing `MergeIterator` used for compaction provides much of the necessary
foundation to unify multiple iterators. We will build an iterator which sorts
and merges memtables (e.g. with `MemTableIterator`), L0 tables (`SstIterator`),
and sorted runs (`SortedRunIterator`). 

**Query Isolation**: When using the `Uncommitted` isolation, unflushed
records recorded in WALs will be used as a source for the key range. Otherwise,
only committed records in memtables, as well as L0 and SR tables will be used.

Range queries must access mutable table state, such as the current WAL. It is 
therefore possible for the query results to include some records which 
were written after the query began scanning its range of records. For example,
a query may include only a subset of records that were inserted in the same 
`WriteBatch`. In other words, range queries will not have snapshot isolation 
initially. 

When a `Db` or `DbReader` is closed, active iterators will be invalidated. This
will cause calls to `DbIterator::next` or `DbIterator::seek` to fail with an
`InvalidatedIterator` error which indicates that the database was shutdown.

[RFC-????](https://github.com/slatedb/slatedb/pull/260) proposes transactional
semantics for SlateDB. This relies on a monotonic sequence number which is tagged
on each key update. Range queries could leverage this sequence number to improve
isolation by filtering any updates with sequence numbers larger than the latest
at the time the query was executed. We leave this as a gap for future work to address.

### Checkpoint References

We intend to rely on state checkpoints introduced in [RFC-0004(https://github.com/slatedb/slatedb/blob/main/rfcs/0004-checkpoints.md)
in order to prevent the cleanup of durable state referenced by the query. When a range
scan is created, it will create an internal reference to the current checkpoint. This
reference will prevent cleanup of the checkpoint state until the scan is complete. 

Checkpoints created by `DbReaders` have an expiry time, which is only refreshed as long
as long as the checkpoint remains current. If a `DbIterator` instance remains active long 
enough for its referenced checkpoint to expire, then the `DbIterator` may eventually encounter
a NotFound error from the object store. This error will be propagated to the next
invocation of `DbIterator::next` or `DbIterator::seek`. After encountering this error,
the iterator will be invalidated. Invalidated iterators will return 
`SlateDbError::InvalidatedIterator` on any subsequent call to `DbIterator::next`
or `DbIterator::seek`.

In general, we make the initial simplifying assumption that scans will not be frequent
enough or last long enough for checkpoint accumulation to be a problem. In the future,
we could either limit the number of checkpoint references that are available for
scanning or we could limit the time that a `DbIterator` will remain valid.

### Memory Usage

In addition to the checkpoint, a range scan may hold references to records in unflushed 
WALs or memtables. A potential optimization is to pre-merge these tables in order to
filter the records within the scan range. This can be useful when targeting a small
range of keys with the scan. This is left for future work.

Iterators will use prefetching to improve performance. Internally, this is 
already implemented by `SstIterator`. Users can influence prefetching to improve
performance using `ScanOptions::read_ahead_size`, which indicates the number of bytes
to prefetch. Internally, this is used to set `SstIterator::blocks_to_fetch`. The 
default `read_ahead_size` will be the size of a single block.

Users can also influence caching behavior using `ScanOptions::cache_blocks`. Internally,
this will be used to set `SstIterator::cache_blocks`. The default behavior will be
to use `cache_blocks=true`. Note that while an iterator is actively scanning from a given
block, it will remain pinned in memory.

## Rejected Alternatives & Follow-Ups

**Improved isolation:** Transactional semantics proposed in 
[RFC-????](https://github.com/slatedb/slatedb/pull/260) will provide the foundation 
to support snapshot isolation or serializability of range scans. We anticipate 
future work to address the existing gaps.

**Space optimizations:**

- Rather than retaining references to in-memory tables for the full duration
of the query, we can filter the set of records within the query's range.
As new records are returned, previous ones fall out of scope and may be cleaned up. This
makes sense for bounded queries where the number of matching keys within the WAL
or memtables may be low. For an unbounded query, we may prefer to hold a reference to the
full table.

- While a range scan is in progress, some of its in-memory 
table references may get flushed to storage. It would be possible to substitute 
these references in order to allow the in-memory state to be cleaned up. This 
might make sense when the rate of updates is high enough that memory must be 
aggressively reclaimed.

**Resource Limits:** As mentioned above, it is possible to impose limits on
active range scans in order to ensure that they do not exhaust memory or create
unnecessary garbage accumulation.

**Persistent Queries** It may be useful in some use cases to persist query state
in order to allow a process to resume after a migration or restart. This may
require exposing the checkpoint that the query results were fetched from through
the `DbIterator`.


## Updates

