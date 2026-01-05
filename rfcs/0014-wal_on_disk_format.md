# Persistence format for the write-ahead log (WAL)


Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
- [Impact Analysis](#impact-analysis)
   * [Core API & Query Semantics](#core-api-query-semantics)
   * [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   * [Time, Retention, and Derived State](#time-retention-and-derived-state)
   * [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   * [Compaction](#compaction)
   * [Storage Engine Internals](#storage-engine-internals)
   * [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
   * [Performance & Cost](#performance-cost)
   * [Observability](#observability)
   * [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Bruno Cadonna](https://github.com/cadonna)

<!-- TOC --><a name="summary"></a>
## Summary

This RFC proposes a dedicated persistence format for the write-ahead log (WAL). 
The new format tries to minimize the overhead when adding incoming records to WAL and 
flushing the buffered WAL to object store.
Tightly bound to the new persistence format is an in-memory data structure for batching and flushing WAL records.
This RFC only specifies the persistence format and some desirable properties of the in-memory data structure. 
No exact implementation of the in-memory data structure is provided, so that the in-memory structure can be 
modified without the need of new RFC.

<!-- Briefly explain what this RFC changes and why. Prefer 3–6 sentences. -->

<!-- TOC --><a name="motivation"></a>
## Motivation

Currently, the WAL is buffered in memory in a `KVTable`. 
That is the same data structure that is used for the memtable. 
The `KVTable` is a skip map -- an ordered map based on a skip list -- that keeps all incoming key-value pairs ordered by key. 
While this is beneficial for the memtable for reading and flushing, it adds unnecessary overhead for the WAL.
The WAL merely needs to maintain records in the order they are ingested to ensure durability and recovery.

In object storage, the WAL is stored as a sorted string table (SST). 
Encoding the SST from a `KVTable` also implies overhead that is unnecessary for the WAL. 
For example, filters and indices are not needed for a WAL object that stores records in the same order they were 
ingested and also reads those records sequentially in that order.

Separating the format of the WAL from the format of the SST allows the WAL format to model sequential writes in 
lock-step with the monotonically increasing sequence number. 
Sequence numbers are is assigned to each ingested record to specify the ingestion order.
Having a WAL format model sequential writes according to the ingestion order is an advantage 
when records need to be read in the same order they were ingested.
This kind of sequential read is the main read pattern of a write-ahead *log* by definition. 
The most prominent example for this sequential read pattern is the recovery of a database state. 
With the current SST format that is also used to store WAL objects, 
the records are sorted by key not by the sequence number making sequential reads by sequence number costly.

For these reasons, the WAL can be implemented with a first-in first-out (FIFO) data structure in memory and with a persistence 
format that sequentially stores and loads records without any specific indices or filters.

The overhead of using the skip map and the SST format for the WAL can be seen in the two flamegraphs included in 
GitHub issue [#1085](https://github.com/slatedb/slatedb/issues/1085). The overhead hinders higher ingest throughput 
into slateDB.

<!-- What problem are we solving? What user or system pain exists today? Include concrete examples and why “do nothing” is insufficient. -->

<!-- TOC --><a name="goals"></a>
## Goals

- Specify a queue-like format for WAL objects that can be flushed with minimal overhead.

<!-- TOC --><a name="non-goals"></a>
## Non-Goals

- Specify the implementation of the in-memory data structure. 
Merely, some desirable properties will be mentioned.  

<!-- TOC --><a name="design"></a>
## Design

The proposed new persistence format for the WAL objects starts with a list of variable-length records.

A record consists of:
- a sequence number represented by an 8-bytes unsigned integer,
- 8 flags in a 1-byte unsigned integer,
- optional expiration timestamp and creation timestamp of the record each represented by 8 bytes signed integer,
- key length as a 2-bytes unsigned integer followed by the number of bytes set in the key length containing the actual key,
- value length as a 4-bytes unsigned integer followed by the number of bytes set in the value length containing the actual value.

All the integers are in little endian.
The flags specify the type of the record and whether the records contain a expiration and/or creation timestamp.
The timestamps are milliseconds since the unix epoch.

Record:
```
+----------------------------------------------------------------+
| sequence number (8-bytes unsigned integer, little endian)      |
+----------------------------------------------------------------+
| flags (1-byte unsigned integer, little endian)                 |
+----------------------------------------------------------------+
| expire_ts (8-bytes signed integer, little endian)              |
+----------------------------------------------------------------+
| create_ts (8-bytes signed integer, little endian)              |
+----------------------------------------------------------------+
| key length (2-bytes unsigned integer, little endian)           |
+----------------------------------------------------------------+
| key (variable length)                                          |
+----------------------------------------------------------------+
| value length (4-bytes unsigned integer, little endian)         |
+----------------------------------------------------------------+
| value (variable length)                                        |
+----------------------------------------------------------------+
```

Flags:
```
b_0, b_1 = (0, 0) if the record is a value,
           (0,1) if the record is a tombstone,
           (1,0) if the record is a merge operand,
           (1, 1) free
b_2 = 1 if the record has an expiration timestamp, 0 otherwise
b_3 = 1 if the record has a creation timestamp, 0 otherwise
b_4 - b_7 = 0 (free)
```

If the record is a tombstone, the value length and the actual values are omitted.

The list of records is followed by a list of record sizes represented by 4-bytes unsigned integers in little endian.
The record sizes have the same order as the records. That is, the first record size is the size of the first record
in the object, the second record size is the size of the second record in the object, and so on.
After, the record sizes the format contains the number of records in the WAL object as a 4 bytes unsigned integer in little endian.
Finally, the last 2 bytes contain the version of the format as an unsigned integer in little endian.

```
+----------------------------------------------------------------+
| record 0 (variable length)                                     |
+----------------------------------------------------------------+
| record 1 (variable length)                                     |
+----------------------------------------------------------------+
| ...                                                            |
+----------------------------------------------------------------+
| record N (variable length)                                     |
+----------------------------------------------------------------+
| size of record 0 (4-bytes unsigned integer, little endian)     |
+----------------------------------------------------------------+
| size of record 1 (4-bytes unsigned integer, little endian)     |
+----------------------------------------------------------------+
| ...                                                            |
+----------------------------------------------------------------+
| size of record N (4-bytes unsigned integer, little endian)     |
+----------------------------------------------------------------+
| number of records N (4-bytes unsigned integer, little endian)  |
+----------------------------------------------------------------+
| version of format (2-bytes, unsigned integer, little endian)   |
+----------------------------------------------------------------+
```

Ideally, the in-memory data structure for the WAL stores the incoming records in ingestion order, so that the records do not need
to be re-ordered before the flush. A simple FIFO data structure like a queue is recommended.

<!-- TOC --><a name="impact-analysis"></a>
## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

<!-- TOC --><a name="core-api-query-semantics"></a>
### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

<!-- TOC --><a name="consistency-isolation-and-multi-versioning"></a>
### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

<!-- TOC --><a name="time-retention-and-derived-state"></a>
### Time, Retention, and Derived State

- [ ] Logical clocks
- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

<!-- TOC --><a name="metadata-coordination-and-lifecycles"></a>
### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

<!-- TOC --><a name="compaction"></a>
### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

<!-- TOC --><a name="storage-engine-internals"></a>
### Storage Engine Internals

- [X] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

<!-- TOC --><a name="ecosystem-operations"></a>
### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

<!-- TOC --><a name="operations"></a>
## Operations

<!-- TOC --><a name="performance-cost"></a>
### Performance & Cost

<!-- Describe performance and cost implications of this change. -->

- **Latency (reads/writes/compactions):** 
The latency for writes with durability guarantee mainly depends on the configured size of the encoded WAL object 
(see `max_wal_bytes_size`) that is used to decide when the in-memory WAL data structure is flushed to object storage.
Since the new persistence format can batch more records than the currently used SST format given the size of the 
encoded WAL object, the latency might be higher.
The latency for writes without durability guarantee might be lower if the in-memory data structure is a simple FIFO
data structure as recommended in this RFC, since writes to the in-memory WAL are faster.
A WAL object is read for recovery.
If we define the latency of recovery as the time until the in-memory state is re-established then the new persistence 
format might decrease the latency of recovery because more records can be read from object storage given an encoded size 
of the WAL object.
The latency of compaction is not affected by the new persistence format.
- **Throughput (reads/writes/compactions):**
The throughput of writes with durability guarantee might be higher since the new persistence format has less space 
overhead without indices and filters. 
Thus, with the new format slateDB can write more WAL records per time unit to object storage compared to the current 
format given the same encoded size of the WAL object.
Additionally, the new persistence format allows to recover more records per time unit.
The throughput of compaction is not affected by the new persistence format.
- **Object-store request (GET/LIST/PUT) and cost profile:**
The new persistence format allows to store more records per PUT to and to get more records per GET from object storage. 
- **Space, read, and write amplification:**
All three amplifications are reduced. 

<!-- TOC --><a name="observability"></a>
### Observability

<!-- Describe any operational changes required to support this change. -->

- Configuration changes: 
	- introduce configuration `wal_flush_size: usize` to specify what encoded size in bytes of the WAL triggers a flush to object storage. 
	The limit is not strict. The flush is triggered as soon as a write to the WAL exceeds the limit.  
	- min value is `0`: the WAL is flushed at each write
	- max value is `4 GiB` (i.e., `4,294,967,295` bytes)
- New components/services: No
- Metrics: No
- Logging: No

<!-- TOC --><a name="compatibility"></a>
### Compatibility

<!-- Describe compatibility considerations with existing versions of SlateDB. -->

- Existing data on object storage / on-disk formats
	- To be determined after agreement on the format
- Existing public APIs (including bindings): No
- Rolling upgrades / mixed-version behavior (if applicable)
	- To be determined after agreement on the format

<!-- TOC --><a name="testing"></a>
## Testing

<!-- Describe the testing plan for this change. -->

- Unit tests: 
	- Adapt unit existing unit tests and add new unit tests where needed.
- Integration tests: 
	- Adapt existing unit tests if needed,
	- Write integration tests for migration from the old to the new persistence format  
- Fault-injection/chaos tests: No
- Deterministic simulation tests: No
- Formal methods verification: No
- Performance tests: Yes, to proof the improvements over the flamgraphs in [#1085](https://github.com/slatedb/slatedb/issues/1085)

<!-- TOC --><a name="rollout"></a>
## Rollout

<!-- Describe the plan for rolling out this change to production. -->

- Milestones / phases:
- Feature flags / opt-in:
- Docs updates:

<!-- TOC --><a name="alternatives"></a>
## Alternatives

List the serious alternatives and why they were rejected (including “status quo”). Include trade-offs and risks.

<!-- TOC --><a name="open-questions"></a>
## Open Questions

- Should the WAL be compressed when flushed? Or at least should compression be configurable?
- Migration from the SST format to the new format is still to be determined.

<!-- TOC --><a name="references"></a>
## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- GitHub issue [#1085](https://github.com/slatedb/slatedb/issues/1085)

<!-- TOC --><a name="updates"></a>
## Updates

Log major changes to this RFC over time (optional).
