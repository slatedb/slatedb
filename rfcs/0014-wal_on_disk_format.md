# Persistence format for the write-ahead log (WAL)


Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [Format](#format)
   * [Serialization](#serialization)
   * [Deserialization](#deserialization)
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

Status: **Rejected**

In the discussion, we realized that having blocks that can be loaded separately makes also sense for the
WAL.
The separately loadable blocks need indices in the file format that point to the blocks.
If we add blocks and indices to a new WAL format, the new format would be really similar to the
existing SST format since also the new WAL format would need versioning and metadata similar to the
one in the existing SST format.
The only component of the existing SST format that is not needed for the WAL format is the filter.
Not encoding the filter is allowed in the existing SST format when there are fewer keys in the SST
than set in config `min_filter_keys`.
Thus, not encoding filters for the WAL would also not justify a new format.
Additionally, it is not impossible that filters for WAL files might be interesting for future use cases.
Given all of these arguments, we decided to reject this RFC about a new persistence format for the WAL
and re-use the existing SST format instead.


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
Sequence numbers are assigned to each ingested record according to the write snapshot they belong to.
Having a WAL format model sequential writes is an advantage when records need to be read in the same order they were ingested.
This kind of sequential read is the main read pattern of a write-ahead *log* by definition.
The most prominent example for this sequential read pattern is the recovery of a database state.
With the current SST format that is also used to store WAL objects,
the records are sorted by key not by the sequence number making sequential reads by sequence number costly.

For these reasons, the WAL can be implemented with a first-in first-out (FIFO) data structure in memory and with a persistence
format that sequentially stores and loads records without any specific indices or filters.

The overhead of using the skip map and the SST format for the WAL can be seen in the two flamegraphs included in
GitHub issue [#1085](https://github.com/slatedb/slatedb/issues/1085).
The overhead hinders higher ingest throughput into SlateDB.

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

<!-- TOC --><a name="format"></a>
### Format

The proposed new persistence format for the WAL objects starts with a list of variable-length records.

A record consists of:
- a sequence number represented by an 8-bytes unsigned integer,
- 8 flags in a 1-byte unsigned integer,
- optional the creation timestamp and the expiration timestamp of the record, each represented by 8 bytes signed integer,
- key length as a 2-bytes unsigned integer followed by the number of bytes set in the key length containing the actual key,
- value length as a 4-bytes unsigned integer followed by the number of bytes set in the value length containing the actual value.

All the integers are in little endian.
The flags specify the type of the record and whether the records contain the expiration and/or creation timestamp.
The timestamps are milliseconds since the unix epoch.

Record:
```
+----------------------------------------------------------------+
| sequence number (8-bytes unsigned integer, little endian)      |
+----------------------------------------------------------------+
| flags (1-byte unsigned integer, little endian)                 |
+----------------------------------------------------------------+
| create_ts (8-bytes signed integer, little endian)              |
+----------------------------------------------------------------+
| expire_ts (8-bytes signed integer, little endian)              |
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
           (0, 1) if the record is a tombstone,
           (1, 0) if the record is a merge operand,
           (1, 1) reserved
b_2 = 1 if the record has a creation timestamp, 0 otherwise
b_3 = 1 if the record has an expiration timestamp, 0 otherwise
b_4 - b_7 = 0 (reserved)
```

If the record is a tombstone, the value length and the actual values are omitted.

The list of records is followed by a compressed list of offsets,
where each offset points to one record.
Since the compressed list of offsets might be of variable length,
it is followed by the size of the compressed list as a 4 bytes unsigned integer.
After the size of the compressed list, the used compression codec is specified as a 1 byte unsigned integer.
Initially, the available compression codecs are no compression and a delta encoding that
still needs to be determined/specified.
The record offsets in the list have the same order as the records.
That is, the 0th offset points to the 0th record,
the 1st offset points to the 1st record in the record list, and so on.
Then, the format stores the CRC32 checksum of the records and the list of offsets
including the size and the compression codecs as a 4 bytes unsigned integer.
The specification of the compression codec for the offset list is followed by the specification of the
compression codec used to compress records and offsets as a 1 byte unsigned integer.
All the fields from the start to the compression codec for the offset list, inclusive
are compressed with that compression codec.
The available compression codecs for records and offsets are no compression, snappy, zlib, lz4, and zstd.
The last two fields are the version of the format as a 2 bytes unsigned integer,
and the type of the object as a 1 byte unsigned integer -- 0x00 for `WAL` in this case.
Future formats of data objects in SlateDB should use the same schema at the end of the
footer to specify its type and the version of its format.
All integers are in little endian.

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
| compressed array of N record offsets                           |
| (variable length,                                              |
|  before compression each offset is                             |
|  8-bytes unsigned integer, little endian)                      |
+----------------------------------------------------------------+
| size of the compressed array of offsets                        |
| (4-bytes unsigned integer, little endian)                      |
+----------------------------------------------------------------+
| compression codec offsets                                       |
| (1-byte unsigned integer, little endian)                       |
+----------------------------------------------------------------+
| CRC32 checksum (4-bytes, unsigned integer, little endian)      |
+----------------------------------------------------------------+
| compression codec for offsets + records + checksum             |
| (1-byte, unsigned integer, little endian)                      |
+----------------------------------------------------------------+
| version of format (2-bytes, unsigned integer, little endian)   |
+----------------------------------------------------------------+
| type of object (1-byte, unsigned integer, little endian)       |
+----------------------------------------------------------------+
```

Ideally, the in-memory data structure for the WAL stores the incoming records in ingestion order, so that the records do not need
to be re-ordered before the flush. A simple FIFO data structure like a queue is recommended.

<!-- TOC --><a name="serialization"></a>
### Serialization

Records are serialized into the record format.
All needed data to serialize the records into the format is present in the records.
The serialized bytes of the records are added one after the other in ingestion order into the WAL object.
For each serialized record, the offset from the beginning of the WAL object to the start of the serialized
record is maintained in a list.
Once all serialized records that should be stored in the WAL object are added,
the list of offsets is compressed with a codec and the compressed list is added to the WAL object.
Then the size of the compressed list is added to the WAL object
followed by the identifier of the used compression codec.
A CRC32 checksum is computed over the records, the offsets, the size of the offsets, as well as the compression codec
and the checksum is added to the WAL object.
Finally, the version of the format (i.e., `1`) and the type of the object (i.e., `0x00`) are added to the WAL object.

<!-- TOC --><a name="deserialization"></a>
### Deserialization

The WAL object is read from the end.
First, the type of the object and the version are deserialized and verified to know whether the object is indeed a
WAL object and with what version the WAL object was serialized.
Then, the CRC checksum is verified that the WAL object is valid.
Next, the compression codec identifier for the list of offsets is deserialized.
After that, the size of the compressed list of offsets is deserialized.
The size is subtracted from the current offset of the WAL object to find the offset of the WAL object
where the compressed list starts.
The bytes from the starting point to the field that holds the size are decompressed with the compression
codec found in the WAL object in the previous step.
Now, all information are available to sequentially read and deserialize all records.

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
The latency for writes with durability guarantee mainly depends on the configured flush interval
(i.e., `flush_interval`) that is used to decide when the in-memory WAL data structure is flushed to object storage.
The latency of writes might be less since fewer bytes need to be flushed to object store with the new WAL format.
However, if there is a difference in latency, I expect that the difference will not be significant, in general.
The latency for writes without durability guarantee might be lower if the in-memory data structure is a simple FIFO
data structure as recommended in this RFC, since writes to the in-memory WAL are faster.
A WAL object is read for recovery.
If we define the latency of recovery as the time until the in-memory state is re-established then the new persistence
format might decrease the latency of recovery because the records in the WAL do not need to be re-sorted according
to the sequence number.
The latency of compaction is not affected by the new persistence format.
- **Throughput (reads/writes/compactions):**
Since the new persistence format has less space overhead without indices and filters,
a WAL object can be sent faster to object storage for large WAL objects
(small object are dominated by the first-byte latency).
Thus, with the new format SlateDB can write more WAL records per time unit to object storage
compared to the current format.
The difference might not be significant, though.
Additionally, the new persistence format might allow to recover more records per time unit
since the records can be replayed faster (no re-sorting)
and fewer bytes need to read from object storage (no filter and index blocks).
The throughput of compaction is not affected by the new persistence format.
- **Object-store request (GET/LIST/PUT) and cost profile:**
If the WAL is stored on S3 Express to reduce latency, the new format reduces transfer costs
(i.e. data uploads, currently $0.0032 per GB).
- **Space, read, and write amplification:**
All three amplifications are reduced.

<!-- TOC --><a name="observability"></a>
### Observability

<!-- Describe any operational changes required to support this change. -->

- Configuration changes: No
- New components/services: No
- Metrics: No
- Logging: No

<!-- TOC --><a name="compatibility"></a>
### Compatibility

<!-- Describe compatibility considerations with existing versions of SlateDB. -->

- Existing data on object storage / on-disk formats
	- WAL objects in the old format are stored in the `\wal` directory and
      have the extension `.sst`.
    - WAL objects in the new format are also stored in the `\wal` directory,
      but have the extension `.wal`.
    - SlateDB decides which codec to use according to the extension of the WAL objects.
    - SlateDB will only write WAL objects in the new format.
- Existing public APIs (including bindings): No
- Rolling upgrades / mixed-version behavior (if applicable)
  - Clean shutdown (ideal rolling upgrade behavior):
    SlateDB flushed all data in the WAL to a L0 SST on object storage.
    No WAL object needs to be replayed.
    New WAL objects are written in the new WAL format with extension `.wal`.
  - Dirty shutdown (erroneous rolling upgrade behavior):
    SlateDB did not flush all data in the WAL to a L0 SST on object storage.
    WAL objects need to be replayed.
    If SlateDB finds WAL objects with extension `.sst` it will decode them with the old format.
    New WAL objects are written in the new WAL format with extension `.wal`.

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
- Performance tests: Yes, to proof the improvements over the flamegraphs in [#1085](https://github.com/slatedb/slatedb/issues/1085)

<!-- TOC --><a name="rollout"></a>
## Rollout

<!-- Describe the plan for rolling out this change to production. -->

- Milestones / phases:
  - Develop new WAL format alongside the old format, guarded by an experimental flag
  - Run performance tests and make adjustments
  - Update docs and switch to the new format by default
  - Remove obsolete code and experimental flag
- Feature flags / opt-in: No feature flag, new WAL format will become the default
- Docs updates:
  - Update design documentation in SlateDB where the WAL is mentioned
  - Check remaining documentation for mentions of the WAL

<!-- TOC --><a name="alternatives"></a>
## Alternatives

- By keeping the current WAL format we would not fix the performance issues seen in the flamegraph in GitHub issue [#1085](https://github.com/slatedb/slatedb/issues/1085)
- Designing the new WAL format as version 2 of the SST format would blur the distinction between WAL objects
  that are used for writing and durability and SST objects that are optimized for reading the data.
- Using sizes of records instead of the offsets of records in the new WAL format would complicate the possible future
  addition of indices to the records in the WAL (e.g. for multi-writer support).
- Adding a configuration for the maximal size of the WAL instead of using the size of the L0 SSTs would increase the
  surface of the configuration without obvious benefits.
- Adding a magic number to the WAL format to distinguish it from files external to SlateDB did not seem to be necessary
  since the likelihood of having external files in SlateDB directories seems low.
- Coding the sequence number as a delta of a minimum sequence number instead of writing the full 8 bytes number
  in each record would save space, but it would not be straight-forward.
  All records in the same write batch have the same sequence number and a write batch can contain an arbitrary number
  of records.
  To use deltas of the sequence numbers, we need to maintain information about write batch boundaries which seemed
  not worth the effort.

<!-- TOC --><a name="open-questions"></a>
## Open Questions
No

<!-- TOC --><a name="references"></a>
## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- GitHub issue [#1085](https://github.com/slatedb/slatedb/issues/1085)

<!-- TOC --><a name="updates"></a>
## Updates

Log major changes to this RFC over time (optional).
