# SlateDB Streaming and Change Data Capture (CDC)

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
   - [Background](#background)
   - [Current State](#current-state)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   - [`WalReader` API](#walreader-api)
   - [Usage](#usage)
   - [Semantics and Guarantees](#semantics-and-guarantees)
   - [WAL and Memtable Synchronization](#wal-and-memtable-synchronization)
   - [Bootstraps and Backfills](#bootstraps-and-backfills)
- [Impact Analysis](#impact-analysis)
   - [Core API & Query Semantics](#core-api-query-semantics)
   - [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   - [Time, Retention, and Derived State](#time-retention-and-derived-state)
   - [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   - [Compaction](#compaction)
   - [Storage Engine Internals](#storage-engine-internals)
   - [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
   - [Performance & Cost](#performance-cost)
   - [Observability](#observability)
   - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

This RFC proposes change data capture (CDC) support in SlateDB. A `WalReader` struct is proposed, which allows to poll for available WAL files and read their contents. Users can poll repeatedly to assemble a stream of changes to the database. Bootstrapping and backfilling are outside the scope of this RFC, however some basic guidelines are discussed in future work.

## Motivation

CDC is the practice of capturing changes to data in a database. Change events are streamed to CDC consumers in write-order. CDC event consumers use the CDC feed to replicate data to other systems, trigger downstream processes, maintain an audit log, and so on. Many databases provide first-class CDC support, such as PostgreSQL's logical replication, MySQL's binlog replication, and MongoDB's change streams. SlateDB CDC support will allow users to more easily integrate SlateDB into their data ecosystem.

### Background

CDC pipelines typically have two phases:

1. **Bootstrap / backfill**: initialize a consumer by reading the existing dataset (often as a consistent snapshot).
2. **Streaming**: continuously consume new mutations as they occur.

The key challenge is ensuring the cutover from snapshot → stream is correct (no missed data and, ideally, no duplicates).
Most systems solve this by choosing a **high-watermark** in a database's durability/replication log (LSN, binlog position,
oplog timestamp, etc.), and then coordinating the snapshot and stream around that boundary.
Common patterns include:

- **Snapshot-then-stream**: take a consistent snapshot, record the corresponding log position, then start streaming from that position.
- **Stream-then-snapshot**: start streaming first, then take a snapshot later and de-duplicate (or ignore) streamed events already covered by the snapshot.
- **Pure log replay**: if the log is retained long enough and contains sufficient information, rebuild state by replaying the log from an initial point.

CDC is most commonly implemented by reading an existing write/replication log (WAL/binlog/oplog/commitlog). Log-based CDC
provides a natural ordering and low overhead on the write path, but introduces operational concerns around log retention and
consumer lag. Depending on the database architecture, ordering may be global (single-leader log) or only per shard/partition
(distributed logs), which affects whether consumers must merge multiple streams.

#### PostgreSQL

PostgreSQL CDC is commonly built on **logical decoding** / **logical replication**, which decodes row-level changes out of the
WAL. Consumers use a **replication slot** (an LSN-based resume point) and a logical decoding output plugin to stream changes.
Replication slots also act as backpressure on retention: the server must keep WAL needed by the slot until the consumer advances.
PostgreSQL can pair streaming with an initial table copy so consumers can bootstrap from a consistent snapshot and then continue
from the corresponding LSN.

#### MySQL

MySQL CDC is typically built on the **binary log (binlog)**. Consumers resume from a binlog file+offset or a GTID set, and
stream transaction events in commit order. For bootstrapping, systems take a consistent snapshot and record the binlog position
(or GTID) at the snapshot boundary, then stream binlog events after that point. Row-based binlogging is commonly required to
reliably capture row-level mutations.

#### Cassandra

Apache Cassandra provides CDC by exposing a copy of **commitlog segments** for tables with CDC enabled. CDC output is
**node-local** (each node emits mutations it applies), so consumers typically tail CDC per node and then de-duplicate/reconcile
events across replicas. Retention is bounded by local disk and configured CDC space limits, so slow consumers can fall behind
and lose events.

#### MongoDB

MongoDB exposes CDC via **change streams**, which are built on the replica set **oplog**. Consumers subscribe with an
aggregation pipeline and receive a stream of change events plus a **resume token** that allows restarting without missing events
(subject to oplog retention). Bootstrapping is typically done by taking an initial collection scan/dump and then starting the
change stream from an operation time or resume token captured at the snapshot boundary.

### Current State

SlateDB does not directly support CDC. Users that wish to receive near-realtime updates of their SlateDB have the following options:

1. Scan the database for changes
2. Dual write to SlateDB and an external message queue
3. Implement their own CDC solution by directly reading SlateDB's WAL and/or
   compacted SSTs.

(1) and (2) are not optimal designs. (3) is the correct architecture, but it is not a simple implementation. This RFC proposes that we implement (3) in SlateDB so users can stream changes out of SlateDB without additional infrastructure.

## Goals

- Provide a durable, ordered stream of mutations to a SlateDB database.
- Single-digit second latency for consumers.
- Exposing WAL file boundaries so WAL files can be treated as distinct write operations. (These are not the same as transactions; a single WAL file may contain multiple transactions, which will have distinct seqnums.)
- Put, delete, and merge events should all be visible.
- The design should be modular enough to allow easy migration to a `slatedb-wal` package that does not depend on the `slatedb` package.
- Support for `wal_object_store_uri` when it is set to a different object store than the main object store.

## Non-Goals

- WAL checkpointing and WAL retention policies. WAL garbage collection is independent of the `WalReader`. The `WalReader` must stay ahead of whatever mechanism is used (likely SlateDB's garbage collector with WAL `min_age` configured). There is no guarantee that the WAL files are not garbage collected. Long-running consumers should copy WAL files to a separate location if they want to ensure they are not lost.
- TTL-based row-expiration events. Clients must implement their own logic to handle TTL expirations.
- Merge operator events. Merge writes will be exposed as-is; the consumer is responsible for applying the merge logic.
- Streaming directly from the writer client. Users will need to create a `WalReader`.
- Transaction boundaries. Though SlateDB writes transactions as a single WAL entry, a single WAL SST might have multiple transactions in it. Users will need to use RowEntry seqnums to track transaction boundaries.
- Message brokering. SlateDB will not provide message broker functionality such as consumer offset management, topic management, consumer groups, or partitioning. The goal of this RFC is not to provide a Kafka-style log broker or message queue.
- Support for `wal_enabled=false` databases (that only write to L0+).
- Support for parent DBs or external DBs. The `WalReader` will only read WAL files for the specific database it is instantiated for.
- Bootstrapping and backfilling. Users will need to implement their own bootstrapping and backfilling logic using `scan()`.
- Update events [similar to Debezium's](https://debezium.io/documentation/reference/stable/transformations/event-changes.html#_change_event_structure). Users will only see the put/delete/merge operation for a given key.

## Design

SlateDB's CDC design is based loosely on RocksDB's [`getUpdatesSince`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h#L1959-L1974) API. In RocksDB, `getUpdatesSince` uses a [`TransactionLogIterator`](https://github.com/facebook/rocksdb/blob/feffb67303b7d8f38fd91acb9a5b6f6f06c068c3/include/rocksdb/transaction_log.h#L92) to read `write_batch`es. In SlateDB, we will use a `WalReader` to return `WalFile`s, which can be read to obtain `RowEntry`s. Unlike RocksDB, we do not provide a single logical iterator over the WAL; instead, we expose WAL file boundaries (`WalFile`) and a per-file iterator (`WalFileIterator`) so users can control polling intervals and parallelism without added configuration.

### `WalReader` API

```rs
/// Iterator over entries in a WAL file.
pub struct WalFileIterator;

impl WalFileIterator {
   /// Returns the next entry in the WAL file.
   pub async fn next_entry(&mut self) -> Result<Option<RowEntry>, crate::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFileMetadata {
   /// The time this WAL file was last written to object storage.
   pub last_modified_dt: DateTime<Utc>,

   /// The size of this WAL file in bytes.
   pub size_bytes: u64,

   /// The path of this WAL file in object storage.
   pub location: Path,
}

/// Represents a single WAL file stored in object storage and provides methods
/// to inspect and read its contents.
pub struct WalFile {
   /// The unique identifier for this WAL file. Corresponds to the SST filename without
   /// the extension. For example, file `000123.sst` would have id `123`.
   pub id: u64,

   table_store: Arc<TableStore>,
}

impl WalFile {
   /// Returns metadata for this WAL file.
   ///
   /// Returns `Ok(None)` if the WAL file does not exist.
   pub async fn metadata(&self) -> Result<Option<WalFileMetadata>, crate::Error>;

   /// Returns an iterator over `RowEntry`s in this WAL file. Raises an error if the
   /// WAL file could not be read.
   ///
   /// Returns `Ok(None)` if the WAL file does not exist.
   pub async fn iterator(&self) -> Result<Option<WalFileIterator>, crate::Error>;
}

/// Reads WAL files in object storage for a specific database.
pub struct WalReader;

impl WalReader {
   /// Creates a new WAL reader for the database at the given path.
   ///
   /// If the database was configured with a separate WAL object store, pass that
   /// object store here.
   pub fn new<P: Into<Path>>(path: P, object_store: Arc<dyn ObjectStore>) -> Self ;

   /// Lists WAL files in ascending order by their ID within the specified range.
   /// If `range` is unbounded, all WAL files are returned.
   pub async fn list<R: RangeBounds<u64>>(&self, range: R) -> Result<Vec<WalFile>, crate::Error>;

   /// Creates a [`WalFile`] handle for a WAL ID.
   pub fn get(&self, id: u64) -> WalFile;
}
```

### Usage

Users can stream database changes by using `list()` to discover an initial set of WAL files, then using `get(last_seen_id + 1)` to poll for the next WAL file without re-listing.

```rs
// One-time discovery (optional). Consumers can also skip this if they are resuming
// from a persisted WAL ID.
let wal_files = wal_reader.list(..).await?;
let mut next_id = 0;
for wal_file in wal_files {
    if let Some(mut iter) = wal_file.iterator().await? {
        while let Some(row) = iter.next_entry().await? {
            // Process the row (send it to a message queue, apply it to another DB, etc.)
        }
        next_id = wal_file.id + 1;
    }
}

// Poll the next WAL ID without calling list() again.
loop {
    let wal_file = wal_reader.get(next_id);
    if let Some(mut iter) = wal_file.iterator().await? {
        while let Some(row) = iter.next_entry().await? {
            // Process the row.
        }
        next_id = wal_file.id + 1;
        continue;
    }

    // The WAL file is not present (either not written yet, or GC'd). Sleep briefly and retry.
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

_NOTE: This API uses file ID-based rather than seqnum-based ranges. This allows the reader to directly read WAL files rather than reading metadata and binary-searching for the appropriate file and seqnum offset in a WAL file. If users wish to resume from a specific seqnum, they will need to implement filtering logic themselves._

This polling approach addresses the [operational concern](https://github.com/slatedb/slatedb/pull/634#discussion_r2791629993) that object store listings become expensive as the number of retained WAL files grows. By polling `get(next_id)` and checking `WalFile::iterator()` (or `WalFile::metadata()`), consumers can wait for future WAL files with a single object-store GET/HEAD per-polling interval, rather than repeatedly scanning the full WAL prefix via `list()`.

If a consumer falls behind (or suspects it missed an ID due to GC), it can use `list(next_id..)` to re-synchronize and find the next available WAL file, then resume polling with `get()`.

### Semantics and Guarantees

The API provides exact row-for-row replication. The consumer will see all puts, deletes, and merges in the order they were written to the WAL.

Consumers that write their output and their WAL position atomically will achieve exactly-once processing semantics. Consumers that do not track their output atomically with their WAL position might see duplicate data if they crash and restart from a WAL position that they have already processed.

The `WalReader` will not interact with any SlateDB components (garbage collector, manifest, compactor, and so on). It will simply read WAL files from object storage as they are written. Consumers are responsible for ensuring that they read WAL files before they are garbage collected.

### WAL and Memtable Synchronization

`batch_write.rs` currently writes to the `WalBuffer` and memtable `KVTable` in parallel. It is possible that the memtable might get flushed to L0 before the WAL file is written to object storage. If this happens, there is a brief window where an entry might be durably written but not end up in the WAL. Consider the following sequence of events:

1. t0: `batch_write.rs` calls self.wal_buffer.append(&entries)?.durable_watcher();
2. t1: `batch_write.rs` calls self.write_entries_to_memtable(entries);
3. t2: Memtable flush is triggered, and the memtable is written to L0.
4. t3: Writer crashes before the WAL file is written to object storage.

In this case, the CDC feed would not see data that is durably written to the database. We considered two options to address this:

1. Replay all writes in the database greater than the max seqnum in the last WAL file. This would require that we prevent the L0 writer and compactor from deleting seqnums that haven't yet been written to the WAL. We would then need to scan all SSTs (and SRs) from top to bottom until we reach the max seqnum in the last WAL file.
2. Ensure that WAL files are written to object storage before memtables are flushed to L0. This would require that we block memtable flushes until the WAL write is confirmed.

This RFC proposes that we pursue option (2): blocking memtable flushes until the WAL write is confirmed. This is simpler to implement and makes the database semantics easier to reason about (any data in the DB is guaranteed to be in the WAL).

The performance impact on memtable flushes should be minimal. When `wal_enabled=true`:

- `await_durable=false` writes will not block on either WAL or memtable flushes, so low-latency writes are unaffected.
- `await_durable=true` writes will block on WAL writes as they do today (we always return `wal_watcher` in `batch_write.rs` if WALs are enabled).
- `flush_with_options` calls with `FlushOptions::flush_type=Wal` will block on WAL writes as they do today.
- `flush_with_options` calls with `FlushOptions::flush_type=Memtable` will now block on WAL writes as well.

When backpressure is applied, memtable flushes will be delayed until the WAL write is confirmed. This can add more latency to the write path since writes will be blocked for longer. Backpressure should be rare in practice, and when applied in this scenario, a WAL write will free memory just as a memtable flush would have, so the overall impact should be minimal.

To implement this, we will modify `mem_table_flush.rs` to check if `self.db_inner.oracle.last_remote_persisted_seq` is less than the memtable's `KVTable::last_seq`. If it is, `flush_wals()` will be called and awaited before proceeding with the memtable flush. This guarantees that the last entry written to the WAL will be >= the last entry written to the memtable. This is not strictly the most efficient approach since WAL entries beyond the memtable's last seqnum might get flushed, which could otherwise be ignored. However, this approach is simpler to implement and reason about.

If the WAL is disabled, `mem_table_flush.rs` will proceed as normal.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [x] Time to live (TTL)
- [ ] Compaction filters
- [x] Merge operator
- [x] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [x] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- Latency (reads/writes/compactions): See "WAL and Memtable Synchronization" section.
- Throughput (reads/writes/compactions): See "WAL and Memtable Synchronization" section.
- Object-store request (GET/LIST/PUT) and cost profile: Additional LIST requests will be made to poll for new WAL files. Each WAL file read will incur GET requests to read `RowEntry` data.
- Space, read, and write amplification: Consumers will read additional data from object storage, but there is no change to the data stored.

### Observability

- Configuration changes: `WalReaderOptions` struct to configure reader behavior.
- New components/services: `WalReader` component.
- Metrics: Metrics will be added to track `WalReader` list and read operations.
- Logging: Logs will be added to `WalReader` and `WalFile` methods to trace usage and errors.

### Compatibility

- Existing data on object storage / on-disk formats: No changes to existing data formats. WAL files will continue to be written as before.
- Existing public APIs (including bindings): New `WalReader` and `WalFile` API added; existing APIs remain unchanged.
- Rolling upgrades / mixed-version behavior (if applicable): No special considerations; new API can be used with existing databases.

## Testing

- Unit tests: `WalReader` and `WalFile` methods.
- Integration tests: Add `WalReader` thread to `slatedb/tests/db.rs` test.
- Fault-injection/chaos tests: `nightly.yaml` chaos test will inherit `WalReader` test from `slatedb/tests/db.rs`.
- Deterministic simulation tests: None
- Formal methods verification: None
- Performance tests: Manual one-time verification that WAL flushes do not significantly impact memtable flush latency.

## Rollout

- Milestones / phases: None
- Feature flags / opt-in: None
- Docs updates: Add CDC documentation to `website/src/content/docs/docs/design/change-data-capture.md`.

## Alternatives

Several alternatives were considered but rejected:

- Polling L0 rather than the WAL. This approach would allow us to support `wal_enabled=false` databases. However, it is more complex to implement correctly. To make the L0 reads work, we would need the CDC client to see every manifest so it could see every single L0 SST written. To do that, we would have to either depend on `min_age` to prevent manifests/compacted SSTs from being deleted, or we would have to use some kind of boundary in the manifest. This approach would also not expose all writes since some data is compacted prior to L0 flush.
- Reading the L0/sorted runs in reverse from oldest to newest. This would require the consumer to track data that's written after the scan begins and read such data on subsequent polls. This was not considered in-depth due to its perceived complexity. This is similar to [this comment](https://github.com/slatedb/slatedb/pull/634#discussion_r2173603400), which proposes diffing database metadata at different points in time.
- Providing a CDC callback directly in the writer client. Writes would simply trigger a callback after WAL/L0 SSTs are written. This is similar to the [Notification on memtable flush](https://github.com/slatedb/slatedb/issues/1245) feature request.
- Attaching to the GC or compactor lifecycle. We could, for example, have GC trigger a callback or write data elsewhere when it removes WAL or L0 entries.
- Implementing a `ScanOptions::range` API that would allow scans to read only data in a specific seqnum range. This would allow users to implement their own CDC solution by repeatedly scanning starting from seqnum 0 and scanning in batches until they reach the latest seqnum. This was rejected due to performance concerns; scanning the entire database repeatedly would be inefficient for large databases.

Notably, this RFC does not preclude us from implementing any of the above alternatives. They are simply not proposed in this RFC.

### WAL and Memtable Synchronization

This RFC proposes blocking memtable flushes on WAL flushes (when `wal_enabled=true`) to guarantee that any data visible in the DB is also present in the WAL.

During discussion, [an alternative was suggested](https://github.com/slatedb/slatedb/pull/634#discussion_r2789422085): filter the memtable during flush so we only flush rows with `seq <= last_remote_persisted_seq`, and carry forward rows with `seq > last_remote_persisted_seq` into a new or existing memtable so they can be flushed later once the WAL is durably persisted remotely. This avoids the explicit synchronization step.

A concern with this approach is that, in the degenerate case, the carry-forward set could grow large, though it won't grow unbounded since `Db::maybe_apply_backpressure` includes `imm_memtables` in its memory usage calculation.

If we ever adopt this approach, one implementation would be to move `seq > last_remote_persisted_seq` rows into the oldest immutable memtable that is not currently flushing (falling back to the active memtable if no immutable memtables exist). This avoids creating a new memtable, but it requires mutating an "immutable" memtable and can cause L0 file sizes to deviate from the configured target (either small durable-only L0 files, or oversized L0 files if the filtered rows accumulate across multiple flushes). An alternative is to create a new memtable containing just the filtered rows and push it onto the `imm_memtables` `VecDeque`, but that can increase the number of very small L0 files.

For now, we keep the synchronization-based implementation; if it becomes a performance bottleneck, this filtering approach should be faster and can be revisited.

## Future work

### Bootstraps and Backfills

Consumers are responsible for bootstrapping or backfilling initial state outside of the CDC pipeline. SlateDB provides no built-in support for this at the moment.

Users can currently bootstrap data using `Db::scan()`, but there is no easy way to do a clean cut over to the WAL stream. Users may choose to simply accept duplicates, or they can use key/value payloads to de-duplicate data. For example, if a value contains a unique ID field, users can de-duplicate using this field.

Various fields in the manifest might be considered, but they are insufficient. The `wal_id_last_seen` field in the manifest is insufficient because subsequent WAL files might be written. Those writes are marked durable and exposed to `scan()` operations. The `last_l0_seq` contains the last sequence number written to L0, but writes with later sequence numbers might be persisted to the WAL (see [0011-transaction.md](0011-transaction.md) for sequence number details).

To provide a clean cutover, we need to expose the sequence number at the point the `scan()` is executed. [#1247](https://github.com/slatedb/slatedb/pull/1247) proposes exactly this. With `scan()`, `DbIterator` will be extended to have a `next_row()` function, which will return a `RowEntry`. The `RowEntry` will contain the sequence number of the row. Users can keep track of the highest sequence number seen during the scan, and then use that sequence number as a boundary when processing the WAL stream to avoid duplicates. This approach provides key-ordered rather than seqnum-ordered bootstrapping. If seqnum-ordered bootstrapping is required, users will need to implement their own logic to read all SSTs and sort the data by seqnum.

### Database diffs

There was [some discussion](https://github.com/slatedb/slatedb/pull/634#discussion_r2778273686) in this RFC about periodic replication. The example cited was an hourly replication feed into Apache Iceberg:

> Consider the case of replicating SlateDB to Iceberg, once every hour or so. We wouldn't need all the updates for this case, it is enough to only get the keys that were updated since previous replication, and just the most recent update for every key. The changes can be coalesced on the read side. Alternate approach for this scenario would be to compare two checkpoints and return the diffs. Wal based approach is more generally applicable, and is simpler. Do you see value in also adding the "only get the recent update since last time" approach for a future, different RFC?

This is an interesting idea, but it is outside the scope of this RFC.

## References

- Githb issue [#249](https://github.com/slatedb/slatedb/issues/249) (CDC Streaming to support data sinks and event driven architectures)
- RocksDB's [`getUpdatesSince`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h#L1959-L1974) API
- SlateDB [CDC Discord thread](https://discord.com/channels/1232385660460204122/1467693163815768178)

## Updates

- 2025-06-22: WIP draft
- 2025-02-02: Re-write to focus on `WalReader` API.
