# SlateDB Streaming and Change Data Capture (CDC)

<!-- Replace "RFC Title" with your RFC's short, descriptive title. -->

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary](#summary)
- [Motivation](#motivation)
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
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Summary

This RFC proposes change data capture (CDC) support in SlateDB. A `WalReader` struct is proposed, which allows to poll for available WAL files and read their contents. Users can poll repeatedly to assemble a stream of changes to the database.

## Motivation

CDC is the practice of capturing changes to data in a database. Change events are streamed to CDC consumers in write-order. CDC event consumers use the CDC feed to replicate data to other systems, trigger downstream processes, maintain an audit log, and so on. Many databases provide first-class CDC support, such as PostgreSQL's logical replication, MySQL's binlog replication, and MongoDB's change streams. SlateDB CDC support will allow users to more easily integrate SlateDB into their data ecosystem.

### Current State

SlateDB does not directly support CDC. Users that wish to receive near-realtime
updates of their SlateDB have the following options:

1. Scan the database for changes
2. Dual write to SlateDB and an external message queue
3. Implement their own CDC solution by directly reading SlateDB's WAL and/or
   compacted SSTs.

(1) and (2) are not optimal designs. (3) is the correct architecture, but it
is not a simple implementation. This RFC proposes that we implement (3) in
SlateDB so users can stream changes out of SlateDB without additional
infrastructure.

## Goals

- Provide a durable, ordered stream of mutations to a SlateDB database.
- Single-digit second latency for consumers.
- Exposing WAL file boundaries so WAL files can be treated as distinct write operations. (These are not the same as transactions; a single WAL file may contain multiple transactions, which will have distinct seqnums.)
- Put, delete, and merge events should all be visible.
- The design should be modular enough to allow easy migration to a `slatedb-wal` package that does not depend on the `slatedb` package.
- Support for `wal_object_store_uri` when it is set to a different object store than the main object store.

## Non-Goals

- WAL checkpointing and WAL retention policies. WAL garbage collection is independent of the `WalReader`. The `WalReader` must stay ahead of whatever mechanism is used (likely SlateDB's garbage collector with WAL `min_age` configured).
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

SlateDB's CDC design is based loosely on RocksDB's [`getUpdatesSince`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h#L1959-L1974) API. In RocksDB, `getUpdatesSince` uses a [`TransactionLogIterator`](https://github.com/facebook/rocksdb/blob/feffb67303b7d8f38fd91acb9a5b6f6f06c068c3/include/rocksdb/transaction_log.h#L92) to read `write_batch`es. In SlateDB, we will use a `WalReader` to return `WalFile`s, which can be read to obtain `RowEntry`s. Unlike RocksDB, no iterator abstraction is provided. Users can poll for new WAL files and read their contents as needed; this allows users to control polling intervals and parallelism without added configuration.

### `WalReader` API

```rs
/// Represents a single WAL file stored in object storage. Contains metadata about the
/// WAL file as well as methods to read its contents.
pub struct WalFile {
   /// The unique identifier for this `WalBatch`. Corresponds to the SST filename without
   /// the extension. For example, file `000123.sst` would have id `123`.
   pub id: u64,

   /// The time this `WalBatch` was written to object storage.
   pub create_time: DateTime<Utc>,

   /// The size of this `WalBatch` in bytes.
   pub size_bytes: u64,
}

impl WalBatch {
   // TODO: should we expose a metadata() method that returns min/max seqnum, timestamps, etc?
   // Users can compute this themselves by reading the rows, but it might be more performant
   // for very large WAL files to expose metadata directly. I was aiming to keep the API surface
   // minimal for now.

   /// Reads and returns all `RowEntry`s in this `WalBatch`. Raises an error if the
   /// `WalBatch` could not be read.
   pub async fn rows(&self) -> Result<Vec<RowEntry>, crate::Error>;
}

pub struct WalReader {
   /// Lists WAL files in ascending order by their ID within the specified range.
   /// If `range` is unbounded, all WAL files are returned.
   pub async fn list(&self, range: Range<u64>) -> Result<Vec<WalFile>, crate::Error>;
}
```

### Usage

Users can stream database changes by repeatedly calling `list()` to find new WAL files, then calling `rows()` on each new `WalFile` to read its contents.

```rs
let mut current_id: Bound<u64> = Unbounded;
loop {
   let wal_files = wal_reader.list(current_id..).await?;
   for wal_file in wal_files {
       let rows = wal_file.rows().await?;
       for row in rows {
           // Process the row (send it to a message queue, apply it to another DB, etc.)
       }
       current_id = Included(wal_file.id + 1);
   }
   // Sleep for a short duration before polling again.
   tokio::time::sleep(Duration::from_secs(1)).await;
}
```

_NOTE: This API uses file ID-based rather than seqnum-based ranges. This allows the reader to directly read WAL files rather than reading metadata and binary-searching for the appropriate file and seqnum offset in a WAL file. If users wish to resume from a specific seqnum, they will need to implement filtering logic themselves._

### Semantics and Guarantees

The API provides exact row-for-row replication. The consumer will see all puts, deletes, and merges in the order they were written to the WAL.

Consumers that write their output and their WAL position atomically will achieve exactly-once processing semantics. Consumers that do not track their output atomically with their WAL position might see duplicate data if they crash and restart from a WAL position that they have already processed.

The `WalReader` will not interact with any SlateDB components (garbage collector, manifest, compactor, and so on). It will simply read WAL files from object storage as they are written. Consumers are responsible for ensuring that they read WAL files before they are garbage collected.

### WAL and Memtable Synchronization

`batch_write.rs` currently writes to the WAL `KVTable` (or `WalBuffer`) and memtable `KVTable` in paralle. It is possible that the memtable might get flushed to L0 before the WAL file is written to object storage. If this happens, there is a brief window where an entry might be durably written but not end up in the WAL. Consider the following sequence of events:

1. t0: `batch_write.rs` calls self.wal_buffer.append(&entries)?.durable_watcher();
2. t1: `batch_write.rs` calls self.write_entries_to_memtable(entries);
3. t2: Memtable flush is triggered, and the memtable is written to L0.
4. t3: Writer crashes before the WAL file is written to object storage.

In this case, the CDC feed would not see data that is durably written to the database. We considered two options to address this:

- Replay all writes in the database greater than the max seqnum in the last WAL file. This would require that we prevent the L0 writer and compactor from deleting seqnums that haven't yet been written to the WAL. We would then need to scan all SSTs (and SRs) from top to bottom until we reach the max seqnum in the last WAL file.
- Ensure that WAL files are written to object storage before memtables are flushed to L0. This would require that we block memtable flushes until the WAL write is confirmed.

This RFC proposes that we pursue option (2): blocking memtable flushes until the WAL write is confirmed. This is simpler to implement and makes the database semantics easier to reason about (any data in the DB is guaranteed to be in the WAL).

The performance impact on memtable flushes should be minimal. When `wal_enabled=true`:

- `await_durable=false` writes will not block on either WAL or memtable flushes, so low-latency writes are unaffected.
- `await_durable=true` writes will block on WAL writes as they do today (we always return `wal_watcher` in `batch_write.rs` if WALs are enabled).
- `flush_with_options` calls with `FlushOptions::flush_type=Wal` will block on WAL writes as they do today.
- `flush_with_options` calls with `FlushOptions::flush_type=Memtable` will now block on WAL writes as well.

When backpressure is applied, memtable flushes will be delayed until the WAL write is confirmed. This can add more latency to the write path since writes will be blocked for longer. Backpressure should be rare in practice, and when applied in this scenario, a WAL write will free memory just as a memtable flush would have, so the overall impact should be minimal.

To implement this, we will modify `mem_table_flush.rs` to check if `self.db_inner.oracle.last_remote_persisted_seq` is less than the memtable's `KVTable::last_seq`. If it is, `flush_wals()` will be called and awaited before proceeding with the memtable flush. This guarantees that the last entry written to the WAL will be >= the last entry written to the memtable. This is not strictly the most efficient approach since WAL entries beyond the memtable's last seqnum might get flushed, which could otherwise be ignored. However, this approach is simpler to implement and reason about.

If the WAL is disabled, `mem_table_flush.rs` will proceed as normal.

### Bootstraps and Backfills

Consumers are responsible for bootstrapping and backfilling their state. SlateDB provides no built-in support for this.

The following guidelines are recommended:

1. Begin streaming WAL changes using `WalReader`.
2. Create a clone of the current database using `Db::clone()`. This will provide a consistent snapshot of all L0 and compacted SSTs up to the current checkpoint (which was created after the `WalReader` stream began).
3. Use `DbReader::scan()` on the clone to read all existing data in the database.
   - If the database is large, scans in non-overlapping key ranges can be parallelized.
   - If desired, rows with seqnums greater than or equal to the starting seqnum of the `WalReader` stream can be skipped to avoid processing duplicate data.

This approach provides key-ordered rather than seqnum-ordered bootstrapping. If seqnum-ordered bootstrapping is required, users will need to implement their own logic to read all SSTs and sort the data by seqnum.

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

<!-- Describe performance and cost implications of this change. -->

- Latency (reads/writes/compactions): See "WAL and Memtable Synchronization" section.
- Throughput (reads/writes/compactions): See "WAL and Memtable Synchronization" section.
- Object-store request (GET/LIST/PUT) and cost profile: Additional LIST requests will be made to poll for new WAL files. Each WAL file read will incur GET requests to read `RowEntry` data.
- Space, read, and write amplification: Consumers will read additional data from object storage, but there is no change to the data stored.

### Observability

<!-- Describe any operational changes required to support this change. -->

- Configuration changes: `WalReaderOptions` struct to configure reader behavior.
- New components/services: `WalReader` component.
- Metrics: Metrics will be added to track `WalReader` list and read operations.
- Logging: Logs will be added to `WalReader` and `WalFile` methods to trace usage and errors.

### Compatibility

<!-- Describe compatibility considerations with existing versions of SlateDB. -->

- Existing data on object storage / on-disk formats: No changes to existing data formats. WAL files will continue to be written as before.
- Existing public APIs (including bindings): New `WalReader` and `WalFile` API added; existing APIs remain unchanged.
- Rolling upgrades / mixed-version behavior (if applicable): No special considerations; new API can be used with existing databases.

## Testing

<!-- Describe the testing plan for this change. -->

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

## Open Questions

- Should we make `WalBuffer` public instead of introducing `WalBatch`?

## References

- Githb issue [#249](https://github.com/slatedb/slatedb/issues/249) (CDC Streaming to support data sinks and event driven architectures)
- RocksDB's [`getUpdatesSince`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h#L1959-L1974) API
- SlateDB [CDC Discord thread](https://discord.com/channels/1232385660460204122/1467693163815768178)

## Updates

- 2025-06-22: WIP draft
- 2025-02-02: Re-write to focus on `WalReader` API.
