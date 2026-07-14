# Pluggable WAL 

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Rohan Desai](https://github.com/rodesai)

## Summary

This RFC proposes (1) an interface to decouple SlateDB from the WAL and (2) adding the ability 
for users to plug in their own WAL implementations.

## Motivation

SlateDB is designed to use object store for all storage, including its WAL. This brings many 
benefits (cheap storage, no xfer costs, simple operations, share-ability). On the other hand, 
object storage forces inherent latency/cost/durability tradeoffs for writes. You can reduce 
latency by tuning the WAL's flush interval down, but this drives up the cost from PUTs. There's 
also a floor for this tradeoff as object store PUTs themselves take 10s of ms on average. 
Alternatively you can opt not to write with `await_durable` but then you're not guaranteed that 
the write is durable.

Users also use the WAL for CDC and to populate readers. As with writes, an object-store based 
WAL imposes a limit on end-to-end latency. Readers/CDC have to poll for new updates. 
Object stores charge for each GET, and GETs themselves can take 10s of ms.

These tradeoffs are appropriate for many systems that are not latency-sensitive. For those that 
are, this RFC proposes allowing plugging in WAL implementations that use alternative backing stores.
Latency-sensitive systems can use an alternate WAL to get low-latency writes and cdc while still 
reaping many of the benefits of SlateDB's object-store native architecture.

## Goals

- Define a set of traits that users can implement to use alternative WALs in lieu of SlateDB's 
  native WAL.
- Add apis to the various builders to enable using alternative WALs.
- Support streaming updates from the WAL to readers before regular manifest polls/updates.

## Non-Goals

- Expose SlateDB's native wal implementation publicly (e.g. for use outside of SlateDB).
- Add alternative WAL implementations to the SlateDB project.
- Exposing a way to source events other than WAL writes (e.g. tree changes for manifest warming)

## Design

<details>
<summary>Status Quo</summary>

### Background/Status Quo 

Lets first take a look at how the WAL works today and how SlateDB uses it. At a high level, 
SlateDb uses the WAL to quickly/cheaply persist new writes, and to recover persisted writes that 
have not made their way into L0.

Internally, the WAL is a sequence of Wal Files. Each Wal File stores a sequence of 
writes (the writes corresponding to some range of sequence numbers) in an object in object 
storage. Each WAL File has a WAL File ID that is exactly one greater than the last WAL File ID. The 
WAL Files are strictly ordered, so reading WAL Files in order yields a total order of writes to 
SlateDB.

SlateDB interacts with the WAL in a few places:

**Fencing**

When SlateDB starts, it fences older writers by fencing both the Manifest and the WAL 
(`WriterFencer`). Both structures need to be fenced as SlateDB does not/can not transactionally 
read-modify-write across the two. SlateDB fences the WAL by writing a so-called Fencing WAL File
(with no rows) to the next WAL ID. WAL File PUTs use `If-None-Match`, so older writers fail with 
an object store collision on the next WAL write. The specifics of the protocol are more involved,
but for now its sufficient to understand that the WAL and Manifest must both be fenced.

**Recovery**

Next, the db replays any writes that have not yet made it into L0 (`WalReplayIterator`). The 
manifest specifies a WAL ID that L0 is guaranteed to fully cover (`replay_after_wal_id`), along with 
the last sequence number written to L0 (`last_l0_seq`). `replay_after_wal_id` gives the db the 
start of the WAL File range it needs to read. Fencing establishes the end of the range. The db 
replays these WAL files into memtables, filtering out any rows with sequence numbers at or below 
`last_l0_seq`.

**Writes**

Once it's recovered persisted writes, the db hands the WAL (`WalBufferManager`) off to the 
Batch Writer task. This task serializes all writes and buffers them in `WalBufferManager`, 
which periodically flushes the writes to a new WAL file. `WalBufferManager` notifies blocked 
write tasks when writes are durably flushed. 

**Memtable/L0 Flushing**

The Batch Writer task adds writes to the memtable once they've been buffered in 
`WalBufferManager`. It "freezes" memtables once they cross the memtable size threshold and 
annotates the frozen memtable with a `replay_after_wal_id` which holds the ID of some WAL File 
whose writes are fully covered by the memtable (in the current implementation this is the last 
durably flushed WAL File). The frozen memtables are picked up by a separate Manifest Writer task,
which stores `replay_after_wal_id` in the manifest alongside the change that commits the 
corresponding L0 file.

**Db Flushing**

SlateDB client can explicitly request flushes by calling either `flush` or `create_checkpoint`. 
Flushes can request a flush of just the WAL, or a flush of the Memtable. Flushes also go through 
the Batch Write task, which either flushes the WAL or uses the memtable freeze mechanics 
described above depending on what the user requested.

**Checkpoints**

When `WalBufferManager` durably persists a WAL File, it notifies the db, which updates 
`last_seen_wal_id` in the manifest with the flushed WAL ID. `DbReader` uses this field to 
determine the range of WAL Files that should be read for a checkpoint.

**Garbage Collection**

GC (`WalGcTask`) is responsible for cleaning up old WAL Files. The GC first resolves all live 
Manifests, and then uses these to determine the set of referenced WAL Files. For a given Manifest
`M` that is not the current manifest, its referenced WAL Files are those with range
`M.replay_after_wal_id..=M.last_seen_wal_id`. For the current Manifest `M_c`, its referenced 
WAL Files are those with range `M.replay_after_wal_id..`. The GC then deletes any WAL files W that 
meet the following conditions:
- W is not referenced.
- W is not a fencing WAL
- W is older than `min_age`

**Reader Maintenance**

`DbReader` reads from the WAL to populate its memtables. When loading a user-provided checkpoint,
`DbReader` replays the WAL range specified from the Manifest. When the user does not specify a 
checkpoint, `DbReader` periodically polls the Manifest and replays WALs referenced by the 
current Manifest.

**CDC**
The `wal_reader` module defines a low-level interface to support CDC. Users create a `WalReader`
to list/get `WalFile` instances. `WalFile#iterator` is used to read the contents of the file.
Polling/batching is left up to the caller.

</details>

### Model

The basic model between SlateDB and the WAL is perfectly reasonable, and we don't propose 
fundamentally changing it in this RFC. The WAL remains a log of sequenced writes written into a 
series of WAL Files. The former is a fundamental requirement, and the latter choice allows for 
implementations to reference the underlying storage structure for recovery and garbage 
collection without maintaining an index that maps from SlateDB's sequence number. Storing writes
in a sequence of "files" is a natural structure that should map well to any backing storage. For 
example a Kafka WAL could store (batches of) write batches in a kafka record, so each kafka 
record is a WAL File whose offset is its WAL ID).

The Alternatives section discusses a couple of alternative levels of abstraction and the 
associated challenges.

### Proposed Interfaces

We will add the following traits which WAL implementations implement and which SlateDB calls 
when accessing the WAL:

```rust
/// A range of WAL File IDs
pub struct WalFileRange(pub Bound<u64>, pub Bound<u64>);

/// Defines the types of errors that can be returned by WAL implementations.
#[derive(Debug, Clone)]
pub enum WalError {
    /// The WAL writer was fenced
    Fenced,
    /// IO error writing/reading the WAL
    IoError(Arc<std::io::Error>),
    /// Fatal error indicating that the WAL is in some unexpected/unrecoverable state.
    InternalError(Arc<dyn Error + Sync + Send + 'static>),
    /// A WalIterator observed that the tail of the WAL was truncated while iterating.
    WalTruncated,
    /// Used internally by SlateDB to propagate its internal error type.
    SlateDBError(Arc<dyn Error + Sync + Send + 'static>),
    /// Operation against wal after it was closed
    Closed,
}

/// The writer's manifest after fencing. Created by calling [`ManifestFencer::fence`]
pub struct WriterManifest {
    manifest: FenceableManifest,
}

impl WriterManifest {
    /// Returns the current manifest.
    pub fn manifest(&self) -> VersionedManifest {
        let (id, manifest) = self.manifest.manifest();
        VersionedManifest::from_manifest(id, manifest.clone())
    }

    /// Returns the WAL ID up to which SlateDB has guaranteed to have stored all data in the
    /// LSM tree.
    pub fn replay_after_wal_id(&self) -> u64 {
        self.manifest().core().replay_after_wal_id
    }

    /// Returns the writer's epoch
    pub fn epoch(&self) -> u64 {
        self.manifest().writer_epoch()
    }

    /// Refreshes the current manifest. Implementations of `WriterInit::fence_and_init` can
    /// use this to detect whether the manifest has been fenced while executing the fencing
    /// protocol. SlateDB will call this after calling [`WriterInit::fence_and_init`]
    pub async fn refresh(&mut self) -> Result<(), WalError> {
        self.manifest.refresh().await?;
        Ok(())
    }
}

/// The result returned by [`WriterInit::fence_and_init`]
pub struct WriterInitResult {
    /// An iterator that returns writes that must be replayed before starting SlateDB to recover
    /// data from the WAL.
    pub replay_iterator: Box<dyn WalIterator>,
    /// The WAL writer that will be used to append new writes to the WAL
    pub wal_writer: Box<dyn WalWriter>,
}

/// API for fencing and initializing a new WAL writer for use by [`crate::db::Db`]. SlateDB requires
/// WAL implementations to execute a fencing protocol that guarantees (1) that earlier writers no
/// longer write to the db and (2) all rows present in the WAL but not in the LSM tree (L0 and
/// sorted runs) are recovered.
///
/// Every [`crate::db::Db`] instance is assigned a unique `u64` epoch. The epoch is assigned when
/// fencing the Manifest. A given Db instance writes both the WAL and its Manifest (e.g. with new
/// SSTs) independently. The fencing protocol that yields epoch E must ensure that:
/// (1) After the first write to the Manifest with epoch E, there are no further writes to either
///     the Manifest or WAL with epoch E' < E
/// (2) After the first write to the WAL with epoch E, there are no further writes to either the
///     Manifest or WAL with epoch E' < E
/// (3) All rows from the WAL from writers with epoch E' < E that are not present in L0/SRs are
///     replayed before serving reads/writes.
///
/// `[WriterInit::fence_and_init]` is responsible for
/// (1) Fencing the WAL such that no writers with an epoch earlier than [`WriterManifest::epoch`]
/// (2) Constructing a [`WalWriter`] instance that the writer uses to append new WAL entries.
/// (3) Resolving the end of the WAL and constructing a [`WalReplayIterator`] that returns all
///     rows in WAL files between [`WriterManifest::replay_after_wal_id`] (exclusive) and the
///     current end of the WAL.
#[async_trait]
pub trait WriterInit {
    /// Fences the WAL and returns a [`WriterInitResult`] with a [`WalWriter`] and
    /// [`WalReplayIterator`] used to recover writes that have not yet been flushed to the tree.
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError>;
}

/// Describes the current status of the WAL
#[derive(Debug, Clone)]
pub struct WalStatus {
    /// Set to Some if the WAL has permanently shut down, along with the reason. The reason should
    /// be [`WalError::Closed`] on a normal shutdown, and some other [`WalError`] variant on
    /// failure.
    pub closed_reason: Option<WalError>,
    /// The estimated in-memory bytes used by the WAL to buffer unflushed writes. Used by
    /// SlateDB to apply backpressure.
    pub estimated_bytes: usize,
    /// The id of the last WAL file that was durably flushed
    pub last_flushed_wal_id: u64,
    /// The last sequence number that was durably flushed
    pub last_flushed_seq: Option<u64>,
    /// The number of writes currently buffered
    #[allow(dead_code)]
    pub buffered_wal_entries_count: usize,
}

/// An event emitted by a [`WalWriter`] to subscribers.
#[derive(Debug, Clone)]
pub enum WalEvent {
    /// Emitted when a WAL file is durably flushed to storage. On receipt of this event, SlateDB
    /// notifies write tasks blocked on [`crate::config::WriteOptions::await_durable`]. SlateDB
    /// also uses this to apply backpressure if the implementation sets
    /// [`WalStatus::estimated_bytes`]. Implementers should update this to reflect clearing the
    /// memory used for buffering the Wal File before emitting this event.
    WalFlushed(WalStatus),
    /// Emitted when the WAL has closed with the final wal status containing the closed reason
    WalClosed(WalStatus),
}

/// A listener that's called back on WAL events.
pub type WalStatusListener = Arc<dyn Fn(WalEvent) + Send + Sync + 'static>;

/// An observer that can read the current [`WalStatus`] and subscribe to event callbacks.
#[async_trait]
pub trait WalObserver: Send + Sync + 'static {
    /// Returns the current [`WalStatus`].
    fn status(&self) -> Result<WalStatus, WalStatus>;

    /// Adds a listener that subscribes to event callbacks.
    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError>;
}

/// A future that yields the result of flushing the WAL. Returned by [`WalWriter::flush`]
pub type FlushResultFuture = BoxFuture<'static, Result<(), WalError>>;

/// The WAL's write API. Used by SlateDB to append new WAL writes. Is returned by
/// [`WalWriterInit::fence_and_init_writer`].
///
/// Each call to [`WalWriter::append`] takes a single SlateDB write batch, where all rows share
/// the same sequence number ([`RowEntry::seq`]). [`WalWriter`] (optionally accumulates/buffers
/// rows and) writes consecutive write batches into consecutive WAL Files, where each WAL File
/// contains some rows from the total sequence of rows. Specifically:
/// - WAL Files must have a total order and each WAL File must have a u64 id that is greater than
///   all earlier WAL Files.
/// - Reading WAL Files in order should yield rows in sequence order.
/// - The writes in a given write batch must be written to WAL files atomically. That is, a
///   [`WalIterator`] should either observe all the writes with a given sequence number or none
///   of them.
#[async_trait]
pub trait WalWriter: Send {
    /// Append a write batch to the WAL.
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError>;

    /// Triggers a flush of all appended write batches to durable storage. Returns a
    /// future that receives the result of the flush once it completes.
    async fn flush(&mut self) -> Result<FlushResultFuture, WalError>;

    /// Returns a `WalObserver` for reading [`WalStatus`] and subscribing to events.
    fn observer(&self) -> Box<dyn WalObserver>;

    /// Returns the current `WalStatus`. If the [`WalWriter`] has failed, then returns Err with the
    /// final [`WalStatus`] and the reason for the failure in [`WalStatus::closed_reason`]
    fn status(&self) -> Result<WalStatus, WalStatus>;

    /// Close the `WalWriter` and release resources
    async fn close(&mut self) -> Result<(), WalError>;
}

/// Rows returned by [`WalIterator`]
pub struct WalRows {
    /// The rows read from the WAL File. All the rows with a given sequence number must be present
    /// in th same [`WalRows`].
    pub rows: Vec<RowEntry>,
    /// The id of the last WAL File containing rows from `rows`. There may still be rows with higher
    /// sequence numbers in the WAL File with this id.
    pub last_wal_file_id: u64,
    /// True when this batch is the last one in its WAL file. This is an
    /// optimization, so its harmless to always set to false. Callers can already infer that a
    /// file is fully applied when they see a batch from a later file, but this flag lets them
    /// advance their WAL watermark over the current file without waiting for the next one.
    pub last_in_file: bool,
}

/// An iterator over rows in some range of the WAL
#[async_trait]
pub trait WalIterator: Send + 'static {
    /// Returns the next set of rows. Rows must be returned in sequence and WAL File order.
    /// Returns None when iterator's range is exhausted. Iterators created using an unbounded
    /// end range that have exhausted the current WAL block until new rows are appended and neverCollapse annotation
    /// return `None`.
    /// Returns [`WalError::WalTruncated`] if the iterator observes that the WAL was truncated
    /// while iterating.
    async fn next(&mut self) -> Result<Option<WalRows>, WalError>;
}

/// API for reading from the WAL. Used by the Reader/
#[async_trait]
pub trait WalReader {
    /// Returns an iterator over the specified range of WAL File IDs. The start of the range must
    /// not be `Unbounded`. If the end of the range is `Unbounded` then the returned iterator
    /// continues returning writes as new writes are appended to the WAL. Otherwise, it returns
    /// `None` upon reaching the end of he range.
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError>;
}

/// API for plugging into WAL GC
#[async_trait]
pub trait WalGC {
    /// Hook for garbage collecting the WAL. Takes a list of ranges of WAL Files that are currently
    /// referenced by some active Manifest. The implementation may delete any WAL File that is not
    /// included in the ranges in this list.
    async fn collect(
        &self,
        referenced_ranges: Vec<WalFileRange>,
    ) -> Result<(), WalError>;
}
```

Users can configure a custom WAL for the writer and reader using the db Builder:
```rust
impl<P: Into<Path>> DbBuilder<P> {
    /// Sets the `[WalWriterInit]` used to initialize a `[WalWriter]` to append new
    /// entries to the WAL. Use this to plug in custom WAL implementations to SlateDB.
    /// By default, SlateDB uses its own object-store based WAL.
    pub fn with_wal_writer(mut self, wal_writer_init: Box<dyn WalWriterInit>) {
        self.wal_writer_init = Some(writer_init);
    }
}

impl<P: Into<Path>> DbReaderBuilder<P> {
    /// Sets the `[WalReader]` used to create `[WalIterator]`s to replay rows from the WAL
    /// Use this to plug in custom WAL implementations to the reader. By default, SlateDB uses
    /// its own object-store based WAL.
    pub fn with_wal_reader(mut self, wal_reader: Box<dyn WalReader>) {
        self.wal_reader = Some(wal_reader);
    }
}

impl <P: Into<Path>> GarbageCollectorBuilder<P>{
    /// Sets the collector for cleaning up WAL Files. Use this if you are using a custom
    /// WAL implementation and want SlateDB to coordinate GC.
    pub fn with_wal_gc(mut self, wal_gc: Arc<dyn WalGc>) -> Self {
        self.wal_gc = Some(wal_gc);
    }
}
```

### SlateDB Integration
Let's look at how SlateDB will use these interfaces from the various WAL touch-points.

#### Fencing

Every [`crate::db::Db`] instance is assigned a unique `u64` epoch. The epoch is assigned when
fencing the Manifest. A given Db instance writes both the WAL and its Manifest (e.g. with new
SSTs) independently. The fencing protocol that yields epoch `E` must ensure that:
1. After the first write to the Manifest with epoch E, there are no further writes to either
   the Manifest or WAL with epoch `E' < E`
2. After the first write to the WAL with epoch `E`, there are no further writes to either the
   Manifest or WAL with epoch `E' < E`
3. All rows from the WAL from writers with epoch `E'` < E that are not present in L0/SRs are
   replayed before serving reads/writes.

The fencing protocol must fence both the Manifest and the WAL. However, this means that the
protocol must be able to deal with the case where another writer `W'` completes the protocol
while a given writer `W` is between the two fencing operations, e.g.:

```
t1: W fences Manifest
t2: W' fences Manifest
t3: W' fences WAL and resolves replay range
t3: W' updates WAL/Manifest
t4: W fences WAL and resolves replay range
```

It's not safe for `W` to write new WAL entries as it breaks the requirements above. In 
practice this is problematic because it has not observed `W'`'s Manifest updates. Further,
if fencing the WAL depends on reading the Manifest (e.g. SlateDB's WAL protocol), `W`'s fencing
operation is operating on a stale Manifest view.

Take the inverse:
```
t1: W fences WAL and resolves replay range
t2: W' fences WAL and resolves replay range
t3: W' fences Manifest
t3: W' updates WAL/Manifest
t4: W fences Manifest
```

It's not safe for `W` to update the Manifest or serve reads as it has not observed `W'`'s WAL writes.

There are 2 approaches you can take to solve this, depending on the isolation primitives that
are available (or practical) on your backing store:
- Fencing: Your backing store allows you to fence existing writers such that they can not
      append new writes. For example, the basic Kafka transaction protocol. SlateDB's native
      WAL also falls into this category (it also has other constraints imposed by the fact
      that fencing depends on reading the Manifest, but the solution is the same).
- Transactions: Your backing store allows you to transactionally read then conditionally append.
      Examples include any database with transactions, or the Kafka transaction protocol if you
      store the epoch in a Kafka topic and read the value after initializing the transactional
      producer and before appending new writes.

If your backing store supports Transactions, the protocol is simple - you simply make each WAL
write conditional on the writer's epoch.

If your backing store only supports Fencing, then the protocol must fence one resource then the
other, then check that the first resource is still fenced. For example:
1. Fence Manifest
2. Fence WAL
3. Check Manifest is fenced.

SlateDB will execute this protocol on behalf of the WAL implementation by first fencing the 
manifest, then calling `WalWriterInit::fence_and_init`, and then refreshing its `FenceableManifest`
to ensure the db is still fenced. This is a minor change to `WriterFencer` to delegate WAL fencing
to the trait implementation.

#### Recovery

`WalWriterInit::fence_and_init` returns a `WriterInitResult` with a `replay_iterator` that 
iterates over the section of the WAL that must be replayed. The DB replays from this iterator. 
This requires a small refactor of `WalReplayIterator` to iterate over `WalIterator` rather than
directly iterating over WAL Files.

#### Writes

Writes mostly stay the same. The batch writer task takes ownership of `WalWriter` and uses it to 
append new writes via `WalWriter::append`.

The WAL no longer maintains a durability watcher for each WAL file. Instead, a write that
awaits durable blocks on `DbStatus` until the durable sequence number is greater than or equal to
the write's sequence number. When the WAL is enabled, slatedb propagates updates to the durable
sequence number via the `WalObserver` subscription.

**Backpressure**

The WAL needs a mechanism to apply backpressure to incoming writes. If writes arrive faster than 
the WAL can flush them to new WAL Files, they accumulate in the WAL's buffer and use more and 
more memory. SlateDB's native WAL backpressure is integrated into SlateDB's api-level 
backpressure mechanism via `WalStatus::estimated_bytes` and propagation of 
`WalEvent::MemoryReleased` events when the WAL releases memory. 

There's some tension between SlateDB's native WAL and custom WAL implementations here. On the 
one hand, SlateDB's existing mechanics mean that its WAL does not need its own backpressure and 
users get a single config for applying a memory cap to the write path (`max_unflushed_bytes`). 
On the other hand, custom WALs may prefer/need to use their own backpressure. Take a Kafka-backed 
WAL for example. The Kafka producer has its own in-built backpressure mechanism that blocks writes 
when too many records are buffered. There isn't a straightforward way to observe the buffer 
memory or be notified when its released.

We propose allowing WAL implementations to opt into the SlateDB backpressure mechanism but not 
require it. To opt in, the implementation must set `WalStatus::estimated_bytes` to a non-zero value
and emit `WalEvent::MemoryReleased` when memory is released. Alternatively, custom `WalWriter`
implementations can apply backpressure internally by blocking calls to `append`. **To accommodate 
this SlateDB needs to account for memory used by writes waiting in the batch writer's channel when
computing the current unflushed bytes when deciding whether to pause a write.**

This avoids adding a new memory-management config for the bulk of SlateDB users while still 
allowing custom WALs to apply backpressure.

#### Flushing

Memtable and Db flushing stay the same. The Batch Writer task annotates each immutable memtable 
with a safe replay point using `WalStatus::last_flushed_wal_id`, and flushes the WAL using 
`WalWriter::flush`.

#### Garbage Collection

`WalGcTask` lists manifests to determine the set of referenced WAL File IDs and then delegates 
cleanup to `WalGc`. The native implementation of `WalGc` prunes wal files based on max-age and 
then deletes them.

#### Readers

`DbReaderBuilder` initializes `DbReader` with a `WalReader` that it uses to construct iterators 
for replaying the WAL when loading a checkpoint.

`DbReader` will now also continually stream WAL updates when configured to track the latest 
writes. It does this by creating its `WalIterator` with an unbounded end range and blocking on 
`next` from its background polling task. If the reader observes a `WalError::WalTruncated` then it
immediately refreshes the manifest.

#### CDC

We'll deprecate/remove the current CDC API. Users can use the `WalReader`/`WalIterator` proposed
in this RFC. SlateDB's native `WalReader` will take a buffer size and a poll interval to use when
tailing the current WAL:

```rust
struct ObjectStoreWalReader {
    pub fn new<P: Into<Path>>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
        /// The number of WAL Files to prefetch and buffer when streaming the WAL
        buffered_files: usize,
        /// The interval at which the next WAL file will be polled when streaming the latest updates
        poll_interval: Duration
    )
}

impl WalReader for ObjectStoreWalReader {
    ...
}
```

#### Error Handling

Custom WAL implementations are expected to manage the lifecycle of any background tasks and 
propagate errors via their regular apis rather than have SlateDB expose its task management 
framework. 

### Example Alternative Implementations

#### Kafka

The prototype branch has an example implementation of the WAL traits that writes records to
a Kafka topic and implements fencing using Kafka transactions:
https://github.com/slatedb/slatedb/tree/wal-rfc-prototype/slatedb/src/wal/kafka

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [x] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [x] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection
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
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- Switching the `await_durable` mechanism to block on the durable sequence number means that 
  there will likely be more spurious wakeups where a blocked write task is woken up, observes 
  that the sequence number hasn't advanced sufficiently and goes back to sleep. I don't expect 
  this to add meaningful overhead.

### Observability

None. Custom WAL implementations are expected to expose their own metrics/configuration.

### Compatibility

No breaking changes.

## Testing

**Correctness**

We will expose a conformance test suite that WAL implementers can use to validate that their WAL 
implementation is correct. Some important test cases we'll cover (non-exhaustive):
- `WalWriterInit::fence_and_init` prevents existing writers from writing to the WAL.
- `WalWriterInit::fence_and_init` returns an iterator that replays unflushed writes.
- `WalWriter` flushes WAL rows durably (so they can be observed by `WalReader`)
- `WalWriter` emits events when rows are durably stored.
- `WalIterator` always iterates over writes in sequence order
- `WalIterator` always returns full write batches in `WalRows`
- `WalIterator` tracks the WAL file id in `WalRows` correctly (TODO: this probably needs some 
  test interfaces in the reader for listing/reading wal files)

**Performance**

SlateDB already exposes a benchmarking utility, `DbBench`. WAL implementers can write their own 
benchmark tools that instantiate `DbBench` with a db configured to use a custom WAL.

## Rollout

- Phase 1 (in-progress): refactor existing WAL to align with the traits proposed here
- Phase 2: introduce traits and pluggability
- Phase 3: add conformance test harnesses and an example implementation

## Alternatives

List the serious alternatives and why they were rejected (including “status quo”). Include
trade-offs and risks.

**Status Quo**
Users that want a custom WAL can opt out of using SlateDB's WAL and write their own WAL in front 
of SlateDB. This puts a lot of burden on the user. On the writer side you need to implement your 
own write serialization, sequencing, replay tracking, and transaction system. On the reader side 
you need to write your own layer that buffers WAL records and merges them with rows returned by 
the reader.

**Plug in at ObjectStore Layer**
We could support alternative stores by plugging in at the object store layer. This is just a very
awkward integration point. The Object Store trait is object-store specific and the primitives 
don't map very well to other stores you may want to use for a WAL. For example it assumes key-based
access, compare-and-swaps, etc. It also will likely require implementations to understand the 
internals of SlateDB's native WAL, which is brittle.

**More Flexible Fencing API**
The proposed interface for fencing forces the Fence Manifest, Fence WAL, Check Manifest 
structure. Custom WAL implementations may not need this (for example if your store supports 
transactions). We could instead just have a more generic `fence_and_init` API that takes (some 
wrapper over) a `StoredManifest` and is expected to implement the full fence protocol. It feels 
too complicated to expect custom WAL implementations to do this. For now we assume minimal 
fencing semantics from the custom WAL to keep the expectations from the trait simpler at the 
cost of an extra manifest check.

**Pure Row/Sequence Based Abstraction**
We could have the abstraction track WAL position just using row sequence numbers. This requires 
each WAL to have some way to efficiently read starting from a SlateDB sequence number, which is 
not always practical. Expecting the WAL to group rows into a series of "files" feels pretty 
reasonable/general.

**Iterate over WAL Files**
We could have `WalReader`/`WalIterator` iterate over WAL Files which in turn support row-based 
iteration (similar to the CDC `WalReader`). I don't really see the benefit of imposing the extra 
layering. It also forces implementations to map each write batch to a single WAL File.

## Open Questions

- This RFC proposes an API for streaming new writes via `WalReader`/`WalIterator`. Should this be 
  used for CDC in lieu of the existing `WalReader`/`WalFile` API? Does it make sense to retain both?
- Should we put the traits and conformance tests in a separate `slatedb-wal` crate?

## References

- https://github.com/slatedb/slatedb/issues/1768

## Updates

Log major changes to this RFC over time (optional).
