# SlateDB Streaming and Change Data Capture (CDC)

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Motivation](#motivation)
- [Current State](#current-state)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Public API](#public-api)
- [Implementation](#implementation)
   * [Write Path](#write-path)
   * [Reading the Stream](#reading-the-stream)
   * [Retention](#retention)
- [Rejected Alternatives](#rejected-alternatives)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Chris Riccomini](https://github.com/criccomini)

## Motivation

This RFC aims to adds support for two use cases:

1. Change data capture (CDC)
2. Messaging

CDC is the practice of capturing changes to data in a database. CDC is used to
replicate data to other systems, trigger downstream processes, maintain an
audit log, and so on.

Disk-less Kafka clones such as WarpStream, AutoMQ, Bufstream, and Kafka Freight
have gained widespread traction. S2 has also begun exploring
non-Kafka-compatible diskless message queues. Users wish to use object stores
to stream data (both production and consumption) because of an object store's
bottomless storage capacity, ease of operation, and cost-effectiveness.

A streaming consumer API will allow applications to write key-value pairs to
SlateDB, where the key is a unique identifier for the event and the value is
the event data. UUIDs, auto-incrementing integers, or even PostgreSQL LSNs are
all reasonable choices for the key.

Users might also use non-unique keys for their messages, which will cause
SlateDB to behave like a compacted topic in Kafka. If a user wishes to emulate
multiple topics, they can do so by:

1. Creating one SlateDB database per topic
2. Using a prefix in the key to identify the topic

(1) prevents cross-topic transactions, while (2) will require applications to
filter events from unrelated topics, since they will see rows for all topics in
the WAL.

_TODO we should expose seqnum so users can provide both key and seqnum to get
compacted topics like Kafka_

## Current State

SlateDB does not currently support either CDC or messaging. Users that wish to
receive near-realtime updates to their SlateDB have the following uptions:

1. Poll the database for changes
2. Dual write to SlateDB and an external message queue
3. Implement their own CDC solution using SlateDB's WALs

(1) and (2) are not optimal designs. (3) is the correct architecture, but it
is not a simple implementation. This RFC proposes that we implement (3) in
SlateDB, so users can stream changes out of SlateDB without additional
infrastructure.

## Goals

1. Provide a durable, ordered stream of all mutations to a SlateDB database.
2. Allow clients to resume from a previous position in the stream by seqnum,
   logical timestamp, or system time.
3. Prevent dataloss if consumers fall behind recently written WALs.
4. At-least-once delivery semantics for consumers.
5. Single-digit second latency for consumers.
6. WAL SST boundaries. Exposing WAL SST boundaries allows clients to treat the
   entire WAL SST as a single transaction. This is useful for clients that
   need to respect transaction boundaries.
7. Strong ordering. Events are ordered by their seqnum and timestamp.
8. Allow clients to bootstrap the entire database. Bootstrapping should
   transparently transition to realtime updates from the WAL.

## Non-Goals

1. Exactly once delivery semantics. Consumers are expected to handle retries.
2. Event transformations. Streaming will only expose raw WAL rows and SST
   boundaries.
3. Streaming from parent DBs or external DBs. Users will only be able to stream
   clone DBs as far back as the WALs that were copied during clone
   initialization.
4. TTL-based row-expiration events. Consumers must track TTL expiration and delete
   expired data outside of SlateDB.
5. Update events [similar to Debezium's](https://debezium.io/documentation/reference/stable/transformations/event-changes.html#_change_event_structure).
   All streaming events are puts, deletes, or WAL SST boundary notifications.
6. Streaming directly from the writer client. Users will need to open a
   `DbReader`.
7. Transaction boundaries. Though SlateDB writes transactions as a single WAL
   entry, a single WAL SST might have multiple transactions in it. SSTs do not
   store individual transaction boundaries in SSTs, so we cannot expose them.
8. Exact row-for-row replication. Rows that are overwritten or expired by TTL
   might not be visible to streaming clients during bootstrapping.
9. Support for `wal_enabled=false` databases (that only write to L0+).

## Public API

SlateDB stream data is exposed through the `DbReader`. Applications instantiate
a `DbReader` and call `stream()` to obtain a `DbIterator` starting
at an optional sequence number. Events returned by the iterator contain the
sequence number, key, value and a flag indicating whether the event represents
a deletion.

```rust
pub enum StreamPosition {
    /// Start streaming from the oldest SST in the oldest `SortedRun`.
    Oldest(),
    /// Start streaming from a specific sequence number in the WAL.
    WalSequenceNumber(u64),
    /// Start streaming from a specific logical time in the WAL.
    WalLogicalTime(u64),
    /// Start streaming from a specific system time in the WAL.
    WalSystemTime(u64),
    /// Start streaming from the beginning of the oldest WAL SST.
    WalOldest(),
    /// Start streaming from the next WAL SST that's written.
    WalNewest(),
}

pub struct DbReader {
    // ...
    /// Returns a `StreamIterator` starting at the specified position.
    pub async fn stream(&self) -> Result<StreamIterator, SlateDBError>;
    // ...
}

pub trait StreamIterator {
    /// Seeks to the specified position.
    pub async fn seek(&mut self, position: StreamPosition) -> Result<(), SlateDBError>;
    /// Returns the next event in the stream or `None` if the stream is closed.
    pub async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError>;
    /// Closes the stream iterator and releases its resources.
    pub async fn close(&mut self) -> Result<(), SlateDBError>;
}
```
_TODO we need to expose seqnum in KeyValue (or KeyValueSeqNum?) so applications
can use it like a Kafka offset to store and resume from a previous position._

_TODO rather than polling, should we simply allow users to call .next(), get a
`None`, and back off itself? Under the hood, Kafka consumers are also polling._

_TODO do we want to use DbIterator? Convert it to a trait?_

_TODO mention frontrunning WAL SSTs. The streamer might see data that's not yet
visible to the reader (since its checkpoint is only refreshed periodically)._

_TODO https://github.com/slatedb/slatedb/pull/530 mentions descending iterator,
and @baobaomaomeng mentions providing a comparator. If we did so, we could use
that to compare seqnum's and timestamps. Not sure it would fit quite right._

## Implementation

### Write Path

...

### Reading the Stream

...

### Retention

...

## Rejected Alternatives

* A blocking iterator that always returns the next `KeyValue`. It blocks until
  the next value is available or the iterator has consumed the most recent WAL
  SST.

## Updates

- 2025-06-22: WIP draft
