# SlateDB Commit & Durability

Status: Draft

Authors:

* [Li Yazhou](https://github.com/flaneur2020)

## Background

The discussion about commit semantics and durability is started in the comments of https://github.com/slatedb/slatedb/pull/260. As more and more details are discussed, it becomes clear that the topic is not trivial. The semantics of commit & durability is a very tough topic, many details are involved, and the differences between different semantics are subtle.

Let's start an RFC to discuss the topic.

## Goals

The goals of this RFC are to:

1. Define clear commit semantics & durability guarantees for SlateDB that users can safely rely on.
2. Define the API for users to specify their commit semantics & durability requirements.
3. Take tiered WAL into consideration.
4. Organize the possible code changes for the above goals.

Also, as discussed in the meeting & comments, we hope this change can be done in a way that is additive, not to compromise the capability which is already provided.

## Other systems

This section is based on @criccomini 's comment in <https://github.com/slatedb/slatedb/pull/260#issuecomment-2576502212>.

We'll compare the commit semantics & durability guarantees of SlateDB with other systems. The comparison will be based on the following aspects:

1. ACID commit semantics
2. Durability vs Performance trade-off
3. API Design
4. Error handling

### PostgreSQL

### RocksDB

If writing with `sync` enabled, the commit is not considered as committed until the data is flushed to storage. And this write will never be visible to readers until it's committed.

If got error while flushing the WAL to storage, the commit will be rolled back, and will mark the RocksDB instance as read-only.

## Possible Improvements

The current model does not provide a way to allow users to read the unpersisted committed data cleanly.

Like, in a transaction with sync commit, this commit is not considered as committed until the data is flushed to storage. But the data is already visible if a read accepts unpersisted data, thus, this means it's possible to read the leaked uncommitted data before it's committed.

If a user do not want to read the leaked uncommitted data, they can ensure all the reads are persisted by using `DurabilityLevel::Persisted` for all the persisted data is committed. But it's not wise to limit the read to persisted -only data in a transaction, or it'll be a problem if some others put some unpersisted writes on some keys, it'll constantly cause rollbacks on conflicts if this transaction accesses these same keys.

