# SlateDB Synchronous Commit & Durability

Status: Draft

Authors:

* [Li Yazhou](https://github.com/flaneur2020)

## Background

The discussion about commit semantics and durability is started in the comments of <https://github.com/slatedb/slatedb/pull/260#issuecomment-2570658593>. As more and more details are discussed, it becomes clear that the topic is not trivial. The semantics of commit & durability is a very tough topic, many details are involved, and the differences between different semantics are subtle.

Let's start an RFC to discuss the topic to allow us to discuss the topic in the code review board.

## Goals

The goals of this RFC are to:

1. Define clear commit semantics & durability guarantees for SlateDB that users can safely rely on.
2. Define the API for users to specify their commit semantics & durability requirements.
3. Take tiered WAL into consideration.
4. Organize the possible code changes for the above goals.

Also, as discussed in the meeting & comments, we hope this change can be done in a way that is additive, not to compromise the capability which is already provided.

## References

- [Understanding synchronous_commit in PostgreSQL](https://medium.com/@mihir20/understanding-synchronous-commit-in-postgresql-54cb5609a221)
- [RocksDB: WAL Performance](https://github.com/facebook/rocksdb/wiki/WAL-Performance)

## Comparison with other systems

We'll compare the synchronous commit semantics & durability guarantees of other systems with SlateDB. The comparison will be based on the following aspects:

1. The API for users to specify their commit semantics & durability requirements
2. Use cases & trade-offs, and the default settings
3. Error handling

### PostgreSQL

PostgreSQL offers a flexible setting called `synchronous_commit` to control the commit semantics & durability guarantees at various levels. These levels contain:

* `off`: The commit is considered complete as soon as the transaction is finished, without waiting for the WAL to be written. Data loss is possible if a crash occurs.
* `local`: The commit waits for the WAL to be written and flushed to local disk before returning.
* `on` (default): The commit waits for the WAL to be written and flushed to local storage, and then waits for at least one standby to apply the WAL if there's synchronous replication configured.
* `remote_write`: The commit waits for the WAL to be written to local storage and replicated to standby servers, and wait for the standby to flush to file system.
* `remote_apply`: The commit waits for the WAL to be written and flushed to local disk, and then waits for the standby to apply the WAL. These writes are expected to be visible on the standby before the commit returns.

The referenced article <Understanding synchronous_commit in PostgreSQL> provides a good diagram to illustrate the commit process about `on` and `off`:

![](./images/postgres-sync-commit.png)

The biggest difference between `on` and `off` is that `on` will wait for the WAL to be written and flushed to local storage. This will become a common pattern in rest of this RFC: the synchronous commit & durability is about the WAL.

Let's try make a summarize about the use cases & trade-offs of different `synchronous_commit` levels:

1. In financial systems, the data is very sensitive, and the data loss is not acceptable. So we should use `on`, `remote_write` or `remote_apply`.
2. On mission critical systems which can not tolerant data inconsistency between primary and standby after a primary switch, we should use `remote_apply` to ensure the data consistency between primary and standby.
3. In some other workloads like logs or stream processing, the data loss is acceptable, but the performance is important. So we can use `off` or `local` to improve the performance.

### RocksDB

Let's directly quote the RocksDB documentation to understand the behavior of Synchronous Commit in RocksDB:

> #### Non-Sync Mode
>
> When WriteOptions.sync = false (the default), WAL writes are not synchronized to disk. Unless the operating system thinks it must flush the data (e.g. too many dirty pages), users don't need to wait for any I/O for write.
>
> Users who want to even reduce the CPU of latency introduced by writing to OS page cache, can choose Options.manual_wal_flush = true. With this option, WAL writes are not even flushed to the file system page cache, but kept in RocksDB. Users need to call DB::FlushWAL() to have buffered entries go to the file system.
>
> Users can call DB::SyncWAL() to force fsync WAL files. The function will not block writes being executed in other threads.
>
> In this mode, the WAL write is not crash safe.
>
> #### Sync Mode
>
> When WriteOptions.sync = true, the WAL file is fsync'ed before returning to the user.
>
> #### Group Commit
>
> As most other systems relying on logs, RocksDB supports group commit to improve WAL writing throughput, as well as write amplification. RocksDB's group commit is implemented in a naive way: when different threads are writing to the same DB at the same time, all outstanding writes that qualify to be combined will be combined together and write to WAL once, with one fsync. In this way, more writes can be completed by the same number of I/Os.
>
> Writes with different write options might disqualify themselves to be combined. The maximum group size is 1MB. RocksDB won't try to increase batch size by proactive delaying the writes.

Same as PostgreSQL, RocksDB also provides a `sync` option to control the commit semantics & durability guarantees. For write operations with `sync = true`, the commit is not considered as committed until the data is `fsync()`ed to storage.

For `sync = false`, the commit is considered as committed as soon as the transaction is finished, without waiting for the WAL to be written. The WAL is still buffered in kernel's Page Cache, data loss is possible if a crash occurs.

But instead of PostgreSQL's `synchronous_commit` which has multiple levels, RocksDB only provides a simple boolean option. The reason is that RocksDB is an embedded database, and do not have the concept of Primary/Standby like PostgreSQL.

To improve the performance of synchronous commit, RocksDB provides a Group Commit mechanism, which is commonly used in WAL based systems. This mechanism will combine multiple writes into a single WAL write, and then flush the WAL to storage.

(In SlateDB, we can leverage the Commit Pipeline to implement a similar Group Commit mechanism which batches multiple writes into a single WAL write.)

One thing worth mentioning is that RocksDB defaults to `sync = false`, which means the WAL write is not crash safe.

This is likely to be a trade-off for performance. In many use cases, especially in distributed systems (which is the most common use case for RocksDB), it's some times acceptable to allow data loss in a single node without hurting the durability of the system. Like making a raft cluster, distributed KV cluster, or a local state store for stream processing, etc. In these cases, `manual_wal_flush` is often a good idea.

### SlateDB's Synchronous Commit & Durability

## Possible Improvements

The current model does not provide a way to allow users to read the unpersisted committed data cleanly.

Like, in a transaction with sync commit, this commit is not considered as committed until the data is flushed to storage. But the data is already visible if a read accepts unpersisted data, thus, this means it's possible to read the leaked uncommitted data before it's committed.

If a user do not want to read the leaked uncommitted data, they can ensure all the reads are persisted by using `DurabilityLevel::Persisted` for all the persisted data is committed. But it's not wise to limit the read to persisted -only data in a transaction, or it'll be a problem if some others put some unpersisted writes on some keys, it'll constantly cause rollbacks on conflicts if this transaction accesses these same keys.

