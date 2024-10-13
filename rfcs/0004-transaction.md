# SlateDB Transaction

## Background

Transaction allows multiple operations to be executed in a single atomic operation, and control the isolation level of the operations. This is a planned feature in SlateDB, and is essential for many critical use cases, like:

- Using SlateDB as a metadata store or coordinator for a distributed system, such as the backend of [Kine](https://github.com/k3s-io/kine) or various metadata stores in OLAP systems.
- Using SlateDB as a underlying KV layer in an OLTP system, such as a [TiKV](https://tikv.org/) replacement or other SQL engines.

This RFC proposes the goals & design draft of the transaction feature in SlateDB.

There're still some ongoing RFCs on the prerequisites of the Transaction feature, such as the Snapshot API and WriteBatch API. We'll refer to these RFCs or PRs in this RFC when they're ready.

Is this RFC considered as too early? Yes, the implementation of the Transaction feature can not be started until the prerequisites got ready. However, this RFC may help us to clarify the requirements of the Snapshot API and WriteBatch API in the sense of the Transaction feature, and organize the roadmap on these developments.

## Goals

- Define the Transaction API in SlateDB.
- Discuss the expected isolation level semantics in SlateDB.
- Organize the prerequisites needed to implement Transaction feature.
- Organize the roadmap for the implementation of transaction feature.

## Non-Goals

- Pessimistic Transaction support: although it is a supported feature in rocksdb, but it is not a goal for the initial implementation. Optimistic transaction is more widely used in practice, and considered as more scalable and efficient. We'll not discuss the support of pessimistic transaction in this RFC.
- Distributed Transaction support: SlateDB is a single-writer database, and distributed transaction is not a goal for the initial implementation. We may consider support [Two Phase Commit](https://github.com/facebook/rocksdb/wiki/Two-Phase-Commit-Implementation) in the future, which may help users coordinate between multiple SlateDB instances, but it is a separate topic and not discussed in this RFC.

## Constraints

- The transaction API should be aligned with the existing APIs like Snapshot API and WriteBatch API.
- The transaction commit operation should be efficient, and should not involve IO overhead on checking conflicts.
- The transaction is better not to produce floating garbage in the storage, and should be able to be GCed after the transaction is committed or rolled back.

## References

- [Concurrent ACID Transactions in Badger](https://dgraph.io/blog/post/badger-txn/)
- [Transactions - RocksDB](https://github.com/facebook/rocksdb/wiki/Transactions)
- [Serializable Snapshot Isolation in PostgreSQL](https://arxiv.org/pdf/1208.4179)

## Proposal

### API

The transaction API should be resembles with the non-transactioned API. All the api like `get()`, `put()`, `range()` should also be supported in the transaction API, and these APIs should be aligned with the Snapshot API + WriteBatch API. The only difference is that the transaction API should be able to `commit()` or `rollback()` the changes.

(note: at the time of this RFC, the Snapshot API and WriteBatch API are still not implemented, the transaction feature should be implemented after these APIs are implemented.)

The write operations like `put()` in the transaction should be buffered in the transaction state, and only be written to the underlying storage when `commit()` is called.

```rust
let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
let options = DbOptions::default();
let kv_store = Db::open_with_opts(
    Path::from("/tmp/test_kv_store"),
    options,
    object_store,
)
.await
.unwrap();

// start transaction and write some data
let txn = kv_store.transaction();
let val1 = txn.get("key1").await;

// could batch the write operations
txn.put("key2", val1);
txn.put("key3", "val3");

// should see the new value of key2
assert_eq!(txn.get("key2").await, val1);

// commit, might fail on conflict, when it fails, the transaction should be rolled back
txn.commit().await?;
```

### Isolation Levels

The Snapshot Isolation have a good reputation in the industry, and is widely used in many databases. It's gives a lightweight Snapshot on each transactions started, and checks whether the writes against the snapshot has modified by others or not during the commit. It is very efficient and is considered as a good default isolation level for most of the use cases.

However, Snapshot Isolation still suffers Write Skew anomaly, for example:

1. Initially, account A has $600, the account B has $500, and the account C has $0.
2. Two transactions occur concurrently: Transaction 1 transfer $400 from account A to account C, while transaction 2 Transfer $300 from account A to account B.
1. Transaction 1 commits: account A has $200, account B has $500, account C has $400.
2. Transaction 2 commits: account A has $300, account B has $800, account C has $400.

In this case, the total amount of money in the system has increased by $100, which is a violation of the integrity constraint that the total amount of money in the system should remain constant.

To mitigate this anomaly, we could choose:

1. use Serializable Snapshot Isolation (SSI) level: increase the isolation level, and no longer allow the write skew anomaly to occur.
2. use `get_for_update()` to synchronize the critical read operations and write operations in the SI transaction.

(1) is the approach that Badger takes, it supported a lightweight SSI transaction implementation.

(2) is the approach that RocksDB takes, it does not built in SSI support, but it provides a `GetForUpdate()` method to lock the key before the write operation.

In my understanding, the pros of the RocksDB approach is mostly easy to implement and efficient, and free of floating garbage (when compares with innodb's UNDO logs approach and the Postgres's multi version rows approach). Also, in most of the time, users can make the decision by themselves about when to use `GetForUpdate()` to make the critical concurrency control correct.

However, it may produce massive writes on read intensive workloads when you hope to keep the read & write operations consistent, because `GetForUpdate()` simply transforms an read operation into a write operation, rewriting the key/value pair into WAL again. Also, some researches found that manual `GetForUpdate()` often introduces unexpected hard to diagnosis bugs, as described in the "2.2 Why Serializability" section of the [Serializable Snapshot Isolation in PostgreSQL](https://arxiv.org/pdf/1208.4179) paper:

> Given the existence of these techniques, one might question the
> need to provide serializable isolation in the database: shouldn’t users
> just program their applications to handle the lower isolation level?
> (We have often been asked this question.) Our view is that providing
> serializability in the database is an important simplification for
> application developers, because concurrency issues are notoriously
> difficult to deal with. Indeed, SI anomalies have been discovered
> in real-world applications [14]. The analysis required to identify
> potential anomalies (or prove that none exist) is complex and is
> likely beyond the reach of many users. In contrast, serializable
> transactions offer simple semantics: users can treat their transactions
> as though they were running in isolation.

As above, I think it's better to finally have SSI support in the transaction feature, but thanks to the simplicity of the RocksDB approach, we could consider to implement the RocksDB approach (Snapshot Isolation + `GetForUpdate()`) first, and then implement the SSI support after it.

With regard of the SSI implementation in Badger, it's a good reference for us about SSI could be implemented as an "additive feature" over the SI.

We'll cover the implementation details about the SI and SSI in the later section.

### Prerequisites

As described above, Snapshot and WriteBatch are considered as the prerequisites for the transaction feature.

In vanilla LevelDB, Snapshot gives the "C" part of the ACID, while WriteBatch gives the "A" and "D" parts of the ACID.

As a fork of LevelDB, all the transaction feature in RocksDB needs to do is to provide the Isolation part. We could regard Transaction as a pure "additive feature" over Snapshot and WriteBatch. In a too simplified view for the optimistic transaction implementation in RocksDB, the transaction feature could be regarded as:

1. Adding a **conflict check before writing a WriteBatch** to guarantee the transaction isolation.
2. Adding an **iterator over WriteBatch & Snapshot** to make MVCC possible.

This is also true for the Badger's implementation.

Let's discuss the prerequisites in this section, and evaluate the possible approaches we'd take in SlateDB to smooth the implementation of the transaction feature.

#### Snapshot

LevelDB and its derivatives mostly tags a sequence number to each key, and each Snapshot simply bounds with a sequence number. All the read in the Snapshot filters out the values with bigger sequence number, including a new value or a tombstone on the same key.

At the moment of writing, SlateDB still haven't have the equivant of sequence number yet, instead, we could regard a manifest + wal number to represent a consistent view of a table.

There're an in-progress RFC on exploring the design on Snapshot. We can refer to that RFC for the details later. However, we'd **assume that each key has a sequence number** in this doc, because it's considered as a common practice in LSM-tree based storage engines.

#### Write Batch

This is a sample [code snippet](https://rust.velas.com/rocksdb/struct.WriteBatch.html) of `WriteBatch` in the `rocksdb` crate:

```
use rocksdb::{DB, Options, WriteBatch};

let path = "_path_for_rocksdb_storage1";
{
    let db = DB::open_default(path).unwrap();
    let mut batch = WriteBatch::default();
    batch.put(b"my key", b"my value");
    batch.put(b"key2", b"value2");
    batch.put(b"key3", b"value3");
    db.write(batch); // Atomically commits the batch
}
```

You may notice that the `WriteBatch` API is very similar to the `Transaction` API we proposed above, the write operation from `WriteBatch` is also considered as atomic. The only differences are:

1. the `WriteBatch` is committed by `db.write(batch)`, while the `Transaction` is committed by `txn.commit()`
2. beside the write operations, the `Transaction` should also support the read operations, and if one key is updated in the `Transaction`, the read operation should return the updated value during the transaction.

The `WriteBatch` is considered as a prerequisite for the transaction feature, as it plays as a place to buffer the writes, and it makes MVCC possible with an iterator over it in front of the iterator over Snapshot. So we should implement the `WriteBatch` first before the transaction feature.

We can refer to the WriteBatch RFC or PR for the details later.

## Conflict Checking: Snapshot Isolation

The conflict checking in the SI transaction is simple:

1. Track the sequence number when transaction is started in the `Transaction` struct.
2. Track the modified keys in the `Transaction` struct.
3. When `commit()` is called, check whether the modified keys are modified by others after the transaction started.

How to verify the modified keys are modified by others after the transaction started?

We could simply check the sequence number of the keys in latest db storage. Given a key `"key1"`, let's say the sequence number of the current transaction is "102", while the sequence number of the key `"key1"` is "103" in the storage, then we could regard the key `"key1"` is modified by others after the transaction started, thus violates the conflict check and should abort the commit.

So the problem now is how to get the latest sequence number of a given key in the storage efficiently. It'd not be very fast to read the keys from the disk storage on conflict checking during every commit.

We could reference RocksDB' approach here: RocksDB assumes each transaction is short-lived, and **only checks the conflicts with the changes in recent**. In LSM, the "recent changes" are buffered in MemTable, so we can leverages MemTable to check the conflicts, making conflict checks could be regarded as a pure in-memory operation without any IO overhead.

But the size of MemTable is limited and might be flushed & rotated at any time, it can not buffer ALL the changes since the earliest running transaction. To deal with this limitation, RocksDB's solution is also very straghtforward:

1. Track the earliest sequence number of keys in the MemTable.
2. When transaction commits, if the MemTable's earliest sequence number is bigger than the transaction's sequence number, then the transaction is considered as `Expired`.

The good part of this approach is that it's very efficient and simple, and it's free of floating garbage. The bad part is that it may abort the transaction when it's not necessary, and prones to encourage the users to set a bigger MemTable size.

Given the production maturity of RocksDB & the simplicity of the implementation, we could consider to implement the conflict checking in the similar way in SlateDB.

## Conflict Checking: Serializable Snapshot Isolation