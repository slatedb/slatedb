# SlateDB Transaction

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [SlateDB Transaction](#slatedb-transaction)
  - [Background](#background)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Constraints](#constraints)
  - [References](#references)
  - [Requirements](#requirements)
    - [Snapshot](#snapshot)
    - [Write Batch](#write-batch)
    - [Isolation Levels](#isolation-levels)
  - [Other Systems](#other-systems)
    - [RocksDB](#rocksdb)
    - [Badger](#badger)
  - [Proposal](#proposal)
    - [API](#api)
    - [Conflict Checking](#conflict-checking)
    - [How to Test](#how-to-test)
  - [Roadmap](#roadmap)
  - [Updates](#updates)

<!-- TOC end -->

Status: Reviewing

Authors:

* [Li Yazhou](https://github.com/flaneur2020)

## Background

Transaction allows multiple operations to be executed in a single atomic operation, and control the isolation level of the operations. This is a planned feature in SlateDB, and is essential for many critical use cases, like:

- Using SlateDB as a metadata store or coordinator for a distributed system, such as the backend of [Kine](https://github.com/k3s-io/kine) or various metadata stores in OLAP systems.
- Using SlateDB as a underlying KV layer in an OLTP system, such as a [TiKV](https://tikv.org/) replacement or other SQL engines.

This RFC proposes the goals & design proposal of the transaction feature in SlateDB.

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

- [Transactions - RocksDB](https://github.com/facebook/rocksdb/wiki/Transactions)
- [Concurrent ACID Transactions in Badger](https://dgraph.io/blog/post/badger-txn/)
- [Transaction Internals: RocksDB](https://flaneur2020.github.io/posts/2021-08-14-rocksdb-txn/)
- [Transaction Internals: Badger](https://flaneur2020.github.io/posts/2021-08-01-badger-txn/)
- [Serializable Snapshot Isolation in PostgreSQL](https://arxiv.org/pdf/1208.4179)

## Requirements

Snapshot and WriteBatch are considered as the prerequisites for the Transaction feature. We could implement the Transaction feature upon them, with the following requirements:

1. Adding a **conflict check before writing a WriteBatch** to guarantee the transaction isolation.
2. Adding an **iterator that merges WriteBatch & Snapshot** to provide a consistent view that includes both uncommitted changes and the underlying snapshot.

In this section, we'll discuss the requirements for implementing transactions, including the necessary capabilities of Snapshots and WriteBatches, as well as the semantics of different Isolation Levels.

### Snapshot

In LSM engines, Snapshot is mostly implemented by adding a sequence number to each key, and simply let each Snapshot bounds with a sequence number. All the read in the Snapshot filters out the values with bigger sequence number, including a new value or a tombstone on the same key.

At the moment of writing, adding sequence number to each key is still WIP in SlateDB. However, we'd **assume that each key has a sequence number** in this doc, because it's considered as a common practice in LSM-tree based storage engines.

### Write Batch

This is a sample code of using `WriteBatch`:

```rust
use slatedb::{Db, WriteBatch};

let db = DB::open_with_opts(...);
let mut batch = WriteBatch::new();
batch.put(b"my key", b"my value");
batch.delete(b"key2");
batch.put(b"key3", b"value3");
db.write(batch); // Atomically commits the batch
```

You'll notice that the `WriteBatch` API is very similar to the `Transaction` API we'll propose below. The write operation from `WriteBatch` is also considered atomic. The only differences are:

1. the `WriteBatch` is committed by `db.write(batch)`, while the `Transaction` is committed by `txn.commit()`
2. beside the write operations, the `Transaction` should also support the read operations, and if one key is updated in the `Transaction`, the read operation should return the updated value during the transaction.

The `WriteBatch` is also considered as a prerequisite for the transaction feature, as it plays as a place to buffer the writes, and it makes MVCC possible with an iterator over it in front of the iterator over Snapshot.

Now WriteBatch has been implemented in SlateDB in [this PR](https://github.com/slatedb/slatedb/pull/264). We can further add the iterator over WriteBatch in front of the iterator over Snapshot while implementing the transaction feature.

### Isolation Levels

When implementing the transaction feature, we should also consider the isolation level semantics. The Snapshot Isolation (SI) and Serializable Snapshot Isolation (SSI) are the two most widely used isolation levels in the industry.

The Snapshot Isolation is widely used in many databases. It's gives a lightweight Snapshot on each transactions started, and checks whether the writes against the snapshot has modified by others or not during the commit. It is very efficient and is considered as a good default isolation level for most of the use cases.

However, Snapshot Isolation still suffers Write Skew anomaly. For example, let's assume we have two accounts, where account A has $600, and account B has $500. Also, the business rule is that Account A's balance + Account B's balance should always be >= $200.

Two transactions occur concurrently:

1. Transaction 1 first checks the total amount of balance in A and B, and then transfers $550 from account A to account C.
2. Transaction 2 first checks the total amount of balance in A and B, and then transfers $450 from account B to account D.

In this case, if the two transactions are executed concurrently, the total amount of balance in A and B might be less than $200, which violates the business rule. There's no Write-Write conflict in the two transactions, but the Read-Write conflict is not detected by Snapshot Isolation.

To mitigate this anomaly, we could choose:

1. use Serializable Snapshot Isolation (SSI) level: increase the isolation level, and no longer allow the write skew anomaly to occur.
2. use `get_for_update()` to synchronize the critical read operations and write operations in the SI transaction.

In my understanding, the pros of the (2) approach is mostly easy to implement and efficient, and free of floating garbage requires to be VACUUMed (when compares with innodb's UNDO logs approach and the Postgres's multi version rows approach). Also, in most of the time, users can make the decision by themselves about when to use `GetForUpdate()` to make the critical concurrency control correct.

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

As above, I think it's better to finally have SSI support in the transaction feature, we could consider to implement the Snapshot Isolation first, and then implement the SSI support after it.

We'll cover the implementation details about the SI and SSI in the later section.

## Other Systems

Before diving into the details of the transaction implementation in SlateDB, let's first review the transaction implementation of other LSM-tree based storage engines, to understand the possible approaches to implement the transaction feature in SlateDB.

### RocksDB

RocksDB supports both optimistic and pessimistic transactions, but we'll focus on the optimistic transaction here since it's more widely used in practice.

The key idea of RocksDB's optimistic transaction is simple: track the sequence number when transaction started, and check whether the modified keys are modified by others after the transaction started during commit.

The conflict checking in RocksDB is very straightforward:

1. Track the sequence number when transaction is started in the transaction state.
2. Track the modified keys in the transaction state.
3. When `commit()` is called, check whether the modified keys are modified by others after the transaction started.

RocksDB assumes each transaction is short-lived, and only checks the conflicts with the changes in recent. In LSM, the "recent changes" are buffered in MemTable, so RocksDB leverages MemTable to check the conflicts, making conflict checks a pure in-memory operation without any IO overhead.

However, since the size of MemTable is limited and might be flushed & rotated at any time, it cannot buffer ALL the changes since the earliest running transaction. To deal with this limitation, RocksDB's solution is simply abort the transaction as 'Expired' if the MemTable's earliest sequence number is bigger than the transaction's sequence number.

Also, RocksDB's Optimistic Transaction does not support Serializable Snapshot Isolation (SSI), because the read keys are not tracked in the transaction state.

The good part of this approach is that it's very efficient and simple, and it's free of floating garbage. The bad part is that it may abort the transaction when it's not necessary, and tends to encourage users to set a bigger MemTable size to reduce the probability of the expired transaction.

### Badger

Badger takes a different approach from RocksDB, it provides Serializable isolation level by default. The key idea is to track both read and write operations in the transaction state, and check conflicts with the recent committed transactions tracked in a global Oracle during commit.

Badger maintains a global "Oracle" which tracks the recent committed transactions. When a transaction commits, it should push itself into the Oracle's recent committed transactions list. The Oracle also maintains a sequence number generator, which is used to assign sequence numbers to transactions.

The conflict checking in Badger is as follows:

1. Track the sequence number when transaction started.
2. Track both read and write keys in the transaction state.
3. When `commit()` is called, check conflicts with the recent committed transactions in the Oracle which committed after the transaction started.
4. The conflict check verifies that: the transaction's read and write keys do not conflict with other transactions' write keys

This approach provides Serializable isolation level by detecting both Write-Write conflicts and Read-Write conflicts. It does not rely on MemTable size like RocksDB does, and provides more accurate conflict detection, without worrying about the MemTable size.

The downside is that it requires more memory to track the read keys and recent committed transactions. However, Badger mitigates this by:

1. Using u64 key fingerprints instead of full keys to track read/write keys.
2. Having an GC mechanism to clean up old committed transactions from the Oracle.

The GC is also straightforward - when all running transactions have sequence numbers greater than a committed transaction's sequence number, that committed transaction can be removed from the Oracle.

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

### Conflict Checking

The approach we choose on conflict checking is expected to be more similar to Badger's global `Oracle` approach, since we hope to provide SSI in the transaction feature, and it's less prone to unnecessarily abort the transaction with limited MemTable size.

Below is the data structure of the global `Oracle` and the transaction state:

```rust
struct Oracle {
    next_seq: AtomicU64,
    recent_committed_txns: Deque<Arc<TransactionState>>,
}

struct TransactionState {
    started_seq: u64,
    write_keys: HashSet<Bytes>,
    read_keys: Vec<Range<Bytes>>,
    committed_seq: Option<u64>
}
```

`Oracle` is considered a global singleton in the DB, and it tracks the recent committed transactions in the `recent_committed_txns` deque. When a transaction commits, it should push itself into the `recent_committed_txns` deque, and the entries in `recent_committed_txns` can be GCed after they are no longer needed. Also, when a `TransactionState` is tracked in the `recent_committed_txns`, only the write keys need to be tracked, while `read_keys` should always be empty.

Besides the started sequence number of the transaction, we also need to track the sequence number of the transaction committed. During the execution of a transaction, some other transactions might commit, and the current transaction should check the conflicts with these committed transactions.

The conflict check simply checks whether the Read & Write keys of the current transaction are overlapped with the Write keys from related recent committed transactions.

Given an execution history of transactions like:

```
txn1: [seq:97] <--- ---- ---> [seq:99]
txn2:     [seq:98]  <--- ---- ---- ---- ---- ---- ---> [seq:105]
txn3:                         [seq:102] <--- ---> [seq:104]
txn4:                    [seq:101] <--- ---- ---- ---- ---> [seq:106]
```

Let's say the current transaction is `txn4`, it should check conflicts with `txn2` and `txn3` during the commit, because `txn2` and `txn3` are committed after `txn4` started. During the conflict checking, `txn4`'s Read & Write keys should not conflict with the Write keys of `txn2` and `txn3`.

It does not need to check conflicts with `txn1`, because `txn1` is committed before `txn4` started. This could tell us when to GC the entry in the `recent_committed_txns` deque: when ALL the running transactions' sequence number are bigger than the committed sequence number of the entry, then the entry could be GCed.

Please note that `read_keys` are represented as a set of `Range<Bytes>`. This allows us to efficiently track ranges of keys that were read during the transaction, and ensure Serializable Snapshot Isolation (SSI) on range conflicts. For example, if transaction A reads a range `["key01", "key10"]` and transaction B writes to `"key05"` which falls within that range, we can detect this conflict and abort one of the transactions to maintain serializability.

This requires we track the original write keys in the `TransactionState`, instead of the key fingerprints like Badger does. We could reuse the buffers of the original keys in WriteBatch to reference the write keys with slices of `Bytes`.

The non-transactioned writes operations should also be tracked in the `TransactionState` and `recent_committed_txns` for conflict checking. They could be regarded as a transaction that only contains a single write operation, and the start sequence number of this transaction is same as the committed sequence number.

This approach can be adjusted to support SI as well by only tracking the keys that are being written to. When user set the isolation level to SI, the `read_keys` could always be set as empty. The GC mechanism to release inactive transactions is still the same as the SSI.

### How to Test

Testing transaction implementations requires verifying both correctness and isolation guarantees.

To test the transaction feature, we could write some test cases that cover the following aspects:

1. Basic transaction operations.
2. Isolation guarantees for both SI and SSI modes.
3. Edge cases and concurrency control.

We could consider to port the test cases from [RocksDB](https://github.com/facebook/rocksdb/blob/3495c94761629404f5476f732e2130134cfcc51b/utilities/transactions/optimistic_transaction_test.cc), [Badger](https://github.com/dgraph-io/badger/blob/36c461a435c53a8a81e7377c2b026b24d37eee0c/txn_test.go) to have a comprehensive coverage of the above aspects.

## Roadmap

1. Implement the WriteBatch API (related with https://github.com/slatedb/slatedb/pull/264).
2. Add sequence to each key in the storage, and implement the Snapshot API.
3. Add the conflict checking in the transaction, implement the SI transaction.
4. Add the global Oracle to track the recent committed transactions, implement the SSI transaction.

## Updates

- 2024-10-13: Initial draft.
- 2024-10-20: Add a simple roadmap.
- 2024-11-17: Restructure the doc to align with the latest implementation.