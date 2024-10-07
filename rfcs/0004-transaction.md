# SlateDB Transaction

## Background

Transaction allows multiple operations to be executed in a single atomic operation, and control the isolation level of the operations. This is an planned feature in SlateDB, and is essential for many use cases, like:

- Making reverse indexes in a table
- Ensuring data consistency in financial transfers
- Manages concurrent updates with proper isolation level

This RFC proposes the goals & design of the transaction in SlateDB.

## Goals

- Define the Transaction API in SlateDB.
- Define the expected isolation level semantics in SlateDB.
- Organize the prerequisites needed to implement Transaction feature.
- Organize the roadmap for the implementation of transaction feature.

## Non-Goals

- Pessimistic Transaction support: although it is a supported feature in rocksdb, but it is not a goal for the initial implementation. Optimistic transaction is more widely used in practice, and considered as more scalable and efficient. We'll not discuss the support of pessimistic transaction in this RFC.
- Distributed Transaction support: SlatedDB is a single-writer database, and distributed transaction is not a goal for the initial implementation. We may consider support [Two Phase Commit](https://github.com/facebook/rocksdb/wiki/Two-Phase-Commit-Implementation) in the future, which may help users coordinate between multiple SlateDB instances, but it is a separate topic and not discussed in this RFC.

## References

- [Concurrent ACID Transactions in Badger](https://dgraph.io/blog/post/badger-txn/)
- [Transactions - RocksDB](https://github.com/facebook/rocksdb/wiki/Transactions)

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
txn.put("key2", val1);
txn.put("key3", "val3");

// commit, might fail on conflict
txn.commit().await?;
```

### Isolation Level

The Snapshot Isolation have a good reputation in the industry, and is widely used in many databases. It's gives a lightweight Snapshot on each transactions started, and checks whether the writes against the snapshot has modified by others or not during the commit. It is very efficient and is considered as a good default isolation level for most of the use cases.

However, Snapshot Isolation still suffers Write Skew anomaly, for example:

1. Initially, account A has $600, the account B has $500, and the account C has $0.
2. Two transactions occur concurrently:
  * Transaction 1: Transfer $400 from account A to account C.
  * Transaction 2: Transfer $300 from account A to account B.
3. Transaction 1 commits: account A has $200, account B has $500, account C has $400.
4. Transaction 2 commits: account A has $300, account B has $800, account C has $400.

In this case, the total amount of money in the system has increased by $100, which is a violation of the integrity constraint that the total amount of money in the system should remain constant.

To mitigate this anomaly, we could choose:

1. use Serializable Snapshot Isolation (SSI) level: increase the isolation level, and no longer allow the write skew anomaly to occur.
2. use `get_for_update()` to synchronize the critical read operations and write operations in the SI transaction.

(1) is the approach that Badger takes, it supported a lightweight SSI transaction implementation.

(2) is the approach that RocksDB takes, it does not built in SSI support, but it provides a `GetForUpdate()` method to lock the key before the write operation.

This is one of the key design decisions that we need to make in this RFC. Let's evaluate what we need to do for each approach in the later part of this RFC.

### Prequsites

As described above, Snapshot and WriteBatch are considered as the prerequisites for the transaction feature.

In vanilla LevelDB, Snapshot gives the "C" part of the ACID, while WriteBatch gives the "A" and "D" parts of the ACID.

As a fork of LevelDB, all the transaction feature in RocksDB needs to do is to provide the Isolation part. In a too simplified view for the optimistic transaction implementation in RocksDB, the transaction feature could be regarded as adding a **conflict check** before writing a WriteBatch. This is also true for the Badger's implementation.

Let's discuss the prerequisites in this section, and evaluate the possible approaches we'd take in SlateDB to smooth the implementation of the transaction feature.

#### Snapshot

LevelDB and its derivatives mostly tags a sequence number to each key, and each Snapshot simply bound with a sequence number. All the read in the Snapshot filters out the values with bigger sequence number, including a new value or a tombstone on the same key.

At the moment of writing, SlateDB still haven't have the equivant of sequence number yet, instead, we could regard a manifest + wal number to represent a consistent view of a table.

There're an in-progress RFC on exploring the design on Snapshot. We can refer to that RFC for the details later. However, we could have an evaluation about using Sequence Number vs Manifest + WAL number approaches to make the implementation about Transactions easier.

#### Write Batch