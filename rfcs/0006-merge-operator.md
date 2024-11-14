# SlateDB Merge Operator

Status: Draft

Authors:

* [David Moravek](https://github.com/dmvk)

References:

* https://github.com/facebook/rocksdb/wiki/Merge-Operator
* https://github.com/facebook/rocksdb/wiki/Merge-Operator#associativity-vs-non-associativity
* https://github.com/dgraph-io/badger/blob/v4.4.0/merge.go

## Motivation

To utilize SlateDB as a fully-featured state backend for a stream processor, it must support three key types of state:

1. **Value**: Latest value or aggregating state.
2. **List / Bag / Buffering**: State that stores multiple items for later processing.
3. **Map**: Key-value state for fine-grained data.

State-of-the-art implementations built on RocksDB often leverage the [Merge Operator](https://github.com/facebook/rocksdb/wiki/Merge-Operator) to efficiently manage types 1 and 2. A prime example is list state, where elements are appended continuously until triggered by an event like a window firing. While this method may not always be the absolute optimal solution, it serves as a solid foundation for future iterations and enhancements.

## Goals
The primary goal is to introduce a new user-facing record type (**Merge Operand**) and a **Merge Operator** interface to SlateDB, allowing applications to bypass the traditional read/modify/update cycle in performance-critical situations where computation can be expressed using an [associative](https://github.com/facebook/rocksdb/wiki/Merge-Operator#associativity-vs-non-associativity) operator.

**Supporting non-associative operators is not a goal of this RFC**. The assumption is that associative operators can cover the majority of use cases while keeping the user-facing API simple and significantly reducing the scope. [Badger](https://github.com/dgraph-io/badger/blob/v4.4.0/merge.go) has adopted the same limitation.

## Public API

We will introduce two new methods for writing Merge Operands into the database. They follow the same structure as existing put methods.

```rust
impl Db {
    
    pub async fn merge(
        &self,
        key: &[u8],
        value: &[u8]
    ) -> Result<(), SlateDBError> {
	    ...
    }

    pub async fn merge_with_options(
        &self,
        key: &[u8],
        value: &[u8],
        merge_opts: &MergeOptions,
        write_opts: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        ...
    }
}

#[derive(Clone, Default)]
pub struct MergeOptions {
    pub ttl: Ttl,
}
```
We also need to introduce a new trait that will allow users to implement custom merging logic.

```rust
trait MergeOperator {

    pub fn name(&self) -> String;

    pub fn merge(
        &self,
        key: Bytes,
        existing_value: Bytes,
        value: Bytes
    ) -> Result<Bytes, MergeOperatorError>;
}
```

Initially, there will be a single optional instance of the merge operator per database. The user needs to ensure that the compactor and writer run with the same merge operator to get predictable results.

## Implementation

### Data Structures

We need to introduce an additional variant of `ValueDeletable` that denotes a **Merge Operand**. As a result of this change, we can no longer represent the deletable as `Option<Bytes>` or `Option<&[u8]>`, because a non-empty state can represent either a value or a merge operand. This requires reusing the ValueDeletable data structure throughout the stack—all the way from the memtable to the row codec.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ValueDeletable {
    ...
    Merge(Bytes)
    ...
}
```

### Row Codec

We need to slightly adjust the binary format of the SST file and introduce a new flag to denote a merge operand. Note that the `Tombstone` and `Merge` flags are mutually exclusive.

Aside from this, `Merge` rows can follow the same structure as `Value` rows.

This change is backwards compatible.

### Read Path

Merges are expected to be read using the standard `get` API.

For each key, SlateDB maintains a history of operations, which is subject to compaction.

Let's denote each operation as `OPi`. A key (`K`) that has experienced `n` changes looks like this logically (physically, the changes could be spread across the memtable and multiple SST files):

```
K:    OP1    OP2    OP3    ...    OPn

earliest --------------------> latest
```

Currently, during a read operation, SlateDB always observes the latest known state of the database. In other words, it only needs to look up the latest operation. If it's a `Value`, it simply returns it, and if it's a `Tombstone`, it returns `None`. All previous operations can be ignored and will be removed by compaction at some point.

```
K:    OP1    OP2    OP3    ...    OPn
                                   ^
                                   |
                                  GET
```

With the addition of a **Merge Operand**, it may be necessary to look backwards at previous values, up to the point where we encounter either a `Value` or a `Tombstone`. Everything beyond that point can be ignored.

```
K:    OP1    OP2    OP3    OP4    OPn
            Value  Merge  Merge  Merge
                                   ^
                                   |
                                  GET
```


In the above example, `get` should essentially return something like:

```
Merge(OP2, Merge(OP3, Merge(OP4, OPn)))
```

### Compactions

Compactions will continuously reduce the history by using the same mechanics as described above, performing partial merges along the way.