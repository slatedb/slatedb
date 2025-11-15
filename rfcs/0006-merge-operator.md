# SlateDB Merge Operator

Status: Draft

Authors:

* [David Moravek](https://github.com/dmvk)

References:

* https://github.com/facebook/rocksdb/wiki/Merge-Operator
* https://github.com/facebook/rocksdb/wiki/Merge-Operator#associativity-vs-non-associativity
* https://github.com/dgraph-io/badger/blob/v4.4.0/merge.go
* https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/#using-keyed-state
* https://kafka.apache.org/25/javadoc/org/apache/kafka/streams/state/KeyValueStore.html
* https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/state/State.html
* https://github.com/facebook/rocksdb/wiki/Merge-Operator-Implementation#compaction

## Motivation

To utilize SlateDB as a fully-featured state backend for a stream processor, it must support three key types of state:

1. **Value**: Latest value or aggregating state.
2. **List / Bag / Buffering**: State that stores multiple items for later processing.
3. **Map**: Key-value state for fine-grained data.

Examples from the stream processing ecosystem:
* [Apache Flink](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/#using-keyed-state)
* [Apache Kafka Streams](https://kafka.apache.org/25/javadoc/org/apache/kafka/streams/state/KeyValueStore.html)
* [Apache Beam](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/state/State.html)

State-of-the-art implementations built on RocksDB often leverage the [Merge Operator](https://github.com/facebook/rocksdb/wiki/Merge-Operator) to efficiently manage types 1 and 2. A prime example is list state, where elements are appended continuously until triggered by an event like a window firing. While this method may not always be the absolute optimal solution, it serves as a solid foundation for future iterations and enhancements.

## Goals

The primary goal is to introduce a new user-facing record type (**Merge Operand**) and a **Merge Operator** interface to SlateDB, allowing applications to bypass the traditional read/modify/update cycle in performance-critical situations where computation can be expressed using an [associative](https://github.com/facebook/rocksdb/wiki/Merge-Operator#associativity-vs-non-associativity) operator.

**Supporting non-associative operators is not a goal of this RFC**. The assumption is that associative operators can cover the majority of use cases while keeping the user-facing API simple and significantly reducing the scope. [Badger](https://github.com/dgraph-io/badger/blob/v4.4.0/merge.go) has adopted the same limitation.

## Proposed Solution

We will introduce a new record type (**Merge Operand**) and a **Merge Operator** interface to SlateDB, allowing applications to bypass the traditional read/modify/update cycle in performance-critical situations where computation can be expressed using an associative operator.

This will allows users to express partial updates to an accumulator using an associative operator, which will be merged during reads/compactions to produce the final result.

This plays quite nice with existing record types, as merge operands can be interleaved with values and tombstones, and will be correctly merged during reads/compactions. Both tombstone and value act as a barrier that clears any previous merge history for the same key.

This approach is similar to how RocksDB and Badger handle merge operations. We're not inventing anything new here, but rather combining ideas from these projects to fit our use case. The main innovation that is not present in either RocksDB or Badger is the handling of TTLs.

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
        write_opts: &WriteOptions
    ) -> Result<(), SlateDBError> {
        ...
    }
}

impl WriteBatch {

    pub fn merge(
        &self,
        key: &[u8],
        value: &[u8]
    ) -> Result<(), SlateDBError> {
	    ...
    }

    pub fn merge_with_options(
        &self,
        key: &[u8],
        value: &[u8],
        merge_opts: &MergeOptions
    ) -> Result<(), SlateDBError> {
        ...
    }
}

#[derive(Clone, Default)]
pub struct MergeOptions {
    /// Merges with different TTLs will only be merged together during read but will be stored separately 
    /// otherwise to allow them to expire independently. This ensures proper TTL handling while still
    /// allowing merge operations to work correctly.
    pub ttl: Ttl,
}
```

### Merge Operator Interface

We also need to introduce a new trait that will allow users to implement custom merging logic.

```rust
trait MergeOperator {

    pub fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes
    ) -> Result<Bytes, MergeOperatorError>;

    pub fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        // Default implementation: pairwise merging (backward compatible)
        let mut result = existing_value;
        for operand in operands {
            result = Some(self.merge(key, result, operand.clone())?);
        }
        result.ok_or(MergeOperatorError::EmptyBatch)
    }
}
```

Initially, the implementation is limited to a single optional merge operator per database. The user must ensure that both the compactor and writer use the same merge operator to guarantee correct results.

The merge operator can be implemented to route to different merge strategies based on key prefixes. Here's an example:

```rust
struct MyMergeOperator;

impl MergeOperator for MyMergeOperator {

    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes
    ) -> Result<Bytes, MergeOperatorError> {
        if key.starts_with(b"list:") {
            // concat values
            match existing_value {
                Some(existing) => {
                    let mut result = existing.to_vec();
                    result.extend_from_slice(&value);
                    Ok(Bytes::from(result))
                }
                None => Ok(value)
            }
        } else if key.starts_with(b"counter:") {
            // add values
            let existing_num = existing_value
                .map(|v| u64::from_le_bytes(v.as_ref().try_into().unwrap()))
                .unwrap_or(0);
            let new_num = u64::from_le_bytes(value.as_ref().try_into().unwrap());
            Ok(Bytes::copy_from_slice(&(existing_num + new_num).to_le_bytes()))
        } else {
            Err(...)
        }
    }

    // Optional: Override merge_batch for better performance
    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        if key.starts_with(b"counter:") {
            // For counters, we can sum all operands at once (O(1) instead of O(N))
            let sum = existing_value
                .map(|v| u64::from_le_bytes(v.as_ref().try_into().unwrap()))
                .unwrap_or(0)
                + operands.iter()
                    .map(|b| u64::from_le_bytes(b.as_ref().try_into().unwrap()))
                    .sum::<u64>();
            Ok(Bytes::copy_from_slice(&sum.to_le_bytes()))
        } else {
            // For other prefixes, use default pairwise merging
            let mut result = existing_value;
            for operand in operands {
                result = Some(self.merge(key, result, operand.clone())?);
            }
            result.ok_or(MergeOperatorError::EmptyBatch)
        }
    }
}
```

It's **user's responsibility to ensure** that the merge operator is correctly implemented and **the same instance of the operator is used across all components** (compactor, writer, garbage collector).

#### Performance Optimization: Overriding `merge_batch`

While the default `merge_batch` implementation provides backward compatibility by calling `merge` pairwise, users may optionally override `merge_batch` to improve performance and reduce intermediate memory allocations. For example, a counter merge operator can sum all operands in a single pass instead of performing N-1 merge operations, avoiding the creation of N-1 intermediate `Bytes` objects. This optimization is particularly beneficial when processing large batches of operands, as it can reduce both function call overhead and memory allocations for certain use cases.

### Merge Operator Configuration

User can provide their own merge operator implementation via `DbOptions`. If not provided, the database will not support merge operations.

```rust
impl DbOptions {

    #[serde(skip)]
    /// The merge operator to use for the database. If not set, the database will not support merge operations.
    pub merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>,
}
```

### Extending TTL support

The last public API change is extending the `Ttl` enum to support a new `ExpireAt(ts)` variant. This allows users to set a specific expiration time for a key, which overrides the default TTL behavior.

```rust
pub enum Ttl {
    ...
    ExpireAt(i64),
}
```

See [TTL Handling with Merge Operations](#ttl-handling-with-merge-operations) for more details.

## Implementation Details

### Internal Data Structures

We need to introduce an additional variant of `ValueDeletable` that denotes a **Merge Operand**. As a result of this change, we can no longer represent the deletable as `Option<Bytes>` or `Option<&[u8]>`, because a non-empty state can represent either a value or a merge operand. This requires reusing the ValueDeletable data structure throughout the stack—all the way from the memtable to the row codec.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ValueDeletable {
    ...
    Merge(Bytes)
    ...
}
```

### WAL / Memtable

The WAL and memtable share a common underlying data structure (`WritableKVTable`). To support merge operations, we need to extend this structure to handle both merge operations and value overrides:

- For merge operations, we need to maintain multiple entries per key in order to preserve the sequence of merge operands
- For put and delete operations, we continue to override any previous values for that key

The `WritableKVTable` must preserve the order of operations to enable proper reconstruction of the merge sequence during reads, while still allowing puts and deletes to clear the merge history. Maintaining this hybrid approach is critical for handling merge-related error cases, such as:

1. If merging operands fails (e.g. due to incompatible data formats), we need the full history to potentially retry with different merge strategies or fall back to the last known good value
2. If merge operands have different TTLs, we need to track the individual expiry times and may need to fall back to earlier values when operands expire

In both cases, maintaining the operation history allows us to handle these edge cases gracefully rather than losing data.

### Row Codec

We need to slightly adjust the binary format of the SST file and introduce a new flag to denote a merge operand. Note that the `Tombstone` and `Merge` flags are mutually exclusive.

Aside from this, `Merge` rows can follow the same structure as `Value` rows.

This change is backwards compatible.

### Block Format

The block format remains unchanged. It's useful to highlight that merge operands are also stored in the bloom filters, so they have the same filtering guarantees as regular values.

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

### Write Path & Compactions

During compaction, we continuously reduce the history by merging entries according to the rules described above. This process performs partial merges along the way, combining merge operands with their base values when possible. We also merge entries in the memtable as they arrive to minimize unnecessary history storage.

Compaction operates on a set of source SST files and writes to a destination SST file. The process reads all source SSTs sequentially and produces a single destination SST containing the merged results.

The table below shows how different combinations of previous and new values are handled during compaction:

| Previous  | New       | Result    |
|-----------|-----------|-----------|
| tombstone | value     | value     |
| tombstone | merge     | value     |
| tombstone | tombstone | tombstone |
| value     | merge     | value     |
| value     | value     | value     |
| value     | tombstone | tombstone |
| merge     | merge     | merge     |
| merge     | value     | value     |
| merge     | tombstone | tombstone |

#### Snapshot Isolation

*This part assumes that [Transactions RFC](https://github.com/slatedb/slatedb/pull/260) is already in place.*

Compaction must preserve snapshot isolation to avoid affecting externally observable state. RocksDB provides a [proven solution to this problem](https://github.com/facebook/rocksdb/wiki/Merge-Operator-Implementation#compaction) that we can adopt.

The key insight is that merging must stop whenever we encounter a snapshot barrier, as this represents a point where a transaction may be reading from. Below is an adapted example from RocksDB's documentation showing how this works in practice, where the `+` symbol represents a merge operation:

```
K:    0    +1    +2    +3    +4     +5      2     +1     +2
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

We show it step by step, as we scan from the newest operation to the oldest operation

K:    0    +1    +2    +3    +4     +5      2    (+1     +2)
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

A Merge operation consumes a previous Merge operation and produces a new Merge operation
      (+1  +2) => Merge(1,2) => +3

K:    0    +1    +2    +3    +4     +5      2            +3
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

K:    0    +1    +2    +3    +4     +5     (2            +3)
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

A Merge operation consumes a previous Put operation and produces a new Put operation
      (2   +3) =>  Merge(2, 3) => 5

K:    0    +1    +2    +3    +4     +5                    5
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

A newly produced Put operation hides any previous operations up to the snapshot barrier
      (+5   5) => 5

K:    0    +1    +2   (+3    +4)                          5
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

(+3  +4) => Merge(3,4) => +7

K:    0    +1    +2          +7                           5
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

A Merge operation cannot consume a previous snapshot barrier
       (+2   +7) can not be combined

K:    0   (+1    +2)         +7                           5
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

(+1  +2) => Merge(1,2) => +3

K:    0          +3          +7                           5
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

K:   (0          +3)         +7                           5
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3

(0   +3) => Merge(0,3) => 3

K:               3           +7                           5
                 ^           ^                            ^
                 |           |                            |
              snapshot1   snapshot2                   snapshot3
```

### TTL Handling with Merge Operations

#### Supported TTL Behaviors 

SlateDB supports two TTL approaches:

1. **Operation-Level TTL**
   - Each operation (put/merge) has its own independent TTL, specified via:
     - `Ttl::ExpireAfter(duration)`: Expires after specified duration (internally this is implemented as `ExpireAt(Instant::now() + duration)`)
     - `Ttl::ExpireAt(timestamp)`: Expires at specified timestamp
   - Enables per-element expiration in collections

2. **TTL Renewal (NOT SUPPORTED NATIVELY)**
   - Refreshing TTL for entire key would require READ + MODIFY + UPDATE
   - Use standard `PUT` API if this behavior is needed

#### TTL Storage and Expiration

When merging values with different TTLs, the merge operation only combines values during reads while keeping entries separate in storage, allowing independent expiration per TTL.

Unlike regular values which become tombstones upon expiration, expired merge entries are simply removed, enabling per-element expiration in collections.

Users can implement custom TTL patterns by consistently using either `ExpireAt` or `ExpireAfter` across operations.

### Ordering Guarantees

If there are multiple merge operands available for the same key, they will be ordered by their corresponding sequence numbers.

### Error Handling

Merge operations can fail for several reasons:

* The user-defined merge operator returns an error during execution
* No merge operator is configured for the database

These failures can occur in three different code paths:

1. **Write Path**
   - During merging of entries into the WAL or memtable
   - On failure: Unmerged operands are preserved in the WAL/memtable and retry occurs on next startup
2. **Read Path** 
   - When merging entries during a read operation
   - On failure: Error is propagated to the user
3. **Compaction Path**
   - When the user-defined merge operator fails during compaction
   - On failure: Unmerged operands are preserved in the SST file and retry occurs on next compaction

## Performance and Benchmarks

We plan to validate the merge operator implementation with the following benchmarks focused on key use cases:

### List Buffering Workload
- Append N elements to a list using merge operations and reading the final result vs read-modify-write
- Compare performance for different value sizes
- Expectation: Merge operations should show significant performance improvements over read-modify-write

### Aggregation Workload  
- Increment counters using merge operations vs read-modify-write
- Compare performance for different value sizes
- Expectation: Merge operations should be faster than read-modify-write against cached records and should show significant improvements over read-modify-write for uncached records.

The benchmarks will be implemented and maintained as part of the existing benchmark suite to validate the merge operator performance characteristics. No benchmark results are available yet as the feature is still under development.

We expect the merge operator to show significant benefits for buffering use cases by eliminating read-modify-write cycles. The actual performance improvements will be quantified once the implementation is complete.

## Possible Follow-up Work

### Multiple Merge Operators

The currently proposed implementation allows for a single merge operator per database. While this may seem limiting, the current interfaces allow implementing support for multiple operators in userspace by routing to different implementations based on key prefixes or other criteria. This approach aligns with RocksDB's design philosophy.

If compelling use cases emerge that demonstrate clear benefits of first-class support for multiple merge operators, we can consider adding this capability in a future iteration. However, the current single-operator design provides a good balance of simplicity and flexibility for most use cases.

### Persisting GET results

One possible optimization is to introduce a new read option for persisting the result of a GET operation when it was merged from multiple operands. This would allow for faster subsequent GETs by avoiding the need to recompute the merge. The result could be stored as a regular value in the memtable or an SST file. Although this optimization sounds appealing, it's not clear whether the benefits outweigh the added complexity and storage overhead. For certain use cases, such as buffering, it might have a negative performance impact.

### Read Options for faster termination

It might be useful to have a read option that allows for early termination of merge operations. This could be beneficial for buffering use cases where the user wants to limit the number of operands that are merged together (effectively allowing partial iterations).

RocksDB provides an alternative approach by exposing `GetMergeOperands` which allows listing the unmerged operands directly.