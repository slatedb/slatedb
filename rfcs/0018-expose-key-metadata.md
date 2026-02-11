# Expose Row Information

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Expose Row Information](#expose-row-information)
  - [Summary](#summary)
  - [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Use Cases](#use-cases)
    - [Durability Guarantees](#durability-guarantees)
    - [TTL Management](#ttl-management)
  - [Design](#design)
    - [1. Row Query Interface](#1-row-query-interface)
    - [2. Modify Put/Write Return Types](#2-modify-putwrite-return-types)
    - [3. Support Query by Version](#3-support-query-by-version)
  - [Impact Analysis](#impact-analysis)
  - [Testing](#testing)
  - [Alternatives](#alternatives)

<!-- TOC end -->

Status: Approved

Authors:

* [marsevilspirit](https://github.com/marsevilspirit)

<!-- TOC --><a name="summary"></a>
## Summary

This RFC proposes three related API improvements:

1.  **Modify Write API Return Types**: Change the return type of `db.put()`, `db.delete()`, `db.merge()`, and `db.write()` (including their `_with_options` variants) from `Result<(), ...>` to `Result<WriteHandle, ...>` to return a write handle containing the assigned sequence number and timestamps. This design allows for future extensions such as `await_durability()`.
2.  **New Row Query Interface**: Introduce `get_row()` and `get_row_with_options()` for single-key row queries that return `RowEntry`. For scans, add `DbIterator::next_row()` which returns `RowEntry` (including metadata: sequence number, creation timestamp, expiration timestamp). The existing `DbIterator::next()` continues to return `KeyValue` for backward compatibility.
3.  **Support Query by Version**: Add a `seqnum` option to `SnapshotOptions`, enabling users to create snapshots at specific sequence numbers and read historical versions of keys.

<!-- TOC --><a name="motivation"></a>
## Motivation

In practical applications, users often need to retrieve metadata information for keys. Currently, SlateDB has the following pain points:

**1. Unable to Query Row Information and Metadata**

Users need to obtain metadata such as TTL, sequence number, and creation time, but currently **no API provides this information**—even the `get()` method only returns the value itself.

**2. Lack of Feedback for Write Operations**

Currently, `db.put()` returns `Result<(), Error>`. Callers cannot know the sequence number assigned to the write operation, which is crucial for debugging, auditing, and MVCC scenarios.

**3. Lack of Versioned Query Capability**

Users cannot query a specific historical version of a key using a sequence number, which limits application scenarios related to Multi-Version Concurrency Control (MVCC).

<!-- TOC --><a name="goals"></a>
## Goals

- Enable retrieval of complete row information including metadata (supporting both single-key queries and range scans).
- Return the sequence number assigned after a write operation (put, delete, merge, or batch write).
- Support reading historical versions of a key by specifying a sequence number.

<!-- TOC --><a name="non-goals"></a>
## Non-Goals

- No change/modification for underlying behavior of any API.

<!-- TOC --><a name="use-cases"></a>
## Use Cases

<!-- TOC --><a name="durability-guarantees"></a>
### Durability Guarantees

Applications requiring strong durability guarantees can use the sequence number to verify that writes have been persisted:

```rust
// Write with durability tracking
let handle = db.put(b"critical_data", value).await?;
let write_seqnum = handle.seqnum();

// All writes with sequence numbers <= write_seqnum are guaranteed to be durable
// Applications can use this for replication, backup, or recovery checkpoints
checkpoint_manager.record_durable_seqnum(write_seqnum).await?;
```

<!-- TOC --><a name="ttl-management"></a>
### TTL Management

Applications can query how long until a key expires using `get_row()`:

```rust
// Query remaining TTL for a key
if let Some(entry) = db.get_row(b"session:abc123").await? {
    if let Some(expire_ts) = entry.expire_ts() {
        let ttl_seconds = (expire_ts - current_time_micros()) / 1_000_000;
        println!("Key expires in {} seconds", ttl_seconds);
    }
}
```

<!-- TOC --><a name="design"></a>
## Design

<!-- TOC --><a name="1-row-query-interface"></a>
### 1. Row Query Interface

> [!NOTE]
> This change introduces new row query APIs that reuse the existing `RowEntry` type for returning complete row information. Since decoding metadata requires decoding the entire row, there is no performance penalty for returning the full row data.

**Public API**:

```rust
// Get complete row information for a single key
pub async fn get_row<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<RowEntry>, crate::Error>;

// Get row with options (new)
pub async fn get_row_with_options<K: AsRef<[u8]>>(
    &self,
    key: K,
    options: &ReadOptions,
) -> Result<Option<RowEntry>, crate::Error>;

// Scan API (unchanged) - returns DbIterator
pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
where
    K: AsRef<[u8]> + Send,
    T: RangeBounds<K> + Send;

// DbIterator methods
impl DbIterator {
    // Returns KeyValue for normal scan usage (default behavior)
    pub async fn next(&mut self) -> Result<Option<KeyValue>, crate::Error>;
    
    // Returns complete row information including metadata
    pub async fn next_row(&mut self) -> Result<Option<RowEntry>, crate::Error>;
}
```

> [!NOTE]
> **Implementation Note**: Internally, the existing `next_key_value()` method is renamed to `next_row()` and modified to return `RowEntry`. The public `next()` method calls `next_row()` internally, extracts the `KeyValue` from the `RowEntry`, and returns it.

```rust
// RowEntry is already public; fields are pub(crate) with getter methods
pub struct RowEntry {
    pub(crate) key: Bytes,
    pub(crate) value: ValueDeletable,
    pub(crate) seq: u64,
    pub(crate) create_ts: Option<i64>,
    pub(crate) expire_ts: Option<i64>,
}

impl RowEntry {
    pub fn key(&self) -> &[u8] { &self.key }
    pub fn value(&self) -> &ValueDeletable { &self.value }
    pub fn seq(&self) -> u64 { self.seq }
    pub fn create_ts(&self) -> Option<i64> { self.create_ts }
    pub fn expire_ts(&self) -> Option<i64> { self.expire_ts }
}

// ValueDeletable type definition (unchanged)
pub enum ValueDeletable {
    Value(Bytes),      // Regular value
    Merge(Bytes),      // Merge operation value
    Tombstone,         // Deleted key marker
}
```

> [!IMPORTANT]
> **ValueDeletable Behavior in `next_row()`**
> 
> While `ValueDeletable` has three variants (`Value`, `Merge`, `Tombstone`), **neither `next()` nor `next_row()` will return entries with `Tombstone`** in practice:
> - **Tombstones are filtered out** by the underlying iterator
> - `next()` returns `KeyValue` (extracts the value from `ValueDeletable`, so users don't need to match on it)
> - `next_row()` returns `RowEntry`, where users will only see `ValueDeletable::Value` or `ValueDeletable::Merge`
> - For merge operations: if a base value exists, returns `Value` with the computed result; otherwise returns `Merge` with the final merge value

**Usage Example**:

```rust
// Get complete row information
let row = db.get_row(b"key").await?;
if let Some(entry) = row {
    println!("Key: {:?}", entry.key());
    println!("Seq: {}, Created: {:?}, Expires: {:?}", 
             entry.seq(), entry.create_ts(), entry.expire_ts());
    
    // Handle ValueDeletable variants (Tombstone will never appear here)
    match entry.value() {
        ValueDeletable::Value(v) => println!("Value: {:?}", v),
        ValueDeletable::Merge(m) => println!("Merge value: {:?}", m),
        ValueDeletable::Tombstone => unreachable!("get_row() never returns Tombstone"),
    }
}

// Normal scan usage - returns KeyValue (no need to match on ValueDeletable)
let mut iter = db.scan(b"a"..b"z").await?;
while let Some((key, value)) = iter.next().await? {
    println!("Key: {:?}, Value: {:?}", key, value);
}

// Scan with complete row information - returns RowEntry
let mut iter = db.scan(b"a"..b"z").await?;
while let Some(row_entry) = iter.next_row().await? {
    match row_entry.value() {
        ValueDeletable::Value(v) => {
            println!("Key: {:?}, Value: {:?}, Seq: {}", 
                     row_entry.key(), v, row_entry.seq());
        }
        ValueDeletable::Merge(m) => {
            println!("Key: {:?}, Merge: {:?}, Seq: {}", 
                     row_entry.key(), m, row_entry.seq());
        }
        ValueDeletable::Tombstone => unreachable!(),
    }
}
```

**Implementation Details**:

1.  **Add `DbIterator::next_row()` Method**:
    - Rename the existing internal `next_key_value()` method to `next_row()` and make it public, with return type `Result<Option<RowEntry>, crate::Error>`.
    - The public `next()` method calls `next_row()` internally, extracts `KeyValue` from the `RowEntry`, and returns it (maintaining backward compatibility).
    - Since decoding metadata requires decoding the entire row, there is no performance penalty for providing both methods.

2.  **`RowEntry.value` Behavior**:
    - `RowEntry.value` remains as `ValueDeletable` type (no type changes needed).
    - Both `next()` and `next_row()` will **never** return entries with `Tombstone` values:
      - The underlying iterator already filters out tombstones.
    - For **Merge Operations**:
      - If merge operator is configured and merges are resolved, returns `ValueDeletable::Value` with the computed result.
      - If no base value exists for merges, returns `ValueDeletable::Merge` with the final merge value.
      - This is consistent with how `KeyValueIterator::next()` handles merges (see line 42-44 in iter.rs).
    - `next()` extracts the value from `ValueDeletable` and returns it as `Bytes` in the `KeyValue` tuple, so normal scan usage doesn't need to match on `ValueDeletable`.

3.  **Implement `get_row()` Using `scan()`**:
    - `get_row(key)` internally calls `scan(key..=key)` to get a `DbIterator`.
    - Then calls `next_row()` on the iterator to get the `RowEntry`.
    - This reuses existing scan logic and avoids code duplication.

4.  **Multi-Version Behavior**: Both `next()` and `next_row()` return the **latest visible version** of each key. If a key has multiple versions at different sequence numbers, only the version visible at the current time (or specified snapshot sequence number) is returned.

<!-- TOC --><a name="2-modify-putwrite-return-types"></a>
### 2. Modify Put/Write Return Types

> [!WARNING]
> **Breaking Change**: All existing code using `db.put`, `db.delete`, `db.merge`, and `db.write` (and their `_with_options` variants) must be updated to handle the new return type.

**WriteHandle Structure**:

```rust
/// Handle returned from write operations, containing metadata about the write.
/// This structure is designed to be extensible for future enhancements.
pub struct WriteHandle {
    /// The sequence number of this entry.
    seq: u64,
    /// The creation timestamp (if set).
    create_ts: Option<i64>,
    /// The expiration timestamp (if set).
    expire_ts: Option<i64>,
}

impl WriteHandle {
    /// Returns the sequence number assigned to this write operation.
    pub fn seqnum(&self) -> u64 {
        self.seq
    }
    
    /// Returns the creation timestamp assigned to this write operation.
    pub fn create_ts(&self) -> Option<i64> {
        self.create_ts
    }
    
    /// Returns the expiration timestamp assigned to this write operation.
    pub fn expire_ts(&self) -> Option<i64> {
        self.expire_ts
    }
    
    // Future extensions can be added here, for example:
    // pub async fn await_durability(&self) -> Result<(), Error> { ... }
}
```

**Single Key Operations**:

```rust
// Old interface
pub async fn put<K, V>(&self, key: K, value: V) -> Result<(), crate::Error>
pub async fn delete<K>(&self, key: K) -> Result<(), crate::Error>
pub async fn merge<K, V>(&self, key: K, value: V) -> Result<(), crate::Error>

// New interface. Returns WriteHandle with the assigned sequence number.
pub async fn put<K, V>(&self, key: K, value: V) -> Result<WriteHandle, crate::Error>
pub async fn delete<K>(&self, key: K) -> Result<WriteHandle, crate::Error>
pub async fn merge<K, V>(&self, key: K, value: V) -> Result<WriteHandle, crate::Error>
```

**Batch Operations**:

```rust
// Old interface
pub async fn write(&self, batch: WriteBatch) -> Result<(), crate::Error>

// New interface. Returns WriteHandle with the batch commit sequence number.
pub async fn write(&self, batch: WriteBatch) -> Result<WriteHandle, crate::Error>
```

**Transaction Commit Operations**:

```rust
// Old interface
pub async fn commit(self) -> Result<(), crate::Error>
pub async fn commit_with_options(self, options: &WriteOptions) -> Result<(), crate::Error>

// New interface. Returns WriteHandle with the commit sequence number.
pub async fn commit(self) -> Result<WriteHandle, crate::Error>
pub async fn commit_with_options(self, options: &WriteOptions) -> Result<WriteHandle, crate::Error>
```

**Migration Example**:

```rust
// Single key operation - get sequence number and timestamps
let handle = db.put(b"key", b"value").await?;
println!("Seq: {}", handle.seqnum());
println!("Created at: {:?}", handle.create_ts());
println!("Expires at: {:?}", handle.expire_ts());

// Batch operation
let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"key1");

let handle = db.write(batch).await?;
println!("Batch committed at seq: {}", handle.seqnum());
// All operations in the batch share this sequence number and timestamps

// If you need the full RowEntry, use get_row with the seqnum
let row = db.get_row(b"key").await?;
if let Some(entry) = row {
    assert_eq!(entry.seq(), handle.seqnum());
    assert_eq!(entry.create_ts(), handle.create_ts());
    assert_eq!(entry.expire_ts(), handle.expire_ts());
}

// Option 2: Ignore return values (if you don't need the metadata)
let _ = db.put(b"key", b"value").await?;

let mut batch2 = WriteBatch::new();
batch2.put(b"another_key", b"another_value");
let _ = db.write(batch2).await?;
```

**Implementation Details**:

- Modify `DbInner::write_with_options` to return `WriteHandle` (containing the assigned `commit_seq`, `create_ts`, and `expire_ts`).
- `put()`, `delete()`, and `merge()` return the `WriteHandle` received from `DbInner`.
- Both `put`, `delete`, and `merge` operations share the same underlying write pipeline.
- `WriteHandle` contains the sequence number, creation timestamp, and expiration timestamp assigned to the write operation.
- The `create_ts` and `expire_ts` are assigned at the same time as the `commit_seq` during write batch processing.
- **Note on Transactions**: Within a transaction, the individual write operations (`put`, `delete`, `merge`) do not return `WriteHandle` because sequence numbers are not known during transaction execution. However, `DbTransaction::commit()` and `commit_with_options()` **do** return `WriteHandle`, allowing users to access the commit sequence number and timestamps assigned when the transaction is successfully committed.

<!-- TOC --><a name="3-support-query-by-version"></a>
### 3. Support Query by Version

Add a `seqnum` field to `SnapshotOptions` to create snapshots at specific sequence numbers:

```rust
pub struct SnapshotOptions {
    pub seqnum: Option<u64>,  // New: create snapshot at specific sequence number
}

impl SnapshotOptions {
    pub fn read_at(self, seq: u64) -> Self {
        Self { seqnum: Some(seq), ..self }
    }
}

// New snapshot API
pub fn snapshot_with_options(&self, options: SnapshotOptions) -> Result<DbSnapshot, Error>;
```

**Usage Example**:

```rust
// Write and get version number
let handle = db.put(b"key", b"value1").await?;
let seq1 = handle.seqnum();

// Subsequent update
db.put(b"key", b"value2").await?;

// Create snapshot at seq1
let snapshot = db.snapshot_with_options(SnapshotOptions::default().read_at(seq1))?;

// Current value
let current_value = db.get(b"key").await?;
assert_eq!(current_value, Some(Bytes::from("value2")));

// Read old version via snapshot
let old_value = snapshot.get(b"key").await?;
assert_eq!(old_value, Some(Bytes::from("value1")));
```

Snapshots created with `seqnum` can be used with all read operations:

**Usage Example**:

```rust
// Write some data
let handle1 = db.put(b"key1", b"value1_v1").await?;
let seq1 = handle1.seqnum();
db.put(b"key2", b"value2_v1").await?;

let handle2 = db.put(b"key1", b"value1_v2").await?;
let seq2 = handle2.seqnum();
db.put(b"key2", b"value2_v2").await?;

// Create snapshot at seq1
let snapshot = db.snapshot_with_options(SnapshotOptions::default().read_at(seq1))?;

// Normal scan at historical version via snapshot
let mut iter = snapshot.scan(b"key1"..=b"key2").await?;
while let Some((key, value)) = iter.next().await? {
    // Will return value1_v1 and value2_v1
    println!("Key: {:?}, Value: {:?}", key, value);
}

// Scan with complete row information at historical version
let mut iter = snapshot.scan(b"key1"..=b"key2").await?;
while let Some(row_entry) = iter.next_row().await? {
    println!("Key: {:?}, Seq: {}, Value: {:?}", 
             row_entry.key(), row_entry.seq(), row_entry.value());
}
```

**Implementation Details**:

- **Snapshot-Based Versioning**: Extend the existing `Snapshot` mechanism to support creating snapshots at specific sequence numbers via `SnapshotOptions::seqnum`.

- **Version Retention and Availability**:
  > [!IMPORTANT]
  > **Understanding Historical Version Availability**
  > 
  > Historical versions are **not guaranteed to be available indefinitely**. Version retention is currently controlled by:
  > 
  > 1. **Active Snapshots**: Historical versions are retained as long as there are open snapshots that reference them. The compactor uses `db_state.recent_snapshot_min_seq` to determine the minimum sequence number to retain.
  > 2. **Per-Key Retention**: The `min_seq` is **per-key**, not global:
  >    - If key `k1` is written once with `seq=1` and never updated, it will remain available with `seq=1` indefinitely
  >    - If key `k2` is written with `seq=2` and later overwritten with `seq=123456`, the old version `(k2, seq=2)` will eventually be compacted away
  >    - There is no single global `min_seq` that applies to all keys
  > 3. **Future Enhancement - Time Travel**: A planned feature (tracked in [#1263](https://github.com/slatedb/slatedb/issues/1263)) will allow configuring a retention window (e.g., 24 hours) to guarantee version availability within that time period, regardless of snapshot lifecycle.
  > 
  > **Best Practices**:
  > - For short-term version queries (e.g., debugging), use `snapshot_with_options()` and keep the snapshot open as needed
  > - For long-term archival, use a separate backup mechanism rather than relying on SlateDB's version history
  > - Once time travel is implemented, configure an appropriate retention window for your use case

- **Snapshot Visibility**: 
  - Creating a snapshot with `seqnum` always succeeds, even if `seqnum < min_seq` or `seqnum > max_seq`
  - Read operations on the snapshot use visibility checks:
    - If data has been compacted (`seq < min_seq` for that key), it is not visible
    - If data has not yet been written (`seq > max_seq`), it is not visible
    - If no data satisfies the visibility condition, queries return empty results (`None` or empty iterator)

- **API Integration**: 
  - `snapshot_with_options(SnapshotOptions::default().read_at(seq))` creates a snapshot view at the specified sequence number
  - All read operations on the snapshot (`get`, `scan`, `get_row`) will see data as of that sequence number
  - The snapshot can be used with `next()` and `next_row()` methods on iterators to retrieve key-value pairs or complete row information respectively

<!-- TOC --><a name="impact-analysis"></a>
## Impact Analysis

**Breaking Changes**:

1.  **New Row Query APIs**:
    - New APIs introduced: `get_row()`, `get_row_with_options()`, `DbIterator::next_row()`.
    - `DbIterator::next()` maintains backward compatibility (still returns `Result<Option<KeyValue>, crate::Error>`).
    - Internal `next_key_value()` method renamed to `next_row()` and made public, returning `RowEntry`.
    - `RowEntry.value` remains as `ValueDeletable` type (no breaking changes to the type itself).
    - Neither `next()` nor `next_row()` return `Tombstone` values (tombstones are filtered out by the underlying iterator).
2.  **Put/Write Return Type Modification**:
    - All `put()`, `put_with_options()`, `delete()`, `delete_with_options()`, `merge()`, `merge_with_options()`, `write()`, and `write_with_options()` calls must be updated to handle the new `Result<WriteHandle, ...>` return type.
    - To access the sequence number, call `.seqnum()` on the returned `WriteHandle`.
    - Language bindings (Go/Python) must be updated accordingly. For example, in Go, consider exposing a `WriteHandle` type with a `Seqnum()` method, or alternatively return `(uint64, error)` directly. Note that for `write()`, all operations in the batch share the same sequence number in the current implementation.

**Backward Compatibility**:

- `get_row()`, `get_row_with_options()`, and `DbIterator::next_row()` are new APIs.
- They return the existing `RowEntry` type, which is already public.
- `DbIterator::next()` maintains backward compatibility (still returns `KeyValue`).
- `snapshot_with_options()` is a new API that extends existing snapshot functionality.
- `SnapshotOptions.seqnum` allows creating snapshots at specific sequence numbers.
- No impact on any storage formats (WAL/SST/Manifest).

**Migration Path**:

No migration needed for existing code. `DbIterator::next()` maintains backward compatibility:

```rust
// Existing scan code continues to work unchanged
let mut iter = db.scan(..).await?;
while let Some((key, value)) = iter.next().await? {
    // No changes needed
}

// When you need complete row information, use next_row()
let mut iter = db.scan(..).await?;
while let Some(row_entry) = iter.next_row().await? {
    // Access metadata: row_entry.seq(), row_entry.create_ts(), row_entry.expire_ts()
    match row_entry.value() {
        ValueDeletable::Value(v) => {
            // Use row_entry.key() and v
        }
        ValueDeletable::Merge(m) => {
            // Use row_entry.key() and m
        }
        ValueDeletable::Tombstone => unreachable!(),
    }
}

// New row query APIs
let row = db.get_row(b"key").await?;
```

<!-- TOC --><a name="testing"></a>
## Testing

**Unit Tests**:

- Row query: Normal cases, expired keys, tombstones, merge operations, sequence number monotonicity.
- Put/Write return values: Verify correctness of sequence numbers.
- Versioned query: Read historical versions, expired key handling, behavior after Compaction.

**Integration Tests**:

- End-to-end workflow: Combined use of row query and versioned query.
- Versioned query across multiple storage layers.
- Consistency of metadata during concurrent writes.

**Performance Testing**:

- `get_row()` performance: Since it returns complete row information, it decodes both metadata and value. Performance should be similar to existing `get()` operations.
- `scan()` with `next()` returning `KeyValue`: Performance should remain unchanged (backward compatible).
- `scan()` with `next_row()` returning `RowEntry`: Performance should be similar to `next()`, as metadata is already decoded during iteration.
- `get_with_options` with specified snapshot sequence number.

<!-- TOC --><a name="alternatives"></a>
## Alternatives

**1. Dedicated Specialized Interfaces (e.g., `get_ttl`, `get_seqnum`, etc.)**

- **Reason for Rejection**: 
  1.  **API Complexity**: This would rapidly expand the API surface area and increase user learning costs. A single generic interface `get_row` that returns complete row information (including metadata) maintains API simplicity.
  2.  **Atomicity**: Some use cases require retrieving both the key/value and its metadata atomically. Separate methods would prevent this (unless a snapshot is created). Returning the complete `RowEntry` naturally supports atomic access.

**2. Introduce `put_with_metadata()` Method**

- **Reason for Rejection**: Introducing a new API would lead to functional overlap. Modifying the existing `put()` return type, while a breaking change, maintains architectural simplicity and consistency.

**3. Return Only Metadata (RowMetadata) Instead of Complete Row (RowEntry)**

- **Reason for Rejection**: Since decoding metadata requires decoding the entire row, returning only metadata would not improve performance. Returning the complete `RowEntry` provides more value to users while utilizing an existing public type, reducing API complexity.

**4. No Change (Status Quo)**

- **Problem**: Users cannot access physically stored metadata (TTL, sequence number, etc.), limiting important scenarios like debugging, auditing, and MVCC implementations.