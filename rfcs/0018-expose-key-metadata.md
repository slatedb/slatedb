# Expose Row Information

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Expose Row Information](#expose-row-information)
  - [Summary](#summary)
  - [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Design](#design)
    - [1. Row Query Interface](#1-row-query-interface)
    - [2. Modify Put/Write Return Types](#2-modify-putwrite-return-types)
    - [3. Support Query by Version](#3-support-query-by-version)
  - [Impact Analysis](#impact-analysis)
  - [Testing](#testing)
  - [Alternatives](#alternatives)

<!-- TOC end -->

Status: Draft

Authors:

* [marsevilspirit](https://github.com/marsevilspirit)

<!-- TOC --><a name="summary"></a>
## Summary

This RFC proposes three related API improvements:

1.  **Modify Write API Return Types**: Change the return type of `db.put()`, `db.delete()`, and `db.merge()` (including their `_with_options` variants) from `Result<(), ...>` to `Result<WriteHandle, ...>` to return a write handle containing the assigned sequence number.
2.  **Enrich Batch Write Return Type**: Change the return type of `db.write()` from `Result<(), ...>` to `Result<WriteHandle, ...>` to return a write handle containing the commit sequence number. This design allows for future extensions such as `await_durability()`.
3.  **New Row Query Interface**: Introduce `get_row()`, `get_row_with_options()`, `scan_rows()`, and `scan_rows_with_options()` interfaces, allowing users to query complete row information including metadata (sequence number, creation timestamp, expiration timestamp) and value.
4.  **Support Query by Version**: Add a `read_at_seq` option to `ReadOptions`, enabling users to read a specific historical version of a key via its sequence number.

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

- No modification of the underlying storage format or SST file structure.
- No implementation of full MVCC transaction semantics.
- No changes to existing TTL expiration mechanisms or compaction logic.


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

// Scan values (unchanged)
pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
where
    K: AsRef<[u8]> + Send,
    T: RangeBounds<K> + Send;

// Scan rows (new) - returns complete row information
pub async fn scan_rows<K, T>(&self, range: T) -> Result<DbRowIterator, crate::Error>
where
    K: AsRef<[u8]> + Send,
    T: RangeBounds<K> + Send;

// Scan rows with options (new)
pub async fn scan_rows_with_options<K, T>(
    &self,
    range: T,
    options: &ScanOptions,
) -> Result<DbRowIterator, crate::Error>
where
    K: AsRef<[u8]> + Send,
    T: RangeBounds<K> + Send;

// RowEntry is already public and contains all necessary information
pub struct RowEntry {
    pub key: Bytes,
    pub value: ValueDeletable,  // Can be Value, Merge, or Tombstone
    pub seq: u64,
    pub create_ts: Option<i64>,
    pub expire_ts: Option<i64>,
}
```

**Usage Example**:

```rust
// Get complete row information
let row = db.get_row(b"my_key").await?;
if let Some(entry) = row {
    println!("Key: {:?}", entry.key);
    println!("Seq: {}, Created: {:?}, Expires: {:?}", 
             entry.seq, entry.create_ts, entry.expire_ts);
    match entry.value {
        ValueDeletable::Value(v) => println!("Value: {:?}", v),
        ValueDeletable::Merge(m) => println!("Merge: {:?}", m),
        ValueDeletable::Tombstone => println!("Tombstone"),
    }
}

// Scan rows in a range
let mut iter = db.scan_rows(b"a"..b"z").await?;
while let Some(row_entry) = iter.next().await? {
    println!("Key: {:?}, Seq: {}", row_entry.key, row_entry.seq);
}

// Scan rows with options
let mut iter = db.scan_rows_with_options(
    b"a"..b"z",
    &ScanOptions::default()
).await?;
```

**Implementation Details**:

1.  **Reuse Existing `RowEntry` Type**:
    - `RowEntry` is already public and contains all necessary fields: `key`, `value`, `seq`, `create_ts`, and `expire_ts`.
    - Since decoding metadata requires decoding the entire row, returning complete `RowEntry` has no performance penalty.
    - Introduce `DbRowIterator` to iterate over `RowEntry` objects.
2.  **Value Type Handling**:
    - `RowEntry.value` is of type `ValueDeletable`, which can be:
      - `ValueDeletable::Value(Bytes)` - Regular value
      - `ValueDeletable::Merge(Bytes)` - Merge operation. **Important**: If a key has multiple merge operations, only the **latest merge** is returned. The merge values are not applied/combined.
      - `ValueDeletable::Tombstone` - Deleted key
3.  **Tombstone and Expired Key Handling**: 
    - **Tombstones**: `get_row` and `scan_rows` can return tombstones (`ValueDeletable::Tombstone`), allowing users to see that a key has been deleted along with its metadata (seq, timestamps).
    - **Expired Keys**: By default, expired keys (based on TTL) are skipped. Future options may allow including expired keys.
4.  **Multi-Version Behavior**: `scan_rows` returns the **latest visible version** of each key (consistent with `scan` behavior). If a key has multiple versions at different sequence numbers, only the version visible at the current time (or specified `read_at_seq`) is returned.

<!-- TOC --><a name="2-modify-putwrite-return-types"></a>
### 2. Modify Put/Write Return Types

> [!WARNING]
> **Breaking Change**: All existing code using `db.put`, `db.delete`, `db.merge`, and `db.write` (and their `_with_options` variants) must be updated to handle the new return type.

**WriteHandle Structure**:

```rust
/// Handle returned from write operations, containing metadata about the write.
/// This structure is designed to be extensible for future enhancements.
pub struct WriteHandle {
    seq: u64,
}

impl WriteHandle {
    /// Returns the sequence number assigned to this write operation.
    pub fn seqnum(&self) -> u64 {
        self.seq
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

// New interface. Returns WriteHandle containing the assigned sequence number.
pub async fn put<K, V>(&self, key: K, value: V) -> Result<WriteHandle, crate::Error>
pub async fn delete<K>(&self, key: K) -> Result<WriteHandle, crate::Error>
pub async fn merge<K, V>(&self, key: K, value: V) -> Result<WriteHandle, crate::Error>
```

**Batch Operations**:

```rust
// New interface. Returns WriteHandle containing the commit sequence number for the batch.
// In the current implementation, all operations in a batch share the same sequence number.
pub async fn write(&self, batch: WriteBatch) -> Result<WriteHandle, crate::Error>
```

**Migration Example**:

```rust
// Single key operation - get sequence number
let handle = db.put(b"key", b"value").await?;
println!("Seq: {}", handle.seqnum());

// Batch operation
let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"key1");

let handle = db.write(batch).await?;
println!("Batch committed at seq: {}", handle.seqnum());
// All operations in the batch share this sequence number

// Option 2: Ignore return values (if you don't need the sequence number)
let _ = db.put(b"key", b"value").await?;

let mut batch2 = WriteBatch::new();
batch2.put(b"another_key", b"another_value");
let _ = db.write(batch2).await?;
```

**Implementation Details**:

- Modify `DbInner::write_with_options` to return `WriteHandle` (containing the assigned `commit_seq`).
- `put()`, `delete()`, and `merge()` return the `WriteHandle` received from `DbInner`.
- Both `put`, `delete`, and `merge` operations share the same underlying write pipeline.
- `WriteHandle` is a simple wrapper around `u64` for now, but provides extensibility for future features.

<!-- TOC --><a name="3-support-query-by-version"></a>
### 3. Support Query by Version

Add a `read_at_seq` field to `ReadOptions` and leverage the existing `max_seq` mechanism:

```rust
pub struct ReadOptions {
    ...
    pub read_at_seq: Option<u64>,  // New
}

impl ReadOptions {
    pub fn with_read_at_seq(self, read_at_seq: u64) -> Self {
        Self { read_at_seq: Some(read_at_seq), ..self }
    }
}
```

**Usage Example**:

```rust
// Write and get version number
let handle = db.put(b"my_key", b"value1").await?;
let seq = handle.seqnum();

// Subsequent update
db.put(b"my_key", b"value2").await?;

// Read old version (single key)
let old_value = db.get_with_options(
    b"my_key",
    &ReadOptions::default().with_read_at_seq(seq)
).await?;
assert_eq!(old_value, Some(Bytes::from("value1")));
```

**Scan Versioned Queries**:

To support range scans with versioned queries, we add `read_at_seq` to `ScanOptions`:

```rust
pub struct ScanOptions {
    ...
    pub read_at_seq: Option<u64>,  // New
}

impl ScanOptions {
    pub fn with_read_at_seq(self, read_at_seq: u64) -> Self {
        Self { read_at_seq: Some(read_at_seq), ..self }
    }
}
```

**Usage Example**:

```rust
// Write some data
let handle1 = db.put(b"key1", b"value1_v1").await?;
let seq1 = handle1.seqnum();
db.put(b"key2", b"value2_v1").await?;

let handle2 = db.put(b"key1", b"value1_v2").await?;
let seq2 = handle2.seqnum();
db.put(b"key2", b"value2_v2").await?;

// Scan at historical version
let mut iter = db.scan_with_options(
    b"key1"..=b"key2",
    &ScanOptions::default().with_read_at_seq(seq1)
).await?;

while let Some((key, value)) = iter.next().await? {
    // Will return value1_v1 and value2_v1
    println!("Key: {:?}, Value: {:?}", key, value);
}
```

**Implementation Details**:

- **Unified Sequence Filtering**: Leverage the existing `Reader::prepare_max_seq()` mechanism, which already supports sequence number filtering.
- **API Integration**: 
  - Pass `ReadOptions::read_at_seq` to the `max_seq` parameter of `Reader::get_with_options()` in `Db::get_with_options()`.
  - Pass `ScanOptions::read_at_seq` to the `max_seq` parameter during iterator construction in `Db::scan_with_options()`.
- **Expiring and Compaction**:
  - Expired keys (based on TTL) return `None` or are skipped in scans, consistent with standard behavior.
  - **Compaction Impact**: Historical versions may be removed by compaction. The query will return the latest available version `v` such that `v.seq <= read_at_seq`. If all such versions have been compacted away, the query will return `None`.

> [!IMPORTANT]
> **Historical Version Availability**: `read_at_seq` does not guarantee that a specific historical version will always be available. As SSTs are compacted, older versions of keys are eventually purged to save space. Users should not rely on this for long-term data archival or recovery unless they manage version retention at a higher level.

<!-- TOC --><a name="impact-analysis"></a>
## Impact Analysis

**Breaking Changes**:

1.  **New Row Query APIs**:
    - New APIs introduced: `get_row()`, `get_row_with_options()`, `scan_rows()`, `scan_rows_with_options()`.
    - A new `DbRowIterator` type is introduced for row iteration.
    - These APIs reuse the existing public `RowEntry` type.
    - Existing `KeyValueIterator` trait and `DbIterator` type remain unchanged.
    - No breaking changes to existing APIs.
2.  **Put/Write Return Type Modification**:
    - All `put()`, `put_with_options()`, `delete()`, `delete_with_options()`, `merge()`, `merge_with_options()`, `write()`, and `write_with_options()` calls must be updated to handle the new `Result<WriteHandle, ...>` return type.
    - To access the sequence number, call `.seqnum()` on the returned `WriteHandle`.
    - Language bindings (Go/Python) must be updated accordingly. For example, in Go, consider exposing a `WriteHandle` type with a `Seqnum()` method, or alternatively return `(uint64, error)` directly. Note that for `write()`, all operations in the batch share the same sequence number in the current implementation.

**Backward Compatibility**:

- `get_row()`, `get_row_with_options()`, `scan_rows()`, and `scan_rows_with_options()` are new APIs.
- They return the existing `RowEntry` type, which is already public.
- `ReadOptions.read_at_seq` extends existing `get_with_options()` and `scan_with_options()`, defaults to `None`.
- No impact on any storage formats (WAL/SST/Manifest).

**Migration Path**:

No migration needed for existing code. Simply use the new row query APIs when you need complete row information:

```rust
// Existing code continues to work
let value = db.get(b"key").await?;
let mut iter: DbIterator = db.scan(..).await?;

// New row query APIs
let row = db.get_row(b"key").await?;
let mut row_iter: DbRowIterator = db.scan_rows(..).await?;
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

- `get_row`/`scan_rows` performance: Since these return complete row information, they decode both metadata and value. Performance should be similar to existing `get`/`scan` operations.
- `get_with_options(read_at_seq)` performance.
- Performance Goal: Impact of new features on existing operations < 5%.

<!-- TOC --><a name="alternatives"></a>
## Alternatives

**1. Dedicated Specialized Interfaces (e.g., `get_ttl`, `get_seqnum`, etc.)**

- **Reason for Rejection**: This would rapidly expand the API surface area and increase user learning costs. A single generic interface `get_row` that returns complete row information (including metadata) maintains API simplicity.

**2. Introduce `put_with_metadata()` Method**

- **Reason for Rejection**: Introducing a new API would lead to functional overlap. Modifying the existing `put()` return type, while a breaking change, maintains architectural simplicity and consistency.

**3. Return Only Metadata (RowMetadata) Instead of Complete Row (RowEntry)**

- **Reason for Rejection**: Since decoding metadata requires decoding the entire row, returning only metadata would not improve performance. Returning the complete `RowEntry` provides more value to users while utilizing an existing public type, reducing API complexity.

**4. No Change (Status Quo)**

- **Problem**: Users cannot access physically stored metadata (TTL, sequence number, etc.), limiting important scenarios like debugging, auditing, and MVCC implementations.