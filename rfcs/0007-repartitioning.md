# Repartitioning Primitives

## Motivation and Problem Statement

Currently, SlateDB does not include a built-in partitioning mechanism. To scale horizontally, users must implement their own partitioning strategy and manage each partition as a separate SlateDB instance. This design is consistent with other LSM-tree–based embedded databases, where partitioning is often left to higher-level orchestration.

While creating static partitions is relatively straightforward, changing the number of partitions over time (repartitioning) is more complex. Existing embedded databases like RocksDB and Badger do not provide convenient primitives to enable a simple and performant repartitioning workflow in user applications.

## High-level Solution

SlateDB already offers a critical feature for repartitioning: snapshots. Users can take a snapshot of the database and clone it without creating a full copy of the original data. This capability significantly reduces the overhead of duplicating data and is a strong foundation for any repartitioning strategy.

To fully support a robust and efficient repartitioning process, SlateDB needs two additional primitives:
1. Creating Partial, Non-Overlapping Clones
2. Merging Multiple Clones

### Creating Partial, Non-Overlapping Clones

The ability to take a single snapshot and split it into multiple, smaller "partial" clones. Each partial clone contains only a subset of the data, allowing users to distribute different segments of the database across multiple new instances.

This feature can piggyback on the existing clone mechanism - we just need the ability to hide unwanted parts of the keyrange. We can achieve this by adding additional metadata to the parent pointer of the cloned database.

### Merging Multiple Clones

The ability to merge multiple, non-overlapping clones back into a single database. This would enable consolidation of data when reducing the number of partitions or combining data from multiple shards into a single, cohesive database instance.

## Implementation Details

### KeyRange aware manifest

It's crucial to be aware of the full keyrange handled by the SST file and therefore the whole database. This will allow us to effectively mask out part of the overall keyrange and ensure two databases don't overlap.

We already have information about the first_key for each SST file stored in the manifest, so we simply need to add last_key to be fully aware of the contained keyrange.

```rust
// Has metadata about a SST file.
table SsTableInfo {
    // First key in the SST file.
    first_key: [ubyte];
    // Last key in the SST file.
    last_key: [ubyte];
    ...
}
```

### Masking parent's KeyRange

To mask out a keyrange from the parent database, I propose simply bookkeeping visible KeyRange next to its pointer. This works recursively in case we have a deeply nested clone.

```rust
table KeyRange {
    first_key: [ubyte];
	last_key: [ubyte];
}

table DbParent {
    // Path to the parent database from which this database was cloned
    path: string (required);
    // Parent checkpoint ID that this clone was derived from
    checkpoint: Uuid (required);
    // NEW: Defines what part of the parent database is visible to the
    // clone. Keys outside of this range will be ignored.
    visible_range: KeyRange (required);
}
```

We also need to tweak the public API to allow users to define visible range (we'll default to making the whole parent database visible).

```rust
pub async fn create_clone<P: Into<Path>>(
    clone_path: P,
    parent_path: P,
    object_store: Arc<dyn ObjectStore>,
    parent_checkpoint: Option<Uuid>,
    visible_range: Option<BytesRange>
) -> Result<(), Box<dyn Error>> {
    ...
}
```

During the cloning process we'll do the following on top of the current logic:
- Compute full parent_range based on first and last key of individual SSTs
- If visible_range is set to:
    - Some: Validate visible_range is a subset of parent_range
    - None: Set visible_range = parent_range
- Filter out SSTs outside of the visible_range
- Filter out WALs outside of the visible_range
    - For this we need to resolve WAL's range by reading the metadata block (SsTableFormat::read_info) of the underlying SST file, since this info is not contained in the manifest
- Populate visible_range metadata in cloned manifest

On the read path, we can leverage existing code paths of SstIterator (new_borrowed) that allows us to create a view over an existing SST file.

```rust
pub(crate) async fn new_borrowed<T: RangeBounds<&'a [u8]>>(
    range: T,
    table: &'a SsTableHandle,
    table_store: Arc<TableStore>,
    options: SstIteratorOptions,
) -> Result<Self, SlateDBError> {
    ...
}
```

### Merging multiple manifests

To allow merging multiple databases, we need to restructure existing manifest blueprints to accept multiple DbParents instead of just one.

```rust
table ManifestV1 {
    // Optional details about the parent checkpoints for the database
    parents: [DbParent];
}
```

This should give us everything we need on the metadata layer and we just need to introduce a new variant of the clone method.

```rust
struct Parent<P: Into<Path>> {
    path: P,
    checkpoint: Option<Uuid>,
    visible_range: Option<BytesRange>,
}

impl Db {
    pub async fn create_merge_clone<P: Into<Path>>(
        clone_path: P,
        object_store: Arc<dyn ObjectStore>,
        parents: Vec<Parent<P>>,
    ) -> Result<(), Box<dyn Error>> {
        ...
    }
}
```

This method can work more or less the same as the one for splitting, we just need to ensure parents are non-overlapping.

## Rejected Alternatives

We've discarded the idea of providing a full implementation of DeleteRange that we could leverage during the splitting process, and instead focus on addressing the specific use case (re-partitioning) that can further differentiate SlateDB against its competitors.
