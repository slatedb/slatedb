# Building Blocks for Range Following

Status: Draft

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Summary

The CDC API in [RFC-0019](0019-change-data-capture.md) gives users a way to follow changes across the full database. The cost is proportional to the overall rate of writes, which is expected when the goal is to follow changes to all keys. However, it does not provide a way to efficiently select changes only within a subrange of the keyspace, which is useful when processing must be parallelized across multiple readers. This RFC addresses this gap by providing building blocks to track changes within a key range through L0 and the sorted runs. Unlike the WALs which are sorted by sequence number, L0s and SRs are sorted by key, which means we can fetch just the records in a range. We can still use the sequence number as a way to track changes, but we need a way to follow the sequence number through the L0s and sorted runs, which means resolving the key/sequence ordering imparity. This RFC describes an approach to treat the sequence number along with a reader checkpoint as a durable cursor so that a reader can follow changes efficiently even in the presence of failures.

## Motivation

Subranges provide a natural way to parallelize a workload across multiple readers. Workload based on incremental view maintenance require an efficient way to read the stream of changes from the database. Today there are two options: follow WAL files (CDC), or poll range scans over the subrange. Neither option provides an efficient way to isolate only the changes within the target range. WAL files isolate the changes effectively, but the scope is the full keyspace. Range scans isolate the target range, but they expose a full snapshot of the current state and not the delta of changes.

As an alternative to the WAL, we can read changes from L0 and the sorted runs, which are sorted by key. This makes it efficient to read a key subrange, but the challenge is tracking progress and isolating change deltas. The sequence number cannot be used on its own because reads from L0s/SRs are not returned in sequence order. Furthermore, the order of data read from the sorted runs is not deterministic since the compactor is constantly rewriting them. To track progress durably so that a restart does not require scanning the entire dataset, we need the help of reader checkpoints.

The main goal of this RFC is to build an approach for efficiently and durably tracking changes within a subrange of keys. It is nearly possible today, but we need to bridge the gap between key and sequence ordering. We propose several API extensions and show how an application can use them to build a durable cursor following changes within a range of keys. The benefit of this approach is that it bounds read amplification; a reader will see changes proportional to the size of the range they are following. The downside is that the reader must await writes to L0, which implies additional latency compared to following WALs. 

## Goals

- Provide an efficient API to isolate the delta of records within a key range.
- Support sequence filtering in SlateDB's normal range scan path.
- Allow range scans to prune LSM sources whose sequence history is below the requested range.
- Define a durable cursor and its semantics.

## Non-Goals

- Provide a managed change-follower abstraction that owns checkpoint creation, cursor persistence, or checkpoint cleanup.

## Design

SlateDB sequence numbers provide the natural ordering that we need to track progress and isolate a delta of changes in SlateDB. In principle, if we know that we have processed up to sequence N, then we can get the delta of changes over a key range by scanning the range and filtering only the records with sequence greater than N. This would be inefficient today because we do not have an easy way to isolate records in the LSM using the sequence number. We would need to fetch all records and filter.

A simple improvement is to extend the manifest so that sequence ranges can be inferred without fetching any data. For each L0 or sorted run, we can include a high watermark sequence number in the manifest representing the largest sequence number that the layer covers. When building a sorted run during compaction, we simply take the maximum sequence watermark among all of the input sources. With this information, a range scan with a minimum sequence number can locate the layers that intersect the range without fetching unnecessary data. Let's call this a "delta scan."

As a strawman, imagine our approach is to track our position in the database using the sequence number. We maintain our cursor as the highest sequence number N that we have processed. We can send a delta scan with a minimum sequence of N to get the next set of changes to process. We then advance the cursor to N', send the next delta scan, and so forth.

This approach can work, but there are two problems. First, records returned from an L0 or SR are not returned in sequence order. While processing the records from a delta scan, we may see sequence N before we have seen all records with sequence M < N. In order to safely advance the cursor, we must first process all records in the scan. This , however, it means we cannot save our progress incrementally and hope to be able to resume after a failure.

A second problem is that our cursor attempts to map a position within a moving target. A delta scan applied to the LSM at one point of time may return a different ordering of changes at another point of time. As new L0s are written and as the compactor creates new SRs, the data is restructured and the scan order changes. If we could fix our scan against a specific LSM state using a reader checkpoint, then the delta scan would be deterministic.

This suggests that we can track a durable position in the database using the following tuple:

- Checkpoint id which binds the scan to a specific manifest version
- The minimum sequence M provided to the delta scan.
- The maximum sequence N of records processed during the scan
- The last processed key from the scan

This tuple can be persisted and resumed after a failure. We load the reader with the checkpoint ID so that we use the correct manifest version. We then send a delta scan using the lower bound sequence M and our target range with the lower bound set to the last processed key. The lower sequence M ensures that we do not see any data that we have not already processed. As we receive new records from the resumed scan, we update the maximum sequence N that we have processed. Once we have reached the end of the scan, N will become the lower-bound sequence number for the next delta scan.

This approach provides fairly weak semantics in the context of IVM. We are ensured that we see the changes to each key in the correct order, but there is no guarantee about the ordering between keys. We cannot guarantee snapshot consistency, which would require following the sequence numbers precisely as with CDC.

This is why we have chosen not to propose a first-class API. Instead, we suggest several smaller API changes which make cursors like this possible:

1. Add sequence watermarks to SlateDB manifest to effectively filter L0s and SRs based on a sequence range.
2. Add a sequence range filter to `ScanOptions` in order to filter a specific delta.
3. Add an API to advance a reader to the latest manifest when we have consumed each delta scan.
4. Add optional checkpoint sequence watermark to ensure tombstones are always witnessed by a reader following changes incrementally

We will specify each of these in the following sections.

### Manifest Sequence Watermarks

In order to be able to filter a scan based on a sequence number, we propose to add a sequence high watermark to each manifest-visible LSM source. The watermark represents the highest SlateDB sequence number whose history is covered by the source. It is lineage metadata, not necessarily the maximum sequence number of rows physically retained in the source.

```rust
pub struct L0Sst {
    pub sst: SsTableView,
    pub seq_hi: u64,
}

pub struct SortedRun {
    pub id: u32,
    pub seq_hi: u64,
    pub ssts: Vec<SsTableView>,
}
```

On L0 flush, `seq_hi` is set to the highest sequence number covered by the flushed memtable. On compaction, the output sorted run inherits:

```rust
output.seq_hi = max(input.seq_hi)
```

We will use this metadata when serving a sequence delta scan, as discussed in the next section.

### Extend `ScanOptions` to Support Delta Scans

Add an optional sequence range to `ScanOptions`.

```rust
pub struct ScanOptions {
    pub min_seq: Option<u64>,
    // existing fields...
}
```

When `min_seq` is set, scans apply SlateDB's normal merge and visibility semantics, but filter returned rows only above the minimum sequence. 


### Tombstone Retention

The durable cursor based on scanning delta changes has a subtle problem for mutable datasets. When a record is deleted, SlateDB inserts a tombstone into the database. This tombstone propagates through L0 and into the sorted runs. When a tombstone is present in the last sorted run in the LSM, the compactor can remove it and any prior (in terms of sequence) records with the same key. The danger is that a reader which is following a durable cursor may skip between a manifest in which the key exists and one in which it does not without having witnessed the tombstone.

Note that this is not a problem for typical scans in SlateDB because they execute against a consistent snapshot of the database based on a single manifest version. A cursor as we have discussed is effectively a cross-snapshot query which must build its state from multiple manifest versions. It is not possible for a read on a single snapshot to see the state both before and after a key is deleted.

To address this, we need the compactor to follow the position of each cursor so that it can tell when a tombstone is safe to remove. The cursor's existence is implied by the checkpoint. The checkpoint points to a manifest version, which tells us the maximum sequence that the cursor may have read up to. We can call this the reader's upper bound sequence. Any tombstone at a lower sequence must have been observed by the reader with that cursor. The corollary is that any tombstone at a higher sequence must remain present.

The compactor can compute a checkpoint watermark by taking the minimum sequence across all cursor upper bound sequences. However, not all checkpoints require this treatment. A checkpoint used for recovery does not place any limits on the compactor. We propose to extend checkpoint metadata so that a reader can indicate directly its upper bound sequence number. Currently, metadata is only used by writers to save their epoch in order to facilitate cleanup. We propose to add `SequenceWatermark` as an additional metadata variant which readers can provide.

```
union CheckpointMetadata { WriterCheckpoint, SequenceWatermark }

table SequenceWatermark {
    sequence: uint64;
}

```

The sequence watermark tells the compactor how far a checkpoint has progressed without needing to fetch the corresponding manifest metadata. In order to determine whether a tombstone can be deleted, the compactor will apply its current validation to ensure the tombstone's location in the final sorted run. It will then compare the sequence of the tombstone against the minimum watermark across all checkpoints. If the tombstone's sequence is higher than the minimum watermark, then it must be retained.

Over time, if a reader is not making progress, tombstones may accumulate. This can be mitigated by using checkpoint expiration. If no readers have set a watermark, then there is no additional restriction on tombstone retention.

### Exposing Records and Metadata in the Scan API

Of course preventing cleanup of tombstones only matters if they are exposed to the user. We propose to modify `DbIterator` in order to expose a `next_entry` method:


```rust
pub async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> { ... }
```

Unlike `next` which automatically prunes tombstones and applies merges, this API will return the raw records. Providing access to `RowEntry` (which is already public) also gives users access to the sequence number, which is necessary to advance the cursor. We assume that readers using merge semantics will be able to duplicate merging logic in their aplication.

### Reader Checkpoint Advancement

When we have reached the end of a delta scan, we want to let the reader advance to the latest checkpoint. The reader today operates in two modes: pinned checkpoint or managed checkpoint. With a pinned checkpoint, the reader remains locked onto a specific checkpoint which is specified in configuration. With a managed checkpoint, the manifest poller automatically advances the reader's checkpoint to the latest manifest version. 

What we need is something in between pinned and managed modes. In particular, we need to stay pinned to a checkpoint until we are ready to advance. We propose to add the following API to `DbReader`:

```rust
struct AdvanceCheckpointOptions {
  /// The checkpoint to advance to. If `None`, advance to the latest.
  checkpoint_id: Option<Uuid>,

  /// Watermark which prevents cleanup of tombstones at higher sequences
  sequence_watermark: Option<u64>,
}

/// Advance the reader to a specific checkpoint.
pub async fn advance_checkpoint(&self, options: AdvanceCheckpointOptions) -> Result<Uuid>;
```

If a checkpoint is provided in the options, the reader will confirm its existence and reestablish its state against the manifest that the checkpoint refers to. This API assumes a use case in which the checkpoint is created externally to the reader. For example, this could be used to propagate a specific checkpoint state from the writer to one or more readers.

If no checkpoint is provided, the reader will establish a new reader checkpoint against the latest manifest version and atomically drop the old one. Once durable, the new checkpoint ID will be returned in the result. This logic exists today in `DbReader::reestablish_checkpoint` internally.

The sequence watermark can be set explicitly if known by the application. There is no strict requirement that it be aligned with the manifest that the checkpoint points to, but generally it would be set to the same as `last_l0_seq`. 

## Impact Analysis

## Operations

## Testing

## Rollout

## Alternatives

## References

LogDb is a SlateDb-based system which exposes a Kafka-like log abstraction. Keys are structured as `<segment><key><sequence>`, where the key presents a single log stream. The `sequence` behaves similarly to Kafka's `offset`, but it is a global value. Every record has a unique global sequence. This structure optimizes locality at the level of each log key so that entries are efficient to fetch, particularly so as the compactor reorganize records. Note that the global sequence order of log entries across all keys cannot be reconstructed without reading all of the data. 

While fetching individual log streams is efficient, there are many use cases which require following multiple logs. Data pipelines which use LogDb to ship data between systems, for example, would require following all logs. The basic issue is that any API which spans more than one key covers all of the log entries. There is no way to efficiently select only the tail of changes within the current key structure. It is also not practical in a pipeline use case to replay the full log after each restart, so our position must be durable (like an offset commit in Kafka).

Since the key structure is optimized for reading each log stream individually, the global sequence is not useful as a cursor for reading across multiple keys. Instead, LogDb can use this RFC to implement a range-based log cursor as described in the Design section. One notable feature of LogDb is that it is an append-only system. The records in an SST do not follow LogDb's global sequence order, but the layers of the LSM will have a monotonic ordering to them. In other words, each L0 will contain keys with sequence numbers that are strictly larger than any prior L0 or sorted run. Similarly, for each layer in the tree. This implies that using the delta scan approach above will return records for each key in LogDb-sequence order because each layer in the tree is also monotonic in Slatedb-sequence order.


## Updates
