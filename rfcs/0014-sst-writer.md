# SST Writer

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [SST Writer](#sst-writer)
  - [Summary](#summary)
  - [Motivation and Background](#motivation-and-background)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Design](#design)
    - [API](#api)
      - [Create and close](#create-and-close)
      - [Write entries](#write-entries)
      - [Read entries](#read-entries)
      - [Metadata](#metadata)
  - [Implementation Details](#implementation-details)
  - [Relation with Ingestion](#relation-with-ingestion)
  - [References](#references)

<!-- TOC end -->

Status: Draft

Authors:

- [AsukaMilet](https://github.com/KarinaMilet)

## Summary
This RFC defines the design for an SST writer component for SlateDB. The goal is to provide a robust, efficient, resumable, and observable path to create SST files directly instead of traditional write path including WAL and Memtable. This component also lays the foundation for future Ingestion feature.

## Motivation and Background
Currently, user of SlateDB relies on the write path (WAL + Memtable + Compaction) to produce SST files. It works well for most of the use cases. However, user may want to create SST files directly for the following reasons:
- Key already sorted: In some scenarios, data is already sorted (e.g., bulk import from another database, data export from analytics systems). Writing through the WAL + Memtable path would introduce unnecessary overhead and write amplification.
- Generate SST files externally: Users may want to generate SST files outside of SlateDB process (e.g., in a data pipeline, or a separate ingestion service) and then download and import them into SlateDB.
- Migrating shards between machines by dumping key-range in SST File and loading the file in a different machine.

## Goals

- Provide a single, well-specified SST writer abstraction which directly public to users.
- Ensure that the SST is internally ordered and meets the basic SST requirements.
- Support users write to different storage backends than `Db` instance and create different configurations (e.g., different block size).

## Non-Goals

- This RFC does not cover the Ingestion feature itself, only the SST writer component that can be used as a building block for Ingestion.
- Because it does not cover the Ingestion feature, it also does not consider transaction and manifest updates. Those will be handled by the Ingestion layer.

## Design

Components:

- `SSTWriter`(tentative name): public API used by users. Accepts a stream of entries and object store instance.

### API
#### Create and close
User can create and close a `SSTWriter` directly even without a `Db` instance. 
```rust
let writer = SSTWriter::new(object_store, path, config);

// write entries
writer.close();
```
In our design, each `SSTWriter` is tied to a single SST file. User can create multiple `SSTWriter` instances to create multiple SST files in parallel. But this also means that user need to create a new 
`SSTWriter` to create a new SST file. They cannot reuse the same `SSTWriter` instance to create multiple SST files.

#### Write entries
User can write entries to the `SSTWriter` instance. But we require user to write entries in sorted order (by key). If user attempts to write entries out of order, the `SSTWriter` will return an error.
```rust
writer.put(key, value)?;
writer.delete(key)?;
```
We can also provide more API such as `merge`, but currently we only support `put` and `delete`.

#### Read entries
`SSTWriter` does not provide read API.

#### Metadata
```rust
let metadata = writer.metadata();
```
User can retrieve metadata about the SST file, but only valid after `close()` is called.


## Implementation Details
Currently, SlateDB already has an internal SST writer component named `EncodedSsTableWriter` which manages the low-level details of creating SST files and streaming uploading to object store. We can leverage this existing component to implement the `SSTWriter` API.

## Relation with Ingestion
As mentioned earlier, this RFC does not cover the Ingestion feature itself. However, some questions may arise about  Ingestion, SSTWriter, and transaction. Here, we will use Pebble to provide a simple description.

Pebble assign each entry `seqnum = 0` during writing into `SSTWriter`. When ingestion occurs, Pebble will call 
`db.ingest` API to import the SST file into the database. 

During this process, Pebble will request a new sequence number from it's commit pipeline, and use this sequence number as global sequence number for all entries in SST file.

Global sequence number will not be stored in the SST file itself, but will be applied to manifest.
```go
// Request sequence number and call commit pipeline
seqNumCount := loadResult.sstCount()
if args.ExciseSpan.Valid() {
	seqNumCount++
}
d.commit.ingestSem <- struct{}{}
d.commit.AllocateSeqNum(seqNumCount, prepare, apply)
```
In other words, SSI/SI isolation, manifest, etc., and SSTWriter are orthogonal parts.

## References
- [Creating and Ingesting SST files - RocksDB](https://github.com/facebook/rocksdb/wiki/creating-and-ingesting-sst-files)
- [sst_file_writer.h - RocksDB](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/sst_file_writer.h)
- [writer.go - Pebble](https://github.com/cockroachdb/pebble/blob/master/sstable/writer.go)
- [ingest.go - Pebble](https://github.com/cockroachdb/pebble/blob/master/ingest.go) (An overview of Pebble's ingestion process can be found from L1274 to L1333)