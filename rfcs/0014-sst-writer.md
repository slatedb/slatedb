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
    - [Pebble SST Format](#pebble-sst-format)
    - [Pebble Ingestion process](#pebble-ingestion-process)
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
- This RFC will not modify the existing SST file format.
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
let metadata = writer.close()?;
```
User can retrieve metadata about the SST file when `SSTWriter::close()` is called. We can return some basic metadata such as:
- Entry count
- SST file location
- File size
- ...


## Implementation Details
Currently, SlateDB already has an internal SST writer component named `EncodedSsTableWriter` which manages the low-level details of creating SST files and streaming uploading to object store. We can leverage this existing component to implement the `SSTWriter` API.

## Relation with Ingestion
As mentioned earlier, this RFC does not cover the Ingestion feature itself. However, some questions may arise about Ingestion, SSTWriter, and transaction. Here, we will use Pebble to provide a simple description.

### Pebble SST Format
```markdown
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│                     PEBBLE SSTABLE FILE                         │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                       DATA BLOCK #1                             │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Key-Value Pairs (sorted by key)                           │ │
│  │  ┌─────────────────────────────────────────────────────┐  │ │
│  │  │ key₁ (user key + seqnum + type) :  value₁           │  │ │
│  │  │ key₂ (user key + seqnum + type) : value₂           │  │ │
│  │  │ key₃ (user key + seqnum + type) : value₃           │  │ │
│  │  │ ...                                                 │  │ │
│  │  └─────────────────────────────────────────────────────┘  │ │
│  │                                                            │ │
│  │ Restart Points Array (every N keys)                       │ │
│  │  [offset₁, offset₂, ..., count]                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Compression Type (1 byte): none/snappy/zstd                   │
│  Checksum (4 bytes): CRC32C                                    │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                       DATA BLOCK #2                             │
│  (same structure as Data Block #1)                             │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                          ...                                     │
│                   (more data blocks)                            │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                       DATA BLOCK #N                             │
│  (same structure as Data Block #1)                             │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                   INDEX BLOCK (First-Level)                     │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Index Entry #1:                                           │ │
│  │   Separator Key₁ → BlockHandle(offset, length)           │ │
│  │   Block Properties (optional, v1+)                        │ │
│  │                                                            │ │
│  │ Index Entry #2:                                           │ │
│  │   Separator Key₂ → BlockHandle(offset, length)           │ │
│  │   Block Properties (optional, v1+)                        │ │
│  │                                                            │ │
│  │ ...                                                         │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Restart Points                                                 │
│  Compression Type (1 byte)                                      │
│  Checksum (4 bytes)                                             │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│              TOP-LEVEL INDEX BLOCK (if two-level)               │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Points to First-Level Index Blocks                        │ │
│  │                                                            │ │
│  │ Entry #1: Key → IndexBlockHandle₁(offset, length)        │ │
│  │ Entry #2: Key → IndexBlockHandle₂(offset, length)        │ │
│  │ ...                                                        │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                    FILTER BLOCK (Bloom Filter)                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Bloom filter data for fast key existence checks          │ │
│  │ - Reduces disk reads for non-existent keys               │ │
│  │ - One filter per data block or partitioned               │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                   RANGE DELETION BLOCK                          │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Range Tombstones (fragmented)                             │ │
│  │                                                            │ │
│  │ [start_key₁, end_key₁) @ seqnum₁                         │ │
│  │ [start_key₂, end_key₂) @ seqnum₂                         │ │
│  │ ...                                                        │ │
│  │                                                            │ │
│  │ Fragments are split at overlap boundaries                 │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                     RANGE KEY BLOCK (v2+)                       │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Range Keys                                                 │ │
│  │                                                            │ │
│  │ [start_key₁, end_key₁) → {suffix:  value}                 │ │
│  │ [start_key₂, end_key₂) → {suffix: value}                 │ │
│  │ ...                                                        │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                    VALUE BLOCK #1 (v3+)                         │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Separated large values                                     │ │
│  │                                                            │ │
│  │ Value #1 (blob data)                                       │ │
│  │ Value #2 (blob data)                                       │ │
│  │ ...                                                        │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                    VALUE BLOCK #2 (v3+)                         │
│  (same structure as Value Block #1)                             │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                   VALUE BLOCK INDEX (v3+)                       │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Maps value handles to value block locations               │ │
│  │                                                            │ │
│  │ ValueHandle₁ → BlockHandle(offset, length)               │ │
│  │ ValueHandle₂ → BlockHandle(offset, length)               │ │
│  │ ...                                                        │ │
│  │                                                            │ │
│  │ Special encoding:                                           │ │
│  │  - BlockNumByteLength                                      │ │
│  │  - BlockOffsetByteLength                                   │ │
│  │  - BlockLengthByteLength                                   │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                      PROPERTIES BLOCK                           │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ SSTable Metadata (key-value format)                        │ │
│  │                                                            │ │
│  │ "rocksdb.block. based.table.index. type" → "0/1"           │ │
│  │ "rocksdb.comparator" → "comparator name"                  │ │
│  │ "rocksdb.compression" → "compression type"                │ │
│  │ "rocksdb.data.size" → "total data size"                   │ │
│  │ "rocksdb.index.size" → "index size"                       │ │
│  │ "rocksdb.num.data.blocks" → "count"                       │ │
│  │ "rocksdb.num.entries" → "key count"                       │ │
│  │ "rocksdb.raw.key.size" → "total key bytes"               │ │
│  │ "rocksdb.raw.value.size" → "total value bytes"           │ │
│  │ "rocksdb.deleted.keys" → "tombstone count"               │ │
│  │ "rocksdb.merge.operands" → "merge count"                 │ │
│  │ "pebble.obsolete.key" → "obsolete keys info (v4+)"       │ │
│  │ ...                                                         │ │
│  │ + Block Property Collector outputs                         │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                      META INDEX BLOCK                           │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Directory of metadata blocks (key → BlockHandle)          │ │
│  │                                                            │ │
│  │ "filter.rocksdb. BuiltinBloomFilter"                       │ │
│  │     → BlockHandle(offset, length)                         │ │
│  │                                                            │ │
│  │ "rocksdb.properties"                                       │ │
│  │     → BlockHandle(offset, length)                         │ │
│  │                                                            │ │
│  │ "rocksdb.range_del"                                        │ │
│  │     → BlockHandle(offset, length)                         │ │
│  │                                                            │ │
│  │ "rocksdb.range_key" (v2+)                                  │ │
│  │     → BlockHandle(offset, length)                         │ │
│  │                                                            │ │
│  │ "pebble.value_index" (v3+)                                 │ │
│  │     → ValueBlockIndexHandle(offset, length, ...)          │ │
│  │                                                            │ │
│  │ "pebble.blob_ref_index" (v6+)                              │ │
│  │     → BlockHandle(offset, length)                         │ │
│  └───────────────────────────────────────────────────────────┘ │
│  Trailer (compression + checksum)                               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                         FOOTER                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ [Optional] Checksum Type (1 byte, RocksDBv2+)            │ │
│  │                                                            │ │
│  │ Metaindex BlockHandle (varint encoded)                    │ │
│  │   - Offset (varint)                                        │ │
│  │   - Length (varint)                                        │ │
│  │                                                            │ │
│  │ Index BlockHandle (varint encoded)                        │ │
│  │   - Offset (varint)                                        │ │
│  │   - Length (varint)                                        │ │
│  │                                                            │ │
│  │ Padding (align to fixed size)                             │ │
│  │                                                            │ │
│  │ [Optional] Attributes (4 bytes, v7+)                       │ │
│  │   - Bit flags for sstable features                         │ │
│  │                                                            │ │
│  │ [Optional] Footer Checksum (4 bytes, v6+)                  │ │
│  │   - CRC32C of footer contents                              │ │
│  │                                                            │ │
│  │ Format Version (4 bytes, little-endian)                   │ │
│  │   - RocksDBv2: version = 2                                 │ │
│  │   - Pebblev1-v8: version = 1-8                             │ │
│  │                                                            │ │
│  │ Magic Number (8 bytes)                                     │ │
│  │   - LevelDB:   0xdb4775248b80fb57                           │ │
│  │   - RocksDB:  0xf09faab3f971db42                           │ │
│  │   - Pebble:   0xf09fabc4f5e2b0a4                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│                 Footer Size:                                     │
│                   - LevelDB:  48 bytes                           │
│                   - RocksDBv2 - Pebblev5: 53 bytes             │
│                   - Pebblev6: 57 bytes                          │
│                   - Pebblev7+: 61 bytes                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```
In Pebble SST file, there is no global sequence number stored.

### Pebble Ingestion process
Pebble will assign each entry `seqnum = 0` during writing into `SSTWriter`. There is no concept of global sequence number during SST file creation.

When ingestion occurs, Pebble will call `db.ingest` API to import the SST file into the database. Entire ingestion process can be divided into several phases as below:
```markdown
┌─────────────────────────────────────────────────────────────────────────────┐
│                        1. INGESTION START PHASE                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│ DB.Ingest()      │
│ (ingest. go)      │
└────────┬─────────┘
         │
         ▼
┌───────────────────────────────────────────────┐
│ Load SSTable Metadata                         │
│ ingestLoad()                                  │
│  - Read SSTable Properties                    │
│  - Create TableMetadata object                │
│  - SeqNum not yet assigned                    │
└────────┬──────────────────────────────────────┘
         │
         │ TableMetadata initial state: 
         │ {
         │   TableNum: xxx,
         │   Size: xxx,
         │   PointKeyBounds: {... },
         │   SeqNums: {Low: 0, High: 0}  ⟵ Not set yet
         │ }
         │
         ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                     2. SEQUENCE NUMBER ALLOCATION PHASE                      │
└─────────────────────────────────────────────────────────────────────────────┘

         ┌──────────────────────┐
         │ commitPipeline.      │
         │ AllocateSeqNum()     │  ⟵ Atomic seqNum allocation
         │ (commit.go)          │
         └───────┬──────────────┘
                 │
                 │ Atomically increment from logSeqNum
                 │ seqNum = env.logSeqNum. Add(count)
                 │
                 ▼
         ┌──────────────────────┐
         │ prepare() callback   │
         │ Update metadata here │
         └───────┬──────────────┘
                 │
                 ▼
         ┌──────────────────────────────┐
         │ ingestUpdateSeqNum()         │
         │ (ingest.go)                  │
         │                              │
         │ For each ingested table:     │
         │   setSeqNumInMetadata(...)   │
         └───────┬──────────────────────┘
                 │
                 ▼
         ┌──────────────────────────────────────┐
         │ setSeqNumInMetadata()                │
         │ (ingest.go)                          │
         │                                      │
         │ Update TableMetadata:                │
         │   m.SeqNums.Low = seqNum            │
         │   m.SeqNums.High = seqNum           │  ⟵ Same value = synthetic
         │   m.LargestSeqNumAbsolute = seqNum  │
         │                                      │
         │ Update seqnum in Smallest/Largest   │
         └──────────────┬───────────────────────┘
                        │
                        │ TableMetadata after update:  
                        │ {
                        │   TableNum: 000123,
                        │   Size:  1024,
                        │   SeqNums: {Low: 100, High: 100},  ⟵ Set
                        │   LargestSeqNumAbsolute: 100,
                        │   PointKeyBounds: {
                        │     Smallest: "a"(seqNum = 100),SET,
                        │     Largest: "z"(seqNum = 100),SET
                        │   }
                        │ }
                        │
                        ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                     3. VERSION EDIT CONSTRUCTION PHASE                       │
└─────────────────────────────────────────────────────────────────────────────┘

                  ┌──────────────────────┐
                  │ ingestApply()        │
                  │ (ingest.go)          │
                  └──────┬───────────────┘
                         │
                         │ Create VersionEdit
                         │
                         ▼
                  ┌──────────────────────────────────┐
                  │ VersionEdit structure:           │
                  │                                  │
                  │ ve = &VersionEdit{              │
                  │   NewTables: []NewTableEntry{   │
                  │     {                            │
                  │       Level: 0,                  │
                  │       Meta: *TableMetadata ⟶    │ Points to updated metadata
                  │     },                           │
                  │     ...                           │
                  │   },                             │
                  │   MinUnflushedLogNum: xxx,      │
                  │   NextFileNum: xxx,             │
                  │   LastSeqNum: 100,              │
                  │ }                                │
                  └──────┬───────────────────────────┘
                         │
                         ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                     4. VERSION EDIT ENCODING PHASE                           │
└─────────────────────────────────────────────────────────────────────────────┘

                  ┌───────────────────────┐
                  │ VersionEdit. Encode()  │
                  │ (version_edit.go)     │
                  └──────┬────────────────┘
                         │
                         │ Create encoder
                         │
                         ▼
              ┌──────────────────────────┐
              │ versionEditEncoder       │
              │ {Buffer}                 │
              └──────┬───────────────────┘
                     │
                     │ Encode fields in order
                     │
                     ├─▶ writeUvarint(tagComparator)
                     │   writeString(comparerName)
                     │
                     ├─▶ writeUvarint(tagLogNumber)
                     │   writeUvarint(minUnflushedLogNum)
                     │
                     ├─▶ writeUvarint(tagNextFileNumber)
                     │   writeUvarint(nextFileNum)
                     │
                     │ For each new table:
                     ├─▶ writeUvarint(tagNewFile2)
                     │   writeUvarint(level)           ⟵ Level number
                     │   writeUvarint(tableNum)        ⟵ File number
                     │   writeUvarint(size)            ⟵ File size
                     │   
                     │   writeKey(smallest)            ⟵ Smallest key (with seqnum)
                     │   writeKey(largest)             ⟵ Largest key (with seqnum)
                     │   
                     │   writeUvarint(smallestSeqNum)  ⟵ SeqNums.Low = 100
                     │   writeUvarint(largestSeqNum)   ⟵ SeqNums.High = 100
                     │
                     │ Binary format example:
                     │ [tag=9][level=0][fileNum=123][size=1024]
                     │ [smallest="a"(seqNum = 100),SET][largest="z"(seqNum = 100),SET]
                     │ [seqNumLow=100][seqNumHigh=100]
                     │ ...  
                     │
                     ▼
              ┌───────────────────────┐
              │ Encoded byte stream   │
              │ []byte{... }           │
              └──────┬────────────────┘
                     │
                     ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                     5. MANIFEST FILE WRITE PHASE                             │
└─────────────────────────────────────────────────────────────────────────────┘

                     ┌─────────────────────────┐
                     │ versionSet.logAndApply()│
                     │ (version_set.go)        │
                     └──────┬──────────────────┘
                            │
                            │ Get manifest writer
                            │
                            ▼
                     ┌──────────────────────────┐
                     │ record.Writer.Next()     │  Get next record
                     │ (record/record.go)       │
                     └──────┬───────────────────┘
                            │
                            ▼
                     ┌──────────────────────────┐
                     │ VersionEdit.Encode(w)    │  Encode to writer
                     └──────┬───────────────────┘
                            │
                            ▼
              ┌─────────────────────────────────┐
              │  Write to MANIFEST file         │
              │  (MANIFEST-xxxxxx)              │
              │                                 │
              │  File structure:                │
              │  ┌──────────────────────┐      │
              │  │ Record Header        │      │
              │  │ - CRC32 checksum     │      │
              │  │ - Length             │      │
              │  ├──────────────────────┤      │
              │  │ Version Edit Data    │      │
              │  │ [encoded bytes]      │  ⟵ Contains TableMetadata seqnums
              │  ├──────────────────────┤      │
              │  │ Record Header        │      │
              │  ├──────────────────────┤      │
              │  │ Version Edit Data    │      │
              │  │ ...                   │      │
              │  └──────────────────────┘      │
              └──────┬──────────────────────────┘
                     │
                     │ fsync()
                     │
                     ▼
              ┌────────────────────────┐
              │ MANIFEST persisted     │
              └────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                     6. VERSION APPLICATION PHASE                             │
└─────────────────────────────────────────────────────────────────────────────┘

                     ┌────────────────────────┐
                     │ BulkVersionEdit.Apply()│
                     │ (version_edit.go)      │
                     └──────┬─────────────────┘
                            │
                            │ Build new Version from old
                            │
                            ▼
                     ┌────────────────────────────────┐
                     │ New Version created            │
                     │                                │
                     │ newVersion. Levels[0] =         │
                     │   LevelMetadata{               │
                     │     tree: btree[TableMetadata] │  ⟵ Contains ingested tables
                     │   }                            │
                     │                                │
                     │ Each TableMetadata has:        │
                     │   - SeqNums: {Low: 100,High:100}│
                     │   - Complete key bounds        │
                     └──────┬─────────────────────────┘
                            │
                            │ installNewVersion()
                            │
                            ▼
                     ┌────────────────────────┐
                     │ New Version activated  │
                     │ currentVersion = new   │
                     └────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                     7. READ-TIME APPLICATION (Synthetic SeqNum)              │
└─────────────────────────────────────────────────────────────────────────────┘

                     ┌────────────────────────────┐
                     │ When reading SSTable       │
                     │ (level_iter.go)            │
                     └──────┬─────────────────────┘
                            │
                            │ Based on TableMetadata
                            │
                            ▼
                ┌───────────────────────────────────┐
                │ TableMetadata.SyntheticSeqNum()   │
                │                                   │
                │ if m.SeqNums.Low == m.SeqNums.High│  ⟵ Detect equality
                │   return SeqNum(m.SeqNums. Low)    │  ⟵ Return 100
                │ else                              │
                │   return NoSyntheticSeqNum        │
                └──────┬────────────────────────────┘
                       │
                       │ syntheticSeqNum = 100
                       │
                       ▼
                ┌──────────────────────────────────┐
                │ When sstable. Reader creates iter: │
                │ Pass IterTransforms:              │
                │   SyntheticSeqNum:  100            │
                └──────┬───────────────────────────┘
                       │
                       │ Auto-apply during iteration
                       │
                       ▼
                ┌──────────────────────────────────┐
                │ When reading keys:                │
                │                                  │
                │ Physical SSTable:                 │
                │   "a"#0,SET          ⟵ Stored   │
                │        ↓                         │
                │ Apply Synthetic SeqNum:          │
                │   "a"(seqNum = 100),SET        ⟵ Returned │
                └──────────────────────────────────┘
```
Aforementioned `TableMetadata` struct equals to `SSTableInfo` in SlateDB.
So we can know that `SSTWriter` does not involve any SSTable format changes and ingestion process should only involve metadata manipulation without modifying the actual SST file.

## References
- [Creating and Ingesting SST files - RocksDB](https://github.com/facebook/rocksdb/wiki/creating-and-ingesting-sst-files)
- [sst_file_writer.h - RocksDB](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/sst_file_writer.h)
- [writer.go - Pebble](https://github.com/cockroachdb/pebble/blob/master/sstable/writer.go)
- [ingest.go - Pebble](https://github.com/cockroachdb/pebble/blob/master/ingest.go) (An overview of Pebble's ingestion process can be found from L1274 to L1333)