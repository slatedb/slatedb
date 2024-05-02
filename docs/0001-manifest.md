# Alternative Manifest Design

## File Structure

```
some-bucket/
├─ manifest/
│  ├─ current
│  ├─ 000000000000000000.manifest
│  ├─ 000000000000000001.manifest
│  ├─ ...
├─ level_0/
│  ├─ 000000000000000000.sst
│  ├─ 000000000000000001.sst
│  ├─ ...
├─ compacted/
│  ├─ 000000000000000000.sst
│  ├─ 000000000000000001.sst
│  ├─ ...
```

## Files

### `manifest/current`

A single line string pointing to the current manifest file:

```
000000000000000001.manifest
```

### `manifest/000000000000000001.manifest`

A file containing commpaction and snapshot information:

```json
{
  "last_compacted_l0_sst": "000000000000000000.sst",
  "compacted": [
    // SsTableInfo, ...
  ],
  "snapshots": {
    "10727630-0f47-49b6-840d-e2d16412ff9b": {
      "manifest": "000000000000000000.manifest",
      "utc_timestamp": 1231714598526
    },
    // ...
  }
}
```

_NOTE: Described as JSON, but the manifest files will be binary to reduce size._

### `level_0/000000000000000000.sst`

Standard SSTs with SsTableInfo, block metadata, bloom filters, and so on.

Each SST will have the following [user-defined metadata fields](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata) set:

* writer_epoch: This is a u64. Writers will set this when the put a new SST into `level_0`.
* crc32: This is a u32 checksum for the file. It's also used to de-duplicate writes if two zombies have the same writer_epoch (see below).

### `compacted/000000000000000000.sst`

Standard SSTs in the same format as `level_0` SSTs. Commpacted SSTs have the same user-defined metadata fields as `level_0` SSTs.

The compaction processes should set `writer_epoch` metadata field to the max of the `writer_epoch`s for the SSTs it used as inputs for the compacted SST. For example, if a compactor is merging the following SSTs:

* 000000000000000000.sst, writer_epoch=1
* 000000000000000003.sst, writer_epoch=7
* 000000000000000042.sst, writer_epoch=3

The resulting `.sst` file would have `writer_epoch=7` (max(1, 7, 3)).

## Writer

### Startup

On startup a writer process:

1. Loads `manifest/current` (if it exists)
2. Loads `manifest/<manifest-id>.json` that `manifest/current` points to (if (1) exists)
3. Insert the SsTableInfos from the `compacted_ssts` field (loaded in (2)) into the in-memory DbState. Otherwise, DbState starts empty.
4. Set DbState.last_compacted_l0_sst from the `last_compacted_l0_sst` field (loaded in (2)).
5. Recover level_0 SSTs:

```rust
let mut current_writer_epoch = state.compacted.map(SsTableInfo::writer_epoch).max();
let mut current_sst_id = state.last_compacted_l0_sst;

// Read all contiguous SSTs starting from the first SST after last_compacted_l0_sst
while Some(sst_info) = table_store.open_sst(current_sst_id + 1) {
  // We've found a new writer_epoch, so seal all future writes from older ones
  if sst_info.writer_epoch > current_writer_epoch:
    current_writer_epoch = current_writer_epoch
  // We've found a valid write, add the SST to the DbStat's l0
  if sst_info.writer_epoch == current_writer_epoch:
    state.l0.insert(0, sst_info);
  // if sst_info.writer_epoch < current_writer_epoch:
  //   ignore since this is a zombie write (a higher epoch has previously written an SST)

  // Advance to the next contigous SST ID
  current_sst_id += 1;
}

state.next_sst_id = current_sst_id;
state.writer_epoch = current_writer_epoch + 1;
```

We now have fully recovered state. There might still be SSTs that we haven't seen if they occur after a gap. For example, if we have l0=[1, 2, 4, 5], current_sst would be set to 3 at this point, and 4 and 5 would not be in state.l0.

Let's consider this set of SSTs with the current manifest containing `last_compacted_l0_sst=000000000000000000.sst`:

```
├─ level_0/
│  ├─ 000000000000000000.sst (writer_epoch=1)
│  ├─ 000000000000000001.sst (writer_epoch=1)
│  ├─ 000000000000000002.sst (writer_epoch=2)
│  ├─ 000000000000000003.sst (writer_epoch=1)
│  ├─ 000000000000000005.sst (writer_epoch=3)
```

The startup protocol would end with:

* `next_sst_id=4` since `000000000000000004.sst` is the first missing SST
* `writer_epoch=3` since the maximum `writer_epoch` with SST ID < `next_sst_id` is `writer_epoch=2` (from 000000000000000002.sst)
* `l0=[000000000000000001.sst, 000000000000000002.sst]`

L0 excludes the following SSTs because:

* `000000000000000000.sst` because it's in the compacted set as defined by `last_compacted_l0_sst`
* `000000000000000003.sst` because its `writer_epoch` is less than a previous SST's (`000000000000000002.sst` in this example)
* `000000000000000005.sst` because a previous SST was missing (`000000000000000004.sst` in this example)

_NOTE: The writer does not snapshot or update the manifest on startup. This can cause two processes to both start simultaneously and end up with the same `writer_epoch`. This is fine; their conflict will get detected and resolved when they write conflicting `level-0` SSTs (see below)._

### Writing

#### `put()`

1. Client calls `put(k, v).await?`.
2. If kv-pair size is >= `skip_memtable_bytes`, a new MemTable is created and inserted directly into `state.imm_memtables`. `inner.flush_imms()` is called.
3. Else if `flush_bytes` is exceeded, `inner.flush()` is called.
4. Else when `flush_ms` is exceeded, `inner.flush()` is called.
5. The `put()` method returns `self.inner.put().await?`.

In all three cases, the kv-pair has been put into an immutable MemTable in `state.imm_memtables` and `flush_imms` has been invoked.

#### `flush()`

We allow parallel MemTable flushing now. Consequently, we have a race condition where later `level_0` SSTs might appear in the object store *before* earlier SSTs are visible. For example, continuing with our prevoous example, we might see:

```
├─ level_0/
│  ├─ 000000000000000000.sst (writer_epoch=1)
│  ├─ 000000000000000001.sst (writer_epoch=1)
│  ├─ 000000000000000002.sst (writer_epoch=2)
│  ├─ 000000000000000003.sst (writer_epoch=1)
│  ├─ 000000000000000005.sst (writer_epoch=3)
```

In this case, a writer would start with a state of `next_sst_id=4` and `writer_epoch=3` (excluding fields like `l0` for brevity).

There are several possible outcomes when we try to write the next SST (`000000000000000004.sst`) to object storage:

1. `000000000000000004.sst` does not exist, and the write goes through
2. `000000000000000004.sst` does exist, but has a `writer_epoch` < our `writer_epoch`
3. `000000000000000004.sst` does exist, and has a `writer_epoch` == our `writer_epoch` but `crc32` != our `crc32`
4. `000000000000000004.sst` does exist, and has a `writer_epoch` == our `writer_epoch` and `crc32` = our `crc32`
5. `000000000000000004.sst` does exist, but has a `writer_epoch` > our `writer_epoch`
6. `000000000000000004.sst` fails because of an object storage issue (e.g. network issue, 503, etc.)

In case (1), we are successful, so we move on to notifying listeners of the successful write (see `await` section below).

In case (2), a zombie process wrote an SST. We must fail all in-flight writes (including this one) and run the startup process again so we get a new valid `next_sst_id` to start from.

In case (3), we've detected that another process also started up while we were running our startup process. This is the race condition mentioned in the "Startup" section above. In this case, we're the zombie because the file's `crc32` doesn't match ours. We must fail all in-flight writes (including this one) and run the startup process again so we get a new valid `next_sst_id` to start from.z

In case (4), we've detected that another process also started up while we were running our startup process. Luckily, we won the race since our `crc32` matches what's in object storage. The other process is the zombie and will restart its startup process to get a new `writer_epoch`.

#### `await`

This is valid and acceptable. Following the startup protocol defined above, the process will determine 

However, it presents a problem:

1. A `put()` should not be acknowledged successfully until its MemTable is written to object storage *and* all previous in-flight MemTables have been written to storage.
2. 
a. The MemTable that's being moved to `state.imm_memtables` must be assigned `state.next_sst_id` and `state.next_sst_id` must be incremented. This is because we're now allowing parallel writes. To maintain strict ordering, the MemTable must be assigned the next contiguous ID as soon as it's frozen (rather than asynchronously in `flush.rs` as it currently is).
b. The new MemTable that's being created must include a pointer to the old MemTable that's being moved to `state.imm_memtables`. This is so the new MemTable can `.await?` for the older MemTable to flush successfully before the new MemTable returns a successful `Result` on its own `put()` calls. This way, if a MemTable flush fails, its failure will cascade to all future MemTable flushes that were in flight.


Asynchronously, `flush()` does the following:

1. Encode 

Asynchronously, `flush()` will encode the MemTable as an SST and write it to `"level_0/{%18d}.sst".format(next_sst_id)`.

If `If-None-Match: "*"` or an equivalent CAS operation is supported, the flusher will use a CAS opeartion to only put if the SST file doesn't exist.

If CAS is not supported, the flusher will 

In all three cases, when the MemTable is moved to `state.imm_memtables`, the MemTable will need to be assigned the SST id `state.next_sst_id`

6. 

2. The kv-pair is inserted into a mutable MemTable.
3. After `flush_ms`, if `flush_bytes` is exceeded, or if the kv-pair size is >= `skip_memtable_bytes`, move the MemTable into state.imm_memtables and create a new state.memtable if needed.
4. 


## Readers

## Compaction

## Garbage Collection

1. Loads latest manifest.
2. Fetches