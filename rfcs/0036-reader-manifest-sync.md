# Manual Manifest Updates for DbReader

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Terms](#terms)
  - [Overview](#overview)
  - [Manual reader mode](#manual-reader-mode)
  - [On-demand refresh](#on-demand-refresh)
  - [Transfer format for VersionedManifest](#transfer-format-for-versionedmanifest)
  - [Durable manifest on DbStatus](#durable-manifest-on-dbstatus)
  - [The sync_manifest API](#the-sync_manifest-api)
  - [Validation](#validation)
  - [State change without IO](#state-change-without-io)
  - [Garbage collection](#garbage-collection)
  - [Races between refresh and sync](#races-between-refresh-and-sync)
  - [Fallback after a failed read](#fallback-after-a-failed-read)
  - [Example](#example)
- [Impact Analysis](#impact-analysis)
- [Operations](#operations)
  - [Performance & Cost](#performance--cost)
  - [Observability](#observability)
  - [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)

<!-- TOC end -->

Status: Draft

Authors:

* [Bobby Calderwood](https://github.com/bobby)

## Summary

This RFC adds a reader mode, `DbReaderMode::Manual`, in which the application decides when the reader changes. A manual reader has no background poller and creates no checkpoint. The application moves it forward with one of two calls:

- `DbReader::refresh` reads the latest manifest and the newer WAL files from object storage, on demand.
- `DbReader::sync_manifest` takes a `VersionedManifest` that the application received from the writer through its own channel, such as a network message. The reader installs the manifest in memory without object store IO, and the next read uses it.

The RFC also adds `ManifestTransfer`, a serde type that carries a `VersionedManifest` as bytes in the existing `.manifest` file format, and a `DbStatus` field that holds only manifests that are durable in object storage. If reads against a pushed manifest fail because an object is missing, the reader goes back to the last state that it built from object storage.

## Motivation

A `DbReader` learns about new manifests in one of two ways today:

- In `DbReaderMode::Checkpoint`, the reader stays on one manifest for its full life.
- In `DbReaderMode::ManagedCheckpoint` and `DbReaderMode::FollowLatest`, a background poller reads the latest manifest from object storage once every `manifest_poll_interval`.

The poll interval sets a floor on reader staleness. Each poll also costs object store requests: a LIST and a GET for the manifest, and in managed mode a manifest write when the checkpoint moves. A short interval makes readers fresh but costs many requests when the database is idle. A long interval saves requests but makes readers stale when the writer is busy.

Consider a cluster with one writer node and many reader nodes. The writer processes thousands of writes per second. The application already has a network path between the nodes. A task on the writer node reads `Db::subscribe` and tells the reader nodes about each new manifest. The application controls the send rate. It can send often when the write rate is high and stop when the database is idle.

This use case has two separate problems:

1. Poll on demand. The reader polls when the application asks, not on a timer. The writer sends a small signal, and each reader reads the manifest from object storage. This removes the cost of polls while the database is idle, and it removes the floor on staleness that the poll interval sets. Each update still costs object store requests for each reader, and the read waits for that IO.
2. Update without object store requests. The writer sends the manifest itself, and each reader installs it without IO. This removes the requests and the IO latency of each update, which matters when many readers follow a busy writer.

The first problem is smaller and has no trust problem, because the reader reads only from object storage. The second problem needs a manifest format for the transport, a durable manifest on the writer, and a fallback for a bad manifest. This RFC solves both, in two phases. See [Rollout](#rollout).

The application cannot build either solution today, for these reasons:

1. No reader mode turns off the poller and still follows newer manifests. `DbReader` has no method to poll on demand, and no method that accepts a manifest from outside.
2. `VersionedManifest` implements `Serialize` but has no public decoder. The flatbuffer codec for the manifest is `pub(crate)`.
3. `DbStatus::current_manifest` on the writer is the in-memory manifest. The writer reports it before the manifest write completes, and it labels it with the version ID of the previous manifest (see `report_to_status_manager` in `slatedb/src/memtable_flusher/manifest_writer.rs`). Two different manifests can carry the same ID, so a reader cannot use the ID to order them.

## Goals

- Add `DbReaderMode::Manual`, a mode without a poller and without a checkpoint.
- Add `DbReader::refresh`, which loads the latest manifest and WAL files from object storage on demand.
- Add `DbReader::sync_manifest`, which installs a manifest without object store IO.
- Reject manifests that are older than the installed manifest, or that fail the local validation rules.
- If reads against a pushed manifest fail because an object is missing, go back to the last state that the reader built from object storage.
- Add public methods that convert a `VersionedManifest` to and from bytes in the `.manifest` file format, and a serde type, `ManifestTransfer`, that carries those bytes.
- Give writers a `DbStatus` field that holds only durable manifests with correct version IDs.

## Non-Goals

- A transport or message format for manifests. The application owns the network path, framing, integrity checksums, batching, and retries.
- Push of WAL data or memtable data to readers. [#1636](https://github.com/slatedb/slatedb/issues/1636) covers temporary memtables for readers.
- Proof that a pushed manifest belongs to the same database as the reader. The manifest has no database identity field. The fallback path limits the damage from a wrong manifest.
- Changes to the manifest poller cost. [#1823](https://github.com/slatedb/slatedb/issues/1823) covers a cheaper poll.
- `refresh` or `sync_manifest` in the other reader modes. See [Alternatives](#alternatives).
- Garbage collection protection for a manual reader. See [Garbage collection](#garbage-collection).

## Design

### Terms

- Pushed manifest: a `VersionedManifest` that the application gives to `sync_manifest`.
- Loaded manifest: a manifest that the reader read from object storage itself, at open time or in a call to `refresh`.
- Base state: the most recent reader state that the reader built from a loaded manifest.
- Pushed state: a reader state that the reader built from a pushed manifest.

### Overview

```text
 writer node                                   reader node (DbReaderMode::Manual)
 ───────────                                   ──────────────────────────────────
 Db::subscribe()
   └─ DbStatus.durable_manifest
        │
        ├─ phase 1: send a signal ──▶ app transport ──▶ DbReader::refresh().await
        │                                                 reads the latest manifest and
        │                                                 WAL files from object storage
        │
        └─ phase 2: ManifestTransfer::from ──▶ app transport ──▶ VersionedManifest::try_from
                                                                   │
                                                                   ▼
                                                      DbReader::sync_manifest(manifest)
                                                         1. validate (CPU only)
                                                         2. build pushed state (CPU only)
                                                         3. swap state under the write lock
                                                         4. keep base state for fallback
```

Each reader state gets one more field. A pushed state holds its base state, so one lock protects both values:

```rust
struct ReaderState {
    // ...existing fields
    /// The base state. It is `Some` only when this state is a pushed state.
    base: Option<Arc<ReaderState>>,
}
```

A pushed state that the reader builds from another pushed state keeps the same base state. The chain of base states is never longer than one.

### Manual reader mode

```rust
pub enum DbReaderMode {
    ManagedCheckpoint,
    Checkpoint(Uuid),
    FollowLatest,
    /// Change the database state only when the application asks for it.
    Manual,
}
```

A manual reader opens like a `FollowLatest` reader. It loads the latest manifest and replays newer WAL files, unless `DbReaderOptions::skip_wal_replay` is set. It creates no checkpoint and starts no poller. After it opens, the reader changes only when the application calls `refresh` or `sync_manifest`. `manifest_poll_interval` and `checkpoint_lifetime` have no effect in this mode.

`refresh` and `sync_manifest` return an error of kind `ErrorKind::Invalid` in the other modes. Each of the other modes has a poller or a pinned checkpoint that owns the reader state. A sync in those modes races with the poller, and in `ManagedCheckpoint` mode each sync must move the checkpoint, which costs a manifest read and a manifest write. That cost removes the saving that the sync gives. The manual mode has none of these interactions.

A reader without a poller task records no close result today, so reads still succeed after `close`. A manual reader depends on the application for all changes, so `close` now records the close result when the reader has no poller. Reads, `refresh`, and `sync_manifest` then fail with `ErrorKind::Closed`. The fix also applies to `Checkpoint` mode, which has no poller either.

### On-demand refresh

```rust
impl DbReader {
    /// Loads the latest manifest and the newer WAL files from object storage.
    pub async fn refresh(&self) -> Result<(), crate::Error>;
}
```

`refresh` does the same work as one poll of a `FollowLatest` reader:

1. Read the latest manifest from object storage, a LIST and a GET.
2. If its ID is greater than the installed ID, build a state from it and install it.
3. Otherwise, replay the WAL files that are newer than the installed state.

An error from `refresh` goes to the caller. The reader keeps its state and stays open. The application decides when to retry.

`refresh` is phase 1 of this RFC. It solves the first problem in the [Motivation](#motivation) without `sync_manifest`, and it is also the way that a phase 2 application reads WAL data and repairs a reader after lost messages.

### Transfer format for VersionedManifest

The RFC adds two layers. The lower layer is two methods on `VersionedManifest` that convert it to and from bytes. The upper layer is a serde type, `ManifestTransfer`, that holds the version ID and those bytes. Most applications use `ManifestTransfer`.

#### Bytes

The RFC adds two public methods:

```rust
impl VersionedManifest {
    /// Returns the manifest in the `.manifest` file format. The bytes do not
    /// include the version ID. Use `id()` to get it.
    pub fn to_bytes(&self) -> Bytes;

    /// Builds a manifest from its version ID and bytes in the `.manifest`
    /// file format, such as the output of `to_bytes`.
    pub fn from_bytes(id: u64, bytes: &[u8]) -> Result<Self, crate::Error>;
}
```

Both methods use `FlatBufferManifestCodec`, the codec that reads and writes `.manifest` files. `to_bytes` returns the same bytes that the writer stores in object storage for that version. The bytes start with a format version, so `from_bytes` reads V1 and V2 manifests, as the manifest store does for files. Both methods are CPU only.

The version ID is not part of the bytes, because a `.manifest` file stores its ID in the file name and not in the content. `from_bytes` takes the ID as a separate argument.

`from_bytes` returns an error of kind `ErrorKind::Invalid` if the bytes are empty, if the format version is unknown, or if the flatbuffer decode fails. The kind is `Invalid` and not `Data`, because the bytes come from the caller and not from object storage.

The flatbuffer verifier accepts some bytes that the decoder cannot convert, and some decoder paths assert instead of returning an error. The bytes can come from outside the process, so `from_bytes` catches a panic in the decoder and returns `ErrorKind::Invalid`. A later change can make those decoder paths return errors.

The bytes follow the compatibility rules of the `.manifest` file format. A reader can decode the bytes from a writer of the same or an older SlateDB version. A reader rejects a format version that it does not know, as it does for a file.

#### ManifestTransfer

`ManifestTransfer` holds the version ID and the bytes from `to_bytes`. It implements `Serialize` and `Deserialize`, so the application can put it inside its own message types and send it in the serde format that it prefers.

```rust
/// A `VersionedManifest` in a form that serde can serialize.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestTransfer {
    id: u64,
    bytes: Bytes,
}

impl ManifestTransfer {
    /// The version ID of the manifest.
    pub fn id(&self) -> u64;

    /// The manifest in the `.manifest` file format.
    pub fn bytes(&self) -> &Bytes;
}

impl From<&VersionedManifest> for ManifestTransfer {
    // calls `VersionedManifest::to_bytes`
}

impl TryFrom<ManifestTransfer> for VersionedManifest {
    type Error = crate::Error;
    // calls `VersionedManifest::from_bytes`
}

impl TryFrom<&ManifestTransfer> for VersionedManifest {
    type Error = crate::Error;
    // calls `VersionedManifest::from_bytes`
}
```

The fields are private. An application cannot build a `ManifestTransfer` with an ID and bytes that do not match, except through `Deserialize`. `TryFrom` decodes the bytes, so bad bytes from `Deserialize` fail at that step with `ErrorKind::Invalid`.

The serde shape has two fields, `id` and `bytes`, and it is public API. The shape does not change when the manifest changes, because the manifest content is in `bytes`. The flatbuffer schema stays the only compatibility contract for the manifest content.

`Bytes` serializes as a byte sequence. Binary formats, such as bincode, postcard, and CBOR, store it in compact form. JSON writes it as an array of numbers, which is several times larger. An application that sends JSON can wrap `ManifestTransfer` in its own type that encodes `bytes()` as base64.

SlateDB does not define a message envelope. The application owns these items:

- The serde format and the framing of each message.
- An integrity checksum, if the transport does not have one. The flatbuffer verifier rejects most bad bytes, but [#1069](https://github.com/slatedb/slatedb/issues/1069) proposes to drop it.
- Compression, batching, and the send rate.

#### No partial manifest

This RFC does not add a transfer type with fewer fields. A reader must have the full SST metadata (`SsTableInfo`) of each SST to read it without IO, and that metadata is most of the size of a manifest. The fields that the read path does not use, such as `checkpoints` and `sequence_tracker`, are small. A partial type also makes `DbReader::manifest()` return a mix of new and old fields that matches no stored version. See [Alternatives](#alternatives).

### Durable manifest on DbStatus

`DbStatus` gets a new field:

```rust
pub struct DbStatus {
    // ...existing fields
    /// The most recent manifest that this handle wrote to object storage or
    /// read from object storage. The ID is the version ID of that manifest file.
    pub durable_manifest: VersionedManifest,
}
```

The writer updates the field in two places:

1. After `StoredManifest::update` succeeds, the writer reports the new ID and the value that it wrote.
2. After `StoredManifest::refresh` succeeds, the writer reports the ID and value that it read.

`current_manifest` keeps its current meaning, the in-memory view. Applications that push manifests to readers must send `durable_manifest`. A durable manifest has two properties that a pushed manifest needs:

- Each ID maps to exactly one manifest value.
- The manifest only refers to SSTs that a durable manifest refers to, so the garbage collector treats them as live.

`DbReader` sets `durable_manifest` to the manifest of its base state, because that manifest came from object storage.

`DbStatus` has a private field, so applications cannot build it. A new public field does not break callers.

### The sync_manifest API

```rust
/// The result of a call to `DbReader::sync_manifest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestSyncOutcome {
    /// The reader installed the manifest. The next read uses it.
    Applied,
    /// The manifest ID is less than or equal to the ID of the installed
    /// manifest. The reader did not change.
    Stale,
}

impl DbReader {
    /// Installs a manifest that the application received out of band.
    ///
    /// The call does no object store IO.
    pub fn sync_manifest(
        &self,
        manifest: VersionedManifest,
    ) -> Result<ManifestSyncOutcome, crate::Error>;
}
```

The method takes `VersionedManifest` by value, because the application decodes a new value for each message and the reader keeps it. The method is not `async`, so the application can call it from any thread.

The method returns `Stale` for an old manifest and does not return an error. Out-of-order delivery is normal for many transports, and the reader is still correct.

The method returns an error in these cases:

- `ErrorKind::Closed` if the reader is closed.
- `ErrorKind::Invalid` if the reader is not in `DbReaderMode::Manual`.
- `ErrorKind::Invalid` if the manifest fails [validation](#validation).

The reader does not change after an error. The application can keep the reader and send the next manifest.

`DbMetadataOps` does not get this method, because `Db` cannot accept manifests from outside.

### Validation

The reader applies these rules before it changes any state. All rules are CPU only.

1. The manifest is initialized.
2. `writer_epoch` is greater than or equal to the `writer_epoch` of the installed manifest.
3. `compactor_epoch` is greater than or equal to the `compactor_epoch` of the installed manifest.
4. `last_l0_seq` is greater than or equal to the `last_l0_seq` of the installed manifest.
5. `segment_extractor_name` is equal to the value in the installed manifest.

If the ID is less than or equal to the installed ID, the reader returns `Stale` before it applies the rules.

These rules do not prove that the manifest belongs to this database. A manifest from a different database can pass them. In that case, the first read that touches a missing SST starts the [fallback](#fallback-after-a-failed-read).

### State change without IO

The reader builds the pushed state from the installed state and the pushed manifest:

1. Set `manifest_id` and `manifest` from the pushed manifest.
2. Filter the immutable memtables. Drop each table whose `last_seq` is less than or equal to the new `last_l0_seq`. Keep each table that is fully newer. For a table that spans the boundary, call `filter_after_seq`. This is the same logic that `rebuild_state` uses today. The implementation moves it into a shared function.
3. Set `last_wal_id` and `last_remote_persisted_seq` with `replayed_watermark`, from the new manifest core and the filtered tables.

The reader does not replay the WAL in `sync_manifest`. WAL replay needs object store IO. The next call to `refresh` replays newer WAL SSTs.

The reader then does these steps while it holds the state write lock:

1. Make sure that the installed state did not change since step 1 of the build. If it changed, rebuild from the new installed state. The build is CPU only, so a retry is cheap.
2. If no base state is stored, store the installed state as the base state.
3. Advance the durable sequence number in the oracle. A read that uses the pushed state must not use the older durable sequence number, because it can then skip rows of the pushed manifest.
4. Install the pushed state.
5. Report the manifest to the status manager. Every change of the installed state reports the status while it holds the lock, so that the status matches the state.

### Garbage collection

A manual reader creates no checkpoint, so like a `FollowLatest` reader it does not protect its SSTs from garbage collection. The garbage collector does not delete a compacted SST until it is older than `compacted_options.min_age`, which is 24 hours by default. A pushed manifest refers only to SSTs that a durable manifest refers to, because the writer sends `durable_manifest`. If the application syncs or refreshes more often than `min_age`, the reader reads from SSTs that the garbage collector keeps. If the garbage collector deleted an SST of a pushed manifest, the [fallback](#fallback-after-a-failed-read) goes back to the base state.

### Races between refresh and sync

The application can call `refresh` and `sync_manifest` at the same time, for example from different tasks. `refresh` reads the installed state, awaits IO, and then installs a new state. A sync can install a newer state during the await. The reader prevents a move back to an older manifest:

- `install_state` keeps the state that has the greater manifest ID. If the IDs are equal, it keeps the new state, because that state includes WAL replay. If it keeps the installed pushed state, the new state becomes the base state of the pushed state, because it is the newest state from object storage.
- `maybe_replay_new_wals` installs its result only if the installed state is still the same `Arc` that it read. Otherwise it drops the replayed rows, and the next `refresh` replays the WAL again for the installed state.
- `rebuild_state` builds a state for a loaded manifest from the tables of the installed state. If the installed state is a pushed state that is newer than the loaded manifest, its tables miss the rows between the two L0 boundaries. The reader then replays those rows from the WAL.

The WAL reader compares WAL IDs with the newest manifest that the reader reported to its status. A sync can report a newer manifest during a replay. The WAL reader can then report WAL files as truncated when the L0 of the newer manifest holds their rows. If the installed state changed during a replay, the reader ignores the error.

When `refresh` reads a manifest with the same ID as the installed pushed manifest, and the two manifests are equal, the reader drops the base state, because object storage confirmed the pushed manifest. If the two manifests are not equal, object storage is authoritative: the reader builds a state from the stored manifest and installs it. The reader compares the two manifests in their `.manifest` file form, because the file format rounds some values, such as checkpoint times.

### Fallback after a failed read

The application can send a bad manifest. The manifest can refer to SSTs that the garbage collector deleted, or it can come from a different database. The reader handles this case on the read path:

1. A `get` or `scan` reads the installed state and its base state.
2. If the read fails, and `SlateDBError::has_object_store_not_found` returns true, and the installed state is a pushed state, the reader goes back to the base state. It installs the base state only if the installed state is still the same `Arc` that the read used.
3. The reader logs a warning with the pushed manifest ID and increments a metric.
4. The reader retries the read once against the base state, and returns that result.

The reader keeps the base state until a `refresh` installs or confirms a newer loaded state. This means that a later read can also fall back, not only the first read. The cost is that the base state keeps its immutable memtables in memory while pushed states are installed.

A `scan` can fall back only while it builds the iterator. If `DbIterator::next` fails in the middle of a scan, the scan returns the error, because it already returned rows from the pushed state. The reader does not go back to the base state in that case. The next `get` or `scan` that fails for the same reason does.

After a fallback, the reader reports the base manifest as `DbStatus::current_manifest`, even though its ID is less than the ID of the pushed manifest. The application can call `refresh` to load the latest manifest from object storage.

The oracle durable sequence number does not move back after a fallback. The base state holds fewer rows, so reads return a subset of what the oracle permits. This is the same visibility as a reader that has not replayed the WAL yet.

### Example

Writer node:

```rust
let mut status_rx = db.subscribe();
loop {
    status_rx.changed().await?;
    let manifest = status_rx.borrow_and_update().durable_manifest.clone();
    // The application chooses the serde format. Here it is postcard.
    let message = postcard::to_allocvec(&ManifestTransfer::from(&manifest))?;
    transport.broadcast(message).await;
}
```

Reader node:

```rust
let reader = DbReader::open(path, object_store, DbReaderMode::Manual, options).await?;
while let Some(message) = transport.recv().await {
    let manifest = match postcard::from_bytes::<ManifestTransfer>(&message)
        .map_err(|err| err.to_string())
        .and_then(|transfer| VersionedManifest::try_from(transfer).map_err(|err| err.to_string()))
    {
        Ok(manifest) => manifest,
        Err(err) => {
            warn!("dropped a bad manifest message [error={err}]");
            continue;
        }
    };
    match reader.sync_manifest(manifest) {
        Ok(ManifestSyncOutcome::Applied | ManifestSyncOutcome::Stale) => {}
        Err(err) if matches!(err.kind(), ErrorKind::Closed(_)) => break,
        Err(err) => warn!("rejected a manifest [error={err}]"),
    }
}
```

An application that sends at a high rate can send only some updates. For example, it can send one manifest for each N changes of `durable_manifest.id()`. A phase 1 application sends only a signal, and the reader node calls `reader.refresh().await` for each signal.

The reader node can also call `refresh` on its own timer, with a long interval, to read WAL data and to repair the reader after lost messages.

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [x] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [ ] Range deletions
- [x] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [x] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [x] Checkpoints
- [ ] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [x] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

The file format of `.manifest` objects does not change. `to_bytes`, `from_bytes`, and `ManifestTransfer` make that format part of the public API, but the format already has compatibility rules for files.

## Operations

### Performance & Cost

- Latency: `sync_manifest` does CPU work only. The cost is the validation, a filter of the immutable memtables, and one write lock on the reader state. Read latency does not change, except for one retry after a fallback.
- Throughput: No change for reads or writes.
- Object store requests: A manual reader sends no requests between calls. Each `refresh` costs a LIST and a GET for the manifest, and the LIST and GETs of WAL replay. A sync costs no requests.
- Space: The reader keeps its base state in memory while a pushed state is installed. This includes the immutable memtables of the base state.

### Observability

- Configuration: No new options. A new `DbReaderMode` variant.
- Metrics, registered by each `DbReader`:
  - `slatedb.db.manifest_sync_count`: count of calls to `sync_manifest`, with an `outcome` label of `applied`, `stale`, or `rejected`.
  - `slatedb.db.manifest_sync_fallback_count`: count of fallbacks to the base state.
- Logging: A `debug` line for each applied sync, and a `warn` line for each rejected sync and each fallback.

### Compatibility

- Data in object storage: No change.
- Public API: The RFC adds `DbReaderMode::Manual`, `DbReader::refresh`, `DbReader::sync_manifest`, `ManifestSyncOutcome`, `VersionedManifest::to_bytes`, `VersionedManifest::from_bytes`, `ManifestTransfer` with its `From` and `TryFrom` implementations, and `DbStatus::durable_manifest`. The new `DbReaderMode` variant breaks exhaustive matches on `DbReaderMode` in applications. The other changes are additions.
- Serde shape: The two-field shape of `ManifestTransfer` is public API. A change to it is a breaking change.
- Behavior: The existing modes do not change, except that `close` on a `Checkpoint` reader now records the close result. Reads after `close` then fail with `ErrorKind::Closed`, as they do in the other modes.
- Mixed versions: The rules for `.manifest` files apply. A reader must use the same or a newer SlateDB version than the writer, so that it knows the manifest format version.

## Testing

The first implementation includes these tests:

- Unit tests:
  - `to_bytes` and `from_bytes` round trip, with a manifest that holds L0 SSTs, a sorted run, and a segment.
  - `to_bytes` returns the same bytes as the `.manifest` file of that version.
  - `ManifestTransfer` round trip through `serde_json` and `bincode`, then `TryFrom` back to an equal `VersionedManifest`.
  - `TryFrom<ManifestTransfer>` returns `ErrorKind::Invalid` for a transfer that `Deserialize` built with bad bytes.
  - `from_bytes` returns `ErrorKind::Invalid` for empty input, an unknown format version, bad flatbuffer bytes, and bytes that the verifier accepts but the decoder cannot convert.
  - `sync_manifest`: a newer manifest is applied, and an equal or older ID returns `Stale`.
  - `sync_manifest` and `refresh` return `ErrorKind::Invalid` in the three other modes.
  - Each validation rule rejects a manifest and leaves the reader state unchanged.
  - Reads, `refresh`, and `sync_manifest` return `ErrorKind::Closed` after a manual reader closes.
  - Immutable memtables are filtered at the new `last_l0_seq`.
  - The writer reports `durable_manifest` with the ID and the value of the file that it wrote.
  - `install_state` does not replace a newer synced state, and the loaded state becomes the base state.
- Integration tests:
  - A manual reader does not poll, even with a short `manifest_poll_interval`.
  - `refresh` makes new L0 data and WAL data visible.
  - `refresh` keeps a synced manifest that object storage does not hold yet.
  - A writer and a reader share an object store. The test sends `durable_manifest` to the reader, also through `ManifestTransfer` and `serde_json`. The reader sees new L0 data without a refresh.
  - `refresh` drops the base state after it reads the synced manifest from object storage. A synced manifest that holds a checkpoint is confirmed, although the file rounds the checkpoint times.
  - `refresh` replaces a synced manifest when object storage holds a different manifest with the same ID.
  - Fallback: sync a manifest, delete its new SSTs, then `get` or `scan`. The read returns the base state result, and later reads use the base state. A reader without a synced manifest does not fall back.
  - Race: run syncs while another task calls `refresh` in a loop, and make sure that the installed manifest ID never moves back.
  - Races, with a gate that blocks the WAL reads of a replay while a sync lands: the replay drops its rows and does not fail, and the next `refresh` replays them for the synced state.
  - A rebuild for an older manifest while a newer synced state is installed replays the rows between the two L0 boundaries.
  - Bindings: one test for each of Python, Node, Java, and Go that syncs a manifest and then refreshes.

These tests are follow-up work, after the design is final:

- Deterministic simulation tests: add a manual reader that gets manifests by push to `slatedb-dst`, with delayed, dropped, and reordered messages.
- Property tests: round trip `to_bytes` and `from_bytes` over generated manifests. `proptest_util` has no manifest strategy today.

## Rollout

- Phase 1: `DbReaderMode::Manual` and `DbReader::refresh`. This phase solves the first problem in the [Motivation](#motivation), and it has no new trust or format questions.
- Phase 2: `VersionedManifest::to_bytes` and `from_bytes`, `ManifestTransfer`, `DbStatus::durable_manifest` on the writer, `DbReader::sync_manifest`, and the fallback path.
- Bindings follow each phase. The binding `ReaderMode` enum gets a `Manual` variant, and the binding `DbReader` gets `refresh`. Bindings do not use serde, so the `ManifestTransfer` binding record has public `id` and `bytes` fields. `Db::durable_manifest` returns the durable manifest as a `ManifestTransfer`, and `DbReader::sync_manifest` takes one. The binding `DbStatus` record gets a `durable_manifest_id` field, so that an application can call `status` to detect a new durable manifest without encoding the manifest on each call.
- Feature flag: None. The feature is active only when the application opens a reader in `DbReaderMode::Manual`.
- Docs: Update `website/src/content/docs/docs/design/readers.mdx` with sections on the manual mode and on manifest sync.

## Alternatives

Status quo with a short poll interval. This needs no new API. Each poll costs a LIST and a GET, and an idle database pays the same cost as a busy one. Staleness cannot go below the poll interval.

Only phase 1, `refresh` without `sync_manifest`. This solves the first problem with much less code and no trust problem. Each update still costs object store requests for each reader, and the read waits for that IO. Phase 2 is separate, so the project can accept phase 1 alone.

A refresh to a known ID, `DbReader::refresh_to(id).await`. The application sends only the version ID, and the reader does one GET for that manifest file. This removes the manifest LIST. WAL replay still lists the WAL directory, unless `skip_wal_replay` is set. A later change can add this method next to `refresh`.

`sync_manifest` in every reader mode. An earlier draft of this RFC let all modes accept a sync. In `ManagedCheckpoint` mode, each sync then started a poll that read the manifest and moved the checkpoint, so a sync saved no requests. The sync also raced with the poller in `ManagedCheckpoint` and `FollowLatest` modes, which needed conditional installs in the poller, and it moved a `Checkpoint` reader off its pinned manifest. The manual mode removes these interactions.

`refresh` in `FollowLatest` mode. This polls at once without a wait for the next tick. A later change can add it, but a reader that the application refreshes on demand does not need a poller.

Change `DbStatus::current_manifest` to hold only durable manifests, and do not add a field. This keeps `DbStatus` small. It changes the meaning of an existing field for current users, such as the cache warming case in [#1405](https://github.com/slatedb/slatedb/issues/1405), which wants the newest view.

A SlateDB message format, with `encode`, `decode`, and `TryFrom<&[u8]>`. The message is an envelope with a format version, the version ID, a CRC32, and the manifest bytes. This gives every application the same integrity check. It also adds a second format that SlateDB must keep compatible, and it makes framing choices that belong to the application. The application can add the ID and a checksum to its own messages with little code.

A public constructor that takes the manifest fields. A manifest is built from about a dozen crate-private types, such as `ManifestCore`, `LsmTreeState`, SST views, checkpoints, and the sequence tracker. A public constructor makes all of them public, and every internal change to the manifest becomes a breaking API change.

Use `serde` with `Deserialize`. `VersionedManifest` derives `Serialize` today, for JSON output. A serde format makes the bytes depend on the internal Rust types, so every internal change becomes a format change. Writers and readers with different SlateDB versions can then fail to agree. The flatbuffer format already has a versioned schema with compatibility rules.

Only `to_bytes` and `from_bytes`, without `ManifestTransfer`. This adds one less public type. Each application must then write its own type to carry the ID next to the bytes. `ManifestTransfer` is small, and it gives every serde application the same type.

A transfer type with only the fields that reads use. The read path needs the version ID, `last_l0_seq`, `replay_after_wal_id`, `next_wal_sst_id`, `segment_extractor_name`, and the full LSM tree of the root and each segment, with the `SsTableInfo` of each SST. Without `SsTableInfo`, the reader must read the footer of each new SST from object storage. The fields that a transfer type can drop are small, so the saving is small. The reader must also fill the dropped fields from its old manifest, so `DbReader::manifest()` and `DbReader::subscribe()` show a manifest that matches no stored version.

Store the version ID in the manifest bytes. The `ManifestV2` flatbuffer has a `manifest_id` field, but the writer sets it to 0 with a note to remove the field, because the ID is in the file name. If the writer filled the field, the bytes alone identify the version, and `ManifestTransfer` reduces to the bytes. `ObjectCodec::encode` gets only the value and not the ID, so the writer needs the ID in the codec or in the value. Files that the writer already stored hold 0, so the reader still needs the ID from the file name for those. This RFC keeps the ID outside the bytes and leaves the field as it is. See [Open Questions](#open-questions).

A delta from a base version. The writer sends only the SSTs that changed since a base manifest ID, and the reader rebuilds the manifest from its installed manifest. This can save much more than a partial manifest. It needs a base ID in each message, and a full resend when the reader does not have the base, for example after a lost message. This RFC leaves deltas for later work.

## Open Questions

- Must the `manifest_id` field of `ManifestV2` hold the version ID, or must a later change remove it? If it holds the ID, `ManifestTransfer` can carry the bytes alone.
- Must the reader keep the base state until the next loaded state, or drop it after the first successful read against a pushed state? This RFC keeps it, because one good read does not prove that all SSTs of the manifest exist.
- Must `refresh` take an optional manifest ID, so that the reader does one GET and no LIST for the manifest?
- Must SlateDB give a way to reject a manifest from a different database? The manifest has no database identity field today, and this RFC leaves the message format to the application.
- Must a later RFC add deltas, so that the writer sends only the SSTs that changed?

## References

- [Readers design doc](https://slatedb.io/docs/design/readers/)
- [RFC-0001: Manifest](https://github.com/slatedb/slatedb/blob/main/rfcs/0001-manifest.md)
- [RFC-0004: Checkpoints](https://github.com/slatedb/slatedb/blob/main/rfcs/0004-checkpoints.md)
- [#1405: add manifest updates to db.subscribe()](https://github.com/slatedb/slatedb/issues/1405)
- [#1408: Make `Db::subscribe` and `Db::status` available on `DbReader`](https://github.com/slatedb/slatedb/issues/1408)
- [#1636: Support temporary memtables for DbReader](https://github.com/slatedb/slatedb/issues/1636)
- [#1823: Use exponential HEAD probe for manifest polling](https://github.com/slatedb/slatedb/issues/1823)
- [#1069: Consider dropping the manifest flatbuffer verification](https://github.com/slatedb/slatedb/issues/1069)
- [#1331: DbReader manifest poll loop silently dies on transient errors](https://github.com/slatedb/slatedb/issues/1331)
