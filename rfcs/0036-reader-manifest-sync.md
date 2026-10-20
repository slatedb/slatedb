# Push Manifest Updates to DbReader

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

- [Summary](#summary)
- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
  - [Terms](#terms)
  - [Overview](#overview)
  - [Transfer format for VersionedManifest](#transfer-format-for-versionedmanifest)
  - [Durable manifest on DbStatus](#durable-manifest-on-dbstatus)
  - [The sync_manifest API](#the-sync_manifest-api)
  - [Validation](#validation)
  - [State change without IO](#state-change-without-io)
  - [Behavior in each reader mode](#behavior-in-each-reader-mode)
  - [Races with the manifest poller](#races-with-the-manifest-poller)
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

This RFC adds `DbReader::sync_manifest`. The method takes a `VersionedManifest` that the application received from the writer through its own channel, such as a network message. The reader installs the manifest in memory without object store IO, and the next read uses it. The method works in every `DbReaderMode` and does not change the mode. The RFC also adds `ManifestTransfer`, a serde type that carries a `VersionedManifest` as bytes in the existing `.manifest` file format, and a `DbStatus` field that holds only manifests that are durable in object storage. If the first reads against a pushed manifest fail because an object is missing, the reader goes back to the last manifest that it loaded from object storage.

## Motivation

A `DbReader` learns about new manifests in one of two ways today:

- In `DbReaderMode::Checkpoint`, the reader stays on one manifest for its full life.
- In `DbReaderMode::ManagedCheckpoint` and `DbReaderMode::FollowLatest`, a background poller reads the latest manifest from object storage once every `manifest_poll_interval`.

The poll interval sets a floor on reader staleness. Each poll also costs object store requests: a LIST and a GET for the manifest, and in managed mode a manifest write when the checkpoint moves. A short interval makes readers fresh but costs many requests when the database is idle. A long interval saves requests but makes readers stale when the writer is busy.

Consider a cluster with one writer node and many reader nodes. The writer processes thousands of writes per second. The application already has a network path between the nodes. A task on the writer node reads `Db::subscribe` and sends each new manifest to the reader nodes. The application controls the send rate. It can send often when the write rate is high and stop when the database is idle. Each reader node then installs the manifest that it receives and serves reads from it at once.

The application cannot build this today, for three reasons:

1. `DbReader` has no method that accepts a manifest from outside.
2. `VersionedManifest` implements `Serialize` but has no public decoder. The flatbuffer codec for the manifest is `pub(crate)`.
3. `DbStatus::current_manifest` on the writer is the in-memory manifest. The writer reports it before the manifest write completes, and it labels it with the version ID of the previous manifest (see `report_to_status_manager` in `slatedb/src/memtable_flusher/manifest_writer.rs`). Two different manifests can carry the same ID, so a reader cannot use the ID to order them.

## Goals

- Add `DbReader::sync_manifest`, which installs a manifest without object store IO.
- Keep the configured `DbReaderMode` and its poller unchanged when the application calls `sync_manifest`.
- Reject manifests that are older than the installed manifest, or that fail the local validation rules.
- If reads against a pushed manifest fail because an object is missing, go back to the last manifest that the reader loaded from object storage.
- Add public methods that convert a `VersionedManifest` to and from bytes in the `.manifest` file format, and a serde type, `ManifestTransfer`, that carries those bytes.
- Give writers a `DbStatus` field that holds only durable manifests with correct version IDs.

## Non-Goals

- A transport or message format for manifests. The application owns the network path, framing, integrity checksums, batching, and retries.
- Push of WAL data or memtable data to readers. [#1636](https://github.com/slatedb/slatedb/issues/1636) covers temporary memtables for readers.
- Proof that a pushed manifest belongs to the same database as the reader. The manifest has no database identity field. The fallback path limits the damage from a wrong manifest.
- Changes to the manifest poller cost. [#1823](https://github.com/slatedb/slatedb/issues/1823) covers a cheaper poll.
- Garbage collection protection for a pushed manifest before the reader moves its own checkpoint. See [Behavior in each reader mode](#behavior-in-each-reader-mode).

## Design

### Terms

- Pushed manifest: a `VersionedManifest` that the application gives to `sync_manifest`.
- Loaded manifest: a manifest that the reader read from object storage itself, at open time or in the poller.
- Base state: the most recent reader state that the reader built from a loaded manifest.
- Pushed state: a reader state that the reader built from a pushed manifest.

### Overview

```text
 writer node                                   reader node
 ───────────                                   ───────────
 Db::subscribe()
   └─ DbStatus.durable_manifest  ──ManifestTransfer::from──▶  app transport  ──VersionedManifest::try_from──▶ VersionedManifest
                                                                                  │
                                                                                  ▼
                                                              DbReader::sync_manifest(manifest)
                                                                 1. validate (CPU only)
                                                                 2. build pushed state (CPU only)
                                                                 3. swap state under the write lock
                                                                 4. keep base state for fallback
                                                                 5. wake the poller (managed mode)
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

1. Keep the `checkpoint` field of the installed state. The pushed manifest does not move the checkpoint.
2. Set `manifest_id` and `manifest` from the pushed manifest.
3. Filter the immutable memtables. Drop each table whose `last_seq` is less than or equal to the new `last_l0_seq`. Keep each table that is fully newer. For a table that spans the boundary, call `filter_after_seq`. This is the same logic that `rebuild_state` uses today. The implementation moves it into a shared function.
4. Set `last_wal_id` and `last_remote_persisted_seq` with `replayed_watermark`, from the new manifest core and the filtered tables.

The reader does not replay the WAL in `sync_manifest`. WAL replay needs object store IO. The poller replays newer WAL SSTs on its next tick, as it does today.

The reader then does these steps while it holds the state write lock:

1. Make sure that the installed state did not change since step 1 of the build. If it changed, rebuild from the new installed state. The build is CPU only, so a retry is cheap.
2. If no base state is stored, store the installed state as the base state.
3. Advance the durable sequence number in the oracle. A read that uses the pushed state must not use the older durable sequence number, because it can then skip rows of the pushed manifest.
4. Install the pushed state.
5. Report the manifest to the status manager. Every change of the installed state reports the status while it holds the lock, so that the status matches the state.

### Behavior in each reader mode

The mode decides what the poller does after a sync. The mode does not change.

`FollowLatest`: The poller already reads the latest manifest and calls `apply_latest_manifest`. That function ignores a manifest whose ID is not greater than the installed ID, and only replays new WAL SSTs. After a sync, the poller keeps this behavior. When the poller installs a loaded manifest with a greater ID, the reader drops the old base state. When the poller reads a manifest with the same ID as the installed pushed manifest, and the two manifests are equal, the reader also drops the base state, because object storage confirmed the pushed manifest. If the two manifests are not equal, object storage is authoritative: the reader builds a state from the stored manifest and installs it. The reader compares the two manifests in their `.manifest` file form, because the file format rounds some values, such as checkpoint times.

`ManagedCheckpoint`: The reader checkpoint protects the SSTs of the checkpoint manifest from garbage collection. After a sync, the reader reads from a newer manifest that the checkpoint does not cover. The reader sends a `ManifestSynced` message to the poller. The message channel holds one message, so many syncs cause one poll. The poller then does these steps at once, and does not wait for the next tick:

1. Load the latest manifest from object storage.
2. If the ID of the latest manifest is less than the installed ID, do not move the checkpoint. Object storage can return an older manifest only if the pushed manifest is not durable yet. The next poll does this test again. This rule prevents a checkpoint write on each poll for a manifest that object storage never holds.
3. Otherwise, replace the reader checkpoint with a checkpoint on the latest manifest, and rebuild the state from the new checkpoint as `reestablish_checkpoint` does today. The rebuilt state is a loaded state, so the reader drops the base state.

In managed mode, an error from a poll closes the reader today. A poll that a sync starts is different: if it fails, the reader logs the error and keeps the pushed state, and the next tick retries. A sync then adds no new way for the reader to close.

The poller must also use the checkpoint manifest ID, not the installed manifest, when it decides to move the checkpoint. Today, `should_reestablish_checkpoint` compares the latest manifest with the installed state. After a sync, the two can be equal while the checkpoint still points at an older manifest.

The SSTs of a pushed manifest are not protected by the reader checkpoint until step 3 completes. The garbage collector does not delete a compacted SST until it is older than `compacted_options.min_age`, which is 24 hours by default. The window between a sync and step 3 is one manifest read and one manifest write.

`Checkpoint(id)`: The poller does nothing in this mode today, and it continues to do nothing. A pinned reader replays the WAL only to the boundary of its manifest. After a sync, the reader reads the L0 SSTs and sorted runs of the pushed manifest, and the WAL rows that it already replayed and that are newer than that L0. It does not replay WAL SSTs that the pushed manifest records. The pinned checkpoint stays the base state for the full life of the reader. The reader does not create or move checkpoints in this mode, so the application is responsible for garbage collection safety of pushed manifests. A pinned reader that gets a manifest which the garbage collector cleaned up falls back to the pinned checkpoint.

### Races with the manifest poller

The poller reads the installed state, awaits IO, and then installs a new state. A sync can install a newer state during the await. Today, `install_state` and `maybe_replay_new_wals` replace the state without a test of the current value. The poller can then put back an older manifest.

The RFC changes both functions to install only if the state did not move:

- `install_state` keeps the state that has the greater manifest ID. If the IDs are equal, it keeps the new state, because that state includes WAL replay. If it keeps the installed pushed state, it copies the checkpoint of the new state to it, because the poller can have replaced the checkpoint during the await. The new state also becomes the base state of the pushed state, because it is the newest state from object storage and the new checkpoint covers it.
- `maybe_replay_new_wals` installs its result as before if the installed state is still the same `Arc` that it read. If a sync installed a newer state during the replay, the installed state holds every row up to the watermark of the old state, and the replayed rows are newer. The reader adds the replayed rows that the L0 of the new manifest does not hold. If a fallback installed an older state, the reader drops the result, because the rows of the older state can end before the replayed rows start. The next tick replays the WAL again.
- `rebuild_state` builds a state for a loaded manifest from the tables of the installed state. If the installed state is a pushed state that is newer than the loaded manifest, its tables miss the rows between the two L0 boundaries. The reader then replays those rows from the WAL.

The WAL reader compares WAL IDs with the newest manifest that the reader reported to its status. A sync can report a newer manifest during a replay. The WAL reader can then report WAL files as truncated when the L0 of the newer manifest holds their rows. If the installed state changed during a replay or a rebuild, the reader ignores the error. In `ManagedCheckpoint` mode, it stores the new checkpoint on the installed state, because the poller already replaced the old one.

### Fallback after a failed read

The application can send a bad manifest. The manifest can refer to SSTs that the garbage collector deleted, or it can come from a different database. The reader handles this case on the read path:

1. A `get` or `scan` reads the installed state and its base state.
2. If the read fails, and `SlateDBError::has_object_store_not_found` returns true, and the installed state is a pushed state, the reader goes back to the base state. It installs the base state only if the installed state is still the same `Arc` that the read used.
3. The reader logs a warning with the pushed manifest ID and increments a metric.
4. The reader retries the read once against the base state, and returns that result.

The reader keeps the base state until it installs a newer loaded state. This means that a later read can also fall back, not only the first read. The cost is that the base state keeps its immutable memtables in memory while pushed states are installed.

A `scan` can fall back only while it builds the iterator. If `DbIterator::next` fails in the middle of a scan, the scan returns the error, because it already returned rows from the pushed state. The reader does not go back to the base state in that case. The next `get` or `scan` that fails for the same reason does.

After a fallback, the reader reports the base manifest as `DbStatus::current_manifest`, even though its ID is less than the ID of the pushed manifest. In `ManagedCheckpoint` mode, the reader also wakes the poller, so that the poller makes sure that the checkpoint covers the installed state.

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

An application that sends at a high rate can send only some updates. For example, it can send one manifest for each N changes of `durable_manifest.id()`.

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
- Object store requests: A reader that gets manifests by push can use a long `manifest_poll_interval`. This reduces LIST and GET requests. In managed mode, each applied sync causes one manifest read and one checkpoint write. An application that syncs often in managed mode can pay more requests than with polls. `FollowLatest` and `Checkpoint` modes cause no requests on a sync.
- Space: The reader keeps its base state in memory while a pushed state is installed. This includes the immutable memtables of the base state.

### Observability

- Configuration: No new options.
- Metrics, registered by each `DbReader`:
  - `slatedb.db.manifest_sync_count`: count of calls to `sync_manifest`, with an `outcome` label of `applied`, `stale`, or `rejected`.
  - `slatedb.db.manifest_sync_fallback_count`: count of fallbacks to the base state.
- Logging: A `debug` line for each applied sync, and a `warn` line for each rejected sync and each fallback.

### Compatibility

- Data in object storage: No change.
- Public API: The RFC adds `DbReader::sync_manifest`, `ManifestSyncOutcome`, `VersionedManifest::to_bytes`, `VersionedManifest::from_bytes`, `ManifestTransfer` with its `From` and `TryFrom` implementations, and `DbStatus::durable_manifest`. These are additions only.
- Serde shape: The two-field shape of `ManifestTransfer` is public API. A change to it is a breaking change.
- Behavior: The checkpoint move in managed mode now uses the checkpoint manifest ID. Without a sync, the checkpoint manifest ID is equal to the installed manifest ID, so the behavior does not change.
- Mixed versions: The rules for `.manifest` files apply. A reader must use the same or a newer SlateDB version than the writer, so that it knows the manifest format version.

## Testing

The first implementation includes these tests:

- Unit tests:
  - `to_bytes` and `from_bytes` round trip, with a manifest that holds L0 SSTs, a sorted run, and a segment.
  - `to_bytes` returns the same bytes as the `.manifest` file of that version.
  - `ManifestTransfer` round trip through `serde_json` and `bincode`, then `TryFrom` back to an equal `VersionedManifest`.
  - `TryFrom<ManifestTransfer>` returns `ErrorKind::Invalid` for a transfer that `Deserialize` built with bad bytes.
  - `from_bytes` returns `ErrorKind::Invalid` for empty input, an unknown format version, and bad flatbuffer bytes.
  - `sync_manifest` in each of the three modes: a newer manifest is applied, and an equal or older ID returns `Stale`.
  - Each validation rule rejects a manifest and leaves the reader state unchanged.
  - `sync_manifest` returns `ErrorKind::Closed` after the reader closes.
  - Immutable memtables are filtered at the new `last_l0_seq`.
  - The writer reports `durable_manifest` with the ID and the value of the file that it wrote.
  - `install_state` does not replace a newer synced state, and copies the new checkpoint to it.
- Integration tests:
  - A writer and a reader share an object store. The test sends `durable_manifest` to the reader, also through `ManifestTransfer` and `serde_json`. The reader sees new L0 data without a poll.
  - Managed mode: after a sync, the poller moves the checkpoint to a manifest with an ID greater than or equal to the pushed ID.
  - Managed mode: if object storage does not hold the synced manifest yet, the poller does not move the checkpoint.
  - Follow-latest mode: a poll that reads the synced manifest drops the base state.
  - Fallback: sync a manifest, delete its new SSTs, then `get` or `scan`. The read returns the base state result, and later reads use the base state. A reader without a synced manifest does not fall back.
  - Race: run syncs against a poller that ticks every 5 ms, in managed and follow-latest modes, and make sure that the installed manifest ID never moves back.
  - Bindings: one sync test for each of Python, Node, Java, and Go.
  - Races, with a gate that blocks the WAL reads of a replay while a sync lands: the replay adds its rows to the synced state, and a replay error after the sync does not fail the poll.
  - A rebuild for an older manifest while a newer synced state is installed replays the rows between the two L0 boundaries.
  - Follow-latest mode: a poll that reads a different manifest with the ID of the synced manifest installs the stored manifest. A synced manifest that holds a checkpoint is confirmed, although the file rounds the checkpoint times.
  - `from_bytes` returns `ErrorKind::Invalid` for bytes that the verifier accepts but the decoder cannot convert.
- Fault injection: fail the checkpoint write that a sync starts. The poll does not fail the reader. The reader keeps serving reads from the synced manifest, and the next poll moves the checkpoint.

These tests are follow-up work, after the design is final:

- Deterministic simulation tests: add a reader that gets manifests by push to `slatedb-dst`, with delayed, dropped, and reordered messages.
- Property tests: round trip `to_bytes` and `from_bytes` over generated manifests. `proptest_util` has no manifest strategy today.

## Rollout

- Phase 1: `VersionedManifest::to_bytes` and `from_bytes`, `ManifestTransfer`, and `DbStatus::durable_manifest` on the writer.
- Phase 2: `DbReader::sync_manifest`, the conditional install in the poller, and the fallback path.
- Phase 3: Bindings. Bindings do not use serde, so the `ManifestTransfer` binding record has public `id` and `bytes` fields. `Db::durable_manifest` returns the durable manifest as a `ManifestTransfer`, and `DbReader::sync_manifest` takes one. The binding `DbStatus` record gets a `durable_manifest_id` field, so that an application can call `status` to detect a new durable manifest without encoding the manifest on each call.
- Feature flag: None. The feature is active only when the application calls the new method.
- Docs: Update `website/src/content/docs/docs/design/readers.mdx` with a section on manifest sync, and add a note to the description of pinned readers.

## Alternatives

Status quo with a short poll interval. This needs no new API. Each poll costs a LIST and a GET, and an idle database pays the same cost as a busy one. Staleness cannot go below the poll interval.

A manual refresh method, `DbReader::refresh().await`. The application sends a small "new manifest" signal, and the reader reads the latest manifest from object storage. This method has no trust problem, and the application does not send manifest bytes. It costs object store requests on each call, and the read waits for that IO. The design in this RFC does not prevent this method, and the two can exist together.

A refresh to a known ID, `DbReader::refresh_to(id).await`. The application sends only the version ID, and the reader does one GET for that manifest file. This removes the LIST, but each update still costs a GET and the IO latency.

Change `DbStatus::current_manifest` to hold only durable manifests, and do not add a field. This keeps `DbStatus` small. It changes the meaning of an existing field for current users, such as the cache warming case in [#1405](https://github.com/slatedb/slatedb/issues/1405), which wants the newest view.

A new `DbReaderMode::Push` that turns off the poller. This makes push and poll exclusive. The use case needs both, so that the poller can repair a reader after lost messages and can move checkpoints. An application that wants push only can set a long `manifest_poll_interval`.

A SlateDB message format, with `encode`, `decode`, and `TryFrom<&[u8]>`. The message is an envelope with a format version, the version ID, a CRC32, and the manifest bytes. This gives every application the same integrity check. It also adds a second format that SlateDB must keep compatible, and it makes framing choices that belong to the application. The application can add the ID and a checksum to its own messages with little code.

A public constructor that takes the manifest fields. A manifest is built from about a dozen crate-private types, such as `ManifestCore`, `LsmTreeState`, SST views, checkpoints, and the sequence tracker. A public constructor makes all of them public, and every internal change to the manifest becomes a breaking API change.

Use `serde` with `Deserialize`. `VersionedManifest` derives `Serialize` today, for JSON output. A serde format makes the bytes depend on the internal Rust types, so every internal change becomes a format change. Writers and readers with different SlateDB versions can then fail to agree. The flatbuffer format already has a versioned schema with compatibility rules.

Only `to_bytes` and `from_bytes`, without `ManifestTransfer`. This adds one less public type. Each application must then write its own type to carry the ID next to the bytes. `ManifestTransfer` is small, and it gives every serde application the same type.

A transfer type with only the fields that reads use. The read path needs the version ID, `last_l0_seq`, `replay_after_wal_id`, `next_wal_sst_id`, `segment_extractor_name`, and the full LSM tree of the root and each segment, with the `SsTableInfo` of each SST. Without `SsTableInfo`, the reader must read the footer of each new SST from object storage. The fields that a transfer type can drop are small, so the saving is small. The reader must also fill the dropped fields from its old manifest, so `DbReader::manifest()` and `DbReader::subscribe()` show a manifest that matches no stored version.

A delta from a base version. The writer sends only the SSTs that changed since a base manifest ID, and the reader rebuilds the manifest from its installed manifest. This can save much more than a partial manifest. It needs a base ID in each message, and a full resend when the reader does not have the base, for example after a lost message. This RFC leaves deltas for later work.

## Open Questions

- Must pinned readers (`DbReaderMode::Checkpoint`) accept `sync_manifest`? This RFC says yes, because the use case asks for it. The current docs say that a pinned reader does not follow newer manifests. An opt-in option on `DbReaderOptions` is a possible middle path.
- Must the reader keep the base state until the next loaded state, or drop it after the first successful read against a pushed state? This RFC keeps it, because one good read does not prove that all SSTs of the manifest exist.
- Must `sync_manifest` trigger WAL replay at once in `FollowLatest` and `ManagedCheckpoint` modes, or wait for the next tick? This RFC waits, because the pushed manifest already covers data up to its `last_l0_seq`.
- Must managed mode limit the rate of checkpoint writes after syncs, for example to one for each `manifest_poll_interval`?
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
