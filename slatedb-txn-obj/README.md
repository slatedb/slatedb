# SlateDB Transactional Object (slatedb-txn-obj)

Reusable transactional object primitives for durable stores. This crate provides optimistic
concurrency control (CAS), versioned objects, and optional epoch fencing. SlateDB uses this
crate to implement its metadata storage layer. The crate is designed to be storage-agnostic,
with an example implementation backed by an object store.

## Overview

- `TransactionalStorageProtocol`: Trait for reading the latest version and writing with CAS.
- `SequencedStorageProtocol`: Extension that supports reading/listing versions by ID.
- `SimpleTransactionalObject`: In-memory view with `refresh`, `prepare_dirty`, and `update`.
- `FenceableTransactionalObject`: Adds epoch-based writer fencing.
- `ObjectStoreSequencedStorageProtocol`: Object store implementation using versioned files.

## Object Store Layout

`ObjectStoreSequencedStorageProtocol` persists versions as files:

- Path pattern: `<root>/<subdir>/<20-digit-id>.<suffix>`
- Example: `00000000000000000001.manifest`
- Writes use create-if-not-exists semantics for CAS
- Listings return sorted metadata in the requested ID range

## Usage

Add to `Cargo.toml`:

```toml
[dependencies]
slatedb-txn-obj = "*"
slatedb-common = "*"
object_store = "*"
bytes = "*"
tokio = { version = "*", features = ["macros", "rt", "time"] }
```

### Basic transactional object

```rust
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use slatedb_txn_obj::object_store::ObjectStoreSequencedStorageProtocol;
use slatedb_txn_obj::{
    MonotonicId, ObjectCodec, SimpleTransactionalObject, TransactionalObject,
    TransactionalStorageProtocol,
};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    epoch: u64,
    payload: u64,
}

struct ConfigCodec;

impl ObjectCodec<Config> for ConfigCodec {
    fn encode(&self, value: &Config) -> Bytes {
        Bytes::from(format!("{}:{}", value.epoch, value.payload))
    }

    fn decode(
        &self,
        bytes: &Bytes,
    ) -> Result<Config, Box<dyn std::error::Error + Send + Sync>> {
        let s = std::str::from_utf8(bytes)?;
        let mut parts = s.split(':');
        let epoch: u64 = parts.next().ok_or("missing epoch")?.parse()?;
        let payload: u64 = parts.next().ok_or("missing payload")?.parse()?;
        Ok(Config { epoch, payload })
    }
}

async fn example() -> Result<(), slatedb_txn_obj::TransactionalObjectError> {
    let object_store = Arc::new(InMemory::new());
    let store = Arc::new(ObjectStoreSequencedStorageProtocol::new(
        &Path::from("/root"),
        object_store,
        "configs", // subdirectory
        "cfg", // file suffix
        Box::new(ConfigCodec),
    ));

    let mut txn = SimpleTransactionalObject::init(
        Arc::clone(&store) as Arc<dyn TransactionalStorageProtocol<Config, MonotonicId>>,
        Config {
            epoch: 0,
            payload: 1,
        },
    )
    .await?;

    let mut dirty = txn.prepare_dirty()?;
    dirty.value.payload += 1;
    txn.update(dirty).await?;

    Ok(())
}
```

### Epoch fencing

Continuing from the previous example:

```rust
use slatedb_common::clock::DefaultSystemClock;
use slatedb_txn_obj::FenceableTransactionalObject;
use std::sync::Arc;
use std::time::Duration;

async fn fenced(
    txn_obj: SimpleTransactionalObject<Config, MonotonicId>,
) -> Result<
    FenceableTransactionalObject<Config, MonotonicId>,
    slatedb_txn_obj::TransactionalObjectError,
> {
    FenceableTransactionalObject::init(
        txn_obj,
        Duration::from_secs(5),
        Arc::new(DefaultSystemClock::new()),
        |v: &Config| v.epoch,
        |v: &mut Config, epoch| v.epoch = epoch,
    )
    .await
}
```

### Retrying on conflicts

`maybe_apply_update` retries on version conflicts by refreshing and re-applying the mutation.

```rust
txn.maybe_apply_update(|current| {
    let mut next = current.object().clone();
    next.payload += 1;
    let mut dirty = current.prepare_dirty()?;
    dirty.value = next;
    Ok(Some(dirty))
})
.await?;
```

## Error Semantics

- `ObjectVersionExists`: CAS failed because another writer won the race.
- `LatestRecordMissing`: No object exists yet (for `load`).
- `InvalidObjectState`: Unexpected storage layout or missing data.
- `Fenced`: A newer epoch has been observed.
- `ObjectUpdateTimeout`: Update did not complete within the provided deadline.

## Feature Flags

- `test-util`: Enables small test helpers like `test_utils::new_dirty_object`.
