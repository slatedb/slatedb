# Separate Object Store for WAL

Status: Draft

Authors:

* [Sergei Sitnikov](https://github.com/taburet)

## Motivation

Sequential writes are a common pattern. Combined with `await_durable` set to
`true`, which is the default, WAL performance is the main limiting factor for
the sequential writes throughput. Even if `flush_interval` is set to an extremely
low value, it can't overcome the inherent latency of the underlying object store.

The obvious solution is switching to a lower latency object store; for instance,
from S3 to S3 Express One Zone. But lower latency stores usually come with a cost,
they might be significantly more expensive or might be limited in features. Using
such stores for the main store containing the entire database might be undesirable.

## Goals

- Support an optional separate object store dedicated specifically for WAL.
- Provide a user-facing configuration mechanism.
- Avoid significant refactorings.

## Public API

If user wants to use a separate WAL object store, the store instance should be
provided while constructing a `Db`:

```rust
impl Db {
    // ...

    /// ...
    ///
    /// ## Arguments
    /// ...
    /// - `wal_object_store`: an optional [object store](ObjectStore) instance
    ///   dedicated specifically for WAL.
    ///
    ///   If set to `None`, the passed `object_store` will be used for WAL storage.
    ///
    ///   NOTE: WAL durability and availability properties depend on the properties
    ///         of the underlying object store. Make sure the configured object
    ///         store is durable and available enough for your use case.
    ///
    /// ...

    pub async fn open_with_opts<P: Into<Path>>(
        path: P,
        options: DbOptions,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Result<Self, SlateDBError> {
        // ...
    }

    // ...
}
```

## Implementation

The configured WAL object store is propagated down to `TableStore` instance
which internally will select the appropriate object store for each operation
based solely on the `SsTableId` involved in the operation. The object path
structure will remain unchanged for both stores.

There is already `transactional_wal_store` field in `TableStore`. The naming
is misleading because apparently this store is used not only for WAL-related
operations. The field should be renamed, the resulting field layout will look
like this:

```rust
pub struct TableStore {
    // ...

    /// The main object store.
    object_store: Arc<dyn ObjectStore>,
    /// The dedicated WAL object store, if any.
    wal_object_store: Option<Arc<dyn ObjectStore>>,

    /// The transactional store wrapper of the main object store.
    transactional_store: Arc<dyn TransactionalObjectStore>,
    /// The transactional store wrapper of the dedicated WAL object store, if any.
    wal_transactional_store: Option<Arc<dyn TransactionalObjectStore>>
}
```

The object store selection procedure will look like this:

```rust
fn object_store_for(&self, id: &SsTableId) -> Arc<dyn ObjectStore> {
    match id {
        SsTableId::Wal(..) =>
            if let Some(wal_object_store) = &self.wal_object_store {
                wal_object_store.clone()
            } else {
                self.object_store.clone()
            }
        SsTableId::Compacted(..) => self.object_store.clone(),
    }
}
```

The transactional object store selection logic would look about the same.

### Block and Object Cache

WAL requires no caching support since it is read only once at startup. All
operations on the dedicated WAL object store should be direct and bypass all
the caches.
