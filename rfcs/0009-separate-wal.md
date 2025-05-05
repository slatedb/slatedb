# Separate Object Store for WAL

Status: Approved

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
        main_object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Result<Self, SlateDBError> {
        // ...
    }

    // ...
}
```

Once RFC-10 "Settings design" is implemented, the WAL object store configuration
should be moved to the builder proposed in RFC-10. With this more scalable approach
the parameter set of `open_with_opts` will be nice and clean.

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
    main_object_store: Arc<dyn ObjectStore>,
    /// The dedicated WAL object store, if any.
    wal_object_store: Option<Arc<dyn ObjectStore>>,

    /// The transactional store wrapper of the main object store.
    main_transactional_store: Arc<dyn TransactionalObjectStore>,
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
                self.main_object_store.clone()
            }
        SsTableId::Compacted(..) => self.main_object_store.clone(),
    }
}
```

The transactional object store selection logic would look about the same.

### Block and Object Cache

WAL requires no caching support since it is read only once at startup. All
operations on the dedicated WAL object store should be direct and bypass all
the caches.

### Cloning Support

Public API methods like `admin::create_clone` could just accept an explicitly
passed WAL object store instance, but this is undesirable because that introduces
another point for a potential misconfiguration. Instead, a reference to the object
store should be included in the associated `Manifest`, so the information about
the WAL store is always encapsulated in the database itself.

`Manifest`-s are (de)serializable while `ObjectStore` instances are not. There is
some support present in `object_store` crate for creating object stores from URIs,
specifically `object_store::parse_url(_opts)`, but it's very limited in features.
For instance, it's impossible to configure a store using a URI alone. At the same
time, it's impossible to re-adjust the configuration after the store creation.
The configuration must be fully known before the construction and passed either
to `parse_url_opts` or to some other custom construction procedure.

Additionally, there is no backward conversion from an object store instance to a
URI. Getting store configuration information from an instance is even more
problematic because the configuration details are private to the store instance.
To avoid this problematic conversion, URIs must be used everywhere along with the
configuration information encoded in the URIs themselves or as standalone objects.
Alternatively, the store configuration might be obtained via some user-provided
callbacks mechanism. Also, there should be a centralized URI-to-store resolver to
construct and cache store instances.

All this raises many open questions that are not directly related to this RFC.
Exact implementation details will be provided in a separate RFC/PR dedicated to
the descriptive representation of store references and configurations.
