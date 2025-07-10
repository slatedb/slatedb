# Settings design

Status: Approved

Authors:

* [David Calavera](https://github.com/calavera)

## Motivation

SlateDB is an embedded storage engine built on top of object storage. Since it's designed to be used in many different environments, SlateDB exposes a number of configuration options to use fine-tune the engine for the environment it's running in. It also exposes several components that users can swap with their own implementations to customize the engine, for example the `object_store` implementation, or the `clock` implementation.

These configuration options and components are somehow intertwined. Some components are included in the `DbOptions` struct and its related structs. Others are passed as parameters to the `Db::open` method.

This coupling makes it hard to understand where all the components are, what they do, and how they fit together with the configuration options. As an example of this coupling, you can see that the `DbOptions` struct has some fields marked with the macro `#[serde(skip)]` to avoid serializing them. This is because these fields are not part of the settings that a user can change, they are components that are internally used by the engine. One could make the argument that all components should be part of the `DbOptions` struct, but this would make the API more complex and harder to understand.

## Goals

This RFC aims to:

* Separate the concerns of configuration options and components.
* Make it easier to introduce new components without breaking existing public APIs.

## Proposal

### Separate the concerns of configuration options and components

To make it easier to understand how SlateDB is configured, I propose to separate the concerns of configuration options, or settings, and components:

- Settings are options that a user can tweak to customize the engine for their use case
- Components are the parts of the database that are responsible for performing the work.

With that in mind, I propose to create a type alias for settings called `Settings` that will be used instead of the `DbOptions` struct. This struct will be cleaned from the components that are not settings. Currently, there are 3 components that are not settings:

- `block_cache`: The block cache to use.
- `clock`: The clock to use.
- `gc_runtime`: The runtime to use for garbage collection.

### Provide a builder API that allows users to customize the engine

Instead of having to pass all the components to the `Db::open` method, we will provide a builder API that allows users to customize the engine. This API will also include a method to provide the settings to the engine. The methods in this builder API will be named after the components they customize, for example `with_block_cache`, or `with_object_store`.

```rust
let object_store = Arc::new(InMemory::new());
let db = Db::builder("path/to/db", object_store)
    .with_settings(Settings {
       manifest_poll_interval: Duration::from_secs(1),
       ..Default::default()
    })
    .build();
```

This API makes forward changes easier to implement, as we can add new methods to the builder API without breaking backwards compatibility. For example, adding a new [WALL object store](https://github.com/slatedb/slatedb/pull/558) would not have to break the existing API since it would add a new method to the builder API, instead of changing the signature of the `open_with_opts` method.

## Backwards compatibility

We can use the [`#[deprecated]` attribute](https://doc.rust-lang.org/reference/attributes/diagnostics.html#the-deprecated-attribute) to signal to users that some parts of the current API are deprecated. This attribute allows you to signal that a function, struct, or field is deprecated and will be removed in a future release.

The `DbOptions` struct will be kept for backwards compatibility. It will be marked as deprecated and will be removed in a future release.

The fields that are not settings will be marked as deprecated and will be removed in a future release. While they are still in use, they can feed the builder API.

The `Db::open_with_opts` method will be marked as deprecated and will be removed in a future release. It will be replaced by the builder API. Until then, it will be implemented as a wrapper around the builder API.

The `Db::open` method can probably stay as is, since it's probably the most used method at the moment. Internally, it will use the builder API to initialize the database.
