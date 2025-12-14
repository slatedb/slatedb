# Use OpenDAL as IO access layer

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Background](#background)
- [Goals and Constraints](#goals-and-constraints)
   * [Goals](#goals)
   * [Constraints](#constraints)
- [Scope](#scope)
- [Proposal](#proposal)
   * [Migration Strategy](#migration-strategy)
   * [Analysis of Direct Usages](#analysis-of-direct-usages)
      + [TableStore](#tablestore)
      + [ObjectStoreSequencedStorageProtocol](#objectstoresequencedstorageprotocol)
   * [RetryingObjectStore](#retryingobjectstore)
   * [CachedObjectStore](#cachedobjectstore)

<!-- TOC end -->

Status: Work in Progress

Authors:

- [Li Yazhou](https://github.com/flaneur2020)

<!-- TOC --><a name="background"></a>
## Background

Currently, SlateDB uses the `object_store` crate as its IO access layer. The `object_store` crate provides a minimal trait abstraction for different storage backends, which has served us well for basic storage operations.

However, as development has progressed, we've encountered limitations with this minimalistic approach. Several advanced data access patterns have emerged as requirements:

1. **Caching**: We implemented `CachedObjectStore` as a wrapper around `object_store::ObjectStore` to provide transparent chunked caching. This required us to maintain our own caching logic and handle cache invalidation strategies.

2. **Retrying**: We implemented `RetryingObjectStore` as another wrapper layer to automatically retry operations on transient failures. This added complexity in determining which errors are retryable and managing backoff strategies.

These wrapper implementations have several drawbacks:

- Each feature requires implementing the full `ObjectStore` trait, leading to significant boilerplate code
- We're duplicating efforts that are already solved problems in the broader ecosystem
- Maintaining and testing these wrappers adds ongoing engineering overhead
- The composition of multiple wrapper layers can be error-prone and harder to reason about

There were issues that discussed about introducing some composability for `object_store` crate:

- https://github.com/apache/arrow-rs-object-store/issues/14
- https://github.com/apache/arrow-rs-object-store/issues/274

Unfortunately, these issues still have little progress.

We hope to have a solution that reduces the complexity of maintaining our own wrapper layers while leveraging extension capabilities from an actively developed ecosystem.

**OpenDAL** offers a promising solution: it's an Apache Incubator project that provides a **composable layer system** where features like caching, retrying, and observability are production-ready layers that can be combined without custom wrapper code. This addresses our exact pain points while being actively maintained by a nice community.

<!-- TOC --><a name="goals-and-constraints"></a>
## Goals and Constraints

<!-- TOC --><a name="goals"></a>
### Goals

The primary goals of migrating to OpenDAL are:

1. **Reduce maintenance burden**: Eliminate the need to maintain custom wrapper layers (`CachedObjectStore`, `RetryingObjectStore`) by leveraging battle-tested ecosystem solutions
2. **Improve extensibility**: Gain access to OpenDAL's rich layer ecosystem for future capabilities (observability, rate limiting, etc.) without additional development effort
3. **Future-proof storage integration**: Benefit from OpenDAL's active development and broader backend support as SlateDB evolves

<!-- TOC --><a name="constraints"></a>
### Constraints

Given that `object_store` is a core dependency deeply integrated throughout SlateDB, we must ensure:

1. **No breaking changes**: The migration must be transparent to SlateDB users. Since we currently expose the `object_store::ObjectStore` trait as our public I/O API, we need to maintain API compatibility—either by keeping the same trait interface with a compatibility layer that preserves existing behavior.
2. **Performance parity**: I/O performance must not regress. Critical paths (read, write, list operations) should be benchmarked to ensure OpenDAL meets or exceeds current performance metrics.
3. **Feature completeness**: All existing functionality (caching, retrying, error handling) must be preserved or improved during migration.
4. **Validation before rollout**: A PoC must be implemented and validated against our test suite before committing to the full migration. The transition should be incremental and allow rollback if issues arise.

<!-- TOC --><a name="scope"></a>
## Scope

This RFC aims to accomplish the following:

1. **Assess migration effort**: Document our current usage of `object_store` throughout the codebase and evaluate the feasibility of migrating to OpenDAL equivalents
2. **Design migration path**: Develop a concrete, step-by-step migration strategy that satisfies the constraints outlined above
3. **Validate OpenDAL compatibility**: Verify that OpenDAL's features and layers can pass our existing test suite and meet our functional requirements

<!-- TOC --><a name="proposal"></a>
## Proposal

<!-- TOC --><a name="migration-strategy"></a>
### Migration Strategy

OpenDAL provides a **compatibility layer** that accepts `object_store::ObjectStore` implementations as backends. This allows us to maintain API compatibility with existing users while internally leveraging OpenDAL's layer system.

The migration approach is straightforward: we keep the public API unchanged while internally wrapping user-provided object stores with OpenDAL's compatibility layer. This wrapper enables us to compose OpenDAL's layers (retry, metrics, caching) on top of any `object_store` implementation:

```rust
let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
let operator = Operator::new(ObjectStoreBuilder::new(store).build()?)
    .layer(RetryLayer::new())
    .layer(MetricsLayer::new())
    .layer(cache_layer)
    .finish()?;
```

This compatibility layer enables a low-risk, incremental migration strategy. In the initial phase, complex wrappers like `CachedObjectStore` can remain unchanged and be passed directly to OpenDAL's compatibility layer. This ensures zero behavioral changes for systems that heavily depend on current caching semantics, such as ZeroFS.

Over time, we can evaluate OpenDAL's built-in layers for feature parity, replace custom wrappers with OpenDAL equivalents where beneficial, and contribute missing functionality upstream to OpenDAL if needed.

This approach decouples the migration timeline from the need to immediately replace all custom logic, reducing risk and allowing thorough validation at each step.

<!-- TOC --><a name="analysis-of-direct-usages"></a>
### Analysis of Direct Usages

SlateDB has significant `object_store` integration across multiple components. This section analyzes the **direct call sites** of `object_store::ObjectStore` APIs in core business logic to understand the migration scope.

We skip the wrapper implementations (`CachedObjectStore`, `RetryingObjectStore`) here since they will be replaced by OpenDAL's layer system. Instead, we focus on where core components directly invoke `object_store` APIs.

Fortunately, direct usage of `object_store` APIs is highly concentrated in just two components: `TableStore` and `ObjectStoreSequencedStorageProtocol`. Most other components use object stores indirectly through these abstractions, which significantly reduces the migration surface area.

<!-- TOC --><a name="tablestore"></a>
#### TableStore

The `TableStore` component (`tablestore.rs`) has four direct call sites to `object_store` APIs: 

1. `head()` for getting object metadata in `ReadOnlyObject::len()`
2. `get_range()` for reading byte ranges in `ReadOnlyObject::read_range()`
3. `get()` for reading full files in `ReadOnlyObject::read()`
4. `delete()` for SST file cleanup in `delete_sst()`

The `ReadOnlyObject` struct, which implements the `ReadOnlyBlob` trait, serves as the primary interface for reading SSTable blocks, indexes, and filters. Additionally, the component uses `object_store::buffered::BufWriter` for efficient SST writes in `write_sst_in_object_store()`. List operations for WAL and SST discovery are delegated to the underlying object store via the `ObjectStores` wrapper.

Migrating these four call sites to OpenDAL equivalents is straightforward, though we need to ensure OpenDAL provides equivalent buffered write capabilities and that stream-based list operations work seamlessly.

<!-- TOC --><a name="objectstoresequencedstorageprotocol"></a>
#### ObjectStoreSequencedStorageProtocol

The `ObjectStoreSequencedStorageProtocol` component (`transactional_object/object_store.rs`) has three direct call sites:

1. `put_opts()` with `PutMode::Create` for writing new versioned manifests with CAS semantics
2. `get()` for reading specific manifest versions
3. `list()` for listing manifest versions in a range

This component provides the versioned storage protocol for `ManifestStore` and is particularly critical because it **uses `PutMode::Create` for optimistic concurrency control**. This create-if-not-exists semantics prevents concurrent manifest corruption, making it essential for fencing in distributed scenarios.

The migration requires that OpenDAL **supports CAS (create-if-not-exists) semantics equivalent to `PutMode::Create`**—this is a P0 validation requirement. Additionally, the list operation must support range queries over versioned objects.

<!-- TOC --><a name="retryingobjectstore"></a>
### RetryingObjectStore

tbd

<!-- TOC --><a name="cachedobjectstore"></a>
### CachedObjectStore

tbd