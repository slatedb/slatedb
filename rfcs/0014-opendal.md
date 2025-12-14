# Use OpenDAL as IO access layer

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

There were issues that discussed about introducing some composibility for `object_store` crate:

- https://github.com/apache/arrow-rs-object-store/issues/14
- https://github.com/apache/arrow-rs-object-store/issues/274

Unfortunately, these issues still have little progress.

We need a solution that reduces the complexity of maintaining our own wrapper layers while leveraging extension capabilities from the ecosystem.

**OpenDAL** offers an alternative approach: it's an Apache Incubator project that provides a **composable layer system** where features like caching, retrying, and observability are production-ready layers that can be combined without custom wrapper code. This addresses our exact pain points while being actively maintained by a large community.

## Goals and Constraints

### Goals

The primary goals of migrating to OpenDAL are:

1. **Reduce maintenance burden**: Eliminate the need to maintain custom wrapper layers (`CachedObjectStore`, `RetryingObjectStore`) by leveraging battle-tested ecosystem solutions
2. **Improve extensibility**: Gain access to OpenDAL's rich layer ecosystem for future capabilities (observability, rate limiting, etc.) without additional development effort
3. **Future-proof storage integration**: Benefit from OpenDAL's active development and broader backend support as SlateDB evolves

### Constraints

Given that `object_store` is a core dependency deeply integrated throughout SlateDB, we must ensure:

1. **No breaking changes**: The migration must be transparent to SlateDB users. Since we currently expose the `object_store::ObjectStore` trait as our public I/O API, we need to maintain API compatibility—either by keeping the same trait interface with a compatibility layer that preserves existing behavior.
2. **Performance parity**: I/O performance must not regress. Critical paths (read, write, list operations) should be benchmarked to ensure OpenDAL meets or exceeds current performance metrics.
3. **Feature completeness**: All existing functionality (caching, retrying, error handling) must be preserved or improved during migration.
4. **Validation before rollout**: A proof-of-concept (PoC) must be implemented and validated against our test suite before committing to the full migration. The transition should be incremental and allow rollback if issues arise.

## Scope

This RFC aims to accomplish the following:

1. **Assess migration effort**: Document our current usage of `object_store` throughout the codebase and evaluate the feasibility of migrating to OpenDAL equivalents
2. **Design migration path**: Develop a concrete, step-by-step migration strategy that satisfies the constraints outlined above
3. **Validate OpenDAL maturity**: Verify that OpenDAL's features and layers can pass our existing test suite and meet our functional requirements

## Proposal

### Assessment of Migration Effort

tbd