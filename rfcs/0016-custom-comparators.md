# Custom Comparators

<!-- Replace "RFC Title" with your RFC's short, descriptive title. -->

Table of Contents:

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Summary & Motivation](#summary-motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Design](#design)
   * [API](#api)
   * [Manifest](#manifest)
   * [Implementation](#implementation)
- [Impact Analysis](#impact-analysis)
   * [Core API & Query Semantics](#core-api-query-semantics)
   * [Consistency, Isolation, and Multi-Versioning](#consistency-isolation-and-multi-versioning)
   * [Time, Retention, and Derived State](#time-retention-and-derived-state)
   * [Metadata, Coordination, and Lifecycles](#metadata-coordination-and-lifecycles)
   * [Compaction](#compaction)
   * [Storage Engine Internals](#storage-engine-internals)
   * [Ecosystem & Operations](#ecosystem-operations)
- [Operations](#operations)
   * [Performance & Cost](#performance-cost)
   * [Observability](#observability)
   * [Compatibility](#compatibility)
- [Testing](#testing)
- [Rollout](#rollout)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
- [References](#references)
- [Updates](#updates)

<!-- TOC end -->

Status: Draft

Authors:

* [Almog Gavra](https://github.com/agavra)

<!-- TOC --><a name="summary-motivation"></a>
## Summary & Motivation

This RFC proposes a new API for customizing the ordering of keys in SlateDB. The
ordering of keys in Slate is a critical component of the performance characteristics
of the database and its ability to efficiently serve range queries.

The current implementation of SlateDB uses a lexicographic ordering of keys. This
is a good default for many use cases, but it may not always be the best choice. Some
examples of use cases where a different ordering is desirable include:

- Correct semantics for non-string keys (e.g. sorting floating point numbers)
- Sorting composite keys (e.g. a composite key of `[user_id, timestamp]` may sort ascending
  by `user_id` and descending by `timestamp`)
- Domain-specific ordering (e.g. Z-ordering for geographic data)

<!-- TOC --><a name="goals"></a>
## Goals

- Provide a new API for customizing the ordering of keys in SlateDB
- Maintain backwards compatibility by defaulting to the existing lexicographic ordering
- Best-effort enforcing of the ordering invariant (e.g. a comparator doesn't 
  change after first deployment)

<!-- TOC --><a name="non-goals"></a>
## Non-Goals

- Providing default implementations for non-lexicographic use cases
- Supporting changes to the ordering invariant after first deployment

<!-- TOC --><a name="design"></a>
## Design

<!-- A detailed description of the proposed change. Include diagrams, examples, schemas, and pseudo-code as appropriate. -->

<!-- TOC --><a name="api"></a>
### API

We will introduce a new interface for customizing the ordering of keys in SlateDB, the
`KeyComparator` trait:

```rust
use std::cmp::Ordering;
use std::sync::Arc;

/// Custom key comparison trait.
/// WARNING: Changing comparators on existing DB causes corruption.
pub trait KeyComparator: Send + Sync + std::fmt::Debug {
    /// Compare two keys.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// Unique identifier stored in manifest for validation.
    fn name(&self) -> &str;
}

pub type KeyComparatorType = Arc<dyn KeyComparator>;
```

This will be passed in when constructing a new `Db` instance with the `DbBuilder`:

```rust
let db = DbBuilder::new(path)
    // ...
    .with_key_comparator(key_comparator)
    .build();
```

<!-- TOC --><a name="manifest"></a>
### Manifest

We will introduce a new field to the manifest format to store the name of the comparator:

```proto
table ManifestV1 {
    // ...
    key_comparator_name: string;
}
```

This field will be used to validate that the comparator is not changed after first deployment.
if it is set in the manifest, a comparator with the same name must be used to create the database 
in runtime (no comparator will also cause a panic). If it is not set and the manifest already exists,
the database will panic if a custom comparator is supplied at runtime.

<!-- TOC --><a name="implementation"></a>
### Implementation

We'll need to modify each of these places to use the new `KeyComparator` passed in
from the `DbBuilder`. Note that because it's a trait, we'll need to use dynamic dispatch
to call the comparator.

> [!NOTE]
> Since comparisons happen on a hot path, we may want to avoid using the `dyn KeyComparator`
> if not necessary to avoid the overhead of dynamic dispatch. Instead, we can choose to 
> use an `Option<KeyComparatorType>` to store the comparator and inline the default 
> lexicographic comparator if not set.
>
> The alternative would be to always use the `dyn KeyComparator` and just implement a default
> implementation of it. This would be simpler, but it would add a small overhead to every
> comparison.

The following table was generated via an agent, but it seems to be accurate as far as I 
can tell:

| Location | Usage |
| --- | --- |
| `slatedb/src/bytes_range.rs:8`<br>`slatedb/src/comparable_range.rs:8` | `BytesRange` and its `ComparableRange` backing types rely on the default `Ord` implementation for `Bytes` when comparing start/end bounds (via `cmp_bound`). Their logic powers `range.contains`, `intersect`, and conversions that every range-aware component (memtables, SST views, sorted runs, iterator seek validation) uses. |
| `slatedb/src/mem_table.rs:36,189`<br>`slatedb/src/batch.rs:153,409` | `SequencedKey::cmp` keeps the memtable `SkipMap` and write-batch `BTreeMap` ordered by ascending user key and descending sequence number. `MemTableIterator::seek` and `WriteBatchIterator::seek` compare `RowEntry.key` / `key.user_key` to `next_key`, and `WriteBatch::remove_ops_by_key` issues `start..=end` range deletes that depend on this ordering. |
| `slatedb/src/merge_iterator.rs:21-119` | `MergeIterator` keeps a binary heap ordered by `RowEntry.key.cmp` (and reverse seq on ties) and short-circuits child seeks when `next_kv.key >= next_key`. This governs how memtable/WAL/SST iterators are merged for reads and compactions. |
| `slatedb/src/block_iterator.rs:73-135`<br>`slatedb/src/sst_iter.rs:93-552`<br>`slatedb/src/partitioned_keyspace.rs:13-80` | Block iterators skip entries with `kv.key < next_key`. Above the block layer, `SstView::contains`/`key_exceeds`, `InternalSstIterator::blocks_covering_view`, and `SstIterator::seek` compare requested keys to SST range bounds and block first-keys (via the partitioned keyspace helpers) to jump to the first block whose contents are `>= next_key`. |
| `slatedb/src/db_state.rs:137-303`<br>`slatedb/src/sorted_run_iterator.rs:176-188` | `SsTableHandle::intersects_range`, `SortedRun::find_sst_with_range_covering_key`, and `table_idx_covering_range` compare SST start keys and requested ranges to decide which files overlap a read/compaction. `SortedRunIterator::seek` then drops whole SSTs whose `first_key < next_key` before seeking inside the current SST iterator. |
| `slatedb/src/db_iter.rs:56-69`<br>`slatedb/src/db_iter.rs:341-357` | `DbIteratorRangeTracker` maintains the min/max key observed via `<`/`>` comparisons for SSI conflict detection, and `DbIterator::seek` enforces that callers only seek forward and stay inside the configured `BytesRange` by comparing `next_key` to `last_key` and the range bounds. |
| `slatedb/src/reader.rs:512-528` | The SST-building helper sorts buffered `RowEntry`s with `a.key.cmp(&b.key)` before emitting a table, modeling the requirement that compaction/flush writers provide keys in comparator order. |

For use of the comparator in the memtable, we will need to modify the `SequencedKey` type to include
a reference to the comparator and override the `Ord` implementation to use it. All other locations can
use the comparator reference directly.

<!-- TOC --><a name="impact-analysis"></a>
## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

<!-- TOC --><a name="core-api-query-semantics"></a>
### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [x] Range queries, iterators, seek semantics
- [x] Range deletions
- [ ] Error model, API errors

Range query semantics will be affected by this change since the comparator is used
to determine the order of keys in the range.

<!-- TOC --><a name="consistency-isolation-and-multi-versioning"></a>
### Consistency, Isolation, and Multi-Versioning

- [x] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

`DbIteratorRangeTracker` maintains the min/max key observed via </> comparisons
for SSI conflict detection. Since the comparator is used to determine the order of keys,
the min/max key observed will be in the order of the comparator (this should be
consistent with the query semantics so the change is mostly transparent to the user).

<!-- TOC --><a name="time-retention-and-derived-state"></a>
### Time, Retention, and Derived State

- [ ] Logical clocks
- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

N/A - the change should not affect time, retention, and derived state.

<!-- TOC --><a name="metadata-coordination-and-lifecycles"></a>
### Metadata, Coordination, and Lifecycles

- [x] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

We will add a new field to the manifest format to store the name of the comparator 
to serve as a basic validation mechanism that ensures the comparator is not changed
after first deployment (this is rudimentary and does not prevent changes if the
comparator uses the same name).

<!-- TOC --><a name="compaction"></a>
### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

Compactions will need to use the comparator to sort the keys in the SSTs.

<!-- TOC --><a name="storage-engine-internals"></a>
### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [x] Indexing (bloom filters, metadata)
- [x] SST format or block format

The indexes and SSts will be sorted by the comparator

<!-- TOC --><a name="ecosystem-operations"></a>
### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

<!-- TOC --><a name="operations"></a>
## Operations

<!-- TOC --><a name="performance-cost"></a>
### Performance & Cost

<!-- Describe performance and cost implications of this change. -->

There will potentially be a performance impact in situations that perform many comparisons,
such as compactions due to the cost of dynamic dispatch. As part of implementation we will
benchmark the performance impact and decide whether or not to use the `Option<KeyComparatorType>`
approach outlined in the implementation section above.

<!-- TOC --><a name="observability"></a>
### Observability

<!-- Describe any operational changes required to support this change. -->

- Configuration changes
- New components/services
- Metrics
- Logging

This will require a new configuration option to set the key comparator.

<!-- TOC --><a name="compatibility"></a>
### Compatibility

<!-- Describe compatibility considerations with existing versions of SlateDB. -->

- Existing data on object storage / on-disk formats
- Existing public APIs (including bindings)
- Rolling upgrades / mixed-version behavior (if applicable)

Existing deployments will set an empty string for the key comparator name in the manifest
the first time it is deployed with a version including this RFC. If the field does not
exist but the manifest already does, a custom comparator will not be allowed.

<!-- TOC --><a name="testing"></a>
## Testing

<!-- Describe the testing plan for this change. -->

- Unit tests:
- Integration tests:
- Fault-injection/chaos tests:
- Deterministic simulation tests:
- Formal methods verification:
- Performance tests:

We should have comprehensive tests to ensure we don't break the ordering invariant
going forward. All main code paths should be covered, in particular queriyng and
compactions.

<!-- TOC --><a name="rollout"></a>
## Rollout

<!-- Describe the plan for rolling out this change to production. -->

- Milestones / phases:
- Feature flags / opt-in:
- Docs updates:

<!-- TOC --><a name="alternatives"></a>
## Alternatives

N/A - the only alternative is to not use a custom comparator.

<!-- TOC --><a name="open-questions"></a>
## Open Questions

The biggest option question is whether to use the `Option<KeyComparatorType>` approach or the `dyn KeyComparator` approach (see implementation section above).

<!-- TOC --><a name="references"></a>
## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- github.com/slatedb/slatedb/issues/663
- github.com/crossbeam-rs/crossbeam/issues/1180 

<!-- TOC --><a name="updates"></a>
## Updates

