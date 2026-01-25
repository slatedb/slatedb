# Compaction State Persistence

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Background](#background)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Constraints](#constraints)
- [References](#references)
- [Problem Statement](#problem-statement)
   * [Core Architecture Issues](#core-architecture-issues)
   * [Operational Limitations ](#operational-limitations)
   * [Impact](#impact)
- [Proposal](#proposal)
   * [Core Strategy: Iterator-Based Persistence](#core-strategy-iterator-based-persistence)
- [Workflow](#workflow)
   * [Current Compaction Workflow](#current-compaction-workflow)
   * [Proposed CompactionState Structure](#proposed-compactionstate-structure)
   * [Proposed `compactor.fbs`](#proposed-compactorfbs)
   * [Coordination between Manifest and CompactionState](#coordination-between-manifest-and-compactionstate)
   * [Persisting Internal Compactions](#persisting-internal-compactions)
   * [Persisting External Compactions](#persisting-external-compactions)
   * [Resuming Partial Compactions](#resuming-partial-compactions)
   * [Key Design Decisions](#key-design-decisions)
      + [Persistence Boundaries](#persistence-boundaries)
      + [Enhanced Job Model](#enhanced-job-model)
      + [State Management Pattern](#state-management-pattern)
      + [Recovery Strategy](#recovery-strategy)
   * [Persistent State Storage](#persistent-state-storage)
      + [Object Store Layout](#object-store-layout)
   * [Protocol for State Management of Manifest and CompactionState](#protocol-for-state-management-of-manifest-and-compactionstate)
      + [On startup...](#on-startup)
      + [On compaction job creation...](#on-compaction-job-creation)
      + [On compaction job progress...](#on-compaction-job-progress)
      + [On compaction job complete...](#on-compaction-job-complete)
   * [External Process Integration](#external-process-integration)
   * [Garbage Collection Integration](#garbage-collection-integration)
- [Observability Enhancements](#observability-enhancements)
   * [Progress Tracking](#progress-tracking)
   * [Statistics](#statistics)
- [Cost Analysis](#cost-analysis)
      + [Operation Count Breakdown](#operation-count-breakdown)
      + [Cloud Cost Analysis](#cloud-cost-analysis)
   * [Recovery Efficiency Analysis](#recovery-efficiency-analysis)
      + [Work Preservation Calculation](#work-preservation-calculation)
      + [Scaling Analysis](#scaling-analysis)
- [Future Extensions](#future-extensions)

<!-- TOC end -->

Status: Accepted

Authors:

* [Sujeet Sawala](https://github.com/sujeetsawala)

## Background

Compaction currently happens for the following:

- L0 SSTs
- Various level Sorted Runs (range partitioned SST across the complete keyspace)

This RFC proposes the goals & design for compaction state persistence along with ways to improve current compaction mechanism by adding retries and tracking.

## Goals

- Provide a mechanism to track progress of a `Compaction`
- Allow retrying compactions based on the state of the `Compaction`
- Improve observability around compactions
- Separate out compaction related details from `Manifest` into a separate `Compactions` struct
- Coordination between `Manifest` and `Compactions`
- Coordination mechanism between externally triggered compactions and the main compaction process.

## Non-Goals

- Distributed compaction: SlateDb is a single writer and currently a single-compactor based database. With distributed compaction, we plan to further parallelise SST compaction across different compaction processes. This topic is out of scope of the RFC.
- Resuming partial compaction under MVCC depends on the sorted-run layout: with multi-versioned keys, how do we partition the keyspace into non-overlapping SSTs within a single SR?

## Constraints

- Changes should be backward compatible and extend the existing compaction structs
- State updates should be cost efficient
- Manifest can be eventually consistent with the latest view after compaction

## References

- [Compaction RFC](https://github.com/slatedb/slatedb/blob/main/rfcs/0002-compaction.md)
- [Universal Compaction](https://github.com/facebook/rocksdb/wiki/universal-compaction)

## Problem Statement

This RFC extends discussions in the below github issue. It also addresses several other sub-issues.

[Issue #673](https://github.com/slatedb/slatedb/issues/673):

### Core Architecture Issues
1. **1:1 Compaction:Job Cardinality**: Cannot retry failed compactions - entire compaction fails if job fails
2. **No Progress Tracking**: `Compaction` state isn't persisted, making progress invisible
3. **No State Persistence**: All compaction state is lost on restart

### Operational Limitations 
5. **Manual Compaction Gaps**: No coordination mechanism for operator-triggered compactions ([Issue #288](https://github.com/slatedb/slatedb/issues/288))
6. **GC Coordination Issues**: Garbage collector needs better visibility into ongoing compactions ([Issue #604](https://github.com/slatedb/slatedb/issues/604))
7. **Limited Observability**: Limited visibility into compaction progress and failures

### Impact
- **Large compactions** (multi-GB) lose hours of work on failure
- **Engineering overhead** for debugging and manually restarting failed compactions
- **Customer impact** from extended recovery times during outages
- **Resource waste** from repeated processing of the same data

## Proposal

### Core Strategy: Iterator-Based Persistence

Rather than complex chunking mechanisms, we leverage SlateDB's existing iterator architecture which provides natural persistence boundaries at **SST completion points**. This approach:

- **Builds on existing infrastructure**: Enhances current `execute_compaction_job` method
- **Uses natural boundaries**: SST completions provide ~256MB recovery granularity
- **Minimizes overhead**: Persistence aligns with existing I/O patterns
- **Scales cost-effectively**: Higher persistence frequency for larger, more valuable compactions

## Workflow

### Current Compaction Workflow

1. `Compactor` initialises the `CompactionScheduler` and `CompactionEventHandler` during startup. It also initialises event loop that periodically polls manifest, periodically logs and provides progress and handles completed compactions [No change required]
2.  The `CompactionEventHandler` refreshes the compaction state by merging it with the `current manifest`.
3. `CompactionEventHandler` communicates this compaction state to the `CompactionScheduler`(scheduler makes a call `maybeScheduleCompaction` with local database state).
4. `CompactionScheduler` is implemented by `SizeTieredCompactionScheduler` to decide and group L0 SSTs and SRs to be compacted together. It returns a list of `Compaction` that are ready for execution.
5. `CompactorEventHandler` iterates over the list of compactions and calls `submitCompaction()` if the count of running compaction is below the threshold.
6. The submitted compaction is validated that it is not being executed (by checking in the local `CompactorState`) and if true, is added to the `CompactorState` struct.
7. Once the `CompactorEventHandler` receives an affirmation, it calls the `startCompaction()` to start the compaction.
8. The compaction is now transformed into a `Compaction` and a blocking task is spawned to execute the `Compaction` by the `CompactionExecutor`
9. The task loads all the iterators in a `MergeIterator` struct and runs compactions on it. It discards older expired versions and continues to write to a SST. Once the SST reaches it's threshold size, the SST is written to the active destination SR. Periodically the task also provides stats on task progress. 
10. When a task completes compaction execution, the task returns the `{destinationId, outputSSTs}` to the worker channel to act upon the compaction terminal state
11. The worker task executes the `finishCompaction()` upon successful `CompactionCompletion` and updates the manifests and trigger scheduling of next compactions by calling `maybeScheduleCompaction()`
12. In case of failure, the compaction_state is updated by calling `finishFailedCompaction()`
13. GC clears the orphaned states and SSTs during it's run.

### Proposed CompactionState Structure
The in-memory state contains the complete view of all compaction activity:

```rust
/// Container for compactions tracked by the compactor alongside its epoch.
pub(crate) struct Compactions {
    // The current compactor's epoch.
    pub(crate) compactor_epoch: u64,
    pub(crate) core: CompactionsCore,
}

/// Represents an immutable in-memory view of .compactions file that is suitable
/// to expose to end-users.
pub struct CompactionsCore {
    /// The set of recent compactions tracked by this compactor. These may
    /// be pending, in progress, or recently completed (either with success
    /// or failure).
    recent_compactions: BTreeMap<Ulid, Compaction>,
}

/// Canonical, internal record of a compaction.
///
/// A compaction is the unit tracked by the compactor: it has a stable `id` (ULID) and a `spec`
/// (what to compact and where).
pub struct Compaction {
    /// Stable id (ULID) used to track this compaction across messages and attempts.
    id: Ulid,
    /// What to compact (sources) and where to write (destination).
    spec: CompactionSpec,
    /// Total number of bytes processed so far for this compaction.
    bytes_processed: u64,
    /// Current status for this compaction.
    ///
    /// This is tracked only in memory at the moment.
    status: CompactionStatus,
    /// Output SSTs produced by this compaction, if any.
    output_ssts: Vec<SsTableHandle>,
}

/// Immutable spec that describes a compaction. Currently, this only holds the
/// input sources and destination SR id for a compaction.
pub struct CompactionSpec {
    /// Input sources for the compaction.
    sources: Vec<SourceId>,
    /// Destination sorted run id for the compaction.
    destination: u32,
}

/// Lifecycle status for a compaction.
///
/// State transitions:
/// ```text
/// Submitted <-> Running --> Completed
///     |             |
///     |             v
///     +----------> Failed
/// ```
///
/// `Completed` and `Failed` are terminal states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum CompactionStatus {
    /// The compaction has been submitted but not yet started.
    Submitted,
    /// The compaction is currently running.
    Running,
    /// The compaction finished successfully.
    Completed,
    /// The compaction failed. It might or might not have started before failure.
    Failed,
}
```

### Proposed `compactor.fbs`

Backwards compatible FlatBuffers schema for persisting compaction state:

```fbs
union CompactionSpec {
    TieredCompactionSpec,
}

enum CompactionStatus : byte {
    /// The compaction has been submitted but not yet started.
    Submitted = 0,
    /// The compaction is currently running.
    Running,
    /// The compaction finished successfully.
    Completed,
    /// The compaction failed. It might or might not have started before failure.
    Failed,
}

table TieredCompactionSpec {
    // input L0 SSTable IDs
    ssts: [Ulid];

    // input Sorted Run IDs
    sorted_runs: [uint32];
}

table Compaction {
    // ID of compaction
    id: Ulid (required);

    // Specificaiton defining the work to be done for this compaction
    spec: CompactionSpec (required);

    // Current lifecycle status for this compaction
    status: CompactionStatus;

    // Output SSTs already produced by this compaction.
    output_ssts: [CompactedSsTable];
}

table CompactionsV1 {
    // Fencing token to ensure single active compactor
    compactor_epoch: uint64;

    // In-flight compactions as of the last update
    recent_compactions: [Compaction] (required);
}
```

### Coordination between Manifest and CompactionState

A new `CompactorStateReader` and `CompactorStateWriter` are introduced to manage the dual persistence of
`.manifest` and `.compactions` files. These encapsulate the ordering and fencing rules between
`.manifest` and `.compactions` so the rest of the codebase can treat them as a single logical state.

- `CompactorStateReader`: loads `.compactions` before `.manifest` so GC/admin readers see a consistent
  view of in-flight compactions alongside a manifest that still references their inputs.
- `CompactorStateWriter`:
  - Fences by incrementing `compactor_epoch` in the `.manifest` first and then the `.compactions`.
  - Refreshes and merges remote state (including externally submitted compactions).
  - Writes `.manifest` first, then `.compactions` to ensure completed compactions output is visible before
    the compaction is marked finished.
  - On startup it rewrites `Running` to `Submitted` so they can be resumed.
  - Trims older finished entries for GC.

### Persisting Internal Compactions

1. On each poll tick, `CompactorEventHandler` calls `state_writer.refresh()`, which loads `.compactions` first and then `.manifest`, merging any remote submitted compactions into the local `CompactorState`.
2. The scheduler proposes `CompactionSpec` values via `CompactionScheduler::propose`. The handler only schedules up to available capacity (`max_concurrent_compactions - running`).
3. For each proposal, the handler allocates a compaction id and calls `CompactorState::add_compaction`. This enforces destination conflict/overwrite rules. Accepted compactions are stored with status `Submitted`.
4. If any new compactions are added, the handler persists them with `write_compactions_safely()`, which writes the next `.compactions` file and retries on version conflicts. The stored view retains all active compactions plus the most recently finished one for GC (see [#1044](https://github.com/slatedb/slatedb/issues/1044)).
5. The handler then processes all `Submitted` compactions (internal and external) in `maybe_start_compactions`. It validates each spec (non-empty sources, L0-only destination must be above the highest existing SR, plus scheduler validation). Invalid specs are marked `Failed`. Valid specs are started in the executor and marked `Running`. The status changes are persisted with `write_compactions_safely()`.
6. The executor emits `CompactionJobProgress` messages with `bytes_processed` and `output_ssts`. The handler updates the in-memory compaction and persists to `.compactions` only when `output_ssts` grows (new output SST), which is the recovery boundary.
7. When a compaction finishes, the handler applies `finish_compaction`, writes a checkpointed manifest, then writes the updated compactions via `write_state_safely()` (manifest first, compactions second). The compaction status becomes `Completed`, and older finished entries are trimmed.

### Persisting External Compactions

External requests are persisted into the same `.compactions` store and then handled by the regular compactor flow:

1. Client submits a `CompactionSpec` via `Admin::submit_compaction`, which calls `Compactor::submit` to write a new `Compaction` (status `Submitted`) into the `.compactions` store. This uses optimistic concurrency and retries on version conflicts. No scheduler validation happens at submit time.
2. On the next poll tick, `state_writer.refresh()` loads `.compactions` and merges newly submitted external compactions into the local state. `merge_remote_compactions` only accepts remote entries that are still in `Submitted` status.
3. The compactor then handles these submissions exactly like internal ones: `maybe_start_compactions` enforces capacity and validates each spec (non-empty sources, L0-only destination rule, scheduler validation). Invalid entries are marked `Failed`; valid ones are started and marked `Running`.
4. Progress and completion persistence is identical to internal compactions: new output SST boundaries are persisted via progress updates, and completion writes the manifest then compactions via `write_state_safely()`.

### Resuming Partial Compactions

1. The executor reports progress with `CompactionJobProgress` messages that include the full `output_ssts` list. The handler persists these updates to `.compactions` only when a new output SST is added.
2. On compactor startup, `CompactorStateWriter::new` flips any `Running` compactions back to `Submitted` and preserves their `output_ssts`, then writes this recovery state to `.compactions`.
3. When such a compaction is started, `StartCompactionJobArgs` carries the existing `output_ssts`. The executor derives a resume cursor by reading the last output SST and calling `last_written_key_and_seq` (last key + sequence from the final block).
4. Input iterators (L0 SSTs and sorted runs) are built as usual and wrapped in `ResumingIterator`, which seeks to the last written key and drains entries for that key while `seq >= last_seq` to avoid duplicates.
5. Compaction continues from that position and produces new output SSTs; each completed SST triggers another persisted progress update.

### Key Design Decisions

#### Persistence Boundaries
**Decision**: Persist state at the critical boundary:
**Output SST Completion**: Every ~256MB of written data (always persisted)
**Rationale**: Output SST completions provide the best recovery value per persistence operation. Each represents significant completed work that we don't want to lose.

#### Enhanced Job Model
**Decision**: Keep 1:1 relationship between a logical `Compaction` and it physical execution attempt.
**Rationale**: Simplifies tracking and state management. Resumes are handled by marking a `Compaction` as `Submitted` again. Retries are not handled. Once a `Compaction` is markfed `Failed`, it's not retried. The number of resumes are not tracked in the `.compactions` file. Users can see resume activity in logs or by viewing previous `.compactions` files to see transitions from `Running` back to `Submitterd`. This approach simplifies the data model and state machine.

#### State Management Pattern
**Decision**: Mirror the existing `ManifestStore` pattern with `CompactorStore`.

**Rationale**: Reuses proven patterns for atomic updates, version checking, and conflict resolution that are already battle-tested in SlateDB.

#### Recovery Strategy
- Resume from last completed output SST

The section below is under discussion here: https://github.com/slatedb/slatedb/pull/695/files#r2239561471

### Persistent State Storage

#### Object Store Layout
The compaction state is persisted to the object store following the same CAS pattern as manifests, ensuring consistency and reliability:

```
/00000000000000000001.compactions  # First compactor state
/00000000000000000002.compactions  # Updated state after compactions
/00000000000000000003.compactions  # Current state
```

_NOTE: The section below is discussed in detail [here](https://github.com/slatedb/slatedb/pull/695/files#r2239561471)._

### Protocol for State Management of Manifest and CompactionState

This section describes the `CompactorStateReader` and `CompactorStateWriter` protocol in detail. The protocol is based on the following principals:

- .manifest should be source of truth for reader and writer clients (and should thus contain SRs)
- .compactions file should be an implementation detail of the compactor, not something the DB needs to pay any attention to.

The `.compactions` file also serves as an interface over which clients can build their custom distributed compactions say based on etcd (Kubernetes), [chitchat](https://github.com/quickwit-oss/chitchat), `object_store`, etc.

#### On startup...

1. Compactor fetches the latest .manifest file (00005.manifest).
2. Manifest increments `compactor_epoch` and tries writing the  `compaction_epoch` to the .manifest file (00006.manifest)
    File version check (in-memory and remote object store): if 00006.manifest exists, 
      - If latest .manifest compactor_epoch > current compactor epoch, die (fenced)
      - If latest .manifest compactor_epoch == current compactor epoch, die (fenced)
      - If latest .manifest compactor_epoch < current compactor epoch, increment the .manifest file ID by 1 and retry. This process would continue until successful compactor write.
      (The current active compactor has updated the .manifest file)
3. Compactor fetches the latest .compactions file (00005.compactions). 
4. Try writing above `compactor_epoch` to the next sequential .compactions position.(00006.compactions).
    File version check (in-memory and remote object store): if 00006.compactions exists, 
      - If latest .compactions compactor_epoch > current compactor epoch, die (fenced)
      - If latest .compactions compactor_epoch == current compactor epoch, panic
      - If latest .compactions compactor_epoch < current compactor epoch, increment the .compactions file ID by 1 and retry. This process would continue until successful compactor write.
      (The current active compactor Job would have updated the .compactions file)
5. If compactor_epoch in in-memory manifest (00005.manifest) >= `compactor_epoch`, older compactors are fenced now.

At this point, the compactor has been successfully initialised. Any updates to write a new .compactions (00006.compactions) or .manifest file (00006.manifest) by stale compactors would fence them.

#### On compaction job creation...

(Manifest is polled periodically to get the list of L0 SSTs created. The scheduler would create a list of new compactions for these L0 SSTs as well)

1. Compactor fetches the latest .manifest file during the manifest poll. (00006.manifest)
2. Compactor writes to the next .compactions file with the list of compactions with empty `output_ssts` (00007.compactions in our example).
    If the file (00007.compactions) exists, 
      - If latest .compactions compactor_epoch > current compactor epoch, die (fenced)
      - If latest .compactions compactor_epoch == current compactor epoch, reconcile the compactor state and go to step (2)
      - If latest .compactions compactor_epoch < current compactor epoch, panic (`compactor_epoch` went backwards)

#### On compaction job progress...

1. Compactor writes to the next .compactions file the `Compactions` (persist when an SST is added to SR) with the latest progress (00008.compactions in our example).
    If the file (00008.compactions) exists, 
      - If latest .compactions compactor_epoch > current compactor epoch, die (fenced)
      - If latest .compactions compactor_epoch == current compactor epoch, reconcile the compactor state and go to step (2)
      - If latest .compactions compactor_epoch < current compactor epoch, panic
        (`compactor_epoch` went backwards)

#### On compaction job complete...

1. Update in-memory state:
  - Remove compacted L0 SSTs and source SRs from `Manifest`
  - Insert the output SR in `Manifest`
  - Update `l0_last_compacted`
  - Mark the compaction finished
  - Remove finished compactions, retaining the most recent finished compaction for GC (see #1044).
2. Write .manifest using protocol described above
3. Write .compactions using protocol described above

If a failure occurs between (2) and (3), on restart, the `Compaction` is still in a `Running` state. The compactor will attempt to resume it. The inputs will have already been removed from the `Manifest` in step (2), so the compaction should fail during validation and be marked as `Failed`.

_NOTE: There is one pathological case where the compaction has a single SR input, which is also its output (e.g. input=[SR1], output=SR1). In this case, the compaction will resume successfully and reprocess the remaining data in SR1. This is acceptable since an SR compaction of itself is a no-op._

### External Process Integration

**Administrative Commands**:

- `slatedb submit-compaction --request '<json>' [--scheduler size-tiered]` - Submit a compaction request (`"Full"` or `{"Spec":{...}}` JSON).
- `slatedb read-compaction --id <ulid> [--compactions-id <id>]` - Read a specific compaction by ULID.
- `slatedb read-compactions [--id <id>]` - Read the latest or a specific compactions file.
- `slatedb list-compactions [--start <id>] [--end <id>]` - List compactions files in an id range.
- `slatedb run-compactor` - Run the compactor in the foreground.

These commands map to `Admin::submit_compaction`, `Admin::read_compaction`, `Admin::read_compactions`,
`Admin::list_compactions`, and `Admin::run_compactor`.

### Garbage Collection Integration
The garbage collection of .compactions file can leverage the existing logic of garbage collecting .sst files and .manifest files. 

The .sst file is deemed to be garbage collected if it satisfies the following conditions:
  - SST is older than the min age configured.
  - SST is not referenced by any active manifest checkpoint. 

Note: We would also need to handle the scenario mentioned in https://github.com/slatedb/slatedb/issues/604 to avoid deletion of compacted SSTs and prevent data corruption.

The .manifest file is deemed to be garbage collected if it satisfies the following conditions:
  - Avoid deletion of the latest manifest
  - Delete manifest not referenced by active checkpoints
  - Delete manifests that have passed the min_age

The .compactions file can be cleaned by the garbage collector similar to .manifest file garbage collection conditions:
  - Avoid deletion of the latest .compactions file
  - Delete compactor files that have passed the min_age

## Observability Enhancements

### Progress Tracking
- **Real-time progress**: Bytes processed, SSTs completed, estimated completion time
- **Phase tracking**: Reading inputs → Writing outputs → Updating manifest → Completed
- **Recovery metrics**: Work preservation percentage, recovery time

### Statistics
- **Performance**: Throughput, duration, success rates by compaction size
- **Recovery**: Jobs recovered, average recovery time, work preservation
- **Errors**: Categorized by type (network, memory, corruption) for retry decisions
- **Cost**: Persistence operations, overhead percentage

## Cost Analysis

#### Operation Count Breakdown

For a **typical 40GB compaction** (160 input SSTs → ~160 output SSTs):

**Baseline compaction operations:**
- **160 SST reads**: Reading input SST files
- **160 SST writes**: Writing output SST files
- **2 manifest updates**: Initial job start + final completion
- **Total baseline**: 322 operations

**With persistence enabled:**
- **160 SST reads**: Input SST files (unchanged)
- **160 SST writes**: Output SST files (unchanged)
- **160 state writes**: Persistence after each output SST (~256MB intervals)
- **2 manifest updates**: Job lifecycle management
- **Total with persistence**: 482 operations

**Overhead calculation:**
- **Additional operations**: 160 (482 - 322)
- **Percentage increase**: +50% operations
- **Operations per GB**: ~4.0 additional ops/GB (160 ÷ 40GB)

#### Cloud Cost Analysis

Using **AWS S3 Standard** pricing:
- **PUT operations**: $0.0005 per 1,000 requests
- **GET operations**: $0.0004 per 1,000 requests
- **DELETE operations**: $0.0005 per 1,000 requests

**Baseline costs:**
- **160 PUTs** (SST writes): $0.000080
- **160 GETs** (SST reads): $0.000064
- **2 PUTs** (manifest): $0.000001
- **Total baseline**: $0.000145

**Additional persistence costs:**
- **160 PUTs** (state writes): $0.000080
- **Additional total**: $0.000080

**Cost impact:**
- **Additional cost**: $0.000080 (~$0.00008)
- **Percentage increase**: +50% operations, but negligible absolute cost
- **Cost per GB**: ~$0.000002 per GB compacted

### Recovery Efficiency Analysis

#### Work Preservation Calculation

**Without persistence:**
- **Recovery strategy**: Restart compaction from beginning
- **Work preserved**: 0% (all progress lost)
- **Additional operations**: Full re-execution (322 operations repeated)
- **Time impact**: 100% of original compaction time

**With persistence:**
- **Average failure point**: 50% through compaction (statistical)
- **Work preserved**: ~50% of progress maintained
- **Recovery operations**: Resume from last checkpoint
- **Time impact**: ~50% of original compaction time saved

**Detailed recovery scenarios:**

| Failure Point | Without Persistence | With Persistence | Work Preserved | Operations Saved(work preserved percentage * 322) |
|---------------|-------------------|------------------|----------------|------------------|
| **10% complete** | Restart (0% preserved) | Resume from 10% (10% preserved) | 10% | 32 operations |
| **25% complete** | Restart (0% preserved) | Resume from 25% (25% preserved) | 25% | 81 operations |
| **50% complete** | Restart (0% preserved) | Resume from 50% (50% preserved) | 50% | 161 operations |
| **75% complete** | Restart (0% preserved) | Resume from 75% (75% preserved) | 75% | 242 operations |
| **90% complete** | Restart (0% preserved) | Resume from 90% (90% preserved) | 90% | 290 operations |

**Average work preservation**: **50%** across all failure scenarios

#### Scaling Analysis

| Compaction Size | SSTs | Base Ops | +Persistence | Additional Cost | % Overhead |
|-----------------|------|----------|--------------|----------------|------------|
| **10GB** (40 SSTs) | 40 | 82 | 122 | $0.000020 | +49% |
| **40GB** (160 SSTs) | 160 | 322 | 482 | $0.000080 | +50% |
| **100GB** (400 SSTs) | 400 | 802 | 1,202 | $0.000200 | +50% |
| **1TB** (4,000 SSTs) | 4,000 | 8,002 | 12,002 | $0.002000 | +50% |

**Key observations:**
- **Operations increase by ~50%**, but absolute costs remain minimal
- **Cost scales linearly** with compaction size (~$0.000002/GB)
- **Percentage overhead is consistent** at ~50% across all sizes
- **Total costs are negligible** compared to storage and compute costs

## Future Extensions

- Persistent state provides foundation for multi-compactor coordination and work distribution.
- Define a minimum time boundary between compaction file updates to prevent excessive writes to the file (see https://github.com/slatedb/slatedb/pull/695#discussion_r2229977189)
- Add last_key to SST metadata to enable efficient range-based SST filtering during compaction source selection and range query execution.
