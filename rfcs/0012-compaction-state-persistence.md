# Compaction State Persistence

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Compaction State Persistence](#compaction-state-persistence)
  - [Background](#background)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Constraints](#constraints)
  - [References](#references)
  - [ProblemStatement](#problemstatement)
    - [Core Architecture Issues](#core-architecture-issues)
    - [Operational Limitations](#operational-limitations)
    - [Impact](#impact)
  - [Proposal](#proposal)
    - [Core Strategy: Iterator-Based Persistence](#core-strategy-iterator-based-persistence)
  - [Workflow](#workflow)
    - [Current Compaction Workflow](#current-compaction-workflow)
    - [Proposed CompactionState Structure](#proposed-compactionstate-structure)
    - [Persisting Internal Compactions](#persisting-internal-compactions)
    - [Persisting External Compactions](#persisting-external-compactions)
    - [Resuming Partial Compactions](#resuming-partial-compactions)
  - [Key Design Decisions](#key-design-decisions)
    - [1. Persistence Boundaries](#1-persistence-boundaries)
    - [2. Enhanced Job Model](#2-enhanced-job-model)
    - [3. State Management Pattern](#3-state-management-pattern)
    - [4. Recovery Strategy](#4-recovery-strategy)
    - [5. Migrate `compaction_epoch` from Manifest to CompactionState](#5-migrate-compaction_epoch-from-manifest-to-compactionstate)
  - [Persistent State Storage](#persistent-state-storage)
    - [Object Store Layout](#object-store-layout)
  - [Protocol for State Management of Manifest and CompactionState](#protocol-for-state-management-of-manifest-and-compactionstate)
    - [On startup...](#on-startup)
    - [On compaction initiation...](#on-compaction-initiation)
    - [On compaction job progress...](#on-compaction-job-progress)
    - [On compaction job complete...](#on-compaction-job-complete)
  - [Summarised Protocol](#summarised-protocol)
  - [Race conditions handled in the protocol](#race-conditions-handled-in-the-protocol)
    - [Incorrect Read order of manifest and compactionState](#incorrect-read-order-of-manifest-and-compactionstate)
    - [Fenced Compactor Process trying to update manifest](#fenced-compactor-process-trying-to-update-manifest)
    - [Fenced Compactor Process trying to update manifest](#fenced-compactor-process-trying-to-update-manifest-1)
  - [Gaps in compactor_epoch in .manifest file](#gaps-in-compactor_epoch-in-manifest-file)
  - [External Process Integration](#external-process-integration)
  - [Garbage Collection Integration](#garbage-collection-integration)
  - [Observability Enhancements](#observability-enhancements)
    - [Progress Tracking](#progress-tracking)
    - [Statistics](#statistics)
  - [Cost Analysis](#cost-analysis)
    - [Operation Count Breakdown](#operation-count-breakdown)
    - [Cloud Cost Analysis](#cloud-cost-analysis)
    - [Recovery Efficiency Analysis](#recovery-efficiency-analysis)
    - [Scaling Analysis](#scaling-analysis)
  - [Future Extensions](#future-extensions)

<!-- TOC end -->

Status: In Review

Authors:

* [Sujeet Sawala](https://github.com/sujeetsawala)

## Background

Compaction currently happens for the following:
- L0 SSTs
- Various level Sorted Runs(range partitioned SST across the complete keyspace)

This RFC proposes the goals & design for compaction state persistence along with ways to improve current compaction mechanism by adding retries and tracking.

## Goals

- Provide a mechanism to track progress of a `CompactionJob`
- Allow retrying compactions based on the state of the `CompactionJob`
- Improve observability around compactions
- Separate out compaction related details from `Manifest` into a separate `CompactionState`
- Coordination between `Manifest` and `CompactionState`
- Coordination mechanism between externally triggered compactions and the main compaction process.
- Refactor Manifest store so that it can be used to store both ,manifest and .compactor files.

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

### **Core Architecture Issues**
1. **1:1 Compaction:Job Cardinality**: Cannot retry failed compactions - entire compaction fails if job fails
2. **No Progress Tracking**: `CompactionJob` state isn't persisted, making progress invisible
3. **No State Persistence**: All compaction state is lost on restart

### **Operational Limitations** 
5. **Manual Compaction Gaps**: No coordination mechanism for operator-triggered compactions ([Issue #288](https://github.com/slatedb/slatedb/issues/288))
6. **GC Coordination Issues**: Garbage collector needs better visibility into ongoing compactions ([Issue #604](https://github.com/slatedb/slatedb/issues/604))
7. **Limited Observability**: Limited visibility into compaction progress and failures

### **Impact**
- **Large compactions** (multi-GB) lose hours of work on failure
- **Engineering overhead** for debugging and manually restarting failed compactions  
- **Customer impact** from extended recovery times during outages
- **Resource waste** from repeated processing of the same data

## Proposal

### **Core Strategy: Iterator-Based Persistence**

Rather than complex chunking mechanisms, we leverage SlateDB's existing iterator architecture which provides natural persistence boundaries at **SST completion points**. This approach:

- **Builds on existing infrastructure**: Enhances current `execute_compaction` method
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

8. The compaction is now transformed into a `compactionJob` and a blocking task is spawned to execute the `compactionJob` by the `CompactionExecutor`

9. The task loads all the iterators in a `MergeIterator` struct and runs compactions on it. It discards older expired versions and continues to write to a SST. Once the SST reaches it's threshold size, the SST is written to the active destination SR. Periodically the task also provides stats on task progress. 

10. When a task completes compaction execution, the task returns the {destinationId, outputSSTs} to the worker channel to act upon the compaction terminal state

11. The worker task executes the `finishCompaction()` upon successful `CompactionCompletion` and updates the manifests and trigger scheduling of next compactions by calling `maybeScheduleCompaction()`

12. In case of failure, the compaction_state is updated by calling `finishFailedCompaction()`

13. GC clears the orphaned states and SSTs during it's run.

### **Proposed CompactionState Structure**
The persistent state contains the complete view of all compaction activity:

```rust
pub(crate) struct CompactionJob {
    pub(crate) id: Ulid,
    pub(crate) destination: u32,
    pub(crate) ssts: Vec<SsTableHandle>,
    pub(crate) sorted_runs: Vec<SortedRun>,
    pub(crate) compaction_ts: i64,
    pub(crate) is_dest_last_run: bool,
    pub(crate) completed_input_sst_ids: Vec<Ulid>;
    pub(crate) completed_input_sr_ids: Vec<u32>;
    pub(crate) output_sr: SortedRun;
}

pub(crate) enum CompactionType {
    Internal,
    External,
}

pub struct Compaction {
    pub(crate) status: CompactionStatus,
    pub(crate) sources: Vec<SourceId>,
    pub(crate) destination: u32,
    pub(crate) compaction_id: Ulid,
    pub(crate) compaction_type: CompactionType,
    pub(crate) job_attempts: Vec<CompactionJob>;
}

pub(crate) CompactorState {
    manifest: DirtyManifest
    compaction_state: DirtyCompactionState
}

pub(crate) struct CompactionState {
    compactor_epoch: u64,
    // active_compactions queued, in-progress and completed
    compactions: HashMap<Ulid, Compaction>,
}

pub(crate) struct DirtyCompactionState {
    id: u64,
    compactor_epoch: u64,
    compaction_state: CompactionState,
}

pub(crate) struct StoredCompactionState {
    id: u64,
    compaction_state: CompactionState,
    compaction_state_store: Arc<CompactionStateStore>,
}

pub(crate) struct FenceableCompactionState {
    compaction_state: StoredCompactionState,
    local_epoch: u64,
    stored_epoch: fn(&CompactionState) -> u64,
}
```

### Persisting Internal Compactions

1. Compactor fetches compactions from the compaction_state polled during this compactionEventLoop iteration with the compactionStatus as `submitted` and returns a list of compactions.

2. SizeTieredCompactionScheduler executes `maybe_schedule_compaction` and appends to this list of compactions.

3. Compactor executes the `submit_compaction` method on the list of compactions from Step(1). The method delegates the validation of the compactions to the compactor_state.rs.

4. For each compaction in the input list of compactions, the compactor_state.rs executes its own `submit_compaction` method that would do the following validations against the compaction_state:

    - Check If the count of runnning compactions is less than the threshold. If yes, continue

    - Check if the source L0 SSTs and SRs are not part of any other compaction. If yes, continue.

    - Check if the destination SR is not part of any other compaction. If yes, continue.

    - Add compaction validations to verify correct group of sources and destinations are selected. Reference: https://github.com/slatedb/slatedb/blob/main/rfcs/0002-compaction.md#compactions.

    - The existing validations in `submit_compaction` method.

5. When a compaction successfully validates, the status of the compaction is updated as `in_progress` in the compaction_state. When the validation is unsuccessful, the status of the compaction is updated as `failed` in the CompactionState.

6. Try writing the compactor_state to the next sequential .compactor file.

    If file exists,

      - If latest .compactor compactor_epoch > current compactor epoch, die (fenced)

      - If latest .compactor compactor_epoch == current compactor epoch, reconcile the compactor_state and retry.
        [This would happen only when an external process like CLI has written a manual compaction request]

      - If latest .compactor compactor_epoch < current compactor epoch, panic
      ( Compactor_epoch going backwards)

    [When this step is successful, compaction is persisted in the .compactor file]

7. Now, `start_compaction()` for each compaction in the `compactions` param if the count of running compactions is below threshold.

8. A new `CompactionJob` is created using the last job_attempt or afresh if it the first CompactionJob. The `CompactionJob` is then handed to the CompactionExecutor for execution.

9. We need to update CompactionExecutor code to support the following:

    - Resuming Partially executed compactions (covered separately in the section below)

    - Writing compaction_state updates to the .compactor file

    The CompactionExecutor would persist the compaction_state in .compactor file by updating the `compactions` param (refer state management protocol). Two possible options:

      - Each compactionExecutor Job tries writing to the .compactor file.

      - Writes the updated compacted_state to a blocking channel that would be listened and executed by the Compaction Event Handler. We can leverage `WorkerToOrchestratorMsg` enum with a oneshot ack to support blocking of the CompactionJob on the write. [Agreed]


10. Once the compactionJob is completed, follow the steps mentioned in the State Managment protocol.    


### Persisting External Compactions

The idea is to leverage the existing compaction workflow. Need a mechanism to plugin the external requests so that it can be picked and executed by the compaction worflow. The steps are outlined here:

1. Client provides the list of source_ssts and source_srs to be compacted by calling the method `submit_manual_compaction`.

2. Use the `pick_next_compaction` to transform the request into a list of compactions.

3. For each compaction in the input list of compactions, The compactor_state.rs executes its own `submit_compaction` method that would do the following validations against the compaction_state:

    - Check If the count of runnning compactions is less than the threshold. If yes, continue

    - Check if the source L0 SSTs and SRs are not part of any other compaction. If yes, continue.

    - Check if the destination SR is not part of any other compaction. If yes, continue.

    - Check if creating new SR(as part of L0 compaction) would it form a tiered SR group and get compacted. If yes, do not create the SR.

    - Add compaction validations to verify correct group of sources and destinations are selected. Reference: https://github.com/slatedb/slatedb/blob/main/rfcs/0002-compaction.md#compactions.

    - The existing validations in `submit_compaction` method.

  Note: Invalid compactions would be dropped from the list of compactions during validation.

4. When a compaction successfully validates, the status of the compaction is updated as `submitted` in the compaction_state and added/updated in the `new_compactions` list.

5. Try writing the compactor_state to the next sequential .compactor file.

    If file exists,

      - If latest .compactor compactor_epoch > current compactor epoch, die (fenced)

      - If latest .compactor compactor_epoch == current compactor epoch, reconcile the compactor_state and pass `new_compactions` object to Step(3). This process would continue until successful .compactor file write or the `new_compaction` object is empty.
      [This would happen when the compactor has written a .compactor file]

      - If latest .compactor compactor_epoch < current compactor epoch, panic
      ( Compactor_epoch going backwards)

    [When this step is successful, compaction is persisted in the .compactor file]

Note: The validations added in this protocol are best effort as the authority to validate a compaction lies with the compactor daemon thread. For more details refer(https://github.com/slatedb/slatedb/pull/695#discussion_r2289989866)

### Resuming Partial Compactions

1. When the output SSTs(part of the partially completed destination SR) are fetched, pick the lastEntry(the lastEntry in lexicographic order) from the last SST of the SR. Possible Approaches:
    - Add a lastKey in the metadata block of SST as suggested here:https://github.com/slatedb/slatedb/pull/695/files#r2243447106 similar to first key and fetch it from the metadata block 

    - Once on the relevant SST, go to the last block by iterating the indexes. Iterate to the lastKey of the last block of the SST.[Agreed]

2. Ignore completed L0 SSTs and move the iterator on each SR to a key >= lastKey on SST partition

3. This is done by doing a binary search on a SR to find the right SST partition and then iterating the blocks of the SST till we find the Entry. 
    [Note: A corner case: With monotonically overlapping SST ranges(specifically the last key), a key might be present across a contiguous range of SST in a SR]

4. These {key, seq_number, sst_iterator} tuple is then added to a min_heap to decide the right order across a group of SRs( can be thought of as a way to get a sorted list from all the sorted SR SSTs).

5. Once the above is constructed, compaction logic continues to create output SST of 256MB with 4KB blocks each and persist it to the .compactor file by updating the compaction in `compactions`. ( This is the CompactionJob progress section in State Management Protocol) 

Note:
 - Step (3) and (4) are already implemented in the `seek()` in merge_iterator. It should handle Tombstones, TTL/Expiration
 - Ensure the CLI requests are executed on the active Compactor process


### **Key Design Decisions**

#### **1. Persistence Boundaries**
**Decision**: Persist state at the critical boundary:
- **Output SST Completion**: Every ~256MB of written data (always persisted)

**Rationale**: Output SST completions provide the best recovery value per persistence operation. Each represents significant completed work that we don't want to lose.

#### **2. Enhanced Job Model**  
**Decision**: Change from 1:1 to 1:N relationship between Compaction and CompactionJob.

**Rationale**: Enables retry logic, progress tracking, and recovery without breaking existing compaction scheduling logic.

#### **3. State Management Pattern**
**Decision**: Mirror the existing `ManifestStore` pattern with `CompactorStore`.

**Rationale**: Reuses proven patterns for atomic updates, version checking, and conflict resolution that are already battle-tested in SlateDB.

#### **4. Recovery Strategy**
- Resume from last completed output SST

The section below is under discussion here: https://github.com/slatedb/slatedb/pull/695/files#r2239561471


### **Persistent State Storage**

#### **Object Store Layout**
The compaction state is persisted to the object store following the same CAS pattern as manifests, ensuring consistency and reliability:

```
/000000001.compactor  # First compactor state
/000000002.compactor  # Updated state after compactions
/000000003.compactor  # Current state
```

The section below is under discussion here: https://github.com/slatedb/slatedb/pull/695/files#r2239561471

### Protocol for State Management of Manifest and CompactionState

This a proposal for Statement Management of Manifest and CompactionState. The protocol is based on the following principals:

- manifest should be source of truth for reader and writer clients (and should thus contain SRs)

- .compactor file should be an implementation detail of the compactor, not something the DB needs to pay any attention to.

With this view, the entire .compactor file is a single implementation for our built-in (non-distributed) compactor. In fact, for distributed compaction, it need not matter at all. The core of our compaction protocol is simply: the owner of the compactor_epoch may manipulate the SRs and L0s in the manifest.

The .compactor file would serve as an interface over which clients can build their custom distributed compactions say based on etcd(k8s), chitchat, object_store, etc. 
They can have separate files for any approach specific state persistence.

#### On startup...

1. Compactor fetches the latest .compactor file (00005.compactor). 

2. Compactor fetches the latest .manifest file (00005.manifest).

3. Compactor increments `compactor_epoch` and writes the dirty CompactionState to the next sequential .compactor position.(00006.compactor).

    File version check (in-memory and remote object store), If 00006.compactor exists, 

      - If latest .compactor compactor_epoch > current compactor epoch, die (fenced)

      - If latest .compactor compactor_epoch == current compactor epoch, die (fenced)

      - If latest .compactor compactor_epoch < current compactor epoch, increment the .compactor file ID by 1 and retry. This process would continue until successful compactor write.
      ( The current active compactor Job would have updated the .compactor file)

4. If compactor_epoch in in-memory manifest(00005.manifest) >= `compactor_epoch`, the compactor is fenced.

5. Try writing the above `compaction_epoch` to the .manifest file (00006.manifest)

    File version check (in-memory and remote object store), If 00006.manifest exists, 

      - If latest .manifest compactor_epoch > current compactor epoch, die (fenced)

      - If latest .manifest compactor_epoch == current compactor epoch, panic

      - If latest .manifest compactor_epoch < current compactor epoch, increment the .manifest file ID by 1 and retry. This process would continue until successful compactor write.
      ( The current active compactor Job would have updated the .manifest file)

At this point, the compactor has been successfully initialised. Any updates to write a new .compactor(00006.compactor) or .manifest file(00006.manifest) by stale compactors would fence them.

#### On compaction initiation...

(Manifest is polled periodically to get the list of L0 SSTs created. The scheduler would create a list of new compactions for these L0 SSTs as well)

1. Compactor Fetches the latest .manifest file during the manifest poll.(00006.manifest)

2. Compactor writes to the next .compactor file the list of scheduled compactions with the empty JobAttempts (00007.compactor in our example).

    If the file(00007.compactor) exists, 

      - If latest .compactor compactor_epoch > current compactor epoch, die (fenced)

      - If latest .compactor compactor_epoch == current compactor epoch, reconcile the compactor state and go to step (2)

      - If latest .compactor compactor_epoch < current compactor epoch, panic
        (compactor_epoch going backwards)

#### On compaction job progress...

1. Compactor writes to the next .compactor file the compactionState(persist when an SST is added to SR) with the latest progress (00008.compactor in our example).

    If the file(00008.compactor) exists, 

      - If latest .compactor compactor_epoch > current compactor epoch, die (fenced)

      - If latest .compactor compactor_epoch == current compactor epoch, reconcile the compactor state and go to step (2)

      - If latest .compactor compactor_epoch < current compactor epoch, panic
        (compactor_epoch going backwards)

#### On compaction job complete...

1. Write the current compactor state (including the completed compaction job) to the next sequential .compactor file(00009.compactor) (steps (1) and (2) in the "progress" section, above).

2. Update in-memory .manifest state (fetched in compaction initiation phase) with the compaction state to reflect the latest SRs/SSTs that were created (and remove old SRs/SSTs).

3. Write the in-memory .manifest state to the next sequential .manifest file. If the file(00007.manifest) exists, it could be due to two possibilities:

    - Writer has written a new manifest.

    - Reader has written a new manifest with new checkpoints.

    - Compactor has written a new manifest.

    In both the cases, 

      - If latest .manifest compactor_epoch > current manifest epoch, die (fenced)

      - If latest .manifest compactor_epoch == current manifest epoch, reconcile with the latest manifest and write to the next sequential(00008.manifest) .manifest file

      (Writer would have flushed L0 SSTs or updated checkpoint to the manifest)

      - If latest .manifest compactor_epoch < current manifest epoch, panic
        (compactor_epoch going backwards)

### Summarised Protocol

```
1. Compactor A fetches the latest .manifest file(00001.manifest)

2. Compactor A fetches the latest .compactor file(00001.compactor)

3. Compactor A writes .compactor file(00002.compactor) with compactor_epoch(compactor_epoch = 1)

4. If compactor_epoch in .manifest file(00001.manifest) >= compactor_epoch(compactor_epoch = 1), fenced

5. Compactor A writes .manifest file(00002.manifest) with compactor_epoch(compactor_epoch = 1)

(Compactions are scheduled based on the latest manifest poll and CompactionJob updates the .compactor file with the in progress SR state) 

After compactionJob completion...,

6. Update in-memory .manifest(00002.manifest) state to reflect the latest SRs/SSTs that were created (and remove old SRs/SSTs) from the latest .compactor file.

7. Write the in-memory .manifest state to the next sequential .manifest file.

8. If the file(00007.manifest) exists, it could be due to two possibilities:
     - Writer has written a new manifest.
     - Reader has written a new manifest.
     - Compactor has written a new manifest.

 In both cases, compare the `compactor_epoch` in both .manifest file and the local manifest file and write manifest to next sequential file if the compactor is not fenced.
```

### Race conditions handled in the protocol 

#### Incorrect Read order of manifest and compactionState

```
Compactor 1 reads .compactor(compactor_epoch=1, [SR0, SR1, SR2])

Compactor 2 updates .compactor(compactor_epoch=2, [SR0, SR1, SR2])

Compactor 2 updates .compactor(compactor_epoch=2, [SR2])

Compactor 2 updates .manifest(compactor_epoch=2, [SR2])

Compactor 1 reads .manifest ([SR2])

Compactor 1 writes .manifest ([SR1, SR2]) // undoes Compactor 2's change when it should be fenced
```

#### Fenced Compactor Process trying to update manifest

```
.manifest file : [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0]

At T = 0, Compactor A starts(compactor_epoch = 1), updates by creating a sequential .compactor file
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0]

At T = 1, Compactor A (compactor_epoch = 1), updates by creating a sequential .compactor file
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR4(merged), SR3, SR2, SR1, SR0]

At T = 3, Compactor B starts(compactor_epoch = 2), updates by creating a sequential .compactor file (Compactor A is fenced)
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR4(merged), SR3, SR2, SR1, SR0] 

At T = 4, Compactor B (compactor_epoch = 2), updates by creating a sequential .compactor file
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR4(merged), SR0(merged)]

At T = 5, Compactor B updates by creating a sequential .manifest file

At T = 6, Compactor A updates by creating a sequential .manifest file (Fenced Compactor updating Manifest)
```

Note: The protocol still allows fenced compactor to update the manifest if they are in order because compactor is always syncing compaction state. However, it would get fenced if the file already exists. Consider the following case:

#### Fenced Compactor Process trying to update manifest

```
.manifest file : [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0]

At T = 0, Compactor A starts(compactor_epoch = 1), 
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0]

At T = 1, Compactor A (compactor_epoch = 1), updates by creating a sequential .compactor file
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR4(merged), SR3, SR2, SR1, SR0]

At T = 3, Compactor B starts(compactor_epoch = 2), 
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR4(merged), SR3, SR2, SR1, SR0] (Compactor A is fenced)

At T = 4, Compactor B updates (compactor_epoch = 2), updates .compactor file 
.manifest file: [SR7, SR6, SR5, SR4, SR3, SR2, SR1, SR0], 
.compactor file : [SR4(merged), SR0(merged)]

At T = 5, Compactor A updates .manifest file [Compactor is fenced but can still update manifest]

At T = 6, Compactor B updates .manifest file
```

#### Gaps in compactor_epoch in .manifest file

```text
Compactor 1 reads latest .compactor file (00005.compactor, compactor_epoch = 1)
Compactor 1 reads latest .manifest file (00005.manifest, compactor_epoch = 1)
Compactor 1 writes .compactor file (00006.compactor, compactor_epoch = 2)
Compactor 2 reads latest .compactor file (00006.compactor, compactor_epoch = 2)
Compactor 2 reads latest .manifest file (00005.manifest, compactor_epoch = 1)
Compactor 2 writes .compactor file (00007.compactor, compactor_epoch = 3)
Compactor 2 writes .manifest file (00006.manifest, compactor_epoch = 3)
Compactor 1 writes .manifest file (00006.manifest, compactor_epoch = 2) (fenced)
```

Note: The above protocol enables us to use the existing compaction logic for merging L0 SSTs/SRs between manifest and compactionState. Hence, that is not added as part of this protocol.

### **External Process Integration**

**Administrative Commands**:
- `slatedb compaction submit --sources SR1,SR2` - Submit manual compaction
- `slatedb compaction status --id <compaction-id>` - Status of a compaction

We leverage `admin.rs` to expose methods that would be triggered during manual compactions requests from external process / CLI

```rust

// API method signature uses the public struct directly
pub async fn submit_manual_compaction(
    &self, 
    source_ssts: Vec<String>,
    source_srs: Vec<String>                         
) -> Result<CompactionInfo, Error>   

pub async fn get_compaction_info(
    &self,
    id: String
) -> Result<CompactionInfo, Error>           // ← Returns public struct directly

/// Status of a compaction job to be shown to the customer
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionStatusResponse {
    Submitted,      // Waiting to be scheduled
    InProgress,     // Currently executing
    Completed,      // Successfully finished
    Failed,         // Failed with error
}

/// Progress information for an active compaction
#[derive(Debug, Clone)]
pub struct CompactionProgressResponse {
    /// Number of input SSTs processed so far
    pub input_ssts_processed: usize,
    /// Total number of input SSTs to process
    pub total_input_ssts: usize,
    /// Number of output SSTs written
    pub output_ssts_written: usize,
    /// Total bytes processed from input
    pub bytes_processed: u64,
    /// Completion percentage (0.0 to 100.0)
    pub completion_percentage: f64,
    /// Estimated completion time
    pub estimated_completion: Option<DateTime<Utc>>,
}

/// Detailed information about a compaction
#[derive(Debug, Clone)]
pub struct CompactionInfo {
    /// Unique identifier for the compaction
    pub id: String,
    /// Current status
    pub status: CompactionStatusResponse,
    /// Source SSTs being compacted
    pub source_ssts: Vec<String>,
    /// Source SRs being compacted
    pub source_srs: Vec<String>,
    /// Target Destination of compaction
    pub target: String,
    /// Current progress (if running)
    pub progress: Option<CompactionProgressResponse>,
    /// When the compaction was created
    pub created_at: DateTime<Utc>,
    /// When the compaction started (if applicable)
    pub started_at: Option<DateTime<Utc>>,
    /// When the compaction completed (if applicable)
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message (if failed)
    pub error_message: Option<String>,
}
```

### **Garbage Collection Integration**
The garbage collection of .compactor file can leverage the existing logic of garbage collecting .sst files and .manifest files. 

The .sst file is deemed to be garbage collected if it satisfies the following conditions:

  - SST is older than the min age configured.

  - SST is not referenced by any active manifest checkpoint. 

  Note: We would also need to handle the scenario mentioned in https://github.com/slatedb/slatedb/issues/604 to avoid deletion of compacted SSTs and prevent data corruption.

The .manifest file is deemed to be garbage collected if it satisfies the following conditions:  

  - Avoid deletion of the latest manifest
  
  - Delete manifest not referenced by active checkpoints

  - Delete manifests that have passed the min_age

The .compactor file can be cleaned by the garbage collector similar to .manifest file garbage collection conditions:

  - Avoid deletion of the latest .compactor file

  - Delete compactor files that have passed the min_age

## Observability Enhancements

### **Progress Tracking**
- **Real-time progress**: Bytes processed, SSTs completed, estimated completion time
- **Phase tracking**: Reading inputs → Writing outputs → Updating manifest → Completed
- **Recovery metrics**: Work preservation percentage, recovery time

### **Statistics**
- **Performance**: Throughput, duration, success rates by compaction size
- **Recovery**: Jobs recovered, average recovery time, work preservation  
- **Errors**: Categorized by type (network, memory, corruption) for retry decisions
- **Cost**: Persistence operations, overhead percentage

## Cost Analysis

#### **Operation Count Breakdown**

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

#### **Cloud Cost Analysis** 

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


### **Recovery Efficiency Analysis**

#### **Work Preservation Calculation**

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

#### **Scaling Analysis**

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