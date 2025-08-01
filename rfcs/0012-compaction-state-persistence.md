# Compaction State Persistence

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Compaction State Persistence](#compaction-state-persistence)
  - [Background](#background)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Constraints](#constraints)
  - [References](#references)
  - [Problem Statement](#problemstatement)
    - [Core Architecture Issues](#core-architecture-issues)
    - [Operational Limitations](#operational-limitations)
    - [Impact](#impact)
  - [Proposal](#proposal)
    - [Core Strategy: Iterator-Based Persistence](#core-strategy-iterator-based-persistence)
    - [Key Design Decisions](#key-design-decisions)
    - [Data Model](#data-model)
    - [Persistent State Storage](#persistent-state-storage)
    - [External Process Integration](#external-process-integration)
  - [Manual Compaction Support](#manual-compaction-support)
    - [Priority-Based Scheduling](#priority-based-scheduling)
    - [Manual Compaction API](#manual-compaction-api)
  - [Public API](#public-api)
    - [Manual Compaction Management](#manual-compaction-management)
    - [Garbage Collection Integration](#garbage-collection-integration)
  - [Observability Enhancements](#observability-enhancements)
    - [Progress Tracking](#progress-tracking)
    - [Statistics](#statistics)
  - [Cost Analysis](#cost-analysis)
    - [Operation Count Breakdown](#operation-count-breakdown)
    - [Cloud Cost Analysis](#cloud-cost-analysis)
    - [Recovery Efficiency Analysis](#recovery-efficiency-analysis)
    - [Work Preservation Calculation](#work-preservation-calculation)
    - [Scaling Analysis](#scaling-analysis)
  - [Future Extensions](#future-extensions)
    - [Distributed Compaction](#distributed-compaction)

<!-- TOC end -->

Status: In Review

Authors:

* [Sujeet Sawala](https://github.com/sujeetsawala)

## Background

Compaction currently happens for the following:
- L0 SSTs
- Various level Sorted Runs(Range partitioned SST across the complete keyspace)

This RFC proposes the goals & design for compaction state persistence along with ways to improve current compaction mechanism by adding retries and tracking.

## Goals

- Provide a mechanism to track progress of a `CompactionJob`
- Allow retrying compactions based on the state of the `CompactionJob`
- Improve observability around Compactions
- Separate out compaction related details from `Manifest` into a separate `CompactionManifest`
- Coordination between `Manifest` and `CompactionManifest`
- Coordination mechanism between externally triggered compactions and the main compaction process.

## Non-Goals

- Distributed Compaction: SlateDb is a single writer and currently a single-compactor based database. With distributed compaction, we plan to further parallelise SSTs compaction across different compaction processes. This topic is out of scope of the RFC.
- Resume logic of partial compactor in case of MVCC because it would depend on the structure of Sorted run. How would the keyspace be partitioned across SSTs of an SR in a non-overlapping manner when MVCC changes are added?

## Constraints

- Changes should be backward compatible and extend the existing compaction structs
- State updates should be cost efficient
- Manifest can be eventually consistent with the latest view after comapaction

## References

- [Compaction RFC](https://github.com/slatedb/slatedb/blob/main/rfcs/0002-compaction.md)
- [Universal Compaction](https://github.com/facebook/rocksdb/wiki/universal-compaction)

## ProblemStatement

This RFC extends discussions in the below github issue. It also addresses several other sub-issues.


[Issue #673](https://github.com/slatedb/slatedb/issues/673):

### **Core Architecture Issues**
1. **1:1 Compaction:Job Cardinality**: Cannot retry failed compactions - entire compaction fails if job fails
2. **No Progress Tracking**: CompactionJob state isn't persisted, making progress invisible
3. **No State Persistence**: All compaction state is lost on restart
4. **SortedRun ID generation**: SortedRun ID is generated depends on the persisted manifest dbState. As a result, there needs to be a sync between compactions and the dbState. Eventual consistency across dbState and CompactionState would be difficult to achive.

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

#### Worflow

### Compaction Workflow

1. `Compactor` initialises the `CompactionScheduler` and `CompactionEventHandler` during startup. It also initialises event loop that periodically polls manifest, periodically logs and provides progress and handles completed compactions [No change required]
2.  The `CompactionEventHandler` refreshes the compaction state by merging it with the `current manifest`[Need to refresh and merge with the persisted compaction state]
3. `CompactionEventHandler` communicates this compaction state to the `CompactionScheduler`(scheduler makes a call `maybeScheduleCompaction` with local database state)
4. `CompactionScheduler` is implemented by `SizeTieredCompactionScheduler` to decide and group L0 SSTs and SRs to be compacted together. It returns a list of `Compaction` that are ready for execution.
[This logic would require some changes to re-process the partially processed]
4. `CompactorEventHandler` iterates over the list of compactions and calls `submitCompaction()` if the count of running compaction is below the threshold.
5. The submitted compaction is validated that it is not being executed( by checking in the local `CompactorState`) and if true, is added to the `CompactorState` struct.
6. Once the `CompactorEventHandler` receives an affirmation, it calls the `startCompaction()` to start the compaction.
7. The compaction is now transformed into a `compactionJob` and a blocking task is spawned to execute the `compactionJob` by the `CompactionExecutor`
8. The task loads all the iterators in a `MergeIterator` struct and runs compactions on it. It discards older expired versions and continues to write to a SST. Once the SST reaches it's threshold size, the SST is written to the active destination SR. Periodically the task also provides stats on task progress. [Need to persist the new SST to the compaction state in object store]
6. When a task completes compaction execution, the task returns the {destinationId, outputSSTs} to the to a worker channel to act upon the compaction terminal state
7. The worker task executes the `finishCompaction()` upon successful `CompactionCompletion` and updates the manifests and trigger scheduling of next compactions by calling `maybeScheduleCompaction()`
8. In case of failure, the compaction_state is updated by calling `finishFailedCompaction()`
9. GC clears the orphaned states and SSTs during it's run.

### Resuming Partial Compactions

1. When the output SSTs(part of the partially completed destination SR) are fetched, pick the lastEntry(the lastEntry in lexicographic order) from the last SST of the SR. Possible Approaches:
    - Have a index on footer as suggested here:https://github.com/slatedb/slatedb/pull/695/files#r2243447106 similar to first key and iterate to the lastKey of each block using the footer 
    - Once on the relevant SST, go to the last block by iterating the indexes. Iterate to the lastKey of the last block of the SST.
3.Ignore completely iterated L0 SSTs and move the iterator on each SR to a key >= lastKey on SST partition
4. This is done by doing a binary search on a SR to find the right SST partition and then iterating the blocks of the SST till we find the Entry. 
5. These {key, seq_number, sst_iterator} tuple is then added to a min_heap to decide the right order across a group of SRs( can be thought of as a way to get a sorted list from all the sorted SR SSTs).
6. Once the above is constructed, compaction logic continues to create output SST of 256MB with 4KB blocks each. 

Note:
 - Step (3) and (4) are already implemented in the `seek()` in merge_iterator. It should handle Tombstones, TTL/Expiration


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

#### **5. Migrate `compaction_epoch` from Manifest to CompactionState**
**Decision**: Deprecate `compaction_epoch` from Manifest.

**Rationale**: 
- Clean separation: DB state vs process coordination
- Process independence: Compactor can run separately
- Logical grouping: Epoch lives with compaction concerns

### **Data Model**
```
Compaction (1) ──→ (N) CompactionJob
    ├── sources: Vec<SourceId>
    ├── destination: u32  
    ├── job_attempts: Vec<CompactionJob>
    └── current_status: CompactionStatus
    └── created_ts: u64
    └── updated_ts: u64

CompactionJob
    ├── attempt_number: u32
    ├── progress: CompactionProgress  
    ├── current_status: CompactionJobStatus
    └── existing_output_ssts: Option<Vec<SsTableId>>
    └── input_ssts_completed: Vec<SsTableId>          // To skip completed L0 SSTs if any
    └── output_ssts_written: Vec<SsTableId>           // Single output SR with a collection of SSTs
    └── created_ts: u64
```

### **Persistent State Storage**

#### **Object Store Layout**
The compaction state is persisted to the object store following the same CAS pattern as manifests, ensuring consistency and reliability:

```
/compactor_state_000000001.compactor  # First compactor state
/compactor_state_000000002.compactor  # Updated state after compactions
/compactor_state_000000003.compactor  # Current state
```

#### **CompactionState Structure**
The persistent state contains the complete view of all compaction activity:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionState {
    /// Fencing token to ensure single active compactor
    /// Incremented each time a new compactor takes control
    pub compactor_epoch: u64,
    
    /// Next state ID to use for CAS operations
    /// Current state ID = this value - 1
    pub next_compactor_state_id: u64,
    
    /// All currently active compactions indexed by ID
    /// Includes queued, running, and recently completed compactions
    pub active_compactions: BTreeMap<CompactionId, Compaction>,
        
    /// Timestamp when this state was created/last updated
    pub state_timestamp: DateTime<Utc>,
}
```
The section below is under discussion here: https://github.com/slatedb/slatedb/pull/695/files#r2239561471
<!-- #### **CAS-Based Atomic Updates**

CAS based atomic updates can occur in two scenarios. These scenarios are as follows:
1. Compactor Fencing.
2. Optimistic Locking of CompactionState

On startup, a compactor must increment `compactor_epoch` in the `CompactionState`.

1. List `CompactionState` to find the `CompactionState` with the largest ID.
2. Read the latest CompactionState (e.g. compaction/00000000000000000002.compactor).
3. Increment the `compactor_epoch` in the current CompactionState in memory.
4. CAS based atomic update of the CompactionState with the updated `compactor_epoch` (e.g. compaction/00000000000000000003.compactor).

Now, there are 4 possible outcomes of the CAS-Based Atomic Updates
- The write is successful.

- The write was unsuccessful. Another compactor wrote the `CompactionState` with the same ID and a lower (older) `compactor_epoch`.[OptimisticLockingConflict]
  1. This case can be reproduced when a new compactor starts, as mentioned in the above state it prepares a `CompactionState` with updated `compaction_epoch`
  2. However, before the compactor can write the `CompactionState` to the object store, the old compactor persisted `CompactionState` from it's Compaction execution cycle.
  3. Now, the new Compactor tries to persist the `CompactionState` and finds that the ID matches and the object store `compactor_epoch` is less than the new `compactor_epoch`
  4. The new Compactor should refresh the local `CompactorState` and increment the `compactor_epoch` in the local `CompactorState`.
  5 Now, it should again retry writing the `CompactionState` to the object_store. If the write fails, go back to step 4

- The write was unsuccessful. Another compactor wrote the `CompactionState` with the same ID and the same `compactor_epoch`.[OptimisticLockingConflict]
  1. This case can be reproduced when two new compactors starts lets call them c1 and c2, as mentioned in the above state it prepares a `CompactionState` with updated `compaction_epoch`
  2. Compactor c1 successfully writes the `CompactionState` to object_store
  3. Now, the Compactor c2 tries to persist the `CompactionState` and finds that the ID and matches and the object store `compactor_epoch` is also equal to the c2's `compactor_epoch`
  4.The new Compactor should refresh the local `CompactorState` and increment the ID in the local `CompactorState`.
  5 Now, it should again retry writing the `CompactionState` to the object_store. If the write fails, go back to step 4

- The write was unsuccessful. Another writer wrote the `CompactionState` with the same ID and a higher (newer) `compactor_epoch`.[FencedCompactor]
  1. This case can be reproduced when the active Compactor is processing a compaction, however another compactor starts and updates the `CompactionState` with updated `compaction_epoch` in the object_store.
  2. Now, when the active Compactor tries to write the `CompactionState` to the object store, it finds the `compactor_epoch` higher than its `compactor_epoch`
  3. In this case, the Compactor should gracefully shutdown after clean up.

Once the compactor the process is active, it can encounter following conditions when persisting SRs

1. if compactor_epoch in file > then we're fenced
2. if compactor_epoch in file == panic, this should never happen (we have two processes writing with the same compactor epoch)
3. if compactor_epoch in file < panic, this should never happen (we somehow went backwards in epochs after the current process fenced any previous epoch writers)

State updates follow the same CAS pattern as manifest updates for guaranteed atomicity:

1. **Read current state** and extract `next_compactor_state_id`
2. **Apply pending updates** to create new state with incremented ID
3. **Write new state file** using CAS operation with `if-none-match: *`
4. **Retry entire operation** if CAS fails, ensuring atomic state transitions

This approach provides:
- **Atomic state transitions**: Either entire update succeeds or nothing changes
- **Natural state history**: All previous states remain available for debugging
- **Conflict detection**: CAS failures indicate concurrent updates
- **Consistency with manifests**: Uses proven SlateDB persistence patterns -->

### **External Process Integration**

#### **Read-Only Access Pattern**
External processes can safely read compaction state without interfering with the active compactor:

**CLI Status Commands**:
- `slatedb compaction status` - Show all active compactions with progress
- `slatedb compaction list --failed` - List failed compactions needing attention
- `slatedb compaction history --last 24h` - Show recent compaction activity
- `slatedb compaction stats` - Display performance and efficiency metrics


#### **Write Access Through Coordination**
External processes that need to trigger compactions coordinate through the persistent state:

**Manual Compaction Submission**:
1. External process submits a CompactionRequest
2. The request is processed  by calling a method analogous to `maybeScheduleCompaction()`. However, this method would take the input source SSTs/SRs provided and return a `Compaction`. This could be a wrapper method deciding which method to call based on `CompactionType`
(Post this step the regular Compaction workflow begins.)
3. If the count of ongoing Compactions is less than the threshold, the `Compaction` is submitted for compaction to the `submitCompaction()` 
4. Once it passes the validations in the `submitCompaction()`, the event handler proceeds with the `startCompaction()` converting the compaction into a compactionJob.
4. The compactionJob is then executed in a blocking task and the terminal state of the execution is then handled by the event handler.

**Administrative Commands**:
- `slatedb compaction submit --sources SR1,SR2 --priority high` - Submit manual compaction
- `slatedb compaction cancel --id <compaction-id>` - Cancel running compaction  
- `slatedb compaction retry --id <compaction-id>` - Retry failed compaction

## Manual Compaction Support

Extend compaction system to support operator-initiated compactions:

### **Priority-Based Scheduling**
- **Critical**: Manual compactions with deadlines
- **High**: L0 threshold breaches, urgent manual compactions
- **Normal**: Regular size-tiered compactions  
- **Low**: Background maintenance

### **Manual Compaction API**
```
submit_manual_compaction(sources, target_level, priority, deadline)
cancel_compaction(compaction_id)  
get_compaction_status(compaction_id)
list_active_compactions()
```

## Public API

### **Manual Compaction Management**

We extend the `Db` API to support manual compaction operations, and introduce a new `CompactionManager` API for administrative operations:

```rust
/// Options for manual compaction submission
pub struct ManualCompactionOptions {
    /// Priority level for the compaction
    pub priority: CompactionRequestPriority,
    /// Optional deadline for completion
    pub deadline: Option<DateTime<Utc>>,
    /// Optional description/reason for the compaction  
    pub reason: Option<String>,
}

/// Priority levels for compaction scheduling
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionRequestPriority {
    Critical,  // Preempts all other compactions
    High,      // Preempts normal/low compactions  
    Normal,    // Standard automatic compaction priority
    Low,       // Background maintenance priority
}

/// Status of a compaction job
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionRequestStatus {
    Submitted,      // Waiting to be scheduled
    InProgress,     // Currently executing
    Completed,      // Successfully finished
    Failed,         // Failed with error
    Cancelled,      // Cancelled by request
}

/// Progress information for an active compaction
#[derive(Debug, Clone)]
pub struct CompactionRequestProgress {
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
    pub id: CompactionId,
    /// Current status
    pub status: CompactionRequestStatus,
    /// Priority level
    pub priority: CompactionRequestPriority,
    /// Source SSTs/SRs being compacted
    pub sources: Vec<String>,
    /// Target Destination of compaction
    pub target: String,
    /// Current progress (if running)
    pub progress: Option<CompactionRequestProgress>,
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

#### Example
```rust
// CLI parses arguments into this public struct
let options = ManualCompactionOptions {
    priority: CompactionRequestPriority::High,           // --priority high
    deadline: Some(parse_datetime("2025-01-30T10:00:00Z")), // --deadline
    reason: Some("urgent cleanup".to_string()),   // --reason
};

// API method signature uses the public struct directly
pub async fn submit_manual_compaction(
    &self, 
    sources: Vec<String>,                    
    options: ManualCompactionOptions        
) -> Result<CompactionInfo, Error>   

pub async fn get_compaction_info(
    &self,
    id: CompactionId
) -> Result<CompactionInfo, Error>           // ← Returns public struct directly

// API method returns vector of public struct
pub async fn list_compactions(
    &self,
    status_filter: Option<CompactionRequestStatus>  // ← Public enum
) -> Result<Vec<CompactionInfo>, Error> 

```        

### **Garbage Collection Integration**
- The garbage collector would be responsible to delete the entries in the compaction state files based on the two conditions:
  - min_age
  - CompactionJob associated with the compaction state is `Complete` or `Attempts_Exhausted`
- As mentioned in the earlier section, the manifest update with the compacted SRs would only happen when the `CompactionJob` completes successfully.

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

### **Distributed Compaction**
- Persistent state provides foundation for multi-compactor coordination and work distribution.
- Define a minimum time boundary between compaction file updates to prevent excessive writes to the file (see https://github.com/slatedb/slatedb/pull/695#discussion_r2229977189)
- Support for compactor persistence on MVCC supporting SRs.