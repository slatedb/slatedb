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

This RFC proposes the goals & design Compaction State Persistence along with ways to improve current compaction mechanism by adding retries and tracking.

## Goals

- Provide a mechanism to track progress of a `CompactionJob`
- Allow retrying compactions based on the state of the `CompactionJob`
- Improve observability around Compactions
- Separate out compaction related details from `Manifest` into a separate `CompactionManifest`
- Coordination between `Manifest` and `CompactionManifest`
- Coordination mechanism between externally triggered compactions and the main compaction process.

## Non-Goals

- Distributed Compaction: SlateDb is a single writer and currently a single-compactor based database. With distributed compaction, we plan to further parallelise SSTs compaction across different compaction processes. This topic is out of scope of the RFC.

## Constraints

- Changes should be backward compatible and extend the existing compaction structs
- State updation should be cost efficient
- Manifest can be eventually consistent with the latest view after comapaction

## References

- [Compaction RFC](https://github.com/slatedb/slatedb/blob/main/rfcs/0002-compaction.md)
- [Universal Compaction](https://github.com/facebook/rocksdb/wiki/universal-compaction)

## ProblemStatement

This RFC discusses on the extensions discussed in the below github issue. It also addresses several other sub-issues.
[Issue #673](https://github.com/slatedb/slatedb/issues/673):

### **Core Architecture Issues**
1. **1:1 Compaction:Job Cardinality**: Cannot retry failed compactions - entire compaction fails if job fails
2. **No Progress Tracking**: CompactionJob state isn't persisted, making progress invisible
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

#### Steps

1. `Compaction` initialises the `CompactionScheduler` and `CompactionEventHandler` during startup [No change required]
2.  The `CompactionEventHandler` refreshes the compaction state by merging it with the `current manifest`[Need to refresh and merge with the persisted compaction state]
3. `CompactionEventHandler` communicates this compaction state to the `CompactionScheduler` which decides and groups L0 SSTs and SRs to be compacted together.
[This logic would require some changes to re-process the partially processed]
4. A `CompactionExecutor` executes these grouped SSTs by spawning tasks that execute the `compactionJob`.[No change required]
5. The task loads all the iterators in a `MergeIterator` struct and runs compactions on it. It discards older expired versions and continues to write to a SST block. Once the SST block reaches it's threshold size, the block is written to the active destination SR. Periodically the task also provides stats on task progress[Need persist the new block to the compaction state in object store]
6. When a task completes compaction execution, the task returns the {destinationId, outputSSTs} to the `CompactionEventHandler`
7. `CompactionEventHandler` is responsible to update in-memory compaction state, cleanup the job task, write logs and update manifest[Need to update the compaction persistence state]
8. GC clears the orphaned states and SSTs during it's run.

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
**Decision**: Smart recovery based on work preservation efficiency:
- **High efficiency** (>80% work preserved): Resume from last completed output SST
- **Low efficiency**: Full retry with exponential backoff

**Rationale**: Balances complexity vs. value - don't over-optimize recovery for cases where restart is simpler.

### **Data Model**
```
Compaction (1) ──→ (N) CompactionJob
    ├── sources: Vec<SourceId>
    ├── destination: u32  
    ├── job_attempts: Vec<CompactionJob>
    └── current_phase: CompactionPhase

CompactionJob
    ├── attempt_number: u32
    ├── progress: CompactionProgress  
    ├── status: CompactionJobStatus
    └── existing_output_ssts: Option<Vec<SsTableId>>

CompactionProgress
    ├── input_ssts_completed: Vec<SsTableId>
    ├── output_ssts_written: Vec<SsTableId>
    ├── bytes_read/written: u64
    └── current_phase: CompactionPhase
```

### **Persistent State Storage**

#### **Object Store Layout**
The compaction state is persisted to the object store following the same CAS pattern as manifests, ensuring consistency and reliability:

```
/compactor_state_000000001.fbs  # First compactor state
/compactor_state_000000002.fbs  # Updated state after compactions
/compactor_state_000000003.fbs  # Current state
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
    pub active_compactions: HashMap<CompactionId, Compaction>,
    
    /// Historical record of completed compaction jobs
    /// Used for statistics and debugging, cleaned up by GC
    pub job_history: HashMap<CompactionJobId, CompactionJob>,
    
    /// Queue of pending manual compaction requests
    /// Processed in priority order by the active compactor
    pub manual_compaction_queue: Vec<ManualCompactionRequest>,
    
    /// Aggregated performance and reliability statistics
    pub statistics: CompactionStatistics,
    
    /// Timestamp of last coordination with garbage collector
    /// Used to prevent GC from removing SSTs still being compacted
    pub last_gc_coordination_ts: DateTime<Utc>,
    
    /// Active configuration settings for compaction behavior
    pub configuration: CompactionConfiguration,
    
    /// Timestamp when this state was created/last updated
    pub state_timestamp: DateTime<Utc>,
}
```

#### **CAS-Based Atomic Updates**
State updates follow the same CAS pattern as manifest updates for guaranteed atomicity:

1. **Read current state** and extract `next_compactor_state_id`
2. **Apply pending updates** to create new state with incremented ID
3. **Write new state file** using CAS operation with `if-none-match: *`
4. **Retry entire operation** if CAS fails, ensuring atomic state transitions

This approach provides:
- **Atomic state transitions**: Either entire update succeeds or nothing changes
- **Natural state history**: All previous states remain available for debugging
- **Conflict detection**: CAS failures indicate concurrent updates
- **Consistency with manifests**: Uses proven SlateDB persistence patterns

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
1. External process writes `ManualCompactionRequest` to state
2. Active compactor polls state and discovers new requests
3. Compactor schedules manual compaction with appropriate priority
4. Progress of the output SSTs is tracked through persistence mechanism
5. External process can monitor completion through state polling

**Administrative Commands**:
- `slatedb compaction submit --sources SR1,SR2 --priority high` - Submit manual compaction
- `slatedb compaction cancel --id <compaction-id>` - Cancel running compaction  
- `slatedb compaction retry --id <compaction-id>` - Retry failed compaction

#### **State Reconciliation**
When external processes interact with compaction state, the system maintains consistency through:

**Compactor Startup Reconciliation**:
- Read all pending manual compaction requests from state
- Resume or retry in-progress compactions based on their phase
- Clean up completed compactions from active tracking
- Update statistics with recovered job outcomes

**Conflict Resolution**:
- Manual compaction requests are queued and processed in priority order
- Conflicting compactions (same sources) are detected and queued appropriately  
- External cancellation requests are honored at the next safe checkpoint
- State version conflicts during external writes trigger automatic retry

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
    pub priority: CompactionPriority,
    /// Optional deadline for completion
    pub deadline: Option<DateTime<Utc>>,
    /// Optional description/reason for the compaction  
    pub reason: Option<String>,
}

/// Priority levels for compaction scheduling
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionPriority {
    Critical,  // Preempts all other compactions
    High,      // Preempts normal/low compactions  
    Normal,    // Standard automatic compaction priority
    Low,       // Background maintenance priority
}

/// Status of a compaction job
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionStatus {
    Queued,      // Waiting to be scheduled
    Running,     // Currently executing
    Completed,   // Successfully finished
    Failed,      // Failed with error
    Cancelled,   // Cancelled by request
}

/// Progress information for an active compaction
#[derive(Debug, Clone)]
pub struct CompactionProgress {
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
    pub status: CompactionStatus,
    /// Priority level
    pub priority: CompactionPriority,
    /// Source SSTs/SRs being compacted
    pub sources: Vec<String>,
    /// Target level for output
    pub target_level: Option<u32>,
    /// Current progress (if running)
    pub progress: Option<CompactionProgress>,
    /// When the compaction was created
    pub created_at: DateTime<Utc>,
    /// When the compaction started (if applicable)
    pub started_at: Option<DateTime<Utc>>,
    /// When the compaction completed (if applicable)
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message (if failed)
    pub error_message: Option<String>,
}

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
Persistent state provides foundation for multi-compactor coordination and work distribution.