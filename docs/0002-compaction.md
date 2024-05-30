# SlateDB Compaction

Table of Contents:

Status: Under Discussion

Authors:
* [Rohan Desai](https://github.com/rodesai)

References:

* https://github.com/slatedb/slatedb/issues/7
* https://smalldatum.blogspot.com/2018/08/name-that-compaction-algorithm.html
* https://smalldatum.blogspot.com/2018/07/tiered-or-leveled-compaction-why-not.html
* https://smalldatum.blogspot.com/2018/10/describing-tiered-and-leveled-compaction.html
* https://github.com/facebook/rocksdb/wiki/Universal-Compaction
* https://stratos.seas.harvard.edu/publications/dostoevsky-better-space-time-trade-offs-lsm-tree-based-key-value-stores
* https://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf
* https://www.scylladb.com/2018/01/17/compaction-series-space-amplification/
* https://enterprise.docs.scylladb.com/stable/architecture/compaction/compaction-strategies.html

## Current Implementation

SlateDB maintains a live memtable, a set of immutable memtables, and metadata about SSTs in the WAL. `put()` writes the key-value into the memtable. At some interval, the flushing thread closes the memtable and flushes the contents to a new WAL SST. Immediately before the flush, the memtable is converted to an immutable memtable.

`get()` operations search the current memtable, immutable memtables, and WAL SSTs in reverse-write order (most recent SSTs first). The search is terminated when the key specified in `get` is found.

## Problem

There are a few problems with the current implementation that we address with this design:
1. As the DB grows, `get()` becomes very expensive because it has to search an ever-growing list of SSTs.
2. Similarly, as the DB grows, start becomes very slow because SlateDB has to reload the state of all the SSTs
3. The storage space used grows without bound even if the key space is finite. This is less of a problem for SlateDB as compared to disk-based databases because space in s3 is practically unlimited, and is relatively cheap (e.g. .023$/GB vs .08$/GB for EBS (.16$/GB for HA)). Still it is not ideal - storage is cheaper but not free. More troubling is that SlateDB relies on caching metadata effectively for good read performance. This gets harder and harder to do as the number of SSTs grows.

## Goals

- Compact WAL SSTs into larger SSTs so that space from overwrites and deletes can be reclaimed, and reduce read amplification.
- Further compact compacted SSTs to further reduce read and space amplification.
- Define a simple initial compaction algorithm that balances the various types of amplification.
- Allow for flexibility in the specific compaction algorithm. This is likely going to be an area that we can uniquely innovate, and the design should support easily iterating on compaction policy.
- Split some compaction work between the writer and compactor processes to allow for better network utilization.
- Support manual major compaction that compacts the whole database.

## Non-Goals

The following are out-of-scope for this design. That said, the design should not preclude them. For some items, we will describe how the design can be adapted to achieve these goals in follow-on work.

- Optimize for specific workloads. For example, some databases optimize for in-order bulk inserts by avoiding compaction and simply writing out the database in its fully compacted form as inserts arrive.
- Similarly, specialized compaction policies optimized for specific workloads are out-of-scope. For example, some databases support specialized compaction for time series data with a finite lifetime.
- Resumable compaction. This proposal will not define how to resume a long-running compaction after a compactor restart. This is important to support, as compactions could take 10s of minutes, and the compactor should not have to restart them after a failure. I’ll include a section at the end on how we can extend the design to support htis.

## Proposal

### Amplification

The design for compaction depends on the database and workload sensitivity to write, read, and space amplification. We will likely ultimately need to support a variety of compaction strategies to accommodate different types of workloads. However there are some common considerations given SlateDB's object-store based architecture:

1. write amplification: write amplification refers to the added work done by compaction for every write. This is usually measured as some multiplier. For example, if a write is written to the database 5 times (once for the initial write and compacted 4 times) - write amplification is 5x. Write amplification is not much of a concern from a cost perspective. The major cloud providers don't charge for data transfers to/from the "standard" tier of object storage (provided you set up networking correctly). There are charges to/from zonal tiers like s3express, but I expect that most of the data will reside in the regional tier. Write amplification is a concern from a bandwidth usage perspective. If write amplification is too high, writes will become bottlenecked on the compactor node's network. This is usually a higher limit than local or attached disk, but not an order of magnitude higher (~1.5-2x nw-out baseline:local disk write). So we will need to take care to avoid too much write amplification to support write-heavy workloads.

2. read amplification: read amplification refers to the added work done by the database for reads. This includes added cpu from searching through multiple possible locations for a read, and added i/o from reading multiple locations. We certainly don't want to be doing multiple, if any, GET operations per SlateDB read, as GETs are expensive and slow. Read amplification can be mitigated by effective caching. Ideally, for optimal performance and cost, reads can be served entirely from cache, which spans memory and local disk. Still, we probably don't want to be doing multiple disk reads per read or we may saturate the local/attached disks. Therefore its desirable that the filter and index blocks used for most reads fit in memory. All of this is to say that it is pretty important for SlateDB to reduce read amplification. We need the set of SSTs to search for a read to be small enough so that indexes/filters can fit in memory, and data being read fits in total cache most of the time.

3. space amplification: SlateDB is not as sensitive to space amplification as disk-based databases are because storage is practically unbound, and is cheap (about 1/5th-1/20th the cost of an on-disk db on s3 depending on replication factor, instance stores vs ebs, reserved vs on-demand, etc). One problem that we can totally sidestep is transient space amplification from compaction. Most dbs suffer from this problem and have to either over-allocate disk space or implement incremental compaction. This won’t be an issue for us. Our main concern with space amplification is that it grows the search space for reads as described above.

### High Level Overview

In the rest of this document, we propose mechanisms for compacting together SSTs to purge duplicates and keep the total count of SSTs contained. Compaction will be executed both by the main writer and by a separate compactor process.

The main writer compacts the WAL to the first level of the database. This has a number of benefits:
As we describe below, the main writer compacts the WAL directly from memory without reading the data back from S3 first. This saves cost for low-latency writers and more importantly, reduces load on the network.
This offloads some of the compaction i/o from the compactor onto the writer. Assuming the main bottleneck for compaction will be network, this should help us achieve more throughput.
It should be easier to coordinate between the compactor and writer for lower-frequency compactions to lower levels than WAL->L1 compactions.

The compactor is responsible for compacting L1 and the lower levels of the database. Compacted SSTs are maintained in a series of Sorted Runs (SRs). Each SR spans the full keyspace of the database. A SR is made up of an ordered series of SSTs, each of which contains a distinct subset of the total keyspace. We use Sorted Runs instead of large SSTs because it is a simple way to keep the size of the metadata blocks small enough so that they can be easily paged in and out of cache without polluting the cache. Larger SSTs have larger metadata blocks. It’s expensive to page them in and out of cache, and they are likely to displace other data that should be retained by the cache to achieve a high hit rate.

The SRs are themselves ordered by age. When executing point lookups, SlateDB looks up the value for a key in age-order, and terminates the search at the first SR that contains the key. Range scans read every SR and sort-merge the result.

### Manifest Changes

```
table SortedRun {
    id: uint32
    ssts:[SstId];
}

table SstId {
    high: uint64,
    low: uint64
}

table Compacted {
    runs: [SortedRun]
}

table Manifest {
    …
    l1: [SstId]
    compacted: Compacted
    compactions: Compaction
    …
}
```

We propose to augment the manifest by adding the following fields:

`l1`: Contains a list of SST IDs in L1

`compacted`: Contains a single instance of `Compacted`. `Compacted` contains a list of `SortedRun` instances. A `SortedRun` instance defines a single sorted run. Each Sorted Run contains a list of SST IDs and has a unique ID. The list of SST IDs defines the SSTs that comprise the sorted run. A given SST belongs to at most 1 SR. The ID describes the SR’s position in the list of sorted runs in `compacted`. That is, an SR S with an S.id must occur after SR S’ with ID S’.id if S.id < S’.id (so the sorted run with ID 0 must be last in the list). The last SR in the list must have ID 0. The semantics of the ID will be important when we describe how to define compactions.

#### Naming Compacted SSTs (L1 and L2+)
We will use ULIDs to name compacted SSTs (TODO: explain reasoning). The ULID is stored in the manifest in the `SstId` table, with the `high` and `low` fields containing the high and low bits of the ULID, respectively.

In the Object Store, compacted SSTs are stored under the compacted directory. Each SST object is named using its ULID and the suffix `.sst`, e.g:

```
/compacted/01ARZ3NDEKTSV4RRFFQ69G5FAV.sst
/compacted/01BX5ZZKBKACTAV9WEVGEMMVRZ.sst
…
```

### DB Options

Compaction behavior is be governed by the following DB options:

`l1_sst_size_bytes`: defines the target L1 SST size in bytes.

`memtable_flush_interval_ms`: defines the minimum interval between memtable flush-related manifest updates in milliseconds.

`l1_compaction_threshold_ssts`: defines the threshold number of SSTs for compacting L1.

`l1_max_ssts`: defines the maximum number of uncompacted L1 SSTs.

`max_compactions`: defines the max number of concurrent compactions.

`compaction_policy`: defines the compaction policy (currently only supports the value `tiered`)

`compaction_policy.tiered.level_compaction_threshold_runs`: defines the threshold number of sorted runs for compacting a lower level (specific to the tiered compaction policy).

`compaction_policy.tiered.level_max_runs`: defines the maximum number of sorted runs at alower level (specific to the tiered compaction policy).

### WAL->L1 Compaction

The writer is responsible for compacting the WAL to the first level of the database. It does this directly from the memtable rather than reading back the WAL first. Instead of freezing the memtable when writing the WAL, it instead retains it until enough data has accumulated to fill an L1 SST. Only then does it freeze the memtable and write out an L1 SST. When it’s been longer than `memtable_flush_interval` since the last manifest update, the writer updates the manifest with the new SSTs. The L1 writes are fenced by virtue of committing L1 SSTs in a manifest update. The newly written L1 SSTs are only considered part of the database when the manifest has been updated.

Let’s look at the write and recovery protocols in more detail:

write:
1. `put` adds the key-value to the current memtable and updates the WAL using the protocol described in [the manifest design](https://github.com/slatedb/slatedb/blob/main/docs/0001-manifest.md). 
2. If the current memtable is larger than `l1_sst_size`, freeze the memtable by converting it to an immutable memtable, and write the memtable to a new ULID-named SST S in `compacted`.
3. When S and all earlier SSTs S’, S’’, … for unflushed immutable tables are written:
    1. update L1 in-memory by prepending S, S’, S’’, … to the list of SSTs in L1.
    2. clear the immutable memtables for S, S’, S’’,...
    3. if time since last manifest update > `memtable_flush_interval`:
        1. if (number l1 SSTs > l1_max_uncompacted):
            1. pause new writes
            2. wait till number of l1 SSTs < l1_max_uncompacted
        2. unpause writes if paused
        3. update the manifest using CAS with the following modifications:
            1. update `last_compacted` to the last WAL SST included in S
            2. update `l1` by prepending S, S’, S’’, … to the list
4. If CAS from (3) fails:
    1. If CAS fails and the manifest has a different writer epoch, exit
    1. If CAS fails and the manifest has the same writer epoch, go back to 3.iii.c
5. If CAS from (3) succeeds, remove the immutable memtable.

write-recovery:
1. Fence older writers as described in the (manifest design)[https://github.com/slatedb/slatedb/blob/main/docs/0001-manifest.md].
2. Create a new mutable memtable
3. For every WAL SST W after `last_compacted`, reapply the writes as described above in the write protocol.

TODO: should we have some way to bound the number of immutable memtables in memory? I left it out since we are free to purge them once their SSTs have been written (even before we update the manifest). But this doesn’t help us if we can’t write the SSTs to S3 for some reason - though in that case we likely can’t commit writes anyway.

### Compacting Lower Levels

The Compactor compacts L1 and the lower levels. It contains two logical processes: a Compaction Executor and a Compaction Policy. The Compaction Policy observes the current state of the database and schedules Compactions. The Compaction Executor bootstraps the Compactor, executes the Compactions scheduled by the Compaction Policy, and notifies the Compaction Policy about status. The Compaction Executor is fixed, while The Compaction Policy is modular to allow SlateDB to support different compaction styles. The specific policy is specified in the `compaction.policy` db option. Initially, we will implement a single policy that performs tiered compaction.

#### Interfaces

Lets start by defining the interface between the CompactionExecutor and CompactionPolicy. I’ll define them as Rust structs/traits. Then, we discuss how each component implements the interfaces.

##### Compactions

The Compaction Policy tells the Compaction Executor what compactions to execute. A Compaction is defined using the following parameters:
* Sources: A list of one or more sources of data to compact. A source can either be a single L1 SST, or a single SR. The Sources must be logically consecutive. This means that for any sources S1 and S2 where S2 appears immediately after S1 in the list:
    * If S1 is an L1 SST, then S2 must either be the next L1 SST OR if S1 is the last L1 SST then S2 must be the first SR (SR with the highest ID)
    * If S1 is an SR, then S2 must be the next SR.
* Destination: A destination SR. This can be a new SR, or it can be the SR from Sources with the lowest ID. If it’s a new SR, The SR must be logically consecutive to the last element of Sources (as described above).

Let’s look at some examples of valid/invalid compactions. I’ll use string IDs for SSTs here instead of ULIDs. Suppose our manifest looks like:

```
l1: [SST-1, SST-2, SST-3, SST-4]
compacted: [100, 50, 3, 1, 0]
```

Here are examples of valid/invalid compactions (I’m using the notation Sources->Destination)

`[SST-3, SST-4]->101`: This describes compacting the last 2 L1 SSTs to a new SR

`[SST-1, SST-2]->101`: This is invalid because it skips SST-3 and SST-4

`[SST-4, 100]->100`: This describes compacting SST-4 and SR 100 and saving the result as SR 100

`[100, 50]->2`: This is invalid because it writes the result to an SR that is not consecutive to 50 (3 is consecutive to 50)

`[SST-1, SST-2, SST-3, SST-4, 100, 50, 3, 1, 0]->0`: This describes a major compaction that compacts everything and saves it as SR 0

Observe that we can use this basic definition to describe compactions done by different compaction algorithms (this isn’t strictly true in the above proposal - e.g. it doesn’t currently support some-to-all compactions like compacting a single SST from one SR into another SR, but that’s a fairly straightforward extension to the definition of a source) - it’s up to the Compaction Policy to decide what compactions to execute. The policy can choose to implement leveled compaction by viewing each SR as a level and scheduling Compactions that always merge one SR into the next SR. Or it can implement tiered compaction by grouping SRs into levels and define compactions that merge all the SRs in a level into a new SR at the next level. The levels themselves are a logical construct maintained by the policy.

In Rust, this looks like:

```
union SourceId {
    sorted_run: u32,
    sst: Ulid,
}

struct Compaction {
    id: u32 // a unique identifier for the compaction (it must be unique to the process’s lifetime)
    sources: Vec<SourceId>
    destination: u32
}
```

##### Compaction Executor
The Compaction Executor provides the following interface to the CompactionPolicy:

```
trait CompactionExecutor {
    /*
     * Notifies the compaction executor about a new compaction to execute. The result
     * is Ok if the compaction was accepted by the executor. This does not mean that the
     * compaction was completed. The executor validates the compaction and returns an
     * error if the compaction is invalid.
     */
    fn compact(&self, compaction: Compaction) -> Result<(), Error>
}
```

##### Compaction Policy
The Compaction Policy provides the following interface to the Compaction Executor:

```
enum CompactorUpdateKind { DBState, CompactionFinished }

struct DBStateUpdate {
    kind: CompactorUpdateKind // always DBState
    state: DBState,  // we probably don’t want to use DBState here, but rather a subset that contains compactor-relevant state like l1 and compacted. But, you get the idea.
}

struct CompactionFinished {
    kind: CompactorUpdateKind // always CompactionFinished
    compaction_id: u32,
    state: DBState,
}

union CompactorUpdate {
    db_state: DBStateUpdate,
    compaction_finished: CompactionFinished,
}

trait CompactionPolicy {
    /*
     * Notifies the policy that it should start evaluating the db for compaction. This method
     * receives a channel over which the Executor sends the Policy updates about changes
     * to the database. This includes updates about changes to the database (e.g. arrival of
     * new L1 files), and completion of compactions.
     */
    fn start(&self, executor: Box<dyn CompactionExecutor>, chan: Receiver<CompactorUpdate>);
}
```

#### Compaction Executor

The Compaction Executor initializes as follows:
1. Update the `compactor_epoch` in the manifest using CAS
2. Initialize the Compaction Policy by creating an instance based on `compaction.policy`, creating, a channel, and calling `start`
3. Send the initial db state (as returned in the manifest) over the channel

Then, the compactor periodically polls the manifest. On every poll, the compactor:
1. Check if the `compactor_epoch` is different than the compactor’s epoch. If it is, exit.
2. If the db has new l1 SSTs, send a `DBStateUpdate` message over the policy channel.

##### Executing a Compaction

The Compaction Executor implements `compact` by:

1. Validate the compaction by running through the following checks. If any fail, return error
    1. Make sure the compaction is valid as defined above in the Compaction section
    2. Make sure there is no other ongoing compaction that includes the SSTs or SRs referenced by the compaction.
2. At this point, the call to `compact` returns.
3. Schedule the compaction for execution in the background.

The Compaction Executor executes the compaction by reading the SSTs and SRs in `sources` and sort-merging them into a new SR.

The Compaction Executor needs to coalesce updates. If the same key appears in multiple sources, then it takes the value from the logically earliest source. The Compaction Executor handles destination SR 0 specially. If the destination SR is 0, and the value for a key is resolved to a tombstone, then the Compaction Executor will not include include the key in the resulting SR.

The new SR is made up of ULID-named SSTs in the `compacted` directory (just like L1). 

We should implement the sort-merge so that we can make good use of the available network. One good option here is to use `async` Object Store APIs to concurrently read the various sources, and then to write the resulting SSTs while we move on to the next key ranges. I think the details are something we can work out in the implementation, and it doesn’t have to be optimal in this iteration of work.

Note that compacting whole sorted runs can create a lot of temporary space amplification, especially for compactions that read the last level. This is not a major concern for SlateDB as ObjectStore capacity is practically infinite, and the usage is temporary so it should not contribute meaningfully to cost.

When the new SR has been fully written out, the Compaction Executor finishes the compaction by:
1. Read the existing manifest and verify that the `compactor_epoch` matches the compactor’s epoch. If it does not, exit.
2. Generate a new manifest that has the new SR in `compacted` and the SR and SSTs from `sources` removed.
3. Write the new manifest using CAS. If CAS fails, go back to 1
4. Send the Compaction Policy a CompactionFinished message with the compaction ID and the updated DB state.

#### Compaction Policy

The Compaction Policy is responsible for selecting the next Compaction. Our goal here is to implement something simple that works, and then iterate/optimize on it in future cycles. Initially we propose to implement basic tiered compaction, which tries to maintain sorted runs in size-based levels, and constrains the number of sorted runs in a given level by merging the runs together when there are too many of them, usually moving the resulting run to the next level.

We choose tiered compaction because it works well for workloads with a moderate to high volume of writes. It still guarantees that the total number of runs is roughly O(log(N)) where N is the size of the db, but allows multiple runs to accumulate at a level to reduce the amount of merging (at the cost of a multiplier T on the number of runs, where T is the number of runs that can accumulate at a level - so it’s really O(Tlog(N))). So the drawback is that there can be significant space and read amplification. However, I think there are reasonable ways to work around most of these drawbacks for now, and we can trade off some write amplification for better read/space amplification down the line by implementing “lazy-leveling” as described in [dostoevetsky](https://scholar.harvard.edu/files/stratos/files/dostoevskykv.pdf).
* Tiered compaction maintains `level_compaction_threshold_ssts` sorted runs even at the lowest level, which means the entire keyspace may be copied `level_compaction_threshold_runs` times. This is somewhat concerning from a cost pov, however as explained at the beginning of the design Object Store costs are much lower than block device costs (by a factor of 5-20x), so this is less of a concern for SlateDB than traditional stores.
* The more concerning consequence of high space amplification is that it will likely directly lead to requiring a proportionally larger cache to achieve high hit rates for good read performance and cost. We can deal with this at first by allocating a larger cache. Eventually, we can do leveled compaction to the final level (meaning, maintain a single run at the final level) to dramatically reduce space amplification as described in <doestoevsky paper>. Further, we can estimate space amplification by looking at the relative size of the non-last levels and final level, and execute compactions when amplification crosses some threshold.
* Tiered compaction also yields a db with more Sorted Runs, which adds to the cost of point lookups. Impact to point lookup cost should be dramatically reduced by SlateDB’s usage of bloom filters. However, more Sorted Runs does mean that we will need more SST reads on average (as the expected number of bloom filters that return a fp increases linearly with the number of Sorted Runs). There are a number of approaches to solving this problem:
* Universal compaction in RocksDB takes the approach of compacting when the total number of runs crosses some threshold. [Monkey](https://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf) describes a simple optimization we can adapt that reallocates bloom filter bits from lower levels to higher levels to dramatically decrease the false-positive-rate (fpr) of higher-level filters at the cost of higher fpr at lower levels, but yielding a much lower average number of lookups (see [here](http://daslab.seas.harvard.edu/monkey/) for a good visualization).
* Short range-scans are fundamentally worse with tiered storage as they need to examine a roughly equal amount of data in every Sorted Run. If it’s problematic we can adjust compaction to more aggressively merge runs together with some goal for the total number of runs (similar to RocksDB’s universal compaction). Long range-scans are not as bad as they derive most of their cost from the final level, and with lazy-leveling there is a single final level.

All of this is to say, Tiered compaction feels like a great starting point for a general-purpose store, and we have reasonable short-term workarounds and long term solutions to most of the problems that we anticipate with tiered compaction.

SlateDB’s tiered Compaction Policy will work as follows:
1. Whenever a new CompactorUpdate arrives on the policy channel:
    1. The Compaction Policy groups SRs into levels L2, L3,... A level with a larger index is considered “lower” (ugh - this is confusing). The size of the runs in LN is at most `l1_sst_size X l1_compaction_threshold X compaction.policy.tiered.level_compaction_threshold^(N - 1)`
    2. Iterate over the levels from lowest to highest. For each level, maybe schedule a compaction. The Compaction includes all SRs in the level as its source (TODO - we could probably include compactions for the higher level as well if it needs to be compacted, and so on - but we can add this later). The destination SR ID is the ID of the last SR in the level. Schedule a compaction for level N if:
        1. The number of SRs in N > `compaction.policy.tiered.level_compaction_threshold_runs`
        2. The number of SRs in N+1 < `compaction.policy.tiered.level_max_runs`
        3. The number of uncompleted compactions < `max_compactions`
        4. No ongoing compaction from level N
    2. Maybe schedule a compaction for L1. The Compaction includes all SSTs in L1 as its source. The destination SR ID is the highest unused SR ID. Schedule a compaction for L1 if:
        1. The number of SRs in L1 > `l1_compaction_threshold_ssts`
        2. The number of SRs in L2 < `compaction.policy.tiered.level_max_runs`
        3. The number of uncompleted compactions < `max_compactions`
        4. No ongoing compaction from L1


### Back-Pressure

The design described above applies back-pressure so that we don’t wind up writing faster than we can compact and get unbounded read/space amplification. SlateDB blocks new writes if the number of SSTs in L1 exceeds `l1_max_ssts`. This shouldn’t happen if compaction is keeping up and is able to merge SSTs from L1 to L2. Compactions to L2 are blocked if the number of runs in L2 is greater than `compaction.policy.tiered.level_max_runs`. Similarly, Compactions to L3, L4, … LN are blocked if the number of runs in those levels exceeds the threshold.

### Reads
*Point-Lookups*

The reader looks up keys in the following order, and terminates the search at the first item that contains a given key:
memtable
immutable memtable
L1 SSTs, in order
SRs, in order

*Range Scans*

To serve Range Scans, the reader needs to look through every memtable, immutable memtable, SST, and SR and sort-merge the key-values that fall within the range.

### Looking Ahead

#### Advanced Compaction Policies

TODO

##### Resuming Compaction

TODO

##### Tiered+Leveled

TODO

##### Time-Series

TODO

## Appendix

### Network Bandwidth vs Disk Bandwidth on AWS

| Instance Type | Network Bandwidth MBps (full-duplex) (Baseline/Burst) | Instance Disk Bandwidth MBps (assumes 4K max io size - experimentally verified for xlarges) (Read/Write) | EBS Bandwidth MBps (full-duplex) (Baseline/Burst) |
| m5d.xlarge | 160/1280 | 230/113 | 143.75/593.75 |
| m5d.metal | 3200/3200 | 5468/2656 | 2375/2375 |
| i3en.xlarge | 537/3200 | 332/253 | 144.2/593.75 |
| i3en.metal | 12800/12800 | 7812/6250 | 2375/2375 |
| c5d.xlarge | 160/1280 | 156/70 | 143.75/593.75 |
| c5d.metal | 12800/12800 | 5468/2656 | 2375/2375 |


