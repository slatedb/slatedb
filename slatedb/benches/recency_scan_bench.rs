// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

//! Measures `scan_prefix_by_recency` when every block is already in the block
//! cache, so the cost comes from work on the CPU, not from I/O.
//!
//! The database has the active memtable, 4 immutable memtables, 8 L0 SSTs,
//! and 4 sorted runs. Every SST holds keys at both ends of the key space, so
//! only the prefix bloom filter can rule an SST out. The recency walk visits
//! the memtables from newest to oldest, then the L0 SSTs from newest to
//! oldest, then the sorted runs from newest to oldest. A fail point pauses the
//! upload of immutable memtables to L0, so they stay in memory.
//!
//! The `first_entry` cases read the first entry of a prefix whose keys are in:
//!
//! - `in_memtable`: the active memtable only, so the walk stops at the first
//!   source.
//! - `in_newest_l0`: the newest L0 SST only, so the walk passes every memtable
//!   first.
//! - `in_sorted_run`: the oldest sorted run only, so the walk reaches every
//!   source.
//! - `absent`: no source, so the walk reaches every source and finds nothing.
//!
//! The `all_entries/everywhere` case reads every entry of a prefix whose keys
//! are in every source, so the walk builds every source and can skip none.
//!
//! Run: cargo bench -p slatedb --bench recency_scan_bench

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;
use criterion::{criterion_group, criterion_main, Criterion};
use fail_parallel::FailPointRegistry;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use pprof::criterion::{Output, PProfProfiler};
use slatedb::config::{CompactorOptions, FlushOptions, FlushType, ScanOptions, Settings};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::SplitCache;
use slatedb::{BloomFilterPolicy, Db, FilterPolicy, PrefixExtractor, PrefixTarget};
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use tokio::runtime::Runtime;

const PATH: &str = "/bench/recency";
const PREFIX_LEN: usize = 6;
const PREFIXES_PER_GROUP: usize = 100;
const KEYS_PER_PREFIX: usize = 4;
/// SSTs flushed before each compaction that builds a sorted run.
const SORTED_RUN_SOURCES: usize = 4;
const SORTED_RUNS: usize = 4;
const L0_SSTS: usize = 8;
const IMM_MEMTABLES: usize = 4;
/// A memtable larger than this is frozen into an immutable memtable. Every
/// group of keys is smaller, so only an `IMM_VALUE_BYTES` value freezes one.
const L0_SST_SIZE_BYTES: usize = 256 * 1024;
const IMM_VALUE_BYTES: usize = 300 * 1024;
/// Pausing this fail point keeps immutable memtables from being uploaded.
const L0_UPLOAD_FAIL_POINT: &str = "write-compacted-sst-io-error";

/// Each group of prefixes has its keys in one place. A prefix is
/// `pk` followed by four digits, and each group starts at a multiple of 1000.
#[derive(Clone, Copy)]
enum Group {
    /// The first and last prefixes, written to every SST so that every SST
    /// covers the whole key space.
    Sentinel,
    /// Written to every SST, so no SST is empty apart from its sentinels.
    Filler,
    SortedRun,
    NewestL0,
    Memtable,
    Absent,
    Everywhere,
    /// One key per immutable memtable, with a value large enough to freeze it.
    ImmMemtable,
}

impl Group {
    fn first_prefix(self) -> usize {
        match self {
            Group::Sentinel => 0,
            Group::SortedRun => 1000,
            Group::Filler => 2000,
            Group::NewestL0 => 3000,
            Group::Memtable => 4000,
            Group::Absent => 5000,
            Group::Everywhere => 6000,
            Group::ImmMemtable => 7000,
        }
    }

    fn prefixes(self) -> Vec<usize> {
        match self {
            Group::Sentinel => vec![0, 9999],
            _ => (0..PREFIXES_PER_GROUP)
                .map(|i| self.first_prefix() + i)
                .collect(),
        }
    }
}

struct FixedPrefixExtractor;

impl PrefixExtractor for FixedPrefixExtractor {
    fn name(&self) -> &str {
        "fixed6"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let input = match target {
            PrefixTarget::Point(k) => k.as_ref(),
            PrefixTarget::Prefix(p) => p.as_ref(),
        };
        (input.len() >= PREFIX_LEN).then_some(PREFIX_LEN)
    }
}

fn prefix(prefix_idx: usize) -> Vec<u8> {
    format!("pk{:04}", prefix_idx).into_bytes()
}

fn key(prefix_idx: usize, key_idx: usize) -> Vec<u8> {
    format!("pk{:04}:{:04}", prefix_idx, key_idx).into_bytes()
}

fn filter_policies() -> Vec<Arc<dyn FilterPolicy>> {
    vec![Arc::new(
        BloomFilterPolicy::new(10).with_prefix_extractor(Arc::new(FixedPrefixExtractor)),
    )]
}

fn cache() -> Arc<SplitCache> {
    let cache = |capacity| {
        Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: capacity,
            ..Default::default()
        }))
    };
    Arc::new(
        SplitCache::new()
            .with_block_cache(Some(cache(256 * 1024 * 1024)))
            .with_meta_cache(Some(cache(64 * 1024 * 1024)))
            .build(),
    )
}

/// With `compactor` set, each compaction merges `SORTED_RUN_SOURCES` L0 SSTs
/// into a new sorted run. Without it, later SSTs stay in L0.
fn settings(compactor: bool) -> Settings {
    let compactor_options = compactor.then(|| {
        let mut scheduler_options = HashMap::new();
        let sources = SORTED_RUN_SOURCES.to_string();
        scheduler_options.insert("min_compaction_sources".to_string(), sources.clone());
        scheduler_options.insert("max_compaction_sources".to_string(), sources);
        // Runs are merged only when the largest is at most this many times
        // the smallest, which a value below 1 never allows.
        scheduler_options.insert("include_size_threshold".to_string(), "0.5".to_string());
        CompactorOptions {
            poll_interval: Duration::from_millis(50),
            max_concurrent_compactions: 1,
            scheduler_options,
            ..CompactorOptions::default()
        }
    });
    Settings {
        // Every SST is small, so build filters for all of them.
        min_filter_keys: 0,
        l0_sst_size_bytes: L0_SST_SIZE_BYTES,
        l0_max_ssts: L0_SSTS + 8,
        l0_max_ssts_per_key: L0_SSTS + 8,
        compactor_options,
        ..Settings::default()
    }
}

fn scan_options() -> ScanOptions {
    ScanOptions {
        cache_blocks: true,
        ..ScanOptions::default()
    }
}

async fn open(
    store: Arc<dyn ObjectStore>,
    compactor: bool,
    fp_registry: Arc<FailPointRegistry>,
) -> Db {
    Db::builder(PATH, store)
        .with_settings(settings(compactor))
        .with_db_cache(cache(), 0)
        .with_filter_policies(filter_policies())
        .with_fp_registry(fp_registry)
        .build()
        .await
        .expect("failed to build db")
}

async fn put_groups(db: &Db, groups: &[Group]) {
    for group in groups {
        for prefix_idx in group.prefixes() {
            for key_idx in 0..KEYS_PER_PREFIX {
                let key = key(prefix_idx, key_idx);
                db.put(&key, &key).await.expect("put failed");
            }
        }
    }
}

async fn flush(db: &Db) {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("flush failed");
}

async fn await_sorted_runs(db: &Db, runs: usize) {
    let clock = DefaultSystemClock::new();
    let deadline = clock.now() + TimeDelta::seconds(300);
    loop {
        db.refresh_manifest()
            .await
            .expect("refresh_manifest failed");
        let manifest = db.manifest();
        if manifest.l0().is_empty() && manifest.compacted().len() == runs {
            return;
        }
        assert!(
            clock.now() < deadline,
            "compaction never produced {runs} sorted runs (l0={}, runs={})",
            manifest.l0().len(),
            manifest.compacted().len()
        );
        clock.sleep(Duration::from_millis(50)).await;
    }
}

/// Reads the entries of `prefix_idx` by recency, only the first one unless
/// `all` is set, and returns how many it read.
async fn read_prefix(db: &Db, prefix_idx: usize, all: bool) -> usize {
    let mut iter = db
        .scan_prefix_by_recency_with_options(prefix(prefix_idx), &scan_options())
        .await
        .expect("scan_prefix_by_recency failed");
    let mut entries = 0;
    while iter
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .is_some()
    {
        entries += 1;
        if !all {
            break;
        }
    }
    entries
}

/// Builds the sorted runs, then the L0 SSTs, then the immutable memtables,
/// then the active memtable, and reads every case once so that all blocks are
/// cached.
async fn build_db() -> (Db, Arc<FailPointRegistry>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fp_registry = Arc::new(FailPointRegistry::new());

    let db = open(store.clone(), true, fp_registry.clone()).await;
    for run in 0..SORTED_RUNS {
        for _ in 0..SORTED_RUN_SOURCES {
            put_groups(&db, &[Group::Sentinel, Group::Filler, Group::Everywhere]).await;
            if run == 0 {
                put_groups(&db, &[Group::SortedRun]).await;
            }
            flush(&db).await;
        }
        await_sorted_runs(&db, run + 1).await;
    }
    db.close().await.expect("close failed");

    let db = open(store, false, fp_registry.clone()).await;
    for i in 0..L0_SSTS {
        put_groups(&db, &[Group::Sentinel, Group::Filler, Group::Everywhere]).await;
        if i == L0_SSTS - 1 {
            put_groups(&db, &[Group::NewestL0]).await;
        }
        flush(&db).await;
    }
    fail_parallel::cfg(fp_registry.clone(), L0_UPLOAD_FAIL_POINT, "pause")
        .expect("failed to pause L0 upload");
    let value = vec![b'x'; IMM_VALUE_BYTES];
    for i in 0..IMM_MEMTABLES {
        put_groups(&db, &[Group::Everywhere]).await;
        // Freezes the memtable that holds the keys above.
        let key = key(Group::ImmMemtable.first_prefix() + i, 0);
        db.put(&key, &value).await.expect("put failed");
    }
    put_groups(&db, &[Group::Memtable, Group::Everywhere]).await;

    let manifest = db.manifest();
    assert_eq!(manifest.l0().len(), L0_SSTS);
    assert_eq!(manifest.compacted().len(), SORTED_RUNS);
    for (_, prefix_group, all, expected) in CASES {
        for prefix_idx in prefix_group.prefixes() {
            assert_eq!(read_prefix(&db, prefix_idx, all).await, expected);
        }
    }
    (db, fp_registry)
}

/// Each case is a name, the group of prefixes it reads, whether it reads every
/// entry, and the number of entries it reads.
const CASES: [(&str, Group, bool, usize); 5] = [
    ("first_entry/in_memtable", Group::Memtable, false, 1),
    ("first_entry/in_newest_l0", Group::NewestL0, false, 1),
    ("first_entry/in_sorted_run", Group::SortedRun, false, 1),
    ("first_entry/absent", Group::Absent, false, 0),
    // Each key has one version in each memtable, each L0 SST, and each run.
    (
        "all_entries/everywhere",
        Group::Everywhere,
        true,
        KEYS_PER_PREFIX * (1 + IMM_MEMTABLES + L0_SSTS + SORTED_RUNS),
    ),
];

fn bench_recency_scan(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create runtime");
    let (db, fp_registry) = runtime.block_on(build_db());
    // Rotates through each group's prefixes, so that iterations read
    // different keys.
    let next = AtomicUsize::new(0);

    let mut group = c.benchmark_group("recency_scan");
    for (name, prefix_group, all, expected) in CASES {
        let first = prefix_group.first_prefix();
        group.bench_function(name, |b| {
            b.to_async(&runtime).iter(|| async {
                let prefix_idx = first + next.fetch_add(1, Ordering::Relaxed) % PREFIXES_PER_GROUP;
                assert_eq!(read_prefix(&db, prefix_idx, all).await, expected);
            });
        });
    }
    group.finish();

    // Let the immutable memtables upload, so that close can flush them.
    fail_parallel::cfg(fp_registry, L0_UPLOAD_FAIL_POINT, "off")
        .expect("failed to resume L0 upload");
    runtime.block_on(async { db.close().await.expect("close failed") });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_recency_scan
}

criterion_main!(benches);
