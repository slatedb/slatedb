// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

//! Measures `scan_prefix_by_recency` when every block is already in the block
//! cache, so the cost comes from work on the CPU, not from I/O.
//!
//! The database has the active memtable, 8 L0 SSTs, and one sorted run. Every
//! SST holds keys at both ends of the key space, so only the prefix bloom
//! filter can rule an SST out. The recency walk visits the memtable, then the
//! L0 SSTs from newest to oldest, then the sorted run.
//!
//! Each case reads the first entry of a prefix whose only keys are in one
//! place:
//!
//! - `in_memtable`: the walk stops at the first source.
//! - `in_newest_l0`: the walk stops at the second source.
//! - `in_sorted_run`: the walk reaches every source.
//! - `absent`: the prefix has no keys, so the walk reaches every source and
//!   finds nothing.
//!
//! Run: cargo bench -p slatedb --bench recency_scan_bench

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
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
/// SSTs flushed before the compaction that builds the sorted run.
const SORTED_RUN_SOURCES: usize = 4;
const L0_SSTS: usize = 8;

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

/// With `compactor` set, one compaction merges the first SSTs into a sorted
/// run. Without it, later SSTs stay in L0.
fn settings(compactor: bool) -> Settings {
    let compactor_options = compactor.then(|| {
        let mut scheduler_options = HashMap::new();
        let sources = SORTED_RUN_SOURCES.to_string();
        scheduler_options.insert("min_compaction_sources".to_string(), sources.clone());
        scheduler_options.insert("max_compaction_sources".to_string(), sources);
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

async fn open(store: Arc<dyn ObjectStore>, compactor: bool) -> Db {
    Db::builder(PATH, store)
        .with_settings(settings(compactor))
        .with_db_cache(cache(), 0)
        .with_filter_policies(filter_policies())
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

async fn await_one_sorted_run(db: &Db) {
    let clock = DefaultSystemClock::new();
    let deadline = clock.now() + TimeDelta::seconds(300);
    loop {
        db.refresh_manifest()
            .await
            .expect("refresh_manifest failed");
        let manifest = db.manifest();
        if manifest.l0().is_empty() && manifest.compacted().len() == 1 {
            return;
        }
        assert!(
            clock.now() < deadline,
            "compaction never produced one sorted run (l0={}, runs={})",
            manifest.l0().len(),
            manifest.compacted().len()
        );
        clock.sleep(Duration::from_millis(50)).await;
    }
}

/// Reads the first entry of `prefix_idx` by recency.
async fn first_entry(db: &Db, prefix_idx: usize) -> bool {
    let mut iter = db
        .scan_prefix_by_recency_with_options(prefix(prefix_idx), &scan_options())
        .await
        .expect("scan_prefix_by_recency failed");
    iter.next_entry()
        .await
        .expect("iterator next_entry failed")
        .is_some()
}

/// Builds the sorted run, then the L0 SSTs, then the memtable keys, and reads
/// every case once so that all blocks are cached.
async fn build_db() -> Db {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let db = open(store.clone(), true).await;
    for _ in 0..SORTED_RUN_SOURCES {
        put_groups(&db, &[Group::Sentinel, Group::Filler, Group::SortedRun]).await;
        flush(&db).await;
    }
    await_one_sorted_run(&db).await;
    db.close().await.expect("close failed");

    let db = open(store, false).await;
    for i in 0..L0_SSTS {
        put_groups(&db, &[Group::Sentinel, Group::Filler]).await;
        if i == L0_SSTS - 1 {
            put_groups(&db, &[Group::NewestL0]).await;
        }
        flush(&db).await;
    }
    put_groups(&db, &[Group::Memtable]).await;

    let manifest = db.manifest();
    assert_eq!(manifest.l0().len(), L0_SSTS);
    assert_eq!(manifest.compacted().len(), 1);
    for (group, found) in [
        (Group::Memtable, true),
        (Group::NewestL0, true),
        (Group::SortedRun, true),
        (Group::Absent, false),
    ] {
        for prefix_idx in group.prefixes() {
            assert_eq!(first_entry(&db, prefix_idx).await, found);
        }
    }
    db
}

fn bench_recency_scan(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create runtime");
    let db = runtime.block_on(build_db());
    // Rotates through each group's prefixes, so that iterations read
    // different keys.
    let next = AtomicUsize::new(0);

    let mut group = c.benchmark_group("recency_scan");
    for (label, prefix_group, found) in [
        ("in_memtable", Group::Memtable, true),
        ("in_newest_l0", Group::NewestL0, true),
        ("in_sorted_run", Group::SortedRun, true),
        ("absent", Group::Absent, false),
    ] {
        let first = prefix_group.first_prefix();
        group.bench_function(BenchmarkId::new("first_entry", label), |b| {
            b.to_async(&runtime).iter(|| async {
                let prefix_idx = first + next.fetch_add(1, Ordering::Relaxed) % PREFIXES_PER_GROUP;
                assert_eq!(first_entry(&db, prefix_idx).await, found);
            });
        });
    }
    group.finish();

    runtime.block_on(async { db.close().await.expect("close failed") });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_recency_scan
}

criterion_main!(benches);
