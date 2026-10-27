// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

//! Measures reads when every SST block is already in the block cache.
//!
//! The data has 1,000 prefixes with 10 keys each and 100-byte values. Each
//! bench runs against two databases: one with a single L0 SST, and one with
//! 8 L0 SSTs. Every SST holds every key, so a scan reads from every SST.
//!
//! - `scan_10_keys` scans one prefix.
//! - `scan_1000_keys` scans 100 prefixes, which covers several blocks in each
//!   SST.
//! - `get` reads one key. It is a baseline to compare against, because a
//!   point read does not spawn a task for its block fetch.
//!
//! Scans use the default `ScanOptions`, except that they keep the blocks they
//! read in the cache.
//!
//! Run: cargo bench -p slatedb --bench sst_block_fetch_bench

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use object_store::memory::InMemory;
use pprof::criterion::{Output, PProfProfiler};
use slatedb::config::{FlushOptions, FlushType, PutOptions, ScanOptions, Settings, WriteOptions};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::SplitCache;
use slatedb::Db;
use tokio::runtime::Runtime;

const NUM_PREFIXES: usize = 1_000;
const KEYS_PER_PREFIX: usize = 10;
const VALUE_LEN: usize = 100;

fn prefix(prefix_idx: usize) -> Vec<u8> {
    format!("p{:04}:", prefix_idx).into_bytes()
}

fn key(prefix_idx: usize, key_idx: usize) -> Vec<u8> {
    format!("p{:04}:{:04}", prefix_idx, key_idx).into_bytes()
}

fn settings(num_ssts: usize) -> Settings {
    Settings {
        l0_max_ssts: num_ssts + 10,
        l0_max_ssts_per_key: num_ssts + 10,
        compactor_options: None,
        ..Settings::default()
    }
}

fn block_cache() -> Arc<SplitCache> {
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

fn scan_options() -> ScanOptions {
    ScanOptions {
        cache_blocks: true,
        ..ScanOptions::default()
    }
}

/// Builds a DB with `num_ssts` L0 SSTs that each hold every key, then reads
/// every block once so that all of them are cached.
async fn build_db(path: &str, num_ssts: usize) -> Db {
    let db = Db::builder(path, Arc::new(InMemory::new()))
        .with_settings(settings(num_ssts))
        .with_db_cache(block_cache(), 0)
        .build()
        .await
        .expect("failed to build db");
    let value = vec![b'v'; VALUE_LEN];
    for _ in 0..num_ssts {
        for prefix_idx in 0..NUM_PREFIXES {
            for key_idx in 0..KEYS_PER_PREFIX {
                db.put_with_options(
                    key(prefix_idx, key_idx),
                    &value,
                    &PutOptions::default(),
                    &WriteOptions::default(),
                )
                .await
                .expect("put failed");
            }
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("flush failed");
    }
    let mut iter = db
        .scan_with_options(.., &scan_options())
        .await
        .expect("scan failed");
    let mut count = 0;
    while iter.next().await.expect("next failed").is_some() {
        count += 1;
    }
    assert_eq!(count, NUM_PREFIXES * KEYS_PER_PREFIX);
    db
}

/// Scans the keys of `num_prefixes` prefixes from `first`, and returns how
/// many rows it read.
async fn scan(db: &Db, first: usize, num_prefixes: usize) -> usize {
    let last = (first + num_prefixes).min(NUM_PREFIXES);
    let end = if last == NUM_PREFIXES {
        b"q".to_vec()
    } else {
        prefix(last)
    };
    let mut iter = db
        .scan_with_options(prefix(first)..end, &scan_options())
        .await
        .expect("scan failed");
    let mut count = 0;
    while iter.next().await.expect("next failed").is_some() {
        count += 1;
    }
    count
}

fn bench_sst_block_fetch(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create runtime");
    let one_sst = runtime.block_on(build_db("/bench/one_sst", 1));
    let eight_ssts = runtime.block_on(build_db("/bench/eight_ssts", 8));
    // Rotates through the prefixes, so that iterations read different blocks.
    let next_prefix = AtomicUsize::new(0);
    let next = |step: usize| next_prefix.fetch_add(step, Ordering::Relaxed) % NUM_PREFIXES;

    let mut group = c.benchmark_group("sst_block_fetch");
    for (label, db) in [("1_sst", &one_sst), ("8_ssts", &eight_ssts)] {
        group.bench_function(BenchmarkId::new("get", label), |b| {
            b.to_async(&runtime).iter(|| async {
                let value = db.get(key(next(1), 0)).await.expect("get failed");
                assert!(value.is_some());
            });
        });
        group.bench_function(BenchmarkId::new("scan_10_keys", label), |b| {
            b.to_async(&runtime).iter(|| async {
                assert_eq!(scan(db, next(1), 1).await, KEYS_PER_PREFIX);
            });
        });
        group.bench_function(BenchmarkId::new("scan_1000_keys", label), |b| {
            b.to_async(&runtime).iter(|| async {
                let first = next(100) / 100 * 100;
                assert_eq!(scan(db, first, 100).await, 100 * KEYS_PER_PREFIX);
            });
        });
    }

    group.finish();

    runtime.block_on(async {
        one_sst.close().await.expect("close failed");
        eight_ssts.close().await.expect("close failed");
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_sst_block_fetch
}

criterion_main!(benches);
