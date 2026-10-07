// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

//! Measures the cost of `DbIterator::seek` when every key is in the memtable.
//!
//! The `memtable_seek_by_size` group changes the number of keys in the
//! memtable. Each iteration opens a full-range scan and seeks to a key near the
//! end.
//!
//! The `memtable_seek_by_distance` group keeps the memtable size fixed and
//! changes the seek distance. The seek distance is the number of keys between
//! the start of the scan and the seek target. Each distance is 10 times the
//! distance before it.
//!
//! Run with: cargo bench -p slatedb --bench memtable_seek

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use object_store::memory::InMemory;
use pprof::criterion::{Output, PProfProfiler};
use slatedb::config::Settings;
use slatedb::{ByteRangeBounds, Db, WriteBatch};
use tokio::runtime::Runtime;

const SIZES: [usize; 4] = [10_000, 50_000, 100_000, 200_000];
/// Number of keys in the memtable for the seek distance group.
const DISTANCE_GROUP_SIZE: usize = 200_000;
const DISTANCES: [usize; 6] = [1, 10, 100, 1_000, 10_000, 100_000];
/// Each case cycles through this many targets.
const TARGETS_PER_CASE: usize = 100;
const BATCH_SIZE: usize = 10_000;

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:010}").into_bytes()
}

/// Keeps every write in the memtable, so no read touches an SST.
fn memtable_only_settings() -> Settings {
    Settings {
        l0_sst_size_bytes: 1 << 30,
        max_unflushed_bytes: 4 << 30,
        max_wal_flushes_before_l0_flush: u64::MAX,
        compactor_options: None,
        ..Settings::default()
    }
}

async fn load(n: usize) -> Db {
    let db = Db::builder(format!("/memtable_seek/{n}"), Arc::new(InMemory::new()))
        .with_settings(memtable_only_settings())
        .build()
        .await
        .expect("failed to build db");
    for start in (0..n).step_by(BATCH_SIZE) {
        let mut batch = WriteBatch::new();
        for i in start..(start + BATCH_SIZE).min(n) {
            batch.put(key(i), b"value");
        }
        db.write(batch).await.expect("write failed");
    }
    db
}

/// Opens a scan over `range`, seeks to `target`, and makes sure that the next
/// key is `target`.
async fn scan_then_seek<T: ByteRangeBounds + Send>(db: &Db, range: T, target: &[u8]) {
    let mut iter = db.scan(range).await.expect("scan failed");
    iter.seek(target).await.expect("seek failed");
    let kv = iter
        .next()
        .await
        .expect("iterator next failed")
        .expect("target key exists");
    assert_eq!(kv.key.as_ref(), target);
}

fn bench_seek_by_size(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create runtime");
    let mut group = c.benchmark_group("memtable_seek_by_size");

    for n in SIZES {
        let db = runtime.block_on(load(n));
        // The targets are the last keys, so each seek skips almost all of the
        // memtable.
        let target = |round: usize| key(n - 1 - round % TARGETS_PER_CASE);

        group.bench_function(BenchmarkId::new("scan_all_then_seek", n), |b| {
            let mut round = 0;
            b.to_async(&runtime).iter(|| {
                let target = target(round);
                round += 1;
                let db = &db;
                async move { scan_then_seek(db, .., &target).await }
            });
        });

        runtime.block_on(async { db.close().await.expect("close failed") });
    }

    group.finish();
}

fn bench_seek_by_distance(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create runtime");
    let db = runtime.block_on(load(DISTANCE_GROUP_SIZE));
    let mut group = c.benchmark_group("memtable_seek_by_distance");

    for distance in DISTANCES {
        group.bench_function(BenchmarkId::new("scan_then_seek", distance), |b| {
            let mut round = 0;
            b.to_async(&runtime).iter(|| {
                let start = round % TARGETS_PER_CASE;
                round += 1;
                let db = &db;
                async move { scan_then_seek(db, key(start).., &key(start + distance)).await }
            });
        });
    }

    group.finish();
    runtime.block_on(async { db.close().await.expect("close failed") });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        // This only runs when `--profile-time <num_seconds>` is set
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_seek_by_size, bench_seek_by_distance
}

criterion_main!(benches);
