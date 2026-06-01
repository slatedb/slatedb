// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use object_store::memory::InMemory;
use pprof::criterion::{Output, PProfProfiler};
use slatedb::config::{PutOptions, Settings, WriteOptions};
use slatedb::Db;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let object_store = Arc::new(InMemory::new());

    let runtime = Runtime::new().expect("Failed to create Tokio runtime");
    let db = Db::open("/tmp/test_kv_store", object_store);
    #[allow(clippy::disallowed_methods)]
    let db = runtime.block_on(db).unwrap();
    c.bench_function("put", |b| {
        b.to_async(&runtime).iter(|| async {
            let key = b"key";
            let value = b"value";
            db.put_with_options(
                key,
                value,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .expect("put failed");
        })
    });
    c.bench_function("open_close", |b| {
        b.to_async(&runtime).iter(|| async {
            let db = Db::open("/tmp/test_kv_store", Arc::new(InMemory::new()))
                .await
                .expect("open failed");
            db.close().await.expect("close failed");
        })
    });

    bench_batch_reads(c, &runtime);
}

/// Compare a single batched `multi_get` against an equivalent loop of `get`s
/// over a database whose keys span many L0 SSTs with no block cache — the case
/// where multi_get's per-SST batching (load index/filter once, coalesce block
/// reads) wins most.
fn bench_batch_reads(c: &mut Criterion, runtime: &Runtime) {
    let object_store = Arc::new(InMemory::new());
    let settings = Settings {
        l0_sst_size_bytes: 4096,
        min_filter_keys: 0,
        ..Default::default()
    };
    let db = runtime.block_on(async {
        let db = Db::builder("/tmp/bench_multi_get", object_store)
            .with_settings(settings)
            .build()
            .await
            .expect("open failed");
        for i in 0..4000u32 {
            let key = format!("key{i:06}");
            let value = format!("value{i:06}");
            db.put_with_options(
                key.as_bytes(),
                value.as_bytes(),
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .expect("put failed");
            if i % 250 == 249 {
                db.flush().await.expect("flush failed");
            }
        }
        db.flush().await.expect("flush failed");
        db
    });

    // 32 keys spread across the key space (so they land in many distinct SSTs).
    let keys: Vec<Vec<u8>> = (0..1000u32)
        .map(|i| format!("key{:06}", i * 4).into_bytes())
        .collect();

    c.bench_function("multi_get_1k", |b| {
        b.to_async(runtime).iter(|| async {
            black_box(db.multi_get(&keys).await.expect("multi_get failed"));
        })
    });
    c.bench_function("get_loop_1k", |b| {
        b.to_async(runtime).iter(|| async {
            let mut out = Vec::with_capacity(keys.len());
            for key in &keys {
                out.push(db.get(key).await.expect("get failed"));
            }
            black_box(out);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(1_000)
        // This only runs when `--profile-time <num_seconds>` is set
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = criterion_benchmark
}

criterion_main!(benches);
