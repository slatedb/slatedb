use criterion::{black_box, criterion_group, criterion_main, Criterion};

use std::sync::Arc;

use object_store::{memory::InMemory, path::Path};
use slatedb::{config::WriteOptions, db::Db};

fn bench_non_durable_puts(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Init DB
    let object_store = Arc::new(InMemory::new());
    let path = Path::from("/benchmark_db");
    let db = runtime.block_on(Db::open(path, object_store)).unwrap();
    let write_opts = WriteOptions {
        await_durable: false,
    };

    // Init keys and values
    let num_operations = 10000;
    let keys: Vec<Vec<u8>> = (0..num_operations)
        .map(|i| format!("key{}", i).into_bytes())
        .collect();
    let value = black_box(vec![b'a'; 4096]);

    c.bench_function("bench_non_durable_puts", |b| {
        b.to_async(&runtime).iter(|| async {
            let futures = keys
                .iter()
                .map(|key| db.put_with_options(key, &value, &write_opts));
            futures::future::join_all(futures).await;
        })
    });

    runtime.block_on(db.close()).unwrap();
}

criterion_group!(benches, bench_non_durable_puts);
criterion_main!(benches);
