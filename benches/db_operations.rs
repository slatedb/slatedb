use criterion::{criterion_group, criterion_main, Criterion};
use object_store::{memory::InMemory, path::Path};
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{config::DbOptions, db::Db};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let object_store = Arc::new(InMemory::new());
    let options = DbOptions::default();

    let runtime = Runtime::new().expect("Failed to create Tokio runtime");
    let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store);
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
                },
            )
            .await
            .expect("put failed");
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(1_000);
    targets = criterion_benchmark
}

criterion_main!(benches);
