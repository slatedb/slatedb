use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use slatedb::filter::{filter_hash, BloomFilterBuilder};
fn bloom_filter_insert_benchmark(c: &mut Criterion) {
    let mut builder = BloomFilterBuilder::new(10); // Set bits_per_key
    let key_sz = std::mem::size_of::<u32>();

    c.bench_function("bloom_filter insert 10000000 keys", |b| {
        b.iter(|| {
            for i in 0..10000000 {
                let mut bytes = BytesMut::with_capacity(key_sz);
                bytes.put_u32(i);
                builder.add_key(black_box(bytes.freeze().as_ref())); // Use `black_box` to prevent compiler optimizations
            }
        })
    });
}
fn bloom_filter_build_benchmark(c: &mut Criterion) {
    let mut builder = BloomFilterBuilder::new(10);
    let key_sz = std::mem::size_of::<u32>();

    // Add keys to builder before measuring build time
    for i in 0..100000 {
        let mut bytes = BytesMut::with_capacity(key_sz);
        bytes.put_u32(i);
        builder.add_key(bytes.freeze().as_ref());
    }

    c.bench_function("bloom_filter build", |b| {
        b.iter(|| {
            let _filter = black_box(builder.build());
        })
    });
}
fn bloom_filter_query_benchmark(c: &mut Criterion) {
    let mut builder = BloomFilterBuilder::new(10);
    let key_sz = std::mem::size_of::<u32>();

    for i in 0..100000 {
        let mut bytes = BytesMut::with_capacity(key_sz);
        bytes.put_u32(i);
        builder.add_key(bytes.freeze().as_ref());
    }
    let filter = builder.build();

    c.bench_function("bloom_filter query", |b| {
        b.iter(|| {
            for i in 0..100000 {
                let mut bytes = BytesMut::with_capacity(key_sz);
                bytes.put_u32(i);
                let hash = filter_hash(bytes.freeze().as_ref());
                black_box(filter.might_contain(hash));
            }
        })
    });
}

criterion_group!(
    bloom_benchmarks,
    bloom_filter_insert_benchmark,
    // bloom_filter_build_benchmark,
    // bloom_filter_query_benchmark
);
criterion_main!(bloom_benchmarks);
