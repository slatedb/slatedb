use bytes::{BufMut, Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use slatedb::filter::{filter_hash, BloomFilterBuilder};
fn make_key(key_size: usize, element: u32) -> Bytes {
    // Pre-generate the keys
    let mut bytes = BytesMut::with_capacity(key_size);
    bytes.put_u32(element);
    bytes.clone().freeze();
    bytes.into()
}
fn bloom_filter_insert_benchmark(c: &mut Criterion) {
    let mut builder = BloomFilterBuilder::new(10); // Set bits_per_key
    let key_sz = std::mem::size_of::<u32>();
    let bytes = make_key(key_sz, 1u32);
    c.bench_function("bloom_filter insert 10000000 keys", |b| {
        b.iter(|| {
            builder.add_key(black_box(bytes.as_ref()));
        })
    });
}
fn bloom_filter_build_benchmark(c: &mut Criterion) {
    let mut builder = BloomFilterBuilder::new(10);
    let key_sz = std::mem::size_of::<u32>();

    let bytes = make_key(key_sz, 1u32);
    builder.add_key(black_box(bytes.as_ref()));
    c.bench_function("bloom_filter build", |b| {
        b.iter(|| {
            black_box(builder.build());
        })
    });
}
fn bloom_filter_query_benchmark(c: &mut Criterion) {
    let mut builder = BloomFilterBuilder::new(10);
    let key_sz = std::mem::size_of::<u32>();

    let bytes = make_key(key_sz, 1u32);
    builder.add_key(black_box(bytes.as_ref()));
    let filter = builder.build();

    c.bench_function("bloom_filter query", |b| {
        b.iter(|| {
            let hash = filter_hash(bytes.as_ref());
            black_box(filter.might_contain(hash));
        })
    });
}

fn bloom_build_and_insert_benchmark(c: &mut Criterion) {
    let mut builder = BloomFilterBuilder::new(10); // Set bits_per_key
    let key_sz = std::mem::size_of::<u32>();
    let bytes = make_key(key_sz, 1u32);
    c.bench_function("bloom_filter build and insert", |b| {
        b.iter(|| {
            builder.add_key(black_box(bytes.as_ref()));
            black_box(builder.build());
        })
    });
}
criterion_group!(
    bloom_benchmarks,
    bloom_filter_insert_benchmark,
    bloom_filter_build_benchmark,
    bloom_filter_query_benchmark,
    bloom_build_and_insert_benchmark
);
criterion_main!(bloom_benchmarks);
