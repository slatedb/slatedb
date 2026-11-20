// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

// Run with: cargo bench --features bench-internal --bench block_iterator_v2
// To run only one kind of case, add `-- iterate` or `-- seek`.
//
// The `bench-internal` feature gates `slatedb::block_iterator_v2_benches`.
// Its pub fns `block_iterator_v2_bench` and `block_seek_v2_bench` build the
// block and yield one iteration or one seek as a closure, so this bench does
// not need crate-private items.

use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use slatedb::block_iterator_v2_benches::{
    block_iterator_v2_bench, block_seek_v2_bench, BlockIteratorV2BenchConfig, SeekTarget,
};
use slatedb::IterationOrder;

#[allow(clippy::redundant_closure)]
fn iterate_benchmark(c: &mut Criterion) {
    // Small values: pool of 16B-key/512B-value entries filling a 4KB block.
    block_iterator_v2_bench(
        BlockIteratorV2BenchConfig {
            block_size: 4096,
            key_size: 16,
            value_size: 512,
            num_entries: 32,
        },
        |inner| {
            c.bench_function("block_iterator_v2_iterate_small_values", |b| {
                b.iter(|| inner());
            });
        },
    );

    // Single 100KB value in a nominally 4KB block. The builder's
    // oversized-entry rule accepts the first entry unconditionally, so the
    // block data expands to ~100KB with exactly one row.
    block_iterator_v2_bench(
        BlockIteratorV2BenchConfig {
            block_size: 4096,
            key_size: 16,
            value_size: 100 * 1024,
            num_entries: 1,
        },
        |inner| {
            c.bench_function("block_iterator_v2_iterate_large_value", |b| {
                b.iter(|| inner());
            });
        },
    );
}

/// Measures only the seek inside one V2 data block.
///
/// The block and the iterator are built one time, outside the timed closure.
/// Each iteration seeks to the next target in a fixed random order and does
/// not call `next`.
#[allow(clippy::redundant_closure)]
fn seek_benchmark(c: &mut Criterion) {
    // (name, value size). With 16-byte keys in a 4 KiB block and the default
    // restart interval of 16, 64-byte values give 44 rows in 3 restart
    // regions, and 16-byte values give 94 rows in 6 restart regions.
    let shapes = [("values64", 64), ("values16", 16)];
    let orders = [
        ("asc", IterationOrder::Ascending),
        ("desc", IterationOrder::Descending),
    ];
    let targets = [
        ("present", SeekTarget::Present),
        ("absent", SeekTarget::Absent),
    ];

    let mut group = c.benchmark_group("block_iterator_v2_seek");
    for (shape, value_size) in shapes {
        for (order_name, order) in orders {
            for (target_name, target) in targets {
                block_seek_v2_bench(
                    BlockIteratorV2BenchConfig {
                        block_size: 4096,
                        key_size: 16,
                        value_size,
                        num_entries: 1_000,
                    },
                    order,
                    target,
                    |inner| {
                        group.bench_function(format!("{shape}/{order_name}/{target_name}"), |b| {
                            b.iter(|| inner())
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(1_000)
        // This only runs when `--profile-time <num_seconds>` is set
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = iterate_benchmark, seek_benchmark
}

criterion_main!(benches);
