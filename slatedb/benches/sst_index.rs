// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use slatedb::sst_index_bench::PreparedSst;
use tokio::runtime::Runtime;

/// Number of completed data blocks to generate for each benchmark variant.
/// - small: exercises a small index (~10 BlockMeta entries; 1 partition)
/// - medium: exercises a medium index (~100 BlockMeta entries; a few partitions)
/// - large: exercises a large index (~1 000 BlockMeta entries; many partitions)
const BLOCK_COUNTS: &[usize] = &[10, 100, 1_000];

fn bench_sst_index(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create Tokio runtime");

    let mut group = c.benchmark_group("read_index");

    for &num_blocks in BLOCK_COUNTS {
        // Build both SST variants once outside the measurement loop so that
        // the benchmark only measures the index decode, not SST construction.
        let flat = runtime.block_on(PreparedSst::build_flat(num_blocks));
        let partitioned = runtime.block_on(PreparedSst::build_partitioned(num_blocks));

        group.bench_with_input(
            BenchmarkId::new("flat", num_blocks),
            &num_blocks,
            |b, _| b.to_async(&runtime).iter(|| flat.read_index()),
        );

        group.bench_with_input(
            BenchmarkId::new("partitioned", num_blocks),
            &num_blocks,
            |b, _| b.to_async(&runtime).iter(|| partitioned.read_index()),
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(200)
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_sst_index
}

criterion_main!(benches);
