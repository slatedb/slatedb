// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

// Run with: cargo bench -p slatedb --features bench-internal --bench write_batch

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use slatedb::config::{MergeOptions, PutOptions, Ttl};
use slatedb::{write_batch_benches, MergeOperator, MergeOperatorError, WriteBatch};
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

const VALUE_SIZE: usize = 128;
const EXTRACT_ENTRY_COUNT: usize = 1_024;
const EXTRACT_MERGE_KEY_COUNT: usize = 256;
const MERGES_PER_KEY: usize = 3;

struct SizeSumMergeOperator;

impl MergeOperator for SizeSumMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let size = existing_value.as_ref().map_or(0, Bytes::len) + operand.len();
        Ok(Bytes::copy_from_slice(&(size as u64).to_le_bytes()))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let size = existing_value.as_ref().map_or(0, Bytes::len)
            + operands.iter().map(Bytes::len).sum::<usize>();
        Ok(Bytes::copy_from_slice(&(size as u64).to_le_bytes()))
    }
}

fn size_sum_merge_operator() -> Arc<dyn MergeOperator + Send + Sync> {
    static MERGE_OPERATOR: OnceLock<Arc<dyn MergeOperator + Send + Sync>> = OnceLock::new();
    MERGE_OPERATOR
        .get_or_init(|| Arc::new(SizeSumMergeOperator))
        .clone()
}

fn put_options() -> PutOptions {
    PutOptions {
        ttl: Ttl::ExpireAfter(3_600),
    }
}

fn merge_options() -> MergeOptions {
    MergeOptions {
        ttl: Ttl::ExpireAfter(3_600),
    }
}

fn key(index: usize) -> Bytes {
    Bytes::from(format!("key-{index:08}"))
}

fn value(index: usize) -> Bytes {
    let mut value = vec![0; VALUE_SIZE];
    value[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
    Bytes::from(value)
}

fn make_batch_without_merges() -> WriteBatch {
    let mut batch = WriteBatch::new();
    let options = put_options();
    for index in 0..EXTRACT_ENTRY_COUNT {
        let key = key(index);
        if index % 4 == 0 {
            batch.delete(key);
        } else {
            batch.put_bytes_with_options(key, value(index), &options);
        }
    }
    batch
}

fn make_batch_with_merges() -> WriteBatch {
    let mut batch = WriteBatch::new();
    let put_options = put_options();
    let merge_options = merge_options();
    for index in 0..EXTRACT_MERGE_KEY_COUNT {
        let key = key(index);
        batch.put_bytes_with_options(key.clone(), value(index), &put_options);
        for merge_index in 0..MERGES_PER_KEY {
            batch.merge_with_options(
                &key,
                value(index * MERGES_PER_KEY + merge_index),
                &merge_options,
            );
        }
    }
    batch
}

fn make_batch_with_repeated_overwrites() -> WriteBatch {
    let mut batch = WriteBatch::new();
    let options = put_options();
    let key = Bytes::from_static(b"repeated-overwrite-key");
    for index in 0..EXTRACT_ENTRY_COUNT {
        batch.put_bytes_with_options(key.clone(), value(index), &options);
    }
    batch
}

fn bench_write_batch(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create runtime");
    let put_options = put_options();
    let merge_options = merge_options();
    let batch_without_merges = make_batch_without_merges();
    let batch_with_merges = make_batch_with_merges();
    let batch_with_repeated_overwrites = make_batch_with_repeated_overwrites();

    let mut group = c.benchmark_group("write_batch");
    group.sample_size(1_000);

    group.bench_function("put_bytes_with_options/empty_batch", |b| {
        b.iter_batched(
            || {
                (
                    WriteBatch::new(),
                    Bytes::from_static(b"write_batch_key"),
                    Bytes::from_static(b"write_batch_value"),
                )
            },
            |(mut batch, key, value)| {
                batch.put_bytes_with_options(key, value, &put_options);
                black_box(batch)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("put_bytes_with_options/new_key_populated", |b| {
        b.iter_batched(
            || {
                (
                    batch_without_merges.clone(),
                    key(EXTRACT_ENTRY_COUNT + 1),
                    value(EXTRACT_ENTRY_COUNT + 1),
                )
            },
            |(mut batch, key, value)| {
                batch.put_bytes_with_options(key, value, &put_options);
                black_box(batch)
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("put_bytes_with_options/overwrite_existing_key", |b| {
        b.iter_batched(
            || {
                (
                    batch_with_merges.clone(),
                    key(0),
                    value(EXTRACT_MERGE_KEY_COUNT + 1),
                )
            },
            |(mut batch, key, value)| {
                batch.put_bytes_with_options(key, value, &put_options);
                black_box(batch)
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("merge_with_options/empty_batch", |b| {
        b.iter_batched(
            || {
                (
                    WriteBatch::new(),
                    Bytes::from_static(b"write_batch_key"),
                    Bytes::from_static(b"write_batch_value"),
                )
            },
            |(mut batch, key, value)| {
                batch.merge_with_options(key, value, &merge_options);
                black_box(batch)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("merge_with_options/new_key_populated", |b| {
        b.iter_batched(
            || {
                (
                    batch_without_merges.clone(),
                    key(EXTRACT_ENTRY_COUNT + 1),
                    value(EXTRACT_ENTRY_COUNT + 1),
                )
            },
            |(mut batch, key, value)| {
                batch.merge_with_options(key, value, &merge_options);
                black_box(batch)
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("merge_with_options/existing_key_with_merges", |b| {
        b.iter_batched(
            || {
                (
                    batch_with_merges.clone(),
                    key(0),
                    value(EXTRACT_MERGE_KEY_COUNT + 1),
                )
            },
            |(mut batch, key, value)| {
                batch.merge_with_options(key, value, &merge_options);
                black_box(batch)
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("delete/empty_batch", |b| {
        b.iter_batched(
            || (WriteBatch::new(), Bytes::from_static(b"write_batch_key")),
            |(mut batch, key)| {
                batch.delete(key);
                black_box(batch)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("delete/new_key_populated", |b| {
        b.iter_batched(
            || (batch_without_merges.clone(), key(EXTRACT_ENTRY_COUNT + 1)),
            |(mut batch, key)| {
                batch.delete(key);
                black_box(batch)
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("delete/existing_key", |b| {
        b.iter_batched(
            || (batch_with_merges.clone(), key(0)),
            |(mut batch, key)| {
                batch.delete(key);
                black_box(batch)
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("keys", |b| {
        b.iter_batched(
            || batch_without_merges.clone(),
            |batch| black_box(write_batch_benches::keys(&batch)),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("extract_entries/no_merges", |b| {
        b.to_async(&runtime).iter_batched(
            || batch_without_merges.clone(),
            |batch| async move {
                black_box(
                    write_batch_benches::extract_entries(&batch, 100, 1_000, None, None, None)
                        .await
                        .expect("extract_entries failed"),
                )
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("extract_entries/with_merges", |b| {
        b.to_async(&runtime).iter_batched(
            || batch_with_merges.clone(),
            |batch| async move {
                black_box(
                    write_batch_benches::extract_entries(
                        &batch,
                        100,
                        1_000,
                        None,
                        Some(size_sum_merge_operator()),
                        None,
                    )
                    .await
                    .expect("extract_entries failed"),
                )
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("drop/no_merges", |b| {
        b.iter_batched(
            || batch_without_merges.clone(),
            |batch| drop(black_box(batch)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function("drop/with_merges", |b| {
        b.iter_batched(
            || batch_with_merges.clone(),
            |batch| drop(black_box(batch)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function("drop/repeated_overwrites", |b| {
        b.iter_batched(
            || batch_with_repeated_overwrites.clone(),
            |batch| drop(black_box(batch)),
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_write_batch
}

criterion_main!(benches);
