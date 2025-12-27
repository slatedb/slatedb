#![cfg(not(windows))]

use bytes::Bytes;
use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::thread;

use crossbeam_skiplist::SkipMap as CrossbeamSkipMap;

use crate::mem_table::SequencedKey;
use crate::skipmap::SkipMap as ArenaSkipMap;
use crate::types::{RowEntry, ValueDeletable};

// Constants for large table benchmark (64MB target)
const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 100;
const ENTRY_SIZE: usize = KEY_SIZE + VALUE_SIZE;
const TARGET_SIZE_MB: usize = 64;
const TARGET_SIZE_BYTES: usize = TARGET_SIZE_MB * 1024 * 1024;
const LARGE_TABLE_ENTRIES: usize = TARGET_SIZE_BYTES / ENTRY_SIZE;

/// Generate a key of fixed size
fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("{:0>width$}", i, width = KEY_SIZE))
}

/// Generate a value of fixed size
fn make_value(i: usize) -> Bytes {
    let base = format!("{:0>16}", i);
    Bytes::from(base.repeat(VALUE_SIZE / 16 + 1)[..VALUE_SIZE].to_string())
}

// ============================================================================
// Large Table Benchmarks (64MB)
// ============================================================================

fn bench_large_table_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_table_insert_64mb");
    group.sample_size(10); // Fewer samples for large tables
    group.throughput(Throughput::Bytes(TARGET_SIZE_BYTES as u64));

    group.bench_function("arena", |b| {
        b.iter(|| {
            let map = ArenaSkipMap::with_initial_allocated_size(TARGET_SIZE_BYTES);
            for i in 0..LARGE_TABLE_ENTRIES {
                let key_bytes = make_key(i);
                let value_bytes = make_value(i);
                let seq_key = SequencedKey::new(key_bytes.clone(), i as u64);
                let row_entry = RowEntry::new(
                    key_bytes,
                    ValueDeletable::Value(value_bytes),
                    i as u64,
                    None,
                    None,
                );
                map.compare_insert(seq_key, row_entry, |_| true);
            }
            map
        });
    });

    group.bench_function("crossbeam", |b| {
        b.iter(|| {
            let map = CrossbeamSkipMap::<Bytes, Bytes>::new();
            for i in 0..LARGE_TABLE_ENTRIES {
                map.insert(make_key(i), make_value(i));
            }
            map
        });
    });

    group.finish();
}

fn bench_large_table_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_table_iteration_64mb");
    group.throughput(Throughput::Bytes(TARGET_SIZE_BYTES as u64));

    // Pre-populate arena map
    let arena_map = ArenaSkipMap::with_initial_allocated_size(TARGET_SIZE_BYTES);
    for i in 0..LARGE_TABLE_ENTRIES {
        let key_bytes = make_key(i);
        let value_bytes = make_value(i);
        let seq_key = SequencedKey::new(key_bytes.clone(), i as u64);
        let row_entry = RowEntry::new(
            key_bytes,
            ValueDeletable::Value(value_bytes),
            i as u64,
            None,
            None,
        );
        arena_map.compare_insert(seq_key, row_entry, |_| true);
    }

    // Pre-populate crossbeam map
    let crossbeam_map = CrossbeamSkipMap::<Bytes, Bytes>::new();
    for i in 0..LARGE_TABLE_ENTRIES {
        crossbeam_map.insert(make_key(i), make_value(i));
    }

    group.bench_function("arena_forward", |b| {
        b.iter(|| {
            let mut range = arena_map.range(..);
            let mut count = 0;
            while range.next().is_some() {
                count += 1;
            }
            count
        });
    });

    group.bench_function("crossbeam_forward", |b| {
        b.iter(|| crossbeam_map.iter().count())
    });

    group.bench_function("arena_backward", |b| {
        b.iter(|| {
            let mut range = arena_map.range(..);
            let mut count = 0;
            while range.next_back().is_some() {
                count += 1;
            }
            count
        });
    });

    group.bench_function("crossbeam_backward", |b| {
        b.iter(|| crossbeam_map.iter().rev().count())
    });

    group.finish();
}

fn bench_large_table_concurrent_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_table_concurrent_64mb");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(TARGET_SIZE_BYTES as u64));

    for num_threads in [4, 8] {
        let ops_per_thread = LARGE_TABLE_ENTRIES / num_threads;

        group.bench_with_input(
            BenchmarkId::new("arena", num_threads),
            &(num_threads, ops_per_thread),
            |b, &(num_threads, ops_per_thread)| {
                b.iter(|| {
                    let map =
                        Arc::new(ArenaSkipMap::with_initial_allocated_size(TARGET_SIZE_BYTES));
                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let map = map.clone();
                            thread::spawn(move || {
                                let start = t * ops_per_thread;
                                for i in start..(start + ops_per_thread) {
                                    let key_bytes = make_key(i);
                                    let value_bytes = make_value(i);
                                    let seq_key = SequencedKey::new(key_bytes.clone(), i as u64);
                                    let row_entry = RowEntry::new(
                                        key_bytes,
                                        ValueDeletable::Value(value_bytes),
                                        i as u64,
                                        None,
                                        None,
                                    );
                                    map.compare_insert(seq_key, row_entry, |_| true);
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("crossbeam", num_threads),
            &(num_threads, ops_per_thread),
            |b, &(num_threads, ops_per_thread)| {
                b.iter(|| {
                    let map = Arc::new(CrossbeamSkipMap::<Bytes, Bytes>::new());
                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let map = map.clone();
                            thread::spawn(move || {
                                let start = t * ops_per_thread;
                                for i in start..(start + ops_per_thread) {
                                    map.insert(make_key(i), make_value(i));
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

// ============================================================================
// Small Table Benchmarks (for quick iteration during development)
// ============================================================================

fn bench_sequential_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_insert");

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("arena", size), &size, |b, &size| {
            b.iter(|| {
                let map = ArenaSkipMap::new();
                for i in 0..size {
                    let key_bytes = make_key(i);
                    let value_bytes = make_value(i);
                    let seq_key = SequencedKey::new(key_bytes.clone(), i as u64);
                    let row_entry = RowEntry::new(
                        key_bytes,
                        ValueDeletable::Value(value_bytes),
                        i as u64,
                        None,
                        None,
                    );
                    map.compare_insert(seq_key, row_entry, |_| true);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("crossbeam", size), &size, |b, &size| {
            b.iter(|| {
                let map = CrossbeamSkipMap::<Bytes, Bytes>::new();
                for i in 0..size {
                    map.insert(make_key(i), make_value(i));
                }
            });
        });
    }
    group.finish();
}

fn bench_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration");
    let size = 10_000;

    // Pre-populate arena map
    let arena_map = ArenaSkipMap::new();
    for i in 0..size {
        let key_bytes = make_key(i);
        let value_bytes = make_value(i);
        let seq_key = SequencedKey::new(key_bytes.clone(), i as u64);
        let row_entry = RowEntry::new(
            key_bytes,
            ValueDeletable::Value(value_bytes),
            i as u64,
            None,
            None,
        );
        arena_map.compare_insert(seq_key, row_entry, |_| true);
    }

    // Pre-populate crossbeam map
    let crossbeam_map = CrossbeamSkipMap::<Bytes, Bytes>::new();
    for i in 0..size {
        crossbeam_map.insert(make_key(i), make_value(i));
    }

    group.throughput(Throughput::Elements(size as u64));

    group.bench_function("arena_forward", |b| {
        b.iter(|| {
            let mut range = arena_map.range(..);
            let mut count = 0;
            while range.next().is_some() {
                count += 1;
            }
            count
        });
    });

    group.bench_function("crossbeam_forward", |b| {
        b.iter(|| crossbeam_map.iter().count())
    });

    group.bench_function("arena_backward", |b| {
        b.iter(|| {
            let mut range = arena_map.range(..);
            let mut count = 0;
            while range.next_back().is_some() {
                count += 1;
            }
            count
        });
    });

    group.bench_function("crossbeam_backward", |b| {
        b.iter(|| crossbeam_map.iter().rev().count())
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_large_table_insert,
    bench_large_table_iteration,
    bench_large_table_concurrent_insert,
    bench_sequential_insert,
    bench_iteration,
);
