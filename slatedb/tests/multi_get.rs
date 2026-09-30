#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

//! Integration tests for `multi_get`.
//!
//! The core gate is the differential property: `multi_get(keys)` must equal
//! `[get(k) for k in keys]` against the same database. We assert this over
//! randomized, layered data (many L0 SSTs over a compacted sorted run), with
//! and without a block cache and a merge operator, plus transaction, reader,
//! snapshot, and projected clone variants. The request count test checks that
//! a batch sends no more object store GETs than a `get` loop.

#[cfg(feature = "wal_disable")]
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
#[cfg(feature = "wal_disable")]
use slatedb::admin::Admin;
use slatedb::bytes::Bytes;
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, DbReaderOptions, FlushOptions, FlushType,
    MultiGetOptions, PutOptions, Settings, SizeTieredCompactionSchedulerOptions, WriteOptions,
};
use slatedb::db_cache::foyer::FoyerCache;
use slatedb::db_stats::{MULTI_GET_KEYS, MULTI_GET_LAYERS, REQUEST_COUNT};
use slatedb::instrumented_object_store_stats::REQUEST_COUNT as OBJECT_STORE_REQUEST_COUNT;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
#[cfg(feature = "wal_disable")]
use slatedb::CloneSourceSpec;
use slatedb::{
    CompactorBuilder, Db, DbReadOps, DbReader, IsolationLevel, MergeOperator, MergeOperatorError,
    SstBlockSize,
};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};

// Writes do not wait for durability by default.
fn no_durable() -> WriteOptions {
    WriteOptions::default()
}

/// A merge operator that concatenates operands onto the base value.
struct ConcatMergeOperator;

impl MergeOperator for ConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        match existing_value {
            Some(base) => {
                let mut merged = base.to_vec();
                merged.extend_from_slice(&operand);
                Ok(Bytes::from(merged))
            }
            None => Ok(operand),
        }
    }
}

/// `Db::flush` only flushes the WAL. This writes the memtable to an L0 SST.
async fn flush_memtable(db: &Db) {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
}

fn key(id: usize) -> Vec<u8> {
    format!("key{id:05}").into_bytes()
}

/// THE differential gate: `multi_get` must agree key-by-key with single `get`.
async fn assert_multi_get_matches_get_loop<R: DbReadOps + Sync>(db: &R, keys: &[Vec<u8>]) {
    let multi = db.multi_get(keys).await.expect("multi_get failed");
    assert_eq!(multi.len(), keys.len(), "one result slot per input key");
    for (i, k) in keys.iter().enumerate() {
        let single = db.get(k).await.expect("get failed");
        assert_eq!(
            multi[i],
            single,
            "multi_get disagreed with get for {:?}",
            String::from_utf8_lossy(k)
        );
    }
}

/// Apply a deterministic sequence of random put/delete ops over `key_space`
/// keys, flushing periodically so data spreads across the memtable and many L0
/// SSTs. Returns the RNG so the caller can keep generating query batches.
async fn populate_random(db: &Db, seed: u64, key_space: usize, ops: usize) -> StdRng {
    let mut rng = StdRng::seed_from_u64(seed);
    for _ in 0..ops {
        let k = key(rng.random_range(0..key_space));
        if rng.random_bool(0.2) {
            db.delete_with_options(&k, &no_durable()).await.unwrap();
        } else {
            let v = format!("v{}", rng.random_range(0..1_000_000)).into_bytes();
            db.put_with_options(&k, &v, &PutOptions::default(), &no_durable())
                .await
                .unwrap();
        }
        if rng.random_bool(0.04) {
            flush_memtable(db).await;
        }
    }
    flush_memtable(db).await;
    rng
}

/// Query many random batches (with duplicates and absent keys) plus a full
/// sweep, asserting the differential invariant each time.
async fn assert_random_batches<R: DbReadOps + Sync>(db: &R, rng: &mut StdRng, key_space: usize) {
    for _ in 0..15 {
        let batch_size = rng.random_range(0..12);
        let keys: Vec<Vec<u8>> = (0..batch_size)
            // key_space + 20 so ~some queried ids are absent
            .map(|_| key(rng.random_range(0..key_space + 20)))
            .collect();
        assert_multi_get_matches_get_loop(db, &keys).await;
    }
    let sweep: Vec<Vec<u8>> = (0..key_space + 20).map(key).collect();
    assert_multi_get_matches_get_loop(db, &sweep).await;
}

fn layered_settings() -> Settings {
    Settings {
        // Small SSTs + always-on filters so a batch spans several bloom-filtered
        // L0 SSTs — the case multi_get's per-SST batching targets.
        l0_sst_size_bytes: 1024,
        min_filter_keys: 0,
        // No L0 stall, so a flush never waits for the compactor.
        l0_max_ssts: 10_000,
        l0_max_ssts_per_key: 10_000,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_matches_get_loop_no_cache() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder("/tmp/test_multi_get_no_cache", object_store)
        .with_settings(layered_settings())
        .build()
        .await
        .unwrap();

    let key_space = 80;
    let mut rng = populate_random(&db, 0xA11CE, key_space, 500).await;
    assert_random_batches(&db, &mut rng, key_space).await;

    // Explicit edge cases.
    assert!(db.multi_get::<Vec<u8>>(&[]).await.unwrap().is_empty());
    let dup = vec![key(0), key(0), key(1), key(0)];
    let got = db.multi_get(&dup).await.unwrap();
    assert_eq!(got[0], got[1]);
    assert_eq!(got[0], got[3]);

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_matches_get_loop_with_block_cache() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = Arc::new(FoyerCache::new());
    let db = Db::builder("/tmp/test_multi_get_cache", object_store)
        .with_settings(layered_settings())
        .with_db_cache(cache, 0)
        .build()
        .await
        .unwrap();

    let key_space = 80;
    let mut rng = populate_random(&db, 0xCACE, key_space, 500).await;
    // Warm the cache with one sweep, then re-run batches against warm caches.
    let sweep: Vec<Vec<u8>> = (0..key_space).map(key).collect();
    assert_multi_get_matches_get_loop(&db, &sweep).await;
    assert_random_batches(&db, &mut rng, key_space).await;

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_matches_get_loop_with_merge_operator() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder("/tmp/test_multi_get_merge", object_store)
        .with_settings(layered_settings())
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .build()
        .await
        .unwrap();

    // Random put / merge / delete so values are built from operands that span
    // the memtable and multiple L0 SSTs.
    let key_space = 50;
    let mut rng = StdRng::seed_from_u64(0x3E26E);
    for _ in 0..600 {
        let k = key(rng.random_range(0..key_space));
        let roll = rng.random_range(0..10);
        if roll < 2 {
            db.delete_with_options(&k, &no_durable()).await.unwrap();
        } else if roll < 6 {
            let v = format!("v{}", rng.random_range(0..1000)).into_bytes();
            db.put_with_options(&k, &v, &PutOptions::default(), &no_durable())
                .await
                .unwrap();
        } else {
            let v = format!("m{}", rng.random_range(0..1000)).into_bytes();
            db.merge_with_options(&k, &v, &Default::default(), &no_durable())
                .await
                .unwrap();
        }
        if rng.random_bool(0.04) {
            flush_memtable(&db).await;
        }
    }
    flush_memtable(&db).await;
    assert_random_batches(&db, &mut rng, key_space).await;

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_transaction_sees_buffered_writes() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder("/tmp/test_multi_get_txn_buffered", object_store)
        .with_settings(layered_settings())
        .build()
        .await
        .unwrap();

    db.put(&key(1), b"committed1").await.unwrap();
    db.put(&key(2), b"committed2").await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    // Buffered (uncommitted) writes are visible to the transaction's own reads.
    txn.put(key(1), b"txn1").unwrap();
    txn.delete(key(2)).unwrap();
    txn.put(key(3), b"txn3").unwrap();

    let got = txn
        .multi_get(&[key(1), key(2), key(3), key(4)])
        .await
        .unwrap();
    assert_eq!(got[0].as_deref(), Some(b"txn1".as_ref())); // overwritten in txn
    assert_eq!(got[1], None); // deleted in txn
    assert_eq!(got[2].as_deref(), Some(b"txn3".as_ref())); // new in txn
    assert_eq!(got[3], None); // absent

    // The base DB does not see the uncommitted writes.
    let base = db.multi_get(&[key(1), key(2), key(3)]).await.unwrap();
    assert_eq!(base[0].as_deref(), Some(b"committed1".as_ref()));
    assert_eq!(base[1].as_deref(), Some(b"committed2".as_ref()));
    assert_eq!(base[2], None);

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_ssi_tracks_all_batch_keys() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder("/tmp/test_multi_get_ssi", object_store)
        .with_settings(layered_settings())
        .build()
        .await
        .unwrap();

    db.put(&key(1), b"a").await.unwrap();
    db.put(&key(2), b"b").await.unwrap();

    // txn1 reads a batch (which must register every key in the SSI read set).
    let txn1 = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let _ = txn1.multi_get(&[key(1), key(2)]).await.unwrap();

    // txn2 modifies key(2) — one of txn1's batch reads — and commits.
    let txn2 = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    txn2.put(key(2), b"b2").unwrap();
    txn2.commit().await.unwrap();

    // txn1 now writes and commits; it must abort because a key it read via
    // multi_get was modified by a committed concurrent transaction.
    txn1.put(key(9), b"x").unwrap();
    let result = txn1.commit().await;
    assert!(
        result.is_err(),
        "txn1 must abort: multi_get read key(2), which txn2 modified"
    );

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_db_reader() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "/tmp/test_multi_get_reader";
    let db = Db::builder(path, object_store.clone())
        .with_settings(layered_settings())
        .build()
        .await
        .unwrap();

    let key_space = 60;
    populate_random(&db, 0x9EADE2, key_space, 500).await;
    db.close().await.unwrap();

    // Open a read-only reader over the persisted state and check the
    // differential invariant against the reader's own single get.
    let reader = DbReader::builder(path, object_store).build().await.unwrap();
    for batch_start in [0usize, 25, 60] {
        let keys: Vec<Vec<u8>> = (batch_start..batch_start + 30).map(key).collect();
        let multi = reader.multi_get(&keys).await.unwrap();
        for (i, k) in keys.iter().enumerate() {
            let single = reader.get(k).await.unwrap();
            assert_eq!(multi[i], single, "reader multi_get vs get mismatch");
        }
    }
    reader.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_snapshot_matches_get_loop() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder("/tmp/test_multi_get_snapshot", object_store)
        .with_settings(layered_settings())
        .build()
        .await
        .unwrap();

    let key_space = 80;
    populate_random(&db, 0x5A9, key_space, 500).await;
    let snapshot = db.snapshot().await.unwrap();
    // Writes after the snapshot stay invisible to it.
    let mut rng = populate_random(&db, 0x5A9 + 1, key_space, 300).await;
    db.put(&key(key_space + 5), b"after").await.unwrap();
    flush_memtable(&db).await;

    assert_random_batches(snapshot.as_ref(), &mut rng, key_space).await;
    let got = snapshot.multi_get(&[key(key_space + 5)]).await.unwrap();
    assert_eq!(got[0], None, "the snapshot must not see a later write");

    db.close().await.unwrap();
}

/// A projection rejects a parent with WAL data, so the parent runs without a WAL.
#[cfg(feature = "wal_disable")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_projected_clone_matches_get_loop() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let parent_path = "/tmp/test_multi_get_clone_parent";
    let clone_path = "/tmp/test_multi_get_clone";
    let db = Db::builder(parent_path, object_store.clone())
        .with_settings(Settings {
            wal_enabled: false,
            ..layered_settings()
        })
        .build()
        .await
        .unwrap();
    let key_space = 80;
    let mut rng = populate_random(&db, 0xC10E, key_space, 500).await;
    db.close().await.unwrap();

    // The clone sees only [key(20), key(50)) of the parent.
    let range = (
        Bound::Included(Bytes::from(key(20))),
        Bound::Excluded(Bytes::from(key(50))),
    );
    Admin::builder(clone_path, object_store.clone())
        .build()
        .create_clone_builder_from_source(
            CloneSourceSpec::new(parent_path).with_projection_range(range),
        )
        .build()
        .await
        .unwrap();
    let clone = Db::builder(clone_path, object_store)
        .with_settings(layered_settings())
        .build()
        .await
        .unwrap();

    assert_random_batches(&clone, &mut rng, key_space).await;
    let outside = clone
        .multi_get(&[key(0), key(19), key(50), key(79)])
        .await
        .unwrap();
    assert!(
        outside.iter().all(Option::is_none),
        "keys outside the projection must be absent"
    );

    clone.close().await.unwrap();
}

/// Same compactor setup as `tests/scan_model.rs`: each L0 flush compacts into
/// a sorted run of many small SSTs.
async fn open_compacting_db(path: &str, object_store: Arc<dyn ObjectStore>) -> Db {
    let settings = Settings {
        manifest_poll_interval: Duration::from_millis(10),
        l0_sst_size_bytes: 1024,
        l0_max_ssts: 10_000,
        l0_max_ssts_per_key: 10_000,
        min_filter_keys: 0,
        ..Settings::default()
    };
    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(1),
        commit_compacted_interval: Duration::from_millis(1),
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 1,
            ..Default::default()
        }
        .into(),
        worker: Some(CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(1),
            max_sst_size: 64,
            ..Default::default()
        }),
        ..Default::default()
    };
    Db::builder(path, object_store.clone())
        .with_settings(settings)
        .with_sst_block_size(SstBlockSize::Block1Kib)
        .with_compactor_builder(
            CompactorBuilder::new(path, object_store)
                .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier::new()))
                .with_options(compactor_options),
        )
        .build()
        .await
        .unwrap()
}

async fn compact_l0(db: &Db) {
    flush_memtable(db).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !db.manifest().l0().is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("compactor did not drain L0 within 30s");
}

/// Keys spread over L0 SSTs and sorted runs in the same query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_matches_get_loop_over_l0_and_sorted_runs() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "/tmp/test_multi_get_mixed";
    let db = open_compacting_db(path, object_store.clone()).await;
    let key_space = 80;
    populate_random(&db, 0xC0FFEE, key_space, 300).await;
    compact_l0(&db).await;
    db.close().await.unwrap();

    // Reopen without a compactor, so new L0 SSTs stay on top of the runs.
    let db = Db::builder(path, object_store)
        .with_settings(Settings {
            compactor_options: None,
            ..layered_settings()
        })
        .build()
        .await
        .unwrap();
    let mut rng = populate_random(&db, 0xC0FFEE + 1, key_space, 300).await;
    let manifest = db.manifest();
    let run_ssts: usize = manifest
        .compacted()
        .iter()
        .map(|sr| sr.sst_views().len())
        .sum();
    assert!(
        run_ssts > 1,
        "the fixture must build a sorted run of many SSTs"
    );
    assert!(
        manifest.l0().len() > 1,
        "the fixture must hold L0 SSTs over the sorted run"
    );

    assert_random_batches(&db, &mut rng, key_space).await;

    db.close().await.unwrap();
}

fn counter(recorder: &DefaultMetricsRecorder, name: &str, labels: &[(&str, &str)]) -> u64 {
    recorder
        .snapshot()
        .by_name_and_labels(name, labels)
        .map(|m| match m.value {
            MetricValue::Counter(v) => v,
            ref other => panic!("expected counter, got {other:?}"),
        })
        .unwrap_or(0)
}

/// Ranged GET requests of the reader. An SST read is a ranged request, a
/// manifest read is not.
fn sst_reads(recorder: &DefaultMetricsRecorder) -> u64 {
    ["get_range", "get_ranges"]
        .iter()
        .map(|api| {
            counter(
                recorder,
                OBJECT_STORE_REQUEST_COUNT,
                &[
                    ("component", "reader"),
                    ("store_type", "main"),
                    ("op", "get"),
                    ("api", api),
                ],
            )
        })
        .sum()
}

/// Writes `ssts` L0 SSTs. With `overwrite`, each SST holds a new version of all
/// keys. Without it, each key lives in one SST. With `merge`, the versions
/// after the first are merge operands.
async fn open_l0_db(
    path: &str,
    cached: bool,
    merge: bool,
    keys: &[Vec<u8>],
    ssts: usize,
    overwrite: bool,
) -> (Db, Arc<dyn ObjectStore>, Arc<DefaultMetricsRecorder>) {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut builder = Db::builder(path, object_store.clone())
        .with_settings(Settings {
            min_filter_keys: 0,
            compactor_options: None,
            ..Settings::default()
        })
        .with_metrics_recorder(recorder.clone());
    builder = if cached {
        builder.with_db_cache(Arc::new(FoyerCache::new()), 0)
    } else {
        builder.with_db_cache_disabled()
    };
    if merge {
        builder = builder.with_merge_operator(Arc::new(ConcatMergeOperator));
    }
    let db = builder.build().await.unwrap();
    for sst in 0..ssts {
        for (i, k) in keys.iter().enumerate() {
            if overwrite || i % ssts == sst {
                let v = format!("v{sst}").into_bytes();
                if merge && sst > 0 {
                    db.merge_with_options(k, &v, &Default::default(), &no_durable())
                        .await
                        .unwrap();
                } else {
                    db.put_with_options(k, &v, &PutOptions::default(), &no_durable())
                        .await
                        .unwrap();
                }
            }
        }
        flush_memtable(&db).await;
    }
    (db, object_store, recorder)
}

/// Opens a reader with an empty block cache over the SSTs of `path`. The reader
/// does not poll the manifest or read the WAL, so only the reads count.
async fn open_reader(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    recorder: Arc<DefaultMetricsRecorder>,
    merge: bool,
) -> DbReader {
    let mut builder = DbReader::builder(path, object_store)
        .with_options(DbReaderOptions {
            manifest_poll_interval: Duration::from_secs(3600),
            checkpoint_lifetime: Duration::from_secs(4 * 3600),
            skip_wal_replay: true,
            ..Default::default()
        })
        .with_db_cache(Arc::new(FoyerCache::new()), 0)
        .with_metrics_recorder(recorder);
    if merge {
        builder = builder.with_merge_operator(Arc::new(ConcatMergeOperator));
    }
    builder.build().await.unwrap()
}

/// Goal 3 of the RFC: a batch sends no more GETs than a `get` loop over the
/// same keys, plus the reads of the walks in flight when the batch stops early.
/// Cold means a fresh reader for the batch and another one for the loop.
#[rstest]
#[case::distinct_keys(32, false, false)]
// A small batch, so shared reads cannot hide the reads of shadowed versions.
#[case::many_versions(2, true, false)]
// Merge operands in every SST, so every version must be read.
#[case::merge_operands(2, true, true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_requests_not_above_get_loop(
    #[case] num_keys: usize,
    #[case] overwrite: bool,
    #[case] merge: bool,
    #[values(1, 4)] lookahead: usize,
    #[values(false, true)] warm: bool,
) {
    const SSTS: usize = 6;
    let keys: Vec<Vec<u8>> = (0..num_keys).map(key).collect();
    let path =
        format!("/tmp/test_multi_get_requests_{num_keys}_{overwrite}_{merge}_{lookahead}_{warm}");
    let (db, object_store, recorder) =
        open_l0_db(&path, false, merge, &keys, SSTS, overwrite).await;
    db.close().await.unwrap();
    let options = MultiGetOptions::default().with_lookahead(lookahead);

    let reader = open_reader(&path, object_store.clone(), recorder.clone(), merge).await;
    if warm {
        reader
            .multi_get_with_options(&keys, &options)
            .await
            .unwrap();
        for k in &keys {
            reader.get(k).await.unwrap();
        }
    }
    let start = sst_reads(&recorder);
    let batch = reader
        .multi_get_with_options(&keys, &options)
        .await
        .unwrap();
    let batch_reads = sst_reads(&recorder) - start;

    let reader = if warm {
        reader
    } else {
        reader.close().await.unwrap();
        open_reader(&path, object_store, recorder.clone(), merge).await
    };
    let start = sst_reads(&recorder);
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(reader.get(k).await.unwrap(), batch[i]);
    }
    let loop_reads = sst_reads(&recorder) - start;
    reader.close().await.unwrap();

    // A walk in flight reads at most a filter, an index and one block before it is dropped.
    let in_flight = 3 * (lookahead as u64 - 1);
    assert!(
        batch_reads <= loop_reads + in_flight,
        "lookahead={lookahead} warm={warm}: batch sent {batch_reads} reads, loop sent {loop_reads}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_get_metrics() {
    let keys = vec![key(0), key(1), key(0)];
    let (db, _, recorder) =
        open_l0_db("/tmp/test_multi_get_metrics", false, false, &keys, 1, true).await;

    db.multi_get(&keys).await.unwrap();

    assert_eq!(counter(&recorder, REQUEST_COUNT, &[("op", "multi_get")]), 1);
    assert_eq!(counter(&recorder, MULTI_GET_KEYS, &[]), 3);
    assert_eq!(counter(&recorder, MULTI_GET_LAYERS, &[]), 1);
    assert_eq!(counter(&recorder, REQUEST_COUNT, &[("op", "get")]), 0);
    db.close().await.unwrap();
}
