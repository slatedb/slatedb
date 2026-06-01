#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

//! Integration tests for `multi_get`.
//!
//! The core gate is the differential property: `multi_get(keys)` must equal
//! `[get(k) for k in keys]` against the same database. We assert this over
//! randomized, layered data (many L0 SSTs from repeated flushes), with and
//! without a block cache and a merge operator, plus transaction and reader
//! variants.

use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::bytes::Bytes;
use slatedb::config::{PutOptions, Settings, WriteOptions};
use slatedb::db_cache::foyer::FoyerCache;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel, MergeOperator, MergeOperatorError};

fn no_durable() -> WriteOptions {
    WriteOptions {
        await_durable: false,
        ..Default::default()
    }
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

fn key(id: usize) -> Vec<u8> {
    format!("key{id:05}").into_bytes()
}

/// THE differential gate: `multi_get` must agree key-by-key with single `get`.
async fn assert_multi_get_matches_get_loop(db: &Db, keys: &[Vec<u8>]) {
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
            db.flush().await.unwrap();
        }
    }
    db.flush().await.unwrap();
    rng
}

/// Query many random batches (with duplicates and absent keys) plus a full
/// sweep, asserting the differential invariant each time.
async fn assert_random_batches(db: &Db, rng: &mut StdRng, key_space: usize) {
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
        .with_db_cache(cache)
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
            db.flush().await.unwrap();
        }
    }
    db.flush().await.unwrap();
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
    let txn1 = db.begin(IsolationLevel::SerializableSnapshot).await.unwrap();
    let _ = txn1.multi_get(&[key(1), key(2)]).await.unwrap();

    // txn2 modifies key(2) — one of txn1's batch reads — and commits.
    let txn2 = db.begin(IsolationLevel::SerializableSnapshot).await.unwrap();
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
    let reader = slatedb::DbReader::builder(path, object_store)
        .build()
        .await
        .unwrap();
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
