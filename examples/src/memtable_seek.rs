//! Shows that `DbIterator::seek` is linear in the number of memtable entries
//! that it skips.
//!
//! The program loads N keys into a memtable. Each round opens a full-range
//! scan and seeks to a key near the end. The cost of a round grows with N.
//! For comparison, each round also opens a scan that starts at the target key.
//!
//! Run with: cargo run --release -p examples --bin memtable-seek

use std::sync::Arc;
#[allow(clippy::disallowed_types)]
use std::time::Instant;

use slatedb::config::Settings;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, Error, WriteBatch};

const SIZES: [usize; 4] = [10_000, 50_000, 100_000, 200_000];
const ROUNDS: usize = 200;
const BATCH_SIZE: usize = 10_000;

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:010}").into_bytes()
}

/// Keeps every write in the memtable, so no read touches an SST.
fn memtable_only_settings() -> Settings {
    Settings {
        l0_sst_size_bytes: 1 << 30,
        max_unflushed_bytes: 4 << 30,
        max_wal_flushes_before_l0_flush: u64::MAX,
        compactor_options: None,
        ..Settings::default()
    }
}

async fn load(n: usize) -> Result<Db, Error> {
    let db = Db::builder(format!("/memtable_seek/{n}"), Arc::new(InMemory::new()))
        .with_settings(memtable_only_settings())
        .build()
        .await?;
    for start in (0..n).step_by(BATCH_SIZE) {
        let mut batch = WriteBatch::new();
        for i in start..(start + BATCH_SIZE).min(n) {
            batch.put(key(i), b"value");
        }
        db.write(batch).await?;
    }
    Ok(db)
}

/// The target keys are the last 100 keys, so each seek skips almost all of
/// the memtable.
fn target(n: usize, round: usize) -> Vec<u8> {
    key(n - 1 - (round % 100))
}

#[allow(clippy::disallowed_types)]
async fn full_scan_then_seek(db: &Db, n: usize) -> Result<f64, Error> {
    let start = Instant::now();
    for round in 0..ROUNDS {
        let target = target(n, round);
        let mut iter = db.scan(..).await?;
        iter.seek(&target).await?;
        let kv = iter.next().await?.expect("target key exists");
        assert_eq!(kv.key.as_ref(), target.as_slice());
    }
    Ok(start.elapsed().as_secs_f64() * 1e6 / ROUNDS as f64)
}

#[allow(clippy::disallowed_types)]
async fn scan_from_target(db: &Db, n: usize) -> Result<f64, Error> {
    let start = Instant::now();
    for round in 0..ROUNDS {
        let target = target(n, round);
        let mut iter = db.scan(target.clone()..).await?;
        let kv = iter.next().await?.expect("target key exists");
        assert_eq!(kv.key.as_ref(), target.as_slice());
    }
    Ok(start.elapsed().as_secs_f64() * 1e6 / ROUNDS as f64)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!(
        "{:>10} {:>22} {:>22}",
        "keys", "scan(..) + seek (us)", "scan(target..) (us)"
    );
    for n in SIZES {
        let db = load(n).await?;
        // Warm up both paths once before the timed rounds.
        full_scan_then_seek(&db, n).await?;
        scan_from_target(&db, n).await?;
        let seek_us = full_scan_then_seek(&db, n).await?;
        let reopen_us = scan_from_target(&db, n).await?;
        println!("{n:>10} {seek_us:>22.1} {reopen_us:>22.1}");
        db.close().await?;
    }
    Ok(())
}
