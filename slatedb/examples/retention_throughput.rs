//! Measures explicit memtable flushes and one eight-source compaction.
//! Run with `cargo run --release -p slatedb --example retention_throughput
//! --features wal_disable,lz4,zstd -- 4000000`.

use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use slatedb::admin::Admin;
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, CompressionCodec, FlushOptions, FlushType,
    ScanOptions, SizeTieredCompactionSchedulerOptions,
};
use slatedb::{CompactorBuilder, Db, DbReader, Settings, SstBlockSize, WriteBatch};
use slatedb_common::clock::{DefaultSystemClock, SystemClock};

const SOURCES: u64 = 8;

fn key(index: u64) -> [u8; 20] {
    let mut key = [0; 20];
    key[12..].copy_from_slice(&index.to_be_bytes());
    key
}

fn value(index: u64) -> [u8; 12] {
    let mut mixed = index.wrapping_add(0x9e3779b97f4a7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
    mixed ^= mixed >> 31;
    let mut value = [0; 12];
    value[..8].copy_from_slice(&mixed.to_be_bytes());
    value[8..].copy_from_slice(&(mixed as u32).to_be_bytes());
    value
}

async fn verify(store: Arc<dyn ObjectStore>, rows: u64) -> Result<(), Box<dyn Error>> {
    let reader = DbReader::builder("bench", store)
        .with_db_cache_disabled()
        .build()
        .await?;
    let mut iter = reader
        .scan_with_options(.., &ScanOptions::new().with_read_ahead_bytes(65536))
        .await?;
    for index in 0..rows {
        let entry = iter.next().await?.expect("expected a stored row");
        assert_eq!(entry.key.as_ref(), key(index));
        assert_eq!(entry.value.as_ref(), value(index));
    }
    assert!(iter.next().await?.is_none());
    drop(iter);
    reader.close().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let rows: u64 = std::env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(4_000_000);
    assert!(rows > 0 && rows.is_multiple_of(SOURCES));
    let clock = DefaultSystemClock::new();
    let root = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root.path())?);
    let db = Db::builder("bench", store.clone())
        .with_settings(Settings {
            wal_enabled: false,
            compactor_options: None,
            garbage_collector_options: None,
            l0_sst_size_bytes: 128 * 1024 * 1024,
            max_unflushed_bytes: 256 * 1024 * 1024,
            l0_max_ssts: 32,
            l0_max_ssts_per_key: 32,
            min_filter_keys: u32::MAX,
            compression_codec: Some(CompressionCodec::Lz4),
            ..Default::default()
        })
        .with_sst_block_size(SstBlockSize::Block16Kib)
        .with_db_cache_disabled()
        .with_seed(42)
        .build()
        .await?;
    let mut flush_time = Duration::ZERO;
    for source in 0..SOURCES {
        let mut batch = WriteBatch::new();
        for offset in 0..rows / SOURCES {
            // Interleave the eight key ranges so compaction must merge all inputs.
            let index = offset * SOURCES + source;
            batch.put(key(index), value(index));
            if (offset + 1).is_multiple_of(4096) {
                db.write(std::mem::take(&mut batch)).await?;
            }
        }
        db.write(batch).await?;
        let start = clock.now();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await?;
        flush_time += clock.now().signed_duration_since(start).to_std()?;
        assert_eq!(db.manifest().l0().len(), (source + 1) as usize);
    }
    db.close().await?;
    // Validate persisted flush output and warm OS pages before timing compaction.
    verify(store.clone(), rows).await?;

    let compactor = CompactorBuilder::new("bench", store.clone())
        .with_options(CompactorOptions {
            poll_interval: Duration::from_millis(5),
            commit_compacted_interval: Duration::from_millis(5),
            max_concurrent_compactions: 1,
            scheduler_options: SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: SOURCES as usize,
                max_compaction_sources: SOURCES as usize,
                ..Default::default()
            }
            .into(),
            worker: Some(CompactionWorkerOptions {
                compactions_poll_interval: Duration::from_millis(5),
                max_concurrent_compactions: 1,
                max_subcompactions: 1,
                max_sst_size: 1024 * 1024 * 1024,
                min_filter_keys: u32::MAX,
                compression_codec: Some(CompressionCodec::Zstd),
                ..Default::default()
            }),
            ..Default::default()
        })
        .with_sst_block_size(SstBlockSize::Block16Kib)
        .with_seed(42)
        .build();
    let admin = Admin::builder("bench", store.clone()).build();
    let runner = compactor.clone();
    let start = clock.now();
    let task = tokio::spawn(async move { runner.run().await });
    let measured: Result<Duration, Box<dyn Error>> = async {
        loop {
            let manifest = admin.read_manifest(None).await?.expect("stored manifest");
            let elapsed = clock.now().signed_duration_since(start).to_std()?;
            if manifest.l0().is_empty() && manifest.compacted().len() == 1 {
                return Ok(elapsed);
            }
            if elapsed > Duration::from_secs(120) {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            clock.sleep(Duration::from_millis(5)).await;
        }
    }
    .await;
    compactor.stop().await?;
    task.await??;
    let compaction_time = measured?;
    verify(store, rows).await?;
    for (phase, elapsed) in [("flush", flush_time), ("compaction", compaction_time)] {
        writeln!(
            std::io::stdout().lock(),
            "{phase},{rows},{:.6},{:.0}",
            elapsed.as_secs_f64(),
            rows as f64 / elapsed.as_secs_f64()
        )?;
    }
    Ok(())
}
