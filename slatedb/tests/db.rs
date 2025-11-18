#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::config::{
    CompactorOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions, PutOptions,
    Settings, SizeTieredCompactionSchedulerOptions, WriteOptions,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use slatedb::Db;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_writers_and_readers() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_test_writer()
        .init();

    const NUM_WRITERS: usize = 10;
    const NUM_READERS: usize = 2;
    const WRITES_PER_TASK: usize = 100;
    const KEY_LENGTH: usize = 256;

    // Pad keys to allow us to control how many blocks we take up
    // Since block size is not configurable
    fn zero_pad_key(key: u64, length: usize) -> Vec<u8> {
        let mut padded_key = vec![0; length];
        // Set the first 8 bytes to the key
        padded_key[0..8].copy_from_slice(&key.to_le_bytes());
        padded_key
    }

    // Create an InMemory object store and DB
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = Settings {
        flush_interval: Some(Duration::from_millis(100)),
        manifest_poll_interval: Duration::from_millis(100),
        manifest_update_timeout: Duration::from_secs(300),
        compactor_options: Some(CompactorOptions {
            poll_interval: Duration::from_millis(100),
            ..Default::default()
        }),
        // Allow 16KB of unflushed data
        max_unflushed_bytes: 16 * 1024,
        min_filter_keys: 0,
        // Allow up to four 4096-byte blocks per-SST
        l0_sst_size_bytes: 4 * 4096,
        ..Default::default()
    };
    let supplier = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
        SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 1,
            ..Default::default()
        },
    ));
    let db = Arc::new(
        Db::builder("/tmp/test_concurrent_writers_readers", object_store.clone())
            .with_settings(config)
            .with_compaction_scheduler_supplier(supplier)
            .build()
            .await
            .unwrap(),
    );
    let reader_cancellation_token = CancellationToken::new();

    // Writer tasks: each writer writes to its own key with incrementing values
    let writer_handles = (0..NUM_WRITERS)
        .map(|writer_id| {
            let db = db.clone();
            tokio::spawn(async move {
                let key = zero_pad_key(writer_id.try_into().unwrap(), KEY_LENGTH);
                for i in 1..=WRITES_PER_TASK {
                    // Write the incremented value
                    db.put_with_options(
                        &key,
                        i.to_be_bytes().as_ref(),
                        &PutOptions::default(),
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .expect("Failed to write value");

                    if i % 10 == 0 {
                        log::info!("wrote values [writer_id={}, write_count={}]", writer_id, i);
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    // Reader tasks: each reader reads all keys and verifies values are increasing
    let reader_handles = (0..NUM_READERS)
        .map(|reader_id| {
            let db = db.clone();
            let reader_cancellation_token = reader_cancellation_token.clone();

            tokio::spawn(async move {
                let mut latest_values = HashMap::<usize, AtomicU64>::new();
                let mut iterations = 0;
                let mut rng = StdRng::from_os_rng();

                while !reader_cancellation_token.is_cancelled() {
                    // Pick a random key and validate that it's higher than the last value for that key
                    let key = rng.random_range(0..NUM_WRITERS);
                    if let Some(bytes) = db
                        .get(zero_pad_key(key.try_into().unwrap(), KEY_LENGTH))
                        .await?
                    {
                        // Convert bytes to u64 value
                        let value_bytes: [u8; 8] =
                            bytes.as_ref().try_into().map_err(|_| slatedb::Error::internal("invalid byte conversion".to_string()))?;
                        let current_value = u64::from_be_bytes(value_bytes);

                        // Check if this value is greater than the last seen value for this key
                        let last_seen_atomic = latest_values
                            .entry(key)
                            .or_insert(AtomicU64::new(current_value));
                        let last_seen_value = last_seen_atomic.load(Ordering::SeqCst);

                        assert!(
                            current_value >= last_seen_value,
                            "Value {} is less than last seen value {}",
                            current_value,
                            last_seen_value
                        );

                        // Update the latest value seen
                        last_seen_atomic
                            .compare_exchange(
                                last_seen_value,
                                current_value,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .unwrap();

                        iterations += 1;

                        if current_value != last_seen_value {
                            log::info!(
                                "progress [reader_id={}, iterations={}, sample_key={}, last_seen_value={}, current_value={}]",
                                reader_id,
                                iterations,
                                key,
                                last_seen_value,
                                current_value
                            );
                        }
                    }
                }
                Ok::<(), slatedb::Error>(())
            })
        })
        .collect::<Vec<_>>();

    // Wait for writers to complete
    futures::future::try_join_all(writer_handles)
        .await
        .expect("Writer handles failed");

    // Shut down readers
    reader_cancellation_token.cancel();
    futures::future::try_join_all(reader_handles)
        .await
        .expect("Reader handles failed");

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_writers_and_readers_with_aggressive_gc() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_test_writer()
        .try_init();

    const NUM_WRITERS: usize = 4;
    const NUM_READERS: usize = 2;
    const KEY_LENGTH: usize = 256;
    const TEST_DURATION: Duration = Duration::from_secs(30);

    fn zero_pad_key(key: u64, length: usize) -> Vec<u8> {
        let mut padded_key = vec![0; length];
        padded_key[0..8].copy_from_slice(&key.to_le_bytes());
        padded_key
    }

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = Settings {
        flush_interval: Some(Duration::from_millis(50)),
        manifest_poll_interval: Duration::from_millis(50),
        manifest_update_timeout: Duration::from_secs(60),
        compactor_options: Some(CompactorOptions {
            poll_interval: Duration::from_millis(50),
            ..Default::default()
        }),
        max_unflushed_bytes: 16 * 1024,
        min_filter_keys: 0,
        l0_sst_size_bytes: 4 * 4096,
        garbage_collector_options: Some(GarbageCollectorOptions {
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_millis(100)),
                min_age: Duration::from_millis(100),
            }),
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_millis(100)),
                min_age: Duration::from_millis(100),
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_millis(100)),
                min_age: Duration::from_millis(100),
            }),
        }),
        ..Default::default()
    };

    let supplier = Arc::new(SizeTieredCompactionSchedulerSupplier::new(
        SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 1,
            ..Default::default()
        },
    ));
    let db = Arc::new(
        Db::builder(
            "/tmp/test_concurrent_writers_readers_with_aggressive_gc",
            object_store.clone(),
        )
        .with_settings(config)
        .with_compaction_scheduler_supplier(supplier)
        .build()
        .await
        .unwrap(),
    );
    let stop_token = CancellationToken::new();

    let writer_handles = (0..NUM_WRITERS)
        .map(|writer_id| {
            let db = db.clone();
            let stop_token = stop_token.clone();
            tokio::spawn(async move {
                let key = zero_pad_key(writer_id.try_into().unwrap(), KEY_LENGTH);
                let mut value: u64 = 0;
                while !stop_token.is_cancelled() {
                    value += 1;
                    let write_result = tokio::time::timeout(
                        Duration::from_secs(30),
                        db.put_with_options(
                            &key,
                            value.to_be_bytes().as_ref(),
                            &PutOptions::default(),
                            &WriteOptions {
                                await_durable: false,
                            },
                        ),
                    )
                    .await;

                    match write_result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => panic!("Failed to write value: {:?}", e),
                        Err(e) => panic!("Write timed out: {:?}", e),
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    let reader_handles = (0..NUM_READERS)
        .map(|reader_id| {
            let db = db.clone();
            let stop_token = stop_token.clone();

            tokio::spawn(async move {
                let mut latest_values = HashMap::<usize, AtomicU64>::new();
                let mut iterations = 0;
                let mut rng = StdRng::from_os_rng();

                while !stop_token.is_cancelled() {
                    let key = rng.random_range(0..NUM_WRITERS);
                    if let Some(bytes) = db
                        .get(zero_pad_key(key.try_into().unwrap(), KEY_LENGTH))
                        .await?
                    {
                        let value_bytes: [u8; 8] =
                            bytes.as_ref().try_into().map_err(|_| slatedb::Error::internal("invalid byte conversion".to_string()))?;
                        let current_value = u64::from_be_bytes(value_bytes);

                        let last_seen_atomic = latest_values
                            .entry(key)
                            .or_insert(AtomicU64::new(current_value));
                        let last_seen_value = last_seen_atomic.load(Ordering::SeqCst);

                        assert!(
                            current_value >= last_seen_value,
                            "Value {} is less than last seen value {}",
                            current_value,
                            last_seen_value
                        );

                        last_seen_atomic
                            .compare_exchange(
                                last_seen_value,
                                current_value,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .unwrap();

                        iterations += 1;

                        if current_value != last_seen_value {
                            log::info!(
                                "gc progress [reader_id={}, iterations={}, sample_key={}, last_seen_value={}, current_value={}]",
                                reader_id,
                                iterations,
                                key,
                                last_seen_value,
                                current_value
                            );
                        }
                    }
                }
                Ok::<(), slatedb::Error>(())
            })
        })
        .collect::<Vec<_>>();

    tokio::time::sleep(TEST_DURATION).await;
    log::info!("test_concurrent_writers_and_readers_with_aggressive_gc finished, canceling writers and readers");
    stop_token.cancel();

    futures::future::try_join_all(writer_handles)
        .await
        .expect("Writer handles failed");

    log::info!("writers finished");
    futures::future::try_join_all(reader_handles)
        .await
        .expect("Reader handles failed");

    log::info!("readers finished");
    db.close().await.unwrap();
}
