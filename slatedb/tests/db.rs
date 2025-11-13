#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::admin;
use slatedb::config::{
    CompactorOptions, PutOptions, Settings, SizeTieredCompactionSchedulerOptions, WriteOptions,
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

    let num_writers: usize = std::env::var("SLATEDB_TEST_NUM_WRITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let num_readers: usize = std::env::var("SLATEDB_TEST_NUM_READERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let writes_per_task: usize = std::env::var("SLATEDB_TEST_WRITES_PER_TASK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let key_length: usize = std::env::var("SLATEDB_TEST_KEY_LENGTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);

    // Pad keys to allow us to control how many blocks we take up
    // Since block size is not configurable
    fn zero_pad_key(key: u64, length: usize) -> Vec<u8> {
        let mut padded_key = vec![0; length];
        // Set the first 8 bytes to the key
        padded_key[0..8].copy_from_slice(&key.to_le_bytes());
        padded_key
    }

    let object_store: Arc<dyn ObjectStore> = if let Ok(provider) = std::env::var("CLOUD_PROVIDER") {
        log::info!("Using object store from env (CLOUD_PROVIDER={})", provider);
        admin::load_object_store_from_env(None).expect("failed to load object store from env")
    } else {
        Arc::new(InMemory::new()) as Arc<dyn ObjectStore>
    };
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
    let writer_handles = (0..num_writers)
        .map(|writer_id| {
            let db = db.clone();
            tokio::spawn(async move {
                let key = zero_pad_key(writer_id.try_into().unwrap(), key_length);
                for i in 1..=writes_per_task {
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
                // Flush to ensure all writes are durable before returning
                db.flush().await.expect("Failed to flush");
            })
        })
        .collect::<Vec<_>>();

    // Reader tasks: each reader reads all keys and verifies values are increasing
    let reader_handles = (0..num_readers)
        .map(|reader_id| {
            let db = db.clone();
            let reader_cancellation_token = reader_cancellation_token.clone();

            tokio::spawn(async move {
                let mut latest_values = HashMap::<usize, AtomicU64>::new();
                let mut iterations = 0;
                let mut rng = StdRng::from_os_rng();

                while !reader_cancellation_token.is_cancelled() {
                    // Pick a random key and validate that it's higher than the last value for that key
                    let key = rng.random_range(0..num_writers);
                    if let Some(bytes) = db
                        .get(zero_pad_key(key.try_into().unwrap(), key_length))
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
