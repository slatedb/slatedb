// Allowed because we don't have access to SlateDB's private rand crate
#![allow(clippy::disallowed_types)]

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::config::{CompactorOptions, PutOptions, Settings, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_writers_and_readers() {
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
    let db = Arc::new(
        Db::builder("/tmp/test_concurrent_writers_readers", object_store.clone())
            .with_settings(config)
            .build()
            .await
            .unwrap(),
    );

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
                        println!("Writer {} wrote {} values", writer_id, i);
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    // Reader tasks: each reader reads all keys and verifies values are increasing
    let reader_handles = (0..NUM_READERS)
        .map(|reader_id| {
            let db = db.clone();

            tokio::spawn(async move {
                let mut latest_values = HashMap::<usize, AtomicU64>::new();
                let mut iterations = 0;
                let mut rng = StdRng::from_entropy();

                loop {
                    // Pick a random key and validate that it's higher than the last value for that key
                    let key = rng.gen_range(0..NUM_WRITERS);
                    if let Some(bytes) = db
                        .get(zero_pad_key(key.try_into().unwrap(), KEY_LENGTH))
                        .await
                        .expect("Failed to read value")
                    {
                        // Convert bytes to u64 value
                        let value_bytes: [u8; 8] =
                            bytes.as_ref().try_into().expect("Invalid value size");
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
                            println!("Reader {} processed {} values. Sample key: {}, last seen value: {}, current value: {}", reader_id, iterations, key, last_seen_value, current_value);
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    // Wait for writers to complete
    futures::future::try_join_all(writer_handles)
        .await
        .expect("Writer handles failed");

    // Shut down readers
    let reader_handles = reader_handles
        .into_iter()
        .map(|handle| {
            handle.abort();
            handle
        })
        .collect::<Vec<_>>();
    let _ = futures::future::try_join_all(reader_handles).await;

    db.close().await.unwrap();
}
