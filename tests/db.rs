use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::config::{DbOptions, PutOptions, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    let config = DbOptions {
        flush_interval: Some(Duration::from_millis(100)),
        manifest_poll_interval: Duration::from_millis(100),
        // Allow 16KB of unflushed data
        max_unflushed_bytes: 16 * 1024,
        min_filter_keys: 0,
        // Allow up to four 4096-byte blocks per-SST
        l0_sst_size_bytes: 4 * 4096,
        ..Default::default()
    };
    let db = Arc::new(
        Db::open_with_opts(
            Path::from("/tmp/test_concurrent_writers_readers"),
            config,
            object_store.clone(),
        )
        .await
        .unwrap(),
    );

    // Flag to signal readers to stop
    let stop = Arc::new(AtomicBool::new(false));

    // Writer tasks: each writer writes to its own key with incrementing values
    let writer_handles = (0..NUM_WRITERS)
        .map(|writer_id| {
            let db = db.clone();
            let stop_writers = stop.clone();
            tokio::spawn(async move {
                // Get a random key in the keyspace. Don't use nth. Pick one at random.
                let key = zero_pad_key(writer_id.try_into().unwrap(), KEY_LENGTH);
                for i in 1..=WRITES_PER_TASK {
                    if stop_writers.load(Ordering::Relaxed) {
                        break;
                    }

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

                    if i % 1000 == 0 {
                        println!("Writer {} wrote {} values", writer_id, i);
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    let stop_flushers = stop.clone();
    let flusher_db = db.clone();
    let flusher = tokio::spawn(async move {
        while !stop_flushers.load(Ordering::Relaxed) {
            flusher_db.flush().await.expect("Failed to flush");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Reader tasks: each reader reads all keys and verifies values are increasing
    let reader_handles = (0..NUM_READERS)
        .map(|reader_id| {
            let db = db.clone();
            let stop_readers = stop.clone();

            tokio::spawn(async move {
                let mut latest_values = HashMap::<usize, AtomicU64>::new();
                let mut iterations = 0;

                while !stop_readers.load(Ordering::Relaxed) {
                    let mut rng = StdRng::from_entropy();
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
                            .entry(key.try_into().unwrap())
                            .or_insert(AtomicU64::new(0));
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

                        if iterations % 1000 == 0 {
                            println!("Reader {} processed {} values. Sample key: {}, last seen value: {}, current value: {}", reader_id, iterations, key, last_seen_value, current_value);
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    loop {
        // Check if any handles are still running.
        let finished_writers = writer_handles
            .iter()
            .filter(|handle| handle.is_finished())
            .count();
        let finished_readers = reader_handles
            .iter()
            .filter(|handle| handle.is_finished())
            .count();
        println!(
            "Finished writers: {}, finished readers: {}",
            finished_writers, finished_readers
        );

        if finished_writers == NUM_WRITERS || finished_readers > 0 {
            stop.store(true, Ordering::Relaxed);
            break;
        }
    }

    // Wait for all readers and writers to complete, and verify none ended with an error using try_join_all
    let all_handles = writer_handles
        .into_iter()
        .chain(reader_handles)
        .chain(vec![flusher]);
    futures::future::try_join_all(all_handles).await.unwrap();
    db.close().await.unwrap();
}
