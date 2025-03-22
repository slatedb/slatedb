use slatedb::object_store::ObjectStore;
use slatedb::object_store::path::Path;
use slatedb::object_store::memory::InMemory;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering, AtomicU64};
use std::time::Duration;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use slatedb::Db;
use slatedb::config::{DbOptions, WriteOptions, PutOptions};
use futures::future::join_all;
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_writers_and_readers() {
    const NUM_WRITERS: usize = 10;
    const NUM_READERS: usize = 2;
    const WRITES_PER_TASK: usize = 10000;

    // Create an InMemory object store and DB
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = DbOptions {
        flush_interval: Some(Duration::from_millis(100)),
        manifest_poll_interval: Duration::from_millis(100),
        max_unflushed_bytes: 4 * 1024,
        min_filter_keys: 0,
        l0_sst_size_bytes: 1024,
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
    let stop_readers = Arc::new(AtomicBool::new(false));

    // Writer tasks: each writer writes to its own key with incrementing values
    let writer_handles = (0..NUM_WRITERS)
        .map(|writer_id| {
            let db = db.clone();
            tokio::spawn(async move {
                // Get a random key in the keyspace. Don't use nth. Pick one at random.
                let key = writer_id.to_be_bytes();
                for i in 1..=WRITES_PER_TASK {
                    // Write the incremented value
                    db.put_with_options(
                        key.as_ref(),
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

    let stop_flushers = stop_readers.clone();
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
            let stop_readers = stop_readers.clone();

            tokio::spawn(async move {
                let mut latest_values = HashMap::<usize, AtomicU64>::new();
                let mut iterations = 0;

                while !stop_readers.load(Ordering::Relaxed) {
                    let mut rng = StdRng::from_entropy();
                    // Pick a random key and validate that it's higher than the last value for that key
                    let key = rng.gen_range(0..NUM_WRITERS);
                    if let Some(bytes) = db
                        .get(key.to_be_bytes().as_ref())
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

    // Wait for all writers to complete
    join_all(writer_handles).await;

    // Signal readers to stop, wait for them to complete, and clean up
    stop_readers.store(true, Ordering::Relaxed);
    join_all(reader_handles).await;
    flusher.await.unwrap();
    db.close().await.unwrap();
}
