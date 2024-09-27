use clap::{command, Arg, ArgAction};
use object_store::aws::{DynamoCommit, S3ConditionalPut};
use object_store::path::Path;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use slatedb::config::ReadLevel::Uncommitted;
use slatedb::config::{DbOptions, ObjectStoreCacheOptions, ReadOptions, WriteOptions};
use slatedb::db::Db;
use slatedb::inmemory_cache::InMemoryCacheOptions;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::remove_dir_all;
use ulid::Ulid;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::main]
async fn main() {
    let (params, options, object_store) = configure();

    let db_name = Ulid::new();
    let s3_db_path = Path::from(format!("s3-bench/{}", db_name));
    if !params.plot {
        println!("Creating database: {}", s3_db_path);
    }
    let db = Db::open_with_opts(
        s3_db_path.clone(),
        options.clone(),
        Arc::clone(&object_store),
    )
    .await
    .expect("failed to create database");

    if params.prepopulate_percentage != 0 {
        if !params.plot {
            println!(
                "Prepopulating database with {}% of keys",
                params.prepopulate_percentage
            );
        }

        let mut value = vec![0; params.value_size];
        rand::thread_rng().fill_bytes(value.as_mut_slice());
        let seed = rand::thread_rng().next_u32();

        let mut inserted = 0;
        for key in 0..params.key_count {
            if (key ^ seed) % 100 < params.prepopulate_percentage {
                db.put_with_options(
                    &key.to_be_bytes(),
                    value.as_slice(),
                    &WriteOptions {
                        await_durable: false,
                    },
                )
                .await;
                inserted += 1;
            }
        }

        if !params.plot {
            println!("Database prepopulated with {} key(s)", inserted);
        }
    }

    let counters = (0..params.concurrency)
        .map(|_| CounterPair {
            puts: AtomicU64::new(0),
            gets: AtomicU64::new(0),
        })
        .collect();
    let state = Arc::new(State { db, counters });
    let mut snapshots: Vec<_> = (0..params.concurrency)
        .map(|_| SnapshotPair { puts: 0, gets: 0 })
        .collect();

    let tasks: Vec<_> = (0..params.concurrency)
        .map(|id| {
            let params = params.clone();
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                run(id, params, state).await;
            })
        })
        .collect();

    let start = Instant::now();
    let mut last_sample = start;
    while start.elapsed() < params.duration {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let elapsed_since_last_sample = last_sample.elapsed();
        if elapsed_since_last_sample >= SAMPLE_INTERVAL {
            last_sample = Instant::now();

            let mut puts = 0u64;
            let mut gets = 0u64;

            #[allow(clippy::needless_range_loop)]
            for i in 0..params.concurrency {
                let v = state.counters[i].puts.load(Ordering::Relaxed);
                puts += v - snapshots[i].puts;
                snapshots[i].puts = v;

                let v = state.counters[i].gets.load(Ordering::Relaxed);
                gets += v - snapshots[i].gets;
                snapshots[i].gets = v;
            }

            let elapsed = elapsed_since_last_sample.as_secs_f64();
            if params.plot {
                println!(
                    "{:.3} {:.3}",
                    // XXX: min is here to align the last sample, so the plot looks nice
                    f64::min(start.elapsed().as_secs_f64(), params.duration.as_secs_f64()),
                    (puts + gets) as f64 / elapsed
                );
            } else {
                println!(
                    "{:.3} put/s + {:.3} get/s = {:.3} op/s, {:.3} s elapsed",
                    puts as f64 / elapsed,
                    gets as f64 / elapsed,
                    (puts + gets) as f64 / elapsed,
                    elapsed
                );
            }
        }
    }

    futures::future::join_all(tasks).await;

    state.db.close().await.expect("failed to close database");

    if let Some(object_cache_root) = options.object_store_cache_options.root_folder {
        let db_cache_path = format!("{}/{}", object_cache_root.display(), s3_db_path);
        let result = remove_dir_all(std::path::Path::new(&db_cache_path)).await;
        if let Err(e) = result {
            println!("Failed to delete object cache at {}: {}", db_cache_path, e);
        }
    }
}

async fn run(id: usize, params: Params, state: Arc<State>) {
    let mut random = StdRng::from_entropy();
    let mut value = vec![0; params.value_size];
    random.fill_bytes(value.as_mut_slice());

    let mut puts = 0u64;
    let mut gets = 0u64;
    let start = Instant::now();
    let mut last_report = start;
    while start.elapsed() < params.duration {
        let elapsed_since_last_report = last_report.elapsed();
        if elapsed_since_last_report >= REPORT_INTERVAL {
            last_report = Instant::now();
            state.counters[id].puts.fetch_add(puts, Ordering::Relaxed);
            state.counters[id].gets.fetch_add(gets, Ordering::Relaxed);
            puts = 0;
            gets = 0;
        }

        let key: u32 = random.gen_range(0..params.key_count);
        if random.gen_range(0..100) < params.put_percentage {
            puts += 1;
            state
                .db
                .put_with_options(
                    &key.to_be_bytes(),
                    value.as_slice(),
                    &WriteOptions {
                        await_durable: false,
                    },
                )
                .await;
        } else {
            gets += 1;
            state
                .db
                .get_with_options(
                    &key.to_be_bytes(),
                    &ReadOptions {
                        read_level: Uncommitted,
                    },
                )
                .await
                .expect("failed to get a key");
        }
    }
}

struct State {
    db: Db,
    counters: Vec<CounterPair>,
}

struct CounterPair {
    puts: AtomicU64,
    gets: AtomicU64,
}

struct SnapshotPair {
    puts: u64,
    gets: u64,
}

#[derive(Clone)]
struct Params {
    duration: Duration,
    concurrency: usize,
    key_count: u32,
    value_size: usize,
    put_percentage: u32,
    prepopulate_percentage: u32,
    plot: bool,
}

fn configure() -> (Params, DbOptions, Arc<dyn ObjectStore>) {
    let default_block_cache_capacity: &'static str = InMemoryCacheOptions::default()
        .max_capacity
        .to_string()
        .leak();
    let default_block_cache_block_size: &'static str = InMemoryCacheOptions::default()
        .cached_block_size
        .to_string()
        .leak();
    let default_object_cache_part_size: &'static str = ObjectStoreCacheOptions::default()
        .part_size_bytes
        .to_string()
        .leak();
    let default_object_cache_location = "target/s3-bench/cache";

    let args = command!()
        .name("s3-bench")
        .about(
            "

Runs SlateDB S3 benchmarks

The following environment variables must be configured externally:
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - AWS_REGION
  - AWS_S3_BUCKET
  - AWS_DYNAMODB_TABLE",
        )
        .arg(
            Arg::new("duration")
                .long("duration")
                .value_parser(clap::value_parser!(u64))
                .default_value("60")
                .help("Sets benchmarking duration in seconds"),
        )
        .arg(
            Arg::new("concurrency")
                .long("concurrency")
                .value_parser(clap::value_parser!(usize))
                .default_value("1")
                .help("Sets the maximum number of operations performed simultaneously"),
        )
        .arg(
            Arg::new("key-count")
                .long("key-count")
                .value_parser(clap::value_parser!(u32))
                .default_value("100000")
                .help("Sets the number of keys to benchmark with"),
        )
        .arg(
            Arg::new("value-size")
                .long("value-size")
                .value_parser(clap::value_parser!(usize))
                .default_value("256")
                .help("Sets the value size"),
        )
        .arg(
            Arg::new("put-percentage")
                .long("put-percentage")
                .value_parser(clap::value_parser!(u32).range(0..=100))
                .default_value("20")
                .help("Sets the percentage of put operations"),
        )
        .arg(
            Arg::new("prepopulate")
                .long("prepopulate")
                .value_parser(clap::value_parser!(u32).range(0..=100))
                .default_value("0")
                .help("Prepopulates database with the given percentage of keys"),
        )
        .arg(
            Arg::new("plot")
                .long("plot")
                .action(ArgAction::SetTrue)
                .help("Enables output format suitable for gnuplot"),
        )
        .arg(
            Arg::new("no-wal")
                .long("no-wal")
                .action(ArgAction::SetTrue)
                .help("Disables WAL"),
        )
        .arg(
            Arg::new("block-cache")
                .long("block-cache")
                .num_args(0..=2)
                .value_names(["CAPACITY", "BLOCK_SIZE"])
                .value_parser(clap::value_parser!(u64))
                .default_missing_values([
                    default_block_cache_capacity,
                    default_block_cache_block_size,
                ])
                .help("Enables block cache and optionally configures its capacity and block size"),
        )
        .arg(
            Arg::new("object-cache")
                .long("object-cache")
                .num_args(0..=2)
                .value_names(["PART_SIZE", "PATH"])
                .default_missing_values([
                    default_object_cache_part_size,
                    default_object_cache_location,
                ])
                .help("Enables object cache and optionally configures its part size and location"),
        )
        .get_matches();

    let params = Params {
        duration: Duration::from_secs(*args.get_one::<u64>("duration").unwrap()),
        concurrency: *args.get_one::<usize>("concurrency").unwrap(),
        key_count: *args.get_one::<u32>("key-count").unwrap(),
        value_size: *args.get_one::<usize>("value-size").unwrap(),
        put_percentage: *args.get_one::<u32>("put-percentage").unwrap(),
        prepopulate_percentage: *args.get_one::<u32>("prepopulate").unwrap(),
        plot: *args.get_one::<bool>("plot").unwrap(),
    };

    let mut options = DbOptions {
        wal_enabled: !args.get_one::<bool>("no-wal").unwrap(),
        ..Default::default()
    };

    if let Some(values) = args.get_many::<u64>("block-cache") {
        let values: Vec<u64> = values.copied().collect();
        let block_cache_options = InMemoryCacheOptions {
            max_capacity: values[0],
            cached_block_size: *(values
                .get(1)
                .unwrap_or(&(InMemoryCacheOptions::default().cached_block_size as u64)))
                as u32,
            ..Default::default()
        };
        options.block_cache_options = Some(block_cache_options);
    } else {
        options.block_cache_options = None;
    }

    if let Some(values) = args.get_many::<String>("object-cache") {
        let values: Vec<String> = values.cloned().collect();
        let default_location = default_object_cache_location.to_string();
        let location = values.get(1).unwrap_or(&default_location);

        let object_cache_options = ObjectStoreCacheOptions {
            part_size_bytes: values[0].parse().unwrap(),
            root_folder: Some(PathBuf::from(location)),
        };
        options.object_store_cache_options = object_cache_options;
    } else {
        options.object_store_cache_options.root_folder = None;
    }

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_access_key_id(
                std::env::var("AWS_ACCESS_KEY_ID").expect("missing AWS_ACCESS_KEY_ID"),
            )
            .with_secret_access_key(
                std::env::var("AWS_SECRET_ACCESS_KEY").expect("missing AWS_SECRET_ACCESS_KEY"),
            )
            .with_region(std::env::var("AWS_REGION").expect("missing AWS_REGION"))
            .with_bucket_name(std::env::var("AWS_S3_BUCKET").expect("missing AWS_S3_BUCKET"))
            .with_conditional_put(S3ConditionalPut::Dynamo(DynamoCommit::new(
                std::env::var("AWS_DYNAMODB_TABLE").expect("missing AWS_DYNAMODB_TABLE"),
            )))
            .build()
            .expect("failed to build S3 config"),
    );

    (params, options, object_store)
}
