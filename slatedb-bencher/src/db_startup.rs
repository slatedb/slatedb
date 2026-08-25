use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::config::{CloseOptions, FlushOptions, FlushType};
use slatedb::wal::SlateDbWalReaderOptions;
use slatedb::{Db, Settings, WriteBatch};
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

const GIB: u64 = 1024 * 1024 * 1024;
const DEFAULT_TOTAL_WRITE_SIZE_BYTES: u64 = GIB;
const DEFAULT_WAL_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const MIN_L0_SIZE_BYTES: u64 = 100 * GIB;
const KEY_SIZE_BYTES: u64 = 16;
const VALUE_SIZE_BYTES: u64 = 4096;
const MAX_ROW_LOGICAL_BYTES: u64 = KEY_SIZE_BYTES + VALUE_SIZE_BYTES;
const MAX_BATCH_LOGICAL_BYTES: u64 = 1024 * 1024;

type BenchError = Box<dyn Error + Send + Sync>;

#[derive(Parser)]
#[command(name = "db-startup-bench")]
#[command(about = "Benchmark SlateDB startup after replaying a WAL-only database from S3.")]
struct Args {
    #[arg(long, help = "S3 bucket used for benchmark database files.")]
    bucket: String,

    #[arg(long, help = "AWS region containing the S3 bucket.")]
    region: String,

    #[arg(
        long,
        alias = "path",
        help = "Fresh object path prefix dedicated to this benchmark run."
    )]
    prefix: String,

    #[arg(
        long,
        alias = "total_write_size",
        alias = "total-write-size-bytes",
        default_value_t = DEFAULT_TOTAL_WRITE_SIZE_BYTES,
        help = "Exact logical key/value bytes written before restarting."
    )]
    total_write_size: u64,

    #[arg(
        long,
        alias = "wal_file_size",
        alias = "wal-file-size-bytes",
        default_value_t = DEFAULT_WAL_FILE_SIZE_BYTES,
        help = "Logical key/value bytes written between explicit WAL flushes."
    )]
    wal_file_size: u64,

    #[arg(
        long,
        help = "Skip data generation and only time reopening the existing benchmark prefix."
    )]
    restart_only: bool,

    #[arg(
        long,
        default_value_t = 128,
        help = "Maximum WAL SST fetch tasks shared by startup replay iterators."
    )]
    max_fetch_tasks: usize,

    #[arg(
        long,
        default_value_t = 134_217_728,
        help = "Maximum bytes buffered across startup replay iterators."
    )]
    max_buffered_bytes: usize,

    #[arg(
        long,
        alias = "target-bytes-to-fetch",
        default_value_t = 8_388_608,
        help = "Target read-ahead bytes per WAL SST fetch request."
    )]
    read_ahead_bytes: usize,
}

#[derive(Debug)]
struct StartupBenchResult {
    total_write_size_bytes: u64,
    wal_file_size_bytes: u64,
    row_count: u64,
    wal_flush_count: u64,
    l0_sst_size_bytes: usize,
    write_elapsed: Option<Duration>,
    startup_elapsed: Duration,
}

struct WriteSummary {
    row_count: u64,
    wal_flush_count: u64,
    last_key: Bytes,
    last_value: Bytes,
}

#[tokio::main]
async fn main() -> Result<(), BenchError> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();

    let args = Args::parse();
    let replay_options = SlateDbWalReaderOptions {
        max_buffered_bytes: args.max_buffered_bytes,
        max_fetch_tasks: args.max_fetch_tasks,
        read_ahead_bytes: args.read_ahead_bytes,
    };
    validate_args(
        &args.bucket,
        &args.region,
        &args.prefix,
        args.total_write_size,
        args.wal_file_size,
        &replay_options,
    )?;

    let object_store = AmazonS3Builder::from_env()
        .with_bucket_name(&args.bucket)
        .with_region(&args.region)
        .build()?;
    let result = run_startup_bench(
        Arc::new(object_store),
        Path::from(args.prefix.clone()),
        args.total_write_size,
        args.wal_file_size,
        args.restart_only,
        replay_options,
    )
    .await?;

    println!("bucket={}", args.bucket);
    println!("region={}", args.region);
    println!("prefix={}", args.prefix);
    println!("restart_only={}", args.restart_only);
    println!("total_write_size_bytes={}", result.total_write_size_bytes);
    println!("wal_file_size_bytes={}", result.wal_file_size_bytes);
    println!("max_fetch_tasks={}", args.max_fetch_tasks);
    println!("max_buffered_bytes={}", args.max_buffered_bytes);
    println!("read_ahead_bytes={}", args.read_ahead_bytes);
    println!("rows={}", result.row_count);
    println!("wal_flushes={}", result.wal_flush_count);
    println!("l0_sst_size_bytes={}", result.l0_sst_size_bytes);
    if let Some(write_elapsed) = result.write_elapsed {
        println!("write_elapsed_seconds={:.3}", write_elapsed.as_secs_f64());
        println!(
            "write_throughput_mib_per_second={:.2}",
            throughput_mib_per_second(result.total_write_size_bytes, write_elapsed)
        );
    }
    println!(
        "startup_elapsed_seconds={:.3}",
        result.startup_elapsed.as_secs_f64()
    );
    println!(
        "startup_replay_mib_per_second={:.2}",
        throughput_mib_per_second(result.total_write_size_bytes, result.startup_elapsed)
    );

    Ok(())
}

fn validate_args(
    bucket: &str,
    region: &str,
    prefix: &str,
    total_write_size: u64,
    wal_file_size: u64,
    replay_options: &SlateDbWalReaderOptions,
) -> Result<(), BenchError> {
    if bucket.trim().is_empty() {
        return Err(invalid_input("bucket must not be empty"));
    }
    if region.trim().is_empty() {
        return Err(invalid_input("region must not be empty"));
    }
    if prefix.trim().is_empty() {
        return Err(invalid_input("prefix must not be empty"));
    }
    if total_write_size == 0 {
        return Err(invalid_input("total write size must be greater than zero"));
    }
    if wal_file_size < KEY_SIZE_BYTES {
        return Err(invalid_input(format!(
            "WAL file size must be at least {KEY_SIZE_BYTES} bytes"
        )));
    }
    if replay_options.max_fetch_tasks == 0 {
        return Err(invalid_input("max fetch tasks must be greater than zero"));
    }
    if replay_options.max_buffered_bytes == 0 {
        return Err(invalid_input(
            "max buffered bytes must be greater than zero",
        ));
    }
    if replay_options.read_ahead_bytes == 0 {
        return Err(invalid_input("read-ahead bytes must be greater than zero"));
    }
    Ok(())
}

async fn run_startup_bench(
    object_store: Arc<dyn ObjectStore>,
    prefix: Path,
    total_write_size: u64,
    wal_file_size: u64,
    restart_only: bool,
    replay_options: SlateDbWalReaderOptions,
) -> Result<StartupBenchResult, BenchError> {
    let settings = startup_bench_settings(total_write_size)?;

    let (summary, write_elapsed) = if restart_only {
        ensure_prefix_is_not_empty(object_store.as_ref(), &prefix).await?;
        info!(
            "skipping startup benchmark data generation [prefix={}, total_write_size_bytes={}, wal_file_size_bytes={}]",
            prefix, total_write_size, wal_file_size,
        );
        (
            expected_write_summary(total_write_size, wal_file_size)?,
            None,
        )
    } else {
        ensure_prefix_is_empty(object_store.as_ref(), &prefix).await?;
        info!(
            "creating startup benchmark database [prefix={}, total_write_size_bytes={}, wal_file_size_bytes={}, l0_sst_size_bytes={}, flush_interval={:?}]",
            prefix,
            total_write_size,
            wal_file_size,
            settings.l0_sst_size_bytes,
            settings.flush_interval,
        );
        let db = open_db(
            &prefix,
            Arc::clone(&object_store),
            &settings,
            &replay_options,
        )
        .await?;

        let write_started = Instant::now();
        let summary = write_data(&db, total_write_size, wal_file_size).await?;
        let write_elapsed = write_started.elapsed();

        close_wal_only(&db).await?;
        drop(db);
        (summary, Some(write_elapsed))
    };

    info!(
        "restarting startup benchmark database [prefix={}, wal_flushes={}, rows={}, max_fetch_tasks={}, max_buffered_bytes={}, read_ahead_bytes={}]",
        prefix,
        summary.wal_flush_count,
        summary.row_count,
        replay_options.max_fetch_tasks,
        replay_options.max_buffered_bytes,
        replay_options.read_ahead_bytes,
    );
    let startup_started = Instant::now();
    let reopened = open_db(
        &prefix,
        Arc::clone(&object_store),
        &settings,
        &replay_options,
    )
    .await?;
    let startup_elapsed = startup_started.elapsed();

    let replayed_value = reopened.get(summary.last_key.clone()).await?;
    if replayed_value.as_ref() != Some(&summary.last_value) {
        close_wal_only(&reopened).await?;
        return Err(invalid_data(
            "last written value was not recovered during startup",
        ));
    }
    close_wal_only(&reopened).await?;

    info!(
        "completed startup benchmark [prefix={}, total_write_size_bytes={}, wal_flushes={}, startup_elapsed_seconds={:.3}]",
        prefix,
        total_write_size,
        summary.wal_flush_count,
        startup_elapsed.as_secs_f64(),
    );
    Ok(StartupBenchResult {
        total_write_size_bytes: total_write_size,
        wal_file_size_bytes: wal_file_size,
        row_count: summary.row_count,
        wal_flush_count: summary.wal_flush_count,
        l0_sst_size_bytes: settings.l0_sst_size_bytes,
        write_elapsed,
        startup_elapsed,
    })
}

fn startup_bench_settings(total_write_size: u64) -> Result<Settings, BenchError> {
    let workload_l0_bound = total_write_size
        .checked_mul(2)
        .ok_or_else(|| invalid_input("total write size is too large"))?;
    let l0_sst_size_bytes = MIN_L0_SIZE_BYTES.max(workload_l0_bound);
    let max_unflushed_bytes = l0_sst_size_bytes
        .checked_mul(2)
        .ok_or_else(|| invalid_input("derived unflushed byte limit overflowed"))?;

    let l0_sst_size_bytes = usize::try_from(l0_sst_size_bytes)
        .map_err(|_| invalid_input("derived L0 size does not fit usize"))?;
    let max_unflushed_bytes = usize::try_from(max_unflushed_bytes)
        .map_err(|_| invalid_input("derived unflushed byte limit does not fit usize"))?;
    let settings = Settings {
        wal_enabled: true,
        flush_interval: Some(Duration::MAX),
        l0_sst_size_bytes,
        max_wal_flushes_before_l0_flush: u64::MAX,
        max_unflushed_bytes,
        ..Settings::default()
    };
    settings.validate()?;
    Ok(settings)
}

async fn open_db(
    prefix: &Path,
    object_store: Arc<dyn ObjectStore>,
    settings: &Settings,
    replay_options: &SlateDbWalReaderOptions,
) -> Result<Db, BenchError> {
    Ok(Db::builder(prefix.clone(), object_store)
        .with_settings(settings.clone())
        .with_wal_replay_options(replay_options.clone())
        .build()
        .await?)
}

async fn ensure_prefix_is_empty(
    object_store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<(), BenchError> {
    let mut objects = object_store.list(Some(prefix));
    match objects.next().await {
        Some(Ok(object)) => Err(invalid_input(format!(
            "benchmark prefix {prefix} is not empty; found {}",
            object.location
        ))),
        Some(Err(error)) => Err(error.into()),
        None => Ok(()),
    }
}

async fn ensure_prefix_is_not_empty(
    object_store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<(), BenchError> {
    let mut objects = object_store.list(Some(prefix));
    match objects.next().await {
        Some(Ok(_)) => Ok(()),
        Some(Err(error)) => Err(error.into()),
        None => Err(invalid_input(format!(
            "benchmark prefix {prefix} is empty; restart-only mode requires an existing run"
        ))),
    }
}

async fn write_data(
    db: &Db,
    total_write_size: u64,
    wal_file_size: u64,
) -> Result<WriteSummary, BenchError> {
    let full_value = Bytes::from(vec![0xA5; VALUE_SIZE_BYTES as usize]);
    let mut logical_bytes_written = 0u64;
    let mut row_count = 0u64;
    let mut wal_flush_count = 0u64;
    let mut last_key = None;
    let mut last_value = None;

    while logical_bytes_written < total_write_size {
        let current_wal_bytes = wal_file_size.min(total_write_size - logical_bytes_written);
        write_exact_logical_bytes(
            db,
            current_wal_bytes,
            &full_value,
            &mut row_count,
            &mut last_key,
            &mut last_value,
        )
        .await?;
        logical_bytes_written = logical_bytes_written
            .checked_add(current_wal_bytes)
            .ok_or_else(|| invalid_input("logical byte counter overflowed"))?;

        if logical_bytes_written < total_write_size {
            flush_wal(db).await?;
            wal_flush_count += 1;
            info!(
                "flushed startup benchmark WAL [logical_bytes_written={}, total_write_size_bytes={}, wal_flushes={}]",
                logical_bytes_written, total_write_size, wal_flush_count,
            );
        }
    }

    flush_wal(db).await?;
    wal_flush_count += 1;
    info!(
        "flushed final startup benchmark WAL [logical_bytes_written={}, wal_flushes={}]",
        logical_bytes_written, wal_flush_count,
    );

    Ok(WriteSummary {
        row_count,
        wal_flush_count,
        last_key: last_key.expect("positive write size produces at least one row"),
        last_value: last_value.expect("positive write size produces at least one row"),
    })
}

fn expected_write_summary(
    total_write_size: u64,
    wal_file_size: u64,
) -> Result<WriteSummary, BenchError> {
    let full_value = Bytes::from(vec![0xA5; VALUE_SIZE_BYTES as usize]);
    let mut logical_bytes_summarized = 0u64;
    let mut row_count = 0u64;
    let mut last_key = None;
    let mut last_value = None;

    while logical_bytes_summarized < total_write_size {
        let current_wal_bytes = wal_file_size.min(total_write_size - logical_bytes_summarized);
        let mut wal_remaining = current_wal_bytes;
        while wal_remaining > 0 {
            let batch_bytes = next_piece_size(wal_remaining, MAX_BATCH_LOGICAL_BYTES);
            let mut batch_remaining = batch_bytes;
            while batch_remaining > 0 {
                let row_bytes = next_piece_size(batch_remaining, MAX_ROW_LOGICAL_BYTES);
                let key_size = KEY_SIZE_BYTES.min(row_bytes) as usize;
                let value_size = (row_bytes - key_size as u64) as usize;
                row_count = row_count
                    .checked_add(1)
                    .ok_or_else(|| invalid_input("row counter overflowed"))?;
                last_key = Some(benchmark_key(row_count, key_size));
                last_value = Some(full_value.slice(..value_size));
                batch_remaining -= row_bytes;
            }
            wal_remaining -= batch_bytes;
        }
        logical_bytes_summarized = logical_bytes_summarized
            .checked_add(current_wal_bytes)
            .ok_or_else(|| invalid_input("logical byte counter overflowed"))?;
    }

    Ok(WriteSummary {
        row_count,
        wal_flush_count: total_write_size.div_ceil(wal_file_size),
        last_key: last_key.expect("positive write size produces at least one row"),
        last_value: last_value.expect("positive write size produces at least one row"),
    })
}

async fn write_exact_logical_bytes(
    db: &Db,
    logical_bytes: u64,
    full_value: &Bytes,
    row_count: &mut u64,
    last_key: &mut Option<Bytes>,
    last_value: &mut Option<Bytes>,
) -> Result<(), BenchError> {
    let mut remaining = logical_bytes;
    while remaining > 0 {
        let batch_bytes = next_piece_size(remaining, MAX_BATCH_LOGICAL_BYTES);
        let mut batch_remaining = batch_bytes;
        let mut batch = WriteBatch::new();

        while batch_remaining > 0 {
            let row_bytes = next_piece_size(batch_remaining, MAX_ROW_LOGICAL_BYTES);
            let key_size = KEY_SIZE_BYTES.min(row_bytes) as usize;
            let value_size = (row_bytes - key_size as u64) as usize;
            *row_count = row_count
                .checked_add(1)
                .ok_or_else(|| invalid_input("row counter overflowed"))?;
            let key = benchmark_key(*row_count, key_size);
            let value = full_value.slice(..value_size);
            batch.put_bytes(key.clone(), value.clone());
            *last_key = Some(key);
            *last_value = Some(value);
            batch_remaining -= row_bytes;
        }

        let _write_handle = db.write(batch).await?;
        remaining -= batch_bytes;
    }
    Ok(())
}

fn next_piece_size(remaining: u64, maximum: u64) -> u64 {
    let mut size = remaining.min(maximum);
    let tail = remaining - size;
    if tail > 0 && tail < KEY_SIZE_BYTES {
        size -= KEY_SIZE_BYTES - tail;
    }
    size
}

async fn flush_wal(db: &Db) -> Result<(), BenchError> {
    Ok(db
        .flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await?)
}

async fn close_wal_only(db: &Db) -> Result<(), BenchError> {
    Ok(db
        .close_with_options(CloseOptions::default().with_flush_type(Some(FlushType::Wal)))
        .await?)
}

fn benchmark_key(seq: u64, key_size: usize) -> Bytes {
    let first = splitmix64(seq);
    let second = splitmix64(first);
    let mut key = [0u8; KEY_SIZE_BYTES as usize];
    key[..8].copy_from_slice(&first.to_be_bytes());
    key[8..].copy_from_slice(&second.to_be_bytes());
    Bytes::copy_from_slice(&key[..key_size])
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn throughput_mib_per_second(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return f64::INFINITY;
    }
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

fn invalid_input(message: impl Into<String>) -> BenchError {
    IoError::new(ErrorKind::InvalidInput, message.into()).into()
}

fn invalid_data(message: impl Into<String>) -> BenchError {
    IoError::new(ErrorKind::InvalidData, message.into()).into()
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use object_store::memory::InMemory;

    use super::*;

    const MAX_FETCH_TASKS_FOR_RESTART_TEST: usize = 1024;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_only_wals_and_replays_them_during_startup() {
        let object_store = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = object_store.clone();
        let prefix = Path::from("db-startup-bench-test");
        let total_write_size = 256 * 1024 + 7;
        let wal_file_size = 64 * 1024;

        let result = run_startup_bench(
            Arc::clone(&store),
            prefix.clone(),
            total_write_size,
            wal_file_size,
            false,
            SlateDbWalReaderOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.total_write_size_bytes, total_write_size);
        assert_eq!(result.wal_file_size_bytes, wal_file_size);
        assert_eq!(
            result.wal_flush_count,
            total_write_size.div_ceil(wal_file_size)
        );
        assert!(result.row_count > 0);
        assert!(result.write_elapsed.is_some());

        let objects: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
        let locations: Vec<_> = objects
            .iter()
            .map(|object| object.location.as_ref())
            .collect();
        assert!(locations.iter().any(|location| location.contains("/wal/")));
        assert!(
            !locations
                .iter()
                .any(|location| location.contains("/compacted/")),
            "unexpected L0 object in {locations:?}"
        );

        let restart_result = run_startup_bench(
            Arc::clone(&store),
            prefix,
            total_write_size,
            wal_file_size,
            true,
            SlateDbWalReaderOptions {
                max_fetch_tasks: MAX_FETCH_TASKS_FOR_RESTART_TEST,
                ..SlateDbWalReaderOptions::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(restart_result.row_count, result.row_count);
        assert_eq!(restart_result.wal_flush_count, result.wal_flush_count);
        assert!(restart_result.write_elapsed.is_none());
    }

    #[test]
    fn validates_sizes() {
        let options = SlateDbWalReaderOptions::default();
        assert!(validate_args("bucket", "region", "prefix", 0, 64, &options).is_err());
        assert!(validate_args("bucket", "region", "prefix", 64, 15, &options).is_err());
        assert!(validate_args("bucket", "region", "prefix", 64, 16, &options).is_ok());

        let invalid_options = SlateDbWalReaderOptions {
            max_fetch_tasks: 0,
            ..options.clone()
        };
        assert!(validate_args("bucket", "region", "prefix", 64, 16, &invalid_options).is_err());
    }

    #[tokio::test]
    async fn restart_only_requires_an_existing_prefix() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let error = run_startup_bench(
            store,
            Path::from("empty-prefix"),
            64,
            16,
            true,
            SlateDbWalReaderOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("restart-only mode requires an existing run"));
    }
}
