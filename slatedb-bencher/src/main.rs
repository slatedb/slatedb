#![allow(clippy::result_large_err)]

use crate::args::BencherArgs;
use args::{
    BencherCommands, BenchmarkCompactionArgs, BenchmarkDbArgs, BenchmarkTransactionArgs,
    CompactionSubcommands, DbMode, KeyGeneratorSupplier,
};
use bytes::Bytes;
use clap::Parser;
use db::{wait_for_compaction, DbBench, ManifestShape, ReadMode};
use futures::StreamExt;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::Error as ObjectStoreError;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::PutResult;
use slatedb::admin;
use slatedb::compaction_execute_bench::CompactionExecuteBench;
use slatedb::config::WriteOptions;
use slatedb::Db;
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use transactions::TransactionBench;

mod args;
pub mod bench_object_store;
pub mod db;
pub mod stats;
pub mod system_monitor;
pub mod transactions;

const CLEANUP_NAME: &str = ".clean_benchmark_data";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_test_writer()
        .init();

    let args = BencherArgs::parse();
    let path = Path::from(args.path);
    let object_store = admin::load_object_store_from_env(args.env_file)?;

    // Start system monitoring in background
    let mut monitor = system_monitor::SystemMonitor::new(Some(tokio::runtime::Handle::current()));
    monitor.start();

    if args.clean {
        create_cleanup_lock(object_store.clone(), &path).await?;
    }

    // With BENCHER_PPROF=<prefix>, sample the CPU at 999 Hz and write
    // <prefix>.svg and <prefix>.folded at the end of the run.
    let pprof_prefix = std::env::var("BENCHER_PPROF").ok();
    let profiler = pprof_prefix.as_ref().map(|_| {
        pprof::ProfilerGuardBuilder::default()
            .frequency(999)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .expect("start pprof")
    });

    match args.command {
        BencherCommands::Db(subcommand_args) => {
            exec_benchmark_db(path.clone(), object_store.clone(), subcommand_args).await;
        }
        BencherCommands::Compaction(subcommand_args) => {
            exec_benchmark_compaction(path.clone(), object_store.clone(), subcommand_args).await;
        }
        BencherCommands::Transaction(subcommand_args) => {
            exec_benchmark_transaction(path.clone(), object_store.clone(), subcommand_args).await;
        }
    }

    monitor.stop();

    if let (Some(prefix), Some(profiler)) = (pprof_prefix, profiler) {
        write_profile(&prefix, &profiler)?;
    }

    if args.clean {
        cleanup_data(object_store, &path).await?;
    }

    Ok(())
}

/// Write the samples of `profiler` as a flamegraph and as folded stacks
/// (one line per stack, root first, then the sample count).
fn write_profile(prefix: &str, profiler: &pprof::ProfilerGuard) -> Result<(), Box<dyn Error>> {
    let report = profiler.report().build()?;
    report.flamegraph(std::fs::File::create(format!("{prefix}.svg"))?)?;
    let mut folded = String::new();
    for (frames, count) in &report.data {
        let mut names: Vec<String> = Vec::new();
        for frame in frames.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                names.push(symbol.name());
            }
        }
        folded.push_str(&format!("{} {}\n", names.join(";"), count));
    }
    std::fs::write(format!("{prefix}.folded"), folded)?;
    info!("wrote pprof profile to {prefix}.svg and {prefix}.folded");
    Ok(())
}

async fn exec_benchmark_db(path: Path, object_store: Arc<dyn ObjectStore>, args: BenchmarkDbArgs) {
    let (mut config, db_cache) = args.db_args.config().await.unwrap();
    if args.no_compactor {
        config.compactor_options = None;
    }
    let write_options = WriteOptions::default();
    let store = args.db_args.wrap_store(object_store);
    let recorder = Arc::new(DefaultMetricsRecorder::new());

    let mut builder = Db::builder(path.clone(), store.clone())
        .with_settings(config)
        .with_metrics_recorder(recorder.clone());

    if let Some(cache) = &db_cache {
        builder = builder.with_db_cache(cache.clone(), 0);
    }

    let db = Arc::new(builder.build().await.unwrap());
    ManifestShape::of(&db).log();
    let read_mode = match &args.mode {
        None => ReadMode::Get,
        Some(DbMode::Mget(mget)) => {
            info!(reader = %mget.reader, batch_size = mget.batch_size, "using batch reads");
            ReadMode::Mget {
                reader: mget.reader,
                batch_size: mget.batch_size,
                options: mget.mget_options(),
            }
        }
    };
    let bencher = DbBench::new(
        args.key_gen_supplier(),
        args.val_len,
        write_options,
        args.await_durable,
        args.concurrency,
        args.num_rows,
        args.duration.map(|d| Duration::from_secs(d as u64)),
        args.put_percentage,
        args.get_hit_percentage,
        read_mode,
        db.clone(),
        store,
        recorder,
    );
    bencher.run().await;

    if args.wait_compaction {
        wait_for_compaction(&db, Duration::from_secs(30 * 60)).await;
    }
    db.close().await.expect("failed to close db");
    // The benchmark owns the cache, so SlateDB does not close it. A close
    // moves the memory tier of a hybrid cache to disk, so the next run over
    // the same directory starts warm.
    if let Some(cache) = db_cache {
        cache.close().await.expect("failed to close the db cache");
    }
}

async fn exec_benchmark_compaction(
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    args: BenchmarkCompactionArgs,
) {
    let compaction_execute_bench = CompactionExecuteBench::new(path, object_store);
    match args.subcommand {
        CompactionSubcommands::Load(load_args) => {
            compaction_execute_bench
                .run_load(
                    load_args.num_ssts,
                    load_args.sst_bytes,
                    load_args.key_bytes,
                    load_args.val_bytes,
                    load_args.compression_codec,
                )
                .await
                .expect("failed to run load");
        }
        CompactionSubcommands::Run(run_args) => {
            compaction_execute_bench
                .run_bench(
                    run_args.num_ssts,
                    run_args.compaction_sources,
                    run_args.compaction_destination,
                    run_args.compression_codec,
                )
                .await
                .expect("failed to run bench");
        }
        CompactionSubcommands::Clear(clear_args) => {
            compaction_execute_bench
                .run_clear(clear_args.num_ssts)
                .await
                .expect("failed to run clear");
        }
    }
}

async fn exec_benchmark_transaction(
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    args: BenchmarkTransactionArgs,
) {
    let (config, db_cache) = args.db_args.config().await.unwrap();
    let write_options = WriteOptions::default();
    let store = args.db_args.wrap_store(object_store);

    let mut builder = Db::builder(path.clone(), store).with_settings(config);

    if let Some(db_cache) = db_cache {
        builder = builder.with_db_cache(db_cache, 0);
    }

    let db = Arc::new(builder.build().await.unwrap());

    let bencher = TransactionBench::new(
        args.key_gen_supplier(),
        args.val_len,
        write_options,
        args.await_durable,
        args.concurrency,
        args.duration.map(|d| Duration::from_secs(d as u64)),
        args.transaction_size,
        args.abort_percentage,
        args.use_write_batch,
        args.isolation_level,
        db.clone(),
    );
    bencher.run().await;

    db.close().await.expect("failed to close db");
}

/// Creates a lock file that's used as a signal to clean up test data.
async fn create_cleanup_lock(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<PutResult, ObjectStoreError> {
    if (object_store.list(Some(path)).next().await.transpose()?).is_some() {
        warn!("path is not empty but `--clean` is set. failing since cleanup could cause data loss. [path={}]", path);
        return Err(ObjectStoreError::Generic {
            store: "local",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("path {} is not empty", path),
            )),
        });
    }

    let temp_path = path.clone().join(CLEANUP_NAME);
    info!("creating cleanup lock file [path={}]", temp_path);
    object_store
        .put(
            &temp_path,
            PutPayload::from_bytes(Bytes::from(format!("{}", chrono::Utc::now()))),
        )
        .await
}

/// Cleans up test data if a temporary lock file exists.
async fn cleanup_data(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<(), Box<dyn Error>> {
    let temp_path = path.clone().join(CLEANUP_NAME);
    if object_store.head(&temp_path).await.is_ok() {
        info!("cleaning up test data [path={}]", path);
        if let Err(e) = delete_objects_with_prefix(object_store.clone(), Some(path)).await {
            error!("error cleaning up test data [path={}, error={}]", path, e);
        }
    } else {
        warn!(
            "cleanup lock file not found. skipping cleanup to prevent data corruption. [path={}]",
            temp_path
        );
    }
    Ok(())
}

/// Deletes all objects with the specified prefix. This includes all
/// "subdirectories" objects, since object stores are not hierarchical.
async fn delete_objects_with_prefix(
    object_store: Arc<dyn ObjectStore>,
    maybe_prefix: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    let stream = object_store
        .list(maybe_prefix)
        .map_ok(|m| m.location)
        .boxed();
    object_store
        .delete_stream(stream)
        .try_collect::<Vec<Path>>()
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}
