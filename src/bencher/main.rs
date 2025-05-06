#![doc = include_str!("README.md")]
#![allow(clippy::result_large_err)]

use crate::args::BencherArgs;
use args::{BencherCommands, BenchmarkCompactionArgs, BenchmarkDbArgs, CompactionSubcommands};
use bytes::Bytes;
use clap::Parser;
use db::DbBench;
use futures::StreamExt;
use object_store::path::Path;
use object_store::Error as ObjectStoreError;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::PutResult;
use slatedb::admin;
use slatedb::compaction_execute_bench::CompactionExecuteBench;
use slatedb::config::WriteOptions;
use slatedb::Db;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

mod args;
mod db;

const CLEANUP_NAME: &str = ".clean_benchmark_data";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args = BencherArgs::parse();
    let path = Path::from(args.path);
    let object_store = admin::load_object_store_from_env(args.env_file)?;

    if args.clean {
        create_cleanup_lock(object_store.clone(), &path).await?;
    }

    match args.command {
        BencherCommands::Db(subcommand_args) => {
            exec_benchmark_db(path.clone(), object_store.clone(), subcommand_args).await;
        }
        BencherCommands::Compaction(subcommand_args) => {
            exec_benchmark_compaction(path.clone(), object_store.clone(), subcommand_args).await;
        }
    }

    if args.clean {
        cleanup_data(object_store, &path).await?;
    }

    Ok(())
}

async fn exec_benchmark_db(path: Path, object_store: Arc<dyn ObjectStore>, args: BenchmarkDbArgs) {
    let (config, block_cache) = args.db_args.config().unwrap();
    let write_options = WriteOptions {
        await_durable: args.await_durable,
    };

    let mut builder = Db::builder(path.clone(), object_store.clone()).with_settings(config);

    if let Some(block_cache) = block_cache {
        builder = builder.with_block_cache(block_cache);
    }

    let db = Arc::new(builder.build().await.unwrap());
    let bencher = DbBench::new(
        args.key_gen_supplier(),
        args.val_len,
        write_options,
        args.concurrency,
        args.num_rows,
        args.duration.map(|d| Duration::from_secs(d as u64)),
        args.put_percentage,
        db.clone(),
    );
    bencher.run().await;

    db.close().await.expect("Failed to close db");
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
                .expect("Failed to run load");
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
                .expect("Failed to run bench");
        }
        CompactionSubcommands::Clear(clear_args) => {
            compaction_execute_bench
                .run_clear(clear_args.num_ssts)
                .await
                .expect("Failed to run clear");
        }
    }
}

/// Creates a lock file that's used as a signal to clean up test data.
async fn create_cleanup_lock(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<PutResult, ObjectStoreError> {
    if (object_store.list(Some(path)).next().await.transpose()?).is_some() {
        warn!("Path {} is not empty but `--clean` is set. Failing since cleanup could cause data loss.", path);
        return Err(ObjectStoreError::Generic {
            store: "local",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Path {} is not empty", path),
            )),
        });
    }

    let temp_path = path.child(CLEANUP_NAME);
    info!("Creating cleanup lock file at: {}", temp_path);
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
    let temp_path = path.child(CLEANUP_NAME);
    if object_store.head(&temp_path).await.is_ok() {
        info!("Cleaning up test data in: {}", path);
        if let Err(e) = admin::delete_objects_with_prefix(object_store.clone(), Some(path)).await {
            error!("Error cleaning up test data: {}", e);
        }
    } else {
        warn!(
            "Cleanup lock file not found at {}. Skipping cleanup to prevent data corruption.",
            temp_path
        );
    }
    Ok(())
}
