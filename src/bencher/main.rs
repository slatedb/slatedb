#![doc = include_str!("README.md")]

use crate::args::BencherArgs;
use args::{BencherCommands, BenchmarkCompactionArgs, BenchmarkDbArgs, CompactionSubcommands};
use clap::Parser;
use db::DbBench;
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin;
use slatedb::compaction_execute_bench::CompactionExecuteBench;
use slatedb::config::WriteOptions;
use slatedb::db::Db;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;

mod args;
mod db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args = BencherArgs::parse();
    let path = Path::from(args.path);
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    match args.command {
        BencherCommands::Db(subcommand_args) => {
            exec_benchmark_db(path, object_store, subcommand_args).await;
        }
        BencherCommands::Compaction(subcommand_args) => {
            exec_benchmark_compaction(path, object_store, subcommand_args).await;
        }
    }

    Ok(())
}

async fn exec_benchmark_db(path: Path, object_store: Arc<dyn ObjectStore>, args: BenchmarkDbArgs) {
    let config = args.db_args.config();
    let write_options = WriteOptions {
        await_durable: args.await_durable,
    };
    let db = Arc::new(
        Db::open_with_opts(path, config, object_store)
            .await
            .unwrap(),
    );
    let bencher = DbBench::new(
        args.key_gen_supplier(),
        args.val_len,
        write_options,
        args.concurrency,
        args.num_rows,
        args.duration.map(|d| Duration::from_secs(d as u64)),
        args.put_percentage,
        db,
    );
    bencher.run().await;
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
                .run_clear(clear_args.num_ssts, Handle::current())
                .expect("Failed to run clear");
        }
    }
}
