use crate::args::BencherArgs;
use args::{BencherCommands, BenchmarkDbArgs};
use clap::Parser;
use db::DbBench;
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin;
use slatedb::config::WriteOptions;
use slatedb::db::Db;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

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
        args.plot,
        db,
    );
    bencher.run().await;
}
