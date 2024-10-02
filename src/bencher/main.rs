use crate::args::BencherArgs;
use args::{BencherCommands, BenchmarkDbArgs};
use clap::Parser;
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin;
use slatedb::error::SlateDBError;
use std::error::Error;
use std::sync::Arc;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = BencherArgs::parse();
    let path = Path::from(args.path);
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    match args.command {
        BencherCommands::Db(subcommand_args) => {
            exec_benchmark_db(path, object_store, subcommand_args).await?
        }
    }

    Ok(())
}

async fn exec_benchmark_db(
    _path: Path,
    _object_store: Arc<dyn ObjectStore>,
    _benchmark_db_args: BenchmarkDbArgs,
) -> Result<(), SlateDBError> {
    Ok(())
}
