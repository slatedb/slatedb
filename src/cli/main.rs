use crate::args::{parse_args, CliArgs, CliCommands};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin;
use slatedb::admin::read_manifest;
use std::error::Error;
use std::sync::Arc;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    match args.command {
        CliCommands::ReadManifest => exec_read_manifest(&path, object_store).await?,
    }

    Ok(())
}

async fn exec_read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn Error>> {
    match read_manifest(path, object_store).await? {
        None => {
            println!("No manifest file found.")
        }
        Some(manifest) => {
            println!("{}", manifest);
        }
    }
    Ok(())
}
