use crate::args::{parse_args, CliArgs, CliCommands};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin;
use slatedb::admin::{list_checkpoints, list_manifests, read_manifest};
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::db::Db;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    match args.command {
        CliCommands::ReadManifest { id } => exec_read_manifest(&path, object_store, id).await?,
        CliCommands::ListManifests { start, end } => {
            exec_list_manifest(&path, object_store, start, end).await?
        }
        CliCommands::CreateCheckpoint { lifetime, source } => {
            exec_create_checkpoint(&path, object_store, lifetime, source).await?
        }
        CliCommands::RefreshCheckpoint { id, lifetime } => {
            exec_refresh_checkpoint(&path, object_store, id, lifetime).await?
        }
        CliCommands::DeleteCheckpoint { id } => {
            exec_delete_checkpoint(&path, object_store, id).await?
        }
        CliCommands::ListCheckpoints {} => exec_list_checkpoints(&path, object_store).await?,
    }

    Ok(())
}

async fn exec_read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    match read_manifest(path, object_store, id).await? {
        None => {
            println!("No manifest file found.")
        }
        Some(manifest) => {
            println!("{}", manifest);
        }
    }
    Ok(())
}

async fn exec_list_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let range = match (start, end) {
        (Some(s), Some(e)) => s..e,
        (Some(s), None) => s..u64::MAX,
        (None, Some(e)) => u64::MIN..e,
        _ => u64::MIN..u64::MAX,
    };

    Ok(println!(
        "{}",
        list_manifests(path, object_store, range).await?
    ))
}

async fn exec_create_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    lifetime: Option<Duration>,
    source: Option<Uuid>,
) -> Result<(), Box<dyn Error>> {
    let result = Db::create_checkpoint(
        path,
        object_store,
        &CheckpointOptions {
            scope: CheckpointScope::Durable,
            lifetime,
            source,
        },
    )
    .await?;
    Ok(println!("{:?}", result))
}

async fn exec_refresh_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid,
    lifetime: Option<Duration>,
) -> Result<(), Box<dyn Error>> {
    Ok(println!(
        "{:?}",
        Db::refresh_checkpoint(path, object_store, id, lifetime).await?
    ))
}

async fn exec_delete_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid,
) -> Result<(), Box<dyn Error>> {
    Ok(println!(
        "{:?}",
        Db::delete_checkpoint(path, object_store, id).await?
    ))
}

async fn exec_list_checkpoints(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn Error>> {
    let checkpoint = list_checkpoints(path, object_store).await?;
    let checkpoint_json = serde_json::to_string(&checkpoint)?;
    Ok(println!("{}", checkpoint_json))
}
