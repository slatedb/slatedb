use crate::args::{parse_args, CliArgs, CliCommands, GcResource, GcSchedule};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin::{
    self, list_checkpoints, list_manifests, read_manifest, run_gc_in_background, run_gc_once,
};
use slatedb::config::{
    CheckpointOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::db_context::DbContext;
use slatedb::Db;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    let cancellation_token = CancellationToken::new();

    let ct = cancellation_token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        debug!("Intercepted SIGINT ... shutting down background processes");
        // if we cant send a shutdown message it's probably because it's already closed
        ct.cancel();
    });

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
        CliCommands::RunGarbageCollection { resource, min_age } => {
            exec_gc_once(&path, object_store, resource, min_age).await?
        }
        CliCommands::ScheduleGarbageCollection {
            manifest,
            wal,
            compacted,
        } => {
            schedule_gc(&path, object_store, manifest, wal, compacted, cancellation_token)
                .await?
        }
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

    println!("{}", list_manifests(path, object_store, range).await?);
    Ok(())
}

async fn exec_create_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    lifetime: Option<Duration>,
    source: Option<Uuid>,
) -> Result<(), Box<dyn Error>> {
    let result = admin::create_checkpoint(
        path.clone(),
        object_store,
        &CheckpointOptions { lifetime, source },
        None,
    )
    .await?;
    println!("{:?}", result);
    Ok(())
}

async fn exec_refresh_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid,
    lifetime: Option<Duration>,
) -> Result<(), Box<dyn Error>> {
    println!(
        "{:?}",
        Db::refresh_checkpoint(path, object_store, id, lifetime, None).await?
    );
    Ok(())
}

async fn exec_delete_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid,
) -> Result<(), Box<dyn Error>> {
    println!(
        "{:?}",
        Db::delete_checkpoint(path, object_store, id, None).await?
    );
    Ok(())
}

async fn exec_list_checkpoints(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn Error>> {
    let checkpoint = list_checkpoints(path, object_store).await?;
    let checkpoint_json = serde_json::to_string(&checkpoint)?;
    println!("{}", checkpoint_json);
    Ok(())
}

async fn exec_gc_once(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    resource: GcResource,
    min_age: Duration,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(min_age: Duration) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age,
        })
    }
    let gc_opts = match resource {
        GcResource::Manifest => GarbageCollectorOptions {
            manifest_options: create_gc_dir_opts(min_age),
            wal_options: None,
            compacted_options: None,
        },
        GcResource::Wal => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: create_gc_dir_opts(min_age),
            compacted_options: None,
        },
        GcResource::Compacted => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            compacted_options: create_gc_dir_opts(min_age),
        },
    };
    run_gc_once(path, object_store, gc_opts, None).await?;
    Ok(())
}

async fn schedule_gc(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    manifest_schedule: Option<GcSchedule>,
    wal_schedule: Option<GcSchedule>,
    compacted_schedule: Option<GcSchedule>,
    cancellation_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(schedule: GcSchedule) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            interval: Some(schedule.period),
            min_age: schedule.min_age,
        })
    }
    let gc_opts = GarbageCollectorOptions {
        manifest_options: manifest_schedule.and_then(create_gc_dir_opts),
        wal_options: wal_schedule.and_then(create_gc_dir_opts),
        compacted_options: compacted_schedule.and_then(create_gc_dir_opts),
    };

    run_gc_in_background(
        path,
        object_store,
        gc_opts,
        cancellation_token,
        None,
    )
    .await?;
    Ok(())
}
