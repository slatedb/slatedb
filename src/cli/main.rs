use crate::args::{parse_args, CliArgs, CliCommands, GcResource, GcSchedule};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin;
use slatedb::admin::{list_checkpoints, list_manifests, read_manifest, run_gc_instance};
use slatedb::config::GcExecutionMode::{Once, Periodic};
use slatedb::config::{
    CheckpointOptions, CheckpointScope, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::db::Db;
use std::error::Error;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
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
        CliCommands::RunGarbageCollection { resource, min_age } => {
            exec_gc_once(&path, object_store, resource, min_age).await?
        }
        CliCommands::ScheduleGarbageCollection {
            manifest,
            wal,
            compacted,
        } => schedule_gc(&path, object_store, manifest, wal, compacted).await?,
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

async fn exec_gc_once(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    resource: GcResource,
    min_age: Duration,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(min_age: Duration) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            execution_mode: Once,
            min_age,
        })
    }
    let gc_opts = match resource {
        GcResource::Manifest => GarbageCollectorOptions {
            manifest_options: create_gc_dir_opts(min_age),
            wal_options: None,
            compacted_options: None,
            ..GarbageCollectorOptions::default()
        },
        GcResource::Wal => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: create_gc_dir_opts(min_age),
            compacted_options: None,
            ..GarbageCollectorOptions::default()
        },
        GcResource::Compacted => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            compacted_options: create_gc_dir_opts(min_age),
            ..GarbageCollectorOptions::default()
        },
    };
    let (stats, _collector) = run_gc_instance(path, object_store, gc_opts).await?;

    match resource {
        GcResource::Manifest => {
            println!(
                "Collected {} manifests",
                stats.gc_manifest_count.value.load(Ordering::SeqCst)
            );
        }
        GcResource::Wal => {
            println!(
                "Collected {} wals",
                stats.gc_wal_count.value.load(Ordering::SeqCst)
            );
        }
        GcResource::Compacted => {
            println!(
                "Collected {} compacted SSTs",
                stats.gc_compacted_count.value.load(Ordering::SeqCst)
            );
        }
    }

    Ok(())
}

async fn schedule_gc(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    manifest_schedule: Option<GcSchedule>,
    wal_schedule: Option<GcSchedule>,
    compacted_schedule: Option<GcSchedule>,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(schedule: GcSchedule) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            execution_mode: Periodic(schedule.period),
            min_age: schedule.min_age,
        })
    }
    let gc_opts = GarbageCollectorOptions {
        manifest_options: manifest_schedule.and_then(create_gc_dir_opts),
        wal_options: wal_schedule.and_then(create_gc_dir_opts),
        compacted_options: compacted_schedule.and_then(create_gc_dir_opts),
        ..GarbageCollectorOptions::default()
    };
    let (stats, _collector) = run_gc_instance(path, object_store, gc_opts).await?;

    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        info!(
            "Collected {:?} Manifests, {:?} WALs, {:?} Compacted SSTs",
            stats.gc_manifest_count.value.load(Ordering::SeqCst),
            stats.gc_wal_count.value.load(Ordering::SeqCst),
            stats.gc_compacted_count.value.load(Ordering::SeqCst)
        );
    }
}
