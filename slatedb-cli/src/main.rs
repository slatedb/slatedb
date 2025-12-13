use crate::args::{parse_args, CliArgs, CliCommands, GcResource, GcSchedule};
use chrono::{TimeZone, Utc};
use object_store::path::Path;
use slatedb::admin::{self, Admin, AdminBuilder};
use slatedb::config::{
    CheckpointOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::FindOption;
use std::error::Error;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_test_writer()
        .init();

    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    let cancellation_token = CancellationToken::new();
    let admin = AdminBuilder::new(path, object_store).build();

    let ct = cancellation_token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C signal handler");
        debug!("intercepted SIGINT ... shutting down background processes");
        // if we cant send a shutdown message it's probably because it's already closed
        ct.cancel();
    });

    match args.command {
        CliCommands::ReadManifest { id } => exec_read_manifest(&admin, id).await?,
        CliCommands::ListManifests { start, end } => exec_list_manifest(&admin, start, end).await?,
        CliCommands::CreateCheckpoint {
            lifetime,
            source,
            name,
        } => exec_create_checkpoint(&admin, lifetime, source, name).await?,
        CliCommands::RefreshCheckpoint { id, lifetime } => {
            exec_refresh_checkpoint(&admin, id, lifetime).await?;
        }
        CliCommands::DeleteCheckpoint { id } => exec_delete_checkpoint(&admin, id).await?,
        CliCommands::ListCheckpoints { name } => exec_list_checkpoints(&admin, name).await?,
        CliCommands::RunGarbageCollection { resource, min_age } => {
            exec_gc_once(&admin, resource, min_age).await?
        }
        CliCommands::ScheduleGarbageCollection {
            manifest,
            wal,
            compacted,
        } => schedule_gc(&admin, manifest, wal, compacted).await?,

        CliCommands::SeqToTs { seq, round } => {
            exec_seq_to_ts(&admin, seq, matches!(round, FindOption::RoundUp)).await?
        }
        CliCommands::TsToSeq { ts_secs, round } => {
            exec_ts_to_seq(&admin, ts_secs, matches!(round, FindOption::RoundUp)).await?
        }
    }

    Ok(())
}

async fn exec_read_manifest(admin: &Admin, id: Option<u64>) -> Result<(), Box<dyn Error>> {
    match admin.read_manifest(id).await? {
        None => {
            println!("no manifest file found")
        }
        Some(manifest) => {
            println!("{}", manifest);
        }
    }
    Ok(())
}

async fn exec_list_manifest(
    admin: &Admin,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let range = match (start, end) {
        (Some(s), Some(e)) => s..e,
        (Some(s), None) => s..u64::MAX,
        (None, Some(e)) => u64::MIN..e,
        _ => u64::MIN..u64::MAX,
    };

    println!("{}", admin.list_manifests(range).await?);
    Ok(())
}

async fn exec_create_checkpoint(
    admin: &Admin,
    lifetime: Option<Duration>,
    source: Option<Uuid>,
    name: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let result = admin
        .create_detached_checkpoint(&CheckpointOptions {
            lifetime,
            source,
            name,
        })
        .await?;
    println!("{:?}", result);
    Ok(())
}

async fn exec_refresh_checkpoint(
    admin: &Admin,
    id: Uuid,
    lifetime: Option<Duration>,
) -> Result<(), Box<dyn Error>> {
    println!("{:?}", admin.refresh_checkpoint(id, lifetime).await?);
    Ok(())
}

async fn exec_delete_checkpoint(admin: &Admin, id: Uuid) -> Result<(), Box<dyn Error>> {
    println!("{:?}", admin.delete_checkpoint(id).await?);
    Ok(())
}

async fn exec_list_checkpoints(
    admin: &Admin,
    name_filter: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let checkpoint = admin.list_checkpoints(name_filter.as_deref()).await?;
    let checkpoint_json = serde_json::to_string(&checkpoint)?;
    println!("{}", checkpoint_json);
    Ok(())
}

async fn exec_gc_once(
    admin: &Admin,
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
    admin.run_gc_once(gc_opts).await?;
    Ok(())
}

async fn schedule_gc(
    admin: &Admin,
    manifest_schedule: Option<GcSchedule>,
    wal_schedule: Option<GcSchedule>,
    compacted_schedule: Option<GcSchedule>,
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

    admin.run_gc(gc_opts).await?;
    Ok(())
}

async fn exec_seq_to_ts(admin: &Admin, seq: u64, round_up: bool) -> Result<(), Box<dyn Error>> {
    match admin.get_timestamp_for_sequence(seq, round_up).await? {
        Some(ts) => println!("{}", ts.to_rfc3339()),
        None => println!("not found"),
    }
    Ok(())
}

async fn exec_ts_to_seq(admin: &Admin, ts_secs: i64, round_up: bool) -> Result<(), Box<dyn Error>> {
    let ts = Utc
        .timestamp_opt(ts_secs, 0)
        .single()
        .ok_or("invalid unix seconds")?;
    match admin.get_sequence_for_timestamp(ts, round_up).await? {
        Some(seq) => println!("{}", seq),
        None => println!("not found"),
    }
    Ok(())
}
