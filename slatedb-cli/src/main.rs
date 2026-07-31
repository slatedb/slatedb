use crate::args::{parse_args, CliArgs, CliCommands, GcResource, GcSchedule};
use chrono::{TimeZone, Utc};
use object_store::path::Path;
use slatedb::admin::{self, Admin, AdminBuilder};
use slatedb::compactor::{
    CompactionRequest, CompactionSchedulerSupplier, SizeTieredCompactionSchedulerSupplier,
};
use slatedb::config::{
    CheckpointOptions, CompactionWorkerOptions, CompactorOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, Settings,
};
use slatedb::seq_tracker::FindOption;
use std::error::Error;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;
use uuid::Uuid;

mod args;
mod scan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .init();

    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    let cancellation_token = CancellationToken::new();
    let admin = AdminBuilder::new(path.clone(), object_store.clone()).build();

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
        CliCommands::ReadCompactions { id } => exec_read_compactions(&admin, id).await?,
        CliCommands::ListCompactions { start, end } => {
            exec_list_compactions(&admin, start, end).await?
        }
        CliCommands::ReadCompaction { id, compactions_id } => {
            exec_read_compaction(&admin, id, compactions_id).await?
        }
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
        CliCommands::RunGarbageCollection {
            resource,
            min_age,
            disable_boundary_files,
        } => exec_gc_once(&admin, resource, min_age, !disable_boundary_files).await?,
        CliCommands::RunCompactor { no_embedded_worker } => {
            admin
                .run_compactor_with_options(
                    cancellation_token.clone(),
                    CompactorOptions {
                        worker: (!no_embedded_worker).then(CompactionWorkerOptions::default),
                        ..CompactorOptions::default()
                    },
                )
                .await?
        }
        CliCommands::RunWorker => exec_run_worker(&admin, cancellation_token).await?,
        CliCommands::Scan {
            prefix,
            from,
            to,
            key,
            value,
            max_keys,
            count,
            checkpoint,
        } => {
            scan::exec_scan(
                path.clone(),
                object_store.clone(),
                prefix,
                from,
                to,
                key,
                value,
                max_keys,
                count,
                checkpoint,
            )
            .await?
        }
        CliCommands::ScheduleGarbageCollection {
            manifest,
            wal,
            wal_fence,
            compacted,
            compactions,
            disable_boundary_files,
        } => {
            schedule_gc(
                &admin,
                manifest,
                wal,
                wal_fence,
                compacted,
                compactions,
                !disable_boundary_files,
                cancellation_token.clone(),
            )
            .await?
        }
        CliCommands::SubmitCompaction { scheduler, request } => {
            exec_submit_compaction(&admin, scheduler, request).await?
        }

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
            println!("{}", serde_json::to_string(&manifest)?);
        }
    }
    Ok(())
}

async fn exec_read_compactions(admin: &Admin, id: Option<u64>) -> Result<(), Box<dyn Error>> {
    match admin.read_compactions(id).await? {
        None => {
            println!("no compactions file found")
        }
        Some(compactions) => {
            println!("{}", serde_json::to_string(&compactions)?);
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

    println!(
        "{}",
        serde_json::to_string(&admin.list_manifests(range).await?)?
    );
    Ok(())
}

async fn exec_list_compactions(
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

    println!(
        "{}",
        serde_json::to_string(&admin.list_compactions(range).await?)?
    );
    Ok(())
}

async fn exec_submit_compaction(
    admin: &Admin,
    scheduler: String,
    request: CompactionRequest,
) -> Result<(), Box<dyn Error>> {
    let state = admin.read_compactor_state_view().await?;
    let supplier = match scheduler.as_str() {
        "size-tiered" => SizeTieredCompactionSchedulerSupplier,
        _ => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported scheduler: {scheduler}"),
            )))
        }
    };
    let scheduler = supplier.compaction_scheduler(&CompactorOptions::default());
    let specs = scheduler.generate(&state, &request)?;
    let mut compactions = Vec::with_capacity(specs.len());
    for spec in specs {
        compactions.push(admin.submit_compaction(spec).await?);
    }
    let compaction_json = serde_json::to_string(&compactions)?;
    println!("{}", compaction_json);
    Ok(())
}

async fn exec_read_compaction(
    admin: &Admin,
    id: String,
    compactions_id: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let compaction_id = Ulid::from_string(&id)?;
    match admin.read_compaction(compaction_id, compactions_id).await? {
        None => {
            println!("no compaction found");
        }
        Some(compaction) => {
            let compaction_json = serde_json::to_string(&compaction)?;
            println!("{}", compaction_json);
        }
    }
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
    boundary_files_enabled: bool,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(min_age: Duration) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age,
            dry_run: false,
        })
    }
    let gc_opts = match resource {
        GcResource::Manifest => GarbageCollectorOptions {
            manifest_options: create_gc_dir_opts(min_age),
            wal_options: None,
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
            metric_level: None,
            boundary_files_enabled,
            object_store_max_retries: None,
        },
        GcResource::Wal => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: create_gc_dir_opts(min_age),
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
            metric_level: None,
            boundary_files_enabled,
            object_store_max_retries: None,
        },
        GcResource::WalFence => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: create_gc_dir_opts(min_age),
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
            metric_level: None,
            boundary_files_enabled,
            object_store_max_retries: None,
        },
        GcResource::Compacted => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: create_gc_dir_opts(min_age),
            compactions_options: None,
            detach_options: None,
            metric_level: None,
            boundary_files_enabled,
            object_store_max_retries: None,
        },
        GcResource::Compactions => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: create_gc_dir_opts(min_age),
            detach_options: None,
            metric_level: None,
            boundary_files_enabled,
            object_store_max_retries: None,
        },
    };
    admin.run_gc_once(gc_opts).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn schedule_gc(
    admin: &Admin,
    manifest_schedule: Option<GcSchedule>,
    wal_schedule: Option<GcSchedule>,
    wal_fence_schedule: Option<GcSchedule>,
    compacted_schedule: Option<GcSchedule>,
    compactions_schedule: Option<GcSchedule>,
    boundary_files_enabled: bool,
    cancellation_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(schedule: GcSchedule) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            interval: Some(schedule.period),
            min_age: schedule.min_age,
            dry_run: false,
        })
    }
    let gc_opts = GarbageCollectorOptions {
        manifest_options: manifest_schedule.and_then(create_gc_dir_opts),
        wal_options: wal_schedule.and_then(create_gc_dir_opts),
        wal_fence_options: wal_fence_schedule.and_then(create_gc_dir_opts),
        compacted_options: compacted_schedule.and_then(create_gc_dir_opts),
        compactions_options: compactions_schedule.and_then(create_gc_dir_opts),
        detach_options: None,
        metric_level: None,
        boundary_files_enabled,
        object_store_max_retries: None,
    };

    admin
        .run_gc_with_options(cancellation_token, gc_opts)
        .await?;
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

async fn exec_run_worker(
    admin: &Admin,
    cancellation_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    let options = match Settings::load() {
        Ok(s) => {
            let opts = s
                .compactor_options
                .and_then(|c| c.worker)
                .unwrap_or_default();
            tracing::info!("loaded worker options from settings [options={:?}]", opts);
            opts
        }
        Err(e) => {
            tracing::warn!("failed to load settings, using defaults [error={:?}]", e);
            Default::default()
        }
    };
    admin
        .run_compaction_worker_with_options(cancellation_token, options)
        .await?;
    Ok(())
}
