use crate::args::{parse_args, CliArgs, CliCommands, GcResource, GcSchedule};
use crate::data::{DataEncoding, DataFormat, DataRecordWriter};
use chrono::{TimeZone, Utc};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use slatedb::admin::{self, Admin, AdminBuilder};
use slatedb::compactor::{
    CompactionRequest, CompactionSchedulerSupplier, SizeTieredCompactionSchedulerSupplier,
};
use slatedb::config::{
    CheckpointOptions, CompactorOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::seq_tracker::FindOption;
use slatedb::{Db, WriteBatch};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;
use uuid::Uuid;

mod args;
mod data;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_test_writer()
        .init();

    let args: CliArgs = parse_args();
    let path = ObjectStorePath::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    let cancellation_token = CancellationToken::new();

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
        CliCommands::ImportData {
            input,
            format,
            encoding,
            batch_size,
        } => exec_import_data(path, object_store, input, format, encoding, batch_size).await?,
        CliCommands::ExportData {
            output,
            format,
            encoding,
        } => exec_export_data(path, object_store, output, format, encoding).await?,
        command => {
            let admin = AdminBuilder::new(path, object_store).build();
            exec_admin_command(&admin, cancellation_token, command).await?
        }
    }

    Ok(())
}

async fn exec_admin_command(
    admin: &Admin,
    cancellation_token: CancellationToken,
    command: CliCommands,
) -> Result<(), Box<dyn Error>> {
    match command {
        CliCommands::ReadManifest { id } => exec_read_manifest(admin, id).await?,
        CliCommands::ListManifests { start, end } => exec_list_manifest(admin, start, end).await?,
        CliCommands::ReadCompactions { id } => exec_read_compactions(admin, id).await?,
        CliCommands::ListCompactions { start, end } => {
            exec_list_compactions(admin, start, end).await?
        }
        CliCommands::ReadCompaction { id, compactions_id } => {
            exec_read_compaction(admin, id, compactions_id).await?
        }
        CliCommands::CreateCheckpoint {
            lifetime,
            source,
            name,
        } => exec_create_checkpoint(admin, lifetime, source, name).await?,
        CliCommands::RefreshCheckpoint { id, lifetime } => {
            exec_refresh_checkpoint(admin, id, lifetime).await?;
        }
        CliCommands::DeleteCheckpoint { id } => exec_delete_checkpoint(admin, id).await?,
        CliCommands::ListCheckpoints { name } => exec_list_checkpoints(admin, name).await?,
        CliCommands::RunGarbageCollection { resource, min_age } => {
            exec_gc_once(admin, resource, min_age).await?
        }
        CliCommands::RunCompactor => admin.run_compactor(cancellation_token.clone()).await?,
        CliCommands::ScheduleGarbageCollection {
            manifest,
            wal,
            wal_fence,
            compacted,
            compactions,
        } => schedule_gc(admin, manifest, wal, wal_fence, compacted, compactions).await?,
        CliCommands::SubmitCompaction { scheduler, request } => {
            exec_submit_compaction(admin, scheduler, request).await?
        }

        CliCommands::SeqToTs { seq, round } => {
            exec_seq_to_ts(admin, seq, matches!(round, FindOption::RoundUp)).await?
        }
        CliCommands::TsToSeq { ts_secs, round } => {
            exec_ts_to_seq(admin, ts_secs, matches!(round, FindOption::RoundUp)).await?
        }
        CliCommands::ImportData { .. } | CliCommands::ExportData { .. } => unreachable!(),
    }

    Ok(())
}

async fn exec_import_data(
    path: ObjectStorePath,
    object_store: Arc<dyn ObjectStore>,
    input: PathBuf,
    format: DataFormat,
    encoding: DataEncoding,
    batch_size: usize,
) -> Result<(), Box<dyn Error>> {
    if batch_size == 0 {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--batch-size must be greater than 0",
        )));
    }

    let records = {
        let reader = open_input(&input)?;
        data::read_records(reader, format, encoding)?
    };

    let db = Db::open(path, object_store).await?;
    let mut batch = WriteBatch::new();
    let mut imported = 0_u64;

    for record in records {
        batch.put_bytes(record.key, record.value);
        imported += 1;
        if (imported as usize).is_multiple_of(batch_size) {
            let _ = db.write(batch).await?;
            batch = WriteBatch::new();
        }
    }

    if !batch.is_empty() {
        let _ = db.write(batch).await?;
    }
    db.close().await?;

    println!("{}", serde_json::json!({ "imported": imported }));
    Ok(())
}

async fn exec_export_data(
    path: ObjectStorePath,
    object_store: Arc<dyn ObjectStore>,
    output: PathBuf,
    format: DataFormat,
    encoding: DataEncoding,
) -> Result<(), Box<dyn Error>> {
    let db = Db::open(path, object_store).await?;
    let writer = open_output(&output)?;
    let mut record_writer = DataRecordWriter::new(writer, format, encoding)?;
    let mut iter = db.scan::<Vec<u8>, _>(..).await?;

    while let Some(item) = iter.next().await? {
        record_writer.write_record(&item.key, &item.value)?;
    }

    let exported = record_writer.finish()?;
    db.close().await?;

    if !is_stdio_path(&output) {
        println!("{}", serde_json::json!({ "exported": exported }));
    }
    Ok(())
}

fn open_input(path: &FsPath) -> Result<Box<dyn Read>, Box<dyn Error>> {
    if is_stdio_path(path) {
        Ok(Box::new(io::stdin()))
    } else {
        Ok(Box::new(BufReader::new(File::open(path)?)))
    }
}

fn open_output(path: &FsPath) -> Result<Box<dyn Write>, Box<dyn Error>> {
    if is_stdio_path(path) {
        Ok(Box::new(io::stdout()))
    } else {
        Ok(Box::new(BufWriter::new(File::create(path)?)))
    }
}

fn is_stdio_path(path: &FsPath) -> bool {
    path == FsPath::new("-")
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
        },
        GcResource::Wal => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: create_gc_dir_opts(min_age),
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
        },
        GcResource::WalFence => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: create_gc_dir_opts(min_age),
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
        },
        GcResource::Compacted => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: create_gc_dir_opts(min_age),
            compactions_options: None,
            detach_options: None,
        },
        GcResource::Compactions => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: create_gc_dir_opts(min_age),
            detach_options: None,
        },
    };
    admin.run_gc_once(gc_opts).await?;
    Ok(())
}

async fn schedule_gc(
    admin: &Admin,
    manifest_schedule: Option<GcSchedule>,
    wal_schedule: Option<GcSchedule>,
    wal_fence_schedule: Option<GcSchedule>,
    compacted_schedule: Option<GcSchedule>,
    compactions_schedule: Option<GcSchedule>,
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
