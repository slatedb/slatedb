use std::collections::BTreeSet;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use log::info;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use slatedb_common::metrics::MetricsRecorderHelper;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::{
    SlateDbWalReader, SlateDbWalReaderOptions, WalError, WalIterator, WalReader, WalWriter,
};
use crate::config::Settings;
use crate::db_status::{ClosedResultWriter, DbStatusManager};
use crate::dispatcher::MessageHandlerExecutor;
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::manifest::{Manifest, ManifestCore, VersionedManifest};
use crate::object_store_tag::TableStoreKind;
use crate::types::{RowEntry, ValueDeletable};
use crate::wal::slatedb::store::WalTableStore;
use crate::wal::slatedb::writer::SlateDbWalWriter;

const KEY_SIZE_BYTES: usize = 32;
const VALUE_SIZE_BYTES: usize = 128;
const ROW_DATA_SIZE_BYTES: u64 = (KEY_SIZE_BYTES + VALUE_SIZE_BYTES) as u64;
const DEFAULT_WAL_MEMORY_LIMIT_BYTES: usize = 256 * 1024 * 1024;
const REPLAY_PROGRESS_INTERVAL_BYTES: u64 = 10 * 1024 * 1024;

/// The default amount of logical key/value data written by the WAL benchmark.
pub const DEFAULT_WAL_BENCH_DATA_SIZE_BYTES: u64 = 1024 * 1024 * 1024;
/// The production WAL replay default for the shared buffered-byte limit.
pub const DEFAULT_WAL_BENCH_MAX_BUFFERED_BYTES: usize = 128 * 1024 * 1024;
/// The production WAL replay default for shared concurrent fetch tasks.
pub const DEFAULT_WAL_BENCH_MAX_FETCH_TASKS: usize = 128;
/// The production WAL replay default target bytes per fetch request.
pub const DEFAULT_WAL_BENCH_TARGET_BYTES_TO_FETCH: usize = 8 * 1024 * 1024;

/// Tunable resource limits and fetch sizing for the WAL replay benchmark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalBenchReplayOptions {
    /// Shared soft limit on bytes buffered across WAL SST iterators.
    pub max_buffered_bytes: usize,
    /// Shared soft limit on in-flight fetch tasks across WAL SST iterators.
    pub max_fetch_tasks: usize,
    /// Target number of bytes fetched per request.
    pub target_bytes_to_fetch: usize,
}

impl Default for WalBenchReplayOptions {
    fn default() -> Self {
        Self {
            max_buffered_bytes: DEFAULT_WAL_BENCH_MAX_BUFFERED_BYTES,
            max_fetch_tasks: DEFAULT_WAL_BENCH_MAX_FETCH_TASKS,
            target_bytes_to_fetch: DEFAULT_WAL_BENCH_TARGET_BYTES_TO_FETCH,
        }
    }
}

/// Selects which phases the WAL benchmark runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalBenchPhase {
    /// Write benchmark data without replaying it.
    Write,
    /// Replay existing benchmark data without writing new data.
    Read,
    /// Write benchmark data and then replay the newly written WAL range.
    Both,
}

impl WalBenchPhase {
    fn includes_write(self) -> bool {
        matches!(self, Self::Write | Self::Both)
    }

    fn includes_read(self) -> bool {
        matches!(self, Self::Read | Self::Both)
    }
}

/// Throughput measured for one benchmark phase.
#[derive(Clone, Debug)]
pub struct WalBenchMeasurement {
    /// Logical key/value bytes processed by this phase.
    pub data_size_bytes: u64,
    /// Rows processed by this phase.
    pub row_count: u64,
    /// Wall-clock time for this phase.
    pub elapsed: Duration,
    /// Logical key/value throughput in bytes per second.
    pub throughput_bytes_per_second: f64,
}

/// Measurements returned by the WAL write and replay benchmark.
#[derive(Clone, Debug)]
pub struct WalBenchResult {
    /// The requested maximum logical key/value bytes.
    pub size_limit_bytes: u64,
    /// The first WAL file ID included in this run.
    pub first_wal_file_id: u64,
    /// The last WAL file ID included in this run.
    pub last_wal_file_id: u64,
    /// The number of WAL files in the selected range.
    pub wal_file_count: u64,
    /// The write measurement, if the write phase ran.
    pub write: Option<WalBenchMeasurement>,
    /// The replay measurement, if the read phase ran.
    pub replay: Option<WalBenchMeasurement>,
    /// Maximum active plus immutable WAL files observed during the write phase.
    pub max_unflushed_wal_files: Option<usize>,
}

#[derive(Clone, Copy)]
struct WalBenchSettings {
    flush_interval: Duration,
    max_wal_file_size_bytes: usize,
    max_wal_flushes_before_l0_flush: u64,
}

fn validate_replay_options(options: WalBenchReplayOptions) -> Result<(), crate::Error> {
    if options.max_buffered_bytes == 0 {
        return Err(crate::Error::invalid(
            "WAL benchmark max buffered bytes must be greater than zero".to_string(),
        ));
    }
    if options.max_fetch_tasks == 0 {
        return Err(crate::Error::invalid(
            "WAL benchmark max fetch tasks must be greater than zero".to_string(),
        ));
    }
    if options.target_bytes_to_fetch == 0 {
        return Err(crate::Error::invalid(
            "WAL benchmark target bytes to fetch must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

/// Runs the native WAL write and replay benchmark against an S3 bucket.
///
/// AWS credentials and optional endpoint configuration are read using the object_store
/// AmazonS3Builder environment variables. The bucket and region supplied here override
/// any bucket and region in the environment. The prefix should be dedicated to WAL
/// benchmark data. Write phases append after an existing contiguous WAL. A read-only
/// phase starts at WAL file 1 and reads up to the configured size limit or the end of
/// the existing WAL, whichever comes first.
pub async fn run_bench(
    bucket: &str,
    region: &str,
    prefix: Path,
    phase: WalBenchPhase,
    size_limit_bytes: u64,
    replay_options: WalBenchReplayOptions,
) -> Result<WalBenchResult, crate::Error> {
    if bucket.trim().is_empty() {
        return Err(crate::Error::invalid(
            "WAL benchmark bucket must not be empty".to_string(),
        ));
    }
    if region.trim().is_empty() {
        return Err(crate::Error::invalid(
            "WAL benchmark region must not be empty".to_string(),
        ));
    }
    if prefix.as_ref().is_empty() {
        return Err(crate::Error::invalid(
            "WAL benchmark prefix must not be empty".to_string(),
        ));
    }
    if size_limit_bytes == 0 {
        return Err(crate::Error::invalid(
            "WAL benchmark size limit must be greater than zero".to_string(),
        ));
    }
    validate_replay_options(replay_options)?;

    let object_store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(region)
        .build()
        .map_err(|error| {
            crate::Error::unavailable(format!(
                "failed to build S3 object store for WAL benchmark bucket {bucket}"
            ))
            .with_source(Box::new(error))
        })?;
    let object_store = Arc::new(object_store) as Arc<dyn ObjectStore>;

    let settings = Settings::default();
    let flush_interval = settings.flush_interval.ok_or_else(|| {
        crate::Error::internal("default WAL flush interval is disabled".to_string())
    })?;
    let bench_settings = WalBenchSettings {
        flush_interval,
        max_wal_file_size_bytes: settings.l0_sst_size_bytes,
        max_wal_flushes_before_l0_flush: settings.max_wal_flushes_before_l0_flush,
    };

    run_bench_with_object_store(
        object_store,
        prefix,
        phase,
        size_limit_bytes,
        bench_settings,
        replay_options,
    )
    .await
}

async fn run_bench_with_object_store(
    object_store: Arc<dyn ObjectStore>,
    prefix: Path,
    phase: WalBenchPhase,
    size_limit_bytes: u64,
    settings: WalBenchSettings,
    replay_options: WalBenchReplayOptions,
) -> Result<WalBenchResult, crate::Error> {
    validate_replay_options(replay_options)?;

    let wal_store = Arc::new(WalTableStore::new(
        object_store,
        SsTableFormat::default(),
        prefix.clone(),
        TableStoreKind::Main,
    ));
    let previous_wal_file_id = wal_store.last_seen_wal_id(0.into()).await?.value();
    let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

    if !phase.includes_write() {
        if previous_wal_file_id == 0 {
            return Err(crate::Error::invalid(format!(
                "no WAL files found under benchmark prefix {prefix}"
            )));
        }
        let first_wal_file_id = 1;
        let last_wal_file_id = previous_wal_file_id;
        let wal_file_count = last_wal_file_id;
        let replay = run_replay_phase(
            Arc::clone(&wal_store),
            &prefix,
            first_wal_file_id,
            last_wal_file_id,
            Some(size_limit_bytes),
            replay_options,
            Arc::clone(&clock),
        )
        .await?;
        return Ok(WalBenchResult {
            size_limit_bytes,
            first_wal_file_id,
            last_wal_file_id,
            wal_file_count,
            write: None,
            replay: Some(replay),
            max_unflushed_wal_files: None,
        });
    }

    let first_wal_file_id = previous_wal_file_id.checked_add(1).ok_or_else(|| {
        crate::Error::invalid("WAL benchmark file ID space is exhausted".to_string())
    })?;
    let mut initial_core = ManifestCore::new();
    initial_core.replay_after_wal_id = previous_wal_file_id;
    initial_core.next_wal_sst_id = first_wal_file_id;
    let status_manager = Arc::new(DbStatusManager::new_with_initial_values(
        0,
        VersionedManifest::from_manifest(1, Manifest::initial(initial_core)),
        BTreeSet::new(),
    ));
    let task_executor = Arc::new(MessageHandlerExecutor::new(
        status_manager.clone(),
        Arc::clone(&clock),
    ));
    let recorder = MetricsRecorderHelper::noop();
    let mut writer = SlateDbWalWriter::start_new(
        status_manager.result_reader(),
        &recorder,
        previous_wal_file_id,
        Arc::clone(&wal_store),
        settings.max_wal_file_size_bytes,
        settings.max_wal_flushes_before_l0_flush,
        Some(settings.flush_interval),
        Arc::clone(&task_executor),
    )
    .await?;

    let flushed = Arc::new(Notify::new());
    let listener_flushed = Arc::clone(&flushed);
    writer
        .observer()
        .subscribe(Arc::new(move |_event| listener_flushed.notify_one()))
        .map_err(wal_error)?;
    let monitor = task_executor.monitor_on(&Handle::current())?;

    let row_count = size_limit_bytes.div_ceil(ROW_DATA_SIZE_BYTES);
    let data_size_bytes = row_count
        .checked_mul(ROW_DATA_SIZE_BYTES)
        .ok_or_else(|| crate::Error::invalid("WAL benchmark data size overflow".to_string()))?;

    info!(
        "starting WAL write benchmark [prefix={}, logical_bytes={}, rows={}, key_bytes={}, value_bytes={}, flush_interval_ms={}, max_wal_file_bytes={}, wal_memory_limit_bytes={}]",
        prefix,
        data_size_bytes,
        row_count,
        KEY_SIZE_BYTES,
        VALUE_SIZE_BYTES,
        settings.flush_interval.as_millis(),
        settings.max_wal_file_size_bytes,
        DEFAULT_WAL_MEMORY_LIMIT_BYTES,
    );

    let write_result = write_rows(
        &mut writer,
        &flushed,
        row_count,
        data_size_bytes,
        clock.as_ref(),
    )
    .await;
    let (write_elapsed, max_unflushed_wal_files) = match write_result {
        Ok(result) => result,
        Err(error) => {
            let _ = shutdown_writer(&mut writer, monitor).await;
            return Err(error);
        }
    };

    let final_status = writer
        .status()
        .map_err(|status| wal_error(WalError::from(status)));
    let shutdown_result = shutdown_writer(&mut writer, monitor).await;
    let final_status = final_status?;
    shutdown_result?;

    let last_wal_file_id = final_status.last_flushed_wal_id;
    if last_wal_file_id < first_wal_file_id {
        return Err(crate::Error::internal(format!(
            "WAL benchmark flush did not produce a WAL file [first_wal_file_id={first_wal_file_id}, last_flushed_wal_file_id={last_wal_file_id}]"
        )));
    }
    let wal_file_count = last_wal_file_id - first_wal_file_id + 1;
    let write_throughput_bytes_per_second = throughput(data_size_bytes, write_elapsed);
    info!(
        "completed WAL write benchmark [logical_bytes={}, rows={}, wal_files={}, elapsed_seconds={:.3}, throughput_bytes_per_second={:.0}, throughput_mib_per_second={:.2}]",
        data_size_bytes,
        row_count,
        wal_file_count,
        write_elapsed.as_secs_f64(),
        write_throughput_bytes_per_second,
        write_throughput_bytes_per_second / (1024.0 * 1024.0),
    );
    let write = WalBenchMeasurement {
        data_size_bytes,
        row_count,
        elapsed: write_elapsed,
        throughput_bytes_per_second: write_throughput_bytes_per_second,
    };

    let replay = if phase.includes_read() {
        let replay = run_replay_phase(
            Arc::clone(&wal_store),
            &prefix,
            first_wal_file_id,
            last_wal_file_id,
            None,
            replay_options,
            Arc::clone(&clock),
        )
        .await?;
        if replay.row_count != write.row_count || replay.data_size_bytes != write.data_size_bytes {
            return Err(crate::Error::data(format!(
                "WAL replay count mismatch [expected_rows={}, actual_rows={}, expected_bytes={}, actual_bytes={}]",
                write.row_count,
                replay.row_count,
                write.data_size_bytes,
                replay.data_size_bytes,
            )));
        }
        Some(replay)
    } else {
        None
    };

    Ok(WalBenchResult {
        size_limit_bytes,
        first_wal_file_id,
        last_wal_file_id,
        wal_file_count,
        write: Some(write),
        replay,
        max_unflushed_wal_files: Some(max_unflushed_wal_files),
    })
}

async fn run_replay_phase(
    wal_store: Arc<WalTableStore>,
    prefix: &Path,
    first_wal_file_id: u64,
    last_wal_file_id: u64,
    size_limit_bytes: Option<u64>,
    replay_options: WalBenchReplayOptions,
    clock: Arc<dyn SystemClock>,
) -> Result<WalBenchMeasurement, crate::Error> {
    let replay_end = last_wal_file_id
        .checked_add(1)
        .ok_or_else(|| crate::Error::invalid("WAL benchmark replay range overflow".to_string()))?;
    let mut replay_core = ManifestCore::new();
    replay_core.replay_after_wal_id = first_wal_file_id
        .checked_sub(1)
        .expect("WAL benchmark ranges start at file ID 1 or later");
    replay_core.next_wal_sst_id = replay_end;
    let status_manager = DbStatusManager::new_with_initial_values(
        0,
        VersionedManifest::from_manifest(1, Manifest::initial(replay_core)),
        BTreeSet::new(),
    );
    let reader = SlateDbWalReader::new_with_status_manager(
        Arc::clone(&wal_store),
        &status_manager,
        Arc::clone(&clock),
        SlateDbWalReaderOptions {
            max_buffered_bytes: replay_options.max_buffered_bytes,
            max_fetch_tasks: replay_options.max_fetch_tasks,
            read_ahead_bytes: replay_options.target_bytes_to_fetch,
        },
    );
    let mut iterator = reader
        .iterator((first_wal_file_id..replay_end).into())
        .await
        .map_err(wal_error)?;

    info!(
        "starting WAL replay benchmark [prefix={}, first_wal_file_id={}, last_wal_file_id={}, size_limit_bytes={}, max_buffered_bytes={}, max_fetch_tasks={}, target_bytes_to_fetch={}]",
        prefix,
        first_wal_file_id,
        last_wal_file_id,
        size_limit_bytes
            .map(|limit| limit.to_string())
            .unwrap_or_else(|| "unbounded".to_string()),
        replay_options.max_buffered_bytes,
        replay_options.max_fetch_tasks,
        replay_options.target_bytes_to_fetch,
    );
    let replay_started = clock.now();
    let (row_count, data_size_bytes) = consume_wal(
        iterator.as_mut(),
        size_limit_bytes,
        clock.as_ref(),
        replay_started,
    )
    .await?;
    let elapsed = elapsed_since(clock.as_ref(), replay_started)?;
    let throughput_bytes_per_second = throughput(data_size_bytes, elapsed);
    info!(
        "completed WAL replay benchmark [logical_bytes={}, rows={}, wal_files={}, elapsed_seconds={:.3}, throughput_bytes_per_second={:.0}, throughput_mib_per_second={:.2}]",
        data_size_bytes,
        row_count,
        last_wal_file_id - first_wal_file_id + 1,
        elapsed.as_secs_f64(),
        throughput_bytes_per_second,
        throughput_bytes_per_second / (1024.0 * 1024.0),
    );

    Ok(WalBenchMeasurement {
        data_size_bytes,
        row_count,
        elapsed,
        throughput_bytes_per_second,
    })
}

async fn write_rows(
    writer: &mut SlateDbWalWriter,
    flushed: &Notify,
    row_count: u64,
    data_size_bytes: u64,
    clock: &dyn SystemClock,
) -> Result<(Duration, usize), crate::Error> {
    let value = Bytes::from(vec![0xA5; VALUE_SIZE_BYTES]);
    let started = clock.now();
    let mut max_unflushed_wal_files = 0;

    for seq in 1..=row_count {
        wait_for_write_capacity(writer, flushed).await?;

        let row = RowEntry::new(
            benchmark_key(seq),
            ValueDeletable::Value(value.clone()),
            seq,
            None,
            None,
        );
        writer
            .append(std::slice::from_ref(&row))
            .await
            .map_err(wal_error)?;

        let current_unflushed = writer.unflushed_wal_file_count();
        max_unflushed_wal_files = max_unflushed_wal_files.max(current_unflushed);
    }

    let final_flush = writer.flush().await.map_err(wal_error)?;
    final_flush.await.map_err(wal_error)?;
    let elapsed = elapsed_since(clock, started)?;
    if writer.unflushed_wal_file_count() != 0 {
        return Err(crate::Error::internal(
            "WAL benchmark final flush left unflushed WAL files".to_string(),
        ));
    }
    if data_size_bytes == 0 {
        return Err(crate::Error::internal(
            "WAL benchmark wrote no data".to_string(),
        ));
    }
    Ok((elapsed, max_unflushed_wal_files))
}

async fn wait_for_write_capacity(
    writer: &SlateDbWalWriter,
    flushed: &Notify,
) -> Result<(), crate::Error> {
    loop {
        let notified = flushed.notified();
        let estimated_bytes = writer
            .status()
            .map_err(|status| wal_error(WalError::from(status)))?
            .estimated_bytes;
        if estimated_bytes < DEFAULT_WAL_MEMORY_LIMIT_BYTES {
            return Ok(());
        }
        notified.await;
    }
}

async fn consume_wal(
    iterator: &mut dyn WalIterator,
    size_limit_bytes: Option<u64>,
    clock: &dyn SystemClock,
    replay_started: chrono::DateTime<chrono::Utc>,
) -> Result<(u64, u64), crate::Error> {
    let mut replayed_rows = 0u64;
    let mut replayed_bytes = 0u64;
    let mut next_progress_bytes = REPLAY_PROGRESS_INTERVAL_BYTES;

    'wal_files: while let Some(wal_rows) = iterator.next().await.map_err(wal_error)? {
        for row in wal_rows.rows {
            if row.key.len() != KEY_SIZE_BYTES || row.value.len() != VALUE_SIZE_BYTES {
                return Err(crate::Error::data(format!(
                    "unexpected WAL row dimensions [key_bytes={}, value_bytes={}]",
                    row.key.len(),
                    row.value.len(),
                )));
            }
            replayed_rows = replayed_rows.checked_add(1).ok_or_else(|| {
                crate::Error::internal("WAL replay row count overflow".to_string())
            })?;
            replayed_bytes = replayed_bytes
                .checked_add(ROW_DATA_SIZE_BYTES)
                .ok_or_else(|| {
                    crate::Error::internal("WAL replay byte count overflow".to_string())
                })?;

            if replayed_bytes >= next_progress_bytes {
                let elapsed = elapsed_since(clock, replay_started)?;
                let current_throughput = throughput(replayed_bytes, elapsed);
                info!(
                    "WAL replay progress [logical_bytes={}, logical_mib={:.2}, rows={}, elapsed_seconds={:.3}, throughput_bytes_per_second={:.0}, throughput_mib_per_second={:.2}]",
                    replayed_bytes,
                    replayed_bytes as f64 / (1024.0 * 1024.0),
                    replayed_rows,
                    elapsed.as_secs_f64(),
                    current_throughput,
                    current_throughput / (1024.0 * 1024.0),
                );
                next_progress_bytes =
                    next_progress_bytes.saturating_add(REPLAY_PROGRESS_INTERVAL_BYTES);
            }

            if size_limit_bytes.is_some_and(|limit| replayed_bytes >= limit) {
                break 'wal_files;
            }
        }
    }
    Ok((replayed_rows, replayed_bytes))
}

async fn shutdown_writer(
    writer: &mut SlateDbWalWriter,
    monitor: JoinHandle<()>,
) -> Result<(), crate::Error> {
    let close_result = writer.close().await.map_err(wal_error);
    let monitor_result = monitor.await.map_err(|error| {
        crate::Error::internal("WAL benchmark task monitor failed".to_string())
            .with_source(Box::new(error))
    });
    close_result?;
    monitor_result
}

fn benchmark_key(seq: u64) -> Bytes {
    let mut key = [0u8; KEY_SIZE_BYTES];
    let mut state = seq;
    for chunk in key.chunks_exact_mut(size_of::<u64>()) {
        state = splitmix64(state);
        chunk.copy_from_slice(&state.to_be_bytes());
    }
    Bytes::copy_from_slice(&key)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn throughput(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return f64::INFINITY;
    }
    bytes as f64 / elapsed.as_secs_f64()
}

fn elapsed_since(
    clock: &dyn SystemClock,
    started: chrono::DateTime<chrono::Utc>,
) -> Result<Duration, crate::Error> {
    clock
        .now()
        .signed_duration_since(started)
        .to_std()
        .map_err(|error| {
            crate::Error::internal(format!(
                "WAL benchmark clock moved backwards while measuring elapsed time: {error}"
            ))
        })
}

fn wal_error(error: WalError) -> crate::Error {
    SlateDBError::from(error).into()
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn test_settings() -> WalBenchSettings {
        WalBenchSettings {
            flush_interval: Duration::from_millis(10),
            max_wal_file_size_bytes: 8 * 1024,
            max_wal_flushes_before_l0_flush: 4096,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_write_and_replay_with_memory_backpressure() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let size_limit_bytes = 512 * 1024;
        let result = run_bench_with_object_store(
            object_store,
            Path::from("wal-bench-test"),
            WalBenchPhase::Both,
            size_limit_bytes,
            test_settings(),
            WalBenchReplayOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.size_limit_bytes, size_limit_bytes);
        let write = result.write.as_ref().unwrap();
        let replay = result.replay.as_ref().unwrap();
        assert!(write.data_size_bytes >= size_limit_bytes);
        assert_eq!(write.data_size_bytes, write.row_count * ROW_DATA_SIZE_BYTES);
        assert_eq!(replay.data_size_bytes, write.data_size_bytes);
        assert_eq!(replay.row_count, write.row_count);
        assert!(result.wal_file_count > 0);
        assert!(result.max_unflushed_wal_files.unwrap() > 0);
        assert!(write.throughput_bytes_per_second > 0.0);
        assert!(replay.throughput_bytes_per_second > 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_run_write_and_read_phases_separately() {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let prefix = Path::from("wal-bench-separate-phases");
        let write_result = run_bench_with_object_store(
            Arc::clone(&object_store),
            prefix.clone(),
            WalBenchPhase::Write,
            256 * 1024,
            test_settings(),
            WalBenchReplayOptions::default(),
        )
        .await
        .unwrap();

        assert!(write_result.write.is_some());
        assert!(write_result.replay.is_none());
        assert!(write_result.max_unflushed_wal_files.is_some());

        let read_limit_bytes = 128 * 1024;
        let read_result = run_bench_with_object_store(
            object_store,
            prefix,
            WalBenchPhase::Read,
            read_limit_bytes,
            test_settings(),
            WalBenchReplayOptions::default(),
        )
        .await
        .unwrap();

        assert!(read_result.write.is_none());
        assert!(read_result.max_unflushed_wal_files.is_none());
        let replay = read_result.replay.unwrap();
        assert!(replay.data_size_bytes >= read_limit_bytes);
        assert!(replay.data_size_bytes < read_limit_bytes + ROW_DATA_SIZE_BYTES);
        assert_eq!(
            replay.data_size_bytes,
            replay.row_count * ROW_DATA_SIZE_BYTES
        );
    }

    #[test]
    fn replay_options_must_be_nonzero() {
        for options in [
            WalBenchReplayOptions {
                max_buffered_bytes: 0,
                ..WalBenchReplayOptions::default()
            },
            WalBenchReplayOptions {
                max_fetch_tasks: 0,
                ..WalBenchReplayOptions::default()
            },
            WalBenchReplayOptions {
                target_bytes_to_fetch: 0,
                ..WalBenchReplayOptions::default()
            },
        ] {
            assert!(validate_replay_options(options).is_err());
        }
        assert!(validate_replay_options(WalBenchReplayOptions::default()).is_ok());
    }

    #[test]
    fn replay_options_match_reader_defaults() {
        let bench = WalBenchReplayOptions::default();
        let reader = SlateDbWalReaderOptions::default();

        assert_eq!(bench.max_buffered_bytes, reader.max_buffered_bytes);
        assert_eq!(bench.max_fetch_tasks, reader.max_fetch_tasks);
        assert_eq!(bench.target_bytes_to_fetch, reader.read_ahead_bytes);
    }
}
