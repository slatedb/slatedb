use std::str::FromStr;
use std::sync::Once;
use std::time::Duration;

use rand::Rng;
use slatedb::config::{
    CompactorOptions, CompressionCodec, DbReaderOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, GarbageCollectorScheduleOptions, SizeTieredCompactionSchedulerOptions,
};
use slatedb::{DbRand, Settings};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use crate::{Operation, StreamDirection, Toxic, ToxicKind};

const KIB_8: usize = 8 * 1024;
const MIB_1: usize = 1024 * 1024;
const MIB_500: usize = 500 * MIB_1;
const GIB_2: usize = 2048 * MIB_1;

const COMPRESSION_CODECS: [Option<&str>; 5] = [
    Some("snappy"),
    Some("zlib"),
    Some("lz4"),
    Some("zstd"),
    None,
];

/// Builds a randomized deterministic [`Settings`] value for DST scenarios.
///
/// The returned settings are entirely derived from `rand`, except that
/// object-store caching is always disabled. The cache implementation uses
/// filesystem and blocking-task wakeups outside the seeded current-thread DST
/// runtime, which breaks logical-clock determinism for the harness-managed
/// clock.
pub async fn build_settings(rand: &DbRand) -> Settings {
    let mut rng = rand.rng();
    let flush_interval = rng.random_range(Duration::from_millis(1)..Duration::from_secs(60));
    let manifest_poll_interval = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let manifest_update_timeout = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let min_filter_keys = rng.random_range(100..1000);
    let l0_sst_size_bytes = rng.random_range(MIB_1..MIB_500);
    let l0_max_ssts = rng.random_range(4..8);
    let l0_max_ssts_per_key = l0_max_ssts;
    let max_unflushed_bytes = rng.random_range(MIB_1..GIB_2);
    let compression_codec_idx = rng.random_range(0..COMPRESSION_CODECS.len());
    let compression_codec =
        if let Some(compression_codec) = COMPRESSION_CODECS[compression_codec_idx] {
            CompressionCodec::from_str(compression_codec).ok()
        } else {
            None
        };
    let settings = Settings {
        flush_interval: Some(flush_interval),
        manifest_poll_interval,
        manifest_update_timeout,
        min_filter_keys,
        l0_sst_size_bytes,
        l0_max_ssts,
        l0_max_ssts_per_key,
        max_unflushed_bytes,
        compression_codec,
        compactor_options: Some(build_settings_compactor(&mut *rng)),
        garbage_collector_options: Some(build_settings_gc(&mut *rng)),
        #[cfg(feature = "wal_disable")]
        wal_enabled: rng.random_bool(0.5),
        ..Default::default()
    };
    settings
}

/// Builds randomized deterministic reader options for DST scenarios.
pub fn build_reader_options(rand: &DbRand) -> DbReaderOptions {
    let mut rng = rand.rng();
    let manifest_poll_interval =
        rng.random_range(Duration::from_millis(100)..Duration::from_secs(5));
    // Lifetime must always be greater than twice the poll interval.
    let min_checkpoint_lifetime = manifest_poll_interval * 2 + Duration::from_micros(1);
    let checkpoint_lifetime =
        rng.random_range(min_checkpoint_lifetime..Duration::from_secs(4 * 60 * 60));
    let max_memtable_bytes = rng.random_range((MIB_1 as u64)..=(MIB_500 as u64));
    DbReaderOptions {
        manifest_poll_interval,
        checkpoint_lifetime,
        max_memtable_bytes,
        ..DbReaderOptions::default()
    }
}

/// Builds randomized deterministic compactor options for DST scenarios.
pub fn build_settings_compactor(rng: &mut impl Rng) -> CompactorOptions {
    let min_compaction_sources = rng.random_range(2..=4);
    let max_compaction_sources = rng.random_range(min_compaction_sources..=16);
    CompactorOptions {
        poll_interval: rng.random_range(Duration::from_millis(1)..Duration::from_secs(5)),
        manifest_update_timeout: rng
            .random_range(Duration::from_millis(100)..Duration::from_secs(60)),
        max_sst_size: rng.random_range(KIB_8..GIB_2),
        max_concurrent_compactions: rng.random_range(1..=4),
        max_fetch_tasks: rng.random_range(1..=8),
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources,
            max_compaction_sources,
            include_size_threshold: rng.random_range(2.0..=8.0),
        }
        .into(),
    }
}

/// Builds randomized deterministic garbage collector options for DST scenarios.
pub fn build_settings_gc(rng: &mut impl Rng) -> GarbageCollectorOptions {
    GarbageCollectorOptions {
        manifest_options: Some(GarbageCollectorDirectoryOptions {
            interval: Some(rng.random_range(Duration::from_millis(1)..Duration::from_secs(600))),
            min_age: rng.random_range(Duration::from_millis(1)..Duration::from_secs(900)),
        }),
        wal_options: Some(GarbageCollectorDirectoryOptions {
            interval: Some(rng.random_range(Duration::from_millis(1)..Duration::from_secs(600))),
            min_age: rng.random_range(Duration::from_millis(1)..Duration::from_secs(900)),
        }),
        compacted_options: Some(GarbageCollectorDirectoryOptions {
            interval: Some(rng.random_range(Duration::from_millis(1)..Duration::from_secs(600))),
            min_age: rng.random_range(Duration::from_millis(1)..Duration::from_secs(900)),
        }),
        compactions_options: Some(GarbageCollectorDirectoryOptions {
            interval: Some(rng.random_range(Duration::from_millis(1)..Duration::from_secs(600))),
            min_age: rng.random_range(Duration::from_millis(1)..Duration::from_secs(900)),
        }),
        detach_options: Some(GarbageCollectorScheduleOptions {
            interval: Some(rng.random_range(Duration::from_millis(1)..Duration::from_secs(600))),
        }),
    }
}

/// Builds a deterministic randomized object-store toxic.
///
/// Generated toxics may be:
/// - `Latency`: 1 to 5 ms base latency, 0 to 15 ms jitter, and 0.35 to 0.95
///   toxicity.
/// - `Bandwidth`: 16 to 256 KiB/s bandwidth and 0.20 to 0.80 toxicity.
/// - `SlowClose`: 1 to 10 ms close delay and 0.30 to 0.90 toxicity.
/// - `ResetPeer`: connection reset failures with 0.005 to 0.035 toxicity.
///
/// ## Arguments
/// - `rand`: The deterministic RNG used to choose each toxic's kind, operation
///   filter, path filter, direction, and toxicity.
/// - `root_path`: The object-store root path used by the scenario. When non-empty,
///   generated path filters may target the root itself or one of SlateDB's standard
///   subdirectories: `wal`, `manifest`, `compacted`, or `compactions`.
/// - `index`: The index used in the generated toxic name.
///
/// ## Returns
/// Returns a generated toxic.
pub fn build_toxic(rand: &DbRand, root_path: &str, index: usize) -> Toxic {
    let mut rng = rand.rng();
    let root_path = root_path.trim_matches('/');

    let (kind_name, kind, direction, toxicity) = match rng.random_range(0..10) {
        0..=4 => {
            let direction = if rng.random_bool(0.5) {
                StreamDirection::Upstream
            } else {
                StreamDirection::Downstream
            };
            (
                "latency",
                ToxicKind::Latency {
                    latency: Duration::from_millis(rng.random_range(1_u64..=5)),
                    jitter: Duration::from_millis(rng.random_range(0_u64..=15)),
                },
                direction,
                rng.random_range(0.35..=0.95),
            )
        }
        5..=6 => {
            let direction = if rng.random_bool(0.5) {
                StreamDirection::Upstream
            } else {
                StreamDirection::Downstream
            };
            (
                "bandwidth",
                ToxicKind::Bandwidth {
                    bytes_per_sec: rng.random_range(16_u64..=256) * 1024,
                },
                direction,
                rng.random_range(0.20..=0.80),
            )
        }
        7 => (
            "slow-close",
            ToxicKind::SlowClose {
                delay: Duration::from_millis(rng.random_range(1_u64..=10)),
            },
            StreamDirection::Downstream,
            rng.random_range(0.30..=0.90),
        ),
        _ => {
            let direction = if rng.random_bool(0.5) {
                StreamDirection::Upstream
            } else {
                StreamDirection::Downstream
            };
            (
                "reset-peer",
                ToxicKind::ResetPeer,
                direction,
                rng.random_range(0.005..=0.035),
            )
        }
    };

    let operations = match rng.random_range(0..8) {
        0 => vec![],
        1 => vec![Operation::PutOpts],
        2 => vec![
            Operation::GetOpts,
            Operation::GetRange,
            Operation::GetRanges,
        ],
        3 => vec![Operation::GetOpts, Operation::GetRange, Operation::Head],
        4 => vec![Operation::List, Operation::ListWithOffset],
        5 => vec![Operation::Delete],
        6 => vec![Operation::Copy, Operation::Rename],
        _ => vec![
            Operation::PutOpts,
            Operation::GetOpts,
            Operation::GetRange,
            Operation::GetRanges,
            Operation::Head,
            Operation::List,
            Operation::ListWithOffset,
        ],
    };

    let path_prefix = if root_path.is_empty() || rng.random_bool(0.15) {
        None
    } else {
        Some(match rng.random_range(0..5) {
            0 => root_path.to_string(),
            1 => format!("{root_path}/wal"),
            2 => format!("{root_path}/manifest"),
            3 => format!("{root_path}/compacted"),
            _ => format!("{root_path}/compactions"),
        })
    };

    Toxic {
        name: format!("random-{index:02}-{kind_name}"),
        kind,
        direction,
        toxicity,
        operations,
        path_prefix,
    }
}

// A flag so we only initialize logging once.
static INIT_LOGGING: Once = Once::new();

/// Initialize logging for tests so we get log output. Uses `RUST_LOG` environment
/// variable to set the log level, or defaults to `info` if not set.
#[ctor::ctor]
fn init_tracing() {
    INIT_LOGGING.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_test_writer()
            .init();
    });
}
