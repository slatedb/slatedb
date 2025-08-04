//! This module contains helper functions to simplify deterministic simulation (DST).

use rand::Rng;
use slatedb::clock::LogicalClock;
use slatedb::clock::SystemClock;
use slatedb::config::CompactorOptions;
use slatedb::config::CompressionCodec;
use slatedb::config::GarbageCollectorOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use slatedb::DbBuilder;
use slatedb::DbRand;
use slatedb::Error;
use slatedb::Settings;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use crate::DefaultDstDistribution;
use crate::Dst;
use crate::DstOptions;

const MIB_1: usize = 1024 * 1024;
const MIB_500: usize = 500 * MIB_1;
const GIB_5: usize = 5 * MIB_500;

const COMPRESSION_CODECS: [Option<&str>; 5] = [
    Some("snappy"),
    Some("zlib"),
    Some("lz4"),
    Some("zstd"),
    None,
];

/// Builds a [Dst] instance (including its [Db]) with [Settings] that are selected
/// at random.
///
/// All arguments are expected to be deterministic.
pub async fn build_dst(
    system_clock: Arc<dyn SystemClock>,
    logical_clock: Arc<dyn LogicalClock>,
    rand: Rc<DbRand>,
    dst_opts: DstOptions,
) -> Dst {
    let db = build_db(system_clock.clone(), logical_clock.clone(), &rand).await;
    Dst::new(
        db,
        system_clock,
        rand.clone(),
        Box::new(DefaultDstDistribution::new(rand, dst_opts.clone())),
        dst_opts,
    )
}

/// Builds a DB instance with [Settings] that are selected at random.
///
/// All arguments are expected to be deterministic.
pub async fn build_db(
    system_clock: Arc<dyn SystemClock>,
    logical_clock: Arc<dyn LogicalClock>,
    rand: &DbRand,
) -> Db {
    let mut builder = DbBuilder::new("test_db", Arc::new(InMemory::new()));
    builder = builder.with_settings(build_settings(rand).await);
    builder = builder.with_seed(rand.rng().random_range(0..u64::MAX));
    builder = builder.with_system_clock(system_clock.clone());
    builder = builder.with_logical_clock(logical_clock.clone());
    builder.build().await.unwrap()
}

/// Builds a Settings instance with random values.
///
/// All arguments are expected to be deterministic.
pub async fn build_settings(rand: &DbRand) -> Settings {
    let mut rng = rand.rng();
    let flush_interval = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let manifest_poll_interval = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let manifest_update_timeout = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let min_filter_keys = rng.random_range(100..1000);
    let filter_bits_per_key = rng.random_range(1..20);
    let l0_sst_size_bytes = rng.random_range(MIB_1..MIB_500);
    let l0_max_ssts = rng.random_range(1..100);
    let max_unflushed_bytes = rng.random_range(MIB_1..GIB_5);
    let compression_codec_idx = rng.random_range(0..COMPRESSION_CODECS.len());
    let compression_codec =
        if let Some(compression_codec) = COMPRESSION_CODECS[compression_codec_idx] {
            CompressionCodec::from_str(compression_codec).ok()
        } else {
            None
        };

    Settings {
        flush_interval: Some(flush_interval),
        manifest_poll_interval,
        manifest_update_timeout,
        min_filter_keys,
        filter_bits_per_key,
        l0_sst_size_bytes,
        l0_max_ssts,
        max_unflushed_bytes,
        compression_codec,
        garbage_collector_options: Some(GarbageCollectorOptions {
            manifest_options: GarbageCollectorOptions::default().manifest_options,
            wal_options: GarbageCollectorOptions::default().wal_options,
            compacted_options: GarbageCollectorOptions::default().compacted_options,
        }),
        compactor_options: Some(CompactorOptions::default()),
        wal_enabled: rng.random_bool(0.5),
        ..Default::default()
    }
}

/// Tokio's default Runtime is non-deterministic even if a single thread is used.
/// Certain methods such as [tokio::select] pick a branch to poll at random (see
/// [tokio::select!](https://docs.rs/tokio/latest/tokio/macro.select.html#fairness)).
///
/// This function uses a seed to build a deterministic Tokio runtime.
///
/// `RUSTFLAGS="--cfg tokio_unstable"` must be set, and Tokio's `rt` feature
/// must be enabled to use this function. See [tokio::runtime::Builder::rng_seed] for
/// more details.
#[cfg(tokio_unstable)]
pub fn build_runtime(seed: u64) -> tokio::runtime::LocalRuntime {
    use tokio::runtime::RngSeed;

    // https://pierrezemb.fr/posts/tokio-hidden-gems/
    tokio::runtime::Builder::new_current_thread()
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build_local(&mut Default::default())
        .unwrap()
}

/// Builds a [Dst] instance (including its [Db]) with [Settings] that are selected
/// at random. Then runs a simulation for the given number of iterations.
///
/// All arguments are expected to be deterministic.
pub async fn run_simulation(
    system_clock: Arc<dyn SystemClock>,
    logical_clock: Arc<dyn LogicalClock>,
    rand: Rc<DbRand>,
    iterations: u32,
    dst_opts: DstOptions,
) -> Result<(), Error> {
    let seed = rand.seed();
    info!("running simulation [seed={}]", seed);
    let mut dst = build_dst(
        system_clock.clone(),
        logical_clock.clone(),
        rand.clone(),
        dst_opts,
    )
    .await;
    match dst.run_simulation(iterations).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("simulation failed [seed={}, error={}]", seed, e);
            Err(e)
        }
    }
}

/// Formats a number of bytes in a human-readable way.
pub(crate) fn pretty_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{}b", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{}kb", bytes / 1024)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{}mb", bytes / (1024 * 1024))
    } else {
        format!("{0:.2}gb", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
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
