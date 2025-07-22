use rand::Rng;
use slatedb::clock::SystemClock;
use slatedb::config::CompactorOptions;
use slatedb::config::CompressionCodec;
use slatedb::config::GarbageCollectorOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use slatedb::DbBuilder;
use slatedb::DbRand;
use slatedb::Settings;
use slatedb::SlateDBError;
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

pub async fn build_dst(
    system_clock: Arc<dyn SystemClock>,
    rand: Rc<DbRand>,
    dst_opts: DstOptions,
) -> Dst {
    let db = build_db(system_clock.clone(), &rand).await;
    Dst::new(
        db,
        system_clock,
        rand.clone(),
        Box::new(DefaultDstDistribution::new(rand, dst_opts.clone())),
        dst_opts,
    )
}

/// Builds a DB instance with components that are selected at random.
pub async fn build_db(system_clock: Arc<dyn SystemClock>, rand: &DbRand) -> Db {
    let mut builder = DbBuilder::new("test_db", Arc::new(InMemory::new()));
    builder = builder.with_settings(build_settings(rand).await);
    builder = builder.with_seed(rand.rng().random_range(0..u64::MAX));
    builder = builder.with_system_clock(system_clock.clone());
    builder.build().await.unwrap()
}

/// Builds a Settings instance with random values.
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
            match CompressionCodec::from_str(compression_codec) {
                Ok(codec) => Some(codec),
                Err(_) => None,
            }
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
        // default_ttl,
        compression_codec,
        // TODO: add object store filesystem cache configs
        // TODO: add random GC configs
        garbage_collector_options: Some(GarbageCollectorOptions {
            manifest_options: GarbageCollectorOptions::default().manifest_options,
            wal_options: GarbageCollectorOptions::default().wal_options,
            compacted_options: GarbageCollectorOptions::default().compacted_options,
        }),
        // TODO: add random compactor configs
        compactor_options: Some(CompactorOptions::default()),
        wal_enabled: rng.random_bool(0.5),
        ..Default::default()
    }
}

#[cfg(tokio_unstable)]
pub fn build_runtime(seed: u64) -> tokio::runtime::LocalRuntime {
    use tokio::runtime::RngSeed;

    // https://pierrezemb.fr/posts/tokio-hidden-gems/
    tokio::runtime::Builder::new_current_thread()
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build_local(&mut Default::default())
        .unwrap()
}

pub async fn run_simulation(
    system_clock: Arc<dyn SystemClock>,
    rand: Rc<DbRand>,
    iterations: u32,
    dst_opts: DstOptions,
) -> Result<(), SlateDBError> {
    let seed = rand.seed();
    info!("running simulation with seed {}", seed);
    let mut dst = build_dst(system_clock.clone(), rand.clone(), dst_opts).await;
    match dst.run_simulation(iterations).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("simulation failed with seed {}: {}", seed, e);
            Err(e)
        }
    }
}

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

pub(crate) fn pretty_duration(d: &Duration) -> String {
    let total_secs = d.as_secs();
    let weeks = total_secs / 604_800;
    let days = (total_secs % 604_800) / 86_400;
    let hours = (total_secs % 86_400) / 3_600;
    let mins = (total_secs % 3_600) / 60;
    let secs = total_secs % 60;
    let millis = d.subsec_millis();
    let micros = d.subsec_micros();

    let mut parts = Vec::new();
    if weeks > 0 {
        parts.push(format!("{}w", weeks));
    }
    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if mins > 0 {
        parts.push(format!("{}m", mins));
    }
    if secs > 0 {
        parts.push(format!("{}s", secs));
    }
    if millis > 0 {
        parts.push(format!("{}ms", millis));
    } else if micros > 0 {
        parts.push(format!("{}μs", micros));
    }

    if parts.is_empty() {
        // handle zero-duration specially
        "0s".to_string()
    } else {
        parts.join(" ")
    }
}

// A flag so we only initialize logging once.
static INIT_LOGGING: Once = Once::new();

/// Initialize logging for tests so we get log output. Uses `RUST_LOG` environment
/// variable to set the log level, or defaults to `debug` if not set.
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
