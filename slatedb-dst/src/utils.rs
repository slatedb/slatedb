//! Helpers for scenario-driven deterministic simulation testing.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

use log::info;
use object_store::ObjectStore;
use rand::Rng;
use slatedb::config::ScanOptions;
use slatedb::config::{
    CompactorOptions, CompressionCodec, DurabilityLevel, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, SizeTieredCompactionSchedulerOptions,
};
use slatedb::{Db, DbBuilder, DbRand, Error, Settings};
use slatedb_common::clock::MockSystemClock;
use std::rc::Rc;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use crate::object_store::ClockedObjectStore;
use crate::scenarios::{ScanScenario, TimedShutdownScenario};
use crate::ScenarioContext;
use crate::{Dst, Scenario};

/// Builds a deterministic DST run, executes the supplied scenarios, validates
/// the final SlateDB state against the SQLite model, and then closes the DB.
///
/// The simulation uses a randomized-but-deterministic
/// [`slatedb::config::Settings`] instance derived from `rand`, plus a separate
/// DB builder seed drawn from the same RNG stream. After all scenarios finish,
/// the helper performs a final front-to-back scan comparison between the real
/// DB snapshot and the recorded SQLite state before shutting the database down.
///
/// ## Arguments
///
/// - `object_store`: Object store backing the SlateDB instance for this run.
/// - `system_clock`: Shared mock clock used by SlateDB and available to the
///   caller for post-run determinism checks.
/// - `rand`: Shared deterministic RNG that drives settings generation, DB
///   seeding, and scenario behavior.
/// - `simulation_scenarios`: Scenario tasks to run concurrently against the
///   shared DST instance.
/// - `wall_clock_time`: Optional real-time limit. When present, a timed
///   shutdown scenario is added so open-ended simulations terminate after the
///   specified duration.
pub async fn run_simulation(
    object_store: Arc<dyn ObjectStore>,
    system_clock: Arc<MockSystemClock>,
    rand: Rc<DbRand>,
    mut simulation_scenarios: Vec<Box<dyn Scenario>>,
    wall_clock_time: Option<Duration>,
) -> Result<(), Error> {
    let settings = build_settings(&rand);
    let db_seed = rand.rng().random::<u64>();
    let db = build_scenario_db(
        object_store,
        system_clock.clone(),
        db_seed,
        settings.clone(),
    )
    .await?;
    let dst = Dst::new(db, system_clock.clone(), settings);

    if let Some(duration) = wall_clock_time {
        simulation_scenarios.push(Box::new(TimedShutdownScenario {
            name: "wall-clock",
            duration,
        }));
    }

    dst.run_scenarios(simulation_scenarios).await?;

    let verifier = dst.context("verifier");
    verify_final_state(&verifier).await?;

    dst.close().await?;

    Ok(())
}

async fn verify_final_state(ctx: &ScenarioContext) -> Result<(), Error> {
    ScanScenario::validate_full_range(ctx, &ScanOptions::default()).await?;
    ScanScenario::validate_full_range(
        ctx,
        &ScanOptions::default().with_durability_filter(DurabilityLevel::Remote),
    )
    .await?;
    Ok(())
}

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

/// Builds a Settings instance with random values.
///
/// All arguments are expected to be deterministic.
pub fn build_settings(rand: &DbRand) -> Settings {
    let mut rng = rand.rng();
    let flush_interval = rng.random_range(Duration::from_millis(1)..Duration::from_secs(60));
    let manifest_poll_interval = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let manifest_update_timeout = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let min_filter_keys = rng.random_range(100..1000);
    let filter_bits_per_key = rng.random_range(1..20);
    let l0_sst_size_bytes = rng.random_range(MIB_1..MIB_500);
    let l0_max_ssts = rng.random_range(4..8); // max L0 size of 4GiB (8 * 500MiB l0 sst size)
    let max_unflushed_bytes = rng.random_range(MIB_1..GIB_2);
    let compression_codec_idx = rng.random_range(0..COMPRESSION_CODECS.len());
    let compression_codec = COMPRESSION_CODECS[compression_codec_idx]
        .and_then(|name| CompressionCodec::from_str(name).ok());
    // Otherwise, the compactor never runs and writers get blocked permanently.
    let min_compaction_sources = rng.random_range(4..10).min(l0_max_ssts);
    // Prevent scheduler from having a higher min compaction sources than max compaction sources.
    let max_compaction_sources = 8.max(min_compaction_sources);

    let settings = Settings {
        flush_interval: Some(flush_interval),
        manifest_poll_interval,
        manifest_update_timeout,
        min_filter_keys,
        filter_bits_per_key,
        l0_sst_size_bytes,
        l0_max_ssts,
        max_unflushed_bytes,
        compression_codec,
        compactor_options: Some(CompactorOptions {
            scheduler_options: SizeTieredCompactionSchedulerOptions {
                min_compaction_sources,
                max_compaction_sources,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }),
        garbage_collector_options: Some(GarbageCollectorOptions {
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
            compactions_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
        }),
        default_ttl: None,
        ..Default::default()
    };

    #[cfg(feature = "wal_disable")]
    {
        let mut settings = settings;
        settings.wal_enabled = rng.random_bool(0.5);
        return settings;
    }

    settings
}

/// Builds a scenario DB with explicit settings and a deterministic seed.
pub async fn build_scenario_db(
    object_store: Arc<dyn ObjectStore>,
    system_clock: Arc<MockSystemClock>,
    seed: u64,
    settings: Settings,
) -> Result<Db, Error> {
    let object_store = Arc::new(ClockedObjectStore::new(object_store, system_clock.clone()));
    info!("building scenario db [seed={}]", seed);
    DbBuilder::new("test_db", object_store)
        .with_settings(settings)
        .with_seed(seed)
        .with_system_clock(system_clock)
        .build()
        .await
}

/// Builds a [`Dst`], runs the supplied scenarios, and returns the runner for
/// follow-up inspection and verification.
pub async fn run_scenarios<I>(
    db: Db,
    system_clock: Arc<MockSystemClock>,
    settings: Settings,
    scenarios: I,
) -> Result<Dst, Error>
where
    I: IntoIterator<Item = Box<dyn Scenario>>,
{
    let dst = Dst::new(db, system_clock, settings);
    dst.run_scenarios(scenarios).await?;
    Ok(dst)
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

    tokio::runtime::Builder::new_current_thread()
        .enable_time() // Required ONLY for TimedShutdownScenario
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build_local(Default::default())
        .unwrap()
}

static INIT_LOGGING: Once = Once::new();

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
