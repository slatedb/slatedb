//! Helpers for scenario-driven deterministic simulation testing.

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

use log::info;
use object_store::ObjectStore;
use rand::Rng;
use slatedb::config::{
    CompressionCodec, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions};
use slatedb::{Db, DbBuilder, DbRand, Error, KeyValue, Settings};
use slatedb_common::clock::MockSystemClock;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use crate::object_store::ClockedObjectStore;
use crate::ScenarioContext;
use crate::{Dst, Scenario};

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
        compactor_options: None,
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
        .enable_time()
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build_local(Default::default())
        .unwrap()
}

/// Number of attempts to retry remote durability validation in `validate_get`
/// and `validate_scan` before panicking. This is needed to account for the
/// fact that the remote durable frontier may advance while we're trying to
/// validate against it, which can cause transient validation failures that
/// require retrying.
const REMOTE_VALIDATION_RETRY_LIMIT: usize = 128;

pub async fn validate_get<K>(
    ctx: &ScenarioContext,
    key: K,
    options: &ReadOptions,
) -> Result<Option<KeyValue>, Error>
where
    K: AsRef<[u8]> + Send,
{
    let key = key.as_ref().to_vec();
    for _attempt in 0..REMOTE_VALIDATION_RETRY_LIMIT {
        let snapshot = ctx.db().snapshot().await?;
        let snapshot_seq = snapshot.seq();
        let before_status =
            matches!(options.durability_filter, DurabilityLevel::Remote).then(|| ctx.db().status());
        let actual = snapshot.get_key_value_with_options(&key, options).await?;
        let after_status = ctx.db().status();

        if let Some(before_status) = before_status.as_ref() {
            if before_status.durable_seq != after_status.durable_seq {
                tokio::task::yield_now().await;
                continue;
            }
        }

        let visible_seq = match options.durability_filter {
            DurabilityLevel::Remote => snapshot_seq.min(after_status.durable_seq),
            DurabilityLevel::Memory => snapshot_seq,
            _ => snapshot_seq,
        };
        let expected = ctx
            .as_of(visible_seq)
            .get_key_value_with_options(&key, options)?;

        assert_eq!(
            actual,
            expected,
            "validate_get mismatch: scenario={} key={:?} options={:?} snapshot_seq={} visible_seq={} status={:?}",
            ctx.scenario(),
            key,
            options,
            snapshot_seq,
            visible_seq,
            after_status
        );

        return Ok(actual);
    }

    panic!(
        "validate_get could not obtain a stable remote durable frontier: scenario={} key={:?} options={:?}",
        ctx.scenario(),
        key,
        options
    )
}

pub async fn validate_scan<K, T>(
    ctx: &ScenarioContext,
    range: T,
    options: &ScanOptions,
) -> Result<Vec<KeyValue>, Error>
where
    K: AsRef<[u8]> + Send,
    T: RangeBounds<K> + Send + Clone + Debug,
{
    for _attempt in 0..REMOTE_VALIDATION_RETRY_LIMIT {
        let snapshot = ctx.db().snapshot().await?;
        let snapshot_seq = snapshot.seq();
        let before_status =
            matches!(options.durability_filter, DurabilityLevel::Remote).then(|| ctx.db().status());
        let mut actual_iter = snapshot.scan_with_options(range.clone(), options).await?;
        let mut actual = Vec::new();
        while let Some(kv) = actual_iter.next().await? {
            actual.push(kv);
        }
        let after_status = ctx.db().status();

        if let Some(before_status) = before_status.as_ref() {
            if before_status.durable_seq != after_status.durable_seq {
                tokio::task::yield_now().await;
                continue;
            }
        }

        let visible_seq = match options.durability_filter {
            DurabilityLevel::Remote => snapshot_seq.min(after_status.durable_seq),
            DurabilityLevel::Memory => snapshot_seq,
            _ => snapshot_seq,
        };
        let expected = ctx
            .as_of(visible_seq)
            .scan_with_options::<K, _>(range.clone(), options)?;

        assert_eq!(
            actual,
            expected,
            "validate_scan mismatch: scenario={} range={:?} options={:?} snapshot_seq={} visible_seq={} status={:?}",
            ctx.scenario(),
            range,
            options,
            snapshot_seq,
            visible_seq,
            after_status
        );

        return Ok(actual);
    }

    panic!(
        "validate_scan could not obtain a stable remote durable frontier: scenario={} range={:?} options={:?}",
        ctx.scenario(),
        range,
        options
    )
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
