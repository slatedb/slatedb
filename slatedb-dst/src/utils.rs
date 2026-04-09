//! Helpers for scenario-driven deterministic simulation testing.

use std::sync::Arc;
use std::sync::Once;

use log::info;
use object_store::ObjectStore;
use slatedb::{Db, DbBuilder, Error, Settings};
use slatedb_common::clock::MockSystemClock;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use crate::object_store::ClockedObjectStore;
use crate::{Dst, Scenario};

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
    let dst = Dst::new(db, system_clock, settings)?;
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
