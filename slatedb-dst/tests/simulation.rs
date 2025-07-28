//! # Deterministic Simulation Tests
//!
//! ## Usage
//!
//! These tests can only be run when DST is enabled. Use one of the following commands to run them:
//!
//! - `RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo test test_dst --all-features`
//! - `RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo nextest run test_dst  --profile dst`
#![cfg(all(dst, tokio_unstable))]

use rand::Rng;
use rstest::rstest;
use slatedb::clock::MockLogicalClock;
use slatedb::clock::MockSystemClock;
use slatedb::clock::SystemClock;
use slatedb::DbRand;
use slatedb::Error;
use slatedb_dst::utils::{build_dst, build_runtime, run_simulation};
use slatedb_dst::DstOptions;
use std::rc::Rc;
use std::sync::Arc;
use tracing::error;
use tracing::info;

/// Runs some DSTs with a small number of iterations. This is just a brief safety
/// check to be run against PRs.
///
/// # Arguments
///
/// * `system_clock` - The system clock to use for the simulation.
/// * `rand` - The random number generator to use for the simulation.
/// * `iterations` - The number of steps to run for the simulation.
/// * `dst_opts` - The DST options to use for the simulation.
#[rstest]
#[case(
    Arc::new(MockSystemClock::new()),
    Rc::new(DbRand::new(1)),
    100,
    DstOptions::default()
)]
#[case(
    Arc::new(MockSystemClock::new()),
    Rc::new(DbRand::new(2)),
    100,
    DstOptions::default()
)]
#[case(
    Arc::new(MockSystemClock::new()),
    Rc::new(DbRand::new(3)),
    100,
    DstOptions::default()
)]
fn test_dst(
    #[case] system_clock: Arc<dyn SystemClock>,
    #[case] rand: Rc<DbRand>,
    #[case] iterations: u32,
    #[case] dst_opts: DstOptions,
) -> Result<(), Error> {
    let runtime = build_runtime(rand.seed());
    let logical_clock = Arc::new(MockLogicalClock::new());
    runtime.block_on(async move {
        run_simulation(system_clock, logical_clock, rand, iterations, dst_opts).await
    })
}

/// Verifies that SlateDB is deterministic when we seed the random number
/// generator, system clock, and runtime appropriately. DSTs are not meaningful
/// if SlateDB is not deterministic when configured for DSTs.
///
/// The test runs multiple simulations with the same seed. After each simulation,
/// it verifies that the random number generator and system clock are in the same
/// state as they were before the simulation. It does this by generating a random
/// u64 and a getting the current system time from the clock after each simulation
/// and verifying that they are the same for each simulation run.
///
/// # Arguments
///
/// * `seed` - The seed to use for the random number generator and system clock.
/// * `simulations` - The number of simulations to run.
/// * `iterations` - The number of iterations to run for each simulation.
#[rstest]
#[case(101, 10, 50)]
#[case(102, 10, 50)]
#[case(103, 10, 50)]
#[case(104, 10, 50)]
#[case(105, 10, 50)]
#[case(106, 10, 50)]
#[case(107, 10, 50)]
#[case(108, 10, 50)]
#[case(109, 10, 50)]
#[case(110, 10, 50)]
fn test_dst_is_deterministic(
    #[case] seed: u64,
    #[case] simulations: u32,
    #[case] iterations: u32,
) -> Result<(), Error> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut expected_u64: Option<u64> = None;
    let mut expected_time: Option<SystemTime> = None;

    for simulation_count in 0..simulations {
        let rand = Rc::new(DbRand::new(seed));
        let system_clock = Arc::new(MockSystemClock::new());
        let logical_clock = Arc::new(MockLogicalClock::new());
        let runtime = build_runtime(rand.rng().random::<u64>());
        runtime.block_on(async {
            let mut dst = build_dst(system_clock.clone(), logical_clock.clone(), rand.clone(), DstOptions::default()).await;
            info!(seed, simulation_count, "running simulation");
            match dst.run_simulation(iterations).await {
                Ok(()) => {
                    let next_u64 = rand.rng().random::<u64>();
                    let next_time = system_clock.now();
                    if let Some(expected_u64) = expected_u64 {
                        assert_eq!(
                            next_u64, expected_u64,
                            "non-determinism detected: seed={}, simulation_count={}, next_u64={}, expected_u64={}",
                            seed, simulation_count, next_u64, expected_u64
                        );
                    }
                    if let Some(expected_time) = expected_time {
                        assert_eq!(
                            next_time,
                            expected_time,
                            "non-determinism detected: seed={}, simulation_count={}, next_time={:?}, expected_time={:?}",
                            seed,
                            simulation_count,
                            next_time.duration_since(UNIX_EPOCH),
                            expected_time.duration_since(UNIX_EPOCH)
                        );
                    }
                    info!(seed, simulation_count, "simulation passed");
                    expected_u64 = Some(next_u64);
                    expected_time = Some(next_time);
                    Ok(())
                }
                Err(e) => {
                    error!("simulation failed with seed {}: {}", seed, e);
                    Err(e)
                }
            }
        })?;
    }
    Ok(())
}
