// These tests are only enabled when DST is enabled.
// To run the tests, use one of:
// - `RUSTFLAGS="--cfg dst" cargo test test_dst --all-features`
// - `RUSTFLAGS="--cfg dst" cargo nextest run test_dst  --profile dst`
#![cfg(dst)]

use rand::Rng;
use rstest::rstest;
use slatedb::clock::MockSystemClock;
use slatedb::clock::SystemClock;
use slatedb::DbRand;
use slatedb::SlateDBError;
use slatedb_dst::utils::{build_dst, run_simulation};
use slatedb_dst::DstOptions;
use std::rc::Rc;
use std::sync::Arc;
use tracing::error;

#[rstest]
#[case(Arc::new(MockSystemClock::new()), Rc::new(DbRand::new(1)), 100)]
#[case(Arc::new(MockSystemClock::new()), Rc::new(DbRand::new(2)), 100)]
#[tokio::test(start_paused = true, flavor = "current_thread")]
async fn test_dst_short(
    #[case] system_clock: Arc<dyn SystemClock>,
    #[case] rand: Rc<DbRand>,
    #[case] iterations: u32,
) -> Result<(), SlateDBError> {
    run_simulation(system_clock, rand, iterations, DstOptions::default()).await
}

#[rstest]
#[case(
    Arc::new(MockSystemClock::new()),
    Rc::new(DbRand::new(6561056955098952705)),
    99999,
    DstOptions::default()
)]
#[tokio::test(start_paused = true, flavor = "current_thread")]
async fn test_dst_regressions(
    #[case] system_clock: Arc<dyn SystemClock>,
    #[case] rand: Rc<DbRand>,
    #[case] iterations: u32,
    #[case] dst_opts: DstOptions,
) -> Result<(), SlateDBError> {
    run_simulation(system_clock, rand, iterations, dst_opts).await
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
#[tokio::test(start_paused = true, flavor = "current_thread")]
#[rstest]
#[case(101, 10, 100)]
#[case(102, 10, 100)]
#[case(103, 10, 100)]
#[case(104, 10, 100)]
#[case(105, 10, 100)]
async fn test_dst_is_deterministic(
    #[case] seed: u64,
    #[case] simulations: u32,
    #[case] iterations: u32,
) -> Result<(), SlateDBError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut expected_u64: Option<u64> = None;
    let mut expected_time: Option<SystemTime> = None;

    for _i in 0..simulations {
        let rand = Rc::new(DbRand::new(seed));
        let system_clock = Arc::new(MockSystemClock::new());
        let mut dst = build_dst(system_clock.clone(), rand.clone(), DstOptions::default()).await;
        match dst.run_simulation(iterations).await {
            Ok(()) => {
                let next_u64 = rand.rng().random::<u64>();
                let next_time = system_clock.now();
                if let Some(expected_u64) = expected_u64 {
                    assert_eq!(
                        next_u64, expected_u64,
                        "non-determinism detected: seed={}, next_u64={}, expected_u64={}",
                        seed, next_u64, expected_u64
                    );
                }
                if let Some(expected_time) = expected_time {
                    assert_eq!(
                        next_time,
                        expected_time,
                        "non-determinism detected: seed={}, next_time={:?}, expected_time={:?}",
                        seed,
                        next_time.duration_since(UNIX_EPOCH),
                        expected_time.duration_since(UNIX_EPOCH)
                    );
                }
                expected_u64 = Some(next_u64);
                expected_time = Some(next_time);
            }
            Err(e) => {
                error!("simulation failed with seed {}: {}", seed, e);
                return Err(e);
            }
        }
    }
    Ok(())
}
