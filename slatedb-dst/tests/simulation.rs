//! # Deterministic Simulation Tests
//!
//! ## Usage
//!
//! These tests can only be run when DST is enabled. Use one of the following commands to run them:
//!
//! - `RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo test test_dst --all-features`
//! - `RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo nextest run test_dst --profile dst`
//!
//! This module also contains a slow test that's meant to be run nightly. It is only run when
//! `slow`, `dst`, and `tokio_unstable` cfgs are all set.
#![cfg(all(dst, tokio_unstable))]

use object_store::memory::InMemory;
use rand::Rng;
use rstest::rstest;
use slatedb::clock::MockLogicalClock;
use slatedb::clock::MockSystemClock;
use slatedb::clock::SystemClock;
use slatedb::DbRand;
use slatedb::Error;
use slatedb_dst::utils::{build_dst, build_runtime, run_simulation};
use slatedb_dst::{DstDuration, DstOptions};
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
/// * `dst_duration` - The duration to run for the simulation.
/// * `dst_opts` - The DST options to use for the simulation.
#[rstest]
#[case(
    Arc::new(MockSystemClock::new()),
    Rc::new(DbRand::new(1)),
    DstDuration::Iterations(100),
    DstOptions::default()
)]
#[case(
    Arc::new(MockSystemClock::new()),
    Rc::new(DbRand::new(2)),
    DstDuration::Iterations(100),
    DstOptions::default()
)]
#[case(
    Arc::new(MockSystemClock::new()),
    Rc::new(DbRand::new(3)),
    DstDuration::Iterations(100),
    DstOptions::default()
)]
fn test_dst(
    #[case] system_clock: Arc<dyn SystemClock>,
    #[case] rand: Rc<DbRand>,
    #[case] dst_duration: DstDuration,
    #[case] dst_opts: DstOptions,
) -> Result<(), Error> {
    let object_store = Arc::new(InMemory::new());
    let runtime = build_runtime(rand.seed());
    let logical_clock = Arc::new(MockLogicalClock::new());
    runtime.block_on(async move {
        run_simulation(
            object_store,
            system_clock,
            logical_clock,
            rand,
            dst_duration,
            dst_opts,
        )
        .await
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
/// * `dst_duration` - The duration to run for each simulation.
#[rstest]
#[case(101, 10, DstDuration::Iterations(50))]
#[case(102, 10, DstDuration::Iterations(50))]
#[case(103, 10, DstDuration::Iterations(50))]
#[case(104, 10, DstDuration::Iterations(50))]
#[case(105, 10, DstDuration::Iterations(50))]
#[case(106, 10, DstDuration::Iterations(50))]
#[case(107, 10, DstDuration::Iterations(50))]
#[case(108, 10, DstDuration::Iterations(50))]
#[case(109, 10, DstDuration::Iterations(50))]
#[case(110, 10, DstDuration::Iterations(50))]
fn test_dst_is_deterministic(
    #[case] seed: u64,
    #[case] simulations: u32,
    #[case] dst_duration: DstDuration,
) -> Result<(), Error> {
    use chrono::{DateTime, Utc};

    let mut expected_u64: Option<u64> = None;
    let mut expected_time: Option<DateTime<Utc>> = None;

    for simulation_count in 0..simulations {
        let object_store = Arc::new(InMemory::new());
        let rand = Rc::new(DbRand::new(seed));
        let system_clock = Arc::new(MockSystemClock::new());
        let logical_clock = Arc::new(MockLogicalClock::new());
        let runtime = build_runtime(rand.rng().random::<u64>());
        runtime.block_on(async {
            let mut dst = build_dst(object_store.clone(), system_clock.clone(), logical_clock.clone(), rand.clone(), DstOptions::default()).await?;
            info!(seed, simulation_count, "running simulation");
            match dst.run_simulation(dst_duration).await {
                Ok(()) => {
                    let next_u64 = rand.rng().random::<u64>();
                    let next_time = system_clock.now();
                    if let Some(expected_u64) = expected_u64 {
                        assert_eq!(
                            next_u64, expected_u64,
                            "non-determinism detected [seed={}, simulation_count={}, next_u64={}, expected_u64={}]",
                            seed, simulation_count, next_u64, expected_u64
                        );
                    }
                    if let Some(expected_time) = expected_time {
                        assert_eq!(
                            next_time,
                            expected_time,
                            "non-determinism detected [seed={}, simulation_count={}, next_time={:?}, expected_time={:?}]",
                            seed,
                            simulation_count,
                            next_time,
                            expected_time,
                        );
                    }
                    info!(seed, simulation_count, "simulation passed");
                    expected_u64 = Some(next_u64);
                    expected_time = Some(next_time);
                    Ok(())
                }
                Err(e) => {
                    error!("simulation failed [seed={}, error={}]", seed, e);
                    Err(e)
                }
            }
        })?;
    }
    Ok(())
}

/// Runs one DST per-core for a long time on all available CPU cores. Tests run for
/// 50 minutes. Each core runs a DST with a different seed, and its own `Db`. Each
/// `Db` uses a `LocalFileSystem` for its object store. The test will create a
/// subdirectory for each object store inside the directory specified by the
/// `SLATEDB_DST_ROOT` environment variable.
///
/// Set the following environment variables to run this test:
///
/// - `RUSTFLAGS="--cfg dst --cfg slow --cfg tokio_unstable"`
/// - `SLATEDB_DST_ROOT` must be set to a directory to store test data.
#[test]
#[cfg(slow)]
fn test_dst_nightly() -> Result<(), Error> {
    use object_store::local::LocalFileSystem;
    use slatedb_dst::DstDuration;
    use std::path::PathBuf;
    use sysinfo::System;

    let test_root: PathBuf = std::env::var("SLATEDB_DST_ROOT")
        .expect("SLATEDB_DST_ROOT must be set to a directory to store test data")
        .into();
    let mut handles = Vec::new();
    let mut system = System::new();
    system.refresh_cpu_all();
    let num_cores = system.cpus().len();
    info!("running nightly [num_cores={}]", num_cores);
    for core in 0..num_cores {
        let test_dir = test_root.join(format!("core-{}", core));
        std::fs::create_dir_all(&test_dir).expect("failed to create test root");
        let handle = std::thread::spawn(move || {
            let object_store = Arc::new(
                LocalFileSystem::new_with_prefix(test_dir)
                    .expect("failed to create object store")
                    .with_automatic_cleanup(true),
            );
            let seed = rand::rng().random::<u64>();
            let rand = Rc::new(DbRand::new(seed));
            let runtime = build_runtime(rand.seed());
            let system_clock = Arc::new(MockSystemClock::new());
            let logical_clock = Arc::new(MockLogicalClock::new());
            let duration = DstDuration::WallClock(std::time::Duration::from_secs(60)); // 12 minutes
            runtime.block_on(async move {
                let span = tracing::info_span!("run_simulation", core = core, seed = seed);
                let _enter = span.enter();
                run_simulation(
                    object_store,
                    system_clock,
                    logical_clock,
                    rand,
                    duration,
                    DstOptions::default(),
                )
                .await
            })
        });
        handles.push(handle);
    }
    for (core, handle) in handles.into_iter().enumerate() {
        let result = handle.join().expect("join failed");
        info!("simulation result [core={}, result={:?}]", core, result);
    }
    Ok(())
}
