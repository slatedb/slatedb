//! # Deterministic Simulation Tests
//!
//! ## Usage
//!
//! These tests can only be run when DST is enabled. Use one of the following commands to run
//! them:
//!
//! - `RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo test -p slatedb-dst --test simulation`
//! - `RUSTFLAGS="--cfg dst --cfg tokio_unstable" cargo nextest run -p slatedb-dst --profile dst --test simulation`
//!
//! This module also contains a slow test that's meant to be run nightly. It is only run when
//! `slow`, `dst`, and `tokio_unstable` cfgs are all set.
#![cfg(all(dst, tokio_unstable))]

use std::rc::Rc;
use std::sync::Arc;
#[cfg(slow)]
use std::time::Duration;

use chrono::{DateTime, Utc};
use object_store::memory::InMemory;
use rand::Rng;
use rstest::rstest;
use slatedb::{DbRand, Error};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::scenarios::{ClockScenario, FlusherScenario, ReaderScenario, WriterScenario};
use slatedb_dst::utils::{build_runtime, run_simulation};
use slatedb_dst::Scenario;
#[cfg(slow)]
use tracing::info_span;
use tracing::{error, info};

#[cfg(slow)]
const NIGHTLY_WALL_CLOCK: Duration = Duration::from_secs(12 * 60);
const MAX_KEY_SPACE: u64 = 1024;

/// Verifies that SlateDB is deterministic when we seed the random number generator, system
/// clock, and runtime appropriately.
///
/// The scenario harness uses a single shared `DbRand` across all local tasks. If task scheduling
/// changes, the RNG will be consumed in a different order and the post-simulation state will
/// differ.
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
#[case(111, 10, 50)]
#[case(112, 10, 50)]
#[case(113, 10, 50)]
#[case(114, 10, 50)]
#[case(115, 10, 50)]
#[case(116, 10, 50)]
#[case(117, 10, 50)]
#[case(118, 10, 50)]
#[case(119, 10, 50)]
#[case(120, 10, 50)]
fn test_dst_is_deterministic(
    #[case] seed: u64,
    #[case] simulations: u32,
    #[case] iterations: u32,
) -> Result<(), Error> {
    let mut expected_next_u64: Option<u64> = None;
    let mut expected_next_time: Option<DateTime<Utc>> = None;

    for simulation_count in 0..simulations {
        let object_store = Arc::new(InMemory::new());
        let rand = Rc::new(DbRand::new(seed));
        let system_clock = Arc::new(MockSystemClock::new());
        let writer_key_space = rand.rng().random_range(1..(MAX_KEY_SPACE + 1));
        let reader_key_space = rand.rng().random_range(1..(MAX_KEY_SPACE + 1));
        let mut simulation_scenarios: Vec<Box<dyn Scenario>> = Vec::with_capacity(11);

        // Add scenarios.
        for name in ["writer-0", "writer-1", "writer-2", "writer-3"] {
            simulation_scenarios.push(Box::new(WriterScenario {
                name,
                rand: rand.clone(),
                key_space: writer_key_space,
                iterations: Some(iterations),
            }));
        }
        for name in ["reader-0", "reader-1", "reader-2", "reader-3"] {
            simulation_scenarios.push(Box::new(ReaderScenario {
                name,
                rand: rand.clone(),
                key_space: reader_key_space,
                iterations: Some(iterations),
            }));
        }
        for name in ["flusher-0", "flusher-1"] {
            simulation_scenarios.push(Box::new(FlusherScenario {
                name,
                rand: rand.clone(),
                iterations: Some(iterations),
            }));
        }
        simulation_scenarios.push(Box::new(ClockScenario {
            name: "clock",
            rand: rand.clone(),
        }));

        // Run the simulation.
        let runtime = build_runtime(rand.rng().random::<u64>());
        let rand_for_run = rand.clone();
        let system_clock_for_run = system_clock.clone();
        runtime.block_on(async {
            info!(seed, simulation_count, iterations, "running simulation");
            match run_simulation(
                object_store,
                system_clock_for_run,
                rand_for_run,
                simulation_scenarios,
                None,
            )
            .await
            {
                Ok(()) => {
                    info!(seed, simulation_count, iterations, "simulation passed");
                    Ok(())
                }
                Err(err) => {
                    error!("simulation failed [seed={}, error={}]", seed, err);
                    Err(err)
                }
            }
        })?;

        // Verify the RNG/clock always match on each run.
        let next_u64 = Some(rand.rng().random::<u64>());
        let next_time = Some(system_clock.now());

        if expected_next_time.is_none() && expected_next_u64.is_none() {
            expected_next_time = next_time;
            expected_next_u64 = next_u64;
        }

        assert_eq!(
            next_time, expected_next_time,
            "non-determinism detected [seed={}, simulation_count={}, next_time={:?}, expected_time={:?}]",
            seed, simulation_count, next_time, expected_next_time
        );
        assert_eq!(
            next_u64, expected_next_u64,
            "non-determinism detected [seed={}, simulation_count={}, next_u64={:?}, expected_u64={:?}]",
            seed, simulation_count, next_u64, expected_next_u64
        );
    }

    Ok(())
}

/// Runs one DST per-core on all available CPU cores.
///
/// Each core gets a unique seed, its own `Db`, and a `LocalFileSystem` object store rooted under
/// `SLATEDB_DST_ROOT`.
///
/// Set the following environment variables to run this test:
///
/// - `RUSTFLAGS="--cfg dst --cfg slow --cfg tokio_unstable"`
/// - `SLATEDB_DST_ROOT` must be set to a directory to store test data.
#[test]
#[cfg(slow)]
fn test_dst_nightly() -> Result<(), Error> {
    use object_store::local::LocalFileSystem;
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
        if test_dir.exists() {
            std::fs::remove_dir_all(&test_dir).expect("failed to clear stale test dir");
        }
        std::fs::create_dir_all(&test_dir).expect("failed to create test root");
        let handle = std::thread::spawn(move || -> Result<(), String> {
            let object_store = Arc::new(
                LocalFileSystem::new_with_prefix(test_dir)
                    .expect("failed to create object store")
                    .with_automatic_cleanup(true),
            );
            let seed = rand::rng().random::<u64>();
            let rand = Rc::new(DbRand::new(seed));
            let runtime = build_runtime(rand.seed());
            let system_clock = Arc::new(MockSystemClock::new());
            let writer_key_space = rand.rng().random_range(1..(MAX_KEY_SPACE + 1));
            let reader_key_space = rand.rng().random_range(1..(MAX_KEY_SPACE + 1));
            let mut simulation_scenarios: Vec<Box<dyn Scenario>> = Vec::with_capacity(11);
            for name in ["writer-0", "writer-1", "writer-2", "writer-3"] {
                simulation_scenarios.push(Box::new(WriterScenario {
                    name,
                    rand: rand.clone(),
                    key_space: writer_key_space,
                    iterations: None,
                }));
            }
            for name in ["reader-0", "reader-1", "reader-2", "reader-3"] {
                simulation_scenarios.push(Box::new(ReaderScenario {
                    name,
                    rand: rand.clone(),
                    key_space: reader_key_space,
                    iterations: None,
                }));
            }
            for name in ["flusher-0", "flusher-1"] {
                simulation_scenarios.push(Box::new(FlusherScenario {
                    name,
                    rand: rand.clone(),
                    iterations: None,
                }));
            }
            simulation_scenarios.push(Box::new(ClockScenario {
                name: "clock",
                rand: rand.clone(),
            }));
            runtime.block_on(async move {
                let span = info_span!("run_simulation", core = core, seed = seed);
                let _enter = span.enter();
                run_simulation(
                    object_store,
                    system_clock,
                    rand,
                    simulation_scenarios,
                    Some(NIGHTLY_WALL_CLOCK),
                )
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
            })
        });
        handles.push(handle);
    }

    let failed = handles
        .into_iter()
        .enumerate()
        .any(|(core, handle)| match handle.join() {
            Ok(Ok(())) => {
                info!("simulation passed [core={}]", core);
                false
            }
            Ok(Err(err)) => {
                error!("simulation failed [core={}, error={}]", core, err);
                true
            }
            Err(err) => {
                error!("simulation failed [core={}, result={:?}]", core, err);
                true
            }
        });

    assert!(!failed, "one or more DSTs failed");
    Ok(())
}
