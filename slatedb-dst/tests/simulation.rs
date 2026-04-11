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

#[path = "simulation/scenarios.rs"]
mod scenarios;

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::Rng;
use rstest::rstest;
use slatedb::{DbRand, Error};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::utils::{build_runtime, build_scenario_db, build_settings};
use slatedb_dst::{Dst, Scenario, ScenarioContext};
#[cfg(slow)]
use tracing::info_span;
use tracing::{error, info};

#[cfg(slow)]
const NIGHTLY_WALL_CLOCK: Duration = Duration::from_secs(12 * 60);

/// Builds a deterministic DST run, executes the supplied scenarios, validates
/// the final SlateDB state against the SQLite model, and returns the values the
/// determinism test compares across repeated runs.
///
/// The simulation uses a randomized-but-deterministic
/// [`slatedb::config::Settings`] instance derived from `rand`, plus a separate
/// DB builder seed drawn from the same RNG stream. After all scenarios finish,
/// the helper performs a final front-to-back scan comparison between the real
/// DB snapshot and the recorded SQLite state. If that verification succeeds,
/// the helper captures the next RNG value and current mock time so the caller
/// can assert that scheduler and state evolution stayed deterministic across
/// identical runs.
///
/// ## Arguments
///
/// - `object_store`: Object store backing the SlateDB instance for this run.
/// - `system_clock`: Shared mock clock used both by SlateDB and by the
///   determinism check's final timestamp capture.
/// - `rand`: Shared deterministic RNG that drives settings generation, DB
///   seeding, and scenario behavior.
/// - `simulation_scenarios`: Scenario tasks to run concurrently against the
///   shared DST instance.
/// - `wall_clock_time`: Optional real-time limit. When present, a timed
///   shutdown scenario is added so open-ended simulations terminate after the
///   specified duration.
///
/// ## Returns
///
/// Returns `(final_seq, next_u64, next_time)` where:
///
/// * `final_seq` is the sequence number of the final DB snapshot after all
///   scenarios complete.
/// * `next_u64` is the next value drawn from `rand` after the run and final
///   verification.
/// * `next_time` is the mock clock's current time at the end of the run.
///
/// Together these values form the determinism signature compared across
/// repeated simulations with the same seed.
async fn run_simulation(
    object_store: Arc<dyn ObjectStore>,
    system_clock: Arc<MockSystemClock>,
    rand: Rc<DbRand>,
    mut simulation_scenarios: Vec<Box<dyn Scenario>>,
    wall_clock_time: Option<Duration>,
) -> Result<(u64, u64, DateTime<Utc>), Error> {
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
        simulation_scenarios.push(Box::new(scenarios::TimedShutdownScenario {
            name: "wall-clock",
            duration,
        }));
    }

    dst.run_scenarios(simulation_scenarios).await?;

    let verifier = dst.context("verifier");
    let final_seq = verify_final_state(&verifier).await?;

    let next_u64 = rand.rng().random::<u64>();
    let next_time = system_clock.now();

    dst.close().await?;

    Ok((final_seq, next_u64, next_time))
}

async fn verify_final_state(ctx: &ScenarioContext) -> Result<u64, Error> {
    let full_range = ..;
    let snapshot = ctx.db().snapshot().await?;
    let final_seq = snapshot.seq();
    let mut expected_iter = ctx
        .as_of(final_seq)
        .scan::<Vec<u8>, _>(full_range.clone())?
        .into_iter();
    let mut actual_iter = snapshot.scan::<Vec<u8>, _>(full_range).await?;
    for (row_index, expected) in expected_iter.by_ref().enumerate() {
        let actual = actual_iter.next().await?.expect(&format!(
            "final state mismatch: db ended early at index={} final_seq={} expected={:?}",
            row_index, final_seq, expected
        ));
        assert_eq!(
            actual, expected,
            "final state mismatch at index={} final_seq={}",
            row_index, final_seq
        );
    }

    let trailing_actual = actual_iter.next().await?;
    assert!(
        trailing_actual.is_none(),
        "final state mismatch: db has extra row after sqlite rows final_seq={} actual={:?}",
        final_seq,
        trailing_actual
    );

    Ok(final_seq)
}

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
    let mut expected_result: Option<(u64, u64, DateTime<Utc>)> = None;

    for simulation_count in 0..simulations {
        let object_store = Arc::new(InMemory::new());
        let rand = Rc::new(DbRand::new(seed));
        let system_clock = Arc::new(MockSystemClock::new());
        let mut simulation_scenarios: Vec<Box<dyn Scenario>> = Vec::with_capacity(11);
        for name in ["writer-0", "writer-1", "writer-2", "writer-3"] {
            simulation_scenarios.push(Box::new(scenarios::WriterScenario {
                name,
                rand: rand.clone(),
                iterations: Some(iterations),
            }));
        }
        for name in ["reader-0", "reader-1", "reader-2", "reader-3"] {
            simulation_scenarios.push(Box::new(scenarios::ReaderScenario {
                name,
                rand: rand.clone(),
                iterations: Some(iterations),
            }));
        }
        for name in ["flusher-0", "flusher-1"] {
            simulation_scenarios.push(Box::new(scenarios::FlusherScenario {
                name,
                rand: rand.clone(),
                iterations: Some(iterations),
            }));
        }
        simulation_scenarios.push(Box::new(scenarios::ClockScenario {
            name: "clock",
            rand: rand.clone(),
        }));

        let runtime = build_runtime(rand.rng().random::<u64>());
        let result = runtime.block_on(async {
            info!(seed, simulation_count, iterations, "running simulation");
            match run_simulation(object_store, system_clock, rand, simulation_scenarios, None).await
            {
                Ok(result) => {
                    info!(seed, simulation_count, iterations, "simulation passed");
                    Ok(result)
                }
                Err(err) => {
                    error!("simulation failed [seed={}, error={}]", seed, err);
                    Err(err)
                }
            }
        })?;

        if let Some((expected_final_seq, expected_next_u64, expected_next_time)) = &expected_result
        {
            let (final_seq, next_u64, next_time) = result;
            assert_eq!(
                next_u64, *expected_next_u64,
                "non-determinism detected [seed={}, simulation_count={}, next_u64={}, expected_u64={}]",
                seed, simulation_count, next_u64, expected_next_u64
            );
            assert_eq!(
                &next_time, expected_next_time,
                "non-determinism detected [seed={}, simulation_count={}, next_time={:?}, expected_time={:?}]",
                seed, simulation_count, next_time, expected_next_time
            );
            assert_eq!(
                final_seq, *expected_final_seq,
                "non-determinism detected [seed={}, simulation_count={}, final_seq={}, expected_seq={}]",
                seed, simulation_count, final_seq, expected_final_seq
            );
        } else {
            expected_result = Some(result);
        }
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
            let mut simulation_scenarios: Vec<Box<dyn Scenario>> = Vec::with_capacity(11);
            for name in ["writer-0", "writer-1", "writer-2", "writer-3"] {
                simulation_scenarios.push(Box::new(scenarios::WriterScenario {
                    name,
                    rand: rand.clone(),
                    iterations: None,
                }));
            }
            for name in ["reader-0", "reader-1", "reader-2", "reader-3"] {
                simulation_scenarios.push(Box::new(scenarios::ReaderScenario {
                    name,
                    rand: rand.clone(),
                    iterations: None,
                }));
            }
            for name in ["flusher-0", "flusher-1"] {
                simulation_scenarios.push(Box::new(scenarios::FlusherScenario {
                    name,
                    rand: rand.clone(),
                    iterations: None,
                }));
            }
            simulation_scenarios.push(Box::new(scenarios::ClockScenario {
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
