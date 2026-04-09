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
use slatedb::config::{DurabilityLevel, ScanOptions, Settings};
use slatedb::{DbRand, Error, IterationOrder};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::utils::{build_runtime, build_scenario_db};
use slatedb_dst::{Dst, OracleSnapshot, Scenario};
#[cfg(slow)]
use tracing::info_span;
use tracing::{error, info};

#[cfg(slow)]
const NIGHTLY_WALL_CLOCK: Duration = Duration::from_secs(12 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimulationResult {
    snapshot: OracleSnapshot,
    next_u64: u64,
    next_time: DateTime<Utc>,
}

async fn verify_scan_orders(ctx: &slatedb_dst::ScenarioContext) -> Result<(), Error> {
    let full_range = vec![0x00]..vec![0xff];

    for order in [IterationOrder::Ascending, IterationOrder::Descending] {
        let _ = ctx
            .checked_scan::<Vec<u8>, _>(
                full_range.clone(),
                &ScanOptions::default().with_order(order),
            )
            .await?;
    }

    ctx.flush().await?;

    for order in [IterationOrder::Ascending, IterationOrder::Descending] {
        let _ = ctx
            .checked_scan::<Vec<u8>, _>(
                full_range.clone(),
                &ScanOptions::default()
                    .with_order(order)
                    .with_durability_filter(DurabilityLevel::Remote),
            )
            .await?;
    }

    Ok(())
}

async fn run_simulation(
    object_store: Arc<dyn ObjectStore>,
    system_clock: Arc<MockSystemClock>,
    rand: Rc<DbRand>,
    mut simulation_scenarios: Vec<Box<dyn Scenario>>,
    wall_clock_time: Option<Duration>,
) -> Result<SimulationResult, Error> {
    let settings = Settings {
        flush_interval: None,
        compactor_options: None,
        garbage_collector_options: None,
        // The bundled DST simulation avoids TTL writes until the oracle models
        // flush-time TTL tombstoning.
        default_ttl: None,
        ..Default::default()
    };
    let db_seed = rand.rng().random::<u64>();
    let db = build_scenario_db(
        object_store,
        system_clock.clone(),
        db_seed,
        settings.clone(),
    )
    .await?;
    let dst = Dst::new(db, system_clock.clone(), settings)?;

    if let Some(duration) = wall_clock_time {
        simulation_scenarios.push(Box::new(scenarios::TimedShutdownScenario {
            name: "wall-clock",
            duration,
        }));
    }

    dst.run_scenarios(simulation_scenarios).await?;

    let verifier = dst.context("verifier");
    verify_scan_orders(&verifier).await?;

    let snapshot = dst.oracle_snapshot()?;
    let next_u64 = rand.rng().random::<u64>();
    let next_time = system_clock.now();

    dst.close().await?;

    Ok(SimulationResult {
        snapshot,
        next_u64,
        next_time,
    })
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
    let mut expected_result: Option<SimulationResult> = None;

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

        if let Some(expected_result) = &expected_result {
            assert_eq!(
                result.next_u64, expected_result.next_u64,
                "non-determinism detected [seed={}, simulation_count={}, next_u64={}, expected_u64={}]",
                seed, simulation_count, result.next_u64, expected_result.next_u64
            );
            assert_eq!(
                result.next_time, expected_result.next_time,
                "non-determinism detected [seed={}, simulation_count={}, next_time={:?}, expected_time={:?}]",
                seed, simulation_count, result.next_time, expected_result.next_time
            );
            assert_eq!(
                result.snapshot, expected_result.snapshot,
                "non-determinism detected [seed={}, simulation_count={}]",
                seed, simulation_count
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
