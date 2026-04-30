//! Verifies deterministic scenario-test behavior for SlateDB under DST
//! configuration.
//!
//! Each simulation run:
//! - starts from a fresh harness root [`DbRand`] initialized with the same seed
//! - starts from a fresh shared [`MockSystemClock`]
//! - opens a real [`Db`] using randomized deterministic settings from
//!   [`build_settings`]
//! - runs looping workload, flusher, and compactor actors against
//!   deterministic local filesystem-backed object stores until a shutdown actor
//!   cancels the shared token at a fixed mock-clock deadline
//! - compares the next random `u64` and current clock time after the run
//!
//! If either the harness or SlateDB consumes randomness differently, advances
//! the clock differently, or executes different deterministic branches for the
//! same seed, one of those post-run checks will diverge.
#![cfg(dst)]

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use log::{error, info};
use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use rstest::rstest;
use slatedb::config::{CompactorOptions, SizeTieredCompactionSchedulerOptions};
use slatedb::{Db, DbRand};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::{
    actors::{
        CompactorActor, CompactorActorOptions, FlusherActor, ShutdownActor, WorkloadActor,
        WorkloadActorOptions,
    },
    utils::{build_settings, build_toxic},
    DeterministicLocalFilesystem, Harness,
};
use tempfile::TempDir;
use tracing::instrument;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T> = Result<T, TestError>;

/// Verifies that the DST harness produces repeatable outcomes for independently
/// chosen seeds.
///
/// The test creates one random seed per available CPU core, then runs those
/// seed groups concurrently on separate OS threads. Each thread owns one seed
/// and executes that seed repeatedly in serial, so the determinism assertion is
/// still made by comparing multiple runs of the same seed against one another.
/// Printing the seed and core gives a direct reproduction handle when a worker
/// fails or panics.
#[rstest]
#[cfg_attr(not(slow), case::regular(4, 200))]
#[cfg_attr(slow, case::slow(2, 1_000))]
fn test_dst_is_deterministic(
    #[case] simulations: u32,
    #[case] shutdown_at_ms: i64,
) -> TestResult<()> {
    let num_cores = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let seeds: Vec<u64> = (0..num_cores).map(|_| rand::random::<u64>()).collect();

    let handles = seeds
        .into_iter()
        .enumerate()
        .map(|(core, seed)| {
            info!("dst determinism seed [core={core}, seed={seed}]");
            (
                core,
                seed,
                std::thread::spawn(move || {
                    run_seed_is_deterministic(core, seed, simulations, shutdown_at_ms)
                }),
            )
        })
        .collect::<Vec<_>>();

    for (core, seed, handle) in handles {
        match handle.join() {
            Ok(result) => result?,
            Err(payload) => {
                error!("dst determinism panicked [core={core}, seed={seed}]");
                std::panic::resume_unwind(payload);
            }
        }
    }

    Ok(())
}

/// Re-runs a single seed and checks that every simulation leaves deterministic
/// state in the same post-run position.
///
/// The first simulation establishes the expected next root RNG value and
/// mock-clock time. Later simulations with the same seed must produce the same
/// values. The `core` argument is diagnostic context only; it identifies which
/// worker thread owned the seed when reporting assertion failures.
fn run_seed_is_deterministic(
    core: usize,
    seed: u64,
    simulations: u32,
    shutdown_at_ms: i64,
) -> TestResult<()> {
    let mut expected_u64: Option<u64> = None;
    let mut expected_time: Option<DateTime<Utc>> = None;

    for simulation_count in 0..simulations {
        let (next_u64, next_time) = run_seed_once(seed, shutdown_at_ms)?;

        if let Some(expected_u64) = expected_u64 {
            assert_eq!(
                next_u64,
                expected_u64,
                "non-determinism detected [core={}, seed={}, simulation_count={}, next_u64={}, expected_u64={}]",
                core,
                seed,
                simulation_count,
                next_u64,
                expected_u64,
            );
        }

        if let Some(expected_time) = expected_time {
            assert_eq!(
                next_time,
                expected_time,
                "non-determinism detected [core={}, seed={}, simulation_count={}, next_time={:?}, expected_time={:?}]",
                core,
                seed,
                simulation_count,
                next_time,
                expected_time,
            );
        }

        expected_u64 = Some(next_u64);
        expected_time = Some(next_time);
    }

    Ok(())
}

/// Runs one complete seeded scenario and returns the observable deterministic
/// state after the harness shuts down.
///
/// Each invocation builds a fresh temporary main and WAL object-store root,
/// fresh root RNG, and fresh mock clock. The harness then owns the
/// fault-injection controller, opens a real `Db`, starts workload, flusher,
/// compactor, and shutdown actors, and runs until the shutdown actor cancels
/// the simulation. Returning the next root RNG value and current mock-clock
/// time gives callers a compact fingerprint of the deterministic execution
/// path.
#[instrument(level = "debug", skip_all, fields(seed = seed))]
fn run_seed_once(seed: u64, shutdown_at_ms: i64) -> TestResult<(u64, DateTime<Utc>)> {
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let rand = Arc::new(DbRand::new(seed));
    let system_clock = Arc::new(MockSystemClock::new());
    let main_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&main_dir)?);
    let wal_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&wal_dir)?);
    let workload_options = WorkloadActorOptions::default();
    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(10),
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            max_compaction_sources: 999,
            include_size_threshold: 4.0,
        }
        .into(),
        ..CompactorOptions::default()
    };
    let harness = Harness::new("determinism", seed, move |ctx| async move {
        let failures = ctx.failure_controller();
        for index in 0..10 {
            failures.add_toxic(build_toxic(ctx.rand(), ctx.path().as_ref(), index));
        }

        let db_seed = ctx.rand().rng().next_u64();
        let mut settings = build_settings(ctx.rand()).await;

        // Keep L0 tiny and compactor polling aggressive so a small number of
        // explicit memtable flushes will trigger real compaction during the run.
        settings.l0_sst_size_bytes = 1024;
        settings.l0_max_ssts = 4;
        settings.manifest_poll_interval = Duration::from_millis(10);
        // Disable since we're using the standalone compactor actor.
        settings.compactor_options = None;

        let db = Db::builder(ctx.path().clone(), ctx.main_object_store())
            .with_wal_object_store(ctx.wal_object_store().expect("configured"))
            .with_system_clock(ctx.system_clock())
            .with_fp_registry(ctx.fp_registry())
            .with_seed(db_seed)
            .with_settings(settings)
            .build()
            .await?;
        Ok(Arc::new(db))
    })
    .with_rand(Arc::clone(&rand))
    .with_system_clock(Arc::clone(&system_clock))
    .with_path(Path::from("determinism"))
    .with_main_object_store(main_store)
    .with_wal_object_store(wal_store);

    let harness = harness
        .actor("workload-1", WorkloadActor::new(workload_options.clone())?)
        .actor("workload-2", WorkloadActor::new(workload_options.clone())?)
        .actor("workload-3", WorkloadActor::new(workload_options.clone())?)
        .actor("workload-4", WorkloadActor::new(workload_options)?)
        .actor("flusher", FlusherActor::new(1_u64..=5_u64)?)
        .actor(
            "compactor",
            CompactorActor::new(CompactorActorOptions {
                restart_interval: Duration::from_millis(25),
                compactor_options,
            })?,
        )
        .actor("shutdown", ShutdownActor::new(shutdown_at_ms)?);

    harness.run()?;

    let next_u64 = rand.rng().next_u64();
    let next_time = system_clock.now();
    Ok((next_u64, next_time))
}
