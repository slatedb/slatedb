//! Shared structures and execution logic for running DST scenarios.
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
//!
//! Each run also turns these features on or off at random with the seeded RNG.
//! The seeds then cover all feature combinations, without one scenario for
//! each combination:
//! - RFC-0024 segmentation with [`FirstDelimiterPrefixExtractor`], so each
//!   workload actor's `{name}/N` keys route to a distinct segment
//! - incremental compaction, with the settings of
//!   [`enable_incremental_compaction`] that make incremental commits likely
//! - RFC-0034 local mirrors in front of the main object store

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use log::{error, info};
use object_store::path::Path;
use object_store::ObjectStore;
use rand::{Rng, RngExt};
use slatedb::config::DurabilityLevel;
use slatedb::{Db, DbRand};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use tempfile::TempDir;
use tracing::instrument;

use crate::actors::{
    CompactorActor, CompactorActorOptions, FlusherActor, ShutdownActor, WorkloadActor,
    WorkloadActorOptions, WorkloadMergeOperator,
};
use crate::utils::{
    build_mirror_options, build_settings, build_settings_compactor, build_toxic, dst_seeds,
    enable_incremental_compaction,
};
use crate::{DeterministicLocalFilesystem, FirstDelimiterPrefixExtractor, Harness};

type ScenarioError = Box<dyn std::error::Error + Send + Sync>;
type ScenarioResult<T> = Result<T, ScenarioError>;

/// Configuration for a deterministic SlateDB scenario test.
///
/// The scenario fans out one random seed per available CPU core (or the
/// seeds listed in [`crate::utils::DST_SEEDS_ENV`]), runs those seed
/// groups on separate OS threads, and within each thread replays the
/// seeded scenario `simulations` times in serial. Every replay must
/// leave the harness in the same observable state (next root RNG value +
/// current mock-clock time); a divergence indicates a non-deterministic
/// code path in the harness or SlateDB.
#[derive(Clone)]
pub struct DeterministicScenario {
    /// Logical name used for the harness label and tempdir path prefix.
    pub name: &'static str,
    /// Number of times to replay each seed in serial. The first replay
    /// establishes the expected post-run state; later replays must match.
    pub simulations: u32,
    /// Mock-clock deadline (ms since the unix epoch) at which the
    /// shutdown actor cancels the simulation.
    pub shutdown_at_ms: i64,
}

impl DeterministicScenario {
    /// Runs the scenario across all available CPU cores in parallel.
    ///
    /// Creates one random seed per available CPU core (or takes the seeds
    /// listed in [`crate::utils::DST_SEEDS_ENV`]), then runs those seed
    /// groups concurrently on separate OS threads. Each thread owns one seed
    /// and executes that seed repeatedly in serial, so the determinism
    /// assertion is still made by comparing multiple runs of the same seed
    /// against one another. The seed and core index are logged so a failing
    /// or panicking worker gives a direct reproduction handle.
    pub fn run(self) -> ScenarioResult<()> {
        let name = self.name;
        let num_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        let seeds = dst_seeds(num_cores)?;

        let handles = seeds
            .into_iter()
            .enumerate()
            .map(|(core, seed)| {
                info!("dst {name} seed [core={core}, seed={seed}]");
                let scenario = self.clone();
                (
                    core,
                    seed,
                    std::thread::spawn(move || run_seed_is_deterministic(&scenario, core, seed)),
                )
            })
            .collect::<Vec<_>>();

        for (core, seed, handle) in handles {
            match handle.join() {
                Ok(result) => result?,
                Err(payload) => {
                    error!("dst {name} panicked [core={core}, seed={seed}]");
                    std::panic::resume_unwind(payload);
                }
            }
        }

        Ok(())
    }
}

/// Re-runs a single seed and checks that every simulation leaves the
/// harness in the same post-run position.
///
/// The first simulation establishes the expected next root RNG value and
/// mock-clock time. Later simulations with the same seed must produce the
/// same values. `core` is diagnostic context only; it identifies which
/// worker thread owned the seed when reporting assertion failures.
fn run_seed_is_deterministic(
    scenario: &DeterministicScenario,
    core: usize,
    seed: u64,
) -> ScenarioResult<()> {
    let name = scenario.name;
    let mut expected_u64: Option<u64> = None;
    let mut expected_time: Option<DateTime<Utc>> = None;

    for simulation_count in 0..scenario.simulations {
        let (next_u64, next_time) = run_seed_once(scenario, seed)?;

        if let Some(expected_u64) = expected_u64 {
            assert_eq!(
                next_u64, expected_u64,
                "non-determinism detected [scenario={name}, core={core}, seed={seed}, simulation_count={simulation_count}, next_u64={next_u64}, expected_u64={expected_u64}]",
            );
        }

        if let Some(expected_time) = expected_time {
            assert_eq!(
                next_time, expected_time,
                "non-determinism detected [scenario={name}, core={core}, seed={seed}, simulation_count={simulation_count}, next_time={next_time:?}, expected_time={expected_time:?}]",
            );
        }

        expected_u64 = Some(next_u64);
        expected_time = Some(next_time);
    }

    Ok(())
}

/// Runs one complete seeded scenario and returns the observable
/// deterministic state after the harness shuts down.
///
/// Each invocation builds a fresh temporary main and WAL object-store root,
/// fresh root RNG, and fresh mock clock. The harness then owns the
/// fault-injection controller, opens a real `Db`, starts workload, flusher,
/// compactor, and shutdown actors, and runs until the shutdown actor cancels
/// the simulation. Returning the next root RNG value and current mock-clock
/// time gives callers a compact fingerprint of the deterministic execution
/// path.
#[instrument(level = "debug", skip_all, fields(scenario = scenario.name, seed = seed))]
fn run_seed_once(
    scenario: &DeterministicScenario,
    seed: u64,
) -> ScenarioResult<(u64, DateTime<Utc>)> {
    let name = scenario.name;
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let rand = Arc::new(DbRand::new(seed));
    let (segments, incremental_compaction, mirror) = {
        let mut rng = rand.rng();
        (
            rng.random_bool(0.5),
            rng.random_bool(0.5),
            rng.random_bool(0.5),
        )
    };
    info!("dst {name} features [seed={seed}, segments={segments}, incremental_compaction={incremental_compaction}, mirror={mirror}]");
    let system_clock = Arc::new(MockSystemClock::new());
    let main_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&main_dir)?);
    let wal_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&wal_dir)?);
    let workload_options = WorkloadActorOptions {
        read_durability: DurabilityLevel::Remote,
        ..WorkloadActorOptions::default()
    };
    let mut compactor_options = build_settings_compactor(&mut *rand.rng());
    if incremental_compaction {
        enable_incremental_compaction(&mut compactor_options, &mut *rand.rng());
    } else {
        compactor_options.enable_experimental_incremental_compaction = false;
    }
    let mirror_options = mirror.then(|| build_mirror_options(&mut *rand.rng()));
    let mut harness = Harness::new(name, seed, move |ctx| async move {
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
        settings.max_unflushed_bytes = 64 * 1024;
        settings.manifest_poll_interval = Duration::from_millis(10);
        // Disable since we're using the standalone compactor actor.
        settings.compactor_options = None;

        let mut builder = Db::builder(ctx.path().clone(), ctx.main_object_store())
            .with_wal_object_store(ctx.wal_object_store().expect("configured"))
            .with_system_clock(ctx.system_clock())
            .with_fp_registry(ctx.fp_registry())
            .with_seed(db_seed)
            .with_settings(settings);
        if let Some(merge_operator) = ctx.merge_operator() {
            builder = builder.with_merge_operator(merge_operator);
        }
        if let Some(extractor) = ctx.segment_extractor() {
            builder = builder.with_segment_extractor(extractor);
        }
        let db = builder.build().await?;
        Ok(Arc::new(db))
    })
    .with_rand(Arc::clone(&rand))
    .with_system_clock(Arc::clone(&system_clock))
    .with_path(Path::from(name))
    .with_main_object_store(main_store)
    .with_wal_object_store(wal_store)
    .with_merge_operator(Arc::new(WorkloadMergeOperator));

    if segments {
        harness = harness.with_segment_extractor(Arc::new(FirstDelimiterPrefixExtractor));
    }
    if let Some(options) = mirror_options {
        harness = harness.with_mirror_fn(move |name, store, clock| {
            let options = options.clone();
            async move { options.build(name, store, clock).await }
        });
    }

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
        .actor("shutdown", ShutdownActor::new(scenario.shutdown_at_ms)?);

    harness.run()?;

    let next_u64 = rand.rng().next_u64();
    let next_time = system_clock.now();
    Ok((next_u64, next_time))
}
