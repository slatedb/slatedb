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
//! Variants are configured by the fields of [`DeterministicScenario`] —
//! e.g. enabling RFC-0024 segmentation by supplying a
//! [`slatedb::PrefixExtractor`], in which case each workload actor's
//! `{name}/N` keys route to a distinct segment.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use log::{error, info};
use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use slatedb::config::DurabilityLevel;
use slatedb::{Db, DbRand, PrefixExtractor};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use tempfile::TempDir;
use tracing::instrument;

use crate::actors::{
    CompactorActor, CompactorActorOptions, FlusherActor, ShutdownActor, WorkloadActor,
    WorkloadActorOptions, WorkloadMergeOperator,
};
use crate::utils::{build_settings, build_settings_compactor, build_toxic, dst_seeds};
use crate::{DeterministicLocalFilesystem, Harness};

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
pub struct DeterministicScenario {
    /// Logical name used for the harness label and tempdir path prefix.
    pub name: &'static str,
    /// Number of times to replay each seed in serial. The first replay
    /// establishes the expected post-run state; later replays must match.
    pub simulations: u32,
    /// Mock-clock deadline (ms since the unix epoch) at which the
    /// shutdown actor cancels the simulation.
    pub shutdown_at_ms: i64,
    /// Optional segment extractor. When `Some`, the scenario configures
    /// the harness and `DbBuilder` for RFC-0024 segmentation; when
    /// `None`, the scenario runs the unsegmented configuration.
    pub segment_extractor: Option<Arc<dyn PrefixExtractor>>,
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
        let DeterministicScenario {
            name,
            simulations,
            shutdown_at_ms,
            segment_extractor,
        } = self;
        let num_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        let seeds = dst_seeds(num_cores)?;

        let handles = seeds
            .into_iter()
            .enumerate()
            .map(|(core, seed)| {
                info!("dst {name} seed [core={core}, seed={seed}]");
                let extractor = segment_extractor.clone();
                (
                    core,
                    seed,
                    std::thread::spawn(move || {
                        run_seed_is_deterministic(
                            name,
                            core,
                            seed,
                            simulations,
                            shutdown_at_ms,
                            extractor,
                        )
                    }),
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
    name: &'static str,
    core: usize,
    seed: u64,
    simulations: u32,
    shutdown_at_ms: i64,
    segment_extractor: Option<Arc<dyn PrefixExtractor>>,
) -> ScenarioResult<()> {
    let mut expected_u64: Option<u64> = None;
    let mut expected_time: Option<DateTime<Utc>> = None;

    for simulation_count in 0..simulations {
        let (next_u64, next_time) =
            run_seed_once(name, seed, shutdown_at_ms, segment_extractor.clone())?;

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
#[instrument(level = "debug", skip_all, fields(scenario = name, seed = seed))]
fn run_seed_once(
    name: &'static str,
    seed: u64,
    shutdown_at_ms: i64,
    segment_extractor: Option<Arc<dyn PrefixExtractor>>,
) -> ScenarioResult<(u64, DateTime<Utc>)> {
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
    let workload_options = WorkloadActorOptions {
        read_durability: DurabilityLevel::Remote,
        ..WorkloadActorOptions::default()
    };
    let compactor_options = build_settings_compactor(&mut *rand.rng());
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

    if let Some(extractor) = segment_extractor {
        harness = harness.with_segment_extractor(extractor);
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
        .actor("shutdown", ShutdownActor::new(shutdown_at_ms)?);

    harness.run()?;

    let next_u64 = rand.rng().next_u64();
    let next_time = system_clock.now();
    Ok((next_u64, next_time))
}
