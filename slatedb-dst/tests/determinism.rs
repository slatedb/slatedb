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
use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use slatedb::config::{CompactorOptions, SizeTieredCompactionSchedulerOptions};
use slatedb::{Db, DbRand};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::{
    actors::{compactor, flusher, shutdown, workload, CompactorActorOptions, WorkloadActorOptions},
    utils::build_settings,
    DeterministicLocalFilesystem, FailingObjectStore, FailingObjectStoreController, Harness,
    Operation, StreamDirection, Toxic, ToxicKind,
};
use tempfile::TempDir;
use tracing::instrument;

#[test]
fn test_dst_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
    let simulations = 10;

    for seed in 101..=110 {
        let mut expected_u64: Option<u64> = None;
        let mut expected_time: Option<DateTime<Utc>> = None;
        for simulation_count in 0..simulations {
            let (next_u64, next_time) = run_seed_once(seed)?;

            if let Some(expected_u64) = expected_u64 {
                assert_eq!(
                    next_u64,
                    expected_u64,
                    "non-determinism detected [seed={}, simulation_count={}, next_u64={}, expected_u64={}]",
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
                    "non-determinism detected [seed={}, simulation_count={}, next_time={:?}, expected_time={:?}]",
                    seed,
                    simulation_count,
                    next_time,
                    expected_time,
                );
            }

            expected_u64 = Some(next_u64);
            expected_time = Some(next_time);
        }
    }

    Ok(())
}

/// Runs one seeded deterministic scenario and returns the next root RNG value
/// and current mock-clock time after the harness completes.
#[instrument(level = "debug", skip_all, fields(seed = seed))]
fn run_seed_once(seed: u64) -> Result<(u64, DateTime<Utc>), Box<dyn std::error::Error>> {
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let rand = Arc::new(DbRand::new(seed));
    let system_clock = Arc::new(MockSystemClock::new());
    let failure_seed = rand.rng().next_u64();
    let failures = FailingObjectStoreController::new(Arc::new(DbRand::new(failure_seed)));
    failures.add_toxic(Toxic {
        name: "put-latency".into(),
        kind: ToxicKind::Latency {
            latency: Duration::from_millis(1),
            jitter: Duration::from_millis(3),
        },
        direction: StreamDirection::Upstream,
        toxicity: 1.0,
        operations: vec![Operation::PutOpts],
        path_prefix: None,
    });
    let main_store: Arc<dyn ObjectStore> = Arc::new(FailingObjectStore::new(
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&main_dir)?),
        failures.clone(),
        system_clock.clone(),
    ));
    let wal_store: Arc<dyn ObjectStore> = Arc::new(FailingObjectStore::new(
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&wal_dir)?),
        failures,
        system_clock.clone(),
    ));
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
    Harness::new("determinism", seed, move |ctx| async move {
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
    .with_wal_object_store(wal_store)
    .actor_with_state("workload", 4, workload_options, workload)
    .actor_with_state("flusher", 1, 1_u64..=5_u64, flusher)
    .actor_with_state(
        "compactor",
        1,
        CompactorActorOptions {
            restart_interval: Duration::from_millis(25),
            compactor_options,
        },
        compactor,
    )
    .actor_with_state("shutdown", 1, 100, shutdown)
    .run()?;

    let next_u64 = rand.rng().next_u64();
    let next_time = system_clock.now();
    Ok((next_u64, next_time))
}
