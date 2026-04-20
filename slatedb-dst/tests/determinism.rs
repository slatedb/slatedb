//! Verifies deterministic scenario-test behavior for SlateDB under DST
//! configuration.
//!
//! Each simulation run:
//! - starts from a fresh harness root [`DbRand`] initialized with the same seed
//! - starts from a fresh shared [`MockSystemClock`]
//! - opens a real [`Db`] using randomized deterministic settings from
//!   [`build_settings`]
//! - runs a background clock actor alongside 500 randomized writer-actor steps
//!   covering writes, deletes, flushes, and idle steps against deterministic
//!   local filesystem-backed object stores
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
use slatedb::config::{
    CompactorOptions, FlushOptions, FlushType, PutOptions, SizeTieredCompactionSchedulerOptions,
    WriteOptions,
};
use slatedb::{Db, DbRand, Error};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::{
    utils::build_settings, ActorCtx, ActorType, DeterministicLocalFilesystem, Harness, Operation,
    StreamDirection, Toxic, ToxicKind,
};
use tempfile::TempDir;

const ACTOR_STEPS: u64 = 500;
const KEY_COUNT: usize = 32;

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
fn run_seed_once(seed: u64) -> Result<(u64, DateTime<Utc>), Box<dyn std::error::Error>> {
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
    Harness::new("determinism", seed)
        .with_rand(Arc::clone(&rand))
        .with_system_clock(Arc::clone(&system_clock))
        .with_path(Path::from("determinism"))
        .with_main_object_store(main_store)
        .with_wal_object_store(wal_store)
        .with_db(move |ctx| async move {
            let db_seed = ctx.rand().rng().next_u64();
            let mut settings = build_settings(ctx.rand()).await;

            // Keep L0 tiny and compactor polling aggressive so a small number of
            // explicit memtable flushes will trigger real compaction during the run.
            settings.l0_sst_size_bytes = 256;
            settings.l0_max_ssts = 2;
            settings.manifest_poll_interval = Duration::from_millis(10);

            let compactor_options = settings
                .compactor_options
                .get_or_insert_with(CompactorOptions::default);
            compactor_options.poll_interval = Duration::from_millis(100);
            compactor_options.max_concurrent_compactions = 1;
            compactor_options.scheduler_options = SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: 2,
                max_compaction_sources: 999,
                include_size_threshold: 4.0,
            }
            .into();

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
        .actor("writer", ActorType::Foreground, 1, |ctx| async move {
            run_actor(ctx).await
        })
        .actor("clock", ActorType::Background, 1, |ctx| async move {
            run_clock(ctx).await
        })
        .run()?;

    let next_u64 = rand.rng().next_u64();
    let next_time = system_clock.now();
    Ok((next_u64, next_time))
}

/// Executes the single-actor deterministic workload used by this test.
///
/// The workload is intentionally simple structurally, but large enough to
/// exercise many seeded state transitions:
/// - object-store fault injection via a deterministic latency toxic on writes
/// - actor-local RNG consumption on every iteration
/// - 500 randomly chosen operations across a fixed key set
/// - a weighted mix of puts, deletes, memtable flushes, and idle steps selected
///   from the actor RNG
async fn run_actor(ctx: ActorCtx) -> Result<(), Error> {
    ctx.failures().add_toxic(Toxic {
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

    let db = ctx.db();
    let put_options = PutOptions::default();
    let mut write_options = WriteOptions::default();
    write_options.await_durable = false;

    for step in 0..ACTOR_STEPS {
        let rand_value = ctx.rand().rng().next_u64();
        let key_index = ((rand_value >> 8) as usize) % KEY_COUNT;
        let key = format!("key-{key_index}");

        match rand_value % 1_000 {
            0..=49 => {
                let value = format!("{step:04}-{rand_value:016x}").into_bytes();
                db.put_with_options(key.as_bytes(), &value, &put_options, &write_options)
                    .await?;
            }
            50..=69 => {
                db.delete_with_options(key.as_bytes(), &write_options)
                    .await?;
            }
            70..=74 => {
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await?;
            }
            _ => {}
        }
    }

    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await
}

async fn run_clock(ctx: ActorCtx) -> Result<(), Error> {
    loop {
        let rand_value = ctx.rand().rng().next_u64();
        ctx.advance_time(Duration::from_millis(1 + (rand_value % 5)))
            .await;
    }
}
