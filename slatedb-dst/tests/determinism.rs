//! Verifies deterministic scenario-test behavior for SlateDB under DST
//! configuration.
//!
//! Each simulation run:
//! - starts from a fresh harness root [`DbRand`] initialized with the same seed
//! - starts from a fresh shared [`MockSystemClock`]
//! - opens a real [`Db`] using randomized deterministic settings from
//!   [`build_settings`]
//! - exercises 500 randomized actor steps covering writes, deletes, flushes,
//!   explicit clock advancement, and DB reopen paths against deterministic
//!   local filesystem-backed object stores
//! - compares the next random `u64` and current clock time after the run
//!
//! If either the harness or SlateDB consumes randomness differently, advances
//! the clock differently, or executes different deterministic branches for the
//! same seed, one of those post-run checks will diverge.

#![cfg(dst)]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use fail_parallel::FailPointRegistry;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use slatedb::config::{FlushOptions, FlushType, PutOptions, WriteOptions};
use slatedb::{Db, DbRand, Error, Settings};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::{
    utils::build_settings, ActorCtx, DeterministicLocalFilesystem, Harness, Operation,
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
    let cache_dir = tempdir.path().join("cache");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;
    std::fs::create_dir_all(&cache_dir)?;

    let rand = Arc::new(DbRand::new(seed));
    let system_clock = Arc::new(MockSystemClock::new());
    let main_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&main_dir)?);
    let wal_store: Arc<dyn ObjectStore> =
        Arc::new(DeterministicLocalFilesystem::new_with_prefix(&wal_dir)?);
    let settings = Arc::new(OnceLock::new());
    let startup_settings = Arc::clone(&settings);

    Harness::new("determinism", seed)
        .with_rand(Arc::clone(&rand))
        .with_system_clock(Arc::clone(&system_clock))
        .with_path(Path::from("determinism"))
        .with_main_object_store(main_store)
        .with_wal_object_store(wal_store)
        .with_db(move |ctx| {
            let settings_slot = Arc::clone(&startup_settings);
            let cache_dir = cache_dir.clone();
            async move {
                let db_seed = ctx.rand().rng().next_u64();
                let settings = build_settings(ctx.rand(), &cache_dir).await;
                assert!(
                    settings_slot.set(settings.clone()).is_ok(),
                    "settings should only be initialized once",
                );

                open_db(
                    ctx.path().clone(),
                    ctx.main_object_store(),
                    ctx.wal_object_store().expect("configured"),
                    ctx.system_clock(),
                    ctx.fp_registry(),
                    settings,
                    db_seed,
                )
                .await
            }
        })
        .actor_with_state("writer", 1, settings, |ctx, settings| async move {
            run_actor(ctx, settings).await
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
/// - a weighted mix of puts, deletes, memtable flushes, and advance-only steps
///   selected from the actor RNG
/// - explicit mock-clock advancement on most, but not all, steps
/// - two reopen passes that read back the persisted state using the exact
///   startup settings, which exercises DB reopen logic and any randomized
///   object-store cache configuration chosen by `build_settings`
async fn run_actor(ctx: ActorCtx, settings: Arc<OnceLock<Settings>>) -> Result<(), Error> {
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
    let mut expected_values = vec![None::<Vec<u8>>; KEY_COUNT];
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
                expected_values[key_index] = Some(value);
            }
            50..=69 => {
                db.delete_with_options(key.as_bytes(), &write_options)
                    .await?;
                expected_values[key_index] = None;
            }
            70..=74 => {
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await?;
            }
            _ => {}
        }

        if rand_value % 8 != 0 {
            ctx.advance_time(Duration::from_millis(1 + (rand_value % 5)))
                .await;
        }
    }

    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    db.close().await?;

    let settings = settings
        .get()
        .cloned()
        .expect("startup settings should be recorded");

    let reopen_seed = ctx.rand().rng().next_u64();
    let reopened = open_db_from_actor(&ctx, settings.clone(), reopen_seed).await?;
    let _closed = ctx.swap_db(Arc::clone(&reopened));
    assert_expected_values(&reopened, &expected_values).await?;
    reopened.close().await?;

    let reopen_again_seed = ctx.rand().rng().next_u64();
    let reopened_again = open_db_from_actor(&ctx, settings, reopen_again_seed).await?;
    let _closed = ctx.swap_db(Arc::clone(&reopened_again));
    assert_expected_values(&reopened_again, &expected_values).await?;
    reopened_again.close().await?;

    Ok(())
}

/// Reopens the database from actor context using the same harness-managed
/// stores, clock, failpoint registry, and settings.
async fn open_db_from_actor(
    ctx: &ActorCtx,
    settings: Settings,
    seed: u64,
) -> Result<Arc<Db>, Error> {
    open_db(
        ctx.path().clone(),
        ctx.main_object_store(),
        ctx.wal_object_store().expect("configured"),
        ctx.system_clock(),
        ctx.fp_registry(),
        settings,
        seed,
    )
    .await
}

/// Opens a [`Db`] with the deterministic harness infrastructure and supplied
/// settings and seed.
async fn open_db(
    path: Path,
    main_object_store: Arc<dyn ObjectStore>,
    wal_object_store: Arc<dyn ObjectStore>,
    system_clock: Arc<dyn SystemClock>,
    fp_registry: Arc<FailPointRegistry>,
    settings: Settings,
    seed: u64,
) -> Result<Arc<Db>, Error> {
    let db = Db::builder(path, main_object_store)
        .with_wal_object_store(wal_object_store)
        .with_system_clock(system_clock)
        .with_fp_registry(fp_registry)
        .with_seed(seed)
        .with_settings(settings)
        .build()
        .await?;
    Ok(Arc::new(db))
}

/// Verifies that the reopened database returns the values produced by the
/// actor's write and delete sequence.
async fn assert_expected_values(db: &Db, expected_values: &[Option<Vec<u8>>]) -> Result<(), Error> {
    for (key_index, expected_value) in expected_values.iter().enumerate() {
        let key = format!("key-{key_index}");
        let actual_value = db.get(key.as_bytes()).await?.map(|bytes| bytes.to_vec());
        assert_eq!(
            actual_value,
            expected_value.clone(),
            "unexpected persisted value for {key}",
        );
    }

    Ok(())
}
