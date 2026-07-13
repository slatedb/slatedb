//! Data-preservation checks for RFC-0004 split and merge scenarios.
//!
//! [`RescalingScenario`] keeps [`Harness`] focused on one physical database at
//! a time. Rescaling happens between harness runs: the scenario stops the
//! source database, uses SlateDB's administrative clone operations to project
//! or union manifests, and starts new harnesses for the resulting databases.
//!
//! Each run performs these phases:
//! - run four prefix-scoped workload actors against a root database
//! - scan the quiesced root and project it at `workload-3/` into adjacent left
//!   and right databases
//! - verify that both projections exactly match their ranges in the root
//! - run the left and right databases concurrently, assigning actors 1-2 to
//!   the left database and actors 3-4 to the right
//! - scan the quiesced children, union them into a merged database, and verify
//!   that the merged rows exactly equal both child snapshots
//! - run all four workload actors against the merged database to verify that it
//!   remains usable after the union
//!
//! Every workload operation remains inside one actor prefix, so point
//! operations, write batches, and scans stay within one child. The child
//! harnesses have independent seeded Tokio runtimes, [`DbRand`] instances,
//! [`MockSystemClock`] instances, and fault controllers. They share the same
//! underlying [`DeterministicLocalFilesystem`] object stores, which exercises
//! concurrent object-store access without making either runtime's scheduling
//! order part of the other runtime's state.
//!
//! Garbage collection remains enabled, but detach GC is disabled because both
//! projected children reference the root checkpoint. WALs are disabled because
//! manifest union does not combine live WAL state. Projection and union run
//! only after their source harnesses have stopped.
//!
//! The exact snapshot comparisons at the split and merge barriers are the
//! primary assertions: every root row must appear in exactly one child, and
//! every child row must appear in the merged database.

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use log::{error, info};
use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use slatedb::admin::{AdminBuilder, CloneSourceSpec};
use slatedb::config::DurabilityLevel;
use slatedb::{Db, DbRand, DbReader};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use tempfile::TempDir;
use tokio::runtime::RngSeed;
use tracing::instrument;

use crate::actors::{
    FlusherActor, ShutdownActor, WorkloadActor, WorkloadActorOptions, WorkloadMergeOperator,
};
use crate::utils::{build_settings, build_toxic, dst_seeds};
use crate::{DeterministicLocalFilesystem, Harness};

type ScenarioError = Box<dyn std::error::Error + Send + Sync>;
type ScenarioResult<T> = Result<T, ScenarioError>;
type ProjectionRange = (Bound<Bytes>, Bound<Bytes>);
type Rows = Vec<(Bytes, Bytes)>;

const ROOT_ACTORS: &[&str] = &["workload-1", "workload-2", "workload-3", "workload-4"];
const LEFT_ACTORS: &[&str] = &["workload-1", "workload-2"];
const RIGHT_ACTORS: &[&str] = &["workload-3", "workload-4"];
const SPLIT_KEY: &[u8] = b"workload-3/";

/// Configuration for the RFC-0004 split/merge data-preservation scenario.
///
/// A run exercises one root database, projects it into two children, runs the
/// children concurrently, unions their quiesced states, and then runs the
/// merged database. Exact snapshots verify that projection and union preserve
/// every row.
pub struct RescalingScenario {
    /// Logical name used for harness labels and database paths.
    pub name: &'static str,
    /// Per-harness mock-clock deadline, in milliseconds since the Unix epoch.
    pub shutdown_at_ms: i64,
}

impl RescalingScenario {
    /// Runs the rescaling scenario across the configured DST seed budget.
    ///
    /// Each seed worker runs its root and merged phases locally and spawns two
    /// child threads for the disjoint projected databases. The default number
    /// of seed workers is half the available cores so the parallel child phase
    /// does not systematically oversubscribe the machine.
    pub fn run(self) -> ScenarioResult<()> {
        let Self {
            name,
            shutdown_at_ms,
        } = self;
        let num_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        let seeds = dst_seeds((num_cores / 2).max(1))?;

        let handles = seeds
            .into_iter()
            .enumerate()
            .map(|(core, seed)| {
                info!("dst {name} seed [core={core}, seed={seed}]");
                (
                    core,
                    seed,
                    std::thread::spawn(move || run_seed(name, seed, shutdown_at_ms)),
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

#[derive(Clone)]
struct ScenarioStores {
    main: Arc<dyn ObjectStore>,
    wal: Arc<dyn ObjectStore>,
}

#[instrument(level = "debug", skip_all, fields(scenario = name, seed = seed))]
fn run_seed(name: &'static str, seed: u64, shutdown_at_ms: i64) -> ScenarioResult<()> {
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let stores = ScenarioStores {
        main: Arc::new(DeterministicLocalFilesystem::new_with_prefix(&main_dir)?),
        wal: Arc::new(DeterministicLocalFilesystem::new_with_prefix(&wal_dir)?),
    };
    let plan_rand = DbRand::new(seed);
    let next_seed = || plan_rand.rng().next_u64();
    let root_path = Path::from(format!("{name}/root"));
    let left_path = Path::from(format!("{name}/split/left"));
    let right_path = Path::from(format!("{name}/split/right"));
    let merged_path = Path::from(format!("{name}/merged"));

    run_harness_phase(
        format!("{name}-root"),
        root_path.clone(),
        stores.clone(),
        next_seed(),
        shutdown_at_ms,
        ROOT_ACTORS,
    )?;
    let root_rows = snapshot_rows(root_path.clone(), stores.clone(), next_seed())?;

    let split_key = Bytes::from_static(SPLIT_KEY);
    let left_range = (Bound::Unbounded, Bound::Excluded(split_key.clone()));
    let right_range = (Bound::Included(split_key), Bound::Unbounded);
    create_clone(
        left_path.clone(),
        vec![CloneSourceSpec::new(root_path.clone()).with_projection_range(left_range.clone())],
        stores.clone(),
        next_seed(),
    )?;
    create_clone(
        right_path.clone(),
        vec![CloneSourceSpec::new(root_path).with_projection_range(right_range.clone())],
        stores.clone(),
        next_seed(),
    )?;

    let left_after_split = snapshot_rows(left_path.clone(), stores.clone(), next_seed())?;
    let right_after_split = snapshot_rows(right_path.clone(), stores.clone(), next_seed())?;
    let (expected_left, expected_right): (Rows, Rows) = root_rows
        .into_iter()
        .partition(|(key, _)| key.as_ref() < SPLIT_KEY);
    assert_eq!(left_after_split, expected_left);
    assert_eq!(right_after_split, expected_right);

    // Allocate both seeds before either thread starts so scheduling cannot
    // change which random stream is assigned to a child.
    let left_harness_seed = next_seed();
    let right_harness_seed = next_seed();
    let left_handle = {
        let stores = stores.clone();
        let path = left_path.clone();
        std::thread::spawn(move || {
            run_harness_phase(
                format!("{name}-left"),
                path,
                stores,
                left_harness_seed,
                shutdown_at_ms,
                LEFT_ACTORS,
            )
        })
    };
    let right_handle = {
        let stores = stores.clone();
        let path = right_path.clone();
        std::thread::spawn(move || {
            run_harness_phase(
                format!("{name}-right"),
                path,
                stores,
                right_harness_seed,
                shutdown_at_ms,
                RIGHT_ACTORS,
            )
        })
    };
    join_phase(name, "left", left_handle)?;
    join_phase(name, "right", right_handle)?;

    let left_rows = snapshot_rows(left_path.clone(), stores.clone(), next_seed())?;
    let right_rows = snapshot_rows(right_path.clone(), stores.clone(), next_seed())?;
    assert!(left_rows.iter().all(|(key, _)| key.as_ref() < SPLIT_KEY));
    assert!(right_rows.iter().all(|(key, _)| key.as_ref() >= SPLIT_KEY));

    create_clone(
        merged_path.clone(),
        vec![
            CloneSourceSpec::new(left_path).with_projection_range(left_range),
            CloneSourceSpec::new(right_path).with_projection_range(right_range),
        ],
        stores.clone(),
        next_seed(),
    )?;

    let merged_rows = snapshot_rows(merged_path.clone(), stores.clone(), next_seed())?;
    let expected_merged = left_rows.into_iter().chain(right_rows).collect::<Rows>();
    assert_eq!(merged_rows, expected_merged);

    run_harness_phase(
        format!("{name}-merged"),
        merged_path,
        stores,
        next_seed(),
        shutdown_at_ms,
        ROOT_ACTORS,
    )?;

    Ok(())
}

fn run_harness_phase(
    name: String,
    path: Path,
    stores: ScenarioStores,
    seed: u64,
    shutdown_at_ms: i64,
    actor_names: &'static [&'static str],
) -> ScenarioResult<()> {
    let rand = Arc::new(DbRand::new(seed));
    let system_clock = Arc::new(MockSystemClock::new());
    let workload_options = WorkloadActorOptions {
        read_durability: DurabilityLevel::Remote,
        ..WorkloadActorOptions::default()
    };
    let harness_name = name.clone();
    let mut harness = Harness::new(name, seed, move |ctx| async move {
        let failures = ctx.failure_controller();
        for index in 0..10 {
            failures.add_toxic(build_toxic(ctx.rand(), ctx.path().as_ref(), index));
        }

        let db_seed = ctx.rand().rng().next_u64();
        let mut settings = build_settings(ctx.rand()).await;
        settings.l0_sst_size_bytes = 1024;
        settings.l0_max_ssts = 4;
        settings.max_unflushed_bytes = 64 * 1024;
        settings.manifest_poll_interval = Duration::from_millis(10);
        if let Some(gc_options) = settings.garbage_collector_options.as_mut() {
            // Parallel projected children share a parent. Keep the shard-local
            // GC tasks active, but prevent both children from concurrently
            // releasing checkpoints in the parent's manifest.
            gc_options.detach_options = None;
        }
        // RFC-0004 union cannot merge WAL state. Keep this scenario focused on
        // projection/union and exercise WAL draining in a separate protocol test.
        settings.wal_enabled = false;

        let mut builder = Db::builder(ctx.path().clone(), ctx.main_object_store())
            .with_wal_object_store(ctx.wal_object_store().expect("configured"))
            .with_system_clock(ctx.system_clock())
            .with_fp_registry(ctx.fp_registry())
            .with_seed(db_seed)
            .with_settings(settings);
        if let Some(merge_operator) = ctx.merge_operator() {
            builder = builder.with_merge_operator(merge_operator);
        }
        let db = builder.build().await?;
        Ok(Arc::new(db))
    })
    .with_rand(rand)
    .with_system_clock(system_clock)
    .with_path(path)
    .with_main_object_store(stores.main)
    .with_wal_object_store(stores.wal)
    .with_merge_operator(Arc::new(WorkloadMergeOperator));

    for actor_name in actor_names {
        harness = harness.actor(*actor_name, WorkloadActor::new(workload_options.clone())?);
    }
    harness = harness
        .actor("flusher", FlusherActor::new(1_u64..=5_u64)?)
        .actor("shutdown", ShutdownActor::new(shutdown_at_ms)?);

    info!("starting rescaling harness phase [name={harness_name}]");
    harness.run()?;
    Ok(())
}

fn join_phase(
    scenario: &str,
    phase: &str,
    handle: std::thread::JoinHandle<ScenarioResult<()>>,
) -> ScenarioResult<()> {
    match handle.join() {
        Ok(result) => result,
        Err(payload) => {
            error!("dst {scenario} child phase panicked [phase={phase}]");
            std::panic::resume_unwind(payload);
        }
    }
}

fn block_on_seeded<F>(seed: u64, future: F) -> F::Output
where
    F: Future,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build_local(Default::default())
        .expect("failed to build rescaling scenario runtime");
    runtime.block_on(future)
}

fn create_clone(
    clone_path: Path,
    sources: Vec<CloneSourceSpec<ProjectionRange>>,
    stores: ScenarioStores,
    seed: u64,
) -> Result<(), slatedb::Error> {
    block_on_seeded(seed, async move {
        let system_clock: Arc<dyn SystemClock> = Arc::new(MockSystemClock::new());
        let admin = AdminBuilder::new(clone_path, stores.main.clone())
            .with_wal_object_store(stores.wal.clone())
            .with_system_clock(system_clock.clone())
            .with_seed(seed)
            .build();
        let mut sources = sources.into_iter();
        let first = sources
            .next()
            .expect("rescaling clone requires at least one source");
        let mut builder = admin
            .create_clone_builder_from_source(first)
            .with_system_clock(system_clock)
            .with_seed(seed);
        for source in sources {
            builder = builder.with_source(source);
        }
        builder.build().await
    })
}

fn snapshot_rows(path: Path, stores: ScenarioStores, seed: u64) -> ScenarioResult<Rows> {
    block_on_seeded(seed, async move {
        let system_clock: Arc<dyn SystemClock> = Arc::new(MockSystemClock::new());
        let reader = DbReader::builder(path, stores.main)
            .with_wal_object_store(stores.wal)
            .with_system_clock(system_clock)
            .with_seed(seed)
            .with_merge_operator(Arc::new(WorkloadMergeOperator))
            .build()
            .await?;
        let rows_result = async {
            let mut iter = reader.scan(..).await?;
            let mut rows = Vec::new();
            while let Some(kv) = iter.next().await? {
                rows.push((kv.key, kv.value));
            }
            Ok::<_, slatedb::Error>(rows)
        }
        .await;
        let close_result = reader.close().await;
        let rows = rows_result?;
        close_result?;
        Ok::<_, ScenarioError>(rows)
    })
}
