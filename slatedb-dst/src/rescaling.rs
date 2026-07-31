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
//! harnesses run concurrently on one seeded current-thread runtime and share
//! one underlying [`DeterministicLocalFilesystem`]. This exercises interleaved
//! access to the same object store while keeping operation ordering
//! reproducible from the scenario seed.
//!
//! Garbage collection remains enabled, but detach GC is disabled because both
//! projected children reference the root checkpoint. WALs are disabled because
//! manifest union does not combine live WAL state. Projection and union run
//! only after their source harnesses have stopped.
//!
//! The exact snapshot comparisons at the split and merge barriers are the
//! primary assertions: every root row must appear in exactly one child, and
//! every child row must appear in the merged database.

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
    decode_workload_value, FlusherActor, ShutdownActor, WorkloadActor, WorkloadActorOptions,
    WorkloadMergeOperator,
};
use crate::utils::{build_settings, build_toxic, dst_seeds};
use crate::{DeterministicLocalFilesystem, Harness};

type ScenarioError = Box<dyn std::error::Error + Send + Sync>;
type ScenarioResult<T> = Result<T, ScenarioError>;
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
    /// Per-harness mock-clock duration, in milliseconds.
    pub shutdown_at_ms: i64,
}

impl RescalingScenario {
    /// Runs the rescaling scenario across the configured DST seed budget.
    ///
    /// Each seed worker runs all phases on one seeded current-thread runtime.
    /// The disjoint child databases run as concurrent tasks on that shared
    /// deterministic scheduler, so the default seed count matches the number
    /// of available cores.
    pub fn run(self) -> ScenarioResult<()> {
        let Self {
            name,
            shutdown_at_ms,
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

#[instrument(level = "debug", skip_all, fields(scenario = name, seed = seed))]
fn run_seed(name: &'static str, seed: u64, shutdown_at_ms: i64) -> ScenarioResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build_local(Default::default())
        .expect("failed to build rescaling scenario runtime");
    runtime.block_on(run_seed_async(name, seed, shutdown_at_ms))
}

async fn run_seed_async(name: &'static str, seed: u64, shutdown_at_ms: i64) -> ScenarioResult<()> {
    let tempdir = TempDir::new()?;
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        DeterministicLocalFilesystem::new_with_prefix(tempdir.path())?,
    );
    let seed_rng = DbRand::new(seed);
    let next_seed = || seed_rng.rng().next_u64();
    let root_path = Path::from(format!("{name}/root"));
    let left_path = Path::from(format!("{name}/split/left"));
    let right_path = Path::from(format!("{name}/split/right"));
    let merged_path = Path::from(format!("{name}/merged"));

    let root_end_ms = run_harness_phase(
        format!("{name}-root"),
        root_path.clone(),
        object_store.clone(),
        next_seed(),
        0,
        shutdown_at_ms,
        1,
        ROOT_ACTORS,
    )
    .await?;
    let root_rows = snapshot_rows(
        root_path.clone(),
        object_store.clone(),
        next_seed(),
        root_end_ms,
    )
    .await?;
    let child_next_value_version = next_workload_value_version(&root_rows);

    let split_key = Bytes::from_static(SPLIT_KEY);
    let left_range = (Bound::Unbounded, Bound::Excluded(split_key.clone()));
    let right_range = (Bound::Included(split_key), Bound::Unbounded);
    create_clone(
        left_path.clone(),
        vec![CloneSourceSpec::new(root_path.clone()).with_projection_range(left_range.clone())],
        object_store.clone(),
        next_seed(),
        root_end_ms,
    )
    .await?;
    create_clone(
        right_path.clone(),
        vec![CloneSourceSpec::new(root_path).with_projection_range(right_range.clone())],
        object_store.clone(),
        next_seed(),
        root_end_ms,
    )
    .await?;

    let left_after_split = snapshot_rows(
        left_path.clone(),
        object_store.clone(),
        next_seed(),
        root_end_ms,
    )
    .await?;
    let right_after_split = snapshot_rows(
        right_path.clone(),
        object_store.clone(),
        next_seed(),
        root_end_ms,
    )
    .await?;
    let (expected_left, expected_right): (Rows, Rows) = root_rows
        .into_iter()
        .partition(|(key, _)| key.as_ref() < SPLIT_KEY);
    assert_eq!(
        left_after_split, expected_left,
        "projection mismatch [scenario={name}, seed={seed}, phase=split, partition=left]"
    );
    assert_eq!(
        right_after_split, expected_right,
        "projection mismatch [scenario={name}, seed={seed}, phase=split, partition=right]"
    );

    let (left_end_ms, right_end_ms) = tokio::try_join!(
        run_harness_phase(
            format!("{name}-left"),
            left_path.clone(),
            object_store.clone(),
            next_seed(),
            root_end_ms,
            shutdown_at_ms,
            child_next_value_version,
            LEFT_ACTORS,
        ),
        run_harness_phase(
            format!("{name}-right"),
            right_path.clone(),
            object_store.clone(),
            next_seed(),
            root_end_ms,
            shutdown_at_ms,
            child_next_value_version,
            RIGHT_ACTORS,
        ),
    )?;
    let children_end_ms = left_end_ms.max(right_end_ms);

    let left_rows = snapshot_rows(
        left_path.clone(),
        object_store.clone(),
        next_seed(),
        children_end_ms,
    )
    .await?;
    let right_rows = snapshot_rows(
        right_path.clone(),
        object_store.clone(),
        next_seed(),
        children_end_ms,
    )
    .await?;
    assert!(
        left_rows.iter().all(|(key, _)| key.as_ref() < SPLIT_KEY),
        "child contains key outside its range [scenario={name}, seed={seed}, phase=children, partition=left]"
    );
    assert!(
        right_rows.iter().all(|(key, _)| key.as_ref() >= SPLIT_KEY),
        "child contains key outside its range [scenario={name}, seed={seed}, phase=children, partition=right]"
    );

    create_clone(
        merged_path.clone(),
        vec![
            CloneSourceSpec::new(left_path).with_projection_range(left_range),
            CloneSourceSpec::new(right_path).with_projection_range(right_range),
        ],
        object_store.clone(),
        next_seed(),
        children_end_ms,
    )
    .await?;

    let merged_rows = snapshot_rows(
        merged_path.clone(),
        object_store.clone(),
        next_seed(),
        children_end_ms,
    )
    .await?;
    let expected_merged = left_rows.into_iter().chain(right_rows).collect::<Rows>();
    let merged_next_value_version = next_workload_value_version(&expected_merged);
    assert_eq!(
        merged_rows, expected_merged,
        "union mismatch [scenario={name}, seed={seed}, phase=merge]"
    );

    run_harness_phase(
        format!("{name}-merged"),
        merged_path,
        object_store,
        next_seed(),
        children_end_ms,
        shutdown_at_ms,
        merged_next_value_version,
        ROOT_ACTORS,
    )
    .await?;

    Ok(())
}

async fn run_harness_phase(
    name: String,
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    seed: u64,
    start_at_ms: i64,
    shutdown_at_ms: i64,
    next_value_version: u64,
    actor_names: &'static [&'static str],
) -> ScenarioResult<i64> {
    let system_clock = Arc::new(MockSystemClock::with_time(start_at_ms));
    let shutdown_at_ms = start_at_ms
        .checked_add(shutdown_at_ms)
        .expect("rescaling phase shutdown timestamp must not overflow");
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
        settings
            .garbage_collector_options
            .as_mut()
            .expect("rescaling scenario requires garbage collection")
            .detach_options = None;
        // Manifest union rejects sources with live WAL data.
        settings.wal_enabled = false;

        let db = Db::builder(ctx.path().clone(), ctx.main_object_store())
            .with_system_clock(ctx.system_clock())
            .with_fp_registry(ctx.fp_registry())
            .with_seed(db_seed)
            .with_settings(settings)
            .with_merge_operator(
                ctx.merge_operator()
                    .expect("rescaling workload requires a merge operator"),
            )
            .build()
            .await?;
        Ok(Arc::new(db))
    })
    .with_path(path)
    .with_main_object_store(object_store)
    .with_system_clock(system_clock.clone())
    .with_merge_operator(Arc::new(WorkloadMergeOperator));

    for actor_name in actor_names {
        let actor = WorkloadActor::new(workload_options.clone())?
            .with_next_value_version(next_value_version);
        harness = harness.actor(*actor_name, actor);
    }
    harness = harness
        .actor("flusher", FlusherActor::new(1_u64..=5_u64)?)
        .actor("shutdown", ShutdownActor::new(shutdown_at_ms)?);

    info!("starting rescaling harness phase [name={harness_name}]");
    harness.run_async().await?;
    Ok(system_clock.now().timestamp_millis())
}

fn next_workload_value_version(rows: &Rows) -> u64 {
    rows.iter()
        .map(|(_, value)| decode_workload_value(value.as_ref()))
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .expect("workload value version must not overflow")
}

async fn create_clone(
    clone_path: Path,
    sources: Vec<CloneSourceSpec<(Bound<Bytes>, Bound<Bytes>)>>,
    object_store: Arc<dyn ObjectStore>,
    seed: u64,
    start_at_ms: i64,
) -> Result<(), slatedb::Error> {
    let system_clock: Arc<dyn SystemClock> = Arc::new(MockSystemClock::with_time(start_at_ms));
    let admin = AdminBuilder::new(clone_path, object_store)
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
}

async fn snapshot_rows(
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    seed: u64,
    start_at_ms: i64,
) -> ScenarioResult<Rows> {
    let system_clock: Arc<dyn SystemClock> = Arc::new(MockSystemClock::with_time(start_at_ms));
    let reader = DbReader::builder(path, object_store)
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
    Ok(rows)
}
