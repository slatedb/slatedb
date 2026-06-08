//! Exercises the DST bank actors under injected object-store faults.
#![cfg(dst)]

use std::sync::Arc;
use std::time::Duration;

use log::info;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::{Rng, RngCore};
use rstest::rstest;
use slatedb::{Db, DbRand, Error};
use slatedb_common::clock::MockSystemClock;
use slatedb_dst::{
    actors::{
        initialize_accounts, AuditorActor, BankAuditView, BankMergeOperator, BankOptions,
        CompactorActor, CompactorActorOptions, DbFencerActor, DbFencerActorOptions, ShutdownActor,
        SuppressFenced, TransferActor, TransferMode,
    },
    failing_object_store::ToxicKind,
    utils::{build_reader_options, build_settings, build_settings_compactor, build_toxic},
    DeterministicLocalFilesystem, Harness, StartupCtx,
};
use tempfile::TempDir;

#[rstest]
#[cfg_attr(not(slow), case::regular(200_000))]
#[cfg_attr(slow, case::slow(2_000_000))]
fn test_dst_bank_with_toxics(
    #[case] shutdown_at_ms: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let seed = rand::random::<u64>();
    info!("dst bank seed: {seed}");
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

    let bank_options = random_bank_options(&rand);
    let audit_interval = Duration::from_millis(1000);
    let reader_options = build_reader_options(&rand);
    let fencer_restart_interval = Duration::from_secs(120);
    let compactor_options = build_settings_compactor(&mut *rand.rng());
    let merge_operator = Arc::new(BankMergeOperator::new(&bank_options)?);

    info!("dst bank options: {bank_options:?}");
    info!("dst bank reader options: {reader_options:?}");
    info!("dst bank compactor options: {compactor_options:?}");
    info!("dst bank audit interval: {audit_interval:?}");
    info!("dst bank fencer restart interval: {fencer_restart_interval:?}");
    info!("dst bank shutdown at: {shutdown_at_ms}ms");

    let harness = Harness::new("bank", seed, {
        let bank_options = bank_options.clone();
        move |ctx| async move {
            let failures = ctx.failure_controller();
            for index in 0..10 {
                let toxic = build_toxic(ctx.rand(), ctx.path().as_ref(), index);
                // Skip bandwidth toxics for WAL paths. If bandwidth is too low, reader
                // checkpoints can expire during WAL replay. Since we only refresh checkpoint
                // lifetimes _after_ replay, this can cause readers to fail with checkpoint
                // missing errors.
                let skip_toxic = matches!(toxic.kind, ToxicKind::Bandwidth { .. })
                    && toxic
                        .path_prefix
                        .as_deref()
                        // None and "bank" both apply to all paths, so skip them, too.
                        .is_none_or(|p| p.ends_with("wal") || p.ends_with("bank"));
                if !skip_toxic {
                    info!("adding toxic: {toxic:?}");
                    failures.add_toxic(toxic);
                }
            }

            let db = open_bank_db(ctx).await?;
            initialize_accounts(db.as_ref(), &bank_options).await?;

            Ok(db)
        }
    })
    .with_rand(rand)
    .with_system_clock(system_clock)
    .with_path(Path::from("bank"))
    .with_main_object_store(main_store)
    .with_wal_object_store(wal_store)
    .with_merge_operator(merge_operator)
    .with_clock_advance(1..=5);

    let harness = harness
        .actor(
            "transfer-1",
            SuppressFenced::new(TransferActor::new(bank_options.clone(), TransferMode::Put)?),
        )
        .actor(
            "transfer-2",
            SuppressFenced::new(TransferActor::new(bank_options.clone(), TransferMode::Put)?),
        )
        .actor(
            "transfer-3",
            SuppressFenced::new(TransferActor::new(bank_options.clone(), TransferMode::Put)?),
        )
        .actor(
            "transfer-4",
            SuppressFenced::new(TransferActor::new(
                bank_options.clone(),
                TransferMode::Merge,
            )?),
        )
        .actor(
            "transfer-5",
            SuppressFenced::new(TransferActor::new(
                bank_options.clone(),
                TransferMode::Merge,
            )?),
        )
        .actor(
            "transfer-6",
            SuppressFenced::new(TransferActor::new(
                bank_options.clone(),
                TransferMode::Merge,
            )?),
        )
        .actor(
            "regular-auditor",
            SuppressFenced::new(AuditorActor::new(bank_options.clone(), audit_interval)?),
        )
        .actor(
            "snapshot-auditor",
            SuppressFenced::new(AuditorActor::new_with_view(
                bank_options.clone(),
                audit_interval,
                BankAuditView::Snapshot,
            )?),
        )
        .actor(
            "reader-auditor",
            AuditorActor::new_with_view(
                bank_options,
                audit_interval,
                BankAuditView::Reader {
                    options: reader_options,
                },
            )?,
        )
        .actor(
            "db-fencer",
            DbFencerActor::new(
                DbFencerActorOptions {
                    restart_interval: fencer_restart_interval,
                },
                |ctx| async move { open_bank_db(ctx.startup_ctx().clone()).await },
            )?,
        )
        .actor(
            "compactor",
            CompactorActor::new(CompactorActorOptions {
                restart_interval: Duration::from_millis(250),
                compactor_options,
            })?,
        )
        .actor("shutdown", ShutdownActor::new(shutdown_at_ms)?);

    harness.run()?;

    Ok(())
}

async fn open_bank_db(ctx: StartupCtx) -> Result<Arc<Db>, Error> {
    let db_seed = ctx.rand().rng().next_u64();
    let mut settings = build_settings(ctx.rand()).await;

    // Clock ticks in the harness and `Toxic` clock advances go _very_ fast.
    // This can cause the auditor's scan to appear to take longer than 15
    // minutes. Since the compactor sets a checkpoint with a 15m timeout before
    // updating the manifest, scans that take longer than 15m can result in a
    // "FileNotFound" if the GC removes an SST in the scan after the checkpoint
    // expires. Disable `compacted` GC until #319 is done.
    settings
        .garbage_collector_options
        .as_mut()
        .expect("build_settings should configure garbage collection")
        .compacted_options = None;

    // The test registers the standalone compactor actor below.
    settings.compactor_options = None;

    // DB fencing currently relies on WAL barrier files.
    #[cfg(feature = "wal_disable")]
    {
        settings.wal_enabled = true;
    }

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
}

fn random_bank_options(rand: &DbRand) -> BankOptions {
    let mut rng = rand.rng();
    let account_count = rng.random_range(2..999);
    let initial_balance = rng.random_range(1_000..=100_000);
    let max_transfer = rng.random_range(1..=initial_balance.min(1_000));
    let value_size_bytes = rng.random_range(8..=8192);

    BankOptions {
        prefix: format!("acct-{:016x}", rng.next_u64()),
        account_count,
        initial_balance,
        max_transfer,
        value_size_bytes,
    }
}
