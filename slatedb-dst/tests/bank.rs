//! Exercises the DST bank actors under injected object-store faults.
#![cfg(dst)]

use std::sync::Arc;
use std::time::Duration;

use log::info;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::{Rng, RngCore};
use slatedb::config::{CompactorOptions, SizeTieredCompactionSchedulerOptions};
use slatedb::{Db, DbRand};
use slatedb_common::clock::MockSystemClock;
use slatedb_dst::{
    actors::{
        initialize_accounts, AuditorActor, BankOptions, CompactorActor, CompactorActorOptions,
        ShutdownActor, TransferActor,
    },
    utils::{add_toxics, build_settings},
    DeterministicLocalFilesystem, FailingObjectStore, FailingObjectStoreController, Harness,
};
use tempfile::TempDir;

#[test]
fn test_dst_bank_with_toxics() -> Result<(), Box<dyn std::error::Error>> {
    let seed = rand::random::<u64>();
    info!("dst bank seed: {seed}");
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let rand = Arc::new(DbRand::new(seed));
    let system_clock = Arc::new(MockSystemClock::new());
    let failure_seed = rand.rng().next_u64();
    let failure_rand = Arc::new(DbRand::new(failure_seed));
    let failures = FailingObjectStoreController::new(failure_rand.clone());
    add_toxics(&failures, failure_rand.as_ref(), "bank", 10);

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

    let bank_options = random_bank_options(&rand);
    info!("dst bank options: {bank_options:?}");
    let audit_interval = Duration::from_millis(1000);
    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(5),
        manifest_update_timeout: Duration::from_millis(250),
        max_sst_size: 8 * 1024,
        max_concurrent_compactions: 2,
        max_fetch_tasks: 4,
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            max_compaction_sources: 16,
            include_size_threshold: 4.0,
        }
        .into(),
    };

    let harness = Harness::new("bank", seed, {
        let bank_options = bank_options.clone();
        move |ctx| async move {
            let db_seed = ctx.rand().rng().next_u64();
            let mut settings = build_settings(ctx.rand()).await;

            // The auditor scans account ranges and needs SSTs to stay live for
            // the duration of the scan. Keep compacted GC from deleting SSTs
            // out from under it when they are less than 5s old.
            let gc_compacted_options = settings
                .garbage_collector_options
                .as_mut()
                .expect("build_settings should configure garbage collection")
                .compacted_options
                .as_mut()
                .expect("build_settings should configure compacted garbage collection");
            gc_compacted_options.min_age = gc_compacted_options.min_age.max(Duration::from_secs(5));

            // The test registers the standalone compactor actor below.
            settings.compactor_options = None;

            let db = Db::builder(ctx.path().clone(), ctx.main_object_store())
                .with_wal_object_store(ctx.wal_object_store().expect("configured"))
                .with_system_clock(ctx.system_clock())
                .with_fp_registry(ctx.fp_registry())
                .with_seed(db_seed)
                .with_settings(settings)
                .build()
                .await?;
            initialize_accounts(&db, &bank_options).await?;

            Ok(Arc::new(db))
        }
    })
    .with_rand(rand)
    .with_system_clock(system_clock)
    .with_path(Path::from("bank"))
    .with_main_object_store(main_store)
    .with_wal_object_store(wal_store)
    .with_clock_advance(1..=5);

    let harness = harness
        .actor("transfer-1", TransferActor::new(bank_options.clone())?)
        .actor("transfer-2", TransferActor::new(bank_options.clone())?)
        .actor("transfer-3", TransferActor::new(bank_options.clone())?)
        .actor("transfer-4", TransferActor::new(bank_options.clone())?)
        .actor("transfer-5", TransferActor::new(bank_options.clone())?)
        .actor("transfer-6", TransferActor::new(bank_options.clone())?)
        .actor(
            "auditor-1",
            AuditorActor::new(bank_options.clone(), audit_interval)?,
        )
        .actor(
            "auditor-2",
            AuditorActor::new(bank_options, audit_interval)?,
        )
        .actor(
            "compactor",
            CompactorActor::new(CompactorActorOptions {
                restart_interval: Duration::from_millis(250),
                compactor_options,
            })?,
        )
        .actor("shutdown", ShutdownActor::new(200_000)?);

    harness.run()?;

    Ok(())
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
