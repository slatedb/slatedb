//! Exercises the DST bank actors under injected object-store faults.
#![cfg(dst)]

use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use slatedb::config::{CompactorOptions, SizeTieredCompactionSchedulerOptions};
use slatedb::{Db, DbRand};
use slatedb_common::clock::MockSystemClock;
use slatedb_dst::{
    actors::{
        bank::{auditor, transfer, BankOptions},
        compactor, initialize_accounts, shutdown, CompactorActorOptions,
    },
    utils::build_settings,
    DeterministicLocalFilesystem, FailingObjectStore, FailingObjectStoreController, Harness,
    Operation, StreamDirection, Toxic, ToxicKind,
};
use tempfile::TempDir;

#[test]
fn test_dst_bank_with_toxics() -> Result<(), Box<dyn std::error::Error>> {
    let seed = rand::random::<u64>();
    println!("dst bank seed: {seed}");
    let tempdir = TempDir::new()?;
    let main_dir = tempdir.path().join("main");
    let wal_dir = tempdir.path().join("wal");
    std::fs::create_dir_all(&main_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let rand = Arc::new(DbRand::new(seed));
    let system_clock = Arc::new(MockSystemClock::new());
    let failure_seed = rand.rng().next_u64();
    let failures = FailingObjectStoreController::new(Arc::new(DbRand::new(failure_seed)));
    add_toxics(&failures);

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

    let bank_options = BankOptions {
        account_count: 48,
        initial_balance: 25_000,
        max_transfer: 500,
        ..BankOptions::default()
    };
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

    Harness::new("bank", seed, {
        let bank_options = bank_options.clone();
        move |ctx| async move {
            let db_seed = ctx.rand().rng().next_u64();
            let mut settings = build_settings(ctx.rand()).await;

            settings.l0_sst_size_bytes = 1024;
            settings.l0_max_ssts = 4;
            settings.manifest_poll_interval = Duration::from_millis(10);
            settings.manifest_update_timeout = Duration::from_millis(250);
            settings.max_unflushed_bytes = 8 * 1024;
            settings.flush_interval = Some(Duration::from_millis(10));
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
    .with_clock_advance(1..=5)
    .actor_with_state("transfer", 6, bank_options.clone(), transfer)
    .actor_with_state("auditor", 2, bank_options, auditor)
    .actor_with_state(
        "compactor",
        1,
        CompactorActorOptions {
            restart_interval: Duration::from_millis(250),
            compactor_options,
        },
        compactor,
    )
    .actor_with_state("shutdown", 1, 20_000, shutdown)
    .run()?;

    Ok(())
}

fn add_toxics(failures: &FailingObjectStoreController) {
    for toxic in [
        Toxic {
            name: "wal-put-latency".into(),
            kind: ToxicKind::Latency {
                latency: Duration::from_millis(1),
                jitter: Duration::from_millis(4),
            },
            direction: StreamDirection::Upstream,
            toxicity: 0.90,
            operations: vec![Operation::PutOpts],
            path_prefix: Some("bank/wal".into()),
        },
        Toxic {
            name: "main-put-latency".into(),
            kind: ToxicKind::Latency {
                latency: Duration::from_millis(2),
                jitter: Duration::from_millis(6),
            },
            direction: StreamDirection::Upstream,
            toxicity: 0.75,
            operations: vec![Operation::PutOpts],
            path_prefix: None,
        },
        Toxic {
            name: "manifest-read-latency".into(),
            kind: ToxicKind::Latency {
                latency: Duration::from_millis(1),
                jitter: Duration::from_millis(5),
            },
            direction: StreamDirection::Downstream,
            toxicity: 0.80,
            operations: vec![Operation::GetOpts, Operation::GetRange, Operation::Head],
            path_prefix: Some("bank/manifest".into()),
        },
        Toxic {
            name: "sst-get-bandwidth".into(),
            kind: ToxicKind::Bandwidth {
                bytes_per_sec: 64 * 1024,
            },
            direction: StreamDirection::Downstream,
            toxicity: 0.70,
            operations: vec![
                Operation::GetOpts,
                Operation::GetRange,
                Operation::GetRanges,
            ],
            path_prefix: Some("bank/compacted".into()),
        },
        Toxic {
            name: "wal-get-bandwidth".into(),
            kind: ToxicKind::Bandwidth {
                bytes_per_sec: 48 * 1024,
            },
            direction: StreamDirection::Downstream,
            toxicity: 0.65,
            operations: vec![
                Operation::GetOpts,
                Operation::GetRange,
                Operation::GetRanges,
            ],
            path_prefix: Some("bank/wal".into()),
        },
        Toxic {
            name: "list-slow-close".into(),
            kind: ToxicKind::SlowClose {
                delay: Duration::from_millis(2),
            },
            direction: StreamDirection::Downstream,
            toxicity: 0.80,
            operations: vec![Operation::List, Operation::ListWithOffset],
            path_prefix: Some("bank".into()),
        },
        Toxic {
            name: "compactions-put-latency".into(),
            kind: ToxicKind::Latency {
                latency: Duration::from_millis(2),
                jitter: Duration::from_millis(8),
            },
            direction: StreamDirection::Upstream,
            toxicity: 0.80,
            operations: vec![Operation::PutOpts],
            path_prefix: Some("bank/compactions".into()),
        },
        Toxic {
            name: "read-reset".into(),
            kind: ToxicKind::ResetPeer,
            direction: StreamDirection::Upstream,
            toxicity: 0.03,
            operations: vec![
                Operation::GetOpts,
                Operation::GetRange,
                Operation::GetRanges,
            ],
            path_prefix: Some("bank".into()),
        },
        Toxic {
            name: "list-reset".into(),
            kind: ToxicKind::ResetPeer,
            direction: StreamDirection::Upstream,
            toxicity: 0.02,
            operations: vec![Operation::List, Operation::ListWithOffset],
            path_prefix: Some("bank".into()),
        },
        Toxic {
            name: "put-reset".into(),
            kind: ToxicKind::ResetPeer,
            direction: StreamDirection::Upstream,
            toxicity: 0.01,
            operations: vec![Operation::PutOpts],
            path_prefix: Some("bank".into()),
        },
    ] {
        failures.add_toxic(toxic);
    }
}
