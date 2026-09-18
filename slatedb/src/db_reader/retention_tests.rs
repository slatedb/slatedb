use super::{DbReader, DbReaderMessage, DbReaderMode, ManifestPoller};
use crate::{
    block_cache_policy::BlockCachePolicy,
    compactions_store::{CompactionsStore, StoredCompactions},
    compactor_state::{Compaction, CompactionSpec, SourceId},
    config::{
        CompactionWorkerOptions, CompactorOptions, DbReaderOptions, FlushOptions, FlushType,
        GarbageCollectorDirectoryOptions, GarbageCollectorOptions, ScanOptions, Settings,
    },
    db::builder::CompactorBuilder,
    db_state::{SsTableHandle, SsTableId, SsTableView},
    dispatcher::MessageHandler,
    format::sst::SsTableFormat,
    garbage_collector::GarbageCollector,
    manifest::{
        store::{ManifestStore, StoredManifest},
        ManifestCore,
    },
    tablestore::{TableStore, TableStoreKind},
    test_utils::{OnDemandCompactionSchedulerSupplier, RecordingObjectStore},
    types::RowEntry,
    wal::slatedb::store::WalTableStore,
    Db,
};
use bytes::Bytes;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use slatedb_common::{
    clock::{DefaultSystemClock, SystemClock},
    metrics::MetricsRecorderHelper,
    DbRand, MockSystemClock,
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use ulid::Ulid;

async fn write_rows(table_store: &Arc<TableStore>, id: SsTableId, value: &[u8]) -> SsTableHandle {
    let mut builder = table_store.table_builder();
    for index in 0..64 {
        let key = Bytes::from(format!("key-{index:03}"));
        let mut row_value = Vec::from(value);
        row_value.resize(96, b'x');
        builder
            .add(RowEntry::new_value(key.as_ref(), &row_value, 1))
            .await
            .unwrap();
    }
    let table = builder.build().await.unwrap();
    table_store
        .write_sst(&id, &table, Some(Bytes::new()))
        .await
        .unwrap()
}

#[tokio::test]
async fn managed_scan_retains_ssts_across_refresh_and_gc() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reader_recording = Arc::new(RecordingObjectStore::new(Arc::clone(&raw_store)));
    let reader_store: Arc<dyn ObjectStore> = reader_recording.clone();
    let root = Path::from("reader-retention-regression");
    let clock = Arc::new(MockSystemClock::with_time(10_000));
    let format = SsTableFormat {
        block_size: 128,
        ..SsTableFormat::default()
    };

    let writer_tables = Arc::new(TableStore::new(
        Arc::clone(&raw_store),
        format.clone(),
        root.clone(),
        None,
        TableStoreKind::Main,
        BlockCachePolicy::default(),
    ));
    let reader_tables = Arc::new(TableStore::new(
        Arc::clone(&reader_store),
        format.clone(),
        root.clone(),
        None,
        TableStoreKind::Reader,
        BlockCachePolicy::default(),
    ));
    let gc_tables = Arc::new(TableStore::new(
        Arc::clone(&raw_store),
        format.clone(),
        root.clone(),
        None,
        TableStoreKind::GC,
        BlockCachePolicy::default(),
    ));
    let manifest_store = Arc::new(ManifestStore::new(&root, Arc::clone(&raw_store)));
    let reader_manifest_store = Arc::new(ManifestStore::new(&root, Arc::clone(&reader_store)));
    let compactions_store = Arc::new(CompactionsStore::new(&root, Arc::clone(&raw_store)));
    let wal_store = Arc::new(WalTableStore::new(
        Arc::clone(&raw_store),
        format,
        root.clone(),
        TableStoreKind::Reader,
    ));

    let old_id = SsTableId::from(Ulid::from_parts(1_000, 0));
    let control_id = SsTableId::from(Ulid::from_parts(2_000, 0));
    let replacement_id = SsTableId::from(Ulid::from_parts(8_000, 0));
    let old_handle = write_rows(&writer_tables, old_id, b"old-value").await;
    let _control_handle = write_rows(&writer_tables, control_id, b"control").await;
    let replacement_handle = write_rows(&writer_tables, replacement_id, b"new-value").await;

    let mut stored_manifest = StoredManifest::create_new_db(
        Arc::clone(&manifest_store),
        ManifestCore::new(),
        clock.clone(),
    )
    .await
    .unwrap();
    let mut dirty = stored_manifest.prepare_dirty().unwrap();
    Arc::make_mut(&mut dirty.value.core.tree)
        .l0
        .push_front(SsTableView::identity(old_handle));
    dirty.value.core.last_l0_seq = 1;
    stored_manifest.update(dirty).await.unwrap();

    let reader = DbReader::open_internal(
        Arc::clone(&reader_manifest_store),
        Arc::clone(&reader_tables),
        Arc::clone(&wal_store),
        DbReaderMode::ManagedCheckpoint,
        None,
        None,
        None,
        DbReaderOptions {
            manifest_poll_interval: Duration::from_secs(10),
            checkpoint_lifetime: Duration::from_secs(60),
            skip_wal_replay: true,
            object_store_max_retries: Some(0),
            ..DbReaderOptions::default()
        },
        clock.clone(),
        Arc::new(DbRand::default()),
        MetricsRecorderHelper::noop(),
    )
    .await
    .unwrap();

    let mut scan = reader
        .scan_with_options(
            ..,
            &ScanOptions {
                cache_blocks: false,
                max_fetch_tasks: 1,
                read_ahead_bytes: 1,
                ..ScanOptions::default()
            },
        )
        .await
        .unwrap();
    let first = scan.next().await.unwrap().unwrap();
    assert_eq!(first.key, Bytes::from_static(b"key-000"));
    assert!(first.value.starts_with(b"old-value"));

    let mut stored_manifest = StoredManifest::load(Arc::clone(&manifest_store), clock.clone())
        .await
        .unwrap();
    let mut dirty = stored_manifest.prepare_dirty().unwrap();
    let tree = Arc::make_mut(&mut dirty.value.core.tree);
    tree.l0.clear();
    tree.l0
        .push_front(SsTableView::identity(replacement_handle));
    dirty.value.core.last_l0_seq = 2;
    stored_manifest.update(dirty).await.unwrap();

    let mut poller = ManifestPoller {
        inner: Arc::clone(&reader.inner),
    };
    poller.handle(DbReaderMessage::PollManifest).await.unwrap();
    let new_value = reader.get(b"key-060").await.unwrap().unwrap();
    assert!(new_value.starts_with(b"new-value"));

    let mut stored_compactions = StoredCompactions::create(
        Arc::clone(&compactions_store),
        stored_manifest.manifest().compactor_epoch,
    )
    .await
    .unwrap();
    let mut dirty = stored_compactions.prepare_dirty().unwrap();
    dirty.value.insert(Compaction::new(
        Ulid::from_parts(9_000, 0),
        CompactionSpec::new(vec![SourceId::SortedRun(0)], 0),
    ));
    stored_compactions.update(dirty).await.unwrap();

    let gc = GarbageCollector::new(
        Arc::clone(&manifest_store),
        compactions_store,
        gc_tables,
        wal_store,
        Arc::clone(&raw_store),
        GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: None,
                min_age: Duration::from_secs(1),
                dry_run: false,
            }),
            compactions_options: None,
            detach_options: None,
            metric_level: None,
            boundary_files_enabled: true,
            object_store_max_retries: Some(0),
        },
        &MetricsRecorderHelper::noop(),
        clock,
        None,
        None,
    );
    gc.run_gc_once().await;

    let remaining_ids = writer_tables
        .list_compacted_ssts(..)
        .await
        .unwrap()
        .into_iter()
        .map(|metadata| metadata.id)
        .collect::<HashSet<_>>();
    let old_sst_present = remaining_ids.contains(&old_id);
    assert!(
        !remaining_ids.contains(&control_id),
        "GC did not delete the eligible control SST"
    );

    reader_recording.clear();
    let seek_result = scan.seek(b"key-060").await;
    let reader_gets_after_gc = reader_recording
        .get_kinds(false)
        .into_iter()
        .filter(|kind| *kind == Some(TableStoreKind::Reader))
        .count();
    assert!(
        reader_gets_after_gc > 0,
        "later seek did not read from the backing object store"
    );
    assert!(
        old_sst_present,
        "managed scan lost its backing SST after checkpoint refresh and GC; seek result: {seek_result:?}; reader GET count: {reader_gets_after_gc}"
    );
    seek_result.unwrap();
    let later = scan.next().await.unwrap().unwrap();
    assert_eq!(later.key, Bytes::from_static(b"key-060"));
    assert!(later.value.starts_with(b"old-value"));
    drop(scan);
    reader.close().await.unwrap();
}

fn expected_rows(value: &[u8]) -> Vec<(Bytes, Bytes)> {
    (0..64)
        .map(|index| {
            let key = Bytes::from(format!("key-{index:03}"));
            let mut row_value = Vec::from(value);
            row_value.resize(96, b'x');
            (key, Bytes::from(row_value))
        })
        .collect()
}

async fn collect_rows(scan: &mut crate::DbIterator) -> Vec<(Bytes, Bytes)> {
    let mut rows = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        rows.push((row.key, row.value));
    }
    rows
}

async fn publish_generation(
    manifest_store: &Arc<ManifestStore>,
    clock: &Arc<MockSystemClock>,
    handle: SsTableHandle,
    last_l0_seq: u64,
) {
    let mut stored_manifest = StoredManifest::load(Arc::clone(manifest_store), clock.clone())
        .await
        .unwrap();
    let mut dirty = stored_manifest.prepare_dirty().unwrap();
    let tree = Arc::make_mut(&mut dirty.value.core.tree);
    tree.l0.clear();
    tree.l0.push_front(SsTableView::identity(handle));
    dirty.value.core.last_l0_seq = last_l0_seq;
    stored_manifest.update(dirty).await.unwrap();
}

async fn checkpoint_count(
    manifest_store: &Arc<ManifestStore>,
    clock: &Arc<MockSystemClock>,
) -> usize {
    StoredManifest::load(Arc::clone(manifest_store), clock.clone())
        .await
        .unwrap()
        .manifest()
        .core
        .checkpoints
        .len()
}

#[tokio::test]
async fn managed_scans_keep_multiple_generations_until_final_release() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reader_recording = Arc::new(RecordingObjectStore::new(Arc::clone(&raw_store)));
    let reader_store: Arc<dyn ObjectStore> = reader_recording.clone();
    let root = Path::from("reader-retention-generations");
    let clock = Arc::new(MockSystemClock::with_time(10_000));
    let format = SsTableFormat {
        block_size: 128,
        ..SsTableFormat::default()
    };
    let writer_tables = Arc::new(TableStore::new(
        Arc::clone(&raw_store),
        format.clone(),
        root.clone(),
        None,
        TableStoreKind::Main,
        BlockCachePolicy::default(),
    ));
    let reader_tables = Arc::new(TableStore::new(
        Arc::clone(&reader_store),
        format.clone(),
        root.clone(),
        None,
        TableStoreKind::Reader,
        BlockCachePolicy::default(),
    ));
    let gc_tables = Arc::new(TableStore::new(
        Arc::clone(&raw_store),
        format.clone(),
        root.clone(),
        None,
        TableStoreKind::GC,
        BlockCachePolicy::default(),
    ));
    let manifest_store = Arc::new(ManifestStore::new(&root, Arc::clone(&raw_store)));
    let reader_manifest_store = Arc::new(ManifestStore::new(&root, Arc::clone(&reader_store)));
    let compactions_store = Arc::new(CompactionsStore::new(&root, Arc::clone(&raw_store)));
    let wal_store = Arc::new(WalTableStore::new(
        Arc::clone(&raw_store),
        format,
        root.clone(),
        TableStoreKind::Reader,
    ));

    let old_id = SsTableId::from(Ulid::from_parts(1_000, 0));
    let control_id = SsTableId::from(Ulid::from_parts(2_000, 0));
    let middle_id = SsTableId::from(Ulid::from_parts(7_000, 0));
    let latest_id = SsTableId::from(Ulid::from_parts(8_500, 0));
    let old_handle = write_rows(&writer_tables, old_id, b"old-value").await;
    let _control_handle = write_rows(&writer_tables, control_id, b"control").await;
    let middle_handle = write_rows(&writer_tables, middle_id, b"middle-value").await;
    let latest_handle = write_rows(&writer_tables, latest_id, b"latest-value").await;

    let mut stored_manifest = StoredManifest::create_new_db(
        Arc::clone(&manifest_store),
        ManifestCore::new(),
        clock.clone(),
    )
    .await
    .unwrap();
    let mut dirty = stored_manifest.prepare_dirty().unwrap();
    Arc::make_mut(&mut dirty.value.core.tree)
        .l0
        .push_front(SsTableView::identity(old_handle));
    dirty.value.core.last_l0_seq = 1;
    stored_manifest.update(dirty).await.unwrap();

    let reader = DbReader::open_internal(
        Arc::clone(&reader_manifest_store),
        Arc::clone(&reader_tables),
        Arc::clone(&wal_store),
        DbReaderMode::ManagedCheckpoint,
        None,
        None,
        None,
        DbReaderOptions {
            manifest_poll_interval: Duration::from_secs(10),
            checkpoint_lifetime: Duration::from_secs(60),
            skip_wal_replay: true,
            object_store_max_retries: Some(0),
            ..DbReaderOptions::default()
        },
        clock.clone(),
        Arc::new(DbRand::default()),
        MetricsRecorderHelper::noop(),
    )
    .await
    .unwrap();
    let scan_options = ScanOptions {
        cache_blocks: false,
        max_fetch_tasks: 1,
        read_ahead_bytes: 1,
        ..ScanOptions::default()
    };

    reader_recording.clear();
    let mut old_scan = reader.scan_with_options(.., &scan_options).await.unwrap();
    let mut old_peer = reader.scan_with_options(.., &scan_options).await.unwrap();
    for _ in 0..32 {
        let mut short_scan = reader.scan_with_options(.., &scan_options).await.unwrap();
        assert!(short_scan.next().await.unwrap().is_some());
    }
    assert!(reader_recording.write_kinds().is_empty());
    assert_eq!(checkpoint_count(&manifest_store, &clock).await, 1);
    let first = old_scan.next().await.unwrap().unwrap();
    assert_eq!(first.key, Bytes::from_static(b"key-000"));
    assert!(first.value.starts_with(b"old-value"));

    publish_generation(&manifest_store, &clock, middle_handle, 2).await;
    let mut poller = ManifestPoller {
        inner: Arc::clone(&reader.inner),
    };
    poller.handle(DbReaderMessage::PollManifest).await.unwrap();
    let mut middle_scan = reader.scan_with_options(.., &scan_options).await.unwrap();
    assert_eq!(checkpoint_count(&manifest_store, &clock).await, 2);

    publish_generation(&manifest_store, &clock, latest_handle, 3).await;
    poller.handle(DbReaderMessage::PollManifest).await.unwrap();
    assert_eq!(checkpoint_count(&manifest_store, &clock).await, 3);
    let latest_value = reader.get(b"key-060").await.unwrap().unwrap();
    assert!(latest_value.starts_with(b"latest-value"));

    clock.set(41_000);
    reader
        .inner
        .retention
        .as_ref()
        .unwrap()
        .maintain()
        .await
        .unwrap();
    clock.set(72_000);
    reader
        .inner
        .retention
        .as_ref()
        .unwrap()
        .maintain()
        .await
        .unwrap();
    let manifest_after_renewal = StoredManifest::load(Arc::clone(&manifest_store), clock.clone())
        .await
        .unwrap();
    assert_eq!(manifest_after_renewal.manifest().core.checkpoints.len(), 3);
    assert!(manifest_after_renewal
        .manifest()
        .core
        .checkpoints
        .iter()
        .all(|checkpoint| checkpoint
            .expire_time
            .is_some_and(|expiry| expiry > clock.now())));

    let mut stored_compactions = StoredCompactions::create(
        Arc::clone(&compactions_store),
        stored_manifest.manifest().compactor_epoch,
    )
    .await
    .unwrap();
    let mut dirty = stored_compactions.prepare_dirty().unwrap();
    dirty.value.insert(Compaction::new(
        Ulid::from_parts(90_000, 0),
        CompactionSpec::new(vec![SourceId::SortedRun(0)], 0),
    ));
    stored_compactions.update(dirty).await.unwrap();
    let gc = GarbageCollector::new(
        Arc::clone(&manifest_store),
        compactions_store,
        gc_tables,
        wal_store,
        Arc::clone(&raw_store),
        GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: None,
                min_age: Duration::from_secs(1),
                dry_run: false,
            }),
            compactions_options: None,
            detach_options: None,
            metric_level: None,
            boundary_files_enabled: true,
            object_store_max_retries: Some(0),
        },
        &MetricsRecorderHelper::noop(),
        clock.clone(),
        None,
        None,
    );
    gc.run_gc_once().await;

    let retained_ids = writer_tables
        .list_compacted_ssts(..)
        .await
        .unwrap()
        .into_iter()
        .map(|metadata| metadata.id)
        .collect::<HashSet<_>>();
    assert!(!retained_ids.contains(&control_id));
    assert!(retained_ids.contains(&old_id));
    assert!(retained_ids.contains(&middle_id));
    assert!(retained_ids.contains(&latest_id));

    reader_recording.clear();
    old_scan.seek(b"key-060").await.unwrap();
    let later = old_scan.next().await.unwrap().unwrap();
    assert_eq!(later.key, Bytes::from_static(b"key-060"));
    assert!(later.value.starts_with(b"old-value"));
    drop(old_scan);
    reader
        .inner
        .retention
        .as_ref()
        .unwrap()
        .maintain()
        .await
        .unwrap();
    gc.run_gc_once().await;
    assert!(writer_tables
        .list_compacted_ssts(..)
        .await
        .unwrap()
        .into_iter()
        .any(|metadata| metadata.id == old_id));
    assert_eq!(
        collect_rows(&mut old_peer).await,
        expected_rows(b"old-value")
    );
    assert_eq!(
        collect_rows(&mut middle_scan).await,
        expected_rows(b"middle-value")
    );
    assert!(reader_recording
        .get_kinds(false)
        .into_iter()
        .any(|kind| kind == Some(TableStoreKind::Reader)));

    drop(old_peer);
    drop(middle_scan);
    for _ in 0..20 {
        reader
            .inner
            .retention
            .as_ref()
            .unwrap()
            .maintain()
            .await
            .unwrap();
        if checkpoint_count(&manifest_store, &clock).await == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(checkpoint_count(&manifest_store, &clock).await, 1);
    gc.run_gc_once().await;
    let reclaimed_ids = writer_tables
        .list_compacted_ssts(..)
        .await
        .unwrap()
        .into_iter()
        .map(|metadata| metadata.id)
        .collect::<HashSet<_>>();
    assert!(!reclaimed_ids.contains(&old_id));
    assert!(!reclaimed_ids.contains(&middle_id));
    assert_eq!(reclaimed_ids, HashSet::from([latest_id]));
    reader.close().await.unwrap();
}

async fn wait_for_manifest(
    manifest_id: u64,
    manifest: impl Fn() -> crate::manifest::VersionedManifest,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if manifest().id() >= manifest_id {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("manifest did not refresh before the timeout");
}

async fn wait_for_last_l0_seq(
    last_l0_seq: u64,
    manifest: impl Fn() -> crate::manifest::VersionedManifest,
) -> crate::manifest::VersionedManifest {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let manifest = manifest();
            if manifest.manifest.core.last_l0_seq >= last_l0_seq {
                return manifest;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("flushed sequence did not become visible before the timeout")
}

async fn wait_for_checkpoint_expiration_after(
    manifest_store: &Arc<ManifestStore>,
    clock: &Arc<DefaultSystemClock>,
    checkpoint_id: uuid::Uuid,
    previous_expiration: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let stored_manifest = StoredManifest::load(Arc::clone(manifest_store), clock.clone())
                .await
                .unwrap();
            let expiration = stored_manifest
                .manifest()
                .core
                .checkpoints
                .iter()
                .find(|checkpoint| checkpoint.id == checkpoint_id)
                .and_then(|checkpoint| checkpoint.expire_time);
            if expiration.is_some_and(|expiration| expiration > previous_expiration) {
                return expiration.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background lease renewal did not finish before the timeout")
}

fn manifest_sst_ids(manifest: &crate::manifest::Manifest) -> HashSet<SsTableId> {
    manifest
        .core
        .all_sst_views()
        .map(|view| view.sst.id)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_scan_survives_real_compaction_background_refresh_and_gc() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reader_recording = Arc::new(RecordingObjectStore::new(Arc::clone(&raw_store)));
    let reader_store: Arc<dyn ObjectStore> = reader_recording.clone();
    let root = Path::from("reader-retention-real-compaction");
    let clock = Arc::new(DefaultSystemClock::new());
    let compact = Arc::new(AtomicBool::new(false));
    let compact_for_scheduler = Arc::clone(&compact);
    let scheduler = Arc::new(OnDemandCompactionSchedulerSupplier::new(Arc::new(
        move |_| compact_for_scheduler.swap(false, Ordering::SeqCst),
    )));
    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(20),
        max_concurrent_compactions: 1,
        commit_compacted_interval: Duration::from_millis(10),
        checkpoint_lifetime: Duration::from_millis(250),
        worker: Some(CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(20),
            ..CompactionWorkerOptions::default()
        }),
        object_store_max_retries: Some(0),
        ..CompactorOptions::default()
    };
    let db = Db::builder(root.clone(), Arc::clone(&raw_store))
        .with_settings(Settings {
            flush_interval: None,
            manifest_poll_interval: Duration::from_millis(20),
            l0_sst_size_bytes: 1024 * 1024,
            compactor_options: None,
            garbage_collector_options: None,
            object_store_max_retries: Some(0),
            ..Settings::default()
        })
        .with_compactor_builder(
            CompactorBuilder::new(root.clone(), Arc::clone(&raw_store))
                .with_system_clock(clock.clone())
                .with_options(compactor_options)
                .with_scheduler_supplier(scheduler),
        )
        .with_system_clock(clock.clone())
        .build()
        .await
        .unwrap();

    for (key, value) in expected_rows(b"old-value") {
        db.put(key, value).await.unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    let initial_manifest = wait_for_last_l0_seq(1, || db.manifest()).await;
    let old_sst_ids = initial_manifest
        .l0()
        .iter()
        .map(|view| view.sst.id)
        .collect::<HashSet<_>>();
    assert!(!old_sst_ids.is_empty());

    let reader = DbReader::builder(root.clone(), Arc::clone(&reader_store))
        .with_options(DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(20),
            checkpoint_lifetime: Duration::from_secs(1),
            skip_wal_replay: true,
            object_store_max_retries: Some(0),
            ..DbReaderOptions::default()
        })
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    let old_checkpoint = reader
        .inner
        .state
        .read()
        .snapshot_lease
        .as_ref()
        .unwrap()
        .checkpoint();
    let old_checkpoint_id = old_checkpoint.id;
    let old_checkpoint_initial_expiration = old_checkpoint.expire_time.unwrap();
    let scan_options = ScanOptions {
        cache_blocks: false,
        max_fetch_tasks: 1,
        read_ahead_bytes: 1,
        ..ScanOptions::default()
    };
    let mut old_scan = reader.scan_with_options(.., &scan_options).await.unwrap();
    let first = old_scan.next().await.unwrap().unwrap();
    assert_eq!(first.key, Bytes::from_static(b"key-000"));
    assert!(first.value.starts_with(b"old-value"));

    compact.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let manifest = db.manifest();
            if manifest.l0().is_empty() && !manifest.compacted().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("compaction did not finish before the timeout");
    let compacted_manifest = db.manifest();
    assert!(old_sst_ids.is_disjoint(&manifest_sst_ids(&compacted_manifest.manifest)));
    let compacted_manifest_id = compacted_manifest.id();
    wait_for_manifest(compacted_manifest_id, || reader.manifest()).await;

    for (key, value) in expected_rows(b"new-value") {
        db.put(key, value).await.unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    let updated_manifest =
        wait_for_last_l0_seq(compacted_manifest.manifest.core.last_l0_seq + 1, || {
            db.manifest()
        })
        .await;
    wait_for_last_l0_seq(updated_manifest.manifest.core.last_l0_seq, || {
        reader.manifest()
    })
    .await;
    let fresh_value = reader.get(b"key-060").await.unwrap().unwrap();
    assert!(fresh_value.starts_with(b"new-value"));

    let control_id = SsTableId::from(Ulid::from_parts(
        (clock.now().timestamp_millis() - 10_000) as u64,
        0,
    ));
    let control_tables = Arc::new(TableStore::new(
        Arc::clone(&raw_store),
        SsTableFormat::default(),
        root.clone(),
        None,
        TableStoreKind::Main,
        BlockCachePolicy::default(),
    ));
    let _control_handle = write_rows(&control_tables, control_id, b"control").await;

    let manifest_store = Arc::new(ManifestStore::new(&root, Arc::clone(&raw_store)));
    let first_renewed_expiration = wait_for_checkpoint_expiration_after(
        &manifest_store,
        &clock,
        old_checkpoint_id,
        old_checkpoint_initial_expiration,
    )
    .await;
    let second_renewed_expiration = wait_for_checkpoint_expiration_after(
        &manifest_store,
        &clock,
        old_checkpoint_id,
        first_renewed_expiration,
    )
    .await;
    let past_first_renewal =
        first_renewed_expiration + chrono::Duration::milliseconds(10) - clock.now();
    if let Ok(wait) = past_first_renewal.to_std() {
        tokio::time::sleep(wait).await;
    }
    assert!(clock.now() > old_checkpoint_initial_expiration);
    assert!(clock.now() > first_renewed_expiration);
    assert!(second_renewed_expiration > first_renewed_expiration);

    let compactions_store = Arc::new(CompactionsStore::new(&root, Arc::clone(&raw_store)));
    let gc_tables = Arc::new(TableStore::new(
        Arc::clone(&raw_store),
        SsTableFormat::default(),
        root.clone(),
        None,
        TableStoreKind::GC,
        BlockCachePolicy::default(),
    ));
    let wal_store = Arc::new(WalTableStore::new(
        Arc::clone(&raw_store),
        SsTableFormat::default(),
        root.clone(),
        TableStoreKind::GC,
    ));
    let gc = GarbageCollector::new(
        Arc::clone(&manifest_store),
        compactions_store,
        Arc::clone(&gc_tables),
        wal_store,
        Arc::clone(&raw_store),
        GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            wal_fence_options: None,
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: None,
                min_age: Duration::from_millis(100),
                dry_run: false,
            }),
            compactions_options: None,
            detach_options: None,
            metric_level: None,
            boundary_files_enabled: true,
            object_store_max_retries: Some(0),
        },
        &MetricsRecorderHelper::noop(),
        clock.clone(),
        None,
        None,
    );
    gc.run_gc_once().await;

    let stored_manifest = StoredManifest::load(Arc::clone(&manifest_store), clock.clone())
        .await
        .unwrap();
    assert!(stored_manifest
        .manifest()
        .core
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.id == old_checkpoint_id));
    assert!(stored_manifest
        .manifest()
        .core
        .checkpoints
        .iter()
        .all(|checkpoint| checkpoint
            .expire_time
            .is_none_or(|expiry| expiry > clock.now())));
    for checkpoint in &stored_manifest.manifest().core.checkpoints {
        let checkpoint_manifest = manifest_store
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();
        if !old_sst_ids.is_disjoint(&manifest_sst_ids(&checkpoint_manifest)) {
            assert_eq!(checkpoint.id, old_checkpoint_id);
        }
    }
    let retained_ids = gc_tables
        .list_compacted_ssts(..)
        .await
        .unwrap()
        .into_iter()
        .map(|metadata| metadata.id)
        .collect::<HashSet<_>>();
    assert!(!retained_ids.contains(&control_id));
    assert!(old_sst_ids.is_subset(&retained_ids));

    reader_recording.clear();
    let mut old_rows = vec![(first.key, first.value)];
    old_rows.extend(collect_rows(&mut old_scan).await);
    assert_eq!(old_rows, expected_rows(b"old-value"));
    assert!(reader_recording
        .get_kinds(false)
        .into_iter()
        .any(|kind| kind == Some(TableStoreKind::Reader)));
    let mut fresh_scan = reader.scan_with_options(.., &scan_options).await.unwrap();
    assert_eq!(
        collect_rows(&mut fresh_scan).await,
        expected_rows(b"new-value")
    );

    drop(old_scan);
    drop(fresh_scan);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let stored_manifest = StoredManifest::load(Arc::clone(&manifest_store), clock.clone())
                .await
                .unwrap();
            if stored_manifest
                .manifest()
                .core
                .find_checkpoint(old_checkpoint_id)
                .is_none()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("released checkpoint was not retired before the timeout");
    gc.run_gc_once().await;
    let reclaimed_ids = gc_tables
        .list_compacted_ssts(..)
        .await
        .unwrap()
        .into_iter()
        .map(|metadata| metadata.id)
        .collect::<HashSet<_>>();
    assert!(old_sst_ids.is_disjoint(&reclaimed_ids));
    let mut fresh_scan = reader.scan_with_options(.., &scan_options).await.unwrap();
    assert_eq!(
        collect_rows(&mut fresh_scan).await,
        expected_rows(b"new-value")
    );
    drop(fresh_scan);
    reader.close().await.unwrap();
    db.close().await.unwrap();
}
