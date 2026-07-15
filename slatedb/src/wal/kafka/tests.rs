use super::KafkaWal;
use crate::manifest::store::{FenceableManifest, ManifestStore, StoredManifest};
use crate::manifest::ManifestCore;
use crate::wal::{WalEvent, WalReader, WalStatusListener, WriterInit, WriterManifest};
use crate::RowEntry;
use object_store::memory::InMemory;
use object_store::path::Path;
use parking_lot::Mutex;
use rdkafka::mocking::MockCluster;
use slatedb_common::clock::DefaultSystemClock;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const WRITER_TOPIC: &str = "slatedb-wal-writer-test";
const WRITER_TRANSACTIONAL_ID: &str = "slatedb-wal-writer-test";
const REPLAY_TOPIC: &str = "slatedb-wal-replay-test";
const REPLAY_TRANSACTIONAL_ID: &str = "slatedb-wal-replay-test";
const MANIFEST_TIMEOUT: Duration = Duration::from_secs(30);
// Functional tests flush explicitly. A long interval keeps the periodic task
// from introducing timing-dependent extra transactions into their assertions.
const COMMIT_INTERVAL: Duration = Duration::from_secs(60);
static MOCK_BOOTSTRAP_SERVERS: OnceLock<String> = OnceLock::new();

#[tokio::test]
async fn mock_cluster_writer_commits_batches_and_emits_flushed_event() {
    let wal = KafkaWal::new_with_commit_interval(
        mock_bootstrap_servers(),
        WRITER_TOPIC,
        WRITER_TRANSACTIONAL_ID,
        COMMIT_INTERVAL,
    );
    let manifest_store = new_manifest_store().await;
    let mut manifest = first_writer_manifest(manifest_store).await;
    let mut initialized = wal.fence_and_init(&mut manifest).await.unwrap();
    assert!(initialized.replay_iterator.next().await.unwrap().is_none());

    let events = Arc::new(Mutex::new(Vec::new()));
    let captured_events = events.clone();
    let listener: WalStatusListener = Arc::new(move |event| captured_events.lock().push(event));
    initialized
        .wal_writer
        .observer()
        .subscribe(listener)
        .unwrap();

    let first_batch = vec![
        RowEntry::new_value(b"key-1", b"value-1", 1),
        RowEntry::new_value(b"key-2", b"value-2", 1),
    ];
    let second_batch = vec![RowEntry::new_tombstone(b"key-1", 2)];
    initialized.wal_writer.append(&first_batch).await.unwrap();
    initialized.wal_writer.append(&second_batch).await.unwrap();

    let buffered_status = initialized.wal_writer.status().unwrap();
    assert_eq!(buffered_status.buffered_wal_entries_count, 2);
    assert!(buffered_status.estimated_bytes > 0);

    initialized.wal_writer.flush().await.unwrap().await.unwrap();
    let flushed_status = initialized.wal_writer.status().unwrap();
    assert_eq!(flushed_status.last_flushed_seq, Some(2));
    assert_eq!(flushed_status.buffered_wal_entries_count, 0);
    assert_eq!(flushed_status.estimated_bytes, 0);

    {
        let events = events.lock();
        assert_eq!(events.len(), 1);
        match &events[0] {
            WalEvent::WalFlushed(status) => {
                assert_eq!(
                    status.last_flushed_wal_id,
                    flushed_status.last_flushed_wal_id
                );
                assert_eq!(status.last_flushed_seq, Some(2));
            }
            event => panic!("expected a flushed event, got {event:?}"),
        }
    }

    // Read the committed records back through the Kafka consumer. Offset zero
    // is the fence record created during writer initialization.
    let mut reader = wal
        .iterator((1..flushed_status.last_flushed_wal_id + 1).into())
        .await
        .unwrap();
    let first = reader.next().await.unwrap().unwrap();
    let second = reader.next().await.unwrap().unwrap();
    assert_eq!(first.rows, first_batch);
    assert_eq!(second.rows, second_batch);
    assert!(first.last_in_file);
    assert!(second.last_in_file);
    assert!(reader.next().await.unwrap().is_none());

    initialized.wal_writer.close().await.unwrap();
}

#[tokio::test]
async fn mock_cluster_replays_committed_batches_between_writer_epochs() {
    let wal = KafkaWal::new_with_commit_interval(
        mock_bootstrap_servers(),
        REPLAY_TOPIC,
        REPLAY_TRANSACTIONAL_ID,
        COMMIT_INTERVAL,
    );
    let manifest_store = new_manifest_store().await;

    let mut first_manifest = first_writer_manifest(manifest_store.clone()).await;
    let mut first_writer = wal.fence_and_init(&mut first_manifest).await.unwrap();
    assert!(first_writer.replay_iterator.next().await.unwrap().is_none());

    let first_batch = vec![RowEntry::new_value(b"before-fence-1", b"one", 10)];
    let second_batch = vec![RowEntry::new_value(b"before-fence-2", b"two", 11)];
    first_writer.wal_writer.append(&first_batch).await.unwrap();
    first_writer.wal_writer.append(&second_batch).await.unwrap();
    first_writer
        .wal_writer
        .flush()
        .await
        .unwrap()
        .await
        .unwrap();
    first_writer.wal_writer.close().await.unwrap();

    // A second writer epoch commits another fence record. Its replay iterator
    // is bounded by that record and skips both epochs' fence records.
    let mut second_manifest = next_writer_manifest(manifest_store).await;
    let mut second_writer = wal.fence_and_init(&mut second_manifest).await.unwrap();
    let replayed_first = second_writer.replay_iterator.next().await.unwrap().unwrap();
    let replayed_second = second_writer.replay_iterator.next().await.unwrap().unwrap();
    assert_eq!(replayed_first.rows, first_batch);
    assert_eq!(replayed_second.rows, second_batch);
    assert!(replayed_first.last_wal_file_id < replayed_second.last_wal_file_id);
    assert!(second_writer
        .replay_iterator
        .next()
        .await
        .unwrap()
        .is_none());

    second_writer.wal_writer.close().await.unwrap();
}

fn mock_bootstrap_servers() -> String {
    MOCK_BOOTSTRAP_SERVERS
        .get_or_init(|| {
            let cluster = MockCluster::new(3).unwrap();
            cluster.create_topic(WRITER_TOPIC, 1, 1).unwrap();
            cluster.create_topic(REPLAY_TOPIC, 1, 1).unwrap();
            let bootstrap_servers = cluster.bootstrap_servers();

            // An owned MockCluster is backed by process-global librdkafka state.
            // Keep this test cluster alive until process exit so independently
            // scheduled tests cannot race client shutdown with cluster teardown.
            std::mem::forget(cluster);
            bootstrap_servers
        })
        .clone()
}

async fn new_manifest_store() -> Arc<ManifestStore> {
    let store = Arc::new(ManifestStore::new(
        &Path::from("kafka-functional-test"),
        Arc::new(InMemory::new()),
    ));
    StoredManifest::create_new_db(
        store.clone(),
        ManifestCore::new(),
        Arc::new(DefaultSystemClock::new()),
    )
    .await
    .unwrap();
    store
}

async fn first_writer_manifest(store: Arc<ManifestStore>) -> WriterManifest {
    let stored = StoredManifest::load(store, Arc::new(DefaultSystemClock::new()))
        .await
        .unwrap();
    fence_manifest(stored).await
}

async fn next_writer_manifest(store: Arc<ManifestStore>) -> WriterManifest {
    let stored = StoredManifest::load(store, Arc::new(DefaultSystemClock::new()))
        .await
        .unwrap();
    fence_manifest(stored).await
}

async fn fence_manifest(stored: StoredManifest) -> WriterManifest {
    FenceableManifest::init_writer(
        stored,
        MANIFEST_TIMEOUT,
        Arc::new(DefaultSystemClock::new()),
    )
    .await
    .unwrap()
    .into()
}
