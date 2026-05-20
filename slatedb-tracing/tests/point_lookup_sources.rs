//! Per-source point-lookup tracing tests.
//!
//! Each test arranges the database so a known key lives in exactly one
//! source — active memtable, immutable memtable, an L0 SST, or a
//! sorted run — then performs a traced `get_with_options` and asserts
//! that the `QueryTracingLayer`'s per-query duration counters reflect
//! the work the read path actually had to do.
//!
//! Conventions used by every test in this file:
//! - Each test creates its own `QueryTracingLayer` and `Dispatch`, and
//!   scopes the traced call with `.with_subscriber(dispatch)` so tests
//!   don't share global subscriber state.
//! - SST tests use `min_filter_keys = 1` so a single-entry SST still
//!   gets a bloom filter (otherwise the read-path skips both
//!   `read_filter` and `bloom_filter`).
//! - Background flushing is effectively disabled via a long
//!   `flush_interval`; tests trigger flushes explicitly so memtable vs.
//!   SST placement is deterministic.

use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::bytes::Bytes;
use slatedb::config::{
    CompactorOptions, FlushOptions, FlushType, ReadOptions, Settings,
};
use slatedb::fail_parallel::{self, FailPointRegistry};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use slatedb_tracing::QueryTracingLayer;
use tracing::instrument::WithSubscriber;
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

/// Bundles a `Db`, the `QueryTracingLayer` attached to its subscriber, and
/// the `Dispatch` carrying that subscriber. Traced operations call
/// `traced_get` so each test's spans land on its own dedicated layer.
struct TracedDb {
    db: Db,
    layer: QueryTracingLayer,
    dispatch: tracing::Dispatch,
    _guard: FlushGuard,
}

impl TracedDb {
    async fn open(
        path: &str,
        settings: Settings,
        fp_registry: Option<Arc<FailPointRegistry>>,
    ) -> Self {
        let store = Arc::new(InMemory::new());
        let layer = QueryTracingLayer::new();
        let (chrome_layer, guard) = ChromeLayerBuilder::new()
            .include_args(true)
            // .trace_style(tracing_chrome::TraceStyle::Async)
            .build();
        let subscriber = tracing_subscriber::Registry::default()
            .with(layer.clone())
            .with(chrome_layer);
        let dispatch: tracing::Dispatch = subscriber.into();

        let mut builder = Db::builder(path, store).with_settings(settings);
        if let Some(fp) = fp_registry {
            builder = builder.with_fp_registry(fp);
        }
        let db = builder.build().await.expect("open db");
        Self { db, layer, dispatch, _guard: guard }
    }

    async fn traced_get(&self, key: &[u8], query_id: &str) -> Option<Bytes> {
        let opts = ReadOptions::new().with_query_id(query_id.to_string());
        self.db
            .get_with_options(key, &opts)
            .with_subscriber(self.dispatch.clone())
            .await
            .expect("get")
    }
}

/// Baseline settings used by every test. Keeps the default 100ms
/// `flush_interval` (writes use `await_durable=true`, so disabling the
/// WAL-flush timer would make every put block on a never-arriving WAL
/// flush). Memtable→L0 promotion is driven by `l0_sst_size_bytes` which
/// stays at its 64MiB default — far above anything these tests write, so
/// the background flusher never auto-flushes our memtables. The tests
/// drive that transition explicitly via `flush_with_options`.
///
/// `min_filter_keys = 1` ensures even one-row SSTs carry a bloom filter
/// so SST tests can assert filter/bloom spans fire.
fn base_settings() -> Settings {
    Settings {
        min_filter_keys: 1,
        ..Settings::default()
    }
}

/// Settings that additionally enable the size-tiered compactor with a fast
/// poll and a lowered `min_compaction_sources` so a handful of L0 SSTs is
/// enough to trigger a compaction into a sorted run.
fn settings_with_eager_compactor() -> Settings {
    use std::collections::HashMap;
    let mut scheduler_options = HashMap::new();
    scheduler_options.insert("min_compaction_sources".to_string(), "2".to_string());
    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(50),
        scheduler_options,
        ..CompactorOptions::default()
    };
    Settings {
        compactor_options: Some(compactor_options),
        ..base_settings()
    }
}

/// Poll the manifest until at least one sorted run is recorded or the
/// timeout expires. Returns whether a sorted run was observed.
async fn wait_for_sorted_run(db: &Db, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let _ = db.refresh_manifest().await;
        if !db.manifest().compacted().is_empty() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Asserts that none of the SST-side duration counters fired for
/// `query_id`. Used by the memtable-only tests.
fn assert_no_sst_work(layer: &QueryTracingLayer, query_id: &str) {
    assert_eq!(
        layer.block_read_duration_micros(query_id),
        0,
        "block_read_duration should be 0 for {query_id}"
    );
    assert_eq!(
        layer.index_read_duration_micros(query_id),
        0,
        "index_read_duration should be 0 for {query_id}"
    );
    assert_eq!(
        layer.filter_read_duration_micros(query_id),
        0,
        "filter_read_duration should be 0 for {query_id}"
    );
    assert_eq!(
        layer.bloom_filter_check_duration_micros(query_id),
        0,
        "bloom_filter_check_duration should be 0 for {query_id}"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn result_in_active_memtable() {
    let h = TracedDb::open("/tmp/pl_active", base_settings(), None).await;

    // The put goes straight to the active memtable; with the long
    // flush_interval and no explicit flush, it stays there.
    h.db.put(b"k", b"v").await.expect("put");

    let value = h.traced_get(b"k", "q").await;
    assert_eq!(value.as_deref(), Some(b"v".as_ref()));

    // The active memtable was consulted at least once; the SST path was
    // never entered, so all SST-side timers are zero.
    assert!(
        h.layer.memtable_consulted("q") >= 1,
        "expected memtable_consulted >= 1, got {}",
        h.layer.memtable_consulted("q"),
    );
    assert_no_sst_work(&h.layer, "q");

    println!("{}", h.layer);
    h.db.close().await.expect("close");
}

// TODO(query-tracing): the immutable-memtable case requires a slatedb hook
// that lets the test pause the L0 write while still letting the active
// memtable be frozen.
//
// The existing `flush-memtable-to-l0` fail point sits inside
// `FlushTracker::handle_flush_request`, but L0 upload is also driven from
// the `MemtableFrozen` path fired synchronously by `freeze_current_memtable`
// — so by the time `handle_flush_request` runs (and could be paused), the
// L0 SST has typically already been built. We confirmed this experimentally:
// pausing `flush-memtable-to-l0` still left `manifest().l0().len() == 1`
// before the read, and the read consequently exercised the SST path.
//
// From the layer's perspective the spans for active and immutable
// memtables are identical (`slatedb.query.memtable` with `memtable_consulted`
// incremented), so this test would only meaningfully differ from
// `result_in_active_memtable` if we could *prove* the row was served from
// an immutable memtable rather than the active one. That proof needs a
// fail point on the L0 upload path (e.g. inside `Uploader` or
// `reconcile_and_dispatch`) which doesn't exist today.
#[tokio::test]
#[ignore = "needs a slatedb fail point that halts L0 upload but not freeze; see comment above"]
async fn result_in_immutable_memtable() {
    // Halt the memtable→L0 flush so the active memtable can be frozen
    // (becoming immutable) without being written to L0.
    let fp_registry = Arc::new(FailPointRegistry::new());
    fail_parallel::cfg(
        fp_registry.clone(),
        "flush-memtable-to-l0",
        "pause",
    )
    .expect("cfg pause");

    let h = TracedDb::open(
        "/tmp/pl_immutable",
        base_settings(),
        Some(fp_registry.clone()),
    )
    .await;

    h.db.put(b"k", b"v").await.expect("put");

    let flusher = h.db.clone();
    let flush_task = tokio::spawn(async move {
        flusher
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let value = h.traced_get(b"k", "q").await;
    assert_eq!(value.as_deref(), Some(b"v".as_ref()));

    fail_parallel::cfg(fp_registry, "flush-memtable-to-l0", "off").expect("cfg off");
    let _ = flush_task.await.expect("flush task panicked");

    assert!(
        h.layer.memtable_consulted("q") >= 1,
        "expected memtable_consulted >= 1, got {}",
        h.layer.memtable_consulted("q"),
    );
    assert_no_sst_work(&h.layer, "q");

    h.db.close().await.expect("close");
}

#[tokio::test]
async fn result_in_l0_sst() {
    let h = TracedDb::open("/tmp/pl_l0", base_settings(), None).await;

    h.db.put(b"k", b"v").await.expect("put");
    h.db
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("flush");

    // The active memtable is now empty; the row lives only in an L0 SST.
    // Verify via the manifest that there's exactly one L0 SST and no
    // compacted runs yet.
    let m = h.db.manifest();
    assert_eq!(m.l0().len(), 1, "expected 1 L0 SST, manifest = {m:?}");
    assert!(
        m.compacted().is_empty(),
        "no compactor configured; sorted runs should be empty, manifest = {m:?}",
    );

    let value = h.traced_get(b"k", "q").await;
    assert_eq!(value.as_deref(), Some(b"v".as_ref()));

    // Reading the L0 SST must have loaded its index, filter section, and
    // the data block holding the row, and exercised the bloom filter.
    assert!(
        h.layer.index_read_duration_micros("q") > 0,
        "expected index_read_duration > 0 for L0 read"
    );
    assert!(
        h.layer.filter_read_duration_micros("q") > 0,
        "expected filter_read_duration > 0 for L0 read"
    );
    assert!(
        h.layer.block_read_duration_micros("q") > 0,
        "expected block_read_duration > 0 for L0 read"
    );
    // The bloom check itself can round to 0us; the read_filter span above
    // is the load-time proof. Assert only that work happened cumulatively.
    let total = h.layer.filter_read_duration_micros("q")
        + h.layer.bloom_filter_check_duration_micros("q");
    assert!(
        total > 0,
        "expected non-zero filter+bloom time for L0 read, got {total}"
    );

    println!("{}", h.layer);
    h.db.close().await.expect("close");
}

#[tokio::test]
async fn result_in_sorted_run() {
    let h = TracedDb::open(
        "/tmp/pl_sorted_run",
        settings_with_eager_compactor(),
        None,
    )
    .await;

    // Write several keys across distinct L0 SSTs. With the size-tiered
    // scheduler's `min_compaction_sources = 2` and a 50ms poll interval,
    // the compactor will fold these L0s into a sorted run shortly.
    for (i, key) in [b"k1", b"k2", b"k3", b"k4"].iter().enumerate() {
        h.db.put(*key, format!("v{i}").as_bytes())
            .await
            .expect("put");
        h.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .expect("flush");
    }

    assert!(
        wait_for_sorted_run(&h.db, Duration::from_secs(5)).await,
        "compactor did not produce a sorted run within 5s; manifest = {:?}",
        h.db.manifest()
    );

    let value = h.traced_get(b"k2", "q").await;
    assert_eq!(value.as_deref(), Some(b"v1".as_ref()));

    // The read path opened at least one SST (whether that SST now lives
    // in a sorted run or remains in L0 doesn't change the spans fired —
    // both routes go through the same SST iterator) and consulted the
    // index, filter, and at least one data block.
    assert!(
        h.layer.index_read_duration_micros("q") > 0,
        "expected index_read_duration > 0 for sorted-run read"
    );
    assert!(
        h.layer.filter_read_duration_micros("q") > 0,
        "expected filter_read_duration > 0 for sorted-run read"
    );
    assert!(
        h.layer.block_read_duration_micros("q") > 0,
        "expected block_read_duration > 0 for sorted-run read"
    );

    println!("{}", h.layer);
    h.db.close().await.expect("close");
}

#[tokio::test]
async fn result_in_sorted_run_global_subscriber() {
    let store = Arc::new(InMemory::new());
    let builder = Db::builder("/tmp/pl_immutable", store).with_settings(base_settings());
    let db = builder.build().await.expect("open db");

    let layer = QueryTracingLayer::new();

    tracing_subscriber::registry()
        .with(layer.clone())
        .init();

    // Write several keys across distinct L0 SSTs. With the size-tiered
    // scheduler's `min_compaction_sources = 2` and a 50ms poll interval,
    // the compactor will fold these L0s into a sorted run shortly.
    for (i, key) in [b"k1", b"k2", b"k3", b"k4"].iter().enumerate() {
        db.put(*key, format!("v{i}").as_bytes())
            .await
            .expect("put");
        db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .expect("flush");
    }

    assert!(
        wait_for_sorted_run(&db, Duration::from_secs(5)).await,
        "compactor did not produce a sorted run within 5s; manifest = {:?}",
        db.manifest()
    );

    let opts = ReadOptions::new().with_query_id("id1".to_string());
    let value = db.get_with_options(b"k2", &opts).await.expect("get value");
    assert_eq!(value.as_deref(), Some(b"v1".as_ref()));

    // The read path opened at least one SST (whether that SST now lives
    // in a sorted run or remains in L0 doesn't change the spans fired —
    // both routes go through the same SST iterator) and consulted the
    // index, filter, and at least one data block.
    // assert!(
    //     h.layer.index_read_duration_micros("q") > 0,
    //     "expected index_read_duration > 0 for sorted-run read"
    // );
    // assert!(
    //     h.layer.filter_read_duration_micros("q") > 0,
    //     "expected filter_read_duration > 0 for sorted-run read"
    // );
    // assert!(
    //     h.layer.block_read_duration_micros("q") > 0,
    //     "expected block_read_duration > 0 for sorted-run read"
    // );

    println!("{}", layer);
    db.close().await.expect("close");
}
