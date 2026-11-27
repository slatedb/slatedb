#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use slatedb::admin::Admin;
use slatedb::config::{
    CheckpointOptions, CheckpointScope, CompactionWorkerOptions, CompactorOptions, DbReaderOptions,
    DurabilityLevel, FlushOptions, FlushType, ReadOptions, ScanOptions, Settings,
    SizeTieredCompactionSchedulerOptions,
};
use slatedb::db_stats::REQUEST_COUNT;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::row_cache::{
    BatchStep, GetOutcome, Host, KeyRange, OpenContext, RowCache, ScanIterator, ScanOutcome,
    SyncStep, ViewScanOptions,
};
use slatedb::{
    Db, DbIterator, DbReader, DbReaderMode, DbStatus, Error, ErrorKind, IsolationLevel,
    IterationOrder, KeyValue, MergeOperator, MergeOperatorError, RowEntry, ValueDeletable,
    WriteBatch,
};
use slatedb_common::metrics::{lookup_metric_with_labels, DefaultMetricsRecorder};
use tokio::sync::watch;

#[derive(Default)]
struct Events {
    opened: usize,
    applied_batches: Vec<(u64, Vec<RowEntry>)>,
    misses: Vec<Bytes>,
    invalidations: Vec<(u64, u64)>,
}

struct TestRowCache {
    host: OnceLock<Host>,
    events: Mutex<Events>,
    versions: Mutex<BTreeMap<Bytes, Vec<RowEntry>>>,
    serve_reads: AtomicBool,
    reject_open: AtomicBool,
    fail_next_get: AtomicBool,
    /// When set, only keys under this prefix are served; everything else misses.
    covered_prefix: Option<&'static [u8]>,
}

impl TestRowCache {
    fn new() -> Arc<Self> {
        Self::with_prefix(None)
    }

    /// A cache that serves only `prefix`.
    fn covering(prefix: &'static [u8]) -> Arc<Self> {
        Self::with_prefix(Some(prefix))
    }

    fn with_prefix(covered_prefix: Option<&'static [u8]>) -> Arc<Self> {
        Arc::new(Self {
            host: OnceLock::new(),
            events: Mutex::new(Events::default()),
            versions: Mutex::new(BTreeMap::new()),
            serve_reads: AtomicBool::new(true),
            reject_open: AtomicBool::new(false),
            fail_next_get: AtomicBool::new(false),
            covered_prefix,
        })
    }

    fn covers(&self, range: KeyRange<'_>) -> bool {
        let Some(prefix) = self.covered_prefix else {
            return true;
        };
        let mut prefix_end = prefix.to_vec();
        *prefix_end.last_mut().unwrap() += 1;
        range.start_key().is_some_and(|s| s.starts_with(prefix))
            && range.end_key().is_some_and(|e| e <= &prefix_end[..])
    }

    fn misses(&self) -> Vec<Bytes> {
        self.events.lock().unwrap().misses.clone()
    }

    fn host(&self) -> &Host {
        self.host.get().expect("row cache was opened")
    }

    fn lookup_at(&self, key: &[u8], seq: u64) -> Option<Option<KeyValue>> {
        let versions = self.versions.lock().unwrap();
        let history = versions.get(key)?;
        for entry in history.iter().rev() {
            if entry.seq <= seq {
                return match &entry.value {
                    ValueDeletable::Value(val) => Some(Some(KeyValue::new(
                        entry.key.clone(),
                        val.clone(),
                        entry.seq,
                        entry.create_ts.unwrap_or(0),
                        entry.expire_ts,
                    ))),
                    ValueDeletable::Tombstone => Some(None),
                    ValueDeletable::Merge(_) => None,
                };
            }
        }
        Some(None)
    }
}

struct VecScanIterator {
    rows: Vec<KeyValue>,
    pos: usize,
    async_at: Option<usize>,
    fail_at: Option<usize>,
    async_calls: Arc<AtomicU64>,
}

impl ScanIterator for VecScanIterator {
    fn next_sync(&mut self) -> SyncStep {
        if self.fail_at == Some(self.pos) {
            self.fail_at = None;
            return SyncStep::Error(Error::data("injected scan failure".to_string()));
        }
        if self.async_at == Some(self.pos) {
            return SyncStep::NeedsAsync;
        }
        if self.pos >= self.rows.len() {
            return SyncStep::Done;
        }
        let kv = self.rows[self.pos].clone();
        self.pos += 1;
        SyncStep::Row(kv)
    }

    async fn next(&mut self) -> Result<Option<KeyValue>, Error> {
        self.async_calls.fetch_add(1, Ordering::Relaxed);
        if self.async_at == Some(self.pos) {
            self.async_at = None;
        }
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let kv = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(kv))
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), Error> {
        while self.pos < self.rows.len() && self.rows[self.pos].key.as_ref() < next_key {
            self.pos += 1;
        }
        Ok(())
    }
}

impl RowCache for TestRowCache {
    fn open(&self, ctx: OpenContext) -> Result<(), Error> {
        assert!(!format!("{ctx:?}").is_empty());
        self.events.lock().unwrap().opened += 1;
        let _ = self.host.set(ctx.host);
        if self.reject_open.load(Ordering::Relaxed) {
            return Err(Error::invalid("row cache rejected open".to_string()));
        }
        Ok(())
    }

    fn write(&self, seq: u64, entries: &[RowEntry]) {
        assert!(
            self.host().last_committed_seq() < seq,
            "write must run before committed_seq advances"
        );
        if entries.is_empty() {
            return;
        }
        assert!(entries.iter().all(|e| e.seq == seq), "one seq per batch");
        assert!(
            entries.windows(2).all(|w| w[0].key < w[1].key),
            "a batch comes in key order"
        );
        let mut versions = self.versions.lock().unwrap();
        for entry in entries {
            versions
                .entry(entry.key.clone())
                .or_default()
                .push(entry.clone());
        }
        self.events
            .lock()
            .unwrap()
            .applied_batches
            .push((seq, entries.to_vec()));
    }

    fn invalidate(&self, valid_through: u64, new_seq: u64) {
        assert!(new_seq > valid_through);
        assert_eq!(
            self.host().last_committed_seq(),
            valid_through,
            "invalidation runs before the reader's seq advances"
        );
        self.versions.lock().unwrap().clear();
        self.events
            .lock()
            .unwrap()
            .invalidations
            .push((valid_through, new_seq));
    }

    fn get(&self, key: &[u8], options: &ReadOptions, at: Option<u64>) -> GetOutcome {
        if self.fail_next_get.swap(false, Ordering::Relaxed) {
            return GetOutcome::Error(Error::invalid("injected get error".to_string()));
        }
        let key_range = KeyRange {
            start: Bound::Included(key),
            end: Bound::Included(key),
        };
        if !self.covers(key_range)
            || !self.serve_reads.load(Ordering::Relaxed)
            || !self.host().allows_read(options)
        {
            self.events
                .lock()
                .unwrap()
                .misses
                .push(Bytes::copy_from_slice(key));
            return GetOutcome::Miss;
        }
        let seq = self.host().read_seq(at);
        match self.lookup_at(key, seq) {
            Some(kv) => {
                self.host().record_get_requests(1);
                GetOutcome::Hit(kv)
            }
            None => {
                self.events
                    .lock()
                    .unwrap()
                    .misses
                    .push(Bytes::copy_from_slice(key));
                GetOutcome::Miss
            }
        }
    }

    fn scan(
        &self,
        range: KeyRange<'_>,
        options: &ScanOptions,
        _prefix: Option<&[u8]>,
        at: Option<u64>,
    ) -> ScanOutcome {
        if !self.covers(range)
            || !self.serve_reads.load(Ordering::Relaxed)
            || !self.host().allows_scan(options)
        {
            let start = range.start_key().unwrap_or_default();
            self.events
                .lock()
                .unwrap()
                .misses
                .push(Bytes::copy_from_slice(start));
            return ScanOutcome::Miss;
        }
        let seq = self.host().read_seq(at);
        let versions = self.versions.lock().unwrap();
        let mut rows = Vec::new();
        for (_key, history) in versions.range::<[u8], _>(range) {
            for entry in history.iter().rev() {
                if entry.seq <= seq {
                    if let ValueDeletable::Value(val) = &entry.value {
                        rows.push(KeyValue::new(
                            entry.key.clone(),
                            val.clone(),
                            entry.seq,
                            entry.create_ts.unwrap_or(0),
                            entry.expire_ts,
                        ));
                    }
                    break;
                }
            }
        }
        self.host().record_scan_requests(1);
        ScanOutcome::hit(VecScanIterator {
            rows,
            pos: 0,
            async_at: None,
            fail_at: None,
            async_calls: Arc::new(AtomicU64::new(0)),
        })
    }
}

struct ConcatMergeOperator;

impl MergeOperator for ConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut merged = existing_value.map(|v| v.to_vec()).unwrap_or_default();
        merged.extend_from_slice(&operand);
        Ok(Bytes::from(merged))
    }
}

#[tokio::test]
async fn test_row_cache_lifecycle_writes_reads_and_host_apis() {
    let cache = TestRowCache::new();
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = Db::builder("/tmp/test_row_cache_lifecycle", Arc::new(InMemory::new()))
        .with_metrics_recorder(recorder.clone())
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();

    assert_eq!(cache.events.lock().unwrap().opened, 1);
    assert!(!cache.host().is_reader());
    assert!(cache.host().allows_read(&ReadOptions::default()));
    assert!(!cache.host().allows_read(&ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..ReadOptions::default()
    }));
    assert!(!cache.host().allows_read(&ReadOptions {
        dirty: true,
        ..ReadOptions::default()
    }));
    assert!(cache.host().allows_scan(&ScanOptions::default()));
    assert!(!cache.host().allows_scan(&ScanOptions {
        order: IterationOrder::Descending,
        ..ScanOptions::default()
    }));

    // Writes pass through write() strictly before committed_seq advances.
    db.put(b"a", b"1").await.unwrap();
    db.put(b"b", b"2").await.unwrap();
    db.merge(b"m", b"part1").await.unwrap();
    assert_eq!(cache.events.lock().unwrap().applied_batches.len(), 3);

    // run_on_writer executes on the batch writer at the current committed sequence number.
    let writer_seq = cache.host().run_on_writer(|seq| seq).await.unwrap();
    assert_eq!(writer_seq, cache.host().last_committed_seq());

    // Point reads hit the cache and update SlateDB's request counter.
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(
        lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "get")]),
        Some(1)
    );

    // Errors returned from the cache preserve ErrorKind.
    cache.fail_next_get.store(true, Ordering::Relaxed);
    let err = db.get(b"a").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Invalid);

    // Snapshots read at their started_seq.
    let snap = db.snapshot().unwrap();
    let snap_seq = snap.seq();
    db.put(b"a", b"2").await.unwrap();
    assert_eq!(
        snap.get(b"a").await.unwrap(),
        Some(Bytes::from_static(b"1"))
    );
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"2")));

    // Descending scans bypass the row cache automatically.
    let desc_opts = ScanOptions {
        order: IterationOrder::Descending,
        ..ScanOptions::default()
    };
    let desc_rows = collect(db.scan_with_options(b"a"..b"c", &desc_opts).await.unwrap()).await;
    assert_eq!(
        desc_rows,
        vec![
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"2")),
        ]
    );
    let snap_desc = collect(
        snap.scan_with_options(b"a"..b"c", &desc_opts)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        snap_desc,
        vec![
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
        ]
    );

    // Transactions see uncommitted puts/deletes and fall through on uncommitted merges.
    let txn = db.begin(IsolationLevel::Snapshot).unwrap();
    txn.put(b"a", b"txn_val").unwrap();
    txn.delete(b"b").unwrap();
    txn.merge(b"m", b"+part2").unwrap();
    assert_eq!(
        txn.get(b"a").await.unwrap(),
        Some(Bytes::from_static(b"txn_val"))
    );
    assert_eq!(txn.get(b"b").await.unwrap(), None);
    assert_eq!(
        txn.get(b"m").await.unwrap(),
        Some(Bytes::from_static(b"part1+part2"))
    );
    // Overlapping scan bypasses cache to see txn writes.
    let mut txn_scan = txn.scan(b"a"..b"c").await.unwrap();
    let mut txn_rows = Vec::new();
    txn_scan.next_batch(&mut txn_rows, 10).await.unwrap();
    assert_eq!(txn_rows.len(), 1);
    assert_eq!(txn_rows[0].key, Bytes::from_static(b"a"));
    assert_eq!(txn_rows[0].value, Bytes::from_static(b"txn_val"));
    txn.rollback();

    // Host::capture_state_view, is_view_current, retains_seq, scan_view, and next_entry.
    let view_before_flush = cache.host().capture_state_view().unwrap();
    assert!(cache.host().is_view_current(&view_before_flush));
    assert!(cache.host().retains_seq(&view_before_flush, snap_seq));
    assert!(format!("{:?}", cache.host()).contains("writer"));
    assert!(format!("{view_before_flush:?}").contains("writer"));

    // scan_view bypasses the row cache and preserves ValueDeletable::Merge on unbased merges.
    let mut raw = cache
        .host()
        .scan_view(
            &view_before_flush,
            KeyRange {
                start: Bound::Included(b"m"),
                end: Bound::Excluded(b"n"),
            },
            &ViewScanOptions::default(),
        )
        .await
        .unwrap();
    assert!(format!("{raw:?}").contains("ViewIterator"));
    let entry = raw.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.key, Bytes::from_static(b"m"));
    assert_eq!(
        entry.value,
        ValueDeletable::Merge(Bytes::from_static(b"part1"))
    );

    // Flushing freezes the active memtable and creates an L0 SST whose index Host can inspect.
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    assert!(!cache.host().is_view_current(&view_before_flush));
    let view_after_flush = cache.host().capture_state_view().unwrap();
    assert!(cache.host().retains_seq(&view_after_flush, snap_seq));
    drop(snap);

    // Host::snapshot opens a retention snapshot at the committed sequence number.
    let host_snap = cache.host().snapshot().await.unwrap();
    assert_eq!(host_snap.seq(), cache.host().last_committed_seq());
    assert!(format!("{host_snap:?}").contains(&host_snap.seq().to_string()));
    db.put(b"hs", b"1").await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    let view_after_snap = cache.host().capture_state_view().unwrap();
    assert!(cache.host().retains_seq(&view_after_snap, host_snap.seq()));
    drop(host_snap);
    assert!(!cache
        .host()
        .retains_seq(&cache.host().capture_state_view().unwrap(), snap_seq));

    let manifest = cache.host().manifest().unwrap();
    let l0 = manifest.l0().front().expect("flushed L0 SST");
    let sst_index = cache.host().read_sst_index(&l0.sst).await.unwrap();
    assert!(!sst_index.is_empty());

    // Misses are observed directly inside get and scan.
    cache.serve_reads.store(false, Ordering::Relaxed);
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"2")));
    let _ = db.scan(b"b"..b"z").await.unwrap();
    assert_eq!(
        cache.misses(),
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );

    // Closing the database stops the writer task and fails subsequent run_on_writer calls.
    db.close().await.unwrap();
    assert!(cache.host().check_closed().is_err());
    assert!(cache.host().run_on_writer(|seq| seq).await.is_err());

    // Dropping Db breaks the weak reference in Host (no Arc cycle).
    drop(db);
    assert!(cache.host().capture_state_view().is_err());
}

#[tokio::test]
async fn test_row_cache_open_rejection_aborts_build() {
    let cache = TestRowCache::new();
    cache.reject_open.store(true, Ordering::Relaxed);
    let err = Db::builder("/tmp/test_row_cache_reject_open", Arc::new(InMemory::new()))
        .with_row_cache(cache.clone())
        .build()
        .await
        .err()
        .expect("open rejection must fail build");
    assert_eq!(err.kind(), ErrorKind::Invalid);
    assert_eq!(cache.events.lock().unwrap().opened, 1);
    assert!(cache.host().run_on_writer(|seq| seq).await.is_err());
}

#[tokio::test]
async fn test_row_cache_minimal_defaults() {
    struct MinimalPointCache {
        host: OnceLock<Host>,
        rows: Mutex<BTreeMap<Bytes, (u64, Option<KeyValue>)>>,
        hits: AtomicU64,
    }

    impl RowCache for MinimalPointCache {
        fn open(&self, ctx: OpenContext) -> Result<(), Error> {
            let _ = self.host.set(ctx.host);
            Ok(())
        }

        fn write(&self, seq: u64, entries: &[RowEntry]) {
            let mut rows = self.rows.lock().unwrap();
            for e in entries {
                match &e.value {
                    ValueDeletable::Value(val) => {
                        let kv = KeyValue::new(
                            e.key.clone(),
                            val.clone(),
                            seq,
                            e.create_ts.unwrap_or(0),
                            e.expire_ts,
                        );
                        rows.insert(e.key.clone(), (seq, Some(kv)));
                    }
                    ValueDeletable::Tombstone => {
                        rows.insert(e.key.clone(), (seq, None));
                    }
                    ValueDeletable::Merge(_) => {
                        rows.remove(&e.key);
                    }
                }
            }
        }

        fn get(&self, key: &[u8], options: &ReadOptions, at: Option<u64>) -> GetOutcome {
            let Some(host) = self.host.get() else {
                return GetOutcome::Miss;
            };
            if !host.allows_read(options) {
                return GetOutcome::Miss;
            }
            let seq = host.read_seq(at);
            let rows = self.rows.lock().unwrap();
            match rows.get(key) {
                Some((entry_seq, kv)) if *entry_seq <= seq => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    host.record_get_requests(1);
                    GetOutcome::Hit(kv.clone())
                }
                _ => GetOutcome::Miss,
            }
        }
    }

    let cache = Arc::new(MinimalPointCache {
        host: OnceLock::new(),
        rows: Mutex::default(),
        hits: AtomicU64::new(0),
    });
    let db = Db::builder("/tmp/test_row_cache_minimal", Arc::new(InMemory::new()))
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();

    db.put(b"a", b"1").await.unwrap();
    db.put(b"b", b"2").await.unwrap();
    db.delete(b"b").await.unwrap();
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(db.get(b"b").await.unwrap(), None);
    assert_eq!(cache.hits.load(Ordering::Relaxed), 2);

    // Default RowCache::scan misses and falls back to SlateDB's LSM scan.
    let scanned = collect(db.scan(b"a"..b"z").await.unwrap()).await;
    assert_eq!(
        scanned,
        vec![(Bytes::from_static(b"a"), Bytes::from_static(b"1"))]
    );
    db.close().await.unwrap();

    let range = KeyRange {
        start: Bound::Included(b"b"),
        end: Bound::Excluded(b"d"),
    };
    assert!(!range.contains(b"a") && range.contains(b"b") && !range.contains(b"d"));
    assert_eq!(
        (range.start_key(), range.end_key()),
        (Some(&b"b"[..]), Some(&b"d"[..]))
    );
    assert!(matches!(BatchStep::Done(0), BatchStep::Done(0)));
}

#[tokio::test]
async fn test_row_cache_scan_next_batch_async_gap_seek_and_invalidation() {
    struct CustomBatchCache {
        async_calls: Arc<AtomicU64>,
        fail_at: Option<usize>,
    }

    impl RowCache for CustomBatchCache {
        fn write(&self, _seq: u64, _entries: &[RowEntry]) {}

        fn get(&self, _key: &[u8], _options: &ReadOptions, _at: Option<u64>) -> GetOutcome {
            GetOutcome::Miss
        }

        fn scan(
            &self,
            _range: KeyRange<'_>,
            _options: &ScanOptions,
            _prefix: Option<&[u8]>,
            _at: Option<u64>,
        ) -> ScanOutcome {
            let rows = (0..5u8)
                .map(|i| {
                    KeyValue::new(
                        Bytes::from(vec![b'k', b'0' + i]),
                        Bytes::from(vec![b'v', b'0' + i]),
                        i as u64 + 1,
                        0,
                        None,
                    )
                })
                .collect();
            ScanOutcome::hit(VecScanIterator {
                rows,
                pos: 0,
                async_at: Some(2),
                fail_at: self.fail_at,
                async_calls: self.async_calls.clone(),
            })
        }
    }

    // Mixed sync + NeedsAsync + seek through next_batch.
    let async_calls = Arc::new(AtomicU64::new(0));
    let db = Db::builder("/tmp/test_row_cache_batch_scan", Arc::new(InMemory::new()))
        .with_row_cache(Arc::new(CustomBatchCache {
            async_calls: async_calls.clone(),
            fail_at: None,
        }))
        .build()
        .await
        .unwrap();

    let mut iter = db.scan(..).await.unwrap();
    let mut out = Vec::new();
    let n = iter.next_batch(&mut out, 3).await.unwrap();
    assert_eq!(n, 3);
    assert_eq!(async_calls.load(Ordering::Relaxed), 1);
    assert_eq!(out[2].key, Bytes::from_static(b"k2"));

    iter.seek(b"k4").await.unwrap();
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key, Bytes::from_static(b"k4"));
    assert!(iter.next().await.unwrap().is_none());
    db.close().await.unwrap();

    // Error midway through next_batch preserves preceding rows and invalidates the iterator.
    let db = Db::builder("/tmp/test_row_cache_batch_err", Arc::new(InMemory::new()))
        .with_row_cache(Arc::new(CustomBatchCache {
            async_calls: Arc::new(AtomicU64::new(0)),
            fail_at: Some(2),
        }))
        .build()
        .await
        .unwrap();

    let mut iter = db.scan(..).await.unwrap();
    let mut out = Vec::new();
    let err = iter.next_batch(&mut out, 5).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Data);
    assert_eq!(out.len(), 2);
    let second = iter.next().await.unwrap_err();
    assert_eq!(second.kind(), ErrorKind::Data);
    db.close().await.unwrap();

    // Default ScanIterator::next returns an internal error if NeedsAsync is not overridden.
    struct SyncOnlyBrokenIter;

    impl ScanIterator for SyncOnlyBrokenIter {
        fn next_sync(&mut self) -> SyncStep {
            SyncStep::NeedsAsync
        }

        async fn seek(&mut self, _next_key: &[u8]) -> Result<(), Error> {
            Ok(())
        }
    }

    let mut broken = SyncOnlyBrokenIter;
    let err = broken.next().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Internal);
}

// DbReader tests. A writer and a reader share one in-memory store; the reader's cache covers
// `hot/`, which the writer writes only after the reader opened, so those keys reach the cache
// through the manifest poller.

const PATH: &str = "reader_row_cache";
const HOT: &[u8] = b"hot/";

fn hot(name: &str) -> Bytes {
    Bytes::from(format!("hot/{name}"))
}

fn reader_options(poll_ms: u64, checkpoint_lifetime: Duration) -> DbReaderOptions {
    DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(poll_ms),
        checkpoint_lifetime,
        ..DbReaderOptions::default()
    }
}

async fn open_writer(store: &Arc<dyn ObjectStore>, compacting: bool) -> Db {
    let compactor_options = compacting.then(|| CompactorOptions {
        poll_interval: Duration::from_millis(5),
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            ..Default::default()
        }
        .into(),
        worker: Some(CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(5),
            ..Default::default()
        }),
        checkpoint_lifetime: Duration::from_millis(1),
        ..CompactorOptions::default()
    });
    let settings = Settings {
        flush_interval: None,
        compactor_options,
        garbage_collector_options: None,
        ..Settings::default()
    };
    Db::builder(PATH, Arc::clone(store))
        .with_settings(settings)
        .build()
        .await
        .unwrap()
}

async fn open_reader(
    store: &Arc<dyn ObjectStore>,
    cache: &Arc<TestRowCache>,
    options: DbReaderOptions,
) -> DbReader {
    DbReader::builder(PATH, Arc::clone(store))
        .with_options(options)
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap()
}

/// Writes each `hot/` pair in its own batch, then makes them durable in the WAL.
async fn put_durable(db: &Db, rows: &[(&str, &str)]) {
    for (key, value) in rows {
        db.put(hot(key), value.as_bytes()).await.unwrap();
    }
    db.flush().await.unwrap();
}

async fn flush_memtable(db: &Db) {
    let options = FlushOptions {
        flush_type: FlushType::MemTable,
    };
    db.flush_with_options(options).await.unwrap();
}

type Status = watch::Receiver<DbStatus>;

async fn wait_for_status(status: &mut Status, f: impl FnMut(&DbStatus) -> bool) {
    tokio::time::timeout(Duration::from_secs(10), status.wait_for(f))
        .await
        .expect("reader status in time")
        .expect("reader open");
}

/// Waits until the reader serves every durable write of `db`, and its L0.
async fn caught_up(status: &mut Status, db: &Db) {
    let target = db.status();
    let (seq, l0_seq) = (target.durable_seq, target.current_manifest.last_l0_seq());
    wait_for_status(status, |s| {
        s.durable_seq >= seq && s.current_manifest.last_l0_seq() >= l0_seq
    })
    .await;
}

async fn eventually<F: Future<Output = bool>>(mut condition: impl FnMut() -> F) {
    for _ in 0..1000 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition did not hold in time");
}

async fn collect(mut iter: DbIterator) -> Vec<(Bytes, Bytes)> {
    let mut rows = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        rows.push((row.key, row.value));
    }
    rows
}

fn rows(pairs: &[(&str, &'static str)]) -> Vec<(Bytes, Bytes)> {
    let row = |&(key, v): &(&str, &'static str)| (hot(key), value(v).unwrap());
    pairs.iter().map(row).collect()
}

async fn checkpoint_ids(admin: &Admin) -> Vec<uuid::Uuid> {
    let checkpoints = admin.list_checkpoints(None).await.unwrap();
    checkpoints.into_iter().map(|cp| cp.id).collect()
}

fn value(v: &'static str) -> Option<Bytes> {
    Some(Bytes::from_static(v.as_bytes()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reader_row_cache_lifecycle_replay_reads_and_host_apis() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store, true).await;
    db.put(b"cold/a", b"a1").await.unwrap();
    db.flush().await.unwrap();
    let cache = TestRowCache::covering(HOT);
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let reader = DbReader::builder(PATH, Arc::clone(&store))
        .with_options(reader_options(20, Duration::from_secs(3600)))
        .with_metrics_recorder(recorder.clone())
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();
    let mut status = reader.subscribe();
    let host = cache.host().clone();
    assert!(host.is_reader());
    assert!(host.allows_read(&ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..ReadOptions::default()
    }));
    assert!(format!("{host:?}").contains("reader"));
    let opened_at = host.last_committed_seq();

    // Replay feeds one key-ordered batch per seq in ascending order, before the seq is visible.
    let mut batch = WriteBatch::new();
    batch.put(hot("c"), b"c1");
    batch.put(hot("a"), b"a1");
    batch.delete(hot("b"));
    db.write(batch).await.unwrap();
    put_durable(&db, &[("y", "y1"), ("x", "x1")]).await;
    caught_up(&mut status, &db).await;
    let batches = cache.events.lock().unwrap().applied_batches.clone();
    assert_eq!(batches.len(), 3);
    assert!(batches[0].0 > opened_at);
    let keys: Vec<Bytes> = batches[0].1.iter().map(|e| e.key.clone()).collect();
    assert_eq!(keys, vec![hot("a"), hot("b"), hot("c")]);
    assert_eq!(batches[0].1[1].value, ValueDeletable::Tombstone);
    assert!(batches.windows(2).all(|w| w[1].0 > w[0].0));
    assert_eq!(host.last_committed_seq(), batches[2].0);

    // An uncovered key misses and falls through to the LSM; covered keys hit.
    let gets = lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "get")]).unwrap();
    assert_eq!(reader.get(b"cold/a").await.unwrap(), value("a1"));
    assert_eq!(cache.misses(), vec![Bytes::from_static(b"cold/a")]);
    assert_eq!(reader.get(hot("x")).await.unwrap(), value("x1"));
    let kv = reader.get_key_value(hot("a")).await.unwrap().unwrap();
    assert_eq!(kv.seq, batches[0].0);
    assert_eq!(reader.get(hot("b")).await.unwrap(), None);
    assert_eq!(
        lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "get")]),
        Some(gets + 4)
    );

    // Covered scans and prefix scans hit; uncovered ones miss.
    let expected = rows(&[("a", "a1"), ("c", "c1"), ("x", "x1"), ("y", "y1")]);
    let scanned = collect(reader.scan(hot("a")..hot("z")).await.unwrap()).await;
    assert_eq!(scanned, expected);
    let scanned = collect(reader.scan_prefix(HOT, ..).await.unwrap()).await;
    assert_eq!(scanned, expected);
    assert_eq!(cache.misses().len(), 1);
    let cold = collect(reader.scan_prefix(b"cold/", ..).await.unwrap()).await;
    assert_eq!(cold.len(), 1);
    assert_eq!(cache.misses().last(), Some(&Bytes::from_static(b"cold/")));

    // run_on_writer executes on the idle poller at the reader's seq.
    let poller_seq = host.run_on_writer(|seq| seq).await.unwrap();
    assert_eq!(poller_seq, host.last_committed_seq());

    // Views: currency, retention, and scans that bypass the cache.
    assert!(host.check_closed().is_ok());
    let x1_seq = host.last_committed_seq();
    let view = host.capture_state_view().unwrap();
    assert!(host.is_view_current(&view));
    assert!(host.retains_seq(&view, x1_seq));
    assert!(format!("{view:?}").contains("reader"));
    put_durable(&db, &[("x", "x2")]).await;
    caught_up(&mut status, &db).await;
    assert!(!host.is_view_current(&view));
    let view = host.capture_state_view().unwrap();
    assert!(host.is_view_current(&view));
    let mut old = host
        .scan_view(
            &view,
            hot("x")..hot("y"),
            &ViewScanOptions {
                max_seq: Some(x1_seq),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let old = old.next().await.unwrap().unwrap();
    assert_eq!(Some(old.value), value("x1"));
    let prefix_opts = ViewScanOptions {
        prefix: Some(Bytes::from_static(HOT)),
        ..Default::default()
    };
    let mut newest = host
        .scan_view(&view, hot("x")..hot("y"), &prefix_opts)
        .await
        .unwrap();
    let entry = newest.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.value, ValueDeletable::Value(value("x2").unwrap()));
    let empty_range = host
        .scan_view(&view, hot("b")..hot("a"), &ViewScanOptions::default())
        .await;
    assert!(empty_range.is_err());

    let snapshot = host.snapshot().await.unwrap();
    assert_eq!(snapshot.seq(), host.last_committed_seq());
    assert_eq!(snapshot.seq(), reader.status().durable_seq);

    // Flushing replayed writes to L0 invalidates nothing; the host reads the flushed SST.
    flush_memtable(&db).await;
    caught_up(&mut status, &db).await;
    let manifest = host.manifest().unwrap();
    let l0 = manifest.l0().front().expect("flushed L0 SST");
    assert!(!host.read_sst_index(&l0.sst).await.unwrap().is_empty());

    // So does compacting them. A view retains only seqs at or above its L0, even with a live
    // snapshot: the writer's compactor does not know the reader's snapshots.
    put_durable(&db, &[("z", "z1")]).await;
    caught_up(&mut status, &db).await;
    flush_memtable(&db).await;
    wait_for_status(&mut status, |s| {
        s.current_manifest.l0().is_empty() && !s.current_manifest.compacted().is_empty()
    })
    .await;
    let view = host.capture_state_view().unwrap();
    assert!(host.retains_seq(&view, host.manifest().unwrap().last_l0_seq()));
    assert!(!host.retains_seq(&view, x1_seq));
    assert!(cache.events.lock().unwrap().invalidations.is_empty());
    assert_eq!(cache.events.lock().unwrap().applied_batches.len(), 5);
    assert_eq!(reader.get(hot("x")).await.unwrap(), value("x2"));
    assert_eq!(reader.get(hot("z")).await.unwrap(), value("z1"));
    assert_eq!(cache.misses().len(), 2);
    drop(snapshot);

    // Closing the reader stops the poller and fails subsequent run_on_writer and snapshot calls.
    reader.close().await.unwrap();
    assert!(host.check_closed().is_err());
    assert!(host.run_on_writer(|seq| seq).await.is_err());
    assert!(host.snapshot().await.is_err());
    // Dropping the reader breaks the weak reference in Host.
    drop(reader);
    assert!(host.capture_state_view().is_err());
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_reader_row_cache_invalidation_on_l0_jump_and_skip_wal() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store, false).await;

    // A WAL-enabled reader whose next poll finds an L0 holding writes it never replayed drops
    // its rows above its seq, then receives the WAL writes above the L0.
    let cache = TestRowCache::covering(HOT);
    let options = reader_options(150, Duration::from_secs(10));
    let reader = open_reader(&store, &cache, options).await;
    let mut status = reader.subscribe();
    let host = cache.host().clone();
    put_durable(&db, &[("x", "x1")]).await;
    caught_up(&mut status, &db).await;
    let before = host.last_committed_seq();
    put_durable(&db, &[("x", "x2")]).await;
    flush_memtable(&db).await;
    put_durable(&db, &[("z", "z1")]).await;
    caught_up(&mut status, &db).await;
    let last_l0_seq = host.manifest().unwrap().last_l0_seq();
    assert!(last_l0_seq > before);
    let (invalidations, last) = {
        let events = cache.events.lock().unwrap();
        let last = events.applied_batches.last().cloned().unwrap();
        (events.invalidations.clone(), last)
    };
    assert_eq!(invalidations, vec![(before, last_l0_seq)]);
    assert!(last.0 > last_l0_seq);
    assert_eq!(&last.1[0].key, &hot("z"));
    // x2 comes from the L0 (a miss), z1 from the cache.
    assert_eq!(reader.get(hot("x")).await.unwrap(), value("x2"));
    assert_eq!(reader.get(hot("z")).await.unwrap(), value("z1"));
    assert_eq!(cache.misses(), vec![hot("x")]);
    reader.close().await.unwrap();

    // A reader that skips WAL replay feeds nothing, and invalidates at each L0 flush.
    let cache = TestRowCache::covering(HOT);
    let options = DbReaderOptions {
        skip_wal_replay: true,
        ..reader_options(20, Duration::from_secs(10))
    };
    let reader = open_reader(&store, &cache, options).await;
    let mut status = reader.subscribe();
    let host = cache.host().clone();
    let mut previous = reader.get(hot("y")).await.unwrap();
    for (round, v) in ["y1", "y2"].into_iter().enumerate() {
        let before = host.last_committed_seq();
        put_durable(&db, &[("y", v)]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.events.lock().unwrap().invalidations.len(), round);
        assert_eq!(reader.get(hot("y")).await.unwrap(), previous);
        flush_memtable(&db).await;
        wait_for_status(&mut status, |s| s.durable_seq > before).await;
        let invalidations = cache.events.lock().unwrap().invalidations.clone();
        assert_eq!(
            invalidations.last(),
            Some(&(before, host.last_committed_seq()))
        );
        assert_eq!(invalidations.len(), round + 1);
        previous = reader.get(hot("y")).await.unwrap();
        assert_eq!(previous, value(v));
    }
    assert!(cache.events.lock().unwrap().applied_batches.is_empty());
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_reader_row_cache_host_snapshot_pins_checkpoint() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store, false).await;
    let admin = Admin::builder(PATH, Arc::clone(&store)).build();
    let cache = TestRowCache::covering(HOT);
    let lifetime = Duration::from_secs(2);
    let reader = open_reader(&store, &cache, reader_options(20, lifetime)).await;
    let mut status = reader.subscribe();
    let host = cache.host().clone();
    let initial = checkpoint_ids(&admin).await;
    assert_eq!(initial.len(), 1);

    // Without a pin, the replacement swaps the checkpoint in place.
    put_durable(&db, &[("x", "x1")]).await;
    flush_memtable(&db).await;
    caught_up(&mut status, &db).await;
    let unpinned = checkpoint_ids(&admin).await;
    assert_eq!(unpinned.len(), 1);
    assert_ne!(unpinned, initial);
    let pinned_id = unpinned[0];

    // A snapshot pins its checkpoint through a replacement.
    let snapshot = host.snapshot().await.unwrap();
    let seq = snapshot.seq();
    let old_view = host.capture_state_view().unwrap();
    put_durable(&db, &[("x", "x2")]).await;
    flush_memtable(&db).await;
    caught_up(&mut status, &db).await;
    let kept = checkpoint_ids(&admin).await;
    assert_eq!(kept.len(), 2);
    assert!(kept.contains(&pinned_id));
    assert!(host.last_committed_seq() > seq);

    // The poller refreshes the kept checkpoint past half its lifetime.
    let expiry = || async {
        let checkpoints = admin.list_checkpoints(None).await.unwrap();
        let kept = checkpoints.iter().find(|cp| cp.id == pinned_id);
        kept.and_then(|cp| cp.expire_time).expect("kept checkpoint")
    };
    let first_expiry = expiry().await;
    tokio::time::sleep(lifetime / 2 + Duration::from_millis(200)).await;
    eventually(|| async { expiry().await > first_expiry }).await;

    // The old view still reads the pinned state at the snapshot's seq.
    let mut old = host
        .scan_view(
            &old_view,
            hot("a")..hot("z"),
            &ViewScanOptions {
                max_seq: Some(seq),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        old.next().await.unwrap().unwrap().value,
        value("x1").unwrap()
    );
    drop((old, old_view));

    // Once unpinned, the next poll deletes the kept checkpoint.
    drop(snapshot);
    eventually(|| async {
        let ids = checkpoint_ids(&admin).await;
        ids.len() == 1 && !ids.contains(&pinned_id)
    })
    .await;

    // Close deletes the reader's checkpoint and any it kept for a pin.
    let snapshot = host.snapshot().await.unwrap();
    put_durable(&db, &[("x", "x3")]).await;
    flush_memtable(&db).await;
    caught_up(&mut status, &db).await;
    assert_eq!(checkpoint_ids(&admin).await.len(), 2);
    reader.close().await.unwrap();
    assert!(checkpoint_ids(&admin).await.is_empty());
    drop(snapshot);
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_reader_row_cache_pinned_mode_and_open_rejection() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store, false).await;
    let admin = Admin::builder(PATH, Arc::clone(&store)).build();
    put_durable(&db, &[("x", "x1")]).await;
    flush_memtable(&db).await;
    let checkpoint = db
        .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
        .await
        .unwrap();

    // A pinned reader runs a poller for its cache: it runs run_on_writer callbacks but never polls.
    let cache = TestRowCache::covering(HOT);
    let reader = DbReader::builder(PATH, Arc::clone(&store))
        .with_reader_mode(DbReaderMode::Checkpoint(checkpoint.id))
        .with_options(reader_options(5, Duration::from_secs(10)))
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();
    let seq = cache.host().last_committed_seq();
    assert_eq!(cache.host().run_on_writer(|s| s).await.unwrap(), seq);
    put_durable(&db, &[("x", "x2")]).await;
    flush_memtable(&db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache.host().last_committed_seq(), seq);
    assert!(cache.events.lock().unwrap().applied_batches.is_empty());
    assert_eq!(reader.get(hot("x")).await.unwrap(), value("x1"));
    assert_eq!(cache.host().snapshot().await.unwrap().seq(), seq);
    reader.close().await.unwrap();
    admin.delete_checkpoint(checkpoint.id).await.unwrap();

    // A cache that rejects open fails the build, stops the poller, and leaves no checkpoint.
    let cache = TestRowCache::covering(HOT);
    cache.reject_open.store(true, Ordering::Relaxed);
    let err = DbReader::builder(PATH, Arc::clone(&store))
        .with_row_cache(cache.clone())
        .build()
        .await
        .err()
        .expect("open rejection must fail build");
    assert_eq!(err.kind(), ErrorKind::Invalid);
    assert_eq!(cache.events.lock().unwrap().opened, 1);
    assert!(cache.host().run_on_writer(|s| s).await.is_err());
    assert!(checkpoint_ids(&admin).await.is_empty());
    db.close().await.unwrap();
}
