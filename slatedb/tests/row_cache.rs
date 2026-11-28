#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use slatedb::admin::Admin;
use slatedb::config::{
    CheckpointOptions, CheckpointScope, CloseOptions, CompactionWorkerOptions, CompactorOptions,
    DbReaderOptions, DurabilityLevel, FlushOptions, FlushType, MergeOptions, PutOptions,
    ReadOptions, ScanOptions, Settings, SizeTieredCompactionSchedulerOptions, Ttl, WriteOptions,
};
use slatedb::db_stats::REQUEST_COUNT;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::row_cache::{GetOutcome, KeyRange, RowCache, ScanIterator, ScanOutcome, ScanSegment};
use slatedb::{
    Db, DbIterator, DbReader, DbReaderMode, DbStatus, Error, ErrorKind, IsolationLevel,
    IterationOrder, KeyValue, MergeOperator, MergeOperatorError, RowEntry, ValueDeletable,
    WriteBatch,
};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_common::metrics::{lookup_metric_with_labels, DefaultMetricsRecorder};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OwnedRange {
    start: Bound<Bytes>,
    end: Bound<Bytes>,
}

impl OwnedRange {
    fn from_key_range(range: KeyRange<'_>) -> Self {
        Self {
            start: range.start.map(Bytes::copy_from_slice),
            end: range.end.map(Bytes::copy_from_slice),
        }
    }

    fn is_all(&self) -> bool {
        matches!(
            (&self.start, &self.end),
            (Bound::Unbounded, Bound::Unbounded)
        )
    }

    fn contains(&self, key: &[u8]) -> bool {
        let start_ok = match &self.start {
            Bound::Included(s) => key >= s.as_ref(),
            Bound::Excluded(s) => key > s.as_ref(),
            Bound::Unbounded => true,
        };
        let end_ok = match &self.end {
            Bound::Included(e) => key <= e.as_ref(),
            Bound::Excluded(e) => key < e.as_ref(),
            Bound::Unbounded => true,
        };
        start_ok && end_ok
    }
}

#[derive(Default)]
struct Events {
    applied_batches: Vec<(u64, Vec<RowEntry>)>,
    last_written_seq: u64,
    misses: Vec<Bytes>,
    invalidations: Vec<OwnedRange>,
    get_calls: usize,
    scan_calls: usize,
}

struct TestRowCache {
    events: Mutex<Events>,
    versions: Mutex<BTreeMap<Bytes, Vec<RowEntry>>>,
    serve_reads: AtomicBool,
    fail_next_get: AtomicBool,
    /// When set, only keys matching one of these prefixes are served; everything else misses.
    covered_prefixes: Vec<&'static [u8]>,
}

impl TestRowCache {
    fn new() -> Arc<Self> {
        Self::covering(&[])
    }

    fn covering(prefixes: &[&'static [u8]]) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Events::default()),
            versions: Mutex::new(BTreeMap::new()),
            serve_reads: AtomicBool::new(true),
            fail_next_get: AtomicBool::new(false),
            covered_prefixes: prefixes.to_vec(),
        })
    }

    fn covers_key(&self, key: &[u8]) -> bool {
        if self.covered_prefixes.is_empty() {
            return true;
        }
        self.covered_prefixes.iter().any(|p| key.starts_with(p))
    }

    fn covers_range(&self, range: KeyRange<'_>) -> bool {
        if self.covered_prefixes.is_empty() {
            return true;
        }
        self.covered_prefixes.iter().any(|prefix| {
            let mut prefix_end = prefix.to_vec();
            *prefix_end.last_mut().unwrap() += 1;
            range.start_key().is_some_and(|s| s.starts_with(prefix))
                && range.end_key().is_some_and(|e| e <= &prefix_end[..])
        })
    }

    fn misses(&self) -> Vec<Bytes> {
        self.events.lock().unwrap().misses.clone()
    }

    fn lookup_at(&self, key: &[u8], seq: u64, now_ms: i64) -> Option<Option<KeyValue>> {
        let versions = self.versions.lock().unwrap();
        let history = versions.get(key)?;
        for entry in history.iter().rev() {
            if entry.seq <= seq {
                return match &entry.value {
                    ValueDeletable::Value(val) => {
                        if entry.expire_ts.is_some_and(|ts| ts <= now_ms) {
                            Some(None)
                        } else {
                            Some(Some(KeyValue::new(
                                entry.key.clone(),
                                val.clone(),
                                entry.seq,
                                entry.create_ts.unwrap_or(0),
                                entry.expire_ts,
                            )))
                        }
                    }
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
    fail_at: Option<usize>,
}

impl VecScanIterator {
    fn from_rows(rows: Vec<KeyValue>) -> Self {
        Self {
            rows,
            pos: 0,
            fail_at: None,
        }
    }
}

impl ScanIterator for VecScanIterator {
    fn next(&mut self) -> Result<Option<KeyValue>, Error> {
        if self.fail_at == Some(self.pos) {
            self.fail_at = None;
            return Err(Error::data("injected scan failure".to_string()));
        }
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let kv = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(kv))
    }

    fn seek(&mut self, next_key: &[u8]) -> Result<(), Error> {
        while self.pos < self.rows.len() && self.rows[self.pos].key.as_ref() < next_key {
            self.pos += 1;
        }
        Ok(())
    }
}

impl RowCache for TestRowCache {
    fn write(&self, seq: u64, entries: &[RowEntry]) {
        {
            let mut events = self.events.lock().unwrap();
            assert!(
                seq > events.last_written_seq,
                "write sequence numbers must strictly increase"
            );
            events.last_written_seq = seq;
        }
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

    fn invalidate(&self, range: KeyRange<'_>) {
        let owned = OwnedRange::from_key_range(range);
        {
            let mut versions = self.versions.lock().unwrap();
            versions.retain(|key, _| !owned.contains(key));
        }
        self.events.lock().unwrap().invalidations.push(owned);
    }

    fn get(&self, key: &[u8], seq: u64, now_ms: i64) -> GetOutcome {
        self.events.lock().unwrap().get_calls += 1;
        if self.fail_next_get.swap(false, Ordering::Relaxed) {
            return GetOutcome::Error(Error::invalid("injected get error".to_string()));
        }
        if !self.covers_key(key) || !self.serve_reads.load(Ordering::Relaxed) {
            self.events
                .lock()
                .unwrap()
                .misses
                .push(Bytes::copy_from_slice(key));
            return GetOutcome::Miss;
        }
        match self.lookup_at(key, seq, now_ms) {
            Some(kv) => GetOutcome::Hit(kv),
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
        seq: u64,
        now_ms: i64,
        _prefix: Option<&[u8]>,
    ) -> ScanOutcome {
        self.events.lock().unwrap().scan_calls += 1;
        if !self.covers_range(range) || !self.serve_reads.load(Ordering::Relaxed) {
            let start = range.start_key().unwrap_or_default();
            self.events
                .lock()
                .unwrap()
                .misses
                .push(Bytes::copy_from_slice(start));
            return ScanOutcome::Miss;
        }
        let versions = self.versions.lock().unwrap();
        let mut rows = Vec::new();
        for (_key, history) in versions.range::<[u8], _>(range) {
            for entry in history.iter().rev() {
                if entry.seq <= seq {
                    match &entry.value {
                        ValueDeletable::Value(val)
                            if entry.expire_ts.is_none_or(|ts| ts > now_ms) =>
                        {
                            rows.push(KeyValue::new(
                                entry.key.clone(),
                                val.clone(),
                                entry.seq,
                                entry.create_ts.unwrap_or(0),
                                entry.expire_ts,
                            ));
                        }
                        ValueDeletable::Merge(_) => {
                            let start = range.start_key().unwrap_or_default();
                            self.events
                                .lock()
                                .unwrap()
                                .misses
                                .push(Bytes::copy_from_slice(start));
                            return ScanOutcome::Miss;
                        }
                        _ => {}
                    }
                    break;
                }
            }
        }
        ScanOutcome::hit(VecScanIterator::from_rows(rows))
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
async fn test_row_cache_writes_reads_options_and_metrics() {
    let cache = TestRowCache::new();
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = Db::builder("/tmp/test_row_cache_lifecycle", Arc::new(InMemory::new()))
        .with_settings(Settings {
            flush_interval: None,
            ..Settings::default()
        })
        .with_metrics_recorder(recorder.clone())
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();

    // Writes pass through write() in ascending sequence order.
    db.put(b"a", b"1").await.unwrap();
    db.put(b"b", b"2").await.unwrap();
    db.merge(b"m", b"part1").await.unwrap();
    assert_eq!(cache.events.lock().unwrap().applied_batches.len(), 3);

    // Point reads hit the cache and SlateDB updates its own request counter.
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(
        lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "get")]),
        Some(1)
    );

    // Option gating bypasses the cache without invoking RowCache::get or RowCache::scan.
    let get_calls_before = cache.events.lock().unwrap().get_calls;
    let scan_calls_before = cache.events.lock().unwrap().scan_calls;
    assert_eq!(
        db.get_with_options(b"a", &ReadOptions::new().with_row_cache(false))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"1"))
    );
    assert_eq!(
        db.get_with_options(b"a", &ReadOptions::new().with_dirty(true))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"1"))
    );
    assert_eq!(
        db.get_with_options(
            b"a",
            &ReadOptions::new().with_durability_filter(DurabilityLevel::Remote),
        )
        .await
        .unwrap(),
        None
    );
    let _ = collect(
        db.scan_with_options(b"a"..b"c", &ScanOptions::new().with_row_cache(false))
            .await
            .unwrap(),
    )
    .await;
    let desc_opts = ScanOptions::new().with_order(IterationOrder::Descending);
    let desc_rows = collect(db.scan_with_options(b"a"..b"c", &desc_opts).await.unwrap()).await;
    assert_eq!(
        desc_rows,
        vec![
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
        ]
    );
    assert_eq!(cache.events.lock().unwrap().get_calls, get_calls_before);
    assert_eq!(cache.events.lock().unwrap().scan_calls, scan_calls_before);

    // Errors returned from the cache preserve ErrorKind.
    cache.fail_next_get.store(true, Ordering::Relaxed);
    let err = db.get(b"a").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Invalid);

    // Snapshots read at their started_seq and support bypassing the cache for cold loading.
    let snap = db.snapshot().unwrap();
    db.put(b"a", b"2").await.unwrap();
    assert_eq!(
        snap.get(b"a").await.unwrap(),
        Some(Bytes::from_static(b"1"))
    );
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"2")));
    let get_calls_before = cache.events.lock().unwrap().get_calls;
    assert_eq!(
        snap.get_with_options(b"a", &ReadOptions::new().with_row_cache(false))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"1"))
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
    assert_eq!(cache.events.lock().unwrap().get_calls, get_calls_before);
    drop(snap);

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
    let mut txn_scan = txn.scan(b"a"..b"c").await.unwrap();
    let mut txn_rows = Vec::new();
    txn_scan.next_batch(&mut txn_rows, 10).await.unwrap();
    assert_eq!(txn_rows.len(), 1);
    assert_eq!(txn_rows[0].key, Bytes::from_static(b"a"));
    assert_eq!(txn_rows[0].value, Bytes::from_static(b"txn_val"));
    txn.rollback();

    // Misses are observed directly inside get and scan.
    cache.serve_reads.store(false, Ordering::Relaxed);
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"2")));
    let _ = db.scan(b"b"..b"z").await.unwrap();
    assert_eq!(
        cache.misses(),
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );

    // Closing the database does not close or invalidate the caller-owned cache.
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_row_cache_minimal_defaults() {
    struct MinimalPointCache {
        rows: Mutex<BTreeMap<Bytes, (u64, Option<KeyValue>)>>,
        hits: AtomicU64,
    }

    impl RowCache for MinimalPointCache {
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

        fn get(&self, key: &[u8], seq: u64, now_ms: i64) -> GetOutcome {
            let rows = self.rows.lock().unwrap();
            match rows.get(key) {
                Some((entry_seq, Some(kv))) if *entry_seq <= seq => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    if kv.expire_ts.is_some_and(|ts| ts <= now_ms) {
                        GetOutcome::Hit(None)
                    } else {
                        GetOutcome::Hit(Some(kv.clone()))
                    }
                }
                Some((entry_seq, None)) if *entry_seq <= seq => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    GetOutcome::Hit(None)
                }
                _ => GetOutcome::Miss,
            }
        }
    }

    let cache = Arc::new(MinimalPointCache {
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
    assert_eq!(KeyRange::all().start_key(), None);
    assert_eq!(KeyRange::all().end_key(), None);
    assert!(KeyRange::all().contains(b"anything"));
}

#[tokio::test]
async fn test_row_cache_scan_next_batch_seek_and_invalidation() {
    struct CustomBatchCache {
        fail_at: Option<usize>,
    }

    impl RowCache for CustomBatchCache {
        fn write(&self, _seq: u64, _entries: &[RowEntry]) {}

        fn get(&self, _key: &[u8], _seq: u64, _now_ms: i64) -> GetOutcome {
            GetOutcome::Miss
        }

        fn scan(
            &self,
            _range: KeyRange<'_>,
            _seq: u64,
            _now_ms: i64,
            _prefix: Option<&[u8]>,
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
                fail_at: self.fail_at,
            })
        }
    }

    // Batch read + seek through next_batch.
    let db = Db::builder("/tmp/test_row_cache_batch_scan", Arc::new(InMemory::new()))
        .with_row_cache(Arc::new(CustomBatchCache { fail_at: None }))
        .build()
        .await
        .unwrap();

    let mut iter = db.scan(..).await.unwrap();
    let mut out = Vec::new();
    let n = iter.next_batch(&mut out, 3).await.unwrap();
    assert_eq!(n, 3);
    assert_eq!(out[2].key, Bytes::from_static(b"k2"));

    iter.seek(b"k4").await.unwrap();
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key, Bytes::from_static(b"k4"));
    assert!(iter.next().await.unwrap().is_none());
    db.close().await.unwrap();

    // Error midway through next_batch preserves preceding rows and invalidates the iterator.
    let db = Db::builder("/tmp/test_row_cache_batch_err", Arc::new(InMemory::new()))
        .with_row_cache(Arc::new(CustomBatchCache { fail_at: Some(2) }))
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
}

#[tokio::test]
async fn test_row_cache_partial_scan_stitching_seek_and_metrics() {
    /// Caches only `a/` and `c/` prefixes; leaves `b/` and `d/` as uncached LSM gaps.
    struct PartialRowCache {
        versions: Mutex<BTreeMap<Bytes, Vec<RowEntry>>>,
        fail_cached_at: Mutex<Option<usize>>,
        return_empty: AtomicBool,
    }

    impl PartialRowCache {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                versions: Mutex::new(BTreeMap::new()),
                fail_cached_at: Mutex::new(None),
                return_empty: AtomicBool::new(false),
            })
        }

        fn cached_rows(&self, start: &[u8], end: &[u8], seq: u64, now_ms: i64) -> Vec<KeyValue> {
            let versions = self.versions.lock().unwrap();
            let mut rows = Vec::new();
            for (_key, history) in versions.range::<[u8], _>(KeyRange {
                start: Bound::Included(start),
                end: Bound::Excluded(end),
            }) {
                for entry in history.iter().rev() {
                    if entry.seq <= seq {
                        if let ValueDeletable::Value(val) = &entry.value {
                            if entry.expire_ts.is_none_or(|ts| ts > now_ms) {
                                rows.push(KeyValue::new(
                                    entry.key.clone(),
                                    val.clone(),
                                    entry.seq,
                                    entry.create_ts.unwrap_or(0),
                                    entry.expire_ts,
                                ));
                            }
                        }
                        break;
                    }
                }
            }
            rows
        }
    }

    impl RowCache for PartialRowCache {
        fn write(&self, _seq: u64, entries: &[RowEntry]) {
            let mut versions = self.versions.lock().unwrap();
            for e in entries {
                if e.key.starts_with(b"a/") || e.key.starts_with(b"c/") {
                    versions.entry(e.key.clone()).or_default().push(e.clone());
                }
            }
        }

        fn get(&self, _key: &[u8], _seq: u64, _now_ms: i64) -> GetOutcome {
            GetOutcome::Miss
        }

        fn scan(
            &self,
            range: KeyRange<'_>,
            seq: u64,
            now_ms: i64,
            _prefix: Option<&[u8]>,
        ) -> ScanOutcome {
            if self.return_empty.load(Ordering::Relaxed) {
                return ScanOutcome::Partial(Vec::new());
            }
            let fail_at = *self.fail_cached_at.lock().unwrap();
            let a_rows = self.cached_rows(b"a/", b"b/", seq, now_ms);
            let c_rows = self.cached_rows(b"c/", b"d/", seq, now_ms);
            // Clip cached rows to the requested range.
            let filter = |rows: Vec<KeyValue>| -> Vec<KeyValue> {
                rows.into_iter()
                    .filter(|kv| range.contains(&kv.key))
                    .collect()
            };
            let segments = vec![
                ScanSegment::cached(
                    VecScanIterator {
                        rows: filter(a_rows),
                        pos: 0,
                        fail_at,
                    },
                    Bound::Excluded(Bytes::from_static(b"b/")),
                ),
                // Empty gap between segments must be skipped without opening an LSM scan.
                ScanSegment::gap(
                    Bound::Excluded(Bytes::from_static(b"b/")),
                    Bound::Excluded(Bytes::from_static(b"b/")),
                ),
                ScanSegment::gap(
                    Bound::Included(Bytes::from_static(b"b/")),
                    Bound::Excluded(Bytes::from_static(b"c/")),
                ),
                ScanSegment::cached(
                    VecScanIterator::from_rows(filter(c_rows)),
                    Bound::Excluded(Bytes::from_static(b"d/")),
                ),
                ScanSegment::gap(Bound::Included(Bytes::from_static(b"d/")), Bound::Unbounded),
            ];
            let outcome = ScanOutcome::Partial(segments);
            assert!(format!("{outcome:?}").contains("Partial"));
            outcome
        }
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cache = PartialRowCache::new();
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = Db::builder("partial_scan_db", Arc::clone(&store))
        .with_metrics_recorder(recorder.clone())
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();

    // Put rows across cached (a/, c/) and uncached (b/, d/) prefixes, flushing b/ and d/ to L0.
    for (k, v) in [
        ("a/1", "av1"),
        ("a/2", "av2"),
        ("b/1", "bv1"),
        ("b/2", "bv2"),
        ("c/1", "cv1"),
        ("c/2", "cv2"),
        ("d/1", "dv1"),
        ("d/2", "dv2"),
    ] {
        db.put(k.as_bytes(), v.as_bytes()).await.unwrap();
    }
    flush_memtable(&db).await;
    // Add one more write in memtable to test both L0 and memtable gaps.
    db.put(b"b/3", b"bv3").await.unwrap();
    db.put(b"c/3", b"cv3").await.unwrap();

    let expected: Vec<(Bytes, Bytes)> = [
        ("a/1", "av1"),
        ("a/2", "av2"),
        ("b/1", "bv1"),
        ("b/2", "bv2"),
        ("b/3", "bv3"),
        ("c/1", "cv1"),
        ("c/2", "cv2"),
        ("c/3", "cv3"),
        ("d/1", "dv1"),
        ("d/2", "dv2"),
    ]
    .into_iter()
    .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
    .collect();

    // 1. Full next() walk across Cached -> Gap -> Cached -> Gap counts scan_requests once.
    assert_eq!(
        collect(db.scan(b"a/"..b"e/").await.unwrap()).await,
        expected
    );
    assert_eq!(
        lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "scan")]),
        Some(1)
    );

    // 2. next_batch() across all segment boundaries matches next() identically.
    let mut iter = db.scan(b"a/"..b"e/").await.unwrap();
    let mut batched = Vec::new();
    loop {
        let n = iter.next_batch(&mut batched, 3).await.unwrap();
        if n == 0 {
            break;
        }
    }
    let batched_pairs: Vec<(Bytes, Bytes)> =
        batched.into_iter().map(|kv| (kv.key, kv.value)).collect();
    assert_eq!(batched_pairs, expected);
    assert_eq!(
        lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "scan")]),
        Some(2)
    );

    // 3. seek() across Cached -> skip unopened Gap -> Cached -> clamp into unopened Gap -> end.
    let mut iter = db.scan(b"a/"..b"e/").await.unwrap();
    assert_eq!(
        iter.next().await.unwrap().unwrap().key,
        Bytes::from_static(b"a/1")
    );
    // Jump over b/ gap straight into c/2 (never opens b/ gap).
    iter.seek(b"c/2").await.unwrap();
    assert_eq!(
        iter.next().await.unwrap().unwrap().key,
        Bytes::from_static(b"c/2")
    );
    // Seek into unopened d/ gap twice before calling next(), clamping start forward.
    iter.seek(b"d/1").await.unwrap();
    iter.seek(b"d/2").await.unwrap();
    assert_eq!(
        iter.next().await.unwrap().unwrap().key,
        Bytes::from_static(b"d/2")
    );
    assert!(iter.next().await.unwrap().is_none());

    // Seek inside an already-open Gap segment.
    let mut iter = db.scan(b"a/"..b"e/").await.unwrap();
    iter.seek(b"b/1").await.unwrap();
    assert_eq!(
        iter.next().await.unwrap().unwrap().key,
        Bytes::from_static(b"b/1")
    );
    iter.seek(b"b/3").await.unwrap();
    assert_eq!(
        iter.next().await.unwrap().unwrap().key,
        Bytes::from_static(b"b/3")
    );
    // Seek past all remaining keys returns None.
    iter.seek(b"d/9").await.unwrap();
    assert!(iter.next().await.unwrap().is_none());

    // 4. Snapshot and Transaction partial scans read both cached and gap segments at started_seq.
    let snap = db.snapshot().unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).unwrap();
    db.put(b"a/1", b"av1_new").await.unwrap();
    db.put(b"b/1", b"bv1_new").await.unwrap();
    db.put(b"c/1", b"cv1_new").await.unwrap();
    db.put(b"d/1", b"dv1_new").await.unwrap();
    flush_memtable(&db).await;

    assert_eq!(
        collect(snap.scan(b"a/"..b"e/").await.unwrap()).await,
        expected
    );
    assert_eq!(
        collect(txn.scan(b"a/"..b"e/").await.unwrap()).await,
        expected
    );
    txn.rollback();
    drop(snap);

    // 5. DbReader partial scan stitches cached and uncached segments from object storage.
    let reader_cache = PartialRowCache::new();
    let reader = DbReader::builder("partial_scan_db", Arc::clone(&store))
        .with_options(reader_options(20, Duration::from_secs(60)))
        .with_row_cache(reader_cache.clone())
        .build()
        .await
        .unwrap();
    let mut r_status = reader.subscribe();
    db.put(b"a/9", b"av9").await.unwrap();
    db.put(b"b/9", b"bv9").await.unwrap();
    db.flush().await.unwrap();
    caught_up(&mut r_status, &db).await;
    // Reader's cache has a/9 (replayed from WAL) and misses b/9 (read from WAL/LSM gap).
    let reader_rows = collect(reader.scan(&b"a/9"[..]..&b"c/"[..]).await.unwrap()).await;
    assert_eq!(
        reader_rows,
        vec![
            (Bytes::from_static(b"a/9"), Bytes::from_static(b"av9")),
            (Bytes::from_static(b"b/1"), Bytes::from_static(b"bv1_new")),
            (Bytes::from_static(b"b/2"), Bytes::from_static(b"bv2")),
            (Bytes::from_static(b"b/3"), Bytes::from_static(b"bv3")),
            (Bytes::from_static(b"b/9"), Bytes::from_static(b"bv9")),
        ]
    );
    reader.close().await.unwrap();

    // 6. Empty Partial segment list returns no rows cleanly.
    cache.return_empty.store(true, Ordering::Relaxed);
    assert!(collect(db.scan(b"a/"..b"e/").await.unwrap())
        .await
        .is_empty());
    cache.return_empty.store(false, Ordering::Relaxed);

    // 7. Error inside a Cached segment invalidates the partial iterator and preserves ErrorKind.
    *cache.fail_cached_at.lock().unwrap() = Some(1);
    let mut iter = db.scan(b"a/"..b"e/").await.unwrap();
    let mut out = Vec::new();
    let err = iter.next_batch(&mut out, 5).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Data);
    assert_eq!(out.len(), 1);
    assert_eq!(iter.next().await.unwrap_err().kind(), ErrorKind::Data);
    db.close().await.unwrap();
}

// DbReader tests. A writer and a reader share one in-memory store; the reader's cache covers
// `hot/` (and `warm/` in range-invalidation tests) written after the reader opens.

const PATH: &str = "reader_row_cache";
const HOT: &[u8] = b"hot/";
const WARM: &[u8] = b"warm/";

fn hot(name: &str) -> Bytes {
    Bytes::from(format!("hot/{name}"))
}

fn warm(name: &str) -> Bytes {
    Bytes::from(format!("warm/{name}"))
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

fn value(v: &'static str) -> Option<Bytes> {
    Some(Bytes::from_static(v.as_bytes()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reader_row_cache_replay_reads_and_metrics() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store, true).await;
    db.put(b"cold/a", b"a1").await.unwrap();
    db.flush().await.unwrap();
    let cache = TestRowCache::covering(&[HOT]);
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let reader = DbReader::builder(PATH, Arc::clone(&store))
        .with_options(reader_options(20, Duration::from_secs(3600)))
        .with_metrics_recorder(recorder.clone())
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();
    let mut status = reader.subscribe();
    let opened_at = reader.status().durable_seq;
    // Startup WAL replay already fed `cold/a` before build() returned.
    assert_eq!(cache.events.lock().unwrap().applied_batches.len(), 1);
    assert_eq!(cache.events.lock().unwrap().applied_batches[0].0, opened_at);

    // Replay feeds one key-ordered batch per seq in ascending order.
    let mut batch = WriteBatch::new();
    batch.put(hot("c"), b"c1");
    batch.put(hot("a"), b"a1");
    batch.delete(hot("b"));
    db.write(batch).await.unwrap();
    put_durable(&db, &[("y", "y1"), ("x", "x1")]).await;
    caught_up(&mut status, &db).await;
    let batches = cache.events.lock().unwrap().applied_batches[1..].to_vec();
    assert_eq!(batches.len(), 3);
    assert!(batches[0].0 > opened_at);
    let keys: Vec<Bytes> = batches[0].1.iter().map(|e| e.key.clone()).collect();
    assert_eq!(keys, vec![hot("a"), hot("b"), hot("c")]);
    assert_eq!(batches[0].1[1].value, ValueDeletable::Tombstone);
    assert!(batches.windows(2).all(|w| w[1].0 > w[0].0));

    // Uncovered key misses; covered keys hit (including DurabilityLevel::Remote on DbReader).
    let gets = lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "get")]).unwrap();
    assert_eq!(reader.get(b"cold/a").await.unwrap(), value("a1"));
    assert_eq!(cache.misses(), vec![Bytes::from_static(b"cold/a")]);
    assert_eq!(
        reader
            .get_with_options(
                hot("x"),
                &ReadOptions::new().with_durability_filter(DurabilityLevel::Remote),
            )
            .await
            .unwrap(),
        value("x1")
    );
    let kv = reader.get_key_value(hot("a")).await.unwrap().unwrap();
    assert_eq!(kv.seq, batches[0].0);
    assert_eq!(reader.get(hot("b")).await.unwrap(), None);
    assert_eq!(
        lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "get")]),
        Some(gets + 4)
    );

    // row_cache: false bypasses the cache on DbReader.
    let get_calls_before = cache.events.lock().unwrap().get_calls;
    assert_eq!(
        reader
            .get_with_options(hot("x"), &ReadOptions::new().with_row_cache(false))
            .await
            .unwrap(),
        value("x1")
    );
    assert_eq!(cache.events.lock().unwrap().get_calls, get_calls_before);

    // Covered scans and prefix scans hit; uncovered ones miss; scan_requests increments.
    let scans = lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "scan")]).unwrap();
    let expected = rows(&[("a", "a1"), ("c", "c1"), ("x", "x1"), ("y", "y1")]);
    let scanned = collect(reader.scan(hot("a")..hot("z")).await.unwrap()).await;
    assert_eq!(scanned, expected);
    let scanned = collect(reader.scan_prefix(HOT, ..).await.unwrap()).await;
    assert_eq!(scanned, expected);
    assert_eq!(cache.misses().len(), 1);
    let cold = collect(reader.scan_prefix(b"cold/", ..).await.unwrap()).await;
    assert_eq!(cold.len(), 1);
    assert_eq!(cache.misses().last(), Some(&Bytes::from_static(b"cold/")));
    assert_eq!(
        lookup_metric_with_labels(&recorder, REQUEST_COUNT, &[("op", "scan")]),
        Some(scans + 3)
    );

    // Flushing and compacting already-replayed writes invalidates nothing.
    flush_memtable(&db).await;
    caught_up(&mut status, &db).await;
    put_durable(&db, &[("z", "z1")]).await;
    caught_up(&mut status, &db).await;
    flush_memtable(&db).await;
    wait_for_status(&mut status, |s| {
        s.current_manifest.l0().is_empty() && !s.current_manifest.compacted().is_empty()
    })
    .await;
    assert!(cache.events.lock().unwrap().invalidations.is_empty());
    assert_eq!(reader.get(hot("x")).await.unwrap(), value("x1"));
    assert_eq!(reader.get(hot("z")).await.unwrap(), value("z1"));
    assert_eq!(cache.misses().len(), 2);

    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_reader_row_cache_range_invalidation_compaction_fallback_and_pinned_mode() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store, false).await;

    // 1. Range-scoped invalidation: an unreplayed L0 touching only `warm/` keeps `hot/` warm!
    let cache = TestRowCache::covering(&[HOT, WARM]);
    let options = reader_options(300, Duration::from_secs(10));
    let reader = open_reader(&store, &cache, options).await;
    let mut status = reader.subscribe();
    db.put(hot("a"), b"h1").await.unwrap();
    db.put(warm("a"), b"w1").await.unwrap();
    db.flush().await.unwrap();
    caught_up(&mut status, &db).await;
    flush_memtable(&db).await;
    caught_up(&mut status, &db).await;
    assert!(cache.events.lock().unwrap().invalidations.is_empty());

    // Now write ONLY `warm/a` and flush before the reader polls again, plus a WAL write `hot/z`.
    db.put(warm("a"), b"w2").await.unwrap();
    db.flush().await.unwrap();
    flush_memtable(&db).await;
    db.put(hot("z"), b"hz").await.unwrap();
    db.flush().await.unwrap();
    caught_up(&mut status, &db).await;

    let invalidations = cache.events.lock().unwrap().invalidations.clone();
    assert_eq!(invalidations.len(), 1);
    assert!(
        !invalidations[0].is_all(),
        "L0 flush should be range-scoped"
    );
    assert!(invalidations[0].contains(&warm("a")));
    assert!(!invalidations[0].contains(&hot("a")));

    // hot/a stayed cached (hit), hot/z arrived via WAL replay (hit), warm/a missed -> LSM w2.
    assert_eq!(reader.get(hot("a")).await.unwrap(), value("h1"));
    assert_eq!(reader.get(hot("z")).await.unwrap(), value("hz"));
    assert_eq!(reader.get(warm("a")).await.unwrap(), value("w2"));
    assert_eq!(cache.misses(), vec![warm("a")]);
    reader.close().await.unwrap();

    // 2. skip_wal_replay: feeds no non-empty batches and invalidates the flushed L0 range.
    let cache = TestRowCache::covering(&[HOT]);
    let options = DbReaderOptions {
        skip_wal_replay: true,
        ..reader_options(20, Duration::from_secs(10))
    };
    let reader = open_reader(&store, &cache, options).await;
    let mut status = reader.subscribe();
    let mut previous = reader.get(hot("y")).await.unwrap();
    for (round, v) in ["y1", "y2"].into_iter().enumerate() {
        let before = reader.status().durable_seq;
        put_durable(&db, &[("y", v)]).await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(cache.events.lock().unwrap().invalidations.len(), round);
        assert_eq!(reader.get(hot("y")).await.unwrap(), previous);
        flush_memtable(&db).await;
        wait_for_status(&mut status, |s| s.durable_seq > before).await;
        let invalidations = cache.events.lock().unwrap().invalidations.clone();
        assert_eq!(invalidations.len(), round + 1);
        assert!(invalidations[round].contains(&hot("y")));
        assert_eq!(
            cache.events.lock().unwrap().last_written_seq,
            reader.status().durable_seq
        );
        previous = reader.get(hot("y")).await.unwrap();
        assert_eq!(previous, value(v));
    }
    assert!(cache.events.lock().unwrap().applied_batches.is_empty());
    reader.close().await.unwrap();

    // 3. Pinned DbReaderMode::Checkpoint never polls or invalidates.
    let admin = Admin::builder(PATH, Arc::clone(&store)).build();
    let checkpoint = db
        .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
        .await
        .unwrap();
    let cache = TestRowCache::covering(&[HOT]);
    let reader = DbReader::builder(PATH, Arc::clone(&store))
        .with_reader_mode(DbReaderMode::Checkpoint(checkpoint.id))
        .with_options(reader_options(5, Duration::from_secs(10)))
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();
    let pinned_seq = reader.status().durable_seq;
    put_durable(&db, &[("y", "y3")]).await;
    flush_memtable(&db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reader.status().durable_seq, pinned_seq);
    assert!(cache.events.lock().unwrap().applied_batches.is_empty());
    assert!(cache.events.lock().unwrap().invalidations.is_empty());
    assert_eq!(reader.get(hot("y")).await.unwrap(), value("y2"));
    reader.close().await.unwrap();
    admin.delete_checkpoint(checkpoint.id).await.unwrap();
    db.close().await.unwrap();

    // 4. Compaction fallback: when unseen L0s compact before a poll, invalidate(KeyRange::all()).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store, true).await;
    let cache = TestRowCache::covering(&[HOT]);
    // Synchronize right after a poll tick so the full poll interval is available for two L0
    // flushes plus compaction before the next poll runs.
    let reader = open_reader(&store, &cache, reader_options(400, Duration::from_secs(10))).await;
    let mut r_status = reader.subscribe();
    let mut w_status = db.subscribe();
    put_durable(&db, &[("a", "a1")]).await;
    caught_up(&mut r_status, &db).await;

    put_durable(&db, &[("b", "b1")]).await;
    flush_memtable(&db).await;
    put_durable(&db, &[("c", "c1")]).await;
    flush_memtable(&db).await;
    wait_for_status(&mut w_status, |s| {
        s.current_manifest.l0().is_empty() && !s.current_manifest.compacted().is_empty()
    })
    .await;
    caught_up(&mut r_status, &db).await;

    let invalidations = cache.events.lock().unwrap().invalidations.clone();
    assert!(
        invalidations.iter().any(OwnedRange::is_all),
        "expected KeyRange::all() invalidation after compaction, got {invalidations:?}"
    );
    assert_eq!(reader.get(hot("c")).await.unwrap(), value("c1"));
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_row_cache_startup_wal_replay_on_db_and_reader() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "startup_wal_replay_cache";
    let writer_settings = || Settings {
        flush_interval: None,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };

    // 1. Seed L0 (`k1`, `k2`), then leave unflushed WAL batches (`k3`, multi-key batch deleting
    // `k2` and putting `k4`, `k5`) by closing with WAL flush only.
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(writer_settings())
        .build()
        .await
        .unwrap();
    let h1 = db.put(b"k1", b"v1").await.unwrap();
    let h2 = db.put(b"k2", b"v2").await.unwrap();
    flush_memtable(&db).await;
    let last_l0_seq = db.status().current_manifest.last_l0_seq();
    assert_eq!(last_l0_seq, h2.seqnum());

    let h3 = db.put(b"k3", b"v3").await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"k5", b"v5");
    batch.put(b"k4", b"v4");
    batch.delete(b"k2");
    let h4 = db.write(batch).await.unwrap();
    db.close_with_options(CloseOptions {
        flush_type: Some(FlushType::Wal),
    })
    .await
    .unwrap();

    // 2. Reopen Db with a persistent-style cache pre-seeded with the L0 rows (`k1`, `k2`).
    let cache = TestRowCache::new();
    {
        let mut versions = cache.versions.lock().unwrap();
        versions.insert(
            Bytes::from_static(b"k1"),
            vec![RowEntry {
                key: Bytes::from_static(b"k1"),
                value: ValueDeletable::Value(Bytes::from_static(b"v1")),
                seq: h1.seqnum(),
                create_ts: Some(0),
                expire_ts: None,
            }],
        );
        versions.insert(
            Bytes::from_static(b"k2"),
            vec![RowEntry {
                key: Bytes::from_static(b"k2"),
                value: ValueDeletable::Value(Bytes::from_static(b"v2")),
                seq: h2.seqnum(),
                create_ts: Some(0),
                expire_ts: None,
            }],
        );
    }
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(writer_settings())
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();

    // Startup WAL replay fed both unflushed batches in ascending (seq, key) order before open
    // returned, without re-sending L0 entries at or below `last_l0_seq`.
    let batches = cache.events.lock().unwrap().applied_batches.clone();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].0, h3.seqnum());
    assert_eq!(batches[1].0, h4.seqnum());
    let batch2_keys: Vec<Bytes> = batches[1].1.iter().map(|e| e.key.clone()).collect();
    assert_eq!(
        batch2_keys,
        vec![
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"k4"),
            Bytes::from_static(b"k5"),
        ]
    );
    assert_eq!(batches[1].1[0].value, ValueDeletable::Tombstone);

    // Every key (pre-seeded L0 + replayed WAL puts + replayed WAL delete) hits the cache cleanly.
    assert_eq!(
        db.get(b"k1").await.unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(db.get(b"k2").await.unwrap(), None);
    assert_eq!(
        db.get(b"k3").await.unwrap(),
        Some(Bytes::from_static(b"v3"))
    );
    assert_eq!(
        db.get(b"k4").await.unwrap(),
        Some(Bytes::from_static(b"v4"))
    );
    assert_eq!(
        db.get(b"k5").await.unwrap(),
        Some(Bytes::from_static(b"v5"))
    );
    assert!(cache.misses().is_empty());

    // Live writes continue strictly above the replayed sequence number.
    let h5 = db.put(b"k6", b"v6").await.unwrap();
    assert!(h5.seqnum() > h4.seqnum());
    assert_eq!(
        db.get(b"k6").await.unwrap(),
        Some(Bytes::from_static(b"v6"))
    );
    db.flush().await.unwrap();

    // 3. DbReader::open also feeds initial replayed WAL batches before returning.
    let reader_cache = TestRowCache::new();
    let reader = DbReader::builder(path, Arc::clone(&store))
        .with_options(reader_options(500, Duration::from_secs(60)))
        .with_row_cache(reader_cache.clone())
        .build()
        .await
        .unwrap();
    let replayed = reader_cache.events.lock().unwrap().applied_batches.clone();
    assert!(!replayed.is_empty());
    assert_eq!(replayed.last().unwrap().0, h5.seqnum());
    assert_eq!(
        reader.get(b"k6").await.unwrap(),
        Some(Bytes::from_static(b"v6"))
    );
    assert!(reader_cache.misses().is_empty());
    reader.close().await.unwrap();

    // 4. DbReader with skip_wal_replay: true feeds nothing on open.
    let skip_cache = TestRowCache::new();
    let reader = DbReader::builder(path, Arc::clone(&store))
        .with_options(DbReaderOptions {
            skip_wal_replay: true,
            ..reader_options(500, Duration::from_secs(60))
        })
        .with_row_cache(skip_cache.clone())
        .build()
        .await
        .unwrap();
    assert!(skip_cache.events.lock().unwrap().applied_batches.is_empty());
    assert_eq!(skip_cache.events.lock().unwrap().last_written_seq, 0);
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[cfg(feature = "compaction_filters")]
#[tokio::test]
async fn test_row_cache_disallows_compaction_filter_supplier() {
    use async_trait::async_trait;
    use slatedb::{
        CompactionFilter, CompactionFilterDecision, CompactionFilterError,
        CompactionFilterSupplier, CompactionJobContext, CompactorBuilder,
    };

    struct NoopFilter;

    #[async_trait]
    impl CompactionFilter for NoopFilter {
        async fn filter(
            &mut self,
            _entry: &RowEntry,
        ) -> Result<CompactionFilterDecision, CompactionFilterError> {
            Ok(CompactionFilterDecision::Keep)
        }

        async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
            Ok(())
        }
    }

    struct NoopFilterSupplier;

    #[async_trait]
    impl CompactionFilterSupplier for NoopFilterSupplier {
        async fn create_compaction_filter(
            &self,
            _context: &CompactionJobContext,
        ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
            Ok(Box::new(NoopFilter))
        }
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "row_cache_disallows_compaction_filter";

    // Rejects with_row_cache followed by with_compactor_builder(filter).
    let compactor = CompactorBuilder::new(path, Arc::clone(&store))
        .with_compaction_filter_supplier(Arc::new(NoopFilterSupplier));
    let err = match Db::builder(path, Arc::clone(&store))
        .with_row_cache(TestRowCache::new())
        .with_compactor_builder(compactor)
        .build()
        .await
    {
        Ok(_) => panic!("expected invalid configuration error"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::Invalid);

    // Rejects with_compactor_builder(filter) followed by with_row_cache.
    let compactor = CompactorBuilder::new(path, Arc::clone(&store))
        .with_compaction_filter_supplier(Arc::new(NoopFilterSupplier));
    let err = match Db::builder(path, Arc::clone(&store))
        .with_compactor_builder(compactor)
        .with_row_cache(TestRowCache::new())
        .build()
        .await
    {
        Ok(_) => panic!("expected invalid configuration error"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::Invalid);

    // Allows with_row_cache with a CompactorBuilder that has no compaction filter.
    let compactor = CompactorBuilder::new(path, Arc::clone(&store));
    let db = Db::builder(path, Arc::clone(&store))
        .with_row_cache(TestRowCache::new())
        .with_compactor_builder(compactor)
        .build()
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_row_cache_ttl_expiration() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "row_cache_ttl_expiration";
    let clock = Arc::new(MockSystemClock::with_time(1000));
    let cache = TestRowCache::new();
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(Settings {
            flush_interval: None,
            default_ttl_millis: Some(50),
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        })
        .with_system_clock(clock.clone())
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();

    let no_expiry = PutOptions { ttl: Ttl::NoExpiry };
    let write_opts = WriteOptions::default();

    // Write keys with default TTL (1050), relative TTL (1100), absolute TTL (1200), and no expiry.
    db.put_with_options(b"k_default", b"d1", &PutOptions::default(), &write_opts)
        .await
        .unwrap();
    db.put_with_options(
        b"k_after",
        b"a1",
        &PutOptions {
            ttl: Ttl::ExpireAfterMillis(100),
        },
        &write_opts,
    )
    .await
    .unwrap();
    db.put_with_options(
        b"k_at",
        b"at1",
        &PutOptions {
            ttl: Ttl::ExpireAtMillis(1200),
        },
        &write_opts,
    )
    .await
    .unwrap();
    db.put_with_options(b"k_forever", b"f1", &no_expiry, &write_opts)
        .await
        .unwrap();
    // Overwrite an unexpired value with an expiring one; expiry must hide the key, not revive old.
    db.put_with_options(b"k_overwrite", b"old", &no_expiry, &write_opts)
        .await
        .unwrap();
    db.put_with_options(
        b"k_overwrite",
        b"new",
        &PutOptions {
            ttl: Ttl::ExpireAfterMillis(100),
        },
        &write_opts,
    )
    .await
    .unwrap();
    // Expiring merge operand on top of an unexpired base value.
    db.put_with_options(b"m_ttl", b"base", &no_expiry, &write_opts)
        .await
        .unwrap();
    db.merge_with_options(
        b"m_ttl",
        b"+tmp",
        &MergeOptions {
            ttl: Ttl::ExpireAfterMillis(50),
        },
        &write_opts,
    )
    .await
    .unwrap();

    let snap = db.snapshot().unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).unwrap();

    // At t = 1049, all keys are live and served from the cache with their expire_ts intact.
    clock.set(1049);
    assert_eq!(
        db.get_key_value(b"k_default")
            .await
            .unwrap()
            .unwrap()
            .expire_ts,
        Some(1050)
    );
    assert_eq!(
        db.get(b"k_after").await.unwrap(),
        Some(Bytes::from_static(b"a1"))
    );
    assert_eq!(
        db.get(b"k_at").await.unwrap(),
        Some(Bytes::from_static(b"at1"))
    );
    assert_eq!(
        db.get(b"k_forever").await.unwrap(),
        Some(Bytes::from_static(b"f1"))
    );
    assert_eq!(
        db.get(b"k_overwrite").await.unwrap(),
        Some(Bytes::from_static(b"new"))
    );
    let expected_1049 = vec![
        (Bytes::from_static(b"k_after"), Bytes::from_static(b"a1")),
        (Bytes::from_static(b"k_at"), Bytes::from_static(b"at1")),
        (Bytes::from_static(b"k_default"), Bytes::from_static(b"d1")),
        (Bytes::from_static(b"k_forever"), Bytes::from_static(b"f1")),
        (
            Bytes::from_static(b"k_overwrite"),
            Bytes::from_static(b"new"),
        ),
    ];
    assert_eq!(
        collect(db.scan(b"k_"..b"k`").await.unwrap()).await,
        expected_1049
    );
    assert_eq!(
        collect(snap.scan(b"k_"..b"k`").await.unwrap()).await,
        expected_1049
    );
    assert_eq!(
        collect(txn.scan(b"k_"..b"k`").await.unwrap()).await,
        expected_1049
    );
    assert!(cache.misses().is_empty());

    // Merge key misses cache and resolves via LSM at t = 1049.
    assert_eq!(
        db.get(b"m_ttl").await.unwrap(),
        Some(Bytes::from_static(b"base+tmp"))
    );
    assert_eq!(cache.misses(), vec![Bytes::from_static(b"m_ttl")]);

    // At t = 1050, k_default expires on Db, DbSnapshot, and DbTransaction via cache hits.
    clock.set(1050);
    assert_eq!(db.get(b"k_default").await.unwrap(), None);
    assert_eq!(snap.get(b"k_default").await.unwrap(), None);
    assert_eq!(txn.get(b"k_default").await.unwrap(), None);
    let expected_1050 = vec![
        (Bytes::from_static(b"k_after"), Bytes::from_static(b"a1")),
        (Bytes::from_static(b"k_at"), Bytes::from_static(b"at1")),
        (Bytes::from_static(b"k_forever"), Bytes::from_static(b"f1")),
        (
            Bytes::from_static(b"k_overwrite"),
            Bytes::from_static(b"new"),
        ),
    ];
    assert_eq!(
        collect(db.scan(b"k_"..b"k`").await.unwrap()).await,
        expected_1050
    );
    assert_eq!(
        collect(snap.scan(b"k_"..b"k`").await.unwrap()).await,
        expected_1050
    );
    assert_eq!(
        collect(txn.scan(b"k_"..b"k`").await.unwrap()).await,
        expected_1050
    );
    assert_eq!(cache.misses().len(), 1);
    txn.rollback();
    drop(snap);

    // At t = 1100, k_after and k_overwrite expire without resurrecting "old".
    clock.set(1100);
    assert_eq!(db.get(b"k_after").await.unwrap(), None);
    assert_eq!(db.get(b"k_overwrite").await.unwrap(), None);
    assert_eq!(
        collect(db.scan(b"k_"..b"k`").await.unwrap()).await,
        vec![
            (Bytes::from_static(b"k_at"), Bytes::from_static(b"at1")),
            (Bytes::from_static(b"k_forever"), Bytes::from_static(b"f1")),
        ]
    );
    // Advance memtable tick to 1100 and flush so RetentionIterator purges expired entries.
    db.put_with_options(b"z_tick", b"t1", &no_expiry, &write_opts)
        .await
        .unwrap();
    flush_memtable(&db).await;
    assert_eq!(
        db.get_with_options(b"k_overwrite", &ReadOptions::new().with_row_cache(false))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        db.get(b"m_ttl").await.unwrap(),
        Some(Bytes::from_static(b"base"))
    );

    // Write a WAL-only expiring key at t = 1100 (expire_ts = 1200) for DbReader tests.
    db.put_with_options(
        b"r_key",
        b"r1",
        &PutOptions {
            ttl: Ttl::ExpireAtMillis(1200),
        },
        &write_opts,
    )
    .await
    .unwrap();
    db.flush().await.unwrap();

    // DbReader sharing the system clock observes expiration on both get and scan.
    let reader_cache = TestRowCache::new();
    let reader = DbReader::builder(path, Arc::clone(&store))
        .with_system_clock(clock.clone())
        .with_options(reader_options(50, Duration::from_secs(3600)))
        .with_row_cache(reader_cache.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        reader.get(b"r_key").await.unwrap(),
        Some(Bytes::from_static(b"r1"))
    );
    assert_eq!(
        collect(reader.scan(b"r_"..b"r`").await.unwrap()).await,
        vec![(Bytes::from_static(b"r_key"), Bytes::from_static(b"r1"))]
    );
    clock.set(1200);
    assert_eq!(reader.get(b"r_key").await.unwrap(), None);
    assert!(collect(reader.scan(b"r_"..b"r`").await.unwrap())
        .await
        .is_empty());
    assert!(reader_cache.misses().is_empty());
    reader.close().await.unwrap();

    // A DbReader with a lagging local clock floors now_ms at manifest.last_l0_clock_tick after
    // polling, so it does not serve a row that the writer already flushed/expired at t = 1200.
    let lagging_clock = Arc::new(MockSystemClock::with_time(500));
    let lagging_cache = TestRowCache::new();
    let lagging_reader = DbReader::builder(path, Arc::clone(&store))
        .with_system_clock(lagging_clock.clone())
        .with_options(reader_options(50, Duration::from_secs(3600)))
        .with_row_cache(lagging_cache.clone())
        .build()
        .await
        .unwrap();
    let mut lagging_status = lagging_reader.subscribe();
    // Initial last_l0_clock_tick is 1100, so r_key (expire_ts = 1200) is still live.
    assert_eq!(
        lagging_reader.get(b"r_key").await.unwrap(),
        Some(Bytes::from_static(b"r1"))
    );
    // Writer writes z_tick2 at t = 1200 and flushes WAL so the reader replays it without L0
    // invalidation, then flushes the memtable to advance last_l0_clock_tick to 1200.
    db.put_with_options(b"z_tick2", b"t2", &no_expiry, &write_opts)
        .await
        .unwrap();
    db.flush().await.unwrap();
    lagging_clock.advance(Duration::from_millis(100)).await;
    caught_up(&mut lagging_status, &db).await;
    flush_memtable(&db).await;
    // Advance lagging_clock by only 100ms (to 700, still behind 1200) to fire a poll tick.
    lagging_clock.advance(Duration::from_millis(100)).await;
    caught_up(&mut lagging_status, &db).await;
    assert!(lagging_cache
        .events
        .lock()
        .unwrap()
        .invalidations
        .is_empty());
    assert_eq!(lagging_reader.get(b"r_key").await.unwrap(), None);
    assert!(lagging_cache.misses().is_empty());
    lagging_reader.close().await.unwrap();
    db.close().await.unwrap();

    // An operand-preserving cache uses now_ms to drop expired merge operands while keeping base.
    struct MergeTtlRowCache {
        versions: Mutex<BTreeMap<Bytes, Vec<RowEntry>>>,
        hits: AtomicU64,
    }

    impl RowCache for MergeTtlRowCache {
        fn write(&self, _seq: u64, entries: &[RowEntry]) {
            let mut versions = self.versions.lock().unwrap();
            for entry in entries {
                versions
                    .entry(entry.key.clone())
                    .or_default()
                    .push(entry.clone());
            }
        }

        fn get(&self, key: &[u8], seq: u64, now_ms: i64) -> GetOutcome {
            let versions = self.versions.lock().unwrap();
            let Some(history) = versions.get(key) else {
                return GetOutcome::Miss;
            };
            let mut operands = Vec::new();
            let mut base: Option<&RowEntry> = None;
            let mut latest_seq = 0;
            let mut latest_create_ts = 0;
            let mut min_expire_ts: Option<i64> = None;
            for entry in history.iter().rev() {
                if entry.seq > seq {
                    continue;
                }
                let expired = entry.expire_ts.is_some_and(|ts| ts <= now_ms);
                match &entry.value {
                    ValueDeletable::Merge(op) => {
                        if expired {
                            continue;
                        }
                        if latest_seq == 0 {
                            latest_seq = entry.seq;
                            latest_create_ts = entry.create_ts.unwrap_or(0);
                        }
                        min_expire_ts = match (min_expire_ts, entry.expire_ts) {
                            (Some(a), Some(b)) => Some(a.min(b)),
                            (a, b) => a.or(b),
                        };
                        operands.push(op.clone());
                    }
                    ValueDeletable::Value(_) => {
                        if !expired {
                            if latest_seq == 0 {
                                latest_seq = entry.seq;
                                latest_create_ts = entry.create_ts.unwrap_or(0);
                            }
                            min_expire_ts = match (min_expire_ts, entry.expire_ts) {
                                (Some(a), Some(b)) => Some(a.min(b)),
                                (a, b) => a.or(b),
                            };
                            base = Some(entry);
                        }
                        break;
                    }
                    ValueDeletable::Tombstone => break,
                }
            }
            self.hits.fetch_add(1, Ordering::Relaxed);
            let mut merged = match base {
                Some(RowEntry {
                    value: ValueDeletable::Value(v),
                    ..
                }) => Some(v.clone()),
                _ => None,
            };
            for op in operands.into_iter().rev() {
                merged = Some(
                    ConcatMergeOperator
                        .merge(&Bytes::copy_from_slice(key), merged, op)
                        .unwrap(),
                );
            }
            GetOutcome::Hit(merged.map(|value| {
                KeyValue::new(
                    Bytes::copy_from_slice(key),
                    value,
                    latest_seq,
                    latest_create_ts,
                    min_expire_ts,
                )
            }))
        }
    }

    let merge_clock = Arc::new(MockSystemClock::with_time(2000));
    let merge_cache = Arc::new(MergeTtlRowCache {
        versions: Mutex::default(),
        hits: AtomicU64::new(0),
    });
    let db = Db::builder("row_cache_merge_ttl", Arc::new(InMemory::new()))
        .with_settings(Settings {
            flush_interval: None,
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        })
        .with_system_clock(merge_clock.clone())
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .with_row_cache(merge_cache.clone())
        .build()
        .await
        .unwrap();

    db.put_with_options(b"m", b"base", &no_expiry, &write_opts)
        .await
        .unwrap();
    db.merge_with_options(
        b"m",
        b"+tmp",
        &MergeOptions {
            ttl: Ttl::ExpireAfterMillis(50),
        },
        &write_opts,
    )
    .await
    .unwrap();

    let kv = db.get_key_value(b"m").await.unwrap().unwrap();
    assert_eq!(kv.value, Bytes::from_static(b"base+tmp"));
    assert_eq!(kv.expire_ts, Some(2050));

    merge_clock.set(2050);
    let kv = db.get_key_value(b"m").await.unwrap().unwrap();
    assert_eq!(kv.value, Bytes::from_static(b"base"));
    assert_eq!(kv.expire_ts, None);
    assert_eq!(merge_cache.hits.load(Ordering::Relaxed), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_row_cache_disallows_segmented_custom_scheduler() {
    use slatedb::compactor::{CompactorBuilder, SizeTieredCompactionSchedulerSupplier};
    use slatedb::{PrefixExtractor, PrefixTarget};

    struct FixedThreeByteExtractor;

    impl PrefixExtractor for FixedThreeByteExtractor {
        fn name(&self) -> &str {
            "fixed-3"
        }

        fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
            let bytes = match target {
                PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b,
            };
            (bytes.len() >= 3).then_some(3)
        }
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "row_cache_disallows_segmented_custom_scheduler";

    // Rejects with_row_cache + with_segment_extractor + embedded custom scheduler.
    let compactor = CompactorBuilder::new(path, Arc::clone(&store))
        .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier));
    let err = match Db::builder(path, Arc::clone(&store))
        .with_row_cache(TestRowCache::new())
        .with_segment_extractor(Arc::new(FixedThreeByteExtractor))
        .with_compactor_builder(compactor)
        .build()
        .await
    {
        Ok(_) => panic!("expected invalid configuration error"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::Invalid);

    // Rejects regardless of builder call order.
    let compactor = CompactorBuilder::new(path, Arc::clone(&store))
        .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier));
    let err = match Db::builder(path, Arc::clone(&store))
        .with_compactor_builder(compactor)
        .with_segment_extractor(Arc::new(FixedThreeByteExtractor))
        .with_row_cache(TestRowCache::new())
        .build()
        .await
    {
        Ok(_) => panic!("expected invalid configuration error"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::Invalid);

    // Allows with_row_cache + with_segment_extractor using the default scheduler.
    let compactor = CompactorBuilder::new(path, Arc::clone(&store));
    let db = Db::builder(path, Arc::clone(&store))
        .with_row_cache(TestRowCache::new())
        .with_segment_extractor(Arc::new(FixedThreeByteExtractor))
        .with_compactor_builder(compactor)
        .build()
        .await
        .unwrap();
    db.close().await.unwrap();

    // Allows with_row_cache + embedded custom scheduler when segments are not enabled.
    let unsegmented_path = "row_cache_allows_unsegmented_custom_scheduler";
    let compactor = CompactorBuilder::new(unsegmented_path, Arc::clone(&store))
        .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier));
    let db = Db::builder(unsegmented_path, Arc::clone(&store))
        .with_row_cache(TestRowCache::new())
        .with_compactor_builder(compactor)
        .build()
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_row_cache_persistent_open_reconciliation() {
    #[derive(Default)]
    struct PersistentState {
        watermark: u64,
        rows: BTreeMap<Bytes, KeyValue>,
        /// Ordered log of `("open", last_l0_seq)` and `("write", seq)` events.
        log: Vec<(&'static str, u64)>,
        misses: Vec<Bytes>,
    }

    struct PersistentRowCache {
        state: Mutex<PersistentState>,
    }

    impl PersistentRowCache {
        fn new_at(watermark: u64, initial_rows: &[(&[u8], &[u8], u64)]) -> Arc<Self> {
            let mut rows = BTreeMap::new();
            for &(k, v, seq) in initial_rows {
                rows.insert(
                    Bytes::copy_from_slice(k),
                    KeyValue::new(
                        Bytes::copy_from_slice(k),
                        Bytes::copy_from_slice(v),
                        seq,
                        0,
                        None,
                    ),
                );
            }
            Arc::new(Self {
                state: Mutex::new(PersistentState {
                    watermark,
                    rows,
                    log: Vec::new(),
                    misses: Vec::new(),
                }),
            })
        }

        fn take_misses(&self) -> Vec<Bytes> {
            std::mem::take(&mut self.state.lock().unwrap().misses)
        }
    }

    impl RowCache for PersistentRowCache {
        fn open(&self, last_l0_seq: u64) {
            let mut st = self.state.lock().unwrap();
            st.log.push(("open", last_l0_seq));
            if st.watermark < last_l0_seq {
                st.rows.clear();
                st.watermark = last_l0_seq;
            }
        }

        fn write(&self, seq: u64, entries: &[RowEntry]) {
            let mut st = self.state.lock().unwrap();
            st.log.push(("write", seq));
            if seq <= st.watermark {
                return;
            }
            for e in entries {
                match &e.value {
                    ValueDeletable::Value(v) => {
                        st.rows.insert(
                            e.key.clone(),
                            KeyValue::new(
                                e.key.clone(),
                                v.clone(),
                                seq,
                                e.create_ts.unwrap_or(0),
                                e.expire_ts,
                            ),
                        );
                    }
                    ValueDeletable::Tombstone | ValueDeletable::Merge(_) => {
                        st.rows.remove(&e.key);
                    }
                }
            }
            st.watermark = seq;
        }

        fn get(&self, key: &[u8], seq: u64, now_ms: i64) -> GetOutcome {
            let mut st = self.state.lock().unwrap();
            if let Some(kv) = st.rows.get(key) {
                if kv.seq <= seq {
                    if kv.expire_ts.is_some_and(|ts| ts <= now_ms) {
                        return GetOutcome::Hit(None);
                    }
                    return GetOutcome::Hit(Some(kv.clone()));
                }
            }
            st.misses.push(Bytes::copy_from_slice(key));
            GetOutcome::Miss
        }
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "row_cache_persistent_open_reconciliation";
    let settings = || Settings {
        flush_interval: None,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };

    // Seed: k1=v1_old (h1), k1=v1_new (h2), k2=v2 (h3) flushed to L0 (last_l0_seq=h3),
    // then k3=v3 (h4) left in WAL only.
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(settings())
        .build()
        .await
        .unwrap();
    let h1 = db.put(b"k1", b"v1_old").await.unwrap();
    let h2 = db.put(b"k1", b"v1_new").await.unwrap();
    let h3 = db.put(b"k2", b"v2").await.unwrap();
    flush_memtable(&db).await;
    let last_l0_seq = db.status().current_manifest.last_l0_seq();
    assert_eq!(last_l0_seq, h3.seqnum());

    let h4 = db.put(b"k3", b"v3").await.unwrap();
    db.close_with_options(CloseOptions {
        flush_type: Some(FlushType::Wal),
    })
    .await
    .unwrap();

    // 1. L0 gap + unflushed WAL on Db: open(h3) runs BEFORE write(h4), clearing stale k1
    // while keeping the newly replayed WAL entry k3 cached.
    let stale_cache = PersistentRowCache::new_at(h1.seqnum(), &[(b"k1", b"v1_old", h1.seqnum())]);
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(settings())
        .with_row_cache(stale_cache.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        stale_cache.state.lock().unwrap().log,
        vec![("open", h3.seqnum()), ("write", h4.seqnum())]
    );
    assert_eq!(
        db.get(b"k1").await.unwrap(),
        Some(Bytes::from_static(b"v1_new"))
    );
    assert_eq!(
        db.get(b"k2").await.unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(
        stale_cache.take_misses(),
        vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]
    );
    // k3 landed via WAL replay after open(h3) cleared the stale state, so it hits cache.
    assert_eq!(
        db.get(b"k3").await.unwrap(),
        Some(Bytes::from_static(b"v3"))
    );
    assert!(stale_cache.take_misses().is_empty());

    // Add one more WAL-only write (h5) and close with WAL flush only.
    let h5 = db.put(b"k4", b"v4").await.unwrap();
    db.close_with_options(CloseOptions {
        flush_type: Some(FlushType::Wal),
    })
    .await
    .unwrap();

    // 2. Caught-up persistent cache (watermark=h4 >= last_l0_seq=h3) keeps persisted rows,
    // skips overlapping WAL entry h4, and applies h5.
    let warm_cache = PersistentRowCache::new_at(
        h4.seqnum(),
        &[
            (b"k1", b"v1_new", h2.seqnum()),
            (b"k2", b"v2", h3.seqnum()),
            (b"k3", b"v3", h4.seqnum()),
        ],
    );
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(settings())
        .with_row_cache(warm_cache.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        warm_cache.state.lock().unwrap().log,
        vec![
            ("open", h3.seqnum()),
            ("write", h4.seqnum()),
            ("write", h5.seqnum()),
        ]
    );
    for (k, v) in [
        (&b"k1"[..], &b"v1_new"[..]),
        (b"k2", b"v2"),
        (b"k3", b"v3"),
        (b"k4", b"v4"),
    ] {
        assert_eq!(db.get(k).await.unwrap(), Some(Bytes::copy_from_slice(v)));
    }
    assert!(warm_cache.take_misses().is_empty());

    // 3. DbReader open reconciliation with WAL replay: open(h3) clears stale k1 before
    // replaying h4 and h5 into the cache.
    let reader_stale = PersistentRowCache::new_at(h1.seqnum(), &[(b"k1", b"v1_old", h1.seqnum())]);
    let reader = DbReader::builder(path, Arc::clone(&store))
        .with_options(reader_options(500, Duration::from_secs(60)))
        .with_row_cache(reader_stale.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        reader_stale.state.lock().unwrap().log,
        vec![
            ("open", h3.seqnum()),
            ("write", h4.seqnum()),
            ("write", h5.seqnum()),
        ]
    );
    assert_eq!(
        reader.get(b"k1").await.unwrap(),
        Some(Bytes::from_static(b"v1_new"))
    );
    assert_eq!(reader_stale.take_misses(), vec![Bytes::from_static(b"k1")]);
    assert_eq!(
        reader.get(b"k4").await.unwrap(),
        Some(Bytes::from_static(b"v4"))
    );
    assert!(reader_stale.take_misses().is_empty());
    reader.close().await.unwrap();

    // 4. DbReader with skip_wal_replay still invokes open(last_l0_seq) and clears stale state.
    let reader_skip = PersistentRowCache::new_at(h1.seqnum(), &[(b"k1", b"v1_old", h1.seqnum())]);
    let reader = DbReader::builder(path, Arc::clone(&store))
        .with_options(DbReaderOptions {
            skip_wal_replay: true,
            ..reader_options(500, Duration::from_secs(60))
        })
        .with_row_cache(reader_skip.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        reader_skip.state.lock().unwrap().log,
        vec![("open", h3.seqnum())]
    );
    assert_eq!(
        reader.get(b"k1").await.unwrap(),
        Some(Bytes::from_static(b"v1_new"))
    );
    assert_eq!(reader_skip.take_misses(), vec![Bytes::from_static(b"k1")]);
    reader.close().await.unwrap();

    // 5. Empty WAL + L0 gap on Db: open(last_l0_seq) still fires with zero write() calls.
    flush_memtable(&db).await;
    let final_l0_seq = db.status().current_manifest.last_l0_seq();
    assert_eq!(final_l0_seq, h5.seqnum());
    db.close().await.unwrap();

    let empty_wal_cache =
        PersistentRowCache::new_at(h1.seqnum(), &[(b"k1", b"v1_old", h1.seqnum())]);
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(settings())
        .with_row_cache(empty_wal_cache.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        empty_wal_cache.state.lock().unwrap().log,
        vec![("open", final_l0_seq)]
    );
    assert_eq!(
        db.get(b"k1").await.unwrap(),
        Some(Bytes::from_static(b"v1_new"))
    );
    assert_eq!(
        empty_wal_cache.take_misses(),
        vec![Bytes::from_static(b"k1")]
    );
    db.close().await.unwrap();
}
