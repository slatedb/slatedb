#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    BatchStep, GetOutcome, KeyRange, RowCache, ScanIterator, ScanOutcome, ScanSegment, SyncStep,
};
use slatedb::{
    Db, DbIterator, DbReader, DbReaderMode, DbStatus, Error, ErrorKind, IsolationLevel,
    IterationOrder, KeyValue, MergeOperator, MergeOperatorError, RowEntry, ValueDeletable,
    WriteBatch,
};
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

impl VecScanIterator {
    fn from_rows(rows: Vec<KeyValue>) -> Self {
        Self {
            rows,
            pos: 0,
            async_at: None,
            fail_at: None,
            async_calls: Arc::new(AtomicU64::new(0)),
        }
    }
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

    fn get(&self, key: &[u8], seq: u64) -> GetOutcome {
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
        match self.lookup_at(key, seq) {
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

    fn scan(&self, range: KeyRange<'_>, seq: u64, _prefix: Option<&[u8]>) -> ScanOutcome {
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

        fn get(&self, key: &[u8], seq: u64) -> GetOutcome {
            let rows = self.rows.lock().unwrap();
            match rows.get(key) {
                Some((entry_seq, kv)) if *entry_seq <= seq => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    GetOutcome::Hit(kv.clone())
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
    assert!(matches!(BatchStep::Done(0), BatchStep::Done(0)));
}

#[tokio::test]
async fn test_row_cache_scan_next_batch_async_and_invalidation() {
    struct CustomBatchCache {
        async_calls: Arc<AtomicU64>,
        fail_at: Option<usize>,
    }

    impl RowCache for CustomBatchCache {
        fn write(&self, _seq: u64, _entries: &[RowEntry]) {}

        fn get(&self, _key: &[u8], _seq: u64) -> GetOutcome {
            GetOutcome::Miss
        }

        fn scan(&self, _range: KeyRange<'_>, _seq: u64, _prefix: Option<&[u8]>) -> ScanOutcome {
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

        fn cached_rows(&self, start: &[u8], end: &[u8], seq: u64) -> Vec<KeyValue> {
            let versions = self.versions.lock().unwrap();
            let mut rows = Vec::new();
            for (_key, history) in versions.range::<[u8], _>(KeyRange {
                start: Bound::Included(start),
                end: Bound::Excluded(end),
            }) {
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

        fn get(&self, _key: &[u8], _seq: u64) -> GetOutcome {
            GetOutcome::Miss
        }

        fn scan(&self, range: KeyRange<'_>, seq: u64, _prefix: Option<&[u8]>) -> ScanOutcome {
            if self.return_empty.load(Ordering::Relaxed) {
                return ScanOutcome::Partial(Vec::new());
            }
            let fail_at = *self.fail_cached_at.lock().unwrap();
            let a_rows = self.cached_rows(b"a/", b"b/", seq);
            let c_rows = self.cached_rows(b"c/", b"d/", seq);
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
                        async_at: None,
                        fail_at,
                        async_calls: Arc::new(AtomicU64::new(0)),
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

    // Replay feeds one key-ordered batch per seq in ascending order.
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
