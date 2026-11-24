#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU64;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use slatedb::config::{FlushOptions, FlushType, ReadOptions, ScanOptions};
use slatedb::db_stats::REQUEST_COUNT;
use slatedb::object_store::memory::InMemory;
use slatedb::row_cache::{
    BatchStatus, BatchStep, GetOutcome, Host, MissToken, OpenContext, RowCache, ScanIterator,
    ScanOutcome,
};
use slatedb::{
    Db, Error, ErrorKind, IsolationLevel, KeyValue, MergeOperator, MergeOperatorError, RowEntry,
    ValueDeletable,
};
use slatedb_common::metrics::{lookup_metric_with_labels, DefaultMetricsRecorder};
use tokio::sync::Notify;

#[derive(Default)]
struct Events {
    lifecycle: Vec<&'static str>,
    applied_batches: Vec<(u64, Vec<RowEntry>)>,
    drained_seqs: Vec<u64>,
    recorded_misses: Vec<(u64, Bytes)>,
}

struct TestRowCache {
    host: OnceLock<Host>,
    events: Mutex<Events>,
    versions: Mutex<BTreeMap<Bytes, Vec<RowEntry>>>,
    serve_reads: AtomicBool,
    sample_misses: AtomicBool,
    next_token: AtomicU64,
    reject_open: AtomicBool,
    fail_next_get: AtomicBool,
    drain_notify: Notify,
    wake_on_close: AtomicBool,
}

impl TestRowCache {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            host: OnceLock::new(),
            events: Mutex::new(Events::default()),
            versions: Mutex::new(BTreeMap::new()),
            serve_reads: AtomicBool::new(true),
            sample_misses: AtomicBool::new(false),
            next_token: AtomicU64::new(1),
            reject_open: AtomicBool::new(false),
            fail_next_get: AtomicBool::new(false),
            drain_notify: Notify::new(),
            wake_on_close: AtomicBool::new(false),
        })
    }

    fn host(&self) -> &Host {
        self.host.get().expect("row cache was opened")
    }

    fn next_token(&self) -> Option<MissToken> {
        if !self.sample_misses.load(Ordering::Relaxed) {
            return None;
        }
        let id = self.next_token.fetch_add(1, Ordering::Relaxed);
        NonZeroU64::new(id).map(MissToken)
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
    fn next_sync(&mut self) -> Option<Result<Option<KeyValue>, Error>> {
        if self.fail_at == Some(self.pos) {
            self.fail_at = None;
            return Some(Err(Error::data("injected scan failure".to_string())));
        }
        if self.async_at == Some(self.pos) {
            return None;
        }
        if self.pos >= self.rows.len() {
            return Some(Ok(None));
        }
        let kv = self.rows[self.pos].clone();
        self.pos += 1;
        Some(Ok(Some(kv)))
    }

    fn next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<KeyValue>, Error>> + Send + '_>> {
        Box::pin(async move {
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
        })
    }

    fn seek<'a>(
        &'a mut self,
        next_key: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            while self.pos < self.rows.len() && self.rows[self.pos].key.as_ref() < next_key {
                self.pos += 1;
            }
            Ok(())
        })
    }
}

impl RowCache for TestRowCache {
    fn open(&self, ctx: OpenContext) -> Result<(), Error> {
        self.events.lock().unwrap().lifecycle.push("open");
        if self.reject_open.load(Ordering::Relaxed) {
            return Err(Error::invalid("row cache rejected open".to_string()));
        }
        let _ = self.host.set(ctx.host);
        Ok(())
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.events.lock().unwrap().lifecycle.push("close");
            if self.wake_on_close.load(Ordering::Relaxed) {
                let notified = self.drain_notify.notified();
                assert!(
                    self.host().request_drain(),
                    "batch writer must still accept drain requests during RowCache::close"
                );
                notified.await;
            }
        })
    }

    fn apply_write(&self, entries: &[RowEntry], committed_seq: u64) {
        if let Some(first) = entries.first() {
            assert!(
                committed_seq < first.seq,
                "apply_write must run before committed_seq advances"
            );
        }
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
            .push((committed_seq, entries.to_vec()));
    }

    fn drain_writer(&self, committed_seq: u64) {
        self.events.lock().unwrap().drained_seqs.push(committed_seq);
        self.drain_notify.notify_waiters();
    }

    fn close_writer(&self) {
        self.events.lock().unwrap().lifecycle.push("close_writer");
    }

    fn get(&self, key: &[u8], options: &ReadOptions, at: Option<u64>) -> GetOutcome {
        if self.fail_next_get.swap(false, Ordering::Relaxed) {
            return GetOutcome::Hit(Err(Error::invalid("injected get error".to_string())));
        }
        if !self.serve_reads.load(Ordering::Relaxed) || options.dirty {
            return GetOutcome::Miss(self.next_token());
        }
        let seq = at
            .map(|at| at.min(self.host().last_committed_seq()))
            .unwrap_or_else(|| self.host().last_committed_seq());
        match self.lookup_at(key, seq) {
            Some(kv) => {
                self.host().record_get_requests(1);
                GetOutcome::Hit(Ok(kv))
            }
            None => GetOutcome::Miss(self.next_token()),
        }
    }

    fn scan(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        options: &ScanOptions,
        _prefix: Option<&[u8]>,
        at: Option<u64>,
    ) -> ScanOutcome {
        if !self.serve_reads.load(Ordering::Relaxed) || options.dirty {
            return ScanOutcome::Miss(self.next_token());
        }
        let seq = at
            .map(|at| at.min(self.host().last_committed_seq()))
            .unwrap_or_else(|| self.host().last_committed_seq());
        let versions = self.versions.lock().unwrap();
        let mut rows = Vec::new();
        for (key, history) in versions.iter() {
            let after_start = match range.0 {
                Bound::Unbounded => true,
                Bound::Included(s) => key.as_ref() >= s,
                Bound::Excluded(s) => key.as_ref() > s,
            };
            let before_end = match range.1 {
                Bound::Unbounded => true,
                Bound::Included(e) => key.as_ref() <= e,
                Bound::Excluded(e) => key.as_ref() < e,
            };
            if !after_start || !before_end {
                continue;
            }
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
        ScanOutcome::Hit(Ok(Box::new(VecScanIterator {
            rows,
            pos: 0,
            async_at: None,
            fail_at: None,
            async_calls: Arc::new(AtomicU64::new(0)),
        })))
    }

    fn record_miss(&self, token: MissToken, key: &[u8]) {
        self.events
            .lock()
            .unwrap()
            .recorded_misses
            .push((token.0.get(), Bytes::copy_from_slice(key)));
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
    cache.wake_on_close.store(true, Ordering::Relaxed);
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = Db::builder("/tmp/test_row_cache_lifecycle", Arc::new(InMemory::new()))
        .with_metrics_recorder(recorder.clone())
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .with_row_cache(cache.clone())
        .build()
        .await
        .unwrap();

    // Writes pass through apply_write strictly before committed_seq advances.
    db.put(b"a", b"1").await.unwrap();
    db.put(b"b", b"2").await.unwrap();
    db.merge(b"m", b"part1").await.unwrap();
    assert_eq!(cache.events.lock().unwrap().applied_batches.len(), 3);

    // Idle writer drain runs at the current committed sequence number.
    let notified = cache.drain_notify.notified();
    assert!(cache.host().request_drain());
    notified.await;
    let committed = cache.host().last_committed_seq();
    assert_eq!(
        cache.events.lock().unwrap().drained_seqs.last().copied(),
        Some(committed)
    );

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
    // Overlapping scan bypasses cache to see txn writes; non-overlapping scan hits cache.
    let mut txn_scan = txn.scan(b"a"..b"c").await.unwrap();
    let mut txn_rows = Vec::new();
    txn_scan.next_batch(&mut txn_rows, 10).await.unwrap();
    assert_eq!(txn_rows.len(), 1);
    assert_eq!(txn_rows[0].key, Bytes::from_static(b"a"));
    assert_eq!(txn_rows[0].value, Bytes::from_static(b"txn_val"));
    txn.rollback();

    // Host::capture_state_view, is_memtable_current, retains_seq, scan_view, and next_entry.
    let view_before_flush = cache.host().capture_state_view().unwrap();
    assert!(cache.host().is_memtable_current(&view_before_flush));
    assert!(cache.host().retains_seq(&view_before_flush, snap_seq));

    // scan_view bypasses the row cache and preserves ValueDeletable::Merge on unbased merges.
    let mut raw = cache
        .host()
        .scan_view(
            &view_before_flush,
            b"m"..b"n",
            &ScanOptions::default(),
            None,
            None,
        )
        .await
        .unwrap();
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
    assert!(!cache.host().is_memtable_current(&view_before_flush));
    let view_after_flush = cache.host().capture_state_view().unwrap();
    assert!(cache.host().retains_seq(&view_after_flush, snap_seq));
    drop(snap);

    let manifest = cache.host().manifest().unwrap();
    let l0 = manifest.l0().front().expect("flushed L0 SST");
    let sst_index = cache.host().read_sst_index(&l0.sst).await.unwrap();
    assert!(!sst_index.is_empty());

    // Sampled misses invoke record_miss for both gets and scans.
    cache.serve_reads.store(false, Ordering::Relaxed);
    cache.sample_misses.store(true, Ordering::Relaxed);
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from_static(b"2")));
    let _ = db.scan(b"b"..b"z").await.unwrap();
    let misses = cache.events.lock().unwrap().recorded_misses.clone();
    assert_eq!(misses.len(), 2);
    assert_eq!(misses[0].1, Bytes::from_static(b"a"));
    assert_eq!(misses[1].1, Bytes::from_static(b"b"));

    // Close runs RowCache::close before stopping the batch writer (close_writer).
    db.close().await.unwrap();
    assert_eq!(
        cache.events.lock().unwrap().lifecycle,
        vec!["open", "close", "close_writer"]
    );
    assert!(cache.host().check_closed().is_err());

    // Dropping Db breaks the weak reference in Host (no Arc cycle).
    drop(db);
    assert!(cache.host().capture_state_view().is_err());
}

#[tokio::test]
async fn test_row_cache_open_rejection_cleans_up_without_calling_close() {
    let cache = TestRowCache::new();
    cache.reject_open.store(true, Ordering::Relaxed);
    let err = Db::builder("/tmp/test_row_cache_reject_open", Arc::new(InMemory::new()))
        .with_row_cache(cache.clone())
        .build()
        .await
        .err()
        .expect("open rejection must fail build");
    assert_eq!(err.kind(), ErrorKind::Invalid);
    let lifecycle = cache.events.lock().unwrap().lifecycle.clone();
    assert_eq!(lifecycle[0], "open");
    assert!(!lifecycle.contains(&"close"));
}

#[tokio::test]
async fn test_row_cache_scan_next_batch_async_gap_seek_and_invalidation() {
    struct CustomBatchCache {
        async_calls: Arc<AtomicU64>,
        fail_at: Option<usize>,
    }

    impl RowCache for CustomBatchCache {
        fn open(&self, _ctx: OpenContext) -> Result<(), Error> {
            Ok(())
        }
        fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }
        fn apply_write(&self, _entries: &[RowEntry], _committed_seq: u64) {}
        fn drain_writer(&self, _committed_seq: u64) {}
        fn close_writer(&self) {}
        fn get(&self, _key: &[u8], _options: &ReadOptions, _at: Option<u64>) -> GetOutcome {
            GetOutcome::Miss(None)
        }
        fn scan(
            &self,
            _range: (Bound<&[u8]>, Bound<&[u8]>),
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
            ScanOutcome::Hit(Ok(Box::new(VecScanIterator {
                rows,
                pos: 0,
                async_at: Some(2),
                fail_at: self.fail_at,
                async_calls: self.async_calls.clone(),
            })))
        }
        fn record_miss(&self, _token: MissToken, _key: &[u8]) {}
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

    // Verify BatchStep / BatchStatus are publicly constructible for custom overrides.
    let step = BatchStep {
        count: 0,
        status: BatchStatus::Done,
    };
    assert_eq!(step.count, 0);
}
