use crate::clock::LogicalClock;
use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
use crate::compactor_state::{Compaction, CompactionSpec, CompactorState, SourceId};
use crate::config::{CompactorOptions, PutOptions, WriteOptions};
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::row_codec::SstRowCodecV0;
use crate::types::{KeyValue, RowAttributes, RowEntry, ValueDeletable};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutOptions as OS_PutOptions,
    PutPayload, PutResult,
};
use rand::{Rng, RngCore};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Once};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

/// Asserts that the iterator returns the exact set of expected values in correct order.
pub(crate) async fn assert_iterator<T: KeyValueIterator>(iterator: &mut T, entries: Vec<RowEntry>) {
    for expected_entry in entries.iter() {
        assert_next_entry(iterator, expected_entry).await;
    }
    assert!(iterator
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .is_none());
}

pub(crate) async fn assert_next_entry<T: KeyValueIterator>(
    iterator: &mut T,
    expected_entry: &RowEntry,
) {
    let actual_entry = iterator
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .expect("expected iterator to return a value");
    assert_eq!(actual_entry, expected_entry.clone())
}

pub fn assert_kv(kv: &KeyValue, key: &[u8], val: &[u8]) {
    assert_eq!(kv.key, key);
    assert_eq!(kv.value, val);
}

pub(crate) fn gen_attrs(ts: i64) -> RowAttributes {
    RowAttributes {
        ts: Some(ts),
        expire_ts: None,
    }
}

pub(crate) fn gen_empty_attrs() -> RowAttributes {
    RowAttributes {
        ts: None,
        expire_ts: None,
    }
}

pub(crate) struct TestIterator {
    entries: VecDeque<Result<RowEntry, SlateDBError>>,
}

impl TestIterator {
    pub(crate) fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    pub(crate) fn with_entry(mut self, key: &'static [u8], val: &'static [u8], seq: u64) -> Self {
        let entry = RowEntry::new_value(key, val, seq);
        self.entries.push_back(Ok(entry));
        self
    }

    pub(crate) fn with_row_entry(mut self, entry: RowEntry) -> Self {
        self.entries.push_back(Ok(entry));
        self
    }
}

#[async_trait]
impl KeyValueIterator for TestIterator {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        self.entries.pop_front().map_or(Ok(None), |e| match e {
            Ok(kv) => Ok(Some(kv)),
            Err(err) => Err(err),
        })
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        while let Some(entry_result) = self.entries.front() {
            let entry = entry_result.clone()?;
            if entry.key < next_key {
                self.entries.pop_front();
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TestClock {
    pub(crate) ticker: AtomicI64,
}

impl TestClock {
    pub(crate) fn new() -> TestClock {
        TestClock {
            ticker: AtomicI64::new(0),
        }
    }

    pub(crate) fn set(&self, tick: i64) {
        self.ticker.store(tick, Ordering::SeqCst);
    }
}

impl LogicalClock for TestClock {
    fn now(&self) -> i64 {
        self.ticker.load(Ordering::SeqCst)
    }
}

pub(crate) fn gen_rand_bytes(n: usize) -> Bytes {
    let mut rng = rand::rng();
    let random_bytes: Vec<u8> = (0..n).map(|_| rng.random::<u8>()).collect();
    Bytes::from(random_bytes)
}

// it seems that insta still does not allow to customize the snapshot path in insta.yaml,
// we can remove this macro once insta supports it.
macro_rules! assert_debug_snapshot {
    ($name:expr, $output:expr) => {
        let mut settings = insta::Settings::clone_current();
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/snapshots");
        settings.set_snapshot_path(path);
        settings.bind(|| insta::assert_debug_snapshot!($name, $output));
    };
}

use crate::bytes_generator::OrderedBytesGenerator;
use crate::bytes_range::BytesRange;
use crate::db::Db;
use crate::db_iter::DbIterator;
use crate::sst::{EncodedSsTable, SsTableFormat};
pub(crate) use assert_debug_snapshot;

pub(crate) fn decode_codec_entries(
    data: Bytes,
    offsets: &[u16],
) -> Result<Vec<RowEntry>, SlateDBError> {
    let codec = SstRowCodecV0::new();
    let mut entries = Vec::new();
    let mut last_key = Bytes::new(); // Track the last full key

    for &offset in offsets {
        let mut cursor = data.slice(offset as usize..);
        let sst_row_entry = codec.decode(&mut cursor)?;

        let full_key = sst_row_entry.restore_full_key(&last_key);

        // Update last_key for the next entry
        last_key = full_key.clone();

        let row_entry = RowEntry {
            key: full_key,
            value: sst_row_entry.value,
            seq: sst_row_entry.seq,
            create_ts: sst_row_entry.create_ts,
            expire_ts: sst_row_entry.expire_ts,
        };
        entries.push(row_entry);
    }

    Ok(entries)
}

pub(crate) async fn assert_ranged_db_scan<T: RangeBounds<Bytes>>(
    table: &BTreeMap<Bytes, Bytes>,
    range: T,
    iter: &mut DbIterator<'_>,
) {
    let mut expected = table.range(range);
    loop {
        let expected_next = expected.next();
        let actual_next = iter.next().await.unwrap();
        if expected_next.is_none() && actual_next.is_none() {
            return;
        }
        assert_next_kv(expected_next, actual_next);
    }
}

pub(crate) async fn assert_ranged_kv_scan<T: KeyValueIterator>(
    table: &BTreeMap<Bytes, Bytes>,
    range: &BytesRange,
    ordering: IterationOrder,
    iter: &mut T,
) {
    let mut expected = table.range((range.start_bound().cloned(), range.end_bound().cloned()));

    loop {
        let expected_next = match ordering {
            IterationOrder::Ascending => expected.next(),
            IterationOrder::Descending => expected.next_back(),
        };
        let actual_next = iter.next().await.unwrap();
        if expected_next.is_none() && actual_next.is_none() {
            return;
        }

        assert_next_kv(expected_next, actual_next);
    }
}

fn assert_next_kv(expected: Option<(&Bytes, &Bytes)>, actual: Option<KeyValue>) {
    match (expected, actual) {
        (None, None) => (),
        (Some((expected_key, expected_value)), Some(actual)) => {
            assert_eq!(expected_key, &actual.key);
            assert_eq!(expected_value, &actual.value);
        }
        (Some(expected_record), None) => {
            panic!("Expected record {expected_record:?} missing from scan result")
        }
        (None, Some(actual)) => panic!("Unexpected record {actual:?} in scan result"),
    }
}

pub(crate) fn bound_as_option<T>(bound: Bound<&T>) -> Option<&T>
where
    T: ?Sized,
{
    match bound {
        Included(b) | Excluded(b) => Some(b),
        Unbounded => None,
    }
}

pub(crate) async fn seed_database(
    db: &Db,
    table: &BTreeMap<Bytes, Bytes>,
    await_durable: bool,
) -> Result<(), crate::Error> {
    let put_options = PutOptions::default();
    let write_options = WriteOptions { await_durable };

    for (key, value) in table.iter() {
        db.put_with_options(key, value, &put_options, &write_options)
            .await?;
    }

    Ok(())
}

pub(crate) fn build_test_sst(format: &SsTableFormat, num_blocks: usize) -> EncodedSsTable {
    let mut rng = rand::rng();
    let mut keygen = OrderedBytesGenerator::new_with_suffix(&[], &[0u8; 16]);
    let mut encoded_sst_builder = format.table_builder();
    while encoded_sst_builder.num_blocks() < num_blocks {
        let k = keygen.next();
        let mut val = BytesMut::with_capacity(32);
        val.put_bytes(0u8, 32);
        rng.fill_bytes(&mut val);
        let row = RowEntry::new(k, ValueDeletable::Value(val.freeze()), 0u64, None, None);
        encoded_sst_builder.add(row).unwrap();
    }
    encoded_sst_builder.build().unwrap()
}

/// A compactor that compacts if there are L0s and `should_compact` returns true.
/// All SSTs from L0 and all sorted runs are always compacted into sorted run 0.
#[derive(Clone)]
pub(crate) struct OnDemandCompactionScheduler {
    pub(crate) should_compact: Arc<dyn Fn(&CompactorState) -> bool + Send + Sync>,
}

impl OnDemandCompactionScheduler {
    fn new(should_compact: Arc<dyn Fn(&CompactorState) -> bool + Send + Sync>) -> Self {
        Self { should_compact }
    }
}

impl CompactionScheduler for OnDemandCompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction> {
        if !(self.should_compact)(state) {
            return vec![];
        }

        let db_state = state.db_state();

        // always compact into sorted run 0
        let next_sr_id = 0;

        // Create a compaction of all SSTs from L0 and all sorted runs
        let mut sources: Vec<SourceId> = db_state
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .collect();

        // Add SSTs from all sorted runs
        for sr in &db_state.compacted {
            sources.push(SourceId::SortedRun(sr.id));
        }

        let spec: CompactionSpec = CompactionSpec::SortedRunCompaction {
            ssts: Compaction::get_ssts(db_state, &sources),
            sorted_runs: Compaction::get_sorted_runs(db_state, &sources),
        };

        vec![Compaction::new(sources, spec, next_sr_id)]
    }
}

pub(crate) struct OnDemandCompactionSchedulerSupplier {
    pub(crate) scheduler: OnDemandCompactionScheduler,
}

impl OnDemandCompactionSchedulerSupplier {
    pub(crate) fn new(should_compact: Arc<dyn Fn(&CompactorState) -> bool + Send + Sync>) -> Self {
        Self {
            scheduler: OnDemandCompactionScheduler::new(should_compact),
        }
    }
}

impl CompactionSchedulerSupplier for OnDemandCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        _compactor_options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(self.scheduler.clone())
    }
}

// A flag so we only initialize logging once.
static INIT_LOGGING: Once = Once::new();

/// Initialize logging for tests so we get log output. Uses `RUST_LOG` environment
/// variable to set the log level, or defaults to `debug` if not set.
#[ctor::ctor]
fn init_tracing() {
    INIT_LOGGING.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_test_writer()
            .init();
    });
}

/// An ObjectStore wrapper that injects a transient timeout on the first N `put_opts` calls
/// to exercise retry logic. All operations delegate to the inner store otherwise.
#[derive(Debug)]
pub(crate) struct FlakyObjectStore {
    inner: Arc<dyn ObjectStore>,
    // Put options: transient failures on first N attempts
    fail_first_put_opts: AtomicUsize,
    put_opts_attempts: AtomicUsize,
    // Put options: if set, always return Precondition error (non-retryable)
    put_precondition_always: std::sync::atomic::AtomicBool,
    // Head: transient failures on first N attempts
    fail_first_head: AtomicUsize,
    head_attempts: AtomicUsize,
}

impl FlakyObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, fail_first_put_opts: usize) -> Self {
        Self {
            inner,
            fail_first_put_opts: AtomicUsize::new(fail_first_put_opts),
            put_opts_attempts: AtomicUsize::new(0),
            put_precondition_always: std::sync::atomic::AtomicBool::new(false),
            fail_first_head: AtomicUsize::new(0),
            head_attempts: AtomicUsize::new(0),
        }
    }

    pub(crate) fn with_put_precondition_always(self) -> Self {
        self.put_precondition_always.store(true, Ordering::SeqCst);
        self
    }

    pub(crate) fn with_head_failures(self, n: usize) -> Self {
        self.fail_first_head.store(n, Ordering::SeqCst);
        self
    }

    pub(crate) fn put_attempts(&self) -> usize {
        self.put_opts_attempts.load(Ordering::SeqCst)
    }

    pub(crate) fn head_attempts(&self) -> usize {
        self.head_attempts.load(Ordering::SeqCst)
    }
}

impl fmt::Display for FlakyObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlakyObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FlakyObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.head_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .fail_first_head
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v > 0 {
                    Some(v - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            return Err(object_store::Error::Generic {
                store: "flaky_head",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "injected head timeout",
                )),
            });
        }
        self.inner.head(location).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: OS_PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_opts_attempts.fetch_add(1, Ordering::SeqCst);
        if self.put_precondition_always.load(Ordering::SeqCst) {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: Box::new(std::io::Error::other("injected precondition")),
            });
        }
        if self
            .fail_first_put_opts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v > 0 {
                    Some(v - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            // Inject a timeout error wrapped in object_store::Error so retry logic triggers
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "injected timeout",
                )),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}
