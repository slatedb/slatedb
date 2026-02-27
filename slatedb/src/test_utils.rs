use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
use crate::compactor_state::{CompactionSpec, SourceId};
use crate::compactor_state_protocols::CompactorStateView;
use crate::config::{CompactorOptions, PutOptions, WriteOptions};
use crate::db_state::{SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::format::row::SstRowCodecV0;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::tablestore::TableStore;
use crate::types::{KeyValue, RowAttributes, RowEntry, ValueDeletable};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutOptions as OS_PutOptions,
    PutPayload, PutResult,
};
use rand::{Rng, RngCore};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Once;
use std::thread;
use std::time::Duration;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

/// Asserts that the iterator returns the exact set of expected values in correct order.
pub(crate) async fn assert_iterator<T: KeyValueIterator>(iterator: &mut T, entries: Vec<RowEntry>) {
    iterator
        .init()
        .await
        .expect("iterator init failed in assert_iterator");
    for expected_entry in entries.iter() {
        assert_next(iterator, expected_entry).await;
    }
    assert!(iterator
        .next()
        .await
        .expect("iterator next failed")
        .is_none());
}

pub(crate) async fn assert_next<T: KeyValueIterator>(iterator: &mut T, expected_entry: &RowEntry) {
    iterator
        .init()
        .await
        .expect("iterator init failed in assert_next");
    let actual_entry = iterator
        .next()
        .await
        .expect("iterator next failed")
        .expect("expected iterator to return a value");
    assert_eq!(actual_entry, expected_entry.clone())
}

pub(crate) fn assert_kv(kv: &KeyValue, key: &[u8], val: &[u8]) {
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
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
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
use crate::format::sst::{EncodedSsTable, SsTableFormat};
use crate::{MergeOperator, MergeOperatorError};
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
    iter: &mut DbIterator,
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
        let actual_next = iter.next().await.unwrap().map(KeyValue::from);
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

pub(crate) async fn build_test_sst(format: &SsTableFormat, num_blocks: usize) -> EncodedSsTable {
    let mut rng = rand::rng();
    let mut keygen = OrderedBytesGenerator::new_with_suffix(&[], &[0u8; 16]);
    let mut encoded_sst_builder = format.table_builder();
    while encoded_sst_builder.num_blocks() < num_blocks {
        let k = keygen.next();
        let mut val = BytesMut::with_capacity(32);
        val.put_bytes(0u8, 32);
        rng.fill_bytes(&mut val);
        let row = RowEntry::new(k, ValueDeletable::Value(val.freeze()), 0u64, None, None);
        encoded_sst_builder.add(row).await.unwrap();
    }
    encoded_sst_builder.build().await.unwrap()
}

/// Builds RowEntry values from (key, seq, value) tuples and sorts by key asc/seq desc.
pub(crate) fn build_row_entries(specs: Vec<(Bytes, u64, ValueDeletable)>) -> Vec<RowEntry> {
    let mut entries = specs
        .into_iter()
        .map(|(key, seq, value)| RowEntry::new(key, value, seq, None, None))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| match left.key.cmp(&right.key) {
        CmpOrdering::Equal => right.seq.cmp(&left.seq),
        other => other,
    });
    entries
}

/// Writes entries into SSTs, splitting when the accumulated block size exceeds `max_sst_size`.
pub(crate) async fn write_ssts(
    table_store: &Arc<TableStore>,
    entries: &[RowEntry],
    max_sst_size: usize,
) -> Vec<SsTableHandle> {
    if entries.is_empty() {
        return Vec::new();
    }

    let mut output_ssts = Vec::new();
    let mut writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
    let mut bytes_written = 0usize;

    for (index, entry) in entries.iter().cloned().enumerate() {
        if let Some(block_size) = writer.add(entry).await.unwrap() {
            bytes_written += block_size;
        }

        if bytes_written > max_sst_size {
            output_ssts.push(writer.close().await.unwrap());
            bytes_written = 0;

            if index + 1 < entries.len() {
                writer = table_store.table_writer(SsTableId::Compacted(Ulid::new()));
            } else {
                return output_ssts;
            }
        }
    }

    output_ssts.push(writer.close().await.unwrap());
    output_ssts
}

/// Builds SortedRun inputs from nested entry sets, writing each set as one or more SSTs.
pub(crate) async fn build_sorted_runs(
    table_store: &Arc<TableStore>,
    sr_entry_sets: &[Vec<Vec<RowEntry>>],
    max_sst_size: usize,
) -> Vec<SortedRun> {
    let mut sorted_runs = Vec::new();

    for (sr_id, sr_sst_sets) in sr_entry_sets.iter().enumerate() {
        let mut sr_ssts = Vec::new();
        for entries in sr_sst_sets {
            let ssts = write_ssts(table_store, entries, max_sst_size).await;
            sr_ssts.extend(ssts);
        }
        sorted_runs.push(SortedRun {
            id: sr_id as u32,
            ssts: sr_ssts,
        });
    }

    sorted_runs
}

/// A compactor that compacts if there are L0s and `should_compact` returns true.
/// All SSTs from L0 and all sorted runs are always compacted into sorted run 0.
#[derive(Clone)]
pub(crate) struct OnDemandCompactionScheduler {
    pub(crate) should_compact: Arc<dyn Fn(&CompactorStateView) -> bool + Send + Sync>,
}

impl OnDemandCompactionScheduler {
    fn new(should_compact: Arc<dyn Fn(&CompactorStateView) -> bool + Send + Sync>) -> Self {
        Self { should_compact }
    }
}

impl CompactionScheduler for OnDemandCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        if !(self.should_compact)(state) {
            return vec![];
        }

        let db_state = state.manifest();

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

        vec![CompactionSpec::new(sources, next_sr_id)]
    }
}

pub(crate) struct OnDemandCompactionSchedulerSupplier {
    pub(crate) scheduler: OnDemandCompactionScheduler,
}

impl OnDemandCompactionSchedulerSupplier {
    pub(crate) fn new(
        should_compact: Arc<dyn Fn(&CompactorStateView) -> bool + Send + Sync>,
    ) -> Self {
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
    // Put options: if set, write succeeds but returns AlreadyExists error
    // This simulates a timeout that occurs after the write completes but before the response
    put_succeeds_but_returns_already_exists: std::sync::atomic::AtomicBool,
    // Head: transient failures on first N attempts
    fail_first_head: AtomicUsize,
    head_attempts: AtomicUsize,
    // List: transient failures on first N `list` calls
    fail_first_list: AtomicUsize,
    // List: on a failing call, inject an error after this many successful items
    list_fail_after: AtomicUsize,
    // List: total number of `list` invocations
    list_attempts: AtomicUsize,
    // List with offset: transient failures on first N `list_with_offset` calls
    fail_first_list_with_offset: AtomicUsize,
    // List with offset: on a failing call, inject an error after this many successful items
    list_with_offset_fail_after: AtomicUsize,
    // List with offset: total number of `list_with_offset` invocations
    list_with_offset_attempts: AtomicUsize,
    // get_range: transient failures on first N attempts
    fail_first_get_range: AtomicUsize,
    get_range_attempts: AtomicUsize,
}

impl FlakyObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, fail_first_put_opts: usize) -> Self {
        Self {
            inner,
            fail_first_put_opts: AtomicUsize::new(fail_first_put_opts),
            put_opts_attempts: AtomicUsize::new(0),
            put_precondition_always: std::sync::atomic::AtomicBool::new(false),
            put_succeeds_but_returns_already_exists: std::sync::atomic::AtomicBool::new(false),
            fail_first_head: AtomicUsize::new(0),
            head_attempts: AtomicUsize::new(0),
            fail_first_list: AtomicUsize::new(0),
            list_fail_after: AtomicUsize::new(0),
            list_attempts: AtomicUsize::new(0),
            fail_first_list_with_offset: AtomicUsize::new(0),
            list_with_offset_fail_after: AtomicUsize::new(0),
            list_with_offset_attempts: AtomicUsize::new(0),
            fail_first_get_range: AtomicUsize::new(0),
            get_range_attempts: AtomicUsize::new(0),
        }
    }

    pub(crate) fn with_put_precondition_always(self) -> Self {
        self.put_precondition_always.store(true, Ordering::SeqCst);
        self
    }

    /// Configure the store to write data successfully but return an AlreadyExists error.
    /// This simulates a timeout that occurs after the write completes but before the response.
    pub(crate) fn with_put_succeeds_but_returns_already_exists(self) -> Self {
        self.put_succeeds_but_returns_already_exists
            .store(true, Ordering::SeqCst);
        self
    }

    pub(crate) fn with_head_failures(self, n: usize) -> Self {
        self.fail_first_head.store(n, Ordering::SeqCst);
        self
    }

    pub(crate) fn with_list_failures(self, fail_times: usize, fail_after: usize) -> Self {
        self.fail_first_list.store(fail_times, Ordering::SeqCst);
        self.list_fail_after.store(fail_after, Ordering::SeqCst);
        self
    }

    pub(crate) fn with_list_with_offset_failures(
        self,
        fail_times: usize,
        fail_after: usize,
    ) -> Self {
        self.fail_first_list_with_offset
            .store(fail_times, Ordering::SeqCst);
        self.list_with_offset_fail_after
            .store(fail_after, Ordering::SeqCst);
        self
    }

    pub(crate) fn put_attempts(&self) -> usize {
        self.put_opts_attempts.load(Ordering::SeqCst)
    }

    pub(crate) fn head_attempts(&self) -> usize {
        self.head_attempts.load(Ordering::SeqCst)
    }

    pub(crate) fn list_attempts(&self) -> usize {
        self.list_attempts.load(Ordering::SeqCst)
    }

    pub(crate) fn list_with_offset_attempts(&self) -> usize {
        self.list_with_offset_attempts.load(Ordering::SeqCst)
    }

    pub(crate) fn with_get_range_failures(self, n: usize) -> Self {
        self.fail_first_get_range.store(n, Ordering::SeqCst);
        self
    }

    pub(crate) fn get_range_attempts(&self) -> usize {
        self.get_range_attempts.load(Ordering::SeqCst)
    }

    /// Inject a failure after `fail_after` successful items in the stream.
    ///
    /// ## Args
    /// * `stream`: The stream to inject the failure into
    /// * `fail_after`: The number of successful items to yield before injecting the failure
    /// * `store_label`: The label to use in the error
    /// * `message`: The message to use in the error
    fn fail_stream(
        stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
        fail_after: usize,
        store_label: &'static str,
        message: &'static str,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        stream
            .take(fail_after)
            .chain(stream::once(async move {
                Err(object_store::Error::Generic {
                    store: store_label,
                    source: Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, message)),
                })
            }))
            .boxed()
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

    async fn get_range(
        &self,
        location: &Path,
        range: std::ops::Range<u64>,
    ) -> object_store::Result<Bytes> {
        self.get_range_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .fail_first_get_range
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
                store: "flaky_get_range",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "injected get_range timeout",
                )),
            });
        }
        self.inner.get_range(location, range).await
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
        // Simulate: write succeeds but returns AlreadyExists error (timeout after write)
        if self
            .put_succeeds_but_returns_already_exists
            .load(Ordering::SeqCst)
        {
            // Actually write the data
            let _ = self.inner.put_opts(location, payload, opts).await?;
            // But return an error as if we didn't get the response
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: Box::new(std::io::Error::other(
                    "injected already exists after successful write",
                )),
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
        self.list_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .fail_first_list
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v > 0 {
                    Some(v - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            let fail_after = self.list_fail_after.load(Ordering::SeqCst);
            return Self::fail_stream(
                self.inner.list(prefix),
                fail_after,
                "flaky_list",
                "injected list failure",
            );
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_with_offset_attempts
            .fetch_add(1, Ordering::SeqCst);
        if self
            .fail_first_list_with_offset
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v > 0 {
                    Some(v - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            let fail_after = self.list_with_offset_fail_after.load(Ordering::SeqCst);
            return Self::fail_stream(
                self.inner.list_with_offset(prefix, offset),
                fail_after,
                "flaky_list_with_offset",
                "injected list_with_offset failure",
            );
        }
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

pub(crate) struct StringConcatMergeOperator;

impl MergeOperator for StringConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut result = BytesMut::new();
        existing_value.inspect(|v| result.extend_from_slice(v.as_ref()));
        result.extend_from_slice(value.as_ref());
        Ok(result.freeze())
    }
}

/// Extract library name from a file path
/// e.g., "/Users/.../slatedb/src/db.rs" -> "slatedb"
/// e.g., "/.cargo/registry/.../parking_lot-0.12.1/src/mutex.rs" -> "parking_lot"
/// e.g., "/rustc/.../library/std/src/sync/mutex.rs" -> "std"
fn extract_library(path: &str) -> String {
    // Check for .cargo/registry paths (external crates)
    if let Some(idx) = path.find(".cargo/registry/") {
        let after_registry = &path[idx..];
        // Format: .cargo/registry/src/<hash>/<crate-name>-<version>/...
        // Split: [".cargo", "registry", "src", "<hash>", "<crate-name-version>", ...]
        if let Some(crate_start) = after_registry.split('/').nth(4) {
            // Remove version suffix (e.g., "parking_lot-0.12.1" -> "parking_lot")
            if let Some(dash_idx) = crate_start.rfind('-') {
                // Check if what follows the dash looks like a version
                let after_dash = &crate_start[dash_idx + 1..];
                if after_dash
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_digit())
                {
                    return crate_start[..dash_idx].to_string();
                }
            }
            return crate_start.to_string();
        }
    }

    // Check for rustc paths (std library)
    if path.contains("/rustc/") {
        if let Some(idx) = path.find("/library/") {
            let after_library = &path[idx + 9..];
            if let Some(lib_name) = after_library.split('/').next() {
                return lib_name.to_string();
            }
        }
    }

    // Check for local workspace crates (look for /src/ and take the directory before it)
    if let Some(src_idx) = path.rfind("/src/") {
        let before_src = &path[..src_idx];
        if let Some(last_slash) = before_src.rfind('/') {
            return before_src[last_slash + 1..].to_string();
        }
    }

    "-".to_string()
}

/// Format a backtrace as a table, showing all frames
pub(crate) fn format_backtrace(bt: &backtrace::Backtrace) -> String {
    // Collect frame info: (library, filename, line, function_name)
    let mut frames: Vec<(String, String, String, String)> = Vec::new();

    for frame in bt.frames() {
        for symbol in frame.symbols() {
            let name = symbol
                .name()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "<unknown>".to_string());

            let (library, filename, line) =
                if let (Some(f), Some(l)) = (symbol.filename(), symbol.lineno()) {
                    let path_str = f.to_string_lossy();
                    let lib = extract_library(&path_str);
                    let file = f
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| "-".to_string());
                    (lib, file, l.to_string())
                } else {
                    ("-".to_string(), "-".to_string(), "-".to_string())
                };

            frames.push((library, filename, line, name));
        }
    }

    if frames.is_empty() {
        return "  (no relevant frames found - try RUST_BACKTRACE=1)\n".to_string();
    }

    // Calculate column widths
    let lib_width = frames
        .iter()
        .map(|(l, _, _, _)| l.len())
        .max()
        .unwrap_or(0)
        .max(7); // min width for "Library"
    let file_width = frames
        .iter()
        .map(|(_, f, _, _)| f.len())
        .max()
        .unwrap_or(0)
        .max(4); // min width for "File"
    let line_width = frames
        .iter()
        .map(|(_, _, l, _)| l.len())
        .max()
        .unwrap_or(0)
        .max(4); // min width for "Line"
    let func_width = frames.iter().map(|(_, _, _, f)| f.len()).max().unwrap_or(0);

    let mut output = String::new();

    // Header: Library | File | Line | Function
    output.push_str(&format!(
        "  {:<lib_width$} | {:<file_width$} | {:>line_width$} | {:<func_width$}\n",
        "Library", "File", "Line", "Function"
    ));
    output.push_str(&format!(
        "  {:-<lib_width$}-+-{:-<file_width$}-+-{:->line_width$}-+-{:-<func_width$}\n",
        "", "", "", ""
    ));

    // Rows
    for (library, filename, line, func) in &frames {
        output.push_str(&format!(
            "  {:<lib_width$} | {:<file_width$} | {:>line_width$} | {:<func_width$}\n",
            library, filename, line, func
        ));
    }

    output
}

static INIT_LOGGING: Once = Once::new();
static INIT_DEADLOCK_DETECTOR: Once = Once::new();

/// Initialize tracing/logging for tests. Uses `RUST_LOG` environment
/// variable to set the log level, or defaults to `debug` if not set.
pub(crate) fn init_logging() {
    INIT_LOGGING.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_test_writer()
            .init();
    });
}

/// Start the deadlock detector thread that periodically checks for parking_lot deadlocks.
pub(crate) fn init_deadlock_detector() {
    INIT_DEADLOCK_DETECTOR.call_once(|| {
        thread::spawn(|| loop {
            thread::sleep(Duration::from_secs(10));
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            let separator = "=".repeat(60);
            eprintln!("\n{separator}");
            eprintln!("DEADLOCK DETECTED: {} deadlock(s) found", deadlocks.len());
            eprintln!("{separator}\n");

            for (i, threads) in deadlocks.iter().enumerate() {
                eprintln!("--- Deadlock #{} ({} threads) ---\n", i + 1, threads.len());
                for (j, t) in threads.iter().enumerate() {
                    eprintln!("Thread {} (id: {:?}):", j + 1, t.thread_id());
                    eprintln!("{}", format_backtrace(t.backtrace()));
                }
            }

            eprintln!("{separator}\n");
        });
    });
}

/// Initialize all test infrastructure (logging and deadlock detection).
pub(crate) fn init_test_infrastructure() {
    init_logging();
    init_deadlock_detector();
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    /// Test to verify the deadlock detector is working.
    /// This test intentionally creates a deadlock and is ignored by default.
    /// Run with: cargo test --package slatedb test_deadlock_detector -- --ignored --nocapture
    /// The deadlock detector should print deadlock info after ~10 seconds.
    #[test]
    #[ignore]
    fn test_deadlock_detector_should_detect_deadlock() {
        let lock_a = Arc::new(Mutex::new(()));
        let lock_b = Arc::new(Mutex::new(()));

        let lock_a_clone = lock_a.clone();
        let lock_b_clone = lock_b.clone();

        // Thread 1: acquire lock_a, then try to acquire lock_b
        let thread1 = thread::spawn(move || {
            let _guard_a = lock_a_clone.lock();
            thread::sleep(Duration::from_millis(100)); // Give thread2 time to acquire lock_b
            let _guard_b = lock_b_clone.lock(); // This will deadlock
        });

        // Thread 2: acquire lock_b, then try to acquire lock_a
        let thread2 = thread::spawn(move || {
            let _guard_b = lock_b.lock();
            thread::sleep(Duration::from_millis(100)); // Give thread1 time to acquire lock_a
            let _guard_a = lock_a.lock(); // This will deadlock
        });

        // Wait for the deadlock detector to report (it checks every 10 seconds)
        // This test will hang indefinitely due to the deadlock - that's expected.
        // The deadlock detector should print the deadlock info to the logs.
        thread1.join().unwrap();
        thread2.join().unwrap();
    }
}
