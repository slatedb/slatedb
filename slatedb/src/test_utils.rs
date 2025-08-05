use crate::clock::LogicalClock;
use crate::compactor::{CompactionScheduler, CompactionSchedulerSupplier};
use crate::compactor_state::{Compaction, CompactorState, SourceId};
use crate::config::{CompactorOptions, PutOptions, WriteOptions};
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::row_codec::SstRowCodecV0;
use crate::types::{KeyValue, RowAttributes, RowEntry, ValueDeletable};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use rand::{Rng, RngCore};
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
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
}

impl LogicalClock for TestClock {
    fn now(&self) -> i64 {
        self.ticker.load(Ordering::SeqCst)
    }
}

pub(crate) fn gen_rand_bytes(n: usize) -> Bytes {
    let mut rng = rand::rng();
    let random_bytes: Vec<u8> = (0..n).map(|_| rng.random()).collect();
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

#[derive(Clone)]
pub(crate) struct OnDemandCompactionScheduler {
    pub(crate) should_compact: Arc<AtomicBool>,
}

impl OnDemandCompactionScheduler {
    fn new() -> Self {
        Self {
            should_compact: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl CompactionScheduler for OnDemandCompactionScheduler {
    fn maybe_schedule_compaction(&self, state: &CompactorState) -> Vec<Compaction> {
        if !self.should_compact.load(Ordering::SeqCst) {
            return vec![];
        }

        // this compactor will only compact if there are L0s,
        // it won't compact only lower levels for simplicity
        let db_state = state.db_state();
        if db_state.l0.is_empty() {
            return vec![];
        }

        self.should_compact.store(false, Ordering::SeqCst);

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

        vec![Compaction::new(sources, next_sr_id)]
    }
}

pub(crate) struct OnDemandCompactionSchedulerSupplier {
    pub(crate) scheduler: OnDemandCompactionScheduler,
}

impl OnDemandCompactionSchedulerSupplier {
    pub(crate) fn new() -> Self {
        Self {
            scheduler: OnDemandCompactionScheduler::new(),
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
