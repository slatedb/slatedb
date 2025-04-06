use crate::config::{Clock, PutOptions, WriteOptions};
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator, SeekToKey};
use crate::row_codec::SstRowCodecV0;
use crate::types::{KeyValue, RowAttributes, RowEntry, ValueDeletable};
use bytes::{BufMut, Bytes, BytesMut};
use fail_parallel::FailPointRegistry;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::{Rng, RngCore};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    order: IterationOrder,
}

impl TestIterator {
    pub(crate) fn new(order: IterationOrder) -> Self {
        Self {
            entries: VecDeque::new(),
            order,
        }
    }

    pub(crate) fn with_entry(mut self, key: &'static [u8], val: &'static [u8], seq: u64) -> Self {
        let entry = RowEntry::new_value(key, val, seq);
        self.entries.push_back(Ok(entry));
        self
    }
}

impl KeyValueIterator for TestIterator {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let next_entry = match self.order {
            Ascending => self.entries.pop_front(),
            Descending => self.entries.pop_back(),
        };

        next_entry.map_or(Ok(None), |e| match e {
            Ok(kv) => Ok(Some(kv)),
            Err(err) => Err(err),
        })
    }

    fn order(&self) -> IterationOrder {
        self.order
    }
}

impl SeekToKey for TestIterator {
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        while let Some(entry_result) = self.entries.front() {
            let entry = entry_result.clone()?;
            if self.order.precedes(entry.key.as_ref(), next_key) {
                self.entries.pop_front();
            } else {
                break;
            }
        }
        Ok(())
    }
}

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

impl Clock for TestClock {
    fn now(&self) -> i64 {
        self.ticker.load(Ordering::SeqCst)
    }
}

pub(crate) struct TokioClock {
    initial_ts: u128,
    initial_instant: tokio::time::Instant,
}

impl TokioClock {
    pub(crate) fn new() -> Self {
        let ts_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        Self {
            initial_ts: ts_millis,
            initial_instant: tokio::time::Instant::now(),
        }
    }
}

impl Clock for TokioClock {
    fn now(&self) -> i64 {
        let elapsed = tokio::time::Instant::now().duration_since(self.initial_instant);
        (self.initial_ts + elapsed.as_millis()) as i64
    }
}

pub(crate) fn gen_rand_bytes(n: usize) -> Bytes {
    let mut rng = rand::thread_rng();
    let random_bytes: Vec<u8> = (0..n).map(|_| rng.gen()).collect();
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
use crate::db_state::SsTableInfo;
use crate::iter::IterationOrder::Ascending;
use crate::iter::IterationOrder::Descending;
use crate::manifest::store::ManifestStore;
use crate::paths::PathResolver;
use crate::sst::SsTableFormat;
use crate::store_provider::StoreProvider;
use crate::tablestore::TableStore;
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
    ordering: IterationOrder,
) {
    let mut expected = table.range(range);
    loop {
        let expected_next = match ordering {
            Ascending => expected.next(),
            Descending => expected.next_back(),
        };
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
            Ascending => expected.next(),
            Descending => expected.next_back(),
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
) -> Result<(), SlateDBError> {
    let put_options = PutOptions::default();
    let write_options = WriteOptions { await_durable };

    let unsorted_table: HashMap<Bytes, Bytes> = table
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    for (key, value) in unsorted_table.iter() {
        db.put_with_options(key, value, &put_options, &write_options)
            .await?;
    }

    Ok(())
}

pub(crate) struct SstData {
    pub(crate) info: SsTableInfo,
    pub(crate) data: Bytes,
}

pub(crate) fn build_test_sst(format: &SsTableFormat, num_blocks: usize) -> SstData {
    let mut rng = rand::thread_rng();
    let mut keygen = OrderedBytesGenerator::new_with_suffix(&[], &[0u8; 16]);
    let mut encoded_sst_builder = format.table_builder();
    let mut blocks = 0;
    let mut data = BytesMut::with_capacity(num_blocks * (format.block_size + 1));
    while blocks < num_blocks {
        let k = keygen.next();
        let mut val = BytesMut::with_capacity(32);
        val.put_bytes(0u8, 32);
        rng.fill_bytes(&mut val);
        let row = RowEntry::new(k, ValueDeletable::Value(val.freeze()), 0u64, None, None);
        encoded_sst_builder.add(row).unwrap();
        if let Some(block) = encoded_sst_builder.next_block() {
            data.put(block);
            blocks += 1;
        }
    }
    let mut encoded_table = encoded_sst_builder.build().unwrap();
    while let Some(block) = encoded_table.unconsumed_blocks.pop_front() {
        data.put(block);
    }
    SstData {
        info: encoded_table.info,
        data: data.freeze(),
    }
}

pub(crate) struct TestStoreProvider {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) fp_registry: Arc<FailPointRegistry>,
    pub(crate) clock: Arc<dyn Clock + Send + Sync>,
    pub(crate) sst_format: SsTableFormat,
}

impl TestStoreProvider {
    pub(crate) fn new<P: Into<Path>>(path: P) -> Self {
        let path = path.into();
        let object_store = Arc::new(InMemory::new());
        let clock = Arc::new(TokioClock::new()) as Arc<dyn Clock + Send + Sync>;
        TestStoreProvider {
            object_store,
            path,
            fp_registry: Arc::new(FailPointRegistry::new()),
            clock,
            sst_format: SsTableFormat::default(),
        }
    }
}

impl StoreProvider for TestStoreProvider {
    fn table_store(&self) -> Arc<TableStore> {
        Arc::new(TableStore::new_with_fp_registry(
            Arc::clone(&self.object_store),
            self.sst_format.clone(),
            PathResolver::new(self.path.clone()),
            Arc::clone(&self.fp_registry),
            None,
        ))
    }

    fn manifest_store(&self) -> Arc<ManifestStore> {
        Arc::new(ManifestStore::new_with_clock(
            &self.path,
            Arc::clone(&self.object_store),
            Arc::clone(&self.clock),
        ))
    }
}
