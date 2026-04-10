//! SQLite-backed recorded state for deterministic simulation testing.
//!
//! DST keeps an independent SQLite database alongside the real SlateDB
//! instance. Mutation scenarios append the state they successfully wrote to the
//! real [`slatedb::Db`], and readers can later open point-in-time snapshots of
//! that recorded history with [`StateSnapshot`].
//!
//! SQLite is used here as a compact, deterministic state engine rather than as
//! a storage backend under test. Keeping the recorded state in SQL makes it
//! easy to:
//!
//! - record writes atomically;
//! - reconstruct the latest visible row for a key or key range at a fixed
//!   sequence number;
//! - persist the raw recorded history for post-run inspection.
//!
//! The schema is intentionally small:
//!
//! - `rows` stores append-only value and tombstone rows.

use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{params, Connection};
use slatedb::bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::{Error, IterationOrder, KeyValue, RowEntry, ValueDeletable};

use crate::error::DstError;

const ROW_KIND_VALUE: i64 = 0;
const ROW_KIND_TOMBSTONE: i64 = 1;

const RESET_SCHEMA_SQL: &str = "
DROP TABLE IF EXISTS rows;
DROP TABLE IF EXISTS watermarks;
PRAGMA user_version = 0;
CREATE TABLE rows (
    seq INTEGER NOT NULL,
    key BLOB NOT NULL,
    scenario TEXT NOT NULL,
    kind INTEGER NOT NULL,
    value BLOB NULL,
    create_ts INTEGER NOT NULL,
    expire_ts INTEGER NULL,
    PRIMARY KEY (seq, key)
);
";

/// A raw row captured in the recorded state's `rows` table.
///
/// Each logical write performed by a scenario contributes one or more rows to
/// the append-only row history. These rows are intentionally stored before
/// visibility rules are applied. Consumers inspecting a [`RecordedRow`]
/// should treat it as historical state, not necessarily the value a point-in-
/// time snapshot would observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedRow {
    /// Sequence number assigned to the write that produced this row.
    pub seq: u64,
    /// User key for the recorded entry.
    pub key: Vec<u8>,
    /// Scenario name that generated the write.
    pub scenario: String,
    /// Encoded row kind stored in SQLite.
    ///
    /// `0` means a value row and `1` means a tombstone row.
    pub kind: i64,
    /// Value payload for live rows.
    ///
    /// Tombstones store `None`.
    pub value: Option<Vec<u8>>,
    /// Logical creation timestamp captured from the mock clock when the write
    /// was issued.
    pub create_ts: i64,
    /// Logical expiration timestamp, if the value is subject to TTL.
    pub expire_ts: Option<i64>,
}

/// A full snapshot of the raw persisted SQLite state.
///
/// This is useful for post-run determinism checks and debugging because it
/// exposes the append-only row history exactly as recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedSnapshot {
    /// All recorded rows in `(seq, key)` order.
    pub rows: Vec<RecordedRow>,
}

/// A point-in-time view of the recorded SQLite state.
///
/// The caller chooses the visibility frontier up front via [`seq`](Self::seq).
/// Read methods then resolve keys exactly as SlateDB would at that sequence
/// number: newest visible row wins, tombstones hide older rows, and
/// scans return at most one visible row per key.
///
/// Methods intentionally mirror the `Db` read surface, but only
/// [`ScanOptions::order`] affects query shape. Durability and cache-related
/// options are assumed to have already been reflected in the chosen sequence
/// number.
#[derive(Clone)]
pub struct StateSnapshot {
    state: Arc<Mutex<SQLiteState>>,
    seq: u64,
}

impl StateSnapshot {
    pub(crate) fn new(state: Arc<Mutex<SQLiteState>>, seq: u64) -> Self {
        Self { state, seq }
    }

    /// Returns the maximum visible sequence number for this snapshot.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the value for `key` with default read options.
    pub fn get<K>(&self, key: K) -> Result<Option<Bytes>, Error>
    where
        K: AsRef<[u8]>,
    {
        self.get_with_options(key, &ReadOptions::default())
    }

    /// Returns the value for `key` with explicit read options.
    pub fn get_with_options<K>(&self, key: K, options: &ReadOptions) -> Result<Option<Bytes>, Error>
    where
        K: AsRef<[u8]>,
    {
        self.get_key_value_with_options(key, options)
            .map(|kv_opt| kv_opt.map(|kv| kv.value))
    }

    /// Returns the key/value row for `key` with default read options.
    pub fn get_key_value<K>(&self, key: K) -> Result<Option<KeyValue>, Error>
    where
        K: AsRef<[u8]>,
    {
        self.get_key_value_with_options(key, &ReadOptions::default())
    }

    /// Returns the key/value row for `key` with explicit read options.
    pub fn get_key_value_with_options<K>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, Error>
    where
        K: AsRef<[u8]>,
    {
        ensure_supported_snapshot_read(options.dirty)?;
        self.state
            .lock()
            .get_key_value_at_seq(key.as_ref(), self.seq)
    }

    /// Returns the visible rows in `range` with default scan options.
    pub fn scan<K, T>(&self, range: T) -> Result<Vec<KeyValue>, Error>
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>,
    {
        self.scan_with_options(range, &ScanOptions::default())
    }

    /// Returns the visible rows in `range` with explicit scan options.
    pub fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<Vec<KeyValue>, Error>
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>,
    {
        ensure_supported_snapshot_read(options.dirty)?;
        let (start, end) = owned_bounds(&range);
        self.state.lock().scan_key_values_at_seq(
            self.seq,
            ScanFilter::Range { start, end },
            options.order,
        )
    }

    /// Returns all visible rows whose keys begin with `prefix`.
    pub fn scan_prefix<P>(&self, prefix: P) -> Result<Vec<KeyValue>, Error>
    where
        P: AsRef<[u8]>,
    {
        self.scan_prefix_with_options(prefix, &ScanOptions::default())
    }

    /// Returns all visible rows whose keys begin with `prefix` with explicit
    /// scan options.
    pub fn scan_prefix_with_options<P>(
        &self,
        prefix: P,
        options: &ScanOptions,
    ) -> Result<Vec<KeyValue>, Error>
    where
        P: AsRef<[u8]>,
    {
        ensure_supported_snapshot_read(options.dirty)?;
        self.state.lock().scan_key_values_at_seq(
            self.seq,
            ScanFilter::Prefix(prefix.as_ref().to_vec()),
            options.order,
        )
    }
}

/// The persisted kind of a recorded row.
///
/// The discriminant values for this enum are encoded explicitly in SQLite and
/// should remain stable so snapshots and debug output stay interpretable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordedRowKind {
    /// A live value row that may be returned by point-in-time snapshots.
    Value,
    /// A delete marker that hides older value rows for the same key.
    Tombstone,
}

impl RecordedRowKind {
    fn to_sql(self) -> i64 {
        match self {
            Self::Value => ROW_KIND_VALUE,
            Self::Tombstone => ROW_KIND_TOMBSTONE,
        }
    }
}

/// A row staged by a scenario write before it is recorded in SQLite.
///
/// This is the in-memory write representation used by [`crate::Dst`] before a
/// write is atomically inserted into the recorded-state database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingRow {
    /// Sequence number assigned by the underlying SlateDB write.
    pub seq: u64,
    /// User key affected by the write.
    pub key: Vec<u8>,
    /// Whether this staged row is a value or tombstone.
    pub kind: RecordedRowKind,
    /// Value payload for live rows, or `None` for tombstones.
    pub value: Option<Vec<u8>>,
    /// Logical creation timestamp captured for the write.
    pub create_ts: i64,
    /// Logical expiration timestamp derived from TTL, if any.
    pub expire_ts: Option<i64>,
}

#[derive(Debug)]
struct VisibilityRow {
    seq: u64,
    key: Vec<u8>,
    kind: i64,
    value: Option<Vec<u8>>,
    create_ts: i64,
    expire_ts: Option<i64>,
}

impl VisibilityRow {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            seq: row.get::<_, i64>(0)? as u64,
            key: row.get(1)?,
            kind: row.get(2)?,
            value: row.get(3)?,
            create_ts: row.get(4)?,
            expire_ts: row.get(5)?,
        })
    }
}

#[derive(Debug, Clone)]
enum ScanFilter {
    Range {
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    },
    Prefix(Vec<u8>),
}

/// SQLite-backed append-only recorded state used by DST.
///
/// The state owns a dedicated SQLite connection whose schema is reset on
/// construction. All writes to it are driven by DST mutation helpers rather
/// than by SlateDB internals.
pub(crate) struct SQLiteState {
    conn: Connection,
}

impl SQLiteState {
    /// Opens the SQLite database, falling back to an in-memory database when
    /// `path` is `None`, and resets the schema to a known empty state.
    pub(crate) fn new(path: Option<&'static str>) -> Result<Self, Error> {
        let path = path.unwrap_or(":memory:");
        let conn = Connection::open(path).map_err(DstError::SQLiteStateError)?;
        let mut state = Self { conn };
        state.init_schema()?;
        Ok(state)
    }

    fn init_schema(&mut self) -> Result<(), Error> {
        self.conn
            .execute_batch(RESET_SCHEMA_SQL)
            .map_err(DstError::SQLiteStateError)?;
        Ok(())
    }

    /// Records one logical write operation atomically.
    ///
    /// This inserts the supplied rows in a single SQLite transaction.
    pub(crate) fn record_write(
        &mut self,
        rows: &[PendingRow],
        scenario: &str,
    ) -> Result<(), Error> {
        let tx = self
            .conn
            .transaction()
            .map_err(DstError::SQLiteStateError)?;
        for row in rows {
            tx.execute(
                "INSERT INTO rows (seq, key, scenario, kind, value, create_ts, expire_ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    row.seq as i64,
                    row.key,
                    scenario,
                    row.kind.to_sql(),
                    row.value,
                    row.create_ts,
                    row.expire_ts
                ],
            )
            .map_err(DstError::SQLiteStateError)?;
        }
        tx.commit().map_err(DstError::SQLiteStateError)?;
        Ok(())
    }

    /// Returns a complete snapshot of the persisted SQLite state.
    pub(crate) fn snapshot(&self) -> Result<RecordedSnapshot, Error> {
        let mut rows_stmt = self
            .conn
            .prepare(
                "SELECT seq, key, scenario, kind, value, create_ts, expire_ts
                 FROM rows
                 ORDER BY seq ASC, key ASC",
            )
            .map_err(DstError::SQLiteStateError)?;
        let rows = rows_stmt
            .query_map((), |row| {
                Ok(RecordedRow {
                    seq: row.get::<_, i64>(0)? as u64,
                    key: row.get(1)?,
                    scenario: row.get(2)?,
                    kind: row.get(3)?,
                    value: row.get(4)?,
                    create_ts: row.get(5)?,
                    expire_ts: row.get(6)?,
                })
            })
            .map_err(DstError::SQLiteStateError)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DstError::SQLiteStateError)?;

        Ok(RecordedSnapshot { rows })
    }

    fn get_key_value_at_seq(&self, key: &[u8], seq: u64) -> Result<Option<KeyValue>, Error> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT seq, key, kind, value, create_ts, expire_ts
                 FROM rows
                 WHERE key = ?1 AND seq <= ?2
                 ORDER BY seq DESC",
            )
            .map_err(DstError::SQLiteStateError)?;
        let mut rows = stmt
            .query(params![key, seq as i64])
            .map_err(DstError::SQLiteStateError)?;

        if let Some(row) = rows.next().map_err(DstError::SQLiteStateError)? {
            let visible = VisibilityRow::from_row(row).map_err(DstError::SQLiteStateError)?;
            return Ok(visible_row_to_key_value(visible));
        }

        Ok(None)
    }

    fn scan_key_values_at_seq(
        &self,
        seq: u64,
        filter: ScanFilter,
        order: IterationOrder,
    ) -> Result<Vec<KeyValue>, Error> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT seq, key, kind, value, create_ts, expire_ts
                 FROM rows
                 WHERE seq <= ?1
                 ORDER BY key ASC, seq DESC",
            )
            .map_err(DstError::SQLiteStateError)?;
        let mut rows = stmt
            .query(params![seq as i64])
            .map_err(DstError::SQLiteStateError)?;

        let mut results = Vec::new();
        let mut current_key: Option<Vec<u8>> = None;

        while let Some(row) = rows.next().map_err(DstError::SQLiteStateError)? {
            let visible = VisibilityRow::from_row(row).map_err(DstError::SQLiteStateError)?;
            if !scan_filter_contains(&visible.key, &filter) {
                continue;
            }
            if current_key.as_ref() == Some(&visible.key) {
                continue;
            }
            current_key = Some(visible.key.clone());

            if let Some(kv) = visible_row_to_key_value(visible) {
                results.push(kv);
            }
        }

        if matches!(order, IterationOrder::Descending) {
            results.reverse();
        }

        Ok(results)
    }
}

fn ensure_supported_snapshot_read(dirty: bool) -> Result<(), Error> {
    if dirty {
        return Err(Error::internal(
            "DST SQLite snapshots do not support dirty=true".to_string(),
        ));
    }
    Ok(())
}

fn visible_row_to_key_value(row: VisibilityRow) -> Option<KeyValue> {
    if row.kind == ROW_KIND_TOMBSTONE {
        return None;
    }

    Some(KeyValue::from(RowEntry {
        key: Bytes::from(row.key),
        value: ValueDeletable::Value(Bytes::from(
            row.value.expect("value rows must include a value"),
        )),
        seq: row.seq,
        create_ts: Some(row.create_ts),
        expire_ts: row.expire_ts,
    }))
}

fn scan_filter_contains(key: &[u8], filter: &ScanFilter) -> bool {
    match filter {
        ScanFilter::Range { start, end } => range_contains(key, start, end),
        ScanFilter::Prefix(prefix) => key.starts_with(prefix),
    }
}

fn range_contains(key: &[u8], start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    let start_ok = match start {
        Bound::Included(bound) => key >= bound.as_slice(),
        Bound::Excluded(bound) => key > bound.as_slice(),
        Bound::Unbounded => true,
    };
    let end_ok = match end {
        Bound::Included(bound) => key <= bound.as_slice(),
        Bound::Excluded(bound) => key < bound.as_slice(),
        Bound::Unbounded => true,
    };
    start_ok && end_ok
}

fn owned_bounds<K, T>(range: &T) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
where
    K: AsRef<[u8]>,
    T: RangeBounds<K>,
{
    let start = match range.start_bound() {
        Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match range.end_bound() {
        Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(seq: u64, key: &[u8], value: &[u8]) -> PendingRow {
        PendingRow {
            seq,
            key: key.to_vec(),
            kind: RecordedRowKind::Value,
            value: Some(value.to_vec()),
            create_ts: seq as i64,
            expire_ts: None,
        }
    }

    fn tombstone(seq: u64, key: &[u8]) -> PendingRow {
        PendingRow {
            seq,
            key: key.to_vec(),
            kind: RecordedRowKind::Tombstone,
            value: None,
            create_ts: seq as i64,
            expire_ts: None,
        }
    }

    fn expected_kv(seq: u64, key: &[u8], value: &[u8]) -> KeyValue {
        KeyValue::from(RowEntry {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq,
            create_ts: Some(seq as i64),
            expire_ts: None,
        })
    }

    #[test]
    fn should_read_point_in_time_values() {
        let state = Arc::new(Mutex::new(SQLiteState::new(None).unwrap()));
        {
            let mut state_guard = state.lock();
            state_guard
                .record_write(&[put(1, b"k", b"v1")], "writer")
                .unwrap();
            state_guard
                .record_write(&[put(2, b"k", b"v2")], "writer")
                .unwrap();
        }

        let snapshot_v1 = StateSnapshot::new(state.clone(), 1);
        let snapshot_v2 = StateSnapshot::new(state, 2);

        assert_eq!(
            snapshot_v1.get_key_value(b"k").unwrap(),
            Some(expected_kv(1, b"k", b"v1"))
        );
        assert_eq!(
            snapshot_v2.get_key_value(b"k").unwrap(),
            Some(expected_kv(2, b"k", b"v2"))
        );
    }

    #[test]
    fn should_hide_values_behind_tombstones() {
        let state = Arc::new(Mutex::new(SQLiteState::new(None).unwrap()));
        {
            let mut state_guard = state.lock();
            state_guard
                .record_write(&[put(1, b"k", b"v1")], "writer")
                .unwrap();
            state_guard
                .record_write(&[tombstone(2, b"k")], "writer")
                .unwrap();
        }

        assert_eq!(
            StateSnapshot::new(state.clone(), 1)
                .get_key_value(b"k")
                .unwrap(),
            Some(expected_kv(1, b"k", b"v1"))
        );
        assert_eq!(
            StateSnapshot::new(state, 2).get_key_value(b"k").unwrap(),
            None
        );
    }

    #[test]
    fn should_scan_visible_rows_in_requested_order() {
        let state = Arc::new(Mutex::new(SQLiteState::new(None).unwrap()));
        {
            let mut state_guard = state.lock();
            state_guard
                .record_write(&[put(1, b"a", b"v1"), put(1, b"b", b"v2")], "writer")
                .unwrap();
            state_guard
                .record_write(&[put(2, b"a", b"v3"), tombstone(2, b"b")], "writer")
                .unwrap();
        }

        let ascending = StateSnapshot::new(state.clone(), 2)
            .scan_with_options::<Vec<u8>, _>(
                vec![0x00]..vec![0xff],
                &ScanOptions::default().with_order(IterationOrder::Ascending),
            )
            .unwrap();
        let descending = StateSnapshot::new(state.clone(), 2)
            .scan_with_options::<Vec<u8>, _>(
                vec![0x00]..vec![0xff],
                &ScanOptions::default().with_order(IterationOrder::Descending),
            )
            .unwrap();
        let prefixed = StateSnapshot::new(state, 2)
            .scan_prefix_with_options(
                b"a",
                &ScanOptions::default().with_order(IterationOrder::Ascending),
            )
            .unwrap();

        assert_eq!(ascending, vec![expected_kv(2, b"a", b"v3")]);
        assert_eq!(descending, vec![expected_kv(2, b"a", b"v3")]);
        assert_eq!(prefixed, vec![expected_kv(2, b"a", b"v3")]);
    }
}
