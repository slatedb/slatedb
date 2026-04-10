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
//! - persist the recorded history that backs point-in-time reads.
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
    /// This inserts the supplied row entries in a single SQLite transaction.
    pub(crate) fn record_write(&mut self, rows: &[RowEntry], scenario: &str) -> Result<(), Error> {
        let tx = self
            .conn
            .transaction()
            .map_err(DstError::SQLiteStateError)?;
        for row in rows {
            let create_ts = row.create_ts.ok_or_else(|| {
                Error::internal("DST recorded state requires RowEntry.create_ts".to_string())
            })?;
            let (kind, value) = match &row.value {
                ValueDeletable::Value(value) => (ROW_KIND_VALUE, Some(value.to_vec())),
                ValueDeletable::Tombstone => (ROW_KIND_TOMBSTONE, None),
                ValueDeletable::Merge(_) => {
                    return Err(Error::internal(
                        "DST recorded state does not support merge rows".to_string(),
                    ));
                }
            };
            tx.execute(
                "INSERT INTO rows (seq, key, scenario, kind, value, create_ts, expire_ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    row.seq as i64,
                    row.key.to_vec(),
                    scenario,
                    kind,
                    value,
                    create_ts,
                    row.expire_ts
                ],
            )
            .map_err(DstError::SQLiteStateError)?;
        }
        tx.commit().map_err(DstError::SQLiteStateError)?;
        Ok(())
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
            let entry = row_entry_from_read_row(row).map_err(DstError::SQLiteStateError)?;
            return Ok(row_entry_to_key_value(entry));
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
        let mut current_key: Option<Bytes> = None;

        while let Some(row) = rows.next().map_err(DstError::SQLiteStateError)? {
            let entry = row_entry_from_read_row(row).map_err(DstError::SQLiteStateError)?;
            let matches_filter = match &filter {
                ScanFilter::Range { start, end } => {
                    let start_ok = match start {
                        Bound::Included(bound) => entry.key.as_ref() >= bound.as_slice(),
                        Bound::Excluded(bound) => entry.key.as_ref() > bound.as_slice(),
                        Bound::Unbounded => true,
                    };
                    let end_ok = match end {
                        Bound::Included(bound) => entry.key.as_ref() <= bound.as_slice(),
                        Bound::Excluded(bound) => entry.key.as_ref() < bound.as_slice(),
                        Bound::Unbounded => true,
                    };
                    start_ok && end_ok
                }
                ScanFilter::Prefix(prefix) => entry.key.starts_with(prefix),
            };
            if !matches_filter {
                continue;
            }
            if current_key.as_ref() == Some(&entry.key) {
                continue;
            }
            current_key = Some(entry.key.clone());

            if let Some(kv) = row_entry_to_key_value(entry) {
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

fn row_entry_from_read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RowEntry> {
    let kind = row.get::<_, i64>(2)?;
    let value = if kind == ROW_KIND_TOMBSTONE {
        ValueDeletable::Tombstone
    } else {
        ValueDeletable::Value(Bytes::from(
            row.get::<_, Option<Vec<u8>>>(3)?
                .expect("value rows must include a value"),
        ))
    };

    Ok(RowEntry {
        key: Bytes::from(row.get::<_, Vec<u8>>(1)?),
        value,
        seq: row.get::<_, i64>(0)? as u64,
        create_ts: Some(row.get(4)?),
        expire_ts: row.get(5)?,
    })
}

fn row_entry_to_key_value(row: RowEntry) -> Option<KeyValue> {
    if row.value.is_tombstone() {
        return None;
    }

    Some(KeyValue::from(row))
}
