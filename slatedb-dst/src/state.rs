//! SQLite-backed oracle state for deterministic simulation testing.
//!
//! The DST runner maintains an independent oracle alongside the real SlateDB
//! instance. Scenario operations append state transitions into this oracle, and
//! checked reads query it to answer a narrower question than the real database:
//! "given the writes we have recorded so far, what result should be visible at
//! this logical time and durability level?"
//!
//! SQLite is used here as a compact, deterministic state engine rather than as
//! a storage backend under test. Keeping the oracle in SQL makes it easy to:
//!
//! - record writes and flushes atomically;
//! - reconstruct the latest visible version for a key or key range;
//! - persist the resolved oracle state for post-run inspection.
//!
//! The schema is intentionally small:
//!
//! - `versions` stores append-only value and tombstone rows.
//! - `watermarks` stores the highest committed and durable sequence numbers.
//!
//! Visibility is derived from those tables with the same inputs a checked read
//! cares about:
//!
//! - sequence-number visibility based on [`DurabilityLevel`];
//! - expiration metadata recorded alongside each version row;
//! - tombstone suppression for deleted keys;
//! - newest-version-wins semantics for scans.
//!
//! The oracle does not currently synthesize the extra tombstones that mainline
//! SlateDB can create when expired values are rewritten during memtable flush.
//!
//! Public snapshot types such as [`OracleSnapshot`] expose the raw persisted
//! oracle state for inspection. Internal helper types model the intermediate
//! forms used while recording writes and resolving visibility.

use std::ops::Bound;

use rusqlite::{params, Connection};
use slatedb::config::DurabilityLevel;
use slatedb::{Error, IterationOrder};

use crate::error::DstError;

const VERSION_KIND_VALUE: i64 = 0;
const VERSION_KIND_TOMBSTONE: i64 = 1;

const RESET_SCHEMA_SQL: &str = "
DROP TABLE IF EXISTS versions;
DROP TABLE IF EXISTS watermarks;
PRAGMA user_version = 0;
CREATE TABLE versions (
    seq INTEGER NOT NULL,
    key BLOB NOT NULL,
    scenario TEXT NOT NULL,
    kind INTEGER NOT NULL,
    value BLOB NULL,
    create_ts INTEGER NOT NULL,
    expire_ts INTEGER NULL,
    PRIMARY KEY (seq, key)
);
CREATE TABLE watermarks (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    committed_seq INTEGER NOT NULL,
    durable_seq INTEGER NOT NULL
);
INSERT INTO watermarks (id, committed_seq, durable_seq) VALUES (1, 0, 0);
";

/// A raw version row captured in the oracle's `versions` table.
///
/// Each logical write performed by a scenario contributes one or more rows to
/// the append-only version history. These rows are intentionally stored before
/// visibility rules are applied. Consumers inspecting an [`OracleVersion`]
/// should treat it as historical state, not necessarily the value a checked
/// read would observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleVersion {
    /// Sequence number assigned to the write that produced this row.
    ///
    /// Visibility queries compare this against the current committed and/or
    /// durable watermark.
    pub seq: u64,
    /// User key for the versioned entry.
    ///
    /// Multiple rows with the same key may exist at different sequence numbers.
    pub key: Vec<u8>,
    /// Scenario name that generated the write.
    ///
    /// This is useful when debugging mismatches across concurrent scenarios.
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
    ///
    /// Checked reads on this branch surface the metadata but do not currently
    /// hide rows whose expiration time is in the past, and the oracle does not
    /// currently synthesize flush-time TTL tombstones.
    pub expire_ts: Option<i64>,
}

/// The sequence watermarks that bound oracle visibility.
///
/// The oracle tracks two notions of progress because DST reads can be filtered
/// by durability:
///
/// - committed visibility, which advances when a write is recorded;
/// - durable visibility, which advances only after a flush is observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleWatermarks {
    /// Highest sequence number known to be committed.
    ///
    /// This is advanced when the oracle records a write.
    pub committed_seq: u64,
    /// Highest sequence number known to be durable.
    ///
    /// This is advanced when the oracle records a flush.
    pub durable_seq: u64,
}

/// A full snapshot of the oracle's persisted state.
///
/// This is the main inspection surface exposed to crate users. It contains the
/// raw version history and the final visibility watermarks exactly as
/// persisted by the oracle. It is primarily useful for:
///
/// - post-run assertions in tests;
/// - debugging checked-read mismatches;
/// - inspecting the final logical state the oracle observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleSnapshot {
    /// All recorded version rows in `(seq, key)` order.
    ///
    /// This is historical state, so newer rows may shadow older rows for the
    /// same key.
    pub versions: Vec<OracleVersion>,
    /// Final committed and durable watermarks visible at snapshot time.
    pub watermarks: OracleWatermarks,
}

/// The key/value pair that should be visible for a checked read.
///
/// Unlike [`OracleVersion`], this is already a resolved read result. It
/// represents the newest visible non-tombstone row for a key after applying
/// sequence-number filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExpectedKeyValue {
    /// Key returned by the resolved read.
    pub key: Vec<u8>,
    /// Value returned by the resolved read.
    ///
    /// This is always present because tombstones are removed before this struct
    /// is constructed.
    pub value: Vec<u8>,
    /// Sequence number of the winning version row.
    pub seq: u64,
    /// Logical creation timestamp of the winning version row.
    pub create_ts: i64,
    /// Expiration timestamp carried by the winning version row, if any.
    pub expire_ts: Option<i64>,
}

/// The persisted kind of an oracle version row.
///
/// The discriminant values for this enum are encoded explicitly in SQLite and
/// should remain stable so snapshots and debug output stay interpretable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OracleVersionKind {
    /// A live value row that may be returned by checked reads if it survives
    /// sequence filtering and is not shadowed by a tombstone.
    Value,
    /// A delete marker that suppresses the key at this sequence number and
    /// hides older value rows until a newer value row appears.
    Tombstone,
}

impl OracleVersionKind {
    fn to_sql(self) -> i64 {
        match self {
            Self::Value => VERSION_KIND_VALUE,
            Self::Tombstone => VERSION_KIND_TOMBSTONE,
        }
    }
}

/// A version staged by a scenario write before it is recorded in SQLite.
///
/// This is the in-memory write representation used by [`Dst`](crate::Dst)
/// before a write is atomically inserted into the oracle together with its
/// corresponding watermark update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingVersion {
    /// Sequence number assigned by the underlying SlateDB write.
    pub seq: u64,
    /// User key affected by the write.
    pub key: Vec<u8>,
    /// Whether this staged row is a value or tombstone.
    pub kind: OracleVersionKind,
    /// Value payload for live rows, or `None` for tombstones.
    pub value: Option<Vec<u8>>,
    /// Logical creation timestamp captured for the write.
    pub create_ts: i64,
    /// Logical expiration timestamp derived from TTL, if any.
    pub expire_ts: Option<i64>,
}

/// Read-time visibility inputs used when deriving expected results.
///
/// The oracle does not implicitly consult the live database when resolving a
/// read. Instead, callers compute the relevant visibility inputs up front and
/// pass them through this struct so the oracle can deterministically replay the
/// same visibility rules that the checked read should respect.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OracleReadContext {
    /// Highest committed sequence known to the oracle.
    ///
    /// Rows above this watermark are invisible to all checked reads.
    pub committed_seq: u64,
    /// Highest durable sequence observed from the database status stream.
    ///
    /// This matters when the requested durability filter only exposes durable
    /// data.
    pub durable_seq: u64,
    /// Logical wall-clock time carried alongside checked reads.
    pub now: i64,
    /// Durability filter requested by the checked read.
    pub durability_filter: DurabilityLevel,
}

impl OracleReadContext {
    /// Returns the highest sequence number visible under the requested
    /// durability filter.
    fn max_visible_seq(self) -> u64 {
        match self.durability_filter {
            DurabilityLevel::Remote => self.committed_seq.min(self.durable_seq),
            DurabilityLevel::Memory => self.committed_seq,
            _ => self.committed_seq,
        }
    }
}

/// Raw row loaded from the `versions` table before visibility filtering.
///
/// This mirrors the subset of columns needed by `expected_get` and
/// `expected_scan`. It intentionally omits the `scenario` field because
/// scenario provenance is not needed once a row is being evaluated for
/// read-time visibility.
#[derive(Debug)]
struct VisibilityRow {
    seq: u64,
    key: Vec<u8>,
    kind: i64,
    value: Option<Vec<u8>>,
    create_ts: i64,
    expire_ts: Option<i64>,
}

/// SQLite-backed oracle used to reconstruct the state checked reads should
/// observe during DST runs.
///
/// The oracle owns a dedicated SQLite connection whose schema is reset on
/// construction. It acts as the append-only source of truth for expected-state
/// resolution and for post-run debugging via [`OracleSnapshot`]. All writes to
/// it are driven by DST wrapper operations rather than by SlateDB internals.
pub(crate) struct SQLiteOracle {
    conn: Connection,
}

impl SQLiteOracle {
    /// Opens the oracle database, falling back to an in-memory SQLite database
    /// when `path` is `None`, and resets the schema to a known empty state.
    pub(crate) fn new(path: Option<&'static str>) -> Result<Self, Error> {
        let path = path.unwrap_or(":memory:");
        let conn = Connection::open(path).map_err(DstError::SQLiteStateError)?;
        let mut oracle = Self { conn };
        oracle.init_schema()?;
        Ok(oracle)
    }

    fn init_schema(&mut self) -> Result<(), Error> {
        self.conn
            .execute_batch(RESET_SCHEMA_SQL)
            .map_err(DstError::SQLiteStateError)?;
        Ok(())
    }

    /// Records one logical write operation atomically.
    ///
    /// This inserts the supplied versions and advances the committed watermark
    /// in the same SQLite transaction.
    pub(crate) fn record_write(
        &mut self,
        versions: &[PendingVersion],
        committed_seq: u64,
        scenario: &str,
    ) -> Result<(), Error> {
        let tx = self
            .conn
            .transaction()
            .map_err(DstError::SQLiteStateError)?;
        for version in versions {
            tx.execute(
                "INSERT INTO versions (seq, key, scenario, kind, value, create_ts, expire_ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    version.seq as i64,
                    version.key,
                    scenario,
                    version.kind.to_sql(),
                    version.value,
                    version.create_ts,
                    version.expire_ts
                ],
            )
            .map_err(DstError::SQLiteStateError)?;
        }
        tx.execute(
            "UPDATE watermarks
             SET committed_seq = MAX(committed_seq, ?1)
             WHERE id = 1",
            params![committed_seq as i64],
        )
        .map_err(DstError::SQLiteStateError)?;
        tx.commit().map_err(DstError::SQLiteStateError)?;
        Ok(())
    }

    /// Records a flush by advancing the durable watermark.
    pub(crate) fn record_flush(&mut self, durable_seq: u64) -> Result<(), Error> {
        let tx = self
            .conn
            .transaction()
            .map_err(DstError::SQLiteStateError)?;
        tx.execute(
            "UPDATE watermarks
             SET durable_seq = MAX(durable_seq, ?1)
             WHERE id = 1",
            params![durable_seq as i64],
        )
        .map_err(DstError::SQLiteStateError)?;
        tx.commit().map_err(DstError::SQLiteStateError)?;
        Ok(())
    }

    /// Returns the oracle's latest committed and durable sequence watermarks.
    pub(crate) fn watermarks(&self) -> Result<OracleWatermarks, Error> {
        self.conn
            .query_row(
                "SELECT committed_seq, durable_seq FROM watermarks WHERE id = 1",
                (),
                |row| {
                    Ok(OracleWatermarks {
                        committed_seq: row.get::<_, i64>(0)? as u64,
                        durable_seq: row.get::<_, i64>(1)? as u64,
                    })
                },
            )
            .map_err(|err| DstError::SQLiteStateError(err).into())
    }

    /// Returns the latest visible value for `key` under the supplied read
    /// context.
    ///
    /// Tombstones and versions above the visible sequence watermark are
    /// filtered out. `expire_ts` is preserved as metadata only.
    pub(crate) fn expected_get(
        &self,
        key: &[u8],
        ctx: OracleReadContext,
    ) -> Result<Option<ExpectedKeyValue>, Error> {
        let max_visible_seq = ctx.max_visible_seq();
        let mut stmt = self
            .conn
            .prepare(
                "SELECT seq, key, kind, value, create_ts, expire_ts
                 FROM versions
                 WHERE key = ?1 AND seq <= ?2
                 ORDER BY seq DESC",
            )
            .map_err(DstError::SQLiteStateError)?;
        let mut rows = stmt
            .query(params![key, max_visible_seq as i64])
            .map_err(DstError::SQLiteStateError)?;

        if let Some(row) = rows.next().map_err(DstError::SQLiteStateError)? {
            let visible = VisibilityRow {
                seq: row.get::<_, i64>(0).map_err(DstError::SQLiteStateError)? as u64,
                key: row.get(1).map_err(DstError::SQLiteStateError)?,
                kind: row.get(2).map_err(DstError::SQLiteStateError)?,
                value: row.get(3).map_err(DstError::SQLiteStateError)?,
                create_ts: row.get(4).map_err(DstError::SQLiteStateError)?,
                expire_ts: row.get(5).map_err(DstError::SQLiteStateError)?,
            };
            return Ok(self.visible_row_to_key_value(Some(visible), ctx.now));
        }

        Ok(None)
    }

    /// Returns the visible key/value rows for the requested key range under the
    /// supplied read context.
    ///
    /// At most one version per key is returned, after applying sequence and
    /// tombstone filtering. `expire_ts` is preserved as metadata only.
    pub(crate) fn expected_scan(
        &self,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        ctx: OracleReadContext,
        order: IterationOrder,
    ) -> Result<Vec<ExpectedKeyValue>, Error> {
        let max_visible_seq = ctx.max_visible_seq();
        let mut stmt = self
            .conn
            .prepare(
                "SELECT seq, key, kind, value, create_ts, expire_ts
                 FROM versions
                 WHERE seq <= ?1
                 ORDER BY key ASC, seq DESC",
            )
            .map_err(DstError::SQLiteStateError)?;
        let mut rows = stmt
            .query(params![max_visible_seq as i64])
            .map_err(DstError::SQLiteStateError)?;

        let mut results = Vec::new();
        let mut current_key: Option<Vec<u8>> = None;

        while let Some(row) = rows.next().map_err(DstError::SQLiteStateError)? {
            let visible = VisibilityRow {
                seq: row.get::<_, i64>(0).map_err(DstError::SQLiteStateError)? as u64,
                key: row.get(1).map_err(DstError::SQLiteStateError)?,
                kind: row.get(2).map_err(DstError::SQLiteStateError)?,
                value: row.get(3).map_err(DstError::SQLiteStateError)?,
                create_ts: row.get(4).map_err(DstError::SQLiteStateError)?,
                expire_ts: row.get(5).map_err(DstError::SQLiteStateError)?,
            };

            if !range_contains(&visible.key, &start, &end) {
                continue;
            }

            if current_key.as_ref() == Some(&visible.key) {
                continue;
            }
            current_key = Some(visible.key.clone());

            if let Some(kv) = self.visible_row_to_key_value(Some(visible), ctx.now) {
                results.push(kv);
            }
        }

        if matches!(order, IterationOrder::Descending) {
            results.reverse();
        }

        Ok(results)
    }

    /// Returns a complete snapshot of the oracle tables for debugging and
    /// post-run inspection.
    pub(crate) fn snapshot(&self) -> Result<OracleSnapshot, Error> {
        let mut versions_stmt = self
            .conn
            .prepare(
                "SELECT seq, key, scenario, kind, value, create_ts, expire_ts
                 FROM versions
                 ORDER BY seq ASC, key ASC",
            )
            .map_err(DstError::SQLiteStateError)?;
        let versions = versions_stmt
            .query_map((), |row| {
                Ok(OracleVersion {
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

        let watermarks = self.watermarks()?;

        Ok(OracleSnapshot {
            versions,
            watermarks,
        })
    }

    fn visible_row_to_key_value(
        &self,
        row: Option<VisibilityRow>,
        now: i64,
    ) -> Option<ExpectedKeyValue> {
        let row = row?;
        if row.kind == VERSION_KIND_TOMBSTONE {
            return None;
        }
        let _ = now;
        // Newer main exposes expire_ts metadata on reads but does not currently
        // hide rows whose expire_ts is in the past, so the DST oracle mirrors
        // that behavior for checked get/scan validation.
        Some(ExpectedKeyValue {
            key: row.key,
            value: row.value.expect("value rows must include a value"),
            seq: row.seq,
            create_ts: row.create_ts,
            expire_ts: row.expire_ts,
        })
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
