use std::fmt;

use crate::{error::DstError, DstWriteOp};
use rusqlite::{Connection, OptionalExtension};
use slatedb::Error;

const CREATE_STATE_SQL: &str =
    "CREATE TABLE IF NOT EXISTS dst_state (key BLOB PRIMARY KEY, val BLOB)";
const INSERT_STATE_SQL: &str =
    "INSERT INTO dst_state (key, val) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET val = ?2";
const DELETE_STATE_SQL: &str = "DELETE FROM dst_state WHERE key = ?1";
const SELECT_STATE_SQL: &str = "SELECT val FROM dst_state WHERE key = ?1";
const SCAN_STATE_SQL: &str =
    "SELECT key, val FROM dst_state WHERE key >= ?1 AND key < ?2 ORDER BY key ASC";
const COUNT_STATE_SQL: &str = "SELECT COUNT(*) FROM dst_state";
const KEYS_STATE_SQL: &str = "SELECT key FROM dst_state";

/// A key-value pair in the DST state.
pub type StateKeyValue = (Vec<u8>, Vec<u8>);

/// A trait for the DST state that can be used to store and retrieve data.
pub trait State {
    fn write_batch(&mut self, batch: &[DstWriteOp]) -> Result<(), Error>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<StateKeyValue>, Error>;
    fn count(&self) -> Result<i64, Error>;
    fn keys(&self) -> Result<Vec<Vec<u8>>, Error>;
    fn is_empty(&self) -> bool;
}

/// A DST state that uses SQLite as the backend.
pub struct SQLiteState {
    conn: Connection,
}

impl SQLiteState {
    /// Create a new SQLite state.
    ///
    /// If `path` is `None`, the state will be stored in memory.
    pub fn new(path: Option<&'static str>) -> Result<Self, Error> {
        let path = path.unwrap_or(":memory:");
        let conn = Connection::open(path).map_err(DstError::SQLiteStateError)?;
        conn.execute(CREATE_STATE_SQL, ())
            .map_err(DstError::SQLiteStateError)?;
        Ok(Self { conn })
    }
}

impl State for SQLiteState {
    /// Write a batch of write operations to the state.
    ///
    /// The batch is a vector of tuples, where each tuple contains a key, a value.
    /// It performs an insert or update operation if the value is some, and a delete operation
    /// if the value is none.
    ///
    /// This function is transactional, applying all the write operations in the batch atomically.
    fn write_batch(&mut self, batch: &[DstWriteOp]) -> Result<(), Error> {
        let tx = self
            .conn
            .transaction()
            .map_err(DstError::SQLiteStateError)?;
        for (key, val, _) in batch {
            if let Some(val) = val {
                tx.execute(INSERT_STATE_SQL, (key, val))
                    .map_err(DstError::SQLiteStateError)?;
            } else {
                tx.execute(DELETE_STATE_SQL, (key,))
                    .map_err(DstError::SQLiteStateError)?;
            }
        }
        tx.commit().map_err(DstError::SQLiteStateError)?;
        Ok(())
    }

    /// Get a value from the state.
    /// It returns `None` if the key is not found.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let mut stmt = self
            .conn
            .prepare(SELECT_STATE_SQL)
            .map_err(DstError::SQLiteStateError)?;

        let value: Option<Vec<u8>> = stmt
            .query_row((key,), |row| row.get(0))
            .optional()
            .map_err(DstError::SQLiteStateError)?;
        Ok(value)
    }

    /// Scan the state for key-value pairs in the range [start_key, end_key).
    /// It returns an empty vector if the range is empty.
    fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<StateKeyValue>, Error> {
        let mut stmt = self
            .conn
            .prepare(SCAN_STATE_SQL)
            .map_err(DstError::SQLiteStateError)?;

        let mut rows = stmt
            .query((start_key, end_key))
            .map_err(DstError::SQLiteStateError)?;

        let mut results = Vec::new();
        while let Some(row) = rows.next().map_err(DstError::SQLiteStateError)? {
            let key: Vec<u8> = row.get(0).map_err(DstError::SQLiteStateError)?;
            let val: Vec<u8> = row.get(1).map_err(DstError::SQLiteStateError)?;
            results.push((key, val));
        }
        Ok(results)
    }

    /// Count the number of key-value pairs in the state.
    /// It returns 0 if the state is empty.
    fn count(&self) -> Result<i64, Error> {
        let mut stmt = self
            .conn
            .prepare(COUNT_STATE_SQL)
            .map_err(DstError::SQLiteStateError)?;

        let count: i64 = stmt
            .query_one((), |row| row.get(0))
            .map_err(DstError::SQLiteStateError)?;
        Ok(count)
    }

    /// Get all the keys in the state.
    /// It returns an empty vector if the state is empty.
    fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        let mut stmt = self
            .conn
            .prepare(KEYS_STATE_SQL)
            .map_err(DstError::SQLiteStateError)?;
        let mut rows = stmt.query(()).map_err(DstError::SQLiteStateError)?;

        let mut keys = Vec::new();
        while let Some(row) = rows.next().map_err(DstError::SQLiteStateError)? {
            let key: Vec<u8> = row.get(0).map_err(DstError::SQLiteStateError)?;
            keys.push(key);
        }
        Ok(keys)
    }

    /// Check if the state is empty.
    /// It returns `true` if there are no key-value pairs in the state.
    fn is_empty(&self) -> bool {
        self.count().unwrap_or(0) == 0
    }
}

impl fmt::Debug for SQLiteState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SQLiteState")
            .field("connection", &self.conn)
            .field("entries", &self.count().unwrap_or(0))
            .finish()
    }
}
