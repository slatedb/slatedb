use std::collections::VecDeque;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;

use crate::bytes_range::BytesRange;
use crate::db_iter::{apply_filters, DbRecencyIterator};
use crate::iter::IterationOrder;
use crate::mem_table::KVTable;

/// A point-in-time wrapper around one in-memory memtable returned by
/// [`Db::memtables`](crate::Db::memtables).
///
/// The wrapper exposes raw [`crate::types::RowEntry`] values from a single
/// active or immutable memtable. It does not merge, deduplicate, filter
/// tombstones, resolve merge operands, or read from SSTs.
pub struct DbMemtable {
    table: Arc<KVTable>,
    max_seq: u64,
}

impl DbMemtable {
    pub(crate) fn from_table(table: Arc<KVTable>) -> Option<Self> {
        let max_seq = table.last_seq()?;
        Some(Self { table, max_seq })
    }

    /// Get all raw entries for a key from this memtable snapshot.
    ///
    /// This method returns a [`DbRecencyIterator`] over the point range for
    /// `key`. The iterator includes every stored sequence number for the key,
    /// including tombstones and merge operands. Rows inserted after this
    /// `DbMemtable` was created are filtered out.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `DbRecencyIterator`: an iterator over the raw entries for `key`
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"key", b"old").await?;
    ///     db.put(b"key", b"new").await?;
    ///
    ///     let memtable = db.memtables().into_iter().next().unwrap();
    ///     let mut iter = memtable.get(b"key");
    ///     let entry = iter.next_entry().await?.unwrap();
    ///     assert_eq!(entry.key.as_ref(), b"key");
    ///     assert_eq!(entry.value.as_bytes().unwrap().as_ref(), b"new");
    ///     Ok(())
    /// }
    /// ```
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> DbRecencyIterator {
        let key = Bytes::copy_from_slice(key.as_ref());
        self.scan(key.clone()..=key)
    }

    /// Scan a range of raw entries from this memtable snapshot.
    ///
    /// The iterator walks only this memtable, in ascending key order. Rows
    /// inserted after this `DbMemtable` was created are filtered out.
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Returns
    /// - `DbRecencyIterator`: an iterator over raw entries in the range
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store).await?;
    ///     db.put(b"a", b"a_value").await?;
    ///     db.put(b"b", b"b_value").await?;
    ///     db.put(b"c", b"c_value").await?;
    ///
    ///     let memtable = db.memtables().into_iter().next().unwrap();
    ///     let mut iter = memtable.scan("a".."c");
    ///     let entry = iter.next_entry().await?.unwrap();
    ///     assert_eq!(entry.key.as_ref(), b"a");
    ///     assert_eq!(entry.value.as_bytes().unwrap().as_ref(), b"a_value");
    ///     Ok(())
    /// }
    /// ```
    pub fn scan<K, T>(&self, range: T) -> DbRecencyIterator
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>,
    {
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let range = BytesRange::from((start, end));
        let iter = self.table.range(range, IterationOrder::Ascending);
        let filtered = apply_filters(std::iter::once(iter), Some(self.max_seq));
        DbRecencyIterator::new(VecDeque::from(filtered))
    }
}
