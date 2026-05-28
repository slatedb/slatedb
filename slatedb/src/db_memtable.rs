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

    pub(crate) fn from_tables<I>(active: Arc<KVTable>, immutable: I) -> Vec<Self>
    where
        I: IntoIterator<Item = Arc<KVTable>>,
    {
        let mut memtables = Vec::new();
        if let Some(memtable) = Self::from_table(active) {
            memtables.push(memtable);
        }
        memtables.extend(immutable.into_iter().filter_map(Self::from_table));
        memtables
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RowEntry, ValueDeletable};

    fn table_with(rows: impl IntoIterator<Item = RowEntry>) -> Arc<KVTable> {
        let table = Arc::new(KVTable::new());
        for row in rows {
            table.put(row);
        }
        table
    }

    fn assert_value(entry: &RowEntry, expected: &[u8]) {
        match &entry.value {
            ValueDeletable::Value(v) => assert_eq!(v.as_ref(), expected),
            other => panic!("expected Value({expected:?}), got {other:?}"),
        }
    }

    async fn collect_entries(mut iter: DbRecencyIterator) -> Vec<RowEntry> {
        let mut entries = Vec::new();
        while let Some(entry) = iter.next_entry().await.unwrap() {
            entries.push(entry);
        }
        entries
    }

    #[test]
    fn test_from_table_filters_empty_table() {
        let table = Arc::new(KVTable::new());

        assert!(DbMemtable::from_table(table).is_none());
    }

    #[tokio::test]
    async fn test_get_returns_snapshot_versions_for_key() {
        let table = table_with([
            RowEntry::new_value(b"key", b"old", 1),
            RowEntry::new_value(b"key", b"new", 2),
        ]);
        let memtable = DbMemtable::from_table(table.clone()).unwrap();

        table.put(RowEntry::new_value(b"key", b"after_snapshot", 3));
        table.put(RowEntry::new_value(b"other", b"other_after_snapshot", 4));

        let entries = collect_entries(memtable.get(b"key")).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.as_ref(), b"key");
        assert_value(&entries[0], b"new");
        assert_eq!(entries[1].key.as_ref(), b"key");
        assert_value(&entries[1], b"old");
        assert!(entries[0].seq > entries[1].seq);
    }

    #[tokio::test]
    async fn test_scan_uses_range_and_snapshot_bound() {
        let table = table_with([
            RowEntry::new_value(b"a", b"va", 1),
            RowEntry::new_value(b"b", b"vb", 2),
            RowEntry::new_value(b"c", b"vc", 3),
        ]);
        let memtable = DbMemtable::from_table(table.clone()).unwrap();

        table.put(RowEntry::new_value(b"b", b"vb_after_snapshot", 4));
        table.put(RowEntry::new_value(b"d", b"vd_after_snapshot", 5));

        let entries =
            collect_entries(memtable.scan::<Vec<u8>, _>(b"b".to_vec()..=b"c".to_vec())).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.as_ref(), b"b");
        assert_value(&entries[0], b"vb");
        assert_eq!(entries[1].key.as_ref(), b"c");
        assert_value(&entries[1], b"vc");
    }

    #[tokio::test]
    async fn test_from_tables_filters_empty_and_preserves_newest_first_order() {
        let active = table_with([RowEntry::new_value(b"active", b"active_value", 3)]);
        let empty_imm = Arc::new(KVTable::new());
        let newest_imm = table_with([RowEntry::new_value(b"middle", b"middle_value", 2)]);
        let oldest_imm = table_with([RowEntry::new_value(b"old", b"old_value", 1)]);

        let memtables =
            DbMemtable::from_tables(active, [empty_imm, newest_imm, oldest_imm].into_iter());
        assert_eq!(memtables.len(), 3);

        let active_entries = collect_entries(memtables[0].scan::<Vec<u8>, _>(..)).await;
        assert_eq!(active_entries.len(), 1);
        assert_eq!(active_entries[0].key.as_ref(), b"active");
        assert_value(&active_entries[0], b"active_value");

        let newest_imm_entries = collect_entries(memtables[1].scan::<Vec<u8>, _>(..)).await;
        assert_eq!(newest_imm_entries.len(), 1);
        assert_eq!(newest_imm_entries[0].key.as_ref(), b"middle");
        assert_value(&newest_imm_entries[0], b"middle_value");

        let oldest_imm_entries = collect_entries(memtables[2].scan::<Vec<u8>, _>(..)).await;
        assert_eq!(oldest_imm_entries.len(), 1);
        assert_eq!(oldest_imm_entries[0].key.as_ref(), b"old");
        assert_value(&oldest_imm_entries[0], b"old_value");
    }
}
