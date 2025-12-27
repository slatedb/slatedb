#![allow(unused)]

//! Lock-free, arena-allocated SkipMap implementation.
//!
//! This module provides a [`SkipMap`] implementation optimized for SlateDB's
//! memtable use case:
//!
//! ## Key Design Decisions
//!
//! ### Insert-Only / Append-Only Semantics
//!
//! In this SkipMap implementation, the skip list nodes are never deleted during
//! normal operation. This is well suited to SlateDB's MVCC use case, where we
//! maintain old versions of data for a given key and handle deletions as tombstone
//! insertions. Memory is reclaimed when the memtable is flushed to L0 and subsequently
//! dropped in its entirety.
//!
//! This removes the need for complex garbage collection mechanisms such as EBR
//! reclamation and eliminates a class of bugs that come from delete-related
//! race conditions.
//!
//! ### Arena Allocation for Nodes and Entries
//!
//! Both node structures and entry data (keys, values, metadata) are allocated from
//! an arena for efficient memory management. This provides:
//!
//! - Zero heap allocations per insert (just arena bump allocation)
//! - Excellent cache locality for sequential scans
//! - O(1) bulk deallocation when the SkipMap is dropped
//!
//! ### SequencedKey and RowEntry Types
//!
//! The SkipMap stores entries keyed by `SequencedKey` (user_key + sequence number)
//! with `RowEntry` values. This is specifically designed for SlateDB's MVCC memtable.
//!
//! This is modeled after RocksDB's InlineSkipList implementation
//! (see <https://github.com/facebook/rocksdb/blob/main/memtable/skiplist.h>)

mod arena;
mod iter;
mod list;
mod node;

use std::ops::RangeBounds;

pub use node::{Entry, ValueTag};

use crate::mem_table::SequencedKey;
use crate::types::RowEntry;
use iter::Range;
use list::SkipList;

/// A lock-free, arena-allocated skip map for SlateDB's memtable.
///
/// This is a concurrent ordered map with O(log n) insert and lookup operations.
/// It stores entries keyed by `SequencedKey` with `RowEntry` values.
///
/// # Thread Safety
///
/// `SkipMap` is both `Send` and `Sync`, allowing it to be shared across threads
/// safely. All operations are lock-free except for arena block allocation, which
/// uses a mutex that is only held briefly.
///
/// # Memory Management
///
/// Both node structures and entry data (keys, values, metadata) are allocated from
/// an internal arena. When the `SkipMap` is dropped, all memory is freed in O(1).
pub struct SkipMap {
    list: SkipList,
}

impl SkipMap {
    /// Create a new empty SkipMap.
    pub fn new() -> Self {
        Self::with_initial_allocated_size(0)
    }

    /// Create a new SkipMap with the specified initial arena allocation size.
    ///
    /// If size is 0, a default block size is used.
    /// Otherwise, the first arena block will be sized to the given value,
    /// with overflow blocks at 1/8 of that size.
    ///
    /// For optimal performance, set this to the expected memtable size
    /// (typically the L0 flush threshold).
    pub fn with_initial_allocated_size(size: usize) -> Self {
        Self {
            list: SkipList::with_initial_allocated_size(size),
        }
    }

    /// Insert a key-value pair, calling the compare function if key exists.
    ///
    /// The `compare_fn` receives a reference to the existing entry if an
    /// entry with the same key already exists. It returns `true` to proceed
    /// with insertion, `false` to skip.
    ///
    /// # Note
    ///
    /// Since this is an append-only skiplist, inserting a duplicate key adds
    /// a new node rather than replacing the old one. The new node will shadow
    /// the old one during lookups.
    pub fn compare_insert<F>(&self, key: SequencedKey, value: RowEntry, compare_fn: F)
    where
        F: Fn(&Entry<'_>) -> bool,
    {
        self.list.compare_insert(key, value, compare_fn);
    }

    /// Returns an iterator over entries in the given range.
    ///
    /// The iterator yields `Entry` views into the arena, providing zero-copy
    /// access to keys and values. It supports both forward and backward iteration
    /// via `next()` and `next_back()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Iterate forward
    /// let mut range = map.range(start..end);
    /// while let Some(entry) = range.next() {
    ///     println!("{:?}", entry.key_bytes());
    /// }
    ///
    /// // Iterate backward
    /// let mut range = map.range(start..end);
    /// while let Some(entry) = range.next_back() {
    ///     println!("{:?}", entry.key_bytes());
    /// }
    /// ```
    pub fn range<R>(&self, range: R) -> MapRange<'_, R>
    where
        R: RangeBounds<SequencedKey>,
    {
        MapRange {
            inner: Range::new(&self.list, range),
        }
    }

    /// Returns the number of entries in the map.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns `true` if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Returns the total bytes allocated by this map's arena.
    ///
    /// This includes all node and entry data stored in the arena.
    pub fn bytes_allocated(&self) -> usize {
        self.list.bytes_allocated()
    }

    /// Returns CAS contention statistics: (attempts, failures, failure_rate).
    ///
    /// Useful for diagnosing multi-threaded performance issues.
    pub fn cas_stats(&self) -> (usize, usize, f64) {
        self.list.cas_stats()
    }
}

impl Default for SkipMap {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: SkipMap is thread-safe because the underlying SkipList is thread-safe
unsafe impl Send for SkipMap {}
unsafe impl Sync for SkipMap {}

/// A range iterator over the map.
///
/// This iterator supports both forward and backward iteration and yields
/// `Entry` views that provide zero-copy access to the arena-allocated data.
pub struct MapRange<'a, R>
where
    R: RangeBounds<SequencedKey>,
{
    inner: Range<'a, R>,
}

impl<'a, R> MapRange<'a, R>
where
    R: RangeBounds<SequencedKey>,
{
    /// Returns the next entry in forward order.
    pub fn next(&mut self) -> Option<Entry<'a>> {
        self.inner.next()
    }

    /// Returns the next entry in backward order.
    pub fn next_back(&mut self) -> Option<Entry<'a>> {
        self.inner.next_back()
    }
}

// SAFETY: MapRange is Send because the underlying Range is Send
unsafe impl<R: RangeBounds<SequencedKey> + Send> Send for MapRange<'_, R> {}

// SAFETY: MapRange is Sync because the underlying Range is Sync
unsafe impl<R: RangeBounds<SequencedKey> + Sync> Sync for MapRange<'_, R> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ValueDeletable;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::thread;

    fn make_key(user_key: &[u8], seq: u64) -> SequencedKey {
        SequencedKey::new(Bytes::copy_from_slice(user_key), seq)
    }

    fn make_entry(user_key: &[u8], value: &[u8], seq: u64) -> RowEntry {
        RowEntry::new(
            Bytes::copy_from_slice(user_key),
            ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq,
            None,
            None,
        )
    }

    #[test]
    fn should_insert_and_iterate() {
        let map = SkipMap::new();

        map.compare_insert(make_key(b"ccc", 1), make_entry(b"ccc", b"3", 1), |_| true);
        map.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);
        map.compare_insert(make_key(b"bbb", 1), make_entry(b"bbb", b"2", 1), |_| true);

        assert_eq!(map.len(), 3);

        // Full range iteration
        let mut range = map.range(..);
        let mut keys = Vec::new();
        while let Some(entry) = range.next() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"aaa");
        assert_eq!(keys[1], b"bbb");
        assert_eq!(keys[2], b"ccc");
    }

    #[test]
    fn should_iterate_range_backward() {
        let map = SkipMap::new();

        map.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);
        map.compare_insert(make_key(b"bbb", 1), make_entry(b"bbb", b"2", 1), |_| true);
        map.compare_insert(make_key(b"ccc", 1), make_entry(b"ccc", b"3", 1), |_| true);

        let mut range = map.range(..);
        let mut keys = Vec::new();
        while let Some(entry) = range.next_back() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"ccc");
        assert_eq!(keys[1], b"bbb");
        assert_eq!(keys[2], b"aaa");
    }

    #[test]
    fn should_call_compare_fn() {
        let map = SkipMap::new();

        map.compare_insert(make_key(b"key", 1), make_entry(b"key", b"v1", 1), |_| true);

        // Try to insert with compare_fn returning false
        let called = std::cell::Cell::new(false);
        map.compare_insert(make_key(b"key", 1), make_entry(b"key", b"v2", 1), |prev| {
            called.set(true);
            assert_eq!(prev.value_bytes(), b"v1");
            false // Don't insert
        });

        assert!(called.get());
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn should_work_with_arc() {
        let map = Arc::new(SkipMap::new());

        let map_clone = map.clone();
        let handle = thread::spawn(move || {
            map_clone.compare_insert(make_key(b"key", 1), make_entry(b"key", b"value", 1), |_| {
                true
            });
        });

        handle.join().unwrap();
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn should_handle_concurrent_inserts() {
        let map = Arc::new(SkipMap::new());
        let num_threads = 4;
        let num_inserts = 500;

        let threads: Vec<_> = (0..num_threads)
            .map(|t| {
                let map = map.clone();
                thread::spawn(move || {
                    for i in 0..num_inserts {
                        let key = format!("key-{:02}-{:04}", t, i);
                        let key_bytes = key.as_bytes();
                        map.compare_insert(
                            make_key(key_bytes, 1),
                            make_entry(key_bytes, b"value", 1),
                            |_| true,
                        );
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(map.len(), num_threads * num_inserts);
    }

    #[test]
    fn should_report_cas_contention_stats() {
        for num_threads in [1, 2, 4, 8] {
            let map = Arc::new(SkipMap::new());
            let num_inserts = 10_000;

            let threads: Vec<_> = (0..num_threads)
                .map(|t| {
                    let map = map.clone();
                    thread::spawn(move || {
                        for i in 0..num_inserts {
                            let key = format!("key-{:02}-{:06}", t, i);
                            let key_bytes = key.as_bytes();
                            map.compare_insert(
                                make_key(key_bytes, 1),
                                make_entry(key_bytes, b"value", 1),
                                |_| true,
                            );
                        }
                    })
                })
                .collect();

            for t in threads {
                t.join().unwrap();
            }

            let (attempts, failures, rate) = map.cas_stats();
            eprintln!(
                "Threads: {} | CAS attempts: {} | failures: {} | failure rate: {:.2}%",
                num_threads,
                attempts,
                failures,
                rate * 100.0
            );

            assert_eq!(map.len(), num_threads * num_inserts);
        }
    }

    #[test]
    fn should_iterate_with_bounded_range() {
        let map = SkipMap::new();

        for i in 0..10 {
            let key = format!("key-{:02}", i);
            let key_bytes = key.as_bytes();
            map.compare_insert(
                make_key(key_bytes, 1),
                make_entry(key_bytes, &[i as u8], 1),
                |_| true,
            );
        }

        // Range from key-03 to key-07 (inclusive)
        // Using seq=MAX for start to include all versions, seq=0 for end to include all versions
        let mut range = map.range(make_key(b"key-03", u64::MAX)..=make_key(b"key-07", 0));
        let mut keys = Vec::new();
        while let Some(entry) = range.next() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 5);
        assert_eq!(keys[0], b"key-03");
        assert_eq!(keys[4], b"key-07");
    }

    #[test]
    fn should_track_bytes_allocated() {
        let map = SkipMap::new();

        assert!(map.bytes_allocated() > 0); // Head node allocated

        map.compare_insert(make_key(b"key", 1), make_entry(b"key", b"value", 1), |_| {
            true
        });

        // Should have allocated more bytes
        assert!(map.bytes_allocated() > 100);
    }

    #[test]
    fn should_order_same_key_by_seq_descending() {
        let map = SkipMap::new();

        // Insert same key with different sequence numbers
        map.compare_insert(make_key(b"key", 1), make_entry(b"key", b"v1", 1), |_| true);
        map.compare_insert(make_key(b"key", 3), make_entry(b"key", b"v3", 3), |_| true);
        map.compare_insert(make_key(b"key", 2), make_entry(b"key", b"v2", 2), |_| true);

        // Should be ordered by seq descending (3, 2, 1)
        let mut range = map.range(..);
        let mut seqs = Vec::new();
        while let Some(entry) = range.next() {
            seqs.push(entry.seq());
        }

        assert_eq!(seqs, vec![3, 2, 1]);
    }

    #[test]
    fn should_handle_tombstone() {
        let map = SkipMap::new();

        let entry = RowEntry::new(
            Bytes::from_static(b"key"),
            ValueDeletable::Tombstone,
            1,
            None,
            None,
        );
        map.compare_insert(make_key(b"key", 1), entry, |_| true);

        let mut range = map.range(..);
        let entry = range.next().unwrap();

        assert_eq!(entry.key_bytes(), b"key");
        assert_eq!(entry.value_bytes(), b"");
        assert_eq!(entry.value_tag(), super::node::ValueTag::Tombstone);
    }

    #[test]
    fn should_store_timestamps() {
        let map = SkipMap::new();

        let entry = RowEntry::new(
            Bytes::from_static(b"key"),
            ValueDeletable::Value(Bytes::from_static(b"value")),
            42,
            Some(100),
            Some(200),
        );
        map.compare_insert(make_key(b"key", 42), entry, |_| true);

        let mut range = map.range(..);
        let entry = range.next().unwrap();

        assert_eq!(entry.seq(), 42);
        assert_eq!(entry.create_ts(), Some(100));
        assert_eq!(entry.expire_ts(), Some(200));
    }
}
