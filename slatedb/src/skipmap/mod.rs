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

    // ==================== Comprehensive Concurrent Tests ====================

    mod concurrent_tests {
        use super::*;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Barrier;

        /// Helper to collect all keys from a map
        fn collect_keys(map: &SkipMap) -> Vec<Vec<u8>> {
            let mut range = map.range(..);
            let mut keys = Vec::new();
            while let Some(entry) = range.next() {
                keys.push(entry.key_bytes().to_vec());
            }
            keys
        }

        // ==================== Category 1: Deterministic Concurrent Tests ====================

        #[test]
        fn should_handle_two_concurrent_inserts_at_same_position() {
            // Given: A skipmap with entries "aaa" and "ccc"
            let map = Arc::new(SkipMap::new());
            map.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);
            map.compare_insert(make_key(b"ccc", 1), make_entry(b"ccc", b"3", 1), |_| true);

            let barrier = Arc::new(Barrier::new(2));

            // When: Two threads simultaneously insert "bbb" with different sequences
            let handles: Vec<_> = (0..2)
                .map(|i| {
                    let map = map.clone();
                    let barrier = barrier.clone();
                    thread::spawn(move || {
                        barrier.wait();
                        map.compare_insert(
                            make_key(b"bbb", i as u64),
                            make_entry(b"bbb", &[i as u8], i as u64),
                            |_| true,
                        );
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Then: All 4 entries exist in correct order
            assert_eq!(map.len(), 4);
            let keys = collect_keys(&map);
            assert_eq!(keys[0], b"aaa");
            assert_eq!(keys[1], b"bbb");
            assert_eq!(keys[2], b"bbb");
            assert_eq!(keys[3], b"ccc");
        }

        #[test]
        fn should_maintain_mvcc_ordering_with_concurrent_sequence_inserts() {
            // Given: Empty skipmap
            let map = Arc::new(SkipMap::new());
            let num_threads = 8;
            let seqs_per_thread = 100;

            // When: Many threads insert same key with different sequences
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let map = map.clone();
                    thread::spawn(move || {
                        for i in 0..seqs_per_thread {
                            let seq = (t * seqs_per_thread + i) as u64;
                            map.compare_insert(
                                make_key(b"key", seq),
                                make_entry(b"key", &seq.to_le_bytes(), seq),
                                |_| true,
                            );
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Then: All entries present, ordered by descending sequence
            assert_eq!(map.len(), num_threads * seqs_per_thread);

            let mut prev_seq = u64::MAX;
            let mut range = map.range(..);
            while let Some(entry) = range.next() {
                assert!(
                    entry.seq() < prev_seq,
                    "MVCC ordering violated: {} should be < {}",
                    entry.seq(),
                    prev_seq
                );
                prev_seq = entry.seq();
            }
        }

        #[test]
        fn should_handle_concurrent_tall_node_insertions() {
            // Given: Empty skipmap
            // This test exercises the max_height CAS race condition
            let map = Arc::new(SkipMap::new());
            let num_threads = 8;
            let inserts_per_thread = 1000;

            // When: Many threads insert keys (some will have tall towers)
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let map = map.clone();
                    thread::spawn(move || {
                        for i in 0..inserts_per_thread {
                            let key = format!("key-{:02}-{:06}", t, i);
                            map.compare_insert(
                                make_key(key.as_bytes(), 1),
                                make_entry(key.as_bytes(), b"v", 1),
                                |_| true,
                            );
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Then: All entries present and sorted
            assert_eq!(map.len(), num_threads * inserts_per_thread);

            let mut prev_key: Option<Vec<u8>> = None;
            let mut range = map.range(..);
            while let Some(entry) = range.next() {
                if let Some(ref pk) = prev_key {
                    assert!(
                        entry.key_bytes() > pk.as_slice(),
                        "Order violated: {:?} should be > {:?}",
                        entry.key_bytes(),
                        pk
                    );
                }
                prev_key = Some(entry.key_bytes().to_vec());
            }
        }

        #[test]
        fn should_handle_interleaved_inserts_across_key_range() {
            // Given: Empty skipmap
            let map = Arc::new(SkipMap::new());
            let num_threads = 4;
            let inserts_per_thread = 1000;

            // When: Threads insert interleaved keys (thread 0 gets keys 0,4,8..., thread 1 gets 1,5,9...)
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let map = map.clone();
                    thread::spawn(move || {
                        for i in 0..inserts_per_thread {
                            let key_num = t + i * num_threads;
                            let key = format!("key-{:08}", key_num);
                            map.compare_insert(
                                make_key(key.as_bytes(), 1),
                                make_entry(key.as_bytes(), b"v", 1),
                                |_| true,
                            );
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Then: All entries present and sorted
            assert_eq!(map.len(), num_threads * inserts_per_thread);

            // Verify contiguous sorted order
            let mut range = map.range(..);
            let mut count = 0;
            while let Some(entry) = range.next() {
                let expected = format!("key-{:08}", count);
                assert_eq!(
                    entry.key_bytes(),
                    expected.as_bytes(),
                    "Key mismatch at position {}",
                    count
                );
                count += 1;
            }
            assert_eq!(count, num_threads * inserts_per_thread);
        }

        #[test]
        fn should_correctly_count_entries_under_concurrency() {
            // Given: Empty skipmap
            let map = Arc::new(SkipMap::new());
            let num_threads = 8;
            let inserts_per_thread = 500;

            // When: Many threads insert concurrently
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let map = map.clone();
                    thread::spawn(move || {
                        for i in 0..inserts_per_thread {
                            let key = format!("t{}-k{}", t, i);
                            map.compare_insert(
                                make_key(key.as_bytes(), 1),
                                make_entry(key.as_bytes(), b"v", 1),
                                |_| true,
                            );
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Then: len() should match actual count
            let expected = num_threads * inserts_per_thread;
            assert_eq!(map.len(), expected);

            // Also verify by iterating
            let mut range = map.range(..);
            let mut actual = 0;
            while range.next().is_some() {
                actual += 1;
            }
            assert_eq!(actual, expected);
        }

        // ==================== Category 2: High-Contention Stress Tests ====================

        #[test]
        fn should_handle_high_contention_compare_fn_at_single_key() {
            // Given: Empty skipmap
            let map = Arc::new(SkipMap::new());
            let compare_count = Arc::new(AtomicUsize::new(0));
            let num_threads = 16;
            let attempts_per_thread = 100;

            // When: Many threads try to insert the same key, only first succeeds
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let map = map.clone();
                    let compare_count = compare_count.clone();
                    thread::spawn(move || {
                        for _ in 0..attempts_per_thread {
                            map.compare_insert(
                                make_key(b"hotkey", 1),
                                make_entry(b"hotkey", b"v", 1),
                                |_existing| {
                                    compare_count.fetch_add(1, Ordering::Relaxed);
                                    false // Reject if exists
                                },
                            );
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Then: Only one entry exists
            assert_eq!(map.len(), 1);
            // And compare_fn was called for all subsequent attempts
            let comparisons = compare_count.load(Ordering::Relaxed);
            assert!(
                comparisons >= (num_threads * attempts_per_thread - 1),
                "Expected at least {} comparisons, got {}",
                num_threads * attempts_per_thread - 1,
                comparisons
            );
        }

        #[test]
        fn should_handle_burst_insertions_from_many_threads() {
            // Given: Empty skipmap
            let map = Arc::new(SkipMap::new());
            let num_threads = 16;
            let inserts_per_thread = 1000;
            let barrier = Arc::new(Barrier::new(num_threads));

            // When: All threads start simultaneously (burst pattern)
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let map = map.clone();
                    let barrier = barrier.clone();
                    thread::spawn(move || {
                        barrier.wait(); // Synchronized start
                        for i in 0..inserts_per_thread {
                            let key = format!("burst-{:02}-{:06}", t, i);
                            map.compare_insert(
                                make_key(key.as_bytes(), 1),
                                make_entry(key.as_bytes(), b"v", 1),
                                |_| true,
                            );
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Then: All entries present
            assert_eq!(map.len(), num_threads * inserts_per_thread);
        }

        #[test]
        fn should_maintain_consistency_with_concurrent_readers_and_writers() {
            // Given: Skipmap with initial data
            let map = Arc::new(SkipMap::new());

            // Pre-populate with 100 entries
            for i in 0..100 {
                let key = format!("{:08}", i);
                map.compare_insert(
                    make_key(key.as_bytes(), 1),
                    make_entry(key.as_bytes(), b"v", 1),
                    |_| true,
                );
            }

            let done = Arc::new(AtomicBool::new(false));

            // When: Writers add more entries while readers iterate
            let writer_handles: Vec<_> = (0..4)
                .map(|t| {
                    let map = map.clone();
                    let done = done.clone();
                    thread::spawn(move || {
                        let mut i = 0;
                        while !done.load(Ordering::Relaxed) {
                            let key = format!("writer-{}-{:08}", t, i);
                            map.compare_insert(
                                make_key(key.as_bytes(), 1),
                                make_entry(key.as_bytes(), b"v", 1),
                                |_| true,
                            );
                            i += 1;
                        }
                    })
                })
                .collect();

            // Readers check consistency
            let reader_handles: Vec<_> = (0..4)
                .map(|_| {
                    let map = map.clone();
                    let done = done.clone();
                    thread::spawn(move || {
                        let mut iterations = 0;
                        while !done.load(Ordering::Relaxed) && iterations < 100 {
                            // Each iteration should see sorted entries
                            let mut prev_key: Option<Vec<u8>> = None;
                            let mut range = map.range(..);
                            while let Some(entry) = range.next() {
                                if let Some(ref pk) = prev_key {
                                    assert!(
                                        entry.key_bytes() > pk.as_slice(),
                                        "Reader saw unsorted data"
                                    );
                                }
                                prev_key = Some(entry.key_bytes().to_vec());
                            }
                            iterations += 1;
                            thread::yield_now();
                        }
                    })
                })
                .collect();

            // Let it run for a bit
            thread::sleep(std::time::Duration::from_millis(100));
            done.store(true, Ordering::Relaxed);

            for h in writer_handles {
                h.join().unwrap();
            }
            for h in reader_handles {
                h.join().unwrap();
            }

            // Then: Map should have all initial entries plus writer entries
            assert!(map.len() >= 100);
        }

        // ==================== Category 3: Iterator Correctness Tests ====================

        #[test]
        fn should_see_consistent_prefix_during_concurrent_inserts() {
            // Given: Skipmap with initial entries
            let map = Arc::new(SkipMap::new());

            for i in 0..100 {
                let key = format!("{:04}", i);
                map.compare_insert(
                    make_key(key.as_bytes(), 1),
                    make_entry(key.as_bytes(), b"v", 1),
                    |_| true,
                );
            }

            let done = Arc::new(AtomicBool::new(false));

            // When: Writer adds new entries
            let writer = {
                let map = map.clone();
                let done = done.clone();
                thread::spawn(move || {
                    for i in 100..200 {
                        if done.load(Ordering::Relaxed) {
                            break;
                        }
                        let key = format!("{:04}", i);
                        map.compare_insert(
                            make_key(key.as_bytes(), 1),
                            make_entry(key.as_bytes(), b"v", 1),
                            |_| true,
                        );
                    }
                })
            };

            // Then: Reader sees monotonically increasing counts
            let mut seen_counts = Vec::new();
            for _ in 0..20 {
                let mut count = 0;
                let mut range = map.range(..);
                while range.next().is_some() {
                    count += 1;
                }
                seen_counts.push(count);
                thread::yield_now();
            }

            done.store(true, Ordering::Relaxed);
            writer.join().unwrap();

            // Counts should be non-decreasing
            for window in seen_counts.windows(2) {
                assert!(
                    window[0] <= window[1],
                    "Count decreased: {} -> {}",
                    window[0],
                    window[1]
                );
            }
        }

        #[test]
        fn should_handle_bidirectional_iteration_with_concurrent_inserts() {
            // Given: Skipmap with entries
            let map = Arc::new(SkipMap::new());

            for i in 0..50 {
                let key = format!("{:04}", i);
                map.compare_insert(
                    make_key(key.as_bytes(), 1),
                    make_entry(key.as_bytes(), b"v", 1),
                    |_| true,
                );
            }

            let done = Arc::new(AtomicBool::new(false));

            // Writer adds entries during iteration
            let writer = {
                let map = map.clone();
                let done = done.clone();
                thread::spawn(move || {
                    for i in 50..100 {
                        if done.load(Ordering::Relaxed) {
                            break;
                        }
                        let key = format!("{:04}", i);
                        map.compare_insert(
                            make_key(key.as_bytes(), 1),
                            make_entry(key.as_bytes(), b"v", 1),
                            |_| true,
                        );
                        thread::yield_now();
                    }
                })
            };

            // When: Reader does bidirectional iteration
            for _ in 0..10 {
                let mut range = map.range(..);
                let mut forward_keys = Vec::new();
                let mut backward_keys = Vec::new();

                // Interleave forward and backward
                for _ in 0..5 {
                    if let Some(entry) = range.next() {
                        forward_keys.push(entry.key_bytes().to_vec());
                    }
                    if let Some(entry) = range.next_back() {
                        backward_keys.push(entry.key_bytes().to_vec());
                    }
                }

                // Then: Forward keys should be ascending
                for window in forward_keys.windows(2) {
                    assert!(window[0] <= window[1], "Forward iteration not ascending");
                }

                // Backward keys should be descending
                for window in backward_keys.windows(2) {
                    assert!(window[0] >= window[1], "Backward iteration not descending");
                }
            }

            done.store(true, Ordering::Relaxed);
            writer.join().unwrap();
        }

        #[test]
        fn should_terminate_correctly_when_forward_and_backward_meet() {
            // Given: Skipmap with entries
            let map = SkipMap::new();

            for i in 0..10 {
                let key = format!("{:02}", i);
                map.compare_insert(
                    make_key(key.as_bytes(), 1),
                    make_entry(key.as_bytes(), b"v", 1),
                    |_| true,
                );
            }

            // When: Iterate forward and backward until they meet
            let mut range = map.range(..);
            let mut forward_keys = Vec::new();
            let mut backward_keys = Vec::new();

            loop {
                let f = range.next();
                let b = range.next_back();

                let f_done = f.is_none();
                let b_done = b.is_none();

                if let Some(entry) = f {
                    forward_keys.push(entry.key_bytes().to_vec());
                }
                if let Some(entry) = b {
                    backward_keys.push(entry.key_bytes().to_vec());
                }

                if f_done && b_done {
                    break;
                }
            }

            // Then: All entries seen exactly once
            let mut all_keys: Vec<_> = forward_keys
                .into_iter()
                .chain(backward_keys.into_iter())
                .collect();
            all_keys.sort();
            all_keys.dedup();

            assert_eq!(all_keys.len(), 10, "Should have seen all 10 entries");
        }
    }

    // ==================== Loom Tests (Exhaustive Thread Interleaving) ====================

    #[cfg(loom)]
    mod loom_tests {
        use super::*;
        use loom::sync::Arc;
        use loom::thread;

        // Note: For loom tests, we use very small data sets because loom
        // explores all possible interleavings, which grows exponentially.

        #[test]
        fn loom_should_handle_two_thread_insert_same_key() {
            loom::model(|| {
                let map = Arc::new(SkipMap::new());

                let h1 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        map.compare_insert(
                            make_key(b"key", 1),
                            make_entry(b"key", b"v1", 1),
                            |_| true,
                        );
                    })
                };

                let h2 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        map.compare_insert(
                            make_key(b"key", 2),
                            make_entry(b"key", b"v2", 2),
                            |_| true,
                        );
                    })
                };

                h1.join().unwrap();
                h2.join().unwrap();

                // Invariant: exactly 2 entries, ordered by seq descending
                assert_eq!(map.len(), 2);
                let mut range = map.range(..);
                let first = range.next().unwrap();
                let second = range.next().unwrap();
                assert_eq!(first.seq(), 2);
                assert_eq!(second.seq(), 1);
            });
        }

        #[test]
        fn loom_should_handle_insert_during_iteration() {
            loom::model(|| {
                let map = Arc::new(SkipMap::new());

                // Pre-insert one entry
                map.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);

                let h1 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        // Insert during iteration
                        map.compare_insert(
                            make_key(b"bbb", 1),
                            make_entry(b"bbb", b"2", 1),
                            |_| true,
                        );
                    })
                };

                // Iterate while insert happens
                let mut range = map.range(..);
                let mut count = 0;
                while range.next().is_some() {
                    count += 1;
                }

                h1.join().unwrap();

                // Iterator sees at least the initial entry
                assert!(count >= 1);
                // Map has both entries at the end
                assert_eq!(map.len(), 2);
            });
        }

        #[test]
        fn loom_should_handle_concurrent_inserts_different_keys() {
            loom::model(|| {
                let map = Arc::new(SkipMap::new());

                let h1 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        map.compare_insert(
                            make_key(b"aaa", 1),
                            make_entry(b"aaa", b"1", 1),
                            |_| true,
                        );
                    })
                };

                let h2 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        map.compare_insert(
                            make_key(b"bbb", 1),
                            make_entry(b"bbb", b"2", 1),
                            |_| true,
                        );
                    })
                };

                h1.join().unwrap();
                h2.join().unwrap();

                // Invariant: both entries present, in sorted order
                assert_eq!(map.len(), 2);
                let mut range = map.range(..);
                assert_eq!(range.next().unwrap().key_bytes(), b"aaa");
                assert_eq!(range.next().unwrap().key_bytes(), b"bbb");
            });
        }

        #[test]
        fn loom_should_retry_cas_on_contention() {
            loom::model(|| {
                let map = Arc::new(SkipMap::new());

                // Three threads inserting adjacent keys creates CAS contention
                let h1 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        map.compare_insert(
                            make_key(b"bbb", 1),
                            make_entry(b"bbb", b"2", 1),
                            |_| true,
                        );
                    })
                };

                let h2 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        map.compare_insert(
                            make_key(b"aaa", 1),
                            make_entry(b"aaa", b"1", 1),
                            |_| true,
                        );
                    })
                };

                let h3 = {
                    let map = map.clone();
                    thread::spawn(move || {
                        map.compare_insert(
                            make_key(b"ccc", 1),
                            make_entry(b"ccc", b"3", 1),
                            |_| true,
                        );
                    })
                };

                h1.join().unwrap();
                h2.join().unwrap();
                h3.join().unwrap();

                // Invariant: all entries present, in sorted order
                assert_eq!(map.len(), 3);
                let keys = {
                    let mut range = map.range(..);
                    let mut keys = Vec::new();
                    while let Some(entry) = range.next() {
                        keys.push(entry.key_bytes().to_vec());
                    }
                    keys
                };
                assert_eq!(keys[0], b"aaa");
                assert_eq!(keys[1], b"bbb");
                assert_eq!(keys[2], b"ccc");
            });
        }
    }

    // ==================== Proptest Property-Based Tests ====================

    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeSet;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(20))]

            #[test]
            fn prop_concurrent_inserts_maintain_sorted_order(
                keys in prop::collection::vec("[a-z]{1,8}", 10..50),
                num_threads in 2..8usize,
            ) {
                let map = Arc::new(SkipMap::new());
                let keys = Arc::new(keys);

                // Distribute keys across threads
                let handles: Vec<_> = (0..num_threads)
                    .map(|t| {
                        let map = map.clone();
                        let keys = keys.clone();
                        thread::spawn(move || {
                            for (i, key) in keys.iter().enumerate() {
                                if i % num_threads == t {
                                    map.compare_insert(
                                        make_key(key.as_bytes(), 1),
                                        make_entry(key.as_bytes(), b"v", 1),
                                        |_| true,
                                    );
                                }
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.join().unwrap();
                }

                // Verify sorted order
                let mut prev_key: Option<Vec<u8>> = None;
                let mut range = map.range(..);
                while let Some(entry) = range.next() {
                    if let Some(ref pk) = prev_key {
                        prop_assert!(entry.key_bytes() >= pk.as_slice());
                    }
                    prev_key = Some(entry.key_bytes().to_vec());
                }
            }

            #[test]
            fn prop_iterator_sees_all_committed_entries(
                keys in prop::collection::vec("[a-z]{1,4}", 5..20),
            ) {
                let map = Arc::new(SkipMap::new());
                let unique_keys: BTreeSet<_> = keys.iter().cloned().collect();
                let expected_count = unique_keys.len();

                // Insert all keys
                for key in &keys {
                    map.compare_insert(
                        make_key(key.as_bytes(), 1),
                        make_entry(key.as_bytes(), b"v", 1),
                        |_| true, // Allow duplicates
                    );
                }

                // Iterator should see at least expected_count entries
                let mut range = map.range(..);
                let mut count = 0;
                while range.next().is_some() {
                    count += 1;
                }

                // We allow duplicates, so count >= unique_keys.len()
                prop_assert!(count >= expected_count);
            }

            #[test]
            fn prop_range_bounds_always_respected(
                keys in prop::collection::vec(0u32..1000, 20..50),
                start in 0u32..500,
                end in 500u32..1000,
            ) {
                let map = SkipMap::new();

                for key in &keys {
                    let key_str = format!("{:04}", key);
                    map.compare_insert(
                        make_key(key_str.as_bytes(), 1),
                        make_entry(key_str.as_bytes(), b"v", 1),
                        |_| true,
                    );
                }

                let start_key = format!("{:04}", start);
                let end_key = format!("{:04}", end);

                // Use seq=u64::MAX for end bound to exclude all entries with end_key
                // (because higher seq comes first in MVCC ordering)
                let mut range = map.range(
                    make_key(start_key.as_bytes(), u64::MAX)..make_key(end_key.as_bytes(), u64::MAX)
                );

                while let Some(entry) = range.next() {
                    let key_bytes = entry.key_bytes();
                    prop_assert!(key_bytes >= start_key.as_bytes());
                    prop_assert!(key_bytes < end_key.as_bytes());
                }
            }
        }
    }
}
