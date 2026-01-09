//! Core lock-free skiplist implementation.
//!
//! This skiplist is optimized for SlateDB's memtable use case:
//! - Append-only (no deletions during operation)
//! - Arena allocation for both nodes and entries (zero heap allocations per insert)
//! - Lock-free reads and inserts

use std::cell::Cell;
use std::cmp::Ordering;
use std::ptr;

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use super::arena::ConcurrentArena;
use super::node::{init_entry, Entry, EntryHeader, Node, MAX_HEIGHT};
use crate::mem_table::SequencedKey;
use crate::types::{RowEntry, ValueDeletable};

// Thread-local fast RNG for height generation.
// Uses xorshift64 algorithm - fast and good enough for skiplist heights.
thread_local! {
    static RNG: Cell<u64> = Cell::new({
        // Seed with a mix of time and thread ID for uniqueness
        let time_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x12345678);
        let thread_id = std::thread::current().id();
        let thread_hash = format!("{:?}", thread_id)
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        time_seed ^ thread_hash ^ 0x517cc1b727220a95
    });
}

/// Fast xorshift64 PRNG step
#[inline]
fn xorshift64(state: u64) -> u64 {
    let mut x = state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Branching factor for height generation.
/// With factor 4, each level is ~1/4 as likely as the previous.
const BRANCHING_FACTOR: u32 = 4;

/// A lock-free skiplist optimized for SlateDB's memtable.
///
/// Provides O(log n) insert and lookup with lock-free concurrent access.
/// The skiplist is append-only; nodes are never removed until the entire
/// list is dropped.
///
/// Both node structures and entry data are allocated in the arena, providing:
/// - Zero heap allocations per insert (just arena bump allocation)
/// - Excellent cache locality
/// - O(1) bulk deallocation when the skiplist is dropped
pub struct SkipList {
    /// Arena for all allocations (nodes and entries)
    arena: ConcurrentArena,
    /// Sentinel head node (never contains real data)
    head: *mut Node,
    /// Current maximum height of any node
    max_height: AtomicUsize,
    /// Number of entries in the list
    len: AtomicUsize,
}

impl SkipList {
    /// Create a new empty SkipList.
    pub fn new() -> Self {
        Self::with_initial_allocated_size(0)
    }

    /// Create a new SkipList with the specified initial arena allocation size.
    ///
    /// If size is 0, a default block size is used.
    /// Otherwise, the first arena block will be sized to the given value,
    /// with overflow blocks at 1/8 of that size.
    pub fn with_initial_allocated_size(size: usize) -> Self {
        let arena = if size == 0 {
            ConcurrentArena::new()
        } else {
            ConcurrentArena::with_initial_allocated_size(size)
        };

        // Allocate sentinel head node with maximum height
        let head = arena.alloc_bytes_aligned(
            Node::size_for_height(MAX_HEIGHT),
            std::mem::align_of::<Node>(),
        ) as *mut Node;
        unsafe {
            Node::init(head, ptr::null(), MAX_HEIGHT);
        }

        Self {
            arena,
            head,
            max_height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
        }
    }

    /// Insert a row entry, calling the compare function if key exists.
    ///
    /// The `compare_fn` receives a reference to the existing entry if
    /// an entry with the same key already exists. It returns `true` to proceed
    /// with insertion (which adds a new node), `false` to skip.
    ///
    /// Note: Since this is an append-only skiplist, "replacing" a key actually
    /// adds a new node. The old node remains in the list but will be shadowed
    /// by the new one during lookups.
    pub fn compare_insert<F>(&self, key: SequencedKey, value: RowEntry, compare_fn: F)
    where
        F: Fn(&Entry<'_>) -> bool,
    {
        let mut prev = [ptr::null_mut::<Node>(); MAX_HEIGHT];
        let mut next = [ptr::null_mut::<Node>(); MAX_HEIGHT];

        // Find insert position
        let existing = self.find_position(&key, &mut prev, &mut next);

        // If key exists, call compare function
        if let Some(node) = existing {
            let entry = unsafe { (*node).entry() };
            if !compare_fn(&entry) {
                return;
            }
        }

        // Generate random height for new node
        let height = self.random_height();

        // Calculate sizes for combined allocation
        let val_len = match &value.value {
            ValueDeletable::Tombstone => 0,
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => v.len(),
        };
        let node_size = Node::size_for_height(height);
        let entry_size = EntryHeader::total_size(key.user_key.len(), val_len);

        // Calculate padding needed between node and entry for alignment
        let entry_align = std::mem::align_of::<EntryHeader>();
        let entry_offset = (node_size + entry_align - 1) & !(entry_align - 1);
        let total_size = entry_offset + entry_size;

        // Single allocation for both node and entry
        // Layout: [Node][Tower][padding][EntryHeader][key][value]
        let combined_ptr = self
            .arena
            .alloc_bytes_aligned(total_size, std::mem::align_of::<Node>());

        let node_ptr = combined_ptr as *mut Node;
        let entry_ptr = unsafe { combined_ptr.add(entry_offset) } as *mut EntryHeader;

        unsafe {
            // Initialize entry first (node will point to it)
            init_entry(
                entry_ptr,
                key.user_key.as_ref(),
                &value.value,
                key.seq,
                value.create_ts,
                value.expire_ts,
            );

            // Initialize node with pointer to embedded entry
            Node::init(node_ptr, entry_ptr, height);
        }

        // Update max height if needed (lock-free)
        let mut current_max = self.max_height.load(AtomicOrdering::Relaxed);
        while height > current_max {
            match self.max_height.compare_exchange_weak(
                current_max,
                height,
                AtomicOrdering::SeqCst,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    // Update prev for new levels to point to head
                    for level in current_max..height {
                        prev[level] = self.head;
                    }
                    break;
                }
                Err(actual) => current_max = actual,
            }
        }

        // Link node into list (bottom-up for correctness)
        for level in 0..height {
            loop {
                // Get current next at this level
                let next_node = unsafe { (*prev[level]).next(level) };

                // Verify our key should go between prev and next_node
                // If next_node < key, we need to move forward
                if !next_node.is_null() {
                    let next_cmp = unsafe { (*next_node).compare_key(&key) };
                    if next_cmp == Ordering::Less {
                        // next_node < key, so key should go AFTER next_node, not here
                        // Move prev forward and retry
                        prev[level] = next_node;
                        continue;
                    }
                }

                // Set our next pointer
                unsafe {
                    (*node_ptr).set_next(level, next_node);
                }

                // Try to CAS into the list
                if unsafe { (*prev[level]).cas_next(level, next_node, node_ptr) } {
                    break;
                }

                // CAS failed - someone inserted concurrently. Since this is an append-only
                // skiplist (no deletions), prev is still valid. Just retry the loop which
                // will re-read next and move forward if needed. This avoids expensive
                // re-traversal from head.
            }
        }

        // Update count
        self.len.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Find the first node with key >= the given key.
    pub fn find_greater_or_equal(&self, key: &SequencedKey) -> *mut Node {
        let mut current = self.head;
        let max_height = self.max_height.load(AtomicOrdering::Acquire);

        for level in (0..max_height).rev() {
            loop {
                let next = unsafe { (*current).next(level) };
                if next.is_null() {
                    break;
                }

                match unsafe { (*next).compare_key(key) } {
                    Ordering::Less => {
                        current = next;
                    }
                    _ => break,
                }
            }
        }

        // Return the next node at level 0
        unsafe { (*current).next(0) }
    }

    /// Find the last node with key < the given key.
    pub fn find_less_than(&self, key: &SequencedKey) -> *mut Node {
        let mut current = self.head;
        let max_height = self.max_height.load(AtomicOrdering::Acquire);

        for level in (0..max_height).rev() {
            loop {
                let next = unsafe { (*current).next(level) };
                if next.is_null() {
                    break;
                }

                match unsafe { (*next).compare_key(key) } {
                    Ordering::Less => {
                        current = next;
                    }
                    _ => break,
                }
            }
        }

        // current is the last node < key (or head if none)
        if current == self.head {
            ptr::null_mut()
        } else {
            current
        }
    }

    /// Find the last node in the list.
    pub fn find_last(&self) -> *mut Node {
        let mut current = self.head;
        let max_height = self.max_height.load(AtomicOrdering::Acquire);

        for level in (0..max_height).rev() {
            loop {
                let next = unsafe { (*current).next(level) };
                if next.is_null() {
                    break;
                }
                current = next;
            }
        }

        if current == self.head {
            ptr::null_mut()
        } else {
            current
        }
    }

    /// Get the first node (smallest key).
    pub fn first(&self) -> *mut Node {
        unsafe { (*self.head).next(0) }
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.len.load(AtomicOrdering::Relaxed)
    }

    /// Returns true if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns total bytes allocated in the arena.
    pub fn bytes_allocated(&self) -> usize {
        self.arena.bytes_allocated()
    }

    /// Returns CAS contention statistics: (attempts, failures, failure_rate).
    pub fn cas_stats(&self) -> (usize, usize, f64) {
        self.arena.cas_stats()
    }

    /// Find the insert position for a key, returning existing node if found.
    fn find_position(
        &self,
        key: &SequencedKey,
        prev: &mut [*mut Node; MAX_HEIGHT],
        next: &mut [*mut Node; MAX_HEIGHT],
    ) -> Option<*mut Node> {
        let mut current = self.head;
        let max_height = self.max_height.load(AtomicOrdering::Acquire);

        for level in (0..max_height).rev() {
            loop {
                let next_node = unsafe { (*current).next(level) };
                if next_node.is_null() {
                    break;
                }

                match unsafe { (*next_node).compare_key(key) } {
                    Ordering::Less => {
                        current = next_node;
                    }
                    Ordering::Equal => {
                        // Found exact match
                        prev[level] = current;
                        next[level] = next_node;

                        // Fill in lower levels
                        for l in (0..level).rev() {
                            prev[l] = current;
                            next[l] = unsafe { (*current).next(l) };
                        }

                        return Some(next_node);
                    }
                    Ordering::Greater => {
                        break;
                    }
                }
            }

            prev[level] = current;
            next[level] = unsafe { (*current).next(level) };
        }

        // Fill in remaining levels with head
        for level in max_height..MAX_HEIGHT {
            prev[level] = self.head;
            next[level] = ptr::null_mut();
        }

        None
    }

    /// Generate a random height using geometric distribution.
    ///
    /// Uses thread-local xorshift64 RNG for fast, allocation-free random numbers.
    fn random_height(&self) -> usize {
        RNG.with(|rng| {
            let mut state = rng.get();
            let mut height = 1;

            while height < MAX_HEIGHT {
                state = xorshift64(state);
                if (state >> 32) as u32 % BRANCHING_FACTOR != 0 {
                    break;
                }
                height += 1;
            }

            rng.set(state);
            height
        })
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

// No Drop impl needed - arena handles all memory cleanup

// SAFETY: SkipList is thread-safe because:
// - Arena is Send + Sync
// - All node access uses atomic operations for tower pointers
// - Entries are immutable after creation
// - Length counter uses atomic operations
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

#[cfg(test)]
mod tests {
    use super::*;
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
        let list = SkipList::new();

        list.compare_insert(make_key(b"key", 1), make_entry(b"key", b"value", 1), |_| {
            true
        });

        assert_eq!(list.len(), 1);

        let first = list.first();
        assert!(!first.is_null());

        let entry = unsafe { (*first).entry() };
        assert_eq!(entry.key_bytes(), b"key");
        assert_eq!(entry.value_bytes(), b"value");
        assert_eq!(entry.seq(), 1);
    }

    #[test]
    fn should_maintain_sorted_order() {
        let list = SkipList::new();

        // Insert in random order
        list.compare_insert(make_key(b"ccc", 1), make_entry(b"ccc", b"3", 1), |_| true);
        list.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);
        list.compare_insert(make_key(b"bbb", 1), make_entry(b"bbb", b"2", 1), |_| true);

        // Verify order by iterating
        let mut current = list.first();
        let mut keys = Vec::new();

        while !current.is_null() {
            let entry = unsafe { (*current).entry() };
            keys.push(entry.key_bytes().to_vec());
            current = unsafe { (*current).next(0) };
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"aaa");
        assert_eq!(keys[1], b"bbb");
        assert_eq!(keys[2], b"ccc");
    }

    #[test]
    fn should_order_same_key_by_seq_descending() {
        let list = SkipList::new();

        // Insert same key with different sequence numbers
        list.compare_insert(make_key(b"key", 1), make_entry(b"key", b"v1", 1), |_| true);
        list.compare_insert(make_key(b"key", 3), make_entry(b"key", b"v3", 3), |_| true);
        list.compare_insert(make_key(b"key", 2), make_entry(b"key", b"v2", 2), |_| true);

        // Should be ordered by seq descending (3, 2, 1)
        let mut current = list.first();
        let mut seqs = Vec::new();

        while !current.is_null() {
            let entry = unsafe { (*current).entry() };
            seqs.push(entry.seq());
            current = unsafe { (*current).next(0) };
        }

        assert_eq!(seqs, vec![3, 2, 1]);
    }

    #[test]
    fn should_call_compare_fn_on_existing_key() {
        let list = SkipList::new();

        // First insert
        list.compare_insert(make_key(b"key", 1), make_entry(b"key", b"v1", 1), |_| true);

        // Second insert with same key - compare_fn returns false
        let called = std::cell::Cell::new(false);
        list.compare_insert(make_key(b"key", 1), make_entry(b"key", b"v2", 1), |_| {
            called.set(true);
            false
        });

        assert!(called.get());
        assert_eq!(list.len(), 1); // Should not have inserted
    }

    #[test]
    fn should_find_greater_or_equal() {
        let list = SkipList::new();

        list.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);
        list.compare_insert(make_key(b"ccc", 1), make_entry(b"ccc", b"3", 1), |_| true);
        list.compare_insert(make_key(b"eee", 1), make_entry(b"eee", b"5", 1), |_| true);

        // Exact match
        let node = list.find_greater_or_equal(&make_key(b"ccc", 1));
        assert!(!node.is_null());
        assert_eq!(unsafe { (*node).entry() }.key_bytes(), b"ccc");

        // No exact match, find next
        let node = list.find_greater_or_equal(&make_key(b"bbb", 1));
        assert!(!node.is_null());
        assert_eq!(unsafe { (*node).entry() }.key_bytes(), b"ccc");

        // Beyond end
        let node = list.find_greater_or_equal(&make_key(b"zzz", 1));
        assert!(node.is_null());
    }

    #[test]
    fn should_find_less_than() {
        let list = SkipList::new();

        list.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);
        list.compare_insert(make_key(b"ccc", 1), make_entry(b"ccc", b"3", 1), |_| true);
        list.compare_insert(make_key(b"eee", 1), make_entry(b"eee", b"5", 1), |_| true);

        // Find less than ccc
        let node = list.find_less_than(&make_key(b"ccc", 1));
        assert!(!node.is_null());
        assert_eq!(unsafe { (*node).entry() }.key_bytes(), b"aaa");

        // Nothing less than aaa
        let node = list.find_less_than(&make_key(b"aaa", 1));
        assert!(node.is_null());
    }

    #[test]
    fn should_find_last() {
        let list = SkipList::new();

        assert!(list.find_last().is_null());

        list.compare_insert(make_key(b"aaa", 1), make_entry(b"aaa", b"1", 1), |_| true);
        let node = list.find_last();
        assert!(!node.is_null());
        assert_eq!(unsafe { (*node).entry() }.key_bytes(), b"aaa");

        list.compare_insert(make_key(b"zzz", 1), make_entry(b"zzz", b"26", 1), |_| true);
        let node = list.find_last();
        assert!(!node.is_null());
        assert_eq!(unsafe { (*node).entry() }.key_bytes(), b"zzz");
    }

    #[test]
    fn should_handle_tombstone() {
        let list = SkipList::new();

        let entry = RowEntry::new(
            Bytes::from_static(b"key"),
            ValueDeletable::Tombstone,
            1,
            None,
            None,
        );
        list.compare_insert(make_key(b"key", 1), entry, |_| true);

        let node = list.first();
        assert!(!node.is_null());

        let entry = unsafe { (*node).entry() };
        assert_eq!(entry.key_bytes(), b"key");
        assert_eq!(entry.value_bytes(), b"");
        assert_eq!(entry.value_tag(), super::super::node::ValueTag::Tombstone);
    }

    #[test]
    fn should_handle_concurrent_inserts() {
        let list: Arc<SkipList> = Arc::new(SkipList::new());
        let num_threads = 4;
        let num_inserts = 500;

        let threads: Vec<_> = (0..num_threads)
            .map(|t| {
                let list = list.clone();
                thread::spawn(move || {
                    for i in 0..num_inserts {
                        let key = format!("key-{:02}-{:04}", t, i);
                        let key_bytes = key.as_bytes();
                        list.compare_insert(
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

        assert_eq!(list.len(), num_threads * num_inserts);

        // Verify all entries are present and in sorted order
        let mut prev_key: Option<Vec<u8>> = None;
        let mut current = list.first();
        let mut count = 0;

        while !current.is_null() {
            let entry = unsafe { (*current).entry() };
            let key = entry.key_bytes().to_vec();

            if let Some(ref pk) = prev_key {
                assert!(key > *pk, "Order violated: prev={:?}, curr={:?}", pk, key);
            }
            prev_key = Some(key);

            current = unsafe { (*current).next(0) };
            count += 1;
        }

        assert_eq!(count, num_threads * num_inserts);
    }

    #[test]
    fn should_track_bytes_allocated() {
        let list = SkipList::new();

        let initial = list.bytes_allocated();
        assert!(initial > 0); // Head node allocated

        list.compare_insert(make_key(b"key", 1), make_entry(b"key", b"value", 1), |_| {
            true
        });

        // Should have allocated more bytes
        assert!(list.bytes_allocated() > initial);
    }
}
