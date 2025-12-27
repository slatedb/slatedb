//! Skiplist node structure with inline tower and arena-allocated entries.
//!
//! Each node contains a pointer to an arena-allocated Entry and a variable-length
//! tower of atomic next pointers. The Entry stores key/value data inline in the arena.

use std::cmp::Ordering;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering as AtomicOrdering};

use bytes::Bytes;

use crate::mem_table::SequencedKey;
use crate::types::{RowEntry, ValueDeletable};

/// Maximum height of the skiplist.
/// With branching factor 4, this gives us good performance up to ~16M entries.
pub const MAX_HEIGHT: usize = 12;

/// Sentinel value for "no timestamp" in Option<i64> fields.
const NO_TIMESTAMP: i64 = i64::MIN;

/// Tag values for ValueDeletable variants.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueTag {
    Tombstone = 0,
    Value = 1,
    Merge = 2,
}

impl ValueTag {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => ValueTag::Tombstone,
            1 => ValueTag::Value,
            2 => ValueTag::Merge,
            _ => panic!("Invalid value tag: {}", v),
        }
    }
}

/// Fixed-size header for an entry in the arena.
///
/// The variable-length key and value bytes follow immediately after this header.
#[repr(C)]
pub struct EntryHeader {
    /// Length of the key in bytes
    pub key_len: u32,
    /// Length of the value in bytes (0 for tombstone)
    pub val_len: u32,
    /// Sequence number
    pub seq: u64,
    /// Creation timestamp (NO_TIMESTAMP if None)
    pub create_ts: i64,
    /// Expiration timestamp (NO_TIMESTAMP if None)
    pub expire_ts: i64,
    /// Value type tag
    pub val_tag: u8,
    // Padding for alignment
    _padding: [u8; 7],
}

impl EntryHeader {
    /// Calculate total size needed for an entry with given key and value lengths.
    pub const fn total_size(key_len: usize, val_len: usize) -> usize {
        std::mem::size_of::<EntryHeader>() + key_len + val_len
    }

    /// Get the size of this entry including variable data.
    pub fn size(&self) -> usize {
        Self::total_size(self.key_len as usize, self.val_len as usize)
    }

    /// Get a pointer to the key bytes (immediately after header).
    #[inline]
    pub fn key_ptr(&self) -> *const u8 {
        unsafe { (self as *const EntryHeader).add(1) as *const u8 }
    }

    /// Get a pointer to the value bytes (after key).
    #[inline]
    pub fn value_ptr(&self) -> *const u8 {
        unsafe { self.key_ptr().add(self.key_len as usize) }
    }

    /// Get the key as a byte slice.
    #[inline]
    pub fn key_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.key_ptr(), self.key_len as usize) }
    }

    /// Get the value as a byte slice.
    #[inline]
    pub fn value_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.value_ptr(), self.val_len as usize) }
    }

    /// Get the create_ts as Option<i64>.
    #[inline]
    pub fn create_ts_opt(&self) -> Option<i64> {
        if self.create_ts == NO_TIMESTAMP {
            None
        } else {
            Some(self.create_ts)
        }
    }

    /// Get the expire_ts as Option<i64>.
    #[inline]
    pub fn expire_ts_opt(&self) -> Option<i64> {
        if self.expire_ts == NO_TIMESTAMP {
            None
        } else {
            Some(self.expire_ts)
        }
    }

    /// Get the value tag.
    #[inline]
    pub fn value_tag(&self) -> ValueTag {
        ValueTag::from_u8(self.val_tag)
    }
}

/// A view into an arena-allocated entry.
///
/// This provides zero-copy access to the key and value data stored in the arena.
pub struct Entry<'a> {
    header: &'a EntryHeader,
}

impl<'a> Entry<'a> {
    /// Create an Entry view from a header pointer.
    ///
    /// # Safety
    /// The pointer must point to a valid, initialized EntryHeader in the arena.
    #[inline]
    pub unsafe fn from_ptr(ptr: *const EntryHeader) -> Self {
        Self { header: &*ptr }
    }

    /// Get the sequence number.
    #[inline]
    pub fn seq(&self) -> u64 {
        self.header.seq
    }

    /// Get the key as Bytes (zero-copy view into arena).
    ///
    /// Note: This creates a Bytes that references the arena memory.
    /// The caller must ensure the arena outlives the Bytes.
    #[inline]
    pub fn key_bytes(&self) -> &[u8] {
        self.header.key_bytes()
    }

    /// Get the value as a byte slice (zero-copy view into arena).
    #[inline]
    pub fn value_bytes(&self) -> &[u8] {
        self.header.value_bytes()
    }

    /// Get the value tag.
    #[inline]
    pub fn value_tag(&self) -> ValueTag {
        self.header.value_tag()
    }

    /// Get the create timestamp.
    #[inline]
    pub fn create_ts(&self) -> Option<i64> {
        self.header.create_ts_opt()
    }

    /// Get the expire timestamp.
    #[inline]
    pub fn expire_ts(&self) -> Option<i64> {
        self.header.expire_ts_opt()
    }

    /// Reconstruct a SequencedKey from this entry.
    ///
    /// This copies the key bytes into a new Bytes allocation.
    pub fn to_sequenced_key(&self) -> SequencedKey {
        SequencedKey::new(Bytes::copy_from_slice(self.key_bytes()), self.header.seq)
    }

    /// Reconstruct a RowEntry from this entry.
    ///
    /// This copies the key and value bytes into new Bytes allocations.
    pub fn to_row_entry(&self) -> RowEntry {
        let key = Bytes::copy_from_slice(self.key_bytes());
        let value = match self.value_tag() {
            ValueTag::Tombstone => ValueDeletable::Tombstone,
            ValueTag::Value => ValueDeletable::Value(Bytes::copy_from_slice(self.value_bytes())),
            ValueTag::Merge => ValueDeletable::Merge(Bytes::copy_from_slice(self.value_bytes())),
        };
        RowEntry::new(
            key,
            value,
            self.header.seq,
            self.header.create_ts_opt(),
            self.header.expire_ts_opt(),
        )
    }

    /// Compare this entry's key with a SequencedKey.
    ///
    /// Keys are ordered by user_key ascending, then by seq descending.
    #[inline]
    pub fn compare_key(&self, other: &SequencedKey) -> Ordering {
        match self.key_bytes().cmp(other.user_key.as_ref()) {
            Ordering::Equal => {
                // Same user key: higher seq comes first (descending)
                other.seq.cmp(&self.header.seq)
            }
            ord => ord,
        }
    }
}

/// Initialize an entry in pre-allocated arena memory.
///
/// # Safety
/// - `ptr` must point to memory of at least `EntryHeader::total_size(key.len(), val_len)` bytes
/// - `ptr` must be properly aligned for EntryHeader
/// - The memory must be uninitialized or the caller must ensure no other references exist
pub unsafe fn init_entry(
    ptr: *mut EntryHeader,
    key: &[u8],
    value: &ValueDeletable,
    seq: u64,
    create_ts: Option<i64>,
    expire_ts: Option<i64>,
) {
    let (val_tag, val_bytes): (u8, &[u8]) = match value {
        ValueDeletable::Tombstone => (ValueTag::Tombstone as u8, &[]),
        ValueDeletable::Value(v) => (ValueTag::Value as u8, v.as_ref()),
        ValueDeletable::Merge(v) => (ValueTag::Merge as u8, v.as_ref()),
    };

    // Write header
    ptr::write(
        ptr,
        EntryHeader {
            key_len: key.len() as u32,
            val_len: val_bytes.len() as u32,
            seq,
            create_ts: create_ts.unwrap_or(NO_TIMESTAMP),
            expire_ts: expire_ts.unwrap_or(NO_TIMESTAMP),
            val_tag,
            _padding: [0; 7],
        },
    );

    // Copy key bytes
    let key_dst = (*ptr).key_ptr() as *mut u8;
    ptr::copy_nonoverlapping(key.as_ptr(), key_dst, key.len());

    // Copy value bytes
    if !val_bytes.is_empty() {
        let val_dst = (*ptr).value_ptr() as *mut u8;
        ptr::copy_nonoverlapping(val_bytes.as_ptr(), val_dst, val_bytes.len());
    }
}

/// A skiplist node with inline tower.
///
/// Memory layout:
/// ```text
/// [Node { entry, height }][AtomicPtr<Node>; height]
/// ```
///
/// The tower array is allocated immediately after the Node struct.
/// The entry pointer points to an EntryHeader in the arena.
#[repr(C)]
pub struct Node {
    /// Pointer to arena-allocated EntryHeader (or null for sentinel)
    entry: *const EntryHeader,
    /// Height of this node (number of levels)
    height: usize,
    // The tower follows immediately after this struct in memory.
}

impl Node {
    /// Calculate the total size needed for a node with the given height.
    pub const fn size_for_height(height: usize) -> usize {
        std::mem::size_of::<Node>() + std::mem::size_of::<AtomicPtr<Node>>() * height
    }

    /// Initialize a node in pre-allocated memory.
    ///
    /// # Safety
    /// - `ptr` must point to memory of at least `size_for_height(height)` bytes
    /// - `ptr` must be properly aligned for Node
    /// - The memory must be uninitialized or the caller must ensure no other
    ///   references exist
    /// - `entry` must be a valid pointer to an arena-allocated EntryHeader (or null for sentinel)
    pub unsafe fn init(ptr: *mut Node, entry: *const EntryHeader, height: usize) {
        // Write the node header
        ptr::write(ptr, Node { entry, height });

        // Initialize all tower pointers to null
        let tower_ptr = Self::tower_ptr(ptr);
        for i in 0..height {
            ptr::write(tower_ptr.add(i), AtomicPtr::new(ptr::null_mut()));
        }
    }

    /// Get a pointer to the tower array.
    #[inline]
    fn tower_ptr(node: *const Node) -> *mut AtomicPtr<Node> {
        // Tower is immediately after the Node struct
        unsafe { (node as *mut u8).add(std::mem::size_of::<Node>()) as *mut AtomicPtr<Node> }
    }

    /// Get the next pointer at the given level.
    ///
    /// Uses Acquire ordering to ensure visibility of the pointed-to node's data.
    #[inline]
    pub fn next(&self, level: usize) -> *mut Node {
        debug_assert!(level < self.height);
        unsafe {
            let tower = Self::tower_ptr(self);
            (*tower.add(level)).load(AtomicOrdering::Acquire)
        }
    }

    /// Set the next pointer at the given level.
    ///
    /// Uses Release ordering to ensure this node's data is visible to readers.
    #[inline]
    pub fn set_next(&self, level: usize, node: *mut Node) {
        debug_assert!(level < self.height);
        unsafe {
            let tower = Self::tower_ptr(self);
            (*tower.add(level)).store(node, AtomicOrdering::Release);
        }
    }

    /// Atomically compare-and-swap the next pointer at the given level.
    ///
    /// Returns true if the swap succeeded.
    #[inline]
    pub fn cas_next(&self, level: usize, expected: *mut Node, new: *mut Node) -> bool {
        debug_assert!(level < self.height);
        unsafe {
            let tower = Self::tower_ptr(self);
            (*tower.add(level))
                .compare_exchange(
                    expected,
                    new,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
        }
    }

    /// Get a view of the entry.
    ///
    /// # Safety
    /// The entry pointer must be valid and the EntryHeader must be initialized.
    /// This is guaranteed if the node was created through the skiplist's insert.
    #[inline]
    pub fn entry(&self) -> Entry<'_> {
        debug_assert!(!self.entry.is_null());
        unsafe { Entry::from_ptr(self.entry) }
    }

    /// Check if this is a sentinel node (no entry).
    #[inline]
    pub fn is_sentinel(&self) -> bool {
        self.entry.is_null()
    }

    /// Get the height of this node.
    #[inline]
    pub fn height(&self) -> usize {
        self.height
    }

    /// Compare this node's key with a SequencedKey.
    #[inline]
    pub fn compare_key(&self, other: &SequencedKey) -> Ordering {
        self.entry().compare_key(other)
    }
}

// SAFETY: Node is Send because:
// - Entry pointer points to arena-allocated data that outlives the skiplist
// - AtomicPtr provides thread-safe access to tower pointers
unsafe impl Send for Node {}

// SAFETY: Node is Sync because:
// - All mutable state (tower pointers) is accessed through atomics
// - Entry data is immutable after creation
unsafe impl Sync for Node {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{alloc, dealloc, Layout};

    #[test]
    fn should_calculate_correct_size_for_height() {
        let node_size = std::mem::size_of::<Node>();
        let ptr_size = std::mem::size_of::<AtomicPtr<Node>>();

        assert_eq!(Node::size_for_height(1), node_size + ptr_size);
        assert_eq!(Node::size_for_height(4), node_size + 4 * ptr_size);
        assert_eq!(
            Node::size_for_height(MAX_HEIGHT),
            node_size + MAX_HEIGHT * ptr_size
        );
    }

    #[test]
    fn should_init_entry_and_read_back() {
        let key = b"test_key";
        let value = ValueDeletable::Value(Bytes::from_static(b"test_value"));

        let entry_size = EntryHeader::total_size(key.len(), 10);
        let layout = Layout::from_size_align(entry_size, 8).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut EntryHeader;

        unsafe {
            init_entry(ptr, key, &value, 42, Some(100), Some(200));

            let entry = Entry::from_ptr(ptr);
            assert_eq!(entry.key_bytes(), b"test_key");
            assert_eq!(entry.value_bytes(), b"test_value");
            assert_eq!(entry.seq(), 42);
            assert_eq!(entry.create_ts(), Some(100));
            assert_eq!(entry.expire_ts(), Some(200));
            assert_eq!(entry.value_tag(), ValueTag::Value);

            dealloc(ptr as *mut u8, layout);
        }
    }

    #[test]
    fn should_handle_tombstone() {
        let key = b"deleted_key";
        let value = ValueDeletable::Tombstone;

        let entry_size = EntryHeader::total_size(key.len(), 0);
        let layout = Layout::from_size_align(entry_size, 8).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut EntryHeader;

        unsafe {
            init_entry(ptr, key, &value, 99, None, None);

            let entry = Entry::from_ptr(ptr);
            assert_eq!(entry.key_bytes(), b"deleted_key");
            assert_eq!(entry.value_bytes(), b"");
            assert_eq!(entry.seq(), 99);
            assert_eq!(entry.create_ts(), None);
            assert_eq!(entry.expire_ts(), None);
            assert_eq!(entry.value_tag(), ValueTag::Tombstone);

            dealloc(ptr as *mut u8, layout);
        }
    }

    #[test]
    fn should_reconstruct_sequenced_key() {
        let key = b"my_key";
        let value = ValueDeletable::Value(Bytes::from_static(b"val"));

        let entry_size = EntryHeader::total_size(key.len(), 3);
        let layout = Layout::from_size_align(entry_size, 8).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut EntryHeader;

        unsafe {
            init_entry(ptr, key, &value, 123, None, None);

            let entry = Entry::from_ptr(ptr);
            let seq_key = entry.to_sequenced_key();

            assert_eq!(seq_key.user_key.as_ref(), b"my_key");
            assert_eq!(seq_key.seq, 123);

            dealloc(ptr as *mut u8, layout);
        }
    }

    #[test]
    fn should_compare_keys_correctly() {
        let key = b"bbb";
        let value = ValueDeletable::Value(Bytes::from_static(b"v"));

        let entry_size = EntryHeader::total_size(key.len(), 1);
        let layout = Layout::from_size_align(entry_size, 8).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut EntryHeader;

        unsafe {
            init_entry(ptr, key, &value, 10, None, None);

            let entry = Entry::from_ptr(ptr);

            // Same key, same seq
            let key_same = SequencedKey::new(Bytes::from_static(b"bbb"), 10);
            assert_eq!(entry.compare_key(&key_same), Ordering::Equal);

            // Same key, lower seq (entry has higher priority)
            let key_lower_seq = SequencedKey::new(Bytes::from_static(b"bbb"), 5);
            assert_eq!(entry.compare_key(&key_lower_seq), Ordering::Less);

            // Same key, higher seq (other has higher priority)
            let key_higher_seq = SequencedKey::new(Bytes::from_static(b"bbb"), 15);
            assert_eq!(entry.compare_key(&key_higher_seq), Ordering::Greater);

            // Different key
            let key_less = SequencedKey::new(Bytes::from_static(b"aaa"), 10);
            assert_eq!(entry.compare_key(&key_less), Ordering::Greater);

            let key_greater = SequencedKey::new(Bytes::from_static(b"ccc"), 10);
            assert_eq!(entry.compare_key(&key_greater), Ordering::Less);

            dealloc(ptr as *mut u8, layout);
        }
    }

    #[test]
    fn should_init_node_with_null_tower() {
        let height = 4;
        let layout = Layout::from_size_align(Node::size_for_height(height), 8).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut Node;

        unsafe {
            Node::init(ptr, ptr::null(), height);

            let node = &*ptr;
            assert_eq!(node.height(), height);
            assert!(node.is_sentinel());
            for i in 0..height {
                assert!(node.next(i).is_null());
            }

            dealloc(ptr as *mut u8, layout);
        }
    }
}
