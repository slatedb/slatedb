//! Iterator implementations for the skiplist.

use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::ptr;

use super::list::SkipList;
use super::node::{Entry, Node};
use crate::mem_table::SequencedKey;

/// Bidirectional range iterator over entries in the skiplist.
///
/// Supports both forward (`next()`) and backward (`next_back()`) iteration.
pub struct Range<'a, R>
where
    R: RangeBounds<SequencedKey>,
{
    list: &'a SkipList,
    range: R,
    /// Current position for forward iteration
    front: *mut Node,
    /// Current position for backward iteration
    back: *mut Node,
    /// Whether we've initialized the back position
    back_initialized: bool,
    /// Whether front and back have crossed
    done: bool,
    _marker: PhantomData<&'a Entry<'a>>,
}

impl<'a, R> Range<'a, R>
where
    R: RangeBounds<SequencedKey>,
{
    pub(super) fn new(list: &'a SkipList, range: R) -> Self {
        // Find starting position based on range start bound
        let front = match range.start_bound() {
            Bound::Included(key) => list.find_greater_or_equal(key),
            Bound::Excluded(key) => {
                // Find first node > key
                let node = list.find_greater_or_equal(key);
                if node.is_null() {
                    ptr::null_mut()
                } else {
                    let entry = unsafe { (*node).entry() };
                    let entry_key = entry.to_sequenced_key();
                    if entry_key == *key {
                        // Skip this exact match
                        unsafe { (*node).next(0) }
                    } else {
                        node
                    }
                }
            }
            Bound::Unbounded => list.first(),
        };

        Self {
            list,
            range,
            front,
            back: ptr::null_mut(),
            back_initialized: false,
            done: false,
            _marker: PhantomData,
        }
    }

    /// Check if a key is within the range bounds.
    fn in_range(&self, key: &SequencedKey) -> bool {
        // Check start bound
        let after_start = match self.range.start_bound() {
            Bound::Included(start) => key >= start,
            Bound::Excluded(start) => key > start,
            Bound::Unbounded => true,
        };

        // Check end bound
        let before_end = match self.range.end_bound() {
            Bound::Included(end) => key <= end,
            Bound::Excluded(end) => key < end,
            Bound::Unbounded => true,
        };

        after_start && before_end
    }

    /// Initialize the back position for reverse iteration.
    fn init_back(&mut self) {
        if self.back_initialized {
            return;
        }
        self.back_initialized = true;

        // Find the last node in range
        self.back = match self.range.end_bound() {
            Bound::Included(key) => {
                // Find the node with this key, or the last node before it
                let node = self.list.find_greater_or_equal(key);
                if node.is_null() {
                    // No node >= key, so get the last node
                    self.list.find_last()
                } else {
                    let entry = unsafe { (*node).entry() };
                    let entry_key = entry.to_sequenced_key();
                    if entry_key == *key {
                        // Exact match, include it
                        node
                    } else {
                        // node > key, find the one before
                        self.list.find_less_than(key)
                    }
                }
            }
            Bound::Excluded(key) => self.list.find_less_than(key),
            Bound::Unbounded => self.list.find_last(),
        };

        // Check if back is in range
        if !self.back.is_null() {
            let entry = unsafe { (*self.back).entry() };
            let entry_key = entry.to_sequenced_key();
            if !self.in_range(&entry_key) {
                self.back = ptr::null_mut();
            }
        }
    }

    /// Get the next entry in forward direction.
    pub fn next(&mut self) -> Option<Entry<'a>> {
        if self.done || self.front.is_null() {
            return None;
        }

        let entry = unsafe { (*self.front).entry() };
        let entry_key = entry.to_sequenced_key();

        // Check if we're still in range
        if !self.in_range(&entry_key) {
            self.done = true;
            return None;
        }

        // Check if we've met the back iterator
        if self.back_initialized && self.front == self.back {
            self.done = true;
            // Return this last entry
            self.front = ptr::null_mut();
            // SAFETY: The entry is valid for the lifetime of the list
            return Some(unsafe { std::mem::transmute(entry) });
        }

        // Advance front
        self.front = unsafe { (*self.front).next(0) };

        // SAFETY: The entry is valid for the lifetime of the list
        Some(unsafe { std::mem::transmute(entry) })
    }

    /// Get the next entry in backward direction.
    pub fn next_back(&mut self) -> Option<Entry<'a>> {
        // Initialize back position if needed
        self.init_back();

        if self.done || self.back.is_null() {
            return None;
        }

        let entry = unsafe { (*self.back).entry() };
        let entry_key = entry.to_sequenced_key();

        // Check if we're still in range
        if !self.in_range(&entry_key) {
            self.done = true;
            return None;
        }

        // Check if we've met the front iterator
        if self.front == self.back {
            self.done = true;
            // Return this last entry
            self.back = ptr::null_mut();
            // SAFETY: The entry is valid for the lifetime of the list
            return Some(unsafe { std::mem::transmute(entry) });
        }

        // Move back to previous node
        // This is O(log n) because we need to search from the beginning
        self.back = self.list.find_less_than(&entry_key);

        // Check if new back is still in range
        if !self.back.is_null() {
            let new_entry = unsafe { (*self.back).entry() };
            let new_key = new_entry.to_sequenced_key();
            if !self.in_range(&new_key) {
                self.back = ptr::null_mut();
            }
        }

        // SAFETY: The entry is valid for the lifetime of the list
        Some(unsafe { std::mem::transmute(entry) })
    }
}

// SAFETY: Range is Send because:
// - The SkipList reference is Send
// - The raw Node pointers point to data that outlives the iterator
// - Nodes are never deleted during operation
unsafe impl<R: RangeBounds<SequencedKey> + Send> Send for Range<'_, R> {}

// SAFETY: Range is Sync because:
// - All mutable state (front, back, done) is only accessed through &mut self
// - The underlying nodes are immutable after creation
unsafe impl<R: RangeBounds<SequencedKey> + Sync> Sync for Range<'_, R> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skipmap::list::SkipList;
    use crate::types::{RowEntry, ValueDeletable};
    use bytes::Bytes;
    use std::ops::RangeBounds;

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

    fn populate_list(list: &SkipList) {
        // Insert entries: aaa, bbb, ccc, ddd, eee (all with seq=1)
        for (key, value) in [
            (b"aaa", b"1"),
            (b"bbb", b"2"),
            (b"ccc", b"3"),
            (b"ddd", b"4"),
            (b"eee", b"5"),
        ] {
            list.compare_insert(make_key(key, 1), make_entry(key, value, 1), |_| true);
        }
    }

    #[test]
    fn should_iterate_range_forward() {
        let list = SkipList::new();
        populate_list(&list);

        // Range: bbb..=ddd
        let mut range = Range::new(&list, make_key(b"bbb", u64::MAX)..=make_key(b"ddd", 0));

        let mut keys = Vec::new();
        while let Some(entry) = range.next() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"bbb");
        assert_eq!(keys[1], b"ccc");
        assert_eq!(keys[2], b"ddd");
    }

    #[test]
    fn should_iterate_range_backward() {
        let list = SkipList::new();
        populate_list(&list);

        // Range: bbb..=ddd
        let mut range = Range::new(&list, make_key(b"bbb", u64::MAX)..=make_key(b"ddd", 0));

        let mut keys = Vec::new();
        while let Some(entry) = range.next_back() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"ddd");
        assert_eq!(keys[1], b"ccc");
        assert_eq!(keys[2], b"bbb");
    }

    #[test]
    fn should_handle_unbounded_range() {
        let list = SkipList::new();
        populate_list(&list);

        let mut range = Range::new(&list, ..);

        let mut keys = Vec::new();
        while let Some(entry) = range.next() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 5);
    }

    #[test]
    fn should_handle_half_bounded_range() {
        let list = SkipList::new();
        populate_list(&list);

        // Range: ccc.. (using seq=MAX to include all versions of ccc and after)
        let mut range = Range::new(&list, make_key(b"ccc", u64::MAX)..);

        let mut keys = Vec::new();
        while let Some(entry) = range.next() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"ccc");
        assert_eq!(keys[1], b"ddd");
        assert_eq!(keys[2], b"eee");
    }

    #[test]
    fn should_handle_excluded_bounds() {
        let list = SkipList::new();
        populate_list(&list);

        // Range: (bbb, ddd) - exclusive on both ends
        // Need a custom range type for this
        struct ExclusiveRange {
            start: SequencedKey,
            end: SequencedKey,
        }

        impl RangeBounds<SequencedKey> for ExclusiveRange {
            fn start_bound(&self) -> Bound<&SequencedKey> {
                Bound::Excluded(&self.start)
            }
            fn end_bound(&self) -> Bound<&SequencedKey> {
                Bound::Excluded(&self.end)
            }
        }

        let mut range = Range::new(
            &list,
            ExclusiveRange {
                start: make_key(b"bbb", 0), // exclude bbb with seq=0 (highest priority bbb)
                end: make_key(b"ddd", u64::MAX), // exclude ddd with seq=MAX (lowest priority ddd)
            },
        );

        let mut keys = Vec::new();
        while let Some(entry) = range.next() {
            keys.push(entry.key_bytes().to_vec());
        }

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"ccc");
    }

    #[test]
    fn should_handle_empty_range() {
        let list = SkipList::new();
        populate_list(&list);

        // Range that matches nothing
        let mut range = Range::new(&list, make_key(b"xxx", u64::MAX)..=make_key(b"yyy", 0));

        assert!(range.next().is_none());
    }

    #[test]
    fn should_interleave_forward_and_backward() {
        let list = SkipList::new();
        populate_list(&list);

        let mut range = Range::new(&list, make_key(b"aaa", u64::MAX)..=make_key(b"eee", 0));

        // Get first from front
        let e1 = range.next().unwrap();
        assert_eq!(e1.key_bytes(), b"aaa");

        // Get first from back
        let e2 = range.next_back().unwrap();
        assert_eq!(e2.key_bytes(), b"eee");

        // Continue from front
        let e3 = range.next().unwrap();
        assert_eq!(e3.key_bytes(), b"bbb");

        // Continue from back
        let e4 = range.next_back().unwrap();
        assert_eq!(e4.key_bytes(), b"ddd");

        // Only ccc left
        let e5 = range.next().unwrap();
        assert_eq!(e5.key_bytes(), b"ccc");

        // Should be done now
        assert!(range.next().is_none());
        assert!(range.next_back().is_none());
    }
}
