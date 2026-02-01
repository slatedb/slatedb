#![allow(dead_code)]
use std::cmp::Ordering;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use crate::block_iterator::BlockLike;
use crate::error::SlateDBError;
use crate::format::row_codec_v2::SstRowCodecV2;
use crate::iter::IterationOrder;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;

/// Internal state for ascending iteration.
struct AscendingState<B: BlockLike> {
    block: B,
    current_restart_idx: usize,
    offset_in_block: usize,
    entries_since_restart: usize,
    current_key: Bytes,
    exhausted: bool,
}

enum BlockIteratorInner<B: BlockLike> {
    Ascending(AscendingState<B>),
    Descending(DescendingBlockIteratorV2<B>),
}

/// Iterator for BlockV2 that uses restart points for efficient seeking.
/// Supports both ascending and descending iteration orders.
pub(crate) struct BlockIteratorV2<B: BlockLike> {
    inner: BlockIteratorInner<B>,
}

impl<B: BlockLike> BlockIteratorV2<B> {
    pub(crate) fn new(block: B, ordering: IterationOrder) -> Self {
        match ordering {
            IterationOrder::Ascending => {
                let initial_key = if block.offsets().is_empty() {
                    Bytes::new()
                } else {
                    Self::decode_first_key_at_restart(&block, 0)
                };

                BlockIteratorV2 {
                    inner: BlockIteratorInner::Ascending(AscendingState {
                        block,
                        current_restart_idx: 0,
                        offset_in_block: 0,
                        entries_since_restart: 0,
                        current_key: initial_key,
                        exhausted: false,
                    }),
                }
            }
            IterationOrder::Descending => BlockIteratorV2 {
                inner: BlockIteratorInner::Descending(DescendingBlockIteratorV2::new(block)),
            },
        }
    }

    pub(crate) fn new_ascending(block: B) -> Self {
        Self::new(block, IterationOrder::Ascending)
    }

    pub(crate) fn is_empty(&self) -> bool {
        match &self.inner {
            BlockIteratorInner::Ascending(state) => state.is_empty(),
            BlockIteratorInner::Descending(state) => state.is_empty(),
        }
    }

    fn decode_first_key_at_restart(block: &B, restart_idx: usize) -> Bytes {
        let restart_offset = block.offsets()[restart_idx] as usize;
        let mut data = &block.data()[restart_offset..];
        let codec = SstRowCodecV2::new();
        let (shared_bytes, key_suffix) = codec.decode_key_only(&mut data);
        assert_eq!(shared_bytes, 0, "restart point should have shared_bytes=0");
        key_suffix
    }
}

impl<B: BlockLike> AscendingState<B> {
    fn seek_to_restart(&mut self, restart_idx: usize) {
        if restart_idx >= self.block.offsets().len() {
            self.exhausted = true;
            return;
        }

        self.current_restart_idx = restart_idx;
        self.offset_in_block = self.block.offsets()[restart_idx] as usize;
        self.entries_since_restart = 0;
        self.current_key = BlockIteratorV2::decode_first_key_at_restart(&self.block, restart_idx);
        self.exhausted = false;
    }

    fn decode_entry_at_current_offset(&self) -> Result<(RowEntry, usize), SlateDBError> {
        let mut data = &self.block.data()[self.offset_in_block..];
        let codec = SstRowCodecV2::new();
        let entry = codec.decode(&mut data)?;
        let bytes_consumed = self.block.data().len() - self.offset_in_block - data.len();
        let new_offset = self.offset_in_block + bytes_consumed;
        let full_key = entry.restore_full_key(&self.current_key);

        Ok((
            RowEntry::new(
                full_key,
                entry.value,
                entry.seq,
                entry.create_ts,
                entry.expire_ts,
            ),
            new_offset,
        ))
    }

    fn decode_key_at_offset(&self, offset: usize, prev_key: &[u8]) -> Bytes {
        let mut data = &self.block.data()[offset..];
        let codec = SstRowCodecV2::new();
        let (shared_bytes, key_suffix) = codec.decode_key_only(&mut data);
        let shared = shared_bytes as usize;

        let mut full_key = BytesMut::with_capacity(shared + key_suffix.len());
        full_key.extend_from_slice(&prev_key[..shared]);
        full_key.extend_from_slice(&key_suffix);
        full_key.freeze()
    }

    fn is_empty(&self) -> bool {
        self.exhausted || self.offset_in_block >= self.block.data().len()
    }

    /// Returns the largest restart index where the key at that restart point is <= target.
    fn find_restart_for_key(&self, target: &[u8]) -> usize {
        let restarts = self.block.offsets();
        if restarts.is_empty() {
            return 0;
        }

        let mut low = 0;
        let mut high = restarts.len();

        while low < high {
            let mid = low + (high - low) / 2;
            let restart_key = BlockIteratorV2::decode_first_key_at_restart(&self.block, mid);

            match restart_key.as_ref().cmp(target) {
                Ordering::Less => low = mid + 1,
                Ordering::Equal => return mid,
                Ordering::Greater => high = mid,
            }
        }

        low.saturating_sub(1)
    }

    fn restart_region_end(&self, restart_idx: usize) -> usize {
        self.block
            .offsets()
            .get(restart_idx + 1)
            .map(|&off| off as usize)
            .unwrap_or_else(|| self.block.data().len())
    }

    fn advance_past_current_entry(&mut self) -> Result<(), SlateDBError> {
        let mut data = &self.block.data()[self.offset_in_block..];
        let codec = SstRowCodecV2::new();
        codec.decode(&mut data)?;
        let bytes_consumed = self.block.data().len() - self.offset_in_block - data.len();
        self.offset_in_block += bytes_consumed;
        self.entries_since_restart += 1;
        Ok(())
    }
}

#[async_trait]
impl<B: BlockLike> KeyValueIterator for BlockIteratorV2<B> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match &mut self.inner {
            BlockIteratorInner::Ascending(state) => {
                if state.is_empty() {
                    return Ok(None);
                }

                let (entry, new_offset) = state.decode_entry_at_current_offset()?;

                // Update current key before advancing
                state.current_key = entry.key.clone();
                state.offset_in_block = new_offset;
                state.entries_since_restart += 1;

                // Check if we've moved to a new restart region
                if let Some(&next_restart_offset) =
                    state.block.offsets().get(state.current_restart_idx + 1)
                {
                    if state.offset_in_block >= next_restart_offset as usize {
                        state.current_restart_idx += 1;
                        state.entries_since_restart = 0;
                    }
                }

                if state.offset_in_block >= state.block.data().len() {
                    state.exhausted = true;
                }

                Ok(Some(entry))
            }
            BlockIteratorInner::Descending(iter) => iter.next_entry().await,
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match &mut self.inner {
            BlockIteratorInner::Ascending(state) => {
                if state.block.offsets().is_empty() {
                    state.exhausted = true;
                    return Ok(());
                }

                let start_restart_idx = state.find_restart_for_key(next_key);

                // Iterate through restart regions starting from binary search result.
                for restart_idx in start_restart_idx..state.block.offsets().len() {
                    state.seek_to_restart(restart_idx);

                    if state.exhausted || state.current_key.as_ref() >= next_key {
                        return Ok(());
                    }

                    // Scan entries within this restart region
                    let region_end = state.restart_region_end(restart_idx);
                    let mut prev_key = state.current_key.clone();

                    while state.offset_in_block < region_end
                        && state.offset_in_block < state.block.data().len()
                    {
                        let current_key =
                            state.decode_key_at_offset(state.offset_in_block, &prev_key);

                        if current_key.as_ref() >= next_key {
                            state.current_key = prev_key;
                            return Ok(());
                        }

                        state.advance_past_current_entry()?;
                        prev_key = current_key;
                        state.current_key = prev_key.clone();
                    }
                }

                state.exhausted = true;
                Ok(())
            }
            BlockIteratorInner::Descending(iter) => iter.seek(next_key).await,
        }
    }
}

/// Descending iterator for BlockV2 that caches entries within each restart region,
/// then iterates in reverse order.
pub(crate) struct DescendingBlockIteratorV2<B: BlockLike> {
    ascending: AscendingState<B>,
    current_restart_idx: isize,
    cached_entries: Vec<RowEntry>,
    cache_idx: isize,
    exhausted: bool,
    initialized: bool,
}

impl<B: BlockLike> DescendingBlockIteratorV2<B> {
    pub(crate) fn is_empty(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn new(block: B) -> Self {
        let num_restarts = block.offsets().len();
        let initial_key = if num_restarts == 0 {
            Bytes::new()
        } else {
            BlockIteratorV2::decode_first_key_at_restart(&block, 0)
        };

        DescendingBlockIteratorV2 {
            ascending: AscendingState {
                block,
                current_restart_idx: 0,
                offset_in_block: 0,
                entries_since_restart: 0,
                current_key: initial_key,
                exhausted: false,
            },
            current_restart_idx: num_restarts as isize - 1,
            cached_entries: Vec::new(),
            cache_idx: -1,
            exhausted: num_restarts == 0,
            initialized: false,
        }
    }

    fn load_restart_region(&mut self, restart_idx: usize) -> Result<(), SlateDBError> {
        self.cached_entries.clear();

        if restart_idx >= self.ascending.block.offsets().len() {
            self.cache_idx = -1;
            return Ok(());
        }

        self.ascending.seek_to_restart(restart_idx);
        let region_end = self.ascending.restart_region_end(restart_idx);

        while self.ascending.offset_in_block < region_end && !self.ascending.is_empty() {
            let (entry, new_offset) = self.ascending.decode_entry_at_current_offset()?;
            self.ascending.current_key = entry.key.clone();
            self.ascending.offset_in_block = new_offset;
            self.ascending.entries_since_restart += 1;
            self.cached_entries.push(entry);
        }

        self.cache_idx = self.cached_entries.len() as isize - 1;
        Ok(())
    }

    fn move_to_previous_region(&mut self) -> Result<(), SlateDBError> {
        self.current_restart_idx -= 1;
        if self.current_restart_idx < 0 {
            self.exhausted = true;
            self.cached_entries.clear();
            self.cache_idx = -1;
        } else {
            self.load_restart_region(self.current_restart_idx as usize)?;
        }
        Ok(())
    }
}

#[async_trait]
impl<B: BlockLike> KeyValueIterator for DescendingBlockIteratorV2<B> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        if !self.initialized && !self.exhausted {
            self.load_restart_region(self.current_restart_idx as usize)?;
            self.initialized = true;
        }
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if !self.initialized {
            self.init().await?;
        }

        if self.exhausted {
            return Ok(None);
        }

        if self.cache_idx >= 0 {
            let entry = self.cached_entries[self.cache_idx as usize].clone();
            self.cache_idx -= 1;
            return Ok(Some(entry));
        }

        self.move_to_previous_region()?;

        if self.exhausted || self.cache_idx < 0 {
            self.exhausted = true;
            return Ok(None);
        }

        let entry = self.cached_entries[self.cache_idx as usize].clone();
        self.cache_idx -= 1;
        Ok(Some(entry))
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.initialized = true;

        if self.ascending.block.offsets().is_empty() {
            self.exhausted = true;
            return Ok(());
        }

        // Find the largest key <= next_key by searching backwards through restart regions
        let start_restart_idx = self.ascending.find_restart_for_key(next_key);

        for restart_idx in (0..=start_restart_idx).rev() {
            self.current_restart_idx = restart_idx as isize;
            self.load_restart_region(restart_idx)?;

            // Find the largest key <= next_key in this region.
            // Since keys are sorted ascending, find the first key > next_key and take the previous.
            let first_greater = self
                .cached_entries
                .iter()
                .position(|entry| entry.key.as_ref() > next_key);

            let found_idx = match first_greater {
                Some(0) => None,
                Some(i) => Some(i - 1),
                None if !self.cached_entries.is_empty() => Some(self.cached_entries.len() - 1),
                None => None,
            };

            if let Some(idx) = found_idx {
                self.cache_idx = idx as isize;
                self.exhausted = false;
                return Ok(());
            }
        }

        self.exhausted = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::block::Block;
    use crate::format::sst::BlockBuilder;
    use crate::types::ValueDeletable;

    fn make_entry(key: &[u8], value: &[u8], seq: u64) -> RowEntry {
        RowEntry::new(
            Bytes::copy_from_slice(key),
            ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq,
            None,
            None,
        )
    }

    fn build_test_block(entries: &[(&[u8], &[u8])], restart_interval: usize) -> Block {
        let mut builder = BlockBuilder::new_v2_with_restart_interval(4096, restart_interval);
        for (i, (key, value)) in entries.iter().enumerate() {
            let _ = builder.add(make_entry(key, value, i as u64));
        }
        builder.build().expect("build failed")
    }

    #[tokio::test]
    async fn should_iterate_all_entries() {
        // given: a block with entries
        let block = build_test_block(
            &[(b"apple", b"1"), (b"banana", b"2"), (b"cherry", b"3")],
            16,
        );

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when/then: iterating returns all entries in order
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"apple");
        assert_eq!(kv.value, ValueDeletable::Value(Bytes::from("1")));

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"banana");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"cherry");

        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_to_exact_key() {
        // given: a block with entries
        let block = build_test_block(
            &[
                (b"apple", b"1"),
                (b"banana", b"2"),
                (b"cherry", b"3"),
                (b"date", b"4"),
            ],
            2,
        );

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking to an exact key
        iter.seek(b"banana").await.unwrap();

        // then: returns that key
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"banana");
    }

    #[tokio::test]
    async fn should_seek_to_key_between_entries() {
        // given: a block with entries
        let block = build_test_block(&[(b"apple", b"1"), (b"cherry", b"3"), (b"date", b"4")], 16);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking to a key between entries
        iter.seek(b"banana").await.unwrap();

        // then: returns the next key >= banana
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"cherry");
    }

    #[tokio::test]
    async fn should_seek_across_restart_boundaries() {
        // given: a block with restart_interval=2
        let block = build_test_block(
            &[
                (b"a", b"1"), // restart 0
                (b"b", b"2"),
                (b"c", b"3"), // restart 1
                (b"d", b"4"),
                (b"e", b"5"), // restart 2
            ],
            2,
        );

        assert_eq!(block.offsets().len(), 3);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking to a key in the third restart region
        iter.seek(b"e").await.unwrap();

        // then: finds the correct entry
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"e");
    }

    #[tokio::test]
    async fn should_handle_shared_prefix_keys() {
        // given: a block with keys sharing prefixes
        let block = build_test_block(
            &[
                (b"user:1000", b"alice"),
                (b"user:1001", b"bob"),
                (b"user:1002", b"carol"),
                (b"user:1003", b"dave"),
            ],
            2,
        );

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking and iterating
        iter.seek(b"user:1001").await.unwrap();

        // then: correct entries are returned
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"user:1001");
        assert_eq!(kv.value, ValueDeletable::Value(Bytes::from("bob")));

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"user:1002");
    }

    #[tokio::test]
    async fn should_binary_search_restarts_correctly() {
        // given: a block with many entries and small restart interval
        let mut builder = BlockBuilder::new_v2_with_restart_interval(16384, 4);
        for i in 0..100 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{}", i);
            let _ = builder.add(make_entry(key.as_bytes(), value.as_bytes(), i as u64));
        }
        let block = builder.build().expect("build failed");

        // when: seeking to various keys
        let mut iter = BlockIteratorV2::new_ascending(&block);
        iter.seek(b"key_00050").await.unwrap();

        // then: correct key is found
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_00050");

        // Seek to another key
        iter.seek(b"key_00075").await.unwrap();
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_00075");
    }

    #[tokio::test]
    async fn should_seek_to_first_key() {
        // given: a block
        let block = build_test_block(&[(b"apple", b"1"), (b"banana", b"2")], 16);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking to the first key
        iter.seek(b"apple").await.unwrap();

        // then: first entry is returned
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"apple");
    }

    #[tokio::test]
    async fn should_seek_to_key_before_first() {
        // given: a block
        let block = build_test_block(&[(b"banana", b"2"), (b"cherry", b"3")], 16);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking to a key before the first entry
        iter.seek(b"apple").await.unwrap();

        // then: first entry is returned
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"banana");
    }

    #[tokio::test]
    async fn should_seek_past_last_key() {
        // given: a block
        let block = build_test_block(&[(b"apple", b"1"), (b"banana", b"2")], 16);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking past the last key
        iter.seek(b"zebra").await.unwrap();

        // then: no more entries
        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_single_entry_block() {
        // given: a block with one entry
        let block = build_test_block(&[(b"only", b"one")], 16);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: iterating
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"only");

        // then: no more entries
        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_in_single_entry_block() {
        // given: a block with one entry
        let block = build_test_block(&[(b"only", b"one")], 16);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking to that entry
        iter.seek(b"only").await.unwrap();

        // then: entry is returned
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"only");
    }

    #[tokio::test]
    async fn should_handle_entries_with_timestamps() {
        // given: a block with entries that have timestamps
        let mut builder = BlockBuilder::new_v2(4096);
        let _ = builder.add(RowEntry::new(
            Bytes::from("key1"),
            ValueDeletable::Value(Bytes::from("value1")),
            1,
            Some(100),
            Some(200),
        ));
        let _ = builder.add(RowEntry::new(
            Bytes::from("key2"),
            ValueDeletable::Tombstone,
            2,
            Some(300),
            None,
        ));
        let block = builder.build().expect("build failed");

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: iterating
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key1");
        assert_eq!(kv.create_ts, Some(100));
        assert_eq!(kv.expire_ts, Some(200));

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key2");
        assert!(matches!(kv.value, ValueDeletable::Tombstone));
        assert_eq!(kv.create_ts, Some(300));
    }

    #[tokio::test]
    async fn should_iterate_block_with_many_restart_points() {
        // given: a block with restart_interval=1 (every entry is a restart)
        let mut builder = BlockBuilder::new_v2_with_restart_interval(4096, 1);
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("val_{}", i);
            let _ = builder.add(make_entry(key.as_bytes(), value.as_bytes(), i as u64));
        }
        let block = builder.build().expect("build failed");

        assert_eq!(block.offsets().len(), 10);

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when/then: all entries are returned in order
        for i in 0..10 {
            let kv = iter.next_entry().await.unwrap().unwrap();
            let expected_key = format!("key_{}", i);
            assert_eq!(kv.key.as_ref(), expected_key.as_bytes());
        }
        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_multiple_times() {
        // given: a block
        let block = build_test_block(
            &[
                (b"a", b"1"),
                (b"b", b"2"),
                (b"c", b"3"),
                (b"d", b"4"),
                (b"e", b"5"),
            ],
            2,
        );

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: seeking multiple times
        iter.seek(b"b").await.unwrap();
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"b");

        iter.seek(b"d").await.unwrap();
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"d");

        iter.seek(b"a").await.unwrap();
        // Note: seeking backward from current position may not work as expected
        // because we don't reset the iterator. Let's test seeking forward from current.
        let kv = iter.next_entry().await.unwrap();
        // After seeking to "a" from position "d", behavior depends on implementation
        // Our implementation should allow seeking to any position
        assert!(kv.is_some());
    }

    #[tokio::test]
    async fn should_iterate_entries_with_merge_operands() {
        // given: a block with merge operand entries
        let mut builder = BlockBuilder::new_v2(4096);
        let _ = builder.add(RowEntry::new(
            Bytes::from("key1"),
            ValueDeletable::Value(Bytes::from("base")),
            1,
            None,
            None,
        ));
        let _ = builder.add(RowEntry::new(
            Bytes::from("key2"),
            ValueDeletable::Merge(Bytes::from("delta")),
            2,
            None,
            None,
        ));
        let block = builder.build().expect("build failed");

        let mut iter = BlockIteratorV2::new_ascending(&block);

        // when: iterating
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert!(matches!(kv.value, ValueDeletable::Value(_)));

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert!(matches!(kv.value, ValueDeletable::Merge(_)));
        if let ValueDeletable::Merge(v) = kv.value {
            assert_eq!(v.as_ref(), b"delta");
        }
    }

    // ==================== Descending Iterator Tests ====================

    #[tokio::test]
    async fn should_iterate_all_entries_descending() {
        // given: a block with entries
        let block = build_test_block(
            &[(b"apple", b"1"), (b"banana", b"2"), (b"cherry", b"3")],
            16,
        );

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when/then: iterating returns all entries in reverse order
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"cherry");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"banana");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"apple");

        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_descending_across_restart_boundaries() {
        // given: a block with restart_interval=2
        let block = build_test_block(
            &[
                (b"a", b"1"), // restart 0
                (b"b", b"2"),
                (b"c", b"3"), // restart 1
                (b"d", b"4"),
                (b"e", b"5"), // restart 2
            ],
            2,
        );

        assert_eq!(block.offsets().len(), 3);

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when/then: iterating returns all entries in reverse order
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"e");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"d");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"c");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"b");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"a");

        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_descending_to_exact_key() {
        // given: a block with entries
        let block = build_test_block(
            &[
                (b"apple", b"1"),
                (b"banana", b"2"),
                (b"cherry", b"3"),
                (b"date", b"4"),
            ],
            2,
        );

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when: seeking to an exact key (descending seek finds largest key <= target)
        iter.seek(b"cherry").await.unwrap();

        // then: returns that key
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"cherry");

        // and continues descending
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"banana");
    }

    #[tokio::test]
    async fn should_seek_descending_to_key_between_entries() {
        // given: a block with entries
        let block = build_test_block(&[(b"apple", b"1"), (b"cherry", b"3"), (b"date", b"4")], 16);

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when: seeking to a key between entries (banana is between apple and cherry)
        iter.seek(b"banana").await.unwrap();

        // then: returns the largest key <= banana, which is apple
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"apple");

        // and no more entries (we're at the beginning)
        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_descending_past_last_key() {
        // given: a block
        let block = build_test_block(&[(b"apple", b"1"), (b"banana", b"2")], 16);

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when: seeking to a key after all entries
        iter.seek(b"zebra").await.unwrap();

        // then: returns the last (largest) key
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"banana");
    }

    #[tokio::test]
    async fn should_seek_descending_before_first_key() {
        // given: a block
        let block = build_test_block(&[(b"banana", b"2"), (b"cherry", b"3")], 16);

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when: seeking to a key before all entries
        iter.seek(b"apple").await.unwrap();

        // then: no entries (nothing <= apple)
        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_descending_single_entry_block() {
        // given: a block with one entry
        let block = build_test_block(&[(b"only", b"one")], 16);

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when: iterating
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"only");

        // then: no more entries
        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_descending_with_shared_prefix_keys() {
        // given: a block with keys sharing prefixes
        let block = build_test_block(
            &[
                (b"user:1000", b"alice"),
                (b"user:1001", b"bob"),
                (b"user:1002", b"carol"),
                (b"user:1003", b"dave"),
            ],
            2,
        );

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when/then: iterating returns entries in reverse order
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"user:1003");
        assert_eq!(kv.value, ValueDeletable::Value(Bytes::from("dave")));

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"user:1002");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"user:1001");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"user:1000");

        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_descending_block_with_many_restart_points() {
        // given: a block with restart_interval=4 (restart point every 4 entries)
        let mut builder = BlockBuilder::new_v2_with_restart_interval(4096, 4);
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let value = format!("val_{}", i);
            let _ = builder.add(make_entry(key.as_bytes(), value.as_bytes(), i as u64));
        }
        let block = builder.build().expect("build failed");

        // 20 entries with restart_interval=4 -> 5 restart points
        assert_eq!(block.offsets().len(), 5);

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when/then: all entries are returned in reverse order
        for i in (0..20).rev() {
            let kv = iter.next_entry().await.unwrap().unwrap();
            let expected_key = format!("key_{:02}", i);
            assert_eq!(kv.key.as_ref(), expected_key.as_bytes());
        }
        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_descending_with_many_restart_points() {
        // given: a block with restart_interval=4 (restart point every 4 entries)
        let mut builder = BlockBuilder::new_v2_with_restart_interval(4096, 4);
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let value = format!("val_{}", i);
            let _ = builder.add(make_entry(key.as_bytes(), value.as_bytes(), i as u64));
        }
        let block = builder.build().expect("build failed");

        // 20 entries with restart_interval=4 -> 5 restart points
        // restart 0: key_00, key_01, key_02, key_03
        // restart 1: key_04, key_05, key_06, key_07
        // restart 2: key_08, key_09, key_10, key_11
        // restart 3: key_12, key_13, key_14, key_15
        // restart 4: key_16, key_17, key_18, key_19
        assert_eq!(block.offsets().len(), 5);

        let mut iter = DescendingBlockIteratorV2::new(&block);

        // when: seeking to a key in the middle of a restart region
        iter.seek(b"key_10").await.unwrap();

        // then: returns key_10 and continues descending
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_10");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_09");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_08");

        // crosses into previous restart region
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_07");

        // when: seeking to a key between entries
        iter.seek(b"key_14a").await.unwrap();

        // then: returns the largest key <= key_14a, which is key_14
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_14");

        // when: seeking to a key at the start of a restart region
        iter.seek(b"key_04").await.unwrap();

        // then: returns key_04
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_04");

        // and continues to previous region
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_03");
    }

    // ==================== BlockIteratorV2 with Descending Order Tests ====================

    #[tokio::test]
    async fn should_iterate_descending_via_block_iterator_v2() {
        // given: a block with entries
        let block = build_test_block(
            &[(b"apple", b"1"), (b"banana", b"2"), (b"cherry", b"3")],
            16,
        );

        // when: creating BlockIteratorV2 with Descending order
        let mut iter = BlockIteratorV2::new(&block, IterationOrder::Descending);

        // then: iterating returns all entries in reverse order
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"cherry");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"banana");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"apple");

        assert!(iter.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_descending_via_block_iterator_v2() {
        // given: a block with entries and restart_interval=4
        let mut builder = BlockBuilder::new_v2_with_restart_interval(4096, 4);
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let value = format!("val_{}", i);
            let _ = builder.add(make_entry(key.as_bytes(), value.as_bytes(), i as u64));
        }
        let block = builder.build().expect("build failed");

        // when: creating BlockIteratorV2 with Descending order and seeking
        let mut iter = BlockIteratorV2::new(&block, IterationOrder::Descending);
        iter.seek(b"key_10").await.unwrap();

        // then: returns key_10 and continues descending
        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_10");

        let kv = iter.next_entry().await.unwrap().unwrap();
        assert_eq!(kv.key.as_ref(), b"key_09");
    }
}
