use async_trait::async_trait;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::SlateDBError;
use crate::iter::{RowEntryIterator, TrackedRowEntryIterator};
use crate::types::ValueDeletable::Tombstone;
use crate::types::{RowEntry, ValueDeletable};
use slatedb_common::metrics::CounterFn;

/// Counters for expired entries purged by [`RetentionIterator`], split by
/// `entry_type` label.
///
/// - `entry_type="value"`: expired value entries rewritten as tombstones. A
///   user-written `CompactionFilter` can't observe the rewrite (the tombstone
///   reaches the filter with `expire_ts: None`, indistinguishable from one the
///   user wrote), so the counter surfaces it.
/// - `entry_type="merge"`: expired merge entries skipped by the iterator
///   (no tombstone is written).
#[derive(Clone)]
pub(crate) struct RetentionMetrics {
    pub(crate) expired_entries_purged_value: Arc<dyn CounterFn>,
    pub(crate) expired_entries_purged_merge: Arc<dyn CounterFn>,
}

/// Filters key versions by snapshot sequence and handles expired entries and tombstones.
///
/// The upstream iterator must group entries by key, with newer sequence numbers first.
pub(crate) struct RetentionIterator<T: RowEntryIterator> {
    /// The upstream iterator providing entries in decreasing order of sequence numbers
    inner: T,
    /// The min sequence number to retain. It's taken from the minimum sequence number of the
    /// active snapshots.
    retention_min_seq: Option<u64>,
    /// Buffer for collecting and processing multiple versions of the same key
    buffer: RetentionBuffer,
    /// Whether to filter out tombstones
    filter_tombstone: bool,
    /// The current timestamp when the compaction started. This is a local timestamp,
    /// and used on handling expired entries.
    compaction_start_ts: i64,
    /// Optional counters for observing expire_ts-driven decisions. Populated on
    /// the compaction path; `None` on the flush path.
    metrics: Option<RetentionMetrics>,
}

impl<T: RowEntryIterator> RetentionIterator<T> {
    /// Creates a new retention iterator with the specified retention policy
    pub(crate) async fn new(
        inner: T,
        retention_min_seq: Option<u64>,
        filter_tombstone: bool,
        compaction_start_ts: i64,
        metrics: Option<RetentionMetrics>,
    ) -> Result<Self, SlateDBError> {
        Ok(Self {
            inner,
            retention_min_seq,
            filter_tombstone,
            compaction_start_ts,
            metrics,
            buffer: RetentionBuffer::new(),
        })
    }

    /// Applies retention filtering to a collection of versions for the same key
    ///
    /// This function implements the following retention logic:
    ///
    /// - Keeps the versions needed by active snapshots and merge operations.
    /// - Converts expired values to tombstones and drops expired merge entries.
    /// - Removes trailing tombstones when `filter_tombstone` is true.
    fn apply_retention_filter(
        versions: BTreeMap<Reverse<u64>, RowEntry>,
        compaction_start_ts: i64,
        retention_min_seq: Option<u64>,
        filter_tombstone: bool,
        metrics: Option<&RetentionMetrics>,
    ) -> BTreeMap<Reverse<u64>, RowEntry> {
        let mut filtered_versions = BTreeMap::new();
        for (_, entry) in versions.into_iter() {
            // filter out any expired entries -- eventually we can consider
            // abstracting this away into generic, pluggable compaction filters
            // but for now we do it inline
            let is_merge = matches!(&entry.value, ValueDeletable::Merge(_));
            let entry = match entry.expire_ts.as_ref() {
                Some(expire_ts) if *expire_ts <= compaction_start_ts => {
                    if is_merge {
                        // just skip expired merge entries rather than write a tombstone
                        // as earlier merges may still be un-expired
                        if let Some(m) = metrics {
                            m.expired_entries_purged_merge.increment(1);
                        }
                        continue;
                    }
                    // for values, insert a tombstone instead of just filtering out the
                    // value in the iterator because this may otherwise "revive"
                    // an older version of the KV pair that has a larger TTL in
                    // a lower level of the LSM tree
                    if let Some(m) = metrics {
                        m.expired_entries_purged_value.increment(1);
                    }
                    RowEntry {
                        key: entry.key,
                        value: Tombstone,
                        seq: entry.seq,
                        expire_ts: None,
                        create_ts: entry.create_ts,
                    }
                }
                _ => entry,
            };

            let entry_seq = entry.seq;

            // always keep the entry with latest version.
            filtered_versions.insert(Reverse(entry.seq), entry);

            // Keep versions newer than the minimum snapshot sequence and the first version
            // at or below it. Snapshots need that boundary version when sequences do not match.
            // Keep merge entries until their base value or tombstone is available.
            //
            // Example:
            //   1. Write a = 'v0' (seq=1).
            //   2. Write a = 'v1' (seq=2).
            //   3. Write b = 'v2' (seq=3), then create snapshot s1.
            //   4. Delete a (seq=4).
            //   5. Compact with retention_min_seq=3.
            //
            // Keep the tombstone (seq=4) and 'v1' (seq=2). Drop 'v0' (seq=1).
            // Snapshot s1 needs 'v1', the first value at or below its sequence.
            // Without it, s1 would lose the value that existed before the delete.
            let continue_retain_by_seq = retention_min_seq
                .map(|min_seq| entry_seq > min_seq)
                .unwrap_or(false);

            let continue_retain = continue_retain_by_seq || is_merge;
            if !continue_retain {
                // Drop versions older than the boundary value.
                break;
            }
        }

        if filter_tombstone {
            // remove the tombstones in the tail
            while filtered_versions
                .iter()
                .last()
                .map(|(_, entry)| entry.value.is_tombstone())
                .unwrap_or(false)
            {
                filtered_versions.pop_last();
            }
        }

        filtered_versions
    }
}

#[async_trait]
impl<T: RowEntryIterator> RowEntryIterator for RetentionIterator<T> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.inner.init().await?;
        Ok(())
    }

    /// Retrieves the next entry from the retention iterator
    ///
    /// This method implements a state machine that:
    /// 1. Collects all versions of the current key from the upstream iterator
    /// 2. Applies retention filtering to the collected versions
    /// 3. Returns filtered entries one by one in sequence number order (newest first)
    ///
    /// The state machine ensures efficient processing by batching operations for each key.
    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            match self.buffer.state() {
                RetentionBufferState::NeedPush => {
                    // Fetch next entry from upstream iterator
                    let entry = match self.inner.next().await? {
                        Some(entry) => entry,
                        None => {
                            // No more entries from upstream, mark end of input
                            self.buffer.mark_end_of_input();
                            continue;
                        }
                    };

                    // Add entry to buffer (may trigger state change if key changes)
                    self.buffer.push(entry);
                }
                RetentionBufferState::NeedPopAndContinue => {
                    // Return next filtered entry, continue processing current key
                    match self.buffer.pop() {
                        Some(entry) => return Ok(Some(entry)),
                        None => continue, // Buffer empty, need to collect more entries
                    }
                }
                RetentionBufferState::NeedPopAndQuit => {
                    // Return next filtered entry, no more entries available
                    return Ok(self.buffer.pop());
                }
                RetentionBufferState::NeedProcess => {
                    // Apply retention filtering to collected versions
                    let compaction_start_ts = self.compaction_start_ts;
                    let retention_min_seq = self.retention_min_seq;
                    let metrics = self.metrics.clone();
                    self.buffer.process_retention(|versions| {
                        Self::apply_retention_filter(
                            versions,
                            compaction_start_ts,
                            retention_min_seq,
                            self.filter_tombstone,
                            metrics.as_ref(),
                        )
                    })?;
                }
            }
        }
    }

    /// Seeks to the specified key in the upstream iterator
    ///
    /// Clears the internal buffer and resets the iterator state to begin processing
    /// from the specified key position.
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.buffer.clear();
        self.inner.seek(next_key).await?;
        Ok(())
    }
}

impl<T: TrackedRowEntryIterator> TrackedRowEntryIterator for RetentionIterator<T> {
    fn bytes_processed(&self) -> u64 {
        self.inner.bytes_processed()
    }
}

/// A buffer that collects and manages multiple versions of the same key from an iterator.
///
/// This buffer implements a state machine to efficiently collect all versions of a key
/// before applying retention filtering. It maintains the current key's versions and
/// a preview of the next key's first entry.
struct RetentionBuffer {
    /// All versions of the current key being processed, ordered by sequence number (latest first)
    current_versions: BTreeMap<Reverse<u64>, RowEntry>,
    /// The first entry of the next key (if available). This is used to note the current key has
    /// been exhausted, and await to process the current key versions.
    next: Option<RowEntry>,
    /// After the current key versions have been exhausted, process_retention will be called, and
    /// this flag will be set to true.
    processed: bool,
    /// Whether the upstream iterator has reached end of input.
    end_of_input: bool,
}

/// This enum drives the behavior of the retention iterator's main loop, determining what action
/// should be taken next.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum RetentionBufferState {
    /// Need to fetch and push the next entry from upstream iterator.
    NeedPush,
    /// Need to pop the next filtered entry and continue processing current key
    NeedPopAndContinue,
    /// Need to pop the next filtered entry and quit (end of input reached)
    NeedPopAndQuit,
    /// Need to apply retention filtering to collected versions
    NeedProcess,
}

impl RetentionBuffer {
    /// Creates a new empty retention buffer
    fn new() -> Self {
        Self {
            current_versions: BTreeMap::new(),
            next: None,
            processed: false,
            end_of_input: false,
        }
    }

    /// Determines the current state of the buffer state machine
    ///
    /// This method implements the state transition logic based on:
    /// - Whether current versions have been processed
    /// - Whether end of input has been reached
    /// - Whether a next entry is available
    ///
    /// # Returns
    /// The current state indicating what action should be taken next
    fn state(&self) -> RetentionBufferState {
        if self.processed {
            if self.end_of_input {
                return RetentionBufferState::NeedPopAndQuit;
            } else {
                return RetentionBufferState::NeedPopAndContinue;
            }
        }
        if self.end_of_input || self.next.is_some() {
            return RetentionBufferState::NeedProcess;
        }
        RetentionBufferState::NeedPush
    }

    /// Clears the buffer and resets all state flags
    ///
    /// Called when seeking to a new position in the iterator.
    fn clear(&mut self) {
        self.current_versions.clear();
        self.next = None;
        self.processed = false;
        self.end_of_input = false;
    }

    /// Marks that the upstream iterator has reached end of input
    ///
    /// This triggers state transitions to handle the final processing of remaining entries.
    fn mark_end_of_input(&mut self) {
        self.end_of_input = true;
    }

    /// Appends an entry to the buffer
    ///
    /// This method handles key transitions by detecting when a new key is encountered.
    /// It maintains the invariant that all versions of the current key are collected
    /// before moving to the next key.
    fn push(&mut self, entry: RowEntry) {
        let current_key = match self.current_versions.values().next() {
            Some(entry) => entry.key.clone(),
            None => {
                // If current versions are empty, this is the first entry
                self.current_versions.insert(Reverse(entry.seq), entry);
                return;
            }
        };

        // Different key detected - store as next entry and signal key transition
        if entry.key != current_key {
            self.next = Some(entry);
            return;
        }

        // Same key - append to current versions
        self.current_versions.insert(Reverse(entry.seq), entry);
    }

    /// Applies retention filtering to the collected versions
    fn process_retention(
        &mut self,
        f: impl FnOnce(BTreeMap<Reverse<u64>, RowEntry>) -> BTreeMap<Reverse<u64>, RowEntry>,
    ) -> Result<(), SlateDBError> {
        if self.processed {
            return Ok(());
        }
        let current_versions = std::mem::take(&mut self.current_versions);
        let processed_versions = f(current_versions);
        self.current_versions = processed_versions;
        self.processed = true;
        Ok(())
    }

    /// Pops the next entry from the current versions
    /// When current versions are exhausted, it promotes the next entry to become the new current key.
    fn pop(&mut self) -> Option<RowEntry> {
        match self.current_versions.pop_first() {
            Some((_, entry)) => Some(entry),
            None => {
                // Current versions exhausted - promote next entry to current versions
                let next = self.next.take();
                if let Some(entry) = next {
                    self.current_versions.insert(Reverse(entry.seq), entry);
                    self.processed = false;
                    None // Signal that we need to continue processing
                } else if self.end_of_input {
                    // No next entry and at end of input - we're done
                    None
                } else {
                    // No next entry but not at end of input - this shouldn't happen
                    unreachable!("No next entry but not at end of input - this shouldn't happen");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RowEntry;
    use rstest::rstest;

    struct RetentionBufferTestCase {
        name: &'static str,
        build: fn() -> RetentionBuffer,
        expected_current_versions_len: usize,
        expected_has_next: bool,
        expected_processed: bool,
        expected_end_of_input: bool,
        expected_state: RetentionBufferState,
    }

    // Table-driven test for complex scenarios
    #[rstest]
    #[case(RetentionBufferTestCase {
        name: "empty_buffer",
        build: || RetentionBuffer::new(),
        expected_current_versions_len: 0,
        expected_has_next: false,
        expected_processed: false,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPush,
    })]
    #[case(RetentionBufferTestCase {
        name: "single_entry",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer
        },
        expected_current_versions_len: 1,
        expected_has_next: false,
        expected_processed: false,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPush,
    })]
    #[case(RetentionBufferTestCase {
        name: "key_transition",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.push(RowEntry::new_value(b"key2", b"value2", 2));
            buffer
        },
        expected_current_versions_len: 1,
        expected_has_next: true,
        expected_processed: false,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedProcess,
    })]
    #[case(RetentionBufferTestCase {
        name: "processed_state",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.process_retention(|versions| versions).unwrap();
            buffer
        },
        expected_current_versions_len: 1,
        expected_has_next: false,
        expected_processed: true,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPopAndContinue,
    })]
    #[case(RetentionBufferTestCase {
        name: "end_of_input_processed",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.mark_end_of_input();
            buffer.process_retention(|versions| versions).unwrap();
            buffer
        },
        expected_current_versions_len: 1,
        expected_has_next: false,
        expected_processed: true,
        expected_end_of_input: true,
        expected_state: RetentionBufferState::NeedPopAndQuit,
    })]
    #[case(RetentionBufferTestCase {
        name: "multiple_versions_same_key",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.push(RowEntry::new_value(b"key1", b"value2", 2));
            buffer.push(RowEntry::new_value(b"key1", b"value3", 3));
            buffer
        },
        expected_current_versions_len: 3,
        expected_has_next: false,
        expected_processed: false,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPush,
    })]
    #[case(RetentionBufferTestCase {
        name: "pop_operation",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.push(RowEntry::new_value(b"key1", b"value2", 2));
            buffer.process_retention(|versions| versions).unwrap();
            buffer.pop(); // Execute pop operation in the build function
            buffer
        },
        expected_current_versions_len: 1,
        expected_has_next: false,
        expected_processed: true,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPopAndContinue,
    })]
    #[case(RetentionBufferTestCase {
        name: "clear_operation",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.push(RowEntry::new_value(b"key2", b"value2", 2));
            buffer.process_retention(|versions| versions).unwrap();
            buffer.mark_end_of_input();
            buffer.clear(); // Execute clear operation in the build function
            buffer
        },
        expected_current_versions_len: 0,
        expected_has_next: false,
        expected_processed: false,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPush,
    })]
    #[case(RetentionBufferTestCase {
        name: "tombstone_entries",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.push(RowEntry::new_tombstone(b"key1", 2));
            buffer
        },
        expected_current_versions_len: 2,
        expected_has_next: false,
        expected_processed: false,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPush,
    })]
    #[case(RetentionBufferTestCase {
        name: "merge_entries",
        build: || {
            let mut buffer = RetentionBuffer::new();
            buffer.push(RowEntry::new_value(b"key1", b"value1", 1));
            buffer.push(RowEntry::new_merge(b"key1", b"merge1", 2));
            buffer.push(RowEntry::new_tombstone(b"key1", 3));
            buffer
        },
        expected_current_versions_len: 3,
        expected_has_next: false,
        expected_processed: false,
        expected_end_of_input: false,
        expected_state: RetentionBufferState::NeedPush,
    })]
    fn test_retention_buffer_table_driven(#[case] test_case: RetentionBufferTestCase) {
        let buffer = (test_case.build)();

        // Verify expected state
        assert_eq!(
            buffer.current_versions.len(),
            test_case.expected_current_versions_len,
            "Test case '{}': current_versions_len mismatch",
            test_case.name
        );
        assert_eq!(
            buffer.next.is_some(),
            test_case.expected_has_next,
            "Test case '{}': has_next mismatch",
            test_case.name
        );
        assert_eq!(
            buffer.processed, test_case.expected_processed,
            "Test case '{}': processed mismatch",
            test_case.name
        );
        assert_eq!(
            buffer.end_of_input, test_case.expected_end_of_input,
            "Test case '{}': end_of_input mismatch",
            test_case.name
        );

        // Check state using proper comparison
        let current_state = buffer.state();
        assert_eq!(
            std::mem::discriminant(&current_state),
            std::mem::discriminant(&test_case.expected_state),
            "Test case '{}': state mismatch, expected {:?}, got {:?}",
            test_case.name,
            test_case.expected_state,
            current_state
        );
    }

    #[cfg(feature = "test-util")]
    struct RetentionIteratorTestCase {
        name: &'static str,
        input_entries: Vec<RowEntry>,
        retention_min_seq: Option<u64>,
        compaction_start_ts: i64,
        expected_entries: Vec<RowEntry>,
        filter_tombstone: bool,
    }

    // Table-driven test for retention iterator scenarios
    #[rstest]
    #[case(RetentionIteratorTestCase {
        name: "empty_iterator",
        input_entries: vec![],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "single_entry_kept",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(950),
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(950)
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "single_old_entry_kept",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(500),
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(500),
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "retain_all_versions_for_snapshot",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950), // Latest
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        retention_min_seq: Some(0),
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "tombstone_retains_snapshot_versions",
        input_entries: vec![
            RowEntry::new_tombstone(b"key1", 3).with_create_ts(950), // Latest
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(500),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        retention_min_seq: Some(0),
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_tombstone(b"key1", 3).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(500),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "merge_entries",
        input_entries: vec![
            RowEntry::new_merge(b"key1", b"merge3", 3).with_create_ts(950), // Latest
            RowEntry::new_merge(b"key1", b"merge2", 2).with_create_ts(500),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_merge(b"key1", b"merge3", 3).with_create_ts(950),
            RowEntry::new_merge(b"key1", b"merge2", 2).with_create_ts(500),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "keep_latest_without_snapshot",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(1000),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(999),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(998),
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(1000), // Latest always kept
        ],
        filter_tombstone: false,
    })]
    // Test cases for expire_ts handling
    #[case(RetentionIteratorTestCase {
        name: "expired_entry_converted_to_tombstone",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(950).with_expire_ts(900), // Expired
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_tombstone(b"key1", 1).with_create_ts(950), // Converted to tombstone
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "not_expired_entry_kept_as_is",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(950).with_expire_ts(1100), // Not expired
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(950).with_expire_ts(1100), // Kept as is
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "mixed_expired_and_not_expired_entries",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950).with_expire_ts(1100), // Not expired
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900).with_expire_ts(950), // Expired
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850).with_expire_ts(1200), // Not expired
        ],
        retention_min_seq: Some(0),
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950).with_expire_ts(1100), // Not expired
            RowEntry::new_tombstone(b"key1", 2).with_create_ts(900), // Converted to tombstone
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850).with_expire_ts(1200), // Not expired
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "expire_ts_equals_compaction_start_ts",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(950).with_expire_ts(1000), // Expired (equal)
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_tombstone(b"key1", 1).with_create_ts(950), // Converted to tombstone
        ],
        filter_tombstone: false,
    })]
    // Test cases for retention_min_seq handling
    #[case(RetentionIteratorTestCase {
        name: "retention_min_seq_basic",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 30).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 20).with_create_ts(900),
            RowEntry::new_value(b"key1", b"value1", 10).with_create_ts(850),
        ],
        retention_min_seq: Some(25),
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 30).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 20).with_create_ts(900), // Kept (boundary value for snapshots)
            // value1 filtered out because it's older than the boundary
        ],
        filter_tombstone: false,
    })]
    // Test cases for filter_tombstone: true (contrasting with existing cases)
    #[case(RetentionIteratorTestCase {
        name: "expired_entry_converted_to_tombstone_filter_tombstone_true",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(950).with_expire_ts(900), // Expired
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            // Tombstone filtered out, so no entries remain
        ],
        filter_tombstone: true, // Converted tombstone filtered out
    })]
    #[case(RetentionIteratorTestCase {
        name: "tombstone_tail_filtered_out",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950), // Latest
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900), // Middle value
            RowEntry::new_tombstone(b"key1", 1).with_create_ts(850), // Tombstone at end (oldest)
        ],
        retention_min_seq: Some(0),
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900),
            // Tombstone at end filtered out
        ],
        filter_tombstone: true, // Tombstone at end filtered out
    })]
    #[case(RetentionIteratorTestCase {
        name: "all_tombstones_filtered_out",
        input_entries: vec![
            RowEntry::new_tombstone(b"key1", 3).with_create_ts(950), // Latest is tombstone
            RowEntry::new_tombstone(b"key1", 2).with_create_ts(900), // Second tombstone
            RowEntry::new_tombstone(b"key1", 1).with_create_ts(850), // Third tombstone
        ],
        retention_min_seq: Some(0),
        compaction_start_ts: 1000,
        expected_entries: vec![
            // All tombstones filtered out, so no entries remain
        ],
        filter_tombstone: true, // All tombstones filtered out
    })]
    #[case(RetentionIteratorTestCase {
        name: "mixed_expired_and_not_expired_entries_filter_tombstone_true",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950).with_expire_ts(1100), // Not expired
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900).with_expire_ts(950), // Expired
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850).with_expire_ts(1200), // Not expired
        ],
        retention_min_seq: Some(0),
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value3", 3).with_create_ts(950).with_expire_ts(1100), // Not expired
            // Keep the tombstone between retained values.
            RowEntry::new_tombstone(b"key1", 2).with_create_ts(900),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850).with_expire_ts(1200), // Not expired
        ],
        filter_tombstone: true, // tombstone is not in the tail, so not filtered out
    })]
    #[case(RetentionIteratorTestCase {
        name: "filter_out_expired_merge_entries",
        input_entries: vec![
            RowEntry::new_merge(b"key1", b"value3", 3).with_create_ts(950).with_expire_ts(1100), // Not expired
            RowEntry::new_merge(b"key1", b"value2", 2).with_create_ts(900).with_expire_ts(950), // Expired
            RowEntry::new_merge(b"key1", b"value1", 1).with_create_ts(850).with_expire_ts(1200), // Not expired
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_merge(b"key1", b"value3", 3).with_create_ts(950).with_expire_ts(1100), // Not expired
            RowEntry::new_merge(b"key1", b"value1", 1).with_create_ts(850).with_expire_ts(1200), // Not expired
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "retain_up_to_first_non_merge_entry",
        input_entries: vec![
            RowEntry::new_merge(b"key1", b"value5", 5).with_create_ts(1050),
            RowEntry::new_merge(b"key1", b"value4", 4).with_create_ts(1000),
            RowEntry::new_merge(b"key1", b"value3", 3).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        retention_min_seq: None,
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_merge(b"key1", b"value5", 5).with_create_ts(1050),
            RowEntry::new_merge(b"key1", b"value4", 4).with_create_ts(1000),
            RowEntry::new_merge(b"key1", b"value3", 3).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900),
        ],
        filter_tombstone: false,
    })]
    #[case(RetentionIteratorTestCase {
        name: "retain_up_to_first_non_merge_entry_when_seq_num_provided",
        input_entries: vec![
            RowEntry::new_value(b"key1", b"value5", 5).with_create_ts(1050),
            RowEntry::new_merge(b"key1", b"value4", 4).with_create_ts(1000),
            RowEntry::new_merge(b"key1", b"value3", 3).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900),
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(850),
        ],
        retention_min_seq: Some(4),
        compaction_start_ts: 1000,
        expected_entries: vec![
            RowEntry::new_value(b"key1", b"value5", 5).with_create_ts(1050),
            RowEntry::new_merge(b"key1", b"value4", 4).with_create_ts(1000),
            RowEntry::new_merge(b"key1", b"value3", 3).with_create_ts(950),
            RowEntry::new_value(b"key1", b"value2", 2).with_create_ts(900),
        ],
        filter_tombstone: false,
    })]
    #[test]
    #[cfg(feature = "test-util")]
    fn test_retention_iterator_table_driven(#[case] test_case: RetentionIteratorTestCase) {
        use crate::test_utils::TestIterator;

        // Test filtering directly to preserve each entry's timestamps.
        let mut versions = BTreeMap::new();
        for entry in test_case.input_entries.iter() {
            versions.insert(Reverse(entry.seq), entry.clone());
        }

        let filtered_versions = RetentionIterator::<TestIterator>::apply_retention_filter(
            versions,
            test_case.compaction_start_ts,
            test_case.retention_min_seq,
            test_case.filter_tombstone,
            None,
        );

        // Convert filtered versions back to expected order
        let mut actual_entries = Vec::new();
        for (_, entry) in filtered_versions.iter() {
            actual_entries.push(entry.clone());
        }

        // Sort by sequence number (descending) to match expected order
        actual_entries.sort_by(|a, b| b.seq.cmp(&a.seq));

        assert_eq!(
            actual_entries.len(),
            test_case.expected_entries.len(),
            "Test case '{}': Expected {} entries, got {}",
            test_case.name,
            test_case.expected_entries.len(),
            actual_entries.len()
        );

        for (i, (actual, expected)) in actual_entries
            .iter()
            .zip(test_case.expected_entries.iter())
            .enumerate()
        {
            assert_eq!(
                actual.key, expected.key,
                "Test case '{}': Entry {} key mismatch",
                test_case.name, i
            );
            assert_eq!(
                actual.value, expected.value,
                "Test case '{}': Entry {} value mismatch",
                test_case.name, i
            );
            assert_eq!(
                actual.seq, expected.seq,
                "Test case '{}': Entry {} sequence number mismatch",
                test_case.name, i
            );
            assert_eq!(
                actual.create_ts, expected.create_ts,
                "Test case '{}': Entry {} create timestamp mismatch",
                test_case.name, i
            );
            assert_eq!(
                actual.expire_ts, expected.expire_ts,
                "Test case '{}': Entry {} expiry timestamp mismatch",
                test_case.name, i
            );
        }
    }

    #[cfg(feature = "test-util")]
    mod expired_entries_metrics {
        use super::*;
        use crate::compactor::stats::{
            ENTRY_TYPE_LABEL, ENTRY_TYPE_MERGE, ENTRY_TYPE_VALUE, EXPIRED_ENTRIES_PURGED,
        };
        use crate::test_utils::TestIterator;
        use slatedb_common::metrics::{
            lookup_metric_with_labels, test_recorder_helper, DefaultMetricsRecorder,
        };
        use std::collections::BTreeMap;

        fn build_metrics() -> (Arc<DefaultMetricsRecorder>, RetentionMetrics) {
            let (recorder, helper) = test_recorder_helper();
            let metrics = RetentionMetrics {
                expired_entries_purged_value: helper
                    .counter(EXPIRED_ENTRIES_PURGED)
                    .labels(&[(ENTRY_TYPE_LABEL, ENTRY_TYPE_VALUE)])
                    .register(),
                expired_entries_purged_merge: helper
                    .counter(EXPIRED_ENTRIES_PURGED)
                    .labels(&[(ENTRY_TYPE_LABEL, ENTRY_TYPE_MERGE)])
                    .register(),
            };
            (recorder, metrics)
        }

        fn value_count(recorder: &DefaultMetricsRecorder) -> Option<i64> {
            lookup_metric_with_labels(
                recorder,
                EXPIRED_ENTRIES_PURGED,
                &[(ENTRY_TYPE_LABEL, ENTRY_TYPE_VALUE)],
            )
        }

        fn merge_count(recorder: &DefaultMetricsRecorder) -> Option<i64> {
            lookup_metric_with_labels(
                recorder,
                EXPIRED_ENTRIES_PURGED,
                &[(ENTRY_TYPE_LABEL, ENTRY_TYPE_MERGE)],
            )
        }

        #[test]
        fn apply_retention_filter_increments_for_expired_value() {
            let (recorder, metrics) = build_metrics();
            let entry = RowEntry::new_value(b"k", b"v", 1).with_expire_ts(500);
            let mut versions = BTreeMap::new();
            versions.insert(Reverse(entry.seq), entry);

            let filtered = RetentionIterator::<TestIterator>::apply_retention_filter(
                versions,
                1_000,
                None,
                false,
                Some(&metrics),
            );

            assert_eq!(filtered.len(), 1);
            let only = filtered.values().next().unwrap();
            assert!(
                matches!(only.value, Tombstone),
                "expired value should become a tombstone, got {:?}",
                only.value
            );
            assert!(
                only.expire_ts.is_none(),
                "tombstone should have expire_ts cleared"
            );
            assert_eq!(value_count(recorder.as_ref()), Some(1));
            assert_eq!(merge_count(recorder.as_ref()), Some(0));
        }

        #[test]
        fn apply_retention_filter_increments_for_expired_merge() {
            let (recorder, metrics) = build_metrics();
            let entry = RowEntry::new_merge(b"k", b"v", 1).with_expire_ts(500);
            let mut versions = BTreeMap::new();
            versions.insert(Reverse(entry.seq), entry);

            let filtered = RetentionIterator::<TestIterator>::apply_retention_filter(
                versions,
                1_000,
                None,
                false,
                Some(&metrics),
            );

            assert!(filtered.is_empty(), "expired merge entry should be dropped");
            assert_eq!(merge_count(recorder.as_ref()), Some(1));
            assert_eq!(value_count(recorder.as_ref()), Some(0));
        }

        #[test]
        fn apply_retention_filter_no_increment_when_not_expired() {
            let (recorder, metrics) = build_metrics();
            let entry = RowEntry::new_value(b"k", b"v", 1).with_expire_ts(2_000);
            let mut versions = BTreeMap::new();
            versions.insert(Reverse(entry.seq), entry);

            let filtered = RetentionIterator::<TestIterator>::apply_retention_filter(
                versions,
                1_000,
                None,
                false,
                Some(&metrics),
            );

            assert_eq!(filtered.len(), 1);
            assert_eq!(value_count(recorder.as_ref()), Some(0));
            assert_eq!(merge_count(recorder.as_ref()), Some(0));
        }

        #[test]
        fn apply_retention_filter_no_panic_when_metrics_none() {
            let merge_entry = RowEntry::new_merge(b"k1", b"v", 1).with_expire_ts(500);
            let value_entry = RowEntry::new_value(b"k2", b"v", 2).with_expire_ts(500);
            let mut versions = BTreeMap::new();
            versions.insert(Reverse(merge_entry.seq), merge_entry);
            versions.insert(Reverse(value_entry.seq), value_entry);

            let filtered = RetentionIterator::<TestIterator>::apply_retention_filter(
                versions, 1_000, None, false, None,
            );

            // merge dropped, value kept as tombstone
            assert_eq!(filtered.len(), 1);
            let kept = filtered.values().next().unwrap();
            assert!(matches!(kept.value, Tombstone));
        }
    }
}
