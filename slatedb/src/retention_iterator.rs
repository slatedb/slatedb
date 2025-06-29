use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::utils::is_not_expired;

/// A retention iterator that filters entries based on retention time and handles expired/tombstoned keys.
///
/// This iterator implements a retention policy by filtering out entries that are older than a specified
/// retention period. It assumes the upstream iterator provides entries in decreasing order of sequence numbers
/// (newest first) and groups entries by key to apply retention filtering across all versions of each key.
pub(crate) struct RetentionIterator<T: KeyValueIterator> {
    /// The upstream iterator providing entries in decreasing order of sequence numbers
    inner: T,
    /// Retention time duration. Entries with create_ts older than (current_time - retention_time)
    /// will be filtered out (except the latest version)
    retention_time: Duration,
    /// Buffer for collecting and processing multiple versions of the same key
    buffer: RetentionBuffer,
}

impl<T: KeyValueIterator> RetentionIterator<T> {
    /// Creates a new retention iterator with the specified retention policy
    ///
    /// # Arguments
    /// * `inner` - The upstream iterator providing entries in decreasing order of sequence numbers
    /// * `retention_time` - Retention time duration. Entries with create_ts earlier than
    ///   (current_time - retention_time) will be filtered out, except for the latest version
    ///
    /// # Returns
    /// A configured retention iterator ready to filter entries based on the retention policy
    pub(crate) async fn new(mut inner: T, retention_time: Duration) -> Result<Self, SlateDBError> {
        Ok(Self {
            inner,
            retention_time,
            buffer: RetentionBuffer::new(),
        })
    }

    /// Gets the current timestamp in seconds since Unix epoch
    ///
    /// This is used as the reference point for retention calculations.
    /// TODO: Consider injecting a clock dependency for better testability
    pub(crate) fn current_timestamp(&self) -> i64 {
        // TODO: take the clock
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap() as i64
    }

    /// Applies retention filtering to a collection of versions for the same key
    ///
    /// This function implements the core retention logic:
    /// - Always preserves the latest version (highest sequence number)
    /// - Filters out older versions that exceed the retention period
    /// - Uses `create_ts` to determine if a version should be retained
    ///
    /// # Arguments
    /// * `versions` - A BTreeMap of versions for the same key, ordered by sequence number (newest first)
    /// * `current_timestamp` - Current time in seconds since Unix epoch
    /// * `retention_time` - Retention duration in seconds
    ///
    /// # Returns
    /// A filtered BTreeMap containing only versions that meet the retention criteria
    fn apply_retention_filter(
        mut versions: BTreeMap<Reverse<u64>, RowEntry>,
        current_timestamp: i64,
        retention_time: Duration,
    ) -> BTreeMap<Reverse<u64>, RowEntry> {
        // Always preserve the latest version regardless of age
        let latest_version = match versions.pop_first() {
            Some((_, entry)) => entry,
            None => return versions,
        };

        // Filter older versions based on retention time
        let mut filtered_versions = versions
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .create_ts
                    .map(|create_ts| {
                        // Keep version if: create_ts + retention_time >= current_timestamp
                        // (i.e., version is still within retention period)
                        create_ts + (retention_time.as_secs() as i64) >= current_timestamp
                    })
                    .unwrap_or(true) // If no create_ts, keep the version
            })
            .collect::<BTreeMap<_, _>>();

        // Re-insert the latest version at the front
        filtered_versions.insert(Reverse(latest_version.seq), latest_version);
        filtered_versions
    }
}

#[async_trait]
impl<T: KeyValueIterator> KeyValueIterator for RetentionIterator<T> {
    /// Retrieves the next entry from the retention iterator
    ///
    /// This method implements a state machine that:
    /// 1. Collects all versions of the current key from the upstream iterator
    /// 2. Applies retention filtering to the collected versions
    /// 3. Returns filtered entries one by one in sequence number order (newest first)
    ///
    /// The state machine ensures efficient processing by batching operations for each key.
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            match self.buffer.state() {
                RetentionBufferState::NeedPush => {
                    // Fetch next entry from upstream iterator
                    let entry = match self.inner.next_entry().await? {
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
                    let current_timestamp = self.current_timestamp();
                    let retention_time = self.retention_time;
                    self.buffer.process_retention(|versions| {
                        Self::apply_retention_filter(versions, current_timestamp, retention_time)
                    })?;
                }
            }
        }
    }

    /// Seeks to the specified key in the upstream iterator
    ///
    /// Clears the internal buffer and resets the iterator state to begin processing
    /// from the specified key position.
    ///
    /// # Arguments
    /// * `next_key` - The key to seek to in the upstream iterator
    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.buffer.clear();
        self.inner.seek(next_key).await?;
        Ok(())
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
    next_entry: Option<RowEntry>,
    /// After the current key versions have been exhausted, process_retention will be called, and
    /// this flag will be set to true.
    processed: bool,
    /// Whether the upstream iterator has reached end of input.
    end_of_input: bool,
}

/// This enum drives the behavior of the retention iterator's main loop, determining what action
/// should be taken next.
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
            next_entry: None,
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
        } else {
            if self.end_of_input || self.next_entry.is_some() {
                return RetentionBufferState::NeedProcess;
            }
        }
        RetentionBufferState::NeedPush
    }

    /// Clears the buffer and resets all state flags
    ///
    /// Called when seeking to a new position in the iterator.
    fn clear(&mut self) {
        self.current_versions.clear();
        self.next_entry = None;
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
    ///
    /// # Arguments
    /// * `entry` - The row entry to add to the buffer
    ///
    /// # Returns
    /// - `true` if the entry has the same key as current versions (or current versions are empty)
    /// - `false` if the key is different, indicating a key transition
    fn push(&mut self, entry: RowEntry) -> bool {
        let current_key = match self.current_versions.values().next() {
            Some(entry) => entry.key.clone(),
            None => {
                // If current versions are empty, this is the first entry
                self.current_versions.insert(Reverse(entry.seq), entry);
                return true;
            }
        };

        // Different key detected - store as next entry and signal key transition
        if entry.key != current_key {
            self.next_entry = Some(entry);
            return false;
        }

        // Same key - append to current versions
        self.current_versions.insert(Reverse(entry.seq), entry);
        true
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
                let next_entry = self.next_entry.take();
                if let Some(entry) = next_entry {
                    self.current_versions.insert(Reverse(entry.seq), entry);
                    self.processed = false;
                } else if self.end_of_input {
                    // No next entry and at end of input - we're done
                    return None;
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RowEntry, ValueDeletable};
}
