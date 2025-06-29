use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;
use crate::utils::is_not_expired;

/// A retention iterator that filters entries based on retention time and handles expired/tombstoned keys.
///
/// This iterator assumes the upstream iterator provides entries in decreasing order of sequence numbers.
/// For each entry, it:
/// 1. Filters out entries whose create_time is earlier than the retention time
/// 2. Skips entries that are expired/tombstoned
/// 3. Returns the filtered entries in decreasing order of sequence numbers
pub(crate) struct RetentionIterator<T: KeyValueIterator> {
    /// The upstream iterator providing entries in decreasing order of sequence numbers
    inner: T,
    retention_time: Duration,
    buffer: RetentionBuffer,
}

impl<T: KeyValueIterator> RetentionIterator<T> {
    /// Creates a new retention iterator
    ///
    /// # Arguments
    /// * `delegate` - The upstream iterator providing entries in decreasing order of sequence numbers
    /// * `retention_time` - Retention time in milliseconds. Entries with create_ts earlier than this will be filtered out
    pub(crate) async fn new(mut inner: T, retention_time: Duration) -> Result<Self, SlateDBError> {
        Ok(Self {
            inner,
            retention_time,
            buffer: RetentionBuffer::new(),
        })
    }

    pub(crate) fn current_timestamp(&self) -> i64 {
        // TODO: take the clock
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap() as i64
    }

    fn apply_retention_filter(
        mut versions: BTreeMap<Reverse<u64>, RowEntry>,
        current_timestamp: i64,
        retention_time: Duration,
    ) -> BTreeMap<Reverse<u64>, RowEntry> {
        let latest_version = match versions.pop_first() {
            Some((_, entry)) => entry,
            None => return versions,
        };

        let mut filtered_versions = versions
            .into_iter()
            .skip(1)
            .filter(|(_, entry)| {
                entry
                    .create_ts
                    .map(|create_ts| {
                        create_ts + (retention_time.as_secs() as i64) < current_timestamp
                    })
                    .unwrap_or(true)
            })
            .collect::<BTreeMap<_, _>>();

        filtered_versions.insert(Reverse(latest_version.seq), latest_version);
        filtered_versions
    }
}

#[async_trait]
impl<T: KeyValueIterator> KeyValueIterator for RetentionIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            match self.buffer.state() {
                RetentionBufferState::NeedPush => {
                    let entry = match self.inner.next_entry().await? {
                        Some(entry) => entry,
                        None => {
                            self.buffer.mark_end_of_input();
                            continue;
                        }
                    };

                    self.buffer.push(entry);
                }
                RetentionBufferState::NeedPopAndContinue => match self.buffer.pop() {
                    Some(entry) => return Ok(Some(entry)),
                    None => continue,
                },
                RetentionBufferState::NeedPopAndQuit => return Ok(self.buffer.pop()),
                RetentionBufferState::NeedProcess => {
                    let current_timestamp = self.current_timestamp();
                    let retention_time = self.retention_time;
                    self.buffer.process_retention(|versions| {
                        Self::apply_retention_filter(versions, current_timestamp, retention_time)
                    })?;
                }
            }
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.buffer.clear();
        self.inner.seek(next_key).await?;
        Ok(())
    }
}

/// A buffer that collects multiple versions of the same key from an iterator.
///
/// When used in [`RetentionIterator::next_entry`], this buffer first collects all
/// versions of the current key before returning the first row entry.
struct RetentionBuffer {
    current_versions: BTreeMap<Reverse<u64>, RowEntry>,
    next_entry: Option<RowEntry>,
    processed: bool,
    end_of_input: bool,
}

enum RetentionBufferState {
    NeedPush,
    NeedPopAndContinue,
    NeedPopAndQuit,
    NeedProcess,
}

impl RetentionBuffer {
    fn new() -> Self {
        Self {
            current_versions: BTreeMap::new(),
            next_entry: None,
            processed: false,
            end_of_input: false,
        }
    }

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

    fn clear(&mut self) {
        self.current_versions.clear();
        self.next_entry = None;
        self.processed = false;
        self.end_of_input = false;
    }

    fn is_empty(&self) -> bool {
        self.current_versions.is_empty() && self.next_entry.is_none()
    }

    fn mark_end_of_input(&mut self) {
        self.end_of_input = true;
    }

    /// Appends an entry to the buffer.
    ///
    /// Returns `true` if the entry has the same key as the current versions being collected, or the current versions are empty.
    /// Returns `false` if the key is different, indicating the caller should call `pop()`
    /// to retrieve the next entry.
    fn push(&mut self, entry: RowEntry) -> bool {
        let current_key = match self.current_versions.values().next() {
            Some(entry) => entry.key.clone(),
            None => {
                // If current versions are empty, this is the first entry
                self.current_versions.insert(Reverse(entry.seq), entry);
                return true;
            }
        };

        // Different key, store as next entry and return false
        if entry.key != current_key {
            self.next_entry = Some(entry);
            return false;
        }

        // same key, append to current versions
        self.current_versions.insert(Reverse(entry.seq), entry);
        true
    }

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

    /// Pop the latest sequence number of the current key.
    /// When current versions are empty, puts the next entry into current versions
    /// and requires caller to call `append` to add more entries.
    fn pop(&mut self) -> Option<RowEntry> {
        match self.current_versions.pop_first() {
            Some((_, entry)) => Some(entry),
            None => {
                // promote the next entry to current versions, and return None, to
                // tell the caller to call `append` to add more entries.
                let next_entry = self.next_entry.take();
                if let Some(entry) = next_entry {
                    self.current_versions.insert(Reverse(entry.seq), entry);
                }
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ValueDeletable;

    #[test]
    fn test_retention_buffer() -> Result<(), SlateDBError> {
        todo!()
    }
}
