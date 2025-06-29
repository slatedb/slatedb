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
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap() as i64
    }
}

#[async_trait]
impl<T: KeyValueIterator> KeyValueIterator for RetentionIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.buffer.need_push() {
            while let Some(entry) = self.inner.next_entry().await? {
                let current_timestamp = self.current_timestamp();
                let has_more = self.buffer.push_if(entry, |entry| {
                    entry
                        .create_ts
                        .map(|create_ts| {
                            create_ts + self.retention_time.as_secs() as i64 > current_timestamp
                        })
                        .unwrap_or(true)
                })?;
                if !has_more {
                    break;
                }
            }
        }

        Ok(self.buffer.pop())
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
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
}

impl RetentionBuffer {
    fn new() -> Self {
        Self {
            current_versions: BTreeMap::new(),
            next_entry: None,
        }
    }

    /// Before having a different key, we need to push more entries to the buffer.
    fn need_push(&self) -> bool {
        self.next_entry.is_none()
    }

    /// Appends an entry to the buffer.
    ///
    /// Returns `true` if the entry has the same key as the current versions being collected, or the current versions are empty.
    /// Returns `false` if the key is different, indicating the caller should call `pop()`
    /// to retrieve the next entry.
    fn push_if(
        &mut self,
        entry: RowEntry,
        f: impl FnOnce(&RowEntry) -> bool,
    ) -> Result<bool, SlateDBError> {
        let current_key = match self.current_versions.values().next() {
            Some(entry) => entry.key.clone(),
            None => {
                // If current versions are empty, this is the first entry
                self.current_versions.insert(Reverse(entry.seq), entry);
                return Ok(true);
            }
        };

        // Different key, store as next entry and return false
        if entry.key != current_key {
            self.next_entry = Some(entry);
            return Ok(false);
        }

        // if the entry has the same key as the current versions being collected, and this entry passed the filter,
        // we can append it to the current versions.
        if f(&entry) {
            self.current_versions.insert(Reverse(entry.seq), entry);
        }
        Ok(true)
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
        let mut buffer = RetentionBuffer::new();
        let mut has_more = buffer.push_if(
            RowEntry::new(
                Bytes::copy_from_slice(b"key1"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value1:10")),
                10,
                Some(100),
                None,
            ),
            |_| true,
        )?;
        assert!(has_more);

        has_more = buffer.push_if(
            RowEntry::new(
                Bytes::copy_from_slice(b"key1"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value1:9")),
                9,
                Some(200),
                None,
            ),
            |_| true,
        )?;
        assert!(has_more);

        has_more = buffer.push_if(
            RowEntry::new(
                Bytes::copy_from_slice(b"key2"),
                ValueDeletable::Value(Bytes::copy_from_slice(b"value2:8")),
                8,
                Some(300),
                None,
            ),
            |_| true,
        )?;
        assert!(!has_more);

        Ok(())
    }
}
