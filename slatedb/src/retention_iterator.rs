use std::collections::BTreeMap;

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
    /// Retention time in milliseconds - entries with create_ts earlier than this will be filtered out
    retention_time: i64,
    /// Current key
    current_key: Option<Bytes>,
}

impl<T: KeyValueIterator> RetentionIterator<T> {
    /// Creates a new retention iterator
    ///
    /// # Arguments
    /// * `delegate` - The upstream iterator providing entries in decreasing order of sequence numbers
    /// * `retention_time` - Retention time in milliseconds. Entries with create_ts earlier than this will be filtered out
    pub(crate) async fn new(mut inner: T, retention_time: i64) -> Result<Self, SlateDBError> {
        Ok(Self {
            inner,
            retention_time,
            current_key: None,
        })
    }
}

#[async_trait]
impl<T: KeyValueIterator> KeyValueIterator for RetentionIterator<T> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        while let Some(entry) = self.inner.next_entry().await? {
            if self
                .current_key
                .as_ref()
                .map(|key| key == &entry.key)
                .unwrap_or(false)
            {
                // filter out entries that had exceeded the retention time
                if entry
                    .create_ts
                    .map(|ts| ts < self.retention_time)
                    .unwrap_or(false)
                {
                    continue;
                }
            }

            self.current_key = Some(entry.key.clone());
            return Ok(Some(entry));
        }

        todo!()
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.current_key = Some(Bytes::from(next_key.to_vec()));
        self.inner.seek(next_key).await?;
        Ok(())
    }
}

/// A buffer that collects multiple versions of the same key from an iterator.
///
/// When used in [`RetentionIterator::next_entry`], this buffer first collects all
/// versions of the current key before returning the first row entry.
struct RetentionBuffer {
    current_versions: BTreeMap<u64, RowEntry>,
    next_entry: Option<RowEntry>,
}

impl RetentionBuffer {
    fn new() -> Self {
        Self {
            current_versions: Vec::new(),
            next_entry: None,
        }
    }

    /// Appends an entry to the buffer.
    ///
    /// Returns `true` if the entry has the same key as the current versions being collected.
    /// Returns `false` if the key is different, indicating the caller should call `pop()`
    /// to retrieve the next entry.
    fn push(&mut self, entry: RowEntry) -> Result<bool, SlateDBError> {
        todo!()
    }

    /// When current versions are empty, puts the next entry into current versions
    /// and requires caller to call `append` to add more entries.
    fn pop(&mut self) -> Option<RowEntry> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ValueDeletable;

    #[test]
    fn test_retention_buffer() -> Result<(), SlateDBError> {
        let mut buffer = RetentionBuffer::new();
        let mut has_more = buffer.push(RowEntry::new(
            Bytes::copy_from_slice(b"key1"),
            ValueDeletable::Value(Bytes::copy_from_slice(b"value1:10")),
            10,
            Some(100),
            None,
        ))?;
        assert!(has_more);

        has_more = buffer.push(RowEntry::new(
            Bytes::copy_from_slice(b"key1"),
            ValueDeletable::Value(Bytes::copy_from_slice(b"value1:9")),
            9,
            Some(200),
            None,
        ))?;
        assert!(has_more);

        has_more = buffer.push(RowEntry::new(
            Bytes::copy_from_slice(b"key2"),
            ValueDeletable::Value(Bytes::copy_from_slice(b"value2:8")),
            8,
            Some(300),
            None,
        ))?;
        assert!(!has_more);

        Ok(())
    }
}
