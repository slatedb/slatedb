use crate::{
    bytes_range::BytesRange,
    config::{ReadOptions, ScanOptions},
    types::KeyValue,
    DbIterator,
};
use bytes::Bytes;
use std::ops::RangeBounds;

/// Trait for read-only database operations.
///
/// This trait defines the interface for durability-aware reads.
/// Only [`crate::Db`] implements this trait. Fixed-view handles such as
/// [`crate::DbSnapshot`], [`crate::DbTransaction`], and [`crate::DbReader`]
/// deliberately do not implement it because their visibility is fixed when
/// they are created.
#[async_trait::async_trait]
pub trait DbRead {
    /// Get a value from the database with default read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `Error`: if there was an error getting the value
    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, &ReadOptions::default()).await
    }

    /// Get a value from the database with explicit durability-aware read options.
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, crate::Error>;

    /// Get a key-value pair from the database with default read options.
    ///
    /// Returns the key along with its value and metadata (sequence number,
    /// creation timestamp, expiration timestamp). Unlike [`get`](Self::get),
    /// which returns only the value bytes, this method returns a [`KeyValue`]
    /// that includes row metadata.
    ///
    /// ## Arguments
    /// - `key`: the key to look up
    ///
    /// ## Returns
    /// - `Ok(Some(KeyValue))`: if the key exists and is not deleted/expired
    /// - `Ok(None)`: if the key does not exist or is deleted/expired
    ///
    /// ## Errors
    /// - `Error`: if there was an error reading from the database
    async fn get_key_value<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<KeyValue>, crate::Error> {
        self.get_key_value_with_options(key, &ReadOptions::default())
            .await
    }

    /// Get a key-value pair from the database with explicit durability-aware read options.
    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, crate::Error>;

    /// Scan a range of keys using the default scan options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Errors
    /// - `Error`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default()).await
    }

    /// Scan a range of keys with explicit durability-aware scan options.
    async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send;

    /// Scan all keys that share the provided prefix using the default scan options.
    ///
    /// ## Arguments
    /// - `prefix`: the key prefix to scan
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    async fn scan_prefix<P>(&self, prefix: P) -> Result<DbIterator, crate::Error>
    where
        P: AsRef<[u8]> + Send,
    {
        self.scan_prefix_with_options(prefix, &ScanOptions::default())
            .await
    }

    /// Scan all keys that share the provided prefix with explicit durability-aware scan options.
    async fn scan_prefix_with_options<P>(
        &self,
        prefix: P,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        P: AsRef<[u8]> + Send,
    {
        let range = BytesRange::from_prefix(prefix.as_ref());
        self.scan_with_options(range, options).await
    }
}
