use crate::{
    config::{ReadOptions, ScanOptions},
    error::SlateDBError,
    DbIterator,
};
use bytes::Bytes;
use std::ops::RangeBounds;

/// Trait for read-only database operations.
///
/// This trait defines the interface for reading data from SlateDB,
/// and can be implemented by `Db`, `DbReader` and `DbSnapshot`
/// to provide a unified interface for read-only operations.
///
/// The trait is designed to be object-safe, allowing for dynamic dispatch
/// when needed.
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
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, SlateDBError> {
        self.get_with_options(key, &ReadOptions::default()).await
    }

    /// Get a value from the database with custom read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///   - `Some(Bytes)`: the value if it exists
    ///   - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError>;

    /// Scan a range of keys using the default scan options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    async fn scan<K, T>(&self, range: T) -> Result<DbIterator, SlateDBError>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default()).await
    }

    /// Scan a range of keys with the provided options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    /// - `options`: the scan options to use
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send;
}
