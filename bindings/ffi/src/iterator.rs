//! Iterators returned by scan operations.

use tokio::sync::Mutex as AsyncMutex;

use crate::config::FfiKeyValue;
use crate::error::FfiSlatedbError;

/// An asynchronous iterator over key-value pairs.
///
/// Instances of this type are returned by scan operations on [`crate::Db`],
/// [`crate::DbSnapshot`], and [`crate::DbTransaction`].
#[derive(uniffi::Object)]
pub struct DbIterator {
    inner: AsyncMutex<slatedb::DbIterator>,
}

impl DbIterator {
    pub(crate) fn new(inner: slatedb::DbIterator) -> Self {
        Self {
            inner: AsyncMutex::new(inner),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbIterator {
    /// Return the next item from the iterator.
    ///
    /// ## Returns
    /// - `Result<Option<FfiKeyValue>, FfiSlatedbError>`: the next key-value pair, or
    ///   `None` when the iterator is exhausted.
    pub async fn next(&self) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(FfiKeyValue::from_core))
    }

    /// Reposition the iterator to the first key greater than or equal to `key`.
    ///
    /// ## Arguments
    /// - `key`: the key to seek to within the iterator's range.
    ///
    /// ## Errors
    /// - `FfiSlatedbError::Invalid`: if `key` is empty.
    /// - `FfiSlatedbError`: if the key falls outside the iterator's valid range.
    pub async fn seek(&self, key: Vec<u8>) -> Result<(), FfiSlatedbError> {
        if key.is_empty() {
            return Err(FfiSlatedbError::Invalid {
                message: "seek key cannot be empty".to_owned(),
            });
        }
        let mut guard = self.inner.lock().await;
        guard.seek(key).await.map_err(Into::into)
    }
}
