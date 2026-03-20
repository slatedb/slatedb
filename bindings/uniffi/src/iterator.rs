use tokio::sync::Mutex;

use crate::error::Error;
use crate::types::KeyValue;
use crate::validation::validate_key;

/// Async iterator returned by scan APIs.
#[derive(uniffi::Object)]
pub struct DbIterator {
    inner: Mutex<slatedb::DbIterator>,
}

impl DbIterator {
    pub(crate) fn new(inner: slatedb::DbIterator) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbIterator {
    /// Returns the next key/value pair from the iterator.
    pub async fn next(&self) -> Result<Option<KeyValue>, Error> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(KeyValue::from))
    }

    /// Seeks the iterator to the first entry at or after `key`.
    pub async fn seek(&self, key: Vec<u8>) -> Result<(), Error> {
        validate_key(&key)?;
        let mut guard = self.inner.lock().await;
        guard.seek(key).await.map_err(Into::into)
    }
}
