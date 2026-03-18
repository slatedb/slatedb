use tokio::sync::Mutex;

use crate::error::DbError;
use crate::types::KeyValue;
use crate::validation::validate_key;

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
    pub async fn next(&self) -> Result<Option<KeyValue>, DbError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(KeyValue::from_core))
    }

    pub async fn seek(&self, key: Vec<u8>) -> Result<(), DbError> {
        validate_key(&key)?;
        let mut guard = self.inner.lock().await;
        guard.seek(key).await.map_err(Into::into)
    }
}
