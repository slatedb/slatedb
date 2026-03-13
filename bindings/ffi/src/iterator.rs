use tokio::sync::Mutex as AsyncMutex;

use crate::config::KeyValue;
use crate::error::SlatedbError;

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
    pub async fn next(&self) -> Result<Option<KeyValue>, SlatedbError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(KeyValue::from_core))
    }

    pub async fn seek(&self, key: Vec<u8>) -> Result<(), SlatedbError> {
        if key.is_empty() {
            return Err(SlatedbError::Invalid {
                message: "seek key cannot be empty".to_owned(),
            });
        }
        let mut guard = self.inner.lock().await;
        guard.seek(key).await.map_err(Into::into)
    }
}
