use tokio::sync::Mutex;

use crate::error::FfiSlatedbError;
use crate::types::FfiKeyValue;

#[derive(uniffi::Object)]
pub struct FfiDbIterator {
    inner: Mutex<slatedb::DbIterator>,
}

impl FfiDbIterator {
    pub(crate) fn new(inner: slatedb::DbIterator) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl FfiDbIterator {
    pub async fn next(&self) -> Result<Option<FfiKeyValue>, FfiSlatedbError> {
        let mut guard = self.inner.lock().await;
        Ok(guard.next().await?.map(FfiKeyValue::from_core))
    }

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
