use crate::error::Error;

/// Handle returned by a successful write.
#[derive(uniffi::Object)]
pub struct WriteHandle {
    inner: slatedb::WriteHandle,
}

impl WriteHandle {
    pub(crate) fn new(inner: slatedb::WriteHandle) -> Self {
        Self { inner }
    }
}

#[uniffi::export]
impl WriteHandle {
    /// Returns the sequence number assigned to the write.
    pub fn seqnum(&self) -> u64 {
        self.inner.seqnum()
    }

    /// Returns the creation timestamp assigned to the write.
    pub fn create_ts(&self) -> i64 {
        self.inner.create_ts()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WriteHandle {
    /// Waits until the write has been durably persisted.
    pub async fn await_durable(&self) -> Result<(), Error> {
        self.inner.await_durable().await.map_err(Into::into)
    }
}
