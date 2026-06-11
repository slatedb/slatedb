use std::ops::Bound;
use std::sync::Arc;

use parking_lot::Mutex;
use slatedb::bytes::Bytes;
use uuid::Uuid;

use crate::error::{Error, SlateDbError};
use crate::types::{map_bound, KeyRange};

type SlateCloneSourceSpec = slatedb::admin::CloneSourceSpec<(Bound<Bytes>, Bound<Bytes>)>;
type SlateCloneBuilder = slatedb::admin::CloneBuilder<(Bound<Bytes>, Bound<Bytes>)>;

fn into_bytes_bounds(range: KeyRange) -> Result<(Bound<Bytes>, Bound<Bytes>), SlateDbError> {
    let (start, end) = range.into_bounds()?;
    Ok((map_bound(start), map_bound(end)))
}

/// Specifies the source database and optional checkpoint for a clone operation.
#[derive(uniffi::Object)]
pub struct CloneSourceSpec {
    inner: Mutex<SlateCloneSourceSpec>,
}

#[uniffi::export]
impl CloneSourceSpec {
    /// Creates a clone source pointing to the latest state of the database at `path`.
    #[uniffi::constructor]
    pub fn new(path: String) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(slatedb::admin::CloneSourceSpec::new(path)),
        })
    }

    /// Creates a clone source pointing to a specific checkpoint.
    #[uniffi::constructor]
    pub fn with_checkpoint(path: String, checkpoint_id: String) -> Result<Arc<Self>, Error> {
        let id = Uuid::parse_str(&checkpoint_id)
            .map_err(|source| SlateDbError::InvalidCheckpointId { source })?;
        Ok(Arc::new(Self {
            inner: Mutex::new(slatedb::admin::CloneSourceSpec::with_checkpoint(path, id)),
        }))
    }

    /// Limits visible keys to the given range in the source database.
    /// Pass `None` to clear a previously set range.
    pub fn with_projection_range(&self, range: Option<KeyRange>) -> Result<(), Error> {
        let bounds = range.map(into_bytes_bounds).transpose()?;
        self.inner.lock().projection_range = bounds;
        Ok(())
    }
}

impl CloneSourceSpec {
    pub(crate) fn clone_inner(&self) -> SlateCloneSourceSpec {
        self.inner.lock().clone()
    }
}

/// Builder for configuring and executing a database clone operation.
#[derive(uniffi::Object)]
pub struct CloneBuilder {
    inner: Mutex<Option<SlateCloneBuilder>>,
}

impl CloneBuilder {
    pub(crate) fn from_inner(builder: SlateCloneBuilder) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Some(builder)),
        })
    }

    fn update_builder(
        &self,
        f: impl FnOnce(SlateCloneBuilder) -> SlateCloneBuilder,
    ) -> Result<(), SlateDbError> {
        let mut guard = self.inner.lock();
        let builder = guard.take().ok_or(SlateDbError::BuilderConsumed)?;
        *guard = Some(f(builder));
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl CloneBuilder {
    /// Adds an additional source to this clone operation.
    pub fn with_source(&self, source: Arc<CloneSourceSpec>) -> Result<(), Error> {
        let spec = source.clone_inner();
        self.update_builder(|b| b.with_source(spec))
            .map_err(Into::into)
    }

    /// Applies a global projection range to the clone, intersected with any per-source ranges.
    /// Pass `None` to clear a previously set range.
    pub fn with_projection_range(&self, range: Option<KeyRange>) -> Result<(), Error> {
        let bounds = range.map(into_bytes_bounds).transpose()?;
        self.update_builder(|b| b.with_projection_range(bounds))
            .map_err(Into::into)
    }

    /// Executes the clone and consumes this builder.
    pub async fn build(&self) -> Result<(), Error> {
        let builder = self
            .inner
            .lock()
            .take()
            .ok_or(SlateDbError::BuilderConsumed)
            .map_err(Error::from)?;
        builder
            .build()
            .await
            .map_err(|e| Error::Internal { message: e.to_string() })
    }
}
