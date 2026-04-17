use chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::error::{Error, SlateDbError};
use crate::types::{
    Checkpoint, Compaction, CompactorStateView, U64Range, VersionedCompactions, VersionedManifest,
};

/// Administrative read/query handle for SlateDB.
#[derive(uniffi::Object)]
pub struct Admin {
    pub(crate) inner: slatedb::admin::Admin,
}

#[uniffi::export(async_runtime = "tokio")]
impl Admin {
    /// Reads a specific manifest by ID, or the latest when `id` is `None`.
    pub async fn read_manifest(&self, id: Option<u64>) -> Result<Option<VersionedManifest>, Error> {
        let manifest = self.inner.read_manifest(id).await.map_err(Error::from)?;
        Ok(manifest.as_ref().map(VersionedManifest::from))
    }

    /// Lists manifests inside `range`.
    pub async fn list_manifests(&self, range: U64Range) -> Result<Vec<VersionedManifest>, Error> {
        let bounds = range.into_bounds()?;
        let manifests = self
            .inner
            .list_manifests(bounds)
            .await
            .map_err(Error::from)?;
        Ok(manifests.iter().map(VersionedManifest::from).collect())
    }

    /// Reads a specific compactions file by ID, or the latest when `id` is `None`.
    pub async fn read_compactions(
        &self,
        id: Option<u64>,
    ) -> Result<Option<VersionedCompactions>, Error> {
        let compactions = self.inner.read_compactions(id).await.map_err(Error::from)?;
        Ok(compactions.as_ref().map(VersionedCompactions::from))
    }

    /// Reads a compaction by ULID string from a specific or latest compactions file.
    pub async fn read_compaction(
        &self,
        compaction_id: String,
        compactions_id: Option<u64>,
    ) -> Result<Option<Compaction>, Error> {
        let compaction_id = Ulid::from_string(&compaction_id)
            .map_err(|source| SlateDbError::InvalidCompactionId { source })?;
        let compaction = self
            .inner
            .read_compaction(compaction_id, compactions_id)
            .await
            .map_err(Error::from)?;
        Ok(compaction.as_ref().map(Compaction::from))
    }

    /// Reads the latest compactor state view.
    pub async fn read_compactor_state_view(&self) -> Result<CompactorStateView, Error> {
        let view = self
            .inner
            .read_compactor_state_view()
            .await
            .map_err(Error::from)?;
        Ok((&view).into())
    }

    /// Lists compactions files inside `range`.
    pub async fn list_compactions(
        &self,
        range: U64Range,
    ) -> Result<Vec<VersionedCompactions>, Error> {
        let bounds = range.into_bounds()?;
        let compactions = self
            .inner
            .list_compactions(bounds)
            .await
            .map_err(Error::from)?;
        Ok(compactions.iter().map(VersionedCompactions::from).collect())
    }

    /// Lists checkpoints, optionally filtering by exact name.
    pub async fn list_checkpoints(
        &self,
        name_filter: Option<String>,
    ) -> Result<Vec<Checkpoint>, Error> {
        let checkpoints = self
            .inner
            .list_checkpoints(name_filter.as_deref())
            .await
            .map_err(Error::from)?;
        Ok(checkpoints.iter().map(Checkpoint::from).collect())
    }

    /// Looks up a timestamp for the provided sequence number.
    pub async fn get_timestamp_for_sequence(
        &self,
        seq: u64,
        round_up: bool,
    ) -> Result<Option<i64>, Error> {
        let timestamp = self.inner.get_timestamp_for_sequence(seq, round_up).await?;
        Ok(timestamp.map(|ts| ts.timestamp()))
    }

    /// Looks up a sequence number for the provided Unix UTC timestamp seconds.
    pub async fn get_sequence_for_timestamp(
        &self,
        timestamp_secs: i64,
        round_up: bool,
    ) -> Result<Option<u64>, Error> {
        let timestamp = DateTime::<Utc>::from_timestamp(timestamp_secs, 0)
            .ok_or(SlateDbError::InvalidTimestampSeconds { timestamp_secs })?;
        self.inner
            .get_sequence_for_timestamp(timestamp, round_up)
            .await
            .map_err(Into::into)
    }
}
