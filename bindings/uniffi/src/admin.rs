use crate::builder::CloneBuilder;
use crate::config::{CheckpointOptions, GarbageCollectorOptions};
use crate::error::{Error, SlateDbError};
use crate::types::{
    try_checkpoint_id_from_str, Checkpoint, CheckpointCreateResult, CloneSourceSpec, Compaction,
    CompactionSpec, CompactorStateView, VersionedCompactions, VersionedManifest,
};
use chrono::{DateTime, Utc};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;

fn into_u64_bounds(
    from: Option<u64>,
    to: Option<u64>,
) -> Result<(Bound<u64>, Bound<u64>), SlateDbError> {
    Ok((
        from.map_or(Bound::Unbounded, Bound::Included),
        to.map_or(Bound::Unbounded, Bound::Excluded),
    ))
}

/// Administrative read/query handle for SlateDB.
#[derive(uniffi::Object)]
pub struct Admin {
    pub(crate) inner: slatedb::admin::Admin,
}

#[uniffi::export(async_runtime = "tokio")]
impl Admin {
    /// Reads a specific manifest by ID, or the latest when `id` is `None`.
    pub async fn read_manifest(&self, id: Option<u64>) -> Result<Option<VersionedManifest>, Error> {
        let manifest = self.inner.read_manifest(id).await?;
        Ok(manifest.as_ref().map(VersionedManifest::from))
    }

    /// Lists manifests inside the half-open ID range `[from, to)`.
    pub async fn list_manifests(
        &self,
        from: Option<u64>,
        to: Option<u64>,
    ) -> Result<Vec<VersionedManifest>, Error> {
        let bounds = into_u64_bounds(from, to)?;
        let manifests = self.inner.list_manifests(bounds).await?;
        Ok(manifests.iter().map(VersionedManifest::from).collect())
    }

    /// Reads a specific compactions file by ID, or the latest when `id` is `None`.
    pub async fn read_compactions(
        &self,
        id: Option<u64>,
    ) -> Result<Option<VersionedCompactions>, Error> {
        let compactions = self.inner.read_compactions(id).await?;
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
            .await?;
        Ok(compaction.as_ref().map(Compaction::from))
    }

    /// Reads the latest compactor state view.
    pub async fn read_compactor_state_view(&self) -> Result<CompactorStateView, Error> {
        let view = self.inner.read_compactor_state_view().await?;
        Ok((&view).into())
    }

    /// Generate a compaction from a spec and submit it.
    ///
    /// ## Returns
    /// - `Ok(Compaction)`: The submitted compaction.
    /// - `Err`: If there was an error during submission or reading the submitted compaction.
    pub async fn submit_compaction(&self, spec: CompactionSpec) -> Result<Compaction, Error> {
        let compaction = self.inner.submit_compaction((&spec).try_into()?).await?;
        Ok(Compaction::from(&compaction))
    }

    /// Lists compactions files inside the half-open ID range `[from, to)`.
    pub async fn list_compactions(
        &self,
        from: Option<u64>,
        to: Option<u64>,
    ) -> Result<Vec<VersionedCompactions>, Error> {
        let bounds = into_u64_bounds(from, to)?;
        let compactions = self.inner.list_compactions(bounds).await?;
        Ok(compactions.iter().map(VersionedCompactions::from).collect())
    }

    /// Lists checkpoints, optionally filtering by exact name.
    pub async fn list_checkpoints(
        &self,
        name_filter: Option<String>,
    ) -> Result<Vec<Checkpoint>, Error> {
        let checkpoints = self.inner.list_checkpoints(name_filter.as_deref()).await?;
        Ok(checkpoints.iter().map(Checkpoint::from).collect())
    }

    /// Runs the garbage collector once with the provided options.
    ///
    /// When `options` is `None`, SlateDB's default garbage collector options are used.
    pub async fn run_gc_once(&self, options: Option<GarbageCollectorOptions>) -> Result<(), Error> {
        let options = options.map_or_else(
            slatedb::config::GarbageCollectorOptions::default,
            Into::into,
        );
        self.inner.run_gc_once(options).await.map_err(Into::into)
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

    /// Creates a checkpoint of the db stored in the object store at the specified path using the
    /// provided options.
    pub async fn create_detached_checkpoint(
        &self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, Error> {
        Ok(CheckpointCreateResult::from(
            self.inner
                .create_detached_checkpoint(&slatedb::config::CheckpointOptions::try_from(options)?)
                .await?,
        ))
    }

    /// Refresh the lifetime of an existing checkpoint.
    pub async fn refresh_checkpoint(
        &self,
        id: String,
        lifetime_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.inner
            .refresh_checkpoint(
                try_checkpoint_id_from_str(&id)?,
                lifetime_ms.map(Duration::from_millis),
            )
            .await
            .map_err(Into::into)
    }

    /// Deletes the checkpoint with the specified id.
    pub async fn delete_checkpoint(&self, id: String) -> Result<(), crate::Error> {
        self.inner
            .delete_checkpoint(try_checkpoint_id_from_str(&id)?)
            .await
            .map_err(Into::into)
    }

    pub fn create_clone_builder_from_source(
        &self,
        source: CloneSourceSpec,
    ) -> Result<Arc<CloneBuilder>, Error> {
        Ok(CloneBuilder::new(
            self.inner
                .create_clone_builder_from_source(source.try_into()?),
        ))
    }
}
