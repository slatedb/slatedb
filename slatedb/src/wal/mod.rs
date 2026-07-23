use crate::error::SlateDBError;
use crate::manifest::store::FenceableManifest;
use crate::{CloseReason, ErrorKind, RowEntry, VersionedManifest};
use async_trait::async_trait;
use futures::future::BoxFuture;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::{Bound, Range};
use std::sync::Arc;

#[cfg(test)]
pub(crate) mod test_utils;
pub(crate) mod wal_disabled;
pub(crate) mod wal_sst_builder;
pub(crate) mod writer_init;

/// A range of WAL File IDs
pub struct WalFileRange(Bound<u64>, Bound<u64>);

impl From<Range<u64>> for WalFileRange {
    fn from(range: Range<u64>) -> Self {
        WalFileRange(Bound::Included(range.start), Bound::Excluded(range.end))
    }
}

impl TryFrom<WalFileRange> for Range<u64> {
    type Error = ();

    fn try_from(range: WalFileRange) -> Result<Self, Self::Error> {
        match (range.0, range.1) {
            (Bound::Included(start), Bound::Excluded(end)) => Ok(start..end),
            _ => Err(()),
        }
    }
}

/// Defines the types of errors that can be returned by WAL implementations.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WalError {
    /// The WAL writer was fenced
    Fenced,
    /// A WalIterator observed that the tail of the WAL was truncated while iterating.
    WalTruncated,
    /// Operation against wal after it was closed
    Closed,
    /// WAL is unavailable, e.g. due to an I/O error or error in the backing storage system
    Unavailable(Arc<dyn Error + Sync + Send + 'static>),
    /// WAL implementation detected invalid data/corruption
    DataError(Arc<dyn Error + Sync + Send + 'static>),
    /// Indicates that the WAL is in some unexpected/unrecoverable state.
    InternalError(Arc<dyn Error + Sync + Send + 'static>),
}

impl Display for WalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Fenced => write!(f, "WAL writer was fenced"),
            WalError::WalTruncated => write!(f, "WAL was truncated"),
            WalError::Closed => write!(f, "WAL is closed"),
            WalError::Unavailable(source) => write!(f, "WAL is unavailable: {source}"),
            WalError::DataError(source) => write!(f, "WAL data error: {source}"),
            WalError::InternalError(source) => write!(f, "WAL internal error: {source}"),
        }
    }
}

impl Error for WalError {}

/// The writer's manifest after fencing. Created by calling [`ManifestFencer::fence`]
pub struct WriterManifest {
    manifest: FenceableManifest,
}

impl From<WriterManifest> for FenceableManifest {
    fn from(manifest: WriterManifest) -> Self {
        manifest.manifest
    }
}

impl From<FenceableManifest> for WriterManifest {
    fn from(manifest: FenceableManifest) -> Self {
        WriterManifest { manifest }
    }
}

impl WriterManifest {
    /// Returns the current manifest.
    pub fn manifest(&self) -> VersionedManifest {
        let (id, manifest) = self.manifest.manifest();
        VersionedManifest::from_manifest(id, manifest.clone())
    }

    /// Returns the WAL ID up to which SlateDB has guaranteed to have stored all data in the
    /// LSM tree.
    pub fn replay_after_wal_id(&self) -> u64 {
        self.manifest().core().replay_after_wal_id
    }

    /// Returns the writer's epoch
    pub fn epoch(&self) -> u64 {
        self.manifest().writer_epoch()
    }

    /// Refreshes the current manifest. Implementations of `WriterInit::fence_and_init` can
    /// use this to detect whether the manifest has been fenced while executing the fencing
    /// protocol. SlateDB will call this after calling [`WriterInit::fence_and_init`]
    pub async fn refresh(&mut self) -> Result<(), WalError> {
        self.manifest.refresh().await?;
        Ok(())
    }
}

/// The result returned by [`WriterInit::fence_and_init`]
pub struct WriterInitResult {
    // TODO: change me to an iterator
    /// An iterator that returns writes that must be replayed before starting SlateDB to recover
    /// data from the WAL.
    pub replay_range: WalFileRange,
    /// The WAL writer that will be used to append new writes to the WAL
    pub wal_writer: Box<dyn WalWriter>,
}

/// API for fencing and initializing a new WAL writer for use by [`crate::db::Db`]. SlateDB requires
/// WAL implementations to execute a fencing protocol that guarantees (1) that earlier writers no
/// longer write to the db and (2) all rows present in the WAL but not in the LSM tree (L0 and
/// sorted runs) are recovered.
///
/// Every [`crate::db::Db`] instance is assigned a unique `u64` epoch. The epoch is assigned when
/// fencing the Manifest. A given Db instance writes both the WAL and its Manifest (e.g. with new
/// SSTs) independently. The fencing protocol that yields epoch E must ensure that:
/// (1) After the first write to the Manifest with epoch E, there are no further writes to either
///     the Manifest or WAL with epoch E' < E
/// (2) After the first write to the WAL with epoch E, there are no further writes to either the
///     Manifest or WAL with epoch E' < E
/// (3) All rows from the WAL from writers with epoch E' < E that are not present in L0/SRs are
///     replayed before serving reads/writes.
///
/// `[WriterInit::fence_and_init]` is responsible for
/// (1) Fencing the WAL such that no writers with an epoch earlier than [`WriterManifest::epoch`]
/// (2) Constructing a [`WalWriter`] instance that the writer uses to append new WAL entries.
/// (3) Resolving the end of the WAL and constructing a [`WalReplayIterator`] that returns all
///     rows in WAL files between [`WriterManifest::replay_after_wal_id`] (exclusive) and the
///     current end of the WAL.
#[async_trait]
pub trait WriterInit {
    /// Fences the WAL and returns a [`WriterInitResult`] with a [`WalWriter`] and
    /// [`WalReplayIterator`] used to recover writes that have not yet been flushed to the tree.
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError>;
}

/// Describes the current status of the WAL
#[derive(Debug, Clone)]
pub struct WalStatus {
    /// Set to Some if the WAL has permanently shut down, along with the reason. The reason should
    /// be [`WalError::Closed`] on a normal shutdown, and some other [`WalError`] variant on
    /// failure.
    pub closed_reason: Option<WalError>,
    /// The estimated in-memory bytes used by the WAL to buffer unflushed writes.
    pub estimated_bytes: usize,
    /// The id of the last WAL file that was durably flushed
    pub last_flushed_wal_id: u64,
    /// The last sequence number that was durably flushed
    pub last_flushed_seq: Option<u64>,
    /// The number of writes currently buffered
    #[allow(dead_code)]
    pub buffered_wal_entries_count: usize,
}

/// An event emitted by a [`WalWriter`] to subscribers.
#[derive(Debug, Clone)]
pub enum WalEvent {
    /// Emitted when a WAL file is durably flushed to storage. On receipt of this event, SlateDB
    /// notifies write tasks blocked on [`crate::config::WriteOptions::await_durable`]
    WalFlushed(WalStatus),
    /// Emitted when the WAL has closed with the final wal status containing the closed reason
    WalClosed(WalStatus),
}

/// A listener that's called back on WAL events.
pub type WalStatusListener = Arc<dyn Fn(WalEvent) + Send + Sync + 'static>;

/// An observer that can read the current [`WalStatus`] and subscribe to event callbacks.
#[async_trait]
pub trait WalObserver: Send + Sync + 'static {
    /// Returns the current [`WalStatus`].
    fn status(&self) -> Result<WalStatus, WalStatus>;

    /// Adds a listener that subscribes to event callbacks.
    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError>;
}

pub type FlushResultFuture = BoxFuture<'static, Result<(), WalError>>;

/// The WAL's write API. Used by SlateDB to append new WAL writes. Is returned by
/// [`WalWriterInit::fence_and_init_writer`].
///
/// Each call to [`WalWriter::append`] takes a single SlateDB write batch, where all rows share
/// the same sequence number ([`RowEntry::seq`]). [`WalWriter`] (optionally accumulates/buffers
/// rows and) writes consecutive write batches into consecutive WAL Files, where each WAL File
/// contains some rows from the total sequence of rows. Specifically:
/// - WAL Files must have a total order and each WAL File must have a u64 id that is greater than
///   all earlier WAL Files.
/// - Reading WAL Files in order should yield rows in sequence order.
/// - The writes in a given write batch must be written to WAL files atomically. That is, a
///   [`WalIterator`] should either observe all the writes with a given sequence number or none
///   of them.
#[async_trait]
pub trait WalWriter: Send {
    /// Append a write batch to the WAL.
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError>;

    /// Triggers a flush of all appended write batches to durable storage. Returns a
    /// future that receives the result of the flush once it completes.
    async fn flush(&mut self) -> Result<FlushResultFuture, WalError>;

    /// Returns a `WalObserver` for reading [`WalStatus`] and subscribing to events.
    fn observer(&self) -> Box<dyn WalObserver>;

    /// Returns the current `WalStatus`. If the [`WalWriter`] has failed, then returns Err with the
    /// final [`WalStatus`] and the reason for the failure in [`WalStatus::closed_reason`]
    fn status(&self) -> Result<WalStatus, WalStatus>;

    /// Close the `WalWriter` and release resources
    async fn close(&mut self) -> Result<(), WalError>;
}

impl From<WalStatus> for WalError {
    fn from(status: WalStatus) -> Self {
        status
            .closed_reason
            .expect("unexpected conversion of wal status with no error")
    }
}

impl From<WalStatus> for SlateDBError {
    fn from(status: WalStatus) -> Self {
        WalError::from(status).into()
    }
}

impl From<SlateDBError> for WalError {
    fn from(value: SlateDBError) -> Self {
        {
            let public: crate::Error = value.clone().into();
            match public.kind() {
                ErrorKind::Closed(CloseReason::Fenced) => WalError::Fenced,
                ErrorKind::Closed(CloseReason::Clean) => WalError::Closed,
                ErrorKind::Closed(_) => WalError::InternalError(Arc::new(value)),
                ErrorKind::Unavailable => WalError::Unavailable(Arc::new(value)),
                ErrorKind::Invalid => WalError::InternalError(Arc::new(value)),
                ErrorKind::Data => WalError::DataError(Arc::new(value)),
                ErrorKind::Internal => WalError::InternalError(Arc::new(value)),
                ErrorKind::Transaction => WalError::InternalError(Arc::new(value)),
            }
        }
    }
}

impl From<WalError> for SlateDBError {
    fn from(value: WalError) -> Self {
        match value {
            WalError::Fenced => SlateDBError::Fenced,
            WalError::WalTruncated => SlateDBError::WalTruncated,
            WalError::Closed => SlateDBError::Closed,
            WalError::Unavailable(err) => SlateDBError::WalUnavailable(err),
            WalError::DataError(err) => SlateDBError::WalDataError(err),
            WalError::InternalError(err) => SlateDBError::WalInternalError(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WalError;
    use std::sync::Arc;

    #[test]
    fn wal_error_display() {
        let source = || {
            Arc::new(std::io::Error::other("source error"))
                as Arc<dyn std::error::Error + Send + Sync + 'static>
        };

        assert_eq!(WalError::Fenced.to_string(), "WAL writer was fenced");
        assert_eq!(WalError::WalTruncated.to_string(), "WAL was truncated");
        assert_eq!(WalError::Closed.to_string(), "WAL is closed");
        assert_eq!(
            WalError::Unavailable(source()).to_string(),
            "WAL is unavailable: source error"
        );
        assert_eq!(
            WalError::DataError(source()).to_string(),
            "WAL data error: source error"
        );
        assert_eq!(
            WalError::InternalError(source()).to_string(),
            "WAL internal error: source error"
        );
    }
}
