use std::error::Error;
use std::ops::{Bound, Range};
use std::sync::Arc;
use async_trait::async_trait;
use futures::future::BoxFuture;
use log::warn;
use tokio::sync::oneshot;
use crate::error::SlateDBError;
use crate::manifest::Manifest;
use crate::manifest::store::FenceableManifest;
use crate::RowEntry;

pub(crate) mod wal_sst_builder;
pub(crate) mod writer_init;
pub(crate) mod wal_disabled;
#[cfg(test)]
pub(crate) mod test_utils;

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
            _ => Err(())
        }
    }
}

/// Defines the types of errors that can be returned by WAL implementations.
#[derive(Debug)]
pub enum WalError {
    /// The WAL writer was fenced
    Fenced,
    /// IO error writing/reading the WAL
    IoError(Box<std::io::Error>),
    /// Fatal error indicating that the WAL is in some unexpected/unrecoverable state.
    InternalError(Box<dyn Error + 'static>),
    /// A WalIterator observed that the tail of the WAL was truncated while iterating.
    WalTruncated,
    /// Used internally by SlateDB to propagate its internal error type.
    SlateDBError(Box<dyn Error + 'static>),
    /// Operation against wal after it was closed
    Closed,
}

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
    pub fn manifest(&self) -> &Manifest {
        self.manifest.manifest()
    }

    /// Returns the WAL ID up to which SlateDB has guaranteed to have stored all data in the
    /// LSM tree.
    pub fn replay_after_wal_id(&self) -> u64 {
        self.manifest().core.replay_after_wal_id
    }

    /// Returns the writer's epoch
    pub fn epoch(&self) -> u64 {
        self.manifest().writer_epoch
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
        manifest: &mut WriterManifest
    ) -> Result<WriterInitResult, WalError>;
}

/// Describes the current status of the WAL
#[derive(Debug, Clone)]
pub(crate) struct WalStatus {
    /// The estimated in-memory bytes used by the WAL to buffer unflushed writes.
    pub(crate) estimated_bytes: usize,
    /// The id of the last WAL file that was durably flushed
    pub(crate) last_flushed_wal_id: u64,
    /// The last sequence number that was durably flushed
    pub(crate) last_flushed_seq: Option<u64>,
    /// The number of writes currently buffered
    #[allow(dead_code)]
    pub(crate) buffered_wal_entries_count: usize,
}

/// An event emitted by a [`WalWriter`] to subscribers.
#[derive(Debug, Clone)]
pub enum WalEvent {
    /// Emitted when a WAL file is durably flushed to storage. On receipt of this event, SlateDB
    /// notifies write tasks blocked on [`crate::config::WriteOptions::await_durable`]
    WalFlushed(WalStatus),
}

/// A listener that's called back on WAL events.
pub type WalStatusListener = Arc<dyn Fn(WalEvent) + Send + Sync + 'static>;

/// An observer that can read the current [`WalStatus`] and subscribe to event callbacks.
#[async_trait]
pub trait WalObserver: Send + Sync + 'static {
    /// Returns the current [`WalStatus`].
    fn status(&self) -> WalStatus;

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

    /// Returns the current `WalStatus`
    fn status(&self) -> WalStatus;

    /// Close the `WalWriter` and release resources
    async fn close(&mut self) -> Result<(), WalError>;
}

impl From<SlateDBError> for WalError {
    fn from(value: SlateDBError) -> Self {
        match value {
            SlateDBError::Fenced => WalError::Fenced,
            SlateDBError::Closed => WalError::Closed,
            _ => WalError::SlateDBError(Box::new(value)),
        }
    }
}

impl From<WalError> for SlateDBError {
    fn from(value: WalError) -> Self {
        match value {
            WalError::Fenced => SlateDBError::Fenced,
            WalError::IoError(err) => {
                SlateDBError::IoError(Arc::new(*err))
            },
            WalError::InternalError(err) => SlateDBError::InvalidDBState,
            WalError::WalTruncated => todo!(),
            WalError::SlateDBError(err) => {
                match err.downcast::<SlateDBError>() {
                    Ok(err) => *err,
                    Err(err) => {
                        warn!("unexpected error type in WalError::SlateDBError: {:?}", err);
                        SlateDBError::InvalidDBState
                    }
                }
            },
            WalError::Closed => SlateDBError::Closed,
        }
    }
}
