use slatedb_txn_obj::DirtyObject;
use tokio::sync::watch;

use crate::db_state::ManifestCore;
use crate::error::SlateDBError;
use crate::manifest::Manifest;
use crate::utils::WatchableOnceCell;
use crate::CloseReason;

/// A manifest snapshot paired with its version ID for monotonic ordering.
#[derive(Clone, Debug, PartialEq)]
pub struct VersionedManifest {
    /// The version ID of the manifest.
    pub id: u64,
    /// The manifest state at this version.
    pub manifest: ManifestCore,
}

impl From<DirtyObject<Manifest>> for VersionedManifest {
    fn from(dirty: DirtyObject<Manifest>) -> Self {
        Self {
            id: dirty.id.id(),
            manifest: dirty.value.core,
        }
    }
}

/// Current status of the database, exposed via [`crate::Db::subscribe`].
///
/// Subscribers receive a [`tokio::sync::watch::Receiver<DbStatus>`] which
/// always reflects the latest state. When the database is dropped the watch
/// channel closes and [`changed()`](tokio::sync::watch::Receiver::changed)
/// returns an error.
#[derive(Clone, Debug, PartialEq)]
pub struct DbStatus {
    /// The durable sequence number. All writes with a sequence number less
    /// than or equal to this value are durably persisted to object storage
    /// and will survive process restarts.
    pub durable_seq: u64,
    /// The current in-memory manifest snapshot observed by this handle.
    ///
    /// This matches the manifest returned by [`crate::Db::manifest`] for the
    /// same handle.
    pub current_manifest: VersionedManifest,
    /// Set once the database has been closed, indicating the reason.
    pub close_reason: Option<CloseReason>,
}

pub(crate) trait ClosedResultWriter: std::fmt::Debug + Send + Sync + 'static {
    fn write_result(&self, result: Result<(), SlateDBError>);
    fn result_reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>>;
}

/// Manages database lifecycle status, including the close result and
/// status subscriptions.
#[derive(Clone, Debug)]
pub(crate) struct DbStatusManager {
    cell: WatchableOnceCell<Result<(), SlateDBError>>,
    tx: watch::Sender<DbStatus>,
}

impl DbStatusManager {
    #[cfg(test)]
    pub(crate) fn new(initial_durable_seq: u64) -> Self {
        Self::new_with_manifest(
            initial_durable_seq,
            VersionedManifest {
                id: 1,
                manifest: ManifestCore::new(),
            },
        )
    }

    pub(crate) fn new_with_manifest(
        initial_durable_seq: u64,
        initial_manifest: VersionedManifest,
    ) -> Self {
        let (tx, _) = watch::channel(DbStatus {
            durable_seq: initial_durable_seq,
            current_manifest: initial_manifest,
            close_reason: None,
        });
        Self {
            cell: WatchableOnceCell::new(),
            tx,
        }
    }

    pub(crate) fn report_durable_seq(&self, seq: u64) {
        self.tx.send_if_modified(|s| {
            if seq > s.durable_seq {
                s.durable_seq = seq;
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn report_manifest(&self, versioned: VersionedManifest) {
        self.tx.send_if_modified(|s| {
            if versioned.id >= s.current_manifest.id && s.current_manifest != versioned {
                s.current_manifest = versioned;
                true
            } else {
                false
            }
        });
    }

    fn report_closed(&self, reason: CloseReason) {
        self.tx.send_if_modified(|s| {
            if s.close_reason.is_none() {
                s.close_reason = Some(reason);
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<DbStatus> {
        self.tx.subscribe()
    }

    pub(crate) fn status(&self) -> DbStatus {
        let rx = self.subscribe();
        let status = rx.borrow().clone();
        status
    }
}

impl ClosedResultWriter for WatchableOnceCell<Result<(), SlateDBError>> {
    fn write_result(&self, result: Result<(), SlateDBError>) {
        self.write(result);
    }

    fn result_reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.reader()
    }
}

impl ClosedResultWriter for DbStatusManager {
    fn write_result(&self, result: Result<(), SlateDBError>) {
        let reason = match &result {
            Ok(()) => CloseReason::Clean,
            Err(err) => CloseReason::from(crate::Error::from(err.clone()).kind()),
        };
        if self.cell.write(result) {
            self.report_closed(reason);
        }
    }

    fn result_reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.cell.reader()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versioned_manifest(id: u64) -> VersionedManifest {
        VersionedManifest {
            id,
            manifest: ManifestCore::new(),
        }
    }

    #[test]
    fn should_not_notify_on_same_manifest() {
        // given
        let initial = versioned_manifest(1);
        let mgr = DbStatusManager::new_with_manifest(0, initial.clone());
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.report_manifest(initial);

        // then
        assert!(!rx.has_changed().unwrap());
    }

    #[test]
    fn should_not_notify_on_older_manifest() {
        // given
        let mgr = DbStatusManager::new_with_manifest(0, versioned_manifest(5));
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.report_manifest(versioned_manifest(3));

        // then
        assert!(!rx.has_changed().unwrap());
    }
}
