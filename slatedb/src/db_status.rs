use std::sync::Arc;

use tokio::sync::watch;

use crate::error::SlateDBError;
use crate::utils::WatchableOnceCell;
use crate::CloseReason;

/// Current status of the database, exposed via [`crate::Db::subscribe`].
///
/// Subscribers receive a [`tokio::sync::watch::Receiver<DbStatus>`] which
/// always reflects the latest state. When the database is dropped the watch
/// channel closes and [`changed()`](tokio::sync::watch::Receiver::changed)
/// returns an error.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct DbStatus {
    /// The durable sequence number. All writes with a sequence number less
    /// than or equal to this value are durably persisted to object storage
    /// and will survive process restarts.
    pub durable_seq: u64,
    /// Set once the database has been closed, indicating the reason.
    pub close_reason: Option<CloseReason>,
}

#[derive(Clone)]
pub(crate) struct DbStatusReporter {
    tx: watch::Sender<DbStatus>,
}

impl DbStatusReporter {
    pub(crate) fn new(initial_durable_seq: u64) -> Self {
        let (tx, _) = watch::channel(DbStatus {
            durable_seq: initial_durable_seq,
            close_reason: None,
        });
        Self { tx }
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

    pub(crate) fn report_closed(&self, reason: CloseReason) {
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
}

type OnCloseCallback = Arc<dyn Fn(CloseReason) + Send + Sync>;

#[derive(Clone)]
pub(crate) struct ClosedResultWriter {
    cell: WatchableOnceCell<Result<(), SlateDBError>>,
    on_close: Option<OnCloseCallback>,
}

impl std::fmt::Debug for ClosedResultWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosedResultWriter")
            .field("cell", &self.cell)
            .field("on_close", &self.on_close.as_ref().map(|_| "..."))
            .finish()
    }
}

impl ClosedResultWriter {
    pub(crate) fn new(cell: WatchableOnceCell<Result<(), SlateDBError>>) -> Self {
        Self {
            cell,
            on_close: None,
        }
    }

    pub(crate) fn with_on_close(mut self, callback: OnCloseCallback) -> Self {
        self.on_close = Some(callback);
        self
    }

    pub(crate) fn write(&self, result: Result<(), SlateDBError>) {
        let reason = match &result {
            Ok(()) => CloseReason::Clean,
            Err(err) => CloseReason::from(crate::Error::from(err.clone()).kind()),
        };
        self.cell.write(result);
        if let Some(on_close) = &self.on_close {
            on_close(reason);
        }
    }

    pub(crate) fn reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.cell.reader()
    }
}
