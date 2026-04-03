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

#[derive(Clone, Debug)]
pub(crate) struct ClosedResultWriter {
    cell: WatchableOnceCell<Result<(), SlateDBError>>,
}

impl ClosedResultWriter {
    pub(crate) fn new() -> Self {
        Self {
            cell: WatchableOnceCell::new(),
        }
    }

    pub(crate) fn write(&self, result: Result<(), SlateDBError>) {
        self.cell.write(result);
    }

    pub(crate) fn reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.cell.reader()
    }

    /// Create a safe sender + raw receiver pair wired to this closed state.
    ///
    /// The sender uses this state's closed result for shutdown-aware error
    /// propagation. The receiver is a raw `async_channel::Receiver` suitable
    /// for use with `MessageHandlerExecutor::add_handler`.
    pub(crate) fn channel<T>(
        &self,
    ) -> (
        crate::utils::safe_async_channel::SafeSender<T>,
        async_channel::Receiver<T>,
    ) {
        let (tx, rx) = async_channel::unbounded();
        let safe_tx = crate::utils::safe_async_channel::SafeSender::new(tx, self.reader());
        (safe_tx, rx)
    }

    /// Spawn a background task that watches for the closed result and reports
    /// the close reason to the status reporter.
    pub(crate) fn spawn_close_watcher(&self, reporter: DbStatusReporter) {
        let mut reader = self.reader();
        tokio::spawn(async move {
            let result = reader.await_value().await;
            let reason = match &result {
                Ok(()) => CloseReason::Clean,
                Err(err) => CloseReason::from(crate::Error::from(err.clone()).kind()),
            };
            reporter.report_closed(reason);
        });
    }
}
