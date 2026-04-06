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

/// Manages database lifecycle status, including the close result and
/// status subscriptions.
#[derive(Clone, Debug)]
pub(crate) struct DbStatusManager {
    cell: WatchableOnceCell<Result<(), SlateDBError>>,
    tx: watch::Sender<DbStatus>,
}

impl DbStatusManager {
    pub(crate) fn new(initial_durable_seq: u64) -> Self {
        let (tx, _) = watch::channel(DbStatus {
            durable_seq: initial_durable_seq,
            close_reason: None,
        });
        Self {
            cell: WatchableOnceCell::new(),
            tx,
        }
    }

    pub(crate) fn write_close_result(&self, result: Result<(), SlateDBError>) {
        let reason = match &result {
            Ok(()) => CloseReason::Clean,
            Err(err) => CloseReason::from(crate::Error::from(err.clone()).kind()),
        };
        if self.cell.write(result) {
            self.report_closed(reason);
        }
    }

    pub(crate) fn closed_result_reader(
        &self,
    ) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.cell.reader()
    }

    /// Create a safe sender + raw receiver pair wired to this closed state.
    ///
    /// The sender uses this state's closed result for shutdown-aware error
    /// propagation. The receiver is a raw `async_channel::Receiver` suitable
    /// for use with `MessageHandlerExecutor::add_handler`.
    pub(crate) fn create_safe_channel<T>(
        &self,
    ) -> (
        crate::utils::safe_async_channel::SafeSender<T>,
        async_channel::Receiver<T>,
    ) {
        let (tx, rx) = async_channel::unbounded();
        let safe_tx =
            crate::utils::safe_async_channel::SafeSender::new(tx, self.closed_result_reader());
        (safe_tx, rx)
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
}
