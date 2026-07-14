use crate::wal::{WalError, WalEvent, WalObserver, WalStatus, WalStatusListener};
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct SharedWalStatus {
    inner: Arc<Mutex<StatusState>>,
}

struct StatusState {
    status: WalStatus,
    listeners: Vec<WalStatusListener>,
}

impl SharedWalStatus {
    pub(super) fn new(last_flushed_wal_id: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StatusState {
                status: WalStatus {
                    closed_reason: None,
                    estimated_bytes: 0,
                    last_flushed_wal_id,
                    last_flushed_seq: None,
                    buffered_wal_entries_count: 0,
                },
                listeners: Vec::new(),
            })),
        }
    }

    pub(super) fn status(&self) -> Result<WalStatus, WalStatus> {
        let status = self.inner.lock().status.clone();
        if status.closed_reason.is_some() {
            Err(status)
        } else {
            Ok(status)
        }
    }

    pub(super) fn record_buffered(&self, bytes: usize) {
        let mut state = self.inner.lock();
        state.status.estimated_bytes = state.status.estimated_bytes.saturating_add(bytes);
        state.status.buffered_wal_entries_count =
            state.status.buffered_wal_entries_count.saturating_add(1);
    }

    pub(super) fn record_flushed(&self, last_flushed_wal_id: u64, last_flushed_seq: Option<u64>) {
        let (status, listeners) = {
            let mut state = self.inner.lock();
            if state.status.closed_reason.is_some() {
                return;
            }
            state.status.estimated_bytes = 0;
            state.status.buffered_wal_entries_count = 0;
            state.status.last_flushed_wal_id = last_flushed_wal_id;
            if last_flushed_seq.is_some() {
                state.status.last_flushed_seq = last_flushed_seq;
            }
            (state.status.clone(), state.listeners.clone())
        };
        for listener in listeners {
            listener(WalEvent::WalFlushed(status.clone()));
        }
    }

    pub(super) fn close(&self, reason: WalError) {
        let (status, listeners) = {
            let mut state = self.inner.lock();
            if state.status.closed_reason.is_some() {
                return;
            }
            state.status.closed_reason = Some(reason);
            state.status.estimated_bytes = 0;
            state.status.buffered_wal_entries_count = 0;
            (state.status.clone(), state.listeners.clone())
        };
        for listener in listeners {
            listener(WalEvent::WalClosed(status.clone()));
        }
    }

    pub(super) fn observer(&self) -> KafkaWalObserver {
        KafkaWalObserver {
            status: self.clone(),
        }
    }
}

pub struct KafkaWalObserver {
    status: SharedWalStatus,
}

impl WalObserver for KafkaWalObserver {
    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.status.status()
    }

    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError> {
        let closed_status = {
            let mut state = self.status.inner.lock();
            if state.status.closed_reason.is_some() {
                Some(state.status.clone())
            } else {
                state.listeners.push(listener.clone());
                None
            }
        };
        if let Some(status) = closed_status {
            listener(WalEvent::WalClosed(status));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifies_subscribers_after_flush_and_close() {
        let status = SharedWalStatus::new(3);
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = events.clone();
        status
            .observer()
            .subscribe(Arc::new(move |event| captured.lock().push(event)))
            .unwrap();

        status.record_buffered(100);
        status.record_flushed(4, Some(9));
        status.close(WalError::Closed);

        let events = events.lock();
        assert_eq!(events.len(), 2);
        match &events[0] {
            WalEvent::WalFlushed(status) => {
                assert_eq!(status.last_flushed_wal_id, 4);
                assert_eq!(status.last_flushed_seq, Some(9));
                assert_eq!(status.estimated_bytes, 0);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(matches!(&events[1], WalEvent::WalClosed(_)));
    }
}
