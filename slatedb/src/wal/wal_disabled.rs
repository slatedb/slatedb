use async_trait::async_trait;
use crate::RowEntry;
use crate::wal::{FlushResultFuture, WalError, WalObserver, WalStatus, WalStatusListener, WalWriter};

pub(crate) struct DisabledWalWriter {
    observer: DisabledWalObserver
}

impl DisabledWalWriter {
    pub(crate) fn new(status: WalStatus) -> Self {
        Self {
            observer: DisabledWalObserver { status }
        }
    }
}

#[async_trait]
impl WalWriter for DisabledWalWriter {
    async fn append(&mut self, _write_batch: &[RowEntry]) -> Result<(), WalError> {
        panic!("append called on DisabledWalWriter")
    }

    async fn flush(&mut self) -> Result<FlushResultFuture, WalError> {
        panic!("flush called on DisabledWalWriter")
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(self.observer.clone())
    }

    fn status(&self) -> WalStatus {
        self.observer.status.clone()
    }

    async fn close(&mut self) -> Result<(), WalError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DisabledWalObserver {
    status: WalStatus
}

impl WalObserver for DisabledWalObserver {
    fn status(&self) -> WalStatus {
        self.status.clone()
    }

    fn subscribe(&self, _listener: WalStatusListener) -> Result<(), WalError> {
        Ok(())
    }
}