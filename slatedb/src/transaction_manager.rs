use crate::error::SlateDBError;
use crate::utils::spawn_bg_task;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use tokio::time::Instant;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) struct TransactionState {
    id: Uuid,
    pub(crate) seq: u64,
}

/// Manages the lifecycle of DbSnapshot objects, tracking all living transaction states
pub struct TransactionManager {
    /// Map of transaction state ID to weak reference
    inner: Arc<RwLock<TransactionManagerInner>>,
    /// cancellation token for the background task.
    cancellation_token: CancellationToken,
}

struct TransactionManagerInner {
    active_txns: HashMap<Uuid, Weak<TransactionState>>,
    /// The channel to send work to the background worker.
    work_tx: Option<mpsc::Sender<TransactionBackgroundWork>>,
    /// task handle of the background worker.
    background_task: Option<JoinHandle<Result<(), SlateDBError>>>,
    /// The last min retention seq that has been synced to the object store.
    last_manifest_sync_time: Option<Instant>,
}

impl TransactionManager {
    pub fn new(cancellation_token: CancellationToken) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TransactionManagerInner {
                active_txns: HashMap::new(),
                work_tx: None,
                background_task: None,
                last_manifest_sync_time: None,
            })),
            cancellation_token,
        }
    }

    /// Start the background task for transaction management
    pub async fn start_background(self: &Arc<Self>) -> Result<(), SlateDBError> {
        if self.inner.read().background_task.is_some() {
            return Err(SlateDBError::WalBufferAlreadyStarted);
        }

        let (work_tx, work_rx) = mpsc::channel(128);
        {
            let mut inner = self.inner.write();
            inner.work_tx = Some(work_tx);
        }

        let background_fut = self
            .clone()
            .do_background_work(work_rx, self.cancellation_token.clone());
        let task_handle = spawn_bg_task(
            &tokio::runtime::Handle::current(),
            move |_| {},
            background_fut,
        );
        {
            let mut inner = self.inner.write();
            inner.background_task = Some(task_handle);
        }
        Ok(())
    }

    /// Register a transaction state with a specific ID
    pub fn new_txn(&self, seq: u64) -> Arc<TransactionState> {
        let id = Uuid::new_v4();
        let txn_state = Arc::new(TransactionState { id, seq });
        {
            let mut inner = self.inner.write();
            inner.active_txns.insert(id, Arc::downgrade(&txn_state));
        }
        txn_state
    }

    /// Remove a transaction state when it's dropped
    pub fn remove_txn(&self, txn_state: &TransactionState) {
        let need_sync_manifest = {
            let mut inner = self.inner.write();
            inner.active_txns.remove(&txn_state.id);

            let need_sync_manifest = inner
                .last_manifest_sync_time
                .map(|t| t.elapsed().as_secs() > 10)
                .unwrap_or(true);
            need_sync_manifest
        };

        if need_sync_manifest {
            if let Some(tx) = self.inner.write().work_tx.as_ref() {
                tx.try_send(TransactionBackgroundWork::SyncManifest).ok();
            }
            self.inner.write().last_manifest_sync_time = Some(Instant::now());
        }
    }

    fn min_retention_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner
            .active_txns
            .values()
            .filter_map(|state| state.upgrade().map(|state| state.seq))
            .min()
    }

    async fn do_background_work(
        self: Arc<Self>,
        mut work_rx: mpsc::Receiver<TransactionBackgroundWork>,
        cancellation_token: CancellationToken,
    ) -> Result<(), SlateDBError> {
        loop {
            select! {
                work = work_rx.recv() => {
                    match work {
                        None => break,
                        Some(work) => {
                            match work {
                                TransactionBackgroundWork::SyncManifest => {
                                    // TODO: sync manifest to the object store
                                }
                            }
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    return Ok(());
                }
            };
        }

        Ok(())
    }
}

enum TransactionBackgroundWork {
    SyncManifest,
}

impl std::fmt::Debug for TransactionBackgroundWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBackgroundWork").finish()
    }
}
