use std::sync::Arc;

use crate::utils::MonotonicSeq;

/// Oracle is a struct that centralizes the generation & maintainance of various
/// sequence numbers. These sequence numbers are mostly related to the lifecycle
/// of a transaction commit.
pub(crate) struct Oracle {
    /// is assigned immediately when a write begins, it's possible that the write
    /// has not been committed or finally failed.
    pub(crate) last_seq: Arc<MonotonicSeq>,
    /// The sequence number of the most recent write that has been fully committed.
    /// For reads with dirty=false, the maximum visible sequence number is capped
    /// at last_committed_seq.
    pub(crate) last_committed_seq: Arc<MonotonicSeq>,
    /// The sequence number of the most recent write that has been fully durable
    /// flushed to the remote storage.
    pub(crate) last_remote_persisted_seq: Arc<MonotonicSeq>,
    /// The next wal id to be assigned. This field is incremented when a new WAL
    /// is created in WAL buffer, or during WAL replay after replayed a WAL file.
    pub(crate) next_wal_id: Arc<MonotonicSeq>,
}

impl Oracle {
    /// Create a new Oracle with the last committed sequence number and the next wal id.
    pub(crate) fn new(last_committed_seq: MonotonicSeq, next_wal_id: MonotonicSeq) -> Self {
        let last_committed_seq = Arc::new(last_committed_seq);
        Self {
            last_seq: last_committed_seq.clone(),
            last_committed_seq: last_committed_seq.clone(),
            last_remote_persisted_seq: last_committed_seq,
            next_wal_id: Arc::new(next_wal_id),
        }
    }

    pub(crate) fn with_last_seq(self, last_seq: MonotonicSeq) -> Self {
        Self {
            last_seq: Arc::new(last_seq),
            ..self
        }
    }

    pub(crate) fn with_last_remote_persisted_seq(
        self,
        last_remote_persisted_seq: MonotonicSeq,
    ) -> Self {
        Self {
            last_remote_persisted_seq: Arc::new(last_remote_persisted_seq),
            ..self
        }
    }
}
