use crate::utils::MonotonicSeq;

/// Oracle is a trait that centralizes the generation & maintenance of various
/// sequence numbers. These sequence numbers are mostly related to the lifecycle
/// of a transaction commit.
pub(crate) trait Oracle: Send + Sync + 'static {
    /// The sequence number of the most recent write that has been fully committed.
    /// For reads with dirty=false, the maximum visible sequence number is capped
    /// at last_committed_seq.
    fn last_committed_seq(&self) -> u64;

    /// The sequence number of the most recent write that has been fully durable
    /// flushed to the remote storage.
    fn last_remote_persisted_seq(&self) -> u64;
}

pub(crate) struct DbOracle {
    pub(crate) last_seq: MonotonicSeq,

    pub(crate) last_committed_seq: MonotonicSeq,

    pub(crate) last_remote_persisted_seq: MonotonicSeq,
}

impl DbOracle {
    pub(crate) fn new(
        last_seq: MonotonicSeq,
        last_committed_seq: MonotonicSeq,
        last_remote_persisted_seq: MonotonicSeq,
    ) -> Self {
        Self {
            last_seq,
            last_committed_seq,
            last_remote_persisted_seq,
        }
    }
}

impl Oracle for DbOracle {
    fn last_committed_seq(&self) -> u64 {
        self.last_committed_seq.load()
    }

    fn last_remote_persisted_seq(&self) -> u64 {
        self.last_remote_persisted_seq.load()
    }
}

pub(crate) struct DbReaderOracle {
    pub(crate) last_remote_persisted_seq: MonotonicSeq,
}

impl DbReaderOracle {
    /// for the read-only db instance (DbReader), only the last remote persisted sequence number
    /// is needed to be tracked, and last_seq and last_remote_persisted_seq are considered to be
    /// the same as last_committed_seq.
    pub(crate) fn new(last_remote_persisted_seq: MonotonicSeq) -> Self {
        Self {
            last_remote_persisted_seq,
        }
    }
}

impl Oracle for DbReaderOracle {
    fn last_committed_seq(&self) -> u64 {
        self.last_remote_persisted_seq.load()
    }

    fn last_remote_persisted_seq(&self) -> u64 {
        self.last_remote_persisted_seq.load()
    }
}
