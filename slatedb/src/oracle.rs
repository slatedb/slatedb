use std::sync::Arc;

use parking_lot::Mutex;

use crate::clock::SystemClock;
use crate::seq_tracker::{SequenceTracker, TrackedSeq};
use crate::utils::MonotonicSeq;

/// Oracle is a struct that centralizes the generation & maintenance of various
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
    /// A sequence tracker that correlates sequence numbers with system clock ticks.
    /// The tracker is limited to 8192 entries and downsamples data when it gets full.
    sequence_tracker: Arc<Mutex<SequenceTracker>>,
    /// The system clock to use when tracking sequence numbers (sequence numbers
    /// will be associated with the system clock tick and only be recorded every
    /// 60 seconds worth of ticks to this clock)
    system_clock: Arc<dyn SystemClock>,
}

impl Oracle {
    /// Create a new Oracle with the last committed sequence number. for the read only
    /// db instance (DbReader), only the last committed sequence number is needed to be
    /// tracked, and last_seq and last_remote_persisted_seq are considered to be
    /// the same as last_committed_seq.
    pub(crate) fn new(
        last_committed_seq: MonotonicSeq,
        system_clock: Arc<dyn SystemClock>,
    ) -> Self {
        let last_committed_seq = Arc::new(last_committed_seq);
        Self {
            last_seq: last_committed_seq.clone(),
            last_committed_seq: last_committed_seq.clone(),
            last_remote_persisted_seq: last_committed_seq,
            sequence_tracker: Arc::new(Mutex::new(SequenceTracker::new())),
            system_clock,
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

    pub(crate) fn with_sequence_tracker(self, tracker: SequenceTracker) -> Self {
        Self {
            sequence_tracker: Arc::new(Mutex::new(tracker)),
            ..self
        }
    }

    pub(crate) fn record_sequence(&self, seq: u64) {
        let ts = self.system_clock.now();
        let mut tracker = self.sequence_tracker.lock();
        tracker.insert(TrackedSeq { seq, ts });
    }

    pub(crate) fn sequence_tracker_snapshot(&self) -> SequenceTracker {
        self.sequence_tracker.lock().clone()
    }
}
