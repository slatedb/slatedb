use crate::rand::DbRand;
use crate::utils::IdGenerator;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct TransactionState {
    /// id is used to track the lifecycle of a transaction. when a snapshot/transaction
    /// ends, we can remove the transaction state from the transaction manager by this
    /// id. we can not use seq as the txn id, because it's possible to start multiple
    /// transactions with the same seq number.
    pub(crate) id: Uuid,
    pub(crate) read_only: bool,
    /// seq is the sequence number when the transaction started. this is used to establish
    /// a snapshot of this transaction. we should ensure the compactor cannot recycle
    /// the row versions that are below any seq number of active transactions.
    pub(crate) started_seq: u64,
    /// the sequence number when the transaction committed. this field is only set AFTER
    /// a transaction is committed. this is used to check conflicts with recent committed
    /// transactions.
    committed_seq: Option<u64>,
    /// the write keys of the transaction.
    write_keys: HashSet<Bytes>,
}

impl TransactionState {
    fn track_write_keys(&mut self, keys: impl IntoIterator<Item = Bytes>) {
        self.write_keys.extend(keys);
    }

    fn mark_as_committed(&mut self, seq: u64) {
        self.committed_seq = Some(seq);
    }
}

/// Manages the lifecycle of DbSnapshot objects, tracking all living transaction states
pub struct TransactionManager {
    inner: Arc<RwLock<TransactionManagerInner>>,
    // random number generator for generating transaction IDs
    db_rand: Arc<DbRand>,
}

struct TransactionManagerInner {
    /// Map of transaction state ID to weak reference.
    active_txns: HashMap<Uuid, TransactionState>,
    /// Tracks recently committed transaction states for conflict checks at commit.
    ///
    /// An entry can be garbage collected when *all* active transactions (excluding snapshots,
    /// since snapshots are read-only so it's impossible to have any conflict) have `started_seq`
    /// strictly greater than the entry's `committed_seq`.
    ///
    /// Notes:
    /// - Snapshots are treated as read-only transactions.
    /// - Non-transactional writes are modeled as single-op transactions with `started_seq ==
    ///   committed_seq` and follow the same GC rule.
    /// - If there are no active non-readonly transactions, this deque can be fully drained.
    recent_committed_txns: VecDeque<TransactionState>,
}

impl TransactionManager {
    pub fn new(db_rand: Arc<DbRand>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TransactionManagerInner {
                active_txns: HashMap::new(),
                recent_committed_txns: VecDeque::new(),
            })),
            db_rand,
        }
    }

    /// Register a transaction state with a specific ID
    pub fn new_txn(&self, seq: u64, read_only: bool) -> Uuid {
        let id = self.db_rand.rng().gen_uuid();
        let txn_state = TransactionState {
            id,
            started_seq: seq,
            read_only,
            committed_seq: None,
            write_keys: HashSet::new(),
        };

        {
            let mut inner = self.inner.write();
            inner.active_txns.insert(id, txn_state);
        }

        id
    }

    /// Remove a transaction state when it's dropped. The dropped txn is considered
    /// as rolled back, no side effect is ever produced.
    pub fn drop_txn(&self, txn_id: &Uuid) {
        let mut inner = self.inner.write();
        inner.active_txns.remove(txn_id);
        inner.recycle_recent_committed_txns();
    }

    pub fn check_conflict(&self, keys: &HashSet<Bytes>, started_seq: u64) -> bool {
        let inner = self.inner.read();
        inner.check_conflict(keys, started_seq)
    }

    /// Mark the txn as committed, and record it in recent_committed_txns.
    pub fn track_recent_committed_txn(
        &self,
        txn_id: Option<&Uuid>,
        keys: &HashSet<Bytes>,
        committed_seq: u64,
    ) {
        // if txn_id is not provided, we simply track the write keys as a recent committed txn.
        // it's not needed to make conflict check, because it's a write batch that not bounded
        // with any transaction.
        let txn_id = match txn_id {
            Some(txn_id) => txn_id,
            None => {
                let mut inner = self.inner.write();
                inner.recent_committed_txns.push_back(TransactionState {
                    id: self.db_rand.rng().gen_uuid(),
                    read_only: false,
                    started_seq: committed_seq,
                    committed_seq: Some(committed_seq),
                    write_keys: keys.clone(),
                });
                return;
            }
        };

        // remove the transaction from active_txns, and add it to recent_committed_txns
        let mut inner = self.inner.write();
        if let Some(mut txn_state) = inner.active_txns.remove(txn_id) {
            txn_state.track_write_keys(keys.iter().cloned());
            txn_state.mark_as_committed(committed_seq);
            inner.recent_committed_txns.push_back(txn_state);
        }
    }

    /// The min started_seq of all active transactions, including snapshots. This value
    /// is useful to inform the compactor about the min seq of data still needed to be
    /// retained for active transactions, so that the compactor can avoid deleting the
    /// data that is still needed.
    ///
    /// min_active_seq will be persisted to the `recent_snapshot_min_seq` in the manifest
    /// when a new L0 is flushed.
    pub fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.read();
        inner
            .active_txns
            .values()
            .map(|state| state.started_seq)
            .min()
    }
}

impl TransactionManagerInner {
    /// The min started_seq of all non-readonly transactions, this seq is useful to garbage
    /// collect the entries in the `recent_committed_txns` deque.
    fn min_conflict_check_seq(&self) -> Option<u64> {
        self.active_txns
            .values()
            .filter(|state| !state.read_only)
            .map(|state| state.started_seq)
            .min()
    }

    fn recycle_recent_committed_txns(&mut self) {
        let min_conflict_seq = self.min_conflict_check_seq();
        if let Some(min_seq) = min_conflict_seq {
            // Remove transactions that are no longer needed for conflict checking.
            // A transaction can be garbage collected when all active non-readonly transactions
            // have started_seq strictly greater than the transaction's committed_seq.
            // This means we keep transactions where committed_seq >= min_seq.
            self.recent_committed_txns.retain(|txn| {
                if let Some(committed_seq) = txn.committed_seq {
                    committed_seq >= min_seq
                } else {
                    // If committed_seq is None, this shouldn't happen in practice,
                    // but we'll keep it to be safe
                    true
                }
            });
        } else {
            // No active non-readonly transactions, can drain the entire deque
            self.recent_committed_txns.clear();
        }
    }

    fn check_conflict(&self, write_keys: &HashSet<Bytes>, started_seq: u64) -> bool {
        for committed_txn in &self.recent_committed_txns {
            // skip read-only transactions as they don't cause write conflicts
            if committed_txn.read_only {
                continue;
            }

            // this shouldn't happen in recent_committed_txns but let's skip it for safety
            let other_committed_seq = committed_txn.committed_seq.expect(
                "all txns in recent_committed_txns should be committed with committed_seq set",
            );

            // if another transaction committed after the current transaction started,
            // and they have overlapping write keys, then there's a conflict.
            // this means the current transaction couldn't see the other transaction's writes
            // when it started, but they modified the same keys.
            if other_committed_seq > started_seq {
                if !write_keys.is_disjoint(&committed_txn.write_keys) {
                    return true;
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rand::DbRand;
    use bytes::Bytes;
    use std::collections::HashSet;
    use uuid::Uuid;

    struct ConflictTestCase {
        name: &'static str,
        recent_committed_txns: Vec<TransactionState>,
        current_write_keys: Vec<&'static str>,
        current_started_seq: u64,
        expected_conflict: bool,
    }

    #[test]
    fn test_check_conflict_table_driven() {
        let test_cases = vec![
            ConflictTestCase {
                name: "no_recent_committed_txns",
                recent_committed_txns: vec![],
                current_write_keys: vec!["key1", "key2"],
                current_started_seq: 100,
                expected_conflict: false,
            },
            ConflictTestCase {
                name: "no_overlapping_keys",
                recent_committed_txns: vec![TransactionState {
                    id: Uuid::new_v4(),
                    read_only: false,
                    started_seq: 50,
                    committed_seq: Some(80),
                    write_keys: ["key1", "key2"].into_iter().map(Bytes::from).collect(),
                }],
                current_write_keys: vec!["key3", "key4"],
                current_started_seq: 100,
                expected_conflict: false,
            },
            ConflictTestCase {
                name: "concurrent_write_same_key",
                recent_committed_txns: vec![TransactionState {
                    id: Uuid::new_v4(),
                    read_only: false,
                    started_seq: 50,
                    committed_seq: Some(150),
                    write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
                }],
                current_write_keys: vec!["key1"],
                current_started_seq: 100,
                expected_conflict: true,
            },
            ConflictTestCase {
                name: "multiple_committed_mixed_conflict",
                recent_committed_txns: vec![
                    TransactionState {
                        id: Uuid::new_v4(),
                        read_only: false,
                        started_seq: 30,
                        committed_seq: Some(50),
                        write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
                    },
                    TransactionState {
                        id: Uuid::new_v4(),
                        read_only: false,
                        started_seq: 80,
                        committed_seq: Some(150),
                        write_keys: ["key2"].into_iter().map(Bytes::from).collect(),
                    },
                ],
                current_write_keys: vec!["key1", "key2"],
                current_started_seq: 100,
                expected_conflict: true,
            },
            ConflictTestCase {
                name: "readonly_committed_no_conflict",
                recent_committed_txns: vec![TransactionState {
                    id: Uuid::new_v4(),
                    read_only: true,
                    started_seq: 80,
                    committed_seq: Some(150),
                    write_keys: HashSet::new(),
                }],
                current_write_keys: vec!["key1"],
                current_started_seq: 100,
                expected_conflict: false,
            },
            ConflictTestCase {
                name: "committed_before_current_started",
                recent_committed_txns: vec![TransactionState {
                    id: Uuid::new_v4(),
                    read_only: false,
                    started_seq: 30,
                    committed_seq: Some(50),
                    write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
                }],
                current_write_keys: vec!["key1"],
                current_started_seq: 100,
                expected_conflict: false,
            },
            ConflictTestCase {
                name: "exact_seq_boundary",
                recent_committed_txns: vec![TransactionState {
                    id: Uuid::new_v4(),
                    read_only: false,
                    started_seq: 100,
                    committed_seq: Some(100),
                    write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
                }],
                current_write_keys: vec!["key1"],
                current_started_seq: 100,
                expected_conflict: false,
            },
            ConflictTestCase {
                name: "partial_key_overlap_conflict",
                recent_committed_txns: vec![TransactionState {
                    id: Uuid::new_v4(),
                    read_only: false,
                    started_seq: 80,
                    committed_seq: Some(150),
                    write_keys: ["key1", "key2", "key3"]
                        .into_iter()
                        .map(Bytes::from)
                        .collect(),
                }],
                current_write_keys: vec!["key3", "key4", "key5"],
                current_started_seq: 100,
                expected_conflict: true,
            },
            ConflictTestCase {
                name: "max_seq_values",
                recent_committed_txns: vec![TransactionState {
                    id: Uuid::new_v4(),
                    read_only: false,
                    started_seq: u64::MAX - 1,
                    committed_seq: Some(u64::MAX),
                    write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
                }],
                current_write_keys: vec!["key1"],
                current_started_seq: u64::MAX - 1,
                expected_conflict: true, // Should be true since committed_seq > started_seq
            },
        ];

        for case in test_cases {
            let db_rand = Arc::new(DbRand::new(0));
            let txn_manager = TransactionManager::new(db_rand);

            // Set up recent_committed_txns directly
            {
                let mut inner = txn_manager.inner.write();
                inner.recent_committed_txns = case.recent_committed_txns.into();
            }

            // Convert current transaction write keys
            let write_keys: HashSet<Bytes> = case
                .current_write_keys
                .into_iter()
                .map(Bytes::from)
                .collect();

            // Call the method under test
            let inner = txn_manager.inner.read();
            let has_conflict = inner.check_conflict(&write_keys, case.current_started_seq);

            // Verify result
            assert_eq!(
                has_conflict, case.expected_conflict,
                "Test case '{}' failed",
                case.name
            );
        }
    }
}
