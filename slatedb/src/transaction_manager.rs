use crate::rand::DbRand;
use crate::utils::IdGenerator;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct TransactionState {
    pub(crate) read_only: bool,
    /// seq is the sequence number when the transaction started. this is used to establish
    /// a snapshot of this transaction. we should ensure the compactor cannot recycle
    /// the row versions that are below any seq number of active transactions.
    pub(crate) started_seq: u64,
    /// the sequence number when the transaction committed. this field is only set AFTER
    /// a transaction is committed. this is used to check conflicts with recent committed
    /// transactions.
    committed_seq: Option<u64>,
    /// the conflict keys of the transaction.
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
/// TODO: have a quota for max active transactions.
pub struct TransactionManager {
    inner: Arc<RwLock<TransactionManagerInner>>,
    // random number generator for generating transaction IDs
    db_rand: Arc<DbRand>,
}

struct TransactionManagerInner {
    /// Map of transaction state ID to transaction states.
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
    pub fn new_txn(&self, seq: u64, read_only: bool, txn_id: Option<Uuid>) -> Uuid {
        let txn_id = match txn_id {
            Some(id) => id,
            None => self.db_rand.rng().gen_uuid(),
        };

        let txn_state = TransactionState {
            read_only,
            started_seq: seq,
            committed_seq: None,
            write_keys: HashSet::new(),
        };

        {
            let mut inner = self.inner.write();
            inner.active_txns.insert(txn_id, txn_state);
        }

        txn_id
    }

    /// Remove a transaction state when it's dropped. The dropped txn is considered
    /// as rolled back, no side effect should be produced.
    /// When a txn is dropped, it's a chance to recycle the recent committed txns.
    pub fn drop_txn(&self, txn_id: &Uuid) {
        let mut inner = self.inner.write();
        inner.active_txns.remove(txn_id);
        inner.recycle_recent_committed_txns();
    }

    /// Check if the current transaction conflicts with any recent committed transactions.
    pub fn check_conflict(&self, txn_id: &Uuid, keys: &HashSet<Bytes>) -> bool {
        let inner = self.inner.read();
        let started_seq = match inner.active_txns.get(txn_id) {
            None => return false,
            Some(txn_state) => txn_state.started_seq,
        };

        inner.check_conflict(keys, started_seq)
    }

    /// Record the a recent write to `recent_commited_txns`. This method should be called after
    /// [`Self::check_conflict`] is passed.
    ///
    /// Please note that it's not necessary to be a "transaction" to call this method. The normal
    /// write operations in a WriteBatch should also be considered as a recent committed txn, and
    /// it's a MUST to track them for conflict check. In this case, the `txn_id` is `None`.
    pub fn track_recent_committed_txn(
        &self,
        txn_id: Option<&Uuid>,
        keys: &HashSet<Bytes>,
        committed_seq: u64,
    ) {
        // remove the transaction from active_txns, and add it to recent_committed_txns
        let mut inner = self.inner.write();

        // if there's no active non-readonly transactions, we don't need to track the recent
        // committed txn, since it's impossible to have any conflict.
        if !inner.active_txns.values().any(|txn| !txn.read_only) {
            return;
        }

        // if txn_id is not provided, we simply track the write keys as a recent committed txn.
        let txn_id = match txn_id {
            Some(txn_id) => txn_id,
            None => {
                inner.recent_committed_txns.push_back(TransactionState {
                    read_only: false,
                    started_seq: committed_seq,
                    committed_seq: Some(committed_seq),
                    write_keys: keys.clone(),
                });
                return;
            }
        };

        // remove the txn from active txns and append it to recent_committed_txns.
        if let Some(mut txn_state) = inner.active_txns.remove(txn_id) {
            // this should never happen, but having this check may let proptest easier to have.
            if txn_state.read_only {
                return;
            }
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

    fn check_conflict(&self, conflict_keys: &HashSet<Bytes>, started_seq: u64) -> bool {
        for committed_txn in &self.recent_committed_txns {
            // skip read-only transactions as they don't cause write conflicts
            if committed_txn.read_only {
                continue;
            }

            // All the recent committed txn should have committed_seq set.
            let other_committed_seq = committed_txn.committed_seq.expect(
                "all txns in recent_committed_txns should be committed with committed_seq set",
            );

            // if another transaction committed after the current transaction started,
            // and they have overlapping write keys, then there's a conflict.
            if other_committed_seq > started_seq
                && !conflict_keys.is_disjoint(&committed_txn.write_keys)
            {
                return true;
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
    use parking_lot::Mutex;
    use rstest::rstest;
    use std::collections::HashSet;

    struct CheckConflictTestCase {
        name: &'static str,
        recent_committed_txns: Vec<TransactionState>,
        current_write_keys: Vec<&'static str>,
        current_started_seq: u64,
        expected_conflict: bool,
    }

    #[test]
    fn test_drop_txn_removes_active_transaction() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create a transaction
        let txn_id = txn_manager.new_txn(100, false, None);

        // Verify it exists in active transactions
        assert!(txn_manager.inner.read().active_txns.contains_key(&txn_id));

        // Drop the transaction
        txn_manager.drop_txn(&txn_id);

        // Verify it's removed
        assert!(!txn_manager.inner.read().active_txns.contains_key(&txn_id));
    }

    #[test]
    fn test_drop_txn_nonexistent_transaction_safe() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Try to drop a non-existent transaction - should not panic
        let fake_id = Uuid::new_v4();
        txn_manager.drop_txn(&fake_id);

        // Should still be able to create new transactions
        let txn_id = txn_manager.new_txn(100, false, None);
        assert!(txn_manager.inner.read().active_txns.contains_key(&txn_id));
    }

    #[test]
    fn test_drop_txn_triggers_garbage_collection() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create an active transaction first to ensure recent_committed_txns tracking
        let txn_id = txn_manager.new_txn(100, false, None);

        // Create and commit a transaction to populate recent_committed_txns
        let keys: HashSet<Bytes> = ["key1"].into_iter().map(Bytes::from).collect();
        txn_manager.track_recent_committed_txn(None, &keys, 50);

        // Verify recent_committed_txns has content
        assert!(!txn_manager.inner.read().recent_committed_txns.is_empty());

        // Drop the active transaction (this should trigger garbage collection)
        txn_manager.drop_txn(&txn_id);

        // Since there are no more active non-readonly transactions,
        // recent_committed_txns should be cleared
        assert!(txn_manager.inner.read().recent_committed_txns.is_empty());
    }

    #[rstest]
    #[case::no_recent_committed_txns(CheckConflictTestCase {
        name: "no_recent_committed_txns",
        recent_committed_txns: vec![],
        current_write_keys: vec!["key1", "key2"],
        current_started_seq: 100,
        expected_conflict: false,
    })]
    #[case::no_overlapping_keys(CheckConflictTestCase {
        name: "no_overlapping_keys",
        recent_committed_txns: vec![TransactionState {
            read_only: false,
            started_seq: 50,
            committed_seq: Some(80),
            write_keys: ["key1", "key2"].into_iter().map(Bytes::from).collect(),
        }],
        current_write_keys: vec!["key3", "key4"],
        current_started_seq: 100,
        expected_conflict: false,
    })]
    #[case::concurrent_write_same_key(CheckConflictTestCase {
        name: "concurrent_write_same_key",
        recent_committed_txns: vec![TransactionState {
            read_only: false,
            started_seq: 50,
            committed_seq: Some(150),
            write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
        }],
        current_write_keys: vec!["key1"],
        current_started_seq: 100,
        expected_conflict: true,
    })]
    #[case::multiple_committed_mixed_conflict(CheckConflictTestCase {
        name: "multiple_committed_mixed_conflict",
        recent_committed_txns: vec![
            TransactionState {
                read_only: false,
                started_seq: 30,
                committed_seq: Some(50),
                write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
            },
            TransactionState {
                read_only: false,
                started_seq: 80,
                committed_seq: Some(150),
                write_keys: ["key2"].into_iter().map(Bytes::from).collect(),
            },
        ],
        current_write_keys: vec!["key1", "key2"],
        current_started_seq: 100,
        expected_conflict: true,
    })]
    #[case::readonly_committed_no_conflict(CheckConflictTestCase {
        name: "readonly_committed_no_conflict",
        recent_committed_txns: vec![TransactionState {
            read_only: true,
            started_seq: 80,
            committed_seq: Some(150),
            write_keys: HashSet::new(),
        }],
        current_write_keys: vec!["key1"],
        current_started_seq: 100,
        expected_conflict: false,
    })]
    #[case::committed_before_current_started(CheckConflictTestCase {
        name: "committed_before_current_started",
        recent_committed_txns: vec![TransactionState {
            read_only: false,
            started_seq: 30,
            committed_seq: Some(50),
            write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
        }],
        current_write_keys: vec!["key1"],
        current_started_seq: 100,
        expected_conflict: false,
    })]
    #[case::exact_seq_boundary(CheckConflictTestCase {
        name: "exact_seq_boundary",
        recent_committed_txns: vec![TransactionState {
            read_only: false,
            started_seq: 100,
            committed_seq: Some(100),
            write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
        }],
        current_write_keys: vec!["key1"],
        current_started_seq: 100,
        expected_conflict: false,
    })]
    #[case::partial_key_overlap_conflict(CheckConflictTestCase {
        name: "partial_key_overlap_conflict",
        recent_committed_txns: vec![TransactionState {
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
    })]
    #[case::max_seq_values(CheckConflictTestCase {
        name: "max_seq_values",
        recent_committed_txns: vec![TransactionState {
            read_only: false,
            started_seq: u64::MAX - 1,
            committed_seq: Some(u64::MAX),
            write_keys: ["key1"].into_iter().map(Bytes::from).collect(),
        }],
        current_write_keys: vec!["key1"],
        current_started_seq: u64::MAX - 1,
        expected_conflict: true, // Should be true since committed_seq > started_seq
    })]
    fn test_check_conflict_table_driven(#[case] case: CheckConflictTestCase) {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Set up recent_committed_txns directly
        {
            let mut inner = txn_manager.inner.write();
            inner.recent_committed_txns = case.recent_committed_txns.into();
        }

        // Convert current transaction write keys
        let conflict_keys: HashSet<Bytes> = case
            .current_write_keys
            .into_iter()
            .map(Bytes::from)
            .collect();

        // Call the method under test
        let inner = txn_manager.inner.read();
        let has_conflict = inner.check_conflict(&conflict_keys, case.current_started_seq);

        // Verify result
        assert_eq!(
            has_conflict, case.expected_conflict,
            "Test case '{}' failed",
            case.name
        );
    }

    #[derive(Debug)]
    struct MinActiveSeqTestCase {
        name: &'static str,
        transactions: Vec<(u64, bool)>, // (seq_no, read_only)
        expected_min_seq: Option<u64>,
    }

    #[rstest]
    #[case::no_transactions_returns_none(MinActiveSeqTestCase {
        name: "no transactions returns none",
        transactions: vec![],
        expected_min_seq: None,
    })]
    #[case::single_transaction(MinActiveSeqTestCase {
        name: "single transaction",
        transactions: vec![(100, false)],
        expected_min_seq: Some(100),
    })]
    #[case::multiple_transactions_returns_minimum(MinActiveSeqTestCase {
        name: "multiple transactions returns minimum",
        transactions: vec![(200, false), (100, true), (150, false)],
        expected_min_seq: Some(100),
    })]
    #[case::mixed_readonly_and_write_transactions(MinActiveSeqTestCase {
        name: "mixed readonly and write transactions",
        transactions: vec![(50, true), (100, false)],
        expected_min_seq: Some(50),
    })]
    fn test_min_active_seq_table_driven(#[case] case: MinActiveSeqTestCase) {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create transactions according to the test case
        for (seq_no, read_only) in case.transactions {
            let _txn_id = txn_manager.new_txn(seq_no, read_only, None);
        }

        assert_eq!(
            txn_manager.min_active_seq(),
            case.expected_min_seq,
            "Test case '{}' failed",
            case.name
        );
    }

    struct TrackRecentCommittedTxnTestCase {
        name: &'static str,
        setup: Box<dyn Fn(&mut TransactionManager)>,
        expected_recent_committed_txn: Option<TransactionState>,
        expected_recent_committed_txns_len: usize,
        expected_active_txns_len: usize,
    }

    #[rstest]
    #[case::valid_id(TrackRecentCommittedTxnTestCase {
        name: "track committed transaction with valid id",
        setup: Box::new(|txn_manager| {
            // Create a transaction
            let txn_id = txn_manager.new_txn(100, false, None);

            // Create another active transaction to ensure recent_committed_txns is tracked
            let _other_txn = txn_manager.new_txn(200, false, None);

            // Track committed transaction
            let keys: HashSet<Bytes> = ["key1", "key2"].into_iter().map(Bytes::from).collect();
            txn_manager.track_recent_committed_txn(Some(&txn_id), &keys, 150);
        }),
        expected_recent_committed_txn: Some(TransactionState {
            started_seq: 100,
            committed_seq: Some(150),
            write_keys: ["key1", "key2"].into_iter().map(Bytes::from).collect(),
            read_only: false,
        }),
        expected_recent_committed_txns_len: 1,
        expected_active_txns_len: 1, // Only the other transaction remains
    })]
    #[case::nonexistent_id(TrackRecentCommittedTxnTestCase {
        name: "track committed transaction with nonexistent id",
        setup: Box::new(|txn_manager| {
            // Create an active transaction to ensure recent_committed_txns would be tracked
            let _active_txn = txn_manager.new_txn(100, false, None);

            // Try to track a non-existent transaction
            let fake_id = Uuid::new_v4();
            let keys: HashSet<Bytes> = ["key1"].into_iter().map(Bytes::from).collect();
            txn_manager.track_recent_committed_txn(Some(&fake_id), &keys, 150);
        }),
        expected_recent_committed_txn: None,
        expected_recent_committed_txns_len: 0,
        expected_active_txns_len: 1, // The active transaction remains
    })]
    #[case::no_tracking_when_only_readonly_active(TrackRecentCommittedTxnTestCase {
        name: "no tracking when only readonly transactions active",
        setup: Box::new(|txn_manager| {
            // Create a transaction and a readonly transaction
            let txn_id = txn_manager.new_txn(100, false, None);
            let _readonly_txn = txn_manager.new_txn(200, true, None);

            // Drop the transaction first so that after removal, only readonly remains
            txn_manager.drop_txn(&txn_id);

            // Now track committed transaction - this should not be tracked since no active writers
            let keys: HashSet<Bytes> = ["key1"].into_iter().map(Bytes::from).collect();
            txn_manager.track_recent_committed_txn(Some(&txn_id), &keys, 150);
        }),
        expected_recent_committed_txn: None,
        expected_recent_committed_txns_len: 0,
        expected_active_txns_len: 1, // Only the readonly transaction remains
    })]
    #[case::without_id_creates_record(TrackRecentCommittedTxnTestCase {
        name: "track without id creates record",
        setup: Box::new(|txn_manager| {
            // Create an active write transaction first to enable tracking
            let _active_txn = txn_manager.new_txn(50, false, None);
            // Track without transaction ID (non-transactional write)
            let keys: HashSet<Bytes> = ["key1", "key2"].into_iter().map(Bytes::from).collect();
            txn_manager.track_recent_committed_txn(None, &keys, 100);
        }),
        expected_recent_committed_txn: Some(TransactionState {
            started_seq: 100,
            committed_seq: Some(100),
            write_keys: ["key1", "key2"].into_iter().map(Bytes::from).collect(),
            read_only: false,
        }),
        expected_recent_committed_txns_len: 1,
        expected_active_txns_len: 1, // Now we have the active transaction remaining
    })]
    #[case::merges_conflict_keys(TrackRecentCommittedTxnTestCase {
        name: "merges conflict keys from existing transaction",
        setup: Box::new(|txn_manager| {
            // Create a transaction with some conflict keys already tracked
            let txn_id = txn_manager.new_txn(100, false, None);

            // Create another active transaction to ensure tracking happens
            let _other_txn = txn_manager.new_txn(200, false, None);

            // Add some keys to the transaction's conflict_keys before committing
            {
                let mut inner = txn_manager.inner.write();
                if let Some(txn_state) = inner.active_txns.get_mut(&txn_id) {
                    txn_state.track_write_keys(["existing_key"].into_iter().map(Bytes::from));
                }
            }
            // Track committed transaction with additional keys
            let additional_keys: HashSet<Bytes> = ["key1", "key2"].into_iter().map(Bytes::from).collect();
            txn_manager.track_recent_committed_txn(Some(&txn_id), &additional_keys, 150);
        }),
        expected_recent_committed_txn: Some(TransactionState {
            started_seq: 100,
            committed_seq: Some(150),
            write_keys: ["existing_key", "key1", "key2"].into_iter().map(Bytes::from).collect(),
            read_only: false,
        }),
        expected_recent_committed_txns_len: 1,
        expected_active_txns_len: 1, // Only the other transaction remains
    })]
    fn test_track_recent_committed_txn_table_driven(
        #[case] test_case: TrackRecentCommittedTxnTestCase,
    ) {
        let db_rand = Arc::new(DbRand::new(0));
        let mut txn_manager = TransactionManager::new(db_rand);

        // Run the setup
        (test_case.setup)(&mut txn_manager);

        // Verify the results
        let inner = txn_manager.inner.read();

        assert_eq!(
            inner.recent_committed_txns.len(),
            test_case.expected_recent_committed_txns_len,
            "Test case '{}' failed: expected {} recent committed transactions, got {}",
            test_case.name,
            test_case.expected_recent_committed_txns_len,
            inner.recent_committed_txns.len()
        );

        assert_eq!(
            inner.active_txns.len(),
            test_case.expected_active_txns_len,
            "Test case '{}' failed: expected {} active transactions, got {}",
            test_case.name,
            test_case.expected_active_txns_len,
            inner.active_txns.len()
        );

        if let Some(expected_txn) = test_case.expected_recent_committed_txn {
            assert!(
                !inner.recent_committed_txns.is_empty(),
                "Test case '{}' failed: expected a committed transaction but none found",
                test_case.name
            );

            let actual_txn = &inner.recent_committed_txns[0];
            assert_eq!(
                actual_txn.started_seq, expected_txn.started_seq,
                "Test case '{}' failed: expected started_seq {}, got {}",
                test_case.name, expected_txn.started_seq, actual_txn.started_seq
            );
            assert_eq!(
                actual_txn.committed_seq, expected_txn.committed_seq,
                "Test case '{}' failed: expected committed_seq {:?}, got {:?}",
                test_case.name, expected_txn.committed_seq, actual_txn.committed_seq
            );
            assert_eq!(
                actual_txn.write_keys, expected_txn.write_keys,
                "Test case '{}' failed: expected write_keys {:?}, got {:?}",
                test_case.name, expected_txn.write_keys, actual_txn.write_keys
            );
            assert_eq!(
                actual_txn.read_only, expected_txn.read_only,
                "Test case '{}' failed: expected read_only {}, got {}",
                test_case.name, expected_txn.read_only, actual_txn.read_only
            );
        }
    }

    #[test]
    fn test_recycle_recent_committed_txns_filters_by_min_seq() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create an active write transaction first to enable tracking
        let active_txn1 = txn_manager.new_txn(120, false, None);

        // Create some committed transactions first
        let keys1: HashSet<Bytes> = ["key1"].into_iter().map(Bytes::from).collect();
        let keys2: HashSet<Bytes> = ["key2"].into_iter().map(Bytes::from).collect();
        let keys3: HashSet<Bytes> = ["key3"].into_iter().map(Bytes::from).collect();

        // Add committed transactions with different committed_seq values
        txn_manager.track_recent_committed_txn(None, &keys1, 50); // committed_seq = 50
        txn_manager.track_recent_committed_txn(None, &keys2, 100); // committed_seq = 100
        txn_manager.track_recent_committed_txn(None, &keys3, 150); // committed_seq = 150

        // Verify we have all 3 committed transactions
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 3);

        // Trigger garbage collection by dropping the transaction
        // Since there will be no active non-readonly transactions after dropping,
        // all recent_committed_txns will be cleared
        txn_manager.drop_txn(&active_txn1);

        // All should be cleared since no active non-readonly transactions remain
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 0);
    }

    #[test]
    fn test_recycle_recent_committed_txns_clears_all_when_no_active_writers() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create a write transaction first to enable tracking
        let txn_id = txn_manager.new_txn(300, false, None);

        // Add some committed transactions
        let keys: HashSet<Bytes> = ["key1", "key2"].into_iter().map(Bytes::from).collect();
        txn_manager.track_recent_committed_txn(None, &keys, 100);
        txn_manager.track_recent_committed_txn(None, &keys, 200);

        // Verify they exist
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 2);

        // Drop the write transaction (this triggers recycle)
        txn_manager.drop_txn(&txn_id);

        // Since no active non-readonly transactions remain, should clear all
        assert!(txn_manager.inner.read().recent_committed_txns.is_empty());
    }

    #[test]
    fn test_recycle_recent_committed_txns_boundary_condition_equal_seq() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create active write transactions first to enable tracking
        let _active_txn1 = txn_manager.new_txn(100, false, None); // This sets min to 100
        let active_txn2 = txn_manager.new_txn(200, false, None); // Remove this later

        // Create committed transactions with seq values around the boundary
        let keys: HashSet<Bytes> = ["key1"].into_iter().map(Bytes::from).collect();
        txn_manager.track_recent_committed_txn(None, &keys, 99); // should be removed
        txn_manager.track_recent_committed_txn(None, &keys, 100); // should be kept (equal)
        txn_manager.track_recent_committed_txn(None, &keys, 101); // should be kept

        // Trigger garbage collection - this will keep min_conflict_check_seq = 100
        txn_manager.drop_txn(&active_txn2);

        // Should keep transactions with committed_seq >= 100
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 2);
        let committed_seqs: Vec<u64> = txn_manager
            .inner
            .read()
            .recent_committed_txns
            .iter()
            .map(|txn| txn.committed_seq.unwrap())
            .collect();
        assert!(committed_seqs.contains(&100));
        assert!(committed_seqs.contains(&101));
    }

    #[test]
    fn test_recycle_recent_committed_txns_handles_none_committed_seq() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Manually create a transaction state with None committed_seq (edge case)
        {
            let mut inner = txn_manager.inner.write();
            inner.recent_committed_txns.push_back(TransactionState {
                read_only: false,
                started_seq: 50,
                committed_seq: None, // This should not happen in practice but let's test
                write_keys: HashSet::new(),
            });
        }

        // Add a normal committed transaction
        let keys: HashSet<Bytes> = ["key1"].into_iter().map(Bytes::from).collect();
        txn_manager.track_recent_committed_txn(None, &keys, 100);

        // Create active transactions and trigger garbage collection
        let _active_txn1 = txn_manager.new_txn(150, false, None); // This sets min to 150
        let active_txn2 = txn_manager.new_txn(200, false, None);

        txn_manager.drop_txn(&active_txn2);

        // The transaction with None committed_seq should be kept (safety)
        // The transaction with committed_seq=100 should be removed since 100 < 150
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 1);
        assert_eq!(
            txn_manager.inner.read().recent_committed_txns[0].committed_seq,
            None
        );
    }

    // Integration tests for complete transaction lifecycles and complex interactions

    #[test]
    fn test_transaction_lifecycle_complete_flow() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Step 1: Create a transaction
        let txn_id = txn_manager.new_txn(100, false, None);
        assert_eq!(txn_manager.min_active_seq(), Some(100));

        // Step 2: Simulate conflict detection during transaction
        let write_keys: HashSet<Bytes> = ["key1", "key2"].into_iter().map(Bytes::from).collect();
        let has_conflict = txn_manager.check_conflict(&txn_id, &write_keys);
        assert!(!has_conflict); // No conflicts initially

        // Step 3: Create another transaction that will commit first
        let other_txn = txn_manager.new_txn(50, false, None);
        let other_keys: HashSet<Bytes> = ["key1", "key3"].into_iter().map(Bytes::from).collect();

        // Step 4: Commit the other transaction
        txn_manager.track_recent_committed_txn(Some(&other_txn), &other_keys, 120);

        // Step 5: Check for conflicts again - should detect conflict on key1
        let has_conflict = txn_manager.check_conflict(&txn_id, &write_keys);
        assert!(has_conflict); // Should conflict on key1

        // Step 6: Commit our transaction despite conflict (simulating retry logic)
        txn_manager.track_recent_committed_txn(Some(&txn_id), &write_keys, 150);

        // Step 7: Verify final state
        assert!(txn_manager.inner.read().active_txns.is_empty());
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 2);

        // Step 8: Verify min_active_seq is now None
        assert_eq!(txn_manager.min_active_seq(), None);
    }

    #[test]
    fn test_concurrent_transactions_conflict_detection() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create three concurrent transactions
        let txn1 = txn_manager.new_txn(100, false, None); // T1 starts at seq 100
        let txn2 = txn_manager.new_txn(110, false, None); // T2 starts at seq 110
        let txn3 = txn_manager.new_txn(120, false, None); // T3 starts at seq 120

        let keys_a: HashSet<Bytes> = ["keyA"].into_iter().map(Bytes::from).collect();
        let keys_b: HashSet<Bytes> = ["keyB"].into_iter().map(Bytes::from).collect();
        let keys_ab: HashSet<Bytes> = ["keyA", "keyB"].into_iter().map(Bytes::from).collect();

        // Initially no conflicts
        assert!(!txn_manager.check_conflict(&txn1, &keys_a));
        assert!(!txn_manager.check_conflict(&txn2, &keys_b));
        assert!(!txn_manager.check_conflict(&txn3, &keys_ab));

        // T1 commits at seq 130, writing keyA
        txn_manager.track_recent_committed_txn(Some(&txn1), &keys_a, 130);

        // T2 should not conflict (writes different key)
        assert!(!txn_manager.check_conflict(&txn2, &keys_b));

        // T3 should conflict (writes keyA, started at 120 < 130)
        assert!(txn_manager.check_conflict(&txn3, &keys_ab));

        // T2 commits at seq 140, writing keyB
        txn_manager.track_recent_committed_txn(Some(&txn2), &keys_b, 140);

        // T3 should still conflict (now conflicts on both keyA and keyB)
        assert!(txn_manager.check_conflict(&txn3, &keys_ab));

        // Verify state
        assert_eq!(txn_manager.inner.read().active_txns.len(), 1); // Only T3 remains active
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 2); // T1 and T2 committed
    }

    #[test]
    fn test_garbage_collection_timing_with_multiple_operations() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create active transactions first
        let long_txn = txn_manager.new_txn(100, false, None); // Long-running transaction
        let short_txn1 = txn_manager.new_txn(150, false, None); // Short transaction 1
        let short_txn2 = txn_manager.new_txn(200, false, None); // Short transaction 2

        // Create some old committed transactions (now they will be tracked since we have active txns)
        let keys1: HashSet<Bytes> = ["old_key1"].into_iter().map(Bytes::from).collect();
        let keys2: HashSet<Bytes> = ["old_key2"].into_iter().map(Bytes::from).collect();
        txn_manager.track_recent_committed_txn(None, &keys1, 50);
        txn_manager.track_recent_committed_txn(None, &keys2, 60);

        // Verify old committed transactions are kept (min_conflict_check_seq = 100)
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 2);

        // Commit short transactions
        let short_keys: HashSet<Bytes> = ["short_key"].into_iter().map(Bytes::from).collect();
        txn_manager.track_recent_committed_txn(Some(&short_txn1), &short_keys, 180);
        txn_manager.track_recent_committed_txn(Some(&short_txn2), &short_keys, 220);

        // Old transactions should still be kept (long_txn still active with seq 100)
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 4); // 2 old + 2 new

        // Drop the long-running transaction - this should trigger aggressive cleanup
        txn_manager.drop_txn(&long_txn);

        // All committed transactions should be cleared (no active writers remain)
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 0);
        assert!(txn_manager.inner.read().active_txns.is_empty());
    }

    #[test]
    fn test_readonly_vs_write_transaction_interactions() {
        let db_rand = Arc::new(DbRand::new(0));
        let txn_manager = TransactionManager::new(db_rand);

        // Create mixed read-only and write transactions
        let readonly_txn1 = txn_manager.new_txn(50, true, None); // Read-only at seq 50
        let write_txn1 = txn_manager.new_txn(100, false, None); // Write at seq 100
        let _readonly_txn2 = txn_manager.new_txn(150, true, None); // Read-only at seq 150
        let write_txn2 = txn_manager.new_txn(200, false, None); // Write at seq 200

        // min_active_seq should include all transactions (read-only and write)
        assert_eq!(txn_manager.min_active_seq(), Some(50));

        // min_conflict_check_seq should only consider write transactions
        assert_eq!(txn_manager.inner.read().min_conflict_check_seq(), Some(100));

        // Commit a write transaction
        let keys: HashSet<Bytes> = ["test_key"].into_iter().map(Bytes::from).collect();
        txn_manager.track_recent_committed_txn(Some(&write_txn1), &keys, 120);

        // Should track committed transaction because write_txn2 is still active
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 1);

        // Drop one read-only transaction - should not affect conflict tracking
        // The key insight: dropping a readonly transaction still calls recycle_recent_committed_txns
        // But since write_txn2 is still active, the min_conflict_check_seq is still 200
        // The committed transaction has committed_seq=120, which is < 200, so it gets removed!
        txn_manager.drop_txn(&readonly_txn1);

        // The committed transaction should be removed because 120 < 200 (min_conflict_check_seq)
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 0); // Removed due to garbage collection
        assert_eq!(txn_manager.inner.read().min_conflict_check_seq(), Some(200)); // Only write_txn2 remains

        // Drop the remaining write transaction
        txn_manager.drop_txn(&write_txn2);

        // Should clear committed transactions (no active write transactions)
        assert_eq!(txn_manager.inner.read().recent_committed_txns.len(), 0);
        assert_eq!(txn_manager.inner.read().min_conflict_check_seq(), None);

        // Read-only transaction should still be active and affect min_active_seq
        assert_eq!(txn_manager.min_active_seq(), Some(150));
    }

    // Property-based tests using proptest for invariant verification
    use proptest::collection::vec;
    use proptest::prelude::*;
    use rand::Rng;

    #[derive(Debug, Clone)]
    enum TxnOperation {
        Create {
            seq: u64,
            read_only: bool,
            txn_id: Uuid,
        },
        Drop {
            txn_id: Uuid,
        },
        Commit {
            txn_id: Option<Uuid>,
            keys: Vec<String>,
            committed_seq: u64,
        },
    }

    // Generator for transaction operations
    fn txn_operation_strategy() -> impl Strategy<Value = TxnOperation> {
        let tracked_txn_ids = Arc::new(Mutex::new(Vec::<Uuid>::new()));
        let tracked_txn_ids_for_drop = Arc::clone(&tracked_txn_ids);
        let tracked_txn_ids_for_commit = Arc::clone(&tracked_txn_ids);

        let sample_txn_id = move |tracked_txn_ids: Arc<Mutex<Vec<Uuid>>>| {
            let ids = tracked_txn_ids.lock();
            if ids.is_empty() {
                Uuid::new_v4()
            } else {
                // Pick a random element from the set
                let idx = rand::rng().random_range(0..ids.len());
                ids[idx]
            }
        };

        prop_oneof![
            // Create transaction with increasing seq numbers
            (100u64..10000u64, any::<bool>()).prop_map(move |(seq, read_only)| {
                let txn_id = Uuid::new_v4();
                tracked_txn_ids.lock().push(txn_id);
                TxnOperation::Create {
                    seq,
                    read_only,
                    txn_id,
                }
            }),
            // Drop transaction - sample from tracked_txn_ids_for_drop
            (0..1000u32).prop_map(move |_| {
                TxnOperation::Drop {
                    txn_id: sample_txn_id(tracked_txn_ids_for_drop.clone()),
                }
            }),
            // commit - can be with or without txn_id
            (
                prop::option::of(
                    (0..100u32)
                        .prop_map(move |_| sample_txn_id(tracked_txn_ids_for_commit.clone()))
                ),
                vec("key[0-9]+", 1..5),
                1000u64..20000u64
            )
                .prop_map(|(txn_id, keys, seq)| TxnOperation::Commit {
                    txn_id,
                    keys,
                    committed_seq: seq
                })
        ]
    }

    // Strategy for sequences of operations
    fn operation_sequence_strategy() -> impl Strategy<Value = Vec<TxnOperation>> {
        vec(txn_operation_strategy(), 5..100)
    }

    // Helper struct to track operation execution state
    #[derive(Debug)]
    struct ExecutionState {
        seq_counter: u64,
    }

    impl ExecutionState {
        fn new() -> Self {
            Self { seq_counter: 100 }
        }

        fn execute_operation(&mut self, manager: &TransactionManager, mut op: TxnOperation) {
            match &mut op {
                TxnOperation::Create {
                    seq,
                    read_only,
                    txn_id,
                } => {
                    *seq = self.seq_counter;
                    self.seq_counter += 10;
                    manager.new_txn(*seq, *read_only, Some(*txn_id));
                }
                TxnOperation::Drop { txn_id } => {
                    manager.drop_txn(txn_id);
                }
                TxnOperation::Commit {
                    txn_id,
                    keys,
                    committed_seq,
                } => {
                    let key_set = keys
                        .iter()
                        .map(|k: &String| Bytes::from(k.clone()))
                        .collect();
                    match txn_id {
                        None => {
                            *committed_seq = self.seq_counter;
                            self.seq_counter += 10;
                            manager.track_recent_committed_txn(None, &key_set, *committed_seq);
                        }
                        Some(txn_id) => {
                            // only record committed txn if there is no conflict
                            if manager.check_conflict(txn_id, &key_set) {
                                manager.track_recent_committed_txn(
                                    Some(txn_id),
                                    &key_set,
                                    *committed_seq,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    proptest! {
        #[test]
        fn prop_inv_disjoint_active_and_committed_sets(ops in operation_sequence_strategy()) {
            let db_rand = Arc::new(DbRand::new(0));
            let txn_manager = TransactionManager::new(db_rand);
            let mut exec_state = ExecutionState::new();

            for op in ops {
                exec_state.execute_operation(&txn_manager, op);

                // Verify that active_txns.keys() and committed transaction IDs are disjoint
                let inner = txn_manager.inner.read();
                let active_ids: HashSet<Uuid> = inner.active_txns.keys().cloned().collect();

                for active_id in &active_ids {
                    // Each active transaction should not appear in recent_committed_txns
                    // Since we don't store the original ID in committed txns, this is automatically true
                    prop_assert!(inner.active_txns.contains_key(active_id),
                        "Active transaction {:?} should be in active_txns", active_id);
                }

                // The real check: a transaction cannot be both active and committed simultaneously
                // This is enforced by track_recent_committed_txn removing from active_txns
                prop_assert!(
                    inner.recent_committed_txns.iter().all(|_committed_txn| true),
                    "No committed transaction should have an active counterpart"
                );
            }
        }

        #[test]
        fn prop_no_active_non_readonly_txn_and_recent_committed_txns(ops in operation_sequence_strategy()) {
            let db_rand = Arc::new(DbRand::new(0));
            let txn_manager = TransactionManager::new(db_rand);
            let mut exec_state = ExecutionState::new();

            for op in ops {
                exec_state.execute_operation(&txn_manager, op);

                let inner = txn_manager.inner.read();
                // when there's no active non-readonly transactions, recent_committed_txns should be empty
                if !inner.active_txns.values().any(|t| !t.read_only) {
                    prop_assert!(inner.recent_committed_txns.is_empty(), "If there's no active non-readonly transactions, recent_committed_txns should be empty");
                }
            }
        }

        #[test]
        fn prop_inv_min_active_seq_correctness(ops in operation_sequence_strategy()) {
            let db_rand = Arc::new(DbRand::new(0));
            let txn_manager = TransactionManager::new(db_rand);
            let mut exec_state = ExecutionState::new();

            for op in ops {
                exec_state.execute_operation(&txn_manager, op);

                // Verify min_active_seq correctness
                let min_active_seq = txn_manager.min_active_seq();
                let inner = txn_manager.inner.read();

                if inner.active_txns.is_empty() {
                    prop_assert!(
                        min_active_seq.is_none(),
                        "min_active_seq should be None when no active transactions exist"
                    );
                } else {
                    let expected_min = inner.active_txns.values()
                        .map(|txn| txn.started_seq)
                        .min();

                    prop_assert_eq!(
                        min_active_seq,
                        expected_min,
                        "min_active_seq should equal the minimum started_seq of all active transactions"
                    );
                }
            }
        }

        #[test]
        fn prop_inv_garbage_collection_correctness(ops in operation_sequence_strategy()) {
            let db_rand = Arc::new(DbRand::new(0));
            let txn_manager = TransactionManager::new(db_rand);
            let mut exec_state = ExecutionState::new();

            for op in ops {
                exec_state.execute_operation(&txn_manager, op);

                // Garbage collection correctness invariant
                // recent_committed_txns.is_empty() OR (there exists an active transaction in active_txns.values() that is not read-only)
                let inner = txn_manager.inner.read();

                if !inner.recent_committed_txns.is_empty() {
                    // If the recent_committed_txns queue is not empty, there must be at least one active write transaction
                    let has_active_writer = inner.active_txns.values().any(|txn| !txn.read_only);
                    prop_assert!(
                        has_active_writer,
                        "invariant violation: recent_committed_txns is not empty but there are no active write transactions. \
                         Active transaction IDs: {:?}, Number of recent committed: {}",
                        inner.active_txns.keys().collect::<Vec<_>>(),
                        inner.recent_committed_txns.len()
                    );
                }
            }
        }
    }
}
