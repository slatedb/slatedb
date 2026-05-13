//! # Batch Write
//!
//! This module adds batch write functionality to DbInner. Prior to this feature,
//! writes were performed directly in DbInner's `put_with_options` and
//! `delete_with_options` methods. For each operation, a lock was acquired on the
//! db_state to mutate the WAL or memtable. This worked fine for single writes,
//! but for batch writes, which take longer, it could create contention on the lock
//! because. This is dangerous in an async runtime because it can block the
//! threads, leading to starvation.
//!
//! This module spawns a separate task to handle batch writes. The task receives
//! a `WriteBatchMsg``, which contains a `WriteBatchRequest``. The `WriteBatchRequest`
//! contains a `WriteBatch` containing Put/Delete operations and a `oneshot::Sender`.
//! The `Sender` is used to send the table that the batch was written to back to the
//! caller so the caller can `.await` the result. The result is that callers safely
//! `.await` on their writes rather than holding a lock on the db_state.
//!
//! Centralizing the writes in a single event loop also provides a single location to
//! assign sequence numbers when we implement MVCC.
//!
//! [Pebble](https://github.com/cockroachdb/pebble) has a similar design and
//! [a good write-up](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md#commit-pipeline)
//! describing its benefits.
//!
//! _Note: The `write_batch` loop still holds a lock on the db_state. There can still
//! be contention between `get`s, which holds a lock, and the write loop._

use async_trait::async_trait;
use fail_parallel::fail_point;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::warn;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

use std::collections::BTreeSet;

use bytes::Bytes;

use crate::config::WriteOptions;
use crate::dispatcher::MessageHandler;
use crate::mem_table::KVTable;
use crate::types::RowEntry;
use crate::utils::WatchableOnceCellReader;
use crate::{batch::WriteBatch, db::DbInner, db::WriteHandle, error::SlateDBError};
use slatedb_common::clock::SystemClock;

pub(crate) const WRITE_BATCH_TASK_NAME: &str = "writer";

pub(crate) type WriteBatchResult = Result<
    (
        WriteHandle,
        WatchableOnceCellReader<Result<(), SlateDBError>>,
    ),
    SlateDBError,
>;

pub(crate) struct WriteBatchMessage {
    pub(crate) batch: WriteBatch,
    pub(crate) options: WriteOptions,
    pub(crate) done: tokio::sync::oneshot::Sender<WriteBatchResult>,
}

impl std::fmt::Debug for WriteBatchMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let WriteBatchMessage { batch, options, .. } = self;
        f.debug_struct("WriteBatch")
            .field("batch", batch)
            .field("options", options)
            .finish()
    }
}

pub(crate) struct WriteBatchEventHandler {
    db_inner: Arc<DbInner>,
    is_first_write: bool,
}

impl WriteBatchEventHandler {
    pub(crate) fn new(db_inner: Arc<DbInner>) -> Self {
        Self {
            db_inner,
            is_first_write: true,
        }
    }
}

#[async_trait]
impl MessageHandler<WriteBatchMessage> for WriteBatchEventHandler {
    async fn handle(&mut self, message: WriteBatchMessage) -> Result<(), SlateDBError> {
        let WriteBatchMessage {
            batch,
            options,
            done,
        } = message;
        let result = self.db_inner.write_batch(batch, &options).await;
        // if this is the first write and the WAL is disabled, make sure users are flushing
        // their memtables in a timely manner.
        if self.is_first_write && !self.db_inner.wal_enabled && options.await_durable {
            if let Ok((_, this_watcher)) = &result {
                let this_watcher = this_watcher.clone();
                let this_clock = self.db_inner.system_clock.clone();
                tokio::spawn(async move {
                    monitor_first_write(this_watcher, this_clock).await;
                });
            }
        }
        self.is_first_write = false;
        _ = done.send(result);
        Ok(())
    }

    async fn cleanup(
        &mut self,
        mut messages: BoxStream<'async_trait, WriteBatchMessage>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let error = result.clone().err().unwrap_or(SlateDBError::Closed);
        while let Some(msg) = messages.next().await {
            let _ = msg.done.send(Err(error.clone()));
        }
        Ok(())
    }
}

impl DbInner {
    #[allow(clippy::panic)]
    #[instrument(level = "trace", skip_all, fields(batch_size = batch.ops.len()))]
    async fn write_batch(&self, batch: WriteBatch, options: &WriteOptions) -> WriteBatchResult {
        let _options = options;
        #[cfg(not(dst))]
        let now = self.mono_clock.now().await?;
        #[cfg(dst)]
        // Force the current timestamp for DST operations. See #719 for details.
        let now = options.now;
        // If the user supplied a sequence number, validate that it's strictly greater
        // than the current max and advance the oracle. No CAS loop is needed here because
        // write_batch is always called from a single-writer event loop.
        let commit_seq = if options.seqnum > 0 {
            let current = self.oracle.last_seq();
            if options.seqnum <= current {
                return Err(SlateDBError::InvalidSequenceNumber {
                    provided: options.seqnum,
                    current,
                });
            }
            self.oracle.advance_last_seq(options.seqnum);
            options.seqnum
        } else {
            self.oracle.next_seq()
        };

        // Check for transaction conflicts before proceeding with the write batch
        // if this batch is part of a transaction.
        if let Some(txn_id) = batch.txn_id.as_ref() {
            if self.txn_manager.check_has_conflict(txn_id) {
                return Err(SlateDBError::TransactionConflict);
            }
        }

        // Count batch-local merge folding on the flush path so DB-side merge
        // resolution uses one metric for both write batches and memtable flushes.
        let (entries, touched_segments) = batch
            .extract_entries(
                commit_seq,
                now,
                self.settings.default_ttl,
                self.flush_merge_operator.clone(),
                self.segment_extractor.as_deref(),
            )
            .await?;

        // RFC-0024 route-consistency: when a segment extractor is
        // configured, every write must extract a prefix that does
        // not nest with the current segment set. Runs before the
        // WAL append so a rejected batch produces no durable side
        // effects.
        self.validate_segment_antichain(&touched_segments)?;

        let durable_watcher = if self.wal_enabled {
            // WAL entries must be appended to the wal buffer atomically. Otherwise,
            // the WAL buffer might flush the entries in the middle of the batch, which
            // would violate the guarantee that batches are written atomically. We do
            // this by appending the entire entry batch in a single call to the WAL buffer,
            // which holds a write lock during the append.
            let wal_watcher = self.wal_buffer.append(&entries)?;
            self.wal_buffer.maybe_trigger_flush()?;
            // TODO: handle sync here, if sync is enabled, we can call `flush` here. let's put this
            // in another Pull Request.
            self.write_entries_to_memtable(entries, touched_segments);
            wal_watcher
        } else {
            // if WAL is disabled, we just write the entries to memtable.
            self.write_entries_to_memtable(entries, touched_segments)
        };

        // update the last_applied_seq to wal buffer. if a chunk of WAL entries are applied to the memtable
        // and flushed to the remote storage, WAL buffer manager will recycle these WAL entries.
        self.wal_buffer.track_last_applied_seq(commit_seq);

        // insert a fail point to make it easier to test the case where the last_committed_seq is not updated.
        // this is useful for testing the case where the reader is not able to see the writes.
        fail_point!(
            Arc::clone(&self.fp_registry),
            "write-batch-pre-commit",
            |_| { Err(SlateDBError::from(std::io::Error::other("oops"))) }
        );

        // track the recent committed txn for conflict check. if txn_id is not supplied,
        // we still consider this as an transaction commit.
        if let Some(txn_id) = &batch.txn_id {
            self.txn_manager
                .track_recent_committed_txn(txn_id, commit_seq);
        } else {
            let write_keys = batch.keys();
            self.txn_manager
                .track_recent_committed_write_batch(&write_keys, commit_seq);
        }

        // insert a fail point to make it easier to test the case where the transaction is committed but
        // but remaining work hasn't been done. this is useful for testing that transaction commits and
        // commited seqnums get updated in lock-step. See #1301 for details.
        fail_point!(
            Arc::clone(&self.fp_registry),
            "write-batch-post-commit",
            |_| { Err(SlateDBError::from(std::io::Error::other("oops"))) }
        );

        // record the memtable sequence in the memtable's sequence tracker.
        self.record_memtable_sequence(commit_seq);

        // maybe freeze the memtable.
        self.maybe_freeze_current_memtable()?;

        let write_handle = WriteHandle::new(commit_seq, now);

        Ok((write_handle, durable_watcher))
    }

    /// RFC-0024 route-consistency check. Verifies that `batch_prefixes`,
    /// added to the union of every prefix the writer already knows
    /// about (manifest segments, in-flight imms, the active memtable),
    /// still forms an antichain — no prefix is a proper prefix of
    /// another.
    ///
    /// Optimized for the common case where the batch's prefixes are
    /// already recorded on the active memtable (writes concentrate on
    /// a small set of active segments). That case short-circuits
    /// without consulting older in-memory sources or the manifest.
    /// Only truly novel prefixes pay the manifest binary-search cost.
    ///
    /// Read-only and runs before any durable side-effect, so a
    /// rejection leaves no trace.
    pub(crate) fn validate_segment_antichain(
        &self,
        batch_prefixes: &BTreeSet<Bytes>,
    ) -> Result<(), SlateDBError> {
        if batch_prefixes.is_empty() {
            return Ok(());
        }

        check_batch_antichain(batch_prefixes)?;

        let mut remaining = batch_prefixes.clone();
        let guard = self.state.read();
        let memtable = guard.memtable().table();
        check_segment_prefix_antichain(&mut remaining, memtable)?;
        if remaining.is_empty() {
            return Ok(());
        }
        let cow = guard.state();
        for imm in cow.imm_memtable.iter() {
            check_segment_prefix_antichain(&mut remaining, &imm.table())?;
            if remaining.is_empty() {
                return Ok(());
            }
        }
        let core = cow.core();
        for c in &remaining {
            core.check_segment_prefix_antichain(c.as_ref())?;
        }
        Ok(())
    }

    /// Write entries to the currently active memtable and record
    /// the batch's touched-segment prefixes on it. Returns a durable
    /// watcher for the memtable. When no extractor is configured,
    /// `touched_segments` is empty and recording is a no-op.
    fn write_entries_to_memtable(
        &self,
        entries: Vec<RowEntry>,
        touched_segments: BTreeSet<Bytes>,
    ) -> WatchableOnceCellReader<Result<(), SlateDBError>> {
        let guard = self.state.read();
        let memtable = guard.memtable();
        memtable.record_touched_segments(touched_segments);
        entries.into_iter().for_each(|entry| memtable.put(entry));
        memtable.table().durable_watcher()
    }

    fn record_memtable_sequence(&self, seq: u64) {
        let ts = self.system_clock.now();
        let guard = self.state.read();
        guard.memtable().record_sequence(seq, ts);
    }
}

/// Verify that `prefixes` is itself an antichain — no element is a
/// proper prefix of another. The set is sorted (BTreeSet), so it
/// suffices to check every adjacent pair: in a sorted antichain any
/// nesting must surface at an adjacent boundary.
fn check_batch_antichain(prefixes: &BTreeSet<Bytes>) -> Result<(), SlateDBError> {
    let mut prev: Option<&Bytes> = None;
    for cur in prefixes {
        if let Some(p) = prev {
            if cur.starts_with(p.as_ref()) {
                return Err(SlateDBError::InvalidSegmentPrefix {
                    prefix: cur.clone(),
                    conflict: p.clone(),
                });
            }
        }
        prev = Some(cur);
    }
    Ok(())
}

/// Run `check` against every prefix in `remaining` and remove those it
/// reports as exact matches. Stops on the first error from `check`.
fn check_segment_prefix_antichain(
    remaining: &mut BTreeSet<Bytes>,
    table: &KVTable,
) -> Result<(), SlateDBError> {
    let mut to_remove: Vec<Bytes> = Vec::new();
    for c in remaining.iter() {
        if table.ensure_valid_segment(c)? {
            to_remove.push(c.clone());
        }
    }
    for k in to_remove {
        remaining.remove(&k);
    }
    Ok(())
}

async fn monitor_first_write(
    mut watcher: WatchableOnceCellReader<Result<(), SlateDBError>>,
    system_clock: Arc<dyn SystemClock>,
) {
    tokio::select! {
        _ = watcher.await_value() => {}
        _ = system_clock.sleep(Duration::from_secs(5)) => {
            warn!("First write not durable after 5 seconds and WAL is disabled. \
            SlateDB does not automatically flush memtables until `l0_sst_size_bytes` \
            is reached. If writer is single threaded or has low throughput, the \
            applications must call `flush` to ensure durability in a timely manner.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::memory::InMemory;
    use crate::Db;

    #[tokio::test]
    async fn test_is_first_write_set_false_after_first_write() {
        let object_store = Arc::new(InMemory::new());
        let db = Db::open(
            "/tmp/test_is_first_write_set_false_after_first_write",
            object_store,
        )
        .await
        .unwrap();

        let mut handler = WriteBatchEventHandler::new(db.inner.clone());
        assert!(handler.is_first_write);

        let mut batch = WriteBatch::new();
        batch.put(b"key", b"value");

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        handler
            .handle(WriteBatchMessage {
                batch,
                options: WriteOptions::default(),
                done: done_tx,
            })
            .await
            .unwrap();

        let result = done_rx.await.unwrap();
        assert!(result.is_ok());
        assert!(!handler.is_first_write);
    }

    #[tokio::test]
    async fn test_user_defined_seqnum() {
        let object_store = Arc::new(InMemory::new());
        let db = Db::open("/tmp/test_user_defined_seqnum", object_store)
            .await
            .unwrap();

        let mut handler = WriteBatchEventHandler::new(db.inner.clone());

        // Write with a user-defined seqnum
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        handler
            .handle(WriteBatchMessage {
                batch,
                options: WriteOptions {
                    seqnum: 42,
                    ..Default::default()
                },
                done: done_tx,
            })
            .await
            .unwrap();
        let (write_handle, _) = done_rx.await.unwrap().unwrap();
        assert_eq!(write_handle.seqnum(), 42);

        // Write without a seqnum and verify auto-assigned is > 42
        let mut batch = WriteBatch::new();
        batch.put(b"key2", b"value2");
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        handler
            .handle(WriteBatchMessage {
                batch,
                options: WriteOptions::default(),
                done: done_tx,
            })
            .await
            .unwrap();
        let (write_handle, _) = done_rx.await.unwrap().unwrap();
        assert!(write_handle.seqnum() > 42);
    }

    #[tokio::test]
    async fn test_user_defined_seqnum_rejects_lower_value() {
        let object_store = Arc::new(InMemory::new());
        let db = Db::open(
            "/tmp/test_user_defined_seqnum_rejects_lower_value",
            object_store,
        )
        .await
        .unwrap();

        let mut handler = WriteBatchEventHandler::new(db.inner.clone());

        // First, do a normal write to advance the oracle
        let mut batch = WriteBatch::new();
        batch.put(b"key1", b"value1");
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        handler
            .handle(WriteBatchMessage {
                batch,
                options: WriteOptions::default(),
                done: done_tx,
            })
            .await
            .unwrap();
        let (write_handle, _) = done_rx.await.unwrap().unwrap();
        let first_seq = write_handle.seqnum();

        // Try to write with a seqnum <= the current max
        let mut batch = WriteBatch::new();
        batch.put(b"key2", b"value2");
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        handler
            .handle(WriteBatchMessage {
                batch,
                options: WriteOptions {
                    seqnum: 1,
                    ..Default::default()
                },
                done: done_tx,
            })
            .await
            .unwrap();
        let result = done_rx.await.unwrap();
        assert!(matches!(
            result,
            Err(SlateDBError::InvalidSequenceNumber {
                provided: 1,
                current,
            }) if current == first_seq
        ));
    }

    fn batch(prefixes: &[&[u8]]) -> BTreeSet<Bytes> {
        prefixes.iter().map(|p| Bytes::copy_from_slice(p)).collect()
    }

    fn assert_invalid_segment_prefix(err: SlateDBError, prefix: &[u8], conflict: &[u8]) {
        match err {
            SlateDBError::InvalidSegmentPrefix {
                prefix: p,
                conflict: c,
            } => {
                assert_eq!(p.as_ref(), prefix);
                assert_eq!(c.as_ref(), conflict);
            }
            other => panic!("expected InvalidSegmentPrefix, got {other:?}"),
        }
    }

    #[test]
    fn check_batch_antichain_accepts_empty_set() {
        check_batch_antichain(&BTreeSet::new()).unwrap();
    }

    #[test]
    fn check_batch_antichain_accepts_singleton() {
        check_batch_antichain(&batch(&[b"abc"])).unwrap();
    }

    #[test]
    fn check_batch_antichain_accepts_disjoint_prefixes() {
        check_batch_antichain(&batch(&[b"aaa", b"bbb", b"ccc"])).unwrap();
    }

    #[test]
    fn check_batch_antichain_rejects_ancestor_descendant_pair() {
        // Sorted: ["abc", "abcd"]. Adjacent walk catches abcd extends abc.
        let err = check_batch_antichain(&batch(&[b"abc", b"abcd"])).unwrap_err();
        assert_invalid_segment_prefix(err, b"abcd", b"abc");
    }

    #[test]
    fn check_batch_antichain_rejects_when_nesting_is_not_at_input_adjacency() {
        // Sorted: ["abc", "abcd", "z"]. Adjacent (abc, abcd) catches it
        // even though the original input order placed "z" between them.
        let err = check_batch_antichain(&batch(&[b"abc", b"z", b"abcd"])).unwrap_err();
        assert_invalid_segment_prefix(err, b"abcd", b"abc");
    }

    /// Helper: open a fresh DB with the given segment extractor.
    async fn open_db_with_extractor(
        path: &str,
        extractor: Arc<dyn crate::prefix_extractor::PrefixExtractor>,
    ) -> Db {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Db::builder(path, object_store)
            .with_segment_extractor(extractor)
            .build()
            .await
            .unwrap()
    }

    /// Sanity check: writes against an extractor-configured DB succeed and
    /// land in the memtable when every key produces a non-nesting prefix.
    /// Three sibling 3-byte prefixes in a single batch is the typical
    /// happy path — the unsegmented branch (legacy) should match.
    #[tokio::test]
    async fn write_with_extractor_accepts_disjoint_prefixes_in_one_batch() {
        let db = open_db_with_extractor(
            "/tmp/test_write_disjoint_prefixes",
            Arc::new(crate::test_utils::FixedThreeBytePrefixExtractor),
        )
        .await;

        let mut batch = WriteBatch::new();
        batch.put(b"aaa-1", b"v1");
        batch.put(b"bbb-1", b"v2");
        batch.put(b"ccc-1", b"v3");
        db.write(batch).await.unwrap();

        // Round-trip through the read path confirms the entries actually
        // landed somewhere.
        for (k, v) in [
            (&b"aaa-1"[..], &b"v1"[..]),
            (&b"bbb-1"[..], &b"v2"[..]),
            (&b"ccc-1"[..], &b"v3"[..]),
        ] {
            let got = db.get(k).await.unwrap().unwrap();
            assert_eq!(got.as_ref(), v);
        }
        db.close().await.unwrap();
    }

    /// A batch whose extracted prefixes nest with each other is rejected
    /// up front. The deliberately non-conforming test extractor returns
    /// `Some(2)` for `"ab*"` and `Some(3)` for `"abc*"`, so a batch with
    /// both yields prefixes `"ab"` and `"abc"` — a strict nest.
    #[tokio::test]
    async fn write_rejects_intra_batch_nesting_prefixes() {
        let db = open_db_with_extractor(
            "/tmp/test_write_intra_batch_nesting",
            Arc::new(crate::test_utils::NonAntichainTestPrefixExtractor),
        )
        .await;

        let mut batch = WriteBatch::new();
        batch.put(b"abc-x", b"v1"); // prefix "abc"
        batch.put(b"ab-y", b"v2"); // prefix "ab" — nests with "abc"
        let err = db.write(batch).await.unwrap_err();
        assert!(matches!(err.kind(), crate::error::ErrorKind::Invalid));
        // Nothing should be visible — the rejected batch left no trace.
        assert!(db.get(b"abc-x").await.unwrap().is_none());
        assert!(db.get(b"ab-y").await.unwrap().is_none());
        db.close().await.unwrap();
    }

    /// A new write is rejected when its extracted prefix would nest with a
    /// segment already present in the manifest. The first write succeeds
    /// and creates segment `"abc"`; the second tries to introduce `"ab"`
    /// (a strict ancestor) and is rejected.
    #[tokio::test]
    async fn write_rejects_when_new_prefix_nests_existing_segment() {
        let db = open_db_with_extractor(
            "/tmp/test_write_nests_existing_segment",
            Arc::new(crate::test_utils::NonAntichainTestPrefixExtractor),
        )
        .await;

        let mut batch = WriteBatch::new();
        batch.put(b"abc-1", b"v1"); // prefix "abc"
        db.write(batch).await.unwrap();
        // Force memtable → L0 → manifest update so the new segment is
        // visible in the next state read. The default `flush()` only
        // flushes the WAL when WAL is enabled.
        db.flush_with_options(crate::config::FlushOptions {
            flush_type: crate::config::FlushType::MemTable,
        })
        .await
        .unwrap();

        // Sanity: segment "abc" is now in the manifest.
        {
            let guard = db.inner.state.read();
            let cow = guard.state();
            let prefixes: Vec<&[u8]> = cow
                .core()
                .segments
                .iter()
                .map(|s| s.prefix.as_ref())
                .collect();
            assert!(
                prefixes.iter().any(|p| *p == &b"abc"[..]),
                "expected segment 'abc' to be persisted; got {:?}",
                prefixes
            );
        }

        // Second batch routes to "ab", which is a strict prefix of "abc".
        let mut batch = WriteBatch::new();
        batch.put(b"ab-1", b"v2");
        let err = db.write(batch).await.unwrap_err();
        assert!(matches!(err.kind(), crate::error::ErrorKind::Invalid));
        assert!(db.get(b"ab-1").await.unwrap().is_none());
        db.close().await.unwrap();
    }

    /// A new write is rejected when its prefix nests with one already
    /// recorded on the active memtable but not yet flushed. Pins down
    /// the per-source check: removing it would let the manifest-only
    /// fallback miss this conflict because the conflicting prefix
    /// has never been persisted.
    #[tokio::test]
    async fn write_rejects_when_new_prefix_nests_active_memtable_prefix() {
        let db = open_db_with_extractor(
            "/tmp/test_write_nests_memtable_prefix",
            Arc::new(crate::test_utils::NonAntichainTestPrefixExtractor),
        )
        .await;

        // First write lands in the active memtable, recording prefix "abc".
        // No flush — manifest still has no segments.
        let mut first = WriteBatch::new();
        first.put(b"abc-1", b"v1");
        db.write(first).await.unwrap();
        {
            let guard = db.inner.state.read();
            assert!(
                guard.state().core().segments.is_empty(),
                "manifest must not yet contain any segment for this test to be meaningful"
            );
        }

        // Second write routes to "ab" — a strict prefix of "abc" sitting
        // on the active memtable. Must be rejected.
        let mut second = WriteBatch::new();
        second.put(b"ab-2", b"v2");
        let err = db.write(second).await.unwrap_err();
        assert!(matches!(err.kind(), crate::error::ErrorKind::Invalid));
        assert!(db.get(b"ab-2").await.unwrap().is_none());
        assert_eq!(db.get(b"abc-1").await.unwrap().unwrap().as_ref(), b"v1");
        db.close().await.unwrap();
    }

    /// An extractor that returns `Some(0)` — even consistently — is a
    /// configuration error: it would route every key into the empty-
    /// prefix segment, blocking any subsequent named-segment write
    /// because the empty prefix nests with all of them. Reject the
    /// write up front so the user sees the cause clearly.
    #[tokio::test]
    async fn write_rejects_empty_extractor_prefix() {
        #[derive(Debug)]
        struct AlwaysEmptyExtractor;
        impl crate::prefix_extractor::PrefixExtractor for AlwaysEmptyExtractor {
            fn name(&self) -> &str {
                "always-empty"
            }
            fn prefix_len(&self, _target: &crate::prefix_extractor::PrefixTarget) -> Option<usize> {
                Some(0)
            }
        }
        let db = open_db_with_extractor(
            "/tmp/test_write_empty_prefix",
            Arc::new(AlwaysEmptyExtractor),
        )
        .await;

        let mut batch = WriteBatch::new();
        batch.put(b"any-key", b"v1");
        let err = db.write(batch).await.unwrap_err();
        assert!(matches!(err.kind(), crate::error::ErrorKind::Invalid));
        assert!(
            err.to_string().contains("empty prefix"),
            "expected empty-prefix error, got: {err}"
        );
        // Nothing got durable — the rejected batch left no trace.
        assert!(db.get(b"any-key").await.unwrap().is_none());
        db.close().await.unwrap();
    }

    /// No-extractor DBs continue to accept arbitrary keys with no
    /// validation — the segment-routing check is a no-op.
    #[tokio::test]
    async fn write_without_extractor_accepts_arbitrary_keys() {
        let object_store = Arc::new(InMemory::new());
        let db = Db::open("/tmp/test_write_no_extractor", object_store)
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"v1");
        batch.put(b"abc", b"v2");
        batch.put(b"abcdef", b"v3");
        db.write(batch).await.unwrap();
        for (k, v) in [
            (&b"a"[..], &b"v1"[..]),
            (&b"abc"[..], &b"v2"[..]),
            (&b"abcdef"[..], &b"v3"[..]),
        ] {
            assert_eq!(db.get(k).await.unwrap().unwrap().as_ref(), v);
        }
        db.close().await.unwrap();
    }
}
