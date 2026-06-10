use std::collections::BTreeSet;

use bytes::Bytes;
use tokio::sync::watch;

use crate::error::SlateDBError;
use crate::manifest::VersionedManifest;
use crate::utils::WatchableOnceCell;
use crate::CloseReason;

/// A segment (RFC-0024), identified by the key prefix it owns; the segment
/// spans the key interval `[prefix, prefix++)`.
///
/// Note: this is distinct from [`crate::manifest::Segment`], which also carries
/// per-segment LSM state. This type is prefix-only.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentPrefix {
    /// The key prefix owned by the segment.
    pub prefix: Bytes,
}

/// Current status of the database, exposed via [`crate::Db::subscribe`].
///
/// Subscribers receive a [`tokio::sync::watch::Receiver<DbStatus>`] which
/// always reflects the latest state. When the database is dropped the watch
/// channel closes and [`changed()`](tokio::sync::watch::Receiver::changed)
/// returns an error.
#[derive(Clone, Debug, PartialEq)]
pub struct DbStatus {
    /// The durable sequence number. All writes with a sequence number less
    /// than or equal to this value are durably persisted to object storage
    /// and will survive process restarts.
    pub durable_seq: u64,
    /// The current in-memory manifest snapshot observed by this handle,
    /// paired with its manifest version ID.
    pub current_manifest: VersionedManifest,
    /// Segment prefixes (RFC-0024) touched by writes or WAL replay in this
    /// handle's memtables but not yet flushed to the manifest. Empty when no
    /// segment extractor is configured. Read it via
    /// [`DbStatus::list_segments`], which merges in the segments in the manifest.
    memtable_segments: Vec<SegmentPrefix>,
    /// Set once the database has been closed, indicating the reason.
    pub close_reason: Option<CloseReason>,
}

impl DbStatus {
    /// List all segment prefixes (RFC-0024): those in the current manifest
    /// unioned with those touched in this handle's memtables but not yet
    /// flushed.
    ///
    /// The result is sorted ascending by prefix and deduplicated.
    pub fn list_segments(&self) -> Vec<SegmentPrefix> {
        let mut set: BTreeSet<Bytes> = self
            .current_manifest
            .core()
            .segments
            .iter()
            .map(|segment| segment.prefix().clone())
            .collect();
        set.extend(self.memtable_segments.iter().map(|s| s.prefix.clone()));
        set.into_iter()
            .map(|prefix| SegmentPrefix { prefix })
            .collect()
    }
}

pub(crate) trait ClosedResultWriter: std::fmt::Debug + Send + Sync + 'static {
    fn write_result(&self, result: Result<(), SlateDBError>);
    fn result_reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>>;
}

/// Manages database lifecycle status, including the close result and
/// status subscriptions.
#[derive(Clone, Debug)]
pub(crate) struct DbStatusManager {
    cell: WatchableOnceCell<Result<(), SlateDBError>>,
    tx: watch::Sender<DbStatus>,
}

impl DbStatusManager {
    #[cfg(test)]
    pub(crate) fn new(initial_durable_seq: u64) -> Self {
        use crate::manifest::Manifest;
        use crate::manifest::ManifestCore;
        Self::new_with_manifest(
            initial_durable_seq,
            VersionedManifest {
                id: 1,
                manifest: Manifest::initial(ManifestCore::new()),
            },
        )
    }

    pub(crate) fn new_with_manifest(
        initial_durable_seq: u64,
        initial_manifest: VersionedManifest,
    ) -> Self {
        let (tx, _) = watch::channel(DbStatus {
            durable_seq: initial_durable_seq,
            current_manifest: initial_manifest,
            memtable_segments: Vec::new(),
            close_reason: None,
        });
        Self {
            cell: WatchableOnceCell::new(),
            tx,
        }
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

    pub(crate) fn report_manifest(&self, versioned: VersionedManifest) {
        self.tx.send_if_modified(|s| {
            if versioned.id >= s.current_manifest.id && s.current_manifest != versioned {
                s.current_manifest = versioned;
                true
            } else {
                false
            }
        });
    }

    /// Replace the published set of memtable segment prefixes (RFC-0024) with
    /// the union over the handle's currently live memtables. This is the path
    /// that lets the set *shrink*: it is called when establishing the set (open
    /// / WAL replay / checkpoint refresh) and right after a flush pops a memtable
    /// and folds its prefixes into the manifest, so a prefix held only by the
    /// flushed memtable drops out. Notifies subscribers only when the sorted set
    /// actually changes.
    pub(crate) fn report_memtable_segments(&self, prefixes: BTreeSet<Bytes>) {
        let segments: Vec<SegmentPrefix> = prefixes
            .into_iter()
            .map(|prefix| SegmentPrefix { prefix })
            .collect();
        self.tx.send_if_modified(|s| {
            if s.memtable_segments != segments {
                s.memtable_segments = segments;
                true
            } else {
                false
            }
        });
    }

    /// Add newly touched memtable segment prefixes (RFC-0024) to the published
    /// set. This is the write-path counterpart to
    /// [`Self::report_memtable_segments`]: a write can only *grow* the set, so
    /// rather than recomputing the full union over all live memtables on the hot
    /// path, we insert just the prefixes this write touched and notify only when
    /// at least one was not already published. Prefixes leave the set via
    /// [`Self::report_memtable_segments`] when their memtable is flushed. Keeps
    /// `segments` sorted ascending.
    pub(crate) fn add_memtable_segments(&self, prefixes: BTreeSet<Bytes>) {
        if prefixes.is_empty() {
            return;
        }
        self.tx.send_if_modified(|s| {
            let mut changed = false;
            for prefix in prefixes {
                if let Err(idx) = s
                    .memtable_segments
                    .binary_search_by(|sp| sp.prefix.cmp(&prefix))
                {
                    s.memtable_segments.insert(idx, SegmentPrefix { prefix });
                    changed = true;
                }
            }
            changed
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

    pub(crate) fn status(&self) -> DbStatus {
        self.tx.borrow().clone()
    }
}

impl ClosedResultWriter for WatchableOnceCell<Result<(), SlateDBError>> {
    fn write_result(&self, result: Result<(), SlateDBError>) {
        self.write(result);
    }

    fn result_reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.reader()
    }
}

impl ClosedResultWriter for DbStatusManager {
    fn write_result(&self, result: Result<(), SlateDBError>) {
        let reason = match &result {
            Ok(()) => CloseReason::Clean,
            Err(err) => CloseReason::from(crate::Error::from(err.clone()).kind()),
        };
        if self.cell.write(result) {
            self.report_closed(reason);
        }
    }

    fn result_reader(&self) -> crate::utils::WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.cell.reader()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Manifest;
    use crate::manifest::ManifestCore;

    fn versioned_manifest(id: u64) -> VersionedManifest {
        VersionedManifest {
            id,
            manifest: Manifest::initial(ManifestCore::new()),
        }
    }

    fn manifest_with_segments(id: u64, prefixes: &[&[u8]]) -> VersionedManifest {
        use crate::manifest::{LsmTreeState, Segment};
        use std::sync::Arc;
        let mut core = ManifestCore::new();
        core.segment_extractor_name = Some("test".to_string());
        core.segments = prefixes
            .iter()
            .map(|p| Segment {
                prefix: Bytes::from(p.to_vec()),
                tree: Arc::new(LsmTreeState::default()),
            })
            .collect();
        VersionedManifest {
            id,
            manifest: Manifest::initial(core),
        }
    }

    fn segment_prefix(prefix: &[u8]) -> SegmentPrefix {
        SegmentPrefix {
            prefix: Bytes::from(prefix.to_vec()),
        }
    }

    /// The published memtable segments as an order-independent set (ordering is
    /// a property of `list_segments`, not of the raw field).
    fn segment_set(status: &DbStatus) -> BTreeSet<SegmentPrefix> {
        status.memtable_segments.iter().cloned().collect()
    }

    #[test]
    fn should_initialize_with_no_segments_from_empty_manifest() {
        // given
        let mgr = DbStatusManager::new_with_manifest(0, versioned_manifest(1));

        // when
        let status = mgr.status();

        // then
        assert!(status.list_segments().is_empty());
    }

    #[test]
    fn should_initialize_with_segments_from_manifest() {
        // given
        let mgr = DbStatusManager::new_with_manifest(0, manifest_with_segments(1, &[b"a", b"b"]));

        // when
        let status = mgr.status();

        // then
        assert_eq!(
            status.list_segments(),
            vec![segment_prefix(b"a"), segment_prefix(b"b")]
        );
    }

    #[test]
    fn should_union_and_dedup_segments() {
        // given
        let mgr = DbStatusManager::new_with_manifest(0, manifest_with_segments(1, &[b"a", b"b"]));
        mgr.report_memtable_segments(BTreeSet::from([
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ]));

        // when
        let segments = mgr.status().list_segments();

        // then
        assert_eq!(
            segments,
            vec![
                segment_prefix(b"a"),
                segment_prefix(b"b"),
                segment_prefix(b"c")
            ]
        );
    }

    #[test]
    fn should_return_sorted_segments() {
        // given
        let mgr = DbStatusManager::new_with_manifest(0, manifest_with_segments(1, &[b"d", b"b"]));
        mgr.report_memtable_segments(BTreeSet::from([
            Bytes::from_static(b"c"),
            Bytes::from_static(b"a"),
        ]));

        // when
        let segments = mgr.status().list_segments();

        // then
        assert_eq!(
            segments,
            vec![
                segment_prefix(b"a"),
                segment_prefix(b"b"),
                segment_prefix(b"c"),
                segment_prefix(b"d")
            ]
        );
    }

    #[test]
    fn should_notify_when_reported_segments_change() {
        // given
        let mgr = DbStatusManager::new(0);
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.report_memtable_segments(BTreeSet::from([Bytes::from_static(b"x")]));

        // then
        assert!(rx.has_changed().unwrap());
        assert_eq!(
            rx.borrow_and_update().memtable_segments,
            vec![segment_prefix(b"x")]
        );
    }

    #[test]
    fn should_notify_when_adding_a_new_segment() {
        // given
        let mgr = DbStatusManager::new(0);
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.add_memtable_segments(BTreeSet::from([Bytes::from_static(b"m")]));

        // then
        assert!(rx.has_changed().unwrap());
        assert_eq!(
            segment_set(&rx.borrow_and_update()),
            BTreeSet::from([segment_prefix(b"m")])
        );

        // when
        mgr.add_memtable_segments(BTreeSet::from([Bytes::from_static(b"a")]));

        // then
        assert!(rx.has_changed().unwrap());
        assert_eq!(
            segment_set(&rx.borrow_and_update()),
            BTreeSet::from([segment_prefix(b"a"), segment_prefix(b"m")])
        );
    }

    #[test]
    fn should_not_notify_when_adding_a_known_segment() {
        // given
        let mgr = DbStatusManager::new(0);
        mgr.add_memtable_segments(BTreeSet::from([Bytes::from_static(b"x")]));
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.add_memtable_segments(BTreeSet::from([Bytes::from_static(b"x")]));

        // then
        assert!(!rx.has_changed().unwrap());

        // when
        mgr.add_memtable_segments(BTreeSet::from([
            Bytes::from_static(b"x"),
            Bytes::from_static(b"y"),
        ]));

        // then
        assert!(rx.has_changed().unwrap());
    }

    #[test]
    fn should_not_notify_when_reported_segments_unchanged() {
        // given
        let mgr = DbStatusManager::new(0);
        mgr.report_memtable_segments(BTreeSet::from([Bytes::from_static(b"x")]));
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.report_memtable_segments(BTreeSet::from([Bytes::from_static(b"x")]));

        // then
        assert!(!rx.has_changed().unwrap());
    }

    #[test]
    fn should_not_notify_on_same_manifest() {
        // given
        let initial = versioned_manifest(1);
        let mgr = DbStatusManager::new_with_manifest(0, initial.clone());
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.report_manifest(initial);

        // then
        assert!(!rx.has_changed().unwrap());
    }

    #[test]
    fn should_not_notify_on_older_manifest() {
        // given
        let mgr = DbStatusManager::new_with_manifest(0, versioned_manifest(5));
        let mut rx = mgr.subscribe();
        rx.borrow_and_update();

        // when
        mgr.report_manifest(versioned_manifest(3));

        // then
        assert!(!rx.has_changed().unwrap());
    }
}
