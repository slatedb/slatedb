use std::cmp::{max, min, Ordering};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::checkpoint::Checkpoint;
use crate::clone::CloneSource;
use crate::rand::DbRand;
use crate::seq_tracker::SequenceTracker;
use crate::utils::IdGenerator;
use bytes::Bytes;
use log::{debug, warn};
use serde::Serialize;
use slatedb_txn_obj::DirtyObject;
use uuid::Uuid;

pub(crate) mod store;

pub use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableInfo, SsTableView};

/// Per-LSM-tree state. Shared shape between the unsegmented tree (held directly
/// on `ManifestCore`) and each named segment held in `ManifestCore::segments`.
#[derive(Clone, Default, PartialEq, Serialize, Debug)]
pub(crate) struct LsmTreeState {
    /// The last compacted l0 SstView ID.
    pub last_compacted_l0_sst_view_id: Option<ulid::Ulid>,

    /// The SST ID of the last compacted L0. In V2, view IDs differ from SST IDs,
    /// but V1 only stores SST IDs. This field preserves the SST ID so that a
    /// V1-encoded manifest can correctly reference the compacted L0.
    pub last_compacted_l0_sst_id: Option<ulid::Ulid>,

    /// A list of the L0 SST views that are valid to read in the `compacted` folder.
    pub l0: VecDeque<SsTableView>,

    /// A list of the sorted runs that are valid to read in the `compacted` folder.
    pub compacted: Vec<SortedRun>,
}

impl LsmTreeState {
    /// Compactor-side merge: combine the writer's view of this tree (`writer`)
    /// into the compactor's view (`self`). The compactor keeps its compacted
    /// runs and `last_compacted_l0_*` markers (which only change when a
    /// compaction completes) and adopts the writer's L0, trimmed to drop
    /// entries the compactor has already absorbed.
    pub(crate) fn merge_from_writer(&self, writer: &Self) -> Self {
        Self::merge_writer_and_compactor(writer, self)
    }

    /// Writer-side merge: combine the compactor's view of this tree
    /// (`compactor`) into the writer's view (`self`). The writer keeps its L0
    /// (trimmed at the compactor's `last_compacted_l0_*` markers) and adopts
    /// the compactor's compacted runs and markers.
    pub(crate) fn merge_from_compactor(&self, compactor: &Self) -> Self {
        Self::merge_writer_and_compactor(self, compactor)
    }

    /// Canonical merge of a single LSM tree, called by both
    /// [`Self::merge_from_writer`] and [`Self::merge_from_compactor`]. The
    /// writer owns L0 and the compactor owns compacted runs / markers, so the
    /// merge keeps each side's authoritative state and drops L0 entries the
    /// compactor has already absorbed.
    /// True iff this tree is a "drain marker": no L0, no compacted runs, but
    /// the watermark is set. Drain markers are produced when the compactor
    /// drains a segment (advances `last_compacted_l0_*` to cover all observed
    /// L0s and clears `compacted`). They persist on the compactor's side and
    /// propagate to the writer; the writer's side prunes them at merge time
    /// once it has observed the marker and has no new data to add.
    pub(crate) fn is_drained(&self) -> bool {
        self.l0.is_empty()
            && self.compacted.is_empty()
            && (self.last_compacted_l0_sst_view_id.is_some()
                || self.last_compacted_l0_sst_id.is_some())
    }

    /// True iff this tree carries no state at all — no L0, no compacted runs,
    /// and no watermark. Truly-empty trees should not appear in the manifest.
    pub(crate) fn is_empty(&self) -> bool {
        self.l0.is_empty()
            && self.compacted.is_empty()
            && self.last_compacted_l0_sst_view_id.is_none()
            && self.last_compacted_l0_sst_id.is_none()
    }

    pub(crate) fn merge_writer_and_compactor(writer: &Self, compactor: &Self) -> Self {
        let last_compacted_view = compactor.last_compacted_l0_sst_view_id;
        let last_compacted_sst = compactor.last_compacted_l0_sst_id;
        // todo: this is brittle. we are relying on the l0 list always being
        //       updated in an expected order. We should instead encode the
        //       ordering in the l0 SST IDs and assert that it follows the
        //       order.
        let l0: VecDeque<SsTableView> =
            if last_compacted_view.is_some() || last_compacted_sst.is_some() {
                writer
                    .l0
                    .iter()
                    .cloned()
                    .take_while(|view| {
                        // Match by view ID first (V2 manifests), then fall back
                        // to SST ID (V1).
                        if let Some(id) = last_compacted_view {
                            if view.id == id {
                                return false;
                            }
                        }
                        if let Some(id) = last_compacted_sst {
                            if view.sst.id.unwrap_compacted_id() == id {
                                return false;
                            }
                        }
                        true
                    })
                    .collect()
            } else {
                writer.l0.clone()
            };
        Self {
            last_compacted_l0_sst_view_id: last_compacted_view,
            last_compacted_l0_sst_id: last_compacted_sst,
            l0,
            compacted: compactor.compacted.clone(),
        }
    }
}

/// Per-segment LSM state (RFC-0024). Each segment owns the contiguous key
/// interval `[prefix, prefix++)` and is compacted as an independent logical
/// LSM tree. Segments share the manifest-level WAL state and SST identity
/// counter with the unsegmented tree.
#[derive(Clone, PartialEq, Serialize, Debug)]
pub(crate) struct Segment {
    /// The segment's key prefix.
    pub prefix: Bytes,

    /// LSM state for this segment.
    pub tree: LsmTreeState,
}

/// Compactor-side segment merge: combine the writer's segments (`writer`)
/// into the compactor's segments (`local`).
///
/// The compactor never prunes its own drain markers — it preserves them
/// until the writer has observed the marker and pruned its own copy. The
/// only segment-removal action the compactor takes is to *follow* a writer
/// prune: when the compactor's local has a marker for a prefix that the
/// writer's manifest no longer carries, that absence is the writer's
/// signal that the marker has been observed and the segment can be dropped.
///
/// Both inputs are required to be sorted by `prefix`, and the output is
/// sorted by `prefix` (see [`ManifestCore::segments`]). The walk is a
/// linear two-cursor merge.
pub(crate) fn merge_segments_from_writer(local: &[Segment], writer: &[Segment]) -> Vec<Segment> {
    debug_assert!(
        is_sorted_by_prefix(writer),
        "writer segments must be sorted"
    );
    debug_assert!(is_sorted_by_prefix(local), "local segments must be sorted");
    let empty = LsmTreeState::default();
    let mut merged: Vec<Segment> = Vec::with_capacity(writer.len() + local.len());
    for step in MergeIter::new(writer, local) {
        match step {
            MergeStep::WriterOnly(w) => {
                // Compactor hasn't seen this prefix yet (newly-flushed).
                let tree = LsmTreeState::merge_writer_and_compactor(&w.tree, &empty);
                if !tree.is_empty() {
                    merged.push(Segment {
                        prefix: w.prefix.clone(),
                        tree,
                    });
                }
            }
            MergeStep::CompactorOnly(c) => {
                // Expected: writer has pruned a drain marker — drop to
                // follow. Anything else is a protocol violation.
                if !c.tree.is_drained() {
                    unreachable!(
                        "compactor-only segment with data: prefix={:?} tree={:?}",
                        c.prefix, c.tree
                    );
                }
            }
            MergeStep::Both(w, c) => {
                // Kernel merge. The compactor keeps markers in this
                // branch — only the writer prunes.
                let tree = LsmTreeState::merge_writer_and_compactor(&w.tree, &c.tree);
                if !tree.is_empty() {
                    merged.push(Segment {
                        prefix: w.prefix.clone(),
                        tree,
                    });
                }
            }
        }
    }
    merged
}

/// Writer-side segment merge: combine the compactor's segments (`compactor`)
/// into the writer's segments (`local`).
///
/// The writer is the sole pruner. After the kernel per-tree merge, if the
/// result is a drain marker (no L0 above the watermark, no compacted runs,
/// watermark set) the writer drops the segment from its commit. The
/// compactor will observe the absence on its next read and follow.
///
/// Both inputs are required to be sorted by `prefix`, and the output is
/// sorted by `prefix`. The walk is a linear two-cursor merge.
pub(crate) fn merge_segments_from_compactor(
    local: &[Segment],
    compactor: &[Segment],
) -> Vec<Segment> {
    debug_assert!(is_sorted_by_prefix(local), "local segments must be sorted");
    debug_assert!(
        is_sorted_by_prefix(compactor),
        "compactor segments must be sorted"
    );
    let empty = LsmTreeState::default();
    let mut merged: Vec<Segment> = Vec::with_capacity(local.len() + compactor.len());
    for step in MergeIter::new(local, compactor) {
        let (prefix, tree) = match step {
            MergeStep::WriterOnly(w) => (
                w.prefix.clone(),
                LsmTreeState::merge_writer_and_compactor(&w.tree, &empty),
            ),
            MergeStep::CompactorOnly(c) => (
                c.prefix.clone(),
                LsmTreeState::merge_writer_and_compactor(&empty, &c.tree),
            ),
            MergeStep::Both(w, c) => (
                w.prefix.clone(),
                LsmTreeState::merge_writer_and_compactor(&w.tree, &c.tree),
            ),
        };
        // Writer prune: drop drain markers (and truly-empty results, which
        // arise after the compactor has already pruned).
        if !tree.is_drained() && !tree.is_empty() {
            merged.push(Segment { prefix, tree });
        }
    }
    merged
}

/// One step of a linear two-cursor merge over a pair of sorted-by-prefix
/// segment slices. The first slice is interpreted as the writer's view and
/// the second as the compactor's view, advancing whichever cursor has the
/// smaller current prefix (or both, on a tie).
enum MergeStep<'a> {
    WriterOnly(&'a Segment),
    CompactorOnly(&'a Segment),
    Both(&'a Segment, &'a Segment),
}

/// Iterator over a sorted-by-prefix segment merge. Yields a [`MergeStep`]
/// for each prefix that appears in either input, in sorted order.
struct MergeIter<'a> {
    writer: &'a [Segment],
    compactor: &'a [Segment],
    i: usize,
    j: usize,
}

impl<'a> MergeIter<'a> {
    fn new(writer: &'a [Segment], compactor: &'a [Segment]) -> Self {
        Self {
            writer,
            compactor,
            i: 0,
            j: 0,
        }
    }
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = MergeStep<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let step = match (self.writer.get(self.i), self.compactor.get(self.j)) {
            (None, None) => return None,
            (Some(w), None) => MergeStep::WriterOnly(w),
            (None, Some(c)) => MergeStep::CompactorOnly(c),
            (Some(w), Some(c)) => match w.prefix.cmp(&c.prefix) {
                Ordering::Less => MergeStep::WriterOnly(w),
                Ordering::Greater => MergeStep::CompactorOnly(c),
                Ordering::Equal => MergeStep::Both(w, c),
            },
        };
        match &step {
            MergeStep::WriterOnly(_) => self.i += 1,
            MergeStep::CompactorOnly(_) => self.j += 1,
            MergeStep::Both(_, _) => {
                self.i += 1;
                self.j += 1;
            }
        }
        Some(step)
    }
}

fn is_sorted_by_prefix(segments: &[Segment]) -> bool {
    segments.windows(2).all(|w| w[0].prefix < w[1].prefix)
}

/// Internal immutable in-memory view of a `.manifest` file.
#[derive(Clone, PartialEq, Serialize, Debug)]
pub(crate) struct ManifestCore {
    /// Flag to indicate whether initialization has finished. When creating the initial manifest for
    /// a root db (one that is not a clone), this flag will be set to true. When creating the initial
    /// manifest for a clone db, this flag will be set to false and then updated to true once clone
    /// initialization has completed.
    pub initialized: bool,

    /// LSM state for data that is not associated with any named segment. When
    /// segmentation is not configured this is the only tree; otherwise it sits
    /// alongside the named segments in `segments`.
    #[serde(flatten)]
    pub tree: LsmTreeState,

    /// Per-segment LSM state (RFC-0024). Empty when no segment extractor is
    /// configured. Each segment carries the LSM state for the keys whose
    /// extracted prefix matches the segment's `prefix`.
    ///
    /// Invariant: sorted strictly ascending by `prefix`. Mutation sites
    /// (FlatBuffer decode, merge functions, future writer-flush insertion)
    /// must preserve this ordering. Lookup-by-prefix should use
    /// `binary_search_by_key`; range queries use `partition_point`.
    pub segments: Vec<Segment>,

    /// Name of the configured segment extractor (RFC-0024). Persisted so the
    /// writer can detect accidental reconfiguration on startup. `None` when
    /// no extractor is configured.
    pub segment_extractor_name: Option<String>,

    /// The next WAL SST ID to be assigned when creating a new WAL SST. The manifest FlatBuffer
    /// contains `wal_id_last_seen`, which is always one less than this value.
    pub next_wal_sst_id: u64,

    /// the WAL ID after which the WAL replay should start. Default to 0,
    /// which means all the WAL IDs should be greater than or equal to 1.
    /// When a new L0 is flushed, we update this field to the recent
    /// flushed WAL ID.
    pub replay_after_wal_id: u64,

    /// the `last_l0_clock_tick` includes all data in L0 and below --
    /// WAL entries will have their latest ticks recovered on replay
    /// into the in-memory state.
    pub last_l0_clock_tick: i64,

    /// it's persisted in the manifest, and only updated when a new L0
    /// SST is created in the manifest.
    pub last_l0_seq: u64,

    /// Minimum sequence number across all recent in-memory snapshots. The compactor
    /// needs this to determine whether it's safe to drop duplicate key writes. If a
    /// recent snapshot still references an older version of a key, it should not be
    /// recycled. This field is updated when a new L0 is flushed.
    pub recent_snapshot_min_seq: u64,

    /// A sequence tracker that maps sequence numbers to timestamps as defined in
    /// RFC-0012.
    pub sequence_tracker: SequenceTracker,

    /// A list of checkpoints that are currently open.
    pub checkpoints: Vec<Checkpoint>,

    /// The URI of the object store dedicated specifically for WAL, if any.
    pub wal_object_store_uri: Option<String>,
}

impl ManifestCore {
    pub(crate) fn new() -> Self {
        Self {
            initialized: true,
            tree: LsmTreeState::default(),
            segments: vec![],
            segment_extractor_name: None,
            next_wal_sst_id: 1,
            replay_after_wal_id: 0,
            last_l0_clock_tick: i64::MIN,
            last_l0_seq: 0,
            checkpoints: vec![],
            wal_object_store_uri: None,
            recent_snapshot_min_seq: 0,
            sequence_tracker: SequenceTracker::new(),
        }
    }

    pub(crate) fn new_with_wal_object_store(wal_object_store_uri: Option<String>) -> Self {
        let mut this = Self::new();
        this.wal_object_store_uri = wal_object_store_uri;
        this
    }

    pub(crate) fn init_clone_db(&self) -> ManifestCore {
        let mut clone = self.clone();
        clone.initialized = false;
        clone.checkpoints.clear();
        clone
    }

    pub(crate) fn log_db_runs(&self) {
        let l0s: Vec<_> = self.tree.l0.iter().map(|l0| l0.estimate_size()).collect();
        let compacted: Vec<_> = self
            .tree
            .compacted
            .iter()
            .map(|sr| (sr.id, sr.estimate_size()))
            .collect();
        debug!("DB Levels:");
        debug!("-----------------");
        debug!("{:?}", l0s);
        debug!("{:?}", compacted);
        debug!("-----------------");
    }

    pub(crate) fn find_checkpoint(&self, checkpoint_id: Uuid) -> Option<&Checkpoint> {
        self.checkpoints.iter().find(|c| c.id == checkpoint_id)
    }
}

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Manifest {
    // todo: try to make this writable only from module
    pub(crate) external_dbs: Vec<ExternalDb>,
    #[serde(flatten)]
    pub(crate) core: ManifestCore,
    // todo: try to make this writable only from module
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
}

/// A manifest snapshot paired with its version ID for monotonic ordering.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct VersionedManifest {
    /// The version ID of the manifest.
    pub(crate) id: u64,
    /// The flattened manifest state at this version.
    #[serde(flatten)]
    pub(crate) manifest: Manifest,
}

impl VersionedManifest {
    pub(crate) fn from_manifest(id: u64, manifest: Manifest) -> Self {
        Self { id, manifest }
    }

    /// Returns the manifest version ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the writer epoch recorded in this manifest snapshot.
    pub fn writer_epoch(&self) -> u64 {
        self.manifest.writer_epoch
    }

    /// Returns the compactor epoch recorded in this manifest snapshot.
    pub fn compactor_epoch(&self) -> u64 {
        self.manifest.compactor_epoch
    }

    /// Returns the external DB references recorded in this manifest snapshot.
    pub fn external_dbs(&self) -> &Vec<ExternalDb> {
        &self.manifest.external_dbs
    }

    /// Returns whether initialization has completed.
    pub fn initialized(&self) -> bool {
        self.manifest.core.initialized
    }

    /// Returns the last compacted L0 SST view ID, if any.
    pub fn last_compacted_l0_sst_view_id(&self) -> Option<ulid::Ulid> {
        self.manifest.core.tree.last_compacted_l0_sst_view_id
    }

    /// Returns the last compacted L0 SST ID, if any.
    pub fn last_compacted_l0_sst_id(&self) -> Option<ulid::Ulid> {
        self.manifest.core.tree.last_compacted_l0_sst_id
    }

    /// Returns the current L0 SST views.
    pub fn l0(&self) -> &VecDeque<SsTableView> {
        &self.manifest.core.tree.l0
    }

    /// Returns the current compacted sorted runs.
    pub fn compacted(&self) -> &Vec<SortedRun> {
        &self.manifest.core.tree.compacted
    }

    /// Returns the next WAL SST ID to assign.
    pub fn next_wal_sst_id(&self) -> u64 {
        self.manifest.core.next_wal_sst_id
    }

    /// Returns the WAL replay watermark.
    pub fn replay_after_wal_id(&self) -> u64 {
        self.manifest.core.replay_after_wal_id
    }

    /// Returns the last persisted L0 clock tick.
    pub fn last_l0_clock_tick(&self) -> i64 {
        self.manifest.core.last_l0_clock_tick
    }

    /// Returns the last persisted L0 sequence number.
    pub fn last_l0_seq(&self) -> u64 {
        self.manifest.core.last_l0_seq
    }

    /// Returns the minimum sequence number still visible to recent snapshots.
    pub fn recent_snapshot_min_seq(&self) -> u64 {
        self.manifest.core.recent_snapshot_min_seq
    }

    /// Returns the persisted sequence tracker.
    pub fn sequence_tracker(&self) -> &SequenceTracker {
        &self.manifest.core.sequence_tracker
    }

    /// Returns the checkpoints tracked by this manifest snapshot.
    pub fn checkpoints(&self) -> &Vec<Checkpoint> {
        &self.manifest.core.checkpoints
    }

    /// Returns the dedicated WAL object store URI, if any.
    pub fn wal_object_store_uri(&self) -> Option<&str> {
        self.manifest.core.wal_object_store_uri.as_deref()
    }

    pub(crate) fn core(&self) -> &ManifestCore {
        &self.manifest.core
    }
}

impl From<DirtyObject<Manifest>> for VersionedManifest {
    fn from(dirty: DirtyObject<Manifest>) -> Self {
        Self::from_manifest(dirty.id.id(), dirty.value)
    }
}

impl Manifest {
    pub(crate) fn initial(core: ManifestCore) -> Self {
        Self {
            external_dbs: vec![],
            core,
            writer_epoch: 0,
            compactor_epoch: 0,
        }
    }

    /// Create an initial manifest for a new clone. The returned
    /// manifest will set `initialized=false` to allow for additional
    /// initialization (such as copying wals).
    pub(crate) fn cloned(
        parent_manifest: &Manifest,
        parent_path: String,
        source_checkpoint_id: Uuid,
        rand: Arc<DbRand>,
    ) -> Self {
        let mut parent_external_sst_ids = HashSet::<SsTableId>::new();
        let mut clone_external_dbs = vec![];

        for parent_external_db in &parent_manifest.external_dbs {
            parent_external_sst_ids.extend(&parent_external_db.sst_ids);
            clone_external_dbs.push(ExternalDb {
                path: parent_external_db.path.clone(),
                source_checkpoint_id: parent_external_db.source_checkpoint_id,
                final_checkpoint_id: Some(rand.rng().gen_uuid()),
                sst_ids: parent_external_db.sst_ids.clone(),
            });
        }

        let parent_owned_sst_ids = parent_manifest
            .core
            .tree
            .compacted
            .iter()
            .flat_map(|sr| sr.sst_views.iter().map(|s| s.sst.id))
            .chain(parent_manifest.core.tree.l0.iter().map(|s| s.sst.id))
            .filter(|id| !parent_external_sst_ids.contains(id))
            .collect();

        clone_external_dbs.push(ExternalDb {
            path: parent_path,
            source_checkpoint_id,
            final_checkpoint_id: Some(rand.rng().gen_uuid()),
            sst_ids: parent_owned_sst_ids,
        });

        Self {
            external_dbs: clone_external_dbs,
            core: parent_manifest.core.init_clone_db(),
            writer_epoch: parent_manifest.writer_epoch,
            compactor_epoch: parent_manifest.compactor_epoch,
        }
    }

    pub(crate) fn projected(source_manifest: &Manifest, range: BytesRange) -> Manifest {
        let mut projected = source_manifest.clone();
        let mut sorter_runs_filtered = vec![];
        for sorter_run in &projected.core.tree.compacted {
            let sst_views = Self::filter_view_handles(&sorter_run.sst_views, false, &range);
            if !sst_views.is_empty() {
                sorter_runs_filtered.push(SortedRun {
                    id: sorter_run.id,
                    sst_views,
                });
            }
        }
        projected.core.tree.l0 =
            Self::filter_view_handles(&projected.core.tree.l0, true, &range).into();
        projected.core.tree.compacted = sorter_runs_filtered;
        // drop unused external_dbs
        let used_sst_ids: HashSet<SsTableId> = projected
            .core
            .tree
            .compacted
            .iter()
            .flat_map(|sr| sr.sst_views.iter().map(|v| v.sst.id))
            .chain(projected.core.tree.l0.iter().map(|v| v.sst.id))
            .collect();
        projected
            .external_dbs
            .retain(|e| e.sst_ids.iter().any(|id| used_sst_ids.contains(id)));
        projected
    }

    fn filter_view_handles<'a, T>(
        views: T,
        views_overlap: bool,
        projection_range: &BytesRange,
    ) -> Vec<SsTableView>
    where
        T: IntoIterator<Item = &'a SsTableView>,
    {
        let mut iter = views.into_iter().peekable();
        let mut filtered_handles = vec![];
        while let Some(current_handle) = iter.next() {
            let next_handle = if views_overlap {
                None
            } else {
                iter.peek().copied()
            };
            if let Some(intersection) =
                current_handle.compacted_intersection(next_handle, projection_range)
            {
                filtered_handles.push(current_handle.with_visible_range(intersection));
            }
        }
        filtered_handles
    }

    pub(crate) fn cloned_from_union(sources: Vec<CloneSource>, rand: Arc<DbRand>) -> Manifest {
        let mut ranges = vec![];
        for source in &sources {
            let range = source.manifest.range();
            if let Some(range) = range {
                ranges.push((source, range));
            } else {
                warn!("manifest has no SST files [manifest={:?}]", source.manifest);
            }
        }
        ranges.sort_by_key(|(_, range)| range.comparable_start_bound().cloned());

        // Ensure manifests are non-overlapping
        let mut previous_range = None;
        for (_, range) in ranges.iter() {
            if let Some(previous_range) = previous_range {
                if range.intersect(previous_range).is_some() {
                    unreachable!("overlapping ranges found");
                }
            }
            previous_range = Some(range);
        }

        // Now we can zip the manifests together
        let mut external_dbs = vec![];
        let mut core = ManifestCore::new();

        for (source, _) in ranges {
            let manifest = &source.manifest;

            // First, we need to add all the external dbs
            for parent_external_db in &manifest.external_dbs {
                external_dbs.push(ExternalDb {
                    path: parent_external_db.path.clone(),
                    source_checkpoint_id: parent_external_db.source_checkpoint_id,
                    final_checkpoint_id: None, // regenerated after deduplication
                    sst_ids: parent_external_db.sst_ids.clone(),
                });
            }
            // Then, we can add all the l0 ssts
            for sst in &manifest.core.tree.l0 {
                core.tree.l0.push_back(sst.clone());
            }
            // Finally, we can add all the sorted runs
            for sorted_run in &manifest.core.tree.compacted {
                core.tree.compacted.push(sorted_run.clone());
            }

            let owned_ssts = manifest.owned_ssts();
            if !owned_ssts.is_empty() {
                external_dbs.push(ExternalDb {
                    path: source.path.clone().into(),
                    source_checkpoint_id: source.checkpoint.id,
                    final_checkpoint_id: None, // regenerated after deduplication
                    sst_ids: owned_ssts,
                });
            }
        }

        // Renumber sorted runs to ensure sequential IDs without duplicates
        for (idx, sorted_run) in core.tree.compacted.iter_mut().enumerate() {
            sorted_run.id = idx as u32;
        }

        for source in &sources {
            core.last_l0_seq = max(core.last_l0_seq, source.manifest.core.last_l0_seq);
        }
        let external_dbs_merged = external_dbs
            .into_iter()
            .fold(
                HashMap::new(),
                |mut map: HashMap<(String, Uuid), HashSet<SsTableId>>, db| {
                    map.entry((db.path, db.source_checkpoint_id))
                        .or_default()
                        .extend::<HashSet<SsTableId>>(HashSet::from_iter(db.sst_ids));
                    map
                },
            )
            .iter()
            .map(|((path, checkpoint), sst_ids)| ExternalDb {
                path: path.clone(),
                source_checkpoint_id: *checkpoint,
                final_checkpoint_id: Some(rand.rng().gen_uuid()),
                sst_ids: sst_ids.iter().copied().collect(),
            })
            .collect();

        Self {
            external_dbs: external_dbs_merged,
            core,
            writer_epoch: 0,
            compactor_epoch: 0,
        }
    }

    fn range(&self) -> Option<BytesRange> {
        let mut start_bound = None;
        let mut end_bound = None;
        let all_views = self.core.tree.l0.iter().chain(
            self.core
                .tree
                .compacted
                .iter()
                .flat_map(|sr| sr.sst_views.iter()),
        );
        for sst in all_views {
            let range = sst.compacted_effective_range();
            start_bound = start_bound
                .map(|b| min(b, range.comparable_start_bound()))
                .or_else(|| Some(range.comparable_start_bound()));
            end_bound = end_bound
                .map(|b| max(b, range.comparable_end_bound()))
                .or_else(|| Some(range.comparable_end_bound()));
        }
        match (start_bound, end_bound) {
            (Some(start), Some(end)) => {
                let start: Bound<&Bytes> = start.into();
                let end: Bound<&Bytes> = end.into();
                Some(BytesRange::new(start.cloned(), end.cloned()))
            }
            (_, _) => None,
        }
    }
}

#[derive(Clone, Serialize, PartialEq, Debug)]
pub struct ExternalDb {
    pub path: String,
    pub source_checkpoint_id: Uuid,
    pub final_checkpoint_id: Option<Uuid>,
    pub sst_ids: Vec<SsTableId>,
}

impl Manifest {
    /// Returns a map from SST ID to the external DB path for all external SSTs.
    pub(crate) fn external_ssts(&self) -> HashMap<SsTableId, object_store::path::Path> {
        let mut external_ssts = HashMap::new();
        for external_db in &self.external_dbs {
            for id in &external_db.sst_ids {
                external_ssts.insert(*id, external_db.path.clone().into());
            }
        }
        external_ssts
    }

    pub(crate) fn owned_ssts(&self) -> Vec<SsTableId> {
        // SST IDs already tracked by the source's own external_dbs
        let source_external_sst_ids: HashSet<SsTableId> = self
            .external_dbs
            .iter()
            .flat_map(|db| db.sst_ids.iter().copied())
            .collect();
        // Owned SSTs = SSTs in core not already delegated to an external_db
        let owned_sst_ids: Vec<SsTableId> = self
            .core
            .tree
            .compacted
            .iter()
            .flat_map(|sr| sr.sst_views.iter().map(|s| s.sst.id))
            .chain(self.core.tree.l0.iter().map(|s| s.sst.id))
            .filter(|id| !source_external_sst_ids.contains(id))
            .collect();
        owned_sst_ids
    }

    pub(crate) fn has_wal_sst_reference(&self, wal_sst_id: u64) -> bool {
        wal_sst_id > self.core.replay_after_wal_id && wal_sst_id < self.core.next_wal_sst_id
    }

    /// Shrinks each `ExternalDb.sst_ids` to only IDs still referenced by this manifest's
    /// L0 and compacted sorted runs. `ExternalDb` entries are retained even when their
    /// `sst_ids` becomes empty — detaching a clone from its parent is done by the GC,
    /// not here, because it also requires that no live checkpoint references those IDs.
    pub(crate) fn prune_external_sst_ids(&mut self) {
        let used_sst_ids: HashSet<SsTableId> = self
            .core
            .tree
            .compacted
            .iter()
            .flat_map(|sr| sr.sst_views.iter().map(|v| v.sst.id))
            .chain(self.core.tree.l0.iter().map(|v| v.sst.id))
            .collect();
        for external_db in self.external_dbs.iter_mut() {
            external_db.sst_ids.retain(|id| used_sst_ids.contains(id));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};

    use super::{ExternalDb, Manifest};
    use crate::clone::CloneSource;
    use crate::config::CheckpointOptions;
    use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::{LsmTreeState, ManifestCore};
    use crate::rand::DbRand;
    use crate::Checkpoint;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use proptest::proptest;
    use rstest::rstest;
    use std::collections::{BTreeSet, HashMap, VecDeque};
    use std::ops::{Bound, Range, RangeBounds};
    use std::sync::Arc;
    use ulid::Ulid;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_init_clone_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        let parent_path = Path::from("/tmp/test_parent");
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_manifest = StoredManifest::create_new_db(
            parent_manifest_store,
            ManifestCore::new(),
            clock.clone(),
        )
        .await
        .unwrap();
        let checkpoint = parent_manifest
            .write_checkpoint(uuid::Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();

        let clone_path = Path::from("/tmp/test_clone");
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let clone_stored_manifest = StoredManifest::store_uninitialized_clone(
            clone_manifest_store,
            Manifest::cloned(
                parent_manifest.manifest(),
                parent_path.to_string(),
                checkpoint.id,
                Arc::new(DbRand::default()),
            ),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        let clone_manifest = clone_stored_manifest.manifest();

        // There should be single external db, since parent is not deeply nested.
        assert_eq!(clone_manifest.external_dbs.len(), 1);
        assert_eq!(clone_manifest.external_dbs[0].path, parent_path.to_string());
        assert_eq!(
            clone_manifest.external_dbs[0].source_checkpoint_id,
            checkpoint.id
        );
        assert!(clone_manifest.external_dbs[0].final_checkpoint_id.is_some());

        // The clone manifest should not be initialized
        assert!(!clone_manifest.core.initialized);

        // Check epoch has been carried over
        assert_eq!(
            parent_manifest.manifest().writer_epoch,
            clone_manifest.writer_epoch
        );
        assert_eq!(
            parent_manifest.manifest().compactor_epoch,
            clone_manifest.compactor_epoch
        );
    }

    #[tokio::test]
    async fn test_write_new_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        let path = Path::from("/tmp/test_db");
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut manifest = StoredManifest::create_new_db(
            Arc::clone(&manifest_store),
            ManifestCore::new(),
            clock.clone(),
        )
        .await
        .unwrap();

        let checkpoint = manifest
            .write_checkpoint(uuid::Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();

        let latest_manifest_id = manifest_store.read_latest_manifest().await.unwrap().id;
        assert_eq!(latest_manifest_id, checkpoint.manifest_id);
        assert_eq!(None, checkpoint.expire_time);
    }

    struct SstEntry {
        sst_alias: &'static str,
        first_entry: Bytes,
        visible_range: Option<BytesRange>,
    }

    impl SstEntry {
        fn regular(sst_alias: &'static str, first_entry: &'static str) -> Self {
            Self {
                sst_alias,
                first_entry: Bytes::copy_from_slice(first_entry.as_bytes()),
                visible_range: None,
            }
        }

        fn projected<T>(
            sst_alias: &'static str,
            first_entry: &'static str,
            visible_range: T,
        ) -> Self
        where
            T: RangeBounds<&'static str>,
        {
            Self {
                sst_alias,
                first_entry: Bytes::copy_from_slice(first_entry.as_bytes()),
                visible_range: Some(BytesRange::from_ref(visible_range)),
            }
        }
    }

    struct SimpleManifest {
        l0: Vec<SstEntry>,
        sorted_runs: Vec<Vec<SstEntry>>,
    }

    impl SimpleManifest {
        fn new(l0: Vec<SstEntry>, sorted_runs: Vec<(u32, Vec<SstEntry>)>) -> Self {
            Self {
                l0,
                sorted_runs: sorted_runs.into_iter().map(|(_, ssts)| ssts).collect(),
            }
        }
    }

    struct ProjectionTestCase {
        visible_range: Range<&'static str>,
        existing_manifest: SimpleManifest,
        expected_manifest: SimpleManifest,
    }

    #[rstest]
    #[case(ProjectionTestCase {
        visible_range: "h".."o",
        existing_manifest: SimpleManifest {
            l0: vec![
                SstEntry::regular("first", "a"),
                SstEntry::regular("second", "f"),
                SstEntry::regular("third", "m"),
            ],
            sorted_runs: vec![
                vec![
                    SstEntry::regular("sr0_first", "a"),
                ],
                vec![
                    SstEntry::regular("sr1_first", "a"),
                    SstEntry::regular("sr1_second", "f"),
                    SstEntry::regular("sr1_third", "m"),
                ],
            ],
        },
        expected_manifest: SimpleManifest {
            l0: vec![
                SstEntry::projected("first", "a", "h".."o"),
                SstEntry::projected("second", "f", "h".."o"),
                SstEntry::projected("third", "m", "m".."o"),
            ],
            sorted_runs: vec![
                vec![
                    // We can't filter this one out, because we don't know the
                    // end key, so it might still fall within the range
                    SstEntry::projected("sr0_first", "a", "h".."o"),
                ],
                vec![
                    SstEntry::projected("sr1_second", "f", "h".."m"),
                    SstEntry::projected("sr1_third", "m", "m".."o"),
                ],
            ],
        },
    })]
    #[case::distinct_ranges(ProjectionTestCase {
        visible_range: "c".."p",
        existing_manifest: SimpleManifest {
            l0: vec![
                SstEntry::projected("foo", "a", "a".."d"),
                SstEntry::projected("bar", "k", "n".."z"),
                SstEntry::projected("baz", "b", "s".."v"),
            ],
            sorted_runs: vec![],
        },
        expected_manifest: SimpleManifest {
            l0: vec![
                SstEntry::projected("foo", "a", "c".."d"),
                SstEntry::projected("bar", "k", "n".."p"),
            ],
            sorted_runs: vec![],
        },
    })]
    #[case::empty_sorted_run_excluded(ProjectionTestCase {
        visible_range: "a".."c",
        existing_manifest: SimpleManifest {
            l0: vec![],
            sorted_runs: vec![
                vec![
                    SstEntry::regular("sr0_first", "a"),
                    SstEntry::regular("sr0_second", "b"),
                ],
                vec![
                    SstEntry::regular("sr1_first", "a"),
                    SstEntry::regular("sr1_second", "e"),
                ],
                // sr2 is entirely outside "a".."c", so it should be excluded
                vec![
                    SstEntry::regular("sr2_first", "d"),
                    SstEntry::regular("sr2_second", "f"),
                ],
            ],
        },
        expected_manifest: SimpleManifest {
            l0: vec![],
            sorted_runs: vec![
                vec![
                    SstEntry::projected("sr0_first", "a", "a".."b"),
                    SstEntry::projected("sr0_second", "b", "b".."c"),
                ],
                vec![
                    SstEntry::projected("sr1_first", "a", "a".."c"),
                ],
            ],
        },
    })]
    fn test_projected(#[case] test_case: ProjectionTestCase) {
        let mut sst_ids = HashMap::new();
        let initial_manifest = build_manifest(&test_case.existing_manifest, |alias| {
            let sst_id = SsTableId::Compacted(Ulid::new());
            if sst_ids.insert(alias.to_string(), sst_id).is_some() {
                unreachable!("duplicate sst alias")
            }
            sst_id
        });

        let projected = Manifest::projected(
            &initial_manifest,
            BytesRange::from_ref(test_case.visible_range),
        );

        let expected_manifest = build_manifest(&test_case.expected_manifest, |alias| {
            *sst_ids.get(alias).unwrap()
        });

        assert_manifest_equal(&projected, &expected_manifest, &sst_ids);
    }

    struct UnionTestCase {
        manifests: Vec<SimpleManifest>,
        expected: SimpleManifest,
    }

    #[rstest]
    #[case::non_overlapping_l0s(UnionTestCase {
        manifests: vec![
            SimpleManifest {
                l0: vec![
                    SstEntry::projected("foo", "a", "a".."m"),
                    SstEntry::projected("bar", "f", "a".."m"),
                    SstEntry::projected("baz", "j", "a".."m")
                ],
                sorted_runs: vec![]
            },
            SimpleManifest {
                l0: vec![
                    SstEntry::projected("foo", "a", "m"..),
                    SstEntry::projected("bar", "f", "m"..),
                    SstEntry::projected("baz", "j", "m"..)
                ],
                sorted_runs: vec![]
            }
        ],
        expected: SimpleManifest {
            l0: vec![
                SstEntry::projected("foo", "a", "a".."m"),
                SstEntry::projected("bar", "f", "a".."m"),
                SstEntry::projected("baz", "j", "a".."m"),
                SstEntry::projected("foo", "a", "m"..),
                SstEntry::projected("bar", "f", "m"..),
                SstEntry::projected("baz", "j", "m"..)
                // This is not optimal, but it's a good start from correctness point of view. Eventually we want the manifest to look as follows:
                //
                // SstEntry::projected("foo", "a", "a"..),
                // SstEntry::projected("bar", "f", "a"..),
                // SstEntry::projected("baz", "j", "a"..),
            ],
            sorted_runs: vec![]
        },
    })]
    #[case::non_overlapping_l0s_with_gap(UnionTestCase {
        manifests: vec![
            SimpleManifest {
                l0: vec![
                    SstEntry::projected("foo", "a", "a".."m"),
                    SstEntry::projected("bar", "f", "a".."m"),
                    SstEntry::projected("baz", "j", "a".."m")
                ],
                sorted_runs: vec![]
            },
            SimpleManifest {
                l0: vec![
                    SstEntry::projected("foo", "a", "o"..),
                    SstEntry::projected("bar", "f", "o"..),
                    SstEntry::projected("baz", "j", "o"..)
                ],
                sorted_runs: vec![]
            }
        ],
        expected: SimpleManifest {
            l0: vec![
                SstEntry::projected("foo", "a", "a".."m"),
                SstEntry::projected("bar", "f", "a".."m"),
                SstEntry::projected("baz", "j", "a".."m"),
                SstEntry::projected("foo", "a", "o"..),
                SstEntry::projected("bar", "f", "o"..),
                SstEntry::projected("baz", "j", "o"..)
            ],
            sorted_runs: vec![]
        },
    })]
    fn test_union(#[case] test_case: UnionTestCase) {
        let mut sst_ids: HashMap<String, SsTableId> = HashMap::new();
        let rand = Arc::new(DbRand::default());
        let sources: Vec<crate::clone::CloneSource> = test_case
            .manifests
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let manifest = build_manifest(m, |alias| {
                    if let Some(sst_id) = sst_ids.get(alias) {
                        *sst_id
                    } else {
                        let sst_id = SsTableId::Compacted(Ulid::new());
                        sst_ids.insert(alias.to_string(), sst_id);
                        sst_id
                    }
                });
                CloneSource {
                    manifest,
                    path: Path::from(format!("/tmp/db{}", i)),
                    checkpoint: new_checkpoint(Uuid::new_v4()),
                }
            })
            .collect();

        let expected_manifest =
            build_manifest(&test_case.expected, |alias| *sst_ids.get(alias).unwrap());

        let union = Manifest::cloned_from_union(sources, rand);

        assert_manifest_equal(&union, &expected_manifest, &sst_ids);
    }

    #[test]
    fn test_lsm_tree_merge_invariants() {
        // Build a writer L0 with `n` views whose view IDs and SST IDs are all
        // distinct, so we can tell V2 (view-id) and V1 (sst-id) marker
        // matching apart.
        fn build_writer_l0(n: usize) -> VecDeque<SsTableView> {
            (0..n)
                .map(|i| {
                    let view_id = Ulid::from_parts(i as u64, 0);
                    let sst_id = Ulid::from_parts(i as u64, 1);
                    let handle = SsTableHandle::new(
                        SsTableId::Compacted(sst_id),
                        SST_FORMAT_VERSION_LATEST,
                        SsTableInfo::default(),
                    );
                    SsTableView::new(view_id, handle)
                })
                .collect()
        }

        proptest!(|(
            n in 1usize..10,
            // 0 = no marker, 1 = V2 (view id) only, 2 = V1 (sst id) only, 3 = both
            marker_kind in 0u8..4,
            // Resolved against [0, n] below: index `n` means "marker doesn't
            // match any L0 entry," exercising the no-trim path even with a
            // marker present.
            cutoff_idx_raw in 0usize..32,
        )| {
            let writer_l0 = build_writer_l0(n);
            let cutoff_idx = cutoff_idx_raw % (n + 1);

            let (last_view, last_sst) = if marker_kind == 0 {
                (None, None)
            } else if cutoff_idx == n {
                // Marker that doesn't match any entry.
                let nonmatch = Ulid::from_parts(u64::MAX, 0);
                match marker_kind {
                    1 => (Some(nonmatch), None),
                    2 => (None, Some(nonmatch)),
                    _ => (Some(nonmatch), Some(nonmatch)),
                }
            } else {
                let target = &writer_l0[cutoff_idx];
                let view_id = target.id;
                let sst_id = target.sst.id.unwrap_compacted_id();
                match marker_kind {
                    1 => (Some(view_id), None),
                    2 => (None, Some(sst_id)),
                    _ => (Some(view_id), Some(sst_id)),
                }
            };

            let writer = LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: writer_l0.clone(),
                compacted: vec![],
            };
            let compactor_compacted = vec![SortedRun { id: 42, sst_views: vec![] }];
            let compactor = LsmTreeState {
                last_compacted_l0_sst_view_id: last_view,
                last_compacted_l0_sst_id: last_sst,
                l0: VecDeque::new(),
                compacted: compactor_compacted.clone(),
            };

            let merged = writer.merge_from_compactor(&compactor);

            // Effective trim point: cutoff_idx if a matching marker is set,
            // otherwise n (everything passes through).
            let effective_cutoff = if marker_kind == 0 || cutoff_idx == n {
                n
            } else {
                cutoff_idx
            };
            let expected_l0: Vec<_> = writer_l0.iter().take(effective_cutoff).cloned().collect();
            let actual_l0: Vec<_> = merged.l0.iter().cloned().collect();
            assert_eq!(actual_l0, expected_l0);

            // Markers and compacted are taken from the compactor unchanged.
            assert_eq!(merged.last_compacted_l0_sst_view_id, last_view);
            assert_eq!(merged.last_compacted_l0_sst_id, last_sst);
            assert_eq!(merged.compacted, compactor_compacted);

            // The two wrappers must agree for any (writer, compactor) pair —
            // catches accidental arg-swapping in the wrappers.
            let merged_via_writer_side = compactor.merge_from_writer(&writer);
            assert_eq!(merged, merged_via_writer_side);
        });
    }

    #[test]
    fn test_segment_merge_preserves_prefix_order() {
        // Both directions of the segment-list merge must produce a result
        // sorted by `prefix` for any well-formed sorted inputs. Compactor-
        // only prefixes must be drain markers (anything else trips the
        // merge's protocol-violation guard).
        use crate::manifest::{
            is_sorted_by_prefix, merge_segments_from_compactor, merge_segments_from_writer, Segment,
        };

        fn live_tree(seed: u64) -> LsmTreeState {
            let view_id = Ulid::from_parts(seed, 0);
            let handle = SsTableHandle::new(
                SsTableId::Compacted(Ulid::from_parts(seed, 1)),
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo::default(),
            );
            LsmTreeState {
                last_compacted_l0_sst_view_id: None,
                last_compacted_l0_sst_id: None,
                l0: VecDeque::from(vec![SsTableView::new(view_id, handle)]),
                compacted: vec![],
            }
        }
        fn marker_tree(seed: u64) -> LsmTreeState {
            LsmTreeState {
                last_compacted_l0_sst_view_id: Some(Ulid::from_parts(seed, 0)),
                last_compacted_l0_sst_id: None,
                l0: VecDeque::new(),
                compacted: vec![],
            }
        }

        // Each `kind` value picks a (writer-has, compactor-has) pair for
        // a given prefix:
        //   0: writer-only
        //   1: compactor-only (must be marker)
        //   2: both (writer live, compactor live)
        //   3: skip (prefix absent on both sides)
        proptest!(|(kinds in proptest::collection::vec(0u8..4, 0..8))| {
            let mut writer: Vec<Segment> = Vec::new();
            let mut compactor: Vec<Segment> = Vec::new();
            for (idx, kind) in kinds.iter().enumerate() {
                let prefix = Bytes::from(format!("p{:02}/", idx));
                match kind % 4 {
                    0 => writer.push(Segment { prefix: prefix.clone(), tree: live_tree(idx as u64) }),
                    1 => compactor.push(Segment { prefix: prefix.clone(), tree: marker_tree(idx as u64) }),
                    2 => {
                        writer.push(Segment { prefix: prefix.clone(), tree: live_tree(idx as u64) });
                        compactor.push(Segment { prefix: prefix.clone(), tree: live_tree((idx as u64) + 100) });
                    }
                    _ => {}
                }
            }

            // Both inputs are constructed in prefix order via the index.
            assert!(is_sorted_by_prefix(&writer));
            assert!(is_sorted_by_prefix(&compactor));

            let merged_writer_side = merge_segments_from_compactor(&writer, &compactor);
            let merged_compactor_side = merge_segments_from_writer(&compactor, &writer);

            assert!(is_sorted_by_prefix(&merged_writer_side),
                "writer-side merge must produce sorted output");
            assert!(is_sorted_by_prefix(&merged_compactor_side),
                "compactor-side merge must produce sorted output");
        });
    }

    /// Simulator-based protocol check: drive a random interleaving of writer
    /// flushes/commits and compactor compactions/drains/commits through the
    /// segment merge protocol, and verify a battery of invariants on the
    /// resulting manifest history. The strongest is that each L0 the writer
    /// flushes appears in the committed manifest exactly once: never
    /// fabricated, never lost, never resurrected after a drain. Other
    /// invariants check L0 provenance, watermark monotonicity, watermark
    /// trim correctness, and cross-segment L0 uniqueness.
    #[test]
    fn test_protocol_simulation_invariants() {
        use crate::manifest::{merge_segments_from_compactor, merge_segments_from_writer, Segment};
        use proptest::prelude::*;

        const NUM_PREFIXES: u8 = 3;

        #[derive(Debug, Clone)]
        enum Op {
            WriterFlush(u8),
            WriterCommit,
            CompactorReadCompact(u8, u8),
            CompactorReadDrain(u8),
            CompactorCommit,
        }

        fn arb_op() -> impl Strategy<Value = Op> {
            prop_oneof![
                (0..NUM_PREFIXES).prop_map(Op::WriterFlush),
                Just(Op::WriterCommit),
                (0..NUM_PREFIXES, 1u8..4).prop_map(|(p, c)| Op::CompactorReadCompact(p, c)),
                (0..NUM_PREFIXES).prop_map(Op::CompactorReadDrain),
                Just(Op::CompactorCommit),
            ]
        }

        fn make_prefix(idx: u8) -> Bytes {
            Bytes::from(format!("p{:02}/", idx))
        }

        fn make_view(seq: u64) -> SsTableView {
            let view_id = Ulid::from_parts(seq, 0);
            SsTableView::new(
                view_id,
                SsTableHandle::new(
                    SsTableId::Compacted(Ulid::from_parts(seq, 1)),
                    SST_FORMAT_VERSION_LATEST,
                    SsTableInfo::default(),
                ),
            )
        }

        struct Simulator {
            // Published manifest's segment list — what's currently durable.
            store: Vec<Segment>,
            // Writer's in-memory state. Mutated by flushes; merged with
            // `store` on commit.
            writer: Vec<Segment>,
            // Compactor's in-memory state. Mutated by compactions/drains;
            // merged with `store` on commit.
            compactor: Vec<Segment>,
            next_l0_seq: u64,
            next_sr_id: u32,
            // Every L0 view ID the writer has ever flushed — the
            // provenance "ground truth."
            flushed_l0s: BTreeSet<Ulid>,
            // History of L0 view ID sets present in `store` after each
            // published commit. The resurrection check walks this.
            l0_history: Vec<BTreeSet<Ulid>>,
            // For each segment prefix, the watermark observed at each
            // commit (None if the prefix wasn't present). Used to
            // verify watermark monotonicity.
            watermark_history: Vec<HashMap<Bytes, Option<Ulid>>>,
        }

        impl Simulator {
            fn new() -> Self {
                Self {
                    store: Vec::new(),
                    writer: Vec::new(),
                    compactor: Vec::new(),
                    next_l0_seq: 0,
                    next_sr_id: 0,
                    flushed_l0s: BTreeSet::new(),
                    l0_history: Vec::new(),
                    watermark_history: Vec::new(),
                }
            }

            fn writer_flush(&mut self, prefix_idx: u8) {
                self.next_l0_seq += 1;
                let view = make_view(self.next_l0_seq);
                self.flushed_l0s.insert(view.id);
                let prefix = make_prefix(prefix_idx);
                match self
                    .writer
                    .binary_search_by(|s| s.prefix.as_ref().cmp(prefix.as_ref()))
                {
                    Ok(idx) => self.writer[idx].tree.l0.push_front(view),
                    Err(idx) => self.writer.insert(
                        idx,
                        Segment {
                            prefix,
                            tree: LsmTreeState {
                                last_compacted_l0_sst_view_id: None,
                                last_compacted_l0_sst_id: None,
                                l0: VecDeque::from(vec![view]),
                                compacted: vec![],
                            },
                        },
                    ),
                }
            }

            fn writer_commit(&mut self) {
                self.writer = merge_segments_from_compactor(&self.writer, &self.store);
                self.store = self.writer.clone();
                self.snapshot();
            }

            // Sync compactor's local state from the latest published manifest
            // before applying a compactor mutation. This models the compactor
            // reading writer's manifest at the start of each cycle.
            fn compactor_sync(&mut self) {
                self.compactor = merge_segments_from_writer(&self.compactor, &self.store);
            }

            fn compactor_read_compact(&mut self, prefix_idx: u8, count: u8) {
                self.compactor_sync();
                let prefix = make_prefix(prefix_idx);
                if let Ok(idx) = self
                    .compactor
                    .binary_search_by(|s| s.prefix.as_ref().cmp(prefix.as_ref()))
                {
                    let seg = &mut self.compactor[idx];
                    let count = (count as usize).min(seg.tree.l0.len());
                    if count == 0 {
                        return;
                    }
                    let newest = seg.tree.l0[0].id;
                    let sr_views: Vec<_> = (0..count).map(|i| seg.tree.l0[i].clone()).collect();
                    for _ in 0..count {
                        seg.tree.l0.pop_front();
                    }
                    seg.tree.last_compacted_l0_sst_view_id = Some(newest);
                    self.next_sr_id += 1;
                    seg.tree.compacted.insert(
                        0,
                        SortedRun {
                            id: self.next_sr_id,
                            sst_views: sr_views,
                        },
                    );
                }
            }

            fn compactor_read_drain(&mut self, prefix_idx: u8) {
                self.compactor_sync();
                let prefix = make_prefix(prefix_idx);
                if let Ok(idx) = self
                    .compactor
                    .binary_search_by(|s| s.prefix.as_ref().cmp(prefix.as_ref()))
                {
                    let seg = &mut self.compactor[idx];
                    if let Some(newest) = seg.tree.l0.front() {
                        seg.tree.last_compacted_l0_sst_view_id = Some(newest.id);
                    }
                    seg.tree.l0.clear();
                    seg.tree.compacted.clear();
                }
            }

            fn compactor_commit(&mut self) {
                self.compactor = merge_segments_from_writer(&self.compactor, &self.store);
                self.store = self.compactor.clone();
                self.snapshot();
            }

            fn snapshot(&mut self) {
                let mut l0_ids = BTreeSet::new();
                let mut watermarks: HashMap<Bytes, Option<Ulid>> = HashMap::new();
                for seg in &self.store {
                    for view in &seg.tree.l0 {
                        l0_ids.insert(view.id);
                    }
                    watermarks.insert(seg.prefix.clone(), seg.tree.last_compacted_l0_sst_view_id);
                }
                self.l0_history.push(l0_ids);
                self.watermark_history.push(watermarks);
            }

            // Drive any pending writer state into `store` and let the
            // compactor follow, so that "added at least once" can be
            // checked against `flushed_l0s`. Two cycles is enough for a
            // pending flush to traverse Live → potential drain → prune.
            fn settle(&mut self) {
                self.writer_commit();
                self.compactor_commit();
                self.writer_commit();
                self.compactor_commit();
            }

            fn check_invariants(&self) {
                self.check_no_l0_resurrection();
                self.check_l0_provenance();
                self.check_l0_unique_across_segments();
                self.check_watermark_trim();
                self.check_watermark_monotonic();
                self.check_l0_not_in_l0_and_sr_simultaneously();
            }

            // Each L0 ID has at most one NotSeen → Present transition;
            // an Absent → Present transition is a resurrection.
            fn check_no_l0_resurrection(&self) {
                let mut all_ids = BTreeSet::new();
                for snap in &self.l0_history {
                    all_ids.extend(snap.iter().copied());
                }
                for id in all_ids {
                    let mut state = 0u8; // 0 not seen, 1 present, 2 absent-after
                    for snap in &self.l0_history {
                        let present = snap.contains(&id);
                        state = match (state, present) {
                            (0, false) => 0,
                            (0, true) | (1, true) => 1,
                            (1, false) | (2, false) => 2,
                            (2, true) => panic!(
                                "L0 {} resurrected after removal; history={:?}",
                                id, self.l0_history
                            ),
                            _ => unreachable!(),
                        };
                    }
                }
            }

            // Every L0 ID appearing in any committed manifest must have
            // come from a writer flush — the merge must not fabricate IDs.
            fn check_l0_provenance(&self) {
                for snap in &self.l0_history {
                    for id in snap {
                        assert!(
                            self.flushed_l0s.contains(id),
                            "L0 {} appeared in manifest but was never flushed",
                            id
                        );
                    }
                }
                for seg in &self.store {
                    for sr in &seg.tree.compacted {
                        for view in &sr.sst_views {
                            assert!(
                                self.flushed_l0s.contains(&view.id),
                                "SR {} references L0 {} that was never flushed",
                                sr.id,
                                view.id
                            );
                        }
                    }
                }
            }

            // No L0 ID may appear in two different segments' L0 lists
            // simultaneously.
            fn check_l0_unique_across_segments(&self) {
                let mut seen: HashMap<Ulid, Bytes> = HashMap::new();
                for seg in &self.store {
                    for view in &seg.tree.l0 {
                        if let Some(other) = seen.get(&view.id) {
                            panic!(
                                "L0 {} appears in segment {:?} and {:?} simultaneously",
                                view.id, other, seg.prefix
                            );
                        }
                        seen.insert(view.id, seg.prefix.clone());
                    }
                }
            }

            // Within a segment, no L0 in the L0 list may have an ID at
            // or below the segment's watermark — those are supposed to
            // be trimmed by the merge.
            fn check_watermark_trim(&self) {
                for seg in &self.store {
                    if let Some(wm) = seg.tree.last_compacted_l0_sst_view_id {
                        for view in &seg.tree.l0 {
                            assert!(
                                view.id > wm,
                                "L0 {} survived in segment {:?} but watermark is {}",
                                view.id,
                                seg.prefix,
                                wm
                            );
                        }
                    }
                }
            }

            // For each segment prefix, the watermark must only advance
            // (or stay None) across the published-manifest history.
            fn check_watermark_monotonic(&self) {
                let mut all_prefixes = BTreeSet::new();
                for snap in &self.watermark_history {
                    all_prefixes.extend(snap.keys().cloned());
                }
                for prefix in all_prefixes {
                    let mut prev: Option<Ulid> = None;
                    for snap in &self.watermark_history {
                        let cur = snap.get(&prefix).copied().flatten();
                        if let (Some(p), Some(c)) = (prev, cur) {
                            assert!(
                                c >= p,
                                "watermark for {:?} regressed: {} → {}",
                                prefix,
                                p,
                                c
                            );
                        }
                        // Once a segment is dropped (cur == None), the
                        // next reincarnation starts fresh — don't carry
                        // the old watermark forward as a constraint.
                        if cur.is_some() {
                            prev = cur;
                        } else if !snap.contains_key(&prefix) {
                            prev = None;
                        }
                    }
                }
            }

            // Within any single committed manifest, an L0 ID present in
            // a segment's `l0` list must not also appear in any of that
            // segment's SRs.
            fn check_l0_not_in_l0_and_sr_simultaneously(&self) {
                for seg in &self.store {
                    let l0_ids: BTreeSet<Ulid> = seg.tree.l0.iter().map(|v| v.id).collect();
                    for sr in &seg.tree.compacted {
                        for view in &sr.sst_views {
                            assert!(
                                !l0_ids.contains(&view.id),
                                "L0 {} appears in both l0 list and SR {} of segment {:?}",
                                view.id,
                                sr.id,
                                seg.prefix
                            );
                        }
                    }
                }
            }

            // Every L0 the writer flushed must have appeared in at
            // least one published manifest's L0 list. Combined with the
            // resurrection check, this gives "added exactly once."
            fn check_added_exactly_once(&self) {
                for id in &self.flushed_l0s {
                    let appeared = self.l0_history.iter().any(|s| s.contains(id));
                    assert!(
                        appeared,
                        "L0 {} was flushed but never reached a committed manifest",
                        id
                    );
                }
            }
        }

        proptest!(|(ops in proptest::collection::vec(arb_op(), 0..40))| {
            let mut sim = Simulator::new();
            for op in &ops {
                match op {
                    Op::WriterFlush(p) => sim.writer_flush(*p),
                    Op::WriterCommit => sim.writer_commit(),
                    Op::CompactorReadCompact(p, c) => sim.compactor_read_compact(*p, *c),
                    Op::CompactorReadDrain(p) => sim.compactor_read_drain(*p),
                    Op::CompactorCommit => sim.compactor_commit(),
                }
                sim.check_invariants();
            }
            // Drain any pending writer state through the protocol so the
            // "added at least once" half of "exactly once" can hold.
            sim.settle();
            sim.check_invariants();
            sim.check_added_exactly_once();
        });
    }

    #[test]
    fn test_union_renumbers_sr_ids() {
        // Create manifest 1 with 2 sorted runs covering "a".."m"
        let manifest1 = build_manifest(
            &SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    vec![SstEntry::projected("sr1_0_sst0", "a", "a".."g")],
                    vec![SstEntry::projected("sr1_1_sst0", "g", "g".."m")],
                ],
            },
            |_| SsTableId::Compacted(Ulid::new()),
        );

        // Create manifest 2 with 3 sorted runs covering "m"..∞
        let manifest2 = build_manifest(
            &SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    vec![SstEntry::projected("sr2_0_sst0", "m", "m".."s")],
                    vec![SstEntry::projected("sr2_1_sst0", "s", "s".."t")],
                    vec![SstEntry::projected("sr2_2_sst0", "t", "t"..)],
                ],
            },
            |_| SsTableId::Compacted(Ulid::new()),
        );

        let rand = Arc::new(DbRand::default());
        let union = Manifest::cloned_from_union(
            vec![
                CloneSource {
                    manifest: manifest1,
                    path: Path::from("/tmp/db1"),
                    checkpoint: new_checkpoint(Uuid::new_v4()),
                },
                CloneSource {
                    manifest: manifest2,
                    path: Path::from("/tmp/db2"),
                    checkpoint: new_checkpoint(Uuid::new_v4()),
                },
            ],
            rand,
        );

        // After union, we should have 5 SRs with IDs 0, 1, 2, 3, 4
        assert_eq!(union.core.tree.compacted.len(), 5);

        let sr_ids: Vec<u32> = union.core.tree.compacted.iter().map(|sr| sr.id).collect();
        assert_eq!(sr_ids, vec![0, 1, 2, 3, 4], "SR IDs should be sequential");

        // Verify no duplicates
        let mut seen = std::collections::HashSet::new();
        for id in &sr_ids {
            assert!(seen.insert(id), "Duplicate SR ID: {}", id);
        }
    }

    #[test]
    fn test_union_propagates_last_l0_seq() {
        let mut manifest1 = build_manifest(
            &SimpleManifest {
                l0: vec![],
                sorted_runs: vec![vec![SstEntry::projected("sr1", "a", "a".."m")]],
            },
            |_| SsTableId::Compacted(Ulid::new()),
        );
        manifest1.core.last_l0_seq = 100;

        let mut manifest2 = build_manifest(
            &SimpleManifest {
                l0: vec![],
                sorted_runs: vec![vec![SstEntry::projected("sr2", "m", "m"..)]],
            },
            |_| SsTableId::Compacted(Ulid::new()),
        );
        manifest2.core.last_l0_seq = 200;

        let union = Manifest::cloned_from_union(
            vec![
                CloneSource {
                    manifest: manifest1,
                    path: Path::from("/tmp/db1"),
                    checkpoint: new_checkpoint(Uuid::new_v4()),
                },
                CloneSource {
                    manifest: manifest2,
                    path: Path::from("/tmp/db2"),
                    checkpoint: new_checkpoint(Uuid::new_v4()),
                },
            ],
            Arc::new(DbRand::default()),
        );

        assert_eq!(union.core.last_l0_seq, 200);
    }

    #[test]
    fn test_union_external_dbs() {
        // manifest1 is clone-like: owns own_sst in core and inherits grandparent_sst
        // via an existing external_db entry. manifest2 is a plain source.
        // The union must: add each source as an ExternalDb (owned SSTs only),
        // carry over inherited chains, and resolve all SSTs to the correct paths.
        let rand = Arc::new(DbRand::default());

        let parent1_sst1 = SsTableId::Compacted(Ulid::new());
        let parent2_sst1 = SsTableId::Compacted(Ulid::new());
        let grandparent_sst = SsTableId::Compacted(Ulid::new());
        let grandparent_source_cp = Uuid::new_v4();
        let grandparent_final_cp = Uuid::new_v4();

        let mut manifest1 = build_manifest(
            &SimpleManifest {
                l0: vec![SstEntry::projected("own", "a", "a".."m")],
                sorted_runs: vec![],
            },
            |_| parent1_sst1,
        );
        manifest1.external_dbs.push(ExternalDb {
            path: "/tmp/grandparent".to_string(),
            source_checkpoint_id: grandparent_source_cp,
            final_checkpoint_id: Some(grandparent_final_cp),
            sst_ids: vec![grandparent_sst],
        });

        let manifest2 = build_manifest(
            &SimpleManifest {
                l0: vec![SstEntry::projected("sst2", "m", "m"..)],
                sorted_runs: vec![],
            },
            |_| parent2_sst1,
        );

        let cp1 = Uuid::new_v4();
        let cp2 = Uuid::new_v4();
        let union = Manifest::cloned_from_union(
            vec![
                CloneSource {
                    manifest: manifest1,
                    path: Path::from("/tmp/db1"),
                    checkpoint: new_checkpoint(cp1),
                },
                CloneSource {
                    manifest: manifest2,
                    path: Path::from("/tmp/db2"),
                    checkpoint: new_checkpoint(cp2),
                },
            ],
            rand,
        );

        // db1 (owned SSTs only), grandparent (carried over), db2
        assert_eq!(union.external_dbs.len(), 3);

        let db1 = union
            .external_dbs
            .iter()
            .find(|e| e.path == "tmp/db1")
            .unwrap();
        assert_eq!(db1.source_checkpoint_id, cp1);
        assert_eq!(db1.sst_ids, vec![parent1_sst1]); // grandparent_sst must not leak in

        let db2 = union
            .external_dbs
            .iter()
            .find(|e| e.path == "tmp/db2")
            .unwrap();
        assert_eq!(db2.source_checkpoint_id, cp2);
        assert_eq!(db2.sst_ids, vec![parent2_sst1]);

        let grandparent = union
            .external_dbs
            .iter()
            .find(|e| e.path == "/tmp/grandparent")
            .unwrap();
        // source_checkpoint_id is preserved so the union still depends on the
        // same checkpoint on grandparent that the parent's clone depends on.
        assert_eq!(grandparent.source_checkpoint_id, grandparent_source_cp);
        // final_checkpoint_id must be regenerated — the union clone owns its own
        // checkpoint and must not claim ownership over the parent's.
        assert!(grandparent.final_checkpoint_id.is_some());
        assert_ne!(
            grandparent.final_checkpoint_id,
            Some(grandparent_final_cp),
            "inherited final_checkpoint_id must be regenerated"
        );

        // All three SSTs must resolve to their correct source paths
        let external_ssts = union.external_ssts();
        assert_eq!(
            external_ssts.get(&parent1_sst1),
            Some(&Path::from("/tmp/db1"))
        );
        assert_eq!(
            external_ssts.get(&parent2_sst1),
            Some(&Path::from("/tmp/db2"))
        );
        assert_eq!(
            external_ssts.get(&grandparent_sst),
            Some(&Path::from("/tmp/grandparent"))
        );
    }

    fn new_checkpoint(id: Uuid) -> Checkpoint {
        Checkpoint {
            id,
            manifest_id: 1,
            create_time: DefaultSystemClock::new().now(),
            expire_time: None,
            name: None,
        }
    }

    #[test]
    fn test_range_includes_compacted_ssts() {
        let manifest = build_manifest(
            &SimpleManifest::new(
                vec![],
                vec![(
                    0,
                    vec![
                        SstEntry::projected("sr_a", "a", "a".."m"),
                        SstEntry::projected("sr_n", "n", "m"..),
                    ],
                )],
            ),
            |_| SsTableId::Compacted(Ulid::new()),
        );
        let range = manifest
            .range()
            .expect("range should be Some for manifest with sorted runs");
        assert_eq!(range.start_bound(), Bound::Included(&Bytes::from("a")));
        assert_eq!(range.end_bound(), Bound::Unbounded);
    }

    fn build_manifest<F>(manifest: &SimpleManifest, mut sst_id_fn: F) -> Manifest
    where
        F: FnMut(&str) -> SsTableId,
    {
        let mut core = ManifestCore::new();
        for entry in &manifest.l0 {
            let sst_id = sst_id_fn(entry.sst_alias);
            let view_id = sst_id.unwrap_compacted_id();
            core.tree.l0.push_back(SsTableView::new_projected(
                view_id,
                SsTableHandle::new(
                    sst_id,
                    SST_FORMAT_VERSION_LATEST,
                    SsTableInfo {
                        first_entry: Some(entry.first_entry.clone()),
                        ..SsTableInfo::default()
                    },
                ),
                entry.visible_range.clone(),
            ));
        }
        for (idx, sorted_run) in manifest.sorted_runs.iter().enumerate() {
            core.tree.compacted.push(SortedRun {
                id: idx as u32,
                sst_views: sorted_run
                    .iter()
                    .map(|entry| {
                        let sst_id = sst_id_fn(entry.sst_alias);
                        let view_id = sst_id.unwrap_compacted_id();
                        SsTableView::new_projected(
                            view_id,
                            SsTableHandle::new(
                                sst_id,
                                SST_FORMAT_VERSION_LATEST,
                                SsTableInfo {
                                    first_entry: Some(entry.first_entry.clone()),
                                    ..SsTableInfo::default()
                                },
                            ),
                            entry.visible_range.clone(),
                        )
                    })
                    .collect(),
            });
        }
        Manifest::initial(core)
    }

    fn assert_manifest_equal(
        actual: &Manifest,
        expected: &Manifest,
        sst_ids: &HashMap<String, SsTableId>,
    ) {
        let sst_aliases: HashMap<SsTableId, String> =
            sst_ids.iter().map(|(k, v)| (*v, k.clone())).collect();

        if actual.core.tree.l0 != expected.core.tree.l0 {
            let mut error_msg = String::from("Manifest L0 mismatch.\n\nActual: \n");

            // Format actual L0 entries
            for (idx, handle) in actual.core.tree.l0.iter().enumerate() {
                let id_str = sst_aliases
                    .get(&handle.sst.id)
                    .map(|a| a.as_str())
                    .unwrap_or("UNKNOWN");

                let first_entry = handle
                    .sst
                    .info
                    .first_entry
                    .as_ref()
                    .map(|k| format!("{:?}", k))
                    .unwrap();

                let visible_range = handle
                    .visible_range
                    .as_ref()
                    .map(format_range)
                    .unwrap_or_else(|| "None".to_string());

                let result = if expected.core.tree.l0.get(idx) == Some(handle) {
                    ""
                } else {
                    " --> Unexpected"
                };

                error_msg.push_str(&format!(
                    "{}. {} (first_entry: {}, visible_range: {}){}\n",
                    idx + 1,
                    id_str,
                    first_entry,
                    visible_range,
                    result
                ));
            }

            error_msg.push_str("\nExpected: \n");

            // Format expected L0 entries
            for (idx, handle) in expected.core.tree.l0.iter().enumerate() {
                let id_str = sst_aliases.get(&handle.sst.id).unwrap();

                let first_entry = handle
                    .sst
                    .info
                    .first_entry
                    .as_ref()
                    .map(|k| format!("{:?}", k))
                    .unwrap();

                let visible_range = handle
                    .visible_range
                    .as_ref()
                    .map(format_range)
                    .unwrap_or_else(|| "None".to_string());

                error_msg.push_str(&format!(
                    "{}. {} (first_entry: {}, visible_range: {})\n",
                    idx + 1,
                    id_str,
                    first_entry,
                    visible_range
                ));
            }

            panic!("{}", error_msg);
        }

        assert_eq!(
            actual.core.tree.compacted, expected.core.tree.compacted,
            "Sorted runs do not match."
        );
    }

    fn format_range(range: &BytesRange) -> String {
        let start = match range.start_bound() {
            Bound::Included(start) => format!("={:?}", start),
            Bound::Excluded(start) => format!("{:?}", start),
            Bound::Unbounded => "".to_string(),
        };
        let end = match range.end_bound() {
            Bound::Included(end) => format!("={:?}", end),
            Bound::Excluded(end) => format!("{:?}", end),
            Bound::Unbounded => "".to_string(),
        };
        format!("{}..{}", start, end)
    }

    #[test]
    fn test_projected_drops_unused_external_dbs() {
        let projection_range = BytesRange::from_ref("a".."b");

        let sst_id_1 = SsTableId::Compacted(Ulid::new());
        let sst_id_2 = SsTableId::Compacted(Ulid::new());
        let sst_id_3 = SsTableId::Compacted(Ulid::new());
        let sst_id_4 = SsTableId::Compacted(Ulid::new());

        let mut core = ManifestCore::new();

        core.tree.l0.push_back(create_sst_view(sst_id_1, b"a")); // inside projection_range
        core.tree.l0.push_back(create_sst_view(sst_id_2, b"c")); // outside projection_range
        core.tree.l0.push_back(create_sst_view(sst_id_3, b"d")); // outside projection_range
        core.tree.l0.push_back(create_sst_view(sst_id_4, b"e")); // outside projection_range

        let mut manifest = Manifest::initial(core);

        manifest.external_dbs = vec![
            ExternalDb {
                path: "/path/to/db1".to_string(),
                source_checkpoint_id: Uuid::new_v4(),
                final_checkpoint_id: None,
                sst_ids: vec![sst_id_1, sst_id_2],
            },
            ExternalDb {
                path: "/path/to/db2".to_string(),
                source_checkpoint_id: Uuid::new_v4(),
                final_checkpoint_id: None,
                sst_ids: vec![sst_id_3, sst_id_4],
            },
        ];

        assert_eq!(manifest.external_dbs.len(), 2);

        let projected = Manifest::projected(&manifest, projection_range);

        assert_eq!(projected.external_dbs.len(), 1);
        assert_eq!(projected.external_dbs[0].path, "/path/to/db1");
    }

    #[test]
    fn test_prune_external_sst_ids_shrinks_and_keeps_entries() {
        let live_l0 = SsTableId::Compacted(Ulid::new());
        let live_compacted = SsTableId::Compacted(Ulid::new());
        let stale_a = SsTableId::Compacted(Ulid::new());
        let stale_b = SsTableId::Compacted(Ulid::new());

        let mut core = ManifestCore::new();
        core.tree.l0.push_back(create_sst_view(live_l0, b"a"));
        core.tree.compacted.push(SortedRun {
            id: 0,
            sst_views: vec![create_sst_view(live_compacted, b"b")],
        });

        let mut manifest = Manifest::initial(core);
        manifest.external_dbs = vec![
            // Mix of live and stale IDs: stale ones should be pruned, live kept.
            ExternalDb {
                path: "/path/to/partially_referenced".to_string(),
                source_checkpoint_id: Uuid::new_v4(),
                final_checkpoint_id: Some(Uuid::new_v4()),
                sst_ids: vec![live_l0, stale_a, live_compacted],
            },
            // No live IDs: entry must be retained (with empty sst_ids) so that GC can
            // later detach using the final_checkpoint_id.
            ExternalDb {
                path: "/path/to/fully_compacted".to_string(),
                source_checkpoint_id: Uuid::new_v4(),
                final_checkpoint_id: Some(Uuid::new_v4()),
                sst_ids: vec![stale_a, stale_b],
            },
        ];

        manifest.prune_external_sst_ids();

        assert_eq!(manifest.external_dbs.len(), 2);
        assert_eq!(
            manifest.external_dbs[0].sst_ids,
            vec![live_l0, live_compacted]
        );
        assert!(manifest.external_dbs[1].sst_ids.is_empty());
        assert!(manifest.external_dbs[1].final_checkpoint_id.is_some());
    }

    fn create_sst_view(sst_id: SsTableId, first_entry_bytes: &'static [u8; 1]) -> SsTableView {
        SsTableView::new_projected(
            sst_id.unwrap_compacted_id(),
            SsTableHandle::new(
                sst_id,
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo {
                    first_entry: Some(Bytes::from_static(first_entry_bytes)),
                    ..SsTableInfo::default()
                },
            ),
            None,
        )
    }

    fn manifest_with_one_compacted_sst(
        sst_id: SsTableId,
        first_entry: &'static [u8],
        visible_range: BytesRange,
    ) -> Manifest {
        let mut core = ManifestCore::new();
        core.tree.compacted.push(SortedRun {
            id: 0,
            sst_views: vec![SsTableView::new_projected(
                sst_id.unwrap_compacted_id(),
                SsTableHandle::new(
                    sst_id,
                    SST_FORMAT_VERSION_LATEST,
                    SsTableInfo {
                        first_entry: Some(Bytes::from_static(first_entry)),
                        ..SsTableInfo::default()
                    },
                ),
                Some(visible_range),
            )],
        });
        Manifest::initial(core)
    }

    #[test]
    fn test_union_deduplicates_external_dbs() {
        use std::collections::HashSet;

        let shared_path = "shared_ancestor".to_string();
        let shared_source_cp = Uuid::new_v4();
        let original_final_cp = Uuid::new_v4();

        let sst_a = SsTableId::Compacted(Ulid::from_parts(1000, 0));
        let sst_b = SsTableId::Compacted(Ulid::from_parts(1001, 0));
        let sst_c = SsTableId::Compacted(Ulid::from_parts(1002, 0));

        let sst_own1 = SsTableId::Compacted(Ulid::from_parts(2000, 0));
        let mut m1 =
            manifest_with_one_compacted_sst(sst_own1, b"a", BytesRange::from_ref("a".."m"));
        m1.external_dbs.push(ExternalDb {
            path: shared_path.clone(),
            source_checkpoint_id: shared_source_cp,
            final_checkpoint_id: Some(original_final_cp),
            sst_ids: vec![sst_a, sst_b],
        });

        let sst_own2 = SsTableId::Compacted(Ulid::from_parts(3000, 0));
        let mut m2 = manifest_with_one_compacted_sst(sst_own2, b"m", BytesRange::from_ref("m"..));
        m2.external_dbs.push(ExternalDb {
            path: shared_path.clone(),
            source_checkpoint_id: shared_source_cp,
            final_checkpoint_id: Some(original_final_cp),
            sst_ids: vec![sst_b, sst_c],
        });

        let rand = Arc::new(DbRand::default());
        let sources = vec![
            CloneSource {
                manifest: m1,
                path: Path::from("/tmp/db1"),
                checkpoint: new_checkpoint(Uuid::new_v4()),
            },
            CloneSource {
                manifest: m2,
                path: Path::from("/tmp/db2"),
                checkpoint: new_checkpoint(Uuid::new_v4()),
            },
        ];

        let result = Manifest::cloned_from_union(sources, rand);

        let shared_entries: Vec<_> = result
            .external_dbs
            .iter()
            .filter(|db| db.path == shared_path && db.source_checkpoint_id == shared_source_cp)
            .collect();
        assert_eq!(
            shared_entries.len(),
            1,
            "Should have exactly one entry for the shared (path, source_checkpoint_id)"
        );

        let merged_ids: HashSet<SsTableId> = shared_entries[0].sst_ids.iter().copied().collect();
        let expected_ids: HashSet<SsTableId> = [sst_a, sst_b, sst_c].iter().copied().collect();
        assert_eq!(merged_ids, expected_ids);
    }
}
