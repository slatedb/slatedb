use std::cmp::{max, min};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::checkpoint::Checkpoint;
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

/// Internal immutable in-memory view of a `.manifest` file.
#[derive(Clone, PartialEq, Serialize, Debug)]
pub(crate) struct ManifestCore {
    /// Flag to indicate whether initialization has finished. When creating the initial manifest for
    /// a root db (one that is not a clone), this flag will be set to true. When creating the initial
    /// manifest for a clone db, this flag will be set to false and then updated to true once clone
    /// initialization has completed.
    pub initialized: bool,

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
            last_compacted_l0_sst_view_id: None,
            last_compacted_l0_sst_id: None,
            l0: VecDeque::new(),
            compacted: vec![],
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
        let l0s: Vec<_> = self.l0.iter().map(|l0| l0.estimate_size()).collect();
        let compacted: Vec<_> = self
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
        self.manifest.core.last_compacted_l0_sst_view_id
    }

    /// Returns the last compacted L0 SST ID, if any.
    pub fn last_compacted_l0_sst_id(&self) -> Option<ulid::Ulid> {
        self.manifest.core.last_compacted_l0_sst_id
    }

    /// Returns the current L0 SST views.
    pub fn l0(&self) -> &VecDeque<SsTableView> {
        &self.manifest.core.l0
    }

    /// Returns the current compacted sorted runs.
    pub fn compacted(&self) -> &Vec<SortedRun> {
        &self.manifest.core.compacted
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
            .compacted
            .iter()
            .flat_map(|sr| sr.sst_views.iter().map(|s| s.sst.id))
            .chain(parent_manifest.core.l0.iter().map(|s| s.sst.id))
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
        for sorter_run in &projected.core.compacted {
            let sst_views = Self::filter_view_handles(&sorter_run.sst_views, false, &range);
            if !sst_views.is_empty() {
                sorter_runs_filtered.push(SortedRun {
                    id: sorter_run.id,
                    sst_views,
                });
            }
        }
        projected.core.l0 = Self::filter_view_handles(&projected.core.l0, true, &range).into();
        projected.core.compacted = sorter_runs_filtered;
        // drop unused external_dbs
        let used_sst_ids: HashSet<SsTableId> = projected
            .core
            .compacted
            .iter()
            .flat_map(|sr| sr.sst_views.iter().map(|v| v.sst.id))
            .chain(projected.core.l0.iter().map(|v| v.sst.id))
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

    #[allow(unused)]
    pub(crate) fn union(manifests: Vec<Manifest>) -> Manifest {
        if manifests.len() == 1 {
            manifests[0].clone()
        } else {
            let mut ranges = vec![];
            for manifest in &manifests {
                let range = manifest.range();
                if let Some(range) = range {
                    ranges.push((manifest, range));
                } else {
                    warn!("manifest has no SST files [manifest={:?}]", manifest);
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

            for (manifest, _) in ranges {
                // First, we need to add all the external dbs
                external_dbs.extend_from_slice(&manifest.external_dbs);
                // Then, we can add all the l0 ssts
                for sst in &manifest.core.l0 {
                    core.l0.push_back(sst.clone());
                }
                // Finally, we can add all the sorted runs
                for sorted_run in &manifest.core.compacted {
                    core.compacted.push(sorted_run.clone());
                }
            }

            // Renumber sorted runs to ensure sequential IDs without duplicates
            for (idx, sorted_run) in core.compacted.iter_mut().enumerate() {
                sorted_run.id = idx as u32;
            }

            Self {
                external_dbs,
                core,
                writer_epoch: 0,
                compactor_epoch: 0,
            }
        }
    }

    fn range(&self) -> Option<BytesRange> {
        let mut start_bound = None;
        let mut end_bound = None;
        let all_views = self.core.l0.iter().chain(
            self.core
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

    pub(crate) fn has_wal_sst_reference(&self, wal_sst_id: u64) -> bool {
        wal_sst_id > self.core.replay_after_wal_id && wal_sst_id < self.core.next_wal_sst_id
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};

    use super::{ExternalDb, Manifest};
    use crate::config::CheckpointOptions;
    use crate::db_state::{SortedRun, SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::ManifestCore;
    use crate::rand::DbRand;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use rstest::rstest;
    use std::collections::HashMap;
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
        let clone_stored_manifest = StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_manifest.manifest(),
            parent_path.to_string(),
            checkpoint.id,
            Arc::new(DbRand::default()),
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
        let manifests: Vec<Manifest> = test_case
            .manifests
            .iter()
            .map(|m| {
                build_manifest(m, |alias| {
                    if let Some(sst_id) = sst_ids.get(alias) {
                        *sst_id
                    } else {
                        let sst_id = SsTableId::Compacted(Ulid::new());
                        sst_ids.insert(alias.to_string(), sst_id);
                        sst_id
                    }
                })
            })
            .collect();

        let expected_manifest =
            build_manifest(&test_case.expected, |alias| *sst_ids.get(alias).unwrap());

        let union = Manifest::union(manifests);

        assert_manifest_equal(&union, &expected_manifest, &sst_ids);
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

        let union = Manifest::union(vec![manifest1, manifest2]);

        // After union, we should have 5 SRs with IDs 0, 1, 2, 3, 4
        assert_eq!(union.core.compacted.len(), 5);

        let sr_ids: Vec<u32> = union.core.compacted.iter().map(|sr| sr.id).collect();
        assert_eq!(sr_ids, vec![0, 1, 2, 3, 4], "SR IDs should be sequential");

        // Verify no duplicates
        let mut seen = std::collections::HashSet::new();
        for id in &sr_ids {
            assert!(seen.insert(id), "Duplicate SR ID: {}", id);
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
            core.l0.push_back(SsTableView::new_projected(
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
            core.compacted.push(SortedRun {
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

        if actual.core.l0 != expected.core.l0 {
            let mut error_msg = String::from("Manifest L0 mismatch.\n\nActual: \n");

            // Format actual L0 entries
            for (idx, handle) in actual.core.l0.iter().enumerate() {
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

                let result = if expected.core.l0.get(idx) == Some(handle) {
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
            for (idx, handle) in expected.core.l0.iter().enumerate() {
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
            actual.core.compacted, expected.core.compacted,
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

        core.l0.push_back(create_sst_view(sst_id_1, b"a")); // inside projection_range
        core.l0.push_back(create_sst_view(sst_id_2, b"c")); // outside projection_range
        core.l0.push_back(create_sst_view(sst_id_3, b"d")); // outside projection_range
        core.l0.push_back(create_sst_view(sst_id_4, b"e")); // outside projection_range

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
}
