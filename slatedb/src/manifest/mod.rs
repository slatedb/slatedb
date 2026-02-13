use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::rand::DbRand;
use crate::utils::IdGenerator;
use bytes::Bytes;
use log::warn;
use serde::Serialize;
use uuid::Uuid;

pub(crate) mod store;

// TODO: should probably move these into manifest/mod.rs (this file)
pub use crate::db_state::{ManifestCore, SortedRun, SsTableHandle, SsTableId, SsTableInfo};

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Manifest {
    // todo: try to make this writable only from module
    pub(crate) external_dbs: Vec<ExternalDb>,
    pub(crate) core: ManifestCore,
    // todo: try to make this writable only from module
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
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
            .flat_map(|sr| sr.ssts.iter().map(|s| s.id))
            .chain(parent_manifest.core.l0.iter().map(|s| s.id))
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

    #[allow(unused)]
    pub(crate) fn projected(source_manifest: &Manifest, range: BytesRange) -> Manifest {
        let mut projected = source_manifest.clone();
        let mut sorter_runs_filtered = vec![];
        for sorter_run in &projected.core.compacted {
            sorter_runs_filtered.push(SortedRun {
                id: sorter_run.id,
                ssts: Self::filter_sst_handles(&sorter_run.ssts, false, &range),
            });
        }
        sorter_runs_filtered.retain(|sr| !sr.ssts.is_empty());
        projected.core.compacted = sorter_runs_filtered;
        projected.core.l0 = Self::filter_sst_handles(&projected.core.l0, true, &range).into();

        // Collect all SST IDs that remain after projection
        let remaining_sst_ids: HashSet<SsTableId> = projected
            .core
            .l0
            .iter()
            .chain(
                projected
                    .core
                    .compacted
                    .iter()
                    .flat_map(|sr| sr.ssts.iter()),
            )
            .map(|h| h.id)
            .collect();

        // Filter external_dbs entries to only include SST IDs still referenced,
        // and remove entries with no remaining SSTs so their checkpoints can be
        // released and the external database's SSTs garbage collected.
        for external_db in &mut projected.external_dbs {
            external_db
                .sst_ids
                .retain(|id| remaining_sst_ids.contains(id));
        }
        projected.external_dbs.retain(|db| !db.sst_ids.is_empty());

        projected
    }

    fn filter_sst_handles<'a, T>(
        handles: T,
        handles_overlap: bool,
        projection_range: &BytesRange,
    ) -> Vec<SsTableHandle>
    where
        T: IntoIterator<Item = &'a SsTableHandle>,
    {
        let mut iter = handles.into_iter().peekable();
        let mut filtered_handles = vec![];
        while let Some(current_handle) = iter.next() {
            let next_handle = if handles_overlap {
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
    pub(crate) fn union(sources: Vec<(Manifest, String, Uuid)>, rand: Arc<DbRand>) -> Manifest {
        let mut ranges = vec![];
        for (manifest, path, source_checkpoint_id) in &sources {
            let range = manifest.range();
            if let Some(range) = range {
                ranges.push((manifest, path, source_checkpoint_id, range));
            } else {
                warn!("manifest has no SST files [manifest={:?}]", manifest);
            }
        }
        ranges.sort_by_key(|(_, _, _, range)| range.comparable_start_bound().cloned());

        // Ensure manifests are non-overlapping
        let mut previous_range = None;
        for (_, _, _, range) in ranges.iter() {
            if let Some(previous_range) = previous_range {
                if range.intersect(previous_range).is_some() {
                    unreachable!("overlapping ranges found");
                }
            }
            previous_range = Some(range);
        }

        // Merge the contents of all input manifests
        let mut core = ManifestCore::new();

        // The resulting manifest is not yet initialized; the caller must
        // complete setup (e.g. creating final checkpoints) before setting
        // initialized to true.
        core.initialized = false;

        // Deduplicate external_dbs by (path, source_checkpoint_id), merging
        // sst_ids. Without dedup, repeated projection/union cycles cause
        // exponential growth of duplicated entries.
        let mut external_db_map: HashMap<(String, Uuid), ExternalDb> = HashMap::new();

        for (manifest, _, _, _) in &ranges {
            for db in &manifest.external_dbs {
                let key = (db.path.clone(), db.source_checkpoint_id);
                match external_db_map.get_mut(&key) {
                    Some(existing) => {
                        let existing_ids: HashSet<SsTableId> =
                            existing.sst_ids.iter().copied().collect();
                        let new_ids: Vec<SsTableId> = db
                            .sst_ids
                            .iter()
                            .filter(|id| !existing_ids.contains(id))
                            .copied()
                            .collect();
                        existing.sst_ids.extend(new_ids);
                    }
                    None => {
                        // Generate new final_checkpoint_ids — the old ones
                        // belong to the source databases' clone relationships,
                        // not to the new database.
                        external_db_map.insert(
                            key,
                            ExternalDb {
                                path: db.path.clone(),
                                source_checkpoint_id: db.source_checkpoint_id,
                                final_checkpoint_id: Some(rand.rng().gen_uuid()),
                                sst_ids: db.sst_ids.clone(),
                            },
                        );
                    }
                }
            }
        }
        // Add each source database as an external_dbs entry. The owned SST IDs
        // are the SSTs in the source manifest that are not already tracked by its
        // own external_dbs (i.e., SSTs physically stored under the source's path).
        for (manifest, path, source_checkpoint_id, _) in &ranges {
            let ancestor_sst_ids: HashSet<SsTableId> = manifest
                .external_dbs
                .iter()
                .flat_map(|db| db.sst_ids.iter().copied())
                .collect();
            let owned_sst_ids: Vec<SsTableId> = manifest
                .core
                .compacted
                .iter()
                .flat_map(|sr| sr.ssts.iter().map(|s| s.id))
                .chain(manifest.core.l0.iter().map(|s| s.id))
                .filter(|id| !ancestor_sst_ids.contains(id))
                .collect();
            let key = ((*path).clone(), **source_checkpoint_id);
            match external_db_map.get_mut(&key) {
                Some(existing) => {
                    let existing_ids: HashSet<SsTableId> =
                        existing.sst_ids.iter().copied().collect();
                    let new_ids: Vec<SsTableId> = owned_sst_ids
                        .into_iter()
                        .filter(|id| !existing_ids.contains(id))
                        .collect();
                    existing.sst_ids.extend(new_ids);
                }
                None => {
                    external_db_map.insert(
                        key,
                        ExternalDb {
                            path: (*path).clone(),
                            source_checkpoint_id: **source_checkpoint_id,
                            final_checkpoint_id: Some(rand.rng().gen_uuid()),
                            sst_ids: owned_sst_ids,
                        },
                    );
                }
            }
        }

        let external_dbs: Vec<ExternalDb> = external_db_map.into_values().collect();

        // Deduplicate L0 SSTs by ID, merging visible_ranges. Projection
        // includes the same L0 SST in multiple projected manifests with
        // different visible_ranges, so duplicates must be combined.
        let mut l0_by_id: HashMap<SsTableId, SsTableHandle> = HashMap::new();
        for (manifest, _, _, _) in &ranges {
            for sst in &manifest.core.l0 {
                match l0_by_id.get(&sst.id) {
                    Some(existing) => {
                        let merged_range = match (&existing.visible_range, &sst.visible_range) {
                            (Some(a), Some(b)) => Some(a.union(b).unwrap_or_else(|| {
                                panic!("L0 SST {:?} has non-contiguous visible_ranges", sst.id)
                            })),
                            // If either has no visible_range, the merged result covers everything
                            _ => None,
                        };
                        l0_by_id.insert(
                            sst.id,
                            SsTableHandle::new_compacted(sst.id, sst.info.clone(), merged_range),
                        );
                    }
                    None => {
                        l0_by_id.insert(sst.id, sst.clone());
                    }
                }
            }
        }

        // Sort L0 SSTs by ULID descending (newest first) to preserve
        // temporal ordering for correct point-lookup behavior.
        let mut l0_list: Vec<SsTableHandle> = l0_by_id.into_values().collect();
        l0_list.sort_by(|a, b| b.id.unwrap_compacted_id().cmp(&a.id.unwrap_compacted_id()));
        core.l0 = l0_list.into();

        // Merge sorted runs by tier: sorted runs with the same ID across
        // input manifests are combined into a single sorted run. We use
        // the SR ID (not position) to match tiers, since projection can
        // remove sorted runs and shift positions.
        let mut merged_by_id: BTreeMap<u32, Vec<SsTableHandle>> = BTreeMap::new();
        for (manifest, _, _, _) in &ranges {
            for sorted_run in &manifest.core.compacted {
                merged_by_id
                    .entry(sorted_run.id)
                    .or_default()
                    .extend(sorted_run.ssts.iter().cloned());
            }
        }

        // Collect sorted runs ordered by ID descending (newest first),
        // preserving the original IDs.
        for (id, ssts) in merged_by_id.into_iter().rev() {
            core.compacted.push(SortedRun { id, ssts });
        }

        Self {
            external_dbs,
            core,
            writer_epoch: 0,
            compactor_epoch: 0,
        }
    }

    fn range(&self) -> Option<BytesRange> {
        let mut start_bound = None;
        let mut end_bound = None;
        let all_ssts = self
            .core
            .l0
            .iter()
            .chain(self.core.compacted.iter().flat_map(|sr| sr.ssts.iter()));
        for sst in all_ssts {
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
pub(crate) struct ExternalDb {
    pub(crate) path: String,
    pub(crate) source_checkpoint_id: Uuid,
    pub(crate) final_checkpoint_id: Option<Uuid>,
    pub(crate) sst_ids: Vec<SsTableId>,
}

impl Manifest {
    pub(crate) fn has_wal_sst_reference(&self, wal_sst_id: u64) -> bool {
        wal_sst_id > self.core.replay_after_wal_id && wal_sst_id < self.core.next_wal_sst_id
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};

    use super::Manifest;
    use crate::config::CheckpointOptions;
    use crate::db_state::{ManifestCore, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
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

        let latest_manifest_id = manifest_store.read_latest_manifest().await.unwrap().0;
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

    struct SimpleSortedRun {
        id: Option<u32>,
        ssts: Vec<SstEntry>,
    }

    impl SimpleSortedRun {
        fn new(ssts: Vec<SstEntry>) -> Self {
            Self { id: None, ssts }
        }

        fn with_id(id: u32, ssts: Vec<SstEntry>) -> Self {
            Self { id: Some(id), ssts }
        }
    }

    struct SimpleManifest {
        l0: Vec<SstEntry>,
        sorted_runs: Vec<SimpleSortedRun>,
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
                SimpleSortedRun::new(vec![
                    SstEntry::regular("sr0_first", "a"),
                ]),
                SimpleSortedRun::new(vec![
                    SstEntry::regular("sr1_first", "a"),
                    SstEntry::regular("sr1_second", "f"),
                    SstEntry::regular("sr1_third", "m"),
                ]),
            ],
        },
        expected_manifest: SimpleManifest {
            l0: vec![
                SstEntry::projected("first", "a", "h".."o"),
                SstEntry::projected("second", "f", "h".."o"),
                SstEntry::projected("third", "m", "m".."o"),
            ],
            sorted_runs: vec![
                SimpleSortedRun::new(vec![
                    // We can't filter this one out, because we don't know the
                    // end key, so it might still fall within the range
                    SstEntry::projected("sr0_first", "a", "h".."o"),
                ]),
                SimpleSortedRun::new(vec![
                    SstEntry::projected("sr1_second", "f", "h".."m"),
                    SstEntry::projected("sr1_third", "m", "m".."o"),
                ]),
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
    #[case::empty_sorted_run_removed(ProjectionTestCase {
        visible_range: "m".."z",
        existing_manifest: SimpleManifest {
            l0: vec![],
            sorted_runs: vec![
                // This sorted run has keys entirely before the projection range
                SimpleSortedRun::new(vec![
                    SstEntry::projected("sr0_first", "a", "a".."d"),
                    SstEntry::projected("sr0_second", "d", "d".."g"),
                    SstEntry::projected("sr0_third", "g", "g".."k"),
                ]),
                // This sorted run spans the projection range
                SimpleSortedRun::new(vec![
                    SstEntry::regular("sr1_first", "a"),
                    SstEntry::regular("sr1_second", "m"),
                ]),
            ],
        },
        expected_manifest: SimpleManifest {
            l0: vec![],
            sorted_runs: vec![
                // sr0 is removed entirely because all SSTs end before "m"
                // sr1 retains sr1_second (preserves original ID 1)
                SimpleSortedRun::with_id(1, vec![
                    SstEntry::projected("sr1_second", "m", "m".."z"),
                ]),
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
    #[case::l0_deduplicated_and_sorted(UnionTestCase {
        // Same L0 SSTs appear in both manifests with adjacent visible_ranges.
        // After dedup, they're merged into single entries and sorted by ULID
        // descending. ULIDs are created in order foo < bar < baz, so descending
        // gives baz, bar, foo.
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
                SstEntry::projected("baz", "j", "a"..),
                SstEntry::projected("bar", "f", "a"..),
                SstEntry::projected("foo", "a", "a"..),
            ],
            sorted_runs: vec![]
        },
    })]
    #[case::l0_unique_per_manifest(UnionTestCase {
        // L0 SSTs are unique to each manifest (no dedup needed), just sorted
        // by ULID descending. ULIDs created in order: foo, bar (manifest 1),
        // then baz, qux (manifest 2). Descending: qux, baz, bar, foo.
        manifests: vec![
            SimpleManifest {
                l0: vec![
                    SstEntry::projected("foo", "a", "a".."m"),
                    SstEntry::projected("bar", "f", "a".."m"),
                ],
                sorted_runs: vec![]
            },
            SimpleManifest {
                l0: vec![
                    SstEntry::projected("baz", "m", "m"..),
                    SstEntry::projected("qux", "p", "m"..),
                ],
                sorted_runs: vec![]
            }
        ],
        expected: SimpleManifest {
            l0: vec![
                SstEntry::projected("qux", "p", "m"..),
                SstEntry::projected("baz", "m", "m"..),
                SstEntry::projected("bar", "f", "a".."m"),
                SstEntry::projected("foo", "a", "a".."m"),
            ],
            sorted_runs: vec![]
        },
    })]
    #[case::sorted_runs_merged_by_tier(UnionTestCase {
        manifests: vec![
            SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    // Newest tier has highest ID (compactor convention)
                    SimpleSortedRun::with_id(2, vec![
                        SstEntry::projected("sr0_a", "a", "a".."m"),
                        SstEntry::projected("sr0_b", "d", "a".."m"),
                    ]),
                    SimpleSortedRun::with_id(1, vec![
                        SstEntry::projected("sr1_a", "b", "a".."m"),
                    ]),
                ],
            },
            SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    SimpleSortedRun::with_id(2, vec![
                        SstEntry::projected("sr0_c", "m", "m"..),
                    ]),
                    SimpleSortedRun::with_id(1, vec![
                        SstEntry::projected("sr1_b", "n", "m"..),
                        SstEntry::projected("sr1_c", "p", "m"..),
                    ]),
                ],
            },
        ],
        expected: SimpleManifest {
            l0: vec![],
            sorted_runs: vec![
                // Tier with original ID 2: merged
                SimpleSortedRun::with_id(2, vec![
                    SstEntry::projected("sr0_a", "a", "a".."m"),
                    SstEntry::projected("sr0_b", "d", "a".."m"),
                    SstEntry::projected("sr0_c", "m", "m"..),
                ]),
                // Tier with original ID 1: merged
                SimpleSortedRun::with_id(1, vec![
                    SstEntry::projected("sr1_a", "b", "a".."m"),
                    SstEntry::projected("sr1_b", "n", "m"..),
                    SstEntry::projected("sr1_c", "p", "m"..),
                ]),
            ],
        },
    })]
    #[case::sorted_runs_uneven_tiers(UnionTestCase {
        manifests: vec![
            SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    SimpleSortedRun::with_id(3, vec![
                        SstEntry::projected("sr0_a", "a", "a".."m"),
                    ]),
                    SimpleSortedRun::with_id(2, vec![
                        SstEntry::projected("sr1_a", "c", "a".."m"),
                    ]),
                    SimpleSortedRun::with_id(1, vec![
                        SstEntry::projected("sr2_a", "e", "a".."m"),
                    ]),
                ],
            },
            SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    // Only has the newest tier (ID 3)
                    SimpleSortedRun::with_id(3, vec![
                        SstEntry::projected("sr0_b", "m", "m"..),
                    ]),
                ],
            },
        ],
        expected: SimpleManifest {
            l0: vec![],
            sorted_runs: vec![
                SimpleSortedRun::with_id(3, vec![
                    SstEntry::projected("sr0_a", "a", "a".."m"),
                    SstEntry::projected("sr0_b", "m", "m"..),
                ]),
                SimpleSortedRun::with_id(2, vec![
                    SstEntry::projected("sr1_a", "c", "a".."m"),
                ]),
                SimpleSortedRun::with_id(1, vec![
                    SstEntry::projected("sr2_a", "e", "a".."m"),
                ]),
            ],
        },
    })]
    #[case::sorted_runs_matched_by_id(UnionTestCase {
        // Simulates union after projection where some sorted runs were
        // removed from one manifest but not the other, shifting positions.
        manifests: vec![
            SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    // Has SR IDs 2, 1, 0 (3 tiers)
                    SimpleSortedRun::with_id(2, vec![
                        SstEntry::projected("sr2_a", "a", "a".."m"),
                    ]),
                    SimpleSortedRun::with_id(1, vec![
                        SstEntry::projected("sr1_a", "c", "a".."m"),
                    ]),
                    SimpleSortedRun::with_id(0, vec![
                        SstEntry::projected("sr0_a", "e", "a".."m"),
                    ]),
                ],
            },
            SimpleManifest {
                l0: vec![],
                sorted_runs: vec![
                    // Has SR IDs 2, 0 only (SR 1 was removed by projection)
                    // Position 0 here is SR ID 2, position 1 is SR ID 0
                    SimpleSortedRun::with_id(2, vec![
                        SstEntry::projected("sr2_b", "m", "m"..),
                    ]),
                    SimpleSortedRun::with_id(0, vec![
                        SstEntry::projected("sr0_b", "o", "m"..),
                    ]),
                ],
            },
        ],
        expected: SimpleManifest {
            l0: vec![],
            sorted_runs: vec![
                // SR 2: merged from both, original ID preserved
                SimpleSortedRun::with_id(2, vec![
                    SstEntry::projected("sr2_a", "a", "a".."m"),
                    SstEntry::projected("sr2_b", "m", "m"..),
                ]),
                // SR 1: only from first manifest, original ID preserved
                SimpleSortedRun::with_id(1, vec![
                    SstEntry::projected("sr1_a", "c", "a".."m"),
                ]),
                // SR 0: merged from both, original ID preserved
                SimpleSortedRun::with_id(0, vec![
                    SstEntry::projected("sr0_a", "e", "a".."m"),
                    SstEntry::projected("sr0_b", "o", "m"..),
                ]),
            ],
        },
    })]
    fn test_union(#[case] test_case: UnionTestCase) {
        let mut sst_ids: HashMap<String, SsTableId> = HashMap::new();
        // Use monotonically increasing timestamps for deterministic ULID ordering.
        // ULIDs created with Ulid::new() within the same millisecond have random
        // ordering, which makes test assertions on L0 sort order non-deterministic.
        let mut next_ts: u64 = 1000;
        let manifests: Vec<(Manifest, String, Uuid)> = test_case
            .manifests
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let manifest = build_manifest(m, |alias| {
                    if let Some(sst_id) = sst_ids.get(alias) {
                        *sst_id
                    } else {
                        let sst_id = SsTableId::Compacted(Ulid::from_parts(next_ts, 0));
                        next_ts += 1;
                        sst_ids.insert(alias.to_string(), sst_id);
                        sst_id
                    }
                });
                (manifest, format!("source_{}", i), Uuid::new_v4())
            })
            .collect();

        let expected_manifest =
            build_manifest(&test_case.expected, |alias| *sst_ids.get(alias).unwrap());

        let union = Manifest::union(manifests, Arc::new(DbRand::default()));

        assert_manifest_equal(&union, &expected_manifest, &sst_ids);
    }

    #[test]
    #[should_panic(expected = "non-contiguous visible_ranges")]
    fn test_union_fails_on_non_contiguous_l0_ranges() {
        let mut sst_ids: HashMap<String, SsTableId> = HashMap::new();
        let manifests = vec![
            SimpleManifest {
                l0: vec![SstEntry::projected("foo", "a", "a".."m")],
                sorted_runs: vec![],
            },
            SimpleManifest {
                // Gap between "m" and "o" -- not contiguous with "a".."m"
                l0: vec![SstEntry::projected("foo", "a", "o"..)],
                sorted_runs: vec![],
            },
        ];
        let manifests: Vec<(Manifest, String, Uuid)> = manifests
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
                (manifest, format!("source_{}", i), Uuid::new_v4())
            })
            .collect();

        Manifest::union(manifests, Arc::new(DbRand::default()));
    }

    fn build_manifest<F>(manifest: &SimpleManifest, mut sst_id_fn: F) -> Manifest
    where
        F: FnMut(&str) -> SsTableId,
    {
        let mut core = ManifestCore::new();
        for entry in &manifest.l0 {
            core.l0.push_back(SsTableHandle::new_compacted(
                sst_id_fn(entry.sst_alias),
                SsTableInfo {
                    first_entry: Some(entry.first_entry.clone()),
                    ..SsTableInfo::default()
                },
                entry.visible_range.clone(),
            ));
        }
        for (idx, sorted_run) in manifest.sorted_runs.iter().enumerate() {
            core.compacted.push(SortedRun {
                id: sorted_run.id.unwrap_or(idx as u32),
                ssts: sorted_run
                    .ssts
                    .iter()
                    .map(|entry| {
                        SsTableHandle::new_compacted(
                            sst_id_fn(entry.sst_alias),
                            SsTableInfo {
                                first_entry: Some(entry.first_entry.clone()),
                                ..SsTableInfo::default()
                            },
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
                    .get(&handle.id)
                    .map(|a| a.as_str())
                    .unwrap_or("UNKNOWN");

                let first_entry = handle
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
                let id_str = sst_aliases.get(&handle.id).unwrap();

                let first_entry = handle
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
    fn test_projected_filters_external_dbs() {
        use super::ExternalDb;

        // Build a manifest with 3 SSTs in a sorted run spanning a..z
        let sst_in_range = SsTableId::Compacted(Ulid::from_parts(1000, 0));
        let sst_out_of_range = SsTableId::Compacted(Ulid::from_parts(1001, 0));
        let sst_mixed_in = SsTableId::Compacted(Ulid::from_parts(1002, 0));
        let sst_mixed_out = SsTableId::Compacted(Ulid::from_parts(1003, 0));

        let mut core = ManifestCore::new();
        core.compacted.push(SortedRun {
            id: 0,
            ssts: vec![
                SsTableHandle::new_compacted(
                    sst_in_range,
                    SsTableInfo {
                        first_entry: Some(Bytes::from_static(b"d")),
                        ..SsTableInfo::default()
                    },
                    None,
                ),
                SsTableHandle::new_compacted(
                    sst_mixed_in,
                    SsTableInfo {
                        first_entry: Some(Bytes::from_static(b"h")),
                        ..SsTableInfo::default()
                    },
                    None,
                ),
                SsTableHandle::new_compacted(
                    sst_mixed_out,
                    SsTableInfo {
                        first_entry: Some(Bytes::from_static(b"p")),
                        ..SsTableInfo::default()
                    },
                    None,
                ),
                SsTableHandle::new_compacted(
                    sst_out_of_range,
                    SsTableInfo {
                        first_entry: Some(Bytes::from_static(b"u")),
                        ..SsTableInfo::default()
                    },
                    None,
                ),
            ],
        });

        let mut manifest = Manifest::initial(core);

        // ExternalDb fully in range
        let ext_in_range = ExternalDb {
            path: "db_in".to_string(),
            source_checkpoint_id: Uuid::new_v4(),
            final_checkpoint_id: Some(Uuid::new_v4()),
            sst_ids: vec![sst_in_range],
        };
        // ExternalDb fully out of range
        let ext_out_of_range = ExternalDb {
            path: "db_out".to_string(),
            source_checkpoint_id: Uuid::new_v4(),
            final_checkpoint_id: Some(Uuid::new_v4()),
            sst_ids: vec![sst_out_of_range],
        };
        // ExternalDb with mixed SSTs (one in range, one out)
        let ext_mixed = ExternalDb {
            path: "db_mixed".to_string(),
            source_checkpoint_id: Uuid::new_v4(),
            final_checkpoint_id: Some(Uuid::new_v4()),
            sst_ids: vec![sst_mixed_in, sst_mixed_out],
        };

        manifest.external_dbs = vec![
            ext_in_range.clone(),
            ext_out_of_range.clone(),
            ext_mixed.clone(),
        ];

        // Project to range d..p (keeps sst_in_range and sst_mixed_in)
        let projected = Manifest::projected(&manifest, BytesRange::from_ref("d".."p"));

        // The fully-out-of-range entry should be gone
        assert!(
            !projected.external_dbs.iter().any(|db| db.path == "db_out"),
            "ExternalDb with all SSTs out of range should be removed"
        );

        // The fully-in-range entry should be unchanged
        let in_range_entry = projected
            .external_dbs
            .iter()
            .find(|db| db.path == "db_in")
            .expect("ExternalDb fully in range should be present");
        assert_eq!(in_range_entry.sst_ids, vec![sst_in_range]);

        // The mixed entry should only retain the in-range SST
        let mixed_entry = projected
            .external_dbs
            .iter()
            .find(|db| db.path == "db_mixed")
            .expect("ExternalDb with some in-range SSTs should be present");
        assert_eq!(mixed_entry.sst_ids, vec![sst_mixed_in]);
    }

    /// Helper to create a manifest with a single compacted sorted run containing
    /// a single SST with a given first_entry key. The visible_range pins the SST
    /// to a specific key range so that multiple manifests can be non-overlapping
    /// (required by `union`).
    fn manifest_with_one_compacted_sst(
        sst_id: SsTableId,
        first_entry: &'static [u8],
        visible_range: BytesRange,
    ) -> Manifest {
        let mut core = ManifestCore::new();
        core.compacted.push(SortedRun {
            id: 0,
            ssts: vec![SsTableHandle::new_compacted(
                sst_id,
                SsTableInfo {
                    first_entry: Some(Bytes::from_static(first_entry)),
                    ..SsTableInfo::default()
                },
                Some(visible_range),
            )],
        });
        Manifest::initial(core)
    }

    #[test]
    fn test_union_deduplicates_external_dbs() {
        use super::ExternalDb;
        use std::collections::HashSet;

        let shared_path = "shared_ancestor".to_string();
        let shared_source_cp = Uuid::new_v4();
        let original_final_cp = Uuid::new_v4();

        let sst_a = SsTableId::Compacted(Ulid::from_parts(1000, 0));
        let sst_b = SsTableId::Compacted(Ulid::from_parts(1001, 0));
        let sst_c = SsTableId::Compacted(Ulid::from_parts(1002, 0));

        // Manifest 1: has sst_a, sst_b from the shared ancestor, owns sst_own1
        let sst_own1 = SsTableId::Compacted(Ulid::from_parts(2000, 0));
        let mut m1 =
            manifest_with_one_compacted_sst(sst_own1, b"a", BytesRange::from_ref("a".."m"));
        m1.external_dbs.push(ExternalDb {
            path: shared_path.clone(),
            source_checkpoint_id: shared_source_cp,
            final_checkpoint_id: Some(original_final_cp),
            sst_ids: vec![sst_a, sst_b],
        });

        // Manifest 2: has sst_b, sst_c from same shared ancestor, owns sst_own2
        let sst_own2 = SsTableId::Compacted(Ulid::from_parts(3000, 0));
        let mut m2 = manifest_with_one_compacted_sst(sst_own2, b"m", BytesRange::from_ref("m"..));
        m2.external_dbs.push(ExternalDb {
            path: shared_path.clone(),
            source_checkpoint_id: shared_source_cp,
            final_checkpoint_id: Some(original_final_cp),
            sst_ids: vec![sst_b, sst_c],
        });

        let sources = vec![
            (m1, "path1".to_string(), Uuid::new_v4()),
            (m2, "path2".to_string(), Uuid::new_v4()),
        ];

        let result = Manifest::union(sources, Arc::new(DbRand::default()));

        // Find the entry for the shared ancestor
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

        // The merged sst_ids should be the union of {sst_a, sst_b} and {sst_b, sst_c}
        let merged_ids: HashSet<SsTableId> = shared_entries[0].sst_ids.iter().copied().collect();
        let expected_ids: HashSet<SsTableId> = [sst_a, sst_b, sst_c].iter().copied().collect();
        assert_eq!(merged_ids, expected_ids);

        // The final_checkpoint_id should differ from the original
        assert_ne!(
            shared_entries[0].final_checkpoint_id,
            Some(original_final_cp),
            "final_checkpoint_id should be regenerated"
        );
        assert!(
            shared_entries[0].final_checkpoint_id.is_some(),
            "final_checkpoint_id should not be None"
        );
    }

    #[test]
    fn test_union_regenerates_checkpoint_id_for_unique_external_db() {
        use super::ExternalDb;

        let original_final_cp = Uuid::new_v4();
        let ancestor_path = "ancestor".to_string();
        let ancestor_source_cp = Uuid::new_v4();

        let sst_own1 = SsTableId::Compacted(Ulid::from_parts(1000, 0));
        let sst_own2 = SsTableId::Compacted(Ulid::from_parts(2000, 0));
        let sst_ext = SsTableId::Compacted(Ulid::from_parts(3000, 0));

        // Only m1 has the external_db entry — it won't be deduplicated,
        // but union should still regenerate its final_checkpoint_id.
        let mut m1 =
            manifest_with_one_compacted_sst(sst_own1, b"a", BytesRange::from_ref("a".."m"));
        m1.external_dbs.push(ExternalDb {
            path: ancestor_path.clone(),
            source_checkpoint_id: ancestor_source_cp,
            final_checkpoint_id: Some(original_final_cp),
            sst_ids: vec![sst_ext],
        });

        let m2 = manifest_with_one_compacted_sst(sst_own2, b"m", BytesRange::from_ref("m"..));

        let sources = vec![
            (m1, "path1".to_string(), Uuid::new_v4()),
            (m2, "path2".to_string(), Uuid::new_v4()),
        ];

        let result = Manifest::union(sources, Arc::new(DbRand::default()));

        let entry = result
            .external_dbs
            .iter()
            .find(|db| db.path == ancestor_path && db.source_checkpoint_id == ancestor_source_cp)
            .expect("External db entry should be present");

        assert_ne!(
            entry.final_checkpoint_id,
            Some(original_final_cp),
            "final_checkpoint_id should be regenerated even for non-deduplicated entries"
        );
        assert!(
            entry.final_checkpoint_id.is_some(),
            "final_checkpoint_id should not be None"
        );
    }

    #[test]
    fn test_union_tracks_sources_as_external_dbs() {
        let sst1 = SsTableId::Compacted(Ulid::from_parts(1000, 0));
        let sst2 = SsTableId::Compacted(Ulid::from_parts(2000, 0));

        let m1 = manifest_with_one_compacted_sst(sst1, b"a", BytesRange::from_ref("a".."m"));
        let m2 = manifest_with_one_compacted_sst(sst2, b"m", BytesRange::from_ref("m"..));

        let path1 = "source_db_1".to_string();
        let path2 = "source_db_2".to_string();
        let cp1 = Uuid::new_v4();
        let cp2 = Uuid::new_v4();

        let sources = vec![(m1, path1.clone(), cp1), (m2, path2.clone(), cp2)];

        let result = Manifest::union(sources, Arc::new(DbRand::default()));

        // Should have 2 external_dbs entries (one per source)
        assert_eq!(result.external_dbs.len(), 2);

        let entry1 = result
            .external_dbs
            .iter()
            .find(|db| db.path == path1 && db.source_checkpoint_id == cp1)
            .expect("Should have external_dbs entry for source 1");
        assert_eq!(entry1.sst_ids, vec![sst1]);
        assert!(entry1.final_checkpoint_id.is_some());

        let entry2 = result
            .external_dbs
            .iter()
            .find(|db| db.path == path2 && db.source_checkpoint_id == cp2)
            .expect("Should have external_dbs entry for source 2");
        assert_eq!(entry2.sst_ids, vec![sst2]);
        assert!(entry2.final_checkpoint_id.is_some());
    }

    #[test]
    fn test_union_sets_initialized_false() {
        let sst1 = SsTableId::Compacted(Ulid::from_parts(1000, 0));
        let sst2 = SsTableId::Compacted(Ulid::from_parts(2000, 0));

        let m1 = manifest_with_one_compacted_sst(sst1, b"a", BytesRange::from_ref("a".."m"));
        let m2 = manifest_with_one_compacted_sst(sst2, b"m", BytesRange::from_ref("m"..));

        // Both manifests start with initialized=true (from ManifestCore::new())
        assert!(m1.core.initialized);
        assert!(m2.core.initialized);

        let sources = vec![
            (m1, "path1".to_string(), Uuid::new_v4()),
            (m2, "path2".to_string(), Uuid::new_v4()),
        ];

        let result = Manifest::union(sources, Arc::new(DbRand::default()));

        assert!(
            !result.core.initialized,
            "Union result should have initialized=false"
        );
    }
}
