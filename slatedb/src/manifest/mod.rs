use std::cmp::{max, min};
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

use crate::bytes_range::BytesRange;
use crate::db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::rand::DbRand;
use crate::utils::IdGenerator;
use bytes::Bytes;
use log::warn;
use serde::Serialize;
use uuid::Uuid;

pub(crate) mod store;

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Manifest {
    pub(crate) external_dbs: Vec<ExternalDb>,
    pub(crate) core: CoreDbState,
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
}

impl Manifest {
    pub(crate) fn initial(core: CoreDbState) -> Self {
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
        projected.core.l0 = Self::filter_sst_handles(&projected.core.l0, true, &range).into();
        projected.core.compacted = sorter_runs_filtered;
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
            let mut core = CoreDbState::new();

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
        for sst in &self.core.l0 {
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

pub(crate) trait ManifestCodec: Send + Sync {
    fn encode(&self, manifest: &Manifest) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError>;
}

impl Manifest {
    pub(crate) fn has_wal_sst_reference(&self, wal_sst_id: u64) -> bool {
        wal_sst_id > self.core.replay_after_wal_id && wal_sst_id < self.core.last_seen_wal_id
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::manifest::store::{ManifestStore, StoredManifest};

    use crate::config::CheckpointOptions;
    use crate::db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
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

    use super::Manifest;

    #[tokio::test]
    async fn test_init_clone_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parent_path = Path::from("/tmp/test_parent");
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_manifest =
            StoredManifest::create_new_db(parent_manifest_store, CoreDbState::new())
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

        let path = Path::from("/tmp/test_db");
        let manifest_store = Arc::new(ManifestStore::new(&path, object_store.clone()));
        let mut manifest =
            StoredManifest::create_new_db(Arc::clone(&manifest_store), CoreDbState::new())
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
        first_key: Bytes,
        visible_range: Option<BytesRange>,
    }

    impl SstEntry {
        fn regular(sst_alias: &'static str, first_key: &'static str) -> Self {
            Self {
                sst_alias,
                first_key: Bytes::copy_from_slice(first_key.as_bytes()),
                visible_range: None,
            }
        }

        fn projected<T>(sst_alias: &'static str, first_key: &'static str, visible_range: T) -> Self
        where
            T: RangeBounds<&'static str>,
        {
            Self {
                sst_alias,
                first_key: Bytes::copy_from_slice(first_key.as_bytes()),
                visible_range: Some(BytesRange::from_ref(visible_range)),
            }
        }
    }

    struct SimpleManifest {
        l0: Vec<SstEntry>,
        sorted_runs: Vec<Vec<SstEntry>>,
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

    fn build_manifest<F>(manifest: &SimpleManifest, mut sst_id_fn: F) -> Manifest
    where
        F: FnMut(&str) -> SsTableId,
    {
        let mut core = CoreDbState::new();
        for entry in &manifest.l0 {
            core.l0.push_back(SsTableHandle::new_compacted(
                sst_id_fn(entry.sst_alias),
                SsTableInfo {
                    first_key: Some(entry.first_key.clone()),
                    ..SsTableInfo::default()
                },
                entry.visible_range.clone(),
            ));
        }
        for (idx, sorted_run) in manifest.sorted_runs.iter().enumerate() {
            core.compacted.push(SortedRun {
                id: idx as u32,
                ssts: sorted_run
                    .iter()
                    .map(|entry| {
                        SsTableHandle::new_compacted(
                            sst_id_fn(entry.sst_alias),
                            SsTableInfo {
                                first_key: Some(entry.first_key.clone()),
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

                let first_key = handle
                    .info
                    .first_key
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
                    "{}. {} (first_key: {}, visible_range: {}){}\n",
                    idx + 1,
                    id_str,
                    first_key,
                    visible_range,
                    result
                ));
            }

            error_msg.push_str("\nExpected: \n");

            // Format expected L0 entries
            for (idx, handle) in expected.core.l0.iter().enumerate() {
                let id_str = sst_aliases.get(&handle.id).unwrap();

                let first_key = handle
                    .info
                    .first_key
                    .as_ref()
                    .map(|k| format!("{:?}", k))
                    .unwrap();

                let visible_range = handle
                    .visible_range
                    .as_ref()
                    .map(format_range)
                    .unwrap_or_else(|| "None".to_string());

                error_msg.push_str(&format!(
                    "{}. {} (first_key: {}, visible_range: {})\n",
                    idx + 1,
                    id_str,
                    first_key,
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
}
