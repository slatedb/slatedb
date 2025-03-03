use std::collections::HashSet;
use std::fmt::Debug;

use crate::bytes_range::BytesRange;
use crate::db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use bytes::Bytes;
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
    ) -> Self {
        let mut parent_external_sst_ids = HashSet::<SsTableId>::new();
        let mut clone_external_dbs = vec![];

        for parent_external_db in &parent_manifest.external_dbs {
            parent_external_sst_ids.extend(&parent_external_db.sst_ids);
            clone_external_dbs.push(ExternalDb {
                path: parent_external_db.path.clone(),
                source_checkpoint_id: parent_external_db.source_checkpoint_id,
                final_checkpoint_id: Some(crate::utils::uuid()),
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
            final_checkpoint_id: Some(crate::utils::uuid()),
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

    // /// Compacts a set of projections into a minimal set of non-overlapping projections.
    // ///
    // /// Given a set of projections, this method will merge any overlapping projections into a single
    // /// projection. The result will be a set of non-overlapping projections covering the same range as
    // /// the input projections.
    // ///
    // /// For example, given the following projections:
    // ///
    // /// | Range          | SST IDs |
    // /// | -------------- | ------- |
    // /// | [0, 10)        | [A, B]  |
    // /// | [10, 15)       | [B, C]  |
    // /// | [12, 20)       | [C, D]  |
    // /// | [30, 40)       | [D, F]  |
    // ///
    // /// The resulting compacted projections will be:
    // ///
    // /// | Range          | SST IDs      |
    // /// | -------------- | ------------ |
    // /// | [0, 20)        | [A, B, C, D] |
    // /// | [30, 40)       | [D, F]       |
    // ///
    // /// Note that the order of the SST IDs in the resulting projections does not matter.
    // ///
    // /// This method is idempotent, meaning that calling it multiple times on the same input will
    // /// result in the same output.
    // pub(crate) fn compact_projections(
    //     projections: &BTreeMap<BytesRange, Vec<SsTableId>>,
    // ) -> Vec<Projection> {
    //     let mut final_projections = vec![];

    //     let mut candidate_range: Option<BytesRange> = None;
    //     let mut candidate_ssts_unordered = HashSet::new();
    //     let mut candidate_ssts_ordered: Vec<SsTableId> = vec![];

    //     macro_rules! reset_candidate {
    //         ($range: expr, $sst_ids: expr) => {
    //             candidate_ssts_unordered.clear();
    //             candidate_ssts_ordered.clear();
    //             candidate_range = Some($range);
    //             candidate_ssts_unordered.extend($sst_ids);
    //             candidate_ssts_ordered.extend($sst_ids);
    //         };
    //     }

    //     for (current_range, current_sst_ids) in projections.iter() {
    //         if let Some(range) = candidate_range {
    //             if let Some(compacted_range) = range.union(current_range) {
    //                 candidate_range = Some(compacted_range);
    //                 for sst_id in current_sst_ids {
    //                     if candidate_ssts_unordered.insert(*sst_id) {
    //                         candidate_ssts_ordered.push(*sst_id);
    //                     }
    //                 }
    //             } else {
    //                 // The current range does not overlap with the candidate range, so we promote
    //                 // the candidate to the final projections and create a new candidate.
    //                 final_projections.push(Projection {
    //                     visible_range: range,
    //                     sst_ids: candidate_ssts_ordered.clone(),
    //                 });
    //                 reset_candidate!(current_range.clone(), current_sst_ids);
    //             }
    //         } else {
    //             // The current range is the first range, so we set it as the candidate.
    //             reset_candidate!(current_range.clone(), current_sst_ids);
    //         }
    //     }
    //     // Promote the final candidate, if any.
    //     if let Some(range) = candidate_range {
    //         final_projections.push(Projection {
    //             visible_range: range,
    //             sst_ids: candidate_ssts_ordered.clone(),
    //         });
    //     }
    //     final_projections
    // }

    pub(crate) fn merged(_manifests: Vec<Manifest>) -> Manifest {
        todo!()
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
        wal_sst_id > self.core.replay_after_wal_id && wal_sst_id < self.core.next_wal_sst_id
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::manifest::store::{ManifestStore, StoredManifest};

    use crate::config::CheckpointOptions;
    use crate::db_state::{CoreDbState, SortedRun, SsTableHandle, SsTableId, SsTableInfo};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::ops::{Range, RangeBounds};
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
            .write_checkpoint(None, &CheckpointOptions::default())
            .await
            .unwrap();

        let clone_path = Path::from("/tmp/test_clone");
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let clone_stored_manifest = StoredManifest::create_uninitialized_clone(
            Arc::clone(&clone_manifest_store),
            parent_manifest.manifest(),
            parent_path.to_string(),
            checkpoint.id,
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
            .write_checkpoint(None, &CheckpointOptions::default())
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
        let mut core = CoreDbState::new();

        let mut sst_ids = HashMap::new();
        for entry in test_case.existing_manifest.l0 {
            let sst_id = SsTableId::Compacted(Ulid::new());
            if let Some(_) = sst_ids.insert(entry.sst_alias, sst_id) {
                unreachable!("duplicate sst alias: {}", entry.sst_alias);
            }
            core.l0.push_back(SsTableHandle::new_compacted(
                sst_id,
                SsTableInfo {
                    first_key: Some(entry.first_key),
                    ..SsTableInfo::default()
                },
                entry.visible_range.clone(),
            ));
        }
        for (idx, sorted_run) in test_case.existing_manifest.sorted_runs.iter().enumerate() {
            let mut sorted_run_ssts = Vec::new();
            for entry in sorted_run {
                let sst_id = SsTableId::Compacted(Ulid::new());
                if let Some(_) = sst_ids.insert(entry.sst_alias, sst_id) {
                    unreachable!("duplicate sst alias")
                }
                sorted_run_ssts.push(SsTableHandle::new_compacted(
                    sst_id,
                    SsTableInfo {
                        first_key: Some(entry.first_key.clone()),
                        ..SsTableInfo::default()
                    },
                    entry.visible_range.clone(),
                ));
            }
            core.compacted.push(SortedRun {
                id: idx as u32,
                ssts: sorted_run_ssts,
            });
        }

        let initial_manifest = Manifest::initial(core);

        let projected = Manifest::projected(
            &initial_manifest,
            BytesRange::from_ref(test_case.visible_range),
        );

        let expected_l0: Vec<SsTableHandle> = test_case
            .expected_manifest
            .l0
            .iter()
            .map(|entry| {
                SsTableHandle::new_compacted(
                    sst_ids.get(entry.sst_alias).unwrap().clone(),
                    SsTableInfo {
                        first_key: Some(entry.first_key.clone()),
                        ..SsTableInfo::default()
                    },
                    entry.visible_range.clone(),
                )
            })
            .collect();

        let expected_sorted_runs: Vec<SortedRun> = test_case
            .expected_manifest
            .sorted_runs
            .iter()
            .enumerate()
            .map(|(idx, sst_entries)| SortedRun {
                id: idx as u32,
                ssts: sst_entries
                    .iter()
                    .map(|entry| {
                        SsTableHandle::new_compacted(
                            sst_ids.get(entry.sst_alias).unwrap().clone(),
                            SsTableInfo {
                                first_key: Some(entry.first_key.clone()),
                                ..SsTableInfo::default()
                            },
                            entry.visible_range.clone(),
                        )
                    })
                    .collect(),
            })
            .collect();

        assert_eq!(projected.core.l0, expected_l0, "L0s do not match.");
        assert_eq!(
            projected.core.compacted, expected_sorted_runs,
            "Sorted runs do not match."
        );
    }
}
