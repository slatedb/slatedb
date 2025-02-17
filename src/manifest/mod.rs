use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::bytes_range::{self, BytesRange};
use crate::db_state::{CoreDbState, SsTableId};
use crate::error::SlateDBError;
use bytes::Bytes;
use serde::Serialize;
use uuid::Uuid;

pub(crate) mod store;

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Manifest {
    pub(crate) external_dbs: Vec<ExternalDb>,
    pub(crate) projections: Vec<Projection>,
    pub(crate) core: CoreDbState,
    pub(crate) writer_epoch: u64,
    pub(crate) compactor_epoch: u64,
}

impl Manifest {
    pub(crate) fn initial(core: CoreDbState) -> Self {
        Self {
            external_dbs: vec![],
            projections: vec![],
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
                final_checkpoint_id: Some(Uuid::new_v4()),
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
            final_checkpoint_id: Some(Uuid::new_v4()),
            sst_ids: parent_owned_sst_ids,
        });

        Self {
            external_dbs: clone_external_dbs,
            projections: parent_manifest.projections.clone(),
            core: parent_manifest.core.init_clone_db(),
            writer_epoch: parent_manifest.writer_epoch,
            compactor_epoch: parent_manifest.compactor_epoch,
        }
    }

    pub(crate) fn projected(source_manifest: &Manifest, range: BytesRange) -> Manifest {
        let mut projected = source_manifest.clone();

        let mut projected_ssts = HashMap::new();
        for source_projection in &source_manifest.projections {
            let intersection = bytes_range::intersect(&source_projection.visible_range, &range);
            for sst_id in &source_projection.sst_ids {
                projected_ssts.insert(*sst_id, intersection.clone());
            }
        }

        let mut new_projections = BTreeMap::new();
        let mut l0_filtered = VecDeque::new();
        for (idx, current_handle) in projected.core.l0.iter().enumerate() {
            let range_for_handle = match projected_ssts.get(&current_handle.id) {
                Some(existing_projection) => match existing_projection {
                    Some(range) => range.clone(),
                    None => {
                        // We need to filter out this SST because it has been projected out.
                        continue;
                    }
                },
                None => range.clone(),
            };

            let next_handle = projected.core.l0.get(idx + 1);
            if current_handle.has_nonempty_intersection(next_handle, range_for_handle.as_ref()) {
                // We need to keep this SST because it has a non-empty intersection with the new and previous projections.
                l0_filtered.push_back(current_handle.clone());
                new_projections
                    .entry(range_for_handle.clone())
                    .or_insert(vec![])
                    .push(current_handle.id);
            }
        }

        let mut merged_projections = vec![];
        let mut previous: Option<BytesRange> = None;
        let mut sst_ids: Vec<SsTableId> = vec![];
        for (current_range, current_sst_ids) in new_projections.iter() {
            if let Some(previous_range) = previous {
                if let Some(merged_range) = bytes_range::merge(&previous_range, current_range) {
                    previous = Some(merged_range);
                    sst_ids.extend(current_sst_ids);
                } else {
                    merged_projections.push(Projection {
                        visible_range: previous_range,
                        sst_ids: sst_ids.clone(),
                    });
                    previous = None;
                    sst_ids.clear();
                }
            } else {
                previous = Some(current_range.clone());
                sst_ids.extend(current_sst_ids);
            }
        }
        if let Some(previous_range) = previous {
            merged_projections.push(Projection {
                visible_range: previous_range,
                sst_ids: sst_ids.clone(),
            });
        }

        // merge overlapping ranges
        projected.projections = merged_projections;
        projected.core.l0 = l0_filtered;
        projected
    }

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

#[derive(Clone, Serialize, PartialEq, Debug)]
pub(crate) struct Projection {
    pub(crate) visible_range: BytesRange,
    pub(crate) sst_ids: Vec<SsTableId>,
}

pub(crate) trait ManifestCodec: Send + Sync {
    fn encode(&self, manifest: &Manifest) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<Manifest, SlateDBError>;
}

impl Manifest {
    pub(crate) fn has_wal_sst_reference(&self, wal_sst_id: u64) -> bool {
        wal_sst_id > self.core.last_compacted_wal_sst_id && wal_sst_id < self.core.next_wal_sst_id
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes_range::BytesRange;
    use crate::manifest::store::{ManifestStore, StoredManifest};

    use crate::config::CheckpointOptions;
    use crate::db_state::{CoreDbState, SsTableHandle, SsTableId, SsTableInfo};
    use crate::manifest::Projection;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::ops::Range;
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
        first_key: &'static str,
    }

    struct ProjectionEntry {
        range: Range<&'static str>,
        sst_aliases: Vec<&'static str>,
    }

    struct ProjectionTestCase {
        existing_l0: Vec<SstEntry>,
        existing_projections: Vec<ProjectionEntry>,
        new_range: Range<&'static str>,
        new_l0: Vec<SstEntry>,
        new_projections: Vec<ProjectionEntry>,
    }

    #[rstest]
    #[case(ProjectionTestCase {
        existing_l0: vec![
            SstEntry { sst_alias: "first", first_key: "a" },
            SstEntry { sst_alias: "second", first_key: "f" },
            SstEntry { sst_alias: "third", first_key: "m" },
        ],
        existing_projections: vec![
            ProjectionEntry { range: "m".."p", sst_aliases: vec!["third"] },
        ],
        new_range: "h".."o",
        new_l0: vec![
            SstEntry { sst_alias: "second", first_key: "f" },
            SstEntry { sst_alias: "third", first_key: "m" },
        ],
        new_projections: vec![
            ProjectionEntry { range: "h".."o", sst_aliases: vec!["second", "third"] },
        ],
    })]
    #[case(ProjectionTestCase {
        existing_l0: vec![
            SstEntry { sst_alias: "foo", first_key: "a" },
            SstEntry { sst_alias: "bar", first_key: "m" },
            SstEntry { sst_alias: "baz", first_key: "p" },
        ],
        existing_projections: vec![],
        new_range: "m".."p",
        new_l0: vec![
            SstEntry { sst_alias: "bar", first_key: "m" },
        ],
        new_projections: vec![
            ProjectionEntry { range: "m".."p", sst_aliases: vec!["bar"]},
        ],
    })]
    #[case::distinct_ranges(ProjectionTestCase {
        existing_l0: vec![
            SstEntry { sst_alias: "foo", first_key: "a" },
            SstEntry { sst_alias: "bar", first_key: "k" },
        ],
        existing_projections: vec![
            ProjectionEntry { range: "a".."d", sst_aliases: vec!["foo"]},
            ProjectionEntry { range: "f".."z", sst_aliases: vec!["foo", "bar"]},
        ],
        new_range: "c".."p",
        new_l0: vec![
            SstEntry { sst_alias: "foo", first_key: "a" },
            SstEntry { sst_alias: "bar", first_key: "k" },
        ],
        new_projections: vec![
            ProjectionEntry { range: "c".."d", sst_aliases: vec!["foo"]},
            ProjectionEntry { range: "f".."p", sst_aliases: vec!["foo", "bar"]},
        ],
    })]
    fn test_projected(#[case] test_case: ProjectionTestCase) {
        let mut core = CoreDbState::new();

        let mut sst_ids = HashMap::new();
        for entry in test_case.existing_l0 {
            let sst_id = SsTableId::Compacted(Ulid::new());
            if let Some(_) = sst_ids.insert(entry.sst_alias, sst_id) {
                unreachable!("duplicate sst alias")
            }
            core.l0.push_back(SsTableHandle::new(
                sst_id,
                SsTableInfo {
                    first_key: Some(Bytes::from_static(entry.first_key.as_bytes())),
                    index_offset: 0,
                    index_len: 0,
                    filter_offset: 0,
                    filter_len: 0,
                    compression_codec: None,
                },
            ));
        }

        let mut initial_manifest = Manifest::initial(core);
        for entry in test_case.existing_projections {
            initial_manifest.projections.push(Projection {
                visible_range: BytesRange::from_ref(entry.range),
                sst_ids: to_sst_ids(&entry.sst_aliases, &sst_ids),
            });
        }

        let projected =
            Manifest::projected(&initial_manifest, BytesRange::from_ref(test_case.new_range));

        let expected_projections: Vec<Projection> = test_case
            .new_projections
            .iter()
            .map(|entry| Projection {
                visible_range: BytesRange::from_ref(entry.range.clone()),
                sst_ids: to_sst_ids(&entry.sst_aliases, &sst_ids),
            })
            .collect();

        let expected_l0: Vec<SsTableHandle> = test_case
            .new_l0
            .iter()
            .map(|entry| {
                SsTableHandle::new(
                    sst_ids.get(entry.sst_alias).unwrap().clone(),
                    SsTableInfo {
                        first_key: Some(Bytes::from_static(entry.first_key.as_bytes())),
                        index_offset: 0,
                        index_len: 0,
                        filter_offset: 0,
                        filter_len: 0,
                        compression_codec: None,
                    },
                )
            })
            .collect();

        assert_eq!(projected.projections, expected_projections);
        assert_eq!(projected.core.l0, expected_l0);
    }

    fn to_sst_ids(
        entries: &Vec<&'static str>,
        lookup_table: &HashMap<&'static str, SsTableId>,
    ) -> Vec<SsTableId> {
        entries
            .iter()
            .map(|entry| lookup_table.get(*entry).unwrap().clone())
            .collect()
    }
}
