//! The layers of one batch: the L0 SSTs and the sorted runs of each segment.

use bytes::Bytes;
use futures::future::try_join_all;

use super::keys::{Batch, LayerEntries, OpenKey};
use super::sst::{ReadContext, SstRead};
use crate::bytes_range::BytesRange;
use crate::db_state::SortedRun;
use crate::error::SlateDBError;
use crate::manifest::{ManifestCore, Segment, SsTableView};
use crate::reader::SstTraceLevel;

/// One layer of one segment: an L0 SST or a sorted run.
pub(super) struct Layer {
    /// The index of the segment in the batch.
    segment: usize,
    /// The prefix of the segment, a routing hint for the object store.
    prefix: Bytes,
    kind: LayerKind,
}

pub(super) enum LayerKind {
    L0(Box<SsTableView>),
    SortedRun(SortedRun),
}

impl Layer {
    pub(super) fn segment(&self) -> usize {
        self.segment
    }

    fn level(&self) -> SstTraceLevel {
        match &self.kind {
            LayerKind::L0(_) => SstTraceLevel::L0,
            LayerKind::SortedRun(run) => SstTraceLevel::SortedRun(run.id),
        }
    }

    /// Groups the keys by the SST that can hold them, in the order of the layer.
    pub(super) fn candidates(&self, keys: Vec<OpenKey>) -> Vec<SstRead> {
        let mut reads: Vec<SstRead> = Vec::new();
        for open in keys {
            let point = BytesRange::from_slice(open.key.as_ref()..=open.key.as_ref());
            let views: &[SsTableView] = match &self.kind {
                LayerKind::L0(view) => std::slice::from_ref(view),
                LayerKind::SortedRun(run) => run.tables_covering_point_key(&open.key),
            };
            for view in views {
                if view.calculate_view_range(point.clone()).is_none() {
                    continue;
                }
                // The keys are sorted, so the SSTs of one key follow the SSTs of the key before.
                match reads.last_mut() {
                    Some(read) if read.view_id() == view.id => read.push_key(open.clone()),
                    _ => reads.push(SstRead::new(
                        view.clone(),
                        self.level(),
                        self.prefix.clone(),
                        vec![open.clone()],
                    )),
                }
            }
        }
        reads
    }

    /// Reads every candidate SST of the layer and returns the entries in SST order.
    pub(super) async fn walk(
        self,
        keys: Vec<OpenKey>,
        ctx: &ReadContext<'_>,
    ) -> Result<LayerEntries, SlateDBError> {
        let reads = self.candidates(keys).into_iter().map(|read| read.read(ctx));
        let mut entries = LayerEntries::default();
        for read in try_join_all(reads).await? {
            entries.extend(read);
        }
        Ok(entries)
    }
}

/// The layers of the batch: per segment in key order, L0 newest first, then the sorted runs.
pub(super) struct Layers {
    layers: Vec<Layer>,
}

impl Layers {
    /// Finds the segment of each key and lists the layers of the distinct segments.
    pub(super) fn new(core: &ManifestCore, batch: &mut Batch) -> Self {
        let mut segments: Vec<Segment> = Vec::new();
        for index in 0..batch.keys().len() {
            let key = &batch.keys()[index];
            let point = BytesRange::from_slice(key.as_ref()..=key.as_ref());
            let segment = match core.select_segments(&point) {
                Some(found) => found.first().cloned(),
                None => Some(core.default_segment()),
            };
            let Some(segment) = segment else {
                continue;
            };
            if segments
                .last()
                .is_none_or(|last| last.prefix != segment.prefix)
            {
                segments.push(segment);
            }
            batch.set_segment(index, segments.len() - 1);
        }

        let mut layers = Vec::new();
        for (index, segment) in segments.iter().enumerate() {
            let l0 = segment
                .tree
                .l0
                .iter()
                .map(|view| LayerKind::L0(Box::new(view.clone())));
            let runs = segment
                .tree
                .compacted
                .iter()
                .cloned()
                .map(LayerKind::SortedRun);
            layers.extend(l0.chain(runs).map(|kind| Layer {
                segment: index,
                prefix: segment.prefix.clone(),
                kind,
            }));
        }
        Self { layers }
    }
}

impl IntoIterator for Layers {
    type Item = Layer;
    type IntoIter = std::vec::IntoIter<Layer>;

    fn into_iter(self) -> Self::IntoIter {
        self.layers.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use bytes::Bytes;
    use ulid::Ulid;

    use super::*;
    use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::LsmTreeState;

    fn view(first: &[u8], last: &[u8]) -> SsTableView {
        let info = SsTableInfo {
            first_entry: Some(Bytes::copy_from_slice(first)),
            last_entry: Some(Bytes::copy_from_slice(last)),
            ..Default::default()
        };
        let handle = SsTableHandle::new(
            SsTableId::from(Ulid::new()),
            SST_FORMAT_VERSION_LATEST,
            info,
        );
        SsTableView::identity(handle)
    }

    fn open_keys(keys: &[&[u8]]) -> Vec<OpenKey> {
        keys.iter()
            .enumerate()
            .map(|(index, key)| OpenKey {
                index,
                key: Bytes::copy_from_slice(key),
            })
            .collect()
    }

    fn tree(l0: Vec<SsTableView>, compacted: Vec<SortedRun>) -> Arc<LsmTreeState> {
        Arc::new(LsmTreeState {
            l0: VecDeque::from(l0),
            compacted,
            ..Default::default()
        })
    }

    #[test]
    fn test_l0_candidates_apply_the_visible_range() {
        let visible = BytesRange::from_slice(b"b".as_ref()..b"d".as_ref());
        let layer = Layer {
            segment: 0,
            prefix: Bytes::new(),
            kind: LayerKind::L0(Box::new(view(b"a", b"z").with_visible_range(visible))),
        };

        let reads = layer.candidates(open_keys(&[b"a", b"b", b"c", b"d"]));

        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].keys(), &open_keys(&[b"a", b"b", b"c", b"d"])[1..3]);
    }

    #[test]
    fn test_sorted_run_candidates_group_keys_by_sst() {
        let run = SortedRun::new(
            7,
            [
                view(b"a", b"k"),
                view(b"k", b"k"),
                view(b"k", b"m"),
                view(b"z", b"z"),
            ],
        );
        let layer = Layer {
            segment: 0,
            prefix: Bytes::new(),
            kind: LayerKind::SortedRun(run.clone()),
        };
        let keys = open_keys(&[b"0", b"k", b"l", b"z"]);

        let reads = layer.candidates(keys.clone());

        let groups: Vec<(Ulid, Vec<OpenKey>)> = reads
            .iter()
            .map(|read| (read.view_id(), read.keys().to_vec()))
            .collect();
        let views = run.sst_views();
        assert_eq!(
            groups,
            vec![
                (views[0].id, vec![keys[1].clone()]),
                (views[1].id, vec![keys[1].clone()]),
                (views[2].id, vec![keys[1].clone(), keys[2].clone()]),
                (views[3].id, vec![keys[3].clone()]),
            ]
        );
    }

    #[test]
    fn test_layers_of_an_unsegmented_database() {
        let mut core = ManifestCore::new();
        core.tree = tree(
            vec![view(b"a", b"z"), view(b"a", b"z")],
            vec![SortedRun::new(1, [view(b"a", b"z")])],
        );
        let mut batch = Batch::new(&[b"a".as_ref(), b"b"], None);

        let layers = Layers::new(&core, &mut batch);

        let levels: Vec<SstTraceLevel> = layers.layers.iter().map(|layer| layer.level()).collect();
        assert_eq!(
            levels,
            vec![
                SstTraceLevel::L0,
                SstTraceLevel::L0,
                SstTraceLevel::SortedRun(1)
            ]
        );
        assert!(layers.layers.iter().all(|layer| layer.segment == 0));
        assert_eq!(batch.open_keys(0).len(), 2);
    }

    #[test]
    fn test_layers_of_a_segmented_database() {
        let mut core = ManifestCore::new();
        core.segment_extractor_name = Some("prefix".to_string());
        core.segments = vec![
            Segment {
                prefix: Bytes::from_static(b"a/"),
                tree: tree(vec![view(b"a/", b"a/z")], vec![]),
            },
            Segment {
                prefix: Bytes::from_static(b"b/"),
                tree: tree(vec![], vec![SortedRun::new(2, [view(b"b/", b"b/z")])]),
            },
        ];
        let mut batch = Batch::new(&[b"a/1".as_ref(), b"b/1", b"c/1"], None);

        let layers = Layers::new(&core, &mut batch);

        let segments: Vec<(usize, Bytes)> = layers
            .layers
            .iter()
            .map(|layer| (layer.segment, layer.prefix.clone()))
            .collect();
        assert_eq!(
            segments,
            vec![
                (0, Bytes::from_static(b"a/")),
                (1, Bytes::from_static(b"b/"))
            ]
        );
        assert_eq!(batch.open_keys(0).len(), 1);
        assert_eq!(batch.open_keys(1).len(), 1);
        // The key outside every segment has no layer to read.
        assert_eq!(batch.open_keys(2).len(), 0);
    }
}
