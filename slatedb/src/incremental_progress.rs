//! Builds the manifest views for an incremental commit of a running compaction job.
//!
//! An incremental commit publishes output that a worker fully stored before the job
//! finishes. For each subcompaction, the coordinator finds the durable interval, which
//! is the key range whose output is complete. It removes those keys from the input
//! views of the job and adds views of the output SSTs to the destination run.

use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use bytes::Bytes;
use slatedb_common::DbRand;

use crate::bytes_range::BytesRange;
use crate::compactor_state::{Compaction, CompactionSpec, SourceId};
use crate::db_state::{SortedRun, SsTableId, SsTableView};
use crate::error::SlateDBError;
use crate::manifest::ManifestCore;

/// Durable progress of one job, built from its stored subcompactions.
pub(crate) struct IncrementalProgress {
    /// Durable intervals in key order, at most one per subcompaction. A subcompaction
    /// that has no stored output and is not `Completed` has no interval.
    intervals: Vec<DurableInterval>,
    /// Views of the output SSTs inside the durable intervals, in key order. When two
    /// SSTs share a key, the SST with the newer versions comes first. Only an SST that
    /// ends at the boundary key of an unfinished range is cut, so that it ends before
    /// that key.
    output: Vec<SsTableView>,
    /// IDs of all recorded output SSTs, including SSTs left out of `output`.
    /// `apply_to_manifest_core` uses them to remove output views of earlier commits.
    output_ids: HashSet<SsTableId>,
}

impl IncrementalProgress {
    /// Builds the durable progress of `job` from its stored subcompactions.
    ///
    /// Each subcompaction adds one durable interval or none:
    /// - `Completed`: the whole planned range, even when the range has no output.
    /// - Unfinished with output: from the range start up to the last key of the last
    ///   output SST, without that key, because later output SSTs can still hold
    ///   versions of that key.
    /// - Unfinished without output: no interval.
    ///
    /// Each cut output view gets a new view ID from `rand`.
    ///
    /// Panics if the last output SST of an unfinished range has no `last_entry`.
    pub(crate) fn from_compaction(job: &Compaction, rand: &DbRand) -> Self {
        let mut intervals = Vec::new();
        let mut output = Vec::new();
        let mut output_ids = HashSet::new();
        for sub_compaction in job.subcompactions() {
            let output_ssts = sub_compaction.output_ssts();
            // Record every output SST, even one that this commit leaves out.
            output_ids.extend(output_ssts.iter().map(|sst| sst.id));
            if sub_compaction.completed() {
                output.extend(output_ssts.iter().cloned().map(SsTableView::identity));
                intervals.push(DurableInterval::from_planned_range(sub_compaction.range()));
                continue;
            }
            let Some(last) = output_ssts.last() else {
                continue;
            };
            let boundary = last
                .info
                .last_entry
                .clone()
                .expect("SST writers record the last entry");
            debug_assert!(
                sub_compaction.range().contains(&boundary),
                "a subcompaction writes only keys inside its range"
            );
            // The boundary key can still have versions in later output SSTs.
            let interval = DurableInterval {
                end: Some(boundary.clone()),
                ..DurableInterval::from_planned_range(sub_compaction.range())
            };
            // If the boundary key is the range start, no key of the range is durable yet.
            if interval.start.as_ref() == Some(&boundary) {
                continue;
            }
            for sst in output_ssts {
                let view = SsTableView::identity(sst.clone());
                // The versions of the boundary key can span several SSTs, and each of
                // them ends at that key. Cut such an SST to end before the key. An SST
                // that holds only the boundary key drops out.
                if sst.info.last_entry.as_ref() == Some(&boundary) {
                    if let Some(cut_view) = view.try_with_visible_range(interval.to_range(), rand) {
                        output.push(cut_view);
                    }
                } else {
                    output.push(view);
                }
            }
            intervals.push(interval);
        }
        Self {
            intervals,
            output,
            output_ids,
        }
    }

    /// Rebuilds the input runs of the job with `spec` in `core` from this progress.
    ///
    /// The result depends only on the current input views and this progress, so a
    /// commit with more progress replaces the views of an earlier commit, and the
    /// same progress applied twice gives the same result:
    /// - Every view of a recorded output SST is removed first.
    /// - Each durable interval is cut out of each remaining input view. Each piece
    ///   whose range changes gets a new view ID from `rand`. A view with no keys left
    ///   is removed.
    /// - The destination run gets the views in `output`, sorted by start key.
    ///
    /// Input runs stay in the manifest, even when empty, so recovery can still find
    /// them by ID. Updates only `core`. The caller writes the manifest.
    ///
    /// Panics if `spec` has L0 inputs or if its destination is not one of its input
    /// runs. The coordinator passes only jobs that can commit incremental progress.
    ///
    /// Returns `InvalidCompaction` if the segment or an input run of `spec` is missing
    /// from `core`. This can happen when another job removed an input run that both
    /// jobs share.
    pub(crate) fn apply_to_manifest_core(
        &self,
        core: &mut ManifestCore,
        spec: &CompactionSpec,
        rand: &DbRand,
    ) -> Result<(), SlateDBError> {
        assert!(
            !spec.has_l0_sources() && spec.reuses_input_run(),
            "an incremental commit needs a job with only sorted-run inputs and an input \
             run as destination"
        );
        let destination = spec
            .destination()
            .expect("reuses_input_run requires a destination");
        let sources: HashSet<u32> = spec
            .sources()
            .iter()
            .map(SourceId::unwrap_sorted_run)
            .collect();
        let tree = core
            .tree_for_segment_mut(spec.segment())
            .ok_or(SlateDBError::InvalidCompaction)?;
        if !sources
            .iter()
            .all(|id| tree.compacted.iter().any(|sr| sr.id == *id))
        {
            return Err(SlateDBError::InvalidCompaction);
        }
        // Rebuild each input run of the job. Other runs stay unchanged.
        for run in tree
            .compacted
            .iter_mut()
            .filter(|run| sources.contains(&run.id))
        {
            let mut views = Vec::new();
            for view in run.sst_views() {
                // Remove output views of earlier commits from the destination run, the only
                // run that holds them. `output` adds them again below.
                if run.id == destination && self.output_ids.contains(&view.sst.id) {
                    continue;
                }
                // Keep only the keys outside every durable interval.
                let mut retained = vec![view.compacted_effective_range().clone()];
                for interval in &self.intervals {
                    retained = retained
                        .into_iter()
                        .flat_map(|range| interval.subtract_from(&range))
                        .collect();
                }
                // A view can split into several pieces. A view with no keys left is gone.
                for range in retained {
                    views.push(if &range == view.compacted_effective_range() {
                        view.clone()
                    } else {
                        view.try_with_visible_range(range, rand)
                            .expect("retained range intersects its SST")
                    });
                }
            }
            // The destination run also gets the output views, sorted by start key.
            if run.id == destination {
                views.extend(self.output.iter().cloned());
                // Stable sorting preserves version order when output SSTs start at the same key.
                views.sort_by(|left, right| {
                    left.compacted_effective_range()
                        .comparable_start_bound()
                        .cmp(&right.compacted_effective_range().comparable_start_bound())
                });
            }
            *run = SortedRun::new(run.id, views);
        }
        Ok(())
    }
}

/// A durable interval `[start, end)`: the key range of one subcompaction whose output
/// is fully stored, so an incremental commit can publish it.
///
/// A `Completed` subcompaction has its whole planned range as its durable interval.
/// An unfinished subcompaction has the prefix of its planned range that ends before
/// the last key of its last output SST, so the interval grows as the worker writes
/// more output.
///
/// The start is always inclusive and the end always exclusive. So `subtract_from`
/// needs no cases for other bound kinds, and its pieces are always valid view ranges.
#[derive(Clone, Debug, PartialEq)]
struct DurableInterval {
    /// First key of the interval. `None` means that the interval starts at the lowest key.
    start: Option<Bytes>,
    /// First key after the interval. `None` means that the interval has no end.
    end: Option<Bytes>,
}

impl DurableInterval {
    /// Converts a planned subcompaction range.
    ///
    /// The planner builds every range as `[boundary, next boundary)`. The first range
    /// has no start bound, and the last range has no end bound. A `Completed`
    /// subcompaction uses the result as it is. An unfinished subcompaction keeps the
    /// start and replaces the end with its boundary key.
    fn from_planned_range(range: &BytesRange) -> Self {
        let start = match range.start_bound() {
            Included(key) => Some(key.clone()),
            Unbounded => None,
            Excluded(_) => unreachable!("planned ranges start inclusively"),
        };
        let end = match range.end_bound() {
            Excluded(key) => Some(key.clone()),
            Unbounded => None,
            Included(_) => unreachable!("planned ranges end exclusively"),
        };
        Self { start, end }
    }

    /// Returns the same keys as a `BytesRange`, for example to cut an output SST view.
    fn to_range(&self) -> BytesRange {
        BytesRange::new(
            self.start.clone().map_or(Unbounded, Included),
            self.end.clone().map_or(Unbounded, Excluded),
        )
    }

    /// Returns the parts of `range` outside this interval, in key order.
    ///
    /// The parts are the keys of `range` before `start` and the keys of `range` from
    /// `end` on. A part is missing when this interval has no bound on that side or
    /// when `range` has no keys there. So the result has zero, one, or two ranges, and
    /// each one starts inclusively.
    fn subtract_from(&self, range: &BytesRange) -> Vec<BytesRange> {
        // Keys of `range` in `(.., start)`.
        let before = self
            .start
            .clone()
            .and_then(|start| range.intersect(&BytesRange::new(Unbounded, Excluded(start))));
        // Keys of `range` in `[end, ..)`.
        let after = self
            .end
            .clone()
            .and_then(|end| range.intersect(&BytesRange::new(Included(end), Unbounded)));
        before.into_iter().chain(after).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor_state::{CompactionContext, CompactionSpec};
    use crate::db_state::{SsTableHandle, SsTableInfo};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::subcompaction::Subcompaction;
    use bytes::Bytes;
    use std::sync::Arc;
    use ulid::Ulid;

    fn view(first: &'static [u8], last: &'static [u8]) -> SsTableView {
        SsTableView::identity(SsTableHandle::new(
            Ulid::new().into(),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo {
                first_entry: Some(Bytes::from_static(first)),
                last_entry: Some(Bytes::from_static(last)),
                ..Default::default()
            },
        ))
    }

    #[test]
    fn subtract_preserves_edges_and_gaps() {
        let range = BytesRange::from_slice(b"a".as_slice()..b"z".as_slice());
        let removed = BytesRange::from_slice(b"g".as_slice()..b"m".as_slice());
        assert_eq!(
            DurableInterval::from_planned_range(&removed).subtract_from(&range),
            vec![
                BytesRange::from_slice(b"a".as_slice()..b"g".as_slice()),
                BytesRange::from_slice(b"m".as_slice()..b"z".as_slice()),
            ]
        );
        assert_eq!(
            DurableInterval::from_planned_range(&BytesRange::unbounded()).subtract_from(&range),
            vec![]
        );
        assert_eq!(
            DurableInterval::from_planned_range(&BytesRange::from_slice(b"z".as_slice()..))
                .subtract_from(&range),
            vec![range]
        );
    }

    #[test]
    fn incremental_views_get_new_ids_keep_projections_and_empty_runs() {
        let original = view(b"a", b"z")
            .with_visible_range(BytesRange::from_slice(b"c".as_slice()..b"x".as_slice()));
        let released = view(b"g", b"j");
        let output = view(b"g", b"l");
        let mut core = ManifestCore::new();
        Arc::make_mut(&mut core.tree).compacted = vec![
            SortedRun::new(2, [released]),
            SortedRun::new(1, [original.clone()]),
        ];
        let job = Compaction::new(
            Ulid::new(),
            CompactionSpec::new(vec![SourceId::SortedRun(2), SourceId::SortedRun(1)], 1),
        );
        let progress = IncrementalProgress {
            intervals: vec![DurableInterval::from_planned_range(
                &BytesRange::from_slice(b"g".as_slice()..b"m".as_slice()),
            )],
            output_ids: HashSet::from([output.sst.id]),
            output: vec![output],
        };
        progress
            .apply_to_manifest_core(&mut core, job.spec(), &DbRand::default())
            .unwrap();
        assert_eq!(
            core.tree
                .compacted
                .iter()
                .map(|run| run.id)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert!(core.tree.compacted[0].sst_views().is_empty());
        let views = core.tree.compacted[1].sst_views();
        assert_eq!(views.len(), 3);
        assert_ne!(views[0].id, views[2].id);
        for piece in [&views[0], &views[2]] {
            assert_ne!(piece.id, original.id);
            assert_eq!(piece.id.timestamp_ms(), original.id.timestamp_ms());
        }
        assert_eq!(
            views[0].compacted_effective_range(),
            &BytesRange::from_slice(b"c".as_slice()..b"g".as_slice())
        );
        assert_eq!(
            views[2].compacted_effective_range(),
            &BytesRange::from_slice(b"m".as_slice()..b"x".as_slice())
        );
        let before = core.clone();
        progress
            .apply_to_manifest_core(&mut core, job.spec(), &DbRand::default())
            .unwrap();
        assert_eq!(core, before);
    }

    #[test]
    fn clone_projection_preserves_incremental_input_gaps_and_boundary_versions() {
        use crate::manifest::{Manifest, ProjectionConfig};
        let original = view(b"a", b"z");
        let left = original.with_visible_range(BytesRange::from_slice(..b"g".as_slice()));
        let mut right = original.with_visible_range(BytesRange::from_slice(b"m".as_slice()..));
        right.id = Ulid::new();
        let mut core = ManifestCore::new();
        Arc::make_mut(&mut core.tree).compacted = vec![
            SortedRun::new(3, [view(b"g", b"l")]),
            SortedRun::new(2, [left, right]),
            SortedRun::new(1, [view(b"m", b"m"), view(b"m", b"m"), view(b"z", b"z")]),
        ];
        let manifest = Manifest::initial(core);
        let projected = Manifest::projected(
            &manifest,
            &ProjectionConfig::from_global_range(BytesRange::from_slice(
                b"b".as_slice()..b"y".as_slice(),
            )),
            &DbRand::default(),
        )
        .unwrap();
        let retained = &projected.core.tree.compacted[1];
        assert!(retained.sst_views().iter().all(|view| !view
            .compacted_effective_range()
            .contains(&Bytes::from_static(b"h"))));
        let point = Manifest::projected(
            &manifest,
            &ProjectionConfig::from_global_range(BytesRange::from_slice(
                b"m".as_slice()..=b"m".as_slice(),
            )),
            &DbRand::default(),
        )
        .unwrap();
        assert_eq!(
            point
                .core
                .tree
                .compacted
                .iter()
                .find(|run| run.id == 1)
                .unwrap()
                .sst_views()
                .len(),
            2
        );
    }

    #[test]
    fn durable_progress_includes_empty_completion() {
        let output = view(b"a", b"m").sst;
        let make_job = |output| {
            Compaction::new(
                Ulid::new(),
                CompactionSpec::new(vec![SourceId::SortedRun(1)], 1),
            )
            .with_ctx(Some(CompactionContext::new(
                vec![
                    Subcompaction::new(BytesRange::from_slice(..b"z".as_slice()))
                        .with_output_ssts(vec![output]),
                    Subcompaction::new(BytesRange::from_slice(b"z".as_slice()..))
                        .with_status(crate::subcompaction::SubcompactionStatus::Completed),
                ],
                Some(0),
            )))
        };
        let progress = IncrementalProgress::from_compaction(&make_job(output), &DbRand::default());
        assert_eq!(
            progress.intervals,
            vec![
                DurableInterval {
                    start: None,
                    end: Some(Bytes::from_static(b"m")),
                },
                DurableInterval {
                    start: Some(Bytes::from_static(b"z")),
                    end: None,
                },
            ]
        );
        assert_eq!(progress.output.len(), 1);
    }

    #[test]
    fn durable_prefix_excludes_all_versions_of_the_boundary_key() {
        let untouched = view(b"a", b"e");
        let first = view(b"f", b"m");
        let last = view(b"m", b"m");
        let job = Compaction::new(
            Ulid::new(),
            CompactionSpec::new(vec![SourceId::SortedRun(1)], 1),
        )
        .with_ctx(Some(CompactionContext::new(
            vec![
                Subcompaction::new(BytesRange::unbounded()).with_output_ssts(vec![
                    untouched.sst,
                    first.sst,
                    last.sst,
                ]),
            ],
            Some(0),
        )));
        let progress = IncrementalProgress::from_compaction(&job, &DbRand::default());
        assert_eq!(progress.output.len(), 2);
        assert!(progress.output[0].visible_range().is_none());
        assert!(!progress.output[1]
            .compacted_effective_range()
            .contains(&Bytes::from_static(b"m")));
    }

    #[test]
    fn destination_rebuild_preserves_output_order_and_expands_previous_views() {
        let input = view(b"a", b"z");
        let outputs = [view(b"g", b"g"), view(b"g", b"g"), view(b"g", b"m")];
        let mut core = ManifestCore::new();
        Arc::make_mut(&mut core.tree).compacted = vec![SortedRun::new(1, [input.clone()])];
        let mut job = Compaction::new(
            Ulid::new(),
            CompactionSpec::new(vec![SourceId::SortedRun(1)], 1),
        )
        .with_ctx(Some(CompactionContext::new(
            vec![
                Subcompaction::new(BytesRange::from_slice(b"g".as_slice()..b"n".as_slice()))
                    .with_output_ssts(outputs.iter().map(|view| view.sst.clone()).collect()),
            ],
            Some(0),
        )));
        let progress = IncrementalProgress::from_compaction(&job, &DbRand::default());
        progress
            .apply_to_manifest_core(&mut core, job.spec(), &DbRand::default())
            .unwrap();
        let views = core.tree.compacted[0].sst_views();
        assert_eq!(views.len(), 5);
        assert_eq!(views[0].sst.id, input.sst.id);
        assert_eq!(
            views[1..4]
                .iter()
                .map(|view| view.sst.id)
                .collect::<Vec<_>>(),
            outputs.iter().map(|view| view.sst.id).collect::<Vec<_>>()
        );
        assert!(!views[3]
            .compacted_effective_range()
            .contains(&Bytes::from_static(b"m")));

        let mut ctx = job.ctx().unwrap().clone();
        ctx.mark_completed(0);
        job.set_ctx(Some(ctx));
        let completed = IncrementalProgress::from_compaction(&job, &DbRand::default());
        completed
            .apply_to_manifest_core(&mut core, job.spec(), &DbRand::default())
            .unwrap();
        let views = core.tree.compacted[0].sst_views();
        assert_eq!(views.len(), 5);
        assert_eq!(views[4].sst.id, input.sst.id);
        assert!(views[3]
            .compacted_effective_range()
            .contains(&Bytes::from_static(b"m")));
        assert_eq!(
            views[4].compacted_effective_range().start_bound(),
            Included(&Bytes::from_static(b"n"))
        );
        let before = core.clone();
        completed
            .apply_to_manifest_core(&mut core, job.spec(), &DbRand::default())
            .unwrap();
        assert_eq!(core, before);
    }
}
