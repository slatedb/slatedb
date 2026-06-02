//! RFC-0025 "GC cutoff rule enforcement" invariants for the `.manifest` object.
//!
//! Each invariant is a plain [`Invariant<Manifest>`] predicate attached once to
//! the [`StoredManifest`](super::store::StoredManifest)'s transactional object, so
//! every manifest `update` path validates the dirty value against the current
//! committed value before the CAS write.

use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

use slatedb_txn_obj::Invariant;
use ulid::Ulid;

use crate::error::SlateDBError;
use crate::manifest::Manifest;

/// Invariant 1 — L0 ULID cutoff. Every L0 SST view newly added by `dirty` must
/// carry a ULID timestamp "strictly" greater than the current manifest's L0
/// watermark (`current.core.max_l0_ulid_timestamp_across_trees()`): the maximum
/// ULID across all trees' live L0 views and `last_compacted_l0_sst_view_id`
/// markers.
///
/// This guarantees a newly published L0 sorts above every L0 the GC may have
/// already fenced, so a writer on a backwards-skewed wall clock cannot publish an
/// SST whose ULID falls below the GC cutoff. Which could otherwise make a live
/// SST eligible for deletion and corrupt the database state.
///
/// "Newly added" is the set of L0 view ULIDs present in `dirty` but absent from
/// `current`, so re-publishing existing L0s (an idempotent update) never fails
/// the check. On violation, returns [`SlateDBError::InvalidClockTick`] with the
/// watermark timestamp as `last_tick` and the offending ULID timestamp as
/// `next_tick`. The txn-obj layer wraps the boxed error in `CallbackError`, which
/// maps back to `SlateDBError::InvalidClockTick` for the caller.
#[allow(dead_code)]
pub(crate) fn l0_ulid_cutoff(
    dirty: &Manifest,
    current: &Manifest,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(watermark) = current.core.max_l0_ulid_timestamp_across_trees() else {
        // Fresh manifest with no L0 history — nothing to compare against.
        return Ok(());
    };

    // L0 view ULIDs already committed in `current`; anything in `dirty` not
    // in this set is a newly added L0 that must clear the watermark.
    let existing: HashSet<Ulid> = current
        .core
        .trees()
        .flat_map(|tree| tree.l0.iter().map(|view| view.id))
        .collect();

    for view in dirty.core.trees().flat_map(|tree| tree.l0.iter()) {
        if existing.contains(&view.id) {
            continue;
        }
        if view.id.timestamp_ms() <= watermark.timestamp_ms() {
            return Err(Box::new(SlateDBError::InvalidClockTick {
                last_tick: watermark.timestamp_ms() as i64,
                next_tick: view.id.timestamp_ms() as i64,
            }));
        }
    }
    Ok(())
}

/// The invariants enforced on every `.manifest` update (RFC-0025 GC cutoff
/// rules). Will be attached once at [`StoredManifest`](super::store::StoredManifest)
/// construction via `with_invariants` in the follow-up wiring PR.
#[allow(dead_code)]
pub(crate) fn manifest_invariants() -> Vec<Invariant<Manifest>> {
    vec![Arc::new(l0_ulid_cutoff)]
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use ulid::Ulid;

    use super::*;
    use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::{LsmTreeState, ManifestCore, Segment};

    /// An L0 view whose view ULID carries timestamp `ts_ms` and random component
    /// `rand` — two views at the same `ts_ms` with different `rand` are distinct
    /// ULIDs sharing a timestamp, which is exactly the same-millisecond collision
    /// the invariant must reject.
    fn l0_view(ts_ms: u64, rand: u128) -> SsTableView {
        let view_id = Ulid::from_parts(ts_ms, rand);
        let handle = SsTableHandle::new(
            SsTableId::Compacted(view_id),
            SST_FORMAT_VERSION_LATEST,
            SsTableInfo::default(),
        );
        SsTableView::new(view_id, handle)
    }

    /// A manifest with the given L0 view timestamps on the root tree. Each view's
    /// random component is its position, so equal timestamps still yield distinct
    /// ULIDs.
    fn manifest_with_root_l0(ts: &[u64]) -> Manifest {
        let mut core = ManifestCore::new();
        Arc::make_mut(&mut core.tree).l0 = ts
            .iter()
            .enumerate()
            .map(|(i, &t)| l0_view(t, i as u128))
            .collect();
        Manifest::initial(core)
    }

    #[test]
    fn fresh_current_accepts_any_dirty_l0() {
        // No L0 history in `current` → no watermark → anything is allowed.
        let current = manifest_with_root_l0(&[]);
        let dirty = manifest_with_root_l0(&[1]);
        l0_ulid_cutoff(&dirty, &current).unwrap();
    }

    #[test]
    fn dirty_adds_l0_above_watermark_ok() {
        let current = manifest_with_root_l0(&[100]);
        let dirty = manifest_with_root_l0(&[100, 101]);
        l0_ulid_cutoff(&dirty, &current).unwrap();
    }

    #[test]
    fn dirty_adds_l0_below_watermark_errors() {
        let current = manifest_with_root_l0(&[100]);
        let dirty = manifest_with_root_l0(&[100, 50]);
        let err = l0_ulid_cutoff(&dirty, &current).unwrap_err();
        let err = err.downcast::<SlateDBError>().unwrap();
        assert!(matches!(
            *err,
            SlateDBError::InvalidClockTick {
                last_tick: 100,
                next_tick: 50
            }
        ));
    }

    #[test]
    fn dirty_adds_l0_equal_to_watermark_errors() {
        // Strict `>`: a same-millisecond ULID is rejected (the same-ms collision
        // the writer retry path resolves by minting past the ms boundary).
        let current = manifest_with_root_l0(&[100]);
        let dirty = manifest_with_root_l0(&[100, 100]);
        let err = l0_ulid_cutoff(&dirty, &current).unwrap_err();
        let err = err.downcast::<SlateDBError>().unwrap();
        assert!(matches!(
            *err,
            SlateDBError::InvalidClockTick {
                last_tick: 100,
                next_tick: 100
            }
        ));
    }

    #[test]
    fn reincluding_existing_l0_is_idempotent() {
        // `dirty` re-publishes the same L0 set with no additions — even though
        // those ULIDs equal the watermark, they are not "newly added" so the
        // check passes.
        let current = manifest_with_root_l0(&[100, 200]);
        let dirty = manifest_with_root_l0(&[100, 200]);
        l0_ulid_cutoff(&dirty, &current).unwrap();
    }

    #[test]
    fn watermark_uses_last_compacted_marker() {
        // The watermark must include `last_compacted_l0_sst_view_id`, not just
        // live L0 views. Here the live L0 is empty but a compacted marker at 300
        // exists, so a dirty L0 at 250 must be rejected.
        let mut core = ManifestCore::new();
        Arc::make_mut(&mut core.tree).last_compacted_l0_sst_view_id =
            Some(Ulid::from_parts(300, 0));
        let current = Manifest::initial(core);
        let dirty = manifest_with_root_l0(&[250]);
        let err = l0_ulid_cutoff(&dirty, &current).unwrap_err();
        let err = err.downcast::<SlateDBError>().unwrap();
        assert!(matches!(
            *err,
            SlateDBError::InvalidClockTick {
                last_tick: 300,
                next_tick: 250
            }
        ));
    }

    #[test]
    fn watermark_is_max_across_all_trees() {
        // A segment tree carries the highest watermark (400); a dirty L0 added to
        // the root at 350 is below the cross-tree max and must be rejected.
        let mut current_core = ManifestCore::new();
        current_core.segments.push(Segment {
            prefix: bytes::Bytes::from_static(b"seg"),
            tree: Arc::new(LsmTreeState {
                l0: VecDeque::from(vec![l0_view(400, 0)]),
                ..LsmTreeState::default()
            }),
        });
        let current = Manifest::initial(current_core);

        let mut dirty_core = ManifestCore::new();
        Arc::make_mut(&mut dirty_core.tree).l0 = VecDeque::from(vec![l0_view(350, 0)]);
        // The segment's L0 is unchanged (same ULID), so it is not a new addition.
        dirty_core.segments.push(Segment {
            prefix: bytes::Bytes::from_static(b"seg"),
            tree: Arc::new(LsmTreeState {
                l0: VecDeque::from(vec![l0_view(400, 0)]),
                ..LsmTreeState::default()
            }),
        });
        let dirty = Manifest::initial(dirty_core);

        let err = l0_ulid_cutoff(&dirty, &current).unwrap_err();
        let err = err.downcast::<SlateDBError>().unwrap();
        assert!(matches!(
            *err,
            SlateDBError::InvalidClockTick {
                last_tick: 400,
                next_tick: 350
            }
        ));
    }
}
