//! # Subcompaction Data Model
//!
//! In-memory record of a subcompaction (RFC-0028): a compaction over a
//! sub-range of a parent compaction's key space. See [`Subcompaction`].

use serde::Serialize;

use crate::bytes_range::BytesRange;
use crate::db_state::SsTableHandle;

/// A compaction over a sub-range of the parent compaction's key space
/// (RFC-0028).
///
/// Subcompactions are only valid within the context of a parent
/// [`Compaction`](crate::compactor_state::Compaction) and let a single logical
/// compaction execute its disjoint ranges in parallel while resuming at range
/// granularity after a failure. A subcompaction carries no lifecycle status:
/// the parent [`Compaction`](crate::compactor_state::Compaction) owns status,
/// and a range's progress is captured entirely by its recorded `output_ssts`.
/// On resume every range is re-run from its persisted output; a range that
/// already finished has nothing left to merge and completes immediately.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Subcompaction {
    /// Key range covered by this subcompaction. The ranges of the
    /// subcompactions within a parent compaction are non-overlapping and
    /// together cover the full key space.
    range: BytesRange,
    /// Output SSTs produced by this subcompaction so far.
    output_ssts: Vec<SsTableHandle>,
}

impl Subcompaction {
    pub(crate) fn new(range: BytesRange) -> Self {
        Self {
            range,
            output_ssts: Vec::new(),
        }
    }

    pub(crate) fn with_output_ssts(mut self, output_ssts: Vec<SsTableHandle>) -> Self {
        self.output_ssts = output_ssts;
        self
    }

    /// Returns the key range covered by this subcompaction.
    pub(crate) fn range(&self) -> &BytesRange {
        &self.range
    }

    /// Returns the output SSTs produced by this subcompaction so far.
    pub fn output_ssts(&self) -> &Vec<SsTableHandle> {
        &self.output_ssts
    }

    /// Sets the output SSTs produced by this subcompaction.
    // Consumed by the subcompaction executor in a follow-up RFC-0028 PR; this
    // PR only introduces the data model and its extend-only contract.
    #[allow(dead_code)]
    pub(crate) fn set_output_ssts(&mut self, output_ssts: Vec<SsTableHandle>) {
        assert!(
            output_ssts.starts_with(self.output_ssts.as_slice()),
            "new subcompaction output SSTs must always extend previous output SSTs"
        );
        self.output_ssts = output_ssts;
    }
}
