use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};

use log::{error, info};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::db_state::{ManifestCore, SortedRun, SsTableHandle};
use crate::error::SlateDBError;
use crate::manifest::Manifest;
use slatedb_txn_obj::DirtyObject;

/// Identifier for a compaction input source.
///
/// A `SourceId` distinguishes between two kinds of inputs a compaction can read:
/// an existing compacted sorted run (identified by its run id), or an L0 SSTable
/// (identified by its ULID).
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceId {
    SortedRun(u32),
    Sst(Ulid),
}

impl Display for SourceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SourceId::SortedRun(id) => {
                    format!("{}", *id)
                }
                SourceId::Sst(_) => String::from("l0"),
            }
        )
    }
}

impl SourceId {
    /// Unwraps the source as a Sorted Run id, panicking if it is an L0 SST.
    ///
    /// ## Returns
    /// - The sorted run id.
    ///
    /// ## Panics
    /// - If called on `SourceId::Sst`.
    pub(crate) fn unwrap_sorted_run(&self) -> u32 {
        self.maybe_unwrap_sorted_run()
            .expect("tried to unwrap Sst as Sorted Run")
    }

    /// Returns the sorted run id if this source is a `SortedRun`, otherwise `None`.
    pub(crate) fn maybe_unwrap_sorted_run(&self) -> Option<u32> {
        match self {
            SourceId::SortedRun(id) => Some(*id),
            SourceId::Sst(_) => None,
        }
    }

    /// Returns the SST ULID if this source is an `Sst`, otherwise `None`.
    pub(crate) fn maybe_unwrap_sst(&self) -> Option<Ulid> {
        match self {
            SourceId::SortedRun(_) => None,
            SourceId::Sst(ulid) => Some(*ulid),
        }
    }
}

/// Immutable spec that describes a compaction. Currently, this only holds the
/// input sources and destination SR id for a compaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionSpec {
    /// Input sources for the compaction.
    sources: Vec<SourceId>,
    /// Destination sorted run id for the compaction.
    destination: u32,
}

impl CompactionSpec {
    /// Creates a new compaction spec describing which sources to compact and the destination SR id.
    ///
    /// ## Arguments
    /// - `sources`: Ordered list of sources (L0 SST ULIDs and/or existing SR ids).
    /// - `destination`: Sorted Run id for the compaction output.
    pub fn new(sources: Vec<SourceId>, destination: u32) -> Self {
        Self {
            sources,
            destination,
        }
    }

    /// The sources (input SSTs and sorted runs) for this compaction.
    pub fn sources(&self) -> &Vec<SourceId> {
        &self.sources
    }

    /// The destination sorted run id that will be produced by this compaction.
    pub fn destination(&self) -> u32 {
        self.destination
    }
}

impl Display for CompactionSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let displayed_sources: Vec<String> =
            self.sources().iter().map(|s| format!("{}", s)).collect();
        write!(f, "{:?} -> SR({})", displayed_sources, self.destination())
    }
}

/// Lifecycle status for a compaction.
///
/// State transitions:
/// ```text
/// Submitted <-> Running --> Completed
///     |             |
///     |             v
///     +----------> Failed
/// ```
///
/// `Completed` and `Failed` are terminal states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum CompactionStatus {
    /// The compaction has been submitted but not yet started.
    Submitted,
    /// The compaction is currently running.
    Running,
    /// The compaction finished successfully.
    Completed,
    /// The compaction failed. It might or might not have started before failure.
    Failed,
}

impl CompactionStatus {
    fn active(self) -> bool {
        matches!(
            self,
            CompactionStatus::Submitted | CompactionStatus::Running
        )
    }

    fn finished(self) -> bool {
        matches!(self, CompactionStatus::Completed | CompactionStatus::Failed)
    }
}

/// Canonical, internal record of a compaction.
///
/// A compaction is the unit tracked by the compactor: it has a stable `id` (ULID) and a `spec`
/// (what to compact and where).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Compaction {
    /// Stable id (ULID) used to track this compaction across messages and attempts.
    id: Ulid,
    /// What to compact (sources) and where to write (destination).
    spec: CompactionSpec,
    /// Total number of bytes processed so far for this compaction.
    bytes_processed: u64,
    /// Current status for this compaction.
    ///
    /// This is tracked only in memory at the moment.
    status: CompactionStatus,
    /// Output SSTs produced by this compaction, if any.
    output_ssts: Vec<SsTableHandle>,
}

impl Compaction {
    pub(crate) fn new(id: Ulid, spec: CompactionSpec) -> Self {
        Self {
            id,
            spec,
            bytes_processed: 0,
            status: CompactionStatus::Submitted,
            output_ssts: Vec::new(),
        }
    }

    pub(crate) fn with_status(mut self, status: CompactionStatus) -> Self {
        self.status = status;
        self
    }

    pub(crate) fn with_output_ssts(mut self, output_ssts: Vec<SsTableHandle>) -> Self {
        self.output_ssts = output_ssts;
        self
    }

    /// Returns all sorted run sources for this compaction.
    ///
    /// ## Arguments
    /// - `db_state`: The current core DB state from the manifest.
    pub(crate) fn get_sorted_runs(&self, db_state: &ManifestCore) -> Vec<SortedRun> {
        let srs_by_id: HashMap<u32, &SortedRun> =
            db_state.compacted.iter().map(|sr| (sr.id, sr)).collect();

        self.spec
            .sources()
            .iter()
            .filter_map(|s| s.maybe_unwrap_sorted_run())
            .filter_map(|id| srs_by_id.get(&id).map(|t| (*t).clone()))
            .collect()
    }

    /// Returns all L0 SSTable sources for this compaction.
    ///
    /// ## Arguments
    /// - `db_state`: The current core DB state from the manifest.
    pub(crate) fn get_ssts(&self, db_state: &ManifestCore) -> Vec<SsTableHandle> {
        let ssts_by_id: HashMap<Ulid, &SsTableHandle> = db_state
            .l0
            .iter()
            .map(|sst| (sst.id.unwrap_compacted_id(), sst))
            .collect();

        self.spec
            .sources()
            .iter()
            .filter_map(|s| s.maybe_unwrap_sst())
            .filter_map(|ulid| ssts_by_id.get(&ulid).map(|t| (*t).clone()))
            .collect()
    }

    /// The stable id (ULID) used to track this compaction across messages and attempts.
    pub fn id(&self) -> Ulid {
        self.id
    }

    /// Returns the immutable compaction spec describing inputs and destination.
    pub fn spec(&self) -> &CompactionSpec {
        &self.spec
    }

    /// Sets bytes processed so far for this compaction.
    pub(crate) fn set_bytes_processed(&mut self, bytes: u64) {
        self.bytes_processed = bytes;
    }

    /// Gets the bytes processed so far.
    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed
    }

    /// Returns the current status of this compaction.
    pub fn status(&self) -> CompactionStatus {
        self.status
    }

    /// Sets the output SSTs produced by this compaction.
    pub(crate) fn set_output_ssts(&mut self, output_ssts: Vec<SsTableHandle>) {
        assert!(
            output_ssts.starts_with(self.output_ssts.as_slice()),
            "new output SSTs must always extend previous output SSTs"
        );
        self.output_ssts = output_ssts;
    }

    /// Returns the output SSTs produced by this compaction.
    pub fn output_ssts(&self) -> &Vec<SsTableHandle> {
        &self.output_ssts
    }

    /// Sets the current status of this compaction.
    pub(crate) fn set_status(&mut self, status: CompactionStatus) {
        self.status = status;
    }

    /// Returns true if the compaction is active (submitted or running).
    pub fn active(&self) -> bool {
        self.status.active()
    }
}

impl Display for Compaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let displayed_sources: Vec<_> = self
            .spec
            .sources()
            .iter()
            .map(|s| format!("{}", s))
            .collect();
        write!(
            f,
            "{:?} -> SR({})",
            displayed_sources,
            self.spec.destination(),
        )?;
        if self.bytes_processed > 0 {
            let human_bytes_processed = crate::utils::format_bytes_si(self.bytes_processed);

            write!(f, " ({} processed)", human_bytes_processed)?;
        }
        Ok(())
    }
}

/// Represents an immutable in-memory view of .compactions file that is suitable
/// to expose to end-users.
#[derive(Clone, Debug, Serialize)]
pub struct CompactionsCore {
    /// The set of recent compactions tracked by this compactor. These may
    /// be pending, in progress, or recently completed (either with success
    /// or failure).
    recent_compactions: BTreeMap<Ulid, Compaction>,
}

impl CompactionsCore {
    pub(crate) fn new() -> Self {
        Self {
            recent_compactions: BTreeMap::new(),
        }
    }

    pub(crate) fn with_compactions(mut self, compactions: BTreeMap<Ulid, Compaction>) -> Self {
        self.recent_compactions = compactions;
        self
    }

    /// Returns an iterator over all recent compactions. Recent compactions include all
    /// active (submitted or running) compactions as well as the most recently finished
    /// compaction (failed or completed).
    pub fn recent_compactions(&self) -> impl Iterator<Item = &Compaction> {
        self.recent_compactions.values()
    }
}

/// Container for compactions tracked by the compactor alongside its epoch.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct Compactions {
    // The current compactor's epoch.
    pub(crate) compactor_epoch: u64,
    pub(crate) core: CompactionsCore,
}

impl Compactions {
    pub(crate) fn new(compactor_epoch: u64) -> Self {
        Self {
            compactor_epoch,
            core: CompactionsCore::new(),
        }
    }

    pub(crate) fn with_compactions(mut self, compactions: Vec<Compaction>) -> Self {
        let recent_compactions = compactions
            .into_iter()
            .map(|c| (c.id(), c))
            .collect::<BTreeMap<Ulid, Compaction>>();
        self.core.recent_compactions = recent_compactions;
        self
    }

    /// Inserts a new compaction to be tracked.
    pub(crate) fn insert(&mut self, compaction: Compaction) {
        self.core
            .recent_compactions
            .insert(compaction.id, compaction);
    }

    /// Returns the tracked compaction for the specified id, if any.
    pub(crate) fn get(&self, compaction_id: &Ulid) -> Option<&Compaction> {
        self.core.recent_compactions.get(compaction_id)
    }

    pub(crate) fn get_mut(&mut self, compaction_id: &Ulid) -> Option<&mut Compaction> {
        self.core.recent_compactions.get_mut(compaction_id)
    }

    /// Returns true if the specified compaction id is being tracked.
    #[cfg(test)]
    pub(crate) fn contains(&self, compaction_id: &Ulid) -> bool {
        self.core.recent_compactions.contains_key(compaction_id)
    }

    /// Returns an iterator over all tracked compactions.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Compaction> {
        self.core.recent_compactions.values()
    }

    /// Returns an iterator over mutable compactions.
    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Compaction> {
        self.core.recent_compactions.values_mut()
    }

    /// Returns an iterator over all active (submitted or running) compactions.
    pub(crate) fn iter_active(&self) -> impl Iterator<Item = &Compaction> {
        self.core.recent_compactions.values().filter(|c| c.active())
    }

    /// Returns an iterator over all compactions with the given status.
    pub(crate) fn iter_with_status(
        &self,
        status: CompactionStatus,
    ) -> impl Iterator<Item = &Compaction> {
        self.core
            .recent_compactions
            .values()
            .filter(move |c| c.status() == status)
    }

    /// Keeps the most recently finished compaction and any active compactions, and removes others.
    pub(crate) fn retain_active_and_last_finished(&mut self) {
        let latest_finished = self
            .core
            .recent_compactions
            .iter()
            .filter_map(|(_, c)| {
                if c.status().finished() {
                    Some(c.id())
                } else {
                    None
                }
            })
            .max();

        self.core
            .recent_compactions
            .retain(|id, compaction| compaction.active() || Some(id) == latest_finished.as_ref());
    }
}

/// Process-local runtime state owned by the compactor.
///
/// This is the in-memory view that a single compactor task uses to:
/// - keep a fresh `DirtyManifest` (view of `CoreDbState`),
/// - track in-flight compactions by id (ULID).
pub struct CompactorState {
    manifest: DirtyObject<Manifest>,
    compactions: DirtyObject<Compactions>,
}

impl CompactorState {
    /// Creates a new compactor state seeded with the provided dirty manifest and compactions.
    pub(crate) fn new(
        manifest: DirtyObject<Manifest>,
        compactions: DirtyObject<Compactions>,
    ) -> Self {
        assert_eq!(
            manifest.value.compactor_epoch,
            compactions.value.compactor_epoch
        );
        Self {
            manifest,
            compactions,
        }
    }

    /// Returns the current in-memory core DB state derived from the manifest.
    pub(crate) fn db_state(&self) -> &ManifestCore {
        &self.manifest.value.core
    }

    /// Returns the local dirty manifest that will be written back after compactions.
    pub(crate) fn manifest(&self) -> &DirtyObject<Manifest> {
        &self.manifest
    }

    /// Returns an iterator over all in-flight compactions.
    pub(crate) fn active_compactions(&self) -> impl Iterator<Item = &Compaction> {
        self.compactions.value.iter_active()
    }

    pub(crate) fn compactions_with_status(
        &self,
        status: CompactionStatus,
    ) -> impl Iterator<Item = &Compaction> {
        self.compactions.value.iter_with_status(status)
    }

    /// Returns the dirty compactions tracked by this state.
    pub(crate) fn compactions(&self) -> &DirtyObject<Compactions> {
        &self.compactions
    }

    /// Replaces the tracked dirty compactions with the provided value.
    pub(crate) fn set_compactions(&mut self, compactions: DirtyObject<Compactions>) {
        assert_eq!(
            self.manifest.value.compactor_epoch,
            compactions.value.compactor_epoch
        );
        self.compactions = compactions;
    }

    /// Merges the remote compactions view into the compactor's local state.
    ///
    /// This preserves local in-flight compactions while pulling in newly submitted remote
    /// compactions (e.g. external scheduling requests).
    pub(crate) fn merge_remote_compactions(
        &mut self,
        mut remote_compactions: DirtyObject<Compactions>,
    ) {
        let mut merged = BTreeMap::new();

        for compaction in self.compactions.value.iter() {
            merged.insert(compaction.id(), compaction.clone());
        }

        for compaction in remote_compactions.value.iter() {
            if let Entry::Vacant(v) = merged.entry(compaction.id()) {
                // The compactor should control all compaction state transitions. If the
                // compactor finds a new remote compaction (a compaction that the compactor
                // didn't create), it must be in `Submitted` status (the beginning status).
                if !matches!(compaction.status(), CompactionStatus::Submitted) {
                    error!(
                        "skipping remote commpaction with unexpected (non-Submitted) status [compaction={:?}]",
                        compaction,
                    );
                    continue;
                }
                v.insert(compaction.clone());
            }
        }

        let mut merged_compactions = Compactions {
            compactor_epoch: self.compactions.value.compactor_epoch,
            core: CompactionsCore::new().with_compactions(merged),
        };
        merged_compactions.retain_active_and_last_finished();
        remote_compactions.value = merged_compactions;
        self.set_compactions(remote_compactions);
    }

    /// Merges the remote (writer) manifest view into the compactor's local state.
    ///
    /// This preserves local knowledge about compactions already applied (e.g., L0 last
    /// compacted marker, existing compacted runs) while pulling in newly created L0 SSTs
    /// and other writer-updated fields.
    pub(crate) fn merge_remote_manifest(&mut self, mut remote_manifest: DirtyObject<Manifest>) {
        // the writer may have added more l0 SSTs. Add these to our l0 list.
        let my_db_state = self.db_state();
        let last_compacted_l0 = my_db_state.l0_last_compacted;
        let mut merged_l0s = VecDeque::new();
        let writer_l0 = &remote_manifest.value.core.l0;
        for writer_l0_sst in writer_l0 {
            let writer_l0_id = writer_l0_sst.id.unwrap_compacted_id();
            // todo: this is brittle. we are relying on the l0 list always being updated in
            //       an expected order. We should instead encode the ordering in the l0 SST IDs
            //       and assert that it follows the order
            if match &last_compacted_l0 {
                None => true,
                Some(last_compacted_l0_id) => writer_l0_id != *last_compacted_l0_id,
            } {
                merged_l0s.push_back(writer_l0_sst.clone());
            } else {
                break;
            }
        }

        // write out the merged core db state and manifest
        let merged = ManifestCore {
            initialized: remote_manifest.value.core.initialized,
            l0_last_compacted: my_db_state.l0_last_compacted,
            l0: merged_l0s,
            compacted: my_db_state.compacted.clone(),
            next_wal_sst_id: remote_manifest.value.core.next_wal_sst_id,
            replay_after_wal_id: remote_manifest.value.core.replay_after_wal_id,
            last_l0_clock_tick: remote_manifest.value.core.last_l0_clock_tick,
            last_l0_seq: remote_manifest.value.core.last_l0_seq,
            checkpoints: remote_manifest.value.core.checkpoints.clone(),
            wal_object_store_uri: my_db_state.wal_object_store_uri.clone(),
            recent_snapshot_min_seq: remote_manifest.value.core.recent_snapshot_min_seq,
            sequence_tracker: remote_manifest.value.core.sequence_tracker,
        };
        remote_manifest.value.core = merged;
        self.manifest = remote_manifest;
    }

    /// Validates and registers a newly submitted compaction with this compactor.
    ///
    /// ## Returns
    /// - `Ok(())` if accepted, or [`SlateDBError::InvalidCompaction`] if the compaction conflicts
    ///   with an existing destination or violates destination overwrite rules.
    pub(crate) fn add_compaction(&mut self, compaction: Compaction) -> Result<(), SlateDBError> {
        let spec = compaction.spec();
        if self
            .compactions
            .value
            .iter_active()
            .map(|c| c.spec())
            .any(|c| c.destination() == spec.destination())
        {
            // we already have an ongoing compaction for this destination
            return Err(SlateDBError::InvalidCompaction);
        }
        if self
            .db_state()
            .compacted
            .iter()
            .any(|sr| sr.id == spec.destination())
            && !spec.sources().iter().any(|src| match src {
                SourceId::SortedRun(sr) => *sr == spec.destination(),
                SourceId::Sst(_) => false,
            })
        {
            // the compaction overwrites an existing sr but doesn't include the sr
            return Err(SlateDBError::InvalidCompaction);
        }
        info!("accepted submitted compaction [compaction={}]", compaction);

        self.compactions
            .value
            .core
            .recent_compactions
            .insert(compaction.id(), compaction);
        Ok(())
    }

    /// Mutates a compaction in place if it exists, then trims retained state.
    pub(crate) fn update_compaction<F>(&mut self, compaction_id: &Ulid, f: F)
    where
        F: FnOnce(&mut Compaction),
    {
        if let Some(compaction) = self
            .compactions
            .value
            .core
            .recent_compactions
            .get_mut(compaction_id)
        {
            f(compaction);
        }
        self.compactions.value.retain_active_and_last_finished();
    }

    /// Applies the effects of a finished compaction to the in-memory manifest.
    ///
    /// This removes compacted L0 SSTs and source SRs, inserts the output SR in id-descending
    /// order, updates `l0_last_compacted`, and marks the compaction finished (retaining the most
    /// recent finished compaction for GC; see #1044).
    pub(crate) fn finish_compaction(&mut self, compaction_id: Ulid, output_sr: SortedRun) {
        let mut db_state = self.db_state().clone();
        if let Some(compaction) = self.compactions.value.get_mut(&compaction_id) {
            let spec = compaction.spec();
            info!("finished compaction [spec={}]", spec);
            // reconstruct l0
            let compaction_l0s: HashSet<Ulid> = spec
                .sources()
                .iter()
                .filter_map(|id| id.maybe_unwrap_sst())
                .collect();
            let compaction_srs: HashSet<u32> = spec
                .sources()
                .iter()
                .chain(std::iter::once(&SourceId::SortedRun(spec.destination())))
                .filter_map(|id| id.maybe_unwrap_sorted_run())
                .collect();
            let new_l0: VecDeque<SsTableHandle> = db_state
                .l0
                .iter()
                .filter_map(|l0| {
                    let l0_id = l0.id.unwrap_compacted_id();
                    if compaction_l0s.contains(&l0_id) {
                        return None;
                    }
                    Some(l0.clone())
                })
                .collect();
            let mut new_compacted = Vec::new();
            let mut inserted = false;
            for compacted in db_state.compacted.iter() {
                if !inserted && output_sr.id >= compacted.id {
                    new_compacted.push(output_sr.clone());
                    inserted = true;
                }
                if !compaction_srs.contains(&compacted.id) {
                    new_compacted.push(compacted.clone());
                }
            }
            if !inserted {
                new_compacted.push(output_sr.clone());
            }
            Self::assert_compacted_srs_in_id_order(&new_compacted);
            let first_source = spec
                .sources()
                .first()
                .expect("illegal: empty compaction spec");
            if let Some(compacted_l0) = first_source.maybe_unwrap_sst() {
                // if there are l0s, the newest must be the first entry in sources
                // TODO: validate that this is the case
                db_state.l0_last_compacted = Some(compacted_l0)
            }
            db_state.l0 = new_l0;
            db_state.compacted = new_compacted;
            self.manifest.value.core = db_state;
            self.update_compaction(&compaction_id, |c| {
                c.set_status(CompactionStatus::Completed);
            });
        } else {
            error!("compaction not found [compaction_id={}]", compaction_id);
        }
    }

    /// Debug assertion that compacted sorted runs are kept in strictly descending id order.
    fn assert_compacted_srs_in_id_order(compacted: &[SortedRun]) {
        let mut last_sr_id = u32::MAX;
        for sr in compacted.iter() {
            assert!(sr.id < last_sr_id);
            last_sr_id = sr.id;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use super::*;
    use crate::checkpoint::Checkpoint;
    use crate::compactor_state::SourceId::Sst;
    use crate::config::Settings;
    use crate::db::Db;
    use crate::db_state::SsTableId;
    use crate::manifest::store::test_utils::new_dirty_manifest;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::utils::IdGenerator;
    use crate::DbRand;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
    use slatedb_txn_obj::test_utils::new_dirty_object;
    use tokio::runtime::{Handle, Runtime};

    const PATH: &str = "/test/db";

    #[test]
    fn test_trim_keeps_latest_finished_and_active_compactions() {
        let mut compactions = Compactions::new(0);
        let oldest_finished = Ulid::from_parts(1, 0);
        let latest_finished = Ulid::from_parts(2, 0);
        let active = Ulid::from_parts(3, 0);

        compactions.insert(compaction_with_status(
            oldest_finished,
            CompactionStatus::Completed,
        ));
        compactions.insert(compaction_with_status(
            latest_finished,
            CompactionStatus::Completed,
        ));
        compactions.insert(compaction_with_status(active, CompactionStatus::Submitted));

        compactions.retain_active_and_last_finished();

        assert!(compactions.contains(&active));
        assert!(compactions.contains(&latest_finished));
        assert!(!compactions.contains(&oldest_finished));
    }

    #[test]
    fn test_compaction_status_active_and_finished() {
        assert!(CompactionStatus::Submitted.active());
        assert!(CompactionStatus::Running.active());
        assert!(!CompactionStatus::Completed.active());
        assert!(!CompactionStatus::Failed.active());

        assert!(!CompactionStatus::Submitted.finished());
        assert!(!CompactionStatus::Running.finished());
        assert!(CompactionStatus::Completed.finished());
        assert!(CompactionStatus::Failed.finished());
    }

    #[test]
    fn test_trim_keeps_only_most_recent_finished_when_no_active() {
        let mut compactions = Compactions::new(0);
        let older = Ulid::from_parts(10, 0);
        let middle = Ulid::from_parts(20, 0);
        let newest = Ulid::from_parts(30, 0);

        compactions.insert(compaction_with_status(older, CompactionStatus::Completed));
        compactions.insert(compaction_with_status(middle, CompactionStatus::Failed));
        compactions.insert(compaction_with_status(newest, CompactionStatus::Completed));

        compactions.retain_active_and_last_finished();

        assert!(!compactions.contains(&older));
        assert!(!compactions.contains(&middle));
        assert!(compactions.contains(&newest));
    }

    #[test]
    fn test_trim_preserves_all_active_when_no_finished() {
        let mut compactions = Compactions::new(0);
        let submitted = Ulid::from_parts(1, 0);
        let running = Ulid::from_parts(2, 0);

        compactions.insert(compaction_with_status(
            submitted,
            CompactionStatus::Submitted,
        ));
        compactions.insert(compaction_with_status(running, CompactionStatus::Running));

        compactions.retain_active_and_last_finished();

        assert!(compactions.contains(&submitted));
        assert!(compactions.contains(&running));
    }

    #[test]
    fn test_merge_remote_compactions_preserves_local_and_adds_remote() {
        let manifest = new_dirty_manifest();
        let compactor_epoch = manifest.value.compactor_epoch;
        let local_running = Ulid::from_parts(1, 0);
        let local_finished = Ulid::from_parts(2, 0);

        let mut local_compactions = new_dirty_compactions(compactor_epoch);
        local_compactions.value.insert(compaction_with_status(
            local_running,
            CompactionStatus::Running,
        ));
        local_compactions.value.insert(compaction_with_status(
            local_finished,
            CompactionStatus::Completed,
        ));

        let mut state = CompactorState::new(manifest, local_compactions);

        let remote_submitted = Ulid::from_parts(3, 0);
        let mut remote_compactions = new_dirty_compactions(compactor_epoch);
        remote_compactions.value.insert(compaction_with_status(
            remote_submitted,
            CompactionStatus::Submitted,
        ));

        state.merge_remote_compactions(remote_compactions);

        let merged = &state.compactions.value;
        assert!(merged.contains(&local_running));
        assert_eq!(
            merged.get(&local_running).expect("not found").status(),
            CompactionStatus::Running
        );
        assert!(merged.contains(&local_finished));
        assert_eq!(
            merged.get(&local_finished).expect("not found").status(),
            CompactionStatus::Completed
        );
        assert!(merged.contains(&remote_submitted));
        assert_eq!(
            merged.get(&remote_submitted).expect("not found").status(),
            CompactionStatus::Submitted
        );
    }

    #[test]
    fn test_merge_remote_compactions_drops_non_submitted_remote() {
        let manifest = new_dirty_manifest();
        let compactor_epoch = manifest.value.compactor_epoch;

        let local_running = Ulid::from_parts(1, 0);
        let mut local_compactions = new_dirty_compactions(compactor_epoch);
        local_compactions.value.insert(compaction_with_status(
            local_running,
            CompactionStatus::Running,
        ));
        let mut state = CompactorState::new(manifest, local_compactions);

        let remote_running = Ulid::from_parts(3, 0);
        let mut remote_compactions = new_dirty_compactions(compactor_epoch);
        remote_compactions.value.insert(compaction_with_status(
            remote_running,
            CompactionStatus::Running,
        ));

        state.merge_remote_compactions(remote_compactions);

        let merged = &state.compactions.value;
        assert!(merged.contains(&local_running));
        assert_eq!(
            merged.get(&local_running).expect("not found").status(),
            CompactionStatus::Running
        );
        assert!(!merged.contains(&remote_running));
    }

    #[test]
    fn test_should_register_compaction() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());

        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = build_l0_compaction(&state.db_state().l0, 0);
        // when:
        let compaction = Compaction::new(compaction_id, spec.clone());
        state
            .add_compaction(compaction.clone())
            .expect("failed to add compaction");

        // then:
        let mut compactions = state.active_compactions();
        let expected = Compaction::new(compaction_id, spec.clone());
        assert_eq!(compactions.next().expect("compaction not found"), &expected);
        assert!(compactions.next().is_none());
    }

    #[test]
    fn test_should_update_dbstate_when_compaction_finished() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = build_l0_compaction(&before_compaction.l0, 0);
        let compaction = Compaction::new(compaction_id, spec);
        state
            .add_compaction(compaction.clone())
            .expect("failed to add compaction");

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_compaction(compaction_id, sr.clone());

        // then:
        assert_eq!(
            state.db_state().l0_last_compacted,
            Some(
                before_compaction
                    .l0
                    .front()
                    .unwrap()
                    .id
                    .unwrap_compacted_id()
            )
        );
        assert_eq!(state.db_state().l0.len(), 0);
        assert_eq!(state.db_state().compacted.len(), 1);
        assert_eq!(state.db_state().compacted.first().unwrap().id, sr.id);
        let expected_ids: Vec<SsTableId> = sr.ssts.iter().map(|h| h.id).collect();
        let found_ids: Vec<SsTableId> = state
            .db_state()
            .compacted
            .first()
            .unwrap()
            .ssts
            .iter()
            .map(|h| h.id)
            .collect();
        assert_eq!(expected_ids, found_ids);
    }

    #[test]
    fn test_should_remove_compaction_when_compaction_finished() {
        // given:
        let rt = build_runtime();
        let (_, _, mut state, system_clock, rand) = build_test_state(rt.handle());
        let before_compaction = state.db_state().clone();
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = build_l0_compaction(&before_compaction.l0, 0);
        let compaction = Compaction::new(compaction_id, spec);
        state
            .add_compaction(compaction.clone())
            .expect("failed to add compaction");

        // when:
        let compacted_ssts = before_compaction.l0.iter().cloned().collect();
        let sr = SortedRun {
            id: 0,
            ssts: compacted_ssts,
        };
        state.finish_compaction(compaction_id, sr.clone());

        // then:
        assert_eq!(state.active_compactions().count(), 0)
    }

    #[test]
    fn test_should_merge_db_state_correctly_when_never_compacted() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state, _, _) = build_test_state(rt.handle());
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48])).unwrap();
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48])).unwrap();
        wait_for_manifest_with_l0_len(&mut sm, rt.handle(), state.db_state().l0.len() + 1);

        // when:
        state.merge_remote_manifest(sm.prepare_dirty().unwrap());

        // then:
        assert!(state.db_state().l0_last_compacted.is_none());
        let expected_merged_l0s: Vec<Ulid> = sm
            .manifest()
            .core
            .l0
            .iter()
            .map(|t| t.id.unwrap_compacted_id())
            .collect();
        let merged_l0s: Vec<Ulid> = state
            .db_state()
            .l0
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        assert_eq!(expected_merged_l0s, merged_l0s);
    }

    #[test]
    fn test_should_merge_db_state_correctly() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = CompactionSpec::new(
            vec![Sst(original_l0s.back().unwrap().id.unwrap_compacted_id())],
            0,
        );
        let compaction = Compaction::new(compaction_id, spec);
        state
            .add_compaction(compaction.clone())
            .expect("failed to add compaction");
        state.finish_compaction(
            compaction_id,
            SortedRun {
                id: 0,
                ssts: vec![original_l0s.back().unwrap().clone()],
            },
        );
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48])).unwrap();
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48])).unwrap();
        wait_for_manifest_with_l0_len(&mut sm, rt.handle(), original_l0s.len() + 1);
        let db_state_before_merge = state.db_state().clone();

        // when:
        state.merge_remote_manifest(sm.prepare_dirty().unwrap());

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s: VecDeque<Ulid> = original_l0s
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        expected_merged_l0s.pop_back();
        let new_l0 = sm
            .manifest()
            .core
            .l0
            .front()
            .unwrap()
            .id
            .unwrap_compacted_id();
        expected_merged_l0s.push_front(new_l0);
        let merged_l0: VecDeque<Ulid> = db_state
            .l0
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        assert_eq!(merged_l0, expected_merged_l0s);
        assert_eq!(
            compacted_to_description(&db_state.compacted),
            compacted_to_description(&db_state_before_merge.compacted)
        );
        assert_eq!(
            db_state.replay_after_wal_id,
            sm.manifest().core.replay_after_wal_id
        );
        assert_eq!(db_state.next_wal_sst_id, sm.manifest().core.next_wal_sst_id);
    }

    #[test]
    fn test_should_merge_db_state_correctly_when_all_l0_compacted() {
        // given:
        let rt = build_runtime();
        let (os, mut sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());

        let spec = CompactionSpec::new(
            original_l0s
                .iter()
                .map(|h| Sst(h.id.unwrap_compacted_id()))
                .collect(),
            0,
        );
        let compaction = Compaction::new(compaction_id, spec);
        state
            .add_compaction(compaction.clone())
            .expect("failed to add compaction");
        state.finish_compaction(
            compaction_id,
            SortedRun {
                id: 0,
                ssts: original_l0s.clone().into(),
            },
        );
        assert_eq!(state.db_state().l0.len(), 0);
        // open a new db and write another l0
        let db = build_db(os.clone(), rt.handle());
        rt.block_on(db.put(&[b'a'; 16], &[b'b'; 48])).unwrap();
        rt.block_on(db.put(&[b'j'; 16], &[b'k'; 48])).unwrap();
        wait_for_manifest_with_l0_len(&mut sm, rt.handle(), original_l0s.len() + 1);

        // when:
        state.merge_remote_manifest(sm.prepare_dirty().unwrap());

        // then:
        let db_state = state.db_state();
        let mut expected_merged_l0s = VecDeque::new();
        let new_l0 = sm
            .manifest()
            .core
            .l0
            .front()
            .unwrap()
            .id
            .unwrap_compacted_id();
        expected_merged_l0s.push_front(new_l0);
        let merged_l0: VecDeque<Ulid> = db_state
            .l0
            .iter()
            .map(|h| h.id.unwrap_compacted_id())
            .collect();
        assert_eq!(merged_l0, expected_merged_l0s);
    }

    #[test]
    fn test_should_merge_db_state_with_new_checkpoints() {
        // given:
        let manifest = new_dirty_manifest();
        let compactions = new_dirty_compactions(manifest.value.compactor_epoch);
        let mut state = CompactorState::new(manifest, compactions);
        // mimic an externally added checkpoint
        let mut dirty = new_dirty_manifest();
        let checkpoint = Checkpoint {
            id: uuid::Uuid::new_v4(),
            manifest_id: 1,
            expire_time: None,
            create_time: DefaultSystemClock::default().now(),
            name: None,
        };
        dirty.value.core.checkpoints.push(checkpoint.clone());

        // when:
        state.merge_remote_manifest(dirty);

        // then:
        assert_eq!(vec![checkpoint], state.db_state().checkpoints);
    }

    #[test]
    fn test_should_submit_correct_compaction() {
        // given:
        let rt = build_runtime();
        let (_os, mut _sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        // compact the last sst
        let original_l0s = &state.db_state().clone().l0;
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = CompactionSpec::new(
            original_l0s
                .iter()
                .enumerate()
                .filter(|(i, _e)| i > &2usize)
                .map(|(_i, x)| Sst(x.id.unwrap_compacted_id()))
                .collect::<Vec<SourceId>>(),
            0,
        );
        let compaction = Compaction::new(compaction_id, spec);
        let result = state.add_compaction(compaction.clone());

        // then:
        assert!(result.is_ok());
    }

    #[test]
    fn test_source_boundary_compaction() {
        // given:
        let rt = build_runtime();
        let (_os, mut _sm, mut state, system_clock, rand) = build_test_state(rt.handle());
        let original_l0s = &state.db_state().clone().l0;
        let original_srs = &state.db_state().clone().compacted;
        // L0: from 4th onward (index > 2)
        let l0_sources = original_l0s
            .iter()
            .skip(3)
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()));

        // SRs: first 3 (index < 3)
        let sr_sources = original_srs
            .iter()
            .take(3)
            .map(|sr| SourceId::SortedRun(sr.id));

        // If you need both:
        let sources: Vec<SourceId> = l0_sources.chain(sr_sources).collect();

        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        let spec = CompactionSpec::new(sources, 0);
        let compaction = Compaction::new(compaction_id, spec);
        let result = state.add_compaction(compaction.clone());

        // or simply:
        assert!(result.is_ok());
    }

    // test helpers

    fn new_dirty_compactions(compactor_epoch: u64) -> DirtyObject<Compactions> {
        new_dirty_object(1u64, Compactions::new(compactor_epoch))
    }

    fn compaction_with_status(id: Ulid, status: CompactionStatus) -> Compaction {
        Compaction::new(id, CompactionSpec::new(vec![], 0)).with_status(status)
    }

    fn run_for<T, F>(duration: Duration, mut f: F) -> Option<T>
    where
        F: FnMut() -> Option<T>,
    {
        let clock = DefaultSystemClock::default();
        let start = clock.now();
        while clock
            .now()
            .signed_duration_since(start)
            .to_std()
            .expect("duration < 0 not allowed")
            < duration
        {
            let maybe_result = f();
            if maybe_result.is_some() {
                return maybe_result;
            }
            sleep(Duration::from_millis(100));
        }
        None
    }

    #[derive(PartialEq, Debug)]
    struct SortedRunDescription {
        id: u32,
        ssts: Vec<SsTableId>,
    }

    fn build_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn compacted_to_description(compacted: &[SortedRun]) -> Vec<SortedRunDescription> {
        compacted.iter().map(sorted_run_to_description).collect()
    }

    fn sorted_run_to_description(sr: &SortedRun) -> SortedRunDescription {
        SortedRunDescription {
            id: sr.id,
            ssts: sr.ssts.iter().map(|h| h.id).collect(),
        }
    }

    fn wait_for_manifest_with_l0_len(
        stored_manifest: &mut StoredManifest,
        tokio_handle: &Handle,
        len: usize,
    ) {
        run_for(Duration::from_secs(30), || {
            let manifest = tokio_handle.block_on(stored_manifest.refresh()).unwrap();
            if manifest.core.l0.len() == len {
                return Some(manifest.core.clone());
            }
            None
        })
        .expect("no manifest found with l0 len");
    }

    fn build_l0_compaction(ssts: &VecDeque<SsTableHandle>, dst: u32) -> CompactionSpec {
        let sources = ssts
            .iter()
            .map(|h| SourceId::Sst(h.id.unwrap_compacted_id()))
            .collect();
        CompactionSpec::new(sources, dst)
    }

    fn build_db(os: Arc<dyn ObjectStore>, tokio_handle: &Handle) -> Db {
        let opts = Settings {
            l0_sst_size_bytes: 256,
            // make sure to run with the compactor disabled. The tests will explicitly
            // manage compaction execution and assert the associated state mutations.
            compactor_options: None,
            ..Default::default()
        };
        tokio_handle
            .block_on(Db::builder(PATH, os.clone()).with_settings(opts).build())
            .unwrap()
    }

    #[allow(clippy::type_complexity)]
    fn build_test_state(
        tokio_handle: &Handle,
    ) -> (
        Arc<dyn ObjectStore>,
        StoredManifest,
        CompactorState,
        Arc<dyn SystemClock>,
        Arc<DbRand>,
    ) {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = build_db(os.clone(), tokio_handle);
        let l0_count: u64 = 5;
        for i in 0..l0_count {
            tokio_handle
                .block_on(db.put(&[b'a' + i as u8; 16], &[b'b' + i as u8; 48]))
                .unwrap();
            tokio_handle
                .block_on(db.put(&[b'j' + i as u8; 16], &[b'k' + i as u8; 48]))
                .unwrap();
        }
        tokio_handle.block_on(db.close()).unwrap();
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand: Arc<DbRand> = Arc::new(DbRand::default());

        let manifest_store = Arc::new(ManifestStore::new(&Path::from(PATH), os.clone()));
        let stored_manifest = tokio_handle
            .block_on(StoredManifest::load(
                manifest_store,
                Arc::new(DefaultSystemClock::new()),
            ))
            .unwrap();
        let compactions = new_dirty_compactions(stored_manifest.manifest().compactor_epoch);
        let state = CompactorState::new(stored_manifest.prepare_dirty().unwrap(), compactions);
        (os, stored_manifest, state, system_clock, rand)
    }
}
