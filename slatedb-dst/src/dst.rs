//! Scenario-driven deterministic simulation testing for SlateDB.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use parking_lot::Mutex;
use slatedb::bytes::Bytes;
use slatedb::config::{DurabilityLevel, PutOptions, ScanOptions, Settings, Ttl, WriteOptions};
use slatedb::{Db, Error, RowEntry, ValueDeletable, WriteBatch, WriteHandle};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::scenarios::ScanScenario;
use crate::state::{SQLiteState, StateSnapshot};

/// A workload definition executed by [`Dst::run_scenarios`].
///
/// A scenario is a named async task that runs against a shared SlateDB instance
/// through a [`ScenarioContext`]. Scenarios are allowed to be `!Send`, so they
/// run on Tokio local tasks rather than on the multi-threaded task scheduler.
///
/// Implementations typically model one independent actor in a simulation, such
/// as a writer, reader, flusher, or shutdown controller. Multiple scenarios may
/// run concurrently against the same [`Dst`] instance.
#[async_trait(?Send)]
pub trait Scenario {
    /// Returns a stable name for this scenario.
    ///
    /// The name is recorded in SQLite state rows and included in mismatch
    /// errors, so it should identify the workload instance clearly enough to
    /// debug failures.
    fn name(&self) -> &'static str;

    /// Returns whether this scenario is intended to run until the shared
    /// shutdown token is cancelled.
    ///
    /// By default, scenarios are treated as finite. [`Dst::run_scenarios`]
    /// cancels the shared shutdown token after all finite scenarios complete,
    /// which gives any `runs_forever()` scenarios a deterministic signal to
    /// stop. Override this to `true` for background workloads such as clock
    /// tickers or wall-clock shutdown sentinels.
    fn runs_forever(&self) -> bool {
        false
    }

    /// Runs this scenario to completion.
    ///
    /// The provided [`ScenarioContext`] exposes checked mutating helpers,
    /// point-in-time access to the recorded SQLite state, the shared mock
    /// clock, and a run-scoped shutdown token. Return an [`Error`] to fail the
    /// entire `run_scenarios()` invocation.
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error>;
}

#[derive(Debug, Clone, PartialEq)]
enum ScenarioWriteEntry {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        options: PutOptions,
    },
    Delete {
        key: Vec<u8>,
    },
}

/// A deduplicating write builder for [`ScenarioContext::write_batch`].
///
/// The batch stores operations in key order and keeps only the latest staged
/// operation for each key. If the same key is added multiple times, the last
/// `put` or `delete` wins before the batch is materialized into a SlateDB
/// [`WriteBatch`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScenarioWriteBatch {
    ops: BTreeMap<Vec<u8>, ScenarioWriteEntry>,
}

impl ScenarioWriteBatch {
    /// Creates an empty scenario write batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stages a `put` with default [`PutOptions`].
    ///
    /// If the key is already present in the batch, this replaces the existing
    /// staged operation for that key.
    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(key, value, &PutOptions::default());
    }

    /// Stages a `put` with explicit [`PutOptions`].
    ///
    /// This records the exact TTL metadata that will later be written to
    /// SlateDB and to the DST SQLite state.
    pub fn put_with_options<K, V>(&mut self, key: K, value: V, options: &PutOptions)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref().to_vec();
        let entry = ScenarioWriteEntry::Put {
            key: key.clone(),
            value: value.as_ref().to_vec(),
            options: options.clone(),
        };
        self.ops.insert(key, entry);
    }

    /// Stages a delete for `key`.
    ///
    /// If the key is already present in the batch, the delete replaces the
    /// previously staged operation.
    pub fn delete<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref().to_vec();
        self.ops
            .insert(key.clone(), ScenarioWriteEntry::Delete { key });
    }

    /// Returns `true` if the batch currently contains no staged operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Returns the number of distinct keys currently staged in the batch.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    fn materialized_entries(&self) -> Vec<ScenarioWriteEntry> {
        self.ops.values().cloned().collect()
    }

    fn into_write_batch(self) -> WriteBatch {
        let mut batch = WriteBatch::new();
        for entry in self.ops.into_values() {
            match entry {
                ScenarioWriteEntry::Put {
                    key,
                    value,
                    options,
                } => batch.put_with_options(key, value, &options),
                ScenarioWriteEntry::Delete { key } => batch.delete(key),
            }
        }
        batch
    }
}

struct ScenarioShared {
    db: Db,
    settings: Settings,
    clock: Arc<MockSystemClock>,
    state: Arc<Mutex<SQLiteState>>,
    recorded_committed_tx: watch::Sender<u64>,
}

impl ScenarioShared {
    fn publish_recorded_committed_seq(&self, seq: u64) {
        self.recorded_committed_tx.send_if_modified(|recorded| {
            if seq > *recorded {
                *recorded = seq;
                true
            } else {
                false
            }
        });
    }

    fn recorded_committed_seq(&self) -> u64 {
        *self.recorded_committed_tx.borrow()
    }
}

/// A cloneable handle passed to each running [`Scenario`].
///
/// `ScenarioContext` is the main user-facing API for scenario code. It exposes:
///
/// - checked mutating helpers that apply the operation to SlateDB and record
///   the same transition in SQLite;
/// - [`as_of`](Self::as_of), which opens a point-in-time snapshot of the
///   recorded SQLite state chosen by the caller;
/// - access to the shared [`MockSystemClock`];
/// - a raw [`Db`] reference for unchecked experiments outside the DST contract.
///
/// Any unchecked operation performed through [`ScenarioContext::db`] is not
/// reflected in the recorded SQLite state automatically. If unchecked
/// operations mutate state, later validations may fail by design.
#[derive(Clone)]
pub struct ScenarioContext {
    shared: Rc<ScenarioShared>,
    scenario: &'static str,
    shutdown_token: CancellationToken,
}

impl ScenarioContext {
    /// Returns the shared underlying [`Db`].
    ///
    /// This is an escape hatch for experiments that are outside the checked DST
    /// mutation API surface. Operations performed directly on the returned
    /// database are not recorded in SQLite state.
    pub fn db(&self) -> &Db {
        &self.shared.db
    }

    /// Returns the shared mock clock used by this DST run.
    ///
    /// Checked writes stamp SlateDB operations with timestamps derived from
    /// this clock.
    pub fn clock(&self) -> Arc<MockSystemClock> {
        self.shared.clock.clone()
    }

    /// Returns this context's scenario name.
    pub fn scenario(&self) -> &'static str {
        self.scenario
    }

    /// Returns the run-scoped shutdown token for this scenario.
    ///
    /// Cancelling this token requests shutdown for the entire active
    /// `run_scenarios()` invocation, not just the current scenario.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Returns a point-in-time snapshot of the recorded SQLite state.
    ///
    /// The caller is responsible for choosing the appropriate sequence
    /// frontier, typically from a [`slatedb::DbSnapshot`] or [`slatedb::DbStatus`].
    pub fn as_of(&self, seq: u64) -> StateSnapshot {
        StateSnapshot::new(self.shared.state.clone(), seq)
    }

    /// Waits for DST's recorded SQLite state to include all writes through
    /// `seq`.
    ///
    /// Returns `Ok(false)` if the active run is shutting down before the
    /// recorded state catches up, which allows long-lived reader scenarios to
    /// exit without turning a normal shutdown into a failure.
    pub async fn wait_until_committed(&self, seq: u64) -> Result<bool, Error> {
        if self.shared.recorded_committed_seq() >= seq {
            return Ok(true);
        }

        let mut rx = self.shared.recorded_committed_tx.subscribe();
        loop {
            if *rx.borrow() >= seq {
                return Ok(true);
            }

            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    return Ok(self.shared.recorded_committed_seq() >= seq);
                }
                changed = rx.changed() => {
                    if changed.is_err() {
                        return Err(Error::internal(
                            "DST recorded committed frontier channel closed".to_string(),
                        ));
                    }
                }
            }
        }
    }

    /// Performs a checked single-key put.
    ///
    /// The write is issued against SlateDB with deterministic DST
    /// [`WriteOptions`] and then recorded in SQLite state using the returned
    /// sequence number and timestamp.
    ///
    /// Returns the underlying SlateDB [`WriteHandle`] for callers that need the
    /// assigned sequence number or creation timestamp.
    pub async fn put<K, V>(
        &self,
        key: K,
        value: V,
        put_options: &PutOptions,
    ) -> Result<WriteHandle, Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        let write_options = self.checked_write_options();
        let handle = self
            .shared
            .db
            .put_with_options(&key, &value, put_options, &write_options)
            .await?;
        let create_ts = handle.create_ts();

        let expire_ts = resolve_expire_ts(put_options, self.shared.settings.default_ttl, create_ts);
        let row = RowEntry {
            key: Bytes::from(key),
            value: ValueDeletable::Value(Bytes::from(value)),
            seq: handle.seqnum(),
            create_ts: Some(create_ts),
            expire_ts,
        };
        self.record_write_rows(&[row])?;
        Ok(handle)
    }

    /// Performs a checked delete.
    ///
    /// The delete is written to SlateDB and recorded as a tombstone in the
    /// SQLite state using the returned write metadata.
    pub async fn delete<K>(&self, key: K) -> Result<WriteHandle, Error>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref().to_vec();
        let write_options = self.checked_write_options();
        let handle = self
            .shared
            .db
            .delete_with_options(&key, &write_options)
            .await?;

        let row = RowEntry {
            key: Bytes::from(key),
            value: ValueDeletable::Tombstone,
            seq: handle.seqnum(),
            create_ts: Some(handle.create_ts()),
            expire_ts: None,
        };
        self.record_write_rows(&[row])?;
        Ok(handle)
    }

    /// Performs a checked batched write.
    ///
    /// All staged operations are written atomically through SlateDB and then
    /// recorded atomically in SQLite state under the single sequence number
    /// returned by SlateDB.
    ///
    /// Returns an error if `batch` is empty.
    pub async fn write_batch(&self, batch: ScenarioWriteBatch) -> Result<WriteHandle, Error> {
        if batch.is_empty() {
            return Err(Error::internal(
                "checked write_batch requires at least one entry".to_string(),
            ));
        }

        let entries = batch.materialized_entries();
        let write_options = self.checked_write_options();
        let handle = self
            .shared
            .db
            .write_with_options(batch.into_write_batch(), &write_options)
            .await?;
        let create_ts = handle.create_ts();
        let seq = handle.seqnum();

        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            match entry {
                ScenarioWriteEntry::Put {
                    key,
                    value,
                    options,
                } => rows.push(RowEntry {
                    key: Bytes::from(key),
                    value: ValueDeletable::Value(Bytes::from(value)),
                    seq,
                    create_ts: Some(create_ts),
                    expire_ts: resolve_expire_ts(
                        &options,
                        self.shared.settings.default_ttl,
                        create_ts,
                    ),
                }),
                ScenarioWriteEntry::Delete { key } => rows.push(RowEntry {
                    key: Bytes::from(key),
                    value: ValueDeletable::Tombstone,
                    seq,
                    create_ts: Some(create_ts),
                    expire_ts: None,
                }),
            }
        }

        self.record_write_rows(&rows)?;
        Ok(handle)
    }

    /// Flushes the shared database.
    pub async fn flush(&self) -> Result<(), Error> {
        self.shared.db.flush().await
    }

    /// Advances the shared mock clock by `duration`.
    ///
    /// This affects later checked writes and reads for all scenarios in the
    /// active DST run.
    pub async fn advance_clock(&self, duration: Duration) -> Result<(), Error> {
        self.shared.clock.advance(duration).await;
        Ok(())
    }

    fn checked_write_options(&self) -> WriteOptions {
        WriteOptions {
            await_durable: false,
            now: self.shared.clock.now().timestamp_millis(),
            ..Default::default()
        }
    }

    pub(crate) fn record_write_rows(&self, rows: &[RowEntry]) -> Result<(), Error> {
        self.shared.state.lock().record_write(rows, self.scenario)?;
        if let Some(max_seq) = rows.iter().map(|row| row.seq).max() {
            self.shared.publish_recorded_committed_seq(max_seq);
        }
        Ok(())
    }
}

/// A scenario runner for deterministic simulation testing.
///
/// `Dst` owns the shared resources for one simulation run:
///
/// - the SlateDB [`Db`] under test;
/// - the shared [`MockSystemClock`];
/// - the SQLite recorded state used for point-in-time validation and post-run
///   inspection.
///
/// Scenarios are run as Tokio local tasks, so callers should execute
/// [`Dst::run_scenarios`] on a current-thread runtime or within a `LocalSet`.
pub struct Dst {
    shared: Rc<ScenarioShared>,
}

impl Dst {
    /// Creates a DST runner backed by an in-memory SQLite state database.
    ///
    /// The supplied `db`, `clock`, and `settings` must all describe the same
    /// SlateDB instance under test.
    pub fn new(db: Db, clock: Arc<MockSystemClock>, settings: Settings) -> Self {
        Self::new_with_state_path(db, clock, settings, None)
    }

    /// Creates a DST runner with an optional on-disk SQLite state database.
    ///
    /// When `state_path` is `None`, the state lives in memory. Supplying a
    /// path is useful when you want to inspect the SQLite database after a
    /// failed run.
    pub fn new_with_state_path(
        db: Db,
        clock: Arc<MockSystemClock>,
        settings: Settings,
        state_path: Option<&'static str>,
    ) -> Self {
        let state = Arc::new(Mutex::new(SQLiteState::new(state_path)));
        let (recorded_committed_tx, _) = watch::channel(0);
        let shared = ScenarioShared {
            db,
            settings,
            clock,
            state,
            recorded_committed_tx,
        };
        Self {
            shared: Rc::new(shared),
        }
    }

    /// Creates a standalone [`ScenarioContext`] with the given scenario name.
    ///
    /// This is mainly useful for ad hoc follow-up verification after a scenario
    /// run, such as a final snapshot-based validation or flush.
    pub fn context(&self, scenario: &'static str) -> ScenarioContext {
        self.context_with_shutdown_token(scenario, CancellationToken::new())
    }

    fn context_with_shutdown_token(
        &self,
        scenario: &'static str,
        shutdown_token: CancellationToken,
    ) -> ScenarioContext {
        ScenarioContext {
            shared: self.shared.clone(),
            scenario,
            shutdown_token,
        }
    }

    /// Returns the shared mock clock for this DST runner.
    pub fn clock(&self) -> Arc<MockSystemClock> {
        self.shared.clock.clone()
    }

    /// Returns a point-in-time snapshot of the recorded SQLite state.
    pub fn as_of(&self, seq: u64) -> StateSnapshot {
        StateSnapshot::new(self.shared.state.clone(), seq)
    }

    /// Runs the provided scenarios concurrently to completion.
    ///
    /// Finite scenarios are allowed to finish normally. Once all finite
    /// scenarios complete, the shared shutdown token is cancelled so any
    /// `runs_forever()` scenarios can exit. If any scenario returns an error or
    /// fails to join, the shutdown token is cancelled immediately and the first
    /// error is returned after all tasks finish unwinding.
    pub async fn run_scenarios<I>(&self, scenarios: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = Box<dyn Scenario>>,
    {
        let scenarios: Vec<_> = scenarios.into_iter().collect();
        let mut remaining_finite = scenarios
            .iter()
            .filter(|scenario| !scenario.runs_forever())
            .count();

        let shutdown_token = CancellationToken::new();
        let mut handles = FuturesUnordered::new();
        for scenario in scenarios {
            let name = scenario.name();
            let runs_forever = scenario.runs_forever();
            let ctx = self.context_with_shutdown_token(name, shutdown_token.clone());
            handles.push(
                tokio::task::spawn_local(async move { scenario.run(ctx).await })
                    .map(move |result| (name, runs_forever, result)),
            );
        }

        let mut first_error: Option<Error> = None;
        while let Some((name, runs_forever, result)) = handles.next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    shutdown_token.cancel();
                }
                Err(join_err) => {
                    if first_error.is_none() {
                        first_error = Some(Error::internal(format!(
                            "scenario '{}' failed to join: {}",
                            name, join_err
                        )));
                    }
                    shutdown_token.cancel();
                }
            }

            if !runs_forever {
                remaining_finite -= 1;
                if remaining_finite == 0 {
                    shutdown_token.cancel();
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    /// Closes the underlying SlateDB instance.
    ///
    /// This is typically called after a simulation completes and any follow-up
    /// SQLite state inspection has already been performed.
    pub async fn close(&self) -> Result<(), Error> {
        self.shared.db.close().await
    }

    /// Verifies the final SlateDB state against the recorded SQLite model.
    ///
    /// This checks both the committed memory-visible view and the remote-durable
    /// view using the same full-range scan validation logic exercised by
    /// [`crate::scenarios::ScanScenario`].
    pub async fn verify_final_state(&self) -> Result<(), Error> {
        let verifier = self.context("verifier");
        ScanScenario::validate_full_range(&verifier, &ScanOptions::default()).await?;
        ScanScenario::validate_full_range(
            &verifier,
            &ScanOptions::default().with_durability_filter(DurabilityLevel::Remote),
        )
        .await?;
        Ok(())
    }
}

fn resolve_expire_ts(
    options: &PutOptions,
    default_ttl: Option<u64>,
    create_ts: i64,
) -> Option<i64> {
    match options.ttl {
        Ttl::Default => default_ttl.and_then(|ttl| checked_expire_ts(create_ts, ttl)),
        Ttl::NoExpiry => None,
        Ttl::ExpireAfter(ttl) => checked_expire_ts(create_ts, ttl),
        Ttl::ExpireAt(ts) => Some(ts),
        _ => None,
    }
}

fn checked_expire_ts(now: i64, ttl: u64) -> Option<i64> {
    if ttl > i64::MAX as u64 {
        return None;
    }
    let expire_ts = now + ttl as i64;
    if expire_ts < now {
        return None;
    }
    Some(expire_ts)
}
