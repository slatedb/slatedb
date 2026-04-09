//! Scenario-driven deterministic simulation testing for SlateDB.

use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use parking_lot::Mutex;
use slatedb::config::{PutOptions, ReadOptions, ScanOptions, Settings, Ttl, WriteOptions};
use slatedb::{Db, DbStatus, Error, KeyValue, WriteBatch, WriteHandle};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;

use crate::state::{
    ExpectedKeyValue, OracleReadContext, OracleSnapshot, OracleVersionKind, PendingVersion,
    SQLiteOracle,
};

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
    /// The name is recorded in oracle rows and included in mismatch errors, so
    /// it should identify the workload instance clearly enough to debug failures.
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
    /// The provided [`ScenarioContext`] exposes checked read/write helpers,
    /// access to the shared mock clock, and a run-scoped shutdown token. Return
    /// an [`Error`] to fail the entire `run_scenarios()` invocation.
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
    /// SlateDB and to the DST oracle.
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
    oracle: Mutex<SQLiteOracle>,
    status_rx_template: watch::Receiver<DbStatus>,
    observation_gate: RwLock<()>,
}

/// A cloneable handle passed to each running [`Scenario`].
///
/// `ScenarioContext` is the main user-facing API for scenario code. It exposes:
///
/// - checked write helpers that apply the operation to SlateDB and record the
///   same transition in the SQLite oracle;
/// - checked read helpers that compare SlateDB results against oracle-derived
///   expectations at a deterministic barrier;
/// - access to the shared [`MockSystemClock`];
/// - a raw [`Db`] reference for unchecked experiments outside the DST contract.
///
/// Any unchecked operation performed through [`ScenarioContext::db`] is not
/// reflected in the DST oracle automatically. If unchecked operations mutate
/// state, later checked reads may fail by design.
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
    /// API surface. Operations performed directly on the returned database are
    /// not recorded in the oracle.
    pub fn db(&self) -> &Db {
        &self.shared.db
    }

    /// Returns the shared mock clock used by this DST run.
    ///
    /// Checked writes stamp SlateDB operations with timestamps derived from
    /// this clock, and checked reads use the same logical time when querying
    /// the oracle.
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

    /// Performs a checked single-key put.
    ///
    /// The write is issued against SlateDB with deterministic DST
    /// [`WriteOptions`] and then recorded in the oracle using the returned
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
        let _guard = self.shared.observation_gate.read().await;
        let write_options = self.checked_write_options();
        let handle = self
            .shared
            .db
            .put_with_options(&key, &value, put_options, &write_options)
            .await?;
        let create_ts = handle.create_ts();

        let expire_ts = resolve_expire_ts(put_options, self.shared.settings.default_ttl, create_ts);
        let version = PendingVersion {
            seq: handle.seqnum(),
            key: key.clone(),
            kind: OracleVersionKind::Value,
            value: Some(value.clone()),
            create_ts,
            expire_ts,
        };
        self.shared
            .oracle
            .lock()
            .record_write(&[version], handle.seqnum(), self.scenario)?;
        Ok(handle)
    }

    /// Performs a checked delete.
    ///
    /// The delete is written to SlateDB and recorded as a tombstone in the
    /// oracle using the returned write metadata.
    pub async fn delete<K>(&self, key: K) -> Result<WriteHandle, Error>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref().to_vec();
        let _guard = self.shared.observation_gate.read().await;
        let write_options = self.checked_write_options();
        let handle = self
            .shared
            .db
            .delete_with_options(&key, &write_options)
            .await?;

        let version = PendingVersion {
            seq: handle.seqnum(),
            key: key.clone(),
            kind: OracleVersionKind::Tombstone,
            value: None,
            create_ts: handle.create_ts(),
            expire_ts: None,
        };
        self.shared
            .oracle
            .lock()
            .record_write(&[version], handle.seqnum(), self.scenario)?;
        Ok(handle)
    }

    /// Performs a checked batched write.
    ///
    /// All staged operations are written atomically through SlateDB and then
    /// recorded atomically in the oracle under the single sequence number
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
        let _guard = self.shared.observation_gate.read().await;
        let write_options = self.checked_write_options();
        let handle = self
            .shared
            .db
            .write_with_options(batch.into_write_batch(), &write_options)
            .await?;
        let create_ts = handle.create_ts();

        let mut versions = Vec::with_capacity(entries.len());
        for entry in entries {
            match entry {
                ScenarioWriteEntry::Put {
                    key,
                    value,
                    options,
                } => versions.push(PendingVersion {
                    seq: handle.seqnum(),
                    key,
                    kind: OracleVersionKind::Value,
                    value: Some(value),
                    create_ts,
                    expire_ts: resolve_expire_ts(
                        &options,
                        self.shared.settings.default_ttl,
                        create_ts,
                    ),
                }),
                ScenarioWriteEntry::Delete { key } => versions.push(PendingVersion {
                    seq: handle.seqnum(),
                    key,
                    kind: OracleVersionKind::Tombstone,
                    value: None,
                    create_ts,
                    expire_ts: None,
                }),
            }
        }

        self.shared
            .oracle
            .lock()
            .record_write(&versions, handle.seqnum(), self.scenario)?;
        Ok(handle)
    }

    /// Flushes the shared database and advances the oracle's durable watermark.
    ///
    /// Durability is taken from the shared [`Db::subscribe`] status stream so
    /// remote-visibility checks are based on the database's actual durable
    /// sequence number rather than on an inferred watermark.
    pub async fn flush(&self) -> Result<(), Error> {
        let _guard = self.shared.observation_gate.read().await;
        let status_rx = self.shared.status_rx_template.clone();
        self.shared.db.flush().await?;
        let durable_seq = status_rx.borrow().durable_seq;
        self.shared.oracle.lock().record_flush(durable_seq)?;
        Ok(())
    }

    /// Advances the shared mock clock by `duration`.
    ///
    /// This affects later checked writes and reads for all scenarios in the
    /// active DST run.
    pub async fn advance_clock(&self, duration: Duration) -> Result<(), Error> {
        let _guard = self.shared.observation_gate.read().await;
        self.shared.clock.advance(duration).await;
        Ok(())
    }

    /// Performs a checked point read.
    ///
    /// This method runs the read against SlateDB, queries the oracle for the
    /// expected visible value at the same logical time and durability level,
    /// and returns an error if they differ.
    ///
    /// Checked reads currently reject `dirty=true`.
    pub async fn checked_get<K>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, Error>
    where
        K: AsRef<[u8]> + Send,
    {
        ensure_supported_checked_read(options.dirty)?;

        let key = key.as_ref().to_vec();
        let _guard = self.shared.observation_gate.write().await;
        let status_rx = self.shared.status_rx_template.clone();
        let durable_seq = status_rx.borrow().durable_seq;
        let now = self.shared.clock.now().timestamp_millis();
        let actual = self
            .shared
            .db
            .get_key_value_with_options(&key, options)
            .await?;
        let watermarks = self.shared.oracle.lock().watermarks()?;
        let expected = self.shared.oracle.lock().expected_get(
            &key,
            OracleReadContext {
                committed_seq: watermarks.committed_seq,
                durable_seq,
                now,
                durability_filter: options.durability_filter,
            },
        )?;

        if !key_value_matches(actual.as_ref(), expected.as_ref()) {
            return Err(mismatch_error(
                "checked_get",
                format!(
                    "scenario={} key={:?} durable_seq={} now={} actual={:?} expected={:?}",
                    self.scenario, key, durable_seq, now, actual, expected
                ),
            ));
        }
        Ok(actual)
    }

    /// Performs a checked range scan and returns the fully materialized result.
    ///
    /// The scan is executed against SlateDB, drained into memory, and compared
    /// against oracle expectations using the same durability filter and
    /// [`ScanOptions::order`]. Descending scans are validated explicitly.
    ///
    /// Checked reads currently reject `dirty=true`.
    pub async fn checked_scan<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<Vec<KeyValue>, Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send + Clone,
    {
        ensure_supported_checked_read(options.dirty)?;

        let (start, end) = owned_bounds(&range);
        let _guard = self.shared.observation_gate.write().await;
        let status_rx = self.shared.status_rx_template.clone();
        let durable_seq = status_rx.borrow().durable_seq;
        let now = self.shared.clock.now().timestamp_millis();
        let mut actual_iter = self.shared.db.scan_with_options(range, options).await?;
        let mut actual = Vec::new();
        while let Some(kv) = actual_iter.next().await? {
            actual.push(kv);
        }

        let watermarks = self.shared.oracle.lock().watermarks()?;
        let expected = self.shared.oracle.lock().expected_scan(
            start.clone(),
            end.clone(),
            OracleReadContext {
                committed_seq: watermarks.committed_seq,
                durable_seq,
                now,
                durability_filter: options.durability_filter,
            },
            options.order,
        )?;

        if !key_value_vec_matches(&actual, &expected) {
            return Err(mismatch_error(
                "checked_scan",
                format!(
                    "scenario={} start={:?} end={:?} order={:?} durable_seq={} now={} actual={:?} expected={:?}",
                    self.scenario, start, end, options.order, durable_seq, now, actual, expected
                ),
            ));
        }
        Ok(actual)
    }

    fn checked_write_options(&self) -> WriteOptions {
        WriteOptions {
            await_durable: false,
            now: self.shared.clock.now().timestamp_millis(),
            ..Default::default()
        }
    }
}

/// A scenario runner for deterministic simulation testing.
///
/// `Dst` owns the shared resources for one simulation run:
///
/// - the SlateDB [`Db`] under test;
/// - the shared [`MockSystemClock`];
/// - the SQLite oracle used for checked reads and post-run inspection;
/// - a template [`DbStatus`] subscription used to observe durability changes.
///
/// Scenarios are run as Tokio local tasks, so callers should execute
/// [`Dst::run_scenarios`] on a current-thread runtime or within a `LocalSet`.
pub struct Dst {
    shared: Rc<ScenarioShared>,
}

impl Dst {
    /// Creates a DST runner backed by an in-memory SQLite oracle.
    ///
    /// The supplied `db`, `clock`, and `settings` must all describe the same
    /// SlateDB instance under test.
    pub fn new(db: Db, clock: Arc<MockSystemClock>, settings: Settings) -> Result<Self, Error> {
        Self::new_with_oracle_path(db, clock, settings, None)
    }

    /// Creates a DST runner with an optional on-disk SQLite oracle.
    ///
    /// When `oracle_path` is `None`, the oracle lives in memory. Supplying a
    /// path is useful when you want to inspect the oracle database after a
    /// failed run.
    pub fn new_with_oracle_path(
        db: Db,
        clock: Arc<MockSystemClock>,
        settings: Settings,
        oracle_path: Option<&'static str>,
    ) -> Result<Self, Error> {
        let oracle = SQLiteOracle::new(oracle_path)?;
        let shared = ScenarioShared {
            status_rx_template: db.subscribe(),
            db,
            settings,
            clock,
            oracle: Mutex::new(oracle),
            observation_gate: RwLock::new(()),
        };
        Ok(Self {
            shared: Rc::new(shared),
        })
    }

    /// Creates a standalone [`ScenarioContext`] with the given scenario name.
    ///
    /// This is mainly useful for ad hoc follow-up verification after a scenario
    /// run, such as a final checked read or flush.
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

    /// Returns a snapshot of the oracle's persisted state.
    ///
    /// This includes every recorded version row and the final committed and
    /// durable sequence watermarks.
    pub fn oracle_snapshot(&self) -> Result<OracleSnapshot, Error> {
        self.shared.oracle.lock().snapshot()
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
    /// oracle inspection has already been performed.
    pub async fn close(&self) -> Result<(), Error> {
        self.shared.db.close().await
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

fn ensure_supported_checked_read(dirty: bool) -> Result<(), Error> {
    if dirty {
        return Err(Error::internal(
            "checked DST reads do not support dirty=true in v1".to_string(),
        ));
    }
    Ok(())
}

fn mismatch_error(op: &str, detail: String) -> Error {
    Error::internal(format!("{} mismatch: {}", op, detail))
}

fn owned_bounds<K, T>(range: &T) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
where
    K: AsRef<[u8]>,
    T: RangeBounds<K>,
{
    let start = match range.start_bound() {
        Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match range.end_bound() {
        Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
        Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start, end)
}

fn key_value_matches(actual: Option<&KeyValue>, expected: Option<&ExpectedKeyValue>) -> bool {
    match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => {
            actual.key.as_ref() == expected.key.as_slice()
                && actual.value.as_ref() == expected.value.as_slice()
                && actual.seq == expected.seq
                && actual.create_ts == expected.create_ts
                && actual.expire_ts == expected.expire_ts
        }
        _ => false,
    }
}

fn key_value_vec_matches(actual: &[KeyValue], expected: &[ExpectedKeyValue]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(actual, expected)| key_value_matches(Some(actual), Some(expected)))
}
