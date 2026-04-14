//! Scenario implementations for the DST simulation test harness.
//!
//! The simulation is built by composing several independently scheduled
//! workloads against a shared [`ScenarioContext`]. Together they create a
//! randomized but reproducible mix of mutations, reads, clock movement, and
//! shutdown conditions that stress the database and the DST recorded-state
//! model at the same time.
//!
//! Each scenario uses [`DbRand`] for deterministic pseudo-random choices, so a
//! failing run can be reproduced from the same seed. Some scenarios accept an
//! iteration limit and stop on their own, while others run until the shared
//! shutdown token is cancelled by another scenario.

use std::rc::Rc;
use std::time::Duration;

use crate::{Scenario, ScenarioContext, ScenarioWriteBatch};
use async_trait::async_trait;
use rand::Rng;
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, ScanOptions};
use slatedb::{DbRand, DbSnapshot, Error, IterationOrder, KeyValue};
use tracing::info;

/// Issues checked single-key puts against a small key space.
///
/// `PutScenario` creates overwrite-heavy write traffic without mixing in other
/// operation types. Running it as a dedicated scenario makes put interleavings
/// explicit in the simulation schedule instead of hiding them behind an
/// internal random branch.
pub struct PutScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub key_space: u64,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for PutScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn runs_forever(&self) -> bool {
        self.iterations.is_none()
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let rand = &self.rand;
        let shutdown_token = ctx.shutdown_token();
        let mut iteration = 0u32;

        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            if let Some(total_iterations) = self.iterations {
                if iteration >= total_iterations {
                    break;
                }
            }

            iteration += 1;
            if iteration % 100 == 0 {
                info!(iteration, total_iterations = ?self.iterations, "scenario iteration");
            }

            let key_suffix = rand.rng().random::<u64>() % self.key_space;
            let value_suffix = rand.rng().random::<u64>();
            let key = format!("key-{key_suffix}").into_bytes();
            let value = format!("put-{value_suffix}").into_bytes();
            ctx.put(&key, &value, &PutOptions::default()).await?;

            let should_yield = rand.rng().random::<bool>();
            if should_yield {
                tokio::task::yield_now().await;
            }
        }

        Ok(())
    }
}

/// Issues checked deletes against a small key space.
///
/// `DeleteScenario` exists separately from puts and batch writes so delete
/// traffic can be scheduled independently by the harness.
pub struct DeleteScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub key_space: u64,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for DeleteScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn runs_forever(&self) -> bool {
        self.iterations.is_none()
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let rand = &self.rand;
        let shutdown_token = ctx.shutdown_token();
        let mut iteration = 0u32;

        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            if let Some(total_iterations) = self.iterations {
                if iteration >= total_iterations {
                    break;
                }
            }

            iteration += 1;
            if iteration % 100 == 0 {
                info!(iteration, total_iterations = ?self.iterations, "scenario iteration");
            }

            let key_suffix = rand.rng().random::<u64>() % self.key_space;
            let key = format!("key-{key_suffix}").into_bytes();
            ctx.delete(&key).await?;

            let should_yield = rand.rng().random::<bool>();
            if should_yield {
                tokio::task::yield_now().await;
            }
        }

        Ok(())
    }
}

/// Issues checked batched writes against a small key space.
///
/// `BatchWriteScenario` keeps the multi-key atomic write path hot without
/// entangling it with single-key mutations.
pub struct BatchWriteScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub key_space: u64,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for BatchWriteScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn runs_forever(&self) -> bool {
        self.iterations.is_none()
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let rand = &self.rand;
        let shutdown_token = ctx.shutdown_token();
        let mut iteration = 0u32;

        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            if let Some(total_iterations) = self.iterations {
                if iteration >= total_iterations {
                    break;
                }
            }

            iteration += 1;
            if iteration % 100 == 0 {
                info!(iteration, total_iterations = ?self.iterations, "scenario iteration");
            }

            let mut batch = ScenarioWriteBatch::new();
            let key_suffix = rand.rng().random::<u64>() % self.key_space;
            let value_suffix = rand.rng().random::<u64>();
            let key = format!("key-{key_suffix}").into_bytes();
            let value = format!("batch-{value_suffix}").into_bytes();
            batch.put(&key, &value);

            let second_key_suffix = rand.rng().random::<u64>() % self.key_space;
            let second_key = format!("key-{second_key_suffix}").into_bytes();
            let should_put_second = rand.rng().random::<bool>();
            if should_put_second {
                let second_value_suffix = rand.rng().random::<u64>();
                let second_value = format!("batch-{second_value_suffix}").into_bytes();
                batch.put(&second_key, &second_value);
            } else {
                batch.delete(&second_key);
            }

            ctx.write_batch(batch).await?;

            let should_yield = rand.rng().random::<bool>();
            if should_yield {
                tokio::task::yield_now().await;
            }
        }

        Ok(())
    }
}

/// Issues checked point reads against both memory-visible and remotely durable state.
///
/// `GetScenario` continuously validates single-key reads against the DST
/// recorded SQLite state while the other scenarios are mutating SlateDB.
pub struct GetScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub key_space: u64,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for GetScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn runs_forever(&self) -> bool {
        self.iterations.is_none()
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let rand = &self.rand;
        let shutdown_token = ctx.shutdown_token();
        let mut iteration = 0u32;

        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            if let Some(total_iterations) = self.iterations {
                if iteration >= total_iterations {
                    break;
                }
            }

            iteration += 1;
            if iteration % 100 == 0 {
                info!(iteration, total_iterations = ?self.iterations, "scenario iteration");
            }

            let durability_filter = if rand.rng().random::<bool>() {
                DurabilityLevel::Remote
            } else {
                DurabilityLevel::Memory
            };
            let key_suffix = rand.rng().random::<u64>() % self.key_space;
            let key = format!("key-{key_suffix}").into_bytes();
            let options = ReadOptions::default().with_durability_filter(durability_filter);
            let snapshot = ctx.db().snapshot().await?;
            let snapshot_seq = snapshot.seq();
            match options.durability_filter {
                DurabilityLevel::Remote => {
                    Self::validate_remote_read(&ctx, &snapshot, snapshot_seq, &key, &options)
                        .await?;
                }
                _ => {
                    Self::validate_committed_read(&ctx, &snapshot, snapshot_seq, &key, &options)
                        .await?;
                }
            }

            tokio::task::yield_now().await;
        }

        Ok(())
    }
}

impl GetScenario {
    async fn validate_committed_read(
        ctx: &ScenarioContext,
        snapshot: &DbSnapshot,
        snapshot_seq: u64,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<(), Error> {
        let actual = snapshot.get_key_value_with_options(&key, options).await?;
        if !ctx.wait_until_committed(snapshot_seq).await? {
            return Ok(());
        }

        let expected = ctx
            .as_of(snapshot_seq)
            .get_key_value_with_options(&key, options)?;

        assert_eq!(
            actual,
            expected,
            "validate_get mismatch: scenario={} key={:?} options={:?} snapshot_seq={}",
            ctx.scenario(),
            key,
            options,
            snapshot_seq
        );

        Ok(())
    }

    async fn validate_remote_read(
        ctx: &ScenarioContext,
        snapshot: &DbSnapshot,
        snapshot_seq: u64,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<(), Error> {
        loop {
            let frontier_before = snapshot_seq.min(ctx.db().status().durable_seq);
            let actual = snapshot.get_key_value_with_options(key, options).await?;
            let frontier_after = snapshot_seq.min(ctx.db().status().durable_seq);
            if frontier_before != frontier_after {
                tokio::task::yield_now().await;
                continue;
            }

            if !ctx.wait_until_committed(frontier_after).await? {
                return Ok(());
            }

            let expected = ctx
                .as_of(frontier_after)
                .get_key_value_with_options(key, options)?;
            assert_eq!(
                actual,
                expected,
                "validate_get mismatch: scenario={} key={:?} options={:?} snapshot_seq={} frontier={}",
                ctx.scenario(),
                key,
                options,
                snapshot_seq,
                frontier_after
            );
            return Ok(());
        }
    }
}

/// Issues checked full-range scans against both memory-visible and remotely durable state.
///
/// `ScanScenario` continuously validates scan behavior against the DST
/// recorded SQLite state while the other scenarios are mutating SlateDB.
pub struct ScanScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for ScanScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn runs_forever(&self) -> bool {
        self.iterations.is_none()
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let rand = &self.rand;
        let shutdown_token = ctx.shutdown_token();
        let mut iteration = 0u32;

        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            if let Some(total_iterations) = self.iterations {
                if iteration >= total_iterations {
                    break;
                }
            }

            iteration += 1;
            if iteration % 100 == 0 {
                info!(iteration, total_iterations = ?self.iterations, "scenario iteration");
            }

            let durability_filter = if rand.rng().random::<bool>() {
                DurabilityLevel::Remote
            } else {
                DurabilityLevel::Memory
            };
            let order = if rand.rng().random::<bool>() {
                IterationOrder::Ascending
            } else {
                IterationOrder::Descending
            };
            let options = ScanOptions::default()
                .with_durability_filter(durability_filter)
                .with_order(order);
            Self::validate_full_range(&ctx, &options).await?;

            tokio::task::yield_now().await;
        }

        Ok(())
    }
}

impl ScanScenario {
    pub(crate) async fn validate_full_range(
        ctx: &ScenarioContext,
        options: &ScanOptions,
    ) -> Result<(), Error> {
        let snapshot = ctx.db().snapshot().await?;
        let snapshot_seq = snapshot.seq();

        match options.durability_filter {
            DurabilityLevel::Remote => {
                return Self::validate_remote_read(ctx, &snapshot, snapshot_seq, options).await;
            }
            _ => {}
        }

        Self::validate_committed_read(ctx, &snapshot, snapshot_seq, options).await
    }

    async fn validate_committed_read(
        ctx: &ScenarioContext,
        snapshot: &DbSnapshot,
        snapshot_seq: u64,
        options: &ScanOptions,
    ) -> Result<(), Error> {
        let actual = Self::collect_full_range_scan(snapshot, options).await?;
        if !ctx.wait_until_committed(snapshot_seq).await? {
            return Ok(());
        }

        let expected = ctx
            .as_of(snapshot_seq)
            .scan_with_options::<Vec<u8>, _>(.., options)?;

        assert_eq!(
            actual,
            expected,
            "validate_scan mismatch: scenario={} range=.. options={:?} snapshot_seq={}",
            ctx.scenario(),
            options,
            snapshot_seq
        );

        Ok(())
    }

    async fn validate_remote_read(
        ctx: &ScenarioContext,
        snapshot: &DbSnapshot,
        snapshot_seq: u64,
        options: &ScanOptions,
    ) -> Result<(), Error> {
        loop {
            let frontier_before = snapshot_seq.min(ctx.db().status().durable_seq);
            let actual = Self::collect_full_range_scan(snapshot, options).await?;
            let frontier_after = snapshot_seq.min(ctx.db().status().durable_seq);
            if frontier_before != frontier_after {
                tokio::task::yield_now().await;
                continue;
            }

            if !ctx.wait_until_committed(frontier_after).await? {
                return Ok(());
            }

            let expected = ctx
                .as_of(frontier_after)
                .scan_with_options::<Vec<u8>, _>(.., options)?;
            assert_eq!(
                actual,
                expected,
                "validate_scan mismatch: scenario={} range=.. options={:?} snapshot_seq={} frontier={}",
                ctx.scenario(),
                options,
                snapshot_seq,
                frontier_after
            );
            return Ok(());
        }
    }

    async fn collect_full_range_scan(
        snapshot: &DbSnapshot,
        options: &ScanOptions,
    ) -> Result<Vec<KeyValue>, Error> {
        let mut actual_iter = snapshot
            .scan_with_options::<Vec<u8>, _>(.., options)
            .await?;
        let mut actual = Vec::new();
        while let Some(kv) = actual_iter.next().await? {
            actual.push(kv);
        }
        Ok(actual)
    }
}

/// Advances the mock system clock during the simulation.
///
/// `ClockScenario` drives time-dependent behavior in the simulation. It advances
/// the mocked clock by small random increments so timestamps continue moving
/// forward while reads and writes are in flight.
///
/// This keeps timestamp-bearing writes and read metadata evolving
/// deterministically without waiting on real wall-clock time. The scenario runs
/// until the shared shutdown token is cancelled.
pub struct ClockScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
}

#[async_trait(?Send)]
impl Scenario for ClockScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn runs_forever(&self) -> bool {
        true
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let rand = &self.rand;
        let shutdown_token = ctx.shutdown_token();
        let mut iterations = 0;

        while !shutdown_token.is_cancelled() {
            iterations += 1;
            if iterations % 100 == 0 {
                info!(iteration = iterations, "scenario iteration");
            }
            let advance_ms = 1 + (rand.rng().random::<u64>() % 100);
            let should_yield = rand.rng().random::<bool>();
            ctx.advance_clock(Duration::from_millis(advance_ms)).await?;
            if should_yield {
                tokio::task::yield_now().await;
            }
        }

        Ok(())
    }
}

/// Cancels the simulation after a fixed wall-clock duration.
///
/// `TimedShutdownScenario` provides a real-time escape hatch for simulations that
/// contain open-ended scenarios. It waits for either the shared shutdown token
/// to be cancelled by some other task or for `duration` to elapse. If the time
/// limit wins, it logs the timeout and cancels the shared token to stop the
/// rest of the simulation.
pub struct TimedShutdownScenario {
    pub name: &'static str,
    pub duration: Duration,
}

#[async_trait(?Send)]
impl Scenario for TimedShutdownScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let shutdown_token = ctx.shutdown_token();

        tokio::select! {
            _ = shutdown_token.cancelled() => {}
            _ = tokio::time::sleep(self.duration) => {
                info!(duration = ?self.duration, "wall-clock limit reached");
                shutdown_token.cancel();
            }
        }

        Ok(())
    }
}

/// Performs extra flushes independently of the mutation scenarios.
///
/// `FlusherScenario` adds durability pressure that is decoupled from the normal
/// write path. On each iteration it randomly decides whether to flush, which
/// increases the variety of interleavings between acknowledged writes, remote
/// durability, and concurrent reads.
///
/// Running flushes in a separate scenario makes the simulation explore states
/// that would be less common if flushing only happened as a side effect of
/// mutation traffic. Like the other scenarios, it can run for a fixed number
/// of iterations or continue until shutdown.
pub struct FlusherScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for FlusherScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn runs_forever(&self) -> bool {
        self.iterations.is_none()
    }

    #[tracing::instrument(skip_all, fields(scenario = self.name()))]
    async fn run(&self, ctx: ScenarioContext) -> Result<(), Error> {
        let rand = &self.rand;
        let shutdown_token = ctx.shutdown_token();
        let mut iteration = 0u32;

        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            if let Some(total_iterations) = self.iterations {
                if iteration >= total_iterations {
                    break;
                }
            }

            iteration += 1;
            if iteration % 100 == 0 {
                info!(iteration, total_iterations = ?self.iterations, "scenario iteration");
            }

            let should_flush = rand.rng().random::<bool>();
            if should_flush {
                ctx.flush().await?;
            }
            tokio::task::yield_now().await;
        }

        Ok(())
    }
}
