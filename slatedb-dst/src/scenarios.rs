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

use crate::utils::{validate_get, validate_scan};
use crate::{Scenario, ScenarioContext, ScenarioWriteBatch};
use async_trait::async_trait;
use rand::Rng;
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, ScanOptions};
use slatedb::{DbRand, Error, IterationOrder};
use tracing::info;

/// Issues the mutating side of the simulation workload.
///
/// `WriterScenario` is responsible for creating churn in the database state. On
/// each iteration it randomly chooses between two plain put variants, deletes,
/// batched writes, and explicit flushes over a deliberately small key space.
/// That combination creates frequent overwrites and visibility changes, which
/// makes it more likely that the simulation will exercise edge cases in state
/// tracking and durability transitions without depending on TTL semantics the
/// current SQLite model does not fully model.
///
/// When `iterations` is `Some`, the scenario performs exactly that many write
/// steps unless shutdown happens first. When it is `None`, the scenario keeps
/// generating mutations until another scenario cancels the run.
pub struct WriterScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub key_space: u64,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for WriterScenario {
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

            let op = rand.rng().random::<u64>() % 5;
            match op {
                0 => {
                    let key_suffix = rand.rng().random::<u64>() % self.key_space;
                    let value_suffix = rand.rng().random::<u64>();
                    let key = format!("key-{key_suffix}").into_bytes();
                    let value = format!("put-{value_suffix}").into_bytes();
                    ctx.put(&key, &value, &PutOptions::default()).await?;
                }
                1 => {
                    let key_suffix = rand.rng().random::<u64>() % self.key_space;
                    let value_suffix = rand.rng().random::<u64>();
                    let key = format!("key-{key_suffix}").into_bytes();
                    let value = format!("put-alt-{value_suffix}").into_bytes();
                    ctx.put(&key, &value, &PutOptions::default()).await?;
                }
                2 => {
                    let key_suffix = rand.rng().random::<u64>() % self.key_space;
                    let key = format!("key-{key_suffix}").into_bytes();
                    ctx.delete(&key).await?;
                }
                3 => {
                    let mut batch = ScenarioWriteBatch::new();
                    let key_suffix = rand.rng().random::<u64>() % self.key_space;
                    let value_suffix = rand.rng().random::<u64>();
                    let key = format!("key-{key_suffix}").into_bytes();
                    let value = format!("batch-{value_suffix}").into_bytes();
                    batch.put(&key, &value);

                    let second_key_suffix = rand.rng().random::<u64>() % self.key_space;
                    let second_key = format!("key-{second_key_suffix}").into_bytes();
                    let should_put_second = rand.rng().random::<u64>() & 1 == 0;
                    if should_put_second {
                        let second_value_suffix = rand.rng().random::<u64>();
                        let second_value = format!("batch-{second_value_suffix}").into_bytes();
                        batch.put(&second_key, &second_value);
                    } else {
                        batch.delete(&second_key);
                    }

                    ctx.write_batch(batch).await?;
                }
                _ => {
                    ctx.flush().await?;
                }
            }

            let should_yield = rand.rng().random::<u64>() & 1 == 0;
            if should_yield {
                tokio::task::yield_now().await;
            }
        }

        Ok(())
    }
}

/// Issues checked reads against both memory-visible and remotely durable state.
///
/// `ReaderScenario` continuously validates observable behavior against the DST
/// recorded SQLite state while the other scenarios are mutating SlateDB. It
/// randomly alternates between point reads and full-range scans, and for each
/// operation it may read either the memory-visible view or the remotely durable
/// view of the data.
///
/// By running concurrently with writers, clock advancement, and background
/// flushes, this scenario helps catch mismatches in read visibility, scan
/// ordering, and durability filtering under interleaved load.
pub struct ReaderScenario {
    pub name: &'static str,
    pub rand: Rc<DbRand>,
    pub key_space: u64,
    pub iterations: Option<u32>,
}

#[async_trait(?Send)]
impl Scenario for ReaderScenario {
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

            let do_get = rand.rng().random::<u64>() & 1 == 0;
            if do_get {
                let key_suffix = rand.rng().random::<u64>() % self.key_space;
                let key = format!("key-{key_suffix}").into_bytes();
                let read_memory = rand.rng().random::<u64>() & 1 == 0;
                if read_memory {
                    let _ = validate_get(&ctx, &key, &ReadOptions::default()).await?;
                } else {
                    let _ = validate_get(
                        &ctx,
                        &key,
                        &ReadOptions::default().with_durability_filter(DurabilityLevel::Remote),
                    )
                    .await?;
                }
            } else {
                let scan_memory = rand.rng().random::<u64>() & 1 == 0;
                let order = if rand.rng().random::<u64>() & 1 == 0 {
                    IterationOrder::Ascending
                } else {
                    IterationOrder::Descending
                };
                let options = ScanOptions::default().with_order(order);
                if scan_memory {
                    let _ = validate_scan::<Vec<u8>, _>(&ctx, .., &options).await?;
                } else {
                    let _ = validate_scan::<Vec<u8>, _>(
                        &ctx,
                        ..,
                        &options.with_durability_filter(DurabilityLevel::Remote),
                    )
                    .await?;
                }
            }

            tokio::task::yield_now().await;
        }

        Ok(())
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
            let advance_ms = 1 + (rand.rng().random::<u64>() % 7);
            let should_yield = rand.rng().random::<u64>() & 1 == 0;
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

/// Performs extra flushes independently of the writer scenario.
///
/// `FlusherScenario` adds durability pressure that is decoupled from the normal
/// write path. On each iteration it randomly decides whether to flush, which
/// increases the variety of interleavings between acknowledged writes, remote
/// durability, and concurrent reads.
///
/// Running flushes in a separate scenario makes the simulation explore states
/// that would be less common if flushing only happened as a side effect of
/// writer activity. Like the writer and reader scenarios, it can run for a
/// fixed number of iterations or continue until shutdown.
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

            let should_flush = rand.rng().random::<u64>() & 1 == 0;
            if should_flush {
                ctx.flush().await?;
            }
            tokio::task::yield_now().await;
        }

        Ok(())
    }
}
