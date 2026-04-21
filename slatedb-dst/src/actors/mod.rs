//! Reusable deterministic workload actors for DST scenarios.
//!
//! These actors are intentionally small building blocks rather than a full
//! scenario framework. Each actor operates only through [`crate::ActorCtx`], so
//! its behavior is driven entirely by the harness-provided seeded RNG, shared
//! database handle, and shared mock clock.
//!
//! The `writer`, `deleter`, and `flusher` actors are bounded foreground-style
//! workloads. Each one executes exactly [`WORKLOAD_STEPS`] iterations and then
//! returns. Registering them with counts `10/4/1` reproduces the legacy
//! weighted workload mix of roughly 50% writes, 20% deletes, and 5% explicit
//! flushes that previously came from 500 `rand_value % 100` iterations.
//!
//! The `clock` actor is different: it is an unbounded background helper that
//! advances the shared mock clock forever. It is meant to be registered as a
//! background actor so the harness aborts it once all foreground actors finish.

pub mod clock;
pub mod deleter;
pub mod flusher;
pub mod writer;

pub use self::clock::clock;
pub use self::deleter::deleter;
pub use self::flusher::flusher;
pub use self::writer::writer;

/// Each bounded workload actor executes 25 steps.
///
/// A `10/4/1` writer/deleter/flusher registration therefore reproduces the old
/// `250/100/25` active-operation mix from 500 weighted `rand_value % 100`
/// iterations.
const WORKLOAD_STEPS: u64 = 25;

/// Emit one progress log line every N completed steps for the bounded actors.
const PROGRESS_LOG_INTERVAL: u64 = 10;
