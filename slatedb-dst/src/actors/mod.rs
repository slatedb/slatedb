//! Reusable deterministic workload actors for DST scenarios.
//!
//! These actors are intentionally small building blocks rather than a full
//! scenario framework. Each actor operates only through [`crate::ActorCtx`], so
//! its behavior is driven entirely by the harness-provided seeded RNG, shared
//! database handle, and shared mock clock.
//!
//! The `writer`, `deleter`, `flusher`, and `clock` actors are unbounded loops.
//! Register them alongside a separate shutdown actor to build deterministic,
//! time-bounded scenarios.
//!
//! Registering `writer`/`deleter`/`flusher` with counts `10/4/1` preserves the
//! same relative workload mix as the old bounded scenario, but the total number
//! of operations now depends on when the scenario requests shutdown.

pub mod clock;
pub mod deleter;
pub mod flusher;
pub mod shutdown;
pub mod writer;

pub use self::clock::clock;
pub use self::deleter::deleter;
pub use self::flusher::flusher;
pub use self::shutdown::shutdown;
pub use self::writer::writer;

/// Emit one progress log line every N completed steps for the looping actors.
const PROGRESS_LOG_INTERVAL: u64 = 10;
