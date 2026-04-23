//! Reusable deterministic workload actors for DST scenarios.
//!
//! These actors are intentionally small building blocks rather than a full
//! scenario framework. Each actor operates only through [`crate::ActorCtx`], so
//! its behavior is driven entirely by the harness-provided seeded RNG, shared
//! database handle, and shared mock clock.
//!
//! The `workload`, `flusher`, `clock`, and `compactor` actors are
//! unbounded loops. Register them alongside a separate shutdown actor to build
//! deterministic, time-bounded scenarios.
//!
//! Registering `workload` and `flusher` actors together preserves the old
//! scenario shape of steady write churn plus explicit flush pressure, while the
//! workload actor now embeds read verification against an actor-local oracle.

pub mod bank;
pub mod clock;
pub mod compactor;
pub mod flusher;
pub mod shutdown;
pub mod workload;

pub use self::bank::initialize_accounts;
pub use self::clock::clock;
pub use self::compactor::{compactor, CompactorActorOptions};
pub use self::flusher::flusher;
pub use self::shutdown::shutdown;
pub use self::workload::{workload, WorkloadKeyspace};

/// Emit one progress log line every N completed steps for the looping actors.
const PROGRESS_LOG_INTERVAL: u64 = 10;
