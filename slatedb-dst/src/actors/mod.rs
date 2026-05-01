//! Reusable deterministic workload actors for DST scenarios.
//!
//! These actors are intentionally small building blocks rather than a full
//! scenario framework. Each actor operates only through [`crate::ActorCtx`], so
//! its behavior is driven entirely by the harness-provided seeded RNG, shared
//! database handle, and shared mock clock.
//!
//! The harness owns the outer execution loop and repeatedly calls
//! [`crate::Actor::run`] until the shared shutdown token is cancelled.
//!
//! Registering `workload` and `flusher` actors together preserves the old
//! scenario shape of steady write churn plus explicit flush pressure, while the
//! workload actor now embeds monotonic read verification.

pub mod bank;
pub mod compactor;
pub mod fencer;
pub mod flusher;
pub mod shutdown;
pub mod suppress_errors;
pub mod workload;

pub use self::bank::{
    initialize_accounts, AuditorActor, BankAuditView, BankMergeOperator, BankOptions,
    TransferActor, TransferMode,
};
pub use self::compactor::{CompactorActor, CompactorActorOptions};
pub use self::fencer::{DbFencerActor, DbFencerActorOptions, SuppressFenced};
pub use self::flusher::FlusherActor;
pub use self::shutdown::ShutdownActor;
pub use self::suppress_errors::SuppressErrorActor;
pub use self::workload::{WorkloadActor, WorkloadActorOptions};

/// Emit one progress log line every N completed steps for the looping actors.
const PROGRESS_LOG_INTERVAL: u64 = 10;
