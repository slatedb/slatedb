//! Provides global context used by SlateDB.
//!
//! A `DbContext` encapsulates three shared resources:
//! * A root random number generator (RNG) that seeds per‑thread RNGs.
//! * A system clock implementation.
//! * A logical clock implementation.
//!
//! Each thread lazily obtains its own RNG derived from the root RNG via
//! `thread_rng()`. This allows deterministic testing when a seed is provided
//! and avoids contention on the root RNG.  `DbContext` is typically created
//! through [`DbContextBuilder`], which lets callers specify the initial seed
//! and custom clock implementations.
//!
//! `ThreadRng` is a lightweight wrapper that implements `RngCore` and forwards
//! all RNG operations to the per‑thread RNG managed by the context.
//! It is returned by `DbContext::thread_rng()`.
//!
//! Users generally don't need to worry about DbContext. It's primarily useful
//! for deterministic simulation testing, where all random behavior (thread_rand,
//! uuid, ulid, clocks, etc.) must be driven off a single seed.
#![allow(clippy::disallowed_types, clippy::disallowed_methods)]
use crate::clock::{DefaultLogicalClock, DefaultSystemClock, LogicalClock, SystemClock};
use std::sync::Arc;

/// Shared context for database operations.
#[derive(Debug)]
pub struct DbContext {
    system_clock: Arc<dyn SystemClock>,
    logical_clock: Arc<dyn LogicalClock>,
}

impl Default for DbContext {
    fn default() -> Self {
        Self::new(
            Arc::new(DefaultSystemClock::default()),
            Arc::new(DefaultLogicalClock::default()),
        )
    }
}

impl DbContext {
    pub fn new(system_clock: Arc<dyn SystemClock>, logical_clock: Arc<dyn LogicalClock>) -> Self {
        Self {
            system_clock,
            logical_clock,
        }
    }

    #[inline]
    pub(crate) fn system_clock(&self) -> Arc<dyn SystemClock> {
        self.system_clock.clone()
    }

    #[inline]
    pub(crate) fn logical_clock(&self) -> Arc<dyn LogicalClock> {
        self.logical_clock.clone()
    }
}
