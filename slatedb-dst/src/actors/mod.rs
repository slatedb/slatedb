//! Reusable deterministic workload actors for DST scenarios.

pub mod clock;
pub mod deleter;
pub mod flusher;
pub mod writer;

pub use self::clock::clock;
pub use self::deleter::deleter;
pub use self::flusher::flusher;
pub use self::writer::writer;

/// Each split actor executes 25 steps so a `10/4/1` writer/deleter/flusher
/// registration reproduces the old 250/100/25 active-operation mix from 500
/// weighted `rand_value % 100` iterations.
const WORKLOAD_STEPS: u64 = 25;
const PROGRESS_LOG_INTERVAL: u64 = 10;
