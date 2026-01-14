//! Common utilities shared across SlateDB crates.
//!
//! This crate contains utilities that are used by multiple SlateDB
//! crates. It is intended to be a lightweight dependency that can be
//! included in other crates without adding significant overhead.
//!
//! `slatedb-common` should not depend on any other SlateDB crates to avoid
//! circular dependencies. It can depend on third-party crates as needed.

#![cfg_attr(test, allow(clippy::unwrap_used))]
#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic))]
#![allow(clippy::result_large_err, clippy::too_many_arguments)]
// Disallow non-approved non-deterministic types and functions in production code
#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        clippy::disallowed_methods
    )
)]

pub mod clock;
pub mod utils;

#[cfg(feature = "test-util")]
pub use clock::MockSystemClock;
pub use clock::{DefaultSystemClock, SystemClock, SystemClockTicker};
pub use utils::timeout;
