//! A local whole-object mirror for an [`object_store::ObjectStore`].
//!
//! See [RFC 0034](https://github.com/slatedb/slatedb/blob/main/rfcs/0034-local-object-mirroring.md).
//!
//! `slatedb-mirror` must not depend on `slatedb`. SlateDB-specific rules live in
//! `slatedb` as a mirror policy.

#![cfg_attr(test, allow(clippy::unwrap_used))]
#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic))]
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

pub mod error;

pub use error::MirrorError;
