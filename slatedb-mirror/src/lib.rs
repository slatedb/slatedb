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

mod download;
pub mod error;
mod handle;
mod inner;
mod layout;
mod mirror;
mod multipart;
mod ordering;
mod policy;
mod retry;
mod scan;
mod startup;
pub mod vfs;

pub use error::MirrorError;
pub use handle::MirrorHandle;
pub use mirror::{
    ObjectStoreMirror, ObjectStoreMirrorBuilder, DEFAULT_DOWNLOAD_CONCURRENCY,
    DEFAULT_REMOTE_SCAN_INTERVAL,
};
pub use policy::{MirrorPolicy, ReadRoute, WriteRoute};
pub use vfs::{StdVfs, Vfs, VfsEntry, VfsLock, VfsWriter};
