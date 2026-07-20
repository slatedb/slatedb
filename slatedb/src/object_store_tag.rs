//! The per-call tag SlateDB attaches to the object store calls which the
//! [`TableStore`](crate::tablestore::TableStore) issues for an SST.
//!
//! The tag is part of the [`object_store::Extensions`] on every `GetOptions`,
//! `PutOptions`, and `PutMultipartOptions` the TableStore builds for an SST.
//! The TableStore is the only writer of the tag; a caching object store wrapper
//! is the reader.

use object_store::Extensions;

use crate::db_state::SstType;
use crate::error::RetryReason;

/// Identifies the component whose [`TableStore`](crate::tablestore::TableStore)
/// issued an object store call (the call source). Tagged on every SST read and
/// write alongside the [`SstType`]. A caching wrapper combines it with the call
/// type (get vs put) to decide admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TableStoreKind {
    /// The primary database store: foreground reads and memtable flush writes.
    Main,
    /// A read-only store.
    Reader,
    /// The compactor store: compaction-input reads, compaction-output writes.
    Compactor,
    /// The garbage collector store.
    GC,
}

/// The tag carried on every TableStore SST object store call via
/// [`object_store::Extensions`].
///
/// An `ObjectStore` wrapper (such as the bundled object store cache) reads the
/// tag to decide the action for the call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ObjectStoreCallTag {
    /// The source of the call, to distinguish main store, compactor, etc.
    pub(crate) kind: TableStoreKind,
    /// The kind of SST the call is targeting (WAL vs compacted).
    pub(crate) sst_type: SstType,
    /// The reason for retry if this call is reissued after a validation failure
    /// on a read.
    pub(crate) retry: Option<RetryReason>,
}

impl ObjectStoreCallTag {
    /// A tag with no retry reason: the common case (a read sets the retry reason
    /// itself on a reissue).
    pub(crate) fn new(kind: TableStoreKind, sst_type: SstType) -> Self {
        Self {
            kind,
            sst_type,
            retry: None,
        }
    }

    /// Reads the tag back from an extensions map, if present.
    pub(crate) fn from_extensions(extensions: &Extensions) -> Option<Self> {
        extensions.get::<Self>().copied()
    }
}

impl From<ObjectStoreCallTag> for Extensions {
    fn from(tag: ObjectStoreCallTag) -> Self {
        let mut extensions = Extensions::new();
        extensions.insert(tag);
        extensions
    }
}
