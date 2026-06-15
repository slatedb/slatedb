use chrono::{DateTime, Utc};
use object_store::{path::Path, ObjectMeta};

/// Metadata describing a SlateDB object in object storage.
///
/// This mirrors [`object_store::ObjectMeta`] so SlateDB crates can use a common
/// public type without exposing the upstream crate's type directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// The full path to the object.
    pub location: Path,
    /// The last modified time.
    pub last_modified: DateTime<Utc>,
    /// The size in bytes of the object.
    pub size: u64,
    /// The unique identifier for the object.
    pub e_tag: Option<String>,
    /// A version indicator for this object.
    pub version: Option<String>,
}

impl ObjectMetadata {
    pub fn new(meta: ObjectMeta) -> Self {
        Self {
            location: meta.location,
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }
}

impl From<ObjectMetadata> for ObjectMeta {
    fn from(meta: ObjectMetadata) -> Self {
        Self {
            location: meta.location,
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }
}
