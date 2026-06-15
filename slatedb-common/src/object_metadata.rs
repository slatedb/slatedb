use chrono::{DateTime, Utc};
use object_store::{path::Path, ObjectMeta};

/// Metadata describing a SlateDB object in object storage.
///
/// This mirrors [`object_store::ObjectMeta`] so SlateDB crates can use a common
/// public type without exposing the upstream crate's type directly. `Id` is the
/// parsed domain identifier for the object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata<Id> {
    /// The parsed domain identifier for the object.
    pub id: Id,
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

impl<Id> ObjectMetadata<Id> {
    pub fn new(id: Id, meta: ObjectMeta) -> Self {
        Self {
            id,
            location: meta.location,
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }

    pub fn with_id<NewId>(self, id: NewId) -> ObjectMetadata<NewId> {
        ObjectMetadata {
            id,
            location: self.location,
            last_modified: self.last_modified,
            size: self.size,
            e_tag: self.e_tag,
            version: self.version,
        }
    }
}

impl<Id> From<ObjectMetadata<Id>> for ObjectMeta {
    fn from(meta: ObjectMetadata<Id>) -> Self {
        Self {
            location: meta.location,
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }
}
