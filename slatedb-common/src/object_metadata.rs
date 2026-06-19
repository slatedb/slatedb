use chrono::{DateTime, Utc};
use object_store::{path::Path, ObjectMeta};

/// Metadata describing a SlateDB object in object storage.
///
/// This mirrors [`object_store::ObjectMeta`] so SlateDB crates can use a common
/// public type without exposing the upstream crate's type directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

/// Metadata for an object plus the domain identifier parsed from its path.
///
/// This is returned by public APIs that expose both object-store metadata and
/// the SlateDB identifier derived from the object name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifiedObjectMetadata<Id> {
    /// The parsed domain identifier for the object.
    pub id: Id,
    /// The object-store metadata.
    pub metadata: ObjectMetadata,
}

impl<Id> IdentifiedObjectMetadata<Id> {
    /// Creates metadata for an object with the provided identifier.
    pub fn new(id: Id, metadata: ObjectMetadata) -> Self {
        Self { id, metadata }
    }

    /// Creates metadata for an object from an upstream object-store metadata value.
    pub fn from_object_meta(id: Id, meta: ObjectMeta) -> Self {
        Self::new(id, ObjectMetadata::new(meta))
    }

    /// Converts the identifier while preserving the object metadata.
    pub fn map_id<NewId, F>(self, f: F) -> IdentifiedObjectMetadata<NewId>
    where
        F: FnOnce(Id) -> NewId,
    {
        IdentifiedObjectMetadata {
            id: f(self.id),
            metadata: self.metadata,
        }
    }
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
