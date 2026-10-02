//! The mirror's on-disk file layout.
//!
//! The cache root is flat. Every object-related file name starts with the hex
//! MD5 of the object's parent path, then a `.`, then the object's file name:
//!
//! - `<md5>.<name>` is the complete object.
//! - `<md5>.<name>.meta` holds its canonical path and metadata.
//! - `<md5>.<name>.<counter>` and `<md5>.<name>.meta.<counter>` are temporary
//!   files. The counter is unique within the process.
//! - `LOCK` is held for the mirror's lifetime.
//!
//! File names that end in `.meta` or `.<digits>` can't be told apart from
//! metadata or temporary files, so the mirror refuses to keep local copies of
//! them.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use object_store::path::Path;
use object_store::{Attribute, AttributeValue, Attributes, ObjectMeta};
use serde::{Deserialize, Serialize};

use crate::MirrorError;

/// The lock file held for the mirror's lifetime.
pub(crate) const LOCK_FILE: &str = "LOCK";

const META_SUFFIX: &str = ".meta";

/// Length of a hex MD5 digest.
const PREFIX_LEN: usize = 32;

/// The `.meta` format written by this version.
const META_FORMAT: u32 = 1;

/// Splits an object path into its parent path and file name. Objects at the
/// root have an empty parent.
pub(crate) fn split_path(path: &Path) -> (&str, &str) {
    path.as_ref()
        .rsplit_once('/')
        .unwrap_or(("", path.as_ref()))
}

/// Returns the hex MD5 digest of `parent`.
fn md5_hex(parent: &str) -> String {
    Md5::digest(parent.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Returns true if `name` ends in `.` followed by one or more ASCII digits.
fn has_temp_suffix(name: &str) -> bool {
    name.rsplit_once('.')
        .is_some_and(|(_, suffix)| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
}

/// The local file names for one object path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalName {
    /// Hex MD5 of `parent`.
    pub(crate) prefix: String,
    /// The object's parent path.
    pub(crate) parent: String,
    /// `<prefix>.<file name>`, the complete object.
    pub(crate) data: String,
}

impl LocalName {
    /// Returns the local names for `path`, or `Unsupported` if its file name is
    /// reserved by the layout.
    pub(crate) fn new(path: &Path) -> Result<Self, MirrorError> {
        let (parent, name) = split_path(path);
        let prefix = md5_hex(parent);
        let data = format!("{prefix}.{name}");
        // Check the full name so that names like `7` and `meta` are caught too.
        if name.is_empty() || classify(&data) != FileKind::Data {
            return Err(MirrorError::Unsupported {
                operation: "local copy of a file name that is empty, ends in `.meta` or \
                     `.<digits>`, or is `meta` or all digits",
            });
        }
        Ok(Self {
            prefix,
            parent: parent.to_string(),
            data,
        })
    }

    /// `<prefix>.<file name>.meta`
    pub(crate) fn meta(&self) -> String {
        format!("{}{META_SUFFIX}", self.data)
    }

    /// `<prefix>.<file name>.<counter>`
    pub(crate) fn temp(&self, counter: u64) -> String {
        format!("{}.{counter}", self.data)
    }

    /// `<prefix>.<file name>.meta.<counter>`
    pub(crate) fn meta_temp(&self, counter: u64) -> String {
        format!("{}{META_SUFFIX}.{counter}", self.data)
    }
}

/// What a file in the cache root is, judging by its name alone.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FileKind<'a> {
    Lock,
    /// A leftover temporary file.
    Temp,
    /// A `.meta` file for the data file named `data`.
    Meta {
        data: &'a str,
    },
    Data,
    /// Not part of the layout. Left alone.
    Unknown,
}

pub(crate) fn classify(name: &str) -> FileKind<'_> {
    if name == LOCK_FILE {
        return FileKind::Lock;
    }
    let has_prefix = name.len() > PREFIX_LEN + 1
        && name.as_bytes()[PREFIX_LEN] == b'.'
        && name.as_bytes()[..PREFIX_LEN]
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b));
    if !has_prefix {
        return FileKind::Unknown;
    }
    if has_temp_suffix(name) {
        FileKind::Temp
    } else if let Some(data) = name.strip_suffix(META_SUFFIX) {
        FileKind::Meta { data }
    } else {
        FileKind::Data
    }
}

/// Splits a [`FileKind::Data`] file name into its MD5 prefix and the object's
/// file name.
pub(crate) fn split_data_name(name: &str) -> (&str, &str) {
    (&name[..PREFIX_LEN], &name[PREFIX_LEN + 1..])
}

/// A complete local copy's metadata, as returned by HEAD and GET.
#[derive(Debug, Clone)]
pub(crate) struct LocalObject {
    pub(crate) meta: ObjectMeta,
    pub(crate) attributes: Attributes,
}

/// The JSON contents of a `.meta` file.
#[derive(Debug, Serialize, Deserialize)]
struct MetaFile {
    format: u32,
    location: String,
    last_modified: DateTime<Utc>,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
    /// Standard attributes, keyed by HTTP header name.
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    /// User metadata attributes.
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

fn attribute_name(attribute: &Attribute) -> Option<&'static str> {
    Some(match attribute {
        Attribute::CacheControl => "Cache-Control",
        Attribute::ContentDisposition => "Content-Disposition",
        Attribute::ContentEncoding => "Content-Encoding",
        Attribute::ContentLanguage => "Content-Language",
        Attribute::ContentType => "Content-Type",
        Attribute::StorageClass => "Storage-Class",
        _ => return None,
    })
}

fn attribute_from_name(name: &str) -> Option<Attribute> {
    Some(match name {
        "Cache-Control" => Attribute::CacheControl,
        "Content-Disposition" => Attribute::ContentDisposition,
        "Content-Encoding" => Attribute::ContentEncoding,
        "Content-Language" => Attribute::ContentLanguage,
        "Content-Type" => Attribute::ContentType,
        "Storage-Class" => Attribute::StorageClass,
        _ => return None,
    })
}

impl LocalObject {
    /// Serializes this object's `.meta` file.
    pub(crate) fn to_meta_bytes(&self) -> Vec<u8> {
        let mut attributes = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        for (key, value) in self.attributes.iter() {
            match key {
                Attribute::Metadata(key) => {
                    metadata.insert(key.to_string(), value.to_string());
                }
                key => {
                    if let Some(name) = attribute_name(key) {
                        attributes.insert(name.to_string(), value.to_string());
                    }
                }
            }
        }
        let file = MetaFile {
            format: META_FORMAT,
            location: self.meta.location.to_string(),
            last_modified: self.meta.last_modified,
            size: self.meta.size,
            e_tag: self.meta.e_tag.clone(),
            version: self.meta.version.clone(),
            attributes,
            metadata,
        };
        serde_json::to_vec(&file).expect("MetaFile always serializes")
    }

    /// Parses a `.meta` file. Unknown standard attributes are dropped.
    pub(crate) fn from_meta_bytes(bytes: &[u8]) -> Result<Self, String> {
        let file: MetaFile = serde_json::from_slice(bytes).map_err(|err| err.to_string())?;
        if file.format != META_FORMAT {
            return Err(format!("unknown format {}", file.format));
        }
        let location = Path::parse(&file.location).map_err(|err| err.to_string())?;
        let mut attributes = Attributes::new();
        for (name, value) in file.attributes {
            if let Some(attribute) = attribute_from_name(&name) {
                attributes.insert(attribute, AttributeValue::from(value));
            }
        }
        for (key, value) in file.metadata {
            attributes.insert(Attribute::Metadata(key.into()), AttributeValue::from(value));
        }
        Ok(Self {
            meta: ObjectMeta {
                location,
                last_modified: file.last_modified,
                size: file.size,
                e_tag: file.e_tag,
                version: file.version,
            },
            attributes,
        })
    }
}

/// Maps each MD5 prefix to the parent path it stands for.
///
/// A different parent with the same prefix is an MD5 collision. The mirror
/// rejects it rather than mixing two directories' files.
#[derive(Debug, Default)]
pub(crate) struct PrefixMap {
    parents: HashMap<String, String>,
}

impl PrefixMap {
    /// Records `name`'s prefix, or fails if it already maps to another parent.
    pub(crate) fn check_or_insert(&mut self, name: &LocalName) -> Result<(), MirrorError> {
        match self.parents.get(&name.prefix) {
            Some(parent) if *parent == name.parent => Ok(()),
            Some(parent) => Err(MirrorError::Local {
                source: std::io::Error::other(format!(
                    "MD5 prefix `{}` already maps to parent `{parent}`, not `{}`",
                    name.prefix, name.parent
                )),
            }),
            None => {
                self.parents
                    .insert(name.prefix.clone(), name.parent.clone());
                Ok(())
            }
        }
    }

    /// Returns the parent path `prefix` stands for, if it's known.
    pub(crate) fn parent(&self, prefix: &str) -> Option<&str> {
        self.parents.get(prefix).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_name_files_by_parent_md5() {
        let name = LocalName::new(&Path::from("path/to/db/compacted/01K.sst")).unwrap();
        assert_eq!(name.prefix, md5_hex("path/to/db/compacted"));
        assert_eq!(name.parent, "path/to/db/compacted");
        assert_eq!(name.data, format!("{}.01K.sst", name.prefix));
        assert_eq!(name.meta(), format!("{}.01K.sst.meta", name.prefix));
        assert_eq!(name.temp(7), format!("{}.01K.sst.7", name.prefix));
        assert_eq!(name.meta_temp(7), format!("{}.01K.sst.meta.7", name.prefix));

        let root = LocalName::new(&Path::from("01K.sst")).unwrap();
        assert_eq!(root.parent, "");
        assert_eq!(root.prefix, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn should_reject_reserved_file_names() {
        for path in ["a/b.meta", "a/b.123", "a/7", "a/meta", "meta.1", ""] {
            assert!(
                matches!(
                    LocalName::new(&Path::from(path)),
                    Err(MirrorError::Unsupported { .. })
                ),
                "{path}"
            );
        }
        for path in ["a/b.sst", "a/b.1x", "a/b.", "a/metadata", "a/b.meta.sst"] {
            assert!(LocalName::new(&Path::from(path)).is_ok(), "{path}");
        }
    }

    #[test]
    fn should_classify_files() {
        let name = LocalName::new(&Path::from("a/b.sst")).unwrap();
        assert_eq!(classify("LOCK"), FileKind::Lock);
        assert_eq!(classify(&name.data), FileKind::Data);
        assert_eq!(classify(&name.meta()), FileKind::Meta { data: &name.data });
        assert_eq!(classify(&name.temp(3)), FileKind::Temp);
        assert_eq!(classify(&name.meta_temp(3)), FileKind::Temp);
        assert_eq!(classify("notes.txt"), FileKind::Unknown);
        assert_eq!(
            classify("D41D8CD98F00B204E9800998ECF8427E.b.sst"),
            FileKind::Unknown
        );
        assert_eq!(classify(&format!("{}.", name.prefix)), FileKind::Unknown);
        assert_eq!(classify(&name.prefix), FileKind::Unknown);
    }

    #[test]
    fn should_round_trip_meta_files() {
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, "application/octet-stream".into());
        attributes.insert(Attribute::StorageClass, "STANDARD".into());
        attributes.insert(Attribute::Metadata("Content-Type".into()), "user".into());
        let object = LocalObject {
            meta: ObjectMeta {
                location: Path::from("a/b.sst"),
                last_modified: DateTime::from_timestamp(1_700_000_000, 123).unwrap(),
                size: 42,
                e_tag: Some("\"etag\"".to_string()),
                version: None,
            },
            attributes,
        };

        let parsed = LocalObject::from_meta_bytes(&object.to_meta_bytes()).unwrap();
        assert_eq!(parsed.meta, object.meta);
        assert_eq!(parsed.attributes, object.attributes);
    }

    #[test]
    fn should_reject_malformed_meta_files() {
        assert!(LocalObject::from_meta_bytes(b"").is_err());
        assert!(LocalObject::from_meta_bytes(b"{}").is_err());
        let wrong_format = br#"{"format":2,"location":"a","last_modified":"2024-01-01T00:00:00Z","size":1,"e_tag":null,"version":null}"#;
        assert!(LocalObject::from_meta_bytes(wrong_format).is_err());
        let bad_path = br#"{"format":1,"location":"a//b","last_modified":"2024-01-01T00:00:00Z","size":1,"e_tag":null,"version":null}"#;
        assert!(LocalObject::from_meta_bytes(bad_path).is_err());
    }

    #[test]
    fn should_reject_conflicting_prefixes() {
        let mut prefixes = PrefixMap::default();
        let name = LocalName::new(&Path::from("a/b.sst")).unwrap();
        prefixes.check_or_insert(&name).unwrap();
        prefixes.check_or_insert(&name).unwrap();

        let collision = LocalName {
            parent: "other".to_string(),
            ..name
        };
        assert!(matches!(
            prefixes.check_or_insert(&collision),
            Err(MirrorError::Local { .. })
        ));
    }
}
