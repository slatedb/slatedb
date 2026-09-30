//! Startup cleanup and recovery of the in-memory maps.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::path::Path as StdPath;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{self, StreamExt};
use log::{info, warn};
use object_store::path::Path;

use crate::inner::{local_error, remove_quietly};
use crate::layout::{classify, FileKind, LocalName, LocalObject, PrefixMap};
use crate::vfs::Vfs;
use crate::MirrorError;

/// How many `.meta` files to read or files to remove at once.
pub(crate) const IO_CONCURRENCY: usize = 64;

pub(crate) struct Recovered {
    pub(crate) entries: HashMap<Path, Arc<LocalObject>>,
    pub(crate) prefixes: PrefixMap,
}

/// Cleans up `root` and rebuilds the in-memory maps from what's left.
///
/// Removes temporary files, data and `.meta` files without a partner, and
/// pairs with malformed metadata, a path that doesn't match the file name, a
/// size that doesn't match the metadata, or an MD5 prefix that already maps to
/// another parent. Files that aren't part of the layout are left alone.
pub(crate) async fn recover(vfs: &dyn Vfs, root: &StdPath) -> Result<Recovered, MirrorError> {
    let mut files = vfs.list(root).await.map_err(local_error)?;
    // Sorted, so the same directory always resolves prefix conflicts the same
    // way.
    files.sort_by(|a, b| a.name.cmp(&b.name));

    let mut remove = Vec::new();
    let mut data = BTreeMap::new();
    let mut metas = BTreeSet::new();
    for file in files {
        match classify(&file.name) {
            FileKind::Lock => {}
            FileKind::Unknown => warn!(
                "ignoring file that isn't part of the mirror layout [name={}]",
                file.name
            ),
            FileKind::Temp => remove.push(file.name),
            FileKind::Meta { .. } => {
                metas.insert(file.name);
            }
            FileKind::Data => {
                data.insert(file.name, file.size);
            }
        }
    }

    let mut pairs = Vec::new();
    for (data, size) in data {
        let meta = format!("{data}.meta");
        if metas.remove(&meta) {
            pairs.push((data, meta, size));
        } else {
            remove.push(data);
        }
    }
    remove.extend(metas);

    let read: Vec<_> = stream::iter(pairs)
        .map(|(data, meta, size)| async move {
            let bytes = vfs.read(&root.join(&meta)).await;
            (data, meta, size, bytes)
        })
        .buffered(IO_CONCURRENCY)
        .collect()
        .await;

    let mut entries = HashMap::new();
    let mut prefixes = PrefixMap::default();
    for (data, meta, size, bytes) in read {
        let valid = validate(&data, size, bytes).and_then(|(name, object)| {
            prefixes
                .check_or_insert(&name)
                .map_err(|err| err.to_string())?;
            Ok(object)
        });
        match valid {
            Ok(object) => {
                entries.insert(object.meta.location.clone(), Arc::new(object));
            }
            Err(reason) => {
                warn!(
                    "removing invalid mirror entry [name={}, reason={}]",
                    data, reason
                );
                remove.push(data);
                remove.push(meta);
            }
        }
    }

    let removed = remove.len();
    stream::iter(remove)
        .for_each_concurrent(IO_CONCURRENCY, |name| async move {
            remove_quietly(vfs, &root.join(name)).await
        })
        .await;
    info!(
        "recovered object store mirror [root={}, objects={}, removed_files={}]",
        root.display(),
        entries.len(),
        removed
    );

    Ok(Recovered { entries, prefixes })
}

fn validate(
    data: &str,
    size: u64,
    bytes: io::Result<Bytes>,
) -> Result<(LocalName, LocalObject), String> {
    let bytes = bytes.map_err(|err| format!("unreadable metadata: {err}"))?;
    let object = LocalObject::from_meta_bytes(&bytes)?;
    let location = &object.meta.location;
    let name = LocalName::new(location).map_err(|err| err.to_string())?;
    if name.data != data {
        return Err(format!("path `{location}` doesn't match the file name"));
    }
    if object.meta.size != size {
        return Err(format!(
            "file is {size} bytes but metadata says {}",
            object.meta.size
        ));
    }
    Ok((name, object))
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use object_store::{Attributes, ObjectMeta};

    use super::*;
    use crate::vfs::StdVfs;

    fn object(path: &str, size: u64) -> LocalObject {
        LocalObject {
            meta: ObjectMeta {
                location: Path::from(path),
                last_modified: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
                size,
                e_tag: None,
                version: None,
            },
            attributes: Attributes::new(),
        }
    }

    fn write(root: &StdPath, name: &str, contents: &[u8]) {
        std::fs::write(root.join(name), contents).unwrap();
    }

    fn write_pair(root: &StdPath, path: &str, contents: &[u8]) -> LocalName {
        let name = LocalName::new(&Path::from(path)).unwrap();
        write(root, &name.data, contents);
        write(
            root,
            &name.meta(),
            &object(path, contents.len() as u64).to_meta_bytes(),
        );
        name
    }

    fn file_names(root: &StdPath) -> BTreeSet<String> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn should_keep_valid_pairs_and_remove_everything_else() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "LOCK", b"");
        write(root, "notes.txt", b"not ours");

        let valid = write_pair(root, "db/compacted/a.sst", b"aaaa");
        let root_level = write_pair(root, "b.sst", b"b");

        // Temporary files.
        write(root, &valid.temp(3), b"partial");
        write(root, &valid.meta_temp(4), b"partial");

        // Data without metadata, and metadata without data.
        let data_only = LocalName::new(&Path::from("db/compacted/c.sst")).unwrap();
        write(root, &data_only.data, b"c");
        let meta_only = LocalName::new(&Path::from("db/compacted/d.sst")).unwrap();
        write(
            root,
            &meta_only.meta(),
            &object("db/compacted/d.sst", 1).to_meta_bytes(),
        );

        // Malformed metadata.
        let malformed = LocalName::new(&Path::from("db/compacted/e.sst")).unwrap();
        write(root, &malformed.data, b"e");
        write(root, &malformed.meta(), b"{not json");

        // Metadata for a different path than the file name.
        let mismatched = LocalName::new(&Path::from("db/compacted/f.sst")).unwrap();
        write(root, &mismatched.data, b"f");
        write(
            root,
            &mismatched.meta(),
            &object("db/compacted/g.sst", 1).to_meta_bytes(),
        );

        // Torn data file.
        let torn = LocalName::new(&Path::from("db/compacted/h.sst")).unwrap();
        write(root, &torn.data, b"");
        write(
            root,
            &torn.meta(),
            &object("db/compacted/h.sst", 10).to_meta_bytes(),
        );

        let recovered = recover(&StdVfs::new(), root).await.unwrap();

        let mut paths: Vec<_> = recovered.entries.keys().cloned().collect();
        paths.sort();
        assert_eq!(
            paths,
            vec![Path::from("b.sst"), Path::from("db/compacted/a.sst")]
        );
        assert_eq!(
            file_names(root),
            BTreeSet::from([
                "LOCK".to_string(),
                "notes.txt".to_string(),
                valid.data.clone(),
                valid.meta(),
                root_level.data.clone(),
                root_level.meta(),
            ])
        );
    }
}
