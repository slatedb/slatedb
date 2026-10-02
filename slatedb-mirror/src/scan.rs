//! The periodic remote scan, which removes local copies of objects that were
//! deleted without going through the mirror.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use log::{debug, warn};
use object_store::path::Path;

use crate::inner::Inner;
use crate::layout::{classify, split_data_name, split_path, FileKind, LocalName, LocalObject};
use crate::startup::IO_CONCURRENCY;

/// Scans every `interval`, starting immediately. Runs until cancelled.
pub(crate) async fn run(inner: Arc<Inner>, interval: Duration) {
    let clock = Arc::clone(&inner.clock);
    let mut ticker = clock.ticker(interval);
    loop {
        ticker.tick().await;
        scan(&inner).await;
    }
}

/// Removes local copies whose objects are gone from the remote store. Returns
/// how many were removed.
///
/// The snapshot comes from the cache directory, not the entry map, so a local
/// copy the map is missing still gets cleaned up. It's taken before any
/// LIST. Files are only installed after they exist remotely, so anything
/// installed after the snapshot is skipped and anything in the snapshot but
/// missing from a later LIST was deleted.
pub(crate) async fn scan(inner: &Inner) -> usize {
    let files = match inner.vfs.list(&inner.root).await {
        Ok(files) => files,
        Err(err) => {
            warn!(
                "mirror remote scan failed to list cache directory [root={}, error={}]",
                inner.root.display(),
                err
            );
            return 0;
        }
    };
    // Most paths come straight from the file name and the prefix map, which
    // holds every prefix installed or recovered. Only files with an unknown
    // prefix need their `.meta` read.
    let mut paths = Vec::new();
    let mut unknown = Vec::new();
    {
        let prefixes = inner.prefixes.lock();
        for file in files {
            if classify(&file.name) != FileKind::Data {
                continue;
            }
            let (prefix, name) = split_data_name(&file.name);
            let path = prefixes.parent(prefix).and_then(|parent| {
                let path = if parent.is_empty() {
                    name.to_string()
                } else {
                    format!("{parent}/{name}")
                };
                Path::parse(path).ok()
            });
            match path {
                Some(path) => paths.push(path),
                None => unknown.push(file.name),
            }
        }
    }
    let from_meta: Vec<Path> = stream::iter(unknown)
        .map(|name| async move { local_path(inner, &name).await })
        .buffer_unordered(IO_CONCURRENCY)
        .filter_map(|path| async move { path })
        .collect()
        .await;
    paths.extend(from_meta);

    let mut parents: BTreeMap<String, Vec<Path>> = BTreeMap::new();
    for path in paths {
        let (parent, _) = split_path(&path);
        parents.entry(parent.to_string()).or_default().push(path);
    }

    let mut removed = 0;
    for (parent, paths) in parents {
        let prefix = if parent.is_empty() {
            None
        } else {
            match Path::parse(&parent) {
                Ok(prefix) => Some(prefix),
                Err(err) => {
                    warn!(
                        "skipping unparseable parent in mirror remote scan [parent={}, error={}]",
                        parent, err
                    );
                    continue;
                }
            }
        };
        let listed = inner
            .retry
            .run(|| inner.remote.list_with_delimiter(prefix.as_ref()))
            .await;
        let listed = match listed {
            Ok(listed) => listed,
            Err(err) => {
                warn!(
                    "mirror remote scan failed to list parent [parent={}, error={}]",
                    parent, err
                );
                continue;
            }
        };
        let remote: HashSet<Path> = listed
            .objects
            .into_iter()
            .map(|object| object.location)
            .collect();
        for path in paths.into_iter().filter(|path| !remote.contains(path)) {
            match inner.purge_local(&path).await {
                Ok(()) => {
                    debug!(
                        "removed local copy of remotely deleted object [path={}]",
                        path
                    );
                    removed += 1;
                }
                Err(err) => warn!(
                    "mirror remote scan failed to remove local copy [path={}, error={}]",
                    path, err
                ),
            }
        }
    }
    removed
}

/// Returns the object path of the data file `name` from its `.meta` file, or
/// `None` if it can't be read or doesn't match. Startup removes such files.
async fn local_path(inner: &Inner, name: &str) -> Option<Path> {
    let bytes = match inner.vfs.read(&inner.file(&format!("{name}.meta"))).await {
        Ok(bytes) => bytes,
        // Removed since the listing.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            warn!(
                "mirror remote scan failed to read metadata [name={}, error={}]",
                name, err
            );
            return None;
        }
    };
    let path = match LocalObject::from_meta_bytes(&bytes) {
        Ok(object) => object.meta.location,
        Err(err) => {
            warn!(
                "mirror remote scan skipping file with malformed metadata [name={}, error={}]",
                name, err
            );
            return None;
        }
    };
    match LocalName::new(&path) {
        Ok(local) if local.data == name => Some(path),
        _ => {
            warn!(
                "mirror remote scan skipping file whose metadata doesn't match its name [name={}, path={}]",
                name, path
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path as StdPath;

    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;
    use crate::layout::PrefixMap;
    use crate::ObjectStoreMirror;

    const SST: &str = "db/compacted/01A.sst";
    const SST2: &str = "db/compacted/01B.sst";

    fn files(root: &StdPath) -> BTreeSet<String> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    /// The files a set of complete local copies should leave behind.
    fn expected_files(paths: &[&str]) -> BTreeSet<String> {
        let mut expected = BTreeSet::from(["LOCK".to_string()]);
        for path in paths {
            let name = LocalName::new(&Path::from(*path)).unwrap();
            expected.insert(name.meta());
            expected.insert(name.data);
        }
        expected
    }

    #[tokio::test]
    async fn should_remove_remotely_deleted_objects_in_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let mirror = ObjectStoreMirror::for_test(dir.path(), Arc::clone(&store) as _).await;
        for path in [SST, SST2, "root.sst"] {
            mirror
                .put(&Path::from(path), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        store.delete(&Path::from(SST)).await.unwrap();
        store.delete(&Path::from("root.sst")).await.unwrap();

        assert_eq!(scan(mirror.inner()).await, 2);
        assert!(!mirror.handle().contains(&Path::from(SST)));
        assert!(mirror.handle().contains(&Path::from(SST2)));
        assert_eq!(files(dir.path()), expected_files(&[SST2]));
    }

    #[tokio::test]
    async fn should_scan_local_copies_missing_from_the_map() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let mirror = ObjectStoreMirror::for_test(dir.path(), Arc::clone(&store) as _).await;
        for path in [SST, SST2] {
            mirror
                .put(&Path::from(path), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        // Both files stay on disk, but the maps lose track of them. Without the
        // prefix map, the scan reads each `.meta` for its path.
        let inner = mirror.inner();
        inner.entries.lock().clear();
        *inner.prefixes.lock() = PrefixMap::default();
        store.delete(&Path::from(SST)).await.unwrap();

        assert_eq!(scan(inner).await, 1);
        assert_eq!(files(dir.path()), expected_files(&[SST2]));
    }
}
