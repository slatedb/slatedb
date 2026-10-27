use std::collections::HashMap;

use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::wal::slatedb::store::WalFileId;
use object_store::path::Path;
use ulid::{Ulid, ULID_LEN};

const WAL_PATH: &str = "wal";
const COMPACTED_PATH: &str = "compacted";
const SST_EXTENSION: &[u8] = b".sst";
const SST_FILE_NAME_LEN: usize = ULID_LEN + SST_EXTENSION.len();
/// Longest SST path [`PathResolver::sst_path`] assembles on the stack; longer
/// paths fall back to [`Path::join`], which allocates twice.
const SST_PATH_STACK_LEN: usize = 256;

/// Resolves the object store paths of a SlateDB database's files from the
/// database's root path.
///
/// Useful outside the database handle, for example to map the SST ids in a
/// [`VersionedManifest`](crate::VersionedManifest) to paths in Object Store
///
/// Can be used when preloading `CachedObjectStore`, or for administrative
/// tooling that inspects a database's objects directly.
///
/// Constructed from a manifest via [`Self::new`]. The manifest is required
/// because it can reference SSTs owned by another database (external SSTs,
/// from cloning), which live outside this database's root path and cannot be
/// resolved from the root path alone.
#[derive(Clone, Debug)]
pub struct PathResolver {
    root_path: Path,
    /// `<root_path>/compacted`, the directory holding this database's own SSTs.
    /// Precomputed because [`Self::sst_path`] is on the read path.
    pub(crate) compacted_dir: Path,
    /// The `compacted` directory of the database owning each external SST.
    external_compacted_dirs: HashMap<SsTableId, Path>,
}

impl PathResolver {
    /// Creates a resolver for the database rooted at `root_path`, resolving
    /// the external SSTs referenced by `manifest` to their owning database's
    /// path.
    pub fn new<P: Into<Path>>(root_path: P, manifest: &crate::manifest::VersionedManifest) -> Self {
        Self::new_with_external_ssts(root_path.into(), manifest.external_ssts())
    }

    /// Creates a resolver for the database rooted at `root_path`.
    ///
    /// Internal only: without a manifest, external SSTs resolve to wrong
    /// paths under `root_path`, so callers must only use this where external
    /// SSTs cannot appear (for example WAL paths).
    pub(crate) fn from_root<P: Into<Path>>(root_path: P) -> Self {
        Self::new_with_external_ssts(root_path, HashMap::new())
    }

    pub(crate) fn new_with_external_ssts<P: Into<Path>>(
        root_path: P,
        external_ssts: HashMap<SsTableId, P>,
    ) -> Self {
        let root_path = root_path.into();
        Self {
            compacted_dir: Self::compacted_dir_of(&root_path),
            external_compacted_dirs: external_ssts
                .into_iter()
                .map(|(k, v)| (k, Self::compacted_dir_of(&v.into())))
                .collect(),
            root_path,
        }
    }

    fn compacted_dir_of(root_path: &Path) -> Path {
        Path::from(format!("{}/{}", root_path, COMPACTED_PATH))
    }

    pub(crate) fn wal_path(&self) -> Path {
        Path::from(format!("{}/{}/", &self.root_path, WAL_PATH))
    }

    pub(crate) fn parse_table_id(&self, path: &Path) -> Result<Option<SsTableId>, SlateDBError> {
        if let Some(mut suffix_iter) = path.prefix_match(&self.root_path) {
            match suffix_iter.next() {
                Some(a) if a.as_ref() == COMPACTED_PATH => suffix_iter
                    .next()
                    .and_then(|s| s.as_ref().split('.').next().map(Ulid::from_string))
                    .transpose()
                    .map(|result| result.map(SsTableId::from))
                    .map_err(|_| SlateDBError::InvalidDBState),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn parse_wal_file_id(&self, path: &Path) -> Result<Option<WalFileId>, SlateDBError> {
        if let Some(mut suffix_iter) = path.prefix_match(&self.root_path) {
            match suffix_iter.next() {
                Some(a) if a.as_ref() == WAL_PATH => suffix_iter
                    .next()
                    .and_then(|s| s.as_ref().split('.').next().map(|s| s.parse::<u64>()))
                    .transpose()
                    .map(|result| result.map(WalFileId::from))
                    .map_err(|_| SlateDBError::InvalidDBState),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Returns the path of the SST with the given id.
    pub fn sst_path(&self, table_id: &SsTableId) -> Path {
        let compacted_dir = self
            .external_compacted_dirs
            .get(table_id)
            .unwrap_or(&self.compacted_dir);
        let mut file_name = [0u8; SST_FILE_NAME_LEN];
        let (ulid, ext) = file_name.split_at_mut(ULID_LEN);
        table_id
            .value()
            .array_to_str(ulid.try_into().expect("ULID_LEN bytes"));
        ext.copy_from_slice(SST_EXTENSION);
        let file_name = std::str::from_utf8(&file_name).expect("ULID text is ASCII");

        // Assemble the path on the stack so the only allocation is the
        // exact-size copy `Path::parse` makes.
        let dir = compacted_dir.as_ref().as_bytes();
        let len = dir.len() + 1 + SST_FILE_NAME_LEN;
        if len > SST_PATH_STACK_LEN {
            return compacted_dir.clone().join(file_name);
        }
        let mut buf = [0u8; SST_PATH_STACK_LEN];
        buf[..dir.len()].copy_from_slice(dir);
        buf[dir.len()] = b'/';
        buf[dir.len() + 1..len].copy_from_slice(file_name.as_bytes());
        let path = std::str::from_utf8(&buf[..len]).expect("joined from two valid strs");
        Path::parse(path).unwrap_or_else(|_| compacted_dir.clone().join(file_name))
    }

    /// Returns the path of the WAL SST with the given id.
    pub(crate) fn wal_sst_path(&self, wal_file_id: &WalFileId) -> Path {
        Path::from(format!(
            "{}/{}/{:020}.sst",
            self.root_path,
            WAL_PATH,
            wal_file_id.value()
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::db_state::SsTableId;
    use crate::paths::PathResolver;
    use crate::wal::slatedb::store::WalFileId;
    use object_store::path::Path;
    use proptest::arbitrary::any;
    use proptest::proptest;
    use std::collections::HashMap;
    use ulid::Ulid;

    const ROOT: &str = "/root";

    proptest! {
        #[test]
        fn should_build_sst_path_identical_to_legacy_format(
            compacted_id in any::<u128>(),
            // Long enough to cover the fallback past `SST_PATH_STACK_LEN`,
            // especially once characters like `%` are percent-encoded.
            root in "\\PC{0,300}",
            external_root in "\\PC{0,300}",
        ) {
            let table_id = SsTableId::from(Ulid::from(compacted_id));
            let legacy = |root: &Path| {
                Path::from(format!("{}/compacted/{}.sst", root, table_id.value()))
            };
            let root = Path::from(root);
            let external_root = Path::from(external_root);

            let local = PathResolver::from_root(root.clone());
            assert_eq!(legacy(&root), local.sst_path(&table_id));
            // `<root>/compacted/` with the trailing delimiter is the same Path.
            assert_eq!(
                &Path::from(format!("{}/compacted/", root)),
                &local.compacted_dir
            );

            let external = PathResolver::new_with_external_ssts(
                root,
                HashMap::from([(table_id, external_root.clone())]),
            );
            assert_eq!(legacy(&external_root), external.sst_path(&table_id));
        }

        #[test]
        fn should_serialize_and_deserialize_wal_paths(
            wal_id in any::<u64>(),
        ) {
            let path_resolver = PathResolver::from_root(Path::from(ROOT));
            let wal_file_id = WalFileId::from(wal_id);
            let path = path_resolver.wal_sst_path(&wal_file_id);
            let parsed_wal_file_id = path_resolver.parse_wal_file_id(&path).unwrap();
            assert_eq!(Some(wal_file_id), parsed_wal_file_id);
        }

        #[test]
        fn should_serialize_and_deserialize_compacted_paths(
            compacted_id in any::<u128>(),
        ) {
            let path_resolver = PathResolver::from_root(Path::from(ROOT));
            let table_id = SsTableId::from(Ulid::from(compacted_id));
            let path = path_resolver.sst_path(&table_id);
            let parsed_table_id = path_resolver.parse_table_id(&path).unwrap();
            assert_eq!(Some(table_id), parsed_table_id);
        }
    }

    #[test]
    fn test_parse_id() {
        let path_resolver = PathResolver::from_root(Path::from(ROOT));
        let path = Path::from("/root/wal/00000000000000000003.sst");
        let id = path_resolver.parse_wal_file_id(&path).unwrap();
        assert_eq!(id, Some(WalFileId::from(3)));

        let path = Path::from("/root/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let id = path_resolver.parse_table_id(&path).unwrap();
        assert_eq!(
            id,
            Some(SsTableId::from(
                Ulid::from_string("01J79C21YKR31J2BS1EFXJZ7MR").unwrap()
            ))
        );

        let path = Path::from("/root/invalid/00000000000000000001.sst");
        let id = path_resolver.parse_table_id(&path).unwrap();
        assert_eq!(id, None);
    }
}
