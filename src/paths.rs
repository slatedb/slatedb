use crate::db_state::SsTableId;
use crate::db_state::SsTableId::{Compacted, Wal};
use crate::error::SlateDBError;
use object_store::path::Path;
use ulid::Ulid;

#[derive(Clone)]
pub(crate) struct PathResolver {
    root_path: Path,
    fallback_paths: Vec<Path>,
    wal_path: &'static str,
    compacted_path: &'static str,
}

impl PathResolver {
    pub(crate) fn new<P: Into<Path>>(root_path: P) -> Self {
        Self {
            root_path: root_path.into(),
            fallback_paths: Vec::new(),
            wal_path: "wal",
            compacted_path: "compacted",
        }
    }

    pub(crate) fn new_with_fallback<P: Into<Path>>(root_path: P, fallback_paths: Vec<P>) -> Self {
        Self {
            root_path: root_path.into(),
            fallback_paths: fallback_paths.into_iter().map(|p| p.into()).collect(),
            wal_path: "wal",
            compacted_path: "compacted",
        }
    }

    pub(crate) fn wal_path(&self) -> Path {
        Path::from(format!("{}/{}/", &self.root_path, self.wal_path))
    }

    pub(crate) fn compacted_path(&self) -> Path {
        Path::from(format!("{}/{}/", &self.root_path, self.compacted_path))
    }

    pub(crate) fn parse_table_id(&self, path: &Path) -> Result<Option<SsTableId>, SlateDBError> {
        if let Some(mut suffix_iter) = path.prefix_match(&self.root_path) {
            match suffix_iter.next() {
                Some(a) if a.as_ref() == self.wal_path => suffix_iter
                    .next()
                    .and_then(|s| s.as_ref().split('.').next().map(|s| s.parse::<u64>()))
                    .transpose()
                    .map(|r| r.map(SsTableId::Wal))
                    .map_err(|_| SlateDBError::InvalidDBState),
                Some(a) if a.as_ref() == self.compacted_path => suffix_iter
                    .next()
                    .and_then(|s| s.as_ref().split('.').next().map(Ulid::from_string))
                    .transpose()
                    .map(|r| r.map(SsTableId::Compacted))
                    .map_err(|_| SlateDBError::InvalidDBState),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn table_path_with_root(&self, table_id: &SsTableId, root_path: &Path) -> Path {
        match table_id {
            Wal(id) => Path::from(format!("{}/{}/{:020}.sst", root_path, self.wal_path, id)),
            Compacted(ulid) => Path::from(format!(
                "{}/{}/{}.sst",
                root_path,
                self.compacted_path,
                ulid.to_string()
            )),
        }
    }

    pub(crate) fn table_path(&self, table_id: &SsTableId) -> Path {
        self.table_path_with_root(table_id, &self.root_path)
    }

    pub(crate) fn table_path_including_fallbacks<'a>(
        &'a self,
        table_id: &'a SsTableId,
    ) -> impl Iterator<Item = Path> + 'a {
        let primary_path = self.table_path(table_id);
        std::iter::once(primary_path).chain(
            self.fallback_paths
                .iter()
                .map(move |fallback_root| self.table_path_with_root(table_id, fallback_root)),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::db_state::SsTableId;
    use crate::paths::PathResolver;
    use object_store::path::Path;
    use proptest::arbitrary::any;
    use proptest::proptest;
    use ulid::Ulid;

    const ROOT: &str = "/root";

    proptest! {
        #[test]
        fn should_serialize_and_deserialize_wal_paths(
            wal_id in any::<u64>(),
        ) {
            let path_resolver = PathResolver::new(Path::from(ROOT));
            let table_id = SsTableId::Wal(wal_id);
            let path = path_resolver.table_path(&table_id);
            let parsed_table_id = path_resolver.parse_table_id(&path).unwrap();
            assert_eq!(Some(table_id), parsed_table_id);
        }

        #[test]
        fn should_serialize_and_deserialize_compacted_paths(
            compacted_id in any::<u128>(),
        ) {
            let path_resolver = PathResolver::new(Path::from(ROOT));
            let table_id = SsTableId::Compacted(Ulid::from(compacted_id));
            let path = path_resolver.table_path(&table_id);
            let parsed_table_id = path_resolver.parse_table_id(&path).unwrap();
            assert_eq!(Some(table_id), parsed_table_id);
        }
    }

    #[test]
    fn test_parse_id() {
        let path_resolver = PathResolver::new(Path::from(ROOT));
        let path = Path::from("/root/wal/00000000000000000003.sst");
        let id = path_resolver.parse_table_id(&path).unwrap();
        assert_eq!(id, Some(SsTableId::Wal(3)));

        let path = Path::from("/root/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let id = path_resolver.parse_table_id(&path).unwrap();
        assert_eq!(
            id,
            Some(SsTableId::Compacted(
                Ulid::from_string("01J79C21YKR31J2BS1EFXJZ7MR").unwrap()
            ))
        );

        let path = Path::from("/root/invalid/00000000000000000001.sst");
        let id = path_resolver.parse_table_id(&path).unwrap();
        assert_eq!(id, None);
    }
}
