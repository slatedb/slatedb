use crate::db_state::SsTableId;
use crate::db_state::SsTableId::{Compacted, Wal};
use crate::error::SlateDBError;
use object_store::path::Path;
use ulid::Ulid;

pub(crate) struct PathBuilder {
    root_path: Path,
    wal_path: &'static str,
    compacted_path: &'static str,
}

impl PathBuilder {
    pub(crate) fn new<P: Into<Path>>(root_path: P) -> Self {
        Self {
            root_path: root_path.into(),
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

    pub(crate) fn table_path(&self, table_id: &SsTableId) -> Path {
        match table_id {
            Wal(id) => Path::from(format!(
                "{}/{}/{:020}.sst",
                &self.root_path, self.wal_path, id
            )),
            Compacted(ulid) => Path::from(format!(
                "{}/{}/{}.sst",
                &self.root_path,
                self.compacted_path,
                ulid.to_string()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db_state::SsTableId;
    use crate::paths::PathBuilder;
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
            let path_builder = PathBuilder::new(Path::from(ROOT));
            let table_id = SsTableId::Wal(wal_id);
            let path = path_builder.table_path(&table_id);
            let parsed_table_id = path_builder.parse_table_id(&path).unwrap();
            assert_eq!(Some(table_id), parsed_table_id);
        }

        #[test]
        fn should_serialize_and_deserialize_compacted_paths(
            compacted_id in any::<u128>(),
        ) {
            let path_builder = PathBuilder::new(Path::from(ROOT));
            let table_id = SsTableId::Compacted(Ulid::from(compacted_id));
            let path = path_builder.table_path(&table_id);
            let parsed_table_id = path_builder.parse_table_id(&path).unwrap();
            assert_eq!(Some(table_id), parsed_table_id);
        }
    }

    #[test]
    fn test_parse_id() {
        let path_builder = PathBuilder::new(Path::from(ROOT));
        let path = Path::from("/root/wal/00000000000000000003.sst");
        let id = path_builder.parse_table_id(&path).unwrap();
        assert_eq!(id, Some(SsTableId::Wal(3)));

        let path = Path::from("/root/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let id = path_builder.parse_table_id(&path).unwrap();
        assert_eq!(
            id,
            Some(SsTableId::Compacted(
                Ulid::from_string("01J79C21YKR31J2BS1EFXJZ7MR").unwrap()
            ))
        );

        let path = Path::from("/root/invalid/00000000000000000001.sst");
        let id = path_builder.parse_table_id(&path).unwrap();
        assert_eq!(id, None);
    }
}
