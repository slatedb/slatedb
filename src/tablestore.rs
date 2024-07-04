use crate::blob::ReadOnlyBlob;
use crate::block::Block;
use crate::error::SlateDBError;
use crate::fail_point;
use crate::failpoints::FailPointRegistry;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::{ManifestV1Owned, SsTableInfoOwned};
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use std::ops::Range;
use std::sync::Arc;
use ulid::Ulid;

pub struct TableStore {
    object_store: Arc<dyn ObjectStore>,
    sst_format: SsTableFormat,
    root_path: Path,
    wal_path: &'static str,
    compacted_path: &'static str,
    manifest_path: &'static str,
    fp_registry: Arc<FailPointRegistry>,
}

struct ReadOnlyObject {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ReadOnlyBlob for ReadOnlyObject {
    async fn len(&self) -> Result<usize, SlateDBError> {
        let object_metadata = self
            .object_store
            .head(&self.path)
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        Ok(object_metadata.size)
    }

    async fn read_range(&self, range: Range<usize>) -> Result<Bytes, SlateDBError> {
        self.object_store
            .get_range(&self.path, range)
            .await
            .map_err(SlateDBError::ObjectStoreError)
    }

    async fn read(&self) -> Result<Bytes, SlateDBError> {
        let file = self.object_store.get(&self.path).await?;
        file.bytes().await.map_err(SlateDBError::ObjectStoreError)
    }
}

#[derive(Clone)]
pub enum SsTableId {
    Wal(u64),
    Compacted(Ulid),
}

#[derive(Clone)]
pub struct SSTableHandle {
    pub id: SsTableId,
    pub info: SsTableInfoOwned,
    // we stash the filter in the handle for now, as a way to cache it so that
    // the db doesn't need to reload it for each read. Once we've put in a proper
    // cache, we should instead cache the filter block in the cache and get rid
    // of this reference.
    filter: Option<Arc<BloomFilter>>,
}

impl TableStore {
    #[allow(dead_code)]
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: Path,
    ) -> Self {
        Self::new_with_fp_registry(
            object_store,
            sst_format,
            root_path,
            Arc::new(FailPointRegistry::new()),
        )
    }

    pub fn new_with_fp_registry(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: Path,
        fp_registry: Arc<FailPointRegistry>,
    ) -> Self {
        Self {
            object_store: object_store.clone(),
            sst_format,
            root_path,
            wal_path: "wal",
            compacted_path: "compacted",
            manifest_path: "manifest",
            fp_registry,
        }
    }

    pub(crate) async fn open_latest_manifest(
        &self,
    ) -> Result<Option<ManifestV1Owned>, SlateDBError> {
        let manifest_path = &Path::from(format!("{}/{}/", &self.root_path, self.manifest_path));
        let mut files_stream = self.object_store.list(Some(manifest_path));
        let mut manifest_file_path: Option<Path> = None;

        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
        } {
            match self.parse_id(&file.location, "manifest") {
                Ok(_) => {
                    manifest_file_path = match manifest_file_path {
                        Some(path) => Some(if path < file.location {
                            file.location
                        } else {
                            path
                        }),
                        None => Some(file.location.clone()),
                    }
                }
                Err(_) => continue,
            }
        }

        if let Some(resolved_manifest_file_path) = manifest_file_path {
            let manifest_bytes = match self.object_store.get(&resolved_manifest_file_path).await {
                Ok(manifest) => match manifest.bytes().await {
                    Ok(bytes) => bytes,
                    Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
                },
                Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
            };

            ManifestV1Owned::new(manifest_bytes.clone())
                .map(Some)
                .map_err(SlateDBError::InvalidFlatbuffer)
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn get_wal_sst_list(
        &self,
        wal_id_last_compacted: u64,
    ) -> Result<Vec<u64>, SlateDBError> {
        let mut wal_list: Vec<u64> = Vec::new();
        let wal_path = &Path::from(format!("{}/{}/", &self.root_path, self.wal_path));
        let mut files_stream = self.object_store.list(Some(wal_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match self.parse_id(&file.location, "sst") {
                Ok(wal_id) => {
                    if wal_id > wal_id_last_compacted {
                        wal_list.push(wal_id);
                    }
                }
                Err(_) => continue,
            }
        }

        wal_list.sort();
        Ok(wal_list)
    }

    pub(crate) async fn write_manifest(
        &self,
        manifest: &ManifestV1Owned,
    ) -> Result<(), SlateDBError> {
        let manifest_path = &Path::from(format!(
            "{}/{}/{:020}.{}",
            &self.root_path,
            self.manifest_path,
            manifest.borrow().manifest_id(),
            self.manifest_path
        ));

        self.object_store
            .put(manifest_path, Bytes::copy_from_slice(manifest.data()))
            .await
            .map_err(SlateDBError::ObjectStoreError)?;

        Ok(())
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        self.sst_format.table_builder()
    }

    pub(crate) async fn write_sst(
        &self,
        id: &SsTableId,
        encoded_sst: EncodedSsTable,
    ) -> Result<SSTableHandle, SlateDBError> {
        fail_point!(
            self.fp_registry.clone(),
            "write-wal-sst-io-error",
            matches!(id, SsTableId::Wal(_)),
            |_| Result::Err(SlateDBError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "oops"
            )))
        );
        fail_point!(
            self.fp_registry.clone(),
            "write-compacted-sst-io-error",
            matches!(id, SsTableId::Compacted(_)),
            |_| Result::Err(SlateDBError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "oops"
            )))
        );

        let path = self.path(id);
        self.object_store
            .put(&path, encoded_sst.raw.clone())
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        Ok(SSTableHandle {
            id: id.clone(),
            info: encoded_sst.info,
            filter: encoded_sst.filter,
        })
    }

    // todo: clean up the warning suppression when we start using open_sst outside tests
    #[allow(dead_code)]
    pub(crate) async fn open_sst(&self, id: &SsTableId) -> Result<SSTableHandle, SlateDBError> {
        let path = self.path(id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let info = self.sst_format.read_info(&obj).await?;
        let filter = self.sst_format.read_filter(&info, &obj).await?;
        Ok(SSTableHandle {
            id: id.clone(),
            info,
            filter,
        })
    }

    pub(crate) async fn read_filter(
        &self,
        handle: &SSTableHandle,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        Ok(handle.filter.clone())
    }

    pub(crate) async fn read_block(
        &self,
        handle: &SSTableHandle,
        block: usize,
    ) -> Result<Block, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        self.sst_format.read_block(&handle.info, block, &obj).await
    }

    fn path(&self, id: &SsTableId) -> Path {
        match id {
            SsTableId::Wal(id) => Path::from(format!(
                "{}/{}/{:020}.sst",
                &self.root_path, self.wal_path, id
            )),
            SsTableId::Compacted(ulid) => Path::from(format!(
                "{}/{}/{}.sst",
                &self.root_path,
                self.compacted_path,
                ulid.to_string()
            )),
        }
    }

    fn parse_id(&self, path: &Path, expected_extension: &str) -> Result<u64, SlateDBError> {
        match path.extension() {
            Some(ext) if ext == expected_extension => path
                .filename()
                .expect("invalid wal file")
                .split('.')
                .next()
                .ok_or_else(|| SlateDBError::InvalidDBState)?
                .parse()
                .map_err(|_| SlateDBError::InvalidDBState),
            _ => Err(SlateDBError::InvalidDBState),
        }
    }
}
