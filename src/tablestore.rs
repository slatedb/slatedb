use crate::blob::ReadOnlyBlob;
use crate::block::Block;
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::{ManifestOwned, OwnedSsTableInfo};
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use std::ops::Range;
use std::sync::Arc;

pub struct TableStore {
    object_store: Arc<dyn ObjectStore>,
    sst_format: SsTableFormat,
    wal_path: &'static str,
    manifest_path: &'static str,
}

pub enum SstKind {
    Wal,
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
pub struct SSTableHandle {
    pub id: u64,
    pub info: OwnedSsTableInfo,
    // we stash the filter in the handle for now, as a way to cache it so that
    // the db doesn't need to reload it for each read. Once we've put in a proper
    // cache, we should instead cache the filter block in the cache and get rid
    // of this reference.
    filter: Option<Arc<BloomFilter>>,
}

impl TableStore {
    pub fn new(object_store: Arc<dyn ObjectStore>, sst_format: SsTableFormat) -> TableStore {
        TableStore {
            object_store: object_store.clone(),
            sst_format,
            wal_path: "wal",
            manifest_path: "manifest",
        }
    }

    pub(crate) async fn open_latest_manifest(
        &self,
        root_path: &Path,
    ) -> Option<Result<ManifestOwned, InvalidFlatbuffer>> {
        let manifest_path = &Path::from(format!("{}/{}/", root_path, self.manifest_path));
        let mut files_stream = self.object_store.list(Some(manifest_path));
        let mut manifest_file_path: Option<Path> = None;

        while let Some(file) = files_stream.next().await.transpose().unwrap() {
            if file.location.extension().unwrap_or_default() == self.manifest_path && (manifest_file_path.is_none() || manifest_file_path.as_ref().unwrap() < &file.location) {
                manifest_file_path = Some(file.location.clone());
            }
        }

        if manifest_file_path.is_some() {
            let manifest_bytes = self
                .object_store
                .get(&manifest_file_path.unwrap())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();

            Some(ManifestOwned::new(manifest_bytes.clone()))
        } else {
            None
        }
    }

    pub(crate) async fn get_wal_sst_list(
        &self,
        root_path: &Path,
        manifest: &ManifestOwned,
    ) -> Vec<u64> {
        let mut wal_list: Vec<u64> = Vec::new();
        let wal_path = &Path::from(format!("{}/{}/", root_path, self.wal_path));
        let mut files_stream = self.object_store.list(Some(wal_path));
        let wal_id_last_compacted = manifest.borrow().wal_id_last_compacted();

        while let Some(file) = files_stream.next().await.transpose().unwrap() {
            if file.location.extension().unwrap_or_default() == "sst" {
                let wal_id = self.parse_wal_id(&file.location);
                if wal_id > wal_id_last_compacted {
                    wal_list.push(wal_id);
                }
            }
        }

        wal_list.sort();
        wal_list
    }

    pub(crate) async fn write_manifest(
        &self,
        root_path: &Path,
        manifest: &ManifestOwned,
    ) -> Result<(), SlateDBError> {
        let manifest_path = &Path::from(format!(
            "{}/{}/{:020}.{}",
            root_path,
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
        root_path: &Path,
        sst_kind: SstKind,
        id: u64,
        encoded_sst: EncodedSsTable,
    ) -> Result<SSTableHandle, SlateDBError> {
        let path = self.path(root_path, sst_kind, id);
        self.object_store
            .put(&path, encoded_sst.raw.clone())
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        Ok(SSTableHandle {
            id,
            info: encoded_sst.info,
            filter: encoded_sst.filter,
        })
    }

    // todo: clean up the warning suppression when we start using open_sst outside tests
    #[allow(dead_code)]
    pub(crate) async fn open_sst(
        &self,
        root_path: &Path,
        sst_kind: SstKind,
        id: u64,
    ) -> Result<SSTableHandle, SlateDBError> {
        let path = self.path(root_path, sst_kind, id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let info = self.sst_format.read_info(&obj).await?;
        let filter = self.sst_format.read_filter(&info, &obj).await?;
        Ok(SSTableHandle { id, info, filter })
    }

    pub(crate) async fn read_filter(
        &self,
        handle: &SSTableHandle,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        Ok(handle.filter.clone())
    }

    pub(crate) async fn read_block(
        &self,
        root_path: &Path,
        sst_kind: SstKind,
        handle: &SSTableHandle,
        block: usize,
    ) -> Result<Block, SlateDBError> {
        let path = self.path(root_path, sst_kind, handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        self.sst_format.read_block(&handle.info, block, &obj).await
    }

    fn path(&self, root_path: &Path, sst_kind: SstKind, id: u64) -> Path {
        let sub_path = match sst_kind {
            SstKind::Wal => self.wal_path,
        };
        Path::from(format!("{}/{}/{:020}.sst", root_path, sub_path, id))
    }

    fn parse_wal_id(&self, path: &Path) -> u64 {
        // TODO:- throw a specific SlateDBError if the file name is not in the expected format.
        path.filename()
            .unwrap().split('.')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }
}
