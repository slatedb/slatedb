use crate::blob::ReadOnlyBlob;
use crate::block::Block;
use crate::db_state::{SSTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use bytes::{BufMut, Bytes};
use fail_parallel::{fail_point, FailPointRegistry};
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub struct TableStore {
    object_store: Arc<dyn ObjectStore>,
    sst_format: SsTableFormat,
    root_path: Path,
    wal_path: &'static str,
    compacted_path: &'static str,
    fp_registry: Arc<FailPointRegistry>,
    // TODO: we cache the filters here for now, so the db doesn't need to reload them
    //       for each read. This means that all the filters need to fit in memory.
    //       Once we've put in a proper cache, we should instead cache the filter block in
    //       the cache and get rid of this.
    //       https://github.com/slatedb/slatedb/issues/89
    filter_cache: RwLock<HashMap<SsTableId, Option<Arc<BloomFilter>>>>,
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
            fp_registry,
            filter_cache: RwLock::new(HashMap::new()),
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

    pub(crate) fn table_writer(&self, id: SsTableId) -> EncodedSsTableWriter {
        let path = self.path(&id);
        EncodedSsTableWriter {
            id,
            builder: self.sst_format.table_builder(),
            writer: BufWriter::new(self.object_store.clone(), path),
            table_store: self,
            blocks_written: 0,
        }
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
        let total_size = encoded_sst
            .unconsumed_blocks
            .iter()
            .map(|chunk| chunk.len())
            .sum();
        let mut data = Vec::<u8>::with_capacity(total_size);
        for chunk in encoded_sst.unconsumed_blocks {
            data.put_slice(chunk.as_ref())
        }
        self.object_store
            .put(&path, PutPayload::from(data))
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        self.cache_filter(id.clone(), encoded_sst.filter);
        Ok(SSTableHandle {
            id: id.clone(),
            info: encoded_sst.info,
        })
    }

    fn cache_filter(&self, sst: SsTableId, filter: Option<Arc<BloomFilter>>) {
        {
            let mut wguard = self.filter_cache.write();
            wguard.insert(sst, filter);
        }
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
        Ok(SSTableHandle {
            id: id.clone(),
            info,
        })
    }

    pub(crate) async fn read_filter(
        &self,
        handle: &SSTableHandle,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        {
            let rguard = self.filter_cache.read();
            if let Some(filter) = rguard.get(&handle.id) {
                return Ok(filter.clone());
            }
        }
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let filter = self.sst_format.read_filter(&handle.info, &obj).await?;
        let mut wguard = self.filter_cache.write();
        wguard.insert(handle.id.clone(), filter.clone());
        Ok(filter)
    }

    pub(crate) async fn read_index(
        &self,
        handle: &SSTableHandle,
    ) -> Result<SsTableIndexOwned, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        self.sst_format.read_index(&handle.info, &obj).await
    }

    #[allow(dead_code)]
    pub(crate) async fn read_blocks(
        &self,
        handle: &SSTableHandle,
        blocks: Range<usize>,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let index = self.sst_format.read_index(&handle.info, &obj).await?;
        self.sst_format
            .read_blocks(&handle.info, &index, blocks, &obj)
            .await
    }

    // TODO: we probably won't need this once we're caching the index
    pub(crate) async fn read_blocks_using_index(
        &self,
        handle: &SSTableHandle,
        index: Arc<SsTableIndexOwned>,
        blocks: Range<usize>,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        self.sst_format
            .read_blocks(&handle.info, index.as_ref(), blocks, &obj)
            .await
    }

    #[allow(dead_code)]
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
        let index = self.sst_format.read_index(&handle.info, &obj).await?;
        self.sst_format
            .read_block(&handle.info, &index, block, &obj)
            .await
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

pub(crate) struct EncodedSsTableWriter<'a> {
    id: SsTableId,
    builder: EncodedSsTableBuilder<'a>,
    writer: BufWriter,
    table_store: &'a TableStore,
    blocks_written: usize,
}

impl<'a> EncodedSsTableWriter<'a> {
    pub async fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), SlateDBError> {
        self.builder.add(key, value)?;
        self.drain_blocks().await
    }

    pub async fn close(mut self) -> Result<SSTableHandle, SlateDBError> {
        let mut encoded_sst = self.builder.build()?;
        while let Some(block) = encoded_sst.unconsumed_blocks.pop_front() {
            self.writer.write_all(block.as_ref()).await?;
        }
        self.writer.shutdown().await?;
        self.table_store
            .cache_filter(self.id.clone(), encoded_sst.filter);
        Ok(SSTableHandle {
            id: self.id.clone(),
            info: encoded_sst.info,
        })
    }

    async fn drain_blocks(&mut self) -> Result<(), SlateDBError> {
        while let Some(block) = self.builder.next_block() {
            self.writer.write_all(block.as_ref()).await?;
            self.blocks_written += 1;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn blocks_written(&self) -> usize {
        self.blocks_written
    }
}

#[cfg(test)]
mod tests {
    use crate::db_state::SsTableId;
    use crate::sst::SsTableFormat;
    use crate::sst_iter::SstIterator;
    use crate::tablestore::TableStore;
    use crate::test_utils::assert_iterator;
    use crate::types::ValueDeletable;
    use bytes::Bytes;
    use object_store::path::Path;
    use std::sync::Arc;
    use ulid::Ulid;

    const ROOT: &str = "/root";

    #[tokio::test]
    async fn test_sst_writer_should_write_sst() {
        // given:
        let os = Arc::new(object_store::memory::InMemory::new());
        let format = SsTableFormat::new(32, 1, None);
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT)));
        let id = SsTableId::Compacted(Ulid::new());

        // when:
        let mut writer = ts.table_writer(id);
        writer.add(&[b'a'; 16], Some(&[1u8; 16])).await.unwrap();
        writer.add(&[b'b'; 16], Some(&[2u8; 16])).await.unwrap();
        writer.add(&[b'c'; 16], None).await.unwrap();
        writer.add(&[b'd'; 16], Some(&[4u8; 16])).await.unwrap();
        let sst = writer.close().await.unwrap();

        // then:
        let mut iter = SstIterator::new(&sst, ts.clone(), 1, 1).await.unwrap();
        assert_iterator(
            &mut iter,
            &[
                (
                    &[b'a'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 16])),
                ),
                (
                    &[b'b'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 16])),
                ),
                (&[b'c'; 16], ValueDeletable::Tombstone),
                (
                    &[b'd'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[4u8; 16])),
                ),
            ],
        )
        .await;
    }
}
