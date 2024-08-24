use bytes::Bytes;
use bytes::BytesMut;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryFutureExt;
use object_store::GetRange;
use object_store::GetResultPayload;
use std::io::Read;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use crate::error::SlateDBError;

#[derive(Debug, Clone)]
pub(crate) struct CachedObjectStore {
    root_path: std::path::PathBuf,
    object_store: Arc<dyn ObjectStore>,
    part_size: usize, // expected to be aligned with mb or kb, default 64mb
}

impl CachedObjectStore {
    fn read_range(
        &self,
        location: &Path,
        range: Option<GetRange>,
    ) -> object_store::Result<BoxStream<'static, object_store::Result<Bytes>>> {
        // split the parts by range
        // parallel calling read_part, concatenate the stream
        // adjust the offsets
        todo!()
    }

    /// Get from disk if the parts are cached, otherwise start a new GET request.
    fn read_part(&self, partID: PartID) -> BoxStream<'static, object_store::Result<Bytes>> {
        todo!()
    }
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CachedObjectStore({}, {})",
            self.root_path.to_str().unwrap_or_default(),
            self.object_store
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.object_store.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.object_store.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.object_store.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.object_store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.rename_if_not_exists(from, to).await
    }
}

struct DiskCacheEntry {
    root_folder: std::path::PathBuf,
    object_path: object_store::path::Path,
    part_size: usize,
}

type PartID = usize;

impl DiskCacheEntry {
    /// Save the GetResult to the disk cache. The `range` is optional and if provided, it's expected to
    /// be aligned with part_size (default 64mb).
    pub async fn save_result(&self, result: GetResult) -> object_store::Result<()> {
        // TODO: assert the range to be aligned with part_size
        let mut buffer = BytesMut::new();
        // TODO: part_number = result.range.start / part_size
        let mut part_number = 0;

        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);

            while buffer.len() >= self.part_size {
                let to_write = buffer.split_to(self.part_size);
                self.save_part(part_number, to_write.as_ref()).await?;
                part_number += 1;
            }
        }

        // the last part, which is less than part_size or empty, should be saved as well
        // which allows us to determine the end of the object data.
        self.save_part(part_number, buffer.as_ref()).await?;
        Ok(())
    }

    pub async fn read_part(
        &self,
        partID: PartID,
    ) -> Option<BoxStream<'static, object_store::Result<Bytes>>> {
        let path = self.object_path.clone();
        let root_path = self.root_folder.clone();

        // TODO: get file part paths
        let part_file_paths = stream::iter(vec!["".to_string()]);

        let stream = part_file_paths
            .then(move |part_file_path| async move {
                let file = File::open(&part_file_path).await.unwrap();
                let mut reader = tokio::io::BufReader::new(file);
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                Ok(Bytes::from(buffer))
            })
            .boxed();
        Some(stream)
    }

    // return the downloaded parts and a boolean indicating if the parts in the range are all cached.
    // if we still have not got the final part (which size is less than part_size), we can not determine
    // the Offset and Suffix is cached or not, on these cases, we'd always return None.
    pub async fn cached_parts(&self, range: GetRange) -> Option<(Vec<PartID>, bool)> {
        todo!()
    }

    async fn get_part_file_paths(&self, range: Option<Range<usize>>) -> Vec<std::path::PathBuf> {
        todo!()
    }

    // if the disk is full, we'll get an error here.
    // TODO: this error can be ignored in the caller side and print a warning.
    async fn save_part(&self, part_number: usize, buf: &[u8]) -> object_store::Result<()> {
        let part_file_path = self.make_part_path(part_number);

        // ensure the parent folder exists
        if let Some(part_file_folder) = part_file_path.parent() {
            fs::create_dir_all(part_file_folder)
                .await
                .map_err(wrap_io_err)?;
        }

        // save the file content to tmp. if the disk is full, we'll get an error here.
        let tmp_part_file_path = part_file_path.with_extension("tmp");
        let mut file = File::create(&tmp_part_file_path)
            .await
            .map_err(wrap_io_err)?;
        file.write_all(&buf).await.map_err(wrap_io_err)?;

        // atomic rename
        fs::rename(tmp_part_file_path, part_file_path)
            .await
            .map_err(wrap_io_err)?;
        Ok(())
    }

    fn make_part_path(&self, part_number: usize) -> std::path::PathBuf {
        // containing the part size in the file name, allows user change the part size on
        // the fly, without the need to invalidate the cache.
        // TODO: pad with zeros
        let part_size_name = if self.part_size % (1024 * 1024) == 0 {
            format!("{}mb", self.part_size / (1024 * 1024))
        } else {
            format!("{}kb", self.part_size / 1024)
        };
        let part_file_path = self.root_folder.join(&format!(
            "{}._part{}-{}",
            self.object_path.to_string(),
            part_size_name,
            part_number,
        ));
        part_file_path
    }
}

fn wrap_io_err(err: tokio::io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "cached_object_store",
        source: Box::new(err),
    }
}
