use bytes::Bytes;
use bytes::BytesMut;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryFutureExt;
use object_store::GetRange;
use object_store::GetResultPayload;
use std::io::Read;
use std::io::Write;
use std::ops::Bound;
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
    async fn read_range(
        &self,
        location: &Path,
        range: Option<GetRange>,
    ) -> object_store::Result<BoxStream<'static, object_store::Result<Bytes>>> {
        let entry = DiskCacheEntry {
            root_folder: self.root_path.clone(),
            object_path: location.clone(),
            part_size: self.part_size,
        };

        let parts = match &range {
            None => {
                let (_, known_object_size) = entry.cached_parts(None).await?;
                // if not fully cached, fallback to a single GET request (may helpful to reduce the API cost), and
                // save the result into cached parts.
                let object_size = match known_object_size {
                    Some(object_size) => object_size,
                    None => {
                        let get_result = self.object_store.get(location).await?;
                        let object_size = get_result.meta.size;
                        entry.save_result(get_result).await?;
                        object_size
                    }
                };
                self.split_range_into_parts(None, object_size)
            }
            Some(range) => match range {
                GetRange::Bounded(_) => self.split_range_into_parts(Some(range.clone()), 0),
                GetRange::Suffix(suffix) => {
                    let (_, known_object_size) = entry.cached_parts(None).await?;
                    let object_size = match known_object_size {
                        Some(object_size) => object_size,
                        None => {
                            let suffix_aligned =
                                *suffix + self.part_size - *suffix % self.part_size;
                            let opts = GetOptions {
                                range: Some(GetRange::Suffix(suffix_aligned)),
                                ..Default::default()
                            };
                            let get_result = self.object_store.get_opts(location, opts).await?;
                            let object_size = get_result.meta.size;
                            entry.save_result(get_result).await?;
                            object_size
                        }
                    };
                    self.split_range_into_parts(Some(range.clone()), object_size)
                }
                GetRange::Offset(offset) => {
                    let (_, known_object_size) = entry.cached_parts(None).await?;
                    let object_size = match known_object_size {
                        Some(object_size) => object_size,
                        None => {
                            let offset_aligned = *offset - *offset % self.part_size;
                            let opts = GetOptions {
                                range: Some(GetRange::Offset(offset_aligned)),
                                ..Default::default()
                            };
                            let get_result = self.object_store.get_opts(location, opts).await?;
                            let object_size = get_result.meta.size;
                            entry.save_result(get_result).await?;
                            object_size
                        }
                    };
                    self.split_range_into_parts(Some(range.clone()), object_size)
                }
            },
        };

        // stream by parts, and concatenate them. please note that some of these part may not be cached,
        // we'll still fallback to the object store to get the missing parts.
        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| self.read_part(location, part_id, range_in_part))
            .collect::<Vec<_>>();
        let stream = stream::iter(futures).then(|fut| fut).boxed();
        return Ok(stream);
    }

    // given the range and object size, split the range into parts, and return the part id and the range
    // inside the part.
    fn split_range_into_parts(
        &self,
        range: Option<GetRange>,
        known_object_size: usize,
    ) -> Vec<(PartID, Range<usize>)> {
        let (start_offset, end_offset) = match range {
            None => (0, known_object_size),
            Some(range) => match range {
                GetRange::Bounded(range) => (range.start, range.end),
                GetRange::Offset(offset) => (offset, known_object_size),
                GetRange::Suffix(suffix) => (known_object_size - suffix, known_object_size),
            },
        };
        let start_part = start_offset / self.part_size;
        let end_part = end_offset.div_ceil(self.part_size);
        let mut parts: Vec<_> = (start_part..end_part)
            .into_iter()
            .map(|part_id| {
                (
                    part_id,
                    Range {
                        start: 0,
                        end: self.part_size,
                    },
                )
            })
            .collect();
        if parts.is_empty() {
            return vec![];
        }
        let first_part = parts.first_mut().unwrap();
        first_part.1.start = start_offset % self.part_size;
        let last_part = parts.last_mut().unwrap();
        last_part.1.end = end_offset % self.part_size;
        return parts;
    }

    /// Get from disk if the parts are cached, otherwise start a new GET request.
    fn read_part(
        &self,
        location: &Path,
        part_id: PartID,
        range_in_part: Range<usize>,
    ) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let part_size = self.part_size;
        let object_store = self.object_store.clone();
        let root_folder = self.root_path.clone();
        let location = location.clone();
        Box::pin(async move {
            let entry = DiskCacheEntry {
                root_folder,
                object_path: location.clone(),
                part_size: part_size,
            };
            if let Ok(Some(bytes)) = entry.read_part(part_id).await {
                return Ok(bytes.slice(range_in_part));
            }

            // if the part is not cached, fallback to the object store to get the missing part.
            // the object stores is expected to return the result whenever the `start` of the range
            // is not out of the object size.
            let range = Range {
                start: part_id * part_size,
                end: (part_id + 1) * part_size,
            };
            object_store
                .get_range(&location, range)
                .await
                .map(|result| result.slice(range_in_part))
        })
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
        self.object_store.get_opts(location, options).await
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
    pub async fn save_result(&self, result: GetResult) -> object_store::Result<usize> {
        assert!(result.range.start % self.part_size == 0);

        let mut buffer = BytesMut::new();
        let mut part_number = result.range.start / self.part_size;

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
        Ok(part_number)
    }

    pub async fn read_part(&self, part_id: PartID) -> object_store::Result<Option<Bytes>> {
        let part_path = self.make_part_path(part_id);

        // if the part file does not exist, return None
        let exists = tokio::fs::try_exists(&part_path).await.unwrap_or(false);
        if !exists {
            return Ok(None);
        }

        let file = File::open(&part_path).await.map_err(wrap_io_err)?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.map_err(wrap_io_err)?;
        Ok(Some(Bytes::from(buffer)))
    }

    // return the downloaded parts and an bool about whether we've got the end of the object
    // or not.
    // if we still have not got the final part (which size is less than part_size), we can not determine
    // the Offset and Suffix is cached or not, on these cases, we'd always return empty.
    pub async fn cached_parts(
        &self,
        range: Option<GetRange>,
    ) -> object_store::Result<(Vec<PartID>, Option<usize>)> {
        let pattern = self.make_part_path(0).with_extension("_part*");
        let mut part_paths = glob::glob(&pattern.to_string_lossy())
            .map_err(wrap_io_err)?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // not cached at all
        if part_paths.is_empty() {
            return Ok((vec![], None));
        }

        // sort the paths in alphabetical order
        part_paths.sort();

        // retrieve the part numbers from the paths
        let mut part_numbers = Vec::with_capacity(part_paths.len());
        for part_path in part_paths.iter() {
            let file_ext = match part_path.extension() {
                None => continue,
                Some(ext) => ext.to_string_lossy(),
            };
            let part_number = file_ext
                .rsplit('-')
                .last()
                .and_then(|part_number| part_number.parse::<usize>().ok());
            if let Some(part_number) = part_number {
                part_numbers.push(part_number);
            }
        }

        // check if we've cached the last part or not. it's useful to determine the size of the object.
        // if the last part is not cached, we still need fallback to the object store to get the missing
        // parts on the GET request without range, or with Offset and Suffix.
        let last_part_path = part_paths.last().unwrap();
        let last_part_size = tokio::fs::metadata(last_part_path)
            .await
            .map(|m| m.len() as usize)
            .map_err(wrap_io_err)?;
        let cached_last_part = last_part_size < self.part_size;
        let known_object_size = if cached_last_part {
            part_numbers
                .last()
                .copied()
                .map(|last_part_number| (last_part_number - 1) * self.part_size + last_part_size)
        } else {
            None
        };

        // filter the parts by the range, we can assume the part range are always aligned with the part_size
        // here.
        match range {
            None => {
                return Ok((part_numbers, known_object_size));
            }
            Some(range) => match range {
                GetRange::Bounded(range) => {
                    let start_part = range.start / self.part_size;
                    let end_part = range.end / self.part_size;
                    let filtered_part_numbers = part_numbers
                        .into_iter()
                        .filter(|part_number| *part_number >= start_part && *part_number < end_part)
                        .collect();
                    return Ok((filtered_part_numbers, known_object_size));
                }
                GetRange::Suffix(suffix) => {
                    if !cached_last_part {
                        return Ok((vec![], known_object_size));
                    }
                    let last_part_number = part_numbers.last().copied().unwrap();
                    let suffix_part_start = last_part_number - suffix / self.part_size + 1;
                    let filtered_part_numbers = part_numbers
                        .into_iter()
                        .filter(|part_number| *part_number >= suffix_part_start)
                        .collect();
                    return Ok((filtered_part_numbers, known_object_size));
                }
                GetRange::Offset(offset) => {
                    let offset_part = offset / self.part_size;
                    let filtered_part_numbers = part_numbers
                        .into_iter()
                        .filter(|part_number| *part_number >= offset_part)
                        .collect();
                    return Ok((filtered_part_numbers, known_object_size));
                }
            },
        };
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
        let part_size_name = if self.part_size % (1024 * 1024) == 0 {
            format!("{}mb", self.part_size / (1024 * 1024))
        } else {
            format!("{}kb", self.part_size / 1024)
        };
        self.root_folder
            .join(self.object_path.to_string())
            .with_extension(format!("_part{}-{:09}", part_size_name, part_number))
    }
}

fn wrap_io_err(err: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "cached_object_store",
        source: Box::new(err),
    }
}
