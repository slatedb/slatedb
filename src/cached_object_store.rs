use bytes::Bytes;
use bytes::BytesMut;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::GetRange;
use object_store::GetResultPayload;
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

#[derive(Debug, Clone)]
pub(crate) struct CachedObjectStore {
    root_folder: std::path::PathBuf,
    object_store: Arc<dyn ObjectStore>,
    part_size: usize, // expected to be aligned with mb or kb, default 64mb
}

impl CachedObjectStore {
    async fn cached_get_opts(
        &self,
        location: &Path,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        let entry = DiskCacheEntry {
            root_folder: self.root_folder.clone(),
            location: location.clone(),
            part_size: self.part_size,
        };

        let object_size_hint = self.maybe_prefetch_range(&entry, &opts.range).await?;
        let parts = self.split_range_into_parts(opts.range.clone(), object_size_hint);

        // read parts, and concatenate them into a single stream. please note that some of these part may not be cached,
        // we'll still fallback to the object store to get the missing parts.
        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| self.read_part(location, part_id, range_in_part))
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();
        let result_range = self.canonicalize_range(opts.range, object_size_hint);

        Ok(GetResult {
            meta: ObjectMeta {
                size: result_range.len(),
                location: location.clone(),
                last_modified: Default::default(),
                e_tag: None,
                version: None,
            },
            range: result_range,
            attributes: Default::default(),
            payload: GetResultPayload::Stream(result_stream),
        })
    }

    // maybe_prefetch_range will try to prefetch the object from the object store and save the parts
    // into the local disk cache, it returns the object size hint, which is useful to transform `GetRange`
    // into `Range<usize>`.
    // it's ok to not prefetch the object if you've got the object size hint, but prefetching is helpful
    // to reduce the number of GET requests to the object store, it'll try to aggregate the parts into a
    // single GET request, and save the related parts into local disks together.
    // when it sends GET requests to the object store, the range is expected to be ALIGNED with the part
    // size.
    async fn maybe_prefetch_range(
        &self,
        entry: &DiskCacheEntry,
        range: &Option<GetRange>,
    ) -> object_store::Result<usize> {
        let known_object_size = entry.known_object_size().await?;
        let aligned_opts = match &range {
            None => {
                // GET without range, if the object size is unknown, we can prefetch the entire object.
                if let Some(known_object_size) = known_object_size {
                    return Ok(known_object_size);
                }
                GetOptions::default()
            }
            Some(range) => match range {
                GetRange::Bounded(_) => {
                    // GET without bounded range, do not need care about the object size. in practice,
                    // Bounded range is often smaller than the cached part (64mb), so we can simply
                    // not prefetch the object.
                    // the object size hint is also not needed in the case of Bounded range, simply
                    // return 0 ant not use it.
                    return Ok(0);
                }
                GetRange::Suffix(suffix) => {
                    // GET with suffix range, if the object size is unknown, we can not determine the
                    // actual range to prefetch, so we'll fallback to the object store to get the missing
                    // parts.
                    if let Some(known_object_size) = known_object_size {
                        return Ok(known_object_size);
                    }
                    let suffix_aligned = *suffix + self.part_size - *suffix % self.part_size;
                    let opts = GetOptions {
                        range: Some(GetRange::Suffix(suffix_aligned)),
                        ..Default::default()
                    };
                    opts
                }
                GetRange::Offset(offset) => {
                    // GET with offset, same as Suffix, if the object size is unknown, we can not determine
                    // the actual range to prefetch, so fallback to the object store to get the missing
                    // parts.
                    if let Some(known_object_size) = known_object_size {
                        return Ok(known_object_size);
                    }
                    let offset_aligned = *offset - *offset % self.part_size;
                    let opts = GetOptions {
                        range: Some(GetRange::Offset(offset_aligned)),
                        ..Default::default()
                    };
                    opts
                }
            },
        };

        let get_result = self
            .object_store
            .get_opts(&entry.location, aligned_opts)
            .await?;
        entry.save_result(get_result).await
    }

    // given the range and object size, return the canonicalized `Range<usize>`.
    fn canonicalize_range(
        &self,
        range: Option<GetRange>,
        known_object_size: usize,
    ) -> Range<usize> {
        let (start_offset, end_offset) = match range {
            None => (0, known_object_size),
            Some(range) => match range {
                GetRange::Bounded(range) => (range.start, range.end),
                GetRange::Offset(offset) => (offset, known_object_size),
                GetRange::Suffix(suffix) => (known_object_size - suffix, known_object_size),
            },
        };
        Range {
            start: start_offset,
            end: end_offset,
        }
    }

    // given the range and object size, split the range into parts, and return the part id and the range
    // inside the part.
    // the object_size_hint is not needed if the range is bounded, but it's needed in all other cases.
    fn split_range_into_parts(
        &self,
        get_range: Option<GetRange>,
        object_size_hint: usize,
    ) -> Vec<(PartID, Range<usize>)> {
        let range = self.canonicalize_range(get_range, object_size_hint);
        let start_part = range.start / self.part_size;
        let end_part = range.end.div_ceil(self.part_size);
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
        first_part.1.start = range.start % self.part_size;
        let last_part = parts.last_mut().unwrap();
        last_part.1.end = range.end % self.part_size;
        return parts;
    }

    /// get from disk if the parts are cached, otherwise start a new GET request.
    /// the io errors on reading the disk caches will be ignored, just fallback to
    /// the object store.
    /// TODO: add metrics to track the cache hit rate here.
    fn read_part(
        &self,
        location: &Path,
        part_id: PartID,
        range_in_part: Range<usize>,
    ) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let part_size = self.part_size;
        let object_store = self.object_store.clone();
        let root_folder = self.root_folder.clone();
        let location = location.clone();
        Box::pin(async move {
            let entry = DiskCacheEntry {
                root_folder,
                location: location.clone(),
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
            let get_result = object_store
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Bounded(range)),
                        ..Default::default()
                    },
                )
                .await?;

            // save it to the disk cache
            let result_meta = get_result.meta.clone();
            let result_range = get_result.range.clone();
            let result_attributes = get_result.attributes.clone();
            let bytes = get_result.bytes().await?;
            let bytes_cloned = bytes.clone();
            let result_payload =
                GetResultPayload::Stream(stream::once(async move { Ok(bytes_cloned) }).boxed());
            entry
                .save_result(GetResult {
                    payload: result_payload,
                    meta: result_meta,
                    range: result_range,
                    attributes: result_attributes,
                })
                .await?;
            Ok(bytes.slice(range_in_part))
        })
    }
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CachedObjectStore({}, {})",
            self.root_folder.to_str().unwrap_or_default(),
            self.object_store
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.cached_get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.object_store.head(location).await
    }

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
    location: object_store::path::Path,
    part_size: usize,
}

type PartID = usize;

impl DiskCacheEntry {
    /// save the GetResult to the disk cache, a GetResullt may contain multiple parts. please note
    /// that the `range` in the GetResult is expected to be aligned with the part size.
    pub async fn save_result(&self, result: GetResult) -> object_store::Result<usize> {
        assert!(result.range.start % self.part_size == 0);

        let mut buffer = BytesMut::new();
        let mut part_number = result.range.start / self.part_size;
        let object_size = result.meta.size;

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

        // if the last part is not fully filled, save it as the last part. This is useful
        // to determined the end of the object.
        if !buffer.is_empty() {
            self.save_part(part_number, buffer.as_ref()).await?;
            return Ok(object_size);
        }

        // if reached exactly the end of the object file, save an empty part file to indicate
        // the end of the object.
        if part_number * self.part_size == object_size {
            self.save_part(part_number, &[]).await?;
        }
        Ok(object_size)
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

    pub async fn known_object_size(&self) -> object_store::Result<Option<usize>> {
        let pattern = self.make_part_path(0).with_extension("_part*");
        let mut part_paths = glob::glob(&pattern.to_string_lossy())
            .map_err(wrap_io_err)?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // not cached at all
        if part_paths.is_empty() {
            return Ok(None);
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

        // check if we've cached the last part or not. the last part is expected to be not fully filled or zero byte.
        let last_part_path = part_paths.last().unwrap();
        let last_part_size = tokio::fs::metadata(last_part_path)
            .await
            .map(|m| m.len() as usize)
            .map_err(wrap_io_err)?;
        let cached_last_part = last_part_size < self.part_size;

        let object_size = if cached_last_part {
            part_numbers
                .last()
                .copied()
                .map(|last_part_number| (last_part_number - 1) * self.part_size + last_part_size)
        } else {
            None
        };
        Ok(object_size)
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
            .join(self.location.to_string())
            .with_extension(format!("_part{}-{:09}", part_size_name, part_number))
    }
}

fn wrap_io_err(err: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "cached_object_store",
        source: Box::new(err),
    }
}
