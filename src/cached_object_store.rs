use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use std::{collections::HashMap, fmt::Display, ops::Range, sync::Arc};

use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, stream, stream::BoxStream, StreamExt};
use object_store::{path::Path, GetOptions, GetResult, ObjectMeta, ObjectStore};
use object_store::{Attribute, Attributes, GetRange, GetResultPayload, PutResult};
use object_store::{ListResult, MultipartUpload, PutMultipartOpts, PutOptions, PutPayload};
use radix_trie::{Trie, TrieCommon};
use rand::seq::IteratorRandom;
use rand::SeedableRng;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncSeekExt;
use tokio::sync::{Mutex, OnceCell};
use tokio::{
    fs,
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::{debug, warn};
use walkdir::WalkDir;

use crate::error::SlateDBError;
use crate::metrics::DbStats;

#[derive(Debug, Clone)]
pub(crate) struct CachedObjectStore {
    object_store: Arc<dyn ObjectStore>,
    pub(crate) part_size_bytes: usize, // expected to be aligned with mb or kb
    pub(crate) cache_storage: Arc<dyn LocalCacheStorage>,
    db_stats: Arc<DbStats>,
}

impl CachedObjectStore {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        root_folder: std::path::PathBuf,
        max_cache_size_bytes: Option<usize>,
        part_size_bytes: usize,
        db_stats: Arc<DbStats>,
    ) -> Result<Arc<Self>, SlateDBError> {
        if part_size_bytes == 0 || part_size_bytes % 1024 != 0 {
            return Err(SlateDBError::InvalidCachePartSize);
        }

        let cache_storage = Arc::new(FsCacheStorage::new(
            root_folder.clone(),
            max_cache_size_bytes,
            db_stats.clone(),
        ));
        Ok(Arc::new(Self {
            object_store,
            part_size_bytes,
            cache_storage,
            db_stats,
        }))
    }

    pub async fn start_evictor(&self) {
        self.cache_storage.start_evictor().await;
    }

    pub async fn cached_head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let entry = self.cache_storage.entry(location, self.part_size_bytes);
        match entry.read_head().await {
            Ok(Some((meta, _))) => Ok(meta),
            _ => {
                let result = self
                    .object_store
                    .get_opts(
                        location,
                        GetOptions {
                            range: None,
                            head: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                let meta = result.meta.clone();
                self.save_result(result).await.ok();
                Ok(meta)
            }
        }
    }

    pub async fn cached_get_opts(
        &self,
        location: &Path,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        let get_range = opts.range.clone();
        let (meta, attributes) = self.maybe_prefetch_range(location, opts).await?;
        let range = self.canonicalize_range(get_range, meta.size)?;
        let parts = self.split_range_into_parts(range.clone());

        // read parts, and concatenate them into a single stream. please note that some of these part may not be cached,
        // we'll still fallback to the object store to get the missing parts.
        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| self.read_part(location, part_id, range_in_part))
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();

        Ok(GetResult {
            meta,
            range,
            attributes,
            payload: GetResultPayload::Stream(result_stream),
        })
    }

    // TODO: implement the put with cache here
    #[allow(unused)]
    async fn cached_put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<PutResult> {
        self.object_store.put_opts(location, payload, opts).await
    }

    // if an object is not cached before, maybe_prefetch_range will try to prefetch the object from the
    // object store and save the parts into the local disk cache. the prefetching is helpful to reduce the
    // number of GET requests to the object store, it'll try to aggregate the parts among the range into a
    // single GET request, and save the related parts into local disks together.
    // when it sends GET requests to the object store, the range is expected to be ALIGNED with the part
    // size.
    async fn maybe_prefetch_range(
        &self,
        location: &Path,
        mut opts: GetOptions,
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        let entry = self.cache_storage.entry(location, self.part_size_bytes);
        match entry.read_head().await {
            Ok(Some((meta, attrs))) => return Ok((meta, attrs)),
            Ok(None) => {}
            Err(_) => {
                // TODO: add a warning log
            }
        };

        // it's strange that GetOptions did not derive Clone. maybe we could add a derive(Clone) to object_store.
        if let Some(range) = &opts.range {
            opts.range = Some(self.align_get_range(range));
        }

        let get_result = self.object_store.get_opts(location, opts).await?;
        let result_meta = get_result.meta.clone();
        let result_attrs = get_result.attributes.clone();
        // swallow the error on saving to disk here (the disk might be already full), just fallback
        // to the object store.
        // TODO: add a warning log here.
        self.save_result(get_result).await.ok();
        Ok((result_meta, result_attrs))
    }

    /// save the GetResult to the disk cache, a GetResult may be transformed into multiple part
    /// files and a meta file. please note that the `range` in the GetResult is expected to be
    /// aligned with the part size.
    async fn save_result(&self, result: GetResult) -> object_store::Result<usize> {
        assert!(result.range.start % self.part_size_bytes == 0);
        assert!(
            result.range.end % self.part_size_bytes == 0 || result.range.end == result.meta.size
        );

        let entry = self
            .cache_storage
            .entry(&result.meta.location, self.part_size_bytes);
        entry.save_head((&result.meta, &result.attributes)).await?;

        let mut buffer = BytesMut::new();
        let mut part_number = result.range.start / self.part_size_bytes;
        let object_size = result.meta.size;

        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);

            while buffer.len() >= self.part_size_bytes {
                let to_write = buffer.split_to(self.part_size_bytes);
                entry.save_part(part_number, to_write.into()).await?;
                part_number += 1;
            }
        }

        // if the last part is not fully filled, save it as the last part.
        if !buffer.is_empty() {
            entry.save_part(part_number, buffer.into()).await?;
            return Ok(object_size);
        }

        Ok(object_size)
    }

    // split the range into parts, and return the part id and the range inside the part.
    fn split_range_into_parts(&self, range: Range<usize>) -> Vec<(PartID, Range<usize>)> {
        let range_aligned = self.align_range(&range, self.part_size_bytes);
        let start_part = range_aligned.start / self.part_size_bytes;
        let end_part = range_aligned.end / self.part_size_bytes;
        let mut parts: Vec<_> = (start_part..end_part)
            .map(|part_id| {
                (
                    part_id,
                    Range {
                        start: 0,
                        end: self.part_size_bytes,
                    },
                )
            })
            .collect();
        if parts.is_empty() {
            return vec![];
        }
        if let Some(first_part) = parts.first_mut() {
            first_part.1.start = range.start % self.part_size_bytes;
        }
        if let Some(last_part) = parts.last_mut() {
            if range.end % self.part_size_bytes != 0 {
                last_part.1.end = range.end % self.part_size_bytes;
            }
        }
        parts
    }

    /// get from disk if the parts are cached, otherwise start a new GET request.
    /// the io errors on reading the disk caches will be ignored, just fallback to
    /// the object store.
    fn read_part(
        &self,
        location: &Path,
        part_id: PartID,
        range_in_part: Range<usize>,
    ) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let part_size = self.part_size_bytes;
        let object_store = self.object_store.clone();
        let location = location.clone();
        let entry = self.cache_storage.entry(&location, self.part_size_bytes);
        let db_stats = self.db_stats.clone();
        Box::pin(async move {
            db_stats.object_store_cache_part_access.inc();

            if let Ok(Some(bytes)) = entry.read_part(part_id, range_in_part.clone()).await {
                db_stats.object_store_cache_part_hits.inc();
                return Ok(bytes);
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

            // save it to the disk cache, we'll ignore the error on writing to disk here, just return
            // the bytes from the object store.
            let meta = get_result.meta.clone();
            let attrs = get_result.attributes.clone();
            let bytes = get_result.bytes().await?;
            entry.save_head((&meta, &attrs)).await.ok();
            entry.save_part(part_id, bytes.clone()).await.ok();
            Ok(Bytes::copy_from_slice(&bytes.slice(range_in_part)))
        })
    }

    // given the range and object size, return the canonicalized `Range<usize>` with concrete start and
    // end.
    fn canonicalize_range(
        &self,
        range: Option<GetRange>,
        object_size: usize,
    ) -> object_store::Result<Range<usize>> {
        let (start_offset, end_offset) = match range {
            None => (0, object_size),
            Some(range) => match range {
                GetRange::Bounded(range) => {
                    if range.start >= object_size {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::StartTooLarge {
                                requested: range.start,
                                length: object_size,
                            }),
                        });
                    }
                    if range.start >= range.end {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::Inconsistent {
                                start: range.start,
                                end: range.end,
                            }),
                        });
                    }
                    (range.start, range.end.min(object_size))
                }
                GetRange::Offset(offset) => {
                    if offset >= object_size {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::StartTooLarge {
                                requested: offset,
                                length: object_size,
                            }),
                        });
                    }
                    (offset, object_size)
                }
                GetRange::Suffix(suffix) => (object_size.saturating_sub(suffix), object_size),
            },
        };
        Ok(Range {
            start: start_offset,
            end: end_offset,
        })
    }

    fn align_get_range(&self, range: &GetRange) -> GetRange {
        match range {
            GetRange::Bounded(bounded) => {
                let aligned = self.align_range(bounded, self.part_size_bytes);
                GetRange::Bounded(aligned)
            }
            GetRange::Suffix(suffix) => {
                let suffix_aligned = self.align_range(&(0..*suffix), self.part_size_bytes).end;
                GetRange::Suffix(suffix_aligned)
            }
            GetRange::Offset(offset) => {
                let offset_aligned = *offset - *offset % self.part_size_bytes;
                GetRange::Offset(offset_aligned)
            }
        }
    }

    fn align_range(&self, range: &Range<usize>, alignment: usize) -> Range<usize> {
        let start_aligned = range.start - range.start % alignment;
        let end_aligned = ((range.end + alignment - 1) / alignment) * alignment;
        Range {
            start: start_aligned,
            end: end_aligned,
        }
    }
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CachedObjectStore({}, {})",
            self.object_store, self.cache_storage
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
        self.cached_head(location).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // TODO: update the cache on put
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
        // TODO: handle cache eviction
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalCacheHead {
    pub location: String,
    pub last_modified: String,
    pub size: usize,
    pub e_tag: Option<String>,
    pub version: Option<String>,
    pub attributes: HashMap<String, String>,
}

impl LocalCacheHead {
    pub fn meta(&self) -> ObjectMeta {
        ObjectMeta {
            location: self.location.clone().into(),
            last_modified: self.last_modified.parse().unwrap_or_default(),
            size: self.size,
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        }
    }

    pub fn attributes(&self) -> Attributes {
        let mut attrs = Attributes::new();
        for (key, value) in self.attributes.iter() {
            let key = match key.as_str() {
                "Cache-Control" => Attribute::CacheControl,
                "Content-Disposition" => Attribute::ContentDisposition,
                "Content-Encoding" => Attribute::ContentEncoding,
                "Content-Language" => Attribute::ContentLanguage,
                "Content-Type" => Attribute::ContentType,
                _ => Attribute::Metadata(key.to_string().into()),
            };
            let value = value.to_string().into();
            attrs.insert(key, value);
        }
        attrs
    }
}

impl From<(&ObjectMeta, &Attributes)> for LocalCacheHead {
    fn from((meta, attrs): (&ObjectMeta, &Attributes)) -> Self {
        let mut attrs_map = HashMap::new();
        for (key, value) in attrs.iter() {
            let key = match key {
                Attribute::CacheControl => "Cache-Control",
                Attribute::ContentDisposition => "Content-Disposition",
                Attribute::ContentEncoding => "Content-Encoding",
                Attribute::ContentLanguage => "Content-Language",
                Attribute::ContentType => "Content-Type",
                Attribute::Metadata(key) => key,
                _ => continue,
            };
            attrs_map.insert(key.to_string(), value.to_string());
        }
        LocalCacheHead {
            location: meta.location.to_string(),
            last_modified: meta.last_modified.to_rfc3339(),
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
            attributes: attrs_map,
        }
    }
}

// it seems that object_store did not expose this error type, duplicate it here.
// TODO: raise a pr to expose this error type in object_store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidGetRange {
    #[error("Range start too large, requested: {requested}, length: {length}")]
    StartTooLarge { requested: usize, length: usize },

    #[error("Range started at {start} and ended at {end}")]
    Inconsistent { start: usize, end: usize },
}

#[async_trait::async_trait]
pub trait LocalCacheStorage: Send + Sync + std::fmt::Debug + Display + 'static {
    fn entry(
        &self,
        location: &object_store::path::Path,
        part_size: usize,
    ) -> Box<dyn LocalCacheEntry>;

    async fn start_evictor(&self);
}

#[async_trait::async_trait]
pub trait LocalCacheEntry: Send + Sync + std::fmt::Debug + 'static {
    async fn save_part(&self, part_number: PartID, buf: Bytes) -> object_store::Result<()>;

    async fn read_part(
        &self,
        part_number: PartID,
        range_in_part: Range<usize>,
    ) -> object_store::Result<Option<Bytes>>;

    /// might be useful on rewriting GET request on the prefetch phase. the cached files are
    /// expected to be in the same folder, so it'd be expected to be fast without expensive
    /// globbing.
    #[cfg(test)]
    async fn cached_parts(&self) -> object_store::Result<Vec<PartID>>;

    async fn save_head(&self, meta: (&ObjectMeta, &Attributes)) -> object_store::Result<()>;

    async fn read_head(&self) -> object_store::Result<Option<(ObjectMeta, Attributes)>>;
}

#[derive(Debug)]
struct FsCacheStorage {
    root_folder: std::path::PathBuf,
    evictor: Option<Arc<FsCacheEvictor>>,
}

impl FsCacheStorage {
    pub fn new(
        root_folder: std::path::PathBuf,
        max_cache_size_bytes: Option<usize>,
        db_stats: Arc<DbStats>,
    ) -> Self {
        let evictor = max_cache_size_bytes.map(|max_cache_size_bytes| {
            Arc::new(FsCacheEvictor::new(
                root_folder.clone(),
                max_cache_size_bytes,
                db_stats,
            ))
        });

        Self {
            root_folder,
            evictor,
        }
    }
}

#[async_trait::async_trait]
impl LocalCacheStorage for FsCacheStorage {
    fn entry(
        &self,
        location: &object_store::path::Path,
        part_size: usize,
    ) -> Box<dyn LocalCacheEntry> {
        Box::new(FsCacheEntry {
            root_folder: self.root_folder.clone(),
            location: location.clone(),
            evictor: self.evictor.clone(),
            part_size,
        })
    }

    async fn start_evictor(&self) {
        if let Some(evictor) = &self.evictor {
            evictor.start().await
        }
    }
}

impl Display for FsCacheStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FsCacheStorage({})", self.root_folder.display())
    }
}

#[derive(Debug)]
struct FsCacheEntry {
    root_folder: std::path::PathBuf,
    location: Path,
    part_size: usize,
    evictor: Option<Arc<FsCacheEvictor>>,
}

impl FsCacheEntry {
    async fn atomic_write(&self, path: &std::path::Path, buf: Bytes) -> object_store::Result<()> {
        let tmp_path = path.with_extension(format!("_tmp{}", self.make_rand_suffix()));

        // ensure the parent folder exists
        if let Some(folder_path) = tmp_path.parent() {
            fs::create_dir_all(folder_path).await.map_err(wrap_io_err)?;
        }

        // try triggering evict before writing
        if let Some(evictor) = &self.evictor {
            evictor.maybe_evict(path.to_path_buf(), buf.len()).await;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await
            .map_err(wrap_io_err)?;
        file.write_all(&buf).await.map_err(wrap_io_err)?;
        file.sync_all().await.map_err(wrap_io_err)?;
        fs::rename(tmp_path, path).await.map_err(wrap_io_err)
    }

    // every origin file will be split into multiple parts, and all the parts will be saved in the same
    // folder. the part file name is expected to be in the format of `_part{part_size}-{part_number}`,
    // examples: /tmp/mydata.csv/_part1mb-000000001
    fn make_part_path(
        root_folder: std::path::PathBuf,
        location: &Path,
        part_number: usize,
        part_size: usize,
    ) -> std::path::PathBuf {
        // containing the part size in the file name, allows user change the part size on
        // the fly, without the need to invalidate the cache.
        let part_size_name = if part_size % (1024 * 1024) == 0 {
            format!("{}mb", part_size / (1024 * 1024))
        } else {
            format!("{}kb", part_size / 1024)
        };
        let suffix = format!("_part{}-{:09}", part_size_name, part_number);
        let mut path = root_folder.join(location.to_string());
        path.push(suffix);
        path
    }

    fn make_head_path(root_folder: std::path::PathBuf, location: &Path) -> std::path::PathBuf {
        let suffix = "_head".to_string();
        let mut path = root_folder.join(location.to_string());
        path.push(suffix);
        path
    }

    fn make_rand_suffix(&self) -> String {
        let mut rng = rand::thread_rng();
        (0..24).map(|_| rng.sample(Alphanumeric) as char).collect()
    }
}

#[async_trait::async_trait]
impl LocalCacheEntry for FsCacheEntry {
    async fn save_part(&self, part_number: usize, buf: Bytes) -> object_store::Result<()> {
        let part_path = Self::make_part_path(
            self.root_folder.clone(),
            &self.location,
            part_number,
            self.part_size,
        );

        // if the part already exists, do not save again.
        if Some(true) == fs::try_exists(&part_path).await.ok() {
            return Ok(());
        }

        self.atomic_write(&part_path, buf).await
    }

    async fn read_part(
        &self,
        part_number: usize,
        range_in_part: Range<usize>,
    ) -> object_store::Result<Option<Bytes>> {
        let part_path = Self::make_part_path(
            self.root_folder.clone(),
            &self.location,
            part_number,
            self.part_size,
        );

        // if the part file does not exist, return None
        let exists = fs::try_exists(&part_path).await.unwrap_or(false);
        if !exists {
            return Ok(None);
        }

        let file = File::open(&part_path).await.map_err(wrap_io_err)?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut buffer = vec![0; range_in_part.len()];
        reader
            .seek(SeekFrom::Start(range_in_part.start as u64))
            .await
            .map_err(wrap_io_err)?;
        reader.read_exact(&mut buffer).await.map_err(wrap_io_err)?;
        Ok(Some(Bytes::from(buffer)))
    }

    #[cfg(test)]
    async fn cached_parts(&self) -> object_store::Result<Vec<PartID>> {
        let head_path = Self::make_head_path(self.root_folder.clone(), &self.location);
        let directory_path = match head_path.parent() {
            Some(directory_path) => directory_path,
            None => return Ok(vec![]),
        };
        let target_prefix = "_part";

        let mut entries = match fs::read_dir(directory_path).await {
            Ok(entries) => entries,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(vec![]);
                } else {
                    return Err(wrap_io_err(err));
                }
            }
        };

        let mut part_file_names = vec![];
        while let Some(entry) = entries.next_entry().await.map_err(wrap_io_err)? {
            let metadata = entry.metadata().await.map_err(wrap_io_err)?;
            if metadata.is_dir() {
                continue;
            }
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            if file_name_str.starts_with(target_prefix) {
                part_file_names.push(file_name_str.to_string());
            }
        }

        // not cached at all
        if part_file_names.is_empty() {
            return Ok(vec![]);
        }

        // sort the paths in alphabetical order
        part_file_names.sort();

        // retrieve the part numbers from the paths
        let mut part_numbers = Vec::with_capacity(part_file_names.len());
        for part_file_name in part_file_names.iter() {
            let part_number = part_file_name
                .split('-')
                .last()
                .and_then(|part_number| part_number.parse::<usize>().ok());
            if let Some(part_number) = part_number {
                part_numbers.push(part_number);
            }
        }

        Ok(part_numbers)
    }

    async fn save_head(&self, head: (&ObjectMeta, &Attributes)) -> object_store::Result<()> {
        // if the meta file exists and not corrupted, do nothing
        match self.read_head().await {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {}
            Err(_) => {
                // TODO: add a warning
            }
        }

        let head: LocalCacheHead = head.into();
        let buf = serde_json::to_vec(&head).map_err(wrap_io_err)?;

        let meta_path = Self::make_head_path(self.root_folder.clone(), &self.location);
        self.atomic_write(&meta_path, buf.into()).await
    }

    async fn read_head(&self) -> object_store::Result<Option<(ObjectMeta, Attributes)>> {
        let meta_path = Self::make_head_path(self.root_folder.clone(), &self.location);

        let exists = fs::try_exists(&meta_path).await.unwrap_or(false);
        if !exists {
            return Ok(None);
        }

        // TODO: process not found err here instead of check exists
        let file = File::open(&meta_path).await.map_err(wrap_io_err)?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut content = String::new();
        reader
            .read_to_string(&mut content)
            .await
            .map_err(wrap_io_err)?;

        let head: LocalCacheHead = serde_json::from_str(&content).map_err(wrap_io_err)?;
        Ok(Some((head.meta(), head.attributes())))
    }
}

/// FsCacheEvictor evicts the cache entries when the cache size exceeds the limit. it is expected to
/// run in the background to avoid blocking the caller, and it'll be triggered whenever a new cache entry
/// is added.
#[derive(Debug)]
struct FsCacheEvictor {
    root_folder: std::path::PathBuf,
    max_cache_size_bytes: usize,
    tx: tokio::sync::mpsc::Sender<(std::path::PathBuf, usize)>,
    rx: Mutex<Option<tokio::sync::mpsc::Receiver<(std::path::PathBuf, usize)>>>,
    evict_task_handle: OnceCell<tokio::task::JoinHandle<()>>,
    scan_task_handle: OnceCell<tokio::task::JoinHandle<()>>,
    db_stats: Arc<DbStats>,
}

impl FsCacheEvictor {
    pub fn new(
        root_folder: std::path::PathBuf,
        max_cache_size_bytes: usize,
        db_stats: Arc<DbStats>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        Self {
            root_folder,
            max_cache_size_bytes,
            tx,
            rx: Mutex::new(Some(rx)),
            evict_task_handle: OnceCell::new(),
            scan_task_handle: OnceCell::new(),
            db_stats,
        }
    }

    async fn start(&self) {
        let inner = Arc::new(FsCacheEvictorInner::new(
            self.root_folder.clone(),
            self.max_cache_size_bytes,
            self.db_stats.clone(),
        ));

        let guard = self.rx.lock();
        let rx = guard.await.take().expect("evictor already started");

        // scan the cache folder to load the cache entries on disk
        let scan_task_handle = tokio::spawn(inner.clone().scan_entries(true));
        self.scan_task_handle.set(scan_task_handle).ok();

        // start the background evictor task, it'll be triggered whenever a new cache entry is added
        let evict_task_handle = tokio::spawn(Self::background_evict(rx, inner));
        self.evict_task_handle.set(evict_task_handle).ok();
    }

    async fn started(&self) -> bool {
        self.rx.lock().await.is_none()
    }

    async fn background_evict(
        mut rx: tokio::sync::mpsc::Receiver<(std::path::PathBuf, usize)>,
        inner: Arc<FsCacheEvictorInner>,
    ) {
        while let Some((path, bytes)) = rx.recv().await {
            inner
                .track_new_entry(path, bytes, SystemTime::now(), true)
                .await;
        }
    }

    pub async fn maybe_evict(&self, path: std::path::PathBuf, bytes: usize) {
        if !self.started().await {
            return;
        }

        self.tx.send((path, bytes)).await.ok();
    }
}

/// FsCacheEvictorInner manages the cache entries in an in-memory trie, and evict the cache entries
/// when the cache size exceeds the limit. it uses a pick-of-2 strategy to approximate LRU, and evict
/// the older file when the cache size exceeds the limit.
///
/// On start up, FsCacheEvictorInner will scan the cache folder to load the cache files into the in-memory
/// trie cache_entries. This loading process is interleaved with the maybe_evict is being called, so the
/// cache entries should be wrapped with Mutex<_>.
#[derive(Debug)]
struct FsCacheEvictorInner {
    root_folder: std::path::PathBuf,
    batch_factor: usize,
    max_cache_size_bytes: usize,
    cache_entries: Mutex<Trie<std::path::PathBuf, (SystemTime, usize)>>,
    cache_size_bytes: AtomicU64,
    db_stats: Arc<DbStats>,
}

impl FsCacheEvictorInner {
    pub fn new(
        root_folder: std::path::PathBuf,
        max_cache_size_bytes: usize,
        db_stats: Arc<DbStats>,
    ) -> Self {
        Self {
            root_folder,
            batch_factor: 10,
            max_cache_size_bytes,
            cache_entries: Mutex::new(Trie::new()),
            cache_size_bytes: AtomicU64::new(0_u64),
            db_stats,
        }
    }

    // scan the cache folder, and load the cache entries into the in memory trie cache_entries.
    // this function is only called on start up, and it's expected to runned interleavely with
    // maybe_evict is being called.
    pub async fn scan_entries(self: Arc<Self>, evict: bool) {
        // walk the cache folder, record the files and their last access time into the cache_entries
        let iter = WalkDir::new(&self.root_folder).into_iter();
        for entry in iter {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    warn!("evictor: failed to walk the cache folder: {}", err);
                    continue;
                }
            };
            if entry.file_type().is_dir() {
                continue;
            }

            let metadata = match tokio::fs::metadata(entry.path()).await {
                Ok(metadata) => metadata,
                Err(err) => {
                    warn!(
                        "evictor: failed to get the metadata of the cache file: {}",
                        err
                    );
                    continue;
                }
            };
            let atime = metadata.accessed().unwrap_or(SystemTime::UNIX_EPOCH);
            let path = entry.path().to_path_buf();
            let bytes = metadata.len() as usize;

            self.track_new_entry(path, bytes, atime, evict).await;
        }
    }

    async fn track_new_entry(
        &self,
        path: std::path::PathBuf,
        bytes: usize,
        accessed_time: SystemTime,
        eivct: bool,
    ) -> usize {
        // record the new cache entry into the cache_entries, and increase the cache_size_bytes
        self.cache_size_bytes
            .fetch_add(bytes as u64, Ordering::SeqCst);
        self.cache_entries
            .lock()
            .await
            .insert(path.clone(), (accessed_time, bytes));

        // record the metrics
        self.db_stats
            .object_store_cache_keys
            .set(self.cache_entries.lock().await.len() as u64);
        self.db_stats
            .object_store_cache_bytes
            .set(self.cache_size_bytes.load(Ordering::Relaxed));

        if !eivct {
            return 0;
        }

        // if the cache size is still below the limit, do nothing
        if self.cache_size_bytes.load(Ordering::Relaxed) <= self.max_cache_size_bytes as u64 {
            return 0;
        }
        // TODO: check the disk space ratio here, if the disk space is low, also triggers evict.

        // if the cache size exceeds the limit, evict the cache files in batch with the batch_factor,
        // this may help to avoid the cases like triggering the evictor too frequently when the cache
        // size is just slightly above the limit.
        let mut total_bytes: usize = 0;
        for _ in 0..self.batch_factor {
            let evicted_bytes = self.maybe_evict_once().await;
            if evicted_bytes == 0 {
                return total_bytes;
            }

            total_bytes += evicted_bytes;
        }

        total_bytes
    }

    // find a file, and evict it from disk. return the bytes of the evicted file. if no file is evicted or
    // any error occurs, return 0.
    async fn maybe_evict_once(&self) -> usize {
        let (target, target_bytes) = match self.pick_evict_target().await {
            Some(target) => target,
            None => return 0,
        };

        if let Err(err) = tokio::fs::remove_file(&target).await {
            warn!("evictor: failed to remove the cache file: {}", err);
            return 0;
        }

        debug!(
            "evictor: evicted cache file: {:?}, bytes: {}",
            target, target_bytes
        );

        // remove the entry from the cache_entries, and decrease the cache_size_bytes
        self.cache_entries.lock().await.remove(&target);
        self.cache_size_bytes
            .fetch_sub(target_bytes as u64, Ordering::SeqCst);

        // sync the metrics after eviction
        self.db_stats
            .object_store_cache_evicted_bytes
            .add(target_bytes as u64);
        self.db_stats.object_store_cache_evicted_keys.inc();
        self.db_stats
            .object_store_cache_keys
            .set(self.cache_entries.lock().await.len() as u64);
        self.db_stats
            .object_store_cache_bytes
            .set(self.cache_size_bytes.load(Ordering::Relaxed));

        target_bytes
    }

    // pick a file to evict, return None if no file is picked. it takes a pick-of-2 strategy, which is an approximation
    // of LRU, it randomized pick two files, compare their last access time, and choose the older one to evict.
    async fn pick_evict_target(&self) -> Option<(std::path::PathBuf, usize)> {
        if self.cache_entries.lock().await.len() < 2 {
            return None;
        }

        loop {
            let ((path0, (atime0, bytes0)), (path1, (atime1, bytes1))) = match (
                self.random_pick_entry().await,
                self.random_pick_entry().await,
            ) {
                (Some(o0), Some(o1)) => (o0, o1),
                _ => return None,
            };

            // random_pick_entry might return the same file, skip it.
            if path0 == path1 {
                continue;
            }

            if atime0 <= atime1 {
                return Some((path0, bytes0));
            } else {
                return Some((path1, bytes1));
            }
        }
    }

    async fn random_pick_entry(&self) -> Option<(std::path::PathBuf, (SystemTime, usize))> {
        let cache_entries = self.cache_entries.lock().await;
        let mut rng = rand::rngs::StdRng::from_entropy();

        let mut rand_child = match cache_entries.children().choose(&mut rng) {
            None => return None,
            Some(child) => child,
        };
        loop {
            if rand_child.is_leaf() {
                return rand_child.key().cloned().zip(rand_child.value().cloned());
            }
            rand_child = match rand_child.children().choose(&mut rng) {
                None => return None,
                Some(child) => child,
            };
        }
    }
}

type PartID = usize;

fn wrap_io_err(err: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "cached_object_store",
        source: Box::new(err),
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc, time::SystemTime};

    use bytes::Bytes;
    use filetime::FileTime;
    use object_store::{path::Path, GetOptions, GetRange, ObjectStore, PutPayload};
    use rand::{thread_rng, Rng};

    use super::CachedObjectStore;
    use crate::{
        cached_object_store::{FsCacheEntry, FsCacheEvictorInner, PartID},
        metrics::DbStats,
    };

    fn gen_rand_bytes(n: usize) -> Bytes {
        let mut rng = thread_rng();
        let random_bytes: Vec<u8> = (0..n).map(|_| rng.gen()).collect();
        Bytes::from(random_bytes)
    }

    fn gen_rand_file(
        folder_path: &std::path::Path,
        file_name: &str,
        n: usize,
    ) -> std::path::PathBuf {
        let file_path = folder_path.join(file_name);
        let bytes = gen_rand_bytes(n);
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(&bytes).unwrap();
        file_path
    }

    fn new_test_cache_folder() -> std::path::PathBuf {
        let mut rng = rand::thread_rng();
        let dir_name: String = (0..10)
            .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
            .collect();
        let path = format!("/tmp/testcache-{}", dir_name);
        let _ = std::fs::remove_dir_all(&path);
        std::path::PathBuf::from(path)
    }

    #[tokio::test]
    async fn test_save_result_not_aligned() -> object_store::Result<()> {
        let payload = gen_rand_bytes(1024 * 3 + 32);
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let db_stats = Arc::new(DbStats::new());
        object_store
            .put(
                &Path::from("/data/testfile1"),
                PutPayload::from_bytes(payload.clone()),
            )
            .await?;
        let location = Path::from("/data/testfile1");
        let get_result = object_store.get(&location).await?;

        let part_size = 1024;
        let cached_store = CachedObjectStore::new(
            object_store.clone(),
            test_cache_folder.clone(),
            None,
            part_size,
            db_stats,
        )
        .unwrap();
        let entry = cached_store.cache_storage.entry(&location, 1024);

        let object_size_hint = cached_store.save_result(get_result).await?;
        assert_eq!(object_size_hint, 1024 * 3 + 32);

        // assert the cached meta
        let head = entry.read_head().await?;
        assert_eq!(head.unwrap().0.size, 1024 * 3 + 32);

        // assert the parts
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 4);
        assert_eq!(
            entry.read_part(0, 0..part_size).await?,
            Some(payload.slice(0..1024))
        );
        assert_eq!(
            entry.read_part(1, 0..part_size).await?,
            Some(payload.slice(1024..2048))
        );
        assert_eq!(
            entry.read_part(2, 0..part_size).await?,
            Some(payload.slice(2048..3072))
        );

        // delete part 2, known_cache_size is still known
        let evict_part_path =
            FsCacheEntry::make_part_path(test_cache_folder.clone(), &location, 2, 1024);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(2, 0..part_size).await?, None);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts, vec![0, 1, 3]);

        // delete part 3, known_cache_size become None
        let evict_part_path =
            FsCacheEntry::make_part_path(test_cache_folder.clone(), &location, 3, 1024);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(3, 0..part_size).await?, None);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts, vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn test_save_result_aligned() -> object_store::Result<()> {
        let payload = gen_rand_bytes(1024 * 3);
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let db_stats = Arc::new(DbStats::new());
        object_store
            .put(
                &Path::from("/data/testfile1"),
                PutPayload::from_bytes(payload.clone()),
            )
            .await?;
        let location = Path::from("/data/testfile1");
        let get_result = object_store.get(&location).await?;
        let part_size = 1024;

        let cached_store = CachedObjectStore::new(
            object_store,
            test_cache_folder.clone(),
            None,
            part_size,
            db_stats,
        )
        .unwrap();
        let entry = cached_store.cache_storage.entry(&location, part_size);
        let object_size_hint = cached_store.save_result(get_result).await?;
        assert_eq!(object_size_hint, 1024 * 3);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 3);
        assert_eq!(
            entry.read_part(0, 0..part_size).await?,
            Some(payload.slice(0..1024))
        );
        assert_eq!(
            entry.read_part(1, 0..part_size).await?,
            Some(payload.slice(1024..2048))
        );
        assert_eq!(
            entry.read_part(2, 0..part_size).await?,
            Some(payload.slice(2048..3072))
        );

        let evict_part_path =
            FsCacheEntry::make_part_path(test_cache_folder.clone(), &location, 2, part_size);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(2, 0..part_size).await?, None);

        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 2);
        Ok(())
    }

    #[test]
    fn test_split_range_into_parts() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let db_stats = Arc::new(DbStats::new());
        let cached_store =
            CachedObjectStore::new(object_store, test_cache_folder, None, 1024, db_stats).unwrap();

        struct Test {
            input: (Option<GetRange>, usize),
            expect: Vec<(PartID, std::ops::Range<usize>)>,
        }
        let tests = [
            Test {
                input: (None, 1024 * 3),
                expect: vec![(0, 0..1024), (1, 0..1024), (2, 0..1024)],
            },
            Test {
                input: (None, 1024 * 3 + 12),
                expect: vec![(0, 0..1024), (1, 0..1024), (2, 0..1024), (3, 0..12)],
            },
            Test {
                input: (None, 12),
                expect: vec![(0, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(0..1024)), 1024),
                expect: vec![(0, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024)), 20000),
                expect: vec![(0, 128..1024)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024 + 12)), 20000),
                expect: vec![(0, 128..1024), (1, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024 * 2 + 12)), 20000),
                expect: vec![(0, 128..1024), (1, 0..1024), (2, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(1024 * 2..1024 * 3 + 12)), 200000),
                expect: vec![(2, 0..1024), (3, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(1024 * 2 - 2..1024 * 3 + 12)), 20000),
                expect: vec![(1, 1022..1024), (2, 0..1024), (3, 0..12)],
            },
            Test {
                input: (Some(GetRange::Suffix(128)), 1024),
                expect: vec![(0, 896..1024)],
            },
            Test {
                input: (Some(GetRange::Suffix(1024 * 2 + 8)), 1024 * 4),
                expect: vec![(1, 1016..1024), (2, 0..1024), (3, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Offset(8)), 1024 * 4),
                expect: vec![(0, 8..1024), (1, 0..1024), (2, 0..1024), (3, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Offset(1024 * 2 + 8)), 1024 * 4),
                expect: vec![(2, 8..1024), (3, 0..1024)],
            },
            Test {
                input: (Some(GetRange::Offset(1024 * 2 + 8)), 1024 * 4 + 2),
                expect: vec![(2, 8..1024), (3, 0..1024), (4, 0..2)],
            },
        ];

        for t in tests.iter() {
            let range = cached_store
                .canonicalize_range(t.input.0.clone(), t.input.1)
                .unwrap();
            let parts = cached_store.split_range_into_parts(range);
            assert_eq!(parts, t.expect, "input: {:?}", t.input);
        }
    }

    #[test]
    fn test_align_range() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let db_stats = Arc::new(DbStats::new());
        let cached_store =
            CachedObjectStore::new(object_store, test_cache_folder, None, 1024, db_stats).unwrap();

        let aligned = cached_store.align_range(&(9..1025), 1024);
        assert_eq!(aligned, 0..2048);
        let aligned = cached_store.align_range(&(1024 + 1..2048 + 4), 1024);
        assert_eq!(aligned, 1024..3072);
    }

    #[test]
    fn test_align_get_range() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let db_stats = Arc::new(DbStats::new());
        let cached_store =
            CachedObjectStore::new(object_store, test_cache_folder, None, 1024, db_stats).unwrap();

        let aligned = cached_store.align_get_range(&GetRange::Bounded(9..1025));
        assert_eq!(aligned, GetRange::Bounded(0..2048));
        let aligned = cached_store.align_get_range(&GetRange::Bounded(9..2048));
        assert_eq!(aligned, GetRange::Bounded(0..2048));
        let aligned = cached_store.align_get_range(&GetRange::Suffix(12));
        assert_eq!(aligned, GetRange::Suffix(1024));
        let aligned = cached_store.align_get_range(&GetRange::Suffix(1024));
        assert_eq!(aligned, GetRange::Suffix(1024));
        let aligned = cached_store.align_get_range(&GetRange::Offset(1024));
        assert_eq!(aligned, GetRange::Offset(1024));
        let aligned = cached_store.align_get_range(&GetRange::Offset(12));
        assert_eq!(aligned, GetRange::Offset(0));
    }

    #[tokio::test]
    async fn test_cached_object_store_impl_object_store() -> object_store::Result<()> {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let test_cache_folder = new_test_cache_folder();
        let cached_store = CachedObjectStore::new(
            object_store.clone(),
            test_cache_folder,
            None,
            1024,
            Arc::new(DbStats::new()),
        )
        .unwrap();

        let test_path = Path::from("/data/testdata1");
        let test_payload = gen_rand_bytes(1024 * 3 + 2);
        object_store
            .put(&test_path, PutPayload::from_bytes(test_payload.clone()))
            .await?;

        // test get entire object
        let test_ranges = vec![
            Some(GetRange::Offset(260817)),
            None,
            Some(GetRange::Bounded(1000..2048)),
            Some(GetRange::Bounded(1000..260817)),
            Some(GetRange::Suffix(10)),
            Some(GetRange::Suffix(260817)),
            Some(GetRange::Offset(1000)),
            Some(GetRange::Offset(0)),
            Some(GetRange::Offset(1028)),
            Some(GetRange::Offset(260817)),
            Some(GetRange::Offset(1024 * 3 + 2)),
            Some(GetRange::Offset(1024 * 3 + 1)),
            #[allow(clippy::reversed_empty_ranges)]
            Some(GetRange::Bounded(2900..2048)),
            Some(GetRange::Bounded(10..10)),
        ];

        // test get a range
        for range in test_ranges.iter() {
            let want = object_store
                .get_opts(
                    &test_path,
                    GetOptions {
                        range: range.clone(),
                        ..Default::default()
                    },
                )
                .await;
            let got = cached_store
                .cached_get_opts(
                    &test_path,
                    GetOptions {
                        range: range.clone(),
                        ..Default::default()
                    },
                )
                .await;
            match (want, got) {
                (Ok(want), Ok(got)) => {
                    assert_eq!(want.range, got.range);
                    assert_eq!(want.meta, got.meta);
                    assert_eq!(want.bytes().await?, got.bytes().await?);
                }
                (Err(want), Err(got)) => {
                    if want.to_string().to_lowercase().contains("range") {
                        assert!(got.to_string().to_lowercase().contains("range"));
                    }
                }
                (origin_result, cached_result) => {
                    panic!("expect: {:?}, got: {:?}", origin_result, cached_result);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_evictor() {
        let temp_dir = tempfile::Builder::new()
            .prefix("objstore_cache_test_evictor_")
            .tempdir()
            .unwrap();

        let mut evictor = FsCacheEvictorInner::new(
            temp_dir.path().to_path_buf(),
            1024 * 2,
            Arc::new(DbStats::new()),
        );
        evictor.batch_factor = 2;

        let path0 = gen_rand_file(temp_dir.path(), "file0", 1024);
        let evicted = evictor
            .track_new_entry(path0, 1024, SystemTime::now(), true)
            .await;
        assert_eq!(evicted, 0);

        let path1 = gen_rand_file(temp_dir.path(), "file1", 1024);
        let evicted = evictor
            .track_new_entry(path1, 1024, SystemTime::now(), true)
            .await;
        assert_eq!(evicted, 0);

        let path2 = gen_rand_file(temp_dir.path(), "file2", 1024);
        let evicted = evictor
            .track_new_entry(path2, 1024, SystemTime::now(), true)
            .await;
        assert_eq!(evicted, 2048);

        let file_paths = walkdir::WalkDir::new(temp_dir.path())
            .into_iter()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(file_paths.len(), 2); // the folder file "." is also counted
    }

    #[tokio::test]
    async fn test_evictor_pick() {
        let temp_dir = tempfile::Builder::new()
            .prefix("objstore_cache_test_evictor_")
            .tempdir()
            .unwrap();

        let evictor = Arc::new(FsCacheEvictorInner::new(
            temp_dir.path().to_path_buf(),
            1024 * 2,
            Arc::new(DbStats::new()),
        ));

        let path0 = gen_rand_file(temp_dir.path(), "file0", 1024);
        gen_rand_file(temp_dir.path(), "file1", 1025);

        filetime::set_file_atime(&path0, FileTime::from_system_time(SystemTime::UNIX_EPOCH))
            .unwrap();

        evictor.clone().scan_entries(false).await;

        let (target_path, size) = evictor.pick_evict_target().await.unwrap();
        assert_eq!(target_path, path0);
        assert_eq!(size, 1024);
    }
}
