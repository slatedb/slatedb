use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, stream, stream::BoxStream, StreamExt};
use object_store::{buffered::BufWriter, Attribute, Attributes, GetRange, GetResultPayload};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, ops::Range, sync::Arc};
use tokio::{
    fs,
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use object_store::{path::Path, GetOptions, GetResult, ObjectMeta, ObjectStore};

use crate::metrics::DbStats;

pub(crate) struct CacheableGetOptions {
    /// the range of the object to get, if None, the entire object will be fetched.
    pub range: Option<GetRange>,
    /// the cache strategy to use for this request, you can choose to
    /// disable caching.
    pub cache_enabled: bool,
    /// the lower the rank, the less likely the object will be evicted.
    /// if set to Some(0), the object will never be evicted.
    #[allow(unused)]
    pub cache_rank: Option<u8>,
}

impl Default for CacheableGetOptions {
    fn default() -> Self {
        Self {
            range: None,
            cache_enabled: true,
            cache_rank: None,
        }
    }
}

/// CachableObjectStore is a wrapper around ObjectStore, it caches the object
/// in the local, and serves the object from the cache if possible.
#[derive(Clone, Debug)]
pub(crate) enum CacheableObjectStore {
    Cached(Arc<CacheableObjectStoreInner>),
    Direct(Arc<dyn ObjectStore>),
}

impl CacheableObjectStore {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        root_folder: std::path::PathBuf,
        part_size: usize,
        db_stats: Arc<DbStats>,
    ) -> Self {
        Self::Cached(Arc::new(CacheableObjectStoreInner::new(
            object_store,
            root_folder,
            part_size,
            db_stats,
        )))
    }

    pub async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        match self {
            Self::Cached(inner) => inner.cached_head(location).await,
            Self::Direct(object_store) => object_store.head(location).await,
        }
    }

    pub async fn get_opts(
        &self,
        location: &Path,
        opts: CacheableGetOptions,
    ) -> object_store::Result<GetResult> {
        match self {
            Self::Cached(inner) => inner.cached_get_opts(location, opts).await,
            Self::Direct(object_store) => {
                let get_opts = GetOptions {
                    range: opts.range,
                    ..Default::default()
                };
                object_store.get_opts(location, get_opts).await
            }
        }
    }

    pub fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        match self {
            Self::Cached(inner) => inner.object_store.list(prefix),
            Self::Direct(object_store) => object_store.list(prefix),
        }
    }

    pub fn buf_writer(&self, location: Path) -> BufWriter {
        BufWriter::new(self.object_store(), location)
    }

    /// TODO: make this private
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match self {
            Self::Cached(inner) => inner.object_store.clone(),
            Self::Direct(object_store) => object_store.clone(),
        }
    }
}

impl From<Arc<dyn ObjectStore + 'static>> for CacheableObjectStore {
    fn from(object_store: Arc<dyn ObjectStore + 'static>) -> Self {
        Self::Direct(object_store)
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub(crate) struct CacheableObjectStoreInner {
    object_store: Arc<dyn ObjectStore>,
    part_size: usize, // expected to be aligned with mb or kb
    storage: Arc<dyn LocalCacheStorage>,
    db_stats: Arc<DbStats>,
}

impl CacheableObjectStoreInner {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        root_folder: std::path::PathBuf,
        part_size: usize,
        db_stats: Arc<DbStats>,
    ) -> Self {
        assert!(part_size % 1024 == 0);

        let storage = Arc::new(DiskCacheStorage {
            root_folder: root_folder.clone(),
        });
        Self {
            object_store,
            part_size,
            storage,
            db_stats,
        }
    }

    pub async fn cached_head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let entry = self.storage.entry(location, self.part_size);
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
        opts: CacheableGetOptions,
    ) -> object_store::Result<GetResult> {
        if !opts.cache_enabled {
            return self
                .object_store
                .get_opts(
                    location,
                    GetOptions {
                        range: opts.range,
                        ..Default::default()
                    },
                )
                .await;
        }

        let (meta, attributes) = self.maybe_prefetch_range(location, &opts.range).await?;
        let range = self.canonicalize_range(opts.range, meta.size)?;
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

    // if an object is not cached before, maybe_prefetch_range will try to prefetch the object from the
    // object store and save the parts into the local disk cache. the prefetching is helpful to reduce the
    // number of GET requests to the object store, it'll try to aggregate the parts among the range into a
    // single GET request, and save the related parts into local disks together.
    // when it sends GET requests to the object store, the range is expected to be ALIGNED with the part
    // size.
    async fn maybe_prefetch_range(
        &self,
        location: &Path,
        range: &Option<GetRange>,
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        let entry = self.storage.entry(location, self.part_size);
        match entry.read_head().await {
            Ok(Some((meta, attrs))) => return Ok((meta, attrs)),
            Ok(None) => {}
            Err(_) => {
                // TODO: add a warning log
            }
        };

        let aligned_opts = match &range {
            None => GetOptions::default(),
            Some(get_range) => GetOptions {
                range: Some(self.align_get_range(get_range)),
                ..Default::default()
            },
        };

        let get_result = self.object_store.get_opts(location, aligned_opts).await?;
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
        assert!(result.range.start % self.part_size == 0);
        assert!(result.range.end % self.part_size == 0 || result.range.end == result.meta.size);

        let entry = self.storage.entry(&result.meta.location, self.part_size);
        entry.save_head((&result.meta, &result.attributes)).await?;

        let mut buffer = BytesMut::new();
        let mut part_number = result.range.start / self.part_size;
        let object_size = result.meta.size;

        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);

            while buffer.len() >= self.part_size {
                let to_write = buffer.split_to(self.part_size);
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
        let range_aligned = self.align_range(&range, self.part_size);
        let start_part = range_aligned.start / self.part_size;
        let end_part = range_aligned.end / self.part_size;
        let mut parts: Vec<_> = (start_part..end_part)
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
        if let Some(first_part) = parts.first_mut() {
            first_part.1.start = range.start % self.part_size;
        }
        if let Some(last_part) = parts.last_mut() {
            if range.end % self.part_size != 0 {
                last_part.1.end = range.end % self.part_size;
            }
        }
        parts
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
        let location = location.clone();
        let entry = self.storage.entry(&location, self.part_size);
        let db_stats = self.db_stats.clone();
        Box::pin(async move {
            db_stats.object_store_cache_part_access.inc();

            if let Ok(Some(bytes)) = entry.read_part(part_id).await {
                db_stats.object_store_cache_part_hits.inc();
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

            // save it to the disk cache, we'll ignore the error on writing to disk here, just return
            // the bytes from the object store.
            let meta = get_result.meta.clone();
            let attrs = get_result.attributes.clone();
            let bytes = get_result.bytes().await?;
            entry.save_head((&meta, &attrs)).await.ok();
            entry.save_part(part_id, bytes.clone()).await.ok();
            Ok(bytes.slice(range_in_part))
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
                let aligned = self.align_range(bounded, self.part_size);
                GetRange::Bounded(aligned)
            }
            GetRange::Suffix(suffix) => {
                let suffix_aligned = self.align_range(&(0..*suffix), self.part_size).end;
                GetRange::Suffix(suffix_aligned)
            }
            GetRange::Offset(offset) => {
                let offset_aligned = *offset - *offset % self.part_size;
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

impl std::fmt::Display for CacheableObjectStoreInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalCachedObjectStore({}, {})",
            self.object_store, self.storage
        )
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

pub trait LocalCacheStorage: Send + Sync + std::fmt::Debug + Display + 'static {
    fn entry(
        &self,
        location: &object_store::path::Path,
        part_size: usize,
    ) -> Box<dyn LocalCacheEntry>;
}

#[async_trait::async_trait]
pub trait LocalCacheEntry: Send + Sync + std::fmt::Debug + 'static {
    async fn save_part(&self, part_number: PartID, buf: Bytes) -> object_store::Result<()>;

    async fn read_part(&self, part_number: PartID) -> object_store::Result<Option<Bytes>>;

    /// might be useful on rewriting GET request on the prefetch phase. the cached files are
    /// expected to be in the same folder, so it'd be expected to be fast without expensive
    /// globbing.
    #[cfg(test)]
    async fn cached_parts(&self) -> object_store::Result<Vec<PartID>>;

    async fn save_head(&self, meta: (&ObjectMeta, &Attributes)) -> object_store::Result<()>;

    async fn read_head(&self) -> object_store::Result<Option<(ObjectMeta, Attributes)>>;
}

#[derive(Debug)]
struct DiskCacheStorage {
    root_folder: std::path::PathBuf,
}

impl LocalCacheStorage for DiskCacheStorage {
    fn entry(
        &self,
        location: &object_store::path::Path,
        part_size: usize,
    ) -> Box<dyn LocalCacheEntry> {
        Box::new(DiskCacheEntry {
            root_folder: self.root_folder.clone(),
            location: location.clone(),
            part_size,
        })
    }
}

impl Display for DiskCacheStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiskCacheStorage({})", self.root_folder.display())
    }
}

#[derive(Debug)]
struct DiskCacheEntry {
    root_folder: std::path::PathBuf,
    location: Path,
    part_size: usize,
}

impl DiskCacheEntry {
    async fn atomic_write(&self, path: &std::path::Path, buf: Bytes) -> object_store::Result<()> {
        let tmp_path = path.with_extension(format!("_tmp{}", self.make_rand_suffix()));

        // ensure the parent folder exists
        if let Some(folder_path) = tmp_path.parent() {
            fs::create_dir_all(folder_path).await.map_err(wrap_io_err)?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await
            .map_err(wrap_io_err)?;
        file.write_all(&buf).await.map_err(wrap_io_err)?;
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
        (0..6).map(|_| rng.sample(Alphanumeric) as char).collect()
    }
}

#[async_trait::async_trait]
impl LocalCacheEntry for DiskCacheEntry {
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

    async fn read_part(&self, part_number: usize) -> object_store::Result<Option<Bytes>> {
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
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.map_err(wrap_io_err)?;
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

        let mut entries = fs::read_dir(directory_path).await.map_err(wrap_io_err)?;

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

#[allow(unused)]
type PartID = usize;

#[allow(unused)]
fn wrap_io_err(err: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "cached_object_store",
        source: Box::new(err),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{path::Path, GetOptions, GetRange, ObjectStore, PutPayload};
    use rand::{thread_rng, Rng};

    use crate::{
        disk_cache::{CacheableGetOptions, DiskCacheEntry, PartID},
        metrics::DbStats,
    };

    use super::CacheableObjectStoreInner;

    fn gen_rand_bytes(n: usize) -> Bytes {
        let mut rng = thread_rng();
        let random_bytes: Vec<u8> = (0..n).map(|_| rng.gen()).collect();
        Bytes::from(random_bytes)
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

        let cached_store = CacheableObjectStoreInner::new(
            object_store.clone(),
            test_cache_folder.clone(),
            1024,
            db_stats,
        );
        let entry = cached_store.storage.entry(&location, 1024);

        let object_size_hint = cached_store.save_result(get_result).await?;
        assert_eq!(object_size_hint, 1024 * 3 + 32);

        // assert the cached meta
        let head = entry.read_head().await?;
        assert_eq!(head.unwrap().0.size, 1024 * 3 + 32);

        // assert the parts
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 4);
        assert_eq!(entry.read_part(0).await?, Some(payload.slice(0..1024)));
        assert_eq!(entry.read_part(1).await?, Some(payload.slice(1024..2048)));
        assert_eq!(entry.read_part(2).await?, Some(payload.slice(2048..3072)));
        assert_eq!(entry.read_part(3).await?, Some(payload.slice(3072..)));

        // delete part 2, known_cache_size is still known
        let evict_part_path =
            DiskCacheEntry::make_part_path(test_cache_folder.clone(), &location, 2, 1024);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(2).await?, None);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts, vec![0, 1, 3]);

        // delete part 3, known_cache_size become None
        let evict_part_path =
            DiskCacheEntry::make_part_path(test_cache_folder.clone(), &location, 3, 1024);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(3).await?, None);
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

        let cached_store = CacheableObjectStoreInner::new(
            object_store,
            test_cache_folder.clone(),
            part_size,
            db_stats,
        );
        let entry = cached_store.storage.entry(&location, part_size);
        let object_size_hint = cached_store.save_result(get_result).await?;
        assert_eq!(object_size_hint, 1024 * 3);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 3);
        assert_eq!(entry.read_part(0).await?, Some(payload.slice(0..1024)));
        assert_eq!(entry.read_part(1).await?, Some(payload.slice(1024..2048)));
        assert_eq!(entry.read_part(2).await?, Some(payload.slice(2048..3072)));

        let evict_part_path =
            DiskCacheEntry::make_part_path(test_cache_folder.clone(), &location, 2, part_size);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(2).await?, None);

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
            CacheableObjectStoreInner::new(object_store, test_cache_folder, 1024, db_stats);

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
            CacheableObjectStoreInner::new(object_store, test_cache_folder, 1024, db_stats);

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
            CacheableObjectStoreInner::new(object_store, test_cache_folder, 1024, db_stats);

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
        let cached_store = CacheableObjectStoreInner::new(
            object_store.clone(),
            test_cache_folder,
            1024,
            Arc::new(DbStats::new()),
        );

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
                    CacheableGetOptions {
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
}
