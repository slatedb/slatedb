use bytes::Bytes;
use bytes::BytesMut;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::GetRange;
use object_store::GetResultPayload;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::Deserialize;
use serde::Serialize;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use std::time::UNIX_EPOCH;
use tokio::fs;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

#[allow(unused)]
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

        let meta = self.maybe_prefetch_range(&entry, &opts.range).await?;
        let parts = self.split_range_into_parts(opts.range.clone(), meta.size);

        // read parts, and concatenate them into a single stream. please note that some of these part may not be cached,
        // we'll still fallback to the object store to get the missing parts.
        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| self.read_part(location, part_id, range_in_part))
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();
        let result_range = self.canonicalize_range(opts.range, meta.size);

        Ok(GetResult {
            meta: meta,
            range: result_range,
            attributes: Default::default(),
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
        entry: &DiskCacheEntry,
        range: &Option<GetRange>,
    ) -> object_store::Result<ObjectMeta> {
        match entry.read_meta().await {
            Ok(Some(meta)) => return Ok(meta),
            Ok(None) => {}
            Err(_) => {
                // TODO: add a warning log
            }
        };

        let aligned_opts = match &range {
            None => GetOptions::default(),
            Some(range) => match range {
                GetRange::Bounded(bounded) => {
                    let start_aligned = bounded.start - bounded.start % self.part_size;
                    let end_aligned = bounded.end + self.part_size - bounded.end % self.part_size;
                    GetOptions {
                        range: Some(GetRange::Bounded(Range {
                            start: start_aligned,
                            end: end_aligned,
                        })),
                        ..Default::default()
                    }
                }
                GetRange::Suffix(suffix) => {
                    let suffix_aligned = *suffix + self.part_size - *suffix % self.part_size;
                    GetOptions {
                        range: Some(GetRange::Suffix(suffix_aligned)),
                        ..Default::default()
                    }
                }
                GetRange::Offset(offset) => {
                    let offset_aligned = *offset - *offset % self.part_size;
                    GetOptions {
                        range: Some(GetRange::Offset(offset_aligned)),
                        ..Default::default()
                    }
                }
            },
        };

        let get_result = self
            .object_store
            .get_opts(&entry.location, aligned_opts)
            .await?;
        let object_meta = get_result.meta.clone();
        // swallow the error on saving to disk here (the disk might be already full), just fallback
        // to the object store.
        // TODO: add a warning log here.
        entry.save_result(get_result).await.ok();
        Ok(object_meta)
    }

    // given the range and object size, return the canonicalized `Range<usize>` with concrete start and
    // end.
    fn canonicalize_range(&self, range: Option<GetRange>, object_size_hint: usize) -> Range<usize> {
        let (start_offset, end_offset) = match range {
            None => (0, object_size_hint),
            Some(range) => match range {
                GetRange::Bounded(range) => (range.start, range.end),
                GetRange::Offset(offset) => (offset, object_size_hint),
                GetRange::Suffix(suffix) => (object_size_hint - suffix, object_size_hint),
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
        let start_part = (range.start - range.start % self.part_size) / self.part_size;
        let end_part = range.end.div_ceil(self.part_size);
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
        let root_folder = self.root_folder.clone();
        let location = location.clone();
        Box::pin(async move {
            let entry = DiskCacheEntry {
                root_folder,
                location: location.clone(),
                part_size,
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

            // save it to the disk cache, we'll ignore the error on writing to disk here, just return
            // the bytes from the object store.
            let meta = get_result.meta.clone();
            let bytes = get_result.bytes().await?;
            entry.save_meta(&meta).await.ok();
            entry.save_part(part_id, &bytes.clone()).await.ok();
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskCacheMeta {
    pub location: String,
    pub last_modified_timestamp: u64,
    pub size: usize,
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

struct DiskCacheEntry {
    root_folder: std::path::PathBuf,
    location: object_store::path::Path,
    part_size: usize,
}

#[allow(unused)]
type PartID = usize;

#[allow(unused)]
impl DiskCacheEntry {
    /// save the GetResult to the disk cache, a GetResult may be transformed into multiple parts
    /// file and a meta file. please note that the `range` in the GetResult is expected to be
    /// aligned with the part size.
    pub async fn save_result(&self, result: GetResult) -> object_store::Result<usize> {
        assert!(result.range.start % self.part_size == 0);

        self.save_meta(&result.meta).await?;

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
        let part_path = self.make_part_path(part_id, false);

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

    pub async fn read_meta(&self) -> object_store::Result<Option<ObjectMeta>> {
        let meta_path = self.make_meta_path();

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

        let meta: DiskCacheMeta = serde_json::from_str(&content).map_err(wrap_io_err)?;

        Ok(Some(ObjectMeta {
            location: meta.location.into(),
            last_modified: (UNIX_EPOCH + Duration::from_secs(meta.last_modified_timestamp)).into(),
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }))
    }

    // only used in test
    pub async fn cached_parts(&self) -> object_store::Result<Vec<PartID>> {
        let pattern = self.make_part_path(0, true);
        let mut part_paths = glob::glob(&pattern.to_string_lossy())
            .map_err(wrap_io_err)?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // not cached at all
        if part_paths.is_empty() {
            return Ok(vec![]);
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
                .split('-')
                .last()
                .and_then(|part_number| part_number.parse::<usize>().ok());
            if let Some(part_number) = part_number {
                part_numbers.push(part_number);
            }
        }

        Ok(part_numbers)
    }

    // if the disk is full, we'll get an error here.
    // TODO: this error can be ignored in the caller side and print a warning.
    async fn save_part(&self, part_number: usize, buf: &[u8]) -> object_store::Result<()> {
        let part_file_path = self.make_part_path(part_number, false);

        // if the part already exists, do not save again.
        if Some(true) == fs::try_exists(&part_file_path).await.ok() {
            return Ok(());
        }

        // ensure the parent folder exists
        if let Some(part_file_folder) = part_file_path.parent() {
            fs::create_dir_all(part_file_folder)
                .await
                .map_err(wrap_io_err)?;
        }

        // save the file content to tmp. if the disk is full, we'll get an error here.
        let tmp_part_file_path =
            part_file_path.with_extension(format!("tmp{}", self.make_rand_suffix()));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_part_file_path)
            .await
            .map_err(wrap_io_err)?;
        file.write_all(buf).await.map_err(wrap_io_err)?;

        // rename the file
        fs::rename(tmp_part_file_path, part_file_path)
            .await
            .map_err(wrap_io_err)
            .ok();
        Ok(())
    }

    async fn save_meta(&self, meta: &ObjectMeta) -> object_store::Result<()> {
        let meta_file_path = self.make_meta_path();

        // if the meta file exists and valid, do nothing
        let exists = self.read_meta().await.is_ok();
        if exists {
            return Ok(());
        }

        // ensure the parent folder exists
        if let Some(part_file_folder) = meta_file_path.parent() {
            fs::create_dir_all(part_file_folder)
                .await
                .map_err(wrap_io_err)?;
        }

        let meta = DiskCacheMeta {
            location: meta.location.to_string(),
            last_modified_timestamp: meta.last_modified.timestamp() as u64,
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        };
        let buf = serde_json::to_vec(&meta).map_err(wrap_io_err)?;

        let tmp_meta_file_path =
            meta_file_path.with_extension(format!("tmp{}", self.make_rand_suffix()));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_meta_file_path)
            .await
            .map_err(wrap_io_err)?;
        file.write_all(&buf).await.map_err(wrap_io_err)?;

        // rename the tmp file to meta_file_path
        if fs::try_exists(&meta_file_path).await.unwrap_or(false) {
            fs::remove_file(&meta_file_path);
        }
        fs::rename(tmp_meta_file_path, meta_file_path)
            .await
            .map_err(wrap_io_err)
    }

    fn make_part_path(&self, part_number: usize, wildcard: bool) -> std::path::PathBuf {
        // containing the part size in the file name, allows user change the part size on
        // the fly, without the need to invalidate the cache.
        let part_size_name = if self.part_size % (1024 * 1024) == 0 {
            format!("{}mb", self.part_size / (1024 * 1024))
        } else {
            format!("{}kb", self.part_size / 1024)
        };
        let part_number_str = if wildcard {
            "*".to_string()
        } else {
            format!("{:09}", part_number)
        };
        self.root_folder
            .join(self.location.to_string())
            .with_extension(format!("_part{}-{}", part_size_name, part_number_str))
    }

    fn make_meta_path(&self) -> std::path::PathBuf {
        self.root_folder
            .join(self.location.to_string())
            .with_extension("_meta")
    }

    fn make_rand_suffix(&self) -> String {
        let mut rng = rand::thread_rng();
        (0..6).map(|_| rng.sample(Alphanumeric) as char).collect()
    }
}

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
    use object_store::path::Path;
    use object_store::{GetRange, ObjectStore, PutPayload};
    use rand::{thread_rng, Rng};

    use crate::cached_object_store::PartID;

    use super::CachedObjectStore;

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
        let object_store = object_store::memory::InMemory::new();
        let test_cache_folder = new_test_cache_folder();
        object_store
            .put(
                &Path::from("/data/testfile1"),
                PutPayload::from_bytes(payload.clone()),
            )
            .await?;
        let get_result = object_store.get(&Path::from("/data/testfile1")).await?;

        let entry = super::DiskCacheEntry {
            root_folder: test_cache_folder,
            location: Path::from("/data/testfile1"),
            part_size: 1024,
        };
        let object_size_hint = entry.save_result(get_result).await?;
        assert_eq!(object_size_hint, 1024 * 3 + 32);

        // assert the cached meta
        let meta = entry.read_meta().await?;
        assert_eq!(meta.unwrap().size, 1024 * 3 + 32);

        // assert the parts
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 4);
        assert_eq!(entry.read_part(0).await?, Some(payload.slice(0..1024)));
        assert_eq!(entry.read_part(1).await?, Some(payload.slice(1024..2048)));
        assert_eq!(entry.read_part(2).await?, Some(payload.slice(2048..3072)));
        assert_eq!(entry.read_part(3).await?, Some(payload.slice(3072..)));

        // delete part 2, known_cache_size is still known
        let evict_part_path = entry.make_part_path(2, false);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(2).await?, None);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts, vec![0, 1, 3]);

        // delete part 3, known_cache_size become None
        let evict_part_path = entry.make_part_path(3, false);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(3).await?, None);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts, vec![0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn test_save_result_aligned() -> object_store::Result<()> {
        let payload = gen_rand_bytes(1024 * 3);
        let object_store = object_store::memory::InMemory::new();
        let test_cache_folder = new_test_cache_folder();
        object_store
            .put(
                &Path::from("/data/testfile1"),
                PutPayload::from_bytes(payload.clone()),
            )
            .await?;
        let get_result = object_store.get(&Path::from("/data/testfile1")).await?;

        let entry = super::DiskCacheEntry {
            root_folder: test_cache_folder,
            location: Path::from("/data/testfile1"),
            part_size: 1024,
        };
        let object_size_hint = entry.save_result(get_result).await?;
        assert_eq!(object_size_hint, 1024 * 3);
        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 4);
        assert_eq!(entry.read_part(0).await?, Some(payload.slice(0..1024)));
        assert_eq!(entry.read_part(1).await?, Some(payload.slice(1024..2048)));
        assert_eq!(entry.read_part(2).await?, Some(payload.slice(2048..3072)));

        let evict_part_path = entry.make_part_path(3, false);
        std::fs::remove_file(evict_part_path).unwrap();
        assert_eq!(entry.read_part(3).await?, None);

        let cached_parts = entry.cached_parts().await?;
        assert_eq!(cached_parts.len(), 3);
        Ok(())
    }

    #[test]
    fn test_split_range_into_parts() {
        let object_store = object_store::memory::InMemory::new();
        let test_cache_folder = new_test_cache_folder();
        let cached_store = CachedObjectStore {
            root_folder: test_cache_folder,
            object_store: Arc::new(object_store),
            part_size: 1024,
        };

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
                input: (Some(GetRange::Bounded(128..1024)), 2),
                expect: vec![(0, 128..1024)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024 + 12)), 2),
                expect: vec![(0, 128..1024), (1, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(128..1024 * 2 + 12)), 2),
                expect: vec![(0, 128..1024), (1, 0..1024), (2, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(1024 * 2..1024 * 3 + 12)), 2),
                expect: vec![(2, 0..1024), (3, 0..12)],
            },
            Test {
                input: (Some(GetRange::Bounded(1024 * 2 - 2..1024 * 3 + 12)), 2),
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
            let parts = cached_store.split_range_into_parts(t.input.0.clone(), t.input.1);
            assert_eq!(parts, t.expect, "input: {:?}", t.input);
        }
    }
}
