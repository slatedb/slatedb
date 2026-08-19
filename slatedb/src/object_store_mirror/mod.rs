//! A whole-file local mirror for compacted SST objects.

mod vfs;

pub use vfs::{StdVfs, Vfs, VfsLock};

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io;
use std::path::{Path as FsPath, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{stream, StreamExt, TryStreamExt};
use md5::{Digest, Md5};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyOptions, Extensions, GetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use slatedb_txn_obj::ObjectCodec;
use tokio::sync::Semaphore;

use crate::compactions_store::CompactionsStore;
use crate::db_state::SsTableId;
use crate::flatbuffer_types::FlatBufferManifestCodec;
use crate::manifest::Manifest;
use crate::object_store_tag::ObjectStoreCallTag;
use crate::paths::PathResolver;
use crate::single_flight::SingleFlight;
use crate::utils::spawn_bg_task;

const DEFAULT_DOWNLOAD_CONCURRENCY: usize = 8;
const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(600);
const COMPACTIONS_POLL_INTERVAL: Duration = Duration::from_secs(60);
const LOCK_FILE: &str = "LOCK";

type BackgroundFuture = Pin<Box<dyn Future<Output = object_store::Result<()>> + Send>>;

/// An error caused by the local side of [`ObjectStoreMirror`].
///
/// These errors are terminal: retrying the object-store operation cannot repair
/// a missing mirror entry, a full disk, or an invalid mirror directory.
#[derive(Debug, thiserror::Error)]
#[error("local cache error: {message}")]
pub struct LocalCacheError {
    message: String,
}

impl LocalCacheError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub(crate) fn is_local_cache_error(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::Generic { source, .. }
            if source.downcast_ref::<LocalCacheError>().is_some()
    )
}

fn local_error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "object_store_mirror",
        source: Box::new(LocalCacheError::new(message)),
    }
}

fn local_io_error(operation: &str, path: &FsPath, error: io::Error) -> object_store::Error {
    local_error(format!(
        "{operation} failed for {}: {error}",
        path.display()
    ))
}

fn mirror_error(error: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "object_store_mirror",
        source: Box::new(error),
    }
}

fn boxed_mirror_error(error: Box<dyn std::error::Error + Send + Sync>) -> object_store::Error {
    object_store::Error::Generic {
        store: "object_store_mirror",
        source: error,
    }
}

/// Builder for [`ObjectStoreMirror`].
pub struct ObjectStoreMirrorBuilder {
    local_dir: PathBuf,
    object_store: Arc<dyn ObjectStore>,
    vfs: Arc<dyn Vfs>,
    system_clock: Arc<dyn SystemClock>,
    download_concurrency: usize,
    gc_interval: Option<Duration>,
}

impl ObjectStoreMirrorBuilder {
    /// Replaces the default [`StdVfs`].
    pub fn with_vfs(mut self, vfs: Arc<dyn Vfs>) -> Self {
        self.vfs = vfs;
        self
    }

    /// Replaces the default [`DefaultSystemClock`].
    pub fn with_system_clock(mut self, system_clock: Arc<dyn SystemClock>) -> Self {
        self.system_clock = system_clock;
        self
    }

    /// Sets the maximum number of concurrent downloads used for manifest
    /// warming, `.compactions` prefetching, and refetches. The default is 8.
    pub fn with_download_concurrency(mut self, concurrency: usize) -> Self {
        self.download_concurrency = concurrency;
        self
    }

    /// Sets the interval at which the mirror scans remote storage to GC
    /// obsolete local SSTs. The default is `Some(Duration::from_secs(600))`.
    /// Passing `None` disables periodic GC but not metadata-driven GC.
    pub fn with_gc_interval(mut self, interval: Option<Duration>) -> Self {
        self.gc_interval = interval;
        self
    }

    /// Validates the configuration, acquires the cache-directory lock, cleans
    /// invalid local entries, and starts background workers. The database root
    /// is detected from the first `.manifest` read or write.
    pub async fn build(self) -> Result<Arc<ObjectStoreMirror>, crate::Error> {
        if self.download_concurrency == 0 {
            return Err(crate::Error::invalid(
                "object-store mirror download concurrency must be greater than zero".to_string(),
            ));
        }
        if self.gc_interval == Some(Duration::ZERO) {
            return Err(crate::Error::invalid(
                "object-store mirror GC interval must be greater than zero".to_string(),
            ));
        }

        self.vfs
            .create_dir_all(&self.local_dir)
            .await
            .map_err(|e| local_io_error("create directory", &self.local_dir, e))
            .map_err(crate::error::SlateDBError::from)?;
        let lock_path = self.local_dir.join(LOCK_FILE);
        let lock = self
            .vfs
            .try_lock(&lock_path)
            .await
            .map_err(|e| local_io_error("lock", &lock_path, e))
            .map_err(crate::error::SlateDBError::from)?;

        let state = Arc::new(MirrorState {
            local_dir: self.local_dir,
            object_store: self.object_store,
            vfs: self.vfs,
            system_clock: self.system_clock,
            _lock: lock,
            download_concurrency: self.download_concurrency,
            download_semaphore: Arc::new(Semaphore::new(self.download_concurrency)),
            gc_interval: self.gc_interval,
            next_temp: AtomicU64::new(0),
            downloads: SingleFlight::new(),
            metadata: Mutex::new(HashMap::new()),
            parents: Mutex::new(HashMap::new()),
            root: Mutex::new(None),
            latest_manifest: Mutex::new(None),
            manifest_cache: Mutex::new(HashMap::new()),
        });
        state
            .load_local_state()
            .await
            .map_err(crate::error::SlateDBError::from)?;
        state.start_background_tasks();

        Ok(Arc::new(ObjectStoreMirror { state }))
    }
}

/// An [`ObjectStore`] that keeps every tagged compacted SST in a local,
/// whole-file mirror.
pub struct ObjectStoreMirror {
    state: Arc<MirrorState>,
}

impl ObjectStoreMirror {
    pub fn builder(
        local_dir: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
    ) -> ObjectStoreMirrorBuilder {
        ObjectStoreMirrorBuilder {
            local_dir: local_dir.into(),
            object_store,
            vfs: Arc::new(StdVfs),
            system_clock: Arc::new(DefaultSystemClock::new()),
            download_concurrency: DEFAULT_DOWNLOAD_CONCURRENCY,
            gc_interval: Some(DEFAULT_GC_INTERVAL),
        }
    }
}

impl Debug for ObjectStoreMirror {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreMirror")
            .field("local_dir", &self.state.local_dir)
            .field("object_store", &self.state.object_store.to_string())
            .finish_non_exhaustive()
    }
}

impl Display for ObjectStoreMirror {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObjectStoreMirror({}, {})",
            self.state.local_dir.display(),
            self.state.object_store
        )
    }
}

#[derive(Clone)]
struct CachedMetadata {
    meta: ObjectMeta,
    attributes: Attributes,
}

#[derive(Serialize, Deserialize)]
struct StoredMetadata {
    location: String,
    last_modified: DateTime<Utc>,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
    attributes: Vec<StoredAttribute>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum StoredAttribute {
    ContentDisposition(String),
    ContentEncoding(String),
    ContentLanguage(String),
    ContentType(String),
    CacheControl(String),
    StorageClass(String),
    Metadata(String, String),
}

impl StoredMetadata {
    fn from_cached(cached: &CachedMetadata) -> object_store::Result<Self> {
        let mut attributes = Vec::with_capacity(cached.attributes.len());
        for (key, value) in cached.attributes.iter() {
            let value = value.as_ref().to_string();
            #[allow(unreachable_patterns)]
            let stored = match key {
                Attribute::ContentDisposition => StoredAttribute::ContentDisposition(value),
                Attribute::ContentEncoding => StoredAttribute::ContentEncoding(value),
                Attribute::ContentLanguage => StoredAttribute::ContentLanguage(value),
                Attribute::ContentType => StoredAttribute::ContentType(value),
                Attribute::CacheControl => StoredAttribute::CacheControl(value),
                Attribute::StorageClass => StoredAttribute::StorageClass(value),
                Attribute::Metadata(key) => {
                    StoredAttribute::Metadata(key.as_ref().to_string(), value)
                }
                _ => return Err(local_error("unsupported object attribute")),
            };
            attributes.push(stored);
        }
        Ok(Self {
            location: cached.meta.location.to_string(),
            last_modified: cached.meta.last_modified,
            size: cached.meta.size,
            e_tag: cached.meta.e_tag.clone(),
            version: cached.meta.version.clone(),
            attributes,
        })
    }

    fn into_cached(self) -> object_store::Result<CachedMetadata> {
        let location = Path::parse(self.location).map_err(mirror_error)?;
        let mut attributes = Attributes::with_capacity(self.attributes.len());
        for attribute in self.attributes {
            let (key, value) = match attribute {
                StoredAttribute::ContentDisposition(value) => {
                    (Attribute::ContentDisposition, value)
                }
                StoredAttribute::ContentEncoding(value) => (Attribute::ContentEncoding, value),
                StoredAttribute::ContentLanguage(value) => (Attribute::ContentLanguage, value),
                StoredAttribute::ContentType(value) => (Attribute::ContentType, value),
                StoredAttribute::CacheControl(value) => (Attribute::CacheControl, value),
                StoredAttribute::StorageClass(value) => (Attribute::StorageClass, value),
                StoredAttribute::Metadata(key, value) => {
                    (Attribute::Metadata(Cow::Owned(key)), value)
                }
            };
            attributes.insert(key, AttributeValue::from(value));
        }
        Ok(CachedMetadata {
            meta: ObjectMeta {
                location,
                last_modified: self.last_modified,
                size: self.size,
                e_tag: self.e_tag,
                version: self.version,
            },
            attributes,
        })
    }
}

struct LocalPaths {
    hash: String,
    parent: String,
    data: PathBuf,
    meta: PathBuf,
}

struct ManifestState {
    id: u64,
    references: HashSet<Path>,
}

struct PreparedManifest {
    id: u64,
    manifest: Manifest,
    checkpoint_ids: HashSet<u64>,
    references: HashSet<Path>,
}

struct MirrorState {
    local_dir: PathBuf,
    object_store: Arc<dyn ObjectStore>,
    vfs: Arc<dyn Vfs>,
    system_clock: Arc<dyn SystemClock>,
    _lock: Box<dyn VfsLock>,
    download_concurrency: usize,
    download_semaphore: Arc<Semaphore>,
    gc_interval: Option<Duration>,
    next_temp: AtomicU64,
    downloads: SingleFlight<(Path, bool), ()>,
    metadata: Mutex<HashMap<Path, CachedMetadata>>,
    parents: Mutex<HashMap<String, String>>,
    root: Mutex<Option<Path>>,
    latest_manifest: Mutex<Option<ManifestState>>,
    manifest_cache: Mutex<HashMap<u64, Manifest>>,
}

impl MirrorState {
    fn local_paths(&self, location: &Path) -> object_store::Result<LocalPaths> {
        let parent = location
            .parent()
            .ok_or_else(|| local_error(format!("object path has no parent: {location}")))?;
        let filename = location
            .filename()
            .ok_or_else(|| local_error(format!("object path has no filename: {location}")))?;
        let parent = parent.to_string();
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let digest = Md5::digest(parent.as_bytes());
        let mut hash = String::with_capacity(32);
        for byte in digest.as_slice() {
            hash.push(HEX[(byte >> 4) as usize] as char);
            hash.push(HEX[(byte & 0x0f) as usize] as char);
        }
        let data = self.local_dir.join(format!("{hash}.{filename}"));
        let meta = self.local_dir.join(format!("{hash}.{filename}.meta"));
        Ok(LocalPaths {
            hash,
            parent,
            data,
            meta,
        })
    }

    fn temp_path(&self, data: &FsPath) -> PathBuf {
        let suffix = self.next_temp.fetch_add(1, Ordering::Relaxed);
        let mut path = data.as_os_str().to_os_string();
        path.push(format!(".{suffix}"));
        PathBuf::from(path)
    }

    fn register_parent(&self, hash: &str, parent: &str) -> object_store::Result<()> {
        let mut parents = self.parents.lock();
        match parents.get(hash) {
            Some(existing) if existing != parent => Err(local_error(format!(
                "object path hash collision between {existing} and {parent}"
            ))),
            Some(_) => Ok(()),
            None => {
                parents.insert(hash.to_string(), parent.to_string());
                Ok(())
            }
        }
    }

    async fn remove_if_exists(&self, path: &FsPath) -> object_store::Result<()> {
        match self.vfs.remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(local_io_error("remove", path, error)),
        }
    }

    async fn load_local_state(&self) -> object_store::Result<()> {
        let entries = self
            .vfs
            .read_dir(&self.local_dir)
            .await
            .map_err(|e| local_io_error("list directory", &self.local_dir, e))?;
        let mut data_files = HashMap::new();
        let mut meta_files = HashMap::new();

        for path in entries {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name == LOCK_FILE {
                continue;
            }
            if let Some(base) = name.strip_suffix(".meta") {
                if base.ends_with(".sst") {
                    meta_files.insert(base.to_string(), path);
                }
                continue;
            }
            if name.ends_with(".sst") {
                data_files.insert(name.to_string(), path);
                continue;
            }
            if let Some((base, suffix)) = name.rsplit_once('.') {
                if base.ends_with(".sst") && suffix.parse::<u64>().is_ok() {
                    self.remove_if_exists(&path).await?;
                }
            }
        }

        let bases = data_files
            .keys()
            .chain(meta_files.keys())
            .cloned()
            .collect::<HashSet<_>>();
        let mut metadata = HashMap::new();
        let mut parents = HashMap::new();
        for base in bases {
            let data = data_files.get(&base);
            let meta = meta_files.get(&base);
            let (Some(data), Some(meta)) = (data, meta) else {
                if let Some(data) = data {
                    self.remove_if_exists(data).await?;
                }
                if let Some(meta) = meta {
                    self.remove_if_exists(meta).await?;
                }
                continue;
            };

            let valid = async {
                let bytes = self
                    .vfs
                    .read(meta)
                    .await
                    .map_err(|e| local_io_error("read metadata", meta, e))?;
                let stored: StoredMetadata =
                    serde_json::from_slice(&bytes).map_err(mirror_error)?;
                let cached = stored.into_cached()?;
                let paths = self.local_paths(&cached.meta.location)?;
                if paths.data != *data || paths.meta != *meta {
                    return Err(local_error("metadata path does not match local filename"));
                }
                let len = self
                    .vfs
                    .file_len(data)
                    .await
                    .map_err(|e| local_io_error("stat", data, e))?;
                if len != cached.meta.size {
                    return Err(local_error(format!(
                        "local SST size {len} does not match metadata size {}",
                        cached.meta.size
                    )));
                }
                if parents
                    .get(&paths.hash)
                    .is_some_and(|parent| parent != &paths.parent)
                {
                    return Err(local_error("conflicting object path hash mapping"));
                }
                parents.insert(paths.hash, paths.parent);
                Ok::<_, object_store::Error>(cached)
            }
            .await;

            match valid {
                Ok(cached) => {
                    metadata.insert(cached.meta.location.clone(), cached);
                }
                Err(_) => {
                    self.remove_if_exists(data).await?;
                    self.remove_if_exists(meta).await?;
                }
            }
        }

        *self.metadata.lock() = metadata;
        *self.parents.lock() = parents;
        Ok(())
    }

    async fn install_temp(
        &self,
        location: &Path,
        temp: &FsPath,
        cached: CachedMetadata,
    ) -> object_store::Result<()> {
        let paths = self.local_paths(location)?;
        self.register_parent(&paths.hash, &paths.parent)?;
        let stored = StoredMetadata::from_cached(&cached)?;
        let bytes = serde_json::to_vec(&stored).map_err(mirror_error)?;
        let result = async {
            self.vfs
                .write(&paths.meta, Bytes::from(bytes))
                .await
                .map_err(|e| local_io_error("write metadata", &paths.meta, e))?;
            self.vfs
                .rename(temp, &paths.data)
                .await
                .map_err(|e| local_io_error("install SST", &paths.data, e))?;
            self.metadata.lock().insert(location.clone(), cached);
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = self.remove_if_exists(temp).await;
        }
        result
    }

    async fn write_payload_at(
        &self,
        path: &FsPath,
        mut offset: u64,
        payload: PutPayload,
    ) -> object_store::Result<()> {
        for bytes in payload {
            let len = bytes.len() as u64;
            self.vfs
                .write_at(path, offset, bytes)
                .await
                .map_err(|e| local_io_error("write temporary SST", path, e))?;
            offset += len;
        }
        Ok(())
    }

    async fn download(&self, location: &Path, force: bool) -> object_store::Result<()> {
        let location = location.clone();
        self.downloads
            .call((location.clone(), force), || async move {
                if !force && self.metadata.lock().contains_key(&location) {
                    return Ok(());
                }
                let _permit = self
                    .download_semaphore
                    .acquire()
                    .await
                    .map_err(|_| local_error("download semaphore closed"))?;
                let result = self.object_store.get(&location).await?;
                let mut meta = result.meta.clone();
                let attributes = result.attributes.clone();
                let paths = self.local_paths(&location)?;
                let temp = self.temp_path(&paths.data);
                self.vfs
                    .write(&temp, Bytes::new())
                    .await
                    .map_err(|e| local_io_error("create temporary SST", &temp, e))?;
                let mut stream = result.into_stream();
                let download = async {
                    let mut len = 0;
                    while let Some(bytes) = stream.try_next().await? {
                        let chunk_len = bytes.len() as u64;
                        self.vfs
                            .write_at(&temp, len, bytes)
                            .await
                            .map_err(|e| local_io_error("write temporary SST", &temp, e))?;
                        len += chunk_len;
                    }
                    if len != meta.size {
                        return Err(mirror_error(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "downloaded {len} bytes for {location}, expected {}",
                                meta.size
                            ),
                        )));
                    }
                    meta.location = location.clone();
                    self.install_temp(&location, &temp, CachedMetadata { meta, attributes })
                        .await
                }
                .await;
                if download.is_err() {
                    let _ = self.remove_if_exists(&temp).await;
                }
                download
            })
            .await
    }

    async fn warm_paths(&self, paths: HashSet<Path>) -> object_store::Result<()> {
        stream::iter(paths)
            .map(|path| async move { self.download(&path, false).await })
            .buffer_unordered(self.download_concurrency)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    fn manifest_root(location: &Path) -> object_store::Result<Path> {
        location
            .parent()
            .and_then(|manifest_dir| manifest_dir.parent())
            .ok_or_else(|| local_error(format!("invalid manifest path: {location}")))
    }

    fn manifest_id(location: &Path) -> object_store::Result<u64> {
        location
            .filename()
            .and_then(|name| name.split('.').next())
            .ok_or_else(|| local_error(format!("invalid manifest path: {location}")))?
            .parse()
            .map_err(|_| local_error(format!("invalid manifest path: {location}")))
    }

    fn should_process_manifest(&self, location: &Path) -> object_store::Result<bool> {
        let root = Self::manifest_root(location)?;
        let mut known = self.root.lock();
        match known.as_ref() {
            Some(existing) => Ok(existing == &root),
            None => {
                *known = Some(root);
                Ok(true)
            }
        }
    }

    fn referenced_ssts(root: &Path, manifest: &Manifest) -> HashSet<Path> {
        let resolver = PathResolver::new_with_external_ssts(root.clone(), manifest.external_ssts());
        manifest
            .core
            .all_sst_views()
            .filter_map(|view| match view.sst.id {
                SsTableId::Compacted(_) => Some(resolver.sst_path(&view.sst.id)),
                SsTableId::Wal(_) => None,
            })
            .collect()
    }

    async fn read_manifest(&self, root: &Path, id: u64) -> object_store::Result<Manifest> {
        if let Some(manifest) = self.manifest_cache.lock().get(&id).cloned() {
            return Ok(manifest);
        }
        let location = root
            .clone()
            .join("manifest")
            .join(format!("{id:020}.manifest"));
        let bytes = self.object_store.get(&location).await?.bytes().await?;
        let manifest = FlatBufferManifestCodec {}
            .decode(&bytes)
            .map_err(boxed_mirror_error)?;
        self.manifest_cache.lock().insert(id, manifest.clone());
        Ok(manifest)
    }

    async fn prepare_manifest(
        &self,
        location: &Path,
        bytes: &Bytes,
    ) -> object_store::Result<Option<PreparedManifest>> {
        let root = Self::manifest_root(location)?;
        let id = Self::manifest_id(location)?;
        if self
            .latest_manifest
            .lock()
            .as_ref()
            .is_some_and(|latest| id <= latest.id)
        {
            return Ok(None);
        }
        let manifest = FlatBufferManifestCodec {}
            .decode(bytes)
            .map_err(boxed_mirror_error)?;

        let mut references = Self::referenced_ssts(&root, &manifest);
        self.warm_paths(references.clone()).await?;

        if self
            .latest_manifest
            .lock()
            .as_ref()
            .is_some_and(|latest| id <= latest.id)
        {
            return Ok(None);
        }

        let now = self.system_clock.now();
        let checkpoint_ids = manifest
            .core
            .checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.expire_time.is_none_or(|expires| expires > now))
            .map(|checkpoint| checkpoint.manifest_id)
            .collect::<HashSet<_>>();
        for checkpoint_id in &checkpoint_ids {
            let checkpoint = if *checkpoint_id == id {
                manifest.clone()
            } else {
                self.read_manifest(&root, *checkpoint_id).await?
            };
            references.extend(Self::referenced_ssts(&root, &checkpoint));
        }

        Ok(Some(PreparedManifest {
            id,
            manifest,
            checkpoint_ids,
            references,
        }))
    }

    fn apply_manifest(self: &Arc<Self>, prepared: PreparedManifest) {
        let PreparedManifest {
            id,
            manifest,
            mut checkpoint_ids,
            references,
        } = prepared;

        let removed = {
            let mut latest = self.latest_manifest.lock();
            if latest.as_ref().is_some_and(|latest| id <= latest.id) {
                return;
            }
            let removed = latest
                .as_ref()
                .map(|old| {
                    old.references
                        .difference(&references)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            *latest = Some(ManifestState { id, references });
            removed
        };

        self.manifest_cache.lock().insert(id, manifest);
        checkpoint_ids.insert(id);
        self.manifest_cache
            .lock()
            .retain(|manifest_id, _| checkpoint_ids.contains(manifest_id));

        if !removed.is_empty() {
            let state = Arc::clone(self);
            tokio::spawn(async move {
                for path in removed {
                    if let Err(error) = state.remove_local(&path).await {
                        log::warn!(
                            "failed to reclaim mirrored SST [path={}, error={:?}]",
                            path,
                            error
                        );
                    }
                }
            });
        }
    }

    async fn remove_local(&self, location: &Path) -> object_store::Result<()> {
        let paths = self.local_paths(location)?;
        let (data, meta) = futures::future::join(
            self.remove_if_exists(&paths.data),
            self.remove_if_exists(&paths.meta),
        )
        .await;
        self.metadata.lock().remove(location);
        data?;
        meta?;
        Ok(())
    }

    async fn local_get(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let cached = self
            .metadata
            .lock()
            .get(location)
            .cloned()
            .ok_or_else(|| local_error(format!("mirrored SST is missing: {location}")))?;
        if options
            .version
            .as_ref()
            .is_some_and(|version| Some(version) != cached.meta.version.as_ref())
        {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(io::Error::new(
                    io::ErrorKind::NotFound,
                    "object version is not mirrored",
                )),
            });
        }
        options.check_preconditions(&cached.meta)?;
        let range = match options.range {
            Some(range) => range.as_range(cached.meta.size).map_err(mirror_error)?,
            None => 0..cached.meta.size,
        };
        let (payload, range) = if options.head {
            (GetResultPayload::Stream(stream::empty().boxed()), 0..0)
        } else {
            let paths = self.local_paths(location)?;
            let bytes = self
                .vfs
                .read_range(&paths.data, range.clone())
                .await
                .map_err(|e| local_io_error("read SST", &paths.data, e))?;
            (
                GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
                range,
            )
        };
        Ok(GetResult {
            payload,
            meta: cached.meta,
            range,
            attributes: cached.attributes,
            extensions: Extensions::new(),
        })
    }

    async fn mirrored_put(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let size = payload.content_length() as u64;
        let attributes = opts.attributes.clone();
        let paths = self.local_paths(location)?;
        let temp = self.temp_path(&paths.data);
        self.vfs
            .write(&temp, Bytes::new())
            .await
            .map_err(|e| local_io_error("create temporary SST", &temp, e))?;
        let local_payload = payload.clone();
        let (remote, local) = futures::future::join(
            self.object_store.put_opts(location, payload, opts),
            self.write_payload_at(&temp, 0, local_payload),
        )
        .await;
        let result = match remote {
            Ok(result) => result,
            Err(error) => {
                let _ = self.remove_if_exists(&temp).await;
                return Err(error);
            }
        };
        if let Err(error) = local {
            let _ = self.remove_if_exists(&temp).await;
            return Err(error);
        }
        let cached = self.cached_metadata(location, size, attributes, &result);
        self.install_temp(location, &temp, cached).await?;
        Ok(result)
    }

    fn cached_metadata(
        &self,
        location: &Path,
        size: u64,
        attributes: Attributes,
        result: &PutResult,
    ) -> CachedMetadata {
        CachedMetadata {
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: self.system_clock.now(),
                size,
                e_tag: result.e_tag.clone(),
                version: result.version.clone(),
            },
            attributes,
        }
    }

    async fn prefetch_compactions(&self) -> object_store::Result<()> {
        let root = self.root.lock().clone();
        let Some(root) = root else {
            return Ok(());
        };
        let store = CompactionsStore::new(&root, Arc::clone(&self.object_store));
        let Some(compactions) = store
            .try_read_latest_compactions()
            .await
            .map_err(mirror_error)?
        else {
            return Ok(());
        };
        let resolver = PathResolver::from_root(root);
        let mut paths = HashSet::new();
        for compaction in compactions.recent_compactions() {
            for sst in compaction.output_ssts() {
                if matches!(sst.id, SsTableId::Compacted(_)) {
                    paths.insert(resolver.sst_path(&sst.id));
                }
            }
            for subcompaction in compaction.subcompactions() {
                for sst in subcompaction.output_ssts() {
                    if matches!(sst.id, SsTableId::Compacted(_)) {
                        paths.insert(resolver.sst_path(&sst.id));
                    }
                }
            }
        }
        self.warm_paths(paths).await
    }

    async fn reclaim_remote_absent(&self) -> object_store::Result<()> {
        let snapshot = self.metadata.lock().keys().cloned().collect::<Vec<_>>();
        let mut groups: HashMap<Path, Vec<Path>> = HashMap::new();
        for path in snapshot {
            if let Some(parent) = path.parent() {
                groups.entry(parent).or_default().push(path);
            }
        }
        for (parent, local_paths) in groups {
            let remote = self
                .object_store
                .list(Some(&parent))
                .map_ok(|meta| meta.location)
                .try_collect::<HashSet<_>>()
                .await;
            let Ok(remote) = remote else {
                continue;
            };
            for path in local_paths {
                if !remote.contains(&path) {
                    self.remove_local(&path).await?;
                }
            }
        }
        Ok(())
    }

    fn start_background_tasks(self: &Arc<Self>) {
        Self::spawn_periodic(
            "object_store_mirror_compactions",
            Arc::downgrade(self),
            COMPACTIONS_POLL_INTERVAL,
            |state| Box::pin(async move { state.prefetch_compactions().await }),
        );
        if let Some(interval) = self.gc_interval {
            Self::spawn_periodic(
                "object_store_mirror_gc",
                Arc::downgrade(self),
                interval,
                |state| Box::pin(async move { state.reclaim_remote_absent().await }),
            );
        }
    }

    fn spawn_periodic(
        name: &str,
        state: Weak<Self>,
        duration: Duration,
        operation: fn(Arc<Self>) -> BackgroundFuture,
    ) {
        let _task = spawn_bg_task(
            name.to_string(),
            &tokio::runtime::Handle::current(),
            |_| {},
            async move {
                let Some(system_clock) =
                    state.upgrade().map(|state| Arc::clone(&state.system_clock))
                else {
                    return Ok(());
                };
                let mut ticker = system_clock.ticker(duration);
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let Some(state) = state.upgrade() else {
                        break;
                    };
                    if let Err(error) = operation(state).await {
                        log::warn!("object-store mirror background task failed [error={error:?}]");
                    }
                }
                Ok(())
            },
        );
    }
}

fn payload_to_bytes(payload: &PutPayload) -> Bytes {
    if payload.content_length() == 0 {
        return Bytes::new();
    }
    let mut result = BytesMut::with_capacity(payload.content_length());
    for bytes in payload {
        result.extend_from_slice(bytes);
    }
    result.freeze()
}

fn is_manifest(location: &Path) -> bool {
    location.extension() == Some("manifest")
}

fn is_compacted(tag: Option<ObjectStoreCallTag>) -> bool {
    tag.is_some_and(|tag| matches!(tag.sst_type, crate::db_state::SstType::Compacted))
}

#[async_trait]
impl ObjectStore for ObjectStoreMirror {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if is_manifest(location) {
            let should_process = self.state.should_process_manifest(location)?
                && !options.head
                && options.range.is_none();
            let result = self.state.object_store.get_opts(location, options).await?;
            if !should_process {
                return Ok(result);
            }
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = result.extensions.clone();
            let bytes = result.bytes().await?;
            let prepared = self.state.prepare_manifest(location, &bytes).await?;
            if let Some(prepared) = prepared {
                self.state.apply_manifest(prepared);
            }
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
                meta,
                range,
                attributes,
                extensions,
            });
        }

        let tag = ObjectStoreCallTag::from_extensions(&options.extensions);
        if !is_compacted(tag) {
            return self.state.object_store.get_opts(location, options).await;
        }
        if tag.is_some_and(|tag| tag.retry.is_some()) {
            self.state.download(location, true).await?;
        }
        self.state.local_get(location, options).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if is_manifest(location) {
            let should_process = self.state.should_process_manifest(location)?;
            let prepared = if should_process {
                let manifest_bytes = payload_to_bytes(&payload);
                self.state
                    .prepare_manifest(location, &manifest_bytes)
                    .await?
            } else {
                None
            };
            let result = self
                .state
                .object_store
                .put_opts(location, payload, opts)
                .await?;
            if let Some(prepared) = prepared {
                self.state.apply_manifest(prepared);
            }
            return Ok(result);
        }
        let tag = ObjectStoreCallTag::from_extensions(&opts.extensions);
        if is_compacted(tag) {
            self.state.mirrored_put(location, payload, opts).await
        } else {
            self.state
                .object_store
                .put_opts(location, payload, opts)
                .await
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let tag = ObjectStoreCallTag::from_extensions(&opts.extensions);
        if !is_compacted(tag) {
            return self
                .state
                .object_store
                .put_multipart_opts(location, opts)
                .await;
        }
        let attributes = opts.attributes.clone();
        let inner = self
            .state
            .object_store
            .put_multipart_opts(location, opts)
            .await?;
        let paths = self.state.local_paths(location)?;
        let temp = self.state.temp_path(&paths.data);
        self.state
            .vfs
            .write(&temp, Bytes::new())
            .await
            .map_err(|e| local_io_error("create temporary SST", &temp, e))?;
        Ok(Box::new(MirroringMultipartUpload {
            inner,
            state: Arc::clone(&self.state),
            location: location.clone(),
            temp,
            len: 0,
            attributes,
        }))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let state = Arc::clone(&self.state);
        locations
            .then(move |location| {
                let state = Arc::clone(&state);
                async move {
                    let location = location?;
                    let (remote, local) = futures::future::join(
                        state.object_store.delete(&location),
                        state.remove_local(&location),
                    )
                    .await;
                    remote?;
                    local?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.state.object_store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.state.object_store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.state.object_store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.state.object_store.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.state.object_store.rename_opts(from, to, options).await
    }
}

struct MirroringMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    state: Arc<MirrorState>,
    location: Path,
    temp: PathBuf,
    len: u64,
    attributes: Attributes,
}

impl Debug for MirroringMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirroringMultipartUpload")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl MultipartUpload for MirroringMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let offset = self.len;
        self.len += data.content_length() as u64;
        let local_data = data.clone();
        let remote = self.inner.put_part(data);
        let state = Arc::clone(&self.state);
        let temp = self.temp.clone();
        Box::pin(async move {
            let (remote, local) =
                futures::future::join(remote, state.write_payload_at(&temp, offset, local_data))
                    .await;
            remote?;
            local?;
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let result = match self.inner.complete().await {
            Ok(result) => result,
            Err(error) => {
                let _ = self.state.remove_if_exists(&self.temp).await;
                return Err(error);
            }
        };
        let cached =
            self.state
                .cached_metadata(&self.location, self.len, self.attributes.clone(), &result);
        self.state
            .install_temp(&self.location, &self.temp, cached)
            .await?;
        Ok(result)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let (remote, local) =
            futures::future::join(self.inner.abort(), self.state.remove_if_exists(&self.temp))
                .await;
        remote?;
        local
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::error::RetryReason;
    use crate::object_store_tag::TableStoreKind;
    use object_store::memory::InMemory;
    use object_store::GetRange;
    use slatedb_common::clock::MockSystemClock;
    use tempfile::TempDir;

    fn compacted_tag() -> ObjectStoreCallTag {
        ObjectStoreCallTag::new(TableStoreKind::Main, crate::db_state::SstType::Compacted)
    }

    fn compacted_put_options() -> PutOptions {
        PutOptions {
            extensions: compacted_tag().into(),
            ..PutOptions::default()
        }
    }

    fn compacted_get_options() -> GetOptions {
        GetOptions {
            extensions: compacted_tag().into(),
            ..GetOptions::default()
        }
    }

    fn compacted_multipart_options() -> PutMultipartOptions {
        PutMultipartOptions {
            extensions: compacted_tag().into(),
            ..PutMultipartOptions::default()
        }
    }

    #[tokio::test]
    async fn compacted_put_is_read_locally_and_recovers_on_restart() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let location = Path::from("db/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let mirror = ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
            .with_gc_interval(None)
            .with_system_clock(Arc::new(MockSystemClock::new()))
            .build()
            .await
            .unwrap();
        let mut put_options = compacted_put_options();
        put_options.attributes.insert(
            Attribute::ContentType,
            AttributeValue::from("application/octet-stream"),
        );

        let put_result = mirror
            .put_opts(
                &location,
                PutPayload::from_iter([Bytes::from_static(b"mir"), Bytes::from_static(b"rored")]),
                put_options,
            )
            .await
            .unwrap();
        let mut head_options = compacted_get_options();
        head_options.head = true;
        let head = mirror.get_opts(&location, head_options).await.unwrap();
        assert_eq!(head.meta.last_modified, DateTime::<Utc>::UNIX_EPOCH);
        assert_eq!(head.meta.size, 8);
        assert_eq!(head.meta.e_tag, put_result.e_tag);
        assert_eq!(head.meta.version, put_result.version);
        assert_eq!(
            head.attributes.get(&Attribute::ContentType),
            Some(&AttributeValue::from("application/octet-stream"))
        );
        remote.delete(&location).await.unwrap();

        let bytes = mirror
            .get_opts(&location, compacted_get_options())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"mirrored"));
        assert!(
            mirror.get(&location).await.is_err(),
            "untagged reads bypass"
        );

        drop(mirror);
        let reopened = ObjectStoreMirror::builder(local.path(), remote)
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
        let bytes = reopened
            .get_opts(&location, compacted_get_options())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"mirrored"));
    }

    #[tokio::test]
    async fn compacted_cache_miss_does_not_fall_back_to_remote() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let location = Path::from("db/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        remote
            .put(&location, PutPayload::from_static(b"remote-only"))
            .await
            .unwrap();
        let mirror = ObjectStoreMirror::builder(local.path(), remote)
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();

        let error = mirror
            .get_opts(&location, compacted_get_options())
            .await
            .unwrap_err();
        assert!(is_local_cache_error(&error));
    }

    #[tokio::test]
    async fn retry_refetches_the_complete_remote_sst() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let location = Path::from("db/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let mirror = ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
        mirror
            .put_opts(
                &location,
                PutPayload::from_static(b"bad"),
                compacted_put_options(),
            )
            .await
            .unwrap();
        remote
            .put(&location, PutPayload::from_static(b"repaired"))
            .await
            .unwrap();

        let mut tag = compacted_tag();
        tag.retry = Some(RetryReason::CrcMismatch);
        let result = mirror
            .get_opts(
                &location,
                GetOptions {
                    range: Some(GetRange::Bounded(1..5)),
                    extensions: tag.into(),
                    ..GetOptions::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(result, Bytes::from_static(b"epai"));

        remote.delete(&location).await.unwrap();
        let result = mirror
            .get_opts(&location, compacted_get_options())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(result, Bytes::from_static(b"repaired"));
    }

    #[tokio::test]
    async fn failed_retry_keeps_the_existing_local_sst() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let location = Path::from("db/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let mirror = ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
        mirror
            .put_opts(
                &location,
                PutPayload::from_static(b"cached"),
                compacted_put_options(),
            )
            .await
            .unwrap();
        remote.delete(&location).await.unwrap();

        let mut tag = compacted_tag();
        tag.retry = Some(RetryReason::CrcMismatch);
        assert!(mirror
            .get_opts(
                &location,
                GetOptions {
                    extensions: tag.into(),
                    ..GetOptions::default()
                },
            )
            .await
            .is_err());

        assert_eq!(
            mirror
                .get_opts(&location, compacted_get_options())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"cached")
        );
    }

    #[tokio::test]
    async fn multipart_put_is_mirrored() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let location = Path::from("db/compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst");
        let mirror = ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
            .with_gc_interval(None)
            .with_system_clock(Arc::new(MockSystemClock::new()))
            .build()
            .await
            .unwrap();
        let mut multipart_options = compacted_multipart_options();
        multipart_options.attributes.insert(
            Attribute::ContentType,
            AttributeValue::from("application/octet-stream"),
        );
        let mut upload = mirror
            .put_multipart_opts(&location, multipart_options)
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_iter([
                Bytes::from_static(b"fir"),
                Bytes::from_static(b"st"),
            ]))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_iter([
                Bytes::from_static(b"sec"),
                Bytes::from_static(b"ond"),
            ]))
            .await
            .unwrap();
        let put_result = upload.complete().await.unwrap();
        remote.delete(&location).await.unwrap();

        let result = mirror
            .get_opts(&location, compacted_get_options())
            .await
            .unwrap();
        assert_eq!(result.meta.last_modified, DateTime::<Utc>::UNIX_EPOCH);
        assert_eq!(result.meta.size, 11);
        assert_eq!(result.meta.e_tag, put_result.e_tag);
        assert_eq!(result.meta.version, put_result.version);
        assert_eq!(
            result.attributes.get(&Attribute::ContentType),
            Some(&AttributeValue::from("application/octet-stream"))
        );
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"firstsecond"));
    }

    #[tokio::test]
    async fn cache_directory_is_exclusively_locked() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let mirror = ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
        assert!(
            ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
                .with_gc_interval(None)
                .build()
                .await
                .is_err()
        );
        drop(mirror);
        ObjectStoreMirror::builder(local.path(), remote)
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn startup_removes_incomplete_files() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let incomplete = local
            .path()
            .join("754128269b532c9827ffa09d3afb6118.01J79C21YKR31J2BS1EFXJZ7MR.sst.7");
        tokio::fs::write(&incomplete, b"partial").await.unwrap();

        ObjectStoreMirror::builder(local.path(), remote)
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
        assert!(!incomplete.exists());
    }

    #[tokio::test]
    async fn manifests_from_other_roots_pass_through() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = TempDir::new().unwrap();
        let primary = Path::from("primary/manifest/00000000000000000001.manifest");
        let other = Path::from("other/manifest/00000000000000000001.manifest");
        remote
            .put(&primary, PutPayload::from_static(b"primary"))
            .await
            .unwrap();
        let mirror = ObjectStoreMirror::builder(local.path(), remote)
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();

        mirror.head(&primary).await.unwrap();
        mirror
            .put(&other, PutPayload::from_static(b"other"))
            .await
            .unwrap();

        assert_eq!(
            mirror.get(&other).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"other")
        );
    }

    #[tokio::test]
    async fn older_and_equal_manifests_are_not_reprocessed() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let older = Path::from("db/manifest/00000000000000000001.manifest");
        let equal = Path::from("db/manifest/00000000000000000002.manifest");
        for location in [&older, &equal] {
            remote
                .put(location, PutPayload::from_static(b"not a manifest"))
                .await
                .unwrap();
        }
        let local = TempDir::new().unwrap();
        let mirror = ObjectStoreMirror::builder(local.path(), remote)
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
        mirror.state.latest_manifest.lock().replace(ManifestState {
            id: 2,
            references: HashSet::new(),
        });

        for location in [&equal, &older] {
            assert_eq!(
                mirror.get(location).await.unwrap().bytes().await.unwrap(),
                Bytes::from_static(b"not a manifest")
            );
        }
    }

    #[tokio::test]
    async fn manifest_put_does_not_commit_when_warming_fails() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_root = Path::from("source");
        let db = Db::open(source_root.clone(), Arc::clone(&remote))
            .await
            .unwrap();
        db.put(b"key", b"value").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let source_manifest_dir = source_root.clone().join("manifest");
        let source_manifest = remote
            .list(Some(&source_manifest_dir))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .max_by_key(|meta| meta.location.clone())
            .unwrap()
            .location;
        let manifest_bytes = remote
            .get(&source_manifest)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let target_manifest = Path::from("target/manifest/00000000000000000001.manifest");
        let local = TempDir::new().unwrap();
        let mirror = ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();

        assert!(mirror
            .put_opts(
                &target_manifest,
                PutPayload::from_bytes(manifest_bytes),
                PutOptions::from(object_store::PutMode::Create),
            )
            .await
            .is_err());
        assert!(matches!(
            remote.head(&target_manifest).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn opening_a_database_warms_its_manifest_ssts() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = Path::from("mirror-warm-test");
        let db = Db::open(root.clone(), Arc::clone(&remote)).await.unwrap();
        db.put(b"key", b"value").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let local = TempDir::new().unwrap();
        let mirror = ObjectStoreMirror::builder(local.path(), Arc::clone(&remote))
            .with_gc_interval(None)
            .build()
            .await
            .unwrap();
        let db = Db::open(root.clone(), mirror).await.unwrap();

        let compacted = root.clone().join("compacted");
        let remote_ssts = remote
            .list(Some(&compacted))
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!remote_ssts.is_empty());
        for path in remote_ssts {
            remote.delete(&path).await.unwrap();
        }

        assert_eq!(
            db.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        db.close().await.unwrap();
    }
}
