use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{fmt::Display, io::SeekFrom};

use crate::cached_object_store::stats::CachedObjectStoreStats;
use crate::clock::Clock;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{Attributes, ObjectMeta};
use radix_trie::{Trie, TrieCommon};
use rand::seq::IteratorRandom;
use rand::{distributions::Alphanumeric, Rng};
use tokio::fs::File;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, OnceCell},
};
use tracing::{debug, warn};
use walkdir::WalkDir;

use crate::cached_object_store::storage::{LocalCacheEntry, LocalCacheHead, LocalCacheStorage};

#[derive(Debug)]
pub struct FsCacheStorage {
    root_folder: std::path::PathBuf,
    evictor: Option<Arc<FsCacheEvictor>>,
}

impl FsCacheStorage {
    pub fn new(
        root_folder: std::path::PathBuf,
        max_cache_size_bytes: Option<usize>,
        scan_interval: Option<Duration>,
        stats: Arc<CachedObjectStoreStats>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let evictor = max_cache_size_bytes.map(|max_cache_size_bytes| {
            Arc::new(FsCacheEvictor::new(
                root_folder.clone(),
                max_cache_size_bytes,
                scan_interval,
                stats,
                clock,
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
pub(crate) struct FsCacheEntry {
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
            evictor
                .track_entry_accessed(path.to_path_buf(), buf.len(), true)
                .await;
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
    pub(crate) fn make_part_path(
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
        let mut rng = crate::rand::thread_rng();
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

        // track the part access for evictor
        if let Some(evictor) = &self.evictor {
            evictor
                .track_entry_accessed(part_path.to_path_buf(), self.part_size, false)
                .await;
        }

        // read the part file, and return the bytes
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
    async fn cached_parts(
        &self,
    ) -> object_store::Result<Vec<crate::cached_object_store::storage::PartID>> {
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
                .next_back()
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
        let head_path = Self::make_head_path(self.root_folder.clone(), &self.location);

        // if the head file does not exist, return None
        let head_size_bytes = match fs::metadata(&head_path).await {
            Ok(metadata) => metadata.len() as usize,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(wrap_io_err(err)),
        };

        // track the head access for evictor
        if let Some(evictor) = &self.evictor {
            evictor
                .track_entry_accessed(head_path.to_path_buf(), head_size_bytes, false)
                .await;
        }

        let file = File::open(&head_path).await.map_err(wrap_io_err)?;
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

type FsCacheEvictorWork = (std::path::PathBuf, usize, bool);

/// FsCacheEvictor evicts the cache entries when the cache size exceeds the limit. it is expected to
/// run in the background to avoid blocking the caller, and it'll be triggered whenever a new cache entry
/// is added.
#[derive(Debug)]
struct FsCacheEvictor {
    root_folder: std::path::PathBuf,
    max_cache_size_bytes: usize,
    scan_interval: Option<Duration>,
    tx: tokio::sync::mpsc::Sender<FsCacheEvictorWork>,
    rx: Mutex<Option<tokio::sync::mpsc::Receiver<FsCacheEvictorWork>>>,
    background_evict_handle: OnceCell<tokio::task::JoinHandle<()>>,
    background_scan_handle: OnceCell<tokio::task::JoinHandle<()>>,
    stats: Arc<CachedObjectStoreStats>,
    clock: Arc<dyn Clock>,
}

impl FsCacheEvictor {
    pub fn new(
        root_folder: std::path::PathBuf,
        max_cache_size_bytes: usize,
        scan_interval: Option<Duration>,
        stats: Arc<CachedObjectStoreStats>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        Self {
            root_folder,
            scan_interval,
            max_cache_size_bytes,
            tx,
            rx: Mutex::new(Some(rx)),
            background_evict_handle: OnceCell::new(),
            background_scan_handle: OnceCell::new(),
            stats,
            clock,
        }
    }

    async fn start(&self) {
        let inner = Arc::new(FsCacheEvictorInner::new(
            self.root_folder.clone(),
            self.max_cache_size_bytes,
            self.stats.clone(),
        ));

        let guard = self.rx.lock();
        let rx = guard.await.take().expect("evictor already started");

        // scan the cache folder (defaults as every 1 hour) to keep the in-memory cache_entries eventually
        // consistent with the cache folder.
        self.background_scan_handle
            .set(tokio::spawn(Self::background_scan(
                inner.clone(),
                self.scan_interval,
            )))
            .ok();

        // start the background evictor task, it'll be triggered whenever a new cache entry is added
        self.background_evict_handle
            .set(tokio::spawn(Self::background_evict(
                inner,
                rx,
                self.clock.clone(),
            )))
            .ok();
    }

    async fn started(&self) -> bool {
        self.rx.lock().await.is_none()
    }

    async fn background_evict(
        inner: Arc<FsCacheEvictorInner>,
        mut rx: tokio::sync::mpsc::Receiver<FsCacheEvictorWork>,
        clock: Arc<dyn Clock>,
    ) {
        loop {
            match rx.recv().await {
                Some((path, bytes, evict)) => {
                    inner
                        .track_entry_accessed(path, bytes, clock.now_systime(), evict)
                        .await;
                }
                None => return,
            }
        }
    }

    async fn background_scan(inner: Arc<FsCacheEvictorInner>, scan_interval: Option<Duration>) {
        inner.clone().scan_entries(true).await;

        if let Some(scan_interval) = scan_interval {
            loop {
                tokio::time::sleep(scan_interval).await;
                inner.clone().scan_entries(true).await;
            }
        }
    }

    pub async fn track_entry_accessed(&self, path: std::path::PathBuf, bytes: usize, evict: bool) {
        if !self.started().await {
            return;
        }

        self.tx.send((path, bytes, evict)).await.ok();
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
    track_lock: Mutex<()>,
    cache_entries: Mutex<Trie<std::path::PathBuf, (SystemTime, usize)>>,
    cache_size_bytes: AtomicU64,
    stats: Arc<CachedObjectStoreStats>,
}

impl FsCacheEvictorInner {
    pub fn new(
        root_folder: std::path::PathBuf,
        max_cache_size_bytes: usize,
        stats: Arc<CachedObjectStoreStats>,
    ) -> Self {
        Self {
            root_folder,
            batch_factor: 10,
            max_cache_size_bytes,
            track_lock: Mutex::new(()),
            cache_entries: Mutex::new(Trie::new()),
            cache_size_bytes: AtomicU64::new(0_u64),
            stats,
        }
    }

    // scan the cache folder, and load the cache entries into the in memory trie cache_entries.
    // this function is only called on start up, and it's expected to run interleavely with
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

            self.track_entry_accessed(path, bytes, atime, evict).await;
        }
    }

    /// track the cache entry access, and evict the cache files when the cache size exceeds the limit if evict is true,
    /// return the bytes of the evicted files. please note that track_entry_accessed might be called concurrently from
    /// the rescanner and evictor tasks, it's expected to be wrapped with a lock to ensure the serial execution.
    pub async fn track_entry_accessed(
        &self,
        path: std::path::PathBuf,
        bytes: usize,
        accessed_time: SystemTime,
        evict: bool,
    ) -> usize {
        let _track_guard = self.track_lock.lock().await;

        // record the new cache entry into the cache_entries, and increase the cache_size_bytes.
        // NOTE: only increase the cache_size_bytes if the entry is not already in the cache_entries.
        {
            let mut guard = self.cache_entries.lock().await;
            if guard.insert(path.clone(), (accessed_time, bytes)).is_none() {
                self.cache_size_bytes
                    .fetch_add(bytes as u64, Ordering::SeqCst);
            }
        }

        // record the metrics
        self.stats
            .object_store_cache_keys
            .set(self.cache_entries.lock().await.len() as u64);
        self.stats
            .object_store_cache_bytes
            .set(self.cache_size_bytes.load(Ordering::Relaxed));

        if !evict {
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

        // if the file is not found, still try to remove it from the cache_entries, and decrease the cache_size_bytes.
        // this might happen when the file is removed by other processes, but the cache_entries is not updated yet.
        if let Err(err) = tokio::fs::remove_file(&target).await {
            warn!("evictor: failed to remove the cache file: {}", err);
            if err.kind() != std::io::ErrorKind::NotFound {
                return 0;
            }
        }

        debug!(
            "evictor: evicted cache file: {:?}, bytes: {}",
            target, target_bytes
        );

        // remove the entry from the cache_entries, and decrease the cache_size_bytes
        // NOTE: only decrease the cache_size_bytes if the entry is actually removed from the cache_entries.
        {
            let mut guard = self.cache_entries.lock().await;
            if guard.remove(&target).is_some() {
                self.cache_size_bytes
                    .fetch_sub(target_bytes as u64, Ordering::SeqCst);
            }
        }

        // sync the metrics after eviction
        self.stats
            .object_store_cache_evicted_bytes
            .add(target_bytes as u64);
        self.stats.object_store_cache_evicted_keys.inc();
        self.stats
            .object_store_cache_keys
            .set(self.cache_entries.lock().await.len() as u64);
        self.stats
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
        let mut rng = crate::rand::thread_rng();

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

fn wrap_io_err(err: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "cached_object_store",
        source: Box::new(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::StatRegistry;
    use crate::test_utils::gen_rand_bytes;
    use filetime::FileTime;
    use std::{io::Write, sync::atomic::Ordering, time::SystemTime};

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

    #[tokio::test]
    async fn test_evictor() {
        let temp_dir = tempfile::Builder::new()
            .prefix("objstore_cache_test_evictor_")
            .tempdir()
            .unwrap();
        let registry = StatRegistry::new();

        let mut evictor = FsCacheEvictorInner::new(
            temp_dir.path().to_path_buf(),
            1024 * 2,
            Arc::new(CachedObjectStoreStats::new(&registry)),
        );
        evictor.batch_factor = 2;

        let path0 = gen_rand_file(temp_dir.path(), "file0", 1024);
        let evicted = evictor
            .track_entry_accessed(path0, 1024, SystemTime::now(), true)
            .await;
        assert_eq!(evicted, 0);

        let path1 = gen_rand_file(temp_dir.path(), "file1", 1024);
        let evicted = evictor
            .track_entry_accessed(path1, 1024, SystemTime::now(), true)
            .await;
        assert_eq!(evicted, 0);

        let path2 = gen_rand_file(temp_dir.path(), "file2", 1024);
        let evicted = evictor
            .track_entry_accessed(path2, 1024, SystemTime::now(), true)
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
        let registry = StatRegistry::new();
        let evictor = Arc::new(FsCacheEvictorInner::new(
            temp_dir.path().to_path_buf(),
            1024 * 2,
            Arc::new(CachedObjectStoreStats::new(&registry)),
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

    #[tokio::test]
    async fn test_evictor_rescan() {
        let temp_dir = tempfile::Builder::new()
            .prefix("objstore_cache_test_evictor_")
            .tempdir()
            .unwrap();
        let registry = StatRegistry::new();

        let evictor = Arc::new(FsCacheEvictorInner::new(
            temp_dir.path().to_path_buf(),
            1024 * 2,
            Arc::new(CachedObjectStoreStats::new(&registry)),
        ));

        gen_rand_file(temp_dir.path(), "file0", 1024);
        gen_rand_file(temp_dir.path(), "file1", 1025);

        // rescan two times, the cache size should be 2049 unchanged
        evictor.clone().scan_entries(false).await;
        let cache_size_bytes = evictor.cache_size_bytes.load(Ordering::SeqCst);
        assert_eq!(cache_size_bytes, 2049);
        evictor.clone().scan_entries(false).await;
        let cache_size_bytes = evictor.cache_size_bytes.load(Ordering::SeqCst);
        assert_eq!(cache_size_bytes, 2049);
    }
}
