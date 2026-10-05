//! A cache of open files and a helper for positional reads.

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// A cache of open file descriptors, keyed by filesystem path.
///
/// Uses a `Mutex` protecting an `LruCache` for O(1) lookup, promotion, and
/// eviction. Each read specifies its own offset, so threads can share a
/// handle without a lock for each file. On Unix, these reads do not move
/// the file cursor.
#[derive(Clone)]
pub struct FileHandleCache {
    inner: Arc<std::sync::Mutex<LruCache<std::path::PathBuf, Arc<std::fs::File>>>>,
}

impl std::fmt::Debug for FileHandleCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().expect("lock should not be poisoned");
        f.debug_struct("FileHandleCache")
            .field("len", &inner.len())
            .field("cap", &inner.cap())
            .finish()
    }
}

impl FileHandleCache {
    /// Creates a cache that retains at most `max_handles` open files.
    ///
    /// # Panics
    ///
    /// Panics if `max_handles` is zero.
    pub fn new(max_handles: usize) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(LruCache::new(
                NonZeroUsize::new(max_handles).expect("max_handles must be > 0"),
            ))),
        }
    }

    /// Returns the number of handles retained by the cache.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Returns whether the cache retains no handles.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Look up a cached file handle, or open the file and cache it.
    /// Returns `Ok(None)` if the file does not exist on disk.
    /// The returned `Arc` keeps the file open after its cache entry is evicted.
    pub fn get_or_open(
        &self,
        path: &std::path::Path,
    ) -> Result<Option<Arc<std::fs::File>>, std::io::Error> {
        let mut cache = self.inner.lock().expect("lock should not be poisoned");
        if let Some(handle) = cache.get(path) {
            if Self::is_valid(handle, path) {
                return Ok(Some(handle.clone()));
            }
            // Stale entry — remove it so we reopen below.
            cache.pop(path);
        }

        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };

        let handle = Arc::new(file);

        cache.push(path.to_path_buf(), handle.clone());
        Ok(Some(handle))
    }

    /// Check whether a cached file descriptor still refers to a live file.
    ///
    /// On Unix an unlinked file keeps its data accessible through open fds,
    /// but `fstat` will report `nlink == 0`. This single in-kernel syscall is
    /// much cheaper than a full `open` and lets us detect deleted or replaced
    /// files without a TOCTOU-prone path `stat`.
    ///
    /// On non-Unix platforms (e.g. Windows), we fall back to checking whether
    /// the path still exists on disk.
    #[cfg(unix)]
    fn is_valid(handle: &std::fs::File, _path: &std::path::Path) -> bool {
        use std::os::unix::fs::MetadataExt;
        handle.metadata().is_ok_and(|m| m.nlink() > 0)
    }

    #[cfg(not(unix))]
    fn is_valid(_handle: &std::fs::File, path: &std::path::Path) -> bool {
        path.exists()
    }

    /// Remove a cached handle, e.g. after eviction or after a write replaces
    /// the file (since the cached fd would still reference the old inode).
    pub fn invalidate(&self, path: &std::path::Path) {
        let mut cache = self.inner.lock().expect("lock should not be poisoned");
        cache.pop(path);
    }
}

/// Reads exactly `buf.len()` bytes at `offset`.
///
/// Concurrent readers can share the file because each read specifies its offset.
/// On Unix, reads do not move the file cursor. On Windows, they do.
pub fn read_exact_at_offset(
    file: &std::fs::File,
    buf: &mut [u8],
    offset: u64,
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut bytes_read = 0;
        while bytes_read < buf.len() {
            let n = file.seek_read(&mut buf[bytes_read..], offset + bytes_read as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            bytes_read += n;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::io::Seek;

    #[test]
    fn should_reuse_handles_across_clones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        let cache = FileHandleCache::new(2);
        assert!(cache.get_or_open(&path).unwrap().is_none());
        assert!(cache.is_empty());

        std::fs::write(&path, b"contents").unwrap();
        let first = cache.get_or_open(&path).unwrap().unwrap();
        let second = cache.clone().get_or_open(&path).unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn should_evict_least_recently_used_handle_and_keep_active_readers() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHandleCache::new(2);
        let paths = ["a", "b", "c"].map(|name| {
            let path = dir.path().join(name);
            std::fs::write(&path, name).unwrap();
            path
        });
        let a = cache.get_or_open(&paths[0]).unwrap().unwrap();
        let b = cache.get_or_open(&paths[1]).unwrap().unwrap();
        assert!(Arc::ptr_eq(
            &a,
            &cache.get_or_open(&paths[0]).unwrap().unwrap()
        ));
        cache.get_or_open(&paths[2]).unwrap().unwrap();
        assert_eq!(cache.len(), 2);
        assert!(!Arc::ptr_eq(
            &b,
            &cache.get_or_open(&paths[1]).unwrap().unwrap()
        ));

        let mut buf = [0];
        read_exact_at_offset(&b, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"b");
    }

    #[test]
    fn should_read_concurrently_at_independent_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, b"0123456789").unwrap();
        let cache = FileHandleCache::new(1);
        let handle = cache.get_or_open(&path).unwrap().unwrap();

        std::thread::scope(|scope| {
            for offset in 0..10 {
                let handle = &handle;
                scope.spawn(move || {
                    for _ in 0..100 {
                        let mut buf = [0];
                        read_exact_at_offset(handle, &mut buf, offset).unwrap();
                        assert_eq!(buf[0], b'0' + offset as u8);
                    }
                });
            }
        });
        #[cfg(unix)]
        assert_eq!((&*handle).stream_position().unwrap(), 0);

        let err = read_exact_at_offset(&handle, &mut [0; 2], 9).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn should_reopen_after_invalidation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, b"old").unwrap();
        let cache = FileHandleCache::new(1);
        let old = cache.get_or_open(&path).unwrap().unwrap();

        std::fs::rename(&path, dir.path().join("moved")).unwrap();
        std::fs::write(&path, b"new").unwrap();
        cache.invalidate(&path);
        assert!(cache.is_empty());
        let new = cache.get_or_open(&path).unwrap().unwrap();
        assert!(!Arc::ptr_eq(&old, &new));
        let mut buf = [0; 3];
        read_exact_at_offset(&new, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"new");
    }

    #[cfg(unix)]
    #[test]
    fn should_reopen_unlinked_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        let replacement = dir.path().join("replacement");
        std::fs::write(&path, b"old").unwrap();
        let cache = FileHandleCache::new(1);
        let old = cache.get_or_open(&path).unwrap().unwrap();

        std::fs::write(&replacement, b"new").unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        let new = cache.get_or_open(&path).unwrap().unwrap();
        assert!(!Arc::ptr_eq(&old, &new));
        let mut buf = [0; 3];
        read_exact_at_offset(&new, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"new");

        std::fs::remove_file(&path).unwrap();
        assert!(cache.get_or_open(&path).unwrap().is_none());
        assert!(cache.is_empty());
    }
}
