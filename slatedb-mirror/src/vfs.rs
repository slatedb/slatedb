//! Local filesystem access for the mirror.
//!
//! The mirror does all of its local I/O through [`Vfs`]. [`StdVfs`] is the
//! default. Other implementations (io_uring, simulation) can be passed to
//! `ObjectStoreMirrorBuilder::with_vfs`.

use std::fmt::Debug;
use std::fs::TryLockError;
use std::io;
use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb_common::file_handle_cache::{read_exact_at_offset, FileHandleCache};
use tokio::io::AsyncWriteExt;

/// A regular file in a [`Vfs::list`] result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VfsEntry {
    /// The file name, without its directory.
    pub name: String,
    /// The file size in bytes.
    pub size: u64,
}

/// The local filesystem operations the mirror needs.
///
/// A `Vfs` doesn't cache, evict, or know about object stores. It only moves
/// bytes to and from local files.
#[async_trait]
pub trait Vfs: Debug + Send + Sync + 'static {
    /// Creates `path` and any missing parent directories.
    async fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Takes an exclusive lock on `path`, creating the file if it doesn't
    /// exist. The lock is held until the returned guard is dropped, and must
    /// be released if the process dies. Fails with
    /// [`io::ErrorKind::WouldBlock`] if someone else holds it. The file is left
    /// in place when the lock is released.
    async fn lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>>;

    /// Lists the regular files directly in `dir`. Subdirectories and names
    /// that aren't valid UTF-8 are skipped.
    async fn list(&self, dir: &Path) -> io::Result<Vec<VfsEntry>>;

    /// Reads `range` from `path`. Fails with [`io::ErrorKind::UnexpectedEof`]
    /// if the file ends before `range.end`.
    async fn read_range(&self, path: &Path, range: Range<u64>) -> io::Result<Bytes>;

    /// Reads all of `path`.
    async fn read(&self, path: &Path) -> io::Result<Bytes>;

    /// Creates a new file at `path` and returns a writer for it. Fails with
    /// [`io::ErrorKind::AlreadyExists`] if `path` exists.
    async fn create(&self, path: &Path) -> io::Result<Box<dyn VfsWriter>>;

    /// Renames `from` to `to`, atomically replacing `to` if it exists.
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Removes the file at `path`. Fails with [`io::ErrorKind::NotFound`] if it
    /// doesn't exist.
    async fn remove(&self, path: &Path) -> io::Result<()>;
}

/// Writes a file created by [`Vfs::create`], in order.
#[async_trait]
pub trait VfsWriter: Debug + Send {
    /// Appends `bytes` to the file.
    async fn write(&mut self, bytes: Bytes) -> io::Result<()>;

    /// Flushes buffered writes and waits for pending writes to complete.
    /// The mirror calls this once, before renaming the file into place.
    /// This does not guarantee that the contents survive a machine crash.
    async fn finish(&mut self) -> io::Result<()>;
}

/// An exclusive lock taken by [`Vfs::lock`]. Dropping it releases the lock.
pub trait VfsLock: Debug + Send + Sync {}

/// A [`Vfs`] backed by the standard filesystem.
///
/// Range reads reuse open files and run in one blocking task per read.
/// Clones share the cache of open files.
/// The cache retains at most 1,000 open files by default.
/// Writes are flushed before publication, without syncing them to disk.
#[derive(Debug, Clone)]
pub struct StdVfs {
    file_handle_cache: FileHandleCache,
}

impl Default for StdVfs {
    fn default() -> Self {
        Self {
            file_handle_cache: FileHandleCache::new(1000),
        }
    }
}

impl StdVfs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of open files retained by the cache.
    ///
    /// # Panics
    ///
    /// Panics if `max_open_file_handles` is zero.
    pub fn with_max_open_file_handles(mut self, max_open_file_handles: usize) -> Self {
        self.file_handle_cache = FileHandleCache::new(max_open_file_handles);
        self
    }
}

#[async_trait]
impl Vfs for StdVfs {
    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        tokio::fs::create_dir_all(path).await
    }

    async fn lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?
            .into_std()
            .await;
        match file.try_lock() {
            Ok(()) => Ok(Box::new(StdVfsLock { _file: file })),
            Err(TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("`{}` is locked by another process", path.display()),
            )),
            Err(TryLockError::Error(err)) => Err(err),
        }
    }

    async fn list(&self, dir: &Path) -> io::Result<Vec<VfsEntry>> {
        let mut entries = Vec::new();
        let mut read_dir = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                // Removed between read_dir and metadata.
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            if metadata.is_file() {
                entries.push(VfsEntry {
                    name,
                    size: metadata.len(),
                });
            }
        }
        Ok(entries)
    }

    async fn read_range(&self, path: &Path, range: Range<u64>) -> io::Result<Bytes> {
        let len = usize::try_from(range.end.saturating_sub(range.start))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range is too large"))?;
        let path = path.to_path_buf();
        let file_handle_cache = self.file_handle_cache.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || {
            let handle = file_handle_cache.try_get_or_open(&path)?;
            let mut buf = vec![0; len];
            read_exact_at_offset(&handle, &mut buf, range.start)?;
            Ok(Bytes::from(buf))
        })
        .await?
    }

    async fn read(&self, path: &Path) -> io::Result<Bytes> {
        tokio::fs::read(path).await.map(Bytes::from)
    }

    async fn create(&self, path: &Path) -> io::Result<Box<dyn VfsWriter>> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await?;
        Ok(Box::new(StdVfsWriter { file }))
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let from = from.to_path_buf();
        let to = to.to_path_buf();
        let file_handle_cache = self.file_handle_cache.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || {
            std::fs::rename(&from, &to)?;
            // Keep invalidation in this task so cancellation cannot skip it.
            file_handle_cache.invalidate(&from);
            file_handle_cache.invalidate(&to);
            Ok(())
        })
        .await?
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        let path = path.to_path_buf();
        let file_handle_cache = self.file_handle_cache.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || {
            let result = std::fs::remove_file(&path);
            file_handle_cache.invalidate(&path);
            result
        })
        .await?
    }
}

#[derive(Debug)]
struct StdVfsLock {
    // The OS releases the lock when the file is closed.
    _file: std::fs::File,
}

impl VfsLock for StdVfsLock {}

#[derive(Debug)]
struct StdVfsWriter {
    file: tokio::fs::File,
}

#[async_trait]
impl VfsWriter for StdVfsWriter {
    async fn write(&mut self, bytes: Bytes) -> io::Result<()> {
        self.file.write_all(&bytes).await
    }

    async fn finish(&mut self) -> io::Result<()> {
        self.file.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_write_read_and_list_files() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = StdVfs::new();
        let path = dir.path().join("a");

        let mut writer = vfs.create(&path).await.unwrap();
        writer.write(Bytes::from_static(b"hello ")).await.unwrap();
        writer.write(Bytes::from_static(b"world")).await.unwrap();
        writer.finish().await.unwrap();

        assert_eq!(vfs.read(&path).await.unwrap(), "hello world");
        assert_eq!(vfs.read_range(&path, 6..11).await.unwrap(), "world");
        let err = vfs.read_range(&path, 6..12).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        vfs.create_dir_all(&dir.path().join("sub")).await.unwrap();
        assert_eq!(
            vfs.list(dir.path()).await.unwrap(),
            vec![VfsEntry {
                name: "a".to_string(),
                size: 11
            }]
        );
    }

    #[tokio::test]
    async fn should_not_create_over_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = StdVfs::new();
        let path = dir.path().join("a");
        vfs.create(&path).await.unwrap();
        let err = vfs.create(&path).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[tokio::test]
    async fn should_rename_over_and_remove_files() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = StdVfs::new();
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        for (path, contents) in [(&a, "new"), (&b, "old")] {
            let mut writer = vfs.create(path).await.unwrap();
            writer.write(Bytes::from(contents)).await.unwrap();
            writer.finish().await.unwrap();
        }

        // Keep the old file linked so the cache needs explicit invalidation.
        std::fs::hard_link(&b, dir.path().join("old-link")).unwrap();
        assert_eq!(vfs.read_range(&a, 0..3).await.unwrap(), "new");
        assert_eq!(vfs.read_range(&b, 0..3).await.unwrap(), "old");
        let reader = vfs.clone();

        vfs.rename(&a, &b).await.unwrap();
        assert_eq!(reader.read_range(&b, 0..3).await.unwrap(), "new");
        let err = reader.read_range(&a, 0..3).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);

        let mut writer = vfs.create(&a).await.unwrap();
        writer.write(Bytes::from_static(b"fresh")).await.unwrap();
        writer.finish().await.unwrap();
        assert_eq!(reader.read_range(&a, 0..5).await.unwrap(), "fresh");

        std::fs::hard_link(&b, dir.path().join("new-link")).unwrap();
        vfs.remove(&b).await.unwrap();
        let err = reader.read_range(&b, 0..3).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        let err = vfs.remove(&b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn should_read_ranges_concurrently_through_shared_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, b"0123456789").unwrap();
        let vfs = StdVfs::new().with_max_open_file_handles(2);
        let mut tasks = tokio::task::JoinSet::new();
        for offset in 0..10 {
            let vfs = vfs.clone();
            let path = path.clone();
            tasks.spawn(async move {
                for _ in 0..100 {
                    let bytes = vfs.read_range(&path, offset..offset + 1).await.unwrap();
                    assert_eq!(bytes[0], b'0' + offset as u8);
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(vfs.file_handle_cache.len(), 1);

        assert!(vfs.read_range(&path, 10..10).await.unwrap().is_empty());
        let err = vfs.read_range(&path, 9..11).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn should_hold_lock_until_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = StdVfs::new();
        let path = dir.path().join("LOCK");

        let lock = vfs.lock(&path).await.unwrap();
        let err = vfs.lock(&path).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        drop(lock);
        vfs.lock(&path).await.unwrap();
        assert!(path.exists());
    }
}
