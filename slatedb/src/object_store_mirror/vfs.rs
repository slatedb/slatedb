use std::fmt::Debug;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// A lifetime guard for an exclusive VFS lock.
pub trait VfsLock: Send + Sync {}

impl VfsLock for std::fs::File {}

/// The local filesystem operations used by
/// [`ObjectStoreMirror`](super::ObjectStoreMirror).
#[async_trait]
pub trait Vfs: Send + Sync + Debug + 'static {
    async fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    async fn read(&self, path: &Path) -> io::Result<Bytes>;
    async fn read_range(&self, path: &Path, range: Range<u64>) -> io::Result<Bytes>;
    async fn write(&self, path: &Path, data: Bytes) -> io::Result<()>;
    async fn write_at(&self, path: &Path, offset: u64, data: Bytes) -> io::Result<()>;
    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    async fn remove_file(&self, path: &Path) -> io::Result<()>;
    async fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
    async fn file_len(&self, path: &Path) -> io::Result<u64>;
    async fn try_lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>>;
}

/// The standard-filesystem implementation of [`Vfs`].
#[derive(Debug, Default)]
pub struct StdVfs;

#[async_trait]
impl Vfs for StdVfs {
    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        tokio::fs::create_dir_all(path).await
    }

    async fn read(&self, path: &Path) -> io::Result<Bytes> {
        tokio::fs::read(path).await.map(Bytes::from)
    }

    async fn read_range(&self, path: &Path, range: Range<u64>) -> io::Result<Bytes> {
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(io::SeekFrom::Start(range.start)).await?;
        let len = usize::try_from(range.end.saturating_sub(range.start))
            .map_err(|_| io::Error::other("requested local range is too large"))?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes).await?;
        Ok(Bytes::from(bytes))
    }

    async fn write(&self, path: &Path, data: Bytes) -> io::Result<()> {
        tokio::fs::write(path, data).await
    }

    async fn write_at(&self, path: &Path, offset: u64, data: Bytes) -> io::Result<()> {
        let mut file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        file.seek(io::SeekFrom::Start(offset)).await?;
        file.write_all(&data).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        tokio::fs::rename(from, to).await
    }

    async fn remove_file(&self, path: &Path) -> io::Result<()> {
        tokio::fs::remove_file(path).await
    }

    async fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = tokio::fs::read_dir(path).await?;
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            paths.push(entry.path());
        }
        Ok(paths)
    }

    async fn file_len(&self, path: &Path) -> io::Result<u64> {
        tokio::fs::metadata(path)
            .await
            .map(|metadata| metadata.len())
    }

    async fn try_lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.try_lock()?;
        Ok(Box::new(file))
    }
}
