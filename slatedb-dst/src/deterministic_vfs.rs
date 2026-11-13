//! An in-memory [`Vfs`] for the local mirror in DST scenarios.
//!
//! `StdVfs` runs filesystem calls on Tokio's blocking pool, outside the seeded
//! current-thread runtime. This breaks determinism. `DeterministicVfs` keeps
//! every file in memory. Each call yields once before it runs, so other tasks
//! can run between two calls, as with a real filesystem.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use slatedb_mirror::{Vfs, VfsEntry, VfsLock, VfsWriter};
use tokio::task::yield_now;

/// The contents of one file. A writer keeps a reference, so it can write to a
/// file after a rename or a remove, as on a POSIX filesystem. The mirror does
/// not rely on this: it closes each writer before it renames or removes the
/// file.
type Inode = Arc<Mutex<Vec<u8>>>;

#[derive(Debug, Default)]
struct State {
    dirs: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, Inode>,
    locks: BTreeSet<PathBuf>,
}

impl State {
    /// Fails with `NotADirectory` if an ancestor of `path` is a file.
    fn check_ancestors(&self, path: &Path) -> io::Result<()> {
        match path
            .ancestors()
            .skip(1)
            .find(|dir| self.files.contains_key(*dir))
        {
            Some(file) => Err(fs_error(io::ErrorKind::NotADirectory, file, "is a file")),
            None => Ok(()),
        }
    }

    /// Fails if the parent directory of `path` can't hold a new entry: it is
    /// missing, or an ancestor of `path` is a file.
    fn check_parent(&self, path: &Path) -> io::Result<()> {
        self.check_ancestors(path)?;
        match path.parent() {
            Some(parent) if !self.dirs.contains(parent) => Err(not_found(parent)),
            _ => Ok(()),
        }
    }

    /// Fails with `IsADirectory` if `path` is a directory.
    fn check_not_dir(&self, path: &Path) -> io::Result<()> {
        if self.dirs.contains(path) {
            return Err(fs_error(
                io::ErrorKind::IsADirectory,
                path,
                "is a directory",
            ));
        }
        Ok(())
    }
}

/// An in-memory [`Vfs`]. Clones share the same files. `list` returns files in
/// name order.
#[derive(Debug, Clone, Default)]
pub struct DeterministicVfs {
    state: Arc<Mutex<State>>,
}

impl DeterministicVfs {
    pub fn new() -> Self {
        Self::default()
    }

    fn inode(&self, path: &Path) -> io::Result<Inode> {
        self.state
            .lock()
            .files
            .get(path)
            .cloned()
            .ok_or_else(|| not_found(path))
    }
}

#[async_trait]
impl Vfs for DeterministicVfs {
    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        yield_now().await;
        let mut state = self.state.lock();
        if state.files.contains_key(path) {
            return Err(fs_error(io::ErrorKind::AlreadyExists, path, "is a file"));
        }
        state.check_ancestors(path)?;
        for dir in path.ancestors() {
            state.dirs.insert(dir.to_path_buf());
        }
        Ok(())
    }

    async fn lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        yield_now().await;
        let mut state = self.state.lock();
        state.check_parent(path)?;
        state.check_not_dir(path)?;
        if !state.locks.insert(path.to_path_buf()) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("{} is locked", path.display()),
            ));
        }
        state.files.entry(path.to_path_buf()).or_default();
        Ok(Box::new(DeterministicVfsLock {
            state: Arc::clone(&self.state),
            path: path.to_path_buf(),
        }))
    }

    async fn list(&self, dir: &Path) -> io::Result<Vec<VfsEntry>> {
        yield_now().await;
        let state = self.state.lock();
        if !state.dirs.contains(dir) {
            return Err(not_found(dir));
        }
        Ok(state
            .files
            .iter()
            .filter(|(path, _)| path.parent() == Some(dir))
            .filter_map(|(path, inode)| {
                Some(VfsEntry {
                    name: path.file_name()?.to_str()?.to_string(),
                    size: inode.lock().len() as u64,
                })
            })
            .collect())
    }

    async fn read_range(&self, path: &Path, range: Range<u64>) -> io::Result<Bytes> {
        yield_now().await;
        let inode = self.inode(path)?;
        let contents = inode.lock();
        let (start, end) = (range.start as usize, range.end as usize);
        if start > end || end > contents.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{} is shorter than {range:?}", path.display()),
            ));
        }
        Ok(Bytes::copy_from_slice(&contents[start..end]))
    }

    async fn read(&self, path: &Path) -> io::Result<Bytes> {
        yield_now().await;
        let inode = self.inode(path)?;
        let contents = inode.lock();
        Ok(Bytes::copy_from_slice(&contents))
    }

    async fn create(&self, path: &Path) -> io::Result<Box<dyn VfsWriter>> {
        yield_now().await;
        let mut state = self.state.lock();
        state.check_parent(path)?;
        if state.files.contains_key(path) || state.dirs.contains(path) {
            return Err(fs_error(io::ErrorKind::AlreadyExists, path, "exists"));
        }
        let inode = Inode::default();
        state.files.insert(path.to_path_buf(), Arc::clone(&inode));
        Ok(Box::new(DeterministicVfsWriter { inode }))
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        yield_now().await;
        let mut state = self.state.lock();
        if !state.files.contains_key(from) {
            return Err(not_found(from));
        }
        state.check_parent(to)?;
        state.check_not_dir(to)?;
        let inode = state.files.remove(from).expect("checked above");
        state.files.insert(to.to_path_buf(), inode);
        Ok(())
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        yield_now().await;
        let mut state = self.state.lock();
        state.check_not_dir(path)?;
        state
            .files
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| not_found(path))
    }

    async fn warm(&self, _path: &Path) -> io::Result<bool> {
        // Files are in memory, so there is nothing to prepare.
        Ok(false)
    }
}

#[derive(Debug)]
struct DeterministicVfsLock {
    state: Arc<Mutex<State>>,
    path: PathBuf,
}

impl VfsLock for DeterministicVfsLock {}

impl Drop for DeterministicVfsLock {
    fn drop(&mut self) {
        self.state.lock().locks.remove(&self.path);
    }
}

#[derive(Debug)]
struct DeterministicVfsWriter {
    inode: Inode,
}

#[async_trait]
impl VfsWriter for DeterministicVfsWriter {
    async fn write(&mut self, bytes: Bytes) -> io::Result<()> {
        yield_now().await;
        self.inode.lock().extend_from_slice(&bytes);
        Ok(())
    }

    async fn finish(&mut self) -> io::Result<()> {
        yield_now().await;
        Ok(())
    }
}

fn not_found(path: &Path) -> io::Error {
    fs_error(io::ErrorKind::NotFound, path, "does not exist")
}

fn fs_error(kind: io::ErrorKind, path: &Path, reason: &str) -> io::Error {
    io::Error::new(kind, format!("{} {reason}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb_mirror::StdVfs;

    #[tokio::test]
    async fn should_write_read_list_rename_and_remove_files() {
        let vfs = DeterministicVfs::new();
        let dir = Path::new("/mirror");
        vfs.create_dir_all(dir).await.unwrap();

        let mut writer = vfs.create(&dir.join("b.tmp")).await.unwrap();
        writer.write(Bytes::from_static(b"hello")).await.unwrap();
        writer.finish().await.unwrap();
        assert_eq!(
            vfs.create(&dir.join("b.tmp")).await.unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        vfs.rename(&dir.join("b.tmp"), &dir.join("b"))
            .await
            .unwrap();
        vfs.create(&dir.join("a")).await.unwrap();

        assert_eq!(
            vfs.list(dir).await.unwrap(),
            vec![
                VfsEntry {
                    name: "a".to_string(),
                    size: 0
                },
                VfsEntry {
                    name: "b".to_string(),
                    size: 5
                },
            ]
        );
        assert_eq!(vfs.read_range(&dir.join("b"), 1..4).await.unwrap(), "ell");
        assert_eq!(
            vfs.read_range(&dir.join("b"), 1..6)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );

        vfs.remove(&dir.join("b")).await.unwrap();
        assert_eq!(
            vfs.read(&dir.join("b")).await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn should_hold_lock_until_dropped() {
        let vfs = DeterministicVfs::new();
        vfs.create_dir_all(Path::new("/mirror")).await.unwrap();
        let path = Path::new("/mirror/LOCK");
        let lock = vfs.lock(path).await.unwrap();
        assert_eq!(
            vfs.lock(path).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(lock);
        vfs.lock(path).await.unwrap();
    }

    /// Makes the calls that a real filesystem rejects in `root`, an empty
    /// directory, and returns the error kind of each call.
    async fn rejected_call_errors(vfs: &dyn Vfs, root: &Path) -> Vec<Option<io::ErrorKind>> {
        let (dir, file) = (root.join("dir"), root.join("file"));
        let missing = root.join("missing").join("file");
        let under_file = file.join("file");
        vfs.create_dir_all(&dir).await.unwrap();
        vfs.create(&file).await.unwrap().finish().await.unwrap();

        // The error kind differs between Linux and macOS.
        assert!(vfs.remove(&dir).await.is_err());
        [
            vfs.create_dir_all(&file).await,
            vfs.create_dir_all(&under_file).await,
            vfs.create(&dir).await.map(drop),
            vfs.create(&missing).await.map(drop),
            vfs.create(&under_file).await.map(drop),
            vfs.lock(&dir).await.map(drop),
            vfs.lock(&missing).await.map(drop),
            vfs.rename(&file, &dir).await,
            vfs.rename(&file, &missing).await,
        ]
        .into_iter()
        .map(|result| result.err().map(|err| err.kind()))
        .collect()
    }

    #[tokio::test]
    async fn should_reject_the_calls_that_std_vfs_rejects() {
        let tempdir = tempfile::tempdir().unwrap();
        let expected = rejected_call_errors(&StdVfs::new(), tempdir.path()).await;
        assert!(expected.iter().all(Option::is_some));

        let vfs = DeterministicVfs::new();
        let root = Path::new("/root");
        vfs.create_dir_all(root).await.unwrap();
        assert_eq!(rejected_call_errors(&vfs, root).await, expected);
    }
}
