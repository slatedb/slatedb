use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use parking_lot::{Mutex, RwLock};
use slatedb_common::clock::SystemClock;

const DETERMINISTIC_LOCAL_STORE_NAME: &str = "DeterministicLocalFileSystem";

/// A deterministic on-disk object store for the simulation harness.
///
/// All filesystem operations run inline on the current thread. This keeps the
/// harness replay single-threaded even when the backing store is disk-backed.
#[derive(Clone)]
pub struct DeterministicLocalFileSystem {
    state: Arc<DeterministicLocalState>,
}

#[derive(Debug)]
struct DeterministicLocalState {
    root: PathBuf,
    automatic_cleanup: bool,
    attributes: RwLock<HashMap<Path, Attributes>>,
}

impl DeterministicLocalFileSystem {
    pub fn new_with_prefix(prefix: impl AsRef<FsPath>) -> object_store::Result<Self> {
        Self::new_with_prefix_and_cleanup(prefix, false)
    }

    pub fn new_with_prefix_and_cleanup(
        prefix: impl AsRef<FsPath>,
        automatic_cleanup: bool,
    ) -> object_store::Result<Self> {
        let root =
            fs::canonicalize(prefix.as_ref()).expect("failed to canonicalize deterministic root");
        Ok(Self {
            state: Arc::new(DeterministicLocalState {
                root,
                automatic_cleanup,
                attributes: RwLock::new(HashMap::new()),
            }),
        })
    }

    fn path_to_filesystem(&self, location: &Path) -> object_store::Result<PathBuf> {
        is_valid_file_path(location)
            .then_some(())
            .expect("invalid local object-store path");
        Ok(self.prefix_to_filesystem(location))
    }

    fn prefix_to_filesystem(&self, location: &Path) -> PathBuf {
        let mut path = self.state.root.to_path_buf();
        for part in location.parts() {
            path.push(part.as_ref());
        }
        path
    }

    fn filesystem_to_path(&self, location: &FsPath) -> object_store::Result<Path> {
        let relative = location
            .strip_prefix(&self.state.root)
            .expect("failed to strip local object-store root");
        if relative.as_os_str().is_empty() {
            return Ok(Path::default());
        }

        let raw = relative
            .components()
            .map(|component| {
                component
                    .as_os_str()
                    .to_str()
                    .expect("non-unicode path component under local object-store root")
            })
            .collect::<Vec<_>>()
            .join("/");

        Ok(Path::parse(raw).expect("invalid object-store path"))
    }

    fn get_attributes(&self, location: &Path) -> Attributes {
        self.state
            .attributes
            .read()
            .get(location)
            .cloned()
            .unwrap_or_default()
    }

    fn set_attributes(&self, location: &Path, attributes: Attributes) {
        let mut guard = self.state.attributes.write();
        if attributes.is_empty() {
            guard.remove(location);
        } else {
            guard.insert(location.clone(), attributes);
        }
    }

    fn copy_attributes(&self, from: &Path, to: &Path) {
        let attributes = self.get_attributes(from);
        self.set_attributes(to, attributes);
    }

    fn rename_attributes(&self, from: &Path, to: &Path) {
        let mut guard = self.state.attributes.write();
        let attributes = guard.remove(from).unwrap_or_default();
        if attributes.is_empty() {
            guard.remove(to);
        } else {
            guard.insert(to.clone(), attributes);
        }
    }

    fn remove_attributes(&self, location: &Path) {
        self.state.attributes.write().remove(location);
    }

    fn get_result(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let path = self.path_to_filesystem(location)?;
        let (mut file, metadata) = open_file(&path)?;
        let meta = convert_metadata(metadata, location.clone());
        options.check_preconditions(&meta)?;

        let range = match options.range {
            Some(ref range) => range
                .as_range(meta.size)
                .expect("invalid range for local object-store read"),
            None => 0..meta.size,
        };

        let file_metadata = file.metadata().expect("failed to stat object file");
        let file_len = file_metadata.len();
        (range.start < file_len || file_len == 0)
            .then_some(())
            .expect("range start is past end of object file");

        let to_read = range.end.min(file_len).saturating_sub(range.start);
        file.seek(SeekFrom::Start(range.start))
            .expect("failed to seek object file");

        let mut buf = Vec::with_capacity(to_read as usize);
        let read = file
            .take(to_read)
            .read_to_end(&mut buf)
            .expect("failed to read object file") as u64;
        (read == to_read)
            .then_some(())
            .expect("short read from object file");
        let bytes: Bytes = buf.into();

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: self.get_attributes(location),
        })
    }

    fn put_result(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if matches!(opts.mode, PutMode::Update(_)) {
            return Err(object_store::Error::NotImplemented);
        }

        let mode = opts.mode.clone();
        let attributes = opts.attributes.clone();
        let path = self.path_to_filesystem(location)?;
        let (mut file, staging_path) = new_staged_upload(&path)?;
        payload
            .iter()
            .try_for_each(|chunk| file.write_all(chunk))
            .expect("failed to write staged object payload");

        let metadata = file.metadata().expect("failed to stat staged upload");
        let e_tag = Some(get_etag(&metadata));

        let commit_result = match mode {
            PutMode::Overwrite => {
                drop(file);
                fs::rename(&staging_path, &path)
                    .expect("failed to rename staged upload into place");
                Ok(())
            }
            PutMode::Create => match fs::hard_link(&staging_path, &path) {
                Ok(()) => {
                    let _ = fs::remove_file(&staging_path);
                    Ok(())
                }
                Err(source) if source.kind() == ErrorKind::AlreadyExists => {
                    let _ = fs::remove_file(&staging_path);
                    Err(object_store::Error::AlreadyExists {
                        path: path.to_string_lossy().into_owned(),
                        source: Box::new(source),
                    })
                }
                Err(source) => {
                    let _ = fs::remove_file(&staging_path);
                    Result::<(), _>::Err(source)
                        .expect("failed to create object from staged upload");
                    unreachable!()
                }
            },
            PutMode::Update(_) => unreachable!(),
        };

        commit_result?;
        self.set_attributes(location, attributes);

        Ok(PutResult {
            e_tag,
            version: None,
        })
    }

    fn delete_path(&self, location: &Path) -> object_store::Result<()> {
        let path = self.path_to_filesystem(location)?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(source) if source.kind() == ErrorKind::NotFound => {
                return Err(object_store::Error::NotFound {
                    path: path.to_string_lossy().into_owned(),
                    source: Box::new(source),
                });
            }
            Err(source) => {
                Result::<(), _>::Err(source).expect("failed to delete object file");
            }
        }

        if self.state.automatic_cleanup {
            let mut parent = path.parent();
            while let Some(dir) = parent {
                if dir == self.state.root {
                    break;
                }
                match fs::remove_dir(dir) {
                    Ok(()) => parent = dir.parent(),
                    Err(_) => break,
                }
            }
        }

        self.remove_attributes(location);
        Ok(())
    }

    fn collect_objects(&self, prefix: Option<&Path>) -> object_store::Result<Vec<ObjectMeta>> {
        let root = prefix
            .map(|p| self.prefix_to_filesystem(p))
            .unwrap_or_else(|| self.state.root.clone());
        let mut objects = Vec::new();
        self.walk_files(&root, &mut objects)?;
        objects.sort_by(|lhs, rhs| lhs.location.cmp(&rhs.location));
        Ok(objects)
    }

    fn walk_files(&self, root: &FsPath, objects: &mut Vec<ObjectMeta>) -> object_store::Result<()> {
        if !root.exists() {
            return Ok(());
        }

        let mut entries = fs::read_dir(root)
            .expect("failed to read object directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read object directory entry");
        entries.sort_by(|lhs, rhs| lhs.file_name().cmp(&rhs.file_name()));

        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type().expect("failed to read object entry type");
            if file_type.is_dir() {
                self.walk_files(&path, objects)?;
                continue;
            }

            let location = self.filesystem_to_path(&path)?;
            if !is_valid_file_path(&location) {
                continue;
            }

            let metadata = entry.metadata().expect("failed to stat object entry");
            objects.push(self.with_attributes(convert_metadata(metadata, location)));
        }

        Ok(())
    }

    fn with_attributes(&self, meta: ObjectMeta) -> ObjectMeta {
        if self.state.attributes.read().contains_key(&meta.location) {
            meta
        } else {
            meta
        }
    }

    fn list_result(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let root = self.prefix_to_filesystem(&prefix);
        if !root.exists() {
            return Ok(ListResult {
                common_prefixes: Vec::new(),
                objects: Vec::new(),
            });
        }

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        let mut entries = fs::read_dir(&root)
            .expect("failed to read object directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read object directory entry");
        entries.sort_by(|lhs, rhs| lhs.file_name().cmp(&rhs.file_name()));

        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type().expect("failed to read object entry type");
            let location = self.filesystem_to_path(&path)?;
            if file_type.is_dir() {
                common_prefixes.insert(location);
                continue;
            }
            if !is_valid_file_path(&location) {
                continue;
            }
            let metadata = entry.metadata().expect("failed to stat object entry");
            objects.push(convert_metadata(metadata, location));
        }

        objects.sort_by(|lhs, rhs| lhs.location.cmp(&rhs.location));
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }
}

impl fmt::Debug for DeterministicLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            DETERMINISTIC_LOCAL_STORE_NAME,
            self.state.root.display()
        )
    }
}

impl fmt::Display for DeterministicLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            DETERMINISTIC_LOCAL_STORE_NAME,
            self.state.root.display()
        )
    }
}

#[async_trait]
impl ObjectStore for DeterministicLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_result(location, payload, opts)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let path = self.path_to_filesystem(location)?;
        let (file, staging_path) = new_staged_upload(&path)?;
        Ok(Box::new(DeterministicMultipartUpload::new(
            Arc::clone(&self.state),
            location.clone(),
            path,
            staging_path,
            file,
            opts.attributes,
        )))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_result(location, options)
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let path = self.path_to_filesystem(location)?;
        let (_, metadata) = open_file(&path)?;
        Ok(convert_metadata(metadata, location.clone()))
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.delete_path(location)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.collect_objects(prefix) {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(err) => stream::once(async move { Err(err) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let offset = offset.clone();
        match self.collect_objects(prefix) {
            Ok(objects) => stream::iter(
                objects
                    .into_iter()
                    .filter(move |meta| meta.location > offset)
                    .map(Ok),
            )
            .boxed(),
            Err(err) => stream::once(async move { Err(err) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.list_result(prefix)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_path = self.path_to_filesystem(from)?;
        let to_path = self.path_to_filesystem(to)?;
        let (mut file, staging_path) = new_staged_upload(&to_path)?;
        let mut src = match File::open(&from_path) {
            Ok(src) => src,
            Err(source) if source.kind() == ErrorKind::NotFound => {
                return Err(object_store::Error::NotFound {
                    path: from_path.to_string_lossy().into_owned(),
                    source: Box::new(source),
                });
            }
            Err(source) => Result::<File, _>::Err(source).expect("failed to open source object"),
        };
        io::copy(&mut src, &mut file).expect("failed to copy source object into staging file");
        drop(file);
        fs::rename(&staging_path, &to_path)
            .expect("failed to rename copied staging file into place");
        self.copy_attributes(from, to);
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_path = self.path_to_filesystem(from)?;
        let to_path = self.path_to_filesystem(to)?;
        loop {
            match fs::rename(&from_path, &to_path) {
                Ok(()) => {
                    self.rename_attributes(from, to);
                    return Ok(());
                }
                Err(source) if source.kind() == ErrorKind::NotFound => {
                    if from_path.exists() {
                        create_parent_dirs(&to_path)?;
                        continue;
                    }
                    return Err(object_store::Error::NotFound {
                        path: from_path.to_string_lossy().into_owned(),
                        source: Box::new(source),
                    });
                }
                Err(source) => {
                    Result::<(), _>::Err(source).expect("failed to rename object");
                }
            }
        }
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_path = self.path_to_filesystem(from)?;
        let to_path = self.path_to_filesystem(to)?;
        let mut src = match File::open(&from_path) {
            Ok(src) => src,
            Err(source) if source.kind() == ErrorKind::NotFound => {
                return Err(object_store::Error::NotFound {
                    path: from_path.to_string_lossy().into_owned(),
                    source: Box::new(source),
                });
            }
            Err(source) => Result::<File, _>::Err(source).expect("failed to open source object"),
        };
        loop {
            let mut file = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&to_path)
            {
                Ok(file) => file,
                Err(source) if source.kind() == ErrorKind::AlreadyExists => {
                    return Err(object_store::Error::AlreadyExists {
                        path: to_path.to_string_lossy().into_owned(),
                        source: Box::new(source),
                    });
                }
                Err(source) if source.kind() == ErrorKind::NotFound => {
                    create_parent_dirs(&to_path)?;
                    continue;
                }
                Err(source) => {
                    Result::<File, _>::Err(source).expect("failed to create destination object")
                }
            };

            io::copy(&mut src, &mut file).expect("failed to copy source object into destination");
            self.copy_attributes(from, to);
            return Ok(());
        }
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_if_not_exists(from, to).await?;
        self.delete(from).await
    }
}

#[derive(Debug)]
struct DeterministicMultipartUpload {
    state: Arc<DeterministicLocalState>,
    location: Path,
    staged_path: Option<PathBuf>,
    dest_path: PathBuf,
    attributes: Attributes,
    offset: u64,
    file: Arc<Mutex<File>>,
}

impl DeterministicMultipartUpload {
    fn new(
        state: Arc<DeterministicLocalState>,
        location: Path,
        dest_path: PathBuf,
        staged_path: PathBuf,
        file: File,
        attributes: Attributes,
    ) -> Self {
        Self {
            state,
            location,
            staged_path: Some(staged_path),
            dest_path,
            attributes,
            offset: 0,
            file: Arc::new(Mutex::new(file)),
        }
    }
}

#[async_trait]
impl MultipartUpload for DeterministicMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let offset = self.offset;
        self.offset += data.content_length() as u64;
        let file = Arc::clone(&self.file);
        async move {
            let mut file = file.lock();
            file.seek(SeekFrom::Start(offset))
                .expect("failed to seek multipart upload");
            data.iter()
                .try_for_each(|chunk| file.write_all(chunk))
                .expect("failed to write multipart upload");
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let staged_path = self
            .staged_path
            .take()
            .expect("multipart upload is no longer active");
        let metadata = self
            .file
            .lock()
            .metadata()
            .expect("failed to stat multipart upload");
        fs::rename(&staged_path, &self.dest_path)
            .expect("failed to rename completed multipart upload");
        let mut guard = self.state.attributes.write();
        if self.attributes.is_empty() {
            guard.remove(&self.location);
        } else {
            guard.insert(self.location.clone(), self.attributes.clone());
        }
        Ok(PutResult {
            e_tag: Some(get_etag(&metadata)),
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let staged_path = self
            .staged_path
            .take()
            .expect("multipart upload is no longer active");
        fs::remove_file(&staged_path).expect("failed to delete aborted multipart upload");
        Ok(())
    }
}

impl Drop for DeterministicMultipartUpload {
    fn drop(&mut self) {
        if let Some(staged_path) = self.staged_path.take() {
            let _ = fs::remove_file(staged_path);
        }
    }
}

/// Returns whether a local object-store path should be treated as a file entry.
fn is_valid_file_path(path: &Path) -> bool {
    match path.filename() {
        Some(p) => match p.split_once('#') {
            Some((_, suffix)) if !suffix.is_empty() => {
                !suffix.as_bytes().iter().all(|x| x.is_ascii_digit())
            }
            _ => true,
        },
        None => false,
    }
}

/// Creates the parent directory chain for a filesystem path.
fn create_parent_dirs(path: &FsPath) -> object_store::Result<()> {
    let parent = path.parent().expect("path has no parent");
    fs::create_dir_all(parent).expect("failed to create parent directory");
    Ok(())
}

/// Opens a new temporary staging file for a pending write to `base`.
fn new_staged_upload(base: &FsPath) -> object_store::Result<(File, PathBuf)> {
    let mut multipart_id = 1u64;
    loop {
        let suffix = multipart_id.to_string();
        let mut path = base.as_os_str().to_owned();
        path.push("#");
        path.push(&suffix);
        let path: PathBuf = path.into();
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((file, path)),
            Err(source) if source.kind() == ErrorKind::AlreadyExists => {
                multipart_id = multipart_id.wrapping_add(1);
            }
            Err(source) if source.kind() == ErrorKind::NotFound => {
                create_parent_dirs(&path)?;
            }
            Err(source) => Result::<(), _>::Err(source).expect("failed to open staged upload"),
        }
    }
}

/// Opens an object file and returns its metadata, mapping missing files to `NotFound`.
fn open_file(path: &PathBuf) -> object_store::Result<(File, Metadata)> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == ErrorKind::NotFound => {
            return Err(object_store::Error::NotFound {
                path: path.to_string_lossy().into_owned(),
                source: Box::new(source),
            });
        }
        Err(source) => Result::<File, _>::Err(source).expect("failed to open object file"),
    };
    let metadata = file.metadata().expect("failed to stat opened object file");
    if metadata.is_dir() {
        Err(object_store::Error::NotFound {
            path: path.to_string_lossy().into_owned(),
            source: Box::new(io::Error::new(ErrorKind::NotFound, "is directory")),
        })
    } else {
        Ok((file, metadata))
    }
}

/// Converts filesystem metadata into the object-store metadata returned by this backend.
fn convert_metadata(metadata: Metadata, location: Path) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: metadata
            .modified()
            .expect("Modified file time should be supported on this platform")
            .into(),
        size: metadata.len(),
        e_tag: Some(get_etag(&metadata)),
        version: None,
    }
}

/// Builds a deterministic local ETag from inode, mtime, and file size.
fn get_etag(metadata: &Metadata) -> String {
    #[cfg(unix)]
    let inode = std::os::unix::fs::MetadataExt::ino(metadata);

    #[cfg(not(unix))]
    let inode = 0;

    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();
    format!("{inode:x}-{mtime:x}-{size:x}")
}

/// ObjectStore wrapper that overrides metadata times using a provided SystemClock.
/// - Records timestamps for mutating operations (put, copy, rename, delete).
/// - Uses recorded timestamps for `last_modified` in ObjectMeta returned by `head` and `list`.
#[derive(Clone)]
pub struct ClockedObjectStore {
    inner: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
    times: Arc<RwLock<HashMap<Path, DateTime<Utc>>>>,
}

impl ClockedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, clock: Arc<dyn SystemClock>) -> Self {
        Self {
            inner,
            clock,
            times: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a modification for the given path. If the path doesn't exist in the map,
    /// initialize both created and modified to now().
    fn record_modified(&self, path: &Path) -> DateTime<Utc> {
        let now = self.clock.now();
        let mut guard = self.times.write();
        guard
            .entry(path.clone())
            .and_modify(|t| *t = now)
            .or_insert_with(|| now);
        now
    }

    /// Remove any tracked times for this path (after a successful delete or rename).
    fn remove(&self, path: &Path) {
        let mut guard = self.times.write();
        guard.remove(path);
    }

    /// Apply recorded last_modified to ObjectMeta if available.
    fn with_recorded_times(&self, meta: ObjectMeta) -> ObjectMeta {
        let guard = self.times.read();
        if let Some(t) = guard.get(&meta.location) {
            return ObjectMeta {
                last_modified: *t,
                ..meta
            };
        }
        meta
    }
}

impl fmt::Debug for ClockedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClockedObjectStore({})", self.inner)
    }
}

impl fmt::Display for ClockedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClockedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ClockedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let meta = self.inner.head(location).await?;
        Ok(self.with_recorded_times(meta))
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let res = self.inner.put_opts(location, payload, opts).await?;
        self.record_modified(location);
        Ok(res)
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let res = self.inner.put_multipart(location).await;
        self.record_modified(location);
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let res = self.inner.put_multipart_opts(location, opts).await;
        self.record_modified(location);
        res
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await?;
        self.remove(location);
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let times = self.times.clone();
        self.inner
            .list(prefix)
            .map(move |res| match res {
                Ok(meta) => {
                    let guard = times.read();
                    if let Some(t) = guard.get(&meta.location) {
                        return Ok(ObjectMeta {
                            last_modified: *t,
                            ..meta
                        });
                    }
                    Ok(meta)
                }
                Err(e) => Err(e),
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let times = self.times.clone();
        self.inner
            .list_with_offset(prefix, offset)
            .map(move |res| match res {
                Ok(meta) => {
                    let guard = times.read();
                    if let Some(t) = guard.get(&meta.location) {
                        return Ok(ObjectMeta {
                            last_modified: *t,
                            ..meta
                        });
                    }
                    Ok(meta)
                }
                Err(e) => Err(e),
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        let guard = self.times.read();
        result.objects = result
            .objects
            .into_iter()
            .map(|meta| {
                if let Some(t) = guard.get(&meta.location) {
                    ObjectMeta {
                        last_modified: *t,
                        ..meta
                    }
                } else {
                    meta
                }
            })
            .collect();
        Ok(result)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await?;
        self.record_modified(to);
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await?;
        self.remove(from);
        self.record_modified(to);
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await?;
        self.record_modified(to);
        Ok(())
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await?;
        self.remove(from);
        self.record_modified(to);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use slatedb_common::clock::MockSystemClock;
    use tempfile::TempDir;

    fn p(s: &str) -> Path {
        Path::from(s)
    }

    #[tokio::test]
    async fn test_dst_local_file_system_smoke() {
        let dir = TempDir::new().unwrap();
        let store =
            DeterministicLocalFileSystem::new_with_prefix_and_cleanup(dir.path(), true).unwrap();

        store
            .put(&p("dir/a"), PutPayload::from_static(b"abc"))
            .await
            .unwrap();
        store
            .put(&p("dir/b"), PutPayload::from_static(b"def"))
            .await
            .unwrap();

        let bytes = store.get(&p("dir/a")).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"abc");

        let listed: Vec<_> = store.list(Some(&p("dir/"))).try_collect().await.unwrap();
        assert_eq!(
            listed
                .into_iter()
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
            vec!["dir/a".to_string(), "dir/b".to_string()]
        );
    }

    #[tokio::test]
    async fn test_dst_local_file_system_supports_multipart() {
        let dir = TempDir::new().unwrap();
        let store = DeterministicLocalFileSystem::new_with_prefix(dir.path()).unwrap();

        let mut upload = store.put_multipart(&p("large")).await.unwrap();
        upload
            .put_part(PutPayload::from_static(b"hello "))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"world"))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        let bytes = store.get(&p("large")).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_dst_local_file_system_round_trips_attributes() {
        let dir = TempDir::new().unwrap();
        let store = DeterministicLocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let key = object_store::Attribute::Metadata(Cow::Borrowed("slatedbputid"));
        let mut attributes = Attributes::new();
        attributes.insert(key.clone(), "ulid-123".into());

        store
            .put_opts(
                &p("attrs"),
                PutPayload::from_static(b"payload"),
                PutOptions::from(attributes.clone()),
            )
            .await
            .unwrap();

        let result = store
            .get_opts(&p("attrs"), GetOptions::default())
            .await
            .unwrap();
        assert_eq!(result.attributes.get(&key), attributes.get(&key));
    }

    #[tokio::test]
    async fn test_put_and_head_use_clock_time() {
        let clock = Arc::new(MockSystemClock::new());
        clock.set(1_234);
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        let path = p("foo");
        store
            .put(&path, PutPayload::from(b"data".as_slice()))
            .await
            .unwrap();

        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 1_234);
    }

    #[tokio::test]
    async fn test_list_overrides_times() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(1_000);
        store
            .put(&p("a"), PutPayload::from(b"a".as_slice()))
            .await
            .unwrap();

        clock.set(2_000);
        store
            .put(&p("b"), PutPayload::from(b"b".as_slice()))
            .await
            .unwrap();

        let items: Vec<_> = store
            .list(None)
            .try_collect()
            .await
            .expect("listing should succeed");

        let mut map = std::collections::HashMap::new();
        for m in items {
            map.insert(m.location.to_string(), m.last_modified.timestamp_millis());
        }
        assert_eq!(map.get("a"), Some(&1_000));
        assert_eq!(map.get("b"), Some(&2_000));
    }

    #[tokio::test]
    async fn test_delete_removes_entry() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(3_000);
        let path = p("todel");
        store
            .put(&path, PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 3_000);

        store.delete(&path).await.unwrap();
        let head_res = store.head(&path).await;
        assert!(head_res.is_err(), "deleted object should not have head");
    }

    #[tokio::test]
    async fn test_rename_updates_target_and_removes_source() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(4_000);
        store
            .put(&p("src"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        let meta = store.head(&p("src")).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 4_000);

        clock.set(5_000);
        store.rename(&p("src"), &p("dst")).await.unwrap();

        // Source should be gone
        assert!(store.head(&p("src")).await.is_err());
        // Dest should have new timestamp
        let meta = store.head(&p("dst")).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 5_000);
    }

    #[tokio::test]
    async fn test_copy_updates_target_time() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(6_000);
        store
            .put(&p("a"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        let meta = store.head(&p("a")).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 6_000);

        clock.set(7_000);
        store.copy(&p("a"), &p("b")).await.unwrap();
        let meta_b = store.head(&p("b")).await.unwrap();
        assert_eq!(meta_b.last_modified.timestamp_millis(), 7_000);
    }

    #[tokio::test]
    async fn test_list_with_delimiter_overrides_times() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(8_000);
        store
            .put(&p("dir/a"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        clock.set(9_000);
        store
            .put(&p("dir/b"), PutPayload::from(b"y".as_slice()))
            .await
            .unwrap();

        let res = store
            .list_with_delimiter(Some(&p("dir/")))
            .await
            .expect("list_with_delimiter should succeed");
        assert_eq!(res.common_prefixes.len(), 0);
        let mut map = std::collections::HashMap::new();
        for m in res.objects {
            map.insert(m.location.to_string(), m.last_modified.timestamp_millis());
        }
        assert_eq!(map.get("dir/a"), Some(&8_000));
        assert_eq!(map.get("dir/b"), Some(&9_000));
    }

    #[tokio::test]
    async fn test_list_with_offset_overrides_times() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(10_000);
        store
            .put(&p("a"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        clock.set(11_000);
        store
            .put(&p("b"), PutPayload::from(b"y".as_slice()))
            .await
            .unwrap();

        let items: Vec<_> = store
            .list_with_offset(None, &p("a"))
            .try_collect()
            .await
            .expect("list_with_offset should succeed");
        // Should include at least "b" with its timestamp
        let mut map = std::collections::HashMap::new();
        for m in items {
            map.insert(m.location.to_string(), m.last_modified.timestamp_millis());
        }
        assert_eq!(map.get("b"), Some(&11_000));
    }
}
