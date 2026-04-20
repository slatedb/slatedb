//! Deterministic local filesystem-backed [`ObjectStore`] for DST scenarios.
//!
//! Unlike `object_store::local::LocalFileSystem`, this implementation performs
//! filesystem operations synchronously on the current task, emits deterministic
//! synthetic metadata, and returns sorted listing results.

use std::collections::{BTreeSet, HashMap};
use std::fs::{metadata, symlink_metadata, File, Metadata, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Component, Path as StdPath, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use parking_lot::{Mutex, RwLock};
use tokio::task::yield_now;
use walkdir::{DirEntry, WalkDir};

const STORE_NAME: &str = "DeterministicLocalFilesystem";
const IO_CHUNK_SIZE: usize = 64 * 1024;

/// Deterministic filesystem-backed object store for DST harnesses.
///
/// This store preserves local filesystem path behavior while avoiding Tokio's
/// blocking thread pool, sorting list results by path, and synthesizing stable
/// `last_modified` metadata from a logical clock.
#[derive(Debug, Clone)]
pub struct DeterministicLocalFilesystem {
    root: PathBuf,
    automatic_cleanup: bool,
    attribute_state: Arc<AttributeState>,
    metadata_state: Arc<MetadataState>,
}

#[derive(Debug)]
struct MetadataState {
    last_modified: RwLock<HashMap<Path, DateTime<Utc>>>,
    next_micros: AtomicI64,
}

#[derive(Debug, Default)]
struct AttributeState {
    values: RwLock<HashMap<Path, Attributes>>,
}

impl Default for MetadataState {
    fn default() -> Self {
        Self {
            last_modified: RwLock::new(HashMap::new()),
            next_micros: AtomicI64::new(1),
        }
    }
}

impl MetadataState {
    fn zero_time() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(0).expect("unix epoch is valid")
    }

    fn record_modified(&self, location: &Path) -> DateTime<Utc> {
        let micros = self.next_micros.fetch_add(1, Ordering::SeqCst);
        let timestamp = DateTime::from_timestamp_micros(micros)
            .expect("deterministic local filesystem timestamp must be valid");
        self.last_modified
            .write()
            .insert(location.clone(), timestamp);
        timestamp
    }

    fn remove(&self, location: &Path) {
        self.last_modified.write().remove(location);
    }

    fn get(&self, location: &Path) -> DateTime<Utc> {
        self.last_modified
            .read()
            .get(location)
            .copied()
            .unwrap_or_else(Self::zero_time)
    }
}

impl AttributeState {
    fn get(&self, location: &Path) -> Attributes {
        self.values
            .read()
            .get(location)
            .cloned()
            .unwrap_or_default()
    }

    fn set(&self, location: &Path, attributes: Attributes) {
        let mut values = self.values.write();
        if attributes.is_empty() {
            values.remove(location);
        } else {
            values.insert(location.clone(), attributes);
        }
    }

    fn copy(&self, from: &Path, to: &Path) {
        self.set(to, self.get(from));
    }

    fn rename(&self, from: &Path, to: &Path) {
        let mut values = self.values.write();
        match values.remove(from) {
            Some(attributes) if !attributes.is_empty() => {
                values.insert(to.clone(), attributes);
            }
            _ => {
                values.remove(to);
            }
        }
    }

    fn remove(&self, location: &Path) {
        self.values.write().remove(location);
    }
}

impl std::fmt::Display for DeterministicLocalFilesystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE_NAME}({})", self.root.display())
    }
}

impl DeterministicLocalFilesystem {
    /// Creates a deterministic filesystem store rooted at `prefix`.
    ///
    /// ## Arguments
    /// - `prefix`: Existing directory used as the filesystem root.
    ///
    /// ## Returns
    /// - `Ok(Self)`: A deterministic filesystem-backed object store.
    /// - `Err(object_store::Error)`: The prefix could not be canonicalized.
    pub fn new_with_prefix(prefix: impl AsRef<StdPath>) -> object_store::Result<Self> {
        let root = std::fs::canonicalize(prefix.as_ref())
            .expect("failed to canonicalize deterministic local filesystem root");

        Ok(Self {
            root,
            automatic_cleanup: false,
            attribute_state: Arc::new(AttributeState::default()),
            metadata_state: Arc::new(MetadataState::default()),
        })
    }

    /// Enables or disables recursive cleanup of empty parent directories on delete.
    ///
    /// ## Arguments
    /// - `automatic_cleanup`: Whether deleting a file should also remove empty
    ///   parent directories up to the configured root.
    ///
    /// ## Returns
    /// - `Self`: The updated store.
    pub fn with_automatic_cleanup(mut self, automatic_cleanup: bool) -> Self {
        self.automatic_cleanup = automatic_cleanup;
        self
    }

    /// Resolves an object-store path to an absolute filesystem path.
    ///
    /// ## Arguments
    /// - `location`: Object location relative to the configured root.
    ///
    /// ## Returns
    /// - `Ok(PathBuf)`: The absolute filesystem path for `location`.
    /// - `Err(object_store::Error)`: The object path is invalid.
    pub fn path_to_filesystem(&self, location: &Path) -> object_store::Result<PathBuf> {
        if !is_valid_file_path(location) {
            return Err(invalid_input_error(format!(
                "filenames containing trailing '/#\\d+/' are not supported: {location}"
            )));
        }

        Ok(self.prefix_to_filesystem(location))
    }

    fn prefix_to_filesystem(&self, location: &Path) -> PathBuf {
        let mut resolved = self.root.clone();
        for part in location.parts() {
            resolved.push(part.as_ref());
        }
        resolved
    }

    fn filesystem_to_location(&self, path: &StdPath) -> object_store::Result<Path> {
        let relative = path
            .strip_prefix(&self.root)
            .expect("path is outside deterministic local filesystem root");

        let mut parts = Vec::new();
        for component in relative.components() {
            match component {
                Component::Normal(part) => {
                    let part = part.to_str().ok_or_else(|| {
                        invalid_input_error(format!(
                            "path {} contained non-unicode characters",
                            path.display()
                        ))
                    })?;
                    parts.push(part.to_string());
                }
                Component::CurDir => {}
                other => {
                    return Err(invalid_input_error(format!(
                        "unsupported path component {other:?} in {}",
                        path.display()
                    )));
                }
            }
        }

        Ok(Path::from_iter(parts))
    }

    fn object_meta(&self, metadata: Metadata, location: Path) -> ObjectMeta {
        ObjectMeta {
            last_modified: self.metadata_state.get(&location),
            location,
            size: metadata.len(),
            e_tag: None,
            version: None,
        }
    }

    fn convert_entry(
        &self,
        entry: DirEntry,
        location: Path,
    ) -> object_store::Result<Option<ObjectMeta>> {
        match entry.metadata() {
            Ok(metadata) => Ok(Some(self.object_meta(metadata, location))),
            Err(error) => {
                if let Some(io_error) = error.io_error() {
                    if io_error.kind() == ErrorKind::NotFound {
                        return Ok(None);
                    }
                }
                Err(generic_error(error))
            }
        }
    }

    async fn collect_list_with_maybe_offset(
        &self,
        prefix: Option<Path>,
        maybe_offset: Option<Path>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let root_path = prefix
            .as_ref()
            .map(|prefix| self.prefix_to_filesystem(prefix))
            .unwrap_or_else(|| self.root.clone());

        let mut objects = Vec::new();
        let walkdir = WalkDir::new(root_path).min_depth(1).follow_links(true);

        for entry_result in walkdir {
            yield_now().await;

            let entry = match convert_walkdir_result(entry_result) {
                Ok(Some(entry)) => entry,
                Ok(None) => continue,
                Err(error) => return Err(error),
            };

            if !entry.path().is_file() {
                continue;
            }

            let location = self.filesystem_to_location(entry.path())?;

            if !is_valid_file_path(&location) {
                continue;
            }

            if maybe_offset
                .as_ref()
                .is_some_and(|offset| location <= *offset)
            {
                continue;
            }

            match self.convert_entry(entry, location) {
                Ok(Some(meta)) => objects.push(meta),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }

        objects.sort_by(|left, right| left.location.cmp(&right.location));
        Ok(objects)
    }

    fn list_with_maybe_offset(
        &self,
        prefix: Option<&Path>,
        maybe_offset: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        let maybe_offset = maybe_offset.cloned();

        stream::once(async move {
            store
                .collect_list_with_maybe_offset(prefix, maybe_offset)
                .await
        })
        .map_ok(|objects| stream::iter(objects.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }
}

#[async_trait]
impl ObjectStore for DeterministicLocalFilesystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if matches!(opts.mode, PutMode::Update(_)) {
            return Err(object_store::Error::NotImplemented);
        }

        let path = self.path_to_filesystem(location)?;
        let (mut file, staging_path) = new_staged_upload(&path)?;
        yield_now().await;
        let attributes = opts.attributes.clone();

        let result = async {
            write_payload_with_yields(&mut file, &payload).await?;

            match opts.mode {
                PutMode::Overwrite => {
                    drop(file);
                    std::fs::rename(&staging_path, &path)
                        .expect("failed to move staged file into place");
                }
                PutMode::Create => match std::fs::hard_link(&staging_path, &path) {
                    Ok(()) => {
                        let _ = std::fs::remove_file(&staging_path);
                    }
                    Err(source) if source.kind() == ErrorKind::AlreadyExists => {
                        return Err(already_exists_error(&path, source));
                    }
                    Err(source) => return Err(generic_error(source)),
                },
                PutMode::Update(_) => unreachable!(),
            }

            Ok(())
        }
        .await;

        if result.is_err() {
            let _ = std::fs::remove_file(&staging_path);
        } else {
            self.attribute_state.set(location, attributes);
            self.metadata_state.record_modified(location);
        }

        result?;
        yield_now().await;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default())
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let dest = self.path_to_filesystem(location)?;
        let (file, src) = new_staged_upload(&dest)?;
        yield_now().await;
        Ok(Box::new(LocalUpload::new(
            Arc::clone(&self.attribute_state),
            Arc::clone(&self.metadata_state),
            location.clone(),
            opts.attributes,
            src,
            dest,
            file,
        )))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        yield_now().await;
        let path = self.path_to_filesystem(location)?;
        let (_, metadata) = open_file(&path)?;
        let meta = self.object_meta(metadata, location.clone());
        options.check_preconditions(&meta)?;

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                attributes: self.attribute_state.get(location),
                range: 0..0,
                meta,
            });
        }

        let range = match options.range {
            Some(range) => range.as_range(meta.size).expect("invalid requested range"),
            None => 0..meta.size,
        };

        let path_for_stream = path.clone();
        let range_for_stream = range.clone();
        let payload = stream::once(async move {
            read_range_with_yields(&path_for_stream, range_for_stream).await
        })
        .boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            attributes: self.attribute_state.get(location),
            range,
            meta,
        })
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        let path = self.path_to_filesystem(location)?;
        read_range_with_yields(&path, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let path = self.path_to_filesystem(location)?;
        let mut file = open_file(&path)?.0;
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges.iter().cloned() {
            results.push(read_range_with_yields_from_file(&mut file, &path, range).await?);
        }
        Ok(results)
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        yield_now().await;
        let path = self.path_to_filesystem(location)?;
        let (_, metadata) = open_file(&path)?;
        yield_now().await;
        Ok(self.object_meta(metadata, location.clone()))
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        yield_now().await;
        let path = self.path_to_filesystem(location)?;
        match std::fs::remove_file(&path) {
            Ok(()) => {
                self.attribute_state.remove(location);
                self.metadata_state.remove(location);
            }
            Err(source) if source.kind() == ErrorKind::NotFound => {
                return Err(not_found_error(&path, source));
            }
            Err(source) => return Err(generic_error(source)),
        }

        if self.automatic_cleanup {
            let mut parent = path.parent();
            while let Some(candidate) = parent {
                yield_now().await;
                if candidate != self.root && std::fs::remove_dir(candidate).is_ok() {
                    parent = candidate.parent();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_with_maybe_offset(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_with_maybe_offset(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let resolved_prefix = self.prefix_to_filesystem(&prefix);

        let walkdir = WalkDir::new(&resolved_prefix)
            .min_depth(1)
            .max_depth(1)
            .follow_links(true);

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        for entry_result in walkdir {
            yield_now().await;
            let Some(entry) = convert_walkdir_result(entry_result)? else {
                continue;
            };

            let is_directory = entry.file_type().is_dir();
            let entry_location = self.filesystem_to_location(entry.path())?;
            if !is_directory && !is_valid_file_path(&entry_location) {
                continue;
            }

            let mut parts = match entry_location.prefix_match(&prefix) {
                Some(parts) => parts,
                None => continue,
            };

            let common_prefix = match parts.next() {
                Some(part) => part,
                None => continue,
            };

            drop(parts);

            if is_directory {
                common_prefixes.insert(prefix.child(common_prefix));
            } else if let Some(metadata) = self.convert_entry(entry, entry_location)? {
                objects.push(metadata);
            }
        }

        objects.sort_by(|left, right| left.location.cmp(&right.location));
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_path = self.path_to_filesystem(from)?;
        let to_path = self.path_to_filesystem(to)?;
        let mut suffix = 0_u64;

        loop {
            yield_now().await;
            let staged = staged_upload_path(&to_path, &suffix.to_string());
            match std::fs::hard_link(&from_path, &staged) {
                Ok(()) => {
                    std::fs::rename(&staged, &to_path)
                        .expect("failed to move copied object into place");
                    self.attribute_state.copy(from, to);
                    self.metadata_state.record_modified(to);
                    return Ok(());
                }
                Err(source) if source.kind() == ErrorKind::AlreadyExists => suffix += 1,
                Err(source) if source.kind() == ErrorKind::NotFound => {
                    if from_path.exists() {
                        create_parent_dirs(&to_path, source)?;
                    } else {
                        return Err(not_found_error(&from_path, source));
                    }
                }
                Err(source) => return Err(generic_error(source)),
            }
        }
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_path = self.path_to_filesystem(from)?;
        let to_path = self.path_to_filesystem(to)?;

        loop {
            yield_now().await;
            match std::fs::rename(&from_path, &to_path) {
                Ok(()) => {
                    self.attribute_state.rename(from, to);
                    self.metadata_state.remove(from);
                    self.metadata_state.record_modified(to);
                    return Ok(());
                }
                Err(source) if source.kind() == ErrorKind::NotFound => {
                    if from_path.exists() {
                        create_parent_dirs(&to_path, source)?;
                    } else {
                        return Err(not_found_error(&from_path, source));
                    }
                }
                Err(source) => return Err(generic_error(source)),
            }
        }
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_path = self.path_to_filesystem(from)?;
        let to_path = self.path_to_filesystem(to)?;

        loop {
            yield_now().await;
            match std::fs::hard_link(&from_path, &to_path) {
                Ok(()) => {
                    self.attribute_state.copy(from, to);
                    self.metadata_state.record_modified(to);
                    return Ok(());
                }
                Err(source) if source.kind() == ErrorKind::AlreadyExists => {
                    return Err(already_exists_error(&to_path, source));
                }
                Err(source) if source.kind() == ErrorKind::NotFound => {
                    if from_path.exists() {
                        create_parent_dirs(&to_path, source)?;
                    } else {
                        return Err(not_found_error(&from_path, source));
                    }
                }
                Err(source) => return Err(generic_error(source)),
            }
        }
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let head_result = self.head(to).await;
        match head_result {
            Ok(_) => {
                return Err(already_exists_error(
                    &self.path_to_filesystem(to)?,
                    io::Error::new(ErrorKind::AlreadyExists, "destination already exists"),
                ));
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }

        self.rename(from, to).await
    }
}

#[derive(Debug)]
struct LocalUpload {
    state: Arc<UploadState>,
    src: Option<PathBuf>,
    offset: u64,
}

#[derive(Debug)]
struct UploadState {
    dest: PathBuf,
    location: Path,
    attributes: Attributes,
    attribute_state: Arc<AttributeState>,
    file: Mutex<File>,
    metadata_state: Arc<MetadataState>,
}

impl LocalUpload {
    fn new(
        attribute_state: Arc<AttributeState>,
        metadata_state: Arc<MetadataState>,
        location: Path,
        attributes: Attributes,
        src: PathBuf,
        dest: PathBuf,
        file: File,
    ) -> Self {
        Self {
            state: Arc::new(UploadState {
                dest,
                location,
                attributes,
                attribute_state,
                file: Mutex::new(file),
                metadata_state,
            }),
            src: Some(src),
            offset: 0,
        }
    }
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let offset = self.offset;
        self.offset += data.content_length() as u64;

        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let mut current_offset = offset;
            for bytes in data.iter() {
                let mut remaining = bytes.as_ref();
                while !remaining.is_empty() {
                    yield_now().await;

                    let chunk_len = remaining.len().min(IO_CHUNK_SIZE);
                    let (chunk, rest) = remaining.split_at(chunk_len);
                    {
                        let mut file = state.file.lock();
                        file.seek(SeekFrom::Start(current_offset))
                            .expect("failed to seek multipart upload file");
                        file.write_all(chunk)
                            .expect("failed to write multipart upload chunk");
                    }

                    current_offset += u64::try_from(chunk_len).unwrap_or(0);
                    remaining = rest;
                }
            }

            yield_now().await;
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let src = self.src.take().ok_or_else(|| {
            generic_error(io::Error::new(
                ErrorKind::BrokenPipe,
                "multipart upload was already completed or aborted",
            ))
        })?;

        yield_now().await;
        {
            let _file = self.state.file.lock();
            std::fs::rename(&src, &self.state.dest).expect("failed to finalize multipart upload");
        }
        self.state
            .attribute_state
            .set(&self.state.location, self.state.attributes.clone());
        self.state
            .metadata_state
            .record_modified(&self.state.location);
        yield_now().await;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let src = self.src.take().ok_or_else(|| {
            generic_error(io::Error::new(
                ErrorKind::BrokenPipe,
                "multipart upload was already completed or aborted",
            ))
        })?;
        yield_now().await;
        std::fs::remove_file(&src).expect("failed to remove aborted multipart upload");
        yield_now().await;
        Ok(())
    }
}

impl Drop for LocalUpload {
    fn drop(&mut self) {
        if let Some(src) = self.src.take() {
            let _ = std::fs::remove_file(src);
        }
    }
}

fn generic_error<E>(source: E) -> object_store::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    object_store::Error::Generic {
        store: STORE_NAME,
        source: Box::new(source),
    }
}

fn invalid_input_error(message: String) -> object_store::Error {
    generic_error(io::Error::new(ErrorKind::InvalidInput, message))
}

fn not_found_error(path: &StdPath, source: io::Error) -> object_store::Error {
    object_store::Error::NotFound {
        path: path.to_string_lossy().to_string(),
        source: Box::new(source),
    }
}

fn already_exists_error(path: &StdPath, source: io::Error) -> object_store::Error {
    object_store::Error::AlreadyExists {
        path: path.to_string_lossy().to_string(),
        source: Box::new(source),
    }
}

fn is_valid_file_path(path: &Path) -> bool {
    match path.filename() {
        Some(filename) => match filename.split_once('#') {
            Some((_, suffix)) if !suffix.is_empty() => {
                !suffix.as_bytes().iter().all(|byte| byte.is_ascii_digit())
            }
            _ => true,
        },
        None => false,
    }
}

fn create_parent_dirs(path: &StdPath, source: io::Error) -> object_store::Result<()> {
    let parent = path.parent().ok_or_else(|| generic_error(source))?;
    std::fs::create_dir_all(parent).expect("failed to create parent directories");
    Ok(())
}

fn new_staged_upload(base: &StdPath) -> object_store::Result<(File, PathBuf)> {
    let mut multipart_id = 1_u64;
    loop {
        let path = staged_upload_path(base, &multipart_id.to_string());
        let mut options = OpenOptions::new();
        match options.read(true).write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(source) if source.kind() == ErrorKind::AlreadyExists => multipart_id += 1,
            Err(source) if source.kind() == ErrorKind::NotFound => {
                create_parent_dirs(&path, source)?;
            }
            Err(source) => return Err(generic_error(source)),
        }
    }
}

async fn write_payload_with_yields(
    file: &mut File,
    payload: &PutPayload,
) -> object_store::Result<()> {
    for bytes in payload.iter() {
        let mut remaining = bytes.as_ref();
        while !remaining.is_empty() {
            yield_now().await;
            let chunk_len = remaining.len().min(IO_CHUNK_SIZE);
            let (chunk, rest) = remaining.split_at(chunk_len);
            file.write_all(chunk)
                .expect("failed to write deterministic local filesystem payload");
            remaining = rest;
        }
    }
    Ok(())
}

fn staged_upload_path(dest: &StdPath, suffix: &str) -> PathBuf {
    let mut staging_path = dest.as_os_str().to_owned();
    staging_path.push("#");
    staging_path.push(suffix);
    staging_path.into()
}

fn open_file(path: &PathBuf) -> object_store::Result<(File, Metadata)> {
    match File::open(path).and_then(|file| Ok((file.metadata()?, file))) {
        Ok((metadata, file)) if !metadata.is_dir() => Ok((file, metadata)),
        Ok((_metadata, _file)) => Err(not_found_error(
            path,
            io::Error::new(ErrorKind::NotFound, "path is a directory"),
        )),
        Err(source) if source.kind() == ErrorKind::NotFound => Err(not_found_error(path, source)),
        Err(source) => Err(generic_error(source)),
    }
}

async fn read_range_with_yields(path: &PathBuf, range: Range<u64>) -> object_store::Result<Bytes> {
    yield_now().await;
    let (mut file, _) = open_file(path)?;
    let result = read_range_with_yields_from_file(&mut file, path, range).await;
    yield_now().await;
    result
}

async fn read_range_with_yields_from_file(
    file: &mut File,
    path: &PathBuf,
    range: Range<u64>,
) -> object_store::Result<Bytes> {
    let metadata = file
        .metadata()
        .expect("failed to read deterministic local filesystem metadata");
    let file_len = metadata.len();

    if range.start >= file_len {
        return Err(invalid_input_error(format!(
            "requested range start {} is outside file {} with length {}",
            range.start,
            path.display(),
            file_len
        )));
    }

    let to_read = range.end.min(file_len) - range.start;
    file.seek(SeekFrom::Start(range.start))
        .expect("failed to seek deterministic local filesystem file");

    let mut buffer = Vec::with_capacity(usize::try_from(to_read).unwrap_or(0));
    let mut remaining = to_read;
    while remaining > 0 {
        yield_now().await;

        let chunk_len =
            usize::try_from(remaining.min(IO_CHUNK_SIZE as u64)).unwrap_or(IO_CHUNK_SIZE);
        let start = buffer.len();
        buffer.resize(start + chunk_len, 0);
        let read = file
            .read(&mut buffer[start..])
            .expect("failed to read deterministic local filesystem file");

        if read == 0 {
            return Err(generic_error(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "short read for {}: expected {} more bytes",
                    path.display(),
                    remaining
                ),
            )));
        }

        buffer.truncate(start + read);
        remaining -= u64::try_from(read).unwrap_or(0);
    }

    Ok(buffer.into())
}
fn convert_walkdir_result(
    result: std::result::Result<DirEntry, walkdir::Error>,
) -> object_store::Result<Option<DirEntry>> {
    match result {
        Ok(entry) => match symlink_metadata(entry.path()) {
            Ok(attributes) if attributes.is_symlink() => match metadata(entry.path()) {
                Ok(_) => Ok(Some(entry)),
                Err(_) => Ok(None),
            },
            Ok(_) => Ok(Some(entry)),
            Err(_) => Ok(None),
        },
        Err(walkdir_error) => match walkdir_error.io_error() {
            Some(io_error) if io_error.kind() == ErrorKind::NotFound => Ok(None),
            _ => Err(generic_error(walkdir_error)),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::TryStreamExt;
    use object_store::{
        Attribute, AttributeValue, Attributes, GetResultPayload, PutMultipartOptions, PutOptions,
    };
    use slatedb_common::clock::SystemClock;
    use slatedb_common::MockSystemClock;
    use tempfile::TempDir;

    use super::*;
    use crate::clocked_object_store::ClockedObjectStore;

    fn test_attributes() -> Attributes {
        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::ContentType,
            AttributeValue::from("application/octet-stream"),
        );
        attributes.insert(
            Attribute::Metadata(Cow::Borrowed("custom-key")),
            AttributeValue::from("custom-value"),
        );
        attributes
    }

    #[tokio::test]
    async fn should_list_objects_in_sorted_order() {
        let tempdir = TempDir::new().unwrap();
        let store = DeterministicLocalFilesystem::new_with_prefix(tempdir.path()).unwrap();

        store
            .put_opts(
                &Path::from("wal/002.sst"),
                b"b".to_vec().into(),
                PutOptions::default(),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &Path::from("wal/001.sst"),
                b"a".to_vec().into(),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let listed = store
            .list(Some(&Path::from("wal")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let locations: Vec<_> = listed
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();
        assert_eq!(locations, vec!["wal/001.sst", "wal/002.sst"]);
    }

    #[tokio::test]
    async fn should_return_stream_payloads() {
        let tempdir = TempDir::new().unwrap();
        let store = DeterministicLocalFilesystem::new_with_prefix(tempdir.path()).unwrap();

        store
            .put_opts(
                &Path::from("foo"),
                b"hello".to_vec().into(),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let result = store
            .get_opts(&Path::from("foo"), GetOptions::default())
            .await
            .unwrap();

        match result.payload {
            GetResultPayload::Stream(_) => {}
            _ => panic!("deterministic local filesystem should not return file payloads"),
        }

        let bytes = store
            .get_opts(&Path::from("foo"), GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn should_override_get_opts_metadata_in_clocked_wrapper() {
        let tempdir = TempDir::new().unwrap();
        let base: Arc<dyn ObjectStore> =
            Arc::new(DeterministicLocalFilesystem::new_with_prefix(tempdir.path()).unwrap());
        let clock = Arc::new(MockSystemClock::with_time(1_000));
        let store = ClockedObjectStore::new(base, clock.clone());

        store
            .put_opts(
                &Path::from("foo"),
                b"hello".to_vec().into(),
                PutOptions::default(),
            )
            .await
            .unwrap();
        clock.advance(Duration::from_millis(250)).await;

        let head = store.head(&Path::from("foo")).await.unwrap();
        let get = store
            .get_opts(&Path::from("foo"), GetOptions::default())
            .await
            .unwrap();

        assert_eq!(head.last_modified, get.meta.last_modified);
        assert_eq!(head.last_modified.timestamp_millis(), 1_000);
    }

    #[tokio::test]
    async fn should_preserve_attributes_for_put_get_copy_and_rename() {
        let tempdir = TempDir::new().unwrap();
        let store = DeterministicLocalFilesystem::new_with_prefix(tempdir.path()).unwrap();
        let attributes = test_attributes();

        store
            .put_opts(
                &Path::from("foo"),
                b"hello".to_vec().into(),
                PutOptions {
                    attributes: attributes.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let head = store
            .get_opts(
                &Path::from("foo"),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(head.attributes, attributes);

        store
            .copy(&Path::from("foo"), &Path::from("bar"))
            .await
            .unwrap();
        let copied = store
            .get_opts(
                &Path::from("bar"),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(copied.attributes, test_attributes());

        store
            .rename(&Path::from("bar"), &Path::from("baz"))
            .await
            .unwrap();
        let renamed = store
            .get_opts(
                &Path::from("baz"),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(renamed.attributes, test_attributes());
    }

    #[tokio::test]
    async fn should_clear_attributes_on_overwrite_with_defaults() {
        let tempdir = TempDir::new().unwrap();
        let store = DeterministicLocalFilesystem::new_with_prefix(tempdir.path()).unwrap();

        store
            .put_opts(
                &Path::from("foo"),
                b"hello".to_vec().into(),
                PutOptions {
                    attributes: test_attributes(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .put_opts(
                &Path::from("foo"),
                b"world".to_vec().into(),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let result = store
            .get_opts(
                &Path::from("foo"),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(result.attributes.is_empty());
    }

    #[tokio::test]
    async fn should_preserve_attributes_for_multipart_uploads() {
        let tempdir = TempDir::new().unwrap();
        let store = DeterministicLocalFilesystem::new_with_prefix(tempdir.path()).unwrap();
        let attributes = test_attributes();

        let mut upload = store
            .put_multipart_opts(
                &Path::from("foo"),
                PutMultipartOptions {
                    attributes: attributes.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        upload.put_part(b"he".to_vec().into()).await.unwrap();
        upload.put_part(b"llo".to_vec().into()).await.unwrap();
        upload.complete().await.unwrap();

        let result = store
            .get_opts(
                &Path::from("foo"),
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.attributes, attributes);
    }
}
