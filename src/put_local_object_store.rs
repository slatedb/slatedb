//! A local file system-based write buffer for object stores.
//!
//! This module provides [`PutLocalObjectStore`], a wrapper around any [`ObjectStore`] implementation
//! that buffers write operations to local disk before asynchronously uploading them to the underlying
//! store. This approach offers several benefits:
//!
//! * Improved write performance by leveraging local disk speeds
//! * Resilience against temporary network issues or object store outages
//! * Automatic recovery of buffered files after process restart
//!
//! Reads are always directed to the underlying store, which means there may be a delay between a
//! successful write returning and the data being available for read operations. Reads might appear out
//! of order due to the asynchronous nature of the upload process. To prevent this, set `max_put_tasks`
//! to 1.
//!
//! Puts will be retried up to `max_retries` times if the upload fails. If the upload still fails after
//! the maximum retries, a failure will be returned when the next `put_opts` is called.
//!
//! # Examples
//!
//! ```rust
//! use std::sync::Arc;
//! use std::path::PathBuf;
//! use object_store::memory::InMemory;
//! use object_store::{ObjectStore, path::Path};
//! use bytes::Bytes;
//!
//! # async fn example() -> object_store::Result<()> {
//! // Create an underlying object store (e.g., S3, GCS, or in this case Memory)
//! let inner_store = Arc::new(InMemory::new());
//!
//! // Create a temporary directory for buffering writes
//! let temp_dir = PathBuf::from("/tmp/object_store_buffer");
//!
//! // Initialize PutLocalObjectStore with configuration
//! let store = PutLocalObjectStore::new(
//!     inner_store,
//!     temp_dir,
//!     10,    // Maximum concurrent upload tasks
//!     3,     // Maximum retry attempts for failed uploads
//! )?;
//!
//! // Write data - this will be buffered to local disk first
//! let data = Bytes::from("Hello, World!");
//! let path = Path::from("example/file.txt");
//! store.put_opts(&path, data.into(), PutOptions::default()).await?;
//!
//! // Data is now on local disk and being uploaded asynchronously
//! # Ok(())
//! # }
//! ```
//!
//! # Recovery Behavior
//!
//! When a [`PutLocalObjectStore`] is created, it automatically scans the temporary directory for any
//! files from previous sessions and attempts to upload them to the underlying store. This ensures
//! data durability even across process restarts or crashes.
//!
//! # Warning
//!
//! This store should not be used in scenarios requiring immediate read-after-write consistency
//! or where local disk space is constrained. Always ensure the temporary directory is on a
//! reliable disk with adequate free space.
//!

use std::collections::VecDeque;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use object_store::{path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta};
use object_store::{ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult};
use tokio::fs;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// Base delay in milliseconds for exponential backoff during retries
const BASE_DELAY_MS: u64 = 100;

/// A wrapper around an ObjectStore that writes data to local files first, then asynchronously
/// uploads them to the wrapped ObjectStore.
pub struct PutLocalObjectStore {
    /// The wrapped object store where data will eventually be stored
    inner: Arc<dyn ObjectStore>,
    /// Directory where temporary files are stored before being uploaded
    temp_dir: PathBuf,
    /// Queue of ongoing put tasks
    put_tasks: Arc<Mutex<VecDeque<JoinHandle<object_store::Result<PutResult>>>>>,
    /// Maximum number of concurrent put tasks
    max_put_tasks: usize,
    /// Maximum number of retry attempts for put operations
    max_retries: u32,
}

impl PutLocalObjectStore {
    /// Create a new PutLocalObjectStore
    pub async fn new(
        inner: Arc<dyn ObjectStore>,
        temp_dir: PathBuf,
        max_put_tasks: usize,
        max_retries: u32,
    ) -> object_store::Result<Self> {
        // Create temp directory if it doesn't exist
        fs::create_dir_all(&temp_dir).await?;

        let store = Self {
            inner,
            temp_dir: temp_dir.clone(),
            put_tasks: Arc::new(Mutex::new(VecDeque::new())),
            max_put_tasks,
            max_retries,
        };

        // Recover any files that were not uploaded
        store.recover_local_files().await?;

        Ok(store)
    }

    /// Generate a temporary file path for a given object path
    fn temp_file_path(&self, location: &Path) -> PathBuf {
        // Convert the path to a string and base64url encode it
        let path_str = location.as_ref();
        let filename = URL_SAFE.encode(path_str.as_bytes());
        self.temp_dir.join(filename)
    }

    /// Convert a temporary filename back to an object path
    fn temp_file_to_path(&self, filename: &str) -> object_store::Result<Path> {
        let decoded = URL_SAFE.decode(filename.as_bytes())
            .map_err(|e| object_store::Error::Generic {
                store: "PutLocalObjectStore",
                source: Box::new(e),
            })?;

        let path_str = String::from_utf8(decoded)
            .map_err(|e| object_store::Error::Generic {
                store: "PutLocalObjectStore",
                source: Box::new(e),
            })?;

        Ok(Path::from(path_str))
    }

    /// Recover any files in the temporary directory by uploading them
    async fn recover_local_files(&self) -> object_store::Result<()> {
        let mut entries = fs::read_dir(&self.temp_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file().await? {
                let bytes = fs::read(&path).await?;
                let filename = path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap();
                let location = self.temp_file_to_path(filename)?;
                
                // Upload file to object store
                self.inner
                    .put_opts(
                        &location,
                        bytes.into(),
                        PutOptions::default(),
                    )
                    .await?;
                
                // Delete temporary file
                fs::remove_file(path).await?;
            }
        }
        Ok(())
    }

    /// Process completed put tasks and remove them from the queue
    async fn process_completed_tasks(&self) -> object_store::Result<()> {
        let mut tasks = self.put_tasks.lock().await;
        
        let mut i = 0;
        while i < tasks.len() {
            if tasks[i].is_finished() {
                // Remove and await the completed task
                if let Some(handle) = tasks.remove(i) {
                    handle.await??;
                }
            } else {
                i += 1;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for PutLocalObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // Process any completed tasks
        self.process_completed_tasks().await?;

        // Write to local file
        let temp_path = self.temp_file_path(location);
        let bytes = match payload {
            PutPayload::Bytes(bytes) => bytes,
            PutPayload::File(path) => Bytes::from(fs::read(path).await?),
        };
        fs::write(&temp_path, &bytes).await?;

        // Spawn task to upload file and clean up
        let inner = self.inner.clone();
        let temp_path_clone = temp_path.clone();
        let location_clone = location.clone();
        let max_retries = self.max_retries;
        let bytes = bytes.clone();

        let mut tasks = self.put_tasks.lock().await;
        while tasks.len() >= self.max_put_tasks {
            drop(tasks);
            tokio::task::yield_now().await;
            self.process_completed_tasks().await?;
            tasks = self.put_tasks.lock().await;
        }

        let handle = tokio::spawn(async move {
            let mut retries = 0;
            loop {
                match inner
                    .put_opts(
                        &location_clone,
                        bytes.clone().into(),
                        opts.clone(),
                    )
                    .await
                {
                    Ok(result) => {
                        // Delete temporary file on success
                        fs::remove_file(temp_path_clone).await?;
                        break Ok(result);
                    }
                    Err(e) => {
                        if retries >= max_retries {
                            fs::remove_file(temp_path_clone).await?;
                            break Err(e);
                        }
                        // Exponential backoff: delay = base_delay * 2^retries
                        let delay = Duration::from_millis(BASE_DELAY_MS * (1 << retries));
                        sleep(delay).await;
                        retries += 1;
                    }
                }
            }
        });

        tasks.push_back(handle);

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_put_local_object_store() -> object_store::Result<()> {
        let inner = Arc::new(InMemory::new());
        let temp_dir = tempdir()?.into_path();
        let store = PutLocalObjectStore::new(inner.clone(), temp_dir.clone(), 2, 3).await?;

        // Put some data
        let data = Bytes::from("hello world");
        let location = Path::from("test/file.txt");
        store
            .put_opts(&location, data.clone().into(), PutOptions::default())
            .await?;

        // Verify data was written to temp file
        let temp_path = store.temp_file_path(&location);
        assert!(temp_path.exists());

        // Wait for async upload to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify data was uploaded to inner store
        let result = inner.get(&location).await?;
        assert_eq!(result.bytes().await?, data);

        // Verify temp file was cleaned up
        assert!(!temp_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_path_conversion() -> object_store::Result<()> {
        let inner = Arc::new(InMemory::new());
        let temp_dir = tempdir()?.into_path();
        let store = PutLocalObjectStore::new(inner.clone(), temp_dir.clone(), 2, 3).await?;

        let test_paths = vec![
            "simple.txt",
            "path/to/file.txt",
            "path/with/special/@#$%^&*/chars.txt",
        ];

        for path in test_paths {
            let location = Path::from(path);
            let temp_path = store.temp_file_path(&location);
            let filename = temp_path.file_name().unwrap().to_str().unwrap();
            let recovered = store.temp_file_to_path(filename)?;
            assert_eq!(recovered.as_ref(), path);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_recover_local_files() -> object_store::Result<()> {
        let inner = Arc::new(InMemory::new());
        let temp_dir = tempdir()?.into_path();
        
        // Write a file to temp directory
        let data = Bytes::from("test data");
        let location = Path::from("test/recovery.txt");
        let temp_path = temp_dir.join(URL_SAFE.encode("test/recovery.txt"));
        fs::write(&temp_path, &data).await?;

        // Create store which should recover the file
        let store = PutLocalObjectStore::new(inner.clone(), temp_dir.clone(), 2, 3).await?;

        // Verify data was uploaded to inner store
        let result = inner.get(&location).await?;
        assert_eq!(result.bytes().await?, data);

        // Verify temp file was cleaned up
        assert!(!temp_path.exists());

        Ok(())
    }
}
