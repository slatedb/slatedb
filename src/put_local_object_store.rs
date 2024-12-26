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
//! successful write returning and the data being available for read operations.
//!
//! Puts will be retried up to `max_retries` times if the upload fails. If the upload still fails after
//! the maximum retries, a failure will be returned when the next `put_opts` is called. All further `put_opts`
//! calls will return an error.
//!
//! ETags and versions are not supported.
//!
//! # Examples
//!
//! ```rust
//! use std::sync::Arc;
//! use std::path::PathBuf;
//! use object_store::memory::InMemory;
//! use object_store::{ObjectStore, path::Path, PutOptions, GetOptions};
//! use bytes::Bytes;
//!
//! # async fn example() -> object_store::Result<()> {
//! // Create an underlying object store (e.g., S3, GCS, or in this case Memory)
//! let inner_store = Arc::new(InMemory::new());
//!
//! // Create a directory for buffering writes
//! let buffer_dir = PathBuf::from("/tmp/object_store_buffer");
//!
//! // Initialize PutLocalObjectStore with configuration
//! let store = PutLocalObjectStore::new(
//!     inner_store,
//!     buffer_dir,
//!     10,    // Maximum buffered files on disk
//!     3,     // Maximum retry attempts for failed uploads
//! )?;
//!
//! // Write data - this will be buffered to local disk first
//! let data = Bytes::from("Hello, World!");
//! let path = Path::from("example/file.txt");
//!
//! // Put operation with default options
//! let put_result = store.put_opts(&path, data.into(), PutOptions::default()).await?;
//!
//! # Ok(())
//! # }
//! ```
//!
//! Note: Reads are always directed to the underlying store, so there may be a brief delay
//! between a successful write returning and the data being available for read operations.
//! The store will automatically retry failed uploads based on the configured `max_retries`.
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
use std::sync::atomic::{AtomicU64, Ordering};

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use object_store::{path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta};
use object_store::{ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult};
use tokio::fs;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// Base delay in milliseconds for exponential backoff during retries
const BASE_DELAY_MS: u64 = 100;

/// Structure to track a pending upload
struct PendingUpload {
    temp_path: PathBuf,
    location: Path,
    opts: PutOptions,
}

/// A wrapper around an ObjectStore that writes data to local files first, then asynchronously
/// uploads them to the wrapped ObjectStore.
pub struct PutLocalObjectStore {
    /// The wrapped object store where data will eventually be stored
    inner: Arc<dyn ObjectStore>,
    /// Directory where temporary files are stored before being uploaded
    temp_dir: PathBuf,
    /// Sequence counter for ordering files
    sequence: AtomicU64,
    /// Channel sender for upload requests
    upload_sender: mpsc::Sender<PendingUpload>,
    /// Handle to the uploader task
    uploader: Mutex<Option<JoinHandle<object_store::Result<()>>>>,
    /// Maximum number of buffered files
    max_buffered_files: usize,
    /// Maximum number of retry attempts for put operations
    max_retries: u32,
}

impl PutLocalObjectStore {
    /// Create a new PutLocalObjectStore
    pub async fn new(
        inner: Arc<dyn ObjectStore>,
        temp_dir: PathBuf,
        max_buffered_files: usize,
        max_retries: u32,
    ) -> object_store::Result<Self> {
        // Create temp directory if it doesn't exist
        fs::create_dir_all(&temp_dir).await?;

        // Create channel for upload requests
        let (upload_sender, mut upload_receiver) = mpsc::channel(max_buffered_files);

        let store = Self {
            inner: inner.clone(),
            temp_dir: temp_dir.clone(),
            sequence: AtomicU64::new(0),
            upload_sender,
            uploader: Mutex::new(None),
            max_buffered_files,
            max_retries,
        };

        // Start the uploader task
        let uploader = tokio::spawn({
            let inner = inner.clone();
            let max_retries = max_retries;

            async move {
                let mut last_error = None;
                while let Some(upload) = upload_receiver.recv().await {
                    let mut retries = 0;
                    loop {
                        let bytes = match fs::read(&upload.temp_path).await {
                            Ok(b) => b,
                            Err(e) => {
                                last_error = Some(object_store::Error::Generic {
                                    store: "PutLocalObjectStore",
                                    source: Box::new(e),
                                });
                                break;
                            }
                        };

                        match inner
                            .put_opts(
                                &upload.location,
                                bytes.into(),
                                upload.opts.clone(),
                            )
                            .await
                        {
                            Ok(_) => {
                                let _ = fs::remove_file(upload.temp_path).await;
                                break;
                            }
                            Err(e) => {
                                if retries >= max_retries {
                                    let _ = fs::remove_file(upload.temp_path).await;
                                    last_error = Some(e);
                                    break;
                                }
                                let delay = Duration::from_millis(BASE_DELAY_MS * (1 << retries));
                                sleep(delay).await;
                                retries += 1;
                            }
                        }
                    }

                    // Break the outer loop if we hit an error
                    if last_error.is_some() {
                        break;
                    }
                }
                // Return the last error if any
                if let Some(e) = last_error {
                    Err(e)
                } else {
                    Ok(())
                }
            }
        });

        // Store the uploader handle
        *store.uploader.lock().await = Some(uploader);

        // Recover any files that were not uploaded
        store.recover_local_files().await?;

        Ok(store)
    }

    /// Generate a temporary file path for a given object path and sequence number
    fn temp_file_path(&self, location: &Path, sequence: u64) -> PathBuf {
        // Convert the path to a string and base64url encode it
        let path_str = location.as_ref();
        let encoded = URL_SAFE.encode(path_str.as_bytes());
        // Add sequence prefix
        let filename = format!("{:016x}_{}", sequence, encoded);
        self.temp_dir.join(filename)
    }

    /// Convert a temporary filename back to an object path
    fn temp_file_to_path(&self, filename: &str) -> object_store::Result<(u64, Path)> {
        let parts: Vec<&str> = filename.splitn(2, '_').collect();
        if parts.len() != 2 {
            return Err(object_store::Error::Generic {
                store: "PutLocalObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid filename format",
                )),
            });
        }

        let sequence = u64::from_str_radix(parts[0], 16).map_err(|e| object_store::Error::Generic {
            store: "PutLocalObjectStore",
            source: Box::new(e),
        })?;

        let decoded = URL_SAFE.decode(parts[1].as_bytes())
            .map_err(|e| object_store::Error::Generic {
                store: "PutLocalObjectStore",
                source: Box::new(e),
            })?;

        let path_str = String::from_utf8(decoded)
            .map_err(|e| object_store::Error::Generic {
                store: "PutLocalObjectStore",
                source: Box::new(e),
            })?;

        Ok((sequence, Path::from(path_str)))
    }

    /// Recover any files in the temporary directory by uploading them
    async fn recover_local_files(&self) -> object_store::Result<()> {
        let mut entries = fs::read_dir(&self.temp_dir).await?;
        let mut files = Vec::new();
        
        // Collect all files and their sequence numbers
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file().await? {
                if let Ok((sequence, _)) = self.temp_file_to_path(
                    path.file_name().unwrap().to_str().unwrap()
                ) {
                    files.push((sequence, path));
                }
            }
        }

        // Sort by sequence number
        files.sort_by_key(|(seq, _)| *seq);

        // Queue files for upload
        for (_, path) in files {
            let filename = path.file_name().unwrap().to_str().unwrap();
            let (_, location) = self.temp_file_to_path(filename)?;

            self.upload_sender.send(PendingUpload {
                temp_path: path,
                location,
                opts: PutOptions::default(),
            }).await.map_err(|e| object_store::Error::Generic {
                store: "PutLocalObjectStore",
                source: Box::new(e),
            })?;
        }
        Ok(())
    }

    /// Process completed put tasks and remove them from the queue
    async fn process_completed_tasks(&self) -> object_store::Result<()> {
        // Not needed with single uploader task
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
        // Get next sequence number
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        // Write to local file
        let temp_path = self.temp_file_path(location, sequence);
        let bytes = match payload {
            PutPayload::Bytes(bytes) => bytes,
            PutPayload::File(path) => Bytes::from(fs::read(path).await?),
        };
        fs::write(&temp_path, &bytes).await?;

        // Check if uploader task has errored
        if let Some(uploader) = self.uploader.lock().await.as_ref() {
            if uploader.is_finished() {
                // Join the task to get the error
                if let Some(handle) = self.uploader.lock().await.take() {
                    handle.await??;
                }
                return Err(object_store::Error::Generic {
                    store: "PutLocalObjectStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Uploader task has stopped unexpectedly",
                    )),
                });
            }
        }

        // Queue the upload
        self.upload_sender
            .send(PendingUpload {
                temp_path,
                location: location.clone(),
                opts,
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "PutLocalObjectStore",
                source: Box::new(e),
            })?;

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
        let temp_path = store.temp_file_path(&location, 0);
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
            let temp_path = store.temp_file_path(&location, 0);
            let filename = temp_path.file_name().unwrap().to_str().unwrap();
            let recovered = store.temp_file_to_path(filename)?;
            assert_eq!(recovered.1.as_ref(), path);
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
        let temp_path = temp_dir.join(format!("{:016x}_{}", 0, URL_SAFE.encode("test/recovery.txt")));
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
