use std::collections::VecDeque;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use object_store::{path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta};
use object_store::{ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult};
use tokio::fs;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Task representing an async put operation
enum PutTask {
    InFlight(JoinHandle<object_store::Result<PutResult>>),
    Finished(object_store::Result<PutResult>),
}

/// A wrapper around an ObjectStore that writes data to local files first, then asynchronously
/// uploads them to the wrapped ObjectStore.
pub struct PutLocalObjectStore {
    /// The wrapped object store where data will eventually be stored
    inner: Arc<dyn ObjectStore>,
    /// Directory where temporary files are stored before being uploaded
    temp_dir: PathBuf,
    /// Queue of ongoing put tasks
    put_tasks: Arc<Mutex<VecDeque<PutTask>>>,
    /// Maximum number of concurrent put tasks
    max_put_tasks: usize,
}

impl PutLocalObjectStore {
    /// Create a new PutLocalObjectStore
    pub async fn new(
        inner: Arc<dyn ObjectStore>,
        temp_dir: PathBuf,
        max_put_tasks: usize,
    ) -> object_store::Result<Self> {
        // Create temp directory if it doesn't exist
        fs::create_dir_all(&temp_dir).await?;

        let store = Self {
            inner,
            temp_dir: temp_dir.clone(),
            put_tasks: Arc::new(Mutex::new(VecDeque::new())),
            max_put_tasks,
        };

        // Recover any files that were not uploaded
        store.recover_local_files().await?;

        Ok(store)
    }

    /// Generate a temporary file path for a given object path
    fn temp_file_path(&self, location: &Path) -> PathBuf {
        let filename = location
            .parts()
            .collect::<Vec<_>>()
            .join("_");
        self.temp_dir.join(filename)
    }

    /// Recover any files in the temporary directory by uploading them
    async fn recover_local_files(&self) -> object_store::Result<()> {
        let mut entries = fs::read_dir(&self.temp_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file().await? {
                let bytes = fs::read(&path).await?;
                let location = Path::from(
                    path.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .replace("_", "/"),
                );
                
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
            match &tasks[i] {
                PutTask::InFlight(handle) if handle.is_finished() => {
                    // Replace with Finished variant
                    if let Some(PutTask::InFlight(handle)) = tasks.remove(i) {
                        let result = handle.await?;
                        tasks.push_back(PutTask::Finished(Ok(result)));
                    }
                }
                PutTask::Finished(_) => {
                    tasks.remove(i);
                }
                _ => i += 1,
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

        // Create metadata for result
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: SystemTime::now(),
            size: bytes.len(),
            e_tag: None,
            version: None,
        };

        // Spawn task to upload file and clean up
        let inner = self.inner.clone();
        let temp_path_clone = temp_path.clone();
        let location_clone = location.clone();

        let mut tasks = self.put_tasks.lock().await;
        while tasks.len() >= self.max_put_tasks {
            drop(tasks);
            tokio::task::yield_now().await;
            self.process_completed_tasks().await?;
            tasks = self.put_tasks.lock().await;
        }

        let handle = tokio::spawn(async move {
            // Upload to inner store
            let result = inner
                .put_opts(
                    &location_clone,
                    bytes.into(),
                    opts,
                )
                .await?;

            // Delete temporary file
            fs::remove_file(temp_path_clone).await?;

            Ok(result)
        });

        tasks.push_back(PutTask::InFlight(handle));

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
        let store = PutLocalObjectStore::new(inner.clone(), temp_dir.clone(), 2).await?;

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
    async fn test_recover_local_files() -> object_store::Result<()> {
        let inner = Arc::new(InMemory::new());
        let temp_dir = tempdir()?.into_path();
        
        // Write a file to temp directory
        let data = Bytes::from("test data");
        let location = Path::from("test/recovery.txt");
        let temp_path = temp_dir.join("test_recovery.txt");
        fs::write(&temp_path, &data).await?;

        // Create store which should recover the file
        let store = PutLocalObjectStore::new(inner.clone(), temp_dir.clone(), 2).await?;

        // Verify data was uploaded to inner store
        let result = inner.get(&location).await?;
        assert_eq!(result.bytes().await?, data);

        // Verify temp file was cleaned up
        assert!(!temp_path.exists());

        Ok(())
    }
}
