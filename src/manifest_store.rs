use crate::error::SlateDBError;
use crate::flatbuffer_types::ManifestV1Owned;
use crate::transactional_object_store::{
    DelegatingTransactionalObjectStore, TransactionalObjectStore,
};
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::Error::AlreadyExists;
use object_store::ObjectStore;
use std::sync::Arc;

pub(crate) struct ManifestStore {
    object_store: Box<dyn TransactionalObjectStore>,
    manifest_suffix: &'static str,
}

impl ManifestStore {
    pub(crate) fn new(root_path: &Path, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store: Box::new(DelegatingTransactionalObjectStore::new(
                root_path.child("manifest"),
                object_store,
            )),
            manifest_suffix: "manifest",
        }
    }
    pub(crate) async fn write_manifest(
        &self,
        manifest: &ManifestV1Owned,
    ) -> Result<(), SlateDBError> {
        let manifest_path = &Path::from(format!(
            "{:020}.{}",
            manifest.borrow().manifest_id(),
            self.manifest_suffix
        ));

        self.object_store
            .put_if_not_exists(manifest_path, Bytes::copy_from_slice(manifest.data()))
            .await
            .map_err(|err| {
                if let AlreadyExists { path: _, source: _ } = err {
                    SlateDBError::ManifestVersionExists
                } else {
                    SlateDBError::ObjectStoreError(err)
                }
            })?;

        Ok(())
    }

    pub(crate) async fn read_latest_manifest(
        &self,
    ) -> Result<Option<ManifestV1Owned>, SlateDBError> {
        let manifest_path = &Path::from("/");
        let mut files_stream = self.object_store.list(Some(manifest_path));
        let mut id_and_manifest_file_path: Option<(u64, Path)> = None;
        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
        } {
            match self.parse_id(&file.location, "manifest") {
                Ok(id) => {
                    id_and_manifest_file_path = match id_and_manifest_file_path {
                        Some((current_id, current_path)) => Some(if current_id < id {
                            (id, file.location)
                        } else {
                            (current_id, current_path)
                        }),
                        None => Some((id, file.location.clone())),
                    }
                }
                Err(_) => continue,
            }
        }

        if let Some((_, resolved_manifest_file_path)) = id_and_manifest_file_path {
            let manifest_bytes = match self.object_store.get(&resolved_manifest_file_path).await {
                Ok(manifest) => match manifest.bytes().await {
                    Ok(bytes) => bytes,
                    Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
                },
                Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
            };

            ManifestV1Owned::new(manifest_bytes.clone())
                .map(Some)
                .map_err(SlateDBError::InvalidFlatbuffer)
        } else {
            Ok(None)
        }
    }

    fn parse_id(&self, path: &Path, expected_extension: &str) -> Result<u64, SlateDBError> {
        match path.extension() {
            Some(ext) if ext == expected_extension => path
                .filename()
                .expect("invalid filename")
                .split('.')
                .next()
                .ok_or_else(|| SlateDBError::InvalidDBState)?
                .parse()
                .map_err(|_| SlateDBError::InvalidDBState),
            _ => Err(SlateDBError::InvalidDBState),
        }
    }
}
