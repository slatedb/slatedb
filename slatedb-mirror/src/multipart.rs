//! `Mirror` multipart uploads, teed to a local temporary file.

use std::fmt;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::{self, BoxFuture, FutureExt, Shared};
use log::warn;
use object_store::path::Path;
use object_store::{
    Attributes, MultipartUpload, ObjectMeta, PutMultipartOptions, PutPayload, PutResult, UploadPart,
};

use crate::inner::{local_error, Inner, TempFile};
use crate::layout::{LocalName, LocalObject};
use crate::ordering::{OpKind, Ticket};
use crate::MirrorError;

/// Resolves once every part passed to `put_part` so far is written locally.
type LocalWrites = Shared<BoxFuture<'static, Result<(), Arc<io::Error>>>>;

type SharedWriter = Arc<tokio::sync::Mutex<Option<Box<dyn crate::vfs::VfsWriter>>>>;

fn unshare(err: Arc<io::Error>) -> io::Error {
    io::Error::new(err.kind(), err)
}

fn finished() -> io::Error {
    io::Error::other("multipart upload already completed or aborted")
}

pub(crate) fn committed(
    path: &Path,
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> object_store::Error {
    MirrorError::WriteCommitted {
        path: path.clone(),
        source: source.into(),
    }
    .into()
}

/// Starts a multipart upload that also writes the object locally. The caller
/// has already checked that the policy routes `path` to `Mirror`.
pub(crate) async fn start(
    inner: Arc<Inner>,
    path: &Path,
    opts: PutMultipartOptions,
) -> object_store::Result<Box<dyn MultipartUpload>> {
    let name = LocalName::new(path)?;
    inner.prefixes.lock().check_or_insert(&name)?;
    let ticket = inner
        .ordering
        .register(path, OpKind::Write)
        .expect("writes are always queued");
    ticket.ready().await;

    let attributes = opts.attributes.clone();
    let temp = inner.temp_file(&name);
    let writer = match inner.vfs.create(&temp.path).await {
        Ok(writer) => writer,
        Err(err) => {
            temp.remove().await;
            return Err(local_error(err).into());
        }
    };
    let upload = match inner.remote.put_multipart_opts(path, opts).await {
        Ok(upload) => upload,
        Err(err) => {
            drop(writer);
            temp.remove().await;
            return Err(err);
        }
    };

    Ok(Box::new(MirrorUpload {
        inner,
        upload,
        path: path.clone(),
        name,
        attributes,
        writer: Arc::new(tokio::sync::Mutex::new(Some(writer))),
        local: future::ready(Ok(())).boxed().shared(),
        local_done: false,
        size: 0,
        temp: Some(temp),
        ticket: Some(ticket),
    }))
}

struct MirrorUpload {
    inner: Arc<Inner>,
    upload: Box<dyn MultipartUpload>,
    path: Path,
    name: LocalName,
    attributes: Attributes,
    writer: SharedWriter,
    local: LocalWrites,
    /// The local file is synced and closed.
    local_done: bool,
    size: u64,
    /// `None` once the upload is finished.
    temp: Option<TempFile>,
    ticket: Option<Ticket>,
}

impl fmt::Debug for MirrorUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MirrorUpload")
            .field("path", &self.path)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl MirrorUpload {
    /// Closes and removes the local file and releases the path.
    async fn discard(&mut self) {
        self.writer.lock().await.take();
        if let Some(temp) = self.temp.take() {
            temp.remove().await;
        }
        self.ticket.take();
    }

    /// Waits for all local writes, then syncs and closes the file.
    async fn finish_local(&mut self) -> io::Result<()> {
        if self.local_done {
            return Ok(());
        }
        self.local.clone().await.map_err(unshare)?;
        let mut writer = self.writer.lock().await;
        writer.as_mut().ok_or_else(finished)?.finish().await?;
        writer.take();
        self.local_done = true;
        Ok(())
    }
}

#[async_trait]
impl MultipartUpload for MirrorUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.size += data.content_length() as u64;

        // Chain each part's local write after the previous one's, so parts
        // land in the file in the order they were given, whatever order their
        // futures are polled in.
        let previous = self.local.clone();
        let writer = Arc::clone(&self.writer);
        let chunks = data.clone();
        let local = async move {
            previous.await?;
            let mut writer = writer.lock().await;
            let writer = writer.as_mut().ok_or_else(|| Arc::new(finished()))?;
            for chunk in &chunks {
                writer.write(chunk.clone()).await.map_err(Arc::new)?;
            }
            Ok(())
        }
        .boxed()
        .shared();
        self.local = local.clone();

        let remote = self.upload.put_part(data);
        Box::pin(async move {
            let (remote, local) = future::join(remote, local).await;
            remote?;
            local.map_err(|err| local_error(unshare(err)))?;
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.temp.is_none() {
            return Err(local_error(finished()).into());
        }
        if let Err(err) = self.finish_local().await {
            // Nothing is visible remotely until `complete`, so abort and
            // report a local failure.
            if let Err(abort_err) = self.upload.abort().await {
                warn!(
                    "failed to abort mirror multipart upload [path={}, error={}]",
                    self.path, abort_err
                );
            }
            self.discard().await;
            return Err(local_error(err).into());
        }

        // On a remote failure, keep the local file so a retried `complete` can
        // still install it. Dropping or aborting the upload removes it.
        let put = self.upload.complete().await?;

        let object = LocalObject {
            meta: ObjectMeta {
                location: self.path.clone(),
                last_modified: self.inner.clock.now(),
                size: self.size,
                e_tag: put.e_tag.clone(),
                version: put.version.clone(),
            },
            attributes: self.attributes.clone(),
        };
        let temp = self.temp.take().expect("checked above");
        let installed = self
            .inner
            .install(&self.path, &self.name, temp, object)
            .await;
        self.ticket.take();
        installed.map_err(|err| committed(&self.path, err))?;
        Ok(put)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let result = self.upload.abort().await;
        self.discard().await;
        result
    }
}
