//! `Mirror` multipart uploads, teed to a local temporary file.

use std::fmt;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use log::warn;
use object_store::path::Path;
use object_store::{
    Attributes, MultipartUpload, ObjectMeta, PutMultipartOptions, PutPayload, PutResult, UploadPart,
};
use slatedb_common::vfs::VfsWriter;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::inner::{local_error, Inner, TempFile};
use crate::layout::{LocalName, LocalObject};
use crate::ordering::{OpKind, Ticket};
use crate::MirrorError;

/// A part to write locally, and where to send the result.
type Part = (PutPayload, oneshot::Sender<Result<(), Arc<io::Error>>>);

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
    let name = LocalName::new(path);
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

    let (parts, received) = mpsc::unbounded_channel();
    let writes = inner.runtime.spawn(write_parts(writer, received));
    Ok(Box::new(MirrorUpload {
        inner,
        upload,
        path: path.clone(),
        name,
        attributes,
        parts: Some(parts),
        writes: Some(writes),
        size: 0,
        temp: Some(temp),
        ticket: Some(ticket),
    }))
}

/// Writes parts to the local file in the order they were sent, then flushes
/// it once the sender is dropped. After a failed write, fails every later part
/// with the same error.
///
/// This runs in its own task so that local writes don't depend on anyone
/// polling the part futures. Aborting the task closes the file.
async fn write_parts(
    mut writer: Box<dyn VfsWriter>,
    mut parts: mpsc::UnboundedReceiver<Part>,
) -> io::Result<()> {
    let mut failed: Option<Arc<io::Error>> = None;
    while let Some((data, done)) = parts.recv().await {
        let result = match &failed {
            Some(err) => Err(Arc::clone(err)),
            None => async {
                for chunk in &data {
                    writer.write(chunk.clone()).await?;
                }
                Ok::<_, io::Error>(())
            }
            .await
            .map_err(Arc::new),
        };
        if let Err(err) = &result {
            failed = Some(Arc::clone(err));
        }
        let _ = done.send(result);
    }
    match failed {
        Some(err) => Err(unshare(err)),
        None => writer.finish().await,
    }
}

struct MirrorUpload {
    inner: Arc<Inner>,
    upload: Box<dyn MultipartUpload>,
    path: Path,
    name: LocalName,
    attributes: Attributes,
    /// Sends parts to `writes`. `None` once no more parts are accepted.
    parts: Option<mpsc::UnboundedSender<Part>>,
    /// The task writing the local file. `None` once it has finished.
    writes: Option<JoinHandle<io::Result<()>>>,
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
        self.parts.take();
        if let Some(writes) = self.writes.take() {
            writes.abort();
            // Wait for the file to close before removing it.
            let _ = writes.await;
        }
        if let Some(temp) = self.temp.take() {
            temp.remove().await;
        }
        self.ticket.take();
    }

    /// Waits for all local writes, then flushes and closes the file.
    async fn finish_local(&mut self) -> io::Result<()> {
        self.parts.take();
        let Some(writes) = self.writes.as_mut() else {
            return Ok(());
        };
        let result = writes
            .await
            .unwrap_or_else(|err| Err(io::Error::other(err)));
        self.writes = None;
        result
    }
}

impl Drop for MirrorUpload {
    fn drop(&mut self) {
        // Stop writing, and remove the file once it's closed.
        if let Some(writes) = self.writes.take() {
            writes.abort();
            let temp = self.temp.take();
            self.inner.runtime.spawn(async move {
                let _ = writes.await;
                drop(temp);
            });
        }
    }
}

#[async_trait]
impl MultipartUpload for MirrorUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.size += data.content_length() as u64;

        // The writer task takes parts in the order they were given, whatever
        // order their futures are polled in. If the upload is finished, `done`
        // is dropped and the part fails.
        let (done, written) = oneshot::channel();
        if let Some(parts) = &self.parts {
            // The mirror has no `DbStatusManager`. A failed send drops `done`,
            // which fails the part.
            #[allow(clippy::disallowed_methods)]
            let _ = parts.send((data.clone(), done));
        }

        let remote = self.upload.put_part(data);
        Box::pin(async move {
            let (remote, local) = future::join(remote, written).await;
            remote?;
            local
                .unwrap_or_else(|_| Err(Arc::new(finished())))
                .map_err(|err| local_error(unshare(err)))?;
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
