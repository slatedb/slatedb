//! Startup probe for object-store conditional-overwrite (HTTP `If-Match`) support.
//!
//! SlateDB's garbage collector advances its collection boundary with a
//! conditional overwrite (`PutMode::Update`, i.e. HTTP `If-Match`) — see
//! `ObjectStoreBoundaryObject::advance` in `slatedb-txn-obj`. Object stores that
//! support *create-only* conditional writes (`If-None-Match`) but NOT
//! overwrite-if-match — notably DigitalOcean Spaces — return HTTP 412 on every
//! boundary advance, so GC silently never advances and garbage accumulates
//! without bound. There is no capability flag to inspect (`object_store`'s
//! `S3ConditionalPut::ETagMatch` conflates `If-Match` and `If-None-Match`), so we
//! probe the capability once at DB open and fail fast with an actionable error.

use std::sync::Arc;

use object_store::path::Path;
use object_store::{
    Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use tracing::debug;

/// Probe whether `object_store` supports conditional overwrite (`PutMode::Update`
/// / HTTP `If-Match`), which SlateDB's garbage collector requires to advance its
/// collection boundary. Returns a clear `Invalid` error if the store lacks it.
///
/// The probe writes (and best-effort deletes) a throwaway object under
/// `<db_path>/.slatedb_probe/`: an unconditional write to learn the current
/// version, then a conditional overwrite with that version that must succeed on a
/// store with working `If-Match`.
pub(crate) async fn probe_conditional_put(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
) -> Result<(), crate::Error> {
    let probe = Path::from(format!("{db_path}/.slatedb_probe/conditional_put"));

    // 1. Unconditional write to establish the object and learn its version/etag.
    //    Supported by every store (including ones that lack `If-Match`).
    let put = object_store
        .put_opts(
            &probe,
            PutPayload::from_static(b"slatedb-conditional-put-probe"),
            PutOptions::from(PutMode::Overwrite),
        )
        .await
        .map_err(|e| probe_io_error("writing the probe object", e))?;

    let version = UpdateVersion {
        e_tag: put.e_tag,
        version: put.version,
    };

    // 2. Conditional overwrite with the CORRECT version. This MUST succeed on a
    //    store with working `If-Match`; DigitalOcean Spaces (and any store lacking
    //    overwrite-if-match) returns HTTP 412 -> `Error::Precondition` here.
    let result = object_store
        .put_opts(
            &probe,
            PutPayload::from_static(b"slatedb-conditional-put-probe-2"),
            PutOptions::from(PutMode::Update(version)),
        )
        .await;

    // Best-effort cleanup regardless of outcome.
    let _ = object_store.delete(&probe).await;

    match result {
        Ok(_) => {
            debug!("object store conditional-put (If-Match) probe passed");
            Ok(())
        }
        Err(
            Error::Precondition { .. } | Error::NotImplemented { .. } | Error::NotSupported { .. },
        ) => Err(missing_conditional_put_error()),
        Err(e) => Err(probe_io_error("performing the conditional overwrite", e)),
    }
}

fn missing_conditional_put_error() -> crate::Error {
    crate::Error::invalid(
        "the configured object store does not support conditional overwrite \
         (HTTP If-Match / PutMode::Update). SlateDB's garbage collector advances \
         its collection boundary with a conditional overwrite, so on this store \
         garbage collection will silently fail to advance and storage will grow \
         without bound. Known-unsupported: DigitalOcean Spaces. Use a store with \
         full conditional-PUT support (AWS S3, Cloudflare R2, MinIO, GCS, Azure), \
         or configure object_store's DynamoDB-backed conditional commit \
         (S3ConditionalPut::Dynamo). To bypass this check (for example when GC is \
         disabled), call DbBuilder::skip_conditional_put_probe()."
            .to_string(),
    )
}

fn probe_io_error(ctx: &str, e: Error) -> crate::Error {
    crate::Error::invalid(format!(
        "the object-store conditional-put capability probe failed while {ctx}: {e}. \
         If you are confident the store supports conditional overwrite (If-Match), \
         bypass this check with DbBuilder::skip_conditional_put_probe()."
    ))
}

#[cfg(test)]
mod tests {
    use super::probe_conditional_put;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use std::sync::Arc;

    #[tokio::test]
    async fn in_memory_store_passes_probe() {
        // The in-memory store supports conditional put, so the probe must pass.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test-db");
        probe_conditional_put(&store, &path)
            .await
            .expect("in-memory store supports conditional put");
    }
}
