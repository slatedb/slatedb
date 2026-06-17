//! Startup capability preflight for the configured object store.
//!
//! SlateDB relies on a specific set of object-store capabilities for its
//! correctness and durability guarantees. Some object stores advertise an
//! S3-compatible API but silently diverge in ways that corrupt SlateDB's
//! invariants — most notably:
//!
//! * Stores that do not enforce conditional *create* (`If-None-Match`) let two
//!   writers create the same manifest / fencing / compaction object, silently
//!   breaking SlateDB's fencing, manifest, compaction and GC-boundary integrity.
//! * Stores that support create-only conditional writes but NOT
//!   overwrite-if-match (`If-Match`) — notably DigitalOcean Spaces — return HTTP
//!   412 on every GC-boundary advance, so GC silently never advances and garbage
//!   accumulates without bound.
//!
//! There is no reliable capability flag to inspect (`object_store`'s
//! `S3ConditionalPut::ETagMatch` conflates `If-Match` and `If-None-Match`), so we
//! probe the capabilities once at DB open (writer path only) and fail fast with
//! an actionable error. Optional capabilities (custom attributes, copy,
//! list-with-offset) are probed too and only produce a warning.

use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    PutPayload, UpdateVersion,
};
use std::borrow::Cow;
use tracing::{debug, warn};

/// Probe the object store for the capabilities SlateDB depends on, failing fast
/// with an actionable [`crate::Error::invalid`] on any missing REQUIRED
/// capability and emitting a `warn!` for any missing OPTIONAL capability.
///
/// All probe objects are written under `<db_path>/.slatedb_probe/` and are
/// best-effort deleted before returning (including on the error path).
pub(crate) async fn probe_object_store(
    db_path: &Path,
    object_store: &dyn ObjectStore,
) -> Result<(), crate::Error> {
    let probe_prefix = Path::from(format!("{db_path}/.slatedb_probe"));
    let p_basic = Path::from(format!("{db_path}/.slatedb_probe/basic"));
    let p_create = Path::from(format!("{db_path}/.slatedb_probe/create"));
    let p_match = Path::from(format!("{db_path}/.slatedb_probe/match"));
    let p_mp = Path::from(format!("{db_path}/.slatedb_probe/multipart"));
    let p_attr = Path::from(format!("{db_path}/.slatedb_probe/attributes"));
    let p_copy = Path::from(format!("{db_path}/.slatedb_probe/copy"));

    let result = run_probe(
        object_store,
        &probe_prefix,
        &p_basic,
        &p_create,
        &p_match,
        &p_mp,
        &p_attr,
        &p_copy,
    )
    .await;

    // Best-effort cleanup of every probe object regardless of outcome.
    for p in [&p_basic, &p_create, &p_match, &p_mp, &p_attr, &p_copy] {
        let _ = object_store.delete(p).await;
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn run_probe(
    object_store: &dyn ObjectStore,
    probe_prefix: &Path,
    p_basic: &Path,
    p_create: &Path,
    p_match: &Path,
    p_mp: &Path,
    p_attr: &Path,
    p_copy: &Path,
) -> Result<(), crate::Error> {
    const PAYLOAD: &[u8] = b"slatedb-probe-payload";

    // 1. [REQUIRED] basic put + get. This is the reachability/creds/bucket/region
    //    check: if the store is misconfigured this is where it fails.
    object_store
        .put_opts(
            p_basic,
            PutPayload::from_static(PAYLOAD),
            PutOptions::from(PutMode::Overwrite),
        )
        .await
        .map_err(|e| {
            crate::Error::invalid(format!(
                "object-store preflight failed: cannot write to the configured bucket — \
                 check credentials/endpoint/region/permissions (error: {e}). \
                 To bypass this check, call DbBuilder::skip_object_store_probe()."
            ))
        })?;
    let got = object_store
        .get(p_basic)
        .await
        .and_then_bytes()
        .await
        .map_err(|e| basic_io_error("reading back the basic probe object", e))?;
    if got.as_ref() != PAYLOAD {
        return Err(crate::Error::invalid(
            "object-store preflight failed: basic put+get returned mismatched bytes — \
             the configured bucket may be misrouting reads/writes. To bypass this check, \
             call DbBuilder::skip_object_store_probe()."
                .to_string(),
        ));
    }

    // 2. [REQUIRED] ranged get returns the correct sub-slice.
    let range = object_store
        .get_range(p_basic, 1..4)
        .await
        .map_err(|e| basic_io_error("performing a ranged GET", e))?;
    if range.as_ref() != &PAYLOAD[1..4] {
        return Err(required_error(
            "ranged GET (get_range)",
            None,
            "the store returned an incorrect byte sub-range; SlateDB reads SST blocks via \
             ranged GETs and cannot operate correctly on this store",
        ));
    }

    // 3. [REQUIRED] head returns the correct object size.
    let meta = object_store
        .head(p_basic)
        .await
        .map_err(|e| basic_io_error("performing a HEAD", e))?;
    if meta.size as usize != PAYLOAD.len() {
        return Err(required_error(
            "object metadata (head)",
            None,
            "HEAD returned an incorrect object size; SlateDB relies on accurate object \
             metadata",
        ));
    }

    // 4. [REQUIRED] list includes the basic object.
    {
        use futures::StreamExt;
        let mut stream = object_store.list(Some(probe_prefix));
        let mut found = false;
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| basic_io_error("listing the probe prefix", e))?;
            if &meta.location == p_basic {
                found = true;
            }
        }
        if !found {
            return Err(required_error(
                "object listing (list)",
                None,
                "a freshly-written object did not appear in a prefix listing; SlateDB relies \
                 on list to discover manifests, SSTs and WAL segments",
            ));
        }
    }

    // 5. [REQUIRED] If-None-Match ENFORCEMENT (the silent-corruption check).
    //    The first create must succeed; a SECOND create of the same key MUST fail.
    //    If it succeeds the store does not enforce conditional create, which makes
    //    SlateDB's fencing / manifest / compaction / GC-boundary integrity unsafe.
    object_store
        .put_opts(
            p_create,
            PutPayload::from_static(PAYLOAD),
            PutOptions::from(PutMode::Create),
        )
        .await
        .map_err(|e| basic_io_error("performing a conditional create (If-None-Match)", e))?;
    let second_create = object_store
        .put_opts(
            p_create,
            PutPayload::from_static(b"slatedb-probe-payload-2"),
            PutOptions::from(PutMode::Create),
        )
        .await;
    match second_create {
        // Correct behaviour: the store rejected the duplicate create.
        Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {}
        Ok(_) => {
            return Err(required_error(
                "conditional create (If-None-Match)",
                Some(
                    "stores that do not enforce conditional create may include misconfigured \
                     S3-compatible gateways",
                ),
                "a second conditional CREATE of an existing key SUCCEEDED — the store does not \
                 enforce If-None-Match, so SlateDB's fencing, manifest, compaction and \
                 GC-boundary integrity are unsafe on this store (concurrent writers can clobber \
                 each other's objects). Use a store that enforces conditional create \
                 (AWS S3, Cloudflare R2, MinIO, GCS, Azure), or configure object_store's \
                 DynamoDB-backed conditional commit (S3ConditionalPut::Dynamo)",
            ));
        }
        Err(e) => {
            return Err(basic_io_error(
                "performing the second conditional create (If-None-Match)",
                e,
            ));
        }
    }

    // NOTE: SlateDB's only If-Match dependency is the GC-boundary advance. PR #1816 (create-only boundary markers) removes that dependency; if/when it merges, drop this REQUIRED If-Match check (or downgrade it to a warning) — the store is otherwise fully usable without If-Match.
    // 6. [REQUIRED] If-Match overwrite: write to learn the version, then overwrite
    //    conditionally on that version. DigitalOcean Spaces returns HTTP 412 here.
    let put = object_store
        .put_opts(
            p_match,
            PutPayload::from_static(PAYLOAD),
            PutOptions::from(PutMode::Overwrite),
        )
        .await
        .map_err(|e| basic_io_error("writing the If-Match probe object", e))?;
    let version = UpdateVersion {
        e_tag: put.e_tag,
        version: put.version,
    };
    let if_match = object_store
        .put_opts(
            p_match,
            PutPayload::from_static(b"slatedb-probe-payload-2"),
            PutOptions::from(PutMode::Update(version)),
        )
        .await;
    match if_match {
        Ok(_) => {}
        Err(
            Error::Precondition { .. } | Error::NotImplemented { .. } | Error::NotSupported { .. },
        ) => {
            return Err(required_error(
                "conditional overwrite (If-Match / PutMode::Update)",
                Some("DigitalOcean Spaces"),
                "a conditional overwrite with the CORRECT version was rejected — SlateDB's \
                 garbage collector advances its collection boundary with a conditional \
                 overwrite, so on this store GC will silently fail to advance and storage will \
                 grow without bound. Use a store with full conditional-PUT support \
                 (AWS S3, Cloudflare R2, MinIO, GCS, Azure), configure object_store's \
                 DynamoDB-backed conditional commit (S3ConditionalPut::Dynamo), or — if you \
                 never run GC against this store — bypass this check with \
                 DbBuilder::skip_object_store_probe()",
            ));
        }
        Err(e) => {
            return Err(basic_io_error("performing the conditional overwrite", e));
        }
    }

    // 7. [REQUIRED] multipart upload (required for large SST flush).
    match object_store.put_multipart(p_mp).await {
        Ok(mut upload) => {
            if let Err(e) = upload.put_part(PutPayload::from_static(PAYLOAD)).await {
                return Err(multipart_error(e));
            }
            if let Err(e) = upload.complete().await {
                return Err(multipart_error(e));
            }
            let got = object_store
                .get(p_mp)
                .await
                .and_then_bytes()
                .await
                .map_err(|e| basic_io_error("reading back the multipart probe object", e))?;
            if got.as_ref() != PAYLOAD {
                return Err(required_error(
                    "multipart upload",
                    None,
                    "a multipart upload completed but read back mismatched bytes",
                ));
            }
        }
        Err(e) => return Err(multipart_error(e)),
    }

    // 8. [WARN] custom attributes (metadata). On unsupported, SlateDB disables
    //    ULID write-verification and falls back automatically.
    let mut attributes = Attributes::new();
    attributes.insert(
        Attribute::Metadata(Cow::Borrowed("slatedb_probe")),
        AttributeValue::from("1"),
    );
    let attr_result = object_store
        .put_opts(
            p_attr,
            PutPayload::from_static(PAYLOAD),
            PutOptions {
                attributes,
                mode: PutMode::Overwrite,
                ..Default::default()
            },
        )
        .await;
    match attr_result {
        Ok(_) => {}
        Err(Error::NotImplemented { .. } | Error::NotSupported { .. }) => {
            warn!(
                "object-store preflight: custom object attributes (metadata) are unsupported on \
                 this store; SlateDB's ULID write-verification will be disabled (it falls back \
                 automatically)"
            );
        }
        Err(e) => {
            warn!(
                "object-store preflight: probing custom attributes failed ({e}); SlateDB's ULID \
                 write-verification may be unavailable"
            );
        }
    }

    // 9. [WARN] copy. Used by checkpoint/clone.
    match object_store.copy(p_basic, p_copy).await {
        Ok(()) => match object_store.get(p_copy).await.and_then_bytes().await {
            Ok(bytes) if bytes.as_ref() == PAYLOAD => {}
            Ok(_) => warn!(
                "object-store preflight: object copy produced mismatched bytes — \
                     checkpoint/clone may be unreliable"
            ),
            Err(e) => warn!(
                "object-store preflight: reading back a copied object failed ({e}) — \
                     checkpoint/clone may be unavailable"
            ),
        },
        Err(e) => warn!(
            "object-store preflight: object copy unsupported ({e}) — checkpoint/clone will be \
             unavailable"
        ),
    }

    // 10. [WARN] list_with_offset. Best-effort.
    {
        use futures::StreamExt;
        let mut stream = object_store.list_with_offset(Some(probe_prefix), p_basic);
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                warn!("object-store preflight: list_with_offset failed ({e})");
                break;
            }
        }
    }

    debug!("object store capability preflight passed");
    Ok(())
}

/// Builds a REQUIRED-capability error message that (a) names the capability,
/// (b) names the likely culprit store class when known, and (c) mentions the
/// opt-out flag.
fn required_error(capability: &str, culprit: Option<&str>, detail: &str) -> crate::Error {
    let culprit = match culprit {
        Some(c) => format!(" Likely culprit: {c}."),
        None => String::new(),
    };
    crate::Error::invalid(format!(
        "object-store preflight failed: the configured object store does not support the \
         REQUIRED capability '{capability}'. {detail}.{culprit} To bypass this check, call \
         DbBuilder::skip_object_store_probe()."
    ))
}

fn multipart_error(e: Error) -> crate::Error {
    match e {
        Error::NotImplemented { .. } | Error::NotSupported { .. } => required_error(
            "multipart upload",
            None,
            "multipart upload unsupported (required for large SST flush)",
        ),
        e => basic_io_error("performing a multipart upload", e),
    }
}

fn basic_io_error(ctx: &str, e: Error) -> crate::Error {
    crate::Error::invalid(format!(
        "object-store preflight failed while {ctx}: {e}. If you are confident the store is \
         configured correctly, bypass this check with DbBuilder::skip_object_store_probe()."
    ))
}

/// Small helper to collect a `GetResult` into bytes in one `.await` chain.
trait GetResultExt {
    async fn and_then_bytes(self) -> Result<bytes::Bytes, Error>;
}

impl GetResultExt for Result<object_store::GetResult, Error> {
    async fn and_then_bytes(self) -> Result<bytes::Bytes, Error> {
        self?.bytes().await
    }
}

#[cfg(test)]
mod tests {
    use super::probe_object_store;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as OsResult,
    };
    use std::fmt;
    use std::sync::Arc;

    /// Wrapper that delegates every `ObjectStore` method to an inner `InMemory`
    /// except for the behaviour each test needs to override.
    struct Wrapper {
        inner: Arc<InMemory>,
        /// When true, `PutMode::Create` behaves like `Overwrite` (never rejects).
        ignore_if_none_match: bool,
        /// When true, `PutMode::Update` (If-Match) is rejected with `Precondition`.
        reject_if_match: bool,
        /// When true, `put_multipart_opts` returns `NotImplemented`.
        reject_multipart: bool,
    }

    impl Wrapper {
        fn new() -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                ignore_if_none_match: false,
                reject_if_match: false,
                reject_multipart: false,
            }
        }
    }

    impl fmt::Debug for Wrapper {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Wrapper")
        }
    }

    impl fmt::Display for Wrapper {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Wrapper")
        }
    }

    #[async_trait]
    impl ObjectStore for Wrapper {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            if self.ignore_if_none_match && matches!(opts.mode, PutMode::Create) {
                // Model a store that doesn't enforce If-None-Match: treat Create
                // like Overwrite, never returning AlreadyExists/Precondition.
                let mut overwrite = opts;
                overwrite.mode = PutMode::Overwrite;
                return self.inner.put_opts(location, payload, overwrite).await;
            }
            if self.reject_if_match && matches!(opts.mode, PutMode::Update(_)) {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "injected If-Match rejection (models DigitalOcean Spaces)".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            if self.reject_multipart {
                return Err(Error::NotImplemented {
                    operation: "put_multipart_opts".to_string(),
                    implementer: "Wrapper".to_string(),
                });
            }
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn in_memory_store_passes_probe() {
        // The in-memory store supports all required capabilities.
        let store = InMemory::new();
        let path = Path::from("test-db");
        probe_object_store(&path, &store)
            .await
            .expect("in-memory store passes all required probe steps");
    }

    #[tokio::test]
    async fn store_without_if_none_match_enforcement_fails() {
        let mut wrapper = Wrapper::new();
        wrapper.ignore_if_none_match = true;
        let path = Path::from("test-db");
        let err = probe_object_store(&path, &wrapper)
            .await
            .expect_err("store that ignores If-None-Match must fail the probe");
        let msg = err.to_string();
        assert!(
            msg.contains("conditional create") || msg.contains("If-None-Match"),
            "error should mention conditional create / If-None-Match, got: {msg}"
        );
    }

    #[tokio::test]
    async fn store_rejecting_if_match_fails() {
        let mut wrapper = Wrapper::new();
        wrapper.reject_if_match = true;
        let path = Path::from("test-db");
        let err = probe_object_store(&path, &wrapper)
            .await
            .expect_err("store that rejects If-Match must fail the probe");
        let msg = err.to_string();
        assert!(
            msg.contains("If-Match") || msg.contains("DigitalOcean Spaces"),
            "error should mention If-Match / DigitalOcean Spaces, got: {msg}"
        );
    }

    #[tokio::test]
    async fn store_without_multipart_fails() {
        let mut wrapper = Wrapper::new();
        wrapper.reject_multipart = true;
        let path = Path::from("test-db");
        let err = probe_object_store(&path, &wrapper)
            .await
            .expect_err("store without multipart must fail the probe");
        let msg = err.to_string();
        assert!(
            msg.contains("multipart"),
            "error should mention multipart, got: {msg}"
        );
    }
}
