//! Fault-injecting object-store wrappers used by deterministic SlateDB tests.
//!
//! The public types in this module let tests install probabilistic latency,
//! bandwidth, connection reset, and synthetic HTTP failures on top of an
//! existing [`ObjectStore`] implementation.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::RwLock;
use rand::RngCore;
use slatedb::DbRand;
use slatedb_common::clock::SystemClock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Direction of traffic affected by a configured toxic.
pub enum StreamDirection {
    /// Apply the toxic before the wrapped store processes the request.
    Upstream,
    /// Apply the toxic after the wrapped store produces a response.
    Downstream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Object-store operations that can be targeted by a toxic or HTTP failure.
pub enum Operation {
    /// `put_opts` and multipart upload creation requests.
    PutOpts,
    /// `get_opts` requests.
    GetOpts,
    /// `get_range` requests.
    GetRange,
    /// `get_ranges` requests.
    GetRanges,
    /// `head` requests.
    Head,
    /// `list` and `list_with_delimiter` requests.
    List,
    /// `list_with_offset` requests.
    ListWithOffset,
    /// `delete` requests.
    Delete,
    /// `copy` and `copy_if_not_exists` requests.
    Copy,
    /// `rename` and `rename_if_not_exists` requests.
    Rename,
}

#[derive(Debug, Clone)]
/// Fault behavior that can be sampled and applied to a matching request or
/// response path.
pub enum ToxicKind {
    /// Advances the shared clock by a base latency plus sampled jitter.
    Latency {
        /// Fixed latency added whenever the toxic triggers.
        latency: Duration,
        /// Maximum additional latency sampled uniformly from zero.
        jitter: Duration,
    },
    /// Delays transfer completion based on payload size.
    Bandwidth {
        /// Effective transfer rate used to translate bytes into delay.
        bytes_per_sec: u64,
    },
    /// Fails the operation with a connection-reset style error.
    ResetPeer,
    /// Delays stream shutdown after the response is produced.
    SlowClose {
        /// Additional delay applied while closing the response stream.
        delay: Duration,
    },
}

#[derive(Debug, Clone)]
/// Configuration for a probabilistic toxic applied to matching operations.
pub struct Toxic {
    /// Human-readable label for the toxic.
    pub name: String,
    /// The fault behavior to apply when the toxic is sampled.
    pub kind: ToxicKind,
    /// Whether the toxic applies to the request or response side.
    pub direction: StreamDirection,
    /// Probability in the inclusive range `0.0..=1.0` that the toxic applies.
    pub toxicity: f64,
    /// Optional operation filter. An empty list matches every operation.
    pub operations: Vec<Operation>,
    /// Optional path-prefix filter. `None` matches every path.
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone)]
/// Configuration for a synthetic HTTP error returned before dispatching a
/// request to the wrapped store.
pub struct HttpFailBefore {
    /// Percentage in the inclusive range `0..=100` that the failure triggers.
    pub percentage: u8,
    /// HTTP status code exposed through [`HttpStatusError`].
    pub status_code: u16,
    /// Optional operation filter. An empty list matches every operation.
    pub operations: Vec<Operation>,
    /// Optional path-prefix filter. `None` matches every path.
    pub path_prefix: Option<String>,
}

#[derive(Debug)]
struct ControllerState {
    toxics: RwLock<Vec<Toxic>>,
    http_fail_before: RwLock<Option<HttpFailBefore>>,
    rand: Arc<DbRand>,
}

#[derive(Clone, Debug)]
/// Mutable controller used to install or clear failure modes on a
/// [`FailingObjectStore`].
pub struct FailingObjectStoreController {
    state: Arc<ControllerState>,
}

impl FailingObjectStoreController {
    /// Creates a new deterministic fault controller.
    ///
    /// ## Arguments
    /// - `rand`: RNG used to sample toxic probabilities, jitter, and synthetic
    ///   HTTP failures.
    pub fn new(rand: Arc<DbRand>) -> Self {
        Self {
            state: Arc::new(ControllerState {
                toxics: RwLock::new(Vec::new()),
                http_fail_before: RwLock::new(None),
                rand,
            }),
        }
    }

    /// Removes all configured toxics.
    pub fn clear_toxics(&self) {
        self.state.toxics.write().clear();
    }

    /// Adds a toxic to the active controller configuration.
    ///
    /// ## Arguments
    /// - `toxic`: The toxic to append to the controller's active set.
    pub fn add_toxic(&self, toxic: Toxic) {
        self.state.toxics.write().push(toxic);
    }

    /// Clears any configured synthetic HTTP failure.
    pub fn clear_http_failures(&self) {
        *self.state.http_fail_before.write() = None;
    }

    /// Installs a synthetic HTTP failure returned before dispatch.
    ///
    /// ## Arguments
    /// - `failure`: The failure policy to apply to matching operations.
    pub fn set_http_fail_before(&self, failure: HttpFailBefore) {
        *self.state.http_fail_before.write() = Some(failure);
    }

    fn sample_matching_toxics(
        &self,
        direction: StreamDirection,
        operation: Operation,
        paths: &[Path],
    ) -> Vec<ToxicKind> {
        let toxics = self.state.toxics.read().clone();
        toxics
            .into_iter()
            .filter(|toxic| toxic.direction == direction)
            .filter(|toxic| toxic.operations.is_empty() || toxic.operations.contains(&operation))
            .filter(|toxic| path_matches(&toxic.path_prefix, paths))
            .filter(|toxic| self.sample_probability(toxic.toxicity))
            .map(|toxic| toxic.kind)
            .collect()
    }

    fn maybe_http_fail_before(
        &self,
        operation: Operation,
        paths: &[Path],
    ) -> Option<object_store::Error> {
        let config = self.state.http_fail_before.read().clone()?;
        if !(config.operations.is_empty() || config.operations.contains(&operation)) {
            return None;
        }
        if !path_matches(&config.path_prefix, paths) {
            return None;
        }
        if !self.sample_percentage(config.percentage) {
            return None;
        }
        Some(object_store::Error::Generic {
            store: "dst_http_fail_before",
            source: Box::new(HttpStatusError {
                status_code: config.status_code,
            }),
        })
    }

    fn sample_probability(&self, probability: f64) -> bool {
        let probability = probability.clamp(0.0, 1.0);
        if probability <= 0.0 {
            return false;
        }
        if probability >= 1.0 {
            return true;
        }

        let sample = self.state.rand.rng().next_u64() as f64 / u64::MAX as f64;
        sample < probability
    }

    fn sample_percentage(&self, percentage: u8) -> bool {
        if percentage == 0 {
            return false;
        }
        if percentage >= 100 {
            return true;
        }

        (self.state.rand.rng().next_u32() % 100) < u32::from(percentage)
    }

    fn sample_jitter(&self, max_jitter: Duration) -> Duration {
        if max_jitter.is_zero() {
            return Duration::ZERO;
        }

        let upper = u64::try_from(max_jitter.as_millis()).unwrap_or(u64::MAX);
        if upper == 0 {
            return Duration::ZERO;
        }

        Duration::from_millis(self.state.rand.rng().next_u64() % (upper + 1))
    }
}

#[derive(Clone)]
/// [`ObjectStore`] wrapper that injects deterministic latency and failure modes
/// controlled by a [`FailingObjectStoreController`].
pub struct FailingObjectStore {
    inner: Arc<dyn ObjectStore>,
    controller: FailingObjectStoreController,
    clock: Arc<dyn SystemClock>,
}

impl FailingObjectStore {
    /// Wraps an object store with deterministic fault injection.
    ///
    /// ## Arguments
    /// - `inner`: The base object store to wrap.
    /// - `controller`: The mutable controller that configures injected faults.
    /// - `clock`: The clock advanced when latency or bandwidth toxics fire.
    ///
    /// ## Returns
    /// - `FailingObjectStore`: The wrapped object store.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        controller: FailingObjectStoreController,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            inner,
            controller,
            clock,
        }
    }

    async fn apply_toxics(
        &self,
        toxics: &[ToxicKind],
        size_bytes: u64,
        allow_slow_close: bool,
    ) -> object_store::Result<()> {
        for toxic in toxics {
            match toxic {
                ToxicKind::Latency { latency, jitter } => {
                    self.clock
                        .sleep(*latency + self.controller.sample_jitter(*jitter))
                        .await;
                }
                ToxicKind::Bandwidth { bytes_per_sec } => {
                    if let Some(delay) = bandwidth_delay(size_bytes, *bytes_per_sec) {
                        self.clock.sleep(delay).await;
                    }
                }
                ToxicKind::ResetPeer => return Err(reset_peer_error()),
                ToxicKind::SlowClose { delay } if allow_slow_close => {
                    self.clock.sleep(*delay).await;
                }
                ToxicKind::SlowClose { .. } => {}
            }
        }
        Ok(())
    }

    async fn apply_request_faults(
        &self,
        operation: Operation,
        paths: &[Path],
        request_size: u64,
    ) -> object_store::Result<()> {
        if let Some(error) = self.controller.maybe_http_fail_before(operation, paths) {
            return Err(error);
        }

        let toxics =
            self.controller
                .sample_matching_toxics(StreamDirection::Upstream, operation, paths);
        self.apply_toxics(&toxics, request_size, false).await
    }

    async fn apply_response_faults(
        &self,
        operation: Operation,
        paths: &[Path],
        response_size: u64,
    ) -> object_store::Result<()> {
        let toxics =
            self.controller
                .sample_matching_toxics(StreamDirection::Downstream, operation, paths);
        self.apply_toxics(&toxics, response_size, true).await
    }

    async fn put_like(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let size = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        self.apply_request_faults(Operation::PutOpts, &[location.clone()], size)
            .await?;
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.apply_response_faults(Operation::PutOpts, &[location.clone()], 0)
            .await?;
        Ok(result)
    }
}

impl fmt::Debug for FailingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingObjectStore({})", self.inner)
    }
}

impl fmt::Display for FailingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FailingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_like(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.apply_request_faults(Operation::PutOpts, &[location.clone()], 0)
            .await?;
        let result = self.inner.put_multipart(location).await?;
        self.apply_response_faults(Operation::PutOpts, &[location.clone()], 0)
            .await?;
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.apply_request_faults(Operation::PutOpts, &[location.clone()], 0)
            .await?;
        let result = self.inner.put_multipart_opts(location, opts).await?;
        self.apply_response_faults(Operation::PutOpts, &[location.clone()], 0)
            .await?;
        Ok(result)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.apply_request_faults(Operation::GetOpts, &[location.clone()], 0)
            .await?;
        let result = self.inner.get_opts(location, options).await?;
        let size = result.meta.size;
        self.apply_response_faults(Operation::GetOpts, &[location.clone()], size)
            .await?;
        Ok(result)
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        self.apply_request_faults(Operation::GetRange, &[location.clone()], 0)
            .await?;
        let result = self.inner.get_range(location, range).await?;
        let size = u64::try_from(result.len()).unwrap_or(u64::MAX);
        self.apply_response_faults(Operation::GetRange, &[location.clone()], size)
            .await?;
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.apply_request_faults(Operation::GetRanges, &[location.clone()], 0)
            .await?;
        let result = self.inner.get_ranges(location, ranges).await?;
        let size = result.iter().fold(0_u64, |total, bytes| {
            total.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        });
        self.apply_response_faults(Operation::GetRanges, &[location.clone()], size)
            .await?;
        Ok(result)
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.apply_request_faults(Operation::Head, &[location.clone()], 0)
            .await?;
        let result = self.inner.head(location).await?;
        self.apply_response_faults(Operation::Head, &[location.clone()], result.size)
            .await?;
        Ok(result)
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.apply_request_faults(Operation::Delete, &[location.clone()], 0)
            .await?;
        let result = self.inner.delete(location).await;
        self.apply_response_faults(Operation::Delete, &[location.clone()], 0)
            .await?;
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let this = self.clone();
        let this_for_response = self.clone();
        let inner = Arc::clone(&self.inner);
        let prefix_owned = prefix.cloned();
        let request_path = prefix_owned.clone().unwrap_or_default();
        let response_path = request_path.clone();
        stream::once(async move {
            this.apply_request_faults(Operation::List, std::slice::from_ref(&request_path), 0)
                .await
        })
        .map_ok(move |_| inner.list(prefix_owned.as_ref()))
        .try_flatten()
        .then(move |result| {
            let this = this_for_response.clone();
            let response_path = response_path.clone();
            async move {
                if let Ok(meta) = &result {
                    this.apply_response_faults(
                        Operation::List,
                        std::slice::from_ref(&response_path),
                        meta.size,
                    )
                    .await?;
                }
                result
            }
        })
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let this = self.clone();
        let this_for_response = self.clone();
        let inner = Arc::clone(&self.inner);
        let prefix_owned = prefix.cloned();
        let offset_owned = offset.clone();
        let request_paths = vec![
            prefix_owned.clone().unwrap_or_default(),
            offset_owned.clone(),
        ];
        let response_paths = request_paths.clone();
        stream::once(async move {
            this.apply_request_faults(Operation::ListWithOffset, &request_paths, 0)
                .await
        })
        .map_ok(move |_| inner.list_with_offset(prefix_owned.as_ref(), &offset_owned))
        .try_flatten()
        .then(move |result| {
            let this = this_for_response.clone();
            let response_paths = response_paths.clone();
            async move {
                if let Ok(meta) = &result {
                    this.apply_response_faults(
                        Operation::ListWithOffset,
                        &response_paths,
                        meta.size,
                    )
                    .await?;
                }
                result
            }
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.cloned().unwrap_or_default();
        self.apply_request_faults(Operation::List, std::slice::from_ref(&path), 0)
            .await?;
        let result = self.inner.list_with_delimiter(prefix).await?;
        let size = result
            .objects
            .iter()
            .fold(0_u64, |total, meta| total.saturating_add(meta.size));
        self.apply_response_faults(Operation::List, &[path], size)
            .await?;
        Ok(result)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.apply_request_faults(Operation::Copy, &[from.clone(), to.clone()], 0)
            .await?;
        self.inner.copy(from, to).await?;
        self.apply_response_faults(Operation::Copy, &[from.clone(), to.clone()], 0)
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.apply_request_faults(Operation::Rename, &[from.clone(), to.clone()], 0)
            .await?;
        self.inner.rename(from, to).await?;
        self.apply_response_faults(Operation::Rename, &[from.clone(), to.clone()], 0)
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.apply_request_faults(Operation::Copy, &[from.clone(), to.clone()], 0)
            .await?;
        self.inner.copy_if_not_exists(from, to).await?;
        self.apply_response_faults(Operation::Copy, &[from.clone(), to.clone()], 0)
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.apply_request_faults(Operation::Rename, &[from.clone(), to.clone()], 0)
            .await?;
        self.inner.rename_if_not_exists(from, to).await?;
        self.apply_response_faults(Operation::Rename, &[from.clone(), to.clone()], 0)
            .await
    }
}

#[derive(Debug)]
/// Error type used when [`HttpFailBefore`] injects a synthetic HTTP response.
pub struct HttpStatusError {
    status_code: u16,
}

impl HttpStatusError {
    /// Returns the configured HTTP status code for the synthetic failure.
    ///
    /// ## Returns
    /// - `u16`: The HTTP status code carried by this error.
    pub fn status_code(&self) -> u16 {
        self.status_code
    }
}

impl fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HTTP error before request dispatch: {}",
            self.status_code
        )
    }
}

impl std::error::Error for HttpStatusError {}

fn path_matches(prefix: &Option<String>, paths: &[Path]) -> bool {
    match prefix {
        None => true,
        Some(prefix) => paths
            .iter()
            .any(|path| path.as_ref().starts_with(prefix.as_str())),
    }
}

fn bandwidth_delay(size_bytes: u64, bytes_per_sec: u64) -> Option<Duration> {
    if size_bytes == 0 || bytes_per_sec == 0 {
        return None;
    }

    let millis = size_bytes.saturating_mul(1_000).div_ceil(bytes_per_sec);
    Some(Duration::from_millis(millis.max(1)))
}

fn reset_peer_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "dst_toxic_reset_peer",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "dst toxic reset peer",
        )),
    }
}
