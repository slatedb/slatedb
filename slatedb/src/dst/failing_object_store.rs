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
use slatedb_common::clock::SystemClock;

use crate::rand::DbRand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDirection {
    Upstream,
    Downstream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    PutOpts,
    GetOpts,
    GetRange,
    GetRanges,
    Head,
    List,
    ListWithOffset,
    Delete,
    Copy,
    Rename,
}

#[derive(Debug, Clone)]
pub enum ToxicKind {
    Latency { latency: Duration, jitter: Duration },
    Bandwidth { bytes_per_sec: u64 },
    ResetPeer,
    SlowClose { delay: Duration },
}

#[derive(Debug, Clone)]
pub struct Toxic {
    pub name: String,
    pub kind: ToxicKind,
    pub direction: StreamDirection,
    pub toxicity: f64,
    pub operations: Vec<Operation>,
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HttpFailBefore {
    pub percentage: u8,
    pub status_code: u16,
    pub operations: Vec<Operation>,
    pub path_prefix: Option<String>,
}

#[derive(Debug)]
struct ControllerState {
    toxics: RwLock<Vec<Toxic>>,
    http_fail_before: RwLock<Option<HttpFailBefore>>,
    rand: Arc<DbRand>,
}

#[derive(Clone, Debug)]
pub struct FailingObjectStoreController {
    state: Arc<ControllerState>,
}

impl FailingObjectStoreController {
    pub(crate) fn new(rand: Arc<DbRand>) -> Self {
        Self {
            state: Arc::new(ControllerState {
                toxics: RwLock::new(Vec::new()),
                http_fail_before: RwLock::new(None),
                rand,
            }),
        }
    }

    pub fn clear_toxics(&self) {
        self.state.toxics.write().clear();
    }

    pub fn add_toxic(&self, toxic: Toxic) {
        self.state.toxics.write().push(toxic);
    }

    pub fn clear_http_failures(&self) {
        *self.state.http_fail_before.write() = None;
    }

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
pub struct FailingObjectStore {
    inner: Arc<dyn ObjectStore>,
    controller: FailingObjectStoreController,
    clock: Arc<dyn SystemClock>,
}

impl FailingObjectStore {
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
                        .advance(*latency + self.controller.sample_jitter(*jitter))
                        .await;
                }
                ToxicKind::Bandwidth { bytes_per_sec } => {
                    if let Some(delay) = bandwidth_delay(size_bytes, *bytes_per_sec) {
                        self.clock.advance(delay).await;
                    }
                }
                ToxicKind::ResetPeer => return Err(reset_peer_error()),
                ToxicKind::SlowClose { delay } if allow_slow_close => {
                    self.clock.advance(*delay).await;
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
        operation: Operation,
        from: &Path,
        to: Option<&Path>,
        request_size: u64,
        inner: impl std::future::Future<Output = object_store::Result<PutResult>>,
    ) -> object_store::Result<PutResult> {
        let paths = collect_paths(from, to);
        self.apply_request_faults(operation, &paths, request_size)
            .await?;
        let result = inner.await?;
        self.apply_response_faults(operation, &paths, 0).await?;
        Ok(result)
    }

    async fn unit_result(
        &self,
        operation: Operation,
        from: &Path,
        to: Option<&Path>,
        inner: impl std::future::Future<Output = object_store::Result<()>>,
    ) -> object_store::Result<()> {
        let paths = collect_paths(from, to);
        self.apply_request_faults(operation, &paths, 0).await?;
        inner.await?;
        self.apply_response_faults(operation, &paths, 0).await?;
        Ok(())
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
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let paths = vec![location.clone()];
        self.apply_request_faults(Operation::GetOpts, &paths, 0)
            .await?;
        let result = self.inner.get_opts(location, options).await?;
        self.apply_response_faults(
            Operation::GetOpts,
            &paths,
            result.range.end.saturating_sub(result.range.start),
        )
        .await?;
        Ok(result)
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        let paths = vec![location.clone()];
        self.apply_request_faults(Operation::GetRange, &paths, 0)
            .await?;
        let result = self.inner.get_range(location, range).await?;
        self.apply_response_faults(Operation::GetRange, &paths, result.len() as u64)
            .await?;
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let paths = vec![location.clone()];
        self.apply_request_faults(Operation::GetRanges, &paths, 0)
            .await?;
        let result = self.inner.get_ranges(location, ranges).await?;
        let total_size = result.iter().map(|bytes| bytes.len() as u64).sum();
        self.apply_response_faults(Operation::GetRanges, &paths, total_size)
            .await?;
        Ok(result)
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let paths = vec![location.clone()];
        self.apply_request_faults(Operation::Head, &paths, 0)
            .await?;
        let result = self.inner.head(location).await?;
        self.apply_response_faults(Operation::Head, &paths, 0)
            .await?;
        Ok(result)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_like(
            Operation::PutOpts,
            location,
            None,
            payload.content_length() as u64,
            self.inner.put_opts(location, payload, opts),
        )
        .await
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
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.unit_result(
            Operation::Delete,
            location,
            None,
            self.inner.delete(location),
        )
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let this = self.clone();
        let prefix_owned = prefix.cloned();
        stream::once(async move {
            let paths = prefix_owned.iter().cloned().collect::<Vec<_>>();
            this.apply_request_faults(Operation::List, &paths, 0)
                .await?;

            let downstream = this.controller.sample_matching_toxics(
                StreamDirection::Downstream,
                Operation::List,
                &paths,
            );
            if downstream
                .iter()
                .any(|toxic| matches!(toxic, ToxicKind::ResetPeer))
            {
                let error_stream: BoxStream<'static, object_store::Result<ObjectMeta>> =
                    stream::once(async { Err(reset_peer_error()) }).boxed();
                return Ok::<_, object_store::Error>(error_stream);
            }

            let pre_return = downstream
                .iter()
                .filter(|toxic| !matches!(toxic, ToxicKind::Bandwidth { .. }))
                .cloned()
                .collect::<Vec<_>>();
            this.apply_toxics(&pre_return, 0, true).await?;

            let bandwidth = downstream
                .into_iter()
                .filter_map(|toxic| match toxic {
                    ToxicKind::Bandwidth { bytes_per_sec } => Some(bytes_per_sec),
                    _ => None,
                })
                .collect::<Vec<_>>();

            let clock = Arc::clone(&this.clock);
            let stream = inner
                .list(prefix_owned.as_ref())
                .then(move |result| {
                    let clock = Arc::clone(&clock);
                    let bandwidth = bandwidth.clone();
                    async move {
                        if let Ok(meta) = &result {
                            let size = estimate_meta_size(meta);
                            for bytes_per_sec in &bandwidth {
                                if let Some(delay) = bandwidth_delay(size, *bytes_per_sec) {
                                    clock.advance(delay).await;
                                }
                            }
                        }
                        result
                    }
                })
                .boxed();
            Ok(stream)
        })
        .try_flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let this = self.clone();
        let prefix_owned = prefix.cloned();
        let offset_owned = offset.clone();
        stream::once(async move {
            let paths = collect_optional_paths(prefix_owned.as_ref(), Some(&offset_owned));
            this.apply_request_faults(Operation::ListWithOffset, &paths, 0)
                .await?;

            let downstream = this.controller.sample_matching_toxics(
                StreamDirection::Downstream,
                Operation::ListWithOffset,
                &paths,
            );
            if downstream
                .iter()
                .any(|toxic| matches!(toxic, ToxicKind::ResetPeer))
            {
                let error_stream: BoxStream<'static, object_store::Result<ObjectMeta>> =
                    stream::once(async { Err(reset_peer_error()) }).boxed();
                return Ok::<_, object_store::Error>(error_stream);
            }

            let pre_return = downstream
                .iter()
                .filter(|toxic| !matches!(toxic, ToxicKind::Bandwidth { .. }))
                .cloned()
                .collect::<Vec<_>>();
            this.apply_toxics(&pre_return, 0, true).await?;

            let bandwidth = downstream
                .into_iter()
                .filter_map(|toxic| match toxic {
                    ToxicKind::Bandwidth { bytes_per_sec } => Some(bytes_per_sec),
                    _ => None,
                })
                .collect::<Vec<_>>();

            let clock = Arc::clone(&this.clock);
            let stream = inner
                .list_with_offset(prefix_owned.as_ref(), &offset_owned)
                .then(move |result| {
                    let clock = Arc::clone(&clock);
                    let bandwidth = bandwidth.clone();
                    async move {
                        if let Ok(meta) = &result {
                            let size = estimate_meta_size(meta);
                            for bytes_per_sec in &bandwidth {
                                if let Some(delay) = bandwidth_delay(size, *bytes_per_sec) {
                                    clock.advance(delay).await;
                                }
                            }
                        }
                        result
                    }
                })
                .boxed();
            Ok(stream)
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.unit_result(Operation::Copy, from, Some(to), self.inner.copy(from, to))
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.unit_result(
            Operation::Rename,
            from,
            Some(to),
            self.inner.rename(from, to),
        )
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.unit_result(
            Operation::Copy,
            from,
            Some(to),
            self.inner.copy_if_not_exists(from, to),
        )
        .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.unit_result(
            Operation::Rename,
            from,
            Some(to),
            self.inner.rename_if_not_exists(from, to),
        )
        .await
    }
}

#[derive(Debug)]
pub struct HttpStatusError {
    pub status_code: u16,
}

impl fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "injected HTTP status {}", self.status_code)
    }
}

impl std::error::Error for HttpStatusError {}

fn collect_paths(primary: &Path, secondary: Option<&Path>) -> Vec<Path> {
    let mut paths = vec![primary.clone()];
    if let Some(secondary) = secondary {
        paths.push(secondary.clone());
    }
    paths
}

fn collect_optional_paths(primary: Option<&Path>, secondary: Option<&Path>) -> Vec<Path> {
    let mut paths = Vec::new();
    if let Some(primary) = primary {
        paths.push(primary.clone());
    }
    if let Some(secondary) = secondary {
        paths.push(secondary.clone());
    }
    paths
}

fn path_matches(prefix: &Option<String>, paths: &[Path]) -> bool {
    match prefix {
        Some(prefix) => paths
            .iter()
            .any(|path| path.to_string().starts_with(prefix)),
        None => true,
    }
}

fn bandwidth_delay(size_bytes: u64, bytes_per_sec: u64) -> Option<Duration> {
    if size_bytes == 0 || bytes_per_sec == 0 {
        return None;
    }

    let millis = ((size_bytes as u128) * 1000).div_ceil(bytes_per_sec as u128);
    Some(Duration::from_millis(
        u64::try_from(millis).unwrap_or(u64::MAX),
    ))
}

fn estimate_meta_size(meta: &ObjectMeta) -> u64 {
    let mut size = meta.location.to_string().len() as u64;
    size += meta.e_tag.as_ref().map_or(0, |tag| tag.len() as u64);
    size += meta
        .version
        .as_ref()
        .map_or(0, |version| version.len() as u64);
    size + 32
}

fn reset_peer_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "dst_reset_peer",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "dst toxic reset_peer injected a transport reset",
        )),
    }
}
