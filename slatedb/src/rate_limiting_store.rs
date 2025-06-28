//! A rate limiting wrapper for [`ObjectStore`] implementations.
//!
//! [`RateLimitingStore`] uses a simple token bucket implementation to
//! throttle calls to an underlying [`ObjectStore`].  Limits are provided
//! via [`RateLimitingRules`] which can be built using
//! [`RateLimitingRulesBuilder`].

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

use crate::clock::{DefaultSystemClock, SystemClock};
use tracing::warn;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{once, BoxStream},
    StreamExt,
};
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, UploadPart,
};

/// Trait for rate limiting policies.
#[async_trait]
pub trait RateLimitingPolicy: Send + Sync + 'static {
    /// Acquire resources for the specified operation at the given cost.
    async fn acquire(&self, op: Operation, cost: u32);
}

/// Simple token bucket implemented using a counting semaphore.
///
/// Tokens are replenished periodically on a background task and calls await
/// permits from the [`Semaphore`].
#[derive(Debug)]
pub struct TokenBucket {
    semaphore: Arc<Semaphore>,
}

#[allow(dead_code)]
impl TokenBucket {
    /// Interval between token refills in milliseconds.
    const TICK_MS: Duration = Duration::from_millis(100);

    pub fn new(rate: u32) -> Self {
        Self::new_with_clock(rate, Arc::new(DefaultSystemClock::default()))
    }

    pub fn new_with_clock(rate: u32, clock: Arc<dyn SystemClock>) -> Self {
        let semaphore = Arc::new(Semaphore::new(rate as usize));
        let per_sec = rate as u64;
        let this_sem = semaphore.clone();
        tokio::spawn(async move {
            let mut last = clock.now();
            let mut leftover_ms = 0u64;
            loop {
                tokio::time::sleep(Self::TICK_MS).await;
                let now = clock.now();
                let elapsed_ms = now.duration_since(last).unwrap_or_default().as_millis() as u64;
                last = now;
                let total_ms = leftover_ms + elapsed_ms * per_sec;
                let add = (total_ms / 1_000) as u32;
                leftover_ms = total_ms % 1_000;
                let available = this_sem.available_permits() as u32;
                if add > 0 && available < rate {
                    let to_add = add.min(rate - available) as usize;
                    this_sem.add_permits(to_add);
                }
            }
        });

        Self { semaphore }
    }
}

#[async_trait]
impl RateLimitingPolicy for TokenBucket {
    async fn acquire(&self, op: Operation, cost: u32) {
        if self.semaphore.available_permits() < cost as usize {
            warn!(?op, "rate limited");
        }
        self.semaphore
            .acquire_many(cost)
            .await
            .expect("semaphore closed")
            .forget();
    }
}

/// Operations that can be rate limited.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Operation {
    Put,
    PutOpts,
    PutMultipart,
    PutMultipartOpts,
    Get,
    GetOpts,
    GetRange,
    GetRanges,
    Head,
    Delete,
    List,
    ListWithOffset,
    ListWithDelimiter,
    Copy,
    Rename,
    CopyIfNotExists,
    RenameIfNotExists,
    MultipartPutPart,
    MultipartComplete,
    MultipartAbort,
}

/// Configuration for rate limiting behavior.
pub struct RateLimitingRules {
    pub(crate) limits: HashMap<Operation, Arc<dyn RateLimitingPolicy>>,
    pub(crate) total: Option<Arc<dyn RateLimitingPolicy>>,
    pub(crate) cost_fn: Box<dyn Fn(Operation) -> u32 + Send + Sync>,
}

impl Default for RateLimitingRules {
    fn default() -> Self {
        Self {
            limits: HashMap::new(),
            total: None,
            cost_fn: Box::new(|_| 1u32),
        }
    }
}

/// Builder for [`RateLimitingRules`].
pub struct RateLimitingRulesBuilder {
    limits: HashMap<Operation, Arc<dyn RateLimitingPolicy>>,
    total: Option<Arc<dyn RateLimitingPolicy>>,
    cost_fn: Option<Box<dyn Fn(Operation) -> u32 + Send + Sync>>,
}

impl Default for RateLimitingRulesBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimitingRulesBuilder {
    pub fn new() -> Self {
        Self {
            limits: HashMap::new(),
            total: None,
            cost_fn: None,
        }
    }

    /// Set a per-second limit for a specific operation.
    pub fn limit(mut self, op: Operation, policy: Box<dyn RateLimitingPolicy>) -> Self {
        self.limits.insert(op, policy.into());
        self
    }

    /// Set a total per-second limit for all operations combined. This rule
    /// is evaluated after per-operation limits.
    pub fn total_limit(mut self, policy: Box<dyn RateLimitingPolicy>) -> Self {
        self.total = Some(policy.into());
        self
    }

    /// Provide a cost calculation function used for each operation.
    pub fn cost_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(Operation) -> u32 + Send + Sync + 'static,
    {
        self.cost_fn = Some(Box::new(f));
        self
    }

    pub fn build(self) -> RateLimitingRules {
        RateLimitingRules {
            limits: self.limits,
            total: self.total,
            cost_fn: self.cost_fn.unwrap_or_else(|| Box::new(|_| 1u32)),
        }
    }
}

/// Shared state used by [`RateLimitingStore`].
pub(crate) struct RateLimitingState {
    /// Per-operation token buckets.
    limits: HashMap<Operation, Arc<dyn RateLimitingPolicy>>,
    /// Optional token bucket for the total allowed rate.
    total: Option<Arc<dyn RateLimitingPolicy>>,
    /// Function that determines the cost of each call.
    cost_fn: Box<dyn Fn(Operation) -> u32 + Send + Sync>,
}

impl std::fmt::Debug for RateLimitingState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitingState").finish()
    }
}

impl RateLimitingState {
    pub(crate) fn new(rules: RateLimitingRules) -> Self {
        Self {
            limits: rules.limits,
            total: rules.total,
            cost_fn: Box::new(rules.cost_fn),
        }
    }

    async fn acquire(&self, op: Operation) {
        let cost = (self.cost_fn)(op);
        if let Some(limit) = self.limits.get(&op) {
            limit.acquire(op, cost).await;
        }
        if let Some(total) = &self.total {
            total.acquire(op, cost).await;
        }
    }
}

/// Store wrapper that rate limits calls to the inner [`ObjectStore`].
///
/// Each call incurs a "cost" which by default is `1`.  Before delegating an
/// operation to the wrapped store a token bucket is consulted to ensure the
/// rate limit is not exceeded.
#[derive(Debug)]
pub(crate) struct RateLimitingStore<T: ObjectStore> {
    inner: Arc<T>,
    state: Arc<RateLimitingState>,
}

impl<T: ObjectStore> RateLimitingStore<T> {
    /// Create a new [`RateLimitingStore`] wrapping `inner` with the provided [`RateLimitingRules`].
    pub fn new(inner: T, rules: RateLimitingRules) -> Self {
        let state = Arc::new(RateLimitingState::new(rules));
        Self {
            inner: Arc::new(inner),
            state,
        }
    }
}

impl<T: ObjectStore> std::fmt::Display for RateLimitingStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RateLimitingStore")
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for RateLimitingStore<T> {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.state.acquire(Operation::Put).await;
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.state.acquire(Operation::PutOpts).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.state.acquire(Operation::PutMultipart).await;
        let upload = self.inner.put_multipart(location).await?;
        Ok(Box::new(RateLimitedUpload {
            upload,
            state: Arc::clone(&self.state),
        }))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.state.acquire(Operation::PutMultipartOpts).await;
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(RateLimitedUpload {
            upload,
            state: Arc::clone(&self.state),
        }))
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.state.acquire(Operation::Get).await;
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.state.acquire(Operation::GetOpts).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        self.state.acquire(Operation::GetRange).await;
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.state.acquire(Operation::GetRanges).await;
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.state.acquire(Operation::Head).await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.state.acquire(Operation::Delete).await;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        once(async move {
            state.acquire(Operation::List).await;
            inner.list(prefix.as_ref())
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let offset = offset.clone();
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        once(async move {
            state.acquire(Operation::ListWithOffset).await;
            inner.list_with_offset(prefix.as_ref(), &offset)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.state.acquire(Operation::ListWithDelimiter).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.state.acquire(Operation::Copy).await;
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.state.acquire(Operation::Rename).await;
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.state.acquire(Operation::CopyIfNotExists).await;
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.state.acquire(Operation::RenameIfNotExists).await;
        self.inner.rename_if_not_exists(from, to).await
    }
}

/// [`MultipartUpload`] wrapper that applies rate limiting to each part upload.
#[derive(Debug)]
struct RateLimitedUpload {
    upload: Box<dyn MultipartUpload>,
    state: Arc<RateLimitingState>,
}

#[async_trait]
impl MultipartUpload for RateLimitedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part = self.upload.put_part(data);
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state.acquire(Operation::MultipartPutPart).await;
            part.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.state.acquire(Operation::MultipartComplete).await;
        self.upload.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.state.acquire(Operation::MultipartAbort).await;
        self.upload.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{path::Path, ObjectStore, PutPayload};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn test_put_rate_limit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rules = RateLimitingRulesBuilder::new()
            .limit(Operation::Put, Box::new(TokenBucket::new(1)))
            .build();
        let rate_store = RateLimitingStore::new(store, rules);

        let start = Instant::now();
        rate_store
            .put(&Path::from("a"), PutPayload::from("1"))
            .await
            .unwrap();
        rate_store
            .put(&Path::from("b"), PutPayload::from("2"))
            .await
            .unwrap();
        assert!(start.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_total_rate_limit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rules = RateLimitingRulesBuilder::new()
            .total_limit(Box::new(TokenBucket::new(1)))
            .build();
        let rate_store = RateLimitingStore::new(store, rules);

        let start = Instant::now();
        rate_store
            .put(&Path::from("a"), PutPayload::from("1"))
            .await
            .unwrap();
        rate_store.get(&Path::from("a")).await.unwrap();
        assert!(start.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_cost_function() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rules = RateLimitingRulesBuilder::new()
            .total_limit(Box::new(TokenBucket::new(2)))
            .cost_fn(|_| 2)
            .build();
        let rate_store = RateLimitingStore::new(store, rules);

        let start = Instant::now();
        rate_store
            .put(&Path::from("a"), PutPayload::from("1"))
            .await
            .unwrap();
        rate_store.get(&Path::from("a")).await.unwrap();
        assert!(start.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_multipart_part_limit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rules = RateLimitingRulesBuilder::new()
            .limit(Operation::MultipartPutPart, Box::new(TokenBucket::new(1)))
            .build();
        let rate_store = RateLimitingStore::new(store, rules);

        let mut upload = rate_store.put_multipart(&Path::from("a")).await.unwrap();

        let start = Instant::now();
        upload.put_part(PutPayload::from("1")).await.unwrap();
        upload.put_part(PutPayload::from("2")).await.unwrap();
        assert!(start.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_total_overrides_per_op_limit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rules = RateLimitingRulesBuilder::new()
            .limit(Operation::Put, Box::new(TokenBucket::new(10)))
            .total_limit(Box::new(TokenBucket::new(1)))
            .build();
        let rate_store = RateLimitingStore::new(store, rules);

        let start = Instant::now();
        rate_store
            .put(&Path::from("a"), PutPayload::from("1"))
            .await
            .unwrap();
        rate_store
            .put(&Path::from("b"), PutPayload::from("2"))
            .await
            .unwrap();
        assert!(start.elapsed() >= Duration::from_secs(1));
    }
}
