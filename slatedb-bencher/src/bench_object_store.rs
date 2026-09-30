//! An object store wrapper for benchmarks.
//!
//! The wrapper does two things:
//!
//! - It counts the GET requests and the bytes that SlateDB reads from SST
//!   files. The counters give exact request numbers per read call, which do
//!   not depend on the machine or the network.
//! - It can add a delay to each read. The delay comes from a quantile table,
//!   so a run on an in-memory store shows the round trips of a remote store.
//!
//! Writes, lists, and deletes pass through with no delay and no count.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

/// A read latency distribution as a quantile table.
///
/// Each entry is a quantile in `0.0..=1.0` and its latency in milliseconds.
/// A sample draws a uniform number and interpolates between the two entries
/// around it. The sampled percentiles then match the table.
#[derive(Clone, Debug)]
pub struct DelayProfile {
    points: Vec<(f64, f64)>,
}

impl DelayProfile {
    /// Builds a profile from the measured p50, p90, p95, and p99.
    ///
    /// The table has no data below p50 and above p99. The profile uses
    /// `0.8 * p50` as the minimum and `1.5 * p99` as the maximum.
    pub fn from_percentiles(p50: f64, p90: f64, p95: f64, p99: f64) -> Self {
        Self {
            points: vec![
                (0.0, p50 * 0.8),
                (0.5, p50),
                (0.9, p90),
                (0.95, p95),
                (0.99, p99),
                (1.0, p99 * 1.5),
            ],
        }
    }

    /// A profile with one fixed delay.
    pub fn fixed(millis: u64) -> Self {
        let millis = millis as f64;
        Self {
            points: vec![(0.0, millis), (1.0, millis)],
        }
    }

    /// GET latency of S3 in one region, measured from an `m6id.large`
    /// instance. Source: the RFC author, September 2026.
    pub fn s3() -> Self {
        Self::from_percentiles(26.79, 46.85, 69.02, 113.35)
    }

    /// GET latency of S3 Express One Zone, measured from an `m6id.large`
    /// instance. Source: the RFC author, September 2026.
    pub fn s3_express() -> Self {
        Self::from_percentiles(2.48, 3.77, 5.16, 9.29)
    }

    fn sample(&self, rng: &mut XorShiftRng) -> Duration {
        let u: f64 = rng.random();
        let mut millis = self.points[self.points.len() - 1].1;
        for pair in self.points.windows(2) {
            let (q0, v0) = pair[0];
            let (q1, v1) = pair[1];
            if u <= q1 {
                let t = if q1 > q0 { (u - q0) / (q1 - q0) } else { 0.0 };
                millis = v0 + (v1 - v0) * t;
                break;
            }
        }
        Duration::from_secs_f64(millis / 1000.0)
    }
}

/// A snapshot of the request counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// GET requests for `.sst` objects.
    pub sst_gets: u64,
    /// Bytes read from `.sst` objects.
    pub sst_bytes: u64,
    /// GET requests for all other objects, for example manifests.
    pub other_gets: u64,
}

/// An object store that counts SST reads and can delay them.
pub struct BenchObjectStore {
    inner: Arc<dyn ObjectStore>,
    delay: Option<DelayProfile>,
    sst_gets: AtomicU64,
    sst_bytes: AtomicU64,
    other_gets: AtomicU64,
    rng: Mutex<XorShiftRng>,
}

impl BenchObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, delay: Option<DelayProfile>) -> Self {
        Self {
            inner,
            delay,
            sst_gets: AtomicU64::new(0),
            sst_bytes: AtomicU64::new(0),
            other_gets: AtomicU64::new(0),
            rng: Mutex::new(XorShiftRng::seed_from_u64(0)),
        }
    }

    /// Returns the current counter values.
    pub fn counters(&self) -> Counters {
        Counters {
            sst_gets: self.sst_gets.load(Ordering::Relaxed),
            sst_bytes: self.sst_bytes.load(Ordering::Relaxed),
            other_gets: self.other_gets.load(Ordering::Relaxed),
        }
    }

    async fn delay(&self) {
        let Some(profile) = &self.delay else {
            return;
        };
        let delay = profile.sample(&mut self.rng.lock().expect("lock failed"));
        tokio::time::sleep(delay).await;
    }

    fn count_get(&self, location: &Path, requests: u64, bytes: u64) {
        if location.as_ref().ends_with(".sst") {
            self.sst_gets.fetch_add(requests, Ordering::Relaxed);
            self.sst_bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.other_gets.fetch_add(requests, Ordering::Relaxed);
        }
    }
}

impl fmt::Debug for BenchObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BenchObjectStore({})", self.inner)
    }
}

impl fmt::Display for BenchObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BenchObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for BenchObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            return self.inner.get_opts(location, options).await;
        }
        self.delay().await;
        let result = self.inner.get_opts(location, options).await?;
        let bytes = result.range.end.saturating_sub(result.range.start);
        self.count_get(location, 1, bytes);
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.delay().await;
        let result = self.inner.get_ranges(location, ranges).await?;
        let bytes = result.iter().map(|b| b.len() as u64).sum();
        self.count_get(location, ranges.len() as u64, bytes);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn test_counts_sst_range_reads() {
        let store = BenchObjectStore::new(Arc::new(InMemory::new()), None);
        let sst = Path::from("compacted/x.sst");
        let manifest = Path::from("manifest/1.manifest");
        store
            .put(&sst, PutPayload::from_static(&[7u8; 1000]))
            .await
            .unwrap();
        store
            .put(&manifest, PutPayload::from_static(&[1u8; 10]))
            .await
            .unwrap();

        store.get_range(&sst, 0..100).await.unwrap();
        store.get_range(&sst, 500..800).await.unwrap();
        store.get(&manifest).await.unwrap().bytes().await.unwrap();
        store.head(&sst).await.unwrap();

        assert_eq!(
            store.counters(),
            Counters {
                sst_gets: 2,
                sst_bytes: 400,
                other_gets: 1,
            }
        );
    }

    #[test]
    fn test_profile_matches_percentiles() {
        let profile = DelayProfile::s3();
        let mut rng = XorShiftRng::seed_from_u64(1);
        let mut samples: Vec<f64> = (0..100_000)
            .map(|_| profile.sample(&mut rng).as_secs_f64() * 1000.0)
            .collect();
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = samples[samples.len() / 2];
        let p99 = samples[samples.len() * 99 / 100];
        assert!((p50 - 26.79).abs() / 26.79 < 0.05, "p50 was {p50}");
        assert!((p99 - 113.35).abs() / 113.35 < 0.10, "p99 was {p99}");
    }

    #[test]
    fn test_fixed_profile() {
        let profile = DelayProfile::fixed(20);
        let mut rng = XorShiftRng::seed_from_u64(1);
        for _ in 0..100 {
            assert_eq!(profile.sample(&mut rng), Duration::from_millis(20));
        }
    }
}
