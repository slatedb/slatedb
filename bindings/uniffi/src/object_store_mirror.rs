//! The local object mirror of RFC 0034 for the bindings.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::warn;
use object_store::ObjectStore as RawObjectStore;
use parking_lot::Mutex;
use slatedb::{SlateDbMirrorPolicy, SlateDbMirrorPolicyBuilder};
use slatedb_common::clock::SystemClock as Clock;
use slatedb_mirror::{
    MirrorError, ObjectStoreMirror, DEFAULT_DOWNLOAD_CONCURRENCY, DEFAULT_REMOTE_SCAN_INTERVAL,
};

use crate::clock::SystemClock;
use crate::error::{Error, SegmentFilterCallbackError, SlateDbError};
use crate::object_store::ObjectStore;
use crate::runtime;

/// Selects the segments whose compacted SSTs a mirror keeps on local disk.
/// The database reads the SSTs of all other segments from the wrapped store.
///
/// The mirror keeps one answer for each segment and manifest version. It asks
/// again when the database writes a new manifest, at a flush or a compaction.
///
/// - The empty prefix is the root segment. A database without a segment
///   extractor has only the root segment, so `false` for it turns the mirror
///   off.
/// - A changed answer takes effect at the next manifest. Then the mirror
///   downloads the SSTs of a segment that it starts to keep, and removes the
///   local copies of a segment that it stops keeping.
/// - While those downloads run, reads of the segment can fail with
///   `Error::Data`.
/// - An error from `should_mirror` counts as `false` for that manifest version.
///   The mirror logs a warning.
/// - A read or write that needs the answer waits for `should_mirror`, so it
///   must return quickly.
#[uniffi::export(with_foreign)]
pub trait SegmentFilter: Send + Sync {
    /// Returns true if the mirror keeps the compacted SSTs of the segment with
    /// prefix `segment` on local disk.
    fn should_mirror(&self, segment: Vec<u8>) -> Result<bool, SegmentFilterCallbackError>;
}

/// The number of manifest versions that keep their answers. The database reads
/// with the current manifest while the policy processes the next one, so two
/// versions are in use at the same time.
const ANSWERED_MANIFESTS: usize = 2;

/// Gives the policy the answers of a [`SegmentFilter`], one for each segment
/// and manifest version. The policy asks on each compacted SST read and write,
/// so most answers must come from memory and not from the foreign filter.
struct SegmentFilterAdapter {
    filter: Arc<dyn SegmentFilter>,
    /// The answers by manifest ID and segment, for the newest manifest IDs.
    answers: Mutex<BTreeMap<u64, HashMap<Vec<u8>, bool>>>,
}

impl SegmentFilterAdapter {
    fn new(filter: Arc<dyn SegmentFilter>) -> Self {
        Self {
            filter,
            answers: Mutex::new(BTreeMap::new()),
        }
    }

    fn should_mirror(&self, manifest_id: u64, segment: &[u8]) -> bool {
        let cached = self
            .answers
            .lock()
            .get(&manifest_id)
            .and_then(|answers| answers.get(segment).copied());
        if let Some(answer) = cached {
            return answer;
        }
        // Call the filter without the lock, so that a slow filter does not
        // block the answers of other segments.
        let answer = self
            .filter
            .should_mirror(segment.to_vec())
            .unwrap_or_else(|error| {
                warn!(
                    "segment filter failed, the mirror does not keep the segment \
                     [manifest_id={manifest_id}, segment={segment:?}, error={error}]"
                );
                false
            });
        let mut answers = self.answers.lock();
        // Keep the first answer, so that concurrent calls agree.
        let answer = *answers
            .entry(manifest_id)
            .or_default()
            .entry(segment.to_vec())
            .or_insert(answer);
        while answers.len() > ANSWERED_MANIFESTS {
            answers.pop_first();
        }
        answer
    }
}

struct MirrorConfig {
    local_dir: PathBuf,
    remote: Arc<dyn RawObjectStore>,
    policy: SlateDbMirrorPolicyBuilder,
    download_concurrency: usize,
    remote_scan_interval: Option<Duration>,
    max_retries: Option<u32>,
    system_clock: Option<Arc<dyn Clock>>,
}

/// Builds an [`ObjectStore`] that keeps a local copy of the compacted SSTs of
/// one database, so that the database reads them from local disk. WAL SSTs,
/// manifests, and all other objects stay in the wrapped store only.
///
/// Give the built store to `DbBuilder`, `DbReaderBuilder`, or `AdminBuilder`
/// in place of the wrapped store.
///
/// One mirror serves one database. Build it once, and use the same store each
/// time you open that database. Each mirror locks its own local directory. The
/// lock stays until the store and all handles opened with it are released.
/// The mirror does not remove SSTs to free disk space, so the local volume
/// must hold the whole database.
///
/// Create a clone with the wrapped store, not with a mirror. Then open the
/// clone with its own mirror.
///
/// Builders are single-use: calling [`ObjectStoreMirrorBuilder::build`]
/// consumes the builder.
#[derive(uniffi::Object)]
pub struct ObjectStoreMirrorBuilder {
    config: Mutex<Option<MirrorConfig>>,
}

impl ObjectStoreMirrorBuilder {
    fn update(&self, update: impl FnOnce(MirrorConfig) -> MirrorConfig) -> Result<(), Error> {
        let mut guard = self.config.lock();
        let config = guard.take().ok_or(SlateDbError::BuilderConsumed)?;
        *guard = Some(update(config));
        Ok(())
    }

    fn take(&self) -> Result<MirrorConfig, SlateDbError> {
        let mut guard = self.config.lock();
        guard.take().ok_or(SlateDbError::BuilderConsumed)
    }
}

#[uniffi::export]
impl ObjectStoreMirrorBuilder {
    /// Creates a builder for a mirror of `object_store` in the local directory
    /// `local_dir`.
    #[uniffi::constructor]
    pub fn new(local_dir: String, object_store: Arc<ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            config: Mutex::new(Some(MirrorConfig {
                local_dir: PathBuf::from(local_dir),
                remote: object_store.inner.clone(),
                policy: SlateDbMirrorPolicy::builder(),
                download_concurrency: DEFAULT_DOWNLOAD_CONCURRENCY,
                remote_scan_interval: Some(DEFAULT_REMOTE_SCAN_INTERVAL),
                max_retries: None,
                system_clock: None,
            })),
        })
    }

    /// Sets the maximum number of concurrent SST downloads. The value must be
    /// greater than zero. The default is 8.
    pub fn with_download_concurrency(&self, concurrency: u64) -> Result<(), Error> {
        let concurrency =
            usize::try_from(concurrency).map_err(|_| SlateDbError::ValueTooLargeForUsize {
                field: "download_concurrency",
            })?;
        self.update(|mut config| {
            config.download_concurrency = concurrency;
            config
        })
    }

    /// Sets the interval of the scan that removes local copies of SSTs that
    /// are no longer in the wrapped store. For example, a garbage collector in
    /// another process can delete them. The value must be greater than zero.
    /// `None` disables the scan. The default is 600000 (10 minutes).
    ///
    /// The scan needs strongly consistent reads, writes, deletes, and listings
    /// from the wrapped store. If the store does not give them, disable the
    /// scan.
    pub fn with_remote_scan_interval_ms(&self, interval_ms: Option<u64>) -> Result<(), Error> {
        self.update(|mut config| {
            config.remote_scan_interval = interval_ms.map(Duration::from_millis);
            config
        })
    }

    /// Sets how many times the mirror retries a transient error of a download
    /// or a scan listing. The default, `None`, retries without a limit.
    pub fn with_max_retries(&self, max_retries: Option<u32>) -> Result<(), Error> {
        self.update(|mut config| {
            config.max_retries = max_retries;
            config
        })
    }

    /// Sets how often the mirror reads the compaction state, to download the
    /// output of active compactions before a manifest adds it. `None` disables
    /// these reads. Then a manifest downloads the whole output of a compaction
    /// before the caller gets the manifest. The default is 5000 (5 seconds).
    pub fn with_compactions_poll_interval_ms(&self, interval_ms: Option<u64>) -> Result<(), Error> {
        self.update(|mut config| {
            config.policy = config
                .policy
                .with_compactions_poll_interval(interval_ms.map(Duration::from_millis));
            config
        })
    }

    /// Sets the filter that selects the segments to keep on local disk. See
    /// [`SegmentFilter`] for when the mirror asks it and what its answers do.
    /// Without a filter, the mirror keeps all segments.
    pub fn with_segment_filter(&self, filter: Arc<dyn SegmentFilter>) -> Result<(), Error> {
        let adapter = SegmentFilterAdapter::new(filter);
        self.update(|mut config| {
            config.policy = config
                .policy
                .with_segment_predicate(move |manifest, segment| {
                    adapter.should_mirror(manifest.id(), segment)
                });
            config
        })
    }

    /// Sets the clock for retry backoff, the scan, the compaction state reads,
    /// and the last-modified time of mirrored writes.
    pub fn with_system_clock(&self, clock: Arc<SystemClock>) -> Result<(), Error> {
        self.update(|mut config| {
            config.system_clock = Some(clock.inner());
            config
        })
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl ObjectStoreMirrorBuilder {
    /// Creates the local directory if necessary, locks it, and loads the local
    /// copies that it holds. Consumes this builder.
    ///
    /// Fails with `Error::Unavailable` if the directory is locked by another
    /// mirror, or if local I/O fails.
    pub async fn build(&self) -> Result<Arc<ObjectStore>, Error> {
        let config = self.take()?;
        let mut policy = config.policy;
        if let Some(clock) = &config.system_clock {
            policy = policy.with_system_clock(Arc::clone(clock));
        }
        let mut builder =
            ObjectStoreMirror::builder(config.local_dir, config.remote, policy.build())
                .with_download_concurrency(config.download_concurrency)
                .with_remote_scan_interval(config.remote_scan_interval)
                .with_max_retries(config.max_retries);
        if let Some(clock) = config.system_clock {
            builder = builder.with_system_clock(clock);
        }
        // The mirror spawns its background tasks on the runtime that builds
        // it. Enter the dedicated runtime so that they do not run on the
        // single thread of UniFFI's runtime. See `runtime`.
        let mirror = runtime::enter(builder.build()).await.map_err(build_error)?;
        Ok(Arc::new(ObjectStore { inner: mirror }))
    }
}

fn build_error(error: object_store::Error) -> Error {
    match MirrorError::find(&error) {
        Some(err @ MirrorError::InvalidConfig { .. }) => Error::Invalid {
            message: err.to_string(),
        },
        // The message of a `Local` error does not include its source, which
        // tells why the I/O failed, for example a locked directory.
        Some(err @ MirrorError::Local { source }) => Error::Unavailable {
            message: format!("{err}: {source}"),
        },
        _ => Error::Unavailable {
            message: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use object_store::{ObjectStore as _, ObjectStoreExt};

    use super::*;
    use crate::config::{FlushOptions, FlushType};
    use crate::{DbBuilder, PrefixExtractor, PrefixTarget};

    /// Answers `true` for the segments in `kept`, and records each question.
    struct RecordingFilter {
        kept: Vec<Vec<u8>>,
        asked: Mutex<Vec<Vec<u8>>>,
    }

    impl RecordingFilter {
        fn keeping(kept: &[&[u8]]) -> Arc<Self> {
            Arc::new(Self {
                kept: kept.iter().map(|segment| segment.to_vec()).collect(),
                asked: Mutex::new(Vec::new()),
            })
        }

        fn asked(&self) -> usize {
            self.asked.lock().len()
        }
    }

    impl SegmentFilter for RecordingFilter {
        fn should_mirror(&self, segment: Vec<u8>) -> Result<bool, SegmentFilterCallbackError> {
            let keep = self.kept.contains(&segment);
            self.asked.lock().push(segment);
            Ok(keep)
        }
    }

    struct FailingFilter;

    impl SegmentFilter for FailingFilter {
        fn should_mirror(&self, _segment: Vec<u8>) -> Result<bool, SegmentFilterCallbackError> {
            Err(SegmentFilterCallbackError::Failed {
                message: "boom".to_owned(),
            })
        }
    }

    /// Puts each key in the segment of its first three bytes.
    struct ThreeByteExtractor;

    impl PrefixExtractor for ThreeByteExtractor {
        fn name(&self) -> String {
            "fixed3".to_owned()
        }

        fn prefix_len(&self, target: PrefixTarget) -> Option<u64> {
            let input = match target {
                PrefixTarget::Point { key } => key,
                PrefixTarget::Prefix { prefix } => prefix,
            };
            (input.len() >= 3).then_some(3)
        }
    }

    fn memory_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore {
            inner: Arc::new(object_store::memory::InMemory::new()),
        })
    }

    async fn build_mirror(
        local_dir: &Path,
        remote: &Arc<ObjectStore>,
    ) -> Result<Arc<ObjectStore>, Error> {
        ObjectStoreMirrorBuilder::new(local_dir.display().to_string(), remote.clone())
            .build()
            .await
    }

    fn local_objects(local_dir: &Path) -> usize {
        std::fs::read_dir(local_dir)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".obj")
            })
            .count()
    }

    #[tokio::test]
    async fn mirror_serves_flushed_ssts_from_local_disk() {
        let dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(object_store::memory::InMemory::new());
        let store = Arc::new(ObjectStore {
            inner: remote.clone(),
        });
        let mirror = build_mirror(dir.path(), &store).await.unwrap();

        let db = DbBuilder::new("db".to_owned(), mirror.clone())
            .build()
            .await
            .unwrap();
        db.put(b"k".to_vec(), b"v".to_vec()).await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.close().await.unwrap();
        assert!(local_objects(dir.path()) > 0);

        // Without the remote SSTs, only the local copies can serve the read.
        let compacted = object_store::path::Path::from("db/compacted");
        let ssts = remote.list_with_delimiter(Some(&compacted)).await.unwrap();
        assert!(!ssts.objects.is_empty());
        for sst in ssts.objects {
            remote.delete(&sst.location).await.unwrap();
        }

        let db = DbBuilder::new("db".to_owned(), mirror)
            .build()
            .await
            .unwrap();
        assert_eq!(db.get(b"k".to_vec()).await.unwrap(), Some(b"v".to_vec()));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn build_fails_when_directory_is_locked() {
        let dir = tempfile::tempdir().unwrap();
        let remote = memory_store();
        let _mirror = build_mirror(dir.path(), &remote).await.unwrap();

        let error = build_mirror(dir.path(), &remote).await.err().unwrap();
        match error {
            Error::Unavailable { message } => assert!(message.contains("locked"), "{message}"),
            error => panic!("{error:?}"),
        }
    }

    #[tokio::test]
    async fn build_rejects_zero_download_concurrency() {
        let dir = tempfile::tempdir().unwrap();
        let builder =
            ObjectStoreMirrorBuilder::new(dir.path().display().to_string(), memory_store());
        builder.with_download_concurrency(0).unwrap();

        let error = builder.build().await.err().unwrap();
        assert!(matches!(error, Error::Invalid { .. }), "{error:?}");
    }

    #[test]
    fn filter_answers_are_kept_for_the_two_newest_manifests() {
        let filter = RecordingFilter::keeping(&[b"aaa"]);
        let adapter = SegmentFilterAdapter::new(filter.clone());

        assert!(adapter.should_mirror(1, b"aaa"));
        assert!(adapter.should_mirror(1, b"aaa"));
        assert!(!adapter.should_mirror(1, b"bbb"));
        assert_eq!(filter.asked(), 2);

        adapter.should_mirror(2, b"aaa");
        adapter.should_mirror(1, b"aaa");
        assert_eq!(filter.asked(), 3);

        // Manifest 3 drops the answers of manifest 1.
        adapter.should_mirror(3, b"aaa");
        adapter.should_mirror(1, b"aaa");
        assert_eq!(filter.asked(), 5);
    }

    #[test]
    fn filter_error_counts_as_false() {
        let adapter = SegmentFilterAdapter::new(Arc::new(FailingFilter));
        assert!(!adapter.should_mirror(1, b"aaa"));
    }

    #[tokio::test]
    async fn filter_selects_the_segments_kept_on_local_disk() {
        let dir = tempfile::tempdir().unwrap();
        let filter = RecordingFilter::keeping(&[b"aaa"]);
        let builder =
            ObjectStoreMirrorBuilder::new(dir.path().display().to_string(), memory_store());
        builder.with_segment_filter(filter.clone()).unwrap();
        let mirror = builder.build().await.unwrap();

        let builder = DbBuilder::new("db".to_owned(), mirror);
        builder
            .with_segment_extractor(Arc::new(ThreeByteExtractor))
            .unwrap();
        let db = builder.build().await.unwrap();
        db.put(b"aaa-1".to_vec(), b"a".to_vec()).await.unwrap();
        db.put(b"bbb-1".to_vec(), b"b".to_vec()).await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        // Each segment has one L0 SST. Only the one of `aaa` is local.
        assert_eq!(local_objects(dir.path()), 1);
        let asked = filter.asked.lock().clone();
        assert!(asked.contains(&b"aaa".to_vec()), "{asked:?}");
        assert!(asked.contains(&b"bbb".to_vec()), "{asked:?}");
        assert_eq!(
            db.get(b"aaa-1".to_vec()).await.unwrap(),
            Some(b"a".to_vec())
        );
        assert_eq!(
            db.get(b"bbb-1".to_vec()).await.unwrap(),
            Some(b"b".to_vec())
        );
        db.close().await.unwrap();
    }
}
