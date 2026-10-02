//! End-to-end tests of `ObjectStoreMirror` over an in-memory remote store.

#![allow(clippy::disallowed_types, clippy::disallowed_methods, clippy::panic)]

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path as StdPath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use md5::{Digest, Md5};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;
use slatedb_mirror::{
    MirrorError, MirrorHandle, MirrorPolicy, ObjectStoreMirror, ReadRoute, WriteRoute,
};
use tokio::sync::Semaphore;

/// An in-memory store that counts GETs and can fail or hold them.
#[derive(Debug, Default)]
struct TestStore {
    inner: InMemory,
    /// Full-object GETs (not HEADs).
    gets: AtomicUsize,
    multipart_starts: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    /// Fail this many upcoming GETs with a transient error.
    fail_gets: AtomicUsize,
    /// When set, each GET takes a permit first.
    gate: Mutex<Option<Arc<Semaphore>>>,
}

impl fmt::Display for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore")
    }
}

#[async_trait]
impl ObjectStore for TestStore {
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
        self.multipart_starts.fetch_add(1, Ordering::SeqCst);
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
        self.gets.fetch_add(1, Ordering::SeqCst);
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        let gate = self.gate.lock().clone();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        let result = if self
            .fail_gets
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            Err(object_store::Error::Generic {
                store: "TestStore",
                source: "transient".into(),
            })
        } else {
            self.inner.get_opts(location, options).await
        };
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
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
}

/// Routes `*.manifest` through `Observe` and `*.sst` to the mirror. A
/// manifest's body is a newline-separated list of paths to fetch.
struct TestPolicy {
    sst_read: Mutex<ReadRoute>,
    sst_multipart: Mutex<WriteRoute>,
    fail_observe: AtomicBool,
    observed: Mutex<Vec<(Path, Bytes)>>,
    run_started: AtomicBool,
    run_stopped: Arc<AtomicBool>,
}

impl TestPolicy {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            sst_read: Mutex::new(ReadRoute::Local),
            sst_multipart: Mutex::new(WriteRoute::Mirror),
            fail_observe: AtomicBool::new(false),
            observed: Mutex::new(Vec::new()),
            run_started: AtomicBool::new(false),
            run_stopped: Arc::new(AtomicBool::new(false)),
        })
    }

    fn observed(&self) -> Vec<(Path, Bytes)> {
        self.observed.lock().clone()
    }
}

struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl MirrorPolicy for TestPolicy {
    fn read_route(&self, path: &Path, _options: &GetOptions) -> object_store::Result<ReadRoute> {
        Ok(match path.extension() {
            Some("manifest") => ReadRoute::Observe,
            Some("sst") => *self.sst_read.lock(),
            Some("bad") => {
                return Err(object_store::Error::Generic {
                    store: "TestPolicy",
                    source: "bad route".into(),
                })
            }
            _ => ReadRoute::Remote,
        })
    }

    fn put_route(&self, path: &Path, _options: &PutOptions) -> object_store::Result<WriteRoute> {
        Ok(match path.extension() {
            Some("manifest") => WriteRoute::Observe,
            Some("sst") | Some("123") => WriteRoute::Mirror,
            _ => WriteRoute::Remote,
        })
    }

    fn put_multipart_route(
        &self,
        path: &Path,
        _options: &PutMultipartOptions,
    ) -> object_store::Result<WriteRoute> {
        Ok(match path.extension() {
            Some("sst") => *self.sst_multipart.lock(),
            _ => WriteRoute::Remote,
        })
    }

    async fn observe(
        &self,
        path: &Path,
        bytes: &Bytes,
        mirror: &MirrorHandle,
    ) -> object_store::Result<()> {
        self.observed.lock().push((path.clone(), bytes.clone()));
        if self.fail_observe.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "TestPolicy",
                source: "observe failed".into(),
            });
        }
        let paths: Vec<Path> = std::str::from_utf8(bytes)
            .unwrap_or_default()
            .lines()
            .filter(|line| !line.is_empty())
            .map(Path::from)
            .collect();
        mirror.fetch(paths).await
    }

    async fn run(self: Arc<Self>, _mirror: MirrorHandle) {
        let _stopped = SetOnDrop(Arc::clone(&self.run_stopped));
        self.run_started.store(true, Ordering::SeqCst);
        std::future::pending::<()>().await;
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    store: Arc<TestStore>,
    policy: Arc<TestPolicy>,
    mirror: Arc<ObjectStoreMirror>,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_concurrency(8).await
    }

    async fn with_concurrency(concurrency: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mirror");
        let store = Arc::new(TestStore::default());
        let policy = TestPolicy::new();
        let mirror = build(&root, &store, &policy, concurrency).await.unwrap();
        Self {
            _dir: dir,
            root,
            store,
            policy,
            mirror,
        }
    }

    fn handle(&self) -> &MirrorHandle {
        self.mirror.handle()
    }

    fn files(&self) -> BTreeSet<String> {
        files(&self.root)
    }

    fn local_bytes(&self, path: &str) -> Vec<u8> {
        std::fs::read(self.root.join(data_file(path))).unwrap()
    }

    /// The files a set of complete local copies should leave behind.
    fn expected_files(&self, paths: &[&str]) -> BTreeSet<String> {
        let mut expected = BTreeSet::from(["LOCK".to_string()]);
        for path in paths {
            let data = data_file(path);
            expected.insert(format!("{data}.meta"));
            expected.insert(data);
        }
        expected
    }

    async fn put_remote(&self, path: &str, bytes: &'static [u8]) {
        self.store
            .put(&Path::from(path), PutPayload::from_static(bytes))
            .await
            .unwrap();
    }
}

async fn build(
    root: &StdPath,
    store: &Arc<TestStore>,
    policy: &Arc<TestPolicy>,
    concurrency: usize,
) -> object_store::Result<Arc<ObjectStoreMirror>> {
    ObjectStoreMirror::builder(
        root,
        Arc::clone(store) as Arc<dyn ObjectStore>,
        Arc::clone(policy) as Arc<dyn MirrorPolicy>,
    )
    .with_download_concurrency(concurrency)
    .with_remote_scan_interval(None)
    .build()
    .await
}

/// The local data file name for `path`: the hex MD5 of its parent, a `.`, and
/// its file name. See `src/layout.rs`.
fn data_file(path: &str) -> String {
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let prefix: String = Md5::digest(parent.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{prefix}.{name}")
}

fn files(root: &StdPath) -> BTreeSet<String> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

fn mirror_error(err: &object_store::Error) -> &MirrorError {
    MirrorError::find(err).unwrap_or_else(|| panic!("expected a MirrorError, got {err:?}"))
}

/// Polls `condition` until it holds, yielding to background tasks.
async fn eventually(mut condition: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition never held");
}

const SST: &str = "db/compacted/01A.sst";
const SST2: &str = "db/compacted/01B.sst";
const MANIFEST: &str = "db/manifest/00001.manifest";

#[tokio::test]
async fn should_reject_invalid_options() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(TestStore::default());
    let policy = TestPolicy::new();

    let err = build(dir.path(), &store, &policy, 0).await.unwrap_err();
    assert!(matches!(
        mirror_error(&err),
        MirrorError::InvalidConfig { .. }
    ));

    let err = ObjectStoreMirror::builder(dir.path(), store, policy)
        .with_remote_scan_interval(Some(Duration::ZERO))
        .build()
        .await
        .unwrap_err();
    assert!(matches!(
        mirror_error(&err),
        MirrorError::InvalidConfig { .. }
    ));
}

#[tokio::test]
async fn should_lock_directory_and_stop_background_tasks_on_drop() {
    let fixture = Fixture::new().await;
    eventually(|| fixture.policy.run_started.load(Ordering::SeqCst)).await;

    let err = build(&fixture.root, &fixture.store, &fixture.policy, 8)
        .await
        .unwrap_err();
    assert!(matches!(mirror_error(&err), MirrorError::Local { .. }));

    let Fixture {
        _dir,
        root,
        store,
        policy,
        mirror,
    } = fixture;
    drop(mirror);
    eventually(|| policy.run_stopped.load(Ordering::SeqCst)).await;

    // The lock is released once the background tasks let go of the state.
    let mut reopened = None;
    for _ in 0..100 {
        match build(&root, &store, &policy, 8).await {
            Ok(mirror) => {
                reopened = Some(mirror);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    assert!(reopened.is_some());
}

#[tokio::test]
async fn should_pass_remote_routes_through() {
    let fixture = Fixture::new().await;
    let path = Path::from("db/wal/00001.wal");
    fixture
        .mirror
        .put(&path, PutPayload::from_static(b"wal"))
        .await
        .unwrap();

    assert_eq!(
        fixture
            .mirror
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "wal"
    );
    assert!(!fixture.handle().contains(&path));
    assert_eq!(fixture.files(), fixture.expected_files(&[]));

    let listed: Vec<_> = fixture
        .mirror
        .list(Some(&Path::from("db")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
}

#[tokio::test]
async fn should_wrap_route_errors_in_policy_error() {
    let fixture = Fixture::new().await;
    let err = fixture
        .mirror
        .get(&Path::from("a/b.bad"))
        .await
        .unwrap_err();
    assert!(matches!(mirror_error(&err), MirrorError::Policy { .. }));
}

#[tokio::test]
async fn should_mirror_puts_and_serve_local_reads() {
    let fixture = Fixture::new().await;
    let path = Path::from(SST);
    let put = fixture
        .mirror
        .put(&path, PutPayload::from_static(b"0123456789"))
        .await
        .unwrap();

    assert!(fixture.handle().contains(&path));
    assert_eq!(fixture.local_bytes(SST), b"0123456789");
    assert_eq!(fixture.files(), fixture.expected_files(&[SST]));

    // Served locally, without touching the remote store.
    let result = fixture
        .mirror
        .get_opts(
            &path,
            GetOptions {
                range: Some(GetRange::Bounded(2..5)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.range, 2..5);
    assert_eq!(result.bytes().await.unwrap(), "234");
    assert!(fixture.mirror.get_range(&path, 20..30).await.is_err());
    let head = fixture.mirror.head(&path).await.unwrap();
    assert_eq!(head.size, 10);
    assert_eq!(head.e_tag, put.e_tag);
    assert_eq!(fixture.store.gets.load(Ordering::SeqCst), 0);

    // Preconditions are checked against the local metadata.
    let err = fixture
        .mirror
        .get_opts(
            &path,
            GetOptions {
                if_none_match: put.e_tag.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, object_store::Error::NotModified { .. }));
}

#[tokio::test]
async fn should_return_not_local_on_local_miss() {
    let fixture = Fixture::new().await;
    fixture.put_remote(SST, b"remote only").await;

    let err = fixture.mirror.get(&Path::from(SST)).await.unwrap_err();
    assert!(matches!(mirror_error(&err), MirrorError::NotLocal { .. }));
    let err = fixture.mirror.head(&Path::from(SST)).await.unwrap_err();
    assert!(matches!(mirror_error(&err), MirrorError::NotLocal { .. }));
    assert_eq!(fixture.store.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn should_not_write_remotely_when_put_fails_remotely() {
    let fixture = Fixture::new().await;
    fixture.put_remote(SST, b"first").await;

    let err = fixture
        .mirror
        .put_opts(
            &Path::from(SST),
            PutPayload::from_static(b"second"),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, object_store::Error::AlreadyExists { .. }));
    assert!(!fixture.handle().contains(&Path::from(SST)));
    assert_eq!(fixture.files(), fixture.expected_files(&[]));
}

#[tokio::test]
async fn should_reject_reserved_names_before_writing_remotely() {
    let fixture = Fixture::new().await;
    let path = Path::from("db/compacted/file.123");
    let err = fixture
        .mirror
        .put(&path, PutPayload::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(matches!(
        mirror_error(&err),
        MirrorError::Unsupported { .. }
    ));
    assert!(fixture.store.head(&path).await.is_err());
}

#[tokio::test]
async fn should_observe_whole_objects_on_ranged_reads() {
    let fixture = Fixture::new().await;
    fixture.put_remote(SST, b"sst").await;
    fixture
        .put_remote(MANIFEST, b"db/compacted/01A.sst\n")
        .await;
    let manifest = Path::from(MANIFEST);

    let result = fixture
        .mirror
        .get_opts(
            &manifest,
            GetOptions {
                range: Some(GetRange::Bounded(0..2)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.range, 0..2);
    assert_eq!(result.bytes().await.unwrap(), "db");
    assert_eq!(
        fixture.policy.observed(),
        vec![(manifest.clone(), Bytes::from("db/compacted/01A.sst\n"))]
    );

    // `observe` fetched the SST it listed.
    assert!(fixture.handle().contains(&Path::from(SST)));
    assert_eq!(fixture.local_bytes(SST), b"sst");

    // HEAD and bodiless conditional GETs skip `observe`.
    let meta = fixture.mirror.head(&manifest).await.unwrap();
    let err = fixture
        .mirror
        .get_opts(
            &manifest,
            GetOptions {
                if_none_match: meta.e_tag,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, object_store::Error::NotModified { .. }));
    assert_eq!(fixture.policy.observed().len(), 1);
}

#[tokio::test]
async fn should_report_observe_failures() {
    let fixture = Fixture::new().await;
    fixture.policy.fail_observe.store(true, Ordering::SeqCst);
    let manifest = Path::from(MANIFEST);

    // Writes are durable, so the caller has to know.
    let err = fixture
        .mirror
        .put(&manifest, PutPayload::from_static(b""))
        .await
        .unwrap_err();
    assert!(matches!(
        mirror_error(&err),
        MirrorError::WriteCommitted { .. }
    ));
    assert!(fixture.store.head(&manifest).await.is_ok());

    // Reads return the policy's error unchanged.
    let err = fixture.mirror.get(&manifest).await.unwrap_err();
    assert!(MirrorError::find(&err).is_none());
    assert!(err.to_string().contains("observe failed"));
}

#[tokio::test]
async fn should_refetch_and_replace_local_copy() {
    let fixture = Fixture::new().await;
    let path = Path::from(SST);
    fixture
        .mirror
        .put(&path, PutPayload::from_static(b"good bytes"))
        .await
        .unwrap();

    // Corrupt the local copy without changing its size.
    std::fs::write(fixture.root.join(data_file(SST)), b"bad bytes!").unwrap();

    *fixture.policy.sst_read.lock() = ReadRoute::Refetch;
    assert_eq!(fixture.mirror.get_range(&path, 0..4).await.unwrap(), "good");
    assert_eq!(fixture.local_bytes(SST), b"good bytes");
    assert_eq!(fixture.mirror.head(&path).await.unwrap().size, 10);
    assert_eq!(fixture.store.gets.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.files(), fixture.expected_files(&[SST]));
}

#[tokio::test]
async fn should_tee_multipart_uploads() {
    let fixture = Fixture::new().await;
    let path = Path::from(SST);
    let mut upload = fixture.mirror.put_multipart(&path).await.unwrap();

    // Complete the parts out of order; the local file still gets them in
    // order.
    let first = upload.put_part(PutPayload::from_static(b"aaaaa"));
    let second = upload.put_part(PutPayload::from_static(b"bbbbb"));
    let third = upload.put_part(PutPayload::from_static(b"ccccc"));
    third.await.unwrap();
    second.await.unwrap();
    first.await.unwrap();
    assert!(!fixture.handle().contains(&path));
    upload.complete().await.unwrap();

    assert!(fixture.handle().contains(&path));
    assert_eq!(fixture.local_bytes(SST), b"aaaaabbbbbccccc");
    assert_eq!(
        fixture
            .store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "aaaaabbbbbccccc"
    );
    assert_eq!(fixture.mirror.head(&path).await.unwrap().size, 15);
    assert_eq!(fixture.files(), fixture.expected_files(&[SST]));
}

#[tokio::test]
async fn should_remove_temp_file_when_multipart_upload_is_aborted_or_dropped() {
    let fixture = Fixture::new().await;
    let path = Path::from(SST);

    let mut upload = fixture.mirror.put_multipart(&path).await.unwrap();
    upload
        .put_part(PutPayload::from_static(b"part"))
        .await
        .unwrap();
    upload.abort().await.unwrap();
    assert_eq!(fixture.files(), fixture.expected_files(&[]));

    let mut upload = fixture.mirror.put_multipart(&path).await.unwrap();
    upload
        .put_part(PutPayload::from_static(b"part"))
        .await
        .unwrap();
    drop(upload);
    eventually(|| fixture.files() == fixture.expected_files(&[])).await;

    // The dropped upload released the path.
    fixture
        .mirror
        .put(&path, PutPayload::from_static(b"x"))
        .await
        .unwrap();
    assert!(fixture.handle().contains(&path));
}

#[tokio::test]
async fn should_reject_observe_multipart_uploads() {
    let fixture = Fixture::new().await;
    *fixture.policy.sst_multipart.lock() = WriteRoute::Observe;
    let err = fixture
        .mirror
        .put_multipart(&Path::from(SST))
        .await
        .unwrap_err();
    assert!(matches!(
        mirror_error(&err),
        MirrorError::Unsupported { .. }
    ));
    assert_eq!(fixture.store.multipart_starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn should_delete_local_and_remote_copies() {
    let fixture = Fixture::new().await;
    let path = Path::from(SST);
    fixture
        .mirror
        .put(&path, PutPayload::from_static(b"x"))
        .await
        .unwrap();

    fixture.mirror.delete(&path).await.unwrap();
    assert!(!fixture.handle().contains(&path));
    assert!(fixture.store.head(&path).await.is_err());
    assert_eq!(fixture.files(), fixture.expected_files(&[]));
}

#[tokio::test]
async fn should_reject_copy_and_rename() {
    let fixture = Fixture::new().await;
    fixture.put_remote("a/b", b"x").await;
    let (from, to) = (Path::from("a/b"), Path::from("a/c"));

    let err = fixture.mirror.copy(&from, &to).await.unwrap_err();
    assert!(matches!(
        mirror_error(&err),
        MirrorError::Unsupported { .. }
    ));
    let err = fixture.mirror.rename(&from, &to).await.unwrap_err();
    assert!(matches!(
        mirror_error(&err),
        MirrorError::Unsupported { .. }
    ));
    assert!(fixture.store.head(&to).await.is_err());
}

#[tokio::test]
async fn should_restore_local_copies_after_restart() {
    let fixture = Fixture::new().await;
    let path = Path::from(SST);
    let put = fixture
        .mirror
        .put(&path, PutPayload::from_static(b"persisted"))
        .await
        .unwrap();

    let Fixture {
        _dir,
        root,
        store,
        policy,
        mirror,
    } = fixture;
    drop(mirror);
    eventually(|| policy.run_stopped.load(Ordering::SeqCst)).await;
    let mut reopened = None;
    for _ in 0..100 {
        if let Ok(mirror) = build(&root, &store, &policy, 8).await {
            reopened = Some(mirror);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mirror = reopened.expect("mirror reopened");

    assert!(mirror.handle().contains(&path));
    assert_eq!(mirror.head(&path).await.unwrap().e_tag, put.e_tag);
    assert_eq!(
        mirror.get(&path).await.unwrap().bytes().await.unwrap(),
        "persisted"
    );
    assert_eq!(store.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn should_dedupe_concurrent_fetches() {
    let fixture = Fixture::new().await;
    fixture.put_remote(SST, b"x").await;
    let gate = Arc::new(Semaphore::new(0));
    *fixture.store.gate.lock() = Some(Arc::clone(&gate));

    let first = tokio::spawn(fixture.handle().fetch([Path::from(SST)]));
    let second = tokio::spawn(fixture.handle().fetch([Path::from(SST)]));
    eventually(|| fixture.store.gets.load(Ordering::SeqCst) == 1).await;
    gate.add_permits(10);
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();

    assert_eq!(fixture.store.gets.load(Ordering::SeqCst), 1);
    assert!(fixture.handle().contains(&Path::from(SST)));

    // Already local, so no download.
    fixture.handle().fetch([Path::from(SST)]).await.unwrap();
    assert_eq!(fixture.store.gets.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn should_limit_download_concurrency() {
    let fixture = Fixture::with_concurrency(1).await;
    fixture.put_remote(SST, b"a").await;
    fixture.put_remote(SST2, b"b").await;
    let gate = Arc::new(Semaphore::new(0));
    *fixture.store.gate.lock() = Some(Arc::clone(&gate));

    let fetch = tokio::spawn(fixture.handle().fetch([Path::from(SST), Path::from(SST2)]));
    eventually(|| fixture.store.gets.load(Ordering::SeqCst) == 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(fixture.store.gets.load(Ordering::SeqCst), 1);

    gate.add_permits(10);
    fetch.await.unwrap().unwrap();
    assert_eq!(fixture.store.max_in_flight.load(Ordering::SeqCst), 1);
    assert!(fixture.handle().contains(&Path::from(SST2)));
}

#[tokio::test]
async fn should_retry_transient_download_errors() {
    let fixture = Fixture::new().await;
    fixture.put_remote(SST, b"x").await;
    fixture.store.fail_gets.store(2, Ordering::SeqCst);

    fixture.handle().fetch([Path::from(SST)]).await.unwrap();
    assert_eq!(fixture.store.gets.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.files(), fixture.expected_files(&[SST]));
}

#[tokio::test]
async fn should_fail_fetch_of_missing_object() {
    let fixture = Fixture::new().await;
    let err = fixture.handle().fetch([Path::from(SST)]).await.unwrap_err();
    assert!(matches!(err, object_store::Error::NotFound { .. }));
    assert_eq!(fixture.files(), fixture.expected_files(&[]));
}

#[tokio::test]
async fn should_evict_local_copies() {
    let fixture = Fixture::new().await;
    let path = Path::from(SST);
    fixture
        .mirror
        .put(&path, PutPayload::from_static(b"x"))
        .await
        .unwrap();

    fixture.handle().evict([path.clone()]);
    eventually(|| !fixture.handle().contains(&path)).await;
    eventually(|| fixture.files() == fixture.expected_files(&[])).await;
    // The remote copy is untouched.
    assert!(fixture.store.head(&path).await.is_ok());
}

#[tokio::test]
async fn should_run_remote_scan_in_background() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(TestStore::default());
    let policy = TestPolicy::new();
    let mirror = ObjectStoreMirror::builder(
        dir.path(),
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        policy,
    )
    .with_remote_scan_interval(Some(Duration::from_millis(10)))
    .build()
    .await
    .unwrap();
    let path = Path::from(SST);
    mirror
        .put(&path, PutPayload::from_static(b"x"))
        .await
        .unwrap();

    store.delete(&path).await.unwrap();
    eventually(|| !mirror.handle().contains(&path)).await;
}
