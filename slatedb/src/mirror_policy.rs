//! The SlateDB policy for an [`ObjectStoreMirror`](slatedb_mirror::ObjectStoreMirror)
//! (RFC 0034).
//!
//! The policy keeps a local copy of the compacted SSTs that the database uses,
//! so that SlateDB reads them from local disk and not from object storage.
//! WAL SSTs, manifests, and all other objects stay in object storage only.
//!
//! The manifest tells the policy which SSTs to keep. When SlateDB reads or
//! writes a manifest, the policy downloads the missing SSTs of that manifest
//! before the call returns. When an SST is no longer in the newest manifest or
//! in one of its checkpoints, the policy removes the local copy. The mirror
//! writes each new compacted SST to both object storage and local disk.
//!
//! An optional segment predicate limits the mirror to some segments.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use log::warn;
use object_store::path::{Path, DELIMITER};
use object_store::{
    Extensions, GetOptions, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions,
};
use parking_lot::Mutex;
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use slatedb_common::DbRand;
use slatedb_mirror::{
    MirrorError, MirrorHandle, MirrorPolicy, MultipartRoute, ReadRoute, WriteRoute,
};
use slatedb_txn_obj::ObjectCodec;

use crate::compactions_store::CompactionsStore;
use crate::compactor_state::VersionedCompactions;
use crate::flatbuffer_types::FlatBufferManifestCodec;
use crate::manifest::{Manifest, VersionedManifest};
use crate::object_store_tag::{ObjectStoreCallTag, SstType, TableStoreKind};
use crate::paths::PathResolver;
use crate::retrying_object_store::RetryingObjectStore;

/// The `store` name on the errors that the policy returns from a route.
const POLICY_STORE_NAME: &str = "SlateDbMirrorPolicy";
/// The directory under the database root that holds the manifests.
const MANIFEST_DIR: &str = "manifest";
/// The suffix of a manifest file name, after the manifest ID.
const MANIFEST_SUFFIX: &str = ".manifest";
/// The default for [`SlateDbMirrorPolicyBuilder::with_compactions_poll_interval`].
const DEFAULT_COMPACTIONS_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Returns true if the policy mirrors the segment with the given prefix. See
/// [`SlateDbMirrorPolicyBuilder::with_segment_predicate`].
type SegmentPredicate = dyn Fn(&VersionedManifest, &[u8]) -> bool + Send + Sync;

/// A [`MirrorPolicy`] that mirrors the compacted SSTs of one SlateDB database.
///
/// The policy serves the database root of the first manifest that it reads or
/// writes. It rejects manifests of other roots. SSTs of other databases that
/// the manifest references (for example, after a clone) are mirrored.
///
/// Creating a clone and detaching a clone from its parent change the manifest
/// of the parent database, so they fail through this policy. Run them on the
/// wrapped store.
///
/// For a [`DbReader`](crate::DbReader), set
/// [`DbReaderOptions::manifest_poll_interval`](crate::config::DbReaderOptions::manifest_poll_interval)
/// shorter than
/// [`CompactorOptions::checkpoint_lifetime`](crate::config::CompactorOptions::checkpoint_lifetime).
/// A reader that falls further behind can read a manifest whose SSTs the
/// garbage collector already deleted. The download fails, so the reader keeps
/// its older manifest. But the policy already processed the newer manifest and
/// evicted SSTs that the older one uses, so reads of those SSTs fail with
/// `NotLocal`.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use object_store::memory::InMemory;
/// # use object_store::ObjectStore;
/// # use slatedb::{Db, SlateDbMirrorPolicy};
/// # use slatedb_mirror::ObjectStoreMirror;
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
/// let mirror = ObjectStoreMirror::builder(
///     "/var/lib/slatedb/mirror",
///     remote,
///     SlateDbMirrorPolicy::builder().build(),
/// )
/// .build()
/// .await?;
/// let db = Db::builder("db", mirror).build().await?;
/// # Ok(())
/// # }
/// ```
pub struct SlateDbMirrorPolicy {
    state: Mutex<PolicyState>,
    segment_predicate: Option<Box<SegmentPredicate>>,
    compactions_poll_interval: Option<Duration>,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
    /// The wrapped store with retries, for reads of checkpoint manifests. The
    /// first read sets it, because only a `MirrorHandle` gives the wrapped
    /// store.
    remote: OnceLock<RetryingObjectStore>,
}

impl SlateDbMirrorPolicy {
    /// Returns a builder for a policy that mirrors every segment and polls the
    /// compaction state every 5 seconds.
    pub fn builder() -> SlateDbMirrorPolicyBuilder {
        SlateDbMirrorPolicyBuilder {
            segment_predicate: None,
            compactions_poll_interval: Some(DEFAULT_COMPACTIONS_POLL_INTERVAL),
            system_clock: Arc::new(DefaultSystemClock::new()),
        }
    }
}

#[async_trait]
impl MirrorPolicy for SlateDbMirrorPolicy {
    fn read_route(&self, path: &Path, options: &GetOptions) -> object_store::Result<ReadRoute> {
        // Mirrored SSTs come first, so that their reads do not parse the path.
        if let Some(tag) = self.mirrored_sst_tag(&options.extensions) {
            // A compactor read of an SST that the policy does not retain goes
            // to the wrapped store, because no manifest downloaded the SST. A
            // worker that resumes a compaction reads such an SST: the partial
            // output that an earlier worker wrote.
            if tag.kind == TableStoreKind::Compactor && !self.state.lock().retained.contains(path) {
                return Ok(ReadRoute::Remote);
            }
            // A read that SlateDB retries because the SST contents were corrupt
            // downloads the SST again, because the local copy can be the
            // corrupt one. Other reads use the local copy.
            return Ok(match tag.retry {
                Some(_) => ReadRoute::Refetch,
                None => ReadRoute::Local,
            });
        }
        // A manifest is read from object storage. Then `observe` downloads its
        // SSTs before the caller gets the manifest. A manifest of a database
        // other than the first one fails.
        if let Some((root, _)) = parse_manifest_path(path) {
            self.record_or_check_root(&root)?;
            return Ok(ReadRoute::Observe);
        }
        // WAL SSTs, compacted SSTs in segments that are not selected, and all
        // other objects, such as `.compactions`, are read from object storage.
        Ok(ReadRoute::Remote)
    }

    fn put_route(&self, path: &Path, options: &PutOptions) -> object_store::Result<WriteRoute> {
        // A mirrored SST is written to object storage and to local disk.
        if self.mirrored_sst_tag(&options.extensions).is_some() {
            // Mirrored paths must be write-once.
            if matches!(options.mode, PutMode::Update(_)) {
                return Err(policy_error(format!(
                    "conditional update of a mirrored SST. path=`{path}`"
                )));
            }
            return Ok(WriteRoute::Mirror);
        }
        // A manifest is written to object storage. Then `observe` downloads
        // its SSTs before the write returns. A manifest of a database other
        // than the first one fails.
        if let Some((root, _)) = parse_manifest_path(path) {
            self.record_or_check_root(&root)?;
            return Ok(WriteRoute::Observe);
        }
        // All other objects are written to object storage only.
        Ok(WriteRoute::Remote)
    }

    fn put_multipart_route(
        &self,
        _path: &Path,
        options: &PutMultipartOptions,
    ) -> object_store::Result<MultipartRoute> {
        // SlateDB uploads large SSTs in parts, but it writes each manifest with
        // one PUT. So only the SST rule of `put_route` applies here.
        Ok(match self.mirrored_sst_tag(&options.extensions) {
            Some(_) => MultipartRoute::Mirror,
            None => MultipartRoute::Remote,
        })
    }

    /// Processes a manifest that SlateDB read or wrote, and downloads the
    /// selected SSTs of the manifest before the caller gets it.
    ///
    /// A manifest that is newer than the current one replaces `retained` with
    /// its own selected SSTs and those of its checkpoints. It then evicts the
    /// SSTs that it no longer retains. An older manifest, for example the
    /// manifest of a checkpoint that a reader opens, does not change the
    /// state. It downloads only its SSTs that are still retained.
    async fn observe(
        &self,
        path: &Path,
        bytes: &Bytes,
        mirror: &MirrorHandle,
    ) -> object_store::Result<()> {
        // Only manifest calls use the `Observe` routes.
        let Some((root, manifest_id)) = parse_manifest_path(path) else {
            return Ok(());
        };
        let versioned_manifest = Arc::new(VersionedManifest::from_manifest(
            manifest_id,
            decode_manifest(bytes)?,
        ));
        let manifest_paths = Arc::new(ManifestSstPaths::from_manifest(
            &root,
            &versioned_manifest.manifest,
        ));

        let newer = self
            .state
            .lock()
            .is_manifest_newer_than_current(manifest_id);

        // A newer manifest downloads its selected SSTs and also keeps the
        // selected SSTs of its checkpoints. Read the checkpoints and run the
        // predicate.
        let update = if newer {
            let checkpoints = self
                .checkpoint_paths(mirror, &root, &versioned_manifest, &manifest_paths)
                .await?;
            let to_download: HashSet<Path> = self
                .selected_ssts(&versioned_manifest, &manifest_paths)
                .cloned()
                .collect();
            let mut retained = to_download.clone();
            for paths in checkpoints.values() {
                retained.extend(self.selected_ssts(&versioned_manifest, paths).cloned());
            }
            Some((to_download, retained, checkpoints))
        } else {
            None
        };

        let to_download = {
            let mut state = self.state.lock();
            match update {
                // Check again, because another `observe` can process a newer
                // manifest while the lock is released.
                Some((to_download, retained, checkpoints))
                    if state.is_manifest_newer_than_current(manifest_id) =>
                {
                    // Evictions are issued under the lock, so they reach the
                    // mirror in manifest order.
                    //
                    // TODO: On the first manifest, also evict the local copies
                    // that an earlier process left, except the output of active
                    // compactions. This needs a `MirrorHandle` method that
                    // lists local copies. Until then, only the remote scan
                    // removes them.
                    mirror.evict(state.retained.difference(&retained).cloned());
                    state.retained = retained;
                    state.newest = Some(Arc::clone(&versioned_manifest));
                    state.checkpoints = checkpoints;
                    to_download
                }

                // Only a manifest that becomes current changes `retained`. Any
                // other manifest downloads only its SSTs that are still
                // retained.
                _ => manifest_paths
                    .segments()
                    .iter()
                    .flat_map(|segment| &segment.paths)
                    .filter(|sst| state.retained.contains(*sst))
                    .cloned()
                    .collect(),
            }
        };

        // Download without the lock. The mirror skips SSTs that are already
        // local. If the download fails, the state above stays, but the caller
        // does not get this manifest. See the note about readers on
        // `SlateDbMirrorPolicy`.
        mirror.fetch(to_download).await
    }

    async fn run(self: Arc<Self>, mirror: MirrorHandle) {
        let Some(interval) = self.compactions_poll_interval else {
            return;
        };
        let mut compactions_store = None;
        loop {
            self.system_clock.sleep(interval).await;
            let (root, manifest) = {
                let state = self.state.lock();
                match (&state.root, &state.newest) {
                    (Some(root), Some(newest)) => (root.clone(), Arc::clone(newest)),
                    _ => continue,
                }
            };
            let store = compactions_store
                .get_or_insert_with(|| CompactionsStore::new(&root, Arc::clone(mirror.remote())));
            let compactions = match store.try_read_latest_compactions().await {
                Ok(Some(compactions)) => compactions,
                Ok(None) => continue,
                Err(err) => {
                    warn!("failed to read compactions for mirror prefetch [error={err}]");
                    continue;
                }
            };
            // Skip local SSTs, so that a poll during a large compaction stays
            // cheap.
            mirror.prefetch(
                self.compaction_outputs(&root, &manifest, &compactions)
                    .into_iter()
                    .filter(|sst| !mirror.contains(sst)),
            );
        }
    }
}

impl SlateDbMirrorPolicy {
    /// Returns the tag of a compacted SST call in a segment that the policy
    /// mirrors. WAL SST calls, calls with no tag, and compacted SST calls in
    /// other segments return `None`.
    fn mirrored_sst_tag<'a>(&self, extensions: &'a Extensions) -> Option<&'a ObjectStoreCallTag> {
        compacted_sst_tag(extensions).filter(|tag| self.mirrors_segment(tag.segment.as_deref()))
    }

    /// Returns true if the policy mirrors the compacted SSTs in `segment`.
    ///
    /// A caller gets a manifest only after its SSTs are downloaded, so a read
    /// of an SST from that manifest finds the local copy.
    fn mirrors_segment(&self, segment: Option<&[u8]>) -> bool {
        let Some(predicate) = &self.segment_predicate else {
            return true;
        };
        // The predicate needs a segment and a manifest.
        let Some(segment) = segment else {
            return false;
        };
        let newest = self.state.lock().newest.clone();
        newest.is_some_and(|newest| predicate(&newest, segment))
    }

    /// Records `root` as the database root, or fails if the policy already
    /// serves another root.
    fn record_or_check_root(&self, root: &Path) -> object_store::Result<()> {
        let mut state = self.state.lock();
        match &state.root {
            Some(known) if known != root => Err(policy_error(format!(
                "manifest is outside the mirrored database. root=`{root}`, mirrored_root=`{known}`"
            ))),
            Some(_) => Ok(()),
            None => {
                state.root = Some(root.clone());
                Ok(())
            }
        }
    }

    /// The SSTs of `paths` in the segments that `manifest` selects.
    fn selected_ssts<'a>(
        &'a self,
        manifest: &'a VersionedManifest,
        paths: &'a ManifestSstPaths,
    ) -> impl Iterator<Item = &'a Path> + 'a {
        paths
            .segments()
            .iter()
            .filter(move |segment| self.selects(manifest, &segment.prefix))
            .flat_map(|segment| &segment.paths)
    }

    /// Returns true if the predicate selects `segment` under `manifest`.
    fn selects(&self, manifest: &VersionedManifest, segment: &[u8]) -> bool {
        self.segment_predicate
            .as_ref()
            .is_none_or(|predicate| predicate(manifest, segment))
    }

    /// Returns the SST paths of each checkpoint of `manifest`, by manifest ID.
    /// The paths come from the cache, or from a read of the checkpoint
    /// manifest. A checkpoint of `manifest` itself uses `manifest_paths`.
    async fn checkpoint_paths(
        &self,
        mirror: &MirrorHandle,
        root: &Path,
        manifest: &VersionedManifest,
        manifest_paths: &Arc<ManifestSstPaths>,
    ) -> object_store::Result<HashMap<u64, Arc<ManifestSstPaths>>> {
        let mut checkpoints = HashMap::new();
        let mut missing = Vec::new();
        {
            let state = self.state.lock();
            for checkpoint in manifest.checkpoints() {
                let id = checkpoint.manifest_id;
                let cached = if id == manifest.id() {
                    Some(manifest_paths)
                } else {
                    state.checkpoints.get(&id)
                };
                match cached {
                    Some(paths) => {
                        checkpoints.insert(id, Arc::clone(paths));
                    }
                    None => missing.push(id),
                }
            }
        }
        for id in missing {
            let paths = self.read_checkpoint_sst_paths(mirror, root, id).await?;
            checkpoints.insert(id, Arc::new(paths));
        }
        Ok(checkpoints)
    }

    /// Reads a checkpoint manifest from the wrapped store, so that the read
    /// does not call `observe` again. A deleted manifest retains nothing.
    async fn read_checkpoint_sst_paths(
        &self,
        mirror: &MirrorHandle,
        root: &Path,
        manifest_id: u64,
    ) -> object_store::Result<ManifestSstPaths> {
        // A failed `observe` on a manifest write closes the database, so retry
        // transient errors here.
        let remote = self.remote.get_or_init(|| {
            RetryingObjectStore::new(
                Arc::clone(mirror.remote()),
                Arc::clone(&self.rand),
                Arc::clone(&self.system_clock),
                None,
            )
        });
        let path = manifest_path(root, manifest_id);
        let bytes = match remote.get(&path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => {
                warn!("checkpoint manifest is missing, not retaining its SSTs [path={path}]");
                return Ok(ManifestSstPaths::default());
            }
            Err(err) => return Err(err),
        };
        Ok(ManifestSstPaths::from_manifest(
            root,
            &decode_manifest(&bytes)?,
        ))
    }

    /// The SSTs that active compactions in selected segments wrote, including
    /// the output of their subcompactions.
    fn compaction_outputs(
        &self,
        root: &Path,
        manifest: &VersionedManifest,
        compactions: &VersionedCompactions,
    ) -> HashSet<Path> {
        let resolver = PathResolver::from_root(root.clone());
        compactions
            .recent_compactions()
            .filter(|compaction| {
                compaction.active() && self.selects(manifest, compaction.spec().segment())
            })
            .flat_map(|compaction| {
                compaction.output_ssts().into_iter().chain(
                    compaction
                        .subcompactions()
                        .iter()
                        .flat_map(|subcompaction| subcompaction.output_ssts().iter().cloned()),
                )
            })
            .map(|sst| resolver.sst_path(&sst.id))
            .collect()
    }
}

/// Configures and builds a [`SlateDbMirrorPolicy`].
pub struct SlateDbMirrorPolicyBuilder {
    segment_predicate: Option<Box<SegmentPredicate>>,
    compactions_poll_interval: Option<Duration>,
    system_clock: Arc<dyn SystemClock>,
}

impl SlateDbMirrorPolicyBuilder {
    /// Sets the predicate that selects which segments are mirrored.
    ///
    /// The predicate receives a manifest and a segment prefix. An empty prefix
    /// identifies the root segment. When the policy processes a manifest, the
    /// predicate receives that manifest. For reads, writes, and compactions,
    /// it receives the newest manifest that the policy processed, so it can
    /// select a segment that is not in that manifest yet.
    ///
    /// Compacted SST calls in a segment that is not selected use the wrapped
    /// store only. So do compacted SST calls with no segment, and all compacted
    /// SST calls before the first manifest. The policy calls the predicate on
    /// each compacted SST call, so it must be cheap. The default selects every
    /// segment.
    ///
    /// If the predicate starts to select a segment that already has SSTs, reads
    /// of those SSTs can fail with `NotLocal` until the next manifest downloads
    /// them.
    pub fn with_segment_predicate(
        mut self,
        predicate: impl Fn(&VersionedManifest, &[u8]) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.segment_predicate = Some(Box::new(predicate));
        self
    }

    /// Sets how often the policy reads the compaction state to prefetch the
    /// output of active compactions, so that the output is local when a
    /// manifest adds it. `None` disables the prefetch. Then a manifest
    /// downloads the whole output of a compaction before the caller gets the
    /// manifest. The default is 5 seconds.
    pub fn with_compactions_poll_interval(mut self, interval: Option<Duration>) -> Self {
        self.compactions_poll_interval = interval;
        self
    }

    /// Sets the clock for the compaction state polls and for retry backoff.
    /// The default uses Tokio's clock.
    pub fn with_system_clock(mut self, system_clock: Arc<dyn SystemClock>) -> Self {
        self.system_clock = system_clock;
        self
    }

    /// Builds the policy. Pass it to
    /// [`ObjectStoreMirror::builder`](slatedb_mirror::ObjectStoreMirror::builder).
    pub fn build(self) -> Arc<SlateDbMirrorPolicy> {
        Arc::new(SlateDbMirrorPolicy {
            state: Mutex::new(PolicyState::default()),
            segment_predicate: self.segment_predicate,
            compactions_poll_interval: self.compactions_poll_interval,
            system_clock: self.system_clock,
            rand: Arc::new(DbRand::default()),
            remote: OnceLock::new(),
        })
    }
}

#[derive(Default)]
struct PolicyState {
    /// The database root, recorded from the first manifest call. It is `None`
    /// until then, because the policy detects the root and does not get it
    /// from the builder.
    root: Option<Path>,
    /// The newest manifest processed. `retained` belongs to it. It is `None`
    /// until the policy processes its first manifest.
    newest: Option<Arc<VersionedManifest>>,
    /// The SSTs that must not be evicted: the selected SSTs of `newest` and of
    /// its checkpoints. The policy stores this set and does not rebuild it
    /// from `newest` and `checkpoints` for each manifest. The predicate can
    /// give a different answer when it runs again, for example if it depends
    /// on time. Then a rebuilt set can leave out an SST that the old set kept,
    /// and the policy does not evict it.
    retained: HashSet<Path>,
    /// The SST paths of the checkpoint manifests that `newest` references, by
    /// manifest ID. Manifests are immutable, so entries never go stale.
    checkpoints: HashMap<u64, Arc<ManifestSstPaths>>,
}

impl PolicyState {
    /// Returns true if `manifest_id` is newer than the current manifest
    /// (`newest`), or if the policy has not processed a manifest yet.
    fn is_manifest_newer_than_current(&self, manifest_id: u64) -> bool {
        self.newest
            .as_ref()
            .is_none_or(|newest| manifest_id > newest.id())
    }
}

/// The SST paths that a manifest references, grouped by segment.
#[derive(Default)]
struct ManifestSstPaths(Vec<SegmentPaths>);

impl ManifestSstPaths {
    /// SSTs of other databases resolve under the path of their database.
    fn from_manifest(root: &Path, manifest: &Manifest) -> Self {
        let resolver = PathResolver::new_with_external_ssts(root.clone(), manifest.external_ssts());
        Self(
            manifest
                .core
                .trees_with_prefix()
                .map(|(prefix, tree)| SegmentPaths {
                    prefix,
                    paths: tree
                        .sst_views()
                        .map(|view| resolver.sst_path(&view.sst.id))
                        .collect(),
                })
                .collect(),
        )
    }

    fn segments(&self) -> &[SegmentPaths] {
        &self.0
    }
}

/// The SST paths of one segment in a manifest.
struct SegmentPaths {
    /// The key prefix of the segment. The root tree has the empty prefix.
    prefix: Bytes,
    /// The paths of the L0 SSTs and the sorted run SSTs of the segment.
    paths: HashSet<Path>,
}

/// Returns the tag of a compacted SST call. WAL SST calls and calls with no
/// tag return `None`.
fn compacted_sst_tag(extensions: &Extensions) -> Option<&ObjectStoreCallTag> {
    extensions
        .get::<ObjectStoreCallTag>()
        .filter(|tag| tag.sst_type == SstType::Compacted)
}

/// Splits `<root>/manifest/<id>.manifest` into the root and the manifest ID.
/// Other paths return early, before anything is allocated.
fn parse_manifest_path(path: &Path) -> Option<(Path, u64)> {
    let (dir, file) = path.as_ref().rsplit_once(DELIMITER)?;
    let id = file.strip_suffix(MANIFEST_SUFFIX)?.parse().ok()?;
    let root = match dir.rsplit_once(DELIMITER) {
        Some((root, MANIFEST_DIR)) => root,
        None if dir == MANIFEST_DIR => "",
        _ => return None,
    };
    // `root` is part of a valid path, so parsing it keeps its encoding.
    Some((Path::parse(root).ok()?, id))
}

fn policy_error(message: String) -> object_store::Error {
    object_store::Error::Generic {
        store: POLICY_STORE_NAME,
        source: message.into(),
    }
}

/// A manifest that does not decode stays invalid, so the error is not retried.
fn decode_manifest(bytes: &Bytes) -> object_store::Result<Manifest> {
    FlatBufferManifestCodec {}
        .decode(bytes)
        .map_err(|source| MirrorError::Policy { source }.into())
}

fn manifest_path(root: &Path, manifest_id: u64) -> Path {
    root.clone()
        .join(MANIFEST_DIR)
        .join(format!("{manifest_id:020}{MANIFEST_SUFFIX}"))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use chrono::{DateTime, Utc};
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, UpdateVersion};
    use slatedb_mirror::ObjectStoreMirror;
    use ulid::Ulid;
    use uuid::Uuid;

    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::checkpoint::Checkpoint;
    use crate::compactor_state::{
        Compaction, CompactionContext, CompactionSpec, CompactionStatus, Compactions,
    };
    use crate::config::{DbReaderOptions, FlushOptions, FlushType};
    use crate::db_state::{SsTableHandle, SsTableId, SsTableInfo, SsTableView};
    use crate::error::RetryReason;
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::store::ManifestStore;
    use crate::manifest::{LsmTreeState, ManifestCore, Segment};
    use crate::subcompaction::Subcompaction;
    use crate::{Db, DbReader, DbReaderMode};

    const ROOT: &str = "db";

    fn manifest_file(id: u64) -> Path {
        manifest_path(&Path::from(ROOT), id)
    }

    fn sst_id(n: u64) -> SsTableId {
        SsTableId::from(Ulid::from_parts(n, 0))
    }

    fn sst(n: u64) -> Path {
        PathResolver::from_root(ROOT).sst_path(&sst_id(n))
    }

    fn tag(segment: Option<&'static [u8]>) -> ObjectStoreCallTag {
        ObjectStoreCallTag::new_with_segment(
            TableStoreKind::Main,
            SstType::Compacted,
            segment.map(Bytes::from_static),
        )
    }

    fn get_options(tag: Option<ObjectStoreCallTag>) -> GetOptions {
        GetOptions {
            extensions: tag.map(Into::into).unwrap_or_default(),
            ..GetOptions::default()
        }
    }

    fn put_options(tag: Option<ObjectStoreCallTag>, mode: PutMode) -> PutOptions {
        PutOptions {
            mode,
            extensions: tag.map(Into::into).unwrap_or_default(),
            ..PutOptions::default()
        }
    }

    fn l0(ssts: &[u64]) -> LsmTreeState {
        LsmTreeState {
            l0: ssts
                .iter()
                .map(|n| {
                    SsTableView::identity(SsTableHandle::new(
                        sst_id(*n),
                        SST_FORMAT_VERSION_LATEST,
                        // A decoded manifest needs each SST's first key.
                        SsTableInfo {
                            first_entry: Some(Bytes::from_static(b"a")),
                            ..SsTableInfo::default()
                        },
                    ))
                })
                .collect::<VecDeque<_>>(),
            ..LsmTreeState::default()
        }
    }

    /// A manifest with `root` SSTs in the root tree and the other SSTs in
    /// named segments.
    fn manifest(root: &[u64], segments: &[(&'static [u8], &[u64])]) -> Manifest {
        let mut core = ManifestCore::new();
        core.tree = Arc::new(l0(root));
        if !segments.is_empty() {
            core.segment_extractor_name = Some("test".to_string());
        }
        core.segments = segments
            .iter()
            .map(|(prefix, ssts)| Segment {
                prefix: Bytes::from_static(prefix),
                tree: Arc::new(l0(ssts)),
            })
            .collect();
        Manifest::initial(core)
    }

    fn with_checkpoints(mut manifest: Manifest, manifest_ids: &[u64]) -> Manifest {
        manifest.core.checkpoints = manifest_ids
            .iter()
            .map(|manifest_id| Checkpoint {
                id: Uuid::from_u128(u128::from(*manifest_id)),
                manifest_id: *manifest_id,
                expire_time: None,
                create_time: DateTime::<Utc>::UNIX_EPOCH,
                name: None,
            })
            .collect();
        manifest
    }

    fn encode(manifest: &Manifest) -> Bytes {
        FlatBufferManifestCodec {}.encode(manifest)
    }

    struct Fixture {
        remote: Arc<dyn ObjectStore>,
        policy: Arc<SlateDbMirrorPolicy>,
        mirror: Arc<ObjectStoreMirror>,
        _dir: tempfile::TempDir,
    }

    impl Fixture {
        /// A mirror with no background work and the SSTs `1..=ssts` in the
        /// remote store.
        async fn new(ssts: u64, builder: SlateDbMirrorPolicyBuilder) -> Self {
            let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            for n in 1..=ssts {
                remote
                    .put(&sst(n), Bytes::from_static(b"sst").into())
                    .await
                    .unwrap();
            }
            let dir = tempfile::tempdir().unwrap();
            let policy = builder.with_compactions_poll_interval(None).build();
            let mirror =
                ObjectStoreMirror::builder(dir.path(), Arc::clone(&remote), policy.clone())
                    .with_remote_scan_interval(None)
                    .build()
                    .await
                    .unwrap();
            Self {
                remote,
                policy,
                mirror,
                _dir: dir,
            }
        }

        async fn observe(&self, id: u64, manifest: &Manifest) -> object_store::Result<()> {
            self.policy
                .observe(&manifest_file(id), &encode(manifest), &self.mirror.handle())
                .await
        }

        fn contains(&self, n: u64) -> bool {
            self.mirror.handle().contains(&sst(n))
        }

        fn read_route(&self, tag: ObjectStoreCallTag) -> ReadRoute {
            self.policy
                .read_route(&sst(1), &get_options(Some(tag)))
                .unwrap()
        }
    }

    async fn wait_until(mut done: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if done() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        done()
    }

    #[test]
    fn test_read_route() {
        let policy = SlateDbMirrorPolicy::builder().build();
        let route = |path: &Path, tag| policy.read_route(path, &get_options(tag)).unwrap();
        let mut retry = tag(Some(b""));
        retry.retry = Some(RetryReason::CrcMismatch);
        let wal = ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Wal);

        assert_eq!(route(&manifest_file(1), None), ReadRoute::Observe);
        assert_eq!(route(&sst(1), Some(tag(Some(b"")))), ReadRoute::Local);
        assert_eq!(route(&sst(1), Some(tag(None))), ReadRoute::Local);
        assert_eq!(route(&sst(1), Some(retry)), ReadRoute::Refetch);
        assert_eq!(route(&sst(1), Some(wal)), ReadRoute::Remote);
        assert_eq!(route(&sst(1), None), ReadRoute::Remote);
    }

    #[test]
    fn test_put_route() {
        let policy = SlateDbMirrorPolicy::builder().build();
        let route = |tag, mode| policy.put_route(&sst(1), &put_options(tag, mode));

        assert_eq!(
            policy
                .put_route(&manifest_file(1), &put_options(None, PutMode::Create))
                .unwrap(),
            WriteRoute::Observe
        );
        assert_eq!(
            route(Some(tag(Some(b""))), PutMode::Create).unwrap(),
            WriteRoute::Mirror
        );
        assert_eq!(route(None, PutMode::Overwrite).unwrap(), WriteRoute::Remote);
        let update = PutMode::Update(UpdateVersion {
            e_tag: None,
            version: None,
        });
        assert!(route(Some(tag(Some(b""))), update).is_err());
        assert_eq!(
            policy
                .put_multipart_route(
                    &sst(1),
                    &PutMultipartOptions {
                        extensions: tag(Some(b"")).into(),
                        ..PutMultipartOptions::default()
                    },
                )
                .unwrap(),
            MultipartRoute::Mirror
        );
    }

    #[test]
    fn test_reject_manifest_of_other_root() {
        let policy = SlateDbMirrorPolicy::builder().build();
        policy
            .read_route(&manifest_file(1), &get_options(None))
            .unwrap();
        let other = Path::from("other/manifest/00000000000000000001.manifest");
        assert!(policy.read_route(&other, &get_options(None)).is_err());
        assert!(policy
            .put_route(&other, &put_options(None, PutMode::Create))
            .is_err());
    }

    #[tokio::test]
    async fn test_observe_evicts_ssts_a_newer_manifest_drops() {
        let fixture = Fixture::new(3, SlateDbMirrorPolicy::builder()).await;

        fixture.observe(1, &manifest(&[1, 2], &[])).await.unwrap();
        assert!(fixture.contains(1) && fixture.contains(2));

        fixture.observe(2, &manifest(&[2, 3], &[])).await.unwrap();
        assert!(fixture.contains(3));
        assert!(wait_until(|| !fixture.contains(1)).await);

        // A late read of manifest 1 does not download what manifest 2 dropped.
        fixture.observe(1, &manifest(&[1, 2], &[])).await.unwrap();
        assert!(!fixture.contains(1));
        assert!(fixture.contains(2));
    }

    #[tokio::test]
    async fn test_observe_retains_checkpoint_ssts() {
        let fixture = Fixture::new(3, SlateDbMirrorPolicy::builder()).await;
        let first = manifest(&[1, 2], &[]);
        fixture
            .remote
            .put(&manifest_file(1), encode(&first).into())
            .await
            .unwrap();

        fixture.observe(1, &first).await.unwrap();
        fixture
            .observe(2, &with_checkpoints(manifest(&[3], &[]), &[1]))
            .await
            .unwrap();
        assert_eq!(
            fixture.policy.state.lock().retained,
            HashSet::from([sst(1), sst(2), sst(3)])
        );
        fixture.observe(3, &manifest(&[3], &[])).await.unwrap();
        // Manifest 3 releases the checkpoint.
        assert!(wait_until(|| !fixture.contains(1) && !fixture.contains(2)).await);
        fixture
            .observe(4, &with_checkpoints(manifest(&[3], &[]), &[1]))
            .await
            .unwrap();

        // A checkpoint's SSTs are retained but downloaded only when something
        // reads the checkpoint's manifest.
        assert!(!fixture.contains(1));
        fixture.observe(1, &first).await.unwrap();
        assert!(fixture.contains(1) && fixture.contains(2));
    }

    #[tokio::test]
    async fn test_observe_caches_self_checkpoint() {
        let fixture = Fixture::new(2, SlateDbMirrorPolicy::builder()).await;

        // Manifest 2 is not in the remote store, so manifest 3 must find its
        // checkpoint in the cache.
        fixture
            .observe(2, &with_checkpoints(manifest(&[1], &[]), &[2]))
            .await
            .unwrap();
        fixture
            .observe(3, &with_checkpoints(manifest(&[2], &[]), &[2]))
            .await
            .unwrap();
        assert_eq!(
            fixture.policy.state.lock().retained,
            HashSet::from([sst(1), sst(2)])
        );
    }

    #[tokio::test]
    async fn test_segment_predicate() {
        let builder =
            SlateDbMirrorPolicy::builder().with_segment_predicate(|_, segment| segment != b"cold");
        let fixture = Fixture::new(3, builder).await;
        // The predicate cannot select a segment before the first manifest.
        assert_eq!(fixture.read_route(tag(Some(b"hot"))), ReadRoute::Remote);

        fixture
            .observe(1, &manifest(&[1], &[(b"cold", &[2]), (b"hot", &[3])]))
            .await
            .unwrap();
        assert!(fixture.contains(1) && fixture.contains(3));
        assert!(!fixture.contains(2));
        assert_eq!(fixture.read_route(tag(Some(b"hot"))), ReadRoute::Local);
        assert_eq!(fixture.read_route(tag(Some(b"cold"))), ReadRoute::Remote);
        // The predicate cannot select a call with no segment.
        assert_eq!(fixture.read_route(tag(None)), ReadRoute::Remote);
        assert_eq!(
            fixture
                .policy
                .put_route(
                    &sst(1),
                    &put_options(Some(tag(Some(b"cold"))), PutMode::Create)
                )
                .unwrap(),
            WriteRoute::Remote
        );
    }

    #[tokio::test]
    async fn test_segment_predicate_evicts_deselected_segment() {
        // Only manifest 1 selects the segment.
        let builder =
            SlateDbMirrorPolicy::builder().with_segment_predicate(|manifest, _| manifest.id() == 1);
        let fixture = Fixture::new(1, builder).await;
        let segmented = manifest(&[], &[(b"s", &[1])]);

        fixture.observe(1, &segmented).await.unwrap();
        assert!(fixture.contains(1));
        fixture.observe(2, &segmented).await.unwrap();
        assert!(wait_until(|| !fixture.contains(1)).await);
        assert_eq!(fixture.read_route(tag(Some(b"s"))), ReadRoute::Remote);
    }

    #[tokio::test]
    async fn test_older_manifest_downloads_ssts_that_newest_selects() {
        // Only manifests from 2 on select the segment.
        let builder =
            SlateDbMirrorPolicy::builder().with_segment_predicate(|manifest, _| manifest.id() >= 2);
        let fixture = Fixture::new(2, builder).await;
        let first = manifest(&[], &[(b"s", &[1])]);
        fixture
            .remote
            .put(&manifest_file(1), encode(&first).into())
            .await
            .unwrap();
        fixture
            .observe(2, &with_checkpoints(manifest(&[], &[(b"s", &[2])]), &[1]))
            .await
            .unwrap();
        assert!(!fixture.contains(1));

        // A checkpoint reader reads manifest 1, which the predicate rejects.
        fixture.observe(1, &first).await.unwrap();
        assert!(fixture.contains(1));
        assert_eq!(fixture.read_route(tag(Some(b"s"))), ReadRoute::Local);
    }

    #[tokio::test]
    async fn test_compactor_reads_sst_that_is_not_retained_remotely() {
        let fixture = Fixture::new(1, SlateDbMirrorPolicy::builder()).await;
        fixture.observe(1, &manifest(&[1], &[])).await.unwrap();
        let route = |n, kind| {
            let tag = ObjectStoreCallTag::new(kind, SstType::Compacted);
            fixture
                .policy
                .read_route(&sst(n), &get_options(Some(tag)))
                .unwrap()
        };

        assert_eq!(route(1, TableStoreKind::Compactor), ReadRoute::Local);
        // SST 2 is in no manifest, like the partial output of a compaction
        // that a worker resumes.
        assert_eq!(route(2, TableStoreKind::Compactor), ReadRoute::Remote);
        assert_eq!(route(2, TableStoreKind::Main), ReadRoute::Local);
    }

    #[tokio::test]
    async fn test_compaction_outputs() {
        let policy = SlateDbMirrorPolicy::builder()
            .with_segment_predicate(|_, segment| segment != b"cold")
            .build();
        let handle =
            |n| SsTableHandle::new(sst_id(n), SST_FORMAT_VERSION_LATEST, SsTableInfo::default());
        let compaction = |n, segment: &'static [u8]| {
            Compaction::new(
                Ulid::from_parts(n, 0),
                CompactionSpec::for_segment(Bytes::from_static(segment), vec![], 0),
            )
        };
        let compactions = Compactions::new(0).with_compactions(vec![
            compaction(1, b"")
                .with_status(CompactionStatus::Compacted)
                .with_output_ssts(vec![handle(1)]),
            compaction(2, b"")
                .with_status(CompactionStatus::Running)
                .with_ctx(Some(CompactionContext::new(
                    vec![Subcompaction::new(BytesRange::unbounded())
                        .with_output_ssts(vec![handle(2)])],
                    None,
                ))),
            compaction(3, b"cold")
                .with_status(CompactionStatus::Running)
                .with_output_ssts(vec![handle(3)]),
            compaction(4, b"")
                .with_status(CompactionStatus::Completed)
                .with_output_ssts(vec![handle(4)]),
        ]);
        let manifest = VersionedManifest::from_manifest(1, manifest(&[], &[]));

        assert_eq!(
            policy.compaction_outputs(
                &Path::from(ROOT),
                &manifest,
                &VersionedCompactions::from_compactions(1, compactions),
            ),
            HashSet::from([sst(1), sst(2)])
        );
    }

    #[test]
    fn test_parse_manifest_path() {
        assert_eq!(
            parse_manifest_path(&Path::from("db/manifest/00000000000000000007.manifest")),
            Some((Path::from("db"), 7))
        );
        assert_eq!(
            manifest_file(7),
            Path::from("db/manifest/00000000000000000007.manifest")
        );
        assert_eq!(
            parse_manifest_path(&Path::from("a/b/manifest/00000000000000000007.manifest")),
            Some((Path::from("a/b"), 7))
        );
        assert_eq!(
            parse_manifest_path(&Path::from("manifest/00000000000000000007.manifest")),
            Some((Path::default(), 7))
        );
        assert_eq!(
            parse_manifest_path(&Path::from("db/manifest/boundary")),
            None
        );
        assert_eq!(
            parse_manifest_path(&Path::from("db/other/00000000000000000007.manifest")),
            None
        );
        assert_eq!(parse_manifest_path(&Path::from("db/compacted/x.sst")), None);
    }

    async fn latest_ssts(remote: Arc<dyn ObjectStore>) -> HashSet<Path> {
        let root = Path::from(ROOT);
        let manifest = ManifestStore::new(&root, remote)
            .read_latest_manifest()
            .await
            .unwrap();
        ManifestSstPaths::from_manifest(&root, &manifest.manifest)
            .segments()
            .iter()
            .flat_map(|segment| segment.paths.iter().cloned())
            .collect()
    }

    #[tokio::test]
    async fn test_observe_downloads_manifest_ssts_for_reader() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mirror = |dir: &tempfile::TempDir| {
            ObjectStoreMirror::builder(
                dir.path(),
                Arc::clone(&remote),
                SlateDbMirrorPolicy::builder().build(),
            )
            .with_remote_scan_interval(None)
            .build()
        };
        let writer_dir = tempfile::tempdir().unwrap();
        let reader_dir = tempfile::tempdir().unwrap();
        let writer_mirror = mirror(&writer_dir).await.unwrap();
        let db = Db::builder(ROOT, writer_mirror.clone() as Arc<dyn ObjectStore>)
            .build()
            .await
            .unwrap();
        let flush = FlushOptions {
            flush_type: FlushType::MemTable,
        };
        db.put(b"a", b"1").await.unwrap();
        db.flush_with_options(flush.clone()).await.unwrap();
        let first = latest_ssts(Arc::clone(&remote)).await;
        assert!(!first.is_empty());
        // The writer mirrors its own SSTs as it uploads them.
        assert!(first.iter().all(|sst| writer_mirror.handle().contains(sst)));

        let reader_mirror = mirror(&reader_dir).await.unwrap();
        let reader = DbReader::builder(ROOT, reader_mirror.clone() as Arc<dyn ObjectStore>)
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_options(DbReaderOptions {
                manifest_poll_interval: Duration::from_millis(50),
                skip_wal_replay: true,
                ..DbReaderOptions::default()
            })
            .build()
            .await
            .unwrap();
        // Opening the reader reads the manifest, which downloads its SSTs.
        assert!(first.iter().all(|sst| reader_mirror.handle().contains(sst)));
        assert_eq!(
            reader.get(b"a").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );

        db.put(b"b", b"2").await.unwrap();
        db.flush_with_options(flush).await.unwrap();
        let second = latest_ssts(Arc::clone(&remote)).await;
        assert!(second.len() > first.len());
        // The reader's next poll downloads the new SST.
        assert!(
            wait_until(|| second
                .iter()
                .all(|sst| reader_mirror.handle().contains(sst)))
            .await
        );

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }
}
