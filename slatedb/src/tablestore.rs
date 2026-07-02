use std::collections::VecDeque;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use fail_parallel::{fail_point, FailPointRegistry};
use futures::{future::join_all, StreamExt};
use log::{debug, warn};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{
    Extensions, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
};
use slatedb_common::object_metadata::IdentifiedObjectMetadata;
use slatedb_common::ObjectMetadata;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::blob::ReadOnlyBlob;
use crate::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache, EncodedCachedFilter};
use crate::db_cache_manager::CacheTarget;
use crate::db_state::{SsTableHandle, SsTableId, SstType};
use crate::error::SlateDBError;
use crate::filter_policy::NamedFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::format::sst::{EncodedSsTable, SsTableFormat};
use crate::object_store_tag::ObjectStoreCallTag;
pub(crate) use crate::object_store_tag::TableStoreKind;
use crate::object_stores::{ObjectStoreType, ObjectStores};
use crate::paths::PathResolver;
use crate::sst_builder::EncodedSsTableBuilder;
use crate::sst_stats::SstStats;
use crate::types::RowEntry;
use crate::wal::wal_sst_builder::EncodedWalSsTableBuilder;

pub(crate) struct TableStore {
    object_stores: ObjectStores,
    sst_format: SsTableFormat,
    path_resolver: PathResolver,
    #[allow(dead_code)]
    fp_registry: Arc<FailPointRegistry>,
    /// In-memory cache for data blocks, indices, and filters
    cache: Option<Arc<dyn DbCache>>,
    /// Which component owns this store. Tagged on compacted-SST calls.
    kind: TableStoreKind,
}

struct ReadOnlyObject {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    tag: ObjectStoreCallTag,
}

impl ReadOnlyObject {
    fn extensions(&self) -> Extensions {
        self.tag.into()
    }
}

/// Reads from a [`ReadOnlyObject`] for an SST `$id`, with validation-retry.
///
/// It expands to the retry-wrapper future, so callers `.await` it.
/// This is used instead of repeating the same retry logic for every individual
/// read from an SST object.
macro_rules! read_obj {
    ($store:expr, $id:expr, |$obj:ident| $read:expr) => {{
        let object_store = $store.object_stores.store_for($id);
        let path = $store.path($id);
        read_with_validation_retry(
            ObjectStoreCallTag::new($store.kind, SstType::from($id)),
            move |tag| {
                let object_store = object_store.clone();
                let path = path.clone();
                async move {
                    let $obj = ReadOnlyObject {
                        object_store,
                        path,
                        tag,
                    };
                    $read.await.map_err(|e| e.with_path(&$obj.path))
                }
            },
        )
    }};
}

impl ReadOnlyBlob for ReadOnlyObject {
    async fn len(&self) -> Result<u64, SlateDBError> {
        let opts = GetOptions {
            head: true,
            extensions: self.extensions(),
            ..GetOptions::default()
        };
        let result = self.object_store.get_opts(&self.path, opts).await?;
        Ok(result.meta.size)
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SlateDBError> {
        let opts = GetOptions {
            range: Some(GetRange::Bounded(range)),
            extensions: self.extensions(),
            ..GetOptions::default()
        };
        let result = self.object_store.get_opts(&self.path, opts).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    async fn read(&self) -> Result<Bytes, SlateDBError> {
        let opts = GetOptions {
            extensions: self.extensions(),
            ..GetOptions::default()
        };
        let result = self.object_store.get_opts(&self.path, opts).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }
}

impl TableStore {
    pub(crate) fn new<P: Into<Path>>(
        object_stores: ObjectStores,
        sst_format: SsTableFormat,
        root_path: P,
        block_cache: Option<Arc<dyn DbCache>>,
        kind: TableStoreKind,
    ) -> Self {
        Self::new_with_fp_registry(
            object_stores,
            sst_format,
            PathResolver::new(root_path),
            Arc::new(FailPointRegistry::new()),
            block_cache,
            kind,
        )
    }

    pub(crate) fn new_with_fp_registry(
        object_stores: ObjectStores,
        sst_format: SsTableFormat,
        path_resolver: PathResolver,
        fp_registry: Arc<FailPointRegistry>,
        cache: Option<Arc<dyn DbCache>>,
        kind: TableStoreKind,
    ) -> Self {
        Self {
            object_stores,
            sst_format,
            path_resolver,
            fp_registry,
            cache,
            kind,
        }
    }

    /// Get the number of blocks for a size specified in bytes.
    /// The returned value will be rounded down to the nearest block.
    pub(crate) fn bytes_to_blocks(&self, bytes: usize) -> usize {
        bytes.div_ceil(self.sst_format.block_size)
    }

    /// Find the highest WAL SST id present in the object store at or above
    /// `start_after + 1`, returning `start_after` if none exist.
    ///
    /// `start_after` should be a known lower bound (e.g. `replay_after_wal_id`
    /// from the manifest, or the highest already-replayed WAL id). Passing 0
    /// scans the entire WAL id space.
    ///
    /// Two phases:
    ///   1. Parallel exponential probe at offsets `2^0, 2^1, ..., 2^k` from
    ///      `start_after`. One RTT per round of 8 exponents. Brackets the
    ///      frontier between two adjacent powers of two.
    ///   2. Sequential binary search inside the bracketed range to find the
    ///      exact frontier.
    ///
    /// Relies on the fencing protocol's contiguity invariant: "id exists" is
    /// monotone-decreasing in id, so binary search is sound. Total HEAD count
    /// is `O(log N)` for a gap of size N, vs `O(N)` for a windowed scan.
    pub(crate) async fn last_seen_wal_id(&self, start_after: u64) -> Result<u64, SlateDBError> {
        fail_point!(Arc::clone(&self.fp_registry), "probe-wal-ssts", |_| {
            Err(SlateDBError::from(std::io::Error::other("oops")))
        });

        // 8 probes per round amortizes tail latency on one RTT while keeping
        // the concurrent HEAD fan-out bounded against the object store.
        const ROUND_SIZE: u32 = 8;
        // 2^48 ahead is ~280 trillion WALs; far past any plausible gap. Beyond
        // this we give up rather than overflow / loop forever.
        const MAX_EXP: u32 = 48;

        let object_store = self.object_stores.store_of(ObjectStoreType::Wal);

        // ---- Phase 1: bracket the frontier with exponential probes. ----
        //
        // Probe at offsets 2^0, 2^1, ..., 2^MAX_EXP above `start_after`.
        // Contiguity (existence is monotone-decreasing in id) means the
        // moment one probe misses, every higher offset is also guaranteed
        // missing -- so we can stop at the first miss and treat that
        // offset as the upper bound on the answer. The highest hit so far
        // is the lower bound.
        let mut lo_offset: Option<u64> = None; // highest offset known to exist
        let mut hi_offset: Option<u64> = None; // lowest offset known to NOT exist
        let mut next_exp: u32 = 0;

        while hi_offset.is_none() {
            if next_exp >= MAX_EXP {
                // Object store appears to contain ids past our sanity cap;
                // bail rather than overflow `start_after + offset`.
                return Err(SlateDBError::InvalidDBState);
            }

            // Fire ROUND_SIZE probes in parallel at offsets 2^next_exp..2^end_exp.
            // One round = one network RTT regardless of how many probes it contains.
            let end_exp = (next_exp + ROUND_SIZE).min(MAX_EXP);
            let exps: Vec<u32> = (next_exp..end_exp).collect();
            let probes = exps.iter().map(|&e| {
                let offset = 1u64 << e;
                let path = self.path(&SsTableId::Wal(start_after + offset));
                let store = object_store.clone();
                async move { wal_object_exists(&store, &path).await }
            });
            let results = join_all(probes).await;

            // Walk the round in offset order. Track the last hit as `lo_offset`,
            // then stop at the first miss -- anything past that miss in the
            // round is wasted info because we're about to switch to binary search.
            for (e, r) in exps.iter().zip(results) {
                let offset = 1u64 << e;
                if r? {
                    lo_offset = Some(offset);
                } else {
                    hi_offset = Some(offset);
                    break;
                }
            }
            next_exp = end_exp;
        }

        let hi = hi_offset.expect("loop only exits when hi is set");
        let lo = match lo_offset {
            // Round 0's smallest offset (1) didn't exist, so no WALs are
            // visible above the hint -- the frontier IS `start_after`.
            None => return Ok(start_after),
            Some(o) => o,
        };

        // ---- Phase 2: binary search the open interval (lo, hi). ----
        //
        // Invariants entering the loop:
        //   * offset = lo  exists       (highest from Phase 1)
        //   * offset = hi  does not     (first miss from Phase 1)
        // So the largest existing offset lives in [lo, hi - 1]. Search
        // strictly above lo (left = lo + 1) so we never re-probe a slot
        // whose state we already know.
        let mut left = lo + 1;
        let mut right = hi;
        while left < right {
            let mid = left + (right - left) / 2;
            let path = self.path(&SsTableId::Wal(start_after + mid));
            if wal_object_exists(object_store, &path).await? {
                // `mid` exists, so the answer is mid or higher; discard
                // everything at-or-below mid.
                left = mid + 1;
            } else {
                // `mid` is missing, so the answer is strictly below mid.
                right = mid;
            }
        }

        // Loop exits with left == right pointing at the lowest offset known
        // to not exist; the highest existing offset is one below that.
        Ok(start_after + left - 1)
    }

    /// Gracefully close the block cache, flushing in-memory entries to disk.
    pub(crate) async fn close_cache(&self) -> Result<(), crate::Error> {
        if let Some(ref cache) = self.cache {
            cache.close().await?;
        }
        Ok(())
    }

    pub(crate) async fn list_wal_ssts<R: RangeBounds<u64>>(
        &self,
        id_range: R,
    ) -> Result<Vec<IdentifiedObjectMetadata<SsTableId>>, SlateDBError> {
        fail_point!(Arc::clone(&self.fp_registry), "list-wal-ssts", |_| {
            Err(SlateDBError::from(std::io::Error::other("oops")))
        });

        let mut wal_list: Vec<IdentifiedObjectMetadata<SsTableId>> = Vec::new();
        let wal_path = &self.path_resolver.wal_path();
        let mut files_stream = self
            .object_stores
            .store_of(ObjectStoreType::Wal)
            .list(Some(wal_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match self.path_resolver.parse_table_id(&file.location) {
                Ok(Some(SsTableId::Wal(id))) => {
                    if id_range.contains(&id) {
                        wal_list.push(IdentifiedObjectMetadata::from_object_meta(
                            SsTableId::Wal(id),
                            file,
                        ));
                    }
                }
                _ => continue,
            }
        }
        wal_list.sort_by_key(|m| m.id.unwrap_wal_id());
        Ok(wal_list)
    }

    pub(crate) async fn next_wal_sst_id(
        &self,
        wal_id_last_compacted: u64,
    ) -> Result<u64, SlateDBError> {
        Ok(self.last_seen_wal_id(wal_id_last_compacted).await? + 1)
    }

    pub(crate) fn table_writer(self: &Arc<Self>, id: SsTableId) -> EncodedSsTableWriter {
        let object_store = self.object_stores.store_for(&id);
        let path = self.path(&id);
        EncodedSsTableWriter {
            id,
            builder: self.sst_format.table_builder(),
            writer: tagged_buf_writer(
                object_store,
                path,
                ObjectStoreCallTag::new(self.kind, SstType::from(&id)),
            ),
            table_store: self.clone(),
            #[cfg(test)]
            blocks_written: 0,
        }
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        self.sst_format.table_builder()
    }

    pub(crate) fn wal_table_builder(&self) -> EncodedWalSsTableBuilder {
        self.sst_format.wal_table_builder()
    }

    pub(crate) async fn write_sst(
        &self,
        id: &SsTableId,
        encoded_sst: &EncodedSsTable,
        write_cache: bool,
    ) -> Result<SsTableHandle, SlateDBError> {
        fail_point!(
            self.fp_registry.clone(),
            "write-wal-sst-io-error",
            matches!(id, SsTableId::Wal(_)),
            |_| Result::Err(slatedb_io_error())
        );
        fail_point!(
            self.fp_registry.clone(),
            "write-compacted-sst-io-error",
            matches!(id, SsTableId::Compacted(_)),
            |_| Result::Err(slatedb_io_error())
        );

        let object_store = self.object_stores.store_for(id);
        let path = self.path(id);
        match id {
            SsTableId::Compacted(_) => {
                write_sst_streaming_in_object_store(
                    object_store.clone(),
                    &path,
                    encoded_sst,
                    ObjectStoreCallTag::new(self.kind, SstType::from(id)),
                )
                .await?;
            }
            // WAL SSTs rely on PutMode::Create for fencing. The generic
            // object_store multipart API cannot express that condition, so WALs
            // stay on the conditional single-PUT path for now.
            SsTableId::Wal(_) => {
                let data = encoded_sst.remaining_as_bytes();
                write_sst_in_object_store(
                    object_store.clone(),
                    id,
                    &path,
                    &data,
                    ObjectStoreCallTag::new(self.kind, SstType::from(id)),
                )
                .await?;
            }
        }

        if let Some(ref cache) = self.cache {
            if write_cache {
                for block in &encoded_sst.unconsumed_blocks {
                    cache
                        .insert(
                            (*id, block.offset).into(),
                            CachedEntry::with_block(Arc::clone(&block.block)),
                        )
                        .await;
                }
                cache
                    .insert(
                        (*id, encoded_sst.info.index_offset).into(),
                        CachedEntry::with_sst_index(Arc::new(encoded_sst.index.clone())),
                    )
                    .await;
            }
        }
        self.cache_filters(
            *id,
            encoded_sst.info.filter_offset,
            encoded_sst.filters.clone(),
        )
        .await;
        Ok(SsTableHandle::new(
            *id,
            encoded_sst.format_version,
            encoded_sst.info.clone(),
        ))
    }

    /// Writes a zero-byte WAL object as a fencing marker.
    ///
    /// Uses create-if-absent semantics so any existing WAL object at this ID
    /// fences the writer by returning [`SlateDBError::Fenced`].
    pub(crate) async fn write_wal_fence(&self, wal_id: u64) -> Result<(), SlateDBError> {
        let id = SsTableId::Wal(wal_id);
        fail_point!(self.fp_registry.clone(), "write-wal-sst-io-error", |_| {
            Result::Err(slatedb_io_error())
        });
        write_sst_in_object_store(
            self.object_stores.store_for(&id),
            &id,
            &self.path(&id),
            &Bytes::new(),
            ObjectStoreCallTag::new(self.kind, SstType::from(&id)),
        )
        .await
    }

    async fn cache_filters(&self, sst: SsTableId, id: u64, filters: Arc<[NamedFilter]>) {
        let Some(ref cache) = self.cache else {
            return;
        };
        if !filters.is_empty() {
            cache
                .insert((sst, id).into(), CachedEntry::with_filters(filters))
                .await;
        }
    }

    /// Decodes an `EncodedCachedFilter` slice into a fully-decoded
    /// `Arc<[NamedFilter]>` and overwrites the cache entry under `cache_key`
    /// with the decoded form so subsequent hits bypass the decode step.
    /// Entries whose policy name has no match in the configured policies are
    /// dropped.
    async fn decode_and_refresh(
        &self,
        cache: &Arc<dyn DbCache>,
        cache_key: CachedKey,
        encoded: &[EncodedCachedFilter],
    ) -> Arc<[NamedFilter]> {
        let policies = &self.sst_format.filter_policies;
        let decoded: Arc<[NamedFilter]> = encoded
            .iter()
            .filter_map(|ef| match policies.iter().find(|p| p.name() == ef.name) {
                Some(policy) => Some(NamedFilter {
                    name: ef.name.clone(),
                    filter: policy.decode(&ef.data),
                }),
                None => {
                    warn!(
                        "unknown filter policy '{}' in cached filter, skipping",
                        ef.name
                    );
                    None
                }
            })
            .collect::<Vec<_>>()
            .into();

        cache
            .insert(cache_key, CachedEntry::with_filters(decoded.clone()))
            .await;
        decoded
    }

    /// Delete an SSTable from the object store.
    pub(crate) async fn delete_sst(&self, id: &SsTableId) -> Result<(), SlateDBError> {
        let object_store = self.object_stores.store_for(id);
        let path = self.path(id);
        debug!("deleting SST [path={}]", path);
        object_store.delete(&path).await.map_err(SlateDBError::from)
    }

    /// Reads metadata for a specific SST object (WAL or compacted).
    ///
    /// ## Arguments
    /// - `id`: The SST identifier to fetch metadata for.
    ///
    /// ## Returns
    /// - `Ok(ObjectMetadata)` containing the path, last-modified time,
    ///   size in bytes, ETag, and version.
    ///
    /// ## Errors
    /// - Returns [`SlateDBError`] if the underlying object store `head` request
    ///   fails (for example, if the object does not exist or storage access
    ///   fails).
    pub(crate) async fn metadata(&self, id: &SsTableId) -> Result<ObjectMetadata, SlateDBError> {
        let object_store = self.object_stores.store_for(id);
        let path = self.path(id);
        let opts = GetOptions {
            head: true,
            extensions: ObjectStoreCallTag::new(self.kind, SstType::from(id)).into(),
            ..GetOptions::default()
        };
        Ok(ObjectMetadata::new(
            object_store.get_opts(&path, opts).await?.meta,
        ))
    }

    /// List all SSTables in the compacted directory.
    /// The SSTables are returned in ascending order of their IDs. Ulids within
    /// the same millisecond are sorted based on their random suffix.
    /// # Arguments
    /// * `id_range` - The range of IDs to list
    /// # Returns
    /// A list of SSTables in the compacted directory
    pub(crate) async fn list_compacted_ssts<R: RangeBounds<Ulid>>(
        &self,
        id_range: R,
    ) -> Result<Vec<IdentifiedObjectMetadata<SsTableId>>, SlateDBError> {
        let mut sst_list: Vec<IdentifiedObjectMetadata<SsTableId>> = Vec::new();
        let compacted_path = self.path_resolver.compacted_path();
        let mut files_stream = self
            .object_stores
            .store_of(ObjectStoreType::Main)
            .list(Some(&compacted_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match self.path_resolver.parse_table_id(&file.location) {
                Ok(Some(SsTableId::Compacted(id))) => {
                    if id_range.contains(&id) {
                        sst_list.push(IdentifiedObjectMetadata::from_object_meta(
                            SsTableId::Compacted(id),
                            file,
                        ));
                    }
                }
                Err(e) => {
                    warn!(
                        "error while parsing file id [location={}, error={}]",
                        file.location, e
                    );
                }
                _ => {
                    warn!(
                        "unexpected file found in compacted directory [location={}]",
                        file.location
                    );
                }
            }
        }

        sst_list.sort_by_key(|m| m.id.unwrap_compacted_id());
        Ok(sst_list)
    }

    pub(crate) async fn open_sst(&self, id: &SsTableId) -> Result<SsTableHandle, SlateDBError> {
        let (info, version) =
            read_obj!(self, id, |obj| self.sst_format.read_info_and_version(&obj)).await?;
        Ok(SsTableHandle::new(*id, version, info))
    }

    #[cfg(test)]
    pub(crate) async fn read_sst_version(&self, id: &SsTableId) -> Result<u16, SlateDBError> {
        let (_, version) =
            read_obj!(self, id, |obj| self.sst_format.read_info_and_version(&obj)).await?;
        Ok(version)
    }

    /// Reads the filters of an SSTable. Every returned entry is decoded.
    ///
    /// ## Arguments
    /// - `handle`: The handle of the SSTable to read the filters from.
    /// - `cache_blocks`: Whether to cache the filters after reading them.
    pub(crate) async fn read_filters(
        &self,
        handle: &SsTableHandle,
        cache_blocks: bool,
    ) -> Result<Arc<[NamedFilter]>, SlateDBError> {
        // No filter exists for this SST (either no policies configured, or the
        // SST was built below `min_filter_keys`). Return an empty slice without
        // touching the cache: there is nothing to load, and caching the empty
        // answer would mask a future config change that adds filter policies.
        if self.sst_format.filter_policies.is_empty() || handle.info.filter_len == 0 {
            return Ok(Arc::from([]));
        }
        let cache_key: CachedKey = (handle.id, handle.info.filter_offset).into();
        if let Some(ref cache) = self.cache {
            // cache_blocks=true: dedup-aware fetch; concurrent callers collapse onto
            // one loader. cache_blocks=false: read-only lookup that won't pollute the
            // cache on miss. Cache errors fall through to a best-effort direct load;
            // we intentionally don't re-insert there — `fetch_X` errors are almost
            // always the smuggled loader error (so the direct retry will also fail),
            // and on the rare foyer-machinery error an insert would likely fail too.
            let entry = if cache_blocks {
                cache
                    .fetch_filter(
                        cache_key.clone(),
                        self.read_loader(handle, CacheTarget::Filters),
                    )
                    .await
                    .ok()
            } else {
                cache.get_filter(&cache_key).await.unwrap_or(None)
            };
            if let Some(entry) = entry {
                // Already decoded.
                if let Some(filters) = entry.filters() {
                    return Ok(filters);
                }
                // Encoded form from disk-cache deserialize. Decode and overwrite
                // the cache entry with the decoded form.
                if let Some(encoded) = entry.encoded_filters() {
                    return Ok(self.decode_and_refresh(cache, cache_key, &encoded).await);
                }
            }
        }
        read_obj!(self, &handle.id, |obj| self
            .sst_format
            .read_filters(&handle.info, &obj))
        .await
    }

    /// Reads the stats block of an SSTable.
    ///
    /// ## Arguments
    /// - `handle`: The handle of the SSTable to read the stats from.
    pub(crate) async fn read_stats(
        &self,
        handle: &SsTableHandle,
        cache_blocks: bool,
    ) -> Result<Option<SstStats>, SlateDBError> {
        if handle.info.stats_len == 0 {
            return Ok(None);
        }
        let cache_key = (handle.id, handle.info.stats_offset).into();
        if let Some(ref cache) = self.cache {
            // See `read_filters` for the rationale on the fall-through path.
            let entry = if cache_blocks {
                cache
                    .fetch_stats(cache_key, self.read_loader(handle, CacheTarget::Stats))
                    .await
                    .ok()
            } else {
                cache.get_stats(&cache_key).await.unwrap_or(None)
            };
            if let Some(stats) = entry.and_then(|e| e.sst_stats()) {
                return Ok(Some(stats.as_ref().clone()));
            }
        }
        read_obj!(self, &handle.id, |obj| self
            .sst_format
            .read_stats(&handle.info, &obj))
        .await
    }

    /// Reads the index of an SSTable.
    ///
    /// ## Arguments
    /// - `handle`: The handle of the SSTable to read the index from.
    /// - `cache_blocks`: Whether to cache the index blocks after reading them.
    pub(crate) async fn read_index(
        &self,
        handle: &SsTableHandle,
        cache_blocks: bool,
    ) -> Result<Arc<SsTableIndexOwned>, SlateDBError> {
        let cache_key = (handle.id, handle.info.index_offset).into();
        if let Some(ref cache) = self.cache {
            // See `read_filters` for the rationale on the fall-through path.
            let entry = if cache_blocks {
                cache
                    .fetch_index(cache_key, self.read_loader(handle, CacheTarget::Index))
                    .await
                    .ok()
            } else {
                cache.get_index(&cache_key).await.unwrap_or(None)
            };
            if let Some(index) = entry.and_then(|e| e.sst_index()) {
                return Ok(index);
            }
        }
        let index = read_obj!(self, &handle.id, |obj| self
            .sst_format
            .read_index(&handle.info, &obj))
        .await?;
        Ok(Arc::new(index))
    }

    /// Build a [`CacheLoader`] for a section-level cache entry (filter, stats,
    /// or index). Panics on [`CacheTarget::Data`]: data blocks resolve through
    /// [`Self::block_loader`] because they require a block index lookup.
    ///
    /// Builds a fresh boxed closure on every call, even when the cache hits and
    /// the loader is never invoked; revisit if cache-hit allocations show up in
    /// profiles.
    fn read_loader(&self, handle: &SsTableHandle, target: CacheTarget) -> CacheLoader {
        let info = handle.info.clone();
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        let sst_format = self.sst_format.clone();
        let tag = ObjectStoreCallTag::new(self.kind, SstType::from(&handle.id));
        Box::new(move || {
            Box::pin(async move {
                // Only the stats arm can produce `None` (stats_len > 0 but no
                // stats decoded); filters and index always yield an entry.
                let entry = read_with_validation_retry(tag, async |tag| {
                    let obj = ReadOnlyObject {
                        object_store: object_store.clone(),
                        path: path.clone(),
                        tag,
                    };
                    match target.clone() {
                        CacheTarget::Filters => {
                            let filters = sst_format
                                .read_filters(&info, &obj)
                                .await
                                .map_err(|e| e.with_path(&obj.path))?;
                            Ok(Some(CachedEntry::with_filters(filters)))
                        }
                        CacheTarget::Stats => {
                            let stats = sst_format
                                .read_stats(&info, &obj)
                                .await
                                .map_err(|e| e.with_path(&obj.path))?;
                            Ok(stats.map(|s| CachedEntry::with_sst_stats(Arc::new(s))))
                        }
                        CacheTarget::Index => {
                            let index = sst_format
                                .read_index(&info, &obj)
                                .await
                                .map_err(|e| e.with_path(&obj.path))?;
                            Ok(Some(CachedEntry::with_sst_index(Arc::new(index))))
                        }
                        CacheTarget::Data(_) => {
                            unreachable!("data blocks use block_loader, not read_loader")
                        }
                    }
                })
                .await?;
                entry.ok_or_else(|| {
                    crate::Error::data("stats_len > 0 but read_stats returned no stats".to_string())
                })
            })
        })
    }

    /// Builds a fresh boxed closure on every call, even when the cache hits and
    /// the loader is never invoked; revisit if cache-hit allocations show up in
    /// profiles.
    fn block_loader(
        &self,
        handle: &SsTableHandle,
        index: Arc<SsTableIndexOwned>,
        block_num: usize,
    ) -> CacheLoader {
        let info = handle.info.clone();
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        let sst_format = self.sst_format.clone();
        let tag = ObjectStoreCallTag::new(self.kind, SstType::from(&handle.id));
        Box::new(move || {
            Box::pin(async move {
                let block = read_with_validation_retry(tag, async |tag| {
                    let obj = ReadOnlyObject {
                        object_store: object_store.clone(),
                        path: path.clone(),
                        tag,
                    };
                    sst_format
                        .read_block(&info, &index, block_num, &obj)
                        .await
                        .map_err(|e| e.with_path(&obj.path))
                })
                .await?;
                Ok(CachedEntry::with_block(Arc::new(block)))
            })
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn read_blocks(
        &self,
        handle: &SsTableHandle,
        blocks: Range<usize>,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        read_with_validation_retry(
            ObjectStoreCallTag::new(self.kind, SstType::from(&handle.id)),
            |tag| {
                let obj = ReadOnlyObject {
                    object_store: object_store.clone(),
                    path: path.clone(),
                    tag,
                };
                let blocks = blocks.clone();
                async move {
                    let index = self
                        .sst_format
                        .read_index(&handle.info, &obj)
                        .await
                        .map_err(|e| e.with_path(&obj.path))?;
                    self.sst_format
                        .read_blocks(&handle.info, &index, blocks, &obj)
                        .await
                        .map_err(|e| e.with_path(&obj.path))
                }
            },
        )
        .await
    }

    /// Reads specified blocks from an SSTable using the provided index.
    ///
    /// This function attempts to read blocks from the cache if available
    /// and falls back to reading from storage for uncached blocks
    /// using an async fetch for each contiguous range that blocks are not cached.
    /// It can optionally cache newly read blocks.
    pub(crate) async fn read_blocks_using_index(
        &self,
        handle: &SsTableHandle,
        index: Arc<SsTableIndexOwned>,
        blocks: Range<usize>,
        cache_blocks: bool,
    ) -> Result<VecDeque<Arc<Block>>, SlateDBError> {
        // Single-block reads (point-gets via SstIterator::for_key, SstFile::read_block,
        // etc.) take a dedup-aware fast-path: concurrent callers for the same block
        // collapse onto one loader. Multi-block reads fall through to the range-
        // coalesced path below, which issues one object-store GET per contiguous
        // run of uncached blocks. Cache errors fall through to the direct load,
        // which produces the authoritative error if any.
        if cache_blocks && blocks.len() == 1 {
            if let Some(ref cache) = self.cache {
                let block_num = blocks.start;
                let offset = index.borrow().block_meta().get(block_num).offset();
                let cache_key: CachedKey = (handle.id, offset).into();
                let loader = self.block_loader(handle, index.clone(), block_num);
                if let Ok(entry) = cache.fetch_block(cache_key, loader).await {
                    if let Some(block) = entry.block() {
                        let mut result = VecDeque::with_capacity(1);
                        result.push_back(block);
                        return Ok(result);
                    }
                }
            }
        }

        let object_store = self.object_stores.store_for(&handle.id);
        let path = self.path(&handle.id);
        // Initialize the result vector and a vector to track uncached ranges
        let mut blocks_read = VecDeque::with_capacity(blocks.end - blocks.start);
        let mut uncached_ranges = Vec::new();

        // If block cache is available, try to retrieve cached blocks
        if let Some(ref cache) = self.cache {
            let index_borrow = index.borrow();
            // Attempt to get all requested blocks from cache concurrently
            let cached_blocks = join_all(blocks.clone().map(|block_num| async move {
                let block_meta = index_borrow.block_meta().get(block_num);
                let offset = block_meta.offset();
                cache
                    .get_block(&(handle.id, offset).into())
                    .await
                    .unwrap_or(None)
                    .and_then(|entry| entry.block())
            }))
            .await;

            let mut last_uncached_start = None;

            // Process cached block results
            for (index, block_result) in cached_blocks.into_iter().enumerate() {
                match block_result {
                    Some(cached_block) => {
                        // If a cached block is found, add it to blocks_read
                        if let Some(start) = last_uncached_start.take() {
                            uncached_ranges.push((blocks.start + start)..(blocks.start + index));
                        }
                        blocks_read.push_back(cached_block);
                    }
                    None => {
                        // If a block is not in cache, mark the start of an uncached range
                        last_uncached_start.get_or_insert(index);
                    }
                }
            }
            // Add the last uncached range if it exists
            if let Some(start) = last_uncached_start {
                uncached_ranges.push((blocks.start + start)..blocks.end);
            }
        } else {
            // If no cache is available, treat all blocks as uncached
            uncached_ranges.push(blocks.clone());
        }
        // Read uncached blocks concurrently
        let uncached_blocks = join_all(uncached_ranges.iter().map(|range| {
            let object_store = &object_store;
            let path = &path;
            let index_ref = &index;
            async move {
                read_with_validation_retry(
                    ObjectStoreCallTag::new(self.kind, SstType::from(&handle.id)),
                    |tag| {
                        let obj = ReadOnlyObject {
                            object_store: object_store.clone(),
                            path: path.clone(),
                            tag,
                        };
                        async move {
                            self.sst_format
                                .read_blocks(&handle.info, index_ref, range.clone(), &obj)
                                .await
                                .map_err(|e| e.with_path(&obj.path))
                        }
                    },
                )
                .await
            }
        }))
        .await;

        // Merge uncached blocks with blocks_read and prepare blocks for caching
        let mut blocks_to_cache = vec![];
        for (range, range_blocks) in uncached_ranges.into_iter().zip(uncached_blocks) {
            let index_borrow = index.borrow();
            for (block_num, block_read) in range.zip(range_blocks?) {
                let block = Arc::new(block_read);
                if cache_blocks {
                    let block_meta = index_borrow.block_meta().get(block_num);
                    let offset = block_meta.offset();
                    blocks_to_cache.push((handle.id, offset, block.clone()));
                }
                blocks_read.insert(block_num - blocks.start, block);
            }
        }

        // Cache the newly read blocks if caching is enabled
        if let Some(ref cache) = self.cache {
            if !blocks_to_cache.is_empty() {
                join_all(blocks_to_cache.into_iter().map(|(id, offset, block)| {
                    cache.insert((id, offset).into(), CachedEntry::with_block(block))
                }))
                .await;
            }
        }

        Ok(blocks_read)
    }

    #[allow(dead_code)]
    pub(crate) async fn read_block(
        &self,
        handle: &SsTableHandle,
        block: usize,
    ) -> Result<Block, SlateDBError> {
        read_obj!(self, &handle.id, |obj| async {
            let index = self.sst_format.read_index(&handle.info, &obj).await?;
            self.sst_format
                .read_block(&handle.info, &index, block, &obj)
                .await
        })
        .await
    }

    fn path(&self, id: &SsTableId) -> Path {
        self.path_resolver.table_path(id)
    }

    pub(crate) fn estimate_encoded_size_compacted(
        &self,
        num_entries: usize,
        size_entries: usize,
    ) -> usize {
        self.sst_format
            .estimate_encoded_size_compacted(num_entries, size_entries)
    }

    pub(crate) fn estimate_encoded_size_wal(
        &self,
        num_entries: usize,
        size_entries: usize,
    ) -> usize {
        self.sst_format
            .estimate_encoded_size_wal(num_entries, size_entries)
    }

    pub(crate) fn cache(&self) -> Option<&Arc<dyn DbCache>> {
        self.cache.as_ref()
    }

    /// Best-effort removal of all cache entries associated with the given SST:
    /// data blocks, index, filters, and stats. Returns the offsets whose
    /// cache removal was attempted.
    pub(crate) async fn evict_sst_from_cache(&self, handle: &SsTableHandle) {
        let Some(ref cache) = self.cache else {
            return;
        };
        // Best effort: if we can't read the index we can't enumerate blocks,
        // so log and skip. Remaining entries will age out under normal pressure.
        let index = match self.read_index(handle, false).await {
            Ok(index) => index,
            Err(e) => {
                warn!(
                    "evict_sst_from_cache: failed to read index for SST {:?}: {}",
                    handle.id, e
                );
                return;
            }
        };
        {
            let index_borrow = index.borrow();
            let meta = index_borrow.block_meta();
            for block_num in 0..meta.len() {
                let offset = meta.get(block_num).offset();
                cache.remove(&(handle.id, offset).into()).await;
            }
        }
        cache
            .remove(&(handle.id, handle.info.index_offset).into())
            .await;
        // Only evict filter/stats when those sections exist. Otherwise
        // SsTableInfo's filter_offset collides with index_offset (filter_len
        // == 0) and stats_offset collides with the first data block
        // (stats_offset == 0).
        if handle.info.filter_len > 0 {
            cache
                .remove(&(handle.id, handle.info.filter_offset).into())
                .await;
        }
        if handle.info.stats_len > 0 {
            cache
                .remove(&(handle.id, handle.info.stats_offset).into())
                .await;
        }
    }
}

async fn wal_object_exists(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<bool, SlateDBError> {
    match object_store.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(SlateDBError::from(e)),
    }
}

/// Number of additional attempts after an SST read fails validation. The
/// reissue carries a [`RetryReason`](crate::error::RetryReason) so a caching
/// wrapper drops its local copy.
const MAX_VALIDATION_RETRIES: usize = 1;

/// Runs `read` with the source/type `tag`, reissuing it with a
/// [`RetryReason`](crate::error::RetryReason) set on the tag when the result is
/// a recoverable validation failure.
///
/// This is done to enable object store wrappers like a cache to know when
/// to drop a cached entry that failed validation and retry the read from the
/// source of truth (object store) instead of repeatedly returning the same
/// invalid cached entry.
async fn read_with_validation_retry<T, Fut>(
    mut tag: ObjectStoreCallTag,
    mut read: impl FnMut(ObjectStoreCallTag) -> Fut,
) -> Result<T, SlateDBError>
where
    Fut: std::future::Future<Output = Result<T, SlateDBError>>,
{
    for _ in 0..MAX_VALIDATION_RETRIES {
        let result = read(tag).await;
        match result {
            Err(ref err) => match err.maybe_validation_retry_reason() {
                Some(reason) => {
                    warn!(
                        "retrying SST read after validation failure [reason={:?}, error={}]",
                        reason, err
                    );
                    tag.retry = Some(reason);
                }
                None => return result,
            },
            Ok(_) => return result,
        }
    }
    read(tag).await
}

/// Builds a [`BufWriter`] whose upload carries `tag` in its extensions.
fn tagged_buf_writer(
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    tag: ObjectStoreCallTag,
) -> BufWriter {
    // The one sanctioned `BufWriter::new`: it attaches the tag right after, so
    // every SST streaming write carries it. Other sites are steered here by the
    // clippy `disallowed-methods` rule on `BufWriter::new`.
    #[allow(clippy::disallowed_methods)]
    BufWriter::new(object_store, path).with_extensions(tag.into())
}

async fn write_sst_in_object_store(
    object_store: Arc<dyn ObjectStore>,
    id: &SsTableId,
    path: &Path,
    data: &Bytes,
    tag: ObjectStoreCallTag,
) -> Result<(), SlateDBError> {
    let opts = PutOptions {
        mode: PutMode::Create,
        extensions: tag.into(),
        ..PutOptions::default()
    };
    object_store
        .put_opts(path, data.clone().into(), opts)
        .await
        .map_err(|e| match e {
            object_store::Error::AlreadyExists { path: _, source: _ } => match id {
                SsTableId::Wal(_) => {
                    debug!("path already exists [path={}]", path);
                    SlateDBError::Fenced
                }
                SsTableId::Compacted(_) => SlateDBError::from(e),
            },
            _ => SlateDBError::from(e),
        })?;
    Ok(())
}

async fn write_sst_streaming_in_object_store(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
    encoded_sst: &EncodedSsTable,
    tag: ObjectStoreCallTag,
) -> Result<(), SlateDBError> {
    let mut writer = tagged_buf_writer(object_store, path.clone(), tag);
    for block in &encoded_sst.unconsumed_blocks {
        writer.put(block.encoded_bytes.clone()).await?;
    }
    writer.put(encoded_sst.footer.clone()).await?;
    writer.shutdown().await?;
    Ok(())
}

pub(crate) struct EncodedSsTableWriter {
    id: SsTableId,
    builder: EncodedSsTableBuilder,
    writer: BufWriter,
    table_store: Arc<TableStore>,
    #[cfg(test)]
    blocks_written: usize,
}

impl EncodedSsTableWriter {
    /// Adds an entry to the SSTable and returns the size of the block that was finished if any.
    /// The block size is calculated after applying any compression if enabled.
    /// The block size is None if the builder has not finished compacting a block yet.
    pub(crate) async fn add(&mut self, entry: RowEntry) -> Result<Option<usize>, SlateDBError> {
        let block_size = self.builder.add(entry).await?;
        self.drain_blocks().await?;
        Ok(block_size)
    }

    pub(crate) async fn close(mut self) -> Result<SsTableHandle, SlateDBError> {
        let mut encoded_sst = self.builder.build().await?;
        while let Some(block) = encoded_sst.unconsumed_blocks.pop_front() {
            self.writer.write_all(block.encoded_bytes.as_ref()).await?;
        }

        self.writer.write_all(encoded_sst.footer.as_ref()).await?;
        self.writer.shutdown().await?;
        self.table_store
            .cache_filters(self.id, encoded_sst.info.filter_offset, encoded_sst.filters)
            .await;
        Ok(SsTableHandle::new(
            self.id,
            encoded_sst.format_version,
            encoded_sst.info,
        ))
    }

    async fn drain_blocks(&mut self) -> Result<(), SlateDBError> {
        while let Some(block) = self.builder.next_block() {
            self.writer.write_all(block.encoded_bytes.as_ref()).await?;
            #[cfg(test)]
            {
                self.blocks_written += 1;
            }
        }
        Ok(())
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.builder.is_drained()
    }

    pub(crate) fn id(&self) -> SsTableId {
        self.id
    }

    #[cfg(test)]
    pub(crate) fn blocks_written(&self) -> usize {
        self.blocks_written
    }
}

#[allow(dead_code)]
fn slatedb_io_error() -> SlateDBError {
    SlateDBError::from(std::io::Error::other("oops"))
}

#[cfg(test)]
mod tests {
    use crate::types::KeyValue;
    use bytes::Bytes;
    use futures::future;
    use futures::StreamExt;
    use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt};
    use proptest::prelude::any;
    use proptest::proptest;
    use rstest::rstest;
    use std::collections::VecDeque;
    use std::sync::Arc;

    use crate::db_cache::test_utils::TestCache;
    use crate::db_cache::SplitCache;
    use crate::db_cache::{DbCache, DbCacheWrapper};
    use crate::error;
    use crate::format::block::Block;
    use crate::format::sst::SsTableFormat;
    use crate::manifest::SsTableView;
    use crate::object_stores::ObjectStores;
    use crate::retrying_object_store::RetryingObjectStore;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::test_utils::FlakyObjectStore;
    use crate::test_utils::{assert_iterator, build_test_sst};
    use crate::types::{RowEntry, ValueDeletable};
    use crate::{block_iterator::BlockIteratorLatest, db_state::SsTableId, iter::RowEntryIterator};
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::DbRand;

    const ROOT: &str = "/root";

    /// Wraps an object store: counts range-bounded `get_opts` calls and pauses the first
    /// one until `release` is notified. Other methods just delegate. Shared by the
    /// concurrent-read dedup tests.
    #[cfg(feature = "foyer")]
    #[derive(Debug)]
    struct PauseFirstReadStore {
        inner: Arc<dyn ObjectStore>,
        get_range_count: std::sync::atomic::AtomicUsize,
        paused: std::sync::atomic::AtomicBool,
        first_read_started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[cfg(feature = "foyer")]
    impl std::fmt::Display for PauseFirstReadStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PauseFirstReadStore({})", self.inner)
        }
    }

    #[cfg(feature = "foyer")]
    #[async_trait::async_trait]
    impl ObjectStore for PauseFirstReadStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            use std::sync::atomic::Ordering;
            if options.range.is_some() {
                self.get_range_count.fetch_add(1, Ordering::SeqCst);
                if self.paused.swap(false, Ordering::SeqCst) {
                    self.first_read_started.notify_one();
                    self.release.notified().await;
                }
            }
            self.inner.get_opts(location, options).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn count_ssts_in(store: &Arc<dyn ObjectStore>) -> usize {
        store
            .list(None)
            .filter(|r| {
                future::ready(
                    r.as_ref()
                        .unwrap()
                        .location
                        .extension()
                        .unwrap()
                        .to_lowercase()
                        == "sst",
                )
            })
            .count()
            .await
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_sst_writer_should_write_compacted_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        // given:
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let id = SsTableId::Compacted(ulid::Ulid::new());

        // when:
        let mut writer = ts.table_writer(id);
        writer
            .add(RowEntry::new_value(&[b'a'; 16], &[1u8; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(&[b'b'; 16], &[2u8; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_tombstone(&[b'c'; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(&[b'd'; 16], &[4u8; 16], 0))
            .await
            .unwrap();
        let sst = writer.close().await.unwrap();

        let sst_iter_options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        // then:
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            SsTableView::identity(sst),
            ts.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(&[b'a'; 16], &[1u8; 16], 0),
                RowEntry::new_value(&[b'b'; 16], &[2u8; 16], 0),
                RowEntry::new_tombstone(&[b'c'; 16], 0),
                RowEntry::new_value(&[b'd'; 16], &[4u8; 16], 0),
            ],
        )
        .await;

        assert_eq!(count_ssts_in(&main_store).await, 1);
        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&wal_store).await, 0);
        }
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_sst_writer_should_write_wal_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        // given:
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let id = SsTableId::Wal(123);

        // when:
        let mut writer = ts.table_writer(id);
        writer
            .add(RowEntry::new_value(&[b'a'; 16], &[1u8; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(&[b'b'; 16], &[2u8; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_tombstone(&[b'c'; 16], 0))
            .await
            .unwrap();
        writer
            .add(RowEntry::new_value(&[b'd'; 16], &[4u8; 16], 0))
            .await
            .unwrap();
        let sst = writer.close().await.unwrap();

        let sst_iter_options = SstIteratorOptions {
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };
        // then:
        let mut iter = SstIterator::new_owned_initialized(
            ..,
            SsTableView::identity(sst),
            ts.clone(),
            sst_iter_options,
        )
        .await
        .unwrap()
        .expect("Expected Some(iter) but got None");
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(&[b'a'; 16], &[1u8; 16], 0),
                RowEntry::new_value(&[b'b'; 16], &[2u8; 16], 0),
                RowEntry::new_tombstone(&[b'c'; 16], 0),
                RowEntry::new_value(&[b'd'; 16], &[4u8; 16], 0),
            ],
        )
        .await;

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 0);
            assert_eq!(count_ssts_in(&wal_store).await, 1);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 1);
        }
    }

    #[tokio::test]
    async fn test_wal_write_should_fail_when_fenced() {
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let wal_id = SsTableId::Wal(1);

        // write a wal sst
        let mut sst1 = ts.table_builder();
        sst1.add(RowEntry::new_value(b"key", b"value", 0))
            .await
            .unwrap();
        let table = sst1.build().await.unwrap();
        ts.write_sst(&wal_id, &table, false).await.unwrap();

        let mut sst2 = ts.table_builder();
        sst2.add(RowEntry::new_value(b"key", b"value", 0))
            .await
            .unwrap();
        let table2 = sst2.build().await.unwrap();

        // write another wal sst with the same id.
        let result = ts.write_sst(&wal_id, &table2, false).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_wal_fence_should_write_zero_bytes() {
        let os = Arc::new(InMemory::new());
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            SsTableFormat::default(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        ts.write_wal_fence(1).await.unwrap();

        let metadata = ts.metadata(&SsTableId::Wal(1)).await.unwrap();
        assert_eq!(metadata.size, 0);
    }

    #[tokio::test]
    async fn test_wal_fence_should_fail_when_fenced() {
        let os = Arc::new(InMemory::new());
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            SsTableFormat::default(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        ts.write_wal_fence(1).await.unwrap();
        let result = ts.write_wal_fence(1).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_wal_write_should_fail_after_zero_byte_fence() {
        let os = Arc::new(InMemory::new());
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            SsTableFormat::default(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        ts.write_wal_fence(1).await.unwrap();

        let mut sst = ts.table_builder();
        sst.add(RowEntry::new_value(b"key", b"value", 0))
            .await
            .unwrap();
        let table = sst.build().await.unwrap();
        let result = ts.write_sst(&SsTableId::Wal(1), &table, false).await;
        assert!(matches!(result, Err(error::SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn should_use_streaming_upload_for_large_compacted_sst() {
        // given:
        let value_size = 11 * 1024 * 1024;
        let os = Arc::new(
            FlakyObjectStore::new(Arc::new(InMemory::new()), 0)
                .with_single_put_size_limit(1024 * 1024),
        );
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let id = SsTableId::Compacted(ulid::Ulid::new());

        let mut builder = ts.table_builder();
        builder
            .add(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from(vec![b'x'; value_size])),
                0,
                None,
                None,
            ))
            .await
            .unwrap();
        let sst = builder.build().await.unwrap();

        // when:
        ts.write_sst(&id, &sst, false).await.unwrap();

        // then:
        assert_eq!(os.put_attempts(), 0);
        assert_eq!(os.multipart_attempts(), 1);
        ts.open_sst(&id).await.unwrap();
    }

    #[tokio::test]
    async fn should_keep_conditional_single_put_for_large_wal_sst() {
        // given:
        let value_size = 11 * 1024 * 1024;
        let os = Arc::new(
            FlakyObjectStore::new(Arc::new(InMemory::new()), 0)
                .with_single_put_size_limit(1024 * 1024),
        );
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let wal_id = SsTableId::Wal(1);

        let mut builder = ts.table_builder();
        builder
            .add(RowEntry::new(
                Bytes::from_static(b"key"),
                ValueDeletable::Value(Bytes::from(vec![b'x'; value_size])),
                0,
                None,
                None,
            ))
            .await
            .unwrap();
        let sst = builder.build().await.unwrap();

        // when:
        let result = ts.write_sst(&wal_id, &sst, false).await;

        // then:
        assert!(matches!(
            result,
            Err(error::SlateDBError::ObjectStoreError(_))
        ));
        assert_eq!(os.put_attempts(), 1);
        assert_eq!(os.multipart_attempts(), 0);
    }

    #[tokio::test]
    async fn test_checksum_mismatch_error_includes_path() {
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let id = SsTableId::Compacted(ulid::Ulid::new());

        let mut writer = ts.table_writer(id);
        writer
            .add(RowEntry::new_value(&[b'a'; 16], &[1u8; 16], 0))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let sst_path = ts.path(&id);
        let original = os.get(&sst_path).await.unwrap().bytes().await.unwrap();
        let mut corrupted = original.to_vec();
        corrupted[0] ^= 0x01;
        os.put(&sst_path, corrupted.into()).await.unwrap();

        let handle = ts.open_sst(&id).await.unwrap();
        let err = match ts.read_blocks(&handle, 0..1).await {
            Err(e) => e,
            Ok(_) => panic!("expected checksum mismatch"),
        };
        assert!(
            matches!(err, error::SlateDBError::ChecksumMismatch { path: Some(ref p) } if p == &sst_path),
            "expected ChecksumMismatch tagged with sst path, got {err:?}"
        );

        let public_msg = error::Error::from(err).to_string();
        let expected = format!("checksum mismatch in {sst_path}");
        assert!(
            public_msg.contains(&expected),
            "public error message {public_msg:?} did not contain {expected:?}"
        );
    }

    #[tokio::test]
    #[cfg(feature = "moka")]
    async fn test_tablestore_sst_and_partial_cache_hits() {
        use crate::db_cache::{moka::MokaCache, SplitCache};

        // Setup
        let os = Arc::new(InMemory::new());
        let format = SsTableFormat {
            block_size: 32,
            ..SsTableFormat::default()
        };

        let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
        let block_cache = Arc::new(MokaCache::new());
        let meta_cache = Arc::new(MokaCache::new());

        let split_cache = Arc::new(
            SplitCache::new()
                .with_block_cache(Some(block_cache.clone()))
                .with_meta_cache(Some(meta_cache))
                .build(),
        );

        let wrapper = Arc::new(DbCacheWrapper::new(
            split_cache,
            &recorder,
            Arc::new(DefaultSystemClock::default()),
        ));
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            format,
            Path::from("/root"),
            Some(wrapper.clone()),
            TableStoreKind::Main,
        ));

        // Create and write SST
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let mut writer = ts.table_writer(id);
        let mut expected_data = Vec::with_capacity(20);
        for i in 0..20 {
            let key = [i as u8; 16];
            let value = [(i + 1) as u8; 16];
            expected_data.push((
                Vec::from(key.as_slice()),
                ValueDeletable::Value(Bytes::copy_from_slice(&value)),
            ));
            writer
                .add(RowEntry::new_value(key.as_ref(), value.as_ref(), 0))
                .await
                .unwrap();
        }
        let handle = writer.close().await.unwrap();

        // Read the index
        let index = ts.read_index(&handle, true).await.unwrap();

        // Test 1: SST hit
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();

        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are now in cache
        for i in 0..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            let cached = wrapper
                .get_block(&(handle.id, offset).into())
                .await
                .unwrap_or(None);
            assert!(
                cached.is_some(),
                "Block with offset {} should be in cache",
                offset
            );
        }

        // Partially clear the cache (remove blocks 5..10 and 15..20)
        for i in 5..10 {
            let offset = index.borrow().block_meta().get(i).offset();
            wrapper.remove(&(handle.id, offset).into()).await;
        }
        for i in 15..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            wrapper.remove(&(handle.id, offset).into()).await;
        }

        // Test 2: Partial cache hit, everything should be returned since missing blocks are returned from sst
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are again in cache
        for i in 0..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            let cached = wrapper
                .get_block(&(handle.id, offset).into())
                .await
                .unwrap_or(None);
            assert!(
                cached.is_some(),
                "Block with offset {} should be in cache after partial hit",
                offset
            );
        }

        // Replace SST file with an empty file
        let path = ts.path(&id);
        os.put(&path, Bytes::new().into()).await.unwrap();

        // Test 3: All blocks should be in cache after SST file is emptied
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 0..20, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data).await;

        // Check that all blocks are still in cache
        for i in 0..20 {
            let offset = index.borrow().block_meta().get(i).offset();
            assert!(
                wrapper
                    .get_block(&(handle.id, offset).into())
                    .await
                    .unwrap_or(None)
                    .is_some(),
                "Block with offset {} should be in cache after SST emptying",
                offset
            );
        }

        // Test 4: Verify that reading specific ranges still works after SST file is emptied
        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 5..10, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data[5..10]).await;

        let blocks = ts
            .read_blocks_using_index(&handle, index.clone(), 15..20, true)
            .await
            .unwrap();
        assert_blocks(&blocks, &expected_data[15..20]).await;
    }

    #[tokio::test]
    async fn test_read_index_honors_cache_blocks() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: u32::MAX,
            ..SsTableFormat::default()
        };
        let writer = TableStore::new(
            ObjectStores::new(main_store.clone(), None),
            format.clone(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        );

        let mut builder = writer.table_builder();
        builder
            .add(RowEntry::new_value(b"key1", b"value1", 0))
            .await
            .unwrap();
        builder
            .add(RowEntry::new_value(b"key2", b"value2", 0))
            .await
            .unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let handle = writer
            .write_sst(&id, &builder.build().await.unwrap(), false)
            .await
            .unwrap();

        let meta_cache = Arc::new(TestCache::new());
        let cache = Arc::new(
            SplitCache::new()
                .with_meta_cache(Some(meta_cache.clone()))
                .build(),
        );
        let reader = TableStore::new(
            ObjectStores::new(main_store.clone(), None),
            format,
            Path::from(ROOT),
            Some(cache),
            TableStoreKind::Main,
        );
        assert_eq!(meta_cache.entry_count(), 0);

        let _ = reader.read_index(&handle, false).await.unwrap();
        assert!(meta_cache
            .get_index(&(handle.id, handle.info.index_offset).into())
            .await
            .unwrap()
            .is_none());

        let _ = reader.read_index(&handle, true).await.unwrap();
        assert!(meta_cache
            .get_index(&(handle.id, handle.info.index_offset).into())
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn test_read_filter_honors_cache_blocks() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let writer = TableStore::new(
            ObjectStores::new(main_store.clone(), None),
            format.clone(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        );

        let mut builder = writer.table_builder();
        builder
            .add(RowEntry::new_value(b"key1", b"value1", 0))
            .await
            .unwrap();
        builder
            .add(RowEntry::new_value(b"key2", b"value2", 0))
            .await
            .unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let handle = writer
            .write_sst(&id, &builder.build().await.unwrap(), false)
            .await
            .unwrap();

        let meta_cache = Arc::new(TestCache::new());
        let cache = Arc::new(
            SplitCache::new()
                .with_meta_cache(Some(meta_cache.clone()))
                .build(),
        );
        let reader = TableStore::new(
            ObjectStores::new(main_store.clone(), None),
            format,
            Path::from(ROOT),
            Some(cache),
            TableStoreKind::Main,
        );
        assert_eq!(meta_cache.entry_count(), 0);

        let filters = reader.read_filters(&handle, false).await.unwrap();
        assert!(!filters.is_empty());
        assert!(meta_cache
            .get_filter(&(handle.id, handle.info.filter_offset).into())
            .await
            .unwrap()
            .is_none());

        let _ = reader.read_filters(&handle, true).await.unwrap();
        assert!(meta_cache
            .get_filter(&(handle.id, handle.info.filter_offset).into())
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn test_read_stats_honors_cache_blocks() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::default();
        let writer = TableStore::new(
            ObjectStores::new(main_store.clone(), None),
            format.clone(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        );

        let mut builder = writer.table_builder();
        builder
            .add(RowEntry::new_value(b"key1", b"value1", 0))
            .await
            .unwrap();
        builder
            .add(RowEntry::new_tombstone(b"key2", 0))
            .await
            .unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let handle = writer
            .write_sst(&id, &builder.build().await.unwrap(), false)
            .await
            .unwrap();
        assert!(handle.info.stats_len > 0);

        let meta_cache = Arc::new(TestCache::new());
        let cache = Arc::new(
            SplitCache::new()
                .with_meta_cache(Some(meta_cache.clone()))
                .build(),
        );
        let reader = TableStore::new(
            ObjectStores::new(main_store.clone(), None),
            format,
            Path::from(ROOT),
            Some(cache),
            TableStoreKind::Main,
        );
        assert_eq!(meta_cache.entry_count(), 0);

        let stats = reader.read_stats(&handle, false).await.unwrap();
        assert!(stats.is_some());
        assert!(meta_cache
            .get_stats(&(handle.id, handle.info.stats_offset).into())
            .await
            .unwrap()
            .is_none());

        let _ = reader.read_stats(&handle, true).await.unwrap();
        assert!(meta_cache
            .get_stats(&(handle.id, handle.info.stats_offset).into())
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn test_write_sst_should_write_cache() {
        let os = Arc::new(InMemory::new());
        let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();

        let block_cache = Arc::new(TestCache::new());
        let meta_cache = Arc::new(TestCache::new());
        let split_cache = Arc::new(
            SplitCache::new()
                .with_block_cache(Some(block_cache.clone()))
                .with_meta_cache(Some(meta_cache))
                .build(),
        );

        let wrapper = Arc::new(DbCacheWrapper::new(
            split_cache,
            &recorder,
            Arc::new(DefaultSystemClock::default()),
        ));
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            SsTableFormat::default(),
            Path::from("/root"),
            Some(wrapper.clone()),
            TableStoreKind::Main,
        ));
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst = build_test_sst(&ts.sst_format, 3).await;
        let sst_bytes = sst.remaining_as_bytes();
        let sst_info = sst.info.clone();

        ts.write_sst(&id, &sst, true).await.unwrap();

        let index = ts
            .sst_format
            .read_index_raw(&sst_info, &sst_bytes)
            .await
            .unwrap();
        let block_metas = index.borrow().block_meta();
        for i in 0..block_metas.len() {
            let block_meta = block_metas.get(i);
            let block = ts
                .sst_format
                .read_block_raw(&sst_info, &index, i, &sst_bytes)
                .await
                .unwrap();
            let cached_block = wrapper
                .get_block(&(id, block_meta.offset()).into())
                .await
                .unwrap();
            assert!(cached_block.is_some());
            assert!(block == *cached_block.unwrap().block().unwrap());
        }
    }

    #[tokio::test]
    async fn test_write_sst_should_not_write_cache() {
        let os = Arc::new(InMemory::new());
        let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
        let cache = Arc::new(TestCache::new());
        let wrapper = Arc::new(DbCacheWrapper::new(
            cache.clone(),
            &recorder,
            Arc::new(DefaultSystemClock::default()),
        ));
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(os.clone(), None),
            SsTableFormat::default(),
            Path::from("/root"),
            Some(wrapper),
            TableStoreKind::Main,
        ));
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst = build_test_sst(&ts.sst_format, 3).await;
        let sst_bytes = sst.remaining_as_bytes();
        let sst_info = sst.info.clone();

        ts.write_sst(&id, &sst, false).await.unwrap();

        let index = ts
            .sst_format
            .read_index_raw(&sst_info, &sst_bytes)
            .await
            .unwrap();
        let block_metas = index.borrow().block_meta();
        for i in 0..block_metas.len() {
            let block_meta = block_metas.get(i);
            let cached_block = cache
                .get_block(&(id, block_meta.offset()).into())
                .await
                .unwrap();
            assert!(cached_block.is_none());
        }
    }

    #[allow(dead_code)]
    async fn assert_blocks(blocks: &VecDeque<Arc<Block>>, expected: &[(Vec<u8>, ValueDeletable)]) {
        let mut block_iter = blocks.iter();
        let mut expected_iter = expected.iter();

        while let (Some(block), Some(expected_item)) = (block_iter.next(), expected_iter.next()) {
            let mut iter = BlockIteratorLatest::new_ascending(block.clone());
            let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
            assert_eq!(kv.key, expected_item.0);
            assert_eq!(ValueDeletable::Value(kv.value), expected_item.1);
        }

        assert!(block_iter.next().is_none());
        assert!(expected_iter.next().is_none());
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_list_compacted_ssts(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        use ulid::Ulid;

        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        // Create id1, id2, and i3 as three random UUIDs that have been sorted ascending.
        // Need to do this because the Ulids are sometimes generated in the same millisecond
        // and the random suffix is used to break the tie, which might be out of order.
        let mut ulids = (0..3).map(|_| ulid::Ulid::new()).collect::<Vec<Ulid>>();
        ulids.sort();
        let (id1, id2, id3) = (
            SsTableId::Compacted(ulids[0]),
            SsTableId::Compacted(ulids[1]),
            SsTableId::Compacted(ulids[2]),
        );

        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        let path3 = ts.path(&id3);

        main_store.put(&path1, Bytes::new().into()).await.unwrap();
        main_store.put(&path2, Bytes::new().into()).await.unwrap();
        main_store.put(&path3, Bytes::new().into()).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 3);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);
        assert_eq!(ssts[2].id, id3);

        let ssts = ts
            .list_compacted_ssts(id2.unwrap_compacted_id()..id3.unwrap_compacted_id())
            .await
            .unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        let ssts = ts
            .list_compacted_ssts(id2.unwrap_compacted_id()..)
            .await
            .unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id2);
        assert_eq!(ssts[1].id, id3);

        let ssts = ts
            .list_compacted_ssts(..id3.unwrap_compacted_id())
            .await
            .unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_list_wal_ssts(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        let id1 = SsTableId::Wal(1);
        let id2 = SsTableId::Wal(2);
        let id3 = SsTableId::Wal(3);

        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        let path3 = ts.path(&id3);

        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path1, Bytes::new().into())
            .await
            .unwrap();
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path2, Bytes::new().into())
            .await
            .unwrap();
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path3, Bytes::new().into())
            .await
            .unwrap();

        let ssts = ts.list_wal_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 3);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);
        assert_eq!(ssts[2].id, id3);

        let ssts = ts
            .list_wal_ssts(id2.unwrap_wal_id()..id3.unwrap_wal_id())
            .await
            .unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        let ssts = ts.list_wal_ssts(id2.unwrap_wal_id()..).await.unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id2);
        assert_eq!(ssts[1].id, id3);

        let ssts = ts.list_wal_ssts(..id3.unwrap_wal_id()).await.unwrap();
        assert_eq!(ssts.len(), 2);
        assert_eq!(ssts[0].id, id1);
        assert_eq!(ssts[1].id, id2);

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 0);
            assert_eq!(count_ssts_in(&wal_store).await, 3);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 3);
        }
    }

    async fn put_wal_id(ts: &TableStore, store: &Arc<dyn ObjectStore>, id: u64) {
        store
            .put(&ts.path(&SsTableId::Wal(id)), Bytes::new().into())
            .await
            .unwrap();
    }

    fn make_ts(store: Arc<dyn ObjectStore>) -> Arc<TableStore> {
        Arc::new(TableStore::new(
            ObjectStores::new(store, None),
            SsTableFormat::default(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ))
    }

    // Boundary values picked from the algorithm:
    //   - ROUND_SIZE=8 -> first round probes offsets 1, 2, 4, ..., 128
    //   - 2nd round starts at offset 256
    // The (8, 16, 128, 256) cluster pins answers at probe offsets; the
    // surrounding values (7/9, 127/129, 255/257) pin binary search inside
    // each round. start_after=100 with stale ids 1..=100 also catches
    // regressions where the start_after offset is dropped from the probe path.
    #[rstest]
    #[tokio::test]
    async fn should_find_max_wal_id(
        #[values(0, 100)] start_after: u64,
        #[values(0, 1, 5, 7, 8, 9, 16, 127, 128, 129, 200, 255, 256, 257)] n_above: u64,
    ) {
        // given: stale ids at/below the hint (must be ignored) and n_above
        // contiguous ids above the hint.
        let store = make_store();
        let ts = make_ts(store.clone());
        for id in 1..=start_after {
            put_wal_id(&ts, &store, id).await;
        }
        for id in (start_after + 1)..=(start_after + n_above) {
            put_wal_id(&ts, &store, id).await;
        }

        // when: probing with start_after as the hint
        let result = ts.last_seen_wal_id(start_after).await.unwrap();

        // then: the high water mark above the hint is returned; if no ids
        // exist above the hint, start_after itself is returned.
        assert_eq!(result, start_after + n_above);
    }

    #[tokio::test]
    async fn test_retry_write_sst_on_timeout_and_verify_bytes() {
        // Given a flaky store that times out on the first put_opts
        let base: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyObjectStore::new(base.clone(), 1));
        let retrying = Arc::new(RetryingObjectStore::new(
            flaky.clone(),
            Arc::new(DbRand::default()),
            Arc::new(DefaultSystemClock::new()),
        ));

        let format = SsTableFormat {
            block_size: 64,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(retrying, None),
            format.clone(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        // Build an SST and compute expected bytes
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let sst = build_test_sst(&format, 3).await;
        let expected_bytes = sst.remaining_as_bytes();

        // When writing via TableStore (should retry once)
        ts.write_sst(&id, &sst, false).await.unwrap();

        // Then: a retry happened
        assert!(flaky.put_attempts() >= 2);

        // And: the stored file bytes match exactly
        let path = ts.path(&id);
        let actual = base.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(actual, expected_bytes);
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_delete_compacted_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        let id1 = SsTableId::Compacted(ulid::Ulid::new());
        let id2 = SsTableId::Compacted(ulid::Ulid::new());
        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        main_store.put(&path1, Bytes::new().into()).await.unwrap();
        main_store.put(&path2, Bytes::new().into()).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 2);

        ts.delete_sst(&id1).await.unwrap();

        let ssts = ts.list_compacted_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 1);
            assert_eq!(count_ssts_in(&wal_store).await, 0);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 1);
        }
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_delete_wal_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let format = SsTableFormat {
            block_size: 32,
            min_filter_keys: 1,
            ..SsTableFormat::default()
        };
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            format,
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));

        let id1 = SsTableId::Wal(123);
        let id2 = SsTableId::Wal(321);
        let path1 = ts.path(&id1);
        let path2 = ts.path(&id2);
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path1, Bytes::new().into())
            .await
            .unwrap();
        wal_store
            .clone()
            .unwrap_or(main_store.clone())
            .put(&path2, Bytes::new().into())
            .await
            .unwrap();

        let ssts = ts.list_wal_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 2);

        ts.delete_sst(&id1).await.unwrap();

        let ssts = ts.list_wal_ssts(..).await.unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, id2);

        if let Some(wal_store) = wal_store {
            assert_eq!(count_ssts_in(&main_store).await, 0);
            assert_eq!(count_ssts_in(&wal_store).await, 1);
        } else {
            assert_eq!(count_ssts_in(&main_store).await, 1);
        }
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_metadata_for_compacted_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store),
            SsTableFormat::default(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let path = ts.path(&id);
        let bytes = Bytes::from_static(b"compacted");
        main_store.put(&path, bytes.clone().into()).await.unwrap();

        let metadata = ts.metadata(&id).await.unwrap();
        assert_eq!(metadata.size, bytes.len() as u64);
        assert_eq!(metadata.location, path);
    }

    #[rstest]
    #[case::main_only(make_store(), None)]
    #[case::main_and_wal(make_store(), Some(make_store()))]
    #[tokio::test]
    async fn test_metadata_for_wal_sst(
        #[case] main_store: Arc<dyn ObjectStore>,
        #[case] wal_store: Option<Arc<dyn ObjectStore>>,
    ) {
        let ts = Arc::new(TableStore::new(
            ObjectStores::new(main_store.clone(), wal_store.clone()),
            SsTableFormat::default(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        ));
        let id = SsTableId::Wal(42);
        let path = ts.path(&id);
        let bytes = Bytes::from_static(b"wal");
        wal_store
            .unwrap_or(main_store)
            .put(&path, bytes.clone().into())
            .await
            .unwrap();

        let metadata = ts.metadata(&id).await.unwrap();
        assert_eq!(metadata.size, bytes.len() as u64);
        assert_eq!(metadata.location, path);
    }

    proptest! {
        #[test]
        fn convert_bytes_to_blocks_precise_when_aligned_with_block_size(
            block_size in any::<usize>(),
            num_blocks in any::<usize>(),
        ) {
            let os = Arc::new(InMemory::new());
            let format = SsTableFormat { block_size, ..SsTableFormat::default() };
            let ts = Arc::new(TableStore::new(ObjectStores::new(os, None),
                format, Path::from(ROOT), None, TableStoreKind::Main));
            if let Some(bytes) = block_size.checked_mul(num_blocks) {
                assert_eq!(num_blocks, ts.bytes_to_blocks(bytes));
            }
        }
    }

    /// End-to-end test: concurrent index reads through `TableStore` issue a single
    /// object-store request to load the index, even when the underlying object store
    /// is slow.
    ///
    /// Determinism is anchored at two points:
    ///
    /// 1. The first range-bounded `get_opts` call to the wrapped object store signals
    ///    `first_read_started` and parks on `release`. Once we see that signal we
    ///    know task A's load is in flight inside foyer's spawned loader task.
    /// 2. `tokio::join!(task_b, release_task)` polls members in source order. Task
    ///    B's first poll synchronously joins foyer's in-flight fetch (no second
    ///    object-store call); the release fires only after B has registered as a
    ///    waiter.
    #[cfg(feature = "foyer")]
    #[tokio::test]
    async fn dedups_concurrent_reads_through_object_store() {
        use crate::db_cache::foyer::FoyerCache;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use tokio::sync::Notify;

        // given: an SST written through a plain in-memory store
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::default();

        let writer = TableStore::new(
            ObjectStores::new(inner.clone(), None),
            format.clone(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        );
        let mut builder = writer.table_builder();
        builder
            .add(RowEntry::new_value(b"k1", b"v1", 0))
            .await
            .unwrap();
        builder
            .add(RowEntry::new_value(b"k2", b"v2", 0))
            .await
            .unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let handle = writer
            .write_sst(&id, &builder.build().await.unwrap(), false)
            .await
            .unwrap();

        // given: the same store wrapped so the first range read pauses, behind a
        // real FoyerCache that supports dedup
        let first_read_started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let counting = Arc::new(PauseFirstReadStore {
            inner: inner.clone(),
            get_range_count: AtomicUsize::new(0),
            paused: AtomicBool::new(true),
            first_read_started: first_read_started.clone(),
            release: release.clone(),
        });
        let counting_store: Arc<dyn ObjectStore> = counting.clone();
        let cache: Arc<dyn DbCache> = Arc::new(FoyerCache::new());
        let reader = Arc::new(TableStore::new(
            ObjectStores::new(counting_store, None),
            format,
            Path::from(ROOT),
            Some(cache),
            TableStoreKind::Main,
        ));

        // when: task A starts reading the index; its loader will pause inside the
        // wrapped object store
        let handle_a = tokio::spawn({
            let reader = reader.clone();
            let handle = handle.clone();
            async move { reader.read_index(&handle, true).await }
        });

        // wait until A's read has reached the object store and is paused
        first_read_started.notified().await;
        assert_eq!(
            counting.get_range_count.load(Ordering::SeqCst),
            1,
            "exactly one read should have hit the store so far"
        );

        // when: task B races A for the same index. join! polls B first, so B's
        // fetch_index reaches foyer's dedup map before release_task fires.
        let task_b = {
            let reader = reader.clone();
            let handle = handle.clone();
            async move { reader.read_index(&handle, true).await }
        };
        let release_task = {
            let release = release.clone();
            async move {
                tokio::task::yield_now().await;
                release.notify_one();
            }
        };
        let (b_result, _) = tokio::join!(task_b, release_task);

        // then: both callers got an index, and exactly one object-store read happened
        let a_result = handle_a.await.expect("task A panicked");
        assert!(a_result.is_ok(), "task A failed: {:?}", a_result.err());
        assert!(b_result.is_ok(), "task B failed: {:?}", b_result.err());
        assert_eq!(
            counting.get_range_count.load(Ordering::SeqCst),
            1,
            "concurrent index reads must dedup into a single object-store read"
        );
    }

    /// End-to-end test: concurrent single-block reads through `TableStore` issue a
    /// single object-store request, even when the underlying object store is slow.
    /// Same determinism strategy as the index test above. Exercises the fast-path
    /// in `read_blocks_using_index` that routes 1-block reads (e.g. point-gets via
    /// `SstIterator::for_key`) through `cache.fetch_block`.
    #[cfg(feature = "foyer")]
    #[tokio::test]
    async fn dedups_concurrent_block_reads_through_object_store() {
        use crate::db_cache::foyer::FoyerCache;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use tokio::sync::Notify;

        // given: an SST written through a plain in-memory store
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat::default();

        let writer = TableStore::new(
            ObjectStores::new(inner.clone(), None),
            format.clone(),
            Path::from(ROOT),
            None,
            TableStoreKind::Main,
        );
        let mut builder = writer.table_builder();
        builder
            .add(RowEntry::new_value(b"k1", b"v1", 0))
            .await
            .unwrap();
        builder
            .add(RowEntry::new_value(b"k2", b"v2", 0))
            .await
            .unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let handle = writer
            .write_sst(&id, &builder.build().await.unwrap(), false)
            .await
            .unwrap();

        // given: pre-load the index via the unwrapped writer so the wrapped store
        // sees only block reads (the fast-path takes `index` as an argument, so no
        // extra index read happens inside the race).
        let index = writer.read_index(&handle, false).await.unwrap();

        // given: the same store wrapped so the first range read pauses, behind a
        // real FoyerCache that supports dedup
        let first_read_started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let counting = Arc::new(PauseFirstReadStore {
            inner: inner.clone(),
            get_range_count: AtomicUsize::new(0),
            paused: AtomicBool::new(true),
            first_read_started: first_read_started.clone(),
            release: release.clone(),
        });
        let counting_store: Arc<dyn ObjectStore> = counting.clone();
        let cache: Arc<dyn DbCache> = Arc::new(FoyerCache::new());
        let reader = Arc::new(TableStore::new(
            ObjectStores::new(counting_store, None),
            format,
            Path::from(ROOT),
            Some(cache),
            TableStoreKind::Main,
        ));

        // when: task A starts a single-block read; its loader will pause inside the
        // wrapped object store
        let handle_a = tokio::spawn({
            let reader = reader.clone();
            let handle = handle.clone();
            let index = index.clone();
            async move {
                reader
                    .read_blocks_using_index(&handle, index, 0..1, true)
                    .await
            }
        });

        // wait until A's read has reached the object store and is paused
        first_read_started.notified().await;
        assert_eq!(
            counting.get_range_count.load(Ordering::SeqCst),
            1,
            "exactly one read should have hit the store so far"
        );

        // when: task B races A for the same block. join! polls B first, so B's
        // fetch_block reaches foyer's dedup map before release_task fires.
        let task_b = {
            let reader = reader.clone();
            let handle = handle.clone();
            let index = index.clone();
            async move {
                reader
                    .read_blocks_using_index(&handle, index, 0..1, true)
                    .await
            }
        };
        let release_task = {
            let release = release.clone();
            async move {
                tokio::task::yield_now().await;
                release.notify_one();
            }
        };
        let (b_result, _) = tokio::join!(task_b, release_task);

        // then: both callers got the block, and exactly one object-store read happened
        let a_result = handle_a.await.expect("task A panicked");
        assert!(a_result.is_ok(), "task A failed: {:?}", a_result.err());
        assert!(b_result.is_ok(), "task B failed: {:?}", b_result.err());
        assert_eq!(a_result.as_ref().unwrap().len(), 1);
        assert_eq!(b_result.as_ref().unwrap().len(), 1);
        assert_eq!(
            counting.get_range_count.load(Ordering::SeqCst),
            1,
            "concurrent single-block reads must dedup into a single object-store read"
        );
    }

    mod kind_tags {
        use super::super::{
            read_with_validation_retry, ObjectStoreCallTag, TableStoreKind, MAX_VALIDATION_RETRIES,
        };
        use super::{Path, ROOT};
        use crate::db_state::{SsTableId, SstType};
        use crate::error::{RetryReason, SlateDBError};
        use crate::format::sst::SsTableFormat;
        use crate::object_stores::ObjectStores;
        use crate::tablestore::TableStore;
        use crate::test_utils::{build_test_sst, RecordingObjectStore};
        use object_store::memory::InMemory;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};

        fn format() -> SsTableFormat {
            SsTableFormat {
                block_size: 32,
                min_filter_keys: 1,
                ..SsTableFormat::default()
            }
        }

        fn recording_store(kind: TableStoreKind) -> (Arc<RecordingObjectStore>, Arc<TableStore>) {
            let recording = Arc::new(RecordingObjectStore::new(Arc::new(InMemory::new())));
            let ts = Arc::new(TableStore::new(
                ObjectStores::new(recording.clone(), None),
                format(),
                Path::from(ROOT),
                None,
                kind,
            ));
            (recording, ts)
        }

        // Compacted-SST range reads carry the source kind and Compacted type.
        #[tokio::test]
        async fn compacted_reads_carry_source_and_type() {
            let (recording, ts) = recording_store(TableStoreKind::Reader);
            let encoded = build_test_sst(&format(), 4).await;
            let id = SsTableId::Compacted(ulid::Ulid::new());
            let handle = ts.write_sst(&id, &encoded, false).await.unwrap();

            recording.clear();
            ts.read_index(&handle, false).await.unwrap();

            let kinds = recording.get_kinds(false);
            assert!(!kinds.is_empty(), "expected at least one range read");
            assert!(
                kinds.iter().all(|k| *k == Some(TableStoreKind::Reader)),
                "compacted reads should carry the source kind, got {kinds:?}"
            );
            assert!(
                recording
                    .get_sst_types(false)
                    .iter()
                    .all(|t| *t == Some(SstType::Compacted)),
                "compacted reads should carry the Compacted type"
            );
            assert!(
                recording.get_retries(false).iter().all(|r| r.is_none()),
                "a successful read should carry no retry reason"
            );
        }

        // A compacted-SST metadata HEAD read carries the source kind and type.
        #[tokio::test]
        async fn compacted_metadata_head_carries_source_and_type() {
            let (recording, ts) = recording_store(TableStoreKind::Compactor);
            let encoded = build_test_sst(&format(), 1).await;
            let id = SsTableId::Compacted(ulid::Ulid::new());
            ts.write_sst(&id, &encoded, false).await.unwrap();

            recording.clear();
            ts.metadata(&id).await.unwrap();

            assert_eq!(
                recording.get_kinds(true),
                vec![Some(TableStoreKind::Compactor)],
                "the metadata HEAD read should carry the source kind"
            );
            assert_eq!(
                recording.get_sst_types(true),
                vec![Some(SstType::Compacted)],
                "the metadata HEAD read should carry the Compacted type"
            );
        }

        // WAL writes carry the source kind and the Wal type (no longer untagged).
        #[tokio::test]
        async fn wal_writes_carry_source_and_wal_type() {
            let (recording, ts) = recording_store(TableStoreKind::Main);
            let encoded = build_test_sst(&format(), 1).await;
            let id = SsTableId::Wal(1);
            ts.write_sst(&id, &encoded, false).await.unwrap();

            let kinds = recording.write_kinds();
            let sst_types = recording.write_sst_types();
            assert!(!kinds.is_empty(), "expected at least one write");
            assert!(
                kinds.iter().all(|k| *k == Some(TableStoreKind::Main)),
                "WAL writes should carry the source kind, got {kinds:?}"
            );
            assert!(
                sst_types.iter().all(|t| *t == Some(SstType::Wal)),
                "WAL writes should carry the Wal type, got {sst_types:?}"
            );
        }

        // A recoverable validation failure on a compacted read reissues it once
        // with the same source/type and a RetryReason describing the failure.
        #[tokio::test]
        async fn validation_failure_reissues_with_retry_reason() {
            let observed: Arc<Mutex<Vec<ObjectStoreCallTag>>> = Arc::new(Mutex::new(Vec::new()));
            let attempts = AtomicUsize::new(0);
            let obs = observed.clone();
            let result: Result<u8, SlateDBError> = read_with_validation_retry(
                ObjectStoreCallTag::new(TableStoreKind::Compactor, SstType::Compacted),
                |tag| {
                    obs.lock().unwrap().push(tag);
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if n == 0 {
                            Err(SlateDBError::ChecksumMismatch { path: None })
                        } else {
                            Ok(42u8)
                        }
                    }
                },
            )
            .await;

            assert_eq!(result.unwrap(), 42);
            assert_eq!(MAX_VALIDATION_RETRIES, 1);
            let calls = observed.lock().unwrap().clone();
            assert_eq!(
                calls,
                vec![
                    ObjectStoreCallTag {
                        kind: TableStoreKind::Compactor,
                        sst_type: SstType::Compacted,
                        retry: None,
                    },
                    ObjectStoreCallTag {
                        kind: TableStoreKind::Compactor,
                        sst_type: SstType::Compacted,
                        retry: Some(RetryReason::CrcMismatch),
                    },
                ],
                "the reissue must carry the same source/type and the retry reason"
            );
        }

        // A WAL read is reissued once on a recoverable validation failure, the
        // same as a compacted read, with the RetryReason set on the reissue.
        #[tokio::test]
        async fn wal_read_is_retried() {
            let observed: Arc<Mutex<Vec<ObjectStoreCallTag>>> = Arc::new(Mutex::new(Vec::new()));
            let attempts = AtomicUsize::new(0);
            let obs = observed.clone();
            let result: Result<u8, SlateDBError> = read_with_validation_retry(
                ObjectStoreCallTag::new(TableStoreKind::Main, SstType::Wal),
                |tag| {
                    obs.lock().unwrap().push(tag);
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if n == 0 {
                            Err(SlateDBError::ChecksumMismatch { path: None })
                        } else {
                            Ok(7u8)
                        }
                    }
                },
            )
            .await;

            assert_eq!(result.unwrap(), 7);
            assert_eq!(
                observed.lock().unwrap().clone(),
                vec![
                    ObjectStoreCallTag {
                        kind: TableStoreKind::Main,
                        sst_type: SstType::Wal,
                        retry: None,
                    },
                    ObjectStoreCallTag {
                        kind: TableStoreKind::Main,
                        sst_type: SstType::Wal,
                        retry: Some(RetryReason::CrcMismatch),
                    },
                ],
                "a WAL read should be reissued once with the retry reason"
            );
        }

        #[tokio::test]
        async fn compacted_writes_carry_source_and_compacted_type() {
            let (recording, ts) = recording_store(TableStoreKind::Compactor);
            let encoded = build_test_sst(&format(), 4).await;
            let id = SsTableId::Compacted(ulid::Ulid::new());
            ts.write_sst(&id, &encoded, false).await.unwrap();

            let kinds = recording.write_kinds();
            let sst_types = recording.write_sst_types();
            assert!(!kinds.is_empty(), "expected at least one write");
            assert!(
                kinds.iter().all(|k| *k == Some(TableStoreKind::Compactor)),
                "compacted writes should carry the source kind, got {kinds:?}"
            );
            assert!(
                sst_types.iter().all(|t| *t == Some(SstType::Compacted)),
                "compacted writes should carry the Compacted type, got {sst_types:?}"
            );
        }
    }
}
