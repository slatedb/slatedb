use std::ops::Bound::Unbounded;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, warn};
use tokio::sync::OnceCell;

use crate::bytes_range::BytesRange;
use crate::db_state::{SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::manifest::VersionedManifest;
use crate::partitioned_keyspace::partitions_covering_range;
use crate::tablestore::TableStore;

/// Cache content that [`DbCacheManagerOps::warm_sst`] should populate.
#[derive(Clone, Debug)]
pub enum CacheTarget {
    /// Warm all filters on the SST, if any exist.
    Filters,
    /// Warm the SST index.
    Index,
    /// Warm the SST stats block, if one exists.
    Stats,
    /// Warm the SST data blocks that overlap the supplied key range.
    ///
    /// Also warms the SST index, since block planning depends on it.
    Data((Bound<Bytes>, Bound<Bytes>)),
}

impl CacheTarget {
    /// Convenience constructor for [`CacheTarget::Data`] that accepts any
    /// [`RangeBounds`], mirroring the `Db::scan` signature. Pass `..` to
    /// warm all data blocks.
    pub fn data<K, T>(range: T) -> Self
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>,
    {
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        CacheTarget::Data((start, end))
    }
}

/// Trait for block-cache warming and eviction operations.
#[async_trait]
pub trait DbCacheManagerOps {
    /// Warms selected cache content for one SST.
    ///
    /// Callers fan out over SSTs themselves (for example with
    /// `FuturesUnordered`) to get the concurrency they want. Per-target
    /// outcomes are reflected in cache-manager metrics, not the return value.
    ///
    /// Returns `Err` on the first failing target. If no block cache is
    /// configured, or if the SST is not reachable from the current manifest,
    /// the call is a no-op that returns `Ok(())`.
    async fn warm_sst(
        &self,
        sst_id: SsTableId,
        targets: &[CacheTarget],
    ) -> Result<(), crate::Error>;

    /// Best-effort eviction of block-cache entries for one SST.
    ///
    /// If no block cache is configured, logs a warning and returns `Ok(())`.
    /// Does not check whether the SST is still live in the current manifest —
    /// callers own that policy.
    async fn evict_cached_sst(&self, sst_id: SsTableId) -> Result<(), crate::Error>;
}

pub(crate) async fn warm_sst_impl(
    table_store: &Arc<TableStore>,
    manifest: &VersionedManifest,
    sst_id: SsTableId,
    targets: &[CacheTarget],
) -> Result<(), crate::Error> {
    if targets.is_empty() {
        return Ok(());
    }
    if table_store.cache().is_none() {
        warn!("warm_sst called on a Db without a block cache configured");
        return Ok(());
    }

    let visible_ranges: Vec<BytesRange> = manifest
        .l0()
        .iter()
        .chain(manifest.compacted().iter().flat_map(|run| &run.sst_views))
        .filter(|view| view.sst.id == sst_id)
        .filter_map(|view| view.calculate_view_range(BytesRange::new(Unbounded, Unbounded)))
        .collect();
    if visible_ranges.is_empty() {
        debug!(
            "warm_sst: SST {:?} not reachable from current manifest",
            sst_id
        );
        return Ok(());
    }

    let handle = table_store.open_sst(&sst_id).await?;
    // Shared lazy index — populated at most once, so parallel target fanout
    // can share a single object-store read.
    let index_cell: OnceCell<Result<Arc<SsTableIndexOwned>, SlateDBError>> = OnceCell::new();

    for target in targets {
        match target {
            CacheTarget::Filters => warm_filters(table_store, &handle).await?,
            CacheTarget::Index => warm_index(table_store, &handle, &index_cell).await?,
            CacheTarget::Stats => warm_stats(table_store, &handle).await?,
            CacheTarget::Data(data_range) => {
                warm_data(
                    table_store,
                    &handle,
                    &index_cell,
                    &visible_ranges,
                    data_range,
                    &sst_id,
                )
                .await?
            }
        }
    }
    Ok(())
}

pub(crate) async fn evict_cached_sst_impl(
    table_store: &Arc<TableStore>,
    sst_id: SsTableId,
) -> Result<(), crate::Error> {
    if table_store.cache().is_none() {
        warn!("evict_cached_sst called on a Db without a block cache configured");
        return Ok(());
    }
    let handle = table_store.open_sst(&sst_id).await?;
    table_store.evict_sst_from_cache(&handle).await;
    Ok(())
}

async fn warm_data(
    table_store: &Arc<TableStore>,
    handle: &SsTableHandle,
    index_cell: &OnceCell<Result<Arc<SsTableIndexOwned>, SlateDBError>>,
    visible_ranges: &[BytesRange],
    data_range: &(Bound<Bytes>, Bound<Bytes>),
    sst_id: &SsTableId,
) -> Result<(), crate::Error> {
    let Some(requested) = BytesRange::try_new(data_range.0.clone(), data_range.1.clone()) else {
        debug!(
            "warm_sst: SST {:?} data target range {:?} collapses to empty, skipping",
            sst_id, data_range
        );
        return Ok(());
    };

    let intersections: Vec<BytesRange> = visible_ranges
        .iter()
        .filter_map(|v| v.intersect(&requested))
        .collect();
    if intersections.is_empty() {
        debug!(
            "warm_sst: SST {:?} data target range {:?} does not overlap visible ranges",
            sst_id, requested
        );
        return Ok(());
    }

    let index = ensure_index(table_store, handle, index_cell).await?;
    for r in &intersections {
        let block_range = partitions_covering_range(
            &index.borrow(),
            r.start_bound().map(|b| b.as_ref()),
            r.end_bound().map(|b| b.as_ref()),
        );
        if block_range.is_empty() {
            continue;
        }
        table_store
            .read_blocks_using_index(handle, index.clone(), block_range, true)
            .await?;
    }
    Ok(())
}

async fn warm_index(
    table_store: &Arc<TableStore>,
    handle: &SsTableHandle,
    index_cell: &OnceCell<Result<Arc<SsTableIndexOwned>, SlateDBError>>,
) -> Result<(), crate::Error> {
    ensure_index(table_store, handle, index_cell).await?;
    Ok(())
}

async fn warm_filters(
    table_store: &Arc<TableStore>,
    handle: &SsTableHandle,
) -> Result<(), crate::Error> {
    // filter_len == 0 means "no filters"; filter_offset aliases index_offset
    // in that case and cannot be meaningfully probed or warmed.
    if handle.info.filter_len == 0 {
        return Ok(());
    }
    table_store.read_filters(handle, true).await?;
    Ok(())
}

async fn warm_stats(
    table_store: &Arc<TableStore>,
    handle: &SsTableHandle,
) -> Result<(), crate::Error> {
    // stats_len == 0 means "no stats block"; stats_offset is 0 in that case
    // and collides with the first data block's cache key.
    if handle.info.stats_len == 0 {
        return Ok(());
    }
    table_store.read_stats(handle, true).await?;
    Ok(())
}

async fn ensure_index(
    table_store: &Arc<TableStore>,
    handle: &SsTableHandle,
    index_cell: &OnceCell<Result<Arc<SsTableIndexOwned>, SlateDBError>>,
) -> Result<Arc<SsTableIndexOwned>, SlateDBError> {
    let result: &Result<Arc<SsTableIndexOwned>, SlateDBError> = index_cell
        .get_or_init(|| async { table_store.read_index(handle, true).await })
        .await;
    match result {
        Ok(index) => Ok(index.clone()),
        Err(e) => Err(e.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FlushOptions, FlushType, PutOptions, Settings, WriteOptions};
    use crate::db::Db;
    use crate::db_cache::stats as cache_stats;
    use crate::db_cache::{CachedKey, DbCache};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use slatedb_common::metrics::{lookup_metric_with_labels, DefaultMetricsRecorder};

    // Compile-time check: the trait is object-safe.
    fn _assert_object_safe(_: &dyn DbCacheManagerOps) {}

    const PATH: &str = "/cache_manager_test";

    async fn open_db_single_sst_with_metrics(
        object_store: Arc<dyn ObjectStore>,
    ) -> (Db, Arc<DefaultMetricsRecorder>) {
        // No l0_sst_size_bytes cap so one flush yields a single SST whose cache
        // we can then inspect in isolation.
        let metrics = Arc::new(DefaultMetricsRecorder::new());
        let db = Db::builder(PATH, object_store)
            .with_settings(Settings {
                flush_interval: None,
                ..Default::default()
            })
            .with_metrics_recorder(metrics.clone())
            .build()
            .await
            .expect("failed to open db");
        (db, metrics)
    }

    fn data_block_misses(metrics: &Arc<DefaultMetricsRecorder>) -> i64 {
        lookup_metric_with_labels(
            metrics,
            cache_stats::ACCESS_COUNT,
            &[("entry_kind", "data_block"), ("result", "miss")],
        )
        .unwrap_or(0)
    }

    fn data_block_hits(metrics: &Arc<DefaultMetricsRecorder>) -> i64 {
        lookup_metric_with_labels(
            metrics,
            cache_stats::ACCESS_COUNT,
            &[("entry_kind", "data_block"), ("result", "hit")],
        )
        .unwrap_or(0)
    }

    async fn flush_to_l0(db: &Db) {
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("failed to flush memtable");
    }

    async fn write_keys(db: &Db, count: usize) {
        // Pad values so the resulting SST spans several blocks; otherwise
        // small values pack into a single block and range-scoped warming
        // cannot be distinguished from whole-SST warming.
        let padding = vec![b'x'; 256];
        for i in 0..count {
            let key = format!("key{:06}", i);
            let mut value = format!("value{:06}", i).into_bytes();
            value.extend_from_slice(&padding);
            db.put_with_options(
                key.as_bytes(),
                &value,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .expect("put failed");
        }
    }

    fn first_l0_sst_id(db: &Db) -> SsTableId {
        let manifest = db.manifest();
        manifest
            .l0()
            .iter()
            .next()
            .map(|v| v.sst.id)
            .expect("expected at least one L0 SST")
    }

    fn data_bounds(target: &CacheTarget) -> &(Bound<Bytes>, Bound<Bytes>) {
        match target {
            CacheTarget::Data(bounds) => bounds,
            _ => panic!("expected Data variant"),
        }
    }

    #[test]
    fn should_build_data_target_from_closed_range() {
        // given
        let range = b"a".as_slice()..b"z".as_slice();

        // when
        let target = CacheTarget::data(range);

        // then
        let (start, end) = data_bounds(&target);
        assert_eq!(start, &Bound::Included(Bytes::from_static(b"a")));
        assert_eq!(end, &Bound::Excluded(Bytes::from_static(b"z")));
    }

    #[test]
    fn should_build_data_target_from_unbounded_range() {
        // given / when
        let target = CacheTarget::data::<&[u8], _>(..);

        // then
        let (start, end) = data_bounds(&target);
        assert_eq!(start, &Unbounded);
        assert_eq!(end, &Unbounded);
    }

    #[test]
    fn should_reject_empty_data_range_during_planning() {
        // given: reversed bounds
        let start = Bound::Included(Bytes::from_static(b"z"));
        let end = Bound::Excluded(Bytes::from_static(b"a"));

        // when
        let range = BytesRange::try_new(start, end);

        // then
        assert!(range.is_none());
    }

    #[tokio::test]
    async fn should_serve_get_from_cache_after_warming_full_range() {
        // given: a single-SST DB with its cache evicted
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.evict_cached_sst(sst_id).await.expect("evict");

        // when: we warm all data blocks, then read a key
        db.warm_sst(sst_id, &[CacheTarget::data::<&[u8], _>(..)])
            .await
            .expect("warm_sst");
        let misses_after_warm = data_block_misses(&metrics);
        let hits_before_get = data_block_hits(&metrics);
        db.get(b"key000032").await.expect("get");

        // then: the read only produced hits — no new misses
        assert_eq!(data_block_misses(&metrics), misses_after_warm);
        assert!(data_block_hits(&metrics) > hits_before_get);

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_serve_only_warmed_range_from_cache() {
        // given: a single-SST DB with its cache evicted
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.evict_cached_sst(sst_id).await.expect("evict");

        // when: we warm only the upper half of the keyspace
        db.warm_sst(sst_id, &[CacheTarget::data(b"key000032".as_slice()..)])
            .await
            .expect("warm_sst");

        // then: a read inside the warmed range hits, a read below misses
        let misses_before_warmed_read = data_block_misses(&metrics);
        db.get(b"key000040").await.expect("get warmed");
        assert_eq!(
            data_block_misses(&metrics),
            misses_before_warmed_read,
            "read in warmed range should hit",
        );

        db.get(b"key000000").await.expect("get unwarmed");
        assert!(
            data_block_misses(&metrics) > misses_before_warmed_read,
            "read outside warmed range should miss",
        );

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_return_closed_after_db_close() {
        // given: a closed DB with a known SST
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, _) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 8).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.close().await.expect("close");

        // when / then: both ops reject the call with Closed
        let warm_err = db
            .warm_sst(sst_id, &[CacheTarget::Index])
            .await
            .expect_err("warm_sst on closed db");
        assert_eq!(
            warm_err.kind(),
            crate::ErrorKind::Closed(crate::CloseReason::Clean),
        );
        let evict_err = db
            .evict_cached_sst(sst_id)
            .await
            .expect_err("evict_cached_sst on closed db");
        assert_eq!(
            evict_err.kind(),
            crate::ErrorKind::Closed(crate::CloseReason::Clean),
        );
    }

    fn project_l0_view(manifest: &mut VersionedManifest, visible_range: BytesRange) {
        let l0 = &mut manifest.manifest.core.l0;
        let view = l0.pop_front().expect("expected at least one L0 view");
        l0.push_front(view.with_visible_range(visible_range));
    }

    #[tokio::test]
    async fn should_warm_only_within_visible_view_range() {
        // given: a single-SST DB whose cache is empty
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.evict_cached_sst(sst_id).await.expect("evict");

        // and: a manifest snapshot that restricts the SST view to the upper half
        let mut manifest = db.manifest();
        project_l0_view(
            &mut manifest,
            BytesRange::from_ref("key000032".as_bytes()..),
        );

        // when: we warm the full range through the projected manifest
        warm_sst_impl(
            &db.inner.table_store,
            &manifest,
            sst_id,
            &[CacheTarget::data::<&[u8], _>(..)],
        )
        .await
        .expect("warm_sst_impl");

        // then: a read inside the visible range hits, a read below it misses
        let misses_after_warm = data_block_misses(&metrics);
        db.get(b"key000040").await.expect("get in visible range");
        assert_eq!(
            data_block_misses(&metrics),
            misses_after_warm,
            "read inside visible range should hit warmed blocks",
        );

        db.get(b"key000000").await.expect("get below visible range");
        assert!(
            data_block_misses(&metrics) > misses_after_warm,
            "read below visible range should miss",
        );

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_skip_warming_when_requested_range_outside_visible_view() {
        // given: a single-SST DB whose cache is empty
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.evict_cached_sst(sst_id).await.expect("evict");

        // and: a manifest that restricts the view to the upper half
        let mut manifest = db.manifest();
        project_l0_view(
            &mut manifest,
            BytesRange::from_ref("key000032".as_bytes()..),
        );

        // when: we warm a data range that falls entirely below the visible view
        let misses_before_warm = data_block_misses(&metrics);
        warm_sst_impl(
            &db.inner.table_store,
            &manifest,
            sst_id,
            &[CacheTarget::data(
                b"key000000".as_slice()..b"key000010".as_slice(),
            )],
        )
        .await
        .expect("warm_sst_impl");

        // then: warming was a no-op — no blocks fetched, and a later read in that
        // sub-range still misses
        assert_eq!(data_block_misses(&metrics), misses_before_warm);
        db.get(b"key000005").await.expect("get");
        assert!(data_block_misses(&metrics) > misses_before_warm);

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_warm_union_of_multiple_views_referencing_same_sst() {
        // given: a single-SST DB whose cache is empty
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.evict_cached_sst(sst_id).await.expect("evict");

        // and: a manifest with two L0 views of the same SST — one restricted to
        // the low quarter, one to the high quarter, leaving the middle unmapped
        let mut manifest = db.manifest();
        let l0 = &mut manifest.manifest.core.l0;
        let original = l0.pop_front().expect("expected at least one L0 view");
        let low = original.with_visible_range(BytesRange::from_ref(
            "key000000".as_bytes().."key000016".as_bytes(),
        ));
        let high = original.with_visible_range(BytesRange::from_ref("key000048".as_bytes()..));
        l0.push_front(high);
        l0.push_front(low);

        // when: we warm the full range
        warm_sst_impl(
            &db.inner.table_store,
            &manifest,
            sst_id,
            &[CacheTarget::data::<&[u8], _>(..)],
        )
        .await
        .expect("warm_sst_impl");

        // then: reads in either visible view hit; a read in the gap between views misses
        let misses_after_warm = data_block_misses(&metrics);
        db.get(b"key000008").await.expect("get in low view");
        db.get(b"key000056").await.expect("get in high view");
        assert_eq!(
            data_block_misses(&metrics),
            misses_after_warm,
            "reads inside either visible view should hit warmed blocks",
        );

        db.get(b"key000032").await.expect("get in gap");
        assert!(
            data_block_misses(&metrics) > misses_after_warm,
            "read in the gap between visible views should miss",
        );

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_return_ok_when_sst_not_in_manifest() {
        // given: a DB with one flushed SST and a fresh unknown SST id
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 8).await;
        flush_to_l0(&db).await;
        let misses_before = data_block_misses(&metrics);
        let unknown_id = SsTableId::Compacted(ulid::Ulid::new());

        // when: we warm an SST that isn't reachable from the manifest
        db.warm_sst(unknown_id, &[CacheTarget::data::<&[u8], _>(..)])
            .await
            .expect("warm_sst should no-op for unreachable SST");

        // then: no data-block IO happened
        assert_eq!(data_block_misses(&metrics), misses_before);

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_short_circuit_on_target_failure() {
        // given: a flushed SST whose underlying object has been deleted out
        // from under the DB, leaving the manifest reference dangling
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.inner
            .table_store
            .delete_sst(&sst_id)
            .await
            .expect("delete_sst");

        // when: we warm two targets; the first IO will fail
        let misses_before = data_block_misses(&metrics);
        let result = db
            .warm_sst(
                sst_id,
                &[CacheTarget::Index, CacheTarget::data::<&[u8], _>(..)],
            )
            .await;

        // then: warm_sst surfaces the error and the Data target is never attempted
        assert!(result.is_err(), "warm_sst should return Err");
        assert_eq!(
            data_block_misses(&metrics),
            misses_before,
            "later targets must not be attempted after a failure",
        );

        db.close().await.expect("close");
    }

    // Opens a DB whose tiny fixture SST still carries filters (default
    // min_filter_keys=1000 would suppress them), flushes one SST, and evicts
    // its cache entries so meta sections start empty.
    async fn open_db_with_evicted_meta_sections() -> (Db, SsTableId, SsTableHandle, Arc<dyn DbCache>)
    {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(PATH, os)
            .with_settings(Settings {
                flush_interval: None,
                min_filter_keys: 1,
                ..Default::default()
            })
            .build()
            .await
            .expect("failed to open db");
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.evict_cached_sst(sst_id).await.expect("evict");

        let table_store = db.inner.table_store.clone();
        let handle = table_store.open_sst(&sst_id).await.expect("open_sst");
        let cache = table_store.cache().expect("cache configured").clone();
        (db, sst_id, handle, cache)
    }

    #[tokio::test]
    async fn should_populate_cache_for_filters_target() {
        // given
        let (db, sst_id, handle, cache) = open_db_with_evicted_meta_sections().await;
        let filter_key: CachedKey = (sst_id, handle.info.filter_offset).into();
        let index_key: CachedKey = (sst_id, handle.info.index_offset).into();
        let stats_key: CachedKey = (sst_id, handle.info.stats_offset).into();
        assert!(handle.info.filter_len > 0, "expected SST to carry filters");
        assert!(cache.get_filter(&filter_key).await.unwrap().is_none());

        // when
        db.warm_sst(sst_id, &[CacheTarget::Filters])
            .await
            .expect("warm_sst");

        // then: only the filter section was fetched
        assert!(cache.get_filter(&filter_key).await.unwrap().is_some());
        assert!(
            cache.get_index(&index_key).await.unwrap().is_none(),
            "filters target must not fetch the index",
        );
        assert!(
            cache.get_stats(&stats_key).await.unwrap().is_none(),
            "filters target must not fetch stats",
        );

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_populate_cache_for_index_target() {
        // given
        let (db, sst_id, handle, cache) = open_db_with_evicted_meta_sections().await;
        let filter_key: CachedKey = (sst_id, handle.info.filter_offset).into();
        let index_key: CachedKey = (sst_id, handle.info.index_offset).into();
        let stats_key: CachedKey = (sst_id, handle.info.stats_offset).into();
        assert!(cache.get_index(&index_key).await.unwrap().is_none());

        // when
        db.warm_sst(sst_id, &[CacheTarget::Index])
            .await
            .expect("warm_sst");

        // then: only the index section was fetched
        assert!(cache.get_index(&index_key).await.unwrap().is_some());
        assert!(
            cache.get_filter(&filter_key).await.unwrap().is_none(),
            "index target must not fetch filters",
        );
        assert!(
            cache.get_stats(&stats_key).await.unwrap().is_none(),
            "index target must not fetch stats",
        );

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_populate_cache_for_stats_target() {
        // given
        let (db, sst_id, handle, cache) = open_db_with_evicted_meta_sections().await;
        let filter_key: CachedKey = (sst_id, handle.info.filter_offset).into();
        let index_key: CachedKey = (sst_id, handle.info.index_offset).into();
        let stats_key: CachedKey = (sst_id, handle.info.stats_offset).into();
        assert!(handle.info.stats_len > 0, "expected SST to carry stats");
        assert!(cache.get_stats(&stats_key).await.unwrap().is_none());

        // when
        db.warm_sst(sst_id, &[CacheTarget::Stats])
            .await
            .expect("warm_sst");

        // then: only the stats section was fetched
        assert!(cache.get_stats(&stats_key).await.unwrap().is_some());
        assert!(
            cache.get_filter(&filter_key).await.unwrap().is_none(),
            "stats target must not fetch filters",
        );
        assert!(
            cache.get_index(&index_key).await.unwrap().is_none(),
            "stats target must not fetch the index",
        );

        db.close().await.expect("close");
    }

    #[tokio::test]
    async fn should_miss_cache_after_eviction() {
        // given: a warmed SST
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (db, metrics) = open_db_single_sst_with_metrics(os).await;
        write_keys(&db, 64).await;
        flush_to_l0(&db).await;
        let sst_id = first_l0_sst_id(&db);
        db.warm_sst(sst_id, &[CacheTarget::data::<&[u8], _>(..)])
            .await
            .expect("warm_sst");

        // when: we evict the SST and then read a key
        db.evict_cached_sst(sst_id).await.expect("evict");
        let misses_before_get = data_block_misses(&metrics);
        db.get(b"key000032").await.expect("get");

        // then: the read produced a cache miss
        assert!(data_block_misses(&metrics) > misses_before_get);

        db.close().await.expect("close");
    }
}
